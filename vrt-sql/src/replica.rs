//! Read-only D1 wire surface for a [`Follower`](crate::follower::Follower).
//!
//! This is the replica half of bookmark routing. The provider sends eligible reads here with the
//! tenant's session header intact; the replica:
//!
//! - verifies the service-to-service signature (same format as the writer),
//! - rejects `first-primary`, `x-cf-d1-session-commit-token: <bookmark>` values it cannot serve,
//!   and writes, with HTTP 409 + a typed `code` so the provider can fall back to the writer,
//! - executes reads on the current immutable generation with a fresh read-only connection,
//! - returns a bookmark signed at its own `(epoch, lsn)` watermark.
//!
//! Writes are never executed here: the engine is opened with `SQLITE_OPEN_READ_ONLY` and
//! `immutable=1`, and SQLite's readonly error is mapped to `VRT_REPLICA_READ_ONLY`.
//!
//! The status endpoint is internal (auth required) and is intended for ops and for a future
//! provider-side watermark cache; routing itself needs no poller because the replica answers
//! `VRT_REPLICA_STALE` precisely.

use crate::bookmark::BookmarkSigner;
use crate::engine::RusqliteEngine;
use crate::follower::Follower;
use crate::service::ServiceAuth;
use crate::wire;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Routing signal: the requested bookmark is newer than this replica. Retry on the writer.
pub const CODE_STALE: &str = "VRT_REPLICA_STALE";
/// Routing signal: `first-primary` sessions must go to the writer.
pub const CODE_PRIMARY_REQUIRED: &str = "VRT_REPLICA_PRIMARY_REQUIRED";
/// Routing signal: the statement would write. Retry on the writer.
pub const CODE_READ_ONLY: &str = "VRT_REPLICA_READ_ONLY";
/// Routing signal: the bookmark is malformed or not signed by the configured key.
pub const CODE_BOOKMARK_INVALID: &str = "VRT_REPLICA_BOOKMARK_INVALID";
/// Routing signal: this replica does not replicate the requested database.
pub const CODE_UNKNOWN_DB: &str = "VRT_REPLICA_UNKNOWN_DB";
/// Routing signal: no generation is available yet. Retry on the writer.
pub const CODE_NOT_READY: &str = "VRT_REPLICA_NOT_READY";
/// Routing signal: dump is writer-only (single consistent export).
pub const CODE_PRIMARY_ONLY: &str = "VRT_REPLICA_PRIMARY_ONLY";

/// Replica service state.
#[derive(Clone)]
pub struct ReplicaState {
    pub follower: Arc<Follower>,
    /// Signs the replica watermark; absent -> no bookmark header and bookmark constraints are
    /// rejected (the provider falls back to the writer).
    pub signer: Option<BookmarkSigner>,
    /// Writer-shared service auth. `None` is for unit tests only.
    pub auth: Option<ServiceAuth>,
    pub permits: Arc<Semaphore>,
    pub inflight: Arc<Semaphore>,
    pub body_max: usize,
    pub txn_budget: Duration,
}

impl ReplicaState {
    pub fn new(follower: Arc<Follower>) -> Self {
        Self {
            follower,
            signer: None,
            auth: None,
            permits: Arc::new(Semaphore::new(64)),
            inflight: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            body_max: 1024 * 1024,
            txn_budget: Duration::from_secs(30),
        }
    }

    pub fn with_signer(mut self, signer: BookmarkSigner) -> Self {
        self.signer = Some(signer);
        self
    }

    pub fn with_auth(mut self, auth: ServiceAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_permits(mut self, n: usize) -> Self {
        self.permits = Arc::new(Semaphore::new(n));
        self
    }

    pub fn with_body_max(mut self, n: usize) -> Self {
        self.body_max = n;
        self
    }

    pub fn with_txn_budget(mut self, d: Duration) -> Self {
        self.txn_budget = d;
        self
    }

    /// Wait for in-flight requests (graceful shutdown).
    pub async fn drain(&self) {
        let _ = self
            .inflight
            .clone()
            .acquire_many_owned(Semaphore::MAX_PERMITS as u32)
            .await;
    }
}

#[derive(serde::Deserialize)]
struct FormatParam {
    #[serde(rename = "resultsFormat")]
    results_format: Option<String>,
}

/// Replica router.
pub fn app(state: ReplicaState) -> Router {
    Router::new()
        .route("/dbs/{id}/query", post(query_handler))
        .route("/dbs/{id}/execute", post(write_reject_handler))
        .route("/dbs/{id}/dump", post(dump_reject_handler))
        .route("/internal/replica/status", get(status_handler))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(Arc::new(state))
}

async fn healthz() -> Response {
    text(StatusCode::OK, "ok")
}

async fn readyz(State(state): State<Arc<ReplicaState>>) -> Response {
    match state.follower.state() {
        Ok(Some(st)) if st.lsn > 0 => text(StatusCode::OK, "ready"),
        _ => text(StatusCode::SERVICE_UNAVAILABLE, "not ready"),
    }
}

fn text(status: StatusCode, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(body))
        .unwrap()
}

fn json_response(status: StatusCode, body: serde_json::Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// Typed routing error consumed by the provider (see `vrt-provider/src/d1.rs`).
fn routing_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    json_response(
        status,
        json!({
            "success": false,
            "error": message,
            "code": code,
            "retryable": true,
        }),
    )
}

fn db_id_from(path: &str) -> Option<&str> {
    path.strip_prefix("/dbs/")?.rsplit_once('/').map(|(id, _)| id)
}

fn session_constraint(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(wire::BOOKMARK_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

/// Why a request cannot be served locally.
enum NotServable {
    NotReady,
    PrimaryRequired,
    BookmarkInvalid,
    Stale,
}

impl NotServable {
    fn response(self) -> Response {
        match self {
            NotServable::NotReady => routing_error(
                StatusCode::CONFLICT,
                CODE_NOT_READY,
                "replica has no readable generation yet",
            ),
            NotServable::PrimaryRequired => routing_error(
                StatusCode::CONFLICT,
                CODE_PRIMARY_REQUIRED,
                "session requires the primary",
            ),
            NotServable::BookmarkInvalid => routing_error(
                StatusCode::CONFLICT,
                CODE_BOOKMARK_INVALID,
                "bookmark is invalid for this replica",
            ),
            NotServable::Stale => routing_error(
                StatusCode::CONFLICT,
                CODE_STALE,
                "replica is behind the requested bookmark",
            ),
        }
    }
}

fn check_session(
    state: &ReplicaState,
    headers: &HeaderMap,
    db: &str,
    applied_epoch: u64,
    applied_lsn: u64,
) -> Result<(), NotServable> {
    let Some(raw) = session_constraint(headers) else {
        return Ok(());
    };
    if raw.eq_ignore_ascii_case("first-primary") {
        return Err(NotServable::PrimaryRequired);
    }
    if raw.is_empty()
        || raw.eq_ignore_ascii_case("first-unconstrained")
        || raw.eq_ignore_ascii_case("first-primary")
    {
        return Ok(());
    }
    let Some(signer) = &state.signer else {
        return Err(NotServable::BookmarkInvalid);
    };
    let (epoch, lsn) = signer.verify(db, raw).map_err(|_| NotServable::BookmarkInvalid)?;
    if (applied_epoch, applied_lsn) < (epoch, lsn) {
        return Err(NotServable::Stale);
    }
    Ok(())
}

async fn query_handler(
    State(state): State<Arc<ReplicaState>>,
    request: axum::extract::Request,
) -> Response {
    let started = Instant::now();
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return routing_error(StatusCode::TOO_MANY_REQUESTS, CODE_NOT_READY, "replica overloaded"),
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return routing_error(StatusCode::SERVICE_UNAVAILABLE, CODE_NOT_READY, "replica draining"),
    };
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            );
        }
    }
    let path = request.uri().path().to_string();
    let Some(db) = db_id_from(&path) else {
        return json_response(StatusCode::BAD_REQUEST, json!({"error": "invalid path"}));
    };
    if db != state.follower.database_id() {
        return routing_error(StatusCode::CONFLICT, CODE_UNKNOWN_DB, "replica serves another database");
    }
    let db = db.to_string();
    let params = Query::<FormatParam>::try_from_uri(request.uri())
        .map(|q| q.0)
        .unwrap_or(FormatParam { results_format: None });
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| path.clone());
    let method = request.method().clone();
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            )
        }
    };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return json_response(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}));
        }
    }
    let Some(applied) = state.follower.state().ok().flatten() else {
        return NotServable::NotReady.response();
    };
    if let Err(reason) = check_session(&state, &headers, &db, applied.epoch, applied.lsn) {
        return reason.response();
    }
    let Some(generation) = state.follower.current_db_path().ok().flatten() else {
        return NotServable::NotReady.response();
    };
    let engine = match RusqliteEngine::open_immutable(
        &generation,
        &db,
        applied.epoch,
        applied.lsn,
    ) {
        Ok(e) => Arc::new(e),
        Err(e) => {
            tracing::warn!(db = %db, error = %e, "replica generation open failed");
            return routing_error(
                StatusCode::SERVICE_UNAVAILABLE,
                CODE_NOT_READY,
                "replica generation unavailable",
            );
        }
    };
    let budget = wire::clamp_budget(
        state.txn_budget,
        headers
            .get(wire::DEADLINE_MS_HEADER)
            .and_then(|v| v.to_str().ok()),
    );
    let call = wire::TrustedCallCtx {
        epoch: applied.epoch,
        seq: None,
        deadline: Instant::now() + budget,
    };
    let r = wire::handle_d1_on_engine(
        engine,
        &db,
        params.results_format.as_deref(),
        call,
        &body,
    )
    .await;
    let bookmark = state
        .signer
        .as_ref()
        .map(|s| s.sign(&db, applied.epoch, applied.lsn));
    let outcome = if r.status != StatusCode::OK {
        "unavailable"
    } else if r.body.get("success") == Some(&serde_json::Value::Bool(false)) {
        "sql_error"
    } else {
        "ok"
    };
    crate::metrics::query("query", outcome, started.elapsed().as_secs_f64());
    crate::metrics::rows(&db, r.rows_read, r.rows_written);
    let mut b = Response::builder().status(r.status);
    if let Some(bm) = bookmark {
        b = b.header(wire::BOOKMARK_HEADER, bm);
    }
    b.header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r.body.to_string()))
        .unwrap()
}

/// `/execute` is writer-only. The provider never sends it here, but a direct call gets a typed
/// routing error rather than a generic 404.
async fn write_reject_handler() -> Response {
    routing_error(
        StatusCode::CONFLICT,
        CODE_READ_ONLY,
        "replica is read-only; retry on the writer",
    )
}

/// A dump must be a single consistent export; keep it on the writer.
async fn dump_reject_handler() -> Response {
    routing_error(
        StatusCode::CONFLICT,
        CODE_PRIMARY_ONLY,
        "dump is served by the writer",
    )
}

async fn status_handler(
    State(state): State<Arc<ReplicaState>>,
    request: axum::extract::Request,
) -> Response {
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &[]) {
            return json_response(StatusCode::UNAUTHORIZED, json!({"error": "unauthorized"}));
        }
    }
    match state.follower.status() {
        Ok(s) => {
            let bookmark = state
                .signer
                .as_ref()
                .map(|signer| signer.sign(&s.database_id, s.applied_epoch, s.applied_lsn));
            json_response(
                StatusCode::OK,
                json!({
                    "databaseId": s.database_id,
                    "phase": s.phase,
                    "appliedEpoch": s.applied_epoch,
                    "appliedLsn": s.applied_lsn,
                    "headEpoch": s.head_epoch,
                    "headLsn": s.head_lsn,
                    "lagCommits": s.lag_commits(),
                    "snapshotLsn": s.snapshot_lsn,
                    "generation": s.generation,
                    "generationBytes": s.generation_bytes,
                    "updatedAtMs": s.updated_at_ms,
                    "bookmark": bookmark,
                    "lastError": s.last_error,
                }),
            )
        }
        Err(e) => {
            tracing::warn!(error = %e, "replica status read failed");
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "replica status unavailable"}),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> HeaderMap {
        HeaderMap::new()
    }

    #[test]
    fn db_id_parsing_is_strict() {
        assert_eq!(db_id_from("/dbs/abc/query"), Some("abc"));
        assert_eq!(db_id_from("/dbs/abc/execute"), Some("abc"));
        assert_eq!(db_id_from("/dbs/abc"), None);
        assert_eq!(db_id_from("/other/abc/query"), None);
    }

    #[test]
    fn session_constraint_is_case_insensitive() {
        let mut h = headers();
        h.insert(wire::BOOKMARK_HEADER, "first-primary".parse().unwrap());
        assert_eq!(session_constraint(&h), Some("first-primary"));
    }
}
