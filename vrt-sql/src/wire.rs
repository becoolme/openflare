//! D1 wire layer (section 2 protocol spec): `POST /dbs/{id}/query` and `POST /dbs/{id}/execute`
//! share one handler; `POST /dbs/{id}/dump` returns the SQLite-compatible export as bytes.
//! Two error surfaces: SQL failure = HTTP 200 + `{success:false,error}`;
//! malformed request (schema mismatch) = HTTP 400 + a non-`{success:false}` shape.
//! `dump` errors are non-200 because workerd's d1-api only parses an error body then.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::queue::QueueBackend;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use serde_json::json;

use crate::bookmark::BookmarkSigner;
use crate::engine::{EngineError, EngineRegistry, ResultsFormat, RusqliteEngine, SqlStmt, TxnCtx};

pub const BOOKMARK_HEADER: &str = "x-cf-d1-session-commit-token";
/// provider->vrt-sql internal extension headers (the (epoch, seq) dedup key of section 3 R4).
/// **Trust boundary (S4)**: the handler does not read raw headers directly -- the caller constructs
/// `TrustedCallCtx` after internal authentication passes. User Workers cannot control these two values
/// (facade/provider overwrite them). T34 wires in the real provider authentication.
pub const EPOCH_HEADER: &str = "x-vrt-epoch";
pub const SEQ_HEADER: &str = "x-vrt-seq";
/// RV-R71: remaining request budget (ms) forwarded by vrt-provider.
/// Shrink-only: the effective budget is `min(txn_budget, header)`;
/// missing / non-numeric / zero values are ignored and a value larger
/// than `txn_budget` can never enlarge it.
pub const DEADLINE_MS_HEADER: &str = "x-vrt-deadline-ms";

/// RV-R71: pure helper so the clamp rule is unit-testable.
pub fn clamp_budget(own: Duration, header: Option<&str>) -> Duration {
    match header.and_then(|v| v.trim().parse::<u64>().ok()) {
        Some(ms) if ms > 0 => own.min(Duration::from_millis(ms)),
        _ => own,
    }
}

const DEFAULT_TXN_BUDGET_MS: u64 = 30_000;

/// Trusted call context (S4): can only be constructed by a caller that has passed internal authentication.
/// T34: populated after the vrt-provider side service-to-service auth (mTLS/HMAC) passes;
/// user Worker traffic never reaches this step.
pub struct TrustedCallCtx {
    /// Current writer epoch (lease position).
    pub epoch: u64,
    /// Caller-supplied dedup sequence number; None = no dedup.
    pub seq: Option<u64>,
    /// Deadline for this batch.
    pub deadline: Instant,
}

impl TrustedCallCtx {
    /// Test/internal pass-through constructor (no dedup).
    pub fn test(deadline: Instant) -> Self {
        Self {
            epoch: 0,
            seq: None,
            deadline,
        }
    }
}

pub struct WireResponse {
    pub status: StatusCode,
    pub bookmark: Option<String>,
    pub body: serde_json::Value,
    /// Rows read cumulatively in this batch (section 10 approximation; used for metrics).
    pub rows_read: u64,
    /// Rows written cumulatively in this batch.
    pub rows_written: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<EngineRegistry>,
    pub signer: BookmarkSigner,
    pub txn_budget: Duration,
    /// T34: provider->vrt-sql service-to-service auth. `None` is for unit-test pass-through only.
    pub auth: Option<crate::service::ServiceAuth>,
    /// Bounded concurrency (try_acquire, reject when exceeded).
    pub permits: Arc<tokio::sync::Semaphore>,
    /// Request body cap -- Content-Length is validated before reading the body.
    pub body_max: usize,
    /// Bounded databaseId -> label.
    pub db_labels: Arc<crate::service::DbLabels>,
    /// Drain counter (graceful shutdown).
    pub inflight: Arc<tokio::sync::Semaphore>,
}

impl AppState {
    pub fn new(db_root: std::path::PathBuf, hmac_key: [u8; 32]) -> Self {
        Self {
            registry: Arc::new(EngineRegistry::new(db_root)),
            signer: BookmarkSigner::new(hmac_key),
            txn_budget: Duration::from_millis(DEFAULT_TXN_BUDGET_MS),
            auth: None,
            permits: Arc::new(tokio::sync::Semaphore::new(64)),
            body_max: 1024 * 1024,
            db_labels: Arc::new(crate::service::DbLabels::default()),
            inflight: Arc::new(tokio::sync::Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    /// Attach service-to-service auth (must be set on the production path).
    pub fn with_auth(mut self, auth: crate::service::ServiceAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_permits(mut self, n: usize) -> Self {
        self.permits = Arc::new(tokio::sync::Semaphore::new(n));
        self
    }

    pub fn with_body_max(mut self, n: usize) -> Self {
        self.body_max = n;
        self
    }

    /// Graceful shutdown: wait for all in-flight permits to be returned.
    pub async fn drain(&self) {
        // Full-quota inflight semaphore: if all permits can be acquired at once => nothing is in flight.
        let _ = self
            .inflight
            .clone()
            .acquire_many_owned(Semaphore::MAX_PERMITS as u32)
            .await;
    }
}

use tokio::sync::Semaphore;

/// Build a TrustedCallCtx from raw headers. **Call only after service-to-service auth passes**
/// (T34 landed: query_handler verifies the signature before getting here) -- the epoch/seq header
/// values are already covered by the signature, so tampering is rejected at the verification stage.
fn trusted_ctx_from_headers(headers: &HeaderMap, deadline: Instant) -> Result<TrustedCallCtx, ()> {
    let epoch = headers.get(EPOCH_HEADER);
    let seq = headers.get(SEQ_HEADER);
    let (epoch, seq) = match (epoch, seq) {
        (None, None) => (0, None),
        (Some(e), Some(s)) => {
            let e = e
                .to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(())?;
            let s = s
                .to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(())?;
            (e, Some(s))
        }
        _ => return Err(()),
    };
    Ok(TrustedCallCtx {
        epoch,
        seq,
        deadline,
    })
}

/// T36: error surface extension -- additional `code`/`retryable` fields (backward compatible,
/// the D1 `{success,error}` shape is unchanged). Fenced/Unavailable let the provider
/// map to D1 retryable. `pub`: the cross-language wire fixtures take real output directly
/// (RV-R68).
pub fn error_body(e: &EngineError) -> serde_json::Value {
    match e {
        EngineError::Fenced(_) => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_FENCED",
            "retryable": true,
        }),
        EngineError::Unavailable(_) => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_UNAVAILABLE",
            "retryable": true,
        }),
        // RV-R71: deadline expired before anything committed — safe retry.
        EngineError::DeadlineExceeded => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_SQL_TIMEOUT",
            "retryable": true,
        }),
        // RV-R71: deadline expired after commit started — outcome unknown;
        // retry only with the same (epoch, seq) idempotency key.
        EngineError::ResultUnknown => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_SQL_RESULT_UNKNOWN",
            "retryable": true,
        }),
        // Read-only follower: the statement would write. The provider retries on the writer.
        EngineError::ReadOnly => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_REPLICA_READ_ONLY",
            "retryable": true,
        }),
        _ => json!({ "success": false, "error": e.wire_message() }),
    }
}

/// Transient unavailability -> 503 (other error surfaces remain HTTP 200 as before).
/// `pub`: the wire fixtures record both the status code and the body (RV-R68).
pub fn error_status(e: &EngineError) -> StatusCode {
    match e {
        EngineError::Unavailable(_)
        | EngineError::DeadlineExceeded
        | EngineError::ResultUnknown => StatusCode::SERVICE_UNAVAILABLE,
        // Routing signal, not a SQL result: the provider retries the read on the writer.
        EngineError::ReadOnly => StatusCode::CONFLICT,
        _ => StatusCode::OK,
    }
}

/// Parse the D1 statement batch. Malformed input uses the non-`{success:false}` surface,
/// because workerd treats it as a protocol error rather than a SQL error.
fn parse_batch(body: &[u8]) -> Result<Vec<SqlStmt>, WireResponse> {
    let parsed: Result<Vec<SqlStmt>, ()> = (|| {
        let v: serde_json::Value = serde_json::from_slice(body).map_err(|_| ())?;
        let one_or_many = match v {
            serde_json::Value::Array(a) => serde_json::Value::Array(a),
            o @ serde_json::Value::Object(_) => json!([o]),
            _ => return Err(()),
        };
        serde_json::from_value::<Vec<SqlStmt>>(one_or_many).map_err(|_| ())
    })();
    parsed.map_err(|()| WireResponse {
        status: StatusCode::BAD_REQUEST,
        bookmark: None,
        rows_read: 0,
        rows_written: 0,
        body: json!({ "error": "invalid request: expected {sql, params?} or array of such" }),
    })
}

/// Pure-function handler: `/query` and `/execute` have identical semantics (section 2).
/// `results_format` is parsed from the URL parameter by the caller (invalid values fall back to ARRAY_OF_OBJECTS).
pub async fn handle_d1(
    state: &AppState,
    database_id: &str,
    results_format: Option<&str>,
    call: TrustedCallCtx,
    body: &[u8],
) -> WireResponse {
    let stmts = match parse_batch(body) {
        Ok(s) => s,
        Err(bad) => return bad,
    };
    let ctx = TxnCtx {
        epoch: call.epoch,
        deadline: call.deadline,
        dedup: call.seq.map(|s| (call.epoch, s)),
    };
    let format = ResultsFormat::parse(results_format);

    // RV-R71: engine open + SQLite + durable publish are all blocking
    // (SQLite, std Mutex, object-store bridge). Run them on a blocking
    // thread, never on a tokio worker, and cut the wait at the call
    // deadline (+ small grace so the engine's own, more precise, deadline
    // errors win the race). The blocking task itself is bounded too:
    // lock wait, lease renew, publish and SQLite's progress handler all
    // honour `ctx.deadline`.
    let probe = crate::engine::CommitProbe::new();
    let task = {
        let registry = state.registry.clone();
        let signer = state.signer.clone();
        let probe = probe.clone();
        let database_id = database_id.to_string();
        let call_epoch = call.epoch;
        tokio::task::spawn_blocking(move || {
            crate::engine::with_commit_probe(&probe, || {
                let engine = registry
                    .get_until(crate::paths::ResourceKind::D1, &database_id, ctx.deadline)
                    .map_err(|e| (e, false))?;
                // The signed epoch is a routing/dedup hint, not the writer
                // authority: the manifest CAS is authoritative and a process
                // restart may advance the epoch before the control plane has
                // republished it. Rejecting a lower epoch here would strand a
                // healthy restarted writer, so the lease/manifest gate below
                // remains the fence.
                match run_query(&engine, &stmts, &ctx, format) {
                    Ok(results) => {
                        // T36: degraded (lease valid but renewal failed) -> reads still allowed but counted.
                        if engine.is_degraded() {
                            crate::metrics::degraded_read();
                        }
                        // T36: bookmark epoch = the writer lease epoch (when persistence is on),
                        // aligned with the manifest epoch; without persistence the caller's epoch is kept.
                        let book_epoch = if engine.persistence_enabled() {
                            engine.lease_epoch()
                        } else {
                            call_epoch
                        };
                        let bookmark = signer.sign(&database_id, book_epoch, engine.commit_lsn());
                        Ok((results, bookmark))
                    }
                    Err(e) => {
                        // T36: fenced -> detach the engine; the next get self-heals via claim/restore.
                        if let EngineError::Fenced(ep) = e {
                            registry.evict_fenced(crate::paths::ResourceKind::D1, &database_id, ep);
                        }
                        Err((e, true))
                    }
                }
            })
        })
    };
    let cutoff = tokio::time::Instant::from_std(call.deadline) + D1_OUTER_GRACE;
    let joined = match tokio::time::timeout_at(cutoff, task).await {
        Ok(j) => j,
        Err(_) => {
            // The blocking task is not cancellable; it will unwind at its
            // own deadline checks. The caller must not be told "success".
            let e = if probe.committed() {
                EngineError::ResultUnknown
            } else {
                EngineError::DeadlineExceeded
            };
            tracing::warn!(db = %database_id, code = %error_body(&e)["code"], "d1 call cut at deadline");
            return WireResponse {
                status: error_status(&e),
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: error_body(&e),
            };
        }
    };
    let outcome = match joined {
        Ok(r) => r,
        Err(_) => {
            // spawn_blocking task panicked: fail closed, retryable, and
            // never claim success. (No SQL text / params are logged.)
            tracing::error!(db = %database_id, "d1 blocking task panicked");
            let e = EngineError::Unavailable("internal task failed");
            return WireResponse {
                status: error_status(&e),
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: error_body(&e),
            };
        }
    };
    match outcome {
        Ok((results, bookmark)) => {
            let rows_read = results.iter().map(|r| r.meta.rows_read.max(0) as u64).sum();
            let rows_written = results
                .iter()
                .map(|r| r.meta.rows_written.max(0) as u64)
                .sum();
            WireResponse {
                status: StatusCode::OK,
                bookmark: Some(bookmark),
                rows_read,
                rows_written,
                body: json!(results),
            }
        }
        // Path/lock/open failure -> {success:false} error surface (same shape as SQL failure);
        // T36: Unavailable -> 503 (held-elsewhere/restore failure is retryable).
        Err((e, _ran)) => WireResponse {
            status: error_status(&e),
            bookmark: None,
            rows_read: 0,
            rows_written: 0,
            body: error_body(&e),
        },
    }
}

/// Execute a D1 batch on one engine, bypassing the registry.
///
/// This is the read-only follower path: the caller has already selected and opened an immutable
/// generation. No dedup ledger and no bookmark are involved -- the replica handler signs its own
/// `(epoch, lsn)` after the call.
pub async fn handle_d1_on_engine(
    engine: Arc<RusqliteEngine>,
    database_id: &str,
    results_format: Option<&str>,
    call: TrustedCallCtx,
    body: &[u8],
) -> WireResponse {
    let stmts = match parse_batch(body) {
        Ok(s) => s,
        Err(bad) => return bad,
    };
    let ctx = TxnCtx {
        epoch: call.epoch,
        deadline: call.deadline,
        dedup: None,
    };
    let format = ResultsFormat::parse(results_format);
    let read_only = engine.is_read_only();
    let probe = crate::engine::CommitProbe::new();
    let task = {
        let probe = probe.clone();
        tokio::task::spawn_blocking(move || {
            crate::engine::with_commit_probe(&probe, || run_query(&engine, &stmts, &ctx, format))
        })
    };
    let cutoff = tokio::time::Instant::from_std(call.deadline) + D1_OUTER_GRACE;
    let joined = match tokio::time::timeout_at(cutoff, task).await {
        Ok(j) => j,
        Err(_) => {
            // On a replica nothing can commit, so a timeout is always retry-safe.
            let e = if !read_only && probe.committed() {
                EngineError::ResultUnknown
            } else {
                EngineError::DeadlineExceeded
            };
            return WireResponse {
                status: error_status(&e),
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: error_body(&e),
            };
        }
    };
    let outcome = match joined {
        Ok(r) => r,
        Err(_) => {
            tracing::error!(db = %database_id, "replica d1 blocking task panicked");
            let e = EngineError::Unavailable("internal task failed");
            return WireResponse {
                status: error_status(&e),
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: error_body(&e),
            };
        }
    };
    match outcome {
        Ok(results) => {
            let rows_read = results.iter().map(|r| r.meta.rows_read.max(0) as u64).sum();
            let rows_written = results
                .iter()
                .map(|r| r.meta.rows_written.max(0) as u64)
                .sum();
            WireResponse {
                status: StatusCode::OK,
                bookmark: None,
                rows_read,
                rows_written,
                body: json!(results),
            }
        }
        Err(e) => WireResponse {
            status: error_status(&e),
            bookmark: None,
            rows_read: 0,
            rows_written: 0,
            body: error_body(&e),
        },
    }
}

/// RV-R71: how long past the call deadline the async side keeps waiting
/// for the blocking task's own (more precise) deadline error.
const D1_OUTER_GRACE: Duration = Duration::from_millis(500);

fn run_query(
    engine: &Arc<RusqliteEngine>,
    stmts: &[SqlStmt],
    ctx: &TxnCtx,
    format: ResultsFormat,
) -> Result<Vec<crate::engine::QueryResult>, EngineError> {
    engine.query_batch_sync(stmts, ctx, format)
}

// ---------- axum wiring ----------

#[derive(serde::Deserialize)]
struct FormatParam {
    #[serde(rename = "resultsFormat")]
    results_format: Option<String>,
}

fn endpoint_label(path: &str) -> &'static str {
    if path.ends_with("/query") {
        "query"
    } else {
        "execute"
    }
}

/// T34 wiring order: concurrency permit -> Content-Length precheck -> body collection
/// (DefaultBodyLimit as backstop) -> service-to-service signature verification -> only then parse
/// `x-vrt-epoch`/`x-vrt-seq` to build TrustedCallCtx. Authentication failures are always
/// 401 + `outcome="unauthorized"`, and x-vrt-* is never read.
async fn query_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let endpoint = endpoint_label(request.uri().path());
    let fail = |outcome: &'static str, status: StatusCode, body: serde_json::Value| {
        crate::metrics::query(endpoint, outcome, started.elapsed().as_secs_f64());
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    // Bounded concurrency (try_acquire, never queue).
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "rate_limited",
                StatusCode::TOO_MANY_REQUESTS,
                json!({"success": false, "error": "vrt-sql overloaded"}),
            )
        }
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"success": false, "error": "vrt-sql draining"}),
            )
        }
    };
    // Content-Length precheck: reject oversize outright without reading the body (guards against unauthenticated large bodies).
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            );
        }
    }
    let database_id = request
        .uri()
        .path()
        .trim_start_matches("/dbs/")
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string();
    let db_id = request
        .uri()
        .path()
        .strip_prefix("/dbs/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_else(|| database_id.clone());
    let results_format = Query::<FormatParam>::try_from_uri(request.uri())
        .ok()
        .and_then(|q| q.0.results_format);
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            )
        }
    };
    // Service-to-service auth: the signature covers method/path+query/body/epoch/seq.
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return fail(
                "unauthorized",
                StatusCode::UNAUTHORIZED,
                json!({"error": "unauthorized"}),
            );
        }
    }
    // x-vrt-* may be trusted only after authentication passes.
    let budget = clamp_budget(
        state.txn_budget,
        headers
            .get(DEADLINE_MS_HEADER)
            .and_then(|v| v.to_str().ok()),
    );
    let deadline = Instant::now() + budget;
    let call = match trusted_ctx_from_headers(&headers, deadline) {
        Ok(c) => c,
        Err(()) => {
            return fail(
                "protocol_error",
                StatusCode::BAD_REQUEST,
                json!({ "error": "invalid request: x-vrt-epoch/x-vrt-seq must be u64 pair" }),
            )
        }
    };
    let r = handle_d1(&state, &db_id, results_format.as_deref(), call, &body).await;
    // Metrics: endpoint x outcome + rows (bounded db label).
    let outcome = if r.status != StatusCode::OK {
        if r.body.get("code") == Some(&serde_json::Value::String("VRT_UNAVAILABLE".into())) {
            "unavailable"
        } else if matches!(
            r.body.get("code").and_then(|c| c.as_str()),
            Some("VRT_SQL_TIMEOUT" | "VRT_SQL_RESULT_UNKNOWN")
        ) {
            "timeout"
        } else {
            "protocol_error"
        }
    } else if r.body.get("success") == Some(&serde_json::Value::Bool(false)) {
        match r.body.get("code").and_then(|c| c.as_str()) {
            Some("VRT_FENCED") => "fenced",
            _ => "sql_error",
        }
    } else {
        "ok"
    };
    crate::metrics::query(endpoint, outcome, started.elapsed().as_secs_f64());
    let db_label = state.db_labels.label(&db_id);
    crate::metrics::rows(&db_label, r.rows_read, r.rows_written);
    let mut b = Response::builder().status(r.status);
    if let Some(bm) = r.bookmark {
        b = b.header(BOOKMARK_HEADER, bm);
    }
    b.header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r.body.to_string()))
        .unwrap()
}

/// `POST /dbs/{id}/dump`: `D1Database.dump()`. Same trust chain as
/// `query_handler` (permit -> Content-Length -> signature -> trusted ctx);
/// the success body is the SQLite-compatible SQL script as
/// `application/octet-stream` -- exactly what workerd's d1-api returns to
/// the tenant as an ArrayBuffer. Errors are non-200 JSON: d1-api only
/// parses an error body when the status is not 200, and would otherwise
/// hand the tenant the JSON error text as if it were a dump.
async fn dump_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let fail = |outcome: &'static str, status: StatusCode, body: serde_json::Value| {
        crate::metrics::query("dump", outcome, started.elapsed().as_secs_f64());
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "rate_limited",
                StatusCode::TOO_MANY_REQUESTS,
                json!({"success": false, "error": "vrt-sql overloaded"}),
            )
        }
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"success": false, "error": "vrt-sql draining"}),
            )
        }
    };
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            );
        }
    }
    let db_id = request
        .uri()
        .path()
        .strip_prefix("/dbs/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            )
        }
    };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return fail(
                "unauthorized",
                StatusCode::UNAUTHORIZED,
                json!({"error": "unauthorized"}),
            );
        }
    }
    let budget = clamp_budget(
        state.txn_budget,
        headers
            .get(DEADLINE_MS_HEADER)
            .and_then(|v| v.to_str().ok()),
    );
    let deadline = Instant::now() + budget;
    let call = match trusted_ctx_from_headers(&headers, deadline) {
        Ok(c) => c,
        Err(()) => {
            return fail(
                "protocol_error",
                StatusCode::BAD_REQUEST,
                json!({ "error": "invalid request: x-vrt-epoch/x-vrt-seq must be u64 pair" }),
            )
        }
    };
    let task = {
        let registry = state.registry.clone();
        let db_id = db_id.clone();
        tokio::task::spawn_blocking(move || {
            let engine = registry
                .get_until(crate::paths::ResourceKind::D1, &db_id, deadline)
                .map_err(|e| (e, false))?;
            let ctx = TxnCtx {
                epoch: call.epoch,
                deadline: call.deadline,
                dedup: None,
            };
            match engine.dump_sync(&ctx) {
                Ok(bytes) => {
                    if engine.is_degraded() {
                        crate::metrics::degraded_read();
                    }
                    Ok(bytes)
                }
                Err(e) => {
                    if let EngineError::Fenced(ep) = e {
                        registry.evict_fenced(crate::paths::ResourceKind::D1, &db_id, ep);
                    }
                    Err((e, true))
                }
            }
        })
    };
    let cutoff = tokio::time::Instant::from_std(deadline) + D1_OUTER_GRACE;
    let outcome: Result<Vec<u8>, EngineError> = match tokio::time::timeout_at(cutoff, task).await {
        Ok(Ok(Ok(bytes))) => Ok(bytes),
        Ok(Ok(Err((e, _ran)))) => Err(e),
        Ok(Err(_join)) => Err(EngineError::Unavailable("dump task failed")),
        // dump never writes: a cut can only mean "nothing committed".
        Err(_) => Err(EngineError::DeadlineExceeded),
    };
    match outcome {
        Ok(bytes) => {
            crate::metrics::query("dump", "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::from(bytes))
                .unwrap()
        }
        Err(e) => {
            // A 200 here would make d1-api treat the JSON body as dump bytes.
            let status = match error_status(&e) {
                StatusCode::OK => StatusCode::INTERNAL_SERVER_ERROR,
                s => s,
            };
            let outcome = match status {
                StatusCode::SERVICE_UNAVAILABLE => "unavailable",
                _ => "error",
            };
            fail(outcome, status, error_body(&e))
        }
    }
}

/// T36: read-only diagnostic `GET /internal/dbs` -- fenced/degraded databases and the last
/// uploaded lsn. Verified with the same ServiceAuth as queries (GET + empty body,
/// epoch=0 seq=none are covered by the signing string). Allowed through when auth is not configured (unit tests).
async fn diag_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, b"") {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"error": "unauthorized"}).to_string()))
                .unwrap();
        }
    }
    let dbs = state.registry.diagnostics();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(json!({"databases": dbs}).to_string()))
        .unwrap()
}

async fn healthz() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({"ok": true}).to_string()))
        .unwrap()
}

/// T35b: restricted data-plane delete -- `POST /dbs/{id}/drop`.
/// Same trust chain as query_handler: concurrency permit -> Content-Length ->
/// service-to-service signature (covers method/path/body/epoch/seq) -> allowlisted id derives the
/// directory -> registry detaches the engine + deletes the directory. Idempotent: returns dropped:false if absent.
/// The path is composed only of the signature-verified, allowlist-checked databaseId; path/target
/// fields in the body are never read.
///
/// T35c: the whole registry.drop_db (including the in-flight Arc drain wait and directory deletion)
/// is moved onto spawn_blocking -- synchronous blocking no longer stalls tokio worker threads or affects
/// queries of other dbs on the same worker; plus a bounded DROP_TOTAL_TIMEOUT,
/// on timeout it returns a retryable 503 (the control-plane DROP op is re-dispatched and resumes idempotently). The blocking task
/// itself is not cancellable, but the tombstone is already committed, so a retry immediately takes the idempotent path and returns.
const DROP_TOTAL_TIMEOUT: Duration = Duration::from_secs(15);

async fn drop_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    drop_impl(state, request, crate::paths::ResourceKind::D1, "/dbs/").await
}

/// T37r: queue deletion uses a separate namespace endpoint. `/dbs/{id}/drop` only touches the D1
/// kind and `/queues/{id}/drop` only the Queue kind -- after namespace separation a cross-
/// kind drop would tombstone the wrong namespace and leak the other side's db and objects
/// (or even delete a real D1 db with the same id). The control-plane queue DROP op's payload
/// carries {"kind":"queue"}, and cell-controller routes to this endpoint accordingly.
async fn queue_drop_handler(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> Response {
    drop_impl(
        state,
        request,
        crate::paths::ResourceKind::Queue,
        "/queues/",
    )
    .await
}

async fn drop_impl(
    state: AppState,
    request: axum::extract::Request,
    kind: crate::paths::ResourceKind,
    prefix: &'static str,
) -> Response {
    let started = Instant::now();
    let fail = |outcome: &'static str, status: StatusCode, body: serde_json::Value| {
        crate::metrics::query("drop", outcome, started.elapsed().as_secs_f64());
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "rate_limited",
                StatusCode::TOO_MANY_REQUESTS,
                json!({"success": false, "error": "vrt-sql overloaded"}),
            )
        }
    };
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            );
        }
    }
    let db_id = request
        .uri()
        .path()
        .strip_prefix(prefix)
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            )
        }
    };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return fail(
                "unauthorized",
                StatusCode::UNAUTHORIZED,
                json!({"error": "unauthorized"}),
            );
        }
    }
    let registry = state.registry.clone();
    let outcome = tokio::time::timeout(DROP_TOTAL_TIMEOUT, async move {
        tokio::task::spawn_blocking(move || registry.drop_db(kind, &db_id)).await
    })
    .await;
    match outcome {
        Ok(Ok(Ok(dropped))) => {
            crate::metrics::query("drop", "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"success": true, "dropped": dropped}).to_string(),
                ))
                .unwrap()
        }
        Ok(Ok(Err(e))) => fail(
            "drop_error",
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"success": false, "error": e.wire_message()}),
        ),
        Ok(Err(_join)) => fail(
            "drop_error",
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"success": false, "error": "drop task failed"}),
        ),
        Err(_elapsed) => fail(
            "timeout",
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"success": false, "error": "drop timed out; retry"}),
        ),
    }
}

/// Bounded stats request from the cell controller (periodic report).
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatsRequest {
    #[serde(default)]
    d1: Vec<String>,
    #[serde(default)]
    queues: Vec<String>,
}

const STATS_MAX_IDS: usize = 500;

/// `POST /internal/stats` -- read-only size/shape for the D1 databases and
/// queue metrics the control plane asked about. Same ServiceAuth model and
/// body cap as the D1 endpoints. Per-resource failures are typed inside the
/// response; one broken database never hides the rest.
async fn stats_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let label = "stats";
    let fail = |outcome: &'static str, status: StatusCode, body: serde_json::Value| {
        crate::metrics::query(label, outcome, started.elapsed().as_secs_f64());
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "rate_limited",
                StatusCode::TOO_MANY_REQUESTS,
                json!({"success": false, "error": "vrt-sql overloaded"}),
            )
        }
    };
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"success": false, "error": "request body too large"}),
            );
        }
    }
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"success": false, "error": "request body too large"}),
            )
        }
    };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return fail(
                "unauthorized",
                StatusCode::UNAUTHORIZED,
                json!({"success": false, "error": "unauthorized"}),
            );
        }
    }
    let req: StatsRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::BAD_REQUEST,
                json!({"success": false, "error": "invalid stats request"}),
            )
        }
    };
    if req.d1.len() > STATS_MAX_IDS || req.queues.len() > STATS_MAX_IDS {
        return fail(
            "protocol_error",
            StatusCode::BAD_REQUEST,
            json!({"success": false, "error": "too many ids"}),
        );
    }
    let mut databases = Vec::with_capacity(req.d1.len());
    if !req.d1.is_empty() {
        let registry = state.registry.clone();
        let ids = req.d1.clone();
        let joined = tokio::task::spawn_blocking(move || {
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                if crate::paths::validate_database_id(&id).is_err() {
                    out.push(json!({"databaseId": id, "error": {"code": "VRT_INVALID_REQUEST"}}));
                    continue;
                }
                match registry.get(crate::paths::ResourceKind::D1, &id) {
                    Ok(engine) => match engine.stats_sync() {
                        Ok(s) => out.push(json!({"databaseId": id, "stats": s})),
                        Err(e) => out.push(json!({"databaseId": id, "error": error_body(&e)})),
                    },
                    Err(e) => out.push(json!({"databaseId": id, "error": error_body(&e)})),
                }
            }
            out
        })
        .await;
        match joined {
            Ok(v) => databases = v,
            Err(_) => {
                return fail(
                    "error",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({"success": false, "error": "stats task failed"}),
                )
            }
        }
    }
    let mut queues = Vec::with_capacity(req.queues.len());
    let backend = crate::queue::SqliteQueueBackend::new(state.registry.clone());
    for id in req.queues {
        let queue = match crate::queue::QueueId::parse(&id) {
            Ok(q) => q,
            Err(e) => {
                queues.push(json!({
                    "queueId": id,
                    "error": {"code": "VRT_INVALID_REQUEST", "error": e.to_string()},
                }));
                continue;
            }
        };
        match backend.metrics(&queue, now_ms()).await {
            Ok(m) => queues.push(json!({"queueId": id, "metrics": m})),
            Err(e) => queues.push(json!({
                "queueId": id,
                "error": {"code": e.code(), "error": e.to_string()},
            })),
        }
    }
    crate::metrics::query(label, "ok", started.elapsed().as_secs_f64());
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(
            json!({"success": true, "databases": databases, "queues": queues}).to_string(),
        ))
        .unwrap()
}

/// ---------- K2 streams internal wire. Same ServiceAuth + body-cap + op-from-path
/// model as `/queues/*`; streamId is the CF `^[0-9a-f]{32}$` shape, validated
/// at the entry by `StreamId::parse` before any state is touched.
///
/// `/streams/{id}/drop` deletes only the K2 namespace (kind-scoped tombstone).
const K2_BODY_MAX: usize = crate::k2::BATCH_MAX_BYTES * 2 + crate::k2::HEADERS_MAX * 2_048 + 256;

fn k2_fail(
    op: &'static str,
    outcome: &'static str,
    started: Instant,
    e: &crate::k2::K2Error,
) -> Response {
    crate::metrics::query(op, outcome, started.elapsed().as_secs_f64());
    Response::builder()
        .status(e.status())
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "success": false,
                "error": e.to_string(),
                "code": e.code(),
                "retryable": e.retryable(),
            })
            .to_string(),
        ))
        .unwrap()
}

fn k2_bad(
    op: &'static str,
    outcome: &'static str,
    started: Instant,
    status: StatusCode,
    msg: &str,
) -> Response {
    crate::metrics::query(op, outcome, started.elapsed().as_secs_f64());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"success": false, "error": msg, "code": "VRT_K2_INVALID",
                   "retryable": false})
            .to_string(),
        ))
        .unwrap()
}

/// Headers arrive as a JSON object `map<string,string>`; the storage layer
/// keeps them opaque, so the shape + count/byte caps are enforced here.
fn k2_parse_headers(v: Option<&serde_json::Value>) -> Result<String, crate::k2::K2Error> {
    use crate::k2::K2Error;
    match v {
        None | Some(serde_json::Value::Null) => Ok("{}".into()),
        Some(serde_json::Value::Object(map)) => {
            if map.len() > crate::k2::HEADERS_MAX {
                return Err(K2Error::InvalidInput(format!(
                    "headers: max {} entries",
                    crate::k2::HEADERS_MAX
                )));
            }
            for (k, val) in map {
                let Some(s) = val.as_str() else {
                    return Err(K2Error::InvalidInput(
                        "headers values must be strings".into(),
                    ));
                };
                if k.len() + s.len() > crate::k2::HEADER_KV_MAX_BYTES {
                    return Err(K2Error::InvalidInput("headers entry too large".into()));
                }
            }
            Ok(serde_json::to_string(map)
                .map_err(|e| K2Error::InvalidInput(format!("headers json: {e}")))?)
        }
        _ => Err(K2Error::InvalidInput("headers must be an object".into())),
    }
}

fn k2_record_json(r: &crate::k2::ConsumedRecord) -> serde_json::Value {
    json!({
        "seq": r.seq,
        "timestampMs": r.timestamp_ms,
        "content": hex::encode(&r.content),
        "headers": serde_json::from_str::<serde_json::Value>(&r.headers)
            .unwrap_or_else(|_| json!({})),
    })
}

async fn k2_drop_handler(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> Response {
    drop_impl(state, request, crate::paths::ResourceKind::K2, "/streams/").await
}

async fn k2_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let op: &'static str = match request.uri().path().rsplit('/').next() {
        Some("produce") => "produce",
        Some("consume") => "consume",
        Some("ack") => "ack",
        Some("nack") => "nack",
        Some("extend") => "extend",
        Some("subscribe") => "subscribe",
        Some("subscriptions") => "subscriptions",
        Some("unsubscribe") => "unsubscribe",
        Some("configure") => "configure",
        Some("config") => "config",
        Some("reap") => "reap",
        _ => "bad_op",
    };
    let label: &'static str = op;
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return k2_bad(
                label,
                "rate_limited",
                started,
                StatusCode::TOO_MANY_REQUESTS,
                "vrt-sql overloaded",
            )
        }
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return k2_bad(
                label,
                "unavailable",
                started,
                StatusCode::SERVICE_UNAVAILABLE,
                "vrt-sql draining",
            )
        }
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > K2_BODY_MAX.min(state.body_max))
    {
        return k2_bad(
            label,
            "protocol_error",
            started,
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large",
        );
    }
    let id_raw = request
        .uri()
        .path()
        .strip_prefix("/streams/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body =
        match axum::body::to_bytes(request.into_body(), K2_BODY_MAX.min(state.body_max)).await {
            Ok(b) => b,
            Err(_) => {
                return k2_bad(
                    label,
                    "protocol_error",
                    started,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body too large",
                )
            }
        };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return k2_bad(
                label,
                "unauthorized",
                started,
                StatusCode::UNAUTHORIZED,
                "unauthorized",
            );
        }
    }
    let stream = match crate::k2::StreamId::parse(&id_raw) {
        Ok(s) => s,
        Err(e) => return k2_fail(label, "invalid_id", started, &e),
    };
    let backend = crate::k2::SqliteK2Backend::new(state.registry.clone());
    let body_json: serde_json::Value = if body.is_empty() {
        serde_json::Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return k2_fail(
                    label,
                    "invalid_body",
                    started,
                    &crate::k2::K2Error::InvalidInput(format!("body json: {e}")),
                )
            }
        }
    };
    let need = |key: &str| -> Result<serde_json::Value, crate::k2::K2Error> {
        body_json
            .get(key)
            .cloned()
            .ok_or_else(|| crate::k2::K2Error::InvalidInput(format!("missing {key}")))
    };
    let need_str = |key: &str| -> Result<String, crate::k2::K2Error> {
        need(key)?
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| crate::k2::K2Error::InvalidInput(format!("{key} must be a string")))
    };
    let result: Result<serde_json::Value, crate::k2::K2Error> = (|| async {
        match op {
            "produce" => {
                let arr = need("records")?;
                let arr = arr.as_array().ok_or_else(|| {
                    crate::k2::K2Error::InvalidInput("records must be an array".into())
                })?;
                let mut records = Vec::with_capacity(arr.len());
                for r in arr {
                    let content_hex =
                        r.get("content").and_then(|c| c.as_str()).ok_or_else(|| {
                            crate::k2::K2Error::InvalidInput("record content must be hex".into())
                        })?;
                    let content = hex::decode(content_hex).map_err(|_| {
                        crate::k2::K2Error::InvalidInput("record content must be hex".into())
                    })?;
                    records.push(crate::k2::K2Record {
                        content,
                        headers: k2_parse_headers(r.get("headers"))?,
                    });
                }
                let n = backend.produce(&stream, &records).await?;
                Ok(json!({"success": true, "produced": n}))
            }
            "consume" => {
                let sub_id = need_str("subscriptionId")?;
                let worker_id = need_str("workerId")?;
                let max = body_json
                    .get("maxRecords")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(100);
                let max = u32::try_from(max).unwrap_or(u32::MAX);
                let batch = backend.consume(&stream, &sub_id, &worker_id, max).await?;
                let records: Vec<_> = batch.records.iter().map(k2_record_json).collect();
                Ok(json!({
                    "success": true,
                    "batch": {
                        "batchId": batch.batch_id,
                        "leasedUntilMs": batch.leased_until_ms,
                        "records": records,
                    }
                }))
            }
            "ack" => {
                let sub_id = need_str("subscriptionId")?;
                let batch_id = need_str("batchId")?;
                backend.ack(&stream, &sub_id, &batch_id).await?;
                Ok(json!({"success": true}))
            }
            "nack" => {
                let sub_id = need_str("subscriptionId")?;
                let batch_id = need_str("batchId")?;
                backend.nack(&stream, &sub_id, &batch_id).await?;
                Ok(json!({"success": true}))
            }
            "extend" => {
                let sub_id = need_str("subscriptionId")?;
                let batch_id = need_str("batchId")?;
                let worker_id = need_str("workerId")?;
                let until = backend
                    .extend(&stream, &sub_id, &batch_id, &worker_id)
                    .await?;
                Ok(json!({"success": true, "leasedUntilMs": until}))
            }
            "subscribe" => {
                let name = need_str("name")?;
                let start_at = need_str("startAt")?;
                let sub = backend
                    .create_subscription(&stream, &name, &start_at)
                    .await?;
                Ok(json!({"success": true, "subscription": sub}))
            }
            "subscriptions" => {
                let subs = backend.list_subscriptions(&stream).await?;
                Ok(json!({"success": true, "subscriptions": subs}))
            }
            "unsubscribe" => {
                let sub_id = need_str("subscriptionId")?;
                let deleted = backend.delete_subscription(&stream, &sub_id).await?;
                Ok(json!({"success": true, "deleted": deleted}))
            }
            "configure" => {
                let name = need_str("name")?;
                let retention_s = need("retentionSeconds")?.as_i64().ok_or_else(|| {
                    crate::k2::K2Error::InvalidInput("retentionSeconds must be an integer".into())
                })?;
                backend
                    .configure(&stream, &name, retention_s.saturating_mul(1_000))
                    .await?;
                Ok(json!({"success": true}))
            }
            "config" => {
                let cfg = backend.stream_config(&stream).await?;
                let out = cfg.map(|(name, retention_ms)| {
                    json!({"name": name, "retentionSeconds": retention_ms / 1_000})
                });
                Ok(json!({"success": true, "config": out}))
            }
            "reap" => {
                let (records, coverage) = backend.reap(&stream).await?;
                Ok(json!({"success": true, "records": records, "coverage": coverage}))
            }
            _ => Err(crate::k2::K2Error::InvalidInput(format!("unknown op {op}"))),
        }
    })()
    .await;
    match result {
        Ok(v) => {
            crate::metrics::query(label, "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(v.to_string()))
                .unwrap()
        }
        Err(e) => k2_fail(label, "op_error", started, &e),
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/dbs/{id}/query", post(query_handler))
        .route("/dbs/{id}/execute", post(query_handler))
        .route("/dbs/{id}/dump", post(dump_handler))
        .route("/dbs/{id}/drop", post(drop_handler))
        .route("/queues/{id}/drop", post(queue_drop_handler))
        .route("/streams/{id}/drop", post(k2_drop_handler))
        .route("/streams/{id}/produce", post(k2_handler))
        .route("/streams/{id}/consume", post(k2_handler))
        .route("/streams/{id}/ack", post(k2_handler))
        .route("/streams/{id}/nack", post(k2_handler))
        .route("/streams/{id}/extend", post(k2_handler))
        .route("/streams/{id}/subscribe", post(k2_handler))
        .route("/streams/{id}/subscriptions", post(k2_handler))
        .route("/streams/{id}/unsubscribe", post(k2_handler))
        .route("/streams/{id}/configure", post(k2_handler))
        .route("/streams/{id}/config", post(k2_handler))
        .route("/streams/{id}/reap", post(k2_handler))
        .route("/queues/{id}/send", post(queue_handler))
        .route("/queues/{id}/claim", post(queue_handler))
        .route("/queues/{id}/ack", post(queue_handler))
        .route("/queues/{id}/retry", post(queue_handler))
        // T37b-r6: the release handler/op matcher was already in place (r5),
        // only the route was never mounted -> axum 404 -> client Transport -- fixed here.
        .route("/queues/{id}/release", post(queue_handler))
        .route("/queues/{id}/dead-letter", post(queue_handler))
        .route(
            "/queues/{id}/metrics",
            axum::routing::get(queue_metrics_handler),
        )
        .route("/internal/dbs", axum::routing::get(diag_handler))
        .route("/internal/stats", post(stats_handler))
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(healthz))
        .with_state(state)
}

/// ---------- T37: Queues internal wire (section 8). Same ServiceAuth model as the
/// D1 endpoints; queueId is validated at the entry per the section 7 databaseId rule.
/// `/dbs/{id}/drop` only deletes the D1 namespace; queue deletion goes through `/queues/{id}/drop`
/// (T37r -- cell-controller picks the path by op payload.kind).

/// Queue request body cap (T37r refinement, applies only to /queues/* routes -- the D1
/// endpoint cap is unaffected by the enlargement): exactly covers "the largest legal batch + JSON overhead".
///   hex(BATCH_MAX_BYTES=288_000)            = 576_000
///   per-message JSON skeleton <=256B x 100 (id 128 + key names/quotes/numbers)
///   + envelope ({"messages":[...]} + commas)   =    +256
/// Anything over is 413 -- before JSON parsing (content-length precheck +
/// to_bytes length limit).
const QUEUE_BODY_MAX: usize =
    crate::queue::BATCH_MAX_BYTES * 2 + crate::queue::BATCH_MAX_MSGS as usize * 256 + 256;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn queue_fail(
    op: &'static str,
    outcome: &'static str,
    started: Instant,
    e: &crate::queue::QueueError,
) -> Response {
    crate::metrics::query(op, outcome, started.elapsed().as_secs_f64());
    Response::builder()
        .status(e.status())
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "success": false,
                "error": e.to_string(),
                "code": e.code(),
                "retryable": e.retryable(),
            })
            .to_string(),
        ))
        .unwrap()
}

fn queue_bad(
    op: &'static str,
    outcome: &'static str,
    started: Instant,
    status: StatusCode,
    msg: &str,
) -> Response {
    crate::metrics::query(op, outcome, started.elapsed().as_secs_f64());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"success": false, "error": msg, "code": "VRT_QUEUE_INVALID",
                   "retryable": false})
            .to_string(),
        ))
        .unwrap()
}

async fn queue_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let op: &'static str = match request.uri().path().rsplit('/').next() {
        Some("send") => "send",
        Some("claim") => "claim",
        Some("ack") => "ack",
        Some("retry") => "retry",
        Some("release") => "release",
        Some("dead-letter") => "dead-letter",
        _ => "bad_op",
    };
    let label: &'static str = op;
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return queue_bad(
                label,
                "rate_limited",
                started,
                StatusCode::TOO_MANY_REQUESTS,
                "vrt-sql overloaded",
            )
        }
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return queue_bad(
                label,
                "unavailable",
                started,
                StatusCode::SERVICE_UNAVAILABLE,
                "vrt-sql draining",
            )
        }
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > QUEUE_BODY_MAX.min(state.body_max))
    {
        return queue_bad(
            label,
            "protocol_error",
            started,
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large",
        );
    }
    let id_raw = request
        .uri()
        .path()
        .strip_prefix("/queues/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body =
        match axum::body::to_bytes(request.into_body(), QUEUE_BODY_MAX.min(state.body_max)).await {
            Ok(b) => b,
            Err(_) => {
                return queue_bad(
                    label,
                    "protocol_error",
                    started,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body too large",
                )
            }
        };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return queue_bad(
                label,
                "unauthorized",
                started,
                StatusCode::UNAUTHORIZED,
                "unauthorized",
            );
        }
    }
    let queue = match crate::queue::QueueId::parse(&id_raw) {
        Ok(q) => q,
        Err(e) => return queue_fail(label, "invalid_id", started, &e),
    };
    let backend = crate::queue::SqliteQueueBackend::new(state.registry.clone());
    let now = now_ms();
    let result: Result<serde_json::Value, crate::queue::QueueError> = (|| async {
        match op {
            "send" => {
                let req: crate::queue::SendRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let mut msgs = Vec::with_capacity(req.messages.len());
                for m in req.messages {
                    let body_bytes = hex::decode(&m.body).map_err(|_| {
                        crate::queue::QueueError::InvalidInput("message body must be hex".into())
                    })?;
                    msgs.push(crate::queue::IncomingMsg {
                        id: m.id,
                        body: body_bytes,
                        content_type: m.content_type,
                        delay_secs: m.delay_secs,
                        timestamp_ms: m.timestamp_ms,
                    });
                }
                let r = backend.send(&queue, &msgs, now).await?;
                Ok(json!({
                    "success": true,
                    "enqueued": r.inserted,
                    "duplicates": r.duplicates,
                    "metrics": r.metrics,
                }))
            }
            "claim" => {
                let req: crate::queue::ClaimRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let batch = backend
                    .claim(
                        &queue,
                        &req.holder_id,
                        req.holder_ttl_ms,
                        req.max,
                        req.max_bytes,
                        req.lease_ttl_ms,
                        now,
                    )
                    .await?;
                let msgs: Vec<serde_json::Value> = batch
                    .messages
                    .iter()
                    .map(|m| {
                        json!({
                            "id": m.id,
                            "leaseId": m.lease_id,
                            "attempts": m.attempts,
                            "body": hex::encode(&m.body),
                            "contentType": m.content_type,
                            "timestampMs": m.timestamp_ms,
                        })
                    })
                    .collect();
                Ok(json!({"success": true, "leaseId": batch.lease_id,
                          "dispatchEpoch": batch.dispatch_epoch, "messages": msgs}))
            }
            "ack" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let n = backend.ack(&queue, &req.lease_id, &req.ids, now).await?;
                Ok(json!({"success": true, "acked": n}))
            }
            "retry" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let items: Vec<(String, u32)> = req
                    .items
                    .iter()
                    .map(|i| (i.id.clone(), i.delay_secs))
                    .collect();
                let n = backend.retry(&queue, &req.lease_id, &items, now).await?;
                Ok(json!({"success": true, "retried": n}))
            }
            "release" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let items: Vec<(String, u32)> = req
                    .items
                    .iter()
                    .map(|i| (i.id.clone(), i.delay_secs))
                    .collect();
                let n = backend.release(&queue, &req.lease_id, &items, now).await?;
                Ok(json!({"success": true, "released": n}))
            }
            "dead-letter" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let n = backend
                    .dead_letter(
                        &queue,
                        &req.lease_id,
                        &req.ids,
                        req.reason.as_deref().unwrap_or("dead-letter"),
                        now,
                    )
                    .await?;
                Ok(json!({"success": true, "deadLettered": n}))
            }
            _ => Err(crate::queue::QueueError::InvalidInput(format!(
                "unsupported queue op {op:?}"
            ))),
        }
    })()
    .await;
    match result {
        Ok(body) => {
            crate::metrics::query(label, "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        }
        Err(e) => queue_fail(
            label,
            if e.retryable() {
                "unavailable"
            } else {
                "error"
            },
            started,
            &e,
        ),
    }
}

/// `GET /queues/{id}/metrics` -- same ServiceAuth (GET + empty body).
async fn queue_metrics_handler(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> Response {
    let started = Instant::now();
    let label = "queue_metrics";
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return queue_bad(
                label,
                "rate_limited",
                started,
                StatusCode::TOO_MANY_REQUESTS,
                "vrt-sql overloaded",
            )
        }
    };
    let id_raw = request
        .uri()
        .path()
        .strip_prefix("/queues/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, b"") {
            return queue_bad(
                label,
                "unauthorized",
                started,
                StatusCode::UNAUTHORIZED,
                "unauthorized",
            );
        }
    }
    let queue = match crate::queue::QueueId::parse(&id_raw) {
        Ok(q) => q,
        Err(e) => return queue_fail(label, "invalid_id", started, &e),
    };
    let backend = crate::queue::SqliteQueueBackend::new(state.registry.clone());
    match backend.metrics(&queue, now_ms()).await {
        Ok(m) => {
            crate::metrics::query(label, "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::from(
                    json!({"success": true, "metrics": m}).to_string(),
                ))
                .unwrap()
        }
        Err(e) => queue_fail(
            label,
            if e.retryable() {
                "unavailable"
            } else {
                "error"
            },
            started,
            &e,
        ),
    }
}
