//! `vrt serve` management API — axum over a unix socket (0600 dir,
//! token-authenticated). Every handler is a thin adapter onto the
//! typed [`VrtLocal`] ops; input validation lives in the library, the
//! wire layer only enforces shape, body cap, token and timeout.
//!
//! An OPTIONAL TCP listener (`--mgmt-tcp-listen`) shares the same
//! router under a SEPARATE bearer token and a per-source-IP rate
//! limit — it exists for remote `vrt deploy --url` and is off by
//! default. The unix socket's security level is never lowered to
//! accommodate it.

use crate::layout::mgmt_dir;
use crate::{keys, worker_pkg, LocalError, VrtLocal};
use axum::{
    extract::{DefaultBodyLimit, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;

/// Max JSON body (deploy/rollback/secret payloads are tiny).
const MAX_BODY: usize = 64 * 1024;
/// Per-request deadline — library ops are sub-second; deploy packing
/// is bounded by bundle size caps.
const REQ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Shared API state — `renewal` is what `serve` reports on /healthz.
/// The bearer token is NOT here: it lives in the auth middleware
/// state so the unix and TCP listeners can hold different tokens on
/// the same routes.
pub struct Api {
    root: PathBuf,
    renewal: std::sync::RwLock<RenewalView>,
}

/// Last renewal outcome, surfaced on /healthz.
#[derive(Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RenewalView {
    /// Last successful renewal revision.
    pub last_revision: Option<u64>,
    /// Last failure message (typed, no secrets).
    pub last_error: Option<String>,
    /// Failure streak; >0 while the envelope approaches expiry.
    pub consecutive_failures: u64,
}

/// Management API socket path.
pub fn socket_path(root: &std::path::Path) -> PathBuf {
    mgmt_dir(root).join("vrt.sock")
}

/// Max body on `/v1/deploy/upload` — the wire cap, not a content
/// cap: module bytes (96MB) + assets (8MB) base64-inflate ~4/3, and
/// `assemble` re-applies the real content caps after decode.
const UPLOAD_MAX_BODY: usize = 200 * 1024 * 1024;

/// Build the unix-socket router (bind/listen lives in `serve` so
/// tests can attach their own listener).
/// Returns the router plus the shared [`Api`] so `serve`'s renewal
/// loop can record outcomes onto the same /healthz view.
pub fn router(root: PathBuf) -> Result<(Router, Arc<Api>), LocalError> {
    let token = keys::load_mgmt_token(&root)?;
    let api = Arc::new(Api {
        root,
        renewal: std::sync::RwLock::new(RenewalView::default()),
    });
    Ok((
        routes(api.clone()).layer(middleware::from_fn_with_state(token, auth)),
        api,
    ))
}

/// The TCP management listener's router — the same routes on the
/// SHARED [`Api`] (so /healthz reports the real renewal view), under
/// a different bearer token plus a per-source-IP rate limit. Never
/// mounted unless `serve` was explicitly told to listen on TCP.
pub fn tcp_router(api: Arc<Api>, token: Vec<u8>, limiter: Arc<RateLimit>) -> Router {
    routes(api)
        .layer(middleware::from_fn_with_state(token, auth))
        .layer(middleware::from_fn_with_state(limiter, rate_limit))
}

/// Route table shared by both listeners — body caps included.
fn routes(api: Arc<Api>) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/versions", get(versions))
        .route("/v1/deploy", post(deploy))
        // A2: remote deploy — the packed bundle + meta + secrets
        // travel in one body; the route-level body limit overrides
        // the global 64KiB below (innermost DefaultBodyLimit wins).
        .route(
            "/v1/deploy/upload",
            post(deploy_upload)
                .layer(DefaultBodyLimit::max(UPLOAD_MAX_BODY))
                // Declared-oversize bodies die before a byte is read —
                // DefaultBodyLimit alone still buffers up to the cap.
                .route_layer(middleware::from_fn(content_length_gate)),
        )
        .route("/v1/rollback", post(rollback))
        .route("/v1/hostnames", get(hostnames_list).post(hostname_set))
        .route("/v1/secrets", get(secrets_list).put(secret_put))
        .route("/v1/secrets/{secret_ref}", delete(secret_rm))
        .route("/v1/renew", post(renew))
        .route("/healthz", get(healthz))
        // T37b-r: dispatcher metrics live in THIS process's
        // cell-controller registry (standalone runs the
        // dispatcher in-process) — host /metrics can't see them.
        .route("/v1/metrics", get(metrics_text))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(api)
}

/// Bearer check — constant-time, the token arrives as middleware
/// state so the same `auth` serves both listeners under different
/// credentials.
async fn auth(
    State(token): State<Vec<u8>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let ok = bearer.is_some_and(|t| {
        t.len() == token.len()
            && subtle::ConstantTimeEq::ct_eq(t.as_bytes(), token.as_slice()).into()
    });
    if !ok {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

/// Early reject for declared-oversize uploads (Content-Length >
/// UPLOAD_MAX_BODY → 413 without reading the body).
async fn content_length_gate(req: Request, next: Next) -> Result<Response, StatusCode> {
    let declared = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n as usize > UPLOAD_MAX_BODY) {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    Ok(next.run(req).await)
}

/// Per-source-IP rate limiting for the TCP listener only — a fixed
/// 60s window; the unix socket is owner-only and needs none.
pub struct RateLimit {
    inner:
        std::sync::Mutex<std::collections::BTreeMap<std::net::IpAddr, (std::time::Instant, u32)>>,
}

impl RateLimit {
    /// Requests per source IP per 60s window.
    pub const MAX_PER_MIN: u32 = 60;
    /// New empty limiter.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        })
    }
    fn check(&self, ip: std::net::IpAddr) -> bool {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        let entry = g.entry(ip).or_insert((now, 0));
        if now.duration_since(entry.0) >= std::time::Duration::from_secs(60) {
            *entry = (now, 0);
        }
        entry.1 += 1;
        entry.1 <= Self::MAX_PER_MIN
    }
}

async fn rate_limit(
    State(limiter): State<Arc<RateLimit>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // ConnectInfo is guaranteed by `into_make_service_with_connect_info`
    // on the TCP listener; absent it (tests calling the router
    // directly) the limiter simply does not engage.
    if let Some(axum::extract::ConnectInfo(addr)) = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        if !limiter.check(addr.ip()) {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }
    Ok(next.run(req).await)
}

/// `LocalError` → stable wire shape `{error:{code,field}}`.
fn fail(err: LocalError) -> Response {
    let (status, code, field) = match &err {
        LocalError::Locked => (StatusCode::CONFLICT, "locked", None),
        LocalError::NotFound(f) => (StatusCode::NOT_FOUND, "notFound", Some(*f)),
        LocalError::WrongTenant => (StatusCode::FORBIDDEN, "wrongTenant", None),
        LocalError::Worker(f) => (StatusCode::BAD_REQUEST, "worker", Some(*f)),
        LocalError::WorkerOwned(f) => {
            return err_body(StatusCode::BAD_REQUEST, "worker", Some(f.clone()))
        }
        LocalError::Desired(f) => (StatusCode::BAD_REQUEST, "desired", Some(*f)),
        LocalError::State(f) => (StatusCode::BAD_REQUEST, "state", Some(*f)),
        LocalError::Bundle(e) => {
            return err_body(StatusCode::BAD_REQUEST, "bundle", Some(e.to_string()))
        }
        LocalError::Layout(f) => (StatusCode::INTERNAL_SERVER_ERROR, "layout", Some(*f)),
        LocalError::Protocol(_) | LocalError::Io(_) | LocalError::Op(_, _) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal", None)
        }
    };
    err_body(status, code, field.map(str::to_string))
}

fn err_body(status: StatusCode, code: &'static str, field: Option<String>) -> Response {
    (
        status,
        Json(serde_json::json!({"error": {"code": code, "field": field}})),
    )
        .into_response()
}

/// Run a sync `VrtLocal` op off the reactor thread with a deadline.
async fn op<T, F>(f: F) -> Result<T, LocalError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, LocalError> + Send + 'static,
{
    match tokio::time::timeout(REQ_TIMEOUT, tokio::task::spawn_blocking(f)).await {
        Ok(Ok(r)) => r,
        Ok(Err(_join)) => Err(LocalError::Io(std::io::Error::other("join"))),
        Err(_elapsed) => Err(LocalError::Locked),
    }
}

fn local(root: &std::path::Path) -> Result<VrtLocal, LocalError> {
    VrtLocal::open(root)
}

async fn status(State(api): State<Arc<Api>>) -> Result<Json<serde_json::Value>, Response> {
    let root = api.root.clone();
    let st = op(move || local(&root)?.status()).await.map_err(fail)?;
    let mut value = serde_json::to_value(&st)
        .map_err(|_| err_body(StatusCode::INTERNAL_SERVER_ERROR, "internal", None))?;
    // G1: cron state is observable — disabled is a typed field, not a
    // one-shot log line. Written by `cron_loop` (serve.rs).
    let cron = std::fs::read(crate::layout::state_dir(&api.root).join("cron-status.json"))
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .unwrap_or_else(|| serde_json::json!({ "enabled": false, "reason": "not running" }));
    if let Some(obj) = value.as_object_mut() {
        obj.insert("cron".to_string(), cron);
    }
    Ok(Json(value))
}

#[derive(Deserialize)]
struct WorkerQuery {
    worker: String,
}

async fn versions(
    State(api): State<Arc<Api>>,
    Query(q): Query<WorkerQuery>,
) -> Result<Json<serde_json::Value>, Response> {
    let root = api.root.clone();
    let st = op(move || local(&root)?.status()).await.map_err(fail)?;
    let w = st
        .workers
        .iter()
        .find(|w| w.name == q.worker || w.worker_id == q.worker)
        .ok_or_else(|| fail(LocalError::NotFound("worker")))?;
    Ok(Json(serde_json::json!({
        "worker": w.name, "workerId": w.worker_id,
        "activeVersionId": w.active_version_id, "versions": w.versions,
    })))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeployBody {
    worker_dir: String,
    /// Optional worker name override (`vrt.toml name` otherwise).
    name: Option<String>,
}

async fn deploy(
    State(api): State<Arc<Api>>,
    body: Result<Json<DeployBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<serde_json::Value>, Response> {
    let Json(b) = body.map_err(|_| err_body(StatusCode::BAD_REQUEST, "body", None))?;
    // The worker dir is a LOCAL path resolved server-side — restrict
    // to absolute paths under an existing directory; traversal is
    // impossible by construction (canonicalize + must-be-dir).
    let dir = std::path::Path::new(&b.worker_dir)
        .canonicalize()
        .map_err(|_| fail(LocalError::NotFound("workerDir")))?;
    if !dir.is_dir() {
        return Err(fail(LocalError::NotFound("workerDir")));
    }
    let root = api.root.clone();
    let name = b.name.clone();
    let r = op(move || {
        local(&root)?.deploy(
            &dir,
            &worker_pkg::DeployOverrides {
                name,
                ..Default::default()
            },
        )
    })
    .await
    .map_err(fail)?;
    serde_json::to_value(&r)
        .map(Json)
        .map_err(|_| err_body(StatusCode::INTERNAL_SERVER_ERROR, "internal", None))
}

/// A2: remote deploy — one JSON body carries meta + module bytes +
/// `[assets]` objects + secret values. The rejection status is kept
/// (413 for over-cap bodies) but its detail is NOT: secrets ride in
/// the body and error text never echoes it.
async fn deploy_upload(
    State(api): State<Arc<Api>>,
    body: Result<Json<worker_pkg::DeployUpload>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<serde_json::Value>, Response> {
    let Json(b) = body.map_err(|r| err_body(r.status(), "body", None))?;
    let root = api.root.clone();
    let r = op(move || local(&root)?.deploy_upload(&b))
        .await
        .map_err(fail)?;
    serde_json::to_value(&r)
        .map(Json)
        .map_err(|_| err_body(StatusCode::INTERNAL_SERVER_ERROR, "internal", None))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RollbackBody {
    worker: String,
    version_id: String,
}

async fn rollback(
    State(api): State<Arc<Api>>,
    body: Result<Json<RollbackBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<serde_json::Value>, Response> {
    let Json(b) = body.map_err(|_| err_body(StatusCode::BAD_REQUEST, "body", None))?;
    let root = api.root.clone();
    let revision = op(move || local(&root)?.rollback(&b.worker, &b.version_id))
        .await
        .map_err(fail)?;
    Ok(Json(serde_json::json!({"revision": revision})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HostnameBody {
    worker: String,
    hostname: String,
    /// `true` add, `false` remove.
    add: bool,
}

async fn hostname_set(
    State(api): State<Arc<Api>>,
    body: Result<Json<HostnameBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<serde_json::Value>, Response> {
    let Json(b) = body.map_err(|_| err_body(StatusCode::BAD_REQUEST, "body", None))?;
    let root = api.root.clone();
    let revision = op(move || {
        let l = local(&root)?;
        if b.add {
            l.set_hostname(&b.worker, &b.hostname)
        } else {
            l.remove_hostname(&b.worker, &b.hostname)
        }
    })
    .await
    .map_err(fail)?;
    Ok(Json(serde_json::json!({"revision": revision})))
}

async fn hostnames_list(
    State(api): State<Arc<Api>>,
    Query(q): Query<WorkerQuery>,
) -> Result<Json<serde_json::Value>, Response> {
    let root = api.root.clone();
    let st = op(move || local(&root)?.status()).await.map_err(fail)?;
    let w = st
        .workers
        .iter()
        .find(|w| w.name == q.worker || w.worker_id == q.worker)
        .ok_or_else(|| fail(LocalError::NotFound("worker")))?;
    Ok(Json(serde_json::json!({"hostnames": w.hostnames})))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SecretBody {
    /// Tenant id — must match the root's single tenant.
    tenant_id: Option<String>,
    secret_ref: String,
    value: String,
}

async fn secret_put(
    State(api): State<Arc<Api>>,
    body: Result<Json<SecretBody>, axum::extract::rejection::JsonRejection>,
) -> Result<StatusCode, Response> {
    let Json(b) = body.map_err(|_| err_body(StatusCode::BAD_REQUEST, "body", None))?;
    let root = api.root.clone();
    op(move || {
        let l = local(&root)?;
        let st = l.status()?;
        let tenant = b.tenant_id.as_deref().unwrap_or(&st.tenant_id);
        l.put_secret(tenant, &b.secret_ref, &b.value)
    })
    .await
    .map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn secret_rm(
    State(api): State<Arc<Api>>,
    axum::extract::Path(secret_ref): axum::extract::Path<String>,
) -> Result<StatusCode, Response> {
    let root = api.root.clone();
    op(move || {
        let l = local(&root)?;
        let st = l.status()?;
        l.delete_secret(&st.tenant_id, &secret_ref)
    })
    .await
    .map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn secrets_list(State(api): State<Arc<Api>>) -> Result<Json<serde_json::Value>, Response> {
    let root = api.root.clone();
    let st = op(move || local(&root)?.status()).await.map_err(fail)?;
    Ok(Json(serde_json::json!({"secretRefs": st.secret_refs})))
}

async fn renew(State(api): State<Arc<Api>>) -> Result<Json<serde_json::Value>, Response> {
    let root = api.root.clone();
    let revision = op(move || local(&root)?.renew()).await.map_err(fail)?;
    Ok(Json(serde_json::json!({"revision": revision})))
}

/// `vrt serve` liveness: envelope fresh + renewal loop healthy.
/// NOT readiness — it reports the control-plane's own health; the
/// host's /readyz is authoritative for serving state.
async fn healthz(State(api): State<Arc<Api>>) -> Response {
    let expiry = crate::desired::read_envelope_expiry_ms(&api.root);
    let now = chrono::Utc::now().timestamp_millis();
    let renewal = api
        .renewal
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    // No envelope → nothing deployed yet → control plane is healthy
    // (there is nothing to keep fresh); with an envelope it must be
    // unexpired AND the renewal loop must not be failing.
    let fresh = expiry.is_none_or(|e| e > now);
    let healthy = fresh && renewal.consecutive_failures == 0;
    let body = serde_json::json!({
        "healthy": healthy,
        "envelope": {
            "expiresAtMs": expiry,
            "remainingMs": expiry.map(|e| e - now),
        },
        "renewal": renewal,
    });
    (
        if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    )
        .into_response()
}

/// Record a renewal outcome for /healthz.
pub fn record_renewal(api: &Arc<Api>, result: &Result<u64, LocalError>) {
    let mut view = api.renewal.write().unwrap_or_else(|e| e.into_inner());
    match result {
        Ok(revision) => {
            view.last_revision = Some(*revision);
            view.last_error = None;
            view.consecutive_failures = 0;
        }
        Err(e) => {
            view.last_error = Some(e.to_string());
            view.consecutive_failures += 1;
        }
    }
}

/// Prometheus text for the in-process dispatcher/registry — the
/// queue counters (`vrt_queue_*`) only exist here in standalone.
async fn metrics_text() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        cell_controller::metrics::registry().render(),
    )
        .into_response()
}
