//! Axum dispatch of the private binding-backend protocol subset
//! (`/internal/bindings/v1/{kv,r2}/...`), mirroring the upstream
//! validation order:
//!
//! `method → declared-size cap → path → identity headers → content-type
//! → authorize → permission → operation`.
//!
//! Tenant identity is bound by the unix listener ([`TenantService`]);
//! authorization goes through [`Authorizer`] against verified desired
//! state — nothing in the request is trusted beyond its format.

use crate::authorize::Authorizer;
use crate::context::{BindingContext, ResourceKind};
use crate::cursor::{CursorInvalid, CursorSigner};
use crate::error::{error_response, ProviderError};
use crate::identity::parse_request_identity;
use crate::kv::{self, KvProvider};
use crate::limits::*;
use crate::r2::{self, R2GetResult, R2Provider};
use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, Response, StatusCode};
use axum::routing::any;
use axum::Router;
use sha2::{Digest, Sha256};
use std::future::Future;
use std::sync::Arc;

fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

/// Per-tenant service state shared by the listener's router.
pub struct TenantService {
    /// Listener-bound tenant identity.
    pub tenant_id: String,
    /// Desired-state authorizer.
    pub authorizer: Arc<Authorizer>,
    /// KV backend (absent → KV ops fail closed as unavailable).
    pub kv: Option<Arc<dyn KvProvider>>,
    /// R2 backend (absent → R2 ops fail closed as unavailable).
    pub r2: Option<Arc<dyn R2Provider>>,
    /// T21: assets store (absent → asset fetches fail closed as
    /// unavailable — the binding is authorized but undeliverable).
    pub assets: Option<crate::assets::AssetStore>,
    /// List-cursor signer.
    pub signer: Arc<CursorSigner>,
    /// R2 single-part object byte cap.
    pub r2_max_object_bytes: u64,
    /// T13/H3 + RV-R8 #2/#3: PER-TENANT in-flight cap (one semaphore
    /// per listener — never shared). Acquire waits at most
    /// [`PERMIT_ACQUIRE_TIMEOUT`] before a typed 429; the permit is
    /// held inside the response body until the last byte is sent.
    pub permits: Arc<tokio::sync::Semaphore>,
    /// T13/H3: global in-flight cap shared across tenants — one
    /// tenant can never starve the whole provider.
    pub global_permits: Arc<tokio::sync::Semaphore>,
    /// T13/H3: hard deadline for draining a request body.
    pub body_read_timeout: std::time::Duration,
    /// T34: vrt-sql downstream (absent -> D1 ops return the {success:false} unavailable surface).
    pub d1: Option<Arc<crate::d1::D1Client>>,
    /// T37: queue downstream (the same vrt-sql as d1; absent -> queue 503).
    pub queue: Option<Arc<crate::queue::QueueClient>>,
    /// K2 downstream (the same vrt-sql; absent -> k2 produce 503).
    pub k2: Option<Arc<crate::k2::K2Client>>,
    /// P1: PG HTTP data plane (absent → PG ops fail closed
    /// `PG_CREDENTIAL_UNAVAILABLE` — authorized but undeliverable).
    pub pg: Option<Arc<crate::pg::PgBackend>>,
}

impl std::fmt::Debug for TenantService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantService")
            .field("tenant_id", &self.tenant_id)
            .finish_non_exhaustive()
    }
}

/// Router for one tenant listener, NO quota (library/test default).
/// Production wiring uses [`tenant_router_with_quota`].
pub fn tenant_router(service: Arc<TenantService>) -> Router {
    tenant_router_with_quota(service, Arc::new(crate::quota::TenantQuota::unlimited()))
}

struct QuotaState {
    svc: Arc<TenantService>,
    quota: Arc<crate::quota::TenantQuota>,
}

/// RV-R67: router with the per-tenant QPS/bytes quota in front of the
/// shared [`dispatch`] — one hook for every binding family. UNCOMPILED.
pub fn tenant_router_with_quota(
    service: Arc<TenantService>,
    quota: Arc<crate::quota::TenantQuota>,
) -> Router {
    Router::new()
        .fallback(any(quota_dispatch))
        .with_state(Arc::new(QuotaState { svc: service, quota }))
}

async fn quota_dispatch(State(st): State<Arc<QuotaState>>, request: Request) -> Response<Body> {
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if let Err(reason) = st.quota.check(std::time::Instant::now(), declared) {
        tracing::warn!(
            tenant = %st.svc.tenant_id,
            reason = reason.as_str(),
            "tenant quota exceeded — refusing request"
        );
        crate::metrics::quota_exceeded(&st.svc.tenant_id, reason.as_str());
        // Wire shape = the family's existing 429/busy envelope (the
        // facade's error-code allowlist only knows those codes).
        let mut resp = permit_denied_response(request.uri().path());
        resp.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return resp;
    }
    dispatch(State(st.svc.clone()), request).await
}

async fn dispatch(State(svc): State<Arc<TenantService>>, request: Request) -> Response<Body> {
    if request.method() != Method::POST {
        return error_response(
            "BINDING_PROTOCOL_ERROR",
            StatusCode::METHOD_NOT_ALLOWED,
            true,
            false,
        );
    }
    // T13/H3 + RV-R8 #2 + RV-R28 #1: bounded concurrency — a saturated
    // provider waits at most PERMIT_ACQUIRE_TIMEOUT (combined across
    // both waits) for slots, then answers a typed 429 instead of
    // absorbing unbounded in-flight bodies. TENANT FIRST, then global:
    // a request queued on its own tenant cap must not pin a global
    // slot — under the old order one tenant's ~448 queued requests
    // starved every other tenant off the shared semaphore.
    let (tenant_permit, global_permit) =
        match acquire_permits(&svc.permits, &svc.global_permits).await {
            Ok(permits) => permits,
            Err(scope) => {
                tracing::warn!(
                    tenant = %svc.tenant_id,
                    scope,
                    "concurrency permit acquire timed out — refusing request"
                );
                return permit_denied_response(request.uri().path());
            }
        };
    // RV-R8 #3: compute the response budget before `svc` moves into
    // the binding dispatch; permits + budget live inside the body.
    let response_budget = dispatch_response_budget(&svc);
    // B12: worker label must be captured before `request` is moved
    // into the binding dispatch (bounded, identifier-validated or
    // `_unknown`). `tenant_id` likewise — the body guard outlives
    // `svc`'s move into the binding dispatch (RV-R28 #2 metrics).
    let worker_id = crate::metrics::worker_label(&request);
    let tenant_id = svc.tenant_id.clone();
    let path = request.uri().path();
    let (binding_type, response) = if path.starts_with("/internal/bindings/v1/kv/") {
        ("kv", kv_dispatch(svc, request).await)
    } else if path.starts_with("/internal/bindings/v1/r2/") {
        ("r2", r2_dispatch(svc, request).await)
    } else if path.starts_with("/internal/bindings/v1/d1/") {
        ("d1", d1_dispatch(svc, request).await)
    } else if path.starts_with("/internal/bindings/v1/queue/") {
        ("queue", queue_dispatch(svc, request).await)
    } else if path.starts_with("/internal/bindings/v1/k2/") {
        ("k2", k2_dispatch(svc, request).await)
    } else if path == "/internal/assets/v1/fetch" {
        ("assets", assets_dispatch(svc, request).await)
    } else if path == "/internal/assets/v1/route-index" || path == "/internal/assets/v1/can-fetch" {
        ("assets", assets_dispatch(svc, request).await)
    } else if path.starts_with("/internal/assets/v1/") {
        (
            "assets",
            error_response("BINDING_PROTOCOL_ERROR", StatusCode::NOT_FOUND, true, false),
        )
    } else if path.starts_with("/internal/bindings/v1/pg/") {
        ("pg", pg_dispatch(svc, request).await)
    } else if path.starts_with("/internal/pg/") {
        // Defense in depth: a miswired caller gets a typed outcome
        // rather than a protocol guess.
        (
            "pg",
            error_response("BINDING_PROTOCOL_ERROR", StatusCode::NOT_FOUND, true, false),
        )
    } else {
        (
            "unknown",
            error_response("BINDING_PROTOCOL_ERROR", StatusCode::NOT_FOUND, true, false),
        )
    };
    // B12: count on the trusted dispatch path; `worker_id` is the
    // identifier-validated auth header (bounded, folds to `_unknown`).
    crate::metrics::subrequest(
        &worker_id,
        binding_type,
        !response.status().is_success() && !response.status().is_redirection(),
    );
    // RV-R8 #3: hold both permits until the response body is fully
    // written, and bound the emitted bytes — a permit freed at
    // dispatch-return would let a slow client pipeline stack
    // unbounded in-flight response bodies.
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(PermitGuardBody::new(
            body,
            response_budget,
            tenant_id,
            global_permit,
            tenant_permit,
        )),
    )
}

/// RV-R8 #2: a permit slot may be waited for at most this long before
/// the request is refused with the typed 429 — queues stay bounded.
const PERMIT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// RV-R8 #3: largest legitimate dispatch response is an R2 get frame —
/// object payload (`r2_max_object_bytes`) plus a bounded header. KV
/// (≤25MiB+meta) and D1/queue envelopes stay far under it.
fn dispatch_response_budget(svc: &TenantService) -> u64 {
    svc.r2_max_object_bytes + R2_MAX_METADATA_BYTES as u64 + 1024
}

/// RV-R28 #1: tenant permit first, then the shared global slot —
/// a request queued on its own tenant cap holds NO global permit, so
/// one saturated tenant can never starve the others. Both waits share
/// a single PERMIT_ACQUIRE_TIMEOUT deadline (total wait stays bounded).
/// `Err(scope)` names the semaphore that timed out.
async fn acquire_permits(
    tenant_sem: &Arc<tokio::sync::Semaphore>,
    global_sem: &Arc<tokio::sync::Semaphore>,
) -> Result<
    (
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    ),
    &'static str,
> {
    let deadline = tokio::time::Instant::now() + PERMIT_ACQUIRE_TIMEOUT;
    let tenant = tokio::time::timeout_at(deadline, tenant_sem.clone().acquire_owned())
        .await
        .map_err(|_| "tenant")?
        .map_err(|_| "tenant")?;
    let global = tokio::time::timeout_at(deadline, global_sem.clone().acquire_owned())
        .await
        .map_err(|_| "global")?
        .map_err(|_| "global")?;
    Ok((tenant, global))
}

/// RV-R28 #2: the refusal on permit exhaustion must carry the
/// semantics of the binding family being called — the facade parses
/// each family differently (D1 reads `success:false` at HTTP 200, the
/// queue reads `{error:…}` with a status, KV/R2/PG/assets read the
/// typed envelope plus `x-open-compute-error-code`). Emitting
/// `KV_BUSY`/`R2_OVERLOADED` on a D1 or queue call made the facade
/// parse the wrong shape.
fn permit_denied_response(path: &str) -> Response<Body> {
    if path.starts_with("/internal/bindings/v1/kv/") {
        error_response("KV_BUSY", StatusCode::TOO_MANY_REQUESTS, true, false)
    } else if path.starts_with("/internal/bindings/v1/r2/") {
        error_response("R2_OVERLOADED", StatusCode::TOO_MANY_REQUESTS, true, false)
    } else if path.starts_with("/internal/bindings/v1/d1/") {
        if path.ends_with("/dump") {
            d1_dump_error("provider busy; retry")
        } else {
            d1_wire_error("provider busy; retry")
        }
    } else if path.starts_with("/internal/bindings/v1/queue/") {
        queue_wire_error(StatusCode::TOO_MANY_REQUESTS, "provider busy; retry")
    } else if path.starts_with("/internal/bindings/v1/k2/") {
        k2_wire_error(
            StatusCode::TOO_MANY_REQUESTS,
            "OVERLOADED",
            "provider busy; retry",
            true,
        )
    } else if path.starts_with("/internal/bindings/v1/pg/") {
        error_response(
            "PG_RATE_LIMITED",
            StatusCode::TOO_MANY_REQUESTS,
            true,
            false,
        )
    } else if path.starts_with("/internal/assets/v1/") {
        // `ASSET_STORAGE_UNAVAILABLE` is on the facade's error-code
        // allowlist — a foreign code would still parse as this one.
        error_response(
            "ASSET_STORAGE_UNAVAILABLE",
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        )
    } else {
        error_response(
            "RESOURCE_UNAVAILABLE",
            StatusCode::TOO_MANY_REQUESTS,
            true,
            false,
        )
    }
}

/// RV-R28 #2: a streamed response body may not stall without bound —
/// a slow or abandoned reader would otherwise pin both permits
/// forever (64 stalled bodies = a tenant-wide 429 outage; ~8 stalled
/// tenants pin the whole global pool). No frame may be outstanding
/// longer than BODY_IDLE_TIMEOUT; the whole send may take at most
/// BODY_SEND_DEADLINE. Tripping either aborts the body, releases both
/// permits and counts `vrt_provider_response_body_aborted_total`.
const BODY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const BODY_SEND_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);

/// Response body that owns the dispatch permits plus a byte budget
/// (RV-R8 #3) and idle/total send deadlines (RV-R28 #2): the permits
/// drop when the last frame has been polled, on disconnect/abort, or
/// when a deadline trips — never at dispatch-return, so a freed permit
/// cannot let a slow client stack unbounded in-flight bodies (the
/// global slot deliberately covers body delivery too; the deadlines
/// below bound the worst-case hold).
struct PermitGuardBody {
    inner: Body,
    remaining: u64,
    /// Stream can no longer emit (budget cut or deadline trip).
    aborted: bool,
    /// RV-R28 #2: per-frame idle timer — reset on each yielded frame.
    idle_sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
    /// RV-R28 #2: fixed total-send timer armed at construction.
    total_sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
    /// Tenant label for the abort metric (bounded — listener tenant).
    tenant_id: String,
    /// Released eagerly via Option::take on stream end/abort/error.
    global: Option<tokio::sync::OwnedSemaphorePermit>,
    tenant: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl PermitGuardBody {
    fn new(
        inner: Body,
        budget: u64,
        tenant_id: String,
        global: tokio::sync::OwnedSemaphorePermit,
        tenant: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            inner,
            remaining: budget,
            aborted: false,
            idle_sleep: Box::pin(tokio::time::sleep_until(now + BODY_IDLE_TIMEOUT)),
            total_sleep: Box::pin(tokio::time::sleep_until(now + BODY_SEND_DEADLINE)),
            tenant_id,
            global: Some(global),
            tenant: Some(tenant),
        }
    }

    /// Drop both permit slots the moment the stream can no longer
    /// emit — natural end, abort or inner error alike (RV-R28 #2).
    fn release_permits(&mut self) {
        self.global.take();
        self.tenant.take();
    }
}

#[derive(Debug)]
struct ResponseBudgetExceeded;
impl std::fmt::Display for ResponseBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dispatch response exceeded byte budget")
    }
}
impl std::error::Error for ResponseBudgetExceeded {}

/// RV-R28 #2: the response body send deadline/idle timeout tripped.
#[derive(Debug)]
struct ResponseBodyTimeout;
impl std::fmt::Display for ResponseBodyTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("dispatch response body deadline or idle timeout")
    }
}
impl std::error::Error for ResponseBodyTimeout {}

impl http_body::Body for PermitGuardBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.aborted {
            return std::task::Poll::Ready(None);
        }
        // RV-R28 #2: deadlines are checked before the inner body — a
        // pending inner with an elapsed timer errors immediately and
        // releases both permits (total deadline outranks idle).
        let reason = if this.total_sleep.as_mut().poll(cx).is_ready() {
            Some("body_deadline")
        } else if this.idle_sleep.as_mut().poll(cx).is_ready() {
            Some("body_idle_timeout")
        } else {
            None
        };
        if let Some(reason) = reason {
            this.aborted = true;
            this.release_permits();
            crate::metrics::body_aborted(&this.tenant_id, reason);
            tracing::warn!(
                tenant = %this.tenant_id,
                reason,
                "dispatch response body deadline tripped — aborting stream"
            );
            return std::task::Poll::Ready(Some(Err(axum::Error::new(ResponseBodyTimeout))));
        }
        match std::pin::Pin::new(&mut this.inner).poll_frame(cx) {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let n = data.len() as u64;
                    if n > this.remaining {
                        this.aborted = true;
                        this.release_permits();
                        crate::metrics::body_aborted(&this.tenant_id, "budget_exceeded");
                        tracing::warn!(
                            tenant = %this.tenant_id,
                            "dispatch response exceeded byte budget — aborting stream"
                        );
                        return std::task::Poll::Ready(Some(Err(axum::Error::new(
                            ResponseBudgetExceeded,
                        ))));
                    }
                    this.remaining -= n;
                }
                // Any yielded frame counts as liveness for the idle
                // timer (RV-R28 #2).
                this.idle_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + BODY_IDLE_TIMEOUT);
                std::task::Poll::Ready(Some(Ok(frame)))
            }
            other @ (std::task::Poll::Ready(None) | std::task::Poll::Ready(Some(Err(_)))) => {
                // Stream finished or inner errored — release the
                // permits now rather than at drop (RV-R28 #2).
                this.release_permits();
                other
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        acquire_permits, parse_d1_path, permit_denied_response, PermitGuardBody, BODY_IDLE_TIMEOUT,
        BODY_SEND_DEADLINE,
    };
    use http_body_util::BodyExt;

    #[test]
    fn d1_path_accepts_query_execute_dump_only() {
        assert_eq!(
            parse_d1_path("/internal/bindings/v1/d1/db1/query"),
            Some(("db1".to_string(), "query"))
        );
        assert_eq!(
            parse_d1_path("/internal/bindings/v1/d1/db1/execute"),
            Some(("db1".to_string(), "execute"))
        );
        assert_eq!(
            parse_d1_path("/internal/bindings/v1/d1/db1/dump"),
            Some(("db1".to_string(), "dump"))
        );
        for bad in [
            "/internal/bindings/v1/d1/db1/drop",
            "/internal/bindings/v1/d1/db1/dump/extra",
            "/internal/bindings/v1/d1/dump",
        ] {
            assert_eq!(parse_d1_path(bad), None, "{bad}");
        }
    }

    /// Test body: emits the queued frames, then pends forever — the
    /// stalled-reader shape the idle timer exists to cut (RV-R28 #2).
    struct StallBody {
        frames: std::collections::VecDeque<bytes::Bytes>,
    }
    impl http_body::Body for StallBody {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            match self.frames.pop_front() {
                Some(b) => std::task::Poll::Ready(Some(Ok(http_body::Frame::data(b)))),
                None => std::task::Poll::Pending,
            }
        }
    }

    fn stall_body(n: usize) -> axum::body::Body {
        let mut frames = std::collections::VecDeque::new();
        for _ in 0..n {
            frames.push_back(bytes::Bytes::from_static(b"chunk"));
        }
        axum::body::Body::new(StallBody { frames })
    }

    /// RV-R8 #3: the byte budget trips mid-stream, not after the whole
    /// response was buffered.
    #[tokio::test]
    async fn permit_guard_body_aborts_over_budget() {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
        let g = sem.clone().acquire_owned().await.unwrap();
        let t = sem.clone().acquire_owned().await.unwrap();
        let body = axum::body::Body::from(vec![0u8; 64]);
        let guarded = axum::body::Body::new(PermitGuardBody::new(body, 4, "t".to_string(), g, t));
        assert!(guarded.collect().await.is_err());
        assert_eq!(sem.available_permits(), 2);
    }

    /// …and a within-budget body passes through untouched while the
    /// permits stay owned by the guard until it is consumed.
    #[tokio::test]
    async fn permit_guard_body_passes_within_budget() {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
        let g = sem.clone().acquire_owned().await.unwrap();
        let t = sem.clone().acquire_owned().await.unwrap();
        let body = axum::body::Body::from(vec![1u8; 64]);
        let guarded = axum::body::Body::new(PermitGuardBody::new(body, 64, "t".to_string(), g, t));
        let collected = guarded.collect().await.unwrap().to_bytes();
        assert_eq!(collected.len(), 64);
        assert_eq!(sem.available_permits(), 2);
    }

    /// RV-R28 #2: a body that stalls mid-stream is cut at the idle
    /// deadline and both permits are released.
    #[tokio::test(start_paused = true)]
    async fn permit_guard_body_aborts_on_idle_timeout() {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
        let g = sem.clone().acquire_owned().await.unwrap();
        let t = sem.clone().acquire_owned().await.unwrap();
        // One frame, then pending forever.
        let mut guarded = PermitGuardBody::new(stall_body(1), 1 << 20, "t".to_string(), g, t);
        let first = guarded.frame().await;
        assert!(matches!(first, Some(Ok(_))));
        // The stream stalls — the idle deadline must trip.
        tokio::time::advance(BODY_IDLE_TIMEOUT + std::time::Duration::from_secs(1)).await;
        assert!(matches!(guarded.frame().await, Some(Err(_))));
        assert_eq!(sem.available_permits(), 2);
    }

    /// RV-R28 #2: even a body making progress cannot exceed the total
    /// send deadline.
    #[tokio::test(start_paused = true)]
    async fn permit_guard_body_aborts_on_send_deadline() {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
        let g = sem.clone().acquire_owned().await.unwrap();
        let t = sem.clone().acquire_owned().await.unwrap();
        let mut guarded = PermitGuardBody::new(stall_body(8), 1 << 20, "t".to_string(), g, t);
        tokio::time::advance(BODY_SEND_DEADLINE + std::time::Duration::from_secs(1)).await;
        assert!(matches!(guarded.frame().await, Some(Err(_))));
        assert_eq!(sem.available_permits(), 2);
    }

    /// RV-R28 #2: a normal body under both deadlines is unaffected.
    #[tokio::test(start_paused = true)]
    async fn permit_guard_body_normal_stream_unaffected() {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
        let g = sem.clone().acquire_owned().await.unwrap();
        let t = sem.clone().acquire_owned().await.unwrap();
        let mut guarded = PermitGuardBody::new(stall_body(3), 1 << 20, "t".to_string(), g, t);
        for _ in 0..3 {
            assert!(matches!(guarded.frame().await, Some(Ok(_))));
            tokio::time::advance(std::time::Duration::from_secs(5)).await;
        }
        // StallBody then pends — but the deadline logic holds until
        // idle trips; drop releases what take() did not.
        drop(guarded);
        assert_eq!(sem.available_permits(), 2);
    }

    /// RV-R28 #1: tenant-first acquisition — waiters queued on a
    /// saturated tenant semaphore hold NO global slots, so another
    /// tenant still acquires before the combined timeout.
    #[tokio::test(start_paused = true)]
    async fn permits_wait_on_tenant_without_pinning_global() {
        let tenant_a = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let tenant_b = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let global = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
        // Tenant A saturated: its only slot is held for the whole test.
        let _a_held = tenant_a.clone().acquire_owned().await.unwrap();
        // Many queued A-waiters — none may consume a global slot.
        let mut waiters = Vec::new();
        for _ in 0..16 {
            let (ta, g) = (tenant_a.clone(), global.clone());
            waiters.push(tokio::spawn(async move { acquire_permits(&ta, &g).await }));
        }
        tokio::task::yield_now().await;
        assert_eq!(global.available_permits(), 4);
        // Tenant B still gets in under the shared deadline.
        let (_tb, _gb) = acquire_permits(&tenant_b, &global).await.unwrap();
        assert_eq!(global.available_permits(), 3);
        for w in waiters {
            w.abort();
        }
    }

    /// RV-R28 #2: permit-denial carries the binding family's own
    /// error shape — the facade parses each differently.
    #[tokio::test]
    async fn permit_denied_response_matches_binding_family() {
        use axum::http::StatusCode;
        // KV → typed 429 envelope.
        let r = permit_denied_response("/internal/bindings/v1/kv/ns1/get");
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes).unwrap().contains("KV_BUSY"));
        // R2 → typed 429 envelope.
        let r = permit_denied_response("/internal/bindings/v1/r2/b1/get");
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes)
            .unwrap()
            .contains("R2_OVERLOADED"));
        // D1 → HTTP 200 {success:false,error:"D1_ERROR: …"}.
        let r = permit_denied_response("/internal/bindings/v1/d1/db1/query");
        assert_eq!(r.status(), StatusCode::OK);
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("\"success\":false") || text.contains("success"));
        assert!(text.contains("D1_ERROR"));
        // D1 dump → non-200: d1-api treats any 200 body as dump bytes.
        let r = permit_denied_response("/internal/bindings/v1/d1/db1/dump");
        assert_eq!(r.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes).unwrap().contains("D1_ERROR"));
        // Queue → {error:…} at 429.
        let r = permit_denied_response("/internal/bindings/v1/queue/q1/send");
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes)
            .unwrap()
            .contains("provider busy"));
        // PG → typed 429 with the PG rate-limit code.
        let r = permit_denied_response("/internal/bindings/v1/pg/conn1/query");
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes)
            .unwrap()
            .contains("PG_RATE_LIMITED"));
        // Assets → allowlisted code.
        let r = permit_denied_response("/internal/assets/v1/fetch");
        let bytes = r.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes)
            .unwrap()
            .contains("ASSET_STORAGE_UNAVAILABLE"));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KvOp {
    Get,
    GetWithMetadata,
    GetMany,
    Put,
    Delete,
    List,
}

fn parse_kv_path(path: &str) -> Option<(String, KvOp)> {
    let rest = path.strip_prefix("/internal/bindings/v1/kv/")?;
    let (id, operation) = rest.split_once('/')?;
    if operation.contains('/') {
        return None;
    }
    let operation = match operation {
        "get" => KvOp::Get,
        "get-with-metadata" => KvOp::GetWithMetadata,
        "get-many" => KvOp::GetMany,
        "put" => KvOp::Put,
        "delete" => KvOp::Delete,
        "list" => KvOp::List,
        _ => return None,
    };
    Some((id.to_string(), operation))
}

/// T13/H3: request-body drain deadline for handlers without service
/// context (r2 head/get/delete/list — all bounded ≤ R2_MAX_* sizes).
const BODY_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn declared_too_large(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|len| len > KV_MAX_FRAME_BODY_BYTES as u64)
}

fn content_type_is(headers: &axum::http::HeaderMap, expected: &str) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|v| v.trim() == expected)
}

fn frame_response(content_type: &'static str, bytes: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn json_response(bytes: Vec<u8>) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn kv_err(err: ProviderError) -> Response<Body> {
    if let Some(reason) = err.forbidden_reason() {
        tracing::warn!(reason, "kv binding request denied");
    }
    error_response(
        err.code(),
        err.status(),
        err.retryable_in(ResourceKind::Kv),
        err.result_unknown(),
    )
}

async fn kv_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    if declared_too_large(request.headers()) {
        return error_response(
            "BINDING_LIMIT_EXCEEDED",
            StatusCode::PAYLOAD_TOO_LARGE,
            false,
            false,
        );
    }
    let Some((binding_id, op)) = parse_kv_path(request.uri().path()) else {
        return error_response("BINDING_PROTOCOL_ERROR", StatusCode::NOT_FOUND, true, false);
    };
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => return kv_err(err),
    };
    if !content_type_is(request.headers(), KV_FRAME_CONTENT_TYPE) {
        return error_response(
            "BINDING_PROTOCOL_ERROR",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            true,
            false,
        );
    }
    let ctx =
        match svc
            .authorizer
            .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::Kv)
        {
            Ok(ctx) => ctx,
            Err(err) => return kv_err(err),
        };
    let write = matches!(op, KvOp::Put | KvOp::Delete);
    let permissions = ctx.permissions();
    if (write && !permissions.write) || (!write && !permissions.read) {
        return kv_err(ProviderError::Forbidden(
            crate::error::ForbiddenReason::PermissionDenied,
        ));
    }
    let Some(provider) = svc.kv.clone() else {
        return kv_err(ProviderError::ResourceUnavailable);
    };

    match op {
        KvOp::Put => kv_put(svc, provider, ctx, request).await,
        _ => {
            // RV-R8 #6: total deadline on the body drain — a client
            // trickling bytes forever would otherwise hold a permit
            // open indefinitely (separate from header processing time).
            let bytes = match tokio::time::timeout(
                svc.body_read_timeout,
                to_bytes(request.into_body(), KV_MAX_FRAME_BODY_BYTES),
            )
            .await
            {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(_)) => return kv_err(ProviderError::KvValueTooLarge),
                Err(_) => return kv_err(ProviderError::KvBusy),
            };
            match op {
                KvOp::Get | KvOp::GetWithMetadata => kv_get(provider, ctx, &bytes, false).await,
                KvOp::GetMany => kv_get(provider, ctx, &bytes, true).await,
                KvOp::Delete => kv_delete(provider, ctx, &bytes).await,
                KvOp::List => kv_list(svc, provider, ctx, &bytes).await,
                KvOp::Put => unreachable!(),
            }
        }
    }
}

fn deadline_timeout(ctx: &BindingContext) -> std::time::Duration {
    ctx.remaining()
}

async fn kv_get(
    provider: Arc<dyn KvProvider>,
    ctx: BindingContext,
    bytes: &[u8],
    multi: bool,
) -> Response<Body> {
    let request: kv::FrameGetRequest = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(_) => return kv_err(ProviderError::Protocol),
    };
    let valid_count = if multi {
        request.keys.len() <= KV_MAX_MULTI_GET_KEYS
    } else {
        request.keys.len() == 1
    };
    if !valid_count {
        let status = if multi {
            StatusCode::PAYLOAD_TOO_LARGE
        } else {
            StatusCode::BAD_REQUEST
        };
        return error_response("KV_TOO_MANY_KEYS", status, false, false);
    }
    if request
        .cache_ttl
        .is_some_and(|ttl| ttl < KV_MIN_CACHE_TTL_SECONDS)
    {
        return kv_err(ProviderError::KvInvalidOptions);
    }
    let mut keys = Vec::with_capacity(request.keys.len());
    for key in &request.keys {
        match kv::validate_key(key) {
            Ok(key) => keys.push(key),
            Err(err) => return kv_err(err),
        }
    }
    let remaining = deadline_timeout(&ctx);
    if multi {
        let result = tokio::time::timeout(remaining, provider.get_many(&ctx, &keys)).await;
        match result {
            Ok(Ok(entries)) => frame_response(KV_FRAME_CONTENT_TYPE, kv::encode_kvb1(&entries)),
            Ok(Err(err)) => kv_err(err),
            Err(_) => kv_err(ProviderError::KvUnavailable),
        }
    } else {
        let key = keys.pop().unwrap();
        let result = tokio::time::timeout(remaining, provider.get(&ctx, &key)).await;
        match result {
            Ok(Ok(entry)) => frame_response(KV_FRAME_CONTENT_TYPE, kv::encode_kvs1(entry.as_ref())),
            Ok(Err(err)) => kv_err(err),
            Err(_) => kv_err(ProviderError::KvUnavailable),
        }
    }
}

async fn kv_put(
    svc: Arc<TenantService>,
    provider: Arc<dyn KvProvider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    // RV-R8 #6: body drain has the same total deadline as the read
    // ops — a stalled upload cannot pin a permit forever.
    let bytes = match tokio::time::timeout(
        svc.body_read_timeout,
        to_bytes(request.into_body(), KV_MAX_FRAME_BODY_BYTES),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => {
            let mut response = kv_err(ProviderError::KvValueTooLarge);
            response
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("close"));
            return response;
        }
        Err(_) => {
            let mut response = kv_err(ProviderError::KvBusy);
            response
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("close"));
            return response;
        }
    };
    let (header, value) = match kv::parse_put_frame(&bytes) {
        Ok(parsed) => parsed,
        Err(err) => {
            let mut response = kv_err(err);
            response
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("close"));
            return response;
        }
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let options = match kv::prepare_put_options(now_ms, &header) {
        Ok(options) => options,
        Err(err) => return kv_err(err),
    };
    let key = match kv::validate_key(&header.key) {
        Ok(key) => key,
        Err(err) => return kv_err(err),
    };
    let remaining = deadline_timeout(&ctx);
    match tokio::time::timeout(remaining, provider.put(&ctx, &key, value, &options)).await {
        Ok(Ok(())) => frame_response(KV_FRAME_CONTENT_TYPE, Vec::new()),
        Ok(Err(err)) => kv_err(err),
        Err(_) => kv_err(ProviderError::KvResultUnknown),
    }
}

async fn kv_delete(
    provider: Arc<dyn KvProvider>,
    ctx: BindingContext,
    bytes: &[u8],
) -> Response<Body> {
    let request: kv::KeyRequest = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(_) => return kv_err(ProviderError::Protocol),
    };
    let key = match kv::validate_key(&request.key) {
        Ok(key) => key,
        Err(err) => return kv_err(err),
    };
    let remaining = deadline_timeout(&ctx);
    match tokio::time::timeout(remaining, provider.delete(&ctx, &key)).await {
        Ok(Ok(())) => frame_response(KV_FRAME_CONTENT_TYPE, Vec::new()),
        Ok(Err(err)) => kv_err(err),
        Err(_) => kv_err(ProviderError::KvResultUnknown),
    }
}

async fn kv_list(
    svc: Arc<TenantService>,
    provider: Arc<dyn KvProvider>,
    ctx: BindingContext,
    bytes: &[u8],
) -> Response<Body> {
    let request: kv::FrameListRequest = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(_) => return kv_err(ProviderError::Protocol),
    };
    if request.limit == 0 || request.limit > KV_MAX_LIST_LIMIT {
        return kv_err(ProviderError::KvInvalidOptions);
    }
    let prefix = request.prefix.unwrap_or_default();
    if prefix.len() > KV_MAX_KEY_BYTES {
        return kv_err(ProviderError::KvKeyTooLarge);
    }
    let namespace = ctx.resource().resource_id().to_string();
    let after = match request.cursor.as_deref() {
        Some(cursor) => {
            match svc
                .signer
                .verify_kv(ctx.tenant_id(), &namespace, prefix.as_bytes(), cursor)
            {
                Ok(after) => Some(after),
                Err(CursorInvalid) => return kv_err(ProviderError::KvCursorInvalid),
            }
        }
        None => None,
    };
    let remaining = deadline_timeout(&ctx);
    let page = match tokio::time::timeout(
        remaining,
        provider.list(&ctx, &prefix, after.as_deref(), request.limit),
    )
    .await
    {
        Ok(Ok(page)) => page,
        Ok(Err(err)) => return kv_err(err),
        Err(_) => return kv_err(ProviderError::KvUnavailable),
    };
    if !page.complete && page.rows.is_empty() {
        // A truncated page without rows has no resume key — the wire
        // contract cannot express it (cursor would be null), so the
        // provider violated its contract.
        return kv_err(ProviderError::KvCorrupt);
    }
    let cursor = if page.complete {
        None
    } else {
        page.rows.last().map(|row| {
            svc.signer
                .sign_kv(ctx.tenant_id(), &namespace, prefix.as_bytes(), &row.key)
        })
    };
    match kv::encode_list_response(&page.rows, page.complete, cursor.as_deref()) {
        Ok(bytes) => json_response(bytes),
        Err(err) => kv_err(err),
    }
}

// ---------------------------------------------------------------- R2

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum R2Op {
    Head,
    Get,
    Put,
    Delete,
    List,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
}

impl R2Op {
    fn write(self) -> bool {
        matches!(
            self,
            Self::Put
                | Self::Delete
                | Self::CreateMultipartUpload
                | Self::UploadPart
                | Self::CompleteMultipartUpload
                | Self::AbortMultipartUpload
        )
    }
}

fn parse_r2_path(path: &str) -> Option<(String, R2Op)> {
    let rest = path.strip_prefix("/internal/bindings/v1/r2/")?;
    let (id, operation) = rest.split_once('/')?;
    if operation.contains('/') {
        return None;
    }
    let operation = match operation {
        "head" => R2Op::Head,
        "get" => R2Op::Get,
        "put" => R2Op::Put,
        "delete" => R2Op::Delete,
        "list" => R2Op::List,
        "createMultipartUpload" => R2Op::CreateMultipartUpload,
        "uploadPart" => R2Op::UploadPart,
        "completeMultipartUpload" => R2Op::CompleteMultipartUpload,
        "abortMultipartUpload" => R2Op::AbortMultipartUpload,
        _ => return None,
    };
    Some((id.to_string(), operation))
}

fn r2_err(err: ProviderError) -> Response<Body> {
    if let Some(reason) = err.forbidden_reason() {
        tracing::warn!(reason, "r2 binding request denied");
    }
    error_response(
        err.code(),
        err.status(),
        err.retryable_in(ResourceKind::R2),
        err.result_unknown(),
    )
}

async fn r2_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    let Some((binding_id, op)) = parse_r2_path(request.uri().path()) else {
        return error_response(
            "BINDING_PROTOCOL_ERROR",
            StatusCode::BAD_REQUEST,
            false,
            false,
        );
    };
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => return r2_err(err),
    };
    let expected_ct = if matches!(op, R2Op::Get | R2Op::Put | R2Op::UploadPart) {
        R2_FRAME_CONTENT_TYPE
    } else {
        R2_JSON_CONTENT_TYPE
    };
    if !content_type_is(request.headers(), expected_ct) {
        return r2_err(ProviderError::Protocol);
    }
    let ctx =
        match svc
            .authorizer
            .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::R2)
        {
            Ok(ctx) => ctx,
            Err(err) => return r2_err(err),
        };
    let permissions = ctx.permissions();
    if (op.write() && !permissions.write) || (!op.write() && !permissions.read) {
        return r2_err(ProviderError::Forbidden(
            crate::error::ForbiddenReason::PermissionDenied,
        ));
    }
    let Some(provider) = svc.r2.clone() else {
        return r2_err(ProviderError::ResourceUnavailable);
    };

    match op {
        R2Op::Head => r2_head(provider, ctx, request).await,
        R2Op::Get => r2_get(provider, ctx, request).await,
        R2Op::Put => r2_put(svc, provider, ctx, request).await,
        R2Op::Delete => r2_delete(provider, ctx, request).await,
        R2Op::List => r2_list(svc, provider, ctx, request).await,
        R2Op::CreateMultipartUpload => r2_create_multipart(svc, provider, ctx, request).await,
        R2Op::UploadPart => r2_upload_part(svc, provider, ctx, request).await,
        R2Op::CompleteMultipartUpload => r2_complete_multipart(svc, provider, ctx, request).await,
        R2Op::AbortMultipartUpload => r2_abort_multipart(svc, provider, ctx, request).await,
    }
}

async fn bounded_body(
    request: Request,
    limit: usize,
    read_timeout: std::time::Duration,
) -> Result<bytes::Bytes, ProviderError> {
    // T13/H3: a bounded body must also arrive in bounded time — a
    // slow-drip body otherwise pins a concurrency permit forever.
    match tokio::time::timeout(read_timeout, to_bytes(request.into_body(), limit)).await {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(_)) => Err(ProviderError::Protocol),
        Err(_) => Err(ProviderError::R2Overloaded),
    }
}

async fn r2_head(
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_METADATA_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => return r2_err(err),
    };
    let input: r2::wire::KeyRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return r2_err(ProviderError::Protocol),
    };
    let key = match r2::validate_object_key(&input.key) {
        Ok(key) => key.to_string(),
        Err(err) => return r2_err(err),
    };
    match tokio::time::timeout(ctx.remaining(), provider.head(&ctx, &key)).await {
        Ok(Ok(Some(metadata))) => match serde_json::to_vec(&metadata) {
            Ok(body) => json_response(body),
            Err(_) => r2_err(ProviderError::Internal),
        },
        Ok(Ok(None)) => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NO_CONTENT;
            response
        }
        Ok(Err(err)) => r2_err(err),
        Err(_) => r2_err(ProviderError::R2ProviderUnavailable),
    }
}

async fn r2_get(
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_METADATA_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => return r2_err(err),
    };
    let input: r2::wire::GetRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return r2_err(ProviderError::Protocol),
    };
    let key = match r2::validate_object_key(&input.key) {
        Ok(key) => key.to_string(),
        Err(err) => return r2_err(err),
    };
    // SSE-C: workerd forwards the customer key as 64 lowercase hex
    // chars; malformed material is rejected here, valid keys are carried
    // into the backend (which fails closed when it cannot encrypt).
    let ssec_key = match input
        .options
        .ssec_key
        .as_deref()
        .map(r2::R2SsecKey::parse)
        .transpose()
    {
        Ok(key) => key,
        Err(err) => return r2_err(err),
    };
    if let Some(range) = input.options.range {
        if let Err(err) = r2::validate_range_shape(range) {
            return r2_err(err);
        }
    }
    let remaining = ctx.remaining();
    match tokio::time::timeout(
        remaining,
        provider.get(
            &ctx,
            &key,
            input.options.range,
            input.options.only_if.as_ref(),
            ssec_key.as_ref(),
        ),
    )
    .await
    {
        Ok(Ok(R2GetResult::Missing)) => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NO_CONTENT;
            response
        }
        Ok(Ok(R2GetResult::Precondition(metadata))) => {
            match framed_metadata(&metadata, false, Vec::new()) {
                Ok(response) => response,
                Err(err) => r2_err(err),
            }
        }
        Ok(Ok(R2GetResult::Body(metadata, bytes))) => {
            match framed_metadata(&metadata, true, bytes) {
                Ok(response) => response,
                Err(err) => r2_err(err),
            }
        }
        Ok(Err(err)) => r2_err(err),
        Err(_) => r2_err(ProviderError::R2ProviderUnavailable),
    }
}

/// `u32-BE len | {"meta": ..., "hasBody": ...} | bytes` R2 frame.
fn framed_metadata(
    metadata: &r2::R2ObjectMetadata,
    has_body: bool,
    body: Vec<u8>,
) -> Result<Response<Body>, ProviderError> {
    let header_bytes = serde_json::to_vec(&serde_json::json!({
        "meta": metadata,
        "hasBody": has_body,
    }))
    .map_err(|_| ProviderError::Internal)?;
    if header_bytes.len() > R2_MAX_METADATA_BYTES {
        return Err(ProviderError::R2MetadataTooLarge);
    }
    let mut bytes = (header_bytes.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(&body);
    Ok(frame_response(R2_FRAME_CONTENT_TYPE, bytes))
}

async fn r2_put(
    svc: Arc<TenantService>,
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let limit = R2_MAX_METADATA_BYTES + svc.r2_max_object_bytes as usize + 1024;
    let bytes =
        match tokio::time::timeout(svc.body_read_timeout, to_bytes(request.into_body(), limit))
            .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return r2_err(ProviderError::R2ObjectTooLarge),
            Err(_) => return r2_err(ProviderError::R2Overloaded),
        };
    if bytes.len() < 4 {
        return r2_err(ProviderError::Protocol);
    }
    let header_len =
        u32::from_be_bytes(bytes[..4].try_into().expect("length checked above")) as usize;
    let Some(header_end) = 4_usize.checked_add(header_len) else {
        return r2_err(ProviderError::Protocol);
    };
    if header_len > R2_MAX_METADATA_BYTES || header_end > bytes.len() {
        return r2_err(ProviderError::Protocol);
    }
    let input: r2::wire::PutHeader = match serde_json::from_slice(&bytes[4..header_end]) {
        Ok(input) => input,
        Err(_) => return r2_err(ProviderError::Protocol),
    };
    let key = match r2::validate_object_key(&input.key) {
        Ok(key) => key.to_string(),
        Err(err) => return r2_err(err),
    };
    let options: r2::R2PutOptions = match input.options.try_into() {
        Ok(options) => options,
        Err(err) => return r2_err(err),
    };
    let body = &bytes[header_end..];
    if body.len() as u64 > svc.r2_max_object_bytes {
        return r2_err(ProviderError::R2ObjectTooLarge);
    }
    let remaining = ctx.remaining();
    match tokio::time::timeout(remaining, provider.put(&ctx, &key, body, &options)).await {
        Ok(Ok(Some(metadata))) => match serde_json::to_vec(&metadata) {
            Ok(body) => json_response(body),
            Err(_) => r2_err(ProviderError::Internal),
        },
        Ok(Ok(None)) => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NO_CONTENT;
            response
        }
        Ok(Err(err)) => r2_err(err),
        Err(_) => r2_err(ProviderError::R2ResultUnknown),
    }
}

async fn r2_delete(
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_DELETE_BODY_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => return r2_err(err),
    };
    let input: r2::wire::DeleteRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return r2_err(ProviderError::Protocol),
    };
    if input.keys.is_empty() || input.keys.len() > R2_MAX_DELETE_KEYS {
        return r2_err(ProviderError::R2InvalidOptions);
    }
    let mut keys: Vec<String> = Vec::with_capacity(input.keys.len());
    for key in &input.keys {
        let key = match r2::validate_object_key(key) {
            Ok(key) => key.to_string(),
            Err(err) => return r2_err(err),
        };
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    match tokio::time::timeout(ctx.remaining(), provider.delete(&ctx, &keys)).await {
        Ok(Ok(())) => {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NO_CONTENT;
            response
        }
        Ok(Err(err)) => r2_err(err),
        Err(_) => r2_err(ProviderError::R2ResultUnknown),
    }
}

async fn r2_list(
    svc: Arc<TenantService>,
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_METADATA_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => return r2_err(err),
    };
    let input: r2::wire::ListRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return r2_err(ProviderError::Protocol),
    };
    if input.limit > R2_MAX_LIST_LIMIT
        || input.delimiter.as_deref().is_some_and(str::is_empty)
        || input.include.len() > 2
    {
        return r2_err(ProviderError::R2InvalidOptions);
    }
    let include_mask = match r2::wire::include_mask(&input.include) {
        Ok(mask) => mask,
        Err(err) => return r2_err(err),
    };
    let cursor_after = match input.cursor.as_deref() {
        Some(cursor) => {
            let payload = match svc.signer.verify_r2(cursor) {
                Ok(payload) => payload,
                Err(CursorInvalid) => return r2_err(ProviderError::R2CursorInvalid),
            };
            let resource = crate::cursor::CursorSigner::r2_resource_id(
                ctx.tenant_id(),
                ctx.resource().resource_id(),
            );
            if payload.resource_id != resource
                || payload.prefix_sha256 != sha256_hex(&input.prefix)
                || payload.delimiter_sha256 != sha256_hex(input.delimiter.as_deref().unwrap_or(""))
                || payload.include_mask != include_mask
                || payload.start_after_sha256
                    != sha256_hex(input.start_after.as_deref().unwrap_or(""))
            {
                return r2_err(ProviderError::R2CursorInvalid);
            }
            payload.after_key
        }
        None => None,
    };
    let after = cursor_after.as_deref().or(input.start_after.as_deref());
    let limit = input.limit.max(1);
    let remaining = ctx.remaining();
    let page = match tokio::time::timeout(
        remaining,
        provider.list(
            &ctx,
            &input.prefix,
            input.delimiter.as_deref(),
            after,
            limit,
        ),
    )
    .await
    {
        Ok(Ok(page)) => page,
        Ok(Err(err)) => return r2_err(err),
        Err(_) => return r2_err(ProviderError::R2ProviderUnavailable),
    };
    let truncated = page.next_after.is_some();
    let (objects, delimited_prefixes) = if input.limit == 0 {
        (Vec::new(), Vec::new())
    } else {
        (
            page.objects
                .into_iter()
                .map(|meta| r2::wire::list_object_json(meta, include_mask))
                .collect(),
            page.delimited_prefixes,
        )
    };
    let cursor = if truncated {
        // Resume at the last raw key scanned — including limit==0 probes,
        // which still consumed one entry.
        let after_key = page.next_after.clone();
        let payload = r2::R2CursorPayload {
            v: 1,
            resource_id: crate::cursor::CursorSigner::r2_resource_id(
                ctx.tenant_id(),
                ctx.resource().resource_id(),
            ),
            generation: ctx.snapshot_revision(),
            prefix_sha256: sha256_hex(&input.prefix),
            delimiter_sha256: sha256_hex(input.delimiter.as_deref().unwrap_or("")),
            include_mask,
            start_after_sha256: sha256_hex(input.start_after.as_deref().unwrap_or("")),
            after_key,
            expires_at_ms: chrono::Utc::now().timestamp_millis() + CURSOR_TTL_MS,
        };
        match svc.signer.sign_r2(&payload) {
            Ok(cursor) => Some(cursor),
            Err(_) => return r2_err(ProviderError::Internal),
        }
    } else {
        None
    };
    // Upstream omits `cursor` entirely when the page is complete; the
    // facade validates `truncated ⇒ cursor is string`, `else nullish`.
    let response = match cursor {
        Some(cursor) => serde_json::json!({
            "objects": objects,
            "truncated": truncated,
            "cursor": cursor,
            "delimitedPrefixes": delimited_prefixes,
        }),
        None => serde_json::json!({
            "objects": objects,
            "truncated": truncated,
            "delimitedPrefixes": delimited_prefixes,
        }),
    };
    match serde_json::to_vec(&response) {
        Ok(bytes) => json_response(bytes),
        Err(_) => r2_err(ProviderError::Internal),
    }
}

/// `POST /internal/assets/v1/fetch` — the facade POSTs with the binding
/// id carried in `x-open-compute-binding-id` (the route itself has no
/// id segment). Authorization is the same desired-state pin as KV/R2;
/// the manifest digest is the `namespaceResourceId`.
async fn assets_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    let binding_id = request
        .headers()
        .get("x-open-compute-binding-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if binding_id.is_empty() || binding_id.len() > 128 {
        return error_response(
            "BINDING_PROTOCOL_ERROR",
            StatusCode::BAD_REQUEST,
            true,
            false,
        );
    }
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => {
            return error_response(
                err.code(),
                err.status(),
                err.retryable_in(ResourceKind::Assets),
                err.result_unknown(),
            )
        }
    };
    let ctx =
        match svc
            .authorizer
            .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::Assets)
        {
            Ok(ctx) => ctx,
            Err(err) => {
                return error_response(
                    err.code(),
                    err.status(),
                    err.retryable_in(ResourceKind::Assets),
                    err.result_unknown(),
                )
            }
        };
    if !ctx.permissions().read {
        return error_response(
            "BINDING_PERMISSION_DENIED",
            StatusCode::FORBIDDEN,
            true,
            false,
        );
    }
    let Some(store) = &svc.assets else {
        return error_response(
            "ASSET_STORAGE_UNAVAILABLE",
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        );
    };
    match request.uri().path() {
        "/internal/assets/v1/route-index" => crate::assets::route_index(
            store,
            ctx.tenant_id(),
            ctx.resource().resource_id(),
            &request,
        ),
        "/internal/assets/v1/can-fetch" => crate::assets::can_fetch(
            store,
            ctx.tenant_id(),
            ctx.resource().resource_id(),
            &request,
        ),
        _ => crate::assets::fetch(
            store,
            ctx.tenant_id(),
            ctx.resource().resource_id(),
            &request,
        ),
    }
}

// ---------------- D1 (T34) ----------------

/// `/internal/bindings/v1/d1/{bindingId}/{op}`, op in query|execute|dump.
fn parse_d1_path(path: &str) -> Option<(String, &'static str)> {
    let rest = path.strip_prefix("/internal/bindings/v1/d1/")?;
    let (id, op) = rest.split_once('/')?;
    if op.contains('/') {
        return None;
    }
    let op = match op {
        "query" => "query",
        "execute" => "execute",
        // `D1Database.dump()` posts to `/dump`; workerd's d1-api turns
        // the 200 body into the tenant's ArrayBuffer.
        "dump" => "dump",
        _ => return None,
    };
    Some((id.to_string(), op))
}

/// D1 error surface: HTTP 200 + `{success:false,error:"D1_ERROR: ..."}`.
fn d1_wire_error(message: &str) -> Response<Body> {
    d1_wire_error_status(message, StatusCode::OK)
}

/// `dump()` errors must not be 200: d1-api only parses an error body when
/// the status is not 200, and would otherwise return the JSON text as the
/// dump itself.
fn d1_dump_error(message: &str) -> Response<Body> {
    d1_wire_error_status(message, StatusCode::INTERNAL_SERVER_ERROR)
}

/// Op-aware local error: `dump` must be non-200, everything else keeps
/// the D1 `{success:false}` 200 surface that d1-api throws on.
fn d1_op_error(op: &str, message: &str) -> Response<Body> {
    if op == "dump" {
        d1_dump_error(message)
    } else {
        d1_wire_error(message)
    }
}

fn d1_wire_error_status(message: &str, status: StatusCode) -> Response<Body> {
    // Error text is bounded: only a restricted message goes in (never a secret/path).
    let bounded: String = message.chars().take(200).collect();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(
            serde_json::json!({"success": false, "error": format!("D1_ERROR: {bounded}")})
                .to_string()
                .into_bytes(),
        ))
        .unwrap()
}

/// T34d: one bounded warn per D1 rejection — reason token + bindingId
/// only (never SQL, params, secrets, or header values). Provider had
/// zero D1 observability when this path denied silently.
fn warn_d1(binding_id: &str, stage: &'static str, err: &ProviderError) {
    let reason = match err {
        ProviderError::Forbidden(r) => r.as_str(),
        other => other.code(),
    };
    tracing::warn!(binding_id, stage, reason, "d1 request denied");
}

async fn d1_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    // worker label is parsed once up-front; used for every outcome.
    let worker_id = crate::metrics::worker_label(&request);
    let Some((binding_id, op)) = parse_d1_path(request.uri().path()) else {
        tracing::warn!("d1 request denied: bad path");
        crate::metrics::d1_outcome(&worker_id, "bad_path");
        return error_response(
            "BINDING_PROTOCOL_ERROR",
            StatusCode::BAD_REQUEST,
            false,
            false,
        );
    };
    // Content-Length is checked first (reject over-limit before reading the body).
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > crate::d1::D1_MAX_BODY_BYTES)
    {
        tracing::warn!(binding_id, "d1 request denied: oversized body");
        crate::metrics::d1_outcome(&worker_id, "oversize");
        return d1_op_error(op, "request too large");
    }
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => {
            warn_d1(&binding_id, "identity", &err);
            crate::metrics::d1_outcome(&worker_id, "denied_identity");
            return error_response(
                err.code(),
                err.status(),
                err.retryable_in(ResourceKind::D1),
                false,
            );
        }
    };
    let ctx =
        match svc
            .authorizer
            .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::D1)
        {
            Ok(ctx) => ctx,
            Err(err) => {
                warn_d1(&binding_id, "authorize", &err);
                crate::metrics::d1_outcome(&worker_id, "denied_authz");
                return error_response(
                    err.code(),
                    err.status(),
                    err.retryable_in(ResourceKind::D1),
                    false,
                );
            }
        };
    let permissions = ctx.permissions();
    // Every D1 statement may write -- require the write grant (reads are covered by write;
    // a finer-grained split follows once the D1 permission model is clarified).
    if !permissions.write {
        tracing::warn!(
            binding_id,
            reason = "permission_denied",
            "d1 request denied"
        );
        crate::metrics::d1_outcome(&worker_id, "denied_permission");
        return error_response(
            "BINDING_PERMISSION_DENIED",
            StatusCode::FORBIDDEN,
            false,
            false,
        );
    }
    let Some(client) = svc.d1.clone() else {
        tracing::warn!(binding = ctx.binding_name(), "d1 backend unconfigured");
        crate::metrics::d1_outcome(&worker_id, "backend_unconfigured");
        return d1_op_error(op, "backend unavailable");
    };
    let query_suffix = request
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    // Pass through session/bookmark headers (x-cf-d1-*); drop all other inbound headers.
    let session_headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-cf-d1-"))
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|v| (k.as_str().to_string(), v.to_string()))
        })
        .collect();
    let body = match tokio::time::timeout(
        svc.body_read_timeout,
        to_bytes(request.into_body(), crate::d1::D1_MAX_BODY_BYTES),
    )
    .await
    {
        Ok(Ok(b)) => b,
        _ => {
            tracing::warn!(binding_id, "d1 request denied: body read failed");
            crate::metrics::d1_outcome(&worker_id, "body_error");
            return d1_op_error(op, "failed to read request body");
        }
    };
    // `dump()` posts no body; every other op is a JSON statement batch.
    if op != "dump" {
        if let Err(why) = crate::d1::D1Client::precheck(&body) {
            tracing::warn!(binding_id, reason = "precheck", "d1 request denied");
            crate::metrics::d1_outcome(&worker_id, "precheck");
            return d1_wire_error(why);
        }
    }
    match client
        .forward(&ctx, op, &query_suffix, body, session_headers)
        .await
    {
        Ok((status, bookmark, bytes)) => {
            // T36: vrt-sql fenced/unavailable → D1 retryable error
            // surface. (No writerAddr cache exists — the client holds a
            // single static addr; recovery happens inside vrt-sql.)
            // `dump` errors must stay non-200 (d1-api decides success by
            // status alone), so its JSON error bodies pass through as-is.
            if op != "dump" {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    match v.get("code").and_then(|c| c.as_str()) {
                        Some("VRT_FENCED") => {
                            tracing::warn!(binding_id, "vrt-sql writer fenced");
                            crate::metrics::d1_outcome(&worker_id, "fenced");
                            return d1_wire_error("backend fenced; retry");
                        }
                        Some("VRT_UNAVAILABLE") => {
                            crate::metrics::d1_outcome(&worker_id, "backend_unavailable");
                            return d1_wire_error("backend unavailable; retry");
                        }
                        _ => {}
                    }
                }
            }
            let mut b = Response::builder().status(status);
            if let Some(bm) = bookmark {
                b = b.header(crate::d1::BOOKMARK_HEADER, bm);
            }
            crate::metrics::d1_outcome(&worker_id, "ok");
            let content_type = if op == "dump" && status == 200 {
                "application/octet-stream"
            } else {
                "application/json"
            };
            b.header(header::CONTENT_TYPE, content_type)
                .body(Body::from(bytes))
                .unwrap()
        }
        Err(_) => {
            // vrt-sql unavailable: D1 error surface + structured log (forward already warned).
            crate::metrics::d1_backend_unavailable(ctx.worker_id());
            crate::metrics::d1_outcome(&worker_id, "backend_error");
            d1_op_error(op, "backend unavailable")
        }
    }
}

// ---------------- PG (P1) ----------------

/// `/internal/bindings/v1/pg/{bindingId}/sql` — Neon-compatible wire
/// passthrough. The provider owns the tenant credential; the body and
/// the `neon-batch-*` allowlisted headers are relayed upstream with
/// read-only enforcement applied provider-side.
fn parse_pg_path(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/internal/bindings/v1/pg/")?;
    let (id, op) = rest.split_once('/')?;
    if op != "sql" || op.contains('/') {
        return None;
    }
    Some(id.to_string())
}

/// One bounded warn per PG rejection — reason code + bindingId only
/// (never SQL, params, tokens, endpoints, or header values).
fn warn_pg(binding_id: &str, stage: &'static str, err: &ProviderError) {
    let reason = match err {
        ProviderError::Forbidden(r) => r.as_str(),
        other => other.code(),
    };
    tracing::warn!(binding_id, stage, reason, "pg request denied");
}

async fn pg_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    let _worker_id = crate::metrics::worker_label(&request);
    let Some(binding_id) = parse_pg_path(request.uri().path()) else {
        tracing::warn!("pg request denied: bad path");
        return error_response(
            "BINDING_PROTOCOL_ERROR",
            StatusCode::BAD_REQUEST,
            false,
            false,
        );
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > crate::pg::PG_MAX_REQUEST_BYTES)
    {
        tracing::warn!(binding_id, "pg request denied: oversized body");
        return error_response(
            "BINDING_LIMIT_EXCEEDED",
            StatusCode::PAYLOAD_TOO_LARGE,
            false,
            false,
        );
    }
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => {
            warn_pg(&binding_id, "identity", &err);
            return error_response(
                err.code(),
                err.status(),
                err.retryable_in(ResourceKind::Pg),
                false,
            );
        }
    };
    let ctx =
        match svc
            .authorizer
            .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::Pg)
        {
            Ok(ctx) => ctx,
            Err(err) => {
                warn_pg(&binding_id, "authorize", &err);
                return error_response(
                    err.code(),
                    err.status(),
                    err.retryable_in(ResourceKind::Pg),
                    false,
                );
            }
        };
    let Some(backend) = svc.pg.clone() else {
        warn_pg(
            &binding_id,
            "backend",
            &ProviderError::PgCredentialUnavailable,
        );
        return error_response(
            "PG_CREDENTIAL_UNAVAILABLE",
            StatusCode::SERVICE_UNAVAILABLE,
            true,
            false,
        );
    };
    // Allowlisted Neon batch headers — everything else is dropped
    // (auth is minted provider-side; inbound copies are forgery).
    let passthrough: Vec<(String, String)> = request
        .headers()
        .iter()
        .filter_map(|(k, v)| {
            let k = k.as_str().to_ascii_lowercase();
            if k.starts_with("neon-batch-") {
                v.to_str().ok().map(|v| (k, v.to_string()))
            } else {
                None
            }
        })
        .collect();
    let body = match tokio::time::timeout(
        svc.body_read_timeout,
        axum::body::to_bytes(request.into_body(), crate::pg::PG_MAX_REQUEST_BYTES),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(_)) => {
            return error_response(
                "BINDING_LIMIT_EXCEEDED",
                StatusCode::PAYLOAD_TOO_LARGE,
                false,
                false,
            )
        }
        Err(_) => {
            return error_response(
                "BINDING_PROTOCOL_ERROR",
                StatusCode::REQUEST_TIMEOUT,
                false,
                false,
            )
        }
    };
    match backend.execute(&ctx, &body, &passthrough).await {
        Ok(resp) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(resp.body))
            .unwrap_or_else(|_| {
                error_response(
                    "INTERNAL_ERROR",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    false,
                    false,
                )
            }),
        Err(err) => {
            warn_pg(&binding_id, "upstream", &err);
            error_response(
                err.code(),
                err.status(),
                err.retryable_in(ResourceKind::Pg),
                err.result_unknown(),
            )
        }
    }
}

/// ---------- T37: queue producer (section 8 broker wire -> vrt-sql) ----------

/// `/internal/bindings/v1/queue/{bindingId}/{op}`, op in message|batch|metrics.
fn parse_queue_path(path: &str) -> Option<(String, &'static str)> {
    let rest = path.strip_prefix("/internal/bindings/v1/queue/")?;
    let (id, op) = rest.rsplit_once('/')?;
    if id.is_empty() {
        return None;
    }
    let op = match op {
        "message" => "message",
        "batch" => "batch",
        "metrics" => "metrics",
        _ => return None,
    };
    Some((id.to_string(), op))
}

/// broker error surface: status code + `{"error": msg}` (turned into an exception on the workerd side).
fn queue_wire_error(status: StatusCode, msg: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::json!({"error": msg}).to_string()))
        .unwrap()
}

async fn queue_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    let worker_id = crate::metrics::worker_label(&request);
    let tenant = svc.tenant_id.clone();
    let Some((binding_id, op)) = parse_queue_path(request.uri().path()) else {
        crate::metrics::queue_rejected(&tenant, &worker_id, "bad_path");
        return queue_wire_error(StatusCode::BAD_REQUEST, "bad queue route");
    };
    // `GET /metrics` has no body; write ops check Content-Length first.
    if op != "metrics" {
        if request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .is_some_and(|n| n > crate::queue::QUEUE_MAX_BODY_BYTES)
        {
            crate::metrics::queue_rejected(&tenant, &worker_id, "oversize");
            return queue_wire_error(StatusCode::PAYLOAD_TOO_LARGE, "request too large");
        }
    }
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => {
            crate::metrics::queue_rejected(&tenant, &worker_id, "denied_identity");
            return error_response(
                err.code(),
                err.status(),
                err.retryable_in(ResourceKind::Queue),
                false,
            );
        }
    };
    let ctx =
        match svc
            .authorizer
            .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::Queue)
        {
            Ok(ctx) => ctx,
            Err(err) => {
                crate::metrics::queue_rejected(&tenant, &worker_id, "denied_authz");
                return error_response(
                    err.code(),
                    err.status(),
                    err.retryable_in(ResourceKind::Queue),
                    false,
                );
            }
        };
    let permissions = ctx.permissions();
    // Producer ops need write; metrics needs read (either grant allows reading the numbers).
    let allowed = if op == "metrics" {
        permissions.read || permissions.write
    } else {
        permissions.write
    };
    if !allowed {
        crate::metrics::queue_rejected(&tenant, &worker_id, "denied_permission");
        return error_response(
            "BINDING_PERMISSION_DENIED",
            StatusCode::FORBIDDEN,
            false,
            false,
        );
    }
    let Some(client) = svc.queue.clone() else {
        crate::metrics::queue_rejected(&tenant, &worker_id, "backend_unconfigured");
        return queue_wire_error(StatusCode::SERVICE_UNAVAILABLE, "queue backend unavailable");
    };
    let queue_id = match ctx.resource() {
        crate::context::BindingResource::Queue { id } => id.clone(),
        _ => return queue_wire_error(StatusCode::FORBIDDEN, "binding kind mismatch"),
    };
    match op {
        "metrics" => {
            match client
                .forward(&ctx, "metrics", "GET", axum::body::Bytes::new())
                .await
            {
                Ok((status, bytes)) => {
                    if status != 200 {
                        return queue_backend_error(status, &bytes, &tenant, &worker_id);
                    }
                    match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(v) => {
                            let m = v.get("metrics").cloned().unwrap_or_default();
                            let wm: crate::queue::WireMetrics =
                                serde_json::from_value(m).unwrap_or_default();
                            crate::metrics::queue_backlog(&tenant, &queue_id, &wm);
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(header::CONTENT_TYPE, "application/json")
                                .body(Body::from(
                                    crate::queue::broker_metrics_json(&wm).to_string(),
                                ))
                                .unwrap()
                        }
                        Err(_) => queue_wire_error(
                            StatusCode::BAD_GATEWAY,
                            "queue backend protocol error",
                        ),
                    }
                }
                Err(_) => {
                    crate::metrics::queue_rejected(&tenant, &worker_id, "backend_error");
                    queue_wire_error(StatusCode::SERVICE_UNAVAILABLE, "queue backend unavailable")
                }
            }
        }
        _ => {
            let headers = request.headers().clone();
            let body = match tokio::time::timeout(
                svc.body_read_timeout,
                to_bytes(request.into_body(), crate::queue::QUEUE_MAX_BODY_BYTES),
            )
            .await
            {
                Ok(Ok(b)) => b,
                _ => {
                    crate::metrics::queue_rejected(&tenant, &worker_id, "body_error");
                    return queue_wire_error(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "failed to read request body",
                    );
                }
            };
            // broker wire -> vrt-sql send request (including 413/400 semantics).
            let parsed = if op == "message" {
                crate::queue::producer_message(&headers, &body)
            } else {
                crate::queue::producer_batch(&headers, &body)
            };
            let send_body = match parsed {
                Ok(v) => v,
                Err((status, msg)) => {
                    crate::metrics::queue_rejected(
                        &tenant,
                        &worker_id,
                        if status == 413 { "oversize" } else { "invalid" },
                    );
                    return queue_wire_error(
                        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
                        &msg,
                    );
                }
            };
            match client
                .forward(
                    &ctx,
                    "send",
                    "POST",
                    axum::body::Bytes::from(send_body.to_string()),
                )
                .await
            {
                Ok((status, bytes)) => {
                    if status != 200 {
                        return queue_backend_error(status, &bytes, &tenant, &worker_id);
                    }
                    match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(v) => {
                            let m = v.get("metrics").cloned().unwrap_or_default();
                            let wm: crate::queue::WireMetrics =
                                serde_json::from_value(m).unwrap_or_default();
                            crate::metrics::queue_backlog(&tenant, &queue_id, &wm);
                            crate::metrics::queue_sent(
                                &tenant,
                                &worker_id,
                                v.get("enqueued").and_then(|e| e.as_u64()).unwrap_or(0),
                            );
                            crate::metrics::queue_duplicate_ids(
                                &tenant,
                                &worker_id,
                                v.get("duplicates").and_then(|e| e.as_u64()).unwrap_or(0),
                            );
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(header::CONTENT_TYPE, "application/json")
                                .body(Body::from(
                                    serde_json::json!({
                                        "metadata": {
                                            "metrics": crate::queue::broker_metrics_json(&wm)
                                        }
                                    })
                                    .to_string(),
                                ))
                                .unwrap()
                        }
                        Err(_) => queue_wire_error(
                            StatusCode::BAD_GATEWAY,
                            "queue backend protocol error",
                        ),
                    }
                }
                Err(_) => {
                    crate::metrics::queue_rejected(&tenant, &worker_id, "backend_error");
                    queue_wire_error(StatusCode::SERVICE_UNAVAILABLE, "queue backend unavailable")
                }
            }
        }
    }
}

/// ---------- K2 producer (CF K2 `send()` wire -> vrt-sql) ----------
///
/// `/internal/bindings/v1/k2/{bindingId}/send` — POST only. The
/// response surface is CF K2's non-throwing `{success:true}` /
/// `{error:{code,message,retryable}}` envelope; backend envelopes are
/// relayed verbatim so the shim sees one uniform contract.
fn parse_k2_path(path: &str) -> Option<(String, &'static str)> {
    let rest = path.strip_prefix("/internal/bindings/v1/k2/")?;
    let (id, op) = rest.rsplit_once('/')?;
    if id.is_empty() {
        return None;
    }
    let op = match op {
        "send" => "send",
        _ => return None,
    };
    Some((id.to_string(), op))
}

/// K2 error surface: the CF `{success:false,error:{code,message,
/// retryable}}` envelope the shim returns from `send()` — never a
/// thrown exception on expected failure paths.
fn k2_wire_error(status: StatusCode, code: &str, msg: &str, retryable: bool) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            crate::k2::error_json(code, msg, retryable).to_string(),
        ))
        .unwrap()
}

async fn k2_dispatch(svc: Arc<TenantService>, request: Request) -> Response<Body> {
    let Some((binding_id, _op)) = parse_k2_path(request.uri().path()) else {
        return k2_wire_error(
            StatusCode::BAD_REQUEST,
            "BAD_ROUTE",
            "bad k2 route",
            false,
        );
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > crate::k2::K2_MAX_BODY_BYTES)
    {
        return k2_wire_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "BATCH_TOO_LARGE",
            "request too large",
            false,
        );
    }
    let identity = match parse_request_identity(&binding_id, request.headers()) {
        Ok(identity) => identity,
        Err(err) => {
            return k2_wire_error(
                err.status(),
                err.code(),
                "identity rejected",
                err.retryable_in(ResourceKind::K2),
            );
        }
    };
    let ctx = match svc
        .authorizer
        .authorize(&svc.tenant_id, &identity, &binding_id, ResourceKind::K2)
    {
        Ok(ctx) => ctx,
        Err(err) => {
            return k2_wire_error(
                err.status(),
                err.code(),
                "authorization denied",
                err.retryable_in(ResourceKind::K2),
            );
        }
    };
    let permissions = ctx.permissions();
    // `send` is produce → requires the signed `write` grant.
    if !permissions.write {
        return k2_wire_error(
            StatusCode::FORBIDDEN,
            "BINDING_PERMISSION_DENIED",
            "binding lacks write permission",
            false,
        );
    }
    let Some(client) = svc.k2.clone() else {
        return k2_wire_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "BACKEND_UNAVAILABLE",
            "k2 backend unavailable",
            true,
        );
    };
    let body = match tokio::time::timeout(
        svc.body_read_timeout,
        to_bytes(request.into_body(), crate::k2::K2_MAX_BODY_BYTES),
    )
    .await
    {
        Ok(Ok(b)) => b,
        _ => {
            return k2_wire_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "BATCH_TOO_LARGE",
                "failed to read request body",
                false,
            );
        }
    };
    // CF K2 `send()` contract → vrt-sql produce request (record <=1MB,
    // batch <=5MB, headers shape — 4xx/413 rejections already carry the
    // typed envelope).
    let send_body = match crate::k2::validate_send_body(&body) {
        Ok(v) => v,
        Err((status, err)) => {
            return Response::builder()
                .status(StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(err.to_string()))
                .unwrap();
        }
    };
    match client
        .forward(&ctx, "produce", axum::body::Bytes::from(send_body.to_string()))
        .await
    {
        Ok((_status, bytes)) => Response::builder()
            // The typed envelope carries the outcome; a single 200
            // surface keeps the shim contract uniform (backend
            // `success:false` envelopes arrive on non-200 upstream
            // statuses too).
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .unwrap(),
        Err(_) => k2_wire_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "BACKEND_UNAVAILABLE",
            "k2 backend unavailable",
            true,
        ),
    }
}

/// vrt-sql non-200 -> broker error surface (status passed through, error body unified).
fn queue_backend_error(
    status: u16,
    bytes: &axum::body::Bytes,
    tenant: &str,
    worker_id: &str,
) -> Response<Body> {
    let code = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| {
            v.get("code")
                .and_then(|c| c.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_default();
    let outcome = match code.as_str() {
        "VRT_QUEUE_TOO_LARGE" => "oversize",
        "VRT_QUEUE_INVALID" => "invalid",
        "VRT_QUEUE_DROPPED" => "dropped",
        "VRT_FENCED" => "fenced",
        "VRT_UNAVAILABLE" => "backend_unavailable",
        _ => "backend_error",
    };
    crate::metrics::queue_rejected(
        tenant,
        worker_id,
        match outcome {
            "oversize" | "invalid" | "dropped" | "fenced" | "backend_unavailable" => outcome,
            _ => "backend_error",
        },
    );
    let st = StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
    let msg = match code.as_str() {
        "VRT_QUEUE_TOO_LARGE" => "queue payload too large",
        "VRT_QUEUE_INVALID" => "invalid queue request",
        "VRT_QUEUE_DROPPED" => "queue deleted",
        "VRT_FENCED" => "queue backend fenced; retry",
        _ => "queue backend unavailable",
    };
    queue_wire_error(st, msg)
}

// -------------------------------------------------- R2 multipart (W5)
//
// The tenant-facing `uploadId` is a signed opaque token issued here at
// create (`CursorSigner::sign_r2_upload`): HMAC-SHA256 binding
// (tenant, bucket, key, backend-internal upload id, issue/expiry).
// uploadPart/complete/abort verify signature + scope BEFORE the backend
// sees the request — forged, replayed, cross-tenant/bucket/key, or
// expired tokens fail closed as `R2_MULTIPART_INVALID`. Raw tokens are
// never logged.

/// Per-tenant multipart op accounting; `outcome` ∈ `ok|error`.
fn mp_metric(tenant: &str, op: &'static str, ok: bool) {
    crate::metrics::registry().inc(
        "vrt_provider_r2_multipart_ops_total",
        &[
            ("tenant", tenant),
            ("op", op),
            ("outcome", if ok { "ok" } else { "error" }),
        ],
        1.0,
    );
}

/// Bucket name from the authorized resource (validated by the
/// authorizer — never trusted from the request).
fn r2_bucket(ctx: &BindingContext) -> Result<String, ProviderError> {
    match ctx.resource() {
        crate::context::BindingResource::R2Bucket { name } => Ok(name.clone()),
        _ => Err(ProviderError::Internal),
    }
}

/// Verify the signed upload token against `(tenant, bucket, key)` and
/// return the backend-internal upload id (utf8). Any malformed,
/// tampered, expired, or cross-scope token → `R2_MULTIPART_INVALID`.
fn verify_upload_token(
    svc: &TenantService,
    ctx: &BindingContext,
    key: &str,
    token: &str,
) -> Result<String, ProviderError> {
    let internal = svc
        .signer
        .verify_r2_upload(ctx.tenant_id(), &r2_bucket(ctx)?, key, token)
        .map_err(|_| ProviderError::R2MultipartInvalid)?;
    String::from_utf8(internal).map_err(|_| ProviderError::R2MultipartInvalid)
}

async fn r2_create_multipart(
    svc: Arc<TenantService>,
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_METADATA_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(err);
        }
    };
    let input: r2::wire::CreateMultipartRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(ProviderError::Protocol);
        }
    };
    let key = match r2::validate_object_key(&input.key) {
        Ok(key) => key.to_string(),
        Err(err) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(err);
        }
    };
    let options = match input.options.into_options() {
        Ok(options) => options,
        Err(err) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(err);
        }
    };
    let remaining = ctx.remaining();
    let result = match tokio::time::timeout(
        remaining,
        provider.create_multipart(&ctx, &key, &options),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(err);
        }
        Err(_) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(ProviderError::R2ProviderUnavailable);
        }
    };
    let bucket = match r2_bucket(&ctx) {
        Ok(bucket) => bucket,
        Err(err) => {
            mp_metric(&svc.tenant_id, "create", false);
            return r2_err(err);
        }
    };
    // Token expiry is decided by the provider (fs staging TTL is
    // configurable; S3 mirrors its bucket lifecycle), not a constant.
    let token = svc.signer.sign_r2_upload_until(
        ctx.tenant_id(),
        &bucket,
        &key,
        result.upload_id.as_bytes(),
        result.expires_at_ms,
    );
    mp_metric(&svc.tenant_id, "create", true);
    match serde_json::to_vec(&serde_json::json!({
        "key": key,
        "uploadId": token,
    })) {
        Ok(body) => json_response(body),
        Err(_) => r2_err(ProviderError::Internal),
    }
}

async fn r2_upload_part(
    svc: Arc<TenantService>,
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let fail = |err: ProviderError| {
        mp_metric(&svc.tenant_id, "part", false);
        r2_err(err)
    };
    let limit = R2_MAX_METADATA_BYTES + svc.r2_max_object_bytes as usize + 1024;
    let bytes =
        match tokio::time::timeout(svc.body_read_timeout, to_bytes(request.into_body(), limit))
            .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return fail(ProviderError::R2ObjectTooLarge),
            Err(_) => return fail(ProviderError::R2Overloaded),
        };
    if bytes.len() < 4 {
        return fail(ProviderError::Protocol);
    }
    let header_len =
        u32::from_be_bytes(bytes[..4].try_into().expect("length checked above")) as usize;
    let Some(header_end) = 4_usize.checked_add(header_len) else {
        return fail(ProviderError::Protocol);
    };
    if header_len > R2_MAX_METADATA_BYTES || header_end > bytes.len() {
        return fail(ProviderError::Protocol);
    }
    let header: r2::wire::UploadPartHeader = match serde_json::from_slice(&bytes[4..header_end]) {
        Ok(header) => header,
        Err(_) => return fail(ProviderError::Protocol),
    };
    let ssec_key = match header
        .ssec_key
        .as_deref()
        .map(r2::R2SsecKey::parse)
        .transpose()
    {
        Ok(key) => key,
        Err(err) => return fail(err),
    };
    let key = match r2::validate_object_key(&header.key) {
        Ok(key) => key.to_string(),
        Err(err) => return fail(err),
    };
    let part_number = match r2::wire::validate_part_number(header.part_number) {
        Ok(n) => n,
        Err(err) => return fail(err),
    };
    let internal = match verify_upload_token(&svc, &ctx, &key, &header.upload_id) {
        Ok(internal) => internal,
        Err(err) => return fail(err),
    };
    let body = &bytes[header_end..];
    if body.len() as u64 > svc.r2_max_object_bytes {
        return fail(ProviderError::R2ObjectTooLarge);
    }
    let remaining = ctx.remaining();
    match tokio::time::timeout(
        remaining,
        provider.upload_part(&ctx, &key, &internal, part_number, body, ssec_key.as_ref()),
    )
    .await
    {
        Ok(Ok(part)) => {
            mp_metric(&svc.tenant_id, "part", true);
            match serde_json::to_vec(&serde_json::json!({
                "partNumber": part.part_number,
                "etag": part.etag,
            })) {
                Ok(body) => json_response(body),
                Err(_) => r2_err(ProviderError::Internal),
            }
        }
        Ok(Err(err)) => fail(err),
        // The part may have committed (S3 UploadPart is idempotent per
        // partNumber) — honest unknown, not a fake unavailable.
        Err(_) => fail(ProviderError::R2ResultUnknown),
    }
}

async fn r2_complete_multipart(
    svc: Arc<TenantService>,
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_METADATA_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => {
            mp_metric(&svc.tenant_id, "complete", false);
            return r2_err(err);
        }
    };
    let input: r2::wire::CompleteMultipartRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => {
            mp_metric(&svc.tenant_id, "complete", false);
            return r2_err(ProviderError::Protocol);
        }
    };
    let key = match r2::validate_object_key(&input.key) {
        Ok(key) => key.to_string(),
        Err(err) => {
            mp_metric(&svc.tenant_id, "complete", false);
            return r2_err(err);
        }
    };
    let internal = match verify_upload_token(&svc, &ctx, &key, &input.upload_id) {
        Ok(internal) => internal,
        Err(err) => {
            mp_metric(&svc.tenant_id, "complete", false);
            return r2_err(err);
        }
    };
    // Outer timeout = request deadline + a small confirm grace: the S3
    // provider reserves the tail of `remaining` for its ambiguous-result
    // HEAD, so the grace lets an in-flight confirmation finish before we
    // declare the outcome unknown. It extends no downstream call — every
    // provider op is still bounded by `ctx.remaining()`.
    let remaining = ctx.remaining() + std::time::Duration::from_secs(2);
    match tokio::time::timeout(
        remaining,
        provider.complete_multipart(&ctx, &key, &internal, &input.parts),
    )
    .await
    {
        Ok(Ok(Some(metadata))) => {
            mp_metric(&svc.tenant_id, "complete", true);
            match serde_json::to_vec(&metadata) {
                Ok(body) => json_response(body),
                Err(_) => r2_err(ProviderError::Internal),
            }
        }
        // `None` is not a legitimate complete outcome — no conditional
        // precondition exists on multipart; contract violation → typed
        // internal, not a retryable fake-failure.
        Ok(Ok(None)) => {
            mp_metric(&svc.tenant_id, "complete", false);
            r2_err(ProviderError::Internal)
        }
        // Complete may already have committed on the backend — retrying
        // would then see `R2_MULTIPART_INVALID` (a false failure). Report
        // the honest unknown, same as `r2_put`'s timeout mapping.
        Err(_) => {
            mp_metric(&svc.tenant_id, "complete", false);
            r2_err(ProviderError::R2ResultUnknown)
        }
        Ok(Err(err)) => {
            mp_metric(&svc.tenant_id, "complete", false);
            r2_err(err)
        }
    }
}

async fn r2_abort_multipart(
    svc: Arc<TenantService>,
    provider: Arc<dyn R2Provider>,
    ctx: BindingContext,
    request: Request,
) -> Response<Body> {
    let bytes = match bounded_body(request, R2_MAX_METADATA_BYTES, BODY_READ_TIMEOUT).await {
        Ok(bytes) => bytes,
        Err(err) => {
            mp_metric(&svc.tenant_id, "abort", false);
            return r2_err(err);
        }
    };
    let input: r2::wire::AbortMultipartRequest = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => {
            mp_metric(&svc.tenant_id, "abort", false);
            return r2_err(ProviderError::Protocol);
        }
    };
    let key = match r2::validate_object_key(&input.key) {
        Ok(key) => key.to_string(),
        Err(err) => {
            mp_metric(&svc.tenant_id, "abort", false);
            return r2_err(err);
        }
    };
    let internal = match verify_upload_token(&svc, &ctx, &key, &input.upload_id) {
        Ok(internal) => internal,
        Err(err) => {
            mp_metric(&svc.tenant_id, "abort", false);
            return r2_err(err);
        }
    };
    let remaining = ctx.remaining();
    match tokio::time::timeout(remaining, provider.abort_multipart(&ctx, &key, &internal)).await {
        Ok(Ok(())) => {
            mp_metric(&svc.tenant_id, "abort", true);
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NO_CONTENT;
            response
        }
        Ok(Err(err)) => {
            mp_metric(&svc.tenant_id, "abort", false);
            r2_err(err)
        }
        // The abort may have committed — a retry would then see
        // `R2_MULTIPART_INVALID` and report a false failure.
        Err(_) => {
            mp_metric(&svc.tenant_id, "abort", false);
            r2_err(ProviderError::R2ResultUnknown)
        }
    }
}
