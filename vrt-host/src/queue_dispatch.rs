//! `POST /v1/queue/dispatch` — the cell-controller → vrt-host queue
//! consumer delivery surface (T37b), served on the pod's loopback
//! control listener alongside `/v1/cron/trigger` and `/readyz`.
//!
//! Contract:
//! - bounded JSON body `{tenantId, workerId, dispatch}` where
//!   `dispatch` is the opaque facade payload `{queue, messages[],
//!   metadata?}` forwarded verbatim to `sock-qconsumer-<workerId>`
//!   as `POST /vrt/queue-dispatch`;
//! - worker without `queueConsumers` → 404, generation draining or
//!   not ready → 503, upstream failure → 502, dispatch over the
//!   deadline → 504 — typed JSON, never a bare 500;
//! - `dispatchEpoch` rides in `x-vrt-dispatch-epoch` for log
//!   correlation only — the message-lease pair (claim lease_id) is
//!   the actual fencing mechanism (a superseded dispatcher's in-flight
//!   delivery is still CORRECT: it holds message leases the successor
//!   cannot see until expiry; settlement is lease-gated at vrt-sql).
//!
//! The facade on the far end reads the body before any return
//! (facade-qconsumer.ts, T37r2 invariant) and answers a QueueResult
//! envelope; this endpoint passes its bytes through untouched.

use crate::supervisor::{SwitchHoldParams, TenantRegistry};
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

/// Upper bound for the dispatch envelope — T37b-r: dispatcher claim
/// budget is 4MB RAW body bytes per lane (byte-bounded claim, not
/// count-only), so the worst legal batch is (4MB + one oversize
/// 128KB msg) × 2 hex + tenant/worker wrapper + per-message fields.
const MAX_DISPATCH_BODY: usize = 9_000_000;
/// RV-R8 #5: the consumer-facade answer is a small QueueResult JSON
/// envelope — hard-cap the upstream body so a misbehaving facade can
/// never grow the proxy buffer. Fail closed with a typed 502.
const UPSTREAM_MAX_BODY: usize = 1024 * 1024;
/// Upstream deadline — above the facade's internal 55s delivery
/// timeout so a facade timeout result still reaches the dispatcher;
/// below the dispatch message-lease budget enforced by the caller.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(70);

/// Facade delivery budget (mirror of `DELIVERY_TIMEOUT_MS` in
/// facade-qconsumer.ts). Wake time is deducted from it.
const DELIVERY_BUDGET_MS: u64 = 55_000;
/// Floor: less than this left after a wake → not worth delivering.
const MIN_DELIVERY_BUDGET_MS: u64 = 1_000;
/// Internal header carrying the remaining budget to the facade. Always
/// built by the host on a fresh request — the controller payload is the
/// body only, so no external value can reach it.
const BUDGET_HEADER: &str = "x-vrt-delivery-budget-ms";

/// Shared handler state.
pub struct QueueDispatchState {
    /// Tenant registry (worker → qconsumer endpoint lookup).
    pub registry: Arc<TenantRegistry>,
    /// MIG-11: bounded serialized-switch hold ceilings — shared with
    /// ingress/cron/wfops via [`SwitchHoldParams`].
    pub hold: SwitchHoldParams,
    /// T37b-r5: dispatch log rate limiting (at most one per key per 10s) --
    /// the dispatcher polling path would be flooded by unbounded info; rejection paths are counted separately.
    log_throttle: std::sync::Mutex<std::collections::BTreeMap<String, std::time::Instant>>,
}

impl QueueDispatchState {
    fn log_allow(&self, key: &str) -> bool {
        const IV: Duration = Duration::from_secs(10);
        let now = std::time::Instant::now();
        let mut m = self.log_throttle.lock().unwrap_or_else(|e| e.into_inner());
        match m.get_mut(key) {
            Some(t) if now.duration_since(*t) < IV => false,
            Some(t) => {
                *t = now;
                true
            }
            None => {
                m.insert(key.to_string(), now);
                true
            }
        }
    }
}

/// Router for `POST /v1/queue/dispatch` — merged onto the control router.
pub fn router(registry: Arc<TenantRegistry>) -> Router {
    router_with_hold(registry, SwitchHoldParams::default())
}

/// MIG-11: same as [`router`] with explicit serialized-switch hold
/// ceilings (production wires `[ingress].switchHold*`; default matches
/// the ingress defaults).
pub fn router_with_hold(registry: Arc<TenantRegistry>, hold: SwitchHoldParams) -> Router {
    Router::new()
        .route("/v1/queue/dispatch", post(dispatch))
        // RV-R8 #5: explicit body bound (was axum's 2MiB default —
        // which silently rejected legal dispatches above 2MiB AND was
        // an untyped rejection). The handler reads via `Limited` +
        // typed 413; this layer is the fail-closed backstop if the
        // extractor ever regresses to a pre-buffered `Bytes`.
        .layer(DefaultBodyLimit::max(MAX_DISPATCH_BODY + 1))
        .with_state(Arc::new(QueueDispatchState {
            registry,
            hold,
            log_throttle: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DispatchBody {
    tenant_id: String,
    worker_id: String,
    /// Opaque facade payload — forwarded verbatim; validated only for
    /// presence/shape (it must be a JSON object).
    dispatch: serde_json::Value,
}

/// T37b-r6: upstream response body diagnostic snippet -- <=256B printable ASCII (others '.').
/// The facade/upstream error body does not contain the message body; still sanitized as untrusted input, with
/// the same rule as the dispatcher-side `snippet()`.
fn snippet(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(256)
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}

fn err(status: StatusCode, code: &'static str, msg: &'static str) -> Response {
    (
        status,
        axum::Json(serde_json::json!({
            "delivered": false,
            "error": { "code": code, "message": msg }
        })),
    )
        .into_response()
}

/// Pre-delivery wake rejection: 503 + `Retry-After: 1`, same body shape
/// as every other reject; rate-limited warn.
fn wake_reject(state: &QueueDispatchState, code: &'static str, msg: &'static str) -> Response {
    if state.log_allow(&format!("reject:{code}")) {
        tracing::warn!(code = code, "queue dispatch rejected");
    }
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        axum::Json(serde_json::json!({
            "delivered": false,
            "error": { "code": code, "message": msg }
        })),
    )
        .into_response()
}

async fn dispatch(State(state): State<Arc<QueueDispatchState>>, request: Request) -> Response {
    let t0 = std::time::Instant::now();
    // T37b-r5: log a rate-limited warn on every rejection -- the typed code is directly diagnosable;
    // a rejection may happen before body parsing (unknown tenant) -> logged separately.
    macro_rules! reject {
        ($status:expr, $code:expr, $msg:expr) => {{
            if state.log_allow(&format!("reject:{}", $code)) {
                tracing::warn!(code = $code, "queue dispatch rejected");
            }
            return err($status, $code, $msg);
        }};
    }
    // RV-R8 #5: fail closed on size BEFORE buffering — a declared
    // length over the bound rejects without reading, and `Limited`
    // hard-stops a lying/chunked body mid-stream. Both typed.
    let declared_too_big = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n as usize > MAX_DISPATCH_BODY);
    if declared_too_big {
        reject!(
            StatusCode::PAYLOAD_TOO_LARGE,
            "VRT_QDISP_OVERSIZED",
            "dispatch body exceeds the bound"
        );
    }
    let body = match http_body_util::Limited::new(request.into_body(), MAX_DISPATCH_BODY)
        .collect()
        .await
    {
        Ok(c) => c.to_bytes(),
        Err(e) if e
            .downcast_ref::<http_body_util::LengthLimitError>()
            .is_some() =>
        {
            reject!(
                StatusCode::PAYLOAD_TOO_LARGE,
                "VRT_QDISP_OVERSIZED",
                "dispatch body exceeds the bound"
            );
        }
        Err(_) => {
            reject!(
                StatusCode::BAD_REQUEST,
                "VRT_QDISP_MALFORMED",
                "dispatch body is not a valid queue dispatch"
            );
        }
    };
    // MIG-11: declared bytes charged to the switch-hold ceilings.
    let body_len = body.len();
    let body: DispatchBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => {
            reject!(
                StatusCode::BAD_REQUEST,
                "VRT_QDISP_MALFORMED",
                "dispatch body is not a valid queue dispatch"
            );
        }
    };
    if !body.dispatch.is_object() {
        reject!(
            StatusCode::BAD_REQUEST,
            "VRT_QDISP_MALFORMED",
            "dispatch payload must be a json object"
        );
    }
    let queue_name = body
        .dispatch
        .get("queue")
        .and_then(|q| q.as_str())
        .unwrap_or("")
        .to_string();
    let msg_count = body
        .dispatch
        .get("messages")
        .and_then(|m| m.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let Some(runtime) = state.registry.get(&body.tenant_id) else {
        reject!(
            StatusCode::NOT_FOUND,
            "VRT_QDISP_UNKNOWN_WORKER",
            "tenant is not running on this pod"
        );
    };
    // Scale-to-zero: a dispatch is activity and wakes a sleeping
    // tenant. Every failure here is BEFORE delivery (the consumer never
    // ran), so it is a retryable platform error with attempts intact.
    runtime.touch();
    let mut wake_elapsed = Duration::ZERO;
    let st = runtime.state();
    if st.is_asleep() || st == crate::state::TenantState::Hibernating {
        use crate::hibernate::{WakeError, WakeSourceKind};
        let w0 = std::time::Instant::now();
        let woke = runtime
            .await_wake(
                body_len,
                state.hold.max_reqs,
                state.hold.max_bytes,
                WakeSourceKind::Queue,
            )
            .await;
        wake_elapsed = w0.elapsed();
        if let Err(e) = woke {
            let (code, msg) = match e {
                WakeError::Timeout => ("VRT_QDISP_WAKE_TIMEOUT", "tenant wake timed out"),
                WakeError::Failed(_) => ("VRT_QDISP_WAKE_FAILED", "tenant wake failed"),
                WakeError::Capacity => {
                    ("VRT_QDISP_WAKE_CAPACITY", "host wake capacity exhausted")
                }
                WakeError::CircuitOpen => (
                    "VRT_QDISP_WORKER_NOT_READY",
                    "tenant circuit breaker is open",
                ),
            };
            return wake_reject(&state, code, msg);
        }
        tracing::info!(
            tenant = %body.tenant_id,
            worker = %body.worker_id,
            wake_ms = wake_elapsed.as_millis() as u64,
            "queue dispatch woke hibernated tenant"
        );
    }
    let wake_ms = wake_elapsed.as_millis() as u64;
    if DELIVERY_BUDGET_MS.saturating_sub(wake_ms) < MIN_DELIVERY_BUDGET_MS {
        return wake_reject(
            &state,
            "VRT_QDISP_WAKE_TIMEOUT",
            "tenant wake left no delivery budget",
        );
    }
    let delivery_budget_ms = DELIVERY_BUDGET_MS - wake_ms;
    // T37b-r2: `can_serve` — SwitchFailed still serves the OLD
    // generation's consumers; Ready-only would halt queue dispatch
    // tenant-wide on a bad spec.
    if !runtime.state().can_serve() {
        reject!(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_QDISP_WORKER_NOT_READY",
            "tenant generation is not serving"
        );
    }
    // MIG-11: while the tenant's serialized-switch gate is active the
    // route map is EMPTY — a miss is "switching", not "no consumer".
    // `resolve_through_switch` holds the wait (bounded by the shared
    // ingress ceilings). MIG-11-r2: a miss after the gate clears splits
    // typed — `NotDeclared` keeps the genuine-absence 404, `NoServing`
    // maps onto `VRT_QDISP_WORKER_NOT_READY` — a platform error to the
    // dispatcher, attempts not consumed (same class as
    // `VRT_QDISP_SWITCHING`, never a fake 404).
    let slot = match runtime
        .resolve_through_switch(
            body_len,
            state.hold.budget,
            state.hold.max_reqs,
            state.hold.max_bytes,
            |rt| rt.qconsumer_endpoint_slot(&body.worker_id),
        )
        .await
    {
        Ok(crate::supervisor::SwitchResolve::Hit(slot)) => slot,
        Ok(crate::supervisor::SwitchResolve::NotDeclared) => {
            reject!(
                StatusCode::NOT_FOUND,
                "VRT_QDISP_NO_CONSUMER",
                "worker has no queue consumer on this generation"
            );
        }
        Ok(crate::supervisor::SwitchResolve::NoServing) => {
            reject!(
                StatusCode::SERVICE_UNAVAILABLE,
                "VRT_QDISP_WORKER_NOT_READY",
                "tenant has no serving generation"
            );
        }
        Err(crate::supervisor::SwitchHoldReject::Timeout)
        | Err(crate::supervisor::SwitchHoldReject::Capacity) => {
            if state.log_allow("reject:VRT_QDISP_SWITCHING") {
                tracing::warn!(code = "VRT_QDISP_SWITCHING", "queue dispatch rejected");
            }
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
                axum::Json(serde_json::json!({
                    "delivered": false,
                    "error": {
                        "code": "VRT_QDISP_SWITCHING",
                        "message": "tenant generation switch in progress; retry shortly"
                    }
                })),
            )
                .into_response();
        }
    };
    let Some(_guard) = slot.acquire() else {
        reject!(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_QDISP_WORKER_NOT_READY",
            "worker generation is draining"
        );
    };
    let endpoint = slot.endpoint().clone();
    let payload = body.dispatch.to_string();
    // T37b-r6: must be origin-form + Host -- the ingress upstream convention
    // (same shape as ingress.rs). Previously an absolute-form URI with no Host:
    // the workerd socket service could not construct request.url -> bare-text "Bad
    // Request" 400 (the dispatcher side reported malformed).
    let req = match hyper::Request::builder()
        .method(hyper::http::Method::POST)
        .uri("/vrt/queue-dispatch")
        .header(header::HOST, "vrt-queue.internal")
        .header(header::CONTENT_TYPE, "application/json")
        .header(BUDGET_HEADER, delivery_budget_ms.to_string())
        .body(
            Full::new(Bytes::from(payload))
                .map_err(axum::Error::new)
                .boxed_unsync(),
        ) {
        Ok(r) => r,
        Err(_) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "VRT_QDISP_INTERNAL",
                "dispatch request build failed",
            )
        }
    };

    // Wake time is charged to the upstream cap too, keeping the same
    // 15s margin above the (shrunk) facade budget so wake + delivery
    // never outlives the controller's message lease.
    let upstream_timeout = UPSTREAM_TIMEOUT.saturating_sub(wake_elapsed);
    let upstream = tokio::time::timeout(upstream_timeout, async move {
        let io = endpoint.connect().await.map_err(|_| ())?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake::<
            _,
            http_body_util::combinators::UnsyncBoxBody<Bytes, axum::Error>,
        >(hyper_util::rt::TokioIo::new(io))
        .await
        .map_err(|_| ())?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        sender.ready().await.map_err(|_| ())?;
        let resp = sender.send_request(req).await.map_err(|_| ())?;
        let status = resp.status();
        // RV-R8 #5: bound the facade answer — Err(true) = over cap.
        let body = match http_body_util::Limited::new(resp.into_body(), UPSTREAM_MAX_BODY)
            .collect()
            .await
        {
            Ok(c) => Ok(c.to_bytes()),
            Err(e) => Err(e
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()),
        };
        Ok::<_, ()>((status, body))
    })
    .await;

    match upstream {
        Ok(Ok((_, Err(true)))) => {
            crate::metrics::registry().inc(
                "vrt_worker_queue_dispatch_total",
                &[("worker_id", &body.worker_id), ("outcome", "failed")],
                1.0,
            );
            if state.log_allow(&format!("fail:{}:{}", body.tenant_id, body.worker_id)) {
                tracing::warn!(
                    tenant = %body.tenant_id,
                    worker = %body.worker_id,
                    queue = %queue_name,
                    n = msg_count,
                    cap = UPSTREAM_MAX_BODY,
                    "queue dispatch upstream response exceeded byte cap"
                );
            }
            err(
                StatusCode::BAD_GATEWAY,
                "VRT_QDISP_UPSTREAM_OVERSIZED",
                "consumer facade response exceeded the bound",
            )
        }
        Ok(Ok((status, Ok(bytes)))) if status.is_success() => {
            crate::metrics::registry().inc(
                "vrt_worker_queue_dispatch_total",
                &[("worker_id", &body.worker_id), ("outcome", "delivered")],
                1.0,
            );
            // T37b-r5: one rate-limited info per dispatch -- tenant/worker/queue/
            // message count/status code/duration, making the delivery chain observable.
            if state.log_allow(&format!("ok:{}:{}", body.tenant_id, body.worker_id)) {
                tracing::info!(
                    tenant = %body.tenant_id,
                    worker = %body.worker_id,
                    queue = %queue_name,
                    n = msg_count,
                    status = status.as_u16(),
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "queue dispatch delivered"
                );
            }
            (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
        }
        Ok(Ok((status, Ok(bytes)))) => {
            crate::metrics::registry().inc(
                "vrt_worker_queue_dispatch_total",
                &[
                    ("worker_id", &body.worker_id),
                    ("outcome", "facade_rejected"),
                ],
                1.0,
            );
            if state.log_allow(&format!("rej:{}:{}", body.tenant_id, body.worker_id)) {
                tracing::warn!(
                    tenant = %body.tenant_id,
                    worker = %body.worker_id,
                    queue = %queue_name,
                    n = msg_count,
                    status = status.as_u16(),
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    body = %snippet(&bytes),
                    "queue dispatch rejected by consumer facade"
                );
            }
            // A typed facade rejection (400/413/503) is passed through —
            // the dispatcher settles on it as a delivery failure.
            (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
        }
        Ok(Ok((_, Err(false)))) | Ok(Err(())) => {
            crate::metrics::registry().inc(
                "vrt_worker_queue_dispatch_total",
                &[("worker_id", &body.worker_id), ("outcome", "failed")],
                1.0,
            );
            if state.log_allow(&format!("fail:{}:{}", body.tenant_id, body.worker_id)) {
                tracing::warn!(
                    tenant = %body.tenant_id,
                    worker = %body.worker_id,
                    queue = %queue_name,
                    n = msg_count,
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "queue dispatch upstream connect/handshake failed"
                );
            }
            err(
                StatusCode::BAD_GATEWAY,
                "VRT_QDISP_DELIVERY_FAILED",
                "consumer facade did not accept the dispatch",
            )
        }
        Err(_) => {
            crate::metrics::registry().inc(
                "vrt_worker_queue_dispatch_total",
                &[("worker_id", &body.worker_id), ("outcome", "timeout")],
                1.0,
            );
            if state.log_allow(&format!("tmo:{}:{}", body.tenant_id, body.worker_id)) {
                tracing::warn!(
                    tenant = %body.tenant_id,
                    worker = %body.worker_id,
                    queue = %queue_name,
                    n = msg_count,
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    "queue dispatch upstream deadline exceeded"
                );
            }
            err(
                StatusCode::GATEWAY_TIMEOUT,
                "VRT_QDISP_TIMEOUT",
                "consumer facade dispatch deadline exceeded",
            )
        }
    }
}
