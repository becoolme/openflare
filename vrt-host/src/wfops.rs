//! W3: `POST /v1/wfops` — platform-internal workflow-op bridge.
//!
//! The cell-controller's wfops executor calls this host-local endpoint;
//! the request is forwarded to `sock-wfops-<workerId>` →
//! `facade-wfops-<workerId>` inside the tenant workerd.
//!
//! W3-r2 trust boundary: the caller NEVER supplies a token. This hop
//! resolves `(workerId, op.workflow)` against the serving spec's
//! `wfop_tokens` map — material rendered from the signed desired
//! surface — and injects `x-vrt-wfop-token` itself. A workflow name
//! absent from the spec map is `workflow.undeclared` (the
//! signed-desired authority check); the facade re-verifies the header
//! against its own env table, so a direct socket call without the
//! hop still fails closed with 403.

use crate::supervisor::{SwitchHoldParams, TenantRegistry};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

/// Op envelope bound — workflow params are server-capped at 64KiB; the
/// wrapper (ids/token/op metadata) is small. 96KiB is generous.
const MAX_WFOPS_BODY: usize = 96 * 1024;
/// Facade deadline — engine calls are fast; 20s also covers an
/// instance-init `create` round-trip under load.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(20);
/// Total ceiling (wake + forward) once a wake happened. The controller
/// gives one wfops call 15s and blindly retries `request timeout` /
/// transport errors, which would execute the op twice after a wake. So
/// after a wake the host must answer (typed) before 15s: the forward
/// gets `WOKEN_UPSTREAM_CEILING - wake_elapsed`.
const WOKEN_UPSTREAM_CEILING: Duration = Duration::from_secs(14);
/// Minimum forward budget after a wake; below it the op is NOT forwarded.
const MIN_WOKEN_FORWARD: Duration = Duration::from_secs(1);
/// Upstream response cap — the facade may carry a workflow `output`
/// blob; bounded here and re-bounded (with truncation) by the executor.
const MAX_UPSTREAM_BODY: usize = 256 * 1024;

pub struct WfopsState {
    pub registry: Arc<TenantRegistry>,
    /// MIG-11: bounded serialized-switch hold ceilings — shared with
    /// ingress/cron/queue via [`SwitchHoldParams`].
    pub hold: SwitchHoldParams,
}

/// Router for `POST /v1/wfops` — merged onto the control router.
pub fn router(registry: Arc<TenantRegistry>) -> Router {
    router_with_hold(registry, SwitchHoldParams::default())
}

/// MIG-11: same as [`router`] with explicit serialized-switch hold
/// ceilings (production wires `[ingress].switchHold*`; default matches
/// the ingress defaults).
pub fn router_with_hold(registry: Arc<TenantRegistry>, hold: SwitchHoldParams) -> Router {
    Router::new()
        .route("/v1/wfops", post(dispatch))
        .with_state(Arc::new(WfopsState { registry, hold }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WfopsBody {
    tenant_id: String,
    worker_id: String,
    /// Opaque op payload — `{op, workflow, instanceId?, action?, params?}`.
    /// `deny_unknown_fields` rejects a smuggled `token`: the hop owns
    /// token injection; a caller-supplied one can never arrive.
    op: serde_json::Value,
}

fn err(status: StatusCode, code: &'static str, msg: &'static str) -> Response {
    (
        status,
        axum::Json(serde_json::json!({ "error": { "code": code, "message": msg } })),
    )
        .into_response()
}

/// 503 + `Retry-After: 1` with the typed error body.
fn retryable(code: &'static str, msg: &'static str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        axum::Json(serde_json::json!({ "error": { "code": code, "message": msg } })),
    )
        .into_response()
}

async fn dispatch(State(state): State<Arc<WfopsState>>, body: Bytes) -> Response {
    if body.len() > MAX_WFOPS_BODY {
        return err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "VRT_WFOPS_OVERSIZED",
            "wfops body exceeds the bound",
        );
    }
    // MIG-11: declared bytes charged to the switch-hold ceilings.
    let body_len = body.len();
    let body: WfopsBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "VRT_WFOPS_MALFORMED",
                "wfops body is not a valid op envelope",
            )
        }
    };
    if !body.op.is_object() {
        return err(
            StatusCode::BAD_REQUEST,
            "VRT_WFOPS_MALFORMED",
            "wfops op must be a json object",
        );
    }
    let Some(wf_name) = body.op.get("workflow").and_then(|v| v.as_str()) else {
        return err(
            StatusCode::BAD_REQUEST,
            "VRT_WFOPS_MALFORMED",
            "wfops op must name a workflow",
        );
    };
    let wf_name = wf_name.to_string();
    let Some(runtime) = state.registry.get(&body.tenant_id) else {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_WFOPS_UNKNOWN_WORKER",
            "tenant is not running on this pod",
        );
    };
    // Scale-to-zero: a workflow op is activity and wakes a sleeping
    // tenant BEFORE `can_serve`, the spec lookups and routing, so a
    // workflow added while asleep (lazy Apply) is in the serving spec.
    // Failures are before forwarding (nothing executed): typed 503.
    runtime.touch();
    let mut woke_after: Option<Duration> = None;
    let st = runtime.state();
    if st.is_asleep() || st == crate::state::TenantState::Hibernating {
        use crate::hibernate::{WakeError, WakeSourceKind};
        let w0 = std::time::Instant::now();
        let woke = runtime
            .await_wake(
                body_len,
                state.hold.max_reqs,
                state.hold.max_bytes,
                WakeSourceKind::Workflow,
            )
            .await;
        let elapsed = w0.elapsed();
        if let Err(e) = woke {
            let (code, msg) = match e {
                WakeError::Timeout => ("VRT_WFOPS_WAKE_TIMEOUT", "tenant wake timed out"),
                WakeError::Failed(_) => ("VRT_WFOPS_WAKE_FAILED", "tenant wake failed"),
                WakeError::Capacity => {
                    ("VRT_WFOPS_WAKE_CAPACITY", "host wake capacity exhausted")
                }
                WakeError::CircuitOpen => (
                    "VRT_WFOPS_WORKER_NOT_READY",
                    "tenant circuit breaker is open",
                ),
            };
            return retryable(code, msg);
        }
        tracing::info!(
            tenant = %body.tenant_id,
            worker = %body.worker_id,
            wake_ms = elapsed.as_millis() as u64,
            "wfops woke hibernated tenant"
        );
        woke_after = Some(elapsed);
    }
    let upstream_timeout = match woke_after {
        None => UPSTREAM_TIMEOUT,
        Some(el) => {
            let left = WOKEN_UPSTREAM_CEILING.saturating_sub(el);
            if left < MIN_WOKEN_FORWARD {
                return retryable(
                    "VRT_WFOPS_WAKE_TIMEOUT",
                    "tenant wake left no budget for the op",
                );
            }
            left
        }
    };
    if !runtime.state().can_serve() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_WFOPS_WORKER_NOT_READY",
            "tenant generation is not serving",
        );
    }
    // MIG-11: while the tenant's serialized-switch gate is active the
    // route map is EMPTY — a miss is "switching", not "no workflows".
    // `resolve_through_switch` holds the wait (bounded by the shared
    // ingress ceilings). MIG-11-r2: a miss after the gate clears splits
    // typed — `NotDeclared` keeps the genuine-absence 404, `NoServing`
    // (launch failed / crashed, no generation serving) maps onto the
    // existing `VRT_WFOPS_WORKER_NOT_READY` 503 instead of a fake
    // `VRT_WFOPS_NO_WORKFLOWS`.
    let slot = match runtime
        .resolve_through_switch(
            body_len,
            state.hold.budget,
            state.hold.max_reqs,
            state.hold.max_bytes,
            |rt| rt.wfops_endpoint_slot(&body.worker_id),
        )
        .await
    {
        Ok(crate::supervisor::SwitchResolve::Hit(slot)) => slot,
        Ok(crate::supervisor::SwitchResolve::NotDeclared) => {
            return err(
                StatusCode::NOT_FOUND,
                "VRT_WFOPS_NO_WORKFLOWS",
                "worker has no wfops facade on this generation",
            )
        }
        Ok(crate::supervisor::SwitchResolve::NoServing) => {
            return err(
                StatusCode::SERVICE_UNAVAILABLE,
                "VRT_WFOPS_WORKER_NOT_READY",
                "tenant has no serving generation",
            )
        }
        Err(crate::supervisor::SwitchHoldReject::Timeout)
        | Err(crate::supervisor::SwitchHoldReject::Capacity) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "VRT_WFOPS_SWITCHING",
                        "message": "tenant generation switch in progress; retry shortly"
                    }
                })),
            )
                .into_response();
        }
    };
    // W3-r2: the workflow must exist in the serving spec's token map —
    // rendered from the signed desired — or the call is undeclared.
    // This is the authoritative check; the executor's own desired
    // lookup and the facade's env table are defense in depth.
    let Some(token) = runtime.wfop_token(&body.worker_id, &wf_name) else {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_WFOPS_UNDECLARED",
            "workflow.undeclared",
        );
    };
    let Some(_guard) = slot.acquire() else {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_WFOPS_WORKER_NOT_READY",
            "worker generation is draining",
        );
    };
    let endpoint = slot.endpoint().clone();
    let payload = body.op.to_string();
    let req = match hyper::Request::builder()
        .method(hyper::http::Method::POST)
        .uri("/vrt/wfops")
        .header(header::HOST, "vrt-wfops.internal")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-vrt-wfop-token", &token)
        .body(
            Full::new(Bytes::from(payload))
                .map_err(axum::Error::new)
                .boxed_unsync(),
        ) {
        Ok(r) => r,
        Err(_) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "VRT_WFOPS_INTERNAL",
                "wfops request build failed",
            )
        }
    };

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
        let body = resp.into_body().collect().await.map_err(|_| ())?.to_bytes();
        Ok::<_, ()>((status, body))
    })
    .await;

    match upstream {
        Ok(Ok((status, bytes))) => {
            let truncated = bytes.len() > MAX_UPSTREAM_BODY;
            let bytes = if truncated {
                // Facade exceeded the cap — never proxy an unbounded
                // body downstream; the executor marks `truncated`.
                Bytes::from_static(br#"{"error":"upstream body too large","oversize":true}"#)
            } else {
                bytes
            };
            (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
        }
        Ok(Err(())) => err(
            StatusCode::BAD_GATEWAY,
            "VRT_WFOPS_DELIVERY_FAILED",
            "wfops facade did not accept the call",
        ),
        Err(_) => err(
            StatusCode::GATEWAY_TIMEOUT,
            "VRT_WFOPS_TIMEOUT",
            "wfops facade call timed out",
        ),
    }
}
