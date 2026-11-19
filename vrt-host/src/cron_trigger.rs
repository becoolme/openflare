//! `POST /v1/cron/trigger` — the cell-controller → vrt-host scheduled
//! trigger surface (T21/F1), served on the pod's loopback control
//! listener alongside `/readyz`.
//!
//! Contract:
//! - bounded JSON body `{tenantId, workerId, versionId, scheduleId,
//!   scheduledTime, cron}` plus the `x-vrt-cron-idempotency-key`
//!   header, which MUST equal
//!   `<workerId>:<versionId>:<scheduleId>:<scheduledTime>` — a key that
//!   disagrees with the body is a protocol error, never a dispatch;
//! - the key is deduped in a bounded LRU (4096 entries) scoped by the
//!   RESOLVED tenant runtime's identity (not the body's tenantId): a
//!   replay returns `{"delivered": false, "duplicate": true}` instead
//!   of re-firing — the host is the LAST at-most-once guard;
//! - delivery is an HTTP/1.1 POST to the worker's CRON FACADE socket
//!   (`sock-cron-<id>` → `facade-cron-<id>`), which invokes
//!   `env.TARGET.scheduled()` — the only workerd mechanism producing a
//!   real scheduled event (verified 2026-11-12: `service_binding_extra_handlers`
//!   is `$experimental`; tenants cannot declare it, so the event is
//!   unforgeable — ingress HTTP posts to `__vrt/scheduled` or
//!   `/cdn-cgi/handler/scheduled` hit `fetch`, never `scheduled`).
//!   The facade responds `{delivered, outcome, noRetry}`; anything but
//!   `outcome:"ok"` is reported as a failed delivery, never a false
//!   success (workerd resolves `.scheduled()` with
//!   `{outcome:"exception"}` on a throwing handler).
//! - unknown worker → 404, not-ready → 503, upstream failure → 502 —
//!   typed JSON, never a bare 500.
//!
//! `versionId` is part of the idempotency identity (slot authority),
//! not a dispatch pin: scheduled events reach the currently serving
//! generation, same as workerd's own cron semantics.

use crate::supervisor::{SwitchHoldParams, TenantRegistry};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Bounded idempotency-key dedupe set (insertion-order eviction).
const MAX_DEDUPE_KEYS: usize = 4096;
/// Upper bound for the trigger body — larger is a protocol violation.
const MAX_TRIGGER_BODY: usize = 4 * 1024;
/// Upstream (facade socket) timeout for the scheduled call — ~CF's
/// cron wall limit; the facade's own scheduled() await is the budget.
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);
/// CR1-r2: upper bound on a single request's fence advance. The
/// `leaseEpoch` value arrives over the trusted loopback channel, but a
/// confused peer could still send `u64::MAX` and permanently fence
/// every real trigger (monotonic `fetch_max`). A request claiming an
/// epoch more than this far above the current fence is rejected 409
/// `VRT_CRON_EPOCH_JUMP` and the fence is NOT advanced — per-request
/// harm is bounded. The very first observed epoch (fence == 0)
/// establishes the baseline unconditionally: a fresh host cannot
/// distinguish a large legitimate `leaseTransitions` from poison, and
/// a restart zeroes the fence anyway. Legitimate epoch sources
/// (k8s `leaseTransitions`, memory-store counters) advance one step
/// per real leader transition — a >1024 jump between consecutive
/// observed triggers means ≥1024 leader transitions with zero
/// observed triggers, which is itself suspicious; the rejection is
/// non-destructive (nothing consumed, fence unmoved).
const DEFAULT_MAX_EPOCH_JUMP: u64 = 1024;

/// Shared dedupe state — keyed by the idempotency header.
#[derive(Debug, Default)]
pub struct CronDedupe {
    // FIFO eviction: seq → key list for O(1)-ish expiry.
    keys: Mutex<HashMap<String, ()>>,
    order: Mutex<std::collections::VecDeque<String>>,
}

impl CronDedupe {
    /// True when `key` is new (and now recorded); false = replay.
    fn insert_if_new(&self, key: &str) -> bool {
        let mut keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        if keys.contains_key(key) {
            return false;
        }
        keys.insert(key.to_string(), ());
        let mut order = self.order.lock().unwrap_or_else(|e| e.into_inner());
        order.push_back(key.to_string());
        while order.len() > MAX_DEDUPE_KEYS {
            if let Some(old) = order.pop_front() {
                keys.remove(&old);
            }
        }
        true
    }
}

/// Shared handler state.
pub struct CronTriggerState {
    /// Tenant registry (worker → endpoint lookup).
    pub registry: Arc<TenantRegistry>,
    /// Dedupe LRU.
    pub dedupe: CronDedupe,
    /// Highest accepted lease epoch — a delayed trigger from a
    /// partitioned ex-leader carries a smaller epoch and is rejected.
    /// Monotonic: once `n` lands, epochs `< n` never serve again.
    pub max_epoch: std::sync::atomic::AtomicU64,
    /// CR1-r2: bound on a single request's fence advance (see
    /// [`DEFAULT_MAX_EPOCH_JUMP`]). Stored on state so tests can
    /// construct a router with a small bound.
    pub max_epoch_jump: u64,
    /// MIG-11: bounded serialized-switch hold ceilings — shared with
    /// ingress/queue/wfops via [`SwitchHoldParams`].
    pub hold: SwitchHoldParams,
}

/// Router for `POST /v1/cron/trigger` — merged onto the control router.
pub fn router(registry: Arc<TenantRegistry>) -> Router {
    router_with_epoch_jump(registry, DEFAULT_MAX_EPOCH_JUMP)
}

/// MIG-11: same as [`router`] with explicit serialized-switch hold
/// ceilings (production wires `[ingress].switchHold*`; default matches
/// the ingress defaults).
pub fn router_with_hold(registry: Arc<TenantRegistry>, hold: SwitchHoldParams) -> Router {
    router_inner(registry, DEFAULT_MAX_EPOCH_JUMP, hold)
}

/// CR1-r2: same as [`router`] with an explicit fence-advance bound
/// (tests construct a small bound; production uses the default).
pub fn router_with_epoch_jump(registry: Arc<TenantRegistry>, max_epoch_jump: u64) -> Router {
    router_inner(registry, max_epoch_jump, SwitchHoldParams::default())
}

fn router_inner(
    registry: Arc<TenantRegistry>,
    max_epoch_jump: u64,
    hold: SwitchHoldParams,
) -> Router {
    let state = Arc::new(CronTriggerState {
        registry,
        dedupe: CronDedupe::default(),
        max_epoch: std::sync::atomic::AtomicU64::new(0),
        max_epoch_jump,
        hold,
    });
    Router::new()
        .route("/v1/cron/trigger", post(trigger))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TriggerBody {
    tenant_id: String,
    worker_id: String,
    version_id: String,
    schedule_id: String,
    scheduled_time: String,
    cron: String,
    /// Lease epoch fencing token — the host rejects anything older
    /// than the newest epoch it has accepted (B9).
    lease_epoch: u64,
}

fn idem_key(b: &TriggerBody) -> String {
    format!(
        "{}:{}:{}:{}",
        b.worker_id, b.version_id, b.schedule_id, b.scheduled_time
    )
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

/// MIG-11: a trigger that hit the serialized-switch window and could
/// not wait it out — 503 + `VRT_CRON_SWITCHING` + `Retry-After`,
/// PROVABLY un-delivered (no facade POST was attempted, no dedupe key
/// consumed), so the cell-controller may safely retry inside the
/// minute's dispatch deadline.
fn switching() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        axum::Json(serde_json::json!({
            "delivered": false,
            "error": {
                "code": "VRT_CRON_SWITCHING",
                "message": "tenant generation switch in progress; retry shortly"
            }
        })),
    )
        .into_response()
}

async fn trigger(
    State(state): State<Arc<CronTriggerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > MAX_TRIGGER_BODY {
        return err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "VRT_CRON_OVERSIZED",
            "trigger body exceeds the bound",
        );
    }
    // MIG-11: declared bytes charged to the switch-hold ceilings.
    let body_len = body.len();
    let body: TriggerBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "VRT_CRON_MALFORMED",
                "trigger body is not a valid cron trigger",
            )
        }
    };
    let header_key = headers
        .get("x-vrt-cron-idempotency-key")
        .and_then(|v| v.to_str().ok());
    let expected = idem_key(&body);
    if header_key != Some(expected.as_str()) {
        return err(
            StatusCode::BAD_REQUEST,
            "VRT_CRON_KEY_MISMATCH",
            "idempotency key does not match the trigger identity",
        );
    }
    // CR1-r2: bound a single request's fence advance BEFORE the
    // stale check — a claimed epoch further than `max_epoch_jump`
    // above the current fence is rejected and does NOT move the
    // fence (poison cannot ratchet in one shot). The baseline
    // (fence == 0) is exempt: a fresh host cannot distinguish a
    // legitimate `leaseTransitions` value from poison.
    let fence_now = state.max_epoch.load(std::sync::atomic::Ordering::SeqCst);
    if fence_now != 0 && body.lease_epoch > fence_now.saturating_add(state.max_epoch_jump) {
        return err(
            StatusCode::CONFLICT,
            "VRT_CRON_EPOCH_JUMP",
            "lease epoch exceeds the bounded fence advance",
        );
    }
    // B9 fencing: reject triggers dispatched under a stale lease epoch.
    // fetch_max so a tie is still accepted (same leader re-sending).
    if state
        .max_epoch
        .fetch_max(body.lease_epoch, std::sync::atomic::Ordering::SeqCst)
        > body.lease_epoch
    {
        return err(
            StatusCode::CONFLICT,
            "VRT_CRON_STALE_EPOCH",
            "trigger carries a lease epoch older than the fence",
        );
    }
    // Worker lookup + readiness gate — a not-yet-serving generation
    // must never receive scheduled events.
    let Some(runtime) = state.registry.get(&body.tenant_id) else {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_CRON_UNKNOWN_WORKER",
            "tenant is not running on this pod",
        );
    };
    // Scale-to-zero: a trigger is activity (aborts an in-progress
    // hibernation commit with the traffic reason) and wakes a sleeping
    // tenant through the single-flight wake path. EVERY failure here is
    // before dedupe, so the idempotency key is NOT consumed and the
    // controller can retry the same key.
    runtime.touch();
    let st = runtime.state();
    if st.is_asleep() || st == crate::state::TenantState::Hibernating {
        use crate::hibernate::{WakeError, WakeSourceKind};
        if let Err(e) = runtime
            .await_wake(
                body_len,
                state.hold.max_reqs,
                state.hold.max_bytes,
                WakeSourceKind::Cron,
            )
            .await
        {
            // Rolling-upgrade compat: the cell-controller retries ONLY
            // `VRT_CRON_SWITCHING` / `VRT_CRON_WORKER_NOT_READY` (both
            // proven pre-dedupe). Every wake failure is likewise
            // un-dispatched, so it reuses the retryable code; the cause
            // is in the message and in the `source="cron"` wake metrics.
            return match e {
                WakeError::Timeout => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_CRON_WORKER_NOT_READY",
                    "tenant wake timed out",
                ),
                WakeError::Failed(_) => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_CRON_WORKER_NOT_READY",
                    "tenant wake failed",
                ),
                WakeError::Capacity => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_CRON_WORKER_NOT_READY",
                    "host wake capacity exhausted",
                ),
                WakeError::CircuitOpen => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_CRON_WORKER_NOT_READY",
                    "tenant circuit breaker is open",
                ),
            };
        }
    }
    // `can_serve` (Ready or SwitchFailed) — SwitchFailed keeps serving
    // the previous generation, same as ingress.
    if !runtime.state().can_serve() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_CRON_WORKER_NOT_READY",
            "tenant generation is not serving",
        );
    }
    // MIG-11: while the tenant's serialized-switch gate is active the
    // route map is EMPTY — a miss is "switching", not "unknown".
    // `resolve_through_switch` holds the wait (bounded by the shared
    // ingress ceilings). MIG-11-r2: a miss after the gate clears splits
    // typed — `NotDeclared` keeps the genuine-absence 404, `NoServing`
    // (launch failed / crashed, no generation serving) is a retryable
    // `VRT_CRON_WORKER_NOT_READY` 503 the cell-controller retries
    // inside the minute instead of losing the trigger as a fake 404.
    let slot = match runtime
        .resolve_through_switch(
            body_len,
            state.hold.budget,
            state.hold.max_reqs,
            state.hold.max_bytes,
            |rt| rt.cron_endpoint_slot(&body.worker_id),
        )
        .await
    {
        Ok(crate::supervisor::SwitchResolve::Hit(slot)) => slot,
        Ok(crate::supervisor::SwitchResolve::NotDeclared) => {
            return err(
                StatusCode::NOT_FOUND,
                "VRT_CRON_UNKNOWN_WORKER",
                "worker declares no cron schedules on this generation",
            )
        }
        Ok(crate::supervisor::SwitchResolve::NoServing) => {
            return err(
                StatusCode::SERVICE_UNAVAILABLE,
                "VRT_CRON_WORKER_NOT_READY",
                "tenant has no serving generation",
            )
        }
        Err(crate::supervisor::SwitchHoldReject::Timeout)
        | Err(crate::supervisor::SwitchHoldReject::Capacity) => return switching(),
    };
    // MIG-11: slot acquire BEFORE dedupe — every rejection above this
    // line is provably un-consumed (the idempotency slot is burned only
    // when dispatch is actually possible), so `VRT_CRON_SWITCHING` and
    // `VRT_CRON_WORKER_NOT_READY` are both safe for the controller to
    // retry with the same key inside the minute's deadline.
    let Some(_guard) = slot.acquire() else {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_CRON_WORKER_NOT_READY",
            "worker generation is draining",
        );
    };
    // Dedupe AFTER tenant/slot resolution: the dedupe scope is the
    // TRUSTED tenant identity taken from the slot's owning runtime —
    // never the request body's tenantId — so two tenants can never
    // dedupe each other's trigger, and a rejected (unknown/not-ready)
    // trigger cannot burn a key it never owned.
    let dedupe_key = format!("{}:{}", runtime.tenant_id(), expected);
    if !state.dedupe.insert_if_new(&dedupe_key) {
        crate::metrics::registry().inc(
            "vrt_worker_cron_invocations_total",
            &[("worker_id", &body.worker_id), ("outcome", "deduped")],
            1.0,
        );
        return axum::Json(serde_json::json!({
            "delivered": false,
            "duplicate": true
        }))
        .into_response();
    }
    let endpoint = slot.endpoint().clone();
    let payload = serde_json::json!({
        "scheduledTime": body.scheduled_time,
        "scheduleId": body.schedule_id,
        "cron": body.cron,
    });
    let req = hyper::Request::builder()
        .method(hyper::http::Method::POST)
        // origin-form path + explicit Host (qconsumer/wfops pattern) —
        // workerd's URL parser rejects the absolute-form
        // `http://vrt-cron.internal/` URI.
        .uri("/vrt/cron-trigger")
        .header(header::HOST, "vrt-cron.internal")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-vrt-cron-idempotency-key", expected.as_str())
        .body(
            Full::new(Bytes::from(payload.to_string()))
                .map_err(axum::Error::new)
                .boxed_unsync(),
        );

    let req = match req {
        Ok(r) => r,
        Err(_) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "VRT_CRON_INTERNAL",
                "trigger request build failed",
            )
        }
    };

    let dispatched = tokio::time::timeout(DISPATCH_TIMEOUT, async move {
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
        let bytes = resp.into_body().collect().await.map_err(|_| ())?.to_bytes();
        let json: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
        // Honesty: `delivered:true` ONLY when the facade reports
        // outcome:"ok" — workerd resolves scheduled() with
        // {outcome:"exception"} on a throwing handler, and the facade
        // surfaces that as delivered:false.
        let delivered = status.is_success()
            && json
                .as_ref()
                .and_then(|j| j.get("delivered"))
                .and_then(|d| d.as_bool())
                == Some(true);
        let outcome = json
            .as_ref()
            .and_then(|j| j.get("outcome"))
            .and_then(|o| o.as_str())
            .unwrap_or("unparseable")
            .to_string();
        Ok::<_, ()>((delivered, outcome))
    })
    .await;

    match dispatched {
        Ok(Ok((true, outcome))) => {
            crate::metrics::registry().inc(
                "vrt_worker_cron_invocations_total",
                &[("worker_id", &body.worker_id), ("outcome", "delivered")],
                1.0,
            );
            axum::Json(serde_json::json!({"delivered": true, "outcome": outcome})).into_response()
        }
        Ok(Ok((false, outcome))) => {
            let reason = match outcome.as_str() {
                "exception" => "exception",
                _ => "failed",
            };
            crate::metrics::registry().inc(
                "vrt_worker_cron_invocations_total",
                &[("worker_id", &body.worker_id), ("outcome", reason)],
                1.0,
            );
            tracing::info!(
                tenant = %body.tenant_id,
                worker = %body.worker_id,
                outcome = %outcome,
                "cron scheduled() delivery reported failure by facade"
            );
            (
                StatusCode::BAD_GATEWAY,
                axum::Json(serde_json::json!({
                    "delivered": false,
                    "outcome": outcome,
                    "error": {
                        "code": "VRT_CRON_DELIVERY_FAILED",
                        "message": "scheduled() handler reported a non-ok outcome",
                    }
                })),
            )
                .into_response()
        }
        _ => {
            crate::metrics::registry().inc(
                "vrt_worker_cron_invocations_total",
                &[("worker_id", &body.worker_id), ("outcome", "failed")],
                1.0,
            );
            err(
                StatusCode::BAD_GATEWAY,
                "VRT_CRON_DELIVERY_FAILED",
                "worker did not accept the scheduled event",
            )
        }
    }
}
