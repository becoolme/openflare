//! Cell-controller -> vrt-host Durable Object alarm wake and sleep gate.
//!
//! `POST /v1/do-alarm/wake` starts a hibernated tenant so workerd's own
//! `metadata.sqlite` `_cf_ALARM` scheduler can deliver the overdue alarm. It
//! deliberately does not rewrite `_cf_ALARM`/`_cf_METADATA`: workerd owns the
//! alarm state, and re-arming is the handler's job.
//!
//! `POST /v1/do-alarm/gate` carries the cell alarm-index verdict
//! (`safe`/`unsafe` + TTL). The host trusts it only until the TTL expires; a
//! missing or expired gate keeps DO tenants resident (fail closed).
//!
//! Both routes live on the loopback control listener and resolve class paths
//! only through the serving tenant's `DoStorageView`, never from request data.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

use crate::hibernate::{WakeError, WakeSourceKind};
use crate::supervisor::{SwitchHoldParams, TenantRegistry};

/// Wake body bound.
const MAX_WAKE_BODY: usize = 4 * 1024;
/// Gate body bound.
const MAX_GATE_BODY: usize = 4 * 1024;
/// Longest TTL the host accepts for a gate (the controller refreshes it).
const MAX_GATE_TTL_MS: u64 = 300_000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WakeBody {
    tenant_id: String,
    worker_id: String,
    class_key: String,
    object_id: String,
    #[serde(default)]
    at_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GateBody {
    tenant_id: String,
    safe: bool,
    ttl_ms: u64,
}

fn valid_tenant(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn valid_worker(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 160
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn valid_class_key(key: &str) -> bool {
    key.len() == 64
        && key
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn valid_object_id(id: &str) -> bool {
    id.len() == 64
        && id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn err(status: StatusCode, code: &'static str, msg: &'static str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "delivered": false,
            "error": { "code": code, "message": msg }
        })),
    )
        .into_response()
}

pub fn router(registry: Arc<TenantRegistry>, hold: SwitchHoldParams) -> axum::Router {
    axum::Router::new()
        .route("/v1/do-alarm/wake", post(wake))
        .route("/v1/do-alarm/gate", post(gate))
        .with_state(Arc::new(AlarmState { registry, hold }))
}

struct AlarmState {
    registry: Arc<TenantRegistry>,
    hold: SwitchHoldParams,
}

async fn wake(State(state): State<Arc<AlarmState>>, body: axum::body::Bytes) -> Response {
    if body.len() > MAX_WAKE_BODY {
        return err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "VRT_DO_ALARM_OVERSIZED",
            "wake body exceeds the bound",
        );
    }
    let body: WakeBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "VRT_DO_ALARM_MALFORMED",
                "wake body is not a valid alarm wake",
            )
        }
    };
    if !valid_tenant(&body.tenant_id)
        || !valid_worker(&body.worker_id)
        || !valid_class_key(&body.class_key)
        || !valid_object_id(&body.object_id)
    {
        return err(
            StatusCode::BAD_REQUEST,
            "VRT_DO_ALARM_MALFORMED",
            "wake identity is malformed",
        );
    }
    let Some(runtime) = state.registry.get(&body.tenant_id) else {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_DO_ALARM_UNKNOWN_TENANT",
            "tenant is not running on this pod",
        );
    };
    let Some(view) = runtime.do_storage_view() else {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_DO_ALARM_UNKNOWN_CLASS",
            "tenant declares no durable object storage",
        );
    };
    if view.class_dir(&body.worker_id, &body.class_key).is_none() {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_DO_ALARM_UNKNOWN_CLASS",
            "class is not declared by the serving generation",
        );
    }

    let st = runtime.state();
    if st.is_asleep() || st == crate::state::TenantState::Hibernating {
        if let Err(e) = runtime
            .await_wake(
                0,
                state.hold.max_reqs,
                state.hold.max_bytes,
                WakeSourceKind::Alarm,
            )
            .await
        {
            return match e {
                WakeError::Timeout => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_DO_ALARM_WAKE_TIMEOUT",
                    "tenant wake timed out",
                ),
                WakeError::Failed(_) => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_DO_ALARM_WAKE_FAILED",
                    "tenant wake failed",
                ),
                WakeError::Capacity => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_DO_ALARM_WAKE_CAPACITY",
                    "host wake capacity exhausted",
                ),
                WakeError::CircuitOpen => err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "VRT_DO_ALARM_WAKE_FAILED",
                    "tenant circuit breaker is open",
                ),
            };
        }
    }
    if !runtime.state().can_serve() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "VRT_DO_ALARM_WAKE_FAILED",
            "tenant generation is not serving",
        );
    }
    Json(serde_json::json!({
        "delivered": true,
        "alreadyServing": !st.is_asleep(),
        "atMs": body.at_ms,
        "tenantId": body.tenant_id,
        "workerId": body.worker_id,
        "classKey": body.class_key,
        "objectId": body.object_id,
    }))
    .into_response()
}

async fn gate(State(state): State<Arc<AlarmState>>, body: axum::body::Bytes) -> Response {
    if body.len() > MAX_GATE_BODY {
        return err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "VRT_DO_ALARM_GATE_OVERSIZED",
            "gate body exceeds the bound",
        );
    }
    let body: GateBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "VRT_DO_ALARM_GATE_MALFORMED",
                "gate body is not a valid alarm gate",
            )
        }
    };
    if !valid_tenant(&body.tenant_id) {
        return err(
            StatusCode::BAD_REQUEST,
            "VRT_DO_ALARM_GATE_MALFORMED",
            "gate tenant is malformed",
        );
    }
    if body.ttl_ms == 0 || body.ttl_ms > MAX_GATE_TTL_MS {
        return err(
            StatusCode::BAD_REQUEST,
            "VRT_DO_ALARM_GATE_TTL",
            "gate TTL is out of range",
        );
    }
    let Some(runtime) = state.registry.get(&body.tenant_id) else {
        return err(
            StatusCode::NOT_FOUND,
            "VRT_DO_ALARM_UNKNOWN_TENANT",
            "tenant is not running on this pod",
        );
    };
    runtime.set_alarm_gate(body.safe, Duration::from_millis(body.ttl_ms));
    Json(serde_json::json!({
        "accepted": true,
        "safe": body.safe,
        "ttlMs": body.ttl_ms,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::{DoStorageView, TenantRuntime};
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    fn test_app() -> axum::Router {
        let registry = TenantRegistry::new();
        let rt = TenantRuntime::new("team");
        rt.set_do_storage_view(Some(DoStorageView {
            tenant_dir: std::path::PathBuf::from("/tmp/none"),
            worker_classes: BTreeMap::from([("team:w".into(), vec!["a".repeat(64)])]),
        }));
        registry.insert("team".into(), rt);
        router(registry, SwitchHoldParams::default())
    }

    async fn post(
        app: axum::Router,
        uri: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let req = axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn wake_validates_identity_and_unknown_tenants() {
        let app = test_app();
        let (s, _) = post(
            app.clone(),
            "/v1/do-alarm/wake",
            serde_json::json!({
                "tenantId": "team",
                "workerId": "team:w",
                "classKey": "a".repeat(64),
                "objectId": "b".repeat(64),
            }),
        )
        .await;
        // No hibernated runtime in this unit harness: the endpoint reports
        // not-ready rather than false success.
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);

        let (s, body) = post(
            app.clone(),
            "/v1/do-alarm/wake",
            serde_json::json!({
                "tenantId": "missing",
                "workerId": "team:w",
                "classKey": "a".repeat(64),
                "objectId": "b".repeat(64),
            }),
        )
        .await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "VRT_DO_ALARM_UNKNOWN_TENANT");

        let (s, _) = post(
            app,
            "/v1/do-alarm/wake",
            serde_json::json!({
                "tenantId": "team",
                "workerId": "team:w",
                "classKey": "A".repeat(64),
                "objectId": "b".repeat(64),
            }),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn gate_sets_and_bounds_the_ttl() {
        let app = test_app();
        let (s, body) = post(
            app.clone(),
            "/v1/do-alarm/gate",
            serde_json::json!({"tenantId":"team","safe":true,"ttlMs":1000}),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["accepted"], true);

        let (s, _) = post(
            app,
            "/v1/do-alarm/gate",
            serde_json::json!({"tenantId":"team","safe":true,"ttlMs":0}),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
    }
}
