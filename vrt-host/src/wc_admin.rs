//! Workers Cache purge admin — loopback control-listener endpoint used by
//! the control plane to invalidate cached responses by tag, path prefix,
//! worker, or everything. Trust model: the control listener is
//! loopback-only by config, and `VRT_CACHE_PURGE_TOKEN` is a second,
//! explicit gate. Without the env token the route is not mounted at all
//! (fail closed), so an old process cannot be purged accidentally.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;

#[derive(Clone)]
pub struct AdminState {
    pub store: Arc<crate::wc::Store>,
    pub token: Arc<str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PurgeRequest {
    tenant_id: String,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    path_prefix: Option<String>,
    #[serde(default)]
    all: Option<bool>,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/internal/cache/purge", post(purge))
        .with_state(state)
}

fn token_ok(want: &str, got: &str) -> bool {
    if want.len() != got.len() {
        return false;
    }
    want.bytes()
        .zip(got.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

async fn purge(State(state): State<AdminState>, headers: HeaderMap, req: Json<PurgeRequest>) -> Response {
    let got = headers
        .get("x-vrt-cache-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !token_ok(&state.token, got) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error": "forbidden"})))
            .into_response();
    }
    let req = req.0;
    if !valid_id(&req.tenant_id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid tenantId"})),
        )
            .into_response();
    }
    let modes = [
        req.worker_id.is_some(),
        req.tags.is_some(),
        req.path_prefix.is_some(),
        req.all.unwrap_or(false),
    ]
    .into_iter()
    .filter(|v| *v)
    .count();
    if modes != 1 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "exactly one of workerId/tags/pathPrefix/all"})),
        )
            .into_response();
    }
    let purged = if let Some(worker) = &req.worker_id {
        if !valid_id(worker) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid workerId"})),
            )
                .into_response();
        }
        state.store.purge_worker(&req.tenant_id, worker)
    } else if let Some(tags) = &req.tags {
        if tags.is_empty() || tags.len() > crate::wc::MAX_TAGS {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid tags"})),
            )
                .into_response();
        }
        state.store.purge_tags(&req.tenant_id, tags)
    } else if let Some(prefix) = &req.path_prefix {
        if !prefix.starts_with('/') || prefix.len() > 2048 {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "pathPrefix must start with /"})),
            )
                .into_response();
        }
        state.store.purge_prefix(&req.tenant_id, prefix)
    } else {
        state.store.purge_all(&req.tenant_id)
    };
    let (entries, stored_bytes, _) = state.store.stats();
    crate::metrics::registry().set("vrt_host_cache_entries", &[], entries as f64);
    crate::metrics::registry().set("vrt_host_cache_bytes", &[], stored_bytes as f64);
    (StatusCode::OK, Json(serde_json::json!({ "purged": purged }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use bytes::Bytes;
    use std::time::{Duration, SystemTime};
    use tower::ServiceExt;

    fn state() -> (AdminState, Arc<crate::wc::Store>) {
        let store = Arc::new(crate::wc::Store::new());
        let base = "t\u{1f}w\u{1f}v\u{1f}default\u{1f}/x?";
        store.put(
            base,
            crate::wc::Entry {
                status: axum::http::StatusCode::OK,
                version: axum::http::Version::HTTP_11,
                headers: vec![],
                body: Bytes::from_static(b"x"),
                stored_at: SystemTime::now(),
                fresh_for: Duration::from_secs(60),
                stale_for: Duration::ZERO,
                no_cache: false,
                vary: vec![],
                tags: vec!["page".into()],
                size: 0,
            },
        );
        (
            AdminState {
                store: store.clone(),
                token: Arc::from("secret"),
            },
            store,
        )
    }

    async fn call(state: AdminState, body: serde_json::Value, token: Option<&str>) -> (StatusCode, serde_json::Value) {
        let mut req = Request::post("/internal/cache/purge")
            .header("content-type", "application/json");
        if let Some(t) = token {
            req = req.header("x-vrt-cache-token", t);
        }
        let resp = router(state)
            .oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({})))
    }

    #[tokio::test]
    async fn purge_requires_token_and_one_mode() {
        let (state, store) = state();
        let (s, _) = call(state.clone(), serde_json::json!({"tenantId":"t","all":true}), None).await;
        assert_eq!(s, StatusCode::FORBIDDEN);

        let (s, _) = call(
            state.clone(),
            serde_json::json!({"tenantId":"t"}),
            Some("secret"),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST);

        let (s, body) = call(
            state.clone(),
            serde_json::json!({"tenantId":"t","workerId":"w"}),
            Some("secret"),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["purged"], 1);
        assert_eq!(store.stats().0, 0);
    }

    #[tokio::test]
    async fn purge_is_tenant_scoped() {
        let (state, store) = state();
        let (s, body) = call(
            state.clone(),
            serde_json::json!({"tenantId":"other","all":true}),
            Some("secret"),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["purged"], 0);
        assert_eq!(store.stats().0, 1);
    }
}
