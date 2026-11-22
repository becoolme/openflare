//! T20/B10 — declared-but-undelivered binding backends answer PRECISE
//! typed errors, never the generic BINDING_PROTOCOL_ERROR fallthrough:
//! `/internal/assets/v1/*` → ASSET_STORAGE_UNAVAILABLE (facade-assets
//! whitelists this code into a typed worker error), `/internal/pg/*` →
//! BINDING_CAPABILITY_UNSUPPORTED (defense in depth — facade-pg denies
//! before reaching the provider).

mod support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use snapshot_protocol::compute_desired::BindingDesired;
use std::sync::Arc;
use support::*;
use tower::ServiceExt;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::kv::MemoryKvProvider;
use vrt_provider::r2::MemoryR2Provider;
use vrt_provider::server::{tenant_router, TenantService};

fn svc() -> Arc<TenantService> {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(
                    WORKER,
                    VERSION,
                    DIGEST,
                    Vec::<BindingDesired>::new(),
                )],
            )],
        ),
        7,
    );
    Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
        vectorize: None,
    })
}

async fn post(app: &axum::Router, path: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn assets_backend_reports_storage_unavailable() {
    // T21: an AUTHORIZED assets fetch against a service with no store
    // configured → 503 ASSET_STORAGE_UNAVAILABLE (fail closed). An
    // unauthenticated request still fails earlier (400) — identity is
    // checked before capability, so an unconfigured store leaks nothing.
    use snapshot_protocol::compute_desired::{AssetsBinding, BindingPermissions};
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(
                    WORKER,
                    VERSION,
                    DIGEST,
                    vec![BindingDesired::Assets(AssetsBinding {
                        name: "SITE".into(),
                        manifest_digest: "a".repeat(64),
                        permissions: BindingPermissions {
                            read: true,
                            write: false,
                        },
                        assets_first: None,
                        run_worker_first: None,
                    })],
                )],
            )],
        ),
        7,
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
        vectorize: None,
    });
    let app = tenant_router(svc);
    // unauthenticated → 400 before the store check
    let res = post(&app, "/internal/assets/v1/fetch").await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    // authorized → 503 typed
    let mut headers = standard_headers(
        &format!("{WORKER}~SITE"),
        WORKER,
        VERSION,
        DIGEST,
        &"a".repeat(64),
        1,
    );
    headers.push(("x-open-compute-asset-method".to_string(), "GET".to_string()));
    headers.push((
        "x-open-compute-asset-url".to_string(),
        "https://x/a".to_string(),
    ));
    let mut b = Request::builder()
        .method("POST")
        .uri("/internal/assets/v1/fetch");
    for (k, v) in &headers {
        b = b.header(k.as_str(), v.as_str());
    }
    let res = app
        .clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        res.headers()
            .get("x-open-compute-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("ASSET_STORAGE_UNAVAILABLE")
    );
}

#[tokio::test]
async fn pg_backend_reports_capability_unsupported() {
    // P1: the placeholder 422/BINDING_CAPABILITY_UNSUPPORTED is gone —
    // pg is a real backend. Unknown/malformed paths fail closed with
    // the typed protocol error (never a guess, never 200).
    let app = tenant_router(svc());
    // Old-style `/internal/pg/*` namespace: defense-in-depth NOT_FOUND.
    let res = post(&app, "/internal/pg/v1/query").await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        res.headers()
            .get("x-open-compute-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("BINDING_PROTOCOL_ERROR"),
    );
    // `/internal/bindings/v1/pg/query` parses as bindingId="query"
    // missing the `/sql` op → BAD_REQUEST protocol error.
    let res = post(&app, "/internal/bindings/v1/pg/query").await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        res.headers()
            .get("x-open-compute-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("BINDING_PROTOCOL_ERROR"),
    );
    // The real op exists but the request carries no host-injected
    // identity → typed identity rejection before any backend call.
    let res = post(&app, "/internal/bindings/v1/pg/some-binding/sql").await;
    assert_ne!(res.status(), StatusCode::OK);
    let code = res
        .headers()
        .get("x-open-compute-error-code")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        code.starts_with("BINDING_") || code.starts_with("PG_"),
        "unexpected code {code}"
    );
}

#[tokio::test]
async fn unknown_paths_still_report_protocol_error() {
    let app = tenant_router(svc());
    let res = post(&app, "/internal/whatever").await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        res.headers()
            .get("x-open-compute-error-code")
            .and_then(|v| v.to_str().ok()),
        Some("BINDING_PROTOCOL_ERROR")
    );
}
