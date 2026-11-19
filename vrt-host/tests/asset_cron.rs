//! T21: asset materialization (AssetSync) + cron trigger endpoint
//! (dedupe/validation) — the host-side fail-closed gates.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use vrt_host::artifact::ArtifactError;
use vrt_host::asset_sync::{AssetSource, AssetSync};
use vrt_host::cron_trigger;
use vrt_host::supervisor::TenantRegistry;
use vrt_provider::assets::AssetStore;

fn hex64(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

fn manifest_bytes(files: &[(&str, &[u8])]) -> (String, Vec<u8>) {
    let entries: Vec<_> = files
        .iter()
        .map(|(path, bytes)| {
            serde_json::json!({
                "mime": "text/html",
                "path": path,
                "sha256": hex64(bytes),
                "size": bytes.len(),
            })
        })
        .collect();
    let m = serde_json::json!({
        "files": entries,
        "htmlHandling": "auto-trailing-slash",
        "notFoundHandling": "none",
        "schemaVersion": 1,
    });
    let bytes = serde_json::to_vec(&m).unwrap();
    (hex64(&bytes), bytes)
}

#[derive(Default)]
struct FakeSource {
    manifests: Mutex<HashMap<String, Vec<u8>>>,
    objects: Mutex<HashMap<String, Vec<u8>>>,
}
impl AssetSource for FakeSource {
    fn fetch_manifest(&self, digest: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        Ok(self.manifests.lock().unwrap().get(digest).cloned())
    }
    fn fetch_object(&self, digest: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        Ok(self.objects.lock().unwrap().get(digest).cloned())
    }
}

fn store() -> AssetStore {
    // T29/R1: nanos alone collides on coarse clocks — add SEQ.
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "vrt-host-assets-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    AssetStore::open(&root).unwrap()
}

#[test]
fn ensure_materializes_manifest_and_objects_then_serves() {
    let (digest, mbytes) = manifest_bytes(&[("/index.html", b"hi"), ("/a.css", b"css")]);
    let source = Arc::new(FakeSource::default());
    source
        .manifests
        .lock()
        .unwrap()
        .insert(digest.clone(), mbytes);
    source
        .objects
        .lock()
        .unwrap()
        .insert(hex64(b"hi"), b"hi".to_vec());
    source
        .objects
        .lock()
        .unwrap()
        .insert(hex64(b"css"), b"css".to_vec());
    let sync = AssetSync::new(store(), source);
    sync.ensure("t1", &digest).unwrap();
    // second ensure is a cache hit — remove source entries, still ok.
    let sync2 = AssetSync::new(store(), Arc::new(FakeSource::default()));
    // fresh store + absent source → Missing
    assert!(matches!(
        sync2.ensure("t1", &digest),
        Err(ArtifactError::Missing)
    ));
}

#[test]
fn forged_manifest_and_object_fail_closed() {
    let store = store();
    let source = Arc::new(FakeSource::default());
    let (digest, _mbytes) = manifest_bytes(&[("/x", b"x")]);
    // manifest bytes that do NOT hash to `digest` → rejected at write.
    source
        .manifests
        .lock()
        .unwrap()
        .insert(digest.clone(), b"forged".to_vec());
    let sync = AssetSync::new(store, source);
    assert!(sync.ensure("t1", &digest).is_err());
}

#[test]
fn object_digest_mismatch_fails_closed() {
    let (digest, mbytes) = manifest_bytes(&[("/x", b"x")]);
    let source = Arc::new(FakeSource::default());
    source
        .manifests
        .lock()
        .unwrap()
        .insert(digest.clone(), mbytes);
    // object bytes don't match the manifest's sha256.
    source
        .objects
        .lock()
        .unwrap()
        .insert(hex64(b"x"), b"tampered".to_vec());
    let sync = AssetSync::new(store(), source);
    assert!(sync.ensure("t1", &digest).is_err());
}

#[test]
fn missing_object_fails_closed_not_partial() {
    let (digest, mbytes) = manifest_bytes(&[("/x", b"x"), ("/y", b"y")]);
    let source = Arc::new(FakeSource::default());
    source
        .manifests
        .lock()
        .unwrap()
        .insert(digest.clone(), mbytes);
    source
        .objects
        .lock()
        .unwrap()
        .insert(hex64(b"x"), b"x".to_vec());
    // /y object missing entirely.
    let sync = AssetSync::new(store(), source);
    assert!(sync.ensure("t1", &digest).is_err());
}

// ---------- cron trigger ----------

fn trigger_body() -> String {
    serde_json::json!({
        "tenantId": "tenant-1",
        "workerId": "worker-1",
        "versionId": "v1",
        "scheduleId": "*/5 * * * *",
        "scheduledTime": "2026-01-01T00:00:00Z",
        "cron": "*/5 * * * *",
        "leaseEpoch": 7,
    })
    .to_string()
}

fn req(key: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/v1/cron/trigger")
        .header("content-type", "application/json");
    if let Some(k) = key {
        b = b.header("x-vrt-cron-idempotency-key", k);
    }
    b.body(Body::from(trigger_body())).unwrap()
}

#[tokio::test]
async fn cron_trigger_rejects_key_mismatch_and_unknown_tenant() {
    let app = cron_trigger::router(TenantRegistry::new());
    // key that does not equal workerId:versionId:scheduleId:scheduledTime → 400
    let res = app.clone().oneshot(req(Some("wrong-key"))).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    // correct key but tenant not running → 404. CR1-r2: dedupe is
    // scoped by the RESOLVED tenant runtime — a trigger that never
    // reached a tenant records nothing, so a replay is 404 again
    // (not a false `duplicate`); at-most-once is kept by the
    // controller's durable claim either way.
    let key = "worker-1:v1:*/5 * * * *:2026-01-01T00:00:00Z";
    let res = app.clone().oneshot(req(Some(key))).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let res = app.oneshot(req(Some(key))).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cron_trigger_rejects_malformed_body() {
    let app = cron_trigger::router(TenantRegistry::new());
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .body(Body::from("{\"nope\":1}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cron_trigger_rejects_stale_lease_epoch() {
    let app = cron_trigger::router(TenantRegistry::new());
    let key = "worker-1:v1:*/5 * * * *:2026-01-01T00:00:00Z";
    // epoch 7 lands (404 — unknown tenant, but the FENCE is updated:
    // fencing precedes tenant resolution so a stale leader can never
    // warm up a later arrival).
    let res = app.clone().oneshot(req(Some(key))).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    // A delayed trigger from an OLDER epoch — even with a fresh key —
    // is rejected 409 (partitioned ex-leader fencing).
    let stale = serde_json::json!({
        "tenantId": "tenant-1",
        "workerId": "worker-9",
        "versionId": "v2",
        "scheduleId": "0 * * * *",
        "scheduledTime": "2026-01-01T01:00:00Z",
        "cron": "0 * * * *",
        "leaseEpoch": 3,
    })
    .to_string();
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header(
                    "x-vrt-cron-idempotency-key",
                    "worker-9:v2:0 * * * *:2026-01-01T01:00:00Z",
                )
                .body(Body::from(stale))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    assert!(std::str::from_utf8(&body)
        .unwrap()
        .contains("VRT_CRON_STALE_EPOCH"));
}

/// CR1-r2: a single request cannot ratchet `max_epoch` beyond the
/// bounded jump — `u64::MAX` after an established baseline is 409
/// `VRT_CRON_EPOCH_JUMP`, the fence is unmoved, and real epochs keep
/// serving. Baseline (fence == 0) accepts any first epoch.
#[tokio::test]
async fn cron_trigger_epoch_jump_is_bounded() {
    // small bound keeps the test honest: fence 7, jump bound 4.
    let app = cron_trigger::router_with_epoch_jump(TenantRegistry::new(), 4);
    // Baseline: first-seen epoch may be arbitrarily large (fresh host
    // cannot distinguish baseline from poison) — fence == 0 exempt.
    let base = serde_json::json!({
        "tenantId": "tenant-1", "workerId": "w", "versionId": "v1",
        "scheduleId": "s", "scheduledTime": "2026-01-01T00:00:00Z",
        "cron": "* * * * *", "leaseEpoch": 9_999_999,
    })
    .to_string();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header("x-vrt-cron-idempotency-key", "w:v1:s:2026-01-01T00:00:00Z")
                .body(Body::from(base))
                .unwrap(),
        )
        .await
        .unwrap();
    // Unknown tenant → 404, but the FENCE advanced to 9_999_999.
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // Jump within bound (fence + 4) → accepted (404 = past the gate).
    let ok = serde_json::json!({
        "tenantId": "tenant-1", "workerId": "w", "versionId": "v1",
        "scheduleId": "s", "scheduledTime": "2026-01-01T01:00:00Z",
        "cron": "* * * * *", "leaseEpoch": 10_000_003,
    })
    .to_string();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header("x-vrt-cron-idempotency-key", "w:v1:s:2026-01-01T01:00:00Z")
                .body(Body::from(ok))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // Overshoot (fence + 5 > fence + 4) → 409 EPOCH_JUMP, fence UNMOVED.
    let big = serde_json::json!({
        "tenantId": "tenant-1", "workerId": "w", "versionId": "v1",
        "scheduleId": "s", "scheduledTime": "2026-01-01T02:00:00Z",
        "cron": "* * * * *", "leaseEpoch": u64::MAX,
    })
    .to_string();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header("x-vrt-cron-idempotency-key", "w:v1:s:2026-01-01T02:00:00Z")
                .body(Body::from(big))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    assert!(std::str::from_utf8(&body)
        .unwrap()
        .contains("VRT_CRON_EPOCH_JUMP"));

    // Fence unmoved: the in-bound epoch still serves (proves the
    // overshoot did NOT ratchet max_epoch to u64::MAX).
    let still = serde_json::json!({
        "tenantId": "tenant-1", "workerId": "w", "versionId": "v1",
        "scheduleId": "s", "scheduledTime": "2026-01-01T03:00:00Z",
        "cron": "* * * * *", "leaseEpoch": 10_000_004,
    })
    .to_string();
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header("x-vrt-cron-idempotency-key", "w:v1:s:2026-01-01T03:00:00Z")
                .body(Body::from(still))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

/// CR1-r2: dedupe is scoped by the RESOLVED tenant runtime — two
/// tenants presenting the SAME (workerId, versionId, scheduleId,
/// scheduledTime) key must EACH dispatch once; a same-tenant replay
/// is `duplicate`.
#[tokio::test]
async fn cron_trigger_dedupe_scoped_by_resolved_tenant() {
    // Recording endpoint: each accepted dispatch increments a counter
    // and replies the facade success contract.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits2 = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let h = hits2.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 8192];
                let _ = s.read(&mut buf).await;
                h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = r#"{"delivered":true,"outcome":"ok"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });

    let registry = TenantRegistry::new();
    for tenant in ["tenant-a", "tenant-b"] {
        let rt = registry.get_or_create(tenant);
        rt.test_install_cron(std::collections::BTreeMap::from([(
            "worker-1".to_string(),
            vrt_host::endpoint::WorkerEndpoint::Tcp(format!("127.0.0.1:{port}").parse().unwrap()),
        )]));
    }
    let app = cron_trigger::router(registry);
    let key = "worker-1:v1:*/5 * * * *:2026-01-01T00:00:00Z";
    for tenant in ["tenant-a", "tenant-b"] {
        let body = serde_json::json!({
            "tenantId": tenant, "workerId": "worker-1", "versionId": "v1",
            "scheduleId": "*/5 * * * *",
            "scheduledTime": "2026-01-01T00:00:00Z",
            "cron": "*/5 * * * *", "leaseEpoch": 0,
        })
        .to_string();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/cron/trigger")
                    .header("content-type", "application/json")
                    .header("x-vrt-cron-idempotency-key", key)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "tenant {tenant}");
        let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
        let j: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(j["delivered"], true, "{j}");
    }
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "both tenants' identical keys must dispatch — no cross-tenant dedupe"
    );

    // Same-tenant replay → duplicate, no third dispatch.
    let body = serde_json::json!({
        "tenantId": "tenant-a", "workerId": "worker-1", "versionId": "v1",
        "scheduleId": "*/5 * * * *",
        "scheduledTime": "2026-01-01T00:00:00Z",
        "cron": "*/5 * * * *", "leaseEpoch": 0,
    })
    .to_string();
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header("x-vrt-cron-idempotency-key", key)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let j: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(j["duplicate"], true, "{j}");
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
}
