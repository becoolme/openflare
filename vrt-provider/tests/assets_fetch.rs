//! T21: assets fetch end-to-end through the provider dispatch —
//! authorized identity → manifest resolve → bytes with ETag, HEAD,
//! If-None-Match, directory redirects, not-found semantics, traversal
//! rejection, cross-tenant manifest isolation and forged-object
//! handling (the store only serves digest-verified objects).

mod support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::{AssetsBinding, BindingDesired, BindingPermissions};
use std::sync::Arc;
use support::*;
use tower::ServiceExt;
use vrt_provider::assets::AssetStore;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::kv::MemoryKvProvider;
use vrt_provider::r2::MemoryR2Provider;
use vrt_provider::server::{tenant_router, TenantService};

fn hex64(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

fn manifest_bytes(files: &[(&str, &[u8])], not_found: &str) -> (String, Vec<u8>) {
    let entries: Vec<_> = files
        .iter()
        .map(|(path, bytes)| {
            json!({
                "mime": "text/html",
                "path": path,
                "sha256": hex64(bytes),
                "size": bytes.len(),
            })
        })
        .collect();
    let m = json!({
        "files": entries,
        "htmlHandling": "auto-trailing-slash",
        "notFoundHandling": not_found,
        "schemaVersion": 1,
    });
    let bytes = serde_json::to_vec(&m).unwrap();
    (hex64(&bytes), bytes)
}

struct Env {
    app: axum::Router,
    digest: String,
    root: std::path::PathBuf,
}

/// Remove every stored copy of an object/manifest file named `name`.
fn remove_stored(root: &std::path::Path, name: &str) {
    fn walk(dir: &std::path::Path, name: &str) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, name);
            } else if p.file_name().is_some_and(|n| n.to_string_lossy().starts_with(name)) {
                std::fs::remove_file(&p).unwrap();
            }
        }
    }
    walk(root, name);
}

fn env(files: &[(&str, &[u8])], not_found: &str, store_tenant: &str) -> Env {
    let (digest, mbytes) = manifest_bytes(files, not_found);
    let root = std::env::temp_dir().join(format!("vrt-assets-test-{}", uuid_v4()));
    let store = AssetStore::open(&root).unwrap();
    for (path, bytes) in files {
        store
            .put_object_verified(store_tenant, &hex64(bytes), bytes)
            .unwrap();
        let _ = path;
    }
    store
        .put_manifest_verified(store_tenant, &digest, &mbytes)
        .unwrap();
    let f = fixture();
    let d = desired(
        7,
        vec![tenant(
            TENANT,
            vec![worker(
                WORKER,
                VERSION,
                DIGEST,
                vec![BindingDesired::Assets(AssetsBinding {
                    name: "SITE".into(),
                    manifest_digest: digest.clone(),
                    permissions: BindingPermissions {
                        read: true,
                        write: false,
                    },
                    assets_first: None,
                    run_worker_first: None,
                })],
            )],
        )],
    );
    let store_state = store_with(&f, &d, 7);
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store_state, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: Some(store),
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        pg: None,
    });
    Env {
        app: tenant_router(svc),
        digest,
        root,
    }
}

/// T29/R1: nanos+pid collided on macOS (µs clock → two parallel tests
/// drew the same dir → same temp file → ENOENT). Counter + entropy
/// makes every call unique inside the process; pid keeps cross-
/// process runs apart.
fn uuid_v4() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{n:x}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

async fn fetch(
    app: &axum::Router,
    digest: &str,
    method: &str,
    url: &str,
    extra: &[(&str, &str)],
) -> axum::response::Response {
    let mut b = Request::builder()
        .method("POST")
        .uri("http://binding-backend/internal/assets/v1/fetch")
        .header("x-open-compute-binding-id", format!("{WORKER}~SITE"))
        .header("x-open-compute-version-id", VERSION)
        .header("x-open-compute-descriptor-sha256", DIGEST)
        .header("x-open-compute-request-id", REQUEST_ID)
        .header("x-open-compute-worker-id", WORKER)
        .header("x-open-compute-namespace-resource-id", digest)
        .header("x-open-compute-resource-spec-generation", "1")
        .header("x-open-compute-asset-method", method)
        .header("x-open-compute-asset-url", url);
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    app.clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn get_serves_bytes_with_etag_and_mime() {
    let e = env(&[("/index.html", b"<h1>hi</h1>")], "none", TENANT);
    let res = fetch(
        &e.app,
        &e.digest,
        "GET",
        "https://site.example/index.html",
        &[],
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let etag = res
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(etag, format!("\"{}\"", hex64(b"<h1>hi</h1>")));
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"<h1>hi</h1>");
}

#[tokio::test]
async fn head_returns_headers_empty_body() {
    let e = env(&[("/a.txt", b"abc")], "none", TENANT);
    let res = fetch(&e.app, &e.digest, "HEAD", "https://x/a.txt", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn if_none_match_returns_304() {
    let e = env(&[("/a.txt", b"abc")], "none", TENANT);
    let etag = format!("\"{}\"", hex64(b"abc"));
    let res = fetch(
        &e.app,
        &e.digest,
        "GET",
        "https://x/a.txt",
        &[("if-none-match", &etag)],
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn directory_without_slash_redirects_308() {
    let e = env(&[("/docs/index.html", b"d")], "none", TENANT);
    let res = fetch(&e.app, &e.digest, "GET", "https://x/docs", &[]).await;
    assert_eq!(res.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(res.headers().get("location").unwrap(), "/docs/");
    let res = fetch(&e.app, &e.digest, "GET", "https://x/docs/", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn spa_fallback_serves_index_for_unknown_path() {
    let e = env(
        &[("/index.html", b"spa")],
        "single-page-application",
        TENANT,
    );
    let res = fetch(&e.app, &e.digest, "GET", "https://x/app/route/1", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"spa");
}

#[tokio::test]
async fn traversal_and_bad_method_and_missing_manifest_typed() {
    let e = env(&[("/index.html", b"i")], "none", TENANT);
    // traversal → protocol error
    let res = fetch(&e.app, &e.digest, "GET", "https://x/../etc/passwd", &[]).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    // POST asset method → capability unsupported
    let res = fetch(&e.app, &e.digest, "POST", "https://x/index.html", &[]).await;
    assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    // unknown manifest → fail closed (503) or forbidden (no oracle)
    let res = fetch(&e.app, &"0".repeat(64), "GET", "https://x/index.html", &[]).await;
    assert!(
        res.status() == StatusCode::SERVICE_UNAVAILABLE || res.status() == StatusCode::FORBIDDEN
    );
}

async fn body_of(res: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

fn code_of(res: &axum::response::Response) -> String {
    res.headers()
        .get("x-open-compute-error-code")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default()
}

#[tokio::test]
async fn page_404_serves_404_html_with_status_404() {
    let e = env(
        &[("/index.html", b"i"), ("/404.html", b"<h1>nope</h1>")],
        "404-page",
        TENANT,
    );
    let res = fetch(&e.app, &e.digest, "GET", "https://x/missing", &[]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(res.headers().get("content-type").unwrap(), "text/html");
    assert_eq!(code_of(&res), "");
    assert_eq!(body_of(res).await, b"<h1>nope</h1>");
}

#[tokio::test]
async fn page_404_ignores_matching_if_none_match() {
    let e = env(&[("/404.html", b"<h1>nope</h1>")], "404-page", TENANT);
    let etag = format!("\"{}\"", hex64(b"<h1>nope</h1>"));
    let res = fetch(
        &e.app,
        &e.digest,
        "GET",
        "https://x/missing",
        &[("if-none-match", etag.as_str())],
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_of(res).await, b"<h1>nope</h1>");
}

#[tokio::test]
async fn page_404_without_404_html_is_typed_not_found() {
    let e = env(&[("/index.html", b"i")], "404-page", TENANT);
    let res = fetch(&e.app, &e.digest, "GET", "https://x/missing", &[]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(code_of(&res), "ASSET_NOT_FOUND");
}

#[tokio::test]
async fn missing_manifest_is_storage_unavailable() {
    let e = env(&[("/index.html", b"i")], "none", TENANT);
    remove_stored(&e.root, &e.digest);
    let res = fetch(&e.app, &e.digest, "GET", "https://x/index.html", &[]).await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(code_of(&res), "ASSET_STORAGE_UNAVAILABLE");
}

#[tokio::test]
async fn listed_object_missing_is_storage_unavailable() {
    let e = env(&[("/index.html", b"i")], "none", TENANT);
    remove_stored(&e.root, &hex64(b"i"));
    let res = fetch(&e.app, &e.digest, "GET", "https://x/index.html", &[]).await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(code_of(&res), "ASSET_STORAGE_UNAVAILABLE");
}

#[tokio::test]
async fn cross_tenant_manifest_is_invisible() {
    // The manifest lives under tenant "other" — this tenant's binding
    // pin resolves the same digest but the store scopes by tenant id.
    let e = env(&[("/index.html", b"i")], "none", "other-tenant");
    let res = fetch(&e.app, &e.digest, "GET", "https://x/index.html", &[]).await;
    assert_ne!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn forged_object_digest_never_served() {
    // put_object_verified refuses bytes that do not hash to the digest.
    let root = std::env::temp_dir().join(format!("vrt-assets-test-{}", uuid_v4()));
    let store = AssetStore::open(&root).unwrap();
    let fake = "f".repeat(64);
    assert!(store
        .put_object_verified(TENANT, &fake, b"not-the-digest")
        .is_err());
    // And a manifest entry pointing at a missing object → typed miss.
    let e = env(&[], "none", TENANT);
    let res = fetch(&e.app, &e.digest, "GET", "https://x/ghost.html", &[]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// T31c: router endpoints — route-index + can-fetch
// ---------------------------------------------------------------------------

async fn probe(
    app: &axum::Router,
    endpoint: &str,
    digest: &str,
    extra: &[(&str, &str)],
) -> axum::response::Response {
    let mut b = Request::builder()
        .method("POST")
        .uri(format!(
            "http://binding-backend/internal/assets/v1/{endpoint}"
        ))
        .header("x-open-compute-binding-id", format!("{WORKER}~SITE"))
        .header("x-open-compute-version-id", VERSION)
        .header("x-open-compute-descriptor-sha256", DIGEST)
        .header("x-open-compute-request-id", REQUEST_ID)
        .header("x-open-compute-worker-id", WORKER)
        .header("x-open-compute-namespace-resource-id", digest)
        .header("x-open-compute-resource-spec-generation", "1");
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    app.clone()
        .oneshot(b.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn route_index_lists_paths_and_modes() {
    let e = env(
        &[
            ("/index.html", b"i"),
            ("/app.js", b"j"),
            ("/about.html", b"a"),
        ],
        "single-page-application",
        TENANT,
    );
    let res = probe(&e.app, "route-index", &e.digest, &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["htmlHandling"], "auto-trailing-slash");
    assert_eq!(v["notFoundHandling"], "single-page-application");
    let paths: Vec<&str> = v["paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["/index.html", "/app.js", "/about.html"]);
}

#[tokio::test]
async fn route_index_unknown_manifest_is_typed_404() {
    let e = env(&[("/index.html", b"i")], "none", TENANT);
    let res = probe(&e.app, "route-index", &"9".repeat(64), &[]).await;
    assert!(res.status() == StatusCode::NOT_FOUND || res.status() == StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn can_fetch_hits_and_misses() {
    let e = env(&[("/index.html", b"i"), ("/app.js", b"j")], "none", TENANT);
    let hit = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-method", "GET"),
            ("x-open-compute-asset-url", "https://site.example/app.js"),
        ],
    )
    .await;
    assert_eq!(hit.status(), StatusCode::OK);
    let body = axum::body::to_bytes(hit.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"true");

    let miss = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-method", "GET"),
            ("x-open-compute-asset-url", "https://site.example/nope.js"),
        ],
    )
    .await;
    assert_eq!(miss.status(), StatusCode::OK);
    let body = axum::body::to_bytes(miss.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"false");

    // POST is never fetchable.
    let post = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-method", "POST"),
            ("x-open-compute-asset-url", "https://site.example/app.js"),
        ],
    )
    .await;
    let body = axum::body::to_bytes(post.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"false");
}

#[tokio::test]
async fn can_fetch_navigation_unlocks_spa_fallback() {
    // SPA manifest: a non-navigation probe must NOT see /index.html
    // through the fallback — Miniflare forces not_found_handling=none
    // unless the request is a navigation or carries static routing.
    let e = env(&[("/index.html", b"i")], "single-page-application", TENANT);
    let plain = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-method", "GET"),
            (
                "x-open-compute-asset-url",
                "https://site.example/deep/route",
            ),
        ],
    )
    .await;
    let body = axum::body::to_bytes(plain.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"false");

    let nav = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-method", "GET"),
            (
                "x-open-compute-asset-url",
                "https://site.example/deep/route",
            ),
            ("sec-fetch-mode", "navigate"),
        ],
    )
    .await;
    let body = axum::body::to_bytes(nav.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"true");

    // static-routing presence also unlocks the fallback.
    let routed = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-method", "GET"),
            (
                "x-open-compute-asset-url",
                "https://site.example/deep/route",
            ),
            ("x-open-compute-asset-has-static-routing", "true"),
        ],
    )
    .await;
    let body = axum::body::to_bytes(routed.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"true");
}

/// T31d/R1: a manifest over `MAX_ROUTE_INDEX_PATHS` answers `tooLarge`
/// without streaming paths — the router degrades to can-fetch probes
/// instead of receiving multi-MB JSON it cannot cache.
#[tokio::test]
async fn route_index_over_limit_returns_too_large_flag() {
    // 8192 = platform `assetMaxFiles` hard ceiling — the manifest is
    // legal end-to-end; route-index still answers `tooLarge` so the
    // router degrades instead of buffering ~300KB of path JSON.
    let files: Vec<(String, Vec<u8>)> = (0..8192)
        .map(|i| (format!("/f{i}.txt"), format!("b{i}").into_bytes()))
        .collect();
    let entries: Vec<_> = files
        .iter()
        .map(|(path, bytes)| {
            json!({
                "mime": "text/plain",
                "path": path,
                "sha256": hex64(bytes),
                "size": bytes.len(),
            })
        })
        .collect();
    let m = json!({
        "files": entries,
        "htmlHandling": "auto-trailing-slash",
        "notFoundHandling": "none",
        "schemaVersion": 1,
    });
    let mbytes = serde_json::to_vec(&m).unwrap();
    let digest = hex64(&mbytes);
    let root = std::env::temp_dir().join(format!("vrt-assets-test-{}", uuid_v4()));
    let store = AssetStore::open(&root).unwrap();
    store
        .put_manifest_verified(TENANT, &digest, &mbytes)
        .unwrap();
    let f = fixture();
    let d = desired(
        7,
        vec![tenant(
            TENANT,
            vec![worker(
                WORKER,
                VERSION,
                DIGEST,
                vec![BindingDesired::Assets(AssetsBinding {
                    name: "SITE".into(),
                    manifest_digest: digest.clone(),
                    permissions: BindingPermissions {
                        read: true,
                        write: false,
                    },
                    assets_first: None,
                    run_worker_first: None,
                })],
            )],
        )],
    );
    let store_state = store_with(&f, &d, 7);
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store_state, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: Some(store),
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        pg: None,
    });
    let app = tenant_router(svc);
    let res = probe(&app, "route-index", &digest, &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["tooLarge"], true);
    assert!(v["paths"].is_null(), "paths must not stream over the bound");
    assert_eq!(v["htmlHandling"], "auto-trailing-slash");
}

/// T31d/R4: percent-encoded request paths decode before manifest
/// matching (Miniflare decodePath mirror) — `/my%20file.txt` resolves
/// the stored `/my file.txt`, and percent-encoded CJK works.
#[tokio::test]
async fn percent_encoded_paths_decode_before_match() {
    let e = env(
        &[("/my file.txt", b"s"), ("/\u{4e2d}\u{6587} page.html", b"c")],
        "none",
        TENANT,
    );
    for url in [
        "http://w.test/my%20file.txt",
        "http://w.test/%E4%B8%AD%E6%96%87%20page.html",
    ] {
        let res = fetch(&e.app, &e.digest, "GET", url, &[]).await;
        assert_eq!(res.status(), StatusCode::OK, "{url}");
    }
    // can-fetch probe resolves encoded paths identically.
    let hit = probe(
        &e.app,
        "can-fetch",
        &e.digest,
        &[
            ("x-open-compute-asset-url", "http://w.test/my%20file.txt"),
            ("x-open-compute-asset-method", "GET"),
        ],
    )
    .await;
    let body = axum::body::to_bytes(hit.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"true");
}
