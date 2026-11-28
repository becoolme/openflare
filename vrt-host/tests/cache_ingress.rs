//! Workers Cache ingress regression (C).
//!
//! Covers the behaviours the wc.rs units cannot prove end to end through the
//! real ingress + pod-local store + stub upstream:
//! - Vary variants are independent entries and a worker purge clears all;
//! - `Set-Cookie` bypasses (never stored);
//! - the version segment isolates entries unless `crossVersionCache` shares
//!   the `*` key;
//! - `stale-if-error` serves the stored copy on an upstream 5xx (`STALE`);
//! - `stale-while-revalidate` serves the stored copy immediately
//!   (`UPDATING`) and refreshes it in the background.

mod common;

use common::*;
use std::sync::Arc;
use std::time::Duration;
use vrt_host::ingress::IngressState;
use vrt_host::route::RouteTable;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{SupervisorCmd, SupervisorHandle, TenantRegistry, TenantRuntime};

fn cache_worker(
    version: &str,
    cross: bool,
) -> snapshot_protocol::compute_desired::WorkerDesired {
    let mut w = worker("w1", "app.example.com", "d1");
    w.active_version.version_id = version.into();
    w.cache = Some(snapshot_protocol::compute_desired::CacheDesired {
        enabled: true,
        cross_version_cache: cross,
        key: None,
    });
    w
}

fn desired(
    version: &str,
    cross: bool,
    revision: u64,
) -> snapshot_protocol::compute_desired::ComputeDesired {
    snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision,
        tenants: vec![tenant("t1", vec![cache_worker(version, cross)])],
        deletions: vec![],
    }
}

struct CacheHarness {
    _fx: Fixture,
    rt: Arc<TenantRuntime>,
    handle: SupervisorHandle,
    state: Arc<IngressState>,
    ig: IngressGuard,
}

impl CacheHarness {
    async fn new(version: &str, cross: bool) -> Self {
        let registry = TenantRegistry::new();
        let rt = TenantRuntime::new("t1");
        registry.insert("t1".to_string(), rt.clone());
        let mut fx = Fixture::new();
        let spec = fx.stub_spec("t1", &["w1"], &[]);
        let handle = spawn(&rt, spec);
        wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
        let state = IngressState::new(
            registry,
            Duration::from_secs(5),
            Duration::from_millis(500),
            1024,
        );
        state.set_routes(RouteTable::from_desired(&desired(version, cross, 1)));
        let ig = start_ingress(&state).await;
        Self {
            _fx: fx,
            rt,
            handle,
            state,
            ig,
        }
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.ig.addr
    }

    async fn stop(self) {
        self.ig.stop().await;
        self.handle.send(SupervisorCmd::Stop).await;
        wait_state(&self.rt, TenantState::Stopped, Duration::from_secs(8)).await;
        self.handle.join().await;
    }
}

fn status_of(headers: &[(String, String)]) -> &str {
    headers
        .iter()
        .find(|(k, _)| k == "cf-cache-status")
        .map(|(_, v)| v.as_str())
        .unwrap_or("<absent>")
}


/// POST a JSON body with the cache-purge token; returns (status, body).
async fn http_post_json(
    addr: std::net::SocketAddr,
    path: &str,
    token: &str,
    json: &str,
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\nx-vrt-cache-token: {token}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{json}",
        json.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

async fn purge(h: &CacheHarness, body: &str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = vrt_host::wc_admin::router(vrt_host::wc_admin::AdminState {
        store: h.state.cache_store().clone(),
        token: "test-token".into(),
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let (status, body) = http_post_json(addr, "/internal/cache/purge", "test-token", body).await;
    task.abort();
    assert_eq!(status, 200, "purge: {body}");
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vary_variants_are_isolated_and_purged() {
    let h = CacheHarness::new("v1", false).await;
    let emit = "cache-control=max-age=60,vary=x-variant";

    let req = |variant: &'static str| {
        [
            ("x-variant", variant),
            ("x-stub-emit-header", emit),
        ]
    };

    let (status, headers, body_a) = http_get(h.addr(), "app.example.com", "/", &req("a")).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "MISS", "{headers:?}");
    assert!(body_a.contains("hdr x-variant=a"), "{body_a}");

    let (_, headers, hit_a) = http_get(h.addr(), "app.example.com", "/", &req("a")).await;
    assert_eq!(status_of(&headers), "HIT", "{headers:?}");
    assert_eq!(hit_a, body_a, "same variant must serve the stored copy");

    let (_, headers, body_b) = http_get(h.addr(), "app.example.com", "/", &req("b")).await;
    assert_eq!(status_of(&headers), "MISS", "a new Vary value is a new entry");
    assert!(body_b.contains("hdr x-variant=b"), "{body_b}");
    assert_ne!(body_b, body_a);

    // A worker purge clears every variant of the URL.
    assert!(
        purge(&h, r#"{"tenantId":"t1","workerId":"w1"}"#)
            .await
            .contains("\"purged\":2")
    );
    let (_, headers, _) = http_get(h.addr(), "app.example.com", "/", &req("a")).await;
    assert_eq!(status_of(&headers), "MISS", "purge must drop both variants");

    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_cookie_bypasses_and_never_stores() {
    let h = CacheHarness::new("v1", false).await;
    let emit = "cache-control=max-age=60,set-cookie=a=b";

    for _ in 0..2 {
        let (status, headers, _) = http_get(
            h.addr(),
            "app.example.com",
            "/",
            &[("x-stub-emit-header", emit)],
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(status_of(&headers), "BYPASS", "{headers:?}");
    }
    assert!(
        purge(&h, r#"{"tenantId":"t1","workerId":"w1"}"#)
            .await
            .contains("\"purged\":0"),
        "a bypassed response must never be stored"
    );

    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_scoping_and_cross_version_sharing() {
    let h = CacheHarness::new("v1", false).await;
    let emit = [("x-stub-emit-header", "cache-control=max-age=60")];

    let (_, headers, v1_body) = http_get(h.addr(), "app.example.com", "/", &emit).await;
    assert_eq!(status_of(&headers), "MISS", "{headers:?}");
    let (_, headers, _) = http_get(h.addr(), "app.example.com", "/", &emit).await;
    assert_eq!(status_of(&headers), "HIT", "{headers:?}");

    // A new version with cross-version off starts from an empty cache.
    h.state
        .set_routes(RouteTable::from_desired(&desired("v2", false, 2)));
    let (_, headers, v2_body) = http_get(h.addr(), "app.example.com", "/", &emit).await;
    assert_eq!(status_of(&headers), "MISS", "version is part of the key");
    assert_ne!(v2_body, v1_body);

    // cross_version_cache=true shares the `*` key across versions.
    h.state
        .set_routes(RouteTable::from_desired(&desired("v2", true, 3)));
    let (_, headers, shared_body) = http_get(h.addr(), "app.example.com", "/", &emit).await;
    assert_eq!(status_of(&headers), "MISS", "the * key is new");
    h.state
        .set_routes(RouteTable::from_desired(&desired("v3", true, 4)));
    let (_, headers, shared_hit) = http_get(h.addr(), "app.example.com", "/", &emit).await;
    assert_eq!(status_of(&headers), "HIT", "{headers:?}");
    assert_eq!(shared_hit, shared_body, "cross-version entries are shared");

    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_if_error_serves_stale_on_5xx() {
    let h = CacheHarness::new("v1", false).await;
    let emit = (
        "x-stub-emit-raw",
        "cache-control: max-age=1, stale-if-error=60",
    );

    let (status, headers, cold) = http_get(h.addr(), "app.example.com", "/", &[emit]).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "MISS", "{headers:?}");

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let (status, headers, stale) = http_get(
        h.addr(),
        "app.example.com",
        "/",
        &[emit, ("x-stub-status", "500")],
    )
    .await;
    assert_eq!(status, 200, "an upstream 5xx must fall back to the stale copy");
    assert_eq!(status_of(&headers), "STALE", "{headers:?}");
    assert_eq!(stale, cold, "STALE serves the stored copy");

    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swr_serves_updating_then_background_refreshes() {
    let h = CacheHarness::new("v1", false).await;
    let emit = (
        "x-stub-emit-raw",
        "cache-control: max-age=2, stale-while-revalidate=60",
    );

    let (status, headers, cold) = http_get(h.addr(), "app.example.com", "/", &[emit]).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "MISS", "{headers:?}");

    tokio::time::sleep(Duration::from_millis(2_100)).await;

    // Stale but within SWR: serve the stored copy now, refresh in background.
    let (status, headers, stale) = http_get(h.addr(), "app.example.com", "/", &[emit]).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "UPDATING", "{headers:?}");
    assert_eq!(stale, cold, "UPDATING serves the stored copy");

    // The background refresh replaces the stored copy; poll briefly because
    // the refresh runs after the response is sent.
    let mut refreshed = None;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let (_, _, body) = http_get(h.addr(), "app.example.com", "/", &[emit]).await;
        if body != cold {
            refreshed = Some(body);
            break;
        }
    }
    let refreshed = refreshed.expect("background refresh must update the stored copy");
    assert!(refreshed.contains("stub ok"), "{refreshed}");

    h.stop().await;
}
