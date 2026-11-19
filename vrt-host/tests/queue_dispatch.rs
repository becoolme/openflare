//! T37b-r2: `/v1/queue/dispatch` tenant-state gate — the dispatcher's
//! proxy must serve `SwitchFailed` tenants (old generation's consumers
//! still run), matching ingress `can_serve()`. Before the fix the gate
//! matched `Ready` alone, so a latched bad spec halted queue dispatch
//! tenant-wide too.
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use tower::ServiceExt;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{TenantRegistry, TenantRuntime};

fn post(body: serde_json::Value) -> axum::http::Request<axum::body::Body> {
    axum::http::Request::builder()
        .method("POST")
        .uri("/v1/queue/dispatch")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap()
}

async fn dispatch_status(rt: &Arc<TenantRuntime>, state: TenantState) -> (u16, serde_json::Value) {
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    rt.test_set_state(state);
    let res = vrt_host::queue_dispatch::router(registry)
        .oneshot(post(serde_json::json!({
            "tenantId": "t1",
            "workerId": "w1",
            "dispatch": {"queue": "jobs", "messages": []}
        })))
        .await
        .unwrap();
    let status = res.status().as_u16();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_failed_passes_gate_to_slot_lookup() {
    let rt = TenantRuntime::new("t1");
    // SwitchFailed keeps the OLD generation serving. Gate passes
    // (can_serve) → qconsumer slot lookup misses `w1` on a generation
    // that IS serving → 404 NO_CONSUMER, NOT 503 WORKER_NOT_READY.
    // MIG-11-r2: the serving generation must actually be installed —
    // with nothing serving the same miss is a retryable 503 (see
    // `switching_window_*` in this file).
    rt.test_install_qconsumer(std::collections::BTreeMap::from([(
        "other".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    let (status, body) = dispatch_status(&rt, TenantState::SwitchFailed).await;
    assert_eq!(status, 404, "{body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_NO_CONSUMER"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unserving_states_still_503() {
    let rt = TenantRuntime::new("t1");
    for state in [
        TenantState::Starting,
        TenantState::CircuitOpen,
        TenantState::Crashed,
    ] {
        let (status, body) = dispatch_status(&rt, state).await;
        assert_eq!(status, 503, "{state:?}: {body}");
        assert_eq!(
            body["error"]["code"].as_str(),
            Some("VRT_QDISP_WORKER_NOT_READY"),
            "{state:?}: {body}"
        );
        // Small yield so the next iteration's runtime is fresh-state.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ---------------- RV-R8 #5: body bounds, both directions ----------------

async fn dispatch_raw(
    registry: Arc<TenantRegistry>,
    req: axum::http::Request<axum::body::Body>,
) -> (u16, serde_json::Value) {
    let res = vrt_host::queue_dispatch::router(registry)
        .oneshot(req)
        .await
        .unwrap();
    let status = res.status().as_u16();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_oversize_body_gets_typed_413() {
    // Content-Length above the bound → typed 413, no tenant lookup.
    let (status, body) = dispatch_raw(
        TenantRegistry::new(),
        axum::http::Request::builder()
            .method("POST")
            .uri("/v1/queue/dispatch")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(vec![b'x'; 9_000_001]))
            .unwrap(),
    )
    .await;
    assert_eq!(status, 413, "{body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_OVERSIZED"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunked_oversize_body_gets_typed_413() {
    // No Content-Length — `Limited` must stop the stream mid-body.
    let chunks = futures_util::stream::iter(
        (0..4).map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b'y'; 4_000_000]))),
    );
    let (status, body) = dispatch_raw(
        TenantRegistry::new(),
        axum::http::Request::builder()
            .method("POST")
            .uri("/v1/queue/dispatch")
            .header("content-type", "application/json")
            .body(axum::body::Body::from_stream(chunks))
            .unwrap(),
    )
    .await;
    assert_eq!(status, 413, "{body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_OVERSIZED"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn at_limit_body_passes_the_size_gate() {
    // Just under the bound → the size gate passes; the request must
    // reach tenant lookup (404 unknown tenant, NOT 413).
    let pad = "p".repeat(9_000_000 - 400);
    let req = serde_json::json!({
        "tenantId": "ghost",
        "workerId": "w1",
        "dispatch": {"queue": "jobs", "messages": [], "pad": pad}
    });
    let body_len = req.to_string().len();
    let (status, body) = dispatch_raw(
        TenantRegistry::new(),
        axum::http::Request::builder()
            .method("POST")
            .uri("/v1/queue/dispatch")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(req.to_string()))
            .unwrap(),
    )
    .await;
    assert!(body_len <= 9_000_000, "fixture must be at-limit: {body_len}");
    assert_eq!(status, 404, "{body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_UNKNOWN_WORKER"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_upstream_response_gets_502() {
    // Facade stub answers a 2MiB body (cap is 1MiB) → typed 502.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let facade_addr = lis.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = lis.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let mut resp =
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2097153\r\n\r\n".to_vec();
                resp.extend_from_slice(&vec![b'z'; 2_097_153]);
                let _ = sock.write_all(&resp).await;
            });
        }
    });
    let rt = TenantRuntime::new("t1");
    rt.test_set_state(TenantState::Ready);
    let mut eps = std::collections::BTreeMap::new();
    eps.insert(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(facade_addr),
    );
    rt.test_install_qconsumer(eps);
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt);

    let (status, body) = dispatch_raw(
        registry,
        post(serde_json::json!({
            "tenantId": "t1",
            "workerId": "w1",
            "dispatch": {"queue": "jobs", "messages": []}
        })),
    )
    .await;
    assert_eq!(status, 502, "{body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_UPSTREAM_OVERSIZED"),
        "{body}"
    );
}


// ---------------- MIG-11: serialized-switch window ----------------

/// MIG-11: while the tenant's serialized-switch gate is active the
/// route map is empty — the dispatch HOLDS (bounded) and resolves
/// against the freshly-installed routes instead of mis-reporting
/// `VRT_QDISP_NO_CONSUMER`. Proven by landing a dead consumer endpoint
/// mid-window: the response is the upstream failure (502), not a 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switching_window_waits_then_reaches_slot() {
    let rt = TenantRuntime::new("t1");
    rt.test_set_state(TenantState::Ready);
    rt.switch_gate.begin();
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    // Land the consumer route mid-window, then release the gate —
    // production install order is routes-then-end.
    let rt2 = rt.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        rt2.test_install_qconsumer(std::collections::BTreeMap::from([(
            "w1".to_string(),
            vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
        )]));
        rt2.switch_gate.end();
    });
    let res = vrt_host::queue_dispatch::router_with_hold(
        registry,
        vrt_host::supervisor::SwitchHoldParams {
            budget: Duration::from_secs(2),
            max_reqs: 8,
            max_bytes: 1 << 20,
        },
    )
    .oneshot(post(serde_json::json!({
        "tenantId": "t1",
        "workerId": "w1",
        "dispatch": {"queue": "jobs", "messages": []}
    })))
    .await
    .unwrap();
    let status = res.status().as_u16();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(status, 502, "waited + dispatched to dead endpoint: {body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_DELIVERY_FAILED"),
        "{body}"
    );
}

/// MIG-11: the switch window outliving the hold budget answers 503
/// `VRT_QDISP_SWITCHING` + `Retry-After` — a platform error that does
/// not consume attempts. MIG-11-r2: after the gate clears the miss
/// splits typed — NOTHING serving is 503 `VRT_QDISP_WORKER_NOT_READY`
/// (still a platform error), while a serving generation that never
/// declared the consumer keeps the genuine 404 `NO_CONSUMER`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switching_window_timeout_is_503_switching_then_404_after() {
    let rt = TenantRuntime::new("t1");
    rt.test_set_state(TenantState::Ready);
    rt.switch_gate.begin();
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let hold = vrt_host::supervisor::SwitchHoldParams {
        budget: Duration::from_millis(150),
        max_reqs: 8,
        max_bytes: 1 << 20,
    };
    let req = || {
        post(serde_json::json!({
            "tenantId": "t1",
            "workerId": "w1",
            "dispatch": {"queue": "jobs", "messages": []}
        }))
    };
    let res = vrt_host::queue_dispatch::router_with_hold(registry.clone(), hold)
        .oneshot(req())
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 503);
    assert!(
        res.headers().get("retry-after").is_some(),
        "SWITCHING must carry Retry-After"
    );
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_SWITCHING"),
        "{body}"
    );
    rt.switch_gate.end();
    // MIG-11-r2: gate cleared with NOTHING serving (the generation
    // never installed — launch failed) → retryable
    // `VRT_QDISP_WORKER_NOT_READY`, still a platform error — never
    // the fake-absence 404.
    let res = vrt_host::queue_dispatch::router_with_hold(registry.clone(), hold)
        .oneshot(req())
        .await
        .unwrap();
    let status = res.status().as_u16();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        status, 503,
        "no serving generation after gate end → 503, not 404: {body}"
    );
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_WORKER_NOT_READY"),
        "{body}"
    );
    // A serving generation that never declared `w1`'s consumer keeps
    // the genuine-absence contract.
    rt.test_install_qconsumer(std::collections::BTreeMap::from([(
        "other".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    let res = vrt_host::queue_dispatch::router_with_hold(registry, hold)
        .oneshot(req())
        .await
        .unwrap();
    let status = res.status().as_u16();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(status, 404, "serving gen without the consumer → genuine 404: {body}");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_QDISP_NO_CONSUMER"),
        "{body}"
    );
}
