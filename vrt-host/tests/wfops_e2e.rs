//! W3: production-path workflow-op dispatch -- the real `WfOpsExecutor`
//! (cell-controller) + the real vrt-host `/v1/wfops` route (production
//! router/handler/response encoding). The facade is replaced by a raw TCP stub -- the protocol surface
//! (hyper HTTP/1.1 POST `/vrt/wfops` + `x-vrt-wfop-token` +
//! Host) matches the workerd socket service; the facade->engine real chain is covered by
//! the real workerd test in `wfops_smoke.rs`.
//!
//! It is also a two-way contract test: the stub asserts the request line/header shape forwarded by vrt-host,
//! and the executor applies a whitelist + truncation to the vrt-host response -- drift on either side
//! blows up here.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use cell_controller::wfops::{CellOp, WfOpsExecutor};
use serde_json::{json, Value};
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::supervisor::{TenantRegistry, TenantRuntime};

fn op(seq: i64, kind: &str, payload: Value) -> CellOp {
    CellOp {
        id: format!("op-{seq}"),
        seq,
        kind: kind.to_string(),
        worker_id: "w1".into(),
        resource_key: String::new(),
        payload,
        expires_at: None,
    }
}

fn payload(extra: Value) -> Value {
    // W3-r2: op args only — no identity material on the op channel.
    let mut base = json!({ "workflow": "order-pipe" });
    for (k, v) in extra.as_object().unwrap() {
        base[k.clone()] = v.clone();
    }
    base
}

/// Signed-desired fixture — the executor resolves `w1` → tenant `t1`
/// and verifies `order-pipe` is declared, all from this document.
fn desired() -> snapshot_protocol::compute_desired::ComputeDesired {
    serde_json::from_value(json!({
        "schemaVersion": 1,
        "cellId": "cell-1",
        "revision": 3,
        "tenants": [{
            "tenantId": "t1",
            "workers": [{
                "workerId": "w1",
                "name": "orders",
                "activeVersion": {
                    "versionId": "vrt_v1",
                    "artifactDigest": "a".repeat(64),
                    "compatibilityDate": "2024-01-01",
                    "compatibilityFlags": []
                },
                "bindings": [],
                "crons": [],
                "quotas": {"cpuMs": 50, "memoryMb": 128, "outboundConnections": 16, "subrequests": 50},
                "routes": [],
                "workflows": [{
                    "name": "order-pipe",
                    "binding": "ORDER_FLOW",
                    "className": "OrderPipe",
                    "uniqueKey": "u".repeat(64)
                }]
            }]
        }]
    }))
    .unwrap()
}

/// Spec-rendered token map (W3-r2): what the hop injects. Any value
/// works here — production derives it as `wfop_token(pepper, key)`.
fn spec_tokens() -> BTreeMap<String, BTreeMap<String, String>> {
    BTreeMap::from([(
        "w1".to_string(),
        BTreeMap::from([(
            "order-pipe".to_string(),
            vrt_config::workflows::wfop_token("e2e-pepper", &"u".repeat(64)),
        )]),
    )])
}

async fn serve_on(app: axum::Router) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    addr
}

/// Bare-TCP facade stub: asserts workerd-strict wire shape (origin-form
/// + Host + token header), returns a canned JSON response. `seen`
/// captures the request for header assertions.
async fn stub_facade(
    respond_body: &'static str,
    respond_status: u16,
) -> (WorkerEndpoint, Arc<StdMutex<Vec<String>>>) {
    let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
    let seen2 = seen.clone();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = l.accept().await.unwrap();
            let seen = seen2.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                seen.lock().unwrap().push(req.clone());
                let line = req.lines().next().unwrap_or("");
                assert!(
                    line.starts_with("POST /vrt/wfops "),
                    "request line must be origin-form: {line:?}"
                );
                assert!(
                    req.to_ascii_lowercase().contains("host:"),
                    "Host header required by workerd: {req:?}"
                );
                assert!(
                    req.to_ascii_lowercase().contains("x-vrt-wfop-token:"),
                    "facade token header must be forwarded: {req:?}"
                );
                let reason = if respond_status == 200 { "OK" } else { "ERR" };
                let resp = format!(
                    "HTTP/1.1 {respond_status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    respond_body.len(),
                    respond_body
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });
    (WorkerEndpoint::Tcp(addr), seen)
}

fn host_with_wfops(endpoint: WorkerEndpoint) -> Arc<TenantRegistry> {
    let rt = TenantRuntime::new("t1");
    let mut eps = BTreeMap::new();
    eps.insert("w1".to_string(), endpoint);
    rt.test_install_wfops(eps, spec_tokens());
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt);
    registry
}

fn executor_to(host_addr: &str) -> WfOpsExecutor {
    WfOpsExecutor::new(
        &[("local".into(), host_addr.to_string())],
        Duration::from_secs(5),
    )
    .unwrap()
}

/// executor -> vrt-host (production route) -> stub facade -> whitelisted result -> DONE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wfops_get_through_real_vrt_host() {
    let (endpoint, seen) =
        stub_facade(r#"{"status":"paused","internal":"must-not-leak"}"#, 200).await;
    let host_addr = serve_on(vrt_host::wfops::router(host_with_wfops(endpoint))).await;
    let exec = executor_to(&host_addr);

    let d = desired();
    let ack = exec
        .execute(
            &op(1, "WF_GET", payload(json!({"instanceId": "inst-1"}))),
            Some(&d),
        )
        .await;
    assert_eq!(ack.status, "DONE");
    let r = ack.result.unwrap();
    assert_eq!(r["status"], json!("paused"));
    // Unknown engine fields never reach the control plane.
    assert!(r.get("internal").is_none());

    // W3-r2: the caller sends NO token — the hop injects the
    // serving spec's map entry for (w1, order-pipe).
    let req = &seen.lock().unwrap()[0];
    let want = format!("x-vrt-wfop-token: {}", spec_tokens()["w1"]["order-pipe"]);
    assert!(
        req.to_ascii_lowercase()
            .contains(&want.to_ascii_lowercase()),
        "derived token forwarded: {req:?}"
    );
    // Op body is the forwarded `{op,workflow,instanceId}` — never the
    // cell-op row (no uniqueKey, no dedupKey).
    let body_start = req.find("\r\n\r\n").unwrap();
    let body: Value = serde_json::from_str(&req[body_start + 4..]).unwrap();
    assert_eq!(body["op"], json!("get"));
    assert_eq!(body["workflow"], json!("order-pipe"));
    assert_eq!(body["instanceId"], json!("inst-1"));
    assert!(body.get("wfUniqueKey").is_none());
    assert!(body.get("dedupKey").is_none());
}

/// Action op forwards `{op:"action",action:"pause"}`; whitelist keeps
/// only `status`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wfops_action_through_real_vrt_host() {
    let (endpoint, seen) = stub_facade(r#"{"status":"paused","logs":[1]}"#, 200).await;
    let host_addr = serve_on(vrt_host::wfops::router(host_with_wfops(endpoint))).await;
    let exec = executor_to(&host_addr);
    let d = desired();
    let ack = exec
        .execute(
            &op(
                2,
                "WF_ACTION",
                payload(json!({"instanceId": "i-9", "action": "pause"})),
            ),
            Some(&d),
        )
        .await;
    assert_eq!(ack.status, "DONE");
    assert_eq!(ack.result.unwrap()["status"], json!("paused"));
    let req = &seen.lock().unwrap()[0];
    let body: Value = serde_json::from_str(&req[req.find("\r\n\r\n").unwrap() + 4..]).unwrap();
    assert_eq!(body["action"], json!("pause"));
}

/// Tenant missing entirely → typed 404, executor surfaces FAILED (not
/// a transport retry-storm).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wfops_unknown_tenant_fails_op() {
    let registry = TenantRegistry::new();
    let host_addr = serve_on(vrt_host::wfops::router(registry)).await;
    let exec = executor_to(&host_addr);
    let d = desired();
    let ack = exec
        .execute(
            &op(3, "WF_GET", payload(json!({"instanceId": "i"}))),
            Some(&d),
        )
        .await;
    assert_eq!(ack.status, "FAILED");
    assert_eq!(
        ack.error.as_deref(),
        Some("VRT_WFOPS_UNKNOWN_WORKER"),
        "{:?}",
        ack.error
    );
}

/// Tenant ready but the worker declares no workflows → typed 404
/// NO_WORKFLOWS — a stale-generation op fails closed, never retries
/// forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wfops_no_facade_fails_op() {
    let rt = TenantRuntime::new("t1");
    // MIG-11-r2: a generation must actually be serving (a different
    // worker declares workflows) — with nothing serving the miss is
    // the retryable 503 WORKER_NOT_READY, not this genuine 404.
    rt.test_install_wfops(
        std::collections::BTreeMap::from([(
            "other".to_string(),
            vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
        )]),
        std::collections::BTreeMap::new(),
    );
    rt.test_set_state(vrt_host::state::TenantState::Ready);
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt);
    let host_addr = serve_on(vrt_host::wfops::router(registry)).await;
    let exec = executor_to(&host_addr);
    let d = desired();
    let ack = exec
        .execute(
            &op(4, "WF_GET", payload(json!({"instanceId": "i"}))),
            Some(&d),
        )
        .await;
    assert_eq!(ack.status, "FAILED");
    assert_eq!(ack.error.as_deref(), Some("VRT_WFOPS_NO_WORKFLOWS"));
}

/// Facade non-2xx typed error surfaces its `error.code` as the op's
/// failure reason — never retried (typed, not transport).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wfops_facade_typed_error_no_retry() {
    let calls = Arc::new(AtomicU64::new(0));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let calls2 = calls.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = l.accept().await.unwrap();
            let calls = calls2.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await.unwrap();
                calls.fetch_add(1, Ordering::Relaxed);
                let body = r#"{"error":{"code":"VRT_WFOPS_FORBIDDEN","message":"bad token"}}"#;
                let resp = format!(
                    "HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });
    let host_addr = serve_on(vrt_host::wfops::router(host_with_wfops(
        WorkerEndpoint::Tcp(addr),
    )))
    .await;
    let exec = executor_to(&host_addr);
    let d = desired();
    let ack = exec
        .execute(
            &op(
                5,
                "WF_ACTION",
                payload(json!({"instanceId": "i", "action": "resume"})),
            ),
            Some(&d),
        )
        .await;
    assert_eq!(ack.status, "FAILED");
    assert_eq!(ack.error.as_deref(), Some("VRT_WFOPS_FORBIDDEN"));
    // Exactly ONE call — a typed error must not burn the retry budget.
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}


// ---------------- MIG-11: serialized-switch window ----------------

/// MIG-11: a wfops op inside the serialized-switch window HOLDS
/// (bounded) instead of mis-reporting `VRT_WFOPS_NO_WORKFLOWS`; a
/// window that outlives the budget answers 503 `VRT_WFOPS_SWITCHING`
/// + `Retry-After`. MIG-11-r2: a miss after the gate clears splits
/// typed — NOTHING serving (launch failed / crashed) is a retryable
/// 503 `VRT_WFOPS_WORKER_NOT_READY`, while a serving generation that
/// never declared the worker keeps the genuine 404
/// `VRT_WFOPS_NO_WORKFLOWS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wfops_switching_window_is_503_then_404() {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let rt = TenantRuntime::new("t1");
    rt.test_set_state(vrt_host::state::TenantState::Ready);
    rt.switch_gate.begin();
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let hold = vrt_host::supervisor::SwitchHoldParams {
        budget: Duration::from_millis(150),
        max_reqs: 8,
        max_bytes: 1 << 20,
    };
    let req = || {
        axum::http::Request::builder()
            .method("POST")
            .uri("/v1/wfops")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "tenantId": "t1",
                    "workerId": "w1",
                    "op": {"op": "get", "workflow": "order-pipe", "instanceId": "i-1"}
                })
                .to_string(),
            ))
            .unwrap()
    };
    let res = vrt_host::wfops::router_with_hold(registry.clone(), hold)
        .oneshot(req())
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 503);
    assert!(
        res.headers().get("retry-after").is_some(),
        "SWITCHING must carry Retry-After"
    );
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_WFOPS_SWITCHING"),
        "{body}"
    );
    rt.switch_gate.end();
    // MIG-11-r2: the gate cleared but NOTHING serves (the generation
    // never installed — launch failed). That is a retryable
    // not-ready, never the fake-absence 404 the pre-fix code emitted.
    let res = vrt_host::wfops::router_with_hold(registry.clone(), hold)
        .oneshot(req())
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 503);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_WFOPS_WORKER_NOT_READY"),
        "no serving generation after gate end → 503, not 404: {body}"
    );
    // A generation that IS serving but never declared `w1` keeps the
    // genuine-absence contract.
    rt.test_install_wfops(
        std::collections::BTreeMap::from([(
            "other".to_string(),
            vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
        )]),
        std::collections::BTreeMap::new(),
    );
    let res = vrt_host::wfops::router_with_hold(registry, hold)
        .oneshot(req())
        .await
        .unwrap();
    assert_eq!(res.status().as_u16(), 404);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_WFOPS_NO_WORKFLOWS"),
        "{body}"
    );
}
