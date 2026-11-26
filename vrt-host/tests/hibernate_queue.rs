//! Scale-to-zero P1: a queue dispatch wakes a hibernated tenant. Real
//! supervisor + vrt-stub-workerd behind the real `/v1/queue/dispatch`
//! router. Spec: doc/platform/workers-runtime-scale-to-zero.md.

mod common;

use axum::body::Body;
use axum::http::Request;
use common::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use vrt_host::hibernate::{
    BlockerChain, FixedWindowPolicy, ScaleToZero, WakeCoordinator, WakeSourceKind,
};
use vrt_host::spec::TenantLaunchSpec;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{
    SupervisorCmd, SupervisorDeps, SupervisorHandle, TenantRegistry, TenantRuntime,
};

fn s2z(idle_ms: u64) -> ScaleToZero {
    ScaleToZero {
        policy: Arc::new(FixedWindowPolicy {
            idle_window: Duration::from_millis(idle_ms),
            min_residency: Duration::from_millis(50),
        }),
        auto: None,
        max_sleep: None,
        blockers: Arc::new(BlockerChain::p0()),
        coordinator: WakeCoordinator::new(4, 256),
        drain_timeout: Duration::from_millis(1_500),
        wake_deadline: Duration::from_secs(8),
        blocked_recheck: Duration::from_millis(200),
        quiesce_poll: Duration::from_millis(20),
        alarm_wake_grace: Duration::from_secs(60),
    }
}

fn spawn_s2z(
    runtime: &Arc<TenantRuntime>,
    spec: TenantLaunchSpec,
    cfg: Option<ScaleToZero>,
) -> SupervisorHandle {
    let mut p = test_params();
    p.scale_to_zero = cfg;
    vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, SupervisorDeps::default(), p)
}

/// Tenant `t1` with a queue consumer on `w1` (socket
/// `sock-qconsumer-w1`); the stub records every dispatch there.
fn queue_spec(fx: &mut Fixture, rec: &std::path::Path, extra: &[(&str, &str)]) -> TenantLaunchSpec {
    let rec_s = rec.to_str().unwrap().to_string();
    let mut env: Vec<(&str, &str)> = vec![("VRT_STUB_RECORD_DIR", rec_s.as_str())];
    env.extend_from_slice(extra);
    let mut spec = fx.stub_spec("t1", &["w1", "qconsumer-w1"], &env);
    let ep = spec.worker_endpoints.remove("qconsumer-w1").unwrap();
    spec.qconsumer_endpoints.insert("w1".to_string(), ep);
    spec
}

fn log_lines(rec: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(rec.join("sock-qconsumer-w1.log"))
        .map(|s| {
            s.lines()
                .filter(|l| l.contains("/vrt/queue-dispatch"))
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

async fn dispatch(app: &axum::Router) -> (u16, Option<String>, serde_json::Value) {
    let body = serde_json::json!({
        "tenantId": "t1", "workerId": "w1",
        "dispatch": {"queue": "q1", "messages": [{"id": "m1", "body": "00"}]},
    })
    .to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/queue/dispatch")
        .header("content-type", "application/json")
        // A forged budget header must never reach the facade.
        .header("x-vrt-delivery-budget-ms", "999999")
        .body(Body::from(body))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let st = res.status().as_u16();
    let retry = res
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().unwrap().to_string());
    let b = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    (
        st,
        retry,
        serde_json::from_slice(&b).unwrap_or(serde_json::Value::Null),
    )
}

async fn stop(rt: &Arc<TenantRuntime>, h: SupervisorHandle) {
    h.send(SupervisorCmd::Stop).await;
    wait_state(rt, TenantState::Stopped, Duration::from_secs(8)).await;
    h.join().await;
}

fn code(j: &serde_json::Value) -> &str {
    j["error"]["code"].as_str().unwrap_or("")
}

#[test]
fn queue_wake_wait_is_capped_but_http_is_not() {
    let d = Duration::from_secs(30);
    assert_eq!(WakeSourceKind::Queue.max_wait(d), Duration::from_secs(20));
    assert_eq!(WakeSourceKind::Http.max_wait(d), d);
    assert_eq!(WakeSourceKind::Cron.max_wait(d), d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_consumer_tenant_hibernates_when_idle() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = queue_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 1);
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_dispatch_wakes_hibernated_tenant_with_bounded_budget() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = queue_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;

    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let app = vrt_host::queue_dispatch::router(registry);

    let (st, _, j) = dispatch(&app).await;
    assert_eq!(st, 200, "{j}");
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    let lines = log_lines(rec.path());
    assert_eq!(lines.len(), 1, "{lines:?}");
    let budget: u64 = lines[0]
        .rsplit("budget=")
        .next()
        .unwrap()
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("budget header missing: {}", lines[0]));
    assert!(
        (1_000..=55_000).contains(&budget),
        "budget {budget} (forged 999999 must not pass through)"
    );
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_wake_failure_is_typed_503_then_recovers() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = queue_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    let bad = queue_spec(&mut fx, rec.path(), &[("VRT_STUB_FAIL_BIND", "1")]);
    h.send(SupervisorCmd::Apply(Box::new(bad))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rt.state().is_asleep());

    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let app = vrt_host::queue_dispatch::router(registry);

    let (st, retry, j) = dispatch(&app).await;
    assert_eq!(st, 503, "{j}");
    assert_eq!(code(&j), "VRT_QDISP_WAKE_FAILED", "{j}");
    assert_eq!(j["delivered"], false);
    assert_eq!(retry.as_deref(), Some("1"));
    assert!(log_lines(rec.path()).is_empty(), "consumer must not run");
    assert!(rt.stats().wake_failures.load(Ordering::Relaxed) >= 1);

    let good = queue_spec(&mut fx, rec.path(), &[]);
    h.send(SupervisorCmd::Apply(Box::new(good))).await;
    let mut last = (0, None, serde_json::Value::Null);
    for _ in 0..40 {
        last = dispatch(&app).await;
        if last.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(last.0, 200, "{}", last.2);
    assert_eq!(log_lines(rec.path()).len(), 1, "exactly one delivery");
    stop(&rt, h).await;
}
