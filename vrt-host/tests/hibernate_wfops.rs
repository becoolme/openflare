//! Scale-to-zero P1: a workflow op (`/v1/wfops`) wakes a hibernated
//! tenant. Real supervisor + vrt-stub-workerd behind the real wfops
//! router. Workflow tenants themselves never hibernate (the engine is a
//! DO inside workerd) — the wake covers a tenant that slept BEFORE a
//! workflow was added (lazy Apply) and any wfops landing on a sleeper.
//! Spec: doc/platform/workers-runtime-scale-to-zero.md.

mod common;

use axum::body::Body;
use axum::http::Request;
use common::*;
use std::collections::BTreeMap;
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
        blockers: Arc::new(BlockerChain::p0()),
        coordinator: WakeCoordinator::new(4, 256),
        drain_timeout: Duration::from_millis(1_500),
        wake_deadline: Duration::from_secs(8),
        blocked_recheck: Duration::from_millis(200),
        quiesce_poll: Duration::from_millis(20),
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

/// Tenant `t1` with a plain worker `w1` and NO workflow.
fn plain_spec(fx: &mut Fixture, rec: &std::path::Path, extra: &[(&str, &str)]) -> TenantLaunchSpec {
    let rec_s = rec.to_str().unwrap().to_string();
    let mut env: Vec<(&str, &str)> = vec![("VRT_STUB_RECORD_DIR", rec_s.as_str())];
    env.extend_from_slice(extra);
    fx.stub_spec("t1", &["w1"], &env)
}

/// Tenant `t1` whose `w1` declares workflow `wf1` (socket
/// `sock-wfops-w1`; token in `wfop_tokens`).
fn wf_spec(fx: &mut Fixture, rec: &std::path::Path, extra: &[(&str, &str)]) -> TenantLaunchSpec {
    let rec_s = rec.to_str().unwrap().to_string();
    let mut env: Vec<(&str, &str)> = vec![("VRT_STUB_RECORD_DIR", rec_s.as_str())];
    env.extend_from_slice(extra);
    let mut spec = fx.stub_spec("t1", &["w1", "wfops-w1"], &env);
    let ep = spec.worker_endpoints.remove("wfops-w1").unwrap();
    spec.wfops_endpoints.insert("w1".to_string(), ep);
    let mut toks = BTreeMap::new();
    toks.insert("wf1".to_string(), "tok-wf1".to_string());
    spec.wfop_tokens.insert("w1".to_string(), toks);
    spec
}

fn log_lines(rec: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(rec.join("sock-wfops-w1.log"))
        .map(|s| {
            s.lines()
                .filter(|l| l.contains("/vrt/wfops"))
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

async fn wfop(app: &axum::Router) -> (u16, Option<String>, serde_json::Value) {
    let body = serde_json::json!({
        "tenantId": "t1", "workerId": "w1",
        "op": {"op": "get", "workflow": "wf1", "instanceId": "i1"},
    })
    .to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/wfops")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let st = res.status().as_u16();
    let retry = res
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().unwrap().to_string());
    let b = axum::body::to_bytes(res.into_body(), 16384).await.unwrap();
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

fn app_for(rt: &Arc<TenantRuntime>) -> axum::Router {
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    vrt_host::wfops::router(registry)
}

#[test]
fn workflow_wake_wait_is_capped_below_controller_timeout() {
    let d = Duration::from_secs(30);
    assert_eq!(WakeSourceKind::Workflow.max_wait(d), Duration::from_secs(8));
    assert_eq!(WakeSourceKind::Http.max_wait(d), d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wfops_wakes_hibernated_tenant_once_then_normal_path() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = plain_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    let app = app_for(&rt);

    // No workflow declared: the wake ran and the normal checks answered.
    let (st, _, j) = wfop(&app).await;
    assert_eq!(st, 404, "{j}");
    assert_eq!(code(&j), "VRT_WFOPS_NO_WORKFLOWS", "{j}");
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    assert!(log_lines(rec.path()).is_empty());
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wfops_wake_failure_is_typed_503_then_recovers() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = plain_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    // Lazy Apply while asleep: a broken spec that DECLARES the workflow.
    let bad = wf_spec(&mut fx, rec.path(), &[("VRT_STUB_FAIL_BIND", "1")]);
    h.send(SupervisorCmd::Apply(Box::new(bad))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rt.state().is_asleep());
    let app = app_for(&rt);

    let (st, retry, j) = wfop(&app).await;
    assert_eq!(st, 503, "{j}");
    assert_eq!(code(&j), "VRT_WFOPS_WAKE_FAILED", "{j}");
    assert_eq!(retry.as_deref(), Some("1"));
    assert!(log_lines(rec.path()).is_empty(), "op must not be forwarded");
    assert!(rt.stats().wake_failures.load(Ordering::Relaxed) >= 1);

    let good = wf_spec(&mut fx, rec.path(), &[]);
    h.send(SupervisorCmd::Apply(Box::new(good))).await;
    let mut last = (0, None, serde_json::Value::Null);
    for _ in 0..40 {
        last = wfop(&app).await;
        if last.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(last.0, 200, "{}", last.2);
    assert_eq!(log_lines(rec.path()).len(), 1, "exactly one forward");
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_tenant_never_hibernates() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = wf_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(200)));
    wait_state(&rt, TenantState::Ready, Duration::from_secs(10)).await;
    // Several idle windows pass; the structural blocker keeps it up.
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    assert_eq!(rt.state(), TenantState::Ready);
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 0);
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_added_while_asleep_is_live_after_wfops_wake() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = plain_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    // Apply is lazy while asleep: the new spec only takes effect on wake.
    let with_wf = wf_spec(&mut fx, rec.path(), &[]);
    h.send(SupervisorCmd::Apply(Box::new(with_wf))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rt.state().is_asleep());
    let app = app_for(&rt);

    let (st, _, _) = wfop(&app).await;
    assert_eq!(st, 200, "wake must install the NEW spec before routing");
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    let lines = log_lines(rec.path());
    assert_eq!(lines.len(), 1, "{lines:?}");
    stop(&rt, h).await;
}
