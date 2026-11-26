//! Scale-to-zero P1: a cron trigger wakes a hibernated tenant. Real
//! supervisor + vrt-stub-workerd behind the real `/v1/cron/trigger`
//! router. Spec: doc/platform/workers-runtime-scale-to-zero.md.

mod common;

use axum::body::Body;
use axum::http::Request;
use common::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use vrt_host::hibernate::{BlockerChain, FixedWindowPolicy, ScaleToZero, WakeCoordinator};
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

/// Tenant `t1` declaring cron on `w1` (socket contract:
/// `sock-cron-<workerId>`); the stub records every dispatch under
/// `rec/sock-cron-w1.log`.
fn cron_spec(fx: &mut Fixture, rec: &std::path::Path, extra: &[(&str, &str)]) -> TenantLaunchSpec {
    let rec_s = rec.to_str().unwrap().to_string();
    let mut env: Vec<(&str, &str)> = vec![("VRT_STUB_RECORD_DIR", rec_s.as_str())];
    env.extend_from_slice(extra);
    let mut spec = fx.stub_spec("t1", &["w1", "cron-w1"], &env);
    let ep = spec.worker_endpoints.remove("cron-w1").unwrap();
    spec.cron_endpoints.insert("w1".to_string(), ep);
    spec
}

fn deliveries(rec: &std::path::Path) -> usize {
    std::fs::read_to_string(rec.join("sock-cron-w1.log"))
        .map(|s| s.lines().filter(|l| l.contains("/vrt/cron-trigger")).count())
        .unwrap_or(0)
}

async fn trigger(app: &axum::Router, minute: u32) -> (u16, serde_json::Value) {
    let scheduled = format!("2026-01-01T00:{minute:02}:00Z");
    let key = format!("w1:v1:sch1:{scheduled}");
    let body = serde_json::json!({
        "tenantId": "t1", "workerId": "w1", "versionId": "v1",
        "scheduleId": "sch1", "scheduledTime": scheduled,
        "cron": "* * * * *", "leaseEpoch": 0,
    })
    .to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/cron/trigger")
        .header("content-type", "application/json")
        .header("x-vrt-cron-idempotency-key", key)
        .body(Body::from(body))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let st = res.status().as_u16();
    let b = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    (
        st,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_tenant_hibernates_when_idle() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = cron_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 1);
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_trigger_wakes_hibernated_tenant_and_dedupes() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = cron_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;

    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let app = vrt_host::cron_trigger::router(registry);

    let (st, j) = trigger(&app, 0).await;
    assert_eq!(st, 200, "{j}");
    assert_eq!(j["delivered"], true, "{j}");
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    assert_eq!(deliveries(rec.path()), 1);

    // Same key after the wake: deduped, zero extra deliveries.
    let (st, j) = trigger(&app, 0).await;
    assert_eq!(st, 200, "{j}");
    assert_eq!(j["duplicate"], true, "{j}");
    assert_eq!(deliveries(rec.path()), 1);
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_wake_failure_is_typed_503_and_key_not_consumed() {
    let mut fx = Fixture::new();
    let rec = tempfile::tempdir().unwrap();
    let spec = cron_spec(&mut fx, rec.path(), &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    let bad = cron_spec(&mut fx, rec.path(), &[("VRT_STUB_FAIL_BIND", "1")]);
    h.send(SupervisorCmd::Apply(Box::new(bad))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rt.state().is_asleep());

    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let app = vrt_host::cron_trigger::router(registry);

    let (st, j) = trigger(&app, 1).await;
    assert_eq!(st, 503, "{j}");
    assert_eq!(code(&j), "VRT_CRON_WORKER_NOT_READY", "{j}");
    assert!(j.to_string().contains("wake failed"), "{j}");
    assert_eq!(j["delivered"], false);
    assert_eq!(deliveries(rec.path()), 0);
    assert!(rt.stats().wake_failures.load(Ordering::Relaxed) >= 1);

    // Fix the tenant; the SAME key now delivers exactly once.
    let good = cron_spec(&mut fx, rec.path(), &[]);
    h.send(SupervisorCmd::Apply(Box::new(good))).await;
    let mut last = (0, serde_json::Value::Null);
    for _ in 0..40 {
        last = trigger(&app, 1).await;
        if last.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(last.0, 200, "{}", last.1);
    assert_eq!(last.1["delivered"], true, "{}", last.1);
    assert_eq!(deliveries(rec.path()), 1, "key was not burned by the failure");
    stop(&rt, h).await;
}
