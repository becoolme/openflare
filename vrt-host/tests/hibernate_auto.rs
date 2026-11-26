//! Stage-4 automatic scale-to-zero policy — `[scaleToZero.auto]`:
//! adaptive idle window, per-tenant wake budget, max-sleep bound.
//! Real supervisor + vrt-stub-workerd behind the real ingress
//! (same harness as `hibernate.rs`; spec: `docs/do-hibernation.md` §4).

mod common;

use common::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use vrt_host::hibernate::{AutoParams, AutoPolicy, BlockerChain, ScaleToZero, WakeCoordinator};
use vrt_host::ingress::IngressState;
use vrt_host::route::RouteTable;
use vrt_host::spec::TenantLaunchSpec;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{
    SupervisorCmd, SupervisorDeps, SupervisorHandle, TenantRegistry, TenantRuntime,
};

fn auto_s2z(idle_ms: u64, auto: AutoParams, max_sleep_ms: u64) -> ScaleToZero {
    ScaleToZero {
        policy: Arc::new(AutoPolicy {
            params: auto,
            min_residency: Duration::from_millis(50),
        }),
        auto: Some(auto),
        max_sleep: (max_sleep_ms > 0).then(|| Duration::from_millis(max_sleep_ms)),
        blockers: Arc::new(BlockerChain::p0()),
        coordinator: WakeCoordinator::new(4, 256),
        drain_timeout: Duration::from_millis(1_500),
        wake_deadline: Duration::from_secs(8),
        blocked_recheck: Duration::from_millis(200),
        quiesce_poll: Duration::from_millis(20),
        alarm_wake_grace: Duration::from_secs(60),
    }
}

/// Params tuned for stub-scale timings: every test sleep is shorter
/// than `premature_wake` (so every wake adapts the window upward),
/// sound sleeps do not exist, and the budget window is long enough
/// that counted wakes never expire mid-test.
fn auto_params(floor_ms: u64, wake_budget: u32) -> AutoParams {
    AutoParams {
        floor: Duration::from_millis(floor_ms),
        ceiling: Duration::from_secs(3_600),
        premature_wake: Duration::from_secs(300),
        sound_sleep: Duration::from_secs(600),
        wake_budget,
        wake_budget_window: Duration::from_secs(3_600),
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

fn desired() -> snapshot_protocol::compute_desired::ComputeDesired {
    snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t1", vec![worker("w1", "app.example.com", "d1")])],
        deletions: vec![],
    }
}

fn ingress_for(rt: &Arc<TenantRuntime>) -> Arc<IngressState> {
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let state = IngressState::new(
        registry,
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired()));
    state
}

async fn stop(rt: &Arc<TenantRuntime>, h: SupervisorHandle) {
    h.send(SupervisorCmd::Stop).await;
    wait_state(rt, TenantState::Stopped, Duration::from_secs(8)).await;
    h.join().await;
}

/// Drive one HTTP request through a fresh ingress and keep it running.
async fn http_once(rt: &Arc<TenantRuntime>) -> IngressGuard {
    let state = ingress_for(rt);
    let ig = start_ingress(&state).await;
    let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 200, "wake must serve: {body}");
    ig
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn premature_wake_doubles_idle_window() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    // floor 300ms; every sleep in this test is "premature" (<300s).
    let h = spawn_s2z(&rt, spec, Some(auto_s2z(300, auto_params(300, 0), 0)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    let ig = http_once(&rt).await;
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    // The premature wake doubled the window to 600ms — deterministic
    // check via the stat the eval publishes (no timing race against
    // the adapted deadline).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while rt.stats().s2z_idle_window_ms.load(Ordering::Relaxed) != 600 {
        assert!(
            std::time::Instant::now() < deadline,
            "window never adapted: still {}ms",
            rt.stats().s2z_idle_window_ms.load(Ordering::Relaxed)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // The tenant still hibernates once the adapted window elapses.
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    ig.stop().await;
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wake_budget_keeps_thrasher_resident() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    // Budget of 2 wakes per window: after two wakes the tenant must
    // stop hibernating (budget denies SLEEP, never traffic).
    let h = spawn_s2z(&rt, spec, Some(auto_s2z(300, auto_params(300, 2), 0)));
    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    for i in 1..=2 {
        wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
        let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
        assert_eq!(status, 200, "wake {i} must serve: {body}");
        wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    }
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 2);
    // Budget exhausted: the tenant stays Ready far past the idle
    // window — the policy defers to the refill instant.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(
        rt.state(),
        TenantState::Ready,
        "wake budget exhausted — tenant must stay resident",
    );
    ig.stop().await;
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_sleep_self_wakes_via_flight() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    // maxSleep = 800ms: the tenant wakes itself out of hibernation
    // without any traffic.
    let h = spawn_s2z(&rt, spec, Some(auto_s2z(300, auto_params(300, 0), 800)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    // No ingress at all — the host's own deadline drives the wake.
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    assert_eq!(
        rt.stats().wakes.load(Ordering::Relaxed),
        1,
        "max-sleep must produce a real wake",
    );
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    stop(&rt, h).await;
}
