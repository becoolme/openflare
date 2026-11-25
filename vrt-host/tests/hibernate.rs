//! Tenant-level scale-to-zero (P0) — real supervisor + vrt-stub-workerd
//! behind the real ingress. Spec: doc/platform/workers-runtime-scale-to-zero.md §9.2.

mod common;

use common::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use vrt_host::hibernate::{BlockerChain, FixedWindowPolicy, ScaleToZero, WakeCoordinator};
use vrt_host::ingress::IngressState;
use vrt_host::route::RouteTable;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_hibernates_then_http_wake_serves() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    assert!(rt.state().is_asleep());
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 1);
    // A hibernated exit is expected: no crash, no backoff.
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 200, "wake must serve the held request: {body}");
    assert!(body.contains("stub ok"));
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    assert_eq!(rt.state(), TenantState::Ready);
    ig.stop().await;
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_cause_one_wake() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(12)).await;
    let readies_before = rt.stats().readies.load(Ordering::Relaxed);
    let flights_before = rt.wake.flights_started();

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let addr = ig.addr;
    let mut joins = Vec::new();
    for _ in 0..10 {
        joins.push(tokio::spawn(async move {
            http_get(addr, "app.example.com", "/", &[]).await.0
        }));
    }
    for j in joins {
        assert_eq!(j.await.unwrap(), 200);
    }
    assert_eq!(rt.wake.flights_started() - flights_before, 1, "single flight");
    assert_eq!(
        rt.stats().readies.load(Ordering::Relaxed) - readies_before,
        1,
        "exactly one launch"
    );
    ig.stop().await;
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wake_failure_returns_503() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    // Redeploy-less breakage: a lazily applied spec that can never bind.
    let bad = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_FAIL_BIND", "1")]);
    h.send(SupervisorCmd::Apply(Box::new(bad))).await;
    // Apply on a hibernated tenant is lazy: still asleep.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rt.state().is_asleep());

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let (status, hdrs, _) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 503);
    assert!(
        hdrs.iter().any(|(k, _)| k.eq_ignore_ascii_case("retry-after")),
        "503 carries Retry-After: {hdrs:?}"
    );
    assert!(rt.stats().wake_failures.load(Ordering::Relaxed) >= 1);
    ig.stop().await;
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_request_blocks_hibernation() {
    let mut fx = Fixture::new();
    // Each response takes 1.2 s: idle window (100 ms) elapses mid-request.
    let spec = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_SLOW_MS", "1200")]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(100)));
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let addr = ig.addr;
    let req = tokio::spawn(async move { http_get(addr, "app.example.com", "/", &[]).await.0 });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !rt.state().is_asleep(),
        "must not hibernate with a request in flight"
    );
    assert_eq!(req.await.unwrap(), 200, "in-flight request completes");
    ig.stop().await;
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_off_never_hibernates() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, None);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(rt.state(), TenantState::Ready);
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 0);
    stop(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_while_hibernated_completes() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    stop(&rt, h).await;
}

// ---- Apply racing a wake (Hibernated / Waking) ----

const REV_CHECK: &str = "REV_A,REV_B,REV_C";

/// Which `REV_*` marker env the answering generation carries (the stub
/// echoes presence of the names in `x-stub-env-check`).
fn rev_of(hdrs: &[(String, String)]) -> String {
    let mut present: Vec<String> = hdrs
        .iter()
        .filter(|(k, v)| k.starts_with("x-stub-env-rev_") && v == "present")
        .map(|(k, _)| k.trim_start_matches("x-stub-env-").to_string())
        .collect();
    present.sort();
    present.join(",")
}

async fn get_rev(addr: std::net::SocketAddr) -> (u16, String, Vec<(String, String)>) {
    let (status, hdrs, _) = http_get(
        addr,
        "app.example.com",
        "/",
        &[("x-stub-env-check", REV_CHECK)],
    )
    .await;
    (status, rev_of(&hdrs), hdrs)
}

/// Apply while Hibernated is lazy; the HTTP wake must launch the NEWEST
/// desired (never the pre-sleep revision) and `serving_digest` follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_while_hibernated_wakes_into_new_revision() {
    let mut fx = Fixture::new();
    let spec_a = fx.stub_spec("t1", &["w1"], &[("REV_A", "1")]);
    let digest_a = spec_a.config_digest.clone();
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec_a, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    assert_eq!(rt.serving_digest().as_deref(), Some(digest_a.as_str()));

    let spec_b = fx.stub_spec("t1", &["w1"], &[("REV_B", "1")]);
    let digest_b = spec_b.config_digest.clone();
    h.send(SupervisorCmd::Apply(Box::new(spec_b))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Lazy: still asleep, no process was started for the Apply.
    assert!(rt.state().is_asleep());
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 0);

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let (status, rev, hdrs) = get_rev(ig.addr).await;
    assert_eq!(status, 200, "{hdrs:?}");
    assert_eq!(rev, "rev_b", "wake must serve the new revision: {hdrs:?}");
    assert_eq!(rt.state(), TenantState::Ready);
    assert_eq!(
        rt.serving_digest().as_deref(),
        Some(digest_b.as_str()),
        "ACK digest reflects the revision actually serving"
    );
    ig.stop().await;
    stop(&rt, h).await;
}

/// Apply arriving mid-wake restarts the launch against the newest
/// desired; the held request is answered (200) by the NEW revision, the
/// serving digest is the new one, and nothing hangs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_during_waking_restarts_into_newest_revision() {
    let mut fx = Fixture::new();
    let spec_a = fx.stub_spec("t1", &["w1"], &[("REV_A", "1")]);
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec_a, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;

    // Lazy slow revision B: the wake launch is held ~2 s before ready.
    let spec_b = fx.stub_spec(
        "t1",
        &["w1"],
        &[("REV_B", "1"), ("VRT_STUB_READY_DELAY_MS", "2000")],
    );
    h.send(SupervisorCmd::Apply(Box::new(spec_b))).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let addr = ig.addr;
    let req = tokio::spawn(async move { get_rev(addr).await });
    wait_state(&rt, TenantState::Waking, Duration::from_secs(5)).await;

    let spec_c = fx.stub_spec("t1", &["w1"], &[("REV_C", "1")]);
    let digest_c = spec_c.config_digest.clone();
    h.send(SupervisorCmd::Apply(Box::new(spec_c))).await;

    let (status, rev, hdrs) = tokio::time::timeout(Duration::from_secs(12), req)
        .await
        .expect("held wake request must not hang")
        .unwrap();
    assert_eq!(status, 200, "{hdrs:?}");
    assert_eq!(
        rev, "rev_c",
        "newest revision serves, not the aborted one: {hdrs:?}"
    );
    wait_state(&rt, TenantState::Ready, Duration::from_secs(5)).await;
    assert_eq!(rt.serving_digest().as_deref(), Some(digest_c.as_str()));
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    // A follow-up request is served by the same new generation.
    let (s2, rev2, _) = get_rev(ig.addr).await;
    assert_eq!((s2, rev2.as_str()), (200, "rev_c"));
    ig.stop().await;
    stop(&rt, h).await;
}

/// Invalid (never-binding) revision arriving mid-wake: fail closed. The
/// held request gets a typed 503 (not a hang, not a 200 from the old
/// revision), the bad digest is never reported as serving, and a later
/// good Apply recovers the tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_apply_during_waking_fails_closed() {
    let mut fx = Fixture::new();
    let spec_a = fx.stub_spec("t1", &["w1"], &[("REV_A", "1")]);
    let digest_a = spec_a.config_digest.clone();
    let rt = TenantRuntime::new("t1");
    let h = spawn_s2z(&rt, spec_a, Some(s2z(300)));
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;

    let spec_b = fx.stub_spec(
        "t1",
        &["w1"],
        &[("REV_B", "1"), ("VRT_STUB_READY_DELAY_MS", "2000")],
    );
    h.send(SupervisorCmd::Apply(Box::new(spec_b))).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let addr = ig.addr;
    let req = tokio::spawn(async move { get_rev(addr).await });
    wait_state(&rt, TenantState::Waking, Duration::from_secs(5)).await;

    let bad = fx.stub_spec(
        "t1",
        &["w1"],
        &[("REV_C", "1"), ("VRT_STUB_FAIL_BIND", "1")],
    );
    let bad_digest = bad.config_digest.clone();
    h.send(SupervisorCmd::Apply(Box::new(bad))).await;

    let (status, rev, hdrs) = tokio::time::timeout(Duration::from_secs(12), req)
        .await
        .expect("held wake request must not hang")
        .unwrap();
    assert_eq!(status, 503, "fail closed, no stale 200: rev={rev} {hdrs:?}");
    assert!(rev.is_empty(), "no generation answered: {hdrs:?}");
    assert!(rt.stats().wake_failures.load(Ordering::Relaxed) >= 1);
    assert_ne!(rt.serving_digest().as_deref(), Some(bad_digest.as_str()));
    // Observable: the pre-sleep revision stays the last serving digest
    // and the tenant is not serving.
    assert_eq!(rt.serving_digest().as_deref(), Some(digest_a.as_str()));
    assert!(!rt.state().can_serve(), "state={:?}", rt.state());

    // Recovery: a fixed revision applied afterwards serves again.
    let spec_d = fx.stub_spec("t1", &["w1"], &[("REV_B", "1")]);
    let digest_d = spec_d.config_digest.clone();
    h.send(SupervisorCmd::Apply(Box::new(spec_d))).await;
    let mut ok = None;
    for _ in 0..40 {
        let (s, rev, _) = get_rev(ig.addr).await;
        if s == 200 {
            ok = Some(rev);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(
        ok.as_deref(),
        Some("rev_b"),
        "tenant recovers after a fixed Apply"
    );
    assert_eq!(rt.serving_digest().as_deref(), Some(digest_d.as_str()));
    ig.stop().await;
    stop(&rt, h).await;
}
