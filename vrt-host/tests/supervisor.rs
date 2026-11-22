//! Supervisor lifecycle tests — launch/ready, timeouts, crash restart,
//! circuit breaker + half-open, tenant isolation, version switching,
//! drain + forced kill, global shutdown. Real stub children, ms-scale
//! params, explicit cleanup at the end of every test.

mod common;

use common::*;
use std::sync::atomic::Ordering;
use std::time::Duration;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{SupervisorCmd, TenantRuntime};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_reaches_ready_and_serves() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    assert_eq!(runtime.worker_ids(), vec!["w1".to_string()]);
    let slot = runtime.endpoint_slot("w1").expect("slot");
    // Endpoint was resolved from the control event (port != 0).
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => assert_ne!(addr.port(), 0),
        _ => panic!("expected tcp endpoint"),
    }
    assert_eq!(runtime.stats().readies.load(Ordering::Relaxed), 1);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_timeout_kills_child_and_retries() {
    let mut fx = Fixture::new();
    // `never` mode: binds nothing, reports nothing → ready deadline.
    let spec = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_MODE", "never")]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_for(
        || runtime.stats().ready_timeouts.load(Ordering::Relaxed) >= 1,
        Duration::from_secs(10),
        "ready timeout",
    )
    .await;
    // Backoff-gated retry is in flight or pending — tenant never Ready.
    assert_ne!(runtime.state(), TenantState::Ready);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_restarts_with_backoff() {
    init_logs();
    let mut fx = Fixture::new();
    // Ready, then die after 150 ms → crash detection → restart.
    let spec = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_MODE", "die:150")]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    wait_for(
        || runtime.stats().crashes.load(Ordering::Relaxed) >= 2,
        Duration::from_secs(15),
        "second crash (restart attempted)",
    )
    .await;
    // Every restart becomes Ready first, then crashes again.
    assert!(runtime.stats().readies.load(Ordering::Relaxed) >= 2);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn circuit_opens_then_half_open_recovers() {
    let mut fx = Fixture::new();
    // Always crashing → threshold (3) → CircuitOpen.
    let spec = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_MODE", "crash")]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::CircuitOpen, Duration::from_secs(15)).await;
    assert!(runtime.stats().circuit_opens.load(Ordering::Relaxed) >= 1);
    // Apply a fixed spec — the half-open attempt after cooldown uses it.
    let good = fx.stub_spec("t1", &["w1"], &[]);
    handle.send(SupervisorCmd::Apply(Box::new(good))).await;
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(15)).await;
    assert_eq!(
        runtime.stats().half_open_attempts.load(Ordering::Relaxed),
        1,
        "exactly one half-open attempt"
    );
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_switch_keeps_old_generation_serving() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    let old_slot = runtime.endpoint_slot("w1").expect("old slot");
    let old_endpoint = old_slot.endpoint().clone();
    drop(old_slot);

    // Switch to a spec that never becomes ready.
    let bad = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_MODE", "never")]);
    handle.send(SupervisorCmd::Apply(Box::new(bad))).await;
    wait_for(
        || runtime.stats().switch_fail.load(Ordering::Relaxed) >= 1,
        Duration::from_secs(10),
        "switch failure",
    )
    .await;
    // Still Ready, still the OLD endpoint — no service gap.
    assert_eq!(runtime.state(), TenantState::Ready);
    let slot = runtime.endpoint_slot("w1").expect("slot still there");
    assert_eq!(slot.endpoint(), &old_endpoint);
    // The old process still answers.
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, _, body) = http_get(*addr, "x.test", "/", &[]).await;
            assert_eq!(status, 200);
            assert!(body.contains("stub ok"));
        }
        _ => panic!("expected tcp"),
    }
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// MIG-7: a failed SWITCH must surface the sanitized stderr tail —
/// `last_error` carries `reason: detail` (the 2026-11-16 incident
/// needed pod-shell access only because the switch path dropped the
/// detail). The stub's `VRT_STUB_STDERR` line proves propagation AND
/// env-needle redaction: the marker survives, the value is `***`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_switch_last_error_carries_sanitized_stderr_tail() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    // Switch to a generation that prints a marker+secret to stderr
    // then hangs forever → ready-timeout SpawnFailure with a tail.
    let bad = fx.stub_spec(
        "t1",
        &["w1"],
        &[
            ("VRT_STUB_MODE", "never"),
            ("VRT_STUB_STDERR", "s3cr3t-token-value-123456"),
        ],
    );
    handle.send(SupervisorCmd::Apply(Box::new(bad))).await;
    wait_for(
        || runtime.stats().switch_fail.load(Ordering::Relaxed) >= 1,
        Duration::from_secs(15),
        "switch failure",
    )
    .await;
    let err = runtime.last_error().expect("last_error set on switch failure");
    // `reason: detail` — the sanitized tail reached status.
    assert!(
        err.contains("vrt-stub-stderr:"),
        "stderr detail present in last_error: {err}"
    );
    // The env value itself is a needle — redacted, never leaked.
    assert!(err.contains("***"), "env value redacted: {err}");
    assert!(
        !err.contains("s3cr3t-token-value-123456"),
        "raw secret absent: {err}"
    );
    // Old generation keeps serving through the failed switch.
    assert_eq!(runtime.state(), TenantState::Ready);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_switch_swaps_endpoint_and_drains_old() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    let old_endpoint = runtime.endpoint_slot("w1").unwrap().endpoint().clone();

    let new = fx.stub_spec("t1", &["w1"], &[]);
    handle.send(SupervisorCmd::Apply(Box::new(new))).await;
    wait_for(
        || runtime.stats().switch_ok.load(Ordering::Relaxed) >= 1,
        Duration::from_secs(10),
        "successful switch",
    )
    .await;
    let new_endpoint = runtime.endpoint_slot("w1").unwrap().endpoint().clone();
    assert_ne!(new_endpoint, old_endpoint, "route swapped to new socket");
    // New endpoint answers; old one is SIGTERMd and goes away.
    match (&new_endpoint, &old_endpoint) {
        (
            vrt_host::endpoint::WorkerEndpoint::Tcp(new_addr),
            vrt_host::endpoint::WorkerEndpoint::Tcp(old_addr),
        ) => {
            let (status, _, _) = http_get(*new_addr, "x.test", "/", &[]).await;
            assert_eq!(status, 200);
            wait_for(
                || std::net::TcpStream::connect(old_addr).is_err(),
                Duration::from_secs(5),
                "old listener closed",
            )
            .await;
        }
        _ => panic!("expected tcp"),
    }
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_deadline_force_kills_with_inflight() {
    let mut fx = Fixture::new();
    // Response slower than the drain cap + SIGTERM ignored → the drain
    // deadline hits with in-flight requests → SIGKILL.
    let spec = fx.stub_spec(
        "t1",
        &["w1"],
        &[
            ("VRT_STUB_IGNORE_SIGTERM", "1"),
            ("VRT_STUB_SLOW_MS", "5000"),
        ],
    );
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    let addr = match runtime.endpoint_slot("w1").unwrap().endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(a) => *a,
        _ => panic!("tcp"),
    };
    // Hold a real in-flight guard past the drain deadline — the actual
    // drain mechanism counts acquire()s, not raw connections.
    let guard = runtime.endpoint_slot("w1").unwrap().acquire().unwrap();
    // A real slow request too, so the kill also drops a live connection.
    let req = tokio::spawn(async move { http_get(addr, "x.test", "/", &[]).await });
    handle.send(SupervisorCmd::Stop).await;
    // drain_timeout = 600 ms: in-flight > 0 at deadline → SIGKILL,
    // the connection drops, the tenant reaches Stopped anyway.
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(10)).await;
    assert!(
        runtime.stats().drain_forced.load(Ordering::Relaxed) >= 1,
        "drain deadline exceeded → SIGKILL counted"
    );
    drop(guard);
    let _ = req.await; // connection dropped by SIGKILL
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn circuit_open_tenant_does_not_affect_others() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(EnvAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
            env: std::collections::HashMap::from([
                ("ta".to_string(), vec![("VRT_STUB_MODE", "crash")]),
                ("tb".to_string(), vec![]),
            ]),
        });
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![
            tenant("ta", vec![worker("w1", "a.test", "d1")]),
            tenant("tb", vec![worker("w1", "b.test", "d2")]),
        ],
        deletions: vec![],
};
    host.converge(&desired, &assembler);
    wait_state(
        &registry.get("tb").unwrap(),
        TenantState::Ready,
        Duration::from_secs(8),
    )
    .await;
    wait_state(
        &registry.get("ta").unwrap(),
        TenantState::CircuitOpen,
        Duration::from_secs(15),
    )
    .await;
    // ta is circuit-open; tb still Ready and still answering.
    assert_eq!(registry.get("tb").unwrap().state(), TenantState::Ready);
    let slot = registry.get("tb").unwrap().endpoint_slot("w1").unwrap();
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, _, body) = http_get(*addr, "b.test", "/", &[]).await;
            assert_eq!(status, 200);
            assert!(body.contains("stub ok"));
        }
        _ => panic!("tcp"),
    }
    host.shutdown_all(Duration::from_secs(15)).await;
}

/// MIG-4 assembler: emits `config.capnp` (preflightable) stub specs;
/// any spec still containing `wbad` fails `workerd test` with a
/// `service worker-wbad:` stderr attribution — the real workerd
/// top-level-throw shape. `wbad2..=wbad9` likewise fail when named in
/// `fail_ids`.
struct PreflightAssembler {
    fixture: std::sync::Mutex<Fixture>,
    fail_ids: Vec<String>,
}

impl vrt_host::supervisor::TenantAssembler for PreflightAssembler {
    fn assemble(
        &self,
        tenant: &snapshot_protocol::compute_desired::TenantDesired,
        _revision: u64,
        _generation: u64,
    ) -> Result<vrt_host::spec::TenantLaunchSpec, vrt_host::assemble::AssembleError> {
        let workers: Vec<&str> = tenant
            .workers
            .iter()
            .map(|w| w.worker_id.as_str())
            .collect();
        let mut spec = self
            .fixture
            .lock()
            .unwrap()
            .stub_spec(&tenant.tenant_id, &workers, &[]);
        let mut cfg: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&spec.config_path).unwrap(),
        )
        .unwrap();
        // A worker fails `workerd test` only on its `badv-*` version —
        // rev1 deploys `v-*` (clean) so an old generation can serve;
        // rev2 deploys `badv-*` (top-level throw) to trigger the
        // exclusion path.
        let failing: Vec<&String> = tenant
            .workers
            .iter()
            .filter(|w| {
                self.fail_ids.contains(&w.worker_id)
                    && w.active_version.version_id.starts_with("badv-")
            })
            .map(|w| &w.worker_id)
            .collect();
        if let Some(bad) = failing.first() {
            cfg["testFail"] = serde_json::Value::String(format!(
                "service worker-{bad}: Uncaught Error: top-level boom"
            ));
        }
        let capnp = spec.work_dir.join("config.capnp");
        std::fs::write(&capnp, serde_json::to_string_pretty(&cfg).unwrap()).unwrap();
        spec.config_path = capnp;
        Ok(spec)
    }

    fn params_fingerprint(&self) -> Result<String, vrt_host::assemble::AssembleError> {
        Ok("stub".to_string())
    }
}

fn desired_with(
    tenant_id: &str,
    worker_ids: &[&str],
    revision: u64,
    bad_ids: &[&str],
) -> snapshot_protocol::compute_desired::ComputeDesired {
    snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision,
        tenants: vec![tenant(
            tenant_id,
            worker_ids
                .iter()
                .map(|id| {
                    let mut w = worker(id, &format!("{id}.test"), "d1");
                    // `badv-*` marks the broken top-level deploy; `v-*`
                    // is clean. Version ids differ per worker+revision.
                    w.active_version.version_id = if bad_ids.contains(id) {
                        format!("badv-{id}-r{revision}")
                    } else {
                        format!("v-{id}-r{revision}")
                    };
                    w
                })
                .collect(),
        )],
        deletions: vec![],
    }
}

/// MIG-4: a config-load failure attributed to `service worker-wbad`
/// excludes ONLY that worker — the sibling still switches onto the
/// new generation and the excluded worker is reported (never routed).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusion_rebuild_serves_siblings_and_marks_bad_worker() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(PreflightAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
            fail_ids: vec!["wbad".to_string()],
        });
    // rev1: both workers serve clean versions.
    host.converge(&desired_with("ta", &["wgood", "wbad"], 1, &[]), &assembler);
    let rt = registry.get("ta").unwrap();
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    assert_eq!(rt.worker_ids().len(), 2);
    // rev2: wbad's new version throws top-level → the whole-tenant
    // config fails `workerd test`; the attributed `service worker-wbad`
    // line excludes ONLY wbad from the rebuilt generation.
    host.converge(
        &desired_with("ta", &["wgood", "wbad"], 2, &["wbad"]),
        &assembler,
    );
    wait_for(
        || !rt.excluded_workers().is_empty(),
        Duration::from_secs(15),
        "wbad exclusion",
    )
    .await;
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    // The reduced generation runs wgood only.
    assert_eq!(rt.excluded_workers(), vec!["wbad".to_string()]);
    assert_eq!(rt.worker_ids(), vec!["wgood".to_string()]);
    let slot = rt.endpoint_slot("wgood").expect("wgood routed");
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, _, body) = http_get(*addr, "wgood.test", "/", &[]).await;
            assert_eq!(status, 200);
            assert!(body.contains("stub ok"));
        }
        _ => panic!("expected tcp"),
    }
    drop(slot);
    assert!(
        rt.endpoint_slot("wbad").is_none(),
        "excluded worker must not be routed"
    );
    // readyz names the excluded worker as the tenant failure.
    let status = vrt_host::status::new_status();
    {
        let mut inner = status.write().unwrap();
        inner.revision = Some(2);
        inner.assigned = vec![
            ("ta".to_string(), "wgood".to_string()),
            ("ta".to_string(), "wbad".to_string()),
        ];
    }
    let view = vrt_host::status::readyz_view(&status, &registry);
    let f = view
        .tenant_failures
        .iter()
        .find(|f| f.tenant == "ta")
        .expect("tenant failure reported");
    assert_eq!(f.worker_ids, vec!["wbad".to_string()]);
    host.shutdown_all(Duration::from_secs(15)).await;
}

/// MIG-4: exclusion is bounded per desired — attributing more than
/// MAX_EXCLUDED_PER_DESIRED workers falls back to the classic
/// whole-tenant failure path (bounded retries → latch), never an
/// unbounded rebuild loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusion_cap_falls_back_to_switch_failed() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    // 9 failing workers + 1 good one — exceeds MAX_EXCLUDED_PER_DESIRED=8.
    let ids: Vec<String> = (0..9).map(|i| format!("wb{i}")).collect();
    let mut all: Vec<String> = ids.clone();
    all.push("wgood".to_string());
    let all_refs: Vec<&str> = all.iter().map(|s| s.as_str()).collect();
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(PreflightAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
            fail_ids: ids.clone(),
        });
    // rev1: every worker clean → old generation serving.
    host.converge(&desired_with("ta", &all_refs, 1, &[]), &assembler);
    let rt = registry.get("ta").unwrap();
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    // rev2: 9 bad workers — beyond MAX_EXCLUDED_PER_DESIRED (8), the
    // rebuild loop must give up and fall back to the bounded
    // switch-retry path; the tenant then latches SwitchFailed with
    // the OLD generation still serving.
    host.converge(&desired_with("ta", &all_refs, 2, &ids.iter().map(|s| s.as_str()).collect::<Vec<_>>()), &assembler);
    wait_state(&rt, TenantState::SwitchFailed, Duration::from_secs(45)).await;
    let status = vrt_host::status::new_status();
    {
        let mut inner = status.write().unwrap();
        inner.revision = Some(2);
        inner.assigned = all
            .iter()
            .map(|id| ("ta".to_string(), id.clone()))
            .collect();
    }
    let view = vrt_host::status::readyz_view(&status, &registry);
    let f = view
        .tenant_failures
        .iter()
        .find(|f| f.tenant == "ta")
        .expect("tenant failure reported");
    assert!(
        !f.worker_ids.is_empty(),
        "attributed workers must be named in the latched failure"
    );
    host.shutdown_all(Duration::from_secs(15)).await;
}

/// MIG-4: when the reduced generation ALSO fails (a second worker
/// breaks too), the loop still terminates — each failure either adds
/// an exclusion (≤8) or gives up; here both are attributable so the
/// reduced spec [wgood] launches after excluding wbad1+wbad2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusion_iterates_until_clean_or_gives_up() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(PreflightAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
            fail_ids: vec!["wbad1".to_string(), "wbad2".to_string()],
        });
    host.converge(
        &desired_with("ta", &["wgood", "wbad1", "wbad2"], 1, &[]),
        &assembler,
    );
    let rt = registry.get("ta").unwrap();
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    host.converge(
        &desired_with("ta", &["wgood", "wbad1", "wbad2"], 2, &["wbad1", "wbad2"]),
        &assembler,
    );
    // The tenant was already Ready — wait for the exclusion verdict
    // itself, not the (stale) state.
    wait_for(
        || rt.excluded_workers().len() == 2,
        Duration::from_secs(20),
        "two exclusions",
    )
    .await;
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    let mut excluded = rt.excluded_workers();
    excluded.sort();
    assert_eq!(excluded, vec!["wbad1".to_string(), "wbad2".to_string()]);
    assert_eq!(rt.worker_ids(), vec!["wgood".to_string()]);
    host.shutdown_all(Duration::from_secs(15)).await;
}

/// MIG-5: the control plane's automatic rollback republishes the OLD
/// version under a NEW desired digest — the digest-keyed exclusion
/// state must reset and the previously excluded worker must re-enter
/// the serving generation (Cloudflare semantics: the proven-good
/// version keeps serving, the bad deploy never took it offline).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_desired_reincludes_excluded_worker() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(PreflightAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
            fail_ids: vec!["wbad".to_string()],
        });
    // rev1: both workers clean.
    host.converge(&desired_with("ta", &["wgood", "wbad"], 1, &[]), &assembler);
    let rt = registry.get("ta").unwrap();
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    // rev2: wbad's bad version → excluded; wgood alone keeps serving.
    host.converge(
        &desired_with("ta", &["wgood", "wbad"], 2, &["wbad"]),
        &assembler,
    );
    wait_for(
        || !rt.excluded_workers().is_empty(),
        Duration::from_secs(15),
        "wbad exclusion",
    )
    .await;
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    assert_eq!(rt.excluded_workers(), vec!["wbad".to_string()]);
    assert!(rt.endpoint_slot("wbad").is_none());
    // rev3: the rollback — wbad's active version is a `v-*` id again
    // (the proven-good bundle content under a fresh version id), so
    // the desired digest differs and the exclusion must be reset.
    host.converge(&desired_with("ta", &["wgood", "wbad"], 3, &[]), &assembler);
    wait_for(
        || rt.excluded_workers().is_empty() && rt.endpoint_slot("wbad").is_some(),
        Duration::from_secs(30),
        "wbad re-inclusion",
    )
    .await;
    wait_state(&rt, TenantState::Ready, Duration::from_secs(15)).await;
    let mut ids = rt.worker_ids();
    ids.sort();
    assert_eq!(ids, vec!["wbad".to_string(), "wgood".to_string()]);
    // The re-included worker actually answers traffic again.
    let slot = rt.endpoint_slot("wbad").expect("wbad routed");
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, _, body) = http_get(*addr, "wbad.test", "/", &[]).await;
            assert_eq!(status, 200);
            assert!(body.contains("stub ok"));
        }
        _ => panic!("expected tcp"),
    }
    drop(slot);
    host.shutdown_all(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_shutdown_drains_all_tenants() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    // Two tenants via converge (stub assembler).
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(TestAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
        });
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![
            tenant("ta", vec![worker("w1", "a.test", "d1")]),
            tenant("tb", vec![worker("w1", "b.test", "d2")]),
        ],
        deletions: vec![],
};
    host.converge(&desired, &assembler);
    wait_state(
        &registry.get("ta").unwrap(),
        TenantState::Ready,
        Duration::from_secs(8),
    )
    .await;
    wait_state(
        &registry.get("tb").unwrap(),
        TenantState::Ready,
        Duration::from_secs(8),
    )
    .await;
    host.shutdown_all(Duration::from_secs(15)).await;
    assert!(registry.tenant_ids().is_empty());
}

/// T34f2/R2: `shutdown_all` with a deadline shorter than the drain
/// chain must still leave no live children — the join timeout aborts
/// the wedged actor, and the task drop runs `GroupChild` guards which
/// SIGKILL the process group. Uses a SIGTERM-IGNORING stub so the
/// group can only die via the abort/drop path (drain would sit in
/// kill_grace otherwise).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_all_deadline_abort_still_kills_process_group() {
    let fx = Fixture::new();
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(EnvAssembler {
            fixture: std::sync::Mutex::new(fx),
            env: std::collections::HashMap::from([(
                "ta".to_string(),
                vec![("VRT_STUB_IGNORE_SIGTERM", "1")],
            )]),
        });
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("ta", vec![worker("w1", "a.test", "d1")])],
        deletions: vec![],
};
    host.converge(&desired, &assembler);
    let runtime = registry.get("ta").unwrap();
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    let pid = runtime.active_pid();
    assert_ne!(pid, 0, "live gen pid");
    // 50 ms << drain_timeout(600)+kill_grace(200): the actor cannot
    // finish draining → abort → GroupChild drop kills the group.
    host.shutdown_all(Duration::from_millis(50)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let alive = unsafe { libc::kill(-(pid as i32), 0) };
    assert_ne!(
        alive, 0,
        "process group alive after deadline-abort shutdown"
    );
}

/// Stub `TenantAssembler` — builds stub specs through the fixture.
struct TestAssembler {
    fixture: std::sync::Mutex<Fixture>,
}

/// Stub assembler with per-tenant `VRT_STUB_*` env.
struct EnvAssembler {
    fixture: std::sync::Mutex<Fixture>,
    env: std::collections::HashMap<String, Vec<(&'static str, &'static str)>>,
}

macro_rules! impl_assembler {
    ($ty:ty, $env:expr) => {
        impl vrt_host::supervisor::TenantAssembler for $ty {
            fn assemble(
                &self,
                tenant: &snapshot_protocol::compute_desired::TenantDesired,
                _revision: u64,
                _generation: u64,
            ) -> Result<vrt_host::spec::TenantLaunchSpec, vrt_host::assemble::AssembleError> {
                let workers: Vec<&str> = tenant
                    .workers
                    .iter()
                    .map(|w| w.worker_id.as_str())
                    .collect();
                #[allow(clippy::redundant_closure_call)]
                let env = ($env)(self, tenant);
                Ok(self
                    .fixture
                    .lock()
                    .unwrap()
                    .stub_spec(&tenant.tenant_id, &workers, &env))
            }

            fn params_fingerprint(&self) -> Result<String, vrt_host::assemble::AssembleError> {
                Ok("stub".to_string())
            }
        }
    };
}

impl_assembler!(TestAssembler, |_s: &TestAssembler, _t| Vec::new());
impl_assembler!(
    EnvAssembler,
    |s: &EnvAssembler, t: &snapshot_protocol::compute_desired::TenantDesired| {
        s.env.get(&t.tenant_id).cloned().unwrap_or_default()
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secrets_never_reach_argv_config_or_debug() {
    const SECRET: &str = "topsecret-value-9f8e7d6c5b";
    let mut fx = Fixture::new();
    let mut spec = fx.stub_spec("t1", &["w1"], &[]);
    spec.env.insert(
        "T1_W1_API_KEY".to_string(),
        vrt_config::SecretValue::new(SECRET),
    );
    // 1) argv never carries secrets (they're env-only).
    let argv = vrt_host::launch::ProcessWorkerdLauncher::argv(&spec);
    for arg in &argv {
        assert!(!arg.contains(SECRET), "secret in argv: {arg}");
    }
    // 2) The spec's Debug (logs, diagnostics) redacts SecretValue.
    let dbg = format!("{spec:?}");
    assert!(!dbg.contains(SECRET), "secret in Debug output");
    assert!(dbg.contains("SecretValue(***)"));
    // 3) The rendered config file does not contain it.
    let config = std::fs::read_to_string(&spec.config_path).unwrap();
    assert!(!config.contains(SECRET), "secret in config file");

    // 4) E2E: the child still RECEIVES it via its cleared environment —
    // the stub reports presence (never the value) on request.
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    let slot = runtime.endpoint_slot("w1").unwrap();
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, headers, body) = http_get(
                *addr,
                "x.test",
                "/",
                &[("x-stub-env-check", "T1_W1_API_KEY")],
            )
            .await;
            assert_eq!(status, 200);
            assert!(
                headers
                    .iter()
                    .any(|(k, v)| k == "x-stub-env-t1_w1_api_key" && v == "present"),
                "env injection failed: {headers:?}"
            );
            assert!(!body.contains(SECRET), "secret echoed in body");
        }
        _ => panic!("tcp"),
    }
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
    // The generation dir is cleaned up at actor stop.
    assert!(
        !std::path::Path::new(&fx.work_root()).join("t1").exists()
            || std::fs::read_dir(fx.work_root().join("t1"))
                .unwrap()
                .next()
                .is_none()
    );
}

// ---- T13: per-attempt work dirs, bounded switch retries, killpg ----

/// T13/M4: failed switch retries are BOUNDED — after
/// `max_switch_failures` the tenant latches `SwitchFailed`, retries
/// stop, and the old generation keeps serving. A subsequent working
/// Apply re-arms the switch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_switch_latches_switch_failed_and_oldservs() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    // New generation with a bogus binary — every switch attempt fails
    // at launch; test_params.max_switch_failures == 3.
    let mut bad = fx.stub_spec("t1", &["w1"], &[]);
    bad.binary = std::path::PathBuf::from("/nonexistent/workerd-bogus");
    let bad_digest = bad.config_digest.clone();
    handle.send(SupervisorCmd::Apply(Box::new(bad))).await;
    wait_state(&runtime, TenantState::SwitchFailed, Duration::from_secs(15)).await;
    // Old generation still serving (SwitchFailed is serve-able).
    assert!(runtime.state().can_serve());
    let slot = runtime.endpoint_slot("w1").expect("slot still routed");
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, _, body) = http_get(*addr, "x.test", "/", &[]).await;
            assert_eq!(status, 200);
            assert!(body.contains("stub ok"));
        }
        _ => panic!("tcp"),
    }
    assert!(runtime.stats().switch_latched.load(Ordering::Relaxed) >= 1);

    // MIG-1: SwitchFailed is a TENANT-LEVEL verdict — the pod stays
    // ready (revision processed, old generation serving) while the
    // failure is listed in tenant_failures, never as a pod reason.
    let reg = vrt_host::supervisor::TenantRegistry::new();
    reg.insert("t1".to_string(), runtime.clone());
    let st = vrt_host::status::new_status();
    {
        let mut inner = st.write().unwrap_or_else(|e| e.into_inner());
        inner.revision = Some(1);
        inner.assigned = vec![("t1".to_string(), "w1".to_string())];
    }
    let view = vrt_host::status::readyz_view(&st, &reg);
    assert!(view.ready, "pod-level ready despite tenant failure: {view:?}");
    assert_eq!(view.reason, None, "{view:?}");
    assert_eq!(view.tenant_failures.len(), 1, "{view:?}");
    assert_eq!(view.tenant_failures[0].code, "switch-failed");
    assert_eq!(view.tenant_failures[0].tenant, "t1");
    // MIG-4: the bogus binary fails at spawn — no workerd stderr, so no
    // safe `service worker-<id>:` attribution. `worker_ids` stays empty
    // (absent on the wire = tenant-wide); the control plane applies its
    // changed ∩ tenant rule, which for a single-worker tenant marks w1.
    assert!(
        view.tenant_failures[0].worker_ids.is_empty(),
        "unattributed failure must not name workers: {view:?}"
    );
    assert_eq!(
        view.workers[0].reason.as_deref(),
        Some("switch-failed"),
        "{view:?}"
    );

    // MIG-1: re-publishing the SAME bad digest keeps the latch — the
    // bounded switch budget is per content, not per apply. (The stub
    // mints a fresh digest per spec — reuse the latched one verbatim.)
    let latched = runtime.stats().switch_fail.load(Ordering::Relaxed);
    let mut same_bad = fx.stub_spec("t1", &["w1"], &[]);
    same_bad.binary = std::path::PathBuf::from("/nonexistent/workerd-bogus");
    same_bad.config_digest = bad_digest;
    handle
        .send(SupervisorCmd::Apply(Box::new(same_bad)))
        .await;
    // The latch path returns synchronously — a fixed settle window is
    // enough to prove no switch was attempted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        runtime.state(),
        TenantState::SwitchFailed,
        "same desired digest must stay latched"
    );
    assert_eq!(
        runtime.stats().switch_fail.load(Ordering::Relaxed),
        latched,
        "same digest must not burn another switch attempt"
    );

    // A working Apply re-arms: DIFFERENT digest resets the budget.
    let good = fx.stub_spec("t1", &["w1"], &[]);
    handle.send(SupervisorCmd::Apply(Box::new(good))).await;
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// T13/M4: `try_send` reports failure instead of silently dropping —
/// a dead actor yields `Err`, never a fake "applied".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_send_reports_dead_actor() {
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    handle.send(SupervisorCmd::Stop).await;
    wait_for(
        || handle.is_finished(),
        Duration::from_secs(8),
        "actor stop",
    )
    .await;
    let spec2 = fx.stub_spec("t1", &["w1"], &[]);
    assert!(handle
        .try_send(SupervisorCmd::Apply(Box::new(spec2)))
        .is_err());
    handle.join().await;
}

/// T13/L1: dropping `GroupChild` SIGKILLs the whole process group, not
/// just the leader — grandchildren die too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_child_drop_kills_process_group() {
    // /bin/sh forks `sleep` — the sleep is a grandchild in the group.
    let mut cmd = tokio::process::Command::new("/bin/sh");
    cmd.arg("-c").arg("sleep 60 & sleep 60");
    cmd.process_group(0);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    let child = cmd.spawn().expect("spawn sh");
    let pid = child.id().unwrap();
    let guard = vrt_host::launch::GroupChild::wrap(child, pid);
    drop(guard);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !common::pid_alive(pid),
        "leader still alive after group kill"
    );
    // The grandchild `sleep` is re-parented; killing the group already
    // signalled it — verify the pgid no longer resolves.
    let alive = unsafe { libc::kill(-(pid as i32), 0) };
    assert_ne!(alive, 0, "process group still alive");
}

/// T13/H2: a crash retry launches into a FRESH generation dir and the
/// crashed dir is removed — dirs are per-attempt, never shared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_retry_gets_fresh_work_dir() {
    let root_holder = std::cell::RefCell::new(std::path::PathBuf::new());
    let fx = Fixture::new();
    let root = fx.work_root();
    root_holder.replace(root.clone());
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(EnvAssembler {
            fixture: std::sync::Mutex::new(fx),
            env: std::collections::HashMap::from([(
                "ta".to_string(),
                vec![("VRT_STUB_MODE", "die:120")],
            )]),
        });
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("ta", vec![worker("w1", "a.test", "d1")])],
        deletions: vec![],
};
    host.converge(&desired, &assembler);
    wait_state(
        &registry.get("ta").unwrap(),
        TenantState::Ready,
        Duration::from_secs(8),
    )
    .await;
    let dirs_before: Vec<std::ffi::OsString> = std::fs::read_dir(root.join("ta"))
        .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.file_name()).collect())
        .unwrap_or_default();
    assert_eq!(dirs_before.len(), 1);
    // The tenant dies every ~120ms and restarts — wait until a second
    // generation dir appeared (proof the retry re-assembled a fresh
    // dir instead of reusing the crashed one).
    let root2 = root.clone();
    wait_for(
        || {
            std::fs::read_dir(root2.join("ta"))
                .map(|rd| rd.count() >= 1)
                .unwrap_or(false)
                && std::fs::read_dir(root2.join("ta"))
                    .map(|rd| {
                        rd.filter_map(|e| e.ok())
                            .filter(|e| !dirs_before.contains(&e.file_name()))
                            .count()
                            >= 1
                    })
                    .unwrap_or(false)
        },
        Duration::from_secs(15),
        "fresh generation dir for retry",
    )
    .await;
    host.shutdown_all(Duration::from_secs(15)).await;
}

// ---- T25/L9: zero-worker tenant converges without launching ----

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_tenant_converges_as_explicit_empty_state() {
    // B8/L9: `of init` now publishes a SIGNED EMPTY envelope so the
    // stack can go healthy before the first deploy. Converge must
    // apply it as "expected empty", never as an assemble error, and
    // never spawn a workerd for it.
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let mut host = vrt_host::supervisor::SupervisorHost::new(
        registry.clone(),
        vrt_host::supervisor::SupervisorDeps::default(),
        test_params(),
    );
    let assembler: std::sync::Arc<dyn vrt_host::supervisor::TenantAssembler> =
        std::sync::Arc::new(TestAssembler {
            fixture: std::sync::Mutex::new(Fixture::new()),
        });
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t-empty", vec![])],
        deletions: vec![],
};
    let outcome = host.converge(&desired, &assembler);
    assert_eq!(outcome.applied, vec!["t-empty".to_string()]);
    assert!(outcome.failed.is_empty(), "empty state is not a failure");
    // Give the (non-existent) launcher a moment to misbehave — nothing
    // should spawn for a zero-worker tenant.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rt = registry.get("t-empty").unwrap();
    assert!(rt.assemble_error().is_none(), "no assemble error");
    // A later desired WITH workers then launches normally.
    let desired2 = snapshot_protocol::compute_desired::ComputeDesired {
        tenants: vec![tenant("t-empty", vec![worker("w1", "e.test", "d1")])],
        ..desired
};
    host.converge(&desired2, &assembler);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    host.shutdown_all(Duration::from_secs(15)).await;
}

/// T25/M3: a circuit opened by CONTROL-PROTOCOL failures (the real
/// workerd 1.20261103.1 failure shape — addressless listen events
/// were rejected as malformed) recovers identically: a corrected
/// spec + the cooldown's half-open attempt returns to Ready with no
/// host restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_malformed_circuit_recovers_via_apply() {
    let mut fx = Fixture::new();
    // Stub emits garbage on the control fd → control-malformed on
    // every launch → threshold (3) → CircuitOpen.
    let bad = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_BAD_CONTROL", "1")]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, bad);
    wait_state(&runtime, TenantState::CircuitOpen, Duration::from_secs(20)).await;
    assert!(runtime.stats().circuit_opens.load(Ordering::Relaxed) >= 1);
    // last_error must carry the typed reason — never a bare None
    // (M2: real workerd died with EMPTY stderr; the reason is all
    // the operator gets).
    let err = runtime.last_error().expect("last_error set");
    assert!(err.contains("control"), "reason token present: {err}");

    // Apply the corrected spec — the half-open probe after cooldown
    // converges to Ready without a host restart.
    let good = fx.stub_spec("t1", &["w1"], &[]);
    handle.send(SupervisorCmd::Apply(Box::new(good))).await;
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(20)).await;
    assert_eq!(
        runtime.stats().half_open_attempts.load(Ordering::Relaxed),
        1,
        "exactly one half-open attempt"
    );
    // Actually serving, not just state-labelled.
    let slot = runtime.endpoint_slot("w1").expect("slot routed");
    match slot.endpoint() {
        vrt_host::endpoint::WorkerEndpoint::Tcp(addr) => {
            let (status, _, body) = http_get(*addr, "x.test", "/", &[]).await;
            assert_eq!(status, 200);
            assert!(body.contains("stub ok"));
        }
        _ => panic!("expected tcp"),
    }
    drop(slot);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// T25/M1: a multi-worker spec where only SOME sockets report listen
/// must hit the ready deadline — never mark partially-ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_listen_times_out_not_ready() {
    let mut fx = Fixture::new();
    // Two workers; stub emits only the first socket's event.
    let spec = fx.stub_spec("t1", &["w1", "w2"], &[("VRT_STUB_ONLY_FIRST", "1")]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec);
    // Every launch exhausts the ready deadline while the second
    // socket stays silent; the tenant must NEVER pass through Ready —
    // it keeps retrying with bounded ready_timeouts instead. (The
    // 4s-per-attempt cycle exceeds the 2s failure window, so the
    // circuit does not trip here — that's the timeout path, not a
    // readiness pass.)
    wait_for(
        || runtime.stats().ready_timeouts.load(Ordering::Relaxed) >= 2,
        Duration::from_secs(15),
        "two ready timeouts",
    )
    .await;
    assert_ne!(runtime.state(), TenantState::Ready);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// P3c-1 P0 — serialized DO switching: when either generation owns DO
/// storage, the OLD generation must be fully dead before the new one
/// launches (two workerd processes must never hold the same
/// localDisk dirs open). Proven by timing: an old gen that ignores
/// SIGTERM survives to the kill-grace horizon — a PARALLEL switch
/// would serve the new digest immediately, a serialized switch can
/// only serve after the kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_serialized_switch_waits_for_old_generation_exit() {
    init_logs();
    let mut fx = Fixture::new();
    let do_dir = fx.dir.path().join("do").join("t1");
    std::fs::create_dir_all(&do_dir).unwrap();
    let storage = || {
        Some(vrt_host::spec::DoStorageSpec {
            tenant_dir: do_dir.clone(),
            worker_classes: std::collections::BTreeMap::from([(
                "w1".to_string(),
                vec!["a".repeat(64)],
            )]),
            transfer_sources: std::collections::BTreeMap::new(),
        })
    };
    let mut spec_a = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_IGNORE_SIGTERM", "1")]);
    spec_a.do_storage = storage();
    let digest_a = spec_a.config_digest.clone();
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec_a);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    assert_eq!(runtime.serving_digest().as_deref(), Some(digest_a.as_str()));

    // Apply spec B — DO storage on BOTH sides ⇒ serialized gate.
    let mut spec_b = fx.stub_spec("t1", &["w1"], &[]);
    spec_b.do_storage = storage();
    let digest_b = spec_b.config_digest.clone();
    let t0 = std::time::Instant::now();
    handle.send(SupervisorCmd::Apply(Box::new(spec_b))).await;
    wait_for(
        || runtime.serving_digest().as_deref() == Some(digest_b.as_str()),
        Duration::from_secs(10),
        "new generation serving",
    )
    .await;
    let elapsed = t0.elapsed();
    // kill_grace=200ms floor: a serialized switch cannot beat the
    // SIGKILL horizon because the old gen ignores SIGTERM. A parallel
    // switch would have served B in well under 100ms.
    assert!(
        elapsed >= Duration::from_millis(150),
        "new gen served after {elapsed:?} — old generation was not awaited"
    );
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// Same switch WITHOUT DO storage keeps the fast parallel path —
/// the gate must not slow ordinary tenants.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_do_switch_stays_parallel() {
    let mut fx = Fixture::new();
    let spec_a = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_IGNORE_SIGTERM", "1")]);
    let runtime = TenantRuntime::new("t1");
    let handle = spawn(&runtime, spec_a);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;
    let spec_b = fx.stub_spec("t1", &["w1"], &[]);
    let digest_b = spec_b.config_digest.clone();
    let t0 = std::time::Instant::now();
    handle.send(SupervisorCmd::Apply(Box::new(spec_b))).await;
    wait_for(
        || runtime.serving_digest().as_deref() == Some(digest_b.as_str()),
        Duration::from_secs(10),
        "new generation serving",
    )
    .await;
    // Parallel switch: new gen serves while the SIGTERM-ignoring old
    // gen is still draining — well inside the kill-grace window.
    assert!(
        t0.elapsed() < Duration::from_millis(150),
        "non-DO switch waited {:?} — serialized gate leaked",
        t0.elapsed()
    );
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

// ---- MIG-8: typed ever_served readiness gating ----

/// MIG-8: pod readiness distinguishes INITIAL convergence (gates the
/// pod — a cold tenant must never take traffic) from a generation
/// switch on a tenant that already served (a tenant-level gap only —
/// the pod stays on the EndpointSlice and the switching tenant's own
/// requests get the typed `worker_unavailable` 503).
#[test]
fn mig8_readyz_never_served_converging_gates_pod() {
    let reg = vrt_host::supervisor::TenantRegistry::new();
    // t1: assigned, runtime exists, still converging on its FIRST
    // generation — nothing ever installed.
    let t1 = TenantRuntime::new("t1");
    t1.test_set_state(TenantState::Starting);
    reg.insert("t1".to_string(), t1);
    let st = vrt_host::status::new_status();
    {
        let mut inner = st.write().unwrap();
        inner.revision = Some(1);
        inner.assigned = vec![("t1".to_string(), "w1".to_string())];
    }
    let view = vrt_host::status::readyz_view(&st, &reg);
    assert!(!view.ready, "never-served converging tenant must gate: {view:?}");
    assert_eq!(view.reason.as_deref(), Some("workers-not-ready"), "{view:?}");
    assert!(
        view.tenant_failures.is_empty(),
        "convergence is not a failure: {view:?}"
    );
    assert_eq!(
        view.workers[0].reason.as_deref(),
        Some("workers-not-ready"),
        "{view:?}"
    );
}

/// MIG-8: an assigned tenant with NO runtime yet (converge has not
/// adopted it) is the same never-served gate — the pod must not take
/// traffic for a tenant it cannot route.
#[test]
fn mig8_readyz_unadopted_tenant_gates_pod() {
    let reg = vrt_host::supervisor::TenantRegistry::new();
    let st = vrt_host::status::new_status();
    {
        let mut inner = st.write().unwrap();
        inner.revision = Some(1);
        inner.assigned = vec![("ghost".to_string(), "w1".to_string())];
    }
    let view = vrt_host::status::readyz_view(&st, &reg);
    assert!(!view.ready, "{view:?}");
}

/// MIG-8: a tenant that INSTALLED ≥1 generation (`ever_served`) and
/// is now mid-switch — state Ready, assigned worker not routed by
/// the incoming generation yet — is a tenant-level gap, never pod
/// pending. Its worker line still reports `workers-not-ready`
/// (per-request traffic gets the typed 503), and a SECOND
/// never-served tenant keeps gating the pod in the same view.
#[test]
fn mig8_readyz_served_tenant_switching_does_not_gate() {
    let reg = vrt_host::supervisor::TenantRegistry::new();
    // t1 served `w-old` (a prior generation), now converging onto a
    // generation that does not yet route the assigned worker.
    let t1 = TenantRuntime::new("t1");
    t1.test_install(std::collections::BTreeMap::from([(
        "w-old".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
    )]));
    assert!(t1.ever_served(), "test_install marks the flag");
    reg.insert("t1".to_string(), t1);
    // t2 never served, still Starting.
    let t2 = TenantRuntime::new("t2");
    t2.test_set_state(TenantState::Starting);
    reg.insert("t2".to_string(), t2);
    let st = vrt_host::status::new_status();
    {
        let mut inner = st.write().unwrap();
        inner.revision = Some(1);
        inner.assigned = vec![
            ("t1".to_string(), "w-new".to_string()),
            ("t2".to_string(), "w2".to_string()),
        ];
    }
    let view = vrt_host::status::readyz_view(&st, &reg);
    // t2 gates the pod; t1 does not.
    assert!(!view.ready, "never-served t2 must still gate: {view:?}");
    let w1 = view
        .workers
        .iter()
        .find(|w| w.tenant == "t1")
        .unwrap();
    assert!(!w1.ready);
    assert_eq!(w1.reason.as_deref(), Some("workers-not-ready"));

    // Drop the cold tenant — the switching tenant alone leaves the
    // pod READY (the 2026-11-17 incident: one DO tenant's serialized
    // switch 503'd the whole pod).
    {
        let mut inner = st.write().unwrap();
        inner.assigned = vec![("t1".to_string(), "w-new".to_string())];
    }
    let view = vrt_host::status::readyz_view(&st, &reg);
    assert!(
        view.ready,
        "served tenant mid-switch must not gate the pod: {view:?}"
    );
    assert_eq!(view.reason, None, "{view:?}");
}

/// MIG-8: pod-level blockers are unchanged — snapshot loss still
/// holds the pod off the EndpointSlice even when every tenant has
/// served before.
#[test]
fn mig8_readyz_snapshot_loss_still_gates() {
    let reg = vrt_host::supervisor::TenantRegistry::new();
    let t1 = TenantRuntime::new("t1");
    t1.test_install(std::collections::BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
    )]));
    reg.insert("t1".to_string(), t1);
    let st = vrt_host::status::new_status();
    {
        let mut inner = st.write().unwrap();
        inner.revision = None; // snapshot unavailable
        inner.assigned = vec![("t1".to_string(), "w1".to_string())];
    }
    let view = vrt_host::status::readyz_view(&st, &reg);
    assert!(!view.ready);
    assert_eq!(view.reason.as_deref(), Some("snapshot-unavailable"), "{view:?}");
}

