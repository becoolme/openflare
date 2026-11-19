//! W1-r4: supervisor-driven DO storage lifecycle over the REAL
//! placement backend — `ManifestDoPlacement` (the same conditional-
//! write object-store impl the standalone fs backend runs, here on
//! MemoryStore which shares its CAS semantics), plus `DoSnapCtx` so
//! the launch path writes `<uniqueKey>.lease-epoch` markers exactly
//! like production.
//!
//! Two e2e-observed regressions are reproduced WITHOUT docker:
//!   1. `do storage stale class entry removed` — the monitor sweep
//!      misclassified the `<ukey>.lease-epoch` marker (it ends with
//!      `.epoch`, so `strip_suffix(".epoch")` reduced it to
//!      `<ukey>.lease`, never a declared key) and deleted it on every
//!      Ready tick. DO data (`<ukey>/`) was never at risk — the
//!      marker is — and its loss also downgrades the stale-dir
//!      restore guard to "unmarked → keep local".
//!   2. `fenced: a newer epoch (N) holds the class` for 20–35s after
//!      every restart — generations leaving `draining` normally (the
//!      ONLY way a graceful stop ends them) never released their
//!      placement leases; the next holder's acquire fenced on the
//!      still-live TTL lease until expiry.
//!
//! Test 1 asserts the keep semantics; test 2 asserts a dead-marker
//! release after graceful Stop and an immediate re-acquire by the
//! next holder. Both fail against the pre-fix code.

mod common;

use common::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use vrt_host::do_placement_store::ManifestDoPlacement;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{DoSnapCtx, SupervisorCmd, SupervisorDeps, TenantRuntime};
use vrt_objstore::{MemoryStore, ObjectStore};

const LEASE_MS: u64 = 4_000;
const MARGIN_MS: u64 = 500;

fn placement_pair(holder: &str) -> (Arc<dyn ObjectStore>, Arc<ManifestDoPlacement>) {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        holder,
        LEASE_MS,
        MARGIN_MS,
    ));
    (store, plc)
}

fn deps_with(
    store: Arc<dyn ObjectStore>,
    plc: Arc<ManifestDoPlacement>,
    tmp_root: &Path,
) -> SupervisorDeps {
    let mut deps = SupervisorDeps::default();
    deps.do_snap = Some(Arc::new(DoSnapCtx {
        store,
        placement: plc.clone(),
        tmp_root: tmp_root.to_path_buf(),
        interval_ms: 3_600_000,
        jitter_ms: 0,
        departing_max_ms: 60_000,
        xfer_quarantine_ms: 7 * 24 * 3600 * 1000,
        departing_retry_base_ms: 50,
        departing_retry_cap_ms: 500,
        wal: None,
    }));
    deps.placement = plc;
    deps
}

fn do_spec(tenant_dir: &Path, classes: &[(&str, &str)]) -> vrt_host::spec::DoStorageSpec {
    let mut worker_classes: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (w, k) in classes {
        worker_classes.insert(w.to_string(), vec![k.to_string()]);
    }
    vrt_host::spec::DoStorageSpec {
        tenant_dir: tenant_dir.to_path_buf(),
        worker_classes,
        transfer_sources: BTreeMap::new(),
    }
}

fn key64(b: u8) -> String {
    std::iter::repeat(b).take(64).map(|c| c as char).collect()
}

/// Seed `<tenant>/<worker>/<ukey>/metadata.sqlite` the way workerd's
/// localDisk would leave it after first open.
fn seed_class_dir(tenant_dir: &Path, worker: &str, ukey: &str) {
    let d = tenant_dir.join(worker).join(ukey);
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("metadata.sqlite"), b"sqlite").unwrap();
}

/// Ready → monitor sweep: declared class dirs AND their `.lease-epoch`
/// markers survive; only undeclared entries (junk, restore staging
/// leftovers, another class's key) are RETAINED unless a signed
/// tombstone authorizes removal (RV-R53). `wf`'s class models
/// the workflow Engine namespace — an ordinary uniqueKey on the
/// declaring worker's class set (assemble.rs `worker_classes`), so its
/// storage is covered by exactly the same keep-set, no special case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_declared_classes_and_lease_markers() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root = fx.dir.path().join("do");
    let tenant_dir = do_root.join("t1");
    let uk1 = key64(b'a');
    let wfuk = key64(b'c'); // engine-class key on the workflow worker
    seed_class_dir(&tenant_dir, "w1", &uk1);
    seed_class_dir(&tenant_dir, "wf", &wfuk);
    // Undeclared entries (RV-R53: retained absent a signed tombstone).
    std::fs::create_dir_all(tenant_dir.join("w1").join("stale-junk")).unwrap();
    std::fs::create_dir_all(tenant_dir.join("w1").join(format!("{uk1}.restore-tmp"))).unwrap();
    std::fs::create_dir_all(tenant_dir.join("w1").join(format!("{uk1}.replaced-bak"))).unwrap();

    let (store, plc) = placement_pair("cell:test:holder-a");
    let deps = deps_with(store, plc, &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w1", "wf"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w1", &uk1), ("wf", &wfuk)]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    // Post-acquire the supervisor marked `.lease-epoch` beside every
    // class dir (DoSnapCtx configured, same as the standalone fs path).
    let w1_marker = tenant_dir.join("w1").join(format!("{uk1}.lease-epoch"));
    let wf_marker = tenant_dir.join("wf").join(format!("{wfuk}.lease-epoch"));
    assert!(w1_marker.exists(), "lease marker written post-acquire");
    assert!(wf_marker.exists(), "engine-class lease marker written");

    // Give the monitor several ticks — the sweep runs every tick once
    // Ready (and once right after install).
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Class dirs (DO data / engine sqlite) are never stale.
    assert!(
        tenant_dir
            .join("w1")
            .join(&uk1)
            .join("metadata.sqlite")
            .exists(),
        "declared class dir must survive the sweep"
    );
    assert!(
        tenant_dir
            .join("wf")
            .join(&wfuk)
            .join("metadata.sqlite")
            .exists(),
        "engine class dir must survive the sweep"
    );
    // Lease markers belong to the class — they must NOT be swept as
    // stale (pre-fix they were deleted every pass because stripping
    // only `.epoch` leaves `<ukey>.lease` which is never declared).
    assert!(
        w1_marker.exists(),
        "lease marker {:?} swept — restore staleness guard degraded",
        w1_marker
    );
    assert!(
        wf_marker.exists(),
        "engine lease marker {:?} swept",
        wf_marker
    );
    // RV-R53: undeclared entries are only destroyed under a signed
    // tombstone (spec-absence is NOT deletion). No tombstone here, so
    // the sweep must RETAIN them (fail closed) — they are removed by
    // the tombstone path, covered in do_transfer.rs / supervisor tests.
    for retained in [
        tenant_dir.join("w1").join("stale-junk"),
        tenant_dir.join("w1").join(format!("{uk1}.restore-tmp")),
        tenant_dir.join("w1").join(format!("{uk1}.replaced-bak")),
    ] {
        assert!(
            retained.exists(),
            "RV-R53: untombstoned undeclared entry must be retained: {}",
            retained.display()
        );
    }

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// Graceful Stop must RELEASE the generation's class leases (fence →
/// dead-marker) so the next holder acquires immediately instead of
/// fencing on the dead holder's unexpired TTL lease — the 20–35s
/// restart stall the standalone e2e hit on every `docker restart`.
/// Crash-only paths keep TTL recovery; this changes nothing about
/// fencing strength, only skips the wait when the writer is dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_stop_releases_leases_for_next_holder() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root = fx.dir.path().join("do");
    let tenant_dir = do_root.join("t1");
    let uk1 = key64(b'a');
    seed_class_dir(&tenant_dir, "w1", &uk1);

    let (store, plc_a) = placement_pair("cell:test:holder-a");
    let deps = deps_with(
        store.clone(),
        plc_a.clone(),
        &fx.dir.path().join("snap-tmp"),
    );
    let mut spec = fx.stub_spec("t1", &["w1"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w1", &uk1)]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    let rec = plc_a
        .record_for_class(&uk1)
        .unwrap()
        .expect("lease record after acquire");
    assert!(!rec.holder.is_empty() && rec.holder != "");
    assert_eq!(rec.epoch, 1);

    // Graceful stop — drained generations exit normally; this is the
    // path `docker restart` takes.
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&runtime, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;

    // The lease MUST be a dead-marker now: writer confirmed dead → no
    // reason to hold the TTL. Pre-fix the record still named the dead
    // holder with an unexpired lease → next holder fenced ~lease_ms.
    let rec = plc_a
        .record_for_class(&uk1)
        .unwrap()
        .expect("record persists as dead-marker");
    assert!(
        rec.holder.is_empty() && rec.expires_at_ms == 0,
        "graceful stop left a live lease: {rec:?} — next holder waits the full TTL"
    );

    // Next holder (new process → new holder id): acquire must be
    // instant — a dead-marker mints epoch+1, no TTL wait. With a
    // still-live lease this stalls ~LEASE_MS at 5s retry cadence.
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let mut deps = SupervisorDeps::default();
    deps.placement = plc_b;
    let t0 = std::time::Instant::now();
    let spec2 = {
        let mut s = fx.stub_spec("t1", &["w1"], &[]);
        s.do_storage = Some(do_spec(&tenant_dir, &[("w1", &uk1)]));
        s
    };
    let runtime2 = TenantRuntime::new("t1");
    let handle2 =
        vrt_host::supervisor::spawn_supervisor(runtime2.clone(), spec2, deps, test_params());
    wait_state(&runtime2, TenantState::Ready, Duration::from_secs(8)).await;
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(LEASE_MS),
        "restart acquire took {elapsed:?} — fenced on dead holder's lease"
    );
    handle2.send(SupervisorCmd::Stop).await;
    wait_state(&runtime2, TenantState::Stopped, Duration::from_secs(8)).await;
    handle2.join().await;
}
