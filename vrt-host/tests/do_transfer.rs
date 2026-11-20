//! DO1 `transferred_classes` — cell-side handoff.
//!
//! Coverage over the REAL ManifestDoPlacement backend (MemoryStore,
//! same CAS semantics as fs/s3) using the stub-spec harness:
//!
//!   1. same-cell: dst spec carries `transfer_sources[(w2,k)]=w1`,
//!      `<t>/w1/k` exists → the launch moves it to `<t>/w2/k` and
//!      the tenant reaches Ready — data readable at the dst path.
//!   2. fail-closed: no local source dir AND no committed snapshot →
//!      the launch refuses (`do-transfer-...` last_error) and NO
//!      empty dst dir appears — never an empty-store fork.
//!   3. cross-cell: the source cell's drain commits a final fenced
//!      snapshot (departing-class rule), a SECOND supervisor (another
//!      holder) acquires epoch+1 and restores it into the dst dir.
//!   4. pure `move_class_dir` unit cases: move, empty-stub dst
//!      cleanup, dst-wins quarantine, missing source.
//!
//! The fenced-old-owner guarantee itself is the placement layer's
//! epoch CAS (covered in do_w1r4.rs / do_p3c2*.rs); these tests assert
//! the handoff only ever runs UNDER a freshly-acquired dst lease.

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

fn do_spec(
    tenant_dir: &Path,
    classes: &[(&str, &str)],
    transfers: &[(&str, &str, &str)],
) -> vrt_host::spec::DoStorageSpec {
    let mut worker_classes: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (w, k) in classes {
        worker_classes.insert(w.to_string(), vec![k.to_string()]);
    }
    let mut transfer_sources: BTreeMap<(String, String), String> = BTreeMap::new();
    for (w, k, src) in transfers {
        transfer_sources.insert((w.to_string(), k.to_string()), src.to_string());
    }
    vrt_host::spec::DoStorageSpec {
        tenant_dir: tenant_dir.to_path_buf(),
        worker_classes,
        transfer_sources,
    }
}

fn key64(b: u8) -> String {
    std::iter::repeat(b).take(64).map(|c| c as char).collect()
}

fn seed_class_dir(tenant_dir: &Path, worker: &str, ukey: &str) {
    let d = tenant_dir.join(worker).join(ukey);
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("payload.bin"), b"sqlite-payload").unwrap();
}

// ---------- pure move_class_dir unit cases ----------

#[test]
fn move_class_dir_moves_dir_and_epoch_siblings() {
    let tmp = tempfile::tempdir().unwrap();
    let tenant = tmp.path().join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant, "w1", &key);
    std::fs::write(
        tenant.join("w1").join(format!("{key}.epoch")),
        b"7".as_slice(),
    )
    .unwrap();
    std::fs::write(
        tenant.join("w1").join(format!("{key}.lease-epoch")),
        b"{\"epoch\":7}".as_slice(),
    )
    .unwrap();
    let dst = tenant.join("w2").join(&key);
    let r = vrt_host::do_transfer::move_class_dir(&tenant, "w1", &key, &dst).unwrap();
    assert_eq!(r, vrt_host::do_transfer::HandoffMove::Moved);
    assert!(dst.join("payload.bin").exists());
    assert!(tenant.join("w2").join(format!("{key}.epoch")).exists());
    assert!(tenant
        .join("w2")
        .join(format!("{key}.lease-epoch"))
        .exists());
    assert!(!tenant.join("w1").join(&key).exists());
}

#[test]
fn move_class_dir_empty_dst_stub_is_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let tenant = tmp.path().join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant, "w1", &key);
    let dst = tenant.join("w2").join(&key);
    std::fs::create_dir_all(&dst).unwrap(); // empty stub — carries no data
    let r = vrt_host::do_transfer::move_class_dir(&tenant, "w1", &key, &dst).unwrap();
    assert_eq!(r, vrt_host::do_transfer::HandoffMove::Moved);
    assert!(dst.join("payload.bin").exists());
}

#[test]
fn move_class_dir_dst_wins_quarantines_source() {
    let tmp = tempfile::tempdir().unwrap();
    let tenant = tmp.path().join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant, "w1", &key);
    let dst = tenant.join("w2").join(&key);
    std::fs::create_dir_all(&dst).unwrap();
    std::fs::write(dst.join("payload.bin"), b"newer").unwrap();
    let r = vrt_host::do_transfer::move_class_dir(&tenant, "w1", &key, &dst).unwrap();
    assert_eq!(r, vrt_host::do_transfer::HandoffMove::Quarantined);
    assert_eq!(std::fs::read(dst.join("payload.bin")).unwrap(), b"newer");
    assert!(!tenant.join("w1").join(&key).exists());
    assert!(tenant
        .join("w1")
        .join(format!("{key}.xfer-quarantine"))
        .exists());
}

#[test]
fn move_class_dir_no_local_source() {
    let tmp = tempfile::tempdir().unwrap();
    let tenant = tmp.path().join("t1");
    std::fs::create_dir_all(&tenant).unwrap();
    let key = key64(b'a');
    let dst = tenant.join("w2").join(&key);
    let r = vrt_host::do_transfer::move_class_dir(&tenant, "w1", &key, &dst).unwrap();
    assert_eq!(r, vrt_host::do_transfer::HandoffMove::NoLocalSource);
    assert!(!dst.exists(), "must not fabricate an empty dst dir");
}

// ---------- supervisor-level ----------

/// Same-cell: the dst spec carries the lineage marker and the source
/// dir exists — the launch moves it under the dst worker's segment
/// and serves. The dst dir is the live data, not an empty fork.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_cell_handoff_moves_dir_and_serves() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root = fx.dir.path().join("do");
    let tenant_dir = do_root.join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir, "w1", &key);

    let (store, plc) = placement_pair("cell:test:holder-a");
    let deps = deps_with(store, plc, &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w1", "w2"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w1", &key)], &[]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    let mut spec2 = fx.stub_spec("t1", &["w1", "w2"], &[]);
    spec2.do_storage = Some(do_spec(&tenant_dir, &[("w2", &key)], &[("w2", &key, "w1")]));
    handle.send(SupervisorCmd::Apply(Box::new(spec2))).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    assert!(
        tenant_dir
            .join("w2")
            .join(&key)
            .join("payload.bin")
            .exists(),
        "transferred dir must land under the dst worker segment"
    );
    assert!(!tenant_dir.join("w1").join(&key).exists());
    handle.send(SupervisorCmd::Stop).await;
}

/// Fail-closed: transfer declared but neither a local source dir nor
/// a committed snapshot exists — the launch must refuse and MUST NOT
/// fabricate an empty dst dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_without_source_or_snapshot_fails_closed() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root = fx.dir.path().join("do");
    let tenant_dir = do_root.join("t1");
    let key = key64(b'a');
    std::fs::create_dir_all(&tenant_dir).unwrap();

    let (store, plc) = placement_pair("cell:test:holder-a");
    let deps = deps_with(store, plc, &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w2"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w2", &key)], &[("w2", &key, "w1")]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_ne!(
        runtime.state(),
        TenantState::Ready,
        "data-less transfer must not serve (empty fork)"
    );
    assert!(!tenant_dir.join("w2").join(&key).exists());
    handle.send(SupervisorCmd::Stop).await;
}

/// Cross-cell: cell A serves w1's class; the post-transfer desired
/// drops (w1,key) so A's drain commits a FINAL fenced snapshot before
/// releasing the lease. Cell B (different holder) then acquires
/// epoch+1 and restores the data into w2's segment — the full
/// departing-snapshot → restore chain, not just the local rename.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_cell_handoff_restores_committed_snapshot() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root_a = fx.dir.path().join("do-a");
    let tenant_dir_a = do_root_a.join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a, &fx.dir.path().join("snap-a"));
    let deps_b = deps_with(store.clone(), plc_b, &fx.dir.path().join("snap-b"));

    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;

    // Post-transfer desired on A: (w1,key) departs (dst lives on B).
    let mut spec_a2 = fx.stub_spec("t1", &["w1"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;
    tokio::time::sleep(Duration::from_millis(900)).await;

    let do_root_b = fx.dir.path().join("do-b");
    let tenant_dir_b = do_root_b.join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(
        &tenant_dir_b,
        &[("w2", &key)],
        &[("w2", &key, "w1")],
    ));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(15)).await;

    assert!(
        tenant_dir_b
            .join("w2")
            .join(&key)
            .join("payload.bin")
            .exists(),
        "cell B must restore the committed snapshot into the dst dir"
    );
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// Retry semantics: the transfer desired lands before the baseline
/// snapshot commit (source's drain still in flight elsewhere) — the
/// launch fails closed; once the baseline exists the supervisor's
/// retry converges to Ready with the data, never an empty fork.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_retry_after_snapshot_commit() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root = fx.dir.path().join("do");
    let tenant_dir = do_root.join("t1");
    let key = key64(b'a');
    std::fs::create_dir_all(&tenant_dir).unwrap();

    let (store, plc) = placement_pair("cell:test:holder-a");
    let deps = deps_with(store.clone(), plc.clone(), &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w2"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w2", &key)], &[("w2", &key, "w1")]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_ne!(runtime.state(), TenantState::Ready);

    // Late-commit the baseline the way the source cell's drain does:
    // snapshot under the CURRENT record's epoch, fenced-committed.
    let seed_dir = tenant_dir.join("seed-snap");
    std::fs::create_dir_all(&seed_dir).unwrap();
    std::fs::write(seed_dir.join("payload.bin"), b"remote-data").unwrap();
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: seed_dir.clone(),
    };
    let rec = plc.record(&scope).unwrap().expect("placement record");
    plc.fence_for_migration(
        &scope,
        vrt_host::do_placement::PlacementEpoch(rec.epoch),
        "cell:test:holder-b",
        30_000,
    )
    .unwrap();
    let stage = fx.dir.path().join("seed-stage");
    let meta = vrt_sql::do_snap::snapshot_dir(&seed_dir, &stage, rec.epoch, 42).unwrap();
    for f in &meta.files {
        let data = std::fs::read(stage.join(&f.path)).unwrap();
        vrt_objstore::block_on_store(store.put(
            &vrt_host::do_placement_store::snap_file_key(&key, rec.epoch, 42, &f.path),
            &data,
        ))
        .unwrap();
    }
    let mkey = vrt_host::do_placement_store::snap_manifest_key(&key, rec.epoch, 42);
    vrt_objstore::block_on_store(store.put(&mkey, &serde_json::to_vec(&meta).unwrap())).unwrap();
    plc.commit_snapshot_fenced(
        &scope,
        rec.epoch,
        vrt_host::do_placement_store::SnapRef {
            manifest_key: mkey,
            epoch: rec.epoch,
            seq: 42,
            files: meta.files.len() as u32,
            bytes: meta.files.iter().map(|f| f.size).sum(),
            at_ms: meta.created_ms,
            // The drain's post-fence baseline commit IS `kind=Final` —
            // a periodic ref here would trip the r2 fail-closed gate.
            kind: vrt_host::do_placement_store::SnapKind::Final,
        },
    )
    .unwrap();
    plc.release_reservation(&scope, rec.epoch).unwrap();

    wait_state(&runtime, TenantState::Ready, Duration::from_secs(15)).await;
    assert!(
        tenant_dir
            .join("w2")
            .join(&key)
            .join("payload.bin")
            .exists(),
        "retry after baseline commit must restore, not fork empty"
    );
    handle.send(SupervisorCmd::Stop).await;
}

// ---------- DO1-r2: ordering, final-only gating, retention, terminal ----------

use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

/// Object-store wrapper that fails snapshot-object puts (`/snap-`
/// keys — manifests + file payloads) while `block` is set; placement
/// CAS stays live so the source's held lease keeps renewing.
struct SnapBlockStore {
    inner: MemoryStore,
    /// Snapshot-object puts only (`/snap-` keys).
    block: Arc<AtomicBool>,
    /// Every mutation (placement CAS included) — simulates a full
    /// store outage while reads keep working.
    block_all: Arc<AtomicBool>,
    /// DO1-r4: snapshot-object GETs only — the placement record read
    /// (`do/<key>/placement`, no `/snap-` segment) keeps working so
    /// acquire/claim succeed while `restore_snapshot`'s manifest and
    /// file fetches fail — a transient restore outage.
    block_get: Arc<AtomicBool>,
    /// RV-R28 #3: `/wal/` listings only — the WAL replay path fails
    /// while snapshot restore + placement stay healthy.
    block_wal: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl ObjectStore for SnapBlockStore {
    async fn get(
        &self,
        k: &str,
    ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
        if self.block_get.load(AtomicOrdering::SeqCst) && k.contains("/snap-") {
            return Err(vrt_objstore::StoreError::unavailable(
                false,
                "restore read blocked by test",
            ));
        }
        self.inner.get(k).await
    }
    async fn head(
        &self,
        k: &str,
    ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
        self.inner.head(k).await
    }
    async fn put_if_none_match(
        &self,
        k: &str,
        b: &[u8],
    ) -> Result<String, vrt_objstore::StoreError> {
        if self.block.load(AtomicOrdering::SeqCst) && k.contains("/snap-") {
            return Err(vrt_objstore::StoreError::unavailable(false, "blocked"));
        }
        if self.block_all.load(AtomicOrdering::SeqCst) {
            return Err(vrt_objstore::StoreError::unavailable(false, "store down"));
        }
        self.inner.put_if_none_match(k, b).await
    }
    async fn put_if_match(
        &self,
        k: &str,
        e: &str,
        b: &[u8],
    ) -> Result<String, vrt_objstore::StoreError> {
        if self.block.load(AtomicOrdering::SeqCst) && k.contains("/snap-") {
            return Err(vrt_objstore::StoreError::unavailable(false, "blocked"));
        }
        if self.block_all.load(AtomicOrdering::SeqCst) {
            return Err(vrt_objstore::StoreError::unavailable(false, "store down"));
        }
        self.inner.put_if_match(k, e, b).await
    }
    async fn put(&self, k: &str, b: &[u8]) -> Result<String, vrt_objstore::StoreError> {
        if self.block.load(AtomicOrdering::SeqCst) && k.contains("/snap-") {
            return Err(vrt_objstore::StoreError::unavailable(
                false,
                "snapshot backend blocked by test",
            ));
        }
        if self.block_all.load(AtomicOrdering::SeqCst) {
            return Err(vrt_objstore::StoreError::unavailable(false, "store down"));
        }
        self.inner.put(k, b).await
    }
    async fn list(
        &self,
        p: &str,
    ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
        if self.block_wal.load(AtomicOrdering::SeqCst) && p.contains("/wal/") {
            return Err(vrt_objstore::StoreError::unavailable(
                true,
                "wal listing blocked by test",
            ));
        }
        self.inner.list(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
        self.inner.delete(k).await
    }
}

/// P0-1: a write landing AFTER slot close but BEFORE process exit
/// must appear in the Final snapshot the target restores. The stub
/// writer ignores SIGTERM so the drain window is deterministic
/// (kill_grace = 1.2s): the test substitutes the writer and drops
/// `late.bin` into the class dir mid-drain; cell B must serve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_snapshot_includes_post_close_pre_exit_writes() {
    init_logs();
    let mut fx = Fixture::new();
    let do_root_a = fx.dir.path().join("do-a");
    let tenant_dir_a = do_root_a.join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));
    let deps_b = deps_with(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));
    let mut params = test_params();
    params.kill_grace = Duration::from_millis(1_200);

    let mut spec_a = fx.stub_spec("t1", &["w1"], &[("VRT_STUB_IGNORE_SIGTERM", "1")]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, params.clone());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;

    // Depart the class — the SIGTERM-ignoring writer stays alive for
    // the whole kill_grace window after slot close.
    let mut spec_a2 = fx.stub_spec("t1", &["w1"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;
    // Mid-drain: the "writer's" last in-flight write lands in the dir
    // — AFTER slots closed, BEFORE the process actually died.
    tokio::time::sleep(Duration::from_millis(500)).await;
    std::fs::write(
        tenant_dir_a.join("w1").join(&key).join("late.bin"),
        b"written-after-slot-close",
    )
    .unwrap();
    // Wait for writer-dead → Final-commit → release before B launches.
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key),
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    loop {
        let rec = plc_a.record(&scope).unwrap();
        let done = rec
            .as_ref()
            .is_some_and(|r| r.holder.is_empty() && r.snapshot.is_some());
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Final snapshot never committed/released: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let do_root_b = fx.dir.path().join("do-b");
    let tenant_dir_b = do_root_b.join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(
        &tenant_dir_b,
        &[("w2", &key)],
        &[("w2", &key, "w1")],
    ));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(15)).await;

    assert!(
        tenant_dir_b.join("w2").join(&key).join("late.bin").exists(),
        "post-close/pre-exit writes MUST be in the Final baseline the target restores"
    );
    assert!(tenant_dir_b
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// P0-2a end-to-end: cell A's writer exits with the Final commit
/// blocked — its lease stays HELD at the drained epoch (live holder,
/// never a dead-marker), so a foreign cell can neither acquire nor
/// serve. When the store recovers, the bounded retry commits Final,
/// the held lease is fenced, and the target restores the newest
/// data. This also covers a cross-tenant transfer: at drain time a
/// departing class is indistinguishable from a delete, so the hold
/// applies to every departing lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_final_holds_lease_then_retry_unblocks_target() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    let block = Arc::new(AtomicBool::new(false));
    let block_all = Arc::new(AtomicBool::new(false));
    let block_get = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(SnapBlockStore {
        inner: MemoryStore::new(),
        block: block.clone(),
        block_all: block_all.clone(),
        block_get: block_get.clone(),
        block_wal: block.clone(),
    });
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));
    let deps_b = deps_with(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));

    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;

    // Outage: snapshot puts AND placement CAS both fail — the drain's
    // Final commit cannot land AND no foreign acquirer can race the
    // observation window.
    block.store(true, AtomicOrdering::SeqCst);
    block_all.store(true, AtomicOrdering::SeqCst);
    let mut spec_a2 = fx.stub_spec("t1", &["w1"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;
    tokio::time::sleep(Duration::from_millis(900)).await;

    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key),
    };
    let rec = plc_a.record(&scope).unwrap().expect("placement record");
    assert_eq!(rec.epoch, 1, "held lease keeps the drained epoch");
    assert!(
        !rec.holder.is_empty(),
        "failed Final must HOLD the lease — never a dead-marker"
    );
    assert!(
        tenant_dir_a.join("w1").join(&key).exists(),
        "source dir preserved"
    );
    assert!(tenant_dir_a
        .join("w1")
        .join(format!("{key}.final-snap-pending"))
        .exists());

    // Target cell B: held live lease + no Final baseline → acquire
    // refuses (Fenced), the class stays down — fail closed.
    let tenant_dir_b = fx.dir.path().join("do-b").join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(
        &tenant_dir_b,
        &[("w2", &key)],
        &[("w2", &key, "w1")],
    ));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_ne!(runtime_b.state(), TenantState::Ready);

    // Store recovers → A's retry commits Final → held lease fenced →
    // B acquires epoch+1 → gate sees kind=Final → restores the data.
    block.store(false, AtomicOrdering::SeqCst);
    block_all.store(false, AtomicOrdering::SeqCst);
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(20)).await;
    assert!(tenant_dir_b
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    let rec = plc_a.record(&scope).unwrap().expect("placement record");
    assert_eq!(
        rec.snapshot.as_ref().map(|s| s.kind),
        Some(vrt_host::do_placement_store::SnapKind::Final),
        "the retried commit must land as kind=Final"
    );
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// RV-R24: removing the WORKER from the spec must not let the DO-dir
/// sweep erase a class whose `Final` is still pending — that dir is
/// the ONLY remaining copy (the transfer target would get a stale or
/// missing baseline and the held lease would release into a
/// fence-in). While pending, the whole worker dir stays; once the
/// retried Final commits and the lease releases, the sweep reaps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_worker_dir_survives_pending_final_then_swept() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key = key64(b'r');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    let block = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(SnapBlockStore {
        inner: MemoryStore::new(),
        block: block.clone(),
        block_all: Arc::new(AtomicBool::new(false)),
        block_get: Arc::new(AtomicBool::new(false)),
        block_wal: block.clone(),
    });
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));

    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;

    // Snapshot puts blocked (placement CAS stays live — the held
    // lease keeps renewing), then w1 leaves the spec entirely.
    block.store(true, AtomicOrdering::SeqCst);
    let mut spec_a2 = fx.stub_spec("t1", &["w2"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;
    // A stale sibling entry created while the old gen drains — the
    // pending Final pins the WHOLE worker dir (fail closed); it is
    // reaped together once the commit lands.
    let wdir = tenant_dir_a.join("w1");
    let stale = wdir.join(key64(b's'));
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("stale.bin"), b"stale").unwrap();

    // drain_timeout(600ms) + several monitor ticks (50ms): with the
    // bug, the very first post-drain sweep deletes <t>/w1 wholesale.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let marker = wdir.join(format!("{key}.final-snap-pending"));
    assert!(
        wdir.join(&key).join("payload.bin").exists(),
        "RV-R24: pending-Final source dir must survive the worker-removal sweep"
    );
    assert!(marker.exists(), "final-snap-pending marker must survive");
    assert!(
        stale.join("stale.bin").exists(),
        "whole worker dir pinned while a Final is pending"
    );
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: wdir.join(&key),
    };
    let rec = plc_a.record(&scope).unwrap().expect("placement record");
    assert!(
        !rec.holder.is_empty(),
        "failed Final must still hold the lease — never released by the sweep"
    );

    // Store recovers → the bounded retry commits Final, releases the
    // held lease. RV-R53: spec-absence is NOT deletion — without a
    // signed tombstone the worker dir is RETAINED even after the
    // Final lands (an explicit deletion tombstone is the only sweep
    // authorization now).
    block.store(false, AtomicOrdering::SeqCst);
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let rec = plc_a.record(&scope).unwrap();
        let committed = rec.as_ref().is_some_and(|r| {
            r.snapshot
                .as_ref()
                .is_some_and(|s| s.kind == vrt_host::do_placement_store::SnapKind::Final)
        });
        if committed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Final commit never converged: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        wdir.join(&key).join("payload.bin").exists(),
        "RV-R53: spec-absent worker dir retained — no tombstone, no sweep"
    );
    handle_a.send(SupervisorCmd::Stop).await;
}

/// P0-2b: a periodic snapshot alone NEVER satisfies a transferred
/// class — the launch stays fail-closed until an operator accepts
/// the baseline explicitly (`accept_baseline`, the admin route's CAS).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_snapshot_rejected_until_admin_accepts() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir = fx.dir.path().join("do").join("t1");
    let key = key64(b'a');
    std::fs::create_dir_all(&tenant_dir).unwrap();

    let (store, plc) = placement_pair("cell:test:holder-a");
    let deps = deps_with(store.clone(), plc.clone(), &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w2"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w2", &key)], &[("w2", &key, "w1")]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_ne!(runtime.state(), TenantState::Ready);

    // Seed a PERIODIC baseline the way an hourly snapshot would:
    // fence the record, commit kind=Periodic onto the dead-marker.
    let seed_dir = tenant_dir.join("seed-snap");
    std::fs::create_dir_all(&seed_dir).unwrap();
    std::fs::write(seed_dir.join("payload.bin"), b"stale-periodic").unwrap();
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: seed_dir.clone(),
    };
    let rec = plc.record(&scope).unwrap().expect("placement record");
    plc.fence_for_migration(
        &scope,
        vrt_host::do_placement::PlacementEpoch(rec.epoch),
        "cell:test:holder-b",
        30_000,
    )
    .unwrap();
    let stage = fx.dir.path().join("seed-stage");
    let meta = vrt_sql::do_snap::snapshot_dir(&seed_dir, &stage, rec.epoch, 42).unwrap();
    for f in &meta.files {
        let data = std::fs::read(stage.join(&f.path)).unwrap();
        vrt_objstore::block_on_store(store.put(
            &vrt_host::do_placement_store::snap_file_key(&key, rec.epoch, 42, &f.path),
            &data,
        ))
        .unwrap();
    }
    let mkey = vrt_host::do_placement_store::snap_manifest_key(&key, rec.epoch, 42);
    vrt_objstore::block_on_store(store.put(&mkey, &serde_json::to_vec(&meta).unwrap())).unwrap();
    plc.commit_snapshot_fenced(
        &scope,
        rec.epoch,
        vrt_host::do_placement_store::SnapRef {
            manifest_key: mkey,
            epoch: rec.epoch,
            seq: 42,
            files: meta.files.len() as u32,
            bytes: meta.files.iter().map(|f| f.size).sum(),
            at_ms: meta.created_ms,
            kind: vrt_host::do_placement_store::SnapKind::Periodic,
        },
    )
    .unwrap();
    plc.release_reservation(&scope, rec.epoch).unwrap();

    // Periodic-only baseline: the gate keeps the launch refused even
    // across retries — no silent stale restore.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_ne!(
        runtime.state(),
        TenantState::Ready,
        "a periodic snapshot must never satisfy a transferred class"
    );
    assert!(!tenant_dir.join("w2").join(&key).exists());

    // Admin escape: accept the baseline → the next retry serves it.
    let accepted = plc.accept_baseline(&scope).expect("admin accept");
    assert_eq!(accepted.seq, 42);
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(15)).await;
    assert!(tenant_dir
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    handle.send(SupervisorCmd::Stop).await;
}

/// P1-1: a quarantined source dir is evidence, not garbage — the
/// sweep keeps it inside the retention window and only reclaims it
/// after the `.since` marker ages past the configured bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quarantine_respects_retention_window() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir = fx.dir.path().join("do").join("t1");
    let key = key64(b'a');
    let qdir = tenant_dir.join("w1").join(format!("{key}.xfer-quarantine"));
    std::fs::create_dir_all(&qdir).unwrap();
    std::fs::write(qdir.join("payload.bin"), b"fenced-provenance").unwrap();
    let (store, plc) = placement_pair("cell:test:holder-a");
    let mut deps = deps_with(store, plc, &fx.dir.path().join("snap-tmp"));
    // Tight retention for the test — still inside the [1h,30d] clamp
    // at config level; deps is the raw field so any ms value applies.
    // 3 s, not sub-second: the mark→sweep gap includes spawn latency,
    // which a loaded runner can stretch past a smaller window.
    Arc::get_mut(deps.do_snap.as_mut().unwrap())
        .unwrap()
        .xfer_quarantine_ms = 3_000;
    let mut spec = fx.stub_spec("t1", &["w1"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[], &[]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    // Mark AFTER Ready so the wall-clock window starts where the
    // asserts measure it — a marker written before spawn ages through
    // the whole bringup.
    vrt_host::do_transfer::mark_quarantine(
        &tenant_dir.join("w1"),
        &format!("{key}.xfer-quarantine"),
    );

    // Within retention the quarantine must survive the sweep.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        qdir.join("payload.bin").exists(),
        "in-window quarantine kept"
    );

    // Past the window the sweep must reclaim it — bounded poll, not a
    // bare sleep, so a slow monitor tick can't fake the outcome.
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    while qdir.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "expired quarantine never reclaimed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    handle.send(SupervisorCmd::Stop).await;
}

/// P1-2: a pending Final retry on an epoch another holder provably
/// owns is TERMINAL — the entry leaves the retry set, the marker
/// clears, and the fenced source moves to retention quarantine
/// instead of looping forever or being deleted outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_terminal_quarantines_pending_source() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir = fx.dir.path().join("do").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir, "w1", &key);
    // Restart-marker: epoch 1 was ours, we crashed before committing.
    std::fs::write(
        tenant_dir
            .join("w1")
            .join(format!("{key}.final-snap-pending")),
        "1 transfer",
    )
    .unwrap();

    let (store, plc) = placement_pair("cell:test:holder-a");
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    // Another holder provably owns the epoch — terminal for our retry.
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir.join("w1").join(&key),
    };
    use vrt_host::do_placement::DoPlacement;
    plc_b.acquire(&scope).unwrap();

    let deps = deps_with(store, plc, &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w1"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[], &[]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    // Sweep seeds the pending entry from the marker; the retry sees
    // the epoch owned by holder-b → terminal → quarantine.
    let qdir = tenant_dir.join("w1").join(format!("{key}.xfer-quarantine"));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline && !qdir.exists() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        qdir.join("payload.bin").exists(),
        "fenced source quarantined"
    );
    assert!(!tenant_dir.join("w1").join(&key).exists());
    assert!(!tenant_dir
        .join("w1")
        .join(format!("{key}.final-snap-pending"))
        .exists());
    handle.send(SupervisorCmd::Stop).await;
}

// ---------- DO1-r3 ----------

/// P0-1: an acceptance is pinned to ONE snapshot {epoch, seq} — a
/// later commit of ANY kind clears it (never widens a stale bless);
/// a pre-r3 `baseline_accepted: true` record decodes as UNACCEPTED
/// (fail closed — the blanket flag can never carry forward).
#[test]
fn accepted_baseline_is_snapshot_pinned_and_cleared() {
    use vrt_host::do_placement::{DoPlacement, PlacementEpoch};
    use vrt_host::do_placement_store::*;
    let (store, plc) = placement_pair("cell:test:holder-a");
    let tmp = tempfile::tempdir().unwrap();
    let key = key64(b'c');
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tmp.path().join("cls"),
    };
    plc.acquire(&scope).unwrap(); // live @1 holder-a
    let snap = |seq| SnapRef {
        manifest_key: snap_manifest_key(&key, 1, seq),
        epoch: 1,
        seq,
        files: 1,
        bytes: 1,
        at_ms: 1,
        kind: SnapKind::Periodic,
    };
    plc.commit_snapshot(&scope, PlacementEpoch(1), snap(7))
        .unwrap();
    let accepted = plc.accept_baseline(&scope).unwrap();
    assert_eq!(accepted.seq, 7);
    let rec = plc.record(&scope).unwrap().unwrap();
    assert_eq!(
        rec.accepted_baseline,
        Some(AcceptedBaseline {
            epoch: 1,
            seq: 7,
            at_ms: rec.accepted_baseline.unwrap().at_ms
        }),
        "pin binds exactly (epoch,seq) of the accepted snapshot"
    );
    // Idempotent re-accept of the SAME pin.
    plc.accept_baseline(&scope).unwrap();
    // A NEWER snapshot commit invalidates the pin — the acceptance
    // can never silently bless a baseline the operator never saw.
    plc.commit_snapshot(&scope, PlacementEpoch(1), snap(8))
        .unwrap();
    let rec = plc.record(&scope).unwrap().unwrap();
    assert_eq!(rec.accepted_baseline, None, "commit must clear the pin");
    assert_eq!(rec.snapshot.unwrap().seq, 8);

    // fenced-commit path clears it the same way.
    plc.accept_baseline(&scope).unwrap();
    plc.fence(&scope, PlacementEpoch(1)).unwrap();
    plc.commit_snapshot_fenced(
        &scope,
        1,
        SnapRef {
            manifest_key: snap_manifest_key(&key, 1, 9),
            epoch: 1,
            seq: 9,
            files: 1,
            bytes: 1,
            at_ms: 2,
            kind: SnapKind::Final,
        },
    )
    .unwrap();
    assert_eq!(plc.record(&scope).unwrap().unwrap().accepted_baseline, None);

    // Pre-r3 record: `baseline_accepted: true` with no `accepted_
    // baseline` field → decodes as UNACCEPTED (fail closed).
    let key2 = key64(b'd');
    let legacy = serde_json::json!({
        "v": 1, "epoch": 3, "holder": "", "expires_at_ms": 0,
        "snapshot": null, "baseline_accepted": true,
    });
    vrt_objstore::block_on_store(
        store.put(&placement_key(&key2), &serde_json::to_vec(&legacy).unwrap()),
    )
    .unwrap();
    let scope2 = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key2.clone(),
        class_dir: tmp.path().join("cls2"),
    };
    let rec2 = plc.record(&scope2).unwrap().unwrap();
    assert_eq!(rec2.epoch, 3);
    assert_eq!(rec2.accepted_baseline, None, "r2 blanket bool → unaccepted");
}

/// P0-2: `Final` alone is insufficient — it must have been committed
/// under the epoch IMMEDIATELY preceding our acquire (truth table of
/// the extracted gate predicate).
#[test]
fn transfer_baseline_ok_truth_table() {
    use vrt_host::do_placement_store::*;
    let snap = |epoch, seq, kind| SnapRef {
        manifest_key: format!("do/k/{epoch}/snap-{seq}.json"),
        epoch,
        seq,
        files: 1,
        bytes: 1,
        at_ms: 1,
        kind,
    };
    let acc = |epoch, seq| {
        Some(AcceptedBaseline {
            epoch,
            seq,
            at_ms: 1,
        })
    };
    let ok = vrt_host::do_transfer::transfer_baseline_ok;
    // --- r4 writer_epoch semantics ---
    // Final stamped by the LAST REAL WRITER (writer_epoch=4) → serve,
    // regardless of how many times we re-acquired (5, 6, …).
    assert!(ok(&snap(4, 9, SnapKind::Final), None, 5, 4));
    assert!(ok(&snap(4, 9, SnapKind::Final), None, 6, 4));
    // The r3 review scenario, expressed via writer_epoch: the last
    // writer was epoch 5 (it launched and wrote) but its Final never
    // committed — Final@4 is stale provenance → reject.
    assert!(!ok(&snap(4, 9, SnapKind::Final), None, 6, 5));
    // An acquire-only epoch does not move the requirement: holder @5
    // never launched (writer_epoch still 4) → Final@4 still valid.
    assert!(ok(&snap(4, 9, SnapKind::Final), None, 6, 4));
    // Legacy fallback (writer_epoch=0, pre-r4 record): r3 rule —
    // Final at acquired-1 only.
    assert!(ok(&snap(4, 9, SnapKind::Final), None, 5, 0));
    assert!(!ok(&snap(4, 9, SnapKind::Final), None, 6, 0));
    // A Final stamped with OUR epoch is invalid under both rules.
    assert!(!ok(&snap(5, 9, SnapKind::Final), None, 5, 4));
    assert!(!ok(&snap(5, 9, SnapKind::Final), None, 5, 0));
    // Periodic + exact pin → serve (admin explicitly accepted).
    assert!(ok(&snap(4, 9, SnapKind::Periodic), acc(4, 9), 6, 5));
    // A pin for an OLDER seq does not bless the newer snapshot.
    assert!(!ok(&snap(4, 10, SnapKind::Periodic), acc(4, 9), 6, 5));
    // A pin for another epoch does not travel sideways either.
    assert!(!ok(&snap(3, 9, SnapKind::Periodic), acc(4, 9), 5, 4));
    // Periodic is never enough without the pin — even at the exact
    // writer epoch.
    assert!(!ok(&snap(4, 9, SnapKind::Periodic), None, 5, 4));
}

/// P0-2 integration: the record carries ONLY an earlier-epoch Final
/// (epoch E's; holder E+1 wrote without ever snapshotting) — the
/// target must stay fail-closed; the admin pin on {epoch,seq} is the
/// explicit way out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_epoch_final_rejected_until_admin_accepts() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir = fx.dir.path().join("do").join("t1");
    let key = key64(b'a');
    std::fs::create_dir_all(&tenant_dir).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_x = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-x",
        LEASE_MS,
        MARGIN_MS,
    ));
    use vrt_host::do_placement::{DoPlacement, PlacementEpoch};
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir.join("w1").join(&key),
    };

    // Record history: holder-x @1 committed a REAL Final@1, fenced;
    // holder-b took @2, wrote (never snapshotted), fenced. Our
    // supervisor's w2 acquire mints @3 — the Final@1 predates the
    // @2 holder's writes and must NOT satisfy the gate.
    plc_x.acquire(&scope).unwrap();
    let stage = fx.dir.path().join("stage");
    // Staging OUTSIDE the worker segment — a dir at w1/<key> would be
    // a same-cell local source and take the move path, bypassing the
    // cross-cell gate this test targets.
    let seed_dir = fx.dir.path().join("seed-src");
    std::fs::create_dir_all(&seed_dir).unwrap();
    std::fs::write(seed_dir.join("payload.bin"), b"epoch-1-final").unwrap();
    let meta = vrt_sql::do_snap::snapshot_dir(&seed_dir, &stage, 1, 9).unwrap();
    for f in &meta.files {
        let data = std::fs::read(stage.join(&f.path)).unwrap();
        vrt_objstore::block_on_store(store.put(
            &vrt_host::do_placement_store::snap_file_key(&key, 1, 9, &f.path),
            &data,
        ))
        .unwrap();
    }
    let mkey = vrt_host::do_placement_store::snap_manifest_key(&key, 1, 9);
    vrt_objstore::block_on_store(store.put(&mkey, &serde_json::to_vec(&meta).unwrap())).unwrap();
    plc_x
        .commit_snapshot(
            &scope,
            PlacementEpoch(1),
            vrt_host::do_placement_store::SnapRef {
                manifest_key: mkey,
                epoch: 1,
                seq: 9,
                files: meta.files.len() as u32,
                bytes: meta.files.iter().map(|f| f.size).sum(),
                at_ms: meta.created_ms,
                kind: vrt_host::do_placement_store::SnapKind::Final,
            },
        )
        .unwrap();
    plc_x.fence(&scope, PlacementEpoch(1)).unwrap(); // dead@1, Final@1 rides
    let plc_b0 = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b0",
        LEASE_MS,
        MARGIN_MS,
    ));
    plc_b0.acquire(&scope).unwrap(); // live @2
                                     // DO1-r4: epoch-2's holder LAUNCHED a writer (claim) then wrote
                                     // without ever snapshotting — writer_epoch=2 is what makes
                                     // Final@1 provably stale. An acquire-only epoch would not.
    plc_b0.claim_writer(&scope, PlacementEpoch(2)).unwrap();
    plc_b0.fence(&scope, PlacementEpoch(2)).unwrap(); // dead@2
                                                      // The supervisor-side scope (dst worker) — record keyed by class.
    let dst_scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w2".into(),
        class_key: key.clone(),
        class_dir: tenant_dir.join("w2").join(&key),
    };

    let deps = deps_with(store.clone(), plc.clone(), &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w2"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w2", &key)], &[("w2", &key, "w1")]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_ne!(
        runtime.state(),
        TenantState::Ready,
        "a Final from an earlier epoch must not satisfy the gate"
    );
    assert!(!tenant_dir.join("w2").join(&key).exists());
    let rec = plc.record(&dst_scope).unwrap().unwrap();
    assert_eq!(rec.epoch, 3, "target must have taken epoch+1 over dead@2");
    assert!(rec.holder.is_empty() || rec.holder == "cell:test:holder-a");

    // Admin pin on the EXACT snapshot → the next retry serves it —
    // acceptance, not epoch math, is what legitimizes a stale Final.
    plc.accept_baseline(&dst_scope).unwrap();
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(15)).await;
    assert_eq!(
        std::fs::read(tenant_dir.join("w2").join(&key).join("payload.bin")).unwrap(),
        b"epoch-1-final"
    );
    handle.send(SupervisorCmd::Stop).await;
}

/// P1-2: host-restart recovery — `held_departing` is in-memory and
/// lost; the `.final-snap-pending` marker re-seeds the retry set on
/// the next sweep. With the lease already lapsed/fenced (record is a
/// dead-marker at the departed epoch), the retry commits the Final
/// via the fenced-commit branch; the target then acquires epoch+1
/// over it and serves — never a stale baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_reseeds_pending_and_final_unblocks_target() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);
    // The restart marker: epoch 1 was ours, we crashed pre-commit.
    std::fs::write(
        tenant_dir_a
            .join("w1")
            .join(format!("{key}.final-snap-pending")),
        "1 transfer",
    )
    .unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    use vrt_host::do_placement::{DoPlacement, PlacementEpoch};
    // The lease lapsed while we were down: holder-x (a foreign cell's
    // view) had fenced us into a dead-marker @1 — no snapshot yet.
    let plc_x = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-x",
        LEASE_MS,
        MARGIN_MS,
    ));
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key),
    };
    plc_x.acquire(&scope).unwrap();
    plc_x.fence(&scope, PlacementEpoch(1)).unwrap(); // dead@1, no snap

    // Restarted cell A: no in-memory pending — only the marker.
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));
    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;

    // Sweep re-seeds pending from the marker → the retry's fenced
    // branch lands Final@1 on the dead-marker and clears the marker.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let rec = plc_a.record(&scope).unwrap();
        let done = rec.as_ref().is_some_and(|r| {
            r.snapshot.as_ref().is_some_and(|s| {
                s.kind == vrt_host::do_placement_store::SnapKind::Final && s.epoch == 1
            })
        });
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "restart re-seeded retry never landed Final@1: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!tenant_dir_a
        .join("w1")
        .join(format!("{key}.final-snap-pending"))
        .exists());

    // Target cell B acquires @2 over dead@1 → Final@1 is at the
    // immediate predecessor epoch → restore and serve.
    let tenant_dir_b = fx.dir.path().join("do-b").join("t1");
    let deps_b = deps_with(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(
        &tenant_dir_b,
        &[("w2", &key)],
        &[("w2", &key, "w1")],
    ));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(15)).await;
    assert!(tenant_dir_b
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// P2/r3: a stale pending entry whose class key was RE-DECLARED
/// (delete→re-declare) must NEVER quarantine the new live writer's
/// dir when the retry hits a fenced-terminal verdict — the dir now
/// belongs to the current generation, not to the dead epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_terminal_never_quarantines_redeclared_live_dir() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir = fx.dir.path().join("do").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir, "w1", &key);
    // Stale marker from an earlier delete-shaped departure @1.
    std::fs::write(
        tenant_dir
            .join("w1")
            .join(format!("{key}.final-snap-pending")),
        "1 transfer",
    )
    .unwrap();

    let (store, plc) = placement_pair("cell:test:holder-a");
    // The class was RE-DECLARED and our own cell already holds @2 —
    // the live writer's lease reaffirms this same record.
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir.join("w1").join(&key),
    };
    let live = serde_json::json!({
        "v": 1, "epoch": 2, "holder": "cell:test:holder-a",
        "expires_at_ms": 9_999_999_999_999u64, "snapshot": null,
    });
    vrt_objstore::block_on_store(store.put(
        &vrt_host::do_placement_store::placement_key(&key),
        &serde_json::to_vec(&live).unwrap(),
    ))
    .unwrap();

    let deps = deps_with(store.clone(), plc.clone(), &fx.dir.path().join("snap-tmp"));
    let mut spec = fx.stub_spec("t1", &["w1"], &[]);
    spec.do_storage = Some(do_spec(&tenant_dir, &[("w1", &key)], &[]));
    let runtime = TenantRuntime::new("t1");
    let handle = vrt_host::supervisor::spawn_supervisor(runtime.clone(), spec, deps, test_params());
    wait_state(&runtime, TenantState::Ready, Duration::from_secs(8)).await;

    // Sweep seeds pending@1 → retry: record live@2 held by us → held
    // branch commit CAS fails (epoch 2≠1) → rec.epoch > entry.epoch
    // → fenced-TERMINAL. The guard must see the active gen's lease
    // entry for `key` and REFUSE to quarantine the live dir.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline
        && tenant_dir
            .join("w1")
            .join(format!("{key}.final-snap-pending"))
            .exists()
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!tenant_dir
        .join("w1")
        .join(format!("{key}.final-snap-pending"))
        .exists());
    assert!(
        tenant_dir
            .join("w1")
            .join(&key)
            .join("payload.bin")
            .exists(),
        "a re-declared live dir must never be quarantined"
    );
    assert!(!tenant_dir
        .join("w1")
        .join(format!("{key}.xfer-quarantine"))
        .exists());
    let rec = plc.record(&scope).unwrap().unwrap();
    assert_eq!(rec.epoch, 2);
    handle.send(SupervisorCmd::Stop).await;
}
/// DO1-r4 P0a (liveness): a TRANSIENT restore failure must not
/// strand a valid Final. Target acquires@2, the snapshot GET fails
/// mid-restore, leases release, retry acquires@3 — `Final@1` must
/// still satisfy the gate because `writer_epoch` never moved (epoch 2
/// only ever acquired, never launched). r3's strict `epoch+1` rule
/// made this permanently stuck.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_restore_failure_recovers_at_later_epoch() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    let block_get = Arc::new(AtomicBool::new(false));
    let off = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(SnapBlockStore {
        inner: MemoryStore::new(),
        block: off.clone(),
        block_all: off.clone(),
        block_get: block_get.clone(),
        block_wal: off.clone(),
    });
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));
    let deps_b = deps_with(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));

    // Cell A: real launch (claims writer_epoch=1) → drain → Final@1
    // → dead-marker carries writer_epoch=1.
    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;
    let mut spec_a2 = fx.stub_spec("t1", &["w1"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;

    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key),
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let rec = plc_a.record(&scope).unwrap();
        let done = rec
            .as_ref()
            .is_some_and(|r| r.holder.is_empty() && r.snapshot.is_some());
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Final snapshot never committed/released: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rec = plc_a.record(&scope).unwrap().unwrap();
    assert_eq!(
        rec.writer_epoch, 1,
        "writer_epoch must ride the dead-marker"
    );

    // Transient outage: snapshot reads fail; placement reads/CAS fine.
    block_get.store(true, AtomicOrdering::SeqCst);
    let tenant_dir_b = fx.dir.path().join("do-b").join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(
        &tenant_dir_b,
        &[("w2", &key)],
        &[("w2", &key, "w1")],
    ));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());

    // Epoch-2 attempt: gate passes (Final@1 == writer_epoch 1) but the
    // manifest GET fails → release → retry. Wait for one full acquire/
    // fail/release cycle to land before unblocking — a fixed sleep is a
    // race on a loaded runner: zero completed cycles lets the FIRST
    // unblocked attempt claim epoch 2 and the `epoch >= 3` check below
    // can never hold. The released record (empty holder, epoch advanced
    // past the dead-marker) is the proof a cycle ran.
    let cycle_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rec = plc_a.record(&scope).unwrap();
        if rec
            .as_ref()
            .is_some_and(|r| r.epoch >= 2 && r.holder.is_empty())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < cycle_deadline,
            "blocked attempt never completed an acquire/release cycle: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_ne!(runtime_b.state(), TenantState::Ready);
    let rec_mid = plc_a.record(&scope).unwrap().unwrap();
    assert!(
        rec_mid.epoch >= 2 && rec_mid.holder.is_empty() && rec_mid.writer_epoch == 1,
        "acquire-only epochs must NOT advance writer_epoch: {rec_mid:?}"
    );

    // Outage clears → retry re-acquires a LATER epoch and the SAME
    // Final@1 still restores — the liveness r3 lost.
    block_get.store(false, AtomicOrdering::SeqCst);
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(20)).await;
    assert!(tenant_dir_b
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    let rec = plc_a.record(&scope).unwrap().unwrap();
    assert!(
        rec.epoch >= 3,
        "the failed attempt must have bumped the acquire epoch: {rec:?}"
    );
    assert_eq!(
        rec.writer_epoch, rec.epoch,
        "the surviving attempt's claim stamped its own epoch"
    );
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// DO1-r4 P0b: a sibling class's restore failure releases EVERY lease
/// (restore_failed → release_leases(&leases)) — the transfer class is
/// a pure victim. After the retry at a later epoch, `Final@1` must
/// still restore via writer_epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sibling_fail_closed_recovers_transfer_class() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key_a = key64(b'a'); // sibling — restores a periodic snapshot
    let key_b = key64(b'b'); // the transferred class
    seed_class_dir(&tenant_dir_a, "w1", &key_b);

    let block_get = Arc::new(AtomicBool::new(false));
    let off = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(SnapBlockStore {
        inner: MemoryStore::new(),
        block: off.clone(),
        block_all: off.clone(),
        block_get: block_get.clone(),
        block_wal: off.clone(),
    });
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));
    let deps_b = deps_with(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));

    // Cell A drains key_b → Final@1, dead-marker writer_epoch=1.
    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key_b)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;
    let mut spec_a2 = fx.stub_spec("t1", &["w1"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;
    let scope_b = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key_b.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key_b),
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let rec = plc_a.record(&scope_b).unwrap();
        if rec
            .as_ref()
            .is_some_and(|r| r.holder.is_empty() && r.snapshot.is_some())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Final snapshot never committed/released: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Seed key_a: dead@1 with a committed PERIODIC snapshot (a plain
    // class — never gated, always restorable).
    use vrt_host::do_placement::{DoPlacement, PlacementEpoch};
    let scope_a = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key_a.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key_a),
    };
    plc_a.acquire(&scope_a).unwrap();
    let stage = fx.dir.path().join("stage-a");
    let seed_dir = fx.dir.path().join("seed-a");
    std::fs::create_dir_all(&seed_dir).unwrap();
    std::fs::write(seed_dir.join("sibling.bin"), b"sibling-data").unwrap();
    let meta = vrt_sql::do_snap::snapshot_dir(&seed_dir, &stage, 1, 7).unwrap();
    for f in &meta.files {
        let data = std::fs::read(stage.join(&f.path)).unwrap();
        vrt_objstore::block_on_store(store.put(
            &vrt_host::do_placement_store::snap_file_key(&key_a, 1, 7, &f.path),
            &data,
        ))
        .unwrap();
    }
    let mkey = vrt_host::do_placement_store::snap_manifest_key(&key_a, 1, 7);
    vrt_objstore::block_on_store(store.put(&mkey, &serde_json::to_vec(&meta).unwrap())).unwrap();
    plc_a
        .commit_snapshot(
            &scope_a,
            PlacementEpoch(1),
            vrt_host::do_placement_store::SnapRef {
                manifest_key: mkey,
                epoch: 1,
                seq: 7,
                files: meta.files.len() as u32,
                bytes: meta.files.iter().map(|f| f.size).sum(),
                at_ms: meta.created_ms,
                kind: vrt_host::do_placement_store::SnapKind::Periodic,
            },
        )
        .unwrap();
    plc_a.fence(&scope_a, PlacementEpoch(1)).unwrap(); // dead@1

    // Target worker w2 runs BOTH classes (a first — its blocked
    // restore is the sibling failure that releases b's lease too).
    block_get.store(true, AtomicOrdering::SeqCst);
    let tenant_dir_b = fx.dir.path().join("do-b").join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    let mut worker_classes: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    worker_classes.insert("w2".to_string(), vec![key_a.clone(), key_b.clone()]);
    let mut transfer_sources: std::collections::BTreeMap<(String, String), String> =
        std::collections::BTreeMap::new();
    transfer_sources.insert(("w2".to_string(), key_b.clone()), "w1".to_string());
    spec_b.do_storage = Some(vrt_host::spec::DoStorageSpec {
        tenant_dir: tenant_dir_b.to_path_buf(),
        worker_classes,
        transfer_sources,
    });
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    // Same cycle wait as above: a completed acquire/fail/release round
    // is what bumps the victim lease past the dead-marker epoch — a bare
    // sleep leaves it to scheduler luck on a loaded runner.
    let cycle_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rec = plc_a.record(&scope_b).unwrap();
        if rec
            .as_ref()
            .is_some_and(|r| r.epoch >= 2 && r.holder.is_empty())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < cycle_deadline,
            "blocked sibling attempt never completed an acquire/release cycle: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_ne!(runtime_b.state(), TenantState::Ready);

    // Sibling's outage clears → retry acquires both at later epochs;
    // a restores its periodic, b passes the gate via writer_epoch.
    block_get.store(false, AtomicOrdering::SeqCst);
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(20)).await;
    assert!(tenant_dir_b
        .join("w2")
        .join(&key_b)
        .join("payload.bin")
        .exists());
    assert!(tenant_dir_b
        .join("w2")
        .join(&key_a)
        .join("sibling.bin")
        .exists());
    let rec = plc_a.record(&scope_b).unwrap().unwrap();
    assert!(
        rec.epoch >= 3 && rec.writer_epoch == rec.epoch,
        "the victim class re-acquired a later epoch and claimed it: {rec:?}"
    );
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// DO1-r4 P0c: when `claim_writer`'s CAS cannot land, the writer must
/// NOT launch — stamped epochs and acquired epochs must stay
/// distinguishable forever (fail closed, then recover on unblock).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_epoch_claim_failure_blocks_launch() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    // A claim write is the ONLY record body carrying
    // `writer_epoch == epoch` — CAS bodies that merely CARRY a stale
    // field (acquire/renew/commit) always have writer < epoch.
    struct ClaimBlockStore {
        inner: MemoryStore,
        block_claim: Arc<AtomicBool>,
    }
    #[async_trait::async_trait]
    impl ObjectStore for ClaimBlockStore {
        async fn get(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
            self.inner.get(k).await
        }
        async fn head(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
            self.inner.head(k).await
        }
        async fn put_if_none_match(
            &self,
            k: &str,
            b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put_if_none_match(k, b).await
        }
        async fn put_if_match(
            &self,
            k: &str,
            e: &str,
            b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            if self.block_claim.load(AtomicOrdering::SeqCst) {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(b) {
                    if v.get("epoch").and_then(|x| x.as_u64())
                        == v.get("writer_epoch").and_then(|x| x.as_u64())
                        && v["epoch"].as_u64().is_some_and(|n| n >= 1)
                    {
                        return Err(vrt_objstore::StoreError::unavailable(
                            false,
                            "claim CAS blocked by test",
                        ));
                    }
                }
            }
            self.inner.put_if_match(k, e, b).await
        }
        async fn put(&self, k: &str, b: &[u8]) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put(k, b).await
        }
        async fn list(
            &self,
            p: &str,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            self.inner.list(p).await
        }
        async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
            self.inner.delete(k).await
        }
    }
    let block_claim = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(ClaimBlockStore {
        inner: MemoryStore::new(),
        block_claim: block_claim.clone(),
    });
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    let deps_a = deps_with(store.clone(), plc_a.clone(), &fx.dir.path().join("snap-a"));
    let deps_b = deps_with(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));

    // Cell A drains → Final@1, dead-marker writer_epoch=1.
    let mut spec_a = fx.stub_spec("t1", &["w1"], &[]);
    spec_a.do_storage = Some(do_spec(&tenant_dir_a, &[("w1", &key)], &[]));
    let runtime_a = TenantRuntime::new("t1");
    let handle_a =
        vrt_host::supervisor::spawn_supervisor(runtime_a.clone(), spec_a, deps_a, test_params());
    wait_state(&runtime_a, TenantState::Ready, Duration::from_secs(8)).await;
    let mut spec_a2 = fx.stub_spec("t1", &["w1"], &[]);
    spec_a2.do_storage = Some(do_spec(&tenant_dir_a, &[], &[]));
    handle_a.send(SupervisorCmd::Apply(Box::new(spec_a2))).await;
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key),
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let rec = plc_a.record(&scope).unwrap();
        if rec
            .as_ref()
            .is_some_and(|r| r.holder.is_empty() && r.snapshot.is_some())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Final snapshot never committed/released: {rec:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Target with claim writes blocked: acquire/restore succeed but
    // launch must never happen — writer_epoch stays at 1 forever.
    block_claim.store(true, AtomicOrdering::SeqCst);
    let tenant_dir_b = fx.dir.path().join("do-b").join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(
        &tenant_dir_b,
        &[("w2", &key)],
        &[("w2", &key, "w1")],
    ));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_ne!(
        runtime_b.state(),
        TenantState::Ready,
        "a failed writer_epoch claim must prevent the launch"
    );
    let rec = plc_a.record(&scope).unwrap().unwrap();
    assert_eq!(
        rec.writer_epoch, 1,
        "no epoch may be stamped writer without a real launch"
    );

    // CAS recovers → next retry claims its epoch and serves.
    block_claim.store(false, AtomicOrdering::SeqCst);
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(20)).await;
    assert!(tenant_dir_b
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    let rec = plc_a.record(&scope).unwrap().unwrap();
    assert_eq!(rec.writer_epoch, rec.epoch);
    handle_a.send(SupervisorCmd::Stop).await;
    handle_b.send(SupervisorCmd::Stop).await;
}

/// Deps variant with the WAL replicator wired in — restore-time
/// replay runs through `snap.wal` only when it is `Some` (RV-R28 #3).
fn deps_with_wal(
    store: Arc<dyn ObjectStore>,
    plc: Arc<ManifestDoPlacement>,
    tmp_root: &Path,
) -> SupervisorDeps {
    let mut deps = SupervisorDeps::default();
    let wal = Arc::new(vrt_host::do_wal::DoWalReplicator::new(
        vrt_host::do_wal::WalCfg {
            enabled: true,
            tick_ms: 0,
            max_dbs: 8,
            max_pending_bytes: 256 << 20,
            max_segment_bytes: 1 << 20,
            upload_attempts: 2,
            tick_upload_budget_ms: 500,
            idle_stop_ms: u64::MAX,
            flush_max_ms: 5_000,
            gc_max_per_tick: 64,
            replay_max_bytes: 1 << 30,
            rebaseline_chain_bytes: 1 << 29,
            baseline_backoff_max_ms: 1_800_000,
        },
        store.clone(),
        tmp_root.to_path_buf(),
        Arc::new(|| 1_000_000u64),
    ));
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
        wal: Some(wal),
    }));
    deps.placement = plc;
    deps
}

/// RV-R28 #3: a WAL replay failure must REFUSE the launch — before
/// this fix the Err branch logged "serving snapshot baseline" and the
/// instance started from the baseline alone; post-baseline commits
/// were silently dropped and a new writer epoch made the loss
/// permanent. Now it fails closed like the genesis path: leases
/// released, `do-restore-failed` last_error, bounded retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_replay_failure_refuses_baseline_launch() {
    init_logs();
    let mut fx = Fixture::new();
    let tenant_dir_a = fx.dir.path().join("do-a").join("t1");
    let key = key64(b'a');
    seed_class_dir(&tenant_dir_a, "w1", &key);

    let block_wal = Arc::new(AtomicBool::new(false));
    let off = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(SnapBlockStore {
        inner: MemoryStore::new(),
        block: off.clone(),
        block_all: off.clone(),
        block_get: off.clone(),
        block_wal: block_wal.clone(),
    });
    let plc_a = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-a",
        LEASE_MS,
        MARGIN_MS,
    ));
    let plc_b = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell:test:holder-b",
        LEASE_MS,
        MARGIN_MS,
    ));
    // Cell B gets the WAL replicator wired — replay actually runs.
    let deps_b = deps_with_wal(store.clone(), plc_b.clone(), &fx.dir.path().join("snap-b"));

    // Seed: dead@1 with a committed PERIODIC snapshot + writer_epoch=1
    // stamped (claim_writer) so the restore reaches the WAL replay
    // branch (a Final or epoch-mismatched baseline skips it — I1).
    use vrt_host::do_placement::{DoPlacement, PlacementEpoch};
    let scope = vrt_host::do_placement::PlacementScope {
        tenant: "t1".into(),
        worker: "w1".into(),
        class_key: key.clone(),
        class_dir: tenant_dir_a.join("w1").join(&key),
    };
    plc_a.acquire(&scope).unwrap();
    plc_a.claim_writer(&scope, PlacementEpoch(1)).unwrap();
    let stage = fx.dir.path().join("stage-w");
    let seed_dir = fx.dir.path().join("seed-w");
    std::fs::create_dir_all(&seed_dir).unwrap();
    std::fs::write(seed_dir.join("payload.bin"), b"wal-restore-data").unwrap();
    let meta = vrt_sql::do_snap::snapshot_dir(&seed_dir, &stage, 1, 7).unwrap();
    for f in &meta.files {
        let data = std::fs::read(stage.join(&f.path)).unwrap();
        vrt_objstore::block_on_store(store.put(
            &vrt_host::do_placement_store::snap_file_key(&key, 1, 7, &f.path),
            &data,
        ))
        .unwrap();
    }
    let mkey = vrt_host::do_placement_store::snap_manifest_key(&key, 1, 7);
    vrt_objstore::block_on_store(store.put(&mkey, &serde_json::to_vec(&meta).unwrap())).unwrap();
    plc_a
        .commit_snapshot(
            &scope,
            PlacementEpoch(1),
            vrt_host::do_placement_store::SnapRef {
                manifest_key: mkey,
                epoch: 1,
                seq: 7,
                files: meta.files.len() as u32,
                bytes: meta.files.iter().map(|f| f.size).sum(),
                at_ms: meta.created_ms,
                kind: vrt_host::do_placement_store::SnapKind::Periodic,
            },
        )
        .unwrap();
    plc_a.fence(&scope, PlacementEpoch(1)).unwrap(); // dead@1

    // Cell B: plain class (never gated) — restores the periodic
    // baseline, then the WAL replay fails while `block_wal` is set.
    block_wal.store(true, AtomicOrdering::SeqCst);
    let tenant_dir_b = fx.dir.path().join("do-b").join("t1");
    let mut spec_b = fx.stub_spec("t1", &["w2"], &[]);
    spec_b.do_storage = Some(do_spec(&tenant_dir_b, &[("w2", &key)], &[]));
    let runtime_b = TenantRuntime::new("t1");
    let handle_b =
        vrt_host::supervisor::spawn_supervisor(runtime_b.clone(), spec_b, deps_b, test_params());
    // A couple of acquire/fail/retry windows: never Ready, typed error.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_ne!(
        runtime_b.state(),
        TenantState::Ready,
        "wal replay failure must not serve the baseline alone"
    );
    assert_eq!(
        runtime_b.last_error().as_deref(),
        Some("do-restore-failed"),
        "typed restore-failure surface expected: {:?}",
        runtime_b.last_error()
    );
    // Store heals → the bounded retry converges and the baseline
    // restores — the instance never served the pre-replay image.
    block_wal.store(false, AtomicOrdering::SeqCst);
    wait_state(&runtime_b, TenantState::Ready, Duration::from_secs(20)).await;
    assert!(tenant_dir_b
        .join("w2")
        .join(&key)
        .join("payload.bin")
        .exists());
    handle_b.send(SupervisorCmd::Stop).await;
}
