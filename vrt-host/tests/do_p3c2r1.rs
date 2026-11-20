//! P3c-2-r1: adversarial-review regressions.
//!
//! - P0-1 takeover barrier: `usable()` holds a lease until
//!   `expires + ACQUIRER margin` while the holder kills at
//!   `expires - HOLDER margin` — the 2×margin gap absorbs clock skew
//!   and kill latency. Test drives two instances on clocks skewed by
//!   `margin - 1` and proves takeover lands strictly after the old
//!   holder's kill_due.
//! - P0-3 migration reservation: a fenced+reserved dead-marker
//!   rejects ordinary source respawn (`Reserved`), admits the named
//!   target at epoch+1, auto-releases on expiry, and the explicit
//!   release path restores the source.
//! - P2 defensive validation: SnapRef epoch / manifest_key prefix,
//!   migrate `target_epoch == rec.epoch + 1`, snapshot seq
//!   independent of epoch.
//! - P1-1 trusted path resolution: `DoStorageView::class_dir`
//!   answers only desired-declared (worker, uniqueKey) pairs and
//!   `resolve_scope`/`valid_id` reject `..` and shape violations.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use vrt_host::do_placement::{DoPlacement, PlacementEpoch, PlacementError, PlacementScope};
use vrt_host::do_placement_store::{snap_manifest_key, ManifestDoPlacement, SnapRef};
use vrt_host::do_snapshot::upload_snapshot;
use vrt_host::supervisor::DoStorageView;
use vrt_objstore::{MemoryStore, ObjectStore};

fn scope(dir: PathBuf) -> PlacementScope {
    PlacementScope {
        tenant: "t".into(),
        worker: "w".into(),
        class_key: "a".repeat(64),
        class_dir: dir,
    }
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    // Repository convention: target/tmp-p3c2r1/ (task constraint, cleaned up together at the end).
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp-p3c2r1")
        .join(format!("{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::canonicalize(&dir).unwrap()
}

fn mk_sqlite(dir: &std::path::Path, val: i64) {
    std::fs::create_dir_all(dir).unwrap();
    let db = dir.join("o1.sqlite");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t(v INTEGER);")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (?)", [val]).unwrap();
}

fn clock(cell: &Arc<AtomicU64>) -> Arc<dyn Fn() -> u64 + Send + Sync> {
    let c = cell.clone();
    Arc::new(move || c.load(Ordering::SeqCst))
}

// ---------- P0-1: takeover strictly after the holder's kill_due ----------

/// Two cells, clocks skewed by `margin - 1` (acquirer fast — the
/// worst direction). The acquirer must NOT take over while the
/// holder is still inside its kill window, and MUST be able to once
/// `expires + margin` passes — takeover_real > kill_due_real.
#[test]
fn takeover_waits_expires_plus_margin_under_skew() {
    const LEASE: u64 = 10_000;
    const MARGIN: u64 = 1_000;
    let s: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let t_a = Arc::new(AtomicU64::new(0));
    let t_b = Arc::new(AtomicU64::new(MARGIN - 1)); // b's clock runs fast
    let a = ManifestDoPlacement::new(s.clone(), "cell-a", LEASE, MARGIN).with_clock(clock(&t_a));
    let b = ManifestDoPlacement::new(s, "cell-b", LEASE, MARGIN).with_clock(clock(&t_b));
    let sc = scope(tmpdir("skew").join("cls"));

    // a acquires at t_a=0 → expires = LEASE (a-written wall clock).
    let la = a.acquire(&sc).unwrap();
    assert_eq!(la.expires_at_ms, LEASE);
    let kill_due = LEASE - MARGIN; // holder kills at expires - margin

    // Just before a's kill_due: b (already at t_b = L-2) must still
    // see the lease held — expires+margin > t_b.
    t_a.store(kill_due - 1, Ordering::SeqCst);
    t_b.store(kill_due - 1 + MARGIN - 1, Ordering::SeqCst);
    assert!(
        b.acquire(&sc).is_err(),
        "pre-kill-due takeover = dual-write window"
    );

    // a killed at kill_due; b's clock advanced past expiry but the
    // barrier (expires + margin) still holds — THIS is the window
    // the old `expires - margin` check leaked a takeover into.
    t_a.store(kill_due + 100, Ordering::SeqCst); // a dead (past kill_due)
    t_b.store(LEASE + MARGIN - 1, Ordering::SeqCst);
    assert!(
        b.acquire(&sc).is_err(),
        "expiry+margin-1 must still count as held"
    );

    // Barrier reached: t_b = expires + margin → takeover at E+1.
    t_b.store(LEASE + MARGIN, Ordering::SeqCst);
    let lb = b.acquire(&sc).unwrap();
    assert_eq!(lb.epoch.0, la.epoch.0 + 1);
    // Takeover wall-clock (a-time L+1) is strictly after kill_due.
    let takeover_a_time = LEASE + MARGIN - (MARGIN - 1); // = L+1
    assert!(takeover_a_time > kill_due);
}

// ---------- P0-3: migration reservation ----------

fn mig(
    store: &Arc<dyn ObjectStore>,
    placement: &Arc<ManifestDoPlacement>,
    tmp: &std::path::Path,
) -> vrt_host::do_migration::Migrator {
    vrt_host::do_migration::Migrator {
        store: store.clone(),
        placement: placement.clone(),
        tmp_root: tmp.to_path_buf(),
        stage_timeout: Duration::from_secs(5),
        reserve_ms: 60_000,
        poll: Duration::from_millis(5),
    }
}

/// Source fenced + reserved → its own respawn is `Reserved`; the
/// named target takes E+1. Covers the review's two races: source
/// winning E+1 before commit_snapshot_fenced, and target rejected
/// after commit.
#[test]
fn reservation_blocks_source_admits_target() {
    let tmp = tmpdir("resv");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 7);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let src = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-src",
        60_000,
        5_000,
    ));
    let dst = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-dst",
        60_000,
        5_000,
    ));
    let sc = scope(class_dir);
    let l1 = src.acquire(&sc).unwrap();

    let m = mig(&store, &src, &tmp);
    let fenced = m.fence(&sc, "cell-dst").unwrap();
    assert_eq!(fenced, l1.epoch.0);

    // Source respawn during the reservation → Reserved (the
    // supervisor's retry loop fails closed, never dual-writes).
    match src.acquire(&sc) {
        Err(PlacementError::Reserved { until_ms }) => assert!(until_ms > 0),
        other => panic!("source respawn must be Reserved, got {other:?}"),
    }
    // Snapshot commits on the fenced epoch while source is locked out.
    let snap = m
        .snapshot_commit(
            &sc,
            fenced,
            123,
            vrt_host::do_placement_store::SnapKind::Final,
        )
        .expect("commit on fenced epoch");
    assert_eq!(snap.epoch, fenced);

    // Named target takes E+1 — reservation cleared by the takeover.
    let l2 = dst.acquire(&sc).unwrap();
    assert_eq!(l2.epoch.0, fenced + 1);
    // Post-takeover the reservation is gone — live holder now.
    assert!(src.acquire(&sc).is_err());
}

/// Reservation expiry auto-releases: a crashed coordinator can never
/// wedge the class — bounded outage, source recovers.
#[test]
fn reservation_expiry_lets_source_recover() {
    let tmp = tmpdir("resv-exp");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 7);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let now = Arc::new(AtomicU64::new(1_000_000));
    let src = Arc::new(
        ManifestDoPlacement::new(store.clone(), "cell-src", 60_000, 5_000).with_clock(clock(&now)),
    );
    let sc = scope(class_dir);
    src.acquire(&sc).unwrap();

    let m = vrt_host::do_migration::Migrator {
        store: store.clone(),
        placement: src.clone(),
        tmp_root: tmp.clone(),
        stage_timeout: Duration::from_secs(5),
        reserve_ms: 1_000,
        poll: Duration::from_millis(5),
    };
    let fenced = m.fence(&sc, "cell-dst").unwrap();
    assert!(matches!(
        src.acquire(&sc),
        Err(PlacementError::Reserved { .. })
    ));
    // Advance past reserve_until — source takes E+1 itself.
    now.store(1_000_000 + 1_001, Ordering::SeqCst);
    let l = src.acquire(&sc).unwrap();
    assert_eq!(l.epoch.0, fenced + 1);
}

/// Explicit release: rollback releases the reservation, source
/// re-acquires E+1 and resumes from its untouched local dir.
#[test]
fn rollback_releases_reservation() {
    let tmp = tmpdir("resv-rb");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 7);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let src = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-src",
        60_000,
        5_000,
    ));
    let sc = scope(class_dir);
    src.acquire(&sc).unwrap();
    let m = mig(&store, &src, &tmp);
    let fenced = m.fence(&sc, "cell-dst").unwrap();
    assert!(matches!(
        src.acquire(&sc),
        Err(PlacementError::Reserved { .. })
    ));
    let l = m.rollback_source(&sc, fenced).unwrap();
    assert_eq!(l.epoch.0, fenced + 1);
    // release_reservation is idempotent on a moved-on record.
    src.release_reservation(&sc, fenced).unwrap();
}

/// Self-review (P0-3 follow-through): a plain `fence()` on an
/// already-reserved dead-marker must PRESERVE the reservation —
/// `release_leases` (abort/undeploy/freeze paths) calls plain
/// `fence` and would otherwise strip `reserved_for`, letting the
/// fenced source steal epoch+1 mid-migration.
#[test]
fn plain_fence_preserves_reservation() {
    let tmp = tmpdir("resv-preserve");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 7);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let src = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-src",
        60_000,
        5_000,
    ));
    let sc = scope(class_dir);
    let l1 = src.acquire(&sc).unwrap();
    let m = mig(&store, &src, &tmp);
    let fenced = m.fence(&sc, "cell-dst").unwrap();
    assert_eq!(fenced, l1.epoch.0);
    // Release-path plain fence on the same epoch — must NOT strip
    // the migration reservation.
    src.fence(&sc, PlacementEpoch(fenced)).unwrap();
    match src.acquire(&sc) {
        Err(PlacementError::Reserved { .. }) => {}
        other => panic!("reservation wiped by plain fence: {other:?}"),
    }
}

// ---------- P2: defensive validation ----------

fn wellformed_snap(sc: &PlacementScope, epoch: u64, seq: u64) -> SnapRef {
    SnapRef {
        manifest_key: snap_manifest_key(&sc.class_key, epoch, seq),
        epoch,
        seq,
        files: 1,
        bytes: 10,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    }
}

/// commit_snapshot rejects a SnapRef whose declared epoch differs
/// from the commit epoch — caller bug, not a retryable condition.
#[test]
fn snap_ref_epoch_mismatch_rejected() {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let p = ManifestDoPlacement::new(store, "cell-a", 60_000, 5_000);
    let sc = scope(tmpdir("p2-epoch").join("c"));
    let l = p.acquire(&sc).unwrap();
    let mut snap = wellformed_snap(&sc, l.epoch.0, 1);
    snap.epoch = l.epoch.0 + 1; // lies about its epoch
    assert!(matches!(
        p.commit_snapshot(&sc, l.epoch, snap),
        Err(PlacementError::Precondition(_))
    ));
}

/// commit_snapshot rejects a manifest_key outside the class+epoch
/// snapshot prefix — a cross-epoch/cross-class key can never be
/// referenced.
#[test]
fn snap_ref_manifest_prefix_rejected() {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let p = ManifestDoPlacement::new(store, "cell-a", 60_000, 5_000);
    let sc = scope(tmpdir("p2-key").join("c"));
    let l = p.acquire(&sc).unwrap();
    let mut snap = wellformed_snap(&sc, l.epoch.0, 1);
    snap.manifest_key = format!("do/{}/{}/snap-1", "b".repeat(64), l.epoch.0); // wrong class
    assert!(matches!(
        p.commit_snapshot(&sc, l.epoch, snap.clone()),
        Err(PlacementError::Precondition(_))
    ));
    snap.manifest_key = snap_manifest_key(&sc.class_key, l.epoch.0 + 9, 1); // wrong epoch
    assert!(matches!(
        p.commit_snapshot(&sc, l.epoch, snap),
        Err(PlacementError::Precondition(_))
    ));
}

/// migrate with target_epoch != rec.epoch + 1 → Precondition (409
/// mapping) BEFORE fencing — the live record is untouched.
#[test]
fn migrate_wrong_target_epoch_is_precondition() {
    let tmp = tmpdir("p2-te");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 3);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let src = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-src",
        60_000,
        5_000,
    ));
    let sc = scope(class_dir);
    let l = src.acquire(&sc).unwrap();
    let m = mig(&store, &src, &tmp);
    let err = m
        .migrate(&sc, || false, l.epoch.0 + 5, "cell-dst", || {})
        .expect_err("wrong target_epoch");
    assert!(matches!(
        err,
        vrt_host::do_migration::MigError::Precondition(_)
    ));
    // Record untouched — still live under the source.
    assert_eq!(src.acquire(&sc).unwrap().epoch.0, l.epoch.0);
}

/// Snapshot seq is independent of epoch numbering — caller-supplied
/// monotone seqs on one epoch commit fine; the fenced-commit path
/// takes seq from the caller, never `target_epoch`.
#[test]
fn snapshot_seq_independent_of_epoch() {
    let tmp = tmpdir("p2-seq");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 3);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let p = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-src",
        60_000,
        5_000,
    ));
    let sc = scope(class_dir);
    let l = p.acquire(&sc).unwrap();
    let s1 = upload_snapshot(
        &store,
        &p,
        &sc,
        l.epoch,
        900_001,
        &tmp.join("st"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .unwrap();
    assert_eq!(s1.seq, 900_001);
    let s2 = upload_snapshot(
        &store,
        &p,
        &sc,
        l.epoch,
        900_002,
        &tmp.join("st"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .unwrap();
    assert_eq!(s2.seq, 900_002);
    assert_eq!(p.snapshot_ref(&sc).unwrap().unwrap().seq, 900_002);
}

// ---------- capnp disk-path → workerId (real sample) ----------

/// The emitted capnp declares `localDisk = (path =
/// "<doStorageDir>/<workerId>")` — workerd then creates
/// `<workerId>/<uniqueKey>` beneath it. This is the SAME marker
/// parse `d1_smoke.rs::render_do_cfg` runs on real rendered text,
/// exercised on the production sample from the r1 review so a
/// layout change fails here, not silently in e2e.
#[test]
fn capnp_disk_path_derives_worker_id() {
    // Real gen-2 sample: the emit writes a disk SERVICE entry
    // `(name = ..., disk = (path = "<doStorageDir>/<workerId>", ...))`
    // (capnp.rs:401); the review-observed path follows verbatim.
    let text = r#"    (name = "do-store-w_5f5f391765232dca", disk = (path = "/data/do/local/w_5f5f391765232dca", writable = true)),"#;
    let marker = "disk = (path = \"";
    let i = text.find(marker).expect("no localDisk service in cfg");
    let rest = &text[i + marker.len()..];
    let disk_dir = rest[..rest.find('"').unwrap()].to_string();
    let worker_id = std::path::Path::new(&disk_dir)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(worker_id, "w_5f5f391765232dca");
    // And the class dir under it is workerId/uniqueKey — matching
    // DoStorageView::class_dir's trusted layout.
    let unique = "a".repeat(64);
    let view = DoStorageView {
        tenant_dir: PathBuf::from("/data/do/local"),
        worker_classes: std::collections::BTreeMap::from([(
            worker_id.to_string(),
            vec![unique.clone()],
        )]),
    };
    assert_eq!(
        view.class_dir(worker_id, &unique).unwrap(),
        PathBuf::from(&disk_dir).join(&unique)
    );
}

// ---------- P1-1: trusted class-dir resolution ----------

/// DoStorageView answers `tenant_dir/worker/uniqueKey` ONLY for
/// desired-declared pairs — unknown worker/class is a miss (404),
/// never a constructed path.
#[test]
fn do_storage_view_resolves_only_declared() {
    let tenant_dir = tmpdir("view").join("ten");
    let key = "a".repeat(64);
    let view = DoStorageView {
        tenant_dir: tenant_dir.clone(),
        worker_classes: std::collections::BTreeMap::from([(
            "w_abc".to_string(),
            vec![key.clone()],
        )]),
    };
    assert_eq!(
        view.class_dir("w_abc", &key).unwrap(),
        tenant_dir.join("w_abc").join(&key)
    );
    assert!(view.class_dir("w_other", &key).is_none());
    assert!(view.class_dir("w_abc", &"b".repeat(64)).is_none());
    // Traversal-shaped ids can never resolve — the map has no entry.
    assert!(view.class_dir("..", &key).is_none());
}

/// `resolve_scope` rejects `..`/shape violations (400), unknown
/// tenants (404), and classes absent from desired (404) — the
/// request is a lookup key only.
#[test]
fn resolve_scope_is_lookup_only() {
    use vrt_host::do_migration::admin::{resolve_scope, valid_id, AdminState};
    use vrt_host::supervisor::{DoStorageView, TenantRegistry};

    let tmp = tmpdir("scope");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-a",
        60_000,
        5_000,
    ));
    let registry = TenantRegistry::new();
    let rt = registry.get_or_create("ten-1");
    let key = "a".repeat(64);
    rt.set_do_storage_view(Some(DoStorageView {
        tenant_dir: tmp.join("ten"),
        worker_classes: std::collections::BTreeMap::from([(
            "w_abc".to_string(),
            vec![key.clone()],
        )]),
    }));
    let st = AdminState {
        migrator: Arc::new(vrt_host::do_migration::Migrator {
            store: store.clone(),
            placement: plc,
            tmp_root: tmp.join("mig"),
            stage_timeout: Duration::from_secs(5),
            reserve_ms: 60_000,
            poll: Duration::from_millis(5),
        }),
        registry: registry.clone(),
        seen: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    };

    // Traversal / shape violations → 400.
    assert!(!valid_id(".."));
    assert!(!valid_id("a/b"));
    assert!(!valid_id("a\\b"));
    assert!(!valid_id(""));
    assert!(!valid_id(&"x".repeat(65)));
    assert!(valid_id("w_abc-1"));
    let (code, _) = resolve_scope(&st, "..", "w_abc", &key).unwrap_err();
    assert_eq!(code, axum::http::StatusCode::BAD_REQUEST);
    let (code, _) = resolve_scope(&st, "ten-1", "../etc", &key).unwrap_err();
    assert_eq!(code, axum::http::StatusCode::BAD_REQUEST);
    let (code, _) = resolve_scope(&st, "ten-1", "w_abc", "nothex").unwrap_err();
    assert_eq!(code, axum::http::StatusCode::BAD_REQUEST);

    // Unknown tenant / undeclared class → 404.
    let (code, _) = resolve_scope(&st, "ghost", "w_abc", &key).unwrap_err();
    assert_eq!(code, axum::http::StatusCode::NOT_FOUND);
    let (code, _) = resolve_scope(&st, "ten-1", "w_abc", &"c".repeat(64)).unwrap_err();
    assert_eq!(code, axum::http::StatusCode::NOT_FOUND);

    // Declared triple → trusted dir from the view, not the request.
    let scope = resolve_scope(&st, "ten-1", "w_abc", &key).unwrap();
    assert_eq!(scope.class_dir, tmp.join("ten").join("w_abc").join(&key));
}
