//! P3c-2: placement lease fencing, snapshot roundtrip through the
//! shared object store, migration state machine + rollback, and the
//! store-unreachable acquire failure that gates the supervisor's
//! `authoritative()` fail-closed launch refusal — all over
//! `MemoryStore`, which implements the SAME conditional-write
//! semantics as the production fs/s3 clients (etag-CAS, not a
//! weaker stub). The supervisor-side `authoritative()` branch itself
//! is covered by inspection + the trait-level assertions here (the
//! actor's launch path needs a spawned workerd — covered by the
//! standalone e2e `do-snapshot` block).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use vrt_host::do_placement::{DoPlacement, PlacementScope};
use vrt_host::do_placement_store::ManifestDoPlacement;
use vrt_host::do_snapshot::{cleanup_class_objects, restore_snapshot, upload_snapshot};
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
    // 仓库约定：`target/tmp-p3c2/`（不落系统 temp；任务约束）。
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp-p3c2")
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

fn read_sqlite(dir: &std::path::Path) -> i64 {
    let conn = rusqlite::Connection::open(dir.join("o1.sqlite")).unwrap();
    conn.query_row("SELECT v FROM t", [], |r| r.get(0)).unwrap()
}

/// Snapshot → delete local dir → restore → same data, verified
/// integrity. The whole round trip only sees the store API.
#[test]
fn snapshot_upload_restore_roundtrip() {
    let tmp = tmpdir("rt");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 42);

    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(
        store.clone(),
        "cell-a",
        60_000,
        5_000,
    ));
    let sc = scope(class_dir.clone());
    let lease = plc.acquire(&sc).unwrap();
    let snap = upload_snapshot(
        &store,
        &plc,
        &sc,
        lease.epoch,
        1,
        &tmp.join("stage"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .expect("upload");
    assert!(snap.files >= 1);

    std::fs::remove_dir_all(&class_dir).unwrap();
    let r = plc.snapshot_ref(&sc).unwrap().expect("ref committed");
    assert!(restore_snapshot(&store, &r, &sc, false, None).unwrap());
    assert_eq!(read_sqlite(&class_dir), 42);

    // Local dir present → restore is a no-op (never clobbers live data).
    assert!(!restore_snapshot(&store, &r, &sc, false, None).unwrap());
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Corrupted snapshot object → restore fails closed, local data
/// untouched (fail closed at integrity, never halfway).
#[test]
fn corrupted_snapshot_refuses_and_preserves() {
    let tmp = tmpdir("corr");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 7);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(store.clone(), "c", 60_000, 5_000));
    let sc = scope(class_dir.clone());
    let lease = plc.acquire(&sc).unwrap();
    let snap = upload_snapshot(
        &store,
        &plc,
        &sc,
        lease.epoch,
        1,
        &tmp.join("s"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .unwrap();
    std::fs::remove_dir_all(&class_dir).unwrap();

    // Tamper: overwrite a payload object — sha256 must catch it.
    let key = vrt_host::do_placement_store::snap_file_key(
        &sc.class_key,
        snap.epoch,
        snap.seq,
        "o1.sqlite",
    );
    vrt_objstore::block_on_store(store.put(&key, b"tampered")).unwrap();
    let r = plc.snapshot_ref(&sc).unwrap().unwrap();
    assert!(restore_snapshot(&store, &r, &sc, false, None).is_err());
    assert!(!class_dir.join("o1.sqlite").exists());
    let _ = std::fs::remove_dir_all(&tmp);
}

/// RV-R31 #4：本地 `o1.sqlite` 被掉电截成 0 字节（meta 中
/// size/sha 不符）——`allow_replace=false` 也必须走恢复，
/// 绝不能放行空存储直接服务。伴生的 -wal/-shm 一并清掉：
/// 有快照外文件会按"活体目录 local-wins"判定，那是另一条
/// 保护路径（post-snapshot 提交绝不丢）。
#[test]
fn zero_byte_local_sqlite_triggers_restore() {
    let tmp = tmpdir("zb");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 42);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(store.clone(), "c", 60_000, 5_000));
    let sc = scope(class_dir.clone());
    let lease = plc.acquire(&sc).unwrap();
    upload_snapshot(
        &store,
        &plc,
        &sc,
        lease.epoch,
        1,
        &tmp.join("s"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .unwrap();

    // 掉电幽灵：目录还在、主文件被截成 0 字节。
    for e in std::fs::read_dir(&class_dir).unwrap() {
        std::fs::remove_file(e.unwrap().path()).unwrap();
    }
    std::fs::write(class_dir.join("o1.sqlite"), b"").unwrap();

    let r = plc.snapshot_ref(&sc).unwrap().unwrap();
    assert!(
        restore_snapshot(&store, &r, &sc, false, None).unwrap(),
        "0-byte local sqlite must trigger restore, not serve empty"
    );
    assert_eq!(read_sqlite(&class_dir), 42);
    let _ = std::fs::remove_dir_all(&tmp);
}

/// A fenced epoch's snapshot commit is rejected — old-epoch uploads
/// become unreachable orphans; the new holder's restore sees only
/// the committed ref.
#[test]
fn stale_epoch_commit_rejected() {
    let tmp = tmpdir("fence");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 1);
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let a = Arc::new(ManifestDoPlacement::new(store.clone(), "cell-a", 1, 0));
    // Takeover barrier is expires + ACQUIRER margin (P0-1): with
    // margin=0 the acquirer treats the dead 1ms lease as free at
    // expiry — the production margin lives on the HOLDER side.
    let b = Arc::new(ManifestDoPlacement::new(store.clone(), "cell-b", 60_000, 0));
    let sc = scope(class_dir.clone());
    let lease_a = a.acquire(&sc).unwrap();
    // a's 1ms lease expires → b takes epoch 2.
    std::thread::sleep(Duration::from_millis(5));
    let lease_b = b.acquire(&sc).unwrap();
    assert_eq!(lease_b.epoch.0, lease_a.epoch.0 + 1);
    // Old-epoch commit → fenced.
    let r = upload_snapshot(
        &store,
        &a,
        &sc,
        lease_a.epoch,
        9,
        &tmp.join("s"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    );
    assert!(matches!(r, Err(vrt_host::do_snapshot::DoSnapOp::Fenced)));
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Migration: fence → writer dead → snapshot → fenced-commit →
/// target acquires epoch+1 and restores the committed ref; the
/// source's rollback path re-acquires cleanly. Stage failures map
/// to typed errors.
#[test]
fn migration_state_machine_and_rollback() {
    let tmp = tmpdir("mig");
    let class_dir = tmp.join("class");
    mk_sqlite(&class_dir, 99);
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
    let sc = scope(class_dir.clone());
    src.acquire(&sc).unwrap();

    let mig = vrt_host::do_migration::Migrator {
        store: store.clone(),
        placement: src.clone(),
        tmp_root: tmp.join("mig"),
        stage_timeout: Duration::from_secs(5),
        reserve_ms: 60_000,
        poll: Duration::from_millis(5),
    };
    // target_epoch = fenced(1)+1; reservation names the target cell.
    let out = mig
        .migrate(&sc, || false, 2, "cell-dst", || {})
        .expect("migrate");
    assert!(out.snapshot.is_some());

    // Target: epoch+1, manifest carries the ref, restore works.
    let l2 = dst.acquire(&sc).unwrap();
    assert_eq!(l2.epoch.0, out.fenced_epoch + 1);
    std::fs::remove_dir_all(&class_dir).unwrap();
    let r = dst.snapshot_ref(&sc).unwrap().unwrap();
    assert!(restore_snapshot(&store, &r, &sc, false, None).unwrap());
    assert_eq!(read_sqlite(&class_dir), 99);

    // Writer-dead timeout — a live writer blocks the freeze.
    let class_dir2 = tmp.join("c2");
    mk_sqlite(&class_dir2, 5);
    let sc2 = PlacementScope {
        class_key: "b".repeat(64),
        class_dir: class_dir2,
        ..scope(tmp.join("x"))
    };
    src.acquire(&sc2).unwrap();
    let mig2 = vrt_host::do_migration::Migrator {
        store: store.clone(),
        placement: src.clone(),
        tmp_root: tmp.join("mig2"),
        stage_timeout: Duration::from_millis(50),
        reserve_ms: 60_000,
        poll: Duration::from_millis(5),
    };
    let err = mig2
        .migrate(&sc2, || true, 2, "cell-dst", || {})
        .expect_err("live writer must time out");
    assert!(matches!(err, vrt_host::do_migration::MigError::Timeout(_)));
    // Rollback: release the reservation, source re-acquires (epoch+1)
    // and keeps serving.
    assert!(mig2.rollback_source(&sc2, 1).is_ok());
    let _ = std::fs::remove_dir_all(&tmp);
}

/// Cleanup removes only the class's own prefix — a neighbouring
/// uniqueKey's objects are unreachable but never touched.
#[test]
fn cleanup_is_scoped_to_class_prefix() {
    let tmp = tmpdir("cl");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let plc = Arc::new(ManifestDoPlacement::new(store.clone(), "c", 60_000, 5_000));
    let d1 = tmp.join("a");
    mk_sqlite(&d1, 1);
    let sc1 = scope(d1);
    let sc2 = PlacementScope {
        class_key: "f".repeat(64),
        class_dir: tmp.join("b"),
        ..sc1.clone()
    };
    let l1 = plc.acquire(&sc1).unwrap();
    upload_snapshot(
        &store,
        &plc,
        &sc1,
        l1.epoch,
        1,
        &tmp.join("s"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .unwrap();
    mk_sqlite(&sc2.class_dir, 2);
    let l2 = plc.acquire(&sc2).unwrap();
    upload_snapshot(
        &store,
        &plc,
        &sc2,
        l2.epoch,
        1,
        &tmp.join("s"),
        vrt_host::do_placement_store::SnapKind::Periodic,
        None,
    )
    .unwrap();

    let n = cleanup_class_objects(&store, &sc1.class_key, 10_000).unwrap();
    assert!(n.deleted >= 3); // placement + manifest + ≥1 file
    assert!(n.complete);
                     // sc2's placement record still exists.
    assert!(plc.record(&sc2).unwrap().is_some());
    // Malformed keys are refused (never a bare `do/` sweep).
    assert!(cleanup_class_objects(&store, "nope", 10).is_err());
    let _ = std::fs::remove_dir_all(&tmp);
}

/// An object store that fails every call — a configured-but-dead
/// backend. `acquire` MUST error AND `authoritative()` MUST be true:
/// together they are the supervisor's "no lease, no workerd" gate.
#[test]
fn unreachable_store_acquire_fails_closed() {
    struct DeadStore;
    #[async_trait::async_trait]
    impl ObjectStore for DeadStore {
        async fn get(
            &self,
            _k: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
        async fn head(
            &self,
            _k: &str,
        ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
        async fn put_if_none_match(
            &self,
            _k: &str,
            _b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
        async fn put_if_match(
            &self,
            _k: &str,
            _e: &str,
            _b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
        async fn put(&self, _k: &str, _b: &[u8]) -> Result<String, vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
        async fn list(
            &self,
            _p: &str,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
        async fn delete(&self, _k: &str) -> Result<(), vrt_objstore::StoreError> {
            Err(vrt_objstore::StoreError::unavailable(false, "down"))
        }
    }
    let tmp = tmpdir("dead");
    let plc = ManifestDoPlacement::new(Arc::new(DeadStore), "cell-a", 60_000, 5_000);
    assert!(plc.authoritative());
    let err = plc.acquire(&scope(tmp.join("c"))).unwrap_err();
    assert!(err.to_string().contains("down"), "{err}");
    // And LocalPlacement is NEVER authoritative — the two impls are
    // distinct config choices, not a fallback chain.
    assert!(!vrt_host::do_placement::LocalPlacement.authoritative());
}
