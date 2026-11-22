//! Offline Time Travel: timeline indexing, restore by time/bookmark, dry run, writer lock and
//! retention-window GC.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vrt_objstore::{MemoryStore, ObjectStore};
use vrt_sql::bookmark::BookmarkSigner;
use vrt_sql::clock::{Clock, TestClock};
use vrt_sql::engine::{EngineRegistry, PersisterPair, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::D1;
use vrt_sql::persist::{PersistConfig, Persister};
use vrt_sql::seg;
use vrt_sql::time_travel::{restore, TimeTravelError};

const DB: &str = "abcdefghijklmnopqrstuvwxyz";

fn stmt(sql: &str) -> SqlStmt {
    SqlStmt {
        sql: sql.into(),
        params: None,
    }
}

fn ctx() -> TxnCtx {
    TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(30),
        dedup: None,
    }
}

fn write(e: &vrt_sql::engine::RusqliteEngine, sql: &str) {
    e.query_batch_sync(&[stmt(sql)], &ctx(), ResultsFormat::ArrayOfObjects)
        .unwrap();
}

fn cfg(checkpoint_every_commits: u64) -> PersistConfig {
    PersistConfig {
        lease_ms: 10_000,
        clock_skew_margin_ms: 500,
        holder: "writer".into(),
        checkpoint_every_commits,
        retention_ms: 60_000,
        snapshot_max_bytes: 1 << 30,
    }
}

fn persister(
    store: Arc<dyn ObjectStore>,
    clock: Arc<TestClock>,
    root: PathBuf,
    checkpoint_every_commits: u64,
) -> Arc<Persister> {
    Arc::new(Persister::new(
        store,
        cfg(checkpoint_every_commits),
        clock,
        root,
        D1,
    ))
}

fn registry(root: PathBuf, p: Arc<Persister>) -> EngineRegistry {
    let queue = Arc::new(Persister::new(
        p.store().clone(),
        p.config().clone(),
        p.clock().clone(),
        queue_root_for(&root),
        vrt_sql::paths::ResourceKind::Queue,
    ));
    EngineRegistry::new(root).with_persisters(PersisterPair { d1: p, queue })
}

fn row_count(path: &std::path::Path) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap()
}

#[test]
fn timeline_is_flushed_every_sixteen_commits() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let root = tmp.path().join("writer");
    let p = persister(store.clone(), clock.clone(), root.clone(), 0);
    let reg = registry(root, p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)");
    for i in 0..16 {
        clock.advance(10);
        write(&e, &format!("INSERT INTO t VALUES ({i})"));
    }
    let timeline = p.read_timeline(DB).unwrap();
    assert!(
        timeline.len() >= 16,
        "timeline should be flushed independently of checkpoints, got {}",
        timeline.len()
    );
    let first = timeline.oldest().unwrap();
    let last = timeline.latest().unwrap();
    assert!(first.committed_at_ms <= last.committed_at_ms);
    // 17 commits were made; the 17th is still in the unflushed buffer.
    assert_eq!(last.lsn + 1, e.commit_lsn());
}

#[test]
fn restores_to_a_time_then_continues_with_a_new_lsn() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let root = tmp.path().join("writer");
    let p = persister(store.clone(), clock.clone(), root.clone(), 0);
    let reg = registry(root.clone(), p.clone());
    let e = reg.get(D1, DB).unwrap();
    clock.advance(10);
    write(&e, "CREATE TABLE t(v INTEGER)");
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (1)");
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (2)");
    let target = clock.now_ms();
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (3)");
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (4)");
    let head = e.commit_lsn();
    let old_epoch = e.lease_epoch();
    e.release_lease();
    drop(e);
    drop(reg);

    let signer = BookmarkSigner::new([5u8; 32]);
    let outcome = restore(&p, DB, Some(target), None, Some(&signer), false).unwrap();
    assert_eq!(outcome.resolved_lsn, 3, "target resolves before INSERT 3");
    // The first publish writes the post-init empty image as snap-0, so replay starts there.
    assert_eq!(outcome.snapshot_lsn, Some(0));
    assert_eq!(outcome.segments_replayed, 3);
    assert_eq!(outcome.previous_lsn, head);
    assert_eq!(outcome.new_lsn, head + 1);
    assert_eq!(outcome.previous_epoch, old_epoch);
    assert_eq!(
        outcome.previous_bookmark.as_deref(),
        Some(signer.sign(DB, old_epoch, head).as_str())
    );

    let db_file = p.db_file_path(DB);
    assert_eq!(row_count(&db_file), 2, "restored image has INSERT 1 and 2");

    // The next writer claims the published epoch at new_lsn and commits at new_lsn + 1.
    let reg2 = registry(root, p.clone());
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(e2.commit_lsn(), head + 1);
    // Time Travel claimed epoch+1 (published there); restarting the writer takes epoch+2.
    assert_eq!(e2.lease_epoch(), old_epoch + 2);
    write(&e2, "INSERT INTO t VALUES (99)");
    assert_eq!(e2.commit_lsn(), head + 2);
    let conn = rusqlite::Connection::open(&db_file).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3);
    drop(conn);

    // The restored point is a first-class bookmark: restoring it again selects the snapshot
    // published at new_lsn and does not roll anything back.
    e2.release_lease();
    drop(e2);
    drop(reg2);
    let new_bm = outcome.new_bookmark.expect("signer configured");
    let again = restore(&p, DB, None, Some(&new_bm), Some(&signer), false).unwrap();
    assert_eq!(again.resolved_lsn, head + 1);
    assert_eq!(again.new_lsn, head + 3); // head+1 restore, head+2 the later INSERT, +1 for this restore
    assert_eq!(row_count(&p.db_file_path(DB)), 2);
}

#[test]
fn restores_a_signed_bookmark() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let root = tmp.path().join("writer");
    let p = persister(store.clone(), clock.clone(), root.clone(), 0);
    let reg = registry(root, p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)");
    write(&e, "INSERT INTO t VALUES (1)");
    let epoch = e.lease_epoch();
    let lsn = e.commit_lsn();
    let signer = BookmarkSigner::new([6u8; 32]);
    let bm = signer.sign(DB, epoch, lsn);
    write(&e, "INSERT INTO t VALUES (2)");
    e.release_lease();
    drop(e);
    drop(reg);

    let outcome = restore(&p, DB, None, Some(&bm), Some(&signer), false).unwrap();
    assert_eq!(outcome.resolved_lsn, lsn);
    assert_eq!(outcome.resolved_epoch, epoch);
    assert_eq!(row_count(&p.db_file_path(DB)), 1);

    // A forged bookmark is rejected.
    let err = restore(&p, DB, None, Some("0-0-0"), Some(&signer), false).unwrap_err();
    assert!(matches!(err, TimeTravelError::Bookmark));
}

#[test]
fn dry_run_does_not_publish_or_rewrite() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let root = tmp.path().join("writer");
    let p = persister(store.clone(), clock.clone(), root.clone(), 0);
    let reg = registry(root, p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)");
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (1)");
    let target = clock.now_ms();
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (2)");
    let head = e.commit_lsn();
    e.release_lease();
    drop(e);
    drop(reg);

    let before = p.read_manifest_doc(DB).unwrap().unwrap();
    let outcome = restore(&p, DB, Some(target), None, None, true).unwrap();
    assert!(outcome.dry_run);
    assert_eq!(outcome.new_lsn, head);
    let after = p.read_manifest_doc(DB).unwrap().unwrap();
    assert_eq!(after.lsn, before.lsn);
    assert_eq!(after.epoch, before.epoch);
    assert_eq!(row_count(&p.db_file_path(DB)), 2);
}

#[test]
fn refuses_to_run_while_the_writer_lock_is_held() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let root = tmp.path().join("writer");
    let p = persister(store.clone(), clock.clone(), root.clone(), 0);
    let reg = registry(root, p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)");

    let err = restore(&p, DB, Some(clock.now_ms()), None, None, true).unwrap_err();
    assert!(matches!(err, TimeTravelError::LockBusy), "{err:?}");
}

#[test]
fn gc_keeps_the_time_travel_window_and_one_baseline() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let root = tmp.path().join("writer");
    let p = Arc::new(
        Persister::new(
            store.clone() as Arc<dyn ObjectStore>,
            cfg(1),
            clock.clone(),
            root.clone(),
            D1,
        )
        .with_time_travel_retention(500),
    );
    let reg = registry(root, p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)"); // lsn 1, snapshot 1 at t0
    clock.advance(200);
    write(&e, "INSERT INTO t VALUES (1)"); // lsn 2, snapshot 2 at t0+200
    clock.advance(600);
    write(&e, "INSERT INTO t VALUES (2)"); // lsn 3, snapshot 3 at t0+800
    let head = e.commit_lsn();
    e.release_lease();
    drop(e);
    drop(reg);

    let chain = p.read_manifest_doc(DB).unwrap().unwrap().chain;
    p.gc_orphans(DB, head, &chain, head);
    // Window = now-500: keeps snapshot 2 (baseline, older than cutoff) and snapshot 3; deletes 1.
    assert!(store_get(&store, &seg::snap_key(D1, DB, 1, 2)).is_some());
    assert!(store_get(&store, &seg::snap_key(D1, DB, 1, 3)).is_some());
    assert!(store_get(&store, &seg::snap_key(D1, DB, 1, 1)).is_none());
    // Segments above the baseline (lsn 2) remain available for replay.
    assert!(store_get(&store, &seg::seg_key(D1, DB, 1, 3)).is_some());
    assert!(store_get(&store, &seg::seg_key(D1, DB, 1, 2)).is_none());
}

fn store_get(store: &Arc<MemoryStore>, key: &str) -> Option<Vec<u8>> {
    vrt_objstore::block_on_store(store.get(key))
        .unwrap()
        .map(|o| o.body)
}
