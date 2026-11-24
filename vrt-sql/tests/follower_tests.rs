//! Read-only follower tests: bootstrap from the chain / snapshot, incremental tail, pinned
//! readers, GC-driven re-bootstrap, corruption fail-closed, writer takeover and generation pruning.

use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vrt_objstore::{MemoryStore, ObjectStore};
use vrt_sql::clock::{Clock, TestClock};
use vrt_sql::engine::{EngineError, EngineRegistry, PersisterPair, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::follower::{Follower, FollowerError, FollowerPhase};
use vrt_sql::paths::ResourceKind::D1;
use vrt_sql::paths::queue_root_for;
use vrt_sql::persist::{PersistConfig, Persister};
use vrt_sql::seg;

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

fn write(e: &vrt_sql::engine::RusqliteEngine, sql: &str) -> Result<(), EngineError> {
    e.query_batch_sync(&[stmt(sql)], &ctx(), ResultsFormat::ArrayOfObjects)
        .map(|_| ())
}

fn writer(
    store: Arc<dyn ObjectStore>,
    clock: Arc<TestClock>,
    root: PathBuf,
    checkpoint_every_commits: u64,
) -> EngineRegistry {
    let d1 = Arc::new(Persister::new(
        store.clone(),
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: "writer".into(),
            checkpoint_every_commits,
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        root.clone(),
        D1,
    ));
    let queue = Arc::new(Persister::new(
        d1.store().clone(),
        d1.config().clone(),
        d1.clock().clone(),
        queue_root_for(&root),
        vrt_sql::paths::ResourceKind::Queue,
    ));
    let k2 = Arc::new(Persister::new(
        d1.store().clone(),
        d1.config().clone(),
        d1.clock().clone(),
        vrt_sql::paths::k2_root_for(&root),
        vrt_sql::paths::ResourceKind::K2,
    ));
    EngineRegistry::new(root).with_persisters(PersisterPair { d1, queue, k2 })
}

fn follower(store: Arc<dyn ObjectStore>, clock: Arc<TestClock>, root: PathBuf) -> Follower {
    Follower::new(
        store,
        D1,
        DB,
        root,
        clock as Arc<dyn Clock>,
    )
    .unwrap()
}

fn count_conn(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap()
}

fn count(f: &Follower) -> i64 {
    count_conn(&f.open_readonly().unwrap())
}

#[test]
fn promotion_readiness_rejects_empty_follower() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let f = follower(store, clock, tmp.path().join("follower"));
    let readiness = f.promotion_readiness().unwrap();
    assert!(!readiness.ready);
    assert_eq!(readiness.reason, "no-state");
    assert!(!readiness.generation_exists);
}

#[test]
fn tails_segments_and_pins_a_commit_point() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(
        store.clone(),
        clock.clone(),
        tmp.path().join("writer"),
        0, // no snapshot: bootstrap replays the chain from an empty image
    );
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    let r = f.sync_once().unwrap();
    assert!(r.bootstrapped);
    assert_eq!((r.from_lsn, r.to_lsn), (0, 2));
    assert_eq!(r.segments_applied, 2);
    assert!(r.caught_up());
    assert_eq!(count(&f), 1);
    assert_eq!(r.head_epoch, 1);
    let readiness = f.promotion_readiness().unwrap();
    assert!(readiness.ready, "{readiness:?}");
    assert_eq!(readiness.reason, "ready");
    assert!(readiness.generation_exists);

    // A reader holds one immutable generation: newer commits are invisible until it reopens.
    let pinned = f.open_readonly().unwrap();
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    let r = f.sync_once().unwrap();
    assert!(!r.bootstrapped);
    assert_eq!((r.from_lsn, r.to_lsn), (2, 3));
    assert_eq!(r.segments_applied, 1);
    assert_eq!(count_conn(&pinned), 1);
    drop(pinned);
    assert_eq!(count(&f), 2);

    // Steady state does not write a new generation.
    let before = f.state().unwrap().unwrap().generation;
    let r = f.sync_once().unwrap();
    assert!(!r.progressed());
    assert_eq!(r.phase, FollowerPhase::CaughtUp);
    assert_eq!(f.state().unwrap().unwrap().generation, before);
}

#[test]
fn bootstraps_from_the_latest_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    // Snapshot after every commit: the follower can start from the baseline alone.
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 1);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    let r = f.sync_once().unwrap();
    assert!(r.bootstrapped);
    assert_eq!(r.segments_applied, 0);
    assert_eq!((r.from_lsn, r.to_lsn), (2, 2));
    assert_eq!(count(&f), 1);

    let state = f.state().unwrap().unwrap();
    assert_eq!(state.lsn, 2);
    assert_eq!(state.epoch, 1);
    assert_eq!(state.snapshot_lsn, 2);

    let status = f.status().unwrap();
    assert_eq!(status.phase, FollowerPhase::CaughtUp);
    assert_eq!(status.lag_commits(), 0);
    assert!(status.generation_bytes.unwrap() > 0);
}

#[test]
fn resumes_from_local_generation_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let root = tmp.path().join("replica");
    {
        let f = follower(store.clone(), clock.clone(), root.clone());
        f.sync_once().unwrap();
        assert_eq!(count(&f), 1);
    }

    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    let f = follower(store.clone(), clock.clone(), root);
    let r = f.sync_once().unwrap();
    assert!(!r.bootstrapped);
    assert_eq!(r.segments_applied, 1);
    assert_eq!(count(&f), 2);
}

#[test]
fn gc_past_the_watermark_rebootstraps_from_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 1);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();
    assert_eq!(f.state().unwrap().unwrap().lsn, 2);

    // The writer checkpoints at lsn 3, then the follower's segment is reclaimed (as the writer's
    // GC may do once a snapshot covers it).
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    let key = seg::seg_key(D1, DB, 1, 3);
    vrt_objstore::block_on_store(store.delete(&key)).unwrap();

    let r = f.sync_once().unwrap();
    assert!(r.bootstrapped);
    assert_eq!(r.to_lsn, 3);
    assert_eq!(r.segments_applied, 0);
    assert_eq!(count(&f), 2);
}

#[test]
fn corrupt_segment_fails_closed_without_publishing() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();
    let before = f.state().unwrap().unwrap().generation.clone();

    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    let key = seg::seg_key(D1, DB, 1, 3);
    let mut body = vrt_objstore::block_on_store(store.get(&key))
        .unwrap()
        .unwrap()
        .body;
    let last = body.len() - 1;
    body[last] ^= 0xff;
    vrt_objstore::block_on_store(store.put(&key, &body)).unwrap();

    let err = f.sync_once().unwrap_err();
    assert!(matches!(err, FollowerError::Corrupt(_)), "{err:?}");
    let state = f.state().unwrap().unwrap();
    assert_eq!(state.lsn, 2);
    assert_eq!(state.generation, before);
    assert_eq!(count(&f), 1);
    assert!(f.status().unwrap().last_error.is_some());
}

#[test]
fn follows_a_writer_takeover_across_epochs() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let writer_a = writer(store.clone(), clock.clone(), tmp.path().join("writer-a"), 0);
    let ea = writer_a.get(D1, DB).unwrap();
    write(&ea, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&ea, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();

    // A's lease expires; B takes over at epoch 2, restores the chain, and commits lsn 3.
    clock.advance(20_000);
    let writer_b = writer(store.clone(), clock.clone(), tmp.path().join("writer-b"), 0);
    let eb = writer_b.get(D1, DB).unwrap();
    assert_eq!(eb.lease_epoch(), 2);
    write(&eb, "INSERT INTO t VALUES (2)").unwrap();

    let r = f.sync_once().unwrap();
    assert!(!r.bootstrapped);
    assert_eq!((r.from_lsn, r.to_lsn), (2, 3));
    let state = f.state().unwrap().unwrap();
    assert_eq!(state.epoch, 2);
    assert_eq!(state.lsn, 3);
    assert_eq!(count(&f), 2);
}

#[test]
fn waits_for_an_empty_source_then_bootstraps() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));

    let r = f.sync_once().unwrap();
    assert_eq!(r.phase, FollowerPhase::Empty);
    assert_eq!(r.head_lsn, 0);
    assert!(matches!(f.open_readonly(), Err(FollowerError::NotReady)));

    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let r = f.sync_once().unwrap();
    assert!(r.bootstrapped);
    assert_eq!(count(&f), 1);
}

#[test]
fn prunes_old_generations_and_keeps_the_previous_one() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let root = tmp.path().join("replica");
    let f = follower(store.clone(), clock.clone(), root.clone());
    f.sync_once().unwrap();
    let gen2 = f.state().unwrap().unwrap().generation;
    let path2 = root.join(&gen2);
    assert!(path2.exists());

    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    f.sync_once().unwrap();
    let gen3 = f.state().unwrap().unwrap().generation;
    assert_ne!(gen3, gen2);
    assert!(path2.exists(), "previous generation is retained for in-flight readers");

    write(&e, "INSERT INTO t VALUES (3)").unwrap();
    f.sync_once().unwrap();
    let gen4 = f.state().unwrap().unwrap().generation;
    assert!(root.join(&gen3).exists());
    assert!(root.join(&gen4).exists());
    assert!(!path2.exists(), "generation older than the retention window is pruned");
}

#[test]
fn status_reports_lag_before_the_next_sync() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();
    write(&e, "INSERT INTO t VALUES (2)").unwrap();

    let status = f.status().unwrap();
    assert_eq!(status.phase, FollowerPhase::CatchingUp);
    assert_eq!(status.applied_lsn, 2);
    assert_eq!(status.head_lsn, 3);
    assert_eq!(status.lag_commits(), 1);
}
