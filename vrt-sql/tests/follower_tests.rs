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
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::D1;
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
    Follower::new(store, D1, DB, root, clock as Arc<dyn Clock>).unwrap()
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
    let plan = f.promotion_plan().unwrap();
    assert_eq!(plan.expected_epoch, r.head_epoch);
    assert_eq!(plan.expected_lsn, r.head_lsn);
    assert_eq!(plan.next_epoch, r.head_epoch + 1);
    assert_eq!(plan.generation, readiness.generation.unwrap());

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
    assert!(
        path2.exists(),
        "previous generation is retained for in-flight readers"
    );

    write(&e, "INSERT INTO t VALUES (3)").unwrap();
    f.sync_once().unwrap();
    let gen4 = f.state().unwrap().unwrap().generation;
    assert!(root.join(&gen3).exists());
    assert!(root.join(&gen4).exists());
    assert!(
        !path2.exists(),
        "generation older than the retention window is pruned"
    );
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

fn count_engine(e: &vrt_sql::engine::RusqliteEngine) -> i64 {
    let out = e
        .query_batch_sync(
            &[stmt("SELECT count(*) AS count FROM t")],
            &ctx(),
            ResultsFormat::ArrayOfObjects,
        )
        .unwrap();
    out[0].results[0]["count"].as_i64().unwrap()
}

fn promoter(
    store: Arc<dyn ObjectStore>,
    clock: Arc<TestClock>,
    root: PathBuf,
    holder: &str,
) -> Persister {
    Persister::new(
        store,
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: holder.into(),
            checkpoint_every_commits: 0,
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        root,
        D1,
    )
}

#[test]
fn promote_cas_adopts_generation_and_fences_stale_writer() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let old_reg = writer(store.clone(), clock.clone(), tmp.path().join("old"), 0);
    let old = old_reg.get(D1, DB).unwrap();
    write(&old, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&old, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();
    let plan = f.promotion_plan().unwrap();

    let p = promoter(
        store.clone(),
        clock.clone(),
        tmp.path().join("promoted"),
        "promoter",
    );
    let out = f.promote(&p).unwrap();
    assert_eq!(
        (out.previous_epoch, out.previous_lsn, out.epoch, out.lsn),
        (
            plan.expected_epoch,
            plan.expected_lsn,
            plan.next_epoch,
            plan.expected_lsn
        )
    );
    assert!(!out.resumed);
    assert!(
        out.db_file.exists(),
        "writer image adopted from the generation"
    );
    assert_eq!(out.handle.epoch, out.epoch);
    assert_eq!(out.handle.lsn, out.lsn);

    // Hand the lease to the serving writer immediately.
    let mut handle = out.handle.clone();
    p.release(DB, &mut handle, clock.now_ms()).unwrap();

    // The old writer is fenced on its next commit.
    let err = write(&old, "INSERT INTO t VALUES (99)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");
    assert!(old.is_fenced());

    // A fresh writer claims the promoted local image without restoring.
    let new_reg = writer(store.clone(), clock.clone(), tmp.path().join("promoted"), 0);
    let new = new_reg.get(D1, DB).unwrap();
    assert_eq!(new.commit_lsn(), plan.expected_lsn);
    assert_eq!(count_engine(&new), 1);
    write(&new, "INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(count_engine(&new), 2);

    // The follower can tail the promoted writer's next epoch.
    let r = f.sync_once().unwrap();
    assert_eq!(r.to_lsn, plan.expected_lsn + 1);
    assert_eq!(count(&f), 2);
}

#[test]
fn promote_rejects_a_follower_behind_the_head() {
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

    let p = promoter(store, clock, tmp.path().join("promoted"), "promoter");
    let err = f.promote(&p).unwrap_err();
    assert!(matches!(err, FollowerError::NotReady), "{err:?}");
}

#[test]
fn promote_cas_conflict_reports_manifest_change() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();
    let plan = f.promotion_plan().unwrap();
    let generation = f.root().join(&plan.generation);

    // The head advances between the plan and the CAS.
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    let p = promoter(store, clock, tmp.path().join("promoted"), "promoter");
    let err = p
        .promote_from_generation(DB, plan.expected_epoch, plan.expected_lsn, &generation)
        .unwrap_err();
    assert!(
        matches!(err, vrt_sql::persist::PersistError::ManifestChanged { .. }),
        "{err:?}"
    );
    // The CAS never ran: the old writer keeps its lease and can still commit.
    write(&e, "INSERT INTO t VALUES (3)").unwrap();
    assert!(!e.is_fenced());
}

#[test]
fn promote_resumes_local_adoption_after_a_crash_between_cas_and_install() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let old_reg = writer(store.clone(), clock.clone(), tmp.path().join("old"), 0);
    let old = old_reg.get(D1, DB).unwrap();
    write(&old, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&old, "INSERT INTO t VALUES (1)").unwrap();

    let f = follower(store.clone(), clock.clone(), tmp.path().join("replica"));
    f.sync_once().unwrap();
    let plan = f.promotion_plan().unwrap();
    let generation = f.root().join(&plan.generation);
    let p = promoter(
        store.clone(),
        clock.clone(),
        tmp.path().join("promoted"),
        "promoter",
    );
    let first = p
        .promote_from_generation(DB, plan.expected_epoch, plan.expected_lsn, &generation)
        .unwrap();
    assert!(!first.resumed);

    // Simulate the crash window: CAS committed, local adoption vanished.
    std::fs::remove_file(&first.db_file).unwrap();
    let sidecar = first.db_file.parent().unwrap().join(".vrt-lsn");
    let _ = std::fs::remove_file(sidecar);

    let retry = p
        .promote_from_generation(DB, plan.expected_epoch, plan.expected_lsn, &generation)
        .unwrap();
    assert!(retry.resumed);
    assert_eq!(
        retry.epoch, first.epoch,
        "resume must not bump the epoch again"
    );
    assert!(retry.db_file.exists());
    assert_eq!(count(&f), 1);
}

#[tokio::test]
async fn promote_http_cas_requires_service_auth_and_promotes() {
    use std::sync::Arc as StdArc;
    use tower::ServiceExt;

    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = writer(store.clone(), clock.clone(), tmp.path().join("writer"), 0);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    let f = StdArc::new(follower(
        store.clone(),
        clock.clone(),
        tmp.path().join("replica"),
    ));
    f.sync_once().unwrap();
    let p = StdArc::new(promoter(
        store,
        clock,
        tmp.path().join("promoted"),
        "promoter",
    ));
    let auth = vrt_sql::service::ServiceAuth::new(b"test-secret".to_vec());
    let app = vrt_sql::promote_http::router(f.clone(), p, auth.clone());
    let path = format!("/internal/dbs/{DB}/promote");
    let body = b"{}".to_vec();

    let req = axum::http::Request::builder()
        .method("POST")
        .uri(&path)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.clone()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::UNAUTHORIZED);

    let sig = auth.sign("POST", &path, &body, None, None);
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(&path)
        .header("content-type", "application/json")
        .header(vrt_sql::service::AUTH_HEADER, sig)
        .body(axum::body::Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::OK);
    let bytes = http_body_util_body(res).await;
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["epoch"], 2);
    assert_eq!(body["lsn"], 2);
    assert_eq!(body["released"], true);
}

async fn http_body_util_body(res: axum::response::Response) -> Vec<u8> {
    axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap()
        .to_vec()
}
