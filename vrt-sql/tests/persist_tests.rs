//! T36 adversarial tests: lease seizure / upload failure / kill -9 recovery / segment corruption /
//! fs CAS concurrency / lease boundary / adopt upgrade / drop tombstone / GC.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vrt_objstore::{FsStore, MemoryStore, ObjectStore, StoreError};
use vrt_sql::clock::{Clock, SystemClock, TestClock};
use vrt_sql::engine::{EngineError, EngineRegistry, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::{self, D1};
use vrt_sql::persist::{Manifest, PersistConfig, Persister};
use vrt_sql::seg;

const DB: &str = "abcdefghijklmnopqrstuvwxyz"; // 26 lowercase letters, a legal id

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

fn read_i64(e: &vrt_sql::engine::RusqliteEngine, sql: &str) -> i64 {
    let out = e
        .query_batch_sync(&[stmt(sql)], &ctx(), ResultsFormat::ArrayOfObjects)
        .unwrap();
    out[0].results[0]["count"].as_i64().unwrap()
}

fn persister(
    store: Arc<dyn ObjectStore>,
    holder: &str,
    clock: Arc<dyn Clock>,
    db_root: PathBuf,
) -> Arc<Persister> {
    Arc::new(Persister::new(
        store,
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: holder.into(),
            checkpoint_every_commits: 0, // off by default in tests, enable on demand
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        db_root,
        D1,
    ))
}

/// T37r: the registry needs a Persister for each of the two kinds -- the queue side shares
/// store/cfg/clock with D1, and root is the `queues/` sibling of `dbs`.
fn pair(root: &std::path::Path, d1: Arc<Persister>) -> vrt_sql::engine::PersisterPair {
    vrt_sql::engine::PersisterPair {
        d1: d1.clone(),
        queue: Arc::new(Persister::new(
            d1.store().clone(),
            d1.config().clone(),
            d1.clock().clone(),
            queue_root_for(root),
            ResourceKind::Queue,
        )),
    }
}

fn registry(root: PathBuf, p: Arc<Persister>) -> EngineRegistry {
    EngineRegistry::new(root.clone()).with_persisters(pair(&root, p))
}

/// ---------- section 4.1 lease seizure: A expires -> B takes over at epoch+1 -> A fenced ----------

#[test]
fn takeover_fences_expired_writer() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));

    let pa = persister(store.clone(), "A", clock.clone(), tmp.path().join("a"));
    let pb = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg_a = registry(tmp.path().join("a"), pa);
    let reg_b = registry(tmp.path().join("b"), pb);

    let ea = reg_a.get(D1, DB).unwrap();
    write(&ea, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&ea, "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(ea.lease_epoch(), 1);
    assert_eq!(ea.commit_lsn(), 2);

    // A's writer lease expires -> B takes over at epoch+1.
    clock.advance(20_000);
    let eb = reg_b.get(D1, DB).unwrap();
    assert_eq!(eb.lease_epoch(), 2);
    assert_eq!(eb.commit_lsn(), 2); // lsn does not go backwards after takeover
    assert_eq!(read_i64(&eb, "SELECT count(*) AS count FROM t"), 1);

    // A writes again -> Fenced (lease lost).
    let err = write(&ea, "INSERT INTO t VALUES (99)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");
    // A's reads are rejected as well (section 4.3: an unusable lease always rejects).
    assert!(ea
        .query_batch_sync(&[stmt("SELECT 1")], &ctx(), ResultsFormat::ArrayOfObjects)
        .is_err());

    // A's orphan segment (hand-planted in its old epoch key space) does not enter the manifest.
    let orphan = seg::Segment {
        database_id: DB.into(),
        epoch: 1,
        lsn: 3,
        prev_lsn: 2,
        page_size: 4096,
        wal_begin: 0,
        frames: vec![],
    };
    let orphan_key = seg::seg_key(D1, DB, 1, 3);
    vrt_objstore::block_on_store(store.put(&orphan_key, &seg::encode(&orphan))).unwrap();

    // What B reads does not include A's expired write (A's 99 never entered the manifest chain).
    assert_eq!(read_i64(&eb, "SELECT count(*) AS count FROM t"), 1);

    // B keeps writing and advances the manifest; orphan segments are GC'd/ignored on restore.
    write(&eb, "INSERT INTO t VALUES (2)").unwrap();
    let eb2 = reg_b.get(D1, DB).unwrap();
    assert_eq!(eb2.commit_lsn(), 3);
}

/// ---------- section 4.2 upload failure: no ack, no committed-but-unpersisted data left behind ----------

#[test]
fn publish_failure_no_phantom_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);

    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();

    // Inject one put failure -> the write returns an error (no ack).
    mem.inject_put_failures(vec![StoreError::unavailable(true, "injected")]);
    let err = write(&e, "INSERT INTO t VALUES (2)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");

    // Engine fenced -> release the Arc (returns the flock) -> evict -> claim again
    // (the lease is still ours -> restore).
    let epoch = e.lease_epoch();
    drop(e);
    reg.evict_fenced(D1, DB, epoch);
    let e2 = reg.get(D1, DB).unwrap();
    // The un-acked INSERT (2) was cleared by the restore -- locally there is never a ghost commit.
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 1);
}

/// ---------- kill -9: everything acked is present, nothing un-acked is ----------

/// Child process entry (the parent test re-enters with the current test binary + env).
/// Running `cargo test kill9_child_worker` directly returns immediately.
#[test]
fn kill9_child_worker() {
    if std::env::var("VRT_T36_KILL_CHILD").is_err() {
        return;
    }
    let root = PathBuf::from(std::env::var("VRT_T36_ROOT").unwrap());
    let store_root = PathBuf::from(std::env::var("VRT_T36_STORE").unwrap());
    let progress = PathBuf::from(std::env::var("VRT_T36_PROGRESS").unwrap());
    // `[VRT_T36_START, VRT_T36_N]`: a repeated-kill run resumes the same
    // table at the persisted prefix, so the child re-enters instead of
    // recreating it (the FK/PK keeps a replayed insert a hard failure).
    let start: u64 = std::env::var("VRT_T36_START").unwrap().parse().unwrap();
    let n: u64 = std::env::var("VRT_T36_N").unwrap().parse().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(FsStore::new(store_root).unwrap());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let p = Arc::new(Persister::new(
        store,
        PersistConfig {
            lease_ms: 120_000,
            clock_skew_margin_ms: 500,
            holder: "killchild".into(),
            checkpoint_every_commits: 0,
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        root.clone(),
        D1,
    ));
    let reg = EngineRegistry::new(root.clone()).with_persisters(pair(&root, p));
    let e = reg.get(D1, DB).unwrap();
    // A resumed cycle finds the table from the previous one.
    write(&e, "CREATE TABLE IF NOT EXISTS t(k INTEGER PRIMARY KEY)").unwrap();
    for k in start..=n {
        write(&e, &format!("INSERT INTO t VALUES ({k})")).unwrap();
        // progress is recorded only after the ack -- the parent asserts by the ack count.
        std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&progress)
            .map(|mut f| {
                use std::io::Write;
                let _ = writeln!(f, "{k}");
            })
            .ok();
    }
}

/// Force the manifest lease to expire so the next "process" (the next child
/// or the takeover) claims at epoch+1. With SystemClock, waiting out the
/// lease (60-120 s) is impractical, so rewrite the expiry -- the equivalent
/// of advancing the clock past it.
fn expire_manifest(store: &Arc<dyn ObjectStore>) -> Manifest {
    let mkey = seg::manifest_key(D1, DB);
    let obj = vrt_objstore::block_on_store(store.get(&mkey))
        .unwrap()
        .expect("manifest exists");
    let mut m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    m.expires_at_ms = 0;
    let body = serde_json::to_vec(&m).unwrap();
    vrt_objstore::block_on_store(store.put_if_match(&mkey, &obj.meta.etag, &body)).unwrap();
    m
}

/// Hard-kill fuzz over one contiguous `1..=n_total` insert stream. Rounds
/// insert a `n_total / slices`-row slice in a child, SIGKILL it once at least
/// half the slice is acked (a short tail runs to completion instead), take the
/// database over at a newer lease epoch, and assert that every acked
/// transaction survived, nothing un-acked leaked in, and the persisted set is
/// still a contiguous prefix. Rounds repeat until `n_total` rows are
/// persisted, so a mid-slice kill does not shrink the budget: the rest of the
/// slice is retried by the next round. Kill timing therefore repeats against a
/// growing manifest chain, which the single-kill default does not cover.
fn kill9_fuzz(n_total: u64, slices: u32) {
    if std::env::var("VRT_T36_KILL_CHILD").is_ok() {
        return; // child-process branch
    }
    assert!(slices >= 1, "at least one kill/recovery slice");
    assert!(
        n_total >= u64::from(slices),
        "every slice must be able to ack at least one row"
    );
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbroot");
    let store_root = tmp.path().join("store");
    std::fs::create_dir_all(&root).unwrap();

    let exe = std::env::current_exe().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(FsStore::new(store_root.clone()).unwrap());
    let per_cycle = n_total.div_ceil(u64::from(slices));
    // Each slice needs at most a kill round (half the slice) plus a
    // completion round (the rest); anything beyond that means a round made no
    // progress (e.g. its deadline expired) and the run must fail loudly
    // instead of passing with less coverage than advertised.
    let max_rounds = 2 * u64::from(slices) + 2;
    let mut persisted = 0u64;
    let mut killed = 0u32;
    let mut round = 0u64;
    while persisted < n_total {
        assert!(
            round < max_rounds,
            "kill -9 rounds stopped making progress: {persisted}/{n_total} rows after {round} rounds"
        );
        let first = persisted + 1;
        let last = (first + per_cycle - 1).min(n_total);
        // Round 0 starts on an empty store (no manifest to hand over); every
        // later round must expire the previous takeover's lease first, or the
        // child cannot claim.
        if round > 0 {
            expire_manifest(&store);
        }
        let progress = tmp.path().join(format!("progress-{round}"));
        // Keep the child's output: a child that cannot claim (lease, restore)
        // must fail the round with its stderr, never look like a short round.
        let child_log = tmp.path().join(format!("child-{round}.log"));
        let log_fd = std::fs::File::create(&child_log).unwrap();
        let mut child = std::process::Command::new(&exe)
            .args([
                "kill9_child_worker",
                "--exact",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env("VRT_T36_KILL_CHILD", "1")
            .env("VRT_T36_ROOT", &root)
            .env("VRT_T36_STORE", &store_root)
            .env("VRT_T36_PROGRESS", &progress)
            .env("VRT_T36_START", first.to_string())
            .env("VRT_T36_N", last.to_string())
            .stdout(std::process::Stdio::from(log_fd.try_clone().unwrap()))
            .stderr(std::process::Stdio::from(log_fd))
            .spawn()
            .unwrap();

        // Kill only when the round has more than half a slice left to write.
        // A short tail runs to completion instead of being killed at a
        // fraction of itself -- otherwise each round would halve the remaining
        // gap and the requested budget would never be reached.
        let remaining = n_total - persisted;
        let target = if remaining > per_cycle / 2 {
            ((last - first + 1) / 2).max(1)
        } else {
            u64::MAX // let the child finish the tail
        };
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut acked = 0u64;
        loop {
            if let Ok(raw) = std::fs::read_to_string(&progress) {
                acked = raw.lines().count() as u64;
            }
            let alive = child.try_wait().unwrap().is_none();
            if acked >= target || !alive || Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let mut killed_now = false;
        if child.try_wait().unwrap().is_none() {
            child.kill().unwrap(); // SIGKILL -- equivalent to kill -9.
            killed += 1;
            killed_now = true;
        }
        let status = child.wait().unwrap();
        if let Ok(raw) = std::fs::read_to_string(&progress) {
            acked = raw.lines().count() as u64;
        }
        let child_tail = std::fs::read_to_string(&child_log)
            .unwrap_or_default()
            .lines()
            .rev()
            .take(20)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            killed_now || status.success(),
            "round {round}: child exited with {status} before the kill:\n{child_tail}"
        );
        eprintln!(
            "round {round}: rows {persisted}..{last}, acked {acked}, killed {killed_now}"
        );

        // A new "process" (new registry + new holder) takes over.
        let m = expire_manifest(&store);
        let rows = {
            let p = Arc::new(Persister::new(
                store.clone(),
                PersistConfig {
                    lease_ms: 60_000,
                    clock_skew_margin_ms: 500,
                    holder: format!("recovery-{round}"),
                    checkpoint_every_commits: 0,
                    retention_ms: 60_000,
                    snapshot_max_bytes: 1 << 30,
                },
                Arc::new(SystemClock),
                root.clone(),
                D1,
            ));
            let reg = EngineRegistry::new(root.clone()).with_persisters(pair(&root, p));
            let e = reg.get(D1, DB).unwrap();
            assert_eq!(e.lease_epoch(), m.epoch + 1, "round {round}");
            assert_eq!(e.commit_lsn(), m.lsn, "round {round}");
            // Every acked k is present; no un-acked one is.
            let n_rows = read_i64(&e, "SELECT count(*) AS count FROM t");
            assert_eq!(
                n_rows,
                m.lsn as i64 - 1,
                "round {round}: lsn covers DDL+inserts; manifest.lsn={} rows={n_rows}",
                m.lsn
            );
            // progress is appended only after the ack; SIGKILL may land in the
            // gap between "after ack, before append" -> rows in
            // {acked, acked+1}, and never < acked (a lost ack).
            let acked_total = persisted + acked;
            assert!(
                n_rows as u64 >= acked_total,
                "round {round}: acked={acked_total} rows={n_rows} -- all acked transactions must be present"
            );
            assert!(
                n_rows as u64 <= acked_total + 1,
                "round {round}: acked={acked_total} rows={n_rows} -- at most one ack/append gap off"
            );
            let max_k = read_i64(&e, "SELECT COALESCE(MAX(k),0) AS count FROM t");
            assert_eq!(
                max_k, n_rows,
                "round {round}: persisted set must be a contiguous prefix"
            );
            n_rows as u64
        };
        assert!(
            rows >= first,
            "round {round}: the round must persist progress ({persisted} -> {rows}):\n{child_tail}"
        );
        persisted = rows;
        round += 1;
    }
    assert!(
        killed >= 1,
        "a kill -9 round must actually SIGKILL a live child"
    );
    assert_eq!(
        persisted, n_total,
        "the requested transaction budget must be fully persisted"
    );
}

#[test]
fn kill9_recovers_acked_and_drops_unacked() {
    // The regular gate: one kill, small default so the workspace suite stays fast.
    kill9_fuzz(1_000, 1);
}

/// Repeated kill/recovery rounds. `#[ignore]`d: it costs minutes, so the CI
/// job names it explicitly (`--ignored`) and the workspace suite does not.
#[test]
#[ignore = "slow: repeated kill -9 rounds; run with --ignored"]
fn kill9_fuzz_repeated() {
    kill9_fuzz(5_000, 5);
}

/// Manual soak: 1e5 durable transactions over 5 kill/recovery rounds
/// (roughly 2-3 h with FsStore -- a release-branch soak, never a PR gate).
#[test]
#[ignore = "soak: 100k-op kill -9 fuzz; run with --ignored"]
fn kill9_fuzz_100k() {
    kill9_fuzz(100_000, 5);
}

/// ---------- segment corruption / sha mismatch / seq gap -> restore fails closed ----------

fn build_persisted_db(store: Arc<dyn ObjectStore>, tmp: &tempfile::TempDir, clock: Arc<dyn Clock>) {
    let p = persister(store.clone(), "A", clock, tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
}

fn corrupt_store_claim(
    store: Arc<dyn ObjectStore>,
    tmp: &tempfile::TempDir,
    clock: Arc<dyn Clock>,
) -> EngineError {
    let p = persister(store, "B", clock, tmp.path().join("r2"));
    let reg = registry(tmp.path().join("r2"), p);
    reg.get(D1, DB).err().unwrap()
}

#[test]
fn corrupt_segment_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    build_persisted_db(store.clone(), &tmp, clock.clone());
    // Before takeover, corrupt the lsn=2 segment (change bytes -> sha256 mismatch).
    let key = seg::seg_key(D1, DB, 1, 2);
    let mut body = vrt_objstore::block_on_store(store.get(&key))
        .unwrap()
        .unwrap()
        .body;
    body[40] ^= 0xff;
    mem.corrupt(&key, body);
    clock.advance(20_000);
    let err = corrupt_store_claim(store, &tmp, clock);
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
}

#[test]
fn sequence_gap_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    build_persisted_db(store.clone(), &tmp, clock.clone());
    // Create a gap: delete the lsn=2 segment (manifest.lsn=3).
    vrt_objstore::block_on_store(store.delete(&seg::seg_key(D1, DB, 1, 2))).unwrap();
    clock.advance(20_000);
    let err = corrupt_store_claim(store, &tmp, clock);
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
}

#[test]
fn bad_manifest_json_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    mem.corrupt(&seg::manifest_key(D1, DB), b"not json".to_vec());
    let err = corrupt_store_claim(store, &tmp, clock);
    assert!(matches!(err, EngineError::Message(_)), "{err:?}");
}

/// ---------- fs store CAS concurrency: many threads put_if_match at once, exactly one wins ----------

#[test]
fn fs_cas_exactly_one_winner() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStore::new(tmp.path().to_path_buf()).unwrap());
    // Seed the object first to obtain an etag.
    let etag = vrt_objstore::block_on_store(store.put("k", b"v0")).unwrap();
    let winners = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..16 {
        let (s, e, w) = (store.clone(), etag.clone(), winners.clone());
        handles.push(std::thread::spawn(move || {
            // write distinct values: a CAS with the same content as v0 is a legal no-op (the etag does not change,
            // and others' preconditions are not invalidated) -- only mutually distinct values can assert "exactly one winner".
            let body = format!("w{i}").into_bytes();
            if vrt_objstore::block_on_store(s.put_if_match("k", &e, &body)).is_ok() {
                w.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(winners.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn fs_put_if_none_match_race() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStore::new(tmp.path().to_path_buf()).unwrap());
    let winners = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..16 {
        let (s, w) = (store.clone(), winners.clone());
        handles.push(std::thread::spawn(move || {
            let body = format!("w{i}").into_bytes();
            if vrt_objstore::block_on_store(s.put_if_none_match("k", &body)).is_ok() {
                w.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(winners.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// ---------- lease boundary: expiry-margin edge / renewal failure -> fenced / recovery ----------

#[test]
fn lease_boundary_and_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    // lease_ms=10_000 margin=500: usable iff now < expires-500.
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    // claim at now=0: expires=10_000. The usable boundary is now<9_500.
    clock.set(9_499);
    write(&e, "INSERT INTO t VALUES (1)").unwrap(); // usable inside the boundary
    clock.set(9_500);
    // now == expires-margin -> unusable -> renew -> succeeds (lease extended to 19_500).
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    assert!(!e.is_fenced());

    // Note: the write at 9_499 already triggered due_renew (lead time lease/4, threshold 7000),
    // the lease was actually renewed to expires=19_499 -> usable boundary now<18_999.
    // Renewal failure injection: usable and due_renew -> degraded, the write still succeeds.
    clock.set(18_000); // 16_499 ≤ 18_000 < 18_999
    mem.inject_put_failures(vec![StoreError::unavailable(true, "renewfail")]);
    write(&e, "INSERT INTO t VALUES (3)").unwrap(); // a degraded write still succeeds (CAS succeeded)
    assert!(e.is_degraded());
    // Advance to the unusable boundary: renewal still fails -> fenced.
    mem.inject_put_failures(vec![
        StoreError::unavailable(true, "renewfail"),
        StoreError::unavailable(true, "renewfail"),
        StoreError::unavailable(true, "renewfail"),
    ]);
    clock.set(18_999);
    let err = write(&e, "INSERT INTO t VALUES (4)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");

    // Recovery: fenced -> evict -> the lease is obtainable again (we are the holder) -> self-heal.
    // First clear leftover injections (the CAS of the recover claim must not be polluted again), and return
    // the old engine's Arc (the lock can only be taken on reopen after the flock is released).
    mem.inject_put_failures(vec![]);
    let old_epoch = e.lease_epoch();
    drop(e);
    reg.evict_fenced(D1, DB, old_epoch);
    let e2 = reg.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 3);
}

/// ---------- adopt: migrating in an old-version local db with no manifest ----------

#[test]
fn adopt_pre_manifest_local_db() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    // Old-version write: a registry without persistence.
    {
        let reg = EngineRegistry::new(root.clone());
        let e = reg.get(D1, DB).unwrap();
        write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
        write(&e, "INSERT INTO t VALUES (42)").unwrap();
    }
    // T36 startup: attach persistence on the same root -> adopt uploads the snapshot + manifest epoch=1.
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    assert_eq!(e.lease_epoch(), 1);
    assert_eq!(read_i64(&e, "SELECT v AS count FROM t"), 42);
    // The manifest has been created and carries a snapshot baseline.
    let obj = vrt_objstore::block_on_store(store.get(&seg::manifest_key(D1, DB)))
        .unwrap()
        .unwrap();
    let m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    assert!(m.snapshot.is_some());
    assert_eq!(m.epoch, 1);
    // After another writer takes over it can also read the adopted data.
    clock.advance(20_000);
    let p2 = persister(store, "B", clock, tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT v AS count FROM t"), 42);
}

/// ---------- startup self-check: read-only object root -> self_check must fail (P0-B reproduction) ----------

/// Reproduces the in-container `vrt-sql-objects` volume ownership error (root:root 0755 mounted to
/// uid 10001): the write probe gets EACCES -> self_check Err -> startup fails closed.
#[cfg(unix)]
#[test]
fn self_check_fails_on_readonly_store_root() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("ro-store");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555)).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(FsStore::new(root.clone()).unwrap());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store, "A", clock, tmp.path().join("db"));
    let err = p.self_check().err().unwrap();
    assert!(
        err.to_string().contains("unavailable") || err.to_string().contains("Permission"),
        "unexpected: {err}"
    );
    // Restore permissions so the tempdir can be cleaned up.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// ---------- observable failure path: claim failure is counted in persistence_failures_total ----------

fn counter_val(metric: &str, reason: &str) -> f64 {
    let text = vrt_sql::metrics::registry().render();
    let pat = format!("{metric}{{reason=\"{reason}\"}}");
    text.lines()
        .find(|l| l.starts_with(&pat))
        .and_then(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .unwrap_or(0.0)
}

#[test]
fn claim_failure_increments_persistence_failures_metric() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let pa = persister(store.clone(), "A", clock.clone(), tmp.path().join("a"));
    let reg_a = registry(tmp.path().join("a"), pa);
    let before = counter_val("vrt_sql_persistence_failures_total", "claim");
    store.inject_get_failures(vec![StoreError::unavailable(true, "injected")]);
    let err = reg_a.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    let after = counter_val("vrt_sql_persistence_failures_total", "claim");
    assert!(
        after > before,
        "claim failure must count: before={before} after={after}"
    );
}

/// ---------- claim during someone else's validity -> HeldByOther (503, no seizure) ----------

#[test]
fn held_by_other_lease_not_stolen() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let pa = persister(store.clone(), "A", clock.clone(), tmp.path().join("a"));
    let pb = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg_a = registry(tmp.path().join("a"), pa);
    let reg_b = registry(tmp.path().join("b"), pb);
    let ea = reg_a.get(D1, DB).unwrap();
    write(&ea, "CREATE TABLE t(v INTEGER)").unwrap();
    // A's lease is still valid (now=1000 << 10_000-margin) -> B must not seize it.
    clock.set(1_000);
    let err = reg_b.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // A is unaffected after B fails.
    write(&ea, "INSERT INTO t VALUES (1)").unwrap();
}

/// ---------- graceful-shutdown lease release (T36b) ----------

#[test]
fn release_lease_lets_next_holder_claim_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let pa = persister(store.clone(), "A", clock.clone(), tmp.path().join("a"));
    let pb = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg_a = registry(tmp.path().join("a"), pa);
    let reg_b = registry(tmp.path().join("b"), pb);
    let ea = reg_a.get(D1, DB).unwrap();
    write(&ea, "CREATE TABLE t(v INTEGER)").unwrap();
    // A releases (the lease is far from expiry -- now=1000, lease 15000).
    clock.set(1_000);
    reg_a.begin_shutdown();
    reg_a.release_leases(std::time::Instant::now() + std::time::Duration::from_secs(5));
    // B takes over at epoch+1 without waiting.
    let eb = reg_b.get(D1, DB).unwrap();
    assert_eq!(eb.lease_epoch(), 2);
    write(&eb, "INSERT INTO t VALUES (1)").unwrap();
    // After release, A's engine writes again -> Fenced.
    let err = write(&ea, "INSERT INTO t VALUES (9)").err().unwrap();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");
}

#[test]
fn release_lease_with_store_down_does_not_hang() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(0));
    let pa = persister(store, "A", clock.clone(), tmp.path().join("a"));
    let reg_a = registry(tmp.path().join("a"), pa);
    let _ea = reg_a.get(D1, DB).unwrap();
    mem.inject_get_failures(vec![
        StoreError::unavailable(false, "hard down"),
        StoreError::unavailable(false, "hard down"),
        StoreError::unavailable(false, "hard down"),
    ]);
    mem.inject_put_failures(vec![
        StoreError::unavailable(false, "hard down"),
        StoreError::unavailable(false, "hard down"),
        StoreError::unavailable(false, "hard down"),
    ]);
    reg_a.begin_shutdown();
    let t0 = std::time::Instant::now();
    reg_a.release_leases(std::time::Instant::now() + std::time::Duration::from_secs(5));
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(4),
        "release hung"
    );
    // After closing, get() is always Unavailable (the same process must not re-seize).
    let err = reg_a.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
}

/// ---------- snapshot over cap: stat short-circuit + exponential backoff (T36b) ----------

#[test]
fn snapshot_over_cap_backs_off_and_reports() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    // snapshot_max_bytes=1: the db is always over the cap; every=2.
    let p = Arc::new(Persister::new(
        store,
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: "A".into(),
            checkpoint_every_commits: 2,
            retention_ms: 60_000,
            // RV-R31: the cap must sit between "the lsn0 baseline (~20KB init image, which
            // must upload successfully with the first segment)" and "the bloated db" -- after lsn2 inserts
            // a 60KB row, db>cap, and every checkpoint snapshot after that is over the cap.
            snapshot_max_bytes: 48 * 1024,
        },
        clock,
        tmp.path().join("db"),
        D1,
    ));
    let reg = registry(tmp.path().join("db"), p);
    let e = reg.get(D1, DB).unwrap();
    let cap = || counter_val("vrt_sql_persistence_failures_total", "snapshot_over_cap");
    let c0 = cap();
    // The 2nd commit triggers the first checkpoint -> over the cap -> gap 2->4, count+1.
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (zeroblob(60000))").unwrap();
    assert_eq!(cap(), c0 + 1.0, "first over-cap attempt counted");
    // After reset 4 commits are needed to reach gap=4: the 3rd still does not attempt, the 4th attempts -> +1.
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    write(&e, "INSERT INTO t VALUES (3)").unwrap();
    write(&e, "INSERT INTO t VALUES (4)").unwrap();
    assert_eq!(cap(), c0 + 1.0, "backed off at c<4");
    write(&e, "INSERT INTO t VALUES (5)").unwrap();
    assert_eq!(cap(), c0 + 2.0, "second attempt at gap boundary");
    // On the second failure gap 4->8: the next 5 commits (c<8) do not attempt.
    for i in 6..11 {
        write(&e, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    assert_eq!(cap(), c0 + 2.0, "gap doubled to 8 — no further attempts");
    // Diagnostics expose snapshotOverCap + chainLen.
    let d = reg
        .diagnostics()
        .into_iter()
        .find(|d| d.database_id == DB)
        .unwrap();
    assert!(d.snapshot_over_cap, "diag must expose over-cap: {d:?}");
    assert!(d.chain_len >= 6, "chain_len grows: {d:?}");
}

/// ---------- drop tombstone: manifest CAS fences first, then local cleanup ----------

#[test]
fn drop_tombstone_fences_all_writers() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();

    assert!(reg.drop_db(D1, DB).unwrap());
    // The manifest tombstone has landed.
    let obj = vrt_objstore::block_on_store(store.get(&seg::manifest_key(D1, DB)))
        .unwrap()
        .unwrap();
    let m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    assert!(m.tombstone);
    // get again in the same process -> Dropped; a new registry -> also Dropped (via the manifest).
    assert!(matches!(reg.get(D1, DB), Err(EngineError::Dropped)));
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    assert!(matches!(reg2.get(D1, DB), Err(EngineError::Dropped)));
    // Idempotent.
    assert!(!reg.drop_db(D1, DB).unwrap());
}

/// ---------- persistence layer unavailable: startup/cold open fails closed ----------

#[test]
fn unavailable_store_fails_closed_on_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(0));
    mem.inject_get_failures(vec![
        StoreError::unavailable(true, "down"),
        StoreError::unavailable(true, "down"),
        StoreError::unavailable(true, "down"),
        StoreError::unavailable(true, "down"),
    ]);
    let p = persister(store, "A", clock, tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);
    let err = reg.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
}

/// ---------- seg encode/decode roundtrip + format gate ----------

#[test]
fn seg_encode_decode_roundtrip() {
    let s = seg::Segment {
        database_id: DB.into(),
        epoch: 7,
        lsn: 42,
        prev_lsn: 41,
        page_size: 4096,
        wal_begin: 10,
        frames: vec![
            seg::SegFrame {
                page_no: 1,
                commit_db_size: 0,
                data: vec![0xAA; 4096],
            },
            seg::SegFrame {
                page_no: 3,
                commit_db_size: 5,
                data: vec![0xBB; 4096],
            },
        ],
    };
    let bytes = seg::encode(&s);
    let d = seg::decode(&bytes).unwrap();
    assert_eq!(d.epoch, 7);
    assert_eq!(d.lsn, 42);
    assert_eq!(d.prev_lsn, 41);
    assert_eq!(d.database_id, DB);
    assert_eq!(d.frames.len(), 2);
    assert_eq!(d.frames[1].commit_db_size, 5);
    // Tampering -> Checksum.
    let mut bad = bytes.clone();
    let n = bad.len();
    bad[n - 40] ^= 1;
    assert!(matches!(seg::decode(&bad), Err(seg::SegError::Checksum)));
    // Truncation.
    assert!(seg::decode(&bytes[..20]).is_err());
}

// ---------- T37r: ResourceKind namespaces ----------

/// Explicit object-key layout assertions: D1 `db/{id}/...` (T36 layout unchanged),
/// Queue `q/{id}/...`.
#[test]
fn object_keys_carry_resource_kind_namespace() {
    assert_eq!(seg::manifest_key(D1, DB), format!("db/{DB}/manifest"));
    assert_eq!(
        seg::seg_key(D1, DB, 7, 9),
        format!("db/{DB}/0000000000000007/0000000000000009.seg")
    );
    assert_eq!(
        seg::snap_key(D1, DB, 7, 9),
        format!("db/{DB}/0000000000000007/snap-0000000000000009.snap")
    );
    assert_eq!(
        seg::manifest_key(ResourceKind::Queue, DB),
        format!("q/{DB}/manifest")
    );
    assert_eq!(
        seg::seg_key(ResourceKind::Queue, DB, 7, 9),
        format!("q/{DB}/0000000000000007/0000000000000009.seg")
    );
    assert_eq!(
        seg::snap_key(ResourceKind::Queue, DB, 7, 9),
        format!("q/{DB}/0000000000000007/snap-0000000000000009.snap")
    );
}

/// GC is strictly by kind prefix: the queue's gc_orphans/gc_tombstoned never touch
/// the `db/{id}/` objects of the same id, and vice versa for D1.
#[test]
fn gc_is_kind_namespaced() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000_000));
    let cfg = || PersistConfig {
        lease_ms: 10_000,
        clock_skew_margin_ms: 500,
        holder: "h1".into(),
        checkpoint_every_commits: 0,
        retention_ms: 60_000,
        snapshot_max_bytes: 1 << 30,
    };
    let dbs = tmp.path().join("dbs");
    let d1 = Arc::new(Persister::new(
        store.clone(),
        cfg(),
        clock.clone(),
        dbs.clone(),
        D1,
    ));
    let qp = Arc::new(Persister::new(
        store.clone(),
        cfg(),
        clock.clone(),
        queue_root_for(&dbs),
        ResourceKind::Queue,
    ));
    let put = |k: &str, b: &[u8]| {
        vrt_objstore::block_on_store(store.put(k, b)).unwrap();
    };
    let get = |k: &str| vrt_objstore::block_on_store(store.get(k)).unwrap();

    // Objects of both kinds with the same id: one orphan segment each + each its own manifest.
    put(&seg::seg_key(D1, DB, 1, 9), b"d1-stray");
    put(&seg::seg_key(ResourceKind::Queue, DB, 1, 9), b"q-stray");
    let live = serde_json::to_vec(&vrt_sql::persist::Manifest {
        v: 1,
        epoch: 1,
        lsn: 1,
        holder: "h1".into(),
        expires_at_ms: u64::MAX,
        tombstone: false,
        snapshot: None,
        chain: vec![vrt_sql::persist::EpochSpan {
            epoch: 1,
            first_lsn: 1,
        }],
    })
    .unwrap();
    put(&seg::manifest_key(D1, DB), &live);
    put(&seg::manifest_key(ResourceKind::Queue, DB), &live);

    // queue gc_orphans: only clears orphan segments under q/{id}/; db/{id}/ orphans are left as-is.
    qp.gc_orphans(
        DB,
        1,
        &[vrt_sql::persist::EpochSpan {
            epoch: 1,
            first_lsn: 1,
        }],
        0,
    );
    assert!(get(&seg::seg_key(ResourceKind::Queue, DB, 1, 9)).is_none());
    assert!(get(&seg::seg_key(D1, DB, 1, 9)).is_some());
    assert!(get(&seg::manifest_key(D1, DB)).is_some());

    // D1 gc_orphans: clears orphan segments under db/{id}/; q/{id}/ objects are unaffected.
    d1.gc_orphans(
        DB,
        1,
        &[vrt_sql::persist::EpochSpan {
            epoch: 1,
            first_lsn: 1,
        }],
        0,
    );
    assert!(get(&seg::seg_key(D1, DB, 1, 9)).is_none());
    // Replay: the queue manifest/seg are not affected by D1 gc.
    assert!(get(&seg::manifest_key(ResourceKind::Queue, DB)).is_some());
}

/// RV-R66 (verified 2026-11-15): a single GC run deletes at most a cap; the remainder is counted in deferred and the next round
/// finishes it; segments on the chain that are not covered by the baseline are never deleted.
#[test]
fn gc_orphans_is_bounded_per_pass_and_keeps_live_chain() {
    use vrt_sql::persist::{EpochSpan, MAX_GC_DELETES_PER_PASS};
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<MemoryStore> = Arc::new(MemoryStore::new());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000_000));
    let d1 = Persister::new(
        store.clone(),
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: "h1".into(),
            checkpoint_every_commits: 0,
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        tmp.path().join("dbs"),
        D1,
    );
    let put = |k: &str| vrt_objstore::block_on_store(store.put(k, b"x")).unwrap();
    let get = |k: &str| vrt_objstore::block_on_store(store.get(k)).unwrap();
    // One live segment on the chain (epoch 1, lsn 1) + N+10 orphans (epoch 9, not on the chain).
    put(&seg::seg_key(D1, DB, 1, 1));
    let extra = 10usize;
    for i in 0..(MAX_GC_DELETES_PER_PASS + extra) {
        put(&seg::seg_key(D1, DB, 9, 1000 + i as u64));
    }
    let chain = [EpochSpan { epoch: 1, first_lsn: 1 }];
    let r1 = d1.gc_orphans(DB, 1, &chain, 0);
    assert_eq!(r1.deleted, MAX_GC_DELETES_PER_PASS);
    assert_eq!(r1.deferred, extra);
    assert_eq!(r1.failed, 0);
    assert!(get(&seg::seg_key(D1, DB, 1, 1)).is_some(), "live segment kept");
    // The next round finishes the rest; the live segment is still there.
    let r2 = d1.gc_orphans(DB, 1, &chain, 0);
    assert_eq!(r2.deleted, extra);
    assert_eq!(r2.deferred, 0);
    assert!(get(&seg::seg_key(D1, DB, 1, 1)).is_some());
}

/// ---------- RV-R31 #1: dead frames from a rollback spill do not enter segments; commits that land
/// within the old file length also produce segments ----------

fn db_file(root: &std::path::Path) -> PathBuf {
    vrt_sql::paths::derive_db_file(root, DB).unwrap()
}
fn wal_file(root: &std::path::Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_file(root).display()))
}
fn wal_len(root: &std::path::Path) -> u64 {
    std::fs::metadata(wal_file(root))
        .map(|m| m.len())
        .unwrap_or(0)
}

#[test]
fn rollback_spill_dead_frames_excluded_and_infile_commit_publishes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v BLOB)").unwrap();
    write(&e, "INSERT INTO t VALUES (zeroblob(64))").unwrap();
    let lsn_before = e.commit_lsn(); // 2
    let wal_before = wal_len(&root);

    // A large batch (48MB blob row) exceeds the page cache -> spills into the WAL; the last statement fails ->
    // the whole batch rolls back -> dead frames stay at the physical tail (mxFrame has moved back).
    let big = "WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM r WHERE i<768) \
               INSERT INTO t(v) SELECT zeroblob(65536) FROM r";
    assert!(e
        .query_batch_sync(
            &[stmt(big), stmt("SELECT * FROM missing_table")],
            &ctx(),
            ResultsFormat::ArrayOfObjects,
        )
        .is_err());
    let wal_dead = wal_len(&root);
    assert!(
        wal_dead > wal_before,
        "rollback spill must leave dead frames in wal: {wal_before} -> {wal_dead}"
    );

    // Small commit: its frames land entirely within the old file length occupied by dead frames (physical zero growth).
    // The old logic treated len==0 as read-only and would drop this batch; the new logic judges by verified frame count -> a segment must be produced.
    write(&e, "INSERT INTO t VALUES (zeroblob(64))").unwrap();
    assert_eq!(e.commit_lsn(), lsn_before + 1);
    assert_eq!(
        wal_len(&root),
        wal_dead,
        "in-file commit must not grow wal physically"
    );
    let key = seg::seg_key(D1, DB, 1, lsn_before + 1);
    let body = vrt_objstore::block_on_store(store.get(&key))
        .unwrap()
        .expect("segment for in-file commit must exist");
    let s = seg::decode(&body.body).unwrap();
    assert!(
        s.frames.len() < 20,
        "dead spill frames must not enter the segment: {} frames",
        s.frames.len()
    );
    assert_eq!(s.lsn, lsn_before + 1);
    assert!(s.frames.iter().any(|f| f.commit_db_size != 0));

    // Takeover restore: row-level consistency + integrity_check (the restore side has an integrity gate).
    clock.advance(20_000);
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 2);
    assert_eq!(
        read_i64(&e2, "SELECT length(v) AS count FROM t LIMIT 1"),
        64
    );
    assert!(vrt_sql::wal::integrity_ok(&db_file(&tmp.path().join("r2"))).unwrap());
}

/// ---------- RV-R31 #2: marker write failure -> commit returns an error rather than success ----------

#[test]
fn sidecar_write_failure_fails_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();

    // `.vrt-lsn` becomes a directory -> atomic_write's rename must fail ->
    // mark_intent returns Err -> this batch rolls back, no ack.
    // (No chmod: the persister calls ensure_secure_dir before every write, which pulls the directory
    // permissions back to 0700, so permission-bit injection is unreliable.)
    let dir = db_file(&root).parent().unwrap().to_path_buf();
    let fake = dir.join(".vrt-lsn");
    std::fs::remove_file(&fake).unwrap();
    std::fs::create_dir(&fake).unwrap();
    let err = write(&e, "INSERT INTO t VALUES (1)").unwrap_err();
    std::fs::remove_dir(&fake).unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // Rolled back rather than partially committed.
    assert_eq!(read_i64(&e, "SELECT count(*) AS count FROM t"), 0);
    // The engine was not fenced -- it keeps working normally once the directory is writable again.
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(read_i64(&e, "SELECT count(*) AS count FROM t"), 1);
}

/// ---------- RV-R31 #2: sidecar inconsistent with the local WAL -> claim forces restore ----------

#[test]
fn wal_prefix_mismatch_forces_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (1)").unwrap();
    write(&e, "INSERT INTO t VALUES (2)").unwrap(); // lsn 3

    // Before the crash, capture the full wal and frame chain, and compute the prefix length that "drops the last commit".
    let walp = wal_file(&root);
    let wal_bytes = std::fs::read(&walp).unwrap();
    let hdr = vrt_sql::wal::wal_header(&wal_bytes[..vrt_sql::wal::WAL_HDR]).unwrap();
    let ps = hdr.page_size();
    let fsz = vrt_sql::wal::WAL_FRAME_HDR + ps as usize;
    let seed = (
        u32::from_be_bytes(wal_bytes[24..28].try_into().unwrap()),
        u32::from_be_bytes(wal_bytes[28..32].try_into().unwrap()),
    );
    let (_v, heads, _ck) = vrt_sql::wal::scan_frames(
        &wal_bytes[vrt_sql::wal::WAL_HDR..],
        0,
        ps,
        hdr.cksum_be(),
        (hdr.salt1, hdr.salt2),
        seed,
    );
    let last_commit = heads
        .iter()
        .rposition(|h| h.commit_db_size != 0)
        .expect("committed frames exist");
    assert!(last_commit > 0, "need a strict prefix to truncate to");

    drop(e);
    drop(reg);
    // Simulate power-loss leftovers: the sidecar recorded F frames, but the disk only has the valid prefix of `last_commit` frames
    // (the chain is legal but shorter than the marker -- the frames of the last acked commit are gone).
    let keep_bytes = vrt_sql::wal::WAL_HDR + last_commit * fsz;
    if walp.exists() {
        let f = std::fs::OpenOptions::new().write(true).open(&walp).unwrap();
        f.set_len(keep_bytes as u64).unwrap();
        f.sync_all().unwrap();
    } else {
        std::fs::write(&walp, &wal_bytes[..keep_bytes]).unwrap();
    }
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-shm", db_file(&root).display())));

    // Same-node restart: the sidecar still points at manifest.lsn, but the wal is no longer complete --
    // it must go through restore; if it were wrongly trusted, prefix replay would only see the state at lsn<3 (count=1).
    let p2 = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg2 = registry(root.clone(), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 2);
}

/// ---------- RV-R31 #3: ADOPT first merges the leftover -wal, then snapshots ----------

#[test]
fn adopt_folds_leftover_wal_into_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    {
        // Old version (no persistence) writes the baseline row.
        let reg0 = EngineRegistry::new(root.clone());
        let e0 = reg0.get(D1, DB).unwrap();
        write(&e0, "CREATE TABLE t(v INTEGER)").unwrap();
        write(&e0, "INSERT INTO t VALUES (42)").unwrap();
    }
    // SIGKILL leftovers: another connection writes without checkpointing -- the wal holds
    // committed but unmerged pages (mem::forget simulates the process being killed; the fd/connection is not closed).
    let dbf = db_file(&root);
    let conn = rusqlite::Connection::open(&dbf).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (43)", []).unwrap();
    let walp = PathBuf::from(format!("{}-wal", dbf.display()));
    assert!(
        std::fs::metadata(&walp)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "leftover wal must exist"
    );
    std::mem::forget(conn);

    // ADOPT: fold -> integrity -> snapshot -> manifest.
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e, "SELECT count(*) AS count FROM t"), 2);
    assert_eq!(read_i64(&e, "SELECT max(v) AS count FROM t"), 43);

    // Remote restore takeover: the leftover wal's data must also be in the snapshot.
    clock.advance(20_000);
    let p2 = persister(store, "B", clock, tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 2);
    assert_eq!(read_i64(&e2, "SELECT max(v) AS count FROM t"), 43);
}

/// ---------- RV-R31 #5: renew by half period during restore ----------

/// A wrapper store that advances the test clock on every read -- simulating "very slow restore"; can be configured so that puts
/// fail after the Nth one (simulating a renewal CAS failure).
struct SlowStore {
    inner: Arc<MemoryStore>,
    clock: Arc<TestClock>,
    advance_ms: u64,
    puts_left: std::sync::atomic::AtomicU64,
}

impl SlowStore {
    fn gate_put(&self) -> Result<(), StoreError> {
        let left = self.puts_left.load(std::sync::atomic::Ordering::SeqCst);
        if left == 0 {
            return Err(StoreError::unavailable(true, "slow-store put gate"));
        }
        if left != u64::MAX {
            self.puts_left
                .store(left - 1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowStore {
    async fn get(&self, key: &str) -> Result<Option<vrt_objstore::StoredObject>, StoreError> {
        self.clock.advance(self.advance_ms);
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> Result<Option<vrt_objstore::ObjectMeta>, StoreError> {
        self.inner.head(key).await
    }
    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.gate_put()?;
        self.inner.put_if_none_match(key, body).await
    }
    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError> {
        self.gate_put()?;
        self.inner.put_if_match(key, expected, body).await
    }
    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.gate_put()?;
        self.inner.put(key, body).await
    }
    async fn list(&self, prefix: &str) -> Result<vrt_objstore::ListPage, StoreError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

/// lease_ms=10_000, each get advances 3_000ms -> fetching 8 segments takes ~24s >> lease.
fn slow_persisted_db() -> (
    tempfile::TempDir,
    Arc<MemoryStore>,
    Arc<SlowStore>,
    Arc<TestClock>,
) {
    let tmp = tempfile::tempdir().unwrap();
    let inner = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = inner.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store, "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    for i in 1..8 {
        write(&e, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    drop(e);
    drop(reg);
    let wrapped = Arc::new(SlowStore {
        inner: inner.clone(),
        clock: clock.clone(),
        advance_ms: 3_000,
        puts_left: std::sync::atomic::AtomicU64::new(u64::MAX),
    });
    (tmp, inner, wrapped, clock)
}

#[test]
fn restore_renews_lease_and_survives_past_expiry() {
    let (tmp, _inner, wrapped, clock) = slow_persisted_db();
    clock.advance(20_000); // A's lease expires -> B takes over.
    let store: Arc<dyn ObjectStore> = wrapped.clone();
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg2 = registry(tmp.path().join("b"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 7);
    // Restore takes ~8x3s=24s > lease 10s -- renewal must have happened:
    // manifest.expires_at_ms > the initial value at takeover (now_at_claim+lease).
    let obj = vrt_objstore::block_on_store(store.get(&seg::manifest_key(D1, DB)))
        .unwrap()
        .unwrap();
    let m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    assert_eq!(m.holder, "B");
    assert!(
        m.expires_at_ms > clock.now_ms() + 5_000,
        "lease must have been renewed during restore: expires={} now={}",
        m.expires_at_ms,
        clock.now_ms()
    );
    assert!(clock.now_ms() > 1_030_000, "restore must have taken >lease");
}

#[test]
fn restore_renew_failure_aborts_without_opening() {
    let (tmp, _inner, wrapped, clock) = slow_persisted_db();
    // Allow exactly 1 put (the manifest CAS of the takeover claim) -- the
    // renewal CAS during restore must fail -> restore aborts -> the db is not opened.
    wrapped
        .puts_left
        .store(1, std::sync::atomic::Ordering::SeqCst);
    clock.advance(20_000);
    let store: Arc<dyn ObjectStore> = wrapped.clone();
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg2 = registry(tmp.path().join("b"), p2);
    let err = reg2.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // The db was not opened/installed: the new db image must not exist (an aborted restore produces no servable file).
    let dbf = db_file(&tmp.path().join("b"));
    assert!(!dbf.exists(), "aborted restore must not install db");
}

/// ---------- RV-R53.3: periodic bounded tombstone GC ----------

fn db_keys(store: &MemoryStore, id: &str) -> Vec<String> {
    store
        .keys()
        .into_iter()
        .filter(|k| k.starts_with(&format!("db/{id}/")))
        .collect()
}

/// Retention not reached: tombstone written but expires is still in the future -> all objects retained.
#[test]
fn gc_round_retains_fresh_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);

    // Live lease (expires = now+10s) -> drop -> the tombstone keeps expires,
    // retention counts from expires -> now is still 10s short of expires, far below 60s.
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    reg.drop_db(D1, DB).unwrap();
    assert!(db_keys(&mem, DB).len() > 0);

    let rep = reg.gc_tombstoned_round();
    assert_eq!(rep.scanned, 1);
    assert_eq!(rep.reclaimed, 0);
    assert!(db_keys(&mem, DB).len() > 0, "within retention — kept");
}

/// Reclaim on expiry: advance the clock past expires+retention -> the prefix is emptied;
/// the local .dropped marker is kept (the contract of the open-refusal record).
#[test]
fn gc_round_reclaims_expired_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);

    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    reg.drop_db(D1, DB).unwrap();
    assert!(!db_keys(&mem, DB).is_empty());

    clock.advance(10_000 + 60_000 + 1); // lease + retention + ε
    let rep = reg.gc_tombstoned_round();
    assert_eq!(rep.reclaimed, 1);
    assert!(rep.objects_freed > 0);
    assert!(db_keys(&mem, DB).is_empty(), "objects gone past retention");
    assert!(
        vrt_sql::paths::marker_exists(&tmp.path().join("r"), DB).unwrap(),
        "local marker stays — open stays refused"
    );
    assert!(reg.get(D1, DB).is_err(), "dropped db still refuses open");
}

/// Per-round cap: with more than GC_TOMBSTONED_MAX_PER_ROUND tombstones, one round only scans
/// up to the cap; the rest continue next round (bounded, not wedged).
#[test]
fn gc_round_bounded_per_round() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);

    let n = vrt_sql::engine::GC_TOMBSTONED_MAX_PER_ROUND + 4;
    for i in 0..n {
        // 26-char ULID-shape ids — distinct per row (DB itself is 26
        // chars, so appending to it collided in the first draft).
        let id = format!("a{i:0>25}");
        reg.drop_db(D1, &id).unwrap();
    }
    clock.advance(1);
    let rep = reg.gc_tombstoned_round();
    assert_eq!(rep.scanned, vrt_sql::engine::GC_TOMBSTONED_MAX_PER_ROUND);
    // The `.gc-done` sentinel moves finished items out of the candidate set -- otherwise permanent markers would
    // fill every round's quota and starve the tail of the queue. The second round has only 4 candidates left, all reclaimed.
    let rep2 = reg.gc_tombstoned_round();
    assert_eq!(rep2.scanned, 4);
    assert_eq!(rep.reclaimed + rep2.reclaimed, n);
}

/// Failure isolation: one db's GC failure does not block the rest -- inject one list failure,
/// the first expired db fails and the second is still reclaimed.
#[test]
fn gc_round_failure_isolated() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);

    let id_a = "a".to_string() + &"0".repeat(25);
    let id_b = "b".to_string() + &"0".repeat(25);
    reg.drop_db(D1, &id_a).unwrap();
    reg.drop_db(D1, &id_b).unwrap();
    clock.advance(1); // expires=0 tombstones → immediately eligible

    // The next list fails -> hits exactly the first db in scan order.
    mem.inject_list_failures(vec![StoreError::unavailable(true, "boom")]);
    let rep = reg.gc_tombstoned_round();
    assert_eq!(rep.failures, 1);
    assert_eq!(rep.reclaimed + rep.failures, rep.scanned);
    assert_eq!(rep.reclaimed, 1, "the other db still reclaimed");
}

/// ---------- MIG-9: bounded startup self-check retry ----------

/// The first `fails_left` put/get calls return `Unavailable{retryable:true}`,
/// after which calls are transparently forwarded to the in-memory backend -- simulating R2 cold-start transient not-ready.
struct FlakyStore {
    inner: Arc<MemoryStore>,
    fails_left: std::sync::atomic::AtomicU64,
    calls: std::sync::atomic::AtomicU64,
    retryable: bool,
}

impl FlakyStore {
    fn gate(&self, op: &str) -> Result<(), StoreError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .fails_left
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |v| Some(v.saturating_sub(1)),
            )
            .unwrap()
            > 0
        {
            return Err(StoreError::unavailable(
                self.retryable,
                format!("{op}: simulated backend not ready"),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for FlakyStore {
    async fn get(&self, key: &str) -> Result<Option<vrt_objstore::StoredObject>, StoreError> {
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> Result<Option<vrt_objstore::ObjectMeta>, StoreError> {
        self.inner.head(key).await
    }
    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.gate("put")?;
        self.inner.put_if_none_match(key, body).await
    }
    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError> {
        self.inner.put_if_match(key, expected, body).await
    }
    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, body).await
    }
    async fn list(&self, prefix: &str) -> Result<vrt_objstore::ListPage, StoreError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

fn flaky_persister(fails: u64, retryable: bool) -> (tempfile::TempDir, Arc<Persister>, Arc<FlakyStore>) {
    let tmp = tempfile::tempdir().unwrap();
    let st = Arc::new(FlakyStore {
        inner: Arc::new(MemoryStore::new()),
        fails_left: std::sync::atomic::AtomicU64::new(fails),
        calls: std::sync::atomic::AtomicU64::new(0),
        retryable,
    });
    let p = persister(st.clone(), "A", Arc::new(TestClock::new(0)), tmp.path().join("db"));
    (tmp, p, st)
}

fn retry_policy(budget_ms: u64) -> vrt_sql::persist::SelfCheckRetry {
    vrt_sql::persist::SelfCheckRetry {
        budget: Duration::from_millis(budget_ms),
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
    }
}

/// First N transient failures -> succeeds after retry; the probe object is not left behind.
#[test]
fn self_check_retry_succeeds_after_transient_failures() {
    let (_t, p, st) = flaky_persister(3, true);
    let mut logs = 0u32;
    p.self_check_with_retry(&retry_policy(10_000), |_a, _e, _b, _err| logs += 1)
        .unwrap();
    assert!(logs >= 1, "retryable failures must be logged");
    // 3 failures + 1 success = put is called at least 4 times (after success get/del also count in calls).
    assert!(
        st.calls.load(std::sync::atomic::Ordering::SeqCst) >= 4,
        "expected retried calls"
    );
}

/// Transient failures forever -> the budget is exhausted and an error is returned, with a bounded number of backoffs (no infinite retry).
#[test]
fn self_check_retry_exhausts_budget() {
    let (_t, p, st) = flaky_persister(u64::MAX, true);
    let t0 = Instant::now();
    let mut logs = 0u32;
    let err = p
        .self_check_with_retry(&retry_policy(80), |_a, _e, _b, _err| logs += 1)
        .err()
        .unwrap();
    let calls = st.calls.load(std::sync::atomic::Ordering::SeqCst);
    // budget=80ms, backoff 5->10->20->20...: attempts are bounded and far below unbounded.
    assert!(calls >= 2 && calls < 40, "calls={calls}");
    assert!(t0.elapsed() < Duration::from_secs(5), "bounded wall time");
    assert!(logs + 1 == calls as u32 || logs == calls as u32, "every retryable failure logged");
    assert!(err.to_string().contains("unavailable"), "{err}");
}

/// Auth-class failures (retryable=false, e.g. 403) are not retried -- a single attempt returns.
#[test]
fn self_check_auth_failure_never_retries() {
    let (_t, p, st) = flaky_persister(u64::MAX, false);
    let mut logs = 0u32;
    let err = p
        .self_check_with_retry(&retry_policy(10_000), |_a, _e, _b, _err| logs += 1)
        .err()
        .unwrap();
    assert_eq!(
        st.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "non-retryable error must fail closed after the first attempt"
    );
    assert_eq!(logs, 0, "non-retryable error is not logged as retry");
    assert!(err.to_string().contains("unavailable"), "{err}");
}

/// Non-Unavailable errors such as Corrupt (probe vanished) are likewise not retried.
#[test]
fn self_check_corrupt_failure_never_retries() {
    // Use a store whose put succeeds but get returns None to trigger the Corrupt branch.
    struct VanishStore {
        inner: Arc<MemoryStore>,
        puts: std::sync::atomic::AtomicU64,
    }
    #[async_trait::async_trait]
    impl ObjectStore for VanishStore {
        async fn get(&self, _k: &str) -> Result<Option<vrt_objstore::StoredObject>, StoreError> {
            Ok(None)
        }
        async fn head(&self, k: &str) -> Result<Option<vrt_objstore::ObjectMeta>, StoreError> {
            self.inner.head(k).await
        }
        async fn put_if_none_match(&self, k: &str, b: &[u8]) -> Result<String, StoreError> {
            self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_if_none_match(k, b).await
        }
        async fn put_if_match(&self, k: &str, e: &str, b: &[u8]) -> Result<String, StoreError> {
            self.inner.put_if_match(k, e, b).await
        }
        async fn put(&self, k: &str, b: &[u8]) -> Result<String, StoreError> {
            self.inner.put(k, b).await
        }
        async fn list(&self, p: &str) -> Result<vrt_objstore::ListPage, StoreError> {
            self.inner.list(p).await
        }
        async fn delete(&self, k: &str) -> Result<(), StoreError> {
            self.inner.delete(k).await
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let st = Arc::new(VanishStore {
        inner: Arc::new(MemoryStore::new()),
        puts: std::sync::atomic::AtomicU64::new(0),
    });
    let p = persister(st.clone(), "A", Arc::new(TestClock::new(0)), tmp.path().join("db"));
    let err = p
        .self_check_with_retry(&retry_policy(10_000), |_a, _e, _b, _err| {})
        .err()
        .unwrap();
    assert_eq!(st.puts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(err.to_string().contains("vanished"), "{err}");
}

/// Error sanitization: embedded http(s) URLs (including query/signature parameters) are stripped entirely.
#[test]
fn self_check_error_sanitizes_urls() {
    use vrt_sql::persist::sanitize_self_check_error;
    let raw = "send: error sending request for url (https://acct.r2.cloudflarestorage.com/bucket/probe/x?X-Amz-Signature=deadbeef&X-Amz-Credential=AKID): connection refused";
    let out = sanitize_self_check_error(raw);
    assert!(out.contains("<url>"), "{out}");
    assert!(!out.contains("r2.cloudflarestorage.com"), "{out}");
    assert!(!out.contains("X-Amz-Signature"), "{out}");
    assert!(!out.contains("AKID"), "{out}");
    assert!(out.contains("connection refused"), "{out}");
    // Multiple URLs + the no-URL case.
    assert_eq!(
        sanitize_self_check_error("a http://x/1 b https://y/2 c"),
        "a <url> b <url> c"
    );
    assert_eq!(sanitize_self_check_error("no url"), "no url");
}
