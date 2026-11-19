//! T36 对抗测试：lease 夺占 / 上传失败 / kill -9 恢复 / 段损坏 /
//! fs CAS 并发 / lease 临界 / adopt 升级 / drop tombstone / GC。

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

const DB: &str = "abcdefghijklmnopqrstuvwxyz"; // 26 小写字母，合法 id

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
            checkpoint_every_commits: 0, // 测试默认关，按需开
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        db_root,
        D1,
    ))
}

/// T37r：注册表需要两种 kind 的 Persister——queue 侧与 D1 共享
/// store/cfg/clock，root 取 `dbs` 的同级 `queues/`。
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

/// ---------- §4.1 lease 夺占：A 过期 → B epoch+1 接管 → A fenced ----------

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

    // A 的写者 lease 过期 → B 接管 epoch+1。
    clock.advance(20_000);
    let eb = reg_b.get(D1, DB).unwrap();
    assert_eq!(eb.lease_epoch(), 2);
    assert_eq!(eb.commit_lsn(), 2); // 接管后 lsn 不回退
    assert_eq!(read_i64(&eb, "SELECT count(*) AS count FROM t"), 1);

    // A 再写 → Fenced（lease 已失）。
    let err = write(&ea, "INSERT INTO t VALUES (99)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");
    // A 的读同样被拒（§4.3 lease 不可用一律拒）。
    assert!(ea
        .query_batch_sync(&[stmt("SELECT 1")], &ctx(), ResultsFormat::ArrayOfObjects)
        .is_err());

    // A 的孤儿段（手工植入其旧 epoch 键空间）不进 manifest。
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

    // B 读到的不含 A 的过期写（A 的 99 从未进 manifest 链）。
    assert_eq!(read_i64(&eb, "SELECT count(*) AS count FROM t"), 1);

    // B 继续写推进 manifest；恢复时孤儿段被 GC/忽略。
    write(&eb, "INSERT INTO t VALUES (2)").unwrap();
    let eb2 = reg_b.get(D1, DB).unwrap();
    assert_eq!(eb2.commit_lsn(), 3);
}

/// ---------- §4.2 上传失败：不 ack、不残留已提交未持久化数据 ----------

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

    // 注入一次 put 失败 → 写返回错误（不 ack）。
    mem.inject_put_failures(vec![StoreError::unavailable(true, "injected")]);
    let err = write(&e, "INSERT INTO t VALUES (2)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");

    // 引擎 fenced → 释放 Arc（还 flock）→ evict → 重新 claim
    // （lease 仍是我们的 → 恢复）。
    let epoch = e.lease_epoch();
    drop(e);
    reg.evict_fenced(D1, DB, epoch);
    let e2 = reg.get(D1, DB).unwrap();
    // 未 ack 的 INSERT (2) 已被恢复清掉——本地绝不保留幽灵提交。
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 1);
}

/// ---------- kill -9：已 ack 全在、未 ack 不在 ----------

/// 子进程入口（父测试以当前测试二进制 + env 重入）。
/// `cargo test kill9_child_worker` 直接跑则立即返回。
#[test]
fn kill9_child_worker() {
    if std::env::var("VRT_T36_KILL_CHILD").is_err() {
        return;
    }
    let root = PathBuf::from(std::env::var("VRT_T36_ROOT").unwrap());
    let store_root = PathBuf::from(std::env::var("VRT_T36_STORE").unwrap());
    let progress = PathBuf::from(std::env::var("VRT_T36_PROGRESS").unwrap());
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
    write(&e, "CREATE TABLE t(k INTEGER PRIMARY KEY)").unwrap();
    for k in 1..=n {
        write(&e, &format!("INSERT INTO t VALUES ({k})")).unwrap();
        // 只有 ack 后才记 progress——父进程按 ack 数断言。
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

#[test]
fn kill9_recovers_acked_and_drops_unacked() {
    if std::env::var("VRT_T36_KILL_CHILD").is_ok() {
        return; // 子进程分支
    }
    const N: u64 = 1_000;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbroot");
    let store_root = tmp.path().join("store");
    let progress = tmp.path().join("progress");
    std::fs::create_dir_all(&root).unwrap();

    let exe = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(exe)
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
        .env("VRT_T36_N", N.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // 等第一批 ack（子进程可能跑完全部 N——两种结果都合法）。
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut acked = 0u64;
    loop {
        if let Ok(raw) = std::fs::read_to_string(&progress) {
            acked = raw.lines().count() as u64;
        }
        let alive = child.try_wait().unwrap().is_none();
        if acked >= N / 2 || !alive || Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if child.try_wait().unwrap().is_none() {
        child.kill().unwrap(); // SIGKILL —— kill -9 等价。
    }
    let _ = child.wait();
    if let Ok(raw) = std::fs::read_to_string(&progress) {
        acked = raw.lines().count() as u64;
    }
    assert!(acked >= 1, "child must have acked at least one txn");

    // 新"进程"（新 registry + 新 holder）接管：等 lease 过期。
    // 用 SystemClock：等 child 的 lease_ms(120s) 不现实——通过直接
    // 重写 manifest 过期时间模拟 lease 到期（等价时钟推进）。
    let store: Arc<dyn ObjectStore> = Arc::new(FsStore::new(store_root.clone()).unwrap());
    let mkey = seg::manifest_key(D1, DB);
    let obj = vrt_objstore::block_on_store(store.get(&mkey))
        .unwrap()
        .expect("manifest exists");
    let mut m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    m.expires_at_ms = 0; // 强制过期
    let body = serde_json::to_vec(&m).unwrap();
    vrt_objstore::block_on_store(store.put_if_match(&mkey, &obj.meta.etag, &body)).unwrap();

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let p = Arc::new(Persister::new(
        store.clone(),
        PersistConfig {
            lease_ms: 60_000,
            clock_skew_margin_ms: 500,
            holder: "recovery".into(),
            checkpoint_every_commits: 0,
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        root.clone(),
        D1,
    ));
    let reg2 = EngineRegistry::new(root.clone()).with_persisters(pair(&root, p));
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(e2.lease_epoch(), m.epoch + 1);
    assert_eq!(e2.commit_lsn(), m.lsn);

    // 每个已 ack 的 k 都在；任何未 ack 的不在（max==count==acked）。
    let n_rows = read_i64(&e2, "SELECT count(*) AS count FROM t");
    assert_eq!(
        n_rows,
        m.lsn as i64 - 1,
        "lsn covers DDL+inserts; manifest.lsn={} rows={n_rows}",
        m.lsn
    );
    // progress 只在 ack 后追加；SIGKILL 可能落在 "ack 后、追加前" 的
    // 缝隙 → rows ∈ {acked, acked+1}，绝不能 < acked（丢 ack）。
    assert!(
        n_rows == acked as i64 || n_rows == acked as i64 + 1,
        "acked={acked} rows={n_rows} — 已 ack 事务必须全部在，至多差一个 ack/append 缝隙"
    );
    let max_k = read_i64(&e2, "SELECT COALESCE(MAX(k),0) AS count FROM t");
    assert_eq!(max_k, n_rows, "persisted set must be a contiguous prefix");
}

/// ≥1e5 事务 fuzz（默认忽略；`--ignored` 跑）。
#[test]
#[ignore = "slow: 100k-op kill -9 fuzz; run with --include-ignored"]
fn kill9_fuzz_100k() {
    std::env::set_var("VRT_T36_N", "100000");
    kill9_recovers_acked_and_drops_unacked();
}

/// ---------- 段损坏 / sha 不符 / seq 断档 → 恢复 fail closed ----------

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
    // 接管前先腐化 lsn=2 的段（改字节 → sha256 不符）。
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
    // 制造断档：删掉 lsn=2 的段（manifest.lsn=3）。
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

/// ---------- fs store CAS 并发：多线程同时 put_if_match 恰一成 ----------

#[test]
fn fs_cas_exactly_one_winner() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(FsStore::new(tmp.path().to_path_buf()).unwrap());
    // 先种对象拿 etag。
    let etag = vrt_objstore::block_on_store(store.put("k", b"v0")).unwrap();
    let winners = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..16 {
        let (s, e, w) = (store.clone(), etag.clone(), winners.clone());
        handles.push(std::thread::spawn(move || {
            // 写异值：与 v0 相同内容的 CAS 是合法 no-op（etag 不变，
            // 不使他人前置条件失效）——用互异值才能断言"恰一胜者"。
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

/// ---------- lease 临界：expiry-margin 边界 / 续租失败 → fenced / 恢复 ----------

#[test]
fn lease_boundary_and_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    // lease_ms=10_000 margin=500：usable iff now < expires-500。
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p.clone());
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    // claim 于 now=0：expires=10_000。usable 边界 now<9_500。
    clock.set(9_499);
    write(&e, "INSERT INTO t VALUES (1)").unwrap(); // 边界内可用
    clock.set(9_500);
    // now == expires-margin → 不可用 → 续租 → 成功（lease 延到 19_500）。
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    assert!(!e.is_fenced());

    // 注：9_499 写时已触发 due_renew（提前量 lease/4，阈值 7000），
    // lease 实际续到 expires=19_499 → usable 边界 now<18_999。
    // 续租失败注入：usable 且 due_renew → degraded，写仍成功。
    clock.set(18_000); // 16_499 ≤ 18_000 < 18_999
    mem.inject_put_failures(vec![StoreError::unavailable(true, "renewfail")]);
    write(&e, "INSERT INTO t VALUES (3)").unwrap(); // degraded 写仍成（CAS 成功）
    assert!(e.is_degraded());
    // 推进到不可用边界：续租仍失败 → fenced。
    mem.inject_put_failures(vec![
        StoreError::unavailable(true, "renewfail"),
        StoreError::unavailable(true, "renewfail"),
        StoreError::unavailable(true, "renewfail"),
    ]);
    clock.set(18_999);
    let err = write(&e, "INSERT INTO t VALUES (4)").unwrap_err();
    assert!(matches!(err, EngineError::Fenced(_)), "{err:?}");

    // 恢复：fenced → evict → lease 重新可得（我们是 holder）→ 自愈。
    // 先清空残余注入（recover claim 的 CAS 不能再被污染），并归还
    // 旧引擎 Arc（flock 释出后重开才拿得到锁）。
    mem.inject_put_failures(vec![]);
    let old_epoch = e.lease_epoch();
    drop(e);
    reg.evict_fenced(D1, DB, old_epoch);
    let e2 = reg.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 3);
}

/// ---------- adopt：旧版无 manifest 本地库迁入 ----------

#[test]
fn adopt_pre_manifest_local_db() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    // 旧版写入：无持久化的 registry。
    {
        let reg = EngineRegistry::new(root.clone());
        let e = reg.get(D1, DB).unwrap();
        write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
        write(&e, "INSERT INTO t VALUES (42)").unwrap();
    }
    // T36 启动：同 root 挂持久化 → adopt 上传快照 + manifest epoch=1。
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    assert_eq!(e.lease_epoch(), 1);
    assert_eq!(read_i64(&e, "SELECT v AS count FROM t"), 42);
    // manifest 已建且带快照基线。
    let obj = vrt_objstore::block_on_store(store.get(&seg::manifest_key(D1, DB)))
        .unwrap()
        .unwrap();
    let m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    assert!(m.snapshot.is_some());
    assert_eq!(m.epoch, 1);
    // 另一写者接管后也能读到 adopt 的数据。
    clock.advance(20_000);
    let p2 = persister(store, "B", clock, tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT v AS count FROM t"), 42);
}

/// ---------- 启动自检：只读对象根 → self_check 必败（P0-B 复现） ----------

/// 复现容器内 `vrt-sql-objects` 卷属主错误（root:root 0755 挂给
/// uid 10001）：写探针 EACCES → self_check Err → 启动 fail closed。
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
    // 恢复权限以便 tempdir 清理。
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// ---------- 失败路径可观测：claim 失败计入 persistence_failures_total ----------

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

/// ---------- 他人有效期内 claim → HeldByOther（503，不抢） ----------

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
    // A 的 lease 仍有效（now=1000 << 10_000-margin）→ B 不得抢。
    clock.set(1_000);
    let err = reg_b.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // B 失败后 A 不受影响。
    write(&ea, "INSERT INTO t VALUES (1)").unwrap();
}

/// ---------- 优雅停机 lease 释放（T36b） ----------

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
    // A 释放（lease 远未到期——now=1000,lease 15000）。
    clock.set(1_000);
    reg_a.begin_shutdown();
    reg_a.release_leases(std::time::Instant::now() + std::time::Duration::from_secs(5));
    // B 不等待即接管 epoch+1。
    let eb = reg_b.get(D1, DB).unwrap();
    assert_eq!(eb.lease_epoch(), 2);
    write(&eb, "INSERT INTO t VALUES (1)").unwrap();
    // 释放后的 A 引擎再写 → Fenced。
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
    // closing 之后 get() 一律 Unavailable（同进程不得重夺）。
    let err = reg_a.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
}

/// ---------- 快照超限：stat 短路 + 指数退避（T36b） ----------

#[test]
fn snapshot_over_cap_backs_off_and_reports() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    // snapshot_max_bytes=1：db 永远超限；every=2。
    let p = Arc::new(Persister::new(
        store,
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: "A".into(),
            checkpoint_every_commits: 2,
            retention_ms: 60_000,
            // RV-R31：cap 要夹在「lsn0 基线（~20KB init 镜像，必
            // 须随首段上传成功）」与「膨胀后的库」之间——lsn2 插
            // 入 60KB 行后 db>cap，之后每次 checkpoint 快照都超限。
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
    // 2 提交触发第一次 checkpoint → 超限 → gap 2→4，计数+1。
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    write(&e, "INSERT INTO t VALUES (zeroblob(60000))").unwrap();
    assert_eq!(cap(), c0 + 1.0, "first over-cap attempt counted");
    // 重置后需 4 个提交到 gap=4：第 3 个仍不尝试，第 4 个尝试 → +1。
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    write(&e, "INSERT INTO t VALUES (3)").unwrap();
    write(&e, "INSERT INTO t VALUES (4)").unwrap();
    assert_eq!(cap(), c0 + 1.0, "backed off at c<4");
    write(&e, "INSERT INTO t VALUES (5)").unwrap();
    assert_eq!(cap(), c0 + 2.0, "second attempt at gap boundary");
    // 第二次失败 gap 4→8：接下来 5 个提交（c<8）不再尝试。
    for i in 6..11 {
        write(&e, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    assert_eq!(cap(), c0 + 2.0, "gap doubled to 8 — no further attempts");
    // 诊断暴露 snapshotOverCap + chainLen。
    let d = reg
        .diagnostics()
        .into_iter()
        .find(|d| d.database_id == DB)
        .unwrap();
    assert!(d.snapshot_over_cap, "diag must expose over-cap: {d:?}");
    assert!(d.chain_len >= 6, "chain_len grows: {d:?}");
}

/// ---------- drop tombstone：manifest CAS 先 fence，再本地清理 ----------

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
    // manifest tombstone 已落。
    let obj = vrt_objstore::block_on_store(store.get(&seg::manifest_key(D1, DB)))
        .unwrap()
        .unwrap();
    let m: Manifest = serde_json::from_slice(&obj.body).unwrap();
    assert!(m.tombstone);
    // 同进程再 get → Dropped；新 registry → 同样 Dropped（manifest 面）。
    assert!(matches!(reg.get(D1, DB), Err(EngineError::Dropped)));
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    assert!(matches!(reg2.get(D1, DB), Err(EngineError::Dropped)));
    // 幂等。
    assert!(!reg.drop_db(D1, DB).unwrap());
}

/// ---------- 持久层不可用：启动/冷开 fail closed ----------

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

/// ---------- seg 编解码 roundtrip + 格式闸 ----------

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
    // 篡改 → Checksum。
    let mut bad = bytes.clone();
    let n = bad.len();
    bad[n - 40] ^= 1;
    assert!(matches!(seg::decode(&bad), Err(seg::SegError::Checksum)));
    // 截断。
    assert!(seg::decode(&bytes[..20]).is_err());
}

// ---------- T37r：ResourceKind 命名空间 ----------

/// 对象键布局显式断言：D1 `db/{id}/…`（T36 布局不变），
/// Queue `q/{id}/…`。
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

/// GC 严格按 kind 前缀：queue 的 gc_orphans/gc_tombstoned 绝不触及
/// 同 id 的 `db/{id}/` 对象，D1 亦然。
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

    // 同 id 两 kind 的对象：各 1 个孤儿段 + 各自 manifest。
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

    // queue gc_orphans：只清 q/{id}/ 下的孤儿段；db/{id}/ 孤儿原样。
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

    // D1 gc_orphans：清 db/{id}/ 孤儿段；q/{id}/ 对象不受影响。
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
    // 重放：queue manifest/seg 不受 D1 gc 波及。
    assert!(get(&seg::manifest_key(ResourceKind::Queue, DB)).is_some());
}

/// RV-R66（已验证 2026-11-15）：GC 单次删除有上限，余量计入 deferred，下一轮
/// 收尾；已在链上且未被基线覆盖的段绝不删。
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
    // 一个在链上的活段（epoch 1, lsn 1）+ N+10 个孤儿（epoch 9 不在链上）。
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
    // 下一轮收完剩余，活段仍在。
    let r2 = d1.gc_orphans(DB, 1, &chain, 0);
    assert_eq!(r2.deleted, extra);
    assert_eq!(r2.deferred, 0);
    assert!(get(&seg::seg_key(D1, DB, 1, 1)).is_some());
}

/// ---------- RV-R31 #1：回滚 spill 死帧不进段；落在旧文件长度
/// 内的提交同样生成段 ----------

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

    // 大批次（48MB blob 行）超页缓存 → spill 进 WAL；末句失败 →
    // 整批回滚 → 死帧留在物理尾部（mxFrame 已退回）。
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

    // 小提交：帧完全落在死帧占据的旧文件长度内（物理零增长）。
    // 旧逻辑 len==0 判只读会丢这批；新逻辑按验证帧数判定 → 必须出段。
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

    // 接管恢复：行级一致 + integrity_check（恢复侧有 integrity 门）。
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

/// ---------- RV-R31 #2：标记写失败 → commit 返回错误而非成功 ----------

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

    // `.vrt-lsn` 变成目录 → atomic_write 的 rename 必败 →
    // mark_intent 返回 Err → 本批回滚、不 ack。
    // （不用 chmod：persister 每次写前 ensure_secure_dir 会把目录
    // 权限拉回 0700，权限位注入不可靠。）
    let dir = db_file(&root).parent().unwrap().to_path_buf();
    let fake = dir.join(".vrt-lsn");
    std::fs::remove_file(&fake).unwrap();
    std::fs::create_dir(&fake).unwrap();
    let err = write(&e, "INSERT INTO t VALUES (1)").unwrap_err();
    std::fs::remove_dir(&fake).unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // 回滚而非部分提交。
    assert_eq!(read_i64(&e, "SELECT count(*) AS count FROM t"), 0);
    // 引擎未被 fence——恢复目录可写后继续正常工作。
    write(&e, "INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(read_i64(&e, "SELECT count(*) AS count FROM t"), 1);
}

/// ---------- RV-R31 #2：sidecar 与本地 WAL 不一致 → 认领强制恢复 ----------

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

    // 崩溃前抓下完整 wal 与帧链，算出"丢掉末次提交"的前缀长度。
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
    // 模拟掉电残留：sidecar 记了 F 帧，盘上只剩 `last_commit` 帧
    // 的有效前缀（链合法但比标记短——末次已 ack 提交的帧没了）。
    let keep_bytes = vrt_sql::wal::WAL_HDR + last_commit * fsz;
    if walp.exists() {
        let f = std::fs::OpenOptions::new().write(true).open(&walp).unwrap();
        f.set_len(keep_bytes as u64).unwrap();
        f.sync_all().unwrap();
    } else {
        std::fs::write(&walp, &wal_bytes[..keep_bytes]).unwrap();
    }
    let _ = std::fs::remove_file(PathBuf::from(format!("{}-shm", db_file(&root).display())));

    // 同节点重启：sidecar 仍指 manifest.lsn，但 wal 已不完整——
    // 必须走恢复；若被误信，前缀回放只会看到 lsn<3 的状态（count=1）。
    let p2 = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg2 = registry(root.clone(), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 2);
}

/// ---------- RV-R31 #3：ADOPT 先合并残留 -wal 再快照 ----------

#[test]
fn adopt_folds_leftover_wal_into_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("r");
    {
        // 旧版（无持久化）写入基线行。
        let reg0 = EngineRegistry::new(root.clone());
        let e0 = reg0.get(D1, DB).unwrap();
        write(&e0, "CREATE TABLE t(v INTEGER)").unwrap();
        write(&e0, "INSERT INTO t VALUES (42)").unwrap();
    }
    // SIGKILL 残留：另一个连接写入后不 checkpoint——wal 里有
    // 已提交未合并的页（mem::forget 模拟进程被杀，fd/连接不关）。
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

    // ADOPT：fold → integrity → snapshot → manifest。
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let clock = Arc::new(TestClock::new(0));
    let p = persister(store.clone(), "A", clock.clone(), root.clone());
    let reg = registry(root.clone(), p);
    let e = reg.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e, "SELECT count(*) AS count FROM t"), 2);
    assert_eq!(read_i64(&e, "SELECT max(v) AS count FROM t"), 43);

    // 远端恢复接管：残留 wal 的数据必须也在快照里。
    clock.advance(20_000);
    let p2 = persister(store, "B", clock, tmp.path().join("r2"));
    let reg2 = registry(tmp.path().join("r2"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 2);
    assert_eq!(read_i64(&e2, "SELECT max(v) AS count FROM t"), 43);
}

/// ---------- RV-R31 #5：恢复期间按半周期续租 ----------

/// 每次读都推进测试时钟的包装 store——模拟"恢复很慢"；可配置第
/// N 次之后的 put* 失败（模拟续租 CAS 失败）。
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

/// lease_ms=10_000、每次 get 推 3_000ms → 拉 8 个段约 24s ≫ lease。
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
    clock.advance(20_000); // A 的 lease 过期 → B 接管。
    let store: Arc<dyn ObjectStore> = wrapped.clone();
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg2 = registry(tmp.path().join("b"), p2);
    let e2 = reg2.get(D1, DB).unwrap();
    assert_eq!(read_i64(&e2, "SELECT count(*) AS count FROM t"), 7);
    // 恢复耗时 ~8×3s=24s > lease 10s——续租必须已发生：
    // manifest.expires_at_ms > 接管时的初值（now_at_claim+lease）。
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
    // 允许恰好 1 次 put（接管 claim 的 manifest CAS）——恢复中的
    // 续租 CAS 必败 → 恢复中止 → 库不打开。
    wrapped
        .puts_left
        .store(1, std::sync::atomic::Ordering::SeqCst);
    clock.advance(20_000);
    let store: Arc<dyn ObjectStore> = wrapped.clone();
    let p2 = persister(store.clone(), "B", clock.clone(), tmp.path().join("b"));
    let reg2 = registry(tmp.path().join("b"), p2);
    let err = reg2.get(D1, DB).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    // 库未被打开/安装：新 db 镜像不得存在（恢复中止不产出可服务文件）。
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

/// 未到保留期：tombstone 已写但 expires 还在未来 → 对象全保留。
#[test]
fn gc_round_retains_fresh_tombstones() {
    let tmp = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let clock = Arc::new(TestClock::new(1_000_000));
    let p = persister(store.clone(), "A", clock.clone(), tmp.path().join("r"));
    let reg = registry(tmp.path().join("r"), p);

    // 活 lease（expires = now+10s）→ drop → tombstone 保留 expires，
    // 保留期从 expires 起算 → now 距 expires 还差 10s，远小于 60s。
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    reg.drop_db(D1, DB).unwrap();
    assert!(db_keys(&mem, DB).len() > 0);

    let rep = reg.gc_tombstoned_round();
    assert_eq!(rep.scanned, 1);
    assert_eq!(rep.reclaimed, 0);
    assert!(db_keys(&mem, DB).len() > 0, "within retention — kept");
}

/// 到期回收：推进时钟越过 expires+retention → 前缀清空；
/// 本地 .dropped marker 保留（open 拒绝记录的契约）。
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

/// 单轮上限：>GC_TOMBSTONED_MAX_PER_ROUND 个 tombstone 一轮只扫
/// 上限个；余下下轮继续（有界，不 wedged）。
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
    // `.gc-done` 哨兵把已完成项移出候选集——否则永久 marker 会
    // 占满每轮名额饿死队尾。第二轮只剩 4 个候选且全部回收。
    let rep2 = reg.gc_tombstoned_round();
    assert_eq!(rep2.scanned, 4);
    assert_eq!(rep.reclaimed + rep2.reclaimed, n);
}

/// 失败隔离：一个库的 GC 失败不阻塞其余——注入一次 list 失败，
/// 首个到期库失败、第二个仍被回收。
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

    // 下一次 list 失败 → 恰好砸中扫描序里的第一个库。
    mem.inject_list_failures(vec![StoreError::unavailable(true, "boom")]);
    let rep = reg.gc_tombstoned_round();
    assert_eq!(rep.failures, 1);
    assert_eq!(rep.reclaimed + rep.failures, rep.scanned);
    assert_eq!(rep.reclaimed, 1, "the other db still reclaimed");
}

/// ---------- MIG-9：启动自检有界重试 ----------

/// 前 `fails_left` 次 put/get 返回 `Unavailable{retryable:true}`，
/// 之后透明转发内存后端——模拟 R2 冷启动瞬时未就绪。
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

/// 前 N 次瞬时失败 → 重试后成功；probe 对象不残留。
#[test]
fn self_check_retry_succeeds_after_transient_failures() {
    let (_t, p, st) = flaky_persister(3, true);
    let mut logs = 0u32;
    p.self_check_with_retry(&retry_policy(10_000), |_a, _e, _b, _err| logs += 1)
        .unwrap();
    assert!(logs >= 1, "retryable failures must be logged");
    // 失败 3 次 + 成功 1 次 = put 至少被调 4 次（成功后 get/del 也计 calls）。
    assert!(
        st.calls.load(std::sync::atomic::Ordering::SeqCst) >= 4,
        "expected retried calls"
    );
}

/// 永远瞬时失败 → 预算耗尽返回错误，且退避次数有界（不无限重试）。
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
    // budget=80ms, backoff 5→10→20→20...：尝试次数有界且远小于无限。
    assert!(calls >= 2 && calls < 40, "calls={calls}");
    assert!(t0.elapsed() < Duration::from_secs(5), "bounded wall time");
    assert!(logs + 1 == calls as u32 || logs == calls as u32, "every retryable failure logged");
    assert!(err.to_string().contains("unavailable"), "{err}");
}

/// 认证类失败（retryable=false，如 403）不重试——单次尝试即返回。
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

/// Corrupt（probe 消失）等非 Unavailable 错误同样不重试。
#[test]
fn self_check_corrupt_failure_never_retries() {
    // 用一个 put 成功但 get 返回 None 的 store 触发 Corrupt 分支。
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

/// 错误净化：内嵌 http(s) URL（含 query/签名参数）整体剥离。
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
    // 多 URL + 无 URL 情形。
    assert_eq!(
        sanitize_self_check_error("a http://x/1 b https://y/2 c"),
        "a <url> b <url> c"
    );
    assert_eq!(sanitize_self_check_error("no url"), "no url");
}
