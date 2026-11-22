//! 2026-11-15 regression: snapshot -> restart restore on the real `S3Store` (local mock S3 wire).
//! The `get_to_file` of the in-memory/fs backends is a different implementation,
//! and it once masked a double-hash bug in the s3 backend's `get_to_file` (sha256(sha256(x))) that caused
//! "snapshot sha256 mismatch" and every db failing to restore after a restart.
//!
//! Covers: snapshot/segment publication during concurrent writes + periodic checkpoints -> restore on a new node
//! (brand-new local disk); a corrupt snapshot fails closed with bounded backoff (no repeated
//! downloads, no dragging down other dbs, self-heals once the object is repaired).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use vrt_objstore::sigv4::{sha256_hex, SecretString};
use vrt_objstore::{ObjectStore, S3Store, S3StoreConfig};
use vrt_sql::clock::TestClock;
use vrt_sql::engine::{EngineError, EngineRegistry, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::{self, D1};
use vrt_sql::persist::{PersistConfig, Persister};

const DB_A: &str = "abcdefghijklmnopqrstuvwxyz";
const DB_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Default)]
struct Mock {
    objects: BTreeMap<String, (Vec<u8>, String)>,
    /// key -> GET count (asserts "bounded retry" does not re-download).
    gets: BTreeMap<String, usize>,
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn respond(stream: &mut std::net::TcpStream, status: &str, etag: &str, body: &[u8], head: bool) {
    let mut out = format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    if !etag.is_empty() {
        out.push_str(&format!("etag: \"{etag}\"\r\n"));
    }
    out.push_str("\r\n");
    let _ = stream.write_all(out.as_bytes());
    if !head {
        let _ = stream.write_all(body);
    }
}

fn handle(state: &Arc<Mutex<Mock>>, mut stream: std::net::TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut it = line.split(' ');
    let method = it.next().unwrap().to_string();
    let target = it.next().unwrap_or_default().to_string();
    let mut cl = 0usize;
    let (mut if_none, mut if_match) = (false, None::<String>);
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).unwrap();
        let h = h.trim();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            match k.as_str() {
                "content-length" => cl = v.parse().unwrap_or(0),
                "if-none-match" => if_none = true,
                "if-match" => if_match = Some(v.trim_matches('"').to_string()),
                _ => {}
            }
        }
    }
    let mut body = vec![0u8; cl];
    reader.read_exact(&mut body).unwrap();
    let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    let key = pct_decode(path.trim_start_matches("/b/"));
    let mut st = state.lock().unwrap();
    if method == "GET" && query.contains("list-type=2") {
        let pfx = query
            .split('&')
            .find_map(|p| p.strip_prefix("prefix="))
            .map(pct_decode)
            .unwrap_or_default();
        let mut xml =
            String::from("<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>false</IsTruncated>");
        for (k, (v, _)) in st.objects.iter().filter(|(k, _)| k.starts_with(&pfx)) {
            xml.push_str(&format!(
                "<Contents><Key>{k}</Key><Size>{}</Size></Contents>",
                v.len()
            ));
        }
        xml.push_str("</ListBucketResult>");
        drop(st);
        respond(&mut stream, "200 OK", "", xml.as_bytes(), false);
        return;
    }
    match method.as_str() {
        "PUT" => {
            let exists = st.objects.get(&key).map(|(_, e)| e.clone());
            let blocked = (if_none && exists.is_some())
                || match &if_match {
                    Some(m) => exists.as_deref() != Some(m.as_str()),
                    None => false,
                };
            if blocked {
                drop(st);
                respond(&mut stream, "412 Precondition Failed", "", b"", false);
            } else {
                let etag = sha256_hex(&body)[..32].to_string();
                st.objects.insert(key, (body, etag.clone()));
                drop(st);
                respond(&mut stream, "200 OK", &etag, b"", false);
            }
        }
        "GET" | "HEAD" => {
            if method == "GET" {
                *st.gets.entry(key.clone()).or_default() += 1;
            }
            let found = st.objects.get(&key).cloned();
            drop(st);
            match found {
                Some((b, e)) => respond(&mut stream, "200 OK", &e, &b, method == "HEAD"),
                None => respond(&mut stream, "404 Not Found", "", b"", false),
            }
        }
        "DELETE" => {
            st.objects.remove(&key);
            drop(st);
            respond(&mut stream, "204 No Content", "", b"", false);
        }
        _ => {
            drop(st);
            respond(&mut stream, "405 Method Not Allowed", "", b"", false);
        }
    }
}

fn spawn_s3() -> (Arc<dyn ObjectStore>, Arc<Mutex<Mock>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(Mutex::new(Mock::default()));
    let st = state.clone();
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            let st = st.clone();
            std::thread::spawn(move || handle(&st, s));
        }
    });
    let store = S3Store::new(S3StoreConfig {
        endpoint: format!("http://{addr}"),
        region: "auto".into(),
        bucket: "b".into(),
        access_key: "ak".into(),
        secret_key: SecretString::new("sk"),
        timeout: Duration::from_secs(5),
        conditional_writes: true,
        retries: 1,
        prefix: String::new(),
    })
    .unwrap();
    (Arc::new(store), state)
}

fn registry(
    store: Arc<dyn ObjectStore>,
    holder: &str,
    clock: Arc<TestClock>,
    root: PathBuf,
    ckpt: u64,
) -> EngineRegistry {
    let cfg = PersistConfig {
        lease_ms: 10_000,
        clock_skew_margin_ms: 500,
        holder: holder.into(),
        checkpoint_every_commits: ckpt,
        retention_ms: 60_000,
        snapshot_max_bytes: 1 << 30,
    };
    let d1 = Arc::new(Persister::new(
        store.clone(),
        cfg.clone(),
        clock.clone(),
        root.clone(),
        D1,
    ));
    let queue = Arc::new(Persister::new(
        store,
        cfg,
        clock,
        queue_root_for(&root),
        ResourceKind::Queue,
    ));
    EngineRegistry::new(root).with_persisters(vrt_sql::engine::PersisterPair { d1, queue })
}

fn exec(
    e: &vrt_sql::engine::RusqliteEngine,
    sql: &str,
) -> Result<serde_json::Value, EngineError> {
    let ctx = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(30),
        dedup: None,
    };
    e.query_batch_sync(
        &[SqlStmt {
            sql: sql.into(),
            params: None,
        }],
        &ctx,
        ResultsFormat::ArrayOfObjects,
    )
    .map(|mut o| o.remove(0).results)
}

fn count(e: &vrt_sql::engine::RusqliteEngine) -> i64 {
    exec(e, "SELECT count(*) AS count FROM t").unwrap()[0]["count"]
        .as_i64()
        .unwrap()
}

fn counter(reason: &str) -> f64 {
    let text = vrt_sql::metrics::registry().render();
    let pat = format!("vrt_sql_persistence_failures_total{{reason=\"{reason}\"}}");
    text.lines()
        .find(|l| l.starts_with(&pat))
        .and_then(|l| l.rsplit(' ').next()?.parse().ok())
        .unwrap_or(0.0)
}

/// Publish snapshots/segments during concurrent writes + a checkpoint every 5 commits, then
/// a "pod rebuild" (new node, empty local disk, lease expired) restores via S3: every acked
/// write must be present, with no sha mismatch.
#[test]
fn s3_restart_restores_after_concurrent_writes_and_checkpoints() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, _mock) = spawn_s3();
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = registry(store.clone(), "A", clock.clone(), tmp.path().join("a"), 5);
    let e = reg.get(D1, DB_A).unwrap();
    exec(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    let acked = Arc::new(Mutex::new(0i64));
    let mut hs = vec![];
    for t in 0..4 {
        let e = e.clone();
        let acked = acked.clone();
        hs.push(std::thread::spawn(move || {
            for i in 0..12 {
                if exec(&e, &format!("INSERT INTO t VALUES ({})", t * 100 + i)).is_ok() {
                    *acked.lock().unwrap() += 1;
                }
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let want = *acked.lock().unwrap();
    assert_eq!(want, 48, "all writes must ack on a healthy store");
    drop(e);
    drop(reg);

    clock.advance(20_000);
    let reg2 = registry(store, "B", clock, tmp.path().join("b"), 5);
    let e2 = reg2
        .get(D1, DB_A)
        .expect("restore after restart must succeed");
    assert_eq!(count(&e2), want);
}

/// Without a checkpoint the snapshot is snap-0 (uploaded from the live db.sqlite when the first segment is published)
/// -- restart restore must likewise pass the sha check.
#[test]
fn s3_restart_restores_snap0_baseline_plus_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, mock) = spawn_s3();
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = registry(store.clone(), "A", clock.clone(), tmp.path().join("a"), 0);
    let e = reg.get(D1, DB_A).unwrap();
    exec(&e, "CREATE TABLE t(v INTEGER)").unwrap();
    for i in 0..5 {
        exec(&e, &format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    assert!(mock
        .lock()
        .unwrap()
        .objects
        .keys()
        .any(|k| k.contains("snap-")));
    drop(e);
    drop(reg);
    clock.advance(20_000);
    let reg2 = registry(store, "B", clock, tmp.path().join("b"), 0);
    assert_eq!(count(&reg2.get(D1, DB_A).unwrap()), 5);
}

/// Corrupt snapshot: restore still fails closed (the check is not skipped), but enters bounded
/// backoff -- within the window N gets download only once; other dbs are unaffected; once the object is repaired and
/// the window passes it self-heals.
#[test]
fn corrupt_snapshot_fails_closed_with_bounded_backoff_and_isolates_other_dbs() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, mock) = spawn_s3();
    let clock = Arc::new(TestClock::new(1_000_000));
    let reg = registry(store.clone(), "A", clock.clone(), tmp.path().join("a"), 0);
    for id in [DB_A, DB_B] {
        let e = reg.get(D1, id).unwrap();
        exec(&e, "CREATE TABLE t(v INTEGER)").unwrap();
        exec(&e, "INSERT INTO t VALUES (1)").unwrap();
    }
    drop(reg);

    // Corrupt A's snapshot (length unchanged, content changed).
    let (snap_key, good) = {
        let m = mock.lock().unwrap();
        let k = m
            .objects
            .keys()
            .find(|k| k.contains(DB_A) && k.ends_with(".snap"))
            .cloned()
            .expect("snapshot exists");
        let v = m.objects[&k].0.clone();
        (k, v)
    };
    {
        let mut m = mock.lock().unwrap();
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xff;
        let etag = sha256_hex(&bad)[..32].to_string();
        m.objects.insert(snap_key.clone(), (bad, etag));
        m.gets.clear();
    }
    clock.advance(20_000);
    let reg2 = registry(store, "B", clock, tmp.path().join("b"), 0)
        .with_restore_backoff(Duration::from_millis(600), Duration::from_secs(5));
    let before = counter("restore");
    for _ in 0..50 {
        let err = reg2.get(D1, DB_A).err().unwrap();
        assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    }
    assert_eq!(
        mock.lock()
            .unwrap()
            .gets
            .get(&snap_key)
            .copied()
            .unwrap_or(0),
        1,
        "backoff must stop repeated snapshot downloads"
    );
    assert!(
        counter("restore") - before >= 1.0,
        "restore failure must be counted"
    );
    // Other dbs serve as usual.
    assert_eq!(count(&reg2.get(D1, DB_B).expect("other db unaffected")), 1);

    // Repair the object + backoff window passes -> self-heal.
    {
        let mut m = mock.lock().unwrap();
        let etag = sha256_hex(&good)[..32].to_string();
        m.objects.insert(snap_key, (good, etag));
    }
    std::thread::sleep(Duration::from_millis(700));
    assert_eq!(count(&reg2.get(D1, DB_A).expect("heals after repair")), 1);
}
