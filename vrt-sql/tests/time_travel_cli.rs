//! CLI smoke: `vrt-sql time-travel --at ... --json` against a real fs persistence root.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vrt_objstore::{FsStore, ObjectStore};
use vrt_sql::clock::{Clock, TestClock};
use vrt_sql::engine::{EngineRegistry, PersisterPair, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::D1;
use vrt_sql::persist::{PersistConfig, Persister};

const DB: &str = "abcdefghijklmnopqrstuvwxyz";

fn stmt(sql: &str) -> SqlStmt {
    SqlStmt {
        sql: sql.into(),
        params: None,
    }
}

fn write(e: &vrt_sql::engine::RusqliteEngine, sql: &str) {
    e.query_batch_sync(
        &[stmt(sql)],
        &TxnCtx {
            epoch: 1,
            deadline: Instant::now() + Duration::from_secs(30),
            dedup: None,
        },
        ResultsFormat::ArrayOfObjects,
    )
    .unwrap();
}

#[test]
fn cli_restores_to_time_and_reports_bookmarks() {
    let tmp = tempfile::tempdir().unwrap();
    let persist_root = tmp.path().join("persist");
    let db_root = tmp.path().join("dbs");
    std::fs::create_dir_all(&persist_root).unwrap();
    std::fs::create_dir_all(&db_root).unwrap();
    // fs persistence refuses a same-device root only in the `serve` startup path; this is a
    // dev/test store on one disk.
    let store: Arc<dyn ObjectStore> = Arc::new(FsStore::new(persist_root.clone()).unwrap());
    let clock = Arc::new(TestClock::new(1_000_000));
    let cfg = PersistConfig {
        lease_ms: 10_000,
        clock_skew_margin_ms: 500,
        holder: "cli-writer".into(),
        checkpoint_every_commits: 0,
        retention_ms: 60_000,
        snapshot_max_bytes: 1 << 30,
    };
    let d1 = Arc::new(Persister::new(
        store.clone(),
        cfg.clone(),
        clock.clone(),
        db_root.clone(),
        D1,
    ));
    let queue = Arc::new(Persister::new(
        store,
        cfg,
        clock.clone(),
        queue_root_for(&db_root),
        vrt_sql::paths::ResourceKind::Queue,
    ));
    let k2 = Arc::new(Persister::new(
        queue.store().clone(),
        queue.config().clone(),
        queue.clock().clone(),
        vrt_sql::paths::k2_root_for(&db_root),
        vrt_sql::paths::ResourceKind::K2,
    ));
    let reg = EngineRegistry::new(db_root.clone()).with_persisters(PersisterPair { d1, queue, k2 });
    let e = reg.get(D1, DB).unwrap();
    write(&e, "CREATE TABLE t(v INTEGER)");
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (1)");
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (2)");
    let target = clock.now_ms();
    clock.advance(10);
    write(&e, "INSERT INTO t VALUES (3)");
    e.release_lease();
    drop(e);
    drop(reg);

    let secret = tmp.path().join("bookmark");
    std::fs::write(&secret, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let config = tmp.path().join("time-travel.toml");
    std::fs::write(
        &config,
        format!(
            r#"
dbRoot = "{db_root}"
bookmarkSecretFile = "{secret}"

[persistence]
backend = "fs"

[persistence.fs]
root = "{persist_root}"
"#,
            db_root = db_root.display(),
            secret = secret.display(),
            persist_root = persist_root.display(),
        ),
    )
    .unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_vrt-sql"))
        .args([
            "time-travel",
            "--config",
            config.to_str().unwrap(),
            "--db",
            DB,
            "--at",
            &target.to_string(),
            "--json",
        ])
        .output()
        .expect("run vrt-sql time-travel");
    assert!(
        out.status.success(),
        "status {:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["databaseId"], DB);
    assert_eq!(v["resolvedLsn"], 3);
    assert_eq!(v["snapshotLsn"], 0);
    assert_eq!(v["segmentsReplayed"], 3);
    assert_eq!(v["newLsn"], 5); // CREATE + 3 INSERTs = lsn 4, restore publishes 5
    assert!(v["newBookmark"].as_str().unwrap().len() >= 32);

    // The restore is visible through a normal SQLite open of the published image.
    let db_file: PathBuf = vrt_sql::paths::derive_db_file(&db_root, DB).unwrap();
    let conn = rusqlite::Connection::open(db_file).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);
}
