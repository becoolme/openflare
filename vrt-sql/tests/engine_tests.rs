//! Engine behavior tests (section 3): transaction/meta/format/BLOB/authorizer/limits/timeout/path/single-writer/dedup.

use std::time::{Duration, Instant};
use vrt_sql::paths::ResourceKind::D1;

use tempfile::TempDir;
use vrt_sql::engine::{EngineError, ResultsFormat, RusqliteEngine, SqlStmt, TxnCtx};

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

fn open(tmp: &TempDir) -> RusqliteEngine {
    RusqliteEngine::open_file(&tmp.path().join("db.sqlite"), "db").unwrap()
}

fn batch(
    e: &RusqliteEngine,
    sqls: &[&str],
) -> Result<Vec<vrt_sql::engine::QueryResult>, EngineError> {
    let stmts: Vec<SqlStmt> = sqls.iter().map(|s| stmt(s)).collect();
    e.query_batch_sync(&stmts, &ctx(), ResultsFormat::ArrayOfObjects)
}

#[test]
fn single_txn_rolls_back_whole_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // Second statement fails -> whole batch rolls back: CREATE should not exist either
    let r = batch(
        &e,
        &[
            "CREATE TABLE t(a)",
            "INSERT INTO t VALUES(1)",
            "SELECT * FROM nonexistent",
        ],
    );
    assert!(r.is_err());
    // Table does not exist = rollback took effect
    let r2 = batch(&e, &["SELECT * FROM t"]);
    assert!(r2.is_err());
    assert!(r2.unwrap_err().wire_message().contains("t"));
}

#[test]
fn first_error_only_returned() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    let r = batch(&e, &["SELECT * FROM missing1", "SELECT * FROM missing2"]).unwrap_err();
    let msg = r.wire_message();
    assert!(
        msg.contains("missing1") && !msg.contains("missing2"),
        "{msg}"
    );
}

#[test]
fn meta_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    let r = batch(&e, &["INSERT INTO t VALUES(1)", "SELECT a FROM t"]).unwrap();
    let ins = &r[0].meta;
    assert_eq!(ins.changes, 1);
    assert_eq!(ins.last_row_id, 1);
    assert!(ins.changed_db);
    assert!(ins.size_after > 0);
    let sel = &r[1].meta;
    assert_eq!(sel.rows_read, 1);
    assert_eq!(sel.rows_written, 0); // SELECT does not write
    assert!(!sel.changed_db);
}

#[test]
fn results_format_variants() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a,b)", "INSERT INTO t VALUES(1,'x')"]).unwrap();
    let stmts = vec![stmt("SELECT a,b FROM t")];
    let c = ctx();
    let r = e
        .query_batch_sync(&stmts, &c, ResultsFormat::RowsAndColumns)
        .unwrap();
    let v = &r[0].results;
    assert_eq!(v["columns"], serde_json::json!(["a", "b"]));
    assert_eq!(v["rows"], serde_json::json!([[1, "x"]]));
    let r = e
        .query_batch_sync(&stmts, &c, ResultsFormat::ArrayOfObjects)
        .unwrap();
    assert_eq!(r[0].results, serde_json::json!([{ "a": 1, "b": "x" }]));
    // NONE: same as miniflare, still returned as an array of objects
    let r = e.query_batch_sync(&stmts, &c, ResultsFormat::None).unwrap();
    assert!(r[0].results.is_array());
}

#[test]
fn results_format_invalid_falls_back() {
    assert_eq!(ResultsFormat::parse(None), ResultsFormat::ArrayOfObjects);
    assert_eq!(
        ResultsFormat::parse(Some("bogus")),
        ResultsFormat::ArrayOfObjects
    );
    assert_eq!(
        ResultsFormat::parse(Some("")),
        ResultsFormat::ArrayOfObjects
    );
}

#[test]
fn empty_and_comment_only_batches_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // miniflare filters `^\s+--`: line comments after indentation/newline are stripped -> no "no statement" error
    for sql in ["", "   ", "  -- c", "  -- c\n   -- d"] {
        let r = batch(&e, &[sql]);
        assert!(
            matches!(
                r.unwrap_err(),
                EngineError::Message(m) if m == "No SQL statements detected."
            ),
            "{sql}"
        );
    }
    // `--` at the very start of the string (no leading whitespace) is not stripped -> reaches SQLite, which reports a "no statement"-class error
    assert!(batch(&e, &["-- comment"]).is_err());
    assert!(batch(&e, &["-- a\n-- b"]).is_err());
}

#[test]
fn blob_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(b BLOB)"]).unwrap();
    let stmts = vec![SqlStmt {
        sql: "INSERT INTO t VALUES(?)".into(),
        params: Some(vec![vrt_sql::engine::SqlValue::Blob(vec![0, 1, 2, 255])]),
    }];
    e.query_batch_sync(&stmts, &ctx(), ResultsFormat::ArrayOfObjects)
        .unwrap();
    let r = batch(&e, &["SELECT b FROM t"]).unwrap();
    assert_eq!(r[0].results, serde_json::json!([{ "b": [0, 1, 2, 255] }]));
}

#[test]
fn big_int_and_float() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    let stmts = vec![stmt(
        "SELECT 9223372036854775807 as big, 1.5e300 as f, -0.0 as z",
    )];
    let r = e
        .query_batch_sync(&stmts, &ctx(), ResultsFormat::ArrayOfObjects)
        .unwrap();
    assert_eq!(
        r[0].results[0]["big"],
        serde_json::json!(9223372036854775807i64)
    );
    assert!(r[0].results[0]["f"].as_f64().unwrap() > 1e299);
}

// ---------- authorizer deny items ----------

#[test]
fn attach_and_variants_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    for sql in [
        "ATTACH DATABASE 'x.db' AS x",
        "ATTACH ':memory:' AS x",
        "attach 'file:victim?mode=ro' as v",
        "DETACH DATABASE main",
        "ATTACH 't2' as t2; CREATE TABLE t2.evil(a)",
    ] {
        let r = batch(&e, &[sql]);
        assert!(r.is_err(), "should deny: {sql}");
    }
}

#[test]
fn pragma_restrictions() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // read allowlist passes
    assert!(batch(&e, &["PRAGMA table_info(t)"]).is_ok());
    assert!(batch(&e, &["PRAGMA integrity_check"]).is_ok());
    // writes and engine-managed items rejected
    for sql in [
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=OFF",
        "PRAGMA foreign_keys=ON",
        "PRAGMA cache_size=1000",
        "PRAGMA wal_checkpoint",
        "PRAGMA miniflare_d1_export('/tmp/x', '', '')",
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
    // foreign_keys read allowed
    assert!(batch(&e, &["PRAGMA foreign_keys"]).is_ok());
}

#[test]
fn function_denylist() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    for sql in [
        "SELECT load_extension('/tmp/evil')",
        "SELECT fts3_tokenizer('simple')",
        "SELECT sqlite_dbpage(1)",
        "SELECT pragma_table_info('t')", // pragma_*() function form
        "SELECT pragma_journal_mode()",
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
}

#[test]
fn sqlite_master_write_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // the authorizer does not block sqlite_master (internal DDL depends on it); rejected by SQLite itself
    assert!(batch(&e, &["INSERT INTO sqlite_master VALUES('x','y','z',1,'s')"]).is_err());
    assert!(batch(&e, &["SELECT name FROM sqlite_master"]).is_ok());
}

#[test]
fn dedup_table_invisible_to_users() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    assert!(batch(&e, &["SELECT * FROM _vrt_dedup"]).is_err());
    assert!(batch(&e, &["INSERT INTO _vrt_dedup VALUES(1,1,'{}',0)"]).is_err());
    assert!(batch(&e, &["DROP TABLE _vrt_dedup"]).is_err());
}

#[test]
fn vacuum_all_forms_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // T33: VACUUM entirely banned (internal Attach{''} is indistinguishable from the user's ATTACH)
    assert!(batch(&e, &["VACUUM INTO '/tmp/escape.db'"]).is_err());
    assert!(batch(&e, &["VACUUM"]).is_err());
    assert!(batch(&e, &["vacuum main"]).is_err());
}

// ---------- limits and timeout ----------

#[test]
fn oversized_sql_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    let sql = format!("SELECT '{}'", "x".repeat(200 * 1024));
    assert!(batch(&e, &[&sql]).is_err());
}

#[test]
fn too_many_params_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    let placeholders = (0..101).map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!("SELECT {placeholders}");
    assert!(batch(&e, &[&sql]).is_err());
}

#[test]
fn infinite_cte_interrupted() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    let c = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_millis(500),
        dedup: None,
    };
    let t0 = Instant::now();
    let r = e.query_batch_sync(
        &[stmt(
            "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT * FROM r",
        )],
        &c,
        ResultsFormat::ArrayOfObjects,
    );
    assert!(r.is_err());
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "timeout took too long"
    );
}

// ---------- path/single-writer ----------

#[test]
fn invalid_db_id_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    for bad in ["../x", "a", "A".repeat(26).as_str(), &"b".repeat(27)] {
        assert!(vrt_sql::paths::derive_db_file(tmp.path(), bad).is_err());
    }
}

#[test]
fn double_open_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let f = tmp.path().join("db.sqlite");
    let _e1 = RusqliteEngine::open_file(&f, "db").unwrap();
    assert!(matches!(
        RusqliteEngine::open_file(&f, "db"),
        Err(EngineError::AlreadyOpen)
    ));
}

// ---------- dedup ----------

#[test]
fn dedup_replay_returns_stored_result() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    let stmts = vec![stmt("INSERT INTO t VALUES(9)")];
    let c = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(5),
        dedup: Some((1, 42)),
    };
    let r1 = e
        .query_batch_sync(&stmts, &c, ResultsFormat::ArrayOfObjects)
        .unwrap();
    // Replay with the same (epoch,seq): not executed again (row count should still be 1), original result returned
    let r2 = e
        .query_batch_sync(&stmts, &c, ResultsFormat::ArrayOfObjects)
        .unwrap();
    // The replay returns the stored original result payload (except dynamic fields such as duration in meta)
    assert_eq!(r1[0].results, r2[0].results);
    assert_eq!(r1.len(), r2.len());
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(1));
    // Different seq -> really executes
    let c2 = TxnCtx {
        dedup: Some((1, 43)),
        ..c
    };
    e.query_batch_sync(&stmts, &c2, ResultsFormat::ArrayOfObjects)
        .unwrap();
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(2));
}

#[test]
fn dedup_collision_different_payload_rejected() {
    // T34b: same (epoch,seq) with a different payload => seq allocation collision (e.g. provider restart
    // wraparound) -- fail closed, never replay the old result.
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    let c = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(5),
        dedup: Some((1, 42)),
    };
    e.query_batch_sync(
        &[stmt("INSERT INTO t VALUES(9)")],
        &c,
        ResultsFormat::ArrayOfObjects,
    )
    .unwrap();
    let err = e
        .query_batch_sync(
            &[stmt("INSERT INTO t VALUES(10)")],
            &c,
            ResultsFormat::ArrayOfObjects,
        )
        .unwrap_err();
    assert!(err.to_string().contains("dedup key collision"), "{err}");
    // The conflicting write was not executed: the table still has only the first row.
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(1));
    // Same (epoch,seq) with the same payload => still a legitimate replay (no error, no re-execution).
    e.query_batch_sync(
        &[stmt("INSERT INTO t VALUES(9)")],
        &c,
        ResultsFormat::ArrayOfObjects,
    )
    .unwrap();
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(1));
}

#[test]
fn dedup_not_committed_on_business_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a UNIQUE)"]).unwrap();
    batch(&e, &["INSERT INTO t VALUES(1)"]).unwrap();
    // Business failure (unique constraint) -> dedup must not be recorded; retrying the same seq should re-execute and fail again
    let stmts = vec![stmt("INSERT INTO t VALUES(1)")];
    let c = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(5),
        dedup: Some((1, 77)),
    };
    assert!(e
        .query_batch_sync(&stmts, &c, ResultsFormat::ArrayOfObjects)
        .is_err());
    let c2 = TxnCtx {
        dedup: Some((1, 77)),
        ..c
    };
    assert!(e
        .query_batch_sync(&stmts, &c2, ResultsFormat::ArrayOfObjects)
        .is_err());
}

#[test]
fn commit_lsn_advances() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    let l0 = e.commit_lsn();
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    batch(&e, &["INSERT INTO t VALUES(1)"]).unwrap();
    assert_eq!(e.commit_lsn(), l0 + 2);
}

// ---------- T33b S1: reserved-name DDL / trigger bypass ----------

#[test]
fn reserved_ddl_on_vrt_objects_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    for sql in [
        // bypass path where the trigger body runs inside the internal window -- refuse to even create it
        "CREATE TRIGGER evil AFTER INSERT ON _vrt_dedup BEGIN UPDATE t SET a=0; END",
        "CREATE TRIGGER evil2 AFTER INSERT ON _vrt_dedup BEGIN SELECT 1; END",
        "CREATE INDEX i ON _vrt_dedup(seq)",
        "DROP TABLE _vrt_dedup",
        "ALTER TABLE _vrt_dedup RENAME TO x",
        "CREATE TABLE _vrt_evil(a)",
        "CREATE TABLE _vrt_x AS SELECT 1",
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
    // sqlite_ prefix (SQLite itself also rejects, stacking defense in depth)
    assert!(batch(&e, &["CREATE TABLE sqlite_evil(a)"]).is_err());
    // CREATE VIEW does not parse the SELECT body (SQLite lazy authorization) -- the view can be created
    // but reading through to _vrt_dedup is rejected with Read at SELECT time (the real security boundary)
    assert!(batch(&e, &["CREATE VIEW v AS SELECT * FROM _vrt_dedup"]).is_ok());
    assert!(batch(&e, &["SELECT * FROM v"]).is_err());
    // normal DDL is unaffected
    assert!(batch(&e, &["CREATE TABLE u2(a UNIQUE)"]).is_ok()); // autoindex
    assert!(batch(&e, &["CREATE INDEX iu ON t(a)"]).is_ok());
    assert!(batch(
        &e,
        &["CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END"]
    )
    .is_ok());
}

#[test]
fn dedup_write_safe_against_trigger_bypass() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    // no trigger can be attached -> the dedup write path has no hijack point
    assert!(batch(
        &e,
        &["CREATE TRIGGER e AFTER INSERT ON _vrt_dedup BEGIN INSERT INTO t VALUES(999); END"]
    )
    .is_err());
    let c = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(5),
        dedup: Some((1, 1)),
    };
    e.query_batch_sync(
        &[stmt("INSERT INTO t VALUES(1)")],
        &c,
        ResultsFormat::ArrayOfObjects,
    )
    .unwrap();
    let r = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(r[0].results[0]["c"], serde_json::json!(1)); // no 999
}

// ---------- T33b S2: function allowlist ----------

#[test]
fn function_whitelist_matches_bundled_universe() {
    // every allowlisted function really exists in bundled 3.53.2
    let c = rusqlite::Connection::open_in_memory().unwrap();
    let mut st = c
        .prepare("SELECT DISTINCT name FROM pragma_function_list")
        .unwrap();
    let universe: std::collections::HashSet<String> = st
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .flatten()
        .collect();
    // table-valued functions do not appear in function_list; no allowlist element should be missing
    let allow = FUNCTION_ALLOW_FOR_TEST;
    for f in allow {
        assert!(
            universe.contains(*f),
            "whitelist fn missing from bundled: {f}"
        );
    }
    // every function in the full set is either on the allowlist or on the explicit deny list -- no unevaluated functions
    for f in &universe {
        let allowed = allow.iter().any(|a| a.eq_ignore_ascii_case(f));
        let known_denied = vrt_sql::security::FUNCTION_KNOWN_DENY
            .iter()
            .any(|d| d.eq_ignore_ascii_case(f));
        assert!(
            allowed || known_denied,
            "unaccounted function in bundled: {f}"
        );
    }
}

// Kept in sync with the allowlist in security.rs (re-listed explicitly on the test side to prevent mistaken edits going unnoticed)
const FUNCTION_ALLOW_FOR_TEST: &[&str] = &[
    "abs",
    "changes",
    "char",
    "coalesce",
    "concat",
    "concat_ws",
    "format",
    "glob",
    "hex",
    "if",
    "ifnull",
    "iif",
    "instr",
    "last_insert_rowid",
    "length",
    "like",
    "likelihood",
    "likely",
    "lower",
    "ltrim",
    "max",
    "min",
    "nullif",
    "octet_length",
    "printf",
    "quote",
    "random",
    "randomblob",
    "replace",
    "round",
    "rtrim",
    "sign",
    "soundex",
    "sqlite_version",
    "substr",
    "substring",
    "subtype",
    "total_changes",
    "trim",
    "typeof",
    "unhex",
    "unicode",
    "unistr",
    "unistr_quote",
    "unlikely",
    "upper",
    "zeroblob",
    "->",
    "->>",
    "avg",
    "count",
    "group_concat",
    "string_agg",
    "sum",
    "total",
    "cume_dist",
    "dense_rank",
    "first_value",
    "lag",
    "last_value",
    "lead",
    "nth_value",
    "ntile",
    "percent_rank",
    "rank",
    "row_number",
    "date",
    "time",
    "datetime",
    "julianday",
    "unixepoch",
    "strftime",
    "timediff",
    "current_date",
    "current_time",
    "current_timestamp",
    "json",
    "json_array",
    "json_array_insert",
    "json_array_length",
    "json_error_position",
    "json_extract",
    "json_group_array",
    "json_group_object",
    "json_insert",
    "json_object",
    "json_patch",
    "json_pretty",
    "json_quote",
    "json_remove",
    "json_replace",
    "json_set",
    "json_type",
    "json_valid",
    "jsonb",
    "jsonb_array",
    "jsonb_array_insert",
    "jsonb_extract",
    "jsonb_group_array",
    "jsonb_group_object",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_patch",
    "jsonb_remove",
    "jsonb_replace",
    "jsonb_set",
];

#[test]
fn function_whitelist_smoke() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // allowlisted functions work normally
    assert!(batch(
        &e,
        &["SELECT typeof(1), upper('a'), json_extract('{\"x\":1}','$.x'), strftime('%Y','now')"]
    )
    .is_ok());
    assert!(batch(
        &e,
        &["SELECT unistr('a'), concat('a','b'), octet_length('x')"]
    )
    .is_ok());
    // non-allowlisted ones (including those absent from bundled) are always rejected
    for sql in [
        "SELECT load_extension('/x')",
        "SELECT fts3_tokenizer('simple')",
        "SELECT sqlite_offset(1)",
        "SELECT nonexistent_fn(1)",
        "SELECT sqlite_source_id()",
        "SELECT sqlite_compileoption_get(0)",
        "SELECT sqlite_log('x')",
        "SELECT bm25(t)",
        "SELECT matchinfo(t)",
        "SELECT rtreenode(1,'x')",
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
}

#[test]
fn oversized_value_hit_length_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    assert!(batch(&e, &["SELECT zeroblob(64*1024*1024)"]).is_err());
    assert!(batch(&e, &["SELECT randomblob(64*1024*1024)"]).is_err());
}

// ---------- T33b S3: information leak ----------

#[test]
fn info_leak_pragmas_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    for sql in [
        "PRAGMA database_list",   // absolute path
        "PRAGMA compile_options", // build details
        "PRAGMA function_list",   // build details
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
    // sqlite_version() allowed: version number only, no paths/keys, commonly used for compatibility probes
    assert!(batch(&e, &["SELECT sqlite_version()"]).is_ok());
    assert!(batch(&e, &["PRAGMA table_info(t)"]).is_ok());
}

// ---------- T33b S4: dedup epoch consistency ----------

#[test]
fn dedup_epoch_mismatch_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    // dedup.epoch != ctx.epoch → fail closed
    let c = TxnCtx {
        epoch: 2,
        deadline: Instant::now() + Duration::from_secs(5),
        dedup: Some((1, 5)),
    };
    let r = e.query_batch_sync(
        &[stmt("INSERT INTO t VALUES(1)")],
        &c,
        ResultsFormat::ArrayOfObjects,
    );
    assert!(r.is_err());
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(0)); // not executed
}

// ---------- T35b: restricted DROP (data-plane delete) ----------

#[test]
fn registry_drop_removes_db_dir_idempotent() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    // open the engine -> directory/file exists
    let e = reg.get(D1, db_id).unwrap();
    let leaf = vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .expect("leaf exists after open");
    assert!(leaf.join("db.sqlite").exists());
    drop(e);
    // DROP -> true, directory gone
    assert!(reg.drop_db(D1, db_id).unwrap());
    assert!(!leaf.exists());
    // idempotent: delete again -> false (does not exist), no error
    assert!(!reg.drop_db(D1, db_id).unwrap());
    // T35c: after deletion get() is always Dropped -- the tombstone takes effect, the directory does not come back.
    assert!(matches!(
        reg.get(D1, db_id),
        Err(vrt_sql::engine::EngineError::Dropped)
    ));
    assert!(!leaf.exists());
}

#[test]
fn registry_drop_rejects_bad_id_and_missing() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    for bad in [
        "../escape",
        "a/b",
        "short",
        &"A".repeat(26),
        &"x".repeat(27),
    ] {
        assert!(reg.drop_db(D1, bad).is_err(), "{bad} must fail");
    }
    // root had no directories created outside its authority
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    // a legal id that never existed -> Ok(false)
    assert!(!reg.drop_db(D1, "zzzzzzzzzzzzzzzzzzzzzzzz26").unwrap());
}

// ---------- T35c: DROP concurrency safety (tombstone / drain / anti-resurrection) ----------

/// A dropped id: the tombstone marker is persisted, the directory goes into .trash and is then deleted, and any
/// get()/open() is Dropped forever; the directory is never lazily rebuilt (resurrection protection).
#[test]
fn drop_tombstone_blocks_reopen_and_resurrection() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    let _e = reg.get(D1, db_id).unwrap();
    assert!(reg.drop_db(D1, db_id).unwrap());
    // persistent marker is written
    assert!(vrt_sql::paths::marker_exists(&root, db_id).unwrap());
    // both get() and a bare open() are Dropped
    assert!(matches!(reg.get(D1, db_id), Err(EngineError::Dropped)));
    assert!(matches!(
        RusqliteEngine::open(&root, db_id),
        Err(EngineError::Dropped)
    ));
    // the directory did not come back
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// tombstone across a process restart: a new registry scans the disk to pre-fill markers + the get() cold path's
/// stat backstop, so opening is likewise refused after the restart (persistence semantics).
#[test]
fn drop_tombstone_survives_registry_restart() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    {
        let reg = EngineRegistry::new(root.clone());
        let _e = reg.get(D1, db_id).unwrap();
        assert!(reg.drop_db(D1, db_id).unwrap());
    }
    // simulate a process restart: a brand-new registry (the startup scan already fed the markers into the tombstone set)
    let reg2 = EngineRegistry::new(root.clone());
    assert!(matches!(reg2.get(D1, db_id), Err(EngineError::Dropped)));
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// marker precedes all writes: DROP on an id that never existed also leaves a tombstone,
/// after which that id can never be created/opened (ids are never reused; fail closed).
#[test]
fn drop_missing_db_still_tombstones() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    let db_id = "zzzzzzzzzzzzzzzzzzzzzzzz26";
    assert!(!reg.drop_db(D1, db_id).unwrap()); // no entity deleted -> false
    assert!(matches!(reg.get(D1, db_id), Err(EngineError::Dropped)));
    // but under the root only the marker's shard and .trash exist, no db leaf
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// Session of a detached engine's holder: after DROP, a new batch issued with the already-held Arc
/// is directly Dropped -- there is no false success of "write succeeded but the directory was deleted".
#[test]
fn dropped_engine_rejects_new_batches() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    let e = reg.get(D1, db_id).unwrap();
    batch(&e, &["CREATE TABLE t(x)"]).unwrap();
    assert!(reg.drop_db(D1, db_id).unwrap());
    // the previously held Arc: the new batch is rejected (entry flag check)
    assert!(matches!(
        batch(&e, &["INSERT INTO t VALUES (1)"]),
        Err(EngineError::Dropped)
    ));
}

/// DROP interleaved with concurrent get/query: after the drop completes any get must fail;
/// the directory must not be rebuilt; no "write succeeded but data lost" is allowed.
#[test]
fn drop_vs_concurrent_get_no_resurrection() {
    use std::sync::Arc;
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = Arc::new(EngineRegistry::new(root.clone()));
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";

    // multiple workers keep doing get+write; the drop lands midway.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let reg = reg.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            let mut post_drop_success = 0u32;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                // Tombstones are monotonic: sample BEFORE get(). A get()
                // that started after the tombstone was visible must never
                // succeed. Sampling after get() raced (get linearized
                // before drop, tombstone observed later) and flaked in CI.
                let dropped_before_get = reg.state_tombstoned_for_test(D1, db_id);
                match reg.get(D1, db_id) {
                    Ok(e) => {
                        // after DROP, getting an engine or a successful write is never allowed again
                        if dropped_before_get {
                            post_drop_success += 1;
                        }
                        let _ = batch(
                            &e,
                            &[
                                "CREATE TABLE IF NOT EXISTS t(x)",
                                "INSERT INTO t VALUES (1)",
                            ],
                        );
                    }
                    Err(EngineError::Dropped) => {}
                    Err(_) => {}
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            post_drop_success
        }));
    }
    std::thread::sleep(Duration::from_millis(50));
    assert!(reg.drop_db(D1, db_id).unwrap());
    std::thread::sleep(Duration::from_millis(30));
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut violations = 0;
    for h in handles {
        violations += h.join().unwrap();
    }
    assert_eq!(violations, 0, "get() returned a live engine after drop");
    // confirm again: the directory does not come back, get is refused forever
    assert!(matches!(reg.get(D1, db_id), Err(EngineError::Dropped)));
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// The drain wait must not hold the registry lock: while one db's drop (the in-flight Arc is slow
/// to return, running the whole drain) is in progress, another db's get+query must finish within a bounded
/// time -- verifying the lock is not held across drain/remove_dir_all.
#[test]
fn drop_drain_does_not_starve_other_dbs() {
    use std::sync::Arc;
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = Arc::new(EngineRegistry::new(root.clone()));
    let victim = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    let other = "aaaaaaaaaaaaaaaaaaaaaaaaaa";
    // victim's Arc is held externally -> drop_db runs the full drain window
    let held = reg.get(D1, victim).unwrap();
    let reg2 = reg.clone();
    let dropper = std::thread::spawn(move || reg2.drop_db(D1, victim).unwrap());
    std::thread::sleep(Duration::from_millis(100)); // make sure drop enters the drain
    let t0 = Instant::now();
    let e = reg.get(D1, other).unwrap();
    batch(&e, &["CREATE TABLE t(x)", "INSERT INTO t VALUES (1)"]).unwrap();
    let latency = t0.elapsed();
    assert!(
        latency < Duration::from_secs(5),
        "other-db query starved by drain: {latency:?}"
    );
    drop(held);
    assert!(dropper.join().unwrap());
}

/// Startup sweep: debris in .trash (a crash after the last drop) is cleared in new().
#[test]
fn registry_new_sweeps_stale_trash() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let stale = root.join(".trash").join("deadbeefdeadbeefdeadbeef00-1");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("db.sqlite"), b"x").unwrap();
    let _reg = EngineRegistry::new(root.clone());
    assert!(!stale.exists());
}
