//! 引擎行为测试（§3）：事务/meta/格式/BLOB/authorizer/限额/超时/路径/单写者/dedup。

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
    // 第二条失败 → 整批回滚：CREATE 也应不存在
    let r = batch(
        &e,
        &[
            "CREATE TABLE t(a)",
            "INSERT INTO t VALUES(1)",
            "SELECT * FROM nonexistent",
        ],
    );
    assert!(r.is_err());
    // 表不存在 = 回滚生效
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
    assert_eq!(sel.rows_written, 0); // SELECT 不写
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
    // NONE：与 miniflare 一致仍按对象数组返回
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
    // miniflare 过滤 `^\s+--`：缩进/换行后的行注释剥掉 → 无语句错
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
    // 串首即 `--`（无前置空白）不剥 → 到达 SQLite 报"无语句"类错误
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

// ---------- authorizer 禁项 ----------

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
    // 读白名单放行
    assert!(batch(&e, &["PRAGMA table_info(t)"]).is_ok());
    assert!(batch(&e, &["PRAGMA integrity_check"]).is_ok());
    // 写与引擎托管项拒
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
    // foreign_keys 读放行
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
        "SELECT pragma_table_info('t')", // pragma_*() 函数形态
        "SELECT pragma_journal_mode()",
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
}

#[test]
fn sqlite_master_write_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // authorizer 不拦 sqlite_master（内部 DDL 依赖）；由 SQLite 自身拒
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
    // T33：VACUUM 全禁（内部 Attach{''} 与用户 ATTACH 不可分）
    assert!(batch(&e, &["VACUUM INTO '/tmp/escape.db'"]).is_err());
    assert!(batch(&e, &["VACUUM"]).is_err());
    assert!(batch(&e, &["vacuum main"]).is_err());
}

// ---------- 限额与超时 ----------

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

// ---------- 路径/单写者 ----------

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
    // 同 (epoch,seq) 重放：不重复执行（行数应仍 1），返回原结果
    let r2 = e
        .query_batch_sync(&stmts, &c, ResultsFormat::ArrayOfObjects)
        .unwrap();
    // 重放返回存储的原始结果负载（meta 中的 duration 等动态字段除外）
    assert_eq!(r1[0].results, r2[0].results);
    assert_eq!(r1.len(), r2.len());
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(1));
    // 不同 seq → 真执行
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
    // T34b: 同 (epoch,seq) 不同负载 ⇒ seq 分配撞键（如 provider 重启
    // 回绕）——fail closed，绝不回放旧结果。
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
    // 冲突写未执行：表里仍只有第一行。
    let cnt = batch(&e, &["SELECT COUNT(*) c FROM t"]).unwrap();
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(1));
    // 同 (epoch,seq) 同负载 ⇒ 仍是合法重放（不报错、不重复执行）。
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
    // 业务失败（唯一约束）→ dedup 不应落账，同 seq 重试应重放执行再失败
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

// ---------- T33b S1：保留名 DDL / trigger 绕过 ----------

#[test]
fn reserved_ddl_on_vrt_objects_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &["CREATE TABLE t(a)"]).unwrap();
    for sql in [
        // trigger 体在 internal 窗口执行的绕过路径——建都不让建
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
    // sqlite_ 前缀（SQLite 自身也拒，叠防御纵深）
    assert!(batch(&e, &["CREATE TABLE sqlite_evil(a)"]).is_err());
    // CREATE VIEW 不解析 SELECT 体（SQLite 惰性授权）——视图可建
    // 但透读 _vrt_dedup 在 SELECT 时被 Read 拒（真正的安全边界）
    assert!(batch(&e, &["CREATE VIEW v AS SELECT * FROM _vrt_dedup"]).is_ok());
    assert!(batch(&e, &["SELECT * FROM v"]).is_err());
    // 正常 DDL 不受影响
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
    // 挂不上 trigger → dedup 写入路径没有可劫持点
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
    assert_eq!(r[0].results[0]["c"], serde_json::json!(1)); // 无 999
}

// ---------- T33b S2：函数白名单 ----------

#[test]
fn function_whitelist_matches_bundled_universe() {
    // 每个白名单函数都真实存在于 bundled 3.53.2
    let c = rusqlite::Connection::open_in_memory().unwrap();
    let mut st = c
        .prepare("SELECT DISTINCT name FROM pragma_function_list")
        .unwrap();
    let universe: std::collections::HashSet<String> = st
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .flatten()
        .collect();
    // 表值函数不出现在 function_list；白名单元素不应缺失
    let allow = FUNCTION_ALLOW_FOR_TEST;
    for f in allow {
        assert!(
            universe.contains(*f),
            "whitelist fn missing from bundled: {f}"
        );
    }
    // 全集中每个函数要么在白名单，要么在显式拒名单——无未评估函数
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

// 与 security.rs 的白名单保持同步（测试侧显式重列，防止误改漏检）
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
    // 白名单函数正常可用
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
    // 非白名单（含 bundled 中不存在的）一律拒
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

// ---------- T33b S3：信息泄漏 ----------

#[test]
fn info_leak_pragmas_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    for sql in [
        "PRAGMA database_list",   // 绝对路径
        "PRAGMA compile_options", // 构建细节
        "PRAGMA function_list",   // 构建细节
    ] {
        assert!(batch(&e, &[sql]).is_err(), "should deny: {sql}");
    }
    // sqlite_version() 放行：仅版本号、无路径/密钥，兼容探测常用
    assert!(batch(&e, &["SELECT sqlite_version()"]).is_ok());
    assert!(batch(&e, &["PRAGMA table_info(t)"]).is_ok());
}

// ---------- T33b S4：dedup epoch 一致 ----------

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
    assert_eq!(cnt[0].results[0]["c"], serde_json::json!(0)); // 未执行
}

// ---------- T35b：受限 DROP（数据面删除） ----------

#[test]
fn registry_drop_removes_db_dir_idempotent() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    // 打开引擎 → 目录/文件存在
    let e = reg.get(D1, db_id).unwrap();
    let leaf = vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .expect("leaf exists after open");
    assert!(leaf.join("db.sqlite").exists());
    drop(e);
    // DROP → true，目录消失
    assert!(reg.drop_db(D1, db_id).unwrap());
    assert!(!leaf.exists());
    // 幂等：再删 → false（不存在），不报错
    assert!(!reg.drop_db(D1, db_id).unwrap());
    // T35c：删除后 get() 永远 Dropped——tombstone 生效，目录不复活。
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
    // root 未被越权创建任何目录
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    // 从未存在的合法 id → Ok(false)
    assert!(!reg.drop_db(D1, "zzzzzzzzzzzzzzzzzzzzzzzz26").unwrap());
}

// ---------- T35c：DROP 并发安全（tombstone / 排空 / 防复活） ----------

/// 被 DROP 的 id：tombstone marker 落盘、目录进 .trash 后被删，任何
/// get()/open() 永远 Dropped，目录不会被惰性重建（复活防护）。
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
    // 持久 marker 已写
    assert!(vrt_sql::paths::marker_exists(&root, db_id).unwrap());
    // get() 与裸 open() 均 Dropped
    assert!(matches!(reg.get(D1, db_id), Err(EngineError::Dropped)));
    assert!(matches!(
        RusqliteEngine::open(&root, db_id),
        Err(EngineError::Dropped)
    ));
    // 目录没有复活
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// tombstone 跨进程重启：新 registry 扫盘预灌 marker + get() 冷路径
/// stat 兜底，重启后同样拒绝打开（持久化语义）。
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
    // 模拟进程重启：全新 registry（启动扫盘已把 marker 灌入 tombstone）
    let reg2 = EngineRegistry::new(root.clone());
    assert!(matches!(reg2.get(D1, db_id), Err(EngineError::Dropped)));
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// marker 先于一切写入：对从未存在的 id DROP 也留下 tombstone，
/// 之后该 id 无法被创建打开（id 不复用语义 fail closed）。
#[test]
fn drop_missing_db_still_tombstones() {
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = EngineRegistry::new(root.clone());
    let db_id = "zzzzzzzzzzzzzzzzzzzzzzzz26";
    assert!(!reg.drop_db(D1, db_id).unwrap()); // 未删到实体 → false
    assert!(matches!(reg.get(D1, db_id), Err(EngineError::Dropped)));
    // 但根下只有 marker 所在 shard 与 .trash，无 db leaf
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// 摘除的引擎持有人会话：DROP 之后用已持有 Arc 发起的新批次
/// 直接 Dropped——不存在"写成功但目录已被删"的假成功。
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
    // 持有的旧 Arc：新批次被拒（入口标志检查）
    assert!(matches!(
        batch(&e, &["INSERT INTO t VALUES (1)"]),
        Err(EngineError::Dropped)
    ));
}

/// DROP 与并发 get/query 交错：drop 完成后任何 get 必须失败；
/// 目录不得被重建；不允许出现"写成功但数据丢失"。
#[test]
fn drop_vs_concurrent_get_no_resurrection() {
    use std::sync::Arc;
    use vrt_sql::engine::EngineRegistry;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("dbs");
    std::fs::create_dir(&root).unwrap();
    let reg = Arc::new(EngineRegistry::new(root.clone()));
    let db_id = "01jqqm3b4y6zq8f0v2w5x7k9ae";

    // 多个 worker 持续 get+写；drop 在中途落下。
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
                        // DROP 之后绝不允许再拿到引擎或写成功
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
    // 再确认：目录不复活、get 永远拒
    assert!(matches!(reg.get(D1, db_id), Err(EngineError::Dropped)));
    assert!(vrt_sql::paths::derive_db_dir_readonly(&root, db_id)
        .unwrap()
        .is_none());
}

/// 排空等待不得持有 registry 锁：一个 db 的 drop（在途 Arc 迟迟不
/// 归还，走完整个 drain）期间，另一个 db 的 get+query 必须在有界
/// 时延内完成——验证锁不跨 drain/remove_dir_all。
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
    // victim 的 Arc 被外部持有 → drop_db 走满 drain 窗口
    let held = reg.get(D1, victim).unwrap();
    let reg2 = reg.clone();
    let dropper = std::thread::spawn(move || reg2.drop_db(D1, victim).unwrap());
    std::thread::sleep(Duration::from_millis(100)); // 确保 drop 进入 drain
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

/// 启动清扫：.trash 里的残骸（上次 drop 后崩溃）在 new() 时清除。
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
