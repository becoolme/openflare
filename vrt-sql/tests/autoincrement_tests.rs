//! 2026-11-15：D1 AUTOINCREMENT（sqlite_sequence）授权回归。
//!
//! 现象：`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, ...)` 返回
//! `D1_ERROR: not authorized`——SQLite 为 AUTOINCREMENT 内部创建
//! `sqlite_sequence`，该 CREATE TABLE 经 authorizer 被 `is_reserved_object`
//! 的 `sqlite_` 前缀规则拒绝。Cloudflare D1 支持 AUTOINCREMENT，必须放行，
//! 且只放行 SQLite 自身维护 sqlite_sequence 所需的最小动作。

use std::time::{Duration, Instant};

use tempfile::TempDir;
use vrt_sql::engine::{EngineError, QueryResult, ResultsFormat, RusqliteEngine, SqlStmt, TxnCtx};

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

fn batch(e: &RusqliteEngine, sqls: &[&str]) -> Result<Vec<QueryResult>, EngineError> {
    let stmts: Vec<SqlStmt> = sqls.iter().map(|s| stmt(s)).collect();
    e.query_batch_sync(&stmts, &ctx(), ResultsFormat::ArrayOfObjects)
}

fn dump(r: &[QueryResult]) -> String {
    format!("{:?}", r.last().unwrap())
}

const CREATE_T: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT)";

#[test]
fn autoincrement_create_and_ids_increment() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &[CREATE_T]).expect("AUTOINCREMENT CREATE TABLE must be allowed (CF D1 supports it)");
    batch(&e, &["INSERT INTO t(b) VALUES('a')", "INSERT INTO t(b) VALUES('b')"]).unwrap();
    // AUTOINCREMENT 不复用已删 id：删最大行后下一个仍递增
    batch(&e, &["DELETE FROM t WHERE id = 2"]).unwrap();
    batch(&e, &["INSERT INTO t(b) VALUES('c')"]).unwrap();
    let r = batch(&e, &["SELECT id FROM t ORDER BY id"]).unwrap();
    let s = dump(&r);
    assert!(
        s.contains(r#"[Object {"id": Number(1)}, Object {"id": Number(3)}]"#),
        "ids: {s}"
    );
    // 用户可读 sqlite_sequence（CF D1 行为），seq=3
    let r = batch(&e, &["SELECT name, seq FROM sqlite_sequence"]).unwrap();
    assert!(dump(&r).contains('3'), "{}", dump(&r));
    // 用户可 DELETE sqlite_sequence 行（CF D1 行为，重置计数）
    batch(&e, &["DELETE FROM sqlite_sequence WHERE name='t'"]).unwrap();
}

#[test]
fn autoincrement_ids_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let e = open(&tmp);
        batch(&e, &[CREATE_T]).unwrap();
        batch(&e, &["INSERT INTO t(b) VALUES('a')", "INSERT INTO t(b) VALUES('b')"]).unwrap();
        batch(&e, &["DELETE FROM t"]).unwrap();
    }
    let e = open(&tmp);
    batch(&e, &["INSERT INTO t(b) VALUES('c')"]).unwrap();
    let r = batch(&e, &["SELECT id FROM t"]).unwrap();
    assert!(dump(&r).contains('3'), "id must continue after reopen: {}", dump(&r));
}

#[test]
fn sqlite_internal_objects_still_denied_to_users() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, &[CREATE_T]).unwrap();
    for sql in [
        "CREATE TABLE sqlite_foo(a)",
        "CREATE TABLE sqlite_sequence2(a)",
        "DROP TABLE sqlite_sequence",
        "ALTER TABLE sqlite_sequence RENAME TO x",
        "ALTER TABLE sqlite_sequence ADD COLUMN c",
        "CREATE INDEX i ON sqlite_sequence(name)",
        "CREATE TRIGGER trg AFTER INSERT ON sqlite_sequence BEGIN SELECT 1; END",
        "CREATE VIEW sqlite_v AS SELECT 1",
        "INSERT INTO sqlite_master VALUES('x','y','z',1,'s')",
        "UPDATE sqlite_master SET name='q'",
        "DELETE FROM sqlite_master",
        "SELECT * FROM sqlite_dbpage",
    ] {
        assert!(batch(&e, &[sql]).is_err(), "must stay denied: {sql}");
    }
    // 拒绝后表仍可用
    batch(&e, &["INSERT INTO t(b) VALUES('ok')"]).unwrap();
}

#[test]
fn user_cannot_create_sqlite_sequence_directly() {
    // 没有 AUTOINCREMENT 表时，用户不能手工建 sqlite_sequence
    // （否则可抢先建出带触发器/异常 schema 的同名表）。
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    assert!(batch(&e, &["CREATE TABLE sqlite_sequence(name,seq)"]).is_err());
    assert!(batch(&e, &["CREATE TEMP TABLE sqlite_sequence(name,seq)"]).is_err());
}
