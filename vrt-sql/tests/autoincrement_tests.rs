//! 2026-11-15: D1 AUTOINCREMENT (sqlite_sequence) authorization regression.
//!
//! Symptom: `CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, ...)` returned
//! `D1_ERROR: not authorized` -- SQLite internally creates `sqlite_sequence` for AUTOINCREMENT,
//! and that CREATE TABLE was rejected by the authorizer via the `sqlite_` prefix rule of `is_reserved_object`.
//! Cloudflare D1 supports AUTOINCREMENT, so it must be allowed,
//! and only the minimal actions SQLite needs to maintain sqlite_sequence itself.

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
    // AUTOINCREMENT does not reuse deleted ids: after deleting the max row the next id still increments
    batch(&e, &["DELETE FROM t WHERE id = 2"]).unwrap();
    batch(&e, &["INSERT INTO t(b) VALUES('c')"]).unwrap();
    let r = batch(&e, &["SELECT id FROM t ORDER BY id"]).unwrap();
    let s = dump(&r);
    assert!(
        s.contains(r#"[Object {"id": Number(1)}, Object {"id": Number(3)}]"#),
        "ids: {s}"
    );
    // Users can read sqlite_sequence (CF D1 behavior), seq=3
    let r = batch(&e, &["SELECT name, seq FROM sqlite_sequence"]).unwrap();
    assert!(dump(&r).contains('3'), "{}", dump(&r));
    // Users can DELETE sqlite_sequence rows (CF D1 behavior, resets the counter)
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
    // The table is still usable after the rejection
    batch(&e, &["INSERT INTO t(b) VALUES('ok')"]).unwrap();
}

#[test]
fn user_cannot_create_sqlite_sequence_directly() {
    // Without an AUTOINCREMENT table, users cannot hand-create sqlite_sequence
    // (otherwise they could create a same-named table with triggers/odd schema first).
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    assert!(batch(&e, &["CREATE TABLE sqlite_sequence(name,seq)"]).is_err());
    assert!(batch(&e, &["CREATE TEMP TABLE sqlite_sequence(name,seq)"]).is_err());
}
