//! `dump()` + the documented 50 MB query-result cap.
//!
//! The expected dump text is copied from workerd's own golden vectors
//! (`src/workerd/server/tests/d1/d1-export-test.js`, upstream commit
//! 7545be644952d71d76b904b37ae71b10c0ba8cad), which record what
//! Miniflare's `dumpSql` answers for the same fixture. Keeping the exact
//! bytes here means a Rust-side change cannot silently drift from the
//! format Cloudflare's runtime produces.

use std::time::{Duration, Instant};

use tempfile::TempDir;
use vrt_sql::engine::{
    EngineError, ResultsFormat, RusqliteEngine, SqlStmt, TxnCtx, D1_RESULT_MAX_BYTES,
    D1_RESULT_TOO_LARGE,
};

fn stmt(sql: &str) -> SqlStmt {
    SqlStmt {
        sql: sql.into(),
        params: None,
    }
}

fn ctx() -> TxnCtx {
    TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(60),
        dedup: None,
    }
}

fn open(tmp: &TempDir) -> RusqliteEngine {
    RusqliteEngine::open_file(&tmp.path().join("db.sqlite"), "db").unwrap()
}

fn batch(e: &RusqliteEngine, sqls: &[&str]) -> Result<(), EngineError> {
    let stmts: Vec<SqlStmt> = sqls.iter().map(|s| stmt(s)).collect();
    e.query_batch_sync(&stmts, &ctx(), ResultsFormat::ArrayOfObjects)
        .map(|_| ())
}

fn dump(e: &RusqliteEngine) -> String {
    String::from_utf8(e.dump_sync(&ctx()).unwrap()).unwrap()
}

const SELECT_ROWS: &str = "SELECT id, t, r, n, hex(b) AS h FROM plain ORDER BY id";

const FIXTURE: &[&str] = &[
    "CREATE TABLE plain (id INTEGER PRIMARY KEY, t TEXT, r REAL, b BLOB, n)",
    "CREATE TABLE \"Quoted \"\"T\"\"\" (id INTEGER PRIMARY KEY AUTOINCREMENT, \"select\" TEXT)",
    "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES plain(id) ON DELETE CASCADE)",
    "INSERT INTO plain VALUES (1, 'it''s', 1.5, X'00ff10', NULL)",
    "INSERT INTO plain VALUES (2, 'line1\nline2\rend', 9e999, X'', 'x')",
    "INSERT INTO plain VALUES (3, '', -9e999, NULL, 42)",
    "INSERT INTO \"Quoted \"\"T\"\"\" (\"select\") VALUES ('a'), ('b')",
    "INSERT INTO child VALUES (1, 1)",
    "CREATE INDEX plain_t ON plain (t)",
    "CREATE VIEW v AS SELECT id, t FROM plain",
    "CREATE TRIGGER trg AFTER INSERT ON child BEGIN UPDATE plain SET n = 1 WHERE id = new.p; END",
];

/// workerd golden vector `GOLDEN.all`, verbatim.
const GOLDEN: &[&str] = &[
    "PRAGMA defer_foreign_keys=TRUE;",
    "CREATE TABLE plain (id INTEGER PRIMARY KEY, t TEXT, r REAL, b BLOB, n);",
    "INSERT INTO \"plain\" (\"id\",\"t\",\"r\",\"b\",\"n\") VALUES(1,'it''s',1.5,X'00ff10',NULL);",
    "INSERT INTO \"plain\" (\"id\",\"t\",\"r\",\"b\",\"n\") VALUES(2,replace(replace('line1\\nline2\\rend','\\r',char(13)),'\\n',char(10)),9e999,X'','x');",
    "INSERT INTO \"plain\" (\"id\",\"t\",\"r\",\"b\",\"n\") VALUES(3,'',-9e999,NULL,42);",
    "CREATE TABLE IF NOT EXISTS \"Quoted \"\"T\"\"\" (id INTEGER PRIMARY KEY AUTOINCREMENT, \"select\" TEXT);",
    "INSERT INTO \"Quoted \"\"T\"\"\" (\"id\",\"select\") VALUES(1,'a');",
    "INSERT INTO \"Quoted \"\"T\"\"\" (\"id\",\"select\") VALUES(2,'b');",
    "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES plain(id) ON DELETE CASCADE);",
    "INSERT INTO \"child\" (\"id\",\"p\") VALUES(1,1);",
    "DELETE FROM sqlite_sequence;",
    "INSERT INTO \"sqlite_sequence\" (\"name\",\"seq\") VALUES('Quoted \"T\"',2);",
    "CREATE INDEX plain_t ON plain (t);",
    "CREATE TRIGGER trg AFTER INSERT ON child BEGIN UPDATE plain SET n = 1 WHERE id = new.p; END;",
    "CREATE VIEW v AS SELECT id, t FROM plain;",
];

#[test]
fn dump_matches_workerd_golden_vector() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, FIXTURE).unwrap();
    assert_eq!(dump(&e), GOLDEN.join("\n"));
}

#[test]
fn empty_database_dump_is_the_leading_pragma_only() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // The engine's internal ledger table must not leak into the export.
    assert_eq!(dump(&e), "PRAGMA defer_foreign_keys=TRUE;");
}

#[test]
fn dump_executes_back_into_a_fresh_database() {
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    batch(&e, FIXTURE).unwrap();
    let before = e
        .query_batch_sync(&[stmt(SELECT_ROWS)], &ctx(), ResultsFormat::RowsAndColumns)
        .unwrap();
    let script = dump(&e);

    // Drop and restore from the dump itself (each line is one exec statement,
    // matching how d1-api's exec() submits it).
    batch(
        &e,
        &[
            "DROP TABLE plain",
            "DROP TABLE child",
            "DROP TABLE \"Quoted \"\"T\"\"\"",
            "DROP VIEW v",
        ],
    )
    .unwrap();
    let restore: Vec<&str> = script
        .split('\n')
        .filter(|l| !l.is_empty() && *l != "PRAGMA defer_foreign_keys=TRUE;")
        .collect();
    batch(&e, &restore).unwrap();

    let after = e
        .query_batch_sync(&[stmt(SELECT_ROWS)], &ctx(), ResultsFormat::RowsAndColumns)
        .unwrap();
    assert_eq!(
        serde_json::to_value(&before[0].results).unwrap(),
        serde_json::to_value(&after[0].results).unwrap()
    );
}

#[test]
fn oversized_query_result_is_rejected_with_the_documented_text() {
    assert_eq!(D1_RESULT_MAX_BYTES, 50_000_000);
    let tmp = tempfile::tempdir().unwrap();
    let e = open(&tmp);
    // ~55 x 1,000,000 chars of text -> serialized result > 50 MB.
    let sql = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<55) \
               SELECT x, replace(hex(zeroblob(1000000)), '00', 'a') AS s FROM c";
    let err = e
        .query_batch_sync(&[stmt(sql)], &ctx(), ResultsFormat::ArrayOfObjects)
        .unwrap_err();
    assert_eq!(err.wire_message(), D1_RESULT_TOO_LARGE);
}
