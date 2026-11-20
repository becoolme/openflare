//! RV-R68: cross-language wire contract fixtures -- real vrt-sql output.
//!
//! Every fixture is REAL serialization / real handler output (no hand-written mocks):
//! - successful EXECUTE -> the bare-array body of `handle_d1`;
//! - SQL error -> the `{success:false,error}` of `handle_d1`;
//! - fenced -> `error_body(EngineError::Fenced)` (the real error surface);
//! - unavailable -> the 503 typed body of `handle_d1` after `begin_shutdown`.
//!
//! `UPDATE_FIXTURES=1` rewrites `server/src/test/fixtures/vrt-wire/`;
//! otherwise compare file by file against the committed fixtures (value-level equality, tolerating formatting differences).

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use vrt_sql::engine::EngineError;
use vrt_sql::wire::{self, AppState, TrustedCallCtx};

const KEY: [u8; 32] = [3u8; 32];
const DBID: &str = "01jqqm3b4y6zq8f0v2w5x7k9ae";

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures/vrt-wire")
}

/// Normalize non-deterministic fields: the engine's `meta.duration` is real elapsed time and differs on every run
/// -- compare after recursively zeroing it; shape/keys/other values are still locked field by field.
fn deterministic(mut v: Value) -> Value {
    match &mut v {
        Value::Object(m) => {
            for (k, val) in m.iter_mut() {
                if k == "duration" && val.is_number() {
                    *val = json!(0);
                } else {
                    *val = deterministic(val.clone());
                }
            }
            v
        }
        Value::Array(a) => Value::Array(a.drain(..).map(deterministic).collect()),
        _ => v,
    }
}

/// UPDATE mode writes the files; otherwise read the committed files for value-level comparison, and any drift fails.
fn check(name: &str, actual: Value) {
    let actual = deterministic(actual);
    let path = fixture_dir().join(name);
    if std::env::var("UPDATE_FIXTURES").is_ok() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&actual).unwrap()),
        )
        .unwrap();
        return;
    }
    let committed: Value = serde_json::from_str(
        &std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("fixture {name} missing — run UPDATE_FIXTURES=1 cargo test -p vrt-sql --test wire_fixtures")),
    )
    .unwrap();
    assert_eq!(committed, actual, "wire fixture {name} drifted");
}

fn tctx() -> TrustedCallCtx {
    TrustedCallCtx::test(Instant::now() + Duration::from_secs(30))
}

async fn call(st: &AppState, body: &serde_json::Value) -> wire::WireResponse {
    wire::handle_d1(st, DBID, None, tctx(), body.to_string().as_bytes()).await
}

#[tokio::test]
async fn d1_wire_fixtures() {
    let tmp = tempfile::tempdir().unwrap();
    let st = AppState::new(tmp.path().join("dbs"), KEY);

    // Single-statement success: a bare Vec<QueryResult> array (exactly the shape the control-plane strict
    // schema rejects and cell must normalize into {results:[...]}).
    let r = call(&st, &json!({"sql": "CREATE TABLE t(a)"})).await;
    assert_eq!(r.status, 200);
    assert!(r.body.is_array());
    check("d1-wire-execute.json", r.body);

    // Multi-statement batch: likewise a bare array, per statement {success,results,meta}.
    let r = call(
        &st,
        &json!([
            {"sql": "INSERT INTO t VALUES(1)"},
            {"sql": "SELECT a FROM t"},
        ]),
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body.as_array().unwrap().len(), 2);
    check("d1-wire-execute-batch.json", r.body);

    // SQL error: HTTP 200 + {success:false,error} (no code -- a statement-level
    // error is part of the statement result, not an engine fault).
    let r = call(&st, &json!({"sql": "SELEC bad"})).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["success"], false);
    check("d1-wire-sql-error.json", r.body);

    // Fenced: HTTP 200 + {success:false,error,code:"VRT_FENCED",
    // retryable:true} (error_body is the real on-the-wire serializer).
    let e = EngineError::Fenced(7);
    check(
        "d1-wire-fenced.json",
        json!({ "status": wire::error_status(&e).as_u16(), "body": wire::error_body(&e) }),
    );

    // Unavailable: after begin_shutdown handle_d1 -> 503 + typed body
    // (real handler, end to end).
    st.registry.begin_shutdown();
    let r = call(&st, &json!({"sql": "SELECT 1"})).await;
    assert_eq!(r.status, 503);
    assert_eq!(r.body["code"], "VRT_UNAVAILABLE");
    check(
        "d1-wire-unavailable.json",
        json!({ "status": r.status.as_u16(), "body": r.body }),
    );
}
