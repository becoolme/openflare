//! RV-R68：跨语言 wire 契约 fixture —— vrt-sql 真实输出。
//!
//! 每个 fixture 都是 REAL 序列化/真实 handler 输出（不走手写 mock）：
//! - 成功 EXECUTE → `handle_d1` 的裸数组 body；
//! - SQL 错误 → `handle_d1` 的 `{success:false,error}`；
//! - fenced → `error_body(EngineError::Fenced)`（真实错误面）；
//! - unavailable → `begin_shutdown` 后 `handle_d1` 的 503 typed body。
//!
//! `UPDATE_FIXTURES=1` 重写 `server/src/test/fixtures/vrt-wire/`；
//! 否则逐文件与已提交 fixture 比对（值级相等，容忍格式化差异）。

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

/// 非确定性字段归一：引擎 `meta.duration` 是真实耗时，每次运行
/// 不同——递归归零后比对，形状/键/其余值仍逐字段锁定。
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

/// UPDATE 模式写文件；否则读已提交文件做值级比对，漂移即失败。
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

    // 单语句成功：裸 Vec<QueryResult> 数组（这正是控制面 strict
    // schema 拒绝、cell 必须规范化成 {results:[...]} 的形状）。
    let r = call(&st, &json!({"sql": "CREATE TABLE t(a)"})).await;
    assert_eq!(r.status, 200);
    assert!(r.body.is_array());
    check("d1-wire-execute.json", r.body);

    // 多语句 batch：同样裸数组，逐语句 {success,results,meta}。
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

    // SQL 错误：HTTP 200 + {success:false,error}（无 code —— 语句级
    // 错误是语句结果的一部分，不是引擎故障）。
    let r = call(&st, &json!({"sql": "SELEC bad"})).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body["success"], false);
    check("d1-wire-sql-error.json", r.body);

    // Fenced：HTTP 200 + {success:false,error,code:"VRT_FENCED",
    // retryable:true}（error_body 是真实线上序列化器）。
    let e = EngineError::Fenced(7);
    check(
        "d1-wire-fenced.json",
        json!({ "status": wire::error_status(&e).as_u16(), "body": wire::error_body(&e) }),
    );

    // Unavailable：begin_shutdown 后 handle_d1 → 503 + typed body
    // （真实 handler 端到端）。
    st.registry.begin_shutdown();
    let r = call(&st, &json!({"sql": "SELECT 1"})).await;
    assert_eq!(r.status, 503);
    assert_eq!(r.body["code"], "VRT_UNAVAILABLE");
    check(
        "d1-wire-unavailable.json",
        json!({ "status": r.status.as_u16(), "body": r.body }),
    );
}
