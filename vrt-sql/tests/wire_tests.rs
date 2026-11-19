//! wire 层测试：两个错误面（R1）、bookmark 头、resultsFormat、dedup 头。

use axum::http::StatusCode;
use serde_json::json;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use vrt_sql::wire::{self, AppState, TrustedCallCtx, BOOKMARK_HEADER};

fn tctx() -> TrustedCallCtx {
    TrustedCallCtx::test(Instant::now() + Duration::from_secs(30))
}

const KEY: [u8; 32] = [3u8; 32];
const DBID: &str = "01jqqm3b4y6zq8f0v2w5x7k9ae";

fn state(tmp: &TempDir) -> AppState {
    AppState::new(tmp.path().join("dbs"), KEY)
}

async fn call(st: &AppState, body: &serde_json::Value) -> wire::WireResponse {
    wire::handle_d1(st, DBID, None, tctx(), body.to_string().as_bytes()).await
}

#[tokio::test]
async fn sql_error_surface_http200_success_false() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    let r = call(&st, &json!({"sql": "SELECT * FROM nonexistent"})).await;
    assert_eq!(r.status, StatusCode::OK); // R1：D1 错误面是 HTTP 200
    assert_eq!(r.body["success"], false);
    assert!(r.body["error"].as_str().unwrap().contains("nonexistent"));
    assert!(r.bookmark.is_none());
}

#[tokio::test]
async fn malformed_request_separate_surface() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    // 非 JSON
    let r = wire::handle_d1(&st, DBID, None, tctx(), b"not json").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.body["success"].is_null());
    // schema 不合：sql 非字符串
    let r = call(&st, &json!({"sql": 123})).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    // 顶层标量
    let r = call(&st, &json!(42)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    // params 元素非法（bool）→ schema 面
    let r = call(&st, &json!({"sql": "SELECT ?", "params": [true]})).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn success_carries_bookmark_header_value() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    let r = call(&st, &json!({"sql": "CREATE TABLE t(a)"})).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body[0]["success"], true);
    let bm = r.bookmark.unwrap();
    assert_eq!(bm.len(), 16 + 1 + 16 + 1 + 16, "{bm}");
    assert!(st.signer.verify(DBID, &bm).is_ok());
    // 第二个提交后 bookmark 单调递增
    let r2 = call(&st, &json!({"sql": "INSERT INTO t VALUES(1)"})).await;
    assert!(r2.bookmark.unwrap() > bm);
}

#[tokio::test]
async fn batch_and_single_shapes() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    // 数组请求 → 数组响应
    let r = call(
        &st,
        &json!([
            {"sql": "CREATE TABLE t(a)"},
            {"sql": "INSERT INTO t VALUES(1)", "params": null},
        ]),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body.as_array().unwrap().len(), 2);
    // 单对象 → 单元素数组响应
    let r = call(&st, &json!({"sql": "SELECT a FROM t"})).await;
    assert_eq!(r.body.as_array().unwrap().len(), 1);
    // batch 中途失败 → 整批回滚 + 首错
    let r = call(
        &st,
        &json!([
            {"sql": "INSERT INTO t VALUES(2)"},
            {"sql": "SELEC bad"},
        ]),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["success"], false);
    let r = call(&st, &json!({"sql": "SELECT COUNT(*) c FROM t"})).await;
    assert_eq!(r.body[0]["results"][0]["c"], 1); // INSERT 2 已回滚
}

#[tokio::test]
async fn results_format_param() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    call(&st, &json!({"sql": "CREATE TABLE t(a,b)"})).await;
    call(&st, &json!({"sql": "INSERT INTO t VALUES(1,'x')"})).await;
    for (fmt, expect_cols) in [
        (Some("ROWS_AND_COLUMNS"), true),
        (Some("ARRAY_OF_OBJECTS"), false),
        (Some("NONE"), false),
        (Some("bogus"), false), // 非法回落
        (None, false),
    ] {
        let r = wire::handle_d1(
            &st,
            DBID,
            fmt,
            tctx(),
            json!({"sql": "SELECT a,b FROM t"}).to_string().as_bytes(),
        )
        .await;
        assert_eq!(
            r.body[0]["results"]["columns"].is_array(),
            expect_cols,
            "{fmt:?}"
        );
    }
}

#[tokio::test]
async fn dedup_replay_via_trusted_ctx() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    call(&st, &json!({"sql": "CREATE TABLE t(a)"})).await;
    let mk = |seq| TrustedCallCtx {
        epoch: 1,
        seq: Some(seq),
        deadline: Instant::now() + Duration::from_secs(30),
    };
    let body = json!({"sql": "INSERT INTO t VALUES(1)"}).to_string();
    wire::handle_d1(&st, DBID, None, mk(5), body.as_bytes()).await;
    wire::handle_d1(&st, DBID, None, mk(5), body.as_bytes()).await; // 重放
    let r = call(&st, &json!({"sql": "SELECT COUNT(*) c FROM t"})).await;
    assert_eq!(r.body[0]["results"][0]["c"], 1);
}

#[tokio::test]
async fn invalid_db_id_fails_closed_via_engine_error() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    let r = wire::handle_d1(
        &st,
        "../etc/passwd",
        None,
        tctx(),
        json!({"sql": "SELECT 1"}).to_string().as_bytes(),
    )
    .await;
    // 路径非法 → {success:false} 面（非崩溃/非 5xx）
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["success"], false);
}

#[test]
fn bookmark_header_name_matches_cf() {
    assert_eq!(BOOKMARK_HEADER, "x-cf-d1-session-commit-token");
}

// ---------- T35c：DROP 端点（spawn_blocking + tombstone 语义） ----------

async fn oneshot_post(app: axum::Router, uri: &str) -> (u16, serde_json::Value) {
    use tower::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from("{}"))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status().as_u16();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[tokio::test]
async fn drop_endpoint_roundtrip_and_tombstone() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    let db = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    // 先写一行建出目录
    let r = call(&st, &serde_json::json!({"sql": "CREATE TABLE t(x)"})).await;
    assert_eq!(r.status, 200);
    let app = wire::app(st.clone());
    let (code, body) = oneshot_post(app.clone(), &format!("/dbs/{db}/drop")).await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["success"], serde_json::json!(true));
    assert_eq!(body["dropped"], serde_json::json!(true));
    // DROP 之后查询同一 db → tombstone fail closed
    let r = call(&st, &serde_json::json!({"sql": "SELECT 1"})).await;
    assert_eq!(
        r.body["error"].as_str().unwrap_or_default(),
        "database dropped"
    );
    // 幂等重放：tombstone 命中，dropped:false（本次未删到实体）
    let (code2, body2) = oneshot_post(app.clone(), &format!("/dbs/{db}/drop")).await;
    assert_eq!(code2, 200, "{body2}");
    assert_eq!(body2["success"], serde_json::json!(true));
    assert_eq!(body2["dropped"], serde_json::json!(false));
}
