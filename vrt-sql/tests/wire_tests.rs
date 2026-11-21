//! wire layer tests: the two error surfaces (R1), bookmark header, resultsFormat, dedup header.

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
    assert_eq!(r.status, StatusCode::OK); // R1: the D1 error surface is HTTP 200
    assert_eq!(r.body["success"], false);
    assert!(r.body["error"].as_str().unwrap().contains("nonexistent"));
    assert!(r.bookmark.is_none());
}

#[tokio::test]
async fn malformed_request_separate_surface() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    // not JSON
    let r = wire::handle_d1(&st, DBID, None, tctx(), b"not json").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.body["success"].is_null());
    // schema mismatch: sql is not a string
    let r = call(&st, &json!({"sql": 123})).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    // top-level scalar
    let r = call(&st, &json!(42)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    // illegal params element (bool) -> schema surface
    let r = call(&st, &json!({"sql": "SELECT ?", "params": [true]})).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn dump_endpoint_returns_sql_bytes_and_non_200_errors() {
    use tower::ServiceExt;
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    let app = wire::app(st.clone());
    // Seed a table through the normal query path.
    let r = call(&st, &json!({"sql": "CREATE TABLE t(a TEXT)"})).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = call(&st, &json!({"sql": "INSERT INTO t VALUES('x''y')"})).await;
    assert_eq!(r.status, StatusCode::OK);

    let req = axum::http::Request::builder()
        .method("POST")
        .uri(format!("/dbs/{DBID}/dump"))
        .body(axum::body::Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
        "application/octet-stream"
    );
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.starts_with("PRAGMA defer_foreign_keys=TRUE;"),
        "{text}"
    );
    assert!(text.contains("CREATE TABLE t(a TEXT);"), "{text}");
    assert!(
        text.contains("INSERT INTO \"t\" (\"a\") VALUES('x''y');"),
        "{text}"
    );

    // Non-200: d1-api only parses an error body when the dump status is not 200.
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/dbs/UPPERCASE/dump")
        .body(axum::body::Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_ne!(res.status(), StatusCode::OK);
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
    // after the second commit the bookmark increases monotonically
    let r2 = call(&st, &json!({"sql": "INSERT INTO t VALUES(1)"})).await;
    assert!(r2.bookmark.unwrap() > bm);
}

#[tokio::test]
async fn batch_and_single_shapes() {
    let tmp = tempfile::tempdir().unwrap();
    let st = state(&tmp);
    // array request -> array response
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
    // single object -> single-element array response
    let r = call(&st, &json!({"sql": "SELECT a FROM t"})).await;
    assert_eq!(r.body.as_array().unwrap().len(), 1);
    // mid-batch failure -> whole batch rolls back + first error
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
    assert_eq!(r.body[0]["results"][0]["c"], 1); // INSERT 2 rolled back
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
        (Some("bogus"), false), // illegal falls back
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
    wire::handle_d1(&st, DBID, None, mk(5), body.as_bytes()).await; // replay
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
    // illegal path -> {success:false} surface (not a crash / not 5xx)
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["success"], false);
}

#[test]
fn bookmark_header_name_matches_cf() {
    assert_eq!(BOOKMARK_HEADER, "x-cf-d1-session-commit-token");
}

// ---------- T35c: DROP endpoint (spawn_blocking + tombstone semantics) ----------

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
    // first write a row to create the directory
    let r = call(&st, &serde_json::json!({"sql": "CREATE TABLE t(x)"})).await;
    assert_eq!(r.status, 200);
    let app = wire::app(st.clone());
    let (code, body) = oneshot_post(app.clone(), &format!("/dbs/{db}/drop")).await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["success"], serde_json::json!(true));
    assert_eq!(body["dropped"], serde_json::json!(true));
    // after DROP, querying the same db -> tombstone fail closed
    let r = call(&st, &serde_json::json!({"sql": "SELECT 1"})).await;
    assert_eq!(
        r.body["error"].as_str().unwrap_or_default(),
        "database dropped"
    );
    // idempotent replay: the tombstone hits, dropped:false (nothing was deleted this time)
    let (code2, body2) = oneshot_post(app.clone(), &format!("/dbs/{db}/drop")).await;
    assert_eq!(code2, 200, "{body2}");
    assert_eq!(body2["success"], serde_json::json!(true));
    assert_eq!(body2["dropped"], serde_json::json!(false));
}
