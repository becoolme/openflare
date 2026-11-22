//! Read-only replica wire tests: query serving, bookmark watermark, stale/first-primary routing
//! signals, write rejection and the internal status endpoint.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceExt;
use vrt_objstore::{MemoryStore, ObjectStore};
use vrt_sql::bookmark::BookmarkSigner;
use vrt_sql::clock::{Clock, TestClock};
use vrt_sql::engine::{EngineRegistry, PersisterPair, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::follower::Follower;
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::D1;
use vrt_sql::persist::{PersistConfig, Persister};
use vrt_sql::replica::{self, ReplicaState};
use vrt_sql::service::{ServiceAuth, AUTH_HEADER};
use vrt_sql::wire;

const DB: &str = "abcdefghijklmnopqrstuvwxyz";
const SECRET: &[u8] = b"replica-test-secret";

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

fn write(e: &vrt_sql::engine::RusqliteEngine, sql: &str) {
    e.query_batch_sync(&[stmt(sql)], &ctx(), ResultsFormat::ArrayOfObjects)
        .unwrap();
}

fn writer(
    store: Arc<dyn ObjectStore>,
    clock: Arc<TestClock>,
    root: PathBuf,
) -> EngineRegistry {
    let d1 = Arc::new(Persister::new(
        store.clone(),
        PersistConfig {
            lease_ms: 10_000,
            clock_skew_margin_ms: 500,
            holder: "writer".into(),
            checkpoint_every_commits: 0,
            retention_ms: 60_000,
            snapshot_max_bytes: 1 << 30,
        },
        clock,
        root.clone(),
        D1,
    ));
    let queue = Arc::new(Persister::new(
        d1.store().clone(),
        d1.config().clone(),
        d1.clock().clone(),
        queue_root_for(&root),
        vrt_sql::paths::ResourceKind::Queue,
    ));
    let k2 = Arc::new(Persister::new(
        d1.store().clone(),
        d1.config().clone(),
        d1.clock().clone(),
        vrt_sql::paths::k2_root_for(&root),
        vrt_sql::paths::ResourceKind::K2,
    ));
    EngineRegistry::new(root).with_persisters(PersisterPair { d1, queue, k2 })
}

struct Harness {
    _tmp: tempfile::TempDir,
    engine: Arc<vrt_sql::engine::RusqliteEngine>,
    follower: Arc<Follower>,
    auth: ServiceAuth,
    signer: BookmarkSigner,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let clock = Arc::new(TestClock::new(1_000_000));
        let reg = writer(
            store.clone(),
            clock.clone(),
            tmp.path().join("writer"),
        );
        let e = reg.get(D1, DB).unwrap();
        write(&e, "CREATE TABLE t(v INTEGER)");
        write(&e, "INSERT INTO t VALUES (1)");
        let follower = Arc::new(
            Follower::new(
                store.clone(),
                D1,
                DB,
                tmp.path().join("replica"),
                clock.clone() as Arc<dyn Clock>,
            )
            .unwrap(),
        );
        follower.sync_once().unwrap();
        Self {
            _tmp: tmp,
            engine: e,
            follower,
            auth: ServiceAuth::new(SECRET.to_vec()),
            signer: BookmarkSigner::new([9u8; 32]),
        }
    }

    fn app(&self) -> axum::Router {
        replica::app(
            ReplicaState::new(self.follower.clone())
                .with_auth(self.auth.clone())
                .with_signer(self.signer.clone()),
        )
    }

    fn request(&self, path: &str, body: &[u8], session: Option<&str>) -> Request<Body> {
        let epoch = self.follower.state().unwrap().unwrap().epoch;
        let token = self.auth.sign("POST", path, body, Some(epoch), Some(1));
        let mut b = Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .header(AUTH_HEADER, token)
            .header(wire::EPOCH_HEADER, epoch.to_string())
            .header(wire::SEQ_HEADER, "1");
        if let Some(v) = session {
            b = b.header(wire::BOOKMARK_HEADER, v);
        }
        b.body(Body::from(body.to_vec())).unwrap()
    }
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn serves_query_and_signs_its_watermark() {
    let h = Harness::new();
    let path = format!("/dbs/{DB}/query");
    let body = br#"[{"sql":"SELECT count(*) AS n FROM t"}]"#;
    let resp = h
        .app()
        .oneshot(h.request(&path, body, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bm = resp
        .headers()
        .get(wire::BOOKMARK_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    let st = h.follower.state().unwrap().unwrap();
    assert_eq!(bm, h.signer.sign(DB, st.epoch, st.lsn));
    let v = body_json(resp).await;
    assert_eq!(v[0]["success"], true);
    assert_eq!(v[0]["results"][0]["n"], 1);
    assert_eq!(v[0]["meta"]["served_by"], "vrt-sql-replica");
}

#[tokio::test]
async fn newer_commit_is_visible_after_the_next_sync() {
    let h = Harness::new();
    write(&h.engine, "INSERT INTO t VALUES (2)");
    h.follower.sync_once().unwrap();

    let path = format!("/dbs/{DB}/query");
    let body = br#"[{"sql":"SELECT count(*) AS n FROM t"}]"#;
    let resp = h
        .app()
        .oneshot(h.request(&path, body, None))
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v[0]["results"][0]["n"], 2);
}

#[tokio::test]
async fn stale_bookmark_routes_back_to_primary() {
    let h = Harness::new();
    let path = format!("/dbs/{DB}/query");
    let body = br#"[{"sql":"SELECT 1"}]"#;
    let st = h.follower.state().unwrap().unwrap();
    let ahead = h.signer.sign(DB, st.epoch, st.lsn + 1);
    let resp = h
        .app()
        .oneshot(h.request(&path, body, Some(&ahead)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let v = body_json(resp).await;
    assert_eq!(v["code"], replica::CODE_STALE);
}

#[tokio::test]
async fn first_primary_and_invalid_bookmarks_route_away() {
    let h = Harness::new();
    let path = format!("/dbs/{DB}/query");
    let body = br#"[{"sql":"SELECT 1"}]"#;
    for (session, code) in [
        ("first-primary", replica::CODE_PRIMARY_REQUIRED),
        ("not-a-bookmark", replica::CODE_BOOKMARK_INVALID),
    ] {
        let resp = h
            .app()
            .oneshot(h.request(&path, body, Some(session)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let v = body_json(resp).await;
        assert_eq!(v["code"], code, "session {session}");
    }
}

#[tokio::test]
async fn writes_are_rejected_with_a_routing_code() {
    let h = Harness::new();
    let path = format!("/dbs/{DB}/query");
    let body = br#"[{"sql":"INSERT INTO t VALUES (2)"}]"#;
    let resp = h
        .app()
        .oneshot(h.request(&path, body, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let v = body_json(resp).await;
    assert_eq!(v["code"], replica::CODE_READ_ONLY);

    // The replica generation and the writer are both unchanged.
    let path = format!("/dbs/{DB}/query");
    let body = br#"[{"sql":"SELECT count(*) AS n FROM t"}]"#;
    let resp = h
        .app()
        .oneshot(h.request(&path, body, None))
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v[0]["results"][0]["n"], 1);
}

#[tokio::test]
async fn unknown_database_is_rejected_before_opening() {
    let h = Harness::new();
    let path = "/dbs/zzzzzzzzzzzzzzzzzzzzzzzzzz/query";
    let body = br#"[{"sql":"SELECT 1"}]"#;
    let resp = h
        .app()
        .oneshot(h.request(path, body, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let v = body_json(resp).await;
    assert_eq!(v["code"], replica::CODE_UNKNOWN_DB);
}

#[tokio::test]
async fn execute_and_dump_are_writer_only() {
    let h = Harness::new();
    let app = h.app();
    let path = format!("/dbs/{DB}/execute");
    let body = br#"[{"sql":"INSERT INTO t VALUES (2)"}]"#;
    let resp = app
        .clone()
        .oneshot(h.request(&path, body, None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let v = body_json(resp).await;
    assert_eq!(v["code"], replica::CODE_READ_ONLY);

    let path = format!("/dbs/{DB}/dump");
    let resp = app.oneshot(h.request(&path, b"", None)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let v = body_json(resp).await;
    assert_eq!(v["code"], replica::CODE_PRIMARY_ONLY);
}

#[tokio::test]
async fn status_requires_auth_and_reports_the_watermark() {
    let h = Harness::new();
    let app = h.app();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/replica/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let path = "/internal/replica/status";
    let token = h.auth.sign("GET", path, b"", None, None);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(path)
                .header(AUTH_HEADER, token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;
    assert_eq!(v["databaseId"], DB);
    assert_eq!(v["appliedLsn"], 2);
    assert_eq!(v["lagCommits"], 0);
    assert!(v["bookmark"].as_str().unwrap().len() > 32);
}
