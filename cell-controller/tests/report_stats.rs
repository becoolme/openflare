//! Real signed e2e for the report's data-plane stats fetch: the production
//! `D1OpsExecutor::stats` calls a real `vrt-sql` app with ServiceAuth on,
//! seeds one D1 table + one queue message, and verifies the returned shape.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use vrt_sql::queue::{IncomingMsg, QueueBackend, QueueId, SqliteQueueBackend};
use vrt_sql::service::ServiceAuth;
use vrt_sql::wire::{self, AppState, TrustedCallCtx};

const SECRET: &[u8] = b"cell-report-stats-secret-0123456789abcdef";
const DB: &str = "01jqqm3b4y6zq8f0v2w5x7k9ae";

#[tokio::test]
async fn report_stats_round_trip_through_vrt_sql_auth() {
    let tmp = tempfile::tempdir().unwrap();
    let secret_file = tmp.path().join("service.secret");
    vrt_securefs::atomic_write(&secret_file, SECRET, 0o600).unwrap();

    let mut state = AppState::new(tmp.path().join("dbs"), [7u8; 32]);
    state.auth = Some(ServiceAuth::new(SECRET.to_vec()));
    let registry = state.registry.clone();

    // Seed through the trusted internal path (same engine/registry the app uses).
    for sql in ["CREATE TABLE t(a TEXT)", "INSERT INTO t VALUES('x')"] {
        let ctx = TrustedCallCtx::test(Instant::now() + Duration::from_secs(10));
        let body = serde_json::json!({"sql": sql});
        let r = wire::handle_d1(&state, DB, None, ctx, body.to_string().as_bytes()).await;
        assert_eq!(r.status, axum::http::StatusCode::OK, "{:?}", r.body);
    }
    let queue = QueueId::parse(&"q".repeat(26)).unwrap();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let backend = SqliteQueueBackend::new(registry);
    backend
        .send(
            &queue,
            &[IncomingMsg {
                id: Some("m1".into()),
                body: b"hi".to_vec(),
                content_type: "text".into(),
                delay_secs: 0,
                timestamp_ms: None,
            }],
            now_ms,
        )
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = wire::app(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let exec = cell_controller::d1ops::D1OpsExecutor::new(&cell_controller::d1ops::D1SqlConfig {
        endpoint: format!("http://{addr}"),
        auth_secret_file: secret_file.to_string_lossy().into(),
        timeout_secs: 5,
    })
    .unwrap();
    let value = exec
        .stats(
            &[DB.to_string(), "bad".into()],
            &[queue.to_string(), "short".into()],
        )
        .await
        .unwrap();
    assert_eq!(value["success"], true, "{value}");
    assert_eq!(value["databases"][0]["stats"]["tableCount"], 1, "{value}");
    assert_eq!(value["databases"][0]["stats"]["tables"][0], "t", "{value}");
    assert_eq!(
        value["databases"][1]["error"]["code"],
        "VRT_INVALID_REQUEST"
    );
    assert_eq!(value["queues"][0]["metrics"]["backlogCount"], 1, "{value}");
    assert_eq!(value["queues"][1]["error"]["code"], "VRT_INVALID_REQUEST");
}
