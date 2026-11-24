//! Production `HttpAutoDeps`: real HTTP health/promote/control-report wire.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{routing::post, Router};

#[tokio::test]
async fn http_auto_failover_promotes_and_reports() {
    // Writer health is down.
    let health = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_addr = health.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            health,
            Router::new().route(
                "/healthz",
                axum::routing::get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
            ),
        )
        .await
        .unwrap();
    });

    // Follower promote endpoint reports epoch 2 / lsn 42.
    let promote = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let promote_addr = promote.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            promote,
            Router::new().route(
                "/internal/dbs/{id}/promote",
                post(|axum::extract::Path(id): axum::extract::Path<String>| async move {
                    axum::Json(serde_json::json!({
                        "success": true,
                        "epoch": 2,
                        "lsn": 42,
                        "previousEpoch": 1,
                        "previousLsn": 42,
                        "resumed": false,
                        "databaseId": id,
                    }))
                }),
            ),
        )
        .await
        .unwrap();
    });

    // Control-plane writer report.
    let reports = Arc::new(AtomicUsize::new(0));
    let cp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cp_addr = cp.local_addr().unwrap();
    let reports2 = reports.clone();
    tokio::spawn(async move {
        axum::serve(
            cp,
            Router::new().route(
                "/api/internal/clusters/cell-1/vrt/d1-writer",
                post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let reports = reports2.clone();
                    async move {
                        assert_eq!(body["databaseId"], "a".repeat(26));
                        assert_eq!(body["epoch"], 2);
                        assert_eq!(body["lsn"], 42);
                        assert_eq!(body["writerAddr"], "http://127.0.0.1:9000");
                        reports.fetch_add(1, Ordering::SeqCst);
                        axum::Json(serde_json::json!({"data":{"applied":true}}))
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });

    let deps = cell_controller::d1_auto::HttpAutoDeps::new(
        &format!("http://{cp_addr}"),
        "cell-1",
        "cell-token".into(),
        b"secret".to_vec(),
    )
    .unwrap();
    let cfg = cell_controller::d1_auto::AutoFailoverConfig {
        database_id: "a".repeat(26),
        writer_probe: format!("http://{health_addr}"),
        writer_addr: "http://127.0.0.1:9000".into(),
        promote_endpoint: format!("http://{promote_addr}"),
        failure_threshold: 2,
        cooldown_secs: 30,
        probe_interval_secs: 5,
    };
    cfg.validate().unwrap();
    let mut auto = cell_controller::d1_auto::AutoFailover::new(cfg);
    let now = std::time::Instant::now();
    assert!(matches!(
        auto.tick(&deps, now).await,
        cell_controller::d1_auto::AutoOutcome::Failed { .. }
    ));
    assert!(matches!(
        auto.tick(&deps, now + Duration::from_secs(1)).await,
        cell_controller::d1_auto::AutoOutcome::Promoted { epoch: 2 }
    ));
    assert_eq!(reports.load(Ordering::SeqCst), 1);
}
