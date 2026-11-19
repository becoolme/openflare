//! Bounded-retry coverage for `HttpDesiredSource` against a real TCP
//! stub: retry on 5xx, bounded attempts on per-attempt timeout, 404 →
//! `Ok(None)`, no retries burned on success.

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use cell_controller::retry::RetryPlan;
use cell_controller::source::{DesiredSource, HttpDesiredSource, SourceError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn plan(attempts: usize) -> RetryPlan {
    RetryPlan {
        max_attempts: attempts,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        jitter_seed: 1,
    }
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}/desired")
}

#[tokio::test]
async fn retries_on_5xx_then_succeeds() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let url = serve(Router::new().route(
        "/desired",
        get(move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                } else {
                    axum::Json(serde_json::json!({"ok": true})).into_response()
                }
            }
        }),
    ))
    .await;
    let source = HttpDesiredSource::new(&url, "tok", Duration::from_secs(2), plan(3)).unwrap();
    let out = source.fetch().await.unwrap();
    assert_eq!(out, Some(br#"{"ok":true}"#.to_vec()));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn per_attempt_timeout_is_bounded() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let url = serve(Router::new().route(
        "/desired",
        get(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                StatusCode::OK.into_response()
            }
        }),
    ))
    .await;
    let source = HttpDesiredSource::new(&url, "tok", Duration::from_millis(50), plan(2)).unwrap();
    let out = source.fetch().await;
    assert!(matches!(out, Err(SourceError::Timeout)));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "exactly max_attempts requests"
    );
}

#[tokio::test]
async fn not_found_maps_to_none() {
    let url = serve(Router::new().route(
        "/desired",
        get(|| async { StatusCode::NOT_FOUND.into_response() }),
    ))
    .await;
    let source = HttpDesiredSource::new(&url, "tok", Duration::from_secs(2), plan(3)).unwrap();
    assert_eq!(source.fetch().await.unwrap(), None);
}
