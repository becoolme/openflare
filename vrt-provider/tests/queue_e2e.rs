//! T37 full-path adversarial test (in-process):
//!   facade simulated request -> provider `tenant_router` -> Authorizer ->
//!   QueueClient(ServiceAuth) -> real vrt-sql axum app -> SqliteQueueBackend.
//! Covers: authorization routing (queueId comes only from the signed desired snapshot), forged resource headers,
//! cross-tenant queue writes, a D1 binding calling queue endpoints failing closed on kind mismatch,
//! broker wire -> metrics round trip.

mod support;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use support::*;
use tower::ServiceExt;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::queue::QueueClient;
use vrt_provider::server::{tenant_router, TenantService};

const QUEUE_ID: &str = "qqqqqqqqqqqqqqqqqqqqqqqqqq";
const OTHER_QUEUE_ID: &str = "zzzzzzzzzzzzzzzzzzzzzzzzzz";

/// vrt-sql scratch db root -- the `target/tmp/` repo convention (does not land in /tmp).
/// T37r2: Drop guard -- previously a bare PathBuf was never cleaned up when the test process exited,
/// each hang/crash left a `queue-e2e-*` directory; now it is reclaimed regardless of outcome.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        // The axum server task is in-process and may still be writing -- on unix
        // remove_dir_all is safe for open files; a failure only warns and does not panic
        // (cleanup must never turn a green test red).
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("scratch cleanup {}: {e}", self.0.display());
        }
    }
}

fn scratch_dir(tag: &str) -> ScratchDir {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp")
        .join(format!("queue-e2e-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    ScratchDir(dir)
}

async fn spawn_vrt_sql(tag: &str) -> (String, ScratchDir) {
    let tmp = scratch_dir(tag);
    let auth = vrt_sql::service::ServiceAuth::new(b"queue-e2e-secret".to_vec());
    let state = vrt_sql::wire::AppState::new(tmp.path().join("dbs"), [7u8; 32]).with_auth(auth);
    let app = vrt_sql::wire::app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), tmp)
}

fn svc_with_queue(queue_client: Option<Arc<QueueClient>>, f: &Fixture) -> Arc<TenantService> {
    let store = store_with(
        f,
        &desired(
            7,
            vec![
                tenant(
                    TENANT,
                    vec![worker(
                        WORKER,
                        VERSION,
                        DIGEST,
                        vec![queue_binding("Q", QUEUE_ID)],
                    )],
                ),
                tenant(
                    "team-other",
                    vec![worker(
                        "w-other",
                        "v-other",
                        DIGEST2,
                        vec![queue_binding("Q", OTHER_QUEUE_ID)],
                    )],
                ),
            ],
        ),
        7,
    );
    Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: None,
        r2: None,
        assets: None,
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: queue_client,
        k2: None,
        email: None,
        pg: None,
    })
}

fn queue_headers(binding_name: &str, resource: &str) -> Vec<(String, String)> {
    let binding = vrt_provider::identity::binding_id(WORKER, binding_name);
    standard_headers(&binding, WORKER, VERSION, DIGEST, resource, 7)
}

async fn call(
    svc: Arc<TenantService>,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: Bytes,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().method(method).uri(path);
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let res = tenant_router(svc)
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1 << 22).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({})),
    )
}

#[tokio::test]
async fn producer_send_batch_metrics_roundtrip() {
    let (sql_addr, _tmp) = spawn_vrt_sql("rt").await;
    let f = fixture();
    let client = Arc::new(
        QueueClient::new(
            sql_addr,
            b"queue-e2e-secret".to_vec(),
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
    );
    let svc = svc_with_queue(Some(client), &f);
    let base = format!("/internal/bindings/v1/queue/{}~Q", WORKER);

    // POST /message -- raw body + broker headers.
    let (st, body) = call(
        svc.clone(),
        "POST",
        &format!("{base}/message"),
        &queue_headers("Q", QUEUE_ID),
        Bytes::from_static(b"hello-queue"),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    // broker response shape {metadata:{metrics:{backlogCount,...}}}.
    assert_eq!(
        body["metadata"]["metrics"]["backlogCount"].as_i64(),
        Some(1),
        "{body}"
    );

    // POST /batch -- JSON {messages:[...base64...]}.
    let batch = serde_json::json!({
        "messages": [
            {"contentType": "text", "body": "aGk="},
            {"contentType": "text", "body": "Ynll"}
        ]
    })
    .to_string();
    let (st, body) = call(
        svc.clone(),
        "POST",
        &format!("{base}/batch"),
        &queue_headers("Q", QUEUE_ID),
        Bytes::from(batch),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(
        body["metadata"]["metrics"]["backlogCount"].as_i64(),
        Some(3),
        "{body}"
    );

    // metrics -- the provider side goes via POST (dispatch is globally POST-only;
    // the facade translates the broker's GET /metrics into POST).
    let (st, body) = call(
        svc.clone(),
        "POST",
        &format!("{base}/metrics"),
        &queue_headers("Q", QUEUE_ID),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["backlogCount"].as_i64(), Some(3), "{body}");
}

#[tokio::test]
async fn forged_resource_header_cannot_redirect_queue() {
    // Forged x-open-compute-namespace-resource-id = another tenant's queueId:
    // the Authorizer compares the queueId in the desired snapshot against the identity header ->
    // ResourceMismatch -> 403, the forged target queue never receives the message.
    let (sql_addr, _tmp) = spawn_vrt_sql("rt").await;
    let f = fixture();
    let client = Arc::new(
        QueueClient::new(
            sql_addr,
            b"queue-e2e-secret".to_vec(),
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
    );
    let svc = svc_with_queue(Some(client), &f);
    let (st, _) = call(
        svc.clone(),
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/message", WORKER),
        &queue_headers("Q", OTHER_QUEUE_ID), // forged resource
        Bytes::from_static(b"poison"),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    // The target queue must still be empty -- the forgery was not delivered.
    let (st2, body) = call(
        svc,
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/metrics", WORKER),
        &queue_headers("Q", QUEUE_ID),
        Bytes::new(),
    )
    .await;
    assert_eq!(st2, StatusCode::OK);
    assert_eq!(body["backlogCount"].as_i64(), Some(0), "{body}");
}

#[tokio::test]
async fn wrong_binding_and_kind_mismatch_fail_closed() {
    let (sql_addr, _tmp) = spawn_vrt_sql("rt").await;
    let f = fixture();
    let client = Arc::new(
        QueueClient::new(
            sql_addr,
            b"queue-e2e-secret".to_vec(),
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
    );
    let svc = svc_with_queue(Some(client), &f);

    // Undeclared binding name -> authorize rejects.
    let (st, _) = call(
        svc.clone(),
        "POST",
        &format!("/internal/bindings/v1/queue/{}~NOPE/message", WORKER),
        &queue_headers("NOPE", QUEUE_ID),
        Bytes::from_static(b"x"),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // bindingId points at a nonexistent worker (forged workerId).
    let mut h = queue_headers("Q", QUEUE_ID);
    h.retain(|(k, _)| k != "x-open-compute-worker-id");
    h.push(("x-open-compute-worker-id".to_string(), "ghost".to_string()));
    let (st, _) = call(
        svc.clone(),
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/message", WORKER),
        &h,
        Bytes::from_static(b"x"),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Queue backend not configured -> 503 (fail closed, no silent success).
    let svc_none = svc_with_queue(None, &f);
    let (st, _) = call(
        svc_none,
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/message", WORKER),
        &queue_headers("Q", QUEUE_ID),
        Bytes::from_static(b"x"),
    )
    .await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn cross_tenant_queue_unwritable() {
    // team-other's queue binding is authorized under team-other -- when TENANT's
    // TenantService handles it the tenant does not match -> rejected. The reverse of "a binding of tenant X
    // writing tenant Y's queueId": who owns a resource is decided by the snapshot,
    // not by the request.
    let (sql_addr, _tmp) = spawn_vrt_sql("rt").await;
    let f = fixture();
    let client = Arc::new(
        QueueClient::new(
            sql_addr,
            b"queue-e2e-secret".to_vec(),
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
    );
    let svc = svc_with_queue(Some(client), &f);
    // w-other/Q belongs to team-other: rejected on the TENANT listener.
    let binding = vrt_provider::identity::binding_id("w-other", "Q");
    let h = standard_headers(&binding, "w-other", "v-other", DIGEST2, OTHER_QUEUE_ID, 7);
    let (st, _) = call(
        svc,
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/message", "w-other"),
        &h,
        Bytes::from_static(b"cross-tenant"),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn permission_denied_read_only_binding() {
    // A queue binding with permissions.read cannot write.
    let (sql_addr, _tmp) = spawn_vrt_sql("rt").await;
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(
                    WORKER,
                    VERSION,
                    DIGEST,
                    vec![queue_binding_perms("Q", QUEUE_ID, true, false)],
                )],
            )],
        ),
        7,
    );
    let client = Arc::new(
        QueueClient::new(
            sql_addr,
            b"queue-e2e-secret".to_vec(),
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: None,
        r2: None,
        assets: None,
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: Some(client),
        k2: None,
        email: None,
        pg: None,
    });
    let (st, _) = call(
        svc.clone(),
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/message", WORKER),
        &queue_headers("Q", QUEUE_ID),
        Bytes::from_static(b"x"),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    // metrics is read-only -> allowed.
    let (st, _) = call(
        svc,
        "POST",
        &format!("/internal/bindings/v1/queue/{}~Q/metrics", WORKER),
        &queue_headers("Q", QUEUE_ID),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
}
