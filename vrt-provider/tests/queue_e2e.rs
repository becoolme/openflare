//! T37 全链路对抗测试（in-process）：
//!   facade 模拟请求 → provider `tenant_router` → Authorizer →
//!   QueueClient(ServiceAuth) → 真 vrt-sql axum app → SqliteQueueBackend。
//! 覆盖：授权路由（queueId 只来自签名 desired snapshot）、伪造资源头、
//! 跨租户队列写入、D1 binding 调队列端点的 kind-mismatch fail-closed、
//! broker wire → metrics 往返。

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

/// vrt-sql scratch db root — `target/tmp/` 仓库约定（不落 /tmp）。
/// T37r2：Drop guard——以前裸 PathBuf 随测试进程退出而永不清理，
/// 挂死/崩溃各留一个 `queue-e2e-*` 目录；无论成败现在都回收。
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        // axum server task 是 in-process 的，可能仍在写——unix 上
        // remove_dir_all 对已打开文件安全；失败只告警不 panic
        //（清理绝不能让绿测试变红）。
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

    // POST /message — raw body + broker headers。
    let (st, body) = call(
        svc.clone(),
        "POST",
        &format!("{base}/message"),
        &queue_headers("Q", QUEUE_ID),
        Bytes::from_static(b"hello-queue"),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    // broker 响应形状 {metadata:{metrics:{backlogCount,…}}}。
    assert_eq!(
        body["metadata"]["metrics"]["backlogCount"].as_i64(),
        Some(1),
        "{body}"
    );

    // POST /batch — JSON {messages:[…base64…]}。
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

    // metrics — provider 侧走 POST（dispatch 全局 POST-only；
    // facade 把 broker 的 GET /metrics 翻译成 POST）。
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
    // 伪造 x-open-compute-namespace-resource-id = 另一租户的 queueId：
    // Authorizer 拿 desired snapshot 里的 queueId 与身份头比对 →
    // ResourceMismatch → 403，伪造的目标队列永远收不到消息。
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
    // 目标队列必须仍为空——伪造未被送达。
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

    // 未声明的 binding 名 → authorize 拒。
    let (st, _) = call(
        svc.clone(),
        "POST",
        &format!("/internal/bindings/v1/queue/{}~NOPE/message", WORKER),
        &queue_headers("NOPE", QUEUE_ID),
        Bytes::from_static(b"x"),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // bindingId 指向不存在的 worker（伪造 workerId）。
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

    // 队列后端未配置 → 503（fail closed，不静默成功）。
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
    // team-other 的队列 binding 授权在 team-other 下——TENANT 的
    // TenantService 处理它时 tenant 不匹配 → 拒。等价于"租户 X 的
    // binding 写租户 Y 的 queueId"的反向：资源属于谁由快照决定，
    // 不由请求决定。
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
    // w-other/Q 属于 team-other：TENANT listener 上拒绝。
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
    // permissions.read 的 queue binding 不能写。
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
    // metrics 只读 → 允许。
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
