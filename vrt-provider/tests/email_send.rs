//! send_email binding dispatch: identity → authorize → permission →
//! body validation → signed restriction enforcement → adapter.
//! Every bad path must carry a CF E_* code in the error envelope.

mod support;

use std::sync::Arc;

use support::*;
use tokio::sync::oneshot;
use vrt_provider::email::{EmailBackend, EmailError, NormalizedEmail};
use vrt_provider::server::{tenant_router, TenantService};
use vrt_provider::unixsock;

/// Same loopback-TCP serve path as server_http's harness (unix bind is
/// denied in the test sandbox).
async fn spawn_tenant(svc: Arc<TenantService>) -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        unixsock::serve(listener, tenant_router(svc), async move {
            rx.await.ok();
        })
        .await
        .ok();
    });
    (socket, tx)
}

struct StubBackend(Result<String, EmailError>);

#[async_trait::async_trait]
impl EmailBackend for StubBackend {
    async fn send(&self, _msg: &NormalizedEmail) -> Result<String, EmailError> {
        self.0.clone()
    }
}

fn svc_with(
    bindings: Vec<snapshot_protocol::compute_desired::BindingDesired>,
    backend: Option<Arc<dyn EmailBackend>>,
) -> (Fixture, Arc<TenantService>) {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(WORKER, VERSION, DIGEST, bindings)],
            )],
        ),
        7,
    );
    (
        f,
        Arc::new(TenantService {
            tenant_id: TENANT.to_string(),
            authorizer: Arc::new(authorizer(store, 30_000)),
            kv: None,
            assets: None,
            r2: None,
            signer: Arc::new(vrt_provider::cursor::CursorSigner::new(b"cursor-secret")),
            r2_max_object_bytes: 1024 * 1024,
            permits: Arc::new(tokio::sync::Semaphore::new(64)),
            global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
            body_read_timeout: std::time::Duration::from_secs(30),
            d1: None,
            queue: None,
            k2: None,
            email: backend,
            pg: None,
        }),
    )
}

fn email_headers(name: &str) -> Vec<(String, String)> {
    let binding = vrt_provider::identity::binding_id(WORKER, name);
    standard_headers(&binding, WORKER, VERSION, DIGEST, name, 7)
}

fn email_path(name: &str) -> String {
    format!("/internal/bindings/v1/email/{WORKER}~{name}/send")
}

fn send_body() -> serde_json::Value {
    serde_json::json!({
        "from": {"email": "hello@example.com"},
        "to": [{"email": "user@example.com"}],
        "subject": "hi",
        "text": "hello",
    })
}

async fn round_trip(
    svc: Arc<TenantService>,
    path: &str,
    headers: Vec<(String, String)>,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    let (socket, stop) = spawn_tenant(svc).await;
    let mut refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    refs.push(("content-type", "application/json"));
    let (status, _, resp) = http_post(socket, path, &refs, body.to_string().as_bytes()).await;
    stop.send(()).ok();
    let json = serde_json::from_slice(&resp).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn ok_backend() -> Option<Arc<dyn EmailBackend>> {
    Some(Arc::new(StubBackend(Ok("msg-1".to_string()))))
}

#[tokio::test]
async fn send_success() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    let (status, json) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(json["success"], true);
    assert_eq!(json["messageId"], "msg-1");
}

#[tokio::test]
async fn send_unauthenticated() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    // Missing identity headers → rejected at identity parse (400),
    // before authorization.
    let (status, _) = round_trip(svc, &email_path("EMAIL"), vec![], send_body()).await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn send_unknown_binding() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    // Request an undeclared binding — authorization must fail closed.
    let (status, _) = round_trip(
        svc,
        &email_path("OTHER"),
        email_headers("OTHER"),
        send_body(),
    )
    .await;
    assert!(status == 403 || status == 404);
}

#[tokio::test]
async fn send_read_only_denied() {
    let (_f, svc) = svc_with(
        vec![email_binding_perms("EMAIL", None, &[], &[], true, false)],
        ok_backend(),
    );
    let (status, _) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 403);
}

#[tokio::test]
async fn send_backend_unavailable() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], None);
    let (status, json) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 503);
    assert_eq!(json["error"]["retryable"], true);
}

#[tokio::test]
async fn send_malformed_body() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    let (socket, stop) = spawn_tenant(svc).await;
    let headers = email_headers("EMAIL");
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (status, _, resp) = http_post(socket, &email_path("EMAIL"), &refs, b"not json{").await;
    stop.send(()).ok();
    assert_eq!(status, 400);
    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json["error"]["code"], "E_VALIDATION_ERROR");
}

#[tokio::test]
async fn send_no_recipients() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    let mut body = send_body();
    body.as_object_mut().unwrap().remove("to");
    let (status, json) = round_trip(svc, &email_path("EMAIL"), email_headers("EMAIL"), body).await;
    assert_eq!(status, 400);
    assert_eq!(json["error"]["code"], "E_VALIDATION_ERROR");
}

#[tokio::test]
async fn send_too_many_recipients() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    let to: Vec<serde_json::Value> = (0..51)
        .map(|i| serde_json::json!({"email": format!("u{i}@example.com")}))
        .collect();
    let mut body = send_body();
    body["to"] = serde_json::json!(to);
    let (status, json) = round_trip(svc, &email_path("EMAIL"), email_headers("EMAIL"), body).await;
    assert_eq!(status, 400);
    assert_eq!(json["error"]["code"], "E_TOO_MANY_RECIPIENTS");
}

#[tokio::test]
async fn send_sender_not_allowed() {
    let (_f, svc) = svc_with(
        vec![email_binding_perms(
            "EMAIL",
            None,
            &[],
            &["other@example.com"],
            true,
            true,
        )],
        ok_backend(),
    );
    let (status, json) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(json["error"]["code"], "E_SENDER_NOT_VERIFIED");
}

#[tokio::test]
async fn send_recipient_pin_enforced() {
    let (_f, svc) = svc_with(
        vec![email_binding_perms(
            "EMAIL",
            Some("pin@example.com"),
            &[],
            &[],
            true,
            true,
        )],
        ok_backend(),
    );
    let (status, json) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(json["error"]["code"], "E_RECIPIENT_NOT_ALLOWED");
}

#[tokio::test]
async fn send_recipient_allowlist_enforced() {
    let (_f, svc) = svc_with(
        vec![email_binding_perms(
            "EMAIL",
            None,
            &["allowed@example.com"],
            &[],
            true,
            true,
        )],
        ok_backend(),
    );
    // Recipient not in allowlist → 403; rewriting to the allowed
    // address passes.
    let (status, _) = round_trip(
        svc.clone(),
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 403);
    let mut body = send_body();
    body["to"] = serde_json::json!([{"email": "allowed@example.com"}]);
    let (status, json) = round_trip(svc, &email_path("EMAIL"), email_headers("EMAIL"), body).await;
    assert_eq!(status, 200);
    assert_eq!(json["success"], true);
}

#[tokio::test]
async fn send_reserved_header_rejected() {
    let (_f, svc) = svc_with(vec![email_binding("EMAIL")], ok_backend());
    let mut body = send_body();
    body["headers"] = serde_json::json!({"From": "spoof@example.com"});
    let (status, json) = round_trip(svc, &email_path("EMAIL"), email_headers("EMAIL"), body).await;
    assert_eq!(status, 400);
    assert_eq!(json["error"]["code"], "E_HEADER_USE_API_FIELD");
}

#[tokio::test]
async fn send_backend_retryable_maps_503() {
    let (_f, svc) = svc_with(
        vec![email_binding("EMAIL")],
        Some(Arc::new(StubBackend(Err(EmailError {
            code: "E_INTERNAL_SERVER_ERROR",
            message: "upstream down".to_string(),
            retryable: true,
        })))),
    );
    let (status, json) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 503);
    assert_eq!(json["error"]["retryable"], true);
}

#[tokio::test]
async fn send_backend_failure_maps_502() {
    let (_f, svc) = svc_with(
        vec![email_binding("EMAIL")],
        Some(Arc::new(StubBackend(Err(EmailError {
            code: "E_DELIVERY_FAILED",
            message: "relay rejected".to_string(),
            retryable: false,
        })))),
    );
    let (status, json) = round_trip(
        svc,
        &email_path("EMAIL"),
        email_headers("EMAIL"),
        send_body(),
    )
    .await;
    assert_eq!(status, 502);
    assert_eq!(json["error"]["code"], "E_DELIVERY_FAILED");
}
