//! End-to-end tests over a real per-tenant unix listener: KV/R2 protocol
//! subset, authorization denials at the HTTP boundary, error envelope
//! shape, listener-directory hardening, graceful shutdown.

mod support;

use snapshot_protocol::compute_desired::BindingDesired;
use std::sync::Arc;
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::desired::MemoryDesiredStore;
use vrt_provider::kv::{self, MemoryKvProvider};
use vrt_provider::limits::{KV_FRAME_CONTENT_TYPE, R2_FRAME_CONTENT_TYPE, R2_JSON_CONTENT_TYPE};
use vrt_provider::r2::MemoryR2Provider;
use vrt_provider::server::{tenant_router, TenantService};
use vrt_provider::unixsock;

fn service_fixture() -> (Fixture, Arc<TenantService>) {
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
                    vec![kv_binding("KV", NS_ID), r2_binding("BUCKET", BUCKET)],
                )],
            )],
        ),
        7,
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    (f, svc)
}

fn kv_headers(extra: &[(&str, &str)]) -> Vec<(String, String)> {
    let binding = vrt_provider::identity::binding_id(WORKER, "KV");
    let mut headers = standard_headers(&binding, WORKER, VERSION, DIGEST, NS_ID, 7);
    headers.push((
        "content-type".to_string(),
        KV_FRAME_CONTENT_TYPE.to_string(),
    ));
    for (k, v) in extra {
        headers.retain(|(name, _)| name != k);
        headers.push((k.to_string(), v.to_string()));
    }
    headers
}

fn r2_headers(binding_name: &str, resource: &str, ct: &str) -> Vec<(String, String)> {
    let binding = vrt_provider::identity::binding_id(WORKER, binding_name);
    let mut headers = standard_headers(&binding, WORKER, VERSION, DIGEST, resource, 7);
    headers.push(("content-type".to_string(), ct.to_string()));
    headers
}

fn kv_path(op: &str) -> String {
    format!("/internal/bindings/v1/kv/{}~KV/{op}", WORKER)
}

fn r2_path(op: &str) -> String {
    format!("/internal/bindings/v1/r2/{}~BUCKET/{op}", WORKER)
}

fn header_ref(headers: &[(String, String)]) -> Vec<(&str, &str)> {
    headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

fn put_frame(header: &serde_json::Value, value: &[u8]) -> Vec<u8> {
    let header_bytes = serde_json::to_vec(header).unwrap();
    let mut frame = (header_bytes.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&header_bytes);
    frame.extend_from_slice(value);
    frame
}

/// Spawn the tenant router over loopback TCP through the same
/// `unixsock::serve` drain path used by the production unix listener
/// (unix `bind()` is denied in the test sandbox).
async fn spawn_tenant(svc: Arc<TenantService>) -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        unixsock::serve(listener, tenant_router(svc), async move {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    (socket, tx)
}

#[tokio::test]
async fn kv_protocol_end_to_end() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;

    // put with metadata
    let body = put_frame(
        &serde_json::json!({"key": "alpha", "metadata": {"z": 1}, "metadataPresent": true}),
        b"value-alpha",
    );
    let (status, headers, _) = http_post(
        socket,
        &kv_path("put"),
        &header_ref(&kv_headers(&[])),
        &body,
    )
    .await;
    assert_eq!(status, 200, "put");
    assert_eq!(
        headers
            .iter()
            .find(|(n, _)| n == "content-type")
            .map(|(_, v)| v.as_str()),
        Some(KV_FRAME_CONTENT_TYPE)
    );

    // get → KVS1 frame
    let (status, _, body) = http_post(
        socket,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"keys": ["alpha"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "get");
    assert_eq!(&body[..4], b"KVS1");
    let (entry, _) = kv::decode_entry(&body[4..]).unwrap();
    let entry = entry.unwrap();
    assert_eq!(entry.value, b"value-alpha");
    assert_eq!(
        entry.metadata_json.as_deref(),
        Some(br#"{"z":1}"#.as_slice())
    );

    // get-with-metadata shares the KVS1 shape
    let (status, _, _body) = http_post(
        socket,
        &kv_path("get-with-metadata"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"keys": ["alpha", "missing"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 400, "get-with-metadata requires exactly 1 key");

    // get-many → KVB1
    let (status, _, body) = http_post(
        socket,
        &kv_path("get-many"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"keys": ["alpha", "missing"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "get-many");
    assert_eq!(&body[..4], b"KVB1");
    assert_eq!(u16::from_be_bytes(body[4..6].try_into().unwrap()), 2);

    // list → JSON page
    let (status, _, body) = http_post(
        socket,
        &kv_path("list"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"prefix": "", "limit": 100, "cursor": null}))
            .unwrap(),
    )
    .await;
    assert_eq!(status, 200, "list");
    let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(page["keys"][0]["name"], "alpha");
    assert_eq!(page["list_complete"], true);

    // delete → 200 empty
    let (status, _, _) = http_post(
        socket,
        &kv_path("delete"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"key": "alpha"})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "delete");

    // absent get → presence byte 0
    let (status, _, body) = http_post(
        socket,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"keys": ["alpha"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body[4], 0);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn kv_error_envelope_shape() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;

    // cross-namespace claim → 403 uniform denial
    let mut headers = standard_headers(
        &vrt_provider::identity::binding_id(WORKER, "KV"),
        WORKER,
        VERSION,
        DIGEST,
        "ns-forged",
        7,
    );
    headers.push((
        "content-type".to_string(),
        KV_FRAME_CONTENT_TYPE.to_string(),
    ));
    let (status, response_headers, body) = http_post(
        socket,
        &kv_path("get"),
        &header_ref(&headers),
        &serde_json::to_vec(&serde_json::json!({"keys": ["k"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(
        response_headers
            .iter()
            .find(|(n, _)| n == "x-open-compute-error-code")
            .map(|(_, v)| v.as_str()),
        Some("BINDING_PERMISSION_DENIED")
    );
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["ok"], false);
    assert_eq!(envelope["error"]["code"], "BINDING_PERMISSION_DENIED");
    assert_eq!(envelope["error"]["retryable"], false);
    assert_eq!(envelope["error"]["resultUnknown"], false);

    // wrong content-type → 415
    let mut headers = kv_headers(&[]);
    headers.retain(|(n, _)| n != "content-type");
    headers.push(("content-type".to_string(), "application/json".to_string()));
    let (status, _, _) = http_post(socket, &kv_path("get"), &header_ref(&headers), b"{}").await;
    assert_eq!(status, 415);

    // missing identity headers → protocol error
    let (status, _, body) = http_post(
        socket,
        &kv_path("get"),
        &[("content-type", KV_FRAME_CONTENT_TYPE)],
        &serde_json::to_vec(&serde_json::json!({"keys": ["k"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["error"]["code"], "BINDING_PROTOCOL_ERROR");
    // KV protocol errors are retryable upstream
    assert_eq!(envelope["error"]["retryable"], true);

    // unparseable binding id → protocol error
    let (status, _, _) = http_post(
        socket,
        "/internal/bindings/v1/kv/noseparator/get",
        &header_ref(&kv_headers(&[])),
        b"{}",
    )
    .await;
    assert_eq!(status, 400);

    // unknown operation → 404
    let (status, _, _) = http_post(
        socket,
        &kv_path("bogus-op"),
        &header_ref(&kv_headers(&[])),
        b"{}",
    )
    .await;
    assert_eq!(status, 404);

    // path outside the protocol → 404
    let (status, _, _) = http_post(
        socket,
        "/internal/other",
        &header_ref(&kv_headers(&[])),
        b"{}",
    )
    .await;
    assert_eq!(status, 404);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn kv_method_and_declared_size_guards() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;

    // GET → 405
    let mut stream = tokio::net::TcpStream::connect(socket).await.unwrap();
    stream
        .write_all(
            format!(
                "GET {} HTTP/1.1\r\nHost: unix\r\nConnection: close\r\n\r\n",
                kv_path("get")
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let head = String::from_utf8_lossy(&raw).to_string();
    assert!(head.starts_with("HTTP/1.1 405"), "{head}");

    // declared content-length beyond the hard cap → 413 without reading
    let mut stream = tokio::net::TcpStream::connect(socket).await.unwrap();
    let headers = kv_headers(&[]);
    let mut request = format!(
        "POST {} HTTP/1.1\r\nHost: unix\r\nConnection: close\r\nContent-Length: {}\r\n",
        kv_path("get"),
        vrt_provider::limits::DECLARED_BODY_HARD_CAP + 1,
    );
    for (name, value) in &headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let head = String::from_utf8_lossy(&raw).to_string();
    assert!(head.starts_with("HTTP/1.1 413"), "{head}");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn kv_cursor_replay_across_prefix_denied() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;

    for key in ["a/1", "a/2", "b/1"] {
        let body = put_frame(&serde_json::json!({"key": key}), b"v");
        http_post(
            socket,
            &kv_path("put"),
            &header_ref(&kv_headers(&[])),
            &body,
        )
        .await;
    }
    // page 1 with limit 1 → cursor bound to prefix "a/"
    let (status, _, body) = http_post(
        socket,
        &kv_path("list"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"prefix": "a/", "limit": 1})).unwrap(),
    )
    .await;
    assert_eq!(status, 200);
    let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let cursor = page["cursor"].as_str().unwrap().to_string();
    // same-prefix replay works
    let (status, _, body) = http_post(
        socket,
        &kv_path("list"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"prefix": "a/", "limit": 10, "cursor": cursor}))
            .unwrap(),
    )
    .await;
    assert_eq!(status, 200);
    let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(page["keys"].as_array().unwrap().len(), 1);
    // cross-prefix replay → KV_CURSOR_INVALID
    let (status, _, body) = http_post(
        socket,
        &kv_path("list"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"prefix": "b/", "limit": 10, "cursor": cursor}))
            .unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["error"]["code"], "KV_CURSOR_INVALID");
    // garbage cursor → KV_CURSOR_INVALID
    let (status, _, body) = http_post(
        socket,
        &kv_path("list"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"prefix": "a/", "limit": 10, "cursor": "%%%"}))
            .unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope["error"]["code"], "KV_CURSOR_INVALID");

    let _ = shutdown.send(());
}

#[tokio::test]
async fn kv_validation_errors() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;
    let headers = kv_headers(&[]);
    macro_rules! post {
        ($op:expr, $body:expr) => {
            http_post(socket, &kv_path($op), &header_ref(&headers), &$body)
        };
    }

    // too many keys in single get
    let (status, _, body) = post!(
        "get",
        serde_json::to_vec(&serde_json::json!({"keys": ["a", "b"]})).unwrap()
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_TOO_MANY_KEYS"
    );
    // >100 keys in get-many → 413
    let keys: Vec<String> = (0..101).map(|i| format!("k{i}")).collect();
    let (status, _, body) = post!(
        "get-many",
        serde_json::to_vec(&serde_json::json!({"keys": keys})).unwrap()
    )
    .await;
    assert_eq!(status, 413);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_TOO_MANY_KEYS"
    );
    // invalid key "." → KV_KEY_INVALID
    let (status, _, body) = post!(
        "get",
        serde_json::to_vec(&serde_json::json!({"keys": ["."]})).unwrap()
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_KEY_INVALID"
    );
    // oversized key → 413 KV_KEY_TOO_LARGE
    let big = "x".repeat(513);
    let (status, _, body) = post!(
        "get",
        serde_json::to_vec(&serde_json::json!({"keys": [big]})).unwrap()
    )
    .await;
    assert_eq!(status, 413);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_KEY_TOO_LARGE"
    );
    // cacheTtl below minimum → KV_INVALID_OPTIONS
    let (status, _, body) = post!(
        "get",
        serde_json::to_vec(&serde_json::json!({"keys": ["k"], "cacheTtl": 10})).unwrap()
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_INVALID_OPTIONS"
    );
    // list limit 0 → KV_INVALID_OPTIONS
    let (status, _, body) = post!(
        "list",
        serde_json::to_vec(&serde_json::json!({"limit": 0})).unwrap()
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_INVALID_OPTIONS"
    );
    // malformed JSON body → BINDING_PROTOCOL_ERROR
    let (status, _, body) = post!("get", b"not json".as_slice()).await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "BINDING_PROTOCOL_ERROR"
    );
    // put frame with expiration too soon → KV_INVALID_OPTIONS
    let soon = chrono::Utc::now().timestamp() + 30;
    let body = put_frame(&serde_json::json!({"key": "k", "expiration": soon}), b"v");
    let (status, _, body) = post!("put", body).await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "KV_INVALID_OPTIONS"
    );

    let _ = shutdown.send(());
}

#[tokio::test]
async fn r2_protocol_end_to_end() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;
    let json_headers = r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE);
    let frame_headers = r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE);

    // put → framed request, JSON metadata response
    let put_body = put_frame(
        &serde_json::json!({"key": "dir/obj.txt", "options": {"customMetadata": {"a": "1"}}}),
        b"object-bytes",
    );
    let (status, _, body) = http_post(
        socket,
        &r2_path("put"),
        &header_ref(&frame_headers),
        &put_body,
    )
    .await;
    assert_eq!(status, 200, "r2 put");
    let meta: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(meta["key"], "dir/obj.txt");
    assert_eq!(meta["size"], 12);
    assert!(meta["etag"].as_str().unwrap().len() == 32);
    assert_eq!(meta["customMetadata"]["a"], "1");

    // head → JSON metadata
    let (status, _, body) = http_post(
        socket,
        &r2_path("head"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "dir/obj.txt"})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "r2 head");
    let head_meta: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(head_meta["etag"], meta["etag"]);

    // head missing → 204
    let (status, _, _) = http_post(
        socket,
        &r2_path("head"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "nope"})).unwrap(),
    )
    .await;
    assert_eq!(status, 204);

    // get → frame response {meta, hasBody:true} + bytes
    let (status, headers, body) = http_post(
        socket,
        &r2_path("get"),
        &header_ref(&frame_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "dir/obj.txt", "options": {}})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "r2 get");
    assert_eq!(
        headers
            .iter()
            .find(|(n, _)| n == "content-type")
            .map(|(_, v)| v.as_str()),
        Some(R2_FRAME_CONTENT_TYPE)
    );
    let header_len = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
    let frame: serde_json::Value = serde_json::from_slice(&body[4..4 + header_len]).unwrap();
    assert_eq!(frame["hasBody"], true);
    assert_eq!(frame["meta"]["key"], "dir/obj.txt");
    assert_eq!(&body[4 + header_len..], b"object-bytes");

    // ranged get
    let (status, _, body) = http_post(
        socket, &r2_path("get"), &header_ref(&frame_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "dir/obj.txt", "options": {"range": {"offset": 0, "length": 6}}})).unwrap(),
    ).await;
    assert_eq!(status, 200);
    let header_len = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
    let frame: serde_json::Value = serde_json::from_slice(&body[4..4 + header_len]).unwrap();
    assert_eq!(frame["meta"]["range"]["offset"], 0);
    assert_eq!(&body[4 + header_len..], b"object");

    // get missing → 204
    let (status, _, _) = http_post(
        socket,
        &r2_path("get"),
        &header_ref(&frame_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "nope", "options": {}})).unwrap(),
    )
    .await;
    assert_eq!(status, 204);

    // list → objects + delimitedPrefixes
    let (status, _, body) = http_post(
        socket,
        &r2_path("list"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"limit": 100, "delimiter": "/"})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "r2 list");
    let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(page["truncated"], false);
    assert_eq!(page["delimitedPrefixes"], serde_json::json!(["dir/"]));
    assert!(page["objects"].as_array().unwrap().is_empty());

    // delete → 204
    let (status, _, _) = http_post(
        socket,
        &r2_path("delete"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"keys": ["dir/obj.txt"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 204, "r2 delete");

    // conditional put on missing object (etagMatches) → 204
    let put_body = put_frame(
        &serde_json::json!({"key": "new-obj", "options": {"onlyIf": {"etagMatches": [{"kind": "wildcard"}]}}}),
        b"x",
    );
    let (status, _, _) = http_post(
        socket,
        &r2_path("put"),
        &header_ref(&frame_headers),
        &put_body,
    )
    .await;
    assert_eq!(status, 204);

    // W5: multipart create now succeeds — returns `{key, uploadId}`
    // where uploadId is a signed opaque token.
    let (status, _, body) = http_post(
        socket,
        &r2_path("createMultipartUpload"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "k", "options": {}})).unwrap(),
    )
    .await;
    assert_eq!(status, 200);
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(created["key"], "k");
    assert!(created["uploadId"].as_str().unwrap().len() > 32);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn r2_denials_and_protocol_errors() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;
    let json_headers = r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE);
    let frame_headers = r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE);

    // forged binding id → 403 (binding not found on worker)
    let mut forged = standard_headers(
        &vrt_provider::identity::binding_id(WORKER, "NOPE"),
        WORKER,
        VERSION,
        DIGEST,
        BUCKET,
        7,
    );
    forged.push(("content-type".to_string(), R2_JSON_CONTENT_TYPE.to_string()));
    let (status, _, body) = http_post(
        socket,
        &format!("/internal/bindings/v1/r2/{}~NOPE/list", WORKER),
        &header_ref(&forged),
        &serde_json::to_vec(&serde_json::json!({"limit": 1})).unwrap(),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "BINDING_PERMISSION_DENIED"
    );

    // KV binding name on R2 path → 403 kind mismatch
    let mut wrong = standard_headers(
        &vrt_provider::identity::binding_id(WORKER, "KV"),
        WORKER,
        VERSION,
        DIGEST,
        NS_ID,
        7,
    );
    wrong.push(("content-type".to_string(), R2_JSON_CONTENT_TYPE.to_string()));
    let (status, _, _body) = http_post(
        socket,
        &format!("/internal/bindings/v1/r2/{}~KV/list", WORKER),
        &header_ref(&wrong),
        &serde_json::to_vec(&serde_json::json!({"limit": 1})).unwrap(),
    )
    .await;
    assert_eq!(status, 403);

    // get with JSON content-type (must be frame) → BINDING_PROTOCOL_ERROR
    let (status, _, body) = http_post(
        socket,
        &r2_path("get"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"key": "k", "options": {}})).unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "BINDING_PROTOCOL_ERROR"
    );

    // oversized key → R2_KEY_TOO_LARGE
    let big = "x".repeat(1025);
    let (status, _, body) = http_post(
        socket,
        &r2_path("head"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"key": big})).unwrap(),
    )
    .await;
    assert_eq!(status, 413);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "R2_KEY_TOO_LARGE"
    );

    // delete with empty keys → R2_INVALID_OPTIONS
    let (status, _, body) = http_post(
        socket,
        &r2_path("delete"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"keys": []})).unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "R2_INVALID_OPTIONS"
    );

    // list include with unknown field → R2_INVALID_OPTIONS
    let (status, _, body) = http_post(
        socket,
        &r2_path("list"),
        &header_ref(&json_headers),
        &serde_json::to_vec(&serde_json::json!({"limit": 10, "include": ["bogus"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "R2_INVALID_OPTIONS"
    );

    // SSE-C against the in-memory backend → R2_SSEC_INVALID (fail closed;
    // only the S3 backend can honor customer-provided keys).
    let (status, _, body) = http_post(
        socket,
        &r2_path("get"),
        &header_ref(&frame_headers),
        &serde_json::to_vec(
            &serde_json::json!({"key": "k", "options": {"ssecKey": "ab".repeat(32)}}),
        )
        .unwrap(),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "R2_SSEC_INVALID"
    );

    // unknown path under r2 prefix → 400 BINDING_PROTOCOL_ERROR
    let (status, _, _) = http_post(
        socket,
        "/internal/bindings/v1/r2/noid",
        &header_ref(&json_headers),
        b"{}",
    )
    .await;
    assert_eq!(status, 400);

    let _ = shutdown.send(());
}

#[tokio::test]
async fn missing_snapshot_fails_closed_over_http() {
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(MemoryDesiredStore::new(1), 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: None,
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    let (socket, shutdown) = spawn_tenant(svc).await;
    let (status, _, body) = http_post(
        socket,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"keys": ["k"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 503);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "RESOURCE_UNAVAILABLE"
    );
    let _ = shutdown.send(());
}

#[test]
fn listener_dir_hardening() {
    // unix `bind()` is denied in the test sandbox, so this covers the
    // fail-closed *validation* path (which runs before bind) — the bind
    // and `0600` chmod assertions require a permissive environment.
    let dir = TestDir::new();
    unixsock::validate_listener_dir(dir.path()).unwrap();

    // mode != 0700 → refuse before bind
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    assert!(unixsock::validate_listener_dir(dir.path()).is_err());
    assert!(unixsock::bind_listener(dir.path(), TENANT).is_err());
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    unixsock::validate_listener_dir(dir.path()).unwrap();

    // not a directory → refuse
    let file = dir.path().join("a-file");
    std::fs::write(&file, b"x").unwrap();
    assert!(unixsock::validate_listener_dir(&file).is_err());

    // regular file at socket path → refuse (checked before bind)
    let path = unixsock::socket_path(dir.path(), TENANT);
    std::fs::write(&path, b"not a socket").unwrap();
    let err = unixsock::bind_listener(dir.path(), TENANT).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

    // tenant id injection never escapes the directory
    let injected = unixsock::socket_path(dir.path(), "../escape");
    assert!(injected.parent().unwrap() == dir.path());
    assert!(!injected.to_string_lossy().contains("escape/"));
    let injected = unixsock::socket_path(dir.path(), r"a/b\..c");
    assert!(injected.parent().unwrap() == dir.path());
}

#[tokio::test]
async fn graceful_shutdown_drains() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;

    // in-flight request completes before shutdown finishes
    let sock = socket;
    let headers = kv_headers(&[]);
    let request = tokio::spawn(async move {
        http_post(
            sock,
            &kv_path("get"),
            &header_ref(&headers),
            &serde_json::to_vec(&serde_json::json!({"keys": ["k"]})).unwrap(),
        )
        .await
    });
    let (status, _, _) = request.await.unwrap();
    assert_eq!(status, 200);
    let _ = shutdown.send(());
    // server exits; new connections are refused once drained
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(socket).await.is_err() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("listener did not shut down");
}

/// Build a service whose worker carries the given bindings; used by the
/// permission-enforcement tests where grants differ per binding.
fn perm_service(bindings: Vec<BindingDesired>) -> Arc<TenantService> {
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
    Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    })
}

#[tokio::test]
async fn kv_desired_permissions_are_enforced() {
    let svc = perm_service(vec![
        kv_binding_perms("KVRO", NS_ID, true, false),
        kv_binding_perms("KVWO", "ns-write-only", false, true),
    ]);
    let (socket, shutdown) = spawn_tenant(svc).await;

    let hdrs = |binding: &str| {
        let id = vrt_provider::identity::binding_id(WORKER, binding);
        let resource = if binding == "KVRO" {
            NS_ID
        } else {
            "ns-write-only"
        };
        let mut h = standard_headers(&id, WORKER, VERSION, DIGEST, resource, 7);
        h.push((
            "content-type".to_string(),
            KV_FRAME_CONTENT_TYPE.to_string(),
        ));
        h
    };
    let denied_code = |body: &[u8]| {
        serde_json::from_slice::<serde_json::Value>(body).unwrap()["error"]["code"]
            .as_str()
            .unwrap()
            .to_string()
    };

    // read-only binding: writes denied, reads allowed.
    for op in ["put", "delete"] {
        let body = if op == "put" {
            put_frame(&serde_json::json!({"key": "a"}), b"v")
        } else {
            serde_json::to_vec(&serde_json::json!({"key": "a"})).unwrap()
        };
        let (status, _, body) = http_post(
            socket,
            &format!("/internal/bindings/v1/kv/{WORKER}~KVRO/{op}"),
            &header_ref(&hdrs("KVRO")),
            &body,
        )
        .await;
        assert_eq!(status, 403, "KVRO {op}");
        assert_eq!(denied_code(&body), "BINDING_PERMISSION_DENIED");
    }
    for op in ["get", "list"] {
        let body = if op == "get" {
            serde_json::to_vec(&serde_json::json!({"keys": ["a"]})).unwrap()
        } else {
            serde_json::to_vec(&serde_json::json!({"prefix": "", "limit": 1, "cursor": null}))
                .unwrap()
        };
        let (status, _, _) = http_post(
            socket,
            &format!("/internal/bindings/v1/kv/{WORKER}~KVRO/{op}"),
            &header_ref(&hdrs("KVRO")),
            &body,
        )
        .await;
        assert_eq!(status, 200, "KVRO {op} stays readable");
    }

    // write-only binding: reads denied, write allowed.
    for (op, body) in [
        (
            "get",
            serde_json::to_vec(&serde_json::json!({"keys": ["a"]})).unwrap(),
        ),
        (
            "list",
            serde_json::to_vec(&serde_json::json!({"prefix": "", "limit": 1, "cursor": null}))
                .unwrap(),
        ),
    ] {
        let (status, _, body) = http_post(
            socket,
            &format!("/internal/bindings/v1/kv/{WORKER}~KVWO/{op}"),
            &header_ref(&hdrs("KVWO")),
            &body,
        )
        .await;
        assert_eq!(status, 403, "KVWO {op}");
        assert_eq!(denied_code(&body), "BINDING_PERMISSION_DENIED");
    }
    let (status, _, _) = http_post(
        socket,
        &format!("/internal/bindings/v1/kv/{WORKER}~KVWO/put"),
        &header_ref(&hdrs("KVWO")),
        &put_frame(&serde_json::json!({"key": "a"}), b"v"),
    )
    .await;
    assert_eq!(status, 200, "KVWO put allowed");
    let _ = shutdown.send(());
}

#[tokio::test]
async fn r2_desired_permissions_are_enforced() {
    let svc = perm_service(vec![
        r2_binding_perms("BRO", BUCKET, true, false),
        r2_binding_perms("BWO", "bucket-write-1", false, true),
    ]);
    let (socket, shutdown) = spawn_tenant(svc).await;

    let hdrs = |binding: &str, ct: &str| {
        let id = vrt_provider::identity::binding_id(WORKER, binding);
        let resource = if binding == "BRO" {
            BUCKET
        } else {
            "bucket-write-1"
        };
        let mut h = standard_headers(&id, WORKER, VERSION, DIGEST, resource, 7);
        h.push(("content-type".to_string(), ct.to_string()));
        h
    };
    let put_body = put_frame(&serde_json::json!({"key": "o", "options": {}}), b"v");

    // read-only bucket: writes denied, reads allowed.
    for (op, ct, body) in [
        ("put", R2_FRAME_CONTENT_TYPE, put_body.clone()),
        (
            "delete",
            R2_JSON_CONTENT_TYPE,
            serde_json::to_vec(&serde_json::json!({"keys": ["o"]})).unwrap(),
        ),
    ] {
        let (status, _, body) = http_post(
            socket,
            &format!("/internal/bindings/v1/r2/{WORKER}~BRO/{op}"),
            &header_ref(&hdrs("BRO", ct)),
            &body,
        )
        .await;
        assert_eq!(status, 403, "BRO {op}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
            "BINDING_PERMISSION_DENIED"
        );
    }
    for (op, ct, body) in [
        (
            "head",
            R2_JSON_CONTENT_TYPE,
            serde_json::to_vec(&serde_json::json!({"key": "o"})).unwrap(),
        ),
        (
            "get",
            R2_FRAME_CONTENT_TYPE,
            serde_json::to_vec(&serde_json::json!({"key": "o", "options": {}})).unwrap(),
        ),
        (
            "list",
            R2_JSON_CONTENT_TYPE,
            serde_json::to_vec(&serde_json::json!({"limit": 1})).unwrap(),
        ),
    ] {
        let (status, _, _) = http_post(
            socket,
            &format!("/internal/bindings/v1/r2/{WORKER}~BRO/{op}"),
            &header_ref(&hdrs("BRO", ct)),
            &body,
        )
        .await;
        assert!(
            status == 200 || status == 204,
            "BRO {op} stays readable ({status})"
        );
    }

    // write-only bucket: reads denied, write allowed.
    for (op, ct, body) in [
        (
            "head",
            R2_JSON_CONTENT_TYPE,
            serde_json::to_vec(&serde_json::json!({"key": "o"})).unwrap(),
        ),
        (
            "list",
            R2_JSON_CONTENT_TYPE,
            serde_json::to_vec(&serde_json::json!({"limit": 1})).unwrap(),
        ),
    ] {
        let (status, _, body) = http_post(
            socket,
            &format!("/internal/bindings/v1/r2/{WORKER}~BWO/{op}"),
            &header_ref(&hdrs("BWO", ct)),
            &body,
        )
        .await;
        assert_eq!(status, 403, "BWO {op}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["code"],
            "BINDING_PERMISSION_DENIED"
        );
    }
    let (status, _, _) = http_post(
        socket,
        &format!("/internal/bindings/v1/r2/{WORKER}~BWO/put"),
        &header_ref(&hdrs("BWO", R2_FRAME_CONTENT_TYPE)),
        &put_body,
    )
    .await;
    assert_eq!(status, 200, "BWO put allowed");
    let _ = shutdown.send(());
}

// ------------------------------------------------------------------
// T13/H3 — bounded concurrency: exhausted permits answer 429, never queue
// ------------------------------------------------------------------

#[tokio::test]
async fn saturated_tenant_permits_answer_429() {
    // One-permit service: hold the permit, then a real request must be
    // rejected rather than queued.
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
                    vec![kv_binding("KV", NS_ID)],
                )],
            )],
        ),
        7,
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(1)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(1)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    // Hold the only global permit — the next request must 429 fast.
    let held = svc.global_permits.clone().try_acquire_owned().unwrap();
    let (socket, shutdown) = spawn_tenant(svc.clone()).await;
    let (status, _, body) = http_post(
        socket,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &serde_json::to_vec(&serde_json::json!({"keys": ["k1"]})).unwrap(),
    )
    .await;
    assert_eq!(status, 429);
    assert!(
        String::from_utf8_lossy(&body).contains("KV_BUSY"),
        "{body:?}"
    );
    drop(held);
    drop(shutdown);
}

// ------------------------------------------------- W5: multipart over HTTP

/// rw binding + a 64MiB object cap — the multipart e2e exercises the
/// upstream 5MiB minimum-part rule.
fn service_fixture_mp() -> (Fixture, Arc<TenantService>) {
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
                    vec![kv_binding("KV", NS_ID), r2_binding("BUCKET", BUCKET)],
                )],
            )],
        ),
        7,
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 64 * 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    (f, svc)
}

fn service_fixture_readonly_r2() -> (Fixture, Arc<TenantService>) {
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
                    vec![r2_binding_perms("BUCKET", BUCKET, true, false)],
                )],
            )],
        ),
        7,
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 64 * 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    (f, svc)
}

async fn mp_create(socket: std::net::SocketAddr, key: &str) -> (u16, serde_json::Value) {
    let (status, _, body) = http_post(
        socket,
        &r2_path("createMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": key,
            "options": {"customMetadata": {"k": "v"}},
        }))
        .unwrap(),
    )
    .await;
    (
        status,
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
async fn r2_multipart_end_to_end() {
    let (_f, svc) = service_fixture_mp();
    let (socket, shutdown) = spawn_tenant(svc).await;
    let part5 = vec![0xCDu8; 5 * 1024 * 1024];

    let (status, created) = mp_create(socket, "mp/a").await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(created["key"], "mp/a");
    let upload_id = created["uploadId"].as_str().unwrap().to_string();

    // part 1 (5MiB) + part 2 (small) framed uploadPart
    let mut etags = Vec::new();
    for (n, bytes) in [(1, part5.as_slice()), (2, b"tail".as_slice())] {
        let frame = put_frame(
            &serde_json::json!({
                "key": "mp/a",
                "uploadId": upload_id,
                "partNumber": n,
            }),
            bytes,
        );
        let (status, _, body) = http_post(
            socket,
            &r2_path("uploadPart"),
            &header_ref(&r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE)),
            &frame,
        )
        .await;
        assert_eq!(status, 200, "part {n}: {}", String::from_utf8_lossy(&body));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        etags.push(v["etag"].as_str().unwrap().to_string());
    }

    let (status, _, body) = http_post(
        socket,
        &r2_path("completeMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": "mp/a",
            "uploadId": upload_id,
            "parts": [
                {"partNumber": 1, "etag": etags[0]},
                {"partNumber": 2, "etag": etags[1]},
            ],
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(status, 200, "complete: {}", String::from_utf8_lossy(&body));
    let meta: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(meta["size"], (5 * 1024 * 1024 + 4) as u64);
    assert!(meta["etag"].as_str().unwrap().ends_with("-2"));
    assert_eq!(meta["customMetadata"]["k"], "v");

    // GET the committed object back
    let (status, _, body) = http_post(
        socket,
        &r2_path("get"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({"key": "mp/a", "options": {}})).unwrap(),
    )
    .await;
    assert_eq!(status, 200, "get");
    let hlen = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
    assert_eq!(&body[4 + hlen + 5 * 1024 * 1024..], b"tail");
    drop(shutdown);
}

#[tokio::test]
async fn r2_multipart_token_scope_enforced() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;

    let (status, created) = mp_create(socket, "mp/a").await;
    assert_eq!(status, 200);
    let upload_id = created["uploadId"].as_str().unwrap().to_string();

    // Cross-key replay: token minted for mp/a used on mp/b → invalid.
    let frame = put_frame(
        &serde_json::json!({
            "key": "mp/b",
            "uploadId": upload_id,
            "partNumber": 1,
        }),
        b"x",
    );
    let (status, _, body) = http_post(
        socket,
        &r2_path("uploadPart"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE)),
        &frame,
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        status == 400 && text.contains("R2_MULTIPART_INVALID"),
        "cross-key token accepted: {status} {text}"
    );

    // Tampered token → invalid.
    let mut forged = upload_id.into_bytes();
    let i = forged.len() - 5;
    forged[i] = if forged[i] == b'A' { b'B' } else { b'A' };
    let forged = String::from_utf8(forged).unwrap();
    let (status, _, body) = http_post(
        socket,
        &r2_path("completeMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": "mp/a",
            "uploadId": forged,
            "parts": [{"partNumber": 1, "etag": "aa"}],
        }))
        .unwrap(),
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        status == 400 && text.contains("R2_MULTIPART_INVALID"),
        "tampered token accepted: {status} {text}"
    );

    // Forged token entirely → invalid.
    let (status, _, body) = http_post(
        socket,
        &r2_path("abortMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": "mp/a",
            "uploadId": "forged-token",
        }))
        .unwrap(),
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        status == 400 && text.contains("R2_MULTIPART_INVALID"),
        "forged token accepted: {status} {text}"
    );

    // partNumber out of range → invalid.
    let (_, created) = mp_create(socket, "mp/range").await;
    let upload_id = created["uploadId"].as_str().unwrap().to_string();
    let frame = put_frame(
        &serde_json::json!({
            "key": "mp/range",
            "uploadId": upload_id,
            "partNumber": 0,
        }),
        b"x",
    );
    let (status, _, body) = http_post(
        socket,
        &r2_path("uploadPart"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE)),
        &frame,
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        status == 400 && text.contains("R2_MULTIPART_INVALID"),
        "partNumber 0 accepted: {status} {text}"
    );
    drop(shutdown);
}

#[tokio::test]
async fn r2_multipart_abort_then_complete_rejected() {
    let (_f, svc) = service_fixture();
    let (socket, shutdown) = spawn_tenant(svc).await;
    let (status, created) = mp_create(socket, "mp/ab").await;
    assert_eq!(status, 200);
    let upload_id = created["uploadId"].as_str().unwrap().to_string();
    let (status, _, _) = http_post(
        socket,
        &r2_path("abortMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": "mp/ab",
            "uploadId": upload_id,
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(status, 204, "abort");
    let (status, _, body) = http_post(
        socket,
        &r2_path("completeMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": "mp/ab",
            "uploadId": upload_id,
            "parts": [{"partNumber": 1, "etag": "aa"}],
        }))
        .unwrap(),
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        status == 400 && text.contains("R2_MULTIPART_INVALID"),
        "complete after abort: {status} {text}"
    );
    drop(shutdown);
}

#[tokio::test]
async fn r2_multipart_requires_write_permission() {
    let (_f, svc) = service_fixture_readonly_r2();
    let (socket, shutdown) = spawn_tenant(svc).await;
    let (status, _, body) = http_post(
        socket,
        &r2_path("createMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({"key": "k"})).unwrap(),
    )
    .await;
    assert_eq!(
        status,
        403,
        "read-only binding must not create uploads: {}",
        String::from_utf8_lossy(&body)
    );
    drop(shutdown);
}

// ------------------------------------------------- W5-r2: timeout semantics

/// Provider whose `complete` hangs far beyond the request deadline —
/// exercises the outer-timeout → `R2_RESULT_UNKNOWN` mapping (the
/// commit may have landed; a retry must not report a false failure).
#[derive(Debug)]
struct HangingCompleteR2(MemoryR2Provider);

#[async_trait::async_trait]
impl vrt_provider::r2::R2Provider for HangingCompleteR2 {
    async fn head(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
    ) -> Result<Option<vrt_provider::r2::wire::R2ObjectMetadata>, vrt_provider::ProviderError> {
        self.0.head(ctx, key).await
    }
    async fn get(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
        range: Option<vrt_provider::r2::wire::R2Range>,
        only_if: Option<&vrt_provider::r2::wire::R2Condition>,
        ssec_key: Option<&vrt_provider::r2::wire::R2SsecKey>,
    ) -> Result<vrt_provider::r2::R2GetResult, vrt_provider::ProviderError> {
        self.0.get(ctx, key, range, only_if, ssec_key).await
    }
    async fn put(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
        body: &[u8],
        options: &vrt_provider::r2::wire::R2PutOptions,
    ) -> Result<Option<vrt_provider::r2::wire::R2ObjectMetadata>, vrt_provider::ProviderError> {
        self.0.put(ctx, key, body, options).await
    }
    async fn delete(
        &self,
        ctx: &vrt_provider::BindingContext,
        keys: &[String],
    ) -> Result<(), vrt_provider::ProviderError> {
        self.0.delete(ctx, keys).await
    }
    async fn list(
        &self,
        ctx: &vrt_provider::BindingContext,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
    ) -> Result<vrt_provider::r2::wire::R2ListPage, vrt_provider::ProviderError> {
        self.0.list(ctx, prefix, delimiter, after, limit).await
    }
    async fn create_multipart(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
        options: &vrt_provider::r2::wire::R2MultipartCreateOptions,
    ) -> Result<vrt_provider::r2::wire::MultipartCreateResult, vrt_provider::ProviderError> {
        self.0.create_multipart(ctx, key, options).await
    }
    async fn upload_part(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: &[u8],
        ssec_key: Option<&vrt_provider::r2::wire::R2SsecKey>,
    ) -> Result<vrt_provider::r2::wire::R2UploadedPart, vrt_provider::ProviderError> {
        self.0
            .upload_part(ctx, key, upload_id, part_number, body, ssec_key)
            .await
    }
    async fn complete_multipart(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
        upload_id: &str,
        parts: &[vrt_provider::r2::wire::R2UploadedPart],
    ) -> Result<Option<vrt_provider::r2::wire::R2ObjectMetadata>, vrt_provider::ProviderError> {
        // Simulate S3's CompleteMultipartUpload accepted-but-slow: the
        // call outlives the request deadline + the server-side grace.
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        self.0.complete_multipart(ctx, key, upload_id, parts).await
    }
    async fn abort_multipart(
        &self,
        ctx: &vrt_provider::BindingContext,
        key: &str,
        upload_id: &str,
    ) -> Result<(), vrt_provider::ProviderError> {
        self.0.abort_multipart(ctx, key, upload_id).await
    }
}

#[tokio::test]
async fn multipart_complete_outer_timeout_is_result_unknown() {
    // Tiny operation deadline: create/upload fit (in-memory ops are
    // microseconds), complete hangs → outer timeout must answer
    // R2_RESULT_UNKNOWN, never a retryable-false-failure code.
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
                    vec![r2_binding("BUCKET", BUCKET)],
                )],
            )],
        ),
        7,
    );
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 50)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(HangingCompleteR2(MemoryR2Provider::new()))),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 64 * 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    let (socket, shutdown) = spawn_tenant(svc).await;
    let (status, created) = mp_create(socket, "mp/t").await;
    assert_eq!(status, 200, "{created}");
    let upload_id = created["uploadId"].as_str().unwrap();
    let frame = put_frame(
        &serde_json::json!({
            "key": "mp/t",
            "uploadId": upload_id,
            "partNumber": 1,
        }),
        b"x",
    );
    let (status, _, body) = http_post(
        socket,
        &r2_path("uploadPart"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_FRAME_CONTENT_TYPE)),
        &frame,
    )
    .await;
    assert_eq!(status, 200, "part: {}", String::from_utf8_lossy(&body));
    let etag = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["etag"]
        .as_str()
        .unwrap()
        .to_string();
    // Complete: provider hangs 30s ≫ deadline(50ms)+grace(2s) →
    // outer timeout fires → honest RESULT_UNKNOWN.
    let (status, _, body) = http_post(
        socket,
        &r2_path("completeMultipartUpload"),
        &header_ref(&r2_headers("BUCKET", BUCKET, R2_JSON_CONTENT_TYPE)),
        &serde_json::to_vec(&serde_json::json!({
            "key": "mp/t",
            "uploadId": upload_id,
            "parts": [{"partNumber": 1, "etag": etag}],
        }))
        .unwrap(),
    )
    .await;
    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, 503, "{envelope}");
    assert_eq!(envelope["error"]["code"], "R2_RESULT_UNKNOWN", "{envelope}");
    assert_eq!(envelope["error"]["resultUnknown"], true, "{envelope}");
    let _ = shutdown.send(());
}

// ------------------------------------------------- RV-R8 #2/#3: concurrency isolation + permit lifetime

fn tenant_svc(tenant_permits: usize, global: &Arc<tokio::sync::Semaphore>) -> Arc<TenantService> {
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
                    vec![kv_binding("KV", NS_ID)],
                )],
            )],
        ),
        7,
    );
    Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(authorizer(store, 30_000)),
        kv: Some(Arc::new(MemoryKvProvider::new())),
        assets: None,
        r2: Some(Arc::new(MemoryR2Provider::new())),
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(tenant_permits)),
        global_permits: global.clone(),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    })
}

#[tokio::test]
async fn per_tenant_permits_isolate_and_global_still_bounds() {
    // RV-R8 #2: two listeners = two independent tenant semaphores over
    // ONE global semaphore. Saturating tenant A must not block B;
    // draining the global pool must refuse both.
    let global = Arc::new(tokio::sync::Semaphore::new(2));
    let svc_a = tenant_svc(1, &global);
    let svc_b = tenant_svc(1, &global);
    let (sock_a, shutdown_a) = spawn_tenant(svc_a.clone()).await;
    let (sock_b, shutdown_b) = spawn_tenant(svc_b.clone()).await;
    let body = serde_json::to_vec(&serde_json::json!({"keys": ["k1"]})).unwrap();

    // A's only tenant slot held → its request 429s (acquire timeout),
    // while B still answers normally.
    let held_a = svc_a.permits.clone().try_acquire_owned().unwrap();
    let (status, _, resp) = http_post(
        sock_a,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &body,
    )
    .await;
    assert_eq!(status, 429, "{resp:?}");
    assert!(String::from_utf8_lossy(&resp).contains("KV_BUSY"));
    let (status, _, resp) = http_post(
        sock_b,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &body,
    )
    .await;
    assert_eq!(status, 200, "tenant B unaffected: {resp:?}");

    // Global pool drained → B also refused: the global cap still binds
    // when tenant slots are free.
    let g1 = global.clone().try_acquire_owned().unwrap();
    let g2 = global.clone().try_acquire_owned().unwrap();
    let (status, _, _) = http_post(
        sock_b,
        &kv_path("get"),
        &header_ref(&kv_headers(&[])),
        &body,
    )
    .await;
    assert_eq!(status, 429, "global saturation still refuses");
    drop((held_a, g1, g2, shutdown_a, shutdown_b));
}

#[tokio::test]
async fn permit_held_until_response_body_drained() {
    // RV-R8 #3: a 1-slot service's permit must stay inside the response
    // body — a request that returned headers but whose body is still
    // open keeps the slot; a second request can only proceed once the
    // first body is fully consumed/dropped.
    let global = Arc::new(tokio::sync::Semaphore::new(4));
    let svc = tenant_svc(1, &global);
    let router = tenant_router(svc.clone());
    let body = serde_json::to_vec(&serde_json::json!({"keys": ["k1"]})).unwrap();
    let mut req = axum::http::Request::builder()
        .method("POST")
        .uri(kv_path("get"));
    for (k, v) in kv_headers(&[]) {
        req = req.header(k, v);
    }
    let req = req.body(axum::body::Body::from(body)).unwrap();
    let res = tower::ServiceExt::oneshot(router, req).await.unwrap();
    // Headers are produced; the body hasn't been consumed — the permit
    // must still be inside it.
    assert_eq!(
        svc.permits.available_permits(),
        0,
        "slot held while body open"
    );
    use http_body_util::BodyExt;
    let _ = res.into_body().collect().await.unwrap();
    assert_eq!(
        svc.permits.available_permits(),
        1,
        "slot released after body drained"
    );
}

#[tokio::test]
async fn kv_body_read_deadline_returns_typed_busy() {
    // RV-R8 #6: a client that starts a frame body then stalls must hit
    // the body-read deadline — a typed error, never an infinite hang.
    let global = Arc::new(tokio::sync::Semaphore::new(4));
    let svc = tenant_svc(4, &global);
    // Shrink the deadline for the test.
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: svc.authorizer.clone(),
        kv: svc.kv.clone(),
        assets: None,
        r2: svc.r2.clone(),
        signer: svc.signer.clone(),
        r2_max_object_bytes: 1024 * 1024,
        permits: svc.permits.clone(),
        global_permits: svc.global_permits.clone(),
        body_read_timeout: std::time::Duration::from_millis(150),
        d1: None,
        queue: None,
        k2: None,
        email: None,
        pg: None,
    });
    let (socket, shutdown) = spawn_tenant(svc).await;

    // POST with Content-Length 4096 but only a partial body — the
    // server must answer the typed error after ~150ms rather than
    // waiting forever.
    let mut stream = tokio::net::TcpStream::connect(socket).await.unwrap();
    let mut req = format!(
        "POST {} HTTP/1.1\r\nHost: unix\r\nContent-Length: 4096\r\nConnection: close\r\n",
        kv_path("get")
    );
    for (k, v) in kv_headers(&[]) {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(&[b'{'; 64]).await.unwrap(); // partial body, then stall
    let mut raw = Vec::new();
    let read = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut chunk = [0u8; 8192];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => raw.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(e) => panic!("read: {e}"),
            }
        }
    })
    .await;
    read.expect("server must answer within the body deadline");
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.starts_with("HTTP/1.1 503") || text.contains("KV_BUSY"),
        "typed busy expected, got: {text}"
    );
    drop(shutdown);
}
