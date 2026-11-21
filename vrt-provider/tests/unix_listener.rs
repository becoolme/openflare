//! Unix-listener integration test — the production binding-backend
//! address family. The sandbox denies unix `bind()`, so this is
//! `#[ignore]`d and gated on `VRT_HOST_UNIX_TESTS=1` (missing → print
//! and return, never fail):
//!
//! ```sh
//! VRT_HOST_UNIX_TESTS=1 cargo test --workspace --offline -- --ignored --test-threads=1
//! ```

mod support;

use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::kv::MemoryKvProvider;
use vrt_provider::server::{tenant_router, TenantService};
use vrt_provider::unixsock;

fn enabled() -> bool {
    if std::env::var("VRT_HOST_UNIX_TESTS").ok().as_deref() == Some("1") {
        true
    } else {
        eprintln!("skip: set VRT_HOST_UNIX_TESTS=1 (unix bind() unavailable in the sandbox)");
        false
    }
}

#[tokio::test]
#[ignore = "requires VRT_HOST_UNIX_TESTS=1 and unix bind() allowed"]
async fn unix_listener_serves_kv_protocol() {
    if !enabled() {
        return;
    }
    // Real per-tenant unix listener: bind → perms → serve → KVS1 reply.
    let dir = TestDir::new();
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
        r2: None,
        signer: Arc::new(CursorSigner::new(b"cursor-secret")),
        r2_max_object_bytes: 1024 * 1024,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(512)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        k2: None,
        pg: None,
    });

    let listener = unixsock::bind_listener(dir.path(), TENANT).expect("bind");
    let sock = unixsock::socket_path(dir.path(), TENANT);
    let meta = std::fs::metadata(&sock).expect("socket file");
    assert_eq!(meta.permissions().mode() & 0o777, 0o600, "socket mode");
    assert_eq!(meta.uid(), unsafe { libc::geteuid() }, "socket owner");
    assert_eq!(
        std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
        0o700,
        "listener dir"
    );

    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        unixsock::serve(listener, tenant_router(svc), async move {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });

    // Real HTTP/1.1 over UnixStream: KV `get` → KVS1 frame.
    let binding = vrt_provider::identity::binding_id(WORKER, "KV");
    let mut request = format!(
        "POST /internal/bindings/v1/kv/{WORKER}~KV/get HTTP/1.1\r\n\
         Host: unix\r\nConnection: close\r\n"
    );
    for (k, v) in standard_headers(&binding, WORKER, VERSION, DIGEST, NS_ID, 7) {
        request.push_str(&format!("{k}: {v}\r\n"));
    }
    let body = serde_json::to_vec(&serde_json::json!({"keys": ["alpha"]})).unwrap();
    request.push_str(&format!(
        "content-type: {}\r\ncontent-length: {}\r\n\r\n",
        vrt_provider::limits::KV_FRAME_CONTENT_TYPE,
        body.len()
    ));
    let mut stream = tokio::net::UnixStream::connect(&sock)
        .await
        .expect("connect unix listener");
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw);
    assert!(text.starts_with("HTTP/1.1 200"), "response: {text}");
    assert!(raw.windows(4).any(|w| w == b"KVS1"), "KVS1 frame: {raw:?}");

    let _ = tx.send(());
    task.await.unwrap();
    // Listener dropped; socket file remains bound-path side — remove it
    // (production removes stale sockets at next bind).
    let _ = std::fs::remove_file(&sock);
}
