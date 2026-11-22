//! A2: remote `of deploy` — client gather → upload wire body →
//! server-side revalidation + signed publish. The TCP listener is
//! exercised over real loopback HTTP; the unix socket is untouched.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use vrt_local::{InitParams, VrtLocal};

struct TmpDir(PathBuf);
impl std::ops::Deref for TmpDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Unique per test: `Instant`'s `Debug` output is not a uniqueness
/// guarantee (two threads can observe the same clock tick), and sharing a
/// root makes `init_begin` delete the other test's staging dir as crash
/// residue. The rest of the suite already uses this pid+sequence shape.
fn root() -> TmpDir {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-remote-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    // A reused pid may leave a directory from an interrupted earlier run;
    // `init` fails closed on a non-empty root, so clear ours first.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TmpDir(dir)
}

fn init(root: &Path) -> VrtLocal {
    VrtLocal::init(
        root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: None,
        },
    )
    .unwrap();
    VrtLocal::open(root).unwrap()
}

/// A worker dir fixture: vrt.toml + two modules + a var + a secret
/// binding + a kv binding + an `[assets]` dir.
fn worker_dir(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(format!("w-{name}"));
    std::fs::create_dir_all(dir.join("public")).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n\
             \n[vars]\nLOG_LEVEL = \"info\"\n\
             \n[secrets]\nAPI_KEY = \"api-key-ref\"\n\
             \n[bindings]\nkv = {{ CACHE = \"ns-1\" }}\n\
             \n[assets]\ndir = \"public\"\nbinding = \"SITE\"\n"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    std::fs::write(dir.join("util.js"), "export const x = 1\n").unwrap();
    std::fs::write(dir.join("public/index.html"), "<h1>hi</h1>\n").unwrap();
    std::fs::write(dir.join(".dev.vars"), "IGNORED_DOTFILE=1\n").unwrap();
    dir
}

/// Build the wire body exactly as `cmd_deploy` does (gather → b64).
fn upload_body(dir: &Path) -> vrt_local::worker_pkg::DeployUpload {
    use base64::Engine;
    let g = vrt_local::worker_pkg::gather_dir(dir).unwrap();
    vrt_local::worker_pkg::DeployUpload {
        meta: g.meta.clone(),
        modules: g
            .files
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    base64::engine::general_purpose::STANDARD.encode(v),
                )
            })
            .collect(),
        assets: g
            .pending_assets
            .as_ref()
            .map(|p| vrt_local::worker_pkg::UploadAssets {
                manifest: base64::engine::general_purpose::STANDARD.encode(&p.manifest_bytes),
                objects: p
                    .objects
                    .iter()
                    .map(|(d, b)| {
                        (
                            d.clone(),
                            base64::engine::general_purpose::STANDARD.encode(b),
                        )
                    })
                    .collect(),
            }),
        vars: BTreeMap::from([("EXTRA".to_string(), "yes".to_string())]),
        secrets: BTreeMap::from([("api-key-ref".to_string(), "s3cr3t-value".to_string())]),
        name: None,
        tag: None,
        flagship_apps: g.flagship_apps.clone(),
    }
}

/// Spin the TCP management router on an ephemeral loopback port.
/// Returns the bound addr; the task aborts on test end.
async fn tcp_serve(root: &Path, token: &[u8]) -> std::net::SocketAddr {
    let (_router, api) = vrt_local::mgmt::router(root.to_path_buf()).unwrap();
    let tcp = vrt_local::mgmt::tcp_router(api, token.to_vec(), vrt_local::mgmt::RateLimit::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            tcp.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    addr
}

async fn post(
    addr: std::net::SocketAddr,
    token: Option<&[u8]>,
    path: &str,
    body: &[u8],
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = token
        .map(|t| format!("authorization: Bearer {}\r\n", String::from_utf8_lossy(t)))
        .unwrap_or_default();
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nhost: x\r\n{auth}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    ).into_bytes();
    req.extend_from_slice(body);
    // One write; a rejecting middleware may close before reading the
    // body — the response bytes still arrive, so a write reset is not
    // fatal, only a missing response is.
    let _ = s.write_all(&req).await;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    let (head, out) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    (status, out.to_string())
}

const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// End-to-end over the TCP listener: upload → deploy lands → secrets
/// and vars written → second identical upload still 200 (idempotent).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_roundtrip_idempotent() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "edge");
    let addr = tcp_serve(&root, TOKEN.as_bytes()).await;

    let body = serde_json::to_vec(&upload_body(&dir)).unwrap();
    let (status, text) = post(addr, Some(TOKEN.as_bytes()), "/v1/deploy/upload", &body).await;
    assert_eq!(status, 200, "deploy rejected: {text}");
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(v["versionId"].as_str().is_some(), "{v}");

    let st = local.status().unwrap();
    assert_eq!(st.workers.len(), 1);
    assert_eq!(st.workers[0].name, "edge");
    assert_eq!(st.secret_refs, vec!["api-key-ref".to_string()]);

    // Secret value materialized (name-check only — never echoed).
    let secrets: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("secrets/secrets.json")).unwrap()).unwrap();
    assert_eq!(secrets["local"]["api-key-ref"], "s3cr3t-value");
    // --var value landed in the version-scoped vars union.
    let vars: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("vars/vars.json")).unwrap()).unwrap();
    assert!(
        vars.as_object().unwrap().values().any(|v| v == "yes"),
        "uploaded var missing from vars.json: {vars}"
    );

    // Idempotent: identical upload redeploys cleanly.
    let (status2, text2) = post(addr, Some(TOKEN.as_bytes()), "/v1/deploy/upload", &body).await;
    assert_eq!(status2, 200, "re-upload rejected: {text2}");
}

/// Wrong / missing token → 401; the upload body is never read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_failures() {
    let root = root();
    let _local = init(&root);
    let addr = tcp_serve(&root, TOKEN.as_bytes()).await;
    let body = br#"{"meta":{}}"#;
    let (s1, _) = post(addr, None, "/v1/deploy/upload", body).await;
    assert_eq!(s1, 401);
    let (s2, _) = post(addr, Some(b"wrong"), "/v1/deploy/upload", body).await;
    assert_eq!(s2, 401);
    // Right token → past auth (body then fails validation = 4xx≠401).
    let (s3, _) = post(addr, Some(TOKEN.as_bytes()), "/v1/deploy/upload", body).await;
    assert_ne!(s3, 401);
}

/// Wire caps: over-limit Content-Length is refused 413 before the
/// body is even read; the global 64KiB cap still guards other routes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_limits() {
    let root = root();
    let _local = init(&root);
    let addr = tcp_serve(&root, TOKEN.as_bytes()).await;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Declared Content-Length above the 200MB upload cap → 413 while
    // the (never-arriving) body is still in flight — the gate answers
    // off the header alone.
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST /v1/deploy/upload HTTP/1.1\r\nhost: x\r\nauthorization: Bearer {TOKEN}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
        201 * 1024 * 1024
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut buf))
        .await
        .expect("no response to declared-oversize body")
        .unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(
        text.contains(" 413"),
        "expected 413, got: {}",
        &text[..text.len().min(200)]
    );
    drop(s);
    // Global 64KiB cap on a non-upload route: a padded-but-valid
    // JSON body over the cap is rejected before parsing, while a
    // small well-formed body reaches the handler and fails cleanly —
    // together they prove the cap, not just a syntax rejection.
    let pad = "x".repeat(70 * 1024);
    let big = format!("{{\"workerDir\":\"/nonexistent-{pad}\"}}");
    let (s2, b2) = post(addr, Some(TOKEN.as_bytes()), "/v1/deploy", big.as_bytes()).await;
    assert_eq!(s2, 400);
    assert!(b2.contains("\"body\""), "expected body rejection, got {b2}");
    let (s3, b3) = post(
        addr,
        Some(TOKEN.as_bytes()),
        "/v1/deploy",
        b"{\"workerDir\":\"/nonexistent-x\"}",
    )
    .await;
    assert_eq!(s3, 404);
    assert!(b3.contains("notFound"), "expected notFound, got {b3}");
}

/// Per-source-IP rate limit: the 61st request in a window is 429.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limited() {
    let root = root();
    let _local = init(&root);
    let addr = tcp_serve(&root, TOKEN.as_bytes()).await;
    let mut last = 0;
    for _ in 0..=vrt_local::mgmt::RateLimit::MAX_PER_MIN {
        // Unauthenticated requests count too — the limiter sits in
        // front of auth deliberately.
        let (s, _) = post(addr, None, "/healthz", b"{}").await;
        last = s;
    }
    assert_eq!(last, 429);
}

/// Tamper surface — every forged byte fails closed server-side.
#[test]
fn tampered_upload_rejected() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "edge");
    let good = upload_body(&dir);

    // Object digest key != sha256(bytes).
    let mut bad: vrt_local::worker_pkg::DeployUpload =
        serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
    let objects = &mut bad.assets.as_mut().unwrap().objects;
    let (sha, bytes) = objects
        .iter()
        .next()
        .map(|(k, v)| (k.clone(), v.clone()))
        .unwrap();
    objects.remove(&sha);
    objects.insert("f".repeat(64), bytes);
    assert!(
        local.deploy_upload(&bad).is_err(),
        "forged object digest accepted"
    );

    // Manifest names an object that was never shipped.
    let mut bad: vrt_local::worker_pkg::DeployUpload =
        serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
    bad.assets.as_mut().unwrap().objects.clear();
    assert!(local.deploy_upload(&bad).is_err());

    // Path traversal / excluded names in the modules map.
    for rel in [
        "../evil.js",
        "node_modules/x.js",
        ".secret.js",
        "vrt.toml",
        "a\\b.js",
    ] {
        let mut bad: vrt_local::worker_pkg::DeployUpload =
            serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
        bad.modules.insert(rel.to_string(), "ZXhwb3J0".to_string());
        assert!(local.deploy_upload(&bad).is_err(), "{rel} accepted");
    }

    // Declared-but-absent module (meta is forged through JSON — the
    // same channel the wire body deserializes from).
    let mut bad: vrt_local::worker_pkg::DeployUpload =
        serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
    let mut mv = serde_json::to_value(&bad.meta).unwrap();
    mv["modules"]["ghost.bin"] = serde_json::json!("data");
    bad.meta = serde_json::from_value(mv).unwrap();
    assert!(local.deploy_upload(&bad).is_err());

    // `[bindings].assets` naming a manifest nobody has.
    let mut bad: vrt_local::worker_pkg::DeployUpload =
        serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
    let mut mv = serde_json::to_value(&bad.meta).unwrap();
    mv["bindings"]["assets"]["FORGED"] = serde_json::json!("a".repeat(64));
    bad.meta = serde_json::from_value(mv).unwrap();
    assert!(local.deploy_upload(&bad).is_err());

    // Secret value must not appear in ANY error text. Force a deploy
    // error while shipping a distinctive secret value.
    let mut bad: vrt_local::worker_pkg::DeployUpload =
        serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
    bad.secrets
        .insert("ok-ref".to_string(), "V4LUE-N0T-1N-ERR0RS".to_string());
    // `.xyz` has no extension fallback and matches no module rule —
    // unlike `.bin`, which the implicit `Data: **/*.bin` rule claims.
    bad.modules
        .insert("broken.xyz".to_string(), "AA==".to_string());
    let err = local.deploy_upload(&bad).unwrap_err().to_string();
    assert!(!err.contains("V4LUE-N0T-1N-ERR0RS"), "secret leaked: {err}");

    // Module bytes over the content cap → dir size.
    let mut bad: vrt_local::worker_pkg::DeployUpload =
        serde_json::from_value(serde_json::to_value(&good).unwrap()).unwrap();
    use base64::Engine;
    bad.modules.insert(
        "big.bin".to_string(),
        base64::engine::general_purpose::STANDARD.encode(vec![0u8; 97 * 1024 * 1024]),
    );
    assert!(local.deploy_upload(&bad).is_err());
}

/// `--dry-run` summary — pack-side facts without any API call.
#[test]
fn dry_run_summary() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "edge");
    let g = vrt_local::worker_pkg::gather_dir(&dir).unwrap();
    let s =
        vrt_local::worker_pkg::summarize(&g, &vrt_local::worker_pkg::DeployOverrides::default())
            .unwrap();
    assert_eq!(s.name, "edge");
    assert_eq!(s.module_count, 2); // main.js + util.js
    assert!(s.bundle_bytes > 0);
    assert_eq!(s.bundle_digest.len(), 64);
    assert!(s.bindings.iter().any(|b| b == "kv:CACHE"));
    assert!(s.bindings.iter().any(|b| b == "var:LOG_LEVEL"));
    assert!(s.bindings.iter().any(|b| b == "assets:SITE"));
    assert_eq!(s.secret_bindings, vec!["API_KEY".to_string()]);
    assert!(s.ignored.iter().any(|p| p == ".dev.vars"));
    // And the same dir deploys for real through the upload path —
    // proof the summary is honest.
    let body = upload_body(&dir);
    local.deploy_upload(&body).unwrap();
    assert_eq!(local.status().unwrap().workers.len(), 1);
}
