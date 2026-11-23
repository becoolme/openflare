//! T17 standalone end-to-end smoke — the REAL path, in-process:
//!
//!   of init → `vrt_local::serve` (unix mgmt API) → deploy via API →
//!   FileDesiredStore (verify+fence) → FileRenderInputs (hot reload) →
//!   DirBundleSource → SupervisorHost + vrt-stub-workerd → ingress.
//!
//! Covers: init, serve, deploy, ingress-by-hostname, secret hot
//! reload (presence only), deploy v2, rollback restoring v1 bindings,
//! bad-token rejection, socket permissions. No docker.

mod common;

use common::*;
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use vrt_host::artifact::{BundleCache, DirBundleSource};
use vrt_host::assemble::{Assembler, EmitMode};
use vrt_host::desired::{DesiredSource, HostDesiredStore};
use vrt_host::ingress::IngressState;
use vrt_host::render_inputs::FileRenderInputs;
use vrt_host::route::RouteTable;
use vrt_host::status::new_status;
use vrt_host::supervisor::{SupervisorDeps, SupervisorHost, TenantRegistry};
use vrt_local::{serve::ServeOptions, InitParams, VrtLocal};
use vrt_provider::FileDesiredStore;

/// Drive the REAL management router in-process — the socket
/// transport is covered by the `VRT_UNIX_TESTS=1`-gated test below
/// (this sandbox forbids unix bind()); routing, auth, validation and
/// library ops are identical either way.
async fn api(
    router: &mut axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<serde_json::Value>,
) -> (u16, String) {
    use tower::ServiceExt;
    let mut b = axum::http::Request::builder()
        .method(method)
        .uri(format!("http://vrt{path}"));
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let req = match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => b.body(axum::body::Body::empty()).unwrap(),
    };
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

fn worker_dir(root: &Path, version: u8) -> PathBuf {
    let dir = root.join(format!("worker-v{version}"));
    std::fs::create_dir_all(&dir).unwrap();
    let toml = match version {
        // v1: binds secret API_KEY + var GREETING
        1 => {
            "name = \"edge-api\"\nmainModule = \"main.js\"\n\
              compatibilityDate = \"2024-09-25\"\n\
              hostnames = [\"api.standalone.test\"]\n\n\
              [vars]\nGREETING = \"hello-v1\"\n\n\
              [secrets]\nAPI_KEY = \"api-key\"\n"
        }
        // v2: different var value, NO secret binding
        _ => {
            "name = \"edge-api\"\nmainModule = \"main.js\"\n\
              compatibilityDate = \"2024-09-25\"\n\
              hostnames = [\"api.standalone.test\"]\n\n\
              [vars]\nGREETING = \"hello-v2\"\n"
        }
    };
    std::fs::write(dir.join("vrt.toml"), toml).unwrap();
    std::fs::write(
        dir.join("main.js"),
        "export default { fetch() { return new Response('ok') } }\n",
    )
    .unwrap();
    dir
}

struct Standalone {
    root: PathBuf,
    store: Arc<HostDesiredStore>,
    host: SupervisorHost,
    assembler: Arc<dyn vrt_host::supervisor::TenantAssembler>,
    registry: Arc<TenantRegistry>,
    status: vrt_host::status::HostStatus,
    ingress: Arc<IngressState>,
    _dir: PathBuf,
}

impl Standalone {
    /// One reload+converge tick — standalone mode: no assignment, the
    /// full verified desired converges (same call order as the bin).
    fn tick(&mut self) {
        let _ = self.store.inner().reload(chrono::Utc::now());
        let Ok(desired) = self.store.snapshot() else {
            return;
        };
        self.host.converge(&desired, &self.assembler);
        self.ingress.set_routes(RouteTable::from_desired(&desired));
        {
            let mut st = self.status.write().unwrap_or_else(|e| e.into_inner());
            st.revision = Some(desired.revision);
            st.payload_digest = Some(self.store.inner().payload_digest().unwrap_or_default());
            st.blocked_reason = None;
        }
        self.host.reap_finished();
    }

    async fn drive_until(&mut self, pred: impl Fn(&Self) -> bool) -> bool {
        for _ in 0..300 {
            self.tick();
            if pred(self) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    fn tenant_ready(&self) -> bool {
        self.registry
            .get("local")
            .is_some_and(|rt| rt.state() == vrt_host::state::TenantState::Ready)
    }
}

/// T37b: drop guard — smoke roots must not litter target/tmp. The
/// guard owns the dir; clones held by spawned tasks deref to the same
/// path and cleanup happens once, at test end.
struct TmpDir(std::sync::Arc<PathBuf>);

impl Clone for TmpDir {
    fn clone(&self) -> Self {
        TmpDir(std::sync::Arc::clone(&self.0))
    }
}

impl std::ops::Deref for TmpDir {
    type Target = PathBuf;
    fn deref(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        if std::sync::Arc::strong_count(&self.0) == 1 {
            if let Err(e) = std::fs::remove_dir_all(&**self.0) {
                eprintln!("tmp cleanup {}: {e}", self.0.display());
            }
        }
    }
}

fn tdir() -> TmpDir {
    // Sandbox: unix sockets may only bind inside the workspace —
    // CARGO_TARGET_TMPDIR is in-tree; tempfile's TMPDIR is not.
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "smoke-{}-{:?}",
        std::process::id(),
        std::time::Instant::now()
    ));
    std::fs::create_dir_all(&base).unwrap();
    TmpDir(std::sync::Arc::new(base))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_full_lifecycle_smoke() {
    // ---- 1. init ----
    let base = tdir();
    std::fs::set_permissions(&*base, std::fs::Permissions::from_mode(0o700)).unwrap();
    let dir_path = base.clone();
    let root = dir_path.join("vrt-root");
    // init wants a non-existent or empty root — point one level deeper.
    let report = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: None,
        },
    )
    .unwrap();

    // ---- 2. management API (router in-process; see gated test for
    // the real unix-socket transport) ----
    let (mut router, _api) = vrt_local::mgmt::router(root.clone()).unwrap();
    // Management dir + token file are owner-only (created by init).
    assert_eq!(
        std::fs::metadata(root.join("mgmt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(root.join("mgmt/token"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let token = String::from_utf8(std::fs::read(root.join("mgmt/token")).unwrap()).unwrap();

    // Bad token must be rejected before any op.
    let (st_bad, _) = api(&mut router, "GET", "/v1/status", Some("deadbeef"), None).await;
    assert_eq!(st_bad, 401);
    let (st_noauth, _) = api(&mut router, "GET", "/v1/status", None, None).await;
    assert_eq!(st_noauth, 401);

    // ---- host rig ----
    let host_dir = dir_path.join("host");
    let work = host_dir.join("work");
    let cache = host_dir.join("cache");
    for p in [&work, &cache] {
        mkdir700(p);
    }
    let store = Arc::new(HostDesiredStore::new(FileDesiredStore::new(
        root.join("desired/envelope.json"),
        "local",
        vec![report.trusted_key.clone()],
        1,
    )));
    let inputs = FileRenderInputs::new(
        Some(root.join("vars/vars.json")),
        Some(root.join("secrets/secrets.json")),
    );
    let assembler: Arc<dyn vrt_host::supervisor::TenantAssembler> = Arc::new(Assembler::new(
        stub_binary(),
        work.clone(),
        Arc::new(BundleCache::new(cache)),
        Arc::new(DirBundleSource::new(root.join("objects"))),
        EmitMode::StubJson,
        BTreeMap::new(),
        Default::default(),
        vec![],
        Arc::new(inputs),
    ));
    let registry = TenantRegistry::new();
    let status = new_status();
    let host = SupervisorHost::new(registry.clone(), SupervisorDeps::default(), test_params());
    let ingress = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    let mut sa = Standalone {
        root: root.clone(),
        store,
        host,
        assembler,
        registry,
        status,
        ingress,
        _dir: dir_path.to_path_buf(),
    };
    let ig = start_ingress(&sa.ingress.clone()).await;

    // ---- 3. seed the secret + deploy v1 via the management API ----
    let (st, _) = api(
        &mut router,
        "PUT",
        "/v1/secrets",
        Some(&token),
        Some(serde_json::json!({"secretRef": "api-key", "value": "s3cr3t"})),
    )
    .await;
    assert_eq!(st, 204, "secret put");
    let w1 = worker_dir(&sa.root, 1);
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": w1})),
    )
    .await;
    assert_eq!(st, 200, "deploy v1: {body}");
    let v1_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["versionId"]
        .as_str()
        .unwrap()
        .to_string();

    // ---- 4. converge until serving; ingress by hostname ----
    assert!(
        sa.drive_until(|s| s.tenant_ready()).await,
        "tenant never became Ready"
    );
    let (st, _, body) = http_get(ig.addr, "api.standalone.test", "/", &[]).await;
    assert_eq!(st, 200);
    assert!(body.contains("stub ok"));

    // Secret env presence — the stub reports presence only (never the
    // value).
    let env_name = {
        // VRT_S_<enc(workerId)>__<enc(binding)> — worker id assigned
        // by deploy; read it from status.
        let l = VrtLocal::open(&sa.root).unwrap();
        let st = l.status().unwrap();
        let w = &st.workers[0];
        vrt_config::secret_env_name(&w.worker_id, "API_KEY")
    };
    let (_, headers, _) = http_get(
        ig.addr,
        "api.standalone.test",
        "/",
        &[("x-stub-env-check", &env_name)],
    )
    .await;
    let got = headers
        .iter()
        .find(|(k, _)| k == &format!("x-stub-env-{}", env_name.to_lowercase()))
        .map(|(_, v)| v.clone());
    assert_eq!(got.as_deref(), Some("present"), "secret env present");

    // ---- 5. secret hot reload: delete the bound ref → fail closed,
    // old generation keeps serving; re-put → recovers. ----
    let (st, _) = api(
        &mut router,
        "DELETE",
        "/v1/secrets/api-key",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(st, 204);
    // The tenant fails assembly on the missing ref but the OLD
    // generation keeps serving — ingress stays 200.
    for _ in 0..40 {
        sa.tick();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (st, _, body) = http_get(ig.addr, "api.standalone.test", "/", &[]).await;
    assert_eq!(st, 200, "old generation keeps serving on input error");
    assert!(body.contains("stub ok"));
    // Re-put restores the generation.
    let (st, _) = api(
        &mut router,
        "PUT",
        "/v1/secrets",
        Some(&token),
        Some(serde_json::json!({"secretRef": "api-key", "value": "rotated"})),
    )
    .await;
    assert_eq!(st, 204);
    assert!(
        sa.drive_until(|s| s.tenant_ready()).await,
        "tenant must recover after secret re-put"
    );

    // ---- 6. deploy v2 (no secret binding) — version-scoped bindings:
    // the secret env disappears under v2. ----
    let w2 = worker_dir(&sa.root, 2);
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": w2})),
    )
    .await;
    assert_eq!(st, 200, "deploy v2: {body}");
    // Wait for the v2 generation to actually serve: poll the env check
    // until absent (switch completes asynchronously).
    let mut gone = false;
    for _ in 0..300 {
        sa.tick();
        let (_, headers, _) = http_get(
            ig.addr,
            "api.standalone.test",
            "/",
            &[("x-stub-env-check", &env_name)],
        )
        .await;
        if headers
            .iter()
            .find(|(k, _)| k == &format!("x-stub-env-{}", env_name.to_lowercase()))
            .map(|(_, v)| v == "absent")
            .unwrap_or(false)
        {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(gone, "v2 generation serving without the secret binding");

    // ---- 7. rollback v1 → version-scoped bindings AND vars restore ----
    let (st, _) = api(
        &mut router,
        "POST",
        "/v1/rollback",
        Some(&token),
        Some(serde_json::json!({"worker": "edge-api", "versionId": v1_id})),
    )
    .await;
    assert_eq!(st, 200, "rollback");
    // vars.json (union of stored versions, scoped by tenant) must
    // contain v1's ref with v1's value — keys are
    // `var:{name}:{vhash8}:{binding}` under the tenant object.
    let vars: serde_json::Value =
        serde_json::from_slice(&std::fs::read(sa.root.join("vars/vars.json")).unwrap()).unwrap();
    let v1_var = vars["local"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(k, v)| {
            k.starts_with("var:edge-api:") && k.ends_with(":GREETING") && **v == "hello-v1"
        })
        .map(|(k, _)| k.clone());
    assert!(
        v1_var.is_some(),
        "rollback restores v1 var ref+value: {vars}"
    );
    // And the secret binding env returns — runtime proof of
    // version-scoped state.
    let mut back = false;
    for _ in 0..300 {
        sa.tick();
        let (_, headers, _) = http_get(
            ig.addr,
            "api.standalone.test",
            "/",
            &[("x-stub-env-check", &env_name)],
        )
        .await;
        if headers
            .iter()
            .find(|(k, _)| k == &format!("x-stub-env-{}", env_name.to_lowercase()))
            .map(|(_, v)| v == "present")
            .unwrap_or(false)
        {
            back = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(back, "v1 generation serving again after rollback");

    // ---- cleanup: stop serving children ----
    sa.host.shutdown_all(Duration::from_secs(5)).await;
    ig.stop().await;
}

/// Raw unix-socket HTTP/1.1 client for the gated transport test.
async fn unix_req(
    sock: &Path,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<serde_json::Value>,
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req = format!("{method} {path} HTTP/1.1\r\nhost: vrt\r\nconnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("authorization: Bearer {t}\r\n"));
    }
    let body = body.map(|b| serde_json::to_string(&b).unwrap_or_default());
    if let Some(b) = &body {
        req.push_str(&format!(
            "content-type: application/json\r\ncontent-length: {}\r\n",
            b.len()
        ));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(&b);
    }
    let mut stream = vrt_local::serve::connect_unix(sock)
        .await
        .expect("socket connect");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, body.to_string())
}

/// Real unix-socket transport: `serve::run` binds `mgmt/vrt.sock`
/// (0600), enforces the bearer token over the wire, and serves the
/// same router. Ignored unless `VRT_UNIX_TESTS=1` — dev sandboxes
/// commonly deny unix `bind()` (same gate as `unix_listener.rs`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_UNIX_TESTS=1 and unix bind() allowed"]
async fn standalone_serve_unix_socket_transport() {
    // T34f: real unix bind under this root — CARGO_TARGET_TMPDIR is
    // ~150B inside the worktree, over the sun_path budget once
    // mgmt/vrt.sock is appended; the shared helper keeps it short.
    let base = match vrt_securefs::ShortRoot::new("vrtssu") {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            return;
        }
        Err(e) => panic!("short root: {e}"),
    };
    std::fs::set_permissions(&*base, std::fs::Permissions::from_mode(0o700)).unwrap();
    let root = base.join("vrt-root");
    let report = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: None,
        },
    )
    .unwrap();
    let sock = vrt_local::mgmt::socket_path(&root);
    let serve_root = root.clone();
    let serve =
        tokio::spawn(
            async move { vrt_local::serve::run(&serve_root, &ServeOptions::default()).await },
        );
    // F1: readiness = "socket CONNECTABLE" — not merely present. The
    // file now appears only after chmod+rename (0600 from birth), so a
    // file-exists probe can't beat the mode assertion anymore, but a
    // real connect also proves the listener is up.
    let mut ready = false;
    for _ in 0..300 {
        if tokio::net::UnixStream::connect(&sock).await.is_ok() {
            ready = true;
            break;
        }
        if serve.is_finished() {
            let out = serve.await.unwrap();
            panic!("serve exited early: {out:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ready, "serve bound the management socket");
    assert!(sock.exists());
    // Socket file itself must be owner-only.
    assert_eq!(
        std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let token = String::from_utf8(std::fs::read(root.join("mgmt/token")).unwrap()).unwrap();
    let (st, _) = unix_req(&sock, "GET", "/v1/status", Some("deadbeef"), None).await;
    assert_eq!(st, 401);
    let (st, body) = unix_req(&sock, "GET", "/v1/status", Some(&token), None).await;
    assert_eq!(st, 200);
    assert!(body.contains("cellId"));
    // Healthz is alive (envelope absent pre-deploy → unhealthy is OK).
    let (st, _) = unix_req(&sock, "GET", "/healthz", Some(&token), None).await;
    assert!(st == 200 || st == 503);
    let _ = report;
    serve.abort();
    let _ = serve.await;
}
