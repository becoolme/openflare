//! Versioning MVP at the CONTROL-PLANE HTTP boundary.
//!
//! Everything here goes through a real management listener (loopback
//! TCP, bearer token, axum router) — no direct `VrtLocal` calls for the
//! operations under test. Where a case claims "traffic did/didn't move"
//! the assertion also goes through the host data path:
//!
//!   deploy/preview/promote ──▶ signed desired ──▶ SupervisorHost
//!                                                + ingress GET
//!
//! Version identity is observed three ways so a 200 can never stand in
//! for a state change: `/v1/versions` (lifecycle), the signed desired
//! document (what the host converges), and the ingress response —
//! `x-stub-worker` names the instance that served the request (the stub
//! runs one process per tenant, so `x-stub-env-*` markers are unioned
//! across that tenant's instances and cannot distinguish them), while
//! the bundle bytes for the active version are decoded from `objects/`
//! through the same `DirBundleSource` the host assembles from, which is
//! where the actual code that would run is asserted (the stub does not
//! execute JS — a real-workerd assertion lives in the `VRT_WORKERD_BIN`
//! gated suite).

mod common;

use common::{http_get, mkdir700, stub_binary, test_params};
use std::net::SocketAddr;
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
use vrt_host::supervisor::{SupervisorDeps, SupervisorHost, TenantAssembler, TenantRegistry};
use vrt_local::{InitParams, VrtLocal};

// ---------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------

/// Test root. `CARGO_TARGET_TMPDIR` is in-tree (the sandbox only allows
/// secure dirs + loopback/tcp under the workspace).
struct Env {
    base: PathBuf,
    root: PathBuf,
    trusted_key: snapshot_protocol::TrustedKey,
}

impl Env {
    fn new(tag: &str) -> Self {
        let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "verhttp-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&base).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700)).unwrap();
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
        Self {
            base,
            root,
            trusted_key: report.trusted_key,
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// A worker source dir. `vars` are version-scoped markers the host
/// injects as `VRT_VAR_<name>` — the stub reports their PRESENCE over
/// HTTP, which is how a test observes which version is serving.
fn worker_dir(base: &Path, tag: &str, name: &str, code: &str, vars: &[(&str, &str)]) -> PathBuf {
    let dir = base.join(format!("src-{tag}"));
    std::fs::create_dir_all(&dir).unwrap();
    // Per-worker hostname: standalone desired validation rejects a
    // hostname claimed by two workers.
    let mut toml = format!(
        "name = \"{name}\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\
         hostnames = [\"{name}.versioning.test\"]\n"
    );
    if !vars.is_empty() {
        toml.push_str("\n[vars]\n");
        for (k, v) in vars {
            toml.push_str(&format!("{k} = \"{v}\"\n"));
        }
    }
    std::fs::write(dir.join("vrt.toml"), toml).unwrap();
    std::fs::write(dir.join("main.js"), code).unwrap();
    dir
}

// ---------------------------------------------------------------------
// management listener (real HTTP, real bearer auth)
// ---------------------------------------------------------------------

/// Bind the TCP management router exactly like `vrt serve
/// --mgmt-tcp-listen` does (separate token, per-IP rate limit) and
/// return the address, token, and serve task.
async fn spawn_mgmt(root: &Path) -> (SocketAddr, String, tokio::task::JoinHandle<()>) {
    let token = String::from_utf8(std::fs::read(root.join("mgmt/token")).unwrap()).unwrap();
    let (_unix_router, api) = vrt_local::mgmt::router(root.to_path_buf()).unwrap();
    let tcp = vrt_local::mgmt::tcp_router(
        api,
        token.clone().into_bytes(),
        vrt_local::mgmt::RateLimit::new(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            tcp.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    (addr, token, task)
}

/// One HTTP request → (status, body text).
async fn call(
    addr: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&serde_json::Value>,
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let payload = body
        .map(|b| serde_json::to_vec(b).unwrap())
        .unwrap_or_default();
    let mut req = format!("{method} {path} HTTP/1.1\r\nhost: vrt\r\nconnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("authorization: Bearer {t}\r\n"));
    }
    if body.is_some() {
        req.push_str(&format!(
            "content-type: application/json\r\ncontent-length: {}\r\n",
            payload.len()
        ));
    }
    req.push_str("\r\n");
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    if !payload.is_empty() {
        s.write_all(&payload).await.unwrap();
    }
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, out) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    (status, out.to_string())
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("non-JSON body ({e}): {text}"))
}

/// POST /v1/deploy: `preview` selects the traffic-neutral path.
async fn deploy(
    addr: SocketAddr,
    token: &str,
    dir: &Path,
    preview: bool,
) -> (u16, serde_json::Value) {
    let body = serde_json::json!({"workerDir": dir, "preview": preview});
    let (st, text) = call(addr, "POST", "/v1/deploy", Some(token), Some(&body)).await;
    (st, json(&text))
}

async fn promote(
    addr: SocketAddr,
    token: &str,
    version_id: &str,
    worker: Option<&str>,
) -> (u16, serde_json::Value) {
    let mut body = serde_json::json!({"versionId": version_id});
    if let Some(w) = worker {
        body["worker"] = serde_json::Value::String(w.to_string());
    }
    let (st, text) = call(addr, "POST", "/v1/promote", Some(token), Some(&body)).await;
    (st, json(&text))
}

async fn versions(addr: SocketAddr, token: &str, worker: &str) -> (u16, serde_json::Value) {
    let (st, text) = call(
        addr,
        "GET",
        &format!("/v1/versions?worker={worker}"),
        Some(token),
        None,
    )
    .await;
    (st, json(&text))
}

fn detail<'a>(list: &'a serde_json::Value, version_id: &str) -> &'a serde_json::Value {
    list["versionDetails"]
        .as_array()
        .expect("versionDetails")
        .iter()
        .find(|v| v["versionId"] == version_id)
        .unwrap_or_else(|| panic!("version {version_id} absent from {list}"))
}

fn active_details(list: &serde_json::Value) -> Vec<&serde_json::Value> {
    list["versionDetails"]
        .as_array()
        .expect("versionDetails")
        .iter()
        .filter(|v| v["status"] == "active")
        .collect()
}

/// `activeVersion.versionId` from the signed desired document — what
/// the host actually converges, independent of the state file.
fn desired_active(root: &Path, worker: &str) -> Option<String> {
    let raw = std::fs::read(root.join("desired/envelope.json")).ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    doc["payload"]["tenants"][0]["workers"]
        .as_array()?
        .iter()
        .find(|w| w["name"] == worker)
        .and_then(|w| w["activeVersion"]["versionId"].as_str())
        .map(str::to_string)
}

/// The module bytes the host would assemble for `digest` — read
/// through the same `DirBundleSource` + `VersionBundle` path the
/// assembler uses, so this asserts real serving content, not metadata.
fn bundle_module(root: &Path, digest: &str, module: &str) -> String {
    use vrt_host::artifact::BundleSource;
    let src = DirBundleSource::new(root.join("objects"));
    let bytes = src.fetch(digest).expect("bundle object present");
    let bundle = vrt_config::VersionBundle::parse_stored(&bytes).expect("valid stored bundle");
    let m = bundle
        .manifest()
        .modules
        .iter()
        .find(|m| m.name() == module)
        .unwrap_or_else(|| panic!("module {module} missing from bundle {digest}"));
    String::from_utf8(m.content_bytes().expect("module bytes")).unwrap()
}

// ---------------------------------------------------------------------
// host data path
// ---------------------------------------------------------------------

/// Standalone host rig (same shape as `standalone_smoke`): store →
/// supervisor → ingress. One `tick()` is one converge.
struct Stack {
    store: Arc<HostDesiredStore>,
    host: SupervisorHost,
    assembler: Arc<dyn TenantAssembler>,
    registry: Arc<TenantRegistry>,
    status: vrt_host::status::HostStatus,
    ingress: Arc<IngressState>,
    guard: common::IngressGuard,
}

impl Stack {
    async fn start(env: &Env) -> Self {
        let host_dir = env.base.join("host");
        let work = host_dir.join("work");
        let cache = host_dir.join("cache");
        for p in [&work, &cache] {
            mkdir700(p);
        }
        let store = Arc::new(HostDesiredStore::new(vrt_provider::FileDesiredStore::new(
            env.root.join("desired/envelope.json"),
            "local",
            vec![env.trusted_key.clone()],
            1,
        )));
        let inputs = FileRenderInputs::new(
            Some(env.root.join("vars/vars.json")),
            Some(env.root.join("secrets/secrets.json")),
        );
        let assembler: Arc<dyn TenantAssembler> = Arc::new(Assembler::new(
            stub_binary(),
            work.clone(),
            Arc::new(BundleCache::new(cache)),
            Arc::new(DirBundleSource::new(env.root.join("objects"))),
            EmitMode::StubJson,
            Default::default(),
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
        let guard = common::start_ingress(&ingress).await;
        Self {
            store,
            host,
            assembler,
            registry,
            status,
            ingress,
            guard,
        }
    }

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

    /// Drive converges until `env` markers answer as specified.
    async fn wait_env(&mut self, name: &str, want: &str) -> bool {
        let mut ok = false;
        for _ in 0..300 {
            self.tick();
            let (st, headers, _) = self.get(&[("x-stub-env-check", name)]).await;
            if st == 200
                && headers
                    .iter()
                    .find(|(k, _)| *k == format!("x-stub-env-{}", name.to_lowercase()))
                    .map(|(_, v)| v == want)
                    .unwrap_or(false)
            {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        ok
    }

    async fn get(&self, extra: &[(&str, &str)]) -> (u16, Vec<(String, String)>, String) {
        http_get(self.guard.addr, "edge-api.versioning.test", "/", extra).await
    }

    async fn stop(mut self) {
        self.host.shutdown_all(Duration::from_secs(5)).await;
        self.guard.stop().await;
    }
}

/// Instance socket that served an ingress response (`x-stub-worker:
/// sock-<instanceId>`).
fn served_by(headers: &[(String, String)]) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k == "x-stub-worker")
        .map(|(_, v)| v.clone())
}

/// Env-marker value from an ingress response (`present` / `absent`).
fn marker(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| *k == format!("x-stub-env-{}", name.to_lowercase()))
        .map(|(_, v)| v.clone())
}

// ---------------------------------------------------------------------
// 1. preview never moves traffic
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preview_keeps_traffic_on_the_active_version() {
    let env = Env::new("preview");
    let (addr, token, mgmt) = spawn_mgmt(&env.root).await;
    let mut stack = Stack::start(&env).await;

    let v1 = worker_dir(
        &env.base,
        "v1",
        "edge-api",
        "export default {v: 1}\n",
        &[("ONLY_V1", "1")],
    );
    let (st, first) = deploy(addr, &token, &v1, false).await;
    assert_eq!(st, 200, "deploy v1: {first}");
    let v1_id = first["versionId"].as_str().unwrap().to_string();

    assert!(
        stack.drive_until(|s| s.tenant_ready()).await,
        "v1 tenant never became Ready"
    );
    let (st, headers, body) = stack.get(&[("x-stub-env-check", "VRT_VAR_ONLY_V1")]).await;
    assert_eq!(st, 200, "v1 serving: {body}");
    assert!(body.contains("stub ok"), "{body}");
    assert_eq!(
        marker(&headers, "VRT_VAR_ONLY_V1").as_deref(),
        Some("present"),
        "v1's version-scoped var must be live"
    );
    let v1_instance = served_by(&headers);
    assert!(v1_instance.is_some(), "the stub must name its instance");
    assert_eq!(
        bundle_module(
            &env.root,
            detail(&versions(addr, &token, "edge-api").await.1, &v1_id)["artifactDigest"]
                .as_str()
                .unwrap(),
            "main.js"
        ),
        "export default {v: 1}\n",
        "the active bundle carries v1's code"
    );

    // Preview v2 — a new version, different code and var set.
    let v2 = worker_dir(
        &env.base,
        "v2",
        "edge-api",
        "export default {v: 2}\n",
        &[("ONLY_V2", "1")],
    );
    let (st, preview) = deploy(addr, &token, &v2, true).await;
    assert_eq!(st, 200, "preview v2: {preview}");
    let v2_id = preview["versionId"].as_str().unwrap().to_string();
    assert_ne!(v2_id, v1_id, "preview must mint a distinct version");

    let (st, list) = versions(addr, &token, "edge-api").await;
    assert_eq!(st, 200);
    assert_eq!(list["activeVersionId"], v1_id);
    assert_eq!(detail(&list, &v1_id)["status"], "active");
    assert_eq!(detail(&list, &v1_id)["active"], true);
    assert_eq!(detail(&list, &v2_id)["status"], "preview");
    assert_eq!(detail(&list, &v2_id)["active"], false);
    assert_eq!(active_details(&list).len(), 1);
    assert_eq!(
        desired_active(&env.root, "edge-api").as_deref(),
        Some(v1_id.as_str())
    );

    // Converge a few generations: traffic, code and desired must all
    // still be v1's.
    for _ in 0..20 {
        stack.tick();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (st, headers, body) = stack
        .get(&[("x-stub-env-check", "VRT_VAR_ONLY_V1,VRT_VAR_ONLY_V2")])
        .await;
    assert_eq!(st, 200, "preview must keep v1 serving: {body}");
    assert_eq!(
        marker(&headers, "VRT_VAR_ONLY_V1").as_deref(),
        Some("present")
    );
    // The stub's env is per PROCESS and every instance of a tenant shares
    // one process, so `VRT_VAR_ONLY_V2` being visible here does not mean
    // v2 serves. Production traffic must land on the very same instance
    // socket as before the preview; preview-instance isolation is
    // asserted by that identity here and by `preview_url_e2e`'s
    // route/counter checks.
    assert_eq!(
        served_by(&headers),
        v1_instance,
        "preview must not move production traffic off v1's instance"
    );
    assert_eq!(
        desired_active(&env.root, "edge-api").as_deref(),
        Some(v1_id.as_str())
    );
    assert_eq!(
        bundle_module(
            &env.root,
            detail(&versions(addr, &token, "edge-api").await.1, &v1_id)["artifactDigest"]
                .as_str()
                .unwrap(),
            "main.js"
        ),
        "export default {v: 1}\n"
    );

    stack.stop().await;
    mgmt.abort();
}

// ---------------------------------------------------------------------
// 2. promote moves traffic, replay is idempotent
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_switches_traffic_and_replays_idempotently() {
    let env = Env::new("promote");
    let (addr, token, mgmt) = spawn_mgmt(&env.root).await;
    let mut stack = Stack::start(&env).await;

    let v1 = worker_dir(
        &env.base,
        "v1",
        "edge-api",
        "export default {v: 1}\n",
        &[("ONLY_V1", "1")],
    );
    let (st, first) = deploy(addr, &token, &v1, false).await;
    assert_eq!(st, 200, "deploy v1: {first}");
    let v1_id = first["versionId"].as_str().unwrap().to_string();
    assert!(stack.drive_until(|s| s.tenant_ready()).await);

    let v2 = worker_dir(
        &env.base,
        "v2",
        "edge-api",
        "export default {v: 2}\n",
        &[("ONLY_V2", "1")],
    );
    let (st, preview) = deploy(addr, &token, &v2, true).await;
    assert_eq!(st, 200, "preview v2: {preview}");
    let v2_id = preview["versionId"].as_str().unwrap().to_string();
    // Preview: still v1 at the edge.
    assert!(
        stack.wait_env("VRT_VAR_ONLY_V1", "present").await,
        "preview must not switch traffic"
    );

    // Promote v2 over the management API.
    let (st, promoted) = promote(addr, &token, &v2_id, Some("edge-api")).await;
    assert_eq!(st, 200, "promote v2: {promoted}");
    assert_eq!(promoted["worker"], "edge-api");
    assert_eq!(promoted["versionId"], v2_id);
    assert_eq!(promoted["changed"], true);
    let revision = promoted["revision"].as_u64().expect("revision");

    // Traffic follows the pointer at the real edge.
    assert!(
        stack.wait_env("VRT_VAR_ONLY_V1", "absent").await,
        "v1's var must disappear once v2 serves"
    );
    assert_eq!(
        marker(
            &stack
                .get(&[("x-stub-env-check", "VRT_VAR_ONLY_V2")])
                .await
                .1,
            "VRT_VAR_ONLY_V2"
        )
        .as_deref(),
        Some("present"),
        "v2's var must be live after promote"
    );
    let (st, list) = versions(addr, &token, "edge-api").await;
    assert_eq!(st, 200);
    assert_eq!(list["activeVersionId"], v2_id);
    assert_eq!(active_details(&list).len(), 1, "exactly one active version");
    assert_eq!(detail(&list, &v2_id)["status"], "active");
    assert_eq!(detail(&list, &v2_id)["active"], true);
    assert_eq!(detail(&list, &v1_id)["status"], "archived");
    assert_eq!(
        desired_active(&env.root, "edge-api").as_deref(),
        Some(v2_id.as_str())
    );
    assert_eq!(
        bundle_module(
            &env.root,
            detail(&list, &v2_id)["artifactDigest"].as_str().unwrap(),
            "main.js"
        ),
        "export default {v: 2}\n",
        "the serving bundle is v2's code"
    );

    // Replay: success, but no state change and no new revision.
    let (st, again) = promote(addr, &token, &v2_id, Some("edge-api")).await;
    assert_eq!(st, 200, "idempotent replay: {again}");
    assert_eq!(again["changed"], false);
    assert_eq!(again["revision"].as_u64(), Some(revision));
    let (_, list2) = versions(addr, &token, "edge-api").await;
    assert_eq!(list2["activeVersionId"], v2_id);
    assert_eq!(active_details(&list2).len(), 1);
    assert_eq!(
        marker(
            &stack
                .get(&[("x-stub-env-check", "VRT_VAR_ONLY_V2")])
                .await
                .1,
            "VRT_VAR_ONLY_V2"
        )
        .as_deref(),
        Some("present"),
        "replay must not disturb the serving generation"
    );

    stack.stop().await;
    mgmt.abort();
}

// ---------------------------------------------------------------------
// 3. preview + secret values is refused before anything is written
// ---------------------------------------------------------------------

/// A remote-upload wire body (`of deploy --url`) for `dir`.
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
        assets: None,
        vars: Default::default(),
        secrets: Default::default(),
        name: None,
        tag: None,
        preview: false,
        flagship_apps: g.flagship_apps.clone(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_with_secret_values_is_refused_and_writes_nothing() {
    let env = Env::new("previewsecret");
    let (addr, token, mgmt) = spawn_mgmt(&env.root).await;

    let v1 = worker_dir(&env.base, "v1", "edge-api", "export default {v: 1}\n", &[]);
    let (st, first) = deploy(addr, &token, &v1, false).await;
    assert_eq!(st, 200, "deploy v1: {first}");
    let v1_id = first["versionId"].as_str().unwrap().to_string();

    let secrets_path = env.root.join("secrets/secrets.json");
    let before_secrets = std::fs::read(&secrets_path).ok();

    let mut body = upload_body(&worker_dir(
        &env.base,
        "v2",
        "edge-api",
        "export default {v: 2}\n",
        &[],
    ));
    body.preview = true;
    body.secrets = [("api-key".to_string(), "canary-do-not-echo".to_string())].into();

    let (st, text) = call(
        addr,
        "POST",
        "/v1/deploy/upload",
        Some(&token),
        Some(&serde_json::to_value(&body).unwrap()),
    )
    .await;
    assert_eq!(st, 400, "preview + secrets must be refused: {text}");
    let err = json(&text);
    assert_eq!(err["error"]["code"], "worker");
    assert_eq!(
        err["error"]["field"], "previewSecrets",
        "typed refusal field: {err}"
    );
    assert!(
        !text.contains("canary-do-not-echo"),
        "secret value leaked into the error body: {text}"
    );

    // Nothing written: same version count, same active pointer, same
    // secret store bytes.
    let (st, list) = versions(addr, &token, "edge-api").await;
    assert_eq!(st, 200);
    assert_eq!(list["versions"].as_array().unwrap().len(), 1);
    assert_eq!(list["activeVersionId"], v1_id);
    assert_eq!(std::fs::read(&secrets_path).ok(), before_secrets);

    // Same upload WITHOUT secrets is a legitimate preview: stored, not
    // served, and still no secret-side effect.
    body.secrets.clear();
    let (st, text) = call(
        addr,
        "POST",
        "/v1/deploy/upload",
        Some(&token),
        Some(&serde_json::to_value(&body).unwrap()),
    )
    .await;
    assert_eq!(st, 200, "secret-free preview: {text}");
    let previewed = json(&text);
    let v2_id = previewed["versionId"].as_str().unwrap().to_string();
    assert_ne!(v2_id, v1_id);
    let (_, list) = versions(addr, &token, "edge-api").await;
    assert_eq!(list["versions"].as_array().unwrap().len(), 2);
    assert_eq!(list["activeVersionId"], v1_id, "preview never activates");
    assert_eq!(detail(&list, &v2_id)["status"], "preview");
    assert_eq!(
        desired_active(&env.root, "edge-api").as_deref(),
        Some(v1_id.as_str())
    );
    assert_eq!(std::fs::read(&secrets_path).ok(), before_secrets);

    mgmt.abort();
}

// ---------------------------------------------------------------------
// 4. unknown / ambiguous version resolution
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_and_cross_worker_promotes_fail_with_typed_errors() {
    let env = Env::new("resolve");
    let (addr, token, mgmt) = spawn_mgmt(&env.root).await;

    // Byte-identical apps: version ids are content-derived, so the same
    // bundle owned by two workers is exactly the ambiguous case.
    let a = worker_dir(
        &env.base,
        "a",
        "worker-a",
        "export default {same: 1}\n",
        &[],
    );
    let b = worker_dir(
        &env.base,
        "b",
        "worker-b",
        "export default {same: 1}\n",
        &[],
    );
    let (st, ra) = deploy(addr, &token, &a, false).await;
    assert_eq!(st, 200, "deploy a: {ra}");
    let (st, rb) = deploy(addr, &token, &b, false).await;
    assert_eq!(st, 200, "deploy b: {rb}");
    let a_id = ra["versionId"].as_str().unwrap().to_string();
    let b_id = rb["versionId"].as_str().unwrap().to_string();
    assert_eq!(
        a_id, b_id,
        "fixture precondition: identical content must share a version id"
    );

    // Unknown id → 404, not 500.
    let (st, text) = call(
        addr,
        "POST",
        "/v1/promote",
        Some(&token),
        Some(&serde_json::json!({"versionId": "vrt_0000000000000000000000000000"})),
    )
    .await;
    assert_eq!(st, 404, "unknown version: {text}");
    let err = json(&text);
    assert_eq!(err["error"]["code"], "notFound");
    assert_eq!(err["error"]["field"], "version");

    // Ambiguous owner and no hint → explicit client error (NOT 500).
    let (st, text) = call(
        addr,
        "POST",
        "/v1/promote",
        Some(&token),
        Some(&serde_json::json!({"versionId": a_id})),
    )
    .await;
    assert_eq!(st, 400, "ambiguous owner: {text}");
    let err = json(&text);
    assert_eq!(err["error"]["field"], "ambiguousVersion", "{err}");

    // With the hint it resolves and commits. `changed` is false here
    // because worker-a already serves the shared version (deploy
    // activated it); the hint is what makes the call well-defined.
    let (st, ok) = promote(addr, &token, &a_id, Some("worker-a")).await;
    assert_eq!(st, 200, "hinted promote: {ok}");
    assert_eq!(ok["worker"], "worker-a");
    assert_eq!(ok["versionId"], a_id);
    assert_eq!(
        versions(addr, &token, "worker-a").await.1["activeVersionId"],
        a_id
    );

    // A version owned by ANOTHER worker under an explicit hint → 404.
    let b2 = worker_dir(&env.base, "b2", "worker-b", "export default {b: 2}\n", &[]);
    let (st, rb2) = deploy(addr, &token, &b2, false).await;
    assert_eq!(st, 200, "deploy b2: {rb2}");
    let b2_id = rb2["versionId"].as_str().unwrap().to_string();
    let (st, text) = call(
        addr,
        "POST",
        "/v1/promote",
        Some(&token),
        Some(&serde_json::json!({"versionId": b2_id, "worker": "worker-a"})),
    )
    .await;
    assert_eq!(st, 404, "cross-worker promote: {text}");
    assert_eq!(json(&text)["error"]["field"], "version");

    // Unknown worker hint → 404 on the worker field.
    let (st, text) = call(
        addr,
        "POST",
        "/v1/promote",
        Some(&token),
        Some(&serde_json::json!({"versionId": a_id, "worker": "nobody"})),
    )
    .await;
    assert_eq!(st, 404, "unknown worker: {text}");
    assert_eq!(json(&text)["error"]["field"], "worker");

    mgmt.abort();
}

// ---------------------------------------------------------------------
// 5. concurrent promotes: one active version, honest responses
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_promotes_leave_exactly_one_active_version() {
    let env = Env::new("concurrent");
    let (addr, token, mgmt) = spawn_mgmt(&env.root).await;
    let mut stack = Stack::start(&env).await;

    let v1 = worker_dir(&env.base, "v1", "edge-api", "export default {v: 1}\n", &[]);
    let (st, first) = deploy(addr, &token, &v1, false).await;
    assert_eq!(st, 200, "deploy v1: {first}");
    assert!(stack.drive_until(|s| s.tenant_ready()).await);

    let v2 = worker_dir(
        &env.base,
        "v2",
        "edge-api",
        "export default {v: 2}\n",
        &[("ONLY_V2", "1")],
    );
    let (st, p2) = deploy(addr, &token, &v2, true).await;
    assert_eq!(st, 200, "preview v2: {p2}");
    let v2_id = p2["versionId"].as_str().unwrap().to_string();

    let v3 = worker_dir(
        &env.base,
        "v3",
        "edge-api",
        "export default {v: 3}\n",
        &[("ONLY_V3", "1")],
    );
    let (st, p3) = deploy(addr, &token, &v3, true).await;
    assert_eq!(st, 200, "preview v3: {p3}");
    let v3_id = p3["versionId"].as_str().unwrap().to_string();

    let (r2, r3) = tokio::join!(
        promote(addr, &token, &v2_id, Some("edge-api")),
        promote(addr, &token, &v3_id, Some("edge-api")),
    );
    for (label, (status, body)) in [("v2", &r2), ("v3", &r3)] {
        assert_ne!(*status, 500, "{label} promote lied with 500: {body}");
        assert!(
            *status == 200 || *status == 409,
            "{label} promote gave an unexpected status {status}: {body}"
        );
        if *status == 409 {
            assert_eq!(body["error"]["code"], "locked", "typed contention: {body}");
        }
    }
    let succeeded: Vec<&str> = [("v2", &r2), ("v3", &r3)]
        .iter()
        .filter(|(_, (st, _))| *st == 200)
        .map(|(label, _)| *label)
        .collect();
    assert!(
        !succeeded.is_empty(),
        "at least one concurrent promote must commit: v2={r2:?} v3={r3:?}"
    );
    let changed = [("v2", &r2), ("v3", &r3)]
        .iter()
        .filter(|(_, (st, b))| *st == 200 && b["changed"] == true)
        .map(|(label, _)| *label)
        .collect::<Vec<_>>();
    assert!(
        !changed.is_empty(),
        "a committed promote must report changed=true: v2={r2:?} v3={r3:?}"
    );

    // Truthfulness: the successful response with the highest revision
    // committed last, so it MUST name the final active version. A 200
    // that advertised a newer revision for the other version would be
    // a false report.
    let mut latest: Option<(u64, &str)> = None;
    for (label, (status, body)) in [("v2", &r2), ("v3", &r3)] {
        if *status != 200 {
            continue;
        }
        let rev = body["revision"].as_u64().expect("revision");
        if latest.map(|(r, _)| rev > r).unwrap_or(true) {
            latest = Some((rev, label));
        }
    }
    let (latest_rev, latest_label) = latest.expect("a successful promote");
    let latest_id = if latest_label == "v2" { &v2_id } else { &v3_id };
    let (_, list) = versions(addr, &token, "edge-api").await;
    let active = list["activeVersionId"].as_str().unwrap().to_string();
    assert_eq!(
        &active, latest_id,
        "highest-revision success (rev {latest_rev}) must be the final active version: {list}"
    );
    assert!(
        [v2_id.as_str(), v3_id.as_str()].contains(&active.as_str()),
        "active pointer {active} is not one of the promoted versions: {list}"
    );
    assert_eq!(active_details(&list).len(), 1, "one active version: {list}");
    assert_eq!(detail(&list, &active)["status"], "active");
    assert_eq!(
        desired_active(&env.root, "edge-api").as_deref(),
        Some(active.as_str())
    );

    // The edge must serve the version the final pointer names.
    let winner_marker = if active == v2_id {
        "VRT_VAR_ONLY_V2"
    } else {
        "VRT_VAR_ONLY_V3"
    };
    let loser_marker = if active == v2_id {
        "VRT_VAR_ONLY_V3"
    } else {
        "VRT_VAR_ONLY_V2"
    };
    assert!(
        stack.wait_env(winner_marker, "present").await,
        "winner {active} is not serving"
    );
    let (st, headers, body) = stack
        .get(&[(
            "x-stub-env-check",
            &format!("{winner_marker},{loser_marker}"),
        )])
        .await;
    assert_eq!(st, 200, "{body}");
    assert_eq!(marker(&headers, winner_marker).as_deref(), Some("present"));
    assert_eq!(
        marker(&headers, loser_marker).as_deref(),
        Some("absent"),
        "the losing promote must not serve"
    );
    assert_eq!(
        bundle_module(
            &env.root,
            detail(&list, &active)["artifactDigest"].as_str().unwrap(),
            "main.js"
        ),
        if active == v2_id {
            "export default {v: 2}\n"
        } else {
            "export default {v: 3}\n"
        }
    );

    stack.stop().await;
    mgmt.abort();
}

// ---------------------------------------------------------------------
// 6. management restart: state and pointer survive, host re-reads them
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versions_and_active_pointer_survive_a_management_restart() {
    let env = Env::new("restart");
    let (addr, token, mgmt) = spawn_mgmt(&env.root).await;
    let mut stack = Stack::start(&env).await;

    let v1 = worker_dir(
        &env.base,
        "v1",
        "edge-api",
        "export default {v: 1}\n",
        &[("ONLY_V1", "1")],
    );
    let (st, first) = deploy(addr, &token, &v1, false).await;
    assert_eq!(st, 200, "deploy v1: {first}");
    let v1_id = first["versionId"].as_str().unwrap().to_string();

    let v2 = worker_dir(
        &env.base,
        "v2",
        "edge-api",
        "export default {v: 2}\n",
        &[("ONLY_V2", "1")],
    );
    let (st, p2) = deploy(addr, &token, &v2, true).await;
    assert_eq!(st, 200, "preview v2: {p2}");
    let v2_id = p2["versionId"].as_str().unwrap().to_string();
    let (st, promoted) = promote(addr, &token, &v2_id, Some("edge-api")).await;
    assert_eq!(st, 200, "promote v2: {promoted}");

    let (_, before) = versions(addr, &token, "edge-api").await;
    let (st, status_before) = call(addr, "GET", "/v1/status", Some(&token), None).await;
    assert_eq!(st, 200);
    let before_rev = json(&status_before)["revision"].clone();
    assert!(
        stack.wait_env("VRT_VAR_ONLY_V2", "present").await,
        "v2 must be serving before the restart"
    );

    // --- restart 1: management listener/process only ---
    mgmt.abort();
    let (addr2, token2, mgmt2) = spawn_mgmt(&env.root).await;
    assert_eq!(token2, token, "restart reloads the same mgmt token");
    let (st, after) = versions(addr2, &token2, "edge-api").await;
    assert_eq!(st, 200, "versions readable after restart: {after}");
    assert_eq!(after["activeVersionId"], v2_id);
    assert_eq!(after["activeVersionId"], before["activeVersionId"]);
    assert_eq!(active_details(&after).len(), 1);
    assert_eq!(detail(&after, &v2_id)["status"], "active");
    assert_eq!(detail(&after, &v1_id)["status"], "archived");
    let (st, status_after) = call(addr2, "GET", "/v1/status", Some(&token2), None).await;
    assert_eq!(st, 200);
    assert_eq!(json(&status_after)["revision"], before_rev);

    // --- restart 2: the whole data path is rebuilt from disk ---
    stack.stop().await;
    let mut fresh = Stack::start(&env).await;
    assert!(
        fresh.wait_env("VRT_VAR_ONLY_V2", "present").await,
        "a fresh host must serve the persisted active version"
    );
    let (st, headers, body) = fresh
        .get(&[("x-stub-env-check", "VRT_VAR_ONLY_V1,VRT_VAR_ONLY_V2")])
        .await;
    assert_eq!(st, 200, "{body}");
    assert_eq!(
        marker(&headers, "VRT_VAR_ONLY_V2").as_deref(),
        Some("present")
    );
    assert_eq!(
        marker(&headers, "VRT_VAR_ONLY_V1").as_deref(),
        Some("absent")
    );
    assert_eq!(
        desired_active(&env.root, "edge-api").as_deref(),
        Some(v2_id.as_str())
    );

    fresh.stop().await;
    mgmt2.abort();
}
