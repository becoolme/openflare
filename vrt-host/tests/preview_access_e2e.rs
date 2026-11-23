//! Preview ACCESS CONTROL — signed tokens end to end.
//!
//! Proves the contract that a preview URL is a bearer capability, not a
//! public hostname:
//!
//! - no token / expired / tampered / wrong-version / wrong-worker token
//!   is a hard 403 (never a fallback to production),
//! - a correct token serves the preview instance,
//! - the token is stripped from the forwarded request and bootstrapped as
//!   a host-scoped cookie so subresources work without the query param,
//! - production hostnames need no token (unchanged),
//! - a `[preview] domain` WITHOUT a token secret leaves the preview
//!   domain closed (404) and reports no `previewUrl`.

mod common;

use common::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use snapshot_protocol::preview_token::PreviewTokenKey;
use vrt_host::artifact::{BundleCache, DirBundleSource};
use vrt_host::assemble::{Assembler, EmitMode};
use vrt_host::desired::{DesiredSource, HostDesiredStore};
use vrt_host::ingress::IngressState;
use vrt_host::render_inputs::FileRenderInputs;
use vrt_host::route::RouteTable;
use vrt_host::status::new_status;
use vrt_host::supervisor::{SupervisorDeps, SupervisorHost, TenantRegistry};
use vrt_local::{InitParams, VrtLocal};
use vrt_provider::FileDesiredStore;

const DOMAIN: &str = "preview.access.test";
const PROD_HOST: &str = "api.access.test";

async fn api(
    router: &mut axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    use tower::ServiceExt;
    let b = axum::http::Request::builder()
        .method(method)
        .uri(format!("http://vrt{path}"))
        .header("authorization", format!("Bearer {token}"));
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
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

fn worker_dir(root: &Path, name: &str, version: u8) -> PathBuf {
    let dir = root.join(format!("worker-{name}-v{version}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n\
             hostnames = [\"{PROD_HOST}\"]\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        format!("export default {{ fetch() {{ return new Response('v{version}') }} }}\n"),
    )
    .unwrap();
    dir
}

/// Write the `[preview]` table. `token_secret` = false models the
/// insecure/legacy config shape (domain only), which must stay CLOSED.
fn write_host_config(root: &Path, token_secret: bool) {
    let dir = root.join("config");
    std::fs::create_dir_all(&dir).unwrap();
    let body = if token_secret {
        format!(
            "cellId = \"local\"\n\n[preview]\ndomain = \"{DOMAIN}\"\n\
             tokenSecretFile = \"{}\"\n",
            root.join("keys/preview-token.secret").display()
        )
    } else {
        format!("cellId = \"local\"\n\n[preview]\ndomain = \"{DOMAIN}\"\n")
    };
    std::fs::write(dir.join("vrt-host.toml"), body).unwrap();
}

fn host_routing(root: &Path) -> Option<vrt_host::config::PreviewRouting> {
    let text = std::fs::read_to_string(root.join("config/vrt-host.toml")).unwrap();
    let doc: toml::Value = toml::from_str(&text).unwrap();
    let cfg: vrt_host::config::PreviewConfig = doc
        .get("preview")
        .cloned()
        .map(|v| v.try_into().unwrap())
        .unwrap_or_default();
    cfg.routing().expect("preview routing resolves")
}

struct Rig {
    root: PathBuf,
    worker: String,
    worker_id: String,
    preview_key: PreviewTokenKey,
    router: axum::Router,
    mgmt_token: String,
    store: Arc<HostDesiredStore>,
    host: SupervisorHost,
    assembler: Arc<dyn vrt_host::supervisor::TenantAssembler>,
    registry: Arc<TenantRegistry>,
    status: vrt_host::status::HostStatus,
    ingress: Arc<IngressState>,
    preview_domain: Option<String>,
    _dir: PathBuf,
}

impl Rig {
    fn tick(&mut self) {
        let _ = self.store.inner().reload(chrono::Utc::now());
        let Ok(desired) = self.store.snapshot() else {
            return;
        };
        self.host.converge(&desired, &self.assembler);
        self.ingress
            .set_routes(RouteTable::from_desired_with_preview_domain(
                &desired,
                self.preview_domain.as_deref(),
            ));
        {
            let mut st = self.status.write().unwrap_or_else(|e| e.into_inner());
            st.revision = Some(desired.revision);
            st.payload_digest = Some(self.store.inner().payload_digest().unwrap_or_default());
            st.blocked_reason = None;
        }
        self.host.reap_finished();
    }

    async fn drive_until(&mut self, pred: impl Fn(&Self) -> bool) -> bool {
        for _ in 0..400 {
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

    fn token(&self, version_id: &str, worker_id: &str, expires_at: u64) -> String {
        self.preview_key.mint(version_id, worker_id, expires_at)
    }

    fn now() -> u64 {
        chrono::Utc::now().timestamp().max(0) as u64
    }

    async fn deploy(&mut self, version: u8, preview: bool) -> String {
        let dir = worker_dir(&self.root, &self.worker, version);
        let (st, body) = api(
            &mut self.router.clone(),
            "POST",
            "/v1/deploy",
            &self.mgmt_token,
            Some(serde_json::json!({"workerDir": dir, "preview": preview})),
        )
        .await;
        assert_eq!(st, 200, "deploy v{version} preview={preview}: {body}");
        body["versionId"].as_str().unwrap().to_string()
    }
}

async fn boot(token_secret: bool, worker: &str) -> Rig {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "preview-access-{}-{}",
        std::process::id(),
        worker
    ));
    let _ = std::fs::remove_dir_all(&base);
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
    write_host_config(&root, token_secret);
    let routing = host_routing(&root);
    let (preview_domain, preview_key) = match &routing {
        Some(r) => (Some(r.domain.clone()), Some(r.key.clone())),
        None => {
            // Key for the "no token secret" case: still derive it so the
            // test can prove that presenting a *valid* token does not
            // open a closed preview domain.
            let key = PreviewTokenKey::from_hex(
                &std::fs::read_to_string(root.join("keys/preview-token.secret")).unwrap(),
            )
            .unwrap();
            (None, Some(key))
        }
    };

    let (router, _api) = vrt_local::mgmt::router(root.clone()).unwrap();
    let mgmt_token = String::from_utf8(std::fs::read(root.join("mgmt/token")).unwrap()).unwrap();

    let host_dir = base.join("host");
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
    ingress.set_preview_auth(preview_key.clone());
    Rig {
        root,
        worker: worker.to_string(),
        worker_id: String::new(),
        preview_key: preview_key.unwrap(),
        router,
        mgmt_token,
        store,
        host,
        assembler,
        registry,
        status,
        ingress,
        preview_domain,
        _dir: base,
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preview_requires_a_token_bound_to_version_and_worker() {
    let mut rig = boot(true, "edge-access").await;
    let ig = start_ingress(&rig.ingress.clone()).await;

    let v1_id = rig.deploy(1, false).await;
    assert!(rig.drive_until(|r| r.tenant_ready()).await, "v1 ready");
    rig.worker_id = VrtLocal::open(&rig.root)
        .unwrap()
        .status()
        .unwrap()
        .workers[0]
        .worker_id
        .clone();
    let wid = rig.worker_id.clone();

    let v2_id = rig.deploy(2, true).await;
    let preview_instance = vrt_local::preview_instance_id(&wid, &v2_id);
    assert!(
        rig.drive_until(|r| r.tenant_ready()
            && r.registry
                .get("local")
                .and_then(|rt| rt.endpoint_slot(&preview_instance))
                .is_some())
            .await,
        "preview instance never installed"
    );
    let preview_host = format!("{v2_id}--{}.{DOMAIN}", rig.worker);
    let now = Rig::now();

    // ---- no token: 403, never production ----
    let (st, headers, body) = http_get(ig.addr, &preview_host, "/", &[]).await;
    assert_eq!(st, 403, "anonymous preview must be refused: {body}");
    assert!(
        body.contains("preview_token_missing"),
        "typed refusal: {body}"
    );
    assert!(header(&headers, "x-vrt-preview").is_none());
    assert!(header(&headers, "x-stub-worker").is_none());

    // ---- tampered token: 403 ----
    let valid = rig.token(&v2_id, &wid, now + 600);
    let mut chars: Vec<char> = valid.chars().collect();
    let last = chars.len() - 1;
    chars[last] = if chars[last] == '0' { '1' } else { '0' };
    let tampered: String = chars.into_iter().collect();
    let (st, _, body) = http_get(
        ig.addr,
        &preview_host,
        &format!("/?__vrt_preview_token={tampered}"),
        &[],
    )
    .await;
    assert_eq!(st, 403, "tampered token: {body}");
    assert!(body.contains("preview_token"), "{body}");

    // ---- expired token: 403 ----
    let expired = rig.token(&v2_id, &wid, now.saturating_sub(10));
    let (st, _, body) = http_get(
        ig.addr,
        &preview_host,
        &format!("/?__vrt_preview_token={expired}"),
        &[],
    )
    .await;
    assert_eq!(st, 403, "expired token: {body}");
    assert!(body.contains("preview_token_expired"), "{body}");

    // ---- token for a different VERSION: 403 ----
    let wrong_version = rig.token(&v1_id, &wid, now + 600);
    let (st, _, body) = http_get(
        ig.addr,
        &preview_host,
        &format!("/?__vrt_preview_token={wrong_version}"),
        &[],
    )
    .await;
    assert_eq!(st, 403, "wrong version: {body}");
    assert!(body.contains("preview_token_invalid"), "{body}");

    // ---- token for a different WORKER: 403 ----
    let wrong_worker = rig.token(&v2_id, "w_other", now + 600);
    let (st, _, body) = http_get(
        ig.addr,
        &preview_host,
        &format!("/?__vrt_preview_token={wrong_worker}"),
        &[],
    )
    .await;
    assert_eq!(st, 403, "wrong worker: {body}");

    // ---- correct token: 200 on the PREVIEW instance + cookie ----
    let (st, headers, body) = http_get(
        ig.addr,
        &preview_host,
        &format!("/?__vrt_preview_token={valid}"),
        &[],
    )
    .await;
    assert_eq!(st, 200, "valid token must serve: {body}");
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{preview_instance}").as_str())
    );
    assert_eq!(
        header(&headers, "x-vrt-preview-version"),
        Some(v2_id.as_str())
    );
    assert_eq!(
        header(&headers, "referrer-policy"),
        Some("strict-origin-when-cross-origin"),
        "a tokenized URL must not leak cross-origin through Referer"
    );
    assert_eq!(
        header(&headers, "x-robots-tag"),
        Some("noindex, nofollow"),
        "tokenized previews stay out of search indexes"
    );
    let set_cookie = header(&headers, "set-cookie").expect("bootstrap cookie");
    assert!(set_cookie.contains("__vrt_preview_token="), "{set_cookie}");
    assert!(set_cookie.contains("HttpOnly"), "{set_cookie}");
    assert!(set_cookie.contains("Path=/"), "{set_cookie}");

    // ---- cookie-only follow-up (subresources): 200 ----
    let cookie = set_cookie.split(';').next().unwrap().to_string();
    let (st, headers, _) = http_get(ig.addr, &preview_host, "/asset.js", &[("cookie", &cookie)]).await;
    assert_eq!(st, 200, "bootstrap cookie authenticates subresources");
    assert_eq!(
        header(&headers, "x-vrt-preview-version"),
        Some(v2_id.as_str())
    );

    // ---- production host needs no token and carries no preview markers ----
    let (st, headers, _) = http_get(ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200, "production routing is unchanged");
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str())
    );
    assert!(header(&headers, "x-vrt-preview").is_none());
    assert!(header(&headers, "x-vrt-preview-version").is_none());

    // ---- the ACTIVE version's preview host is token-gated too ----
    let v1_host = format!("{v1_id}--{}.{DOMAIN}", rig.worker);
    let (st, _, _) = http_get(ig.addr, &v1_host, "/", &[]).await;
    assert_eq!(st, 403, "active-version preview host without token");
    let (st, headers, _) = http_get(
        ig.addr,
        &v1_host,
        &format!("/?__vrt_preview_token={}", rig.token(&v1_id, &wid, now + 600)),
        &[],
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str()),
        "the active version's preview URL is served by production"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_domain_without_a_token_secret_stays_closed() {
    let mut rig = boot(false, "edge-closed").await;
    assert_eq!(rig.preview_domain, None, "no token secret ⇒ previews off");
    let ig = start_ingress(&rig.ingress.clone()).await;

    let v1_id = rig.deploy(1, false).await;
    assert!(rig.drive_until(|r| r.tenant_ready()).await);
    rig.worker_id = VrtLocal::open(&rig.root)
        .unwrap()
        .status()
        .unwrap()
        .workers[0]
        .worker_id
        .clone();

    let (st, body) = api(
        &mut rig.router.clone(),
        "POST",
        "/v1/deploy",
        &rig.mgmt_token,
        Some(serde_json::json!({
            "workerDir": worker_dir(&rig.root, &rig.worker, 2),
            "preview": true,
        })),
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let v2_id = body["versionId"].as_str().unwrap().to_string();
    assert!(
        body["previewUrl"].is_null(),
        "no closed-domain URL may be advertised: {body}"
    );
    let wid = rig.worker_id.clone();
    assert!(rig.drive_until(|r| r.tenant_ready()).await);

    // Even a correctly signed token does not open a closed domain: the
    // host never installed the route.
    let host = format!("{v2_id}--{}.{DOMAIN}", rig.worker);
    let now = Rig::now();
    let token = rig.token(&v2_id, &wid, now + 600);
    let (st, _, _) = http_get(
        ig.addr,
        &host,
        &format!("/?__vrt_preview_token={token}"),
        &[],
    )
    .await;
    assert_eq!(st, 404, "no preview route exists");

    // Production is unaffected.
    let (st, _, _) = http_get(ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200);
    assert_eq!(
        VrtLocal::open(&rig.root)
            .unwrap()
            .versions(&rig.worker)
            .unwrap()
            .versions
            .iter()
            .find(|v| v.version_id == v1_id)
            .unwrap()
            .preview_url,
        None,
        "the active version also reports no URL while the domain is closed"
    );
}
