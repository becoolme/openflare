//! Versioned PREVIEW URLs — end to end through the REAL paths:
//!
//!   mgmt API (`/v1/deploy {preview:true}` → `previewUrl`) →
//!   signed desired (preview instance) → FileDesiredStore (verify+fence) →
//!   SupervisorHost + vrt-stub-workerd → ingress by Host header.
//!
//! Proves the MVP contract:
//! - a preview version is served on
//!   `<versionId>--<worker>.<previewDomain>` while production keeps
//!   serving the active version (different socket, labelled response),
//! - a preview request never touches production metrics/content,
//! - an unknown/archived version is a hard 404 (never a production
//!   fallback),
//! - promote switches production and leaves the version's preview URL
//!   pointing at the (now active) production instance,
//! - with no `[preview] domain` configured the feature is off: no
//!   `previewUrl`, no route.
//!
//! The stub runtime reports `x-stub-worker: sock-<workerId>`, so the
//! assertions can tell WHICH instance served a request even though the
//! stub never executes module code. The version→bundle binding itself is
//! proven by the desired document (`activeVersion.versionId` per
//! instance) plus the assembler's per-instance `fetch_bundle` (a missing
//! preview artifact fails assembly).

mod common;

use common::*;
use std::collections::BTreeMap;
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
use vrt_local::{InitParams, VrtLocal};
use vrt_provider::FileDesiredStore;

const PREVIEW_DOMAIN: &str = "preview.local";
const PROD_HOST: &str = "api.preview-e2e.test";

/// Drive the real management router in-process (same helper shape as
/// `standalone_smoke.rs`: the socket transport adds nothing the routing,
/// auth and library ops do not already exercise).
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

fn worker_dir(root: &Path, name: &str, version: u8) -> PathBuf {
    let dir = root.join(format!("worker-{name}-v{version}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n\
             hostnames = [\"{PROD_HOST}\"]\n\n\
             [vars]\nRELEASE = \"v{version}\"\n"
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

/// Write the host config knob that BOTH halves read: `vrt-local` for the
/// displayed `previewUrl`, `vrt-host` for routing.
fn write_host_config(root: &Path, domain: Option<&str>) {
    let dir = root.join("config");
    std::fs::create_dir_all(&dir).unwrap();
    let body = match domain {
        Some(d) => format!("cellId = \"local\"\n\n[preview]\ndomain = \"{d}\"\n"),
        None => "cellId = \"local\"\n".to_string(),
    };
    std::fs::write(dir.join("vrt-host.toml"), body).unwrap();
}

/// The host-side knob, read through the real typed config struct (only
/// the `[preview]` table — a full `HostConfig` needs paths/binaries this
/// test does not use).
fn host_preview_domain(root: &Path) -> Option<String> {
    let text = std::fs::read_to_string(root.join("config/vrt-host.toml")).unwrap();
    let doc: toml::Value = toml::from_str(&text).unwrap();
    let cfg: vrt_host::config::PreviewConfig = doc
        .get("preview")
        .cloned()
        .map(|v| v.try_into().unwrap())
        .unwrap_or_default();
    cfg.validate().expect("host preview config validates");
    cfg.domain()
}

struct Rig {
    root: PathBuf,
    worker: String,
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
    /// One reload+converge tick — standalone mode: no assignment, the
    /// full verified desired converges, then routes swap atomically.
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

    fn serving_digest(&self) -> Option<String> {
        self.registry
            .get("local")
            .and_then(|rt| rt.serving_digest())
    }

    /// The live generation exposes a socket for `worker_id` — i.e. the
    /// switch that includes it has installed.
    fn has_endpoint(&self, worker_id: &str) -> bool {
        self.registry
            .get("local")
            .and_then(|rt| rt.endpoint_slot(worker_id))
            .is_some()
    }

    fn desired(&self) -> serde_json::Value {
        let raw = std::fs::read(self.root.join("desired/envelope.json")).unwrap();
        let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        doc["payload"].clone()
    }

    fn active_version(&self) -> Option<String> {
        self.desired()["tenants"][0]["workers"]
            .as_array()?
            .iter()
            .find(|w| w.get("preview").is_none())
            .and_then(|w| w["activeVersion"]["versionId"].as_str())
            .map(str::to_string)
    }

    fn worker_id(&self) -> String {
        VrtLocal::open(&self.root)
            .unwrap()
            .status()
            .unwrap()
            .workers[0]
            .worker_id
            .clone()
    }
}

async fn boot(preview_domain: Option<&str>, worker: &str) -> (Rig, axum::Router, String) {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "preview-e2e-{}-{:?}",
        std::process::id(),
        std::time::Instant::now()
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
    write_host_config(&root, preview_domain);
    let preview_domain = host_preview_domain(&root);

    let (router, _api) = vrt_local::mgmt::router(root.clone()).unwrap();
    let token = String::from_utf8(std::fs::read(root.join("mgmt/token")).unwrap()).unwrap();

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
    let rig = Rig {
        root,
        worker: worker.to_string(),
        store,
        host,
        assembler,
        registry,
        status,
        ingress,
        preview_domain,
        _dir: base.clone(),
    };
    (rig, router, token)
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// `worker_id` request counter for `2xx` responses, scraped from the
/// Prometheus text (the registry is process-global).
fn worker_requests(worker_id: &str) -> f64 {
    let text = vrt_host::metrics::registry().render();
    let needle = format!("worker_id=\"{worker_id}\"");
    text.lines()
        .filter(|l| l.starts_with("vrt_worker_requests_total{"))
        .filter(|l| l.contains(&needle) && l.contains("status_class=\"2xx\""))
        .filter_map(|l| l.rsplit(' ').next())
        .filter_map(|v| v.parse::<f64>().ok())
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preview_url_serves_the_version_without_touching_production() {
    let (mut rig, mut router, token) = boot(Some(PREVIEW_DOMAIN), "edge-main").await;
    let ig = start_ingress(&rig.ingress.clone()).await;

    // ---- v1: the production deploy ----
    let v1 = worker_dir(&rig.root, &rig.worker, 1);
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": v1})),
    )
    .await;
    assert_eq!(st, 200, "deploy v1: {body}");
    let v1_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let v1_id = v1_json["versionId"].as_str().unwrap().to_string();
    assert!(
        v1_json["previewUrl"].is_null(),
        "a normal deploy is not a preview: {body}"
    );
    assert!(rig.drive_until(|r| r.tenant_ready()).await, "v1 ready");
    let wid = rig.worker_id();
    let (st, headers, _) = http_get(ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200);
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str()),
        "production host serves the production instance"
    );
    assert!(
        header(&headers, "x-vrt-preview-version").is_none(),
        "production responses carry no preview marker"
    );
    let prod_counter_before = worker_requests(&wid);

    // ---- v2: a preview deploy ----
    let v2 = worker_dir(&rig.root, &rig.worker, 2);
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": v2, "preview": true})),
    )
    .await;
    assert_eq!(st, 200, "deploy v2 preview: {body}");
    let v2_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let v2_id = v2_json["versionId"].as_str().unwrap().to_string();
    let preview_url = v2_json["previewUrl"]
        .as_str()
        .unwrap_or_else(|| panic!("previewUrl missing: {body}"))
        .to_string();
    let preview_host = format!("{v2_id}--{}.{PREVIEW_DOMAIN}", rig.worker);
    assert_eq!(preview_url, format!("https://{preview_host}/"));
    // The active pointer did not move.
    assert_eq!(rig.active_version().as_deref(), Some(v1_id.as_str()));
    let preview_instance = vrt_local::preview_instance_id(&wid, &v2_id);
    assert!(
        rig.drive_until(|r| r.tenant_ready() && r.has_endpoint(&preview_instance))
            .await,
        "preview instance never installed a socket"
    );

    // ---- preview host → the preview instance, labelled ----

    let (st, headers, _) = http_get(ig.addr, &preview_host, "/", &[]).await;
    assert_eq!(st, 200, "preview host must serve the preview version");
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{preview_instance}").as_str()),
        "preview host serves the PREVIEW instance, not production"
    );
    assert_eq!(
        header(&headers, "x-vrt-preview-version"),
        Some(v2_id.as_str())
    );
    assert_eq!(header(&headers, "x-vrt-preview"), Some("1"));

    // ---- preview traffic never touches production ----
    assert_eq!(
        worker_requests(&wid),
        prod_counter_before,
        "a preview request must not be counted against the production worker"
    );
    assert!(
        worker_requests(&preview_instance) > 0.0,
        "preview requests are attributed to the preview instance"
    );
    let (st, prod_headers, _) = http_get(ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200);
    assert_eq!(
        header(&prod_headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str())
    );
    assert!(header(&prod_headers, "x-vrt-preview").is_none());

    // ---- unknown / archived versions are hard 404s ----
    let (st, headers, body) = http_get(
        ig.addr,
        &format!("vrt_deadbeef--{}.{PREVIEW_DOMAIN}", rig.worker),
        "/",
        &[],
    )
    .await;
    assert_eq!(st, 404, "unknown preview version must 404: {body}");
    assert!(header(&headers, "x-stub-worker").is_none());
    // The version the preview was built from (v1) is ACTIVE, so its
    // preview hostname resolves — but to the production instance.
    let (st, headers, _) = http_get(
        ig.addr,
        &format!("{v1_id}--{}.{PREVIEW_DOMAIN}", rig.worker),
        "/",
        &[],
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(
        header(&headers, "x-vrt-preview-version"),
        Some(v1_id.as_str()),
        "an active version is reachable through its preview URL"
    );
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str())
    );

    // ---- versions list carries previewUrl per item ----
    let (st, body) = api(
        &mut router,
        "GET",
        &format!("/v1/versions?worker={}", rig.worker),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let list: serde_json::Value = serde_json::from_str(&body).unwrap();
    let details = list["versionDetails"].as_array().unwrap();
    let row = |id: &str| {
        details
            .iter()
            .find(|v| v["versionId"] == id)
            .unwrap_or_else(|| panic!("version {id} missing"))
    };
    assert_eq!(
        row(&v2_id)["previewUrl"].as_str(),
        Some(format!("https://{preview_host}/").as_str())
    );
    assert!(row(&v1_id)["previewUrl"].as_str().is_some(), "active");
    assert_eq!(row(&v2_id)["status"], "preview");
    assert_eq!(row(&v1_id)["status"], "active");

    // ---- promote v2 → production serves v2, preview URL follows ----
    let digest_before = rig.serving_digest();
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/promote",
        Some(&token),
        Some(serde_json::json!({"versionId": v2_id, "worker": rig.worker})),
    )
    .await;
    assert_eq!(st, 200, "promote: {body}");
    assert!(
        rig.drive_until(|r| r.tenant_ready()
            && !r.has_endpoint(&preview_instance)
            && r.serving_digest() != digest_before)
            .await,
        "promoted generation never installed"
    );
    assert_eq!(rig.active_version().as_deref(), Some(v2_id.as_str()));
    // The generation actually re-rendered: the fingerprint moved.
    assert_ne!(
        rig.serving_digest(),
        digest_before,
        "promote must install a new generation"
    );
    // Production host: same socket, now the v2 desired version.
    let (st, headers, _) = http_get(ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200);
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str())
    );
    assert!(header(&headers, "x-vrt-preview").is_none());
    // The preview instance is gone from desired (v2 is now active) and the
    // preview URL resolves to the production instance.
    let remaining_previews = rig.desired()["tenants"][0]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|w| w.get("preview").is_some())
        .count();
    assert_eq!(
        remaining_previews, 0,
        "an active version is not a preview instance"
    );
    let (st, headers, _) = http_get(ig.addr, &preview_host, "/", &[]).await;
    assert_eq!(st, 200);
    assert_eq!(
        header(&headers, "x-vrt-preview-version"),
        Some(v2_id.as_str())
    );
    assert_eq!(
        header(&headers, "x-stub-worker"),
        Some(format!("sock-{wid}").as_str()),
        "the active version's preview URL is served by production"
    );
    // v1 (now archived) loses its preview URL.
    let (st, _, _) = http_get(
        ig.addr,
        &format!("{v1_id}--{}.{PREVIEW_DOMAIN}", rig.worker),
        "/",
        &[],
    )
    .await;
    assert_eq!(st, 404, "an archived version has no preview route");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preview_url_is_off_without_a_configured_domain() {
    let (mut rig, mut router, token) = boot(None, "edge-off").await;
    assert_eq!(rig.preview_domain, None, "no domain ⇒ off");
    let ig = start_ingress(&rig.ingress.clone()).await;

    let v1 = worker_dir(&rig.root, &rig.worker, 1);
    let (st, _) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": v1})),
    )
    .await;
    assert_eq!(st, 200);
    assert!(rig.drive_until(|r| r.tenant_ready()).await);

    let v2 = worker_dir(&rig.root, &rig.worker, 2);
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": v2, "preview": true})),
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let v2_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let v2_id = v2_json["versionId"].as_str().unwrap().to_string();
    assert!(
        v2_json["previewUrl"].is_null(),
        "no configured domain ⇒ previewUrl is null: {body}"
    );
    assert!(rig.drive_until(|r| r.tenant_ready()).await);

    // The preview hostname is not routable at all (default-off), and
    // production is unaffected.
    let (st, _, _) = http_get(
        ig.addr,
        &format!("{v2_id}--{}.{PREVIEW_DOMAIN}", rig.worker),
        "/",
        &[],
    )
    .await;
    assert_eq!(st, 404, "preview hosts must 404 when the feature is off");
    let (st, _, _) = http_get(ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preview_bundle_must_be_present_or_assembly_fails_closed() {
    let (mut rig, mut router, token) = boot(Some(PREVIEW_DOMAIN), "edge-artifact").await;
    let _ig = start_ingress(&rig.ingress.clone()).await;
    let v1 = worker_dir(&rig.root, &rig.worker, 1);
    let (st, _) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": v1})),
    )
    .await;
    assert_eq!(st, 200);
    assert!(rig.drive_until(|r| r.tenant_ready()).await);
    let v2 = worker_dir(&rig.root, &rig.worker, 2);
    let (st, body) = api(
        &mut router,
        "POST",
        "/v1/deploy",
        Some(&token),
        Some(serde_json::json!({"workerDir": v2, "preview": true})),
    )
    .await;
    assert_eq!(st, 200, "{body}");
    let v2_json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let digest = v2_json["artifactDigest"].as_str().unwrap().to_string();

    // Drop the preview artifact: the next generation cannot assemble, and
    // the OLD (production-only) generation must keep serving.
    std::fs::remove_file(rig.root.join("objects").join(&digest)).unwrap();
    for _ in 0..40 {
        rig.tick();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        rig.tenant_ready(),
        "a missing preview artifact must never take production down"
    );
    let (st, _, _) = http_get(_ig.addr, PROD_HOST, "/", &[]).await;
    assert_eq!(st, 200, "production keeps serving");
}
