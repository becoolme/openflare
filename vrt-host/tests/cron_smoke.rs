//! CR1: real-workerd cron delivery — `Fetcher.scheduled()` is the
//! verified scheduled-event channel; the platform emits one
//! `facade-cron-<workerId>` service per cron-carrying worker bound to
//! `sock-cron-<id>` (never on ingress). vrt-host `/v1/cron/trigger`
//! dispatches to that socket and must report the facade's `outcome`
//! honestly — workerd resolves scheduled() with
//! `{outcome:"exception"}` on a throwing handler, which maps to a
//! failed delivery, never a false success.
//!
//! Gated on `VRT_WORKERD_BIN` like the other smokes:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test cron_smoke -- --nocapture

use http_body_util::BodyExt;
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use tower::ServiceExt;
use vrt_config::*;

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) => Some(PathBuf::from(p)),
        Err(_) => {
            eprintln!("skip: set VRT_WORKERD_BIN=/path/to/workerd");
            None
        }
    }
}

static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn smoke_root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/tmp-cron-smoke")
        .join(format!(
            "run-{}-{}",
            std::process::id(),
            RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        if std::env::var("CRON_KEEP_DIR").is_err() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct WorkerProc {
    child: Child,
    log_path: PathBuf,
}
impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// The three worker modules — one healthy scheduled(), one throwing,
/// one missing the handler entirely.
const MOD_OK: &str = r#"
let fired = [];
export default {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/last") return Response.json({ fired });
    return new Response("ingress-ok");
  },
  async scheduled(controller, env, ctx) {
    fired.push({
      cron: controller.cron,
      scheduledTime: controller.scheduledTime,
      type: controller.type ?? null,
      waitUntil: typeof ctx.waitUntil,
      noRetry: typeof controller.noRetry,
    });
  },
};
"#;
const MOD_THROW: &str = r#"
export default {
  async fetch() { return new Response("ok"); },
  async scheduled() { throw new Error("sched boom"); },
};
"#;
const MOD_MISS: &str = r#"
export default { async fetch() { return new Response("ok"); } };
"#;

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let child = Command::new(bin)
        .arg("serve")
        // `binding-backend` external is declared by the manifest;
        // nothing in this smoke calls it, but workerd requires every
        // external service to have an address.
        .arg("--external-addr=binding-backend=127.0.0.1:1")
        .arg(cfg)
        .arg("--experimental")
        .current_dir(dir)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn workerd");
    WorkerProc { child, log_path }
}

fn http_once(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> std::io::Result<(u16, String)> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: x\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        match s.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Ok((status, body))
}

fn http(port: u16, path: &str) -> (u16, String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match http_once(port, "GET", path, None) {
            Ok(r) => return r,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "http {path} never connected: {e}"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
}

fn post_json(port: u16, path: &str, body: &str) -> (u16, String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match http_once(port, "POST", path, Some(body)) {
            Ok(r) => return r,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "post {path} never connected: {e}"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
}

/// Render a 3-worker tenant — each declares crons, each gets its own
/// `sock-cron-<id>` facade socket. Returns (cfg_path, ports).
fn render_write(dir: &Path) -> (PathBuf, BTreeMap<String, u16>, BTreeMap<String, u16>) {
    let workers = [
        ("wok", "cron-ok", MOD_OK),
        ("wthrow", "cron-throw", MOD_THROW),
        ("wmiss", "cron-miss", MOD_MISS),
    ];
    let mut bundles = Vec::new();
    let mut worker_json = Vec::new();
    let mut ingress_ports = BTreeMap::new();
    let mut cron_ports = BTreeMap::new();
    for (id, name, module) in workers {
        let bundle = VersionBundle::build(
            DeployManifest::from_pack(&json!({
                "mainModule": "index.js",
                "modules": { "index.js": module },
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            }))
            .unwrap(),
        )
        .unwrap();
        worker_json.push(json!({
            "workerId": id,
            "name": name,
            "activeVersion": {
                "versionId": format!("v-{id}-1"),
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": format!("{id}--team-smoke.vibeploy.app")}],
            "crons": [{"scheduleId": format!("sch-{id}"), "expression": "* * * * *"}],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }));
        bundles.push(bundle);
        ingress_ports.insert(id.to_string(), free_port());
        cron_ports.insert(id.to_string(), free_port());
    }
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-smoke",
        "workers": worker_json
    }))
    .unwrap();
    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&manifest_raw).unwrap();
    let params = RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 30_000,
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "cron-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &bundles.iter().cloned().collect::<Vec<_>>(),
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

    // Emit invariants on the REAL text.
    for (id, ..) in workers {
        assert!(
            out.text.contains(&format!("facade-cron-{id}")),
            "missing facade for {id}:\n{}",
            out.text
        );
        assert!(
            out.text.contains(&format!("sock-cron-{id}")),
            "missing cron socket for {id}:\n{}",
            out.text
        );
    }
    // The facade carries the `$experimental` flag; the tenant worker
    // itself must NOT (it is not tenant-declarable — only the facade
    // needs `.scheduled()` on its TARGET binding).
    assert!(
        out.text.contains("service_binding_extra_handlers"),
        "{}",
        out.text
    );

    // Materialize embeds.
    let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap();
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap();
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes = std::fs::read(&src).unwrap();
                assert_eq!(format!("{:x}", Sha256::digest(&bytes)), embed.digest);
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { worker_id, .. } => {
                let module = workers
                    .iter()
                    .find(|(id, ..)| *id == worker_id.as_str())
                    .unwrap()
                    .2;
                std::fs::write(&dst, module).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                std::fs::write(&dst, vrt_config::workflows::vendored_bytes(name).unwrap()).unwrap();
            }
        }
    }

    // Unix sockets → TCP (sandbox forbids unix bind).
    let mut text = out.text.clone();
    for (id, ..) in workers {
        text = text.replace(
            &format!("unix:sock/{id}-cron.sock"),
            &format!("127.0.0.1:{}", cron_ports[id]),
        );
        text = text.replace(
            &format!("unix:sock/{id}.sock"),
            &format!("127.0.0.1:{}", ingress_ports[id]),
        );
    }
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    (path, ingress_ports, cron_ports)
}

/// CR1-a: end-to-end — facade → scheduled() → observable state +
/// honest outcome, plus the host `/v1/cron/trigger` path through the
/// cron endpoint slot (dedupe proves at-most-once at the host seam).
#[tokio::test(flavor = "current_thread")]
async fn real_workerd_cron_facade_and_trigger() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let (cfg, ingress, cron) = render_write(&dir);
    let proc = spawn(&bin, &cfg, &dir);

    // Ingress healthy.
    let (st, body) = http(ingress["wok"], "/");
    assert_eq!(
        (st, body.as_str()),
        (200, "ingress-ok"),
        "log: {:?}",
        proc.log_path
    );

    // 1) Direct facade post: `{cron, scheduledTime}` forwarded verbatim.
    let st_ms = 1_767_225_600_000u64; // 2026-01-01T00:00:00Z
    let (st, body) = post_json(
        cron["wok"],
        "/",
        &format!("{{\"cron\":\"* * * * *\",\"scheduledTime\":{st_ms}}}"),
    );
    assert_eq!(st, 200, "facade must deliver: {body}");
    let j: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(j["delivered"], true, "{body}");
    assert_eq!(j["outcome"], "ok", "{body}");

    // 2) scheduled() actually ran — exact fields.
    let (st, body) = http(ingress["wok"], "/last");
    assert_eq!(st, 200);
    let j: serde_json::Value = serde_json::from_str(&body).unwrap();
    let fired = j["fired"].as_array().unwrap();
    assert_eq!(fired.len(), 1, "{body}");
    assert_eq!(fired[0]["cron"], "* * * * *", "{body}");
    assert_eq!(fired[0]["scheduledTime"], st_ms, "{body}");
    assert_eq!(fired[0]["waitUntil"], "function", "{body}");
    assert_eq!(fired[0]["noRetry"], "function", "{body}");

    // 3) RFC3339 scheduledTime is accepted and converted.
    let (st, body) = post_json(
        cron["wok"],
        "/",
        "{\"cron\":\"*/5 * * * *\",\"scheduledTime\":\"2026-01-01T00:05:00Z\"}",
    );
    assert_eq!(
        (
            st,
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["delivered"].as_bool()
        ),
        (200, Some(true)),
        "{body}"
    );

    // 4) Throwing handler → facade reports outcome:"exception" +
    //    delivered:false — the honesty invariant (workerd resolves,
    //    it does NOT reject).
    let (st, body) = post_json(
        cron["wthrow"],
        "/",
        &format!("{{\"cron\":\"* * * * *\",\"scheduledTime\":{st_ms}}}"),
    );
    assert_eq!(st, 502, "exception must surface as failure: {body}");
    let j: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(j["delivered"], false, "{body}");
    assert_eq!(j["outcome"], "exception", "{body}");

    // 5) Missing scheduled export → same failure surface.
    let (st, body) = post_json(
        cron["wmiss"],
        "/",
        &format!("{{\"cron\":\"* * * * *\",\"scheduledTime\":{st_ms}}}"),
    );
    assert_eq!(st, 502, "{body}");
    let j: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(j["delivered"], false, "{body}");
    assert_eq!(j["outcome"], "exception", "{body}");

    // 6) Host trigger path — `/v1/cron/trigger` through the cron slot.
    use axum::body::Body;
    use axum::http::Request;
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("team-smoke");
    runtime.test_install_cron(BTreeMap::from([(
        "wok".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(
            format!("127.0.0.1:{}", cron["wok"]).parse().unwrap(),
        ),
    )]));
    let app = vrt_host::cron_trigger::router(registry);
    let scheduled_time = "2026-01-01T00:10:00Z";
    let key = format!("wok:v-wok-1:sch-wok:{scheduled_time}");
    let trig = serde_json::json!({
        "tenantId": "team-smoke",
        "workerId": "wok",
        "versionId": "v-wok-1",
        "scheduleId": "sch-wok",
        "scheduledTime": scheduled_time,
        "cron": "* * * * *",
        "leaseEpoch": 0,
    })
    .to_string();
    let mk = || {
        Request::builder()
            .method("POST")
            .uri("/v1/cron/trigger")
            .header("content-type", "application/json")
            .header("x-vrt-cron-idempotency-key", &key)
            .body(Body::from(trig.clone()))
            .unwrap()
    };
    let res = app.clone().oneshot(mk()).await.unwrap();
    let st = res.status().as_u16();
    let body = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    let j: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(st, 200, "{j}");
    assert_eq!(j["delivered"], true, "{j}");
    assert_eq!(j["outcome"], "ok", "{j}");
    // Replay of the same idempotency key → duplicate, NOT re-fired.
    let res = app.clone().oneshot(mk()).await.unwrap();
    assert_eq!(res.status(), 200);
    let body = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    let j: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(j["delivered"], false, "{j}");
    assert_eq!(j["duplicate"], true, "{j}");
    // scheduled() saw exactly three firings — two direct facade posts
    // (epoch ms + RFC3339) and the one host trigger; the replay
    // consumed the dedupe hit before dispatch.
    let (_, body) = http(ingress["wok"], "/last");
    let j: serde_json::Value = serde_json::from_str(&body).unwrap();
    let fired = j["fired"].as_array().unwrap();
    assert_eq!(
        fired.len(),
        3,
        "three deliveries total; replay must not fire: {body}"
    );
    assert_eq!(
        fired[2]["scheduledTime"],
        1_767_226_200_000u64, // 2026-01-01T00:10:00Z
        "{body}"
    );
}

/// CR1-b: a throwing tenant handler surfaces as 502 upstream through
/// the HOST trigger — never a false success.
#[tokio::test(flavor = "current_thread")]
async fn real_workerd_cron_trigger_reports_exception() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let (cfg, _ingress, cron) = render_write(&dir);
    let proc = spawn(&bin, &cfg, &dir);
    // wait for the throwing worker's ingress.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if http_once(_ingress["wthrow"], "GET", "/", None)
            .map(|(s, _)| s == 200)
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "wthrow never ready: {:?}",
            proc.log_path
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    use axum::body::Body;
    use axum::http::Request;
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("team-smoke");
    runtime.test_install_cron(BTreeMap::from([(
        "wthrow".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(
            format!("127.0.0.1:{}", cron["wthrow"]).parse().unwrap(),
        ),
    )]));
    let app = vrt_host::cron_trigger::router(registry);
    let scheduled_time = "2026-01-01T00:15:00Z";
    let trig = serde_json::json!({
        "tenantId": "team-smoke",
        "workerId": "wthrow",
        "versionId": "v-wthrow-1",
        "scheduleId": "sch-wthrow",
        "scheduledTime": scheduled_time,
        "cron": "* * * * *",
        "leaseEpoch": 0,
    })
    .to_string();
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header(
                    "x-vrt-cron-idempotency-key",
                    format!("wthrow:v-wthrow-1:sch-wthrow:{scheduled_time}"),
                )
                .body(Body::from(trig))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 502, "exception must be a failed delivery");
    let body = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    let j: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(j["error"]["code"], "VRT_CRON_DELIVERY_FAILED", "{j}");
}

/// CR1-c: ingress cannot forge a scheduled event — HTTP posts to
/// `__vrt/scheduled` and `/cdn-cgi/handler/scheduled` land in fetch
/// (they get a fetch response, never run scheduled()).
#[tokio::test(flavor = "current_thread")]
async fn real_workerd_cron_ingress_cannot_forge() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let (cfg, ingress, _cron) = render_write(&dir);
    let _proc = spawn(&bin, &cfg, &dir);
    let (st, body) = http(ingress["wok"], "/");
    assert_eq!((st, body.as_str()), (200, "ingress-ok"));
    for path in [
        "/__vrt/scheduled",
        "/cdn-cgi/handler/scheduled",
        "/cdn-cgi/workerd/scheduled",
    ] {
        let (st, body) = post_json(
            ingress["wok"],
            path,
            "{\"cron\":\"* * * * *\",\"scheduledTime\":1767225600000}",
        );
        // The tenant fetch handler owns these paths — whatever it
        // returns (200 here), scheduled() must not have fired.
        let _ = (st, body);
    }
    let (st, body) = http(ingress["wok"], "/last");
    assert_eq!(st, 200);
    let j: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        j["fired"].as_array().unwrap().len(),
        0,
        "forged ingress posts must not reach scheduled(): {body}"
    );
}

/// CR1-d: a worker with no cron schedules has NO `sock-cron-<id>`
/// endpoint — the trigger fails closed at the slot lookup (404),
/// never routed to the worker's ingress.
#[tokio::test]
async fn cron_trigger_no_cron_slot_is_404() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    // Ingress endpoint exists, cron slot does NOT — the slot lookup is
    // the fail-closed boundary.
    runtime.test_install(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    let app = vrt_host::cron_trigger::router(registry);
    let trig = serde_json::json!({
        "tenantId": "t",
        "workerId": "w1",
        "versionId": "v1",
        "scheduleId": "s1",
        "scheduledTime": "2026-01-01T00:00:00Z",
        "cron": "* * * * *",
        "leaseEpoch": 0,
    })
    .to_string();
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header(
                    "x-vrt-cron-idempotency-key",
                    "w1:v1:s1:2026-01-01T00:00:00Z",
                )
                .body(axum::body::Body::from(trig))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
}

/// CR1-e: upstream timeout — an endpoint that accepts but never
/// responds must surface 502 at DISPATCH_TIMEOUT (30 s wall).
#[tokio::test]
async fn cron_trigger_timeout_reports_failure() {
    // TCP acceptor that holds the connection forever.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || loop {
        if let Ok((s, _)) = listener.accept() {
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(90));
                drop(s);
            });
        }
    });
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    runtime.test_install_cron(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(format!("127.0.0.1:{port}").parse().unwrap()),
    )]));
    let app = vrt_host::cron_trigger::router(registry);
    let trig = serde_json::json!({
        "tenantId": "t",
        "workerId": "w1",
        "versionId": "v1",
        "scheduleId": "s1",
        "scheduledTime": "2026-01-01T00:00:00Z",
        "cron": "* * * * *",
        "leaseEpoch": 0,
    })
    .to_string();
    let start = std::time::Instant::now();
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header(
                    "x-vrt-cron-idempotency-key",
                    "w1:v1:s1:2026-01-01T00:00:00Z",
                )
                .body(axum::body::Body::from(trig))
                .unwrap(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(res.status(), 502, "timeout must be a failed delivery");
    assert!(
        elapsed >= std::time::Duration::from_secs(25),
        "timeout fired early: {elapsed:?}"
    );
}

/// CR1-f: stub-mode parity — the stub binds `sock-cron-<id>` like any
/// declared socket and answers the facade contract
/// (`{delivered:true,outcome:"ok"}`) while recording the dispatch to
/// `sock-cron-<id>.log`. The host trigger + dedupe path is the same
/// production code the real facade drives.
#[tokio::test(flavor = "current_thread")]
async fn stub_cron_endpoint_delivers_and_records() {
    let port = free_port();
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let record_dir = dir.join("record");
    std::fs::create_dir_all(&record_dir).unwrap();
    let config = serde_json::json!({
        "format": "vrt-stub-config/v1",
        "tenantId": "t",
        "sockets": {
            "sock-w1": format!("tcp:127.0.0.1:{}", free_port()),
            "sock-cron-w1": format!("tcp:127.0.0.1:{port}"),
        },
    });
    std::fs::write(dir.join("stub-config.json"), config.to_string()).unwrap();
    let mut child = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_vrt-stub-workerd")))
        .arg("serve")
        .arg(dir.join("stub-config.json"))
        .env("VRT_STUB_RECORD_DIR", &record_dir)
        .current_dir(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Wait for the cron socket to accept.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "stub cron socket never bound"
        );
        std::thread::sleep(std::time::Duration::from_millis(30));
    }

    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    runtime.test_install_cron(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(format!("127.0.0.1:{port}").parse().unwrap()),
    )]));
    let app = vrt_host::cron_trigger::router(registry);
    let key = "w1:v1:sch-1:2026-01-01T00:00:00Z";
    let trig = serde_json::json!({
        "tenantId": "t", "workerId": "w1", "versionId": "v1",
        "scheduleId": "sch-1", "scheduledTime": "2026-01-01T00:00:00Z",
        "cron": "* * * * *", "leaseEpoch": 0,
    })
    .to_string();
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/cron/trigger")
                .header("content-type", "application/json")
                .header("x-vrt-cron-idempotency-key", key)
                .body(axum::body::Body::from(trig))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body = axum::body::to_bytes(res.into_body(), 8192).await.unwrap();
    let j: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(j["delivered"], true, "{j}");
    // The dispatch was recorded under the CRON socket's log — never
    // the worker's ingress log.
    let log = std::fs::read_to_string(record_dir.join("sock-cron-w1.log")).unwrap();
    assert!(log.contains("POST /vrt/cron-trigger"), "{log}");
    assert!(log.contains(&format!("idem={key}")), "{log}");
    assert!(
        !record_dir.join("sock-w1.log").exists(),
        "ingress socket must not see cron dispatches"
    );
    let _ = child.kill();
    let _ = child.wait();
}


// ---------------- MIG-11: serialized-switch window ----------------

fn mig11_req(key: &str) -> axum::http::Request<axum::body::Body> {
    axum::http::Request::builder()
        .method("POST")
        .uri("/v1/cron/trigger")
        .header("content-type", "application/json")
        .header("x-vrt-cron-idempotency-key", key)
        .body(axum::body::Body::from(serde_json::json!({
            "tenantId": "t",
            "workerId": "w1",
            "versionId": "v1",
            "scheduleId": "s1",
            "scheduledTime": "2026-01-01T00:00:00Z",
            "cron": "* * * * *",
            "leaseEpoch": 0,
        }).to_string()))
        .unwrap()
}

/// MIG-11: while the serialized-switch gate is active the cron route
/// map is empty — the trigger HOLDS (bounded) and resolves against the
/// freshly-installed generation instead of mis-reporting
/// `VRT_CRON_UNKNOWN_WORKER` (which used to lose the whole minute
/// under at-most-once). Proven by landing a dead cron endpoint
/// mid-window: the answer is the upstream failure, never a 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_trigger_switching_window_waits_then_dispatches() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    runtime.test_install(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    runtime.switch_gate.begin();
    // Land the cron route mid-window, then release — production
    // install order is routes-then-end.
    let rt = runtime.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        rt.test_install_cron(BTreeMap::from([(
            "w1".to_string(),
            vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
        )]));
        rt.switch_gate.end();
    });
    let app = vrt_host::cron_trigger::router_with_hold(
        registry,
        vrt_host::supervisor::SwitchHoldParams {
            budget: std::time::Duration::from_secs(2),
            max_reqs: 8,
            max_bytes: 1 << 20,
        },
    );
    let res = app
        .oneshot(mig11_req("w1:v1:s1:2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        502,
        "waited through the window then dispatched to the dead endpoint"
    );
}

/// MIG-11: the window outliving the hold budget answers 503
/// `VRT_CRON_SWITCHING` + `Retry-After` — PROVABLY un-delivered and
/// NOT dedupe-consumed, so the same key can retry inside the minute:
/// a second identical trigger is refused SWITCHING again (never
/// `duplicate:true`), and after the window it reaches dispatch (502
/// on the dead endpoint — still not a deduped replay).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_trigger_switching_503_preserves_idempotency_key() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    runtime.test_install(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    runtime.switch_gate.begin();
    let hold = vrt_host::supervisor::SwitchHoldParams {
        budget: std::time::Duration::from_millis(150),
        max_reqs: 8,
        max_bytes: 1 << 20,
    };
    let key = "w1:v1:s1:2026-01-01T00:00:00Z";
    for attempt in 0..2 {
        let res = vrt_host::cron_trigger::router_with_hold(registry.clone(), hold)
            .oneshot(mig11_req(key))
            .await
            .unwrap();
        assert_eq!(res.status(), 503, "attempt {attempt}");
        assert!(
            res.headers().get("retry-after").is_some(),
            "SWITCHING must carry Retry-After"
        );
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        assert_eq!(
            body["error"]["code"].as_str(),
            Some("VRT_CRON_SWITCHING"),
            "attempt {attempt}: {body}"
        );
        assert_ne!(
            body["duplicate"].as_bool(),
            Some(true),
            "a SWITCHING refusal must never consume the key: {body}"
        );
    }
    // Window closes with the cron route installed on the new gen —
    // the SAME key now dispatches (502 on the dead endpoint), proving
    // neither SWITCHING response burned the idempotency slot.
    runtime.test_install_cron(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    runtime.switch_gate.end();
    let res = vrt_host::cron_trigger::router_with_hold(registry, hold)
        .oneshot(mig11_req(key))
        .await
        .unwrap();
    assert_eq!(res.status(), 502, "retry inside the minute must dispatch");
}

/// MIG-11: gate cleared with routes still empty ⇒ genuine absence —
/// 404 `VRT_CRON_UNKNOWN_WORKER`, not a switch-window 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_trigger_genuine_absence_after_gate_is_404() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    runtime.test_install(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    runtime.switch_gate.begin();
    runtime.switch_gate.end(); // abandoned window (e.g. rollback)
    let app = vrt_host::cron_trigger::router(registry);
    let res = app
        .oneshot(mig11_req("w1:v1:s1:2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_CRON_UNKNOWN_WORKER"),
        "{body}"
    );
}

/// MIG-11-r2: gate end with NOTHING serving — the serialized-switch
/// launch failed after the old generation drained, so the tenant is
/// deployed but has no live generation. That is a retryable 503
/// `VRT_CRON_WORKER_NOT_READY` (the cell-controller retries inside
/// the minute), NEVER the fake-absence 404 that used to lose the
/// trigger outright.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cron_trigger_no_serving_after_gate_is_503_not_ready() {
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let runtime = registry.get_or_create("t");
    runtime.test_install(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    runtime.switch_gate.begin();
    // The serialized-switch launch FAILED: routes are gone and no
    // generation is serving (state still reads Ready — a failed
    // switch keeps it by the transition table).
    runtime.test_uninstall();
    runtime.switch_gate.end();
    let app = vrt_host::cron_trigger::router(registry);
    let res = app
        .oneshot(mig11_req("w1:v1:s1:2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(res.status(), 503);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("VRT_CRON_WORKER_NOT_READY"),
        "no serving generation after gate end → 503, not 404: {body}"
    );
}
