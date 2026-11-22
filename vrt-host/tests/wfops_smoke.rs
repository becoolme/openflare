//! W3: real-workerd wfops facade smoke — the platform
//! `facade-wfops-<workerId>` service (unix socket
//! `sock-wfops-<id>`) is exercised over a TCP-substituted socket
//! exactly the way vrt-host `/v1/wfops` forwards to it:
//! `POST /vrt/wfops` + `x-vrt-wfop-token` header.
//!
//! Covers the required W3 chain on REAL workerd + vendored engine:
//!   create → get(running) → pause(paused) → resume(running)
//!          → terminate(terminated) → get(terminated)
//! plus the trust-boundary negatives: wrong token → 403, undeclared
//! workflow → 404, bad action → 400, GET → 405, malformed body → 400,
//! and a token derived from a DIFFERENT uniqueKey → 403 (cross-worker
//! forgery fails closed).
//!
//! Gated on `VRT_WORKERD_BIN` like the other smokes:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test wfops_smoke -- --nocapture

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use vrt_config::*;

const UNIQUE_KEY: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) => Some(PathBuf::from(p)),
        Err(_) => {
            eprintln!(
                "skip: set VRT_WORKERD_BIN=/path/to/workerd \
                 (or deploy/standalone/workerd)"
            );
            None
        }
    }
}

fn smoke_root() -> PathBuf {
    // Inside the workspace target dir (W3 tmp policy): the sandbox
    // denies unix-socket binds outside the workspace, and `target/` is
    // the repo's designated scratch space — same `target/tmp-*`
    // convention the other smokes use.
    // NB: join via parent() — a literal ".." segment in the path
    // would fail the renderer's `valid_path` traversal check on
    // `doStorageDir`.
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/tmp-wf-smoke")
        .join(format!(
            "run-{}-{}",
            std::process::id(),
            RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// W3-r4: removes the run dir on success AND panic.
struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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

/// Workflow that parks in a long `step.sleep` — the instance stays
/// `running` long enough for pause/resume/terminate to land
/// deterministically (no race with natural completion).
const USER_MODULE: &str = r#"
import { WorkflowEntrypoint } from "cloudflare:workers";

export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    const marker = event && event.payload && event.payload.marker;
    await step.do("first", async () => "a:" + String(marker));
    await step.sleep("hold", "5 minutes");
    return "unreached-after-terminate";
  }
}

export default {
  async fetch() { return new Response("nf", { status: 404 }); }
};
"#;

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join(format!("workerd-{}.log", std::process::id()));
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    cmd.arg("serve")
        .arg("--external-addr=binding-backend=127.0.0.1:1")
        .arg(cfg)
        .arg("--experimental")
        .current_dir(dir);
    let child = cmd
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    WorkerProc { child, log_path }
}

fn tcp_conn(port: u16) -> std::net::TcpStream {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let s = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(5))
        .unwrap_or_else(|e| panic!("tcp connect :{port}: {e}"));
    s.set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .unwrap();
    s
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    false
}

/// POST the facade contract exactly as vrt-host `/v1/wfops` does:
/// origin-form + Host + `x-vrt-wfop-token`. HTTP/1.0 keeps the
/// response close-delimited (workerd holds keep-alive conns ~5s).
fn wfop(port: u16, token: &str, body: &Value) -> (u16, Value) {
    let payload = body.to_string();
    let mut s = tcp_conn(port);
    write!(
        s,
        "POST /vrt/wfops HTTP/1.0\r\nhost: x\r\ncontent-type: application/json\r\nx-vrt-wfop-token: {token}\r\ncontent-length: {}\r\n\r\n{payload}",
        payload.len()
    )
    .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let raw = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    let v: Value =
        serde_json::from_str(&raw).unwrap_or_else(|_| panic!("facade non-json {status}: {raw}"));
    (status, v)
}

/// Test pepper — the smoke derives expected tokens with
/// `vrt_config::workflows::wfop_token` against this value, exactly
/// the derivation the render applied into `WFOP_TABLE`.
const WFOP_PEPPER: &str = "wfops-smoke-pepper-0123456789abcdef";

fn token_for(unique_key: &str) -> String {
    vrt_config::workflows::wfop_token(WFOP_PEPPER, unique_key)
}

fn render_write(dir: &Path, wf_port: u16, ingress_port: u16) -> PathBuf {
    let worker_id = "wsmokeop";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": USER_MODULE },
            "compatibilityDate": "2025-01-01",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-ops",
        "workers": [{
            "workerId": worker_id,
            "name": "wf-ops",
            "activeVersion": {
                "versionId": "v-ops-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": "wf--team-ops.vibeploy.app"}],
            "crons": [],
            "workflows": [{
                "name": "order-flow",
                "binding": "WF",
                "className": "OrderFlow",
                "uniqueKey": UNIQUE_KEY
            }],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
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
        wfop_pepper: WFOP_PEPPER.into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle.clone()],
        params: &params,
    })
    .unwrap();

    // Invariants on the REAL render: wfops service + socket + the
    // token never lands in the tenant worker's env.
    assert!(out.text.contains("facade-wfops-wsmokeop"), "{}", out.text);
    assert!(out.text.contains("wsmokeop-wfops.sock"), "{}", out.text);
    assert!(out.text.contains("WFOP_TABLE"), "{}", out.text);

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
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, USER_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                std::fs::write(&dst, vrt_config::workflows::vendored_bytes(name).unwrap()).unwrap();
            }
        }
    }

    // workerd binds sockets relative to ITS cwd; the dirs must exist
    // (vrt-host's assemble creates them on the real path).
    std::fs::create_dir_all(dir.join("sock")).unwrap();
    std::fs::create_dir_all(dir.join("private")).unwrap();
    // ALL unix sockets → TCP: the wfops socket so the test can drive
    // it exactly as vrt-host's `/v1/wfops` proxy does, and the tenant
    // ingress socket because sandboxed test runs cannot bind unix
    // sockets at all (the ingress is never called either way).
    let text = out
        .text
        .replace(
            &format!("unix:sock/{worker_id}-wfops.sock"),
            &format!("127.0.0.1:{wf_port}"),
        )
        .replace(
            &format!("unix:sock/{worker_id}.sock"),
            &format!("127.0.0.1:{ingress_port}"),
        );
    std::fs::create_dir_all(dir.join("do").join(worker_id)).unwrap();
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

/// Poll `get` until `want` status (engine transitions are async:
/// create→queued→running, pause→pausing→paused).
fn wait_status(port: u16, token: &str, id: &str, want: &str, timeout_s: u64, log: &Path) -> Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_s);
    loop {
        let (code, v) = wfop(
            port,
            token,
            &json!({"op":"get","workflow":"order-flow","instanceId":id}),
        );
        assert_eq!(code, 200, "get {id}: {v}");
        if v["status"].as_str() == Some(want) {
            return v;
        }
        if v["status"].as_str() == Some("errored") {
            panic!(
                "instance {id} errored: {v}\nlog tail:\n{}",
                std::fs::read_to_string(log)
                    .unwrap_or_default()
                    .lines()
                    .rev()
                    .take(30)
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "{id} never reached {want} in {timeout_s}s (last {v})\nlog tail:\n{}",
                std::fs::read_to_string(log)
                    .unwrap_or_default()
                    .lines()
                    .rev()
                    .take(30)
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
}

#[test]
fn real_workerd_wfops_lifecycle() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let wf_port = free_port();
    let cfg = render_write(&dir, wf_port, free_port());
    let proc = spawn(&bin, &cfg, &dir);
    assert!(
        wait_ready(wf_port),
        "workerd ready: {}",
        std::fs::read_to_string(&proc.log_path).unwrap_or_default()
    );
    let token = token_for(UNIQUE_KEY);

    // --- trust-boundary negatives BEFORE any success path ---
    // Wrong token → 403 (tenant forging a platform call).
    let (code, v) = wfop(
        wf_port,
        "deadbeef",
        &json!({"op":"get","workflow":"order-flow","instanceId":"x"}),
    );
    assert_eq!((code, v["error"].as_str()), (403, Some("forbidden")), "{v}");
    // Token derived from a DIFFERENT uniqueKey → 403 (cross-worker).
    let foreign = token_for(&"f".repeat(64));
    let (code, v) = wfop(
        wf_port,
        &foreign,
        &json!({"op":"get","workflow":"order-flow","instanceId":"x"}),
    );
    assert_eq!((code, v["error"].as_str()), (403, Some("forbidden")), "{v}");
    // Undeclared workflow name (even with the real token) → 404.
    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({"op":"get","workflow":"no-such-wf","instanceId":"x"}),
    );
    assert_eq!(
        (code, v["error"].as_str()),
        (404, Some("workflow.undeclared")),
        "{v}"
    );
    // Bad action verb → 400.
    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({"op":"action","workflow":"order-flow","instanceId":"x","action":"explode"}),
    );
    assert_eq!(
        (code, v["error"].as_str()),
        (400, Some("action.invalid")),
        "{v}"
    );

    // --- required W3 chain: create → pause → resume → terminate → status ---
    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({
            "op": "create",
            "workflow": "order-flow",
            "instanceId": "inst-w3-1",
            "params": {"marker": "w3"}
        }),
    );
    assert_eq!(code, 200, "create: {v}");
    assert_eq!(v["instanceId"].as_str(), Some("inst-w3-1"), "{v}");
    // W3-r4: create is durable-confirmed — the facade reports the
    // engine's COMMITTED status, never the pre-commit "queued" reply.
    // A fresh instance's first committed status is "running"
    // (setStatus(Running) lands before the workflow's first step).
    assert_eq!(v["status"].as_str(), Some("running"), "{v}");

    wait_status(wf_port, &token, "inst-w3-1", "running", 30, &proc.log_path);

    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({"op":"action","workflow":"order-flow","instanceId":"inst-w3-1","action":"pause"}),
    );
    assert_eq!(code, 200, "pause: {v}");
    wait_status(wf_port, &token, "inst-w3-1", "paused", 20, &proc.log_path);

    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({"op":"action","workflow":"order-flow","instanceId":"inst-w3-1","action":"resume"}),
    );
    assert_eq!(code, 200, "resume: {v}");
    wait_status(wf_port, &token, "inst-w3-1", "running", 20, &proc.log_path);

    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({"op":"action","workflow":"order-flow","instanceId":"inst-w3-1","action":"terminate"}),
    );
    assert_eq!(code, 200, "terminate: {v}");
    let term = wait_status(
        wf_port,
        &token,
        "inst-w3-1",
        "terminated",
        20,
        &proc.log_path,
    );
    assert_eq!(term["status"].as_str(), Some("terminated"), "{term}");

    // Engine `create({id})` is get-or-create: a terminated instance
    // stays terminal — `initInner` early-returns on the committed
    // Terminated status, so the truthful response is "terminated"
    // (W3-r4: the facade reports the engine's committed status —
    // pre-W3-r4 this pinned the fake hardcoded "queued"). Idempotency
    // against ACCIDENTAL double-dispatch lives in the control-plane
    // dedupKey (`vrt_cell_ops` unique index), not here.
    let (code, v) = wfop(
        wf_port,
        &token,
        &json!({"op":"create","workflow":"order-flow","instanceId":"inst-w3-1"}),
    );
    assert_eq!(
        (code, v["status"].as_str()),
        (200, Some("terminated")),
        "engine create(id) is idempotent get-or-recreate: {v}"
    );
}
