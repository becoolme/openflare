//! MIG-16: real-workerd proof of the Workers Rate Limiting binding.
//!
//! `RATELIMIT` bindings render as service bindings to the
//! `facade-ratelimit` `RateLimiter` entrypoint with host-injected props.
//! Proves on the real binary: fixed-window counting, per-key isolation,
//! per-binding (namespace_id) isolation, key validation, and per-worker
//! scope isolation for the same namespace_id.
//!
//! Not exercised here: cross-tenant (`instanceId`) isolation — the render
//! path takes one tenant per config; that part of the scope is covered by
//! facade/test/ratelimit.test.mjs.
//!
//! Gated on `VRT_WORKERD_BIN` and `VRT_HOST_UNIX_TESTS=1` — absent →
//! explicit skip, never silently green:
//!   VRT_HOST_UNIX_TESTS=1 VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test m16_ratelimit -- --include-ignored --nocapture

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry,
    EgressPolicy, EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => None,
    }
}

fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn smoke_root() -> Option<PathBuf> {
    match vrt_securefs::test_short_root("vrt-m16") {
        Ok(d) => Some(d),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            None
        }
        Err(e) => panic!("short root: {e}"),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct WorkerProc {
    child: Child,
    log_path: PathBuf,
    dir: PathBuf,
}

impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::env::var("VRT_SMOKE_KEEP").is_ok() {
            eprintln!("kept smoke dir: {}", self.dir.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let child = Command::new(bin)
        .arg("serve")
        .arg(cfg)
        .arg("--experimental")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    WorkerProc {
        child,
        log_path,
        dir: dir.to_path_buf(),
    }
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}

fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    write!(
        s,
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if response_complete(&buf) {
                    break;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn response_complete(buf: &[u8]) -> bool {
    let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4) else {
        return false;
    };
    let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_lowercase();
    let body_len = buf.len() - hdr_end;
    for line in headers.split("\r\n") {
        if let Some(v) = line.strip_prefix("content-length:") {
            return v.trim().parse::<usize>().map_or(false, |n| body_len >= n);
        }
        if line.starts_with("transfer-encoding:") && line.contains("chunked") {
            return buf[hdr_end..].windows(5).any(|w| w == b"0\r\n\r\n");
        }
    }
    false
}

fn log_text(proc: &WorkerProc) -> String {
    let raw = std::fs::read(&proc.log_path).unwrap_or_default();
    // workerd wraps each console line as JSON, escaping the quotes of
    // our structured log; unescape so assertions match the raw event.
    String::from_utf8_lossy(&raw).replace('\0', "").replace("\\\"", "\"")
}

/// A plain TCP echo server on 0.0.0.0 — reachable via the pod's LAN
/// IP (what the internal exception names) AND via workerd's dial from

const USER_MODULE: &str = r#"export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const b = env[u.searchParams.get("b")];
    const mode = u.searchParams.get("m") || "s";
    let key = u.searchParams.get("k") ?? "";
    if (mode === "n") key = 42;
    if (mode === "o") key = { a: 1 };
    if (mode === "l") key = "x".repeat(Number(u.searchParams.get("len") || "2000"));
    try {
      const r = await b.limit({ key });
      return Response.json({ success: r.success });
    } catch (e) {
      return Response.json({ error: String((e && e.name) || "Error"), msg: String(e && e.message) });
    }
  }
}
"#;

const BACKEND_STUB: &str = r#"export default {
  async fetch() { return new Response("m16-stub", { status: 404 }); }
}
"#;

fn smoke_params() -> RuntimeParams {
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&raw).expect("facade manifest parses");
    RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::<EgressAllowEntry>::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 30_000,
            tcp_allowed_ports: Vec::new(),
            internal_tcp_allow: Vec::new(),
            internal_tcp_targets: Vec::new(),
            tcp_connect_timeout_ms: 10_000,
            tcp_idle_timeout_ms: 2_000,
            tcp_max_concurrent: 8,
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        disk_services: vrt_config::resolve_disk_paths(&facade_root(), &manifest.disk_services).unwrap(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: String::new(),
        wfop_pepper: "m16-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

/// One tenant, two workers (`wm16a`, `wm16b`) sharing one bundle. Both
/// declare `RL_A` (ns 1001, 3/60) and `RL_B` (ns 1002, 3/60).
fn render_and_write(dir: &Path, port_a: u16, port_b: u16) -> PathBuf {
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": USER_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let worker = |id: &str, host: &str| {
        json!({
            "workerId": id,
            "name": id,
            "activeVersion": {
                "versionId": format!("v-{id}-1"),
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [
                {"type": "ratelimit", "name": "RL_A", "namespaceId": "1001", "limit": 3, "period": 60},
                {"type": "ratelimit", "name": "RL_B", "namespaceId": "1002", "limit": 3, "period": 60}
            ],
            "routes": [{"hostname": host}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        })
    };
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-m16",
        "workers": [
            worker("wm16a", "a--team-m16.vibeploy.app"),
            worker("wm16b", "b--team-m16.vibeploy.app")
        ]
    }))
    .unwrap();
    let params = smoke_params();
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

    // Materialize every embed the renderer demands (system modules from
    // dist, the tenant bundle verbatim).
    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
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
                    .unwrap_or_else(|| panic!("{service_name} missing from manifest"));
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap_or_else(|| panic!("{service_name}/{module_name} not in manifest"));
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes =
                    std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                let got = format!("{:x}", Sha256::digest(&bytes));
                assert_eq!(got, embed.digest, "embed digest drift — regen facade/dist");
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, USER_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }


    let mut text = out.text.clone();
    for (id, port) in [("wm16a", port_a), ("wm16b", port_b)] {
        let unix_addr = format!("unix:sock/{id}.sock");
        assert!(
            text.contains(&unix_addr),
            "expected unix socket addr in render:\n{text}"
        );
        text = text.replace(&unix_addr, &format!("127.0.0.1:{port}"));
    }
    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(
        text.contains(external),
        "expected binding-backend external in render:\n{text}"
    );
    std::fs::write(dir.join("backend-stub.js"), BACKEND_STUB).unwrap();
    text = text.replace(
        external,
        "(name = \"binding-backend\", worker = (\n            \
           modules = [(name = \"backend.js\", \
           esModule = embed \"backend-stub.js\")],\n            \
           compatibilityDate = \"2026-09-08\",\n            \
           globalOutbound = (name = \"vrt-no-network\", \
           entrypoint = \"DenyAll\"),\n        ))",
    );
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

fn hit(port: u16, binding: &str, key: &str) -> serde_json::Value {
    let (st, body) = http_get(port, &format!("/?b={binding}&k={key}"));
    assert_eq!(st, 200, "hit status: {body}");
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("bad json {body:?}: {e}"))
}

fn success(v: &serde_json::Value) -> bool {
    v["success"]
        .as_bool()
        .unwrap_or_else(|| panic!("no success field: {v}"))
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN pointing at a real workerd binary"]
fn mig16_ratelimit_end_to_end() {
    if std::env::var("VRT_HOST_UNIX_TESTS").ok().as_deref() != Some("1") {
        eprintln!("SKIP: VRT_HOST_UNIX_TESTS!=1");
        return;
    }
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real-workerd test not run");
        return;
    };
    let Some(dir) = smoke_root() else {
        return;
    };
    let (pa, pb) = (free_port(), free_port());
    let cfg = render_and_write(&dir, pa, pb);
    let proc = spawn(&bin, &cfg, &dir);
    if !wait_ready(pa) || !wait_ready(pb) {
        panic!("workerd never listened\n-- log --\n{}", log_text(&proc));
    }

    // 1. limit=3: first three succeed, the rest are rejected.
    let seq: Vec<bool> = (0..5).map(|_| success(&hit(pa, "RL_A", "k1"))).collect();
    assert_eq!(seq, [true, true, true, false, false], "log:\n{}", log_text(&proc));

    // 2. A different key counts independently.
    assert!(success(&hit(pa, "RL_A", "k2")), "fresh key must pass");

    // 3. Another binding (other namespace_id) is unaffected by RL_A's k1.
    for i in 0..3 {
        assert!(success(&hit(pa, "RL_B", "k1")), "RL_B k1 hit {i}");
    }
    assert!(!success(&hit(pa, "RL_B", "k1")), "RL_B exhausts on its own");

    // 4. Same namespace_id + same key on a DIFFERENT worker: isolated scope.
    for i in 0..3 {
        assert!(success(&hit(pb, "RL_A", "k1")), "worker B RL_A k1 hit {i}");
    }
    assert!(!success(&hit(pb, "RL_A", "k1")));

    // 5. Key validation: non-string and oversized keys throw to the tenant.
    for (m, extra) in [("n", ""), ("o", ""), ("l", "&len=2000"), ("l", "&len=1025")] {
        let (st, body) = http_get(pa, &format!("/?b=RL_A&m={m}{extra}"));
        assert_eq!(st, 200, "{body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v.get("success").is_none(), "invalid key must not succeed: {v}");
        assert_eq!(v["error"], "TypeError", "mode {m}{extra}: {v}");
    }
    // Boundary: exactly 1024 bytes is accepted.
    let (st, body) = http_get(pa, "/?b=RL_A&m=l&len=1024");
    assert_eq!(st, 200);
    assert!(body.contains("\"success\":true"), "1024-byte key ok: {body}");
}
