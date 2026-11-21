#![allow(dead_code)]

//! G1 Phase-1 exit-gate harness — real workerd, no probe-skip.
//!
//! Layer-attribution rule (adversarial): a denial counts as
//! egress-layer only when the real `vrt-egress` worker's own evidence
//! is observed — `x-vrt-egress-denied` carrying the typed code
//! (layer-3 policy) or `x-vrt-egress-error` (layer-1 `outbound-network`
//! dial rejection). A bare connection failure is never proof.

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use vrt_config::*;

pub fn workerd_bin() -> PathBuf {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if Path::new(&p).exists() => PathBuf::from(p),
        _ => panic!("G1 gate: VRT_WORKERD_BIN must point at a native workerd binary"),
    }
}

static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

pub fn gate_root(tag: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/tmp-g1r3")
        .join(format!(
            "{tag}-{}-{}",
            std::process::id(),
            RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub struct DirGuard(pub PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        if std::env::var("G1_KEEP_DIR").is_err() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// Ports handed out by this test process. A released :0 bind returns
/// the port to the kernel pool, and a LATER `bind(:0)` in the same
/// test can get it back — PoC7b hit exactly that: tenant A's workerd
/// had not bound yet when tenant B's free_port() re-issued A's port,
/// so B's workerd exited EADDRINUSE. Remembering allocations makes
/// repeat collisions inside this process impossible (cross-process
/// races remain — spawn callers retry early exits).
static ALLOC_PORTS: std::sync::Mutex<std::collections::BTreeSet<u16>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

pub fn free_port() -> u16 {
    for _ in 0..64 {
        let port = std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if ALLOC_PORTS.lock().unwrap().insert(port) {
            return port;
        }
    }
    panic!("free_port: 64 consecutive duplicate allocations");
}

pub struct WorkerProc {
    pub child: Child,
    pub log_path: PathBuf,
}
impl WorkerProc {
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}
impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

pub fn spawn(bin: &Path, cfg: &Path, dir: &Path, extra: &[String]) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    cmd.arg("serve")
        .arg("--external-addr=binding-backend=127.0.0.1:1")
        .arg(cfg)
        .current_dir(dir)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));
    for a in extra {
        cmd.arg(a);
    }
    let child = cmd.spawn().expect("spawn workerd");
    WorkerProc { child, log_path }
}

pub fn urlenc(s: &str) -> String {
    s.replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
        .replace('[', "%5B")
        .replace(']', "%5D")
        .replace('?', "%3F")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('"', "%22")
        .replace(' ', "%20")
}

/// Open loopback HTTP endpoint a tenant must never reach.
pub fn mini_http_spawn() -> (u16, std::thread::JoinHandle<()>) {
    let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = l.local_addr().unwrap().port();
    l.set_nonblocking(true).unwrap();
    let h = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(300);
        while Instant::now() < deadline {
            match l.accept() {
                Ok((mut s, _)) => {
                    use std::io::Write;
                    let _ = s.write_all(b"HTTP/1.0 200 OK\r\ncontent-length:7\r\n\r\nINT-HIT");
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    });
    (port, h)
}

/// Single HTTP/1.1 GET on a loopback port; returns (status, body).
/// Reads until the RESPONSE BODY IS COMPLETE — Content-Length,
/// chunked terminal, or (no-framing only) connection close — using
/// the shared `super::body_framing` rules. Never waits for EOF on a
/// framed response: workerd holds the socket ~5s after the reply
/// (keep-alive drain even under `connection: close`), so an EOF read
/// reports that drain as latency (G1-r4: the fake 5s PoC7 baseline).
/// `timeout` still bounds every read — a slow/no-response request can
/// never exceed it.
pub fn http_once(port: u16, path: &str, timeout: Duration) -> std::io::Result<(u16, String)> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    s.set_read_timeout(Some(timeout)).ok();
    s.set_write_timeout(Some(timeout)).ok();
    let req = format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(i) = super::find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        let mut tmp = [0u8; 8192];
        match s.read(&mut tmp) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "closed before response headers",
                ))
            }
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e) => return Err(e),
        }
    };
    let head_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let (head_only, _) = head_text.split_once("\r\n\r\n").unwrap_or((&head_text, ""));
    let mut header_map = std::collections::BTreeMap::new();
    for line in head_only.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            header_map.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let mut body_bytes = buf[header_end..].to_vec();
    match super::body_framing(&header_map) {
        super::BodyFraming::Fixed(len) => {
            while body_bytes.len() < len {
                let mut tmp = [0u8; 8192];
                match s.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => body_bytes.extend_from_slice(&tmp[..n]),
                    Err(e) => return Err(e),
                }
            }
            body_bytes.truncate(len);
        }
        super::BodyFraming::Chunked => {
            loop {
                if super::find_subslice(&body_bytes, b"0\r\n\r\n").is_some() {
                    break;
                }
                let mut tmp = [0u8; 8192];
                match s.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => body_bytes.extend_from_slice(&tmp[..n]),
                    Err(e) => return Err(e),
                }
            }
            body_bytes = super::decode_chunked(&body_bytes);
        }
        super::BodyFraming::CloseDelimited => {
            // No length/framing — only here do we read until close or
            // read-timeout (that IS the body delimiter).
            let mut tmp = [0u8; 8192];
            loop {
                match s.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => body_bytes.extend_from_slice(&tmp[..n]),
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut =>
                    {
                        break
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
    let status: u16 = head_only
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok((status, String::from_utf8_lossy(&body_bytes).to_string()))
}

pub fn http(port: u16, path: &str) -> (u16, String) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match http_once(port, path, Duration::from_secs(25)) {
            Ok(r) => return r,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "http {path} never connected: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Render a real tenant config for N workers through `vrt-config`
/// (same path as cron_smoke). `workers` = (id, name, module, bindings).
/// Returns (cfg_path, {worker_id: ingress_port}, extra_args, text).
pub fn render(
    dir: &Path,
    workers: &[(&str, &str, &str, serde_json::Value)],
    egress_policy: EgressPolicy,
) -> (PathBuf, BTreeMap<String, u16>, Vec<String>, String) {
    let mut bundles = Vec::new();
    let mut worker_json = Vec::new();
    let mut ingress_ports = BTreeMap::new();
    let mut has_do = false;
    for (id, name, module, bindings) in workers {
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
        if bindings.to_string().contains("durableObject") {
            has_do = true;
        }
        worker_json.push(json!({
            "workerId": id, "name": name,
            "activeVersion": {
                "versionId": format!("v-{id}-1"),
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            },
            "bindings": bindings,
            "routes": [{"hostname": format!("{id}--team-gate.vibeploy.app")}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }));
        ingress_ports.insert(id.to_string(), free_port());
        bundles.push(bundle);
    }
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-gate", "workers": worker_json
    }))
    .unwrap();
    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&manifest_raw).unwrap();
    let params = RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy,
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "g1-gate-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &bundles.iter().cloned().collect::<Vec<_>>(),
        params: &params,
    })
    .unwrap();
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
                std::fs::write(
                    &dst,
                    vrt_config::workflows::vendored_bytes(name)
                        .unwrap_or_else(|| panic!("vendored module {name}")),
                )
                .unwrap();
            }
        }
    }
    // Unix sockets → TCP (sandbox forbids unix bind; layer-1 unix
    // denial is asserted separately on the rendered text).
    let mut text = out.text.clone();
    for (id, ..) in workers {
        text = text.replace(
            &format!("unix:sock/{id}.sock"),
            &format!("127.0.0.1:{}", ingress_ports[*id]),
        );
    }
    let cfg = dir.join("cfg.capnp");
    std::fs::write(&cfg, &text).unwrap();
    let args = vrt_host::assemble::experimental_extra_args(
        &params.system_compat_flags,
        vrt_host::assemble::ExperimentalNeeds {
            durable_objects_or_workflows: has_do,
            ..Default::default()
        },
    );
    (cfg, ingress_ports, args, text)
}

/// Tenant probe module: BATCHES targets inside the worker — one
/// host→workerd roundtrip per matrix (egress denials are per-target,
/// in-worker, ~ms each). Returns `{"results":[{t,...}]}`.
pub const MOD_PROBE: &str = r#"
import { connect } from "cloudflare:sockets";
async function probeFetch(t) {
  try {
    const r = await fetch(t, { signal: AbortSignal.timeout(4000) });
    const body = await r.text();
    return { t, status: r.status,
      denied: r.headers.get("x-vrt-egress-denied"),
      errHdr: r.headers.get("x-vrt-egress-error"),
      body: body.slice(0, 200) };
  } catch (e) { return { t, threw: true, err: String(e).slice(0, 200) }; }
}
async function probeConnect(t) {
  try {
    const s = connect(t);
    await s.opened;
    const w = s.writable.getWriter();
    await w.write(new TextEncoder().encode("GET / HTTP/1.0\r\nhost: x\r\n\r\n"));
    const r = s.readable.getReader();
    const res = await Promise.race([
      r.read(),
      new Promise(rr => setTimeout(() => rr({ timeout: true }), 800)),
    ]);
    let data = "";
    if (res && res.value) data = new TextDecoder().decode(res.value).slice(0, 120);
    try { s.close(); } catch {}
    return { t, opened: true, data, timeout: !!res.timeout };
  } catch (e) { return { t, opened: false, err: String(e).slice(0, 160) }; }
}
export default {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/batch") {
      const kind = u.searchParams.get("kind") ?? "fetch";
      const ts = (u.searchParams.get("ts") ?? "").split("\u001f");
      const results = [];
      for (const t of ts) {
        results.push(kind === "connect" ? await probeConnect(t) : await probeFetch(t));
      }
      return Response.json({ results });
    }
    return new Response("probe");
  },
};
"#;

/// phys_footprint of `pid` (macOS proc_pid_rusage) / VmRSS (linux).
pub fn rss_bytes(pid: u32) -> u64 {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut info: libc::rusage_info_v4 = std::mem::zeroed();
        let r = libc::proc_pid_rusage(
            pid as i32,
            libc::RUSAGE_INFO_V4,
            &mut info as *mut _ as *mut _,
        );
        if r == 0 {
            return info.ri_phys_footprint;
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
                    * 1024;
            }
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        0
    }
}

/// Like `spawn` but binding-backend points at a REAL provider port.
pub fn spawn_backend(
    bin: &Path,
    cfg: &Path,
    dir: &Path,
    prov_port: u16,
    extra: &[String],
) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    cmd.arg("serve")
        .arg(format!(
            "--external-addr=binding-backend=127.0.0.1:{prov_port}"
        ))
        .arg(cfg)
        .current_dir(dir)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));
    for a in extra {
        cmd.arg(a);
    }
    let child = cmd.spawn().expect("spawn workerd");
    WorkerProc { child, log_path }
}
