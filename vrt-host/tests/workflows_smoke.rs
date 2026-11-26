//! W1: real-workerd Workflows smoke — the vendored Cloudflare Engine
//! runs as a Durable Object inside the tenant workerd, the wrapped
//! `env.WF` binding drives `create/get/status`, a failing step
//! exercises engine retry, `step.sleep` survives a workerd restart
//! (engine state lives in localDisk SQLite — durable across spawns).
//!
//! Gated on `VRT_WORKERD_BIN` like the other smokes:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test workflows_smoke -- --nocapture

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use vrt_config::*;

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
    // W3-r4: `target/tmp-wf-smoke/` — a fresh child dir per run
    // (pid + counter: tests in one binary share the pid) so no run
    // can inherit another's DO storage. `parent().join()` — a `..`
    // segment would trip `valid_path` during render.
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

/// W3-r4: removes the run dir on success AND panic — a leftover dir
/// pins DO storage a later run must not see.
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
    // tests live in vrt-host/tests — facade is a sibling crate dir
    // (same convention as workerd_entry_smoke.rs / d1_smoke.rs).
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// User worker: `OrderFlow` exports a `WorkflowEntrypoint` subclass
/// (same-worker W1 requirement). Step "boom" throws on its first
/// attempt to exercise the engine's persisted retry; `step.sleep`
/// bridges a workerd restart (the Engine's alarm + SQLite state live
/// in the worker's localDisk dir).
const USER_MODULE: &str = r#"
import { WorkflowEntrypoint } from "cloudflare:workers";

let boomAttempts = 0;

export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    const p = (event && event.payload) || {};
    // W6: waitForEvent — resolves with the event delivered by
    // sendEvent; the payload lands in the output for assertion.
    if (p.wait === "event") {
      const ev = await step.waitForEvent("wait-approval", {
        type: "approval",
        timeout: "30 seconds",
      });
      return `got-${ev.payload.v}`;
    }
    // W6: waitForEvent timeout path — engine throws
    // WorkflowTimeoutError; catch and surface a marker output.
    if (p.wait === "timeout") {
      try {
        await step.waitForEvent("wait-never", {
          type: "never",
          timeout: "1 second",
        });
        return "unreached";
      } catch (_e) {
        return "timed-out";
      }
    }
    const a = await step.do("first", async () => "a");
    const b = await step.do(
      "boom",
      { retries: { limit: 3, delay: "50 milliseconds", backoff: "constant" } },
      async () => {
        boomAttempts += 1;
        if (boomAttempts === 1) throw new Error("boom-once");
        return "b";
      },
    );
    // W6: a longer sleep window gives pause an observable hold —
    // the engine parks at the step boundary when the alarm fires.
    if (p.slow) {
      await step.sleep("window", "10 seconds");
    } else {
      await step.sleep("pause", "2 seconds");
    }
    return `${a}-${b}-done`;
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    try {
      if (url.pathname === "/create") {
        const opts = {};
        const id = url.searchParams.get("id");
        const params = url.searchParams.get("params");
        if (id) opts.id = id;
        if (params) opts.params = JSON.parse(params);
        const inst = await env.WF.create(opts);
        return Response.json({ id: inst.id });
      }
      if (url.pathname === "/status") {
        const inst = await env.WF.get(url.searchParams.get("id"));
        return Response.json(await inst.status());
      }
      // W6: binding-level actions — pause/resume/restart/terminate/
      // delete — exercised through the SAME wrapped binding a tenant
      // would call.
      if (url.pathname === "/action") {
        const inst = await env.WF.get(url.searchParams.get("id"));
        const op = url.searchParams.get("op");
        await inst[op]();
        return Response.json({ ok: true });
      }
      // W6: sendEvent through the tenant binding.
      if (url.pathname === "/event") {
        const inst = await env.WF.get(url.searchParams.get("id"));
        const payload = url.searchParams.get("payload");
        await inst.sendEvent({
          type: url.searchParams.get("type"),
          payload: payload ? JSON.parse(payload) : undefined,
        });
        return Response.json({ ok: true });
      }
      // W6: tenant-side reachability probe — the egress gate must
      // refuse outbound fetches to the wfdel control port. Returns
      // ok:false + the denial text (never throws the request).
      if (url.pathname === "/probe") {
        const port = url.searchParams.get("port");
        try {
          const r = await fetch(
            `http://127.0.0.1:${port}/core/workflow-storage/x`,
          );
          return Response.json({ reached: true, status: r.status });
        } catch (e) {
          return Response.json({
            reached: false,
            err: String(e && e.message ? e.message : e).slice(0, 200),
          });
        }
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      const msg = String(e && e.message ? e.message : e);
      const code = msg.includes("instance.not_found") ? 404 : 500;
      return Response.json({ error: msg.slice(0, 300) }, { status: code });
    }
  }
};
"#;

fn spawn(bin: &Path, cfg: &Path, dir: &Path, wfdel_port: Option<u16>) -> WorkerProc {
    let log_path = dir.join(format!("workerd-{}.log", std::process::id()));
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    // `binding-backend` external service is declared by the facade
    // manifest; nothing in this smoke calls it (no d1/kv bindings),
    // but workerd requires every external service to have an address.
    cmd.arg("serve")
        .arg("--external-addr=binding-backend=127.0.0.1:1");
    if let Some(p) = wfdel_port {
        // W6: `vrt-wfdel` external → the loopback shim forwards
        // engine storage deletes to this control listener.
        cmd.arg(format!("--external-addr=vrt-wfdel=127.0.0.1:{p}"));
    }
    cmd.arg(cfg).arg("--experimental");
    let child = cmd
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    WorkerProc { child, log_path }
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}

/// One HTTP attempt. Returns Err on connect/timeout/IO failure —
/// instrumented so a slow-path flake names itself (W3-r3 diagnosis:
/// engine cold-init / post-restart resume can legitimately take tens
/// of seconds under CPU pressure; a fixed 5s read deadline conflated
/// "hung" with "initialising").
fn http_once(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    let start = std::time::Instant::now();
    let r = http_once_inner(port, path);
    match &r {
        Ok((code, _)) => {
            eprintln!("[http] {path} -> {code} in {:?}", start.elapsed())
        }
        Err(e) => eprintln!("[http] {path} -> ERR {e} in {:?}", start.elapsed()),
    }
    r
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn http_once_inner(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
    // Per-attempt read deadline — a hang guard only. NOT the timing
    // mechanism: reads stop at body-complete (below), so normal
    // requests resolve in milliseconds even under load.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
    )?;

    // W3-r3 root cause of the flake: `read_to_end` waited for EOF,
    // but workerd drains keep-alive ~5s AFTER the response (see
    // tests/common::http_get — same diagnosis, W2). Every request sat
    // at the 5s deadline; load pushed EOF past it → WouldBlock.
    // Read to body-complete instead: headers, then Content-Length /
    // chunked-terminal — never EOF.
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "closed before response headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let (head_only, _) = head_text.split_once("\r\n\r\n").unwrap_or((&head_text, ""));
    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in head_only.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            match k.trim().to_ascii_lowercase().as_str() {
                "content-length" => content_length = v.trim().parse().ok(),
                "transfer-encoding" => chunked = v.to_ascii_lowercase().contains("chunked"),
                _ => {}
            }
        }
    }
    let mut body_bytes = buf[header_end..].to_vec();
    if let Some(len) = content_length {
        while body_bytes.len() < len {
            let mut chunk = [0u8; 8192];
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "closed mid-body",
                ));
            }
            body_bytes.extend_from_slice(&chunk[..n]);
        }
        body_bytes.truncate(len);
    } else if chunked {
        loop {
            if find_subslice(&body_bytes, b"0\r\n\r\n").is_some() {
                break;
            }
            let mut chunk = [0u8; 8192];
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "closed mid-chunked-body",
                ));
            }
            body_bytes.extend_from_slice(&chunk[..n]);
        }
    } else {
        // No framing — close-delimited: EOF is the terminator here.
        stream.read_to_end(&mut body_bytes)?;
    }

    let code: u16 = head_only
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    // Chunked framing: strip the terminal marker before returning
    // (callers only inspect the JSON payload prefix).
    let body = String::from_utf8_lossy(&body_bytes).to_string();
    let body = body.split("\r\n0\r\n\r\n").next().unwrap_or(&body);
    Ok((code, body.trim().to_string()))
}

/// Bounded poll for transport failures (connect refused during
/// workerd bring-up, a torn connection under load): retry every
/// 200ms for up to 60s. It is NOT a latency fix — body-complete
/// reads make every request ms-fast; this only covers the window
/// where the socket itself is not yet accepting.
fn http_deadline(port: u16, path: &str, deadline: std::time::Duration) -> (u16, String) {
    let start = std::time::Instant::now();
    loop {
        match http_once(port, path) {
            Ok(r) => return r,
            Err(e) => {
                if start.elapsed() > deadline {
                    panic!("http {path} failed after {:?}: {e}", start.elapsed());
                }
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        }
    }
}

fn http(port: u16, path: &str) -> (u16, String) {
    http_deadline(port, path, std::time::Duration::from_secs(60))
}

fn render_write(dir: &Path, port: u16, wfops_port: u16, wfdel_port: Option<u16>) -> PathBuf {
    let worker_id = "wsmokewf";
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
        "tenantId": "team-smoke",
        "workers": [{
            "workerId": worker_id,
            "name": "wf-smoke",
            "activeVersion": {
                "versionId": "v-smoke-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": "wf--team-smoke.vibeploy.app"}],
            "crons": [],
            "workflows": [{
                "name": "order-flow",
                "binding": "WF",
                "className": "OrderFlow",
                "uniqueKey": "c".repeat(64)
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
        disk_services: vrt_config::resolve_disk_paths(&facade_root(), &manifest.disk_services).unwrap(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        // Absolute path inside the test tempdir — workerd resolves
        // `localDisk` relative to ITS cwd, not the config dir
        // (d1_smoke.rs uses the same absolute pattern).
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "wf-smoke-pepper".into(),
        // W6: Some → the `vrt-wfdel` external emits and the shim
        // forwards deletes here; None → degraded explicit-501 shim.
        wf_delete_addr: wfdel_port
            .map(|p| format!("127.0.0.1:{p}"))
            .unwrap_or_default(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle.clone()],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

    // Emit invariants on the REAL text: engine + wrapped + loopback.
    assert!(
        out.text.contains("wf-engine-wsmokewf-order-flow"),
        "{}",
        out.text
    );
    assert!(out.text.contains("preventEviction = true"), "{}", out.text);
    assert!(out.text.contains("vrt-wf-loopback"), "{}", out.text);
    assert!(out.text.contains("extensions = ["), "{}", out.text);
    // W6 unroutability invariant: the loopback shim + the `vrt-wfdel`
    // external are INTERNAL services — no socket may route to them
    // (sockets are the only ingress-reachable surface).
    for line in out.text.lines() {
        if line.contains("service =") && line.contains("http = ()") {
            assert!(
                !line.contains("vrt-wf-loopback"),
                "loopback routable: {line}"
            );
            assert!(!line.contains("vrt-wfdel"), "wfdel routable: {line}");
        }
    }
    if wfdel_port.is_some() {
        assert!(out.text.contains("vrt-wfdel"), "{}", out.text);
    } else {
        assert!(!out.text.contains("vrt-wfdel"), "{}", out.text);
    }

    // Materialize embeds (vendored + bundle + system).
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

    // Unix sockets → TCP (sandbox forbids unix bind); each socket gets
    // its own pre-allocated port. The wfops socket is the W3 endpoint —
    // without this replacement the tenant workerd dies at bind, which
    // is the same cascade the standalone e2e hit.
    let mut text = out.text.clone();
    text = text.replace(
        &format!("unix:sock/{worker_id}-wfops.sock"),
        &format!("127.0.0.1:{wfops_port}"),
    );
    text = text.replace(
        &format!("unix:sock/{worker_id}.sock"),
        &format!("127.0.0.1:{port}"),
    );
    // workerd's localDisk DiskDirectory requires the per-worker dir to
    // already exist (assemble creates it on the real path).
    std::fs::create_dir_all(dir.join("do").join(worker_id)).unwrap();
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

/// Poll instance status with a hard deadline; on timeout or error
/// state, dump the tail of the workerd log so the failure names
/// itself instead of hanging or asserting blind.
fn wait_status(
    port: u16,
    id: &str,
    want: &str,
    timeout_s: u64,
    log_path: &Path,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_s);
    let tail = || {
        std::fs::read_to_string(log_path)
            .unwrap_or_default()
            .lines()
            .rev()
            .take(30)
            .collect::<Vec<_>>()
            .join("\n")
    };
    loop {
        let (code, body) = http(port, &format!("/status?id={id}"));
        if code == 200 {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                if v["status"].as_str() == Some(want) {
                    return v;
                }
                if v["status"].as_str() == Some("errored") {
                    panic!(
                        "instance {id} errored: {body}\nworkerd log tail:\n{}",
                        tail()
                    );
                }
            }
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "instance {id} never reached {want} in {timeout_s}s\nworkerd log tail:\n{}",
                tail()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

#[test]
fn real_workerd_workflow() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let port = free_port();
    let wfops_port = free_port();
    let cfg = render_write(&dir, port, wfops_port, None);
    let proc = spawn(&bin, &cfg, &dir, None);
    assert!(
        wait_ready(port),
        "workerd ready: {}",
        std::fs::read_to_string(&proc.log_path).unwrap_or_default()
    );

    // create → poll to complete; a 2s sleep + one retried step are
    // inside the run.
    let (code, body) = http(port, "/create");
    assert_eq!(code, 200, "create: {body}");
    let created: serde_json::Value = serde_json::from_str(&body).unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    let done = wait_status(port, &id, "complete", 45, &proc.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");

    // get(id) retrieves the same instance.
    let (code, body) = http(port, &format!("/status?id={id}"));
    assert_eq!(code, 200);
    assert!(body.contains("complete"), "{body}");

    // Restart DURING the sleep window: create a second instance,
    // kill workerd quickly, respawn on the SAME localDisk dir — the
    // engine's persisted alarm/state resumes it to completion.
    let (code, body) = http(port, "/create");
    assert_eq!(code, 200);
    let id2 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    // W3-r4: NO probe and NO commit beat. The wrapped binding's
    // create() resolves only after the engine confirms the instance
    // record is committed (bounded get() poll), so a /create 200 is
    // itself the durability signal — SIGKILL immediately.
    drop(proc); // kill workerd; the `do/` localDisk dir persists
    let _proc2 = spawn(&bin, &cfg, &dir, None);
    assert!(wait_ready(port));
    let done2 = wait_status(port, &id2, "complete", 60, &_proc2.log_path);
    assert_eq!(done2["output"].as_str(), Some("a-b-done"), "{done2}");
}

// ---------------------------------------------------------------------------
// W6: lifecycle smoke — pause/resume/terminate/restart/sendEvent/
// waitForEvent(+timeout)/SIGKILL/delete/recreate + tenant isolation.
// ---------------------------------------------------------------------------

/// Minimal control listener hosting `wf_delete::router` — the same
/// handler production merges onto `control_listen`. Runs on a
/// std::thread with its own tokio runtime; returns the bound port.
fn start_wfdel(dir: &Path) -> u16 {
    use vrt_host::supervisor::{DoStorageView, TenantRuntime};
    let tomb_dir = dir.join("wf-del");
    let reg = vrt_host::supervisor::TenantRegistry::new();
    let rt = TenantRuntime::new("team-smoke");
    rt.set_do_storage_view(Some(DoStorageView {
        tenant_dir: dir.join("do"),
        worker_classes: std::collections::BTreeMap::from([(
            "wsmokewf".to_string(),
            vec!["c".repeat(64)],
        )]),
    }));
    reg.insert("team-smoke".into(), rt);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    // tokio::net::TcpListener::from_std requires nonblocking.
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let rtok = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rtok.block_on(async move {
            let app =
                vrt_host::wf_delete::router(vrt_host::wf_delete::WfDelState::new(reg, tomb_dir));
            let tl = tokio::net::TcpListener::from_std(listener).unwrap();
            axum::serve(tl, app).await.unwrap();
        });
    });
    port
}

/// POST helper — mirrors `http_once_inner`'s body-complete framing
/// but sends a JSON body + optional headers (wfops token).
fn http_post(
    port: u16,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> std::io::Result<(u16, String)> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    let mut head = format!(
        "POST {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    stream.write_all(format!("{head}\r\n{body}").as_bytes())?;
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "closed before response headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let code: u16 = head_text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let mut body_bytes = buf[header_end..].to_vec();
    let mut content_length: Option<usize> = None;
    for line in head_text.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().ok();
            }
        }
    }
    if let Some(len) = content_length {
        while body_bytes.len() < len {
            let mut chunk = [0u8; 8192];
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            body_bytes.extend_from_slice(&chunk[..n]);
        }
        body_bytes.truncate(len);
    } else {
        stream.read_to_end(&mut body_bytes)?;
    }
    Ok((
        code,
        String::from_utf8_lossy(&body_bytes).trim().to_string(),
    ))
}

/// `POST <wfops_sock>` with the op-token the vrt-host hop would
/// inject — derived identically (`wfop_token(pepper, uniqueKey)`).
fn wfop(port: u16, op: serde_json::Value) -> (u16, String) {
    let token = vrt_config::workflows::wfop_token("wf-smoke-pepper", &"c".repeat(64));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match http_post(port, "/", &[("x-vrt-wfop-token", &token)], &op.to_string()) {
            Ok(r) => return r,
            Err(e) => {
                if std::time::Instant::now() > deadline {
                    panic!("wfop {op} failed: {e}");
                }
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        }
    }
}

/// `.sqlite` stems under the engine class dir — instance files are
/// `<hexId>.sqlite{,-shm,-wal}`; the set of stems tracks which
/// instances persist on disk.
fn sqlite_stems(dir: &Path) -> std::collections::BTreeSet<String> {
    let class = dir.join("do").join("wsmokewf").join("c".repeat(64));
    let mut out = std::collections::BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir(&class) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(stem) = name.strip_suffix(".sqlite") {
                // `metadata` is workerd's own per-namespace file,
                // not a workflow instance actor.
                if stem != "metadata" {
                    out.insert(stem.to_string());
                }
            }
        }
    }
    out
}

/// Poll until status ∈ `wants`; panics with a log tail on deadline.
fn wait_status_any(
    port: u16,
    id: &str,
    wants: &[&str],
    timeout_s: u64,
    log_path: &Path,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_s);
    loop {
        let (code, body) = http(port, &format!("/status?id={id}"));
        if code == 200 {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                let st = v["status"].as_str().unwrap_or("");
                if wants.contains(&st) {
                    return v;
                }
                if st == "errored" {
                    panic!("instance {id} errored while waiting for {wants:?}: {body}");
                }
            }
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "instance {id} never reached {wants:?} in {timeout_s}s\nlog tail:\n{}",
                std::fs::read_to_string(log_path)
                    .unwrap_or_default()
                    .lines()
                    .rev()
                    .take(30)
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

#[test]
fn real_workerd_wf_lifecycle() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let wfdel = start_wfdel(&dir);
    let port = free_port();
    let wfops_port = free_port();
    let cfg = render_write(&dir, port, wfops_port, Some(wfdel));
    let proc = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(
        wait_ready(port),
        "workerd ready: {}",
        std::fs::read_to_string(&proc.log_path).unwrap_or_default()
    );

    // --- sendEvent: waitForEvent resolves with the payload ---------
    let (code, body) = http(port, "/create?params=%7B%22wait%22%3A%22event%22%7D");
    assert_eq!(code, 200, "create event-waiter: {body}");
    let id_ev = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status_any(port, &id_ev, &["running", "waiting"], 20, &proc.log_path);
    let (code, body) = http(
        port,
        &format!("/event?id={id_ev}&type=approval&payload=%7B%22v%22%3A42%7D"),
    );
    assert_eq!(code, 200, "sendEvent: {body}");
    let done = wait_status(port, &id_ev, "complete", 30, &proc.log_path);
    assert_eq!(done["output"].as_str(), Some("got-42"), "{done}");

    // --- waitForEvent timeout → typed error path -------------------
    let (code, body) = http(port, "/create?params=%7B%22wait%22%3A%22timeout%22%7D");
    assert_eq!(code, 200, "create timeout-waiter: {body}");
    let id_to = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let done = wait_status(port, &id_to, "complete", 30, &proc.log_path);
    assert_eq!(done["output"].as_str(), Some("timed-out"), "{done}");

    // --- pause → progress halts → resume → completes ---------------
    let (code, body) = http(port, "/create?params=%7B%22slow%22%3Atrue%7D");
    assert_eq!(code, 200, "create slow: {body}");
    let id_slow = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status_any(port, &id_slow, &["running", "waiting"], 20, &proc.log_path);
    let (code, body) = http(port, &format!("/action?id={id_slow}&op=pause"));
    assert_eq!(code, 200, "pause: {body}");
    // paused directly OR waitingForPause (parked at the sleep's
    // alarm boundary) — either proves the pause intent registered.
    wait_status_any(
        port,
        &id_slow,
        &["paused", "waitingForPause"],
        15,
        &proc.log_path,
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    let (_, body) = http(port, &format!("/status?id={id_slow}"));
    assert!(!body.contains("\"complete\""), "still progressing: {body}");
    let (code, body) = http(port, &format!("/action?id={id_slow}&op=resume"));
    assert_eq!(code, 200, "resume: {body}");
    let done = wait_status(port, &id_slow, "complete", 30, &proc.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");

    // --- restart → reruns from the beginning -----------------------
    let (code, body) = http(port, "/create");
    assert_eq!(code, 200);
    let id_re = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status(port, &id_re, "complete", 30, &proc.log_path);
    let (code, body) = http(port, &format!("/action?id={id_re}&op=restart"));
    assert_eq!(code, 200, "restart: {body}");
    // The rerun traverses running again before reaching complete.
    wait_status_any(
        port,
        &id_re,
        &["running", "queued", "waiting", "complete"],
        15,
        &proc.log_path,
    );
    let done = wait_status(port, &id_re, "complete", 30, &proc.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");

    // --- terminate persists across a workerd restart ---------------
    let (code, body) = http(port, "/create");
    assert_eq!(code, 200);
    let id_term = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status_any(port, &id_term, &["running", "queued"], 20, &proc.log_path);
    let (code, body) = http(port, &format!("/action?id={id_term}&op=terminate"));
    assert_eq!(code, 200, "terminate: {body}");
    wait_status(port, &id_term, "terminated", 20, &proc.log_path);
    drop(proc); // SIGKILL semantics (Drop::kill + wait)
    let proc2 = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(wait_ready(port));
    wait_status(port, &id_term, "terminated", 30, &proc2.log_path);

    // --- SIGKILL while waiting for event → respawn → sendEvent -----
    let (code, body) = http(port, "/create?params=%7B%22wait%22%3A%22event%22%7D");
    assert_eq!(code, 200);
    let id_kill = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status_any(port, &id_kill, &["running", "waiting"], 20, &proc2.log_path);
    drop(proc2); // kill mid-wait
    let proc3 = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(wait_ready(port));
    let (code, body) = http(
        port,
        &format!("/event?id={id_kill}&type=approval&payload=%7B%22v%22%3A%22after-kill%22%7D"),
    );
    assert_eq!(code, 200, "sendEvent after kill: {body}");
    let done = wait_status(port, &id_kill, "complete", 40, &proc3.log_path);
    assert_eq!(done["output"].as_str(), Some("got-after-kill"), "{done}");

    // --- op-path delete → files unlinked → same-id recreate --------
    let before = sqlite_stems(&dir);
    let (code, body) = http(port, "/create");
    assert_eq!(code, 200);
    let id_del = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status(port, &id_del, "complete", 30, &proc3.log_path);
    let after_create = sqlite_stems(&dir);
    let stem: Vec<String> = after_create.difference(&before).cloned().collect();
    assert_eq!(stem.len(), 1, "one new sqlite for the instance: {stem:?}");
    let (code, body) = wfop(
        wfops_port,
        serde_json::json!({
            "op": "delete",
            "workflow": "order-flow",
            "instanceId": id_del,
        }),
    );
    assert_eq!(code, 200, "op delete: {body}");
    assert!(body.contains("deleted"), "{body}");
    // W6-r2: "deleted" = logical delete committed — a tombstone now
    // exists in the host state dir and the on-disk sqlite REMAINS
    // (possibly held by a resident actor). Physical unlink is the
    // safe-point reclaimer's job, exercised below after SIGKILL.
    let tomb = dir
        .join("wf-del")
        .join("team-smoke")
        .join("c".repeat(64))
        .join(format!("{}.json", stem[0]));
    assert!(
        tomb.exists(),
        "tombstone must persist after op delete: {}",
        tomb.display()
    );
    assert!(
        sqlite_stems(&dir).contains(&stem[0]),
        "r2: deleted incarnation's files stay until the safe point"
    );
    // Instance gone at the binding level (metadata wiped). This get
    // may activate an empty actor — DO semantics, same as CF.
    let (code, body) = http(port, &format!("/status?id={id_del}"));
    assert_eq!(code, 404, "deleted instance must 404: {body}");
    // Repeated op-path delete on the missing instance → typed 404.
    let (code, body) = wfop(
        wfops_port,
        serde_json::json!({
            "op": "delete",
            "workflow": "order-flow",
            "instanceId": id_del,
        }),
    );
    assert_eq!(code, 404, "re-delete: {body}");
    // Same logical id recreates into a clean, fresh instance — the
    // tombstone for this hex must NOT unlink the recreated db.
    let (code, body) = http(port, &format!("/create?id={id_del}"));
    assert_eq!(code, 200, "recreate: {body}");
    let done = wait_status(port, &id_del, "complete", 30, &proc3.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");
    // --- safe point with a LIVE recreate: SIGKILL → reclaim must
    // find INSTANCE_METADATA present → drop the stale tombstone and
    // KEEP the file. This is the only gate between a tombstone and a
    // recreated instance's storage — exercised on real workerd.
    drop(proc3);
    let stats = vrt_host::wf_delete::reclaim_tenant("team-smoke", &dir.join("wf-del"), &None, None);
    assert!(
        stats.skipped_nonempty >= 1,
        "reclaim must skip the recreated (nonempty) instance: {stats:?}"
    );
    assert!(
        sqlite_stems(&dir).contains(&stem[0]),
        "recreated instance file must survive the tombstone"
    );
    assert!(!tomb.exists(), "stale tombstone dropped on nonempty gate");
    let proc4 = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(wait_ready(port));
    wait_status(port, &id_del, "complete", 30, &proc4.log_path);

    // Delete AGAIN — this tombstone points at the now-empty
    // incarnation → safe point unlinks for real.
    let (code, body) = wfop(
        wfops_port,
        serde_json::json!({
            "op": "delete",
            "workflow": "order-flow",
            "instanceId": id_del,
        }),
    );
    assert_eq!(code, 200, "second op delete: {body}");
    assert!(tomb.exists(), "second tombstone persisted");
    drop(proc4);
    let stats = vrt_host::wf_delete::reclaim_tenant("team-smoke", &dir.join("wf-del"), &None, None);
    assert!(
        stats.reclaimed >= 1,
        "reclaim must unlink the emptied incarnation: {stats:?}"
    );
    assert!(
        !sqlite_stems(&dir).contains(&stem[0]),
        "{} must be unlinked at the safe point",
        stem[0]
    );
    assert!(!tomb.exists(), "tombstone dropped after reclaim");
    // Respawn — the delete stays deleted (deleteAll is durable), and
    // the same id recreates cleanly once more.
    let proc5 = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(wait_ready(port));
    let (code, body) = http(port, &format!("/status?id={id_del}"));
    assert_eq!(code, 404, "deleted instance must stay 404: {body}");
    let (code, body) = http(port, &format!("/create?id={id_del}"));
    assert_eq!(code, 200, "post-reclaim recreate: {body}");
    let done = wait_status(port, &id_del, "complete", 30, &proc5.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");

    // --- op-path sendEvent through the cell facade -----------------
    let (code, body) = http(port, "/create?params=%7B%22wait%22%3A%22event%22%7D");
    assert_eq!(code, 200);
    let id_ev2 = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status_any(port, &id_ev2, &["running", "waiting"], 20, &proc5.log_path);
    let (code, body) = wfop(
        wfops_port,
        serde_json::json!({
            "op": "sendEvent",
            "workflow": "order-flow",
            "instanceId": id_ev2,
            "eventType": "approval",
            "payload": {"v": "via-op"},
        }),
    );
    assert_eq!(code, 200, "op sendEvent: {body}");
    let done = wait_status(port, &id_ev2, "complete", 30, &proc5.log_path);
    assert_eq!(done["output"].as_str(), Some("got-via-op"), "{done}");

    // --- tenant cannot reach the wfdel control listener ------------
    let (code, body) = http(port, &format!("/probe?port={wfdel}"));
    assert_eq!(code, 200, "probe: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    // Egress denial surfaces as EITHER a fetch rejection
    // (reached:false) or a 403 the egress proxy itself returns —
    // the wfdel handler never emits 403, so seeing it proves the
    // request died at the egress gate before reaching the listener.
    let denied = v["reached"].as_bool() == Some(false) || v["status"].as_u64() == Some(403);
    assert!(
        denied,
        "tenant worker reached the wfdel control port: {body}"
    );
}

/// W6 measurement of the PRE-W6 contract: with no control listener
/// (the W1 stub condition), `inst.delete()` reports success at the
/// binding level while the actor's sqlite stays on disk — the false
/// success this task removes for the configured path.
#[test]
fn real_workerd_wf_delete_degraded_residue() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let port = free_port();
    let wfops_port = free_port();
    let cfg = render_write(&dir, port, wfops_port, None); // NO wfdel → degraded shim
    let proc = spawn(&bin, &cfg, &dir, None);
    assert!(
        wait_ready(port),
        "workerd ready: {}",
        std::fs::read_to_string(&proc.log_path).unwrap_or_default()
    );

    let before = sqlite_stems(&dir);
    let (code, body) = http(port, "/create");
    assert_eq!(code, 200, "create: {body}");
    let id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status(port, &id, "complete", 30, &proc.log_path);
    let stems = sqlite_stems(&dir);
    let stem: Vec<String> = stems.difference(&before).cloned().collect();
    assert_eq!(stem.len(), 1, "instance sqlite exists: {stem:?}");

    // Binding-level delete(): engine wipe + loopback 501 (degraded)
    // → the vendored code logs the failure and returns void success.
    let (code, body) = http(port, &format!("/action?id={id}&op=delete"));
    assert_eq!(code, 200, "binding delete: {body}");

    // Measured residue (the pre-W6 false success): logical state
    // gone (404) while the sqlite file remains on disk.
    let (code, body) = http(port, &format!("/status?id={id}"));
    assert_eq!(code, 404, "metadata wiped: {body}");
    assert!(
        sqlite_stems(&dir).contains(&stem[0]),
        "degraded path must leave the residue it documents"
    );

    // And the op path fails closed rather than faking success: the
    // facade's delete op calls inst.delete() — the engine reports the
    // already-deleted instance as not_found → typed 404, never a
    // false 200.
    let (code, body) = wfop(
        wfops_port,
        serde_json::json!({
            "op": "delete",
            "workflow": "order-flow",
            "instanceId": id,
        }),
    );
    // instance is already logically deleted → engine not_found 404,
    // or unconfirmable 502 — both fail closed, never a fake success.
    assert!(
        code == 404 || code == 502,
        "degraded op delete must fail closed: {code} {body}"
    );
}

/// W6-r2 regression — FAILED on the pre-r2 convergence-unlink impl:
/// a `get()` issued inside the pending-delete window re-instantiated
/// the actor, which recreated `<hex>.sqlite` while the delete loop
/// was still unlinking — the loop then removed a file the LIVING
/// actor held open, the following `create()` committed init into
/// that unlinked inode, and the next workerd restart lost the
/// instance entirely (W3-r4 phantom). Under r2 the DELETE records a
/// tombstone only (no unlink while workerd lives), so the recreated
/// incarnation's file is never removed from under an actor — the
/// instance must survive the SIGKILL/respawn below.
#[test]
fn real_workerd_wf_delete_get_race_loses_recreate() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let wfdel = start_wfdel(&dir);
    let port = free_port();
    let wfops_port = free_port();
    let cfg = render_write(&dir, port, wfops_port, Some(wfdel));
    let proc = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(
        wait_ready(port),
        "workerd ready: {}",
        std::fs::read_to_string(&proc.log_path).unwrap_or_default()
    );

    let (code, body) = http(port, "/create");
    assert_eq!(code, 200, "create: {body}");
    let id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_status(port, &id, "complete", 30, &proc.log_path);

    // Issue the op delete from a thread; concurrently `get()` the
    // instance — get() spins a fresh actor that holds the sqlite open
    // through the whole delete op. Pre-r2 this file was unlinked from
    // under the actor; r2 leaves it (tombstone only) and the actor's
    // emptied state is what any restore would see.
    let id_t = id.clone();
    let del = std::thread::spawn(move || {
        wfop(
            wfops_port,
            serde_json::json!({
                "op": "delete",
                "workflow": "order-flow",
                "instanceId": id_t,
            }),
        )
    });
    for _ in 0..60 {
        let _ = http(port, &format!("/status?id={id}"));
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let (code, body) = del.join().unwrap();
    assert_eq!(code, 200, "op delete: {body}");

    // Same-id recreate — vendored create() polls the wait endpoint
    // (always 204 now — no pending-delete concept remains) and writes
    // init durably into the real file.
    let (code, body) = http(port, &format!("/create?id={id}"));
    assert_eq!(code, 200, "recreate: {body}");
    wait_status_any(
        port,
        &id,
        &["running", "queued", "complete"],
        20,
        &proc.log_path,
    );

    // SIGKILL + respawn — the recreated instance MUST still exist
    // and finish its run.
    drop(proc);
    let proc2 = spawn(&bin, &cfg, &dir, Some(wfdel));
    assert!(wait_ready(port));
    let done = wait_status(port, &id, "complete", 40, &proc2.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");
}

// ---------------------------------------------------------------------------
// W7: cross-worker `script_name` — X binds a workflow DEFINED by Y.
// Engine, storage and ops identity live under Y; X's wrapped binding
// is just a service designator into Y's engine. Real workerd
// create→complete through X's socket proves the retarget end to end.
// ---------------------------------------------------------------------------

/// Referrer worker module: fetch-only, no workflow class of its own —
/// `env.WF` must resolve against the DEFINING worker's engine.
const X_MODULE: &str = r#"
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    try {
      if (url.pathname === "/create") {
        const inst = await env.WF.create({});
        return Response.json({ id: inst.id });
      }
      if (url.pathname === "/status") {
        const inst = await env.WF.get(url.searchParams.get("id"));
        return Response.json(await inst.status());
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return Response.json(
        { error: String(e && e.message ? e.message : e).slice(0, 300) },
        { status: 500 },
      );
    }
  }
};
"#;

fn render_write_cross(dir: &Path, x_port: u16, y_port: u16, wfops_port: u16) -> PathBuf {
    let x_id = "wsmokex";
    let y_id = "wsmokey";
    let x_bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": X_MODULE },
            "compatibilityDate": "2025-01-01",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let y_bundle = VersionBundle::build(
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
        "tenantId": "team-smoke",
        "workers": [
            {
                "workerId": x_id,
                "name": "wf-x",
                "activeVersion": {
                    "versionId": "v-x-1",
                    "artifactDigest": x_bundle.digest(),
                    "compatibilityDate": "2025-01-01",
                    "compatibilityFlags": []
                },
                "bindings": [],
                "routes": [{"hostname": "wfx--team-smoke.vibeploy.app"}],
                "crons": [],
                "workflows": [{
                    "name": "order-flow",
                    "binding": "WF",
                    "className": "OrderFlow",
                    "uniqueKey": "c".repeat(64),
                    "serviceWorkerId": y_id
                }],
                "quotas": {"cpuMs": 50, "memoryMb": 128,
                           "subrequests": 100, "outboundConnections": 16}
            },
            {
                "workerId": y_id,
                "name": "wf-y",
                "activeVersion": {
                    "versionId": "v-y-1",
                    "artifactDigest": y_bundle.digest(),
                    "compatibilityDate": "2025-01-01",
                    "compatibilityFlags": []
                },
                "bindings": [],
                "routes": [{"hostname": "wfy--team-smoke.vibeploy.app"}],
                "crons": [],
                "quotas": {"cpuMs": 50, "memoryMb": 128,
                           "subrequests": 100, "outboundConnections": 16}
            }
        ]
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
        disk_services: vrt_config::resolve_disk_paths(&facade_root(), &manifest.disk_services).unwrap(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "wf-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[x_bundle, y_bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

    // Emit invariants: the engine is keyed under the DEFINING worker;
    // X's binding is a designator into it; USER_WORKFLOW resolves the
    // class against Y's service — never X's.
    assert!(
        out.text.contains("wf-engine-wsmokey-order-flow"),
        "{}",
        out.text
    );
    assert!(
        !out.text.contains("wf-engine-wsmokex-order-flow"),
        "{}",
        out.text
    );
    assert!(out.text.contains("do-storage-wsmokey"), "{}", out.text);

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
                let bytes = if worker_id == x_id {
                    X_MODULE
                } else {
                    USER_MODULE
                };
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                std::fs::write(&dst, vrt_config::workflows::vendored_bytes(name).unwrap()).unwrap();
            }
        }
    }

    let mut text = out.text.clone();
    // The wfops socket is keyed under the DEFINING worker (Y) — this
    // replacement fails loudly if emit regresses to X.
    let wfops_unix = format!("unix:sock/{y_id}-wfops.sock");
    assert!(text.contains(&wfops_unix), "{text}");
    text = text.replace(&wfops_unix, &format!("127.0.0.1:{wfops_port}"));
    text = text.replace(
        &format!("unix:sock/{x_id}.sock"),
        &format!("127.0.0.1:{x_port}"),
    );
    text = text.replace(
        &format!("unix:sock/{y_id}.sock"),
        &format!("127.0.0.1:{y_port}"),
    );
    std::fs::create_dir_all(dir.join("do").join(x_id)).unwrap();
    std::fs::create_dir_all(dir.join("do").join(y_id)).unwrap();
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn real_workerd_wf_cross_worker() {
    let Some(bin) = workerd_bin() else { return };
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let x_port = free_port();
    let y_port = free_port();
    let wfops_port = free_port();
    let cfg = render_write_cross(&dir, x_port, y_port, wfops_port);
    let proc = spawn(&bin, &cfg, &dir, None);
    assert!(
        wait_ready(x_port),
        "workerd ready: {}",
        std::fs::read_to_string(&proc.log_path).unwrap_or_default()
    );

    // Create via X's socket — env.WF on X resolves to Y's engine,
    // which instantiates Y's OrderFlow class. Poll to completion.
    let (code, body) = http(x_port, "/create");
    assert_eq!(code, 200, "create: {body}");
    let created: serde_json::Value = serde_json::from_str(&body).unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    let done = wait_status(x_port, &id, "complete", 45, &proc.log_path);
    assert_eq!(done["output"].as_str(), Some("a-b-done"), "{done}");

    // Engine storage landed under the DEFINING worker's disk, not X's.
    let y_do = dir.join("do").join("wsmokey");
    assert!(
        std::fs::read_dir(&y_do)
            .map(|mut it| it.next().is_some())
            .unwrap_or(false),
        "engine storage under defining worker: {}",
        y_do.display()
    );
}
