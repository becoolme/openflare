//! C1: real-workerd Cache API smoke — `cacheApiOutbound` → the
//! vrt-host `cache::CacheService`. Covers the wire contract end to
//! end: put → hit → delete → miss, TTL expiry, named namespaces,
//! tenant isolation across two workerd configs on ONE shared store,
//! oversized-body rejection, unavailable-service degradation, and
//! the explicit `No Cache was configured` baseline.
//!
//! Gated on `VRT_WORKERD_BIN` like the other smokes:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test cache_smoke -- --include-ignored
//!
//! Sockets: TCP loopback only — the dev sandbox refuses unix binds;
//! the unix path is the production default covered by `serve_tenant`
//! (the standalone e2e runs it unsandboxed).

#![cfg(unix)]

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use vrt_config::*;
use vrt_host::cache::{CacheBind, CacheLimits, CacheService};

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => {
            eprintln!(
                "skip: set VRT_WORKERD_BIN=/path/to/workerd \
                 to run the real-workerd cache smoke"
            );
            None
        }
    }
}

fn smoke_root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/tmp-c1")
        .join(format!(
            "run-{}-{}",
            std::process::id(),
            RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// workerd smokes must not run CONCURRENTLY: `free_port`'s
/// bind-then-drop can hand the same port to two tests racing in
/// different threads — `wait_ready` then connects to the OTHER
/// test's listener and asserts nonsense. Serialize the file.
static SMOKE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        // Keep the run dir on panic — the workerd log inside is the
        // only evidence of why a generation died.
        if !std::thread::panicking() {
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

/// `free_port` bind-then-drop lets the kernel hand back the SAME
/// port to the next caller — two workerds colliding on one port is a
/// flaky-test class. Loop until `n` distinct ports.
fn free_ports(n: usize) -> Vec<u16> {
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < n {
        seen.insert(free_port());
    }
    seen.into_iter().collect()
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

/// Tenant module: every cache op surfaces EXPLICITLY — a workerd-side
/// throw returns `err:<msg>` so a fake success can never pass as one.
const USER_MODULE: &str = r#"
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const ns = u.searchParams.get("ns");
    const cache = ns ? await caches.open(ns) : caches.default;
    const key = "https://k.test" + (u.searchParams.get("k") || "/x");
    try {
      switch (u.pathname) {
        case "/put": {
          const ttl = u.searchParams.get("ttl") || "300";
          const n = parseInt(u.searchParams.get("n") || "0", 10);
          const body = u.searchParams.get("v") || "x".repeat(n || 1);
          await cache.put(key, new Response(body, {
            headers: { "Cache-Control": `max-age=${ttl}` },
          }));
          return new Response("ok");
        }
        case "/putsc": {
          await cache.put(key, new Response("sc", { headers: {
            "Cache-Control": "max-age=60", "Set-Cookie": "a=b" } }));
          return new Response("ok");
        }
        case "/match": {
          const r = await cache.match(key);
          return r ? new Response("hit:" + await r.text())
                   : new Response("miss");
        }
        case "/del":
          return new Response("del:" + await cache.delete(key));
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return new Response("err:" + String(e && e.message ? e.message : e).slice(0, 200), { status: 500 });
    }
  }
};
"#;

fn mk_bundle() -> VersionBundle {
    VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": USER_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap()
}

fn tenant_json(worker_id: &str, tenant_id: &str) -> TenantDesired {
    serde_json::from_value(json!({
        "tenantId": tenant_id,
        "workers": [{
            "workerId": worker_id,
            "name": "cache",
            "activeVersion": {
                "versionId": "v-c1-1",
                "artifactDigest": mk_bundle().digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": format!("{worker_id}--{tenant_id}.vibeploy.app")}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap()
}

fn emit_config(
    dir: &Path,
    tenant: &TenantDesired,
    worker_id: &str,
    port: u16,
    cache_addr: &str,
) -> PathBuf {
    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&manifest_raw).unwrap();
    let bundle = mk_bundle();
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
        do_storage_dir: String::new(),
        wfop_pepper: "c1-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: cache_addr.into(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let input = TenantRenderInput {
        tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();
    if cache_addr.is_empty() {
        assert!(!out.text.contains("cacheApiOutbound"), "{}", out.text);
    } else {
        assert!(out.text.contains("cacheApiOutbound"), "{}", out.text);
        assert!(
            out.external_addrs.iter().any(|a| a.service == "vrt-cache"),
            "vrt-cache external addr missing"
        );
    }
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
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
    let text = out.text.replace(
        &format!("unix:sock/{worker_id}.sock"),
        &format!("127.0.0.1:{port}"),
    );
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path, extra: &[String]) -> WorkerProc {
    let log_path = dir.join(format!("workerd-{}.log", std::process::id()));
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    cmd.arg("serve")
        .arg("--external-addr=binding-backend=127.0.0.1:1")
        .arg(cfg)
        .arg("--experimental");
    for a in extra {
        cmd.arg(a);
    }
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

fn http(port: u16, path: &str) -> (u16, String) {
    let mut s = match std::net::TcpStream::connect(("127.0.0.1", port)) {
        Ok(s) => s,
        Err(_) => return (0, String::new()),
    };
    s.set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf);
    let code = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (code, body)
}

fn rt() -> tokio::runtime::Runtime {
    // MULTI-THREAD: `serve()` spawns the accept loop onto the ambient
    // runtime — a current_thread runtime freezes spawned tasks as
    // soon as block_on returns (the listener's backlog still
    // completes TCP handshakes, so connects "succeed" while no
    // response ever comes — that exact hang showed up as the
    // empty-body flake before this note existed).
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

// =============== scenarios ===============

/// put → hit → delete → miss, named namespaces, TTL, Set-Cookie
/// refusal, oversized body, and the no-listener degradation path.
#[test]
fn real_workerd_cache_lifecycle() {
    let Some(bin) = workerd_bin() else { return };
    let _serial = SMOKE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let mut ports = free_ports(2).into_iter();
    let port = ports.next().unwrap();
    let cache_port = ports.next().unwrap();
    let tenant = tenant_json("w-c1-1", "team-c1");
    let cfg = emit_config(
        &dir,
        &tenant,
        "w-c1-1",
        port,
        &format!("127.0.0.1:{cache_port}"),
    );
    let proc = spawn(
        &bin,
        &cfg,
        &dir,
        &[format!("--external-addr=vrt-cache=127.0.0.1:{cache_port}")],
    );
    assert!(
        wait_ready(port),
        "workerd must come up; log: {:?}",
        proc.log_path
    );

    // No listener bound yet → the emitted external addr is dead.
    // MEASURED workerd contract: `put()` RESOLVES with a dead
    // backend (the write is advisory — connect failure is swallowed
    // inside workerd), while `match` throws `err:Network connection
    // lost` (explicit, never a fake hit). Assert the real invariant:
    // whatever put returned, NOTHING was stored.
    let (_code, put_body) = http(port, "/put?k=/a&v=hi");
    let (_, body) = http(port, "/match?k=/a");
    assert_ne!(
        body, "hit:hi",
        "dead cache service must not store (put: {put_body}, match: {body})"
    );

    // Bind this tenant's listener on the shared service.
    let svc = CacheService::new(
        None,
        CacheLimits {
            max_entry_bytes: 4 * 1024,
            tenant_bytes: 128 * 1024,
            global_bytes: 256 * 1024,
            global_inflight_bytes: 128 * 1024,
            tenant_inflight_bytes: 64 * 1024,
        },
    )
    .unwrap();
    // The runtime MUST outlive the listener — dropping it kills the
    // spawned accept task and closes the socket (same class of bug
    // as the discarded CacheListener).
    let rt = rt();
    let listener = rt
        .block_on(svc.serve(
            "team-c1",
            CacheBind::Tcp(format!("127.0.0.1:{cache_port}").parse().unwrap()),
        ))
        .unwrap();

    // put → hit.
    let (_, body) = http(port, "/put?k=/a&v=hello");
    assert_eq!(body, "ok", "put must succeed: {body}");
    let (_, body) = http(port, "/match?k=/a");
    assert_eq!(body, "hit:hello", "stored entry must hit: {body}");
    // Other keys miss.
    let (_, body) = http(port, "/match?k=/b");
    assert_eq!(body, "miss");
    // delete → true, then miss.
    let (_, body) = http(port, "/del?k=/a");
    assert_eq!(body, "del:true");
    let (_, body) = http(port, "/match?k=/a");
    assert_eq!(body, "miss");

    // Named namespace: disjoint from default.
    let (_, body) = http(port, "/put?ns=ns1&k=/a&v=nsv");
    assert_eq!(body, "ok");
    let (_, body) = http(port, "/match?ns=ns1&k=/a");
    assert_eq!(body, "hit:nsv");
    let (_, body) = http(port, "/match?k=/a");
    assert_eq!(body, "miss", "named cache must not leak into default");

    // TTL: 1s entry expires → miss after the deadline.
    let (_, body) = http(port, "/put?k=/t&v=ttl&ttl=1");
    assert_eq!(body, "ok");
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let (_, body) = http(port, "/match?k=/t");
    assert_eq!(body, "miss", "expired entry must miss");

    // Set-Cookie response: never stored (either an explicit put error
    // or a silent not-store — the INVARIANT is match misses).
    let (_, put_body) = http(port, "/putsc?k=/sc");
    let (_, body) = http(port, "/match?k=/sc");
    assert_eq!(
        body, "miss",
        "set-cookie must never be stored (put: {put_body})"
    );

    // Oversized body (>4KiB cap): rejected by the service — workerd
    // surfaces an explicit error or a not-store; match misses either
    // way, and the process stays bounded.
    let (_, put_body) = http(port, "/put?k=/big&n=8192");
    let (_, body) = http(port, "/match?k=/big");
    assert_eq!(
        body, "miss",
        "oversized entry must not be stored (put: {put_body})"
    );

    // Listener drop → match surfaces an explicit workerd-side error
    // (never a fake hit, never a hang past the read timeout).
    drop(listener);
    std::thread::sleep(std::time::Duration::from_millis(100));
    let (_, body) = http(port, "/match?k=/a");
    assert_ne!(body, "hit:hello", "dead listener must not fake a hit");
}

/// Two tenants on ONE shared CacheService — same URL, same namespace:
/// each sees only its own key space.
#[test]
fn real_workerd_cache_tenant_isolation() {
    let Some(bin) = workerd_bin() else { return };
    let _serial = SMOKE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let mut ports = free_ports(4).into_iter();
    let port_a = ports.next().unwrap();
    let port_b = ports.next().unwrap();
    let cache_a = ports.next().unwrap();
    let cache_b = ports.next().unwrap();
    let ta = tenant_json("w-c1-a", "team-a");
    let tb = tenant_json("w-c1-b", "team-b");
    std::fs::create_dir_all(dir.join("a")).unwrap();
    let cfg_a = emit_config(
        &dir.join("a"),
        &ta,
        "w-c1-a",
        port_a,
        &format!("127.0.0.1:{cache_a}"),
    );
    std::fs::create_dir_all(dir.join("b")).unwrap();
    let cfg_b = emit_config(
        &dir.join("b"),
        &tb,
        "w-c1-b",
        port_b,
        &format!("127.0.0.1:{cache_b}"),
    );
    let mut proc_a = spawn(
        &bin,
        &cfg_a,
        &dir.join("a"),
        &[format!("--external-addr=vrt-cache=127.0.0.1:{cache_a}")],
    );
    let mut proc_b = spawn(
        &bin,
        &cfg_b,
        &dir.join("b"),
        &[format!("--external-addr=vrt-cache=127.0.0.1:{cache_b}")],
    );
    assert!(wait_ready(port_a) && wait_ready(port_b));

    // ONE shared store, TWO tenant-scoped listeners.
    let svc = CacheService::new(None, CacheLimits::standard()).unwrap();
    let rt = rt();
    let _l_a = rt
        .block_on(svc.serve(
            "team-a",
            CacheBind::Tcp(format!("127.0.0.1:{cache_a}").parse().unwrap()),
        ))
        .unwrap();
    let _l_b = rt
        .block_on(svc.serve(
            "team-b",
            CacheBind::Tcp(format!("127.0.0.1:{cache_b}").parse().unwrap()),
        ))
        .unwrap();

    let (_, body) = http(port_a, "/put?k=/iso&v=A");
    if body != "ok" {
        eprintln!(
            "diag: proc_a={:?} proc_b={:?} reconnect={:?} log={:?}",
            proc_a.child.try_wait(),
            proc_b.child.try_wait(),
            std::net::TcpStream::connect(("127.0.0.1", port_a)),
            proc_a.log_path
        );
    }
    assert_eq!(body, "ok");
    // B reads nothing of A's — same key.
    let (_, body) = http(port_b, "/match?k=/iso");
    assert_eq!(body, "miss", "tenant-b must not see tenant-a entries");
    // B writes its own; A still sees A's value (no clobber).
    let (_, body) = http(port_b, "/put?k=/iso&v=B");
    assert_eq!(body, "ok");
    let (_, body) = http(port_a, "/match?k=/iso");
    assert_eq!(body, "hit:A", "B's write must not clobber A's entry");
    let (_, body) = http(port_b, "/match?k=/iso");
    assert_eq!(body, "hit:B");
    drop(proc_a);
    drop(proc_b);
}

/// Baseline: NO `cacheApiOutbound` — `caches.*` throws
/// `No Cache was configured` (explicit, never a silent no-op).
#[test]
fn real_workerd_cache_absent() {
    let Some(bin) = workerd_bin() else { return };
    let _serial = SMOKE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let port = free_port();
    let tenant = tenant_json("w-c1-0", "team-none");
    let cfg = emit_config(&dir, &tenant, "w-c1-0", port, "");
    let proc = spawn(&bin, &cfg, &dir, &[]);
    assert!(wait_ready(port));
    let (_, body) = http(port, "/put?k=/a&v=hi");
    assert!(
        body.contains("No Cache was configured"),
        "absent cache must throw explicitly: {body}"
    );
    drop(proc);
}

/// C1-r2 P2: real workerd over the PRODUCTION transport — a unix
/// socket, not TCP loopback. Both ends reach the socket node through
/// the SAME relative path: the test process and spawned workerd
/// share cwd (the crate dir), so `../target/...` names one inode.
///
/// Sandbox note (measured): this dev sandbox denies ALL unix-socket
/// binds — `bind()` returns EPERM even inside the workspace — so the
/// probe below SKIPs there. On an unrestricted host (the docker e2e
/// review chain runs `--include-ignored` which includes normal
/// tests) it executes for real. NOT marked `#[ignore]`: where binds
/// are permitted it must run, where denied it self-skips — exactly
/// the d1_smoke probe contract.
#[test]
fn real_workerd_cache_unix_socket() {
    let Some(bin) = workerd_bin() else { return };
    let _serial = SMOKE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    // Relative form of `dir` — sun_path budget is ~104B and the
    // absolute worktree path overflows it.
    let rel_dir = Path::new("../target")
        .join("tmp-c1")
        .join(dir.file_name().unwrap());
    let sock = rel_dir.join("cache.sock");
    // Probe: if unix binds are denied in this sandbox, skip loudly.
    match std::os::unix::net::UnixListener::bind(rel_dir.join("probe.sock")) {
        Ok(l) => {
            drop(l);
            let _ = std::fs::remove_file(rel_dir.join("probe.sock"));
        }
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: unix socket bind denied ({e}) — sandboxed run");
            return;
        }
        Err(e) => panic!("probe unix bind in {:?}: {e}", rel_dir),
    }
    let mut ports = free_ports(1).into_iter();
    let port = ports.next().unwrap();
    let tenant = tenant_json("w-c1u-1", "team-c1u");
    let cfg = emit_config(
        &dir,
        &tenant,
        "w-c1u-1",
        port,
        &format!("unix:{}", sock.display()),
    );
    let proc = spawn(
        &bin,
        &cfg,
        &dir,
        &[format!("--external-addr=vrt-cache=unix:{}", sock.display())],
    );
    assert!(
        wait_ready(port),
        "workerd must come up; log: {:?}",
        proc.log_path
    );
    let svc = CacheService::new(None, CacheLimits::standard()).unwrap();
    let rt = rt();
    let listener = rt
        .block_on(svc.serve("team-c1u", CacheBind::Unix(sock.clone())))
        .unwrap();
    let (_code, body) = http(port, "/put?k=/u&v=unix");
    assert_eq!(body, "ok", "put over unix socket: {body}");
    let (_, body) = http(port, "/match?k=/u");
    assert_eq!(body, "hit:unix", "hit over unix socket: {body}");
    let (_, body) = http(port, "/del?k=/u");
    assert_eq!(body, "del:true");
    let (_, body) = http(port, "/match?k=/u");
    assert_eq!(body, "miss");
    drop(listener);
    drop(proc);
}
