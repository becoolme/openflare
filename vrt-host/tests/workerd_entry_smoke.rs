//! Real-workerd entrypoint smoke test (T31e/E0, extended T31f/F0.3+F1.2).
//!
//! Two regressions this exists for:
//! - `facade-router.ts` originally exported a plain class as
//!   `export default` — workerd treats a non-`WorkerEntrypoint` default
//!   class export as an *actor class* ("Exported actor class as default
//!   entrypoint …"), requests never reach the module, and the fetch
//!   falls through to `globalOutbound` → DenyAll → 403 for every path.
//!   Unit tests can't see this: they call `new Class().fetch()`
//!   directly, and the capnp golden only proves the emitted text, not
//!   that workerd's loader accepts the module's export shape.
//! - The router→facade-assets RPC contract (`fetchRouteIndex`) was
//!   only ever exercised through independent stubs on both sides —
//!   the real pair never ran together (T31f/F0). Here the router binds
//!   the REAL `facade-assets` module closure with real props, and the
//!   only stub is the provider itself, answering the exact wire format
//!   `vrt-provider/src/assets.rs` emits.
//!
//! The capnp is produced by the REAL renderer — `vrt_config::
//! render_tenant_config` on a `TenantDesired` with `assetsFirst` — and
//! post-processed only where the smoke environment differs:
//!   - `unix:` socket addresses → `127.0.0.1:PORT` (what is under test
//!     is the service/binding wiring, not the address family)
//!   - the `binding-backend` external service → a same-name worker
//!     service answering the provider's exact wire format. A real
//!     `vrt-provider` cannot start here (it needs its manifest store,
//!     unix listener, and identity plumbing); the contract under test
//!     is the wire protocol, which the stub serves verbatim.
//!
//! The assets binding name is parameterized ("ASSETS" and a custom
//! "STATIC") — T31f/F1: the router's env key is the fixed
//! `ROUTER_ASSETS_BINDING` while the user worker keeps the declared
//! name, so a custom name must not break routing.
//!
//! Gated on `VRT_WORKERD_BIN` — absent → explicit skip with a printed
//! reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test workerd_entry_smoke -- --include-ignored

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => None,
    }
}

fn facade_root() -> PathBuf {
    // tests live in vrt-host/tests — facade is a sibling crate dir.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// T34f: shared short root — real workerd binds unix service sockets
/// under the work dir; macOS temp_dir (~50B) + the gen/sock tail is
/// right at the AF_UNIX edge. `None` only on EPERM/EACCES (sandbox).
fn smoke_root() -> Option<PathBuf> {
    match vrt_securefs::test_short_root("vrt-smoke") {
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

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Write a minimal capnp serving `service` (the system worker's real
/// module closure). Used by the entry-load sweep only — the routing
/// case renders through `vrt-config` instead.
fn write_capnp(
    dir: &Path,
    service_name: &str,
    modules: &[(String, String)], // (logical name, source abs path)
    socket_service: &str,
    port: u16,
) -> PathBuf {
    let mut mods = String::new();
    for (i, (name, src)) in modules.iter().enumerate() {
        let file = format!("m{i}.js");
        let dst = dir.join(&file);
        std::fs::copy(src, &dst).unwrap();
        let decl = if name.ends_with(".wasm") {
            "wasm"
        } else {
            "esModule"
        };
        mods.push_str(&format!(
            "(name = \"{}\", {} = embed \"{}\"),",
            escape(name),
            decl,
            escape(&file)
        ));
    }
    let cfg = format!(
        r#"using Workerd = import "/workerd/workerd.capnp";
const cfg :Workerd.Config = (
  services = [
    (name = "{svc}", worker = (
      modules = [{mods}],
      compatibilityDate = "2025-01-01",
      globalOutbound = (name = "deny", entrypoint = "DenyAll"),
    )),
    (name = "deny", worker = (
      modules = [(name = "deny.js", esModule = embed "deny.js")],
      compatibilityDate = "2025-01-01",
    )),
  ],
  sockets = [(name = "http", address = "127.0.0.1:{port}",
              http = (), service = (name = "{socket}"))],
);
"#,
        svc = escape(service_name),
        mods = mods,
        port = port,
        socket = escape(socket_service),
    );
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, cfg).unwrap();
    path
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    spawn_extra(bin, cfg, dir, &[])
}

/// `extra` mirrors what a deployment would pass via
/// `--experimental`-style `extra_args` in the host config (the
/// `experimental` compat flag in `system_compat_flags` requires it).
fn spawn_extra(bin: &Path, cfg: &Path, dir: &Path, extra: &[&str]) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    cmd.arg("serve").arg(cfg);
    for a in extra {
        cmd.arg(a);
    }
    let child = cmd
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

/// A bare connect is not enough: `free_port` releases its probe
/// before the spawn can bind, and a concurrently-running test's
/// server can grab the port — a connect then succeeds against the
/// FOREIGN listener, whose later exit resets our request. Require
/// this spawned workerd's own `Listening on ...:<port>` log line.
fn wait_ready(port: u16, proc: &mut WorkerProc) -> bool {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
            && log_text(proc).contains(&format!("127.0.0.1:{port}"))
        {
            return true;
        }
        if matches!(proc.child.try_wait(), Ok(Some(_))) {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}

fn http_get(port: u16, path: &str) -> (u16, String) {
    http_get_within(port, path, std::time::Duration::from_secs(5))
}

/// `http_get` with a caller-chosen read timeout — the router RPC
/// budget is 5s, so timeout tests need the client to outlive it.
fn http_get_within(port: u16, path: &str, read_to: std::time::Duration) -> (u16, String) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(read_to)).unwrap();
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
                // G1/H1: workerd holds a ~5s socket teardown grace
                // on EVERY request — verified against a bare
                // Response.json with no RPC and no timers; it is a
                // fixed lifecycle artifact, not gated on orphaned
                // work. A complete Content-Length response is
                // delivered on time even though EOF lags. Stop at
                // the full response instead of waiting for EOF.
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

/// A buffered HTTP/1.1 response is complete once the header block
/// arrived and either `content-length` bytes of body did, or the
/// chunked terminator did. EOF-only signals (no framing header)
/// still fall through to the socket-close/timeout path.
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

/// The DenyAll globalOutbound fallback shape — what a request looks
/// like when the entrypoint module never executes (actor-class bug).
const DENY_SHAPE: &str = "\"code\":\"no_outbound\"";

fn log_text(proc: &WorkerProc) -> String {
    let raw = std::fs::read(&proc.log_path).unwrap_or_default();
    String::from_utf8_lossy(&raw).replace('\0', "")
}

fn assert_no_actor_warning(proc: &WorkerProc) {
    let log = log_text(proc);
    assert!(
        !log.contains("Exported actor class as default entrypoint"),
        "workerd treated a default class export as an actor class:\n{log}"
    );
}

fn manifest_modules(worker: &str) -> Vec<(String, String)> {
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
    // workerd treats the FIRST module as the main module; the manifest
    // lists modules sorted by name, so put `mainModule` first explicitly.
    let main = doc["workers"][worker]["mainModule"].as_str().unwrap();
    let mut mods: Vec<&serde_json::Value> = doc["workers"][worker]["modules"]
        .as_array()
        .unwrap()
        .iter()
        .collect();
    mods.sort_by_key(|m| m["name"].as_str() != Some(main));
    mods.into_iter()
        .map(|m| {
            (
                m["name"].as_str().unwrap().to_string(),
                // manifest `dist` paths are repo-root-relative —
                // facade/ sits at facade.
                facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap())
                    .to_string_lossy()
                    .into_owned(),
            )
        })
        .collect()
}

fn write_deny_stub(dir: &Path) {
    std::fs::write(
        dir.join("deny.js"),
        "import { WorkerEntrypoint } from \"cloudflare:workers\";\nexport class DenyAll extends WorkerEntrypoint { async fetch() { return new Response(JSON.stringify({error:\"EGRESS_DENIED\",code:\"no_outbound\"}),{status:403,headers:{\"cache-control\":\"no-store\"}}); } }\nexport default DenyAll;\n",
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Dynamic Workers (workerLoader) smoke — the pinned fork binary must
// accept the `workerLoader` group `vrt-config` emits under
// `--experimental` (including the fork's `maxWorkers` field, which a
// stock upstream binary would fail to parse), and the runtime must
// actually load, name-cache and quota-bound dynamic workers.
// ---------------------------------------------------------------------------

/// The outer worker module: exercises `env.LOADER.get()` (named cache +
/// entrypoint fetch), `env.LOADER.load()` (anonymous), the named-cache
/// callback-run-once contract, and the fork's live-worker quota on a
/// dedicated `LOADER_A` namespace so earlier `LOADER` traffic cannot
/// pollute the count.
const LOADER_MAIN_JS: &str = r#"
const DYN = {
  compatibilityDate: '2025-01-01',
  mainModule: 'w.js',
  modules: {
    'w.js': "export default { async fetch(req) { return new Response('dyn:' + new URL(req.url).pathname); } }",
  },
};

export default {
  async fetch(req, env) {
    const path = new URL(req.url).pathname;
    if (path === '/get') {
      const res = await env.LOADER.get('a', () => DYN).getEntrypoint().fetch('http://dyn/a');
      return new Response('get:' + (await res.text()));
    }
    if (path === '/load') {
      const res = await env.LOADER.load(DYN).getEntrypoint().fetch('http://dyn/load');
      return new Response('load:' + (await res.text()));
    }
    if (path === '/cache') {
      // get() is lazy: the code callback fires on first stub use, and a
      // second get() for the same name reuses the cached worker without
      // re-running the callback.
      let ran = 0;
      const a = env.LOADER.get('c', () => { ran++; return DYN; });
      const b = env.LOADER.get('c', () => { ran++; return DYN; });
      const ta = await (await a.getEntrypoint().fetch('http://dyn/x')).text();
      const tb = await (await b.getEntrypoint().fetch('http://dyn/x')).text();
      return new Response(`cache:ran=${ran}:${ta}:${tb}`);
    }
    if (path === '/quota') {
      // maxWorkers = 2 on LOADER_A: held stubs keep workers live, so the
      // third anonymous load must throw the fork's quota error.
      const held = [];
      let err = '';
      for (let i = 0; i < 4 && !err; i++) {
        try { held.push(env.LOADER_A.load(DYN)); }
        catch (e) { err = String(e); }
      }
      return new Response(`quota:held=${held.length}:err=${err}`);
    }
    return new Response('ok');
  },
};
"#;

/// Minimal capnp with two `workerLoader` bindings — the same group
/// shape `vrt-config` emits (`(name = X, workerLoader = (maxWorkers =
/// N))`); `LOADER_A` gets a small quota so the test can fill it.
fn write_loader_capnp(dir: &Path, port: u16) -> PathBuf {
    std::fs::write(dir.join("main.js"), LOADER_MAIN_JS).unwrap();
    let cfg = format!(
        r#"using Workerd = import "/workerd/workerd.capnp";
const cfg :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "main.js", esModule = embed "main.js")],
      compatibilityDate = "2025-01-01",
      bindings = [
        (name = "LOADER", workerLoader = (maxWorkers = 32)),
        (name = "LOADER_A", workerLoader = (maxWorkers = 2)),
      ],
      globalOutbound = (name = "deny", entrypoint = "DenyAll"),
    )),
    (name = "deny", worker = (
      modules = [(name = "deny.js", esModule = embed "deny.js")],
      compatibilityDate = "2025-01-01",
    )),
  ],
  sockets = [(name = "http", address = "127.0.0.1:{port}",
              http = (), service = (name = "main"))],
);
"#,
        port = port,
    );
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, cfg).unwrap();
    path
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn real_workerd_worker_loader() {
    let bin = workerd_bin().expect("VRT_WORKERD_BIN unset");
    assert!(bin.exists(), "VRT_WORKERD_BIN does not exist: {bin:?}");

    let Some(dir) = smoke_root() else {
        return;
    };
    write_deny_stub(&dir);
    let port = free_port();
    let cfg = write_loader_capnp(&dir, port);
    // `workerLoader` bindings are experimental-gated upstream;
    // vrt-host's assemble adds `--experimental` to argv for tenants
    // that declare `[[worker_loaders]]` — same flag here.
    let mut proc = spawn_extra(&bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "worker_loader: never listened\n-- workerd.log --\n{}",
            log_text(&proc)
        );
    }

    // A named `get()` loads and serves the dynamic worker through its
    // default entrypoint.
    let (status, body) = http_get(port, "/get");
    assert_eq!(
        (status, body.as_str()),
        (200, "get:dyn:/a"),
        "LOADER.get entrypoint must serve: {body}"
    );

    // An anonymous `load()` serves too.
    let (status, body) = http_get(port, "/load");
    assert_eq!(
        (status, body.as_str()),
        (200, "load:dyn:/load"),
        "LOADER.load entrypoint must serve: {body}"
    );

    // The named cache runs the code callback once per name: both stubs
    // from `get('c', ...)` serve the same loaded worker.
    let (status, body) = http_get(port, "/cache");
    assert_eq!(
        (status, body.as_str()),
        (200, "cache:ran=1:dyn:/x:dyn:/x"),
        "named loader cache must reuse the worker: {body}"
    );

    // The fork's quota: held stubs keep loaded workers live, so the
    // third load on a maxWorkers=2 namespace must throw. The bound is
    // enforced in the loader's kj layer, so the JS caller currently
    // sees workerd's generic `internal error; reference = <id>` rather
    // than a descriptive message — assert the count held at 2 and that
    // the load was rejected, not the wording (a later pin may add one).
    let (status, body) = http_get(port, "/quota");
    assert_eq!(status, 200, "quota probe must answer: {body}");
    let (held, err) = body
        .strip_prefix("quota:held=")
        .and_then(|rest| rest.split_once(":err="))
        .unwrap_or_else(|| panic!("quota probe shape: {body}"));
    assert_eq!(held, "2", "two live workers then quota: {body}");
    assert!(
        !err.is_empty(),
        "the third load on maxWorkers=2 must be rejected: {body}"
    );
    drop(proc);
}

// ---------------------------------------------------------------------------
// unsafe_eval smoke — the `unsafeEval = void` group `vrt-config` emits
// must parse on the pinned binary and actually unlock eval/new
// Function/WebAssembly through `env.<name>` in the holding worker, while a sibling worker
// without the binding keeps all three refused.
// ---------------------------------------------------------------------------

/// The `unsafeEval` binding is an OBJECT exposed as `env.<name>` with
/// `eval`/`newFunction`/`newAsyncFunction`/`newWasmModule` methods — it
/// does NOT re-enable the global `eval()` inside request handlers
/// (workerd keeps that blocked unless `allow_eval_during_startup`
/// applies at module scope). The unbound worker has no `env.EVAL`, so
/// every probe throws `TypeError` and reports `denied`.
const UNSAFE_MAIN_JS: &str = r#"
export default {
  async fetch(_req, env) {
    const probe = (f) => { try { return String(f()); } catch { return 'denied'; } };
    const u = env.EVAL;
    const wasm = new Uint8Array([0,97,115,109,1,0,0,0]);
    const results = {
      eval: probe(() => u.eval('1+1')),
      fn: probe(() => u.newFunction('return 3')()),
      wasm: probe(() => u.newWasmModule(wasm) instanceof WebAssembly.Module),
    };
    return new Response(JSON.stringify(results));
  },
};
"#;

fn write_unsafe_capnp(dir: &Path, port: u16, with_binding: bool) -> PathBuf {
    std::fs::write(dir.join("main.js"), UNSAFE_MAIN_JS).unwrap();
    let binding = if with_binding {
        "bindings = [(name = \"EVAL\", unsafeEval = void)],\n      "
    } else {
        ""
    };
    let cfg = format!(
        r#"using Workerd = import "/workerd/workerd.capnp";
const cfg :Workerd.Config = (
  services = [
    (name = "main", worker = (
      modules = [(name = "main.js", esModule = embed "main.js")],
      compatibilityDate = "2025-01-01",
      {binding}globalOutbound = (name = "deny", entrypoint = "DenyAll"),
    )),
    (name = "deny", worker = (
      modules = [(name = "deny.js", esModule = embed "deny.js")],
      compatibilityDate = "2025-01-01",
    )),
  ],
  sockets = [(name = "http", address = "127.0.0.1:{port}",
              http = (), service = (name = "main"))],
);
"#,
        port = port,
        binding = binding,
    );
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, cfg).unwrap();
    path
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn real_workerd_unsafe_eval() {
    let bin = workerd_bin().expect("VRT_WORKERD_BIN unset");
    assert!(bin.exists(), "VRT_WORKERD_BIN does not exist: {bin:?}");

    // With the binding: all three primitives work.
    let Some(dir) = smoke_root() else {
        return;
    };
    write_deny_stub(&dir);
    let port = free_port();
    let cfg = write_unsafe_capnp(&dir, port, true);
    let mut proc = spawn_extra(&bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "unsafe_eval(bound): never listened\n-- workerd.log --\n{}",
            log_text(&proc)
        );
    }
    let (status, body) = http_get(port, "/");
    assert_eq!(
        (status, body.as_str()),
        (
            200,
            r#"{"eval":"2","fn":"3","wasm":"true"}"#
        ),
        "bound worker must run env.EVAL.eval/newFunction/newWasmModule: {body}"
    );
    drop(proc);

    // Without the binding: the same worker loads fine but all three
    // primitives throw (capability absent, not a parse failure).
    let Some(dir) = smoke_root() else {
        return;
    };
    write_deny_stub(&dir);
    let port = free_port();
    let cfg = write_unsafe_capnp(&dir, port, false);
    let mut proc = spawn_extra(&bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "unsafe_eval(unbound): never listened\n-- workerd.log --\n{}",
            log_text(&proc)
        );
    }
    let (status, body) = http_get(port, "/");
    assert_eq!(
        (status, body.as_str()),
        (
            200,
            r#"{"eval":"denied","fn":"denied","wasm":"denied"}"#
        ),
        "unbound worker must refuse env.EVAL.eval/newFunction/newWasmModule: {body}"
    );
    drop(proc);
}

// ---------------------------------------------------------------------------
// T31f: real-render router smoke (F0.3 real facade-assets + F1.2 custom name)
// ---------------------------------------------------------------------------

/// A backend stub worker answering the exact wire format
/// `vrt-provider/src/assets.rs` emits:
///   POST /internal/assets/v1/route-index → JSON index doc
///   POST /internal/assets/v1/can-fetch  → "true"/"false" text
///   POST /internal/assets/v1/fetch      → asset bytes + rep. headers
/// `<DIGEST>` is replaced with the tenant's manifestDigest so the
/// router's E3 digest check passes.
const BACKEND_STUB: &str = r#"export default {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/internal/assets/v1/route-index") {
      return new Response(JSON.stringify({
        htmlHandling: "none", notFoundHandling: "none",
        manifestDigest: "<DIGEST>", paths: ["/app.js"]
      }), { headers: { "content-type": "application/json" } });
    }
    if (p === "/internal/assets/v1/can-fetch") {
      const u = req.headers.get("x-open-compute-asset-url") || "";
      return new Response(
        u.endsWith("/app.js") ? "true" : "false",
        { headers: { "content-type": "application/json" } });
    }
    if (p === "/internal/assets/v1/fetch") {
      const u = req.headers.get("x-open-compute-asset-url") || "";
      if (!u.endsWith("/app.js")) {
        return new Response("not found", { status: 404,
          headers: { "x-open-compute-error-code": "ASSET_NOT_FOUND" } });
      }
      return new Response("asset-body-from-backend", {
        headers: { "content-type": "text/javascript", etag: '"beef"' } });
    }
    return new Response("unknown endpoint", { status: 400,
      headers: { "x-open-compute-error-code": "BINDING_PROTOCOL_ERROR" } });
  }
}
"#;

/// RuntimeParams over the real facade manifest (same shape as
/// vrt-config's test fixture — the checked-in artifact is the source
/// of truth for system-worker module digests).
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
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        disk_services: manifest.disk_services.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: String::new(),
        wfop_pepper: "entry-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

const USER_MODULE: &str = r#"export default {
  async fetch(req) {
    return new Response("user-worker:" + new URL(req.url).pathname);
  }
}
"#;

/// Render the real config for a single `assetsFirst` worker, rewrite
/// the environment-specific bits (unix sockets → TCP, external
/// backend → stub worker), and materialize every embed the renderer
/// demands. Returns the cfg path.
fn render_and_write(
    dir: &Path,
    assets_binding_name: &str,
    port: u16,
    digest: &str,
    backend_stub_src: &str,
) -> PathBuf {
    let worker_id = "wsmoke";
    let digest = digest.to_string();
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
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-smoke",
        "workers": [{
            "workerId": worker_id,
            "name": "smoke",
            "activeVersion": {
                "versionId": "v-smoke-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [{
                "type": "assets", "name": assets_binding_name,
                "manifestDigest": digest,
                "permissions": {"read": true, "write": false},
                "assetsFirst": true
            }],
            "routes": [{"hostname": "smoke--team-smoke.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
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

    // The render contract surfaces binding-backend's address for
    // `--external-addr`; the smoke replaces the external service with
    // a same-name stub WORKER below, but the render must still carry
    // the production wiring.
    assert!(out
        .external_addrs
        .iter()
        .any(|a| a.service == "binding-backend"));

    // F1 assertions on the REAL emitted text: the router's assets env
    // key is the fixed constant; the user worker's key is the declared
    // name — the two must diverge when a custom name is used.
    assert!(
        out.text
            .contains("(name = \"ASSETS\", service = (name = \"facade-assets\""),
        "router assets binding must use the fixed ASSETS key:\n{}",
        out.text
    );
    assert!(
        out.text.contains(&format!(
            "(name = \"{}\", service = (name = \"facade-assets\"",
            assets_binding_name
        )),
        "user worker assets binding keeps the declared name:\n{}",
        out.text
    );
    assert!(
        out.text.contains("(name = \"USER_WORKER\""),
        "router user-worker binding present"
    );
    assert!(
        out.text.contains("(name = \"VRT_ROUTER_CFG\""),
        "router cfg binding present"
    );

    // Materialize every embed the renderer requires.
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
                // digest → dist bytes from the checked-in manifest.
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
                assert_eq!(
                    got, embed.digest,
                    "embed {service_name}/{module_name} digest drift — \
                     regenerate facade/dist"
                );
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule {
                worker_id: w,
                module_name,
            } => {
                assert_eq!(w, worker_id);
                assert_eq!(module_name, "index.js");
                std::fs::write(&dst, USER_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    // Environment rewrites (documented above): unix socket → TCP,
    // external backend → stub worker answering the provider wire
    // format.
    let mut text = out.text.clone();
    let unix_addr = format!("unix:sock/{worker_id}.sock");
    assert!(
        text.contains(&unix_addr),
        "expected unix socket addr in render:\n{text}"
    );
    text = text.replace(&unix_addr, &format!("127.0.0.1:{port}"));
    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(
        text.contains(external),
        "expected binding-backend external in render:\n{text}"
    );
    std::fs::write(
        dir.join("backend-stub.js"),
        backend_stub_src.replace("<DIGEST>", &digest),
    )
    .unwrap();
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

fn rendered_smoke(bin: &Path, assets_binding_name: &str) {
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        assets_binding_name,
        port,
        &"c".repeat(64),
        BACKEND_STUB,
    );
    let mut proc = spawn_extra(bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "workerd never listened on {port} (binding {assets_binding_name})
-- workerd.log --
{}",
            log_text(&proc)
        );
    }

    // F0.3: real router → real facade-assets → backend stub. Body
    // proves backend provenance, not a stubbed binding.
    let (st, body) = http_get(port, "/app.js");
    assert_eq!(
        (st, body.as_str()),
        (200, "asset-body-from-backend"),
        "asset hit through real facade-assets (binding {assets_binding_name})"
    );
    // Index-miss paths fall back to the user worker.
    for path in ["/api/hits", "/unknown"] {
        let (st, body) = http_get(port, path);
        assert_eq!(
            (st, body.as_str()),
            (200, format!("user-worker:{path}").as_str()),
            "miss → user worker (binding {assets_binding_name})"
        );
    }
    assert_no_actor_warning(&proc);
    drop(proc);
}

// ---------------------------------------------------------------------------
// T31g/G1: cross-request shared-promise semantics under real workerd
// ---------------------------------------------------------------------------

/// Backend stub with a delayed, counted route-index — `<DELAY>` ms via
/// `scheduler.wait` (verified present on the pinned workerd). Every
/// route-index call logs `route-index-call` so the test can assert the
/// dedup fan-in exactly once in the workerd log.
const BACKEND_STUB_TIMED: &str = r#"export default {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/internal/assets/v1/route-index") {
      console.log("route-index-call");
      await scheduler.wait(<DELAY>);
      return new Response(JSON.stringify({
        htmlHandling: "none", notFoundHandling: "none",
        manifestDigest: "<DIGEST>", paths: ["/app.js"]
      }), { headers: { "content-type": "application/json" } });
    }
    if (p === "/internal/assets/v1/can-fetch") {
      const u = req.headers.get("x-open-compute-asset-url") || "";
      return new Response(u.endsWith("/app.js") ? "true" : "false",
        { headers: { "content-type": "application/json" } });
    }
    if (p === "/internal/assets/v1/fetch") {
      const u = req.headers.get("x-open-compute-asset-url") || "";
      if (!u.endsWith("/app.js")) {
        return new Response("not found", { status: 404,
          headers: { "x-open-compute-error-code": "ASSET_NOT_FOUND" } });
      }
      return new Response("asset-body-from-backend", {
        headers: { "content-type": "text/javascript", etag: '"beef"' } });
    }
    return new Response("unknown endpoint", { status: 400,
      headers: { "x-open-compute-error-code": "BINDING_PROTOCOL_ERROR" } });
  }
}
"#;

/// Timed variant: report (status, body, first-byte ms, eof ms).
/// Read-until-EOF — used to measure workerd's socket lifecycle.
fn http_get_timed(port: u16, path: &str) -> (u16, String, u128, u128) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(15)))
        .unwrap();
    write!(
        s,
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    )
    .unwrap();
    let t0 = std::time::Instant::now();
    let mut first_ms: Option<u128> = None;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                if first_ms.is_none() {
                    first_ms = Some(t0.elapsed().as_millis());
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => break,
        }
    }
    let eof_ms = t0.elapsed().as_millis();
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body, first_ms.unwrap_or(eof_ms), eof_ms)
}

/// Fire `n` concurrent GETs and collect (status, body) per request.
fn concurrent_gets(port: u16, path: &'static str, n: usize) -> Vec<(u16, String)> {
    let mut handles = Vec::new();
    for _ in 0..n {
        handles.push(std::thread::spawn(move || http_get(port, path)));
    }
    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

/// workerd-log scan for cross-context / hanging diagnostics — the
/// strings workerd emits for IoContext misuse and promise problems.
/// Returns the matched lines (empty = clean).
fn context_warnings(log: &str) -> Vec<String> {
    log.lines()
        .filter(|l| {
            let l = l.to_lowercase();
            l.contains("different request")
                || l.contains("iocontext")
                || l.contains("on behalf of")
                || l.contains("hanging promise")
                || l.contains("uncaught exception")
                || l.contains("unhandled rejection")
        })
        .map(|l| l.to_string())
        .collect()
}

/// G1.1a — cold-cache fan-in: N concurrent requests on a fresh digest
/// share ONE route-index RPC; all resolve correctly; no cross-context
/// diagnostics in the workerd log.
fn smoke_concurrent_fanin(bin: &Path) {
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        "ASSETS",
        port,
        &"d".repeat(64),
        &BACKEND_STUB_TIMED.replace("<DELAY>", "150"),
    );
    let mut proc = spawn_extra(bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "workerd never listened on {port}
-- workerd.log --
{}",
            log_text(&proc)
        );
    }

    let results = concurrent_gets(port, "/app.js", 10);
    for (i, (st, body)) in results.iter().enumerate() {
        assert_eq!(
            (*st, body.as_str()),
            (200, "asset-body-from-backend"),
            "concurrent request {i} failed"
        );
    }
    let log = log_text(&proc);
    let calls = log.matches("route-index-call").count();
    assert_eq!(
        calls, 1,
        "expected exactly 1 route-index RPC for 10 concurrent requests,          got {calls}
-- workerd.log --
{log}"
    );
    let warns = context_warnings(&log);
    assert!(
        warns.is_empty(),
        "cross-request promise diagnostics in workerd log:
{}",
        warns.join(
            "
"
        )
    );
    drop(proc);
}

/// G1.1b — owner disconnect: the first request's client drops its TCP
/// connection mid-RPC (IoContext cancellation candidate). The 9
/// co-waiters must still resolve correctly and in bounded time; a
/// follow-up request must also succeed. This is where "the promise is
/// owned by a different request's IoContext" bites, if it does.
fn smoke_owner_disconnect(bin: &Path) {
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        "ASSETS",
        port,
        &"e".repeat(64),
        &BACKEND_STUB_TIMED.replace("<DELAY>", "1200"),
    );
    let mut proc = spawn_extra(bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "workerd never listened on {port}
-- workerd.log --
{}",
            log_text(&proc)
        );
    }

    // Request 1: send GET, then disconnect at ~200ms while the
    // route-index RPC (delayed 1200ms) is still in flight.
    {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(
            s,
            "GET /app.js HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        // Drop → client disconnect mid-request.
    }

    // Requests 2-10: attach to the same cold-cache window.
    std::thread::sleep(std::time::Duration::from_millis(100));
    let results = concurrent_gets(port, "/app.js", 9);
    let mut failures = Vec::new();
    for (i, (st, body)) in results.iter().enumerate() {
        if !(*st == 200 && body.as_str() == "asset-body-from-backend") {
            failures.push(format!("req{}: status={} body={}", i + 2, st, body));
        }
    }

    // Request 11: after the storm, a fresh request must still work.
    let (st11, body11) = http_get(port, "/app.js");

    let log = log_text(&proc);
    let calls = log.matches("route-index-call").count();
    let warns = context_warnings(&log);
    eprintln!(
        "G1b semantics: route-index calls={calls},          context_warnings={} | {:?}",
        warns.len(),
        warns
    );

    assert!(
        failures.is_empty(),
        "co-waiters failed after owner disconnect: {failures:?}
         route-index calls: {calls}
-- context warnings --
{}",
        warns.join(
            "
"
        )
    );
    assert_eq!(
        (st11, body11.as_str()),
        (200, "asset-body-from-backend"),
        "request after disconnect storm failed"
    );
    drop(proc);
}

/// H1 — teardown attribution probe: on a warm asset HIT (the
/// request already armed and settled its race timers), the response
/// must arrive promptly while socket EOF follows workerd's fixed
/// ~5s lifecycle grace — measured identical on bare responses with
/// no timers at all. Asserts: first bytes < 1.5s (router not gated
/// on leftover timers), EOF bounded (< 9s) — the teardown value is
/// workerd's, not the router's (T31h/H1 refutation evidence).
fn smoke_teardown_lifecycle(bin: &Path) {
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(&dir, "ASSETS", port, &"b".repeat(64), &BACKEND_STUB);
    let mut proc = spawn_extra(bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "workerd never listened on {port}
-- workerd.log --
{}",
            log_text(&proc)
        );
    }

    // Cold request: cold-cache → route-index RPC raced against an
    // armed timer (cancelled on settle post-H1). Warm request: index
    // cache hit → plain asset fetch, no race timer at all. If the
    // ~5s EOF were caused by armed timers, the two would differ.
    let (st1, body1, first1, eof1) = http_get_timed(port, "/app.js");
    assert_eq!((st1, body1.as_str()), (200, "asset-body-from-backend"));
    let (st2, body2, first2, eof2) = http_get_timed(port, "/app.js");
    assert_eq!((st2, body2.as_str()), (200, "asset-body-from-backend"));
    assert!(
        first1 < 1500 && first2 < 1500,
        "first bytes slow: cold={first1}ms warm={first2}ms"
    );
    assert!(
        eof1 < 9000 && eof2 < 9000,
        "unbounded teardown: cold={eof1}ms warm={eof2}ms"
    );
    eprintln!(
        "H1 probe: cold(first={first1}ms eof={eof1}ms)          warm(first={first2}ms eof={eof2}ms) — equal EOFs show          teardown is workerd's fixed lifecycle, not armed timers"
    );
    drop(proc);
}

/// G1.3 — the route-index RPC is bounded by RPC_BUDGET (5s): a
/// backend that outlives it must surface as a typed 503 with the
/// `route_index_timeout` signal, not an indefinite stall.
fn smoke_route_index_timeout(bin: &Path) {
    let Some(dir) = smoke_root() else {
        return;
    };
    let port = free_port();
    let cfg = render_and_write(
        &dir,
        "ASSETS",
        port,
        &"f".repeat(64),
        &BACKEND_STUB_TIMED.replace("<DELAY>", "8000"),
    );
    let mut proc = spawn_extra(bin, &cfg, &dir, &["--experimental"]);
    if !wait_ready(port, &mut proc) {
        panic!(
            "workerd never listened on {port}
-- workerd.log --
{}",
            log_text(&proc)
        );
    }

    let t0 = std::time::Instant::now();
    let (st, body) = http_get_within(port, "/app.js", std::time::Duration::from_secs(12));
    let elapsed = t0.elapsed();

    assert_eq!(st, 503, "hung route-index must 503, body={body}");
    assert!(
        body.contains("ASSETS_ROUTE_UNAVAILABLE"),
        "fail-closed error body: {body}"
    );
    // Timeout (5s) must fire well before the stub's 8s delay.
    assert!(
        elapsed < std::time::Duration::from_millis(7000),
        "response took {elapsed:?} — timeout did not bound the await"
    );
    let log = log_text(&proc);
    assert!(
        log.contains("ROUTE_INDEX_TIMEOUT"),
        "structured timeout signal missing from log:
{log}"
    );
    drop(proc);
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn real_workerd_router_routes_and_all_entries_load() {
    // RV-R48: an explicitly-run ignored test must never pass vacuously —
    // a missing binary is a hard failure (same contract as
    // runaway_workerd.rs). Run: VRT_WORKERD_BIN=/path/to/workerd cargo test
    // -p vrt-host --test workerd_entry_smoke -- --include-ignored
    let bin = workerd_bin().expect("VRT_WORKERD_BIN unset");
    assert!(bin.exists(), "VRT_WORKERD_BIN does not exist: {bin:?}");

    // ── 1. router end-to-end via the REAL render — both the default
    //    and a custom assets binding name (T31f/F1) ──
    for name in ["ASSETS", "STATIC"] {
        rendered_smoke(&bin, name);
    }

    // ── 1b. G1: shared-promise semantics under real workerd ──
    smoke_concurrent_fanin(&bin);
    smoke_owner_disconnect(&bin);
    smoke_route_index_timeout(&bin);
    smoke_teardown_lifecycle(&bin);

    // ── 2. every other entry module loads and serves ──
    for worker in [
        "facade-kv",
        "facade-r2",
        "facade-assets",
        "facade-pg",
        "vrt-egress",
        "vrt-no-network",
    ] {
        let Some(dir) = smoke_root() else {
            return;
        };
        write_deny_stub(&dir);
        // Port theft: a foreign process can grab the free_port probe's
        // port before our spawn binds — respawn on a fresh port.
        let mut spawned = None;
        let mut last_log = String::new();
        for _ in 0..3 {
            let p_port = free_port();
            let cfg = write_capnp(&dir, worker, &manifest_modules(worker), worker, p_port);
            let mut p = spawn(&bin, &cfg, &dir);
            if wait_ready(p_port, &mut p) {
                spawned = Some((p, p_port));
                break;
            }
            last_log = log_text(&p);
        }
        let Some((proc, port)) = spawned else {
            panic!("{worker}: never listened\n-- workerd.log --\n{last_log}");
        };
        let (status, body) = http_get(port, "/");
        assert!(status > 0, "{worker}: no HTTP response");
        if worker != "vrt-no-network" {
            // The entry executed — not swallowed into globalOutbound.
            assert!(
                !body.contains(DENY_SHAPE),
                "{worker}: request hit DenyAll globalOutbound — \
                 entrypoint never ran (status {status})"
            );
        }
        assert_no_actor_warning(&proc);
        drop(proc);
    }
}
