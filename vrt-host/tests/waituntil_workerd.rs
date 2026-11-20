//! MIG-12 — `ctx.waitUntil` drain semantics on REAL workerd.
//!
//! The supervisor must defer SIGTERM on a draining generation while
//! ITS OWN pending waitUntil count is >0 (bounded by waitUntilGrace),
//! so post-response work (logging, webhooks, writes) is not silently
//! dropped on every deploy switch / scale / stop.
//!
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test waituntil_workerd -- --ignored --nocapture

mod common;

use common::g1::{facade_root, free_port};
use common::*;

static WORKERD_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::spec::{DoStorageSpec, TenantLaunchSpec};
use vrt_host::state::TenantState;
use vrt_host::supervisor::{spawn_supervisor, SupervisorCmd, SupervisorDeps, TenantRuntime};

const W1: &str = "w1";
const W2: &str = "w2";

/// w1 arms a waitUntil: after `ms` it marks the same-generation w2
/// through the declared same-tenant service binding (internal RPC —
/// never touches the egress gate, so the side effect is observable
/// while the draining process still lives).
const MOD_W1: &str = r#"
export default {
  async fetch(req, env, ctx) {
    const u = new URL(req.url);
    if (u.pathname === "/arm") {
      const ms = parseInt(u.searchParams.get("ms") ?? "1500");
      ctx.waitUntil((async () => {
        await new Promise((r) => setTimeout(r, ms));
        await env.SIDE.fetch("http://side/hit");
      })());
      return new Response("armed");
    }
    if (u.pathname === "/hang") {
      ctx.waitUntil(new Promise(() => {}));
      return new Response("hanging");
    }
    // Transparency probes.
    if (u.pathname === "/env") {
      return new Response("var=" + (env.WU_MARKER ?? "missing"));
    }
    if (u.pathname === "/ctx") {
      return new Response(
        "props=" + String(ctx.props && ctx.props.tag) + " hasWait=" +
        (typeof ctx.waitUntil === "function"));
    }
    if (u.pathname === "/ptoe") {
      if (typeof ctx.passThroughOnException !== "function") {
        return new Response("ptoe-missing", { status: 590 });
      }
      ctx.passThroughOnException();
      return new Response("ptoe-called");
    }
    if (u.pathname === "/forge") {
      // Tenant tries to fabricate probe state through its own socket —
      // this path is NOT the probe surface; it must not move the count.
      return Response.json({ pending: 0, wrapped: true });
    }
    return new Response("w1-ok");
  },
};
"#;

/// w2 records hits in isolate-global state, readable on its own socket
/// while the generation is still alive.
const MOD_W2: &str = r#"
export default {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/hit") { globalThis.__hit = (globalThis.__hit ?? 0) + 1; return new Response("marked"); }
    if (u.pathname === "/read") { return new Response(String(globalThis.__hit ?? 0)); }
    return new Response("w2-ok");
  },
};
"#;

/// Class-style default export (WorkerEntrypoint shape) — the wrapper
/// must subclass it and still pass a counting ctx.
const MOD_CLASS: &str = r#"
import { WorkerEntrypoint } from "cloudflare:workers";
export default class extends WorkerEntrypoint {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/arm") {
      const ms = parseInt(u.searchParams.get("ms") ?? "1200");
      this.ctx.waitUntil((async () => {
        await new Promise((r) => setTimeout(r, ms));
        await this.env.SIDE.fetch("http://side/hit");
      })());
      return new Response("armed-class");
    }
    return new Response("class-ok props=" + String(this.ctx.props && this.ctx.props.tag));
  }
}
"#;

struct Rendered {
    spec: TenantLaunchSpec,
}

fn params(dir: &Path) -> RuntimeParams {
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
            request_timeout_ms: 8_000,
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::from([("v1:wu-marker".to_string(), "present".to_string())]),
        secrets: BTreeMap::new(),
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "wu-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: "sock/vrt-wu.sock".into(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

/// Render the REAL capnp for a two-worker tenant with the waitUntil
/// probe surface enabled, then rewrite the three unix sockets to
/// pre-allocated TCP ports (sandbox forbids unix bind — same
/// post-processing contract as runaway/DO smoke tests).
fn render_spec(
    fx: &Fixture,
    p_w1: u16,
    p_w2: u16,
    p_wu: u16,
    gen: u64,
    mod_w1: &str,
    class_w1: bool,
) -> Rendered {
    render_spec_with(fx, p_w1, p_w2, p_wu, gen, mod_w1, class_w1, None)
}

/// `do_class`: declare that class as a SQL-less Durable Object on w1
/// with an `EVLOG` namespace binding (the wrapper must then re-export
/// a ctx-wrapping subclass under the same name).
#[allow(clippy::too_many_arguments)]
fn render_spec_with(
    fx: &Fixture,
    p_w1: u16,
    p_w2: u16,
    p_wu: u16,
    gen: u64,
    mod_w1: &str,
    class_w1: bool,
    do_class: Option<&str>,
) -> Rendered {
    let work_dir = fx.dir.path().join("work").join("t1").join(format!("gen-{gen}"));
    mkdir700(&work_dir);
    let mk = |module: &str| {
        VersionBundle::build(
            DeployManifest::from_pack(&json!({
                "mainModule": "main.js",
                "modules": { "main.js": module },
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            }))
            .unwrap(),
        )
        .unwrap()
    };
    let mut w1_bindings = vec![
        json!({"type": "service", "name": "SIDE", "workerId": W2}),
        json!({"type": "var", "name": "WU_MARKER", "varRef": "v1:wu-marker"}),
    ];
    let mut w1_do = Vec::new();
    if let Some(class) = do_class {
        w1_bindings.push(json!({"type": "durableObject", "name": "EVLOG", "className": class}));
        w1_do.push(json!({
            "className": class,
            "uniqueKey": "e".repeat(64),
            "enableSql": false
        }));
        // workerd's DiskDirectory must exist: `<doStorageDir>/<workerId>`.
        std::fs::create_dir_all(fx.dir.path().join("do").join(W1)).unwrap();
    }
    let b1 = mk(mod_w1);
    let b2 = mk(MOD_W2);
    let tenant: snapshot_protocol::compute_desired::TenantDesired =
        serde_json::from_value(json!({
            "tenantId": "t1",
            "workers": [
                {
                    "workerId": W1, "name": "armed",
                    "activeVersion": {
                        "versionId": "v-w1-1",
                        "artifactDigest": b1.digest(),
                        "compatibilityDate": "2026-09-08",
                        "compatibilityFlags": []
                    },
                    "bindings": w1_bindings,
                    "routes": [{"hostname": "w1.test"}],
                    "crons": [], "queueConsumers": [], "durableObjectClasses": w1_do,
                    "quotas": {"cpuMs": 50, "memoryMb": 128,
                               "subrequests": 100, "outboundConnections": 16}
                },
                {
                    "workerId": W2, "name": "side",
                    "activeVersion": {
                        "versionId": "v-w2-1",
                        "artifactDigest": b2.digest(),
                        "compatibilityDate": "2026-09-08",
                        "compatibilityFlags": []
                    },
                    "bindings": [],
                    "routes": [{"hostname": "w2.test"}],
                    "crons": [], "queueConsumers": [], "durableObjectClasses": [],
                    "quotas": {"cpuMs": 50, "memoryMb": 128,
                               "subrequests": 100, "outboundConnections": 16}
                }
            ]
        }))
        .unwrap();
    let p = params(fx.dir.path());
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[b1.clone(), b2.clone()],
        params: &p,
    })
    .unwrap();

    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest_doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
    for embed in &out.embeds {
        let dst = work_dir.join(&embed.path);
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
                let bytes = std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                assert_eq!(format!("{:x}", Sha256::digest(&bytes)), embed.digest, "embed digest drift");
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule {
                worker_id,
                module_name,
            } => {
                assert_eq!(module_name, "main.js");
                let m = if worker_id == W1 { mod_w1 } else { MOD_W2 };
                std::fs::write(&dst, m).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    // Isolation assertions on the rendered text — the probe surface
    // must be present and private.
    let mut text = out.text.clone();
    assert!(text.contains("vrt-wu-probe"), "probe worker missing:\n{text}");
    assert!(text.contains("VrtWaituntilProbe"), "probe entrypoint missing");
    let wu_unix = "unix:sock/vrt-wu.sock";
    assert!(text.contains(wu_unix), "waituntil socket missing:\n{text}");
    for (sock, port) in [
        ("unix:sock/w1.sock", p_w1),
        ("unix:sock/w2.sock", p_w2),
        (wu_unix, p_wu),
    ] {
        assert!(text.contains(sock), "{sock} missing:\n{text}");
        text = text.replace(sock, &format!("127.0.0.1:{port}"));
    }
    // MIG-13-r4: a DO-carrying render also emits the `vrt-switch`
    // swap marker — bound to the probe service in the probe's own
    // dir. It must be rewritten to TCP like every other unix bind:
    // workerd has no cwd, `sock/` does not exist, and the relative
    // bind was the r4 gate's crash loop.
    let marker_unix = format!("unix:sock/{}", vrt_config::capnp::SWITCH_MARKER_FILE);
    if do_class.is_some() {
        assert!(
            text.contains(&format!(
                "name = \"{}\", address = \"{marker_unix}\"",
                vrt_config::capnp::SWITCH_MARKER_SOCKET
            )),
            "marker must exist beside the probe socket:\n{text}"
        );
        text = text.replace(&marker_unix, &format!("127.0.0.1:{}", free_port()));
    } else {
        assert!(
            !text.contains(&marker_unix),
            "no DO storage must mean no marker:\n{text}"
        );
    }
    let _ = class_w1;

    let config_path = work_dir.join("config.capnp");
    std::fs::write(&config_path, &text).unwrap();
    if std::env::var("WU_DUMP").is_ok() {
        eprintln!("=== config {gen} ===\n{text}");
    }
    let spec = TenantLaunchSpec {
        tenant_id: "t1".into(),
        binary: PathBuf::from(std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset")),
        config_path,
        config_digest: format!("wu-{gen}"),
        work_dir,
        external_addrs: out
            .external_addrs
            .iter()
            .map(|a| vrt_config::ExternalAddr {
                service: a.service.clone(),
                address: "127.0.0.1:1".into(),
            })
            .collect(),
        env: BTreeMap::new(),
        base_env: BTreeMap::new(),
        worker_endpoints: BTreeMap::from([
            (
                W1.to_string(),
                WorkerEndpoint::Tcp(format!("127.0.0.1:{p_w1}").parse().unwrap()),
            ),
            (
                W2.to_string(),
                WorkerEndpoint::Tcp(format!("127.0.0.1:{p_w2}").parse().unwrap()),
            ),
        ]),
        qconsumer_endpoints: BTreeMap::new(),
        wfops_endpoints: BTreeMap::new(),
        cron_endpoints: BTreeMap::new(),
        wfop_tokens: BTreeMap::new(),
        limits: vrt_host::spec::ResourceLimits::default(),
        heartbeat_endpoint: None,
        waituntil_endpoint: Some(WorkerEndpoint::Tcp(
            format!("127.0.0.1:{p_wu}").parse().unwrap(),
        )),
        extra_args: vec!["--experimental".into()],
        relaunch: None,
        do_storage: None,
        generation: gen,
        isolation: None,
    };
    Rendered { spec }
}

/// Raw HTTP GET against a workerd service socket (bypasses ingress —
/// reads a draining generation's live state directly).
fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

async fn get(port: u16, path: &str) -> Option<(u16, String)> {
    get_dbg(port, path, false).await
}

async fn get_dbg(port: u16, path: &str, dbg: bool) -> Option<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = match tokio::net::TcpStream::connect(std::net::SocketAddr::from((
        [127, 0, 0, 1],
        port,
    )))
    .await
    {
        Ok(s) => s,
        Err(e) => {
            if dbg {
                eprintln!("get {port}{path}: connect {e}");
            }
            return None;
        }
    };
    let req = format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n");
    if let Err(e) = s.write_all(req.as_bytes()).await {
        if dbg {
            eprintln!("get {port}{path}: write {e}");
        }
        return None;
    }
    // workerd keeps the connection alive regardless of the request's
    // `connection: close` — read until headers + Content-Length bytes
    // are in, never to EOF.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let complete = |b: &[u8]| -> bool {
        let Some(h) = find_sub(b, b"\r\n\r\n") else {
            return false;
        };
        let head = &b[..h];
        let Some(cl) = find_sub(head, b"Content-Length:").map(|i| {
            let line_end = find_sub(&head[i..], b"\r\n").unwrap_or(head.len() - i);
            String::from_utf8_lossy(&head[i + 15..i + line_end])
                .trim()
                .parse::<usize>()
                .unwrap_or(0)
        }) else {
            return true; // no Content-Length — headers alone are complete
        };
        b.len() >= h + 4 + cl
    };
    loop {
        match tokio::time::timeout(Duration::from_secs(3), s.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) if complete(&buf) => break,
            // Peer closed mid-response: EOF with a partial body must NOT
            // fall into the data arm — a zero-length extend never
            // completes `buf`, spinning the read loop forever (seen as
            // 15 min binary hangs in CI). Fail the probe; callers retry.
            Ok(Ok(0)) => return None,
            Ok(Ok(n)) => {
                buf.extend_from_slice(&chunk[..n]);
                if complete(&buf) {
                    break;
                }
            }
            Ok(Err(e)) => {
                if dbg {
                    eprintln!("get {port}{path}: read {e}");
                }
                return None;
            }
            Err(_) => {
                if dbg {
                    eprintln!("get {port}{path}: timeout, partial={}", String::from_utf8_lossy(&buf));
                }
                return None;
            }
        }
    }
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = match text.split_whitespace().nth(1).and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => {
            if dbg {
                eprintln!("get {port}{path}: unparseable reply {:?}", &text[..text.len().min(300)]);
            }
            return None;
        }
    };
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Some((status, body))
}

fn wu_params() -> vrt_host::supervisor::SupervisorParams {
    let mut p = test_params();
    p.wait_until_grace = Duration::from_secs(8);
    p.drain_timeout = Duration::from_secs(15);
    p.kill_grace = Duration::from_millis(300);
    // Real `workerd check` can stall past the 5 s test default on a loaded CI runner.
    p.preflight_timeout = Duration::from_secs(20);
    p
}

fn workerd_bin_or_skip() -> String {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(b) if Path::new(&b).exists() => b,
        _ => panic!("VRT_WORKERD_BIN must point at a native workerd binary"),
    }
}

/// THE MIG-12 core gate: 200 → waitUntil(1.5 s) → side effect through
/// an internal service binding. Mid-wait, a same-tenant generation
/// switch runs — the side effect MUST be delivered and the new
/// generation must serve without paying the wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn waituntil_side_effect_survives_generation_switch() {
    init_logs();
    let _b = workerd_bin_or_skip();
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let (p1_w1, p1_w2, p1_wu) = (free_port(), free_port(), free_port());
    let r1 = render_spec(&fx, p1_w1, p1_w2, p1_wu, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    // The armed waitUntil must outlive ANY gen2 bringup — the
    // `workerd check` preflight alone may burn its whole 20 s bound on a
    // loaded CI runner (observed >15 s) — so "gen2 served while gen1
    // still had it pending" stays an ordering fact, not a wall-clock
    // race. `drain_timeout` is the TOTAL cap on the deferred-SIGTERM
    // wait and `wait_until_grace` the idle bound: both must exceed the
    // arm plus delivery slack or the gate kills gen1 before the side
    // effect lands.
    const ARM_MS: u64 = 45_000;
    let mut params = wu_params();
    params.wait_until_grace = Duration::from_secs(75);
    params.drain_timeout = Duration::from_secs(75);
    let h = spawn_supervisor(rt.clone(), r1.spec, SupervisorDeps::default(), params);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    // Arm the long waitUntil, then immediately switch generations.
    let (st, body) = get(p1_w1, &format!("/arm?ms={ARM_MS}")).await.expect("arm reply");
    assert_eq!((st, body.as_str()), (200, "armed"));

    let t_switch = Instant::now();
    let (p2_w1, p2_w2, p2_wu) = (free_port(), free_port(), free_port());
    let r2 = render_spec(&fx, p2_w1, p2_w2, p2_wu, 2, MOD_W1, false);
    h.send(SupervisorCmd::Apply(Box::new(r2.spec))).await;

    // New generation must serve without waiting out the old waitUntil —
    // the deferral lives only on the already-offline old generation.
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let deadline2 = t_switch + Duration::from_millis(ARM_MS);
    loop {
        if let Some((200, _)) = get(p2_w1, "/").await {
            break;
        }
        assert!(Instant::now() < deadline2, "gen2 must serve");
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let switch_latency = t_switch.elapsed();

    // Gen2 serves while the old waitUntil is still pending: the switch did
    // not wait for it. Both probes are mandatory — a dead gen1 or a landed
    // side effect here means the switch was held behind the drain.
    let (st, body) = get(p1_wu, "/vrt/waituntil")
        .await
        .unwrap_or_else(|| panic!("gen1 must still drain when gen2 serves ({switch_latency:?})"));
    assert_eq!(st, 200, "probe must answer: {body}");
    assert!(
        body.contains("\"pending\":1"),
        "switch must not wait for the old waitUntil ({switch_latency:?}): {body}"
    );
    let (_, early) = get(p1_w2, "/read").await.expect("side counter reachable");
    assert_eq!(early.trim(), "0", "side effect landed before gen2 served ({switch_latency:?})");

    // THE GATE: the deferred SIGTERM lets the waitUntil complete — the
    // side effect must land on w2 while gen1 is still alive.
    let deadline = t_switch + Duration::from_millis(ARM_MS) + Duration::from_secs(10);
    let mut hits = String::new();
    while Instant::now() < deadline {
        if let Some((200, b)) = get(p1_w2, "/read").await {
            hits = b;
            if hits.trim() != "0" {
                break;
            }
        } else {
            // Socket dead = gen1 exited before the side effect → drop.
            break;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    assert_eq!(hits.trim(), "1", "waitUntil side effect must be delivered");

    // Tenant forgery: the worker's own `/vrt/waituntil`-shaped route is
    // its own code — it cannot move the platform counter.
    let (fst, _) = get(p1_w1, "/forge").await.unwrap_or((0, String::new()));
    assert_eq!(fst, 200, "tenant route serves its own code");
    let (_, body) = get(p1_wu, "/vrt/waituntil").await.unwrap();
    assert!(
        body.contains("\"pending\":0") && body.contains("\"failed\":0"),
        "forged tenant output must not move the platform count: {body}"
    );

    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}

/// A waitUntil that never resolves is dropped after waitUntilGrace —
/// SIGTERM lands and `vrt_host_waituntil_dropped_total` counts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn waituntil_never_resolving_dropped_after_grace() {
    init_logs();
    let _b = workerd_bin_or_skip();
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let (p1_w1, p1_w2, p1_wu) = (free_port(), free_port(), free_port());
    let r1 = render_spec(&fx, p1_w1, p1_w2, p1_wu, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    let mut params = wu_params();
    params.wait_until_grace = Duration::from_millis(1200);
    let h = spawn_supervisor(rt.clone(), r1.spec, SupervisorDeps::default(), params);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, _) = get(p1_w1, "/hang").await.expect("hang reply");
    assert_eq!(st, 200);

    let (p2_w1, p2_w2, p2_wu) = (free_port(), free_port(), free_port());
    let r2 = render_spec(&fx, p2_w1, p2_w2, p2_wu, 2, MOD_W1, false);
    h.send(SupervisorCmd::Apply(Box::new(r2.spec))).await;

    // Past grace + kill_grace the old probe socket must be dead — the
    // generation was SIGTERMed (then reaped) despite pending=1.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut dead = false;
    while Instant::now() < deadline {
        if get(p1_wu, "/vrt/waituntil").await.is_none() {
            dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert!(dead, "hung waitUntil gen must be reaped after grace");
    let rendered = vrt_host::metrics::registry().render();
    assert!(
        rendered.contains("vrt_host_waituntil_dropped_total{reason=\"grace_expired\"}"),
        "grace_expired drop must be counted: {rendered}"
    );

    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}

/// Wrapper transparency: object default export env/ctx/ctx.props,
/// passThroughOnException, and a WorkerEntrypoint class default export
/// all behave exactly as unwrapped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn waituntil_wrapper_is_transparent() {
    init_logs();
    let _b = workerd_bin_or_skip();
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let (p_w1, p_w2, p_wu) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, p_w1, p_w2, p_wu, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), wu_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, body) = get_dbg(p_w1, "/env", true).await.unwrap();
    assert_eq!((st, body.as_str()), (200, "var=present"), "env must pass through");
    let (st, body) = get(p_w1, "/ctx").await.unwrap();
    assert_eq!(st, 200);
    assert!(body.contains("hasWait=true"), "ctx.waitUntil stays callable: {body}");
    // passThroughOnException stays callable on the wrapped ctx.
    let (st, body) = get(p_w1, "/ptoe").await.unwrap();
    assert_eq!((st, body.as_str()), (200, "ptoe-called"), "ptoe must pass through");
    // The probe itself reports a successfully-wrapped export.
    let (st, body) = get(p_wu, "/vrt/waituntil").await.unwrap();
    assert_eq!(st, 200);
    assert!(body.contains("\"pending\":0"), "{body}");

    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;

    // Class default export gets the same treatment in a fresh gen.
    let fx2 = Fixture::new();
    let (c_w1, c_w2, c_wu) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx2, c_w1, c_w2, c_wu, 1, MOD_CLASS, true);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), wu_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let (st, body) = get(c_w1, "/").await.unwrap();
    assert_eq!(st, 200, "class default export serves: {body}");
    let (st, body) = get(c_wu, "/vrt/waituntil").await.unwrap();
    assert_eq!(st, 200, "class worker probe answers: {body}");
    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}

/// MIG-12-r2: serialized DO switch — the old generation's waitUntil
/// wait is truncated at `do_switch_wait_until_grace`, NOT the global
/// `wait_until_grace`, because its drain extends the switch-hold
/// window directly. A never-resolving waitUntil must be dropped at
/// ~500 ms (not 8 s) and the whole switch must still complete: the
/// drop is counted `do_switch_grace_expired`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn waituntil_do_switch_truncates_within_hold() {
    init_logs();
    let _b = workerd_bin_or_skip();
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let do_dir = fx.dir.path().join("do-root").join("t1");
    mkdir700(&do_dir);
    let mk_do = |spec: &mut TenantLaunchSpec| {
        spec.do_storage = Some(DoStorageSpec {
            tenant_dir: do_dir.clone(),
            worker_classes: BTreeMap::new(),
            transfer_sources: BTreeMap::new(),
        });
    };

    let (p1_w1, p1_w2, p1_wu) = (free_port(), free_port(), free_port());
    let mut r1 = render_spec(&fx, p1_w1, p1_w2, p1_wu, 1, MOD_W1, false);
    mk_do(&mut r1.spec);
    let rt = TenantRuntime::new("t1");
    let mut params = wu_params();
    params.wait_until_grace = Duration::from_secs(8);
    params.do_switch_wait_until_grace = Duration::from_millis(500);
    let h = spawn_supervisor(rt.clone(), r1.spec, SupervisorDeps::default(), params);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    // Arm a waitUntil that NEVER resolves — absent the do-switch bound
    // it would pin the old generation for the full 8 s global grace.
    let (st, _) = get(p1_w1, "/hang").await.expect("hang reply");
    assert_eq!(st, 200);

    let t_switch = Instant::now();
    let (p2_w1, p2_w2, p2_wu) = (free_port(), free_port(), free_port());
    let mut r2 = render_spec(&fx, p2_w1, p2_w2, p2_wu, 2, MOD_W1, false);
    mk_do(&mut r2.spec);
    h.send(SupervisorCmd::Apply(Box::new(r2.spec))).await;

    // The old probe socket must die near the 500 ms do-switch bound —
    // nowhere near the 8 s global grace.
    let mut dead_at = None;
    let probe_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < probe_deadline {
        if get(p1_wu, "/vrt/waituntil").await.is_none() {
            dead_at = Some(t_switch.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let dead_at = dead_at.expect("old gen must be reaped near the do-switch grace");
    assert!(
        dead_at < Duration::from_secs(4),
        "do-switch truncation must reap well inside the global grace: {dead_at:?}"
    );

    // The serialized switch itself completes — gen2 launches and
    // serves (requests would otherwise 503 past the hold window).
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let serve_deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some((200, _)) = get(p2_w1, "/").await {
            break;
        }
        assert!(Instant::now() < serve_deadline, "gen2 must serve");
        tokio::time::sleep(Duration::from_millis(60)).await;
    }

    let rendered = vrt_host::metrics::registry().render();
    assert!(
        rendered
            .contains("vrt_host_waituntil_dropped_total{reason=\"do_switch_grace_expired\"}"),
        "do_switch_grace_expired drop must be counted: {rendered}"
    );

    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}

/// DO extending the native `DurableObject` base (the production
/// regression: a Proxy ctx fails workerd's constructor type check).
const MOD_DO: &str = r#"
import { DurableObject } from "cloudflare:workers";
export class EventLog extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.tag = "ctor-ok";
  }
  async arm(ms) {
    this.ctx.waitUntil(new Promise((r) => setTimeout(r, ms)));
    return "armed-do";
  }
  async hello() { return "hello " + this.tag; }
}
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const stub = env.EVLOG.get(env.EVLOG.idFromName("a"));
    if (u.pathname === "/do-arm") {
      return new Response(await stub.arm(parseInt(u.searchParams.get("ms") ?? "1500")));
    }
    if (u.pathname === "/do-hello") return new Response(await stub.hello());
    return new Response("do-host-ok");
  },
};
"#;

/// Default export is `class extends WorkerEntrypoint` using
/// `this.ctx.waitUntil` — a native-base subclass, not a plain class.
const MOD_WE: &str = r#"
import { WorkerEntrypoint } from "cloudflare:workers";
export default class extends WorkerEntrypoint {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/arm") {
      const ms = parseInt(u.searchParams.get("ms") ?? "1500");
      this.ctx.waitUntil(new Promise((r) => setTimeout(r, ms)));
      return new Response("armed-we");
    }
    return new Response("we-ok");
  }
}
"#;

fn pending_of(body: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    v["pending"].as_u64()
}

/// Poll the platform probe until `pending` matches `want` (or panic).
async fn expect_pending(p_wu: u16, want: impl Fn(u64) -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Some((200, body)) = get(p_wu, "/vrt/waituntil").await {
            if body.contains("\"failed\":0") && pending_of(&body).is_some_and(&want) {
                return;
            }
            last = body;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    panic!("{what}: probe never matched, last={last}");
}

/// Regression (MIG-12 r4): a DO `extends DurableObject` must construct
/// (workerd type-checks ctor param 1 — a Proxy ctx is rejected), its
/// methods must run, and `this.ctx.waitUntil` must be counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn waituntil_durable_object_extends_native_base() {
    init_logs();
    let _b = workerd_bin_or_skip();
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let (p_w1, p_w2, p_wu) = (free_port(), free_port(), free_port());
    let r = render_spec_with(&fx, p_w1, p_w2, p_wu, 1, MOD_DO, false, Some("EventLog"));
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), wu_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, body) = get(p_w1, "/do-hello").await.expect("do hello reply");
    assert_eq!(
        (st, body.as_str()),
        (200, "hello ctor-ok"),
        "DO extending DurableObject must construct and run: {body}"
    );
    expect_pending(p_wu, |n| n == 0, "baseline").await;

    let (st, body) = get(p_w1, "/do-arm?ms=1500").await.expect("do arm reply");
    assert_eq!((st, body.as_str()), (200, "armed-do"));
    expect_pending(p_wu, |n| n >= 1, "pending while DO waitUntil is open").await;
    expect_pending(p_wu, |n| n == 0, "pending after DO waitUntil settles").await;

    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}

/// Regression (MIG-12 r4): default export `class extends
/// WorkerEntrypoint` using `this.ctx.waitUntil` serves 200 and counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn waituntil_worker_entrypoint_class_counts() {
    init_logs();
    let _b = workerd_bin_or_skip();
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let (p_w1, p_w2, p_wu) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, p_w1, p_w2, p_wu, 1, MOD_WE, true);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), wu_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, body) = get(p_w1, "/").await.expect("we reply");
    assert_eq!((st, body.as_str()), (200, "we-ok"));
    expect_pending(p_wu, |n| n == 0, "baseline").await;

    // Two concurrent armed requests: each request gets its own ctx and
    // both must be counted, then both must drain back to 0.
    let (a, b) = tokio::join!(get(p_w1, "/arm?ms=1500"), get(p_w1, "/arm?ms=1500"));
    assert_eq!(a.expect("arm a"), (200, "armed-we".to_string()));
    assert_eq!(b.expect("arm b"), (200, "armed-we".to_string()));
    expect_pending(p_wu, |n| n >= 2, "both waitUntils counted").await;
    expect_pending(p_wu, |n| n == 0, "no count leak after settle").await;

    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}
