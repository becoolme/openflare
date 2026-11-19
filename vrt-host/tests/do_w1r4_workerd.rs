//! W1-r4: the standalone e2e failure reproduced WITHOUT docker —
//! a REAL workerd generation launched BY THE SUPERVISOR (not a
//! direct `workerd serve`), over the REAL placement backend
//! (`ManifestDoPlacement` + `DoSnapCtx`, the standalone fs-path
//! shape on MemoryStore's identical CAS semantics), driven through
//! install → monitor sweep → graceful Stop → re-spawn as a NEW
//! holder — the exact `docker restart` sequence.
//!
//! What this proves end-to-end that `real_workerd_workflow` (direct
//! spawn) cannot: the Engine class's localDisk dir rides the same
//! acquire/mark/sweep/release path as user classes, survives the
//! sweep, and a sleeping workflow instance resumes to `complete`
//! after a supervisor-managed restart — provided leases are
//! gracefully released (test 2 in do_w1r4.rs shows why the restart
//! used to stall ~leaseMs fencing a dead holder's live lease).
//!
//! Gated on `VRT_WORKERD_BIN` — explicit skip otherwise:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test do_w1r4_workerd -- --include-ignored --nocapture

mod common;

use common::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_host::do_placement_store::ManifestDoPlacement;
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{DoSnapCtx, SupervisorCmd, SupervisorDeps, TenantRuntime};
use vrt_objstore::{MemoryStore, ObjectStore};

/// Real-workerd tests in this file serialize: parallel generations of
/// cold-start workerd processes starve the 20s Ready budget.
static WORKERD_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const WORKER_ID: &str = "w-wfsup";
const WF_UKEY: &str = "9999999999999999999999999999999999999999999999999999999999999999";
const LEASE_MS: u64 = 5_000;
const MARGIN_MS: u64 = 500;

/// The example workflow — `boom` retries once, `sleep` gives the
/// restart window; identical semantics to examples/standalone-workflow.
const WF_MODULE: &str = r#"import { WorkflowEntrypoint } from "cloudflare:workers";
let boomAttempts = 0;
export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    const a = await step.do("first", async () => "a");
    const b = await step.do("boom",
      { retries: { limit: 3, delay: "50 milliseconds", backoff: "constant" } },
      async () => { boomAttempts += 1; if (boomAttempts === 1) throw new Error("boom-once"); return "b"; });
    await step.sleep("pause", "2 seconds");
    return `${a}-${b}-done`;
  }
}
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ping") return new Response("wf-ok");
    if (url.pathname === "/create") { const i = await env.WF.create(); return Response.json({ id: i.id }); }
    if (url.pathname === "/status") { const i = await env.WF.get(url.searchParams.get("id")); return Response.json(await i.status()); }
    return new Response("nf", { status: 404 });
  }
};
"#;

/// r5 variant — a sleep long enough that the ~4s graceful drain ends
/// BEFORE the deadline, so the deadline provably passes while no
/// workerd process exists (the e2e `docker restart` window).
const WF_MODULE_SLOW: &str = r#"import { WorkflowEntrypoint } from "cloudflare:workers";
let boomAttempts = 0;
export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    const a = await step.do("first", async () => "a");
    const b = await step.do("boom",
      { retries: { limit: 3, delay: "50 milliseconds", backoff: "constant" } },
      async () => { boomAttempts += 1; if (boomAttempts === 1) throw new Error("boom-once"); return "b"; });
    console.log("WFDBG before-sleep", Date.now(), "typeof-scheduler", typeof scheduler);
    const _t = Date.now();
    await step.sleep("pause", "20 seconds");
    console.log("WFDBG after-sleep", Date.now(), "elapsed", Date.now() - _t);
    return `${a}-${b}-done`;
  }
}
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ping") return new Response("wf-ok");
    if (url.pathname === "/create") { const i = await env.WF.create(); return Response.json({ id: i.id }); }
    if (url.pathname === "/status") { const i = await env.WF.get(url.searchParams.get("id")); return Response.json(await i.status()); }
    return new Response("nf", { status: 404 });
  }
};
"#;

/// W2 P0-0 probe — the run parks in a 1s gate sleep so a restart
/// mid-flight aborts the actor; `__ex*` count step executions across
/// every run in the isolate and `__steps` records which run executed
/// each step, both embedded in the return value. Pre-fix: two
/// concurrent restarts run `attemptRestart` → bare `init` with no
/// single-flight — the interleaved engines collide on
/// `priority_queue`'s UNIQUE(action, entryType, hash) and the
/// instance errors out. Post-fix: the durable restart flag routes
/// every entry point through one deduped attemptRestart claim →
/// `e1-1`.
const WF_MODULE_RESTART: &str = r#"import { WorkflowEntrypoint } from "cloudflare:workers";
export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    globalThis.__live = (globalThis.__live || 0) + 1;
    globalThis.__maxLive = Math.max(globalThis.__maxLive || 0, globalThis.__live);
    const myRun = globalThis.__runSeq = (globalThis.__runSeq || 0) + 1;
    globalThis.__steps = globalThis.__steps || [];
    try {
      // Parks the run inside a named sleep with ZERO step.do
      // executions — gen-1's contribution to the counters is always 0,
      // so post-fix the restarted run is the only executor.
      await step.sleep("gate", "1 second");
      const a = await step.do("first", async () => {
        globalThis.__ex1 = (globalThis.__ex1 || 0) + 1;
        globalThis.__steps.push(`r${myRun}f@${Date.now()}`);
        await scheduler.wait(300);
        return "a";
      });
      const b = await step.do("second", async () => {
        globalThis.__ex2 = (globalThis.__ex2 || 0) + 1;
        globalThis.__steps.push(`r${myRun}s@${Date.now()}`);
        await scheduler.wait(300);
        return "b";
      });
      const m = await step.do("mark", async () =>
        `m${globalThis.__maxLive}-e${globalThis.__ex1}-${globalThis.__ex2}-[${globalThis.__steps.join(",")}]`);
      return `${a}-${b}-${m}`;
    } finally {
      globalThis.__live -= 1;
    }
  }
}
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ping") return new Response("wf-ok");
    if (url.pathname === "/create") { const i = await env.WF.create(); return Response.json({ id: i.id }); }
    if (url.pathname === "/status") { const i = await env.WF.get(url.searchParams.get("id")); return Response.json(await i.status()); }
    if (url.pathname === "/restart") { const i = await env.WF.get(url.searchParams.get("id")); await i.restart(); return Response.json({ ok: true }); }
    return new Response("nf", { status: 404 });
  }
};
"#;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn params(do_dir: &Path) -> RuntimeParams {
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
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: do_dir.to_string_lossy().into_owned(),
        wfop_pepper: "do-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

struct Rendered {
    tenant_dir: PathBuf,
    spec: vrt_host::spec::TenantLaunchSpec,
}

/// Render the REAL capnp for the workflow tenant, materialize embeds,
/// rewrite the single unix socket to a pre-allocated TCP port — same
/// post-processing contract as workerd_entry_smoke (what is under
/// test is storage lifecycle, not the address family). `gen` makes
/// each attempt's work dir unique like the production assembler.
fn render_wf_spec(fx: &Fixture, port: u16, gen: u64, module: &str, compat: &str) -> Rendered {
    let work_dir = fx
        .dir
        .path()
        .join("work")
        .join("t1")
        .join(format!("gen-{gen}"));
    mkdir700(&work_dir);
    let tenant_dir = fx.dir.path().join("do").join("t1");
    mkdir700(&tenant_dir.join(WORKER_ID));

    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "main.js",
            "modules": { "main.js": module },
            "compatibilityDate": compat,
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "t1",
        "workers": [{
            "workerId": WORKER_ID,
            "name": "wf",
            "activeVersion": {
                "versionId": "v-wf-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": compat,
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": "wf.test"}],
            "crons": [], "queueConsumers": [], "durableObjectClasses": [],
            "workflows": [{
                "name": "order-flow", "binding": "WF",
                "className": "OrderFlow", "uniqueKey": WF_UKEY
            }],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let p = params(&tenant_dir);
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &p,
    })
    .unwrap();

    // Materialize every embed the render pins.
    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
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
                let bytes =
                    std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                let got = format!("{:x}", Sha256::digest(&bytes));
                assert_eq!(got, embed.digest, "embed digest drift");
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { module_name, .. } => {
                assert_eq!(module_name, "main.js");
                std::fs::write(&dst, module).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    // unix sockets → pre-allocated TCP ports (sandbox forbids unix
    // bind; the listen event reports port only, and a nonzero spec
    // port resolves without an event address — ready.rs). The wfops
    // endpoint also gets its own port: nothing connects to it in these
    // tests, but workerd must still bind it — an unbound `unix:sock/`
    // path with a missing parent dir kills the whole generation
    // (the W3-r2 standalone cascade).
    let mut text = out.text.clone();
    let wfops_addr = format!("unix:sock/{WORKER_ID}-wfops.sock");
    assert!(
        text.contains(&wfops_addr),
        "wfops socket addr missing:\n{text}"
    );
    let wfops_port = free_port();
    text = text.replace(&wfops_addr, &format!("127.0.0.1:{wfops_port}"));
    let unix_addr = format!("unix:sock/{WORKER_ID}.sock");
    assert!(
        text.contains(&unix_addr),
        "unix socket addr missing:\n{text}"
    );
    text = text.replace(&unix_addr, &format!("127.0.0.1:{port}"));

    let config_path = work_dir.join("config.capnp");
    std::fs::write(&config_path, &text).unwrap();
    let mut worker_classes = BTreeMap::new();
    worker_classes.insert(WORKER_ID.to_string(), vec![WF_UKEY.to_string()]);
    let spec = vrt_host::spec::TenantLaunchSpec {
        tenant_id: "t1".into(),
        binary: PathBuf::from(std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset")),
        config_path,
        config_digest: format!("wf-{gen}"),
        work_dir: work_dir.clone(),
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
        worker_endpoints: BTreeMap::from([(
            WORKER_ID.to_string(),
            WorkerEndpoint::Tcp(format!("127.0.0.1:{port}").parse().unwrap()),
        )]),
        qconsumer_endpoints: BTreeMap::new(),
        // The supervisor's ready check rejects any `sock-wfops-*`
        // listen event not declared here (`unexpected_socket`) — the
        // wfops endpoint must be registered even though these tests
        // never drive ops through it.
        wfops_endpoints: BTreeMap::from([(
            WORKER_ID.to_string(),
            WorkerEndpoint::Tcp(format!("127.0.0.1:{wfops_port}").parse().unwrap()),
        )]),
        cron_endpoints: BTreeMap::new(),
        // These tests never drive `/v1/wfops` — an empty token map is
        // correct (the endpoint exists only to satisfy the ready-check
        // socket-declaration contract).
        wfop_tokens: BTreeMap::new(),
        limits: vrt_host::spec::ResourceLimits::default(),
        heartbeat_endpoint: None,
        waituntil_endpoint: None,
        extra_args: vec!["--experimental".into()],
        relaunch: None,
        do_storage: Some(vrt_host::spec::DoStorageSpec {
            tenant_dir: tenant_dir.clone(),
            worker_classes,
            transfer_sources: Default::default(),
        }),
        generation: gen,
        isolation: None,
    };
    Rendered { tenant_dir, spec }
}

fn deps(store: Arc<dyn ObjectStore>, plc: Arc<ManifestDoPlacement>, tmp: &Path) -> SupervisorDeps {
    let mut d = SupervisorDeps::default();
    d.do_snap = Some(Arc::new(DoSnapCtx {
        store,
        placement: plc.clone(),
        tmp_root: tmp.to_path_buf(),
        interval_ms: 3_600_000,
        jitter_ms: 0,
        departing_max_ms: 60_000,
        xfer_quarantine_ms: 7 * 24 * 3600 * 1000,
        departing_retry_base_ms: 50,
        departing_retry_cap_ms: 500,
        wal: None,
    }));
    d.placement = plc;
    d
}

fn holder(tag: &str, store: Arc<dyn ObjectStore>) -> Arc<ManifestDoPlacement> {
    Arc::new(ManifestDoPlacement::new(
        store,
        format!("cell:test:{tag}"),
        LEASE_MS,
        MARGIN_MS,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn workflow_resumes_through_supervisor_restart() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let port_a = free_port();
    let r1 = render_wf_spec(&fx, port_a, 1, WF_MODULE, "2026-09-08");
    let tenant_dir = r1.tenant_dir.clone();
    let marker = tenant_dir
        .join(WORKER_ID)
        .join(format!("{WF_UKEY}.lease-epoch"));
    let engine_dir = tenant_dir.join(WORKER_ID).join(WF_UKEY);

    // --- holder A: install → Ready → create a sleeping instance ---
    let plc_a = holder("a", store.clone());
    let h1 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r1.spec,
        deps(
            store.clone(),
            plc_a.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt1 = h1.runtime();
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(20)).await;

    // The sweep already ran post-install; give it a monitor tick too —
    // engine storage + its lease marker must both survive.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(marker.exists(), "engine .lease-epoch swept by monitor pass");
    let (st, _, body) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
        "wf.test",
        "/create",
        &[],
    )
    .await;
    assert_eq!(st, 200, "create failed: {body}");
    let inst = body
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("instance id")
        .to_string();
    // Land the restart INSIDE the 2s sleep window.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- graceful stop (docker restart's SIGTERM path) ---
    h1.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(10)).await;
    h1.join().await;

    // Engine storage untouched by the whole stop path.
    assert!(engine_dir.is_dir(), "engine class dir lost on stop");

    // --- holder B (new process → new holder id): restart ---
    let plc_b = holder("b", store.clone());
    let port_b = free_port();
    let r2 = render_wf_spec(&fx, port_b, 2, WF_MODULE, "2026-09-08");
    let t0 = std::time::Instant::now();
    let h2 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r2.spec,
        deps(
            store.clone(),
            plc_b.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt2 = h2.runtime();
    wait_state(&rt2, TenantState::Ready, Duration::from_secs(20)).await;
    eprintln!("restart acquire+launch took {:?}", t0.elapsed());

    // The instance resumes from the persisted engine state — poll
    // like the e2e does (status body printed on timeout below).
    let mut last = String::new();
    let mut done = false;
    for _ in 0..45 {
        let (_, _, body) = http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_b)),
            "wf.test",
            &format!("/status?id={inst}"),
            &[],
        )
        .await;
        last = body.clone();
        if body.contains("\"status\":\"complete\"") {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        done,
        "workflow {inst} never completed after restart; last status: {last}"
    );
    assert!(
        last.contains("\"output\":\"a-b-done\""),
        "workflow output wrong: {last}"
    );
    assert!(marker.exists(), "engine .lease-epoch swept across restart");

    h2.send(SupervisorCmd::Stop).await;
    wait_state(&rt2, TenantState::Stopped, Duration::from_secs(10)).await;
    h2.join().await;
}

/// W1-r5 reproduction — the e2e timing, not the r4 test's: the engine
/// is stopped while a 20s `step.sleep` is in flight, the ~4s graceful
/// drain ends ~15s BEFORE the deadline, and the NEXT generation only
/// starts ~10s AFTER the deadline passed while no workerd existed —
/// the `docker restart` + wait_ready window, with margin that removes
/// any doubt the sleep could have finished inside gen-1. Pre-fix the instance is stuck
/// `"status":"running"` forever: the engine's only resume entry
/// (`init`) is never re-invoked — `status()` is a pure read, the
/// in-memory `scheduler.wait` died with the process, and the vendored
/// engine arms no durable alarm.
///
/// (The r4 test above cannot catch this: its 2s sleep expires during
/// gen-1's ~4s drain, so the in-memory timer completes the run before
/// the process dies — the resumed "pass" was storage, not resume.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn workflow_resumes_when_restart_lands_past_sleep_deadline() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let port_a = free_port();
    let r1 = render_wf_spec(&fx, port_a, 1, WF_MODULE_SLOW, "2024-09-25");
    let plc_a = holder("a", store.clone());
    let h1 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r1.spec,
        deps(
            store.clone(),
            plc_a.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt1 = h1.runtime();
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, _, body) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
        "wf.test",
        "/create",
        &[],
    )
    .await;
    assert_eq!(st, 200, "create failed: {body}");
    let inst = body
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("instance id")
        .to_string();
    // A second instance in the SAME workflow — adversarial coverage:
    // every persisted in-flight instance must resume, not just one.
    let (st, _, body2) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
        "wf.test",
        "/create",
        &[],
    )
    .await;
    assert_eq!(st, 200, "create2 failed: {body2}");
    let inst2 = body2
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("instance id 2")
        .to_string();
    // Instance is mid-sleep (20s). Prove the deadline cannot have
    // passed in gen-1: status must still be running right before Stop.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_, _, body) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
        "wf.test",
        &format!("/status?id={inst}"),
        &[],
    )
    .await;
    assert!(
        body.contains("\"status\":\"running\""),
        "expected mid-sleep running before stop: {body}"
    );
    h1.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(15)).await;
    h1.join().await;
    // Stopped ~T+6s, deadline ~T+21s — wait 16s so the deadline is
    // ~9s in the past while NO workerd process existed.
    eprintln!("gen-1 stopped mid-sleep; waiting 16s so the deadline passes dead");
    tokio::time::sleep(Duration::from_secs(16)).await;

    let plc_b = holder("b", store.clone());
    let port_b = free_port();
    let r2 = render_wf_spec(&fx, port_b, 2, WF_MODULE_SLOW, "2024-09-25");
    let h2 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r2.spec,
        deps(
            store.clone(),
            plc_b.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt2 = h2.runtime();
    wait_state(&rt2, TenantState::Ready, Duration::from_secs(20)).await;

    // inst1 is polled (any touch resumes via the constructor hook);
    // inst2 is NEVER touched after restart — if it completes, the only
    // possible wake was the persisted `setAlarm` firing on workerd
    // startup with zero traffic. A broken arming path is caught by
    // inst2's single read below.
    let mut last = String::new();
    let mut done = false;
    for _ in 0..45 {
        let (_, _, body) = http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_b)),
            "wf.test",
            &format!("/status?id={inst}"),
            &[],
        )
        .await;
        last = body;
        if last.contains("\"status\":\"complete\"") {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        done,
        "workflow {inst} stuck after restart past sleep deadline; last status: {last}"
    );
    assert!(
        last.contains("\"output\":\"a-b-done\""),
        "workflow output wrong: {last}"
    );
    // First-ever touch of inst2: it must ALREADY be complete — its
    // overdue alarm woke the actor during startup and the resume ran
    // to completion with no request ever hitting this instance.
    let (_, _, body2) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_b)),
        "wf.test",
        &format!("/status?id={inst2}"),
        &[],
    )
    .await;
    assert!(
        body2.contains("\"status\":\"complete\"") && body2.contains("\"output\":\"a-b-done\""),
        "untouched instance {inst2} did not self-resume via alarm: {body2}"
    );

    h2.send(SupervisorCmd::Stop).await;
    wait_state(&rt2, TenantState::Stopped, Duration::from_secs(10)).await;
    h2.join().await;
}
/// W1-r5 adversarial — an errored instance must NOT be resurrected by
/// the resume path. `init`'s terminal-status gate (Errored) is the
/// only guard; prove it end-to-end across a real restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn errored_instance_is_not_resurrected_after_restart() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    // A workflow whose first step always fails with zero retries —
    // the engine marks the instance errored within ~1s.
    const WF_MODULE_ERR: &str = r#"import { WorkflowEntrypoint } from "cloudflare:workers";
export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    await step.do("always-fail",
      { retries: { limit: 0, delay: "1 millisecond" } },
      async () => { throw new Error("always-fails"); });
    return "unreachable";
  }
}
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ping") return new Response("wf-ok");
    if (url.pathname === "/create") { const i = await env.WF.create(); return Response.json({ id: i.id }); }
    if (url.pathname === "/status") { const i = await env.WF.get(url.searchParams.get("id")); return Response.json(await i.status()); }
    return new Response("nf", { status: 404 });
  }
};
"#;

    let fx = Fixture::new();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let port_a = free_port();
    let r1 = render_wf_spec(&fx, port_a, 1, WF_MODULE_ERR, "2024-09-25");
    let plc_a = holder("a", store.clone());
    let h1 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r1.spec,
        deps(
            store.clone(),
            plc_a.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt1 = h1.runtime();
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, _, body) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
        "wf.test",
        "/create",
        &[],
    )
    .await;
    assert_eq!(st, 200, "create failed: {body}");
    let inst = body
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("instance id")
        .to_string();
    // Wait for the engine to mark the instance errored.
    let mut errored = false;
    for _ in 0..15 {
        let (_, _, body) = http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
            "wf.test",
            &format!("/status?id={inst}"),
            &[],
        )
        .await;
        if body.contains("\"status\":\"errored\"") {
            errored = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(errored, "instance never reached errored: {inst}");
    h1.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(15)).await;
    h1.join().await;

    // Restart — the resume path must early-return at the Errored
    // gate: the instance stays errored, the run never re-executes.
    let plc_b = holder("b", store.clone());
    let port_b = free_port();
    let r2 = render_wf_spec(&fx, port_b, 2, WF_MODULE_ERR, "2024-09-25");
    let h2 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r2.spec,
        deps(
            store.clone(),
            plc_b.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt2 = h2.runtime();
    wait_state(&rt2, TenantState::Ready, Duration::from_secs(20)).await;
    for _ in 0..10 {
        let (_, _, body) = http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_b)),
            "wf.test",
            &format!("/status?id={inst}"),
            &[],
        )
        .await;
        assert!(
            body.contains("\"status\":\"errored\""),
            "errored instance resurrected/mutated: {body}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    h2.send(SupervisorCmd::Stop).await;
    wait_state(&rt2, TenantState::Stopped, Duration::from_secs(15)).await;
    h2.join().await;
}

/// W2 P0-0 — `init` had no single-flight of its own: `isRunning` is
/// checked ~10 awaits before it is set, and `attemptRestart` invoked
/// `this.init` directly, bypassing the r5 resume gate. A restart
/// lands on a FRESH actor (userTriggeredRestart → ctx.abort kills the
/// old one) whose constructor-resume fires `init` off the still-
/// Running metadata — two `init`s race the gate together → two
/// concurrent `USER_WORKFLOW.run`s on the same state.
///
/// Two concurrent restarts land while gen-1 is parked in the gate
/// sleep — abort destroys the actor mid-run; the fresh actor's
/// constructor-resume sees the persisted restart intent and all
/// entry points coalesce into ONE deduped attemptRestart → `e1-1`,
/// single re-run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn restart_during_resume_does_not_double_run() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let port_a = free_port();
    let r1 = render_wf_spec(&fx, port_a, 1, WF_MODULE_RESTART, "2024-09-25");
    let plc_a = holder("a", store.clone());
    let h1 = vrt_host::supervisor::spawn_supervisor(
        TenantRuntime::new("t1"),
        r1.spec,
        deps(
            store.clone(),
            plc_a.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    let rt1 = h1.runtime();
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(20)).await;

    let (st, _, body) = http_get(
        std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
        "wf.test",
        "/create",
        &[],
    )
    .await;
    assert_eq!(st, 200, "create failed: {body}");
    let inst = body
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("instance id")
        .to_string();
    // Land TWO concurrent restarts while gen-1 is parked inside the
    // 1s gate sleep (zero step.do executions yet). Both aborts evict
    // the actor; on the fresh actor the constructor-resume plus BOTH
    // attemptRestart RPCs all reach `init` — pre-fix there is no
    // single-flight so overlapping bare init calls double-run.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let restart_path = format!("/restart?id={inst}");
    let (r1, r2) = tokio::join!(
        http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
            "wf.test",
            &restart_path,
            &[],
        ),
        http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
            "wf.test",
            &restart_path,
            &[],
        )
    );
    let (st, _, body) = r1;
    assert_eq!(st, 200, "restart #1 call failed: {body}");
    let (st2, _, body2) = r2;
    assert_eq!(st2, 200, "restart #2 call failed: {body2}");

    // Wait for the restarted run to reach a terminal state.
    let mut last = String::new();
    let mut done = false;
    for _ in 0..40 {
        let (_, _, body) = http_get(
            std::net::SocketAddr::from(([127, 0, 0, 1], port_a)),
            "wf.test",
            &format!("/status?id={inst}"),
            &[],
        )
        .await;
        last = body;
        if last.contains("\"status\":\"complete\"")
            || last.contains("\"status\":\"errored\"")
            || last.contains("\"status\":\"terminated\"")
        {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        done,
        "restarted workflow never reached terminal state; last: {last}"
    );
    assert!(
        last.contains("\"status\":\"complete\""),
        "restarted workflow did not complete: {last}"
    );
    eprintln!("post-restart status: {last}");
    // `eX-Y` in the marker output = total executions of step
    // first/second across every run in this isolate — `e1-1` asserts
    // each step ran exactly once, i.e. no overlapping AND no
    // sequential double execution. `maxLive` (the m component) is
    // diagnostic only: the killed run's finally-decrement may lag
    // the new run's entry by a microtask in the tenant isolate even
    // though the engine only launched one run.
    assert!(
        last.contains("-e1-1"),
        "expected single clean restarted run (each step executed once): {last}"
    );

    h1.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(15)).await;
    h1.join().await;
}

/// W4-r2 P0-1 — REAL workerd serialized DO switch: while the old
/// generation drains and the new one launches (~4-5s measured), a
/// request to a NON-DO path must be HELD by the ingress and forwarded
/// to the new generation — never a bare 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn serialized_switch_holds_requests_until_new_gen_serves() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");
    let _wd_guard = WORKERD_TEST_LOCK.lock().await;

    let fx = Fixture::new();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let port_a = free_port();
    let r1 = render_wf_spec(&fx, port_a, 1, WF_MODULE, "2026-09-08");
    let plc_a = holder("a", store.clone());
    let registry = vrt_host::supervisor::TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let h1 = vrt_host::supervisor::spawn_supervisor(
        rt.clone(),
        r1.spec,
        deps(
            store.clone(),
            plc_a.clone(),
            &fx.dir.path().join("snap-tmp"),
        ),
        test_params(),
    );
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    // Ingress over the same registry — Host "wf.test" → (t1, WORKER_ID).
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: snapshot_protocol::compute_desired::SCHEMA_VERSION,
        cell_id: "c1".into(),
        revision: 1,
        tenants: vec![common::tenant(
            "t1",
            vec![common::worker(WORKER_ID, "wf.test", "d1")],
        )],
        deletions: vec![],
};
    let state = vrt_host::ingress::IngressState::new(
        registry.clone(),
        Duration::from_secs(15),
        Duration::from_millis(1000),
        1 << 20,
    );
    state.set_routes(vrt_host::route::RouteTable::from_desired(&desired));
    let ig = start_ingress(&state).await;

    // Warm: gen-1 serves through the ingress.
    let (st0, _, _) = http_get(ig.addr, "wf.test", "/status?id=x", &[]).await;
    assert!(st0 != 503, "gen1 must serve before the switch: {st0}");

    // Apply gen-2 → serialized DO switch begins.
    let port_b = free_port();
    let r2 = render_wf_spec(&fx, port_b, 2, WF_MODULE, "2026-09-08");
    h1.send(SupervisorCmd::Apply(Box::new(r2.spec))).await;

    // Fire requests through the whole drain→launch→ready window:
    // NONE may surface 503 while the hold budget (10s) covers the gap.
    let mut statuses = Vec::new();
    for _ in 0..60 {
        let (st, _, _) = http_get(ig.addr, "wf.test", "/status?id=x", &[]).await;
        statuses.push(st);
        if rt.state() == TenantState::Ready
            && rt.expected_digest().is_some()
            && rt.serving_digest() == rt.expected_digest()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    assert!(
        statuses.iter().all(|&s| s != 503),
        "switch window produced 503s: {statuses:?}"
    );
    assert!(
        statuses.len() > 2,
        "switch gap collapsed — hold window untested"
    );
    ig.stop().await;
    h1.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(15)).await;
    h1.join().await;
}
