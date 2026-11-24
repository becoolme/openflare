//! Scale-to-zero P0 (tenant hibernate / wake) on REAL workerd.
//! Spec: doc/platform/workers-runtime-scale-to-zero.md.
//!
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test s2z_hibernate_workerd -- --ignored --nocapture
#![allow(dead_code)]

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
        vars: BTreeMap::from([(
            "t1".to_string(),
            BTreeMap::from([("v1:wu-marker".to_string(), "present".to_string())]),
        )]),
        secrets: BTreeMap::new(),
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "wu-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
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


// ---------------------------------------------------------------- s2z

use std::sync::atomic::Ordering;
use std::sync::Arc;
use vrt_host::hibernate::{BlockerChain, FixedWindowPolicy, ScaleToZero, WakeCoordinator};
use vrt_host::ingress::IngressState;
use vrt_host::route::RouteTable;
use vrt_host::supervisor::TenantRegistry;

fn workerd_bin_or_skip() -> Option<String> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(b) if Path::new(&b).exists() => Some(b),
        _ => {
            eprintln!("SKIP: VRT_WORKERD_BIN unset or missing");
            None
        }
    }
}

fn s2z(idle_ms: u64, min_res_ms: u64) -> ScaleToZero {
    ScaleToZero {
        policy: Arc::new(FixedWindowPolicy {
            idle_window: Duration::from_millis(idle_ms),
            min_residency: Duration::from_millis(min_res_ms),
        }),
        blockers: Arc::new(BlockerChain::p0()),
        coordinator: WakeCoordinator::new(4, 256),
        drain_timeout: Duration::from_secs(5),
        wake_deadline: Duration::from_secs(20),
        blocked_recheck: Duration::from_millis(200),
        quiesce_poll: Duration::from_millis(20),
    }
}

fn s2z_params() -> vrt_host::supervisor::SupervisorParams {
    let mut p = test_params();
    p.ready_timeout = Duration::from_secs(15);
    p.wait_until_grace = Duration::from_secs(8);
    p.drain_timeout = Duration::from_secs(15);
    p.kill_grace = Duration::from_millis(500);
    p.scale_to_zero = Some(s2z(1_000, 500));
    p
}

fn ingress_for(rt: &Arc<TenantRuntime>) -> Arc<IngressState> {
    let registry = TenantRegistry::new();
    registry.insert("t1".to_string(), rt.clone());
    let state = IngressState::new(
        registry,
        Duration::from_secs(20),
        Duration::from_secs(15),
        1024,
    );
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t1", vec![worker("w1", "app.example.com", "d1")])],
        deletions: vec![],
    };
    state.set_routes(RouteTable::from_desired(&desired));
    state
}

async fn shutdown(rt: &Arc<TenantRuntime>, h: vrt_host::supervisor::SupervisorHandle) {
    h.send(SupervisorCmd::Stop).await;
    wait_state(rt, TenantState::Stopped, Duration::from_secs(20)).await;
    h.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_idle_pure_fetch_hibernates_and_process_exits() {
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let (a, b, c) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, a, b, c, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let pid = rt.active_pid();
    assert!(pid > 0 && pid_alive(pid), "workerd up, pid={pid}");

    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(20)).await;
    assert!(rt.state().is_asleep());
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 1);
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0, "expected exit, not crash");
    assert!(rt.stats().hibernated_at_ms.load(Ordering::Relaxed) > 0);
    wait_for(|| !pid_alive(pid), Duration::from_secs(5), "workerd pid exited").await;
    assert_eq!(rt.active_pid(), 0);
    assert!(get(a, "/").await.is_none(), "w1 socket must be closed while asleep");

    let rendered = vrt_host::metrics::registry().render();
    assert!(rendered.contains("vrt_host_hibernate_total"), "{rendered}");
    shutdown(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_http_wake_cold_start_serves_200() {
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let (a, b, c) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, a, b, c, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(25)).await;
    assert_eq!(rt.active_pid(), 0);

    let state = ingress_for(&rt);
    let ig = start_ingress(&state).await;
    let t0 = Instant::now();
    let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    let cold = t0.elapsed();
    eprintln!("S2Z cold-start (hibernated -> 200 via ingress): {cold:?}");
    assert_eq!(status, 200, "wake must serve the held request: {body}");
    assert_eq!(body, "w1-ok");
    assert_eq!(rt.state(), TenantState::Ready);
    assert_eq!(rt.stats().wakes.load(Ordering::Relaxed), 1);
    assert_eq!(rt.stats().wake_failures.load(Ordering::Relaxed), 0);
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    let last = rt.stats().last_wake_ms.load(Ordering::Relaxed);
    eprintln!("S2Z last_wake_ms={last}");
    assert!(last > 0);
    assert_eq!(rt.stats().hibernated_at_ms.load(Ordering::Relaxed), 0);
    let pid = rt.active_pid();
    assert!(pid > 0 && pid_alive(pid), "new workerd pid alive");

    let rendered = vrt_host::metrics::registry().render();
    assert!(
        rendered.contains("vrt_host_wake_total{") && rendered.contains("outcome=\"ok\""),
        "{rendered}"
    );
    assert!(rendered.contains("vrt_host_wake_seconds"), "{rendered}");
    ig.stop().await;
    shutdown(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_durable_object_tenant_never_hibernates() {
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let do_dir = fx.dir.path().join("do-root").join("t1");
    mkdir700(&do_dir);
    let (a, b, c) = (free_port(), free_port(), free_port());
    let mut r = render_spec_with(&fx, a, b, c, 1, MOD_DO, false, Some("EventLog"));
    r.spec.do_storage = Some(DoStorageSpec {
        tenant_dir: do_dir,
        worker_classes: BTreeMap::new(),
        transfer_sources: BTreeMap::new(),
    });
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let pid = rt.active_pid();

    // Several idle windows (idle=1s).
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(rt.state(), TenantState::Ready);
    assert_eq!(rt.stats().hibernations.load(Ordering::Relaxed), 0);
    assert!(rt.stats().hibernate_blocked.load(Ordering::Relaxed) >= 1, "structural block counted");
    assert!(pid_alive(pid), "workerd must stay alive");
    assert_eq!(get(a, "/").await.map(|x| x.0), Some(200));
    let rendered = vrt_host::metrics::registry().render();
    assert!(rendered.contains("structural-do"), "{rendered}");
    shutdown(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_do_alarm_wakes_a_hibernated_tenant() {
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let do_dir = fx.dir.path().join("do-root").join("t1");
    mkdir700(&do_dir);
    let (a, b, c) = (free_port(), free_port(), free_port());
    let mut r = render_spec_with(&fx, a, b, c, 1, MOD_DO_ALARM, false, Some("EventLog"));
    r.spec.do_storage = Some(DoStorageSpec {
        tenant_dir: do_dir,
        worker_classes: BTreeMap::new(),
        transfer_sources: BTreeMap::new(),
    });
    let rt = TenantRuntime::new("t1");
    // The cell alarm index says this class has no due alarm: sleep allowed.
    rt.set_alarm_gate(true, Duration::from_secs(60));
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    // Arm a 30 s alarm, then let the tenant idle out.
    let (status, _) = get(a, "/alarm").await.unwrap();
    assert_eq!(status, 200);
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(10)).await;
    assert!(!pid_alive(rt.active_pid()));

    // The controller's alarm waker asks the host to start the generation.
    rt.await_wake(0, 64, 1 << 20, vrt_host::hibernate::WakeSourceKind::Alarm)
        .await
        .expect("alarm wake");
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;

    // workerd reloads `_cf_ALARM` and fires the overdue alarm.
    let mut hits = 0;
    for _ in 0..40 {
        if let Some((200, body)) = get(a, "/alarm-hits").await {
            hits = body.trim().parse().unwrap_or(0);
            if hits > 0 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(hits, 1, "alarm fires exactly once after wake");
    shutdown(&rt, h).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_websocket_reattaches_after_hibernation() {
    use tokio::io::AsyncWriteExt;
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let (a, b, c) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, a, b, c, 1, MOD_WS, false);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let ingress = ingress_for(&rt).with_ws_hibernation(true, 64 * 1024);
    let ig = start_ingress(&ingress).await;

    let mut s = tokio::net::TcpStream::connect(ig.addr).await.unwrap();
    s.write_all(
        b"GET /ws HTTP/1.1\r\nhost: w1.test\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\n\r\n",
    )
    .await
    .unwrap();
    let head = read_head(&mut s).await;
    assert!(head.starts_with("HTTP/1.1 101"), "{head}");

    // No frames -> idle -> the tenant sleeps while the client socket stays open.
    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(15)).await;
    assert!(!pid_alive(rt.active_pid()));

    // A client frame wakes the tenant and is replayed to a fresh generation.
    s.write_all(&ws_text_frame("after-sleep")).await.unwrap();
    let body = tokio::time::timeout(Duration::from_secs(20), read_ws_text(&mut s))
        .await
        .expect("reattached echo timed out");
    assert_eq!(body, "after-sleep");
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    ig.stop().await;
    shutdown(&rt, h).await;
}

async fn read_head(s: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    loop {
        let mut c = [0u8; 512];
        let n = s.read(&mut c).await.unwrap();
        assert!(n > 0, "closed before handshake");
        buf.extend_from_slice(&c[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            buf.truncate(i + 4);
            break;
        }
    }
    String::from_utf8_lossy(&buf).to_string()
}

fn ws_text_frame(text: &str) -> Vec<u8> {
    let payload = text.as_bytes();
    assert!(payload.len() < 126);
    let mask = [1u8, 2, 3, 4];
    let mut out = vec![0x81, 0x80 | payload.len() as u8];
    out.extend_from_slice(&mask);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    out
}

async fn read_ws_text(s: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 2];
    s.read_exact(&mut head).await.unwrap();
    assert_eq!(head[0] & 0x0f, 1, "expected a text frame");
    let masked = head[1] & 0x80 != 0;
    let len = (head[1] & 0x7f) as usize;
    assert!(len < 126);
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload).await.unwrap();
    if masked {
        let mut mask = [0u8; 4];
        s.read_exact(&mut mask).await.unwrap();
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    String::from_utf8_lossy(&payload).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_pending_waituntil_defers_hibernate() {
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let (a, b, c) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, a, b, c, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    // Direct socket hits bypass ingress and do not touch
    // last_activity, so the 1 s idle verdict can fire while the 4 s
    // waitUntil is still pending: the commit-phase blocker must hold.
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let pid = rt.active_pid();
    let armed_at = Instant::now();
    let (st, body) = get(a, "/arm?ms=4000").await.expect("arm");
    assert_eq!((st, body.as_str()), (200, "armed"));

    tokio::time::sleep(Duration::from_millis(2_000)).await;
    assert!(!rt.state().is_asleep(), "hibernated with a pending waitUntil");
    assert!(pid_alive(pid));

    wait_state(&rt, TenantState::Hibernated, Duration::from_secs(30)).await;
    assert!(
        armed_at.elapsed() >= Duration::from_millis(3_900),
        "hibernated before waitUntil finished: {:?}",
        armed_at.elapsed()
    );
    eprintln!("S2Z hibernated {:?} after arming 4s waitUntil", armed_at.elapsed());
    wait_for(|| !pid_alive(pid), Duration::from_secs(5), "pid exit").await;
    assert_eq!(rt.stats().crashes.load(Ordering::Relaxed), 0);
    shutdown(&rt, h).await;
}

/// A never-resolving waitUntil makes every commit-phase attempt abort;
/// the abort backoff (blocked_recheck * 2^(n-1), blocked_recheck=200ms
/// here) must stretch the interval between attempts instead of
/// re-trying at a fixed cadence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn s2z_never_ending_waituntil_backs_off() {
    init_logs();
    if workerd_bin_or_skip().is_none() {
        return;
    }
    let _g = WORKERD_TEST_LOCK.lock().await;
    let fx = Fixture::new();
    let (a, b, c) = (free_port(), free_port(), free_port());
    let r = render_spec(&fx, a, b, c, 1, MOD_W1, false);
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, SupervisorDeps::default(), s2z_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(20)).await;
    let pid = rt.active_pid();
    let (st, body) = get(a, "/hang").await.expect("hang");
    assert_eq!((st, body.as_str()), (200, "hanging"));

    const WANT: usize = 5;
    let started = Instant::now();
    let mut seen = rt.stats().hibernate_aborts.load(Ordering::Relaxed);
    let mut stamps: Vec<Duration> = Vec::new();
    while stamps.len() < WANT && started.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!rt.state().is_asleep(), "hibernated with a never-ending waitUntil");
        let n = rt.stats().hibernate_aborts.load(Ordering::Relaxed);
        if n > seen {
            // One stamp per observed increment (poll granularity 20ms).
            for _ in seen..n {
                stamps.push(started.elapsed());
            }
            seen = n;
        }
    }
    assert!(pid_alive(pid), "process must stay resident");
    assert!(!rt.state().is_asleep());
    assert!(
        stamps.len() >= WANT,
        "expected >= {WANT} aborts within 10s, got {}",
        stamps.len()
    );
    let gaps: Vec<Duration> = stamps.windows(2).map(|w| w[1] - w[0]).collect();
    eprintln!("S2Z abort backoff gaps: {gaps:?} (stamps {stamps:?})");
    // Backoff: the 4th gap (~1600ms) is well above the 1st (~200ms).
    assert!(
        gaps[3] >= gaps[0] * 3,
        "abort interval did not back off: {gaps:?}"
    );
    assert!(
        gaps[3] > gaps[1],
        "abort intervals not growing: {gaps:?}"
    );
    // A fixed 200ms cadence would have fit span/200ms further aborts.
    let span = stamps[WANT - 1] - stamps[0];
    let fixed_count = span.as_millis() / 200;
    assert!(
        (gaps.len() as u128) * 2 <= fixed_count + 1,
        "backoff not meaningfully sparser than fixed cadence: {} aborts over {span:?} (fixed would be ~{fixed_count})",
        gaps.len()
    );
    shutdown(&rt, h).await;
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


/// DO extending the native `DurableObject` base (the production
/// regression: a Proxy ctx fails workerd's constructor type check).
const MOD_WS: &str = r#"
export default {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname !== "/ws") return new Response("ok");
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    server.addEventListener("message", (ev) => server.send(ev.data));
    return new Response(null, { status: 101, webSocket: client });
  },
};
"#;

const MOD_DO_ALARM: &str = r#"
import { DurableObject } from "cloudflare:workers";
export class EventLog extends DurableObject {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/alarm") {
      await this.ctx.storage.setAlarm(Date.now() + 30000);
      return new Response("armed");
    }
    if (u.pathname === "/alarm-hits") {
      return new Response(String((await this.ctx.storage.get("hits")) ?? 0));
    }
    return new Response("ok");
  }
  async alarm() {
    const n = ((await this.ctx.storage.get("hits")) ?? 0) + 1;
    await this.ctx.storage.put("hits", n);
  }
}
export default {
  async fetch(req, env) {
    const stub = env.EVLOG.get(env.EVLOG.idFromName("a"));
    return stub.fetch(req);
  },
};
"#;

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
