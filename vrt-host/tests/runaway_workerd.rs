//! PT1: runaway watchdog through the REAL supervisor + REAL workerd.
//!
//! A tenant worker spins on a route; the dual-signal watchdog
//! (sustained CPU saturation AND dead event-loop heartbeat on the
//! in-process `vrt-heartbeat` worker) must SIGKILL the process group,
//! a fresh generation must take over, repeat runaways must trip the
//! existing failure-window breaker, and a busy-but-responsive tenant
//! must survive. `vrt-heartbeat` itself gets a loopback/isolation
//! check: tenant `fetch` to its address is egress-denied.
//!
//! Gated on `VRT_WORKERD_BIN` (hard-required — never skipped):
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test runaway_workerd -- --include-ignored --nocapture

mod common;

use common::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::runaway::RunawaySpec;
use vrt_host::spec::{ResourceLimits, TenantLaunchSpec};
use vrt_host::state::TenantState;
use vrt_host::supervisor::{spawn_supervisor, SupervisorCmd, SupervisorHandle, TenantRuntime};

const WORKER_ID: &str = "w1";
static WORKERD_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// One worker, all PT1 routes. `/spin` burns CPU until its bound —
/// 20s keeps a watchdog failure under the 120s test cap; `/burn`
/// does a short bounded burn per request (busy-but-responsive
/// counter-example); `/hb` probes the heartbeat socket from TENANT
/// code (must be egress-denied, loopback is not in the allowlist).
const MOD: &str = r#"
export default {
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/spin") {
      const secs = Math.min(parseInt(u.searchParams.get("secs") ?? "20"), 20);
      const until = Date.now() + secs * 1000;
      let n = 0;
      while (Date.now() < until) n++;
      return new Response("spun " + n);
    }
    if (u.pathname === "/burn") {
      const ms = Math.min(parseInt(u.searchParams.get("ms") ?? "40"), 200);
      const until = Date.now() + ms;
      let n = 0;
      while (Date.now() < until) n++;
      return new Response("burnt " + n);
    }
    if (u.pathname === "/hb") {
      const port = u.searchParams.get("port");
      try {
        const r = await fetch("http://127.0.0.1:" + port + "/", {
          signal: AbortSignal.timeout(1500),
        });
        // Egress denial is an ANSWER (403 + x-vrt-egress-denied), not
        // a throw — surface both so the host can assert the real path.
        const denied = r.headers.get("x-vrt-egress-denied");
        const body = (await r.text()).slice(0, 80);
        return new Response(
          "hb-answer status=" + r.status + " denied=" + denied + " body=" + body);
      } catch (e) {
        return new Response("hb-threw " + String(e).slice(0, 120));
      }
    }
    return new Response("ok");
  },
};
"#;

struct Rendered {
    spec: TenantLaunchSpec,
}

/// Render the real capnp (heartbeat worker + private socket included),
/// rewrite unix sockets to pre-allocated TCP ports (sandbox forbids
/// unix bind — same post-processing contract as the other real-workerd
/// tests).
fn render_spec(fx: &Fixture, port: u16, hb_port: u16, gen: u64, runaway: RunawaySpec) -> Rendered {
    let work_dir = fx
        .dir
        .path()
        .join("work")
        .join("t1")
        .join(format!("gen-{gen}"));
    std::fs::create_dir_all(&work_dir).unwrap();

    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "main.js",
            "modules": { "main.js": MOD },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: snapshot_protocol::compute_desired::TenantDesired = serde_json::from_value(json!({
        "tenantId": "t1",
        "workers": [{
            "workerId": WORKER_ID, "name": "runaway",
            "activeVersion": {
                "versionId": "v-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": "runaway.test"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&raw).expect("facade manifest parses");
    let params = RuntimeParams {
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
        disk_services: manifest.disk_services.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: fx.dir.path().join("do").display().to_string(),
        wfop_pepper: "pt1-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: "sock/vrt-heartbeat.sock".into(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    })
    .unwrap();

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
                let bytes =
                    std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                assert_eq!(
                    format!("{:x}", Sha256::digest(&bytes)),
                    embed.digest,
                    "embed digest drift"
                );
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { module_name, .. } => {
                assert_eq!(module_name, "main.js");
                std::fs::write(&dst, MOD).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    let mut text = out.text.clone();
    // Isolation assertions on the rendered text — the heartbeat worker
    // must exist with deny-all outbound and no tenant-facing surface:
    assert!(text.contains("vrt-heartbeat"), "heartbeat worker missing");
    let hb_unix = "unix:sock/vrt-heartbeat.sock";
    assert!(text.contains(hb_unix), "heartbeat socket missing:\n{text}");
    let w_unix = "unix:sock/w1.sock";
    assert!(text.contains(w_unix), "worker socket missing:\n{text}");
    text = text.replace(w_unix, &format!("127.0.0.1:{port}"));
    text = text.replace(hb_unix, &format!("127.0.0.1:{hb_port}"));

    let config_path = work_dir.join("config.capnp");
    std::fs::write(&config_path, &text).unwrap();
    let spec = TenantLaunchSpec {
        tenant_id: "t1".into(),
        binary: PathBuf::from(std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset")),
        config_path,
        config_digest: format!("runaway-{gen}"),
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
        worker_endpoints: BTreeMap::from([(
            WORKER_ID.to_string(),
            WorkerEndpoint::Tcp(format!("127.0.0.1:{port}").parse().unwrap()),
        )]),
        qconsumer_endpoints: BTreeMap::new(),
        wfops_endpoints: BTreeMap::new(),
        cron_endpoints: BTreeMap::new(),
        wfop_tokens: BTreeMap::new(),
        limits: ResourceLimits {
            runaway: Some(runaway),
            ..Default::default()
        },
        heartbeat_endpoint: Some(WorkerEndpoint::Tcp(
            format!("127.0.0.1:{hb_port}").parse().unwrap(),
        )),
        waituntil_endpoint: None,
        extra_args: vec!["--experimental".into()],
        relaunch: None,
        do_storage: None,
        generation: gen,
        isolation: None,
    };
    Rendered { spec }
}

/// Watchdog params sized for the test: monitor tick 50ms, saturation
/// window 200ms (4 ticks), probe 150ms ×2 failures.
fn runaway_cfg() -> RunawaySpec {
    RunawaySpec {
        window: Duration::from_millis(200),
        min_cpu_ratio: 0.7,
        heartbeat_timeout: Duration::from_millis(150),
        heartbeat_failures: 2,
    }
}

fn addr(port: u16) -> std::net::SocketAddr {
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

/// Fire the spin request and keep the socket open — the response only
/// matters if the watchdog FAILS (the burn then completes on its own
/// bound). A reset on kill is expected, never an assertion failure.
fn fire_spin(port: u16, secs: u64) -> tokio::task::JoinHandle<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let Ok(mut s) = tokio::net::TcpStream::connect(addr(port)).await else {
            return;
        };
        let req = format!(
            "GET /spin?secs={secs} HTTP/1.1\r\nhost: runaway.test\r\nconnection: close\r\n\r\n"
        );
        let _ = s.write_all(req.as_bytes()).await;
        let mut buf = [0u8; 4096];
        let _ = tokio::time::timeout(Duration::from_secs(30), s.read(&mut buf)).await;
    })
}

/// Wait until `stats().runaway_kills` reaches `n`.
async fn wait_kills(rt: &TenantRuntime, n: u64, timeout: Duration) {
    wait_for(
        || rt.stats().runaway_kills.load(Ordering::Relaxed) >= n,
        timeout,
        &format!("runaway_kills>={n}"),
    )
    .await;
}

/// A crashed/relaunched generation must pass ready again — poll the
/// worker port directly (state alone flaps through Crashed first).
async fn wait_serving(port: u16, timeout: Duration) {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if let Ok(mut s) = tokio::net::TcpStream::connect(addr(port)).await {
            use tokio::io::AsyncWriteExt;
            let req = "GET / HTTP/1.1\r\nhost: runaway.test\r\nconnection: close\r\n\r\n";
            if s.write_all(req.as_bytes()).await.is_ok() {
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 2048];
                if let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_secs(5), s.read(&mut buf)).await
                {
                    if n > 0 {
                        return;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("generation never served on :{port}");
}

async fn stop(h: SupervisorHandle) {
    let rt = h.runtime();
    h.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(15)).await;
    h.join().await;
}

/// PT1 core: runaway → SIGKILL(process group) → new generation serves →
/// metrics recorded. Also drives the tenant-side heartbeat isolation
/// check while everything is healthy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn runaway_killed_then_new_generation_serves() {
    init_logs();
    let bin = std::env::var("VRT_WORKERD_BIN").expect("VRT_WORKERD_BIN unset");
    assert!(Path::new(&bin).exists(), "VRT_WORKERD_BIN missing: {bin}");
    let _wd = WORKERD_TEST_LOCK.lock().await;
    // PT1: serialized in-binary; shared gate keeps a whole-core
    // saturation test in another binary from diluting the CPU
    // ratio below the kill threshold.
    let _cpu = common::cpu_load_shared();

    let fx = Fixture::new();
    let port = free_port();
    let hb_port = free_port();
    let r = render_spec(&fx, port, hb_port, 1, runaway_cfg());
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, Default::default(), test_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(30)).await;

    // Heartbeat isolation while healthy: the hb port answers the host
    // probe (the watchdog relies on it — proven by the kill path
    // below), but TENANT code hitting the same address must hit the
    // egress worker's loopback denial.
    let (st, _h, body) = http_get(addr(port), "runaway.test", "/", &[]).await;
    assert_eq!(st, 200, "healthy probe failed: {body}");
    let (st, _h, body) = http_get(
        addr(port),
        "runaway.test",
        &format!("/hb?port={hb_port}"),
        &[],
    )
    .await;
    assert_eq!(st, 200);
    // Any typed deny code proves the egress layer stopped it — the
    // code itself is `port` (high ports stop at the port check, PoC5)
    // or `ip_loopback`; NEVER the heartbeat worker's "ok" body.
    assert!(
        body.contains("status=403") && !body.contains("denied=null") || body.contains("hb-threw"),
        "tenant reached heartbeat socket un-denied: {body}"
    );

    // Fire the burn. Detection needs window(200ms) + 2×probe(150ms)
    // plus monitor cadence (50ms) — expect SIGKILL inside ~2s.
    let t0 = Instant::now();
    let spin = fire_spin(port, 20);
    wait_kills(&rt, 1, Duration::from_secs(15)).await;
    let detect = t0.elapsed();
    eprintln!("pt1: SIGKILL at +{detect:?} after spin start");

    // Fresh generation takes over and serves within a bounded window —
    // the runaway fast-path retry skips backoff.
    wait_serving(port, Duration::from_secs(20)).await;
    let recover = t0.elapsed();
    eprintln!("pt1: new generation serving at +{recover:?}");
    let (st, _h, body) = http_get(addr(port), "runaway.test", "/", &[]).await;
    assert_eq!(st, 200, "new generation not serving: {body}");
    assert_eq!(rt.stats().runaway_kills.load(Ordering::Relaxed), 1);
    spin.abort();
    stop(h).await;
}

/// Counter-example: sustained high CPU WITH a live event loop must
/// never kill — `/burn?ms=40` requests back-to-back keep CPU hot but
/// the heartbeat answers between requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn busy_but_responsive_survives() {
    init_logs();
    let _wd = WORKERD_TEST_LOCK.lock().await;
    // PT1: serialized in-binary; shared gate keeps a whole-core
    // saturation test in another binary from diluting the CPU
    // ratio below the kill threshold.
    let _cpu = common::cpu_load_shared();
    let fx = Fixture::new();
    let port = free_port();
    let hb_port = free_port();
    let r = render_spec(&fx, port, hb_port, 1, runaway_cfg());
    let rt = TenantRuntime::new("t1");
    let h = spawn_supervisor(rt.clone(), r.spec, Default::default(), test_params());
    wait_state(&rt, TenantState::Ready, Duration::from_secs(30)).await;

    // ~4s of continuous short burns — comfortably past the 200ms
    // window so signal 1 can assert (CpuOnly is fine, never Runaway).
    let end = Instant::now() + Duration::from_secs(4);
    let mut n = 0u32;
    while Instant::now() < end {
        let (st, _h, body) = http_get(addr(port), "runaway.test", "/burn?ms=40", &[]).await;
        assert_eq!(st, 200, "burn request failed: {body}");
        n += 1;
    }
    eprintln!("pt1: {n} short burns survived");
    assert_eq!(
        rt.stats().runaway_kills.load(Ordering::Relaxed),
        0,
        "busy-but-responsive tenant was killed — false positive"
    );
    assert_eq!(rt.state(), TenantState::Ready);
    stop(h).await;
}

/// Breaker integration: repeated runaways are CRASHES in the failure
/// window — 3 inside the window trips CircuitOpen (typed 503 path);
/// after the cooldown the half-open attempt installs a healthy gen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires VRT_WORKERD_BIN"]
async fn repeated_runaway_trips_breaker_then_recovers() {
    init_logs();
    let _wd = WORKERD_TEST_LOCK.lock().await;
    // PT1: serialized in-binary; shared gate keeps a whole-core
    // saturation test in another binary from diluting the CPU
    // ratio below the kill threshold.
    let _cpu = common::cpu_load_shared();
    let fx = Fixture::new();
    let port = free_port();
    let hb_port = free_port();
    let r = render_spec(&fx, port, hb_port, 1, runaway_cfg());
    let rt = TenantRuntime::new("t1");
    let mut params = test_params();
    params.fail_threshold = 3;
    params.fail_window = Duration::from_secs(60);
    params.circuit_cooldown = Duration::from_millis(800);
    let h = spawn_supervisor(rt.clone(), r.spec, Default::default(), params);

    for cycle in 1..=3 {
        wait_serving(port, Duration::from_secs(30)).await;
        let spin = fire_spin(port, 20);
        wait_kills(&rt, cycle, Duration::from_secs(20)).await;
        eprintln!("pt1: kill #{cycle} done");
        spin.abort();
    }
    wait_state(&rt, TenantState::CircuitOpen, Duration::from_secs(10)).await;
    eprintln!("pt1: breaker open after 3 runaway crashes");
    assert_eq!(rt.stats().runaway_kills.load(Ordering::Relaxed), 3);

    // Cooldown elapses → half-open attempt installs a healthy gen.
    wait_state(&rt, TenantState::Ready, Duration::from_secs(30)).await;
    wait_serving(port, Duration::from_secs(15)).await;
    let (st, _h, body) = http_get(addr(port), "runaway.test", "/", &[]).await;
    assert_eq!(st, 200, "post-breaker generation not serving: {body}");
    eprintln!("pt1: breaker recovered, generation serving");
    stop(h).await;
}
