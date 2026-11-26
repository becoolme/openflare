//! Streaming Tail Workers — real-workerd delivery smoke.
//!
//! The config unit tests prove the renderer emits
//! `streamingTails = [(name = "worker-<consumer>")]`, but only a real
//! workerd proves that (a) the pinned binary accepts the designator
//! (upstream marks the field provisional) and (b) the consumer's
//! `tailStream()` handler actually receives the producer's onset and
//! outcome events.
//!
//! Flow: render a two-worker tenant through the REAL
//! `vrt_config::render_tenant_config`, replace the producer's unix
//! socket with TCP loopback, start the pinned workerd, issue one HTTP
//! request to the producer, then assert the consumer's `console.log`
//! markers (written from its `tailStream` handlers) appear in workerd's
//! log.
//!
//! Gated on `VRT_WORKERD_BIN` — absent → explicit skip with a printed
//! reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test streaming_tail_workerd -- --include-ignored

#![cfg(unix)]

use serde_json::json;
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use vrt_config::*;

const PRODUCER_ID: &str = "w-prod-1";
const CONSUMER_ID: &str = "w-cons-1";

const PRODUCER_MODULE: &str = r#"
export default {
  async fetch(req, env) {
    console.log("VRT_PRODUCER_INVOKED");
    return new Response("producer-ok");
  }
};
"#;

/// `tailStream(onset)` returns a handler object; the runtime then calls
/// `log`/`outcome` as the invocation progresses. The markers include the
/// producer's script name so a stray event from another worker cannot
/// satisfy the assertion.
const CONSUMER_MODULE: &str = r#"
export default {
  tailStream(event) {
    console.log("VRT_TAILSTREAM_ONSET:" + String(event.event.scriptName || ""));
    return {
      log(e) {
        console.log("VRT_TAILSTREAM_LOG:" + String(e.event.level || ""));
      },
      outcome(e) {
        console.log("VRT_TAILSTREAM_OUTCOME:" + String(e.event.outcome || ""));
      }
    };
  }
};
"#;

fn workerd_bin() -> Option<PathBuf> {
    let p = std::env::var_os("VRT_WORKERD_BIN").map(PathBuf::from)?;
    if p.exists() {
        Some(p)
    } else {
        eprintln!("skip: set VRT_WORKERD_BIN=/path/to/workerd to run the streaming-tail smoke");
        None
    }
}

fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn bundle(source: &str) -> VersionBundle {
    VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": source },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap()
}

fn worker(id: &str, name: &str, bundle: &VersionBundle, route: bool) -> serde_json::Value {
    let routes = if route {
        vec![json!({"hostname": format!("{id}--tenant.vibeploy.app")})]
    } else {
        vec![]
    };
    json!({
        "workerId": id,
        "name": name,
        "activeVersion": {
            "versionId": format!("v-{id}"),
            "artifactDigest": bundle.digest(),
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        },
        "bindings": [],
        "routes": routes,
        "crons": [],
        "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 50, "outboundConnections": 16}
    })
}

fn tenant() -> (TenantDesired, Vec<VersionBundle>) {
    let prod = bundle(PRODUCER_MODULE);
    let cons = bundle(CONSUMER_MODULE);
    let mut p = worker(PRODUCER_ID, "producer", &prod, true);
    p["streamingTailConsumers"] = json!([{"workerId": CONSUMER_ID}]);
    let c = worker(CONSUMER_ID, "consumer", &cons, false);
    let t: TenantDesired = serde_json::from_value(json!({
        "tenantId": "tenant",
        "workers": [p, c]
    }))
    .unwrap();
    debug_assert_eq!(t.workers[0].streaming_tail_consumers.len(), 1);
    (t, vec![prod, cons])
}

fn emit_config(
    dir: &Path,
    tenant: &TenantDesired,
    bundles: &[VersionBundle],
    port: u16,
    consumer_port: u16,
) -> PathBuf {
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
        do_storage_dir: String::new(),
        wfop_pepper: "streaming-tail-smoke".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let input = TenantRenderInput {
        tenant,
        revision: 1,
        bundles,
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();
    assert!(
        out.text
            .contains(r#"streamingTails = [(name = "worker-w-cons-1")]"#),
        "{}",
        out.text
    );
    // Materialize embeds (workerd resolves `embed "..."` relative to
    // the config file). Tenant modules come from this test's sources;
    // system workers from the checked-in facade manifest.
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
                let src = facade_root().join("..").join(m["dist"].as_str().unwrap());
                std::fs::write(&dst, std::fs::read(&src).unwrap()).unwrap();
            }
            EmbedKind::BundleModule { worker_id, .. } => {
                let source = if worker_id == PRODUCER_ID {
                    PRODUCER_MODULE
                } else {
                    CONSUMER_MODULE
                };
                std::fs::write(&dst, source).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                std::fs::write(&dst, vrt_config::workflows::vendored_bytes(name).unwrap()).unwrap();
            }
        }
    }
    // The renderer emits a socket per worker. Point both at TCP
    // loopback: relative `unix:sock/...` addresses resolve against the
    // CWD (not the config dir), and only the producer is exercised.
    let text = out
        .text
        .replace(
            &format!("unix:sock/{PRODUCER_ID}.sock"),
            &format!("127.0.0.1:{port}"),
        )
        .replace(
            &format!("unix:sock/{CONSUMER_ID}.sock"),
            &format!("127.0.0.1:{consumer_port}"),
        );
    assert!(!text.contains("unix:sock/"), "socket rewrite missed: {text}");
    let path = dir.join("workerd.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

struct WorkerProc {
    child: Child,
}
impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..200 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN pointing at a real workerd binary"]
fn real_workerd_streaming_tail_delivers_to_tail_stream() {
    let Some(bin) = workerd_bin() else { return };
    let dir = std::env::temp_dir().join(format!("vrt-streaming-tail-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = DirGuard(dir.clone());

    let (tenant, bundles) = tenant();
    let log_path = dir.join("workerd.log");
    // `free_port` drops the listener before workerd binds it, so another
    // process can steal the port between the two calls. Retry with fresh
    // ports when that happens instead of failing the whole suite.
    let mut last_cfg = PathBuf::new();
    let (port, _proc) = (1..=5)
        .find_map(|_| {
            let port = free_port();
            let consumer_port = free_port();
            let cfg = emit_config(&dir, &tenant, &bundles, port, consumer_port);
            let log = std::fs::File::create(&log_path).unwrap();
            let err = log.try_clone().unwrap();
            let child = Command::new(&bin)
                .arg("serve")
                .arg("--external-addr=binding-backend=127.0.0.1:1")
                .arg(&cfg)
                .arg("--experimental")
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(err))
                .spawn()
                .unwrap();
            let proc = WorkerProc { child };
            last_cfg = cfg.clone();
            if wait_ready(port) {
                return Some((port, proc));
            }
            None
        })
        .unwrap_or_else(|| {
            panic!(
                "workerd did not listen after 5 attempts.\n--- workerd log ---\n{}\n--- capnp ---\n{}",
                std::fs::read_to_string(&log_path).unwrap_or_default(),
                std::fs::read_to_string(&last_cfg).unwrap_or_default()
            )
        });

    // One invocation on the producer.
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut buf = Vec::new();
    let _ = s.read_to_end(&mut buf);
    let body = String::from_utf8_lossy(&buf);
    assert!(body.contains("producer-ok"), "{body}");

    // TailStream delivery is asynchronous — poll the log for the onset
    // and outcome markers (bounded).
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut text = String::new();
    while Instant::now() < deadline {
        text = std::fs::read_to_string(&log_path).unwrap_or_default();
        if text.contains("VRT_TAILSTREAM_ONSET:")
            && text.contains("VRT_TAILSTREAM_OUTCOME:ok")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "tailStream events not delivered within 20s.\n--- workerd log ---\n{text}\n--- capnp ---\n{}",
        std::fs::read_to_string(&last_cfg).unwrap_or_default()
    );
}

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
