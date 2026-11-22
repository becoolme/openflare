//! Tail Workers (`tail_consumers`) end-to-end on real workerd.
//!
//! A producer declares `tail_consumers`; vrt-config renders the
//! consumer as a workerd `tails` designator (`worker-<consumerId>`).
//! The test renders the tenant config, always asserts the designator
//! (assemble-layer evidence), then — when `VRT_WORKERD_BIN` is set —
//! boots workerd, invokes the producer and asserts the consumer's
//! `tail(events)` handler received the `TailEvent` batch.
//!
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test tail_consumers -- --nocapture

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

fn workerd_bin() -> Option<PathBuf> {
    std::env::var("VRT_WORKERD_BIN").ok().map(PathBuf::from)
}

static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn smoke_root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/tmp-tail-consumers")
        .join(format!(
            "run-{}-{}",
            std::process::id(),
            RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        if std::env::var("TAIL_KEEP_DIR").is_err() {
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

const PRODUCER: &str = r#"
export default {
  async fetch(req) {
    console.log("producer-log", new URL(req.url).pathname);
    return new Response("producer-ok");
  },
};
"#;

/// Keeps the received batches in module state; `/events` exposes them
/// through the same worker, so no external sink or binding is needed.
const CONSUMER: &str = r#"
let batches = [];
export default {
  async fetch(req) {
    if (new URL(req.url).pathname === "/events") {
      return Response.json(batches);
    }
    return new Response("tail-ok");
  },
  async tail(events) {
    for (const ev of events) {
      batches.push({
        scriptName: ev.scriptName,
        outcome: ev.outcome,
        logs: ev.logs,
        exceptions: ev.exceptions,
      });
    }
  },
};
"#;

fn http_once(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    let req =
        format!("GET {path} HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes())?;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        match s.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    let text = String::from_utf8_lossy(&buf).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Ok((status, body))
}

fn http(port: u16, path: &str) -> (u16, String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match http_once(port, path) {
            Ok(r) => return r,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "http {path} never connected: {e}"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
}

/// Build the two-worker tenant + config. Always returns after asserting
/// the rendered `tails` designators; the ports let the caller boot it.
fn render_write(dir: &Path) -> (PathBuf, u16, u16) {
    let mut bundles = Vec::new();
    let mut worker_json = Vec::new();
    for (id, name, module, hostname) in [
        ("wprod", "producer", PRODUCER, "producer.test"),
        ("wtail", "tail", CONSUMER, "tail.test"),
    ] {
        let bundle = vrt_config::VersionBundle::build(
            vrt_config::DeployManifest::from_pack(&json!({
                "mainModule": "index.js",
                "modules": { "index.js": module },
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            }))
            .unwrap(),
        )
        .unwrap();
        worker_json.push(json!({
            "workerId": id,
            "name": name,
            "activeVersion": {
                "versionId": format!("v-{id}-1"),
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2025-01-01",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": hostname}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 50, "outboundConnections": 16}
        }));
        bundles.push(bundle);
    }
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-tails",
        "workers": worker_json
    }))
    .unwrap();
    // The producer declares the tail consumer; the consumer declares none.
    let mut tenant = tenant;
    tenant.workers[0].tail_consumers =
        vec![serde_json::from_value(json!({"workerId": "wtail"})).unwrap()];

    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = vrt_config::parse_facade_manifest(&manifest_raw).unwrap();
    let params = vrt_config::RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: vrt_config::EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 30_000,
            ..Default::default()
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: dir.join("do").display().to_string(),
        wfop_pepper: "tail-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        ae_addr: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = vrt_config::render_tenant_config(&vrt_config::TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &bundles,
        params: &params,
    })
    .unwrap();

    // Assemble-layer evidence, independent of the runtime gate.
    assert!(
        out.text
            .contains("      tails = [(name = \"worker-wtail\")],\n"),
        "producer must render the consumer designator:\n{}",
        out.text
    );
    assert!(
        !out.text.contains("vrt-tail-"),
        "no platform collector is configured in this render:\n{}",
        out.text
    );
    let consumer_start = out
        .text
        .find("(name = \"worker-wtail\", worker = (")
        .expect("consumer service");
    let rest = &out.text[consumer_start..];
    let end = rest.find("\n    )),").unwrap();
    assert!(
        !rest[..end].contains("tails ="),
        "the consumer itself declares no tail consumer"
    );

    // Materialize embeds exactly like the other real-workerd smokes.
    let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            vrt_config::EmbedKind::SystemModule {
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
                let src = facade_root().join("..").join(m["dist"].as_str().unwrap());
                let bytes = std::fs::read(&src).unwrap();
                assert_eq!(format!("{:x}", Sha256::digest(&bytes)), embed.digest);
                std::fs::write(&dst, bytes).unwrap();
            }
            vrt_config::EmbedKind::BundleModule { worker_id, .. } => {
                let module = if worker_id == "wprod" {
                    PRODUCER
                } else {
                    CONSUMER
                };
                std::fs::write(&dst, module).unwrap();
            }
            vrt_config::EmbedKind::VendoredModule { name } => {
                std::fs::write(&dst, vrt_config::workflows::vendored_bytes(name).unwrap()).unwrap();
            }
        }
    }

    // Unix sockets → loopback TCP (the sandbox forbids unix bind).
    let prod_port = free_port();
    let tail_port = free_port();
    let mut text = out.text.clone();
    text = text.replace("unix:sock/wprod.sock", &format!("127.0.0.1:{prod_port}"));
    text = text.replace("unix:sock/wtail.sock", &format!("127.0.0.1:{tail_port}"));
    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    (path, prod_port, tail_port)
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let child = Command::new(bin)
        .arg("serve")
        .arg("--external-addr=binding-backend=127.0.0.1:1")
        .arg(cfg)
        .arg("--experimental")
        .current_dir(dir)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn workerd");
    WorkerProc { child, log_path }
}

#[test]
fn real_workerd_delivers_tail_events_to_the_declared_consumer() {
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let (cfg, prod_port, tail_port) = render_write(&dir);

    let Some(bin) = workerd_bin() else {
        eprintln!(
            "assembled tail designators verified; set VRT_WORKERD_BIN to run the \
             real-workerd delivery assertion"
        );
        return;
    };
    let proc = spawn(&bin, &cfg, &dir);

    let (status, body) = http(prod_port, "/hello");
    assert_eq!(
        (status, body.as_str()),
        (200, "producer-ok"),
        "producer ingress; log: {:?}",
        proc.log_path
    );

    // Tail delivery is asynchronous and best-effort: poll briefly.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut batches: Vec<serde_json::Value> = Vec::new();
    while std::time::Instant::now() < deadline {
        let (st, body) = http(tail_port, "/events");
        assert_eq!(st, 200, "consumer /events status: {body}");
        batches = serde_json::from_str(&body).unwrap_or_default();
        if !batches.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        !batches.is_empty(),
        "tail consumer received no TailEvent; workerd log: {:?}",
        proc.log_path
    );

    let ev = &batches[0];
    assert_eq!(ev["outcome"], "ok", "tail event: {ev}");
    // The TailItem always carries `scriptName`; workerd's standalone
    // config has no per-script name, so it is usually null here (the
    // producer identity is the service designator). Accept a non-empty
    // value if a runtime build fills it, but never a missing field.
    assert!(
        ev.as_object().is_some_and(|o| o.contains_key("scriptName")),
        "scriptName field must be present: {ev}"
    );
    if let Some(name) = ev["scriptName"].as_str() {
        assert!(!name.is_empty(), "scriptName must not be empty: {ev}");
    }
    let logs = ev["logs"].as_array().expect("logs array");
    assert!(
        logs.iter().any(|l| {
            l["message"]
                .as_array()
                .is_some_and(|m| m.iter().any(|p| p == "producer-log"))
        }),
        "console.log was not delivered in the TailEvent: {ev}"
    );
}
