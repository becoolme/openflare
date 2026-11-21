//! AE: real-workerd Analytics Engine smoke — `analyticsEngine`
//! binding → `env.AE.writeDataPoint()` → the per-tenant ingest
//! service receives the exact Cap'n Proto JSON body, spools it, and
//! flushes it to a Parquet part file that reads back the event's
//! fields verbatim.
//!
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test ae_smoke -- --include-ignored
//!
//! Sockets: TCP loopback only — the dev sandbox refuses unix binds;
//! the unix path is the production default covered by `serve_tenant`
//! unit-level tests (same accept loop).

#![cfg(unix)]

use serde_json::json;
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use vrt_config::*;
use vrt_host::ae::{AeLimits, AeService};

fn workerd_bin() -> Option<PathBuf> {
    let p = std::env::var_os("VRT_WORKERD_BIN").map(PathBuf::from)?;
    if p.exists() {
        Some(p)
    } else {
        eprintln!(
            "skip: set VRT_WORKERD_BIN=/path/to/workerd \
             to run the real-workerd AE smoke"
        );
        None
    }
}

/// Tenant module: `/write` emits one datapoint with a marker in every
/// slot family (index/double/blob) so the Parquet read-back proves
/// the wire decode, not just liveness.
const USER_MODULE: &str = r#"
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/write") {
      env.AE.writeDataPoint({
        indexes: ["route:/write"],
        doubles: [7.5, -3.25],
        blobs: ["hello-ae", "second-blob"],
      });
      return new Response("ok");
    }
    return new Response("nf", { status: 404 });
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
            "name": "ae",
            "activeVersion": {
                "versionId": "v-ae-1",
                "artifactDigest": mk_bundle().digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [
                {"type": "analyticsEngine", "name": "AE", "dataset": "weather"}
            ],
            "routes": [{"hostname": format!("{worker_id}--{tenant_id}.vibeploy.app")}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap()
}

fn emit_config(dir: &Path, tenant: &TenantDesired, ae_addr: &str) -> PathBuf {
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
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: String::new(),
        wfop_pepper: "ae-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: ae_addr.into(),
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
    assert!(
        out.text.contains("analyticsEngine = \"weather\""),
        "{}",
        out.text
    );
    let path = dir.join("workerd.capnp");
    std::fs::write(&path, &out.text).unwrap();
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

static SMOKE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
static RUN_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn smoke_root() -> PathBuf {
    let n = RUN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("vrt-ae-smoke-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
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

fn free_ports(n: usize) -> Vec<u16> {
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < n {
        seen.insert(
            std::net::TcpListener::bind(("127.0.0.1", 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
        );
    }
    seen.into_iter().collect()
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
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Collect Parquet part files under `<root>/out/<seg>/<dataset>/dt=*/`.
fn part_files(root: &Path, tenant: &str, dataset: &str) -> Vec<PathBuf> {
    let base = root
        .join("out")
        .join(vrt_securefs::segment_encode(tenant))
        .join(dataset);
    let mut out = Vec::new();
    if let Ok(days) = std::fs::read_dir(&base) {
        for d in days.flatten() {
            if let Ok(files) = std::fs::read_dir(d.path()) {
                for f in files.flatten() {
                    if f.path().extension().is_some_and(|x| x == "parquet") {
                        out.push(f.path());
                    }
                }
            }
        }
    }
    out
}

/// The full write → spool → Parquet lifecycle through a real workerd:
/// the emitted `analyticsEngine` designator resolves to the tenant's
/// TCP ingest listener, `writeDataPoint()` lands in the spool, and a
/// fast flush cadence materializes the canonical part file whose
/// columns decode the exact event fields.
#[test]
fn real_workerd_ae_write_to_parquet() {
    let Some(bin) = workerd_bin() else { return };
    let _serial = SMOKE_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = smoke_root();
    let _guard = DirGuard(dir.clone());
    let mut ports = free_ports(2).into_iter();
    let port = ports.next().unwrap();
    let ae_port = ports.next().unwrap();

    // Ingest service bound BEFORE workerd comes up — the designator
    // resolves lazily per write, but ordering keeps the log honest.
    let ae_dir = dir.join("ae");
    let svc = Arc::new(
        AeService::new(
            None,
            ae_dir.clone(),
            AeLimits {
                max_body_bytes: 1 << 20,
                max_spool_bytes: 1 << 20,
                flush_interval: std::time::Duration::from_millis(200),
            },
        )
        .unwrap(),
    );
    let rt = rt();
    let listener = rt
        .block_on(svc.serve_tenant_tcp("team-ae1", format!("127.0.0.1:{ae_port}").parse().unwrap()))
        .unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let _flush_task = svc.spawn_flush(rx);

    let tenant = tenant_json("w-ae-1", "team-ae1");
    let cfg = emit_config(&dir, &tenant, &format!("127.0.0.1:{ae_port}"));
    let proc = spawn(
        &bin,
        &cfg,
        &dir,
        &[format!("--external-addr=weather=127.0.0.1:{ae_port}")],
    );
    assert!(
        wait_ready(port),
        "workerd must come up; log: {:?}",
        proc.log_path
    );

    let (code, body) = http(port, "/write");
    assert_eq!((code, body.as_str()), (200, "ok"));

    // writeDataPoint is fire-and-forget — poll for the part file.
    let mut parts = Vec::new();
    for _ in 0..50 {
        parts = part_files(&ae_dir, "team-ae1", "weather");
        if !parts.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(
        !parts.is_empty(),
        "no parquet part written; workerd log: {:?}",
        proc.log_path
    );

    // Read the batch back and assert the event's fields verbatim.
    let file = std::fs::File::open(&parts[0]).unwrap();
    let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    use arrow::array::Array;
    let col_str = |idx: usize| {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
    };
    assert_eq!(col_str(0).value(0), "team-ae1"); // tenant
    assert_eq!(col_str(1).value(0), "weather"); // dataset
    assert_eq!(col_str(3).value(0), "route:/write"); // index1
    let d1 = batch
        .column(4)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let d2 = batch
        .column(5)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(d1.value(0), 7.5);
    assert_eq!(d2.value(0), -3.25);
    let b1 = col_str(24);
    let b2 = col_str(25);
    assert_eq!(b1.value(0), "hello-ae");
    assert_eq!(b2.value(0), "second-blob");

    drop(listener);
    drop(_flush_task);
    rt.shutdown_background();
}
