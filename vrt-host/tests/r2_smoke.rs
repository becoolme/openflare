//! Real-workerd R2 smoke (W5): facade-r2 → binding-backend → REAL
//! in-process `vrt_provider::server` (fs backend).
//!
//! Renders the REAL `vrt_config::render_tenant_config` capnp with a
//! `{"type":"r2"}` binding → workerd's R2Bucket API → vendored
//! facade `r2/native-adapter.js` + `transport.js` (unmodified) →
//! `binding-backend` external — pointed at a REAL `tenant_router`
//! served by the test over loopback TCP (vrt-host wires the same
//! router to a unix socket in production; the dispatch path is
//! identical). Backend = `FsR2Provider` on a scratch root — the
//! standalone production backend — so the signed-uploadId mint/verify
//! path, staging, and composite-etag assembly all run for real.
//!
//! The user module is `examples/standalone-r2/main.js` verbatim —
//! the same artifact `deploy/standalone/e2e.sh` asserts against, so
//! the cargo test and the compose e2e cannot silently diverge.
//!
//! Gated on `VRT_WORKERD_BIN` (must point at the NATIVE workerd
//! binary, not the npm Node wrapper — the wrapper pollutes stderr
//! with a Node exception on nonzero exit and breaks log triage).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::{verify_compute_desired, TenantDesired};
use snapshot_protocol::envelope::{sign, RevisionGuard, SignParams, SigningKey};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressPolicy, EmbedKind,
    RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_provider::authorize::Authorizer;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::desired::MemoryDesiredStore;
use vrt_provider::r2::fs::FsR2Provider;
use vrt_provider::server::{tenant_router, TenantService};

const CELL: &str = "cell-a";
const TENANT: &str = "team-r2";
const WORKER: &str = "w-r2";
const BINDING: &str = "BUCKET";
const BUCKET: &str = "demo-bucket-01";
const VERSION: &str = "v-r2-1";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn facade_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn workerd_bin() -> Option<PathBuf> {
    std::env::var("VRT_WORKERD_BIN")
        .ok()
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
}

fn smoke_dir(suffix: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp-w5")
        .join(format!("vrt-r2-smoke-{}-{}", free_port(), suffix));
    std::fs::create_dir_all(&dir).unwrap();
    // capnp `valid_path` rejects `..` segments (same note as d1_smoke).
    std::fs::canonicalize(&dir).unwrap()
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn read_all(s: &mut std::net::TcpStream, what: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                panic!("read {what}: timeout");
            }
            Err(e) => panic!("read {what}: {e}"),
        }
        // `Connection: close` frames the body by EOF.
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            for line in buf[..i].split(|b| *b == b'\n') {
                if line
                    .windows(15)
                    .any(|w| w.eq_ignore_ascii_case(b"content-length:"))
                {
                    let v: usize = String::from_utf8_lossy(
                        &line[line.iter().position(|b| *b == b':').unwrap() + 1..],
                    )
                    .trim()
                    .parse()
                    .unwrap_or(usize::MAX);
                    if buf.len() >= i + 4 + v {
                        return buf;
                    }
                }
            }
        }
    }
    buf
}

fn http(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut s = std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(60)))
        .unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: w-r2.example.test\r\n\
         Connection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let raw = read_all(&mut s, "response");
    let text = String::from_utf8_lossy(&raw);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body_start = find_subslice(&raw, b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(raw.len());
    (status, raw[body_start..].to_vec())
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..300 {
        if std::net::TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            std::time::Duration::from_millis(300),
        )
        .is_ok()
        {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    false
}

struct Proc {
    child: Child,
    log: Option<PathBuf>,
}

impl Proc {
    fn logged(child: Child, log: PathBuf) -> Self {
        Self {
            child,
            log: Some(log),
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let pid = self.child.id() as i32;
        let _ = unsafe { libc::kill(-pid, libc::SIGTERM) };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let exited = loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break true,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Ok(None) => break false,
            }
        };
        if !exited {
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if std::thread::panicking() {
            if let Some(log) = &self.log {
                let bytes = std::fs::read(log).unwrap_or_default();
                let start = bytes.len().saturating_sub(8192);
                eprintln!(
                    "-- workerd.log tail --\n{}",
                    String::from_utf8_lossy(&bytes[start..])
                );
            }
        }
    }
}

struct SmokeDir(PathBuf);

impl Drop for SmokeDir {
    fn drop(&mut self) {
        if std::env::var_os("VRT_SMOKE_KEEP").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn tail_log(log: &Path) -> String {
    let bytes = std::fs::read(log).unwrap_or_default();
    let start = bytes.len().saturating_sub(8192);
    String::from_utf8_lossy(&bytes[start..]).to_string()
}

/// Real in-process provider: signed desired snapshot →
/// MemoryDesiredStore → Authorizer → TenantService{fs backend} →
/// tenant_router on TCP. The facade props carry
/// `resourceSpecGeneration = render revision`; the store applies the
/// same payload at the same revision.
fn spawn_provider(
    dir: &Path,
    bundle_digest: &str,
    revision: u64,
    port: u16,
) -> std::thread::JoinHandle<()> {
    let key = SigningKey::from_seed(&[9u8; 32]).unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": TENANT,
        "workers": [{
            "workerId": WORKER,
            "name": "r2w",
            "activeVersion": {
                "versionId": VERSION,
                "artifactDigest": bundle_digest,
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [{
                "type": "r2", "name": BINDING, "bucket": BUCKET,
                "permissions": {"read": true, "write": true}
            }],
            "routes": [{"hostname": "w-r2.example.test"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let payload = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: CELL.to_string(),
        revision,
        tenants: vec![tenant],
        deletions: vec![],
};
    let signed = sign(
        &SignParams {
            kind: "compute.desired",
            scope: &snapshot_protocol::compute_desired::cell_scope(CELL),
            revision,
            issued_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now() + chrono::Duration::minutes(10),
            payload: &payload,
        },
        &key,
    )
    .unwrap();
    let mut guard = RevisionGuard::new();
    let verified = verify_compute_desired(
        &signed.bytes,
        CELL,
        &[key.trusted_key()],
        &mut guard,
        chrono::Utc::now(),
    )
    .unwrap();
    let store = MemoryDesiredStore::new(1);
    store.apply(&verified).unwrap();

    let fs = FsR2Provider::new(dir.join("r2"), 64 << 20).unwrap();
    let svc = Arc::new(TenantService {
        tenant_id: TENANT.to_string(),
        authorizer: Arc::new(Authorizer::new(
            Arc::new(store),
            std::time::Duration::from_millis(30_000),
        )),
        kv: None,
        r2: Some(Arc::new(fs)),
        assets: None,
        signer: Arc::new(CursorSigner::new(b"r2-smoke-signing-secret")),
        r2_max_object_bytes: 64 << 20,
        permits: Arc::new(tokio::sync::Semaphore::new(64)),
        global_permits: Arc::new(tokio::sync::Semaphore::new(64)),
        body_read_timeout: std::time::Duration::from_secs(30),
        d1: None,
        queue: None,
        pg: None,
    });
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
                .await
                .unwrap();
            axum::serve(listener, tenant_router(svc).into_make_service())
                .await
                .unwrap();
        });
    })
}

fn render_cfg(dir: &Path, port: u16, bundle: &VersionBundle) -> (PathBuf, Vec<String>) {
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": TENANT,
        "workers": [{
            "workerId": WORKER,
            "name": "r2w",
            "activeVersion": {
                "versionId": VERSION,
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [{
                "type": "r2", "name": BINDING, "bucket": BUCKET,
                "permissions": {"read": true, "write": true}
            }],
            "routes": [{"hostname": "w-r2.example.test"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let manifest = parse_facade_manifest(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
    let params = RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy::default(),
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "r2-smoke-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        ae_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    let out = render_tenant_config(&TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: std::slice::from_ref(bundle),
        params: &params,
    })
    .unwrap();
    assert!(
        out.text.contains("facade-r2"),
        "r2 facade not rendered:\n{}",
        out.text
    );

    // Materialize embeds (same materialization contract as d1_smoke).
    let manifest_doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(facade_root().join("manifest.json")).unwrap(),
    )
    .unwrap();
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
                let user = std::fs::read(
                    Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/standalone-r2/main.js"),
                )
                .unwrap();
                std::fs::write(&dst, user).unwrap();
            }
            EmbedKind::VendoredModule { ref name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }
    let mut text = out.text.clone();
    text = text.replace(
        &format!("unix:sock/{WORKER}.sock"),
        &format!("127.0.0.1:{port}"),
    );
    let cfg = dir.join("cfg.capnp");
    std::fs::write(&cfg, text).unwrap();
    (
        cfg,
        vrt_host::assemble::experimental_extra_args(
            &params.system_compat_flags,
            false,
            false,
            false,
            false,
            false,
        ),
    )
}

fn build_bundle() -> VersionBundle {
    // The bundle digest must equal the desired snapshot's
    // artifactDigest (descriptorSha256 pinning); module TEXT is
    // materialized separately into the BundleModule embed, so a
    // placeholder body is fine here.
    VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "main.js",
            "modules": { "main.js": "export default {fetch(){return new Response('x')}}" },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn real_workerd_r2_multipart_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("skip: set VRT_WORKERD_BIN");
        return;
    };
    let dir = smoke_dir("fs");
    let _dir_guard = SmokeDir(dir.clone());

    let bundle = build_bundle();
    let revision = 1u64;

    let prov_port = free_port();
    let _prov = spawn_provider(&dir, &bundle.digest(), revision, prov_port);
    for _ in 0..50 {
        if std::net::TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], prov_port)),
            std::time::Duration::from_millis(200),
        )
        .is_ok()
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let port = free_port();
    let (cfg, argv_extra) = render_cfg(&dir, port, &bundle);
    let log = dir.join("workerd.log");
    let out_file = std::fs::File::create(&log).unwrap();
    let err_file = out_file.try_clone().unwrap();
    let proc = Command::new(&bin)
        .arg("serve")
        .args(&argv_extra)
        .arg(format!(
            "--external-addr=binding-backend=127.0.0.1:{prov_port}"
        ))
        .arg(&cfg)
        .current_dir(&dir)
        .process_group(0)
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .expect("spawn workerd");
    let _wd = Proc::logged(proc, log.clone());
    if !wait_ready(port) {
        panic!("workerd never listened:\n{}", tail_log(&log));
    }

    // put/get round-trip.
    let (st, body) = http(port, "POST", "/put?key=w5-put", b"w5-payload");
    let v: serde_json::Value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| panic!("put {st}: {:?}\n{}", body, tail_log(&log)));
    assert_eq!(st, 200, "{v}\n{}", tail_log(&log));
    assert_eq!(v["size"], 10, "{v}");
    let (st, body) = http(port, "GET", "/get?key=w5-put", b"");
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["size"], 10, "{v}");
    let expect_sha = format!("{:x}", Sha256::digest(b"w5-payload"));
    assert_eq!(v["sha256"], expect_sha, "{v}");

    // multipart chain: 5MiB + 5MiB + tail → composite etag `-3`.
    let (st, body) = http(port, "POST", "/multipart?key=w5-mp", b"");
    let v: serde_json::Value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| panic!("mp {st}: {:?}\n{}", body, tail_log(&log)));
    assert_eq!(st, 200, "{v}\n{}", tail_log(&log));
    assert_eq!(v["size"], 2 * 5 * 1024 * 1024 + 5, "{v}");
    assert_eq!(v["sha256"], v["expectedSha256"], "{v}");
    assert!(
        v["etag"].as_str().unwrap_or("").ends_with("-3"),
        "composite etag: {v}"
    );

    // abort → complete must be rejected (terminal upload state).
    let (st, body) = http(port, "POST", "/abort?key=w5-ab", b"");
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["completeAfterAbortRejected"], true, "{v}");

    // uploadId minted for one key must not operate on another —
    // the HMAC scope check in `verify_r2_upload` rejects it.
    let (st, body) = http(port, "GET", "/crosskey?key=w5-ck", b"");
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(st, 200, "{v}\n{}", tail_log(&log));
    assert_eq!(v["crossKeyRejected"], true, "{v}");
}
