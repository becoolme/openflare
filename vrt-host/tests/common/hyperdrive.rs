//! Shared real-workerd + real-PostgreSQL Hyperdrive test harness.
//!
//! Extracted from `m17_hyperdrive_workerd.rs` (MIG-17) so MIG-18's
//! real `node-postgres` proof can reuse every fixture unchanged:
//! throwaway PG16 lifecycle, workerd spawn/log plumbing, capnp render
//! + embed rewrite, signed `compute.desired` provider fixtures and the
//! pgproxy session gauge. Behaviour is identical — differences between
//! the migrations are expressed via [`HdSpec`], the worker module text
//! and the credential table, never by editing helpers.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::{cell_scope, ComputeDesired, TenantDesired};
use snapshot_protocol::envelope::{sign, SignParams, SigningKey};
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry, EgressPolicy,
    EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};
use vrt_provider::config::PgProxyConfig;
use vrt_provider::desired_file::FileDesiredStore;
use vrt_provider::pg::PgCredentialStore;
use vrt_provider::pgproxy::PgProxy;

/// Test-only login of the throwaway PostgreSQL instance (never reused
/// anywhere; the instance lives in a tmpdir and dies with the test).
pub const PG_SUPER: &str = "vrtadmin";
/// Password for [`PG_SUPER`]; a throwaway value, never a real secret.
pub const PG_PASS: &str = "m17-throwaway-pw";

/// Per-migration render/fixture differences. Everything else — the
/// workerd invocation, the pgproxy fixture, the credential file
/// schema, the synthetic host derivation — is fixed product shape.
pub struct HdSpec<'a> {
    /// `tenantId` in desired and the credential doc.
    pub tenant: &'a str,
    /// `cell_id` scope of the signed `compute.desired`.
    pub cell: &'a str,
    /// `workerId`/`name` of the tenant worker (also its unix-socket
    /// basename pre-rewrite).
    pub worker: &'a str,
    /// `activeVersion.compatibilityDate`.
    pub compat_date: &'a str,
    /// `activeVersion.compatibilityFlags` — tenant-declared flags,
    /// filtered through `flags_for_compat_date` at render time.
    pub compat_flags: &'a [&'a str],
}

pub fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => None,
    }
}

pub fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// Repo root: `vrt-host` lives at `vrt-host`.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

pub fn smoke_root(prefix: &str) -> Option<PathBuf> {
    match vrt_securefs::test_short_root(prefix) {
        Ok(d) => Some(d),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            None
        }
        Err(e) => panic!("short root: {e}"),
    }
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// ---------------------------------------------------------------- workerd

pub struct WorkerProc {
    pub child: Child,
    pub log_path: PathBuf,
    pub dir: PathBuf,
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

pub fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let child = Command::new(bin)
        .arg("serve")
        .arg(cfg)
        .arg("--experimental")
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

pub fn wait_ready(port: u16) -> bool {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}

pub fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .unwrap();
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

pub fn log_text(proc: &WorkerProc) -> String {
    let raw = std::fs::read(&proc.log_path).unwrap_or_default();
    String::from_utf8_lossy(&raw)
        .replace('\0', "")
        .replace("\\\"", "\"")
}

pub fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// GET + JSON body — the worker contract every migration shares.
pub fn http_json(port: u16, path: &str) -> serde_json::Value {
    let (st, body) = http_get(port, path);
    assert_eq!(st, 200, "worker status: {body}");
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("bad json {body:?}: {e}"))
}

pub fn err_text(v: &serde_json::Value) -> String {
    v["error"].to_string()
}

// ------------------------------------------------------- throwaway PG16

pub struct TempPg {
    bin: PathBuf,
    root: PathBuf,
    data: PathBuf,
    pub port: u16,
}

impl Drop for TempPg {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "immediate", "-w", "stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if std::env::var("VRT_SMOKE_KEEP").is_ok() {
            eprintln!("kept pg dir: {}", self.root.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

pub fn pg_bin_dir() -> Option<PathBuf> {
    let dir = match std::env::var("VRT_TEST_PG_BIN") {
        Ok(p) if !p.is_empty() => PathBuf::from(p),
        _ => PathBuf::from("/opt/homebrew/opt/postgresql@16/bin"),
    };
    (dir.join("initdb").is_file() && dir.join("pg_ctl").is_file()).then_some(dir)
}

pub fn start_temp_pg(bin: PathBuf, root_prefix: &str) -> Option<TempPg> {
    let root = smoke_root(root_prefix)?;
    let data = root.join("data");
    let pwfile = root.join("pw");
    std::fs::write(&pwfile, PG_PASS).unwrap();
    let port = free_port();
    let mut pg = TempPg {
        bin,
        root: root.clone(),
        data: data.clone(),
        port,
    };
    let out = Command::new(pg.bin.join("initdb"))
        .arg("-D")
        .arg(&data)
        .args([
            "-A",
            "scram-sha-256",
            "-U",
            PG_SUPER,
            "-E",
            "UTF8",
            "--no-locale",
        ])
        .arg(format!("--pwfile={}", pwfile.display()))
        .env("LC_ALL", "en_US.UTF-8")
        .output()
        .expect("spawn initdb");
    assert!(
        out.status.success(),
        "initdb failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let opts = format!(
        "-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories={} -c fsync=off",
        root.display()
    );
    let out = Command::new(pg.bin.join("pg_ctl"))
        .arg("-D")
        .arg(&data)
        .arg("-l")
        .arg(root.join("pg.log"))
        .arg("-o")
        .arg(&opts)
        .args(["-w", "start"])
        .env("LC_ALL", "en_US.UTF-8")
        .output()
        .expect("spawn pg_ctl");
    if !out.status.success() {
        let log = std::fs::read_to_string(root.join("pg.log")).unwrap_or_default();
        pg.port = 0; // Drop still stops/cleans
        panic!(
            "pg_ctl start failed: {}\n{}\n-- pg.log --\n{log}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
    }
    Some(pg)
}

// ------------------------------------------------------------ render path

pub fn smoke_params() -> RuntimeParams {
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
            tcp_allowed_ports: Vec::new(),
            internal_tcp_allow: Vec::new(),
            internal_tcp_targets: Vec::new(),
            tcp_connect_timeout_ms: 10_000,
            tcp_idle_timeout_ms: 2_000,
            tcp_max_concurrent: 64,
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
        do_storage_dir: String::new(),
        wfop_pepper: "m17-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

pub fn binding_id(spec: &HdSpec, name: &str) -> String {
    format!("vrthd:{}:{name}", spec.tenant)
}

pub fn synthetic_host(spec: &HdSpec, name: &str) -> String {
    let d = Sha256::digest(binding_id(spec, name).as_bytes());
    format!("hd{}.hyperdrive.vrt.internal", &hex::encode(d)[..24])
}

pub fn cred_ref(name: &str) -> String {
    format!("vrtpgcred:{}", name.to_ascii_lowercase())
}

/// Tenant desired. `names` selects which hyperdrive bindings it
/// declares (render declares all; the proxy snapshot omits UNKNOWN).
pub fn tenant_desired(spec: &HdSpec, digest: &str, names: &[&str]) -> TenantDesired {
    let bindings: Vec<_> = names
        .iter()
        .map(|n| {
            json!({
                "type": "hyperdrive",
                "name": n,
                "bindingId": binding_id(spec, n),
                "syntheticHost": synthetic_host(spec, n),
                "credentialRef": cred_ref(n),
                "permissions": {"read": true, "write": true}
            })
        })
        .collect();
    serde_json::from_value(json!({
        "tenantId": spec.tenant,
        "workers": [{
            "workerId": spec.worker,
            "name": spec.worker,
            "activeVersion": {
                "versionId": format!("v-{}-1", spec.worker),
                "artifactDigest": digest,
                "compatibilityDate": spec.compat_date,
                "compatibilityFlags": spec.compat_flags
            },
            "bindings": bindings,
            "routes": [{"hostname": format!("hd--{}.vibeploy.app", spec.tenant)}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap()
}

pub fn user_bundle(spec: &HdSpec, module_src: &str) -> VersionBundle {
    VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": module_src },
            "compatibilityDate": spec.compat_date,
            "compatibilityFlags": spec.compat_flags
        }))
        .unwrap(),
    )
    .unwrap()
}

const BACKEND_STUB: &str = r#"export default {
  async fetch() { return new Response("hyperdrive-stub", { status: 404 }); }
}
"#;

/// Render the tenant capnp into `dir`, rewrite the unix socket to
/// `http_port`, retarget the pinned upstream port to `proxy_port` and
/// stub the external binding-backend. Returns the written cfg path.
pub fn render_and_write(
    dir: &Path,
    http_port: u16,
    proxy_port: u16,
    spec: &HdSpec,
    module_src: &str,
    bindings: &[&str],
) -> PathBuf {
    let bundle = user_bundle(spec, module_src);
    let tenant = tenant_desired(spec, &bundle.digest(), bindings);
    let params = smoke_params();
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

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
                assert_eq!(got, embed.digest, "embed digest drift — regen facade/dist");
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, module_src).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    let mut text = out.text.clone();
    let unix_addr = format!("unix:sock/{}.sock", spec.worker);
    assert!(
        text.contains(&unix_addr),
        "expected unix socket addr:\n{text}"
    );
    text = text.replace(&unix_addr, &format!("127.0.0.1:{http_port}"));

    // The policy pins the production proxy port (19543); the test proxy
    // listens on a free port, so retarget the rendered upstream port.
    // Both the JSON policy (egress `upstreamPort`) occurrences must move.
    let pinned = "upstreamPort\\\":19543";
    let n = text.matches(pinned).count();
    assert!(n >= 1, "expected pinned upstreamPort in render:\n{text}");
    text = text.replace(pinned, &format!("upstreamPort\\\":{proxy_port}"));

    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(text.contains(external), "expected binding-backend external");
    std::fs::write(dir.join("backend-stub.js"), BACKEND_STUB).unwrap();
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

// ------------------------------------------------- provider-side fixtures

/// Signed `compute.desired` holding every `bindings` name except those
/// in `omit` (they are rendered into the worker but unknown to the
/// proxy → `PG_BINDING_UNKNOWN`).
pub fn signed_desired_store(
    dir: &Path,
    spec: &HdSpec,
    digest: &str,
    bindings: &[&str],
    omit: &[&str],
) -> Arc<FileDesiredStore> {
    let key = SigningKey::from_seed(&[17u8; 32]).expect("seeded key");
    let known: Vec<&str> = bindings
        .iter()
        .copied()
        .filter(|n| !omit.contains(n))
        .collect();
    let payload = ComputeDesired {
        schema_version: 1,
        cell_id: spec.cell.to_string(),
        revision: 1,
        tenants: vec![tenant_desired(spec, digest, &known)],
        deletions: vec![],
    };
    let now = chrono::Utc::now();
    let signed = sign(
        &SignParams {
            kind: "compute.desired",
            scope: &cell_scope(spec.cell),
            revision: 1,
            issued_at: now,
            expires_at: now + chrono::Duration::minutes(30),
            payload: &payload,
        },
        &key,
    )
    .expect("sign desired");
    let store = Arc::new(FileDesiredStore::new(
        dir.join("desired.json"),
        spec.cell,
        vec![key.trusted_key()],
        1,
    ));
    store
        .load_bytes(&signed.bytes, now)
        .expect("verify+load signed desired");
    store
}

/// One pg credential-file entry: TCP dial info, or a `tcpBlocked`
/// sentinel (absent = binding stays in desired but has no credential).
pub enum PgCred {
    /// Upstream TCP + this password.
    Tcp(String),
    /// `tcpBlocked`/`readOnly` — credential exists but refuses work.
    Blocked,
}

pub fn write_creds(dir: &Path, spec: &HdSpec, pg_port: u16, entries: &[(&str, PgCred)]) -> PathBuf {
    let mut map = serde_json::Map::new();
    for (name, cred) in entries {
        let v = match cred {
            PgCred::Tcp(password) => json!({
                "token": "t", "endpoint": "http://gw",
                "tenantSlug": format!("{}-x", spec.tenant),
                "credentialVersion": 1, "readOnly": false,
                "tcp": {"host": "127.0.0.1", "port": pg_port,
                        "user": PG_SUPER, "password": password,
                        "database": "postgres"}
            }),
            PgCred::Blocked => json!({
                "token": "t", "endpoint": "http://gw",
                "tenantSlug": format!("{}-x", spec.tenant),
                "credentialVersion": 1, "readOnly": true,
                "tcpBlocked": "migration_read_only"
            }),
        };
        map.insert(cred_ref(name), v);
    }
    let doc = json!({"tenants": {spec.tenant: map}});
    let p = dir.join("pg-credentials.json");
    std::fs::write(&p, serde_json::to_vec(&doc).unwrap()).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    p
}

/// `vrt_provider_pgproxy_sessions_active` — the leak gauge.
pub fn active_sessions() -> f64 {
    let text = vrt_provider::metrics::registry().render();
    text.lines()
        .find(|l| l.starts_with("vrt_provider_pgproxy_sessions_active"))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

/// pgproxy on its own runtime (the test body is blocking). Returns the
/// bound port; the serve task is spawned onto `rt`.
pub fn spawn_pgproxy(
    rt: &tokio::runtime::Runtime,
    dir: &Path,
    spec: &HdSpec,
    digest: &str,
    bindings: &[&str],
    omit: &[&str],
    creds_path: PathBuf,
) -> u16 {
    let desired = signed_desired_store(dir, spec, digest, bindings, omit);
    let creds = Arc::new(PgCredentialStore::new(creds_path));
    let proxy = PgProxy::new(
        PgProxyConfig {
            listen: "127.0.0.1:0".into(),
            connect_timeout_ms: 5_000,
            idle_timeout_ms: 30_000,
            max_session_ms: 120_000,
            max_sessions: 16,
            max_sessions_per_binding: 8,
        },
        creds,
        desired,
    );
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    rt.spawn(async move { proxy.serve(listener).await });
    port
}

/// Baseline (no workerd): drive the proxy directly with the egress
/// preamble + a v3 startup so a later worker-side hang is attributable
/// to the egress/worker hop, not to the proxy/PG hop.
pub fn assert_proxy_baseline(rt: &tokio::runtime::Runtime, proxy_port: u16, host_label: &str) {
    let label = host_label.split('.').next().unwrap().to_string();
    let got = rt.block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", proxy_port))
            .await
            .unwrap();
        s.write_all(format!("VRT-PG1 {label}\n").as_bytes())
            .await
            .unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&196608u32.to_be_bytes());
        for kv in ["user", "vrt-hd", "database", "vrt"] {
            body.extend_from_slice(kv.as_bytes());
            body.push(0);
        }
        body.push(0);
        s.write_all(&((body.len() as u32 + 4).to_be_bytes()))
            .await
            .unwrap();
        s.write_all(&body).await.unwrap();
        let mut seen = Vec::new();
        let mut buf = [0u8; 4096];
        let r = tokio::time::timeout(std::time::Duration::from_secs(8), async {
            loop {
                let n = s.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                seen.extend_from_slice(&buf[..n]);
                if seen.windows(6).any(|w| w == [b'Z', 0, 0, 0, 5, b'I']) {
                    break;
                }
            }
        })
        .await;
        (r.is_ok(), seen.len(), seen.first().copied())
    });
    eprintln!(
        "proxy baseline: ready={} bytes={} first={:?}",
        got.0, got.1, got.2
    );
    assert!(
        got.0 && got.1 > 0,
        "proxy/PG baseline failed (no ReadyForQuery)"
    );
}
