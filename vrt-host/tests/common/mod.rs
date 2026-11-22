//! Shared test harness — stub-binary specs, supervisor fixtures, raw
//! HTTP helpers. Every fixture owns its TempDir and kills its child
//! processes before the test ends (no orphans, no stray sockets).
#![allow(dead_code)]

pub mod g1;
pub mod hyperdrive;

/// PT1: cross-binary CPU scheduling gate for CPU-sensitive tests.
/// cargo runs test BINARIES in parallel; `g1_poc7b_multi_proc_saturated`
/// burns all cores (k=8 spinners) for seconds — any concurrently running
/// watchdog/latency test then sees diluted CPU ratios and inflated
/// latencies. A `flock` on a workspace-stable file coordinates them:
/// latency/watchdog tests take LOCK_SH (several single-core spinners
/// still each get a full core), the whole-core saturation test takes
/// LOCK_EX and runs alone. File lock (not a process mutex) because the
/// interferers live in different test binaries.
pub struct CpuLoadGuard(std::fs::File);

fn cpu_load(exclusive: bool) -> CpuLoadGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/vrt-cpu-load.lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let op = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    let rc = unsafe { libc::flock(f.as_raw_fd(), op) };
    assert_eq!(rc, 0, "flock {op}");
    CpuLoadGuard(f)
}

/// Shared: CPU-sensitive tests (watchdog kills, latency gates).
pub fn cpu_load_shared() -> CpuLoadGuard {
    cpu_load(false)
}

/// Exclusive: tests that saturate every core (PoC7b saturated).
pub fn cpu_load_exclusive() -> CpuLoadGuard {
    cpu_load(true)
}

use snapshot_protocol::compute_desired::{
    RouteDesired, TenantDesired, VersionDesired, WorkerDesired, WorkerQuotas,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::spec::{ResourceLimits, TenantLaunchSpec};
use vrt_host::state::TenantState;
use vrt_host::supervisor::{SupervisorDeps, SupervisorHandle, SupervisorParams, TenantRuntime};

/// Path to the built `vrt-stub-workerd` binary (cargo injects it).
pub fn stub_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_vrt-stub-workerd"))
}

/// Create `path` (and missing parents) with mode `0700` so
/// `vrt_securefs::ensure_secure_dir` accepts it. Only directories we
/// create (plus the leaf) are chmod'd — existing ancestors are left
/// alone (the TempDir root is already 0700).
pub fn mkdir700(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut missing = Vec::new();
    let mut cur = path.to_path_buf();
    while !cur.exists() {
        missing.push(cur.clone());
        if !cur.pop() {
            break;
        }
    }
    for p in missing.iter().rev() {
        std::fs::create_dir(p).unwrap();
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    if path.is_dir() {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

/// A fixture: temp dir + stub spec builder.
pub struct Fixture {
    /// Root temp dir (auto-removed on drop).
    pub dir: TempDir,
    next_digest: u64,
}

impl Fixture {
    /// New fixture with `work/` created.
    pub fn new() -> Self {
        let dir = TempDir::new().expect("tempdir");
        // tempfile roots are umask-created (0755); the secure-dir chain
        // requires 0700 on every ancestor, so tighten the root too.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        mkdir700(&dir.path().join("work"));
        Self {
            dir,
            next_digest: 0,
        }
    }

    /// `work/` root.
    pub fn work_root(&self) -> PathBuf {
        self.dir.path().join("work")
    }

    /// Write `name` under the temp dir and return its path.
    pub fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    /// Build a stub-mode launch spec for `tenant` with `workers` and
    /// extra env knobs (`VRT_STUB_*`). `digest` defaults to a unique
    /// value per call so applies are never deduped accidentally.
    pub fn stub_spec(
        &mut self,
        tenant: &str,
        workers: &[&str],
        env: &[(&str, &str)],
    ) -> TenantLaunchSpec {
        self.next_digest += 1;
        let gen_dir = self
            .dir
            .path()
            .join("work")
            .join(tenant)
            .join(format!("gen-{}", self.next_digest));
        mkdir700(&gen_dir);
        let mut sockets = serde_json::Map::new();
        let mut endpoints = BTreeMap::new();
        for worker in workers {
            let socket = format!("sock-{worker}");
            let endpoint = WorkerEndpoint::Tcp("127.0.0.1:0".parse().unwrap());
            sockets.insert(
                socket,
                serde_json::Value::String(endpoint.control_address()),
            );
            endpoints.insert(worker.to_string(), endpoint);
        }
        let config = serde_json::json!({
            "format": "vrt-stub-config/v1",
            "tenantId": tenant,
            "sockets": sockets,
        });
        let config_path = gen_dir.join("stub-config.json");
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
        let mut base_env = BTreeMap::new();
        for (k, v) in env {
            base_env.insert(k.to_string(), v.to_string());
        }
        TenantLaunchSpec {
            tenant_id: tenant.to_string(),
            binary: stub_binary(),
            config_path,
            config_digest: format!("digest-{}", self.next_digest),
            work_dir: gen_dir,
            external_addrs: Vec::new(),
            env: BTreeMap::new(),
            base_env,
            worker_endpoints: endpoints,
            qconsumer_endpoints: BTreeMap::new(),
            wfops_endpoints: BTreeMap::new(),
            cron_endpoints: BTreeMap::new(),
            wfop_tokens: BTreeMap::new(),
            limits: ResourceLimits::default(),
            heartbeat_endpoint: None,
        waituntil_endpoint: None,
            extra_args: Vec::new(),
            generation: 0,
            isolation: None,
            relaunch: None,
            do_storage: None,
        }
    }
}

/// Millisecond-scale supervisor params for tests.
pub fn test_params() -> SupervisorParams {
    SupervisorParams {
        ready_timeout: Duration::from_millis(4_000),
        probe_timeout: Duration::from_millis(1_500),
        kill_grace: Duration::from_millis(200),
        backoff: (Duration::from_millis(10), Duration::from_millis(200)),
        fail_window: Duration::from_millis(2_000),
        fail_threshold: 3,
        circuit_cooldown: Duration::from_millis(400),
        drain_timeout: Duration::from_millis(600),
        wait_until_grace: Duration::from_millis(600),
        do_switch_wait_until_grace: Duration::from_millis(600),
        monitor_interval: Duration::from_millis(50),
        max_switch_failures: 3,
        preflight_timeout: Duration::from_secs(5),
        prewarm_ttl: Duration::from_secs(10),
        do_pipeline: vrt_host::do_switch_pipeline::PipeCfg::default(),
        scale_to_zero: None,
    }
}

/// Spawn a supervisor for `spec` on a fresh runtime.
pub fn spawn(runtime: &Arc<TenantRuntime>, spec: TenantLaunchSpec) -> SupervisorHandle {
    vrt_host::supervisor::spawn_supervisor(
        runtime.clone(),
        spec,
        SupervisorDeps::default(),
        test_params(),
    )
}

/// Init tracing once per process so failures show structured logs.
pub fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,vrt_host=info".to_string()),
        ))
        .with_test_writer()
        .try_init();
}

/// Poll until `pred` holds or `timeout` elapses.
pub async fn wait_for(mut pred: impl FnMut() -> bool, timeout: Duration, what: &str) {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Wait for a tenant state.
pub async fn wait_state(runtime: &Arc<TenantRuntime>, state: TenantState, timeout: Duration) {
    wait_for(
        || runtime.state() == state,
        timeout,
        &format!("state {state:?}"),
    )
    .await
}

/// `kill(pid, 0)` liveness.
pub fn pid_alive(pid: u32) -> bool {
    vrt_host::launch::pid_alive(pid)
}

/// First index of `needle` in `hay`, or `None`.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Decode an HTTP/1.1 chunked body (trailer lines are ignored).
fn decode_chunked(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    loop {
        let Some(rel) = find_subslice(&raw[pos..], b"\r\n") else {
            break;
        };
        let size_line = String::from_utf8_lossy(&raw[pos..pos + rel]).to_string();
        let size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
        pos += rel + 2;
        if size == 0 {
            break;
        }
        let end = (pos + size).min(raw.len());
        out.extend_from_slice(&raw[pos..end]);
        pos = (end + 2).min(raw.len());
    }
    out
}

/// How a response body is delimited, derived from its headers.
/// Shared by the async (`http_get`) and sync (`g1::http_once`)
/// readers so "body complete" has ONE definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyFraming {
    /// `Content-Length: n`.
    Fixed(usize),
    /// `Transfer-Encoding: chunked`.
    Chunked,
    /// No framing — body ends at connection close.
    CloseDelimited,
}

/// Classify the response body's framing from the (lowercased) header
/// map. `Content-Length` wins over `Transfer-Encoding`, matching the
/// previous inline logic.
pub fn body_framing(headers: &BTreeMap<String, String>) -> BodyFraming {
    if let Some(len) = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
    {
        return BodyFraming::Fixed(len);
    }
    if headers
        .get("transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false)
    {
        return BodyFraming::Chunked;
    }
    BodyFraming::CloseDelimited
}

/// A raw HTTP/1.1 request over TCP; returns (status, headers-lower, body).
pub async fn http_get(
    addr: std::net::SocketAddr,
    host: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
) -> (u16, Vec<(String, String)>, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req = format!("GET {path} HTTP/1.1\r\nhost: {host}\r\n");
    for (k, v) in extra_headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("connection: close\r\n\r\n");
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    stream.write_all(req.as_bytes()).await.unwrap();
    // W2: stop at body-complete — never read_to_end. workerd holds the
    // socket open ~5s after the response (keep-alive drain even under
    // `connection: close`), so an EOF read turns every request into a
    // 5s wait and makes "restart inside a 1s window" impossible.
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 8192];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed before response headers");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head_bytes = buf[..header_end].to_vec();
    let head_text = String::from_utf8_lossy(&head_bytes).to_string();
    let (head_only, _) = head_text.split_once("\r\n\r\n").unwrap_or((&head_text, ""));
    let mut header_map = std::collections::BTreeMap::new();
    for line in head_only.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            header_map.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let mut body_bytes = buf[header_end..].to_vec();
    match body_framing(&header_map) {
        BodyFraming::Fixed(len) => {
            while body_bytes.len() < len {
                let mut chunk = [0u8; 8192];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "connection closed mid-body");
                body_bytes.extend_from_slice(&chunk[..n]);
            }
            body_bytes.truncate(len);
        }
        BodyFraming::Chunked => {
            // Minimal chunked decoder: consume size lines + frames until
            // the zero-size terminal chunk.
            loop {
                let term = find_subslice(&body_bytes, b"0\r\n\r\n");
                if term.is_some() {
                    break;
                }
                let mut chunk = [0u8; 8192];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "connection closed mid-chunked-body");
                body_bytes.extend_from_slice(&chunk[..n]);
            }
            body_bytes = decode_chunked(&body_bytes);
        }
        BodyFraming::CloseDelimited => {
            // No framing — fall back to close-delimited body.
            while stream.read_buf(&mut body_bytes).await.unwrap_or(0) > 0 {}
        }
    }
    let body = String::from_utf8_lossy(&body_bytes).to_string();
    let mut lines = head_only.lines();
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let headers: Vec<(String, String)> = header_map.into_iter().collect();
    (status, headers, body.to_string())
}

/// Build a `WorkerDesired` with one route.
pub fn worker(worker_id: &str, hostname: &str, digest: &str) -> WorkerDesired {
    WorkerDesired {
        worker_id: worker_id.to_string(),
        name: worker_id.to_string(),
        active_version: VersionDesired {
            version_id: format!("v-{worker_id}"),
            artifact_digest: digest.to_string(),
            compatibility_date: "2024-01-01".to_string(),
            compatibility_flags: vec![],
        },
        bindings: vec![],
        routes: vec![RouteDesired {
            hostname: hostname.to_string(),
        }],
        crons: vec![],
        queue_consumers: vec![],
        observability: None,
        durable_object_classes: vec![],
        workflows: vec![],
        tail_consumers: vec![],
        streaming_tail_consumers: vec![],
        quotas: WorkerQuotas {
            cpu_ms: 50,
            memory_mb: 128,
            subrequests: 0,
            outbound_connections: 0,
        },
    }
}

/// Build a `TenantDesired`.
pub fn tenant(tenant_id: &str, workers: Vec<WorkerDesired>) -> TenantDesired {
    TenantDesired {
        tenant_id: tenant_id.to_string(),
        workers,
        flagship_apps: Vec::new(),
    }
}

/// Short root for unix-socket specs: macOS `SUN_LEN` is 104 bytes and
/// `tempfile` paths under `/var/folders/...` can exceed it once the
/// gen/sock tail is appended — `/tmp` keeps the whole path short.
/// `#[ignore]`d unix tests run outside the sandbox, where `/tmp` writes
/// are allowed.
pub fn unix_root(tag: &str) -> PathBuf {
    let root = PathBuf::from(format!("/tmp/vrtu-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    mkdir700(&root);
    root
}

/// Stub spec whose worker endpoints are `unix:` sockets under
/// `<root>/<tenant>/gen-<generation>/sock/` — the production address
/// family, only usable where unix `bind()` is allowed.
pub fn stub_spec_unix(
    root: &Path,
    tenant: &str,
    workers: &[&str],
    generation: u64,
    env: &[(&str, &str)],
) -> TenantLaunchSpec {
    let gen_dir = root.join(tenant).join(format!("gen-{generation}"));
    let sock_dir = gen_dir.join("sock");
    mkdir700(&sock_dir);
    let mut sockets = serde_json::Map::new();
    let mut endpoints = BTreeMap::new();
    for worker in workers {
        let socket = format!("sock-{worker}");
        let endpoint = WorkerEndpoint::Unix(vrt_securefs::socket_path(&sock_dir, worker));
        sockets.insert(
            socket,
            serde_json::Value::String(endpoint.control_address()),
        );
        endpoints.insert(worker.to_string(), endpoint);
    }
    let config = serde_json::json!({
        "format": "vrt-stub-config/v1",
        "tenantId": tenant,
        "sockets": sockets,
    });
    let config_path = gen_dir.join("stub-config.json");
    std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
    let mut base_env = BTreeMap::new();
    for (k, v) in env {
        base_env.insert(k.to_string(), v.to_string());
    }
    TenantLaunchSpec {
        tenant_id: tenant.to_string(),
        binary: stub_binary(),
        config_path,
        config_digest: format!("digest-{generation}"),
        work_dir: gen_dir,
        external_addrs: Vec::new(),
        env: BTreeMap::new(),
        base_env,
        worker_endpoints: endpoints,
        qconsumer_endpoints: BTreeMap::new(),
        wfops_endpoints: BTreeMap::new(),
        cron_endpoints: BTreeMap::new(),
        wfop_tokens: BTreeMap::new(),
        limits: ResourceLimits::default(),
        heartbeat_endpoint: None,
        waituntil_endpoint: None,
        extra_args: Vec::new(),
        generation,
        isolation: None,
        relaunch: None,
        do_storage: None,
    }
}

/// Running ingress under test. The shutdown sender is OWNED by this
/// guard: dropping a `oneshot::Sender` resolves the graceful-shutdown
/// future and silently stops the accept loop — the exact reason the
/// previous `let (_tx, rx)` helper flaked (request raced the shutdown).
pub struct IngressGuard {
    /// Bound listen address.
    pub addr: std::net::SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl IngressGuard {
    /// Graceful stop: close the listener, drain, join the task.
    /// Returns whether the serve task actually finished within the
    /// deadline (the deterministic "ingress really stopped" signal).
    pub async fn stop(mut self) -> bool {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        tokio::time::timeout(Duration::from_secs(5), &mut self.task)
            .await
            .is_ok()
    }
}

impl Drop for IngressGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Bind the ingress on an ephemeral loopback port and serve until
/// `IngressGuard::stop` (or drop → abort). Always prefer this over
/// hand-rolled spawn helpers.
pub async fn start_ingress(state: &Arc<vrt_host::ingress::IngressState>) -> IngressGuard {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = state.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        vrt_host::ingress::serve(listener, state, async move {
            let _ = rx.await;
        })
        .await
    });
    IngressGuard {
        addr,
        shutdown: Some(tx),
        task,
    }
}
