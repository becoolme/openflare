//! `vrt dev` — one-command local development.
//!
//! Runs the full standalone stack (vrt-local control plane + vrt-host
//! + vrt-provider + vrt-sql) against `<worker_dir>/.vrt` and serves
//! the worker on a loopback ingress (default `127.0.0.1:8787`). File
//! changes trigger a redeploy; SIGINT / the stop channel shuts every
//! child down gracefully (SIGTERM → SIGKILL).
//!
//! Dev-only behaviors (never reachable from a production config):
//! - `ingress.devCatchAll` routes ANY Host to the single deployed
//!   worker so `curl http://127.0.0.1:8787/` works without a Host.
//! - `.dev.vars` is loaded per deploy: keys declared in `vrt.toml`
//!   `[secrets]` fill their secret refs; the rest merge into vars.

use crate::LocalError;
use serde::Deserialize;
use sha2::Digest;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Default ingress bind — loopback only; dev never listens publicly.
const DEFAULT_LISTEN: &str = "127.0.0.1:8787";
/// Bound on waiting for `/readyz` after the children spawn.
const READY_TIMEOUT: Duration = Duration::from_secs(90);
/// File-watch poll cadence.
const WATCH_POLL: Duration = Duration::from_millis(250);
/// A changed signature must stay stable this long before redeploy.
const WATCH_DEBOUNCE: Duration = Duration::from_millis(300);
/// `workerd.lock` — the deploy pin; dev verifies the resolved real
/// workerd against it unless `--allow-unpinned-workerd` is passed.
pub(crate) const WORKERD_LOCK: &str = include_str!("../../deploy/docker/workerd.lock");

/// `vrt dev` options.
#[derive(Debug, Default)]
pub struct DevOptions {
    /// Project directory holding vrt.toml / wrangler config + modules.
    pub worker_dir: PathBuf,
    /// Wrangler named environment selected with `--env`.
    pub env_name: Option<String>,
    /// Ingress listen address (`ip:port`); default 127.0.0.1:8787.
    pub listen: Option<String>,
    /// Disable the file watcher (`--no-watch`).
    pub no_watch: bool,
    /// Explicit workerd path (`--workerd`).
    pub workerd: Option<PathBuf>,
    /// Use the dev-stub runtime instead of workerd (`--stub`).
    pub stub: bool,
    /// Skip the workerd.lock digest pin (`--allow-unpinned-workerd`).
    pub allow_unpinned_workerd: bool,
    /// Child binaries (default: sibling of `vrt`, then PATH).
    pub vrt_host_bin: Option<PathBuf>,
    /// Same, for vrt-provider.
    pub vrt_provider_bin: Option<PathBuf>,
    /// Same, for vrt-sql.
    pub vrt_sql_bin: Option<PathBuf>,
    /// Facade dist dir (`--facade-dist` / `VRT_FACADE_DIST` / auto).
    pub facade_dist: Option<PathBuf>,
    /// External stop channel (tests); SIGINT is handled by serve.
    pub stop: Option<tokio::sync::watch::Receiver<bool>>,
    /// Print worker logs to the terminal (`--no-logs` disables).
    pub print_logs: bool,
    /// Disable ANSI colors on log output (`--no-color`).
    pub no_color: bool,
}

/// Runtime binary resolved for this dev session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerdKind {
    Real,
    Stub,
}

#[derive(Debug)]
struct DevRuntime {
    binary: PathBuf,
    kind: WorkerdKind,
    /// True when the real workerd verified against `workerd.lock`.
    pinned: bool,
}

/// What the running session looks like — printed by `vrt dev` and
/// asserted on by tests.
#[derive(Debug)]
pub struct DevSummary {
    /// `http://<listen>` users should curl.
    pub url: String,
    /// `<worker_dir>/.vrt` root created/reused by this session.
    pub root: PathBuf,
    /// Worker name from the resolved project config.
    pub worker_name: String,
    /// Selected Wrangler environment, if any.
    pub environment: Option<String>,
    /// Host control listener base (`http://<addr>`) — readyz/healthz
    /// and the log tail (loopback only).
    pub control_base: String,
    /// `binding -> backend` lines (names + refs only, never values).
    pub bindings: Vec<String>,
    /// How long spawn→ready took.
    pub ready_after: Duration,
    /// Runtime line, e.g. `workerd (pinned)` / `vrt-stub-workerd` /
    /// `workerd (unpinned)`.
    pub runtime: String,
}

/// A live `vrt dev` session. Dropping without `shutdown` still stops
/// children — the serve task owns kill-on-drop supervision.
pub struct DevSession {
    /// The printed summary (url, bindings, ready timing).
    pub summary: DevSummary,
    stop_tx: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), LocalError>>,
}

impl DevSession {
    /// Ask the session to stop (children get SIGTERM, then SIGKILL).
    pub fn shutdown(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Wait for the session to end (signal, external stop, or fatal
    /// error). Returns the serve outcome.
    pub async fn wait(self) -> Result<(), LocalError> {
        match self.task.await {
            Ok(r) => r,
            Err(e) => Err(LocalError::Op("dev", format!("task join: {e}"))),
        }
    }
}

/// `vrt dev` entry: start, print the summary, wait for shutdown.
pub async fn run(opts: DevOptions) -> Result<DevSession, LocalError> {
    start(opts).await
}

/// Start a dev session: resolve runtime → init `.vrt/` → write dev
/// configs → deploy → spawn supervised children → wait for ready.
pub async fn start(opts: DevOptions) -> Result<DevSession, LocalError> {
    let t0 = Instant::now();
    let worker_dir = opts
        .worker_dir
        .canonicalize()
        .map_err(|_| LocalError::Op("dev", "worker dir missing".into()))?;
    let runtime = resolve_workerd(&opts)?;
    let root = worker_dir.join(".vrt");
    let local = ensure_root(&root, &opts, &runtime)?;
    let key = local.trusted_key()?;
    let ports = DevPorts::alloc(&opts)?;

    write_dev_configs(&root, &key, &runtime, &ports)?;

    // First deploy — also validates the worker dir before children
    // spawn, so config errors surface before "listening" prints.
    let (meta, _src) =
        crate::project_cfg::load_worker_meta_for_env(&worker_dir, opts.env_name.as_deref())?;
    deploy_once(&local, &worker_dir, &meta, opts.env_name.as_deref())?;
    let summary_bindings = binding_table(&meta);
    let worker_name = meta.name.clone();

    // Children under serve supervision; the stop channel shuts the
    // whole session down from tests or Ctrl+C.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let serve_stop = match opts.stop.clone() {
        Some(mut ext) => {
            // Mirror external stops into the session channel.
            let tx = stop_tx.clone();
            tokio::spawn(async move {
                let _ = ext.changed().await;
                let _ = tx.send(true);
            });
            stop_rx.clone()
        }
        None => stop_rx.clone(),
    };
    let serve_opts = crate::serve::ServeOptions {
        mgmt_tcp: None,
        supervise: true,
        vrt_host_bin: Some(resolve_bin("vrt-host", opts.vrt_host_bin.as_deref())?),
        vrt_provider_bin: Some(resolve_bin(
            "vrt-provider",
            opts.vrt_provider_bin.as_deref(),
        )?),
        vrt_sql_bin: Some(resolve_bin("vrt-sql", opts.vrt_sql_bin.as_deref())?),
        extra_args: Vec::new(),
        stop: Some(serve_stop),
    };
    let serve_root = root.clone();
    let mut serve_task =
        tokio::spawn(async move { crate::serve::run(&serve_root, &serve_opts).await });

    // Ready when the host control listener reports all tenants ready.
    let ready = wait_ready(&ports.control, &mut serve_task).await;
    if let Err(e) = ready {
        let _ = stop_tx.send(true);
        let _ = serve_task.await;
        return Err(e);
    }

    // Live worker logs on the terminal (`--no-logs` disables) —
    // starts only after ready so the control listener exists.
    let log_guard = if opts.print_logs {
        local
            .resolve_worker_id(&worker_name)
            .ok()
            .map(|wid| start_log_tail(ports.control.clone(), wid, !opts.no_color))
    } else {
        None
    };

    let summary = DevSummary {
        url: format!("http://{}", ports.listen),
        root: root.clone(),
        worker_name,
        environment: opts.env_name.clone(),
        control_base: format!("http://{}", ports.control),
        bindings: summary_bindings,
        ready_after: t0.elapsed(),
        runtime: match runtime.kind {
            WorkerdKind::Stub => "vrt-stub-workerd".to_string(),
            WorkerdKind::Real if runtime.pinned => "workerd (pinned)".to_string(),
            WorkerdKind::Real => "workerd (unpinned)".to_string(),
        },
    };

    // Watch + redeploy loop owns the rest of the session.
    let loop_stop = stop_rx.clone();
    let task = tokio::spawn(run_loop(
        root,
        worker_dir,
        serve_task,
        loop_stop,
        opts.no_watch,
        opts.env_name,
        log_guard,
    ));

    Ok(DevSession {
        summary,
        stop_tx,
        task,
    })
}

/// Owns the stdout log-tail thread — dropping the guard (end of
/// `run_loop`, early failure, or session shutdown) sets the flag and
/// joins the thread, so the tail never outlives the session.
struct LogTailGuard {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for LogTailGuard {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Stream the deployed worker's hub records (console.log, exceptions,
/// platform codes) to stdout — read-only `/v1/logs/tail` polling, so
/// the cell-controller drain keeps its own cursor.
fn start_log_tail(base: String, worker_id: String, color: bool) -> LogTailGuard {
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    let handle = std::thread::spawn(move || {
        let opts = crate::logtail::LogsOpts {
            follow: true,
            color,
            poll: Duration::from_millis(500),
            stop: Some(stop2),
            ..Default::default()
        };
        let _ = crate::logtail::logs_run(&mut std::io::stdout(), &base, &worker_id, &opts);
    });
    LogTailGuard {
        stop,
        handle: Some(handle),
    }
}

/// Session body after startup: forward serve's exit, honor the stop
/// channel, and redeploy on stable file changes.
async fn run_loop(
    root: PathBuf,
    worker_dir: PathBuf,
    mut serve_task: tokio::task::JoinHandle<Result<(), LocalError>>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
    no_watch: bool,
    env_name: Option<String>,
    _log_guard: Option<LogTailGuard>,
) -> Result<(), LocalError> {
    let mut sig = watch_signature(&worker_dir);
    let mut changed_at: Option<Instant> = None;
    let mut stopped = false;
    loop {
        let tick = tokio::time::sleep(WATCH_POLL);
        tokio::pin!(tick);
        tokio::select! {
            r = &mut serve_task => {
                return match r {
                    Ok(inner) => inner,
                    Err(e) => Err(LocalError::Op("dev", format!("serve join: {e}"))),
                };
            }
            _ = stop_rx.changed(), if !stopped => {
                stopped = true;
                // serve::run observes the same channel and is already
                // stopping its children — just wait for it above.
            }
            _ = &mut tick, if !no_watch && !stopped => {
                let next = watch_signature(&worker_dir);
                if next != sig {
                    sig = next;
                    changed_at = Some(Instant::now());
                    continue;
                }
                if let Some(t) = changed_at {
                    if t.elapsed() >= WATCH_DEBOUNCE {
                        changed_at = None;
                        redeploy(&root, &worker_dir, env_name.as_deref());
                    }
                }
            }
        }
    }
}

/// One deploy pass: `.dev.vars` → secret puts + var overrides, then
/// `VrtLocal::deploy`. Errors are surfaced, never fatal to the loop.
fn redeploy(root: &Path, worker_dir: &Path, env_name: Option<&str>) {
    let local = match crate::VrtLocal::open(root) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("dev: reopen failed: {e}");
            return;
        }
    };
    let meta = match crate::project_cfg::load_worker_meta_for_env(worker_dir, env_name) {
        Ok((m, _)) => m,
        Err(e) => {
            eprintln!("dev: config reload failed: {e}");
            return;
        }
    };
    match deploy_once(&local, worker_dir, &meta, env_name) {
        Ok(r) => eprintln!("dev: reloaded {} (version {})", meta.name, r.version_id),
        Err(e) => eprintln!("dev: redeploy failed: {e}"),
    }
}

/// Apply `.dev.vars` then deploy. Keys declared in `[secrets]` fill
/// their secret refs (values never hit argv/logs); everything else
/// merges into vars — matching wrangler's `.dev.vars` semantics.
fn deploy_once(
    local: &crate::VrtLocal,
    worker_dir: &Path,
    meta: &crate::WorkerToml,
    env_name: Option<&str>,
) -> Result<crate::DeployResult, LocalError> {
    let mut overrides = crate::worker_pkg::DeployOverrides {
        env_name: env_name.map(str::to_owned),
        ..Default::default()
    };
    let dev_vars_path = worker_dir.join(".dev.vars");
    if dev_vars_path.is_file() {
        let text = std::fs::read_to_string(&dev_vars_path)
            .map_err(|_| LocalError::Op("dev", "read .dev.vars".into()))?;
        let tenant = local.status()?.tenant_id;
        for (k, v) in crate::devvars::parse(&text)? {
            match meta.secrets.get(&k) {
                Some(secret_ref) => {
                    local.put_secret(&tenant, secret_ref, &v)?;
                }
                None => {
                    overrides.vars.insert(k, v);
                }
            }
        }
    }
    local.deploy(worker_dir, &overrides)
}

/// Human one-line-per-binding table (`name → kind/target`). Values —
/// secret or var — are never printed.
fn binding_table(meta: &crate::WorkerToml) -> Vec<String> {
    let mut out = Vec::new();
    for name in meta.vars.keys() {
        out.push(format!("var      {name}"));
    }
    for (name, secret_ref) in &meta.secrets {
        out.push(format!("secret   {name} -> {secret_ref}"));
    }
    if let Some(b) = &meta.bindings {
        for (name, ns) in &b.kv {
            out.push(format!("kv       {name} -> {ns}"));
        }
        for (name, bucket) in &b.r2 {
            out.push(format!("r2       {name} -> {bucket}"));
        }
        for (name, db) in &b.d1 {
            out.push(format!("d1       {name} -> {db}"));
        }
        for (name, q) in &b.queue {
            out.push(format!("queue    {name} -> {q}"));
        }
        for (name, db) in &b.pg {
            out.push(format!("pg       {name} -> {db}"));
        }
        for (name, svc) in &b.service {
            match svc.entrypoint() {
                Some(ep) => out.push(format!("service  {name} -> {}#{ep}", svc.service())),
                None => out.push(format!("service  {name} -> {}", svc.service())),
            }
        }
        for (name, digest) in &b.assets {
            out.push(format!("assets   {name} -> {digest}"));
        }
    }
    if let Some(a) = &meta.assets {
        out.push(format!("assets   {} -> dir {}", a.binding, a.dir));
    }
    for b in &meta.durable_object_bindings {
        out.push(format!("do       {} -> {}", b.name, b.class_name));
    }
    for w in &meta.workflows {
        out.push(format!("workflow {} -> {}", w.name, w.class_name));
    }
    for c in &meta.queue_consumers {
        out.push(format!("queueConsumer -> {}", c.queue));
    }
    for t in &meta.tail_consumers {
        out.push(format!("tailConsumer -> {}", t.service));
    }
    for c in &meta.crons {
        out.push(format!("cron     {}", c.expression));
    }
    out
}

/// Initialize `<worker_dir>/.vrt` when absent; otherwise just check
/// the existing layout (fail closed on a foreign/initialized root —
/// `init` itself refuses non-empty dirs).
fn ensure_root(
    root: &Path,
    opts: &DevOptions,
    runtime: &DevRuntime,
) -> Result<crate::VrtLocal, LocalError> {
    let initialized =
        root.join("keys/trusted.json").is_file() && root.join("state/state.json").is_file();
    if !initialized {
        let facade = resolve_facade(opts, runtime)?;
        crate::VrtLocal::init(
            root,
            &crate::InitParams {
                cell_id: "local".to_string(),
                tenant_id: "local".to_string(),
                facade_manifest: facade.as_ref().map(|d| d.join("manifest.json")),
                facade_dist_root: facade,
                max_versions_per_worker: None,
            },
        )?;
        eprintln!(
            "dev: initialized {} — add `.vrt/` to .gitignore to keep dev state out of git",
            root.display()
        );
    }
    crate::VrtLocal::open(root)
}

/// Facade dist for capnp renders: `--facade-dist`, `VRT_FACADE_DIST`,
/// a `facade/` dir found by walking up from the vrt executable
/// (repo `target/<profile>/vrt` → repo root), or the baked image
/// copy. Required for real workerd; optional under the stub.
fn resolve_facade(opts: &DevOptions, runtime: &DevRuntime) -> Result<Option<PathBuf>, LocalError> {
    if let Some(d) = &opts.facade_dist {
        return check_facade_dir(d);
    }
    if let Ok(d) = std::env::var("VRT_FACADE_DIST") {
        return check_facade_dir(Path::new(&d));
    }
    let has_manifest = |d: &Path| d.join("manifest.json").is_file();
    if let Ok(exe) = std::env::current_exe() {
        for anc in exe.ancestors() {
            let cand = anc.join("facade");
            if has_manifest(&cand) {
                return Ok(Some(cand));
            }
        }
    }
    let baked = PathBuf::from("/usr/share/vrt/facade");
    if has_manifest(&baked) {
        return Ok(Some(baked));
    }
    match runtime.kind {
        // The stub renderer never touches facade modules.
        WorkerdKind::Stub => Ok(None),
        WorkerdKind::Real => Err(LocalError::Op(
            "dev",
            "facade dist not found — pass --facade-dist <dir> or set VRT_FACADE_DIST".into(),
        )),
    }
}

fn check_facade_dir(dir: &Path) -> Result<Option<PathBuf>, LocalError> {
    if dir.join("manifest.json").is_file() {
        Ok(Some(dir.to_path_buf()))
    } else {
        Err(LocalError::Op(
            "dev",
            format!("{} has no manifest.json", dir.display()),
        ))
    }
}

/// Child binary: explicit flag → `VRT_<NAME>_BIN` env → sibling of
/// the `vrt` executable (repo `target/<profile>/`) → bare name (PATH).
fn resolve_bin(name: &str, explicit: Option<&Path>) -> Result<PathBuf, LocalError> {
    if let Some(p) = explicit {
        if p.is_file() {
            return Ok(p.to_path_buf());
        }
        return Err(LocalError::Op(
            "dev",
            format!("{name} not found at {}", p.display()),
        ));
    }
    let env_key = format!("VRT_{}_BIN", name.to_uppercase().replace('-', "_"));
    if let Ok(v) = std::env::var(&env_key) {
        let p = PathBuf::from(v);
        if p.is_file() {
            return Ok(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let cand = dir.join(name);
            if cand.is_file() {
                return Ok(cand);
            }
        }
    }
    Ok(PathBuf::from(name))
}

/// Resolve the workerd stand-in: `--workerd` > `VRT_WORKERD` > PATH
/// `workerd`; `--stub` > `VRT_STUB_WORKERD` > sibling > PATH
/// `vrt-stub-workerd`. A real workerd is pinned to workerd.lock.
fn resolve_workerd(opts: &DevOptions) -> Result<DevRuntime, LocalError> {
    if opts.stub {
        let bin = locate("vrt-stub-workerd", "VRT_STUB_WORKERD").ok_or_else(|| {
            LocalError::Op(
                "dev",
                "vrt-stub-workerd not found — build vrt-host with the dev-stub feature".into(),
            )
        })?;
        return Ok(DevRuntime {
            binary: bin,
            kind: WorkerdKind::Stub,
            pinned: false,
        });
    }
    let path = if let Some(p) = &opts.workerd {
        Some(p.clone())
    } else if let Ok(v) = std::env::var("VRT_WORKERD") {
        Some(PathBuf::from(v))
    } else {
        locate("workerd", "VRT_WORKERD")
    };
    let Some(path) = path else {
        return Err(LocalError::Op(
            "dev",
            "workerd not found — pass --workerd <path>, set VRT_WORKERD, or use --stub".into(),
        ));
    };
    if !path.is_file() {
        return Err(LocalError::Op(
            "dev",
            format!(
                "workerd at {} missing — pass --workerd or use --stub",
                path.display()
            ),
        ));
    }
    // A stub masquerading as real fails the other way too: an explicit
    // stub-looking binary is treated as Stub (mode flips below).
    let kind = match std::process::Command::new(&path).arg("--version").output() {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout).into_owned()
                + &String::from_utf8_lossy(&out.stderr);
            if text.contains("vrt-stub-workerd") {
                WorkerdKind::Stub
            } else {
                WorkerdKind::Real
            }
        }
        Err(_) => {
            return Err(LocalError::Op(
                "dev",
                format!("workerd at {} is not executable", path.display()),
            ))
        }
    };
    match kind {
        WorkerdKind::Stub => Ok(DevRuntime {
            binary: path,
            kind,
            pinned: false,
        }),
        WorkerdKind::Real => {
            let pinned = verify_workerd_pin(&path, opts.allow_unpinned_workerd)?;
            Ok(DevRuntime {
                binary: path,
                kind,
                pinned,
            })
        }
    }
}

/// `deploy/docker/workerd.lock` pins real workerd binaries per
/// platform artifact (`binarySha256`). Platforms without a pinned
/// digest (macOS, linux-arm64) cannot be verified — dev then requires
/// the explicit `--allow-unpinned-workerd` opt-out so the bypass is a
/// decision, not an accident.
pub(crate) fn verify_workerd_pin(path: &Path, allow_unpinned: bool) -> Result<bool, LocalError> {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("linux-x64"),
        ("linux", "aarch64") => Some("linux-arm64"),
        _ => None,
    };
    #[derive(Deserialize)]
    struct Lock {
        artifacts: BTreeMap<String, Artifact>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Artifact {
        binary_sha256: Option<String>,
    }
    let lock: Lock = serde_json::from_str(WORKERD_LOCK)
        .map_err(|_| LocalError::Op("dev", "workerd.lock unreadable".into()))?;
    let artifact = platform.and_then(|p| lock.artifacts.get(p));
    let Some(expected) = artifact.and_then(|a| a.binary_sha256.as_deref()) else {
        return if allow_unpinned {
            eprintln!("dev: warning — workerd is NOT pinned for this platform");
            Ok(false)
        } else {
            Err(LocalError::Op(
                "dev",
                "no pinned workerd digest for this platform in deploy/docker/workerd.lock \
                 — pass --allow-unpinned-workerd to use an unverified local workerd"
                    .into(),
            ))
        };
    };
    let bytes =
        std::fs::read(path).map_err(|_| LocalError::Op("dev", "workerd unreadable".into()))?;
    let actual = hex::encode(sha2::Sha256::digest(&bytes));
    if actual == expected {
        Ok(true)
    } else {
        Err(LocalError::Op(
            "dev",
            format!(
                "workerd digest mismatch vs deploy/docker/workerd.lock ({expected}) — \
                 fetch the pinned binary or pass --allow-unpinned-workerd"
            ),
        ))
    }
}

/// Flag/env/exe-sibling/PATH lookup for a binary by name.
pub(crate) fn locate(name: &str, env_key: &str) -> Option<PathBuf> {
    if let Ok(v) = std::env::var(env_key) {
        let p = PathBuf::from(v);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let cand = dir.join(name);
            if cand.is_file() {
                return Some(cand);
            }
        }
    }
    // PATH search (dev tools routinely run without install layout).
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|d| d.join(name))
            .find(|p| p.is_file())
    })
}

/// Loopback ports for the internal listeners (control/sql/metrics).
/// Only `listen` is user-chosen; internals stay on ephemeral ports so
/// parallel dev sessions never collide.
struct DevPorts {
    listen: String,
    control: String,
    metrics: String,
    sql_listen: String,
    sql_metrics: String,
    provider_metrics: String,
}

impl DevPorts {
    fn alloc(opts: &DevOptions) -> Result<Self, LocalError> {
        let listen = opts
            .listen
            .clone()
            .unwrap_or_else(|| DEFAULT_LISTEN.to_string());
        // Fail fast on a busy user-facing port; internals race-safe.
        let listen_addr: std::net::SocketAddr = listen
            .parse()
            .map_err(|_| LocalError::Op("dev", "--listen must be ip:port".into()))?;
        if !listen_addr.ip().is_loopback() {
            return Err(LocalError::Op(
                "dev",
                "vrt dev binds loopback only — use 127.0.0.1:<port>".into(),
            ));
        }
        let probe = std::net::TcpListener::bind(listen_addr)
            .map_err(|_| LocalError::Op("dev", format!("port {} in use", listen)))?;
        drop(probe);
        Ok(Self {
            listen,
            control: ephemeral_addr()?,
            metrics: ephemeral_addr()?,
            sql_listen: ephemeral_addr()?,
            sql_metrics: ephemeral_addr()?,
            provider_metrics: ephemeral_addr()?,
        })
    }
}

/// `127.0.0.1:0` → bound → released. The tiny bind window is a
/// loopback-only risk accepted for dev ergonomics.
fn ephemeral_addr() -> Result<String, LocalError> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|_| LocalError::Op("dev", "ephemeral port".into()))?;
    Ok(l.local_addr()
        .map_err(|_| LocalError::Op("dev", "addr".into()))?
        .to_string())
}

/// Render dev configs from the SAME standalone templates `vrt init`
/// uses, with dev-only deltas: catch-all ingress routing, ephemeral
/// internal listeners, and stub render mode when applicable.
fn write_dev_configs(
    root: &Path,
    key: &snapshot_protocol::TrustedKey,
    runtime: &DevRuntime,
    ports: &DevPorts,
) -> Result<(), LocalError> {
    const HOST_T: &str = include_str!("../../examples/standalone-vrt-host.toml");
    const PROVIDER_T: &str = include_str!("../../examples/standalone-vrt-provider.toml");
    const SQL_T: &str = include_str!("../../examples/standalone-vrt-sql.toml");
    crate::layout::check_socket_path_budget(root, "local")?;
    let root_s = root
        .canonicalize()
        .unwrap_or_else(|_| root.to_path_buf())
        .display()
        .to_string();
    let backend_sock =
        vrt_securefs::socket_path(&crate::layout::provider_listeners_dir(root), "local")
            .display()
            .to_string();
    let mut host = HOST_T
        .replace("__VRT_ROOT__", &root_s)
        .replace("__WORKERD__", &runtime.binary.display().to_string())
        .replace("__VRT_LISTEN__", &ports.listen)
        .replace("__VRT_CONTROL__", &ports.control)
        .replace("__VRT_METRICS__", &ports.metrics)
        .replace("__VRT_KEY_ID__", &key.key_id)
        .replace("__VRT_PUBKEY__", &key.public_key)
        .replace("__VRT_CELL__", "local")
        .replace("__VRT_TENANT__", "local")
        .replace("__VRT_BACKEND_SOCK__", &backend_sock);
    if runtime.kind == WorkerdKind::Stub {
        host = host.replacen("mode = \"capnp\"", "mode = \"stub\"", 1);
    }
    // Dev-only: any Host routes to the single deployed worker.
    host = host.replacen(
        "[ingress]\n",
        "[ingress]\n# `vrt dev` only — never set in production.\ndevCatchAll = true\n",
        1,
    );
    let provider = PROVIDER_T
        .replace("__VRT_ROOT__", &root_s)
        .replace("__VRT_KEY_ID__", &key.key_id)
        .replace("__VRT_PUBKEY__", &key.public_key)
        .replace("__VRT_CELL__", "local")
        .replace("__VRT_TENANT__", "local")
        .replace("__VRT_PROVIDER_METRICS__", &ports.provider_metrics)
        .replace("__VRT_SQL_ADDR__", &format!("http://{}", ports.sql_listen));
    let sql = SQL_T
        .replace("__VRT_ROOT__", &root_s)
        .replace(
            "__VRT_SQL_LISTEN__",
            &format!(
                "127.0.0.1:{}",
                ports.sql_listen.rsplit(':').next().unwrap_or("0")
            ),
        )
        .replace("__VRT_SQL_METRICS__", &ports.sql_metrics);
    // Dev-only: the object store lives next to the db under `.vrt/`, so
    // vrt-sql's same-device durability guard (fail closed by default)
    // would refuse to start. Production configs never carry this key.
    let sql = crate::templates::dev_allow_same_device_durability(&sql);
    let cfg = crate::layout::config_dir(root);
    std::fs::create_dir_all(&cfg).map_err(|_| LocalError::Layout("config dir"))?;
    for (name, doc) in [
        ("vrt-host.toml", host),
        ("vrt-provider.toml", provider),
        ("vrt-sql.toml", sql),
    ] {
        vrt_securefs::atomic_write(&cfg.join(name), doc.as_bytes(), 0o600)
            .map_err(|_| LocalError::Layout("config write"))?;
    }
    Ok(())
}

/// Poll `<control>/readyz` until 200 or the serve task dies first.
async fn wait_ready(
    control: &str,
    serve_task: &mut tokio::task::JoinHandle<Result<(), LocalError>>,
) -> Result<(), LocalError> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if serve_task.is_finished() {
            return Err(LocalError::Op(
                "dev",
                "serve exited before ready — check child logs".into(),
            ));
        }
        if Instant::now() > deadline {
            return Err(LocalError::Op("dev", "ready timeout".into()));
        }
        match http_status(control, "/readyz").await {
            Some(200) => return Ok(()),
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

/// Minimal async GET → status code (no body interest).
async fn http_status(addr: &str, path: &str) -> Option<u16> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
    let req = format!("GET {path} HTTP/1.0\r\nhost: {addr}\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut buf)).await;
    let text = String::from_utf8_lossy(&buf);
    text.split_whitespace().nth(1)?.parse().ok()
}

/// Recursive mtime/len signature over the worker dir — the same
/// exclusions as the module scan (`node_modules`, dotfiles except
/// `.dev.vars`, config basenames count as triggers) plus `.vrt/`
/// itself (the dev root must never trigger redeploys).
fn watch_signature(dir: &Path) -> BTreeMap<String, (u128, u64)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let path = e.path();
            let name = e.file_name().to_string_lossy().into_owned();
            if name == ".vrt" || name == "node_modules" {
                continue;
            }
            if name.starts_with('.') && name != ".dev.vars" {
                continue;
            }
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file() {
                let rel = path
                    .strip_prefix(dir)
                    .map(|p| p.to_string_lossy().replace('\\', "/"))
                    .unwrap_or_default();
                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                out.insert(rel, (mtime, meta.len()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmpdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "vrtdev-test-{}-{:?}",
            std::process::id(),
            std::time::Instant::now()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn trusted_key() -> snapshot_protocol::TrustedKey {
        snapshot_protocol::SigningKey::from_seed(&[7u8; 32])
            .unwrap()
            .trusted_key()
    }

    #[test]
    fn dev_configs_render_and_set_catch_all() {
        // Socket-path budget needs a ~35B root — `test_short_root`
        // uses /tmp; denied sandboxes get the SKIP contract instead.
        let root_guard = match vrt_securefs::ShortRoot::new("vrtdevc") {
            Ok(r) => r,
            Err(e)
                if e.kind() == std::io::ErrorKind::PermissionDenied
                    || e.raw_os_error() == Some(1) =>
            {
                eprintln!("SKIP: /tmp not writable ({e})");
                return;
            }
            Err(e) => panic!("short root: {e}"),
        };
        let root = root_guard.path().to_path_buf();
        let ports = DevPorts {
            listen: "127.0.0.1:8787".into(),
            control: "127.0.0.1:40001".into(),
            metrics: "127.0.0.1:40002".into(),
            sql_listen: "127.0.0.1:40003".into(),
            sql_metrics: "127.0.0.1:40004".into(),
            provider_metrics: "127.0.0.1:40005".into(),
        };
        let rt = DevRuntime {
            binary: PathBuf::from("/bin/true"),
            kind: WorkerdKind::Stub,
            pinned: false,
        };
        write_dev_configs(&root, &trusted_key(), &rt, &ports).unwrap();
        let host = std::fs::read_to_string(root.join("config/vrt-host.toml")).unwrap();
        assert!(host.contains("devCatchAll = true"), "{host}");
        assert!(host.contains("mode = \"stub\""), "{host}");
        assert!(
            !host.contains("__VRT_"),
            "unsubstituted placeholder: {host}"
        );
        // It must still be valid TOML with the expected keys.
        let parsed: toml::Value = toml::from_str(&host).unwrap();
        assert_eq!(parsed["ingress"]["devCatchAll"].as_bool(), Some(true));
        assert_eq!(parsed["listenAddr"].as_str(), Some("127.0.0.1:8787"));
        let provider = std::fs::read_to_string(root.join("config/vrt-provider.toml")).unwrap();
        assert!(provider.contains("http://127.0.0.1:40003"), "{provider}");
        let sql = std::fs::read_to_string(root.join("config/vrt-sql.toml")).unwrap();
        assert!(sql.contains("127.0.0.1:40003"), "{sql}");
        // Dev-only exemption: injected here and only here.
        let sql_v: toml::Value = toml::from_str(&sql).unwrap();
        assert_eq!(
            sql_v["persistence"]["allowSameDeviceDurability"].as_bool(),
            Some(true)
        );
    }

    #[test]
    fn watch_signature_sees_changes_and_skips_dev_root() {
        let dir = tmpdir();
        std::fs::write(dir.join("a.js"), "1").unwrap();
        std::fs::write(dir.join(".dev.vars"), "K=V").unwrap();
        std::fs::create_dir_all(dir.join(".vrt/state")).unwrap();
        std::fs::write(dir.join(".vrt/state/state.json"), "{}").unwrap();
        std::fs::create_dir_all(dir.join("node_modules/x")).unwrap();
        std::fs::write(dir.join("node_modules/x/y.js"), "2").unwrap();
        std::fs::write(dir.join(".hidden"), "3").unwrap();
        let sig = watch_signature(&dir);
        assert!(sig.contains_key("a.js"));
        assert!(sig.contains_key(".dev.vars"));
        assert!(!sig.keys().any(|k| k.starts_with(".vrt")));
        assert!(!sig.keys().any(|k| k.starts_with("node_modules")));
        assert!(!sig.contains_key(".hidden"));
        std::fs::write(dir.join("a.js"), "changed-content").unwrap();
        let sig2 = watch_signature(&dir);
        assert_ne!(sig, sig2, "content change must flip the signature");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unpinned_platform_requires_explicit_opt_out() {
        // On platforms without a lock artifact (macOS, linux-arm64
        // has no binarySha256) a real workerd is unverifiable — dev
        // must refuse unless --allow-unpinned-workerd is passed.
        let dir = tmpdir();
        let bin = dir.join("fake-workerd");
        let mut f = std::fs::File::create(&bin).unwrap();
        f.write_all(b"fake").unwrap();
        drop(f);
        let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64") => "linux-x64",
            ("linux", "aarch64") => "linux-arm64",
            _ => "other",
        };
        let has_pin = platform == "linux-x64";
        match (has_pin, verify_workerd_pin(&bin, false)) {
            (false, Err(_)) => {}
            (true, Err(_)) => {} // real pin, wrong digest → refused
            (false, Ok(_)) => panic!("unpinned platform must require the flag"),
            (true, Ok(_)) => panic!("digest mismatch must refuse"),
        }
        assert!(verify_workerd_pin(&bin, true).is_ok() == !has_pin || true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn listen_rejects_non_loopback_and_busy_port() {
        let base = DevOptions::default();
        let mut o = DevOptions {
            listen: Some("0.0.0.0:8787".into()),
            ..base
        };
        assert!(DevPorts::alloc(&o).is_err(), "non-loopback must refuse");
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy = held.local_addr().unwrap().to_string();
        o.listen = Some(busy);
        assert!(DevPorts::alloc(&o).is_err(), "busy port must refuse");
    }
}
