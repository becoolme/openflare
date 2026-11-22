//! `of serve` — standalone control-plane orchestration (T17).
//!
//! One process owns three duties:
//! 1. the management API on `mgmt/vrt.sock` (see [`crate::mgmt`]);
//! 2. the envelope renewal timer — re-signs while `remaining < TTL/3`,
//!    with bounded retries; sustained failure surfaces on /healthz;
//! 3. OPTIONAL child supervision (`--supervise`): spawn + restart
//!    `vrt-host` and `vrt-provider` from `root/config/*.toml` with
//!    bounded backoff; an unrecoverable child exits serve non-zero so
//!    the outer supervisor (compose/systemd) can restart the unit.
//!
//! Compose deployments run each binary in its own container — `serve`
//! then runs duties 1+2 only (`--no-supervise`, the compose default).

use crate::LocalError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Envelope TTL kept in sync with the library constant (24 h).
const TTL: Duration = Duration::from_secs(24 * 3600);
/// How often the renewal loop checks expiry.
const RENEW_TICK: Duration = Duration::from_secs(60);
/// Max consecutive renewal failures retried inside one tick cycle
/// before /healthz reports — the loop keeps trying every tick until
/// expiry, never silently gives up.
const RENEW_RETRY_PER_TICK: u32 = 2;
/// Child restart cap per 60s window before serve exits non-zero.
const MAX_CHILD_RESTARTS: u32 = 5;
/// Restart backoff ceiling.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// `of serve` options.
#[derive(Clone, Debug)]
pub struct ServeOptions {
    /// Supervise vrt-host/vrt-provider children (bare-metal mode).
    /// `false` → API + renewal only (compose mode).
    pub supervise: bool,
    /// vrt-host binary (PATH lookup when None).
    pub vrt_host_bin: Option<PathBuf>,
    /// vrt-provider binary (PATH lookup when None).
    pub vrt_provider_bin: Option<PathBuf>,
    /// vrt-sql binary (`of dev` / bare-metal). When set,
    /// `config/vrt-sql.toml` must exist and vrt-sql is supervised as
    /// a third child; when unset the child set is unchanged.
    pub vrt_sql_bin: Option<PathBuf>,
    /// Extra args passed to each child after `--config`.
    pub extra_args: Vec<String>,
    /// External stop channel (`of dev`, tests): a `true` send ends
    /// serve exactly like SIGINT. `None` in production serve.
    pub stop: Option<tokio::sync::watch::Receiver<bool>>,
    /// A2: optional TCP management listener (`--mgmt-tcp-listen`).
    /// Off unless explicitly configured — see `MgmtTcpOptions`.
    pub mgmt_tcp: Option<MgmtTcpOptions>,
}

/// A2: the optional TCP management listener — for remote
/// `of deploy --url`. Deliberately flag-only (never a config-file
/// default): an operator must opt in per process, bind defaults to
/// loopback, auth uses a SEPARATE 0600 token file from the unix
/// socket's `mgmt/token`, bodies are capped, and sources are
/// rate-limited. The unix socket's security is never lowered.
#[derive(Clone, Debug)]
pub struct MgmtTcpOptions {
    /// Bind address — `127.0.0.1:…` unless the operator explicitly
    /// chooses otherwise (a non-loopback bind warns: remote deploys
    /// should arrive over an SSH tunnel or TLS-terminating proxy).
    pub listen: std::net::SocketAddr,
    /// Bearer token file — `0600`, owner-only, 64 hex chars
    /// (`keys::load_token_file` enforces all three).
    pub token_file: PathBuf,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            supervise: false,
            vrt_host_bin: None,
            vrt_provider_bin: None,
            vrt_sql_bin: None,
            extra_args: Vec::new(),
            stop: None,
            mgmt_tcp: None,
        }
    }
}

/// Run the control plane until signal or fatal error.
pub async fn run(root: &Path, opts: &ServeOptions) -> Result<(), LocalError> {
    // Layout + permissions validated up front — fail closed.
    crate::layout::check(root)?;
    let (router, api) = crate::mgmt::router(root.to_path_buf())?;
    let sock = crate::mgmt::socket_path(root);
    vrt_securefs::remove_stale_socket(&sock).map_err(|_| LocalError::Layout("socket"))?;
    // F1: the socket is 0600 from birth — `bind_unix_private` stages the
    // bind inside a private dir and renames it into place, so no window
    // where `vrt.sock` exists world-accessible.
    let listener = bind_unix(&sock)?;

    // A2: optional TCP management listener — off unless explicitly
    // configured. A distinct 0600 token file and per-source-IP rate
    // limiting apply; the unix socket above is unaffected.
    let mut tcp_task = match &opts.mgmt_tcp {
        Some(tcp) => {
            let token = crate::keys::load_token_file(&tcp.token_file)?;
            if !tcp.listen.ip().is_loopback() {
                tracing::warn!(
                    addr = %tcp.listen,
                    "mgmt TCP listener bound to a non-loopback address —                      remote deploys should reach it over an SSH tunnel                      or a TLS-terminating proxy"
                );
            }
            let tcp_listener = tokio::net::TcpListener::bind(tcp.listen)
                .await
                .map_err(|_| LocalError::Layout("mgmt tcp bind"))?;
            let tcp_router =
                crate::mgmt::tcp_router(api.clone(), token, crate::mgmt::RateLimit::new());
            Some(tokio::task::spawn(async move {
                let _ = axum::serve(
                    tcp_listener,
                    tcp_router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await;
            }))
        }
        None => None,
    };

    let mut children = if opts.supervise {
        supervise_setup(root, opts)?
    } else {
        Vec::new()
    };
    let mut ext_stop = opts.stop.clone();

    let renewal_root = root.to_path_buf();
    let renewal_api = Arc::clone(&api);
    let mut renewal = tokio::task::spawn(async move {
        renewal_loop(&renewal_root, &renewal_api).await;
    });
    let mut server = tokio::task::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    // T21/F1d: standalone cron — the SAME `cell_controller::cron::
    // CronRunner`, single-instance MemoryLeaseStore (the state lock
    // already guarantees one serve), durable FileTriggerLog claims,
    // and HttpCronInvoker into vrt-host's control listener — the same
    // `/v1/cron/trigger` contract the platform cell uses.
    let cron_root = root.to_path_buf();
    let mut cron_task = tokio::task::spawn(async move {
        cron_loop(&cron_root).await;
    });
    // T37b: standalone queue dispatch — same QueueDispatcher the
    // platform cell runs, ticking off the local signed envelope and
    // the single-pod assignment (consumer batchTimeout ≥ 1s needs a
    // 1s cadence, so this is its own task like cron_loop).
    let qd_root = root.to_path_buf();
    let mut qd_task = tokio::task::spawn(async move {
        queue_dispatch_loop(&qd_root).await;
    });

    // First exit / signal wins; children are reaped on drop.
    let outcome = tokio::select! {
        _ = &mut server => Ok(()),
        _ = &mut renewal => Ok(()),
        // G1: cron_loop never returns on its own (disabled → pend);
        // a JoinError here is a panic — fatal.
        r = &mut cron_task => match r {
            Ok(()) => Ok(()),
            Err(e) => Err(LocalError::Io(std::io::Error::other(format!("cron task panicked: {e}")))),
        },
        r = &mut qd_task => match r {
            Ok(()) => Ok(()),
            Err(e) => Err(LocalError::Io(std::io::Error::other(format!("queue dispatch task panicked: {e}")))),
        },
        r = supervise_watch(&mut children), if !children.is_empty() => r,
        // A TCP management listener that stops serving is a lost
        // remote-deploy surface — surface it, don't silently run on.
        r = async {
            match tcp_task.as_mut() {
                Some(t) => t.await,
                None => std::future::pending().await,
            }
        } => match r {
            Ok(()) => Err(LocalError::Layout("mgmt tcp listener stopped")),
            Err(e) => Err(LocalError::Io(std::io::Error::other(format!("mgmt tcp panicked: {e}")))),
        },
        _ = shutdown_signal() => Ok(()),
        // External stop (`of dev`, tests) — same effect as SIGINT.
        _ = async {
            match ext_stop.as_mut() {
                Some(rx) => { let _ = rx.changed().await; }
                None => std::future::pending::<()>().await,
            }
        } => Ok(()),
    };
    server.abort();
    renewal.abort();
    cron_task.abort();
    qd_task.abort();
    if let Some(t) = tcp_task.take() {
        t.abort();
    }
    kill_children(&mut children).await;
    outcome
}

/// Bind a unix listener at `path`, retrying with a cwd-relative
/// path when the absolute form exceeds `sun_path` (deep worktrees on
/// macOS hit the ~104-byte limit even though the filesystem path is
/// legal — production `/data`-style roots never take the fallback).
pub fn bind_unix(path: &Path) -> Result<tokio::net::UnixListener, std::io::Error> {
    match bind_unix_private_tokio(path) {
        Ok(l) => Ok(l),
        Err(e) if is_sun_len(&e) => bind_unix_private_tokio(&relative_from_cwd(path)?),
        Err(e) => Err(e),
    }
}

/// F1: `vrt_securefs::bind_unix_private` (0600-from-birth via staged
/// rename) adapted to tokio's listener type.
fn bind_unix_private_tokio(path: &Path) -> Result<tokio::net::UnixListener, std::io::Error> {
    let std_listener = vrt_securefs::bind_unix_private(path, 0o600)?;
    std_listener.set_nonblocking(true)?;
    tokio::net::UnixListener::from_std(std_listener)
}

/// Connect a unix stream with the same relative-path fallback as
/// [`bind_unix`] (clients hit `sun_path` too).
pub async fn connect_unix(path: &Path) -> Result<tokio::net::UnixStream, std::io::Error> {
    match tokio::net::UnixStream::connect(path).await {
        Ok(s) => Ok(s),
        Err(e) if is_sun_len(&e) => tokio::net::UnixStream::connect(relative_from_cwd(path)?).await,
        Err(e) => Err(e),
    }
}

fn is_sun_len(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::InvalidInput && e.to_string().contains("SUN_LEN")
}

/// `path` (canonical) re-expressed relative to the process cwd —
/// `..`-prefixed when the trees diverge. `same-file` free: pure
/// component arithmetic on absolute paths.
fn relative_from_cwd(path: &Path) -> Result<PathBuf, std::io::Error> {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let cwd = std::env::current_dir()?;
    let mut pre = Vec::new();
    for anc in cwd.ancestors() {
        if let Ok(rel) = path.strip_prefix(anc) {
            let mut out = PathBuf::new();
            for _ in 0..pre.len() {
                out.push("..");
            }
            out.push(rel);
            return Ok(out);
        }
        pre.push(());
    }
    Ok(path)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Renew when `remaining < TTL/3`; bounded retries inside a tick,
/// outcome recorded for /healthz. Never exits on renewal failure —
/// expiry makes the envelope itself invalid (fail closed downstream)
/// while /healthz keeps reporting the broken state.
async fn renewal_loop(root: &Path, api: &Arc<crate::mgmt::Api>) {
    loop {
        tokio::time::sleep(RENEW_TICK).await;
        let expiry = crate::desired::read_envelope_expiry_ms(root);
        let remaining = expiry.map(|e| {
            Duration::from_millis((e - chrono::Utc::now().timestamp_millis()).max(0) as u64)
        });
        // No envelope yet (nothing deployed) → nothing to renew.
        let Some(remaining) = remaining else {
            continue;
        };
        if remaining >= TTL / 3 {
            continue;
        }
        let root = root.to_path_buf();
        for _ in 0..=RENEW_RETRY_PER_TICK {
            let r = {
                let root = root.clone();
                tokio::task::spawn_blocking(move || {
                    crate::VrtLocal::open(&root).and_then(|l| l.renew())
                })
                .await
                .map_err(|_| LocalError::Locked)
                .and_then(|r| r)
            };
            crate::mgmt::record_renewal(api, &r);
            if r.is_ok() {
                break;
            }
            tracing::warn!("envelope renewal failed; retrying next tick");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

struct Child {
    name: &'static str,
    bin: PathBuf,
    config: PathBuf,
    extra: Vec<String>,
    proc: tokio::process::Child,
    restarts: u32,
    window_start: Instant,
    backoff: Duration,
}

fn supervise_setup(root: &Path, opts: &ServeOptions) -> Result<Vec<Child>, LocalError> {
    let mut children = Vec::new();
    let mut specs: Vec<(&'static str, &Option<PathBuf>, &'static str)> = vec![
        ("vrt-host", &opts.vrt_host_bin, "vrt-host.toml"),
        ("vrt-provider", &opts.vrt_provider_bin, "vrt-provider.toml"),
    ];
    // `of dev` / bare-metal adds vrt-sql — opt-in so the compose and
    // bare-metal child sets stay exactly as before when unset.
    if opts.vrt_sql_bin.is_some() {
        specs.push(("vrt-sql", &opts.vrt_sql_bin, "vrt-sql.toml"));
    }
    for (name, bin, cfg) in specs {
        let bin = bin.clone().unwrap_or_else(|| PathBuf::from(name));
        let config = crate::layout::config_dir(root).join(cfg);
        if !config.is_file() {
            return Err(LocalError::Layout("child config missing"));
        }
        let proc = spawn_child(&bin, &config, &opts.extra_args)?;
        children.push(Child {
            name,
            bin,
            config,
            extra: opts.extra_args.clone(),
            proc,
            restarts: 0,
            window_start: Instant::now(),
            backoff: Duration::from_secs(1),
        });
    }
    Ok(children)
}

fn spawn_child(
    bin: &Path,
    config: &Path,
    extra: &[String],
) -> Result<tokio::process::Child, LocalError> {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--config").arg(config);
    for a in extra {
        cmd.arg(a);
    }
    cmd.kill_on_drop(true);
    cmd.spawn().map_err(|_| LocalError::Layout("child spawn"))
}

/// Wait for any child exit; restart with bounded backoff. A child
/// that crashes > MAX_CHILD_RESTARTS within 60s is unrecoverable →
/// serve exits non-zero so the outer supervisor can restart it.
async fn supervise_watch(children: &mut [Child]) -> Result<(), LocalError> {
    // Poll — the set is tiny and this keeps the code simple.
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        for c in children.iter_mut() {
            match c.proc.try_wait() {
                Ok(Some(status)) => {
                    if c.window_start.elapsed() > Duration::from_secs(60) {
                        c.restarts = 0;
                        c.window_start = Instant::now();
                        c.backoff = Duration::from_secs(1);
                    }
                    c.restarts += 1;
                    if c.restarts > MAX_CHILD_RESTARTS {
                        tracing::error!(
                            child = c.name,
                            ?status,
                            "child crashed repeatedly; giving up"
                        );
                        return Err(LocalError::Layout("child unrecoverable"));
                    }
                    tracing::warn!(child = c.name, ?status, backoff = ?c.backoff, "child exited; restarting");
                    tokio::time::sleep(c.backoff).await;
                    c.backoff = (c.backoff * 2).min(MAX_BACKOFF);
                    c.proc = spawn_child(&c.bin, &c.config, &c.extra)?;
                }
                Ok(None) => {}
                Err(_) => return Err(LocalError::Layout("child wait")),
            }
        }
    }
}

/// Grace window between SIGTERM and SIGKILL per child — vrt-host
/// needs a beat to stop its own workerd children so a serve exit
/// never leaves orphaned runtimes behind.
const CHILD_TERM_GRACE: Duration = Duration::from_secs(5);

async fn kill_children(children: &mut [Child]) {
    // Graceful first: SIGTERM lets each service drain and stop its
    // own children (vrt-host → workerd); SIGKILL only on timeout so
    // `of dev` Ctrl+C never orphans a runtime.
    for c in children.iter_mut() {
        if let Some(pid) = c.proc.id() {
// SAFETY: the pid comes from a Child this process spawned; SIGTERM has no further preconditions.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
    }
    for c in children.iter_mut() {
        if tokio::time::timeout(CHILD_TERM_GRACE, c.proc.wait())
            .await
            .is_err()
        {
            let _ = c.proc.kill().await;
            let _ = c.proc.wait().await;
        }
    }
}

/// Standalone cron loop (T21/F1d): every 30 s, read the local signed
/// envelope, build the single-pod assignment, and tick the shared
/// `CronRunner`. A missing `controlListen` in `vrt-host.toml` means
/// the host cannot receive triggers — cron is disabled (logged once,
/// status file updated, then the task PENDS forever: a disabled
/// capability must never tear down `of serve`). Only a panic inside
/// the runner is fatal, and that surfaces through the JoinHandle.
fn cron_status_path(root: &Path) -> std::path::PathBuf {
    crate::layout::state_dir(root).join("cron-status.json")
}

/// Persist the observable cron state for `/v1/status` — best effort;
/// a status write failure must not kill the loop.
fn write_cron_status(root: &Path, enabled: bool, reason: Option<&str>) {
    let _ = std::fs::create_dir_all(crate::layout::state_dir(root));
    let body = serde_json::json!({
        "enabled": enabled,
        "reason": reason,
        "updatedAt": chrono::Utc::now().to_rfc3339(),
    });
    let path = cron_status_path(root);
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, body.to_string()).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// T37b-r3: signed-desired authority for the local consumers —
/// cell scope + the local verifier record. Without either, NO envelope
/// can be proven genuine: the consumer disables (pend), matching every
/// other degraded-but-alive capability.
fn desired_authority(root: &Path) -> Option<(String, snapshot_protocol::TrustedKey)> {
    let st = crate::state::read(root).ok()?;
    let trusted = crate::keys::load_trusted_key(root).ok()?;
    Some((st.cell_id().to_string(), trusted))
}

async fn cron_loop(root: &Path) {
    let disabled = |reason: &'static str| {
        write_cron_status(root, false, Some(reason));
        tracing::error!(reason, "cron disabled");
    };
    let control = match host_control_base(root) {
        Some(base) => base,
        None => {
            disabled(
                "vrt-host.toml has no controlListen — scheduled triggers cannot reach the runtime",
            );
            return std::future::pending::<()>().await;
        }
    };
    let log = match cell_controller::cron::FileTriggerLog::open(&root.join("state").join("cron")) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, "cron disabled: claim log unavailable");
            disabled("claim log unavailable");
            return std::future::pending::<()>().await;
        }
    };
    let invoker = match cell_controller::cron::HttpCronInvoker::new(
        &[("local".to_string(), control)],
        // CR1: must exceed vrt-host's DISPATCH_TIMEOUT (30s) — a
        // shorter invoker reports Delivery failure while the host is
        // still running scheduled(), which misreports an outcome the
        // facade may later succeed (the claim is already consumed
        // either way; honest reporting needs the real verdict).
        Duration::from_secs(35),
    ) {
        Ok(i) => i,
        Err(e) => {
            tracing::error!(error = %e, "cron disabled: invoker init failed");
            disabled("invoker init failed");
            return std::future::pending::<()>().await;
        }
    };
    let Some((cell_id, trusted)) = desired_authority(root) else {
        tracing::error!("cron disabled: state/keys unreadable — no signed authority");
        write_cron_status(root, false, Some("authority unavailable"));
        return std::future::pending::<()>().await;
    };
    write_cron_status(root, true, None);
    let trusted_keys = [trusted];
    let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
    let mut runner = cell_controller::cron::CronRunner::new(
        cell_controller::lease::MemoryLeaseStore::default(),
        "vrt-serve",
        chrono::Duration::minutes(3),
        Box::new(log),
        Box::new(invoker),
    );
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    // Idle reasons log once then rate-limit (1/60s): Missing = no
    // authority yet (not an error), Invalid = verify failed (typed
    // code only — never payload data).
    let mut idle_log = std::time::Instant::now() - Duration::from_secs(61);
    loop {
        interval.tick().await;
        // T37b-r3: same verification as the host — the old parse-only
        // read also failed to decode the RFC3339 `expiresAt`, so this
        // loop never had authority before.
        let (desired, expires_at) = match crate::desired::read_verified_desired(
            root,
            &cell_id,
            &trusted_keys,
            &mut guard,
            chrono::Utc::now(),
        ) {
            Ok(v) => v,
            Err(kind) => {
                if idle_log.elapsed() >= Duration::from_secs(60) {
                    match kind {
                        crate::desired::DesiredRead::Missing => {
                            tracing::warn!("cron: no signed desired envelope yet; idling");
                        }
                        crate::desired::DesiredRead::Invalid(code) => {
                            tracing::warn!(code, "cron: desired envelope rejected; idling");
                        }
                    }
                    idle_log = std::time::Instant::now();
                }
                continue;
            }
        };
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "local".to_string(),
            cell_controller::assign::PodAssignment {
                pod_id: "local".into(),
                revision: desired.revision,
                payload_digest: String::new(),
                envelope_digest: String::new(),
                generated_at: String::new(),
                workers: desired
                    .tenants
                    .iter()
                    .flat_map(|t| {
                        t.workers
                            .iter()
                            .map(|w| cell_controller::assign::AssignedWorker {
                                tenant_id: t.tenant_id.clone(),
                                worker_id: w.worker_id.clone(),
                            })
                    })
                    .collect(),
            },
        );
        let outcome = runner
            .tick(
                Some(&desired),
                Some(expires_at),
                &assignments,
                chrono::Utc::now(),
            )
            .await;
        if !outcome.fired.is_empty() || !outcome.failed.is_empty() {
            tracing::info!(
                fired = outcome.fired.len(),
                failed = outcome.failed.len(),
                "cron tick"
            );
        }
    }
}

/// Standalone queue dispatcher (T37b): 1s tick over the local signed
/// envelope — identical `QueueDispatcher` semantics to the platform
/// cell. Disabled-but-pending when `controlListen` or `[vrtSql]` is
/// absent (a disabled capability never tears down `of serve`).
async fn queue_dispatch_loop(root: &Path) {
    let Some(control) = host_control_base(root) else {
        tracing::error!("queue dispatch disabled: vrt-host.toml has no controlListen");
        return std::future::pending::<()>().await;
    };
    // `[vrtSql] addr` + `authSecretFile` from vrt-provider.toml —
    // the dispatcher shares the provider's data-plane channel.
    let (sql_addr, secret_file) = match provider_sql(root) {
        Some(v) => v,
        None => {
            tracing::error!("queue dispatch disabled: vrt-provider.toml has no [vrtSql]");
            return std::future::pending::<()>().await;
        }
    };
    let secret = match std::fs::read(&secret_file) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "queue dispatch disabled: sql auth secret unreadable");
            return std::future::pending::<()>().await;
        }
    };
    let sql = match cell_controller::queue_dispatch::QueueSqlClient::with_policy(
        &sql_addr,
        secret,
        Duration::from_secs(10),
        false, // standalone: operator-configured internal endpoint
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "queue dispatch disabled: sql client init failed");
            return std::future::pending::<()>().await;
        }
    };
    let invoker = match cell_controller::queue_dispatch::HttpQueueInvoker::new(
        &[("local".to_string(), control)],
        Duration::from_secs(60),
    ) {
        Ok(i) => i,
        Err(e) => {
            tracing::error!(error = %e, "queue dispatch disabled: invoker init failed");
            return std::future::pending::<()>().await;
        }
    };
    let dispatcher = match cell_controller::queue_dispatch::QueueDispatcher::new(
        std::sync::Arc::new(sql),
        std::sync::Arc::new(invoker),
        cell_controller::queue_dispatch::DispatchConfig {
            holder_id: "vrt-serve".into(),
            ..Default::default()
        },
    ) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(error = %e, "queue dispatch disabled: invalid config");
            return std::future::pending::<()>().await;
        }
    };
    tracing::info!("queue dispatcher enabled (standalone)");
    let Some((cell_id, trusted)) = desired_authority(root) else {
        tracing::error!("queue dispatch disabled: state/keys unreadable — no signed authority");
        return std::future::pending::<()>().await;
    };
    let trusted_keys = [trusted];
    let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // T37b-r observability: log STATE TRANSITIONS, not per-tick noise.
    // `desired_missing` flips on the first absent envelope (and once
    // more if it stays missing for ≥60 ticks) — before this, an
    // absent/malformed envelope just `continue`d silently forever.
    let mut desired_missing = false;
    let mut last_desired_log = std::time::Instant::now() - Duration::from_secs(61);
    let mut last_err_tick: Option<std::time::Instant> = None;
    let mut lease_held_before = false;
    let mut last_consumer_set: Option<Vec<(String, String, String)>> = None;
    let empty_assignments: std::collections::BTreeMap<
        String,
        cell_controller::assign::PodAssignment,
    > = std::collections::BTreeMap::new();
    loop {
        interval.tick().await;
        // T37b-r3: verified re-read EVERY tick — the dispatcher follows
        // the latest signed+verified desired (the earlier parse-only
        // read returned None forever: `expiresAt` is an RFC3339 string,
        // not ms). Missing/tampered/expired/conflicting → no authority:
        // tick(None) tears lanes down NOW rather than dispatching the
        // last good set, and keeps the lease gauge alive as the
        // "dispatcher up, idle" baseline for diagnostics.
        let (desired, expires_at) = match crate::desired::read_verified_desired(
            root,
            &cell_id,
            &trusted_keys,
            &mut guard,
            chrono::Utc::now(),
        ) {
            Ok(v) => v,
            Err(kind) => {
                if !desired_missing || last_desired_log.elapsed() >= Duration::from_secs(60) {
                    match kind {
                        crate::desired::DesiredRead::Missing => {
                            tracing::warn!(
                                "queue dispatch: no signed desired envelope yet; idling"
                            );
                        }
                        crate::desired::DesiredRead::Invalid(code) => {
                            tracing::warn!(
                                code,
                                "queue dispatch: desired envelope rejected; idling"
                            );
                        }
                    }
                    last_desired_log = std::time::Instant::now();
                }
                desired_missing = true;
                if last_consumer_set.take().is_some() {
                    tracing::info!("queue dispatch: consumer set cleared (desired lost)");
                }
                dispatcher
                    .tick(None, None, &empty_assignments, chrono::Utc::now())
                    .await;
                continue;
            }
        };
        if desired_missing {
            tracing::info!("queue dispatch: desired envelope present; resuming");
        }
        desired_missing = false;
        // Consumer-set transitions are the operator's signal — one info
        // per change (revision + member count), never per tick.
        let mut consumer_set: Vec<(String, String, String)> = desired
            .tenants
            .iter()
            .flat_map(|t| {
                t.workers.iter().flat_map(|w| {
                    w.queue_consumers
                        .iter()
                        .map(|c| (t.tenant_id.clone(), w.worker_id.clone(), c.queue_id.clone()))
                })
            })
            .collect();
        consumer_set.sort();
        if last_consumer_set.as_ref() != Some(&consumer_set) {
            tracing::info!(
                revision = desired.revision,
                consumers = consumer_set.len(),
                "queue dispatch: consumer set applied"
            );
            last_consumer_set = Some(consumer_set);
        }
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "local".to_string(),
            cell_controller::assign::PodAssignment {
                pod_id: "local".into(),
                revision: desired.revision,
                payload_digest: String::new(),
                envelope_digest: String::new(),
                generated_at: String::new(),
                workers: desired
                    .tenants
                    .iter()
                    .flat_map(|t| {
                        t.workers
                            .iter()
                            .map(|w| cell_controller::assign::AssignedWorker {
                                tenant_id: t.tenant_id.clone(),
                                worker_id: w.worker_id.clone(),
                            })
                    })
                    .collect(),
            },
        );
        let out = dispatcher
            .tick(
                Some(&desired),
                Some(expires_at),
                &assignments,
                chrono::Utc::now(),
            )
            .await;
        // Lease election transition logging (T37b-r): first win and
        // every change of held-queue set logs at info; losing the
        // last lease is a warn — the dispatcher keeps retrying.
        if out.lease_held > 0 && !lease_held_before {
            tracing::info!(held = out.lease_held, "queue dispatch: lease acquired");
        } else if out.lease_held == 0 && lease_held_before && out.lease_lost > 0 {
            tracing::warn!(
                lease_lost = out.lease_lost,
                "queue dispatch: lease lost to another holder"
            );
        }
        lease_held_before = out.lease_held > 0;
        // Failure surfaces at warn (rate-limited to 1/30s — a dead
        // vrt-sql otherwise spams every 1s tick); recoveries log once.
        if out.lease_lost > 0 || out.claim_errors > 0 {
            let first = last_err_tick.is_none();
            if first || last_err_tick.is_some_and(|t| t.elapsed() >= Duration::from_secs(30)) {
                tracing::warn!(
                    lease_lost = out.lease_lost,
                    claim_errors = out.claim_errors,
                    suppressed = !first,
                    "queue dispatch tick degraded (vrt-sql down or lease held elsewhere)"
                );
                last_err_tick = Some(std::time::Instant::now());
            }
        } else if last_err_tick.take().is_some() {
            tracing::info!("queue dispatch tick recovered");
        }
    }
}

/// `[vrtSql] addr`/`authSecretFile` out of `config/vrt-provider.toml`.
/// T37b-r4: `of queue-metrics` (CLI diagnostics) shares the same read path
/// with the dispatcher/cron.
pub fn provider_sql(root: &Path) -> Option<(String, std::path::PathBuf)> {
    let text =
        std::fs::read_to_string(crate::layout::config_dir(root).join("vrt-provider.toml")).ok()?;
    let doc: toml::Value = toml::from_str(&text).ok()?;
    let sql = doc.get("vrtSql")?;
    let addr = sql.get("addr")?.as_str()?.to_string();
    let secret = sql.get("authSecretFile")?.as_str()?;
    Some((addr, std::path::PathBuf::from(secret)))
}

/// B12: fetch `/metrics` from the host control listener and return a
/// small allowlisted summary for `of status`. Unreachable host →
/// `reachable:false` (never blocks status on a downed host).
pub fn metrics_summary(root: &Path) -> Option<serde_json::Value> {
    let base = host_control_base(root)?;
    let authority = base.strip_prefix("http://")?;
    let stream = std::net::TcpStream::connect(authority).ok()?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    use std::io::{Read, Write};
    let mut stream = stream;
    write!(
        stream,
        "GET /metrics HTTP/1.0\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).ok()?;
    let body = buf.split("\r\n\r\n").nth(1).unwrap_or("");
    // Allowlisted aggregates — no label values are surfaced, only sums.
    let sum = |name: &str| -> f64 {
        body.lines()
            .filter(|l| !l.starts_with('#'))
            .filter(|l| {
                l.starts_with(name)
                    && (l[name.len()..].starts_with('{') || l[name.len()..].starts_with(' '))
            })
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum()
    };
    let ready_tenants = body
        .lines()
        .filter(|l| l.starts_with("vrt_host_ready"))
        .filter(|l| l.rsplit(' ').next() == Some("1"))
        .count();
    Some(serde_json::json!({
        "endpoint": format!("{base}/metrics"),
        "reachable": true,
        "tenantProcesses": sum("vrt_host_tenant_processes"),
        "readyTenants": ready_tenants,
        "requestsTotal": sum("vrt_worker_requests_total"),
        "exceptionsTotal": sum("vrt_worker_exceptions_total"),
        "launchesTotal": sum("vrt_host_launches_total"),
    }))
}

/// Parse `controlListen = "addr"` out of `root/config/vrt-host.toml`.
fn host_control_base(root: &Path) -> Option<String> {
    let text =
        std::fs::read_to_string(crate::layout::config_dir(root).join("vrt-host.toml")).ok()?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("controlListen") {
            let value = rest.trim_start_matches([' ', '=']).trim_matches('"');
            let value = value
                .split('#')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"');
            if !value.is_empty() {
                return Some(format!("http://{value}"));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// G1: no `controlListen` → cron_loop must NOT return (a disabled
    /// capability cannot tear down serve); it must write a typed
    /// disabled status that /v1/status can surface.
    #[tokio::test]
    async fn cron_disabled_pends_and_reports_status() {
        let root = std::env::temp_dir().join(format!(
            "vrt-serve-cron-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        // No config/vrt-host.toml at all → host_control_base misses.
        let root2 = root.clone();
        let handle = tokio::spawn(async move { cron_loop(&root2).await });
        // Give the task a poll — it should have written the status and
        // then pended forever.
        tokio::task::yield_now().await;
        let status: serde_json::Value = serde_json::from_slice(
            &std::fs::read(cron_status_path(&root)).expect("status file written"),
        )
        .unwrap();
        assert_eq!(status["enabled"], false);
        assert!(status["reason"].as_str().unwrap().contains("controlListen"));
        // And the task is still pending — serve stays alive.
        assert!(!handle.is_finished(), "disabled cron must not exit serve");
        handle.abort();
    }

    /// T37b-r3 regression: the standalone dispatcher must FOLLOW the
    /// signed desired — idle while no consumer exists (claims==0, not
    /// merely no hits), deliver after a consumer deploy, stop after a
    /// re-deploy drops the consumer, stay stopped after the envelope is
    /// tampered with, and resume on the next good revision. Everything
    /// goes through the production assembly path: `VrtLocal::deploy`
    /// signs+publishes; `queue_dispatch_loop` verifies+dispatches.
    #[tokio::test]
    async fn dispatcher_follows_signed_desired() {
        use axum::{extract::State, routing::post, Json, Router};
        use std::sync::atomic::{AtomicU64, Ordering};
        let _ = tracing_subscriber::fmt()
            .with_env_filter("vrt_local=debug,cell_controller=debug")
            .with_test_writer()
            .try_init();

        async fn sql_op(
            State(c): State<Arc<AtomicU64>>,
            axum::extract::Path((_qid, op)): axum::extract::Path<(String, String)>,
            Json(_v): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            match op.as_str() {
                "claim" => {
                    let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                    Json(serde_json::json!({
                        "success": true, "leaseId": "l", "dispatchEpoch": 1,
                        "messages": [{"id": format!("m{n}"), "leaseId": "l",
                            "attempts": 1, "body": "00",
                            "contentType": "bytes", "timestampMs": 1}]}))
                }
                "ack" | "retry" | "dead-letter" | "send" => {
                    Json(serde_json::json!({"success": true, "enqueued": 1, "duplicates": 0}))
                }
                _ => Json(serde_json::json!({"success": false,
                    "error": {"code": "X", "message": "x"}})),
            }
        }
        async fn ctl_hit(State(h): State<Arc<AtomicU64>>) -> Json<serde_json::Value> {
            h.fetch_add(1, Ordering::SeqCst);
            Json(serde_json::json!({"accepted": true}))
        }

        let root = std::env::temp_dir().join(format!(
            "vrt-serve-qd-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        crate::VrtLocal::init(
            &root,
            &crate::InitParams {
                cell_id: "local".into(),
                tenant_id: "local".into(),
                facade_manifest: None,
                facade_dist_root: None,
                max_versions_per_worker: None,
            },
        )
        .unwrap();
        let local = crate::VrtLocal::open(&root).unwrap();

        let claims = Arc::new(AtomicU64::new(0));
        // Wire contract: POST /queues/{id}/{op} (queue_dispatch.rs call()).
        let sql = Router::new()
            .route("/queues/{qid}/{op}", post(sql_op))
            .with_state(claims.clone());
        let sql_lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sql_addr = sql_lis.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(sql_lis, sql).await.unwrap() });

        let hits = Arc::new(AtomicU64::new(0));
        let ctl = Router::new()
            .fallback(post(ctl_hit))
            .with_state(hits.clone());
        let ctl_lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctl_addr = ctl_lis.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(ctl_lis, ctl).await.unwrap() });

        let cfg = root.join("config");
        std::fs::create_dir_all(&cfg).unwrap();
        let secret = root.join("sql.secret");
        std::fs::write(&secret, b"deadbeef").unwrap();
        std::fs::write(
            cfg.join("vrt-provider.toml"),
            format!(
                "[vrtSql]\naddr = \"http://{sql_addr}\"\nauthSecretFile = \"{}\"\n",
                secret.display()
            ),
        )
        .unwrap();
        std::fs::write(
            cfg.join("vrt-host.toml"),
            format!("controlListen = \"{ctl_addr}\"\n"),
        )
        .unwrap();

        let r = root.clone();
        let dsp = tokio::spawn(async move { queue_dispatch_loop(&r).await });

        let write_worker = |consumers: bool| {
            let dir = root.join("w-qw");
            std::fs::create_dir_all(&dir).unwrap();
            let mut toml = "name = \"qw\"\nmainModule = \"main.js\"\n                 compatibilityDate = \"2024-09-25\"\ncompatibilityFlags = [\"nodejs_compat\"]\n"
                .to_string();
            if consumers {
                toml.push_str("[[queueConsumers]]\nqueue = \"qq\"\nmaxBatchTimeoutMs = 1000\n");
            }
            std::fs::write(dir.join("vrt.toml"), toml).unwrap();
            std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
            dir
        };
        async fn wait_hits(h: &AtomicU64, min: u64, secs: u64, what: &str) {
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_secs(secs) {
                if h.load(Ordering::SeqCst) >= min {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            panic!(
                "{what}: expected >= {min} queue hits within {secs}s, got {}",
                h.load(Ordering::SeqCst)
            );
        }
        async fn assert_frozen(h: &AtomicU64, c: &AtomicU64, secs: u64, what: &str) {
            let (h0, c0) = (h.load(Ordering::SeqCst), c.load(Ordering::SeqCst));
            tokio::time::sleep(Duration::from_secs(secs)).await;
            assert_eq!(
                (h.load(Ordering::SeqCst), c.load(Ordering::SeqCst)),
                (h0, c0),
                "{what}: dispatch must stop — hits/claims frozen"
            );
        }

        // 1) dispatcher already running, envelope valid but consumer
        //    set empty — no lane exists, so NOTHING is even claimed.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "no consumer → no hits");
        assert_eq!(claims.load(Ordering::SeqCst), 0, "no consumer → no claims");

        // 2) deploy a worker WITH a consumer → signed envelope rev 2 →
        //    the loop picks it up and delivers.
        local
            .deploy(&write_worker(true), &crate::DeployOverrides::default())
            .unwrap();
        wait_hits(&hits, 1, 12, "consumer deploy must start dispatching").await;

        // 3) re-deploy the SAME worker without [[queueConsumers]] →
        //    the consumer set shrinks to empty → lanes torn down.
        local
            .deploy(&write_worker(false), &crate::DeployOverrides::default())
            .unwrap();
        // in-flight deliveries drain within ~1 tick
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert_frozen(&hits, &claims, 3, "consumer removed").await;

        // 4) re-deploy WITH the consumer → dispatch resumes.
        local
            .deploy(&write_worker(true), &crate::DeployOverrides::default())
            .unwrap();
        wait_hits(&hits, 2, 12, "consumer re-deploy must resume dispatch").await;

        // 5) tamper with the envelope on disk → verified read fails →
        //    fail closed: lanes cleared, dispatch stops.
        std::fs::write(
            crate::layout::desired_dir(&root).join("envelope.json"),
            b"tampered-not-json",
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert_frozen(&hits, &claims, 3, "tampered envelope").await;

        // 6) a fresh signed revision must recover dispatch — idling on
        //    a bad file is not terminal.
        local
            .deploy(&write_worker(true), &crate::DeployOverrides::default())
            .unwrap();
        wait_hits(&hits, 3, 12, "valid revision must resume dispatch").await;

        dsp.abort();
    }
}
