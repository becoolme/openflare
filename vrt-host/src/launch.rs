//! `WorkerdLauncher` — typed child-process boundary.
//!
//! [`ProcessWorkerdLauncher`] spawns the tenant process with:
//!
//! - argv `workerd serve <config> --control-fd=3
//!   [--external-addr=<svc>=<addr>...] [extra_args...]` — the config is a
//!   path argument; secrets never reach argv.
//! - the child environment **cleared** then populated only from
//!   `spec.base_env` + `spec.env` (`SecretValue::expose()` is called at
//!   this boundary and nowhere else). Nothing is inherited from vrt-host.
//! - a `UnixStream::pair()` control channel whose child end is `dup2`'d
//!   onto fd 3 via `CommandExt::pre_exec` (the same mapping upstream's
//!   `command_fds::FdMapping{child_fd:3}` performs, without the crate).
//! - `process_group(0)` so the whole process group can be signalled
//!   (SIGTERM then SIGKILL) and `kill_on_drop` as a reaping safety net.
//! - piped stdout/stderr drained into byte/line counters — the content is
//!   tenant-controlled and may contain unregistered secrets, so it is
//!   never logged or retained (same discipline as upstream `LogCollector`
//!   which keeps lifecycle evidence, never bodies).

use crate::spec::TenantLaunchSpec;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

/// fd the control channel is mapped onto inside the child
/// (`--control-fd=3`, matching upstream).
pub const CONTROL_FD: std::os::fd::RawFd = 3;

/// A spawned tenant process, not yet known-ready.
pub struct LaunchedWorkerd {
    /// Child pid (== pgid, the child is a process-group leader).
    pub pid: u32,
    /// Owned child handle (kill-on-drop is set — dropping without an
    /// explicit reap still SIGKILLs and reaps through tokio's orphan
    /// collector).
    pub child: Child,
    /// Parent end of the fd-3 control channel.
    pub control: tokio::net::UnixStream,
    /// Bounded counters for drained stdout/stderr.
    pub log_stats: Arc<ChildLogStats>,
}

/// Byte/line counters for drained child output — evidence without
/// retaining tenant-controlled bytes.
#[derive(Debug, Default)]
pub struct ChildLogStats {
    /// stdout bytes drained.
    pub stdout_bytes: Arc<AtomicU64>,
    /// stderr bytes drained.
    pub stderr_bytes: Arc<AtomicU64>,
    /// T25/L16: bounded, sanitized tail of the child's stderr —
    /// surfaced in launch-failure diagnostics so a real workerd's
    /// "why" is visible without a manual container shell.
    pub stderr_tail: Option<Arc<LogTail>>,
}

/// T25/L16: bounded tail of sanitized child stderr.
///
/// - At most [`LOG_TAIL_CAP`] bytes retained (drop-oldest).
/// - Control characters (except `\n`) are replaced with `.` — the
///   tail can go into logs/diagnostics without terminal escape or
///   log-injection risk.
/// - `redact` needles (launch-time secret/var values, `spec.env`)
///   are replaced with `***` BEFORE the byte cap — a secret can never
///   survive in the retained tail.
#[derive(Debug)]
pub struct LogTail {
    inner: std::sync::Mutex<String>,
    needles: Vec<String>,
}

/// Max bytes of sanitized stderr retained per generation.
pub const LOG_TAIL_CAP: usize = 16 * 1024;
/// Max lines surfaced by [`LogTail::tail_lines`].
pub const LOG_TAIL_LINES: usize = 12;
/// Needles shorter than this are not redacted (single-char secrets
/// would destroy every diagnostic line; secrets are ≥32B anyway).
const NEEDLE_MIN: usize = 8;

impl LogTail {
    /// Fresh tail; `needles` are the values that must never appear —
    /// the caller passes every `spec.env` secret/var value.
    pub fn new(needles: Vec<String>) -> Self {
        Self {
            inner: std::sync::Mutex::new(String::new()),
            needles: needles
                .into_iter()
                .filter(|n| n.len() >= NEEDLE_MIN)
                .collect(),
        }
    }

    /// Sanitize (control chars → `.`, keep `\n`/`\t`) + needle
    /// redact — shared by [`push`] and the live stderr line emitter.
    pub fn sanitize(&self, chunk: &[u8]) -> String {
        let text = String::from_utf8_lossy(chunk);
        let mut clean = String::with_capacity(text.len());
        for ch in text.chars() {
            if ch == '\n' || (!ch.is_control() && ch != '\u{7f}') {
                clean.push(ch);
            } else if ch == '\n' || ch == '\t' {
                clean.push(ch);
            } else {
                clean.push('.');
            }
        }
        for needle in &self.needles {
            if clean.contains(needle.as_str()) {
                clean = clean.replace(needle.as_str(), "***");
            }
        }
        clean
    }

    /// Sanitize + redact + append; bounded at [`LOG_TAIL_CAP`].
    pub fn push(&self, chunk: &[u8]) {
        let clean = self.sanitize(chunk);
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.push_str(&clean);
        if guard.len() > LOG_TAIL_CAP {
            // Drop the oldest surplus; align to a char boundary.
            let mut start = guard.len() - LOG_TAIL_CAP;
            while start < guard.len() && !guard.is_char_boundary(start) {
                start += 1;
            }
            guard.drain(..start);
        }
    }

    /// Last `n` non-empty lines — for failure diagnostics.
    pub fn tail_lines(&self, n: usize) -> String {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let lines: Vec<&str> = guard
            .lines()
            .rev()
            .filter(|l| !l.trim().is_empty())
            .take(n)
            .collect();
        lines.into_iter().rev().collect::<Vec<_>>().join("\n")
    }
}

/// Launch failures — never embed secrets, argv, or tenant data.
#[derive(Debug, thiserror::Error)]
pub enum LaunchError {
    /// The spec itself was unusable (binary missing, no endpoints, …).
    #[error("invalid launch spec: {0}")]
    Spec(&'static str),
    /// OS-level spawn/pipe/fd failure.
    #[error("spawn failed: {0}")]
    Io(#[source] io::Error),
}

/// Spawn-side launcher abstraction.
#[async_trait::async_trait]
pub trait WorkerdLauncher: Send + Sync {
    /// Spawn the process for `spec`. Readiness is checked separately by
    /// [`crate::ready::wait_ready`] so callers own the timeout/kill policy.
    async fn launch(&self, spec: &TenantLaunchSpec) -> Result<LaunchedWorkerd, LaunchError>;
}

/// Production launcher: `workerd serve <config> --control-fd=3`.
/// The same code path launches `vrt-stub-workerd` in tests — the binary
/// is a spec input, so no stub-specific branch exists here.
#[derive(Debug, Default)]
pub struct ProcessWorkerdLauncher;

impl ProcessWorkerdLauncher {
    /// Fixed positional/flag arguments. `serve <config> --control-fd=3`
    /// first, then `--external-addr`, then caller extras. Secrets are
    /// impossible here by construction — nothing in argv is derived from
    /// `spec.env`.
    pub fn argv(spec: &TenantLaunchSpec) -> Vec<String> {
        let mut argv = vec![
            "serve".to_string(),
            spec.config_path.display().to_string(),
            format!("--control-fd={CONTROL_FD}"),
        ];
        argv.extend(external_addr_args(&spec.external_addrs));
        argv.extend(spec.extra_args.iter().cloned());
        argv
    }
}

/// `--external-addr=<service>=<addr>` argv fragments in the production
/// order/format. Shared by [`ProcessWorkerdLauncher::argv`] and tests
/// that spawn workerd outside the launcher (smoke paths drive the same
/// derivation — never hand-format the flag).
pub fn external_addr_args(external_addrs: &[vrt_config::ExternalAddr]) -> Vec<String> {
    external_addrs
        .iter()
        .map(|external| format!("--external-addr={}={}", external.service, external.address))
        .collect()
}

#[async_trait::async_trait]
impl WorkerdLauncher for ProcessWorkerdLauncher {
    async fn launch(&self, spec: &TenantLaunchSpec) -> Result<LaunchedWorkerd, LaunchError> {
        if spec.worker_endpoints.is_empty() {
            return Err(LaunchError::Spec("worker_endpoints"));
        }
        if !spec.binary.is_absolute() {
            return Err(LaunchError::Spec("binary"));
        }
        if !spec.binary.is_file() {
            return Err(LaunchError::Spec("binary"));
        }
        if !spec.config_path.is_file() {
            return Err(LaunchError::Spec("config_path"));
        }

        let (parent, child_end) =
            std::os::unix::net::UnixStream::pair().map_err(|e| LaunchError::Io(e.into()))?;
        parent.set_nonblocking(true).map_err(LaunchError::Io)?;
        let control = tokio::net::UnixStream::from_std(parent).map_err(LaunchError::Io)?;

        let mut cmd = Command::new(&spec.binary);
        cmd.args(ProcessWorkerdLauncher::argv(spec));
        cmd.env_clear();
        for (name, value) in &spec.base_env {
            cmd.env(name, value);
        }
        for (name, value) in &spec.env {
            // The only place a secret plaintext is materialized: straight
            // into the child environment, never argv/logs/diagnostics.
            cmd.env(name, value.expose());
        }
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.process_group(0);
        cmd.kill_on_drop(true);
        // Map the socketpair's child end onto fd 3. `dup2` is
        // async-signal-safe and clears CLOEXEC on the target, so fd 3
        // survives exec; the source fd is dropped by the Command right
        // after spawn.
        let child_raw = child_end.as_raw_fd();
        // T13/M2: per-tenant uid/gid. When the spec carries an
        // `TenantIsolation`, the child drops privileges in pre_exec —
        // order: clear supplementary groups, setgid, setuid, then
        // no_new_privs + dumpable=0 (dumpable=0 also stops other
        // tenants' ptrace and /proc/<pid>/environ reads).
        let isolation = spec.isolation;
        // T34f2/R3: captured for the post-prctl reparent race check —
        // if vrt-host dies between fork and pre_exec, `getppid()`
        // already differs and the child must not exec orphaned.
        let host_pid = std::process::id();
// SAFETY: pre_exec runs only async-signal-safe libc calls (fcntl/dup2/setgroups/setgid/setuid/prctl) on captured POD values, allocates nothing, and returns io::Error on failure.
        unsafe {
            cmd.pre_exec(move || {
                if child_raw == CONTROL_FD {
                    // dup2(fd, fd) is a no-op — the fd is already the
                    // target, just clear CLOEXEC so it survives exec.
                    if libc::fcntl(child_raw, libc::F_SETFD, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                } else if libc::dup2(child_raw, CONTROL_FD) < 0 {
                    return Err(io::Error::last_os_error());
                }
                #[cfg(target_os = "linux")]
                {
                    // T34f/R3: die with the parent. `process_group(0)`
                    // + GroupChild only help while vrt-host LIVES to
                    // signal the group — a SIGKILL'd or crashed host
                    // orphans workerd entirely. PR_SET_PDEATHSIG fires
                    // when the SPAWNING thread exits: safe because
                    // every spawn runs on the dedicated resident
                    // `workerd-spawner` thread (see
                    // spawn_on_resident_thread) — tokio workers can be
                    // retired after block_in_place, this one cannot.
                    // macOS has no equivalent — `workerd.pid` + the
                    // startup orphan reaper covers it.
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                // Standard race (both platforms): if the parent died
                // between fork and pre_exec, we're already reparented
                // — abort before exec instead of exec'ing an orphan.
                if libc::getppid() != host_pid as libc::pid_t {
                    return Err(io::Error::from_raw_os_error(libc::ESRCH));
                }
                if let Some(iso) = isolation {
                    // Fail closed: any error aborts the exec — the
                    // child never runs with host privileges.
                    let empty = libc::gid_t::MAX;
                    if libc::setgroups(1, &empty) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    if libc::setgid(iso.gid) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    if libc::setuid(iso.uid) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    #[cfg(target_os = "linux")]
                    {
                        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                            return Err(io::Error::last_os_error());
                        }
                        if libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) != 0 {
                            return Err(io::Error::last_os_error());
                        }
                    }
                }
                Ok(())
            });
        }
        // Keep the child fd alive across spawn: wrap in an OwnedFd held
        // until after `cmd.spawn` returns.
// SAFETY: the fd is a freshly created socketpair end that has not been wrapped or closed; from_raw_fd takes sole ownership, so it closes exactly once.
        let child_owned = unsafe { OwnedFd::from_raw_fd(child_end.into_raw_fd()) };
        // T13/M2 fail closed: applying a uid/gid identity requires
        // euid 0 (setgroups/setgid/setuid). If isolation is demanded
        // and we cannot apply it, refuse the launch rather than run
        // the tenant with host privileges.
// SAFETY: geteuid(2) has no preconditions and cannot fail.
        if spec.isolation.is_some() && unsafe { libc::geteuid() } != 0 {
            return Err(LaunchError::Spec(
                "tenant isolation requires euid 0 to apply uid/gid",
            ));
        }
        // T34f2/R3: spawn on the resident spawner thread — see
        // spawn_on_resident_thread for why `Command::spawn` may not
        // run on an arbitrary tokio worker (pdeathsig's parent thread
        // must be one that never retires). The caller's runtime
        // Handle is entered inside the thread so tokio's SIGCHLD/
        // orphan plumbing still registers on the real driver.
        let mut child = spawn_on_resident_thread(cmd, tokio::runtime::Handle::current())
            .await
            .map_err(LaunchError::Io)?;
        drop(child_owned);

        let pid = child.id().ok_or(LaunchError::Spec("pid"))?;
        // T34f: pidfile `<workerd_pid> <host_pid>` — the STARTUP
        // reaper only kills a verified workerd whose owning host is
        // dead, so a live instance's generations can never be
        // reaped. Best-effort — the reaper also verifies the live
        // cmdline against this dir before any kill.
        let _ = std::fs::write(
            spec.work_dir.join("workerd.pid"),
            format!("{pid} {}\n", std::process::id()),
        );
        // T25/L16: stderr keeps a bounded sanitized+redacted tail for
        // failure diagnostics; stdout stays counters-only (tenant
        // stdout is arbitrary program output, stderr is where workerd
        // reports "why"). Needles = every env value handed to the
        // child — secrets and vars alike.
        let log_stats = Arc::new(ChildLogStats {
            stderr_tail: Some(Arc::new(LogTail::new(
                spec.env
                    .values()
                    .map(|v| v.expose().to_string())
                    .chain(spec.base_env.iter().map(|(_, v)| v.clone()))
                    .collect(),
            ))),
            ..Default::default()
        });
        if let Some(out) = child.stdout.take() {
            tokio::spawn(drain_pipe(out, log_stats.stdout_bytes.clone()));
        }
        if let Some(err) = child.stderr.take() {
            let counter = log_stats.stderr_bytes.clone();
            let tail = log_stats.stderr_tail.clone();
            tokio::spawn(drain_pipe_tailed(
                err,
                counter,
                tail,
                spec.tenant_id.clone(),
            ));
        }
        Ok(LaunchedWorkerd {
            pid,
            child,
            control,
            log_stats,
        })
    }
}

/// Drain a child output pipe, counting bytes. Content is discarded —
/// tenant stdout/stderr can carry unregistered secrets or source text.
async fn drain_pipe<R: tokio::io::AsyncRead + Unpin>(mut reader: R, counter: Arc<AtomicU64>) {
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                counter.fetch_add(n as u64, Ordering::Relaxed);
            }
        }
    }
}

/// T34d: live stderr lines emitted per generation; beyond the cap a
/// single trailing summary reports how many were suppressed. Bounds:
/// line length [`LOG_STDERR_LINE_MAX`], count [`LOG_STDERR_LIVE_MAX`].
const LOG_STDERR_LINE_MAX: usize = 400;
const LOG_STDERR_LIVE_MAX: u64 = 64;
/// Largest partial line retained between chunks before it is flushed.
const LOG_STDERR_PENDING_MAX: usize = 4 * 1024;

/// T25/L16: stderr drain — byte counter plus a bounded, sanitized,
/// secret-redacted tail retained in [`LogTail`] for diagnostics.
/// T34d: each sanitized line also reaches the host log at WARN —
/// workerd's uncaught-exception output was invisible before (a
/// tenant 500 with zero evidence anywhere; the D1 standalone e2e
/// failure). Lines are needle-redacted and length-bounded; request
/// bodies never appear because stderr is workerd's own channel.
async fn drain_pipe_tailed<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    counter: Arc<AtomicU64>,
    tail: Option<Arc<LogTail>>,
    tenant: String,
) {
    let mut buf = [0u8; 8192];
    let mut pending = String::new();
    let mut emitted: u64 = 0;
    let mut suppressed: u64 = 0;
    // P3c-2: `vrt_host_do_alarm_fired_total` — workerd emits NO
    // verified success line for a fired alarm on the pinned binary
    // (checked: a confirmed alarm run produces zero stderr; only the
    // failure/cancel strings below exist). We do NOT fabricate a
    // signal. Deployments running a workerd build that DOES emit a
    // verified completion line may set
    // `VRT_DO_ALARM_FIRED_PATTERN=<substring>`; the counter stays 0
    // otherwise, and the metric only ever reflects a real log line.
    let fired_pattern = std::env::var("VRT_DO_ALARM_FIRED_PATTERN")
        .ok()
        .filter(|p| !p.is_empty() && p.len() <= 256);
    let emit = |line: &str, emitted: &mut u64, suppressed: &mut u64, tenant: &str| {
        let trimmed = line.trim_end_matches('\r');
        if trimmed.is_empty() {
            return;
        }
        // P3c-1: DO alarm observability — workerd emits stable stderr
        // strings for alarm failures/cancels (verified against the
        // pinned binary): "Failed to run alarm", "Alarm exceeded its
        // allowed execution time", "output lock broke during alarm",
        // "alarm handler canceled". Classified BEFORE the live-line
        // cap so a stderr flood can't hide alarm failures. There is
        // NO success log line for a fired alarm — a `fired` counter
        // needs handler-level instrumentation (P3c-2); anything else
        // would be fabricated.
        if trimmed.contains("Failed to run alarm")
            || trimmed.contains("Alarm exceeded its allowed execution time")
            || trimmed.contains("output lock broke during alarm")
        {
            crate::metrics::registry().inc(
                "vrt_host_do_alarm_stderr_events_total",
                &[("tenant", tenant), ("kind", "failure")],
                1.0,
            );
            crate::metrics::registry().inc(
                "vrt_host_do_alarm_failed_total",
                &[("tenant", tenant)],
                1.0,
            );
        } else if trimmed.contains("alarm handler canceled") {
            crate::metrics::registry().inc(
                "vrt_host_do_alarm_stderr_events_total",
                &[("tenant", tenant), ("kind", "canceled")],
                1.0,
            );
        }
        if let Some(p) = &fired_pattern {
            if trimmed.contains(p.as_str()) {
                crate::metrics::registry().inc(
                    "vrt_host_do_alarm_fired_total",
                    &[("tenant", tenant)],
                    1.0,
                );
            }
        }
        if *emitted >= LOG_STDERR_LIVE_MAX {
            *suppressed += 1;
            return;
        }
        *emitted += 1;
        let bounded = if trimmed.len() > LOG_STDERR_LINE_MAX {
            format!(
                "{}…(+{}B)",
                &trimmed[..LOG_STDERR_LINE_MAX],
                trimmed.len() - LOG_STDERR_LINE_MAX
            )
        } else {
            trimmed.to_string()
        };
        tracing::warn!(tenant, "workerd stderr: {bounded}");
    };
    loop {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => {
                emit(&pending, &mut emitted, &mut suppressed, &tenant);
                if suppressed > 0 {
                    tracing::warn!(
                        tenant,
                        "workerd stderr: {suppressed} lines suppressed past {LOG_STDERR_LIVE_MAX}"
                    );
                }
                return;
            }
            Ok(n) => {
                counter.fetch_add(n as u64, Ordering::Relaxed);
                let Some(tail) = &tail else { continue };
                tail.push(&buf[..n]);
                pending.push_str(&tail.sanitize(&buf[..n]));
                while let Some(pos) = pending.find('\n') {
                    let line = pending[..pos].to_string();
                    pending.drain(..=pos);
                    emit(&line, &mut emitted, &mut suppressed, &tenant);
                }
                if pending.len() > LOG_STDERR_PENDING_MAX {
                    let line = std::mem::take(&mut pending);
                    emit(&line, &mut emitted, &mut suppressed, &tenant);
                }
            }
        }
    }
}

/// T34f2/R3: run every `Command::spawn` on ONE resident thread.
/// `PR_SET_PDEATHSIG` binds to the *spawning thread* — tokio worker
/// threads may be retired (notably after `block_in_place`
/// conversions, used by asset_sync/bundle_http), which would then
/// SIGKILL workerd while vrt-host is perfectly healthy. A dedicated
/// thread has process lifetime by construction: pdeathsig can only
/// fire when the process is actually gone. The caller's runtime
/// Handle travels with the request and is `enter()`ed inside the
/// thread so `tokio::process` registers SIGCHLD/orphan plumbing on
/// the real driver; the thread itself holds no child handles.
async fn spawn_on_resident_thread(
    cmd: Command,
    handle: tokio::runtime::Handle,
) -> io::Result<Child> {
    use std::sync::mpsc::{channel, Sender};
    use std::sync::OnceLock;
    enum Req {
        Spawn(
            Command,
            tokio::runtime::Handle,
            tokio::sync::oneshot::Sender<io::Result<Child>>,
        ),
    }
    static TX: OnceLock<Sender<Req>> = OnceLock::new();
    let tx = TX.get_or_init(|| {
        let (tx, rx) = channel::<Req>();
        std::thread::Builder::new()
            .name("workerd-spawner".into())
            .spawn(move || {
                while let Ok(Req::Spawn(mut cmd, handle, resp)) = rx.recv() {
                    let _guard = handle.enter();
                    let _ = resp.send(cmd.spawn());
                }
            })
            .expect("workerd spawner thread");
        tx
    });
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    tx.send(Req::Spawn(cmd, handle, resp_tx))
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "workerd spawner thread gone"))?;
    resp_rx
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "workerd spawner response lost"))?
}

/// T13/L1: process-group guard. `kill_on_drop` on a `tokio::process::
/// Child` SIGKILLs only the leader PID — forked helpers or JIT workers
/// spawned BELOW the leader would leak. `process_group(0)` at spawn made
/// the child its group leader, so dropping this guard SIGKILLs the
/// whole group. Drop order: the guard's `drop` signals the group first,
/// then field drop reaps the leader via tokio's orphan collector.
pub struct GroupChild {
    /// The spawned child (leader of its process group).
    pub inner: Child,
    /// Leader PID — doubles as the process-group id.
    pub pid: u32,
}

impl GroupChild {
    /// Wrap a spawned child; `pid` must equal `child.id()`.
    pub fn wrap(child: Child, pid: u32) -> Self {
        Self { inner: child, pid }
    }
}

impl Drop for GroupChild {
    fn drop(&mut self) {
        // Idempotent — ESRCH on an already-dead group is ignored.
        signal_process_group(self.pid, libc::SIGKILL);
    }
}

/// Signal the process group led by `pid` (the launcher made the child a
/// group leader). Errors are swallowed by callers that proceed to the
/// next escalation step anyway.
pub fn signal_process_group(pid: u32, sig: libc::c_int) {
// SAFETY: kill(2) with a negative pid signals the process group led by that pid; callers only pass groups they spawned. The return value is deliberately ignored: escalation continues (or the group is already gone, ESRCH) regardless.
    unsafe {
        libc::kill(-(pid as i32), sig);
    }
}

/// Graceful terminate for a still-owned child: SIGTERM the group, wait up
/// to `grace`, escalate to SIGKILL, then wait for reaping. The owned
/// `Child` must be re-awaited (or dropped) afterwards by the caller.
pub async fn terminate_child(child: &mut Child, pid: u32, grace: Duration) {
    signal_process_group(pid, libc::SIGTERM);
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(_) => return,
        Err(_) => {
            signal_process_group(pid, libc::SIGKILL);
            let _ = tokio::time::timeout(Duration::from_secs(10), child.wait()).await;
        }
    }
}

/// `kill(pid, 0)` liveness probe — `false` when the process is gone
/// (already reaped or never existed), `true` when it lives or when the
/// signal is merely denied (EPERM means a live foreign process).
pub fn pid_alive(pid: u32) -> bool {
// SAFETY: signal 0 performs only the existence/permission probe; it has no preconditions beyond the pid value.
    unsafe {
        if libc::kill(pid as i32, 0) == 0 {
            return true;
        }
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}
