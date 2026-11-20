//! `workerd test` config-load check — the shared "will this config
//! even start" verdict used by both apply-time preflight (vrt-host,
//! before any serialized DO drain) and deploy-time preflight
//! (vrt-local, before publish). ONE classifier: the verdict rules
//! below were verified against workerd 1.20260916.1 and must not
//! drift between call sites.
//!
//! ISOLATION CONTRACT (W1-r3 P0 — a failed check must behave like
//! the config was never executed):
//!
//! - **No tenant code runs.** `workerd test <config> <filter>` runs
//!   every matched worker's `test()` handler — arbitrary tenant code
//!   with live bindings. The check always passes a service glob that
//!   matches nothing (`NO_TESTS_FILTER`): every service is still
//!   compiled/instantiated (missing modules, embeds and entrypoints
//!   still fail the load), but no `test` handler is invoked —
//!   verified empirically against 1.20260916.1.
//! - **No live state is touched.** `workerd test` instantiates each
//!   `disk` service AND opens `durableObjectStorage` localDisk at
//!   load — a bare `workerd test` on the serving config CREATES
//!   `<doStorageDir>/<wid>/<uniqueKey>/metadata.sqlite` in the live
//!   generation's directories (that is what emptied the P3c-2
//!   restore class dir and reset its counter). [`isolated_variant`]
//!   therefore rewrites every `disk = (path = "…")` to a fresh empty
//!   scratch dir, and every `unix:` socket to an auto-assigned
//!   loopback port — the twin contains no live path.
//! - **No live service is dialed.** Every declared external service
//!   is overridden to a refuse address (`127.0.0.1:1`); nothing is
//!   dialed under the no-test filter, but the real D1/queue/etc.
//!   addresses never reach the child at all.
//!
//! Verdict rules: a loadable config with no matching tests exits 1
//! printing the `No tests found!` / `Tests failed!` sentinels (both
//! still appear under a non-matching filter — verified). STREAM
//! SPLIT, verified on 1.20260916.1: with `structuredLogging = true`
//! (every VRT render), `No tests found!` and warnings go to
//! **stdout** while `Tests failed!` and load errors go to stderr;
//! with plain logging everything lands on stderr — the verdict
//! therefore reads BOTH streams. Acceptance requires exit code 0, or
//! exit code exactly 1 AND ≥1 `No tests found!` line AND every
//! nonblank line on both streams benign. Any other line (missing
//! module/embed, parse error, workerd warning) rejects, as does a
//! silent non-zero exit, a non-1 code (signals/crashes), or a
//! stream over STDERR_CAP.
//! The verdict is computed on the FULL stderr — truncation applies
//! only to the returned detail, so an early real error can never be
//! tail-cut into a false pass. A tenant forging a sentinel via
//! top-level `console.error` still cannot pass: any real load
//! failure emits a non-benign diagnostic, and a silently-failed load
//! requires a code-1 exit — a path that does not exist without a
//! diagnostic. Top-level module code still runs (loading IS module
//! evaluation) but cannot do I/O, dial out, or touch storage.

use crate::ExternalAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Hard bound on one check — workerd loads these configs in well
/// under a second; 15 s covers pathological module graphs.
const CHECK_TIMEOUT: Duration = Duration::from_secs(15);

/// MIG-8: process-wide bound on concurrent `workerd test` children —
/// each maps a full config + module graph into memory; unbounded
/// parallel checks (many tenants preflighting at once) would be an
/// unaccounted memory/CPU spike on the pod. Excess callers queue
/// inside the caller's `timeout` budget, then fail closed.
const MAX_CONCURRENT_CHECKS: usize = 4;
static CHECKS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Typed failure classification — `detail` is the bounded sanitized
/// diagnostic (already redacted); `kind` is the stable outcome label
/// metrics and structured logs pivot on (never parsed from text).
#[derive(Clone, Debug)]
pub struct CheckFailure {
    /// Stable outcome class.
    pub kind: CheckKind,
    /// Bounded sanitized diagnostic.
    pub detail: String,
}

/// Stable outcome classes for one check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckKind {
    /// workerd refused the config (load error, unexplained exit,
    /// unverifiable output) or spawn/wait failed.
    Rejected,
    /// The child exceeded the caller's timeout and was killed.
    Timeout,
    /// The global concurrency bound could not admit the check inside
    /// the caller's timeout budget.
    AdmissionTimeout,
    /// MIG-9: the caller's cancel flag was set while the child was
    /// running — the process group was SIGKILLed and reaped before
    /// the verdict returned. Never a config verdict; the caller
    /// (actor stop/supersede) abandons the result.
    Cancelled,
}

impl CheckFailure {
    fn rejected(detail: impl Into<String>) -> Self {
        Self {
            kind: CheckKind::Rejected,
            detail: detail.into(),
        }
    }
    fn timeout() -> Self {
        Self {
            kind: CheckKind::Timeout,
            detail: "check timeout".into(),
        }
    }
    fn admission_timeout() -> Self {
        Self {
            kind: CheckKind::AdmissionTimeout,
            detail: "preflight admission timeout (concurrency bound)".into(),
        }
    }
    fn cancelled() -> Self {
        Self {
            kind: CheckKind::Cancelled,
            detail: "preflight cancelled (child group killed)".into(),
        }
    }
}

/// RAII slot in the global bound — release on every exit path.
struct CheckPermit;
impl Drop for CheckPermit {
    fn drop(&mut self) {
        CHECKS_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Take a slot in the global bound; at most `deadline` is spent
/// waiting so queueing can never stall a launch path indefinitely.
fn admit_check(deadline: Instant) -> Result<CheckPermit, CheckFailure> {
    loop {
        let n = CHECKS_IN_FLIGHT.load(Ordering::SeqCst);
        if n < MAX_CONCURRENT_CHECKS
            && CHECKS_IN_FLIGHT
                .compare_exchange_weak(n, n + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Ok(CheckPermit);
        }
        if Instant::now() >= deadline {
            return Err(CheckFailure::admission_timeout());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Kill the check child AND its group — `process_group(0)` at spawn
/// made the child a group leader, so `kill(-pid)` reaches every
/// descendant that inherited the pipes. Falls back to a direct kill
/// off unix.
fn kill_check_child(child: &mut std::process::Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.kill();
}

/// Cap on the returned stderr tail (chars).
const TAIL_CAP: usize = 4 * 1024;

/// MIG-1: cap on the FIRST non-benign line preserved in the detail —
/// the load error is what the operator needs, not just the sentinel
/// tail. Bounded separately so a huge first line cannot crowd the
/// tail out entirely.
const HEAD_CAP: usize = 1024;

/// Hard cap on the stderr collected for the verdict — past this the
/// output cannot be classified safely, so the check fails closed.
const STDERR_CAP: usize = 256 * 1024;

/// Service glob matching nothing — `workerd test` still loads every
/// service but runs zero `test()` handlers.
pub const NO_TESTS_FILTER: &str = "__vrt_preflight_no_tests__";

/// Refuse endpoint every external service is overridden to.
const CHECK_EXTERNAL_ADDR: &str = "127.0.0.1:1";

/// stderr lines a successful load may still print — the sentinels
/// appear bare (`workerd/server/server.c++:NNNN: error: No tests
/// found!` + `Tests failed!`) or inside JSON log records
/// (`"message":"Tests failed!"`), so match by content.
fn benign(line: &str) -> bool {
    line.contains("No tests found!") || line.contains("Tests failed!")
}

/// Produce the isolated variant of a rendered config for the check:
/// every `unix:` socket address becomes an auto-assigned loopback
/// port (the live generation's socket files are never touched and
/// unix-bind restrictions don't apply), and every
/// `disk = (path = "…")` service — the `do-storage-<wid>` localDisk
/// targets — is repointed at a fresh empty `scratch/disk-<i>` dir
/// (created here), so loading the config can create sqlite files
/// only inside the check's own scratch space.
///
/// Both literal classes are emit-confined: `unix:` occurs only as a
/// socket address (external service addresses travel via
/// `--external-addr` argv), and `disk = (path =` occurs only in the
/// `do-storage-<workerId>` services (capnp.rs DO emission).
pub fn isolated_variant(text: &str, scratch: &Path) -> Result<String, std::io::Error> {
    // Twin paths must be ABSOLUTE: the check runs with cwd = the
    // config's dir, so a relative scratch would resolve to
    // <cfgdir>/<scratch> — wrong tree (and possibly inside the
    // tenant root). Canonicalize up front; fail if it can't resolve.
    std::fs::create_dir_all(scratch)?;
    let scratch = scratch.canonicalize()?;
    let scratch = scratch.as_path();
    let text = loopback_socket_variant(text);
    let out = rewrite_disks_to(&text, scratch)?;
    const MARK: &str = "disk = (path = \"";
    // Format-drift guard: if the emit ever stops matching
    // `disk = (path = "` (field reorder, spacing), the rewrite above
    // silently leaves a LIVE path in the twin — the check would then
    // touch the serving generation's storage. Fail closed instead:
    // every residual `disk = (path` must point inside scratch, and
    // no `unix:` literal may survive anywhere.
    let scratch_prefix = format!("{}/", scratch.to_string_lossy().trim_end_matches('/'));
    let mut scan = out.as_str();
    while let Some(i) = scan.find(MARK) {
        let inner = &scan[i + MARK.len()..];
        let Some(e) = inner.find('"') else { break };
        if !inner[..e].starts_with(&scratch_prefix) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "isolated variant: unrewritten disk path",
            ));
        }
        scan = &inner[e..];
    }
    if out.contains("\"unix:") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "isolated variant: unrewritten unix socket",
        ));
    }
    Ok(out)
}

/// Shared `disk = (path = "\u{2026}")` rewriter — every disk service path is
/// repointed at a fresh empty `scratch/disk-<i>` dir (created here).
/// Caller holds the format-drift guard (residual paths must resolve
/// inside scratch).
fn rewrite_disks_to(text: &str, scratch: &Path) -> Result<String, std::io::Error> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut n = 0usize;
    const MARK: &str = "disk = (path = \"";
    while let Some(i) = rest.find(MARK) {
        out.push_str(&rest[..i + MARK.len()]);
        let inner = &rest[i + MARK.len()..];
        let Some(e) = inner.find('"') else {
            out.push_str(inner);
            rest = "";
            break;
        };
        let dir = scratch.join(format!("disk-{n}"));
        std::fs::create_dir_all(&dir)?;
        out.push_str(&dir.to_string_lossy());
        n += 1;
        rest = &inner[e..];
    }
    out.push_str(rest);
    Ok(out)
}

/// MIG-13: the prewarm twin of a serving config — the generation
/// spawned while the OLD generation still serves. Identical to the
/// final config except:
///
/// - every `disk = (path = "\u{2026}")` service (the `do-storage-<wid>`
///   localDisk targets AND any other disk service) is repointed at a
///   fresh empty `scratch/disk-<i>` dir. workerd opens
///   `durableObjectStorage` localDisk EAGERLY at config load and runs
///   its alarm scheduler autonomously (verified on 1.20260916.1 —
///   `metadata.sqlite` + WAL/SHM are created and held open with zero
///   requests, and a second process on the same dirs double-fires
///   pending alarms). Pointing the warm process at scratch dirs is
///   what keeps the DO single-writer invariant while startup, listen
///   and module compilation overlap the old generation's serving.
/// - the `vrt-switch` marker socket is REMOVED — it exists only in
///   the final config, so its `listen` control event after the
///   `--watch` reload is the deterministic "real-storage config
///   applied" signal the supervisor waits on before install.
///
/// Unlike [`isolated_variant`], sockets are NOT rewritten: the twin
/// binds the generation's real unix sockets (per-generation sock dir,
/// no collision with the old generation) so readiness and the
/// `vrt-waituntil` module-warm fan-out exercise the exact surface the
/// generation will serve on.
///
/// Returns `(text, marker_stripped)` — `marker_stripped == false`
/// means the render predates MIG-13 and the supervisor must fall back
/// to the classic serialized launch (no swap-completion signal).
pub fn prewarm_variant(text: &str, scratch: &Path) -> Result<(String, bool), std::io::Error> {
    std::fs::create_dir_all(scratch)?;
    let scratch = scratch.canonicalize()?;
    let scratch = scratch.as_path();
    let (text, marker_stripped) = strip_marker_socket(text);
    let out = rewrite_disks_to(&text, scratch)?;
    // Same fail-closed guard as the isolated variant: every residual
    // `disk = (path` must point inside scratch — a format drift can
    // never let the warm process touch the live generation's storage.
    const MARK: &str = "disk = (path = \"";
    let scratch_prefix = format!("{}/", scratch.to_string_lossy().trim_end_matches('/'));
    let mut scan = out.as_str();
    while let Some(i) = scan.find(MARK) {
        let inner = &scan[i + MARK.len()..];
        let Some(e) = inner.find('"') else { break };
        if !inner[..e].starts_with(&scratch_prefix) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "prewarm variant: unrewritten disk path",
            ));
        }
        scan = &inner[e..];
    }
    Ok((out, marker_stripped))
}

/// Remove the `vrt-switch` socket line from the `sockets` block —
/// the bool reports whether the marker was found and removed.
/// Emit-confined like the disk rewrite: `vrt-switch` occurs only as
/// the marker socket name inside the sockets block (capnp.rs emit).
fn strip_marker_socket(text: &str) -> (std::borrow::Cow<'_, str>, bool) {
    const BLOCK_START: &str = "  sockets = [\n";
    const BLOCK_END: &str = "  ],\n";
    let Some(start) = text.find(BLOCK_START) else {
        return (std::borrow::Cow::Borrowed(text), false);
    };
    let Some(end_rel) = text[start..].find(BLOCK_END) else {
        return (std::borrow::Cow::Borrowed(text), false);
    };
    let end = start + end_rel;
    let block = &text[start..end];
    let key = format!("name = \"{}\"", crate::capnp::SWITCH_MARKER_SOCKET);
    let mut found = false;
    let mut kept = String::with_capacity(block.len());
    for line in block.split_inclusive('\n') {
        if line.contains(&key) && line.contains("address = ") {
            found = true;
            continue;
        }
        kept.push_str(line);
    }
    if !found {
        return (std::borrow::Cow::Borrowed(text), false);
    }
    let mut out = String::with_capacity(text.len());
    out.push_str(&text[..start]);
    out.push_str(&kept);
    out.push_str(&text[end..]);
    (std::borrow::Cow::Owned(out), true)
}

/// `unix:` socket addresses → `127.0.0.1:0`, confined to the
/// `sockets` block.
fn loopback_socket_variant(text: &str) -> String {
    const BLOCK_START: &str = "  sockets = [\n";
    const BLOCK_END: &str = "  ],\n";
    let Some(start) = text.find(BLOCK_START) else {
        return text.to_string();
    };
    let after = &text[start + BLOCK_START.len()..];
    let Some(end_rel) = after.find(BLOCK_END) else {
        return text.to_string();
    };
    let block = &after[..end_rel];
    let mut out = String::with_capacity(text.len());
    out.push_str(&text[..start + BLOCK_START.len()]);
    let mut rest = block;
    while let Some(i) = rest.find("\"unix:") {
        out.push_str(&rest[..i + 1]);
        let inner = &rest[i + 1..];
        let Some(e) = inner.find('"') else { break };
        out.push_str("127.0.0.1:0");
        rest = &inner[e..];
    }
    out.push_str(rest);
    out.push_str(&after[end_rel..]);
    out
}

/// MIG-13-r5: every `unix:` bind path declared inside the `sockets`
/// block of a rendered config. Emit-confined the same way
/// `loopback_socket_variant`/`strip_marker_socket` are: `unix:` only
/// ever appears there as a socket address — external service
/// addresses travel via `--external-addr` argv, never the config
/// text. The supervisor's swap pre-clear unlinks exactly this set
/// before the watched live-config rewrite (the execve'd image
/// re-binds each declared path and kj never unlinks a stale
/// pathname — e2e24 EADDRINUSE): never anything the render did not
/// declare, and the caller additionally refuses any path outside the
/// generation's own sock dir.
pub fn unix_socket_bind_paths(text: &str) -> Vec<PathBuf> {
    const BLOCK_START: &str = "  sockets = [\n";
    const BLOCK_END: &str = "  ],\n";
    let Some(start) = text.find(BLOCK_START) else {
        return Vec::new();
    };
    let after = &text[start + BLOCK_START.len()..];
    let Some(end_rel) = after.find(BLOCK_END) else {
        return Vec::new();
    };
    let block = &after[..end_rel];
    let mut paths = Vec::new();
    let mut rest = block;
    while let Some(i) = rest.find("\"unix:") {
        let inner = &rest[i + "\"unix:".len()..];
        let Some(e) = inner.find('"') else {
            break;
        };
        paths.push(PathBuf::from(&inner[..e]));
        rest = &inner[e..];
    }
    paths
}

/// Run the bounded, isolated check. `external_addrs` supplies only
/// the service NAMES to override — every one is repointed at the
/// refuse endpoint, the real addresses are never passed to the
/// child. `needles` are values that must never leak into the
/// returned detail (secret/var values) — every nonempty needle is
/// redacted to `***` regardless of length.
pub fn check_workerd_loads(
    binary: &Path,
    config: &Path,
    external_addrs: &[ExternalAddr],
    extra_args: &[String],
    needles: &[String],
) -> Result<(), String> {
    check_workerd_loads_bounded(
        binary,
        config,
        external_addrs,
        extra_args,
        needles,
        CHECK_TIMEOUT,
    )
    .map_err(|f| f.detail)
}

/// Typed variant of [`check_workerd_loads`]: the SAME classifier with
/// a caller-chosen `timeout` (also the admission queue's budget — a
/// check may never spend longer than `timeout` queued + running) and
/// a [`CheckFailure`] outcome the caller can count/log without
/// string-matching the detail.
pub fn check_workerd_loads_bounded(
    binary: &Path,
    config: &Path,
    external_addrs: &[ExternalAddr],
    extra_args: &[String],
    needles: &[String],
    timeout: Duration,
) -> Result<(), CheckFailure> {
    run_check(binary, config, external_addrs, extra_args, needles, timeout, None)
}

/// MIG-9: cancellable variant of [`check_workerd_loads_bounded`] for
/// callers that run the check OFF an actor thread (vrt-host launch
/// preflight). `cancel` is polled on the same 25 ms tick as the child
/// wait loop — when set, the whole process group is SIGKILLed and
/// reaped before the verdict returns, so a stop/supersede never
/// leaves a `workerd test` orphan running to the natural deadline.
/// The check still owns the child end-to-end inside this call: a
/// detached caller may forget the result without leaking a process.
pub fn check_workerd_loads_cancellable(
    binary: &Path,
    config: &Path,
    external_addrs: &[ExternalAddr],
    extra_args: &[String],
    needles: &[String],
    timeout: Duration,
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), CheckFailure> {
    run_check(
        binary,
        config,
        external_addrs,
        extra_args,
        needles,
        timeout,
        Some(cancel),
    )
}

fn run_check(
    binary: &Path,
    config: &Path,
    external_addrs: &[ExternalAddr],
    extra_args: &[String],
    needles: &[String],
    timeout: Duration,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> Result<(), CheckFailure> {
    use std::io::Read;
    #[cfg(unix)]
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    // MIG-8: ONE deadline covers admission + execution — queueing can
    // never extend the bound past the caller's timeout.
    let deadline = Instant::now() + timeout;
    let _permit = admit_check(deadline)?;

    let mut cmd = Command::new(binary);
    cmd.arg("test").arg(config).arg(NO_TESTS_FILTER);
    for a in external_addrs {
        cmd.arg(format!(
            "--external-addr={}={CHECK_EXTERNAL_ADDR}",
            a.service
        ));
    }
    // `socketDir` and `embed` paths are relative to the config's own
    // directory by VRT convention — run the check from there.
    if let Some(dir) = config.parent() {
        cmd.current_dir(dir);
    }
    cmd.args(extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // MIG-8: own process group — the timeout path kills the GROUP so
    // anything the child forked (a wrapper's `sleep`, a workerd
    // subprocess) cannot hold the drained pipes open past the
    // deadline and strand the launch path.
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd
        .spawn()
        .map_err(|e| CheckFailure::rejected(format!("spawn: {e}")))?;
    // Drain BOTH streams concurrently — a full pipe would deadlock
    // the bounded wait, and under `structuredLogging = true` (every
    // VRT render) `No tests found!`/warnings live on stdout while
    // `Tests failed!`/load errors live on stderr. Bounded at
    // STDERR_CAP each: past the cap keep draining (discarding) so the
    // child cannot wedge on a full pipe, but mark the stream
    // unverifiable and reject.
    fn drain_stream(
        pipe: Option<impl Read + Send + 'static>,
    ) -> std::thread::JoinHandle<Result<String, ()>> {
        std::thread::spawn(move || {
            let mut buf = Vec::with_capacity(4096);
            if let Some(mut pipe) = pipe {
                let mut chunk = [0u8; 8192];
                let mut overflow = false;
                loop {
                    match pipe.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if !overflow {
                                buf.extend_from_slice(&chunk[..n]);
                                if buf.len() > STDERR_CAP {
                                    overflow = true;
                                    buf.clear();
                                }
                            }
                        }
                    }
                }
                if overflow {
                    return Err(());
                }
            }
            Ok(String::from_utf8_lossy(&buf).into_owned())
        })
    }
    let out_drain = drain_stream(child.stdout.take());
    let err_drain = drain_stream(child.stderr.take());
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => {
                if cancel
                    .as_ref()
                    .is_some_and(|c| c.load(std::sync::atomic::Ordering::SeqCst))
                {
                    kill_check_child(&mut child);
                    let _ = child.wait();
                    let _ = out_drain.join();
                    let _ = err_drain.join();
                    return Err(CheckFailure::cancelled());
                }
                if Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(25));
                    continue;
                }
                kill_check_child(&mut child);
                let _ = child.wait();
                let _ = out_drain.join();
                let _ = err_drain.join();
                return Err(CheckFailure::timeout());
            }
            Err(e) => {
                kill_check_child(&mut child);
                let _ = child.wait();
                let _ = out_drain.join();
                let _ = err_drain.join();
                return Err(CheckFailure::rejected(format!("wait: {e}")));
            }
        }
    };
    let stdout = match out_drain.join() {
        Ok(Ok(s)) => s,
        _ => return Err(CheckFailure::rejected("stdout exceeds 256 KiB cap")),
    };
    let stderr = match err_drain.join() {
        Ok(Ok(s)) => s,
        _ => return Err(CheckFailure::rejected("stderr exceeds 256 KiB cap")),
    };
    // Verdict on the COMPLETE output of both streams — a real error
    // can never be truncated into a pass; the tail cap shapes only
    // the detail. stdout first so stderr's load diagnostics (the
    // actionable part) survive the tail.
    let all = format!("{stdout}{stderr}");
    let has_sentinel = all.lines().any(|l| l.contains("No tests found!"));
    let all_benign = all
        .lines()
        .map(str::trim)
        .all(|l| l.is_empty() || benign(l));
    if status.success() || (status.code() == Some(1) && has_sentinel && all_benign) {
        Ok(())
    } else {
        // MIG-1: the detail leads with the FIRST non-benign line — the
        // actual load failure — followed by the bounded tail. A tail-
        // only detail showed operators `Tests failed!`/`No tests
        // found!` sentinels while the real error scrolled out of view.
        let tail = sanitize_tail(&all, needles);
        let detail = match all
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty() && !benign(l))
        {
            Some(first) => {
                let head = sanitize_tail(first, needles);
                let head = if head.len() > HEAD_CAP {
                    let mut end = HEAD_CAP;
                    while end > 0 && !head.is_char_boundary(end) {
                        end -= 1;
                    }
                    format!("{}…", &head[..end])
                } else {
                    head
                };
                format!("{head}\n(tail) {tail}")
            }
            None => tail,
        };
        Err(CheckFailure::rejected(detail))
    }
}

/// Control-char strip + needle redact + bounded tail — the same
/// treatment vrt-host's `LogTail` gives child stderr. Redaction
/// applies to EVERY nonempty needle (any length) — a short secret
/// must not pass through unredacted.
fn sanitize_tail(raw: &str, needles: &[String]) -> String {
    let mut clean = String::with_capacity(raw.len().min(TAIL_CAP * 2));
    for ch in raw.chars() {
        if ch == '\n' || ch == '\t' || (!ch.is_control() && ch != '\u{7f}') {
            clean.push(ch);
        } else {
            clean.push('.');
        }
    }
    for needle in needles.iter().filter(|n| !n.is_empty()) {
        if clean.contains(needle.as_str()) {
            clean = clean.replace(needle.as_str(), "***");
        }
    }
    // Last TAIL_CAP chars on a char boundary.
    if clean.len() > TAIL_CAP {
        let mut start = clean.len() - TAIL_CAP;
        while start < clean.len() && !clean.is_char_boundary(start) {
            start += 1;
        }
        clean.drain(..start);
    }
    clean
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "vrt-check-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::sync::atomic::AtomicU64::new(0).fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    const SAMPLE: &str = "const config :Workerd.Config = (\n  services = [\n    ( name = \"w1\", worker = (\n        durableObjectStorage = (localDisk = \"ds\"),\n    ) ),\n    ( name = \"ds\", disk = (path = \"/live/do-store/w_abc\", writable = true) ),\n    ( name = \"ds2\", disk = (path = \"/live/do-store/w_def\", writable = true) ),\n    ( name = \"bb\", external = (http = ()) ),\n  ],\n  sockets = [\n    ( name = \"http\", address = \"unix:sock/w1.sock\", http = (), service = \"w1\" ),\n  ],\n);\n";

    #[test]
    fn isolated_variant_scrubs_disks_and_sockets() {
        let dir = tmpdir("iso");
        let scratch = dir.join("scratch");
        let out = isolated_variant(SAMPLE, &scratch).unwrap();
        assert!(
            !out.contains("/live/do-store"),
            "live disk path must not survive: {out}"
        );
        assert!(!out.contains("unix:sock"), "live socket must go: {out}");
        assert!(out.contains("127.0.0.1:0"));
        assert!(out.contains("disk-0") && out.contains("disk-1"), "{out}");
        assert!(scratch.join("disk-0").is_dir() && scratch.join("disk-1").is_dir());
        // localDisk still names the disk SERVICE — unchanged.
        assert!(out.contains("localDisk = \"ds\""));
    }

    const PREWARM_SAMPLE: &str = "const config :Workerd.Config = (\n  services = [\n    ( name = \"w1\", worker = (\n        durableObjectStorage = (localDisk = \"ds\"),\n    ) ),\n    ( name = \"ds\", disk = (path = \"/live/do-store/w_abc\", writable = true) ),\n  ],\n  sockets = [\n    ( name = \"sock-w1\", address = \"unix:gen9/sock/w1.sock\", http = (), service = \"w1\" ),\n    ( name = \"vrt-waituntil\", address = \"unix:gen9/sock/vrt-waituntil.sock\", http = (), service = \"vrt-wu-probe\" ),\n    ( name = \"vrt-switch\", address = \"unix:gen9/sock/vrt-sw.sock\", http = (), service = \"vrt-wu-probe\" ),\n  ],\n);\n";

    #[test]
    fn prewarm_variant_repoints_disks_strips_marker_keeps_sockets() {
        let dir = tmpdir("pre");
        let scratch = dir.join("scratch");
        let (out, marker) = prewarm_variant(PREWARM_SAMPLE, &scratch).unwrap();
        assert!(marker, "marker socket must be found and stripped");
        // Real socket surface is preserved — only the marker went.
        assert!(out.contains("unix:gen9/sock/w1.sock"), "{out}");
        assert!(out.contains("vrt-waituntil"), "{out}");
        assert!(!out.contains("vrt-switch"), "marker must be gone: {out}");
        // Disks moved to scratch; live path is nowhere.
        assert!(!out.contains("/live/do-store"), "{out}");
        assert!(out.contains("disk-0"), "{out}");
        assert!(scratch.join("disk-0").is_dir());
        // localDisk service NAME unchanged — the worker's binding is intact.
        assert!(out.contains("localDisk = \"ds\""));
    }

    #[test]
    fn prewarm_variant_reports_missing_marker_for_fallback() {
        // A pre-MIG-13 render has no marker socket — the bool tells
        // the supervisor to use the classic serialized launch.
        let dir = tmpdir("pre-nm");
        let scratch = dir.join("scratch");
        let no_marker = PREWARM_SAMPLE.replace(
            "    ( name = \"vrt-switch\", address = \"unix:gen9/sock/vrt-sw.sock\", http = (), service = \"vrt-wu-probe\" ),\n",
            "",
        );
        let (out, marker) = prewarm_variant(&no_marker, &scratch).unwrap();
        assert!(!marker);
        assert!(!out.contains("/live/do-store"), "{out}");
    }

    #[test]
    fn prewarm_variant_without_disks_still_strips_marker() {
        // The twin must also be correct with zero disk services
        // (e.g. a worker whose namespace rides another's storage).
        let dir = tmpdir("pre-nodisk");
        let scratch = dir.join("scratch");
        let no_disk = PREWARM_SAMPLE.replace(
            "    ( name = \"ds\", disk = (path = \"/live/do-store/w_abc\", writable = true) ),\n",
            "",
        );
        let (out, marker) = prewarm_variant(&no_disk, &scratch).unwrap();
        assert!(marker);
        assert!(!out.contains("vrt-switch"), "{out}");
        assert!(out.contains("unix:gen9/sock/w1.sock"), "{out}");
    }

    /// MIG-13-r5: the pre-clear path list comes from the `sockets`
    /// block ONLY — same emit confinement the marker strip relies
    /// on.
    #[test]
    fn unix_socket_bind_paths_reads_sockets_block_only() {
        let paths = unix_socket_bind_paths(PREWARM_SAMPLE);
        assert_eq!(
            paths,
            vec![
                PathBuf::from("gen9/sock/w1.sock"),
                PathBuf::from("gen9/sock/vrt-waituntil.sock"),
                PathBuf::from("gen9/sock/vrt-sw.sock"),
            ],
            "{paths:?}"
        );
    }

    /// `unix:` outside the sockets block (a disk path, a service
    /// address, prose) must never be collected — the pre-clear
    /// unlinks only what the execve'd image will re-bind.
    #[test]
    fn unix_socket_bind_paths_ignores_non_socket_unix_literals() {
        let text = concat!(
            "const config :Workerd.Config = (\n",
            "  services = [\n",
            "    ( name = \"x\", disk = (path = \"unix:/planted.sock\") ),\n",
            "  ],\n",
            "  sockets = [\n",
            "    ( name = \"w0\", address = \"unix:/g/sock/w0.sock\", http = (), service = \"w0\" ),\n",
            "  ],\n",
            ");\n",
            "unix:/prose.sock should not parse\n",
        );
        assert_eq!(
            unix_socket_bind_paths(text),
            vec![PathBuf::from("/g/sock/w0.sock")]
        );
    }

    /// No sockets block → empty (never an error — the caller's
    /// containment check does the failing).
    #[test]
    fn unix_socket_bind_paths_empty_without_block() {
        assert!(unix_socket_bind_paths("(services = [])").is_empty());
    }

    fn fake_workerd(dir: &Path, script: &str, code: i32) -> PathBuf {
        // Written by a separate `sh` process, never by this test process:
        // if this process held the write fd, a sibling test thread forking
        // at that moment would inherit it (CLOEXEC only drops it at exec),
        // and our own spawn of the script would fail with ETXTBSY
        // ("Text file busy"). A child's write fd cannot leak that way.
        use std::io::Write;
        let bin = dir.join("workerd");
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("cat > \"$1\" && chmod 755 \"$1\"")
            .arg("sh")
            .arg(&bin)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("#!/bin/sh\n{script}\nexit {code}\n").as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success(), "writing fake workerd failed");
        bin
    }

    fn cfg(dir: &Path) -> PathBuf {
        let c = dir.join("config.capnp");
        std::fs::write(&c, b"(services = [])").unwrap();
        c
    }

    #[test]
    fn verdict_uses_full_stderr_not_tail() {
        // Real error FIRST, then enough sentinel spam to push it past
        // the 4 KiB detail tail — a tail-based verdict would pass.
        let dir = tmpdir("tail");
        let bin = fake_workerd(
            &dir,
            "echo 'service w: Uncaught Error: No such module \"x\".' >&2\ni=0; while [ $i -lt 400 ]; do echo 'Tests failed!' >&2; i=$((i+1)); done\necho 'No tests found!' >&2",
            1,
        );
        // The verdict must reject even though the last 4 KiB (what
        // the returned detail shows) is sentinel-only.
        let err = check_workerd_loads(&bin, &cfg(&dir), &[], &[], &[])
            .expect_err("error hidden past the tail must still reject");
        assert!(err.contains("Tests failed!"), "{err}");
    }

    #[test]
    fn detail_leads_with_first_non_benign_line() {
        // MIG-1: real load error BEFORE the sentinel tail — the detail
        // must contain it, not just the tail's sentinels.
        let dir = tmpdir("head");
        let bin = fake_workerd(
            &dir,
            "echo 'config error: service w1: missing compatibility flag' >&2\necho 'Tests failed!' >&2\necho 'No tests found!' >&2",
            1,
        );
        let err = check_workerd_loads(&bin, &cfg(&dir), &[], &[], &[])
            .expect_err("non-benign output must reject");
        assert!(
            err.starts_with("config error: service w1"),
            "first non-benign line must lead the detail: {err}"
        );
        assert!(err.contains("No tests found!"), "{err}");
    }

    #[test]
    fn oversized_stderr_rejects() {
        let dir = tmpdir("big");
        let bin = fake_workerd(
            &dir,
            "head -c 300000 /dev/zero | tr '\\0' 'y' >&2\necho 'No tests found!' >&2",
            1,
        );
        let err = check_workerd_loads(&bin, &cfg(&dir), &[], &[], &[])
            .expect_err("over-cap stderr cannot be classified");
        assert!(err.contains("cap"), "{err}");
    }

    #[test]
    fn sentinels_with_wrong_exit_code_reject() {
        // Only exit 1 is the documented "no tests" path — a crash
        // code plus forged sentinels must not pass.
        let dir = tmpdir("code");
        let bin = fake_workerd(
            &dir,
            "echo 'No tests found!' >&2\necho 'Tests failed!' >&2",
            3,
        );
        check_workerd_loads(&bin, &cfg(&dir), &[], &[], &[])
            .expect_err("exit 3 + sentinels must reject");
    }

    #[test]
    fn short_needles_are_redacted() {
        let dir = tmpdir("short");
        let bin = fake_workerd(&dir, "echo 'service w: bad value s3cr3t leaked' >&2", 1);
        let err = check_workerd_loads(&bin, &cfg(&dir), &[], &[], &["s3cr3t".to_string()])
            .expect_err("must reject");
        assert!(!err.contains("s3cr3t"), "short needle leaked: {err}");
    }

    #[test]
    fn json_mode_sentinel_on_stdout_passes() {
        // structuredLogging = true (every VRT render): `No tests
        // found!` lands on stdout, `Tests failed!` on stderr.
        let dir = tmpdir("json");
        let bin = fake_workerd(
            &dir,
            "echo '{\"message\":\"No tests found!\"}'\necho '{\"message\":\"Tests failed!\"}' >&2",
            1,
        );
        check_workerd_loads(&bin, &cfg(&dir), &[], &[], &[]).expect("split sentinels = loadable");
    }

    #[test]
    fn stdout_error_rejects_even_when_stderr_is_benign() {
        let dir = tmpdir("outerr");
        let bin = fake_workerd(
            &dir,
            "echo 'parse failed: cfg.capnp:3: bad token'
echo 'Tests failed!' >&2
echo 'No tests found!' >&2",
            1,
        );
        let err = check_workerd_loads(&bin, &cfg(&dir), &[], &[], &[])
            .expect_err("stdout diagnostics must reject");
        assert!(err.contains("parse failed"), "{err}");
    }

    #[test]
    fn argv_isolates_external_addrs_and_disables_tests() {
        // The fake echoes its argv — the check must pass the
        // impossible filter and refuse endpoints, never real addrs.
        let dir = tmpdir("argv");
        let bin = fake_workerd(&dir, "echo \"$@\" >&2", 1);
        let ext = vec![ExternalAddr {
            service: "binding-backend".into(),
            address: "https://real-d1.internal:8443".into(),
        }];
        let err = check_workerd_loads(&bin, &cfg(&dir), &ext, &["--experimental".into()], &[])
            .expect_err("must reject (argv echoed is non-benign)");
        assert!(err.contains("__vrt_preflight_no_tests__"), "{err}");
        assert!(err.contains("binding-backend=127.0.0.1:1"), "{err}");
        assert!(
            !err.contains("real-d1.internal"),
            "real external addr leaked into argv: {err}"
        );
        assert!(err.contains("--experimental"), "{err}");
    }
    /// Serializes the two MIG-8 tests that poke the GLOBAL bound —
    /// the admission test deliberately saturates it, and the timeout
    /// test's 300 ms budget would read the saturation as
    /// `AdmissionTimeout` instead of `Timeout`.
    static BOUND_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// MIG-8: a wedged `workerd test` is killed + reaped inside the
    /// caller's bound — never awaited to natural exit, never left as
    /// a stray process holding scratch dirs or (for real workerd) a
    /// locked DO SQLite file.
    #[test]
    fn timeout_kills_and_reaps_the_child() {
        let _g = BOUND_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("tmo");
        let pidfile = dir.join("child.pid");
        // The GRANDchild holds the pipes: `sleep` is forked off the
        // `sh` leader inheriting stdout/stderr — a direct `kill()`
        // alone would leave it alive holding EOF open for 30 s
        // (exactly what the pre-group-kill code did).
        let bin = fake_workerd(
            &dir,
            &format!("sleep 30 & echo $! > {}; wait", pidfile.display()),
            0,
        );
        let t0 = std::time::Instant::now();
        let err = check_workerd_loads_bounded(
            &bin,
            &cfg(&dir),
            &[],
            &[],
            &[],
            // MIG-9: 3 s, not 300 ms — the deadline must comfortably
            // exceed cold-spawn latency (fork + script parse + pidfile
            // write) under parallel test load, while staying far below
            // the child's 30 s natural exit so "killed at the bound,
            // not awaited" still holds.
            Duration::from_secs(3),
        )
        .expect_err("sleeping child must time out");
        assert_eq!(err.kind, CheckKind::Timeout, "{err:?}");
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "child killed, not awaited: {:?}",
            t0.elapsed()
        );
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // The group-killed grandchild may be a zombie for a few ms
        // while init reaps it — poll rather than read liveness once.
        let alive = (0..50).any(|_| {
            let up = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !up {
                return true; // gone — done polling
            }
            std::thread::sleep(Duration::from_millis(10));
            false
        });
        assert!(alive, "timed-out preflight child pid {pid} must be dead");
    }

    /// MIG-8: the process-wide bound fails closed — saturate it with
    /// hanging checks and a fresh one is refused ADMISSION inside its
    /// own budget (typed `AdmissionTimeout`), it never queues forever.
    #[test]
    fn global_bound_refuses_admission_inside_budget() {
        let _g = BOUND_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("bound");
        let cfgp = cfg(&dir);
        let mut guards = Vec::new();
        let mut marks = Vec::new();
        let mut bins = Vec::new();
        use std::os::unix::fs::PermissionsExt;
        // Release gate: saturating children hold their slot until the
        // test creates this file (hard cap 60 s so a panicking test
        // never strands them) — the hold is event-driven, not a
        // wall-clock `sleep` the assertion has to race.
        let release = dir.join("release");
        // Write EVERY script before spawning any check thread: a
        // concurrent fork() in another thread can briefly inherit a
        // still-open write fd of a script and make its exec fail
        // with ETXTBSY.
        for i in 0..MAX_CONCURRENT_CHECKS {
            // Each saturating child drops a marker WHEN it starts —
            // that is the proof its slot is really held (the permit
            // outlives the child inside the check), immune to foreign
            // tests' checks transiently occupying slots.
            let m = dir.join(format!("started-{i}"));
            let bin = dir.join(format!("workerd-{i}"));
            std::fs::write(
                &bin,
                format!(
                    "#!/bin/sh\necho started > {}\nn=0\nwhile [ ! -e {} ] && [ $n -lt 600 ]; do sleep 0.1; n=$((n+1)); done\n",
                    m.display(),
                    release.display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
            marks.push(m);
            bins.push(bin);
        }
        for bin in bins {
            let c = cfgp.clone();
            guards.push(std::thread::spawn(move || {
                // A foreign test's fork can still race the exec
                // (ETXTBSY) — retry only that spawn failure so the
                // slot is really taken; any other verdict is final.
                for _ in 0..20 {
                    let r = check_workerd_loads_bounded(
                        &bin,
                        &c,
                        &[],
                        &[],
                        &[],
                        Duration::from_secs(30),
                    );
                    match &r {
                        Err(e) if e.kind == CheckKind::Rejected && e.detail.contains("busy") => {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        _ => return r,
                    }
                }
                Err(CheckFailure::rejected("ETXTBSY retries exhausted"))
            }));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !marks.iter().all(|m| m.exists()) {
            assert!(
                Instant::now() < deadline,
                "saturating checks never all spawned — bound={MAX_CONCURRENT_CHECKS}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let bin5 = fake_workerd(&dir, "sleep 5", 0);
        let err =
            check_workerd_loads_bounded(&bin5, &cfgp, &[], &[], &[], Duration::from_millis(400))
                .expect_err("saturated bound must refuse admission");
        assert_eq!(err.kind, CheckKind::AdmissionTimeout, "{err:?}");
        std::fs::write(&release, b"go").unwrap();
        for g in guards {
            let _ = g.join();
        }
    }

    /// MIG-9: a cancelled check kills AND reaps the process group
    /// well inside its deadline — an actor stop/supersede never
    /// leaves a `workerd test` orphan holding scratch dirs.
    #[test]
    fn cancel_kills_and_reaps_the_child_group() {
        let _g = BOUND_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("cncl");
        let pidfile = dir.join("child.pid");
        // Same grandchild-holds-pipes shape as the timeout test: the
        // GROUP must die, not just the leader.
        let bin = fake_workerd(
            &dir,
            &format!("sleep 30 & echo $! > {}; wait", pidfile.display()),
            0,
        );
        let cfgp = cfg(&dir);
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel2 = cancel.clone();
        let handle = std::thread::spawn(move || {
            check_workerd_loads_cancellable(
                &bin,
                &cfgp,
                &[],
                &[],
                &[],
                Duration::from_secs(30),
                cancel2,
            )
        });
        // Set cancel once the child is provably running (pidfile
        // written), so the kill targets a real group.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !pidfile.exists() {
            assert!(Instant::now() < deadline, "child never started");
            std::thread::sleep(Duration::from_millis(10));
        }
        let t0 = Instant::now();
        cancel.store(true, std::sync::atomic::Ordering::SeqCst);
        let err = handle.join().unwrap().expect_err("cancelled check must fail");
        assert_eq!(err.kind, CheckKind::Cancelled, "{err:?}");
        assert!(
            t0.elapsed() + deadline.elapsed() < Duration::from_secs(10),
            "cancel reap must be prompt, not awaited to the 30 s deadline"
        );
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let gone = (0..50).any(|_| {
            let up = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !up {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
            false
        });
        assert!(gone, "cancelled preflight child pid {pid} must be dead");
    }
}
