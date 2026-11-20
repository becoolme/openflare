//! `vrt doctor` — environment and service diagnostics.
//!
//! Each check returns `{id, status, detail, hint, docs}` with a
//! stable shape for `--json` consumers. Statuses mirror the
//! OpenOrange doctor contract: `ok` (healthy), `action` (works but
//! needs a decision/fix), `fail` (broken). Exit code rules live in
//! [`Report::ok`]: any `fail` fails; `--strict` also fails on
//! `action`.
//!
//! Checks are read-only — doctor never mutates root, keys, or state.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::LocalError;

/// One diagnostic result. Field order is the wire contract.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Ok,
    Action,
    Fail,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Action => "action",
            Status::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Check {
    pub id: String,
    pub status: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs: Option<String>,
}

impl Check {
    fn ok(id: &str, detail: impl Into<String>) -> Self {
        Self::new(id, Status::Ok, detail)
    }
    fn action(id: &str, detail: impl Into<String>, hint: &str) -> Self {
        let mut c = Self::new(id, Status::Action, detail);
        c.hint = Some(hint.to_string());
        c
    }
    fn fail(id: &str, detail: impl Into<String>, hint: &str) -> Self {
        let mut c = Self::new(id, Status::Fail, detail);
        c.hint = Some(hint.to_string());
        c
    }
    fn new(id: &str, status: Status, detail: impl Into<String>) -> Self {
        Self {
            id: id.to_string(),
            status: status.as_str().to_string(),
            detail: detail.into(),
            hint: None,
            docs: None,
        }
    }
    fn docs(mut self, d: &str) -> Self {
        self.docs = Some(d.to_string());
        self
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub root: String,
    pub checks: Vec<Check>,
    /// true = no `fail` (and no `action` under --strict — folded in
    /// by the caller via [`Report::status_for`]).
    pub ok: bool,
}

impl Report {
    /// Exit-code rule: `fail` always fails; `action` fails only under
    /// --strict.
    pub fn passing(&self, strict: bool) -> bool {
        self.checks.iter().all(|c| match c.status.as_str() {
            "fail" => false,
            "action" => !strict,
            _ => true,
        })
    }
}

/// Run every check. `root` is the `--root` state dir (may not exist —
/// the checks report that); `project_dir` is the directory whose
/// wrangler/vrt config gets the unsupported-keys check.
pub fn run(root: &Path, project_dir: &Path) -> Report {
    let checks = vec![
        check_workerd(),
        check_root(root),
        check_keys(root),
        check_disk(root),
        check_environment(),
        check_ports(root),
        check_facade(root),
        check_service_mgmt(root),
        check_service_host(root),
        check_service_provider(root),
        check_service_sql(root),
        check_project(project_dir),
    ];
    Report {
        root: root.display().to_string(),
        ok: true, // recomputed by caller against `strict`
        checks,
    }
}

/// --- workerd: resolvable + pinned to deploy/docker/workerd.lock ---
fn check_workerd() -> Check {
    let id = "workerd";
    let path = if let Ok(v) = std::env::var("VRT_WORKERD") {
        Some(PathBuf::from(v))
    } else {
        crate::dev::locate("workerd", "VRT_WORKERD")
    };
    let Some(path) = path else {
        return Check::action(
            id,
            "workerd not found on PATH (VRT_WORKERD unset)",
            "install workerd, set VRT_WORKERD, or pass --workerd to dev/serve; \
             --stub uses vrt-stub-workerd for local development",
        )
        .docs("deploy/standalone/README.md");
    };
    if !path.is_file() {
        return Check::fail(
            id,
            format!("{} missing", path.display()),
            "point VRT_WORKERD at a real binary",
        );
    }
    // `--version` distinguishes a real workerd from the dev stub —
    // same probe `vrt dev` uses.
    let ver = std::process::Command::new(&path).arg("--version").output();
    let Ok(out) = ver else {
        return Check::fail(
            id,
            format!("{} is not executable", path.display()),
            "check the file mode or replace the binary",
        );
    };
    let text =
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr);
    if text.contains("vrt-stub-workerd") {
        return Check::ok(id, format!("{} (vrt-stub-workerd)", path.display()));
    }
    match crate::dev::verify_workerd_pin(&path, true) {
        Ok(true) => Check::ok(id, format!("{} (pinned)", path.display())),
        Ok(false) => Check::action(
            id,
            format!("{} — no pinned digest for this platform", path.display()),
            "the binary runs but is unverified; pin coverage lives in deploy/docker/workerd.lock",
        )
        .docs("deploy/standalone/README.md"),
        Err(e) => Check::fail(
            id,
            format!("digest check failed: {e}"),
            "fetch the workerd pinned by deploy/docker/workerd.lock",
        ),
    }
}

/// --- root: exists + 0700 + owned ---
fn check_root(root: &Path) -> Check {
    let id = "root";
    if !root.is_dir() {
        return Check::fail(
            id,
            format!("{} does not exist", root.display()),
            "run `vrt init --root <dir> --cell <id> --tenant <id>`",
        )
        .docs("deploy/standalone/README.md");
    }
    match vrt_securefs::validate_secure_dir(root) {
        Ok(()) => Check::ok(id, format!("{} (0700, owned)", root.display())),
        Err(e) => Check::fail(
            id,
            format!("{}: {e}", root.display()),
            "chmod 0700 the root and keep it owned by this user",
        ),
    }
}

/// --- keys: signing seed + mgmt token load under mode rules ---
fn check_keys(root: &Path) -> Check {
    let id = "keys";
    if !root.is_dir() {
        return Check::action(id, "root missing — nothing to check", "run vrt init first");
    }
    let mut bad: Vec<String> = Vec::new();
    if let Err(e) = crate::keys::load_signing_seed(root) {
        bad.push(format!("signing.seed ({e})"));
    }
    if let Err(e) = crate::keys::load_mgmt_token(root) {
        bad.push(format!("mgmt/token ({e})"));
    }
    if bad.is_empty() {
        Check::ok(id, "signing.seed + mgmt token (0600, owned)")
    } else {
        Check::fail(
            id,
            format!("unusable key material: {}", bad.join(", ")),
            "key files must be 0600 and owned by this user — re-init or restore permissions",
        )
    }
}

/// --- disk: free space on the root filesystem ---
fn check_disk(root: &Path) -> Check {
    let id = "disk";
    let target = if root.is_dir() {
        root.to_path_buf()
    } else {
        // `Path::parent` of a bare name is `""` — statvfs would fail
        // on the empty string, so fall back to ".".
        root.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    };
    let path = std::ffi::CString::new(target.as_os_str().as_encoded_bytes()).unwrap_or_default();
// SAFETY: statvfs is plain old data; an all-zero value is a valid initializer.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
// SAFETY: path is a valid NUL-terminated CString and the out-pointer is a live, correctly sized statvfs.
    let rc = unsafe { libc::statvfs(path.as_ptr(), &mut stat) };
    if rc != 0 {
        return Check::fail(id, "statvfs failed", "check the root filesystem");
    }
    let free = stat.f_bavail as u128 * stat.f_frsize as u128;
    let free_mb = free / (1024 * 1024);
    if free_mb < 64 {
        Check::fail(
            id,
            format!("{free_mb} MiB free on {}", target.display()),
            "free disk space — the object store and queue/do state grow",
        )
    } else if free_mb < 512 {
        Check::action(
            id,
            format!("{free_mb} MiB free on {}", target.display()),
            "below 512 MiB — clean objects or grow the volume",
        )
    } else {
        Check::ok(id, format!("{free_mb} MiB free"))
    }
}

/// --- environment: container vs host (informational) ---
fn check_environment() -> Check {
    let id = "environment";
    let in_container = Path::new("/.dockerenv").exists()
        || std::fs::read_to_string("/proc/1/cgroup")
            .map(|c| c.contains("docker") || c.contains("kubepods") || c.contains("containerd"))
            .unwrap_or(false);
    let detail = if in_container {
        "running inside a container"
    } else {
        "running on the host"
    };
    Check::ok(
        id,
        format!(
            "{detail} ({} {})",
            std::env::consts::OS,
            std::env::consts::ARCH
        ),
    )
}

/// Minimal `key = "value"` line lookup in a toml-ish config file —
/// same convention as `logtail::control_listen_addr`.
fn toml_value(path: &Path, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(key) {
            let v = rest
                .trim_start_matches([' ', '='])
                .split('#')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"');
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// GET <addr>/healthz over plain TCP → the status code (0 = connect
/// or protocol failure).
fn tcp_healthz(addr: &str) -> u16 {
    let sock: std::net::SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(_) => return 0,
    };
    let Ok(mut s) = std::net::TcpStream::connect_timeout(&sock, std::time::Duration::from_secs(2))
    else {
        return 0;
    };
    let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(2)));
    let req = format!("GET /healthz HTTP/1.0\r\nhost: {addr}\r\n\r\n");
    if s.write_all(req.as_bytes()).is_err() {
        return 0;
    }
    let mut buf = vec![0u8; 4096];
    let n = match s.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return 0,
    };
    String::from_utf8_lossy(&buf[..n])
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// GET /healthz on the mgmt unix socket.
fn unix_healthz(sock: &Path) -> u16 {
    use std::os::unix::net::UnixStream;
    let Ok(mut s) = UnixStream::connect(sock) else {
        return 0;
    };
    let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(2)));
    if s.write_all(b"GET /healthz HTTP/1.0\r\nhost: vrt\r\n\r\n")
        .is_err()
    {
        return 0;
    }
    let mut buf = vec![0u8; 4096];
    let n = match s.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return 0,
    };
    String::from_utf8_lossy(&buf[..n])
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// One listener line → check entry. `free` = bindable right now.
fn port_entry(name: &str, addr: &str) -> Option<Check> {
    let sa: std::net::SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(_) => {
            return Some(Check::fail(
                "ports",
                format!("{name} = {addr} — not a socket address"),
                "fix the config entry",
            ))
        }
    };
    match std::net::TcpListener::bind(sa) {
        Ok(_) => Some(Check::ok("ports", format!("{name} {addr} available"))),
        Err(_) => {
            // Busy — is it OUR stack answering /healthz, or a stranger?
            if tcp_healthz(addr) == 200 {
                Some(Check::ok(
                    "ports",
                    format!("{name} {addr} serving a live listener"),
                ))
            } else {
                Some(Check::action(
                    "ports",
                    format!("{name} {addr} is in use"),
                    "stop the process holding it or change the listen addr",
                ))
            }
        }
    }
}

/// --- ports: every configured listener is free-or-ours ---
fn check_ports(root: &Path) -> Check {
    let id = "ports";
    let cfg = root.join("config");
    if !cfg.is_dir() {
        return Check::action(id, "no config dir — nothing to check", "run vrt init first");
    }
    let mut seen: Vec<String> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    for (file, keys) in [
        (
            "vrt-host.toml",
            &["listen", "controlListen", "metricsListen"][..],
        ),
        ("vrt-provider.toml", &["metricsListen"][..]),
        ("vrt-sql.toml", &["listenTcp", "metricsListen"][..]),
    ] {
        let p = cfg.join(file);
        if !p.is_file() {
            continue;
        }
        for k in keys {
            let Some(addr) = toml_value(&p, k) else {
                continue;
            };
            // Only real ip:port listeners are probed — unix dirs and
            // placeholders are out of scope.
            if addr.parse::<std::net::SocketAddr>().is_err() {
                continue;
            }
            match port_entry(&format!("{file}:{k}"), &addr) {
                Some(c) if c.status == "ok" => seen.push(format!("{k} {addr}")),
                Some(c) => {
                    problems.push(format!("{file} {k} {addr}: {}", c.detail));
                    seen.push(c.detail);
                }
                None => {}
            }
        }
    }
    if problems.is_empty() {
        if seen.is_empty() {
            Check::ok(id, "no listeners configured")
        } else {
            Check::ok(id, format!("{} listeners checked", seen.len()))
        }
    } else {
        Check::action(
            id,
            format!("{} busy/conflict: {}", problems.len(), problems.join("; ")),
            "stop the processes holding the ports or change the listen addrs",
        )
    }
}

/// --- facade: seeded manifest parses + dist digests + objects ---
fn check_facade(root: &Path) -> Check {
    let id = "facade";
    let manifest_path = crate::layout::facade_manifest_path(root);
    if !manifest_path.is_file() {
        return Check::ok(id, "no facade manifest seeded (optional)");
    }
    let Ok(raw) = std::fs::read_to_string(&manifest_path) else {
        return Check::fail(id, "manifest unreadable", "re-run vrt init --facade-dist");
    };
    let Ok(manifest) = vrt_config::parse_facade_manifest(&raw) else {
        return Check::fail(
            id,
            "manifest fails to parse",
            "re-run vrt init --facade-dist",
        );
    };
    let wire: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return Check::fail(id, "manifest is not json", "re-run vrt init"),
    };
    let dist_root = manifest_path.parent().unwrap_or(root).join("dist");
    let mut checked = 0usize;
    for worker in manifest.system_workers {
        let Some(w) = wire["workers"][&worker.service_name].as_object() else {
            return Check::fail(id, "manifest worker missing from wire form", "re-init");
        };
        let Some(modules) = w["modules"].as_array() else {
            return Check::fail(id, "manifest modules missing", "re-init");
        };
        for m in modules {
            let Some(name) = m["name"].as_str() else {
                continue;
            };
            let Some(declared) = m["sha256"].as_str() else {
                continue;
            };
            let rel = Path::new(name);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return Check::fail(id, format!("module path unsafe: {name}"), "re-init");
            }
            let path = dist_root.join(rel);
            let Ok(bytes) = std::fs::read(&path) else {
                return Check::fail(
                    id,
                    format!("module missing: {name}"),
                    "restore facade/dist or re-init",
                );
            };
            use sha2::Digest as _;
            if hex::encode(sha2::Sha256::digest(&bytes)) != declared {
                return Check::fail(
                    id,
                    format!("module digest mismatch: {name}"),
                    "facade dist was modified — re-init from a clean dist",
                );
            }
            checked += 1;
        }
    }
    Check::ok(
        id,
        format!("manifest ok — {checked} system modules verified"),
    )
}

/// Shared service-check mapping: reachable 200 → ok; refused/timeout
/// → action (not running); anything else → fail.
fn service_check(id: &str, what: &str, status: u16, not_running_hint: &str) -> Check {
    match status {
        200 => Check::ok(id, format!("{what} healthy")),
        0 => Check::action(id, format!("{what} not reachable"), not_running_hint),
        s => Check::fail(
            id,
            format!("{what} answered {s}"),
            "the process is up but unhealthy — check its logs",
        ),
    }
}

fn check_service_mgmt(root: &Path) -> Check {
    let sock = crate::layout::mgmt_dir(root).join("vrt.sock");
    if !root.is_dir() {
        return Check::action("service.mgmt", "root missing", "run vrt init first");
    }
    service_check(
        "service.mgmt",
        "mgmt socket /healthz",
        unix_healthz(&sock),
        "start the control plane: vrt serve --root <dir>",
    )
}

fn check_service_host(root: &Path) -> Check {
    let id = "service.host";
    let cfg = root.join("config/vrt-host.toml");
    if !cfg.is_file() {
        return Check::action(id, "no vrt-host.toml", "run vrt init first");
    }
    let Some(addr) = toml_value(&cfg, "controlListen") else {
        return Check::action(
            id,
            "vrt-host.toml has no controlListen",
            "set controlListen so health probes and the log tail work",
        );
    };
    service_check(
        id,
        "vrt-host control /healthz",
        tcp_healthz(&addr),
        "vrt-host is not running (vrt serve --root <dir>)",
    )
}

fn check_service_provider(root: &Path) -> Check {
    let id = "service.provider";
    let cfg = root.join("config/vrt-provider.toml");
    if !cfg.is_file() {
        return Check::action(id, "no vrt-provider.toml", "run vrt init first");
    }
    let Some(addr) = toml_value(&cfg, "metricsListen") else {
        return Check::action(
            id,
            "vrt-provider.toml has no metricsListen",
            "set metricsListen so the provider health probe works",
        );
    };
    service_check(
        id,
        "vrt-provider /healthz",
        tcp_healthz(&addr),
        "vrt-provider is not running",
    )
}

fn check_service_sql(root: &Path) -> Check {
    let id = "service.sql";
    let cfg = root.join("config/vrt-sql.toml");
    if !cfg.is_file() {
        // SQL is optional in standalone — absence is not a problem.
        return Check::ok(id, "vrt-sql not configured (optional)");
    }
    let Some(addr) = toml_value(&cfg, "metricsListen") else {
        return Check::action(
            id,
            "vrt-sql.toml has no metricsListen",
            "set metricsListen so the sql health probe works",
        );
    };
    service_check(
        id,
        "vrt-sql /healthz",
        tcp_healthz(&addr),
        "vrt-sql is not running",
    )
}

/// --- project: wrangler/vrt config loads; unsupported keys surface ---
fn check_project(dir: &Path) -> Check {
    let id = "project";
    let has_cfg = dir.join("vrt.toml").exists()
        || [
            "wrangler.toml",
            "wrangler.json",
            "wrangler.jsonc",
            "wrangler.config.ts",
            "wrangler.config.js",
            "cloudflare.config.ts",
            "cloudflare.config.js",
        ]
        .iter()
        .any(|n| dir.join(n).exists());
    if !has_cfg {
        return Check::ok(id, format!("no project config in {}", dir.display()));
    }
    match crate::project_cfg::load_worker_meta(dir) {
        Ok((meta, src)) => Check::ok(
            id,
            format!("{} loads — worker '{}' ({src:?})", dir.display(), meta.name),
        ),
        Err(LocalError::WorkerOwned(msg)) | Err(LocalError::Op(_, msg)) => {
            classify_project_err(&msg)
        }
        Err(e) => classify_project_err(&e.to_string()),
    }
}

/// Config error → status: "not supported" (and programmatic configs)
/// are actionable edits, not runtime failures; unreadable files are
/// hard failures.
fn classify_project_err(msg: &str) -> Check {
    let id = "project";
    if msg.contains("not supported by the VRT runtime") || msg.contains("programmatic config") {
        Check::action(
            id,
            format!("config issue: {msg}"),
            "edit the wrangler/vrt config — the key is unsupported in standalone",
        )
        .docs("deploy/standalone/README.md")
    } else if msg.contains("cannot read config file") {
        Check::fail(
            id,
            format!("config unreadable: {msg}"),
            "fix file permissions",
        )
    } else {
        Check::fail(
            id,
            format!("config invalid: {msg}"),
            "fix the config — deploy would reject it",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_passing_rules() {
        let mut r = Report {
            root: "x".into(),
            ok: true,
            checks: vec![Check::ok("a", "fine"), Check::action("b", "meh", "do it")],
        };
        assert!(r.passing(false));
        assert!(!r.passing(true));
        r.checks.push(Check::fail("c", "broken", "fix"));
        assert!(!r.passing(false));
        assert!(!r.passing(true));
    }

    #[test]
    fn root_check_flags_missing_and_bad_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        // tempfile's mode is umask-dependent — pin the invariant we
        // are actually testing rather than the platform default.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let missing = dir.path().join("nope");
        assert_eq!(check_root(&missing).status, "fail");
        let ok = check_root(dir.path());
        assert_eq!(ok.status, "ok", "{ok:?}");
        // Loosen it → fail.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(check_root(dir.path()).status, "fail");
    }

    #[test]
    fn toml_value_finds_quoted_addr() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.toml");
        std::fs::write(&p, "listen = \"127.0.0.1:8080\" # trailing comment\n").unwrap();
        assert_eq!(toml_value(&p, "listen"), Some("127.0.0.1:8080".to_string()));
        assert_eq!(toml_value(&p, "metricsListen"), None);
    }

    #[test]
    fn port_entry_reports_free_busy_and_live() {
        // Free port → available.
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = free.local_addr().unwrap().to_string();
        drop(free);
        let c = port_entry("x", &addr).unwrap();
        assert_eq!(c.status, "ok");
        // Busy without /healthz → action.
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy = held.local_addr().unwrap().to_string();
        let c = port_entry("x", &busy).unwrap();
        assert_eq!(c.status, "action");
    }

    #[test]
    fn project_check_no_config_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(check_project(dir.path()).status, "ok");
    }

    #[test]
    fn project_check_unsupported_is_action_invalid_is_fail() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("wrangler.toml"),
            "name = \"x\"\nmain = \"index.js\"\ncompatibility_date = \"2024-09-25\"\nobservability_unknown = true\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("index.js"), "export default {}\n").unwrap();
        let c = check_project(dir.path());
        assert_eq!(c.status, "action", "unsupported key → action, got {c:?}");
        std::fs::write(dir.path().join("wrangler.toml"), "not = [valid\n").unwrap();
        let c = check_project(dir.path());
        assert_eq!(c.status, "fail", "broken toml → fail, got {c:?}");
    }

    #[test]
    fn service_check_maps_statuses() {
        assert_eq!(service_check("s", "x", 200, "h").status, "ok");
        assert_eq!(service_check("s", "x", 0, "h").status, "action");
        assert_eq!(service_check("s", "x", 500, "h").status, "fail");
    }

    #[test]
    fn environment_is_informational_ok() {
        assert_eq!(check_environment().status, "ok");
    }

    /// The `--json` wire shape is a stable contract for tooling —
    /// field names, order, and the ok/status vocabulary must not
    /// drift silently.
    #[test]
    fn json_report_shape_is_stable() {
        let report = Report {
            root: "/r".into(),
            ok: false,
            checks: vec![
                Check::ok("workerd", "present"),
                Check::action("disk", "low", "free space"),
                Check::fail("keys", "bad mode", "chmod 0600").docs("deploy/standalone/README.md"),
            ],
        };
        let v = serde_json::to_value(&report).unwrap();
        assert_eq!(v["root"], "/r");
        assert_eq!(v["ok"], false);
        let arr = v["checks"].as_array().unwrap();
        assert_eq!(arr.len(), 3);
        let obj = arr[0].as_object().unwrap();
        // Exact key set — a new field must be a conscious wire change.
        let mut keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["detail", "id", "status"]);
        assert_eq!(arr[0]["status"], "ok");
        assert_eq!(arr[1]["status"], "action");
        assert_eq!(arr[2]["status"], "fail");
        assert_eq!(arr[1]["hint"], "free space");
        assert_eq!(arr[2]["docs"], "deploy/standalone/README.md");
        // CamelCase naming on the report itself.
        assert!(v.as_object().unwrap().contains_key("checks"));
    }

    /// Every check id the command can emit — guards against silent
    /// id drift (JSON consumers key off these).
    #[test]
    fn check_ids_cover_the_spec() {
        let dir = tempfile::tempdir().unwrap();
        let r = run(&dir.path().join("missing-root"), dir.path());
        let ids: Vec<&str> = r.checks.iter().map(|c| c.id.as_str()).collect();
        for want in [
            "workerd",
            "root",
            "keys",
            "disk",
            "environment",
            "ports",
            "facade",
            "service.mgmt",
            "service.host",
            "service.provider",
            "service.sql",
            "project",
        ] {
            assert!(ids.contains(&want), "missing check {want} in {ids:?}");
        }
    }
}
