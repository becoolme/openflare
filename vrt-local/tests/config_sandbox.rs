//! `--config-sandbox=worker` integration matrix.
//!
//! These tests need a real workerd binary (`VRT_WORKERD_BIN`) and the
//! `of-bundler` helper (built in CI, or `OF_BUNDLER`). Without them every
//! test prints a skip line and passes — the same convention as the other
//! real-workerd suites. CI's `real-workerd` job runs them for real.
//!
//! What is asserted:
//!   * mode / isPreview parity with the Node loader (deep-equal RawConfig);
//!   * nested `defineWorker` / `defineContainer` factories resolve;
//!   * the allowlisted `process.env` is injected (and nothing else);
//!   * `with { type: "cf-worker" }` stays a specifier (entry never runs);
//!   * hostile configs fail closed: host FS, outbound fetch, oversized
//!     payload, and `while (true)` bounded by the wall-clock deadline.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use vrt_local::{load_cf_project_config, ConfigSandbox};

const COMPAT: &str = "2026-09-17";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn workerd() -> Option<PathBuf> {
    let p = std::env::var_os("VRT_WORKERD_BIN")?;
    let p = PathBuf::from(p);
    p.is_file().then_some(p)
}

/// The `of-bundler` helper: `OF_BUNDLER` wins, then the in-repo build.
fn bundler() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("OF_BUNDLER") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    ["debug", "release"].iter().find_map(|profile| {
        let p = repo_root()
            .join("of-bundler/target")
            .join(profile)
            .join("of-bundler");
        p.is_file().then_some(p)
    })
}

fn require_workerd(test: &str) -> bool {
    if std::env::var_os("VRT_REQUIRE_WORKERD").is_some() {
        // CI's real-workerd job sets this: a missing binary must fail the
        // job instead of turning the whole sandbox suite into silent skips.
        assert!(
            workerd().is_some(),
            "{test}: VRT_REQUIRE_WORKERD is set but VRT_WORKERD_BIN is unset or not a file"
        );
        assert!(
            bundler().is_some(),
            "{test}: VRT_REQUIRE_WORKERD is set but the of-bundler helper is missing \
             (build of-bundler/Cargo.toml or set OF_BUNDLER)"
        );
        return true;
    }
    if workerd().is_none() {
        eprintln!("SKIP {test}: VRT_WORKERD_BIN unset or not a file");
        return false;
    }
    if bundler().is_none() {
        eprintln!("SKIP {test}: of-bundler helper missing (build of-bundler/Cargo.toml)");
        return false;
    }
    true
}

fn load(
    dir: &Path,
    sandbox: ConfigSandbox,
    env: Option<&str>,
    preview: bool,
) -> Result<serde_json::Value, String> {
    load_cf_project_config(dir, "cloudflare.config.ts", env, preview, sandbox)
        .map(|(_, raw, _)| raw)
        .map_err(|e| format!("{e}"))
}

/// The Node path resolves `@cloudflare/config` from the project, so a
/// generated project needs the package linked the way a user install does.
#[cfg(unix)]
fn link_config_package(dir: &Path) {
    let package = repo_root().join("node_modules/@cloudflare/config");
    let target = dir.join("node_modules/@cloudflare/config");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(package, target).unwrap();
}

#[cfg(not(unix))]
fn link_config_package(_dir: &Path) {}

fn project(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (name, body) in files {
        let path = dir.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }
    link_config_package(dir.path());
    dir
}

// ── `of check` children with an attributable temp root ─────────────────
//
// The sandbox driver mkdtemps `of-cfg-sandbox-*` inside `os.tmpdir()` and
// starts workerd with that directory as its cwd. Pointing TMPDIR at a
// per-test root therefore makes both the temp dirs and any leftover workerd
// process attributable even when other tests in this binary run in parallel.

/// `of check --config-sandbox=worker` with an isolated TMPDIR and the
/// real-workerd prerequisites pinned for the child.
fn of_check_worker(dir: &Path, tmp: &Path) -> std::process::Command {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_of"));
    cmd.args(["check", "--config-sandbox=worker", dir.to_str().unwrap()])
        .current_dir(repo_root())
        .env("TMPDIR", tmp)
        .env("TMP", tmp)
        .env("TEMP", tmp)
        .env("VRT_WORKERD_BIN", workerd().expect("workerd"))
        .env("OF_BUNDLER", bundler().expect("of-bundler"))
        .env("VRT_CF_CONFIG_SANDBOX_TIMEOUT_MS", "5000");
    cmd
}

fn spawn_of_check_worker(dir: &Path, tmp: &Path) -> std::process::Child {
    of_check_worker(dir, tmp)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn of check")
}

/// Every entry under the private temp root. The driver creates exactly one
/// `of-cfg-sandbox-*` dir per run and removes it, so anything left (of any
/// name) is residue.
fn leftover_workdirs(root: &Path) -> Vec<String> {
    let mut names = match std::fs::read_dir(root) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    names.sort();
    names
}

#[cfg(target_os = "linux")]
fn process_cwd(pid: &str) -> Option<PathBuf> {
    std::fs::read_link(format!("/proc/{pid}/cwd")).ok()
}

#[cfg(target_os = "macos")]
fn process_cwd(pid: &str) -> Option<PathBuf> {
    let out = std::process::Command::new("lsof")
        .args(["-a", "-p", pid, "-d", "cwd", "-Fn"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .find_map(|line| line.strip_prefix('n').map(PathBuf::from))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_cwd(_pid: &str) -> Option<PathBuf> {
    None
}

/// Every `workerd` process whose cwd sits under `root`, formatted for a
/// panic message. workerd is spawned with cwd = the sandbox work dir, so
/// this attributes leftovers to one test instead of the whole machine.
fn leftover_workerd(root: &Path) -> Vec<String> {
    let Ok(out) = std::process::Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
    else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let mut found = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        let Some((pid, command)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        if !command.contains("workerd") {
            continue;
        }
        let pid = pid.trim();
        if process_cwd(pid)
            .map(|cwd| cwd.starts_with(root))
            .unwrap_or(false)
        {
            found.push(format!(
                "pid {pid} (cwd under {}): {command}",
                root.display()
            ));
        }
    }
    found
}

/// Panic with the full diagnostic set the task asks for: temp root contents,
/// attributed workerd processes, and the child's stderr.
fn assert_no_sandbox_residue(root: &Path, context: &str) {
    let dirs = leftover_workdirs(root);
    let procs = leftover_workerd(root);
    assert!(
        dirs.is_empty() && procs.is_empty(),
        "{context}: sandbox residue under {}: temp dirs {dirs:?}; workerd processes {procs:#?}",
        root.display()
    );
}

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

#[test]
fn deploy_overrides_default_to_the_node_sandbox() {
    assert_eq!(
        vrt_local::worker_pkg::DeployOverrides::default().config_sandbox,
        ConfigSandbox::Node
    );
}

#[test]
fn of_check_and_dry_run_accept_the_sandbox_flag() {
    if !require_workerd("of_check_and_dry_run_accept_the_sandbox_flag") {
        return;
    }
    let fixture = repo_root().join("vrt-local/tests/fixtures/cf-config");
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_of"))
            .args(args)
            .current_dir(&repo_root())
            // Tests in this binary run in parallel; the matrix temporarily
            // lowers the deadline, so pin it for these child processes.
            .env("VRT_CF_CONFIG_SANDBOX_TIMEOUT_MS", "5000")
            .output()
            .expect("spawn of")
    };

    // The `--flag=value` spelling must not swallow the positional dir.
    let out = run(&[
        "check",
        "--env",
        "staging",
        "--config-sandbox=worker",
        fixture.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "of check failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["ok"], true);
    assert!(report["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b == "var:STAGE"));

    // `--dry-run` packs through the sandbox without needing a live root.
    let out = run(&[
        "deploy",
        "--root",
        "/nonexistent-of-root",
        "--dry-run",
        "--config-sandbox",
        "worker",
        fixture.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "of deploy --dry-run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Unknown values fail closed; the default stays `node`.
    let out = run(&["check", "--config-sandbox=wasm", fixture.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--config-sandbox must be node|worker"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Worker mode without a workerd binary fails closed (no silent Node
    // fallback).
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_of"))
        .args([
            "check",
            "--config-sandbox=worker",
            fixture.to_str().unwrap(),
        ])
        .current_dir(repo_root())
        .env("VRT_CF_CONFIG_SANDBOX_TIMEOUT_MS", "5000")
        .env_remove("VRT_WORKERD_BIN")
        .env_remove("OF_WORKERD_BIN")
        .output()
        .expect("spawn of");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("VRT_CONFIG_LOADER_FAILED"), "{stderr}");
    assert!(stderr.contains("workerd"), "{stderr}");
}

#[test]
fn worker_sandbox_matrix() {
    if !require_workerd("worker_sandbox_matrix") {
        return;
    }
    // ── parity: mode and isPreview reach the config exactly like Node ──
    let fixture = repo_root().join("vrt-local/tests/fixtures/cf-config");
    for (env, preview) in [(None, false), (Some("staging"), false), (None, true)] {
        let node = load(&fixture, ConfigSandbox::Node, env, preview)
            .unwrap_or_else(|e| panic!("node path failed: {e}"));
        let worker = load(&fixture, ConfigSandbox::Worker, env, preview)
            .unwrap_or_else(|e| panic!("worker path failed: {e}"));
        assert_eq!(
            worker, node,
            "sandbox RawConfig differs (mode={env:?} preview={preview})"
        );
    }

    // ── nested factories ───────────────────────────────────────────────
    let nested = project(&[(
        "cloudflare.config.ts",
        r#"import { bindings, defineConfig, defineWorker } from "@cloudflare/config";
const api = defineWorker(() => ({
  name: "nested-worker",
  compatibilityDate: "2026-09-17",
  env: { FROM_NESTED: bindings.text("yes") },
}));
export default defineConfig({ worker: api });
"#,
    )]);
    let worker = load(nested.path(), ConfigSandbox::Worker, None, false)
        .unwrap_or_else(|e| panic!("nested worker path failed: {e}"));
    assert_eq!(worker["name"], "nested-worker");
    let node = load(nested.path(), ConfigSandbox::Node, None, false)
        .unwrap_or_else(|e| panic!("nested node path failed: {e}"));
    assert_eq!(worker, node, "nested definitions differ from the Node path");

    // Container definitions must resolve to their name before the raw
    // config reaches the runtime normalizer — which then rejects the
    // runtime-unsupported `containers` key. Both paths agree on that code,
    // which also proves the worker resolved the container reference (an
    // unresolved definition would fail schema validation instead).
    let nested_container = project(&[(
        "cloudflare.config.ts",
        r#"import { bindings, defineConfig, defineContainer, defineWorker, exports } from "@cloudflare/config";
const container = defineContainer({ name: "svc", image: { dockerfile: "./Dockerfile" } });
const api = defineWorker(() => ({
  name: "nested-container",
  compatibilityDate: "2026-09-17",
  env: { FROM_NESTED: bindings.text("yes") },
  exports: { MyContainerDO: exports.durableObject({ storage: "sqlite", container }) },
}));
export default defineConfig({ worker: api, containers: [container] });
"#,
    )]);
    let err = load(nested_container.path(), ConfigSandbox::Worker, None, false)
        .expect_err("containers are not supported by the runtime");
    assert!(err.contains("VRT_CONFIG_UNSUPPORTED_FIELD"), "{err}");
    assert!(err.contains("containers"), "{err}");
    let node_err = load(nested_container.path(), ConfigSandbox::Node, None, false)
        .expect_err("node path must reject containers the same way");
    assert!(
        node_err.contains("VRT_CONFIG_UNSUPPORTED_FIELD"),
        "{node_err}"
    );

    // ── process.env: the Node path's allowlist is injected, nothing else ─
    let env_probe = project(&[(
        "cloudflare.config.ts",
        r#"import { bindings, defineConfig } from "@cloudflare/config";
const tz = process.env.TZ;
const nodeEnv = process.env.NODE_ENV;
export default defineConfig(() => ({
  worker: {
    name: "env-probe",
    compatibilityDate: "2026-09-17",
    env: {
      TZ_SEEN: bindings.text(tz === undefined ? "absent" : tz),
      NODE_ENV_SEEN: bindings.text(nodeEnv === undefined ? "absent" : nodeEnv),
      SECRET_SEEN: bindings.text(process.env.VRT_TEST_SECRET === undefined ? "absent" : "leaked"),
      CWD_RELATIVE: bindings.text(process.cwd().split("/").pop() ?? ""),
    },
  },
}));
"#,
    )]);
    let _secret = EnvGuard::set("VRT_TEST_SECRET", "must-not-cross-the-boundary");
    let worker = load(env_probe.path(), ConfigSandbox::Worker, None, false)
        .unwrap_or_else(|e| panic!("env worker path failed: {e}"));
    let vars = worker["vars"].as_object().expect("vars");
    let expected_tz = std::env::var("TZ").unwrap_or_else(|_| "absent".into());
    assert_eq!(vars["TZ_SEEN"], expected_tz);
    assert_eq!(
        vars["NODE_ENV_SEEN"], "absent",
        "NODE_ENV is not allowlisted"
    );
    assert_eq!(
        vars["SECRET_SEEN"], "absent",
        "non-allowlisted secrets must not cross"
    );
    let dir_name = env_probe
        .path()
        .canonicalize()
        .unwrap()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert_eq!(vars["CWD_RELATIVE"], dir_name);

    // ── cf-worker imports stay specifiers; the entry never executes ─────
    let entry = project(&[
        (
            "cloudflare.config.ts",
            r#"import { defineConfig } from "@cloudflare/config";
import * as entrypoint from "./src/index.js" with { type: "cf-worker" };
export default defineConfig({ worker: { name: "entry", compatibilityDate: "2026-09-17", entrypoint } });
"#,
        ),
        (
            "src/index.js",
            "throw new Error('the cf-worker entry must never execute');\n",
        ),
    ]);
    let worker = load(entry.path(), ConfigSandbox::Worker, None, false)
        .unwrap_or_else(|e| panic!("cf-worker import failed: {e}"));
    assert!(
        worker["main"]
            .as_str()
            .unwrap_or_default()
            .ends_with("src/index.js"),
        "{worker}"
    );

    // A computed cf-worker specifier cannot be synthesised; fail closed
    // instead of bundling (and executing) the entry module.
    let computed = project(&[
        (
            "cloudflare.config.ts",
            r#"import { defineConfig } from "@cloudflare/config";
const spec = "./src/index.js";
export default defineConfig(async () => {
  const entrypoint = await import(spec, { with: { type: "cf-worker" } });
  return { worker: { name: "computed", compatibilityDate: "2026-09-17", entrypoint } };
});
"#,
        ),
        ("src/index.js", "export default {};\n"),
    ]);
    let err =
        load(computed.path(), ConfigSandbox::Worker, None, false).expect_err("must fail closed");
    assert!(err.contains("unsupported form"), "{err}");

    // ── hostile configs: host FS and the network are unreachable ───────
    let fs_read = project(&[(
        "cloudflare.config.ts",
        r#"import { readFileSync } from "node:fs";
import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(() => ({
  worker: { name: "fs", compatibilityDate: "2026-09-17", env: { LEAK: bindings.text(readFileSync("/etc/passwd", "utf8")) } },
}));
"#,
    )]);
    let err = load(fs_read.path(), ConfigSandbox::Worker, None, false)
        .expect_err("host FS must be blocked");
    assert!(err.contains("VRT_CONFIG_LOADER_FAILED"), "{err}");

    let network = project(&[(
        "cloudflare.config.ts",
        r#"import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(async () => {
  const res = await fetch("http://169.254.169.254/latest/meta-data/");
  return { worker: { name: "net", compatibilityDate: "2026-09-17", env: { BODY: bindings.text(await res.text()) } } };
});
"#,
    )]);
    let err = load(network.path(), ConfigSandbox::Worker, None, false)
        .expect_err("network must be blocked");
    assert!(err.contains("VRT_CONFIG_LOADER_FAILED"), "{err}");

    // ── oversized payload ───────────────────────────────────────────────
    let big = format!(
        r#"import {{ bindings, defineConfig }} from "@cloudflare/config";
const pad = "x".repeat(600 * 1024);
export default defineConfig(() => ({{ worker: {{ name: "big", compatibilityDate: "{COMPAT}", env: {{ P: bindings.text(pad) }} }} }}));
"#
    );
    let oversized = project(&[("cloudflare.config.ts", &big)]);
    let err = load(oversized.path(), ConfigSandbox::Worker, None, false)
        .expect_err("payload must be capped");
    assert!(
        err.contains("exceeds") || err.contains("too large"),
        "{err}"
    );

    // ── validation stays on the trusted side (exit 2 contract) ──────────
    let invalid = project(&[(
        "cloudflare.config.ts",
        r#"import { defineConfig } from "@cloudflare/config";
export default defineConfig(() => ({ worker: { name: 42 } }));
"#,
    )]);
    let err = load(invalid.path(), ConfigSandbox::Worker, None, false)
        .expect_err("invalid config must fail");
    assert!(err.contains("VRT_CONFIG_INVALID"), "{err}");
    assert!(
        err.contains("worker.name"),
        "issue path must be reported: {err}"
    );

    // ── wall-clock deadline: `while (true)` fails, it never hangs ───────
    let _timeout = EnvGuard::set("VRT_CF_CONFIG_SANDBOX_TIMEOUT_MS", "800");
    let infinite = project(&[(
        "cloudflare.config.ts",
        r#"import { defineConfig } from "@cloudflare/config";
export default defineConfig(() => { while (true) {} });
"#,
    )]);
    let started = Instant::now();
    let err = load(infinite.path(), ConfigSandbox::Worker, None, false).expect_err("must time out");
    let elapsed = started.elapsed();
    assert!(err.contains("VRT_CONFIG_LOADER_TIMEOUT"), "{err}");
    assert!(
        elapsed < Duration::from_secs(15),
        "timeout took {elapsed:?}; the caller deadline must bound workerd"
    );
}

// ── robustness: repeated runs, concurrency, memory, real project parity ─

/// Ten sequential `of check --config-sandbox=worker` runs on one config must
/// all succeed and leave neither sandbox temp dirs nor workerd processes
/// behind. Catches leaks that a single run (or an in-process load) hides.
#[test]
fn repeated_worker_sandbox_runs_leave_no_residue() {
    if !require_workerd("repeated_worker_sandbox_runs_leave_no_residue") {
        return;
    }
    let fixture = repo_root().join("vrt-local/tests/fixtures/cf-config");
    let root = tempfile::tempdir().unwrap();
    for run in 1..=10 {
        let out = of_check_worker(&fixture, root.path())
            .output()
            .expect("spawn of check");
        assert!(
            out.status.success(),
            "run {run}/10 failed (status {:?})\nstdout: {}\nstderr: {}\nresidue: {:#?}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
            leftover_workerd(root.path())
        );
        let report: serde_json::Value = serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|e| panic!("run {run}/10 printed invalid JSON: {e}"));
        assert_eq!(report["ok"], true, "run {run}/10 report: {report}");
        assert!(
            leftover_workerd(root.path()).is_empty(),
            "run {run}/10 left workerd running: {:#?}",
            leftover_workerd(root.path())
        );
    }
    assert_no_sandbox_residue(root.path(), "after 10 sequential runs");
}

/// Two sandbox runs at the same time must not share state. Each driver
/// mkdtemps its own work dir and binds an ephemeral port, so:
///   * distinct TMPDIRs are fully isolated (the required case);
///   * one shared TMPDIR is *not* a lock either — it must still succeed.
/// A stolen ephemeral port is the only shared resource; that path fails
/// closed in the startup phase (`VRT_CONFIG_LOADER_FAILED` within
/// `STARTUP_TIMEOUT_MS`) instead of hanging, which is why this test asserts
/// completion under a wall-clock bound rather than "never collides".
#[test]
fn concurrent_worker_sandbox_runs_are_isolated() {
    if !require_workerd("concurrent_worker_sandbox_runs_are_isolated") {
        return;
    }
    let fixture = repo_root().join("vrt-local/tests/fixtures/cf-config");
    let started = Instant::now();

    // Phase A: the required shape — two runs, two private temp roots.
    let roots = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let children: Vec<_> = roots
        .iter()
        .map(|root| spawn_of_check_worker(&fixture, root.path()))
        .collect();
    for (i, child) in children.into_iter().enumerate() {
        let out = child.wait_with_output().expect("wait of check");
        assert!(
            out.status.success(),
            "concurrent run {i} failed (status {:?})\nstdout: {}\nstderr: {}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    for (i, root) in roots.iter().enumerate() {
        assert_no_sandbox_residue(root.path(), &format!("concurrent run {i}"));
    }

    // Phase B: both children share one TMPDIR on purpose. Nothing in the
    // driver locks or names a path relative to TMPDIR itself, so both must
    // still succeed; if that ever stops being true this test fails loudly.
    let shared = tempfile::tempdir().unwrap();
    let children: Vec<_> = (0..2)
        .map(|_| spawn_of_check_worker(&fixture, shared.path()))
        .collect();
    for (i, child) in children.into_iter().enumerate() {
        let out = child.wait_with_output().expect("wait of check");
        assert!(
            out.status.success(),
            "shared-root concurrent run {i} failed (status {:?})\nstdout: {}\nstderr: {}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    assert_no_sandbox_residue(shared.path(), "shared-root concurrent runs");

    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(60),
        "four concurrent sandbox runs took {elapsed:?}; a run must never hang"
    );
    eprintln!("concurrency: 2x distinct roots + 2x shared root in {elapsed:?}");
}

/// A config that asks for an impossible allocation must fail closed inside a
/// bounded wall clock with `VRT_CONFIG_LOADER_FAILED`, never succeed and
/// never hang.
///
/// `2 ** 52` bytes (~4 PiB) is above the address space of every supported
/// 64-bit host but below V8's max ArrayBuffer length (`2 ** 53 - 1`), so V8
/// *attempts* the allocation and fails — this is a real allocation failure,
/// not an invalid-length range check. It reserves nothing, so the test
/// cannot pressure the CI runner's memory the way `new Array(2e8).fill(0)`
/// (which succeeds on a 16 GiB runner) would.
#[test]
fn huge_allocation_fails_closed() {
    if !require_workerd("huge_allocation_fails_closed") {
        return;
    }
    let dir = project(&[(
        "cloudflare.config.ts",
        r#"import { bindings, defineConfig } from "@cloudflare/config";
export default defineConfig(() => {
  // ~4 PiB: V8 attempts the allocation, the host cannot back it.
  const buffer = new ArrayBuffer(2 ** 52);
  return { worker: { name: "oom", compatibilityDate: "2026-09-17", env: { N: bindings.text(String(buffer.byteLength)) } } };
});
"#,
    )]);
    let root = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let out = of_check_worker(dir.path(), root.path())
        .output()
        .expect("spawn of check");
    let elapsed = started.elapsed();
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    eprintln!(
        "huge allocation: {} in {elapsed:?}; stderr: {}",
        out.status,
        stderr.trim()
    );

    assert!(
        !out.status.success(),
        "an impossible allocation must not succeed; stdout: {stdout}"
    );
    assert!(
        stderr.contains("VRT_CONFIG_LOADER_FAILED"),
        "expected VRT_CONFIG_LOADER_FAILED, got: {stderr}"
    );
    assert!(
        !stderr.contains("VRT_CONFIG_INVALID") && !stderr.contains("VRT_CONFIG_UNSUPPORTED_FIELD"),
        "an allocation failure must not be reported as a config/schema error: {stderr}"
    );
    // Either the worker catches the allocation error (message names it) or
    // workerd dies outright and the driver reports the exit. Both are a
    // fail-closed OOM mapping; a bare socket error with no cause is not.
    assert!(
        stderr.contains("allocation")
            || stderr.contains("out of memory")
            || stderr.contains("OOM")
            || stderr.contains("workerd exited")
            || stderr.contains("ran out of memory"),
        "diagnostic does not identify the memory failure: {stderr}"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "memory failure took {elapsed:?}; the wall-clock deadline must bound it"
    );
    assert_no_sandbox_residue(root.path(), "after the OOM run");
}

/// The checked-in `examples/standalone-config-ts` project must normalize
/// identically through the Node loader and the worker sandbox. This is the
/// real-project counterpart to the synthetic fixture matrix: entrypoint,
/// triggers (routes), vars, and the mode/isPreview branches all cross the
/// sandbox boundary.
#[test]
fn config_ts_example_matches_the_node_loader() {
    if !require_workerd("config_ts_example_matches_the_node_loader") {
        return;
    }
    let example = repo_root().join("examples/standalone-config-ts");
    assert!(
        example.join("cloudflare.config.ts").is_file(),
        "example project missing: {}",
        example.display()
    );

    let mut checked = 0;
    for (env, preview) in [
        (None, false),
        (Some("staging"), false),
        (None, true),
        (Some("staging"), true),
    ] {
        let (node_norm, node_raw) = vrt_local::load_cf_project_config(
            &example,
            "cloudflare.config.ts",
            env,
            preview,
            ConfigSandbox::Node,
        )
        .map(|(norm, raw, _)| (norm, raw))
        .unwrap_or_else(|e| panic!("node path failed (mode={env:?} preview={preview}): {e}"));
        let (worker_norm, worker_raw) = vrt_local::load_cf_project_config(
            &example,
            "cloudflare.config.ts",
            env,
            preview,
            ConfigSandbox::Worker,
        )
        .map(|(norm, raw, _)| (norm, raw))
        .unwrap_or_else(|e| panic!("worker path failed (mode={env:?} preview={preview}): {e}"));
        assert_eq!(
            worker_raw, node_raw,
            "RawConfig differs for examples/standalone-config-ts (mode={env:?} preview={preview})"
        );
        assert_eq!(
            worker_norm, node_norm,
            "normalized config differs for examples/standalone-config-ts (mode={env:?} preview={preview})"
        );
        checked += 1;
    }
    assert_eq!(checked, 4);

    // Guard against a vacuous parity: the example must actually carry the
    // fields both paths are supposed to agree on.
    let raw = vrt_local::load_cf_project_config(
        &example,
        "cloudflare.config.ts",
        Some("staging"),
        false,
        ConfigSandbox::Worker,
    )
    .map(|(_, raw, _)| raw)
    .expect("worker path failed");
    assert_eq!(raw["name"], "config-ts", "{raw}");
    assert_eq!(raw["vars"]["GREETING"], "hello from cloudflare.config.ts");
    assert_eq!(raw["vars"]["STAGE"], "staging");
    assert_eq!(raw["routes"][0], "config-ts.vrt.local/*");
    assert!(
        raw["main"]
            .as_str()
            .unwrap_or_default()
            .ends_with("src/index.js"),
        "entrypoint was not carried into main: {raw}"
    );
}
