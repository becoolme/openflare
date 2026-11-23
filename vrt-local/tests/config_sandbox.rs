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

fn bundler_ready() -> bool {
    if let Ok(p) = std::env::var("OF_BUNDLER") {
        if Path::new(&p).is_file() {
            return true;
        }
    }
    ["debug", "release"].iter().any(|profile| {
        repo_root()
            .join("of-bundler/target")
            .join(profile)
            .join("of-bundler")
            .is_file()
    })
}

fn require_workerd(test: &str) -> bool {
    if workerd().is_none() {
        eprintln!("SKIP {test}: VRT_WORKERD_BIN unset or not a file");
        return false;
    }
    if !bundler_ready() {
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
