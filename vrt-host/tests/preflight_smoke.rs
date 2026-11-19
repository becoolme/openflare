//! W1-r3 P0-0/P0: the config-load check must behave as if the config
//! never ran — no `test()` handler, no live DO `localDisk`, no live
//! external address. Gated on `VRT_WORKERD_BIN` (the vendored
//! 1.20260916.1 pairing); skipped without it.
//!
//! Root cause these tests pin: a bare `workerd test <cfg>` opens
//! `durableObjectStorage` localDisk at load (creating
//! `<uniqueKey>/metadata.sqlite` in the LIVE generation's dirs —
//! which made the P3c-2 restore see a non-empty class dir and skip
//! the restore → counter `n:0`), AND runs every `test()` handler as
//! a second unsandboxed-by-fencing process beside the serving
//! generation.

use std::path::{Path, PathBuf};

fn workerd() -> Option<PathBuf> {
    std::env::var("VRT_WORKERD_BIN")
        .ok()
        .filter(|p| Path::new(p).exists())
        .map(PathBuf::from)
}

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("vrt-preflight-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Worker module: `test()` throws — if the handler ever runs, the
/// check MUST fail (proving handlers execute); under the impossible
/// filter it can only pass (proving they never run).
const MAIN_JS: &str = "import { DurableObject } from \"cloudflare:workers\";\nexport class Counter extends DurableObject {\n  async fetch() { return new Response(\"ok\"); }\n}\nexport default {\n  async fetch() { return new Response(\"ok\"); },\n  async test() { throw new Error(\"tenant test handler must never run\"); },\n};\n";

fn write_config(dir: &Path, do_store: &Path, sockets_loopback: bool) -> PathBuf {
    std::fs::write(dir.join("main.js"), MAIN_JS).unwrap();
    let sock = if sockets_loopback {
        "127.0.0.1:0"
    } else {
        "unix:sock/w1.sock"
    };
    let text = format!(
        "using Workerd = import \"/workerd/workerd.capnp\";\n\
         const config :Workerd.Config = (\n\
         \x20 services = [\n\
         \x20   ( name = \"w1\", worker = (\n\
         \x20       compatibilityDate = \"2024-09-25\",\n\
         \x20       compatibilityFlags = [\"experimental\"],\n\
         \x20       modules = [\n\
         \x20         (name = \"main.js\", esModule = embed \"main.js\"),\n\
         \x20       ],\n\
         \x20       durableObjectNamespaces = [\n\
         \x20         (className = \"Counter\", uniqueKey = \"ukey-preflight\", \
         enableSql = true, preventEviction = true),\n\
         \x20       ],\n\
         \x20       durableObjectStorage = (localDisk = \"ds\"),\n\
         \x20   ) ),\n\
         \x20   ( name = \"ds\", disk = (path = {:?}, writable = true) ),\n\
         \x20   ( name = \"binding-backend\", external = (http = ()) ),\n\
         \x20 ],\n\
         \x20 sockets = [\n\
         \x20   ( name = \"http\", address = {:?}, http = (), service = \"w1\" ),\n\
         \x20 ],\n\
         );\n",
        do_store.display().to_string(),
        sock,
    );
    let cfg = dir.join("config.capnp");
    std::fs::write(&cfg, text).unwrap();
    cfg
}

fn sqlite_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if dir.is_dir() {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                v.extend(p.read_dir().unwrap().map(|f| f.unwrap().path()));
            }
        }
    }
    v
}

/// The PRE-ISOLATION behavior, kept as the documented mechanism of
/// the P3c-2 regression: a bare `workerd test` on the live config
/// runs `test()` handlers AND writes `metadata.sqlite` into the
/// serving generation's `localDisk` dirs.
#[test]
fn unisolated_check_runs_tests_and_touches_live_storage() {
    let Some(bin) = workerd() else {
        eprintln!("VRT_WORKERD_BIN unset — skipping");
        return;
    };
    let d = dir("raw");
    let live = d.join("do-store");
    std::fs::create_dir_all(&live).unwrap();
    let cfg = write_config(&d, &live, true);
    // No filter, live disk path — exactly what r2 did.
    let out = std::process::Command::new(&bin)
        .arg("test")
        .arg(&cfg)
        .arg("--experimental")
        .arg("--external-addr=binding-backend=127.0.0.1:1")
        .current_dir(&d)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("tenant test handler must never run") || stderr.contains("FAIL"),
        "test() handler must have RUN (pre-isolation): {stderr}"
    );
    assert!(
        !sqlite_files(&live).is_empty(),
        "live localDisk must have been written (pre-isolation)"
    );
}

/// The fixed contract: impossible filter + isolated twin + refuse
/// external addrs — the SAME config loads (verdict preserved) while
/// nothing runs and nothing is touched.
#[test]
fn isolated_check_loads_everything_touches_nothing() {
    let Some(bin) = workerd() else {
        eprintln!("VRT_WORKERD_BIN unset — skipping");
        return;
    };
    let d = dir("iso");
    let live = d.join("do-store");
    std::fs::create_dir_all(&live).unwrap();
    let cfg = write_config(&d, &live, true);
    let text = std::fs::read_to_string(&cfg).unwrap();
    let scratch = d.join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let twin = vrt_config::check::isolated_variant(&text, &scratch).unwrap();
    assert!(
        !twin.contains(live.to_str().unwrap()),
        "live path leaked: {twin}"
    );
    let twin_path = d.join("config.twin.capnp");
    std::fs::write(&twin_path, twin).unwrap();
    let ext = vec![vrt_config::ExternalAddr {
        service: "binding-backend".into(),
        // A real-looking unreachable addr — must be overridden anyway.
        address: "https://10.255.255.1:8443".into(),
    }];
    vrt_config::check::check_workerd_loads(
        &bin,
        &twin_path,
        &ext,
        &["--experimental".to_string()],
        &[],
    )
    .expect(
        "isolated check on a loadable config must pass — test() never ran, or it would have thrown",
    );
    assert!(
        sqlite_files(&live).is_empty(),
        "live DO storage must be untouched — found {:?}",
        sqlite_files(&live)
    );
}

/// Load equivalence under isolation: a missing module must still
/// fail the check even though no test runs.
#[test]
fn isolated_check_still_fails_missing_module() {
    let Some(bin) = workerd() else {
        eprintln!("VRT_WORKERD_BIN unset — skipping");
        return;
    };
    let d = dir("bad");
    std::fs::write(
        d.join("main.js"),
        "import \"nonexistent:mod\";\nexport default {}\n",
    )
    .unwrap();
    std::fs::write(
        d.join("cfg.capnp"),
        "using Workerd = import \"/workerd/workerd.capnp\";\nconst config :Workerd.Config = (\n  services = [\n    ( name = \"w1\", worker = (\n        compatibilityDate = \"2024-09-25\",\n        modules = [ (name = \"main.js\", esModule = embed \"main.js\") ],\n    ) ),\n  ],\n  sockets = [ ( name = \"http\", address = \"127.0.0.1:0\", http = (), service = \"w1\" ) ],\n);\n",
    )
    .unwrap();
    let err = vrt_config::check::check_workerd_loads(&bin, &d.join("cfg.capnp"), &[], &[], &[])
        .expect_err("missing module must still reject under the filter");
    assert!(err.contains("nonexistent:mod"), "{err}");
}
