//! `vrt dev` end-to-end with the stub runtime — real processes:
//!
//!   dev::start → supervise vrt-host + vrt-provider + vrt-sql →
//!   catch-all ingress → stub workerd → file watch → redeploy →
//!   graceful shutdown (no orphan listeners).
//!
//! Requires the workspace binaries under `target/debug/` (vrt-host
//! built with dev-stub — `cargo test --workspace` provides it).
//! Skips cleanly when binaries or unix-socket bind are unavailable.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Sibling binaries of `target/debug/vrt` — `None` → skip.
fn bin(name: &str) -> Option<PathBuf> {
    let exe = Path::new(env!("CARGO_BIN_EXE_vrt"));
    let p = exe.parent()?.join(name);
    p.is_file().then_some(p)
}

fn require_stack() -> Option<(PathBuf, PathBuf, PathBuf, PathBuf)> {
    let host = bin("vrt-host")?;
    let provider = bin("vrt-provider")?;
    let sql = bin("vrt-sql")?;
    let stub = bin("vrt-stub-workerd")?;
    Some((host, provider, sql, stub))
}

fn ephemeral() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

/// Blocking GET with a fixed Host header → (status, body).
fn http_get(addr: &str, host: &str, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(&addr).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let req = format!("GET {path} HTTP/1.0\r\nhost: {host}\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).ok()?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Some((status, body.to_string()))
}

fn state_revision(root: &Path) -> u64 {
    let raw = std::fs::read(root.join("state/state.json")).unwrap_or_default();
    serde_json::from_slice::<serde_json::Value>(&raw)
        .ok()
        .and_then(|v| v["revision"].as_u64())
        .unwrap_or(0)
}

fn worker_fixture(dir: &Path, version: u8) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"dev-worker\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n\n[vars]\nMARKER = \"v{version}\"\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        format!("export default {{ fetch() {{ return new Response('v{version}') }} }}\n"),
    )
    .unwrap();
    std::fs::write(dir.join(".dev.vars"), "DEV_VAR=from-dev-vars\n").unwrap();
}

/// Unix-domain sockets are a hard requirement (mgmt socket + provider
/// listeners); deny → skip like the existing VRT_UNIX_TESTS gate.
fn unix_bind_ok(dir: &Path) -> bool {
    let p = dir.join("probe.sock");
    match std::os::unix::net::UnixListener::bind(&p) {
        Ok(_) => {
            let _ = std::fs::remove_file(&p);
            true
        }
        Err(_) => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_start_request_redeploy_cleanup() {
    let Some((host, provider, sql, stub)) = require_stack() else {
        eprintln!("SKIP: workspace binaries not built (cargo test --workspace)");
        return;
    };
    let base = match vrt_securefs::ShortRoot::new("vrtdev") {
        Ok(d) => d,
        Err(e) => {
            eprintln!("SKIP: short root not writable: {e}");
            return;
        }
    };
    if !unix_bind_ok(base.path()) {
        eprintln!("SKIP: unix bind() denied in this sandbox");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(base.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let worker_dir = base.path().join("proj");
    worker_fixture(&worker_dir, 1);

    // resolve_workerd(--stub) checks VRT_STUB_WORKERD first — the
    // test binary's exe dir is target/debug/deps, not target/debug.
    std::env::set_var("VRT_STUB_WORKERD", &stub);
    let listen = ephemeral();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let session = vrt_local::dev::start(vrt_local::dev::DevOptions {
        worker_dir: worker_dir.clone(),
        env_name: None,
        listen: Some(listen.clone()),
        no_watch: false,
        workerd: None,
        stub: true,
        allow_unpinned_workerd: false,
        vrt_host_bin: Some(host),
        vrt_provider_bin: Some(provider),
        vrt_sql_bin: Some(sql),
        facade_dist: Some(repo_facade()),
        stop: Some(stop_rx),
        print_logs: false,
        no_color: true,
    })
    .await
    .unwrap_or_else(|e| {
        let _ = stop_tx.send(true);
        panic!("dev start failed: {e}");
    });
    // env override for stub resolution
    assert!(session.summary.url.starts_with("http://127.0.0.1:"));
    assert_eq!(session.summary.worker_name, "dev-worker");
    assert!(session
        .summary
        .bindings
        .iter()
        .any(|l| l.contains("MARKER")));
    assert_eq!(session.summary.runtime, "vrt-stub-workerd");

    // Catch-all: ANY Host (incl. localhost) routes to the worker.
    let addr = session
        .summary
        .url
        .trim_start_matches("http://")
        .to_string();
    let (st, body) = http_get(&addr, "localhost", "/").expect("ingress request");
    assert_eq!(st, 200, "catch-all request must route: {body}");
    assert!(body.contains("stub ok"), "stub runtime serving: {body}");
    let (st, _) = http_get(&addr, "anything.example.com", "/").expect("ingress request");
    assert_eq!(st, 200, "arbitrary host routes under devCatchAll");

    // .dev.vars merged into vars.json (var ref + value materialized).
    let vars: serde_json::Value = serde_json::from_slice(
        &std::fs::read(base.path().join("proj/.vrt/vars/vars.json")).unwrap(),
    )
    .unwrap();
    assert!(
        vars.as_object()
            .unwrap()
            .iter()
            .any(|(k, v)| k.ends_with(":DEV_VAR") && *v == "from-dev-vars"),
        ".dev.vars must land in vars.json: {vars}"
    );

    // File change → watch → redeploy → revision bump.
    let rev_before = state_revision(&base.path().join("proj/.vrt"));
    worker_fixture(&worker_dir, 2);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if state_revision(&base.path().join("proj/.vrt")) > rev_before {
            break;
        }
        assert!(Instant::now() < deadline, "watch redeploy never fired");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Graceful shutdown: session ends, all listeners close.
    session.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(30), session.wait())
        .await
        .expect("session wait timed out");
    assert!(res.is_ok(), "clean stop: {res:?}");
    assert!(
        TcpStream::connect(&addr).is_err(),
        "ingress must be closed after shutdown"
    );
    assert!(
        TcpStream::connect(session_control_addr(&base.path().join("proj/.vrt")).as_str()).is_err(),
        "control listener must be closed after shutdown"
    );
}

/// Parse controlListen out of the generated dev host config.
fn session_control_addr(root: &Path) -> String {
    let text = std::fs::read_to_string(root.join("config/vrt-host.toml")).unwrap();
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("controlListen") {
            return rest
                .trim_start_matches([' ', '='])
                .split('#')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .to_string();
        }
    }
    panic!("no controlListen in dev config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_no_watch_never_redeploys() {
    let Some((host, provider, sql, _stub)) = require_stack() else {
        eprintln!("SKIP: workspace binaries not built");
        return;
    };
    let base = match vrt_securefs::ShortRoot::new("vrtdevnw") {
        Ok(d) => d,
        Err(e) => {
            eprintln!("SKIP: short root not writable: {e}");
            return;
        }
    };
    if !unix_bind_ok(base.path()) {
        eprintln!("SKIP: unix bind() denied in this sandbox");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(base.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let worker_dir = base.path().join("proj");
    worker_fixture(&worker_dir, 1);

    std::env::set_var("VRT_STUB_WORKERD", &_stub);
    let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let session = vrt_local::dev::start(vrt_local::dev::DevOptions {
        worker_dir: worker_dir.clone(),
        env_name: None,
        listen: Some(ephemeral()),
        no_watch: true,
        workerd: None,
        stub: true,
        allow_unpinned_workerd: false,
        vrt_host_bin: Some(host),
        vrt_provider_bin: Some(provider),
        vrt_sql_bin: Some(sql),
        facade_dist: Some(repo_facade()),
        stop: Some(stop_rx),
        print_logs: false,
        no_color: true,
    })
    .await
    .expect("dev start");
    let rev_before = state_revision(&base.path().join("proj/.vrt"));
    worker_fixture(&worker_dir, 9);
    // Generous window — a (broken) watcher would fire well inside it.
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert_eq!(
        state_revision(&base.path().join("proj/.vrt")),
        rev_before,
        "--no-watch must not redeploy"
    );
    session.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(30), session.wait())
        .await
        .expect("session wait timed out");
    assert!(res.is_ok());
}

/// The repo's facade dir (`manifest.json` + `dist/`). The auto-discovery
/// walks up from the test exe, which breaks when `CARGO_TARGET_DIR`
/// points outside the repo, so tests pass it explicitly.
fn repo_facade() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

/// Real-workerd counterpart of the smoke above: the uploaded worker code is
/// actually executed, and a file update is picked up by the watcher and the
/// NEW response is served. Gated on `VRT_WORKERD_BIN` (the real-workerd CI
/// job); ignored in the regular suite.
#[ignore = "requires VRT_WORKERD_BIN and workspace binaries"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_real_workerd_runs_new_code_after_redeploy() {
    let workerd = match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => PathBuf::from(p),
        _ => {
            eprintln!("SKIP: VRT_WORKERD_BIN unset");
            return;
        }
    };
    if !workerd.is_file() {
        eprintln!("SKIP: VRT_WORKERD_BIN not a file: {}", workerd.display());
        return;
    }
    let Some((host, provider, sql, _stub)) = require_stack() else {
        eprintln!("SKIP: workspace binaries not built");
        return;
    };
    let base = match vrt_securefs::ShortRoot::new("vrtdevwk") {
        Ok(d) => d,
        Err(e) => {
            eprintln!("SKIP: short root not writable: {e}");
            return;
        }
    };
    if !unix_bind_ok(base.path()) {
        eprintln!("SKIP: unix bind() denied in this sandbox");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(base.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let worker_dir = base.path().join("proj");
    worker_fixture(&worker_dir, 1);

    let listen = ephemeral();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let session = vrt_local::dev::start(vrt_local::dev::DevOptions {
        worker_dir: worker_dir.clone(),
        env_name: None,
        listen: Some(listen.clone()),
        no_watch: false,
        workerd: Some(workerd),
        stub: false,
        allow_unpinned_workerd: true,
        vrt_host_bin: Some(host),
        vrt_provider_bin: Some(provider),
        vrt_sql_bin: Some(sql),
        facade_dist: Some(repo_facade()),
        stop: Some(stop_rx),
        print_logs: false,
        no_color: true,
    })
    .await
    .unwrap_or_else(|e| {
        let _ = stop_tx.send(true);
        panic!("dev start failed: {e}");
    });
    let addr = session
        .summary
        .url
        .trim_start_matches("http://")
        .to_string();

    async fn wait_body(addr: &str, expected: &str) -> bool {
        for _ in 0..400 {
            if let Some((200, body)) = http_get(addr, "localhost", "/") {
                if body.contains(expected) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    assert!(
        wait_body(&addr, "v1").await,
        "uploaded v1 code must actually run on real workerd"
    );
    worker_fixture(&worker_dir, 2);
    assert!(
        wait_body(&addr, "v2").await,
        "after update the NEW code must be served"
    );

    session.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(30), session.wait())
        .await
        .expect("session wait timed out");
    assert!(res.is_ok(), "clean stop: {res:?}");
}
