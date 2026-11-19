//! Unix-domain worker-endpoint integration tests — the production
//! address family. The sandbox denies unix `bind()`, so every test is
//! `#[ignore]`d and additionally gated on `VRT_HOST_UNIX_TESTS=1`
//! (missing → print and return, never fail):
//!
//! ```sh
//! VRT_HOST_UNIX_TESTS=1 cargo test --workspace --offline -- --ignored --test-threads=1
//! ```
//!
//! On failure re-run with `--nocapture` (tracing is initialized at
//! `warn,vrt_host=info`; `RUST_LOG=vrt_host=debug` for detail).

mod common;

use common::*;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use vrt_host::ingress::IngressState;
use vrt_host::route::RouteTable;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{SupervisorCmd, TenantRegistry, TenantRuntime};

fn enabled() -> bool {
    if std::env::var("VRT_HOST_UNIX_TESTS").ok().as_deref() == Some("1") {
        true
    } else {
        eprintln!("skip: set VRT_HOST_UNIX_TESTS=1 (unix bind() unavailable in the sandbox)");
        false
    }
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn desired_for(host: &str) -> snapshot_protocol::compute_desired::ComputeDesired {
    snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t1", vec![worker("w1", host, "d1")])],
        deletions: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_HOST_UNIX_TESTS=1 and unix bind() allowed"]
async fn unix_ingress_end_to_end_and_permissions() {
    if !enabled() {
        return;
    }
    init_logs();
    let root = unix_root("ing");
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let spec = stub_spec_unix(&root, "t1", &["w1"], 1, &[]);
    let sock_path = match spec.worker_endpoints.get("w1").unwrap() {
        vrt_host::endpoint::WorkerEndpoint::Unix(p) => p.clone(),
        other => panic!("expected unix endpoint, got {other:?}"),
    };
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    // Filesystem contract: socket dir 0700, socket 0600, owned by euid.
    let meta = std::fs::metadata(&sock_path).expect("socket file exists");
    assert_eq!(meta.permissions().mode() & 0o777, 0o600, "socket mode");
    assert_eq!(meta.uid(), unsafe { libc::geteuid() }, "socket owner");
    assert_eq!(mode_of(sock_path.parent().unwrap()), 0o700, "sock dir");
    assert_eq!(mode_of(&root.join("t1")), 0o700, "tenant dir");

    // Ingress proxies over the unix socket (streamed body both ways).
    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024 * 1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_for("app.test")));
    let ig = start_ingress(&state).await;
    let (status, _, body) = http_get(ig.addr, "app.test", "/", &[]).await;
    assert_eq!(status, 200, "status/body: {status} {body}");
    assert!(body.contains("stub ok"), "body: {body}");
    ig.stop().await;

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
    // Everything under the tenant seg dir is reaped at stop — no stray
    // socket files remain.
    assert!(!sock_path.exists(), "socket file left behind");
    assert!(!root.join("t1").exists(), "tenant dir left behind");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_HOST_UNIX_TESTS=1 and unix bind() allowed"]
async fn unix_version_switch_replaces_socket() {
    if !enabled() {
        return;
    }
    init_logs();
    let root = unix_root("sw");
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());

    let spec1 = stub_spec_unix(&root, "t1", &["w1"], 1, &[]);
    let old_sock = match spec1.worker_endpoints.get("w1").unwrap() {
        vrt_host::endpoint::WorkerEndpoint::Unix(p) => p.clone(),
        _ => panic!("unix"),
    };
    let handle = spawn(&rt, spec1);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    assert!(old_sock.exists());

    // Switch to generation 2: different gen dir → different socket path.
    let spec2 = stub_spec_unix(&root, "t1", &["w1"], 2, &[]);
    let new_sock = match spec2.worker_endpoints.get("w1").unwrap() {
        vrt_host::endpoint::WorkerEndpoint::Unix(p) => p.clone(),
        _ => panic!("unix"),
    };
    handle.send(SupervisorCmd::Apply(Box::new(spec2))).await;
    // Wait until the new socket is live AND the old generation's dir
    // (holding the old socket) has been drained and removed.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if new_sock.exists() && !old_sock.exists() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "old={} new={}",
            old_sock.exists(),
            new_sock.exists()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024 * 1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_for("app.test")));
    let ig = start_ingress(&state).await;
    let (status, _, body) = http_get(ig.addr, "app.test", "/", &[]).await;
    assert_eq!(
        status, 200,
        "switched generation must serve (status/body: {status} {body})"
    );
    ig.stop().await;

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
    assert!(!new_sock.exists());
    assert!(!root.join("t1").exists());
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires VRT_HOST_UNIX_TESTS=1 and unix bind() allowed"]
async fn unix_killed_child_leaves_no_socket() {
    if !enabled() {
        return;
    }
    init_logs();
    let root = unix_root("kill");
    let rt = TenantRuntime::new("t1");
    // SIGTERM-ignoring stub → drain deadline → SIGKILL: the socket file
    // inside the generation dir must still be reaped.
    let spec = stub_spec_unix(&root, "t1", &["w1"], 1, &[("VRT_STUB_IGNORE_SIGTERM", "1")]);
    let sock = match spec.worker_endpoints.get("w1").unwrap() {
        vrt_host::endpoint::WorkerEndpoint::Unix(p) => p.clone(),
        _ => panic!("unix"),
    };
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    assert!(sock.exists());

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
    assert!(!sock.exists(), "killed generation's socket left behind");
    let _ = std::fs::remove_dir_all(&root);
}
