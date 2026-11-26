//! #65 M7 regression: the metrics listener must answer while vrt-host is
//! still waiting for its first desired snapshot.
//!
//! The host deliberately blocks (bounded, 10 min) on the missing snapshot
//! before binding ingress/control.  If the metrics listener were in that
//! post-snapshot block too, the process would be a black box during the
//! wait: indistinguishable from a crash without reading /proc.  Binding it
//! first makes the startup race observable.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

/// A minimal HTTP/1.1 GET; `None` while the port is not accepting yet.
fn metrics(port: u16) -> Option<String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: probe\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    Some(buf)
}

#[test]
fn metrics_answers_before_the_first_desired_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let work = root.join("work");
    let cache = root.join("cache");
    let params = root.join("params.json");
    std::fs::create_dir_all(&work).expect("work dir");
    std::fs::create_dir_all(&cache).expect("cache dir");
    // The host refuses world-readable state roots (secure dir check).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for d in [&work, &cache] {
            std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700))
                .expect("secure dir mode");
        }
    }
    std::fs::write(&params, b"{}").expect("params");
    let workerd = root.join("workerd");
    std::fs::write(&workerd, b"#!/bin/sh\nexit 0\n").expect("workerd stub");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&workerd, std::fs::Permissions::from_mode(0o755))
            .expect("workerd mode");
    }
    let desired = root.join("missing-envelope.json");
    assert!(!desired.exists());
    let port = free_port();
    // Public key is the fixed example SPKI from examples/platform-vrt-host.toml;
    // no envelope is ever verified in this test.
    let cfg = format!(
        r#"cellId = "cell-metrics-probe"
workerdBinary = "{workerd}"
workDir = "{work}"
cacheDir = "{cache}"
listenAddr = "127.0.0.1:0"
controlListen = "127.0.0.1:0"
metricsListen = "127.0.0.1:{port}"

[desired]
path = "{desired}"
trustedKeys = [
  {{ keyId = "bf29c9743adb0a97f43ae4bc", publicKey = "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=" }},
]

[render]
mode = "capnp"
paramsFile = "{params}"
"#,
        workerd = workerd.display(),
        work = work.display(),
        cache = cache.display(),
        port = port,
        desired = desired.display(),
        params = params.display(),
    );
    let cfg_path = root.join("vrt-host.toml");
    std::fs::write(&cfg_path, cfg).expect("config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_vrt-host"))
        .arg("--config")
        .arg(&cfg_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn vrt-host");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last = None;
    loop {
        if let Some(resp) = metrics(port) {
            if resp.starts_with("HTTP/1.1 200") || resp.starts_with("HTTP/1.0 200") {
                assert!(
                    resp.contains("vrt_host_build_info"),
                    "metrics body lacks build info: {resp}"
                );
                assert!(
                    resp.contains("cell-metrics-probe"),
                    "metrics body lacks the early cell identity label: {resp}"
                );
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
            last = Some(resp);
        }
        if Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let out = child.wait_with_output().ok();
    panic!(
        "metrics never answered while the snapshot was absent (desired={}, last={last:?}, stderr={})",
        desired.display(),
        out.map(|o| String::from_utf8_lossy(&o.stderr).into_owned())
            .unwrap_or_default(),
    );
}
