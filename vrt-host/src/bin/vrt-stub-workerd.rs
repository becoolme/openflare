//! `vrt-stub-workerd` — test/development stand-in for the workerd child
//! (built only with `--features dev-stub`; NEVER a production binary).
//! process. **Not for production.** It satisfies the `vrt-host` launch
//! contract: `serve <config.json> --control-fd=3`, binds every declared
//! socket, reports JSON `listen` events on the control fd, and serves a
//! minimal HTTP/1.1 response per connection.
//!
//! Config (written by `vrt-host` stub assemble mode, or by hand in
//! tests):
//! `{"sockets": {"sock-<workerId>": "tcp:127.0.0.1:0" | "unix:/abs/path"}}`
//!
//! Behaviour is controlled via environment (the launcher injects
//! `spec.base_env`):
//!
//! - `VRT_STUB_MODE` — `ready` (default): bind + report + serve.
//!   `crash`: exit(3) immediately. `never`: sleep forever without
//!   binding or reporting. `die:<ms>`: become ready, then exit after
//!   the delay (crash-restart tests).
//! - `VRT_STUB_READY_DELAY_MS` — delay between binding and reporting
//!   (readiness-timeout tests).
//! - `VRT_STUB_SLOW_MS` — delay before every HTTP response (in-flight
//!   drain tests).
//! - `VRT_STUB_IGNORE_SIGTERM=1` — SIG_IGN SIGTERM (forced-kill tests).
//! - `VRT_STUB_BAD_CONTROL=1` — emit garbage on the control fd
//!   (malformed-control tests).
//! - `VRT_STUB_OVERSIZE_CONTROL=1` — emit an oversized line.
//! - `VRT_STUB_DUP_EVENT=1` — emit a duplicate listen event.
//! - `VRT_STUB_EXTRA_SOCKET=1` — emit an unexpected socket event.
//! - `VRT_STUB_ONLY_FIRST=1` — report only the first socket's listen
//!   event (partial multi-worker readiness → timeout).
//! - `VRT_STUB_FAIL_BIND=1` — exit(4) instead of binding.
//! - `VRT_STUB_ADDR=1` — legacy format: include `address` on unix
//!   listen events too (compat coverage). Default matches the real
//!   workerd 1.20261103.1 line measured on B8: unix sockets emit
//!   `{"event":"listen","socket":"<name>","port":0}` with NO address;
//!   tcp sockets keep `address` (the ephemeral port must be learned).
//! - `VRT_STUB_STDERR` — print the value to stderr at startup
//!   (stderr-tail / switch-failed diagnostics tests).
//!
//! HTTP behaviour: `connection: close`, body echoes `x-*` request
//! headers as `hdr <name>=<value>` lines (header-scrubbing tests) plus
//! `x-stub-env-<name>: present` response headers for names listed in
//! the `x-stub-env-check` request header (presence only — values are
//! never emitted). `x-stub-hang-ms: <n>` delays just that response.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::FromRawFd;
use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path: Option<String> = None;
    let mut control_fd: i32 = 3;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--version" => {
                // Identity marker — `of init` uses this to detect the
                // stub runtime and refuse stub/real-mode mismatches.
                println!("vrt-stub-workerd {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "serve" => {
                config_path = args.get(i + 1).cloned();
                i += 2;
            }
            // `workerd test <config> <filter>` — the preflight hook.
            // A config carrying `"testFail": "<text>"` prints <text>
            // on stderr and exits 1 (config-load failure, e.g. a
            // `service worker-<id>:` attribution); any other config
            // loads cleanly and reports `No tests found!`.
            "test" => {
                let cfg = args.get(i + 1).cloned().unwrap_or_default();
                let raw = std::fs::read_to_string(&cfg).unwrap_or_default();
                let parsed: serde_json::Value =
                    serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
                if let Some(fail) = parsed.get("testFail").and_then(|v| v.as_str()) {
                    eprintln!("{fail}");
                    eprintln!("Tests failed!");
                    std::process::exit(1);
                }
                println!("No tests found!");
                std::process::exit(0);
            }
            a if a.starts_with("--control-fd=") => {
                control_fd = a.trim_start_matches("--control-fd=").parse().unwrap_or(3);
                i += 1;
            }
            _ => i += 1,
        }
    }

    let mode = std::env::var("VRT_STUB_MODE").unwrap_or_else(|_| "ready".to_string());
    // MIG-7: optional stderr line at startup — switch-failure tests
    // assert the sanitized tail reaches `last_error`/logs.
    if let Ok(line) = std::env::var("VRT_STUB_STDERR") {
        // Fixed prefix survives LogTail's env-value needle redaction
        // so tests can prove the tail propagated while the env value
        // itself is `***` — the same shape a real secret takes.
        eprintln!("vrt-stub-stderr: {line}");
    }
    if mode == "crash" {
        std::process::exit(3);
    }
    if std::env::var("VRT_STUB_IGNORE_SIGTERM").as_deref() == Ok("1") {
// SAFETY: signal(2) sets a process-global disposition in the test stub before it services anything; SIG_IGN is a constant handler.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    if mode == "never" {
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }

    let config_path = config_path.unwrap_or_else(|| {
        eprintln!("vrt-stub-workerd: missing `serve <config>`");
        std::process::exit(2);
    });
    let raw = std::fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("vrt-stub-workerd: config read failed: {e}");
        std::process::exit(2);
    });
    let config: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|e| {
        eprintln!("vrt-stub-workerd: config parse failed: {e}");
        std::process::exit(2);
    });
    let sockets = config
        .get("sockets")
        .and_then(|s| s.as_object())
        .cloned()
        .unwrap_or_default();

    if std::env::var("VRT_STUB_FAIL_BIND").as_deref() == Ok("1") {
        std::process::exit(4);
    }

    // Bind everything first — listen events only fire once sockets are
    // actually accepting, like real workerd.
    enum Bound {
        Tcp(TcpListener),
        Unix(UnixListener),
    }
    let mut bound: Vec<(String, Bound, String, u16)> = Vec::new();
    for (name, addr) in &sockets {
        let Some(addr) = addr.as_str() else { continue };
        if let Some(tcp) = addr.strip_prefix("tcp:") {
            let listener = TcpListener::bind(tcp).unwrap_or_else(|e| {
                eprintln!("vrt-stub-workerd: bind {tcp}: {e}");
                std::process::exit(4);
            });
            let local = listener.local_addr().expect("local_addr");
            bound.push((
                name.clone(),
                Bound::Tcp(listener),
                format!("tcp:{local}"),
                local.port(),
            ));
        } else if let Some(path) = addr.strip_prefix("unix:") {
            let _ = std::fs::remove_file(path);
            // F1: same 0600-from-birth helper as the real bind sites —
            // the stub mirrors workerd's unix: sockets in tests.
            let listener = vrt_securefs::bind_unix_private(std::path::Path::new(path), 0o600)
                .unwrap_or_else(|e| {
                    eprintln!("vrt-stub-workerd: bind {path}: {e}");
                    std::process::exit(4);
                });
            bound.push((name.clone(), Bound::Unix(listener), addr.to_string(), 0));
        }
    }

    if let Ok(ms) = std::env::var("VRT_STUB_READY_DELAY_MS") {
        std::thread::sleep(Duration::from_millis(ms.parse().unwrap_or(0)));
    }

    // Control-fd listen events.
    {
        // SAFETY: the parent mapped its socketpair end onto this fd
        // before exec (see ProcessWorkerdLauncher pre_exec). We write a
        // bounded number of bounded lines and never read from it.
        let mut control = unsafe { std::fs::File::from_raw_fd(control_fd) };
        if std::env::var("VRT_STUB_BAD_CONTROL").as_deref() == Ok("1") {
            let _ = control.write_all(b"this-is-not-json\n");
        } else if std::env::var("VRT_STUB_OVERSIZE_CONTROL").as_deref() == Ok("1") {
            let line = format!("{{\"event\":\"listen\",\"socket\":\"{}\",\"port\":0,\"address\":\"tcp:127.0.0.1:1\"}}\n", "x".repeat(8192));
            let _ = control.write_all(line.as_bytes());
        } else {
            // Real workerd format (T25/M1): unix sockets report
            // `{"event":"listen","socket":"<name>","port":0}` with no
            // address; the host resolves the endpoint from its own
            // spec. `VRT_STUB_ADDR=1` restores the legacy addressed
            // form for compatibility coverage.
            let with_addr = std::env::var("VRT_STUB_ADDR").as_deref() == Ok("1");
            let emit = |control: &mut std::fs::File, name: &str, address: &str, port: u16| {
                let line = if with_addr || port != 0 {
                    format!(
                        "{{\"event\":\"listen\",\"socket\":\"{name}\",\"port\":{port},\"address\":\"{address}\"}}\n"
                    )
                } else {
                    format!("{{\"event\":\"listen\",\"socket\":\"{name}\",\"port\":{port}}}\n")
                };
                let _ = control.write_all(line.as_bytes());
            };
            // `VRT_STUB_ONLY_FIRST=1` — report only the first bound
            // socket: multi-worker specs then see a PARTIAL listen set
            // and must time out rather than mark ready.
            let only_first = std::env::var("VRT_STUB_ONLY_FIRST").as_deref() == Ok("1");
            for (i, (name, _, address, port)) in bound.iter().enumerate() {
                if only_first && i > 0 {
                    break;
                }
                emit(&mut control, name, address, *port);
            }
            if std::env::var("VRT_STUB_DUP_EVENT").as_deref() == Ok("1") {
                if let Some((name, _, address, port)) = bound.first() {
                    emit(&mut control, name, address, *port);
                }
            }
            if std::env::var("VRT_STUB_EXTRA_SOCKET").as_deref() == Ok("1") {
                emit(&mut control, "sock-ghost", "tcp:127.0.0.1:9", 0);
            }
        }
        let _ = control.flush();
        // Deliberately keep `control` open — workerd holds the control
        // channel for the process lifetime (host-side EOF semantics are
        // unverified; we leak the fd, the kernel closes it on exit).
        std::mem::forget(control);
    }

    if let Some(rest) = mode.strip_prefix("die:") {
        let ms: u64 = rest.parse().unwrap_or(0);
        std::thread::sleep(Duration::from_millis(ms));
        std::process::exit(7);
    }

    let slow_ms: u64 = std::env::var("VRT_STUB_SLOW_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    for (name, listener, _, _) in bound {
        std::thread::spawn(move || match listener {
            Bound::Tcp(l) => loop {
                match l.accept() {
                    Ok((stream, _)) => serve_conn(stream, &name, slow_ms),
                    Err(_) => break,
                }
            },
            Bound::Unix(l) => loop {
                match l.accept() {
                    Ok((stream, _)) => serve_conn(stream, &name, slow_ms),
                    Err(_) => break,
                }
            },
        });
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// T21: `VRT_STUB_RECORD_DIR` — when set, every request line +
/// `x-vrt-cron-idempotency-key` is appended to `<dir>/<socket>.log`
/// so cron/service-dispatch tests can assert deliveries (count +
/// identity) instead of inferring them from echoes.
fn record_request(socket: &str, line: &str) {
    let Ok(dir) = std::env::var("VRT_STUB_RECORD_DIR") else {
        return;
    };
    let path = std::path::Path::new(&dir).join(format!("{socket}.log"));
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = std::io::Write::write_all(&mut f, line.as_bytes());
    }
}

fn serve_conn<S>(mut stream: S, socket: &str, slow_ms: u64)
where
    S: Read + Write + SetReadTimeout,
{
    let seq = CONN_SEQ.fetch_add(1, Ordering::Relaxed);
    let _ = stream.set_read_timeout_if_tcp();
    let mut head = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !head.ends_with(b"\r\n\r\n") && head.len() <= 16 * 1024 {
        if std::time::Instant::now() > deadline {
            return;
        }
        match stream.read(&mut byte) {
            Ok(1) => head.push(byte[0]),
            _ => return,
        }
    }
    let head_text = String::from_utf8_lossy(&head).to_string();
    {
        let first = head_text.lines().next().unwrap_or("");
        let idem = head_text
            .lines()
            .find(|l| {
                l.to_ascii_lowercase()
                    .starts_with("x-vrt-cron-idempotency-key:")
            })
            .map(|l| {
                l.split_once(':')
                    .map(|(_, v)| v.trim().to_string())
                    .unwrap_or_default()
            })
            .unwrap_or_else(|| "-".to_string());
        // Queue wake: the host-built remaining delivery budget.
        let budget = head_text
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("x-vrt-delivery-budget-ms:"))
            .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().to_string()))
            .unwrap_or_else(|| "-".to_string());
        record_request(socket, &format!("{first} idem={idem} budget={budget}\n"));
    }
    let mut content_length = 0usize;
    let mut env_check = String::new();
    let mut hang_ms = 0u64;
    let mut emit_header = String::new();
    let mut emit_raw: Vec<String> = Vec::new();
    let mut status: u16 = 200;
    let mut echo_headers = Vec::new();
    for line in head_text.lines().skip(1) {
        if let Some((name, value)) = line.split_once(':') {
            let name_l = name.trim().to_ascii_lowercase();
            let value = value.trim();
            match name_l.as_str() {
                "content-length" => content_length = value.parse().unwrap_or(0),
                "x-stub-env-check" => env_check = value.to_string(),
                "x-stub-hang-ms" => hang_ms = value.parse().unwrap_or(0),
                "x-stub-emit-header" => emit_header = value.to_string(),
                "x-stub-emit-raw" => emit_raw.push(value.replace(['\r', '\n'], "")),
                "x-stub-status" => status = value.parse().unwrap_or(200),
                _ => {
                    // MIG-9: echo `cf-*` too — client-IP injection tests
                    // assert on the synthesized `cf-connecting-ip` and
                    // on stripped forged `cf-*` values alike.
                    if name_l.starts_with("x-") || name_l.starts_with("cf-") {
                        echo_headers.push(format!("hdr {name_l}={value}"));
                    }
                }
            }
        }
    }
    // Drain the request body (bounded).
    let mut remaining = content_length.min(8 * 1024 * 1024);
    let mut buf = [0u8; 8192];
    while remaining > 0 {
        let want = remaining.min(buf.len());
        match stream.read(&mut buf[..want]) {
            Ok(0) | Err(_) => break,
            Ok(n) => remaining -= n,
        }
    }

    let wait = hang_ms.max(slow_ms);
    if wait > 0 {
        std::thread::sleep(Duration::from_millis(wait));
    }

    // CR1: cron dispatch sockets answer the facade contract — the
    // host's trigger checks `delivered:true` + `outcome:"ok"` in the
    // body, not just status. The request is still recorded above
    // (`sock-cron-<id>.log`) so tests assert real dispatch identity.
    if socket.starts_with("sock-cron-") {
        let body = "{\"delivered\":true,\"outcome\":\"ok\",\"noRetry\":false}\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\ncontent-type: application/json\r\nx-stub-worker: {socket}\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
        return;
    }
    let mut body = format!("stub ok {socket} conn={seq}\n");
    for line in &echo_headers {
        body.push_str(line);
        body.push('\n');
    }
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        301 => "Moved Permanently",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\nconnection: close\r\nx-stub-worker: {socket}\r\n",
        body.len()
    );
    for name in env_check.split(',') {
        let name = name.trim();
        if !name.is_empty() {
            let present = std::env::var_os(name).is_some();
            response.push_str(&format!(
                "x-stub-env-{}: {}\r\n",
                name.to_ascii_lowercase(),
                if present { "present" } else { "absent" }
            ));
        }
    }
    // `x-stub-emit-header: name=value` — emit `name: value` as a response
    // header (test hook for response-scrub checks; value sanitized).
    for pair in emit_header.split(',') {
        if let Some((name, value)) = pair.split_once('=') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().replace(['\r', '\n'], "");
            if !name.is_empty() {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
        }
    }
    for raw in &emit_raw {
        response.push_str(raw);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

trait SetReadTimeout {
    fn set_read_timeout_if_tcp(&self) -> std::io::Result<()>;
}

impl SetReadTimeout for TcpStream {
    fn set_read_timeout_if_tcp(&self) -> std::io::Result<()> {
        self.set_read_timeout(Some(Duration::from_secs(5)))
    }
}

impl SetReadTimeout for std::os::unix::net::UnixStream {
    fn set_read_timeout_if_tcp(&self) -> std::io::Result<()> {
        self.set_read_timeout(Some(Duration::from_secs(5)))
    }
}
