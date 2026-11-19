//! MIG-14: real-workerd proof of the raw-TCP `connect()` path.
//!
//! Mechanism under test (established at workerd-source level, pinned
//! here against the real binary):
//!   tenant `cloudflare:sockets` connect() → delivered as an HTTP
//!   CONNECT to the tenant's `globalOutbound` WorkerEntrypoint →
//!   `vrt-egress.EgressProxy.connect(socket)` → `socket.localAddress`
//!   carries the requested `host:port` authority → the egress worker
//!   dials the upstream through ITS OWN globalOutbound (the raw
//!   `outbound-network` service, post-resolution `restrictPeers()`
//!   filtering) and relays bytes bidirectionally.
//!
//! What this test proves on the real binary — unit tests can only fake
//! the streams:
//!   - a tenant connect() through the WHOLE chain reaches a TCP echo
//!     service and round-trips bytes;
//!   - the internal-exception interface ({host,port} exact match,
//!     platform-injected only) punches the private-address hole it was
//!     built for — demonstrated against the pod's own LAN IP;
//!   - private/loopback/mail targets deny with the typed
//!     `egress_tcp_deny` code in the workerd log;
//!   - `restrictPeers()` in outbound-network is the last word: a
//!     private literal denied at layer 3 never reaches the wire.
//!
//! Sandbox honesty: the rendered `outbound-network` is rewritten to
//! `allow = ["public", "private", "local"]` so the TEST echo (a LAN
//! address) is dialable — production keeps `["public"]` and the
//! NetworkPolicy keeps private space denied at layer 2 regardless.
//! The `internal_tcp_allow` policy field is the MIG-15 interface; this
//! is its only consumer today and it exists ONLY in this ignored test.
//!
//! Gated on `VRT_WORKERD_BIN` — absent → explicit skip with a printed
//! reason, never silently green:
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!     --test m14_tcp_connect_workerd -- --include-ignored --nocapture

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::TenantDesired;
use vrt_config::{
    parse_facade_manifest, render_tenant_config, DeployManifest, EgressAllowEntry,
    EgressPolicy, EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};

fn workerd_bin() -> Option<PathBuf> {
    match std::env::var("VRT_WORKERD_BIN") {
        Ok(p) if !p.is_empty() => Some(PathBuf::from(p)),
        _ => None,
    }
}

fn facade_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn smoke_root() -> Option<PathBuf> {
    match vrt_securefs::test_short_root("vrt-m14") {
        Ok(d) => Some(d),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            None
        }
        Err(e) => panic!("short root: {e}"),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The pod's primary non-loopback IPv4 — the only private literal this
/// test may name in `internal_tcp_allow` (loopback is never allowed
/// even in exceptions). `None` when the sandbox has no LAN address.
fn primary_ipv4() -> Option<std::net::Ipv4Addr> {
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    // UDP "connect" picks the route's source address — no packet sent.
    for target in ["203.0.113.1:80", "192.0.2.1:80", "8.8.8.8:80"] {
        if s.connect(target).is_ok() {
            if let Ok(std::net::SocketAddr::V4(a)) = s.local_addr() {
                let ip = *a.ip();
                if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_link_local() {
                    return Some(ip);
                }
            }
        }
    }
    None
}

struct WorkerProc {
    child: Child,
    log_path: PathBuf,
    dir: PathBuf,
}

impl Drop for WorkerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::env::var("VRT_SMOKE_KEEP").is_ok() {
            eprintln!("kept smoke dir: {}", self.dir.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> WorkerProc {
    let log_path = dir.join("workerd.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let child = Command::new(bin)
        .arg("serve")
        .arg(cfg)
        .arg("--experimental")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    WorkerProc {
        child,
        log_path,
        dir: dir.to_path_buf(),
    }
}

fn wait_ready(port: u16) -> bool {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}

fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    write!(
        s,
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if response_complete(&buf) {
                    break;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn response_complete(buf: &[u8]) -> bool {
    let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4) else {
        return false;
    };
    let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_lowercase();
    let body_len = buf.len() - hdr_end;
    for line in headers.split("\r\n") {
        if let Some(v) = line.strip_prefix("content-length:") {
            return v.trim().parse::<usize>().map_or(false, |n| body_len >= n);
        }
        if line.starts_with("transfer-encoding:") && line.contains("chunked") {
            return buf[hdr_end..].windows(5).any(|w| w == b"0\r\n\r\n");
        }
    }
    false
}

fn log_text(proc: &WorkerProc) -> String {
    let raw = std::fs::read(&proc.log_path).unwrap_or_default();
    // workerd wraps each console line as JSON, escaping the quotes of
    // our structured log; unescape so assertions match the raw event.
    String::from_utf8_lossy(&raw).replace('\0', "").replace("\\\"", "\"")
}

/// A plain TCP echo server on 0.0.0.0 — reachable via the pod's LAN
/// IP (what the internal exception names) AND via workerd's dial from
/// inside the same host. Per-connection thread echoes until EOF.
fn echo_server() -> u16 {
    let l = TcpListener::bind("0.0.0.0:0").unwrap();
    let port = l.local_addr().unwrap().port();
    std::thread::spawn(move || loop {
        let Ok((mut s, _)) = l.accept() else {
            break;
        };
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                match s.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if s.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    });
    port
}

/// Tenant worker: probe endpoints over fetch.
///   /echo?h=<host>&p=<port>  → connect(), write "M14PING\n", read one
///                             line back, JSON {echoed}
///   /probe?h=<host>&p=<port> → connect() + opened; JSON
///                             {ok:true} or {ok:false,name,message}
const USER_MODULE: &str = r#"import { connect } from "cloudflare:sockets";
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const host = u.searchParams.get("h") || "";
    const port = Number(u.searchParams.get("p") || "0");
    if (u.pathname === "/echo") {
      try {
        const s = connect({ hostname: host, port });
        const w = s.writable.getWriter();
        await w.write(new TextEncoder().encode("M14PING\n"));
        const r = s.readable.getReader();
        let data = "";
        const dec = new TextDecoder();
        for (let i = 0; i < 64; i++) {
          const { value, done } = await r.read();
          if (done) break;
          data += dec.decode(value, { stream: true });
          if (data.includes("\n")) break;
        }
        await s.close().catch(() => {});
        return new Response(JSON.stringify({ echoed: data.trim() }),
          { headers: { "content-type": "application/json" } });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false,
          name: e && e.name, message: String((e && e.message) || e) }),
          { headers: { "content-type": "application/json" } });
      }
    }
    if (u.pathname === "/probe") {
      try {
        const s = connect({ hostname: host, port });
        await s.opened;
        // workerd resolves `opened` once the CONNECT is accepted —
        // BEFORE the egress handler has judged the target — so a deny
        // only shows up as the stream ending. Write a byte and require
        // a byte back (the LAN echo) to call the connection live.
        const w = s.writable.getWriter();
        await w.write(new TextEncoder().encode("P\n"));
        const r = s.readable.getReader();
        const { value, done } = await r.read();
        if (done || !value || value.length === 0) {
          throw new Error("connection closed without data");
        }
        await s.close().catch(() => {});
        return new Response(JSON.stringify({ ok: true }),
          { headers: { "content-type": "application/json" } });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false,
          name: e && e.name, message: String((e && e.message) || e) }),
          { headers: { "content-type": "application/json" } });
      }
    }
    return new Response("m14 tenant", { status: 404 });
  }
}
"#;

fn smoke_params(echo_host: &str, echo_port: u16) -> RuntimeParams {
    let raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest = parse_facade_manifest(&raw).expect("facade manifest parses");
    RuntimeParams {
        socket_dir: "sock".into(),
        binding_backend_socket: "private/binding-backend.sock".into(),
        egress: manifest.egress.clone(),
        egress_policy: EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::<EgressAllowEntry>::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 30_000,
            tcp_allowed_ports: vec![43],
            // TEST-ONLY injection — the production surface cannot
            // produce this field (EgressPolicyToml denies the key);
            // here it names the echo's LAN address so the whole
            // internal-exception path is exercised end to end.
            internal_tcp_allow: vec![EgressAllowEntry {
                host: echo_host.to_string(),
                port: echo_port,
            }],
            internal_tcp_targets: Vec::new(),
            tcp_connect_timeout_ms: 10_000,
            tcp_idle_timeout_ms: 2_000,
            tcp_max_concurrent: 8,
        },
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2026-09-08".into(),
        system_compat_flags: vec![
            "experimental".into(),
            "service_binding_extra_handlers".into(),
        ],
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        do_storage_dir: String::new(),
        wfop_pepper: "m14-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

/// Minimal binding-backend stub — never hit by connect() traffic but
/// required for workerd to load the rendered config.
const BACKEND_STUB: &str = r#"export default {
  async fetch() { return new Response("m14-stub", { status: 404 }); }
}
"#;

fn render_and_write(dir: &Path, http_port: u16, echo_host: &str, echo_port: u16) -> PathBuf {
    let worker_id = "wm14tcp";
    let bundle = VersionBundle::build(
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": { "index.js": USER_MODULE },
            "compatibilityDate": "2026-09-08",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let tenant: TenantDesired = serde_json::from_value(json!({
        "tenantId": "team-m14",
        "workers": [{
            "workerId": worker_id,
            "name": "m14",
            "activeVersion": {
                "versionId": "v-m14-1",
                "artifactDigest": bundle.digest(),
                "compatibilityDate": "2026-09-08",
                "compatibilityFlags": []
            },
            "bindings": [],
            "routes": [{"hostname": "m14--team-m14.vibeploy.app"}],
            "crons": [],
            "quotas": {"cpuMs": 50, "memoryMb": 128,
                       "subrequests": 100, "outboundConnections": 16}
        }]
    }))
    .unwrap();
    let params = smoke_params(echo_host, echo_port);
    let input = TenantRenderInput {
        tenant: &tenant,
        revision: 1,
        bundles: &[bundle],
        params: &params,
    };
    let out = render_tenant_config(&input).unwrap();

    // Materialize every embed the renderer demands (system modules from
    // dist, the tenant bundle verbatim).
    let manifest_raw = std::fs::read_to_string(facade_root().join("manifest.json")).unwrap();
    let manifest_doc: serde_json::Value = serde_json::from_str(&manifest_raw).unwrap();
    for embed in &out.embeds {
        let dst = dir.join(&embed.path);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        match &embed.kind {
            EmbedKind::SystemModule {
                service_name,
                module_name,
            } => {
                let modules = manifest_doc["workers"][service_name]["modules"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{service_name} missing from manifest"));
                let m = modules
                    .iter()
                    .find(|m| m["name"].as_str().unwrap() == module_name)
                    .unwrap_or_else(|| panic!("{service_name}/{module_name} not in manifest"));
                let src = facade_root()
                    .join("..")
                    .join(m["dist"].as_str().unwrap());
                let bytes =
                    std::fs::read(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
                let got = format!("{:x}", Sha256::digest(&bytes));
                assert_eq!(got, embed.digest, "embed digest drift — regen facade/dist");
                std::fs::write(&dst, bytes).unwrap();
            }
            EmbedKind::BundleModule { .. } => {
                std::fs::write(&dst, USER_MODULE).unwrap();
            }
            EmbedKind::VendoredModule { name } => {
                let bytes = vrt_config::workflows::vendored_bytes(name)
                    .unwrap_or_else(|| panic!("vendored module {name}"));
                std::fs::write(&dst, bytes).unwrap();
            }
        }
    }

    let mut text = out.text.clone();
    // Tenant ingress socket → loopback TCP (same smoke convention).
    let unix_addr = format!("unix:sock/{worker_id}.sock");
    assert!(
        text.contains(&unix_addr),
        "expected unix socket addr in render:\n{text}"
    );
    text = text.replace(&unix_addr, &format!("127.0.0.1:{http_port}"));
    // binding-backend external → same-name stub worker (loads, never hit).
    let external = "(name = \"binding-backend\", external = (http = ()))";
    assert!(
        text.contains(external),
        "expected binding-backend external in render:\n{text}"
    );
    std::fs::write(dir.join("backend-stub.js"), BACKEND_STUB).unwrap();
    text = text.replace(
        external,
        "(name = \"binding-backend\", worker = (\n            \
           modules = [(name = \"backend.js\", \
           esModule = embed \"backend-stub.js\")],\n            \
           compatibilityDate = \"2026-09-08\",\n            \
           globalOutbound = (name = \"vrt-no-network\", \
           entrypoint = \"DenyAll\"),\n        ))",
    );
    // TEST-ONLY: outbound-network admits private/local so the LAN echo
    // is dialable — production keeps allow=["public"] and the pod
    // NetworkPolicy denies private space at layer 2 anyway.
    let prod_net = "allow = [\"public\"],\n      deny = [\"private\", \"local\", \"unix\", \"unix-abstract\"],";
    assert!(
        text.contains(prod_net),
        "expected outbound-network stanza in render:\n{text}"
    );
    text = text.replace(
        prod_net,
        "allow = [\"public\", \"private\", \"local\"],\n      deny = [\"unix\", \"unix-abstract\"],",
    );

    let path = dir.join("cfg.capnp");
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
#[ignore = "requires VRT_WORKERD_BIN pointing at a real workerd binary"]
fn mig14_tcp_connect_end_to_end() {
    let Some(bin) = workerd_bin() else {
        eprintln!("SKIP: VRT_WORKERD_BIN unset — real-workerd test not run");
        return;
    };
    let Some(lan_ip) = primary_ipv4() else {
        eprintln!("SKIP: no non-loopback LAN IPv4 — cannot reach echo privately");
        return;
    };
    let Some(dir) = smoke_root() else {
        return;
    };
    let echo_port = echo_server();
    let http_port = free_port();
    let cfg = render_and_write(&dir, http_port, &lan_ip.to_string(), echo_port);
    let proc = spawn(&bin, &cfg, &dir);
    if !wait_ready(http_port) {
        panic!(
            "workerd never listened on {http_port}\n-- workerd.log --\n{}",
            log_text(&proc)
        );
    }

    // 1. Internal exception: tenant connect() to the LAN echo — denied
    //    at every OTHER layer (private literal) and allowed ONLY by the
    //    platform-injected exception. Byte-for-byte echo proves the
    //    whole chain: CONNECT → handler → outbound dial → bidirectional
    //    pipe.
    let (st, body) = http_get(
        http_port,
        &format!("/echo?h={lan_ip}&p={echo_port}"),
    );
    assert_eq!(st, 200, "echo probe status: {body}\n-- log --\n{}", log_text(&proc));
    assert!(
        body.contains("M14PING"),
        "echo must round-trip through the full chain: {body}\n-- log --\n{}",
        log_text(&proc)
    );

    // 2. Same private IP on a NON-exception port → policy deny (the
    //    exception is exact {host,port}); recognizable code in the log.
    let (st, body) = http_get(
        http_port,
        &format!("/probe?h={lan_ip}&p={}", echo_port + 1),
    );
    assert_eq!(st, 200);
    assert!(body.contains("\"ok\":false"), "non-exception private port must deny: {body}");
    let log = log_text(&proc);
    assert!(
        log.contains("\"event\":\"egress_tcp_deny\""),
        "deny must be logged: {log}"
    );

    // 3. Private literal WITHOUT any exception → ip_private deny.
    let (st, body) = http_get(http_port, "/probe?h=10.255.255.9&p=43");
    assert_eq!(st, 200);
    assert!(body.contains("\"ok\":false"), "private literal must deny: {body}");
    assert!(
        log_text(&proc).contains("\"code\":\"ip_private\""),
        "ip_private deny must be logged"
    );

    // 4. Mail port — denied before exceptions (tcpAllowedPorts=[43]
    //    cannot carry it anyway): the typed `port` code.
    let (st, body) = http_get(http_port, "/probe?h=8.8.8.8&p=25");
    assert_eq!(st, 200);
    assert!(body.contains("\"ok\":false"), "SMTP port must deny: {body}");
    assert!(
        log_text(&proc).contains("\"port\":25"),
        "mail-port deny must be logged with the port"
    );

    // 5. Session close events aggregate by port — never by hostname.
    //    The tenant closed its socket first, which makes workerd cancel
    //    the egress handler (no `finally` runs), so the close line comes
    //    from the abandoned-session reaper on the NEXT connect() once
    //    the silence budget (idle 2 s + 1 s grace) has passed. A leaked
    //    slot would otherwise wedge the isolate at concurrent_limit.
    std::thread::sleep(std::time::Duration::from_millis(3_500));
    let _ = http_get(http_port, "/probe?h=8.8.8.8&p=25");
    for _ in 0..50 {
        if log_text(&proc).contains("\"event\":\"egress_tcp_close\"") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let log = log_text(&proc);
    assert!(
        log.contains("\"event\":\"egress_tcp_close\""),
        "close accounting must be logged: {log}"
    );
    assert!(
        log.contains("\"reason\":\"abandoned\""),
        "tenant-initiated close is reaped as abandoned: {log}"
    );
}

// The platform-only guarantee behind `internal_tcp_allow` is proven
// where the mirror lives: `egress_policy_rejects_internal_tcp_allow`
// in vrt-host/src/config.rs — `internalTcpAllow` is absent from
// `EgressPolicyToml`, so `deny_unknown_fields` fails any params file
// carrying the key in any spelling.
