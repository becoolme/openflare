//! `vrt` — standalone-mode control-plane CLI (T17).
//!
//! One binary: `init`, `serve`, and the day-2 ops (`deploy`,
//! `rollback`, `hostnames`, `secret`, `status`, `versions`, `renew`).
//! Ops talk to `vrt serve` over the management unix socket when it is
//! live; with serve down they open the root directly (the writer lock
//! serializes against a running serve). Secret values come from stdin
//! only — never argv (shell history / ps).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use vrt_local::{InitParams, LocalError, VrtLocal};

const USAGE: &str = "\
vrt — standalone Vibeploy Workers runtime control plane

USAGE:
  vrt init --root <dir> --cell <id> --tenant <id> [--mode capnp|stub]
           [--facade-dist <dir>] [--workerd <path>] [--ingress-listen <addr>]
  vrt serve --root <dir> [--supervise] [--vrt-host-bin <p>] [--vrt-provider-bin <p>]
  vrt deploy --root <dir> <worker_dir>
  vrt rollback --root <dir> <worker> <version_id>
  vrt hostnames add --root <dir> <worker> <hostname>
  vrt hostnames rm  --root <dir> <worker> <hostname>
  vrt hostnames ls  --root <dir> <worker>
  vrt secret put --root <dir> <secret_ref>        (value on stdin)
  vrt secret rm  --root <dir> <secret_ref>
  vrt secret ls  --root <dir>
  vrt status  --root <dir>
  vrt versions --root <dir> <worker>
  vrt renew   --root <dir>
  vrt key     --root <dir>                        (print trusted public key)
  vrt health  --root <dir>                        (mgmt /healthz → exit code)
  vrt probe   --url <http://host:port/path> | --file <path>
  vrt do-snapshot --root <dir> --worker <name> --class <uniqueKey> --tenant <id>
  vrt do-migrate  --root <dir> --worker <name> --class <uniqueKey> --tenant <id> --target-epoch <n>
  vrt worker-id --root <dir> <name|workerId>      (print workerId, one line)
  vrt do-key    --root <dir> <name|workerId> <className>  (print class uniqueKey)
  vrt workflows --root <dir> <name|workerId>      (print declared workflows JSON)
  vrt wfop --root <dir> --worker <name|id> --workflow <name> --op <create|get|action|delete|sendEvent>
            [--instance <id>] [--action <pause|resume|restart|terminate>]
            [--params <json>] [--event-type <t>] [--payload <json>] [--tenant <id>]
";

#[tokio::main]
async fn main() -> ExitCode {
    // T37b-r: without a subscriber every tracing::info!/error! in
    // serve's dispatch/renewal/supervision loops is silently dropped
    // (e2e saw "vrt" emit zero log lines while the dispatcher failed).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("vrt: {e}");
            ExitCode::FAILURE
        }
    }
}

fn arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

/// Positional args (everything not consumed as a `--flag value` pair).
fn positional(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i].starts_with("--") {
            i += 2;
            continue;
        }
        out.push(args[i].clone());
        i += 1;
    }
    out
}

fn root_of(args: &[String]) -> Result<PathBuf, String> {
    arg(args, "--root")
        .map(PathBuf::from)
        .ok_or_else(|| "--root required".to_string())
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let Some(cmd) = args.first().map(String::as_str) else {
        eprint!("{USAGE}");
        return Err("missing command".into());
    };
    match cmd {
        "init" => cmd_init(&args[1..]),
        "serve" => cmd_serve(&args[1..]).await,
        "deploy" => {
            let root = root_of(&args[1..])?;
            let pos = positional(&args[1..]);
            let dir = pos.first().ok_or("worker_dir required")?.clone();
            let r = direct(&root, |l| {
                l.deploy(
                    Path::new(&dir),
                    &vrt_local::worker_pkg::DeployOverrides::default(),
                )
                .map(|r| {
                    serde_json::json!({"versionId": r.version_id, "artifactDigest": r.artifact_digest, "revision": r.revision})
                })
            })?;
            println!("{}", serde_json::to_string_pretty(&r).unwrap_or_default());
            Ok(())
        }
        "rollback" => {
            let root = root_of(&args[1..])?;
            let pos = positional(&args[1..]);
            let (worker, version) = (
                pos.first().ok_or("worker required")?.clone(),
                pos.get(1).ok_or("version required")?.clone(),
            );
            let revision = direct(&root, |l| l.rollback(&worker, &version))?;
            println!("{revision}");
            Ok(())
        }
        "hostnames" => cmd_hostnames(&args[1..]).await,
        "secret" => cmd_secret(&args[1..]).await,
        "status" => {
            let root = root_of(&args[1..])?;
            let st = direct(&root, |l| {
                serde_json::to_value(l.status()?).map_err(|_| LocalError::State("serde"))
            })?;
            // T25/M2: merge the host supervisor's on-disk status
            // (<root>/work/host-status.json, written atomically by
            // vrt-host each converge tick) — tenant state, last
            // failure reason and circuit state become visible without
            // reaching the host control listener. Absent file → the
            // host isn't running (or hasn't ticked yet), report that
            // explicitly rather than silently omitting.
            let host = match std::fs::read(root.join("work/host-status.json")) {
                Ok(bytes) => serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map(|v| serde_json::json!({"running": true, "status": v}))
                    .unwrap_or_else(|_| serde_json::json!({"running": false, "error": "host-status.json unreadable"})),
                Err(_) => serde_json::json!({"running": false}),
            };
            // B12: metrics summary — best-effort fetch from the host
            // control listener (`null` when controlListen is unset).
            let metrics = vrt_local::serve::metrics_summary(&root)
                .unwrap_or_else(|| serde_json::json!({"reachable": false}));
            let merged = serde_json::json!({"local": st, "host": host, "metrics": metrics});
            println!(
                "{}",
                serde_json::to_string_pretty(&merged).unwrap_or_default()
            );
            Ok(())
        }
        "versions" => {
            let root = root_of(&args[1..])?;
            let pos = positional(&args[1..]);
            let worker = pos.first().ok_or("worker required")?.clone();
            let v = direct(&root, |l| {
                let st = l.status()?;
                let w = st
                    .workers
                    .into_iter()
                    .find(|w| w.name == worker || w.worker_id == worker)
                    .ok_or(LocalError::NotFound("worker"))?;
                Ok(serde_json::json!({
                    "worker": w.name, "activeVersionId": w.active_version_id,
                    "versions": w.versions,
                }))
            })?;
            println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            Ok(())
        }
        // P3c-2-r2: read-only identity queries — single-line stdout so
        // scripts never grep `vrt status` JSON positionally (pretty
        // output sorts keys alphabetically; `name` and `workerId` are
        // NOT adjacent → the old shell grep silently no-matched and
        // `set -e` killed the e2e with no diagnostic).
        "worker-id" => {
            let root = root_of(&args[1..])?;
            let pos = positional(&args[1..]);
            let q = pos
                .first()
                .ok_or("worker name or workerId required")?
                .clone();
            let wid = direct(&root, |l| l.resolve_worker_id(&q))?;
            println!("{wid}");
            Ok(())
        }
        "do-key" => {
            let root = root_of(&args[1..])?;
            let pos = positional(&args[1..]);
            let worker = pos
                .first()
                .ok_or("worker name or workerId required")?
                .clone();
            let class = pos.get(1).ok_or("DO class name required")?.clone();
            let key = direct(&root, |l| l.do_class_key(&worker, &class))?;
            println!("{key}");
            Ok(())
        }
        "workflows" => {
            let root = root_of(&args[1..])?;
            let pos = positional(&args[1..]);
            let worker = pos
                .first()
                .ok_or("worker name or workerId required")?
                .clone();
            let list = direct(&root, |l| l.workflows(&worker))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&list).map_err(|_| "json")?
            );
            Ok(())
        }
        "wfop" => cmd_wfop(&args[1..]),
        "renew" => {
            let root = root_of(&args[1..])?;
            let r = direct(&root, |l| l.renew())?;
            println!("{r}");
            Ok(())
        }
        "health" => {
            let root = root_of(&args[1..])?;
            cmd_health(&root)
        }
        // T37b-r: dump the in-process dispatcher metrics (vrt_queue_*)
        // over the mgmt socket — e2e diag and operators read these
        // directly when delivery stalls.
        "metrics" => {
            let root = root_of(&args[1..])?;
            cmd_metrics(&root)
        }
        // T37b-r4: queue backlog — signed GET /queues/{id}/metrics on
        // vrt-sql (visible/inFlight/dead counts). Diag path for
        // "send returned ok but nothing delivered".
        "queue-metrics" => {
            let root = root_of(&args[1..])?;
            let (id, name) = (
                arg(&args[1..], "--queue-id"),
                arg(&args[1..], "--queue-name"),
            );
            let queue_id = match (id, name) {
                (Some(i), None) => i.to_string(),
                (None, Some(n)) => vrt_local::worker_pkg::derive_queue_id(
                    arg(&args[1..], "--tenant").unwrap_or("local"),
                    n,
                )
                .map_err(|e| e.to_string())?,
                _ => return Err("require exactly one of --queue-id | --queue-name".into()),
            };
            cmd_queue_metrics(&root, &queue_id)
        }
        "probe" => cmd_probe(&args[1..]),
        "do-snapshot" => cmd_do_admin(&args[1..], "snapshot"),
        "do-migrate" => cmd_do_admin(&args[1..], "migrate"),
        "key" => {
            let root = root_of(&args[1..])?;
            let l = VrtLocal::open(&root).map_err(|e| e.to_string())?;
            let k = l.trusted_key().map_err(|e| e.to_string())?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "keyId": k.key_id, "publicKey": k.public_key,
                }))
                .unwrap_or_default()
            );
            Ok(())
        }
        _ => {
            eprint!("{USAGE}");
            Err(format!("unknown command: {cmd}"))
        }
    }
}

/// Open the root and run one library op. The writer lock inside every
/// op serializes against a live `vrt serve` — same code path as the
/// management API.
fn direct<T, F>(root: &Path, f: F) -> Result<T, String>
where
    F: FnOnce(&VrtLocal) -> Result<T, LocalError>,
{
    let l = VrtLocal::open(root).map_err(|e| e.to_string())?;
    f(&l).map_err(|e| e.to_string())
}

/// `vrt health` — management /healthz over the unix socket; exit 0
/// iff the control plane reports healthy. Compose healthcheck.
fn cmd_health(root: &Path) -> Result<(), String> {
    let sock = vrt_local::mgmt::socket_path(root);
    let token =
        std::fs::read(vrt_local::keys::mgmt_token_path(root)).map_err(|e| format!("token: {e}"))?;
    let token = String::from_utf8(token).map_err(|_| "token utf8")?;
    let (status, body) = unix_http(&sock, "GET", "/healthz", Some(&token), None)?;
    print!("{body}");
    if status == 200 {
        Ok(())
    } else {
        Err(format!("healthz status {status}"))
    }
}

/// `vrt metrics` — Prometheus text off the mgmt socket (queue
/// dispatcher + renewal counters; empty registry while serve is down).
fn cmd_metrics(root: &Path) -> Result<(), String> {
    let sock = vrt_local::mgmt::socket_path(root);
    let token =
        std::fs::read(vrt_local::keys::mgmt_token_path(root)).map_err(|e| format!("token: {e}"))?;
    let token = String::from_utf8(token).map_err(|_| "token utf8")?;
    let (status, body) = unix_http(&sock, "GET", "/v1/metrics", Some(&token), None)?;
    print!("{body}");
    if status == 200 {
        Ok(())
    } else {
        Err(format!("metrics status {status}"))
    }
}

/// `vrt queue-metrics` — signed `GET /queues/{id}/metrics` on vrt-sql
/// via the provider's [vrtSql] channel (same auth model as the
/// dispatcher). Prints the raw JSON body — backlog split into
/// visible/inFlight/delayed/dead for queue-stall diagnosis.
fn cmd_queue_metrics(root: &Path, queue_id: &str) -> Result<(), String> {
    if queue_id.len() != 26
        || !queue_id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err("queue id must be 26 [a-z0-9] chars".into());
    }
    let (addr, secret_file) =
        vrt_local::serve::provider_sql(root).ok_or("vrt-provider.toml has no [vrtSql]")?;
    let secret = vrt_provider::cli::load_cursor_secret(&secret_file)
        .map_err(|e| format!("secret: {e}"))?;
    let base = addr.trim_end_matches('/');
    let authority = base
        .strip_prefix("http://")
        .ok_or("vrtSql addr must be http://")?;
    let path_query = format!("/queues/{queue_id}/metrics");
    let signer = vrt_provider::d1::SqlAuthSigner::new(secret);
    let seq = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let auth = signer.sign("GET", &path_query, b"", 1, seq);
    // Bounded I/O — DNS resolve + 5s connect + 10s r/w (诊断绝不挂死).
    let sock_addr = std::net::ToSocketAddrs::to_socket_addrs(&authority)
        .map_err(|e| format!("resolve {authority}: {e}"))?
        .next()
        .ok_or("vrtSql addr resolves to nothing")?;
    let mut stream =
        std::net::TcpStream::connect_timeout(&sock_addr, std::time::Duration::from_secs(5))
            .map_err(|e| format!("connect {authority}: {e}"))?;
    use std::io::{Read, Write};
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .and_then(|_| stream.set_write_timeout(Some(std::time::Duration::from_secs(10))))
        .map_err(|e| e.to_string())?;
    let epoch_h = vrt_provider::d1::SQL_EPOCH_HEADER;
    let seq_h = vrt_provider::d1::SQL_SEQ_HEADER;
    let auth_h = vrt_provider::d1::SQL_AUTH_HEADER;
    let req = format!(
        "GET {path_query} HTTP/1.0\r\nhost: {authority}\r\nconnection: close\r\n\
         {epoch_h}: 1\r\n{seq_h}: {seq}\r\n{auth_h}: {auth}\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if status != 200 {
        return Err(format!(
            "queue-metrics status {status}: {}",
            &body[..body.len().min(200)]
        ));
    }
    println!("{body}");
    Ok(())
}

/// `vrt probe` — liveness for processes without an HTTP endpoint
/// (`--file`: exists + non-empty) or with one (`--url`: 2xx).
/// W3: `vrt wfop` — operator bridge into the platform wfops channel.
///
/// Resolves the real workerId from LOCAL state and verifies the
/// workflow is declared (local check only), then POSTs `/v1/wfops`
/// on vrt-host's loopback control listener — the same trust model as
/// `do-snapshot`. W3-r2: NO token is sent — the hop injects
/// `x-vrt-wfop-token` from the serving spec's map (HMAC'd with the
/// host's in-process pepper); this CLI cannot and must not derive it.
fn cmd_wfop(args: &[String]) -> Result<(), String> {
    use std::io::{Read, Write};
    let root_s = arg(args, "--root").ok_or("--root required")?;
    let root = std::path::Path::new(root_s);
    let worker = arg(args, "--worker").ok_or("--worker required")?;
    let workflow = arg(args, "--workflow").ok_or("--workflow required")?;
    let op = arg(args, "--op").ok_or("--op required")?;
    let tenant = arg(args, "--tenant").unwrap_or("local");
    if !matches!(op, "create" | "get" | "action" | "delete" | "sendEvent") {
        return Err("--op must be create|get|action|delete|sendEvent".into());
    }
    let worker_id = direct(root, |l| l.resolve_worker_id(worker))?;
    let wfs = direct(root, |l| l.workflows(worker))?;
    let decl = wfs
        .iter()
        .find(|w| w.name == workflow)
        .ok_or_else(|| format!("workflow '{workflow}' is not declared on this worker"))?;
    // W7: ops belong to the DEFINING worker — `/v1/wfops` resolves
    // (workerId, name) against `sock-wfops-<definingId>` and the
    // token map keyed under it, so dispatch must carry that id.
    let owning_worker_id = decl
        .service_worker_id
        .clone()
        .unwrap_or_else(|| worker_id.clone());

    // Op body shape mirrors cell-controller/src/wfops.rs — keep the
    // two in lockstep (both feed the same facade contract).
    let mut op_body = serde_json::json!({ "op": op, "workflow": workflow });
    if let Some(id) = arg(args, "--instance") {
        op_body["instanceId"] = serde_json::Value::String(id.to_string());
    }
    if let Some(a) = arg(args, "--action") {
        if !matches!(a, "pause" | "resume" | "restart" | "terminate") {
            return Err("--action must be pause|resume|restart|terminate".into());
        }
        op_body["action"] = serde_json::Value::String(a.to_string());
    }
    if let Some(p) = arg(args, "--params") {
        op_body["params"] = serde_json::from_str(p).map_err(|e| format!("--params json: {e}"))?;
    }
    // W6: sendEvent args — same contract the facade/cell enforce
    // (type charset + serialized payload bound live downstream; the
    // CLI just forwards typed fields).
    if let Some(t) = arg(args, "--event-type") {
        op_body["eventType"] = serde_json::Value::String(t.to_string());
    }
    if let Some(p) = arg(args, "--payload") {
        op_body["payload"] = serde_json::from_str(p).map_err(|e| format!("--payload json: {e}"))?;
    }
    let body = serde_json::json!({
        "tenantId": tenant,
        "workerId": owning_worker_id,
        "op": op_body,
    })
    .to_string();

    // controlListen from config/vrt-host.toml (minimal line parse —
    // same convention as cmd_do_admin above).
    let text = std::fs::read_to_string(root.join("config").join("vrt-host.toml"))
        .map_err(|e| format!("read vrt-host.toml: {e}"))?;
    let mut base: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("controlListen") {
            let v = rest
                .trim_start_matches([' ', '='])
                .split('#')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"');
            if !v.is_empty() {
                base = Some(v.to_string());
            }
        }
    }
    let base = base.ok_or("vrt-host.toml has no controlListen")?;

    let mut stream = std::net::TcpStream::connect(&base).map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(60)))
        .map_err(|e| e.to_string())?;
    let req = format!(
        "POST /v1/wfops HTTP/1.0\r\nhost: {base}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_out = text.split("\r\n\r\n").nth(1).unwrap_or("");
    println!("{body_out}");
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("wfop status {status}: {body_out}"))
    }
}

fn cmd_probe(args: &[String]) -> Result<(), String> {
    if let Some(url) = arg(args, "--url") {
        let url = url
            .strip_prefix("http://")
            .ok_or("probe supports http:// only")?;
        let (authority, path) = url
            .split_once('/')
            .map(|(a, p)| (a, format!("/{p}")))
            .unwrap_or((url, "/".to_string()));
        use std::io::{Read, Write};
        let mut stream =
            std::net::TcpStream::connect(authority).map_err(|e| format!("connect: {e}"))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        let req = format!("GET {path} HTTP/1.0\r\nhost: {authority}\r\n\r\n");
        stream
            .write_all(req.as_bytes())
            .map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&buf);
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if (200..300).contains(&status) {
            return Ok(());
        }
        return Err(format!("probe status {status}"));
    }
    if let Some(path) = arg(args, "--file") {
        let meta = std::fs::metadata(path).map_err(|e| format!("file: {e}"))?;
        if meta.len() == 0 {
            return Err("file empty".into());
        }
        return Ok(());
    }
    // D8: provider liveness — every listener socket under the dir
    // must accept a unix connection (protocol handshake is the
    // provider's own auth layer; connect-accept proves the listener).
    if let Some(dir) = arg(args, "--dir-sockets") {
        use std::os::unix::net::UnixStream;
        let entries = std::fs::read_dir(dir).map_err(|e| format!("dir: {e}"))?;
        let mut count = 0;
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) == Some("sock") {
                UnixStream::connect(&p).map_err(|e| format!("connect {}: {e}", p.display()))?;
                count += 1;
            }
        }
        // Zero listeners is unhealthy — the provider has not adopted
        // any tenant yet (desired empty or reconcile broken).
        if count == 0 {
            return Err("no listener sockets".into());
        }
        return Ok(());
    }
    Err("probe requires --url | --file | --dir-sockets".into())
}

/// Minimal unix-socket HTTP/1.1 client (sync — CLI ops are one-shot).
fn unix_http(
    sock: &Path,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<serde_json::Value>,
) -> Result<(u16, String), String> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let mut req = format!("{method} {path} HTTP/1.1\r\nhost: vrt\r\nconnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("authorization: Bearer {t}\r\n"));
    }
    let body = body.map(|b| serde_json::to_string(&b).unwrap_or_default());
    if let Some(b) = &body {
        req.push_str(&format!(
            "content-type: application/json\r\ncontent-length: {}\r\n",
            b.len()
        ));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(&b);
    }
    let mut stream = UnixStream::connect(sock).map_err(|e| format!("socket: {e}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok((status, body.to_string()))
}

fn cmd_init(args: &[String]) -> Result<(), String> {
    let root = root_of(args)?;
    let reconfigure = args.iter().any(|a| a == "--reconfigure");
    let cell = arg(args, "--cell").unwrap_or("local");
    let tenant = arg(args, "--tenant").unwrap_or("local");
    // D6: facade dist defaults to the image-baked copy; --facade-dist
    // overrides for bare-metal installs.
    let facade_dist = arg(args, "--facade-dist").map(PathBuf::from).or_else(|| {
        let baked = PathBuf::from("/usr/share/vrt/facade");
        baked.join("manifest.json").is_file().then_some(baked)
    });
    // D5: mode derives from the workerd binary identity — `vrt-stub-
    // workerd --version` prints its marker; anything else is treated
    // as a real workerd. --mode may pin the choice but must AGREE
    // with detection (mismatch fails closed, never silently stub).
    let workerd = arg(args, "--workerd").map(PathBuf::from);
    let detected = detect_workerd_kind(workerd.as_deref());
    let mode = match arg(args, "--mode") {
        Some(m) => {
            if m != "capnp" && m != "stub" {
                return Err("--mode must be capnp|stub".into());
            }
            match (m, detected) {
                ("stub", WorkerdKind::Real) => {
                    return Err("--mode stub but workerd is not vrt-stub-workerd".into())
                }
                ("capnp", WorkerdKind::Stub) => {
                    return Err("--mode capnp but workerd IS vrt-stub-workerd".into())
                }
                _ => m.to_string(),
            }
        }
        None => match detected {
            WorkerdKind::Stub => "stub".to_string(),
            _ => "capnp".to_string(),
        },
    };
    // T24/K2: a missing/unrunnable workerd must fail HERE — writing a
    // config that points at a nonexistent binary turns "no runtime"
    // into a fake-successful init (compose WORKERD_SOURCE=none relied
    // on this surfacing). Explicit --workerd → the file must exist AND
    // exec; the mode's default path → must exist.
    let resolved_workerd = workerd.clone().unwrap_or_else(|| {
        PathBuf::from(if mode == "stub" {
            "/usr/local/bin/vrt-stub-workerd"
        } else {
            "/usr/local/bin/workerd"
        })
    });
    if workerd.is_some() && matches!(detected, WorkerdKind::Unknown) {
        return Err(format!(
            "workerd at {} is missing or not executable — install a real workerd there or pass a valid --workerd",
            resolved_workerd.display()
        ));
    }
    if workerd.is_none() && !resolved_workerd.is_file() {
        return Err(format!(
            "no workerd found at the {} default {} — pass --workerd <path>",
            mode,
            resolved_workerd.display()
        ));
    }
    // D7: already-initialized root → compare the would-be config.
    // Identical → no-op; different → fail with the changed field
    // names (never values/secrets); --reconfigure rewrites config/ only.
    let initialized =
        root.join("keys/trusted.json").is_file() && root.join("state/state.json").is_file();
    if initialized {
        let l = VrtLocal::open(&root).map_err(|e| e.to_string())?;
        let key = l.trusted_key().map_err(|e| e.to_string())?;
        let diff = config_diff(&root, &key, &mode, args)?;
        if diff.is_empty() {
            println!("{} already initialized; config unchanged", root.display());
            return Ok(());
        }
        if !reconfigure {
            return Err(format!(
                "already initialized; config differs on: {} — pass --reconfigure to rewrite config/ (keys/state untouched)",
                diff.join(", ")
            ));
        }
        write_configs(
            &vrt_local::layout::config_dir(&root),
            &root,
            &key,
            &mode,
            args,
        )?;
        println!(
            "reconfigured {} (changed: {})",
            root.display(),
            diff.join(", ")
        );
        return Ok(());
    }
    let report = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: cell.to_string(),
            tenant_id: tenant.to_string(),
            facade_manifest: facade_dist.as_ref().map(|d| d.join("manifest.json")),
            facade_dist_root: facade_dist.clone(),
            max_versions_per_worker: None,
        },
    )
    .map_err(|e| e.to_string())?;
    write_configs(
        &vrt_local::layout::config_dir(&root),
        &root,
        &report.trusted_key,
        &mode,
        args,
    )?;
    let token_path = vrt_local::keys::mgmt_token_path(&root);
    println!(
        "initialized {}\ntrustedKey = {}",
        root.display(),
        serde_json::to_string(&serde_json::json!({
            "keyId": report.trusted_key.key_id,
            "publicKey": report.trusted_key.public_key,
        }))
        .unwrap_or_default()
    );
    println!(
        "mgmt token: {} (0600, required by `vrt` ops via serve)",
        token_path.display()
    );
    println!(
        "configs: {}/config/vrt-host.toml, vrt-provider.toml",
        root.display()
    );
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkerdKind {
    Stub,
    Real,
    /// Binary absent/unprobeable — treated as real (capnp); a bad
    /// path still fails later at host launch, init records intent.
    Unknown,
}

/// `workerd --version` → identity. vrt-stub-workerd prints its marker;
/// a real workerd prints `workerd <ver>`; unreadable → Unknown.
fn detect_workerd_kind(workerd: Option<&Path>) -> WorkerdKind {
    let Some(bin) = workerd else {
        // No explicit path: a baked stub is the only stub we can be
        // sure of by name — keep the old default heuristic.
        return WorkerdKind::Unknown;
    };
    match std::process::Command::new(bin).arg("--version").output() {
        Ok(out) => {
            let text: String = String::from_utf8_lossy(&out.stdout).into_owned()
                + &String::from_utf8_lossy(&out.stderr);
            if text.contains("vrt-stub-workerd") {
                WorkerdKind::Stub
            } else {
                WorkerdKind::Real
            }
        }
        Err(_) => WorkerdKind::Unknown,
    }
}

/// Render prospective configs and diff field NAMES that changed vs
/// `root/config/*.toml` — secrets never appear (configs hold none).
fn config_diff(
    root: &Path,
    key: &snapshot_protocol::TrustedKey,
    mode: &str,
    args: &[String],
) -> Result<Vec<String>, String> {
    let tmp = std::env::temp_dir().join(format!("vrt-cfg-diff-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).map_err(|e| e.to_string())?;
    write_configs(&tmp, root, key, mode, args)?;
    let mut changed = Vec::new();
    for name in ["vrt-host.toml", "vrt-provider.toml"] {
        let new = std::fs::read_to_string(tmp.join(name)).unwrap_or_default();
        let old = std::fs::read_to_string(vrt_local::layout::config_dir(root).join(name))
            .unwrap_or_default();
        if new != old {
            // Report the differing top-level keys (names only).
            for line in new.lines().chain(old.lines()) {
                if let Some((k, _)) = line.split_once('=') {
                    let k = k.trim();
                    if !k.is_empty()
                        && !changed.iter().any(|c| c == k)
                        && field_changed(&new, &old, k)
                    {
                        changed.push(k.to_string());
                    }
                }
            }
            if changed.is_empty() {
                changed.push(format!("{name} (whitespace/comments)"));
            } else {
                changed.push(name.to_string());
            }
        }
    }
    let _ = std::fs::remove_dir_all(&tmp);
    Ok(changed)
}

fn field_changed(new: &str, old: &str, key: &str) -> bool {
    let get = |s: &str| {
        s.lines()
            .find(|l| {
                l.split_once('=')
                    .map(|(k, _)| k.trim() == key)
                    .unwrap_or(false)
            })
            .map(str::to_string)
    };
    get(new) != get(old)
}

/// Render `root/config/{vrt-host,vrt-provider}.toml` from the SAME
/// templates as examples/standalone-*.toml (T16) — `include_str!`
/// makes the examples and generated configs a single source of truth;
/// placeholders `__VRT_*__` are substituted here.
/// Render the two service configs into `out_dir`, substituting paths
/// for `root` — `out_dir` differs from root's config dir only when
/// diffing prospective config (D7 --reconfigure).
fn write_configs(
    out_dir: &Path,
    root: &Path,
    key: &snapshot_protocol::TrustedKey,
    mode: &str,
    args: &[String],
) -> Result<(), String> {
    const HOST_T: &str = include_str!("../../../examples/standalone-vrt-host.toml");
    const PROVIDER_T: &str = include_str!("../../../examples/standalone-vrt-provider.toml");
    const SQL_T: &str = include_str!("../../../examples/standalone-vrt-sql.toml");
    // T34e: unix socket paths under this root must fit the AF_UNIX
    // budget — refuse before writing a config the provider cannot
    // actually bind with.
    vrt_local::layout::check_socket_path_budget(root, arg(args, "--tenant").unwrap_or("local"))
        .map_err(|e| e.to_string())?;
    let root_s = root
        .canonicalize()
        .unwrap_or_else(|_| root.to_path_buf())
        .display()
        .to_string();
    let workerd = arg(args, "--workerd").unwrap_or(if mode == "stub" {
        "/usr/local/bin/vrt-stub-workerd"
    } else {
        "/usr/local/bin/workerd"
    });
    let listen = arg(args, "--ingress-listen").unwrap_or("127.0.0.1:8080");
    let control = arg(args, "--control-listen").unwrap_or("127.0.0.1:9091");
    // B12: dedicated /metrics ports — loopback by default; compose
    // passes 0.0.0.0 so the optional monitoring profile can scrape
    // (the ports are never published to the host).
    let metrics = arg(args, "--metrics-listen").unwrap_or("127.0.0.1:9494");
    let provider_metrics = arg(args, "--provider-metrics-listen").unwrap_or("127.0.0.1:9493");
    let mut host = HOST_T
        .replace("__VRT_ROOT__", &root_s)
        .replace("__WORKERD__", workerd)
        .replace("__VRT_LISTEN__", listen)
        .replace("__VRT_CONTROL__", control)
        .replace("__VRT_METRICS__", metrics)
        .replace("__VRT_KEY_ID__", &key.key_id)
        .replace("__VRT_PUBKEY__", &key.public_key)
        .replace("__VRT_CELL__", arg(args, "--cell").unwrap_or("local"))
        .replace("__VRT_TENANT__", arg(args, "--tenant").unwrap_or("local"))
        // T25/L13: the binding-backend address is the provider's
        // per-tenant listener socket — derived through the SAME
        // `vrt_securefs::socket_path` helper the provider binds with,
        // so host render and provider bind can never disagree.
        .replace("__VRT_BACKEND_SOCK__", &{
            let tenant = arg(args, "--tenant").unwrap_or("local");
            vrt_securefs::socket_path(&vrt_local::layout::provider_listeners_dir(root), tenant)
                .display()
                .to_string()
        });
    if mode == "stub" {
        // StubJson ignores the capnp-only fields; keep the file valid
        // by flipping just the mode (fields remain, unused).
        host = host.replacen("mode = \"capnp\"", "mode = \"stub\"", 1);
    }
    let provider = PROVIDER_T
        .replace("__VRT_ROOT__", &root_s)
        .replace("__VRT_KEY_ID__", &key.key_id)
        .replace("__VRT_PUBKEY__", &key.public_key)
        .replace("__VRT_CELL__", arg(args, "--cell").unwrap_or("local"))
        .replace("__VRT_TENANT__", arg(args, "--tenant").unwrap_or("local"))
        .replace("__VRT_PROVIDER_METRICS__", provider_metrics)
        .replace(
            "__VRT_SQL_ADDR__",
            arg(args, "--vrt-sql-addr").unwrap_or("http://vrt-sql:8090"),
        );
    std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
    let sql = SQL_T
        .replace("__VRT_ROOT__", &root_s)
        .replace(
            "__VRT_SQL_LISTEN__",
            arg(args, "--vrt-sql-listen").unwrap_or("0.0.0.0:8090"),
        )
        .replace(
            "__VRT_SQL_METRICS__",
            arg(args, "--vrt-sql-metrics-listen").unwrap_or("127.0.0.1:9492"),
        );
    let cfg = out_dir;
    for (name, doc) in [
        ("vrt-host.toml", host),
        ("vrt-provider.toml", provider),
        ("vrt-sql.toml", sql),
    ] {
        vrt_securefs::atomic_write(&cfg.join(name), doc.as_bytes(), 0o600)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn cmd_serve(args: &[String]) -> Result<(), String> {
    let root = root_of(args)?;
    let opts = vrt_local::serve::ServeOptions {
        supervise: args.iter().any(|a| a == "--supervise"),
        vrt_host_bin: arg(args, "--vrt-host-bin").map(PathBuf::from),
        vrt_provider_bin: arg(args, "--vrt-provider-bin").map(PathBuf::from),
        extra_args: Vec::new(),
    };
    vrt_local::serve::run(&root, &opts)
        .await
        .map_err(|e| e.to_string())
}

async fn cmd_hostnames(args: &[String]) -> Result<(), String> {
    let sub = args
        .first()
        .map(String::as_str)
        .ok_or("add|rm|ls required")?;
    let root = root_of(&args[1..])?;
    let pos = positional(&args[1..]);
    let worker = pos.first().ok_or("worker required")?.clone();
    match sub {
        "add" | "rm" => {
            let host = pos.get(1).ok_or("hostname required")?.clone();
            let add = sub == "add";
            let rev = direct(&root, |l| {
                if add {
                    l.set_hostname(&worker, &host)
                } else {
                    l.remove_hostname(&worker, &host)
                }
            })?;
            println!("{rev}");
            Ok(())
        }
        "ls" => {
            let v = direct(&root, |l| {
                let st = l.status()?;
                let w = st
                    .workers
                    .into_iter()
                    .find(|w| w.name == worker || w.worker_id == worker)
                    .ok_or(LocalError::NotFound("worker"))?;
                Ok(serde_json::json!({"hostnames": w.hostnames}))
            })?;
            println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            Ok(())
        }
        _ => Err("hostnames add|rm|ls".into()),
    }
}

async fn cmd_secret(args: &[String]) -> Result<(), String> {
    let sub = args
        .first()
        .map(String::as_str)
        .ok_or("put|rm|ls required")?;
    let root = root_of(&args[1..])?;
    match sub {
        "put" => {
            let pos = positional(&args[1..]);
            let secret_ref = pos.first().ok_or("secret_ref required")?.clone();
            // Value on stdin only — never argv (shell history / ps).
            let mut value = String::new();
            std::io::stdin()
                .read_to_string(&mut value)
                .map_err(|e| e.to_string())?;
            let value = value.trim_end_matches(['\n', '\r']).to_string();
            if value.is_empty() {
                return Err("empty secret value on stdin".into());
            }
            direct(&root, |l| {
                let st = l.status()?;
                l.put_secret(&st.tenant_id, &secret_ref, &value).map(|_| ())
            })?;
            eprintln!("secret stored");
            Ok(())
        }
        "rm" => {
            let pos = positional(&args[1..]);
            let secret_ref = pos.first().ok_or("secret_ref required")?.clone();
            direct(&root, |l| {
                let st = l.status()?;
                l.delete_secret(&st.tenant_id, &secret_ref)
            })?;
            println!("removed");
            Ok(())
        }
        "ls" => {
            let v = direct(&root, |l| {
                Ok(serde_json::json!({"secretRefs": l.status()?.secret_refs}))
            })?;
            println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            Ok(())
        }
        _ => Err("secret put|rm|ls".into()),
    }
}

/// P3c-2: `vrt do-snapshot|do-migrate` — standalone admin triggers
/// for the DO placement/snapshot/migration control endpoints on the
/// vrt-host loopback control listener (same trust model as cron:
/// loopback-only listener; idempotency header mirrors the body's
/// identity so a replay can never re-drive a fence).
fn cmd_do_admin(args: &[String], op: &str) -> Result<(), String> {
    use std::io::{Read, Write};
    let root_s = arg(args, "--root").ok_or("--root required")?;
    let root = std::path::Path::new(root_s);
    let worker = arg(args, "--worker").ok_or("--worker required")?;
    let class = arg(args, "--class").ok_or("--class <uniqueKey> required")?;
    let tenant = arg(args, "--tenant").unwrap_or("local");
    if class.len() != 64 || !class.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("--class must be the class uniqueKey (64-hex)".into());
    }
    // The admin endpoint resolves the class dir from the host's
    // trusted desired view — it needs the REAL workerId, not the
    // display name. Accept either (same convention as `versions`).
    let worker = direct(root, |l| l.resolve_worker_id(&worker))?;
    // controlListen from config/vrt-host.toml (minimal line parse —
    // same convention serve.rs::host_control_base reads).
    let text = std::fs::read_to_string(root.join("config").join("vrt-host.toml"))
        .map_err(|e| format!("read vrt-host.toml: {e}"))?;
    let mut base: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("controlListen") {
            let v = rest
                .trim_start_matches([' ', '='])
                .split('#')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"');
            if !v.is_empty() {
                base = Some(v.to_string());
            }
        }
    }
    let base = base.ok_or("vrt-host.toml has no controlListen")?;
    let (body, key, path) = if op == "migrate" {
        let te = arg(args, "--target-epoch").ok_or("--target-epoch required")?;
        let te: u64 = te.parse().map_err(|_| "--target-epoch must be u64")?;
        (
            format!(
                "{{\"tenantId\":\"{tenant}\",\"workerId\":\"{worker}\",\"uniqueKey\":\"{class}\",\"targetEpoch\":{te}}}"
            ),
            format!("{tenant}:{worker}:{class}:{te}"),
            "/v1/do/migrate",
        )
    } else {
        (
            format!(
                "{{\"tenantId\":\"{tenant}\",\"workerId\":\"{worker}\",\"uniqueKey\":\"{class}\"}}"
            ),
            String::new(),
            "/v1/do/snapshot",
        )
    };
    let mut stream = std::net::TcpStream::connect(&base).map_err(|e| format!("connect: {e}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(90)))
        .map_err(|e| e.to_string())?;
    let req = format!(
        "POST {path} HTTP/1.0\r\nhost: {base}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nx-vrt-do-migrate-key: {key}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_out = text.split("\r\n\r\n").nth(1).unwrap_or("");
    println!("{body_out}");
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("do-{op} status {status}: {body_out}"))
    }
}
