//! Integration: Hyperdrive session pooling against a real PostgreSQL.
//!
//! Starts a throwaway PG16 (initdb/pg_ctl, exactly like the m17/m18
//! harness), points a pooled `PgProxy` at it and drives the pg-wire
//! protocol directly. Proves the three properties that only a real
//! backend can show:
//!
//! 1. two sessions of the same binding run on ONE backend connection
//!    (`pg_backend_pid()` is unchanged) and the session state of the
//!    first — temp tables, GUCs — is gone for the second (`DISCARD ALL`);
//! 2. with `maxConnections = 1`, the second concurrent session is queued
//!    and denied with the typed pool error after `acquireTimeoutMs`;
//! 3. a second tenant never lands on the first tenant's connection.
//!
//! Skips (loudly) when no PostgreSQL bin dir is available; CI's
//! real-workerd job provides one through `VRT_TEST_PG_BIN`.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use snapshot_protocol::compute_desired::{cell_scope, ComputeDesired};
use snapshot_protocol::envelope::{sign, SignParams, SigningKey};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use vrt_provider::config::{PgProxyConfig, PoolConfig};
use vrt_provider::desired_file::FileDesiredStore;
use vrt_provider::pg::PgCredentialStore;
use vrt_provider::pgproxy::PgProxy;

const PG_USER: &str = "vrtadmin";
/// Throwaway login of the temp instance — never a real secret.
const PG_PASSWORD: &str = "pool-throwaway-pw";
const CELL: &str = "cell-pool-test";
const PROTO_V3: u32 = 196_608;
const ARTIFACT_DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

// ------------------------------------------------------------ temp PG16

struct TempPg {
    bin: PathBuf,
    root: PathBuf,
    data: PathBuf,
    port: u16,
}

impl Drop for TempPg {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
            .arg("-D")
            .arg(&self.data)
            .args(["-m", "immediate", "-w", "stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn pg_bin_dir() -> Option<PathBuf> {
    let dir = std::env::var("VRT_TEST_PG_BIN")
        .ok()
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/opt/homebrew/opt/postgresql@16/bin"));
    let candidates = [
        dir,
        PathBuf::from("/usr/lib/postgresql/16/bin"),
        PathBuf::from("/usr/local/opt/postgresql@16/bin"),
    ];
    candidates
        .into_iter()
        .find(|d| d.join("initdb").is_file() && d.join("pg_ctl").is_file())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Start a throwaway PG16. `None` = no PostgreSQL installed (skip).
fn start_pg() -> Option<TempPg> {
    let bin = pg_bin_dir()?;
    let root = vrt_securefs::test_short_root("vrt-pgpool").ok()?;
    let data = root.join("data");
    let pwfile = root.join("pw");
    std::fs::write(&pwfile, PG_PASSWORD).unwrap();
    let port = free_port();
    let mut pg = TempPg {
        bin,
        root: root.clone(),
        data: data.clone(),
        port,
    };
    let out = Command::new(pg.bin.join("initdb"))
        .arg("-D")
        .arg(&data)
        .args([
            "-A",
            "scram-sha-256",
            "-U",
            PG_USER,
            "-E",
            "UTF8",
            "--no-locale",
        ])
        .arg(format!("--pwfile={}", pwfile.display()))
        .env("LC_ALL", "en_US.UTF-8")
        .output()
        .expect("spawn initdb");
    assert!(
        out.status.success(),
        "initdb failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let opts = format!(
        "-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories={} -c fsync=off",
        root.display()
    );
    let out = Command::new(pg.bin.join("pg_ctl"))
        .arg("-D")
        .arg(&data)
        .arg("-l")
        .arg(root.join("pg.log"))
        .arg("-o")
        .arg(&opts)
        .args(["-w", "start"])
        .env("LC_ALL", "en_US.UTF-8")
        .output()
        .expect("spawn pg_ctl");
    if !out.status.success() {
        let log = std::fs::read_to_string(root.join("pg.log")).unwrap_or_default();
        pg.port = 0;
        panic!(
            "pg_ctl start failed: {}\n-- pg.log --\n{log}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Some(pg)
}

// ------------------------------------------------------ provider fixtures

fn binding_id(tenant: &str) -> String {
    format!("vrthd:{tenant}:DB")
}

fn synthetic_host(tenant: &str) -> String {
    // Same shape as the platform mint: `hd<hex>.hyperdrive.vrt.internal`
    // (the first label is the pinned proxy route, `[a-z0-9]` only).
    format!(
        "hd{}.hyperdrive.vrt.internal",
        hex::encode(tenant.as_bytes())
    )
}

/// Signed `compute.desired` with one Hyperdrive binding per tenant, all
/// pointing at the same throwaway PostgreSQL through `credentialRef`.
fn desired_store(dir: &Path, tenants: &[&str]) -> Arc<FileDesiredStore> {
    let payload: ComputeDesired = serde_json::from_value(json!({
        "schemaVersion": 1,
        "cellId": CELL,
        "revision": 1,
        "tenants": tenants.iter().map(|t| json!({
            "tenantId": t,
            "workers": [{
                "workerId": format!("w-{t}"),
                "name": format!("w-{t}"),
                "activeVersion": {
                    "versionId": format!("v-{t}-1"),
                    "artifactDigest": ARTIFACT_DIGEST,
                    "compatibilityDate": "2026-09-08",
                    "compatibilityFlags": [],
                },
                "bindings": [{
                    "type": "hyperdrive",
                    "name": "DB",
                    "bindingId": binding_id(t),
                    "syntheticHost": synthetic_host(t),
                    "credentialRef": "vrtpgcred:db",
                    "permissions": {"read": true, "write": true},
                }],
                "routes": [],
                "crons": [],
                "quotas": {
                    "cpuMs": 50,
                    "memoryMb": 128,
                    "subrequests": 100,
                    "outboundConnections": 16,
                },
            }],
        })).collect::<Vec<_>>(),
    }))
    .expect("desired payload decodes");

    let key = SigningKey::from_seed(&[23u8; 32]).expect("seeded key");
    let now = chrono::Utc::now();
    let signed = sign(
        &SignParams {
            kind: "compute.desired",
            scope: &cell_scope(CELL),
            revision: 1,
            issued_at: now,
            expires_at: now + chrono::Duration::minutes(30),
            payload: &payload,
        },
        &key,
    )
    .expect("sign desired");
    let store = Arc::new(FileDesiredStore::new(
        dir.join("desired.json"),
        CELL,
        vec![key.trusted_key()],
        1,
    ));
    store
        .load_bytes(&signed.bytes, now)
        .expect("verify+load signed desired");
    store
}

/// Credential file: one `tcp` payload per tenant, same upstream login.
fn credentials(dir: &Path, port: u16, tenants: &[&str]) -> PathBuf {
    let mut doc = serde_json::Map::new();
    for t in tenants {
        doc.insert(
            (*t).to_string(),
            json!({
                "vrtpgcred:db": {
                    "token": "t",
                    "endpoint": "http://gw",
                    "tenantSlug": format!("{t}-x"),
                    "credentialVersion": 1,
                    "readOnly": false,
                    "tcp": {
                        "host": "127.0.0.1",
                        "port": port,
                        "user": PG_USER,
                        "password": PG_PASSWORD,
                        "database": "postgres",
                    },
                }
            }),
        );
    }
    let path = dir.join("pg-credentials.json");
    std::fs::write(&path, serde_json::to_vec(&json!({"tenants": doc})).unwrap()).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}

/// Pooled proxy over the temp PG; returns its loopback address.
async fn start_proxy(
    dir: &Path,
    pg_port: u16,
    tenants: &[&str],
    pool: PoolConfig,
) -> (SocketAddr, Arc<PgProxy>) {
    let creds = Arc::new(PgCredentialStore::new(credentials(dir, pg_port, tenants)));
    let proxy = PgProxy::new(
        PgProxyConfig {
            listen: "127.0.0.1:0".into(),
            connect_timeout_ms: 5_000,
            idle_timeout_ms: 60_000,
            max_session_ms: 120_000,
            max_sessions: 16,
            max_sessions_per_binding: 8,
        },
        Some(pool),
        creds,
        desired_store(dir, tenants),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let px = Arc::clone(&proxy);
    tokio::spawn(async move { px.serve(listener).await });
    (addr, proxy)
}

fn default_pool(max_connections: usize, acquire_timeout_ms: u64) -> PoolConfig {
    PoolConfig {
        max_connections,
        acquire_timeout_ms,
        idle_timeout_ms: 60_000,
        max_lifetime_ms: 600_000,
        reset_on_return: true,
    }
}

// --------------------------------------------------------- pg-wire client

/// One backend reply, as seen by the tenant client.
struct Reply {
    rows: Vec<String>,
    error: Option<String>,
}

async fn connect(addr: SocketAddr, tenant: &str) -> io::Result<TcpStream> {
    let mut c = TcpStream::connect(addr).await?;
    let label = synthetic_host(tenant);
    let label = label.split('.').next().unwrap();
    c.write_all(format!("VRT-PG1 {label}\n").as_bytes()).await?;
    let mut body = Vec::new();
    for kv in ["user", "vrt-hd", "database", "vrt"] {
        body.extend_from_slice(kv.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut startup = Vec::new();
    startup.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
    startup.extend_from_slice(&PROTO_V3.to_be_bytes());
    startup.extend_from_slice(&body);
    c.write_all(&startup).await?;
    let frames = read_to_ready(&mut c).await?;
    assert!(
        frames.iter().any(|(t, _)| *t == b'Z'),
        "proxy did not admit the session: {:?}",
        frames
            .iter()
            .filter(|(t, _)| *t == b'E')
            .map(|(_, b)| String::from_utf8_lossy(b).to_string())
            .collect::<Vec<_>>()
    );
    Ok(c)
}

/// Read frames up to and including ReadyForQuery.
async fn read_to_ready(c: &mut TcpStream) -> io::Result<Vec<(u8, Vec<u8>)>> {
    let mut acc = Vec::new();
    let mut cursor = 0usize;
    let mut frames = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), c.read(&mut buf))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "pg read"))??;
        if n == 0 {
            return Ok(frames);
        }
        acc.extend_from_slice(&buf[..n]);
        while acc.len() >= cursor + 5 {
            let tag = acc[cursor];
            let len = u32::from_be_bytes(acc[cursor + 1..cursor + 5].try_into().unwrap()) as usize;
            if len < 4 || acc.len() < cursor + 1 + len {
                break;
            }
            frames.push((tag, acc[cursor + 5..cursor + 1 + len].to_vec()));
            cursor += 1 + len;
            if tag == b'Z' {
                return Ok(frames);
            }
        }
    }
}

fn first_row_text(body: &[u8]) -> Option<String> {
    // DataRow: int16 column count, then int32 length + bytes per column.
    let ncols = i16::from_be_bytes(body[..2].try_into().ok()?);
    if ncols < 1 {
        return None;
    }
    let len = i32::from_be_bytes(body[2..6].try_into().ok()?);
    if len < 0 {
        return None;
    }
    Some(String::from_utf8_lossy(&body[6..6 + len as usize]).to_string())
}

async fn exec(c: &mut TcpStream, sql: &str) -> Reply {
    let mut frame = vec![b'Q'];
    frame.extend_from_slice(&((sql.len() + 5) as u32).to_be_bytes());
    frame.extend_from_slice(sql.as_bytes());
    frame.push(0);
    c.write_all(&frame).await.expect("send query");
    let frames = read_to_ready(c).await.expect("read query reply");
    let mut rows = Vec::new();
    let mut error = None;
    for (tag, body) in frames {
        match tag {
            b'D' => rows.extend(first_row_text(&body)),
            b'E' => error = Some(String::from_utf8_lossy(&body).to_string()),
            _ => {}
        }
    }
    Reply { rows, error }
}

async fn terminate(c: TcpStream) {
    let mut c = c;
    let _ = c.write_all(&[b'X', 0, 0, 0, 4]).await;
    let _ = c.shutdown().await;
    // Let the proxy run its return-to-pool path.
    tokio::time::sleep(Duration::from_millis(120)).await;
}

async fn backend_pid(c: &mut TcpStream) -> String {
    let reply = exec(c, "SELECT pg_backend_pid()").await;
    assert!(reply.error.is_none(), "pid query failed: {:?}", reply.error);
    reply.rows.first().cloned().expect("pid row")
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn sessions_of_one_binding_share_the_backend_and_reset_state() {
    let Some(pg) = start_pg() else {
        eprintln!("SKIP: no initdb/pg_ctl (set VRT_TEST_PG_BIN) — real-PG pool test not run");
        return;
    };
    let dir = pg.root.clone();
    let (addr, _proxy) = start_proxy(&dir, pg.port, &["team-a"], default_pool(4, 2_000)).await;

    // Session 1 dirties the session and records the backend pid.
    let mut a = connect(addr, "team-a").await.expect("session 1");
    let pid_a = backend_pid(&mut a).await;
    let dirty = exec(
        &mut a,
        "CREATE TEMP TABLE pool_leak(x int); SET application_name = 'leak-me'",
    )
    .await;
    assert!(
        dirty.error.is_none(),
        "dirty session failed: {:?}",
        dirty.error
    );
    terminate(a).await;

    // Session 2 must continue on the SAME backend connection...
    let mut b = connect(addr, "team-a").await.expect("session 2");
    let pid_b = backend_pid(&mut b).await;
    assert_eq!(pid_a, pid_b, "the pooled session reused one backend PID");

    // ...with none of the previous session's state.
    let state = exec(
        &mut b,
        "SELECT (SELECT count(*) FROM pg_class WHERE relname = 'pool_leak')::text \
         || '|' || current_setting('application_name')",
    )
    .await;
    assert!(
        state.error.is_none(),
        "state query failed: {:?}",
        state.error
    );
    let value = state.rows.first().cloned().expect("state row");
    let (tables, app_name) = value.split_once('|').expect("state shape");
    assert_eq!(tables, "0", "DISCARD ALL dropped the temp table: {value}");
    assert_ne!(app_name, "leak-me", "DISCARD ALL reset the GUC: {value}");
    terminate(b).await;
}

#[tokio::test]
async fn the_session_after_max_connections_is_queued_then_denied() {
    let Some(pg) = start_pg() else {
        eprintln!("SKIP: no initdb/pg_ctl (set VRT_TEST_PG_BIN) — real-PG pool test not run");
        return;
    };
    let dir = pg.root.clone();
    let (addr, _proxy) = start_proxy(&dir, pg.port, &["team-a"], default_pool(1, 400)).await;

    // Session 1 holds the only backend connection.
    let mut a = connect(addr, "team-a").await.expect("holder");
    backend_pid(&mut a).await;

    let started = Instant::now();
    let mut b = TcpStream::connect(addr).await.expect("denied client");
    let label = synthetic_host("team-a");
    let label = label.split('.').next().unwrap().to_string();
    b.write_all(format!("VRT-PG1 {label}\n").as_bytes())
        .await
        .unwrap();
    let mut body = Vec::new();
    for kv in ["user", "vrt-hd", "database", "vrt"] {
        body.extend_from_slice(kv.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut startup = Vec::new();
    startup.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
    startup.extend_from_slice(&PROTO_V3.to_be_bytes());
    startup.extend_from_slice(&body);
    b.write_all(&startup).await.unwrap();
    let frames = read_to_ready(&mut b).await.expect("deny reply");
    let elapsed = started.elapsed();
    let error: Vec<u8> = frames
        .iter()
        .filter(|(t, _)| *t == b'E')
        .flat_map(|(_, body)| body.clone())
        .collect();
    let text = String::from_utf8_lossy(&error);
    assert!(
        text.contains("hyperdrive: connection limit reached"),
        "want the typed pool denial, got {text:?}"
    );
    assert!(text.contains("53300"), "want SQLSTATE 53300: {text:?}");
    assert!(
        elapsed >= Duration::from_millis(300),
        "the denial must come from the bounded queue, not an instant refusal ({elapsed:?})"
    );
    terminate(a).await;
}

#[tokio::test]
async fn a_second_tenant_never_inherits_the_first_tenants_session() {
    let Some(pg) = start_pg() else {
        eprintln!("SKIP: no initdb/pg_ctl (set VRT_TEST_PG_BIN) — real-PG pool test not run");
        return;
    };
    let dir = pg.root.clone();
    let (addr, _proxy) =
        start_proxy(&dir, pg.port, &["team-a", "team-b"], default_pool(4, 2_000)).await;

    let mut a = connect(addr, "team-a").await.expect("tenant a");
    let pid_a = backend_pid(&mut a).await;
    let dirty = exec(&mut a, "SET application_name = 'leak-b'").await;
    assert!(dirty.error.is_none());
    terminate(a).await;

    let mut b = connect(addr, "team-b").await.expect("tenant b");
    let pid_b = backend_pid(&mut b).await;
    assert_ne!(pid_a, pid_b, "tenants never share a backend connection");
    let app = exec(&mut b, "SELECT current_setting('application_name')").await;
    assert!(app.error.is_none());
    assert_ne!(
        app.rows.first().cloned().unwrap_or_default(),
        "leak-b",
        "tenant A's session state must not reach tenant B"
    );
    terminate(b).await;
}
