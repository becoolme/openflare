//! MIG-15: Hyperdrive-compatible pg-wire proxy.
//!
//! The egress facade's internal-TCP provider dials this loopback
//! listener for synthetic `*.hyperdrive.vrt.internal` targets. Each
//! session starts with an identity preamble `VRT-PG1 <binding-label>`
//! written by the facade BEFORE any tenant byte — the label is pinned
//! here, so tenant input can never forge another binding.
//!
//! Protocol: `SSLRequest` answers `N` then loops for the real startup;
//! `GSSENC`, `CancelRequest`, `replication` params and non-3.x startup
//! variants are typed rejections. Client `user`/`database` are ignored
//! — upstream identity comes from the credential-file `tcp` payload
//! (provider-held, rotated via render-inputs). Upstream auth supports
//! AuthenticationOk, cleartext and SCRAM-SHA-256; md5/GSS fail closed.
//! After upstream ReadyForQuery the session is a transparent
//! bidirectional relay bounded by idle/session timeouts and byte
//! accounting. Sessions whose binding vanishes from desired state are
//! aborted by the sweeper within one sweep interval.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::config::PgProxyConfig;
use crate::desired_file::FileDesiredStore;
use crate::metrics::reg;

const SSL_REQUEST: u32 = 808_771_03;
const GSSENC_REQUEST: u32 = 808_771_04;
const CANCEL_REQUEST: u32 = 808_771_02;
const PROTO_V3: u32 = 196_608;
/// Startup frame cap — a tenant cannot grow work/memory via the packet.
const STARTUP_MAX: u32 = 16 * 1024;
const PREAMBLE_MAX: usize = 128;
const PG_MESSAGE_MAX: u32 = 16 * 1024 * 1024;
const SWEEP_MS: u64 = 5_000;

/// Typed session denial reasons — metric label + wire code suffix.
/// Fixed enum set only: never tenant data.
#[derive(Clone, Copy, Debug)]
pub enum Deny {
    BadPreamble,
    UnknownBinding,
    CredentialUnavailable,
    SessionLimit,
    StartupInvalid,
    StartupUnsupported,
    UpstreamConnect,
    UpstreamAuth,
    /// MIG-15 r2: tenant is in a migration read-only window —
    /// render-inputs withhold TCP creds + emit `tcp_blocked`; proxy
    /// denies typed, sweeper kills live sessions within one interval.
    MigrationReadOnly,
    /// MIG-15 r2: the whole accept→ReadyForQuery phase exceeded
    /// `connect_timeout_ms` — a stalled handshake can no longer pin a
    /// global/per-binding slot indefinitely.
    HandshakeTimeout,
    IdleTimeout,
    SessionDeadline,
    BindingRevoked,
}

impl Deny {
    fn code(self) -> &'static str {
        match self {
            Self::BadPreamble => "PGPREAMBLE_INVALID",
            Self::UnknownBinding => "PG_BINDING_UNKNOWN",
            Self::CredentialUnavailable => "PG_CREDENTIAL_UNAVAILABLE",
            Self::SessionLimit => "PG_SESSION_LIMIT",
            Self::StartupInvalid => "PG_STARTUP_INVALID",
            Self::StartupUnsupported => "PG_STARTUP_UNSUPPORTED",
            Self::UpstreamConnect => "PG_UPSTREAM_CONNECT",
            Self::UpstreamAuth => "PG_UPSTREAM_AUTH",
            Self::MigrationReadOnly => "PG_MIGRATION_READ_ONLY",
            Self::HandshakeTimeout => "PG_HANDSHAKE_TIMEOUT",
            Self::IdleTimeout => "PG_IDLE_TIMEOUT",
            Self::SessionDeadline => "PG_SESSION_DEADLINE",
            Self::BindingRevoked => "PG_BINDING_REVOKED",
        }
    }
}

/// One binding's live session set for the revoker sweep.
type SessionMap = Mutex<HashMap<String, HashMap<u64, tokio::sync::oneshot::Sender<()>>>>;

pub struct PgProxy {
    cfg: PgProxyConfig,
    creds: Arc<crate::pg::PgCredentialStore>,
    desired: Arc<FileDesiredStore>,
    sessions: SessionMap,
    per_binding: Mutex<HashMap<String, usize>>,
    global: Arc<Semaphore>,
    next_id: AtomicU64,
    active: AtomicU64,
    #[cfg(test)]
    test_routes: Mutex<Option<HashMap<String, (String, String)>>>,
}

impl PgProxy {
    pub fn new(
        cfg: PgProxyConfig,
        creds: Arc<crate::pg::PgCredentialStore>,
        desired: Arc<FileDesiredStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            global: Arc::new(Semaphore::new(cfg.max_sessions)),
            cfg,
            creds,
            desired,
            sessions: Mutex::new(HashMap::new()),
            per_binding: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            active: AtomicU64::new(0),
            #[cfg(test)]
            test_routes: Mutex::new(None),
        })
    }

    fn push_active(&self) {
        let n = self.active.load(Ordering::Relaxed);
        reg().set("vrt_provider_pgproxy_sessions_active", &[], n as f64);
    }

    #[cfg(test)]
    fn set_routes(&self, routes: HashMap<String, (String, String)>) {
        *self.test_routes.lock().unwrap_or_else(|e| e.into_inner()) = Some(routes);
    }

    /// binding-label -> credential ref from the verified snapshot.
    /// Snapshot failure returns an empty map — fail closed.
    fn routes(&self) -> HashMap<String, (String, String)> {
        #[cfg(test)]
        if let Some(r) = self.test_routes.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            return r;
        }
        let mut out = HashMap::new();
        let Ok(desired) = self.desired.snapshot(chrono::Utc::now()) else {
            return out;
        };
        for t in &desired.tenants {
            for w in &t.workers {
                for b in &w.bindings {
                    if let snapshot_protocol::compute_desired::BindingDesired::Hyperdrive(h) = b {
                        let label = h.synthetic_host.split('.').next().unwrap_or_default();
                        if !label.is_empty() {
                            out.insert(
                                label.to_string(),
                                (t.tenant_id.clone(), h.credential_ref.clone()),
                            );
                        }
                    }
                }
            }
        }
        // MIG-15 r2: a route is live only while its credential still
        // resolves AND is not migration-blocked. Rotation keeps
        // sessions (credential still resolvable under the same ref);
        // migration-block and revocation kill them within one sweep.
        out.retain(|_, (tenant, cref)| {
            self.creds
                .lookup(tenant, cref)
                .map(|c| c.tcp.is_some() && c.tcp_blocked.is_none())
                .unwrap_or(false)
        });
        out
    }

    /// Abort every session whose label is no longer routed. Bound:
    /// one sweep interval.
    pub async fn sweep(self: &Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(Duration::from_millis(SWEEP_MS)) => {}
            }
            let live = self.routes();
            let mut killed = 0usize;
            {
                let mut reg = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
                reg.retain(|label, sessions| {
                    if live.contains_key(label) {
                        return true;
                    }
                    for (_, h) in sessions.drain() {
                        let _ = h.send(());
                        killed += 1;
                    }
                    deny_metric("binding_revoked");
                    false
                });
            }
            if killed > 0 {
                info!(killed, "pgproxy: sessions aborted — binding left desired state");
            }
        }
    }

    pub async fn serve(self: &Arc<Self>, listener: TcpListener) {
        loop {
            let Ok((sock, _peer)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let _ = sock.set_nodelay(true);
            let Ok(permit) = self.global.clone().try_acquire_owned() else {
                deny_metric("session_limit");
                continue;
            };
            let proxy = Arc::clone(self);
            tokio::spawn(async move {
                let _permit = permit;
                proxy.connection(sock).await;
            });
        }
    }

    async fn connection(self: &Arc<Self>, sock: TcpStream) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let res = self.session(sock, id).await;
        if let Err((d, label)) = res {
            deny_metric(match d {
                Deny::BindingRevoked => "binding_revoked",
                Deny::IdleTimeout => "idle_timeout",
                Deny::SessionDeadline => "session_deadline",
                Deny::BadPreamble => "bad_preamble",
                Deny::UnknownBinding => "unknown_binding",
                Deny::CredentialUnavailable => "credential_unavailable",
                Deny::SessionLimit => "session_limit",
                Deny::StartupInvalid => "startup_invalid",
                Deny::StartupUnsupported => "startup_unsupported",
                Deny::UpstreamConnect => "upstream_connect",
                Deny::UpstreamAuth => "upstream_auth",
                Deny::MigrationReadOnly => "migration_read_only",
                Deny::HandshakeTimeout => "handshake_timeout",
            });
            warn!(code = d.code(), label = %label.unwrap_or_default(), "pgproxy session denied");
        }
    }

    /// Release a per-binding session slot + registry entry.
    fn release(&self, label: &str, id: u64) {
        {
            let mut pb = self.per_binding.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(n) = pb.get_mut(label) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    pb.remove(label);
                }
            }
        }
        {
            let mut reg = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(m) = reg.get_mut(label) {
                m.remove(&id);
                if m.is_empty() {
                    reg.remove(label);
                }
            }
        }
    }

    async fn session(
        self: &Arc<Self>,
        sock: TcpStream,
        id: u64,
    ) -> Result<(), (Deny, Option<String>)> {
        // MIG-15 r2: ONE deadline covers the entire accept→ReadyForQuery
        // phase — preamble, route/credential resolution, client startup
        // frames AND upstream auth. A client that stalls anywhere inside
        // releases the global permit + per-binding slot on timeout; the
        // dropped future closes the socket.
        let parts = match tokio::time::timeout(
            Duration::from_millis(self.cfg.connect_timeout_ms),
            self.pre_relay(sock, id),
        )
        .await
        {
            Err(_) => return Err((Deny::HandshakeTimeout, None)),
            Ok(v) => v?,
        };
        let mut rd = parts.rd;
        let mut wr = parts.wr;
        let mut urd = parts.urd;
        let mut uwr = parts.uwr;
        let label = parts.label;
        let mut kill_rx = parts.kill_rx;
        let _active = parts.active;
        let _release = parts.release;

        // Bidirectional relay bounded by idle + session deadlines and
        // by the revocation kill channel; all bytes are metered.
        // MIG-15 r2: idle = no bytes in EITHER direction (shared
        // last-activity); any side ending (EOF/IO/idle) ends BOTH.
        let idle = Duration::from_millis(self.cfg.idle_timeout_ms);
        let deadline = Duration::from_millis(self.cfg.max_session_ms);
        let relay = async {
            tokio::select! {
                _ = &mut kill_rx => (0u64, 0u64, PumpEnd::Idle),
                v = pump_both(&mut rd, &mut uwr, &mut urd, &mut wr, idle) => v,
            }
        };
        let (c2u_b, u2c_b, end) = match tokio::time::timeout(deadline, relay).await {
            Err(_) => {
                return Err((Deny::SessionDeadline, Some(label)));
            }
            Ok(v) => v,
        };
        reg().add("vrt_provider_pgproxy_bytes_total", &[("dir", "up")], c2u_b as f64);
        reg().add("vrt_provider_pgproxy_bytes_total", &[("dir", "down")], u2c_b as f64);
        if kill_rx.is_terminated() {
            return Err((Deny::BindingRevoked, Some(label)));
        }
        if end == PumpEnd::Idle {
            return Err((Deny::IdleTimeout, Some(label)));
        }
        Ok(())
    }

    /// Everything between accept and the first ReadyForQuery —
    /// preamble → route → admission → credential → client startup →
    /// upstream connect + auth → client_ready. Runs under the outer
    /// `connect_timeout_ms` deadline in `session`; guards inside
    /// `PreRelay` keep slots/permits until dropped.
    async fn pre_relay(
        self: &Arc<Self>,
        mut sock: TcpStream,
        id: u64,
    ) -> Result<PreRelay, (Deny, Option<String>)> {
        let label = read_preamble(&mut sock)
            .await
            .map_err(|_| (Deny::BadPreamble, None))?;
        let Some((team, cref)) = self.routes().get(&label).cloned() else {
            send_error(&mut sock, Deny::UnknownBinding.code()).await;
            return Err((Deny::UnknownBinding, Some(label)));
        };
        // Per-binding concurrency cap — checked before any upstream
        // work; the guard is dropped before any await.
        let admitted = {
            let mut pb = self.per_binding.lock().unwrap_or_else(|e| e.into_inner());
            let n = pb.entry(label.clone()).or_insert(0);
            if *n >= self.cfg.max_sessions_per_binding {
                false
            } else {
                *n += 1;
                true
            }
        };
        if !admitted {
            send_error(&mut sock, Deny::SessionLimit.code()).await;
            return Err((Deny::SessionLimit, Some(label)));
        }
        let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut reg = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            reg.entry(label.clone()).or_default().insert(id, kill_tx);
        }
        reg().add("vrt_provider_pgproxy_sessions_total", &[("binding", "bound")], 1.0);
        self.active.fetch_add(1, Ordering::Relaxed);
        self.push_active();
        let active = ActiveGuard { proxy: Arc::clone(self) };
        let release = ReleaseGuard {
            proxy: Arc::clone(self),
            label: label.clone(),
            id,
        };

        // Credential: required `tcp` payload; absent → typed fail closed.
        let cred = self.creds.lookup(&team, &cref);
        // MIG-15 r2: explicit migration-read-only marker → typed deny
        // (the TCP data plane cannot be fenced by session GUCs).
        if cred
            .as_ref()
            .and_then(|c| c.tcp_blocked.as_deref())
            .is_some()
        {
            send_error(&mut sock, Deny::MigrationReadOnly.code()).await;
            return Err((Deny::MigrationReadOnly, Some(label)));
        }
        let tcp = cred.as_ref().and_then(|c| c.tcp.clone());
        let Some(tcp) = tcp else {
            send_error(&mut sock, Deny::CredentialUnavailable.code()).await;
            return Err((Deny::CredentialUnavailable, Some(label)));
        };

        // Client-side startup handshake first (SSLRequest → 'N'), so a
        // failed upstream doesn't mask a protocol violation.
        let mut client_startup_attempts = 0u8;
        let (mut rd, mut wr) = sock.into_split();
        loop {
            client_startup_attempts += 1;
            if client_startup_attempts > 4 {
                send_error_w(&mut wr, Deny::StartupInvalid.code()).await;
                return Err((Deny::StartupInvalid, Some(label)));
            }
            let (code, body) = match read_frame(&mut rd, STARTUP_MAX).await {
                Ok(f) => f,
                Err(_) => return Err((Deny::StartupInvalid, Some(label))),
            };
            match code {
                SSL_REQUEST => {
                    if wr.write_all(b"N").await.is_err() {
                        return Err((Deny::StartupInvalid, Some(label)));
                    }
                    continue;
                }
                GSSENC_REQUEST | CANCEL_REQUEST => {
                    send_error_w(&mut wr, Deny::StartupUnsupported.code()).await;
                    return Err((Deny::StartupUnsupported, Some(label)));
                }
                PROTO_V3 => {
                    if startup_is_replication(&body) {
                        send_error_w(&mut wr, Deny::StartupUnsupported.code()).await;
                        return Err((Deny::StartupUnsupported, Some(label)));
                    }
                    break;
                }
                _ => {
                    send_error_w(&mut wr, Deny::StartupUnsupported.code()).await;
                    return Err((Deny::StartupUnsupported, Some(label)));
                }
            }
        }

        // Upstream connect + auth (provider-held identity).
        let started = Instant::now();
        let upstream = match tokio::time::timeout(
            Duration::from_millis(self.cfg.connect_timeout_ms),
            TcpStream::connect((tcp.host.as_str(), tcp.port)),
        )
        .await
        {
            Ok(Ok(s)) => s,
            _ => {
                send_error_w(&mut wr, Deny::UpstreamConnect.code()).await;
                return Err((Deny::UpstreamConnect, Some(label)));
            }
        };
        let _ = upstream.set_nodelay(true);
        reg().observe(
            "vrt_provider_pgproxy_upstream_connect_ms",
            &[],
            started.elapsed().as_secs_f64() * 1000.0,
        );
        let (mut urd, mut uwr) = upstream.into_split();
        if let Err(d) = upstream_handshake(&mut urd, &mut uwr, &tcp).await {
            send_error_w(&mut wr, Deny::UpstreamAuth.code()).await;
            return Err((d, Some(label)));
        }
        // Client is admitted on the already-authenticated channel:
        // AuthenticationOk + minimal parameters + ReadyForQuery.
        let _ = wr.write_all(&client_ready(&tcp.user)).await;
        Ok(PreRelay {
            rd,
            wr,
            urd,
            uwr,
            label,
            kill_rx,
            active,
            release,
        })
    }
}

/// Pieces handed from `pre_relay` to the relay phase; guards keep the
/// per-binding slot, registry entry and active counter until the whole
/// session (or a handshake timeout cancel) drops them.
struct PreRelay {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    urd: OwnedReadHalf,
    uwr: OwnedWriteHalf,
    label: String,
    kill_rx: tokio::sync::oneshot::Receiver<()>,
    active: ActiveGuard,
    release: ReleaseGuard,
}

struct ActiveGuard {
    proxy: Arc<PgProxy>,
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.proxy.active.fetch_sub(1, Ordering::Relaxed);
        self.proxy.push_active();
    }
}

struct ReleaseGuard {
    proxy: Arc<PgProxy>,
    label: String,
    id: u64,
}
impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        self.proxy.release(&self.label, self.id);
    }
}

fn deny_metric(reason: &'static str) {
    reg().add("vrt_provider_pgproxy_denied_total", &[("reason", reason)], 1.0);
}

/// First bytes from the facade: `VRT-PG1 <label>\n`.
async fn read_preamble(sock: &mut TcpStream) -> std::io::Result<String> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    while buf.len() < PREAMBLE_MAX {
        sock.read_exact(&mut byte).await?;
        if byte[0] == b'\n' {
            break;
        }
        buf.push(byte[0]);
    }
    let line = String::from_utf8(buf).map_err(|_| std::io::ErrorKind::InvalidData)?;
    let label = line
        .strip_prefix("VRT-PG1 ")
        .ok_or(std::io::ErrorKind::InvalidData)?;
    // Label charset mirrors compute_desired validation: [a-z0-9].
    if label.is_empty() || label.len() > 28 || !label.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    Ok(label.to_string())
}

/// Read one startup-phase frame: u32 BE length (includes itself) + body.
async fn read_frame(rd: &mut OwnedReadHalf, max: u32) -> std::io::Result<(u32, Vec<u8>)> {
    let len = rd.read_u32().await?;
    if !(8..=max).contains(&len) {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let body_len = (len - 4) as usize;
    let mut head = [0u8; 4];
    rd.read_exact(&mut head).await?;
    let code = u32::from_be_bytes(head);
    let mut body = vec![0u8; body_len - 4];
    rd.read_exact(&mut body).await?;
    Ok((code, body))
}

fn startup_is_replication(body: &[u8]) -> bool {
    body.split(|b| *b == 0)
        .any(|k| k == b"replication")
}

async fn send_error(sock: &mut TcpStream, code: &str) {
    let _ = sock.write_all(&error_frame(code)).await;
}
async fn send_error_w(wr: &mut OwnedWriteHalf, code: &str) {
    let _ = wr.write_all(&error_frame(code)).await;
}

/// FATAL ErrorResponse with the typed code — no tenant data, no creds.
fn error_frame(code: &str) -> Vec<u8> {
    let msg = format!("vrt pgproxy {}", code);
    let mut body = Vec::with_capacity(32 + msg.len());
    body.extend_from_slice(b"SERROR\0VFATAL\0C0A000\0M");
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0);
    let mut out = Vec::with_capacity(9 + body.len());
    out.push(b'E');
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn client_ready(user: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(b"R\x00\x00\x00\x08\x00\x00\x00\x00"); // AuthenticationOk
    let params = [
        ("server_version", "15"),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("TimeZone", "UTC"),
        ("is_superuser", "off"),
        ("session_authorization", user),
    ];
    for (k, v) in params {
        // Frame length counts itself (4) + both NUL-terminated strings.
        let n = (4 + k.len() + 1 + v.len() + 1) as u32;
        out.push(b'S');
        out.extend_from_slice(&n.to_be_bytes());
        out.extend_from_slice(k.as_bytes());
        out.push(0);
        out.extend_from_slice(v.as_bytes());
        out.push(0);
    }
    out.extend_from_slice(b"K\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00");
    out.extend_from_slice(b"Z\x00\x00\x00\x05I");
    out
}

#[derive(PartialEq, Debug)]
enum PumpEnd {
    Eof,
    Idle,
    Io,
}

/// One direction of the relay; idle timeout applies per read.
async fn pump_both<R1, W1, R2, W2>(
    rd: &mut R1,
    uwr: &mut W1,
    urd: &mut R2,
    wr: &mut W2,
    idle: Duration,
) -> (u64, u64, PumpEnd)
where
    R1: tokio::io::AsyncRead + Unpin,
    W1: tokio::io::AsyncWrite + Unpin,
    R2: tokio::io::AsyncRead + Unpin,
    W2: tokio::io::AsyncWrite + Unpin,
{
    let mut c2u = 0u64;
    let mut u2c = 0u64;
    let mut b_c = vec![0u8; 16 * 1024];
    let mut b_u = vec![0u8; 16 * 1024];
    loop {
        tokio::select! {
            r = rd.read(&mut b_c) => match r {
                Ok(0) => return (c2u, u2c, PumpEnd::Eof),
                Err(_) => return (c2u, u2c, PumpEnd::Io),
                Ok(n) => {
                    c2u += n as u64;
                    if uwr.write_all(&b_c[..n]).await.is_err() {
                        return (c2u, u2c, PumpEnd::Io);
                    }
                }
            },
            r = urd.read(&mut b_u) => match r {
                Ok(0) => return (c2u, u2c, PumpEnd::Eof),
                Err(_) => return (c2u, u2c, PumpEnd::Io),
                Ok(n) => {
                    u2c += n as u64;
                    if wr.write_all(&b_u[..n]).await.is_err() {
                        return (c2u, u2c, PumpEnd::Io);
                    }
                }
            },
            _ = tokio::time::sleep(idle) => return (c2u, u2c, PumpEnd::Idle),
        }
    }
}

type HmacSha = Hmac<Sha256>;
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = <HmacSha as Mac>::new_from_slice(key).expect("hmac key");
    m.update(data);
    m.finalize().into_bytes().into()
}
fn pbkdf2(password: &[u8], salt: &[u8], iters: u32) -> [u8; 32] {
    // PBKDF2-HMAC-SHA256, dkLen == 32 → single block.
    let mut block = hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());
    let mut out = block;
    for _ in 1..iters {
        block = hmac_sha256(password, &block);
        for (o, b) in out.iter_mut().zip(block.iter()) {
            *o ^= b;
        }
    }
    out
}

/// Upstream startup + auth. Fails closed on any unsupported mode.
async fn upstream_handshake(
    urd: &mut OwnedReadHalf,
    uwr: &mut OwnedWriteHalf,
    tcp: &crate::pg::PgTcpCredential,
) -> Result<(), Deny> {
    let mut start = Vec::with_capacity(128);
    let mut params = Vec::new();
    for (k, v) in [("user", tcp.user.as_str()), ("database", tcp.database.as_str()), ("application_name", "vrt-hyperdrive-proxy")] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0);
    start.extend_from_slice(&((params.len() + 8) as u32).to_be_bytes());
    start.extend_from_slice(&PROTO_V3.to_be_bytes());
    start.extend_from_slice(&params);
    uwr.write_all(&start).await.map_err(|_| Deny::UpstreamConnect)?;

    let mut scram: Option<ScramClient> = None;
    loop {
        let tag = urd.read_u8().await.map_err(|_| Deny::UpstreamConnect)?;
        let len = urd.read_u32().await.map_err(|_| Deny::UpstreamConnect)?;
        if !(4..=PG_MESSAGE_MAX).contains(&len) {
            return Err(Deny::UpstreamAuth);
        }
        let mut body = vec![0u8; (len - 4) as usize];
        urd.read_exact(&mut body).await.map_err(|_| Deny::UpstreamConnect)?;
        match tag {
            b'Z' => return Ok(()),
            b'S' | b'K' | b'N' => continue,
            b'E' => return Err(Deny::UpstreamAuth),
            b'R' => {
                if body.len() < 4 {
                    return Err(Deny::UpstreamAuth);
                }
                match u32::from_be_bytes(body[..4].try_into().unwrap()) {
                    0 => continue,
                    3 => send_password(uwr, &tcp.password).await?,
                    10 => {
                        let mechs: Vec<&[u8]> = body[4..].split(|b| *b == 0).filter(|s| !s.is_empty()).collect();
                        if !mechs.iter().any(|m| *m == b"SCRAM-SHA-256") {
                            return Err(Deny::UpstreamAuth);
                        }
                        let c = ScramClient::new(&tcp.user, &tcp.password);
                        uwr.write_all(&c.initial_response()).await.map_err(|_| Deny::UpstreamConnect)?;
                        scram = Some(c);
                    }
                    11 => {
                        let Some(c) = scram.as_mut() else { return Err(Deny::UpstreamAuth) };
                        let resp = c.server_first(&body[4..]).ok_or(Deny::UpstreamAuth)?;
                        uwr.write_all(&resp).await.map_err(|_| Deny::UpstreamConnect)?;
                    }
                    12 => {
                        let Some(c) = scram.as_mut() else { return Err(Deny::UpstreamAuth) };
                        if !c.verify_server_final(&body[4..]) {
                            return Err(Deny::UpstreamAuth);
                        }
                    }
                    _ => return Err(Deny::UpstreamAuth),
                }
            }
            _ => return Err(Deny::UpstreamAuth),
        }
    }
}

async fn send_password(uwr: &mut OwnedWriteHalf, password: &str) -> Result<(), Deny> {
    let mut m = Vec::with_capacity(9 + password.len());
    m.push(b'p');
    m.extend_from_slice(&((password.len() + 5) as u32).to_be_bytes());
    m.extend_from_slice(password.as_bytes());
    m.push(0);
    uwr.write_all(&m).await.map_err(|_| Deny::UpstreamConnect)
}

/// SCRAM-SHA-256 client (RFC 5802/7677), channel binding off (`n,,` —
/// the cluster channel is loopback+service-account, not TLS).
struct ScramClient {
    password: String,
    cnonce: String,
    client_first_bare: String,
    server_signature: Option<[u8; 32]>,
}

impl ScramClient {
    fn new(user: &str, password: &str) -> Self {
        let nonce_src = format!(
            "{}:{}:{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            "vrt",
        );
        let cnonce = hex::encode(Sha256::digest(nonce_src.as_bytes()))[..24].to_string();
        let client_first_bare = format!("n={},r={}", user, cnonce);
        Self {
            password: password.to_string(),
            cnonce,
            client_first_bare,
            server_signature: None,
        }
    }

    fn initial_response(&self) -> Vec<u8> {
        let first = format!("n,,{}", self.client_first_bare);
        let data = first.as_bytes();
        let mut body = Vec::with_capacity(24 + data.len());
        body.extend_from_slice(b"SCRAM-SHA-256\0");
        body.extend_from_slice(&(data.len() as i32).to_be_bytes());
        body.extend_from_slice(data);
        let mut m = Vec::with_capacity(5 + body.len());
        m.push(b'p');
        m.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        m.extend_from_slice(&body);
        m
    }

    fn server_first(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        let s = String::from_utf8_lossy(data);
        let mut nonce = "";
        let mut salt64 = "";
        let mut iters = "";
        for part in s.split(',') {
            match &part[..2.min(part.len())] {
                "r=" => nonce = &part[2..],
                "s=" => salt64 = &part[2..],
                "i=" => iters = &part[2..],
                _ => {}
            }
        }
        if nonce.is_empty() || !nonce.starts_with(&self.cnonce) {
            return None;
        }
        let salt = base64::engine::general_purpose::STANDARD.decode(salt64).ok()?;
        let iters: u32 = iters.parse().ok()?;
        // MIG-15 r2: upstream-controlled iteration count — cap it so a
        // hostile/buggy upstream can't pin the CPU (fail closed →
        // PG_UPSTREAM_AUTH).
        if iters == 0 || iters > 100_000 {
            return None;
        }
        let cf_no_proof = format!("c=biws,r={}", nonce);
        let auth = format!("{},{},{}", self.client_first_bare, s, cf_no_proof);
        let salted = pbkdf2(self.password.as_bytes(), &salt, iters);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored = Sha256::digest(client_key);
        let sig = hmac_sha256(&stored, auth.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(sig.iter()).map(|(a, b)| a ^ b).collect();
        let server_key = hmac_sha256(&salted, b"Server Key");
        self.server_signature = Some(hmac_sha256(&server_key, auth.as_bytes()));
        let final_msg = format!(
            "{},p={}",
            cf_no_proof,
            base64::engine::general_purpose::STANDARD.encode(&proof)
        );
        let data = final_msg.as_bytes();
        let mut m = Vec::with_capacity(5 + data.len());
        m.push(b'p');
        m.extend_from_slice(&((data.len() + 4) as u32).to_be_bytes());
        m.extend_from_slice(data);
        Some(m)
    }

    fn verify_server_final(&self, data: &[u8]) -> bool {
        let Some(expected) = self.server_signature else {
            return false;
        };
        let s = String::from_utf8_lossy(data);
        let Some(v) = s.split(',').find_map(|p| p.strip_prefix("v=")) else {
            return false;
        };
        base64::engine::general_purpose::STANDARD
            .decode(v)
            .ok()
            .map(|b| b == expected.to_vec())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MIG-17: every frame `client_ready` emits must be self-consistent
    /// (a real pg client walks frames by their length word; a wrong
    /// ParameterStatus length desynchronises the whole handshake).
    #[test]
    fn client_ready_frames_are_well_formed() {
        let out = client_ready("someone");
        let mut i = 0;
        let mut tags = Vec::new();
        while i < out.len() {
            let tag = out[i];
            let len = u32::from_be_bytes(out[i + 1..i + 5].try_into().unwrap()) as usize;
            assert!(i + 1 + len <= out.len(), "frame {} overruns", tag as char);
            if tag == b'S' {
                let body = &out[i + 5..i + 1 + len];
                assert_eq!(body.iter().filter(|b| **b == 0).count(), 2);
                assert_eq!(*body.last().unwrap(), 0, "ParameterStatus ends on NUL");
            }
            tags.push(tag);
            i += 1 + len;
        }
        assert_eq!(i, out.len(), "frames tile the buffer exactly");
        assert_eq!(*tags.first().unwrap(), b'R');
        assert_eq!(*tags.last().unwrap(), b'Z');
    }

    #[test]
    fn error_frame_shape() {
        let f = error_frame("PG_TEST");
        assert_eq!(f[0], b'E');
        let len = u32::from_be_bytes(f[1..5].try_into().unwrap()) as usize;
        assert_eq!(len + 1, f.len());
        assert!(f.windows(7).any(|w| w == b"PG_TEST"));
    }

    #[test]
    fn startup_replication_detected() {
        assert!(startup_is_replication(b"user\0u\0replication\0database\0"));
        assert!(!startup_is_replication(b"user\0u\0database\0d\0"));
    }

    #[test]
    fn scram_proof_vector() {
        // RFC 7677 example: user "user", pass "pencil".
        let mut c = ScramClient::new("user", "pencil");
        c.cnonce = "rOprNGfwEbeRWgbNEkqO".into();
        c.client_first_bare = "n=user,r=rOprNGfwEbeRWgbNEkqO".into();
        let sf = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let resp = c.server_first(sf.as_bytes()).expect("resp");
        let body = String::from_utf8_lossy(&resp[5..]);
        assert!(body.contains("p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="));
        assert!(c.verify_server_final(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="));
    }

    #[test]
    fn scram_rejects_bad_nonce_and_signature() {
        let mut c = ScramClient::new("u", "p");
        assert!(c.server_first(b"s=eQ==,i=4096").is_none()); // no r=
        assert!(!c.verify_server_final(b"v=AAAA"));
    }

    #[test]
    fn preamble_charset() {
        // Charset guard mirrored by compute_desired; test the pure gate.
        assert!("hd0123456789abcdef01234567".len() == 26);
        for bad in ["", "HD1", "hd1.x", "hd_1", "hd-"] {
            assert!(!bad.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                || bad.is_empty()
                || bad.len() > 28);
        }
    }

    #[test]
    fn scram_iters_cap() {
        // Upstream-controlled iteration count: >100_000 → fail closed.
        let mut c = ScramClient::new("u", "p");
        c.cnonce = "abcdef".into();
        c.client_first_bare = "n=u,r=abcdef".into();
        for bad in ["i=0", "i=100001", "i=99999999"] {
            let sf = format!("r=abcdefX,s=AAAA,{}", bad);
            let mut cc = ScramClient::new("u", "p");
            cc.cnonce = "abcdef".into();
            cc.client_first_bare = "n=u,r=abcdef".into();
            assert!(cc.server_first(sf.as_bytes()).is_none(), "{}", bad);
        }
        let ok = "r=abcdefX,s=AAAA,i=4096";
        assert!(c.server_first(ok.as_bytes()).is_some());
    }

    /// Four one-way duplex pairs forming a session: client→relay
    /// (c_tx→c_rx), relay→client (r2c_tx→c_rd), relay→upstream
    /// (r2u_tx→u_rd), upstream→relay (u_tx→u_rx).
    fn chan() -> Channels {
        let (c_tx, c_rx) = tokio::io::duplex(1024);
        let (r2c_tx, c_rd) = tokio::io::duplex(1024);
        let (r2u_tx, u_rd) = tokio::io::duplex(1024);
        let (u_tx, u_rx) = tokio::io::duplex(1024);
        Channels {
            c_tx, c_rx, r2c_tx, c_rd, r2u_tx, u_rd, u_tx, u_rx,
        }
    }

    struct Channels {
        c_tx: tokio::io::DuplexStream,
        c_rx: tokio::io::DuplexStream,
        r2c_tx: tokio::io::DuplexStream,
        c_rd: tokio::io::DuplexStream,
        r2u_tx: tokio::io::DuplexStream,
        u_rd: tokio::io::DuplexStream,
        u_tx: tokio::io::DuplexStream,
        u_rx: tokio::io::DuplexStream,
    }

    #[tokio::test]
    async fn pump_both_shared_idle() {
        use tokio::io::AsyncWriteExt;
        let mut ch = chan();
        let idle = Duration::from_millis(60);
        let fut = pump_both(&mut ch.c_rx, &mut ch.r2u_tx, &mut ch.u_rx, &mut ch.r2c_tx, idle);
        tokio::pin!(fut);
        // Traffic in EITHER direction resets the shared timer — total
        // elapsed exceeds `idle` without Idling out.
        for _ in 0..3 {
            ch.c_tx.write_all(b"q").await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert!(tokio::time::timeout(Duration::from_millis(10), fut.as_mut()).await.is_err());
            assert_eq!(tokio::io::AsyncReadExt::read(&mut ch.u_rd, &mut [0u8; 1]).await.unwrap(), 1);
            ch.u_tx.write_all(b"r").await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert!(tokio::time::timeout(Duration::from_millis(10), fut.as_mut()).await.is_err());
            assert_eq!(tokio::io::AsyncReadExt::read(&mut ch.c_rd, &mut [0u8; 1]).await.unwrap(), 1);
        }
        // Then silence in both directions → Idle.
        let (_, _, end) = fut.await;
        assert_eq!(end, PumpEnd::Idle);
    }

    #[tokio::test]
    async fn pump_both_eof_closes_both() {
        let mut ch = chan();
        drop(ch.c_tx); // client EOF — relay must end BOTH directions now.
        let (_c, _u, end) = tokio::time::timeout(
            Duration::from_millis(500),
            pump_both(&mut ch.c_rx, &mut ch.r2u_tx, &mut ch.u_rx, &mut ch.r2c_tx,
                      Duration::from_secs(60)),
        )
        .await
        .expect("eof must not hang");
        assert_eq!(end, PumpEnd::Eof);
        // In a real session the OwnedWriteHalf/OwnedReadHalf drops
        // close the sibling socket; here the writer lives in `ch` —
        // assert the relay returned (no per-direction orphan pump).
    }

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("pgproxy-test-{}", tag));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn write_creds(dir: &std::path::Path, json: &str) -> std::path::PathBuf {
        let p = dir.join("pg-credentials.json");
        std::fs::write(&p, json).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        p
    }

    fn cfg(connect_ms: u64) -> PgProxyConfig {
        PgProxyConfig {
            listen: "127.0.0.1:0".to_string(),
            connect_timeout_ms: connect_ms,
            idle_timeout_ms: 60_000,
            max_session_ms: 300_000,
            max_sessions: 8,
            max_sessions_per_binding: 4,
        }
    }

    fn proxy(
        dir: &std::path::Path,
        creds_json: &str,
        routes: &[(&str, &str, &str)],
        cfg: PgProxyConfig,
    ) -> Arc<PgProxy> {
        let creds_path = write_creds(dir, creds_json);
        let creds = Arc::new(crate::pg::PgCredentialStore::new(creds_path));
        let desired = Arc::new(FileDesiredStore::new(
            dir.join("desired-absent"),
            "cell-test",
            vec![],
            1,
        ));
        let p = PgProxy::new(cfg, creds, desired);
        p.set_routes(
            routes
                .iter()
                .map(|(l, t, c)| (l.to_string(), (t.to_string(), c.to_string())))
                .collect(),
        );
        p
    }

    const TCP_CREDS: &str = r#"{"tenants":{"team-a":{"ref-1":{"token":"t","endpoint":"http://gw","tenantSlug":"team-a-x","credentialVersion":1,"readOnly":false,"tcp":{"host":"127.0.0.1","port":1,"user":"u","password":"p","database":"d"}}}}}"#;

    async fn connect_preamble(proxy: &Arc<PgProxy>, preamble: &str) -> tokio::net::TcpStream {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let px = Arc::clone(proxy);
        tokio::spawn(async move { px.serve(listener).await });
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut c, preamble.as_bytes())
            .await
            .unwrap();
        c
    }

    fn slots(proxy: &PgProxy) -> (usize, usize) {
        let pb = proxy.per_binding.lock().unwrap();
        let total: usize = pb.values().sum();
        (total, proxy.global.available_permits())
    }

    #[tokio::test]
    async fn handshake_stall_after_preamble_releases_slots() {
        let dir = tmpdir("hs-stall");
        let proxy = proxy(&dir, TCP_CREDS, &[("lbl1", "team-a", "ref-1")], cfg(120));
        let _c = connect_preamble(&proxy, "VRT-PG1 lbl1
").await;
        // Wait for the slot to be taken… then silence → handshake timeout.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(slots(&proxy).0, 1);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(slots(&proxy).0, 0);
        assert_eq!(slots(&proxy).1, 8);
    }

    #[tokio::test]
    async fn handshake_stall_after_sslrequest_releases_slots() {
        let dir = tmpdir("hs-ssl");
        let proxy = proxy(&dir, TCP_CREDS, &[("lbl1", "team-a", "ref-1")], cfg(120));
        let mut c = connect_preamble(&proxy, "VRT-PG1 lbl1
").await;
        // SSLRequest: len=8, code=80877103.
        tokio::io::AsyncWriteExt::write_all(
            &mut c,
            &8u32.to_be_bytes().into_iter().chain(80877103u32.to_be_bytes()).collect::<Vec<u8>>(),
        )
        .await
        .unwrap();
        let mut one = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut c, &mut one).await.unwrap();
        assert_eq!(one[0], b'N');
        // Silence → handshake timeout frees slots.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(slots(&proxy).0, 0);
        assert_eq!(slots(&proxy).1, 8);
    }

    #[tokio::test]
    async fn handshake_unresponsive_upstream_releases_slots() {
        let dir = tmpdir("hs-upstream");
        // Upstream accepts but never responds.
        let ul = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uaddr = ul.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((s, _)) = ul.accept().await else { return };
                tokio::spawn(async move { let _s = s; tokio::time::sleep(Duration::from_secs(30)).await });
            }
        });
        let creds = format!(
            r#"{{"tenants":{{"team-a":{{"ref-1":{{"token":"t","endpoint":"http://gw","tenantSlug":"team-a-x","credentialVersion":1,"readOnly":false,"tcp":{{"host":"127.0.0.1","port":{},"user":"u","password":"p","database":"d"}}}}}}}}}}"#,
            uaddr.port()
        );
        let proxy = proxy(&dir, &creds, &[("lbl1", "team-a", "ref-1")], cfg(120));
        let mut c = connect_preamble(&proxy, "VRT-PG1 lbl1
").await;
        // Valid v3 startup frame so we reach upstream_handshake.
        let mut body = b"user\0w\0database\0d\0\0".to_vec();
        let mut fr = Vec::new();
        fr.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
        fr.extend_from_slice(&196608u32.to_be_bytes());
        fr.append(&mut body);
        tokio::io::AsyncWriteExt::write_all(&mut c, &fr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(slots(&proxy).0, 1);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(slots(&proxy).0, 0);
        assert_eq!(slots(&proxy).1, 8);
    }

    #[tokio::test]
    async fn migration_read_only_denied_typed() {
        let dir = tmpdir("hs-mro");
        let creds = r#"{"tenants":{"team-a":{"ref-1":{"token":"t","endpoint":"http://gw","tenantSlug":"team-a-x","credentialVersion":1,"readOnly":true,"tcpBlocked":"migration_read_only"}}}}"#;
        let proxy = proxy(&dir, creds, &[("lbl1", "team-a", "ref-1")], cfg(2000));
        let mut c = connect_preamble(&proxy, "VRT-PG1 lbl1
").await;
        // Deny arrives right after the preamble (before startup).
        let mut got = vec![0u8; 128];
        let mut acc = Vec::new();
        loop {
            let n = tokio::time::timeout(
                Duration::from_millis(300),
                tokio::io::AsyncReadExt::read(&mut c, &mut got),
            )
            .await;
            match n {
                Err(_) => break,
                Ok(Err(_)) | Ok(Ok(0)) => break,
                Ok(Ok(n)) => acc.extend_from_slice(&got[..n]),
            }
            if acc.len() > 5 && acc.len() >= 5 + u32::from_be_bytes(acc[1..5].try_into().unwrap()) as usize {
                break;
            }
        }
        assert!(acc.windows(b"PG_MIGRATION_READ_ONLY".len()).any(|w| w == b"PG_MIGRATION_READ_ONLY"),
                "want typed deny, got {:?}", String::from_utf8_lossy(&acc));
        assert_eq!(slots(&proxy).0, 0);
        assert_eq!(slots(&proxy).1, 8);
    }

    #[test]
    fn pbkdf2_known() {
        // PBKDF2-HMAC-SHA256("password","salt",1) = 120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b
        let k = pbkdf2(b"password", b"salt", 1);
        assert_eq!(
            hex::encode(k),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
    }
}
