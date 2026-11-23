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
//! (provider-held, rotated via render-inputs). After upstream
//! ReadyForQuery the session is a transparent bidirectional relay
//! bounded by idle/session timeouts and byte accounting. Sessions whose
//! binding vanishes from desired state are aborted by the sweeper
//! within one sweep interval.
//!
//! MIG-15 r3: with `[hyperdrive.pool]` configured, backend connections
//! come from a per-process session pool ([`crate::pgpool`]) keyed by
//! `(tenant, endpoint, user, database)`; without it every session owns a
//! fresh connection. A session is only returned to the pool when the
//! relay stopped on a clean ReadyForQuery boundary.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::config::{PgProxyConfig, PoolConfig};
use crate::desired_file::FileDesiredStore;
use crate::metrics::reg;
use crate::pgpool::{AcquireError, Checkin, ConnectionPool, PoolKey};
use crate::pgwire::{self, RelayState, WireError};

const SSL_REQUEST: u32 = 808_771_03;
const GSSENC_REQUEST: u32 = 808_771_04;
const CANCEL_REQUEST: u32 = 808_771_02;
const PROTO_V3: u32 = 196_608;
/// Startup frame cap — a tenant cannot grow work/memory via the packet.
const STARTUP_MAX: u32 = 16 * 1024;
const PREAMBLE_MAX: usize = 128;
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
    /// MIG-15 r3: `maxConnections` stayed saturated for the whole
    /// `acquireTimeoutMs` — the queue is bounded, never unbounded.
    PoolExhausted,
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
            Self::PoolExhausted => "PG_CONNECTION_LIMIT",
        }
    }
}

/// One binding's live session set for the revoker sweep.
type SessionMap = Mutex<HashMap<String, HashMap<u64, tokio::sync::oneshot::Sender<()>>>>;

pub struct PgProxy {
    cfg: PgProxyConfig,
    /// `[hyperdrive.pool]` — `None` keeps the pre-r3 one-connection-per-
    /// session shape.
    pool: Option<Arc<ConnectionPool>>,
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
        pool: Option<PoolConfig>,
        creds: Arc<crate::pg::PgCredentialStore>,
        desired: Arc<FileDesiredStore>,
    ) -> Arc<Self> {
        let io_timeout = Duration::from_millis(cfg.connect_timeout_ms);
        Arc::new(Self {
            global: Arc::new(Semaphore::new(cfg.max_sessions)),
            pool: pool.map(|p| ConnectionPool::new(p, io_timeout)),
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

    /// Pool handle (reaper / readiness), `None` when pooling is off.
    pub fn pool(&self) -> Option<&Arc<ConnectionPool>> {
        self.pool.as_ref()
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
        if let Some(r) = self
            .test_routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
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
            // MIG-15 r3: idle pooled sockets follow the same lifecycle —
            // a revoked binding must not keep a backend session alive.
            if let Some(pool) = &self.pool {
                let tenants: HashSet<String> =
                    live.values().map(|(tenant, _)| tenant.clone()).collect();
                pool.evict_tenants(&tenants);
            }
            if killed > 0 {
                info!(
                    killed,
                    "pgproxy: sessions aborted — binding left desired state"
                );
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
                Deny::PoolExhausted => "connection_limit",
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
        // MIG-15 r2: ONE deadline covers the whole accept→client-startup
        // phase — preamble, route/credential resolution, client startup
        // frames. A client that stalls anywhere inside releases the
        // global permit + per-binding slot on timeout; the dropped future
        // closes the socket.
        //
        // MIG-15 r3: backend acquisition (pool queue + dial/auth) is OUT
        // of that deadline on purpose — a saturated pool must answer with
        // its own typed denial after `acquireTimeoutMs` instead of being
        // pre-empted by the handshake timeout.
        let admitted = match tokio::time::timeout(
            Duration::from_millis(self.cfg.connect_timeout_ms),
            self.pre_relay(sock, id),
        )
        .await
        {
            Err(_) => return Err((Deny::HandshakeTimeout, None)),
            Ok(v) => v?,
        };
        let parts = self.attach_upstream(admitted).await?;
        let PreRelay {
            rd: mut client_rd,
            wr: mut client_wr,
            mut conn,
            label,
            mut kill_rx,
            active: _active,
            release: _release,
        } = parts;

        // Bidirectional relay bounded by idle + session deadlines and
        // by the revocation kill channel; all bytes are metered.
        // MIG-15 r2: idle = no bytes in EITHER direction (shared
        // last-activity); any side ending (EOF/IO/idle) ends BOTH.
        let idle = Duration::from_millis(self.cfg.idle_timeout_ms);
        let deadline = Duration::from_millis(self.cfg.max_session_ms);
        let (up_rd, up_wr) = conn.halves();
        let relay = async {
            tokio::select! {
                _ = &mut kill_rx => RelayOutcome::killed(),
                v = relay_both(&mut client_rd, up_wr, up_rd, &mut client_wr, idle) => v,
            }
        };
        let outcome = match tokio::time::timeout(deadline, relay).await {
            Err(_) => return Err((Deny::SessionDeadline, Some(label))),
            Ok(v) => v,
        };
        reg().add(
            "vrt_provider_pgproxy_bytes_total",
            &[("dir", "up")],
            outcome.c2u as f64,
        );
        reg().add(
            "vrt_provider_pgproxy_bytes_total",
            &[("dir", "down")],
            outcome.u2c as f64,
        );
        if kill_rx.is_terminated() {
            // Revoked binding: the socket must not come back to the pool.
            return Err((Deny::BindingRevoked, Some(label)));
        }
        // MIG-15 r3: only a connection that stopped exactly on a
        // ReadyForQuery boundary with no client request outstanding may
        // be reset and reused.
        let reusable =
            matches!(outcome.end, PumpEnd::ClientEof | PumpEnd::Idle) && outcome.state.reusable();
        conn.finish(Checkin {
            reusable,
            ready_status: outcome.state.ready_status(),
        })
        .await;
        if outcome.end == PumpEnd::Idle {
            return Err((Deny::IdleTimeout, Some(label)));
        }
        Ok(())
    }

    /// Client half of the admission: preamble → route → admission →
    /// credential → client startup frames. Runs under the
    /// `connect_timeout_ms` deadline in `session`; the guards it returns
    /// keep the per-binding slot, registry entry and active counter until
    /// the whole session (or a cancel) drops them.
    async fn pre_relay(
        self: &Arc<Self>,
        mut sock: TcpStream,
        id: u64,
    ) -> Result<Admitted, (Deny, Option<String>)> {
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
        reg().add(
            "vrt_provider_pgproxy_sessions_total",
            &[("binding", "bound")],
            1.0,
        );
        self.active.fetch_add(1, Ordering::Relaxed);
        self.push_active();
        let active = ActiveGuard {
            proxy: Arc::clone(self),
        };
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

        Ok(Admitted {
            rd,
            wr,
            tcp,
            team,
            label,
            kill_rx,
            active,
            release,
        })
    }

    /// Backend half of the admission: pooled checkout or a direct dial,
    /// then the synthetic client_ready.
    ///
    /// MIG-15 r3: deliberately outside the `connect_timeout_ms`
    /// handshake deadline — a saturated pool answers with the typed
    /// `PG_CONNECTION_LIMIT` after `acquireTimeoutMs` instead of being
    /// pre-empted by the handshake timeout.
    async fn attach_upstream(
        self: &Arc<Self>,
        admitted: Admitted,
    ) -> Result<PreRelay, (Deny, Option<String>)> {
        let Admitted {
            rd,
            mut wr,
            tcp,
            team,
            label,
            kill_rx,
            active,
            release,
        } = admitted;
        // Upstream connection (provider-held identity): pooled session
        // reuse, or a per-session connection when pooling is off.
        let conn = match &self.pool {
            Some(pool) => {
                let key = PoolKey::new(&team, &tcp);
                match pool.acquire(&key, &tcp).await {
                    Ok(conn) => SessionConn::Pooled {
                        pool: Arc::clone(pool),
                        conn,
                    },
                    Err(AcquireError::Timeout) => {
                        let _ = wr.write_all(&pool_limit_frame()).await;
                        return Err((Deny::PoolExhausted, Some(label)));
                    }
                    Err(AcquireError::Upstream(e)) => {
                        let deny = upstream_deny(e);
                        send_error_w(&mut wr, deny.code()).await;
                        return Err((deny, Some(label)));
                    }
                }
            }
            None => {
                let started = Instant::now();
                match pgwire::connect(&tcp, Duration::from_millis(self.cfg.connect_timeout_ms))
                    .await
                {
                    Ok(conn) => {
                        reg().observe(
                            "vrt_provider_pgproxy_upstream_connect_ms",
                            &[],
                            started.elapsed().as_secs_f64() * 1000.0,
                        );
                        SessionConn::Direct(conn)
                    }
                    Err(e) => {
                        let deny = upstream_deny(e);
                        send_error_w(&mut wr, deny.code()).await;
                        return Err((deny, Some(label)));
                    }
                }
            }
        };
        // Client is admitted on the already-authenticated channel:
        // AuthenticationOk + minimal parameters + ReadyForQuery.
        let _ = wr.write_all(&client_ready(&tcp.user)).await;
        Ok(PreRelay {
            rd,
            wr,
            conn,
            label,
            kill_rx,
            active,
            release,
        })
    }
}

/// Map an upstream wire failure onto the typed session denial.
fn upstream_deny(err: WireError) -> Deny {
    match err {
        WireError::Auth => Deny::UpstreamAuth,
        WireError::Connect | WireError::Protocol => Deny::UpstreamConnect,
    }
}

/// Backend connection of one session: either straight from
/// [`pgwire::connect`] or checked out of the pool (and returned on a
/// clean boundary).
enum SessionConn {
    Direct(pgwire::PgConn),
    Pooled {
        pool: Arc<ConnectionPool>,
        conn: crate::pgpool::PooledConn,
    },
}

impl SessionConn {
    /// Backend read/write halves — one call so both borrows stay
    /// disjoint.
    fn halves(&mut self) -> (&mut OwnedReadHalf, &mut OwnedWriteHalf) {
        match self {
            Self::Direct(conn) => (&mut conn.rd, &mut conn.wr),
            Self::Pooled { conn, .. } => conn.halves(),
        }
    }

    /// Hand the backend socket back: pooled connections are reset and
    /// parked, direct ones simply close.
    async fn finish(self, outcome: Checkin) {
        match self {
            Self::Direct(_) => {}
            Self::Pooled { pool, conn } => pool.checkin(conn, outcome).await,
        }
    }
}

/// Client half of an admitted session, handed from `pre_relay` to
/// `attach_upstream`.
struct Admitted {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    tcp: crate::pg::PgTcpCredential,
    team: String,
    label: String,
    kill_rx: tokio::sync::oneshot::Receiver<()>,
    active: ActiveGuard,
    release: ReleaseGuard,
}

/// Pieces handed from `pre_relay` to the relay phase; guards keep the
/// per-binding slot, registry entry and active counter until the whole
/// session (or a handshake timeout cancel) drops them.
struct PreRelay {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    conn: SessionConn,
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
    reg().add(
        "vrt_provider_pgproxy_denied_total",
        &[("reason", reason)],
        1.0,
    );
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
    if label.is_empty()
        || label.len() > 28
        || !label
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
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
    body.split(|b| *b == 0).any(|k| k == b"replication")
}

async fn send_error(sock: &mut TcpStream, code: &str) {
    let _ = sock.write_all(&error_frame(code)).await;
}
async fn send_error_w(wr: &mut OwnedWriteHalf, code: &str) {
    let _ = wr.write_all(&error_frame(code)).await;
}

/// FATAL ErrorResponse with the typed code — no tenant data, no creds.
fn error_frame(code: &str) -> Vec<u8> {
    error_frame_detail("0A000", &format!("vrt pgproxy {}", code))
}

/// MIG-15 r3: pool exhaustion is a capacity answer, not a protocol
/// rejection — SQLSTATE 53300 (`too_many_connections`) is what clients
/// already understand, and the message names the feature.
fn pool_limit_frame() -> Vec<u8> {
    error_frame_detail("53300", "hyperdrive: connection limit reached")
}

fn error_frame_detail(sqlstate: &str, msg: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(48 + msg.len());
    body.extend_from_slice(b"SERROR\0VFATAL\0C");
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
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
    /// The client closed its side — the backend may be reusable.
    ClientEof,
    /// The backend ended the connection.
    UpstreamEof,
    /// The revocation channel fired.
    Killed,
    Idle,
    Io,
}

/// Relay result: byte counters, how it ended and what it learned about
/// protocol boundaries (used to decide whether the backend connection
/// may go back to the pool).
struct RelayOutcome {
    c2u: u64,
    u2c: u64,
    end: PumpEnd,
    state: RelayState,
}

impl RelayOutcome {
    /// Revocation kill: nothing transferred, nothing reusable.
    fn killed() -> Self {
        Self {
            c2u: 0,
            u2c: 0,
            end: PumpEnd::Killed,
            state: RelayState::new(),
        }
    }
}

/// Bidirectional relay; the idle timeout applies to silence in BOTH
/// directions, and either side ending closes both.
async fn relay_both<R1, W1, R2, W2>(
    rd: &mut R1,
    uwr: &mut W1,
    urd: &mut R2,
    wr: &mut W2,
    idle: Duration,
) -> RelayOutcome
where
    R1: tokio::io::AsyncRead + Unpin,
    W1: tokio::io::AsyncWrite + Unpin,
    R2: tokio::io::AsyncRead + Unpin,
    W2: tokio::io::AsyncWrite + Unpin,
{
    let mut state = RelayState::new();
    let mut c2u = 0u64;
    let mut u2c = 0u64;
    let mut b_c = vec![0u8; 16 * 1024];
    let mut b_u = vec![0u8; 16 * 1024];
    loop {
        let end = tokio::select! {
            r = rd.read(&mut b_c) => match r {
                Ok(0) => Some(PumpEnd::ClientEof),
                Err(_) => Some(PumpEnd::Io),
                Ok(n) => {
                    c2u += n as u64;
                    // A Terminate frame ends the session and is NOT
                    // forwarded: the backend socket survives the client
                    // for the next session of the same key.
                    let forwarded = state.feed_client(&b_c[..n]).unwrap_or(n);
                    if uwr.write_all(&b_c[..forwarded]).await.is_err() {
                        Some(PumpEnd::Io)
                    } else if forwarded < n {
                        Some(PumpEnd::ClientEof)
                    } else {
                        None
                    }
                }
            },
            r = urd.read(&mut b_u) => match r {
                Ok(0) => Some(PumpEnd::UpstreamEof),
                Err(_) => Some(PumpEnd::Io),
                Ok(n) => {
                    u2c += n as u64;
                    state.feed_upstream(&b_u[..n]);
                    if wr.write_all(&b_u[..n]).await.is_err() {
                        Some(PumpEnd::Io)
                    } else {
                        None
                    }
                }
            },
            _ = tokio::time::sleep(idle) => Some(PumpEnd::Idle),
        };
        if let Some(end) = end {
            return RelayOutcome {
                c2u,
                u2c,
                end,
                state,
            };
        }
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
    fn preamble_charset() {
        // Charset guard mirrored by compute_desired; test the pure gate.
        assert!("hd0123456789abcdef01234567".len() == 26);
        for bad in ["", "HD1", "hd1.x", "hd_1", "hd-"] {
            assert!(
                !bad.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                    || bad.is_empty()
                    || bad.len() > 28
            );
        }
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
            c_tx,
            c_rx,
            r2c_tx,
            c_rd,
            r2u_tx,
            u_rd,
            u_tx,
            u_rx,
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
    async fn relay_shared_idle() {
        use tokio::io::AsyncWriteExt;
        let mut ch = chan();
        let idle = Duration::from_millis(60);
        let fut = relay_both(
            &mut ch.c_rx,
            &mut ch.r2u_tx,
            &mut ch.u_rx,
            &mut ch.r2c_tx,
            idle,
        );
        tokio::pin!(fut);
        // Traffic in EITHER direction resets the shared timer — total
        // elapsed exceeds `idle` without Idling out.
        for _ in 0..3 {
            ch.c_tx.write_all(b"q").await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), fut.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(
                tokio::io::AsyncReadExt::read(&mut ch.u_rd, &mut [0u8; 1])
                    .await
                    .unwrap(),
                1
            );
            ch.u_tx.write_all(b"r").await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(10), fut.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(
                tokio::io::AsyncReadExt::read(&mut ch.c_rd, &mut [0u8; 1])
                    .await
                    .unwrap(),
                1
            );
        }
        // Then silence in both directions → Idle.
        let outcome = fut.await;
        assert_eq!(outcome.end, PumpEnd::Idle);
        // Raw `q`/`r` bytes are not valid frames, so the tracker can
        // never vouch for this connection.
        assert!(!outcome.state.reusable());
    }

    #[tokio::test]
    async fn relay_client_eof_closes_both() {
        let mut ch = chan();
        drop(ch.c_tx); // client EOF — relay must end BOTH directions now.
        let outcome = tokio::time::timeout(
            Duration::from_millis(500),
            relay_both(
                &mut ch.c_rx,
                &mut ch.r2u_tx,
                &mut ch.u_rx,
                &mut ch.r2c_tx,
                Duration::from_secs(60),
            ),
        )
        .await
        .expect("eof must not hang");
        assert_eq!(outcome.end, PumpEnd::ClientEof);
        // Nothing was exchanged: the backend is still exactly where the
        // session found it.
        assert!(outcome.state.reusable());
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
        proxy_pooled(dir, creds_json, routes, cfg, None)
    }

    fn proxy_pooled(
        dir: &std::path::Path,
        creds_json: &str,
        routes: &[(&str, &str, &str)],
        cfg: PgProxyConfig,
        pool: Option<PoolConfig>,
    ) -> Arc<PgProxy> {
        let creds_path = write_creds(dir, creds_json);
        let creds = Arc::new(crate::pg::PgCredentialStore::new(creds_path));
        let desired = Arc::new(FileDesiredStore::new(
            dir.join("desired-absent"),
            "cell-test",
            vec![],
            1,
        ));
        let p = PgProxy::new(cfg, pool, creds, desired);
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

    fn creds_json(port: u16) -> String {
        format!(
            r#"{{"tenants":{{"team-a":{{"ref-1":{{"token":"t","endpoint":"http://gw","tenantSlug":"team-a-x","credentialVersion":1,"readOnly":false,"tcp":{{"host":"127.0.0.1","port":{port},"user":"u","password":"p","database":"d"}}}}}}}}}}"#
        )
    }

    fn pool_cfg(max: usize, acquire: u64) -> PoolConfig {
        PoolConfig {
            max_connections: max,
            acquire_timeout_ms: acquire,
            idle_timeout_ms: 60_000,
            max_lifetime_ms: 300_000,
            reset_on_return: true,
        }
    }

    async fn serve_proxy(proxy: &Arc<PgProxy>) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let px = Arc::clone(proxy);
        tokio::spawn(async move { px.serve(listener).await });
        addr
    }

    /// Framed client-side read up to (and including) the next
    /// ReadyForQuery — the proxy speaks plain pg-wire to the client.
    async fn read_to_ready(c: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
        let mut acc = Vec::new();
        let mut cursor = 0usize;
        let mut frames = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = match tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf)).await {
                Ok(Ok(0)) | Err(_) => return frames,
                Ok(Ok(n)) => n,
                Ok(Err(_)) => return frames,
            };
            acc.extend_from_slice(&buf[..n]);
            while acc.len() >= cursor + 5 {
                let tag = acc[cursor];
                let len =
                    u32::from_be_bytes(acc[cursor + 1..cursor + 5].try_into().unwrap()) as usize;
                if len < 4 || acc.len() < cursor + 1 + len {
                    break;
                }
                let body = acc[cursor + 5..cursor + 1 + len].to_vec();
                cursor += 1 + len;
                frames.push((tag, body));
                if tag == b'Z' {
                    return frames;
                }
            }
        }
    }

    /// Open the session up to `ReadyForQuery` (preamble + v3 startup).
    async fn open_session(addr: std::net::SocketAddr, label: &str) -> TcpStream {
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(format!("VRT-PG1 {label}\n").as_bytes())
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
        c.write_all(&startup).await.unwrap();
        let frames = read_to_ready(&mut c).await;
        assert!(
            frames.iter().any(|(t, _)| *t == b'Z'),
            "proxy never admitted the session"
        );
        c
    }

    async fn query(c: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut q = vec![b'Q'];
        q.extend_from_slice(&((sql.len() + 5) as u32).to_be_bytes());
        q.extend_from_slice(sql.as_bytes());
        q.push(0);
        c.write_all(&q).await.unwrap();
        read_to_ready(c).await
    }

    async fn terminate(c: TcpStream) {
        let mut c = c;
        let _ = c.write_all(&[b'X', 0, 0, 0, 4]).await;
        let _ = c.shutdown().await;
    }

    #[tokio::test]
    async fn pooled_sessions_share_one_backend_connection() {
        let dir = tmpdir("pool-reuse");
        let (pg_port, mock) = crate::pgmock::backend().await;
        let proxy = proxy_pooled(
            &dir,
            &creds_json(pg_port),
            &[("lbl1", "team-a", "ref-1")],
            cfg(2000),
            Some(pool_cfg(1, 2_000)),
        );
        let addr = serve_proxy(&proxy).await;

        let mut c = open_session(addr, "lbl1").await;
        query(&mut c, "SELECT 42").await;
        terminate(c).await;

        let mut c = open_session(addr, "lbl1").await;
        query(&mut c, "SELECT 43").await;
        terminate(c).await;

        assert_eq!(
            mock.accepted(),
            1,
            "both sessions ran on one backend connection"
        );
        let stmts = mock.statements();
        let reset = stmts.iter().position(|s| s == "DISCARD ALL");
        let second = stmts.iter().position(|s| s == "SELECT 43");
        assert_eq!(mock.resets(), 1, "the returned session was reset");
        assert!(
            matches!((reset, second), (Some(r), Some(s)) if r < s),
            "DISCARD ALL must run before the next session: {stmts:?}"
        );
        // The client's Terminate is swallowed, never forwarded: the
        // backend socket outlives the client session.
        assert!(!stmts.iter().any(|s| s == "X"));
    }

    #[tokio::test]
    async fn capacity_denies_the_queued_session_with_the_pool_error() {
        let dir = tmpdir("pool-deny");
        let (pg_port, _mock) = crate::pgmock::backend().await;
        let proxy = proxy_pooled(
            &dir,
            &creds_json(pg_port),
            &[("lbl1", "team-a", "ref-1")],
            cfg(2000),
            Some(pool_cfg(1, 150)),
        );
        let addr = serve_proxy(&proxy).await;

        // Session A holds the only backend connection.
        let mut a = open_session(addr, "lbl1").await;
        query(&mut a, "SELECT 1").await;

        let mut b = TcpStream::connect(addr).await.unwrap();
        b.write_all(b"VRT-PG1 lbl1\n").await.unwrap();
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
        let frames = read_to_ready(&mut b).await;
        let error: Vec<u8> = frames
            .iter()
            .filter(|(t, _)| *t == b'E')
            .flat_map(|(_, body)| body.clone())
            .collect();
        let text = String::from_utf8_lossy(&error);
        assert!(
            text.contains("hyperdrive: connection limit reached"),
            "want the typed pool denial, got {text:?} ({frames:?})"
        );
        assert!(text.contains("53300"), "want SQLSTATE 53300: {text:?}");
        terminate(a).await;
    }

    #[tokio::test]
    async fn a_second_tenant_gets_its_own_backend_connection() {
        let dir = tmpdir("pool-tenant");
        let (pg_port, mock) = crate::pgmock::backend().await;
        let creds = format!(
            r#"{{"tenants":{{"team-a":{{"ref-1":{{"token":"t","endpoint":"http://gw","tenantSlug":"team-a-x","credentialVersion":1,"readOnly":false,"tcp":{{"host":"127.0.0.1","port":{pg_port},"user":"u","password":"p","database":"d"}}}}}},"team-b":{{"ref-1":{{"token":"t","endpoint":"http://gw","tenantSlug":"team-b-x","credentialVersion":1,"readOnly":false,"tcp":{{"host":"127.0.0.1","port":{pg_port},"user":"u","password":"p","database":"d"}}}}}}}}}}"#
        );
        let proxy = proxy_pooled(
            &dir,
            &creds,
            &[("lbla", "team-a", "ref-1"), ("lblb", "team-b", "ref-1")],
            cfg(2000),
            Some(pool_cfg(4, 2_000)),
        );
        let addr = serve_proxy(&proxy).await;

        let mut a = open_session(addr, "lbla").await;
        query(&mut a, "SET application_name = 'leak'").await;
        terminate(a).await;

        let mut b = open_session(addr, "lblb").await;
        query(&mut b, "SELECT 43").await;
        terminate(b).await;

        assert_eq!(
            mock.accepted(),
            2,
            "tenant is part of the pool key: no shared session state"
        );
    }

    #[tokio::test]
    async fn the_pool_queue_outlives_the_handshake_deadline() {
        // connect_timeout_ms is deliberately shorter than acquireTimeoutMs:
        // a saturated pool must answer with the typed pool denial, not
        // with PG_HANDSHAKE_TIMEOUT.
        let dir = tmpdir("pool-deadline");
        let (pg_port, _mock) = crate::pgmock::backend().await;
        let proxy = proxy_pooled(
            &dir,
            &creds_json(pg_port),
            &[("lbl1", "team-a", "ref-1")],
            cfg(150),
            Some(pool_cfg(1, 600)),
        );
        let addr = serve_proxy(&proxy).await;

        let mut a = open_session(addr, "lbl1").await;
        query(&mut a, "SELECT 1").await;

        let started = Instant::now();
        let mut b = TcpStream::connect(addr).await.unwrap();
        b.write_all(b"VRT-PG1 lbl1\n").await.unwrap();
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
        let frames = read_to_ready(&mut b).await;
        let text: String = frames
            .iter()
            .filter(|(t, _)| *t == b'E')
            .flat_map(|(_, body)| body.clone())
            .map(char::from)
            .collect();
        assert!(
            text.contains("hyperdrive: connection limit reached"),
            "want the pool denial, got {text:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(500),
            "the queue must run its own acquireTimeoutMs ({:?})",
            started.elapsed()
        );
        terminate(a).await;
    }

    #[tokio::test]
    async fn without_a_pool_a_session_still_relays() {
        let dir = tmpdir("direct");
        let (pg_port, mock) = crate::pgmock::backend().await;
        let proxy = proxy_pooled(
            &dir,
            &creds_json(pg_port),
            &[("lbl1", "team-a", "ref-1")],
            cfg(2_000),
            None,
        );
        assert!(proxy.pool().is_none());
        let addr = serve_proxy(&proxy).await;
        for _ in 0..2 {
            let mut c = open_session(addr, "lbl1").await;
            let reply = query(&mut c, "SELECT 1").await;
            assert!(reply.iter().any(|(t, _)| *t == b'Z'));
            terminate(c).await;
        }
        // No pooling: every session dials its own backend connection.
        assert_eq!(mock.accepted(), 2);
        assert_eq!(mock.resets(), 0);
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
        let _c = connect_preamble(
            &proxy,
            "VRT-PG1 lbl1
",
        )
        .await;
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
        let mut c = connect_preamble(
            &proxy,
            "VRT-PG1 lbl1
",
        )
        .await;
        // SSLRequest: len=8, code=80877103.
        tokio::io::AsyncWriteExt::write_all(
            &mut c,
            &8u32
                .to_be_bytes()
                .into_iter()
                .chain(80877103u32.to_be_bytes())
                .collect::<Vec<u8>>(),
        )
        .await
        .unwrap();
        let mut one = [0u8; 1];
        tokio::io::AsyncReadExt::read_exact(&mut c, &mut one)
            .await
            .unwrap();
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
                let Ok((s, _)) = ul.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let _s = s;
                    tokio::time::sleep(Duration::from_secs(30)).await
                });
            }
        });
        let creds = format!(
            r#"{{"tenants":{{"team-a":{{"ref-1":{{"token":"t","endpoint":"http://gw","tenantSlug":"team-a-x","credentialVersion":1,"readOnly":false,"tcp":{{"host":"127.0.0.1","port":{},"user":"u","password":"p","database":"d"}}}}}}}}}}"#,
            uaddr.port()
        );
        let proxy = proxy(&dir, &creds, &[("lbl1", "team-a", "ref-1")], cfg(120));
        let mut c = connect_preamble(
            &proxy,
            "VRT-PG1 lbl1
",
        )
        .await;
        // Valid v3 startup frame so we reach upstream_handshake.
        let mut body = b"user\0w\0database\0d\0\0".to_vec();
        let mut fr = Vec::new();
        fr.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
        fr.extend_from_slice(&196608u32.to_be_bytes());
        fr.append(&mut body);
        tokio::io::AsyncWriteExt::write_all(&mut c, &fr)
            .await
            .unwrap();
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
        let mut c = connect_preamble(
            &proxy,
            "VRT-PG1 lbl1
",
        )
        .await;
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
            if acc.len() > 5
                && acc.len() >= 5 + u32::from_be_bytes(acc[1..5].try_into().unwrap()) as usize
            {
                break;
            }
        }
        assert!(
            acc.windows(b"PG_MIGRATION_READ_ONLY".len())
                .any(|w| w == b"PG_MIGRATION_READ_ONLY"),
            "want typed deny, got {:?}",
            String::from_utf8_lossy(&acc)
        );
        assert_eq!(slots(&proxy).0, 0);
        assert_eq!(slots(&proxy).1, 8);
    }
}
