//! MIG-15 r3: per-process Hyperdrive session pool.
//!
//! One pool per `vrt-provider` process (the pg-wire proxy owns it).
//! Backend connections are keyed by
//! `(tenant, origin endpoint, user, database)`, so two sessions of the
//! same binding continue on the same authenticated backend socket while
//! a different tenant/database can never inherit it.
//!
//! Semantics — **session pooling only**:
//!
//! - A client session holds exactly one backend connection for its whole
//!   lifetime (no transaction multiplexing); the pool reuses that
//!   connection after the client leaves.
//! - `max_connections` bounds live backend sockets **per process**. With
//!   `N` replicas the cluster-wide ceiling is `N × max_connections`.
//! - Acquisition beyond the cap is queued for at most
//!   `acquire_timeout_ms`, then denied (`AcquireError::Timeout`) — never
//!   unbounded waiting.
//! - A connection is only pooled when the relay ended on a clean
//!   `ReadyForQuery` boundary with no client request outstanding
//!   ([`crate::pgwire::RelayState`]); anything else is closed.
//! - On return the session is rolled back if needed and `DISCARD ALL`-ed
//!   (`reset_on_return`); any failure closes the socket instead of
//!   handing dirty state to the next session.
//! - `idle_timeout_ms` reclaims unused sockets, `max_lifetime_ms` retires
//!   them even under steady use, and the checkout probe (`SELECT 1`)
//!   drops sockets that died while idle.
//!
//! The pool is in-memory only: a process restart drops every backend
//! connection, which is expected and never a correctness problem.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::config::PoolConfig;
use crate::metrics::reg;
use crate::pg::PgTcpCredential;
use crate::pgwire::{self, WireError};

/// Idle-connection reaper cadence.
const REAP_MS: u64 = 2_000;

/// Identity of a reusable backend connection. Never contains secrets —
/// the password is not part of the key.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PoolKey {
    tenant: String,
    endpoint: String,
    user: String,
    database: String,
}

impl PoolKey {
    /// `(tenant, origin endpoint, user, database)` — the granularity at
    /// which session state may be shared.
    pub fn new(tenant: &str, tcp: &PgTcpCredential) -> Self {
        Self {
            tenant: tenant.to_string(),
            endpoint: format!("{}:{}", tcp.host, tcp.port),
            user: tcp.user.clone(),
            database: tcp.database.clone(),
        }
    }

    /// Tenant the connection belongs to (sweeper revocation).
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

/// Why a connection was closed — bounded metric label.
#[derive(Clone, Copy, Debug)]
enum CloseReason {
    /// Probe or reset failed, or the client left mid-exchange.
    Broken,
    /// Idle longer than `idle_timeout_ms`.
    Idle,
    /// Older than `max_lifetime_ms`.
    Lifetime,
    /// Binding/tenant left desired state.
    TenantRevoked,
}

impl CloseReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Broken => "broken",
            Self::Idle => "idle",
            Self::Lifetime => "lifetime",
            Self::TenantRevoked => "tenant_revoked",
        }
    }
}

/// Acquire failures. `Timeout` is the bounded-queue denial; `Upstream`
/// is a dial/auth failure (never a silent fallback to an unpooled
/// connection).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquireError {
    /// `max_connections` reached and nothing freed up within
    /// `acquire_timeout_ms`.
    Timeout,
    /// Dialling/authenticating a replacement backend failed.
    Upstream(WireError),
}

/// A checked-out backend connection. Dropping it without
/// [`ConnectionPool::checkin`] closes the socket (the permit goes back).
pub struct PooledConn {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    key: PoolKey,
    opened: Instant,
    /// Holds one `max_connections` slot for the socket's lifetime.
    _permit: OwnedSemaphorePermit,
}

impl PooledConn {
    /// Backend read/write halves — one call so both borrows stay
    /// disjoint.
    pub fn halves(&mut self) -> (&mut OwnedReadHalf, &mut OwnedWriteHalf) {
        (&mut self.rd, &mut self.wr)
    }
}

/// What the session learned about the connection when the client left.
#[derive(Clone, Copy, Debug)]
pub struct Checkin {
    /// Relay ended on a clean `ReadyForQuery` boundary.
    pub reusable: bool,
    /// Transaction status of that boundary (`None` before the first).
    pub ready_status: Option<u8>,
}

impl Checkin {
    /// The connection cannot be trusted — close it.
    pub fn discard() -> Self {
        Self {
            reusable: false,
            ready_status: None,
        }
    }
}

struct IdleConn {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    key: PoolKey,
    opened: Instant,
    idle_since: Instant,
    _permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct PoolState {
    idle: HashMap<PoolKey, Vec<IdleConn>>,
}

/// Per-process session pool over `max_connections` backend sockets.
pub struct ConnectionPool {
    cfg: PoolConfig,
    /// Bound for dial/auth, probe and reset round trips.
    io_timeout: Duration,
    sem: Arc<Semaphore>,
    /// Wakes queued acquisitions when a socket is parked or closed (a
    /// permit is only freed by a close, so returns need their own
    /// signal).
    notify: tokio::sync::Notify,
    state: Mutex<PoolState>,
}

impl ConnectionPool {
    /// Build the pool. `io_timeout` bounds one dial, one probe and one
    /// reset statement (the proxy passes `connectTimeoutMs`).
    pub fn new(cfg: PoolConfig, io_timeout: Duration) -> Arc<Self> {
        let sem = Arc::new(Semaphore::new(cfg.max_connections));
        let pool = Arc::new(Self {
            cfg,
            io_timeout,
            sem,
            notify: tokio::sync::Notify::new(),
            state: Mutex::new(PoolState::default()),
        });
        pool.publish();
        pool
    }

    /// Configuration in force.
    pub fn config(&self) -> &PoolConfig {
        &self.cfg
    }

    /// Borrow an idle connection for `key`, else dial one. Bounded by
    /// `acquire_timeout_ms`.
    pub async fn acquire(
        &self,
        key: &PoolKey,
        tcp: &PgTcpCredential,
    ) -> Result<PooledConn, AcquireError> {
        let started = Instant::now();
        let mut evicted = false;
        loop {
            // 1. Reuse an idle socket of this key (LIFO, expiry-checked).
            if let Some(idle) = self.take_idle(key) {
                if let Some(conn) = self.try_reuse(idle, started).await {
                    return Ok(conn);
                }
            }
            // 2. A free capacity slot means a fresh dial.
            if let Ok(permit) = self.sem.clone().try_acquire_owned() {
                return self.dial(key, tcp, permit, started).await;
            }
            // 3. Full of idle sockets for other keys: reclaim the oldest
            // so a different tenant/database cannot starve a new key
            // until `idle_timeout_ms` elapses.
            if !evicted {
                evicted = true;
                if self.evict_oldest_idle() {
                    continue;
                }
            }
            // 4. Bounded queue: wake on a return (idle reuse) or on a
            // capacity free (close/expiry), never indefinitely.
            let wait_left = Duration::from_millis(self.cfg.acquire_timeout_ms)
                .checked_sub(started.elapsed())
                .unwrap_or_default();
            if wait_left.is_zero() {
                return Err(self.acquire_timeout(started));
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let permit = self.sem.clone().acquire_owned();
            tokio::select! {
                _ = tokio::time::sleep(wait_left) => return Err(self.acquire_timeout(started)),
                _ = notified => {}
                acquired = permit => match acquired {
                    Ok(permit) => {
                        // A return may have raced with the permit; prefer
                        // the parked socket over a new dial.
                        if let Some(idle) = self.take_idle(key) {
                            drop(permit);
                            if let Some(conn) = self.try_reuse(idle, started).await {
                                return Ok(conn);
                            }
                            continue;
                        }
                        return self.dial(key, tcp, permit, started).await;
                    }
                    // The semaphore is never closed; treat it as a denial.
                    Err(_) => return Err(self.acquire_timeout(started)),
                },
            }
        }
    }

    /// Return a connection after the client left. Rolls back an open
    /// transaction, resets session state (`reset_on_return`), and pools
    /// it — or closes it if any of that fails.
    pub async fn checkin(&self, mut conn: PooledConn, outcome: Checkin) {
        if !outcome.reusable {
            return self.close(conn, CloseReason::Broken);
        }
        // A transaction block left open by the client must never leak
        // into the next session, whatever `reset_on_return` says.
        if matches!(outcome.ready_status, Some(status) if status != b'I') {
            match self.run(&mut conn, "ROLLBACK").await {
                Ok(ready) if ready.status == b'I' => {}
                _ => return self.close(conn, CloseReason::Broken),
            }
        }
        if self.cfg.reset_on_return {
            match self.run(&mut conn, "DISCARD ALL").await {
                Ok(ready) if ready.status == b'I' && !ready.errored => {}
                _ => {
                    reg().inc("vrt_provider_pgproxy_pool_reset_failed_total", &[], 1.0);
                    return self.close(conn, CloseReason::Broken);
                }
            }
        }
        let PooledConn {
            rd,
            wr,
            key,
            opened,
            _permit,
        } = conn;
        let idle = IdleConn {
            rd,
            wr,
            key: key.clone(),
            opened,
            idle_since: Instant::now(),
            _permit,
        };
        {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.idle.entry(key).or_default().push(idle);
        }
        self.publish();
        // A queued acquisition waiting on this key can now be served
        // without a new dial.
        self.notify.notify_waiters();
    }

    /// Drop idle connections for tenants that left desired state — a
    /// revoked binding must not keep a live backend session around.
    pub fn evict_tenants(&self, live: &HashSet<String>) {
        let mut closed = 0usize;
        {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.idle.retain(|key, conns| {
                if live.contains(key.tenant()) {
                    return true;
                }
                closed += conns.len();
                false
            });
        }
        for _ in 0..closed {
            close_metric(CloseReason::TenantRevoked);
        }
        if closed > 0 {
            debug!(
                closed,
                "pgpool: idle connections dropped — tenant left desired state"
            );
            self.publish();
        }
    }

    /// Close idle connections past `idle_timeout_ms` / `max_lifetime_ms`.
    pub fn reap(&self) {
        let now = Instant::now();
        let cfg = self.cfg;
        let (mut idle_closed, mut lifetime_closed) = (0usize, 0usize);
        {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.idle.retain(|_, conns| {
                conns.retain(|c| match expired(c, now, &cfg) {
                    Some(CloseReason::Lifetime) => {
                        lifetime_closed += 1;
                        false
                    }
                    Some(_) => {
                        idle_closed += 1;
                        false
                    }
                    None => true,
                });
                !conns.is_empty()
            });
        }
        for _ in 0..idle_closed {
            close_metric(CloseReason::Idle);
        }
        for _ in 0..lifetime_closed {
            close_metric(CloseReason::Lifetime);
        }
        if idle_closed + lifetime_closed > 0 {
            self.publish();
        }
    }

    /// Reaper loop: bounded idle reclamation.
    pub async fn sweep(self: &Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(Duration::from_millis(REAP_MS)) => {}
            }
            self.reap();
        }
    }

    // ------------------------------------------------------------ internals

    /// Probe a parked socket and hand it over, or close it when the
    /// health check fails (the caller then dials a replacement).
    async fn try_reuse(&self, idle: IdleConn, started: Instant) -> Option<PooledConn> {
        match self.probe(idle).await {
            Ok(conn) => {
                reg().inc("vrt_provider_pgproxy_pool_reused_total", &[], 1.0);
                self.observe_wait(started);
                Some(conn)
            }
            Err(_) => {
                // Died while idle.
                close_metric(CloseReason::Broken);
                self.publish();
                None
            }
        }
    }

    /// Dial + authenticate a replacement backend, holding the capacity
    /// slot it was admitted with.
    async fn dial(
        &self,
        key: &PoolKey,
        tcp: &PgTcpCredential,
        permit: OwnedSemaphorePermit,
        started: Instant,
    ) -> Result<PooledConn, AcquireError> {
        let connect_started = Instant::now();
        match pgwire::connect(tcp, self.io_timeout).await {
            Ok(conn) => {
                reg().observe(
                    "vrt_provider_pgproxy_upstream_connect_ms",
                    &[],
                    connect_started.elapsed().as_secs_f64() * 1000.0,
                );
                reg().inc("vrt_provider_pgproxy_pool_opened_total", &[], 1.0);
                self.observe_wait(started);
                Ok(PooledConn {
                    rd: conn.rd,
                    wr: conn.wr,
                    key: key.clone(),
                    opened: Instant::now(),
                    _permit: permit,
                })
            }
            Err(e) => {
                drop(permit);
                self.publish();
                Err(AcquireError::Upstream(e))
            }
        }
    }

    /// Health-check a reused socket with `SELECT 1` before handing it to
    /// a tenant session.
    async fn probe(&self, idle: IdleConn) -> Result<PooledConn, WireError> {
        let IdleConn {
            mut rd,
            mut wr,
            key,
            opened,
            _permit,
            ..
        } = idle;
        let ready = match tokio::time::timeout(
            self.io_timeout,
            pgwire::query(&mut rd, &mut wr, "SELECT 1"),
        )
        .await
        {
            Ok(Ok(ready)) => ready,
            _ => return Err(WireError::Connect),
        };
        if ready.status != b'I' || ready.errored {
            return Err(WireError::Protocol);
        }
        Ok(PooledConn {
            rd,
            wr,
            key,
            opened,
            _permit,
        })
    }

    /// One bounded statement on a checked-out connection.
    async fn run(
        &self,
        conn: &mut PooledConn,
        sql: &'static str,
    ) -> Result<pgwire::Ready, WireError> {
        match tokio::time::timeout(
            self.io_timeout,
            pgwire::query(&mut conn.rd, &mut conn.wr, sql),
        )
        .await
        {
            Ok(ready) => ready,
            Err(_) => Err(WireError::Connect),
        }
    }

    /// Pop the newest non-expired idle connection for `key`. Expired
    /// sockets are dropped (their permits free up immediately).
    fn take_idle(&self, key: &PoolKey) -> Option<IdleConn> {
        let now = Instant::now();
        let cfg = self.cfg;
        let mut closed = Vec::new();
        let taken = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let mut taken = None;
            if let Some(conns) = st.idle.get_mut(key) {
                while let Some(c) = conns.pop() {
                    match expired(&c, now, &cfg) {
                        Some(reason) => closed.push(reason),
                        None => {
                            taken = Some(c);
                            break;
                        }
                    }
                }
                if conns.is_empty() {
                    st.idle.remove(key);
                }
            }
            taken
        };
        for reason in closed {
            close_metric(reason);
        }
        self.publish();
        taken
    }

    /// Close the oldest idle connection of any key; `false` when the
    /// pool holds no idle socket.
    fn evict_oldest_idle(&self) -> bool {
        let now = Instant::now();
        let victim = {
            let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let mut oldest: Option<(PoolKey, Instant)> = None;
            for (key, conns) in st.idle.iter() {
                let first_is_oldest = match conns.first() {
                    Some(c) => match &oldest {
                        Some((_, t)) => c.idle_since < *t,
                        None => true,
                    },
                    None => false,
                };
                if first_is_oldest {
                    oldest = Some((key.clone(), conns[0].idle_since));
                }
            }
            oldest.and_then(|(key, _)| {
                let conns = st.idle.get_mut(&key)?;
                let victim = if conns.is_empty() {
                    None
                } else {
                    Some(conns.remove(0))
                };
                if conns.is_empty() {
                    st.idle.remove(&key);
                }
                victim
            })
        };
        let Some(victim) = victim else {
            return false;
        };
        // An eviction that happens to hit an expired socket is still a
        // capacity reclaim; label it by its expiry when it has one.
        let reason = expired(&victim, now, &self.cfg).unwrap_or(CloseReason::Idle);
        drop(victim);
        close_metric(reason);
        self.publish();
        true
    }

    /// Close a checked-out connection: the permit (capacity slot) is
    /// released with the socket.
    fn close(&self, conn: PooledConn, reason: CloseReason) {
        close_metric(reason);
        drop(conn);
        self.publish();
        self.notify.notify_waiters();
    }

    fn acquire_timeout(&self, started: Instant) -> AcquireError {
        reg().inc("vrt_provider_pgproxy_pool_acquire_timeout_total", &[], 1.0);
        self.observe_wait(started);
        AcquireError::Timeout
    }

    fn observe_wait(&self, started: Instant) {
        reg().observe(
            "vrt_provider_pgproxy_pool_acquire_wait_ms",
            &[],
            started.elapsed().as_secs_f64() * 1000.0,
        );
    }

    /// Publish `pool_in_use` / `pool_idle` from one snapshot. Note the
    /// two step reads (idle map, permit count) can straddle a concurrent
    /// state change by one sample — the reaper republishes every sweep.
    fn publish(&self) {
        let idle: usize = {
            let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
            st.idle.values().map(Vec::len).sum()
        };
        let live = self
            .cfg
            .max_connections
            .saturating_sub(self.sem.available_permits());
        let in_use = live.saturating_sub(idle);
        reg().set("vrt_provider_pgproxy_pool_in_use", &[], in_use as f64);
        reg().set("vrt_provider_pgproxy_pool_idle", &[], idle as f64);
    }
}

/// Expiry check shared by the reaper and checkout.
fn expired(conn: &IdleConn, now: Instant, cfg: &PoolConfig) -> Option<CloseReason> {
    if now.duration_since(conn.opened) >= Duration::from_millis(cfg.max_lifetime_ms) {
        return Some(CloseReason::Lifetime);
    }
    if now.duration_since(conn.idle_since) >= Duration::from_millis(cfg.idle_timeout_ms) {
        return Some(CloseReason::Idle);
    }
    None
}

fn close_metric(reason: CloseReason) {
    reg().inc(
        "vrt_provider_pgproxy_pool_closed_total",
        &[("reason", reason.as_str())],
        1.0,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgmock::{backend, Mock};

    async fn mock_backend() -> (u16, Arc<Mock>) {
        backend().await
    }

    fn tcp(port: u16) -> PgTcpCredential {
        PgTcpCredential {
            host: "127.0.0.1".into(),
            port,
            user: "u".into(),
            password: "p".into(),
            database: "d".into(),
        }
    }

    fn cfg(max: usize, acquire: u64, idle: u64, lifetime: u64) -> PoolConfig {
        PoolConfig {
            max_connections: max,
            acquire_timeout_ms: acquire,
            idle_timeout_ms: idle,
            max_lifetime_ms: lifetime,
            reset_on_return: true,
        }
    }

    const CLEAN: Checkin = Checkin {
        reusable: true,
        ready_status: Some(b'I'),
    };

    #[tokio::test]
    async fn reuses_the_backend_socket_of_the_same_key() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(4, 1_000, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);

        let conn = pool.acquire(&key, &cred).await.expect("first dial");
        pool.checkin(conn, CLEAN).await;
        assert_eq!(mock.resets(), 1, "returned sessions are DISCARD ALL-ed");
        assert_eq!(mock.accepted(), 1);

        let conn = pool.acquire(&key, &cred).await.expect("reuse");
        assert_eq!(mock.accepted(), 1, "second session reused the socket");
        assert_eq!(mock.pings(), 1, "reuse is health-checked first");
        drop(conn);
    }

    #[tokio::test]
    async fn a_different_tenant_never_reuses_the_socket() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(4, 1_000, 30_000, 60_000), Duration::from_secs(2));
        let a = PoolKey::new("team-a", &cred);
        let b = PoolKey::new("team-b", &cred);
        let conn = pool.acquire(&a, &cred).await.expect("dial");
        pool.checkin(conn, CLEAN).await;
        let conn = pool.acquire(&b, &cred).await.expect("dial b");
        assert_eq!(mock.accepted(), 2, "tenant is part of the pool key");
        drop(conn);
    }

    #[tokio::test]
    async fn capacity_denies_after_the_acquire_timeout() {
        let (port, _mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(1, 150, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);
        let held = pool.acquire(&key, &cred).await.expect("first");

        let started = Instant::now();
        match pool.acquire(&key, &cred).await {
            Err(AcquireError::Timeout) => {}
            Err(other) => panic!("want timeout, got {:?}", other),
            Ok(_) => panic!("max_connections=1 must not admit a second socket"),
        }
        assert!(
            started.elapsed() >= Duration::from_millis(140),
            "the denial must be the bounded queue expiring, not an instant refusal"
        );
        drop(held);
    }

    #[tokio::test]
    async fn a_queued_acquire_is_served_when_the_socket_returns() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(1, 2_000, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);
        let held = pool.acquire(&key, &cred).await.expect("first");

        let waiter = {
            let pool = Arc::clone(&pool);
            let key = key.clone();
            let cred = cred.clone();
            tokio::spawn(async move { pool.acquire(&key, &cred).await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(60)).await;
        pool.checkin(held, CLEAN).await;
        waiter.await.unwrap().expect("queued acquire served");
        assert_eq!(mock.accepted(), 1, "the waiter reused the returned socket");
    }

    #[tokio::test]
    async fn a_socket_that_dies_while_idle_is_replaced() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(4, 1_000, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);
        pool.checkin(pool.acquire(&key, &cred).await.expect("dial"), CLEAN)
            .await;
        mock.fail_probe(true);
        // Probe fails -> the dead socket is discarded and a fresh one
        // takes its place in the same acquisition.
        let conn = pool.acquire(&key, &cred).await.expect("replacement");
        assert_eq!(mock.accepted(), 2);
        drop(conn);
    }

    #[tokio::test]
    async fn idle_and_lifetime_reclaimers_close_sockets() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let key = PoolKey::new("team-a", &cred);
        let io = Duration::from_secs(2);

        let idle_pool = ConnectionPool::new(cfg(4, 1_000, 120, 60_000), io);
        idle_pool
            .checkin(idle_pool.acquire(&key, &cred).await.expect("dial"), CLEAN)
            .await;
        tokio::time::sleep(Duration::from_millis(180)).await;
        idle_pool.reap();
        idle_pool.acquire(&key, &cred).await.expect("redial");
        assert_eq!(mock.accepted(), 2, "idle_timeout_ms reclaimed the socket");

        let lifetime_pool = ConnectionPool::new(cfg(4, 1_000, 60_000, 120), io);
        lifetime_pool
            .checkin(
                lifetime_pool.acquire(&key, &cred).await.expect("dial"),
                CLEAN,
            )
            .await;
        tokio::time::sleep(Duration::from_millis(180)).await;
        lifetime_pool.reap();
        lifetime_pool.acquire(&key, &cred).await.expect("redial");
        assert_eq!(mock.accepted(), 4, "max_lifetime_ms retired the socket");
    }

    #[tokio::test]
    async fn a_failed_reset_discards_the_socket() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(4, 1_000, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);
        let conn = pool.acquire(&key, &cred).await.expect("dial");
        mock.fail_reset(true);
        pool.checkin(conn, CLEAN).await;
        mock.fail_reset(false);
        // Nothing was parked: the next acquisition dials instead of
        // reusing a socket whose session state could not be reset.
        pool.acquire(&key, &cred).await.expect("redial");
        assert_eq!(mock.accepted(), 2);
        assert_eq!(mock.pings(), 0);
    }

    #[tokio::test]
    async fn a_dirty_boundary_is_never_pooled() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(4, 1_000, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);
        let conn = pool.acquire(&key, &cred).await.expect("dial");
        pool.checkin(conn, Checkin::discard()).await;
        pool.acquire(&key, &cred).await.expect("redial");
        assert_eq!(mock.accepted(), 2);
        assert_eq!(mock.resets(), 0, "a discarded socket is never reset");
    }

    #[tokio::test]
    async fn another_keys_idle_socket_is_evicted_for_capacity() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(1, 200, 30_000, 60_000), Duration::from_secs(2));
        let a = PoolKey::new("team-a", &cred);
        let b = PoolKey::new("team-b", &cred);
        pool.checkin(pool.acquire(&a, &cred).await.expect("dial a"), CLEAN)
            .await;
        let conn = pool.acquire(&b, &cred).await.expect("b must not starve");
        assert_eq!(mock.accepted(), 2);
        drop(conn);
    }

    #[tokio::test]
    async fn revoked_tenants_lose_their_idle_sockets() {
        let (port, mock) = mock_backend().await;
        let cred = tcp(port);
        let pool = ConnectionPool::new(cfg(4, 1_000, 30_000, 60_000), Duration::from_secs(2));
        let key = PoolKey::new("team-a", &cred);
        pool.checkin(pool.acquire(&key, &cred).await.expect("dial"), CLEAN)
            .await;
        pool.evict_tenants(&HashSet::from(["team-b".to_string()]));
        pool.acquire(&key, &cred).await.expect("redial");
        assert_eq!(mock.accepted(), 2);
    }

    #[test]
    fn pool_key_covers_tenant_endpoint_user_and_database() {
        let a = PoolKey::new("team-a", &tcp(5432));
        let b = PoolKey::new("team-b", &tcp(5432));
        assert_ne!(a, b);
        let mut other_db = tcp(5432);
        other_db.database = "other".into();
        assert_ne!(a, PoolKey::new("team-a", &other_db));
        let mut other_user = tcp(5432);
        other_user.user = "other".into();
        assert_ne!(a, PoolKey::new("team-a", &other_user));
        assert_ne!(a, PoolKey::new("team-a", &tcp(6432)));
        assert_eq!(a.tenant(), "team-a");
    }
}
