//! C1: per-tenant local Cache API (`caches.default` / `caches.open`)
//! service — workerd's `cacheApiOutbound` designator, one unix socket
//! per tenant. Tenant identity is bound at LISTENER setup: each tenant
//! worker's `cacheApiOutbound` resolves through `--external-addr` to
//! exactly one socket, and the socket owner is decided by config —
//! never by anything on the wire. `CF-Cache-Namespace` only subdivides
//! the owning tenant's key space.
//!
//! Wire contract (measured against workerd 1.20260916.1 +
//! `src/workerd/api/cache.c++`):
//! - `match`  → `GET <path>?<query>` + original request headers +
//!   `Cache-Control: only-if-cached` (+ `CF-Cache-Namespace: <name>`).
//!   Reply must carry `CF-Cache-Status`: `HIT` → workerd returns the
//!   outer response verbatim; `MISS`/`EXPIRED`/`UPDATING`/absent →
//!   `undefined`. (Non-GET requests never leave workerd — the
//!   `ignoreMethod`/`ignoreSearch`/`ignoreVary`/`cacheName` options
//!   are unimplemented in this workerd version.)
//! - `put`    → `PUT <path>?<query>`; body is a full serialized HTTP
//!   response (status line + headers + body; `Content-Length` or,
//!   for streamed bodies, `Transfer-Encoding: chunked` WITHOUT nested
//!   chunk framing — the bytes run to end of stream). Reply `204` =
//!   stored, `413` = not stored; `put()` resolves either way.
//! - `delete` → `PURGE <path>?<query>` (+ `X-Real-IP: 127.0.0.1`).
//!   Reply `200` → `true`, `404` → `false`, `429`/other → throws.
//!
//! Degradation contract: every internal failure degrades to
//! miss/no-store — this service never answers a miss-path request
//! with 5xx (a listener-down connect failure is a workerd-side
//! typed exception the tenant sees, not a silent miss and never a
//! fake hit).

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::net::{TcpListener, UnixListener};

use crate::metrics::registry;

/// `CF-Cache-Status` value workerd treats as a hit.
const CF_STATUS_HIT: &str = "HIT";
/// Miss-family values workerd maps to `undefined`.
const CF_STATUS_MISS: &str = "MISS";
const CF_STATUS_EXPIRED: &str = "EXPIRED";

/// workerd-side cap (`MAX_CACHE_NAME_LENGTH`); enforced defensively.
const MAX_NAMESPACE_LEN: usize = 1024;
/// Serialized-response header-block bound (status line + headers).
const MAX_HEADER_BLOCK: usize = 32 * 1024;
/// Header-count bound for a stored response.
const MAX_HEADER_COUNT: usize = 256;
/// Per-entry accounting overhead (key + entry metadata), bytes.
const ENTRY_OVERHEAD: usize = 128;
/// PUT body collection deadline — workerd streams the payload; an
/// unbounded wait would let a stalled pump pin a connection.
const PUT_BODY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Per-tenant/service capacity contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheLimits {
    /// Single-entry cap (stored headers + body + overhead).
    pub max_entry_bytes: usize,
    /// Per-tenant total byte cap.
    pub tenant_bytes: usize,
    /// Whole-service byte cap across tenants.
    pub global_bytes: usize,
    /// C1-r2 P1: in-flight PUT body bytes, service-wide. Concurrent
    /// puts otherwise let one tenant pin N×max_entry_bytes of host
    /// RAM before any stored-cap check could run.
    pub global_inflight_bytes: usize,
    /// In-flight PUT body bytes per tenant.
    pub tenant_inflight_bytes: usize,
}

impl CacheLimits {
    /// Suggested defaults (8 MiB / 128 MiB / 512 MiB stored,
    /// 64 MiB / 32 MiB in-flight).
    pub fn standard() -> Self {
        Self {
            max_entry_bytes: 8 * 1024 * 1024,
            tenant_bytes: 128 * 1024 * 1024,
            global_bytes: 512 * 1024 * 1024,
            global_inflight_bytes: 64 * 1024 * 1024,
            tenant_inflight_bytes: 32 * 1024 * 1024,
        }
    }

    /// Zero or inverted caps are meaningless — a cache that can store
    /// nothing is a config bug; fail closed at startup.
    pub fn validate(&self) -> io::Result<()> {
        if self.max_entry_bytes == 0
            || self.tenant_bytes == 0
            || self.global_bytes == 0
            || self.global_inflight_bytes == 0
            || self.tenant_inflight_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache limits must be > 0",
            ));
        }
        if self.max_entry_bytes > self.tenant_bytes || self.tenant_bytes > self.global_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache limits must satisfy entry <= tenant <= global",
            ));
        }
        // A CL-less PUT reserves max_entry + header-block — a tenant
        // inflight budget below that would reject EVERY put, so the
        // floor is structural, not advisory.
        if self.tenant_inflight_bytes < self.max_entry_bytes + MAX_HEADER_BLOCK
            || self.tenant_inflight_bytes > self.global_inflight_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache limits must satisfy max_entry+header_block <= tenant_inflight <= global_inflight",
            ));
        }
        Ok(())
    }
}

/// Listener kind — production binds unix sockets per tenant; tests on
/// this host's sandbox use loopback TCP (unix bind is denied there —
/// the same substitution `workflows_smoke.rs` makes for worker sockets).
#[derive(Clone, Debug)]
pub enum CacheBind {
    /// `<cacheRoot>/<seg(tenant)>.sock`.
    Unix(PathBuf),
    /// Loopback TCP — tests only.
    Tcp(SocketAddr),
}

/// `<cacheRoot>/<enc(tenantId)>.sock` — THE derivation shared by
/// assemble (which emits `--external-addr=vrt-cache=unix:<path>`) and
/// the supervisor (which binds the listener). Single source or the
/// two sides silently disagree.
pub fn tenant_socket_path(cache_root: &Path, tenant_id: &str) -> PathBuf {
    vrt_securefs::socket_path(cache_root, tenant_id)
}

/// `unix:<path>` external-addr string for a tenant — emitted into
/// `--external-addr` by assemble; bound by the supervisor.
pub fn tenant_socket_addr(cache_root: &Path, tenant_id: &str) -> io::Result<String> {
    let p = tenant_socket_path(cache_root, tenant_id);
    let s = p
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cache socket path utf8"))?;
    if s.len() > 100 {
        // 104B sun_path minus margin — the same class of bound the
        // binding-backend socket validation enforces.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache socket path exceeds sun_path bound",
        ));
    }
    Ok(format!("unix:{s}"))
}

/// Cache key inside one tenant's space. Tenant is NOT in the key —
/// it is the listener scope. `ns` is the `CF-Cache-Namespace`
/// subdivision (`""` = `caches.default`); `host` + `target`
/// reconstruct the request URL (`path?query` verbatim).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    ns: String,
    host: String,
    target: String,
}

/// One stored response, already parsed out of the PUT payload.
struct Entry {
    status: u16,
    /// Stored response headers (hop-by-hop stripped at put).
    headers: Vec<(String, String)>,
    body: Bytes,
    /// `Vary:` header names → the PUTTING request's values for them.
    vary: Vec<(String, String)>,
    /// Validators for conditional-match evaluation.
    etag: Option<String>,
    last_modified: Option<i64>,
    /// unix seconds — store time + computed freshness deadline.
    stored_at: i64,
    fresh_until: i64,
    /// Accounting size (headers + body + key + overhead).
    size: usize,
    /// LRU order stamp — higher = more recently used.
    seq: u64,
}

#[derive(Default)]
struct TenantCache {
    entries: HashMap<Key, Entry>,
    /// seq → key (per-tenant LRU order index).
    order: BTreeMap<u64, Key>,
    bytes: usize,
    /// C1-r2 P1: reserved PUT-body bytes in flight for this tenant.
    inflight: usize,
}

#[derive(Default)]
struct Store {
    seq: u64,
    tenants: HashMap<String, TenantCache>,
    /// seq → (tenant, key) — the global LRU order index.
    global_order: BTreeMap<u64, (String, Key)>,
    global_bytes: usize,
    /// C1-r2 P1: reserved PUT-body bytes in flight, service-wide.
    global_inflight: usize,
}

/// C1-r2 P1: RAII in-flight PUT-byte permit. `acquire` reserves an
/// estimate (Content-Length, else the read cap) under the store
/// mutex — one critical section decides both budgets atomically.
/// `shrink` releases the over-estimate once the real byte count is
/// known; `Drop` returns whatever remains on EVERY exit path —
/// success, parse failure, timeout, client abort.
struct InflightPermit {
    store: Arc<Mutex<Store>>,
    tenant: String,
    bytes: usize,
}

impl InflightPermit {
    fn acquire(
        store: &Arc<Mutex<Store>>,
        limits: &CacheLimits,
        tenant: &str,
        bytes: usize,
    ) -> Option<Self> {
        let mut s = lock(store);
        if s.global_inflight + bytes > limits.global_inflight_bytes {
            return None;
        }
        let tc = s.tenants.entry(tenant.to_string()).or_default();
        if tc.inflight + bytes > limits.tenant_inflight_bytes {
            return None;
        }
        tc.inflight += bytes;
        s.global_inflight += bytes;
        let g = s.global_inflight;
        drop(s);
        registry().set("vrt_host_cache_inflight_bytes", &[], g as f64);
        Some(Self {
            store: store.clone(),
            tenant: tenant.to_string(),
            bytes,
        })
    }

    /// Actual bytes are known post-collect — hand back the
    /// over-reservation so concurrent puts aren't charged for the
    /// worst-case estimate.
    fn shrink(&mut self, to: usize) {
        if to >= self.bytes {
            return;
        }
        let delta = self.bytes - to;
        self.bytes = to;
        let mut s = lock(&self.store);
        s.global_inflight = s.global_inflight.saturating_sub(delta);
        if let Some(tc) = s.tenants.get_mut(&self.tenant) {
            tc.inflight = tc.inflight.saturating_sub(delta);
        }
        let g = s.global_inflight;
        drop(s);
        registry().set("vrt_host_cache_inflight_bytes", &[], g as f64);
    }
}

impl Drop for InflightPermit {
    fn drop(&mut self) {
        let mut s = lock(&self.store);
        s.global_inflight = s.global_inflight.saturating_sub(self.bytes);
        let cleanup = if let Some(tc) = s.tenants.get_mut(&self.tenant) {
            tc.inflight = tc.inflight.saturating_sub(self.bytes);
            tc.entries.is_empty() && tc.inflight == 0
        } else {
            false
        };
        if cleanup {
            s.tenants.remove(&self.tenant);
        }
        let g = s.global_inflight;
        drop(s);
        registry().set("vrt_host_cache_inflight_bytes", &[], g as f64);
    }
}

/// Typed reason code for a cache-service init failure (metric label;
/// whitelist-stable, never free text).
pub fn cache_init_failure_reason(e: &io::Error) -> &'static str {
    match e.kind() {
        // symlink / foreign owner / mode != 0711 (e.g. kubelet fsGroup
        // recursion turning the tree into 2770).
        io::ErrorKind::PermissionDenied => "root-insecure",
        // zero limits, nul in path, root not a directory.
        io::ErrorKind::InvalidInput => "config-invalid",
        _ => "io",
    }
}

/// Record the outcome of `CacheService::new` at host startup. The init
/// failure DEGRADES the capability (caches.* throws explicitly) rather
/// than failing the host, so it MUST be observable: a warn-level
/// structured log, the error counter and a collectable
/// `vrt_host_cache_degraded{reason}` gauge (1 = degraded, 0 = serving)
/// an operator/alert can read without grepping logs.
pub fn record_cache_init<T>(result: &io::Result<T>) {
    let reg = registry();
    match result {
        Ok(_) => reg.set("vrt_host_cache_degraded", &[("reason", "none")], 0.0),
        Err(e) => {
            let reason = cache_init_failure_reason(e);
            tracing::warn!(
                event = "cache_degraded",
                reason,
                error = %e,
                remediation = "workDir/cache must be a host-owned 0711 directory; check the PV was not recursively chmod'd by fsGroup (fsGroupChangePolicy: OnRootMismatch)",
                "cache service init failed - degraded to unconfigured (caches.* throws explicitly)"
            );
            reg.inc(
                "vrt_host_cache_listener_errors_total",
                &[("tenant", "*"), ("stage", "init")],
                1.0,
            );
            reg.set("vrt_host_cache_degraded", &[("reason", reason)], 1.0);
        }
    }
}

/// Process-wide service: one store, many per-tenant listeners.
/// `root` is the unix-socket directory (a `0700` dir owned by the
/// host uid — [`vrt_securefs::ensure_secure_dir`] contract); `None`
/// disables unix binds entirely (tests bind TCP).
pub struct CacheService {
    store: Arc<Mutex<Store>>,
    limits: CacheLimits,
    root: Option<PathBuf>,
}

impl CacheService {
    /// `limits.validate()` failing is a startup error — a zero-cap
    /// cache that silently dropped every put would fake the feature.
    pub fn new(root: Option<PathBuf>, limits: CacheLimits) -> io::Result<Self> {
        limits.validate()?;
        if let Some(r) = &root {
            Self::ensure_cache_root(r)?;
        }
        Ok(Self {
            store: Arc::new(Mutex::new(Store::default())),
            limits,
            root,
        })
    }

    /// C1-r2 P0: idempotent secure-root contract for the cache socket
    /// dir. `ensure_secure_dir` demands exactly 0700 — correct for
    /// host-only dirs, wrong here: tenant uids must TRAVERSE the dir
    /// (connect to their socket) while never listing or writing it,
    /// hence 0711. Re-validating that mode on every start keeps the
    /// check restart-idempotent; `ensure_secure_dir` stays untouched
    /// for its real 0700 callers.
    ///
    /// Contract — exactly one of:
    ///   - path absent → create the leaf atomically at 0711 (no
    ///     permissive mkdir/chmod window), then validate;
    ///   - path present → must be a NON-SYMLINK directory owned by our
    ///     euid with mode EXACTLY 0711.
    /// Every other shape (symlink, file, foreign owner, group/world
    /// writable, 0700/0755/0777) is refused — fail closed, never
    /// silently chmod someone else's directory.
    fn ensure_cache_root(dir: &Path) -> io::Result<()> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        match std::fs::symlink_metadata(dir) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = dir.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let c =
                    std::ffi::CString::new(dir.as_os_str().as_encoded_bytes()).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "cache root path nul")
                    })?;
// SAFETY: the path is a valid NUL-terminated CString and mode 0o711 is a valid mode_t.
                if unsafe { libc::mkdir(c.as_ptr(), 0o711) } != 0 {
                    let e = io::Error::last_os_error();
                    // A concurrent creator is fine — fall through to the
                    // shared validation and judge the result, not the race.
                    if e.raw_os_error() != Some(libc::EEXIST) {
                        return Err(e);
                    }
                } else {
                    // mkdir honors umask — a restrictive one (e.g. 077)
                    // would land the dir at 0700 and fail validation.
                    // The dir is provably fresh and ours, so an explicit
                    // chmod here carries no foreign-dir risk.
// SAFETY: the path is a valid NUL-terminated CString and 0o711 is a valid mode_t.
                    if unsafe { libc::chmod(c.as_ptr(), 0o711) } != 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
            }
            Err(e) => return Err(e),
        }
        let meta = std::fs::symlink_metadata(dir)?;
        if meta.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "cache root is a symlink",
            ));
        }
        if !meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache root is not a directory",
            ));
        }
// SAFETY: geteuid(2) has no preconditions and cannot fail.
        if meta.uid() != unsafe { libc::geteuid() } {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "cache root is owned by another uid",
            ));
        }
        if meta.permissions().mode() & 0o777 != 0o711 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "cache root mode must be 0711",
            ));
        }
        Ok(())
    }

    /// Limits snapshot — supervisor/tests may introspect.
    pub fn limits(&self) -> CacheLimits {
        self.limits
    }

    /// `unix:<path>` external-addr string for this tenant — the SAME
    /// path the assemble side emits and [`Self::serve_tenant`] binds.
    pub fn tenant_addr(&self, tenant_id: &str) -> io::Result<String> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cache root unset"))?;
        tenant_socket_addr(root, tenant_id)
    }

    /// Bind this tenant's production unix listener — SYNC variant for
    /// the supervisor actor loop (`start_pending_launch` is not
    /// async; `UnixListener::bind` is a std call and `tokio::spawn`
    /// resolves the ambient runtime). `owner` = the tenant's process
    /// uid/gid (from `spec.isolation`); when set, the socket is
    /// chowned so ONLY that tenant's workerd can connect — the
    /// cross-tenant probe dies on EACCES. `None` (unisolated
    /// standalone) keeps host-uid ownership.
    pub fn serve_tenant(
        &self,
        tenant_id: &str,
        owner: Option<(u32, u32)>,
    ) -> io::Result<CacheListener> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cache root unset"))?;
        let path = tenant_socket_path(root, tenant_id);
        let listener = Self::bind_unix(&path)?;
        if let Some((uid, gid)) = owner {
            // Socket stays 0600 — ownership moves to the tenant. The
            // caller runs as euid 0 whenever isolation is on (the
            // launcher fails closed otherwise), so chown cannot fail
            // on permission; a real failure is a hard error.
            let cpath = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "socket path nul"))?;
// SAFETY: path is a valid NUL-terminated CString; uid/gid are plain integers and the result is checked.
            if unsafe { libc::chown(cpath.as_ptr(), uid, gid) } != 0 {
                let _ = std::fs::remove_file(&path);
                return Err(io::Error::last_os_error());
            }
        }
        Ok(self.spawn_serve(tenant_id, ListenerKind::Unix(listener), Some(path)))
    }

    /// Unix bind with stale-socket takeover: a STALE same-named socket
    /// file (dead previous generation) is replaced; any other file
    /// type blocks the bind (fail closed — we never unlink foreign
    /// files).
    fn bind_unix(path: &Path) -> io::Result<UnixListener> {
        match std::fs::metadata(path) {
            Ok(m) => {
                use std::os::unix::fs::FileTypeExt;
                if !m.file_type().is_socket() {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "cache socket path holds a non-socket file",
                    ));
                }
                std::fs::remove_file(path)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // F1: 0600 from birth — the cache root is 0711 by design, so a
        // bind-then-chmod window here is REAL (any local uid can reach
        // the socket file). Staged bind + atomic rename closes it.
        let std_listener = vrt_securefs::bind_unix_private(path, 0o600)?;
        std_listener.set_nonblocking(true)?;
        Ok(UnixListener::from_std(std_listener)?)
    }

    /// Bind `bind` and serve this tenant's cache until the returned
    /// listener is dropped. Async general form — tests bind TCP.
    pub async fn serve(&self, tenant_id: &str, bind: CacheBind) -> io::Result<CacheListener> {
        let (listener, guard_path) = match &bind {
            CacheBind::Unix(path) => (
                ListenerKind::Unix(Self::bind_unix(path)?),
                Some(path.clone()),
            ),
            CacheBind::Tcp(addr) => (ListenerKind::Tcp(TcpListener::bind(addr).await?), None),
        };
        Ok(self.spawn_serve(tenant_id, listener, guard_path))
    }

    /// Shared accept-loop spawn — the tenant identity is pinned to
    /// THIS listener scope; request headers can never select one.
    fn spawn_serve(
        &self,
        tenant_id: &str,
        listener: ListenerKind,
        guard_path: Option<PathBuf>,
    ) -> CacheListener {
        let scope = Arc::new(TenantScope {
            service: ServiceInner {
                store: self.store.clone(),
                limits: self.limits,
            },
            tenant: tenant_id.to_string(),
        });
        let router = Router::new().fallback(any(handle)).with_state(scope);
        let svc = router.into_make_service();
        let task = tokio::spawn(async move {
            match listener {
                ListenerKind::Unix(l) => {
                    let _ = axum::serve(l, svc).await;
                }
                ListenerKind::Tcp(l) => {
                    let _ = axum::serve(l, svc).await;
                }
            }
        });
        CacheListener {
            task: Some(task),
            socket_path: guard_path,
        }
    }

    /// Drop the tenant's whole key space (undeploy/actor teardown) —
    /// cache semantics permit total loss; bounded bytes matter more.
    pub fn remove_tenant(&self, tenant_id: &str) {
        let mut store = lock(&self.store);
        let Some(tc) = store.tenants.remove(tenant_id) else {
            set_tenant_gauges(tenant_id, 0, 0);
            return;
        };
        for seq in tc.order.keys() {
            store.global_order.remove(seq);
        }
        store.global_bytes = store.global_bytes.saturating_sub(tc.bytes);
        set_tenant_gauges(tenant_id, 0, 0);
    }
}

enum ListenerKind {
    Unix(UnixListener),
    Tcp(TcpListener),
}

/// RAII handle for one bound socket — aborts the accept task and
/// unlinks the socket file on drop (actor drop / tests).
pub struct CacheListener {
    task: Option<tokio::task::JoinHandle<()>>,
    socket_path: Option<PathBuf>,
}

impl Drop for CacheListener {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        if let Some(p) = &self.socket_path {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Interior-mutable view handed to each tenant's connections — the
/// shared `Arc<Mutex<Store>>` keeps the global byte cap enforceable.
#[derive(Clone)]
struct ServiceInner {
    store: Arc<Mutex<Store>>,
    limits: CacheLimits,
}

struct TenantScope {
    service: ServiceInner,
    tenant: String,
}

fn lock(m: &Mutex<Store>) -> std::sync::MutexGuard<'_, Store> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn set_tenant_gauges(tenant: &str, bytes: usize, entries: usize) {
    let r = registry();
    r.set("vrt_host_cache_bytes", &[("tenant", tenant)], bytes as f64);
    r.set(
        "vrt_host_cache_entries",
        &[("tenant", tenant)],
        entries as f64,
    );
}

fn inc(tenant: &str, name: &'static str, reason: Option<&'static str>) {
    let r = registry();
    match reason {
        Some(reason) => r.inc(name, &[("tenant", tenant), ("reason", reason)], 1.0),
        None => r.inc(name, &[("tenant", tenant)], 1.0),
    }
}

// ============================ request handling ============================

async fn handle(State(scope): State<Arc<TenantScope>>, req: Request<Body>) -> Response<Body> {
    match *req.method() {
        Method::GET => handle_match(scope, &req),
        Method::PUT => handle_put(scope, req).await,
        ref m if m.as_str() == "PURGE" => handle_delete(scope, &req),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, CF_STATUS_MISS),
    }
}

/// Namespace/host/target from the wire. workerd enforces the 1024B
/// namespace cap at `open()`; we re-check defensively. A bad/missing
/// namespace is a key-parse failure, never a tenant leak (tenant is
/// the socket scope, not wire data).
fn key_parts(req: &Request<Body>) -> Option<(String, String, String)> {
    let headers = req.headers();
    let ns = match headers.get("cf-cache-namespace") {
        Some(v) => {
            let s = v.to_str().ok()?;
            if s.len() > MAX_NAMESPACE_LEN {
                return None;
            }
            s.to_string()
        }
        None => String::new(),
    };
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|h| h.to_ascii_lowercase())
        .or_else(|| {
            req.uri()
                .authority()
                .map(|a| a.as_str().to_ascii_lowercase())
        })
        .unwrap_or_default();
    let target = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    Some((ns, host, target))
}

fn key_of(ns: String, host: String, target: String) -> Key {
    Key { ns, host, target }
}

fn text_response(status: StatusCode, cache_status: &str) -> Response<Body> {
    let mut b = Response::builder().status(status);
    if !cache_status.is_empty() {
        b = b.header("cf-cache-status", cache_status);
    }
    b.body(Body::empty()).unwrap()
}

fn miss(tenant: &str, reason: &'static str) -> Response<Body> {
    inc(tenant, "vrt_host_cache_misses_total", Some(reason));
    text_response(
        StatusCode::GATEWAY_TIMEOUT,
        if reason == "expired" {
            CF_STATUS_EXPIRED
        } else {
            CF_STATUS_MISS
        },
    )
}

fn handle_match(scope: Arc<TenantScope>, req: &Request<Body>) -> Response<Body> {
    let tenant = scope.tenant.as_str();
    let Some((ns, host, target)) = key_parts(req) else {
        return miss(tenant, "bad_key");
    };
    let key = key_of(ns, host, target);
    let now = now_secs();
    let mut store = lock(&scope.service.store);
    enum Act {
        Expired,
        Vary,
        Hit { not_modified: bool },
    }
    let act = {
        let Some(entry) = store
            .tenants
            .get(tenant)
            .and_then(|tc| tc.entries.get(&key))
        else {
            return miss(tenant, "absent");
        };
        if entry.fresh_until <= now {
            Act::Expired
        } else if !vary_matches(entry, req.headers()) {
            Act::Vary
        } else {
            Act::Hit {
                not_modified: not_modified(req.headers(), entry),
            }
        }
    };
    match act {
        Act::Expired => {
            remove_entry(&mut store, tenant, &key);
            set_tenant_gauges(
                tenant,
                tenant_bytes(&store, tenant),
                tenant_entries(&store, tenant),
            );
            miss(tenant, "expired")
        }
        Act::Vary => miss(tenant, "vary"),
        Act::Hit { not_modified } => {
            let old_seq = store.tenants[tenant].entries[&key].seq;
            touch(&mut store, tenant, &key, old_seq);
            inc(tenant, "vrt_host_cache_hits_total", None);
            let entry = &store.tenants[tenant].entries[&key];
            let age = now.saturating_sub(entry.stored_at).max(0);
            let mut b = if not_modified {
                Response::builder().status(StatusCode::NOT_MODIFIED)
            } else {
                Response::builder()
                    .status(StatusCode::from_u16(entry.status).unwrap_or(StatusCode::OK))
            };
            for (n, v) in &entry.headers {
                if let Ok(hv) = HeaderValue::from_str(v) {
                    b = b.header(n.as_str(), hv);
                }
            }
            b = b.header("cf-cache-status", CF_STATUS_HIT);
            b = b.header("age", age.to_string());
            if not_modified {
                b.body(Body::empty()).unwrap()
            } else {
                b.body(Body::from(entry.body.clone())).unwrap()
            }
        }
    }
}

fn handle_delete(scope: Arc<TenantScope>, req: &Request<Body>) -> Response<Body> {
    let tenant = scope.tenant.as_str();
    let Some((ns, host, target)) = key_parts(req) else {
        return text_response(StatusCode::NOT_FOUND, "");
    };
    let key = key_of(ns, host, target);
    let mut store = lock(&scope.service.store);
    if remove_entry(&mut store, tenant, &key) {
        set_tenant_gauges(
            tenant,
            tenant_bytes(&store, tenant),
            tenant_entries(&store, tenant),
        );
        inc(tenant, "vrt_host_cache_evictions_total", Some("purge"));
        text_response(StatusCode::OK, "")
    } else {
        text_response(StatusCode::NOT_FOUND, "")
    }
}

/// Reject reason → 413 (the contract's not-stored code).
fn put_reject(tenant: &str, reason: &'static str) -> Response<Body> {
    inc(tenant, "vrt_host_cache_put_rejected_total", Some(reason));
    text_response(StatusCode::PAYLOAD_TOO_LARGE, "")
}

async fn handle_put(scope: Arc<TenantScope>, req: Request<Body>) -> Response<Body> {
    let tenant = scope.tenant.as_str();
    let Some((ns, host, target)) = key_parts(&req) else {
        return put_reject(tenant, "bad_key");
    };
    // Request-side no-store per RFC 9111 §3.
    if header_has_cc_directive(req.headers(), "cache-control", "no-store") {
        return put_reject(tenant, "no_store");
    }
    let req_authorized = req.headers().contains_key(header::AUTHORIZATION);
    let vary_req_headers = req.headers().clone();
    let max_read = scope.service.limits.max_entry_bytes + MAX_HEADER_BLOCK;
    // C1-r2 P1: reserve in-flight bytes BEFORE touching the body —
    // Content-Length when present (clamped to the read cap), else the
    // cap itself. Refusal is `busy` without a single byte read; CF
    // semantics allow a put to simply not cache.
    let reserve = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .map(|cl| cl.min(max_read))
        .unwrap_or(max_read);
    let Some(mut permit) =
        InflightPermit::acquire(&scope.service.store, &scope.service.limits, tenant, reserve)
    else {
        return put_reject(tenant, "busy");
    };
    let collect = collect_bounded(req.into_body(), max_read);
    let raw = match tokio::time::timeout(PUT_BODY_TIMEOUT, collect).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(BoundedBody::TooLarge)) => return put_reject(tenant, "too_large"),
        Ok(Err(BoundedBody::Incomplete)) => return put_reject(tenant, "incomplete"),
        Err(_) => return put_reject(tenant, "timeout"),
    };
    permit.shrink(raw.len());
    let parsed = match parse_serialized_response(&raw) {
        Some(p) => p,
        None => return put_reject(tenant, "malformed"),
    };
    if let Some(reason) = storable(&parsed, req_authorized) {
        return put_reject(tenant, reason);
    }
    let now = now_secs();
    let Some(fresh_until) = fresh_until(&parsed, now) else {
        return put_reject(tenant, "no_freshness");
    };
    let (key, entry) = build_entry(
        Key { ns, host, target },
        parsed,
        &vary_req_headers,
        now,
        fresh_until,
    );
    if entry.size > scope.service.limits.max_entry_bytes {
        return put_reject(tenant, "too_large");
    }
    let mut store = lock(&scope.service.store);
    insert_entry(&mut store, tenant, key, entry, scope.service.limits);
    inc(tenant, "vrt_host_cache_puts_total", None);
    text_response(StatusCode::NO_CONTENT, "")
}

enum BoundedBody {
    TooLarge,
    Incomplete,
}

/// Read a request body with a hard byte cap — over-cap → TooLarge
/// (we stop reading; dropping the connection mid-pump is safe: the
/// cache contract treats disconnect as "entry not committed", which
/// is exactly our outcome).
async fn collect_bounded(body: Body, cap: usize) -> Result<Bytes, BoundedBody> {
    let mut stream = body.into_data_stream();
    let mut buf = Vec::new();
    while let Some(frame) = stream.next().await {
        let chunk = frame.map_err(|_| BoundedBody::Incomplete)?;
        if buf.len() + chunk.len() > cap {
            return Err(BoundedBody::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

// ============================ stored-response parse ============================

struct ParsedPut {
    status: u16,
    headers: Vec<(String, String)>,
    /// Raw body bytes of the inner response.
    body: Bytes,
}

/// Parse workerd's PUT payload: `HTTP/1.x <code> <text>\r\n` +
/// headers + CRLF + body. Body framing per the workerd contract:
/// `Content-Length` = exact bytes; otherwise raw bytes to end of
/// stream (workerd never nests chunk framing even when the inner
/// message says `Transfer-Encoding: chunked`).
fn parse_serialized_response(buf: &[u8]) -> Option<ParsedPut> {
    // Status line.
    let line_end = find_subslice(buf, b"\r\n")?;
    let status_line = std::str::from_utf8(&buf[..line_end]).ok()?;
    if !status_line.starts_with("HTTP/") {
        return None;
    }
    // `HTTP/<ver> <code> <text>` — the first space terminates the
    // VERSION token; the code sits between it and the next space.
    let after_ver = status_line.get(5..)?;
    let sp = after_ver.find(' ')?;
    let rest = after_ver.get(sp + 1..)?;
    let end = rest.find(' ').unwrap_or(rest.len());
    let status: u16 = rest.get(..end)?.trim().parse().ok()?;
    if !(100..=999).contains(&status) {
        return None;
    }
    // Header block.
    let mut pos = line_end + 2;
    let mut headers: Vec<(String, String)> = Vec::new();
    loop {
        if pos > MAX_HEADER_BLOCK || pos > buf.len() || headers.len() >= MAX_HEADER_COUNT {
            return None;
        }
        let eol = find_subslice(&buf[pos..], b"\r\n").map(|o| pos + o)?;
        if eol == pos {
            pos += 2;
            break;
        }
        let line = std::str::from_utf8(&buf[pos..eol]).ok()?;
        if line.starts_with(' ') || line.starts_with('\t') {
            // obs-fold: append to previous value.
            let (_, v) = headers.last_mut()?;
            v.push(' ');
            v.push_str(line.trim());
        } else {
            let colon = line.find(':')?;
            let name = line[..colon].trim().to_string();
            let value = line[colon + 1..].trim().to_string();
            headers.push((name, value));
        }
        pos = eol + 2;
    }
    let content_length =
        single_header(&headers, "content-length").and_then(|v| v.trim().parse::<u64>().ok());
    let remaining = &buf[pos..];
    let body: Bytes = if let Some(cl) = content_length {
        if remaining.len() != cl as usize {
            // Truncated or smuggled — never store partial entries.
            return None;
        }
        Bytes::copy_from_slice(remaining)
    } else {
        Bytes::copy_from_slice(remaining)
    };
    Some(ParsedPut {
        status,
        headers,
        body,
    })
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn single_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn header_has_token(headers: &[(String, String)], name: &str, token: &str) -> bool {
    headers.iter().any(|(n, v)| {
        n.eq_ignore_ascii_case(name) && v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token))
    })
}

fn has_header(headers: &[(String, String)], name: &str) -> bool {
    headers.iter().any(|(n, _)| n.eq_ignore_ascii_case(name))
}

fn header_has_cc_directive(h: &HeaderMap, name: &str, directive: &str) -> bool {
    h.get_all(name).iter().any(|v| {
        v.to_str()
            .map(|s| {
                s.split(',')
                    .any(|d| d.trim().split('=').next() == Some(directive))
            })
            .unwrap_or(false)
    })
}

/// Lowercased directive list of a stored response's `Cache-Control`.
fn cc_directives(headers: &[(String, String)]) -> Vec<String> {
    headers
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case("cache-control"))
        .flat_map(|(_, v)| {
            v.split(',')
                .map(|d| d.trim().to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// `Some(reason)` when RFC/CF semantics forbid storing `parsed`.
/// `req_authorized` = the PUT request carried `Authorization`.
fn storable(parsed: &ParsedPut, req_authorized: bool) -> Option<&'static str> {
    match parsed.status {
        // workerd refuses these JS-side; refuse defensively so a
        // hand-rolled client can't smuggle them in.
        206 => return Some("status_206"),
        304 => return Some("status_304"),
        _ => {}
    }
    if has_header(&parsed.headers, "set-cookie") {
        return Some("set_cookie");
    }
    if header_has_token(&parsed.headers, "vary", "*") {
        return Some("vary_star");
    }
    let cc = cc_directives(&parsed.headers);
    for d in &cc {
        // Qualified forms (`private="field"`, `no-cache="field"`)
        // still mark the response uncacheable for a cache that can't
        // evaluate revalidation — reject in the conservative
        // direction.
        if d == "no-store"
            || d == "private"
            || d.starts_with("private=")
            || d == "no-cache"
            || d.starts_with("no-cache=")
        {
            return Some("no_store");
        }
    }
    if req_authorized
        && !cc
            .iter()
            .any(|d| d == "public" || d == "must-revalidate" || d.starts_with("s-maxage"))
    {
        // Shared-cache rule (RFC 9111 §3.5): authorized requests are
        // not storable unless the response explicitly re-allows it.
        return Some("authorized");
    }
    None
}

/// Freshness deadline (unix secs) from response headers —
/// `s-maxage` > `max-age` > `Expires`; stored `Age` is resident age.
fn fresh_until(parsed: &ParsedPut, now: i64) -> Option<i64> {
    let cc = cc_directives(&parsed.headers);
    let mut ttl: Option<i64> = None;
    for d in &cc {
        if let Some(v) = d.strip_prefix("s-maxage=") {
            ttl = v.trim_matches('"').parse().ok();
            break;
        }
    }
    if ttl.is_none() {
        for d in &cc {
            if let Some(v) = d.strip_prefix("max-age=") {
                ttl = v.trim_matches('"').parse().ok();
                break;
            }
        }
    }
    if let Some(t) = ttl {
        if t <= 0 {
            return None;
        }
        let age = single_header(&parsed.headers, "age")
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(0)
            .max(0);
        return Some(now + t.saturating_sub(age).max(0));
    }
    let expires = single_header(&parsed.headers, "expires").and_then(http_date)?;
    let date = single_header(&parsed.headers, "date")
        .and_then(http_date)
        .unwrap_or(now);
    let t = expires - date;
    if t <= 0 {
        return None;
    }
    Some(now + t)
}

/// HTTP-date → unix secs: IMF-fixdate (RFC 9110 preferred), rfc850,
/// and asctime — the three forms a stored `Date`/`Expires` may take.
fn http_date(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(d) = chrono::DateTime::parse_from_rfc2822(s) {
        return Some(d.timestamp());
    }
    for fmt in ["%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"] {
        if let Ok(d) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(d.and_utc().timestamp());
        }
    }
    None
}

/// Headers never stored — they belong to a connection, not a
/// representation (recomputed by hyper on serve).
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

fn build_entry(
    key: Key,
    parsed: ParsedPut,
    req_headers: &HeaderMap,
    now: i64,
    fresh_until: i64,
) -> (Key, Entry) {
    let mut headers: Vec<(String, String)> = Vec::with_capacity(parsed.headers.len());
    let mut vary_names: Vec<String> = Vec::new();
    for (n, v) in &parsed.headers {
        if n.eq_ignore_ascii_case("vary") {
            for part in v.split(',') {
                let p = part.trim();
                if !p.is_empty() && p != "*" {
                    vary_names.push(p.to_ascii_lowercase());
                }
            }
        }
        if is_hop_by_hop(n) || n.eq_ignore_ascii_case("age") {
            // `Age` is resident time — recomputed on every hit;
            // storing it would double-emit a stale value.
            continue;
        }
        // Header names/values must be valid HTTP — an invalid name
        // would make `Response::builder().header()` error at SERVE
        // time (panic inside the task). Drop untransmittable pairs
        // at store time instead.
        if http::header::HeaderName::from_bytes(n.as_bytes()).is_err()
            || HeaderValue::from_str(v).is_err()
        {
            continue;
        }
        headers.push((n.clone(), v.clone()));
    }
    let vary: Vec<(String, String)> = vary_names
        .into_iter()
        .map(|name| {
            let val = req_headers
                .get_all(&*name)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .collect::<Vec<_>>()
                .join(", ");
            (name, val)
        })
        .collect();
    let etag = single_header(&parsed.headers, "etag").map(|s| s.to_string());
    let last_modified = single_header(&parsed.headers, "last-modified").and_then(http_date);
    let size = ENTRY_OVERHEAD
        + key.ns.len()
        + key.host.len()
        + key.target.len()
        + parsed
            .headers
            .iter()
            .map(|(n, v)| n.len() + v.len() + 4)
            .sum::<usize>()
        + parsed.body.len();
    let entry = Entry {
        status: parsed.status,
        headers,
        body: parsed.body,
        vary,
        etag,
        last_modified,
        stored_at: now,
        fresh_until,
        size,
        seq: 0,
    };
    (key, entry)
}

// ============================ store ops ============================

fn tenant_bytes(store: &Store, tenant: &str) -> usize {
    store.tenants.get(tenant).map(|t| t.bytes).unwrap_or(0)
}

fn tenant_entries(store: &Store, tenant: &str) -> usize {
    store
        .tenants
        .get(tenant)
        .map(|t| t.entries.len())
        .unwrap_or(0)
}

/// Bump LRU stamps in both order indexes.
fn touch(store: &mut Store, tenant: &str, key: &Key, old_seq: u64) {
    store.seq += 1;
    let new = store.seq;
    if let Some(tc) = store.tenants.get_mut(tenant) {
        tc.order.remove(&old_seq);
        tc.order.insert(new, key.clone());
        if let Some(e) = tc.entries.get_mut(key) {
            e.seq = new;
        }
    }
    store.global_order.remove(&old_seq);
    store
        .global_order
        .insert(new, (tenant.to_string(), key.clone()));
}

/// Remove `key` from `tenant`'s space; returns whether it existed.
fn remove_entry(store: &mut Store, tenant: &str, key: &Key) -> bool {
    let Some(tc) = store.tenants.get_mut(tenant) else {
        return false;
    };
    let Some(e) = tc.entries.remove(key) else {
        return false;
    };
    tc.order.remove(&e.seq);
    tc.bytes = tc.bytes.saturating_sub(e.size);
    store.global_order.remove(&e.seq);
    store.global_bytes = store.global_bytes.saturating_sub(e.size);
    // C1-r2 P1: an in-flight PUT's reservation lives in `tc.inflight`
    // — deleting the entry mid-flight would zero the tenant budget
    // view and let further puts over-reserve. Keep the shell while
    // reservations outstanding; `InflightPermit::drop` collects it.
    if tc.entries.is_empty() && tc.inflight == 0 {
        store.tenants.remove(tenant);
    }
    true
}

fn insert_entry(store: &mut Store, tenant: &str, key: Key, mut entry: Entry, limits: CacheLimits) {
    remove_entry(store, tenant, &key);
    store.seq += 1;
    let seq = store.seq;
    entry.seq = seq;
    let tc = store.tenants.entry(tenant.to_string()).or_default();
    tc.bytes += entry.size;
    store.global_bytes += entry.size;
    tc.order.insert(seq, key.clone());
    store
        .global_order
        .insert(seq, (tenant.to_string(), key.clone()));
    tc.entries.insert(key, entry);
    // Evict: tenant cap first (own coldest), then global cap (world's
    // coldest — the victim may be a DIFFERENT tenant; their gauge
    // reflects their freed bytes).
    loop {
        let over = store
            .tenants
            .get(tenant)
            .map(|tc| tc.bytes > limits.tenant_bytes)
            .unwrap_or(false);
        if !over {
            break;
        }
        let Some(victim) = store
            .tenants
            .get(tenant)
            .and_then(|tc| tc.order.iter().next().map(|(_, k)| k.clone()))
        else {
            break;
        };
        evict(store, tenant, &victim, "tenant_cap");
    }
    // C1-r2 P2: fair-share global eviction — a flooding tenant must
    // not drive another tenant's hit-rate to zero. fair_share =
    // global_cap / active_tenants; when the pool is over cap, SOME
    // tenant is necessarily over its share (if all were ≤ share the
    // total would be ≤ cap — contradiction). Victim = coldest entry
    // belonging to an over-share tenant; tenants at or under their
    // share keep their entries. Fallback to the global coldest keeps
    // the hard cap enforced even if accounting ever drifted.
    while store.global_bytes > limits.global_bytes {
        let active = store
            .tenants
            .values()
            .filter(|t| t.bytes > 0)
            .count()
            .max(1);
        let fair_share = limits.global_bytes / active;
        let victim = store
            .global_order
            .iter()
            .find(|(_, (t, _))| {
                store
                    .tenants
                    .get(t)
                    .map(|tc| tc.bytes > fair_share)
                    .unwrap_or(false)
            })
            .map(|(_, v)| v.clone())
            .or_else(|| store.global_order.iter().next().map(|(_, v)| v.clone()));
        let Some((victim_tenant, victim_key)) = victim else {
            break;
        };
        evict(store, &victim_tenant, &victim_key, "global_cap");
        set_tenant_gauges(
            &victim_tenant,
            tenant_bytes(store, &victim_tenant),
            tenant_entries(store, &victim_tenant),
        );
    }
    set_tenant_gauges(
        tenant,
        tenant_bytes(store, tenant),
        tenant_entries(store, tenant),
    );
}

fn evict(store: &mut Store, tenant: &str, key: &Key, reason: &'static str) {
    if remove_entry(store, tenant, key) {
        inc(tenant, "vrt_host_cache_evictions_total", Some(reason));
    }
}

// ============================ request semantics ============================

/// Compare the request's vary-header values against those captured
/// at put time (trim + ASCII-lowercase — the same normalization on
/// both sides).
fn vary_matches(entry: &Entry, req: &HeaderMap) -> bool {
    entry.vary.iter().all(|(name, stored_val)| {
        let cur = req
            .get_all(name.as_str())
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect::<Vec<_>>()
            .join(", ");
        norm_header(&cur) == norm_header(stored_val)
    })
}

fn norm_header(v: &str) -> String {
    v.trim().to_ascii_lowercase()
}

/// `If-None-Match` (weak tag comparison, `*` counts) else
/// `If-Modified-Since` — RFC 9110 §13 against stored validators.
fn not_modified(req: &HeaderMap, entry: &Entry) -> bool {
    if let Some(inm) = req.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if inm.trim() == "*" {
            return true;
        }
        let Some(etag) = &entry.etag else {
            return false;
        };
        return inm.split(',').any(|t| weak_tag_eq(t.trim(), etag));
    }
    if let (Some(ims), Some(lm)) = (
        req.get(header::IF_MODIFIED_SINCE)
            .and_then(|v| v.to_str().ok())
            .and_then(http_date),
        entry.last_modified,
    ) {
        return lm <= ims;
    }
    false
}

/// RFC weak comparison: `W/` prefix ignored, opaque-tag byte-equal.
fn weak_tag_eq(a: &str, b: &str) -> bool {
    fn strip(t: &str) -> &str {
        let t = t.trim();
        t.strip_prefix("W/")
            .or_else(|| t.strip_prefix("w/"))
            .unwrap_or(t)
    }
    strip(a) == strip(b)
}

// ============================ tests ============================

#[cfg(test)]
mod tests {
    use super::*;

    fn scope_for(tenant: &str, store: &Arc<Mutex<Store>>, limits: CacheLimits) -> Arc<TenantScope> {
        Arc::new(TenantScope {
            service: ServiceInner {
                store: store.clone(),
                limits,
            },
            tenant: tenant.to_string(),
        })
    }

    fn small_limits() -> CacheLimits {
        CacheLimits {
            max_entry_bytes: 8 * 1024,
            tenant_bytes: 64 * 1024,
            global_bytes: 128 * 1024,
            global_inflight_bytes: 256 * 1024,
            tenant_inflight_bytes: 128 * 1024,
        }
    }

    fn serialized(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        let mut v = format!("HTTP/1.1 {status} OK\r\n").into_bytes();
        for (n, val) in headers {
            v.extend_from_slice(format!("{n}: {val}\r\n").as_bytes());
        }
        v.extend_from_slice(b"\r\n");
        v.extend_from_slice(body);
        v
    }

    async fn put(
        scope: Arc<TenantScope>,
        ns: Option<&str>,
        target: &str,
        raw: Vec<u8>,
    ) -> Response<Body> {
        let mut b = Request::builder()
            .method(Method::PUT)
            .uri(format!("http://ex.com{target}"))
            .header(header::HOST, "ex.com");
        if let Some(n) = ns {
            b = b.header("cf-cache-namespace", n);
        }
        handle(State(scope), b.body(Body::from(raw)).unwrap()).await
    }

    async fn put_req(
        scope: Arc<TenantScope>,
        target: &str,
        extra: &[(&'static str, &'static str)],
        raw: Vec<u8>,
    ) -> Response<Body> {
        let mut b = Request::builder()
            .method(Method::PUT)
            .uri(format!("http://ex.com{target}"))
            .header(header::HOST, "ex.com");
        for (n, v) in extra {
            b = b.header(*n, *v);
        }
        handle(State(scope), b.body(Body::from(raw)).unwrap()).await
    }

    async fn get_req(
        scope: Arc<TenantScope>,
        ns: Option<&str>,
        target: &str,
        extra: &[(&'static str, &'static str)],
    ) -> Response<Body> {
        let mut b = Request::builder()
            .method(Method::GET)
            .uri(format!("http://ex.com{target}"))
            .header(header::HOST, "ex.com");
        if let Some(n) = ns {
            b = b.header("cf-cache-namespace", n);
        }
        for (n, v) in extra {
            b = b.header(*n, *v);
        }
        handle(State(scope), b.body(Body::empty()).unwrap()).await
    }

    async fn get(scope: Arc<TenantScope>, ns: Option<&str>, target: &str) -> Response<Body> {
        get_req(scope, ns, target, &[]).await
    }

    async fn purge(scope: Arc<TenantScope>, target: &str) -> Response<Body> {
        handle(
            State(scope),
            Request::builder()
                .method(Method::from_bytes(b"PURGE").unwrap())
                .uri(format!("http://ex.com{target}"))
                .header(header::HOST, "ex.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
    }

    async fn body_bytes(r: Response<Body>) -> Bytes {
        http_body_util::BodyExt::collect(r.into_body())
            .await
            .unwrap()
            .to_bytes()
    }

    fn hit(r: &Response<Body>) -> bool {
        r.headers()
            .get("cf-cache-status")
            .and_then(|v| v.to_str().ok())
            == Some("HIT")
    }

    // ---- core lifecycle: put → hit → delete → miss ----

    #[tokio::test]
    async fn c1_put_match_hit_delete_miss() {
        let store = Arc::new(Mutex::new(Store::default()));
        let s = scope_for("t1", &store, small_limits());
        let r = put(
            s.clone(),
            None,
            "/a?x=1",
            serialized(
                200,
                &[("Cache-Control", "max-age=300"), ("ETag", "\"v1\"")],
                b"hello",
            ),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        let r = get(s.clone(), None, "/a?x=1").await;
        assert!(hit(&r), "stored entry must HIT");
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers().get("etag").unwrap(), "\"v1\"");
        assert_eq!(&body_bytes(r).await[..], b"hello");
        // Query is part of the key — different query misses.
        assert!(!hit(&get(s.clone(), None, "/a?x=2").await));
        assert_eq!(purge(s.clone(), "/a?x=1").await.status(), StatusCode::OK);
        assert_eq!(
            purge(s.clone(), "/a?x=1").await.status(),
            StatusCode::NOT_FOUND
        );
        assert!(!hit(&get(s, None, "/a?x=1").await));
    }

    // ---- storable() rejection matrix ----

    #[tokio::test]
    async fn c1_storable_rejects() {
        let store = Arc::new(Mutex::new(Store::default()));
        let s = scope_for("t1", &store, small_limits());
        let cases: &[(&str, Vec<u8>)] = &[
            (
                "set-cookie",
                serialized(
                    200,
                    &[("Set-Cookie", "a=b"), ("Cache-Control", "max-age=60")],
                    b"x",
                ),
            ),
            (
                "no-store",
                serialized(200, &[("Cache-Control", "no-store")], b"x"),
            ),
            (
                "private",
                serialized(200, &[("Cache-Control", "private")], b"x"),
            ),
            (
                "no-cache",
                serialized(200, &[("Cache-Control", "no-cache")], b"x"),
            ),
            (
                "vary-star",
                serialized(200, &[("Vary", "*"), ("Cache-Control", "max-age=60")], b"x"),
            ),
            (
                "status-206",
                serialized(206, &[("Cache-Control", "max-age=60")], b"x"),
            ),
            (
                "status-304",
                serialized(304, &[("Cache-Control", "max-age=60")], b""),
            ),
            ("no-freshness", serialized(200, &[], b"x")),
            (
                "expired-already",
                serialized(200, &[("Cache-Control", "max-age=0")], b"x"),
            ),
            ("malformed", b"NOT HTTP AT ALL".to_vec()),
            (
                "cl-mismatch",
                serialized(
                    200,
                    &[("Cache-Control", "max-age=60"), ("Content-Length", "99")],
                    b"short",
                ),
            ),
        ];
        for (tag, raw) in cases {
            let target = format!("/{tag}");
            let r = put(s.clone(), None, &target, raw.clone()).await;
            assert_eq!(
                r.status(),
                StatusCode::PAYLOAD_TOO_LARGE,
                "case {tag} must reject"
            );
            assert!(
                !hit(&get(s.clone(), None, &target).await),
                "{tag} must not store"
            );
        }
        // Authorized request without explicit re-allow → reject…
        let r = put_req(
            s.clone(),
            "/authz",
            &[(header::AUTHORIZATION.as_str(), "Bearer t")],
            serialized(200, &[("Cache-Control", "max-age=60")], b"x"),
        )
        .await;
        assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
        // …but `public` re-allows it (RFC 9111 §3.5).
        let r = put_req(
            s.clone(),
            "/authz2",
            &[(header::AUTHORIZATION.as_str(), "Bearer t")],
            serialized(200, &[("Cache-Control", "public, max-age=60")], b"x"),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        assert!(hit(&get(s.clone(), None, "/authz2").await));
    }

    // ---- TTL expiry ----

    #[tokio::test]
    async fn c1_ttl_expiry_and_expires() {
        let store = Arc::new(Mutex::new(Store::default()));
        let s = scope_for("t1", &store, small_limits());
        put(
            s.clone(),
            None,
            "/ttl",
            serialized(200, &[("Cache-Control", "max-age=300")], b"x"),
        )
        .await;
        // Force expiry — deterministic (no sleep).
        {
            let mut st = lock(&store);
            let tc = st.tenants.get_mut("t1").unwrap();
            let key = tc.entries.keys().next().unwrap().clone();
            tc.entries.get_mut(&key).unwrap().fresh_until = now_secs() - 1;
        }
        let r = get(s.clone(), None, "/ttl").await;
        assert_eq!(
            r.headers().get("cf-cache-status").unwrap(),
            "EXPIRED",
            "expired entry reports EXPIRED and is evicted"
        );
        assert!(lock(&store).tenants.get("t1").is_none());
        // Expires header path (no Cache-Control): future date stores.
        let r = put(
            s.clone(),
            None,
            "/exp",
            serialized(
                200,
                &[
                    ("Expires", "Thu, 06 Nov 2994 08:49:37 GMT"),
                    ("Date", "Sun, 06 Nov 1994 08:49:37 GMT"),
                ],
                b"x",
            ),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        assert!(hit(&get(s.clone(), None, "/exp").await));
    }

    // ---- vary + conditional ----

    #[tokio::test]
    async fn c1_vary_and_conditionals() {
        let store = Arc::new(Mutex::new(Store::default()));
        let s = scope_for("t1", &store, small_limits());
        let r = put_req(
            s.clone(),
            "/v",
            &[("accept-encoding", "gzip")],
            serialized(
                200,
                &[
                    ("Cache-Control", "max-age=300"),
                    ("Vary", "Accept-Encoding"),
                    ("ETag", "\"e1\""),
                    ("Last-Modified", "Sun, 06 Nov 1994 08:49:37 GMT"),
                ],
                b"gz",
            ),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        // Same vary value (normalized) → hit.
        let r = get_req(s.clone(), None, "/v", &[("accept-encoding", "GZIP")]).await;
        assert!(hit(&r), "normalized vary value must hit");
        // Different value → miss.
        let r = get_req(s.clone(), None, "/v", &[("accept-encoding", "br")]).await;
        assert!(!hit(&r), "different vary value must miss");
        // If-None-Match weak comparison → 304, empty body.
        let r = get_req(
            s.clone(),
            None,
            "/v",
            &[("accept-encoding", "gzip"), ("if-none-match", "W/\"e1\"")],
        )
        .await;
        assert_eq!(r.status(), StatusCode::NOT_MODIFIED);
        assert!(body_bytes(r).await.is_empty());
        // If-Modified-Since ≥ stored Last-Modified → 304.
        let r = get_req(
            s.clone(),
            None,
            "/v",
            &[
                ("accept-encoding", "gzip"),
                ("if-modified-since", "Thu, 06 Nov 2994 08:49:37 GMT"),
            ],
        )
        .await;
        assert_eq!(r.status(), StatusCode::NOT_MODIFIED);
    }

    // ---- namespaces + tenant isolation ----

    #[tokio::test]
    async fn c1_namespace_and_tenant_isolation() {
        let store = Arc::new(Mutex::new(Store::default()));
        let a = scope_for("tenant-a", &store, small_limits());
        let b = scope_for("tenant-b", &store, small_limits());
        put(
            a.clone(),
            Some("ns1"),
            "/k",
            serialized(200, &[("Cache-Control", "max-age=60")], b"A-ns1"),
        )
        .await;
        put(
            a.clone(),
            Some("ns2"),
            "/k",
            serialized(200, &[("Cache-Control", "max-age=60")], b"A-ns2"),
        )
        .await;
        put(
            a.clone(),
            None,
            "/k",
            serialized(200, &[("Cache-Control", "max-age=60")], b"A-def"),
        )
        .await;
        assert_eq!(
            &body_bytes(get(a.clone(), Some("ns1"), "/k").await).await[..],
            b"A-ns1"
        );
        assert_eq!(
            &body_bytes(get(a.clone(), Some("ns2"), "/k").await).await[..],
            b"A-ns2"
        );
        assert_eq!(
            &body_bytes(get(a.clone(), None, "/k").await).await[..],
            b"A-def"
        );
        // Tenant B reads NOTHING from A — same key, same namespace.
        for ns in [Some("ns1"), Some("ns2"), None] {
            assert!(
                !hit(&get(b.clone(), ns, "/k").await),
                "tenant-b must not see tenant-a entries (ns={ns:?})"
            );
        }
        // Namespace >1024B → reject (workerd caps at open(); re-check).
        let long = "n".repeat(1025);
        let r = put(
            a.clone(),
            Some(&long),
            "/k",
            serialized(200, &[("Cache-Control", "max-age=60")], b"x"),
        )
        .await;
        assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
        // Purging A's space never touches B.
        put(
            b.clone(),
            None,
            "/k",
            serialized(200, &[("Cache-Control", "max-age=60")], b"B"),
        )
        .await;
        CacheService {
            store: store.clone(),
            limits: small_limits(),
            root: None,
        }
        .remove_tenant("tenant-a");
        assert!(!hit(&get(a.clone(), Some("ns1"), "/k").await));
        assert!(
            hit(&get(b.clone(), None, "/k").await),
            "B survives A's purge"
        );
    }

    // ---- bounded: entry cap, tenant LRU, global LRU ----

    #[tokio::test]
    async fn c1_byte_caps_and_lru() {
        let store = Arc::new(Mutex::new(Store::default()));
        let limits = CacheLimits {
            max_entry_bytes: 900,
            tenant_bytes: 2048,
            global_bytes: 4096,
            global_inflight_bytes: 128 * 1024,
            tenant_inflight_bytes: 64 * 1024,
        };
        let s = scope_for("t1", &store, limits);
        // Single entry over max_entry_bytes → reject.
        let r = put(
            s.clone(),
            None,
            "/big",
            serialized(200, &[("Cache-Control", "max-age=60")], &vec![0u8; 2000]),
        )
        .await;
        assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
        // Fill tenant cap: 4×~600B entries > 2048 → coldest evicted.
        for i in 0..4u8 {
            let body = vec![b'a' + i; 500];
            let r = put(
                s.clone(),
                None,
                &format!("/e{i}"),
                serialized(200, &[("Cache-Control", "max-age=300")], &body),
            )
            .await;
            assert_eq!(r.status(), StatusCode::NO_CONTENT);
        }
        {
            let st = lock(&store);
            let tc = st.tenants.get("t1").unwrap();
            assert!(tc.bytes <= limits.tenant_bytes, "tenant cap enforced");
            assert!(
                !tc.entries
                    .contains_key(&key_of(String::new(), "ex.com".into(), "/e0".into())),
                "coldest evicted"
            );
        }
        // Global cap: t2's inserts may evict t1's coldest (shared pool).
        let limits2 = CacheLimits {
            max_entry_bytes: 900,
            tenant_bytes: 4096,
            global_bytes: 1500,
            global_inflight_bytes: 128 * 1024,
            tenant_inflight_bytes: 64 * 1024,
        };
        let store2 = Arc::new(Mutex::new(Store::default()));
        let t1 = scope_for("g1", &store2, limits2);
        let t2 = scope_for("g2", &store2, limits2);
        put(
            t1.clone(),
            None,
            "/o",
            serialized(200, &[("Cache-Control", "max-age=60")], &vec![1u8; 500]),
        )
        .await;
        put(
            t2.clone(),
            None,
            "/p",
            serialized(200, &[("Cache-Control", "max-age=60")], &vec![2u8; 500]),
        )
        .await;
        put(
            t2.clone(),
            None,
            "/q",
            serialized(200, &[("Cache-Control", "max-age=60")], &vec![3u8; 500]),
        )
        .await;
        let st = lock(&store2);
        assert!(
            st.global_bytes <= limits2.global_bytes,
            "global cap enforced"
        );
        drop(st);
        // C1-r2 fair share: g2 (~1.3KiB) is over fair_share (1500/2);
        // g1 (~0.7KiB) is under → g2's OWN coldest is evicted, g1's
        // entry survives the flood — a noisy tenant cannot zero
        // another tenant's hit rate.
        assert!(hit(&get(t1.clone(), None, "/o").await));
        assert!(!hit(&get(t2.clone(), None, "/p").await));
        assert!(hit(&get(t2.clone(), None, "/q").await));
    }

    // ---- concurrent puts of the same key: replace, exact accounting ----

    #[tokio::test]
    async fn c1_concurrent_put_same_key() {
        let store = Arc::new(Mutex::new(Store::default()));
        let s = scope_for("t1", &store, small_limits());
        for i in 0..8 {
            let r = put(
                s.clone(),
                None,
                "/race",
                serialized(
                    200,
                    &[("Cache-Control", "max-age=60")],
                    format!("v{i}").as_bytes(),
                ),
            )
            .await;
            assert_eq!(r.status(), StatusCode::NO_CONTENT);
        }
        let st = lock(&store);
        let tc = st.tenants.get("t1").unwrap();
        assert_eq!(tc.entries.len(), 1, "same key never duplicates");
        assert_eq!(tc.order.len(), 1);
        assert_eq!(
            tc.entries.values().next().unwrap().size,
            tc.bytes,
            "byte accounting stays exact under replace"
        );
    }

    // ---- limits contract ----

    #[test]
    fn c1_limits_validate() {
        assert!(small_limits().validate().is_ok());
        assert!(CacheLimits {
            max_entry_bytes: 0,
            ..small_limits()
        }
        .validate()
        .is_err());
        assert!(CacheLimits {
            max_entry_bytes: 2048,
            tenant_bytes: 1024,
            global_bytes: 4096,
            global_inflight_bytes: 128 * 1024,
            tenant_inflight_bytes: 64 * 1024,
        }
        .validate()
        .is_err());
    }

    // ---- real TCP listener: serve → HTTP roundtrip → drop unbinds ----

    #[tokio::test]
    async fn c1_listener_roundtrip_tcp() {
        let svc = CacheService::new(None, small_limits()).unwrap();
        // Port 0 then read back the bound addr via a probe connect is
        // awkward with our CacheListener (addr not exposed); use a
        // fixed high port range scan.
        let mut listener = None;
        let mut bound = String::new();
        for port in 18590..18600 {
            let addr = format!("127.0.0.1:{port}");
            match svc.serve("ta", CacheBind::Tcp(addr.parse().unwrap())).await {
                Ok(l) => {
                    listener = Some(l);
                    bound = addr;
                    break;
                }
                Err(_) => continue,
            }
        }
        let listener = listener.expect("bind a test port");
        let client = reqwest::Client::new();
        let url = format!("http://{bound}/rt?z=1");
        // PUT serialized response for tenant ta.
        let raw = serialized(200, &[("Cache-Control", "max-age=300")], b"RT");
        let r = client
            .put(&url)
            .header("host", "ex.com")
            .body(raw)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 204);
        let r = client
            .get(&url)
            .header("host", "ex.com")
            .header("cache-control", "only-if-cached")
            .send()
            .await
            .unwrap();
        assert_eq!(r.headers().get("cf-cache-status").unwrap(), "HIT");
        assert_eq!(r.bytes().await.unwrap().as_ref(), b"RT");
        drop(listener);
        // After drop the accept loop is dead — a new connect fails.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(client
            .get(&url)
            .header("host", "ex.com")
            .send()
            .await
            .is_err());
    }

    /// Unix bind policy: a stale same-name SOCKET file is taken over;
    /// any other file type blocks the bind (fail closed — never
    /// unlink a foreign file).
    #[tokio::test]
    async fn c1_unix_bind_stale_takeover_and_non_socket_refused() {
        let svc = CacheService::new(None, small_limits()).unwrap();
        // Relative `../target/...` keeps sun_path < 104 (the absolute
        // path would overflow) and stays inside the workspace —
        // sandboxed environments deny socket binds in $TMPDIR.
        // Cargo runs lib tests with cwd = the crate dir.
        let dir = PathBuf::from("../target/tmp-c1").join(format!("ut-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("t1.sock");
        // A regular file at the socket path must block the bind —
        // the refusal happens BEFORE bind(), so it holds even in
        // sandboxes where socket binds are denied.
        std::fs::write(&sock, b"not-a-socket").unwrap();
        assert!(svc
            .serve("t-unix", CacheBind::Unix(sock.clone()))
            .await
            .is_err());
        // The foreign file survives — we never unlink non-sockets.
        assert!(sock.exists());
        std::fs::remove_file(&sock).unwrap();
        // Takeover paths need a real bind — probe once, skip in
        // sandboxes that deny unix sockets (same as d1_smoke).
        match std::os::unix::net::UnixListener::bind(dir.join("probe.sock")) {
            Ok(l) => {
                drop(l);
                std::fs::remove_file(dir.join("probe.sock")).ok();
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                eprintln!("SKIP: unix socket bind denied ({e}) — sandboxed run");
                std::fs::remove_dir_all(&dir).ok();
                return;
            }
            Err(e) => panic!("probe unix bind: {e}"),
        }
        // First bind succeeds; drop + rebind takes over the stale
        // socket file left behind.
        let l1 = svc
            .serve("t-unix", CacheBind::Unix(sock.clone()))
            .await
            .unwrap();
        drop(l1);
        assert!(!sock.exists(), "drop unlinks the socket file");
        let l2 = svc
            .serve("t-unix", CacheBind::Unix(sock.clone()))
            .await
            .unwrap();
        // And an orphaned socket file (simulating a killed process)
        // is taken over too.
        drop(l2);
        let _orphan = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        drop(_orphan);
        assert!(svc
            .serve("t-unix", CacheBind::Unix(sock.clone()))
            .await
            .is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Bug1: a kubelet-fsGroup-damaged root (2771) is refused AND the
    /// degrade is observable: typed reason + gauge=1 on failure, 0 on ok.
    #[test]
    fn bug1_cache_degrade_is_observable() {
        use std::os::unix::fs::PermissionsExt;
        let base = PathBuf::from("../target/tmp-bug1").join(format!("root-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        let bad = base.join("cache");
        std::fs::create_dir(&bad).unwrap();
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o2771)).unwrap();
        let r = CacheService::new(Some(bad.clone()), small_limits());
        let e = r.as_ref().err().expect("2771 root must be refused");
        assert_eq!(cache_init_failure_reason(e), "root-insecure");
        record_cache_init(&r);
        let text = registry().render();
        assert!(
            text.contains("vrt_host_cache_degraded{reason=\"root-insecure\"} 1"),
            "{text}"
        );
        assert!(text.contains("vrt_host_cache_listener_errors_total"), "{text}");
        // Healed to 0711 -> serves, gauge reset.
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o711)).unwrap();
        let ok = CacheService::new(Some(bad), small_limits());
        assert!(ok.is_ok());
        record_cache_init(&ok);
        assert!(registry().render().contains("vrt_host_cache_degraded{reason=\"none\"} 0"));
        std::fs::remove_dir_all(&base).ok();
    }

    // ---- C1-r2 P0: idempotent cache-root contract ----

    /// `ensure_cache_root` accepts exactly: absent (create 0711) or a
    /// host-owned non-symlink dir at 0711 — restart-safe. Everything
    /// else is refused. This was the docker-e2e crash-loop bug:
    /// `ensure_secure_dir` demanded 0700 while the service needed
    /// 0711, so EVERY restart failed init.
    #[test]
    fn c1r2_cache_root_idempotent() {
        use std::os::unix::fs::PermissionsExt;
        let base = PathBuf::from("../target/tmp-c1r2").join(format!("root-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        let root = base.join("cache");
        // First create → 0711; second new() on the SAME root also
        // succeeds — the restart path.
        let _s1 = CacheService::new(Some(root.clone()), small_limits()).unwrap();
        assert_eq!(
            std::fs::symlink_metadata(&root)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o711
        );
        let _s2 = CacheService::new(Some(root.clone()), small_limits()).unwrap();
        // Pre-made dirs at wrong modes are refused, never chmod'd.
        for mode in [0o755u32, 0o777, 0o700] {
            let bad = base.join(format!("bad-{mode:o}"));
            std::fs::create_dir(&bad).unwrap();
            std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                CacheService::new(Some(bad.clone()), small_limits()).is_err(),
                "mode {mode:o} must be refused"
            );
            // And the dir is left untouched — no silent chmod of a
            // foreign-mode directory.
            assert_eq!(
                std::fs::symlink_metadata(&bad)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                mode
            );
        }
        // A symlink even to a VALID root is refused (fs identity must
        // not be laundered through links).
        let link = base.join("link");
        std::os::unix::fs::symlink(&root, &link).unwrap();
        assert!(CacheService::new(Some(link), small_limits()).is_err());
        // A regular file is refused.
        let file = base.join("file");
        std::fs::write(&file, b"x").unwrap();
        assert!(CacheService::new(Some(file), small_limits()).is_err());
        std::fs::remove_dir_all(&base).ok();
    }

    // ---- C1-r2 P1: in-flight PUT byte budget ----

    /// Permits: atomic two-budget acquire, RAII release, shrink-to-
    /// actual — every path leaves the counters exact.
    #[test]
    fn c1r2_inflight_permit_accounting() {
        let store = Arc::new(Mutex::new(Store::default()));
        let lim = CacheLimits {
            max_entry_bytes: 512,
            tenant_bytes: 64 * 1024,
            global_bytes: 128 * 1024,
            global_inflight_bytes: 2048,
            tenant_inflight_bytes: 1024,
        };
        // Tenant budget: 600+600 > 1024 → second acquire refused.
        let a = InflightPermit::acquire(&store, &lim, "t", 600).unwrap();
        assert!(InflightPermit::acquire(&store, &lim, "t", 600).is_none());
        // Other tenant fits under its own budget AND global.
        let b = InflightPermit::acquire(&store, &lim, "u", 600).unwrap();
        assert_eq!(lock(&store).global_inflight, 1200);
        // Global budget: 1200+900 > 2048 → refused even though the
        // tenant side would fit.
        assert!(InflightPermit::acquire(&store, &lim, "v", 900).is_none());
        // Shrink hands back the over-estimate.
        let mut a = a;
        a.shrink(100);
        assert_eq!(lock(&store).global_inflight, 700);
        assert_eq!(lock(&store).tenants.get("t").unwrap().inflight, 100);
        // RAII release on drop — no leak.
        drop(a);
        drop(b);
        let s = lock(&store);
        assert_eq!(s.global_inflight, 0);
        assert!(s.tenants.get("t").is_none(), "emptied shell collected");
        assert!(s.tenants.get("u").is_none());
    }

    /// Handle-level busy: a held reservation starves the tenant
    /// budget → PUT rejects `busy` (413) without reading the body;
    /// releasing the permit unblocks it.
    #[tokio::test]
    async fn c1r2_inflight_busy_then_release() {
        let store = Arc::new(Mutex::new(Store::default()));
        let lim = small_limits();
        let s = scope_for("t", &store, lim);
        // Hold nearly all of the tenant inflight budget — a CL-less
        // put reserves max_entry+32KiB (40KiB here).
        let hold =
            InflightPermit::acquire(&store, &lim, "t", lim.tenant_inflight_bytes - 1024).unwrap();
        let r = put(
            s.clone(),
            None,
            "/blocked",
            serialized(200, &[("Cache-Control", "max-age=60")], b"X"),
        )
        .await;
        assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE, "busy");
        drop(hold);
        // Released → the same put now stores.
        let r = put(
            s.clone(),
            None,
            "/blocked",
            serialized(200, &[("Cache-Control", "max-age=60")], b"X"),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        assert_eq!(lock(&store).global_inflight, 0);
    }

    /// Abort path: a PUT whose body stream never yields holds its
    /// reservation; aborting the handler task releases it (RAII runs
    /// on drop), not just on clean completion.
    #[tokio::test]
    async fn c1r2_inflight_released_on_abort() {
        let store = Arc::new(Mutex::new(Store::default()));
        let s = scope_for("t", &store, small_limits());
        let body =
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
        let req = Request::builder()
            .method("PUT")
            .uri("/big")
            .header("host", "ex.com")
            .body(body)
            .unwrap();
        let task = tokio::spawn(handle(State(s), req));
        for _ in 0..100 {
            if lock(&store).global_inflight > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // No Content-Length → the FULL read cap (8KiB+32KiB=40KiB)
        // is reserved while the stream is pending.
        assert_eq!(lock(&store).global_inflight, 40 * 1024);
        task.abort();
        let _ = task.await;
        assert_eq!(lock(&store).global_inflight, 0, "abort frees budget");
        assert!(lock(&store).tenants.get("t").is_none());
    }
}
