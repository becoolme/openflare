//! Analytics Engine ingest — the receiving half of workerd's
//! `analyticsEngine` binding (`env.<binding>.writeDataPoint()`).
//!
//! workerd serializes one `AnalyticsEngineEvent` per call as Cap'n
//! Proto JSON and POSTs it to `https://fake-host/` over the binding's
//! service-designator channel. The designator name IS the event's
//! `dataset` field (workerd `bindings.rs`
//! `engine.set_dataset(name)`), so rendering emits one `external`
//! service per distinct dataset, all resolving — via a single
//! `--external-addr=<dataset>=unix:<sock>` entry per dataset — to this
//! tenant's AE unix socket, bound by the supervisor actor like the
//! Cache API socket. Tenant identity is the socket; nothing on the
//! wire authenticates it.
//!
//! Durability: accepted events are appended as raw JSON lines to a
//! per-`(tenant,dataset)` spool file, then converted in batches to
//! canonical Parquet files under
//! `<dir>/out/<seg(tenant)>/<dataset>/dt=<YYYY-MM-DD>/part-*.parquet`.
//! The spool survives process crashes; a rename-then-convert flush is
//! crash-atomic, and leftover `.flushing` files are recovered on the
//! next run. Parquet is the query contract — DuckDB, ClickHouse
//! `s3()`, Spark and pandas all read it natively; the layout is
//! identical under an S3 prefix, so an operator sync job or a later
//! object-store writer can lift the same files into a bucket.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, Request, Response, StatusCode};
use axum::routing::any;
use axum::Router;
use sha2::Digest;
use tokio::net::{TcpListener, UnixListener};

/// `unix:<path>` `--external-addr` string shared by assemble (emit)
/// and the supervisor (bind) — same single-source rule as the cache
/// socket derivation.
pub fn tenant_socket_path(ae_root: &Path, tenant_id: &str) -> PathBuf {
    vrt_securefs::socket_path(ae_root, tenant_id)
}

/// `unix:<path>` external-addr for this tenant's AE socket.
pub fn tenant_socket_addr(ae_root: &Path, tenant_id: &str) -> std::io::Result<String> {
    let p = tenant_socket_path(ae_root, tenant_id);
    let s = p.to_str().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "ae socket path utf8")
    })?;
    Ok(format!("unix:{s}"))
}

/// Hard limits — every bound required >0 at `AeService::new`.
#[derive(Clone, Copy, Debug)]
pub struct AeLimits {
    /// POST body cap — workerd's event is schema-bounded (~64 KiB
    /// worst case); anything larger is malformed, not a datapoint.
    pub max_body_bytes: usize,
    /// Per `(tenant,dataset)` spool file cap. Exceeding it rejects the
    /// write with 503 — workerd logs internally, the JS API stays
    /// fire-and-forget (CF's lossy contract); the drop is observable
    /// via `vrt_host_ae_errors_total{stage="overflow"}`.
    pub max_spool_bytes: u64,
    /// Spool→Parquet flush cadence.
    pub flush_interval: std::time::Duration,
}

impl AeLimits {
    fn validate(&self) -> std::io::Result<()> {
        if self.max_body_bytes == 0 || self.max_spool_bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ae limits must be > 0",
            ));
        }
        Ok(())
    }
}

/// Process-wide service: per-tenant unix listeners + shared spool
/// writer + flush loop. `sock_root` is the 0711 socket dir (same
/// traverse-only contract as the cache root); `data_root` holds
/// `spool/` + `out/`, host-only 0700.
pub struct AeService {
    sock_root: Option<PathBuf>,
    inner: Arc<AeInner>,
}

struct AeInner {
    data_root: PathBuf,
    limits: AeLimits,
    /// Serializes appends + caps per-tenant spool bytes. One lock for
    /// all datasets — appends are µs-scale `write(2)` calls and AE is
    /// a lossy telemetry path, not a hot data plane.
    write: Mutex<BTreeMap<String /*seg(tenant)*/, u64>>,
}

/// Same degrade-observable contract as `cache::record_cache_init` —
/// AE init failure downgrades the feature rather than failing the
/// host, so it MUST be observable: warn log + error counter +
/// `vrt_host_ae_degraded{reason}` gauge (1 = degraded, 0 = serving).
pub fn record_ae_init<T>(result: &std::io::Result<T>) {
    let reg = crate::metrics::registry();
    match result {
        Ok(_) => reg.set("vrt_host_ae_degraded", &[("reason", "none")], 0.0),
        Err(e) => {
            tracing::warn!(
                event = "ae_degraded",
                error = %e,
                remediation = "workDir/ae must be a host-owned 0711 directory; check PV permissions",
                "ae service init failed - degraded to unconfigured (analyticsEngine bindings fail closed)"
            );
            reg.inc(
                "vrt_host_ae_listener_errors_total",
                &[("tenant", "*"), ("stage", "init")],
                1.0,
            );
            reg.set("vrt_host_ae_degraded", &[("reason", "init")], 1.0);
        }
    }
}

impl AeService {
    /// `limits.validate()` failing is a startup error — a zero-cap
    /// spool would fake the feature while dropping everything.
    pub fn new(
        sock_root: Option<PathBuf>,
        data_root: PathBuf,
        limits: AeLimits,
    ) -> std::io::Result<Self> {
        limits.validate()?;
        if let Some(r) = &sock_root {
            crate::cache::ensure_traverse_dir(r, "ae socket root")?;
        }
        std::fs::create_dir_all(data_root.join("spool"))?;
        std::fs::create_dir_all(data_root.join("out"))?;
        Ok(Self {
            sock_root,
            inner: Arc::new(AeInner {
                data_root,
                limits,
                write: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// The ingest socket address this tenant's `external` services
    /// resolve to (`unix:` in production).
    pub fn tenant_addr(&self, tenant_id: &str) -> std::io::Result<String> {
        let root = self.sock_root.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "ae sock root unset")
        })?;
        tenant_socket_addr(root, tenant_id)
    }

    /// Bind this tenant's production unix listener — SYNC variant for
    /// the supervisor actor loop (`start_pending_launch` is not
    /// async). `owner` = the tenant's process uid/gid: the socket is
    /// chowned so ONLY that tenant's workerd can connect (cross-tenant
    /// writes die on EACCES).
    pub fn serve_tenant(
        &self,
        tenant_id: &str,
        owner: Option<(u32, u32)>,
    ) -> std::io::Result<AeListener> {
        let root = self.sock_root.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "ae sock root unset")
        })?;
        let path = tenant_socket_path(root, tenant_id);
        let listener = Self::bind_unix(&path)?;
        if let Some((uid, gid)) = owner {
            let cpath =
                std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "socket path nul")
                })?;
            // SAFETY: path is a valid NUL-terminated CString; uid/gid are plain integers and the result is checked.
            if unsafe { libc::chown(cpath.as_ptr(), uid, gid) } != 0 {
                let _ = std::fs::remove_file(&path);
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(self.spawn_serve(tenant_id, ListenerKind::Unix(listener), Some(path)))
    }

    /// Test-facing TCP bind (unix binds are sandbox-denied on some
    /// hosts — same escape hatch as the cache listener).
    pub async fn serve_tenant_tcp(
        &self,
        tenant_id: &str,
        addr: SocketAddr,
    ) -> std::io::Result<AeListener> {
        self.ensure_tenant_dir(tenant_id)?;
        let listener = TcpListener::bind(addr).await?;
        Ok(self.spawn_serve(tenant_id, ListenerKind::Tcp(listener), None))
    }

    /// Unix bind with stale-socket takeover; a non-socket file at the
    /// path fails closed (never unlink foreign files).
    fn bind_unix(path: &Path) -> std::io::Result<UnixListener> {
        match std::fs::metadata(path) {
            Ok(m) => {
                use std::os::unix::fs::FileTypeExt;
                if !m.file_type().is_socket() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AddrInUse,
                        "ae socket path holds a non-socket file",
                    ));
                }
                std::fs::remove_file(path)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // 0600 from birth — the sock root is 0711, so a
        // bind-then-chmod window here would be real.
        let std_listener = vrt_securefs::bind_unix_private(path, 0o600)?;
        std_listener.set_nonblocking(true)?;
        UnixListener::from_std(std_listener)
    }

    /// `<data_root>/spool/<seg(tenant)>/` — created lazily on first
    /// write AND at listener bind (a tenant with a bound socket but
    /// zero writes still has a predictable dir).
    fn ensure_tenant_dir(&self, tenant_id: &str) -> std::io::Result<()> {
        std::fs::create_dir_all(
            self.inner
                .data_root
                .join("spool")
                .join(vrt_securefs::segment_encode(tenant_id)),
        )
    }

    /// Shared accept-loop spawn — tenant identity is pinned to THIS
    /// listener scope; request fields can never select one.
    fn spawn_serve(
        &self,
        tenant_id: &str,
        listener: ListenerKind,
        guard_path: Option<PathBuf>,
    ) -> AeListener {
        let scope = Arc::new(TenantScope {
            inner: self.inner.clone(),
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
        AeListener {
            task: Some(task),
            socket_path: guard_path,
        }
    }

    /// Spawn the periodic spool→Parquet flush — exits on the shutdown
    /// watch like the other host background tasks. One final flush runs before
    /// exit so a restart loses at most the last buffered writes, not
    /// the whole spool window.
    pub fn spawn_flush(
        self: &Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let inner = self.inner.clone();
        let interval = self.inner.limits.flush_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        flush_all(&inner);
                    }
                    _ = shutdown.changed() => {
                        flush_all(&inner);
                        return;
                    }
                }
            }
        })
    }
}

enum ListenerKind {
    Unix(UnixListener),
    Tcp(TcpListener),
}

/// RAII handle for one bound socket — aborts the accept task and
/// unlinks the socket file on drop (actor drop / undeploy).
pub struct AeListener {
    task: Option<tokio::task::JoinHandle<()>>,
    socket_path: Option<PathBuf>,
}

impl Drop for AeListener {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        if let Some(p) = &self.socket_path {
            let _ = std::fs::remove_file(p);
        }
    }
}

struct TenantScope {
    inner: Arc<AeInner>,
    tenant: String,
}

// ============================ event decode ============================

/// One parsed `AnalyticsEngineEvent` — the Cap'n Proto JSON body
/// workerd POSTs. `dataset`/`index1`/`blob*` are `Data` fields → JSON
/// byte arrays; doubles are optional (unpopulated slots stay null
/// in storage, matching CF's columnar null semantics).
#[derive(Debug, Default)]
pub struct AeEvent {
    /// Dataset name (required) — already charset-bound upstream; the
    /// endpoint re-validates before it ever reaches a path.
    pub dataset: Vec<u8>,
    /// ns since epoch (required, must be >0).
    pub timestamp_ns: i64,
    /// Sampling/index dimension, ≤64 bytes upstream.
    pub index1: Option<Vec<u8>>,
    /// double1..20 positional.
    pub doubles: [Option<f64>; 20],
    /// blob1..20 positional.
    pub blobs: [Option<Vec<u8>>; 20],
}

impl AeEvent {
    /// Strict decode of the workerd body. Unknown fields are ignored
    /// (capnp JSON may carry `accountId`/`schemaVersion`); field TYPE
    /// mismatches reject — never coerce a malformed event.
    pub fn from_json(body: &[u8]) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_slice(body).ok()?;
        let obj = v.as_object()?;
        let mut ev = AeEvent {
            dataset: bytes_field(obj, "dataset")?,
            timestamp_ns: int64_field(obj, "timestamp")?,
            index1: opt_bytes_field(obj, "index1")?,
            ..Default::default()
        };
        if ev.timestamp_ns <= 0 || ev.dataset.is_empty() || ev.dataset.len() > 256 {
            return None;
        }
        for i in 0..20 {
            let dk = format!("double{}", i + 1);
            ev.doubles[i] = match obj.get(&dk) {
                None | Some(serde_json::Value::Null) => None,
                Some(x) => Some(x.as_f64()?),
            };
            let bk = format!("blob{}", i + 1);
            ev.blobs[i] = opt_bytes_field(obj, &bk)?;
        }
        Some(ev)
    }

    /// Lossy UTF-8 view for Parquet `Utf8` columns — workerd only ever
    /// produces valid UTF-8 blobs (JS strings), the lossy path exists
    /// for a hand-crafted socket peer.
    fn lossy(b: &[u8]) -> String {
        String::from_utf8_lossy(b).into_owned()
    }
}

/// capnp JSON encodes Int64 (`timestamp`, `accountId`,
/// `schemaVersion`) as a decimal STRING — the wire carries
/// `"timestamp":"1795237029144"`. Accept the number form too for
/// forward tolerance, reject everything else.
fn int64_field(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<i64> {
    match obj.get(key)? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

fn bytes_field(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<Vec<u8>> {
    opt_bytes_field(obj, key)?
}

/// `None` = malformed shape (reject the event); `Some(None)` = field
/// absent/null (column null); `Some(Some(v))` = decoded bytes.
fn opt_bytes_field(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<Option<Vec<u8>>> {
    match obj.get(key) {
        None | Some(serde_json::Value::Null) => Some(None),
        Some(serde_json::Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                out.push(u8::try_from(it.as_u64()?).ok()?);
            }
            Some(Some(out))
        }
        Some(_) => None,
    }
}

/// Dataset charset — identical to the desired validation
/// (`[A-Za-z0-9_]{1,256}`): the value lands in a filesystem path, so
/// the socket-side check is defense in depth for a peer that bypasses
/// workerd's compiled binding.
fn valid_dataset(ds: &[u8]) -> bool {
    !ds.is_empty() && ds.len() <= 256 && ds.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

// ============================ endpoint ============================

async fn handle(State(scope): State<Arc<TenantScope>>, req: Request<Body>) -> Response<Body> {
    if req.method() != Method::POST {
        return respond(StatusCode::METHOD_NOT_ALLOWED);
    }
    let limit = scope.inner.limits.max_body_bytes;
    let body = match http_body_util::BodyExt::collect(http_body_util::Limited::new(
        req.into_body(),
        limit,
    ))
    .await
    {
        Ok(b) => b.to_bytes(),
        Err(_) => {
            metric_err(&scope.tenant, "body");
            return respond(StatusCode::PAYLOAD_TOO_LARGE);
        }
    };
    let ev = match AeEvent::from_json(&body) {
        Some(ev) if valid_dataset(&ev.dataset) => ev,
        _ => {
            metric_err(&scope.tenant, "parse");
            return respond(StatusCode::BAD_REQUEST);
        }
    };
    let dataset = match std::str::from_utf8(&ev.dataset) {
        Ok(d) => d.to_string(),
        // Charset proof above makes this unreachable in practice —
        // charset is ASCII so lossy never actually alters bytes.
        Err(_) => AeEvent::lossy(&ev.dataset),
    };
    match append(&scope.inner, &scope.tenant, &dataset, &body) {
        Ok(()) => {
            crate::metrics::registry().inc(
                "vrt_host_ae_events_total",
                &[("tenant", &*scope.tenant), ("dataset", &*dataset)],
                1.0,
            );
            respond(StatusCode::OK)
        }
        Err(AppendError::Overflow) => {
            metric_err(&scope.tenant, "overflow");
            respond(StatusCode::SERVICE_UNAVAILABLE)
        }
        Err(AppendError::Io(e)) => {
            metric_err(&scope.tenant, "write");
            tracing::warn!(tenant = %scope.tenant, %e, "ae spool append failed");
            respond(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn respond(code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(code)
        .body(Body::empty())
        .unwrap()
}

fn metric_err(tenant: &str, stage: &'static str) {
    crate::metrics::registry().inc(
        "vrt_host_ae_errors_total",
        &[("tenant", tenant), ("stage", stage)],
        1.0,
    );
}

#[derive(Debug)]
enum AppendError {
    /// Per-file spool cap reached — 503 backpressure (explicit loss,
    /// observable; workerd keeps serving).
    Overflow,
    Io(std::io::Error),
}

/// Append the raw event line to `<spool>/<seg>/<dataset>.jsonl`.
/// The raw body is stored verbatim — the flush parser is the same
/// strict decoder, so a validated line always re-parses.
fn append(inner: &AeInner, tenant: &str, dataset: &str, body: &[u8]) -> Result<(), AppendError> {
    let seg = vrt_securefs::segment_encode(tenant);
    let dir = inner.data_root.join("spool").join(&seg);
    std::fs::create_dir_all(&dir).map_err(AppendError::Io)?;
    let path = dir.join(format!("{dataset}.jsonl"));

    let mut guard = inner.write.lock().unwrap_or_else(|p| p.into_inner());
    let tracked = guard.entry(seg.clone()).or_insert_with(|| {
        // Restart seed: count existing bytes once so the cap survives
        // a process bounce (in-memory counters would silently reset).
        spool_dir_bytes(&dir)
    });
    if *tracked + body.len() as u64 > inner.limits.max_spool_bytes {
        return Err(AppendError::Overflow);
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(AppendError::Io)?;
    // One `write_all` under the shared lock — lines never interleave.
    f.write_all(body.as_ref()).map_err(AppendError::Io)?;
    f.write_all(b"\n").map_err(AppendError::Io)?;
    *tracked += body.len() as u64 + 1;
    crate::metrics::registry().set(
        "vrt_host_ae_spool_bytes",
        &[("tenant", tenant)],
        *tracked as f64,
    );
    Ok(())
}

fn spool_dir_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.path().extension().is_some_and(|x| x == "jsonl") {
                total += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

// ============================ flush ============================

/// One flush pass over every pending spool file. Claim via atomic
/// rename (`x.jsonl` → `x.flushing`), convert, delete the claim.
/// `.flushing` files left by a crash are re-processed — the part-file
/// name is content-derived so a rewrite is idempotent.
fn flush_all(inner: &AeInner) {
    let spool_root = inner.data_root.join("spool");
    let Ok(segs) = std::fs::read_dir(&spool_root) else {
        return;
    };
    for seg in segs.flatten() {
        let seg_name = seg.file_name();
        let Some(seg_name) = seg_name.to_str() else {
            continue;
        };
        let tenant = match vrt_securefs::segment_decode(seg_name) {
            Some(t) => t,
            None => continue,
        };
        let dir = seg.path();
        let Ok(files) = std::fs::read_dir(&dir) else {
            continue;
        };
        for f in files.flatten() {
            let path = f.path();
            let name = f.file_name();
            let Some(name) = name.to_str() else { continue };
            let claim = if let Some(ds) = name.strip_suffix(".jsonl") {
                let c = dir.join(format!("{ds}.flushing"));
                if std::fs::rename(&path, &c).is_ok() {
                    Some((ds.to_string(), c))
                } else {
                    None
                }
            } else {
                name.strip_suffix(".flushing")
                    .map(|ds| (ds.to_string(), path.clone()))
            };
            let Some((dataset, claim_path)) = claim else {
                continue;
            };
            let claim_len = std::fs::metadata(&claim_path).map(|m| m.len()).unwrap_or(0);
            match flush_one(inner, &tenant, &dataset, &claim_path) {
                Ok(rows) => {
                    let _ = std::fs::remove_file(&claim_path);
                    let mut g = inner.write.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(t) = g.get_mut(seg_name) {
                        *t = t.saturating_sub(claim_len);
                    }
                    if rows > 0 {
                        crate::metrics::registry().inc(
                            "vrt_host_ae_flushed_events_total",
                            &[("tenant", &*tenant), ("dataset", &*dataset)],
                            rows as f64,
                        );
                    }
                }
                Err(e) => {
                    crate::metrics::registry().inc(
                        "vrt_host_ae_flush_errors_total",
                        &[("tenant", &*tenant), ("dataset", &*dataset)],
                        1.0,
                    );
                    tracing::warn!(%tenant, %dataset, %e, "ae flush failed; claim retained for retry");
                }
            }
        }
    }
}

/// Convert one claimed spool file into per-day Parquet part files.
/// Returns the number of converted events.
fn flush_one(inner: &AeInner, tenant: &str, dataset: &str, claim: &Path) -> std::io::Result<usize> {
    let raw = std::fs::read(claim)?;
    let mut by_day: BTreeMap<String, Vec<AeEvent>> = BTreeMap::new();
    let mut bad = 0usize;
    for line in raw.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        match AeEvent::from_json(line) {
            Some(ev) => {
                let day = chrono::DateTime::from_timestamp_nanos(ev.timestamp_ns)
                    .format("%Y-%m-%d")
                    .to_string();
                by_day.entry(day).or_default().push(ev);
            }
            None => bad += 1,
        }
    }
    if bad > 0 {
        metric_err(tenant, "spoolLine");
    }
    let mut written = 0usize;
    for (day, events) in by_day {
        written += events.len();
        let out_dir = inner
            .data_root
            .join("out")
            .join(vrt_securefs::segment_encode(tenant))
            .join(dataset)
            .join(format!("dt={day}"));
        std::fs::create_dir_all(&out_dir)?;
        // Content-derived name → a crashed/repeated flush of the SAME
        // bytes lands on the same part file (idempotent rewrite).
        let digest: [u8; 32] = sha2::Sha256::digest(&raw).into();
        let part = out_dir.join(format!("part-{}.parquet", hex::encode(&digest[..8])));
        write_parquet(&part, tenant, dataset, &events)?;
        crate::metrics::registry().inc(
            "vrt_host_ae_parquet_files_total",
            &[("tenant", tenant), ("dataset", dataset)],
            1.0,
        );
    }
    Ok(written)
}

// ============================ parquet ============================

fn ae_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let mut fields = Vec::with_capacity(23 + 20);
    fields.push(Field::new("tenant", DataType::Utf8, false));
    fields.push(Field::new("dataset", DataType::Utf8, false));
    fields.push(Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new("index1", DataType::Utf8, true));
    // CF's query schema carries `_sample_interval` (rows-represented).
    // Storage is full-fidelity, so it is constant 1 — emitting it keeps
    // CF-shaped queries (`SELECT _sample_interval ...`) runnable verbatim.
    fields.push(Field::new("_sample_interval", DataType::Int64, false));
    for i in 1..=20 {
        fields.push(Field::new(format!("double{i}"), DataType::Float64, true));
    }
    for i in 1..=20 {
        fields.push(Field::new(format!("blob{i}"), DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

/// Build the RecordBatch + write one part file via `.part` → rename —
/// a reader only ever sees complete Parquet files.
fn write_parquet(
    path: &Path,
    tenant: &str,
    dataset: &str,
    events: &[AeEvent],
) -> std::io::Result<()> {
    use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampNanosecondBuilder};
    let n = events.len();
    let mut tenant_b = StringBuilder::with_capacity(n, n * tenant.len());
    let mut ds_b = StringBuilder::with_capacity(n, n * dataset.len());
    let mut ts_b = TimestampNanosecondBuilder::with_capacity(n);
    let mut ix_b = StringBuilder::with_capacity(n, n * 16);
    for e in events {
        tenant_b.append_value(tenant);
        ds_b.append_value(dataset);
        ts_b.append_value(e.timestamp_ns);
        match &e.index1 {
            Some(b) => ix_b.append_value(AeEvent::lossy(b)),
            None => ix_b.append_null(),
        }
    }
    use arrow::array::Int64Builder;
    let mut si_b = Int64Builder::with_capacity(n);
    for _ in events {
        si_b.append_value(1);
    }
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(tenant_b.finish()),
        Arc::new(ds_b.finish()),
        Arc::new(ts_b.finish().with_timezone("UTC")),
        Arc::new(ix_b.finish()),
        Arc::new(si_b.finish()),
    ];
    for i in 0..20 {
        let mut b = Float64Builder::with_capacity(n);
        for e in events {
            match e.doubles[i] {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        cols.push(Arc::new(b.finish()));
    }
    for i in 0..20 {
        let mut b = StringBuilder::with_capacity(n, n * 8);
        for e in events {
            match &e.blobs[i] {
                Some(v) => b.append_value(AeEvent::lossy(v)),
                None => b.append_null(),
            }
        }
        cols.push(Arc::new(b.finish()));
    }
    let schema = ae_schema();
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let tmp = path.with_extension("part");
    {
        let file = std::fs::File::create(&tmp)?;
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::default(),
            ))
            .build();
        let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        w.write(&batch)
            .and_then(|_| w.close())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_json() -> serde_json::Value {
        // The real workerd wire shape: Int64 fields (`accountId`,
        // `schemaVersion`, `timestamp`) are decimal STRINGS in capnp
        // JSON; `Data` fields are byte arrays.
        serde_json::json!({
            "accountId": "1",
            "dataset": [119,101,97,116,104,101,114],
            "schemaVersion": "0",
            "index1": [97],
            "timestamp": "1700000000000000000",
            "double1": 25.5,
            "double3": -1,
            "blob1": [84,101,115,116],
            "blob20": [240,159,152,128],
        })
    }

    #[test]
    fn event_decode_wire_shape() {
        let body = serde_json::to_vec(&event_json()).unwrap();
        let ev = AeEvent::from_json(&body).unwrap();
        assert_eq!(ev.dataset, b"weather");
        assert_eq!(ev.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(ev.index1.as_deref(), Some(b"a".as_slice()));
        assert_eq!(ev.doubles[0], Some(25.5));
        assert_eq!(ev.doubles[2], Some(-1.0));
        assert_eq!(ev.doubles[1], None);
        assert_eq!(ev.blobs[0].as_deref(), Some(b"Test".as_slice()));
        assert_eq!(ev.blobs[19].as_deref(), Some("\u{1F600}".as_bytes()));
    }

    #[test]
    fn event_decode_rejects() {
        // Missing dataset / timestamp, wrong shapes.
        for v in [
            serde_json::json!({"timestamp": 1}),
            serde_json::json!({"dataset": [65], "timestamp": 0}),
            serde_json::json!({"dataset": [65], "timestamp": -5}),
            serde_json::json!({"dataset": "weather", "timestamp": 1}),
            serde_json::json!({"dataset": [65], "timestamp": "x"}),
            serde_json::json!({"dataset": [300], "timestamp": 1}),
        ] {
            assert!(
                AeEvent::from_json(&serde_json::to_vec(&v).unwrap()).is_none(),
                "{v}"
            );
        }
        // Unknown extra fields are fine.
        let mut v = event_json();
        v["futureField"] = serde_json::json!(true);
        assert!(AeEvent::from_json(&serde_json::to_vec(&v).unwrap()).is_some());
    }

    #[test]
    fn dataset_charset_bound() {
        assert!(valid_dataset(b"weather_2024"));
        assert!(!valid_dataset(b""));
        assert!(!valid_dataset(b"a/b"));
        assert!(!valid_dataset(b"a=b"));
        assert!(!valid_dataset(b"a.b"));
        assert!(!valid_dataset(&vec![b'x'; 257]));
    }

    #[test]
    fn parquet_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let events = vec![
            AeEvent {
                dataset: b"weather".to_vec(),
                timestamp_ns: 1_700_000_000_000_000_000,
                index1: Some(b"r1".to_vec()),
                doubles: {
                    let mut d = [None; 20];
                    d[0] = Some(1.5);
                    d
                },
                blobs: {
                    let mut b: [Option<Vec<u8>>; 20] = Default::default();
                    b[0] = Some(b"hello".to_vec());
                    b
                },
            },
            AeEvent {
                dataset: b"weather".to_vec(),
                timestamp_ns: 1_700_000_100_000_000_000,
                index1: None,
                doubles: [None; 20],
                blobs: Default::default(),
            },
        ];
        let part = dir.path().join("p.parquet");
        write_parquet(&part, "tenantA", "weather", &events).unwrap();
        let file = std::fs::File::open(&part).unwrap();
        let mut reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 45);
        use arrow::array::Array;
        let ds = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(ds.value(0), "weather");
        let si = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(si.value(0), 1);
        assert_eq!(si.value(1), 1);
        let d1 = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(d1.value(0), 1.5);
        assert!(d1.is_null(1));
        let b1 = batch
            .column(25)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(b1.value(0), "hello");
        assert!(b1.is_null(1));
    }

    #[test]
    fn append_and_flush_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let svc = AeService::new(
            None,
            dir.path().join("ae"),
            AeLimits {
                max_body_bytes: 1 << 20,
                max_spool_bytes: 1 << 20,
                flush_interval: std::time::Duration::from_secs(60),
            },
        )
        .unwrap();
        let mut ev = event_json();
        ev["timestamp"] = serde_json::json!(1_700_000_000_000_000_000i64);
        let body = serde_json::to_vec(&ev).unwrap();
        append(&svc.inner, "t1", "weather", &body).unwrap();
        append(&svc.inner, "t1", "weather", &body).unwrap();
        let spool = dir
            .path()
            .join("ae/spool")
            .join(vrt_securefs::segment_encode("t1"))
            .join("weather.jsonl");
        assert!(spool.exists());
        flush_all(&svc.inner);
        assert!(!spool.exists());
        let out_dir = dir
            .path()
            .join("ae/out")
            .join(vrt_securefs::segment_encode("t1"))
            .join("weather/dt=2023-11-14");
        let parts: Vec<_> = std::fs::read_dir(&out_dir)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
            .collect();
        assert_eq!(parts.len(), 1);
        // Two events in the single part file.
        let file = std::fs::File::open(parts[0].path()).unwrap();
        let mut reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn spool_cap_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let body = serde_json::to_vec(&event_json()).unwrap();
        let svc = AeService::new(
            None,
            dir.path().join("ae"),
            AeLimits {
                max_body_bytes: 1 << 20,
                // First line fits, second overflows the tenant cap.
                max_spool_bytes: body.len() as u64 + 8,
                flush_interval: std::time::Duration::from_secs(60),
            },
        )
        .unwrap();
        assert!(append(&svc.inner, "t1", "weather", &body).is_ok());
        assert!(matches!(
            append(&svc.inner, "t1", "weather", &body),
            Err(AppendError::Overflow)
        ));
    }
}
