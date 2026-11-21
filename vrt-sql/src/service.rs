//! vrt-sql service layer (T34): service-to-service auth, config, bounded concurrency and metrics.
//!
//! Auth model: only the provider may call vrt-sql. Both sides share a 0600 secret file;
//! the client does
//!   `"{ts}|{method}|{path}?{query}|{sha256hex(body)}|{epoch}|{seq}"`
//! HMAC-SHA256 and puts it in `x-vrt-sql-auth: v1:<ts>:<mac-hex>`.
//! Covering epoch/seq/query keeps a man-in-the-middle from rewriting resultsFormat and
//! dedup semantics; the ts window is +-30s (tolerating clock drift), out-of-window requests are rejected, constant-time comparison.
//! `x-vrt-epoch`/`x-vrt-seq` are never read before the signature verifies -- these two values are only
//! trusted after authentication (wire.rs `trusted_ctx_from_headers`).
//!
//! Replay: writes replayed with the same signature inside the window are caught by `_vrt_dedup`(epoch,seq); read
//! replays are acceptable (documented). Body length is checked before reading, and oversize is rejected outright -- guards against
//! unauthenticated large bodies eating memory; auth failures count as `outcome="unauthorized"`.

use axum::http::{HeaderMap, Method};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use vrt_objstore::ObjectStore;

/// Auth request header.
pub const AUTH_HEADER: &str = "x-vrt-sql-auth";
/// Auth clock tolerance (seconds) -- +-30s.
pub const AUTH_SKEW_SECS: i64 = 30;

type HmacSha256 = Hmac<Sha256>;

/// Service config (JSON/TOML, deny_unknown_fields).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceConfig {
    /// Database root directory (0700; sha256 shards are derived inside).
    pub db_root: PathBuf,
    /// unix socket: `{unixDir}/vrt-sql.sock`.
    #[serde(default)]
    pub unix_dir: Option<PathBuf>,
    /// TCP listen `ip:port` (internal network). Exactly one of unixDir and listenTcp must be set.
    #[serde(default)]
    pub listen_tcp: Option<String>,
    /// bookmark HMAC key file (0600, <=4KiB).
    pub bookmark_secret_file: PathBuf,
    /// Service-to-service auth secret file (0600, <=4KiB; the provider holds the same copy).
    pub auth_secret_file: PathBuf,
    /// Internal metrics TCP listen; not public.
    #[serde(default)]
    pub metrics_listen: Option<String>,
    /// Global in-flight concurrency cap (try_acquire, 429 when exceeded).
    #[serde(default = "default_global_concurrency")]
    pub global_concurrency: usize,
    /// Per-request transaction budget (ms).
    #[serde(default = "default_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
    /// Graceful-shutdown drain cap (ms).
    #[serde(default = "default_drain_ms")]
    pub drain_timeout_ms: u64,
    /// Request body cap (bytes) -- checked before reading the body.
    #[serde(default = "default_body_max")]
    pub body_max_bytes: usize,
    /// T36 section 4: object-store persistence. **Required** (fail closed: missing -> startup
    /// failure). `backend="none"` is allowed only when `allowDevBackend=true` (RV-R72).
    pub persistence: PersistenceConfig,
}

/// T36 `[persistence]` (section 4). Key names are camelCase, consistent with ServiceConfig.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PersistenceConfig {
    /// `fs` (standalone default) | `s3` | `none` (test/dev, WARN).
    pub backend: String,
    /// RV-R72: explicit dev switch. `backend="none"` (no replication, D1 writes only do local
    /// fsync) and non-loopback `http://` s3 endpoints are allowed only when this is true.
    /// Production overlays must not contain this key (checked by `vrt-k8s-check`).
    #[serde(default)]
    pub allow_dev_backend: bool,
    /// RV-R66: when the `backend="fs"` persistence root and the local `dbRoot` sit on the same
    /// device (same `st_dev`), the "persistence layer" lives and dies with the local db -- D1/Queue
    /// effectively have a single copy. Default false -> startup fails closed
    /// ([`check_fs_durability_distinct`]); only dev/internal-test should explicitly set it to
    /// true to acknowledge "no disaster recovery". Production overlays must not contain it (`vrt-k8s-check`).
    #[serde(default)]
    pub allow_same_device_durability: bool,
    /// Required for `backend="fs"`.
    #[serde(default)]
    pub fs: Option<FsPersistence>,
    /// Required for `backend="s3"`.
    #[serde(default)]
    pub s3: Option<S3Persistence>,
    /// Lease duration (ms).
    #[serde(default = "default_lease_ms")]
    pub lease_ms: u64,
    /// Clock-skew safety margin (ms): the lease is considered usable only while `expiry-margin > now`
    /// (the same read/write gate, section 4.3).
    #[serde(default = "default_margin_ms")]
    pub clock_skew_margin_ms: u64,
    /// Writer holder identifier; defaults to `hostname:pid:start_nanos`.
    #[serde(default)]
    pub holder: Option<String>,
    /// Run a checkpoint/snapshot every N persisted commits (0 = disabled).
    #[serde(default = "default_ckpt_commits")]
    pub checkpoint_every_commits: u64,
    /// Tombstone/segment retention window (ms); GC runs lazily.
    #[serde(default = "default_retention_ms")]
    pub retention_ms: u64,
    /// Snapshot byte cap (T36b): databases over it recover via the segment chain (cost grows linearly with chain length).
    /// Defaults to 256MiB -- covers typical D1 database sizes; the s3 backend uploads in memory,
    /// validation forces <=256MiB (fail closed); fs streams and is not memory-bound.
    #[serde(default = "default_snapshot_max_bytes")]
    pub snapshot_max_bytes: u64,
    /// MIG-9: total startup self-check retry budget (ms). A transiently not-ready peer (R2/DNS/
    /// slow network start) backs off exponentially within the budget; if it still fails when the budget is spent -> the process exits non-zero.
    /// Auth-class/non-retryable errors are not governed by the budget and fail closed immediately.
    /// Must be < the K8s startupProbe periodSeconds x failureThreshold.
    #[serde(default = "default_self_check_budget_ms")]
    pub self_check_budget_ms: u64,
    /// MIG-9: first backoff (ms).
    #[serde(default = "default_self_check_initial_backoff_ms")]
    pub self_check_initial_backoff_ms: u64,
    /// MIG-9: backoff cap (ms); grows x2 up to this ceiling.
    #[serde(default = "default_self_check_max_backoff_ms")]
    pub self_check_max_backoff_ms: u64,
    /// Time Travel retention window (ms): snapshots/segments inside the window are kept and the
    /// commit timeline resolves restore targets. 0 = disabled (bounded two-snapshot GC only).
    #[serde(default)]
    pub time_travel_retention_ms: u64,
}

impl PersistenceConfig {
    /// Fail-closed persistence validation, shared by the writer (`serve`) and the read-only
    /// follower (`follower`) so both reject the same misconfiguration.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self.backend.as_str() {
            "none" => {
                if !self.allow_dev_backend {
                    return Err(
                        "persistence.backend=none requires persistence.allowDevBackend=true (dev only)",
                    );
                }
            }
            "fs" => {
                if self.fs.is_none() {
                    return Err("persistence.fs.root required");
                }
            }
            "s3" => {
                let Some(s3) = &self.s3 else {
                    return Err("persistence.s3 required");
                };
                if s3.endpoint.is_empty() || s3.bucket.is_empty() || s3.region.is_empty() {
                    return Err("persistence.s3 endpoint/bucket/region");
                }
                // RV-R72: endpoint parse validation (https; http only for loopback/dev).
                if vrt_objstore::endpoint::validate_object_store_endpoint(
                    &s3.endpoint,
                    self.allow_dev_backend,
                )
                .is_err()
                {
                    return Err("persistence.s3.endpoint must be https (http only loopback, <svc>.<ns>.svc:port, or allowDevBackend)");
                }
                // T36b: s3 snapshot upload is an in-memory path -- over the cap fails closed.
                if self.snapshot_max_bytes > S3_SNAPSHOT_MAX_BYTES {
                    return Err("persistence.snapshotMaxBytes exceeds s3 in-memory cap (256MiB)");
                }
            }
            _ => return Err("persistence.backend must be fs|s3|none"),
        }
        if self.lease_ms <= self.clock_skew_margin_ms * 4 {
            // margin must be much smaller than lease (default 500ms vs 15s).
            return Err("persistence.leaseMs too small vs clockSkewMarginMs");
        }
        // MIG-9: self-check retry parameter bounds -- budget/backoff must be positive and self-consistent;
        // the budget is capped at 10 minutes (anything longer should rely on the pod restart policy, not
        // a single startup self-check).
        if self.self_check_initial_backoff_ms == 0 || self.self_check_max_backoff_ms == 0 {
            return Err("persistence.selfCheck* backoff must be > 0");
        }
        if self.self_check_max_backoff_ms < self.self_check_initial_backoff_ms {
            return Err("persistence.selfCheckMaxBackoffMs < selfCheckInitialBackoffMs");
        }
        if self.self_check_budget_ms == 0 || self.self_check_budget_ms > 600_000 {
            return Err("persistence.selfCheckBudgetMs out of range (0, 600000]");
        }
        if self.time_travel_retention_ms > 366 * 24 * 60 * 60 * 1000 {
            return Err("persistence.timeTravelRetentionMs out of range (<= 366 days)");
        }
        Ok(())
    }

    /// Build the persister config shared by `serve`. `holder` defaults to `host:pid:start_nanos`.
    pub fn persist_config(&self) -> crate::persist::PersistConfig {
        let holder = self.holder.clone().unwrap_or_else(|| {
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "vrt-sql".into());
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("{host}:{}:{nanos}", std::process::id())
        });
        crate::persist::PersistConfig {
            lease_ms: self.lease_ms,
            clock_skew_margin_ms: self.clock_skew_margin_ms,
            holder,
            checkpoint_every_commits: self.checkpoint_every_commits,
            retention_ms: self.retention_ms,
            snapshot_max_bytes: self.snapshot_max_bytes,
        }
    }
}

/// Open the object store described by `[persistence]`.
///
/// `backend="none"` -> `Ok(None)` (local-only test/dev; the caller decides whether to warn).
/// Credentials are loaded from 0600 files and never logged.
pub fn open_object_store(
    p: &PersistenceConfig,
) -> Result<Option<Arc<dyn ObjectStore>>, String> {
    Ok(Some(match p.backend.as_str() {
        "none" => return Ok(None),
        "fs" => {
            let fs = p.fs.as_ref().ok_or("persistence.fs missing")?;
            let root = fs.root.clone();
            match vrt_objstore::FsStore::new(root.clone()) {
                Ok(s) => {
                    tracing::info!(root = %root.display(), "persistence backend = fs");
                    Arc::new(s)
                }
                Err(e) => return Err(format!("fs store init: {e}")),
            }
        }
        "s3" => {
            let s3 = p.s3.as_ref().ok_or("persistence.s3 missing")?;
            let access = load_secret_0600(&s3.access_key_file, vrt_securefs::SecretPurpose::Credential)
                .map_err(|e| format!("s3 access key file: {e}"))?;
            let secret = load_secret_0600(&s3.secret_key_file, vrt_securefs::SecretPurpose::Credential)
                .map_err(|e| format!("s3 secret key file: {e}"))?;
            let cfg = vrt_objstore::S3StoreConfig {
                endpoint: s3.endpoint.clone(),
                region: s3.region.clone(),
                bucket: s3.bucket.clone(),
                access_key: String::from_utf8_lossy(&access).trim().to_string(),
                secret_key: vrt_objstore::sigv4::SecretString::new(
                    String::from_utf8_lossy(&secret).trim().to_string(),
                ),
                timeout: std::time::Duration::from_millis(s3.timeout_ms),
                conditional_writes: s3.conditional_writes,
                retries: 2,
                prefix: s3.prefix.clone(),
            };
            match vrt_objstore::S3Store::new(cfg) {
                Ok(s) => {
                    tracing::info!(endpoint = %s3.endpoint, bucket = %s3.bucket, "persistence backend = s3");
                    Arc::new(s)
                }
                Err(e) => return Err(format!("s3 store init: {e}")),
            }
        }
        other => return Err(format!("persistence.backend {other}")),
    }))
}

/// RV-R66: typed startup error for a same-device persistence layer (fail closed).
#[derive(Debug, PartialEq, Eq)]
pub enum DurabilityError {
    /// persistence root and dbRoot are on the same `st_dev`: no independent copy.
    SameDevice {
        db_root: PathBuf,
        persist_root: PathBuf,
    },
    /// Cannot determine the device of the two paths (stat failed) -- refuse, since independence cannot be proven.
    Unverifiable(String),
}

impl std::fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SameDevice {
                db_root,
                persist_root,
            } => write!(
                f,
                "VRT_SQL_DURABILITY_SAME_DEVICE: persistence fs root {} shares a device with dbRoot {} \
                 (single copy; use backend=\"s3\", or set persistence.allowSameDeviceDurability=true for dev only)",
                persist_root.display(),
                db_root.display()
            ),
            Self::Unverifiable(m) => write!(f, "VRT_SQL_DURABILITY_UNVERIFIABLE: {m}"),
        }
    }
}

impl std::error::Error for DurabilityError {}

/// Nearest existing ancestor (including itself) -- the root directory may not have been created yet.
fn nearest_existing(p: &std::path::Path) -> Option<PathBuf> {
    let mut cur = Some(p);
    while let Some(c) = cur {
        if c.exists() {
            return Some(c.to_path_buf());
        }
        cur = c.parent();
    }
    None
}

/// RV-R66: the `fs` persistence layer must be on a different device from the local db, unless an explicit dev exemption applies.
/// When the exemption applies, returns `Ok(true)` (the caller must warn "no disaster recovery"); for an independent device
/// `Ok(false)`. If the device cannot be determined -> `Err` (fail closed, not treated as independent).
#[cfg(unix)]
pub fn check_fs_durability_distinct(
    db_root: &std::path::Path,
    persist_root: &std::path::Path,
    allow_same_device: bool,
) -> Result<bool, DurabilityError> {
    use std::os::unix::fs::MetadataExt;
    let dev = |p: &std::path::Path| -> Result<u64, DurabilityError> {
        let anc = nearest_existing(p)
            .ok_or_else(|| DurabilityError::Unverifiable("no existing ancestor".into()))?;
        std::fs::metadata(&anc)
            .map(|m| m.dev())
            .map_err(|e| DurabilityError::Unverifiable(format!("stat: {e}")))
    };
    let (a, b) = (dev(db_root)?, dev(persist_root)?);
    if a != b {
        return Ok(false);
    }
    if allow_same_device {
        return Ok(true);
    }
    Err(DurabilityError::SameDevice {
        db_root: db_root.to_path_buf(),
        persist_root: persist_root.to_path_buf(),
    })
}

/// `[persistence.fs]`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FsPersistence {
    /// Object root directory (a separate volume is recommended).
    pub root: PathBuf,
}

/// `[persistence.s3]`. Credentials come from 0600 files (secrets are never inlined).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3Persistence {
    /// Base endpoint `https://host[:port]` (path-style).
    pub endpoint: String,
    /// Signing region.
    pub region: String,
    /// Physical bucket.
    pub bucket: String,
    /// access key file (0600).
    pub access_key_file: PathBuf,
    /// secret key file (0600).
    pub secret_key_file: PathBuf,
    /// Whether the backend supports S3 conditional writes (If-Match/If-None-Match). false -> CAS
    /// fails closed (section 12 unconfirmed item: R2/MinIO compatibility must be verified on real hardware).
    #[serde(default = "default_conditional_writes")]
    pub conditional_writes: bool,
    /// Request timeout (ms).
    #[serde(default = "default_s3_timeout_ms")]
    pub timeout_ms: u64,
    /// Object key prefix (isolation when multiple deployments share a bucket).
    #[serde(default)]
    pub prefix: String,
}

fn default_lease_ms() -> u64 {
    15_000
}
fn default_margin_ms() -> u64 {
    500
}
/// T36b snapshot cap default: 256MiB (hard ceiling for s3 in-memory upload; fs streaming may relax it).
fn default_snapshot_max_bytes() -> u64 {
    256 * 1024 * 1024
}
/// Hard cap on s3 backend snapshot bytes -- the upload path reads the object into memory to sign it.
const S3_SNAPSHOT_MAX_BYTES: u64 = 256 * 1024 * 1024;
fn default_ckpt_commits() -> u64 {
    64
}
fn default_retention_ms() -> u64 {
    86_400_000
}
fn default_conditional_writes() -> bool {
    true
}
fn default_s3_timeout_ms() -> u64 {
    10_000
}
fn default_self_check_budget_ms() -> u64 {
    60_000
}
fn default_self_check_initial_backoff_ms() -> u64 {
    500
}
fn default_self_check_max_backoff_ms() -> u64 {
    8_000
}

fn default_global_concurrency() -> usize {
    64
}
fn default_operation_timeout_ms() -> u64 {
    5_000
}
fn default_drain_ms() -> u64 {
    30_000
}
fn default_body_max() -> usize {
    1024 * 1024
}

impl ServiceConfig {
    /// fail-closed validation.
    pub fn validate(&self) -> Result<(), &'static str> {
        match (self.unix_dir.is_some(), self.listen_tcp.is_some()) {
            (true, false) | (false, true) => {}
            _ => return Err("unixDir|listenTcp: exactly one required"),
        }
        if self.global_concurrency == 0 || self.operation_timeout_ms == 0 {
            return Err("concurrency|timeout");
        }
        if self.body_max_bytes == 0 || self.body_max_bytes > 64 * 1024 * 1024 {
            return Err("bodyMaxBytes");
        }
        if let Some(addr) = &self.listen_tcp {
            if addr.parse::<std::net::SocketAddr>().is_err() {
                return Err("listenTcp");
            }
        }
        if let Some(addr) = &self.metrics_listen {
            if addr.parse::<std::net::SocketAddr>().is_err() {
                return Err("metricsListen");
            }
        }
        // T36 persistence validation (fail closed).
        self.persistence.validate()
    }
}

/// Load a 0600 secret file (RV-R72: delegates to `vrt_securefs::load_service_secret`,
/// shared by all three binaries with the same normalization: strip only one trailing newline, reject internal whitespace, minimum length,
/// reject placeholder values). Never prints the content.
pub fn load_secret_0600(
    path: &std::path::Path,
    purpose: vrt_securefs::SecretPurpose,
) -> io::Result<Vec<u8>> {
    Ok(vrt_securefs::load_service_secret(path, purpose)?.into_bytes())
}

/// Signed token, shared both ways: the provider calls [`ServiceAuth::sign`], vrt-sql
/// calls [`ServiceAuth::verify`].
#[derive(Clone)]
pub struct ServiceAuth {
    secret: Vec<u8>,
}

impl std::fmt::Debug for ServiceAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAuth").finish_non_exhaustive()
    }
}

fn mac(secret: &[u8], msg: &str) -> [u8; 32] {
    let mut h = <HmacSha256 as Mac>::new_from_slice(secret).expect("hmac any len");
    h.update(msg.as_bytes());
    h.finalize().into_bytes().into()
}

fn body_sha_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

impl ServiceAuth {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret }
    }

    /// The signed message (epoch/seq default to `-`).
    fn message(
        ts: i64,
        method: &str,
        path_query: &str,
        body: &[u8],
        epoch: &str,
        seq: &str,
    ) -> String {
        format!(
            "{ts}|{method}|{path_query}|{}|{epoch}|{seq}",
            body_sha_hex(body)
        )
    }

    /// Client signing: `v1:<ts>:<mac64>`. `epoch`/`seq` are the header values about to be sent
    /// (or None) and must match what is actually sent -- the signature covers them.
    pub fn sign(
        &self,
        method: &str,
        path_query: &str,
        body: &[u8],
        epoch: Option<u64>,
        seq: Option<u64>,
    ) -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let e = epoch.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        let s = seq.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        let m = mac(
            &self.secret,
            &Self::message(ts, method, path_query, body, &e, &s),
        );
        format!("v1:{ts}:{}", hex::encode(m))
    }

    /// Server-side verification: any mismatch in format/clock/HMAC -> false (fail closed).
    /// `epoch`/`seq` take the raw header values (missing -> "-"); the caller parses them after the signature.
    pub fn verify(
        &self,
        headers: &HeaderMap,
        method: &Method,
        path_query: &str,
        body: &[u8],
    ) -> bool {
        let Some(v) = headers.get(AUTH_HEADER).and_then(|v| v.to_str().ok()) else {
            return false;
        };
        let mut it = v.splitn(3, ':');
        let (Some("v1"), Some(ts_s), Some(mac_hex)) = (it.next(), it.next(), it.next()) else {
            return false;
        };
        let Ok(ts) = ts_s.parse::<i64>() else {
            return false;
        };
        let Ok(expected) = hex::decode(mac_hex) else {
            return false;
        };
        if expected.len() != 32 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        if (now - ts).abs() > AUTH_SKEW_SECS {
            return false;
        }
        let epoch = headers
            .get(crate::wire::EPOCH_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        let seq = headers
            .get(crate::wire::SEQ_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        let actual = mac(
            &self.secret,
            &Self::message(ts, method.as_str(), path_query, body, epoch, seq),
        );
        let mut diff = 0u8;
        for (a, b) in actual.iter().zip(expected.iter()) {
            diff |= a ^ b;
        }
        diff == 0
    }
}

/// Bounded databaseId -> metrics label: <=256 active ids, the rest are folded into
/// `_overflow` (workers-runtime-metrics.md cardinality rule; invalid values become `_unknown`).
#[derive(Default)]
pub struct DbLabels {
    seen: Mutex<std::collections::HashSet<String>>,
}

impl DbLabels {
    const CAP: usize = 256;

    pub fn label(&self, database_id: &str) -> String {
        let valid = database_id.len() == 26
            && database_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        if !valid {
            return "_unknown".to_string();
        }
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        if seen.contains(database_id) || seen.len() < Self::CAP {
            seen.insert(database_id.to_string());
            return database_id.to_string();
        }
        "_overflow".to_string()
    }
}
