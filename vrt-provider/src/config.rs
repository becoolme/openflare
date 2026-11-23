//! Typed provider configuration for the `vrt-provider` binary.
//!
//! Credentials are referenced by environment-variable name — the config
//! file never carries secret material, and secrets are never logged.

use serde::Deserialize;
use snapshot_protocol::envelope::TrustedKey;
use std::path::PathBuf;
use std::time::Duration;

/// Top-level provider configuration (TOML/JSON, `deny_unknown_fields`).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderConfig {
    /// Cell id the `compute.desired` snapshot must be scoped to.
    pub cell_id: String,
    /// Directory for per-tenant unix sockets (`0700`, owned by us).
    pub listener_dir: PathBuf,
    /// Desired-snapshot source.
    pub desired: DesiredConfig,
    /// Kvrocks backend; absent → KV ops return `RESOURCE_UNAVAILABLE`.
    #[serde(default)]
    pub kvrocks: Option<KvrocksConfig>,
    /// S3-compatible backend; absent → R2 ops return `RESOURCE_UNAVAILABLE`.
    #[serde(default)]
    pub s3: Option<S3Config>,
    /// Local-filesystem KV backend (standalone mode, T17). Mutually
    /// exclusive with `kvrocks`.
    #[serde(default)]
    pub kv: Option<FsKvConfig>,
    /// Local-filesystem R2 backend (standalone mode, T17). Mutually
    /// exclusive with `s3`.
    #[serde(default)]
    pub r2: Option<FsR2Config>,
    /// T21: content-addressed assets store shared with the host
    /// materializer / `of deploy`. Absent → asset fetches fail
    /// closed (`ASSET_STORAGE_UNAVAILABLE`), bindings still authorize.
    #[serde(default)]
    pub assets: Option<FsAssetsConfig>,
    /// Vectorize index store (sqlite-vec, one DB file per index).
    /// Absent → vectorize ops fail closed (`VECTORIZE_UNAVAILABLE`).
    #[serde(default)]
    pub vectorize: Option<FsVectorizeConfig>,
    /// T34: vrt-sql data-plane backend; absent → D1 ops return
    /// `{success:false}` unavailable surface.
    #[serde(default)]
    pub vrt_sql: Option<VrtSqlConfig>,
    /// P1: PG HTTP data plane; absent → PG ops fail closed
    /// `PG_CREDENTIAL_UNAVAILABLE`.
    #[serde(default)]
    pub pg: Option<PgConfig>,
    /// MIG-15: pg-wire proxy for synthetic Hyperdrive targets; absent
    /// disables the listener (loopback-only anyway).
    #[serde(default)]
    pub pg_proxy: Option<PgProxyConfig>,
    /// MIG-15 r3: Hyperdrive session pooling (`[hyperdrive.pool]`).
    /// Absent → no pooling: every session owns a fresh upstream
    /// connection for its lifetime (the pre-r3 shape).
    #[serde(default)]
    pub hyperdrive: Option<HyperdriveConfig>,
    /// Env var holding the list-cursor HMAC secret (legacy/platform
    /// delivery). Ignored when `cursor_secret_file` is set.
    #[serde(default = "default_cursor_secret_env")]
    pub cursor_secret_env: String,
    /// T25/L8: file holding the list-cursor HMAC secret — `of init`
    /// writes `keys/cursor.secret` (0600). When set, this file is the
    /// ONLY accepted secret source: mode must be 0600, content
    /// non-empty (≤4 KiB). Missing/insecure → fail closed.
    #[serde(default)]
    pub cursor_secret_file: Option<PathBuf>,
    /// RV-R72: explicit dev switch — permits plain `http://` to a
    /// non-loopback S3 endpoint / non-internal vrt-sql address. Must be
    /// absent (false) in every production overlay.
    #[serde(default)]
    pub allow_dev_backend: bool,
    /// Per-request operation timeout.
    #[serde(default = "default_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
    /// T13/H3: per-tenant in-flight request cap (`try_acquire` — typed
    /// 429, never queued unboundedly).
    #[serde(default = "default_tenant_concurrency")]
    pub tenant_concurrency: usize,
    /// T13/H3: global in-flight cap across all tenants.
    #[serde(default = "default_global_concurrency")]
    pub global_concurrency: usize,
    /// RV-R67: per-tenant sustained requests/second (burst 2 s).
    #[serde(default = "default_tenant_qps")]
    pub tenant_qps: u32,
    /// RV-R67: per-tenant sustained declared request bytes/second
    /// (burst 2 s).
    #[serde(default = "default_tenant_bytes_per_sec")]
    pub tenant_bytes_per_sec: u64,
    /// T13/H3: request-body drain deadline.
    #[serde(default = "default_body_read_timeout_ms")]
    pub body_read_timeout_ms: u64,
    /// Graceful-shutdown drain cap.
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,
    /// B12: internal metrics bind (`ip:port`, e.g. `127.0.0.1:9493`).
    /// Absent → no metrics listener. Never publish this port on the
    /// public facade.
    #[serde(default)]
    pub metrics_listen: Option<String>,
    /// T13/M2: `[isolation]` — per-tenant uid/gid range matching
    /// vrt-host's `[isolation]`. When set, each tenant socket is
    /// bound world-connectable (`0666`) inside a traverse-only
    /// (`0711`) `listenerDir`, and every accepted connection is
    /// gated by SO_PEERCRED: the peer uid must equal
    /// `vrt_securefs::tenant_uid(base, span, tenant)` — the SAME
    /// derivation the host uses to setuid the tenant's workerd.
    /// base/span MUST equal the host's values — a mismatch rejects
    /// every tenant connection (fail closed, loud). Absent → the
    /// shared-uid model: `0700` dir + `0600` sockets, peer uid must
    /// equal the provider's own euid.
    #[serde(default)]
    pub isolation: Option<IsolationConfig>,

    /// `send_email` bindings: the upstream adapter every send relays
    /// through. Absent → the email route 503s (`BACKEND_UNAVAILABLE`),
    /// never a silent drop.
    #[serde(default)]
    pub email: Option<EmailConfig>,
}

/// `[email]` — send_email upstream adapter. Secrets are always env-var
/// NAMES (`apiKeyEnv` etc.), never inline values.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EmailConfig {
    /// `log` | `resend` | `cf` | `smtp`.
    pub backend: String,
    /// HTTPS endpoint (resend default `https://api.resend.com`; cf
    /// requires the full account send URL).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Env var holding the API key/token (resend/cf).
    #[serde(default = "default_email_api_key_env")]
    pub api_key_env: String,
    /// SMTP relay host (required for `smtp`).
    #[serde(default)]
    pub host: Option<String>,
    /// SMTP port (default 465 implicit / 587 otherwise).
    #[serde(default)]
    pub port: Option<u16>,
    /// `plain` | `starttls` | `implicit`.
    #[serde(default = "default_email_tls")]
    pub tls: String,
    /// Env vars holding SMTP credentials (both or neither).
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    /// EHLO name.
    #[serde(default)]
    pub helo: Option<String>,
    /// Adapter round-trip bound.
    #[serde(default = "default_email_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_email_api_key_env() -> String {
    "VRT_EMAIL_API_KEY".to_string()
}
fn default_email_tls() -> String {
    "starttls".to_string()
}
fn default_email_timeout_ms() -> u64 {
    15_000
}

/// T13/M2: `[isolation]` — uid/gid assignment range (mirrors
/// `vrt-host::config::IsolationConfig`; both derive tenant uids via
/// `vrt_securefs::tenant_uid`).
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolationConfig {
    /// First allocatable tenant uid/gid.
    pub base: u32,
    /// Range size — uids are `[base, base + span)`.
    pub span: u32,
}

fn default_cursor_secret_env() -> String {
    "VRT_CURSOR_SECRET".to_string()
}
fn default_tenant_concurrency() -> usize {
    64
}
fn default_tenant_qps() -> u32 {
    crate::quota::DEFAULT_TENANT_QPS
}
fn default_tenant_bytes_per_sec() -> u64 {
    crate::quota::DEFAULT_TENANT_BYTES_PER_SEC
}
fn default_global_concurrency() -> usize {
    512
}
fn default_body_read_timeout_ms() -> u64 {
    30_000
}
fn default_operation_timeout_ms() -> u64 {
    30_000
}
fn default_drain_timeout_ms() -> u64 {
    30_000
}

/// P1: PG backend configuration — the ONLY secret-adjacent input is a
/// path; tokens themselves live in the 0600 credentials file written
/// by cell-controller from the render-inputs channel.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PgConfig {
    /// `pg-credentials.json` (0600) — `{tenantId: {credentialRef:
    /// {token, endpoint, tenantSlug, credentialVersion, readOnly}}}`.
    pub credentials_file: PathBuf,
}

/// MIG-15: pg-wire proxy (Hyperdrive) listener configuration.
/// Loopback-only; the egress `internal-network` connect() lands here.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PgProxyConfig {
    /// Listen address — must be loopback; anything else fails closed.
    pub listen: String,
    /// Preamble/startup and upstream connect timeout.
    #[serde(default = "default_pg_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Per-read idle timeout on the bidirectional relay.
    #[serde(default = "default_pg_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Hard session lifetime cap.
    #[serde(default = "default_pg_max_session_ms")]
    pub max_session_ms: u64,
    /// Global concurrent session cap.
    #[serde(default = "default_pg_max_sessions")]
    pub max_sessions: usize,
    /// Per-binding concurrent session cap.
    #[serde(default = "default_pg_max_sessions_per_binding")]
    pub max_sessions_per_binding: usize,
}

fn default_pg_connect_timeout_ms() -> u64 {
    5_000
}
fn default_pg_idle_timeout_ms() -> u64 {
    90_000
}
fn default_pg_max_session_ms() -> u64 {
    3_600_000
}
fn default_pg_max_sessions() -> usize {
    256
}
fn default_pg_max_sessions_per_binding() -> usize {
    8
}

/// MIG-15 r3: `[hyperdrive]` — the pooling half of the Hyperdrive
/// data plane. The listener itself stays `[pgProxy]`; this section only
/// exists when pooling is enabled, so `[hyperdrive]` without a
/// `[hyperdrive.pool]` table is a configuration error rather than a
/// silent no-op.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HyperdriveConfig {
    /// Per-process session pool configuration.
    pub pool: PoolConfig,
}

/// MIG-15 r3: `[hyperdrive.pool]` — session-pool bounds. Session
/// pooling only: a client session keeps one backend connection for its
/// whole life; there is no transaction multiplexing.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PoolConfig {
    /// Live backend connections this process may hold (idle + in use).
    #[serde(default = "default_pool_max_connections")]
    pub max_connections: usize,
    /// How long an acquisition may queue before it is denied.
    #[serde(default = "default_pool_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,
    /// Idle socket reclamation horizon.
    #[serde(default = "default_pool_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    /// Hard age of a pooled socket, enforced on reuse and by the reaper.
    #[serde(default = "default_pool_max_lifetime_ms")]
    pub max_lifetime_ms: u64,
    /// Run `DISCARD ALL` before an idle socket is reused. Turning this
    /// off reuses session state (GUCs, temp tables, prepared statements)
    /// across sessions of the same tenant/database — intended for
    /// measurement only, never for production.
    #[serde(default = "default_true")]
    pub reset_on_return: bool,
}

/// Ceiling for `maxConnections` — one socket each, per process.
const POOL_MAX_CONNECTIONS_CAP: usize = 1024;

fn default_pool_max_connections() -> usize {
    8
}
fn default_pool_acquire_timeout_ms() -> u64 {
    5_000
}
fn default_pool_idle_timeout_ms() -> u64 {
    30_000
}
fn default_pool_max_lifetime_ms() -> u64 {
    1_800_000
}
fn default_true() -> bool {
    true
}

impl PoolConfig {
    /// Fail-closed bounds: every value is explicit and bounded, so a
    /// typo cannot disable the cap or turn the queue into an unbounded
    /// wait.
    pub fn validate(&self) -> Result<(), ConfigError> {
        use vrt_securefs::{ensure_range, ensure_timeout_ms};
        let err = |m: &'static str| Err(ConfigError::Invalid(m));
        ensure_range!(
            self.max_connections,
            "hyperdrive.pool.maxConnections",
            1,
            POOL_MAX_CONNECTIONS_CAP,
            err
        );
        ensure_timeout_ms!(
            self.acquire_timeout_ms,
            "hyperdrive.pool.acquireTimeoutMs",
            err
        );
        ensure_timeout_ms!(self.idle_timeout_ms, "hyperdrive.pool.idleTimeoutMs", err);
        ensure_range!(
            self.max_lifetime_ms,
            "hyperdrive.pool.maxLifetimeMs",
            1_000,
            2_592_000_000,
            err
        );
        Ok(())
    }
}

impl PgProxyConfig {
    /// Loopback listen is a hard invariant — the preamble is an
    /// authenticated channel only because it can never face tenants
    /// or other pods directly.
    pub fn listen_is_loopback(&self) -> bool {
        self.listen
            .parse::<std::net::SocketAddr>()
            .map(|a| a.ip().is_loopback())
            .unwrap_or(false)
    }
}

/// Desired-snapshot source configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DesiredConfig {
    /// Signed `compute.desired` envelope file.
    pub path: PathBuf,
    /// Trusted verifier keys (must be non-empty).
    pub trusted_keys: Vec<TrustedKey>,
    /// Poll interval for snapshot reload.
    #[serde(default = "default_reload_ms")]
    pub reload_ms: u64,
    /// How many previous active versions stay authorized (drain grace).
    #[serde(default = "default_grace_versions")]
    pub grace_versions: usize,
}

fn default_reload_ms() -> u64 {
    1_000
}
fn default_grace_versions() -> usize {
    1
}

/// Kvrocks backend configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KvrocksConfig {
    /// Redis URL (`redis://host:port/db`); prefer `url_env` for anything
    /// containing credentials.
    pub url: Option<String>,
    /// Env var holding the Redis URL.
    pub url_env: Option<String>,
    /// Per-command timeout.
    #[serde(default = "default_kv_timeout_ms")]
    pub timeout_ms: u64,
    /// Connect timeout.
    #[serde(default = "default_kv_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Disable the single bounded read retry.
    #[serde(default)]
    pub disable_read_retry: bool,
}

fn default_kv_timeout_ms() -> u64 {
    5_000
}
fn default_kv_connect_timeout_ms() -> u64 {
    5_000
}

/// Local-filesystem KV backend configuration (standalone, T17).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FsKvConfig {
    /// Data root (`0700`, created if absent). Layout:
    /// `<dir>/<tenant>/<namespace>/<sha256(key)>`.
    pub dir: PathBuf,
}

/// Local-filesystem Vectorize index store. One sqlite DB per index:
/// `<dir>/<tenant>/<indexId>.vec.db` (`0700`, created if absent).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FsVectorizeConfig {
    /// Data root (`0700`, created if absent).
    pub dir: PathBuf,
}

/// Local-filesystem R2 backend configuration (standalone, T17).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FsR2Config {
    /// Data root (`0700`, created if absent). Layout:
    /// `<dir>/<tenant>/<bucket>/<sha256(key)>`.
    pub dir: PathBuf,
    /// Single-part object byte cap (same default/cap as S3).
    #[serde(default = "default_s3_max_object_bytes")]
    pub max_object_bytes: u64,
    /// Hard ceiling for `max_object_bytes`.
    #[serde(default = "default_s3_max_object_bytes_cap")]
    pub max_object_bytes_cap: u64,
    /// Unfinished multipart upload TTL before the staging GC reclaims
    /// it (default 7 days, matching upstream R2).
    #[serde(default = "default_mp_staging_ttl_ms")]
    pub staging_ttl_ms: i64,
    /// Background staging-GC sweep interval (default 10 min).
    #[serde(default = "default_mp_gc_interval_ms")]
    pub staging_gc_interval_ms: u64,
    /// Per-tenant (cross-bucket) staged-bytes ceiling enforced at
    /// `uploadPart`; `None` → `4 × maxObjectBytes` (one tenant can hold
    /// a few max-size uploads in flight, never a disk-filling backlog).
    #[serde(default)]
    pub staging_max_bytes_per_tenant: Option<u64>,
    /// Per-tenant (cross-bucket) active-upload ceiling enforced at
    /// `createMultipartUpload` (default 64).
    #[serde(default = "default_mp_max_active_uploads")]
    pub max_active_uploads: usize,
}

fn default_mp_staging_ttl_ms() -> i64 {
    crate::r2::wire::R2_MULTIPART_DEFAULT_TTL_MS
}

fn default_mp_gc_interval_ms() -> u64 {
    600_000
}

fn default_mp_max_active_uploads() -> usize {
    crate::r2::wire::R2_MULTIPART_MAX_ACTIVE_UPLOADS
}

/// Local-filesystem assets store (T21) — layout:
/// `<dir>/<tenant>/manifests/<digest>.json` +
/// `<dir>/<tenant>/objects/<sha256>`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FsAssetsConfig {
    /// Data root (`0700`, created if absent).
    pub dir: PathBuf,
}

/// vrt-sql data-plane configuration (T34).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VrtSqlConfig {
    /// Base URL (`http://host:port`, no trailing slash). Internal network.
    pub addr: String,
    /// Inter-service auth secret file (0600, the same one as vrt-sql).
    pub auth_secret_file: PathBuf,
    /// Per-request timeout (bounded; no retry).
    #[serde(default = "default_vrt_sql_timeout_ms")]
    pub timeout_ms: u64,
    /// Optional read-only follower addresses (`vrt-sql follower`) for D1 `query` reads.
    ///
    /// Only `/query` is eligible, and never when the session is `first-primary`. A replica that
    /// cannot satisfy the bookmark (or that would write) answers with a typed 409 and the request
    /// is retried once on `addr`. `execute` and `dump` always use `addr`.
    #[serde(default)]
    pub replicas: Vec<String>,
}

fn default_vrt_sql_timeout_ms() -> u64 {
    30_000
}

/// S3-compatible backend configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3Config {
    /// Base endpoint (`https://host[:port]`, no path).
    pub endpoint: String,
    /// Signing region.
    pub region: String,
    /// Physical bucket name.
    pub bucket: String,
    /// Env var holding the access key id.
    pub access_key_env: String,
    /// Env var holding the secret access key.
    pub secret_key_env: String,
    /// Per-request timeout.
    #[serde(default = "default_s3_timeout_ms")]
    pub timeout_ms: u64,
    /// Single-part object byte cap.
    #[serde(default = "default_s3_max_object_bytes")]
    pub max_object_bytes: u64,
    /// Hard ceiling for `max_object_bytes` (T13/H3) — config above this
    /// is rejected, bounding memory per put/get.
    #[serde(default = "default_s3_max_object_bytes_cap")]
    pub max_object_bytes_cap: u64,
    /// Whether the backend honors S3 conditional-write headers
    /// (`If-Match` / `If-None-Match`). T13/M3: `false` + `onlyIf` ⇒
    /// `CAPABILITY_UNSUPPORTED` — never degrade to non-atomic writes.
    #[serde(default = "default_conditional_writes")]
    pub conditional_writes: bool,
    /// Unfinished multipart upload horizon — signed into the tenant
    /// uploadId expiry. S3 reaps uploads only via bucket
    /// `AbortIncompleteMultipartUpload` lifecycle rules; this value
    /// MUST mirror the configured rule (default 7 days).
    #[serde(default = "default_mp_staging_ttl_ms")]
    pub multipart_ttl_ms: i64,
}

fn default_s3_timeout_ms() -> u64 {
    30_000
}
fn default_s3_max_object_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_s3_max_object_bytes_cap() -> u64 {
    256 * 1024 * 1024
}
fn default_conditional_writes() -> bool {
    true
}

impl ProviderConfig {
    /// Validate fail-closed invariants.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.cell_id.is_empty() || self.cell_id.len() > 128 {
            return Err(ConfigError::Invalid("cellId"));
        }
        if self.desired.trusted_keys.is_empty() {
            return Err(ConfigError::Invalid("desired.trustedKeys"));
        }
        if let Some(s3) = &self.s3 {
            // T13/H3: object cap must be positive and below the hard
            // ceiling — bounds per-request memory.
            if s3.max_object_bytes == 0 || s3.max_object_bytes > s3.max_object_bytes_cap {
                return Err(ConfigError::Invalid("s3.maxObjectBytes"));
            }
        }
        if self
            .kvrocks
            .as_ref()
            .is_some_and(|k| k.url.is_none() && k.url_env.is_none())
        {
            return Err(ConfigError::Invalid("kvrocks.url|urlEnv"));
        }
        if self.kvrocks.is_some() && self.kv.is_some() {
            return Err(ConfigError::Invalid("kv|kvrocks exclusive"));
        }
        if self.s3.is_some() && self.r2.is_some() {
            return Err(ConfigError::Invalid("r2|s3 exclusive"));
        }
        if let Some(r2) = &self.r2 {
            if r2.max_object_bytes == 0 || r2.max_object_bytes > r2.max_object_bytes_cap {
                return Err(ConfigError::Invalid("r2.maxObjectBytes"));
            }
        }
        self.validate_startup_bounds()?;
        if let Some(hd) = &self.hyperdrive {
            // Pooling configures the pg-wire listener; without one there
            // is nothing to pool and the section would silently do
            // nothing — refuse at startup instead.
            if self.pg_proxy.is_none() {
                return Err(ConfigError::Invalid("hyperdrive requires pgProxy"));
            }
            hd.pool.validate()?;
        }
        if self.pg_proxy.is_some() && self.pg.is_none() {
            // The proxy resolves upstream credentials from `[pg]`.
            return Err(ConfigError::Invalid("pgProxy requires pg"));
        }
        // T13/M2: same bounds as the host — uid 0 is never a tenant
        // and a zero/overflowing span could never host one.
        if let Some(iso) = &self.isolation {
            if iso.base == 0 || iso.span == 0 || iso.base.checked_add(iso.span).is_none() {
                return Err(ConfigError::Invalid("isolation.base/span"));
            }
        }
        Ok(())
    }

    /// RV-R72: typed ranges + endpoint protocol rules. Every violation
    /// names the field and the allowed range (static message, no values).
    fn validate_startup_bounds(&self) -> Result<(), ConfigError> {
        use vrt_securefs::{ensure_range, ensure_timeout_ms};
        let err = |m: &'static str| Err(ConfigError::Invalid(m));
        ensure_timeout_ms!(self.operation_timeout_ms, "operationTimeoutMs", err);
        ensure_timeout_ms!(self.body_read_timeout_ms, "bodyReadTimeoutMs", err);
        ensure_timeout_ms!(self.drain_timeout_ms, "drainTimeoutMs", err);
        ensure_range!(
            self.tenant_concurrency,
            "tenantConcurrency",
            1,
            100_000,
            err
        );
        ensure_range!(
            self.global_concurrency,
            "globalConcurrency",
            1,
            100_000,
            err
        );
        ensure_range!(self.tenant_qps, "tenantQps", 1, 10_000_000, err);
        ensure_range!(
            self.tenant_bytes_per_sec,
            "tenantBytesPerSec",
            1,
            1_099_511_627_776,
            err
        );
        if self.tenant_concurrency > self.global_concurrency {
            return Err(ConfigError::Invalid(
                "tenantConcurrency must be <= globalConcurrency",
            ));
        }
        if let Some(s3) = &self.s3 {
            ensure_timeout_ms!(s3.timeout_ms, "s3.timeoutMs", err);
            ensure_range!(
                s3.multipart_ttl_ms,
                "s3.multipartTtlMs",
                1,
                vrt_securefs::limits::TTL_MS_MAX,
                err
            );
            if vrt_objstore::endpoint::validate_object_store_endpoint(
                &s3.endpoint,
                self.allow_dev_backend,
            )
            .is_err()
            {
                return Err(ConfigError::Invalid(
                    "s3.endpoint must be https (http only loopback, <svc>.<ns>.svc:port, or allowDevBackend)",
                ));
            }
        }
        if let Some(k) = &self.kvrocks {
            ensure_timeout_ms!(k.timeout_ms, "kvrocks.timeoutMs", err);
            ensure_timeout_ms!(k.connect_timeout_ms, "kvrocks.connectTimeoutMs", err);
        }
        if let Some(r2) = &self.r2 {
            ensure_range!(
                r2.staging_ttl_ms,
                "r2.stagingTtlMs",
                1,
                vrt_securefs::limits::TTL_MS_MAX,
                err
            );
        }
        if let Some(sql) = &self.vrt_sql {
            ensure_timeout_ms!(sql.timeout_ms, "vrtSql.timeoutMs", err);
            if vrt_objstore::endpoint::validate_internal_service_addr(
                &sql.addr,
                self.allow_dev_backend,
            )
            .is_err()
            {
                return Err(ConfigError::Invalid(
                    "vrtSql.addr must be https, or http to loopback/cluster-internal host",
                ));
            }
            if sql.replicas.len() > 8 {
                return Err(ConfigError::Invalid(
                    "vrtSql.replicas supports at most 8 entries",
                ));
            }
            for replica in &sql.replicas {
                if replica.is_empty()
                    || replica.len() > 256
                    || replica.ends_with('/')
                    || vrt_objstore::endpoint::validate_internal_service_addr(
                        replica,
                        self.allow_dev_backend,
                    )
                    .is_err()
                {
                    return Err(ConfigError::Invalid(
                        "vrtSql.replicas entries must be https, or http to loopback/cluster-internal host (no trailing slash)",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Operation timeout as [`Duration`].
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }

    /// Drain timeout as [`Duration`].
    pub fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms)
    }
}

/// Config validation failures — field names only, never values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// A required or bounded field was invalid.
    #[error("invalid config field: {0}")]
    Invalid(&'static str),
}
