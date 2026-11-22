//! Typed `vrt-host` binary configuration (TOML, `deny_unknown_fields`).
//!
//! Secret discipline: the config file never carries secret material —
//! `secrets_file`/`vars_file` are paths to JSON maps loaded at startup
//! (`secrets_file` must be `0600`; checked, fail closed). Logs are
//! structured and never contain resolved values.

use serde::Deserialize;
use snapshot_protocol::envelope::TrustedKey;
use std::path::PathBuf;
use std::time::Duration;
use vrt_config::RuntimeParams;

/// Top-level host configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostConfig {
    /// Cell id the `compute.desired` snapshot must be scoped to.
    pub cell_id: String,
    /// Absolute path of the workerd binary (no PATH lookup). In stub
    /// mode this points at `vrt-stub-workerd`.
    pub workerd_binary: PathBuf,
    /// Root for per-tenant generation directories (`0700`).
    pub work_dir: PathBuf,
    /// Content-addressed artifact cache root (`0700`).
    pub cache_dir: PathBuf,
    /// Local ingress listen address (Pod-internal TCP).
    pub listen_addr: String,
    /// T14/B1: control listener for `/readyz` + `/healthz` (separate
    /// from tenant ingress — tenant hostnames can never shadow it).
    /// Loopback-only; required when `desired.assignment_file` is set.
    #[serde(default)]
    pub control_listen: Option<String>,
    /// B12: Prometheus text exposition listener (`GET /metrics`).
    /// Optional — when unset no metrics port is opened (the control
    /// listener still serves `/metrics` for `vrt serve`/standalone).
    /// Binding 0.0.0.0 is an operator choice guarded by NetworkPolicy
    /// (k8s) — the endpoint itself is read-only and leaks no secrets.
    #[serde(default)]
    pub metrics_listen: Option<String>,
    /// Desired-snapshot source.
    pub desired: DesiredConfig,
    /// Emit mode + render inputs.
    pub render: RenderConfig,
    /// Supervisor tunables (defaults documented per field).
    #[serde(default)]
    pub supervisor: SupervisorConfig,
    /// Per-tenant process resource bounds enforced by the monitor.
    #[serde(default)]
    pub limits: LimitsConfig,
    /// Ingress request bounds.
    #[serde(default)]
    pub ingress: IngressConfig,
    /// Non-secret environment entries injected into every child
    /// (before secrets; secrets win on conflict).
    #[serde(default)]
    pub base_env: std::collections::BTreeMap<String, String>,
    /// T15/B2: control-plane bundle pull. Absent → the local
    /// `cacheDir/source` directory is the only source (dev fixtures).
    #[serde(default)]
    pub bundles: Option<BundlesConfig>,
    /// T21: static-assets materialization — `dir` is the
    /// provider-shared content-addressed store; `baseUrl`/`tokenFile`
    /// enable control-plane pull (same contract as `[bundles]`).
    /// Absent → ASSETS bindings fail closed at assemble.
    #[serde(default)]
    pub assets: Option<AssetsConfig>,
    /// P3c-1: Durable Object storage root — the assembler creates
    /// `<dir>/<seg(tenantId)>/<workerId>` (0700, tenant uid) and
    /// workerd stores each class under `<workerId>/<uniqueKey>/`.
    /// MUST live outside `workDir` (generation dirs are swept).
    /// Absent → tenants declaring durable objects fail closed at
    /// assemble; tenants without DOs are unaffected.
    #[serde(default)]
    pub do_storage_dir: Option<PathBuf>,
    /// RV-R66: explicit dev acknowledgement that DO/workflow state is a
    /// SINGLE copy (no object-store placement/snapshots). Default false →
    /// [`HostConfig::durability_posture_error`] fails startup closed.
    /// Forbidden in deployable overlays (`vrt-k8s-check`).
    #[serde(default)]
    pub allow_local_durability: bool,
    /// P3c-2: `[placement]` — DO placement lease authority. Shape
    /// reuses `vrt_sql::service::PersistenceConfig` verbatim (same
    /// camelCase keys, same fs/s3/none backends) so one object store
    /// config style covers D1 persistence and DO placement.
    /// Absent or `backend="none"` → `LocalPlacement` (standalone;
    /// mutual exclusion is the serialized-switch gate). `fs`/`s3` →
    /// `ManifestDoPlacement` (authoritative CAS; init/self-check
    /// failure refuses startup — NEVER falls back to Local).
    #[serde(default)]
    pub placement: Option<vrt_sql::service::PersistenceConfig>,
    /// P3c-2: `[doSnapshots]` — periodic snapshot cadence. Bounds:
    /// interval 60s..24h, jitter ≤ interval (clamped). Only
    /// meaningful with an object-store placement backend.
    #[serde(default)]
    pub do_snapshots: Option<DoSnapshotsConfig>,
    /// WAL1: `[doWal]` — continuous WAL replication (default off).
    pub do_wal: Option<DoWalConfig>,
    /// C1: `[cache]` — tenant-local Cache API service
    /// (`caches.default`/`caches.open`). Presence enables it: every
    /// tenant worker gets `cacheApiOutbound` bound to its own unix
    /// socket under `<workDir>/cache/`; absent ⇒ no emit and
    /// `caches.*` throws `No Cache was configured` (explicit).
    #[serde(default)]
    pub cache: Option<CacheConfig>,
    /// T13/M2: `[isolation]` — per-tenant uid/gid process isolation.
    /// Presence enables `Assembler::with_isolation`: tenant workerd
    /// children setuid to `vrt_securefs::tenant_uid(base, span,
    /// tenantId)` at launch — requires the host process to run as
    /// euid 0 (setuid/setgid), so deployments that run the container
    /// non-root MUST leave this unset. The provider authenticates
    /// connecting peers with the SAME derivation (`[isolation]`
    /// in vrt-provider.toml must carry identical base/span).
    #[serde(default)]
    pub isolation: Option<IsolationConfig>,
    /// Scale to zero (host-local; NOT part of the signed snapshot or
    /// the desired spec). Default OFF — absent or `enabled = false`
    /// leaves every tenant's lifecycle byte-identical to before.
    #[serde(default)]
    pub scale_to_zero: ScaleToZeroConfig,
    /// `[geoip]` — local MMDB resolution for `request.cf` geo/ASN
    /// fields. Default ON with the upstream Merged-IP-Data source;
    /// every failure mode degrades to "no geo data", never to a
    /// startup or request failure.
    #[serde(default = "GeoIpConfig::enabled_default")]
    pub geoip: GeoIpConfig,
    /// `[analyticsEngine]` — per-tenant Analytics Engine ingest
    /// (`env.<binding>.writeDataPoint()`). Always-on when the service
    /// builds: the cell-controller advertises `analyticsEngine`
    /// unconditionally, so the only degrade path is an init failure
    /// — observable via `record_ae_init`/`vrt_host_ae_degraded`, with
    /// AE renders failing closed (`aeAddr`) rather than a fake ingest.
    /// Sockets live under `<workDir>/ae/sock/`, spool + Parquet under
    /// `<workDir>/ae/`.
    #[serde(default = "AnalyticsEngineConfig::enabled_default")]
    pub analytics_engine: AnalyticsEngineConfig,
}

/// `[scaleToZero]` — tenant-level hibernate/wake (design:
/// `doc/platform/workers-runtime-scale-to-zero.md`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScaleToZeroConfig {
    /// Master switch (default `false`).
    #[serde(default)]
    pub enabled: bool,
    /// Idle time before a tenant may hibernate.
    #[serde(default = "default_s2z_idle_ms")]
    pub idle_ms: u64,
    /// Minimum residency after becoming Ready (anti-flap).
    #[serde(default = "default_s2z_residency_ms")]
    pub min_residency_ms: u64,
    /// Host-wide concurrent wakes.
    #[serde(default = "default_s2z_wakes")]
    pub max_concurrent_wakes: usize,
    /// Wake queue bound (beyond it: `wake_capacity`).
    #[serde(default = "default_s2z_queue")]
    pub max_wake_queue: usize,
    /// Quiesce (in-flight + waitUntil) bound before an attempt aborts.
    #[serde(default = "default_s2z_drain_ms")]
    pub drain_timeout_ms: u64,
    /// Bound on one whole wake (queue + launch + ready).
    #[serde(default = "default_s2z_wake_ms")]
    pub wake_deadline_ms: u64,
}

fn default_s2z_idle_ms() -> u64 {
    600_000
}
fn default_s2z_residency_ms() -> u64 {
    120_000
}
fn default_s2z_wakes() -> usize {
    4
}
fn default_s2z_queue() -> usize {
    256
}
fn default_s2z_drain_ms() -> u64 {
    30_000
}
fn default_s2z_wake_ms() -> u64 {
    30_000
}

impl Default for ScaleToZeroConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_ms: default_s2z_idle_ms(),
            min_residency_ms: default_s2z_residency_ms(),
            max_concurrent_wakes: default_s2z_wakes(),
            max_wake_queue: default_s2z_queue(),
            drain_timeout_ms: default_s2z_drain_ms(),
            wake_deadline_ms: default_s2z_wake_ms(),
        }
    }
}

impl ScaleToZeroConfig {
    /// Fail closed: an enabled-but-nonsensical section refuses to
    /// start rather than silently hibernating with odd bounds.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.idle_ms < 1_000 || self.idle_ms > 7 * 24 * 3_600_000 {
            return Err(ConfigError::Invalid("scaleToZero.idleMs must be 1s..7d"));
        }
        if self.min_residency_ms > 24 * 3_600_000 {
            return Err(ConfigError::Invalid(
                "scaleToZero.minResidencyMs must be <= 24h",
            ));
        }
        if self.max_concurrent_wakes == 0 || self.max_concurrent_wakes > 256 {
            return Err(ConfigError::Invalid(
                "scaleToZero.maxConcurrentWakes must be 1..256",
            ));
        }
        if self.max_wake_queue > 100_000 {
            return Err(ConfigError::Invalid(
                "scaleToZero.maxWakeQueue must be <= 100000",
            ));
        }
        if self.drain_timeout_ms < 100 || self.drain_timeout_ms > 600_000 {
            return Err(ConfigError::Invalid(
                "scaleToZero.drainTimeoutMs must be 100ms..10min",
            ));
        }
        if self.wake_deadline_ms < 1_000 || self.wake_deadline_ms > 600_000 {
            return Err(ConfigError::Invalid(
                "scaleToZero.wakeDeadlineMs must be 1s..10min",
            ));
        }
        Ok(())
    }

    /// Build the shared runtime object (ONE coordinator for the whole
    /// host); `None` when the switch is off.
    pub fn build(&self) -> Option<crate::hibernate::ScaleToZero> {
        use crate::hibernate::{BlockerChain, FixedWindowPolicy, ScaleToZero, WakeCoordinator};
        if !self.enabled {
            return None;
        }
        Some(ScaleToZero {
            policy: std::sync::Arc::new(FixedWindowPolicy {
                idle_window: Duration::from_millis(self.idle_ms),
                min_residency: Duration::from_millis(self.min_residency_ms),
            }),
            blockers: std::sync::Arc::new(BlockerChain::p0()),
            coordinator: WakeCoordinator::new(self.max_concurrent_wakes, self.max_wake_queue),
            drain_timeout: Duration::from_millis(self.drain_timeout_ms),
            wake_deadline: Duration::from_millis(self.wake_deadline_ms),
            blocked_recheck: Duration::from_secs(60),
            quiesce_poll: Duration::from_millis(250),
        })
    }
}

/// T13/M2: `[isolation]` — uid/gid assignment range for per-tenant
/// process isolation (see `HostConfig::isolation`).
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolationConfig {
    /// First allocatable tenant uid/gid (gid == uid per tenant).
    pub base: u32,
    /// Range size — uids are `[base, base + span)`.
    pub span: u32,
}

/// C1: `[cache]` capacity contract — in-memory LRU bounds. Presence
/// of the section enables the feature; every bound is required to be
/// >0 and `entry <= tenant <= global` (validated at startup, fail
/// closed — a zero-cap cache would fake the API while storing
/// nothing).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CacheConfig {
    /// Single-entry cap in bytes (stored headers+body+overhead).
    #[serde(default = "default_cache_entry_bytes")]
    pub max_entry_bytes: usize,
    /// Per-tenant total stored bytes.
    #[serde(default = "default_cache_tenant_bytes")]
    pub tenant_bytes: usize,
    /// Whole-service stored bytes across tenants.
    #[serde(default = "default_cache_global_bytes")]
    pub global_bytes: usize,
    /// C1-r2 P1: in-flight PUT body bytes, service-wide. Bounds host
    /// RAM while bodies stream in — over-budget puts reject `busy`
    /// without reading a byte.
    #[serde(default = "default_cache_global_inflight")]
    pub global_inflight_bytes: usize,
    /// In-flight PUT body bytes per tenant.
    #[serde(default = "default_cache_tenant_inflight")]
    pub tenant_inflight_bytes: usize,
}

impl CacheConfig {
    /// Typed limits view for `cache::CacheLimits`.
    pub fn limits(&self) -> crate::cache::CacheLimits {
        crate::cache::CacheLimits {
            max_entry_bytes: self.max_entry_bytes,
            tenant_bytes: self.tenant_bytes,
            global_bytes: self.global_bytes,
            global_inflight_bytes: self.global_inflight_bytes,
            tenant_inflight_bytes: self.tenant_inflight_bytes,
        }
    }
}

fn default_cache_entry_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_cache_tenant_bytes() -> usize {
    128 * 1024 * 1024
}
fn default_cache_global_bytes() -> usize {
    512 * 1024 * 1024
}
fn default_cache_global_inflight() -> usize {
    64 * 1024 * 1024
}
fn default_cache_tenant_inflight() -> usize {
    32 * 1024 * 1024
}

/// `[analyticsEngine]` — AE ingest bounds. Default-enabled (see
/// `HostConfig::analytics_engine`); every bound must be >0 and the
/// flush cadence is clamped to [10, 3600] seconds — a zero cap would
/// fake the feature while dropping every event.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticsEngineConfig {
    /// POST body cap — workerd's `AnalyticsEngineEvent` is
    /// schema-bounded (~64 KiB worst); the default leaves headroom.
    #[serde(default = "default_ae_max_body")]
    pub max_body_bytes: usize,
    /// Per-tenant total spool bytes across datasets. Exceeding it
    /// back-pressures writes with 503 — workerd logs internally and
    /// the JS API stays fire-and-forget (CF's lossy contract).
    #[serde(default = "default_ae_max_spool")]
    pub max_spool_bytes: u64,
    /// Spool→Parquet flush cadence in seconds; clamped to [10, 3600].
    #[serde(default = "default_ae_flush_secs")]
    pub flush_interval_secs: u64,
}

impl AnalyticsEngineConfig {
    /// Default when `[analyticsEngine]` is absent entirely.
    pub fn enabled_default() -> Self {
        Self {
            max_body_bytes: default_ae_max_body(),
            max_spool_bytes: default_ae_max_spool(),
            flush_interval_secs: default_ae_flush_secs(),
        }
    }

    /// Startup validation — bounds must be >0 (a zero cap would fake
    /// the feature while dropping every event; the flush cadence
    /// itself is clamped, not rejected).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_body_bytes == 0 || self.max_spool_bytes == 0 {
            return Err(ConfigError::Invalid("analyticsEngine bounds must be > 0"));
        }
        Ok(())
    }

    /// Typed limits view for `ae::AeLimits`.
    pub fn limits(&self) -> crate::ae::AeLimits {
        crate::ae::AeLimits {
            max_body_bytes: self.max_body_bytes,
            max_spool_bytes: self.max_spool_bytes,
            flush_interval: std::time::Duration::from_secs(
                self.flush_interval_secs.clamp(10, 3600),
            ),
        }
    }
}

fn default_ae_max_body() -> usize {
    256 * 1024
}
fn default_ae_max_spool() -> u64 {
    256 * 1024 * 1024
}
fn default_ae_flush_secs() -> u64 {
    60
}

/// P3c-2: `[doSnapshots]` cadence config.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DoSnapshotsConfig {
    /// Base interval between snapshots per class (ms). Default 300s;
    /// clamped to [60_000, 86_400_000].
    #[serde(default = "default_do_snap_interval_ms")]
    pub interval_ms: u64,
    /// Uniform jitter added per cycle (ms). Default 30s; clamped ≤
    /// interval so due-times never invert.
    #[serde(default = "default_do_snap_jitter_ms")]
    pub jitter_ms: u64,
    /// DO1-r2: max wall-clock window a departing class's `Final`
    /// snapshot retry may run before escalating to error-level
    /// observability. The lease stays held past it — the bound only
    /// escalates. Default 15m; clamped to [60s, 24h].
    #[serde(default = "default_departing_max_ms")]
    pub departing_max_ms: u64,
    /// DO1-r2 P1-1: retention window for quarantined transfer-source
    /// dirs before the stale sweep reclaims them. Default 7d;
    /// clamped to [1h, 30d].
    #[serde(default = "default_xfer_quarantine_ms")]
    pub xfer_quarantine_ms: u64,
    /// DO1-r2 P1-2: departing-retry exponential backoff — base delay
    /// (default 2s) doubles per attempt, capped (default 60s).
    #[serde(default = "default_departing_retry_base_ms")]
    pub departing_retry_base_ms: u64,
    #[serde(default = "default_departing_retry_cap_ms")]
    pub departing_retry_cap_ms: u64,
}

fn default_do_snap_interval_ms() -> u64 {
    300_000
}
fn default_do_snap_jitter_ms() -> u64 {
    30_000
}
fn default_departing_max_ms() -> u64 {
    15 * 60_000
}
fn default_xfer_quarantine_ms() -> u64 {
    7 * 24 * 3600 * 1000
}
fn default_departing_retry_base_ms() -> u64 {
    2_000
}
fn default_departing_retry_cap_ms() -> u64 {
    60_000
}

impl DoSnapshotsConfig {
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms.clamp(60_000, 86_400_000)
    }
    pub fn jitter_ms(&self) -> u64 {
        self.jitter_ms.min(self.interval_ms())
    }
    pub fn departing_max_ms(&self) -> u64 {
        self.departing_max_ms.clamp(60_000, 86_400_000)
    }
    pub fn xfer_quarantine_ms(&self) -> u64 {
        self.xfer_quarantine_ms
            .clamp(3_600_000, 30 * 24 * 3600 * 1000)
    }
    pub fn departing_retry_base_ms(&self) -> u64 {
        self.departing_retry_base_ms.clamp(100, 60_000)
    }
    pub fn departing_retry_cap_ms(&self) -> u64 {
        self.departing_retry_cap_ms
            .clamp(self.departing_retry_base_ms(), 600_000)
    }
}

/// WAL1: `[doWal]` — continuous WAL replication bounds. Every field
/// optional; missing/out-of-range values fall back to defaults and
/// are reported via `resolve()` warnings (fail closed, never loose).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DoWalConfig {
    /// Master switch — default OFF (byte-identical pre-WAL1 path).
    #[serde(default)]
    pub enabled: bool,
    /// Tail pass interval per class (ms). Default 2000; [250, 30_000].
    #[serde(default = "default_wal_tick_ms")]
    pub tick_ms: u64,
    /// Max concurrently-tailed dbs per cell. Default 32; [1, 512].
    #[serde(default = "default_wal_max_dbs")]
    pub max_dbs: u64,
    /// Un-uploaded WAL bytes bound per db (memory + pin span).
    /// Breach → drop stream + rebaseline. Default 256MiB; [8MiB, 4GiB].
    #[serde(default = "default_wal_max_pending")]
    pub max_pending_bytes: u64,
    /// Target segment size. Default 8MiB; [1MiB, 64MiB] (seg cap).
    #[serde(default = "default_wal_max_seg")]
    pub max_segment_bytes: u64,
    /// Per-segment upload attempts (50ms linear backoff).
    /// Default 3; [1, 10].
    #[serde(default = "default_wal_upload_attempts")]
    pub upload_attempts: u64,
    /// Idle db pin-release window (ms). Default 300s; [10s, 1h].
    #[serde(default = "default_wal_idle_stop_ms")]
    pub idle_stop_ms: u64,
    /// Drain flush wall budget (ms). Default 5000; [500, 30_000].
    #[serde(default = "default_wal_flush_max_ms")]
    pub flush_max_ms: u64,
    /// Per-tick segment-upload wall budget (ms) — bounds how long the
    /// tail blocks the supervisor actor loop. Default 500; [100, 5000].
    #[serde(default = "default_wal_tick_budget_ms")]
    pub tick_upload_budget_ms: u64,
    /// Total replay bytes bound per class. Default 1GiB; [64MiB, 8GiB].
    #[serde(default = "default_wal_replay_max")]
    pub replay_max_bytes: u64,
    /// MIG-17d: emit-side chain-byte trigger — a label's uploaded
    /// frame bytes reaching this requests a fresh baseline so the
    /// chain never outgrows `replayMaxBytes`. Absent/invalid →
    /// `replayMaxBytes / 2`. Must be >0, < replayMaxBytes, and leave
    /// ≥ maxPendingBytes headroom (a snapshot window can add that
    /// much before it lands); otherwise resolved value is clamped.
    #[serde(default)]
    pub rebaseline_chain_bytes: Option<u64>,
    #[serde(default = "default_wal_baseline_backoff_max_ms")]
    pub baseline_backoff_max_ms: u64,
}
fn default_wal_tick_ms() -> u64 {
    2_000
}
fn default_wal_max_dbs() -> u64 {
    32
}
fn default_wal_max_pending() -> u64 {
    256 << 20
}
fn default_wal_max_seg() -> u64 {
    8 << 20
}
fn default_wal_baseline_backoff_max_ms() -> u64 {
    1_800_000
}

fn default_wal_upload_attempts() -> u64 {
    3
}
fn default_wal_idle_stop_ms() -> u64 {
    300_000
}
fn default_wal_tick_budget_ms() -> u64 {
    500
}
fn default_wal_flush_max_ms() -> u64 {
    5_000
}
fn default_wal_replay_max() -> u64 {
    1 << 30
}

impl DoWalConfig {
    /// Clamp every field; out-of-range values → default + warning
    /// (fail closed: a bad bound never silently widens the blast).
    pub fn resolve(&self) -> (crate::do_wal::WalCfg, Vec<String>) {
        let d = crate::do_wal::WalCfg::disabled();
        let mut warns = Vec::new();
        let mut pick = |name: &'static str, v: u64, lo: u64, hi: u64, dflt: u64| -> u64 {
            if v < lo || v > hi {
                warns.push(format!(
                    "doWal.{name}={v} out of [{lo},{hi}] — using default {dflt}"
                ));
                dflt
            } else {
                v
            }
        };
        let mut cfg = crate::do_wal::WalCfg {
            enabled: self.enabled,
            tick_ms: pick("tickMs", self.tick_ms, 250, 30_000, d.tick_ms),
            max_dbs: pick("maxDbs", self.max_dbs, 1, 512, d.max_dbs as u64) as usize,
            max_pending_bytes: pick(
                "maxPendingBytes",
                self.max_pending_bytes,
                8 << 20,
                4 << 30,
                d.max_pending_bytes,
            ),
            max_segment_bytes: pick(
                "maxSegmentBytes",
                self.max_segment_bytes,
                1 << 20,
                64 << 20,
                d.max_segment_bytes,
            ),
            upload_attempts: pick(
                "uploadAttempts",
                self.upload_attempts,
                1,
                10,
                d.upload_attempts as u64,
            ) as u32,
            idle_stop_ms: pick(
                "idleStopMs",
                self.idle_stop_ms,
                10_000,
                3_600_000,
                d.idle_stop_ms,
            ),
            flush_max_ms: pick("flushMaxMs", self.flush_max_ms, 500, 30_000, d.flush_max_ms),
            tick_upload_budget_ms: pick(
                "tickUploadBudgetMs",
                self.tick_upload_budget_ms,
                100,
                5_000,
                d.tick_upload_budget_ms,
            ),
            gc_max_per_tick: 64,
            replay_max_bytes: pick(
                "replayMaxBytes",
                self.replay_max_bytes,
                64 << 20,
                8 << 30,
                d.replay_max_bytes,
            ),
            baseline_backoff_max_ms: pick(
                "baselineBackoffMaxMs",
                self.baseline_backoff_max_ms,
                30_000,
                3_600_000,
                d.baseline_backoff_max_ms,
            ),
            rebaseline_chain_bytes: 0, // resolved below (cross-field)
        };
        // MIG-17d: needs the PICKED replay/pending values — invalid
        // input falls back to `replay/2`, then clamps to the
        // `replay - maxPending` headroom (never loosens the cap).
        let rb_dflt = cfg.replay_max_bytes / 2;
        let rb = match self.rebaseline_chain_bytes {
            None => rb_dflt,
            Some(v) if v == 0 || v >= cfg.replay_max_bytes => {
                warns.push(format!(
                    "doWal.rebaselineChainBytes={v} out of (0,{}) — using default {rb_dflt}",
                    cfg.replay_max_bytes
                ));
                rb_dflt
            }
            Some(v) => v,
        };
        let headroom = cfg.replay_max_bytes.saturating_sub(cfg.max_pending_bytes);
        // r2 floor: a degenerate headroom (≈0) must not clamp the
        // threshold to ~1B — that would fire a full snapshot every
        // 30s forever. Prefer a sane floor and accept that
        // cap_exceeded may still occasionally trip (it is alerted
        // and recoverable) over infinite forced baselining.
        let floor = headroom.max(cfg.replay_max_bytes / 8);
        cfg.rebaseline_chain_bytes = if rb > headroom {
            warns.push(format!(
                "doWal.rebaselineChainBytes={rb} leaves <{}B headroom vs replayMaxBytes={} — clamping to {}",
                cfg.max_pending_bytes, cfg.replay_max_bytes, floor
            ));
            floor
        } else {
            rb
        };
        (cfg, warns)
    }
}

/// T21: `[assets]` — materialization config for ASSETS bindings.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetsConfig {
    /// Local content-addressed store root shared with vrt-provider
    /// (`<dir>/<tenant>/manifests+objects`).
    pub dir: PathBuf,
    /// Control-plane pull base (`https://…` or loopback http) —
    /// absent means standalone pre-materialization only (no pull).
    #[serde(default)]
    pub base_url: Option<String>,
    /// Scoped cell token file (`0600`) — required with `baseUrl`.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// Per-attempt request timeout (ms).
    #[serde(default = "default_bundle_request_ms")]
    pub request_timeout_ms: u64,
    /// Whole retry-loop deadline (ms).
    #[serde(default = "default_bundle_deadline_ms")]
    pub deadline_ms: u64,
    /// Total attempts including the first.
    #[serde(default = "default_bundle_max_attempts")]
    pub max_attempts: u32,
}

impl AssetsConfig {
    /// Pull source for platform mode; `None` when only `dir` is set
    /// (standalone — deploy pre-materializes, missing → fail closed).
    pub fn pull_source(&self) -> Result<Option<crate::asset_sync::HttpAssetSource>, String> {
        let Some(base) = &self.base_url else {
            if self.token_file.is_some() {
                return Err("assets.tokenFile requires assets.baseUrl".into());
            }
            return Ok(None);
        };
        let token_file = self
            .token_file
            .as_ref()
            .ok_or_else(|| "assets.baseUrl requires assets.tokenFile".to_string())?;
        let token = std::fs::read_to_string(token_file)
            .map_err(|e| format!("assets.tokenFile: {e}"))?
            .trim()
            .to_string();
        if token.is_empty() {
            return Err("assets.tokenFile is empty".into());
        }
        Ok(Some(
            crate::asset_sync::HttpAssetSource::new(
                base.clone(),
                token,
                std::time::Duration::from_millis(self.request_timeout_ms),
                std::time::Duration::from_millis(self.deadline_ms),
                self.max_attempts,
            )
            .map_err(|e| e.to_string())?,
        ))
    }
}

/// T15/B2: `[bundles]` — pull bundles from the control plane.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BundlesConfig {
    /// T17: local directory source (`<dir>/<digest>`) — the standalone
    /// mode reads `vrt-local`'s `objects/` directly. Mutually exclusive
    /// with `baseUrl`/`tokenFile` (validated).
    #[serde(default)]
    pub dir: Option<PathBuf>,
    /// Route base — `https://…` required; loopback `http://` allowed
    /// for dev/test (`http://127.0.0.1`/`[::1]`/`localhost` only).
    #[serde(default)]
    pub base_url: Option<String>,
    /// File containing the scoped cell token (`vrt.cell-controller`).
    /// Must exist, be `0600`, non-empty. Value is never logged.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// Per-attempt request timeout (ms).
    #[serde(default = "default_bundle_request_ms")]
    pub request_timeout_ms: u64,
    /// Whole retry-loop deadline (ms).
    #[serde(default = "default_bundle_deadline_ms")]
    pub deadline_ms: u64,
    /// Total attempts including the first.
    #[serde(default = "default_bundle_max_attempts")]
    pub max_attempts: u32,
    /// Hard cap on one object body — same bound as the control-plane
    /// upload cap (32 MiB).
    #[serde(default = "default_bundle_max_bytes")]
    pub max_bytes: u64,
}

fn default_bundle_request_ms() -> u64 {
    10_000
}
fn default_bundle_deadline_ms() -> u64 {
    30_000
}
fn default_bundle_max_attempts() -> u32 {
    3
}
fn default_bundle_max_bytes() -> u64 {
    32 * 1024 * 1024
}

impl BundlesConfig {
    /// Validated config → `HttpBundleConfig` + token (file read +
    /// permission check; the token never enters config Debug output —
    /// `BundlesConfig` carries the PATH only).
    pub fn to_http_config(&self) -> Result<crate::bundle_http::HttpBundleConfig, ConfigError> {
        if self.dir.is_some() {
            return Err(ConfigError::Invalid("bundles.dir+http"));
        }
        let base_url = self
            .base_url
            .as_ref()
            .ok_or(ConfigError::Invalid("bundles.baseUrl"))?;
        let token_file = self
            .token_file
            .as_ref()
            .ok_or(ConfigError::Invalid("bundles.tokenFile"))?;
        let meta = std::fs::metadata(token_file).map_err(|_| ConfigError::InsecureTokenFile)?;
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o777 != 0o600 {
            return Err(ConfigError::InsecureTokenFile);
        }
        let token = std::fs::read_to_string(token_file)
            .map_err(|_| ConfigError::InsecureTokenFile)?
            .trim()
            .to_string();
        if token.is_empty() || token.len() > 512 || !token.is_ascii() {
            return Err(ConfigError::InsecureTokenFile);
        }
        Ok(crate::bundle_http::HttpBundleConfig {
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
            request_timeout: std::time::Duration::from_millis(self.request_timeout_ms),
            deadline: std::time::Duration::from_millis(self.deadline_ms),
            max_attempts: self.max_attempts,
            max_bytes: self.max_bytes as usize,
        })
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
    /// Poll interval for snapshot reload + converge.
    #[serde(default = "default_reload_ms")]
    pub reload_ms: u64,
    /// Previous active versions kept authorized (drain grace).
    #[serde(default = "default_grace_versions")]
    pub grace_versions: usize,
    /// T14/B1: `assignment.json` dropped beside `path` by the
    /// cell-controller's FileEnvelopePodChannel. When set, this pod
    /// runs ONLY the workers named for `pod_id` — the envelope alone
    /// is never sufficient authority.
    #[serde(default)]
    pub assignment_file: Option<PathBuf>,
    /// T14/B1: this pod's identity — must equal `assignment.podId`.
    /// Required when `assignment_file` is set (else fail closed at
    /// validate: an unlabeled pod could adopt any assignment).
    #[serde(default)]
    pub pod_id: Option<String>,
}

fn default_reload_ms() -> u64 {
    1_000
}
fn default_grace_versions() -> usize {
    1
}

/// Emit mode + render inputs.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenderConfig {
    /// `"capnp"` (production workerd config) or `"stub"`
    /// (`vrt-stub-workerd`, only in `dev-stub` builds — a production
    /// binary refuses it with `StubModeDisabled`).
    pub mode: String,
    /// JSON file with static render params (`bindingBackendSocket`,
    /// `egress`, `facades`, `systemWorkers`, compat). Capnp mode only.
    pub params_file: Option<PathBuf>,
    /// JSON map `varRef -> plaintext` (capnp mode).
    pub vars_file: Option<PathBuf>,
    /// JSON map `secretRef -> value` (capnp mode). File must be `0600`.
    pub secrets_file: Option<PathBuf>,
    /// T19b/B4: combined `{vars,secrets}` file (platform mode). Must be
    /// `0600`; takes precedence over varsFile/secretsFile.
    pub render_inputs_file: Option<PathBuf>,
    /// Extra verbatim argv appended after the fixed arguments (constant
    /// flags only — never secrets).
    #[serde(default)]
    pub extra_args: Vec<String>,
    /// T16/B3: derive render params from `facade/manifest.json` at
    /// startup instead of a static `params_file` — mutually exclusive
    /// (two sources of truth fail closed at validate).
    #[serde(default)]
    pub facade_manifest: Option<PathBuf>,
    /// `binding-backend` unix socket path (facadeManifest mode) —
    /// VERBATIM single path, emitted to every tenant. Single-tenant
    /// contract only (standalone `vrt init` renders it for THE
    /// tenant): vrt-provider binds `<listenerDir>/<seg(tenant)>.sock`
    /// per tenant and treats the socket name as the tenant identity,
    /// so a shared static path would route every tenant through ONE
    /// tenant's listener — cross-tenant authorization (H5-r1).
    /// Forbidden when `desired.assignmentFile` is set (the platform
    /// multi-tenant contract); use `bindingBackendDir` there.
    #[serde(default)]
    pub binding_backend_socket: Option<String>,
    /// `binding-backend` unix socket DIRECTORY (facadeManifest mode,
    /// platform cells) — the provider's `listenerDir`. Every tenant's
    /// `--external-addr` is derived per tenant as
    /// `vrt_securefs::socket_path(dir, tenant)` — byte-identical to
    /// the socket the provider binds, one derivation shared by both
    /// sides. Mutually exclusive with `bindingBackendSocket`
    /// (config load fails closed — never "both set, pick one").
    /// FacadeManifest mode only (a paramsFile carries its own
    /// verbatim socket — two sources of truth are never allowed).
    #[serde(default)]
    pub binding_backend_dir: Option<PathBuf>,
    /// System-worker compatibility date (facadeManifest mode).
    #[serde(default)]
    pub system_compat_date: Option<String>,
    /// System-worker compatibility flags (facadeManifest mode).
    #[serde(default)]
    pub system_compat_flags: Vec<String>,
    /// Optional egress-policy JSON document (facadeManifest mode);
    /// absent → the restrictive default policy.
    #[serde(default)]
    pub egress_policy_file: Option<PathBuf>,
    /// Per-worker quotas: render the workerd `limits` block for quotas a
    /// worker declares (capnp mode only). The pinned workerd
    /// (`deploy/docker/workerd.lock`) carries the VRT limits patch, so
    /// this defaults ON like the other pin-derived capabilities. Set it
    /// to `false` only when `workerdBinary` is a STOCK workerd: a tenant
    /// declaring a limit then fails closed at assemble
    /// (`CONFIG_QUOTA_UNSUPPORTED`) instead of running unlimited. (With
    /// the flag on and a stock binary, the config dry-run rejects the
    /// generation loudly — workerd does not understand `limits`.)
    #[serde(default = "default_true")]
    pub quotas: bool,
}

/// Supervisor tunables — milliseconds unless stated.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SupervisorConfig {
    /// Generation ready deadline.
    #[serde(default = "default_ready_ms")]
    pub ready_timeout_ms: u64,
    /// Per-socket connect-probe slice.
    #[serde(default = "default_probe_ms")]
    pub probe_timeout_ms: u64,
    /// SIGTERM→SIGKILL escalation grace.
    #[serde(default = "default_kill_grace_ms")]
    pub kill_grace_ms: u64,
    /// Restart backoff base.
    #[serde(default = "default_backoff_base_ms")]
    pub backoff_base_ms: u64,
    /// Restart backoff cap.
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: u64,
    /// Rolling failure window.
    #[serde(default = "default_fail_window_ms")]
    pub fail_window_ms: u64,
    /// Failures inside the window that trip the circuit.
    #[serde(default = "default_fail_threshold")]
    pub fail_threshold: usize,
    /// Circuit cooldown before the half-open attempt.
    #[serde(default = "default_cooldown_ms")]
    pub circuit_cooldown_ms: u64,
    /// Per-generation in-flight drain cap.
    #[serde(default = "default_drain_ms")]
    pub drain_timeout_ms: u64,
    /// MIG-12: post-idle `ctx.waitUntil` drain grace — after a
    /// draining generation's HTTP in-flight reaches zero, SIGTERM is
    /// deferred while its probe reports pending waitUntil work, up to
    /// this bound (the CF ceiling). `0` disables the wait.
    #[serde(default = "default_wait_until_grace_ms")]
    pub wait_until_grace_ms: u64,
    /// MIG-12-r2: tighter waitUntil bound for generations draining on
    /// the serialized-DO-switch path — that wait sits INSIDE the
    /// switch-hold window (held requests time out at
    /// `ingress.switchHoldMs`), so it gets its own smaller cap
    /// (default 2 s). `0` disables the wait for do_switch
    /// generations only — an idle old generation SIGTERMs in the
    /// same turn and the hold closes as fast as the process dies.
    /// Must stay strictly below `ingress.switchHoldMs` (cross-field
    /// check in `HostConfig::validate`) or the hold deadline was
    /// never going to be met anyway.
    #[serde(default = "default_do_switch_wait_until_grace_ms")]
    pub do_switch_wait_until_grace_ms: u64,
    /// Resource sampling interval.
    #[serde(default = "default_monitor_ms")]
    pub monitor_interval_ms: u64,
    /// T13/M4: consecutive switch-launch failures before the tenant
    /// latches `SwitchFailed` — retries stop, old generation keeps
    /// serving, a new Apply re-arms.
    #[serde(default = "default_max_switch_failures")]
    pub max_switch_failures: u32,
    /// MIG-8: bound on ONE `workerd test` config preflight — covers
    /// global-admission queueing AND the child itself.
    #[serde(default = "default_preflight_ms")]
    pub preflight_timeout_ms: u64,
    /// MIG-17: per-class DO placement pipeline fan-out — how many
    /// class `acquire → handoff → restore → claim → record` chains
    /// run in flight at once on the (serialized-switch) launch path.
    /// Bounds the store round-trip fan-in to ~ceil(N/K) instead of N.
    /// Valid range 1..=64 (see `do_switch_pipeline::MAX_CONCURRENCY`).
    #[serde(default = "default_do_pipeline_concurrency")]
    pub do_pipeline_concurrency: usize,
    /// MIG-17: soft bound (ms) on ONE placement/restore op — an op
    /// returning past it fails its class (fail closed; the sync op
    /// itself can't be aborted, so the bound lands on completion and
    /// before the next op). Must be >= 1000.
    #[serde(default = "default_do_pipeline_op_timeout_ms")]
    pub do_pipeline_op_timeout_ms: u64,
    /// MIG-17: whole placement-pipeline wall budget (ms) per launch
    /// attempt — unstarted ops fail `Budget` once spent (a +5 s retry
    /// gets a fresh budget). Must be >= doPipelineOpTimeoutMs.
    #[serde(default = "default_do_pipeline_budget_ms")]
    pub do_pipeline_budget_ms: u64,
    /// MIG-17-r2 (B1): bound (ms) on ONE atomic restore unit —
    /// snapshot download + `mark_local_dir_epoch` + WAL tail replay
    /// judged only AS A WHOLE (never mid-unit: a timeout inside it
    /// would strand a restored-but-unreplayed dir). Wider than the
    /// per-op timeout because snapshot pulls legitimately outlive a
    /// placement round trip. Must be >= doPipelineOpTimeoutMs.
    #[serde(default = "default_do_pipeline_restore_timeout_ms")]
    pub do_pipeline_restore_timeout_ms: u64,
}

fn default_ready_ms() -> u64 {
    10_000
}
fn default_probe_ms() -> u64 {
    2_000
}
fn default_kill_grace_ms() -> u64 {
    5_000
}
fn default_backoff_base_ms() -> u64 {
    100
}
fn default_backoff_max_ms() -> u64 {
    5_000
}
fn default_fail_window_ms() -> u64 {
    // T27/N0: must satisfy failThreshold*(readyTimeout+backoffMax)
    // — 5×(10s+5s)=75s → 90s default keeps the stock config valid.
    90_000
}
fn default_fail_threshold() -> usize {
    5
}
fn default_cooldown_ms() -> u64 {
    30_000
}
fn default_drain_ms() -> u64 {
    30_000
}
fn default_monitor_ms() -> u64 {
    5_000
}
fn default_wait_until_grace_ms() -> u64 {
    30_000
}
fn default_do_switch_wait_until_grace_ms() -> u64 {
    2_000
}
fn default_max_switch_failures() -> u32 {
    5
}
fn default_preflight_ms() -> u64 {
    15_000
}
fn default_do_pipeline_concurrency() -> usize {
    crate::do_switch_pipeline::DEFAULT_CONCURRENCY
}
fn default_do_pipeline_op_timeout_ms() -> u64 {
    10_000
}
fn default_do_pipeline_budget_ms() -> u64 {
    30_000
}
fn default_do_pipeline_restore_timeout_ms() -> u64 {
    120_000
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            ready_timeout_ms: default_ready_ms(),
            probe_timeout_ms: default_probe_ms(),
            kill_grace_ms: default_kill_grace_ms(),
            backoff_base_ms: default_backoff_base_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            fail_window_ms: default_fail_window_ms(),
            fail_threshold: default_fail_threshold(),
            circuit_cooldown_ms: default_cooldown_ms(),
            drain_timeout_ms: default_drain_ms(),
            wait_until_grace_ms: default_wait_until_grace_ms(),
            do_switch_wait_until_grace_ms: default_do_switch_wait_until_grace_ms(),
            monitor_interval_ms: default_monitor_ms(),
            max_switch_failures: default_max_switch_failures(),
            preflight_timeout_ms: default_preflight_ms(),
            do_pipeline_concurrency: default_do_pipeline_concurrency(),
            do_pipeline_op_timeout_ms: default_do_pipeline_op_timeout_ms(),
            do_pipeline_budget_ms: default_do_pipeline_budget_ms(),
            do_pipeline_restore_timeout_ms: default_do_pipeline_restore_timeout_ms(),
        }
    }
}

impl SupervisorConfig {
    /// Field bounds enforced at startup — fail closed, never clamp.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // MIG-12: bounded waitUntil grace — `0` is the documented
        // "disable the wait" value, so only an upper bound applies
        // (5 min keeps it inside any sane termination budget).
        if self.wait_until_grace_ms > 300_000 {
            return Err(ConfigError::Invalid(
                "supervisor.waitUntilGraceMs must be <= 300000",
            ));
        }
        // MIG-12-r2: 30 s keeps a do_switch generation's wait inside
        // any sane `ingress.switchHoldMs` ceiling (hold itself is
        // capped at 300 s); `0` is the documented "no wait" value.
        if self.do_switch_wait_until_grace_ms > 30_000 {
            return Err(ConfigError::Invalid(
                "supervisor.doSwitchWaitUntilGraceMs must be <= 30000",
            ));
        }
        // MIG-17: bounded fan-out — 0 would serialize nothing (it is
        // clamped to 1 anyway, but a zero config is a mistake, fail
        // closed) and >64 would pin the shared store runtime on
        // oversized tenants.
        if !(1..=crate::do_switch_pipeline::MAX_CONCURRENCY).contains(&self.do_pipeline_concurrency)
        {
            return Err(ConfigError::Invalid(
                "supervisor.doPipelineConcurrency must be in 1..=64",
            ));
        }
        if self.do_pipeline_op_timeout_ms < 1_000 || self.do_pipeline_op_timeout_ms > 120_000 {
            return Err(ConfigError::Invalid(
                "supervisor.doPipelineOpTimeoutMs must be in 1000..=120000",
            ));
        }
        if self.do_pipeline_budget_ms < self.do_pipeline_op_timeout_ms
            || self.do_pipeline_budget_ms > 600_000
        {
            return Err(ConfigError::Invalid(
                "supervisor.doPipelineBudgetMs must be >= doPipelineOpTimeoutMs and <= 600000",
            ));
        }
        // MIG-17-r2 (B1): the restore unit is atomic — a bound
        // narrower than the per-op timeout would re-open the
        // mid-unit-timeout window the unit exists to close.
        if self.do_pipeline_restore_timeout_ms < self.do_pipeline_op_timeout_ms
            || self.do_pipeline_restore_timeout_ms > 600_000
        {
            return Err(ConfigError::Invalid(
                "supervisor.doPipelineRestoreTimeoutMs must be >= doPipelineOpTimeoutMs and <= 600000",
            ));
        }
        Ok(())
    }

    /// Into actor params.
    pub fn params(&self) -> crate::supervisor::SupervisorParams {
        crate::supervisor::SupervisorParams {
            ready_timeout: Duration::from_millis(self.ready_timeout_ms),
            probe_timeout: Duration::from_millis(self.probe_timeout_ms),
            kill_grace: Duration::from_millis(self.kill_grace_ms),
            backoff: (
                Duration::from_millis(self.backoff_base_ms),
                Duration::from_millis(self.backoff_max_ms),
            ),
            fail_window: Duration::from_millis(self.fail_window_ms),
            fail_threshold: self.fail_threshold,
            circuit_cooldown: Duration::from_millis(self.circuit_cooldown_ms),
            drain_timeout: Duration::from_millis(self.drain_timeout_ms),
            wait_until_grace: Duration::from_millis(self.wait_until_grace_ms),
            do_switch_wait_until_grace: Duration::from_millis(self.do_switch_wait_until_grace_ms),
            monitor_interval: Duration::from_millis(self.monitor_interval_ms),
            max_switch_failures: self.max_switch_failures,
            preflight_timeout: Duration::from_millis(self.preflight_timeout_ms),
            // MIG-13-r3: `bin/vrt-host.rs` may RAISE this to
            // 2x ingress.switchHoldMs, but the floor is owned here so
            // every caller gets a correctly-sized cap: the episode must
            // outlive a worst-case drain window —
            // `drainTimeoutMs` (total drain cap) +
            // `doSwitchWaitUntilGraceMs` + `killGraceMs`
            // (SIGTERM→SIGKILL escalation) + 5 s exit/swap margin —
            // otherwise the expiry could fire inside the gate window
            // and point live early-flip routes at a killed warm
            // process (the r2 502). `on_tick` additionally never fires
            // the cap while the switch gate is open.
            prewarm_ttl: Duration::from_secs(20).max(
                Duration::from_millis(self.drain_timeout_ms)
                    + Duration::from_millis(self.do_switch_wait_until_grace_ms)
                    + Duration::from_millis(self.kill_grace_ms)
                    + Duration::from_secs(5),
            ),
            scale_to_zero: None,
            do_pipeline: crate::do_switch_pipeline::PipeCfg {
                concurrency: self.do_pipeline_concurrency,
                op_timeout: Duration::from_millis(self.do_pipeline_op_timeout_ms),
                total_budget: Duration::from_millis(self.do_pipeline_budget_ms),
                restore_timeout: Duration::from_millis(self.do_pipeline_restore_timeout_ms),
            },
        }
    }
}

/// Per-tenant process resource bounds (monitor-enforced).
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LimitsConfig {
    /// Max child RSS bytes (`None` = unbounded).
    #[serde(default)]
    pub max_rss_bytes: Option<u64>,
    /// Max child file descriptors (`None` = unbounded).
    #[serde(default)]
    pub max_fds: Option<u64>,
    /// PT1: dual-signal runaway watchdog. Absent ⇒ enabled with
    /// conservative defaults (fail closed: a spinning tenant must not
    /// pin a core forever); `cpuRunaway: { enabled: false }` disables.
    #[serde(default = "default_cpu_runaway")]
    pub cpu_runaway: CpuRunawayConfig,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_rss_bytes: None,
            max_fds: None,
            cpu_runaway: default_cpu_runaway(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_cpu_runaway() -> CpuRunawayConfig {
    CpuRunawayConfig {
        enabled: true,
        window_ms: default_runaway_window_ms(),
        min_cpu_milli: default_runaway_min_cpu_milli(),
        heartbeat_timeout_ms: default_runaway_heartbeat_timeout_ms(),
        heartbeat_failures: default_runaway_heartbeat_failures(),
    }
}

fn default_runaway_window_ms() -> u64 {
    15_000
}
fn default_runaway_min_cpu_milli() -> u32 {
    900
}
fn default_runaway_heartbeat_timeout_ms() -> u64 {
    250
}
fn default_runaway_heartbeat_failures() -> u32 {
    2
}

/// PT1 runaway watchdog knobs — every field defaulted so
/// `cpuRunaway: {}` is a valid "use defaults" statement.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuRunawayConfig {
    /// Master switch (absent field set ⇒ on; `enabled:false` ⇒ off).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Continuous-saturation window required before signal 1 counts
    /// (covers >= 2 monitor ticks at the 5s default).
    #[serde(default = "default_runaway_window_ms")]
    pub window_ms: u64,
    /// CPU ratio ×1000 (cores busy / wall time). A tight JS spin pins
    /// ~1 core; 900 tolerates scheduler noise without missing it.
    #[serde(default = "default_runaway_min_cpu_milli")]
    pub min_cpu_milli: u32,
    /// Per-probe heartbeat response timeout — the event-loop-dead
    /// signal (a healthy in-process worker answers in µs).
    #[serde(default = "default_runaway_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: u64,
    /// Consecutive probe failures required for signal 2.
    #[serde(default = "default_runaway_heartbeat_failures")]
    pub heartbeat_failures: u32,
}

impl CpuRunawayConfig {
    /// Fail-closed bounds — a nonsense value must be refused at
    /// startup, never produce a degenerate kill loop.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if !(2_000..=600_000).contains(&self.window_ms) {
            return Err(ConfigError::Parse(
                "limits.cpuRunaway.windowMs must be 2000..=600000",
            ));
        }
        if self.min_cpu_milli == 0 || self.min_cpu_milli > 64_000 {
            return Err(ConfigError::Parse(
                "limits.cpuRunaway.minCpuMilli must be 1..=64000",
            ));
        }
        if !(50..=10_000).contains(&self.heartbeat_timeout_ms) {
            return Err(ConfigError::Parse(
                "limits.cpuRunaway.heartbeatTimeoutMs must be 50..=10000",
            ));
        }
        if self.heartbeat_failures == 0 || self.heartbeat_failures > 64 {
            return Err(ConfigError::Parse(
                "limits.cpuRunaway.heartbeatFailures must be 1..=64",
            ));
        }
        Ok(())
    }

    /// Resolved spec form (`None` when disabled).
    pub fn spec(&self) -> Option<crate::runaway::RunawaySpec> {
        if !self.enabled {
            return None;
        }
        Some(crate::runaway::RunawaySpec {
            window: Duration::from_millis(self.window_ms),
            min_cpu_ratio: self.min_cpu_milli as f64 / 1000.0,
            heartbeat_timeout: Duration::from_millis(self.heartbeat_timeout_ms),
            heartbeat_failures: self.heartbeat_failures,
        })
    }
}

impl LimitsConfig {
    /// Into spec limits.
    pub fn resource_limits(&self) -> crate::spec::ResourceLimits {
        crate::spec::ResourceLimits {
            max_rss_bytes: self.max_rss_bytes,
            max_fds: self.max_fds,
            runaway: self.cpu_runaway.spec(),
        }
    }
}

/// Ingress request bounds.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IngressConfig {
    /// Whole request→response-head timeout.
    #[serde(default = "default_req_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Upstream connect timeout.
    #[serde(default = "default_conn_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Max request body bytes.
    #[serde(default = "default_max_body")]
    pub max_body_bytes: usize,
    /// W4-r2: per-request budget while a serialized DO switch holds
    /// the tenant's routes (0 disables holding → immediate 503).
    #[serde(default = "default_switch_hold_ms")]
    pub switch_hold_ms: u64,
    /// W4-r2: per-tenant bound on simultaneously held requests.
    #[serde(default = "default_switch_hold_reqs")]
    pub switch_hold_max_reqs: usize,
    /// W4-r2: per-tenant bound on declared held bytes.
    #[serde(default = "default_switch_hold_bytes")]
    pub switch_hold_max_bytes: usize,
    /// MIG-7: how the `x-forwarded-proto` value forwarded to workerd
    /// (`forwardedProtoHeader` → `request.url` scheme) is derived.
    /// Default `fixed-http` — fail closed, never reports cleartext
    /// traffic as https.
    #[serde(default)]
    pub forwarded_proto: ForwardedProtoMode,
    /// MIG-7: front-proxy source CIDRs honored by
    /// `forwardedProto="trusted-proxy-header"` (e.g. the Traefik pod
    /// range — the same set NetworkPolicy allows to the ingress port).
    /// REQUIRED with that mode, REJECTED otherwise — an ambiguous
    /// config fails startup rather than silently trusting nothing.
    #[serde(default)]
    pub forwarded_proto_trusted: Vec<vrt_config::Cidr>,
    /// MIG-9: typed upstream client-IP trust (`[ingress.clientIp]`) —
    /// whether and how `cf-connecting-ip` is synthesized for workers.
    /// Default `mode = "none"` — fail closed, never injected.
    #[serde(default)]
    pub client_ip: ClientIpConfig,
    /// WebSocket tunnel bounds (`[ingress.websocket]`). Every field is
    /// optional; an absent section keeps the built-in defaults.
    #[serde(default)]
    pub websocket: WebsocketConfig,
    /// `vrt dev` only: route requests with an unmatched Host to the
    /// desired doc's single worker (any Host). Absent/false keeps the
    /// strict exact-host routing production requires. Not settable to
    /// true by accident — validation rejects it when the desired doc
    /// carries more than one worker is impossible to know at config
    /// time, so the fallback only ever applies to a single-worker
    /// route table (see `RouteTable::catch_all`).
    #[serde(default)]
    pub dev_catch_all: bool,
}

/// `[ingress.websocket]` — bounds for upgraded WebSocket sessions.
/// Out-of-range values fail startup validation; nothing is silently
/// clamped.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebsocketConfig {
    /// Close a session after this many seconds without bytes in either
    /// direction. Range 1..=3600, default 300.
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
    /// Hard ceiling on one session's lifetime in seconds. Range
    /// 1..=86400, default 14400 (4 h).
    #[serde(default)]
    pub max_duration_secs: Option<u64>,
    /// Ingress-wide cap on concurrent sessions. Range 1..=65536,
    /// default 4096.
    #[serde(default)]
    pub max_sessions: Option<usize>,
    /// Per-tenant cap on concurrent sessions. Range 1..=maxSessions,
    /// default 256 (or maxSessions when that is smaller).
    #[serde(default)]
    pub max_sessions_per_tenant: Option<usize>,
}

/// Resolved WebSocket limits (defaults applied, already validated).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WebsocketLimits {
    /// Idle timeout.
    pub idle_timeout: Duration,
    /// Maximum session lifetime.
    pub max_duration: Duration,
    /// Ingress-wide session cap.
    pub max_sessions: usize,
    /// Per-tenant session cap.
    pub max_sessions_per_tenant: usize,
}

impl WebsocketConfig {
    /// Range checks (fail closed — never clamps).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(v) = self.idle_timeout_secs {
            if !(1..=3600).contains(&v) {
                return Err(ConfigError::Invalid(
                    "ingress.websocket.idleTimeoutSecs must be 1..=3600",
                ));
            }
        }
        if let Some(v) = self.max_duration_secs {
            if !(1..=86_400).contains(&v) {
                return Err(ConfigError::Invalid(
                    "ingress.websocket.maxDurationSecs must be 1..=86400",
                ));
            }
        }
        if let Some(v) = self.max_sessions {
            if !(1..=65_536).contains(&v) {
                return Err(ConfigError::Invalid(
                    "ingress.websocket.maxSessions must be 1..=65536",
                ));
            }
        }
        if let Some(v) = self.max_sessions_per_tenant {
            let cap = self
                .max_sessions
                .unwrap_or(crate::ingress::WS_DEFAULT_MAX_SESSIONS);
            if v < 1 || v > cap {
                return Err(ConfigError::Invalid(
                    "ingress.websocket.maxSessionsPerTenant must be 1..=maxSessions",
                ));
            }
        }
        Ok(())
    }

    /// The effective limits. Call after [`Self::validate`].
    pub fn limits(&self) -> WebsocketLimits {
        let max_sessions = self
            .max_sessions
            .unwrap_or(crate::ingress::WS_DEFAULT_MAX_SESSIONS);
        WebsocketLimits {
            idle_timeout: self
                .idle_timeout_secs
                .map(Duration::from_secs)
                .unwrap_or(crate::ingress::WS_DEFAULT_IDLE_TIMEOUT),
            max_duration: self
                .max_duration_secs
                .map(Duration::from_secs)
                .unwrap_or(crate::ingress::WS_DEFAULT_MAX_DURATION),
            max_sessions,
            max_sessions_per_tenant: self
                .max_sessions_per_tenant
                .unwrap_or_else(|| crate::ingress::WS_DEFAULT_MAX_PER_TENANT.min(max_sessions)),
        }
    }
}

/// `[geoip]` — local GeoIP database for `request.cf` (see
/// [`crate::geoip`]). Enabled by default with the public
/// Merged-IP-Data source; set `enabled = false` for airgap pods or
/// when geo fields are unwanted.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GeoIpConfig {
    /// Resolve `request.cf` geo/ASN fields from a local MMDB.
    /// Default true — a missing/unreadable database degrades to
    /// upstream-only fields, so default-on costs nothing.
    #[serde(default = "geoip_enabled")]
    pub enabled: bool,
    /// MMDB file path. Default `<workDir>/geoip/Merged-IP.mmdb`
    /// (resolved against `workDir` at startup, which also puts the
    /// `*.part` download and `*.state.json` next to it).
    #[serde(default)]
    pub mmdb_path: Option<PathBuf>,
    /// Releases-API URL polled for the current tag — the tag check
    /// decides whether the ~100 MB download runs at all. Default the
    /// upstream GitHub API. Set to `""` for mirror mode: skip the tag
    /// check and re-download when the file is older than
    /// `refreshIntervalSecs` (for mirrors serving only the binary).
    #[serde(default)]
    pub releases_api_url: Option<String>,
    /// Direct database download URL — used as-is in mirror mode, and
    /// as the fallback when the API names no `.mmdb` asset. Must be
    /// `https://` when set.
    #[serde(default)]
    pub download_url: Option<String>,
    /// Poll cadence in seconds. Range 300..=604800, default 43200
    /// (12h — upstream ships daily, and anonymous GitHub API callers
    /// get 60 req/h/IP so cells behind one NAT stay far below it).
    #[serde(default)]
    pub refresh_interval_secs: Option<u64>,
}

fn geoip_enabled() -> bool {
    true
}

impl GeoIpConfig {
    /// Default when `[geoip]` is absent entirely — enabled with
    /// upstream defaults (see `HostConfig::geoip` field doc).
    pub fn enabled_default() -> Self {
        Self {
            enabled: true,
            mmdb_path: None,
            releases_api_url: None,
            download_url: None,
            refresh_interval_secs: None,
        }
    }

    /// Startup validation — the only hard failures are malformed
    /// operator input (non-HTTPS URL, out-of-range interval), never
    /// runtime conditions like a missing file.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        for url in [
            self.releases_api_url.as_deref(),
            self.download_url.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if !url.is_empty() && !url.starts_with("https://") {
                return Err(ConfigError::Invalid(
                    "geoip URLs must be https:// (or empty)",
                ));
            }
        }
        // Mirror mode (`releasesApiUrl = ""`) has no API to supply a
        // download URL — `downloadUrl` is then the ONLY source and an
        // explicit empty string would leave the task error-looping.
        if self.releases_api_url.as_deref() == Some("") && self.download_url.as_deref() == Some("") {
            return Err(ConfigError::Invalid(
                "geoip mirror mode (releasesApiUrl=\"\") needs a downloadUrl",
            ));
        }
        if let Some(secs) = self.refresh_interval_secs {
            if !(300..=604800).contains(&secs) {
                return Err(ConfigError::Invalid(
                    "geoip.refreshIntervalSecs must be 300..=604800",
                ));
            }
        }
        Ok(())
    }

    /// Resolved database path (explicit `mmdbPath`, else
    /// `<workDir>/geoip/Merged-IP.mmdb`).
    pub fn mmdb_path_or(&self, work_dir: &std::path::Path) -> PathBuf {
        self.mmdb_path
            .clone()
            .unwrap_or_else(|| work_dir.join("geoip").join("Merged-IP.mmdb"))
    }

    /// The sync task config (only called when `enabled`).
    pub fn sync_config(&self, work_dir: &std::path::Path) -> crate::geoip::sync::GeoIpSyncConfig {
        crate::geoip::sync::GeoIpSyncConfig {
            mmdb_path: self.mmdb_path_or(work_dir),
            releases_api_url: self
                .releases_api_url
                .clone()
                .unwrap_or_else(|| crate::geoip::sync::DEFAULT_RELEASES_API.to_string()),
            download_url: self
                .download_url
                .clone()
                .unwrap_or_else(|| crate::geoip::sync::DEFAULT_DOWNLOAD_URL.to_string()),
            refresh_interval: Duration::from_secs(self.refresh_interval_secs.unwrap_or(43200)),
        }
    }
}

impl Default for GeoIpConfig {
    /// `serde(default)` on the FIELD means "absent → enabled" only via
    /// [`HostConfig::geoip`]'s explicit default fn; a bare
    /// `GeoIpConfig::default()` (tests, other callers) matches that
    /// same enabled-with-upstream-defaults shape.
    fn default() -> Self {
        Self::enabled_default()
    }
}

/// MIG-9: typed upstream client-IP trust layer — see
/// [`crate::ingress::ClientIpPolicy`]. Nested under `[ingress]` as
/// `[ingress.clientIp]` so the mode, the trusted-proxy set, and the
/// Cloudflare range list validate as one unit at startup.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientIpConfig {
    /// Trust mode for proxy-derived client identity.
    #[serde(default)]
    pub mode: ClientIpMode,
    /// Front-proxy source CIDRs allowed to supply proxy-derived client
    /// identity — REQUIRED for `trusted-xff`/`cloudflare`, REJECTED for
    /// `none`. The deployment sources this from the same addresses the
    /// ingress-port NetworkPolicy `from` peers enumerate; `0.0.0.0/0`
    /// or `::/0` would trust EVERY peer and is always rejected.
    #[serde(default)]
    pub trusted_cidrs: Vec<vrt_config::Cidr>,
    /// Upstream Cloudflare edge ranges — REQUIRED for `cloudflare`,
    /// REJECTED otherwise. Only when the XFF hop adjacent to the
    /// trusted proxy lands inside one is `cf-connecting-ip` honored.
    /// Each entry must be at least a /8 (v4) or /32 (v6) — catch-alls
    /// like `0.0.0.0/0` would believe ANY adjacent hop.
    #[serde(default)]
    pub cloudflare_cidrs: Vec<vrt_config::Cidr>,
}

/// MIG-9: trust mode for [`ClientIpConfig::mode`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClientIpMode {
    /// Never inject `cf-connecting-ip` (fail-closed default).
    #[default]
    None,
    /// Client IP = rightmost non-trusted XFF hop, from a trusted peer
    /// only. Upstream `cf-connecting-ip` is never honored.
    TrustedXff,
    /// Honor upstream `cf-connecting-ip`/`true-client-ip` only when the
    /// TCP peer is trusted AND the XFF hop adjacent to it lands inside
    /// `cloudflareCidrs` — Cloudflare-compatible semantics.
    Cloudflare,
}

impl ClientIpConfig {
    /// MIG-9: startup validation — the mode's CIDR sets must be
    /// complete and non-degenerate (fail closed, never silently
    /// trusting nothing or everything).
    pub fn validate(&self) -> Result<(), ConfigError> {
        // A catch-all in EITHER trust list would believe every peer or
        // every adjacent hop — always reject, in every mode.
        let is_default = |c: &vrt_config::Cidr| c.text() == "0.0.0.0/0" || c.text() == "::/0";
        if self.trusted_cidrs.iter().any(is_default) {
            return Err(ConfigError::Invalid(
                "ingress.clientIp.trustedCidrs must not contain 0.0.0.0/0 or ::/0",
            ));
        }
        // Cloudflare's real ranges are /13.. or narrower; a floor of
        // /8 (v4) / /32 (v6) rejects catch-alls and absurdly broad
        // lists while leaving every published CF range valid.
        for c in &self.cloudflare_cidrs {
            if is_default(c) {
                return Err(ConfigError::Invalid(
                    "ingress.clientIp.cloudflareCidrs must not contain 0.0.0.0/0 or ::/0",
                ));
            }
            // Real published CF ranges span up to /13 (v4) and /29
            // (v6, 2a06:98c0::/29). The floor only rejects catch-alls
            // and absurdly broad lists while admitting every CF range.
            if (c.is_ipv4() && c.prefix_len() < 8) || (!c.is_ipv4() && c.prefix_len() < 16) {
                return Err(ConfigError::Invalid(
                    "ingress.clientIp.cloudflareCidrs entries must be at least /8 (v4) or /16 (v6)",
                ));
            }
        }
        match self.mode {
            ClientIpMode::None => {
                if !self.trusted_cidrs.is_empty() || !self.cloudflare_cidrs.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.clientIp cidr lists only valid with trusted-xff/cloudflare",
                    ));
                }
            }
            ClientIpMode::TrustedXff => {
                if self.trusted_cidrs.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.clientIp.trustedCidrs required with trusted-xff",
                    ));
                }
                if !self.cloudflare_cidrs.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.clientIp.cloudflareCidrs only valid with cloudflare",
                    ));
                }
            }
            ClientIpMode::Cloudflare => {
                if self.trusted_cidrs.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.clientIp.trustedCidrs required with cloudflare",
                    ));
                }
                if self.cloudflare_cidrs.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.clientIp.cloudflareCidrs required with cloudflare",
                    ));
                }
            }
        }
        Ok(())
    }

    /// The validated config as the ingress-side policy.
    pub fn policy(&self) -> crate::ingress::ClientIpPolicy {
        match self.mode {
            ClientIpMode::None => crate::ingress::ClientIpPolicy::None,
            ClientIpMode::TrustedXff => crate::ingress::ClientIpPolicy::TrustedXff {
                trusted: self.trusted_cidrs.clone(),
            },
            ClientIpMode::Cloudflare => crate::ingress::ClientIpPolicy::Cloudflare {
                trusted: self.trusted_cidrs.clone(),
                cloudflare: self.cloudflare_cidrs.clone(),
            },
        }
    }
}

/// MIG-7: trusted-source strategy for the forwarded scheme — see
/// [`IngressConfig::forwarded_proto`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ForwardedProtoMode {
    /// Every request is reported `http` (fail-closed default).
    #[default]
    FixedHttp,
    /// Every request is reported `https` — only when the sole path to
    /// the ingress is TLS-terminated upstream (e.g. websecure-only
    /// Traefik Ingress) and no cleartext route can ever be added.
    FixedHttps,
    /// Honor inbound `x-forwarded-proto` only from peers inside
    /// `forwardedProtoTrusted`.
    TrustedProxyHeader,
}

fn default_req_timeout_ms() -> u64 {
    30_000
}
fn default_conn_timeout_ms() -> u64 {
    2_000
}
fn default_max_body() -> usize {
    4 * 1024 * 1024
}
fn default_switch_hold_ms() -> u64 {
    10_000
}
fn default_switch_hold_reqs() -> usize {
    256
}
fn default_switch_hold_bytes() -> usize {
    32 * 1024 * 1024
}

impl IngressConfig {
    /// RV-R72: typed range checks (static messages naming the field and
    /// the allowed range). `switchHoldMs = 0` is the documented "disable
    /// holding" value, so it is bounded above only.
    pub fn validate(&self) -> Result<(), ConfigError> {
        use vrt_securefs::{ensure_range, ensure_timeout_ms};
        let err = |m: &'static str| Err(ConfigError::Invalid(m));
        ensure_timeout_ms!(self.request_timeout_ms, "ingress.requestTimeoutMs", err);
        ensure_timeout_ms!(self.connect_timeout_ms, "ingress.connectTimeoutMs", err);
        if self.connect_timeout_ms > self.request_timeout_ms {
            return Err(ConfigError::Invalid(
                "ingress.connectTimeoutMs must be <= ingress.requestTimeoutMs",
            ));
        }
        ensure_range!(
            self.max_body_bytes,
            "ingress.maxBodyBytes",
            1,
            1_073_741_824,
            err
        );
        ensure_range!(self.switch_hold_ms, "ingress.switchHoldMs", 0, 300_000, err);
        ensure_range!(
            self.switch_hold_max_reqs,
            "ingress.switchHoldMaxReqs",
            1,
            1_000_000,
            err
        );
        ensure_range!(
            self.switch_hold_max_bytes,
            "ingress.switchHoldMaxBytes",
            1,
            17_179_869_184,
            err
        );
        match self.forwarded_proto {
            ForwardedProtoMode::TrustedProxyHeader => {
                if self.forwarded_proto_trusted.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.forwardedProtoTrusted required with trusted-proxy-header",
                    ));
                }
                // MIG-9: a catch-all trusted-proxy list would believe
                // EVERY peer's forwarded headers — always reject.
                if self
                    .forwarded_proto_trusted
                    .iter()
                    .any(|c| c.text() == "0.0.0.0/0" || c.text() == "::/0")
                {
                    return Err(ConfigError::Invalid(
                        "ingress.forwardedProtoTrusted must not contain 0.0.0.0/0 or ::/0",
                    ));
                }
            }
            ForwardedProtoMode::FixedHttp | ForwardedProtoMode::FixedHttps => {
                if !self.forwarded_proto_trusted.is_empty() {
                    return Err(ConfigError::Invalid(
                        "ingress.forwardedProtoTrusted only valid with trusted-proxy-header",
                    ));
                }
            }
        }
        self.client_ip.validate()?;
        self.websocket.validate()?;
        Ok(())
    }

    /// The validated WebSocket limits.
    pub fn websocket_limits(&self) -> WebsocketLimits {
        self.websocket.limits()
    }

    /// MIG-9: the validated client-IP policy.
    pub fn client_ip_policy(&self) -> crate::ingress::ClientIpPolicy {
        self.client_ip.policy()
    }

    /// MIG-7: the validated config as the ingress-side policy.
    pub fn forwarded_policy(&self) -> crate::ingress::ForwardedProto {
        match self.forwarded_proto {
            ForwardedProtoMode::FixedHttp => crate::ingress::ForwardedProto::FixedHttp,
            ForwardedProtoMode::FixedHttps => crate::ingress::ForwardedProto::FixedHttps,
            ForwardedProtoMode::TrustedProxyHeader => {
                crate::ingress::ForwardedProto::TrustedHeader {
                    trusted: self.forwarded_proto_trusted.clone(),
                }
            }
        }
    }
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: default_req_timeout_ms(),
            connect_timeout_ms: default_conn_timeout_ms(),
            max_body_bytes: default_max_body(),
            switch_hold_ms: default_switch_hold_ms(),
            switch_hold_max_reqs: default_switch_hold_reqs(),
            switch_hold_max_bytes: default_switch_hold_bytes(),
            forwarded_proto: ForwardedProtoMode::default(),
            forwarded_proto_trusted: Vec::new(),
            client_ip: ClientIpConfig::default(),
            websocket: WebsocketConfig::default(),
            dev_catch_all: false,
        }
    }
}

/// Config load/validation errors — constant field names only.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// File read/parse failure.
    #[error("config: {0}")]
    Parse(&'static str),
    /// Mutually exclusive / missing typed fields.
    #[error("config invalid: {0}")]
    Invalid(&'static str),
    /// No trusted keys configured.
    #[error("config trustedKeys is empty")]
    NoKeys,
    /// `workerd_binary` is not an absolute, existing file.
    #[error("config workerdBinary invalid")]
    BadBinary,
    /// A required directory failed the secure-dir check.
    #[error("config directory insecure: {0}")]
    InsecureDir(&'static str),
    /// `render.mode` unknown.
    #[error("config render.mode must be capnp|stub")]
    BadMode,
    /// Render params file missing in capnp mode.
    #[error("config render.paramsFile required in capnp mode")]
    MissingParamsFile,
    /// `secrets_file` is not `0600` (fail closed — would expose refs).
    #[error("config secretsFile must be mode 0600")]
    InsecureSecretsFile,
    /// `render.mode = "stub"` is compiled out (no `dev-stub` feature).
    /// Production builds must refuse it, not silently treat it as capnp.
    #[error("config render.mode=stub unavailable in this build (dev-stub)")]
    StubModeDisabled,
    /// `bundles.base_url` is neither https nor loopback http.
    #[error("config bundles.baseUrl must be https or loopback http")]
    InsecureBundleUrl,
    /// `bundles.token_file` missing/unreadable/not-0600/empty.
    #[error("config bundles.tokenFile unreadable or not mode 0600")]
    InsecureTokenFile,
}

/// `true` when the binary was compiled with the `dev-stub` feature —
/// exported so config validation stays a pure, testable decision.
pub const DEV_STUB_ENABLED: bool = cfg!(feature = "dev-stub");

/// `render.mode` accept/reject as a pure function (tests can force the
/// disabled side even in a dev-stub build).
pub fn check_emit_mode(mode: &str, dev_stub: bool) -> Result<(), ConfigError> {
    match mode {
        "capnp" => Ok(()),
        "stub" if dev_stub => Ok(()),
        "stub" => Err(ConfigError::StubModeDisabled),
        _ => Err(ConfigError::BadMode),
    }
}

/// MIG-12-r2 cross-field bound (called from `HostConfig::validate`):
/// a serialized DO switch parks tenant requests inside
/// `ingress.switchHoldMs` — the old generation's post-idle waitUntil
/// wait must fit STRICTLY below it so the new generation's cold
/// start still has headroom before held requests 503. A grace >= the
/// hold means the switch could never meet the hold deadline — fail
/// closed at startup. `switchHoldMs = 0` disables holding entirely,
/// so nothing needs bounding.
pub fn check_do_switch_grace_within_hold(
    do_switch_wait_until_grace_ms: u64,
    switch_hold_ms: u64,
) -> Result<(), ConfigError> {
    if switch_hold_ms > 0 && do_switch_wait_until_grace_ms >= switch_hold_ms {
        return Err(ConfigError::Invalid(
            "supervisor.doSwitchWaitUntilGraceMs must be < ingress.switchHoldMs",
        ));
    }
    Ok(())
}

impl HostConfig {
    /// Load + validate a TOML config file.
    pub fn load(path: &std::path::Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|_| ConfigError::Parse("read"))?;
        let mut cfg: Self = toml::from_str(&raw).map_err(|_| ConfigError::Parse("toml"))?;
        // T28/H2: `VRT_CELL_ID` env (k8s shared `vrt-cell` ConfigMap —
        // the single manifest source) overrides the toml value so
        // desired scoping and metrics export see ONE identity.
        if let Ok(v) = std::env::var("VRT_CELL_ID") {
            if !v.is_empty() {
                cfg.cell_id = v;
            }
        }
        // P3c-1-r1: normalize `doStorageDir` BEFORE validation —
        // create it (DO host root is host-owned, safe to mkdir) and
        // canonicalize so `..` segments / symlinks can't smuggle the
        // root past the under-`workDir` check or capnp `valid_path`
        // (which rejects non-normalized paths at emit). Fail closed.
        if let Some(dir) = cfg.do_storage_dir.clone() {
            if !dir.is_absolute() {
                return Err(ConfigError::InsecureDir("doStorageDir"));
            }
            std::fs::create_dir_all(&dir).map_err(|_| ConfigError::InsecureDir("doStorageDir"))?;
            cfg.do_storage_dir = Some(
                std::fs::canonicalize(&dir)
                    .map_err(|_| ConfigError::InsecureDir("doStorageDir"))?,
            );
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// RV-R66: startup durability posture (called by the binary only —
    /// `validate()` stays pure so example/dev configs still parse).
    /// `Some(msg)` = refuse to start: DO storage is configured but there is
    /// no object-store placement (s3), so DO/workflow state is single-copy.
    /// `allowLocalDurability = true` is the explicit dev waiver (the caller
    /// logs a warning).
    pub fn durability_posture_error(&self) -> Option<&'static str> {
        if self.allow_local_durability || self.do_storage_dir.is_none() {
            return None;
        }
        match &self.placement {
            Some(p) if p.backend == "s3" => None,
            _ => Some(
                "VRT_HOST_DURABILITY_SINGLE_COPY: doStorageDir without [placement] backend=\"s3\" \
                 (set allowLocalDurability=true for dev only)",
            ),
        }
    }

    /// Fail-closed validation.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.desired.trusted_keys.is_empty() {
            return Err(ConfigError::NoKeys);
        }
        if !self.workerd_binary.is_absolute() || !self.workerd_binary.is_file() {
            return Err(ConfigError::BadBinary);
        }
        // P3c-1: doStorageDir must be absolute and MUST NOT live under
        // `workDir` — generation dirs are swept there; DO data would
        // be deleted with them (fail closed at startup, not at write).
        if let Some(dir) = &self.do_storage_dir {
            // Compare against the canonical workDir too — a symlinked
            // or `..`-bearing workDir must not weaken the sweep-safety
            // boundary (load() already canonicalized `dir`).
            let work =
                std::fs::canonicalize(&self.work_dir).unwrap_or_else(|_| self.work_dir.clone());
            let ok = dir.is_absolute() && *dir != work && !dir.starts_with(&work);
            if !ok {
                return Err(ConfigError::Parse("doStorageDir"));
            }
        }
        // P3c-2: placement validation — object-store backends must be
        // complete (fs.root / s3 endpoint+bucket+region+secret files);
        // lease must dominate the margins so the renewal loop has a
        // real window (renew at expiry-2*margin, kill at expiry-margin).
        if let Some(p) = &self.placement {
            match p.backend.as_str() {
                "none" | "local" => {}
                "fs" => {
                    if p.fs.is_none() {
                        return Err(ConfigError::Parse("placement.fs.root required"));
                    }
                }
                "s3" => {
                    let Some(s3) = &p.s3 else {
                        return Err(ConfigError::Parse("placement.s3 required"));
                    };
                    if s3.endpoint.is_empty() || s3.bucket.is_empty() || s3.region.is_empty() {
                        return Err(ConfigError::Parse("placement.s3 endpoint/bucket/region"));
                    }
                }
                _ => {
                    return Err(ConfigError::Parse(
                        "placement.backend must be local|fs|s3|none",
                    ))
                }
            }
            if p.backend != "none" && p.backend != "local" {
                // P3c-2-r1 timeline (see sql-storage §6.9):
                //   holder kills at  expires - margin   (bounded stop)
                //   acquirer takes at expires + margin   (usable gate)
                // Safety needs  margin > kill_ub + skew  — margin is
                // BOTH the clock-skew bound and the kill budget; a
                // floor of 1s covers SIGKILL+waitpid propagation on
                // any sane host. lease > 4*margin leaves a real renew
                // window (renew_due = expires - 2*margin).
                let margin = p.clock_skew_margin_ms;
                if margin < 1_000 {
                    return Err(ConfigError::Parse(
                        "placement.clockSkewMarginMs must be >= 1000 (skew+kill budget)",
                    ));
                }
                if p.lease_ms <= margin * 4 {
                    return Err(ConfigError::Parse("placement.leaseMs too small vs margin"));
                }
                // MIG-10: startup self-check retry bounds — same shape
                // as vrt-sql [persistence] (the fields are shared on
                // PersistenceConfig). A zero/overflowing budget would
                // silently degrade to single-shot or unbounded retry.
                if p.self_check_initial_backoff_ms == 0 || p.self_check_max_backoff_ms == 0 {
                    return Err(ConfigError::Parse(
                        "placement selfCheck backoffs must be > 0",
                    ));
                }
                if p.self_check_max_backoff_ms < p.self_check_initial_backoff_ms {
                    return Err(ConfigError::Parse(
                        "placement selfCheckMaxBackoffMs < selfCheckInitialBackoffMs",
                    ));
                }
                if p.self_check_budget_ms == 0 || p.self_check_budget_ms > 600_000 {
                    return Err(ConfigError::Parse(
                        "placement.selfCheckBudgetMs out of bounds (1..=600000)",
                    ));
                }
            }
        }
        // PT1: runaway watchdog bounds — a nonsense value must fail
        // closed at startup, never produce a degenerate kill loop.
        self.limits.cpu_runaway.validate()?;
        // RV-R72: ingress numeric bounds.
        self.ingress.validate()?;
        // T13/M2: `[isolation]` bounds — uid 0 is never a tenant and
        // a zero/overflowing span could never host a tenant.
        if let Some(iso) = &self.isolation {
            if iso.base == 0 || iso.span == 0 || iso.base.checked_add(iso.span).is_none() {
                return Err(ConfigError::Parse("isolation.base/span"));
            }
        }
        check_emit_mode(&self.render.mode, DEV_STUB_ENABLED)?;
        if self.render.mode == "capnp" {
            match (&self.render.params_file, &self.render.facade_manifest) {
                (None, None) => return Err(ConfigError::MissingParamsFile),
                (Some(_), Some(_)) => {
                    return Err(ConfigError::Parse("paramsFile+facadeManifest conflict"));
                }
                (None, Some(_)) => {
                    // H5-r1: verbatim socket XOR per-tenant dir —
                    // exactly one, never "both set, pick one".
                    let socket = self
                        .render
                        .binding_backend_socket
                        .as_deref()
                        .filter(|v| !v.is_empty());
                    let dir = self.render.binding_backend_dir.as_ref();
                    match (socket, dir) {
                        (Some(_), Some(_)) => {
                            return Err(ConfigError::Parse(
                                "bindingBackendSocket+bindingBackendDir conflict",
                            ));
                        }
                        (None, None) => {
                            return Err(ConfigError::Parse(
                                "bindingBackendSocket|bindingBackendDir",
                            ));
                        }
                        (Some(_), None) => {
                            // Verbatim socket is a single-tenant
                            // contract — the platform cell mode
                            // (desired.assignmentFile) serves multiple
                            // tenants and must derive per-tenant
                            // paths; a static socket would route them
                            // all through one tenant's listener.
                            if self.desired.assignment_file.is_some() {
                                return Err(ConfigError::Parse(
                                    "bindingBackendSocket forbidden in assignment mode — use bindingBackendDir",
                                ));
                            }
                        }
                        (None, Some(d)) => {
                            if !d.is_absolute() {
                                return Err(ConfigError::InsecureDir("bindingBackendDir"));
                            }
                        }
                    }
                    if self
                        .render
                        .system_compat_date
                        .as_deref()
                        .map(|v| v.is_empty())
                        .unwrap_or(true)
                    {
                        return Err(ConfigError::Parse("systemCompatDate"));
                    }
                    // T31g/G1.4: `experimental` compat flag demands
                    // `--experimental` in workerd argv (extraArgs).
                    // Operator-facing early error — the authoritative
                    // per-tenant gate is `AssembleError::LaunchArgs`
                    // (covers paramsFile mode too).
                    if self
                        .render
                        .system_compat_flags
                        .iter()
                        .any(|f| f == "experimental")
                        && !self.render.extra_args.iter().any(|a| a == "--experimental")
                    {
                        return Err(ConfigError::Parse(
                            "systemCompatFlags experimental requires \
                             extraArgs=[\"--experimental\"]",
                        ));
                    }
                }
                (Some(_), None) => {
                    // H5-r1: a paramsFile already carries its own
                    // verbatim socket — two socket sources fail closed.
                    if self.render.binding_backend_dir.is_some() {
                        return Err(ConfigError::Parse(
                            "bindingBackendDir requires facadeManifest mode",
                        ));
                    }
                }
            }
        }
        // Directories must exist or be creatable as 0700 — full check
        // runs at startup via ensure_secure_dir; here we just refuse
        // obviously-wrong shapes (non-absolute).
        if !self.work_dir.is_absolute() {
            return Err(ConfigError::InsecureDir("workDir"));
        }
        if !self.cache_dir.is_absolute() {
            return Err(ConfigError::InsecureDir("cacheDir"));
        }
        if let Some(combined) = &self.render.render_inputs_file {
            let meta = std::fs::metadata(combined)
                .map_err(|_| ConfigError::InsecureDir("renderInputsFile"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if meta.permissions().mode() & 0o777 != 0o600 {
                    return Err(ConfigError::InsecureDir("renderInputsFile"));
                }
            }
        }
        if let Some(secrets) = &self.render.secrets_file {
            let meta = std::fs::metadata(secrets).map_err(|_| ConfigError::InsecureSecretsFile)?;
            use std::os::unix::fs::MetadataExt;
            if meta.mode() & 0o777 != 0o600 {
                return Err(ConfigError::InsecureSecretsFile);
            }
        }
        self.listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|_| ConfigError::Parse("listenAddr"))?;
        // T14/B1: assignment mode requires a pod id + a control
        // listener (the controller's probe endpoint) — both loopback
        // and 0700-dir enforced where applicable.
        if self.desired.assignment_file.is_some() {
            let Some(pod_id) = &self.desired.pod_id else {
                return Err(ConfigError::Parse("desired.podId"));
            };
            if pod_id.is_empty()
                || pod_id.len() > 128
                || !pod_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
            {
                return Err(ConfigError::Parse("desired.podId"));
            }
            let Some(control) = &self.control_listen else {
                return Err(ConfigError::Parse("controlListen"));
            };
            let addr: std::net::SocketAddr = control
                .parse()
                .map_err(|_| ConfigError::Parse("controlListen"))?;
            if !addr.ip().is_loopback() {
                // readyz/healthz are pod-internal — never bind publicly.
                return Err(ConfigError::Parse("controlListen"));
            }
        } else if let Some(control) = &self.control_listen {
            let addr: std::net::SocketAddr = control
                .parse()
                .map_err(|_| ConfigError::Parse("controlListen"))?;
            if !addr.ip().is_loopback() {
                return Err(ConfigError::Parse("controlListen"));
            }
        }
        // T27/N0: the rolling failure window must be wide enough for
        // `failThreshold` launch-failure cycles to coexist — otherwise
        // the circuit can NEVER trip and a broken tenant retries
        // forever. Required bound (spec): threshold full launch cycles
        // — readyTimeoutMs + backoffMaxMs each — must fit the window.
        self.supervisor.validate()?;
        self.scale_to_zero.validate()?;
        self.geoip.validate()?;
        self.analytics_engine.validate()?;
        {
            let t = self.supervisor.fail_threshold.max(1) as u64;
            let cycle = self
                .supervisor
                .ready_timeout_ms
                .saturating_add(self.supervisor.backoff_max_ms);
            let need = t.saturating_mul(cycle);
            if need > self.supervisor.fail_window_ms {
                return Err(ConfigError::Invalid(
                    "supervisor circuit bounds: require failThreshold*(readyTimeoutMs+backoffMaxMs) <= failWindowMs",
                ));
            }
        }
        // MIG-12-r2: the do_switch waitUntil grace lives inside the
        // switch-hold window — bound it against `ingress.switchHoldMs`.
        check_do_switch_grace_within_hold(
            self.supervisor.do_switch_wait_until_grace_ms,
            self.ingress.switch_hold_ms,
        )?;
        if let Some(m) = &self.metrics_listen {
            m.parse::<std::net::SocketAddr>()
                .map_err(|_| ConfigError::Parse("metricsListen"))?;
        }
        // T28/H2: cell_id also becomes a Prom label value — bound it
        // so a hostile config cannot corrupt exposition.
        if self.cell_id.is_empty() || self.cell_id.len() > 256 || self.cell_id.contains('\n') {
            return Err(ConfigError::Invalid("cell_id"));
        }
        if let Some(b) = &self.bundles {
            if let Some(dir) = &b.dir {
                // T17: local dir source (standalone). Exactly one of
                // dir|http may be configured — two sources of truth
                // fail closed.
                if b.base_url.is_some() || b.token_file.is_some() {
                    return Err(ConfigError::Invalid("bundles.dir+http"));
                }
                if !dir.is_absolute() {
                    return Err(ConfigError::Invalid("bundles.dir"));
                }
            } else {
                // https required; loopback http only for dev/test.
                let url = b
                    .base_url
                    .as_ref()
                    .ok_or(ConfigError::Invalid("bundles.baseUrl"))?
                    .trim_end_matches('/');
                let https = url.starts_with("https://");
                let loop_http = url.starts_with("http://")
                    && ["127.0.0.1", "[::1]", "localhost"]
                        .iter()
                        .any(|h| url[7..].starts_with(h));
                if !(https || loop_http) {
                    return Err(ConfigError::InsecureBundleUrl);
                }
                if b.token_file.is_none() {
                    return Err(ConfigError::Invalid("bundles.tokenFile"));
                }
                if b.max_attempts == 0 || b.max_attempts > 8 {
                    return Err(ConfigError::Parse("bundles.maxAttempts"));
                }
                if b.max_bytes == 0 || b.max_bytes > 64 * 1024 * 1024 {
                    return Err(ConfigError::Parse("bundles.maxBytes"));
                }
                if b.request_timeout_ms == 0 || b.request_timeout_ms > b.deadline_ms {
                    return Err(ConfigError::Parse("bundles.requestTimeoutMs"));
                }
            }
        }
        Ok(())
    }

    /// Resolve `EmitMode` + render params (capnp mode reads the params /
    /// vars / secrets files). Secrets are resolved here and flow only
    /// into `RuntimeParams.secrets` → `RenderedConfig.env` → child env.
    pub fn emit_mode(&self) -> Result<crate::assemble::EmitMode, ConfigError> {
        match self.render.mode.as_str() {
            "stub" => {
                #[cfg(feature = "dev-stub")]
                {
                    Ok(crate::assemble::EmitMode::StubJson)
                }
                #[cfg(not(feature = "dev-stub"))]
                {
                    Err(ConfigError::StubModeDisabled)
                }
            }
            "capnp" => {
                let file: RenderParamsFile =
                    match (&self.render.params_file, &self.render.facade_manifest) {
                        (Some(params_path), _) => {
                            let raw = std::fs::read_to_string(params_path)
                                .map_err(|_| ConfigError::Parse("paramsFile"))?;
                            serde_json::from_str(&raw)
                                .map_err(|_| ConfigError::Parse("paramsFile json"))?
                        }
                        (None, Some(manifest_path)) => {
                            // H5-r1: dir mode transports the listener
                            // dir through the same params channel —
                            // assemble rewrites it per tenant via
                            // `vrt_securefs::socket_path` (the doc's
                            // `/`-prefix+printable bound holds for a
                            // dir path too; non-utf8 fails closed).
                            let socket_or_dir = self
                                .render
                                .binding_backend_socket
                                .as_deref()
                                .filter(|v| !v.is_empty())
                                .map(str::to_string)
                                .or_else(|| {
                                    self.render
                                        .binding_backend_dir
                                        .as_ref()
                                        .and_then(|d| d.to_str().map(str::to_string))
                                })
                                .unwrap_or_default();
                            let value = params_json_from_manifest(
                                manifest_path,
                                &socket_or_dir,
                                self.render.system_compat_date.as_deref().unwrap_or(""),
                                &self.render.system_compat_flags,
                                self.render.egress_policy_file.as_ref(),
                            )?;
                            serde_json::from_value(value)
                                .map_err(|_| ConfigError::Parse("derived params"))?
                        }
                        (None, None) => return Err(ConfigError::MissingParamsFile),
                    };
                // T17/F1: vars/secrets are NO LONGER read here — the
                // `RenderInputsProvider` (see `render_inputs_provider`)
                // re-loads them on every converge so rotations take
                // effect without a restart. Params carry empty maps;
                // assemble substitutes the fresh load.
                Ok(crate::assemble::EmitMode::Capnp {
                    params: file.into_params(Default::default(), Default::default())?,
                })
            }
            _ => Err(ConfigError::BadMode),
        }
    }
}

impl HostConfig {
    /// T28/H2: the cell identity this process exports on every
    /// series as the `cell_id` const label — already env-resolved at
    /// `load` (`VRT_CELL_ID` > toml), so this is the single value.
    pub fn effective_cell_id(&self) -> &str {
        &self.cell_id
    }

    /// T17/F1: the hot render-inputs provider wired into the
    /// assembler. Same `varsFile`/`secretsFile` config fields, but
    /// re-read (with `0600` re-verification) on EVERY converge — a
    /// `chmod`/delete regression turns into `render-inputs-unavailable`
    /// instead of silently serving stale secrets.
    pub fn render_inputs_provider(
        &self,
    ) -> std::sync::Arc<dyn crate::render_inputs::RenderInputsProvider> {
        if let Some(combined) = &self.render.render_inputs_file {
            return std::sync::Arc::new(crate::render_inputs::FileRenderInputs::new_combined(
                combined.clone(),
            ));
        }
        std::sync::Arc::new(crate::render_inputs::FileRenderInputs::new(
            self.render.vars_file.clone(),
            self.render.secrets_file.clone(),
        ))
    }
}

/// Static render params JSON (capnp mode). vrt-config's param types
/// carry no serde impls, so the file uses local serde mirrors and
/// converts — the JSON shape matches `compute.desired` camelCase.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenderParamsFile {
    binding_backend_socket: String,
    egress: ServiceEndpointToml,
    /// T6 egress policy; absent → `EgressPolicy::default()` (the
    /// restrictive 80/443 set — still fail closed).
    #[serde(default)]
    egress_policy: Option<EgressPolicyToml>,
    /// Deny-all `globalOutbound` target for non-egress system workers
    /// (T6 review). REQUIRED: absent ⇒ parse failure ⇒ no render —
    /// facades must never fall back to `vrt-egress` or workerd's
    /// `internet` default.
    deny_outbound: ServiceEndpointToml,
    facades: FacadeRefsToml,
    system_workers: Vec<SystemWorkerToml>,
    system_compat_date: String,
    #[serde(default)]
    system_compat_flags: Vec<String>,
}

impl RenderParamsFile {
    /// Convert into typed render params (validation lives in the
    /// `TryFrom`s — a malformed derived document fails closed here).
    fn into_params(
        self,
        vars: std::collections::BTreeMap<String, String>,
        secrets: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, vrt_config::SecretValue>,
        >,
    ) -> Result<RuntimeParams, ConfigError> {
        Ok(RuntimeParams {
            socket_dir: String::new(),       // per-generation
            do_storage_dir: String::new(),   // per-tenant (assemble fills)
            wfop_pepper: String::new(),      // per-process (assemble fills)
            wf_delete_addr: String::new(),   // per-process (assemble fills)
            cache_addr: String::new(),       // per-tenant (assemble fills)
            ae_addr: String::new(),          // per-tenant (assemble fills)
            heartbeat_socket: String::new(), // per-tenant (assemble fills)
            waituntil_socket: String::new(), // per-tenant (assemble fills)
            log_tail: vrt_config::LogTailParams::default(),
            binding_backend_socket: self.binding_backend_socket,
            egress: self.egress.into(),
            egress_policy: self
                .egress_policy
                .map(vrt_config::EgressPolicy::try_from)
                .transpose()?
                .unwrap_or_default(),
            deny_outbound: self.deny_outbound.into(),
            facades: self.facades.into(),
            system_workers: self.system_workers.into_iter().map(Into::into).collect(),
            system_compat_date: self.system_compat_date,
            system_compat_flags: self.system_compat_flags,
            vars,
            secrets,
        })
    }
}

/// T16/B3: derive the params-file JSON document from a validated
/// `facade/manifest.json` plus operator inputs. The output round-trips
/// through [`RenderParamsFile`] (callers re-parse it), so the derived
/// document can never carry a shape the static file path would reject.
pub fn params_json_from_manifest(
    manifest_path: &std::path::Path,
    binding_backend_socket: &str,
    system_compat_date: &str,
    system_compat_flags: &[String],
    egress_policy_file: Option<&PathBuf>,
) -> Result<serde_json::Value, ConfigError> {
    let raw =
        std::fs::read_to_string(manifest_path).map_err(|_| ConfigError::Parse("facadeManifest"))?;
    // Typed boundary in vrt-config: wrong schemaVersion, missing
    // workers, bad digests or non-member mainModule all fail closed.
    let manifest = vrt_config::parse_facade_manifest(&raw)
        .map_err(|_| ConfigError::Parse("facadeManifest schema"))?;

    // Unix socket path: absolute, printable, within the sockaddr_un
    // sun_path bound (104 is the portable minimum — macOS).
    if binding_backend_socket.len() > 104
        || !binding_backend_socket.starts_with('/')
        || !binding_backend_socket
            .bytes()
            .all(|b| (0x21..=0x7e).contains(&b))
    {
        return Err(ConfigError::Parse("bindingBackendSocket"));
    }
    if !vrt_config::valid_compat_date(system_compat_date)
        || system_compat_flags
            .iter()
            .any(|f| !vrt_config::valid_compat_flag(f))
    {
        return Err(ConfigError::Parse("systemCompat"));
    }

    let endpoint = |e: &vrt_config::ServiceEndpoint| serde_json::json!({"service": e.service, "entrypoint": e.entrypoint});
    let egress_policy = match egress_policy_file {
        None => serde_json::Value::Null,
        Some(path) => {
            let raw = std::fs::read_to_string(path)
                .map_err(|_| ConfigError::Parse("egressPolicyFile"))?;
            serde_json::from_str(&raw).map_err(|_| ConfigError::Parse("egressPolicyFile json"))?
        }
    };
    let mut doc = serde_json::json!({
        "bindingBackendSocket": binding_backend_socket,
        "egress": endpoint(&manifest.egress),
        "denyOutbound": endpoint(&manifest.deny_outbound),
        "facades": {
            "kv": endpoint(&manifest.facades.kv),
            "r2": endpoint(&manifest.facades.r2),
            "assets": endpoint(&manifest.facades.assets),
            "pg": endpoint(&manifest.facades.pg),
            "d1": endpoint(&manifest.facades.d1),
            // T37: facade-queue endpoint (manifest REQUIRED_FACADES
            // guarantees presence — an old manifest without it fails
            // closed at parse_facade_manifest).
            "queue": endpoint(&manifest.facades.queue),
            // K2: facade-k2 endpoint (REQUIRED_FACADES).
            "k2": endpoint(&manifest.facades.k2),
            // MIG-16: facade-ratelimit endpoint (REQUIRED_FACADES).
            "ratelimit": endpoint(&manifest.facades.ratelimit),
            // send_email: facade-email endpoint (REQUIRED_FACADES).
            "email": endpoint(&manifest.facades.email),
        },
        "systemWorkers": manifest.system_workers.iter().map(|w| {
            serde_json::json!({
                "serviceName": w.service_name,
                "mainModule": w.main_module,
                "modules": w.modules.iter().map(|m| {
                    serde_json::json!({"name": m.name, "digest": m.digest})
                }).collect::<Vec<_>>(),
                "needsBindingBackend": w.needs_binding_backend,
            })
        }).collect::<Vec<_>>(),
        "systemCompatDate": system_compat_date,
        "systemCompatFlags": system_compat_flags,
    });
    if !egress_policy.is_null() {
        doc["egressPolicy"] = egress_policy;
    }
    // Self-check: the derived document must satisfy the same typed
    // schema + conversions the static file path enforces.
    let file: RenderParamsFile = serde_json::from_value(doc.clone())
        .map_err(|_| ConfigError::Parse("derived params schema"))?;
    file.into_params(Default::default(), Default::default())?;
    Ok(doc)
}

/// Serde mirror of `vrt_config::ServiceEndpoint`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceEndpointToml {
    service: String,
    #[serde(default)]
    entrypoint: Option<String>,
}

impl From<ServiceEndpointToml> for vrt_config::ServiceEndpoint {
    fn from(v: ServiceEndpointToml) -> Self {
        Self {
            service: v.service,
            entrypoint: v.entrypoint.unwrap_or_default(),
        }
    }
}

/// Serde mirror of `vrt_config::EgressPolicy`. Every field defaults to
/// the restrictive value; validation runs later in
/// `EgressPolicy::validate` during render. MIG-14: the TCP knobs are
/// operator-configurable; `internalTcpAllow` deliberately has NO serde
/// field — `deny_unknown_fields` then makes it unrepresentable in any
/// operator/params file (only platform Rust code can inject internal
/// exceptions).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EgressPolicyToml {
    #[serde(default = "default_allowed_ports")]
    allowed_ports: Vec<u16>,
    #[serde(default)]
    allow: Vec<EgressAllowEntryToml>,
    #[serde(default)]
    deny_names: Vec<String>,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u32,
    #[serde(default = "default_tcp_allowed_ports")]
    tcp_allowed_ports: Vec<u16>,
    #[serde(default = "default_tcp_connect_timeout_ms")]
    tcp_connect_timeout_ms: u32,
    #[serde(default = "default_tcp_idle_timeout_ms")]
    tcp_idle_timeout_ms: u32,
    #[serde(default = "default_tcp_max_concurrent")]
    tcp_max_concurrent: u32,
}

fn default_allowed_ports() -> Vec<u16> {
    vec![80, 443]
}
fn default_request_timeout_ms() -> u32 {
    vrt_config::egress::DEFAULT_REQUEST_TIMEOUT_MS
}
fn default_tcp_allowed_ports() -> Vec<u16> {
    vec![43]
}
fn default_tcp_connect_timeout_ms() -> u32 {
    vrt_config::egress::DEFAULT_TCP_CONNECT_TIMEOUT_MS
}
fn default_tcp_idle_timeout_ms() -> u32 {
    vrt_config::egress::DEFAULT_TCP_IDLE_TIMEOUT_MS
}
fn default_tcp_max_concurrent() -> u32 {
    vrt_config::egress::DEFAULT_TCP_MAX_CONCURRENT
}

/// Serde mirror of `vrt_config::EgressAllowEntry`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EgressAllowEntryToml {
    host: String,
    port: u16,
}

impl TryFrom<EgressPolicyToml> for vrt_config::EgressPolicy {
    type Error = ConfigError;

    fn try_from(v: EgressPolicyToml) -> Result<Self, ConfigError> {
        // A malformed `*.`/empty deny name must fail closed, not vanish.
        let deny_names = v
            .deny_names
            .iter()
            .map(|n| {
                vrt_config::EgressNameRule::parse(n)
                    .ok_or(ConfigError::Parse("paramsFile egressPolicy"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            allowed_ports: v.allowed_ports,
            allow: v
                .allow
                .into_iter()
                .map(|e| vrt_config::EgressAllowEntry {
                    host: e.host,
                    port: e.port,
                })
                .collect(),
            deny_names,
            request_timeout_ms: v.request_timeout_ms,
            tcp_allowed_ports: v.tcp_allowed_ports,
            internal_tcp_allow: Vec::new(),
            // MIG-15: Hyperdrive synthetic targets are minted only by
            // the control plane; a local paramsFile never carries them.
            internal_tcp_targets: Vec::new(),
            tcp_connect_timeout_ms: v.tcp_connect_timeout_ms,
            tcp_idle_timeout_ms: v.tcp_idle_timeout_ms,
            tcp_max_concurrent: v.tcp_max_concurrent,
        })
    }
}

/// Serde mirror of `vrt_config::FacadeRefs`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FacadeRefsToml {
    kv: ServiceEndpointToml,
    r2: ServiceEndpointToml,
    assets: ServiceEndpointToml,
    pg: ServiceEndpointToml,
    d1: ServiceEndpointToml,
    queue: ServiceEndpointToml,
    k2: ServiceEndpointToml,
    ratelimit: ServiceEndpointToml,
    email: ServiceEndpointToml,
}

impl From<FacadeRefsToml> for vrt_config::FacadeRefs {
    fn from(v: FacadeRefsToml) -> Self {
        Self {
            kv: v.kv.into(),
            r2: v.r2.into(),
            assets: v.assets.into(),
            pg: v.pg.into(),
            d1: v.d1.into(),
            queue: v.queue.into(),
            k2: v.k2.into(),
            ratelimit: v.ratelimit.into(),
            email: v.email.into(),
        }
    }
}

/// Serde mirror of `vrt_config::SystemWorkerRef` + `SystemModuleRef`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SystemWorkerToml {
    service_name: String,
    main_module: String,
    modules: Vec<SystemModuleToml>,
    #[serde(default)]
    needs_binding_backend: bool,
}

impl From<SystemWorkerToml> for vrt_config::SystemWorkerRef {
    fn from(v: SystemWorkerToml) -> Self {
        Self {
            service_name: v.service_name,
            main_module: v.main_module,
            modules: v.modules.into_iter().map(Into::into).collect(),
            needs_binding_backend: v.needs_binding_backend,
        }
    }
}

/// Serde mirror of `vrt_config::SystemModuleRef`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SystemModuleToml {
    name: String,
    digest: String,
}

impl From<SystemModuleToml> for vrt_config::SystemModuleRef {
    fn from(v: SystemModuleToml) -> Self {
        Self {
            name: v.name,
            digest: v.digest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scale-to-zero P0: default off, off builds nothing, enabled
    /// validates fail-closed and builds the documented defaults.
    #[test]
    fn scale_to_zero_default_off_and_fail_closed() {
        let d = ScaleToZeroConfig::default();
        assert!(!d.enabled);
        d.validate().unwrap();
        assert!(d.build().is_none(), "off must build no runtime object");
        // Off ignores out-of-range numbers (byte-identical behaviour).
        ScaleToZeroConfig {
            idle_ms: 1,
            ..Default::default()
        }
        .validate()
        .unwrap();

        let on = ScaleToZeroConfig {
            enabled: true,
            ..Default::default()
        };
        on.validate().unwrap();
        let s = on.build().expect("enabled builds");
        assert_eq!(s.drain_timeout, Duration::from_secs(30));
        assert_eq!(s.wake_deadline, Duration::from_secs(30));
        assert_eq!(on.idle_ms, 600_000);
        assert_eq!(on.min_residency_ms, 120_000);
        assert_eq!(on.max_concurrent_wakes, 4);

        let bad = [
            ScaleToZeroConfig {
                enabled: true,
                idle_ms: 999,
                ..Default::default()
            },
            ScaleToZeroConfig {
                enabled: true,
                max_concurrent_wakes: 0,
                ..Default::default()
            },
            ScaleToZeroConfig {
                enabled: true,
                drain_timeout_ms: 99,
                ..Default::default()
            },
            ScaleToZeroConfig {
                enabled: true,
                wake_deadline_ms: 999,
                ..Default::default()
            },
            ScaleToZeroConfig {
                enabled: true,
                max_wake_queue: 100_001,
                ..Default::default()
            },
        ];
        for c in bad {
            assert!(c.validate().is_err(), "{c:?} must be rejected");
        }
    }

    #[test]
    fn scale_to_zero_toml_parse_and_unknown_field_rejected() {
        let c: ScaleToZeroConfig =
            toml::from_str("enabled = true\nidleMs = 5000\nmaxConcurrentWakes = 2").unwrap();
        assert!(c.enabled && c.idle_ms == 5000 && c.max_concurrent_wakes == 2);
        assert!(toml::from_str::<ScaleToZeroConfig>("bogus = 1").is_err());
    }

    #[test]
    fn analytics_engine_default_on_and_bounds_validated() {
        // Absent section ⇒ enabled with defaults (the cell advertises
        // the capability unconditionally — see HostConfig field doc).
        let d = AnalyticsEngineConfig::enabled_default();
        d.validate().unwrap();
        let l = d.limits();
        assert_eq!(l.flush_interval, Duration::from_secs(60));

        // Zero bound ⇒ a fake feature that drops every
        // event — rejected at startup.
        for c in [
            AnalyticsEngineConfig {
                max_body_bytes: 0,
                ..d.clone()
            },
            AnalyticsEngineConfig {
                max_spool_bytes: 0,
                ..d.clone()
            },
        ] {
            assert!(c.validate().is_err(), "{c:?} must be rejected");
        }

        // Flush cadence clamps, not rejects (operator typo ≠ fatal).
        let hi = AnalyticsEngineConfig {
            flush_interval_secs: 99_999,
            ..d.clone()
        }
        .limits();
        assert_eq!(hi.flush_interval, Duration::from_secs(3600));
        let lo = AnalyticsEngineConfig {
            flush_interval_secs: 1,
            ..d
        }
        .limits();
        assert_eq!(lo.flush_interval, Duration::from_secs(10));
    }

    /// RV-R72: ingress numeric ranges — defaults pass, each out-of-range
    /// value is rejected with a message naming the field.
    #[test]
    fn ingress_ranges_rv_r72() {
        IngressConfig::default().validate().unwrap();
        let bad: Vec<(&str, IngressConfig)> = vec![
            (
                "requestTimeoutMs",
                IngressConfig {
                    request_timeout_ms: 0,
                    ..Default::default()
                },
            ),
            (
                "requestTimeoutMs",
                IngressConfig {
                    request_timeout_ms: 300_001,
                    ..Default::default()
                },
            ),
            (
                "connectTimeoutMs",
                IngressConfig {
                    connect_timeout_ms: 99,
                    ..Default::default()
                },
            ),
            (
                "connectTimeoutMs",
                IngressConfig {
                    request_timeout_ms: 1_000,
                    connect_timeout_ms: 2_000,
                    ..Default::default()
                },
            ),
            (
                "maxBodyBytes",
                IngressConfig {
                    max_body_bytes: 0,
                    ..Default::default()
                },
            ),
            (
                "switchHoldMs",
                IngressConfig {
                    switch_hold_ms: 300_001,
                    ..Default::default()
                },
            ),
            (
                "switchHoldMaxReqs",
                IngressConfig {
                    switch_hold_max_reqs: 0,
                    ..Default::default()
                },
            ),
        ];
        for (field, cfg) in bad {
            let e = cfg.validate().unwrap_err().to_string();
            assert!(e.contains(field), "{field}: {e}");
        }
        // switchHoldMs = 0 is the documented "disable holding" value.
        IngressConfig {
            switch_hold_ms: 0,
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    /// `[ingress.websocket]`: optional section, range checks fail
    /// closed, old configs without it still parse.
    #[test]
    fn ingress_websocket_section() {
        let d: IngressConfig = toml::from_str("requestTimeoutMs = 5000\n").unwrap();
        d.validate().unwrap();
        let l = d.websocket_limits();
        assert_eq!(l.idle_timeout, Duration::from_secs(300));
        assert_eq!(l.max_duration, Duration::from_secs(4 * 3600));
        assert_eq!(l.max_sessions, 4096);
        assert_eq!(l.max_sessions_per_tenant, 256);

        let p: IngressConfig = toml::from_str(
            "[websocket]\nidleTimeoutSecs = 30\nmaxDurationSecs = 600\nmaxSessions = 100\nmaxSessionsPerTenant = 10\n",
        )
        .unwrap();
        p.validate().unwrap();
        let l = p.websocket_limits();
        assert_eq!(l.idle_timeout, Duration::from_secs(30));
        assert_eq!(l.max_duration, Duration::from_secs(600));
        assert_eq!((l.max_sessions, l.max_sessions_per_tenant), (100, 10));

        // Per-tenant default never exceeds a smaller global cap.
        let small: IngressConfig = toml::from_str("[websocket]\nmaxSessions = 8\n").unwrap();
        small.validate().unwrap();
        assert_eq!(small.websocket_limits().max_sessions_per_tenant, 8);

        // Unknown keys rejected.
        assert!(toml::from_str::<IngressConfig>("[websocket]\nbogus = 1\n").is_err());

        // Out-of-range values are errors, not clamps.
        for (field, body) in [
            ("idleTimeoutSecs", "idleTimeoutSecs = 0"),
            ("idleTimeoutSecs", "idleTimeoutSecs = 3601"),
            ("maxDurationSecs", "maxDurationSecs = 0"),
            ("maxDurationSecs", "maxDurationSecs = 86401"),
            ("maxSessions", "maxSessions = 0"),
            ("maxSessions", "maxSessions = 65537"),
            ("maxSessionsPerTenant", "maxSessionsPerTenant = 0"),
            ("maxSessionsPerTenant", "maxSessionsPerTenant = 4097"),
            (
                "maxSessionsPerTenant",
                "maxSessions = 10\nmaxSessionsPerTenant = 11",
            ),
        ] {
            let cfg: IngressConfig = toml::from_str(&format!("[websocket]\n{body}\n")).unwrap();
            let e = cfg.validate().unwrap_err().to_string();
            assert!(e.contains(field), "{body}: {e}");
        }
        // Boundaries are accepted.
        let edge: IngressConfig = toml::from_str(
            "[websocket]\nidleTimeoutSecs = 3600\nmaxDurationSecs = 86400\nmaxSessions = 65536\nmaxSessionsPerTenant = 65536\n",
        )
        .unwrap();
        edge.validate().unwrap();
    }

    /// MIG-7: forwarded-proto strategy validation — the trusted-peer
    /// list is REQUIRED with `trusted-proxy-header` and REJECTED
    /// otherwise (ambiguous config fails startup, never silently
    /// trusts an empty or ignored set).
    #[test]
    fn ingress_forwarded_proto_strategy() {
        // default = fixed-http, no list → ok, FixedHttp policy.
        let d = IngressConfig::default();
        d.validate().unwrap();
        assert!(matches!(
            d.forwarded_policy(),
            crate::ingress::ForwardedProto::FixedHttp
        ));

        let cidr = vrt_config::Cidr::parse("10.42.0.0/16").unwrap();
        // trusted-proxy-header without a range → reject.
        let e = IngressConfig {
            forwarded_proto: ForwardedProtoMode::TrustedProxyHeader,
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(e.contains("forwardedProtoTrusted"), "{e}");
        // fixed-* with a stray range → reject (fail closed, not ignored).
        for mode in [
            ForwardedProtoMode::FixedHttp,
            ForwardedProtoMode::FixedHttps,
        ] {
            let e = IngressConfig {
                forwarded_proto: mode,
                forwarded_proto_trusted: vec![cidr],
                ..Default::default()
            }
            .validate()
            .unwrap_err()
            .to_string();
            assert!(e.contains("forwardedProtoTrusted"), "{e}");
        }
        // trusted-proxy-header with a range → ok, carries the ranges.
        let cfg = IngressConfig {
            forwarded_proto: ForwardedProtoMode::TrustedProxyHeader,
            forwarded_proto_trusted: vec![cidr],
            ..Default::default()
        };
        cfg.validate().unwrap();
        match cfg.forwarded_policy() {
            crate::ingress::ForwardedProto::TrustedHeader { trusted } => {
                assert_eq!(trusted.len(), 1);
            }
            _ => panic!("expected TrustedHeader"),
        }
        // kebab-case TOML spelling is the config contract.
        let parsed: IngressConfig = toml::from_str(
            "forwardedProto = \"trusted-proxy-header\"\nforwardedProtoTrusted = [\"10.42.0.0/16\"]\n",
        )
        .unwrap();
        parsed.validate().unwrap();
    }

    /// MIG-9: `[ingress.clientIp]` — mode/CIDR cross-validation and the
    /// catch-all guards (a degenerate trust list must fail startup,
    /// never silently trust everything).
    #[test]
    fn ingress_client_ip_strategy() {
        // default = none → ok, ClientIpPolicy::None.
        let d = IngressConfig::default();
        d.validate().unwrap();
        assert!(matches!(
            d.client_ip_policy(),
            crate::ingress::ClientIpPolicy::None
        ));

        let proxy = vrt_config::Cidr::parse("10.42.0.0/16").unwrap();
        let cf = vrt_config::Cidr::parse("172.64.0.0/13").unwrap();
        let cf6 = vrt_config::Cidr::parse("2400:cb00::/32").unwrap();

        // mode=none with stray lists → reject (fail closed, not ignored).
        let e = ClientIpConfig {
            mode: ClientIpMode::None,
            trusted_cidrs: vec![proxy],
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(e.contains("clientIp"), "{e}");

        // trusted-xff without a trusted set → reject; with a CF list → reject.
        let e = ClientIpConfig {
            mode: ClientIpMode::TrustedXff,
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(e.contains("trustedCidrs"), "{e}");
        let e = ClientIpConfig {
            mode: ClientIpMode::TrustedXff,
            trusted_cidrs: vec![proxy],
            cloudflare_cidrs: vec![cf],
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(e.contains("cloudflareCidrs"), "{e}");

        // cloudflare needs BOTH lists.
        let e = ClientIpConfig {
            mode: ClientIpMode::Cloudflare,
            trusted_cidrs: vec![proxy],
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(e.contains("cloudflareCidrs"), "{e}");

        // Catch-all trusted proxies are always rejected — even when the
        // mode legitimately requires a list.
        for cidr in ["0.0.0.0/0", "::/0"] {
            let e = ClientIpConfig {
                mode: ClientIpMode::TrustedXff,
                trusted_cidrs: vec![vrt_config::Cidr::parse(cidr).unwrap()],
                ..Default::default()
            }
            .validate()
            .unwrap_err()
            .to_string();
            assert!(e.contains("trustedCidrs"), "{cidr}: {e}");
        }
        // Overly broad or catch-all CF lists rejected (v4 < /8, v6 < /16;
        // real CF ranges reach /13 v4 and /29 v6, both still admitted).
        for cidr in ["0.0.0.0/0", "10.0.0.0/7", "::/0", "2000::/15"] {
            let e = ClientIpConfig {
                mode: ClientIpMode::Cloudflare,
                trusted_cidrs: vec![proxy],
                cloudflare_cidrs: vec![vrt_config::Cidr::parse(cidr).unwrap()],
            }
            .validate()
            .unwrap_err()
            .to_string();
            assert!(e.contains("cloudflareCidrs"), "{cidr}: {e}");
        }
        // And the same guard on forwardedProtoTrusted.
        let e = IngressConfig {
            forwarded_proto: ForwardedProtoMode::TrustedProxyHeader,
            forwarded_proto_trusted: vec![vrt_config::Cidr::parse("0.0.0.0/0").unwrap()],
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(e.contains("forwardedProtoTrusted"), "{e}");

        // A complete cloudflare config validates and maps to the policy.
        let cfg = ClientIpConfig {
            mode: ClientIpMode::Cloudflare,
            trusted_cidrs: vec![proxy],
            cloudflare_cidrs: vec![cf, cf6],
        };
        cfg.validate().unwrap();
        match cfg.policy() {
            crate::ingress::ClientIpPolicy::Cloudflare {
                trusted,
                cloudflare,
            } => {
                assert_eq!(trusted.len(), 1);
                assert_eq!(cloudflare.len(), 2);
            }
            _ => panic!("expected Cloudflare"),
        }
        // kebab-case TOML contract.
        let parsed: IngressConfig = toml::from_str(
            "[clientIp]\nmode = \"cloudflare\"\ntrustedCidrs = [\"10.42.0.0/16\"]\ncloudflareCidrs = [\"172.64.0.0/13\", \"2400:cb00::/32\"]\n",
        )
        .unwrap();
        parsed.validate().unwrap();
    }

    /// `render.mode = "stub"` is a build-time decision: refused whenever
    /// the `dev-stub` feature is off — asserted via the pure gate so this
    /// runs identically in dev-stub test builds and production builds.
    /// Runtime verification of the production binary: a default
    /// `cargo build -p vrt-host` exits with
    /// `config render.mode=stub unavailable in this build (dev-stub)`.
    #[test]
    fn stub_mode_gate() {
        assert!(check_emit_mode("capnp", false).is_ok());
        assert!(check_emit_mode("stub", true).is_ok());
        assert!(matches!(
            check_emit_mode("stub", false),
            Err(ConfigError::StubModeDisabled)
        ));
        assert!(matches!(
            check_emit_mode("bogus", true),
            Err(ConfigError::BadMode)
        ));
        assert!(matches!(
            check_emit_mode("bogus", false),
            Err(ConfigError::BadMode)
        ));
        // The exported flag reflects this build — in `cargo test` the
        // dev-dependency forces `dev-stub` on, so this is `true` here;
        // a production `cargo build` leaves it `false`.
        assert_eq!(DEV_STUB_ENABLED, cfg!(feature = "dev-stub"));
    }

    // ---- PT1: cpuRunaway config surface ----

    /// `toml_body` IS the LimitsConfig document (its `cpuRunaway`
    /// table sits at top level: `[cpuRunaway]`).
    fn limits(toml_body: &str) -> LimitsConfig {
        toml::from_str(toml_body).unwrap_or_else(|e| panic!("limits toml must parse: {e}"))
    }

    /// Upgrade compatibility: a pre-PT1 config has NO cpuRunaway key —
    /// it must load with the watchdog ON at conservative defaults
    /// (fail closed: a spinning tenant must not pin a core forever).
    #[test]
    fn cpu_runaway_absent_defaults_on() {
        let l = limits("maxRssBytes = 1073741824\n");
        let spec = l.cpu_runaway.spec().expect("absent field must default ON");
        assert_eq!(spec.window, Duration::from_secs(15));
        assert!((spec.min_cpu_ratio - 0.9).abs() < 1e-9);
        assert_eq!(spec.heartbeat_timeout, Duration::from_millis(250));
        assert_eq!(spec.heartbeat_failures, 2);
        l.cpu_runaway.validate().unwrap();
    }

    /// `cpuRunaway = {}` is a valid all-defaults statement;
    /// `enabled=false` resolves to `None` (watchdog off, no heartbeat
    /// socket rendered — the emit gate keys on `limits.runaway`).
    #[test]
    fn cpu_runaway_empty_and_disabled() {
        let l = limits("[cpuRunaway]\n");
        assert!(l.cpu_runaway.spec().is_some());
        let l = limits("[cpuRunaway]\nenabled = false\n");
        assert!(l.cpu_runaway.spec().is_none(), "disabled must resolve None");
        l.cpu_runaway.validate().unwrap(); // disabled skips range checks
    }

    /// Partial override keeps other defaults.
    #[test]
    fn cpu_runaway_partial_override() {
        let l = limits("[cpuRunaway]\nwindowMs = 5000\nminCpuMilli = 750\n");
        let spec = l.cpu_runaway.spec().unwrap();
        assert_eq!(spec.window, Duration::from_secs(5));
        assert!((spec.min_cpu_ratio - 0.75).abs() < 1e-9);
        assert_eq!(spec.heartbeat_timeout, Duration::from_millis(250));
        assert_eq!(spec.heartbeat_failures, 2);
        l.cpu_runaway.validate().unwrap();
    }

    /// Unknown keys inside cpuRunaway are rejected, not ignored.
    #[test]
    fn cpu_runaway_deny_unknown() {
        let r: Result<LimitsConfig, _> = toml::from_str("[cpuRunaway]\nwindow_ms = 5000\n");
        assert!(r.is_err(), "snake_case key must be rejected");
        let r: Result<LimitsConfig, _> = toml::from_str("[cpuRunaway]\nbogusKnob = 1\n");
        assert!(r.is_err(), "unknown key must be rejected");
    }

    /// Every bound is enforced at validate() — disabled skips them.
    #[test]
    fn cpu_runaway_bounds_enforced() {
        for (k, v) in [
            ("windowMs", "1999"),
            ("windowMs", "600001"),
            ("minCpuMilli", "0"),
            ("minCpuMilli", "64001"),
            ("heartbeatTimeoutMs", "49"),
            ("heartbeatTimeoutMs", "10001"),
            ("heartbeatFailures", "0"),
            ("heartbeatFailures", "65"),
        ] {
            let l = limits(&format!("[cpuRunaway]\n{k} = {v}\n"));
            assert!(l.cpu_runaway.validate().is_err(), "{k}={v} must fail");
            let l = limits(&format!("[cpuRunaway]\nenabled = false\n{k} = {v}\n"));
            assert!(
                l.cpu_runaway.validate().is_ok(),
                "{k}={v} must pass when disabled"
            );
        }
        for (k, v) in [
            ("windowMs", "2000"),
            ("windowMs", "600000"),
            ("minCpuMilli", "1"),
            ("minCpuMilli", "64000"),
            ("heartbeatTimeoutMs", "50"),
            ("heartbeatTimeoutMs", "10000"),
            ("heartbeatFailures", "1"),
            ("heartbeatFailures", "64"),
        ] {
            let l = limits(&format!("[cpuRunaway]\n{k} = {v}\n"));
            assert!(l.cpu_runaway.validate().is_ok(), "{k}={v} must pass");
        }
    }

    /// MIG-12: `waitUntilGraceMs` — camelCase TOML parse, 30 s default,
    /// `0` = disable (valid), upper bound enforced.
    #[test]
    fn wait_until_grace_config() {
        let d = SupervisorConfig::default();
        assert_eq!(d.wait_until_grace_ms, 30_000);
        assert_eq!(d.params().wait_until_grace, Duration::from_secs(30));
        d.validate().unwrap();

        let parsed: SupervisorConfig = toml::from_str("waitUntilGraceMs = 45000\n").unwrap();
        assert_eq!(parsed.params().wait_until_grace, Duration::from_secs(45));

        // 0 disables the wait — an explicit, valid value.
        let parsed: SupervisorConfig = toml::from_str("waitUntilGraceMs = 0\n").unwrap();
        assert!(parsed.params().wait_until_grace.is_zero());
        parsed.validate().unwrap();

        // snake_case and unknown keys are rejected, not ignored.
        assert!(toml::from_str::<SupervisorConfig>("wait_until_grace_ms = 1\n").is_err());
        assert!(toml::from_str::<SupervisorConfig>("waitUntilGraceSec = 30\n").is_err());

        // 300 s is the ceiling; anything above fails validation.
        let parsed: SupervisorConfig = toml::from_str("waitUntilGraceMs = 300001\n").unwrap();
        let e = parsed.validate().unwrap_err().to_string();
        assert!(e.contains("waitUntilGraceMs"), "{e}");
        let parsed: SupervisorConfig = toml::from_str("waitUntilGraceMs = 300000\n").unwrap();
        parsed.validate().unwrap();
    }

    /// MIG-12-r2: `doSwitchWaitUntilGraceMs` — camelCase TOML parse,
    /// 2 s default, `0` = do_switch generations skip the wait
    /// entirely, 30 s hard ceiling, and the cross-field bound against
    /// MIG-13-r3: `prewarm_ttl` is a DERIVED cap — its floor must
    /// cover a worst-case serialized-switch window
    /// (`drainTimeoutMs` + `doSwitchWaitUntilGraceMs` + `killGraceMs`
    /// + margin), else the expiry could fire inside the gate and
    /// point live early-flip routes at a killed warm process.
    #[test]
    fn prewarm_ttl_floor_covers_drain_budget() {
        let d = SupervisorConfig::default();
        // Defaults: 30s drain + 2s wu grace + 5s kill grace + 5s
        // margin = 42s floor (above the 20s base).
        assert_eq!(d.params().prewarm_ttl, Duration::from_secs(30 + 2 + 5 + 5));
        // A long drain raises the floor — never clamped below it.
        let parsed: SupervisorConfig =
            toml::from_str("drainTimeoutMs = 120000\nkillGraceMs = 9000\n").unwrap();
        assert_eq!(
            parsed.params().prewarm_ttl,
            Duration::from_secs(120 + 2 + 9 + 5)
        );
        // And `bin/vrt-host.rs` may only RAISE it via 2*switchHoldMs —
        // the floor itself is not configurable, so no drift is possible.
    }

    /// `ingress.switchHoldMs` (the do_switch wait must fit inside the
    /// hold window or held requests 503 anyway).
    #[test]
    fn do_switch_wait_until_grace_config() {
        let d = SupervisorConfig::default();
        assert_eq!(d.do_switch_wait_until_grace_ms, 2_000);
        assert_eq!(
            d.params().do_switch_wait_until_grace,
            Duration::from_secs(2)
        );
        d.validate().unwrap();

        let parsed: SupervisorConfig = toml::from_str("doSwitchWaitUntilGraceMs = 1500\n").unwrap();
        assert_eq!(
            parsed.params().do_switch_wait_until_grace,
            Duration::from_millis(1500)
        );

        // 0 disables the do_switch wait — an explicit, valid value.
        let parsed: SupervisorConfig = toml::from_str("doSwitchWaitUntilGraceMs = 0\n").unwrap();
        assert!(parsed.params().do_switch_wait_until_grace.is_zero());
        parsed.validate().unwrap();

        // snake_case and unknown keys are rejected, not ignored.
        assert!(toml::from_str::<SupervisorConfig>("do_switch_wait_until_grace_ms = 1\n").is_err());
        assert!(toml::from_str::<SupervisorConfig>("doSwitchWaitUntilGraceSec = 2\n").is_err());

        // 30 s is the ceiling; anything above fails validation.
        let parsed: SupervisorConfig =
            toml::from_str("doSwitchWaitUntilGraceMs = 30001\n").unwrap();
        let e = parsed.validate().unwrap_err().to_string();
        assert!(e.contains("doSwitchWaitUntilGraceMs"), "{e}");
        let parsed: SupervisorConfig =
            toml::from_str("doSwitchWaitUntilGraceMs = 30000\n").unwrap();
        parsed.validate().unwrap();

        // Cross-field: the do_switch grace must be strictly below
        // `ingress.switchHoldMs` — equal or larger can never meet the
        // hold deadline, and a disabled hold (0) unbounds the check.
        check_do_switch_grace_within_hold(2_000, 10_000).unwrap();
        check_do_switch_grace_within_hold(0, 10_000).unwrap();
        check_do_switch_grace_within_hold(30_000, 0).unwrap();
        for (grace, hold) in [(10_000u64, 10_000u64), (20_000, 10_000)] {
            let e = check_do_switch_grace_within_hold(grace, hold)
                .unwrap_err()
                .to_string();
            assert!(
                e.contains("doSwitchWaitUntilGraceMs") && e.contains("switchHoldMs"),
                "{grace}/{hold}: {e}"
            );
        }
    }

    /// MIG-14: the operator surface can never carry `internalTcpAllow` —
    /// the field is intentionally absent from `EgressPolicyToml`, so
    /// `deny_unknown_fields` fails the whole params file. Internal TCP
    /// exceptions exist only in platform Rust code (the MIG-15 hook).
    #[test]
    fn egress_policy_rejects_internal_tcp_allow() {
        // TCP fields ARE operator-tunable; prove they deserialize so
        // the deny below is specifically about internalTcpAllow.
        let ok: EgressPolicyToml =
            toml::from_str("tcpAllowedPorts = [43]\ntcpConnectTimeoutMs = 5000\n").unwrap();
        assert_eq!(ok.tcp_allowed_ports, vec![43]);
        assert_eq!(ok.tcp_connect_timeout_ms, 5_000);

        // `internalTcpAllow` in any spelling kills the parse.
        for doc in [
            "internalTcpAllow = []\n",
            "internal_tcp_allow = []\n",
            "internalTcpAllow = [ { host = \"10.0.0.5\", port = 5432 } ]\n",
            "[[internalTcpAllow]]\nhost = \"10.0.0.5\"\nport = 5432\n",
        ] {
            assert!(
                toml::from_str::<EgressPolicyToml>(doc).is_err(),
                "must reject: {doc:?}"
            );
        }

        // And the conversion never produces exceptions of its own.
        let policy = vrt_config::EgressPolicy::try_from(ok).unwrap();
        assert!(policy.internal_tcp_allow.is_empty());
    }
    /// MIG-17-r2: do-pipeline config boundaries — every knob rejects
    /// out-of-range values (fail closed at startup, never clamped to
    /// a silently-degenerate pipeline).
    #[test]
    fn do_pipeline_config_boundaries() {
        let d = SupervisorConfig::default();
        d.validate().unwrap();
        assert_eq!(
            d.params().do_pipeline.concurrency,
            crate::do_switch_pipeline::DEFAULT_CONCURRENCY
        );

        // concurrency: 0 and >64 reject; 1 and 64 accept.
        for (v, ok) in [(0usize, false), (1, true), (64, true), (65, false)] {
            let parsed: SupervisorConfig =
                toml::from_str(&format!("doPipelineConcurrency = {v}\n")).unwrap();
            assert_eq!(parsed.validate().is_ok(), ok, "concurrency={v}");
        }

        // op timeout: below 1s and above 120s reject; edges accept.
        for (v, ok) in [
            (999u64, false),
            (1000, true),
            (120_000, true),
            (120_001, false),
        ] {
            // Raise the dependent bounds too — a large op timeout
            // must not trip the budget/restore cross-field checks
            // (they're tested separately below).
            let parsed: SupervisorConfig = toml::from_str(&format!(
                "doPipelineOpTimeoutMs = {v}\ndoPipelineBudgetMs = 600000\ndoPipelineRestoreTimeoutMs = 600000\n"
            ))
            .unwrap();
            assert_eq!(parsed.validate().is_ok(), ok, "op_timeout={v}");
        }

        // budget < op_timeout rejects (a single op could never fit);
        // >600s rejects; op_timeout..=600s accepts.
        let parsed: SupervisorConfig =
            toml::from_str("doPipelineOpTimeoutMs = 30000\ndoPipelineBudgetMs = 29999\n").unwrap();
        assert!(parsed.validate().is_err(), "budget < op_timeout");
        let parsed: SupervisorConfig =
            toml::from_str("doPipelineOpTimeoutMs = 30000\ndoPipelineBudgetMs = 30000\n").unwrap();
        parsed.validate().unwrap();
        let parsed: SupervisorConfig = toml::from_str("doPipelineBudgetMs = 600001\n").unwrap();
        assert!(parsed.validate().is_err(), "budget > 600s");

        // restore timeout (B1): must cover the per-op bound — a
        // narrower value re-opens the mid-unit-timeout window.
        let parsed: SupervisorConfig =
            toml::from_str("doPipelineOpTimeoutMs = 20000\ndoPipelineRestoreTimeoutMs = 19999\n")
                .unwrap();
        assert!(parsed.validate().is_err(), "restore < op_timeout");
        let parsed: SupervisorConfig =
            toml::from_str("doPipelineOpTimeoutMs = 20000\ndoPipelineRestoreTimeoutMs = 20000\n")
                .unwrap();
        parsed.validate().unwrap();
        let parsed: SupervisorConfig =
            toml::from_str("doPipelineRestoreTimeoutMs = 600001\n").unwrap();
        assert!(parsed.validate().is_err(), "restore > 600s");

        // camelCase contract — snake_case spellings are rejected, not
        // silently defaulted.
        assert!(toml::from_str::<SupervisorConfig>("do_pipeline_concurrency = 4\n").is_err());
        assert!(
            toml::from_str::<SupervisorConfig>("do_pipeline_restore_timeout_ms = 60000\n").is_err()
        );
    }

    /// MIG-17d: `[doWal] rebaselineChainBytes` — absent →
    /// `replayMaxBytes/2`; 0 or ≥ replayMaxBytes → fall back to
    /// default with a warning; headroom (`replay - maxPending`)
    /// violations clamp.
    #[test]
    fn do_wal_rebaseline_chain_bytes_boundaries() {
        let parse =
            |body: &str| DoWalConfig::resolve(&toml::from_str::<DoWalConfig>(body).unwrap());
        // Absent → derived default.
        let (cfg, warns) = parse("");
        assert_eq!(cfg.rebaseline_chain_bytes, (1 << 30) / 2);
        assert!(warns.iter().all(|w| !w.contains("rebaselineChainBytes")));
        // Explicit 0 → invalid → derived default + warning.
        let (cfg, warns) = parse("rebaselineChainBytes = 0\n");
        assert_eq!(cfg.rebaseline_chain_bytes, (1 << 30) / 2);
        assert!(warns.iter().any(|w| w.contains("rebaselineChainBytes")));
        // ≥ replayMax → invalid → default + warning.
        let (cfg, warns) = parse("rebaselineChainBytes = 1073741824\n");
        assert_eq!(cfg.rebaseline_chain_bytes, (1 << 30) / 2);
        assert!(warns.iter().any(|w| w.contains("rebaselineChainBytes")));
        // Valid explicit value passes through.
        let (cfg, warns) = parse("rebaselineChainBytes = 134217728\n");
        assert_eq!(cfg.rebaseline_chain_bytes, 134217728);
        assert!(warns.iter().all(|w| !w.contains("rebaselineChainBytes")));
        // Headroom: replay - maxPending = 128MiB-96MiB → explicit
        // 64MiB leaves <96MiB → clamped to 32MiB + warning.
        let (cfg, warns) = parse(
            "replayMaxBytes = 134217728\nmaxPendingBytes = 100663296\nrebaselineChainBytes = 67108864\n",
        );
        assert_eq!(cfg.rebaseline_chain_bytes, 32 << 20);
        assert!(warns.iter().any(|w| w.contains("headroom")));
        // Headroom with the default: same numbers, derived replay/2
        // = 64MiB also violates → clamped identically.
        let (cfg, warns) = parse("replayMaxBytes = 134217728\nmaxPendingBytes = 100663296\n");
        assert_eq!(cfg.rebaseline_chain_bytes, 32 << 20);
        assert!(warns.iter().any(|w| w.contains("headroom")));
        // Degenerate headroom == 0 (maxPending ≥ replay): r2 clamps
        // to the replay/8 FLOOR, not ~1B — occasional cap_exceeded
        // alerts beat a forced snapshot every 30s.
        let (cfg, warns) = parse("replayMaxBytes = 134217728\nmaxPendingBytes = 134217728\n");
        assert_eq!(cfg.rebaseline_chain_bytes, 134217728 / 8);
        assert!(warns.iter().any(|w| w.contains("headroom")));
    }

    /// `[geoip]` — default-on with upstream defaults; hard failures
    /// only on malformed operator input, and the sync config resolves
    /// workDir-relative defaults.
    #[test]
    fn geoip_defaults_and_validation() {
        // Absent section → enabled with upstream defaults.
        let d = GeoIpConfig::default();
        assert!(d.enabled);
        d.validate().unwrap();
        let sync = d.sync_config(std::path::Path::new("/w"));
        assert_eq!(sync.mmdb_path.to_str().unwrap(), "/w/geoip/Merged-IP.mmdb");
        assert!(sync.releases_api_url.contains("api.github.com"));
        assert!(sync.download_url.contains("/releases/latest/download/"));
        assert_eq!(sync.refresh_interval, Duration::from_secs(43200));

        // enabled=false → validation is a no-op even with junk fields.
        let off: GeoIpConfig =
            toml::from_str("enabled = false\ndownloadUrl = \"http://insecure\"\n").unwrap();
        off.validate().unwrap();

        // Non-https URL with enabled → reject.
        let bad: GeoIpConfig =
            toml::from_str("downloadUrl = \"http://example.com/x.mmdb\"\n").unwrap();
        assert!(bad.validate().is_err());
        // Empty releasesApiUrl = mirror mode → allowed.
        let mirror: GeoIpConfig = toml::from_str(
            "releasesApiUrl = \"\"\ndownloadUrl = \"https://mirror.local/x.mmdb\"\n",
        )
        .unwrap();
        mirror.validate().unwrap();
        assert!(mirror
            .sync_config(std::path::Path::new("/w"))
            .releases_api_url
            .is_empty());
        // Mirror mode with an explicit empty downloadUrl has no
        // source at all — fail fast instead of error-looping.
        let broken: GeoIpConfig =
            toml::from_str("releasesApiUrl = \"\"\ndownloadUrl = \"\"\n").unwrap();
        assert!(broken.validate().is_err());
        // Interval bounds.
        for secs in [299u64, 604801] {
            let c: GeoIpConfig =
                toml::from_str(&format!("refreshIntervalSecs = {secs}\n")).unwrap();
            assert!(c.validate().is_err(), "{secs} must reject");
        }
        // Unknown key → deny_unknown_fields.
        assert!(toml::from_str::<GeoIpConfig>("bogus = 1\n").is_err());
    }
}
