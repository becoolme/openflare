//! Production construction path for the cell controller — extracted
//! from `bin/cell-controller.rs` so the REAL wiring (config → typed
//! deps → `CellController`) is exercised by tests, not re-implemented
//! (T13/H1). The bin stays a thin shell over [`build_controller`].

use crate::ack::{AckSink, HttpAckSink};
use crate::controller::{CellController, CellControllerDeps};
use crate::fence::{self, FenceStore};
use crate::manifest::ManifestApplier;
use crate::pod::PodChannel;
use crate::retry::RetryPlan;
use crate::source::{DesiredSource, HttpDesiredSource};
use crate::verify::CellVerifier;
use serde::Deserialize;
use snapshot_protocol::TrustedKey;
use std::sync::Arc;

/// Controller config (TOML at `/etc/vibeploy/cell-controller.toml`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProdConfig {
    pub cell_id: String,
    pub control_plane_url: String,
    /// B7: render-inputs dir (FileRenderInputs contract). When set,
    /// the controller pulls /vrt/render-inputs each tick and writes
    /// `vars.json`/`secrets.json` (0600) for vrt-host. Absent → no
    /// pull (cells without secret/var bindings need nothing).
    #[serde(default)]
    pub render_inputs_dir: Option<std::path::PathBuf>,
    pub token_file: String,
    #[serde(default = "default_listen")]
    pub health_listen: String,
    /// B12: internal metrics bind (`ip:port`); absent → no listener.
    #[serde(default)]
    pub metrics_listen: Option<String>,
    #[serde(default = "default_poll")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_holder")]
    pub cron_holder: String,
    /// Durable revision-fence file (L10) — required in production.
    pub fence_file: Option<String>,
    /// Explicit escape hatch for local/test HTTP control planes.
    #[serde(default)]
    pub allow_insecure_http: bool,
    /// T12/G5 + T13/H1: when true (default) the NetworkPolicy must be
    /// applied AND read-back confirmed before tenants are delivered —
    /// otherwise the cell ACKs PARTIAL `egress-policy-unavailable` and
    /// /healthz reports the condition. Set false only for local dev.
    #[serde(default = "default_true")]
    pub egress_required: bool,
    /// T13/H1: runtime-pod NetworkPolicy spec, validated at load via
    /// `render_network_policy` (invalid config → startup failure).
    #[serde(default)]
    pub netpolicy: Option<vrt_config::netpolicy::RuntimeNetPolicy>,
    pub trusted_keys: Vec<TrustedKey>,
    /// T14/B1: pod delivery dirs (`[[pods]]` in TOML). Empty list is
    /// legal only for a control-plane-only/drain cell — the channel is
    /// then `Unavailable` and every delivery fails loudly.
    #[serde(default)]
    pub pods: Vec<crate::file_channel::FilePodSpec>,
    /// T21/F1: declared replica count for THIS controller deployment.
    /// Cron leadership needs a distributed lease; the in-memory lease
    /// only covers a single replica, so `replicas > 1` WITHOUT a
    /// distributed lease store fails closed — cron is disabled and the
    /// misconfiguration is logged at startup (never silently elected).
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// T21/F2: durable cron claim directory (`cron-claims.json` inside).
    /// Defaults to the fence file's parent directory, then `.`.
    #[serde(default)]
    pub cron_state_dir: Option<std::path::PathBuf>,
    /// B9: distributed lease for multi-replica cron. `[cron_lease]
    /// type = "kubernetes"` selects `KubernetesLeaseStore`; absent →
    /// memory lease (single replica only — `replicas > 1` fails
    /// closed). Any other `type` is a config error at load.
    #[serde(default)]
    pub cron_lease: Option<CronLeaseConfig>,
    /// T35: `[d1sql]` — local data-plane endpoint for control-plane
    /// D1 ops (execute / migrations). Absent → `d1` capability false
    /// and the ops channel is not polled (fail closed).
    #[serde(default)]
    pub d1sql: Option<crate::d1ops::D1SqlConfig>,
    /// T37b: `[queue_dispatch]` — consumer dispatch tuning. The
    /// dispatcher only runs when `[d1sql]` is configured (queues and
    /// D1 share the one vrt-sql data plane); absent section ⇒
    /// defaults, absent `[d1sql]` ⇒ no dispatcher (fail closed).
    #[serde(default)]
    pub queue_dispatch: Option<QueueDispatchToml>,
    /// RV-R70: `[storage_purge]` — KV/R2 backend wiring for
    /// STORAGE_PURGE cell ops. Absent ⇒ `storagePurge` capability
    /// false (the control plane never dispatches the kind here).
    #[serde(default)]
    pub storage_purge: Option<crate::storagepurge::StoragePurgeConfig>,
    /// Per-worker quotas capability (ACK bit `quotas`): the cell's
    /// vrt-host renders the workerd `limits` block. Default `false`:
    /// the bit is OMITTED from the ACK while false, so a control plane
    /// whose strict schema does not know `quotas` yet keeps accepting
    /// acks. Enable it only after the control plane ships the key (the
    /// pinned workerd and vrt-host already support it; leave
    /// `[render] quotas` at its default on the hosts). With the bit
    /// absent, the control plane rejects deploys that declare a
    /// non-default quota (409, fail closed).
    #[serde(default)]
    pub quotas: bool,
    /// Periodic cell report (pod metrics + D1/queue stats + Workflow/DO
    /// instance listings). Absent → enabled with defaults when pods or
    /// `[d1sql]` are configured; `enabled = false` opts out.
    #[serde(default)]
    pub report: Option<ReportToml>,
}

/// `[report]` — periodic runtime → control-plane collection cadence.
/// `deny_unknown_fields` keeps a typo from silently disabling it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportToml {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_report_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_report_timeout")]
    pub timeout_secs: u64,
}

fn default_report_interval() -> u64 {
    30
}
fn default_report_timeout() -> u64 {
    10
}

impl Default for ReportToml {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: default_report_interval(),
            timeout_secs: default_report_timeout(),
        }
    }
}

/// T37b: `[queue_dispatch]` — optional tuning over
/// [`crate::queue_dispatch::DispatchConfig`] defaults. Any present
/// field is range-checked by `DispatchConfig::validate`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueueDispatchToml {
    /// `_vrt_q_dispatch.holder` — defaults to `cron_holder`.
    #[serde(default)]
    pub holder_id: Option<String>,
    /// Dispatch lease TTL in ms (5s..300s).
    #[serde(default)]
    pub dispatch_ttl_ms: Option<i64>,
    /// Message lease TTL in ms -- must be > delivery_timeout_secs+15s.
    #[serde(default)]
    pub msg_lease_ms: Option<i64>,
    /// Batch delivery timeout in seconds (< msg_lease-15s).
    #[serde(default)]
    pub delivery_timeout_secs: Option<u64>,
    /// Global cap on in-flight batches, 1..=1024.
    #[serde(default)]
    pub global_inflight_max: Option<usize>,
    /// T37b-r: claim raw body byte budget (default 4MB, max 32MB).
    #[serde(default)]
    pub claim_max_bytes: Option<u64>,
}

/// B9: `[cron_lease]` — distributed cron leader election backend.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronLeaseConfig {
    /// Only `"kubernetes"` today — a namespaced
    /// `coordination.k8s.io/v1 Lease` object.
    #[serde(rename = "type")]
    pub kind: String,
    /// API base URL (default in-cluster). Must be https.
    #[serde(default = "default_k8s_api")]
    pub api_url: String,
    /// Lease namespace (required — no silent default).
    pub namespace: String,
    /// Lease object name.
    #[serde(default = "default_lease_name")]
    pub lease_name: String,
    /// ServiceAccount token path.
    #[serde(default = "default_sa_token")]
    pub token_path: String,
    /// CA bundle path (empty = platform roots — dev only).
    #[serde(default = "default_sa_ca")]
    pub ca_path: String,
    /// Lease duration + request timeout, seconds.
    #[serde(default = "default_lease_secs")]
    pub lease_seconds: u64,
    /// Request timeout for API calls, seconds.
    #[serde(default = "default_lease_timeout")]
    pub api_timeout_secs: u64,
}

fn default_k8s_api() -> String {
    "https://kubernetes.default.svc".into()
}
fn default_lease_name() -> String {
    "vrt-cron-leader".into()
}
fn default_sa_token() -> String {
    crate::lease_k8s::DEFAULT_TOKEN_PATH.into()
}
fn default_sa_ca() -> String {
    crate::lease_k8s::DEFAULT_CA_PATH.into()
}
fn default_lease_secs() -> u64 {
    40
}
fn default_lease_timeout() -> u64 {
    5
}

fn default_listen() -> String {
    "0.0.0.0:9090".into()
}
fn default_poll() -> u64 {
    10
}
fn default_holder() -> String {
    "cell-controller".into()
}
fn default_true() -> bool {
    true
}
fn default_replicas() -> u32 {
    1
}

/// Parse + statically validate the config file. A malformed netpolicy
/// spec fails HERE (load time), not on the first reconcile tick.
pub fn load_config(path: &str) -> Result<ProdConfig, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    let mut config: ProdConfig = toml::from_str(&text).map_err(|e| {
        // RV-R93: `[cron.lease]` is NOT the key — the table is
        // `[cron_lease]`. Name it explicitly; an unknown `cron` table
        // must never be tolerated (it would silently degrade to the
        // in-memory lease).
        let hint = match text.parse::<toml::Table>() {
            Ok(t) if t.contains_key("cron") => {
                " (hint: the distributed lease table is `[cron_lease]`, not `[cron.lease]`)"
            }
            _ => "",
        };
        format!("parse {path}: {e}{hint}")
    })?;
    // T28/H2: `VRT_CELL_ID` env (k8s shared `vrt-cell` ConfigMap —
    // the single manifest source) overrides the toml value so every
    // downstream use — desired URL, verifier, metrics `cell_id`
    // const label — sees ONE identity.
    if let Ok(v) = std::env::var("VRT_CELL_ID") {
        if !v.is_empty() {
            config.cell_id = v;
        }
    }
    fence::validate_control_plane_url(&config.control_plane_url, config.allow_insecure_http)
        .map_err(|e| format!("invalid control_plane_url: {e}"))?;
    if let Some(spec) = &config.netpolicy {
        vrt_config::netpolicy::render_network_policy(spec)
            .map_err(|e| format!("invalid netpolicy spec: {e}"))?;
    }
    if let Some(lease) = &config.cron_lease {
        if lease.kind != "kubernetes" {
            return Err(format!("unsupported cron_lease type {:?}", lease.kind));
        }
        if !lease.api_url.starts_with("https://") {
            return Err("cron_lease api_url must be https".into());
        }
    }
    if let Some(report) = &config.report {
        if !(5..=3600).contains(&report.interval_secs) {
            return Err("report.interval_secs must be 5..=3600".into());
        }
        if !(1..=60).contains(&report.timeout_secs) {
            return Err("report.timeout_secs must be 1..=60".into());
        }
    }
    Ok(config)
}

/// HTTP source + ack sink pair for the configured control plane.
/// The token is read once at boot; it is never logged (Debug off).
pub fn http_deps(
    config: &ProdConfig,
) -> Result<(Arc<dyn DesiredSource>, Arc<dyn AckSink>), String> {
    let token = std::fs::read_to_string(&config.token_file)
        .map_err(|e| format!("read token_file: {e}"))?
        .trim()
        .to_string();
    let base = config.control_plane_url.trim_end_matches('/');
    let desired_url = format!(
        "{base}/api/internal/clusters/{}/vrt/desired",
        config.cell_id
    );
    let ack_url = format!("{desired_url}/ack");
    let retry = RetryPlan::default();
    let source = HttpDesiredSource::new(
        desired_url,
        token.clone(),
        std::time::Duration::from_secs(5),
        retry.clone(),
    )
    .map_err(|e| format!("desired source: {e}"))?;
    let ack = HttpAckSink::new(ack_url, token, std::time::Duration::from_secs(5), retry)
        .map_err(|e| format!("ack sink: {e}"))?;
    Ok((Arc::new(source), Arc::new(ack)))
}

/// T14/B1: pod channel construction — the SAME path the binary uses
/// (`[[pods]]` → `FileEnvelopePodChannel`; absent → `Unavailable`).
/// Spec validation errors abort startup (fail closed).
pub fn pod_channel(config: &ProdConfig) -> Result<Arc<dyn PodChannel>, String> {
    if config.pods.is_empty() {
        // Deliberate, logged — never a silent no-op channel.
        tracing::error!(
            "no [[pods]] configured — pod delivery unavailable;              cell will report PARTIAL for every publish"
        );
        return Ok(Arc::new(crate::pod::UnavailablePodChannel(
            "no [[pods]] configured",
        )));
    }
    let channel = crate::file_channel::FileEnvelopePodChannel::new(
        &config.pods,
        std::time::Duration::from_secs(3),
    )?;
    Ok(Arc::new(channel))
}

/// Restore the durable revision fence + verifier. Corrupt/absent-with-
/// error fails closed — the caller must not start the controller.
pub fn restore_fence(
    config: &ProdConfig,
) -> Result<(Option<Box<dyn FenceStore>>, CellVerifier), String> {
    let store = match config.fence_file.as_deref() {
        Some(path) => {
            let s = fence::FileFenceStore::new(path);
            match FenceStore::load_full(&s) {
                Ok(state) => Some((s, state)),
                Err(e) => {
                    return Err(format!(
                        "revision fence unreadable — refusing to start; \
                         see workers-runtime-cell-controller.md recovery steps: {e}"
                    ))
                }
            }
        }
        None => None,
    };
    let verifier = match &store {
        Some((_, Some(state))) => CellVerifier::restore_with_attestation(
            config.cell_id.clone(),
            config.trusted_keys.clone(),
            state.revision,
            &state.envelope_digest,
            state.attestation.clone(),
        )
        .ok_or("revision fence restore rejected — refusing to start".to_string())?,
        _ => CellVerifier::new(config.cell_id.clone(), config.trusted_keys.clone()),
    };
    Ok((
        store.map(|(s, _)| Box::new(s) as Box<dyn FenceStore>),
        verifier,
    ))
}

/// Assemble the typed dependency set + controller — the SAME path the
/// production binary uses. `source`/`channel`/`ack`/`manifests` are
/// the external edges (HTTP/pod/cluster writers); tests inject memory
/// implementations while the wiring (egress_required, netpolicy,
/// retry plans, fence) is exactly what production runs.
pub fn build_controller(
    config: &ProdConfig,
    source: Arc<dyn DesiredSource>,
    channel: Arc<dyn PodChannel>,
    ack: Arc<dyn AckSink>,
    manifests: Option<Arc<dyn ManifestApplier>>,
    capabilities: crate::ack::AckCapabilities,
) -> Result<CellController, String> {
    let (fence, verifier) = restore_fence(config)?;
    // T14/S3: MVP escape hatch — running WITHOUT an egress NetworkPolicy
    // is a documented, opt-in configuration, never a silent default.
    // Log it structured at startup; the controller health view marks
    // `egress-policy-disabled` so /healthz consumers see the degraded
    // isolation posture permanently.
    if !config.egress_required {
        tracing::warn!(
            "egress_required=false — NO NetworkPolicy enforcement:              MVP/local config only; tenants have unrestricted egress"
        );
    }
    Ok(CellController::new(
        config.cell_id.clone(),
        verifier,
        CellControllerDeps {
            source,
            channel,
            ack,
            deliver_retry: RetryPlan::default(),
            ack_retry: RetryPlan::default(),
            manifests,
            egress_required: config.egress_required,
            netpolicy: config.netpolicy.clone(),
            fence,
            capabilities,
        },
    ))
}

/// T21: cron construction — durable claim log + delivery invoker.
///
/// Returns `(log, invoker, lease, lease_ttl, cron_capable)`:
/// - `[cron_lease] type="kubernetes"` → `KubernetesLeaseStore` —
///   `replicas > 1` is then legal and cron reports `true`; a bad
///   lease config fails startup (fail closed);
/// - `replicas > 1` WITHOUT a distributed lease → `Unavailable`-style
///   fail-closed invoker (`cron_capable == false`, logged — a
///   multi-replica cell must never elect cron leaders from memory);
/// - `[[pods]]` configured → `HttpCronInvoker` over the same loopback
///   bases the file channel probes;
/// - no pods → the unavailable placeholder fails loudly per trigger.
pub fn cron_deps(
    config: &ProdConfig,
) -> Result<
    (
        Box<dyn crate::cron::TriggerLog>,
        Box<dyn crate::cron::CronInvoker>,
        Box<dyn crate::lease::LeaseStore>,
        chrono::Duration,
        bool,
    ),
    String,
> {
    // B9: lease selection is decided before the replicas gate — a
    // configured kubernetes lease makes multi-replica legal.
    let (lease, lease_ttl, distributed): (
        Box<dyn crate::lease::LeaseStore>,
        chrono::Duration,
        bool,
    ) = match &config.cron_lease {
        Some(l) => {
            let store =
                crate::lease_k8s::KubernetesLeaseStore::new(&crate::lease_k8s::K8sLeaseConfig {
                    api_url: l.api_url.clone(),
                    namespace: l.namespace.clone(),
                    lease_name: l.lease_name.clone(),
                    token_path: l.token_path.clone(),
                    ca_path: l.ca_path.clone(),
                    timeout: std::time::Duration::from_secs(l.api_timeout_secs.max(1)),
                    allow_insecure: false,
                })?;
            (
                Box::new(store),
                chrono::Duration::seconds(l.lease_seconds.max(5) as i64),
                true,
            )
        }
        None => (
            Box::new(crate::lease::MemoryLeaseStore::default()),
            chrono::Duration::minutes(3),
            false,
        ),
    };
    let dir = config
        .cron_state_dir
        .clone()
        .or_else(|| {
            config
                .fence_file
                .as_deref()
                .and_then(|f| std::path::Path::new(f).parent().map(|p| p.to_path_buf()))
        })
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let log: Box<dyn crate::cron::TriggerLog> =
        Box::new(crate::cron::FileTriggerLog::open(&dir).map_err(|e| format!("cron log: {e}"))?);
    if config.replicas > 1 && !distributed {
        // Fail closed: multi-replica cron without a distributed lease
        // would elect every replica a leader and double-fire.
        tracing::error!(
            replicas = config.replicas,
            "cron disabled: replicas>1 requires a distributed lease store (k8s Lease — B9); refusing to elect from memory"
        );
        return Ok((
            log,
            Box::new(UnavailableCronInvoker),
            lease,
            lease_ttl,
            false,
        ));
    }
    if config.pods.is_empty() {
        tracing::error!("no [[pods]] configured — cron delivery unavailable; triggers fail loudly");
        return Ok((
            log,
            Box::new(UnavailableCronInvoker),
            lease,
            lease_ttl,
            false,
        ));
    }
    let pairs: Vec<(String, String)> = config
        .pods
        .iter()
        .map(|p| (p.id.clone(), p.readyz.clone()))
        .collect();
    // CR1: must exceed vrt-host's cron DISPATCH_TIMEOUT (30s) — a
    // shorter client timeout misreports a still-running scheduled()
    // as a delivery failure (the claim is consumed either way; the
    // report must reflect the facade's real verdict).
    let invoker = crate::cron::HttpCronInvoker::new(&pairs, std::time::Duration::from_secs(35))?;
    Ok((log, Box::new(invoker), lease, lease_ttl, true))
}

/// Unverified-free fail-closed invoker for configurations that cannot
/// honestly deliver (multi-replica without distributed lease, no pods).
/// Delivery always errors → the slot is claimed but never claimed as
/// fired — visible in the tick outcome, never a fake success.
struct UnavailableCronInvoker;

#[async_trait::async_trait]
impl crate::cron::CronInvoker for UnavailableCronInvoker {
    async fn trigger(
        &self,
        _pod_id: &str,
        _trigger: &crate::cron::CronTrigger,
    ) -> Result<(), crate::cron::CronError> {
        Err(crate::cron::CronError::Delivery)
    }
}

/// T37b: dispatcher construction — `[d1sql]` supplies the shared
/// vrt-sql endpoint + auth secret; `[[pods]]` readyz bases carry the
/// dispatch HTTP surface (`/v1/queue/dispatch` on the same listener).
/// `None` ⇒ no `[d1sql]` — dispatcher disabled (fail closed).
pub fn queue_dispatch_deps(
    config: &ProdConfig,
) -> Result<Option<crate::queue_dispatch::QueueDispatcher>, String> {
    let Some(d1cfg) = &config.d1sql else {
        return Ok(None);
    };
    let toml_cfg = config.queue_dispatch.as_ref();
    let secret = vrt_securefs::load_service_secret(
        std::path::Path::new(&d1cfg.auth_secret_file),
        vrt_securefs::SecretPurpose::ServiceHmac,
    )
    .map_err(|e| format!("queue_dispatch: auth_secret_file unusable: {e}"))?
    .into_bytes();
    let sql = crate::queue_dispatch::QueueSqlClient::new(
        &d1cfg.endpoint,
        secret,
        std::time::Duration::from_secs(d1cfg.timeout_secs.max(1)),
    )?;
    let timeout = std::time::Duration::from_secs(
        toml_cfg
            .and_then(|c| c.delivery_timeout_secs)
            .unwrap_or(60)
            .max(1),
    );
    let pods: Vec<(String, String)> = config
        .pods
        .iter()
        .map(|p| (p.id.clone(), p.readyz.clone()))
        .collect();
    let invoker = crate::queue_dispatch::HttpQueueInvoker::new(&pods, timeout)?;
    let cfg = crate::queue_dispatch::DispatchConfig {
        holder_id: toml_cfg
            .and_then(|c| c.holder_id.clone())
            .unwrap_or_else(|| config.cron_holder.clone()),
        dispatch_ttl_ms: toml_cfg
            .and_then(|c| c.dispatch_ttl_ms)
            .unwrap_or(crate::queue_dispatch::DISPATCH_TTL_DEFAULT_MS),
        msg_lease_ms: toml_cfg
            .and_then(|c| c.msg_lease_ms)
            .unwrap_or(crate::queue_dispatch::MSG_LEASE_DEFAULT_MS),
        delivery_timeout: timeout,
        global_inflight_max: toml_cfg
            .and_then(|c| c.global_inflight_max)
            .unwrap_or(crate::queue_dispatch::GLOBAL_INFLIGHT_MAX),
        claim_max_bytes: toml_cfg
            .and_then(|c| c.claim_max_bytes)
            .unwrap_or(crate::queue_dispatch::CLAIM_BUDGET_DEFAULT_BYTES),
    };
    let dispatcher = crate::queue_dispatch::QueueDispatcher::new(
        std::sync::Arc::new(sql),
        std::sync::Arc::new(invoker),
        cfg,
    )?;
    Ok(Some(dispatcher))
}
