//! Worker-directory packing — `vrt.toml` metadata + module files →
//! pack-format JSON → [`VersionBundle::from_pack`]. The stored
//! document/digest are produced by the SAME vrt-config code path the
//! host verifies, and vrt-config's `ts_contract` test pins them
//! byte-for-byte against the TypeScript builder.
//!
//! ```toml
//! name = "edge-api"                # worker name (shared DNS-label rule)
//! mainModule = "main.js"
//! compatibilityDate = "2024-09-25"
//! compatibilityFlags = ["nodejs_compat"]
//!
//! [modules]                        # explicit type overrides:
//! "assets/logo.bin" = "data"       #   esModule|commonJs|text|json|data|wasm
//!
//! [vars]                           # plaintext bindings → vars.json
//! LOG_LEVEL = "info"               # var_ref = var:{name}:{vhash8}:{binding}
//!                                  # — values are part of VERSION identity (F2)
//!
//! [secrets]                        # binding name → secretRef (values
//! API_KEY = "api-key-ref"          #   via `vrt secret put`, never here)
//!
//! [bindings]                       # resource bindings
//! kv = { CACHE = "ns-1" }
//! r2 = { BUCKET = "my-bucket" }
//! pg = { DB = "db-01" }
//! hyperdrive = { DB = "my-db" }    # database_tenant -> vrtpgcred:my-db
//! ratelimit = { LIMITER = { namespaceId = "1001", limit = 1500, period = 60 } }
//! service = { SVC = "other-worker" }   # by worker NAME → resolved to workerId
//! assets = { SITE = "<64-hex>" }
//!
//! hostnames = ["api.example.com"]
//! [[crons]]
//! scheduleId = "cron:edge-api:0"
//! expression = "*/15 * * * *"
//!
//! [quotas]
//! cpuMs = 50
//! memoryMb = 128
//! subrequests = 50
//! outboundConnections = 16
//! ```
//!
//! Module discovery is a recursive scan: `.js/.mjs`→esModule,
//! `.cjs`→commonJs, `.json`→json (canonicalized — safe-integer rule
//! applies exactly as the TypeScript side), `.txt`→text,
//! `.wasm`→wasm; anything else requires an explicit `[modules]`
//! entry or the deploy fails closed. `vrt.toml`, dotfiles and
//! `node_modules/` are skipped; symlinks are rejected.

use crate::LocalError;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use snapshot_protocol::compute_desired::{
    AssetsBinding, BindingDesired, BindingPermissions, CronDesired, DoBindingDesired,
    DoClassDesired, HyperdriveBinding, KvBinding, PgBinding, R2Binding, RatelimitBinding,
    RunWorkerFirst, SecretBinding, ServiceBinding, VarBinding, WorkerQuotas,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One `rules[]` entry — `{type, globs, fallthrough?}`. `type` holds
/// the wrangler `ConfigModuleRuleType` spelling (`ESModule`, ...);
/// validated against the supported set at pack time (vrt.toml authors
/// it natively, wrangler sources arrive already normalized).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModuleRuleToml {
    #[serde(rename = "type")]
    pub(crate) ty: String,
    #[serde(default)]
    pub(crate) globs: Vec<String>,
    #[serde(default)]
    pub(crate) fallthrough: Option<bool>,
}

/// vrt.toml document (deny_unknown_fields — a typo must fail closed).
/// pub(crate) — `project_cfg` synthesizes this shape from wrangler
/// sources through the identical deserialize path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerToml {
    pub(crate) name: String,
    pub(crate) main_module: String,
    pub(crate) compatibility_date: String,
    #[serde(default)]
    pub(crate) compatibility_flags: Vec<String>,
    /// Explicit `path → type` overrides/additions.
    #[serde(default)]
    pub(crate) modules: BTreeMap<String, String>,
    /// CF `rules` — ordered `{type, globs, fallthrough?}` module-typing
    /// rules (wrangler sources; native `vrt.toml` may declare the same
    /// shape). Evaluated per file before the extension table.
    #[serde(default)]
    pub(crate) module_rules: Vec<ModuleRuleToml>,
    /// CF `base_dir` — the directory rule globs are evaluated against
    /// (CF `moduleRoot`). Default = the `main` directory at pack time.
    #[serde(default)]
    pub(crate) base_dir: Option<String>,
    #[serde(default)]
    pub(crate) vars: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) secrets: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) bindings: Option<BindingsToml>,
    #[serde(default)]
    pub(crate) hostnames: Vec<String>,
    #[serde(default)]
    pub(crate) crons: Vec<CronDesired>,
    /// T37b: `[[queueConsumers]]` -- the queues this worker consumes (queue
    /// is a display name, queueId is derived at pack time as `vrt-q:{tenant}:{name}`).
    #[serde(default)]
    pub(crate) queue_consumers: Vec<QueueConsumerToml>,
    /// `[observability]` — normalized `{enabled, logsEnabled,
    /// invocationLogs, headSamplingRate}`; gates the platform
    /// `vrt-tail` collector and head sampling at render time.
    #[serde(default)]
    pub(crate) observability:
        Option<snapshot_protocol::compute_desired::ObservabilityDesired>,
    /// `streamingTailConsumers` — same `{service}`-by-name shape as
    /// `tailConsumers`, but the consumer receives TailStream events
    /// via `tailStream()` instead of batched `tail()` calls (CF
    /// `streaming_tail_consumers`; workerd `streamingTails`).
    #[serde(default)]
    pub(crate) streaming_tail_consumers: Vec<TailConsumerToml>,
    /// P3c-1: `durableObjectBindings` -- DO namespace bindings
    /// (same shape after normalizing wrangler `durable_objects.bindings[]`).
    #[serde(default)]
    pub(crate) durable_object_bindings: Vec<DoBindingToml>,
    /// P3c-1: `durableObjectMigrations` -- the shape after normalizing wrangler `[[migrations]]`
    /// ; at deploy time evaluated in order into the final class set.
    #[serde(default)]
    pub(crate) durable_object_migrations: Vec<DoMigrationToml>,
    /// W1: `[[workflows]]` -- same shape after normalization.
    #[serde(default)]
    pub(crate) workflows: Vec<WorkflowToml>,
    /// `[[tailConsumers]]` -- Tail Workers (same-tenant worker names)
    /// that receive this worker's invocation events.
    #[serde(default)]
    pub(crate) tail_consumers: Vec<TailConsumerToml>,
    #[serde(default)]
    pub(crate) quotas: Option<QuotasToml>,
    /// T21: `[assets]` directory declaration.
    #[serde(default)]
    pub(crate) assets: Option<AssetsToml>,
    /// CF `[dev]` — wrangler dev-server settings (`ip`/`port`/`host`),
    /// consumed by `vrt dev` for the loopback ingress; never part of
    /// the desired state (dev-tooling config, not runtime semantics).
    #[serde(default)]
    pub(crate) dev: Option<DevToml>,
}

/// `[dev]` — wrangler dev-server settings (`vrt dev` only).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DevToml {
    /// Loopback ingress IP (`localhost` normalizes to 127.0.0.1).
    #[serde(default)]
    pub(crate) ip: Option<String>,
    /// Loopback ingress port.
    #[serde(default)]
    pub(crate) port: Option<u16>,
    /// Extra hostname registered for the dev session (CF `dev.host`).
    #[serde(default)]
    pub(crate) host: Option<String>,
}

/// CF `[[send_email]]` restriction set (send-bindings): the
/// destination pin and the allowlists travel through signed desired
/// state; the provider enforces them at send time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SendEmailToml {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) destination_address: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) allowed_destination_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) allowed_sender_addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BindingsToml {
    #[serde(default)]
    pub(crate) kv: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) r2: BTreeMap<String, String>,
    /// `name -> user-side database_id` (the server derives the real id at pack time).
    #[serde(default)]
    pub(crate) d1: BTreeMap<String, String>,
    /// T37: queue producer bindings. The value is the user-side queue
    /// name (queueId is derived at pack time from
    /// `vrt-q:{tenant}:{name}` -- standalone has no control-plane
    /// provisioning), either as the legacy plain-string form or the
    /// `{ queue, deliveryDelaySeconds }` table form carrying a
    /// queue-level default send delay.
    #[serde(default)]
    pub(crate) queue: BTreeMap<String, QueueTargetToml>,
    /// K2: `name -> user-side stream name` (streamId derived at pack time
    /// from `vrt-k2:{tenant}:{name}` — first 32 hex chars of sha256).
    #[serde(default)]
    pub(crate) k2: BTreeMap<String, String>,
    /// `send_email` bindings: `name -> CF send-bindings restriction set`.
    #[serde(default)]
    pub(crate) send_email: BTreeMap<String, SendEmailToml>,
    #[serde(default)]
    pub(crate) pg: BTreeMap<String, String>,
    /// `binding name -> vrt database tenant name`; credentials are resolved
    /// from the provider-side `vrtpgcred:<database tenant>` entry.
    #[serde(default)]
    pub(crate) hyperdrive: BTreeMap<String, String>,
    /// Named fixed-window rate limiters.
    #[serde(default)]
    pub(crate) ratelimit: BTreeMap<String, RatelimitToml>,
    #[serde(default)]
    pub(crate) service: BTreeMap<String, ServiceTargetToml>,
    #[serde(default)]
    pub(crate) assets: BTreeMap<String, String>,
    /// Dynamic Workers loader bindings — binding names only; a loader
    /// points at no resource (the tenant supplies the code at runtime).
    #[serde(default)]
    pub(crate) worker_loaders: Vec<String>,
    /// CF `[[unsafe.bindings]] type="unsafe_eval"` — binding names only;
    /// each renders as workerd's native `unsafeEval` capability.
    #[serde(default)]
    pub(crate) unsafe_evals: Vec<String>,
    /// CF `[images]` — binding names only; transforms run in-worker on
    /// the vendored WASM engine via facade-images (no resource ref).
    #[serde(default)]
    pub(crate) images: Vec<String>,
    /// CF `[[flagship]]` — `binding name -> app_id` (the app record
    /// lives tenant-side: standalone resolves it in `flagship.json`,
    /// the platform in the Flagship app store).
    #[serde(default)]
    pub(crate) flagship: BTreeMap<String, String>,
}

/// `[bindings].ratelimit.<name>` normalized shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RatelimitToml {
    pub(crate) namespace_id: String,
    pub(crate) limit: u64,
    pub(crate) period: u64,
}

/// `[bindings.queue]` value: either the legacy plain queue name or
/// `{ queue, deliveryDelaySeconds }`. The string form serializes exactly
/// as before, so existing packages keep their bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum QueueTargetToml {
    /// Legacy `NAME = "queue-name"`.
    Name(String),
    /// `NAME = { queue = "queue-name", deliveryDelaySeconds = 5 }`.
    Options(QueueOptionsToml),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueueOptionsToml {
    /// User-side queue name (id derived at pack time).
    pub(crate) queue: String,
    /// Queue-level default send delay, 0..=86,400 seconds. Absent = no
    /// default (Cloudflare's own default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) delivery_delay_seconds: Option<u32>,
}

impl QueueTargetToml {
    pub(crate) fn queue(&self) -> &str {
        match self {
            Self::Name(name) => name,
            Self::Options(options) => &options.queue,
        }
    }

    pub(crate) fn delivery_delay_seconds(&self) -> Option<u32> {
        match self {
            Self::Name(_) => None,
            Self::Options(options) => options.delivery_delay_seconds,
        }
    }
}

/// `[bindings.service]` value: either the target worker name (default
/// entrypoint, the original wire shape) or a table with the target plus an
/// optional named `WorkerEntrypoint` export and/or `props` object. The
/// plain-string form serializes exactly as before, so existing packages
/// keep their bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum ServiceTargetToml {
    Worker(String),
    Named(NamedServiceTargetToml),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NamedServiceTargetToml {
    pub(crate) service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) entrypoint: Option<String>,
    /// Cloudflare `services[].props` — a JSON object surfaced as
    /// `ctx.props` on the target entrypoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) props: Option<serde_json::Value>,
}

impl ServiceTargetToml {
    pub(crate) fn service(&self) -> &str {
        match self {
            Self::Worker(s) => s,
            Self::Named(n) => &n.service,
        }
    }

    pub(crate) fn entrypoint(&self) -> Option<&str> {
        match self {
            Self::Worker(_) => None,
            Self::Named(n) => n.entrypoint.as_deref(),
        }
    }

    pub(crate) fn props(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Worker(_) => None,
            Self::Named(n) => n.props.as_ref(),
        }
    }
}

/// T37b `[[queueConsumers]]`: fields aligned with wrangler `queues.consumers[]`
/// (snake->camel is normalized by project_cfg; numeric bounds are checked at pack time,
/// consistent with [`QueueConsumerDesired`] validation).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueueConsumerToml {
    /// Queue display name (id derived at pack time).
    pub(crate) queue: String,
    /// Dead-letter queue display name.
    pub(crate) dead_letter_queue: Option<String>,
    /// 1..=100.
    pub(crate) max_batch_size: Option<u32>,
    /// Milliseconds, 0..=60_000 (normalized schema `maxBatchTimeoutMs`).
    pub(crate) max_batch_timeout_ms: Option<u32>,
    /// 0..=100.
    pub(crate) max_retries: Option<u32>,
    /// Seconds, 0..=86_400 (normalized `retryDelaySeconds`).
    pub(crate) retry_delay_seconds: Option<u32>,
    /// 1..=1000 (platform dispatch concurrency cap).
    pub(crate) max_concurrency: Option<u32>,
}

/// P3c-1 `durableObjectBindings[]` -- fields aligned with wrangler
/// `durable_objects.bindings[]` (`script_name` -> `scriptName`
/// normalization is done by project_cfg; the native vrt.toml uses camelCase directly).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DoBindingToml {
    /// The namespace stub handle on `env.<name>`.
    pub(crate) name: String,
    /// The JS class name exported by the target worker.
    pub(crate) class_name: String,
    /// wrangler `script_name` -- another worker name within the same tenant; omitted = this
    /// worker declares the class.
    pub(crate) script_name: Option<String>,
}

/// P3c-1 `durableObjectMigrations[]` -- the normalized shape of wrangler `[[migrations]]`.
/// Evaluated in array order: `newClasses`/`newSqliteClasses` declare,
/// `renamedClasses` inherit storage identity, `deletedClasses` remove.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DoMigrationToml {
    /// Human-readable tag (CF semantics -- identification only, not part of evaluation).
    pub(crate) tag: Option<String>,
    /// KV-backend class -- under localDisk the same storage as SQLite, `enableSql=false`.
    #[serde(default)]
    pub(crate) new_classes: Vec<String>,
    /// SQLite-backend class -- `enableSql=true`, exposes `storage.sql`.
    #[serde(default)]
    pub(crate) new_sqlite_classes: Vec<String>,
    /// `{from,to}` -- `to` inherits `from`'s uniqueKey (data retained).
    #[serde(default)]
    pub(crate) renamed_classes: Vec<DoRenameToml>,
    /// Remove the class declaration; the data directory is cleaned after the generation switch completes.
    #[serde(default)]
    pub(crate) deleted_classes: Vec<String>,
}

/// The `{from,to}` shape of P3c-1 `renamedClasses[]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DoRenameToml {
    /// Existing class name (declared by the current version or by an earlier migration step in this deploy).
    pub(crate) from: String,
    /// New class name -- inherits `from`'s uniqueKey and enableSql.
    pub(crate) to: String,
}

/// W1 `[[workflows]]` -- fields aligned with wrangler (`class_name` ->
/// `className`, `script_name` -> `scriptName` normalization is done by
/// project_cfg). MVP: only classes of the same worker are allowed; when `script_name` points to another
/// worker, pack fails closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowToml {
    /// Workflow name (`^[A-Za-z0-9_-]{1,64}$`), unique within the worker.
    pub(crate) name: String,
    /// `env.<binding>` binding name (JS identifier).
    pub(crate) binding: String,
    /// The `WorkflowEntrypoint` class name exported by the same worker.
    pub(crate) class_name: String,
    /// wrangler `script_name` -- only allowed to equal this worker's name (same-
    /// worker); pointing at another worker is rejected at pack (W1 scope restriction).
    pub(crate) script_name: Option<String>,
    /// `limits.steps` (1..=100_000).
    pub(crate) limits_steps: Option<u32>,
}

/// One `[[tailConsumers]]` entry (wrangler `tail_consumers`). The
/// `service` is a worker NAME in the same tenant; pack resolves it to
/// the stable worker id (fail closed when it does not exist).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TailConsumerToml {
    pub(crate) service: String,
}

/// T21: `[assets]` — static-assets directory packed at deploy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssetsToml {
    /// Directory relative to the worker dir (canonicalized inside).
    pub(crate) dir: String,
    /// Binding name (`env.<name>.fetch()`) — defaults to `ASSETS`.
    #[serde(default = "default_assets_binding")]
    pub(crate) binding: String,
    /// Cloudflare's `html_handling`: `auto-trailing-slash` |
    /// `force-trailing-slash` | `drop-trailing-slash` | `none`.
    #[serde(default)]
    pub(crate) html_handling: Option<String>,
    /// `none` | `404-page` | `single-page-application`.
    #[serde(default)]
    pub(crate) not_found_handling: Option<String>,
    /// T31c: opt into Cloudflare assets-first routing. `vrt.toml`
    /// native default is `false` — worker-first, assets reachable only
    /// via `env.<binding>.fetch()` — preserving the behavior every
    /// pre-T31c standalone deployment was built and tested against.
    /// Set `assetsFirst = true` for the Cloudflare default.
    #[serde(default)]
    pub(crate) assets_first: bool,
    /// T31c: `run_worker_first` — `true`/array-of-globs overrides per
    /// Cloudflare semantics; `!`-prefixed rules route assets-first.
    #[serde(default)]
    pub(crate) run_worker_first: Option<RunWorkerFirstToml>,
}

/// T31c: `runWorkerFirst` value on `[assets]` — Wrangler wire shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum RunWorkerFirstToml {
    /// `runWorkerFirst = <bool>`.
    Bool(bool),
    /// `runWorkerFirst = ["/path", "!/asset-path", …]`.
    Rules(Vec<String>),
}

fn default_assets_binding() -> String {
    "ASSETS".into()
}

/// `[quotas]` — per-worker resource limits. Every field is optional and
/// defaults to the platform default, so a partial declaration is
/// normalized instead of rejected; `WorkerQuotas::platform_default()` is the single
/// source of truth shared with `vrt-config` (a default that disagreed
/// would silently become an enforced limit).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QuotasToml {
    #[serde(default = "default_cpu_ms")]
    pub(crate) cpu_ms: u32,
    #[serde(default = "default_memory_mb")]
    pub(crate) memory_mb: u32,
    #[serde(default = "default_subrequests")]
    pub(crate) subrequests: u32,
    #[serde(default = "default_outbound_connections")]
    pub(crate) outbound_connections: u32,
}

fn default_cpu_ms() -> u32 {
    WorkerQuotas::platform_default().cpu_ms
}
fn default_memory_mb() -> u32 {
    WorkerQuotas::platform_default().memory_mb
}
fn default_subrequests() -> u32 {
    WorkerQuotas::platform_default().subrequests
}
fn default_outbound_connections() -> u32 {
    WorkerQuotas::platform_default().outbound_connections
}

/// P3c-1: DO packing context -- the deploy key (uniqueKey HMAC derivation) and
/// the class set of the currently active version (source of the `renamed_classes` lineage).
/// `None` => any DO declaration is rejected (`pack`/`pack_with_resolver` have no
/// key/state context, so the produced uniqueKey cannot be proven server-derived).
pub struct DoPackCtx<'a> {
    /// uniqueKey derivation key (standalone: the signing seed; control plane: the snapshot
    /// signing key material). Used only as an HMAC key, never persisted or sent out.
    pub pepper: [u8; 32],
    /// The class set declared by the currently active version (source for resolving rename `from`).
    pub prior_classes: &'a [DoClassDesired],
}

/// Caller-side augmentations applied on top of vrt.toml (API surface —
/// e.g. service bindings resolved against live state).
#[derive(Clone, Debug, Default)]
pub struct DeployOverrides {
    /// Override `vrt.toml` `name` (rare — for programmatic deploys).
    pub name: Option<String>,
    /// Select a named Wrangler environment (`vrt.toml` has no env blocks).
    pub env_name: Option<String>,
    /// Extra desired bindings appended after vrt.toml's.
    pub bindings: Vec<BindingDesired>,
    /// Extra hostnames.
    pub hostnames: Vec<String>,
    /// Non-secret var overrides merged into `[vars]` (`--var`,
    /// `.dev.vars` undeclared keys) — same-name config entries lose.
    /// Secret values must NEVER be passed here; they go through
    /// `put_secret` + `[secrets]` refs.
    pub vars: BTreeMap<String, String>,
}

/// Fully packed worker ready for state application.
pub struct PackedWorker {
    /// Worker name.
    pub name: String,
    /// Bundle digest (content identity).
    pub digest: String,
    /// Stored bundle document bytes (`objects/<digest>`).
    pub stored_document: Vec<u8>,
    /// compat values for the version record.
    pub compatibility_date: String,
    /// compat flags.
    pub compatibility_flags: Vec<String>,
    /// Desired bindings (var/secret/resource).
    pub bindings: Vec<BindingDesired>,
    /// `varRef → value` entries for THIS version — stored on the
    /// version record and projected into `vars.json` as a union
    /// (content-hash-scoped refs keep versions disjoint, F2).
    pub var_values: BTreeMap<String, String>,
    /// Desired crons.
    pub crons: Vec<CronDesired>,
    /// T37b: desired queue consumers (queueId already resolved).
    pub queue_consumers: Vec<snapshot_protocol::compute_desired::QueueConsumerDesired>,
    /// `[observability]` — version-scoped like `tail_consumers`:
    /// rollback restores the earlier collection gates.
    pub observability: Option<snapshot_protocol::compute_desired::ObservabilityDesired>,
    /// Desired streaming tail consumers — same resolution rule as
    /// `tail_consumers`, rendered to `streamingTails` instead of `tails`.
    pub streaming_tail_consumers: Vec<snapshot_protocol::compute_desired::TailConsumerDesired>,
    /// P3c-1: DO class declarations -- the result of evaluating migrations, `uniqueKey`
    /// already derived by HMAC with the deploy key, rename lineage resolved.
    pub durable_object_classes: Vec<DoClassDesired>,
    /// W1: workflow declarations -- `uniqueKey` already derived by HMAC with the deploy key
    /// (`vrt-wf:` domain), the same-worker restriction and name/binding validation have passed.
    pub workflows: Vec<snapshot_protocol::compute_desired::WorkflowDesired>,
    /// Tail Workers this version forwards invocation events to. The
    /// declared service name is resolved to a same-tenant worker id at
    /// pack time; a miss fails closed.
    pub tail_consumers: Vec<snapshot_protocol::compute_desired::TailConsumerDesired>,
    /// Declared hostnames (replaces state list when non-empty).
    pub hostnames: Vec<String>,
    /// Quotas.
    pub quotas: WorkerQuotas,
    /// Deterministic first-deploy worker id material.
    pub first_deploy_id: String,
    /// T21: assets to materialize into the local asset store before
    /// the publish commits (`None` when no `[assets]` dir declared).
    pub pending_assets: Option<PendingAssets>,
    /// Flagship app definitions the deploy carries — merged into the
    /// tenant's `state.flagship_apps` at commit (file-provided apps
    /// overwrite same-id entries; apps bound but undefined get an
    /// empty `{"flags":{}}` config so every binding resolves).
    pub flagship_apps: Vec<snapshot_protocol::compute_desired::FlagshipAppDesired>,
}

/// T21: assets scanned at pack time — the deploy writes objects +
/// manifest through `AssetStore::put_*_verified`, so a digest that
/// does not match the bytes can never be committed.
pub struct PendingAssets {
    /// Canonical manifest JSON bytes.
    pub manifest_bytes: Vec<u8>,
    /// sha256 of `manifest_bytes` (the binding's manifestDigest).
    pub manifest_digest: String,
    /// `(sha256, bytes)` per file — written under `objects/<sha256>`.
    pub objects: Vec<(String, Vec<u8>)>,
}

/// Per-manifest asset caps.
///
/// The file count and per-file size follow Cloudflare Workers' documented
/// limits (20,000 files on the free plan, 100,000 paid; 25 MiB per file).
/// We default to the conservative free-plan count. Cloudflare documents no
/// total-size limit; the total here is a local memory/disk guard for the
/// packer (the bundle is materialized in memory), overridable for large
/// sites through the environment variables below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AssetLimits {
    max_files: usize,
    max_file_bytes: u64,
    max_total_bytes: u64,
}

impl Default for AssetLimits {
    fn default() -> Self {
        Self {
            max_files: 20_000,
            max_file_bytes: 25 * 1024 * 1024,
            max_total_bytes: 512 * 1024 * 1024,
        }
    }
}

fn env_limit(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|v| *v != 0)
        .unwrap_or(default)
}

/// Caps for this process. Read once; invalid or zero values fall back to the
/// default for that dimension (a typo must not disable a guard).
fn asset_limits() -> AssetLimits {
    static LIMITS: std::sync::OnceLock<AssetLimits> = std::sync::OnceLock::new();
    *LIMITS.get_or_init(|| {
        let defaults = AssetLimits::default();
        AssetLimits {
            max_files: env_limit("VRT_ASSET_MAX_FILES", defaults.max_files as u64) as usize,
            max_file_bytes: env_limit("VRT_ASSET_MAX_FILE_BYTES", defaults.max_file_bytes),
            max_total_bytes: env_limit("VRT_ASSET_MAX_TOTAL_BYTES", defaults.max_total_bytes),
        }
    })
}

/// Extension→MIME whitelist (mirrors the platform table; unknown
/// extensions map to `application/octet-stream` — never client input).
pub fn mime_for_path(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "json" | "map" => "application/json",
        "txt" => "text/plain",
        "xml" => "application/xml",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "pdf" => "application/pdf",
        "wasm" => "application/wasm",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "csv" => "text/csv",
        "md" => "text/markdown",
        _ => "application/octet-stream",
    }
}

/// Scan `assets_dir` → `(request path, bytes)` entries. Paths are
/// forward-slash `/`-prefixed request paths; `..`/absolute/backslash/
/// NUL names are impossible under `read_dir`, but symlinks and
/// over-cap inputs are still rejected. Manifest construction lives
/// in [`assets_from_entries`] so the upload path shares it.
fn scan_assets_dir(worker_dir: &Path, assets: &AssetsToml) -> Result<PendingAssets, LocalError> {
    let dir = worker_dir
        .join(&assets.dir)
        .canonicalize()
        .map_err(|_| LocalError::Worker("assets dir missing"))?;
    let base = worker_dir
        .canonicalize()
        .map_err(|_| LocalError::Worker("worker dir"))?;
    if !dir.is_dir() || !dir.starts_with(&base) {
        return Err(LocalError::Worker("assets dir escapes worker dir"));
    }
    let valid_handling = |v: &Option<String>, allowed: &[&str]| -> Result<String, LocalError> {
        match v.as_deref() {
            None => Ok(allowed[0].to_string()),
            Some(v) if allowed.contains(&v) => Ok(v.to_string()),
            Some(_) => Err(LocalError::Worker("assets handling")),
        }
    };
    let html_handling = valid_handling(
        &assets.html_handling,
        &[
            "auto-trailing-slash",
            "force-trailing-slash",
            "drop-trailing-slash",
            "none",
        ],
    )?;
    let not_found = valid_handling(
        &assets.not_found_handling,
        &["none", "404-page", "single-page-application"],
    )?;
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).map_err(|_| LocalError::Worker("assets read"))? {
            let entry = entry.map_err(|_| LocalError::Worker("assets read"))?;
            let ft = entry
                .file_type()
                .map_err(|_| LocalError::Worker("assets stat"))?;
            if ft.is_symlink() {
                return Err(LocalError::Worker("assets symlink rejected"));
            }
            if ft.is_dir() {
                stack.push(entry.path());
                continue;
            }
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue; // dotfiles skipped, same as module scan
            }
            let entry_path = entry.path();
            let rel = entry_path
                .strip_prefix(&dir)
                .map_err(|_| LocalError::Worker("assets path"))?;
            let rel_str = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            let req_path = format!("/{rel_str}");
            let bytes =
                std::fs::read(&entry_path).map_err(|_| LocalError::Worker("assets read"))?;
            validate_config_asset(&req_path, &bytes)?;
            entries.push((req_path, bytes));
        }
    }
    assets_from_entries(entries, &html_handling, &not_found)
}

/// Validate an asset configuration file (`/_headers`, `/_redirects`,
/// `/_routes.json`) before it is published. The provider reads these
/// with a smaller cap than ordinary assets and fails closed at request
/// time, and the facade forwards `_headers` names under a bounded list
/// — so an accepted deployment must not ship a file that can 503 every
/// asset request, silently drop configured headers, or leave routing
/// unable to decide whether the Worker runs.
fn validate_config_asset(path: &str, bytes: &[u8]) -> Result<(), LocalError> {
    let result = match path {
        "/_headers" => vrt_provider::assets_headers::validate_file(bytes),
        "/_redirects" => vrt_provider::assets_redirects::validate_file(bytes),
        "/_routes.json" => vrt_provider::assets_routes::validate_file(bytes),
        _ => Ok(()),
    };
    result.map_err(|e| LocalError::WorkerOwned(format!("{path}: {e}")))
}

/// Build the canonical manifest + digest-keyed objects from
/// `(request path, bytes)` entries — shared by the directory scan
/// and any future in-memory source. All caps apply here.
fn assets_from_entries(
    entries: Vec<(String, Vec<u8>)>,
    html_handling: &str,
    not_found: &str,
) -> Result<PendingAssets, LocalError> {
    let limits = asset_limits();
    let mut files: Vec<(String, String, u64, String, Vec<u8>)> = Vec::new();
    let mut total: u64 = 0;
    for (req_path, bytes) in entries {
        let size = bytes.len() as u64;
        if size > limits.max_file_bytes {
            return Err(LocalError::Worker("asset file too large"));
        }
        total += size;
        if total > limits.max_total_bytes {
            return Err(LocalError::Worker("assets total too large"));
        }
        if files.len() >= limits.max_files {
            return Err(LocalError::Worker("too many asset files"));
        }
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        let rel_str = req_path.trim_start_matches('/');
        let mime = mime_for_path(rel_str).to_string();
        files.push((req_path, digest, size, mime, bytes));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let file_entries: Vec<serde_json::Value> = files
        .iter()
        .map(|(path, sha, size, mime, _)| {
            serde_json::json!({
                "mime": mime,
                "path": path,
                "sha256": sha,
                "size": size,
            })
        })
        .collect();
    let manifest = serde_json::json!({
        "files": file_entries,
        "htmlHandling": html_handling,
        "notFoundHandling": not_found,
        "schemaVersion": 1,
    });
    // Canonical JSON: serde_json emits object keys in insertion order
    // for Map (BTreeMap-backed by default feature `preserve_order`
    // off → keys sorted). Keys above are alphabetically ordered to
    // keep the digest stable across implementations.
    let manifest_bytes =
        serde_json::to_vec(&manifest).map_err(|_| LocalError::Worker("assets manifest"))?;
    let manifest_digest = hex::encode(sha2::Sha256::digest(&manifest_bytes));
    Ok(PendingAssets {
        manifest_bytes,
        manifest_digest,
        objects: files.into_iter().map(|(_, d, _, _, b)| (d, b)).collect(),
    })
}

const MAX_WORKER_DIR_BYTES: u64 = 96 * 1024 * 1024;

/// Pack `worker_dir`. `resolve_worker` maps a worker NAME (service
/// binding target) to a workerId — standalone names are unique, so a
/// name miss fails closed rather than guessing.
pub fn pack(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
) -> Result<PackedWorker, LocalError> {
    pack_inner(worker_dir, overrides, tenant_id, &|_| None, None)
}

/// Same as [`pack`] with a name→workerId resolver for `service`
/// bindings (standalone workers are looked up in state).
pub fn pack_with_resolver(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
) -> Result<PackedWorker, LocalError> {
    pack_inner(worker_dir, overrides, tenant_id, resolve_worker, None)
}

/// [`pack_with_resolver`] + DO context -- the deploy entry point (the only caller that can supply
/// keys/state for uniqueKey derivation and rename lineage).
pub fn pack_with_ctx(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
    do_ctx: Option<&DoPackCtx<'_>>,
) -> Result<PackedWorker, LocalError> {
    pack_inner(worker_dir, overrides, tenant_id, resolve_worker, do_ctx)
}

fn pack_inner(
    worker_dir: &Path,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
    do_ctx: Option<&DoPackCtx<'_>>,
) -> Result<PackedWorker, LocalError> {
    assemble(
        gather_dir_for_env(worker_dir, overrides.env_name.as_deref())?,
        overrides,
        tenant_id,
        resolve_worker,
        do_ctx,
    )
}

/// The content a pack consumes — collected from a local worker
/// directory or decoded from a remote-deploy upload. `assemble` runs
/// the identical validation either way: a remote deploy cannot
/// smuggle module bytes, asset digests, or meta fields that a local
/// one could not produce.
pub struct Gathered {
    /// Worker meta (vrt.toml shape — `project_cfg` normalizes the
    /// wrangler family into this same structure client-side).
    pub meta: WorkerToml,
    /// `/`-separated rel path → file bytes for module candidates.
    /// `[assets] dir` contents and scan exclusions (dotfiles,
    /// `node_modules`, config basenames) are never here.
    pub files: BTreeMap<String, Vec<u8>>,
    /// Paths the directory scan skipped — informational (the
    /// `--dry-run` summary); always empty for uploads.
    pub skipped: Vec<String>,
    /// `[assets] dir` packed content (None when undeclared).
    pub pending_assets: Option<PendingAssets>,
    /// `flagship.json` — the standalone flag store (CF's local flag
    /// store analog): validated `FlagshipAppDesired`s, `None` when the
    /// project ships no file.
    pub flagship_apps: Option<Vec<snapshot_protocol::compute_desired::FlagshipAppDesired>>,
}

/// Collect `worker_dir` into a [`Gathered`]: meta normalization,
/// self-contained-file scan (symlinks rejected), module-byte cap
/// (pre-read), and the `[assets] dir` scan. Type assignment and
/// everything downstream live in [`assemble`] — the single
/// validation path shared by `vrt deploy --root` and remote uploads.
pub fn gather_dir(worker_dir: &Path) -> Result<Gathered, LocalError> {
    gather_dir_for_env(worker_dir, None)
}

/// [`gather_dir`] with a named Wrangler environment selected before
/// module and asset discovery.
pub fn gather_dir_for_env(
    worker_dir: &Path,
    env_name: Option<&str>,
) -> Result<Gathered, LocalError> {
    // T31/C4: `vrt.toml` keeps precedence; otherwise the wrangler family
    // (json > jsonc > toml) normalizes into the same meta shape.
    let (meta, _meta_source) = crate::project_cfg::load_worker_meta_for_env(worker_dir, env_name)?;
    let mut files = BTreeMap::new();
    let mut skipped = Vec::new();
    let mut total: u64 = 0;
    // `[assets] dir` is content-addressed asset data, not worker
    // modules — exclude it from the module scan (an .html file there
    // must not fail "module type").
    // Canonicalize the worker dir ONCE up front — under a symlinked
    // tempdir (`/var` → `/private/var`) the canonical assets root and
    // the raw `worker_dir` disagree, which used to drop the exclusion.
    let worker_canonical = worker_dir
        .canonicalize()
        .unwrap_or_else(|_| worker_dir.to_path_buf());
    let assets_rel = meta
        .assets
        .as_ref()
        .map(|a| {
            worker_dir
                .join(&a.dir)
                .canonicalize()
                .ok()
                .filter(|p| p.starts_with(&worker_canonical))
        })
        .flatten();
    scan_dir(
        worker_dir,
        worker_dir,
        &mut |rel, path| {
            if let Some(root) = &assets_rel {
                let canon = path
                    .canonicalize()
                    .unwrap_or_else(|_| worker_canonical.join(rel));
                if canon.starts_with(root) {
                    return Ok(());
                }
            }
            total += std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            if total > MAX_WORKER_DIR_BYTES {
                return Err(LocalError::Worker("dir size"));
            }
            let bytes = std::fs::read(path).map_err(|_| LocalError::Worker("read"))?;
            files.insert(rel.to_string(), bytes);
            Ok(())
        },
        &mut |rel| skipped.push(rel.to_string()),
    )?;
    let pending_assets = match &meta.assets {
        Some(decl) => Some(scan_assets_dir(worker_dir, decl)?),
        None => None,
    };
    Ok(Gathered {
        meta,
        files,
        skipped,
        pending_assets,
        flagship_apps: gather_flagship_apps(worker_dir)?,
    })
}

/// `flagship.json` cap — a standalone flag store stays small;
/// oversized input fails before parse.
const MAX_FLAGSHIP_FILE_BYTES: u64 = 256 * 1024;

/// Read + validate `<worker_dir>/flagship.json` — the standalone flag
/// store, the analog of CF's local flag store for `wrangler dev` (an
/// app entry supplies the config the `cloudflare-internal:flagship`
/// module receives; absent file → no store, `None`).
fn gather_flagship_apps(
    worker_dir: &Path,
) -> Result<
    Option<Vec<snapshot_protocol::compute_desired::FlagshipAppDesired>>,
    LocalError,
> {
    let raw = match std::fs::read(worker_dir.join("flagship.json")) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(LocalError::Worker("flagship.json")),
    };
    if raw.len() as u64 > MAX_FLAGSHIP_FILE_BYTES {
        return Err(LocalError::Worker("flagship.json size"));
    }
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&raw).map_err(|_| LocalError::Worker("flagship.json"))?;
    build_flagship_apps(&map).map(Some)
}

/// `{<appId>: {accountTag?, flags}}` → validated
/// `FlagshipAppDesired`s — shared by the local `flagship.json` gather
/// and the remote upload body. The appId lands inside the serialized
/// config so the module sees a self-describing document.
pub fn build_flagship_apps(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<snapshot_protocol::compute_desired::FlagshipAppDesired>, LocalError> {
    let mut apps = Vec::with_capacity(map.len());
    for (app_id, body) in map {
        let mut config = body
            .as_object()
            .cloned()
            .ok_or(LocalError::Worker("flagship.json app"))?;
        config.insert("appId".into(), serde_json::Value::from(app_id.clone()));
        let config = serde_json::to_string(&serde_json::Value::Object(config))
            .map_err(|_| LocalError::Worker("flagship.json"))?;
        snapshot_protocol::compute_desired::validate_flagship_config(&config)
            .map_err(|_| LocalError::Worker("flagship.json flag model"))?;
        apps.push(snapshot_protocol::compute_desired::FlagshipAppDesired {
            app_id: app_id.clone(),
            config,
        });
    }
    apps.sort_by(|a, b| a.app_id.cmp(&b.app_id));
    Ok(apps)
}

/// Validate a [`Gathered`] into a [`PackedWorker`]. Everything the
/// signed desired doc depends on is computed HERE — tenant id,
/// worker-name resolution, and DO/workflow unique-key derivation are
/// server-side inputs the upload body never carries.
pub fn assemble(
    g: Gathered,
    overrides: &DeployOverrides,
    tenant_id: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
    do_ctx: Option<&DoPackCtx<'_>>,
) -> Result<PackedWorker, LocalError> {
    let Gathered {
        mut meta,
        files,
        pending_assets: gathered_assets,
        flagship_apps: gathered_flagship_apps,
        ..
    } = g;
    // Caller-side non-secret var overrides (`--var`, `.dev.vars`).
    for (k, v) in &overrides.vars {
        meta.vars.insert(k.clone(), v.clone());
    }
    let name = overrides.name.clone().unwrap_or_else(|| meta.name.clone());

    // --- modules → pack JSON: typing, caps, and the wire forms the
    // TS builder emits — one shared path for local and remote. ---
    let (pack, esm_sources) = build_pack_json(&meta, &files)?;
    // The shared validator/digester — identical code to the platform.
    let bundle = vrt_config::VersionBundle::from_pack(&pack)?;

    // --- bindings ---
    let read_write = BindingPermissions {
        read: true,
        write: true,
    };
    let read_only = BindingPermissions {
        read: true,
        write: false,
    };
    let mut bindings: Vec<BindingDesired> = Vec::new();
    let mut var_values = BTreeMap::new();
    // F2: var values are version-scoped — the ref embeds a content
    // hash of this deploy's var set so vars.json can hold every
    // version's entries as a disjoint union (rollback resolves the
    // old version's refs; a var-only edit still yields a new ref set).
    let vars_hash = {
        let canonical = serde_json::to_vec(&meta.vars).unwrap_or_default();
        hex::encode(sha2::Sha256::digest(&canonical))
    };
    for (binding, value) in &meta.vars {
        let var_ref = format!("var:{name}:{}:{binding}", &vars_hash[..8]);
        var_values.insert(var_ref.clone(), value.clone());
        bindings.push(BindingDesired::Var(VarBinding {
            name: binding.clone(),
            var_ref,
        }));
    }
    for (binding, secret_ref) in &meta.secrets {
        bindings.push(BindingDesired::Secret(SecretBinding {
            name: binding.clone(),
            secret_ref: secret_ref.clone(),
        }));
    }
    if let Some(b) = &meta.bindings {
        for (binding, ns) in &b.kv {
            bindings.push(BindingDesired::Kv(KvBinding {
                name: binding.clone(),
                namespace_id: ns.clone(),
                permissions: read_write.clone(),
            }));
        }
        for (binding, bucket) in &b.r2 {
            bindings.push(BindingDesired::R2(R2Binding {
                name: binding.clone(),
                bucket: bucket.clone(),
                permissions: read_write.clone(),
            }));
        }
        for (binding, user_id) in &b.d1 {
            // databaseId is derived server-side (T34c/F2: includes tenant_id --
            // no format migration is needed when the T35 multi-tenant control plane comes in; for standalone single-tenant
            // it is equivalent to CF account-level database_id semantics). Multiple Workers in the same instance with the same
            // databaseId share the same database (D1 semantics);
            // a Worker never gets an arbitrary id -- the signed snapshot covers the derived value.
            if user_id.is_empty()
                || user_id.len() > 64
                || !user_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(LocalError::Worker(
                    "d1 user id must be 1-64 chars of [A-Za-z0-9_-]",
                ));
            }
            use sha2::Digest;
            let digest = hex::encode(sha2::Sha256::digest(
                format!("vrt-d1:{tenant_id}:{user_id}").as_bytes(),
            ));
            bindings.push(BindingDesired::D1(
                snapshot_protocol::compute_desired::D1Binding {
                    name: binding.clone(),
                    database_id: digest[..26].to_string(),
                    epoch: 1,
                    permissions: read_write.clone(),
                },
            ));
        }
        for (binding, target) in &b.queue {
            // T37: queueId derivation -- the first 26 hex chars of sha256("vrt-q:{tenant}:{name}")
            // (hex is a subset of [a-z0-9], same rule as database_id).
            // Multiple producers with the same name in the same instance share the same queue (CF semantics);
            // a worker never gets an arbitrary id -- the signed snapshot covers the derived value.
            let digest = derive_queue_id(tenant_id, target.queue())?;
            if let Some(delay) = target.delivery_delay_seconds() {
                // Cloudflare `delivery_delay` bound; also enforced by
                // snapshot-protocol, but rejected here with a config-side
                // error before a deploy document is built.
                if delay > snapshot_protocol::compute_desired::MAX_QUEUE_DELIVERY_DELAY_SECS {
                    return Err(LocalError::Worker(
                        "queue delivery delay must be 0..=86400 seconds",
                    ));
                }
            }
            bindings.push(BindingDesired::Queue(
                snapshot_protocol::compute_desired::QueueBinding {
                    name: binding.clone(),
                    queue_id: digest,
                    queue_name: binding.clone(),
                    permissions: read_write.clone(),
                    delivery_delay_seconds: target.delivery_delay_seconds(),
                },
            ));
        }
        for (binding, stream_name) in &b.k2 {
            // K2 streamId derivation — first 32 hex chars of
            // sha256("vrt-k2:{tenant}:{name}"). The produce-only
            // binding carries the write grant (send is a write);
            // consume rides the platform subscription surface, not
            // this binding.
            let stream_id = derive_stream_id(tenant_id, stream_name)?;
            bindings.push(BindingDesired::K2(
                snapshot_protocol::compute_desired::K2Binding {
                    name: binding.clone(),
                    stream_id,
                    permissions: read_write.clone(),
                },
            ));
        }
        for (binding, restriction) in &b.send_email {
            // CF send binding — produce-only write grant; the
            // restriction set is signed into desired state and
            // enforced provider-side on every send.
            bindings.push(BindingDesired::SendEmail(
                snapshot_protocol::compute_desired::SendEmailBinding {
                    name: binding.clone(),
                    destination_address: restriction.destination_address.clone(),
                    allowed_destination_addresses: restriction
                        .allowed_destination_addresses
                        .clone(),
                    allowed_sender_addresses: restriction.allowed_sender_addresses.clone(),
                    permissions: read_write.clone(),
                },
            ));
        }
        for name in &b.images {
            bindings.push(BindingDesired::Images(
                snapshot_protocol::compute_desired::ImagesBinding { name: name.clone() },
            ));
        }
        for (binding, db) in &b.pg {
            // P1: `name → logical tenant id` — the provider resolves
            // credentials from `secrets/pg-credentials.json` under
            // `vrtpgcred:<id>` (never from the bundle). Read+write
            // grant; a `[[pg]] readOnly` toml flag is not part of the
            // standalone shape — read-only local bindings mark the
            // credential payload's `readOnly` instead.
            bindings.push(BindingDesired::Pg(PgBinding {
                name: binding.clone(),
                database_id: format!("vrtpg:{db}"),
                credential_ref: format!("vrtpgcred:{db}"),
                permissions: read_write.clone(),
            }));
        }
        for (binding, database_tenant) in &b.hyperdrive {
            let binding_id = format!("vrthd:{tenant_id}:{binding}");
            let digest = hex::encode(sha2::Sha256::digest(binding_id.as_bytes()));
            bindings.push(BindingDesired::Hyperdrive(HyperdriveBinding {
                name: binding.clone(),
                binding_id,
                synthetic_host: format!("hd{}.hyperdrive.vrt.internal", &digest[..24]),
                credential_ref: format!("vrtpgcred:{database_tenant}"),
                permissions: read_write.clone(),
            }));
        }
        for (binding, config) in &b.ratelimit {
            bindings.push(BindingDesired::Ratelimit(RatelimitBinding {
                name: binding.clone(),
                namespace_id: config.namespace_id.clone(),
                limit: config.limit,
                period: config.period,
            }));
        }
        for (binding, app_id) in &b.flagship {
            bindings.push(BindingDesired::Flagship(
                snapshot_protocol::compute_desired::FlagshipBinding {
                    name: binding.clone(),
                    app_id: app_id.clone(),
                },
            ));
        }
        for (binding, target) in &b.service {
            let worker_id =
                resolve_worker(target.service()).ok_or(LocalError::Worker("service target"))?;
            let entrypoint = target.entrypoint().map(str::to_string);
            if let Some(ep) = &entrypoint {
                if !snapshot_protocol::compute_desired::valid_service_entrypoint(ep) {
                    return Err(LocalError::Worker("service entrypoint"));
                }
            }
            let props = target.props().cloned();
            if let Some(props) = &props {
                if !snapshot_protocol::compute_desired::valid_service_props(props) {
                    return Err(LocalError::Worker("service props"));
                }
            }
            bindings.push(BindingDesired::Service(ServiceBinding {
                name: binding.clone(),
                worker_id,
                entrypoint,
                props,
            }));
        }
        for (binding, digest) in &b.assets {
            bindings.push(BindingDesired::Assets(AssetsBinding {
                name: binding.clone(),
                manifest_digest: digest.clone(),
                permissions: read_only.clone(),
                assets_first: None,
                run_worker_first: None,
            }));
        }
        for binding in &b.worker_loaders {
            bindings.push(BindingDesired::WorkerLoader(
                snapshot_protocol::compute_desired::WorkerLoaderBinding {
                    name: binding.clone(),
                },
            ));
        }
        for binding in &b.unsafe_evals {
            bindings.push(BindingDesired::UnsafeEval(
                snapshot_protocol::compute_desired::UnsafeEvalBinding {
                    name: binding.clone(),
                },
            ));
        }
    }
    for b in &overrides.bindings {
        // P1: standalone PG is declared via `[bindings].pg` / `[[pg]]`
        // so `credential_ref` (`vrtpgcred:<id>`) is minted here, paired
        // with `secrets/pg-credentials.json`. An overrides-injected Pg
        // would smuggle in an arbitrary resource id — fail closed.
        if matches!(b, BindingDesired::Pg(_)) {
            return Err(LocalError::Worker(
                "pg bindings are declared via [bindings].pg or [[pg]], not overrides",
            ));
        }
    }
    bindings.extend(overrides.bindings.iter().cloned());

    // T37b: `[[queueConsumers]]` -- queue name -> queueId derivation (same hash rule as
    // the producer); the DLQ name is derived likewise, self-reference rejected;
    // numeric bounds match the snapshot-protocol validation (two-layer fail closed).
    let mut queue_consumers = Vec::with_capacity(meta.queue_consumers.len());
    for c in &meta.queue_consumers {
        let queue_id = derive_queue_id(tenant_id, &c.queue)?;
        let dead_letter_queue_id = c
            .dead_letter_queue
            .as_deref()
            .map(|dlq| derive_queue_id(tenant_id, dlq))
            .transpose()?;
        if dead_letter_queue_id.as_deref() == Some(queue_id.as_str()) {
            return Err(LocalError::Worker(
                "queueConsumers dead_letter_queue must differ from queue",
            ));
        }
        queue_consumers.push(snapshot_protocol::compute_desired::QueueConsumerDesired {
            queue_id,
            queue_name: c.queue.clone(),
            dead_letter_queue_id,
            max_batch_size: c.max_batch_size,
            max_batch_timeout_secs: c.max_batch_timeout_ms.map(|ms| ms / 1000),
            max_retries: c.max_retries,
            retry_delay_secs: c.retry_delay_seconds,
            max_concurrency: c.max_concurrency,
        });
    }

    // `tailConsumers` / `streamingTailConsumers` — same name→id rule
    // as `[bindings.service]`: the consumer is a sibling worker of
    // THIS tenant/project, and an unresolvable name fails closed
    // (never a dangling trace target). Both lists share one resolver
    // so the self-target, dedupe and cap invariants cannot drift.
    let tail_consumers =
        resolve_tail_consumers(&meta.tail_consumers, &name, "tail_consumers", resolve_worker)?;
    let streaming_tail_consumers = resolve_tail_consumers(
        &meta.streaming_tail_consumers,
        &name,
        "streaming_tail_consumers",
        resolve_worker,
    )?;
    // One consumer cannot appear on both channels: workerd would call
    // its `tail()` AND `tailStream()` for the same event, doubling
    // delivery with no way for the consumer to tell the copies apart.
    // The signed-desired validator enforces the same rule on delivered
    // snapshots; this is the fail-early config error.
    if let Some(dup) = tail_consumers.iter().find(|c| {
        streaming_tail_consumers
            .iter()
            .any(|s| s.worker_id == c.worker_id)
    }) {
        return Err(LocalError::WorkerOwned(format!(
            "worker {} cannot be both a tail consumer and a streaming tail consumer",
            dup.worker_id
        )));
    }

    // Deterministic worker-id material: hash the worker name so a
    // re-deployed directory keeps its identity without stored state.
    // (DO uniqueKey derivation needs it too -- hoisted before assets.)
    let id_material = hex::encode(sha2::Sha256::digest(name.as_bytes()));

    // P3c-1: Durable Objects -- migrations are evaluated in order into the final class
    // set; uniqueKey = HMAC(pepper, "vrt-do:{tenant}:{worker}:{class}")
    // (ring hmac; pepper = deploy signing key material -- unpredictable to the worker,
    // stable server-side; rename inherits `from`'s key so the directory survives renames).
    let worker_id = format!("w_{}", &id_material[..16]);
    let mut durable_object_classes: Vec<DoClassDesired> = Vec::new();
    let has_do_cfg =
        !meta.durable_object_bindings.is_empty() || !meta.durable_object_migrations.is_empty();
    if has_do_cfg {
        let ctx = do_ctx.ok_or(LocalError::Worker("durable objects require deploy context"))?;
        let mut declared: BTreeMap<String, DoClassDesired> = BTreeMap::new();
        macro_rules! mint {
            ($class:expr, $sql:expr) => {{
                let class: &str = $class;
                if declared.contains_key(class) {
                    return Err(LocalError::Worker("do class redeclared"));
                }
                declared.insert(
                    class.to_string(),
                    DoClassDesired {
                        class_name: class.to_string(),
                        unique_key: derive_do_unique_key(&ctx.pepper, tenant_id, &worker_id, class),
                        enable_sql: $sql,
                        from_worker_id: None,
                    },
                );
            }};
        }
        for mig in &meta.durable_object_migrations {
            if mig.tag.as_deref().is_some_and(|t| t.len() > 128) {
                return Err(LocalError::Worker("do migration tag too long"));
            }
            for class in &mig.new_classes {
                valid_do_name(class)?;
                mint!(class, false);
            }
            for class in &mig.new_sqlite_classes {
                valid_do_name(class)?;
                mint!(class, true);
            }
            for rename in &mig.renamed_classes {
                valid_do_name(&rename.from)?;
                valid_do_name(&rename.to)?;
                if declared.contains_key(&rename.to) {
                    return Err(LocalError::Worker("do rename target declared"));
                }
                // `from`'s storage identity = the value already declared in this deploy, otherwise look up the current
                // active version; if neither has it -> the rename points at a nonexistent class,
                // risking silent data loss -> fail closed.
                let entry = declared.remove(&rename.from).or_else(|| {
                    ctx.prior_classes
                        .iter()
                        .find(|c| c.class_name == rename.from)
                        .cloned()
                });
                let Some(entry) = entry else {
                    return Err(LocalError::Worker("do rename from unknown class"));
                };
                declared.insert(
                    rename.to.clone(),
                    DoClassDesired {
                        class_name: rename.to.clone(),
                        unique_key: entry.unique_key,
                        enable_sql: entry.enable_sql,
                        from_worker_id: None,
                    },
                );
            }
            for class in &mig.deleted_classes {
                valid_do_name(class)?;
                let known = declared.remove(class).is_some()
                    || ctx.prior_classes.iter().any(|c| c.class_name == *class);
                if !known {
                    return Err(LocalError::Worker("do delete unknown class"));
                }
            }
        }
        durable_object_classes = declared.into_values().collect();
        // bindings declared but no class declaration: this worker has at least one
        // non-script_name binding -> fail closed (same direction as snapshot validation).
        for b in &meta.durable_object_bindings {
            valid_do_name(&b.name)?;
            valid_do_name(&b.class_name)?;
            let service_worker_id = match &b.script_name {
                None => None,
                Some(svc) => {
                    let target =
                        resolve_worker(svc).ok_or(LocalError::Worker("do script_name target"))?;
                    (target != worker_id).then_some(target)
                }
            };
            if service_worker_id.is_none()
                && !durable_object_classes
                    .iter()
                    .any(|c| c.class_name == b.class_name)
            {
                return Err(LocalError::Worker("do binding class not declared"));
            }
            bindings.push(BindingDesired::DurableObject(DoBindingDesired {
                name: b.name.clone(),
                class_name: b.class_name.clone(),
                service_worker_id,
            }));
        }
    }

    // W1 `[[workflows]]`; since W7 `script_name` may point to another worker in the same tenant
    // worker —— uniqueKey = HMAC(pepper,
    // "vrt-wf:{tenant}:{definingWorker}:{wfName}") (same
    // identity domain as DO classes with a different prefix; ownership by the defining worker -> both storage and op are under the target worker name,
    // consistent with CF). The target must be a deployed worker (resolve_worker 404
    // -> fail closed, same as DO script_name).
    let mut workflows: Vec<snapshot_protocol::compute_desired::WorkflowDesired> = Vec::new();
    if !meta.workflows.is_empty() {
        let ctx = do_ctx.ok_or(LocalError::Worker("workflows require deploy context"))?;
        let mut seen_names = std::collections::BTreeSet::new();
        for w in &meta.workflows {
            valid_workflow_name(&w.name)?;
            valid_do_name(&w.binding)?;
            valid_do_name(&w.class_name)?;
            if !seen_names.insert(w.name.clone()) {
                return Err(LocalError::Worker("workflow name redeclared"));
            }
            // wrangler: `script_name` defaults to the enclosing
            // script — an explicit self-reference is legal even on
            // first deploy (the worker isn't in the registry yet, so
            // resolve_worker would 404). Any other value must resolve
            // to an already-deployed worker of this tenant (deploy
            // order: the target must exist first — fail closed).
            let service_worker_id = match &w.script_name {
                None => None,
                Some(script) if *script == name => None,
                Some(script) => {
                    let target = resolve_worker(script)
                        .ok_or(LocalError::Worker("workflow script_name target"))?;
                    (target != worker_id).then_some(target)
                }
            };
            if let Some(steps) = w.limits_steps {
                if steps == 0 || steps > 100_000 {
                    return Err(LocalError::Worker("workflow limits.steps bounds"));
                }
            }
            // the uniqueKey derivation domain includes the DEFINING worker -- engine storage belongs to
            // the script_name target; target-side fields are validated separately at the desired layer.
            let defining = service_worker_id.clone().unwrap_or(worker_id.clone());
            // W7 deploy-gate: same-worker class_name must be a named
            // export AND a WorkflowEntrypoint subclass — missing
            // classes die at workerd load anyway (preflight) but with
            // a config-level error; a non-subclass passes workerd's
            // load yet dies at engine init — both rejected HERE with a
            // typed reason instead. `script_name` targets: the target
            // bundle isn't in this pack — existence is proven by the
            // workerd preflight (entrypoint designator fails the load
            // → apply rejected, old generation keeps serving).
            if service_worker_id.is_none() {
                match crate::wf_scan::workflow_class_problems(
                    &esm_sources.join("\n"),
                    std::slice::from_ref(&w.class_name),
                )
                .first()
                {
                    Some(crate::wf_scan::WfClassProblem::NotExported(_)) => {
                        return Err(LocalError::Worker("workflow class not exported by bundle"));
                    }
                    Some(crate::wf_scan::WfClassProblem::NotAClass(_)) => {
                        return Err(LocalError::Worker(
                            "workflow class_name export is not a class",
                        ));
                    }
                    Some(crate::wf_scan::WfClassProblem::NotEntrypoint(_)) => {
                        return Err(LocalError::Worker(
                            "workflow class does not extend WorkflowEntrypoint",
                        ));
                    }
                    None => {}
                }
            }
            workflows.push(snapshot_protocol::compute_desired::WorkflowDesired {
                name: w.name.clone(),
                binding: w.binding.clone(),
                class_name: w.class_name.clone(),
                service_worker_id,
                unique_key: derive_wf_unique_key(&ctx.pepper, tenant_id, &defining, &w.name),
                limits_steps: w.limits_steps,
            });
        }
    }

    // T21: `[assets] dir` — pack the directory into the local
    // content-addressed store (materialized by `Local::deploy`; the
    // binding references the manifest digest it produced).
    let pending_assets = match &meta.assets {
        Some(decl) => {
            // `[assets]` declared but no content gathered — on the
            // upload path the client simply did not ship it (a local
            // gather fails earlier at `scan_assets_dir`). Fail closed.
            let pending = gathered_assets.ok_or(LocalError::Worker("assets missing"))?;
            bindings.push(BindingDesired::Assets(AssetsBinding {
                name: decl.binding.clone(),
                manifest_digest: pending.manifest_digest.clone(),
                permissions: read_only.clone(),
                assets_first: decl.assets_first.then_some(true),
                run_worker_first: decl.run_worker_first.as_ref().map(|r| match r {
                    RunWorkerFirstToml::Bool(b) => RunWorkerFirst::Bool(*b),
                    RunWorkerFirstToml::Rules(rules) => RunWorkerFirst::Rules(rules.clone()),
                }),
            }));
            Some(pending)
        }
        None => None,
    };
    // `[bindings].assets` precomputed digests stay declarable — the
    // deploy verifies the manifest exists in the local store (fail
    // closed when it does not).

    let mut hostnames = meta.hostnames.clone();
    hostnames.extend(overrides.hostnames.iter().cloned());

    // Flagship: every bound `app_id` must resolve to an app — CF's
    // local flag store starts empty and evaluations return defaults,
    // so a bound app with no definitions synthesizes an empty
    // `{"flags":{}}` config rather than failing the deploy.
    let mut flagship_apps = gathered_flagship_apps.unwrap_or_default();
    {
        let mut have: std::collections::HashSet<String> =
            flagship_apps.iter().map(|a| a.app_id.clone()).collect();
        for b in &bindings {
            if let BindingDesired::Flagship(f) = b {
                if have.insert(f.app_id.clone()) {
                    flagship_apps.push(snapshot_protocol::compute_desired::FlagshipAppDesired {
                        app_id: f.app_id.clone(),
                        config: format!(
                            "{{\"appId\":{},\"flags\":{{}}}}",
                            serde_json::to_string(&f.app_id)
                                .map_err(|_| LocalError::Worker("flagship appId"))?
                        ),
                    });
                }
            }
        }
        flagship_apps.sort_by(|a, b| a.app_id.cmp(&b.app_id));
    }

    Ok(PackedWorker {
        name,
        digest: bundle.digest().to_string(),
        stored_document: bundle.stored_document().to_vec(),
        compatibility_date: meta.compatibility_date,
        compatibility_flags: meta.compatibility_flags,
        bindings,
        var_values,
        crons: meta.crons,
        queue_consumers,
        observability: meta.observability,
        streaming_tail_consumers,
        durable_object_classes,
        workflows,
        tail_consumers,
        hostnames,
        quotas: normalize_quotas(meta.quotas)?,
        first_deploy_id: id_material[..16].to_string(),
        pending_assets,
        flagship_apps,
    })
}

/// Normalize `[quotas]`: absent → platform defaults (stock behaviour);
/// present → every field is bounds-checked and the FIRST offender is
/// named in a deploy-time error (`VRT_CONFIG_INVALID` on the wrangler
/// path, `LocalError::Worker` here). `compute.desired::validate` runs the
/// same bounds again before signing — defense in depth, but this layer
/// owns the human-readable field name.
fn normalize_quotas(quotas: Option<QuotasToml>) -> Result<WorkerQuotas, LocalError> {
    use snapshot_protocol::compute_desired::{
        MAX_CPU_MS, MAX_MEMORY_MB, MAX_OUTBOUND_CONNECTIONS, MAX_SUBREQUESTS, MIN_CPU_MS,
        MIN_MEMORY_MB,
    };
    let Some(q) = quotas else {
        return Ok(WorkerQuotas::platform_default());
    };
    let quotas = WorkerQuotas {
        cpu_ms: q.cpu_ms,
        memory_mb: q.memory_mb,
        subrequests: q.subrequests,
        outbound_connections: q.outbound_connections,
    };
    if !(MIN_CPU_MS..=MAX_CPU_MS).contains(&quotas.cpu_ms) {
        return Err(LocalError::Worker("quotas.cpuMs"));
    }
    if !(MIN_MEMORY_MB..=MAX_MEMORY_MB).contains(&quotas.memory_mb) {
        return Err(LocalError::Worker("quotas.memoryMb"));
    }
    if quotas.subrequests > MAX_SUBREQUESTS {
        return Err(LocalError::Worker("quotas.subrequests"));
    }
    if quotas.outbound_connections > MAX_OUTBOUND_CONNECTIONS {
        return Err(LocalError::Worker("quotas.outboundConnections"));
    }
    Ok(quotas)
}

fn type_for_extension(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("js" | "mjs") => Some("esModule"),
        Some("cjs") => Some("commonJs"),
        Some("json") => Some("json"),
        Some("txt" | "text") => Some("text"),
        Some("wasm") => Some("wasm"),
        _ => None,
    }
}

/// CF `ConfigModuleRuleType` → wire module type. `PythonModule` /
/// `PythonRequirement` have no mapping (rejected at normalize; a
/// native `moduleRules` carrying them fails here too).
fn rule_type_to_module(ty: &str) -> Option<&'static str> {
    match ty {
        "ESModule" => Some("esModule"),
        "CommonJS" => Some("commonJs"),
        "CompiledWasm" => Some("wasm"),
        "Text" => Some("text"),
        "Data" => Some("data"),
        _ => None,
    }
}

/// CF wrangler implicit rules — appended AFTER user rules
/// (`DEFAULT_MODULE_RULES` in `deployment-bundle/rules.ts`; JS types
/// are intentionally absent — bundling is a wrangler-side concern and
/// our extension table classifies them).
const DEFAULT_MODULE_RULES: [(&str, &[&str]); 3] = [
    ("Text", &["**/*.txt", "**/*.html", "**/*.sql"]),
    ("Data", &["**/*.bin"]),
    ("CompiledWasm", &["**/*.wasm", "**/*.wasm?module"]),
];

/// CF `parseRules`: user rules + implicit defaults, then per-type
/// pruning — the first rule of a type without `fallthrough: true`
/// completes that type; every later rule of the same type is dead and
/// dropped (wrangler warns "does not have a fallback"; pruning keeps
/// the semantics identical). Returns `(wire module type, globs)` in
/// evaluation order.
fn effective_module_rules(
    rules: &[ModuleRuleToml],
) -> Result<Vec<(&'static str, Vec<String>)>, LocalError> {
    struct R {
        module_ty: &'static str,
        globs: Vec<String>,
        fallthrough: bool,
    }
    let mut all: Vec<R> = Vec::with_capacity(rules.len() + DEFAULT_MODULE_RULES.len());
    for r in rules {
        let module_ty = rule_type_to_module(&r.ty).ok_or(LocalError::Worker("moduleRules type"))?;
        for g in &r.globs {
            if g.is_empty() || g.len() > 1024 || g.contains('\0') {
                return Err(LocalError::Worker("moduleRules globs"));
            }
        }
        all.push(R {
            module_ty,
            globs: r.globs.clone(),
            fallthrough: r.fallthrough == Some(true),
        });
    }
    for (ty, globs) in DEFAULT_MODULE_RULES {
        all.push(R {
            module_ty: rule_type_to_module(ty).unwrap(),
            globs: globs.iter().map(|s| (*s).to_string()).collect(),
            fallthrough: false,
        });
    }
    let mut completed: std::collections::BTreeSet<&'static str> = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(all.len());
    for r in all {
        if completed.contains(r.module_ty) {
            continue;
        }
        if !r.fallthrough {
            completed.insert(r.module_ty);
        }
        out.push((r.module_ty, r.globs));
    }
    Ok(out)
}

/// Rule-eval root (CF `moduleRoot`): `base_dir` when declared, else
/// the `main` directory — globs see paths relative to it, so an
/// anchored `*.txt` only matches siblings of `main`.
fn module_root(meta: &WorkerToml) -> Result<String, LocalError> {
    if let Some(b) = &meta.base_dir {
        // Re-prove at the pack boundary — a native vrt.toml can carry
        // `baseDir` without normalize having seen it.
        if b.is_empty()
            || b.len() > 1024
            || b.starts_with('/')
            || b.contains('\\')
            || b.contains('\0')
            || b.contains(':')
            || b.ends_with('/')
            || b.split('/').any(|c| c.is_empty() || c == "." || c == "..")
        {
            return Err(LocalError::Worker("baseDir"));
        }
        return Ok(b.clone());
    }
    Ok(match meta.main_module.rfind('/') {
        Some(i) => meta.main_module[..i].to_string(),
        None => String::new(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum GlobTok {
    Lit(char),
    /// `*` — any run of non-`/` chars.
    Star,
    /// `**` between `/`s or string ends — any run incl. `/`, but it may
    /// only stop at a segment boundary (mirrors
    /// `((?:[^/]*(?:/|$))*)`).
    Globstar,
}

/// Mirror of wrangler's `glob-to-regexp` ({globstar:true}, no
/// `extended`): `*`/`**` are the only wildcards — `?`, `[...]`,
/// `{a,b}` are LITERAL characters; `\` escapes the next char into a
/// literal. Whole-string anchored match, memoized so `**` chains
/// cannot backtrack exponentially.
fn glob_match(pattern: &str, path: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let mut toks: Vec<GlobTok> = Vec::with_capacity(p.len());
    let mut i = 0usize;
    while i < p.len() {
        match p[i] {
            '*' => {
                let start = i;
                while p.get(i + 1) == Some(&'*') {
                    i += 1;
                }
                let prev = if start == 0 { None } else { Some(p[start - 1]) };
                let next = p.get(i + 1).copied();
                if i + 1 - start > 1
                    && (prev.is_none() || prev == Some('/'))
                    && (next.is_none() || next == Some('/'))
                {
                    toks.push(GlobTok::Globstar);
                    if next == Some('/') {
                        i += 1;
                    }
                } else {
                    toks.push(GlobTok::Star);
                }
            }
            '\\' => {
                i += 1;
                toks.push(GlobTok::Lit(p.get(i).copied().unwrap_or('\\')));
            }
            c => toks.push(GlobTok::Lit(c)),
        }
        i += 1;
    }
    let s: Vec<char> = path.chars().collect();
    let mut memo: std::collections::HashMap<(usize, usize), bool> =
        std::collections::HashMap::new();
    fn m(
        toks: &[GlobTok],
        ti: usize,
        s: &[char],
        si: usize,
        memo: &mut std::collections::HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(&v) = memo.get(&(ti, si)) {
            return v;
        }
        let v = match toks.get(ti) {
            None => si == s.len(),
            Some(GlobTok::Lit(c)) => s.get(si) == Some(c) && m(toks, ti + 1, s, si + 1, memo),
            Some(GlobTok::Star) => {
                let mut k = si;
                let mut ok = false;
                loop {
                    if m(toks, ti + 1, s, k, memo) {
                        ok = true;
                        break;
                    }
                    match s.get(k) {
                        Some(&ch) if ch != '/' => k += 1,
                        _ => break,
                    }
                }
                ok
            }
            Some(GlobTok::Globstar) => {
                let mut ok = m(toks, ti + 1, s, si, memo);
                let mut k = si + 1;
                while !ok && k <= s.len() {
                    if k == s.len() || s[k - 1] == '/' {
                        ok = m(toks, ti + 1, s, k, memo);
                    }
                    k += 1;
                }
                ok
            }
        };
        memo.insert((ti, si), v);
        v
    }
    m(&toks, 0, &s, 0, &mut memo)
}

/// Recursive scan; skips `vrt.toml`, dotfiles, `node_modules`
/// (reported via `skipped` so `--dry-run` can list them).
/// Symlinks are rejected — a worker dir must be self-contained.
fn scan_dir(
    root: &Path,
    dir: &Path,
    emit: &mut dyn FnMut(&str, &Path) -> Result<(), LocalError>,
    skipped: &mut dyn FnMut(&str),
) -> Result<(), LocalError> {
    let rel_of = |path: &Path| {
        path.strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/")
    };
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|_| LocalError::Worker("dir read"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for path in entries {
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if file_name.starts_with('.')
            || file_name == "node_modules"
            || crate::project_cfg::is_config_basename(file_name)
        {
            skipped(&rel_of(&path));
            continue;
        }
        let meta = std::fs::symlink_metadata(&path).map_err(|_| LocalError::Worker("stat"))?;
        if meta.file_type().is_symlink() {
            return Err(LocalError::Worker("symlink"));
        }
        if meta.is_dir() {
            scan_dir(root, &path, emit, skipped)?;
        } else if meta.is_file() {
            emit(&rel_of(&path), &path)?;
        }
    }
    Ok(())
}

/// Resolve a `*Consumers[]` `{service}` list to workerIds — shared by
/// `tail_consumers` and `streaming_tail_consumers` so the invariants
/// cannot drift: a self-target is refused (a self `streamingTails`
/// designator amplifies each invocation into another unboundedly, and
/// a self `tails` edge has no documented recursion bound either),
/// duplicates collapse on the declared name, and the resolved list
/// stays within `MAX_TAIL_CONSUMERS_PER_WORKER` — the same bound the
/// signed-desired validator re-checks.
fn resolve_tail_consumers(
    list: &[TailConsumerToml],
    self_name: &str,
    what: &str,
    resolve_worker: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<snapshot_protocol::compute_desired::TailConsumerDesired>, LocalError> {
    use snapshot_protocol::compute_desired::MAX_TAIL_CONSUMERS_PER_WORKER;
    let mut out = Vec::with_capacity(list.len());
    let mut seen = std::collections::BTreeSet::new();
    for c in list {
        if c.service == self_name {
            return Err(LocalError::WorkerOwned(format!(
                "{what} service must be another worker"
            )));
        }
        if !seen.insert(c.service.clone()) {
            continue;
        }
        out.push(snapshot_protocol::compute_desired::TailConsumerDesired {
            worker_id: resolve_worker(&c.service)
                .ok_or_else(|| LocalError::WorkerOwned(format!("{what} service target")))?,
        });
    }
    if out.len() > MAX_TAIL_CONSUMERS_PER_WORKER {
        return Err(LocalError::WorkerOwned(format!(
            "{what} at most {MAX_TAIL_CONSUMERS_PER_WORKER} consumers per worker"
        )));
    }
    Ok(out)
}

/// P3c-1: JS identifier check (class names and binding names follow the same rule --
/// `^[A-Za-z_$][A-Za-z0-9_$]{0,63}$`, consistent with binding name validation;
/// the class name cap is relaxed to 128, consistent with CF).
fn valid_do_name(value: &str) -> Result<(), LocalError> {
    let ok = !value.is_empty()
        && value.len() <= 128
        && (value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'$'))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$');
    if ok {
        Ok(())
    } else {
        Err(LocalError::Worker(
            "durable object names must be JS identifiers (<=128)",
        ))
    }
}

/// P3c-1: `uniqueKey = HMAC-SHA256(pepper,
/// "vrt-do:{tenant}:{worker}:{class}")` -- 64 hex. The derivation input contains no
/// user-controllable storage identity: tenant/worker/class are deploy identities, the pepper is
/// the deploy key (standalone signing seed / control-plane snapshot key) -- a worker
/// can neither predict nor forge another class's storage directory.
/// W1: `vrt-wf:{tenant}:{worker}:{wfName}` -> 64-hex storage identity,
/// same domain as DO uniqueKey with a different prefix (the engine class directory is independent of user classes).
pub fn derive_wf_unique_key(
    pepper: &[u8; 32],
    tenant_id: &str,
    worker_id: &str,
    wf_name: &str,
) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, pepper);
    let tag = ring::hmac::sign(
        &key,
        format!("vrt-wf:{tenant_id}:{worker_id}:{wf_name}").as_bytes(),
    );
    hex::encode(tag.as_ref())
}

/// W1: workflow name validation (`^[A-Za-z0-9_-]{1,64}$`).
fn valid_workflow_name(name: &str) -> Result<(), LocalError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(LocalError::Worker(
            "workflow name must be 1-64 chars of [A-Za-z0-9_-]",
        ));
    }
    Ok(())
}

pub fn derive_do_unique_key(
    pepper: &[u8; 32],
    tenant_id: &str,
    worker_id: &str,
    class_name: &str,
) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, pepper);
    let tag = ring::hmac::sign(
        &key,
        format!("vrt-do:{tenant_id}:{worker_id}:{class_name}").as_bytes(),
    );
    hex::encode(tag.as_ref())
}

/// T37/T37b: `vrt-q:{tenant}:{name}` -> first 26 hex chars of sha256
/// (hex is a subset of `[a-z0-9]{26}`, vrt-sql path rules). standalone has no control-plane
/// provisioning -- queue names with the same name are unique within the instance.
pub fn derive_queue_id(tenant_id: &str, queue_name: &str) -> Result<String, LocalError> {
    if queue_name.is_empty()
        || queue_name.len() > 64
        || !queue_name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(LocalError::Worker(
            "queue name must be 1-64 chars of [A-Za-z0-9_-]",
        ));
    }
    let digest = hex::encode(sha2::Sha256::digest(
        format!("vrt-q:{tenant_id}:{queue_name}").as_bytes(),
    ));
    Ok(digest[..26].to_string())
}

/// CF K2 `^[0-9a-f]{32}$` stream id: first 32 hex chars of
/// sha256("vrt-k2:{tenant}:{stream}") — same derivation rule as
/// `derive_queue_id` (hex is a subset of the id charset; the signed
/// snapshot covers the derived value).
pub fn derive_stream_id(tenant_id: &str, stream_name: &str) -> Result<String, LocalError> {
    if stream_name.is_empty()
        || stream_name.len() > 256
        || !stream_name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(LocalError::Worker(
            "stream name must be 1-256 chars of [A-Za-z0-9_-]",
        ));
    }
    let digest = hex::encode(sha2::Sha256::digest(
        format!("vrt-k2:{tenant_id}:{stream_name}").as_bytes(),
    ));
    Ok(digest[..32].to_string())
}

/// Modules → pack-format JSON: explicit `[modules]` type override
/// wins, else the extension rule; unknown extensions, absent
/// declared files, over-cap totals, non-UTF8 text, and bad JSON all
/// fail closed. Returns the pack doc + the esModule sources (the
/// workflow class scan needs them).
fn build_pack_json(
    meta: &WorkerToml,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<(serde_json::Value, Vec<String>), LocalError> {
    let mut declared = meta.modules.clone();
    let mut typed: BTreeMap<String, String> = BTreeMap::new();
    let mut total: u64 = 0;
    // CF `rules`: effective ordered rules (user + implicit defaults,
    // per-type pruning) classify each gathered file by glob against
    // the module root (`base_dir` ?? `main` dir). The entry is never
    // rule-typed — on CF it is the bundle itself, not an external
    // module. Files under no rule keep the extension table.
    let rules = effective_module_rules(&meta.module_rules)?;
    let root = module_root(meta)?;
    let root_prefix = if root.is_empty() {
        String::new()
    } else {
        format!("{root}/")
    };
    for (rel, bytes) in files {
        total += bytes.len() as u64;
        if total > MAX_WORKER_DIR_BYTES {
            return Err(LocalError::Worker("dir size"));
        }
        let ty = declared
            .remove(rel)
            .or_else(|| {
                if rel == &meta.main_module {
                    return None;
                }
                let eval = rel.strip_prefix(&root_prefix)?;
                rules.iter().find_map(|(ty, globs)| {
                    globs
                        .iter()
                        .any(|g| glob_match(g, eval))
                        .then(|| (*ty).to_string())
                })
            })
            .or_else(|| type_for_extension(Path::new(rel)).map(str::to_string))
            .ok_or(LocalError::Worker("module type"))?;
        typed.insert(rel.clone(), ty);
    }
    // Declared modules that are NOT in the gathered files fail closed.
    if !declared.is_empty() {
        return Err(LocalError::Worker("modules missing"));
    }
    let mut pack_modules = serde_json::Map::new();
    let mut esm_sources: Vec<String> = Vec::new();
    for (rel, ty) in &typed {
        let bytes = &files[rel];
        let wire = match ty.as_str() {
            "esModule" | "commonJs" | "text" | "json" => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| LocalError::Worker("utf8"))?
                    .to_string();
                if ty == "esModule" {
                    esm_sources.push(text.clone());
                }
                match ty.as_str() {
                    "esModule" => serde_json::Value::String(text),
                    "commonJs" => serde_json::json!({"cjs": text}),
                    "text" => serde_json::json!({"text": text}),
                    _ => {
                        serde_json::json!({"json": serde_json::from_str::<serde_json::Value>(&text).map_err(|_| LocalError::Worker("json module"))?})
                    }
                }
            }
            "wasm" | "data" => {
                use base64::Engine;
                let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
                if ty == "wasm" {
                    serde_json::json!({"wasm_b64": b64})
                } else {
                    serde_json::json!({"data_b64": b64})
                }
            }
            _ => return Err(LocalError::Worker("module type")),
        };
        pack_modules.insert(rel.clone(), wire);
    }
    Ok((
        serde_json::json!({
            "mainModule": meta.main_module,
            "modules": pack_modules,
            "compatibilityDate": meta.compatibility_date,
            "compatibilityFlags": meta.compatibility_flags,
        }),
        esm_sources,
    ))
}

/// `vrt deploy --dry-run` output — pack-side facts only: worker name,
/// module count, bundle bytes, declared bindings, scan-skipped paths.
/// Server-side derivations (service target ids, DO/workflow unique
/// keys) are listed as declared, never resolved — the dry run proves
/// the pack is valid and shows what WOULD deploy.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploySummary {
    /// Worker name (after overrides).
    pub name: String,
    /// Module count in the bundle.
    pub module_count: usize,
    /// Stored bundle document bytes.
    pub bundle_bytes: usize,
    /// Bundle digest (the version's content identity).
    pub bundle_digest: String,
    /// `kind:bindingName` per declared binding.
    pub bindings: Vec<String>,
    /// Secret binding names (names only — refs/values never print).
    pub secret_bindings: Vec<String>,
    /// Paths the scan ignored (dotfiles, `node_modules`, configs).
    pub ignored: Vec<String>,
}

/// Build the [`DeploySummary`] — runs the full module validation so
/// a dry run fails on exactly what a real deploy would.
pub fn summarize(g: &Gathered, overrides: &DeployOverrides) -> Result<DeploySummary, LocalError> {
    let mut meta = g.meta.clone();
    for (k, v) in &overrides.vars {
        meta.vars.insert(k.clone(), v.clone());
    }
    let (pack, _esm) = build_pack_json(&meta, &g.files)?;
    let bundle = vrt_config::VersionBundle::from_pack(&pack)?;
    let mut bindings: Vec<String> = Vec::new();
    for name in meta.vars.keys() {
        bindings.push(format!("var:{name}"));
    }
    let mut secret_bindings: Vec<String> = meta.secrets.keys().cloned().collect();
    for name in &secret_bindings {
        bindings.push(format!("secret:{name}"));
    }
    if let Some(b) = &meta.bindings {
        for name in b.kv.keys() {
            bindings.push(format!("kv:{name}"));
        }
        for name in b.r2.keys() {
            bindings.push(format!("r2:{name}"));
        }
        for name in b.d1.keys() {
            bindings.push(format!("d1:{name}"));
        }
        for name in b.queue.keys() {
            bindings.push(format!("queue:{name}"));
        }
        for name in b.k2.keys() {
            bindings.push(format!("k2:{name}"));
        }
        for name in b.send_email.keys() {
            bindings.push(format!("send_email:{name}"));
        }
        for name in &b.images {
            bindings.push(format!("images:{name}"));
        }
        for name in b.pg.keys() {
            bindings.push(format!("pg:{name}"));
        }
        for name in b.hyperdrive.keys() {
            bindings.push(format!("hyperdrive:{name}"));
        }
        for name in b.ratelimit.keys() {
            bindings.push(format!("ratelimit:{name}"));
        }
        for name in b.flagship.keys() {
            bindings.push(format!("flagship:{name}"));
        }
        for name in b.service.keys() {
            bindings.push(format!("service:{name}"));
        }
        for name in b.assets.keys() {
            bindings.push(format!("assets:{name}"));
        }
    }
    if let Some(a) = &meta.assets {
        bindings.push(format!("assets:{}", a.binding));
    }
    for b in &meta.durable_object_bindings {
        bindings.push(format!("durableObject:{}", b.name));
    }
    for w in &meta.workflows {
        bindings.push(format!("workflow:{}({})", w.name, w.binding));
    }
    for c in &meta.queue_consumers {
        bindings.push(format!("queueConsumer:{}", c.queue));
    }
    bindings.sort();
    secret_bindings.sort();
    Ok(DeploySummary {
        name: overrides.name.clone().unwrap_or_else(|| meta.name.clone()),
        module_count: g.files.len(),
        bundle_bytes: bundle.stored_document().len(),
        bundle_digest: bundle.digest().to_string(),
        bindings,
        secret_bindings,
        ignored: g.skipped.clone(),
    })
}

/// Remote-upload wire body (`vrt deploy --url`): the client
/// materializes meta + module bytes + `[assets]` content locally and
/// the server re-validates everything through [`assemble`]. Secret
/// values travel only inside this body — never in argv, logs, or
/// error text.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeployUpload {
    /// Worker meta as normalized by `project_cfg` client-side.
    pub meta: WorkerToml,
    /// `/`-separated rel path → base64 file bytes (module candidates
    /// only; `[assets] dir` content belongs in `assets`).
    #[serde(default)]
    pub modules: BTreeMap<String, String>,
    /// `[assets] dir` content (manifest + digest-keyed objects).
    #[serde(default)]
    pub assets: Option<UploadAssets>,
    /// Non-secret var overrides (`--var KEY:VALUE`) — merged into
    /// `[vars]` by `assemble`, identical to a local deploy.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// secretRef → value (`--secrets-file`, `.dev.vars` format) —
    /// written to the tenant secret store before the deploy commits.
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
    /// Worker name override (`vrt.toml` name otherwise).
    #[serde(default)]
    pub name: Option<String>,
    /// Flagship app definitions gathered client-side
    /// (`flagship.json`) — re-validated server-side with the same
    /// wire-boundary flag-model proof before state application.
    #[serde(default)]
    pub flagship_apps: Option<Vec<snapshot_protocol::compute_desired::FlagshipAppDesired>>,
}

/// Uploaded `[assets]` payload — canonical manifest + objects keyed
/// by their sha256 (each re-hashed server-side).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UploadAssets {
    /// Canonical manifest JSON, base64.
    pub manifest: String,
    /// sha256 hex → base64 file bytes.
    #[serde(default)]
    pub objects: BTreeMap<String, String>,
}

/// Decode + sanity-check a [`DeployUpload`] into a [`Gathered`].
/// A malformed path, an over-cap body, a digest that does not match
/// its bytes, or a manifest that does not enumerate exactly the
/// shipped objects fails closed here, before `assemble` runs.
pub fn gathered_from_upload(body: &DeployUpload) -> Result<Gathered, LocalError> {
    use base64::Engine;
    let mut files = BTreeMap::new();
    let mut total: u64 = 0;
    for (rel, b64) in &body.modules {
        if !valid_module_rel(rel) {
            return Err(LocalError::Worker("module path"));
        }
        // Reject absurd payloads before decode — b64 inflates ~4/3,
        // so anything beyond 2x the cap can never fit.
        if b64.len() as u64 > MAX_WORKER_DIR_BYTES * 2 {
            return Err(LocalError::Worker("dir size"));
        }
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|_| LocalError::Worker("module encoding"))?;
        total += bytes.len() as u64;
        if total > MAX_WORKER_DIR_BYTES {
            return Err(LocalError::Worker("dir size"));
        }
        files.insert(rel.clone(), bytes);
    }
    let pending_assets = match &body.assets {
        Some(a) => Some(upload_assets_verified(a)?),
        None => None,
    };
    Ok(Gathered {
        meta: body.meta.clone(),
        files,
        skipped: Vec::new(),
        pending_assets,
        flagship_apps: match &body.flagship_apps {
            Some(apps) => {
                for app in apps {
                    snapshot_protocol::compute_desired::validate_flagship_config(&app.config)
                        .map_err(|_| LocalError::Worker("flagshipApps flag model"))?;
                }
                Some(apps.clone())
            }
            None => None,
        },
    })
}

/// Upload-path shape check — mirrors `scan_dir`'s exclusions so a
/// remote body cannot place bytes where a local scan never would:
/// no `.`-prefixed components, no `node_modules`, no config
/// basenames at the leaf, no traversal or separators.
fn valid_module_rel(rel: &str) -> bool {
    if rel.is_empty() || rel.len() > 1024 || rel.contains('\\') || rel.contains('\0') {
        return false;
    }
    let comps: Vec<&str> = rel.split('/').collect();
    if comps
        .iter()
        .any(|c| c.is_empty() || *c == "." || *c == "..")
    {
        return false;
    }
    if comps
        .iter()
        .any(|c| c.starts_with('.') || *c == "node_modules")
    {
        return false;
    }
    if crate::project_cfg::is_config_basename(comps[comps.len() - 1]) {
        return false;
    }
    true
}

/// `/`-prefixed request path in an assets manifest — same rules the
/// scan produces: clean components, no traversal, no separators.
fn valid_asset_req_path(path: &str) -> bool {
    let Some(rel) = path.strip_prefix('/') else {
        return false;
    };
    !rel.is_empty()
        && rel.len() <= 1024
        && !rel.contains('\\')
        && !rel.contains('\0')
        && rel
            .split('/')
            .all(|c| !c.is_empty() && c != "." && c != "..")
}

/// Verify an uploaded `[assets]` payload into [`PendingAssets`] —
/// every object digest must match its bytes, the manifest file
/// table must enumerate EXACTLY the shipped objects, and the same
/// file/total caps apply as the directory scan.
fn upload_assets_verified(a: &UploadAssets) -> Result<PendingAssets, LocalError> {
    use base64::Engine;
    let manifest_bytes = base64::engine::general_purpose::STANDARD
        .decode(&a.manifest)
        .map_err(|_| LocalError::Worker("assets encoding"))?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|_| LocalError::Worker("assets manifest"))?;
    let files = manifest["files"]
        .as_array()
        .ok_or(LocalError::Worker("assets manifest"))?;
    let limits = asset_limits();
    if files.len() > limits.max_files {
        return Err(LocalError::Worker("too many asset files"));
    }
    let mut declared = std::collections::BTreeSet::new();
    let mut objects = Vec::with_capacity(files.len());
    let mut total: u64 = 0;
    for f in files {
        let path = f["path"]
            .as_str()
            .ok_or(LocalError::Worker("assets manifest"))?;
        let sha = f["sha256"]
            .as_str()
            .ok_or(LocalError::Worker("assets manifest"))?;
        let size = f["size"]
            .as_u64()
            .ok_or(LocalError::Worker("assets manifest"))?;
        if !valid_asset_req_path(path) {
            return Err(LocalError::Worker("assets path"));
        }
        let Some(b64) = a.objects.get(sha) else {
            return Err(LocalError::Worker("assets object missing"));
        };
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|_| LocalError::Worker("assets encoding"))?;
        if bytes.len() as u64 != size {
            return Err(LocalError::Worker("assets size"));
        }
        if hex::encode(sha2::Sha256::digest(&bytes)) != sha {
            return Err(LocalError::Worker("assets digest"));
        }
        if size > limits.max_file_bytes {
            return Err(LocalError::Worker("asset file too large"));
        }
        total += size;
        if total > limits.max_total_bytes {
            return Err(LocalError::Worker("assets total too large"));
        }
        validate_config_asset(path, &bytes)?;
        declared.insert(sha.to_string());
        objects.push((sha.to_string(), bytes));
    }
    // Exact match both directions — a hand-crafted upload that ships
    // objects the manifest never names is rejected, not ignored.
    if a.objects.keys().any(|k| !declared.contains(k)) {
        return Err(LocalError::Worker("assets object extra"));
    }
    let manifest_digest = hex::encode(sha2::Sha256::digest(&manifest_bytes));
    Ok(PendingAssets {
        manifest_bytes,
        manifest_digest,
        objects,
    })
}

#[cfg(test)]
mod assets_handling_tests {
    use super::*;

    fn assets(mode: Option<&str>) -> AssetsToml {
        AssetsToml {
            dir: "public".to_string(),
            binding: "ASSETS".to_string(),
            html_handling: mode.map(str::to_string),
            not_found_handling: None,
            assets_first: false,
            run_worker_first: None,
        }
    }

    #[test]
    fn scan_accepts_every_cloudflare_html_handling_mode() {
        let root = std::env::temp_dir().join(format!("vrt-assets-modes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dir = root.join("public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.html"), b"x").unwrap();
        for mode in [
            "auto-trailing-slash",
            "force-trailing-slash",
            "drop-trailing-slash",
            "none",
        ] {
            let pending = scan_assets_dir(&root, &assets(Some(mode)))
                .unwrap_or_else(|e| panic!("{mode}: {e:?}"));
            let doc: serde_json::Value = serde_json::from_slice(&pending.manifest_bytes).unwrap();
            assert_eq!(doc["htmlHandling"], mode);
        }
        assert!(scan_assets_dir(&root, &assets(Some("force"))).is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Deploy validation: a `_headers`/`_redirects` file the provider
    /// would fail closed on (or could not forward whole) must be
    /// rejected at pack time — for the directory scan and the upload
    /// path alike.
    #[test]
    fn config_assets_are_validated_before_publish() {
        let root = std::env::temp_dir().join(format!("vrt-assets-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dir = root.join("public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.html"), b"x").unwrap();

        // Valid files deploy.
        std::fs::write(dir.join("_headers"), b"/*\n  X-Frame-Options: DENY\n").unwrap();
        std::fs::write(dir.join("_redirects"), b"/old /new 301\n").unwrap();
        std::fs::write(
            dir.join("_routes.json"),
            br#"{"version":1,"include":["/api/*"],"exclude":["/api/public/*"]}"#,
        )
        .unwrap();
        let pending = scan_assets_dir(&root, &assets(None)).expect("valid config assets");

        // Non-UTF-8 `_headers` would 503 every asset request.
        std::fs::write(dir.join("_headers"), b"/*\n  X-A: \xff\n").unwrap();
        assert!(scan_assets_dir(&root, &assets(None)).is_err());
        std::fs::write(dir.join("_headers"), b"/*\n  X-Ok: yes\n").unwrap();
        // A `_routes.json` the provider would reject at routing time must
        // not reach the store: no include rules, an unsupported version,
        // and non-trailing wildcards all fail closed.
        for bad in [
            br#"{"version":1,"include":[]}"#.as_slice(),
            br#"{"version":2,"include":["/*"]}"#.as_slice(),
            br#"{"version":1,"include":["/api/*/x"]}"#.as_slice(),
        ] {
            std::fs::write(dir.join("_routes.json"), bad).unwrap();
            assert!(scan_assets_dir(&root, &assets(None)).is_err());
        }
        std::fs::write(
            dir.join("_routes.json"),
            br#"{"version":1,"include":["/*"]}"#,
        )
        .unwrap();
        // Over the provider's parse cap, even though the generic asset
        // cap is larger.
        let mut big = String::from("/*\n");
        while big.len() <= vrt_provider::assets_headers::MAX_FILE_BYTES {
            big.push_str("# padding padding padding padding padding padding\n");
        }
        std::fs::write(dir.join("_headers"), big.as_bytes()).unwrap();
        assert!(scan_assets_dir(&root, &assets(None)).is_err());
        std::fs::write(dir.join("_headers"), b"/*\n  X-Ok: yes\n").unwrap();

        // The upload path applies the same validation.
        use base64::Engine;
        let bad = b"/old /new 301\n\xff\n".to_vec();
        let sha = hex::encode(sha2::Sha256::digest(&bad));
        let manifest = serde_json::json!({
            "files": [{
                "mime": "text/plain",
                "path": "/_redirects",
                "sha256": sha,
                "size": bad.len(),
            }],
            "htmlHandling": "auto-trailing-slash",
            "notFoundHandling": "none",
            "schemaVersion": 1,
        });
        let mut objects = BTreeMap::new();
        objects.insert(sha, base64::engine::general_purpose::STANDARD.encode(&bad));
        let upload = UploadAssets {
            manifest: base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&manifest).unwrap()),
            objects,
        };
        assert!(upload_assets_verified(&upload).is_err());
        // A legitimate deployment still verifies (digest-keyed objects).
        let pending_ok = upload_assets_verified(&UploadAssets {
            manifest: base64::engine::general_purpose::STANDARD.encode(&pending.manifest_bytes),
            objects: pending
                .objects
                .iter()
                .map(|(d, b)| {
                    (
                        d.clone(),
                        base64::engine::general_purpose::STANDARD.encode(b),
                    )
                })
                .collect(),
        });
        assert!(pending_ok.is_ok());
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[cfg(test)]
mod quotas_tests {
    use super::*;

    /// `[quotas]` is a partial override: an absent dimension takes the
    /// platform default (one shared source of truth), so
    /// `[quotas] cpuMs = 100` never forces the author to restate memory
    /// or subrequests.
    #[test]
    fn partial_quotas_normalize_to_platform_defaults() {
        let q: QuotasToml = toml::from_str("cpuMs = 100").unwrap();
        assert_eq!(q.memory_mb, WorkerQuotas::platform_default().memory_mb);
        assert_eq!(q.subrequests, WorkerQuotas::platform_default().subrequests);
        let out = normalize_quotas(Some(q)).unwrap();
        assert_eq!(out.cpu_ms, 100);
        assert_eq!(
            out,
            WorkerQuotas {
                cpu_ms: 100,
                ..WorkerQuotas::platform_default()
            }
        );
    }

    /// Absent `[quotas]` = platform defaults (stock workerd behaviour),
    /// not a zeroed limit.
    #[test]
    fn absent_quotas_are_platform_defaults() {
        assert_eq!(
            normalize_quotas(None).unwrap(),
            WorkerQuotas::platform_default()
        );
    }

    /// Default asset caps follow Cloudflare's documented Workers limits
    /// (20,000 files on the free plan, 25 MiB per file). The total is a
    /// local packer guard, not a Cloudflare limit.
    #[test]
    fn asset_limits_default_to_cloudflare_limits() {
        let limits = AssetLimits::default();
        assert_eq!(limits.max_files, 20_000);
        assert_eq!(limits.max_file_bytes, 25 * 1024 * 1024);
        assert_eq!(limits.max_total_bytes, 512 * 1024 * 1024);
    }

    /// Out-of-bounds values name the offending field — a deploy-time
    /// error instead of a generic protocol rejection.
    #[test]
    fn out_of_bounds_quotas_name_the_field() {
        for (toml_src, field) in [
            ("cpuMs = 0", "quotas.cpuMs"),
            ("cpuMs = 300001", "quotas.cpuMs"),
            ("memoryMb = 0", "quotas.memoryMb"),
            ("memoryMb = 4097", "quotas.memoryMb"),
            ("subrequests = 10001", "quotas.subrequests"),
            ("outboundConnections = 1025", "quotas.outboundConnections"),
        ] {
            let q: QuotasToml = toml::from_str(toml_src).unwrap();
            match normalize_quotas(Some(q)) {
                Err(LocalError::Worker(field_name)) => assert_eq!(field_name, field, "{toml_src}"),
                other => panic!("{toml_src}: expected Worker({field}), got {other:?}"),
            }
        }
    }
}

#[cfg(test)]
mod tail_consumer_tests {
    use super::*;

    fn list(names: &[&str]) -> Vec<TailConsumerToml> {
        names
            .iter()
            .map(|s| TailConsumerToml {
                service: (*s).to_string(),
            })
            .collect()
    }

    fn resolve(name: &str) -> Option<String> {
        Some(format!("w-{name}"))
    }

    /// Both lists share this resolver: self-targets are refused (no
    /// recursion bound), duplicates collapse, and the cap is enforced
    /// on the RESOLVED list.
    #[test]
    fn shared_invariants_hold_for_both_lists() {
        let got =
            resolve_tail_consumers(&list(&["a", "a", "b"]), "producer", "tail", &resolve).unwrap();
        assert_eq!(
            got.iter().map(|c| c.worker_id.as_str()).collect::<Vec<_>>(),
            vec!["w-a", "w-b"]
        );

        let err =
            resolve_tail_consumers(&list(&["producer"]), "producer", "tail", &resolve).unwrap_err();
        assert!(format!("{err:?}").contains("another worker"), "{err:?}");

        let many: Vec<TailConsumerToml> = (0..17)
            .map(|i| TailConsumerToml {
                service: format!("c{i}"),
            })
            .collect();
        let err =
            resolve_tail_consumers(&many, "producer", "streaming tail", &resolve).unwrap_err();
        assert!(format!("{err:?}").contains("16"), "{err:?}");
    }
}

mod module_rules_tests {
    use super::*;

    /// Mirrors `glob-to-regexp` ({globstar:true}) behavior — the
    /// oracle for every case below is wrangler's own regex.
    #[test]
    fn glob_match_semantics() {
        let cases: &[(&str, &[&str], &[&str])] = &[
            // `**/*.txt` — globstar crosses dirs AND matches root.
            (
                "**/*.txt",
                &["a.txt", "d/a.txt", "d/e/a.txt"],
                &["a.txt2", "a.md", "d/a.txt.bak"],
            ),
            // `*.txt` — single star never crosses `/`.
            ("*.txt", &["a.txt"], &["d/a.txt", "a.txt2"]),
            (
                "lang/*.mjs",
                &["lang/a.mjs"],
                &["lang/d/a.mjs", "xlang/a.mjs"],
            ),
            // `?` is a LITERAL char (no `extended`) — `*.wasm?module`
            // matches the query-suffixed name only.
            (
                "**/*.wasm?module",
                &["a.wasm?module", "d/a.wasm?module"],
                &["a.wasmXmodule", "a.wasm"],
            ),
            // `[...]`/`{a,b}` are literal too.
            ("a[0-9].txt", &["a[0-9].txt"], &["a5.txt"]),
            ("{a,b}.txt", &["{a,b}.txt"], &["a.txt", "b.txt"]),
            // `a/**/b` — globstar consumes whole segments only; it may
            // not stop mid-segment.
            ("a/**/b", &["a/b", "a/x/b", "a/x/y/b"], &["a/xb", "a/bc"]),
            // `a/**/x` cannot stop mid-segment (`a/xyz` has no stop
            // where `x` aligns at a boundary).
            ("a/**/x", &["a/x", "a/d/x"], &["a/xyz", "a/xy/xz"]),
            // trailing `**` eats the rest including `/`.
            ("a/**", &["a/b", "a/x/y"], &["a", "ab"]),
            // `**` mid-segment collapses to a single `*`.
            ("a**b", &["axb", "ab"], &["a/x/b"]),
            // `.`/`+`/`(`... are literal.
            ("a+.txt", &["a+.txt"], &["aa.txt", "ax.txt"]),
            // `\.` escapes to a literal dot.
            ("a\\.txt", &["a.txt"], &["axtxt"]),
        ];
        for (pat, yes, no) in cases {
            for p in *yes {
                assert!(glob_match(pat, p), "{pat} should match {p}");
            }
            for p in *no {
                assert!(!glob_match(pat, p), "{pat} should NOT match {p}");
            }
        }
    }

    fn rules(src: &str) -> Vec<ModuleRuleToml> {
        let doc: toml::Value = toml::from_str(src).unwrap();
        let json = serde_json::to_value(&doc).unwrap();
        serde_json::from_value(json["moduleRules"].clone()).unwrap()
    }

    /// CF `parseRules`: a non-fallthrough rule completes its type —
    /// later same-type rules (incl. the implicit defaults) are dead.
    #[test]
    fn per_type_pruning() {
        let user = rules(
            r#"moduleRules = [
                { type = "Text", globs = ["**/*.md"] },
            ]"#,
        );
        let eff = effective_module_rules(&user).unwrap();
        // User Text rule completed `text` — the implicit Text default
        // is gone; Data/Wasm defaults remain.
        assert_eq!(
            eff.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            ["text", "data", "wasm"]
        );
        // With fallthrough the default survives and .txt still types.
        let user_ft = rules(
            r#"moduleRules = [
                { type = "Text", globs = ["**/*.md"], fallthrough = true },
            ]"#,
        );
        let eff = effective_module_rules(&user_ft).unwrap();
        assert_eq!(
            eff.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            ["text", "text", "data", "wasm"]
        );
        // `fallthrough = false` completes the type exactly like unset.
        let user_ff = rules(
            r#"moduleRules = [
                { type = "Text", globs = ["**/*.md"], fallthrough = false },
            ]"#,
        );
        assert_eq!(effective_module_rules(&user_ff).unwrap().len(), 3);
    }

    fn meta(src: &str) -> WorkerToml {
        toml::from_str(&format!(
            "name = \"w\"\nmainModule = \"index.mjs\"\ncompatibilityDate = \"2025-01-01\"\n{src}"
        ))
        .unwrap()
    }

    fn pack_modules(meta: &WorkerToml, files: &[(&str, &[u8])]) -> serde_json::Value {
        let files: BTreeMap<String, Vec<u8>> = files
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.to_vec()))
            .collect();
        build_pack_json(meta, &files).unwrap().0["modules"].clone()
    }

    #[test]
    fn rules_classify_gathered_files() {
        let m = meta(
            // `fallthrough = true` keeps the implicit Text default
            // alive so `.html` still classifies (pruning semantics
            // are covered separately in `per_type_pruning`).
            r#"moduleRules = [
                { type = "Text", globs = ["**/*.md"], fallthrough = true },
                { type = "Data", globs = ["**/*.js"], fallthrough = true },
            ]"#,
        );
        let mods = pack_modules(
            &m,
            &[
                ("index.mjs", b"export default {}".as_slice()),
                ("readme.md", b"# hi".as_slice()),
                ("data.js", b"1".as_slice()),
                ("page.html", b"<p/>".as_slice()), // implicit Text default
                ("raw.bin", b"\x01".as_slice()),   // implicit Data default
                ("lib.mjs", b"export {}".as_slice()),
            ],
        );
        assert_eq!(mods["readme.md"], serde_json::json!({"text": "# hi"}));
        assert_eq!(mods["data.js"], serde_json::json!({"data_b64": "MQ=="}));
        assert_eq!(mods["page.html"], serde_json::json!({"text": "<p/>"}));
        assert_eq!(mods["raw.bin"], serde_json::json!({"data_b64": "AQ=="}));
        // Extension table still covers unmatched files; the entry is
        // never rule-typed even though `**/*.js`-style globs exist.
        assert_eq!(mods["lib.mjs"], serde_json::json!("export {}"));
        assert_eq!(mods["index.mjs"], serde_json::json!("export default {}"));
    }

    /// The entry is never retyped by rules — `{ Data: ["**/*.mjs"] }`
    /// cannot turn the main module into a data blob (CF: the entry is
    /// the bundle, not an external module).
    #[test]
    fn entry_skips_rule_typing() {
        let m = meta(r#"moduleRules = [{ type = "Data", globs = ["**/*.mjs"] }]"#);
        let mods = pack_modules(
            &m,
            &[
                ("index.mjs", b"export default {}".as_slice()),
                ("lib.mjs", b"export {}".as_slice()),
            ],
        );
        assert_eq!(mods["index.mjs"], serde_json::json!("export default {}"));
        assert_eq!(
            mods["lib.mjs"],
            serde_json::json!({"data_b64": "ZXhwb3J0IHt9"})
        );
    }

    /// `base_dir` scopes rule evaluation (CF `moduleRoot`): a file
    /// outside the root is not a candidate and falls back to the
    /// extension table.
    #[test]
    fn base_dir_scopes_rule_evaluation() {
        let m = toml::from_str::<WorkerToml>(
            "name = \"w\"\nmainModule = \"src/index.mjs\"\ncompatibilityDate = \"d\"\nbaseDir = \"src\"\nmoduleRules = [{ type = \"Data\", globs = [\"*.mjs\"] }]",
        )
        .unwrap();
        let mods = pack_modules(
            &m,
            &[
                ("src/index.mjs", b"export default {}".as_slice()),
                ("src/lib.mjs", b"export {}".as_slice()),
                ("lib.mjs", b"export {}".as_slice()),
            ],
        );
        // `src/lib.mjs` evaluates as `lib.mjs` — `*.mjs` matches.
        assert_eq!(
            mods["src/lib.mjs"],
            serde_json::json!({"data_b64": "ZXhwb3J0IHt9"})
        );
        // `lib.mjs` at the project root is outside `src/` — not a
        // candidate, extension table types it.
        assert_eq!(mods["lib.mjs"], serde_json::json!("export {}"));
        assert_eq!(
            mods["src/index.mjs"],
            serde_json::json!("export default {}")
        );
    }

    /// Without `base_dir`, the `main` directory is the rule root.
    #[test]
    fn main_dir_is_the_default_rule_root() {
        let m = toml::from_str::<WorkerToml>(
            "name = \"w\"\nmainModule = \"src/index.mjs\"\ncompatibilityDate = \"d\"\nmoduleRules = [{ type = \"Data\", globs = [\"*.txt\"] }]",
        )
        .unwrap();
        let mods = pack_modules(
            &m,
            &[
                ("src/index.mjs", b"export default {}".as_slice()),
                ("src/a.txt", b"x".as_slice()),
                ("b.txt", b"x".as_slice()),
            ],
        );
        // `src/a.txt` → `a.txt` matches `*.txt`; root `b.txt` is
        // outside `src/` but the implicit Text default still types it
        // (its `**/*.txt` crosses the boundary? no — `b.txt` is
        // outside the root, so NO rule sees it → extension table).
        assert_eq!(mods["src/a.txt"], serde_json::json!({"data_b64": "eA=="}));
        assert_eq!(mods["b.txt"], serde_json::json!({"text": "x"}));
    }

    /// Invalid rule types in a native `moduleRules` fail closed at the
    /// pack boundary even without normalize having run.
    #[test]
    fn bad_rule_types_fail_closed() {
        let m = meta(r#"moduleRules = [{ type = "PythonModule", globs = ["**/*.py"] }]"#);
        let files: BTreeMap<String, Vec<u8>> =
            [("index.mjs".to_string(), b"export default {}".to_vec())].into();
        assert!(build_pack_json(&m, &files).is_err());
    }
}
