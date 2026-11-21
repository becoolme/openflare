//! `compute.desired` snapshot payload: the control plane's desired-state
//! document for one runtime cell.
//!
//! One envelope addressed `kind = "compute.desired"`, `scope = "cell:<cellId>"`
//! carries the complete replacement set of tenant/worker assignments for
//! that cell — never a partial patch, matching the platform invariant that a
//! snapshot contains the full state for its scope (see
//! `doc/platform/control-plane-signed-snapshots.md`).
//!
//! Secrets never appear in this payload: `Secret` bindings carry only a
//! `secretRef` the host resolves separately from encrypted storage, and
//! `Var` bindings carry a `varRef` for the same reason (values are delivered
//! out-of-band per version — see the Phase 0 report §B.3 "vars refs (no values)").
//!
//! All fields are required on the wire: `#[serde(deny_unknown_fields)]` plus
//! no `Option` fields, so an unknown or missing member fails closed at typed
//! decode time, before [`ComputeDesired::validate`] runs.

use crate::envelope::{verify, ExpectedScope, RevisionGuard, TrustedKey, Verified};
use crate::error::SnapshotError;
use crate::strict::MAX_SAFE_INTEGER;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Snapshot kind this payload is carried under.
pub const KIND: &str = "compute.desired";
/// Payload `schemaVersion` produced and accepted by this implementation.
pub const SCHEMA_VERSION: u8 = 1;

/// Maximum tenants in one snapshot (same bound as the gateway payload).
pub const MAX_TENANTS: usize = 512;
/// Maximum workers per tenant entry.
pub const MAX_WORKERS_PER_TENANT: usize = 256;
/// Maximum bindings per worker.
pub const MAX_BINDINGS_PER_WORKER: usize = 64;
/// Maximum routes per worker.
pub const MAX_ROUTES_PER_WORKER: usize = 64;
/// Maximum cron triggers per worker.
pub const MAX_CRONS_PER_WORKER: usize = 32;
/// T37b: the max number of queue consumers a worker may declare (same order of magnitude as the bindings
/// capacity; the CF platform constraint is far below this).
const MAX_QUEUE_CONSUMERS_PER_WORKER: usize = 100;
/// P3c-1: the max number of DO classes a worker may declare (same order of magnitude as the bindings capacity;
/// CF has no explicit limit, a conservative cap is chosen to prevent snapshot bloat).
const MAX_DO_CLASSES_PER_WORKER: usize = 64;
const MAX_WORKFLOWS_PER_WORKER: usize = 32;
/// CF `limits.steps` bound — wrangler accepts positive ints; we cap
/// conservatively (engine default is effectively unbounded).
const MAX_WORKFLOW_STEPS: u32 = 100_000;
/// 26-char lowercase alphanumeric id shared by queue/databaseId (vrt-sql path rules).
fn valid_queue_id(id: &str) -> bool {
    id.len() == 26
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}
/// Maximum compatibility flags per version.
pub const MAX_COMPAT_FLAGS: usize = 32;
/// RV-R53: maximum deletion tombstones in one snapshot. Bounded so the
/// signed document stays well inside the envelope byte budget; entries
/// re-emit until the cell ACKs their revision, so a full list simply
/// extends cleanup across revisions.
pub const MAX_DELETIONS: usize = 256;

/// Maximum platform identifier length (same charset/length rule as the
/// gateway's `valid_identifier`).
pub const MAX_IDENTIFIER_LEN: usize = 128;
/// Maximum `varRef`/`secretRef` length.
pub const MAX_REF_LEN: usize = 256;
/// Maximum worker name length (DNS label).
pub const MAX_WORKER_NAME_LEN: usize = 63;
/// Maximum serialized size of a `{"type":"json"}` binding payload
/// (version_metadata objects are ~100 B; typed vars stay far under).
pub const MAX_JSON_BINDING_BYTES: usize = 16 * 1024;
/// Maximum binding name length (JS identifier).
pub const MAX_BINDING_NAME_LEN: usize = 64;
/// Maximum route hostname length (DNS).
pub const MAX_HOSTNAME_LEN: usize = 253;
/// Maximum compatibility-flag token length.
pub const MAX_COMPAT_FLAG_LEN: usize = 64;
/// Maximum cron expression length.
pub const MAX_CRON_EXPR_LEN: usize = 128;

/// `quotas.cpuMs` bounds (isolate CPU milliseconds per request).
pub const MIN_CPU_MS: u32 = 1;
/// `quotas.cpuMs` upper bound.
pub const MAX_CPU_MS: u32 = 300_000;
/// `quotas.memoryMb` bounds (per-isolate memory, MiB).
pub const MIN_MEMORY_MB: u32 = 1;
/// `quotas.memoryMb` upper bound.
pub const MAX_MEMORY_MB: u32 = 4096;
/// `quotas.subrequests` bounds (outbound subrequests per request; 0 disables).
pub const MAX_SUBREQUESTS: u32 = 10_000;
/// `quotas.outboundConnections` bounds (simultaneous outbound connections).
pub const MAX_OUTBOUND_CONNECTIONS: u32 = 1024;

/// Platform default CPU budget (ms) — the value a worker that never
/// declared `limits` carries. One source of truth for the control
/// plane, `vrt-local` and `vrt-config` (a default that disagreed
/// between them would silently turn into an enforced limit).
pub const DEFAULT_CPU_MS: u32 = 50;
/// Platform default isolate memory (MiB); matches Cloudflare's 128 MB
/// isolate.
pub const DEFAULT_MEMORY_MB: u32 = 128;
/// Platform default outbound subrequests per request.
pub const DEFAULT_SUBREQUESTS: u32 = 50;
/// Platform default simultaneous outbound connections.
pub const DEFAULT_OUTBOUND_CONNECTIONS: u32 = 16;

/// The `compute.desired` payload: complete desired state for one cell.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComputeDesired {
    /// Payload schema version; must equal [`SCHEMA_VERSION`]. New payload
    /// fields require bumping this version so older cells fail closed
    /// instead of silently ignoring semantics.
    pub schema_version: u8,

    /// Runtime cell this document addresses. Must equal the `cell:<cellId>`
    /// suffix of the envelope `scope`; [`verify_compute_desired`] enforces
    /// the binding so a snapshot issued for cell A cannot be fed to cell B.
    pub cell_id: String,

    /// Payload copy of the envelope revision. Must equal the envelope's
    /// `revision` (same binding technique as the gateway's bootstrap
    /// receipt, where `current.revision` must equal the envelope revision).
    pub revision: u64,

    /// Per-tenant desired state; a complete replacement set for the cell.
    pub tenants: Vec<TenantDesired>,

    /// RV-R53: signed deletion tombstones — the ONLY authority under
    /// which a cell may destroy durable data. An absent field (old
    /// control plane) deserializes to empty: every absence from
    /// `tenants` then means "not serving here — RETAIN all local and
    /// object-store data", never delete. `skip_serializing_if` keeps
    /// tombstone-free snapshots byte-identical (old cells reject the
    /// field via `deny_unknown_fields` — fail closed: destruction is
    /// delayed until the cell binary understands tombstones).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletions: Vec<DeletionDesired>,
}

/// Desired state for one tenant (platform team) on the cell.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TenantDesired {
    /// Platform team/tenant identifier. Identifier charset
    /// (`[A-Za-z0-9_.:-]`), at most [`MAX_IDENTIFIER_LEN`].
    pub tenant_id: String,

    /// Workers assigned to this tenant on this cell.
    pub workers: Vec<WorkerDesired>,
}

/// RV-R53: one signed deletion tombstone. Scope:
/// - `workerId` absent → the whole tenant's cell-local DO storage;
/// - `workerId` present + `classKey` absent → that worker's DO dirs;
/// - `classKey` present → exactly that class (`workerId` required).
/// A tombstone may only destroy data for the exact scope it names;
/// it NEVER implies deletion for anything not named — data of
/// workers/tenants merely absent from `tenants` is retained.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeletionDesired {
    /// Tenant (team) the deleted resource belonged to.
    pub tenant_id: String,
    /// Worker identity — absent for tenant-scoped deletion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// DO class `uniqueKey` (`^[0-9a-f]{64}$`) — absent for
    /// worker/tenant-scoped deletion. Requires `workerId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class_key: Option<String>,
    /// The `compute.desired` revision in which the control plane
    /// confirmed the deletion — lets cells ignore tombstones
    /// replayed from snapshots older than their last applied one.
    pub revision: u64,
}

/// Desired state for one worker replica assignment on this cell.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerDesired {
    /// Platform worker identifier; unique across the whole snapshot.
    pub worker_id: String,

    /// Worker name as declared at deploy time. Lowercase DNS label without
    /// consecutive dashes (the `--` sequence separates `<worker>--<team>` in
    /// public hostnames); unique per tenant.
    pub name: String,

    /// Version currently serving traffic for this worker.
    pub active_version: VersionDesired,

    /// Bindings declared for this worker; names are unique per worker and
    /// must be valid JS identifiers (they become `env.*` properties).
    pub bindings: Vec<BindingDesired>,

    /// Hostnames routed to this worker; globally unique across the
    /// snapshot.
    pub routes: Vec<RouteDesired>,

    /// Cron triggers; the cell controller fires each `(workerId,
    /// versionId, scheduledTime)` at most once.
    pub crons: Vec<CronDesired>,

    /// T37b: queue consumers -- which queues this worker consumes (CF semantics:
    /// a queue has at most one consumer globally; snapshot-level uniqueness is validated in
    /// [`ComputeDesired::validate`]). Default = no consumer --
    /// compatible with upgrading from old snapshots.
    #[serde(default)]
    pub queue_consumers: Vec<QueueConsumerDesired>,

    /// P3c-1: Durable Object classes this worker DECLARES (workerd
    /// `localDisk` storage). The set is version-scoped — migrations
    /// (`newSqliteClasses`/`newClasses`/`renamedClasses`/
    /// `deletedClasses`) are resolved at deploy into this list; a
    /// `renamed_classes` lineage shares the predecessor's `uniqueKey`
    /// so its data directory survives the rename. Old snapshots
    /// without the field deserialize to empty — upgrade-compatible;
    /// old cells reject a new field via `deny_unknown_fields` (fail
    /// closed, never silently drop DO declarations).
    #[serde(default)]
    pub durable_object_classes: Vec<DoClassDesired>,

    /// W1: `[[workflows]]` declarations. Skipped when empty so
    /// snapshots without workflows stay byte-identical to the TS
    /// canonical form (control-plane W2 hasn't learned the field);
    /// `deny_unknown_fields` on the struct makes OLD cells reject a
    /// snapshot that carries it (fail closed, never silently drop).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<WorkflowDesired>,

    /// Per-isolate/per-request resource limits enforced by the runtime.
    pub quotas: WorkerQuotas,

    /// Tail Workers (`tail_consumers`) — same-tenant workers whose
    /// `tail(events)` handler receives this worker's trace events.
    /// workerIds are resolved server-side at deploy like service
    /// bindings; skipped when empty so snapshots without consumers
    /// stay byte-identical (old cells reject the field via
    /// `deny_unknown_fields` — fail closed).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tail_consumers: Vec<TailConsumerDesired>,

    /// Observability (`[observability]`) — Worker Logs gates +
    /// head sampling applied by the platform `vrt-tail` collector.
    /// Skipped when absent so snapshots without the table stay
    /// byte-identical (old cells reject the field via
    /// `deny_unknown_fields` — fail closed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityDesired>,
}

/// `[observability]` — CF Worker Logs collection gates and head
/// sampling. All fields are EFFECTIVE values (wrangler-side defaults
/// and the deprecated top-level `head_sampling_rate` alias are folded
/// at parse). `traces`/`persist`/`destinations` have no counterpart
/// and are rejected upstream.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObservabilityDesired {
    /// Master gate (`observability.enabled`, default true).
    pub enabled: bool,
    /// Logs sub-gate (`observability.logs.enabled`, default true).
    /// `enabled && logsEnabled == false` => no collection at all —
    /// the cell emits no `vrt-tail` collector for the worker.
    pub logs_enabled: bool,
    /// Head sampling rate in parts-per-million [0,1_000_000]
    /// (`logs.headSamplingRate`, with the deprecated top-level alias
    /// folded in): fraction of INVOCATIONS whose records are kept,
    /// decided per-invocation — a dropped invocation keeps none of
    /// its log/exception/outcome records. Integer ppm because the
    /// signed wire's canonical JSON carries safe integers only.
    pub head_sampling_rate_ppm: u32,
    /// `observability.logs.invocation_logs` (default true) — false
    /// drops only the `outcome` (invocation) record; console and
    /// exception records still collect.
    pub invocation_logs: bool,
}

/// One `tail_consumers[]` entry — the CONSUMER of this worker's tail
/// events. Points at a worker of the same tenant by `workerId`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailConsumerDesired {
    /// Consumer worker identifier; must resolve to a worker in the
    /// SAME tenant's `workers` list — cross-tenant consumers fail
    /// closed (a worker's traces never leave its tenant).
    pub worker_id: String,
}

/// An immutable, content-addressed worker version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VersionDesired {
    /// Platform version identifier.
    pub version_id: String,

    /// sha256 of the version artifact bundle in object storage, lowercase
    /// hex (64 chars, no `sha256:` prefix). The host refuses to load an
    /// artifact whose content hash differs.
    pub artifact_digest: String,

    /// Cloudflare `compatibility_date`, strict `YYYY-MM-DD`.
    pub compatibility_date: String,

    /// Cloudflare `compatibility_flags` tokens.
    pub compatibility_flags: Vec<String>,
}

/// A worker binding. Internally tagged on `type`
/// (`{"type":"kv",...}`); each variant carries the JS binding `name` plus
/// exactly the resource reference for its kind. Only references appear
/// here — never secret material.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BindingDesired {
    /// Plaintext variable: reference into the per-version var store; the
    /// value itself is delivered out-of-band, never inside the snapshot.
    Var(VarBinding),
    /// Secret variable: reference into encrypted storage; the snapshot
    /// never carries the secret value.
    Secret(SecretBinding),
    /// Workers KV namespace owned by this tenant.
    Kv(KvBinding),
    /// R2 bucket owned by this tenant.
    R2(R2Binding),
    /// Hyperdrive-style Postgres database owned by this tenant.
    Pg(PgBinding),
    /// `{\"type\":\"hyperdrive\"}` — MIG-15.
    Hyperdrive(HyperdriveBinding),
    /// `{"type":"ratelimit"}` — MIG-16 in-process fixed-window limiter.
    Ratelimit(RatelimitBinding),
    /// Service binding to another worker of the SAME tenant.
    Service(ServiceBinding),
    /// Static assets manifest, content-addressed by digest.
    Assets(AssetsBinding),
    /// D1-style SQLite database (vrt-sql data plane, T34).
    D1(D1Binding),
    /// Queue producer binding (vrt-sql QueueBackend, T37).
    Queue(QueueBinding),
    /// Durable Object namespace binding (P3c-1) — `env.<name>` is the
    /// stub factory (`idFromName`/`get`); the namespace's SQLite
    /// storage lives under the declaring worker's `localDisk` dir.
    DurableObject(DoBindingDesired),
    /// `{"type":"workerLoader"}` — Dynamic Workers loader (`env.<name>`
    /// is a `WorkerLoader`: `.get(name, () => code)` / `.load(code)`).
    /// Points at no provider resource; the tenant supplies code at
    /// runtime. Rendered as workerd `workerLoader` (experimental).
    WorkerLoader(WorkerLoaderBinding),
    /// `{"type":"analyticsEngine"}` — Analytics Engine dataset
    /// (`env.<name>` is a `AnalyticsEngineDataset` with
    /// `writeDataPoint()`). Rendered as workerd `analyticsEngine`
    /// (experimental) targeting the tenant's AE ingest socket; the
    /// emitted service designator name IS the dataset name — workerd
    /// derives the event's `dataset` field from it.
    AnalyticsEngine(AnalyticsEngineBinding),
    /// `{"type":"json"}` — a static JSON payload bound under
    /// `env.<name>` (workerd `json` binding; `env.<name>` is the parsed
    /// value). Carries CF `version_metadata` (`{id, tag, timestamp}`)
    /// and typed `[vars]` values — the wire holds the serialized JSON
    /// text verbatim and the renderer emits it untouched.
    Json(JsonBinding),
    /// `{"type":"unsafeEval"}` — CF `[[unsafe.bindings]]
    /// {type="unsafe_eval"}`: workerd's native `unsafeEval` binding —
    /// `env.<name>` is the capability enabling `eval()`/`new
    /// Function()`/`WebAssembly` inside the worker. It is the one raw
    /// binding type with no proper wrangler key that workerd runs;
    /// every other `unsafe.bindings` entry fails closed at parse.
    UnsafeEval(UnsafeEvalBinding),
}

/// `{"type":"var"}` — see [`BindingDesired::Var`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VarBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Reference into the per-version var store.
    pub var_ref: String,
}

/// `{"type":"secret"}` — reference only, never the value.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Reference into encrypted secret storage.
    pub secret_ref: String,
}

/// Access grant for a binding that has a provider data plane
/// (`kv`/`r2`/`pg`/`assets`). Required — the control plane states the
/// grant explicitly; `var`/`secret`/`service` bindings have no data-plane
/// grant to declare.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingPermissions {
    /// Read operations (kv get; r2 head/get/list; assets fetch; PG queries
    /// reading data).
    pub read: bool,
    /// Write operations (kv put/delete; r2 put/delete; PG queries
    /// mutating data). Always `false` for assets' current
    /// read-only surface.
    pub write: bool,
}

/// `{"type":"kv"}`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KvBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Platform-assigned KV namespace identifier.
    pub namespace_id: String,
    /// Access grant for this binding.
    pub permissions: BindingPermissions,
}

/// `{"type":"r2"}`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2Binding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Bucket name (lowercase DNS label, 3–63 chars).
    pub bucket: String,
    /// Access grant for this binding.
    pub permissions: BindingPermissions,
}

/// `{"type":"pg"}`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PgBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// `vrtpg:<databaseTenantId>` — server-minted tenant reference;
    /// ownership is assigned control-plane-side, never by the worker.
    pub database_id: String,
    /// P1: render-inputs credential key (`vrtpgcred:<id>`). Reference
    /// only — the token travels out-of-band in pg-credentials.json.
    /// Cells predating P1 reject this field via `deny_unknown_fields`
    /// (fail closed); serde default keeps pre-P1 snapshots loadable.
    #[serde(default)]
    pub credential_ref: String,
    /// Access grant for this binding.
    pub permissions: BindingPermissions,
}

/// `{"type":"hyperdrive"}` — MIG-15: Hyperdrive-compatible pg-wire
/// binding. The worker-visible value is a plain `json` env object
/// (`{connectionString, host, port, user, password, database}`) whose
/// host is the platform-minted `syntheticHost` — an unresolvable name
/// routed by the egress layer to the internal TCP target provider.
/// Real credentials never enter the isolate; `credentialRef` keys the
/// provider-held credential payload (same `vrtpgcred:` channel as PG).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HyperdriveBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Server-minted binding identity (`vrthd:<tenantId>:<name>`).
    pub binding_id: String,
    /// Synthetic internal hostname (`hd<hex24>.hyperdrive.vrt.internal`)
    /// the egress policy routes to the internal TCP provider.
    pub synthetic_host: String,
    /// Render-inputs credential key (`vrtpgcred:<id>`). Reference only.
    pub credential_ref: String,
    /// Access grant for this binding.
    pub permissions: BindingPermissions,
}

/// Max `ratelimit` bindings per worker (MIG-16).
pub const MAX_RATELIMIT_BINDINGS_PER_WORKER: usize = 16;
/// Max `limit` of a ratelimit binding (MIG-16).
pub const MAX_RATELIMIT_LIMIT: u64 = 1_000_000;

/// `{"type":"ratelimit"}` — MIG-16 Workers Rate Limiting binding. The
/// counter lives in the facade-ratelimit system worker (process local,
/// approximate); scope is derived from tenant + worker + `namespaceId`
/// at render time, never from tenant input. No permissions: it grants
/// access to no platform resource.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RatelimitBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Decimal digits, 1..=32 chars (CF `namespace_id`).
    pub namespace_id: String,
    /// Allowed calls per period, 1..=[`MAX_RATELIMIT_LIMIT`].
    pub limit: u64,
    /// Window length in seconds: 10 or 60 (CF rule).
    pub period: u64,
}

/// `{"type":"service"}` — same-tenant worker-to-worker binding.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Target worker identifier; must resolve to a worker in the SAME
    /// tenant's `workers` list — cross-tenant service bindings fail closed.
    pub worker_id: String,
    /// Named `WorkerEntrypoint` export of the target worker (wrangler
    /// `services[].entrypoint`). Absent = the default export; absent is
    /// also omitted on the wire, so legacy snapshots keep their bytes.
    /// JS identifier; `default` and the platform-reserved `Vrt`/`vrt`
    /// prefix are rejected (one canonical form for the default export,
    /// and no tenant binding onto a platform-injected entrypoint).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    /// Wrangler `services[].props` — JSON-serialized object delivered
    /// as `ctx.props` on every invocation through the binding (workerd
    /// `ServiceBindingDesignator.props.json`). Kept as TEXT (not
    /// `serde_json::Value`) for `Eq` + byte-stable round-trip; absent
    /// is omitted on the wire so legacy snapshots keep their bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub props: Option<String>,
}

/// Prefix of entrypoint names the platform injects into tenant workers
/// (e.g. the waitUntil probe). A tenant service binding may not name one.
pub const RESERVED_ENTRYPOINT_PREFIXES: [&str; 2] = ["Vrt", "vrt"];

/// Whether `ep` is an entrypoint a tenant service binding may target.
pub fn valid_service_entrypoint(ep: &str) -> bool {
    valid_binding_name(ep)
        && ep != "default"
        && !RESERVED_ENTRYPOINT_PREFIXES.iter().any(|p| ep.starts_with(p))
}

/// `{"type":"d1"}` — server-assigned database identity + writer epoch.
/// `databaseId` is minted by the control plane; a Worker can never
/// supply it (signature covers it, provider maps binding→databaseId).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct D1Binding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Server-side database id (`^[a-z0-9]{26}$`, validated in vrt-sql).
    pub database_id: String,
    /// Current writer epoch (T34: fixed 1; fencing lands with leases).
    #[serde(default = "default_d1_epoch")]
    pub epoch: u64,
    /// Access grant for this binding.
    pub permissions: BindingPermissions,
}

fn default_d1_epoch() -> u64 {
    1
}

/// `{"type":"queue"}` — server-assigned queue identity (T37). `queueId`
/// is minted by the control plane (same `^[a-z0-9]{26}$` namespace as
/// databaseId); `queueName` is the worker-facing broker name — routing
/// always uses `queueId`, never the request-supplied name.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Platform queue id (`^[a-z0-9]{26}$`).
    pub queue_id: String,
    /// Worker-facing queue name emitted into workerd's `queue` binding.
    pub queue_name: String,
    /// Access grant for this binding.
    pub permissions: BindingPermissions,
}

/// `{"type":"durableObject"}` (P3c-1) — binds one DO class. The class
/// must be DECLARED by `serviceWorkerId`'s worker (self when absent) —
/// resolution checked tenant-wide in [`ComputeDesired::validate`].
/// `uniqueKey` lives on the declaration ([`DoClassDesired`]), never on
/// the binding: two bindings to the same class share one namespace.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DoBindingDesired {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Exported JS class name (JS identifier, ≤128 chars).
    pub class_name: String,
    /// wrangler `script_name` — a SAME-TENANT worker declaring
    /// `class_name`; its identity is resolved control-plane-side
    /// (never a client-supplied storage id). `None` = own worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_worker_id: Option<String>,
}

/// `{"type":"workerLoader"}` — a Dynamic Workers `WorkerLoader`
/// binding. The worker supplies dynamic code at runtime through the
/// binding's `get()`/`load()` callbacks; the binding itself carries no
/// resource reference — the loader's live-worker bound is a platform
/// constant emitted at capnp render time.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerLoaderBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
}

/// `{"type":"analyticsEngine"}` — an Analytics Engine dataset binding.
/// `dataset` becomes both the workerd service-designator name (the
/// event's `dataset` field) and the per-tenant spool/parquet
/// partition, so it is charset-bound at validation
/// (`[A-Za-z0-9_]{1,256}`) — safe as a capnp service name, an
/// `--external-addr` token, and a filesystem path segment.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnalyticsEngineBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Dataset name — wrangler `dataset`, defaults to the binding name
    /// at project-config parse (mirrors CF).
    pub dataset: String,
}

/// `{"type":"json"}` — see [`BindingDesired::Json`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JsonBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// Serialized JSON text — must parse and stay within
    /// [`MAX_JSON_BINDING_BYTES`]; emitted verbatim into workerd's
    /// `json` binding so `env.<name>` is the parsed value.
    pub value: String,
}

/// `{"type":"unsafeEval"}` — see [`BindingDesired::UnsafeEval`].
/// Carries no resource reference; the binding name is the only field.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnsafeEvalBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
}

/// P3c-1 `durableObjectClasses[]`: one DO class declared by this worker.
/// Corresponds to wrangler `new_sqlite_classes` (`enableSql=true`) or
/// `new_classes` (`enableSql=false` -- under localDisk both use the same sqlite
/// backend, and `enableSql` only controls whether `state.storage.sql` is exposed, aligning with CF's
/// KV/SQLite backend semantics).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DoClassDesired {
    /// Exported JS class name; unique per worker (JS identifier).
    pub class_name: String,
    /// Server-minted storage identity, `^[0-9a-f]{64}$` — HMAC over
    /// (tenant, worker, class) derived from deployment-secret key
    /// material (doc §6). Determines the per-class subdirectory under
    /// the worker's `localDisk` dir; stable across generations,
    /// versions and `renamed_classes` lineages. NEVER user input.
    pub unique_key: String,
    /// `true` → `ctx.storage.sql.*` surface enabled (SQLite classes);
    /// `false` → KV surface only (KV classes on the same sqlite store).
    pub enable_sql: bool,
    /// DO1 `transferred_classes` lineage marker — the worker whose
    /// DO-storage segment may still hold the data dir for
    /// `unique_key`. The cell MUST move
    /// `<tenant>/<from_worker_id>/<unique_key>` to this worker's
    /// segment (same-tenant atomic rename, epoch-fenced) before
    /// serving the class; absent → no transfer lineage. Old cells
    /// `deny_unknown_fields` → fail closed on this field, never open
    /// an empty dir (rollback-safe).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_worker_id: Option<String>,
}

/// W1: one wrangler `[[workflows]]` declaration, normalized. The
/// workflows Engine is a Durable Object class named `Engine` owned by
/// an internal per-workflow service worker; `unique_key` is its
/// server-minted storage identity (same HMAC domain as DO classes,
/// `vrt-wf:` prefix) — NEVER user input. W7: `script_name` may
/// point at ANOTHER same-tenant worker (`service_worker_id`,
/// resolved server-side like [`DoBindingDesired::service_worker_id`]);
/// the engine service, DO storage, loopback and ops facade then live
/// under the DEFINING worker — CF's `script_name` ownership rule.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowDesired {
    /// Workflow name (wrangler `name`), `^[A-Za-z0-9_-]{1,64}$`.
    /// Unique per worker; also emitted as the engine's WORKFLOW_NAME.
    pub name: String,
    /// env binding name (wrangler `binding`), JS identifier; unique
    /// against every other binding on the worker.
    pub binding: String,
    /// Exported `WorkflowEntrypoint` class name in the defining
    /// worker (`service_worker_id` target, else this worker).
    pub class_name: String,
    /// wrangler `script_name` — a SAME-TENANT worker exporting
    /// `class_name`; server-resolved, `None` = own worker. Serialized
    /// only when set: pre-W7 cells parse same-worker specs unchanged
    /// and fail closed on `deny_unknown_fields` for cross-worker ones
    /// (deploy also gates on the `workflowsCrossWorker` capability).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_worker_id: Option<String>,
    /// Server-minted storage identity for the Engine DO namespace,
    /// `^[0-9a-f]{64}$` — HMAC over (tenant, worker, workflow name).
    /// Determines the engine subdirectory under the declaring
    /// worker's `localDisk` dir; stable across generations.
    pub unique_key: String,
    /// wrangler `limits.steps` — emitted as the engine's STEP_LIMIT
    /// json binding when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits_steps: Option<u32>,
}

/// T37b `queueConsumers[]`: a worker's consumption relationship to a queue (not a binding --
/// it does not appear in `env.*` and only drives the dispatcher to deliver messages to the worker's
/// `queue(batch, env, ctx)` handler). Numeric bounds were checked against miniflare
/// `QueueConsumerOptionsSchema` (broker.worker.js:57-70):
/// `maxBatchSize`/`maxBatchTimeout`/`maxRetries`/`retryDelay`/`deadLetterQueue`;
/// `maxConcurrency` is the platform dispatch concurrency cap (miniflare does not model it, CF docs cap it at
/// 250 -- this field's cap is relaxed to 1000, see section 8 for platform semantics).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueueConsumerDesired {
    /// Platform queue id (`^[a-z0-9]{26}$`).
    pub queue_id: String,
    /// Queue display name -- the first argument of `.queue(queueName)` and `batch.queue`.
    pub queue_name: String,
    /// Dead-letter queue id after consumption failures exceed the limit (another queue, same team -- already
    /// validated by the upstream control plane; here we check id shape and self-reference).
    pub dead_letter_queue_id: Option<String>,
    /// Batch size 1..=100 (miniflare allows 0 -- 0 never dispatches and is a config error,
    /// rejected fail closed). Default 5.
    pub max_batch_size: Option<u32>,
    /// Seconds to wait for a batch to fill, 0..=60. Default 1.
    pub max_batch_timeout_secs: Option<u32>,
    /// Number of extra retries 0..=100 (total delivery cap = maxRetries+1). Default 2.
    pub max_retries: Option<u32>,
    /// Retry delay in seconds after consumption failure, 0..=86400. Default 0.
    pub retry_delay_secs: Option<u32>,
    /// Cap on in-flight batches for this queue, 1..=1000. Default 10.
    pub max_concurrency: Option<u32>,
}

/// `{"type":"assets"}`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetsBinding {
    /// Binding name (`env.<name>`), JS identifier.
    pub name: String,
    /// sha256 of the assets manifest, lowercase hex (64 chars).
    pub manifest_digest: String,
    /// Access grant for this binding (assets are read-only today).
    pub permissions: BindingPermissions,
    /// T31c: Cloudflare-compatible assets-first routing opt-in. Absent
    /// (every manifest published before T31c) keeps the historical
    /// worker-first wiring: the ingress socket targets the user
    /// worker directly and assets are only reachable through
    /// `env.ASSETS`. `true` emits a `router-<workerId>` system worker
    /// between socket and user worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets_first: Option<bool>,
    /// T31c: `run_worker_first` — `true` invokes the user worker
    /// before any asset lookup; a list of `/`-prefixed glob rules
    /// routes matching paths worker-first while `!`-prefixed rules
    /// route assets-first. Only meaningful with `assets_first`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_worker_first: Option<RunWorkerFirst>,
}

/// T31c: `run_worker_first` — boolean switch or glob-rule list,
/// mirroring Wrangler/Miniflare `static_routing` semantics.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum RunWorkerFirst {
    /// `runWorkerFirst = <bool>`.
    Bool(bool),
    /// `runWorkerFirst = ["/path", "!/asset-path", …]`.
    Rules(Vec<String>),
}

/// Wrangler bound: at most 100 `run_worker_first` rules.
pub const MAX_RUN_WORKER_FIRST_RULES: usize = 100;
/// Wrangler bound: each rule at most 100 bytes.
pub const MAX_RUN_WORKER_FIRST_RULE_LEN: usize = 100;

/// Validate one `run_worker_first` rule set (Wrangler semantics):
/// every rule starts `/` or `!/`; ≤100 rules; ≤100 bytes each; no
/// duplicates; at least one non-`!` rule when `!` rules are present.
pub fn valid_run_worker_first_rules(rules: &[String]) -> bool {
    if rules.is_empty() || rules.len() > MAX_RUN_WORKER_FIRST_RULES {
        return false;
    }
    let mut seen = std::collections::HashSet::with_capacity(rules.len());
    let mut has_negative = false;
    let mut has_positive = false;
    for rule in rules {
        if rule.is_empty()
            || rule.len() > MAX_RUN_WORKER_FIRST_RULE_LEN
            || !rule.bytes().all(|b| (0x20..=0x7e).contains(&b))
        {
            return false;
        }
        let body = if let Some(rest) = rule.strip_prefix('!') {
            has_negative = true;
            rest
        } else {
            has_positive = true;
            rule.as_str()
        };
        if !body.starts_with('/') {
            return false;
        }
        if !seen.insert(rule.as_str()) {
            return false;
        }
    }
    !has_negative || has_positive
}

impl AssetsBinding {
    /// Whether the worker should get a `router-<id>` service — the
    /// Cloudflare assets-first path. Absent field ⇒ legacy wiring.
    pub fn assets_first_enabled(&self) -> bool {
        self.assets_first == Some(true)
    }
}

/// One public route for a worker.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteDesired {
    /// Lowercase DNS hostname, no wildcards; globally unique across the
    /// snapshot so one hostname can never fan out to two workers.
    pub hostname: String,
}

/// One cron trigger for a worker.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronDesired {
    /// Control-plane schedule identifier (idempotency/audit anchor).
    pub schedule_id: String,
    /// Standard 5-field cron expression
    /// (`minute hour day-of-month month day-of-week`).
    pub expression: String,
}

/// Per-isolate resource limits enforced by the runtime (W2 executor
/// limits); bounds here are protocol-level sanity caps — the control plane
/// applies stricter per-plan values.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerQuotas {
    /// CPU milliseconds per request.
    pub cpu_ms: u32,
    /// Isolate memory in MiB.
    pub memory_mb: u32,
    /// Outbound subrequests per request.
    pub subrequests: u32,
    /// Simultaneous outbound connections.
    pub outbound_connections: u32,
}

impl WorkerQuotas {
    /// The platform default set — what a worker that never declared
    /// `limits` carries. Rendering treats these values as "not
    /// declared": no `limits` dimension is emitted for them, so an
    /// undeclared worker keeps stock workerd behaviour.
    pub const fn platform_default() -> Self {
        Self {
            cpu_ms: DEFAULT_CPU_MS,
            memory_mb: DEFAULT_MEMORY_MB,
            subrequests: DEFAULT_SUBREQUESTS,
            outbound_connections: DEFAULT_OUTBOUND_CONNECTIONS,
        }
    }

    /// True when every dimension equals [`Self::platform_default`].
    pub fn is_platform_default(&self) -> bool {
        self.cpu_ms == DEFAULT_CPU_MS
            && self.memory_mb == DEFAULT_MEMORY_MB
            && self.subrequests == DEFAULT_SUBREQUESTS
            && self.outbound_connections == DEFAULT_OUTBOUND_CONNECTIONS
    }

    /// First dimension that differs from [`Self::platform_default`], in
    /// render order. `None` = nothing was explicitly declared.
    pub fn declared_deviation(&self) -> Option<&'static str> {
        if self.cpu_ms != DEFAULT_CPU_MS {
            return Some("cpuMs");
        }
        if self.memory_mb != DEFAULT_MEMORY_MB {
            return Some("memoryMb");
        }
        if self.subrequests != DEFAULT_SUBREQUESTS {
            return Some("subrequests");
        }
        if self.outbound_connections != DEFAULT_OUTBOUND_CONNECTIONS {
            return Some("outboundConnections");
        }
        None
    }

    /// True when at least one dimension this runtime generation can
    /// render (`cpuMs`, `memoryMb`, `subRequests`) was declared, i.e.
    /// the cell must emit a `limits` block for this worker.
    ///
    /// `subrequests = 0` means "no subrequest budget" in the protocol
    /// but "unlimited" on the workerd side; it therefore needs no
    /// `limits` entry (omitted dimension = stock behaviour) and does not
    /// count as rendered.
    pub fn declares_enforceable_limit(&self) -> bool {
        (self.cpu_ms != DEFAULT_CPU_MS && self.cpu_ms > 0)
            || (self.memory_mb != DEFAULT_MEMORY_MB && self.memory_mb > 0)
            || (self.subrequests != DEFAULT_SUBREQUESTS && self.subrequests > 0)
    }

    /// True when a positive `outboundConnections` budget was declared.
    /// This dimension has no `limits` field in the pinned workerd fork
    /// yet (the connection-counter patch is deferred), so it can never
    /// be enforced — a declared value is refused instead of being
    /// silently dropped. `0` means "no declared budget" (stock
    /// behaviour) and is accepted.
    pub fn declares_unenforceable_limit(&self) -> bool {
        self.outbound_connections > 0 && self.outbound_connections != DEFAULT_OUTBOUND_CONNECTIONS
    }
}

/// Typed `compute.desired` validation error with stable `DESIRED_*` codes.
/// `&'static str` payloads are field identifiers, never input data.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DesiredError {
    /// `schemaVersion` is not [`SCHEMA_VERSION`].
    #[error("unsupported compute.desired schema version")]
    SchemaVersion,

    /// `revision` is zero or exceeds the JavaScript safe-integer range.
    #[error("payload revision is outside the accepted range")]
    InvalidRevision,

    /// A collection exceeds its size bound; payload is the field name.
    #[error("collection exceeds its capacity bound: {0}")]
    Capacity(&'static str),

    /// An identifier/ref field is malformed; payload is the field name.
    #[error("malformed identifier: {0}")]
    Identifier(&'static str),

    /// `name` is not a valid lowercase DNS label or contains `--`.
    #[error("invalid worker name")]
    WorkerName,

    /// A binding name is not a valid JS identifier.
    #[error("invalid binding name")]
    BindingName,

    /// A route hostname is malformed; payload is the field name.
    #[error("invalid hostname: {0}")]
    Hostname(&'static str),

    /// A value required to be unique appears twice; payload names the
    /// duplicate domain (`tenantId`, `workerId`, `workerName`,
    /// `bindingName`, `hostname`).
    #[error("duplicate value in a unique domain: {0}")]
    Duplicate(&'static str),

    /// A digest field is not 64 lowercase hex chars; payload is the field.
    #[error("malformed digest: {0}")]
    Digest(&'static str),

    /// `compatibilityDate` is not strict `YYYY-MM-DD`.
    #[error("invalid compatibility date")]
    CompatibilityDate,

    /// A compatibility flag is malformed.
    #[error("invalid compatibility flag")]
    CompatibilityFlag,

    /// A service binding targets a worker outside the same tenant.
    #[error("service binding target is not in the same tenant")]
    ServiceTarget,
    /// T21/F4: service bindings form a directed cycle (A→B→A) — the
    /// runtime cannot bound such recursion inside workerd, so cyclic
    /// graphs are rejected outright (fail closed, never served).
    #[error("service bindings form a cycle")]
    ServiceCycle,

    /// A `durableObject` binding references a class the target worker
    /// does not declare, or a `serviceWorkerId` outside this tenant —
    /// binding without a declared namespace never serves (fail closed).
    #[error("durable object class not declared by target worker")]
    DoClass,

    /// A workflow declaration's `serviceWorkerId` targets a worker
    /// outside this tenant's spec — a dangling engine/USER_WORKFLOW
    /// service designator would fail the whole config load, so the
    /// spec never ships (fail closed). Same rule as `ServiceTarget`.
    #[error("workflow script_name target is not in the same tenant")]
    WorkflowTarget,

    /// A `tailConsumers[]` entry targets a worker outside this tenant
    /// — tail events carry request internals and must never cross the
    /// tenant boundary (fail closed). Same rule as `ServiceTarget`.
    #[error("tail consumer target is not in the same tenant")]
    TailTarget,

    /// `observability.headSamplingRate` is outside [0,1] (NaN
    /// included — a NaN rate can never sample correctly, so it fails
    /// closed rather than degrading to an arbitrary gate).
    #[error("observability headSamplingRate outside [0,1]")]
    ObservabilityBounds,

    /// A cron expression is not a valid 5-field schedule.
    #[error("invalid cron expression")]
    CronExpression,

    /// A quota field is outside its bounds; payload is the field name.
    #[error("quota outside bounds: {0}")]
    QuotaBounds(&'static str),

    /// `cellId`/`revision` inside the payload does not match the verified
    /// envelope scope/revision.
    #[error("payload does not match the envelope binding")]
    EnvelopeBinding,

    /// RV-R53: a `classKey` tombstone without `workerId` — a class
    /// scope is only meaningful under its declaring worker; an
    /// unscoped class deletion could otherwise hit ANY worker's
    /// same-key data (refused, fail closed).
    #[error("deletion tombstone scope is malformed")]
    DeletionScope,
}

impl DesiredError {
    /// Stable protocol error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::SchemaVersion => "DESIRED_SCHEMA_VERSION",
            Self::InvalidRevision => "DESIRED_INVALID_REVISION",
            Self::Capacity(_) => "DESIRED_CAPACITY_EXCEEDED",
            Self::Identifier(_) => "DESIRED_INVALID_IDENTIFIER",
            Self::WorkerName => "DESIRED_INVALID_WORKER_NAME",
            Self::BindingName => "DESIRED_INVALID_BINDING_NAME",
            Self::Hostname(_) => "DESIRED_INVALID_HOSTNAME",
            Self::Duplicate(_) => "DESIRED_DUPLICATE",
            Self::Digest(_) => "DESIRED_INVALID_DIGEST",
            Self::CompatibilityDate => "DESIRED_INVALID_COMPATIBILITY_DATE",
            Self::CompatibilityFlag => "DESIRED_INVALID_COMPATIBILITY_FLAG",
            Self::ServiceTarget => "DESIRED_SERVICE_TARGET_SCOPE",
            Self::CronExpression => "DESIRED_INVALID_CRON_EXPRESSION",
            Self::QuotaBounds(_) => "DESIRED_QUOTA_BOUNDS",
            Self::EnvelopeBinding => "DESIRED_ENVELOPE_BINDING",
            Self::DeletionScope => "DESIRED_DELETION_SCOPE",
            Self::ServiceCycle => "DESIRED_SERVICE_CYCLE",
            Self::DoClass => "DESIRED_DO_CLASS_UNDECLARED",
            Self::WorkflowTarget => "DESIRED_WORKFLOW_TARGET_SCOPE",
            Self::TailTarget => "DESIRED_TAIL_TARGET_SCOPE",
            Self::ObservabilityBounds => "DESIRED_OBSERVABILITY_BOUNDS",
        }
    }
}

/// Error returned by [`verify_compute_desired`]: either an envelope-layer
/// [`SnapshotError`] or a payload-layer [`DesiredError`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ComputeDesiredError {
    /// Envelope verification or typed decode failed.
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    /// Payload semantic validation or envelope binding failed.
    #[error(transparent)]
    Payload(#[from] DesiredError),
}

impl ComputeDesiredError {
    /// Stable error code of the underlying error.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Snapshot(e) => e.code(),
            Self::Payload(e) => e.code(),
        }
    }
}

/// Envelope `scope` value for a `compute.desired` snapshot addressed to
/// `cell_id`.
pub fn cell_scope(cell_id: &str) -> String {
    format!("cell:{cell_id}")
}

/// Full fail-closed verification of a `compute.desired` snapshot:
///
/// 1. envelope verification for `kind = "compute.desired"`,
///    `scope = "cell:<cell_id>"` ([`verify`]);
/// 2. typed decode (`deny_unknown_fields`);
/// 3. [`ComputeDesired::validate`] semantic checks;
/// 4. envelope binding — payload `cellId`/`revision` must equal the
///    envelope `scope`/`revision`;
/// 5. revision fence via `guard.accept(...)` — only after 1–4 pass.
pub fn verify_compute_desired(
    raw: &[u8],
    cell_id: &str,
    trusted_keys: &[TrustedKey],
    guard: &mut RevisionGuard,
    now: DateTime<Utc>,
) -> Result<Verified<ComputeDesired>, ComputeDesiredError> {
    let scope = cell_scope(cell_id);
    let verified = verify::<ComputeDesired>(
        raw,
        &ExpectedScope {
            kind: KIND,
            scope: &scope,
        },
        trusted_keys,
        now,
    )?;
    verified.payload.validate()?;
    if verified.payload.cell_id != cell_id || verified.payload.revision != verified.revision {
        return Err(DesiredError::EnvelopeBinding.into());
    }
    guard.accept(verified.revision, &verified.envelope_digest)?;
    Ok(verified)
}

impl ComputeDesired {
    /// Semantic validation beyond typed decode. All bounds fail closed and
    /// every field named in errors is a `'static` identifier — no input
    /// data is embedded in errors.
    pub fn validate(&self) -> Result<(), DesiredError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(DesiredError::SchemaVersion);
        }
        if self.revision == 0 || self.revision > MAX_SAFE_INTEGER {
            return Err(DesiredError::InvalidRevision);
        }
        if !valid_identifier(&self.cell_id, MAX_IDENTIFIER_LEN) {
            return Err(DesiredError::Identifier("cellId"));
        }
        if self.tenants.len() > MAX_TENANTS {
            return Err(DesiredError::Capacity("tenants"));
        }

        let mut tenant_ids = HashSet::new();
        let mut worker_ids = HashSet::new();
        let mut hostnames = HashSet::new();
        // T37b: CF semantics -- a queue has at most one consumer globally (unique across tenants
        // too: queueId is a platform-wide unique namespace, so two teams' consumer
        // relations pointing at the same queueId must be a config error).
        let mut consumed_queues = HashSet::new();
        for tenant in &self.tenants {
            if !tenant_ids.insert(&tenant.tenant_id) {
                return Err(DesiredError::Duplicate("tenantId"));
            }
            if !valid_identifier(&tenant.tenant_id, MAX_IDENTIFIER_LEN) {
                return Err(DesiredError::Identifier("tenantId"));
            }
            if tenant.workers.len() > MAX_WORKERS_PER_TENANT {
                return Err(DesiredError::Capacity("workers"));
            }

            let mut tenant_worker_ids = HashSet::new();
            let mut worker_names = HashSet::new();
            for worker in &tenant.workers {
                if !tenant_worker_ids.insert(&worker.worker_id)
                    || !worker_ids.insert(&worker.worker_id)
                {
                    return Err(DesiredError::Duplicate("workerId"));
                }
                if !worker_names.insert(&worker.name) {
                    return Err(DesiredError::Duplicate("workerName"));
                }
                if !valid_identifier(&worker.worker_id, MAX_IDENTIFIER_LEN) {
                    return Err(DesiredError::Identifier("workerId"));
                }
                if !valid_worker_name(&worker.name) {
                    return Err(DesiredError::WorkerName);
                }
                worker.active_version.validate()?;
                worker.validate_bindings()?;
                worker.validate_queue_consumers()?;
                worker.validate_do_classes()?;
                for c in &worker.queue_consumers {
                    if !consumed_queues.insert(&c.queue_id) {
                        return Err(DesiredError::Duplicate("queueConsumers.queueId"));
                    }
                }
                worker.validate_routes(&mut hostnames)?;
                worker.validate_crons()?;
                worker.quotas.validate()?;
            }

            // Service bindings may only target workers of the same tenant.
            for worker in &tenant.workers {
                // Tail consumers: same-tenant rule — trace events carry
                // request internals (headers, bodies, outcomes) and must
                // never reach a foreign tenant's worker. Tail edges are
                // observability-only: they do NOT join the service-call
                // cycle check below (a consumer never invokes back).
                for consumer in &worker.tail_consumers {
                    if !valid_identifier(&consumer.worker_id, MAX_IDENTIFIER_LEN)
                        || !tenant_worker_ids.contains(&consumer.worker_id)
                    {
                        return Err(DesiredError::TailTarget);
                    }
                }
                if let Some(obs) = &worker.observability {
                    if obs.head_sampling_rate_ppm > 1_000_000 {
                        return Err(DesiredError::ObservabilityBounds);
                    }
                }
                for binding in &worker.bindings {
                    if let BindingDesired::Service(service) = binding {
                        if !valid_identifier(&service.worker_id, MAX_IDENTIFIER_LEN)
                            || !tenant_worker_ids.contains(&service.worker_id)
                        {
                            return Err(DesiredError::ServiceTarget);
                        }
                    }
                    // P3c-1: DO bindings resolve to a class DECLARED by
                    // the target worker (self unless `serviceWorkerId`
                    // names a same-tenant worker — cross-tenant targets
                    // fail closed; an undeclared class never serves).
                    if let BindingDesired::DurableObject(d) = binding {
                        let target = d.service_worker_id.as_deref().unwrap_or(&worker.worker_id);
                        let declares = tenant.workers.iter().any(|w| {
                            w.worker_id == target
                                && w.durable_object_classes
                                    .iter()
                                    .any(|c| c.class_name == d.class_name)
                        });
                        if !declares {
                            return Err(DesiredError::DoClass);
                        }
                    }
                }
            }
            // W7: workflow `serviceWorkerId` (wrangler `script_name`) —
            // target must be a worker of THIS tenant's spec (the engine
            // service, storage, loopback and ops facade all render
            // under it; a dangling designator fails the config load).
            // (definingWorker, name) is unique across the tenant: two
            // declarations can never share one engine namespace — each
            // row mints an independent uniqueKey, so dedup would make
            // the storage identity depend on which declarer wins (an
            // unstable storage fork). Reject the second declaration.
            {
                let mut wf_targets: HashSet<(&str, &str)> = HashSet::new();
                for worker in &tenant.workers {
                    for wf in &worker.workflows {
                        let target = wf.service_worker_id.as_deref().unwrap_or(&worker.worker_id);
                        if !tenant_worker_ids.iter().any(|id| id.as_str() == target) {
                            return Err(DesiredError::WorkflowTarget);
                        }
                        if !wf_targets.insert((target, wf.name.as_str())) {
                            return Err(DesiredError::Duplicate("workflows.serviceWorkerId+name"));
                        }
                    }
                }
            }
            // T21/F4: reject cyclic service graphs (length ≥ 2). The
            // dispatch happens inside workerd's isolate — vrt cannot
            // inject a depth header or per-hop timeout there — so the
            // only honest bound is structural: a cyclic graph never
            // ships. Self-loops (length 1) are already rejected at
            // deploy-time admission (`assertServiceTarget`), so they
            // remain legal at this layer (back-compat boundary).
            {
                let mut edges: std::collections::BTreeMap<&str, Vec<&str>> =
                    std::collections::BTreeMap::new();
                for worker in &tenant.workers {
                    for binding in &worker.bindings {
                        if let BindingDesired::Service(service) = binding {
                            edges
                                .entry(worker.worker_id.as_str())
                                .or_default()
                                .push(service.worker_id.as_str());
                        }
                    }
                }
                // DFS three-color cycle walk; edges to non-tenant ids
                // already failed above, so every target resolves here.
                let mut color: std::collections::BTreeMap<&str, u8> =
                    std::collections::BTreeMap::new();
                fn visit<'a>(
                    node: &'a str,
                    edges: &std::collections::BTreeMap<&'a str, Vec<&'a str>>,
                    color: &mut std::collections::BTreeMap<&'a str, u8>,
                ) -> bool {
                    match color.get(node).copied().unwrap_or(0) {
                        1 => return true,  // gray — back edge = cycle
                        2 => return false, // black — already proven acyclic
                        _ => {}
                    }
                    color.insert(node, 1);
                    if let Some(targets) = edges.get(node) {
                        for &next in targets {
                            if next != node && visit(next, edges, color) {
                                return true;
                            }
                        }
                    }
                    color.insert(node, 2);
                    false
                }
                for worker in &tenant.workers {
                    if visit(worker.worker_id.as_str(), &edges, &mut color) {
                        return Err(DesiredError::ServiceCycle);
                    }
                }
            }
        }
        // RV-R53: deletion tombstones — bounded, typed, scoped. A
        // malformed entry invalidates the whole document (a silently
        // dropped tombstone could either lose deletion intent or —
        // worse — a widened scope destroy unrelated data).
        if self.deletions.len() > MAX_DELETIONS {
            return Err(DesiredError::Capacity("deletions"));
        }
        let mut seen_deletions = HashSet::new();
        for d in &self.deletions {
            if !valid_identifier(&d.tenant_id, MAX_IDENTIFIER_LEN) {
                return Err(DesiredError::Identifier("deletions.tenantId"));
            }
            if let Some(w) = &d.worker_id {
                if !valid_identifier(w, MAX_IDENTIFIER_LEN) {
                    return Err(DesiredError::Identifier("deletions.workerId"));
                }
            }
            if let Some(k) = &d.class_key {
                if !is_lower_hex(k, 64) {
                    return Err(DesiredError::Digest("deletions.classKey"));
                }
                if d.worker_id.is_none() {
                    return Err(DesiredError::DeletionScope);
                }
            }
            if d.revision == 0 || d.revision > MAX_SAFE_INTEGER {
                return Err(DesiredError::InvalidRevision);
            }
            if !seen_deletions.insert((
                &d.tenant_id,
                &d.worker_id,
                &d.class_key,
            )) {
                return Err(DesiredError::Duplicate("deletions"));
            }
        }
        Ok(())
    }
}

impl VersionDesired {
    fn validate(&self) -> Result<(), DesiredError> {
        if !valid_identifier(&self.version_id, MAX_IDENTIFIER_LEN) {
            return Err(DesiredError::Identifier("activeVersion.versionId"));
        }
        if !is_lower_hex(&self.artifact_digest, 64) {
            return Err(DesiredError::Digest("activeVersion.artifactDigest"));
        }
        if !valid_compat_date(&self.compatibility_date) {
            return Err(DesiredError::CompatibilityDate);
        }
        if self.compatibility_flags.len() > MAX_COMPAT_FLAGS {
            return Err(DesiredError::Capacity("compatibilityFlags"));
        }
        if self
            .compatibility_flags
            .iter()
            .any(|flag| !valid_compat_flag(flag))
        {
            return Err(DesiredError::CompatibilityFlag);
        }
        Ok(())
    }
}

impl WorkerDesired {
    fn validate_bindings(&self) -> Result<(), DesiredError> {
        if self.bindings.len() > MAX_BINDINGS_PER_WORKER {
            return Err(DesiredError::Capacity("bindings"));
        }
        if self
            .bindings
            .iter()
            .filter(|b| matches!(b, BindingDesired::Ratelimit(_)))
            .count()
            > MAX_RATELIMIT_BINDINGS_PER_WORKER
        {
            return Err(DesiredError::Capacity("ratelimitBindings"));
        }
        let mut names = HashSet::new();
        for binding in &self.bindings {
            if !names.insert(binding.name()) {
                return Err(DesiredError::Duplicate("bindingName"));
            }
            if !valid_binding_name(binding.name()) {
                return Err(DesiredError::BindingName);
            }
            match binding {
                BindingDesired::Var(var) => {
                    if !valid_identifier(&var.var_ref, MAX_REF_LEN) {
                        return Err(DesiredError::Identifier("bindings.varRef"));
                    }
                }
                BindingDesired::Secret(secret) => {
                    if !valid_identifier(&secret.secret_ref, MAX_REF_LEN) {
                        return Err(DesiredError::Identifier("bindings.secretRef"));
                    }
                }
                BindingDesired::Kv(kv) => {
                    if !valid_identifier(&kv.namespace_id, MAX_IDENTIFIER_LEN) {
                        return Err(DesiredError::Identifier("bindings.namespaceId"));
                    }
                }
                BindingDesired::R2(r2) => {
                    if !valid_bucket_name(&r2.bucket) {
                        return Err(DesiredError::Identifier("bindings.bucket"));
                    }
                }
                BindingDesired::Pg(pg) => {
                    if !valid_identifier(&pg.database_id, MAX_IDENTIFIER_LEN) {
                        return Err(DesiredError::Identifier("bindings.databaseId"));
                    }
                    if !pg.credential_ref.is_empty()
                        && !valid_identifier(&pg.credential_ref, MAX_REF_LEN)
                    {
                        return Err(DesiredError::Identifier("bindings.credentialRef"));
                    }
                }
                BindingDesired::Hyperdrive(hd) => {
                    if !valid_identifier(&hd.binding_id, MAX_IDENTIFIER_LEN) {
                        return Err(DesiredError::Identifier("bindings.bindingId"));
                    }
                    // The synthetic host is a DNS name, not an
                    // identifier — validate its shape so a hostile or
                    // corrupt desired cannot smuggle a resolvable or
                    // oversized name into the egress policy.
                    let h = &hd.synthetic_host;
                    if !(h.len() <= 253
                        && h.ends_with(".hyperdrive.vrt.internal")
                        && h.chars().all(|c| c.is_ascii_lowercase()
                            || c.is_ascii_digit() || c == '-' || c == '.'))
                    {
                        return Err(DesiredError::Identifier("bindings.syntheticHost"));
                    }
                    if !valid_identifier(&hd.credential_ref, MAX_REF_LEN) {
                        return Err(DesiredError::Identifier("bindings.credentialRef"));
                    }
                }
                BindingDesired::Service(s) => {
                    // Target checked against the tenant worker set in
                    // ComputeDesired::validate (needs tenant scope).
                    if let Some(ep) = &s.entrypoint {
                        if !valid_service_entrypoint(ep) {
                            return Err(DesiredError::Identifier("bindings.entrypoint"));
                        }
                    }
                    if let Some(props) = &s.props {
                        // CF `props` is an object (`Record<string, _>`)
                        // that lands on `ctx.props` — bound the bytes
                        // like a `json` binding and require the object
                        // shape, never an arbitrary scalar/array.
                        if props.len() > MAX_JSON_BINDING_BYTES {
                            return Err(DesiredError::Capacity("servicePropsBytes"));
                        }
                        match serde_json::from_str::<serde_json::Value>(props) {
                            Ok(serde_json::Value::Object(_)) => {}
                            _ => return Err(DesiredError::Identifier("bindings.serviceProps")),
                        }
                    }
                }
                BindingDesired::DurableObject(d) => {
                    // class_name is a JS identifier too (it exports a
                    // class); declaration resolution needs tenant scope
                    // (ComputeDesired::validate).
                    if !valid_binding_name(&d.class_name) {
                        return Err(DesiredError::Identifier("bindings.className"));
                    }
                    if let Some(svc) = &d.service_worker_id {
                        if !valid_identifier(svc, MAX_IDENTIFIER_LEN) {
                            return Err(DesiredError::Identifier("bindings.serviceWorkerId"));
                        }
                    }
                }
                BindingDesired::Assets(assets) => {
                    if !is_lower_hex(&assets.manifest_digest, 64) {
                        return Err(DesiredError::Digest("bindings.manifestDigest"));
                    }
                    if let Some(RunWorkerFirst::Rules(rules)) = &assets.run_worker_first {
                        if !valid_run_worker_first_rules(rules) {
                            return Err(DesiredError::Identifier("bindings.runWorkerFirst"));
                        }
                    }
                }
                BindingDesired::D1(d1) => {
                    // databaseId is server-minted; the charset bound here
                    // mirrors vrt-sql::paths::is_valid_db_id (26 lowercase
                    // alnum). A malformed id fails the snapshot itself.
                    if d1.database_id.len() != 26
                        || !d1
                            .database_id
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                    {
                        return Err(DesiredError::Identifier("bindings.databaseId"));
                    }
                    if d1.epoch == 0 {
                        return Err(DesiredError::Identifier("bindings.epoch"));
                    }
                }
                BindingDesired::Ratelimit(r) => {
                    if r.namespace_id.is_empty()
                        || r.namespace_id.len() > 32
                        || !r.namespace_id.bytes().all(|b| b.is_ascii_digit())
                    {
                        return Err(DesiredError::Identifier("bindings.namespaceId"));
                    }
                    if r.limit < 1 || r.limit > MAX_RATELIMIT_LIMIT {
                        return Err(DesiredError::Identifier("bindings.limit"));
                    }
                    if r.period != 10 && r.period != 60 {
                        return Err(DesiredError::Identifier("bindings.period"));
                    }
                }
                BindingDesired::Queue(q) => {
                    // queueId is server-minted — same charset bound as
                    // databaseId (26 lowercase alnum, §7 path rules).
                    if q.queue_id.len() != 26
                        || !q
                            .queue_id
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                    {
                        return Err(DesiredError::Identifier("bindings.queueId"));
                    }
                    if !valid_identifier(&q.queue_name, MAX_IDENTIFIER_LEN) {
                        return Err(DesiredError::Identifier("bindings.queueName"));
                    }
                }
                // WorkerLoader carries no resource reference — `name`
                // (validated as a binding name at the match head) is
                // the only field.
                BindingDesired::WorkerLoader(_) => {}
                BindingDesired::AnalyticsEngine(ae) => {
                    // `dataset` is emitted as a workerd service name
                    // AND used as a spool/parquet path segment —
                    // `[A-Za-z0-9_]{1,256}` keeps both uses safe
                    // (no `=` for --external-addr, no `/` for paths).
                    if ae.dataset.is_empty()
                        || ae.dataset.len() > 256
                        || !ae
                            .dataset
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    {
                        return Err(DesiredError::Identifier("bindings.dataset"));
                    }
                }
                BindingDesired::Json(j) => {
                    if j.value.len() > MAX_JSON_BINDING_BYTES {
                        return Err(DesiredError::Capacity("jsonBindingBytes"));
                    }
                    if serde_json::from_str::<serde_json::Value>(&j.value).is_err() {
                        return Err(DesiredError::Identifier("bindings.jsonValue"));
                    }
                }
                // UnsafeEval carries no resource reference — `name`
                // (validated as a binding name at the match head) is
                // the only field.
                BindingDesired::UnsafeEval(_) => {}
            }
        }
        Ok(())
    }

    fn validate_routes<'a>(
        &'a self,
        hostnames: &mut HashSet<&'a String>,
    ) -> Result<(), DesiredError> {
        if self.routes.len() > MAX_ROUTES_PER_WORKER {
            return Err(DesiredError::Capacity("routes"));
        }
        for route in &self.routes {
            if !valid_hostname(&route.hostname) {
                return Err(DesiredError::Hostname("routes.hostname"));
            }
            if !hostnames.insert(&route.hostname) {
                return Err(DesiredError::Duplicate("hostname"));
            }
        }
        Ok(())
    }

    /// T37b queue consumers: id shape, DLQ self-reference, numeric bounds, uniqueness of queueId
    /// within a worker (global uniqueness is in ComputeDesired::validate).
    fn validate_queue_consumers(&self) -> Result<(), DesiredError> {
        if self.queue_consumers.len() > MAX_QUEUE_CONSUMERS_PER_WORKER {
            return Err(DesiredError::Capacity("queueConsumers"));
        }
        let mut seen = HashSet::new();
        for c in &self.queue_consumers {
            if !valid_queue_id(&c.queue_id) {
                return Err(DesiredError::Identifier("queueConsumers.queueId"));
            }
            if !valid_identifier(&c.queue_name, MAX_IDENTIFIER_LEN) {
                return Err(DesiredError::Identifier("queueConsumers.queueName"));
            }
            if !seen.insert(&c.queue_id) {
                return Err(DesiredError::Duplicate("queueConsumers.queueId"));
            }
            if let Some(dlq) = &c.dead_letter_queue_id {
                if !valid_queue_id(dlq) {
                    return Err(DesiredError::Identifier("queueConsumers.deadLetterQueueId"));
                }
                if dlq == &c.queue_id {
                    return Err(DesiredError::Identifier("queueConsumers.deadLetterQueueId"));
                }
            }
            if !c.max_batch_size.map_or(true, |v| (1..=100).contains(&v))
                || !c.max_batch_timeout_secs.map_or(true, |v| v <= 60)
                || !c.max_retries.map_or(true, |v| v <= 100)
                || !c.retry_delay_secs.map_or(true, |v| v <= 86_400)
                || !c.max_concurrency.map_or(true, |v| (1..=1_000).contains(&v))
            {
                return Err(DesiredError::Identifier("queueConsumers.range"));
            }
        }
        Ok(())
    }

    /// P3c-1: DO class declaration -- class_name is a JS identifier and unique within a worker,
    /// `uniqueKey` is exactly 64 lowercase hex chars (a shape constraint of the server-side HMAC derivation,
    /// the derivation itself happens in the control plane / standalone pack), bounded capacity.
    fn validate_do_classes(&self) -> Result<(), DesiredError> {
        if self.durable_object_classes.len() > MAX_DO_CLASSES_PER_WORKER {
            return Err(DesiredError::Capacity("durableObjectClasses"));
        }
        let mut seen = HashSet::new();
        for class in &self.durable_object_classes {
            if !valid_binding_name(&class.class_name) {
                return Err(DesiredError::Identifier("durableObjectClasses.className"));
            }
            if !seen.insert(&class.class_name) {
                return Err(DesiredError::Duplicate("durableObjectClasses.className"));
            }
            if !is_lower_hex(&class.unique_key, 64) {
                return Err(DesiredError::Digest("durableObjectClasses.uniqueKey"));
            }
            if let Some(src) = &class.from_worker_id {
                if !valid_identifier(src, MAX_IDENTIFIER_LEN) {
                    return Err(DesiredError::Identifier(
                        "durableObjectClasses.fromWorkerId",
                    ));
                }
                if src == &self.worker_id {
                    return Err(DesiredError::Identifier(
                        "durableObjectClasses.fromWorkerId",
                    ));
                }
            }
        }
        self.validate_workflows()
    }

    /// W1: workflows — name format/uniqueness, binding JS-identifier +
    /// no collision with `env.*` bindings, class JS-identifier,
    /// 64-hex unique_key, steps bounds. Cross-worker `script_name`
    /// never reaches this point (pack rejects it fail-closed).
    fn validate_workflows(&self) -> Result<(), DesiredError> {
        if self.workflows.len() > MAX_WORKFLOWS_PER_WORKER {
            return Err(DesiredError::Capacity("workflows"));
        }
        let mut names = HashSet::new();
        let mut bindings: HashSet<&str> = self.bindings.iter().map(|b| b.name()).collect();
        for wf in &self.workflows {
            if !valid_workflow_name(&wf.name) {
                return Err(DesiredError::Identifier("workflows.name"));
            }
            if !names.insert(&wf.name) {
                return Err(DesiredError::Duplicate("workflows.name"));
            }
            if !valid_binding_name(&wf.binding) {
                return Err(DesiredError::Identifier("workflows.binding"));
            }
            if !bindings.insert(&wf.binding) {
                return Err(DesiredError::Duplicate("workflows.binding"));
            }
            if !valid_binding_name(&wf.class_name) {
                return Err(DesiredError::Identifier("workflows.className"));
            }
            if !is_lower_hex(&wf.unique_key, 64) {
                return Err(DesiredError::Digest("workflows.uniqueKey"));
            }
            if let Some(svc) = &wf.service_worker_id {
                if !valid_identifier(svc, MAX_IDENTIFIER_LEN) {
                    return Err(DesiredError::Identifier("workflows.serviceWorkerId"));
                }
            }
            if let Some(steps) = wf.limits_steps {
                if !(1..=MAX_WORKFLOW_STEPS).contains(&steps) {
                    return Err(DesiredError::QuotaBounds("workflows.limitsSteps"));
                }
            }
        }
        Ok(())
    }

    fn validate_crons(&self) -> Result<(), DesiredError> {
        if self.crons.len() > MAX_CRONS_PER_WORKER {
            return Err(DesiredError::Capacity("crons"));
        }
        for cron in &self.crons {
            if !valid_identifier(&cron.schedule_id, MAX_IDENTIFIER_LEN) {
                return Err(DesiredError::Identifier("crons.scheduleId"));
            }
            if !valid_cron_expression(&cron.expression) {
                return Err(DesiredError::CronExpression);
            }
        }
        Ok(())
    }
}

impl BindingDesired {
    /// The JS binding name (`env.<name>`) regardless of variant.
    pub fn name(&self) -> &str {
        match self {
            Self::Var(b) => &b.name,
            Self::Secret(b) => &b.name,
            Self::Kv(b) => &b.name,
            Self::R2(b) => &b.name,
            Self::Pg(b) => &b.name,
            Self::Hyperdrive(b) => &b.name,
            Self::Ratelimit(b) => &b.name,
            Self::Service(b) => &b.name,
            Self::Assets(b) => &b.name,
            Self::D1(b) => &b.name,
            Self::Queue(b) => &b.name,
            Self::DurableObject(b) => &b.name,
            Self::WorkerLoader(b) => &b.name,
            Self::AnalyticsEngine(b) => &b.name,
            Self::Json(b) => &b.name,
            Self::UnsafeEval(b) => &b.name,
        }
    }

    /// The signed access grant for bindings that have one — verbatim
    /// from the snapshot, never widened. `var`/`secret`/`service` carry
    /// no data-plane grant and return `None`.
    pub fn permissions(&self) -> Option<&BindingPermissions> {
        match self {
            Self::Kv(b) => Some(&b.permissions),
            Self::R2(b) => Some(&b.permissions),
            Self::Pg(b) => Some(&b.permissions),
            Self::Hyperdrive(b) => Some(&b.permissions),
            Self::Assets(b) => Some(&b.permissions),
            Self::D1(b) => Some(&b.permissions),
            Self::Queue(b) => Some(&b.permissions),
            Self::Var(_)
            | Self::Secret(_)
            | Self::Service(_)
            | Self::DurableObject(_)
            | Self::Ratelimit(_)
            | Self::WorkerLoader(_)
            | Self::AnalyticsEngine(_)
            | Self::Json(_)
            | Self::UnsafeEval(_) => None,
        }
    }
}

impl WorkerQuotas {
    fn validate(&self) -> Result<(), DesiredError> {
        if !(MIN_CPU_MS..=MAX_CPU_MS).contains(&self.cpu_ms) {
            return Err(DesiredError::QuotaBounds("cpuMs"));
        }
        if !(MIN_MEMORY_MB..=MAX_MEMORY_MB).contains(&self.memory_mb) {
            return Err(DesiredError::QuotaBounds("memoryMb"));
        }
        if self.subrequests > MAX_SUBREQUESTS {
            return Err(DesiredError::QuotaBounds("subrequests"));
        }
        if self.outbound_connections > MAX_OUTBOUND_CONNECTIONS {
            return Err(DesiredError::QuotaBounds("outboundConnections"));
        }
        Ok(())
    }
}

/// Identifier charset identical to the gateway's `valid_identifier`:
/// `[A-Za-z0-9_.:-]`, non-empty, bounded length.
fn valid_identifier(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

/// Lowercase DNS label used for worker names; `--` is forbidden because it
/// is the `<worker>--<team>` hostname separator.
fn valid_worker_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_WORKER_NAME_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("--")
}

/// JS identifier for `env.<name>` bindings.
fn valid_workflow_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

fn valid_binding_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_BINDING_NAME_LEN
        && (value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'$'))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// R2-style bucket name: lowercase DNS label, 3–63 chars.
fn valid_bucket_name(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !value.starts_with('-')
        && !value.ends_with('-')
}

/// Lowercase FQDN hostname, no wildcards, at least two labels.
fn valid_hostname(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_HOSTNAME_LEN {
        return false;
    }
    let mut label_count = 0usize;
    for label in value.split('.') {
        label_count += 1;
        if label.is_empty()
            || label.len() > 63
            || !label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || label.starts_with('-')
            || label.ends_with('-')
        {
            return false;
        }
    }
    label_count >= 2
}

/// Strict `YYYY-MM-DD` (zero-padded, real calendar date).
fn valid_compat_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
        && NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

/// Compatibility-flag token.
fn valid_compat_flag(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_COMPAT_FLAG_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'.' | b'-'))
}

/// Cloudflare's supported subset of 5-field cron:
/// `minute hour day-of-month month day-of-week`. Supports `*`, lists,
/// ranges, steps, JAN–DEC / SUN–SAT names, DOM `L`/`LW`/`nW`, and DOW
/// `dL`/`d#n` modifiers; numeric weekdays use 1=Sunday through 7=Saturday.
fn valid_cron_expression(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_CRON_EXPR_LEN {
        return false;
    }
    let fields: Vec<&str> = value.split_ascii_whitespace().collect();
    if fields.len() != 5 {
        return false;
    }
    const MONTH_NAMES: [&str; 12] = [
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
    ];
    const DAY_NAMES: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
    // (min, max, allowed names) per field.
    let spec: [(u32, u32, &[&str]); 5] = [
        (0, 59, &[]),
        (0, 23, &[]),
        (1, 31, &[]),
        (1, 12, &MONTH_NAMES),
        (1, 7, &DAY_NAMES),
    ];
    fields
        .iter()
        .zip(spec.iter())
        .enumerate()
        .all(|(idx, (field, &(min, max, names)))| match idx {
            2 => valid_cron_dom_field(field),
            4 => valid_cron_dow_field(field, names),
            _ => {
                !field.is_empty()
                    && field
                        .split(',')
                        .all(|part| valid_cron_part(part, min, max, names, false))
            }
        })
}

fn valid_cron_dom_field(field: &str) -> bool {
    !field.is_empty()
        && field.split(',').all(|part| {
            if part.eq_ignore_ascii_case("L") || part.eq_ignore_ascii_case("LW") {
                return true;
            }
            if let Some(day) = strip_ascii_modifier(part, 'W') {
                return parse_cron_uint(day).is_some_and(|day| (1..=31).contains(&day));
            }
            valid_cron_part(part, 1, 31, &[], false)
        })
}

fn valid_cron_dow_field(field: &str, names: &[&str]) -> bool {
    !field.is_empty()
        && field.split(',').all(|part| {
            if let Some((day, occurrence)) = part.split_once('#') {
                return valid_cron_dow_atom(day, names)
                    && parse_cron_uint(occurrence).is_some_and(|n| (1..=5).contains(&n));
            }
            if let Some(day) = strip_ascii_modifier(part, 'L') {
                return valid_cron_dow_atom(day, names);
            }
            valid_cron_part(part, 1, 7, names, true)
        })
}

fn valid_cron_dow_atom(value: &str, names: &[&str]) -> bool {
    names.iter().any(|name| value.eq_ignore_ascii_case(name))
        || parse_cron_uint(value).is_some_and(|n| (1..=7).contains(&n))
}

fn strip_ascii_modifier(value: &str, modifier: char) -> Option<&str> {
    value
        .strip_suffix(modifier)
        .or_else(|| value.strip_suffix(modifier.to_ascii_lowercase()))
}

/// RV-R69: strict unsigned decimal (`[0-9]+`). `str::parse::<u32>` also
/// accepts a leading `+`, which the TS validator rejects — keep both
/// sides on the same grammar.
fn parse_cron_uint(value: &str) -> Option<u32> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse::<u32>().ok()
}

fn valid_cron_part(part: &str, min: u32, max: u32, names: &[&str], dow: bool) -> bool {
    let base = match part.split_once('/') {
        Some((base, step)) => {
            if step.is_empty() || step.contains('/') {
                return false;
            }
            match parse_cron_uint(step) {
                Some(step) if step >= 1 && step <= max => base,
                _ => return false,
            }
        }
        None => part,
    };
    let atom = |value: &str| -> bool {
        if names.iter().any(|n| value.eq_ignore_ascii_case(n)) {
            return true;
        }
        parse_cron_uint(value).is_some_and(|n| (min..=max).contains(&n))
    };
    match base.split_once('-') {
        Some((lo, hi)) => {
            !lo.is_empty() && !hi.is_empty() && atom(lo) && atom(hi) && {
                // Range endpoints must be ordered. Day-of-week names use
                // Sunday=0 internally; a named `SUN` upper bound means
                // Sunday-at-end-of-week, while numeric values are converted
                // from Cloudflare's 1=Sunday..7=Saturday convention.
                match (
                    cron_atom_value(lo, min, max, names, dow, false),
                    cron_atom_value(hi, min, max, names, dow, true),
                ) {
                    (Some(l), Some(h)) => l <= h,
                    _ => false,
                }
            }
        }
        None => base == "*" || atom(base),
    }
}

/// Resolve a cron atom to its number. `range_hi_dow` marks the upper
/// bound of a day-of-week range, where the name `SUN` resolves to 7.
fn cron_atom_value(
    value: &str,
    min: u32,
    max: u32,
    names: &[&str],
    dow: bool,
    range_hi_dow: bool,
) -> Option<u32> {
    if let Some(n) = parse_cron_uint(value) {
        return (min..=max)
            .contains(&n)
            .then_some(if dow { n - 1 } else { n });
    }
    names
        .iter()
        .position(|n| value.eq_ignore_ascii_case(n))
        .map(|i| {
            if dow && range_hi_dow && i == 0 {
                7
            } else if dow {
                i as u32
            } else {
                i as u32 + min
            }
        })
}

fn is_lower_hex(value: &str, bytes: usize) -> bool {
    value.len() == bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{sign, SignParams, SigningKey};
    use serde_json::json;

    fn key() -> SigningKey {
        SigningKey::from_seed(&[0x42; 32]).unwrap()
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-08T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// A valid, feature-complete payload covering every binding kind.
    fn valid_payload() -> serde_json::Value {
        json!({
            "schemaVersion": 1,
            "cellId": "cell-01",
            "revision": 7,
            "tenants": [{
                "tenantId": "team-alpha",
                "workers": [{
                    "workerId": "w-alpha-1",
                    "name": "api",
                    "activeVersion": {
                        "versionId": "v-2026-11-08-1",
                        "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2026-11-01",
                        "compatibilityFlags": ["nodejs_compat"]
                    },
                    "bindings": [
                        {"type": "var", "name": "LOG_LEVEL", "varRef": "var:w-alpha-1:log"},
                        {"type": "secret", "name": "API_TOKEN", "secretRef": "sec:w-alpha-1:tok"},
                        {"type": "kv", "name": "KV", "namespaceId": "kvns-01", "permissions": {"read": true, "write": true}},
                        {"type": "r2", "name": "MEDIA", "bucket": "media-bucket-01", "permissions": {"read": true, "write": true}},
                        {"type": "pg", "name": "DB", "databaseId": "db-01", "permissions": {"read": true, "write": true}},
                        {"type": "service", "name": "AUTH", "workerId": "w-alpha-2"},
                        {"type": "assets", "name": "ASSETS", "manifestDigest": "b".repeat(64), "permissions": {"read": true, "write": false}}
                    ],
                    "routes": [{"hostname": "api--team-alpha.vibeploy.app"}],
                    "crons": [{"scheduleId": "cron-1", "expression": "*/15 0-23 1-31 jan-dec sun-sat"}],
                    "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 100, "outboundConnections": 16}
                }, {
                    "workerId": "w-alpha-2",
                    "name": "auth",
                    "activeVersion": {
                        "versionId": "v-2026-11-08-1",
                        "artifactDigest": "c".repeat(64),
                        "compatibilityDate": "2026-11-01",
                        "compatibilityFlags": []
                    },
                    "bindings": [],
                    "routes": [],
                    "crons": [],
                    "quotas": {"cpuMs": 10, "memoryMb": 64, "subrequests": 0, "outboundConnections": 0}
                }]
            }]
        })
    }

    fn desired(value: &serde_json::Value) -> Result<ComputeDesired, serde_json::Error> {
        serde_json::from_value(value.clone())
    }

    /// Table-driven invalid payloads: (id, mutator, expected error code).
    #[test]
    fn validate_table_driven() {
        let cases: Vec<(&str, Box<dyn Fn(&mut serde_json::Value)>, &'static str)> = vec![
            ("valid", Box::new(|_| {}), ""),
            (
                "schema-version",
                Box::new(|v| v["schemaVersion"] = json!(2)),
                "DESIRED_SCHEMA_VERSION",
            ),
            (
                "revision-zero",
                Box::new(|v| v["revision"] = json!(0)),
                "DESIRED_INVALID_REVISION",
            ),
            (
                "bad-cell-id",
                Box::new(|v| v["cellId"] = json!("bad cell!")),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "dup-tenant",
                Box::new(|v| {
                    let t = v["tenants"][0].clone();
                    v["tenants"].as_array_mut().unwrap().push(t);
                }),
                "DESIRED_DUPLICATE",
            ),
            (
                "dup-worker-global",
                Box::new(|v| {
                    let w = v["tenants"][0]["workers"][0].clone();
                    v["tenants"].as_array_mut().unwrap().push(json!({
                        "tenantId": "team-beta",
                        "workers": [w]
                    }));
                }),
                "DESIRED_DUPLICATE",
            ),
            (
                "dup-worker-name",
                Box::new(|v| {
                    let mut w = v["tenants"][0]["workers"][1].clone();
                    w["workerId"] = json!("w-alpha-3");
                    w["name"] = json!("api");
                    v["tenants"][0]["workers"].as_array_mut().unwrap().push(w);
                }),
                "DESIRED_DUPLICATE",
            ),
            (
                "bad-worker-name-upper",
                Box::new(|v| v["tenants"][0]["workers"][0]["name"] = json!("Api")),
                "DESIRED_INVALID_WORKER_NAME",
            ),
            (
                "bad-worker-name-doubledash",
                Box::new(|v| v["tenants"][0]["workers"][0]["name"] = json!("a--b")),
                "DESIRED_INVALID_WORKER_NAME",
            ),
            (
                "bad-worker-name-dash",
                Box::new(|v| v["tenants"][0]["workers"][0]["name"] = json!("-api")),
                "DESIRED_INVALID_WORKER_NAME",
            ),
            (
                "bad-digest-short",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["activeVersion"]["artifactDigest"] = json!("ab12")
                }),
                "DESIRED_INVALID_DIGEST",
            ),
            (
                "bad-digest-uppercase",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["activeVersion"]["artifactDigest"] =
                        json!("A".repeat(64))
                }),
                "DESIRED_INVALID_DIGEST",
            ),
            (
                "bad-digest-nonhex",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["activeVersion"]["artifactDigest"] =
                        json!("g".repeat(64))
                }),
                "DESIRED_INVALID_DIGEST",
            ),
            (
                "bad-compat-date",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["activeVersion"]["compatibilityDate"] =
                        json!("2026-13-40")
                }),
                "DESIRED_INVALID_COMPATIBILITY_DATE",
            ),
            (
                "bad-compat-date-format",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["activeVersion"]["compatibilityDate"] =
                        json!("2026-1-1")
                }),
                "DESIRED_INVALID_COMPATIBILITY_DATE",
            ),
            (
                "bad-compat-flag",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["activeVersion"]["compatibilityFlags"] =
                        json!(["bad flag!"])
                }),
                "DESIRED_INVALID_COMPATIBILITY_FLAG",
            ),
            (
                "dup-binding-name",
                Box::new(|v| {
                    let b = v["tenants"][0]["workers"][0]["bindings"][0].clone();
                    v["tenants"][0]["workers"][0]["bindings"]
                        .as_array_mut()
                        .unwrap()
                        .push(b);
                }),
                "DESIRED_DUPLICATE",
            ),
            (
                "bad-binding-name",
                Box::new(|v| v["tenants"][0]["workers"][0]["bindings"][0]["name"] = json!("1BAD")),
                "DESIRED_INVALID_BINDING_NAME",
            ),
            (
                "secret-empty-ref",
                Box::new(|v| v["tenants"][0]["workers"][0]["bindings"][1]["secretRef"] = json!("")),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-cross-tenant",
                Box::new(|v| {
                    let av = v["tenants"][0]["workers"][0]["activeVersion"].clone();
                    let q = v["tenants"][0]["workers"][0]["quotas"].clone();
                    v["tenants"].as_array_mut().unwrap().push(json!({
                        "tenantId": "team-beta",
                        "workers": [{
                            "workerId": "w-beta-1",
                            "name": "beta",
                            "activeVersion": av,
                            "bindings": [],
                            "routes": [],
                            "crons": [],
                            "quotas": q
                        }]
                    }));
                    v["tenants"][0]["workers"][0]["bindings"][5]["workerId"] = json!("w-beta-1");
                }),
                "DESIRED_SERVICE_TARGET_SCOPE",
            ),
            (
                "service-missing-target",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["workerId"] = json!("w-nope")
                }),
                "DESIRED_SERVICE_TARGET_SCOPE",
            ),
            (
                "service-cycle-ab-a",
                Box::new(|v| {
                    v["tenants"][0]["workers"][1]["bindings"] =
                        json!([{"type": "service", "name": "BACK", "workerId": "w-alpha-1"}])
                }),
                "DESIRED_SERVICE_CYCLE",
            ),
            // Self-binding is inside the same tenant and therefore allowed;
            // recursion is bounded by the subrequest quota, not schema.
            (
                "service-self-same-tenant",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["workerId"] = json!("w-alpha-1")
                }),
                "",
            ),
            ("service-ok-same-tenant", Box::new(|_| {}), ""),
            (
                "service-named-entrypoint-ok",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] = json!("Greeter")
                }),
                "",
            ),
            (
                "service-entrypoint-not-identifier",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] = json!("a-b")
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-entrypoint-empty",
                Box::new(|v| v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] = json!("")),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-entrypoint-default",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] = json!("default")
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-entrypoint-reserved-probe",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] =
                        json!("VrtWaituntilProbe")
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-entrypoint-reserved-lower",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] = json!("vrtX")
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-entrypoint-too-long",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["entrypoint"] =
                        json!("E".repeat(MAX_BINDING_NAME_LEN + 1))
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            // CF `services[].props` — a JSON object only, inside the
            // 16 KiB json-binding bound.
            (
                "service-props-ok",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["props"] =
                        json!(r#"{"tier":"gold"}"#)
                }),
                "",
            ),
            (
                "service-props-not-object",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["props"] = json!("[1,2]")
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "service-props-unparseable",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][5]["props"] = json!("{bad")
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            // Tail Workers — same-tenant rule mirrors the service-target
            // rule (trace events carry request internals; never cross a
            // tenant boundary). Self-tail is legal.
            (
                "tail-ok-same-tenant",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["tailConsumers"] =
                        json!([{"workerId": "w-alpha-2"}, {"workerId": "w-alpha-1"}])
                }),
                "",
            ),
            (
                "tail-cross-tenant",
                Box::new(|v| {
                    let av = v["tenants"][0]["workers"][0]["activeVersion"].clone();
                    let q = v["tenants"][0]["workers"][0]["quotas"].clone();
                    v["tenants"].as_array_mut().unwrap().push(json!({
                        "tenantId": "team-beta",
                        "workers": [{
                            "workerId": "w-beta-1",
                            "name": "beta",
                            "activeVersion": av,
                            "bindings": [],
                            "routes": [],
                            "crons": [],
                            "quotas": q
                        }]
                    }));
                    v["tenants"][0]["workers"][0]["tailConsumers"] =
                        json!([{"workerId": "w-beta-1"}]);
                }),
                "DESIRED_TAIL_TARGET_SCOPE",
            ),
            (
                "tail-missing-target",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["tailConsumers"] = json!([{"workerId": "w-nope"}])
                }),
                "DESIRED_TAIL_TARGET_SCOPE",
            ),
            // `[observability]` — the rate is the only bound-checked
            // field (booleans carry no range semantics).
            (
                "observability-ok",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["observability"] = json!({
                        "enabled": true,
                        "logsEnabled": true,
                        "invocationLogs": false,
                        "headSamplingRatePpm": 250000
                    })
                }),
                "",
            ),
            (
                "observability-rate-high",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["observability"] = json!({
                        "enabled": true,
                        "logsEnabled": true,
                        "invocationLogs": true,
                        "headSamplingRatePpm": 1500000
                    })
                }),
                "DESIRED_OBSERVABILITY_BOUNDS",
            ),
            (
                "observability-rate-negative",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["observability"] = json!({
                        "enabled": true,
                        "logsEnabled": true,
                        "invocationLogs": true,
                        "headSamplingRatePpm": 1000001
                    })
                }),
                "DESIRED_OBSERVABILITY_BOUNDS",
            ),
            // `{"type":"json"}` — version_metadata / typed vars carrier.
            (
                "json-ok",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({
                            "type": "json",
                            "name": "CF_VERSION_METADATA",
                            "value": "{\"id\":\"v1\",\"tag\":\"\",\"timestamp\":\"1\"}"
                        }));
                }),
                "",
            ),
            (
                "json-invalid-payload",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"type": "json", "name": "J", "value": "{not json"}));
                }),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "json-oversize",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({
                            "type": "json",
                            "name": "J",
                            "value": format!("\"{}\"", "x".repeat(MAX_JSON_BINDING_BYTES))
                        }));
                }),
                "DESIRED_CAPACITY_EXCEEDED",
            ),
            (
                "bad-assets-digest",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["bindings"][6]["manifestDigest"] = json!("zz")
                }),
                "DESIRED_INVALID_DIGEST",
            ),
            (
                "bad-bucket",
                Box::new(|v| v["tenants"][0]["workers"][0]["bindings"][3]["bucket"] = json!("ab")),
                "DESIRED_INVALID_IDENTIFIER",
            ),
            (
                "dup-hostname",
                Box::new(|v| {
                    v["tenants"][0]["workers"][1]["routes"] =
                        json!([{"hostname": "api--team-alpha.vibeploy.app"}]);
                }),
                "DESIRED_DUPLICATE",
            ),
            (
                "bad-hostname-wildcard",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["routes"][0]["hostname"] = json!("*.vibeploy.app")
                }),
                "DESIRED_INVALID_HOSTNAME",
            ),
            (
                "bad-hostname-upper",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["routes"][0]["hostname"] =
                        json!("API--team-alpha.vibeploy.app")
                }),
                "DESIRED_INVALID_HOSTNAME",
            ),
            (
                "bad-hostname-single-label",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["routes"][0]["hostname"] = json!("localhost")
                }),
                "DESIRED_INVALID_HOSTNAME",
            ),
            (
                "bad-cron-fields",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["crons"][0]["expression"] = json!("* * * *")
                }),
                "DESIRED_INVALID_CRON_EXPRESSION",
            ),
            (
                "bad-cron-range",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["crons"][0]["expression"] = json!("61 * * * *")
                }),
                "DESIRED_INVALID_CRON_EXPRESSION",
            ),
            (
                "bad-cron-step",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["crons"][0]["expression"] = json!("*/0 * * * *")
                }),
                "DESIRED_INVALID_CRON_EXPRESSION",
            ),
            (
                "bad-cron-reversed-range",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["crons"][0]["expression"] = json!("30-10 * * * *")
                }),
                "DESIRED_INVALID_CRON_EXPRESSION",
            ),
            (
                "quota-cpu-zero",
                Box::new(|v| v["tenants"][0]["workers"][0]["quotas"]["cpuMs"] = json!(0)),
                "DESIRED_QUOTA_BOUNDS",
            ),
            (
                "quota-cpu-high",
                Box::new(|v| v["tenants"][0]["workers"][0]["quotas"]["cpuMs"] = json!(300_001)),
                "DESIRED_QUOTA_BOUNDS",
            ),
            (
                "quota-mem-zero",
                Box::new(|v| v["tenants"][0]["workers"][0]["quotas"]["memoryMb"] = json!(0)),
                "DESIRED_QUOTA_BOUNDS",
            ),
            (
                "quota-mem-high",
                Box::new(|v| v["tenants"][0]["workers"][0]["quotas"]["memoryMb"] = json!(4097)),
                "DESIRED_QUOTA_BOUNDS",
            ),
            (
                "quota-subreq-high",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["quotas"]["subrequests"] = json!(10_001)
                }),
                "DESIRED_QUOTA_BOUNDS",
            ),
            (
                "quota-conns-high",
                Box::new(|v| {
                    v["tenants"][0]["workers"][0]["quotas"]["outboundConnections"] = json!(1025)
                }),
                "DESIRED_QUOTA_BOUNDS",
            ),
        ];
        for (id, mutate, expected) in cases {
            let mut value = valid_payload();
            mutate(&mut value);
            let parsed = desired(&value).unwrap_or_else(|e| panic!("{id}: decode failed: {e}"));
            let result = parsed.validate();
            if expected.is_empty() {
                assert!(result.is_ok(), "{id}: {result:?}");
            } else {
                let err = result
                    .err()
                    .unwrap_or_else(|| panic!("{id}: expected {expected}"));
                assert_eq!(err.code(), expected, "{id}");
            }
        }
    }

    /// P3c-1: DO declarations + bindings — fail-closed vectors.
    #[test]
    fn durable_objects_table_driven() {
        let key64 = "d".repeat(64);
        // Helper closures: declare classes / add a DO binding on worker idx.
        let declare = |v: &mut serde_json::Value, w: usize, name: &str| {
            let worker = &mut v["tenants"][0]["workers"][w];
            if worker["durableObjectClasses"].is_null() {
                worker["durableObjectClasses"] = json!([]);
            }
            worker["durableObjectClasses"]
                .as_array_mut()
                .unwrap()
                .push(json!({"className": name, "uniqueKey": "d".repeat(64), "enableSql": true}));
        };
        let bind = |v: &mut serde_json::Value, w: usize, b: serde_json::Value| {
            v["tenants"][0]["workers"][w]["bindings"]
                .as_array_mut()
                .unwrap()
                .push(b);
        };

        // 1. valid: self-declared class + binding.
        let mut v = valid_payload();
        declare(&mut v, 0, "Counter");
        bind(
            &mut v,
            0,
            json!({"type":"durableObject","name":"COUNTER","className":"Counter"}),
        );
        assert!(desired(&v).unwrap().validate().is_ok());

        // 2. valid: same-tenant cross-worker binding (script_name resolved).
        let mut v = valid_payload();
        declare(&mut v, 1, "Counter");
        bind(
            &mut v,
            0,
            json!({"type":"durableObject","name":"C","className":"Counter","serviceWorkerId":"w-alpha-2"}),
        );
        assert!(desired(&v).unwrap().validate().is_ok());

        // 3. binding to a class nobody declares → fail closed.
        let mut v = valid_payload();
        bind(
            &mut v,
            0,
            json!({"type":"durableObject","name":"C","className":"Ghost"}),
        );
        assert_eq!(desired(&v).unwrap().validate(), Err(DesiredError::DoClass));

        // 4. foreign worker declares it, tenant does not → fail.
        let mut v = valid_payload();
        declare(&mut v, 0, "Counter");
        bind(
            &mut v,
            0,
            json!({"type":"durableObject","name":"C","className":"Counter","serviceWorkerId":"w-other-tenant"}),
        );
        assert_eq!(desired(&v).unwrap().validate(), Err(DesiredError::DoClass));

        // 5. invalid class name on declaration.
        let mut v = valid_payload();
        v["tenants"][0]["workers"][0]["durableObjectClasses"] =
            json!([{"className":"1Bad","uniqueKey":key64,"enableSql":true}]);
        assert_eq!(
            desired(&v).unwrap().validate(),
            Err(DesiredError::Identifier("durableObjectClasses.className"))
        );

        // 6. duplicate class names.
        let mut v = valid_payload();
        v["tenants"][0]["workers"][0]["durableObjectClasses"] = json!([
            {"className":"C","uniqueKey":key64,"enableSql":true},
            {"className":"C","uniqueKey":"e".repeat(64),"enableSql":false}
        ]);
        assert_eq!(
            desired(&v).unwrap().validate(),
            Err(DesiredError::Duplicate("durableObjectClasses.className"))
        );

        // 7. non-64-hex / uppercase uniqueKey — storage id is
        // server-minted, never client-shaped.
        for bad in ["f".repeat(32), "D".repeat(64), "g".repeat(64)] {
            let mut v = valid_payload();
            v["tenants"][0]["workers"][0]["durableObjectClasses"] =
                json!([{"className":"C","uniqueKey":bad,"enableSql":true}]);
            assert_eq!(
                desired(&v).unwrap().validate(),
                Err(DesiredError::Digest("durableObjectClasses.uniqueKey"))
            );
        }

        // 8. binding className malformed.
        let mut v = valid_payload();
        declare(&mut v, 0, "Counter");
        bind(
            &mut v,
            0,
            json!({"type":"durableObject","name":"C","className":"bad name"}),
        );
        assert!(desired(&v).unwrap().validate().is_err());
    }

    fn bind_rl(v: &mut serde_json::Value, b: serde_json::Value) {
        v["tenants"][0]["workers"][0]["bindings"]
            .as_array_mut()
            .unwrap()
            .push(b);
    }

    #[test]
    fn ratelimit_binding_validation_fails_closed() {
        let ok = json!({"type":"ratelimit","name":"RL","namespaceId":"1001","limit":40,"period":60});
        let mut v = valid_payload();
        bind_rl(&mut v, ok.clone());
        assert_eq!(desired(&v).unwrap().validate(), Ok(()));
        let mut v10 = ok.clone();
        v10["period"] = json!(10);
        let mut v = valid_payload();
        bind_rl(&mut v, v10);
        assert_eq!(desired(&v).unwrap().validate(), Ok(()));

        let mut cases: Vec<(&str, serde_json::Value, &str)> = Vec::new();
        for (field, val, what) in [
            ("period", json!(30), "bindings.period"),
            ("period", json!(0), "bindings.period"),
            ("limit", json!(0), "bindings.limit"),
            ("limit", json!(1_000_001), "bindings.limit"),
            ("namespaceId", json!(""), "bindings.namespaceId"),
            ("namespaceId", json!("12a"), "bindings.namespaceId"),
            ("namespaceId", json!("1".repeat(33)), "bindings.namespaceId"),
        ] {
            let mut b = ok.clone();
            b[field] = val;
            cases.push((field, b, what));
        }
        for (_, b, what) in cases {
            let mut v = valid_payload();
            bind_rl(&mut v, b);
            assert_eq!(
                desired(&v).unwrap().validate(),
                Err(DesiredError::Identifier(what))
            );
        }
        // Unknown field is rejected (deny_unknown_fields), not ignored.
        let mut b = ok.clone();
        b["burst"] = json!(5);
        let mut v = valid_payload();
        bind_rl(&mut v, b);
        assert!(desired(&v).is_err());
        // Per-worker count cap.
        let mut v = valid_payload();
        for i in 0..=MAX_RATELIMIT_BINDINGS_PER_WORKER {
            bind_rl(&mut v, json!({"type":"ratelimit","name":format!("RL{i}"),"namespaceId":"1","limit":1,"period":10}),
            );
        }
        assert_eq!(
            desired(&v).unwrap().validate(),
            Err(DesiredError::Capacity("ratelimitBindings"))
        );
    }

    #[test]
    fn capacity_bounds() {
        let mut value = valid_payload();
        value["tenants"] = json!((0..=MAX_TENANTS)
            .map(|i| json!({
                "tenantId": format!("t{i}"), "workers": []
            }))
            .collect::<Vec<_>>());
        assert_eq!(
            desired(&value).unwrap().validate(),
            Err(DesiredError::Capacity("tenants"))
        );

        let mut value = valid_payload();
        for i in 0..=MAX_BINDINGS_PER_WORKER {
            value["tenants"][0]["workers"][0]["bindings"]
                .as_array_mut()
                .unwrap()
                .push(json!({"type":"var","name":format!("V{i}"),"varRef":"r"}));
        }
        assert_eq!(
            desired(&value).unwrap().validate(),
            Err(DesiredError::Capacity("bindings"))
        );
    }

    #[test]
    fn unknown_and_missing_fields_fail_closed() {
        // Unknown payload field → decode error (deny_unknown_fields).
        let mut value = valid_payload();
        value["unexpected"] = json!(1);
        assert!(desired(&value).is_err());
        let mut value = valid_payload();
        value["tenants"][0]["workers"][0]["bindings"][0]["extra"] = json!(1);
        assert!(desired(&value).is_err());
        // Missing required field → decode error.
        let mut value = valid_payload();
        value["tenants"][0]["workers"][0]
            .as_object_mut()
            .unwrap()
            .remove("quotas");
        assert!(desired(&value).is_err());
        // Unknown binding type → decode error.
        let mut value = valid_payload();
        value["tenants"][0]["workers"][0]["bindings"][0]["type"] = json!("d1");
        assert!(desired(&value).is_err());
    }

    fn sign_desired(payload: &ComputeDesired, cell: &str, revision: u64) -> Vec<u8> {
        sign(
            &SignParams {
                kind: KIND,
                scope: &cell_scope(cell),
                revision,
                issued_at: now(),
                expires_at: now() + std::time::Duration::from_secs(3600),
                payload,
            },
            &key(),
        )
        .unwrap()
        .bytes
    }

    #[test]
    fn end_to_end_sign_verify() {
        let payload: ComputeDesired = desired(&valid_payload()).unwrap();
        let raw = sign_desired(&payload, "cell-01", 7);
        let keys = [key().trusted_key()];
        let mut guard = RevisionGuard::new();
        let verified = verify_compute_desired(&raw, "cell-01", &keys, &mut guard, now()).unwrap();
        assert_eq!(verified.payload, payload);
        assert_eq!(guard.last_accepted(), 7);

        // Wrong cell scope → ScopeMismatch envelope error.
        assert!(matches!(
            verify_compute_desired(&raw, "cell-02", &keys, &mut RevisionGuard::new(), now()),
            Err(ComputeDesiredError::Snapshot(SnapshotError::ScopeMismatch))
        ));

        // Payload cellId diverges from envelope scope → EnvelopeBinding.
        let mut tampered = payload.clone();
        tampered.cell_id = "cell-02".into();
        let raw2 = sign_desired(&tampered, "cell-01", 8);
        let mut guard2 = RevisionGuard::new();
        assert_eq!(
            verify_compute_desired(&raw2, "cell-01", &keys, &mut guard2, now())
                .err()
                .map(|e| e.code()),
            Some("DESIRED_ENVELOPE_BINDING")
        );

        // Payload revision diverges from envelope revision → EnvelopeBinding.
        let mut tampered = payload.clone();
        tampered.revision = 9;
        let raw3 = sign_desired(&tampered, "cell-01", 8);
        assert_eq!(
            verify_compute_desired(&raw3, "cell-01", &keys, &mut RevisionGuard::new(), now())
                .err()
                .map(|e| e.code()),
            Some("DESIRED_ENVELOPE_BINDING")
        );

        // Semantically invalid payload keeps the fence unmoved.
        let mut bad = payload.clone();
        bad.tenants[0].workers[0].quotas.cpu_ms = 0;
        let raw4 = sign_desired(&bad, "cell-01", 10);
        let mut guard3 = RevisionGuard::new();
        assert_eq!(
            verify_compute_desired(&raw4, "cell-01", &keys, &mut guard3, now())
                .err()
                .map(|e| e.code()),
            Some("DESIRED_QUOTA_BOUNDS")
        );
        assert_eq!(
            guard3.last_accepted(),
            0,
            "invalid payload must not advance the fence"
        );
    }

    #[test]
    fn cron_expressions_accept_standard_forms() {
        for expr in [
            "* * * * *",
            "*/5 * * * *",
            "0 0 * * *",
            "30 14 1 * *",
            "0 0 * * 1",
            "0 0 * * 7",
            "0 0 * * SUN",
            "0 9 * * MON-FRI",
            "0 0 L * *",
            "0 0 LW * *",
            "0 0 15W * *",
            "0 0 1W * *",
            "0 0 * * 6L",
            "0 0 * * FRIL",
            "0 0 * * MON#2",
            "0 0 * * 5#3",
            "0,30 * * * *",
            "0 0 1 jan *",
            "0 0 * jan-mar *",
            "15/45 * * * *",
            "0 0 29 feb *",
        ] {
            assert!(valid_cron_expression(expr), "rejected {expr}");
        }
        for expr in [
            "",
            "* * * *",
            "* * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 0",
            "* * * * 8",
            "0 0 32W * *",
            "0 0 1-5W * *",
            "0 0 * * L",
            "0 0 * * #2",
            "0 0 * * MON#0",
            "0 0 * * 1#6",
            "*/0 * * * *",
            "*/61 * * * *",
            "5-1 * * * *",
            "a b c d e",
            "*/* * * * *",
            "mon-fry * * * *",
        ] {
            assert!(!valid_cron_expression(expr), "accepted {expr}");
        }
    }

    #[test]
    fn hostname_validation_edges() {
        assert!(valid_hostname("api--team.vibeploy.app"));
        assert!(valid_hostname("a.b"));
        assert!(!valid_hostname("no_dots"));
        assert!(!valid_hostname("-bad.example.com"));
        assert!(!valid_hostname("bad-.example.com"));
        assert!(!valid_hostname("a..b.com"));
        assert!(!valid_hostname(&format!("{}.com", "a".repeat(64))));
        assert!(!valid_hostname("UPPER.com"));
    }

    /// T11/F6: shared TS↔Rust boundary vectors — both runtimes must
    /// agree on EVERY verdict. The fixture lives at the repo `shared/`
    /// root so drift between implementations fails both suites.
    #[test]
    fn shared_boundary_vectors() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../shared/vrt-desired-boundary-vectors.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cases = doc["cases"].as_array().expect("cases array");
        assert!(cases.len() >= 50, "fixture shrank — regression?");
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let field = case["field"].as_str().unwrap();
            let value = case["value"].as_str().unwrap();
            let expect = case["expect"].as_str().unwrap();
            let actual = match field {
                "identifier" => valid_identifier(value, MAX_IDENTIFIER_LEN),
                "workerName" => valid_worker_name(value),
                "bindingName" | "doClassName" => valid_binding_name(value),
                "bucket" => valid_bucket_name(value),
                "hostname" => valid_hostname(value),
                "cron" => valid_cron_expression(value),
                "compatDate" => valid_compat_date(value),
                "compatFlag" => valid_compat_flag(value),
                "lowerHex64" => is_lower_hex(value, 64),
                "workflowName" => valid_workflow_name(value),
                other => panic!("unknown field {other} in case {name}"),
            };
            assert_eq!(
                actual,
                expect == "accept",
                "case {name}: field={field} value={value:?} expected {expect}"
            );
        }
    }

    // ---------------------------------------------------------------
    // T31c: assets routing fields — serde + validation
    // ---------------------------------------------------------------

    fn assets_binding(extra: serde_json::Value) -> serde_json::Value {
        let mut b = serde_json::json!({
            "type": "assets", "name": "ASSETS",
            "manifestDigest": "b".repeat(64),
            "permissions": {"read": true, "write": false}
        });
        b.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        b
    }

    fn payload_with_binding(binding: serde_json::Value) -> serde_json::Value {
        let mut p = valid_payload();
        let workers = p["tenants"][0]["workers"].as_array_mut().unwrap();
        workers[0]["bindings"] = serde_json::json!([binding]);
        p
    }

    #[test]
    fn legacy_assets_binding_parses_without_routing_fields() {
        // Manifests published before T31c carry no routing fields —
        // they must keep parsing and deserializing with absent values.
        let p = payload_with_binding(assets_binding(serde_json::json!({})));
        let d = desired(&p).unwrap();
        let w = &d.tenants[0].workers[0];
        let BindingDesired::Assets(a) = &w.bindings[0] else {
            panic!()
        };
        assert_eq!(a.assets_first, None);
        assert_eq!(a.run_worker_first, None);
        assert!(!a.assets_first_enabled());
    }

    #[test]
    fn assets_first_and_run_worker_first_roundtrip() {
        for rwf in [
            serde_json::json!(true),
            serde_json::json!(["/api/*", "!/api/static/*"]),
        ] {
            let p = payload_with_binding(assets_binding(
                serde_json::json!({"assetsFirst": true, "runWorkerFirst": rwf}),
            ));
            let d = desired(&p).unwrap();
            let w = &d.tenants[0].workers[0];
            let BindingDesired::Assets(a) = &w.bindings[0] else {
                panic!()
            };
            assert!(a.assets_first_enabled());
            let d2 = desired(&serde_json::to_value(&d).unwrap()).unwrap();
            let BindingDesired::Assets(a2) = &d2.tenants[0].workers[0].bindings[0] else {
                panic!()
            };
            assert_eq!(a.run_worker_first, a2.run_worker_first);
        }
    }

    #[test]
    fn service_entrypoint_wire_shape() {
        let legacy = serde_json::json!({"type": "service", "name": "AUTH", "workerId": "w-2"});
        let b: BindingDesired = serde_json::from_value(legacy.clone()).unwrap();
        let BindingDesired::Service(s) = &b else { panic!() };
        assert_eq!(s.entrypoint, None);
        // Absent stays absent: legacy snapshots keep their canonical bytes.
        assert_eq!(serde_json::to_value(&b).unwrap(), legacy);

        let named = serde_json::json!(
            {"type": "service", "name": "AUTH", "workerId": "w-2", "entrypoint": "Greeter"}
        );
        let b: BindingDesired = serde_json::from_value(named.clone()).unwrap();
        let BindingDesired::Service(s) = &b else { panic!() };
        assert_eq!(s.entrypoint.as_deref(), Some("Greeter"));
        assert_eq!(serde_json::to_value(&b).unwrap(), named);

        let unknown = serde_json::json!(
            {"type": "service", "name": "AUTH", "workerId": "w-2", "ns": "other"}
        );
        assert!(serde_json::from_value::<BindingDesired>(unknown).is_err());
    }

    #[test]
    fn service_entrypoint_rules() {
        for ok in ["Greeter", "_x", "$y", "Admin2", "VRT"] {
            assert!(valid_service_entrypoint(ok), "{ok}");
        }
        for bad in ["", "default", "1A", "a-b", "a.b", "VrtWaituntilProbe", "vrtX", "Vrt"] {
            assert!(!valid_service_entrypoint(bad), "{bad}");
        }
        assert!(!valid_service_entrypoint(&"E".repeat(MAX_BINDING_NAME_LEN + 1)));
        assert!(valid_service_entrypoint(&"E".repeat(MAX_BINDING_NAME_LEN)));
    }

    #[test]
    fn run_worker_first_rule_validation() {
        assert!(valid_run_worker_first_rules(&[
            "/api/*".into(),
            "!/a".into()
        ]));
        assert!(!valid_run_worker_first_rules(&[]));
        assert!(!valid_run_worker_first_rules(&["!/a".into(), "!/b".into()]));
        assert!(!valid_run_worker_first_rules(&["/a".into(), "/a".into()]));
        assert!(!valid_run_worker_first_rules(&["x".repeat(101)]));
        let many: Vec<String> = (0..101).map(|i| format!("/{i}")).collect();
        assert!(!valid_run_worker_first_rules(&many));
    }

    #[test]
    fn worker_loader_wire_shape_and_validation() {
        // `{"type":"workerLoader","name":...}` is the signed desired
        // snapshot shape emitted by the control plane.
        let doc = serde_json::json!({"type": "workerLoader", "name": "LOADER"});
        let b: BindingDesired = serde_json::from_value(doc.clone()).unwrap();
        let BindingDesired::WorkerLoader(l) = &b else {
            panic!()
        };
        assert_eq!(l.name, "LOADER");
        assert_eq!(serde_json::to_value(&b).unwrap(), doc);

        // deny_unknown_fields: a resource ref is not part of the
        // contract and must not be silently accepted.
        let with_id = serde_json::json!({"type": "workerLoader", "name": "L", "id": "x"});
        assert!(serde_json::from_value::<BindingDesired>(with_id).is_err());

        // The loader name flows through the shared binding-name
        // checks: JS identifier required, duplicates rejected.
        for (name, err) in [
            ("1bad", DesiredError::BindingName),
            ("has-dash", DesiredError::BindingName),
        ] {
            let p = payload_with_binding(serde_json::json!({"type": "workerLoader", "name": name}));
            assert_eq!(desired(&p).unwrap().validate(), Err(err));
        }
        let dup = {
            let mut p = valid_payload();
            let workers = p["tenants"][0]["workers"].as_array_mut().unwrap();
            workers[0]["bindings"] = serde_json::json!([
                {"type": "workerLoader", "name": "L"},
                {"type": "workerLoader", "name": "L"},
            ]);
            p
        };
        assert_eq!(
            desired(&dup).unwrap().validate(),
            Err(DesiredError::Duplicate("bindingName"))
        );
        let ok =
            payload_with_binding(serde_json::json!({"type": "workerLoader", "name": "LOADER"}));
        assert!(desired(&ok).unwrap().validate().is_ok());
    }

    #[test]
    fn unsafe_eval_wire_shape_and_validation() {
        // `{"type":"unsafeEval","name":...}` is the signed desired
        // snapshot shape emitted by the control plane (CF
        // `[[unsafe.bindings]] type="unsafe_eval"`).
        let doc = serde_json::json!({"type": "unsafeEval", "name": "UNSAFE_EVAL"});
        let b: BindingDesired = serde_json::from_value(doc.clone()).unwrap();
        let BindingDesired::UnsafeEval(u) = &b else {
            panic!()
        };
        assert_eq!(u.name, "UNSAFE_EVAL");
        assert_eq!(serde_json::to_value(&b).unwrap(), doc);

        // deny_unknown_fields: extra keys must not be silently
        // accepted — raw binding entries carry only a name.
        let with_extra = serde_json::json!({"type": "unsafeEval", "name": "E", "level": "full"});
        assert!(serde_json::from_value::<BindingDesired>(with_extra).is_err());

        // The binding name flows through the shared binding-name
        // checks: JS identifier required, duplicates rejected.
        for (name, err) in [
            ("1bad", DesiredError::BindingName),
            ("has-dash", DesiredError::BindingName),
        ] {
            let p = payload_with_binding(serde_json::json!({"type": "unsafeEval", "name": name}));
            assert_eq!(desired(&p).unwrap().validate(), Err(err));
        }
        let dup = {
            let mut p = valid_payload();
            let workers = p["tenants"][0]["workers"].as_array_mut().unwrap();
            workers[0]["bindings"] = serde_json::json!([
                {"type": "unsafeEval", "name": "E"},
                {"type": "unsafeEval", "name": "E"},
            ]);
            p
        };
        assert_eq!(
            desired(&dup).unwrap().validate(),
            Err(DesiredError::Duplicate("bindingName"))
        );
        let ok =
            payload_with_binding(serde_json::json!({"type": "unsafeEval", "name": "UNSAFE_EVAL"}));
        assert!(desired(&ok).unwrap().validate().is_ok());
    }
}
