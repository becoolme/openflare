//! `capnp` text generation: one `compute.desired` tenant fragment plus the
//! version bundles and runtime parameters → one deterministic workerd
//! `config.capnp` document for the tenant's workerd process.
//!
//! Layout (mirrors the upstream `packages/runtime/config.capnp` service
//! grammar, minus the dynamic-loader machinery):
//!
//! - one `(name = "worker-<workerId>", worker = …)` service per tenant
//!   worker: modules `embed`d from `bundles/<bundleDigest>/<moduleName>`,
//!   bindings per [`BindingDesired`], `globalOutbound` → the egress
//!   system worker;
//! - one service per [`SystemWorkerRef`] (facade + egress placeholders):
//!   modules `embed`d from `sys/<digest>`; facade workers get
//!   `BINDING_BACKEND` when requested; all non-egress workers share the
//!   tenant `globalOutbound` to the egress service, the egress worker
//!   itself gets `globalOutbound = "outbound-network"`;
//! - `(name = "binding-backend", external = (http = ()))` and
//!   `(name = "outbound-network", network = …)` fixed internal services —
//!   the binding-backend unix address is injected by vrt-host via
//!   `--external-addr` (returned in [`RenderedConfig::external_addrs`]),
//!   matching the upstream spawn convention. **Tenant workers never bind
//!   these services directly** (only facade system workers hold
//!   `BINDING_BACKEND`); see the service-scope invariant below;
//! - one `unix:` socket per worker (`<socketDir>/<workerId>.sock`):
//!   vrt-host's local ingress maps `Host → workerId → socket` (main design
//!   §5.1), so the capnp exposes one socket per worker, not per process.
//!
//! Determinism: services, sockets, bindings and module lists are emitted
//! in declaration order; system workers are sorted by service name; all
//! string fields pass a whitelist or full escaping — identical inputs
//! produce identical bytes, and [`RenderedConfig::digest`] is the
//! sha256 of the document.

use crate::bundle::VersionBundle;
use crate::canonical::canonical_bytes;
use crate::canonical::utf16_cmp;
use crate::egress::{EgressPolicy, EGRESS_POLICY_BINDING};
use crate::error::ConfigError;
use crate::escape::capnp_text;
use crate::manifest::{valid_compat_date, valid_compat_flag, valid_module_name, MAX_COMPAT_FLAGS};
use serde_json::json;
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::{
    BindingDesired, RunWorkerFirst, TenantDesired, WorkerDesired, WorkerQuotas, MAX_IDENTIFIER_LEN,
};
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// Maximum length of a capnp service/worker name.
pub const MAX_SERVICE_NAME_LEN: usize = 64;
/// Maximum length of a filesystem path parameter (socket dir / socket).
pub const MAX_PATH_LEN: usize = 512;
/// Maximum length of a resolved var/secret value (bytes) — bounds the
/// config file size a host parameter set can force.
pub const MAX_VALUE_LEN: usize = 64 * 1024;
/// Maximum length of an entrypoint (exported class) name.
pub const MAX_ENTRYPOINT_LEN: usize = 64;
/// Directory (relative to the config file) where vrt-host materializes
/// extracted bundles: `bundles/<bundleDigest>/<moduleName>`.
pub const BUNDLE_DIR_PREFIX: &str = "bundles";
/// Directory where vrt-host materializes system modules: `sys/<digest>`.
pub const SYS_DIR_PREFIX: &str = "sys";
/// PT1: in-process watchdog heartbeat service + socket name — one
/// per tenant process, never on ingress, never service-bindable, and
/// rendered only when `params.heartbeat_socket` is set.
pub const HEARTBEAT_SERVICE: &str = "vrt-heartbeat";
/// capnp socket name for the heartbeat endpoint — deliberately NOT
/// `sock-*`: the ready classifier treats only `sock-*` names as
/// declared tenant endpoints (an unexpected `sock-` event fails the
/// generation install); a non-sock name is ignored there, which is
/// correct — the heartbeat is host-internal, not a tenant socket.
pub const HEARTBEAT_SOCKET_NAME: &str = "vrt-heartbeat";

/// MIG-13: switch-completion marker socket — emitted only when the
/// render carries DO storage. The prewarmed generation runs a twin
/// config with this socket stripped (and `disk` paths repointed at
/// scratch dirs); the live write that restores real storage re-adds
/// it, so its `listen` control event after a `--watch` reload is the
/// deterministic "final config applied" signal (a same-content reload
/// can never produce it). Non-`sock-*` by the same rule as the
/// heartbeat: never ingress-mapped, never a tenant endpoint.
pub const SWITCH_MARKER_SOCKET: &str = "vrt-switch";
/// Marker socket file name inside the probe socket's dir. MIG-13-r4:
/// never longer than the probe socket file (`vrt-wu.sock`) — the two
/// live in the SAME dir, so the assemble `check_socket_path_len`
/// preflight guarantees "probe fits ⇒ marker fits" (`vrt-switch.sock`
/// was 4 bytes longer and failed deploys the probe still passed —
/// the r4 gate). The socket NAME stays `vrt-switch`: the supervisor
/// keys on the listen-event socket name, never the file path, so the
/// rename is transparent to rolling upgrades.
pub const SWITCH_MARKER_FILE: &str = "vrt-sw.sock";

// Compile-time invariant: the marker file must not be longer than
// the probe socket file (`<PROBE_SOCKET_NAME>.sock`). Same parent
// dir ⇒ the marker path can never exceed the probe path that the
// `check_socket_path_len` preflight already gates.
const _: () = assert!(
    SWITCH_MARKER_FILE.len() <= crate::waituntil::PROBE_SOCKET_NAME.len() + ".sock".len(),
    "SWITCH_MARKER_FILE must not be longer than the probe socket file name"
);

/// Service name of the binding-backend external service.
pub const BINDING_BACKEND_SERVICE: &str = "binding-backend";
/// Service name of the raw network used as the egress worker's own
/// `globalOutbound`.
pub const OUTBOUND_NETWORK_SERVICE: &str = "outbound-network";
/// MIG-15: loopback-only network used exclusively for `connect()` to
/// synthetic internal hosts — it terminates at the provider-side
/// pg-wire proxy inside the same pod. `allow=["local"]` means the
/// only reachable target is loopback; nothing else is added.
pub const INTERNAL_NETWORK_SERVICE: &str = "internal-network";

/// MIG-10: request header workerd's `cfBlobHeader` reads on the
/// tenant-ingress socket — the host-synthesized JSON becomes
/// `request.cf` (worker-side), replacing the unix-socket peer
/// credentials (`clientPid`/`clientUid`) that otherwise leak through.
/// `x-vrt-` prefix = vrt-host's inbound scrub strips any client copy.
/// Cross-pinned with `CF_BLOB_HEADER` in vrt-host/src/ingress.rs.
pub const CF_BLOB_HEADER: &str = "x-vrt-cf-blob";

/// C1: per-tenant Cache API external (`cacheApiOutbound` designator)
/// — bound per tenant to a vrt-host unix socket via
/// `--external-addr=vrt-cache=<addr>`; see [`RuntimeParams::cache_addr`].
pub const CACHE_SERVICE: &str = "vrt-cache";

/// A service name + entrypoint pair (facade/egress targets).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceEndpoint {
    /// capnp service name.
    pub service: String,
    /// `WorkerEntrypoint` class name to invoke on the target service.
    pub entrypoint: String,
}

/// Facade services tenant bindings route to (Phase 1 set).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FacadeRefs {
    /// `kv` bindings → this facade worker + entrypoint (`KVNamespace`).
    pub kv: ServiceEndpoint,
    /// `r2` bindings → (`R2Transport`), rendered as a native `r2Bucket`
    /// binding (see `docs/r2-binding.md`).
    pub r2: ServiceEndpoint,
    /// `assets` bindings → (`AssetTransport`).
    pub assets: ServiceEndpoint,
    /// `pg` bindings → (`PgTransport`). Tenant workers never reach
    /// `binding-backend` directly; the pg facade holds that privilege.
    pub pg: ServiceEndpoint,
    /// `d1` bindings → wrapped `cloudflare-internal:d1-api` whose inner
    /// `fetcher` service is this facade (T34).
    pub d1: ServiceEndpoint,
    /// `queue` bindings → native workerd `queue` binding whose inner
    /// `service` is this facade (T37 broker wire: /message /batch /metrics).
    pub queue: ServiceEndpoint,
    /// `k2` bindings → wrapped `cloudflare-internal:k2` whose inner
    /// `fetcher` service is this facade (K2 produce wire: POST /send).
    pub k2: ServiceEndpoint,
    /// `images` bindings → wrapped `cloudflare-internal:images-api`
    /// whose inner `fetcher` service is this facade — transforms run
    /// inside facade-images on the vendored WASM engine.
    pub images: ServiceEndpoint,
    /// `ratelimit` bindings -> plain service binding to this facade's
    /// `RateLimiter` entrypoint (MIG-16; no binding-backend).
    pub ratelimit: ServiceEndpoint,
    /// `send_email` bindings → wrapped `cloudflare-internal:send-email`
    /// whose inner `fetcher` service is this facade (POST /send).
    pub email: ServiceEndpoint,
    /// `vectorize` bindings → wrapped `cloudflare-internal:vectorize-api`
    /// whose inner `fetcher` service is this facade → provider's
    /// sqlite-vec index backend.
    pub vectorize: ServiceEndpoint,
}

/// A system module referenced by content digest (the artifact for a
/// facade/egress worker — produced by T4/T6, referenced here).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemModuleRef {
    /// Module name (same rule as bundle module names).
    pub name: String,
    /// sha256 of the module bytes, lowercase hex (64 chars).
    pub digest: String,
}

/// The capnp `Worker.Module` union member a system module emits: `.wasm`
/// names embed as `wasm` (workerd hands the importer a `WebAssembly.Module`),
/// everything else as `esModule`.
pub fn system_module_capnp_field(name: &str) -> &'static str {
    if name.ends_with(".wasm") {
        "wasm"
    } else {
        "esModule"
    }
}

/// A system worker placeholder: a named worker whose modules are
/// content-addressed references vrt-host materializes from the artifact
/// store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemWorkerRef {
    /// capnp service name (e.g. `facade-kv`, `vrt-egress`).
    pub service_name: String,
    /// Entry module name; must be present in `modules` (emitted first —
    /// workerd treats `modules[0]` as the worker entry).
    pub main_module: String,
    /// Content-addressed modules (at least one).
    pub modules: Vec<SystemModuleRef>,
    /// Whether to bind `BINDING_BACKEND` → `binding-backend` (facades
    /// need it to reach vrt-provider; the egress worker does not).
    pub needs_binding_backend: bool,
}

/// A resolved secret value. It is emitted **only** into
/// [`RenderedConfig::env`] for vrt-host to inject into the tenant
/// process environment — never into the config text or its digest.
/// `Debug` is redacted and no `Display` exists, so the value cannot leak
/// through logs, diagnostics, or accidental formatting.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    /// Wrap a resolved secret.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Explicitly expose the plaintext. The only legitimate caller is
    /// vrt-host's child-process environment injection.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretValue(***)")
    }
}

/// Runtime parameters injected at render time — never present in
/// `compute.desired` (vars values, secrets, socket paths, system refs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeParams {
    /// Directory (relative or absolute path) that holds the per-worker
    /// ingress unix sockets: `<socketDir>/<workerId>.sock`.
    pub socket_dir: String,
    /// Per-tenant binding-backend unix socket path; returned in
    /// `external_addrs` for vrt-host's `--external-addr` flag (never
    /// written into the config, same convention as upstream).
    pub binding_backend_socket: String,
    /// `globalOutbound` target for tenant + non-egress system workers
    /// (the T6 egress entrypoint).
    pub egress: ServiceEndpoint,
    /// Egress policy rendered into the `VRT_EGRESS_POLICY` text binding
    /// on the egress worker (T6; `egress.rs` shares the shape with
    /// `facade/egress/policy.ts`).
    pub egress_policy: EgressPolicy,
    /// Deny-all `globalOutbound` target for every non-egress system
    /// worker (`vrt-no-network`/`DenyAll`, T6 review): facades only ever
    /// talk to `binding-backend`, so their outbound must be a worker
    /// that always denies. workerd defaults `globalOutbound` to
    /// `internet`, so an explicit deny designator is mandatory — a
    /// network-service `allow = []` alternative is unverified.
    pub deny_outbound: ServiceEndpoint,
    /// Facade service endpoints for `kv` / `r2` / `assets` bindings.
    pub facades: FacadeRefs,
    /// System worker definitions; `egress.service` and every
    /// `facades.*.service` must resolve here.
    pub system_workers: Vec<SystemWorkerRef>,
    /// `compatibilityDate` for system workers (platform lockfile value).
    pub system_compat_date: String,
    /// `compatibilityFlags` for system workers.
    pub system_compat_flags: Vec<String>,
    /// `varRef` → plaintext value, resolved out-of-band by the host.
    pub vars: BTreeMap<String, String>,
    /// `secretRef` → secret value, resolved from encrypted storage.
    /// Secrets become `fromEnvironment` bindings and are returned via
    /// [`RenderedConfig::env`] for vrt-host's child-process env — never
    /// written into the config text, argv, or logs.
    /// T13/M6: `tenantId → secretRef → value` — per-tenant
    /// namespaces; a ref resolves only inside the owning tenant's
    /// map (fail closed on cross-tenant guesses).
    pub secrets: BTreeMap<String, BTreeMap<String, SecretValue>>,
    /// P3c-1: absolute path of THIS tenant's Durable Object storage
    /// root (`<doRoot>/<seg(tenantId)>` — vrt-host pre-creates it and
    /// `<dir>/<workerId>` per declaring worker, 0700 + tenant uid).
    /// Empty ⇒ no DO namespaces may be emitted; a tenant declaring
    /// `durableObjectClasses` with an empty dir fails closed.
    pub do_storage_dir: String,
    /// W3-r2: host-process pepper for wfop facade token derivation
    /// (`HMAC-SHA256(pepper, "vrt-wfop:"+uniqueKey)`). Set per-assemble
    /// from the Assembler's in-process random material — never read
    /// from host config, persisted, or carried on the op channel.
    /// Empty ⇒ any workflow-declaring worker fails its emit surface
    /// (per-worker skip; an all-workflow tenant gets
    /// `AllWorkersExcluded`).
    pub wfop_pepper: String,
    /// W6: vrt-host control-listen address (`127.0.0.1:port`) — the
    /// `vrt-wfdel` external service's `--external-addr` target for
    /// workflow-storage deletes. Empty = no control listener → the
    /// loopback shim degrades to an explicit 501 (never fake success).
    pub wf_delete_addr: String,
    /// C1: this tenant's Cache API service `--external-addr` target
    /// (`unix:<path>` in production; `host:port` under tests). The
    /// path is per-tenant — tenant identity is the socket binding,
    /// never anything on the wire. Empty ⇒ no `cacheApiOutbound` is
    /// emitted and `caches.*` throws `No Cache was configured`
    /// (explicit failure, never silent success).
    pub cache_addr: String,
    /// AE: this tenant's Analytics Engine ingest socket
    /// `--external-addr` target (`unix:<path>`). Empty ⇒ a tenant
    /// declaring `analyticsEngine` bindings fails the whole render
    /// closed — an unbound dataset external would resolve to nothing
    /// and `writeDataPoint()` would fail silently inside workerd.
    pub ae_addr: String,
    /// PT1: watchdog heartbeat unix socket path INSIDE `socket_dir`.
    /// Non-empty ⇒ emit the `vrt-heartbeat` service + its private
    /// socket; empty ⇒ byte-identical legacy render (no heartbeat).
    pub heartbeat_socket: String,
    /// MIG-12: per-generation waitUntil probe unix socket path INSIDE
    /// `socket_dir`. Non-empty => every emitted tenant worker is wrapped
    /// by the platform entry modules (ctx.waitUntil counting) and the
    /// `vrt-wu-probe` aggregate service + `vrt-wu` socket are
    /// added; empty => byte-identical legacy render (no counting).
    pub waituntil_socket: String,
    /// Worker log collection (`doc/platform/workers-runtime-logs.md`):
    /// non-empty sink => every tenant worker gets a platform
    /// `vrt-tail-<id>` tail worker; default => byte-identical legacy
    /// render.
    pub log_tail: crate::logs::LogTailParams,
}

/// Inputs for [`render_tenant_config`].
#[derive(Clone)]
pub struct TenantRenderInput<'a> {
    /// One tenant's `compute.desired` fragment (already snapshot-validated
    /// upstream; rendering re-checks tenant-scoped invariants).
    pub tenant: &'a TenantDesired,
    /// Snapshot revision — emitted into facade props as
    /// `resourceSpecGeneration` (the binding-table generation).
    pub revision: u64,
    /// Version bundles for the workers' `activeVersion.artifactDigest`s.
    /// Missing bundles, extra bundles, or digest mismatch fail closed.
    pub bundles: &'a [VersionBundle],
    /// Runtime injection parameters.
    pub params: &'a RuntimeParams,
}

/// A file vrt-host must materialize next to the config so `embed`
/// resolves at compile time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbedRef {
    /// Path written into the capnp, relative to the config file dir.
    pub path: String,
    /// Expected sha256 of the file content (lowercase hex). The host
    /// verifies bytes against this digest after materialization.
    pub digest: String,
    /// What produced this embed path.
    pub kind: EmbedKind,
}

/// Provenance of an [`EmbedRef`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmbedKind {
    /// `bundles/<bundleDigest>/<moduleName>` — a tenant worker module
    /// extracted from the version bundle.
    BundleModule {
        /// Worker this module belongs to.
        worker_id: String,
        /// Module name inside the bundle.
        module_name: String,
    },
    /// `sys/<digest>` — a system (facade/egress) module artifact.
    SystemModule {
        /// System service the module belongs to.
        service_name: String,
        /// Module name declared in [`SystemModuleRef`].
        module_name: String,
    },
    /// `sys/<digest>` — a vendored module artifact compiled into
    /// vrt-config (W1 workflows engine + loopback stub). Content is
    /// resolved from `vrt_config::workflows::vendored_bytes`, NOT from
    /// params or the object store — the same bytes on every cell.
    VendoredModule {
        /// Registry key understood by `vendored_bytes`.
        name: String,
    },
}

/// An external-service address vrt-host injects via `--external-addr`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalAddr {
    /// capnp service name.
    pub service: String,
    /// Address value (`unix:<path>`).
    pub address: String,
}

/// Render output: the deterministic capnp text, its digest, and the
/// materialization contract for vrt-host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedConfig {
    /// The `config.capnp` text (deterministic — same inputs, same bytes).
    pub text: String,
    /// sha256 of `text` (lowercase hex).
    pub digest: String,
    /// Files that must exist under the config dir before compile.
    pub embeds: Vec<EmbedRef>,
    /// `--external-addr=<service>=<address>` argv for vrt-host.
    pub external_addrs: Vec<ExternalAddr>,
    /// Environment variables vrt-host must set on the tenant workerd
    /// process — the `fromEnvironment` secret bindings resolve against
    /// these. The config text/digest never contains these values.
    pub env: BTreeMap<String, SecretValue>,
    /// W3-r2: workers excluded from this render because their own emit
    /// surface failed the pre-render dry-run (per-worker fault
    /// isolation — one bad worker must not poison siblings sharing the
    /// tenant workerd). Skipped workers get no socket/service/embed —
    /// their routes answer `worker_unavailable` while siblings serve.
    pub skipped_workers: Vec<String>,
    /// W4-r2 P1: WHY each worker was skipped — stable error code plus a
    /// ≤512-char detail. `ConfigError` payloads are `&'static str`
    /// field names only, so the detail carries no secret/env/binding
    /// material by construction; truncation is a log-hygiene bound.
    pub skipped_reasons: Vec<SkippedWorkerReason>,
    /// W3-r2: `workerId → workflowName → facade token` for the serving
    /// generation. Rendered from the same params pepper + desired
    /// uniqueKeys as `WFOP_TABLE`; vrt-host injects the matching token
    /// at the `/v1/wfops` hop — the op channel itself never carries
    /// token material. Only non-skipped workers appear.
    pub wfop_tokens: BTreeMap<String, BTreeMap<String, String>>,
}

/// W4-r2 P1: one skipped worker's dry-run failure summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkippedWorkerReason {
    /// Excluded worker id.
    pub worker_id: String,
    /// Stable `ConfigError::code()`.
    pub code: &'static str,
    /// Display message, truncated to 512 chars.
    pub detail: String,
}

/// Render the deterministic workerd `config.capnp` for one tenant
/// process. All checks fail closed before any output is produced.
pub fn render_tenant_config(input: &TenantRenderInput<'_>) -> Result<RenderedConfig, ConfigError> {
    validate_params(input.params)?;
    validate_tenant(input.tenant)?;

    let bundles = match_bundles(input.tenant, input.bundles)?;

    // Service-name universe: tenant workers, system workers, fixed
    // internal services. Collisions fail closed.
    let mut service_names = HashSet::new();
    service_names.insert(BINDING_BACKEND_SERVICE.to_string());
    service_names.insert(OUTBOUND_NETWORK_SERVICE.to_string());
    for worker in &input.tenant.workers {
        if !service_names.insert(worker_service_name(&worker.worker_id)) {
            return Err(ConfigError::ServiceReference("services"));
        }
        if collects_logs(worker, input.params)
            && !service_names.insert(crate::logs::tail_service_name(&worker.worker_id))
        {
            return Err(ConfigError::ServiceReference("services"));
        }
        if worker_assets_first(worker).is_some()
            && !service_names.insert(router_service_name(&worker.worker_id))
        {
            return Err(ConfigError::ServiceReference("services"));
        }
    }
    for system in &input.params.system_workers {
        if !service_names.insert(system.service_name.clone()) {
            return Err(ConfigError::ServiceReference("systemWorkers.serviceName"));
        }
    }
    // PT1: a defined system worker may never shadow the watchdog
    // service when the heartbeat is emitted.
    if !input.params.heartbeat_socket.is_empty()
        && !service_names.insert(HEARTBEAT_SERVICE.to_string())
    {
        return Err(ConfigError::ServiceReference("heartbeat"));
    }
    // MIG-12: the waitUntil probe service is platform-reserved — a
    // system worker may never shadow it when the probe is emitted.
    if !input.params.waituntil_socket.is_empty()
        && !service_names.insert(crate::waituntil::PROBE_SERVICE.to_string())
    {
        return Err(ConfigError::ServiceReference("waituntilProbe"));
    }

    // Every referenced facade/egress endpoint must resolve to a defined
    // system worker (or an internal service for `binding-backend`).
    for (field, endpoint) in [
        ("egress", &input.params.egress),
        ("denyOutbound", &input.params.deny_outbound),
        ("facades.kv", &input.params.facades.kv),
        ("facades.r2", &input.params.facades.r2),
        ("facades.assets", &input.params.facades.assets),
        ("facades.pg", &input.params.facades.pg),
        ("facades.d1", &input.params.facades.d1),
        ("facades.ratelimit", &input.params.facades.ratelimit),
        ("facades.vectorize", &input.params.facades.vectorize),
    ] {
        // An endpoint must resolve to a defined system worker — and can
        // never be an internal service (`binding-backend`/`outbound-network`
        // are reserved and not defined as system workers, so the reserved
        // check below is belt and braces behind the collision check).
        if endpoint.service == BINDING_BACKEND_SERVICE
            || endpoint.service == OUTBOUND_NETWORK_SERVICE
            || !input
                .params
                .system_workers
                .iter()
                .any(|s| s.service_name == endpoint.service)
        {
            return Err(ConfigError::ServiceReference(field));
        }
    }

    // P3c-1: DO declarations require a configured per-tenant storage
    // root — emitting namespaces without a DiskDirectory would produce
    // a config workerd rejects anyway; failing here keeps the previous
    // generation serving (fail closed at assemble depth).
    if input
        .tenant
        .workers
        .iter()
        .any(|w| !w.durable_object_classes.is_empty() || !w.workflows.is_empty())
        && input.params.do_storage_dir.is_empty()
    {
        return Err(ConfigError::Identifier("doStorageDir"));
    }

    // AE: `analyticsEngine` bindings need the tenant ingest socket —
    // without it the dataset-named `external` service would have no
    // `--external-addr` and workerd could not resolve the designator
    // (fail closed at assemble depth, same as `doStorageDir`).
    if !ae_datasets(input.tenant).is_empty() && input.params.ae_addr.is_empty() {
        return Err(ConfigError::Identifier("aeAddr"));
    }

    // W3-r2 P0: per-worker fault isolation. Every worker in a tenant
    // shares one workerd process, so a render-fatal error inside ANY
    // worker's emit surface would otherwise kill the whole generation
    // (the standalone e2e cascade: one broken workflow worker → every
    // route `worker_unavailable`). Dry-run each worker's complete emit
    // surface into scratch first; a worker that fails is excluded —
    // its routes answer `worker_unavailable`, siblings keep serving —
    // and the exclusion is surfaced via `skipped_workers`. Iterate to
    // a fixpoint: worker B may only fail BECAUSE worker A was dropped
    // (e.g. a service binding targeting A).
    let mut desired_workers = input.tenant.workers.clone();
    let mut skipped_workers: Vec<String> = Vec::new();
    let mut skipped_reasons: Vec<SkippedWorkerReason> = Vec::new();
    loop {
        let filtered_tenant = TenantDesired {
            workers: desired_workers.clone(),
            ..input.tenant.clone()
        };
        let filtered_input = TenantRenderInput {
            tenant: &filtered_tenant,
            revision: input.revision,
            bundles: input.bundles,
            params: input.params,
        };
        let mut dropped: Vec<String> = Vec::new();
        for worker in &desired_workers {
            let mut scratch = String::new();
            let mut scratch_embeds = Vec::new();
            let mut scratch_env = BTreeMap::new();
            let bundle = bundles.get(worker.worker_id.as_str()).copied();
            if let Err(e) = emit_worker_surface(
                &mut scratch,
                worker,
                &filtered_input,
                bundle,
                &mut scratch_embeds,
                &mut scratch_env,
            ) {
                dropped.push(worker.worker_id.clone());
                // W4-r2 P1: keep the typed reason — the operator needs
                // to know WHICH worker died and WHY, not just a count.
                let mut detail = e.to_string();
                detail.truncate(512);
                skipped_reasons.push(SkippedWorkerReason {
                    worker_id: worker.worker_id.clone(),
                    code: e.code(),
                    detail,
                });
            }
        }
        if dropped.is_empty() {
            break;
        }
        skipped_workers.extend(dropped.iter().cloned());
        let drop_set: HashSet<&str> = dropped.iter().map(|d| d.as_str()).collect();
        desired_workers.retain(|w| !drop_set.contains(w.worker_id.as_str()));
    }
    if !skipped_workers.is_empty() && desired_workers.is_empty() {
        // Nothing servable would remain — keep the previous
        // generation serving instead of launching an empty workerd.
        return Err(ConfigError::AllWorkersExcluded);
    }
    // Render over the filtered worker set. When nothing was skipped
    // the filtered tenant is identical to the desired one, so output
    // stays byte-for-byte stable (golden-render contract).
    let filtered_tenant = TenantDesired {
        workers: desired_workers,
        ..input.tenant.clone()
    };
    let filtered_input = TenantRenderInput {
        tenant: &filtered_tenant,
        revision: input.revision,
        bundles: input.bundles,
        params: input.params,
    };
    let input = &filtered_input;

    let mut out = String::new();
    out.push_str("using Workerd = import \"/workerd/workerd.capnp\";\n\n");
    out.push_str(&format!(
        "# vrt tenant config; tenant={} revision={}\n",
        input.tenant.tenant_id, input.revision
    ));
    out.push_str("const config :Workerd.Config = (\n");
    out.push_str("  structuredLogging = true,\n");
    out.push_str("  services = [\n");

    let mut embeds = Vec::new();
    let mut env = BTreeMap::new();
    for worker in &input.tenant.workers {
        emit_tenant_worker(
            &mut out,
            worker,
            input,
            bundles[worker.worker_id.as_str()],
            &mut embeds,
            &mut env,
        )?;
    }

    // T31c: per-worker `router-<id>` services for assets-first
    // workers — emitted between tenant workers and system workers so
    // opted-out manifests render byte-identically to before.
    for worker in &input.tenant.workers {
        if let Some(assets) = worker_assets_first(worker) {
            emit_router_worker(&mut out, worker, assets, input, &mut embeds)?;
        }
    }

    // Logs: per-worker platform tail collectors (after routers so the
    // disabled path stays byte-identical).
    if input.params.log_tail.enabled() {
        for worker in &input.tenant.workers {
            emit_tail_worker(&mut out, worker, input)?;
        }
    }

    // T37b: per-worker `facade-qconsumer-<id>` services for workers
    // carrying queueConsumers — the dispatcher's delivery endpoint
    // (`env.CONSUMER.queue()` service-binding RPC); absent consumers
    // ⇒ byte-identical legacy output.
    for worker in &input.tenant.workers {
        if !worker.queue_consumers.is_empty() {
            emit_qconsumer_worker(&mut out, worker, input, &mut embeds)?;
        }
    }

    // P3c-1: per-worker `do-storage-<workerId>` DiskDirectory services —
    // the ActorStorage localDisk target (workerd.capnp Config.Service
    // `disk` variant). `<doStorageDir>/<workerId>` is created by
    // vrt-host (0700, tenant uid) BEFORE compile; absent dir ⇒ workerd
    // fails closed at startup. workerId is validated FS-safe (valid_
    // identifier charset has no `/`; `.`/`..` rejected in
    // validate_tenant) so the join cannot escape the tenant dir.
    for worker in &input.tenant.workers {
        emit_do_storage_service(&mut out, worker, input);
    }

    // W1: per-workflow `wf-engine-<definingId>-<name>` services —
    // each is a Worker carrying the vendored engine module + an
    // `Engine` DO namespace on the DEFINING worker's localDisk (W7:
    // `script_name` targets another same-tenant worker — storage and
    // ops follow CF ownership, not the binding's home). Internal
    // services: nothing routes a socket to them (ingress can only
    // reach `*.sock` endpoints) and their globalOutbound is deny-all.
    let mut any_workflow = false;
    for worker in &input.tenant.workers {
        let targets = wf_targets(input, &worker.worker_id);
        for (decl_worker, wf) in &targets {
            emit_workflow_engine(&mut out, decl_worker, wf, input, &mut embeds)?;
            any_workflow = true;
        }
        // W3: one platform wfops facade per DEFINING worker carrying
        // any workflow — the only channel the cell's op executor may
        // use (ops belong to `script_name`'s worker, W7).
        if !targets.is_empty() {
            emit_wfops_facade(&mut out, worker, &targets, input, &mut embeds)?;
            any_workflow = true;
        }
    }
    if any_workflow {
        for worker in &input.tenant.workers {
            let targets = wf_targets(input, &worker.worker_id);
            if targets.is_empty() {
                continue;
            }
            emit_workflow_loopback(&mut out, worker, &targets, input, &mut embeds)?;
        }
        // W6: the shared `vrt-wfdel` external designator — emitted
        // only when a control-listen address exists; unreferenced
        // externals would force a spurious --external-addr.
        if !input.params.wf_delete_addr.is_empty() {
            out.push_str(&format!(
                "    (name = {}, external = (http = ())),\n",
                capnp_text(crate::workflows::WFDEL_SERVICE)
            ));
        }
    }

    // Logs: shared `vrt-logsink` external designator for the per-worker
    // tail workers (emitted only when at least one tail exists).
    if input.params.log_tail.enabled() && !input.tenant.workers.is_empty() {
        out.push_str(&format!(
            "    (name = {}, external = (http = ())),\n",
            capnp_text(crate::logs::LOGSINK_SERVICE)
        ));
    }

    // CR1: per-worker `facade-cron-<id>` services — the scheduled()
    // dispatcher bound to `sock-cron-<id>`; cron-less workers render
    // byte-identical legacy output.
    for worker in &input.tenant.workers {
        if !worker.crons.is_empty() {
            emit_cron_facade(&mut out, worker, input, &mut embeds)?;
        }
    }

    let mut systems: Vec<&SystemWorkerRef> = input
        .params
        .system_workers
        .iter()
        // `facade-router`/`facade-qconsumer` are TEMPLATES — only
        // materialized as `router-<workerId>`/`facade-qconsumer-<id>`
        // for opted-in workers above; emitting them unconditionally
        // would change legacy renders byte-for-byte.
        .filter(|s| {
            s.service_name != ROUTER_SYSTEM_SERVICE && s.service_name != QCONSUMER_SYSTEM_SERVICE
        })
        .collect();
    systems.sort_by(|a, b| utf16_cmp(&a.service_name, &b.service_name));
    for system in &systems {
        emit_system_worker(&mut out, system, input.params, &mut embeds, input)?;
    }

    // PT1: watchdog heartbeat worker — same-process event-loop probe.
    // No bindings, no sockets on ingress, `globalOutbound` deny-all:
    // it can only answer the private liveness GET.
    if !input.params.heartbeat_socket.is_empty() {
        out.push_str(&format!(
            "    (name = {}, worker = (\n      compatibilityDate = {},\n      modules = [\n        (name = \"vrt-heartbeat.mjs\", esModule = {}),\n      ],\n      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
            capnp_text(HEARTBEAT_SERVICE),
            capnp_text(&input.params.system_compat_date),
            capnp_text(crate::heartbeat::HEARTBEAT_JS),
            capnp_text(&input.params.deny_outbound.service),
            capnp_text(&input.params.deny_outbound.entrypoint),
        ));
    }

    // MIG-12: per-generation waitUntil aggregate probe — one
    // supervisor GET fans out to every emitted worker's
    // `VrtWaituntilProbe` named entrypoint (the proven facade
    // service-binding mechanism). Deny-all outbound, no tenant
    // bindings: the count is platform-readable only.
    if !input.params.waituntil_socket.is_empty() && !input.tenant.workers.is_empty() {
        let mut wu_bindings = String::new();
        let mut wu_names = Vec::new();
        for (i, worker) in input.tenant.workers.iter().enumerate() {
            let bname = format!("{}{i}", crate::waituntil::PROBE_BINDING_PREFIX);
            wu_bindings.push_str(&format!(
                "        (name = {}, service = (name = {}, entrypoint = {})),\n",
                capnp_text(&bname),
                capnp_text(&worker_service_name(&worker.worker_id)),
                capnp_text(crate::waituntil::PROBE_ENTRYPOINT),
            ));
            wu_names.push(bname);
        }
        out.push_str(&format!(
            "    (name = {}, worker = (\n      compatibilityDate = {},\n      modules = [\n        (name = {}, esModule = {}),\n      ],\n      bindings = [\n{}      ],\n      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
            capnp_text(crate::waituntil::PROBE_SERVICE),
            capnp_text(&input.params.system_compat_date),
            capnp_text(crate::waituntil::PROBE_MODULE),
            capnp_text(&crate::waituntil::probe_js(&wu_names)),
            wu_bindings,
            capnp_text(&input.params.deny_outbound.service),
            capnp_text(&input.params.deny_outbound.entrypoint),
        ));
    }

    // `binding-backend` is declared ONLY when some system worker binds
    // it (`BINDING_BACKEND` emits under the same predicate) — an
    // unreferenced external would still force every spawn to carry
    // `--external-addr` for a service nothing can call (fail closed:
    // emit only what is declared).
    let needs_backend = systems.iter().any(|s| s.needs_binding_backend);
    if needs_backend {
        out.push_str(&format!(
            "    (name = {}, external = (http = ())),\n",
            capnp_text(BINDING_BACKEND_SERVICE)
        ));
    }
    // C1: per-tenant Cache API service — emitted only when the
    // render carries a `cache_addr`; an unreferenced external would
    // still force every spawn to carry a spurious --external-addr.
    if !input.params.cache_addr.is_empty() {
        out.push_str(&format!(
            "    (name = {}, external = (http = ())),\n",
            capnp_text(CACHE_SERVICE)
        ));
    }
    // AE: one `external` service per DISTINCT dataset name — workerd
    // copies the designator's service name into the event's `dataset`
    // field (server/bindings.rs `engine.set_dataset(name)`), so the
    // service must be named verbatim after the dataset. All of them
    // resolve to this tenant's single AE ingest socket. Dataset
    // charset `[A-Za-z0-9_]` can never collide with platform service
    // names, which are all hyphenated (`worker-*`, `vrt-*`, `facade-*`).
    for ds in ae_datasets(input.tenant) {
        out.push_str(&format!(
            "    (name = {}, external = (http = ())),\n",
            capnp_text(ds)
        ));
    }
    out.push_str(&format!(
        "    (name = {}, network = (\n      allow = [\"public\"],\n      deny = [\"private\", \"local\", \"unix\", \"unix-abstract\"],\n      tlsOptions = (trustBrowserCas = true),\n    )),\n    (name = {}, network = (\n      allow = [\"local\"],\n      deny = [\"unix\", \"unix-abstract\"],\n    )),\n",
        capnp_text(OUTBOUND_NETWORK_SERVICE),
        capnp_text(INTERNAL_NETWORK_SERVICE)
    ));
    out.push_str("  ],\n");

    // W1: extension modules. The wrapped-binding module MUST be
    // `internal = true` — workerd requires wrapped-binding moduleName
    // to resolve to an extension-internal module, never a tenant one,
    // so user code cannot import or shadow it. `miniflare:zod` must
    // NOT be internal: the engine service's `workflows.mjs` is an
    // ordinary worker module, and internal modules are invisible to
    // non-internal importers (W1-r2 P0 — this is also exactly how
    // miniflare registers it: no internal flag). Privileged extension
    // modules may import `node-internal:*` without `nodejs_compat`.
    if any_workflow {
        out.push_str(&format!(
            "  extensions = [ (modules = [\n    (name = {}, internal = true, esModule = {}),\n    (name = {}, esModule = {}),\n  ]) ],\n",
            capnp_text(crate::workflows::WRAPPED_BINDING_MODULE),
            capnp_text(std::str::from_utf8(crate::workflows::WRAPPED_BINDING_VRT_JS).map_err(|_| ConfigError::Identifier("wfWrappedUtf8"))?),
            capnp_text(crate::workflows::ZOD_MODULE_NAME),
            capnp_text(std::str::from_utf8(crate::workflows::ZOD_WORKER_JS).map_err(|_| ConfigError::Identifier("wfZodUtf8"))?),
        ));
    }

    out.push_str("  sockets = [\n");
    for worker in &input.tenant.workers {
        emit_worker_sockets(&mut out, worker, input);
    }
    // PT1: private heartbeat socket — same socket_dir (private perms
    // by F1), never registered in `worker_endpoints`, so ingress can
    // never map a Host onto it.
    if !input.params.heartbeat_socket.is_empty() {
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(HEARTBEAT_SOCKET_NAME),
            capnp_text(&format!("unix:{}", input.params.heartbeat_socket)),
            capnp_text(HEARTBEAT_SERVICE),
        ));
    }
    // MIG-12: private waitUntil probe socket — same socket_dir
    // (0700) as the heartbeat, never registered in worker_endpoints,
    // so tenant ingress can never route onto it.
    if !input.params.waituntil_socket.is_empty() && !input.tenant.workers.is_empty() {
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(crate::waituntil::PROBE_SOCKET_NAME),
            capnp_text(&format!("unix:{}", input.params.waituntil_socket)),
            capnp_text(crate::waituntil::PROBE_SERVICE),
        ));
    }
    // MIG-13: switch-completion marker — emitted only when the render
    // carries DO storage (same predicate as the `do-storage-*` disk
    // services — the only case the serialized-switch prewarm swaps
    // configs on) AND a waitUntil probe socket exists. The marker
    // lives in the SAME private dir as the probe
    // (`<dir of waituntil_socket>/vrt-sw.sock`, bound to the probe
    // service) — never a tenant worker service, never `socket_dir`
    // itself: a render outside `assemble_capnp` may carry a relative
    // `socket_dir` whose directory does not exist (`unix:sock/
    // vrt-sw.sock` was the r1 gate regression), while the probe
    // dir is materialized+length-checked wherever the probe socket is
    // real. An empty `waituntil_socket` therefore means NO marker —
    // prewarm cannot prove reload completion and the supervisor falls
    // back to the classic serialized switch.
    let has_do_storage = input
        .tenant
        .workers
        .iter()
        .any(|w| !w.durable_object_classes.is_empty() || !wf_targets(input, &w.worker_id).is_empty());
    if has_do_storage
        && !input.params.waituntil_socket.is_empty()
        && !input.tenant.workers.is_empty()
    {
        let probe_dir = std::path::Path::new(&input.params.waituntil_socket)
            .parent()
            .unwrap_or_else(|| std::path::Path::new(""));
        let marker_path = probe_dir.join(SWITCH_MARKER_FILE);
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(SWITCH_MARKER_SOCKET),
            capnp_text(&format!("unix:{}", marker_path.display())),
            capnp_text(crate::waituntil::PROBE_SERVICE),
        ));
    }
    out.push_str("  ],\n);\n");

    // W3-r2: per-(worker, workflow) facade tokens — the hop's lookup
    // table. Derived from the SAME pepper + uniqueKeys the facade's
    // WFOP_TABLE embeds, so hop-injected headers always match.
    let mut wfop_tokens: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for worker in &input.tenant.workers {
        for wf in &worker.workflows {
            // W7: tokens keyed by the DEFINING worker — the op hop
            // injects on `sock-wfops-<definingId>` (ops belong to
            // `script_name`'s worker, never the declaring one).
            wfop_tokens
                .entry(wf_defining_id(worker, wf).to_string())
                .or_default()
                .insert(
                    wf.name.clone(),
                    crate::workflows::wfop_token(&input.params.wfop_pepper, &wf.unique_key),
                );
        }
    }

    Ok(RenderedConfig {
        digest: hex::encode(Sha256::digest(out.as_bytes())),
        text: out,
        embeds,
        external_addrs: {
            let mut v = Vec::new();
            if needs_backend {
                v.push(ExternalAddr {
                    service: BINDING_BACKEND_SERVICE.into(),
                    address: format!("unix:{}", input.params.binding_backend_socket),
                });
            }
            if any_workflow && !input.params.wf_delete_addr.is_empty() {
                v.push(ExternalAddr {
                    service: crate::workflows::WFDEL_SERVICE.into(),
                    address: input.params.wf_delete_addr.clone(),
                });
            }
            if input.params.log_tail.enabled() && !input.tenant.workers.is_empty() {
                v.push(ExternalAddr {
                    service: crate::logs::LOGSINK_SERVICE.into(),
                    address: input.params.log_tail.sink_addr.clone(),
                });
            }
            if !input.params.cache_addr.is_empty() {
                v.push(ExternalAddr {
                    service: CACHE_SERVICE.into(),
                    address: input.params.cache_addr.clone(),
                });
            }
            for ds in ae_datasets(input.tenant) {
                v.push(ExternalAddr {
                    service: ds.to_string(),
                    address: input.params.ae_addr.clone(),
                });
            }
            v
        },
        env,
        skipped_workers,
        skipped_reasons,
        wfop_tokens,
    })
}

/// W3-r2: one worker's complete emit surface, run into scratch during
/// the pre-render dry-run. Covers every per-worker fragment so a
/// render-fatal error anywhere in the worker's config marks exactly
/// that worker for exclusion — siblings are never poisoned.
#[allow(clippy::too_many_arguments)]
fn emit_worker_surface(
    out: &mut String,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    bundle: Option<&VersionBundle>,
    embeds: &mut Vec<EmbedRef>,
    env: &mut BTreeMap<String, SecretValue>,
) -> Result<(), ConfigError> {
    let bundle = bundle.ok_or(ConfigError::BundleMismatch("missing"))?;
    emit_tenant_worker(out, worker, input, bundle, embeds, env)?;
    if input.params.log_tail.enabled() {
        emit_tail_worker(out, worker, input)?;
    }
    if let Some(assets) = worker_assets_first(worker) {
        emit_router_worker(out, worker, assets, input, embeds)?;
    }
    if !worker.queue_consumers.is_empty() {
        emit_qconsumer_worker(out, worker, input, embeds)?;
    }
    emit_do_storage_service(out, worker, input);
    // W7: engines/facade of declarations DEFINING on this worker —
    // the dry-run surface covers the exact emit a defining worker
    // carries (a foreign `script_name` decl adds its engine here).
    let targets = wf_targets(input, &worker.worker_id);
    for (decl_worker, wf) in &targets {
        emit_workflow_engine(out, decl_worker, wf, input, embeds)?;
    }
    if !targets.is_empty() {
        emit_wfops_facade(out, worker, &targets, input, embeds)?;
    }
    // CR1: cron dispatcher facade — same trusted-socket pattern as
    // wfops/qconsumer; only when the worker declares schedules.
    if !worker.crons.is_empty() {
        emit_cron_facade(out, worker, input, embeds)?;
    }
    emit_worker_sockets(out, worker, input);
    Ok(())
}

/// Emit the workerd `limits` group (`Worker.limits` — a VRT fork
/// extension) for a worker's declared quotas.
///
/// Semantics (see `doc/platform/workers-runtime-quotas.md`):
/// - Only dimensions this generation can enforce are emitted:
///   `cpuMs`, `memoryMb`, `subRequests`.
/// - A dimension equal to the platform default is "not declared" and is
///   omitted, so a worker without explicit limits renders byte-for-byte
///   like stock workerd (no behaviour change on upgrade).
/// - `0` is never emitted (workerd reads an absent dimension as
///   unlimited; `subrequests = 0` is the protocol's "no budget").
/// - `outboundConnections` has no workerd hook yet and is rejected by
///   [`validate_tenant`] instead of being silently dropped.
fn emit_worker_limits(out: &mut String, quotas: &WorkerQuotas) {
    let mut dims: Vec<String> = Vec::new();
    if quotas.cpu_ms > 0 && quotas.cpu_ms != snapshot_protocol::compute_desired::DEFAULT_CPU_MS {
        dims.push(format!("cpuMs = {}", quotas.cpu_ms));
    }
    if quotas.memory_mb > 0
        && quotas.memory_mb != snapshot_protocol::compute_desired::DEFAULT_MEMORY_MB
    {
        dims.push(format!("memoryMb = {}", quotas.memory_mb));
    }
    if quotas.subrequests > 0
        && quotas.subrequests != snapshot_protocol::compute_desired::DEFAULT_SUBREQUESTS
    {
        dims.push(format!("subRequests = {}", quotas.subrequests));
    }
    if !dims.is_empty() {
        out.push_str(&format!("      limits = ({}),\n", dims.join(", ")));
    }
}

/// Fail closed when the target cell cannot enforce per-worker quotas.
///
/// A worker that declares an enforceable limit (`cpuMs`/`memoryMb`/
/// `subRequests` away from the platform default) on a cell whose
/// `quotas` capability is absent must not be served: rendering would
/// either omit the declared limit silently or hand workerd a config it
/// rejects. Callers (vrt-host assemble, vrt-local preflight) pass the
/// cell capability bit; `vrt-config` itself cannot know it.
pub fn ensure_quotas_enforceable(
    tenant: &TenantDesired,
    enforced: bool,
) -> Result<(), ConfigError> {
    if enforced {
        return Ok(());
    }
    for worker in &tenant.workers {
        if worker.quotas.declares_enforceable_limit() {
            return Err(ConfigError::Quotas(
                worker.quotas.declared_deviation().unwrap_or("quotas"),
            ));
        }
    }
    Ok(())
}

/// Emit the `sock-*` socket entries for one worker (main + optional
/// qconsumer/wfops endpoints) — extracted so the dry-run surface can
/// cover them too (capnp_text identifier failures).
fn emit_worker_sockets(out: &mut String, worker: &WorkerDesired, input: &TenantRenderInput<'_>) {
    // MIG-7: the tenant-ingress socket derives `request.url`'s scheme
    // from `x-forwarded-proto`, which vrt-host always overwrites with
    // the protocol resolved from its trusted-source policy (default
    // fail-closed `http`) — client-supplied values never pass through.
    // Queue/cron/wfops sockets only see host-synthesized requests, so
    // their `http = ()` stays untouched.
    // MIG-10: `cfBlobHeader` makes `request.cf` the host-synthesized
    // JSON vrt-host injects (validated country/colo/httpProtocol, or a
    // minimal blob when the CF chain is untrusted). Without it
    // workerd fills `request.cf` with the unix-socket peer
    // credentials — vrt-host's own pid/uid as clientPid/clientUid.
    out.push_str(&format!(
        "    (name = {}, address = {}, http = (forwardedProtoHeader = \"x-forwarded-proto\", cfBlobHeader = {}), service = {}),\n",
        capnp_text(&worker_socket_name(&worker.worker_id)),
        capnp_text(&format!(
            "unix:{}/{}.sock",
            input.params.socket_dir, worker.worker_id
        )),
        capnp_text(CF_BLOB_HEADER),
        capnp_text(&if worker_assets_first(worker).is_some() {
            router_service_name(&worker.worker_id)
        } else {
            worker_service_name(&worker.worker_id)
        }),
    ));
    // T37b: consumer dispatch endpoint socket -- vrt-host `/v1/queue/dispatch`
    // proxies to it; `*.qconsumer.sock` sits alongside `*.sock` in the same directory.
    if !worker.queue_consumers.is_empty() {
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(&qconsumer_socket_name(&worker.worker_id)),
            capnp_text(&format!(
                "unix:{}/{}-qconsumer.sock",
                input.params.socket_dir, worker.worker_id
            )),
            capnp_text(&qconsumer_service_name(&worker.worker_id)),
        ));
    }
    // W3: `<id>-wfops.sock` — vrt-host `/v1/wfops` proxies here;
    // loopback unix only, never on tenant ingress. W7: on the
    // DEFINING worker (ops belong to `script_name`'s worker).
    if !wf_targets(input, &worker.worker_id).is_empty() {
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(&wfops_socket_name(&worker.worker_id)),
            capnp_text(&format!(
                "unix:{}/{}-wfops.sock",
                input.params.socket_dir, worker.worker_id
            )),
            capnp_text(&wfops_service_name(&worker.worker_id)),
        ));
    }
    // CR1: `<id>-cron.sock` — vrt-host `/v1/cron/trigger` dispatches
    // here; loopback unix only. Public traffic can never reach it,
    // and `Fetcher.scheduled()` (the sole scheduled-event channel)
    // requires `service_binding_extra_handlers` — a flag tenants
    // cannot declare — so the event is unforgeable end to end.
    if !worker.crons.is_empty() {
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(&cron_socket_name(&worker.worker_id)),
            capnp_text(&format!(
                "unix:{}/{}-cron.sock",
                input.params.socket_dir, worker.worker_id
            )),
            capnp_text(&cron_facade_service_name(&worker.worker_id)),
        ));
    }
    // IMG1: `<id>-images.sock` — the ingress intercepts `/cdn-cgi/image/`
    // on this worker's hostnames and POSTs multipart transform requests
    // here. Loopback unix only; present iff the worker declares an
    // `images` binding (capability-gated like every other facade).
    if worker
        .bindings
        .iter()
        .any(|b| matches!(b, snapshot_protocol::compute_desired::BindingDesired::Images(_)))
    {
        out.push_str(&format!(
            "    (name = {}, address = {}, http = (), service = {}),\n",
            capnp_text(&format!("sock-images-{}", worker.worker_id)),
            capnp_text(&format!(
                "unix:{}/{}-images.sock",
                input.params.socket_dir, worker.worker_id
            )),
            capnp_text(&input.params.facades.images.service),
        ));
    }
}

/// Emit the per-worker `do-storage-<workerId>` DiskDirectory service
/// (P3c-1) — shared by the real emit and the dry-run surface.
fn emit_do_storage_service(
    out: &mut String,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) {
    // W7: engine SQLite lives under the DEFINING worker — a worker
    // with no own DO/workflow decls still needs its disk service when
    // another worker's `script_name` lands an engine here.
    if !worker.durable_object_classes.is_empty() || !wf_targets(input, &worker.worker_id).is_empty()
    {
        out.push_str(&format!(
            "    (name = {}, disk = (path = {}, writable = true)),\n",
            capnp_text(&do_storage_service_name(&worker.worker_id)),
            capnp_text(&format!(
                "{}/{}",
                input.params.do_storage_dir.trim_end_matches('/'),
                worker.worker_id
            )),
        ));
    }
}

/// Emit `(name = "worker-<id>", worker = (…))`.
fn emit_tenant_worker(
    out: &mut String,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    bundle: &VersionBundle,
    embeds: &mut Vec<EmbedRef>,
    env: &mut BTreeMap<String, SecretValue>,
) -> Result<(), ConfigError> {
    let service = worker_service_name(&worker.worker_id);
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n",
        capnp_text(&service),
        capnp_text(&worker.active_version.compatibility_date),
    ));
    // MIG-1: flags already defaulted at this compat date are stripped —
    // workerd warns on the redundant declaration and `check` refuses.
    let compat_flags = crate::compat_flags::flags_for_compat_date(
        worker
            .active_version
            .compatibility_flags
            .iter()
            .map(|f| f.as_str()),
        &worker.active_version.compatibility_date,
    );
    if !compat_flags.is_empty() {
        out.push_str(&format!(
            "      compatibilityFlags = [{}],\n",
            compat_flags
                .iter()
                .map(|f| capnp_text(f))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    emit_worker_limits(out, &worker.quotas);
    // Modules: main module first (workerd treats modules[0] as the entry),
    // the rest sorted by name for deterministic output.
    let mut modules: Vec<&crate::manifest::Module> = bundle.manifest().modules.iter().collect();
    modules.sort_by(|a, b| utf16_cmp(a.name(), b.name()));
    let main = bundle.manifest().main_module.clone();
    modules.sort_by_key(|m| if m.name() == main { 0 } else { 1 });
    // MIG-12: the platform entry wrapper takes modules[0]/modules[1]
    // — a generated module that re-exports the tenant graph behind a
    // ctx.waitUntil-counting shim, plus the shared prelude. Tenant
    // bundle modules may never shadow the reserved pair, and no DO/
    // workflow class may shadow the `VrtWaituntilProbe` entrypoint
    // (fail closed, per-worker — the dry-run surface converts this to
    // a skipped worker, never a poisoned sibling).
    let wu_wrap = !input.params.waituntil_socket.is_empty();
    if wu_wrap {
        for m in &modules {
            if m.name() == crate::waituntil::ENTRY_MODULE
                || m.name() == crate::waituntil::PRELUDE_MODULE
            {
                return Err(ConfigError::Identifier("waitUntilModule"));
            }
        }
    }
    out.push_str("      modules = [\n");
    if wu_wrap {
        // Locally-defined entrypoint classes whose constructor ctx may
        // see ctx.waitUntil: this worker's DO classes + the workflow
        // classes DEFINED on it (W7: script_name resolution).
        let mut wu_classes: Vec<String> = worker
            .durable_object_classes
            .iter()
            .map(|c| c.class_name.clone())
            .collect();
        for (_, wf) in wf_targets(input, &worker.worker_id) {
            wu_classes.push(wf.class_name.clone());
        }
        wu_classes.sort();
        wu_classes.dedup();
        if wu_classes
            .iter()
            .any(|c| c == crate::waituntil::PROBE_ENTRYPOINT)
        {
            return Err(ConfigError::Identifier("waitUntilEntrypoint"));
        }
        out.push_str(&format!(
            "        (name = {}, esModule = {}),\n        (name = {}, esModule = {}),\n",
            capnp_text(crate::waituntil::ENTRY_MODULE),
            capnp_text(&crate::waituntil::entry_js(&main, &wu_classes)),
            capnp_text(crate::waituntil::PRELUDE_MODULE),
            capnp_text(crate::waituntil::PRELUDE_JS),
        ));
    }
    for module in modules {
        let path = format!("{BUNDLE_DIR_PREFIX}/{}/{}", bundle.digest(), module.name());
        embeds.push(EmbedRef {
            path: path.clone(),
            digest: hex::encode(Sha256::digest(
                module
                    .content_bytes()
                    .map_err(|_| ConfigError::BundleMismatch("moduleContent"))?,
            )),
            kind: EmbedKind::BundleModule {
                worker_id: worker.worker_id.clone(),
                module_name: module.name().to_owned(),
            },
        });
        out.push_str(&format!(
            "        (name = {}, {} = embed {}),\n",
            capnp_text(module.name()),
            module.module_type().capnp_field(),
            capnp_text(&path),
        ));
    }
    out.push_str("      ],\n");

    out.push_str("      bindings = [\n");
    for binding in &worker.bindings {
        emit_binding(out, binding, worker, input, env)?;
    }
    // W1: `env.<binding>` — workerd `wrapped` binding whose inner
    // `binding` service points at this workflow's engine service and
    // its `WorkflowBinding` entrypoint (miniflare wrapped-binding
    // wiring). The module itself resolves via config.extensions.
    for wf in &worker.workflows {
        out.push_str(&format!(
            "        (name = {}, wrapped = (moduleName = {}, innerBindings = [
            (name = \"binding\", service = (name = {}, entrypoint = \"WorkflowBinding\")),
            (name = \"createConfirmMs\", json = \"\\\"10000\\\"\")
        ])),\n",
            capnp_text(&wf.binding),
            capnp_text(crate::workflows::WRAPPED_BINDING_MODULE),
            capnp_text(&wf_engine_service_name(
                wf_defining_id(worker, wf),
                &wf.name,
            )),
        ));
    }
    out.push_str("      ],\n");

    // P3c-1: DO namespaces — `durableObjectNamespaces` (worker.capnp
    // Worker.durableObjectNamespaces @7) + `durableObjectStorage`
    // (ActorStorageConfig @8) pointing at the per-worker DiskDirectory
    // service emitted in render_tenant_config. uniqueKey is the
    // server-minted storage identity; enableSql gates `storage.sql`.
    if !worker.durable_object_classes.is_empty() {
        let mut classes: Vec<&snapshot_protocol::compute_desired::DoClassDesired> =
            worker.durable_object_classes.iter().collect();
        classes.sort_by(|a, b| utf16_cmp(&a.class_name, &b.class_name));
        out.push_str("      durableObjectNamespaces = [\n");
        for class in classes {
            out.push_str(&format!(
                "        (className = {}, uniqueKey = {}, enableSql = {}),\n",
                capnp_text(&class.class_name),
                capnp_text(&class.unique_key),
                if class.enable_sql { "true" } else { "false" },
            ));
        }
        out.push_str("      ],\n");
        out.push_str(&format!(
            "      durableObjectStorage = (localDisk = {}),\n",
            capnp_text(&do_storage_service_name(&worker.worker_id)),
        ));
    }

    // C1: `cacheApiOutbound` is per-WORKER inside this tenant — every
    // worker of the tenant shares the tenant key space (CF per-zone
    // semantic). Empty addr ⇒ omitted ⇒ workerd throws
    // `No Cache was configured` on `caches.*` (explicit, fail closed).
    if !input.params.cache_addr.is_empty() {
        out.push_str(&format!(
            "      cacheApiOutbound = (name = {}),\n",
            capnp_text(CACHE_SERVICE)
        ));
    }
    // One `tails` list per worker: the platform log collector (when
    // enabled) shares it with user-declared Tail Workers
    // (`tail_consumers`). workerd accepts a list, so declaring a user
    // consumer never replaces the internal collector. The internal one
    // is always first; user entries keep declaration order (validated
    // unique, resolved to same-tenant worker ids at pack time).
    let mut tails: Vec<String> = Vec::new();
    if collects_logs(worker, input.params) {
        tails.push(crate::logs::tail_service_name(&worker.worker_id));
    }
    for tail in &worker.tail_consumers {
        // Per-worker fault isolation (`skipped_workers`) can remove a
        // worker AFTER `validate_tenant`; emitting a designator to it
        // would fail the whole generation. Treat a dropped consumer as
        // a render error so the fixpoint also drops its producers
        // instead of handing workerd a dangling `tails` entry.
        if !input
            .tenant
            .workers
            .iter()
            .any(|w| w.worker_id == tail.worker_id)
        {
            return Err(ConfigError::ServiceReference("tailConsumers"));
        }
        let name = worker_service_name(&tail.worker_id);
        if !tails.contains(&name) {
            tails.push(name);
        }
    }
    if !tails.is_empty() {
        out.push_str("      tails = [");
        out.push_str(
            &tails
                .iter()
                .map(|name| format!("(name = {})", capnp_text(name)))
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push_str("],\n");
    }
    // `streamingTails` designators: the worker's declared
    // `streamingTailConsumers` — same same-tenant resolution rule as
    // `tails`, but consumers get TailStream events via `tailStream()`
    // instead of batched `tail()` calls.
    if !worker.streaming_tail_consumers.is_empty() {
        let mut names: Vec<String> = Vec::new();
        for consumer in &worker.streaming_tail_consumers {
            if !input
                .tenant
                .workers
                .iter()
                .any(|w| w.worker_id == consumer.worker_id)
            {
                return Err(ConfigError::Identifier("streamingTailConsumers.workerId"));
            }
            names.push(worker_service_name(&consumer.worker_id));
        }
        let designators = names
            .iter()
            .map(|n| format!("(name = {})", capnp_text(n)))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("      streamingTails = [{designators}],\n"));
    }
    out.push_str(&format!(
        "      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
        capnp_text(&input.params.egress.service),
        capnp_text(&input.params.egress.entrypoint),
    ));
    Ok(())
}

/// Whether this worker gets a `vrt-tail` collector: platform-wide log
/// collection AND the worker's own `[observability]` gates
/// (`enabled`/`logs.enabled` false => no collector, CF semantics).
fn collects_logs(worker: &WorkerDesired, params: &RuntimeParams) -> bool {
    params.log_tail.enabled()
        && !worker
            .observability
            .as_ref()
            .map(|o| !o.enabled || !o.logs_enabled)
            .unwrap_or(false)
}

/// Emit the platform-owned `vrt-tail-<workerId>` collector for one
/// tenant worker (see [`crate::logs`]). Identity (`WORKER_ID`,
/// `TOKEN`) is rendered from trusted config; outbound is deny-all and
/// the only reachable service is the `vrt-logsink` external. Fails
/// closed without a pepper (a forgeable token must never render).
fn emit_tail_worker(
    out: &mut String,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    // `[observability]` gates — `enabled`/`logs.enabled` false means
    // NO collector at all (CF: nothing is emitted or collected). Runs
    // before the pepper check: a disabled worker must not fail a
    // missing-pepper render.
    if worker
        .observability
        .as_ref()
        .map(|o| !o.enabled || !o.logs_enabled)
        .unwrap_or(false)
    {
        return Ok(());
    }
    if input.params.log_tail.pepper.is_empty() {
        return Err(ConfigError::Identifier("logTailPepper"));
    }
    let token = crate::logs::log_token(&input.params.log_tail.pepper, &worker.worker_id);
    // OBS carries the sampling/gate settings the collector applies
    // per-invocation; absent `observability` renders no binding and
    // the JS defaults (rate 1, invocation logs on) keep legacy
    // renders byte-identical.
    let obs_binding = worker
        .observability
        .as_ref()
        .map(|o| {
            format!(
                "        (name = \"OBS\", text = {}),\n",
                capnp_text(
                    &json!({
                        "headSamplingRate": o.head_sampling_rate_ppm as f64 / 1e6,
                        "invocationLogs": o.invocation_logs,
                    })
                    .to_string()
                )
            )
        })
        .unwrap_or_default();
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n      modules = [\n        (name = \"vrt-tail.mjs\", esModule = {}),\n      ],\n      bindings = [\n        (name = \"SINK\", service = {}),\n        (name = \"WORKER_ID\", text = {}),\n        (name = \"TOKEN\", text = {}),\n{}      ],\n      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
        capnp_text(&crate::logs::tail_service_name(&worker.worker_id)),
        capnp_text(&input.params.system_compat_date),
        capnp_text(crate::logs::TAIL_WORKER_JS),
        capnp_text(crate::logs::LOGSINK_SERVICE),
        capnp_text(&worker.worker_id),
        capnp_text(&token),
        obs_binding,
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}

/// Emit one `bindings` entry.
fn emit_binding(
    out: &mut String,
    binding: &BindingDesired,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    env: &mut BTreeMap<String, SecretValue>,
) -> Result<(), ConfigError> {
    match binding {
        BindingDesired::Var(var) => {
            let value = resolve_var(&input.params.vars, &var.var_ref)?;
            out.push_str(&format!(
                "        (name = {}, text = {}),\n",
                capnp_text(&var.name),
                capnp_text(value),
            ));
        }
        BindingDesired::Secret(secret) => {
            // T13/M6: resolve ONLY inside this tenant's namespace —
            // another tenant's ref never resolves (fail closed).
            let value = resolve_secret(
                &input.params.secrets,
                &input.tenant.tenant_id,
                &secret.secret_ref,
            )?;
            // Secrets never enter the config text/digest: the binding is
            // `fromEnvironment` and vrt-host injects `env` into the
            // child process (main doc §2.3/§3).
            let env_name = secret_env_name(&worker.worker_id, &secret.name);
            if env.insert(env_name.clone(), value.clone()).is_some() {
                return Err(ConfigError::Identifier("secretEnv"));
            }
            out.push_str(&format!(
                "        (name = {}, fromEnvironment = {}),\n",
                capnp_text(&secret.name),
                capnp_text(&env_name),
            ));
        }
        BindingDesired::Kv(kv) => {
            emit_facade_binding(
                out,
                &kv.name,
                &input.params.facades.kv,
                &kv.namespace_id,
                worker,
                input,
                &kv.permissions,
                &kv.name,
            )?;
        }
        BindingDesired::R2(r2) => {
            emit_r2_bucket_binding(
                out,
                &r2.name,
                &input.params.facades.r2,
                &r2.bucket,
                worker,
                input,
                &r2.permissions,
                &r2.name,
            )?;
        }
        BindingDesired::D1(d1) => {
            emit_d1_binding(out, d1, worker, input)?;
        }
        BindingDesired::Assets(assets) => {
            emit_facade_binding(
                out,
                &assets.name,
                &input.params.facades.assets,
                &assets.manifest_digest,
                worker,
                input,
                &assets.permissions,
                &assets.name,
            )?;
        }
        BindingDesired::Service(service) => {
            if !input
                .tenant
                .workers
                .iter()
                .any(|w| w.worker_id == service.worker_id)
            {
                return Err(ConfigError::ServiceTarget);
            }
            // Last trusted boundary for tenant-supplied props: a non-object
            // or over-bound payload must never reach the capnp document.
            if let Some(props) = &service.props {
                if !snapshot_protocol::compute_desired::valid_service_props(props) {
                    return Err(ConfigError::Capacity("serviceProps"));
                }
            }
            let props = service
                .props
                .as_ref()
                .map(|p| String::from_utf8(canonical_bytes(p)).expect("canonical props are utf8"));
            match (service.entrypoint.as_deref(), props.as_deref()) {
                // Legacy bytes: a default-export binding renders exactly
                // as before the field existed.
                (None, None) => out.push_str(&format!(
                    "        (name = {}, service = (name = {})),\n",
                    capnp_text(&service.name),
                    capnp_text(&worker_service_name(&service.worker_id)),
                )),
                (Some(ep), None) => {
                    // Re-checked here (the render is the last trusted
                    // boundary): never route a tenant onto a
                    // platform-injected entrypoint such as the probe.
                    if !snapshot_protocol::compute_desired::valid_service_entrypoint(ep)
                        || ep == crate::waituntil::PROBE_ENTRYPOINT
                    {
                        return Err(ConfigError::ServiceTarget);
                    }
                    out.push_str(&format!(
                        "        (name = {}, service = (name = {}, entrypoint = {})),\n",
                        capnp_text(&service.name),
                        capnp_text(&worker_service_name(&service.worker_id)),
                        capnp_text(ep),
                    ));
                }
                (None, Some(props)) => out.push_str(&format!(
                    "        (name = {}, service = (name = {}, props = (json = {}))),\n",
                    capnp_text(&service.name),
                    capnp_text(&worker_service_name(&service.worker_id)),
                    capnp_text(props),
                )),
                (Some(ep), Some(props)) => {
                    if !snapshot_protocol::compute_desired::valid_service_entrypoint(ep)
                        || ep == crate::waituntil::PROBE_ENTRYPOINT
                    {
                        return Err(ConfigError::ServiceTarget);
                    }
                    out.push_str(&format!(
                        "        (name = {}, service = (name = {}, entrypoint = {}, props = (json = {}))),\n",
                        capnp_text(&service.name),
                        capnp_text(&worker_service_name(&service.worker_id)),
                        capnp_text(ep),
                        capnp_text(props),
                    ));
                }
            }
        }
        BindingDesired::Queue(q) => {
            emit_queue_binding(out, q, worker, input)?;
        }
        BindingDesired::K2(k) => {
            emit_k2_binding(out, k, worker, input)?;
        }
        BindingDesired::SendEmail(e) => {
            emit_email_binding(out, e, worker, input)?;
        }
        BindingDesired::Images(img) => {
            emit_images_binding(out, img, worker, input)?;
        }
        BindingDesired::Vectorize(v) => {
            emit_vectorize_binding(out, v, worker, input)?;
        }
        BindingDesired::Ratelimit(r) => {
            emit_ratelimit_binding(out, r, worker, input)?;
        }
        BindingDesired::DurableObject(d) => {
            // P3c-1: `durableObjectNamespace` (Worker.Binding @10) —
            // same-worker designator omits serviceName (workerd
            // default: the actor class lives in this worker); a
            // `serviceWorkerId` names the DECLARING worker's service.
            // Resolution was proven in validate_tenant (fail closed).
            let svc = d.service_worker_id.as_deref();
            if svc.is_none() || svc == Some(worker.worker_id.as_str()) {
                out.push_str(&format!(
                    "        (name = {}, durableObjectNamespace = (className = {})),\n",
                    capnp_text(&d.name),
                    capnp_text(&d.class_name),
                ));
            } else {
                out.push_str(&format!(
                    "        (name = {}, durableObjectNamespace = (className = {}, serviceName = {})),\n",
                    capnp_text(&d.name),
                    capnp_text(&d.class_name),
                    capnp_text(&worker_service_name(svc.unwrap_or_default())),
                ));
            }
        }
        BindingDesired::Hyperdrive(hd) => {
            // MIG-15: worker-visible Hyperdrive-shaped object as a
            // plain `json` binding — placeholder credentials only;
            // real credentials never enter the isolate (the provider
            // holds them under `credentialRef`). The host is the
            // platform-minted unresolvable synthetic name.
            let conn = format!(
                "postgres://vrt-hd:vrt-hd@{}:5432/vrt",
                hd.synthetic_host
            );
            let obj = format!(
                "{{\"connectionString\":\"{}\",\"host\":\"{}\",\"port\":5432,\"user\":\"vrt-hd\",\"password\":\"vrt-hd\",\"database\":\"vrt\"}}",
                conn, hd.synthetic_host
            );
            out.push_str(&format!(
                "        (name = {}, json = {}),\n",
                capnp_text(&hd.name),
                capnp_text(&obj),
            ));
        }
        BindingDesired::Pg(pg) => {
            // Tenant workers never hold `binding-backend` directly —
            // they would be able to forge `x-open-compute-*` identity
            // headers against the private protocol. PG goes through the
            // pg facade (a system worker with `needs_binding_backend`),
            // whose props pin this binding's database identity.
            emit_facade_binding(
                out,
                &pg.name,
                &input.params.facades.pg,
                &pg.database_id,
                worker,
                input,
                &pg.permissions,
                &pg.name,
            )?;
        }
        BindingDesired::AnalyticsEngine(ae) => {
            // `analyticsEngine` (@17 :ServiceDesignator) — the
            // designator NAME becomes the event's `dataset` field
            // (workerd `engine.set_dataset(designator.get_name())`),
            // so the dataset-named `external` service emitted above is
            // the verbatim target. `$experimental`-gated upstream —
            // assemble derives `--experimental` from the binding.
            out.push_str(&format!(
                "        (name = {}, analyticsEngine = {}),\n",
                capnp_text(&ae.name),
                capnp_text(&ae.dataset),
            ));
        }
        BindingDesired::WorkerLoader(loader) => {
            // workerLoader is `$experimental`-gated upstream — assemble
            // adds `--experimental` to argv when a tenant declares one.
            // `maxWorkers` is the fork's quota field: bounds how many
            // dynamic workers this loader may hold live at once (named
            // cache entries and one-shot `load()` alike). Absent on a
            // stock binary the config would not parse — the platform
            // pins the fork release that defines it.
            out.push_str(&format!(
                "        (name = {}, workerLoader = (maxWorkers = {})),\n",
                capnp_text(&loader.name),
                WORKER_LOADER_MAX_WORKERS,
            ));
        }
        BindingDesired::UnsafeEval(u) => {
            // CF `unsafe.bindings[type="unsafe_eval"]` — workerd's
            // native `unsafeEval` (@23 :Void): `env.<name>` is the
            // `UnsafeEval` object (`.eval`/`.newFunction`/
            // `.newWasmModule`); global eval stays blocked in handlers.
            out.push_str(&format!(
                "        (name = {}, unsafeEval = void),\n",
                capnp_text(&u.name),
            ));
        }
        BindingDesired::Flagship(f) => {
            // CF `[[flagship]]` — `env.<name>` is the async Flagship
            // evaluation API. Rendered as a `wrapped` binding over the
            // `cloudflare-internal:flagship` internal module; the
            // app's serialized flag config rides the `config`
            // innerBinding so evaluation stays inside the isolate
            // (Flagship semantics: no central-service round trip).
            let app = input
                .tenant
                .flagship_apps
                .iter()
                .find(|a| a.app_id == f.app_id)
                .ok_or(ConfigError::UnresolvedReference("flagshipAppId"))?;
            out.push_str(&format!(
                "        (name = {}, wrapped = (\n          moduleName = {},\n          innerBindings = [(\n            name = \"config\",\n            json = {}\n          )],\n        )),\n",
                capnp_text(&f.name),
                capnp_text("cloudflare-internal:flagship"),
                capnp_text(&app.config),
            ));
        }
    }
    Ok(())
}

/// Platform cap on live workers one `workerLoader` binding may hold —
/// the binding carries no tenant knob (Cloudflare exposes none), so a
/// single platform constant guards a tenant workerd's memory: each
/// live worker holds a compiled isolate. Cloudflare documents no
/// public bound; 32 leaves room for per-user plugin/dispatch use cases
/// while still bounding a runaway `get()` loop.
const WORKER_LOADER_MAX_WORKERS: u32 = 32;

/// MIG-15: synthetic internal TCP targets derived from this tenant's
/// hyperdrive bindings — the ONLY producer of
/// `EgressPolicy::internal_tcp_targets` (platform Rust only; the
/// params/TOML surface cannot carry it).
fn internal_tcp_targets(input: &TenantRenderInput<'_>) -> Vec<crate::egress::EgressAllowEntry> {
    let mut out: Vec<crate::egress::EgressAllowEntry> = Vec::new();
    for worker in &input.tenant.workers {
        for binding in &worker.bindings {
            if let BindingDesired::Hyperdrive(hd) = binding {
                out.push(crate::egress::EgressAllowEntry {
                    host: hd.synthetic_host.clone(),
                    port: 5432,
                });
            }
        }
    }
    out.sort_by(|a, b| a.host.cmp(&b.host));
    out.dedup_by(|a, b| a.host == b.host && a.port == b.port);
    out
}

/// Egress policy JSON for THIS render — the params policy plus the
/// tenant-derived internal TCP targets (fail closed on validate).
fn egress_policy_json(input: &TenantRenderInput<'_>) -> Result<String, ConfigError> {
    let mut policy = input.params.egress_policy.clone();
    policy.internal_tcp_targets = internal_tcp_targets(input);
    policy.to_policy_json()
}

/// Emit `(name = <b>, service = (name = <facade>, entrypoint = <ep>,
/// props = (json = "<canonical json>"))` — the facade ctx.props contract per
/// `third_party/open-compute` `bindings/protocol.d.ts`
/// (`ResourceBindingProps`), with vrt mappings documented in
/// `doc/platform/workers-runtime-config.md`.
fn emit_facade_binding(
    out: &mut String,
    binding_name: &str,
    facade: &ServiceEndpoint,
    resource_id: &str,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    permissions: &snapshot_protocol::compute_desired::BindingPermissions,
    props_binding_name: &str,
) -> Result<(), ConfigError> {
    emit_facade_binding_as(
        out,
        "service",
        binding_name,
        facade,
        resource_id,
        worker,
        input,
        permissions,
        props_binding_name,
    )
}

/// Native workerd `r2Bucket` binding to the R2 facade. `Worker.Binding.r2Bucket`
/// is a `ServiceDesignator`, so the facade/props contract is identical to
/// [`emit_facade_binding`]; the difference is the tenant-facing surface.
/// workerd supplies the real `R2Bucket` class (R2ObjectBody results,
/// httpMetadata/customMetadata option parsing, `delete(key | keys[])`) and
/// speaks its native R2 wire to the facade's `fetch()`, which the vendored
/// native adapter serves. A plain `service` binding would instead expose the
/// raw `R2Transport` RPC methods to the tenant: string bodies are rejected
/// (stream-only), `get` returns `{meta, body}` without `text()`, and
/// `delete("k")` posts `{keys: "k"}` which the provider rejects.
fn emit_r2_bucket_binding(
    out: &mut String,
    binding_name: &str,
    facade: &ServiceEndpoint,
    resource_id: &str,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    permissions: &snapshot_protocol::compute_desired::BindingPermissions,
    props_binding_name: &str,
) -> Result<(), ConfigError> {
    emit_facade_binding_as(
        out,
        "r2Bucket",
        binding_name,
        facade,
        resource_id,
        worker,
        input,
        permissions,
        props_binding_name,
    )
}

fn emit_facade_binding_as(
    out: &mut String,
    binding_field: &str,
    binding_name: &str,
    facade: &ServiceEndpoint,
    resource_id: &str,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    permissions: &snapshot_protocol::compute_desired::BindingPermissions,
    props_binding_name: &str,
) -> Result<(), ConfigError> {
    let props = String::from_utf8(canonical_bytes(&json!({
        // Wire format `<workerId>~<bindingName>` — matches
        // vrt-provider::identity::parse_binding_id (single path segment).
        // Authorization always names the *declared* binding, even when
        // the env key is a fixed router constant (T31f/F1).
        "bindingId": format!("{}~{}", worker.worker_id, props_binding_name),
        "descriptorSha256": worker.active_version.artifact_digest,
        "instanceId": input.tenant.tenant_id,
        "namespaceResourceId": resource_id,
        "permissions": {"read": permissions.read, "write": permissions.write},
        "resourceSpecGeneration": input.revision,
        "versionId": worker.active_version.version_id,
        "workerId": worker.worker_id,
    })))
    .expect("canonical props are utf8");
    out.push_str(&format!(
        "        (name = {}, {} = (name = {}, entrypoint = {}, props = (json = {}))),\n",
        capnp_text(binding_name),
        binding_field,
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
    ));
    Ok(())
}

/// MIG-16: emit a `ratelimit` binding as a plain service binding to the
/// facade-ratelimit `RateLimiter` entrypoint. The counter scope is
/// derived by the facade from `instanceId` (tenant) + `workerId` +
/// `namespaceId`, all host-injected props - a tenant can supply only the
/// `key` argument, so two tenants never share a counter.
fn emit_ratelimit_binding(
    out: &mut String,
    r: &snapshot_protocol::compute_desired::RatelimitBinding,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let props = String::from_utf8(canonical_bytes(&json!({
        "instanceId": input.tenant.tenant_id,
        "limit": r.limit,
        "namespaceId": r.namespace_id,
        "period": r.period,
        "workerId": worker.worker_id,
    })))
    .expect("canonical props are utf8");
    let facade = &input.params.facades.ratelimit;
    out.push_str(&format!(
        "        (name = {}, service = (name = {}, entrypoint = {}, props = (json = {}))),\n",
        capnp_text(&r.name),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
    ));
    Ok(())
}

/// Emit a `d1` binding as a workerd `wrapped` binding:
/// `moduleName = "cloudflare-internal:d1-api"`, inner `fetcher` → the
/// facade-d1 system worker with signed props (the same props payload
/// facade bindings carry — `bindingId` names the DECLARED binding).
/// workerd d1-api needs no compat flag on the pinned workerd; that
/// stays verified by the VRT_WORKERD_BIN smoke.
fn emit_d1_binding(
    out: &mut String,
    d1: &snapshot_protocol::compute_desired::D1Binding,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let props = String::from_utf8(canonical_bytes(&json!({
        "bindingId": format!("{}~{}", worker.worker_id, d1.name),
        "descriptorSha256": worker.active_version.artifact_digest,
        "instanceId": input.tenant.tenant_id,
        "namespaceResourceId": d1.database_id,
        "permissions": {"read": d1.permissions.read, "write": d1.permissions.write},
        "resourceSpecGeneration": input.revision,
        "versionId": worker.active_version.version_id,
        "workerId": worker.worker_id,
    })))
    .expect("canonical props are utf8");
    let facade = &input.params.facades.d1;
    out.push_str(&format!(
        "        (name = {}, wrapped = (moduleName = {}, innerBindings = [
            (name = \"fetcher\", service = (name = {}, entrypoint = {}, props = (json = {})))
        ])),
",
        capnp_text(&d1.name),
        capnp_text("cloudflare-internal:d1-api"),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
    ));
    Ok(())
}

/// Emit a `queue` binding (T37): `Worker.Binding.queue` IS a
/// `ServiceDesignator` (workerd.capnp:406 `queue @15 :ServiceDesignator`)
/// — the binding points straight at the facade-queue system worker,
/// which receives the broker wire (`POST /message|/batch`, `GET
/// /metrics`) and forwards to the provider's queue route. Routing uses
/// `namespaceResourceId` = queueId in props (request-supplied queue
/// names never reach routing); `queueName` rides in props as worker-
/// facing display metadata only (miniflare: producerEntries →
/// `{name, queue:{name: serviceName}}` — same designator shape).
fn emit_queue_binding(
    out: &mut String,
    q: &snapshot_protocol::compute_desired::QueueBinding,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let mut props = json!({
        "bindingId": format!("{}~{}", worker.worker_id, q.name),
        "descriptorSha256": worker.active_version.artifact_digest,
        "instanceId": input.tenant.tenant_id,
        "namespaceResourceId": q.queue_id,
        "permissions": {"read": q.permissions.read, "write": q.permissions.write},
        "queueName": q.queue_name,
        "resourceSpecGeneration": input.revision,
        "versionId": worker.active_version.version_id,
        "workerId": worker.worker_id,
    });
    // Queue-level default send delay: emitted only when declared, so a
    // queue without `delivery_delay` keeps the exact props bytes it had
    // before the field existed. The facade treats an absent value as
    // "no default"; an explicit per-message delay always wins.
    if let Some(delay) = q.delivery_delay_seconds {
        props
            .as_object_mut()
            .expect("queue props are an object")
            .insert("deliveryDelaySeconds".into(), json!(delay));
    }
    let props = String::from_utf8(canonical_bytes(&props)).expect("canonical props are utf8");
    let facade = &input.params.facades.queue;
    out.push_str(&format!(
        "        (name = {}, queue = (name = {}, entrypoint = {}, props = (json = {}))),
",
        capnp_text(&q.name),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
    ));
    Ok(())
}

/// Emit a `k2` binding (K2 durable event log): `env.<name>` is a
/// `wrapped` binding over the `cloudflare-internal:k2` internal module;
/// the inner `fetcher` points at the facade-k2 system worker, which
/// signs the request and forwards it to the provider's k2 route.
/// Routing uses `namespaceResourceId` = streamId in props — a
/// client-supplied stream id never reaches routing (T37 precedent).
fn emit_k2_binding(
    out: &mut String,
    k: &snapshot_protocol::compute_desired::K2Binding,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let props = String::from_utf8(canonical_bytes(&json!({
        "bindingId": format!("{}~{}", worker.worker_id, k.name),
        "descriptorSha256": worker.active_version.artifact_digest,
        "instanceId": input.tenant.tenant_id,
        "namespaceResourceId": k.stream_id,
        "permissions": {"read": k.permissions.read, "write": k.permissions.write},
        "resourceSpecGeneration": input.revision,
        "versionId": worker.active_version.version_id,
        "workerId": worker.worker_id,
    })))
    .expect("canonical props are utf8");
    let facade = &input.params.facades.k2;
    out.push_str(&format!(
        "        (name = {}, wrapped = (moduleName = {}, innerBindings = [
            (name = \"fetcher\", service = (name = {}, entrypoint = {}, props = (json = {})))
        ])),
",
        capnp_text(&k.name),
        capnp_text("cloudflare-internal:k2"),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
    ));
    Ok(())
}

/// Emit a `send_email` binding (CF Email Service): `env.<name>` is a
/// `wrapped` binding over `cloudflare-internal:send-email`; the inner
/// `fetcher` is facade-email, which signs and forwards to the
/// provider's email route. The binding name doubles as the resource
/// key — restrictions travel in signed desired state, not props.
fn emit_email_binding(
    out: &mut String,
    e: &snapshot_protocol::compute_desired::SendEmailBinding,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let props = String::from_utf8(canonical_bytes(&json!({
        "bindingId": format!("{}~{}", worker.worker_id, e.name),
        "descriptorSha256": worker.active_version.artifact_digest,
        "instanceId": input.tenant.tenant_id,
        "namespaceResourceId": e.name,
        "permissions": {"read": e.permissions.read, "write": e.permissions.write},
        "resourceSpecGeneration": input.revision,
        "versionId": worker.active_version.version_id,
        "workerId": worker.worker_id,
    })))
    .expect("canonical props are utf8");
    let facade = &input.params.facades.email;
    out.push_str(&format!(
        "        (name = {}, wrapped = (moduleName = {}, innerBindings = [
            (name = \"fetcher\", service = (name = {}, entrypoint = {}, props = (json = {})))
        ])),
",
        capnp_text(&e.name),
        capnp_text("cloudflare-internal:send-email"),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
    ));
    Ok(())
}

/// `images` — `wrapped` binding over `cloudflare-internal:images-api`
/// (the upstream Miniflare module name); the inner `fetcher` points at
/// the facade-images system worker, which runs the vendored WASM
/// engine in-worker — no provider hop, no signed props needed (the
/// facade holds no secrets and transforms only its input bytes).
fn emit_images_binding(
    out: &mut String,
    img: &snapshot_protocol::compute_desired::ImagesBinding,
    _worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let facade = &input.params.facades.images;
    out.push_str(&format!(
        "        (name = {}, wrapped = (moduleName = {}, innerBindings = [
            (name = \"fetcher\", service = (name = {}, entrypoint = {}))
        ])),
",
        capnp_text(&img.name),
        capnp_text("cloudflare-internal:images-api"),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
    ));
    Ok(())
}

/// `vectorize` — `wrapped` binding over `cloudflare-internal:vectorize-api`.
/// innerBindings: `fetcher` → facade-vectorize (signed props like k2/d1 —
/// `namespaceResourceId` = indexId), `indexVersion` = "v2" (the current
/// GA wire; v1 is the deprecated beta surface with the same user API).
/// `useNdJson` is omitted so the shim always posts `{vectors:[...]}`
/// JSON — the backend accepts exactly that shape.
fn emit_vectorize_binding(
    out: &mut String,
    v: &snapshot_protocol::compute_desired::VectorizeBinding,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    let props = String::from_utf8(canonical_bytes(&json!({
        "bindingId": format!("{}~{}", worker.worker_id, v.name),
        "descriptorSha256": worker.active_version.artifact_digest,
        "instanceId": input.tenant.tenant_id,
        "namespaceResourceId": v.index_id,
        "permissions": {"read": v.permissions.read, "write": v.permissions.write},
        "resourceSpecGeneration": input.revision,
        "versionId": worker.active_version.version_id,
        "workerId": worker.worker_id,
    })))
    .expect("canonical props are utf8");
    let facade = &input.params.facades.vectorize;
    out.push_str(&format!(
        "        (name = {}, wrapped = (moduleName = {}, innerBindings = [
            (name = \"fetcher\", service = (name = {}, entrypoint = {}, props = (json = {}))),
            (name = \"indexId\", text = {}),
            (name = \"indexVersion\", text = \"v2\")
        ])),
",
        capnp_text(&v.name),
        capnp_text("cloudflare-internal:vectorize-api"),
        capnp_text(&facade.service),
        capnp_text(&facade.entrypoint),
        capnp_text(&props),
        capnp_text(&v.index_id),
    ));
    Ok(())
}

/// Emit one system (facade/egress) worker placeholder.
fn emit_system_worker(
    out: &mut String,
    system: &SystemWorkerRef,
    params: &RuntimeParams,
    embeds: &mut Vec<EmbedRef>,
    input: &TenantRenderInput<'_>,
) -> Result<(), ConfigError> {
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n",
        capnp_text(&system.service_name),
        capnp_text(&params.system_compat_date),
    ));
    let system_flags = crate::compat_flags::flags_for_compat_date(
        params.system_compat_flags.iter().map(|f| f.as_str()),
        &params.system_compat_date,
    );
    if !system_flags.is_empty() {
        out.push_str(&format!(
            "      compatibilityFlags = [{}],\n",
            system_flags
                .iter()
                .map(|f| capnp_text(f))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let mut modules: Vec<&SystemModuleRef> = system.modules.iter().collect();
    modules.sort_by(|a, b| utf16_cmp(&a.name, &b.name));
    let main = system.main_module.as_str();
    modules.sort_by_key(|m| if m.name == main { 0 } else { 1 });
    out.push_str("      modules = [\n");
    for module in modules {
        let path = format!("{SYS_DIR_PREFIX}/{}", module.digest);
        embeds.push(EmbedRef {
            path: path.clone(),
            digest: module.digest.clone(),
            kind: EmbedKind::SystemModule {
                service_name: system.service_name.clone(),
                module_name: module.name.clone(),
            },
        });
        out.push_str(&format!(
            "        (name = {}, {} = embed {}),\n",
            capnp_text(&module.name),
            system_module_capnp_field(&module.name),
            capnp_text(&path),
        ));
    }
    out.push_str("      ],\n");
    let mut bindings = Vec::new();
    if system.needs_binding_backend {
        bindings.push(format!(
            "(name = \"BINDING_BACKEND\", service = {})",
            capnp_text(BINDING_BACKEND_SERVICE),
        ));
    }
    if system.service_name == params.egress.service {
        // The egress worker gets the typed policy as canonical JSON in a
        // text binding; it re-validates the document and fails closed.
        bindings.push(format!(
            "(name = {}, text = {})",
            capnp_text(EGRESS_POLICY_BINDING),
            capnp_text(&egress_policy_json(input)?),
        ));
        // IMG2: `cf.image` fetch options transform through the shared
        // facade-images worker — wired unconditionally because cf.image
        // is a fetch option, not a declared binding (a tenant that never
        // declares `images` must not get a silently untransformed image).
        bindings.push(format!(
            "(name = \"VRT_IMAGES\", service = (name = {}, entrypoint = {}))",
            capnp_text(&params.facades.images.service),
            capnp_text(&params.facades.images.entrypoint),
        ));
    }
    if system.service_name == params.egress.service
        && !internal_tcp_targets(input).is_empty()
    {
        // MIG-15: `connect()` to a synthetic internal host is routed
        // through this service binding to the loopback-only network
        // that terminates at the provider's pg-wire proxy.
        bindings.push(format!(
            "(name = \"INTERNAL_NET\", service = (name = {}))",
            capnp_text(INTERNAL_NETWORK_SERVICE),
        ));
    }
    if !bindings.is_empty() {
        out.push_str("      bindings = [\n");
        for binding in bindings {
            out.push_str(&format!("        {binding},\n"));
        }
        out.push_str("      ],\n");
    }
    // Outbound split (T6 review): the egress worker gets the raw
    // `outbound-network`; EVERY other system worker — facades included —
    // gets the deny-all `vrt-no-network` entrypoint. Facades only talk
    // to `binding-backend` over unix; giving them the tenant egress
    // path would grant public network access they never need. workerd
    // defaults `globalOutbound` to `internet`, so the deny designator
    // must be emitted explicitly.
    if system.service_name == params.egress.service {
        out.push_str(&format!(
            "      globalOutbound = {},\n    )),\n",
            capnp_text(OUTBOUND_NETWORK_SERVICE),
        ));
    } else {
        out.push_str(&format!(
            "      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
            capnp_text(&params.deny_outbound.service),
            capnp_text(&params.deny_outbound.entrypoint),
        ));
    }
    Ok(())
}

/// `worker-<workerId>` capnp service name.
fn worker_service_name(worker_id: &str) -> String {
    format!("worker-{worker_id}")
}

/// Sorted unique dataset names across a tenant's `analyticsEngine`
/// bindings — one `external` service + one `--external-addr` entry per
/// distinct name (the designator name doubles as the event `dataset`).
fn ae_datasets(tenant: &TenantDesired) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    for worker in &tenant.workers {
        for binding in &worker.bindings {
            if let BindingDesired::AnalyticsEngine(ae) = binding {
                out.push(ae.dataset.as_str());
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// P3c-1: `do-storage-<workerId>` capnp service name — the worker's
/// `durableObjectStorage.localDisk` target (a `disk`-backed service).
/// W1: `wf-engine-<workerId>-<wfName>` capnp service name. Both
/// components are already identifier-validated in desired::validate
/// (`w_[0-9a-f]{16}` / `[A-Za-z0-9_-]`), so the join is deterministic
/// and collision-free per (worker, workflow).
/// W3: `facade-wfops-<workerId>` capnp service name — vrt-host's
/// `/v1/wfops` handler resolves this via the endpoint slot map.
pub fn wfops_service_name(worker_id: &str) -> String {
    format!("facade-wfops-{worker_id}")
}

/// W3: `sock-wfops-<workerId>` socket name (vrt-host endpoint key).
fn wfops_socket_name(worker_id: &str) -> String {
    format!("sock-wfops-{worker_id}")
}

/// CR1: `facade-cron-<workerId>` capnp service name — the
/// platform-owned dispatcher that calls `env.TARGET.scheduled()`.
pub fn cron_facade_service_name(worker_id: &str) -> String {
    format!("facade-cron-{worker_id}")
}

/// CR1: `sock-cron-<workerId>` socket name (vrt-host endpoint key) —
/// unix-only, `<id>-cron.sock`; never on tenant ingress.
fn cron_socket_name(worker_id: &str) -> String {
    format!("sock-cron-{worker_id}")
}

/// CR1: emit `facade-cron-<workerId>` — the trusted scheduled()
/// dispatcher. `TARGET` points at the tenant worker's DEFAULT
/// entrypoint; `Fetcher.scheduled()` is the only channel that
/// produces a real scheduled event in workerd. Emitted only when the
/// worker declares crons — a trigger against a cron-less worker fails
/// closed at the endpoint slot instead of materializing a facade.
/// `service_binding_extra_handlers` is mandatory (it is what enables
/// `.scheduled()` on the caller side — `$experimental`, so the tenant
/// argv derivation adds `--experimental` for any cron-carrying
/// generation) and unioned in like the qconsumer surface.
fn emit_cron_facade(
    out: &mut String,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    embeds: &mut Vec<EmbedRef>,
) -> Result<(), ConfigError> {
    let digest = crate::workflows::cron_dispatcher_sha256();
    let path = format!("{SYS_DIR_PREFIX}/{digest}");
    embeds.push(EmbedRef {
        path: path.clone(),
        digest,
        kind: EmbedKind::VendoredModule {
            name: "cron-dispatcher".to_string(),
        },
    });
    let mut flags: Vec<&str> = input
        .params
        .system_compat_flags
        .iter()
        .map(|f| f.as_str())
        .collect();
    if !flags.contains(&"service_binding_extra_handlers") {
        flags.push("service_binding_extra_handlers");
    }
    let flags = crate::compat_flags::flags_for_compat_date(
        flags,
        &input.params.system_compat_date,
    );
    out.push_str(&format!(
        "    (name = {}, worker = (
      compatibilityDate = {},
      compatibilityFlags = [{}],
      modules = [
        (name = {}, esModule = embed {}),
      ],
      bindings = [
        (name = \"TARGET\", service = {}),
      ],
      globalOutbound = (name = {}, entrypoint = {}),
    )),
",
        capnp_text(&cron_facade_service_name(&worker.worker_id)),
        capnp_text(&input.params.system_compat_date),
        flags
            .iter()
            .map(|f| capnp_text(f))
            .collect::<Vec<_>>()
            .join(", "),
        capnp_text("cron-dispatcher.js"),
        capnp_text(&path),
        capnp_text(&worker_service_name(&worker.worker_id)),
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}

/// W3: emit the platform wfops facade service for a worker. Bindings:
/// `WFOP_TABLE` json = {workflowName: {binding, token}} where token =
/// sha256("vrt-wfop:" + uniqueKey) — the cell executor derives the same
/// token from the op payload's wfUniqueKey; `WFB_<binding>` service
/// designators point at the SAME engine `WorkflowBinding` entrypoint
/// the tenant's `env.<binding>` wrapped binding uses.
fn emit_wfops_facade(
    out: &mut String,
    worker: &WorkerDesired,
    targets: &[(
        &WorkerDesired,
        &snapshot_protocol::compute_desired::WorkflowDesired,
    )],
    input: &TenantRenderInput<'_>,
    embeds: &mut Vec<EmbedRef>,
) -> Result<(), ConfigError> {
    let digest = crate::workflows::wfops_facade_sha256();
    let path = format!("{SYS_DIR_PREFIX}/{digest}");
    embeds.push(EmbedRef {
        path: path.clone(),
        digest: digest.clone(),
        kind: EmbedKind::VendoredModule {
            name: "wfops-facade".to_string(),
        },
    });
    out.push_str(&format!(
        "    (name = {}, worker = (
      compatibilityDate = {},
      compatibilityFlags = [{}],
      modules = [
        (name = {}, esModule = embed {}),
      ],
",
        capnp_text(&wfops_service_name(&worker.worker_id)),
        capnp_text(crate::workflows::ENGINE_COMPAT_DATE),
        crate::compat_flags::flags_for_compat_date(
            crate::workflows::ENGINE_COMPAT_FLAGS.iter().copied(),
            crate::workflows::ENGINE_COMPAT_DATE,
        )
        .iter()
        .map(|f| capnp_text(f))
        .collect::<Vec<_>>()
        .join(", "),
        capnp_text("wfops-facade.js"),
        capnp_text(&path),
    ));
    out.push_str(
        "      bindings = [
",
    );
    // WFOP_TABLE: workflow name → {binding, token}. Token input is the
    // declaration uniqueKey — unguessable storage identity; rendered
    // only here (this service's env), never into the tenant worker.
    // W3-r2: tokens are HMAC'd with the host-process pepper — an
    // empty pepper means the assembler never wired one (misconfig),
    // so this worker's emit fails closed rather than rendering a
    // forgeable token.
    if input.params.wfop_pepper.is_empty() {
        return Err(ConfigError::Identifier("wfopPepper"));
    }
    let mut table = String::from("{");
    for (i, (_, wf)) in targets.iter().enumerate() {
        let token = crate::workflows::wfop_token(&input.params.wfop_pepper, &wf.unique_key);
        table.push_str(&format!(
            "{}\"{}\":{{\"binding\":\"{}\",\"token\":\"{}\"}}",
            if i == 0 { "" } else { "," },
            wf.name.replace('\\', "\\\\").replace('"', "\\\""),
            wf.binding.replace('\\', "\\\\").replace('"', "\\\""),
            token
        ));
    }
    table.push('}');
    out.push_str(&format!(
        "        (name = \"WFOP_TABLE\", json = {}),
",
        capnp_text(&table)
    ));
    // W3-r4: create-confirm budget (ms) rendered into the facade env —
    // bounded hold so a wedge in engine init can NEVER hang the op.
    out.push_str(&format!(
        "        (name = \"WF_CREATE_CONFIRM_MS\", text = {}),
",
        capnp_text("10000")
    ));
    // W6-r2: delete-confirm budget (ms) — bounded get() poll until
    // instance.not_found after the engine committed deleteAll(). The
    // facade no longer touches the loopback: physical unlink is the
    // host reclaimer's job at a proven no-actor safe point.
    out.push_str(&format!(
        "        (name = \"WF_DELETE_CONFIRM_MS\", text = {}),
",
        capnp_text("10000")
    ));
    let mut wfs: Vec<(
        &WorkerDesired,
        &snapshot_protocol::compute_desired::WorkflowDesired,
    )> = targets.to_vec();
    wfs.sort_by(|a, b| utf16_cmp(&a.1.name, &b.1.name));
    for (decl_worker, wf) in wfs {
        out.push_str(&format!(
            "        (name = {}, service = (name = {}, entrypoint = \"WorkflowBinding\")),
",
            capnp_text(&format!("WFB_{}", wf.binding)),
            capnp_text(&wf_engine_service_name(
                wf_defining_id(decl_worker, wf),
                &wf.name,
            )),
        ));
    }
    out.push_str(
        "      ],
",
    );
    out.push_str(&format!(
        "      globalOutbound = (name = {}, entrypoint = {}),
    )),
",
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}

fn wf_engine_service_name(worker_id: &str, wf_name: &str) -> String {
    format!("wf-engine-{worker_id}-{wf_name}")
}

/// W7: the DEFINING worker of a workflow declaration — engine
/// service, DO storage, loopback and ops facade all render under it
/// (CF `script_name` ownership: the instance belongs to the target
/// script, not the binding one). `None`/self ⇒ the declarer.
fn wf_defining_id<'a>(
    worker: &'a WorkerDesired,
    wf: &'a snapshot_protocol::compute_desired::WorkflowDesired,
) -> &'a str {
    wf.service_worker_id
        .as_deref()
        .unwrap_or(worker.worker_id.as_str())
}

/// Every `(declaring worker, declaration)` pair whose engine renders
/// under `target_wid` — the worker's own declarations plus foreign
/// `script_name` declarations resolved onto it. Same-tenant
/// containment and (target, name) uniqueness are proven in
/// `validate_tenant` — a dangling target can never reach render.
fn wf_targets<'a>(
    input: &'a TenantRenderInput<'_>,
    target_wid: &str,
) -> Vec<(
    &'a WorkerDesired,
    &'a snapshot_protocol::compute_desired::WorkflowDesired,
)> {
    input
        .tenant
        .workers
        .iter()
        .flat_map(|w| w.workflows.iter().map(move |wf| (w, wf)))
        .filter(|(w, wf)| wf_defining_id(w, wf) == target_wid)
        .collect()
}

/// W1: emit one internal workflows Engine service. Mirrors miniflare
/// WORKFLOWS_PLUGIN wiring: vendored `binding.worker.js` as
/// `workflows.mjs`, an `Engine` DO namespace (`enableSql`,
/// `preventEviction`, server-minted uniqueKey), and the five engine
/// bindings. `durableObjectStorage` reuses the DECLARING worker's
/// `do-storage-<workerId>` disk service → engine SQLite lands in the
/// tenant DO dir under that worker's class set (placement, fencing
/// and snapshots all apply — no parallel storage path).
fn emit_workflow_engine(
    out: &mut String,
    worker: &WorkerDesired,
    wf: &snapshot_protocol::compute_desired::WorkflowDesired,
    input: &TenantRenderInput<'_>,
    embeds: &mut Vec<EmbedRef>,
) -> Result<(), ConfigError> {
    let digest = crate::workflows::engine_worker_sha256();
    let path = format!("{SYS_DIR_PREFIX}/{digest}");
    embeds.push(EmbedRef {
        path: path.clone(),
        digest: digest.clone(),
        kind: EmbedKind::VendoredModule {
            name: "workflows-engine".to_string(),
        },
    });
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n      compatibilityFlags = [{}],\n      modules = [\n        (name = {}, esModule = embed {}),\n      ],\n",
        capnp_text(&wf_engine_service_name(
            wf_defining_id(worker, wf),
            &wf.name,
        )),
        capnp_text(crate::workflows::ENGINE_COMPAT_DATE),
        crate::compat_flags::flags_for_compat_date(
            crate::workflows::ENGINE_COMPAT_FLAGS.iter().copied(),
            crate::workflows::ENGINE_COMPAT_DATE,
        )
        .iter()
        .map(|f| capnp_text(f))
        .collect::<Vec<_>>()
        .join(", "),
        capnp_text(crate::workflows::ENGINE_MODULE_NAME),
        capnp_text(&path),
    ));
    out.push_str("      bindings = [\n");
    out.push_str(
        "        (name = \"ENGINE\", durableObjectNamespace = (className = \"Engine\")),\n",
    );
    // USER_WORKFLOW — service designator points at the DEFINING
    // worker's service + declared entrypoint (W7: `script_name`
    // targets another same-tenant worker's service — workerd service
    // designators are config-global, no loopback hop needed).
    out.push_str(&format!(
        "        (name = \"USER_WORKFLOW\", service = (name = {}, entrypoint = {})),\n",
        capnp_text(&worker_service_name(wf_defining_id(worker, wf))),
        capnp_text(&wf.class_name),
    ));
    out.push_str(&format!(
        "        (name = \"BINDING_NAME\", text = {}),\n",
        capnp_text(&wf.binding),
    ));
    out.push_str(&format!(
        "        (name = \"WORKFLOW_NAME\", text = {}),\n",
        capnp_text(&wf.name),
    ));
    // W6-r2: `env.MINIFLARE_LOOPBACK` → per-worker shim Worker —
    // DELETE records a durable tombstone in the host (physical unlink
    // happens later at the supervisor safe point, never while an
    // actor can hold the db); GET wait returns 204. The engine
    // swallows shim failures, so the FACADE's own bounded get() poll
    // is the op-level confirmation.
    out.push_str(&format!(
        "        (name = {}, service = {}),\n",
        capnp_text(crate::workflows::LOOPBACK_BINDING),
        capnp_text(&crate::workflows::loopback_service_name(wf_defining_id(
            worker, wf,
        ))),
    ));
    if let Some(steps) = wf.limits_steps {
        out.push_str(&format!(
            "        (name = \"STEP_LIMIT\", json = \"{}\"),\n",
            steps,
        ));
    }
    out.push_str("      ],\n");
    out.push_str(&format!(
        "      durableObjectNamespaces = [\n        (className = \"Engine\", uniqueKey = {}, enableSql = true, preventEviction = true),\n      ],\n",
        capnp_text(&wf.unique_key),
    ));
    out.push_str(&format!(
        "      durableObjectStorage = (localDisk = {}),\n",
        capnp_text(&do_storage_service_name(wf_defining_id(worker, wf))),
    ));
    // The engine talks ONLY to its DO storage + the declaring worker
    // via service bindings — no outbound network.
    out.push_str(&format!(
        "      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}

/// W6-r2: emit the per-worker `vrt-wf-loopback-<workerId>` shim —
/// answers the vendored engine's MINIFLARE_LOOPBACK calls by
/// forwarding `DELETE`/`waitForPendingDelete` to the host's
/// tombstone service via `VRT_WFDEL`. Without a host control
/// address the shim degrades to explicit 501 on DELETE and
/// 200 {pending:false} on GET. Internal only: no socket, deny-all
/// outbound — tenant workers can never reach it.
fn emit_workflow_loopback(
    out: &mut String,
    worker: &WorkerDesired,
    targets: &[(
        &WorkerDesired,
        &snapshot_protocol::compute_desired::WorkflowDesired,
    )],
    input: &TenantRenderInput<'_>,
    embeds: &mut Vec<EmbedRef>,
) -> Result<(), ConfigError> {
    let digest = crate::workflows::loopback_worker_sha256();
    let path = format!("{SYS_DIR_PREFIX}/{digest}");
    embeds.push(EmbedRef {
        path: path.clone(),
        digest: digest.clone(),
        kind: EmbedKind::VendoredModule {
            name: "workflows-loopback".to_string(),
        },
    });
    // WF_UK: tenant-facing workflow name → storage uniqueKey (all
    // declarations DEFINING on this worker — the shim can never mint
    // a path for a name no declaration resolved onto it).
    let mut uk_map = serde_json::Map::new();
    for (_, wf) in targets {
        uk_map.insert(
            wf.name.clone(),
            serde_json::Value::String(wf.unique_key.clone()),
        );
    }
    let mut bindings = format!(
        "        (name = \"WF_UK\", json = {}),\n",
        capnp_text(&serde_json::Value::Object(uk_map).to_string()),
    );
    if !input.params.wf_delete_addr.is_empty() {
        bindings.push_str(&format!(
            "        (name = \"VRT_WFDEL\", service = {}),\n",
            capnp_text(crate::workflows::WFDEL_SERVICE),
        ));
    }
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n      modules = [\n        (name = \"loopback.mjs\", esModule = embed {}),\n      ],\n      bindings = [\n{}      ],\n      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
        capnp_text(&crate::workflows::loopback_service_name(
            &worker.worker_id,
        )),
        capnp_text(&input.params.system_compat_date),
        capnp_text(&path),
        bindings,
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}

fn do_storage_service_name(worker_id: &str) -> String {
    format!("do-storage-{worker_id}")
}

/// T31c: `router-<workerId>` capnp service name — the Cloudflare
/// assets-first router system worker emitted between the ingress
/// socket and the user worker when the worker's assets binding
/// carries `assetsFirst`. Absent field ⇒ no router service and the
/// socket points at `worker-<id>` — byte-identical legacy output.
fn router_service_name(worker_id: &str) -> String {
    format!("router-{worker_id}")
}

/// Service name of the assets-first router system worker — resolved
/// from `system_workers` by name (its module set ships in the facade
/// manifest like every other system worker).
const ROUTER_SYSTEM_SERVICE: &str = "facade-router";
/// Text binding carrying the router policy JSON.
const ROUTER_CFG_BINDING: &str = "VRT_ROUTER_CFG";
/// Service binding through which the router invokes the user worker.
const ROUTER_USER_WORKER_BINDING: &str = "USER_WORKER";
/// Fixed env binding name the router uses for the assets facade —
/// decoupled from the user's *declared* binding name (wrangler
/// `"assets": {"binding": "STATIC"}` must never rename the router's
/// own env key; `facade-router.ts` reads `env.ASSETS`). The three
/// router binding names are pinned one-to-one with
/// `facade/router-wire.ts` by `router_binding_names_match_ts_wire`.
const ROUTER_ASSETS_BINDING: &str = "ASSETS";

/// `VRT_S_<enc(workerId)>__<enc(bindingName)>` — the environment
/// variable name backing a `fromEnvironment` secret binding.
///
/// `enc` keeps `[A-Za-z0-9]` and byte-escapes everything else as
/// `_x<hh>`: `_`→`_x5f`, `-`→`_x2d`, `.`→`_x2e`, `:`→`_x3a`, `$`→`_x24`.
/// Escaped output contains `_` only inside `_xHH` sequences, so the
/// `__` separator can never appear inside an encoded half — the mapping
/// `(workerId, bindingName) → envName` is injective (no two bindings can
/// collide on an env name), deterministic, and composed solely of
/// `[A-Za-z0-9_]` (valid env-name charset).
pub fn secret_env_name(worker_id: &str, binding_name: &str) -> String {
    fn enc(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            if b.is_ascii_alphanumeric() {
                out.push(b as char);
            } else {
                out.push_str(&format!("_x{b:02x}"));
            }
        }
        out
    }
    format!("VRT_S_{}__{}", enc(worker_id), enc(binding_name))
}

/// T17/F1: shared `var` binding resolution — ONE lookup used by both
/// the capnp emitter and the stub/standalone env path. A missing ref
/// fails closed, never silently renders empty.
pub fn resolve_var<'m>(
    vars: &'m std::collections::BTreeMap<String, String>,
    var_ref: &str,
) -> Result<&'m str, ConfigError> {
    vars.get(var_ref)
        .map(|v| v.as_str())
        .ok_or(ConfigError::UnresolvedReference("varRef"))
}

/// T17/F1: shared `secret` binding resolution — tenant-scoped ONLY
/// (T13/M6): a ref in another tenant's namespace never resolves.
pub fn resolve_secret<'m>(
    secrets: &'m std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, SecretValue>,
    >,
    tenant_id: &str,
    secret_ref: &str,
) -> Result<&'m SecretValue, ConfigError> {
    secrets
        .get(tenant_id)
        .and_then(|m| m.get(secret_ref))
        .ok_or(ConfigError::UnresolvedReference("secretRef"))
}

/// T17/F1: resolve every `secret` binding of a tenant's workers into
/// the child-env map (`envName → value`). Shared by the capnp emitter
/// and the stub/standalone assembler so env resolution has exactly
/// one implementation. `vars` are also existence-checked (a stub has
/// no config text to fail at launch time).
pub fn resolve_tenant_env(
    tenant: &snapshot_protocol::compute_desired::TenantDesired,
    vars: &std::collections::BTreeMap<String, String>,
    secrets: &std::collections::BTreeMap<String, std::collections::BTreeMap<String, SecretValue>>,
) -> Result<std::collections::BTreeMap<String, SecretValue>, ConfigError> {
    let mut env = std::collections::BTreeMap::new();
    for worker in &tenant.workers {
        for binding in &worker.bindings {
            match binding {
                snapshot_protocol::compute_desired::BindingDesired::Var(var) => {
                    resolve_var(vars, &var.var_ref)?;
                }
                snapshot_protocol::compute_desired::BindingDesired::Secret(secret) => {
                    let value = resolve_secret(secrets, &tenant.tenant_id, &secret.secret_ref)?;
                    let env_name = secret_env_name(&worker.worker_id, &secret.name);
                    if env.insert(env_name, value.clone()).is_some() {
                        return Err(ConfigError::Identifier("secretEnv"));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(env)
}

/// `sock-<workerId>` socket name (vrt-host maps Host → this socket).
fn worker_socket_name(worker_id: &str) -> String {
    format!("sock-{worker_id}")
}

/// T37b: `facade-qconsumer-<workerId>` capnp service name — the
/// dispatch delivery facade bound `CONSUMER → worker-<id>`.
pub fn qconsumer_service_name(worker_id: &str) -> String {
    format!("facade-qconsumer-{worker_id}")
}

/// T37b: `sock-qconsumer-<workerId>` socket name (vrt-host
/// `/v1/queue/dispatch` proxies here).
fn qconsumer_socket_name(worker_id: &str) -> String {
    format!("sock-qconsumer-{worker_id}")
}

/// System-worker manifest template for the qconsumer facade.
const QCONSUMER_SYSTEM_SERVICE: &str = "facade-qconsumer";
/// Service binding through which the facade invokes
/// `Fetcher.queue()` on the consumer worker.
const QCONSUMER_BINDING: &str = "CONSUMER";

/// Match each worker's `artifactDigest` to a provided bundle. Missing or
/// extra bundles fail closed; two workers may share one bundle.
fn match_bundles<'a, 't>(
    tenant: &'t TenantDesired,
    bundles: &'a [VersionBundle],
) -> Result<std::collections::HashMap<&'t str, &'a VersionBundle>, ConfigError> {
    let mut by_digest = std::collections::HashMap::new();
    for bundle in bundles {
        if !by_digest.insert(bundle.digest(), bundle).is_none() {
            return Err(ConfigError::BundleMismatch("duplicateBundle"));
        }
    }
    let mut matched = std::collections::HashMap::new();
    let mut used = HashSet::new();
    for worker in &tenant.workers {
        let digest = worker.active_version.artifact_digest.as_str();
        let bundle = by_digest
            .get(digest)
            .ok_or(ConfigError::BundleMismatch("missing"))?;
        used.insert(bundle.digest());
        matched.insert(worker.worker_id.as_str(), *bundle);
    }
    for bundle in bundles {
        if !used.contains(bundle.digest()) {
            return Err(ConfigError::BundleMismatch("extra"));
        }
    }
    Ok(matched)
}

/// Re-check the tenant fragment's fields that render depends on
/// (identifiers, digests, worker/binding names, compat fields, service
/// targets). `compute.desired::validate` is the authoritative gate; this
/// is defense in depth for callers that hold an already-decoded value.
fn validate_tenant(tenant: &TenantDesired) -> Result<(), ConfigError> {
    if !valid_identifier(&tenant.tenant_id, MAX_IDENTIFIER_LEN) {
        return Err(ConfigError::Identifier("tenantId"));
    }
    let mut worker_ids = HashSet::new();
    for worker in &tenant.workers {
        if !valid_identifier(&worker.worker_id, MAX_IDENTIFIER_LEN)
            || !worker_ids.insert(worker.worker_id.as_str())
        {
            return Err(ConfigError::Identifier("workerId"));
        }
        let version = &worker.active_version;
        if !is_lower_hex(&version.artifact_digest) {
            return Err(ConfigError::Digest("artifactDigest"));
        }
        if !valid_compat_date(&version.compatibility_date)
            || version.compatibility_flags.len() > MAX_COMPAT_FLAGS
            || version
                .compatibility_flags
                .iter()
                .any(|f| !valid_compat_flag(f))
        {
            return Err(ConfigError::Identifier("compatibility"));
        }
        // T37b-r2: tenant flags must be KNOWN STABLE — experimental
        // (`service_binding_extra_handlers`, `experimental`, …) or
        // unknown names are rejected at emit depth too (control-plane
        // zod is the first gate). Consumer generations run workerd with
        // `--experimental`, so a slipped flag would silently APPLY to
        // tenant code otherwise. System/facade workers are exempt —
        // the platform emits them itself.
        if version
            .compatibility_flags
            .iter()
            .any(|f| !crate::compat_flags::is_tenant_allowed_compat_flag(f))
        {
            return Err(ConfigError::CompatFlag(
                if version
                    .compatibility_flags
                    .iter()
                    .any(|f| crate::compat_flags::is_experimental_compat_flag(f))
                {
                    "compatFlag.experimental"
                } else {
                    "compatFlag.unknown"
                },
            ));
        }
        if !valid_identifier(&version.version_id, MAX_IDENTIFIER_LEN) {
            return Err(ConfigError::Identifier("versionId"));
        }
        // Per-worker quotas: bounds are re-checked at emit depth
        // (compute.desired validation is the first gate) and
        // `outboundConnections` is refused outright — the pinned workerd
        // fork has no connection-counter hook, so a declared value would
        // be silently ignored (fail closed, never a fake limit).
        let quotas = &worker.quotas;
        if !(snapshot_protocol::compute_desired::MIN_CPU_MS
            ..=snapshot_protocol::compute_desired::MAX_CPU_MS)
            .contains(&quotas.cpu_ms)
        {
            return Err(ConfigError::Quotas("cpuMs"));
        }
        if !(snapshot_protocol::compute_desired::MIN_MEMORY_MB
            ..=snapshot_protocol::compute_desired::MAX_MEMORY_MB)
            .contains(&quotas.memory_mb)
        {
            return Err(ConfigError::Quotas("memoryMb"));
        }
        if quotas.subrequests > snapshot_protocol::compute_desired::MAX_SUBREQUESTS {
            return Err(ConfigError::Quotas("subrequests"));
        }
        if quotas.outbound_connections > snapshot_protocol::compute_desired::MAX_OUTBOUND_CONNECTIONS
        {
            return Err(ConfigError::Quotas("outboundConnections"));
        }
        if quotas.declares_unenforceable_limit() {
            return Err(ConfigError::Quotas("outboundConnections"));
        }
        let mut names = HashSet::new();
        for binding in &worker.bindings {
            if !valid_binding_name(binding.name()) || !names.insert(binding.name()) {
                return Err(ConfigError::Identifier("bindingName"));
            }
            // AE: `dataset` becomes an `--external-addr` service token
            // AND a spool/parquet path segment — the `[A-Za-z0-9_]`
            // bound must hold at emit depth too (defense-in-depth
            // behind compute.desired validation).
            if let BindingDesired::AnalyticsEngine(ae) = binding {
                if ae.dataset.is_empty()
                    || ae.dataset.len() > 256
                    || !ae
                        .dataset
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(ConfigError::Identifier("bindings.dataset"));
                }
            }
        }
        // P3c-1: DO class declarations — format + uniqueness at emit
        // depth (defense-in-depth behind compute.desired validation).
        // A DO worker's id also becomes a DIRECTORY segment
        // (`<doStorageDir>/<workerId>`): a pure-dot id (`"."`, `".."`,
        // or a `:`-carrying name that encodes oddly on some
        // filesystems) must never reach the join — fail closed.
        if !worker.durable_object_classes.is_empty()
            && (worker.worker_id.bytes().all(|b| b == b'.')
                || worker.worker_id.contains('/')
                || worker.worker_id.contains('\0'))
        {
            return Err(ConfigError::Identifier("workerId.doPath"));
        }
        let mut class_names = HashSet::new();
        for class in &worker.durable_object_classes {
            if !valid_binding_name(&class.class_name)
                || !class_names.insert(class.class_name.as_str())
            {
                return Err(ConfigError::Identifier("durableObjectClasses"));
            }
            if !is_lower_hex(&class.unique_key) {
                return Err(ConfigError::Digest("durableObjectClasses.uniqueKey"));
            }
        }
    }
    // P3c-1: DO binding resolution — every `durableObject` binding's
    // (className, serviceWorkerId) must resolve to a class DECLARED by
    // a worker in THIS tenant; anything else binds nothing and fails
    // closed (mirrors the snapshot-layer check, one depth lower).
    for worker in &tenant.workers {
        for binding in &worker.bindings {
            if let BindingDesired::DurableObject(d) = binding {
                if !valid_binding_name(&d.class_name) {
                    return Err(ConfigError::Identifier("bindings.className"));
                }
                let target = d.service_worker_id.as_deref().unwrap_or(&worker.worker_id);
                let declares = tenant.workers.iter().any(|w| {
                    w.worker_id == target
                        && w.durable_object_classes
                            .iter()
                            .any(|c| c.class_name == d.class_name)
                });
                if !declares {
                    return Err(ConfigError::ServiceReference("durableObject"));
                }
            }
        }
    }
    // Tail consumers (`tail_consumers`): same-tenant, different worker,
    // unique per producer. workerd rejects a `tails` designator that
    // names no service, so any dangling/self entry fails the render
    // before a config is written (the snapshot layer already rejects it;
    // this is the last trusted boundary).
    for worker in &tenant.workers {
        if worker.tail_consumers.len()
            > snapshot_protocol::compute_desired::MAX_TAIL_CONSUMERS_PER_WORKER
        {
            return Err(ConfigError::Capacity("tailConsumers"));
        }
        let mut targets = HashSet::new();
        for tail in &worker.tail_consumers {
            if !valid_identifier(&tail.worker_id, MAX_IDENTIFIER_LEN)
                || tail.worker_id == worker.worker_id
                || !worker_ids.contains(tail.worker_id.as_str())
            {
                return Err(ConfigError::ServiceReference("tailConsumers"));
            }
            if !targets.insert(tail.worker_id.as_str()) {
                return Err(ConfigError::Identifier("tailConsumers"));
            }
        }
    }
    Ok(())
}

/// Validate runtime parameters.
fn validate_params(params: &RuntimeParams) -> Result<(), ConfigError> {
    // Policy validity is render-blocking: an invalid policy must not
    // produce a config that leaves the egress worker silently open.
    params.egress_policy.validate()?;
    if params.log_tail.enabled() && params.log_tail.pepper.is_empty() {
        return Err(ConfigError::Identifier("logTailPepper"));
    }
    for (field, path) in [
        ("socketDir", &params.socket_dir),
        ("bindingBackendSocket", &params.binding_backend_socket),
    ] {
        if !valid_path(path) {
            return Err(ConfigError::Identifier(field));
        }
    }
    // P3c-1: an empty `doStorageDir` means "no DO storage configured";
    // a non-empty one must be a traversal-free absolute path (the
    // per-tenant root vrt-host pre-creates).
    if !params.do_storage_dir.is_empty()
        && (!params.do_storage_dir.starts_with('/') || !valid_path(&params.do_storage_dir))
    {
        return Err(ConfigError::Identifier("doStorageDir"));
    }
    for (field, endpoint) in [
        ("egress", &params.egress),
        ("denyOutbound", &params.deny_outbound),
        ("facades.kv", &params.facades.kv),
        ("facades.r2", &params.facades.r2),
        ("facades.assets", &params.facades.assets),
        ("facades.pg", &params.facades.pg),
        ("facades.vectorize", &params.facades.vectorize),
    ] {
        if !valid_service_name(&endpoint.service) {
            return Err(ConfigError::Identifier(field));
        }
        if !valid_entrypoint(&endpoint.entrypoint) {
            return Err(ConfigError::Identifier(field));
        }
    }
    let mut names = BTreeSet::new();
    for system in &params.system_workers {
        if !valid_service_name(&system.service_name) || !names.insert(&system.service_name) {
            return Err(ConfigError::Identifier("systemWorkers.serviceName"));
        }
        if system.modules.is_empty() {
            return Err(ConfigError::Identifier("systemWorkers.modules"));
        }
        if !valid_module_name(&system.main_module)
            || !system.modules.iter().any(|m| m.name == system.main_module)
        {
            return Err(ConfigError::Identifier("systemWorkers.mainModule"));
        }
        for module in &system.modules {
            if !valid_module_name(&module.name) {
                return Err(ConfigError::Identifier("systemWorkers.moduleName"));
            }
            if !is_lower_hex(&module.digest) {
                return Err(ConfigError::Digest("systemWorkers.digest"));
            }
        }
    }
    if !valid_compat_date(&params.system_compat_date)
        || params.system_compat_flags.len() > MAX_COMPAT_FLAGS
        || params
            .system_compat_flags
            .iter()
            .any(|f| !valid_compat_flag(f))
    {
        return Err(ConfigError::Identifier("systemCompatibility"));
    }
    // MIG-14: `connect_pass_through` on the egress worker would bypass
    // `EgressProxy.connect()` entirely — workerd forwards the CONNECT
    // to the worker's own `globalOutbound` (`outbound-network`), i.e.
    // public TCP with no port/name policy. It must never be renderable.
    if params
        .system_compat_flags
        .iter()
        .any(|f| f == "connect_pass_through")
    {
        return Err(ConfigError::Identifier("systemCompatibility"));
    }
    for (key, value) in &params.vars {
        if !valid_identifier(key, MAX_IDENTIFIER_LEN * 2) {
            return Err(ConfigError::Identifier("vars"));
        }
        if value.len() > MAX_VALUE_LEN {
            return Err(ConfigError::Capacity("vars"));
        }
    }
    for (tenant_id, map) in &params.secrets {
        if !valid_identifier(tenant_id, MAX_IDENTIFIER_LEN * 2) || map.len() > 4096 {
            return Err(ConfigError::Identifier("secrets"));
        }
        for (key, value) in map {
            if !valid_identifier(key, MAX_IDENTIFIER_LEN * 2) {
                return Err(ConfigError::Identifier("secrets"));
            }
            if value.expose().len() > MAX_VALUE_LEN {
                return Err(ConfigError::Capacity("secrets"));
            }
        }
    }
    Ok(())
}

/// `[A-Za-z0-9_.:-]` identifier — same charset as `compute_desired`.
fn valid_identifier(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

/// capnp service names for platform-defined services.
pub(crate) fn valid_service_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SERVICE_NAME_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Exported entrypoint class names (JS identifier).
pub(crate) fn valid_entrypoint(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ENTRYPOINT_LEN
        && (value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'$'))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// Binding names — same rule as `compute_desired::valid_binding_name`.
fn valid_binding_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && (value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'$'))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// Filesystem path parameter: `[A-Za-z0-9._/-]` only, bounded, absolute or
/// relative, no empty, `.` or `..` segments and no trailing `/`. This is
/// deliberately stricter than POSIX: socket/dir paths are host-controlled
/// config, so anything outside the whitelist fails closed.
fn valid_path(value: &str) -> bool {
    let body = value.strip_prefix('/').unwrap_or(value);
    value.len() <= MAX_PATH_LEN
        && !body.is_empty()
        && !value.ends_with('/')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-'))
        && body
            .split('/')
            .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

pub(crate) fn is_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_name_is_injective_and_env_charset_safe() {
        // `__` separator can never appear inside an encoded half, so the
        // (workerId, bindingName) → name mapping cannot collide.
        let cases = [
            ("w-1", "TOK", "VRT_S_w_x2d1__TOK"),
            ("w_1", "$A", "VRT_S_w_x5f1___x24A"),
            ("a.b:c", "N", "VRT_S_a_x2eb_x3ac__N"),
        ];
        for (w, n, want) in cases {
            let got = secret_env_name(w, n);
            assert_eq!(got, want);
            assert!(got.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        }
        // Tricky pairs that naive `A_B` joining would collide are
        // distinct here.
        let a = secret_env_name("x", "y_z");
        let b = secret_env_name("x_y", "z");
        assert_ne!(a, b);
        assert_eq!(a, "VRT_S_x__y_x5fz");
        assert_eq!(b, "VRT_S_x_x5fy__z");
    }
}

// ------------------------------------------------------------------
// T31c: Cloudflare-compatible assets-first routing
// ------------------------------------------------------------------

/// The worker's `assets` binding carrying `assetsFirst: true`, when
/// unique. More than one opted-in assets binding is a fail-closed
/// ambiguity — Wrangler supports at most one `assets` block, and the
/// router could never tell which manifest/index to consult.
fn worker_assets_first_checked(
    worker: &WorkerDesired,
) -> Result<Option<&snapshot_protocol::compute_desired::AssetsBinding>, ConfigError> {
    let mut found = worker.bindings.iter().filter_map(|b| match b {
        BindingDesired::Assets(a) if a.assets_first_enabled() => Some(a),
        _ => None,
    });
    let first = found.next();
    if first.is_some() && found.next().is_some() {
        return Err(ConfigError::ServiceReference("bindings.assetsFirst"));
    }
    Ok(first)
}

/// Non-failing variant used by the socket/service-name sweeps — the
/// checked variant runs during emission and reports the ambiguity.
fn worker_assets_first(
    worker: &WorkerDesired,
) -> Option<&snapshot_protocol::compute_desired::AssetsBinding> {
    worker.bindings.iter().find_map(|b| match b {
        BindingDesired::Assets(a) if a.assets_first_enabled() => Some(a),
        _ => None,
    })
}

/// Router policy JSON for `VRT_ROUTER_CFG` — mirrors Miniflare's
/// router `static_routing`/`invoke_user_worker_ahead_of_assets`
/// contract (router.worker.js:248–258).
fn router_cfg_json(
    assets: &snapshot_protocol::compute_desired::AssetsBinding,
) -> Result<String, ConfigError> {
    let mut user_worker = Vec::new();
    let mut asset_worker = Vec::new();
    let mut invoke_ahead = false;
    match &assets.run_worker_first {
        Some(RunWorkerFirst::Bool(b)) => invoke_ahead = *b,
        Some(RunWorkerFirst::Rules(rules)) => {
            for rule in rules {
                if let Some(asset) = rule.strip_prefix('!') {
                    asset_worker.push(asset.to_string());
                } else {
                    user_worker.push(rule.clone());
                }
            }
        }
        None => {}
    }
    String::from_utf8(canonical_bytes(&json!({
        "invokeUserWorkerAhead": invoke_ahead,
        "userWorker": user_worker,
        "assetWorker": asset_worker,
        "manifestDigest": assets.manifest_digest,
    })))
    .map_err(|_| ConfigError::Identifier("routerConfig"))
}

/// Emit `(name = "router-<id>", worker = (…))` — a system worker that
/// sits between the ingress socket and `worker-<id>`, serving
/// asset-first requests via the facade-assets transport and falling
/// back to the user worker on a miss (T31c).
fn emit_router_worker(
    out: &mut String,
    worker: &WorkerDesired,
    assets: &snapshot_protocol::compute_desired::AssetsBinding,
    input: &TenantRenderInput<'_>,
    embeds: &mut Vec<EmbedRef>,
) -> Result<(), ConfigError> {
    // Fail-closed ambiguity check (two opted-in assets bindings).
    let checked = worker_assets_first_checked(worker)?;
    debug_assert!(checked.map(|a| a.name.as_str()) == Some(assets.name.as_str()));
    let system = input
        .params
        .system_workers
        .iter()
        .find(|s| s.service_name == ROUTER_SYSTEM_SERVICE)
        .ok_or(ConfigError::ServiceReference("facades.router"))?;
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n",
        capnp_text(&router_service_name(&worker.worker_id)),
        capnp_text(&input.params.system_compat_date),
    ));
    let router_flags = crate::compat_flags::flags_for_compat_date(
        input.params.system_compat_flags.iter().map(|f| f.as_str()),
        &input.params.system_compat_date,
    );
    if !router_flags.is_empty() {
        out.push_str(&format!(
            "      compatibilityFlags = [{}],\n",
            router_flags
                .iter()
                .map(|f| capnp_text(f))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let mut modules: Vec<&SystemModuleRef> = system.modules.iter().collect();
    modules.sort_by(|a, b| utf16_cmp(&a.name, &b.name));
    let main = system.main_module.as_str();
    modules.sort_by_key(|m| if m.name == main { 0 } else { 1 });
    out.push_str("      modules = [\n");
    for module in modules {
        let path = format!("{SYS_DIR_PREFIX}/{}", module.digest);
        embeds.push(EmbedRef {
            path: path.clone(),
            digest: module.digest.clone(),
            kind: EmbedKind::SystemModule {
                // The manifest entry that owns these module bytes —
                // `params.system_workers` lookup during assembly keys on
                // this name; `router-<id>` exists only in capnp output.
                service_name: ROUTER_SYSTEM_SERVICE.to_string(),
                module_name: module.name.clone(),
            },
        });
        out.push_str(&format!(
            "        (name = {}, {} = embed {}),\n",
            capnp_text(&module.name),
            system_module_capnp_field(&module.name),
            capnp_text(&path),
        ));
    }
    out.push_str("      ],\n");
    out.push_str("      bindings = [\n");
    // The user worker the router falls back to.
    out.push_str(&format!(
        "        (name = {}, service = (name = {})),\n",
        capnp_text(ROUTER_USER_WORKER_BINDING),
        capnp_text(&worker_service_name(&worker.worker_id)),
    ));
    // The declared assets binding — identical facade endpoint + props
    // the user worker itself would get (same authorized identity).
    emit_facade_binding(
        out,
        ROUTER_ASSETS_BINDING,
        &input.params.facades.assets,
        &assets.manifest_digest,
        worker,
        input,
        &assets.permissions,
        &assets.name,
    )?;
    out.push_str(&format!(
        "        (name = {}, text = {}),\n",
        capnp_text(ROUTER_CFG_BINDING),
        capnp_text(&router_cfg_json(assets)?),
    ));
    out.push_str("      ],\n");
    // Routers only talk to BINDING_BACKEND via the assets facade and
    // the user worker service — deny-all outbound like every other
    // non-egress system worker.
    out.push_str(&format!(
        "      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}

/// Emit `(name = "facade-qconsumer-<id>", worker = (…))` — a system
/// worker that receives dispatch batches from vrt-host
/// `/v1/queue/dispatch` over `sock-qconsumer-<id>` and invokes the
/// consumer's `queue(batch)` handler via the `CONSUMER` service
/// binding (`Fetcher.queue()` RPC — the only workerd-internal way to
/// deliver a queue event; capnp :406 `queue @15 :ServiceDesignator`
/// covers producer bindings only, there is no consumer wire type).
fn emit_qconsumer_worker(
    out: &mut String,
    worker: &WorkerDesired,
    input: &TenantRenderInput<'_>,
    embeds: &mut Vec<EmbedRef>,
) -> Result<(), ConfigError> {
    let system = input
        .params
        .system_workers
        .iter()
        .find(|s| s.service_name == QCONSUMER_SYSTEM_SERVICE)
        .ok_or(ConfigError::ServiceReference("facades.qconsumer"))?;
    out.push_str(&format!(
        "    (name = {}, worker = (\n      compatibilityDate = {},\n",
        capnp_text(&qconsumer_service_name(&worker.worker_id)),
        capnp_text(&input.params.system_compat_date),
    ));
    // T37b-r: `service_binding_extra_handlers` is MANDATORY here —
    // `Fetcher.queue()` throws "queue event not supported" without it
    // (workerd api/queue.h; miniflare sets the same flag on its queue
    // workers, miniflare index.js:107672). Emit-layer invariant, not
    // an operator choice — union into the configured flags, dedup'd.
    let mut flags: Vec<&str> = input
        .params
        .system_compat_flags
        .iter()
        .map(|f| f.as_str())
        .collect();
    if !flags.contains(&"service_binding_extra_handlers") {
        flags.push("service_binding_extra_handlers");
    }
    let flags = crate::compat_flags::flags_for_compat_date(
        flags,
        &input.params.system_compat_date,
    );
    out.push_str(&format!(
        "      compatibilityFlags = [{}],\n",
        flags
            .iter()
            .map(|f| capnp_text(f))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    let mut modules: Vec<&SystemModuleRef> = system.modules.iter().collect();
    modules.sort_by(|a, b| utf16_cmp(&a.name, &b.name));
    let main = system.main_module.as_str();
    modules.sort_by_key(|m| if m.name == main { 0 } else { 1 });
    out.push_str("      modules = [\n");
    for module in modules {
        let path = format!("{SYS_DIR_PREFIX}/{}", module.digest);
        embeds.push(EmbedRef {
            path: path.clone(),
            digest: module.digest.clone(),
            kind: EmbedKind::SystemModule {
                service_name: QCONSUMER_SYSTEM_SERVICE.to_string(),
                module_name: module.name.clone(),
            },
        });
        out.push_str(&format!(
            "        (name = {}, {} = embed {}),\n",
            capnp_text(&module.name),
            system_module_capnp_field(&module.name),
            capnp_text(&path),
        ));
    }
    out.push_str("      ],\n");
    out.push_str("      bindings = [\n");
    out.push_str(&format!(
        "        (name = {}, service = (name = {})),\n",
        capnp_text(QCONSUMER_BINDING),
        capnp_text(&worker_service_name(&worker.worker_id)),
    ));
    out.push_str("      ],\n");
    // The facade is a pure RPC bridge — it never needs binding-backend
    // or network: deny-all outbound like every non-egress system worker.
    out.push_str(&format!(
        "      globalOutbound = (name = {}, entrypoint = {}),\n    )),\n",
        capnp_text(&input.params.deny_outbound.service),
        capnp_text(&input.params.deny_outbound.entrypoint),
    ));
    Ok(())
}
