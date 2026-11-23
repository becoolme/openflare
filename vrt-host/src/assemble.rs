//! Assembly: `compute.desired` tenant fragment + version bundles +
//! runtime params → a materialized [`TenantLaunchSpec`].
//!
//! One generation = one `gen-<n>` directory under
//! `work_root/<seg(tenant)>/` holding the rendered config, embedded
//! modules, and (capnp mode) the per-generation socket dir — so a version
//! switch never mutates the serving generation's files.
//!
//! Two emit modes:
//!
//! - [`EmitMode::Capnp`]: render `config.capnp` via `vrt-config`, worker
//!   sockets are `unix:<gen>/sock/<workerId>.sock`, `--external-addr` and
//!   secret `env` come from `RenderedConfig`. This is the production
//!   shape (never launched in tests).
//! - [`EmitMode::StubJson`]: write `stub-config.json`
//!   (`{"sockets": {"sock-<id>": "tcp:127.0.0.1:0"}}`) for
//!   `vrt-stub-workerd`; endpoints are loopback TCP port 0 (the sandbox
//!   forbids unix `bind()`). Dev/test only.
//!
//! `config_digest` is a semantic fingerprint of
//! `{tenant, revision-independent params}` — per-generation paths (socket
//! dir) are excluded so identical desired state never restarts a serving
//! generation.

use crate::artifact::{sha256_hex, ArtifactError, BundleCache, BundleSource};
use crate::endpoint::WorkerEndpoint;
use crate::render_inputs::{inputs_material, RenderInputsProvider};
use crate::spec::{ResourceLimits, TenantLaunchSpec};
use crate::supervisor::TenantAssembler;
use snapshot_protocol::compute_desired::TenantDesired;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use vrt_config::{
    render_tenant_config, EmbedKind, RuntimeParams, TenantRenderInput, VersionBundle,
};

/// What to emit into the generation directory.
#[derive(Clone, Debug)]
pub enum EmitMode {
    // NOTE: variant order matters for the cfg'd StubJson below.
    /// Production `config.capnp` with unix worker sockets.
    Capnp {
        /// Static render params; `socket_dir` is overridden per
        /// generation, `secrets` resolve through to `env`.
        params: RuntimeParams,
    },
    /// `vrt-stub-workerd` JSON manifest with loopback TCP sockets
    /// (dev/test only — never for production; compiled out unless the
    /// `dev-stub` feature is enabled).
    #[cfg(feature = "dev-stub")]
    StubJson,
}

/// Assembly failures — constant field names only.
#[derive(Debug, thiserror::Error)]
pub enum AssembleError {
    /// Bundle/object fetch or digest verification failed.
    #[error("artifact: {0}")]
    Artifact(#[from] ArtifactError),
    /// `config.capnp` rendering failed.
    #[error("render: {0}")]
    Render(#[from] vrt_config::ConfigError),
    /// Materialization I/O failed.
    #[error("io: {0}")]
    Io(#[source] io::Error),
    /// T17/F1: render inputs (vars/secrets) could not be loaded —
    /// typed `render-inputs-unavailable` on readyz/ACK.
    #[error("render inputs: {0}")]
    Inputs(#[from] crate::render_inputs::RenderInputsError),
    /// H5-r2: the tenant's derived isolation uid collides with another
    /// desired tenant (or the collision set was never synced) — typed
    /// `isolation-uid-collision` on readyz/ACK.
    #[error("isolation: {0}")]
    Isolation(String),
    /// T31g/G1.4: launch-time flag consistency — the rendered config
    /// demands a workerd CLI flag the host config doesn't carry.
    /// Fail closed before the generation is written.
    #[error("launch args: {0}")]
    LaunchArgs(&'static str),
}

/// The [`TenantAssembler`] implementation used by `SupervisorHost`.
pub struct Assembler {
    workerd_binary: PathBuf,
    work_root: PathBuf,
    cache: Arc<BundleCache>,
    source: Arc<dyn BundleSource>,
    mode: EmitMode,
    base_env: BTreeMap<String, String>,
    limits: ResourceLimits,
    extra_args: Vec<String>,
    /// T13/L3: keyed pepper for secret fingerprinting.
    secret_pepper: [u8; 32],
    /// W3-r2: per-process pepper for wfops facade tokens
    /// (`HMAC-SHA256(pepper, "vrt-wfop:"+uniqueKey)`). In-process only
    /// — restart rotates every rendered token; the op channel never
    /// carries token material.
    wfop_pepper: [u8; 32],
    /// T13/M2: per-tenant uid/gid isolation policy (`None` = dev mode,
    /// children run with host ids — production must configure a range).
    isolation: Option<IsolationPolicy>,
    /// H5-r2: tenants whose derived uid is claimed by ≥2 tenants in
    /// the CURRENT desired set — recomputed every converge via
    /// [`Self::sync_desired_tenants`]. `None` = never synced: with an
    /// isolation policy configured, a collision verdict that was never
    /// computed cannot be trusted — every tenant fails closed.
    collided_tenants: std::sync::Mutex<Option<std::collections::BTreeSet<String>>>,
    /// T17/F1: hot render inputs (vars + secrets), re-read per
    /// converge — never a startup snapshot.
    inputs: Arc<dyn RenderInputsProvider>,
    /// T21: assets materializer — absent means ASSETS bindings fail
    /// closed at assemble (undeclared capability is never guessed).
    asset_sync: Option<Arc<crate::asset_sync::AssetSync>>,
    /// P3c-1: DO storage root (`doStorageDir` — a vrt-data volume
    /// path). `None` ⇒ tenants declaring `durableObjectClasses` fail
    /// closed at assemble (undeclared capability, never guessed).
    do_storage_root: Option<PathBuf>,
    /// W6: control-listen address (`127.0.0.1:port`) — forwarded to
    /// `params.wf_delete_addr` so the per-worker loopback shim can
    /// reach `wf_delete::router` via the `vrt-wfdel` external service.
    /// `None` = no control listener → shim degrades to explicit 501.
    wf_delete_addr: Option<String>,
    /// Worker-log hub + control address: tail workers are rendered
    /// only when BOTH are set (fail closed otherwise).
    log_hub: Option<Arc<crate::logs::LogHub>>,
    /// C1: unix-socket root for the per-tenant Cache API service —
    /// `<root>/<seg(tenant)>.sock` drives BOTH the emitted
    /// `params.cache_addr` (`--external-addr`) and the supervisor's
    /// listener bind (`cache::tenant_socket_path`, single
    /// derivation). `None` ⇒ no `cacheApiOutbound` → `caches.*`
    /// throws `No Cache was configured` (explicit, fail closed).
    cache_root: Option<PathBuf>,
    /// AE: unix-socket root for per-tenant Analytics Engine ingest —
    /// `<root>/<seg(tenant)>.sock` drives `params.ae_addr` and the
    /// supervisor's listener bind (`ae::tenant_socket_path`, single
    /// derivation). `None` ⇒ `analyticsEngine` bindings fail the
    /// render closed (`aeAddr`).
    ae_root: Option<PathBuf>,
    /// H5-r1: provider listener directory — the binding-backend
    /// `--external-addr` is derived PER TENANT as
    /// `vrt_securefs::socket_path(dir, tenant)` (byte-identical to the
    /// socket the provider binds and scopes tenant identity by). A
    /// verbatim single socket would route every tenant through ONE
    /// tenant's listener — cross-tenant authorization. `None` keeps
    /// the verbatim `params.binding_backend_socket` (standalone
    /// single-tenant contract only).
    binding_backend_dir: Option<PathBuf>,
    /// Per-worker quotas capability (`render.quotas`): when false, a
    /// worker declaring an enforceable limit fails the assemble
    /// (`CONFIG_QUOTA_UNSUPPORTED`) — the old generation keeps serving,
    /// never a silently unlimited new one. Defaults ON (the pinned
    /// workerd carries the limits patch); see [`Self::with_quotas`].
    quotas: bool,
}

/// T13/M2: deterministic per-tenant uid/gid assignment from a
/// configured range. `uid = base + fnv(tenant) % span`, gid = uid.
/// Two tenants hashing to the same uid fail closed at assemble
/// (collision), never silently sharing an identity.
#[derive(Clone, Copy, Debug)]
pub struct IsolationPolicy {
    /// First allocatable uid/gid.
    pub base: u32,
    /// Range size — the uid space is `[base, base + span)`.
    pub span: u32,
}

impl IsolationPolicy {
    /// Deterministic uid for `tenant` inside the range — delegates to
    /// `vrt_securefs::tenant_uid` so the provider's socket peer gate
    /// derives the SAME uid for the same tenant (one derivation, or
    /// the two sides silently disagree).
    fn uid_for(&self, tenant: &str) -> u32 {
        vrt_securefs::tenant_uid(self.base, self.span, tenant)
    }
}

impl Assembler {
    /// New assembler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        workerd_binary: PathBuf,
        work_root: PathBuf,
        cache: Arc<BundleCache>,
        source: Arc<dyn BundleSource>,
        mode: EmitMode,
        base_env: BTreeMap<String, String>,
        limits: ResourceLimits,
        extra_args: Vec<String>,
        inputs: Arc<dyn RenderInputsProvider>,
    ) -> Self {
        Self::new_with_assets(
            workerd_binary,
            work_root,
            cache,
            source,
            mode,
            base_env,
            limits,
            extra_args,
            inputs,
            None,
        )
    }

    /// T21: assembler with an assets materializer.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_assets(
        workerd_binary: PathBuf,
        work_root: PathBuf,
        cache: Arc<BundleCache>,
        source: Arc<dyn BundleSource>,
        mode: EmitMode,
        base_env: BTreeMap<String, String>,
        limits: ResourceLimits,
        extra_args: Vec<String>,
        inputs: Arc<dyn RenderInputsProvider>,
        asset_sync: Option<Arc<crate::asset_sync::AssetSync>>,
    ) -> Self {
        Self {
            workerd_binary,
            work_root,
            cache,
            source,
            mode,
            base_env,
            limits,
            extra_args,
            inputs,
            asset_sync,
            isolation: None,
            collided_tenants: std::sync::Mutex::new(None),
            // T13/L3: per-process keyed pepper — secret values are
            // hashed `pepper || tenant || ref || value`, so a leaked
            // digest can never be used to dictionary-check secret
            // guesses (unsalted sha256 allowed exactly that).
            secret_pepper: random_pepper(),
            wfop_pepper: random_pepper(),
            do_storage_root: None,
            wf_delete_addr: None,
            log_hub: None,
            cache_root: None,
            ae_root: None,
            binding_backend_dir: None,
            quotas: true,
        }
    }

    /// P3c-1: set the DO storage root (`doStorageDir` host config).
    /// Must be called before the first `assemble` — a DO tenant
    /// without a configured root fails closed at assemble.
    pub fn with_do_storage(mut self, root: PathBuf) -> Self {
        self.do_storage_root = Some(root);
        self
    }

    /// W6: address of this host's control listener — the `vrt-wfdel`
    /// external service targets it. Must be called before the first
    /// `assemble`; leaving it unset keeps the shim's explicit-501
    /// degraded path.
    /// C1: enable the tenant Cache API — every rendered tenant gets
    /// `cacheApiOutbound` on `<root>/<seg(tenant)>.sock` (the
    /// supervisor binds the same path via `cache::tenant_socket_path`).
    pub fn with_cache_root(mut self, root: PathBuf) -> Self {
        self.cache_root = Some(root);
        self
    }

    /// AE: enable Analytics Engine ingest — tenants declaring
    /// `analyticsEngine` bindings get per-dataset `external` services
    /// resolving to `<root>/<seg(tenant)>.sock` (the supervisor binds
    /// the same path via `ae::tenant_socket_path`). Unset ⇒ the
    /// render fails closed (`aeAddr`) for AE-carrying tenants.
    pub fn with_ae_root(mut self, root: PathBuf) -> Self {
        self.ae_root = Some(root);
        self
    }

    /// H5-r1: provider listener directory — per-tenant
    /// `binding-backend` derivation (`vrt_securefs::socket_path`,
    /// the same helper the provider binds with). See the field doc.
    pub fn with_binding_backend_dir(mut self, dir: PathBuf) -> Self {
        self.binding_backend_dir = Some(dir);
        self
    }

    /// Enable worker log collection (needs `with_wf_delete_addr` — the
    /// sink shares the control listener).
    pub fn with_log_hub(mut self, hub: Arc<crate::logs::LogHub>) -> Self {
        self.log_hub = Some(hub);
        self
    }

    pub fn with_wf_delete_addr(mut self, addr: impl Into<String>) -> Self {
        self.wf_delete_addr = Some(addr.into());
        self
    }

    /// Per-worker quotas capability (`render.quotas`): only a host whose
    /// `workerdBinary` carries the VRT limits patch may keep it on
    /// (default). See the field doc.
    pub fn with_quotas(mut self, enabled: bool) -> Self {
        self.quotas = enabled;
        self
    }

    /// T13/M2: enable per-tenant uid/gid isolation. Must be called
    /// before the first `assemble` — production startup requires it.
    pub fn with_isolation(mut self, policy: IsolationPolicy) -> Self {
        self.isolation = Some(policy);
        self
    }

    /// H5-r2: recompute the colliding-tenant set from the CURRENT
    /// desired set — called once per converge (and in tests). The
    /// verdict is order-independent and identical to the provider's:
    /// both sides run the same `vrt_securefs::colliding_tenants` over
    /// the same tenant ids. Removal frees the uid automatically —
    /// next converge's set no longer contains the deleted tenant.
    pub fn sync_desired_tenants(&self, tenant_ids: &[String]) {
        let Some(policy) = self.isolation else {
            return;
        };
        let collided = vrt_securefs::colliding_tenants(policy.base, policy.span, tenant_ids);
        if !collided.is_empty() {
            crate::metrics::registry().inc(
                "vrt_host_isolation_uid_collisions",
                &[],
                collided.len() as f64,
            );
            let mut by_uid: std::collections::BTreeMap<u32, Vec<&String>> =
                std::collections::BTreeMap::new();
            for t in &collided {
                by_uid
                    .entry(policy.uid_for(t))
                    .or_default()
                    .push(t);
            }
            for (uid, group) in &by_uid {
                tracing::warn!(
                    uid,
                    tenants = ?group,
                    "isolation uid collision — refusing ALL tenants in the group (order-independent)"
                );
            }
        }
        if let Ok(mut slot) = self.collided_tenants.lock() {
            *slot = Some(collided);
        }
    }

    /// Resolve the per-tenant uid for `tenant`; fails closed when the
    /// tenant's uid is claimed by another desired tenant (collision
    /// set is derived per converge — never first-come-first-served) or
    /// when the desired set was never synced under an isolation policy
    /// (a verdict that was never computed cannot be trusted).
    fn isolation_for(
        &self,
        tenant: &str,
    ) -> Result<Option<crate::spec::TenantIsolation>, AssembleError> {
        let Some(policy) = self.isolation else {
            return Ok(None);
        };
        let uid = policy.uid_for(tenant);
        let collided = self.collided_tenants.lock().map_err(|_| {
            AssembleError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "isolation collision set",
            ))
        })?;
        let Some(collided) = &*collided else {
            return Err(AssembleError::Isolation(format!(
                "isolation desired set never synced — refusing {tenant}"
            )));
        };
        if collided.contains(tenant) {
            return Err(AssembleError::Isolation(format!(
                "isolation uid {uid} collision: {tenant} shares uid with other desired tenants"
            )));
        }
        Ok(Some(crate::spec::TenantIsolation { uid, gid: uid }))
    }

    /// Params fingerprint = static render params + FRESH render
    /// inputs (T17/F1). The inputs are loaded here — a provider
    /// failure makes the fingerprint uncomputable and converge fails
    /// closed (`render-inputs-unavailable`), never a stale digest.
    fn fingerprint(&self) -> Result<String, AssembleError> {
        let inputs = self.inputs.load().inspect_err(|_| {
            crate::metrics::registry().inc("vrt_host_render_inputs_unavailable", &[], 1.0);
        })?;
        Ok(match &self.mode {
            #[cfg(feature = "dev-stub")]
            EmitMode::StubJson => {
                // The stub has no config text — but its injected env
                // comes from the same inputs, so the fingerprint must
                // still turn over on any var/secret change.
                let mut material = b"stub-v1".to_vec();
                inputs_material(&inputs, &self.secret_pepper, &mut material);
                sha256_hex(&material)
            }
            EmitMode::Capnp { params } => {
                let mut material = b"v1".to_vec();
                inputs_material(&inputs, &self.secret_pepper, &mut material);
                let mut system: Vec<_> = params
                    .system_workers
                    .iter()
                    .map(|s| {
                        let mut mods: Vec<_> = s
                            .modules
                            .iter()
                            .map(|m| (m.name.clone(), m.digest.clone()))
                            .collect();
                        mods.sort();
                        (
                            s.service_name.clone(),
                            s.main_module.clone(),
                            s.needs_binding_backend,
                            mods,
                        )
                    })
                    .collect();
                system.sort();
                if let Ok(encoded) = serde_json::to_vec(&(
                    &params.binding_backend_socket,
                    &params.egress.service,
                    &params.deny_outbound.service,
                    &params.egress_policy.to_policy_json().unwrap_or_default(),
                    &params.facades.kv.service,
                    &params.facades.r2.service,
                    &params.facades.assets.service,
                    &params.facades.pg.service,
                    &params.system_compat_date,
                    &params.system_compat_flags,
                    &system,
                    // C1: cache root presence flips `cacheApiOutbound`
                    // on every tenant — it must rekey the render
                    // fingerprint or a toggled config would reuse a
                    // stale generation digest.
                    &self.cache_root,
                    // AE: the AE socket root rewrites every
                    // AE-carrying tenant's `ae_addr` — same rekey
                    // rule as cache_root.
                    &self.ae_root,
                    // H5-r1: the binding-backend dir rewrites every
                    // tenant's `binding_backend_socket` — same rekey
                    // rule as cache_root.
                    &self.binding_backend_dir,
                )) {
                    material.extend_from_slice(&encoded);
                }
                sha256_hex(&material)
            }
        })
    }

    fn fetch_bundle(&self, digest: &str) -> Result<VersionBundle, AssembleError> {
        Ok(self
            .cache
            .get_or_fetch_bundle(digest, self.source.as_ref())?)
    }
}

impl TenantAssembler for Assembler {
    fn sync_desired_tenants(&self, tenant_ids: &[String]) {
        Assembler::sync_desired_tenants(self, tenant_ids)
    }

    fn params_fingerprint(&self) -> Result<String, AssembleError> {
        self.fingerprint()
    }

    fn sweep_assets(
        &self,
        keep: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    ) -> Option<vrt_provider::assets::AssetGcReport> {
        Assembler::sweep_assets(self, keep)
    }

    fn assemble(
        &self,
        tenant: &TenantDesired,
        revision: u64,
        generation: u64,
    ) -> Result<TenantLaunchSpec, AssembleError> {
        if tenant.workers.is_empty() {
            return Err(AssembleError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tenant has no workers",
            )));
        }
        let seg = vrt_securefs::segment_encode(&tenant.tenant_id);
        let gen_dir = self.work_root.join(&seg).join(format!("gen-{generation}"));
        vrt_securefs::ensure_secure_dir(&gen_dir).map_err(AssembleError::Io)?;

        let built = self.assemble_after_dir(tenant, revision, generation, &gen_dir);
        match built {
            Ok(spec) => Ok(spec),
            Err(error) => {
                // T25/L17: a failed assemble's gen dir is never
                // referenced by anything — remove it immediately so a
                // retry loop cannot pile up gen-1..gen-N garbage.
                let _ = std::fs::remove_dir_all(&gen_dir);
                Err(error)
            }
        }
    }
}

impl Assembler {
    /// Assemble body once `gen_dir` exists — kept on the inherent
    /// impl (trait impls cannot carry extra methods).
    fn assemble_after_dir(
        &self,
        tenant: &TenantDesired,
        revision: u64,
        generation: u64,
        gen_dir: &std::path::Path,
    ) -> Result<TenantLaunchSpec, AssembleError> {
        // Per-worker quotas: a cell whose workerd has no `limits`
        // enforcement must never host a tenant that declared one. The
        // check runs before any generation bytes are written, so the
        // previous generation keeps serving (fail closed).
        vrt_config::ensure_quotas_enforceable(tenant, self.quotas)?;
        // T21: ASSETS bindings must be materialized BEFORE launch —
        // manifest + every object verified into the provider-shared
        // store. Any gap (missing bytes, digest mismatch, poisoned
        // store, no sync configured) fails the assemble: the old
        // generation keeps serving (render-inputs-unavailable pattern).
        for worker in &tenant.workers {
            for binding in &worker.bindings {
                if let snapshot_protocol::compute_desired::BindingDesired::Assets(a) = binding {
                    let sync = self.asset_sync.as_ref().ok_or_else(|| {
                        AssembleError::Artifact(ArtifactError::Unavailable(
                            "assets binding declared but no asset store configured".into(),
                        ))
                    })?;
                    let ok = sync.ensure(&tenant.tenant_id, &a.manifest_digest);
                    crate::metrics::registry().inc(
                        "vrt_host_asset_sync_total",
                        &[("outcome", if ok.is_ok() { "ok" } else { "failed" })],
                        1.0,
                    );
                    ok.map_err(AssembleError::Artifact)?;
                }
            }
        }

        let mut spec = match &self.mode {
            #[cfg(feature = "dev-stub")]
            EmitMode::StubJson => self.assemble_stub(tenant, generation, gen_dir)?,
            EmitMode::Capnp { params } => {
                self.assemble_capnp(tenant, revision, params, generation, gen_dir)?
            }
        };
        // Spec digest: semantic fingerprint of the tenant fragment +
        // params + FRESH inputs, generation-path independent.
        spec.config_digest = spec_fingerprint(tenant, &self.fingerprint()?);
        Ok(spec)
    }

    fn sweep_assets(
        &self,
        keep: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    ) -> Option<vrt_provider::assets::AssetGcReport> {
        self.asset_sync.as_ref().map(|s| s.sweep(keep))
    }
}

/// T13/L3: 32 bytes of per-process keyed material from the OS RNG —
/// in-memory only, never persisted or logged.
fn random_pepper() -> [u8; 32] {
    let mut buf = [0u8; 32];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut buf))
        .is_err()
    {
        // Fallback: process-unique material (time + pid) — weaker but
        // still unknown outside the host.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id() as u128;
        for (i, b) in buf.iter_mut().enumerate() {
            *b = ((nanos ^ pid).wrapping_mul(0x9e3779b97f4a7c15) >> (i % 8)) as u8;
        }
    }
    buf
}

/// T13/M2: hand the generation dir to the tenant uid (0700) when we
/// can — i.e. running as root on unix. Non-root hosts leave the dir
/// owned by the host; the launcher then fails closed on spawn.
#[cfg(unix)]
pub(crate) fn chown_gen_dir(
    dir: &std::path::Path,
    isolation: Option<crate::spec::TenantIsolation>,
) -> Result<(), AssembleError> {
    use std::os::unix::fs::PermissionsExt;
    if isolation.is_none() {
        return Ok(());
    }
// SAFETY: geteuid(2) has no preconditions and cannot fail.
    if unsafe { libc::geteuid() } == 0 {
        chown_isolated(dir, isolation)?;
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(AssembleError::Io)
}

#[cfg(not(unix))]
pub(crate) fn chown_gen_dir(
    _dir: &std::path::Path,
    _isolation: Option<crate::spec::TenantIsolation>,
) -> Result<(), AssembleError> {
    Ok(())
}

/// MIG-13-r2: the same ownership handoff as [`chown_gen_dir`] for a
/// single file — no mode change (the caller's `atomic_write` mode
/// contract, 0600 for the secret-bearing live config, stands). The
/// prewarm live config and swap-time rewrite must be readable by the
/// tenant-uid child under isolation.
#[cfg(unix)]
pub(crate) fn chown_isolated(
    path: &std::path::Path,
    isolation: Option<crate::spec::TenantIsolation>,
) -> Result<(), AssembleError> {
    let Some(iso) = isolation else {
        return Ok(());
    };
// SAFETY: geteuid(2) has no preconditions and cannot fail.
    if unsafe { libc::geteuid() } == 0 {
        let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| AssembleError::Io(io_err("isolated path")))?;
// SAFETY: path is a valid NUL-terminated CString; uid/gid are plain integers and the result is checked.
        if unsafe { libc::chown(c.as_ptr(), iso.uid, iso.gid) } != 0 {
            return Err(AssembleError::Io(std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn chown_isolated(
    _path: &std::path::Path,
    _isolation: Option<crate::spec::TenantIsolation>,
) -> Result<(), AssembleError> {
    Ok(())
}

#[cfg(unix)]
fn io_err(msg: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, msg)
}

/// Semantic spec fingerprint `{tenant, params}` — the value stamped
/// into `TenantLaunchSpec::config_digest`. Public so the converger can
/// publish the EXPECTED fingerprint without a successful assemble
/// (T15/R2): a failed assemble must still leave `expected != serving`.
pub fn spec_fingerprint(tenant: &TenantDesired, params_fp: &str) -> String {
    let encoded = serde_json::to_vec(&(tenant, params_fp)).unwrap_or_default();
    sha256_hex(&encoded)
}

impl Assembler {
    /// Stub mode: `stub-config.json` + loopback TCP endpoints (port 0 —
    /// the stub reports the bound port via its control-fd listen event).
    #[cfg(feature = "dev-stub")]
    fn assemble_stub(
        &self,
        tenant: &TenantDesired,
        generation: u64,
        gen_dir: &std::path::Path,
    ) -> Result<TenantLaunchSpec, AssembleError> {
        // T15/B2: the stub path honors the same artifact contract as
        // capnp — every declared `artifactDigest` must be fetched and
        // verified before the generation may launch (a stub that skips
        // the fetch would let the e2e "prove" a delivery that never
        // could have produced code).
        for worker in &tenant.workers {
            self.fetch_bundle(&worker.active_version.artifact_digest)?;
        }
        let mut sockets = serde_json::Map::new();
        let mut endpoints = BTreeMap::new();
        let mut qconsumer_endpoints = BTreeMap::new();
        let mut cron_endpoints = BTreeMap::new();
        for worker in &tenant.workers {
            let socket = format!("sock-{}", worker.worker_id);
            let endpoint = WorkerEndpoint::Tcp("127.0.0.1:0".parse().expect("loopback"));
            sockets.insert(
                socket,
                serde_json::Value::String(endpoint.control_address()),
            );
            endpoints.insert(worker.worker_id.clone(), endpoint);
            // T37b: consumer dispatch endpoint — same contract surface as
            // capnp mode (`sock-qconsumer-<id>`); the stub binds it like
            // any other declared socket.
            if !worker.queue_consumers.is_empty() {
                let qsocket = format!("sock-qconsumer-{}", worker.worker_id);
                let qendpoint = WorkerEndpoint::Tcp("127.0.0.1:0".parse().expect("loopback"));
                sockets.insert(
                    qsocket,
                    serde_json::Value::String(qendpoint.control_address()),
                );
                qconsumer_endpoints.insert(worker.worker_id.clone(), qendpoint);
            }
            // CR1: cron dispatch endpoint — same contract surface as
            // capnp (`sock-cron-<id>`); the stub binds it like any
            // other declared socket.
            if !worker.crons.is_empty() {
                let csocket = format!("sock-cron-{}", worker.worker_id);
                let cendpoint = WorkerEndpoint::Tcp("127.0.0.1:0".parse().expect("loopback"));
                sockets.insert(
                    csocket,
                    serde_json::Value::String(cendpoint.control_address()),
                );
                cron_endpoints.insert(worker.worker_id.clone(), cendpoint);
            }
        }
        // T17/F1: the stub honors the SAME render-inputs contract as
        // capnp — secret bindings resolve tenant-scoped into
        // `spec.env` (env-only, identical `VRT_S_*` names), and vars
        // are injected as `VRT_VAR_<binding>` so dev/smoke tests can
        // observe them (values never enter the stub config file).
        let inputs = self.inputs.load().inspect_err(|_| {
            crate::metrics::registry().inc("vrt_host_render_inputs_unavailable", &[], 1.0);
        })?;
        let env = vrt_config::resolve_tenant_env(tenant, &inputs.vars, &inputs.secrets)
            .map_err(AssembleError::Render)?;
        let mut spec_env = std::collections::BTreeMap::new();
        for worker in &tenant.workers {
            for binding in &worker.bindings {
                if let snapshot_protocol::compute_desired::BindingDesired::Var(var) = binding {
                    if let Some(value) = inputs
                        .vars
                        .get(&tenant.tenant_id)
                        .and_then(|m| m.get(&var.var_ref))
                    {
                        spec_env.insert(
                            format!("VRT_VAR_{}", var.name),
                            vrt_config::SecretValue::new(value.clone()),
                        );
                    }
                }
            }
        }
        spec_env.extend(env);
        let config = serde_json::json!({
            "format": "vrt-stub-config/v1",
            "tenantId": tenant.tenant_id,
            "sockets": sockets,
        });
        let config_path = gen_dir.join("stub-config.json");
        vrt_securefs::atomic_write(
            &config_path,
            serde_json::to_string_pretty(&config)
                .unwrap_or_default()
                .as_bytes(),
            0o600,
        )
        .map_err(AssembleError::Io)?;
        let isolation = self.isolation_for(&tenant.tenant_id)?;
        chown_gen_dir(gen_dir, isolation)?;
        // T37b-r2: same argv derivation as the capnp path — consumer
        // generations need `--experimental` for the emitted qconsumer
        // flag (the stub launcher has no system_compat_flags channel).
        let mut extra_args = self.extra_args.clone();
        for a in vrt_config::tenant_experimental_extra_args(&[], tenant) {
            if !extra_args.iter().any(|e| e == &a) {
                extra_args.push(a);
            }
        }
        Ok(TenantLaunchSpec {
            tenant_id: tenant.tenant_id.clone(),
            binary: self.workerd_binary.clone(),
            config_path,
            config_digest: String::new(), // replaced by caller
            work_dir: gen_dir.to_path_buf(),
            external_addrs: Vec::new(),
            env: spec_env,
            base_env: self.base_env.clone(),
            worker_endpoints: endpoints,
            qconsumer_endpoints,
            wfops_endpoints: BTreeMap::new(),
            cron_endpoints,
            wfop_tokens: BTreeMap::new(),
            limits: self.limits,
            heartbeat_endpoint: None,
            waituntil_endpoint: None,
            extra_args,
            relaunch: None,
            generation,
            isolation,
            // P3c-1: the stub carries the same serialized-switch flag —
            // supervisor semantics (old gen dead before the new opens
            // the class dirs) are mode-agnostic and get test coverage
            // here without a real workerd.
            do_storage: stub_do_storage(tenant),
        })
    }

    /// Capnp mode: render via `vrt-config`, materialize every embed,
    /// unix sockets per worker under the generation dir.
    fn assemble_capnp(
        &self,
        tenant: &TenantDesired,
        revision: u64,
        params: &RuntimeParams,
        generation: u64,
        gen_dir: &std::path::Path,
    ) -> Result<TenantLaunchSpec, AssembleError> {
        let socket_dir = gen_dir.join("sock");
        vrt_securefs::ensure_secure_dir(&socket_dir).map_err(AssembleError::Io)?;

        let mut bundles = Vec::with_capacity(tenant.workers.len());
        for worker in &tenant.workers {
            bundles.push(self.fetch_bundle(&worker.active_version.artifact_digest)?);
        }

        // T17/F1: vars/secrets come from the provider's FRESH load —
        // the startup snapshot is never consulted at render time.
        let inputs = self.inputs.load().inspect_err(|_| {
            crate::metrics::registry().inc("vrt_host_render_inputs_unavailable", &[], 1.0);
        })?;
        let mut params = params.clone();
        params.vars = inputs.vars;
        // Worker logs: register this tenant's workers (the hub only
        // accepts known workers) and its secret VALUES for redaction
        // before anything is stored. Re-run on every assemble so a
        // rotated secret is masked from the next generation on (the hub
        // keeps the previous values too).
        if let (Some(hub), Some(addr)) = (&self.log_hub, &self.wf_delete_addr) {
            let ids: Vec<String> = tenant.workers.iter().map(|w| w.worker_id.clone()).collect();
            let values: Vec<String> = inputs
                .secrets
                .get(&tenant.tenant_id)
                .map(|m| m.values().map(|v| v.expose().to_string()).collect())
                .unwrap_or_default();
            hub.register_tenant(&ids, &values);
            params.log_tail = vrt_config::LogTailParams {
                sink_addr: addr.clone(),
                pepper: hub.pepper().to_string(),
            };
        }
        params.secrets = inputs.secrets;
        params.socket_dir = socket_dir
            .to_str()
            .ok_or_else(|| {
                AssembleError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "socket_dir utf8",
                ))
            })?
            .to_string();
        // P3c-1: per-tenant DO storage root — `<doRoot>/<seg(tenant)>`;
        // render joins `/<workerId>` per declaring worker. Empty when
        // the tenant declares no DO classes (or, fail closed, when no
        // root is configured — render rejects it too).
        params.do_storage_dir = self
            .do_storage_root
            .as_ref()
            .map(|r| {
                r.join(vrt_securefs::segment_encode(&tenant.tenant_id))
                    .to_string_lossy()
                    .into_owned()
            })
            .unwrap_or_default();
        // W3-r2: process pepper drives HMAC'd facade tokens — rendered
        // into `WFOP_TABLE` env + `spec.wfop_tokens`, never persisted.
        params.wfop_pepper = hex::encode(self.wfop_pepper);
        params.wf_delete_addr = self.wf_delete_addr.clone().unwrap_or_default();
        // C1: per-tenant Cache API external-addr — the SAME path the
        // actor binds via `deps.cache` (`tenant_socket_addr`). Empty ⇒
        // `cacheApiOutbound` is omitted entirely (caches.* throws —
        // never silently succeeds).
        params.cache_addr = self
            .cache_root
            .as_ref()
            .map(|root| crate::cache::tenant_socket_addr(root, &tenant.tenant_id))
            .transpose()
            .map_err(AssembleError::Io)?
            .unwrap_or_default();
        // AE: per-tenant ingest socket — the SAME path the actor
        // binds via `deps.ae` (`ae::tenant_socket_addr`). Empty ⇒
        // `analyticsEngine` bindings fail the render closed.
        params.ae_addr = self
            .ae_root
            .as_ref()
            .map(|root| crate::ae::tenant_socket_addr(root, &tenant.tenant_id))
            .transpose()
            .map_err(AssembleError::Io)?
            .unwrap_or_default();
        // H5-r1: per-tenant `binding-backend` external-addr. The
        // provider binds `<dir>/<seg(tenant)>.sock` and treats the
        // socket NAME as the tenant authority (listener-bound
        // identity — nothing in the request identifies the tenant).
        // A verbatim params socket would connect every tenant to ONE
        // tenant's listener: silent cross-tenant authorization.
        // Derived with the same `vrt_securefs::socket_path` the
        // provider binds — byte-identical, or fail closed.
        if let Some(dir) = &self.binding_backend_dir {
            let path = vrt_securefs::socket_path(dir, &tenant.tenant_id);
            vrt_securefs::check_socket_path_len(&path).map_err(AssembleError::Io)?;
            params.binding_backend_socket = path
                .to_str()
                .ok_or_else(|| {
                    AssembleError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "binding_backend_socket utf8",
                    ))
                })?
                .to_string();
        }
        // PT1: the watchdog heartbeat socket lives in the SAME private
        // socket dir (F1 perms) — emitted iff the runaway watchdog is
        // configured, and reachable only by the host's own probe.
        let heartbeat_path = vrt_securefs::socket_path(&socket_dir, "vrt-heartbeat");
        params.heartbeat_socket = if self.limits.runaway.is_some() {
            heartbeat_path
                .to_str()
                .ok_or_else(|| {
                    AssembleError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "heartbeat_socket utf8",
                    ))
                })?
                .to_string()
        } else {
            String::new()
        };

        // MIG-12: the waitUntil probe socket is emitted for EVERY
        // capnp generation (platform plumbing, like the per-worker
        // sockets) — same private socket_dir, never on ingress.
        let waituntil_path =
            vrt_securefs::socket_path(&socket_dir, vrt_config::waituntil::PROBE_SOCKET_NAME);
        vrt_securefs::check_socket_path_len(&waituntil_path).map_err(AssembleError::Io)?;
        // MIG-13-r2/r4: the `vrt-switch` marker lives in the SAME
        // private dir as the probe under a file name never longer
        // than the probe's (const-asserted in capnp.rs) — the same
        // preflight bounds it (its failure mode is the very bind
        // ENAMETOOLONG this check exists for).
        vrt_securefs::check_socket_path_len(
            &socket_dir.join(vrt_config::capnp::SWITCH_MARKER_FILE),
        )
        .map_err(AssembleError::Io)?;
        params.waituntil_socket =
            waituntil_path
                .to_str()
                .ok_or_else(|| {
                    AssembleError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "waituntil_socket utf8",
                    ))
                })?
                .to_string();

        let rendered = render_tenant_config(&TenantRenderInput {
            tenant,
            revision,
            bundles: &bundles,
            params: &params,
        })?;

        // T31g/G1.4: workerd refuses the `experimental` compat flag
        // unless `--experimental` lands in argv. When render params
        // carry the flag but the host config lacks the arg, EVERY
        // tenant launch would die at startup — fail closed here,
        // before the generation is materialized.
        if params
            .system_compat_flags
            .iter()
            .any(|f| f == "experimental")
            && !self.extra_args.iter().any(|a| a == "--experimental")
        {
            return Err(AssembleError::LaunchArgs(
                "systemCompatFlags contains \"experimental\" but \
                 render.extraArgs lacks \"--experimental\" — workerd \
                 would refuse to start",
            ));
        }

        // T37b-r2: argv is DERIVED, never operator-supplied for this —
        // queue consumers force `service_binding_extra_handlers` into the
        // emitted qconsumer worker (compat-date.capnp:296 `$experimental`)
        // and workerd refuses to start without `--experimental` on argv.
        // The literal `experimental` catch-all is NOT derived (the T31g
        // gate above keeps it a deliberate opt-in). The predicate is
        // shared with vrt-local's deploy preflight
        // (`vrt_config::launch_args`) so both paths agree.
        let mut extra_args = self.extra_args.clone();
        for a in vrt_config::tenant_experimental_extra_args(&params.system_compat_flags, tenant) {
            if !extra_args.iter().any(|e| e == &a) {
                extra_args.push(a);
            }
        }

        // Materialize every embed — content re-verified against the
        // render-declared digest on write.
        for embed in &rendered.embeds {
            let bytes = match &embed.kind {
                EmbedKind::BundleModule {
                    worker_id,
                    module_name,
                } => {
                    let worker_idx = tenant
                        .workers
                        .iter()
                        .position(|w| &w.worker_id == worker_id)
                        .ok_or(AssembleError::Artifact(ArtifactError::Missing))?;
                    let module = bundles[worker_idx]
                        .manifest()
                        .modules
                        .iter()
                        .find(|m| m.name() == module_name.as_str())
                        .ok_or(AssembleError::Artifact(ArtifactError::Missing))?;
                    module
                        .content_bytes()
                        .map_err(|_| AssembleError::Artifact(ArtifactError::BundleInvalid))?
                }
                EmbedKind::SystemModule {
                    service_name,
                    module_name,
                } => {
                    let mref = params
                        .system_workers
                        .iter()
                        .find(|s| &s.service_name == service_name)
                        .and_then(|s| s.modules.iter().find(|m| &m.name == module_name))
                        .ok_or(AssembleError::Artifact(ArtifactError::Missing))?;
                    self.cache
                        .get_or_fetch_raw(&mref.digest, self.source.as_ref())?
                }
                EmbedKind::VendoredModule { name } => vrt_config::workflows::vendored_bytes(name)
                    .ok_or(AssembleError::Artifact(ArtifactError::Missing))?
                    .to_vec(),
            };
            if sha256_hex(&bytes) != embed.digest {
                return Err(AssembleError::Artifact(ArtifactError::DigestMismatch));
            }
            // Embeds must stay inside the generation dir — reject
            // absolute paths and parent components before joining.
            let rel = std::path::Path::new(&embed.path);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return Err(AssembleError::Artifact(ArtifactError::BundleInvalid));
            }
            let path = gen_dir.join(rel);
            if let Some(parent) = path.parent() {
                vrt_securefs::ensure_secure_dir(parent).map_err(AssembleError::Io)?;
            }
            vrt_securefs::atomic_write(&path, &bytes, 0o600).map_err(AssembleError::Io)?;
        }

        let config_path = gen_dir.join("config.capnp");
        vrt_securefs::atomic_write(&config_path, rendered.text.as_bytes(), 0o600)
            .map_err(AssembleError::Io)?;

        // W3-r2: workers excluded by the render dry-run get NO endpoint
        // — their routes answer `worker_unavailable` while siblings
        // serve (one bad worker must not poison the shared workerd).
        if !rendered.skipped_workers.is_empty() {
            tracing::warn!(
                tenant = %tenant.tenant_id,
                skipped = ?rendered.skipped_workers,
                "workers excluded from render; routes will answer worker_unavailable"
            );
            // W4-r2 P1: per-worker typed reason — the flat id list
            // alone forced an operator to reproduce the render to
            // learn WHY a worker was dropped.
            for r in &rendered.skipped_reasons {
                tracing::warn!(
                    tenant = %tenant.tenant_id,
                    worker = %r.worker_id,
                    code = r.code,
                    detail = %r.detail,
                    "worker excluded from render (dry-run failure)"
                );
            }
        }
        let mut endpoints = BTreeMap::new();
        let mut qconsumer_endpoints = BTreeMap::new();
        let mut wfops_endpoints = BTreeMap::new();
        let mut cron_endpoints = BTreeMap::new();
        for worker in &tenant.workers {
            if rendered.skipped_workers.contains(&worker.worker_id) {
                continue;
            }
            endpoints.insert(
                worker.worker_id.clone(),
                WorkerEndpoint::Unix(vrt_securefs::socket_path(&socket_dir, &worker.worker_id)),
            );
            // T37b: `sock-qconsumer-<id>` — capnp binds
            // `<id>-qconsumer.sock`; `-` survives segment_encode.
            if !worker.queue_consumers.is_empty() {
                qconsumer_endpoints.insert(
                    worker.worker_id.clone(),
                    WorkerEndpoint::Unix(vrt_securefs::socket_path(
                        &socket_dir,
                        &format!("{}-qconsumer", worker.worker_id),
                    )),
                );
            }
            // W3: `sock-wfops-<id>` — capnp binds `<id>-wfops.sock`.
            if !worker.workflows.is_empty() {
                wfops_endpoints.insert(
                    worker.worker_id.clone(),
                    WorkerEndpoint::Unix(vrt_securefs::socket_path(
                        &socket_dir,
                        &format!("{}-wfops", worker.worker_id),
                    )),
                );
            }
            // CR1: `sock-cron-<id>` — capnp binds `<id>-cron.sock`.
            if !worker.crons.is_empty() {
                cron_endpoints.insert(
                    worker.worker_id.clone(),
                    WorkerEndpoint::Unix(vrt_securefs::socket_path(
                        &socket_dir,
                        &format!("{}-cron", worker.worker_id),
                    )),
                );
            }
        }

        let isolation = self.isolation_for(&tenant.tenant_id)?;
        chown_gen_dir(gen_dir, isolation)?;
        // P3c-1: DO storage dirs are created BEFORE launch so the
        // emitted `disk` services open existing, tenant-uid-owned dirs
        // (workerd runs as the tenant uid and would otherwise create
        // them itself — we still pre-create for permissions + sweep).
        let do_storage = self.prepare_do_storage(tenant, isolation)?;
        Ok(TenantLaunchSpec {
            tenant_id: tenant.tenant_id.clone(),
            binary: self.workerd_binary.clone(),
            config_path,
            config_digest: String::new(), // replaced by caller
            work_dir: gen_dir.to_path_buf(),
            external_addrs: rendered.external_addrs,
            env: rendered.env,
            base_env: self.base_env.clone(),
            worker_endpoints: endpoints,
            qconsumer_endpoints,
            wfops_endpoints,
            cron_endpoints,
            wfop_tokens: rendered.wfop_tokens,
            limits: self.limits,
            heartbeat_endpoint: self
                .limits
                .runaway
                .map(|_| WorkerEndpoint::Unix(heartbeat_path.clone())),
            waituntil_endpoint: Some(WorkerEndpoint::Unix(waituntil_path)),
            extra_args: extra_args.clone(),
            relaunch: None,
            generation,
            isolation,
            do_storage,
        })
    }

    /// P3c-1: ensure `<doRoot>/<seg(tenant)>` + `<tenantDir>/<workerId>`
    /// exist (0700, tenant uid under isolation) and fill
    /// `params.do_storage_dir`. Returns the spec fragment. `None`
    /// when no worker declares DO classes; a DO tenant without a
    /// configured `doStorageDir` fails closed here (before render).
    fn prepare_do_storage(
        &self,
        tenant: &TenantDesired,
        isolation: Option<crate::spec::TenantIsolation>,
    ) -> Result<Option<crate::spec::DoStorageSpec>, AssembleError> {
        let declaring: Vec<&snapshot_protocol::compute_desired::WorkerDesired> = tenant
            .workers
            .iter()
            .filter(|w| !w.durable_object_classes.is_empty() || !w.workflows.is_empty())
            .collect();
        if declaring.is_empty() {
            return Ok(None);
        }
        let root = self.do_storage_root.clone().ok_or_else(|| {
            AssembleError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "doStorageDir not configured but tenant declares durable objects or workflows",
            ))
        })?;
        let tenant_dir = root.join(vrt_securefs::segment_encode(&tenant.tenant_id));
        let mut worker_classes = std::collections::BTreeMap::new();
        let mut transfer_sources = std::collections::BTreeMap::new();
        for worker in declaring {
            // workerId is validated FS-safe in render (`workerId.doPath`
            // — not "." / ".." / containing `/`) before this runs.
            let worker_dir = tenant_dir.join(&worker.worker_id);
            let created = !worker_dir.exists();
            vrt_securefs::ensure_secure_dir(&worker_dir).map_err(AssembleError::Io)?;
            chown_gen_dir(&worker_dir, isolation)?;
            if created {
                tracing::info!(
                    tenant = tenant.tenant_id,
                    worker = worker.worker_id,
                    "do storage dir created"
                );
            }
            worker_classes.insert(worker.worker_id.clone(), worker_class_keys(worker));
            for c in &worker.durable_object_classes {
                if let Some(src) = &c.from_worker_id {
                    transfer_sources.insert(
                        (worker.worker_id.clone(), c.unique_key.clone()),
                        src.clone(),
                    );
                }
            }
        }
        Ok(Some(crate::spec::DoStorageSpec {
            tenant_dir,
            worker_classes,
            transfer_sources,
        }))
    }
}

/// W1-r4: EVERY DO namespace a worker declares — user classes AND
/// system-generated workflow Engine namespaces — as one class-key
/// list. This is the single typed source the whole storage layer
/// keys on: the localDisk class dir, the placement `PlacementScope`,
/// fencing, snapshots, `DoStorageView` resolution, storage stats and
/// the stale-sweep keep-set all consume the map built from this, so
/// no consumer can drift into its own subset. Engine namespace
/// `uniqueKey`s are emitted by capnp.rs verbatim from
/// `workflows[].unique_key`, so the dir name and the key agree.
fn worker_class_keys(worker: &snapshot_protocol::compute_desired::WorkerDesired) -> Vec<String> {
    worker
        .durable_object_classes
        .iter()
        .map(|c| c.unique_key.clone())
        .chain(worker.workflows.iter().map(|w| w.unique_key.clone()))
        .collect()
}

/// T37b-r2: the `--experimental` argv fragment a generation needs, or
/// empty — the SAME predicate the assembler applies to
/// `spec.extra_args`. Queue consumers imply the emitted qconsumer
/// worker carries `service_binding_extra_handlers`
/// (`compatibility-date.capnp:295-296` `$experimental`), which workerd
/// refuses without `--experimental` on argv; configured experimental
/// SYSTEM flags are operator intent and likewise derive the argv. The
/// literal `experimental` catch-all is excluded — it stays a deliberate
/// opt-in gated by `AssembleError::LaunchArgs` / config validation.
///
/// Test launchers MUST derive argv through this helper (never write
/// `"--experimental"` by hand) so smoke coverage exercises the same
/// derivation as production.
/// P3c-1: stub-mode [`crate::spec::DoStorageSpec`] — no real disk
/// services exist, but the serialized-switch decision and the class
/// set are identical to capnp mode so the supervisor path gets real
/// coverage. `tenant_dir` points under the stub's work root (never
/// touched by workerd itself).
#[cfg(feature = "dev-stub")]
fn stub_do_storage(tenant: &TenantDesired) -> Option<crate::spec::DoStorageSpec> {
    let mut worker_classes = std::collections::BTreeMap::new();
    for worker in &tenant.workers {
        if !worker.durable_object_classes.is_empty() || !worker.workflows.is_empty() {
            let classes = worker_class_keys(worker);
            worker_classes.insert(worker.worker_id.clone(), classes);
        }
    }
    if worker_classes.is_empty() {
        None
    } else {
        Some(crate::spec::DoStorageSpec {
            tenant_dir: std::path::PathBuf::from("do"),
            worker_classes,
            transfer_sources: std::collections::BTreeMap::new(),
        })
    }
}

/// P3c-1: `localDisk` Durable Object storage is **EXPERIMENTAL**
/// (`workerd.capnp` ActorStorageConfig — `localDisk` is the documented
/// on-disk local-disk backend; miniflare launches workerd with
/// `--experimental` for exactly this reason). DO tenants therefore
/// derive `--experimental` the same way queue consumers do.
/// CR1: cron-carrying workers derive `--experimental` the same way —
/// their `facade-cron-<id>` service emits the `$experimental`
/// `service_binding_extra_handlers` flag (Fetcher.scheduled()).
/// AE: `analyticsEngine` bindings derive it too — workerd's
/// `experimental_feature("AnalyticsEngine")` check refuses the
/// binding without the argv flag.
///
/// The predicate itself lives in `vrt_config::launch_args` — one
/// implementation shared with `vrt-local`'s deploy preflight; launch
/// paths call [`vrt_config::tenant_experimental_extra_args`], which
/// derives the needs from the tenant so no caller can drift.
pub use vrt_config::{experimental_extra_args, ExperimentalNeeds};

