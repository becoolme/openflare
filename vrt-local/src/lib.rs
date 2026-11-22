//! # vrt-local — standalone-mode control plane (T16)
//!
//! One shared runtime core, two control-plane sources:
//!
//! | concern            | platform mode                | standalone mode        |
//! |--------------------|------------------------------|------------------------|
//! | desired snapshots  | control plane → controller → | `vrt-local` signs      |
//! |                    | FileEnvelopePodChannel       | `desired/envelope.json`|
//! | bundles            | control-plane HTTP route     | `objects/<digest>`     |
//! | secret values      | platform secret store        | `secrets/secrets.json` |
//! | ack / health       | controller PARTIAL/APPLIED   | `state/oplog.jsonl` +  |
//! |                    |                              | vrt-host `/readyz`     |
//!
//! Everything the host verifies (signature, revision fence, scope,
//! expiry, payload validation) is identical in both modes — this crate
//! only produces the *inputs*, never relaxes the checks.
//!
//! Layout under `<root>` (all dirs `0700`):
//! ```text
//! keys/signing.seed      Ed25519 seed (0600, owner-only)
//! keys/trusted.json      {keyId, publicKey} for vrt-host/vrt-provider (0644)
//! state/state.json       typed local state (schemaVersion 2)
//! state/oplog.jsonl      operation log — never secret values (0600)
//! state/lock             flock single-writer guard
//! desired/envelope.json  latest signed compute.desired (0644 — public doc)
//! objects/<digest>       stored bundles + system modules (0644 — content-
//!                        addressed, non-secret; dir 0700 gates access)
//! secrets/secrets.json   {tenantId:{ref:value}} render secrets (0600)
//! vars/vars.json         {varRef:value} render vars (0600)
//! config/                `of init` generated host/provider configs
//! mgmt/                  `of serve` unix socket + token (T17)
//! ```
//!
//! Write order per mutating op (crash-safe): artifacts → state →
//! signed envelope → oplog. A crash anywhere leaves either the old
//! published revision (envelope unwritten) or an orphan artifact —
//! never a half-state desired document.

mod desired;
pub mod check;
pub mod dev;
pub mod devvars;
pub mod doctor;
pub mod keys;
pub mod layout;
mod lock;
pub mod logtail;
pub mod mgmt;
mod oplog;
mod preflight;
mod project_cfg;
pub use project_cfg::{
    load_worker_meta, load_worker_meta_for_env, MetaSource, WRANGLER_CANDIDATES,
};
pub use worker_pkg::WorkerToml;
pub mod serve;
mod state;
pub mod templates;
mod wf_scan;
pub mod worker_pkg;

pub use worker_pkg::{
    derive_do_unique_key, pack, pack_with_ctx, pack_with_resolver, DeployOverrides, DoPackCtx,
    PackedWorker,
};

use snapshot_protocol::compute_desired::BindingDesired;
use snapshot_protocol::compute_desired::ComputeDesired;
use snapshot_protocol::{sign, SignParams, SigningKey, TrustedKey};
use std::path::{Path, PathBuf};

/// Standalone runtime root handle. `open` validates the layout;
/// `init` creates it.
pub struct VrtLocal {
    root: PathBuf,
}

/// `init` parameters.
pub struct InitParams {
    /// Snapshot cell scope (e.g. `"local"`). `compute.desired.cellId`
    /// and the envelope `cell:<id>` scope both bind to it.
    pub cell_id: String,
    /// The single tenant all standalone workers belong to.
    pub tenant_id: String,
    /// Optional facade `dist/` directory: every system module listed in
    /// `facade/manifest.json` is seeded into `objects/<sha256>` so the
    /// host's `DirBundleSource` can serve capnp `sys/<digest>` embeds.
    pub facade_manifest: Option<PathBuf>,
    pub facade_dist_root: Option<PathBuf>,
    /// F4: retained versions per worker before oldest-non-active
    /// eviction (clamped `1..=256`; default 64).
    pub max_versions_per_worker: Option<u32>,
}

/// Typed failures — variant names are stable, payloads are field ids.
#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    /// I/O on the layout.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Layout/key material fails the fail-closed checks (permissions,
    /// ownership, presence).
    #[error("layout: {0}")]
    Layout(&'static str),
    /// Named filesystem operation failed; the payload carries the
    /// affected path (relative to the init root where practical) —
    /// never secret values (T25/L2).
    #[error("{0}: {1}")]
    Op(&'static str, String),
    /// Another writer holds the lock (or the lock cannot be taken).
    #[error("locked")]
    Locked,
    /// `state.json` failed typed decode or semantic validation.
    #[error("state: {0}")]
    State(&'static str),
    /// Worker package metadata or bundle build failed.
    #[error("worker: {0}")]
    Worker(&'static str),
    /// Worker error carrying a built diagnostic message (parse spans,
    /// redacted toml details) — owned so nothing has to be leaked to
    /// satisfy `'static`.
    #[error("worker: {0}")]
    WorkerOwned(String),
    /// Bundle validation/build error (vrt-config typed error).
    #[error("bundle: {0}")]
    Bundle(#[from] vrt_config::BundleError),
    /// Snapshot protocol error (sign/verify/key).
    #[error("protocol: {0}")]
    Protocol(#[from] snapshot_protocol::SnapshotError),
    /// `compute.desired` semantic validation rejected the built doc.
    #[error("desired: {0}")]
    Desired(&'static str),
    /// Referenced object does not exist.
    #[error("not found: {0}")]
    NotFound(&'static str),
    /// Operation targets a tenant other than the local one.
    #[error("wrong tenant")]
    WrongTenant,
}

/// Outcome of `deploy`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployResult {
    /// Immutable version id (`vrt_<digest-prefix>`).
    pub version_id: String,
    /// Content-addressed bundle digest.
    pub artifact_digest: String,
    /// Published envelope revision.
    pub revision: u64,
}

/// Read-only status view (`of status` backing call).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalStatus {
    /// Cell/tenant ids this root serves.
    pub cell_id: String,
    /// Single tenant id.
    pub tenant_id: String,
    /// State revision (== last signed envelope revision when published).
    pub revision: u64,
    /// Envelope revision on disk, if any (crash window: < `revision`).
    pub published_revision: Option<u64>,
    /// Workers with active version + routes.
    pub workers: Vec<WorkerStatus>,
    /// Secret refs stored for the local tenant (names only).
    pub secret_refs: Vec<String>,
}

/// One worker's status row.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatus {
    /// Worker name (vrt.toml `name`).
    pub name: String,
    /// Stable worker id assigned at first deploy.
    pub worker_id: String,
    /// Active version id, if deployed.
    pub active_version_id: Option<String>,
    /// Known version ids (newest last).
    pub versions: Vec<String>,
    /// Routed hostnames.
    pub hostnames: Vec<String>,
    /// Declared per-worker quotas (platform defaults when the worker
    /// never declared `limits`/`[quotas]`).
    pub quotas: snapshot_protocol::compute_desired::WorkerQuotas,
    /// True only when the host renders/enforces `limits` for this worker
    /// (`render.quotas` on AND at least one enforceable dimension was
    /// declared). A declared limit on a quota-incapable host fails the
    /// deploy, so `false` here always means "nothing to enforce", never
    /// "declared but silently ignored".
    pub quotas_enforced: bool,
}

/// Report from `init`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitReport {
    /// Verifier record to paste into vrt-host/vrt-provider `trustedKeys`.
    pub trusted_key: TrustedKey,
}

/// Envelope lifetime — the protocol maximum (24 h). `renew` (or any
/// mutating op) must run before expiry; `of serve` (T17) renews on a
/// timer. Kept in sync with `envelope::MAX_TTL` — a compile-time
/// assertion lives in the crate tests.
const ENVELOPE_TTL: chrono::Duration = chrono::Duration::seconds(24 * 3600);

impl VrtLocal {
    /// Create a fresh standalone root: secure layout, Ed25519 signing
    /// key, empty state. Fails closed when `root` is non-empty (never
    /// clobber an existing deployment silently).
    pub fn init(root: &Path, params: &InitParams) -> Result<InitReport, LocalError> {
        // Identifier charset mirrors `compute_desired`'s
        // `valid_identifier` (re-validated at publish via
        // `ComputeDesired::validate` — this is the early gate).
        let ident = |v: &str| {
            !v.is_empty()
                && v.len() <= 128
                && v.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
        };
        if !ident(&params.cell_id) || !ident(&params.tenant_id) {
            return Err(LocalError::Layout("identity"));
        }
        // T25/L1: staging lives INSIDE root (`root/.init-tmp-<pid>`)
        // and commit renames entries in — root itself is never removed
        // or renamed, so a mount point (compose named volume) works.
        // Commit point = the `.vrt-init` marker, written last.
        layout::check_init_preconditions(root)?;
        let staging = layout::init_begin(root)?;
        let result = (|| {
            let _lock = lock::StateLock::acquire(&staging)?;
            let mut seed = [0u8; 32];
            ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut seed)
                .map_err(|_| LocalError::Layout("csprng"))?;
            let key = SigningKey::from_seed(&seed)?;
            keys::write_signing_key(&staging, &seed)?;
            let trusted = key.trusted_key();
            keys::write_trusted_key(&staging, &trusted)?;
            // T17: management-API bearer token — random, owner-only.
            keys::write_mgmt_token(&staging)?;
            // T25/L8: provider cursor secret — random file (0600),
            // never env plaintext. Generated at init so the provider
            // config produced by `of init` is self-contained.
            keys::write_cursor_secret(&staging)?;
            keys::write_sql_secrets(&staging)?;
            let mut st = state::LocalState::new(&params.cell_id, &params.tenant_id);
            if let Some(max) = params.max_versions_per_worker {
                if max == 0 || max > state::MAX_VERSIONS_CEILING {
                    return Err(LocalError::Layout("maxVersionsPerWorker"));
                }
                st.max_versions_per_worker = max;
            }
            if let (Some(manifest), Some(dist)) =
                (&params.facade_manifest, &params.facade_dist_root)
            {
                // T25/L4: seeds objects/<sha256>, the dist tree and the
                // manifest INTO the root (<root>/facade/{dist,manifest.json});
                // the host config references <root>/facade/manifest.json
                // and must not depend on the external --facade-dist mount.
                st.system_objects = seed_system_modules(&staging, manifest, dist)?;
            }
            // T25/L9: publish a valid signed EMPTY envelope (revision 1,
            // zero workers) so host/provider start healthy before the
            // first deploy. The tenant exists ⇒ provider binds its
            // listener; zero workers ⇒ host launches nothing.
            st.revision = 1;
            state::write(&staging, &st)?;
            publish_at(&staging, &st, &key)?;
            state::write_secrets(&staging, &Default::default())?;
            state::write_vars(&staging, &Default::default())?;
            oplog::append(
                &staging,
                &oplog::entry(
                    "init",
                    serde_json::json!({
                        "cellId": params.cell_id, "tenantId": params.tenant_id,
                    }),
                ),
            )?;
            Ok::<_, LocalError>(trusted)
        })();
        match result {
            Ok(trusted) => {
                layout::init_commit(root, &staging)?;
                Ok(InitReport {
                    trusted_key: trusted,
                })
            }
            Err(e) => {
                let _ = std::fs::remove_dir_all(&staging);
                Err(e)
            }
        }
    }

    /// Open an existing root — layout + key permissions are rechecked
    /// every time (fail closed on `chmod` regressions).
    pub fn open(root: &Path) -> Result<Self, LocalError> {
        layout::check(root)?;
        keys::load_signing_key(root)?; // permission + parse check
        state::read(root)?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// Build a version bundle from `worker_dir` (vrt.toml + modules),
    /// publish it as the worker's active version, bump revision, sign.
    pub fn deploy(
        &self,
        worker_dir: &Path,
        overrides: &worker_pkg::DeployOverrides,
    ) -> Result<DeployResult, LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let st_probe = state::read(&self.root)?;
        // P3c-1: the HMAC pepper for DO uniqueKey = signing seed (deploy
        // key); the rename lineage source = the class set of the worker's currently
        // active version (empty on first deploy -- `renamed_classes`/`deleted_
        // classes` pointing at a nonexistent class -> fail closed). The worker name
        // is first resolved through the same meta load path (consistent with the decision inside pack).
        let do_ctx = {
            let pepper = keys::load_signing_seed(&self.root)?;
            let (meta, _src) = crate::project_cfg::load_worker_meta_for_env(
                worker_dir,
                overrides.env_name.as_deref(),
            )?;
            let name = overrides.name.clone().unwrap_or(meta.name);
            let prior = st_probe
                .workers
                .get(&name)
                .and_then(|w| {
                    w.active_version_id
                        .as_ref()
                        .and_then(|id| w.versions.iter().find(|v| &v.version_id == id))
                })
                .map(|v| v.durable_object_classes.as_slice())
                .unwrap_or(&[]);
            worker_pkg::DoPackCtx {
                pepper,
                prior_classes: prior,
            }
        };
        let pkg = worker_pkg::pack_with_ctx(
            worker_dir,
            overrides,
            st_probe.tenant_id(),
            &|target| st_probe.workers.get(target).map(|w| w.worker_id.clone()),
            Some(&do_ctx),
        )?;
        self.commit_packaged(pkg, &key)
    }

    /// Deploy a remote-upload body (`of deploy --url`): the client
    /// gathered meta + module bytes + assets; everything trust-bearing
    /// happens here — secrets land in the tenant store, service
    /// targets resolve against live state, DO/workflow unique keys
    /// derive from the signing seed, and the result goes through the
    /// SAME validate → preflight → sign path as a local deploy.
    pub fn deploy_upload(
        &self,
        upload: &worker_pkg::DeployUpload,
    ) -> Result<DeployResult, LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        // Secrets first — same ordering rule as `put_secret`: inputs
        // are written before state/envelope so a crash never leaves a
        // live bundle pointing at a missing ref. Refs are
        // shape-checked; secret VALUES never enter errors or logs.
        let mut secrets = state::read_secrets(&self.root)?;
        let mut secrets_dirty = false;
        let st_probe = state::read(&self.root)?;
        for (sref, value) in &upload.secrets {
            if !valid_secret_ref(sref) {
                return Err(LocalError::Worker("secretRef"));
            }
            secrets
                .entry(st_probe.tenant_id().to_string())
                .or_default()
                .insert(sref.clone(), value.clone());
            secrets_dirty = true;
        }
        if secrets_dirty {
            state::write_secrets(&self.root, &secrets)?;
        }
        let do_ctx = {
            let pepper = keys::load_signing_seed(&self.root)?;
            let name = upload
                .name
                .clone()
                .unwrap_or_else(|| upload.meta.name.clone());
            let prior = st_probe
                .workers
                .get(&name)
                .and_then(|w| {
                    w.active_version_id
                        .as_ref()
                        .and_then(|id| w.versions.iter().find(|v| &v.version_id == id))
                })
                .map(|v| v.durable_object_classes.as_slice())
                .unwrap_or(&[]);
            worker_pkg::DoPackCtx {
                pepper,
                prior_classes: prior,
            }
        };
        let gathered = worker_pkg::gathered_from_upload(upload)?;
        let pkg = worker_pkg::assemble(
            gathered,
            &worker_pkg::DeployOverrides {
                name: upload.name.clone(),
                vars: upload.vars.clone(),
                tag: upload.tag.clone(),
                ..Default::default()
            },
            st_probe.tenant_id(),
            &|target| st_probe.workers.get(target).map(|w| w.worker_id.clone()),
            Some(&do_ctx),
        )?;
        self.commit_packaged(pkg, &key)
    }

    /// The shared deploy tail — state revision, asset materialization,
    /// desired validation, workerd preflight, object/state/vars
    /// writes, signed publish, GC, oplog. `pkg` was produced by
    /// `pack`/`assemble` under the SAME lock this function assumes.
    fn commit_packaged(
        &self,
        pkg: worker_pkg::PackedWorker,
        key: &snapshot_protocol::SigningKey,
    ) -> Result<DeployResult, LocalError> {
        let mut st = state::read(&self.root)?;
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        // T21: materialize `[assets]` content into the provider-shared
        // store BEFORE publishing — verified writes only. Declared
        // `[bindings].assets` digests must already exist in the store.
        let asset_root = layout::provider_dir(&self.root).join("data").join("assets");
        let asset_store = vrt_provider::assets::AssetStore::open(&asset_root)
            .map_err(|_| LocalError::Layout("asset store"))?;
        if let Some(pending) = &pkg.pending_assets {
            for (digest, bytes) in &pending.objects {
                asset_store
                    .put_object_verified(st.tenant_id(), digest, bytes)
                    .map_err(|_| LocalError::Layout("asset object"))?;
            }
            asset_store
                .put_manifest_verified(
                    st.tenant_id(),
                    &pending.manifest_digest,
                    &pending.manifest_bytes,
                )
                .map_err(|_| LocalError::Layout("asset manifest"))?;
        }
        for binding in &pkg.bindings {
            if let BindingDesired::Assets(a) = binding {
                if asset_store
                    .manifest(st.tenant_id(), &a.manifest_digest)
                    .map_err(|_| LocalError::Layout("asset store"))?
                    .is_none()
                {
                    return Err(LocalError::Worker(
                        "assets binding references a manifest not present in the local store",
                    ));
                }
            }
        }
        // Flagship: merge this deploy's app set into the tenant store —
        // a file-provided app overwrites its same-id entry, a bound app
        // with no definitions keeps the empty config `assemble`
        // synthesized. Apps are never removed by a deploy that does
        // not name them (the store is tenant-level, like CF's local
        // flag store: updates land per app).
        if !pkg.flagship_apps.is_empty() {
            for app in pkg.flagship_apps.iter() {
                match st.flagship_apps.iter_mut().find(|a| a.app_id == app.app_id) {
                    Some(existing) => *existing = app.clone(),
                    None => st.flagship_apps.push(app.clone()),
                }
            }
            st.flagship_apps.sort_by(|a, b| a.app_id.cmp(&b.app_id));
        }
        let (version_id, digest) = st.apply_deploy(&pkg).map_err(LocalError::Worker)?;
        // F3: NOTHING is persisted before the desired doc validates —
        // a rejected state must leave every file byte-identical.
        Self::validate_desired(&st)?;
        // W1-r2/P0-2: prove the post-deploy config actually LOADS in
        // workerd before anything is published — a config that cannot
        // load fails the deploy here (explicit error, old version
        // untouched), instead of taking the whole tenant down at the
        // next apply. Skips on stub/non-standalone roots.
        preflight::deploy_preflight(&self.root, &st, Some((&pkg.digest, &pkg.stored_document)))?;
        // 1. artifact(s) first — orphan objects are harmless.
        let object_path = layout::objects_dir(&self.root).join(&pkg.digest);
        vrt_securefs::atomic_write(&object_path, &pkg.stored_document, 0o644)?;
        // vars.json = union of ALL stored versions' vars (F2 — refs
        // are content-scoped, so a torn publish never strands the
        // live envelope).
        state::write_vars(&self.root, &st.vars_union())?;
        // 2. state
        state::write(&self.root, &st)?;
        // 3. signed desired
        self.publish(&st, &key)?;
        // F4: GC objects no version references (evicted bundles and
        // stale uploads); system modules survive via `system_objects`.
        // Still under the writer lock; a failed delete leaves an
        // orphan (harmless), never a broken reference.
        self.gc_objects(&st);
        oplog::append(
            &self.root,
            &oplog::entry(
                "deploy",
                serde_json::json!({
                    "worker": pkg.name, "versionId": version_id, "digest": digest,
                    "revision": st.revision,
                }),
            ),
        )?;
        Ok(DeployResult {
            version_id,
            artifact_digest: digest,
            revision: st.revision,
        })
    }

    /// Point a worker's active version at an already-deployed version.
    pub fn rollback(&self, worker: &str, version_id: &str) -> Result<u64, LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let mut st = state::read(&self.root)?;
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        st.rollback(worker, version_id)?;
        Self::validate_desired(&st)?;
        // W1-r2: same load check as deploy — a rolled-back config must
        // also load (vendored/runtime upgrades can invalidate a
        // previously-loadable version; its artifact is already in
        // objects/, so no pending bytes).
        preflight::deploy_preflight(&self.root, &st, None)?;
        state::write_vars(&self.root, &st.vars_union())?;
        state::write(&self.root, &st)?;
        self.publish(&st, &key)?;
        oplog::append(
            &self.root,
            &oplog::entry(
                "rollback",
                serde_json::json!({
                    "worker": worker, "versionId": version_id, "revision": st.revision,
                }),
            ),
        )?;
        Ok(st.revision)
    }

    /// Add a public hostname to a worker (globally unique enforced by
    /// the shared `compute.desired` validator before signing).
    pub fn set_hostname(&self, worker: &str, hostname: &str) -> Result<u64, LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let mut st = state::read(&self.root)?;
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        st.set_hostname(worker, hostname, true)?;
        Self::validate_desired(&st)?;
        state::write(&self.root, &st)?;
        self.publish(&st, &key)?;
        oplog::append(
            &self.root,
            &oplog::entry(
                "setHostname",
                serde_json::json!({
                    "worker": worker, "hostname": hostname, "revision": st.revision,
                }),
            ),
        )?;
        Ok(st.revision)
    }

    /// Detach a hostname.
    pub fn remove_hostname(&self, worker: &str, hostname: &str) -> Result<u64, LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let mut st = state::read(&self.root)?;
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        st.set_hostname(worker, hostname, false)?;
        Self::validate_desired(&st)?;
        state::write(&self.root, &st)?;
        self.publish(&st, &key)?;
        oplog::append(
            &self.root,
            &oplog::entry(
                "removeHostname",
                serde_json::json!({
                    "worker": worker, "hostname": hostname, "revision": st.revision,
                }),
            ),
        )?;
        Ok(st.revision)
    }

    /// Store a secret value under the local tenant namespace (T13/M6
    /// shape `{tenant:{ref:value}}`). Cross-tenant writes fail closed.
    pub fn put_secret(
        &self,
        tenant_id: &str,
        secret_ref: &str,
        value: &str,
    ) -> Result<(), LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let st = state::read(&self.root)?;
        if tenant_id != st.tenant_id() {
            return Err(LocalError::WrongTenant);
        }
        if !valid_secret_ref(secret_ref) {
            return Err(LocalError::Worker("secretRef"));
        }
        let mut secrets = state::read_secrets(&self.root)?;
        secrets
            .entry(st.tenant_id().to_string())
            .or_default()
            .insert(secret_ref.to_string(), value.to_string());
        // T17/F1: write secrets BEFORE publishing (the documented
        // crash window is: secrets updated + old envelope — the host
        // hot-reloads inputs anyway, so the window is benign).
        // The revision bump is still required: it refreshes the
        // envelope for fence/audit semantics and forces a host reload.
        state::write_secrets(&self.root, &secrets)?;
        let mut st = st;
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        Self::validate_desired(&st)?;
        state::write(&self.root, &st)?;
        self.publish(&st, &key)?;
        oplog::append(
            &self.root,
            &oplog::entry(
                "putSecret",
                serde_json::json!({
                    "tenantId": tenant_id, "secretRef": secret_ref,
                    "revision": st.revision,
                }),
            ),
        )?;
        Ok(())
    }

    /// Remove a secret ref (fails closed if absent).
    pub fn delete_secret(&self, tenant_id: &str, secret_ref: &str) -> Result<(), LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let st = state::read(&self.root)?;
        if tenant_id != st.tenant_id() {
            return Err(LocalError::WrongTenant);
        }
        let mut secrets = state::read_secrets(&self.root)?;
        let removed = secrets
            .get_mut(st.tenant_id())
            .map(|m| m.remove(secret_ref).is_some())
            .unwrap_or(false);
        if !removed {
            return Err(LocalError::NotFound("secretRef"));
        }
        state::write_secrets(&self.root, &secrets)?;
        let mut st = st;
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        Self::validate_desired(&st)?;
        state::write(&self.root, &st)?;
        self.publish(&st, &key)?;
        oplog::append(
            &self.root,
            &oplog::entry(
                "deleteSecret",
                serde_json::json!({
                    "tenantId": tenant_id, "secretRef": secret_ref,
                    "revision": st.revision,
                }),
            ),
        )?;
        Ok(())
    }

    /// Re-sign the current state (envelope TTL refresh — `of serve`
    /// calls this periodically; mutating ops already re-sign).
    pub fn renew(&self) -> Result<u64, LocalError> {
        let _lock = lock::StateLock::acquire(&self.root)?;
        let key = keys::load_signing_key(&self.root)?;
        let mut st = state::read(&self.root)?;
        if st.revision == 0 {
            return Err(LocalError::State("revision"));
        }
        // Renewal is a NEW publication, not a re-signature: the same
        // revision with different envelope bytes would trip the host's
        // `RevisionConflict` fence. Bump then sign.
        st.revision = st
            .revision
            .checked_add(1)
            .ok_or(LocalError::State("revision"))?;
        Self::validate_desired(&st)?;
        state::write(&self.root, &st)?;
        self.publish(&st, &key)?;
        oplog::append(
            &self.root,
            &oplog::entry(
                "renew",
                serde_json::json!({
                    "revision": st.revision,
                }),
            ),
        )?;
        Ok(st.revision)
    }

    /// Read-only status.
    pub fn status(&self) -> Result<LocalStatus, LocalError> {
        let st = state::read(&self.root)?;
        let published = desired::read_envelope_revision(&self.root);
        let secrets = state::read_secrets(&self.root).unwrap_or_default();
        Ok(LocalStatus {
            cell_id: st.cell_id().to_string(),
            tenant_id: st.tenant_id().to_string(),
            revision: st.revision,
            published_revision: published,
            workers: st.worker_status(crate::preflight::host_quotas_enforced(&self.root)),
            secret_refs: secrets
                .get(st.tenant_id())
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default(),
        })
    }

    /// Resolve a worker by display name OR stable `workerId` — the
    /// same convention `versions`/`rollback`/the DO admin commands
    /// use. Typed state lookup: NEVER parse `of status` JSON output
    /// positionally (its keys serialize alphabetically, so `name`
    /// and `workerId` are NOT adjacent — P3c-2-r2).
    pub fn resolve_worker_id(&self, q: &str) -> Result<String, LocalError> {
        let st = state::read(&self.root)?;
        st.workers
            .iter()
            .find(|(n, w)| n.as_str() == q || w.worker_id == q)
            .map(|(_, w)| w.worker_id.clone())
            .ok_or(LocalError::NotFound("worker"))
    }

    /// Active version's declared DO class name → `uniqueKey`
    /// (server-minted at deploy; the admin surface keys classes by
    /// uniqueKey, never by display name). Lookup only — the key
    /// itself is never derived from caller input here.
    pub fn do_class_key(&self, worker: &str, class_name: &str) -> Result<String, LocalError> {
        let st = state::read(&self.root)?;
        let w = st
            .workers
            .iter()
            .find(|(n, w)| n.as_str() == worker || w.worker_id == worker)
            .map(|(_, w)| w)
            .ok_or(LocalError::NotFound("worker"))?;
        let v = w
            .active_version_id
            .as_ref()
            .and_then(|id| w.versions.iter().find(|v| &v.version_id == id))
            .ok_or(LocalError::State("activeVersion"))?;
        v.durable_object_classes
            .iter()
            .find(|c| c.class_name == class_name)
            .map(|c| c.unique_key.clone())
            .ok_or(LocalError::NotFound("do class"))
    }

    /// Active version's declared workflows (W1) — typed lookup for
    /// the read-only `of workflows` surface; never parsed from
    /// status JSON.
    pub fn workflows(
        &self,
        worker: &str,
    ) -> Result<Vec<snapshot_protocol::compute_desired::WorkflowDesired>, LocalError> {
        let st = state::read(&self.root)?;
        let w = st
            .workers
            .iter()
            .find(|(n, w)| n.as_str() == worker || w.worker_id == worker)
            .map(|(_, w)| w)
            .ok_or(LocalError::NotFound("worker"))?;
        let v = w
            .active_version_id
            .as_ref()
            .and_then(|id| w.versions.iter().find(|v| &v.version_id == id))
            .ok_or(LocalError::State("activeVersion"))?;
        Ok(v.workflows.clone())
    }

    /// The local signing key's verifier record (for host config).
    pub fn trusted_key(&self) -> Result<TrustedKey, LocalError> {
        Ok(keys::load_signing_key(&self.root)?.trusted_key())
    }

    /// F4: delete `objects/` entries no stored version references
    /// (system modules are pinned via `system_objects`). Runs under
    /// the caller's writer lock after publish; failures leave
    /// harmless orphans, so they are intentionally non-fatal.
    fn gc_objects(&self, st: &state::LocalState) {
        let keep = st.referenced_digests();
        let dir = layout::objects_dir(&self.root);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };
        for e in entries.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            if !keep.contains(name) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }

    /// Build + validate the desired doc for `st` — must run BEFORE
    /// `state::write` so a state that would produce an un-signable
    /// document is never persisted (fail closed, no poisoned state).
    fn validate_desired(st: &state::LocalState) -> Result<(), LocalError> {
        let payload = st.to_desired()?;
        payload
            .validate()
            .map_err(|_| LocalError::Desired("validate"))
    }

    /// Build → validate → sign → atomically write the envelope.
    /// `ComputeDesired::validate` runs BEFORE signing so a malformed
    /// state can never produce a signed document the host would reject
    /// semantically.
    fn publish(&self, st: &state::LocalState, key: &SigningKey) -> Result<(), LocalError> {
        publish_at(&self.root, st, key)
    }
}

/// [`VrtLocal::publish`] against an arbitrary root — used by `init`
/// to publish the empty envelope inside staging before commit.
fn publish_at(
    root: &std::path::Path,
    st: &state::LocalState,
    key: &SigningKey,
) -> Result<(), LocalError> {
    let payload: ComputeDesired = st.to_desired()?;
    payload
        .validate()
        .map_err(|_| LocalError::Desired("validate"))?;
    let now = chrono::Utc::now();
    let scope = snapshot_protocol::compute_desired::cell_scope(&payload.cell_id);
    let signed = sign(
        &SignParams {
            kind: "compute.desired",
            scope: &scope,
            revision: payload.revision,
            issued_at: now,
            expires_at: now + ENVELOPE_TTL,
            payload: &payload,
        },
        key,
    )?;
    desired::write_envelope(root, &signed.bytes)?;
    Ok(())
}

/// Seed every manifest-declared system module into `objects/<sha256>`
/// so `DirBundleSource` resolves `sys/<digest>` embeds. Missing or
/// digest-mismatched dist files fail closed — a partial system module
/// set would render a capnp workerd refuses to compile anyway.
///
/// T25/L3 mapping convention: `--facade-dist` is a FACADE DIR holding
/// `manifest.json` + `dist/` (the generated dist tree, which mirrors
/// repo-relative paths). A module's bytes live at
/// `<facade-dir>/dist/<module.name>` — `name` is the logical
/// repo-relative module path, and the wire `dist` field must end with
/// `"/" + name` (a manifest whose dist paths stopped mirroring `name`
/// fails closed instead of seeding wrong bytes). The repo's `facade/`
/// dir and the image's `/usr/share/vrt/facade/` share this one layout.
///
/// The SAME verified bytes are also written to
/// `<root>/facade/dist/<module.name>` (the layout vrt-host's
/// `system_seed::read_module` reads next to `<root>/facade/manifest.json`),
/// and the manifest is persisted from the exact text that was parsed —
/// no source file is re-read after verification (no TOCTOU). The manifest
/// is written LAST so a manifest never exists without its full dist.
/// `root` is the init STAGING dir, published atomically by `init_commit`
/// only when every step succeeded; any failure here discards the whole
/// staging dir, so no half-seeded root is ever visible.
fn seed_system_modules(
    root: &Path,
    manifest_path: &Path,
    facade_dir: &Path,
) -> Result<std::collections::BTreeSet<String>, LocalError> {
    let raw = std::fs::read_to_string(manifest_path).map_err(|e| {
        LocalError::Op("seed manifest", format!("{}: {e}", manifest_path.display()))
    })?;
    let manifest = vrt_config::parse_facade_manifest(&raw)
        .map_err(|_| LocalError::Layout("facadeManifest"))?;
    // The manifest strips dist paths into digest refs; we need the
    // raw wire form again for the name/digest mapping.
    let wire: serde_json::Value =
        serde_json::from_str(&raw).map_err(|_| LocalError::Layout("facadeManifest"))?;
    let dist_root = facade_dir.join("dist");
    let mut seeded = std::collections::BTreeSet::new();
    let mut names: std::collections::BTreeMap<String, String> = Default::default();
    let out_dist = layout::facade_manifest_path(root)
        .parent()
        .ok_or(LocalError::Layout("facade dir"))?
        .join("dist");
    for worker in manifest.system_workers {
        let w = wire["workers"][&worker.service_name]
            .as_object()
            .ok_or(LocalError::Layout("facadeManifest worker"))?;
        let modules = w["modules"]
            .as_array()
            .ok_or(LocalError::Layout("modules"))?;
        for m in modules {
            let name = m["name"]
                .as_str()
                .ok_or(LocalError::Layout("module name"))?;
            let dist = m["dist"].as_str().ok_or(LocalError::Layout("dist"))?;
            let declared = m["sha256"].as_str().ok_or(LocalError::Layout("sha256"))?;
            // Cross-check the manifest's own bookkeeping: `dist` is the
            // dist-tree path, which is `<distRoot>/<name>` by the
            // generator's construction — the tail must equal `name`.
            if !dist.ends_with(&format!("/{name}")) {
                return Err(LocalError::Layout("dist/name mismatch"));
            }
            // `name` is a manifest-declared module path — confine to
            // plain relative segments before joining under dist/.
            let rel = Path::new(name);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return Err(LocalError::Layout("module path"));
            }
            let path = dist_root.join(rel);
            let bytes = std::fs::read(&path)
                .map_err(|e| LocalError::Op("seed module", format!("{}: {e}", path.display())))?;
            use sha2::Digest as _;
            let digest = hex::encode(sha2::Sha256::digest(&bytes));
            if digest != declared {
                return Err(LocalError::Layout("module digest"));
            }
            // One name = one file under dist/: a conflicting digest for
            // an already-written name would leave a dist the host rejects.
            match names.get(name) {
                Some(prev) if *prev != digest => {
                    return Err(LocalError::Layout("module name conflict"))
                }
                Some(_) => {}
                None => {
                    let out = out_dist.join(rel);
                    let werr = |e: std::io::Error| {
                        LocalError::Op("seed dist", format!("{}: {e}", out.display()))
                    };
                    if let Some(parent) = out.parent() {
                        vrt_securefs::ensure_secure_dir(parent).map_err(werr)?;
                    }
                    vrt_securefs::atomic_write(&out, &bytes, 0o644).map_err(werr)?;
                    names.insert(name.to_owned(), digest.clone());
                }
            }
            vrt_securefs::atomic_write(&layout::objects_dir(root).join(&digest), &bytes, 0o644)?;
            seeded.insert(digest);
        }
    }
    if seeded.is_empty() {
        return Err(LocalError::Layout("no modules"));
    }
    let manifest_out = layout::facade_manifest_path(root);
    vrt_securefs::atomic_write(&manifest_out, raw.as_bytes(), 0o644)
        .map_err(|e| LocalError::Op("seed manifest", format!("{}: {e}", manifest_out.display())))?;
    Ok(seeded)
}

/// The one secretRef shape rule (`put_secret`, remote-upload secrets)
/// — 1..=512 chars of `[A-Za-z0-9:_-.]`. Validates the NAME only;
/// values never pass through this or any error path.
fn valid_secret_ref(secret_ref: &str) -> bool {
    !secret_ref.is_empty()
        && secret_ref.len() <= 512
        && secret_ref
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b":_-.".contains(&b))
}
