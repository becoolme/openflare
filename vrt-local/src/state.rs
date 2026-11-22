//! `state/state.json` — typed local truth (schema v2). The envelope is
//! always a pure function of this document (`revision` included), so a
//! torn or missing envelope is fully recoverable by re-signing.
//!
//! v2 (T17/F2): version records carry their OWN bindings/var values/
//! crons/quotas — matching platform semantics where `varValuesDigest`
//! and the binding set are part of version identity (`vrtAdapter.ts`
//! `computeVersionSha`). Rollback therefore restores a version's whole
//! behavior, not just its code. `var_ref` embeds a content hash of the
//! var set, so `vars.json` can hold EVERY stored version's entries as
//! a union — the crash window between the vars write and the envelope
//! write can never leave the published revision pointing at missing
//! keys.

use crate::{LocalError, WorkerStatus};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use snapshot_protocol::compute_desired::{
    BindingDesired, ComputeDesired, CronDesired, RouteDesired, TenantDesired, VersionDesired,
    WorkerDesired, WorkerQuotas,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const STATE_SCHEMA_VERSION: u8 = 2;

/// Hard ceiling for `max_versions_per_worker` — bounds state size.
pub const MAX_VERSIONS_CEILING: u32 = 256;
/// Default retained versions per worker (platform-parity ballpark).
pub const DEFAULT_MAX_VERSIONS: u32 = 64;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalState {
    pub schema_version: u8,
    pub cell_id: String,
    pub tenant_id: String,
    /// Monotone publish counter — every mutating op bumps it exactly
    /// once; the envelope fence rejects anything ≤ a delivered rev.
    pub revision: u64,
    /// Retained versions per worker; evicting the oldest non-active
    /// version is the F4 fix (a full history must not brick deploys).
    #[serde(default = "default_max_versions")]
    pub max_versions_per_worker: u32,
    /// System module digests seeded at init (facade dist) — GC keeps
    /// these objects even though no worker version references them.
    #[serde(default)]
    pub system_objects: BTreeSet<String>,
    /// `worker name → record`. BTreeMap = deterministic desired order.
    pub workers: BTreeMap<String, WorkerRecord>,
    /// Tenant-level Flagship app store — flag definitions ride the
    /// desired doc's tenant section so edits propagate without a
    /// redeploy (CF ≤30s propagation analog). `#[serde(default)]`
    /// keeps pre-feature state files loadable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flagship_apps: Vec<snapshot_protocol::compute_desired::FlagshipAppDesired>,
}

fn default_max_versions() -> u32 {
    DEFAULT_MAX_VERSIONS
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerRecord {
    /// Stable identifier assigned at first deploy.
    pub worker_id: String,
    #[serde(default)]
    pub hostnames: Vec<String>,
    /// Immutable versions; `active_version_id` selects which serves.
    #[serde(default)]
    pub versions: Vec<VersionRecord>,
    #[serde(default)]
    pub active_version_id: Option<String>,
}

/// Lifecycle of one stored version.
///
/// Only one version per worker may be `Active`; `Preview` versions
/// are uploaded but not selected by `activeVersionId`, and `Archived`
/// versions are historical. The status is deliberately NOT part of
/// version identity (see [`LocalState::version_id`]), so previewing a
/// version and later promoting it cannot mint a second id.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum VersionStatus {
    /// Historical version that is neither active nor being previewed.
    #[default]
    Archived,
    /// Uploaded by `deploy --preview`; never served until promoted.
    Preview,
    /// The version selected by `activeVersionId` and projected into
    /// the signed desired document.
    Active,
}

/// One immutable version — code identity PLUS everything that decides
/// runtime behavior (platform `VrtWorkerVersion` parity, F2).
///
/// Deliberately NOT `deny_unknown_fields`: a cell running an older
/// build must be able to read a `state.json` written by a newer build
/// that added version-scoped fields. Dropping an unknown field is safe
/// here (the state is re-projected through `to_desired`, and the host
/// re-validates), whereas failing the decode bricks the tenant on a
/// rollback. Same downgrade rule #48 established for `observability`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionRecord {
    pub version_id: String,
    pub artifact_digest: String,
    pub compatibility_date: String,
    #[serde(default)]
    pub compatibility_flags: Vec<String>,
    /// Lifecycle status. Missing on state files written before the
    /// versioning MVP; `read` normalizes the active record to
    /// [`VersionStatus::Active`] and older non-active records to
    /// [`VersionStatus::Archived`].
    #[serde(default)]
    pub status: VersionStatus,
    /// Creation time (unix ms). Metadata only — excluded from version
    /// identity, so redeploying the same input keeps the same id.
    #[serde(default)]
    pub created_at_unix_ms: u64,
    /// Creator identity when the caller can provide one. Standalone
    /// mode has no authenticated principal, so this is `None` today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// Hash of the behavior/config half of the version record
    /// (`cfg_<sha256-prefix>`). Included for diagnostics; the bundle
    /// itself stays content-addressed by `artifact_digest`.
    #[serde(default)]
    pub config_hash: String,
    /// Desired bindings resolved at deploy time (var refs are
    /// content-hash scoped — see `worker_pkg`).
    #[serde(default)]
    pub bindings: Vec<BindingDesired>,
    /// `varRef → value` for THIS version's var bindings.
    #[serde(default)]
    pub var_values: BTreeMap<String, String>,
    #[serde(default)]
    pub crons: Vec<CronDesired>,
    /// T37b: the set of queues the worker consumes when this version is activated (CF semantics --
    /// consumer relations follow the version, and rollback restores the original consumption relations).
    #[serde(default)]
    pub queue_consumers: Vec<snapshot_protocol::compute_desired::QueueConsumerDesired>,
    /// `[observability]` — version-scoped collection gates; a
    /// rollback restores the earlier gates with the version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observability: Option<snapshot_protocol::compute_desired::ObservabilityDesired>,
    /// Streaming tail consumers — version-scoped like `tail_consumers`
    /// (CF treats `streaming_tail_consumers` as a deploy-time setting;
    /// rollback restores the earlier set).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub streaming_tail_consumers: Vec<snapshot_protocol::compute_desired::TailConsumerDesired>,
    /// P3c-1: the DO class set declared by this version (migrations were already
    /// resolved to the final set at pack time, `uniqueKey` is fixed; rollback restores the original declaration --
    /// the storage identity of the `renamed_classes` lineage follows the version).
    #[serde(default)]
    pub durable_object_classes: Vec<snapshot_protocol::compute_desired::DoClassDesired>,
    /// W1: the workflow set declared by this version (`uniqueKey` was already
    /// derived at pack time; the same-worker restriction has passed). skip-if-empty keeps old version records
    /// byte compatible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<snapshot_protocol::compute_desired::WorkflowDesired>,
    /// Tail Workers this version forwards invocation events to. Resolved
    /// to same-tenant worker ids at pack time; rollback restores the
    /// version's consumer set. skip-if-empty keeps old records
    /// byte-compatible.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tail_consumers: Vec<snapshot_protocol::compute_desired::TailConsumerDesired>,
    /// CF `--tag` — the version's user label (`versionMetadata.tag`).
    /// `""` = untagged. Part of version identity (the binding exposes
    /// it), so `version_id` does NOT strip it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tag: String,
    pub quotas: WorkerQuotas,
}

impl LocalState {
    pub fn new(cell_id: &str, tenant_id: &str) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            cell_id: cell_id.to_string(),
            tenant_id: tenant_id.to_string(),
            revision: 0,
            max_versions_per_worker: DEFAULT_MAX_VERSIONS,
            system_objects: BTreeSet::new(),
            workers: BTreeMap::new(),
            flagship_apps: Vec::new(),
        }
    }

    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    fn max_versions(&self) -> usize {
        self.max_versions_per_worker.clamp(1, MAX_VERSIONS_CEILING) as usize
    }

    /// Canonical behavior half of a version record — the input to both
    /// `versionId` and `configHash`. Lifecycle metadata (`versionId`,
    /// timestamps, status, creator, config hash itself) is removed so
    /// a lifecycle transition can never change content identity.
    ///
    /// Deriving the input from the record's own struct (instead of a
    /// hand-maintained `json!` list) makes every version-scoped field
    /// join identity automatically: a field added to `VersionRecord`
    /// — and therefore replayed by `to_desired`/rollback — cannot be
    /// silently deduped away by `apply_deploy` because someone forgot
    /// to copy it here. This was the root cause of repeated fail-open
    /// bugs (`queueConsumers`, `tailConsumers`, `observability`,
    /// `streamingTailConsumers`): a pure config change "deployed"
    /// successfully but never reached desired.
    fn identity_value(record: &VersionRecord) -> serde_json::Value {
        let mut canonical = serde_json::to_value(record).unwrap_or_default();
        if let Some(obj) = canonical.as_object_mut() {
            for metadata in [
                "versionId",
                "createdAtUnixMs",
                "createdBy",
                "status",
                "configHash",
            ] {
                obj.remove(metadata);
            }
        }
        canonical
    }

    /// Semantic version identity — `vrt_<sha256-prefix>` over the
    /// canonical behavior half of the record.
    ///
    /// Absent/skipped fields keep their old canonical bytes, so
    /// workers that never declared the newer fields retain their
    /// pre-feature version ids (no churn on upgrade). The lifecycle
    /// fields added by the versioning MVP are removed above, so ids
    /// minted before it remain byte-identical.
    fn version_id(pkg: &crate::worker_pkg::PackedWorker) -> String {
        let record = Self::version_record(pkg, String::new());
        let bytes = serde_json::to_vec(&Self::identity_value(&record)).unwrap_or_default();
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        format!("vrt_{}", &digest[..28])
    }

    fn config_hash(record: &VersionRecord) -> String {
        let bytes = serde_json::to_vec(&Self::identity_value(record)).unwrap_or_default();
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        format!("cfg_{}", &digest[..28])
    }

    /// One stored version record for `pkg` — shared by `version_id`
    /// (identity input) and `apply_deploy`/`apply_preview` (persisted
    /// row) so the two can never drift.
    fn version_record(pkg: &crate::worker_pkg::PackedWorker, version_id: String) -> VersionRecord {
        let mut record = VersionRecord {
            version_id,
            artifact_digest: pkg.digest.clone(),
            compatibility_date: pkg.compatibility_date.clone(),
            compatibility_flags: pkg.compatibility_flags.clone(),
            status: VersionStatus::Archived,
            created_by: None,
            config_hash: String::new(),
            bindings: pkg.bindings.clone(),
            var_values: pkg.var_values.clone(),
            crons: pkg.crons.clone(),
            queue_consumers: pkg.queue_consumers.clone(),
            streaming_tail_consumers: pkg.streaming_tail_consumers.clone(),
            tail_consumers: pkg.tail_consumers.clone(),
            observability: pkg.observability.clone(),
            durable_object_classes: pkg.durable_object_classes.clone(),
            workflows: pkg.workflows.clone(),
            tag: pkg.tag.clone(),
            created_at_unix_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
            quotas: pkg.quotas.clone(),
        };
        record.config_hash = Self::config_hash(&record);
        record
    }

    fn worker_entry(&mut self, pkg: &crate::worker_pkg::PackedWorker) -> &mut WorkerRecord {
        self.workers
            .entry(pkg.name.clone())
            .or_insert_with(|| WorkerRecord {
                worker_id: format!("w_{}", &pkg.first_deploy_id),
                hostnames: Vec::new(),
                versions: Vec::new(),
                active_version_id: None,
            })
    }

    /// Record one immutable version. `activate` selects whether the
    /// new version becomes the worker's active pointer (deploy) or
    /// stays a preview (deploy --preview).
    ///
    /// Activation keeps the legacy F4 behavior: identical content is
    /// deduped and the oldest NON-ACTIVE version is evicted past the
    /// cap. Preview never evicts (the MVP intentionally has no
    /// retention policy for previews) and never touches hostnames or
    /// `activeVersionId`.
    fn apply_packaged(
        &mut self,
        pkg: &crate::worker_pkg::PackedWorker,
        now_unix_ms: u64,
        activate: bool,
    ) -> Result<(String, String), &'static str> {
        let max = self.max_versions();
        let version_id = Self::version_id(pkg);
        let record = self.worker_entry(pkg);
        if activate {
            // Hostnames remain worker-level (platform parity — routes
            // are not versioned); redeployable, newest non-empty wins.
            // Preview deliberately skips this: a preview must not
            // change the live route map.
            if !pkg.hostnames.is_empty() {
                record.hostnames = pkg.hostnames.clone();
            }
            let mut new_record = Self::version_record(pkg, version_id.clone());
            new_record.status = VersionStatus::Active;
            new_record.created_at_unix_ms = now_unix_ms;
            if !record.versions.iter().any(|v| v.version_id == version_id) {
                // F4: never refuse — evict oldest non-active first.
                while record.versions.len() >= max {
                    let pos = record
                        .versions
                        .iter()
                        .position(|v| record.active_version_id.as_ref() != Some(&v.version_id))
                        .ok_or("versions")?;
                    record.versions.remove(pos);
                }
                record.versions.push(new_record);
            } else if let Some(existing) = record
                .versions
                .iter_mut()
                .find(|v| v.version_id == version_id)
            {
                existing.status = VersionStatus::Active;
                if existing.created_at_unix_ms == 0 {
                    existing.created_at_unix_ms = now_unix_ms;
                }
            }
            for v in record.versions.iter_mut() {
                if v.version_id != version_id && v.status == VersionStatus::Active {
                    v.status = VersionStatus::Archived;
                }
            }
            record.active_version_id = Some(version_id.clone());
        } else {
            let mut new_record = Self::version_record(pkg, version_id.clone());
            new_record.status = VersionStatus::Preview;
            new_record.created_at_unix_ms = now_unix_ms;
            match record
                .versions
                .iter_mut()
                .find(|v| v.version_id == version_id)
            {
                Some(existing) => {
                    // Re-previewing the active version leaves it active;
                    // re-previewing an archived version marks it preview.
                    if record.active_version_id.as_deref() != Some(version_id.as_str()) {
                        existing.status = VersionStatus::Preview;
                    }
                    if existing.created_at_unix_ms == 0 {
                        existing.created_at_unix_ms = now_unix_ms;
                    }
                }
                None => record.versions.push(new_record),
            }
        }
        Ok((version_id, pkg.digest.clone()))
    }

    /// Apply a normal deploy: activate the version (see
    /// [`LocalState::apply_packaged`]).
    pub fn apply_deploy(
        &mut self,
        pkg: &crate::worker_pkg::PackedWorker,
        now_unix_ms: u64,
    ) -> Result<(String, String), &'static str> {
        self.apply_packaged(pkg, now_unix_ms, true)
    }

    /// Store a preview version without changing `activeVersionId`.
    pub fn apply_preview(
        &mut self,
        pkg: &crate::worker_pkg::PackedWorker,
        now_unix_ms: u64,
    ) -> Result<(String, String), &'static str> {
        self.apply_packaged(pkg, now_unix_ms, false)
    }

    /// Look up one stored version on one worker.
    pub fn version(&self, worker: &str, version_id: &str) -> Option<&VersionRecord> {
        self.workers
            .get(worker)
            .and_then(|w| w.versions.iter().find(|v| v.version_id == version_id))
    }

    fn mark_active(record: &mut WorkerRecord, version_id: &str) {
        for v in record.versions.iter_mut() {
            if v.version_id == version_id {
                v.status = VersionStatus::Active;
            } else if v.status == VersionStatus::Active {
                v.status = VersionStatus::Archived;
            }
        }
        record.active_version_id = Some(version_id.to_string());
    }

    /// Point one worker at `version_id`. Returns `true` when the
    /// active pointer changed. A repeated promote of the current
    /// version is a no-op (`false`) and never errors.
    pub fn promote(&mut self, worker: &str, version_id: &str) -> Result<bool, LocalError> {
        let record = self
            .workers
            .get_mut(worker)
            .ok_or(LocalError::NotFound("worker"))?;
        if !record.versions.iter().any(|v| v.version_id == version_id) {
            return Err(LocalError::NotFound("version"));
        }
        let changed = record.active_version_id.as_deref() != Some(version_id);
        Self::mark_active(record, version_id);
        Ok(changed)
    }

    /// Backwards-compatible rollback entry point; identical pointer
    /// semantics to promote.
    pub fn rollback(&mut self, worker: &str, version_id: &str) -> Result<(), LocalError> {
        self.promote(worker, version_id).map(|_| ())
    }

    pub fn set_hostname(
        &mut self,
        worker: &str,
        hostname: &str,
        add: bool,
    ) -> Result<(), LocalError> {
        let record = self
            .workers
            .get_mut(worker)
            .ok_or(LocalError::NotFound("worker"))?;
        if add {
            if !record.hostnames.iter().any(|h| h == hostname) {
                if record.hostnames.len() >= 64 {
                    return Err(LocalError::State("hostnames"));
                }
                record.hostnames.push(hostname.to_string());
            }
        } else {
            let before = record.hostnames.len();
            record.hostnames.retain(|h| h != hostname);
            if record.hostnames.len() == before {
                return Err(LocalError::NotFound("hostname"));
            }
        }
        Ok(())
    }

    /// `vars.json` content = UNION of every stored version's var
    /// values. Content-hash-scoped refs make the union disjoint, so a
    /// torn publish can never strand the live envelope (F2).
    pub fn vars_union(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        for w in self.workers.values() {
            for v in &w.versions {
                out.extend(v.var_values.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
        }
        out
    }

    /// Object digests still referenced — bundles of stored versions
    /// plus init-seeded system modules. Everything else under
    /// `objects/` is GC-eligible (F4).
    pub fn referenced_digests(&self) -> BTreeSet<String> {
        let mut out = self.system_objects.clone();
        for w in self.workers.values() {
            for v in &w.versions {
                out.insert(v.artifact_digest.clone());
            }
        }
        out
    }

    /// Project state into the protocol payload. The host re-validates
    /// everything (`verify_compute_desired` + `validate`) — producing
    /// an invalid doc here is a bug this method surfaces at publish.
    pub fn to_desired(&self) -> Result<ComputeDesired, LocalError> {
        if self.schema_version != STATE_SCHEMA_VERSION {
            return Err(LocalError::State("schemaVersion"));
        }
        if self.revision == 0 {
            return Err(LocalError::State("revision"));
        }
        let mut workers = Vec::with_capacity(self.workers.len());
        for (name, w) in &self.workers {
            // A preview-only worker has versions but no active pointer.
            // It must not enter the desired document (nothing may serve
            // it yet); a dangling pointer still fails closed below.
            let Some(active_id) = w.active_version_id.as_ref() else {
                continue;
            };
            let active = w
                .versions
                .iter()
                .find(|v| &v.version_id == active_id)
                .ok_or(LocalError::State("activeVersion"))?;
            // Version metadata fields ride the wire only when the
            // version declares the binding — `deny_unknown_fields`
            // makes pre-versionMetadata cells reject the whole
            // snapshot, so they must never see the fields otherwise.
            let has_version_metadata = active
                .bindings
                .iter()
                .any(|b| matches!(b, BindingDesired::VersionMetadata(_)));
            let (tag, created_at) = if has_version_metadata {
                (
                    active.tag.clone(),
                    chrono::DateTime::from_timestamp_millis(active.created_at_unix_ms as i64)
                        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                        .unwrap_or_default(),
                )
            } else {
                (String::new(), String::new())
            };
            workers.push(WorkerDesired {
                worker_id: w.worker_id.clone(),
                name: name.clone(),
                active_version: VersionDesired {
                    version_id: active.version_id.clone(),
                    artifact_digest: active.artifact_digest.clone(),
                    compatibility_date: active.compatibility_date.clone(),
                    compatibility_flags: active.compatibility_flags.clone(),
                    tag,
                    created_at,
                },
                bindings: active.bindings.clone(),
                routes: w
                    .hostnames
                    .iter()
                    .map(|h| RouteDesired {
                        hostname: h.clone(),
                    })
                    .collect(),
                crons: active.crons.clone(),
                queue_consumers: active.queue_consumers.clone(),
                observability: active.observability.clone(),
                streaming_tail_consumers: active.streaming_tail_consumers.clone(),
                durable_object_classes: active.durable_object_classes.clone(),
                workflows: active.workflows.clone(),
                tail_consumers: active.tail_consumers.clone(),
                quotas: active.quotas.clone(),
            });
        }
        Ok(ComputeDesired {
            schema_version: snapshot_protocol::compute_desired::SCHEMA_VERSION,
            cell_id: self.cell_id.clone(),
            revision: self.revision,
            tenants: vec![TenantDesired {
                tenant_id: self.tenant_id.clone(),
                workers,
                flagship_apps: self.flagship_apps.clone(),
            }],
            deletions: vec![],
        })
    }

    /// Enforce the lifecycle invariant on load: exactly the record
    /// selected by `activeVersionId` is `Active`; every other record
    /// can never be `Active`. State files written before the versioning
    /// MVP have no `status` field, so their active version decodes as
    /// `Archived` and is corrected here. This runs in memory only; the
    /// normalized form is persisted by the next mutating op.
    fn normalize_lifecycle(&mut self) {
        for w in self.workers.values_mut() {
            let active = w.active_version_id.clone();
            for v in w.versions.iter_mut() {
                if active.as_deref() == Some(v.version_id.as_str()) {
                    v.status = VersionStatus::Active;
                } else if v.status == VersionStatus::Active {
                    v.status = VersionStatus::Archived;
                }
                if v.config_hash.is_empty() {
                    v.config_hash = Self::config_hash(v);
                }
            }
        }
    }

    pub fn worker_status(&self, quotas_enforced: bool) -> Vec<WorkerStatus> {
        self.workers
            .iter()
            .map(|(name, w)| {
                let quotas = w
                    .active_version_id
                    .as_ref()
                    .and_then(|id| w.versions.iter().find(|v| &v.version_id == id))
                    .map(|v| v.quotas.clone())
                    .unwrap_or_else(WorkerQuotas::platform_default);
                WorkerStatus {
                    name: name.clone(),
                    worker_id: w.worker_id.clone(),
                    active_version_id: w.active_version_id.clone(),
                    versions: w.versions.iter().map(|v| v.version_id.clone()).collect(),
                    hostnames: w.hostnames.clone(),
                    // Enforced only when the cell can render limits AND
                    // this worker actually declared an enforceable one.
                    quotas_enforced: quotas_enforced && quotas.declares_enforceable_limit(),
                    quotas,
                }
            })
            .collect()
    }
}

pub fn read(root: &Path) -> Result<LocalState, LocalError> {
    let raw = std::fs::read(crate::layout::state_dir(root).join("state.json"))
        .map_err(|_| LocalError::Layout("state"))?;
    let mut st: LocalState =
        serde_json::from_slice(&raw).map_err(|_| LocalError::State("decode"))?;
    if st.schema_version != STATE_SCHEMA_VERSION {
        return Err(LocalError::State("schemaVersion"));
    }
    st.normalize_lifecycle();
    Ok(st)
}

pub fn write(root: &Path, st: &LocalState) -> Result<(), LocalError> {
    let bytes = serde_json::to_vec_pretty(st).map_err(|_| LocalError::State("encode"))?;
    vrt_securefs::atomic_write(
        &crate::layout::state_dir(root).join("state.json"),
        &bytes,
        0o600,
    )
    .map_err(|_| LocalError::Layout("state write"))
}

pub type SecretMap = BTreeMap<String, BTreeMap<String, String>>;

pub fn read_secrets(root: &Path) -> Result<SecretMap, LocalError> {
    let raw = std::fs::read(crate::layout::secrets_file(root))
        .map_err(|_| LocalError::Layout("secrets"))?;
    serde_json::from_slice(&raw).map_err(|_| LocalError::State("secrets decode"))
}
pub fn write_secrets(root: &Path, secrets: &SecretMap) -> Result<(), LocalError> {
    let bytes = serde_json::to_vec(secrets).map_err(|_| LocalError::State("secrets"))?;
    vrt_securefs::atomic_write(&crate::layout::secrets_file(root), &bytes, 0o600)
        .map_err(|_| LocalError::Layout("secrets write"))
}

pub fn write_vars(root: &Path, vars: &BTreeMap<String, String>) -> Result<(), LocalError> {
    let bytes = serde_json::to_vec(vars).map_err(|_| LocalError::State("vars"))?;
    vrt_securefs::atomic_write(&crate::layout::vars_file(root), &bytes, 0o600)
        .map_err(|_| LocalError::Layout("vars write"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use snapshot_protocol::compute_desired::{ObservabilityDesired, TailConsumerDesired};

    fn obs(rate_ppm: u32) -> ObservabilityDesired {
        ObservabilityDesired {
            enabled: true,
            logs_enabled: true,
            head_sampling_rate_ppm: rate_ppm,
            invocation_logs: true,
        }
    }

    fn tail(id: &str) -> TailConsumerDesired {
        TailConsumerDesired {
            worker_id: id.into(),
        }
    }

    /// Minimal pack input — every version-scoped field at its empty
    /// default so a test can change exactly one dimension.
    fn pkg() -> crate::worker_pkg::PackedWorker {
        crate::worker_pkg::PackedWorker {
            name: "svc".into(),
            digest: "a".repeat(64),
            stored_document: Vec::new(),
            compatibility_date: "2025-01-01".into(),
            compatibility_flags: vec![],
            bindings: vec![],
            var_values: BTreeMap::new(),
            crons: vec![],
            queue_consumers: vec![],
            tail_consumers: vec![],
            observability: None,
            streaming_tail_consumers: vec![],
            flagship_apps: vec![],
            durable_object_classes: vec![],
            workflows: vec![],
            hostnames: vec![],
            quotas: WorkerQuotas::platform_default(),
            first_deploy_id: "0123456789abcdef".into(),
            pending_assets: None,
            tag: String::new(),
        }
    }

    /// Regression for the root cause (#48/#51): version identity is
    /// derived from the stored record's fields, so a deploy that
    /// changes ONLY `observability` (or a tail-consumer list) mints a
    /// new version instead of being deduped away by `apply_deploy`.
    #[test]
    fn version_id_covers_every_version_scoped_field() {
        let base = pkg();
        let base_id = LocalState::version_id(&base);

        let mut rate = pkg();
        rate.observability = Some(obs(500_000));
        assert_ne!(base_id, LocalState::version_id(&rate), "sampling rate");

        let mut gates = pkg();
        gates.observability = Some(ObservabilityDesired {
            enabled: false,
            ..obs(1_000_000)
        });
        assert_ne!(base_id, LocalState::version_id(&gates), "enabled gate");

        let mut tails = pkg();
        tails.tail_consumers = vec![tail("w-consumer")];
        assert_ne!(base_id, LocalState::version_id(&tails), "tail consumers");

        let mut streaming = pkg();
        streaming.streaming_tail_consumers = vec![tail("w-live")];
        assert_ne!(
            base_id,
            LocalState::version_id(&streaming),
            "streaming tail consumers"
        );
        assert_ne!(
            LocalState::version_id(&tails),
            LocalState::version_id(&streaming),
            "the two channels must not alias"
        );

        let mut queues = pkg();
        queues.queue_consumers = vec![snapshot_protocol::compute_desired::QueueConsumerDesired {
            queue_id: "q-1".into(),
            queue_name: "q".into(),
            dead_letter_queue_id: None,
            max_batch_size: None,
            max_batch_timeout_secs: None,
            max_retries: None,
            retry_delay_secs: None,
            max_concurrency: None,
        }];
        assert_ne!(base_id, LocalState::version_id(&queues), "queue consumers");

        // And identity is a pure function of the record (no clock).
        assert_eq!(base_id, LocalState::version_id(&pkg()));
    }

    /// Downgrade compatibility: a newer cell may persist fields an
    /// older build does not know. `VersionRecord` must ignore them
    /// instead of failing the whole state decode (which would brick
    /// the tenant on rollback).
    #[test]
    fn version_record_ignores_unknown_fields() {
        let mut value =
            serde_json::to_value(LocalState::version_record(&pkg(), "vrt_test".into())).unwrap();
        value["futureKnob"] = serde_json::json!({"x": 1});
        let raw = serde_json::to_vec(&value).unwrap();
        let rec: VersionRecord =
            serde_json::from_slice(&raw).expect("unknown version field must be ignored");
        assert_eq!(rec.version_id, "vrt_test");
    }

    #[test]
    fn preview_does_not_activate_or_evict() {
        let mut st = LocalState::new("local", "local");
        st.max_versions_per_worker = 1;
        let active = pkg();
        let active_id = LocalState::version_id(&active);
        st.apply_deploy(&active, 1).unwrap();
        let mut preview = pkg();
        preview.observability = Some(obs(500_000));
        let preview_id = LocalState::version_id(&preview);
        assert_ne!(active_id, preview_id);
        st.apply_preview(&preview, 2).unwrap();
        let w = &st.workers["svc"];
        assert_eq!(w.active_version_id.as_deref(), Some(active_id.as_str()));
        assert_eq!(w.versions.len(), 2, "preview is stored, not discarded");
        assert_eq!(
            w.versions
                .iter()
                .find(|v| v.version_id == active_id)
                .unwrap()
                .status,
            VersionStatus::Active
        );
        assert_eq!(
            w.versions
                .iter()
                .find(|v| v.version_id == preview_id)
                .unwrap()
                .status,
            VersionStatus::Preview
        );
        // Preview does not publish a worker without an active version.
        let mut only_preview = LocalState::new("local", "local");
        only_preview.revision = 1;
        only_preview.apply_preview(&preview, 3).unwrap();
        assert_eq!(
            only_preview.to_desired().unwrap().tenants[0].workers.len(),
            0
        );
    }

    #[test]
    fn promote_is_idempotent_and_keeps_one_active() {
        let mut st = LocalState::new("local", "local");
        let first = pkg();
        let first_id = LocalState::version_id(&first);
        st.apply_deploy(&first, 1).unwrap();
        let mut second = pkg();
        second.observability = Some(obs(250_000));
        let second_id = LocalState::version_id(&second);
        st.apply_preview(&second, 2).unwrap();

        assert!(st.promote("svc", &second_id).unwrap());
        assert!(!st.promote("svc", &second_id).unwrap(), "idempotent replay");
        let w = &st.workers["svc"];
        assert_eq!(w.active_version_id.as_deref(), Some(second_id.as_str()));
        assert_eq!(
            w.versions
                .iter()
                .filter(|v| v.status == VersionStatus::Active)
                .count(),
            1,
            "exactly one active version"
        );
        assert_eq!(
            w.versions
                .iter()
                .find(|v| v.version_id == first_id)
                .unwrap()
                .status,
            VersionStatus::Archived
        );
        // Cross-worker promote fails closed.
        assert!(matches!(
            st.promote("other", &second_id),
            Err(LocalError::NotFound("worker"))
        ));
        assert!(matches!(
            st.promote("svc", "vrt_missing"),
            Err(LocalError::NotFound("version"))
        ));
    }

    #[test]
    fn lifecycle_metadata_is_not_version_identity() {
        let rec = LocalState::version_record(&pkg(), "vrt_a".into());
        let canonical = LocalState::identity_value(&rec);
        let mut changed = rec.clone();
        changed.version_id = "vrt_b".into();
        changed.status = VersionStatus::Active;
        changed.created_at_unix_ms = 42;
        changed.created_by = Some("someone".into());
        changed.config_hash = "cfg_other".into();
        assert_eq!(canonical, LocalState::identity_value(&changed));
        // Same input still yields the same id after a lifecycle edit.
        assert_eq!(
            LocalState::version_id(&pkg()),
            LocalState::version_id(&pkg())
        );
    }

    #[test]
    fn legacy_state_normalizes_lifecycle() {
        let mut st = LocalState::new("local", "local");
        let p = pkg();
        let id = LocalState::version_id(&p);
        st.apply_deploy(&p, 1).unwrap();
        // Simulate a pre-versioning state file: no status field on the
        // record, so decode defaults to Archived.
        let mut value = serde_json::to_value(&st).unwrap();
        value["workers"]["svc"]["versions"][0]
            .as_object_mut()
            .unwrap()
            .remove("status");
        let mut decoded: LocalState = serde_json::from_value(value).unwrap();
        assert_eq!(
            decoded.workers["svc"].versions[0].status,
            VersionStatus::Archived
        );
        decoded.normalize_lifecycle();
        assert_eq!(
            decoded.workers["svc"].versions[0].status,
            VersionStatus::Active
        );
        assert_eq!(
            decoded.workers["svc"].active_version_id.as_deref(),
            Some(id.as_str())
        );
    }
}
