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

/// Version metadata fields (`tag`, `created_at`) ride the wire only when
/// the version declares the `versionMetadata` binding — pre-
/// versionMetadata cells use `deny_unknown_fields` and would reject the
/// whole snapshot, so they must never see the fields otherwise.
fn version_metadata_fields(version: &VersionRecord) -> (String, String) {
    let has_version_metadata = version
        .bindings
        .iter()
        .any(|b| matches!(b, BindingDesired::VersionMetadata(_)));
    if !has_version_metadata {
        return (String::new(), String::new());
    }
    (
        version.tag.clone(),
        chrono::DateTime::from_timestamp_millis(version.created_at_unix_ms as i64)
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_default(),
    )
}

/// PREVIEW: stable instance id for `<worker_id>` serving
/// `<version_id>`. Deterministic (a re-converge of the same desired
/// state must not churn sockets) and SHORT: the id ends up in a
/// `<gen>/sock/<id>.sock` path, and `sun_path` is 104 bytes.
pub fn preview_instance_id(worker_id: &str, version_id: &str) -> String {
    let digest = hex::encode(sha2::Sha256::digest(version_id.as_bytes()));
    format!("{worker_id}.pv{}", &digest[..8])
}

/// PREVIEW: worker display name for an instance — unique per tenant,
/// a valid lowercase DNS label (no `--`), and derived from the
/// production name so logs/status stay readable.
pub fn preview_instance_name(name: &str, version_id: &str) -> String {
    let digest = hex::encode(sha2::Sha256::digest(version_id.as_bytes()));
    let suffix = format!("-pv{}", &digest[..8]);
    let keep = snapshot_protocol::compute_desired::MAX_WORKER_NAME_LEN
        .saturating_sub(suffix.len())
        .max(1);
    let mut base: String = name.chars().take(keep).collect();
    while base.ends_with('-') {
        base.pop();
    }
    format!("{base}{suffix}")
}

/// PREVIEW: project one stored preview version into the worker
/// instance the host renders and routes. Everything that would let a
/// preview act as the production version is dropped here (and the
/// protocol validator rejects a snapshot that reintroduces it):
///
/// - no routes (the preview hostname is host-derived),
/// - no crons / queue consumers / workflows / tail consumers,
/// - no secret bindings (tenant secrets are not version-scoped),
/// - no durable-object classes (previews may not share or fork DO
///   storage; a version declaring DOs is refused at preview time).
fn preview_worker_desired(
    worker: &WorkerRecord,
    name: &str,
    version: &VersionRecord,
) -> WorkerDesired {
    let (tag, created_at) = version_metadata_fields(version);
    WorkerDesired {
        worker_id: preview_instance_id(&worker.worker_id, &version.version_id),
        name: preview_instance_name(name, &version.version_id),
        active_version: VersionDesired {
            version_id: version.version_id.clone(),
            artifact_digest: version.artifact_digest.clone(),
            compatibility_date: version.compatibility_date.clone(),
            compatibility_flags: version.compatibility_flags.clone(),
            tag,
            created_at,
        },
        bindings: version
            .bindings
            .iter()
            .filter(|b| !matches!(b, BindingDesired::Secret(_)))
            .cloned()
            .collect(),
        routes: Vec::new(),
        crons: Vec::new(),
        queue_consumers: Vec::new(),
        durable_object_classes: Vec::new(),
        workflows: Vec::new(),
        tail_consumers: Vec::new(),
        streaming_tail_consumers: Vec::new(),
        observability: version.observability.clone(),
        quotas: version.quotas.clone(),
        preview: Some(snapshot_protocol::compute_desired::PreviewDesired {
            worker_id: worker.worker_id.clone(),
            version_id: version.version_id.clone(),
        }),
    }
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

    /// `of check` reports the version id a real deploy would mint —
    /// same identity function, no state needed.
    pub(crate) fn version_id_for_check(pkg: &crate::worker_pkg::PackedWorker) -> String {
        Self::version_id(pkg)
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
            let (tag, created_at) = version_metadata_fields(active);
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
                preview: None,
            });
            // PREVIEW: every still-previewable version becomes an extra
            // worker instance of this tenant (same process, own socket,
            // own version pin). The host turns each instance into
            // `<versionId>--<name>.<previewDomain>` and never touches
            // `activeVersionId`, so preview traffic can neither move nor
            // impersonate production. Newest first and capped: the cap
            // bounds the rendered config, not the stored history — an
            // older preview stays stored/promotable, it is just not
            // served over HTTP until re-previewed.
            let mut previewable: Vec<&VersionRecord> = w
                .versions
                .iter()
                .filter(|v| v.status == VersionStatus::Preview)
                .collect();
            previewable.sort_by(|a, b| {
                b.created_at_unix_ms
                    .cmp(&a.created_at_unix_ms)
                    .then_with(|| b.version_id.cmp(&a.version_id))
            });
            for version in previewable
                .into_iter()
                .take(snapshot_protocol::compute_desired::MAX_PREVIEWS_PER_WORKER)
            {
                workers.push(preview_worker_desired(w, name, version));
            }
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

    /// PREVIEW: a preview version is projected as a separate worker
    /// instance — own socket/version pin, production active pointer
    /// untouched, and none of the production-only surface.
    #[test]
    fn preview_versions_project_as_isolated_instances() {
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        let mut active = pkg();
        active.hostnames = vec!["api.example.com".into()];
        active.crons = vec![snapshot_protocol::compute_desired::CronDesired {
            schedule_id: "c1".into(),
            expression: "*/5 * * * *".into(),
        }];
        active.bindings = vec![BindingDesired::Secret(
            snapshot_protocol::compute_desired::SecretBinding {
                name: "API_TOKEN".into(),
                secret_ref: "sec:tok".into(),
            },
        )];
        let active_id = LocalState::version_id(&active);
        st.apply_deploy(&active, 1).unwrap();
        // Same behavior but a different version identity.
        let mut preview = pkg();
        preview.hostnames = active.hostnames.clone();
        preview.crons = active.crons.clone();
        preview.bindings = active.bindings.clone();
        preview.compatibility_flags = vec!["nodejs_compat".into()];
        let preview_id = LocalState::version_id(&preview);
        st.apply_preview(&preview, 2).unwrap();

        let desired = st.to_desired().unwrap();
        let workers = &desired.tenants[0].workers;
        assert_eq!(workers.len(), 2, "active + one preview instance");

        let prod = workers.iter().find(|w| w.preview.is_none()).unwrap();
        assert_eq!(prod.active_version.version_id, active_id);
        assert_eq!(prod.routes[0].hostname, "api.example.com");
        assert_eq!(prod.crons.len(), 1);
        assert_eq!(prod.bindings.len(), 1);

        let pv = workers.iter().find(|w| w.preview.is_some()).unwrap();
        let marker = pv.preview.as_ref().unwrap();
        assert_eq!(marker.worker_id, prod.worker_id);
        assert_eq!(marker.version_id, preview_id);
        assert_eq!(pv.active_version.version_id, preview_id);
        // Own identity (its own socket) — never the production worker id.
        assert_eq!(
            pv.worker_id,
            preview_instance_id(&prod.worker_id, &preview_id)
        );
        assert_ne!(pv.worker_id, prod.worker_id);
        assert_ne!(pv.name, prod.name);
        // No route (the host derives the preview hostname), no crons,
        // no secret bindings.
        assert!(pv.routes.is_empty());
        assert!(pv.crons.is_empty());
        assert!(pv.bindings.is_empty());
        assert!(pv.durable_object_classes.is_empty());
        // The signed document must validate (protocol invariants).
        desired.validate().unwrap();
    }

    /// REBASE MERGE (#74 version metadata + #82 preview instances): a
    /// preview instance carries the SAME `versionMetadata` projection as
    /// production — `tag`/`created_at` are set when the version declares
    /// the binding, and stay empty otherwise (pre-versionMetadata cells
    /// reject the fields outright).
    #[test]
    fn preview_instance_projects_version_metadata_only_when_declared() {
        use snapshot_protocol::compute_desired::{VersionMetadataBinding, VersionDesired};

        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg(), 1).unwrap();

        let mut untagged = pkg();
        untagged.compatibility_flags = vec!["untagged".into()];
        let untagged_id = LocalState::version_id(&untagged);
        st.apply_preview(&untagged, 2).unwrap();

        let mut tagged = pkg();
        tagged.compatibility_flags = vec!["tagged".into()];
        tagged.tag = "release-7".into();
        tagged.bindings = vec![BindingDesired::VersionMetadata(VersionMetadataBinding {
            name: "CF_VERSION_METADATA".into(),
        })];
        let tagged_id = LocalState::version_id(&tagged);
        st.apply_preview(&tagged, 1_700_000_000_000).unwrap();

        let desired = st.to_desired().unwrap();
        desired.validate().unwrap();
        let preview = |id: &str| -> &VersionDesired {
            &desired.tenants[0]
                .workers
                .iter()
                .find(|w| w.active_version.version_id == id)
                .unwrap()
                .active_version
        };
        assert_eq!(preview(&untagged_id).tag, "");
        assert_eq!(preview(&untagged_id).created_at, "");
        assert_eq!(preview(&tagged_id).tag, "release-7");
        assert_eq!(
            preview(&tagged_id).created_at,
            "2023-11-14T22:13:20.000Z",
            "the preview instance reports its own creation time"
        );
    }

    #[test]
    fn preview_instances_are_capped_newest_first() {
        let cap = snapshot_protocol::compute_desired::MAX_PREVIEWS_PER_WORKER;
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg(), 1).unwrap();
        let mut ids = Vec::new();
        for i in 0..(cap + 2) {
            let mut p = pkg();
            p.compatibility_flags = vec![format!("flag{i}")];
            ids.push(LocalState::version_id(&p));
            st.apply_preview(&p, 10 + i as u64).unwrap();
        }
        let desired = st.to_desired().unwrap();
        desired.validate().unwrap();
        let projected: std::collections::BTreeSet<String> = desired.tenants[0]
            .workers
            .iter()
            .filter(|w| w.preview.is_some())
            .map(|w| w.active_version.version_id.clone())
            .collect();
        assert_eq!(projected.len(), cap);
        // Exactly the NEWEST `cap` versions are projected; the older ones
        // stay stored and promotable, they are only not served.
        let newest: std::collections::BTreeSet<String> =
            ids.iter().rev().take(cap).cloned().collect();
        assert_eq!(projected, newest);
    }

    /// A promoted version stops being a preview instance: the production
    /// worker serves it, and the derived active-version preview host still
    /// resolves (route-table side).
    #[test]
    fn promoted_version_leaves_the_preview_instance_set() {
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg(), 1).unwrap();
        let mut p = pkg();
        p.compatibility_flags = vec!["nodejs_compat".into()];
        let preview_id = LocalState::version_id(&p);
        st.apply_preview(&p, 2).unwrap();
        assert_eq!(
            st.to_desired().unwrap().tenants[0]
                .workers
                .iter()
                .filter(|w| w.preview.is_some())
                .count(),
            1
        );
        st.promote("svc", &preview_id).unwrap();
        let desired = st.to_desired().unwrap();
        let workers = &desired.tenants[0].workers;
        assert_eq!(workers.len(), 1);
        assert!(workers[0].preview.is_none());
        assert_eq!(workers[0].active_version.version_id, preview_id);
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
