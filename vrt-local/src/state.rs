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

/// One immutable version — code identity PLUS everything that decides
/// runtime behavior (platform `VrtWorkerVersion` parity, F2).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VersionRecord {
    pub version_id: String,
    pub artifact_digest: String,
    pub compatibility_date: String,
    #[serde(default)]
    pub compatibility_flags: Vec<String>,
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
    /// Tail consumers of this version — CF treats `tail_consumers` as a
    /// deploy-time setting, so it is version-scoped like queue consumers
    /// and a rollback restores the earlier consumer set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tail_consumers: Vec<snapshot_protocol::compute_desired::TailConsumerDesired>,
    /// `[observability]` — version-scoped collection gates; a
    /// rollback restores the earlier gates with the version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observability: Option<snapshot_protocol::compute_desired::ObservabilityDesired>,
    /// `version_metadata` binding name — `to_desired` synthesizes the
    /// `{id, tag, timestamp}` `json` binding from THIS record so a
    /// rollback reports the reactivated version's values (CF).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_metadata_binding: Option<String>,
    /// Version creation time (unix ms) — the `timestamp` field of the
    /// synthesized version_metadata binding. `0` on pre-feature rows
    /// (renders the epoch, never blocks a legacy rollback).
    #[serde(default)]
    pub created_at_unix_ms: u64,
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

    /// Semantic version identity — mirrors platform `computeVersionSha`
    /// coverage (code digest + compat + bindings + var values + crons +
    /// quotas). A var-only change MUST yield a new version (F2).
    fn version_id(pkg: &crate::worker_pkg::PackedWorker) -> String {
        // T37b-r3: `queueConsumers` MUST be part of version identity —
        // it lives on VersionRecord (per-version config), so a deploy
        // that changes ONLY the consumer set must mint a new version.
        // Without it the dedup at apply_deploy silently dropped the
        // new consumer config and desired kept dispatching the removed
        // queue — fail-open on a data-plane knob.
        let canonical = serde_json::json!({
            "artifactDigest": pkg.digest,
            "compatibilityDate": pkg.compatibility_date,
            "compatibilityFlags": pkg.compatibility_flags,
            "bindings": pkg.bindings,
            "varValues": pkg.var_values,
            "crons": pkg.crons,
            "queueConsumers": pkg.queue_consumers,
            // P3c-1: DO class declarations (incl. uniqueKey lineage)
            // are version identity — a pure class-set change must mint
            // a new version so rollback restores the exact namespace set.
            "durableObjectClasses": pkg.durable_object_classes,
            "quotas": pkg.quotas,
        });
        // W1: `workflows` joins version identity ONLY when declared —
        // workers without workflows keep their pre-W1 version ids
        // (same canonical bytes) so a runtime upgrade doesn't mint
        // churn versions.
        let canonical = if pkg.workflows.is_empty() {
            canonical
        } else {
            let mut c = canonical;
            c["workflows"] = serde_json::json!(pkg.workflows);
            c
        };
        let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
        let digest = hex::encode(sha2::Sha256::digest(&bytes));
        format!("vrt_{}", &digest[..28])
    }

    /// Apply a packed worker dir: dedup identical versions by content
    /// identity, evict the oldest NON-ACTIVE versions past the cap,
    /// activate the new version. Returns `(version_id, artifact_digest)`.
    pub fn apply_deploy(
        &mut self,
        pkg: &crate::worker_pkg::PackedWorker,
    ) -> Result<(String, String), &'static str> {
        let max = self.max_versions();
        let record = self
            .workers
            .entry(pkg.name.clone())
            .or_insert_with(|| WorkerRecord {
                worker_id: format!("w_{}", &pkg.first_deploy_id),
                hostnames: Vec::new(),
                versions: Vec::new(),
                active_version_id: None,
            });
        // Hostnames remain worker-level (platform parity — routes are
        // not versioned); redeployable, newest non-empty wins.
        if !pkg.hostnames.is_empty() {
            record.hostnames = pkg.hostnames.clone();
        }
        let version_id = Self::version_id(pkg);
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
            record.versions.push(VersionRecord {
                version_id: version_id.clone(),
                artifact_digest: pkg.digest.clone(),
                compatibility_date: pkg.compatibility_date.clone(),
                compatibility_flags: pkg.compatibility_flags.clone(),
                bindings: pkg.bindings.clone(),
                var_values: pkg.var_values.clone(),
                crons: pkg.crons.clone(),
                queue_consumers: pkg.queue_consumers.clone(),
                tail_consumers: pkg.tail_consumers.clone(),
                observability: pkg.observability.clone(),
                version_metadata_binding: pkg.version_metadata_binding.clone(),
                created_at_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                durable_object_classes: pkg.durable_object_classes.clone(),
                workflows: pkg.workflows.clone(),
                quotas: pkg.quotas.clone(),
            });
        }
        record.active_version_id = Some(version_id.clone());
        Ok((version_id, pkg.digest.clone()))
    }

    pub fn rollback(&mut self, worker: &str, version_id: &str) -> Result<(), LocalError> {
        let record = self
            .workers
            .get_mut(worker)
            .ok_or(LocalError::NotFound("worker"))?;
        if !record.versions.iter().any(|v| v.version_id == version_id) {
            return Err(LocalError::NotFound("version"));
        }
        record.active_version_id = Some(version_id.to_string());
        Ok(())
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
            let active = w
                .active_version_id
                .as_ref()
                .and_then(|id| w.versions.iter().find(|v| &v.version_id == id))
                .ok_or(LocalError::State("activeVersion"))?;
            // CF `[version_metadata]`: `env.<binding>` is
            // `{id, tag, timestamp}` of the SERVING version —
            // synthesized here (not stored as a literal) so a rollback
            // reports the reactivated version's own values, matching
            // CF where metadata describes the version, not the deploy.
            let mut bindings = active.bindings.clone();
            if let Some(bname) = &active.version_metadata_binding {
                bindings.push(BindingDesired::Json(
                    snapshot_protocol::compute_desired::JsonBinding {
                        name: bname.clone(),
                        value: serde_json::json!({
                            "id": active.version_id,
                            "tag": "",
                            "timestamp": active.created_at_unix_ms.to_string(),
                        })
                        .to_string(),
                    },
                ));
            }
            workers.push(WorkerDesired {
                worker_id: w.worker_id.clone(),
                name: name.clone(),
                active_version: VersionDesired {
                    version_id: active.version_id.clone(),
                    artifact_digest: active.artifact_digest.clone(),
                    compatibility_date: active.compatibility_date.clone(),
                    compatibility_flags: active.compatibility_flags.clone(),
                },
                bindings,
                routes: w
                    .hostnames
                    .iter()
                    .map(|h| RouteDesired {
                        hostname: h.clone(),
                    })
                    .collect(),
                crons: active.crons.clone(),
                queue_consumers: active.queue_consumers.clone(),
                tail_consumers: active.tail_consumers.clone(),
                observability: active.observability.clone(),
                durable_object_classes: active.durable_object_classes.clone(),
                workflows: active.workflows.clone(),
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
            }],
            deletions: vec![],
})
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
    let st: LocalState = serde_json::from_slice(&raw).map_err(|_| LocalError::State("decode"))?;
    if st.schema_version != STATE_SCHEMA_VERSION {
        return Err(LocalError::State("schemaVersion"));
    }
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
