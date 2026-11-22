//! `assignment.json` — the pod-local slice of authority the
//! cell-controller drops next to the signed envelope (T14/B1). The
//! host trusts NOTHING about it: it only narrows the already-verified
//! snapshot to the workers named for this pod. A missing/incomplete/
//! mismatched assignment never produces tenant traffic — convergence
//! keeps the previous good state and `readyz` reports the typed reason.

use serde::Deserialize;
use snapshot_protocol::compute_desired::{ComputeDesired, TenantDesired};
use std::collections::BTreeSet;
use std::path::Path;

/// Assignment file size bound — a cell's per-pod worker list is small;
/// anything larger is malformed transport, not authority.
pub const MAX_ASSIGNMENT_BYTES: usize = 256 * 1024;

/// Wire mirror of the controller's `PodAssignment` (kept local —
/// vrt-host must not depend on the controller crate; the contract is
/// the JSON schema).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PodAssignment {
    /// Pod identity the assignment is addressed to.
    pub pod_id: String,
    /// Snapshot revision this assignment partitions.
    pub revision: u64,
    /// `payloadDigest` of the signed snapshot payload.
    pub payload_digest: String,
    /// sha256 of the raw envelope bytes the controller wrote.
    #[serde(default)]
    pub envelope_digest: String,
    /// Controller-side generation time (RFC3339; informational).
    #[serde(default)]
    pub generated_at: String,
    /// Assigned `(tenantId, workerId)` pairs.
    pub workers: Vec<AssignedWorker>,
}

/// One assigned worker pair.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssignedWorker {
    /// Tenant id.
    pub tenant_id: String,
    /// Worker id.
    pub worker_id: String,
}

/// Why the assignment cannot drive convergence — mapped onto the
/// controller's typed ACK whitelist (`pods-not-applied`-family codes).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AssignmentError {
    /// `envelope.json` exists but `assignment.json` does not (mid-write
    /// gap) — incomplete delivery, retry next tick; NOT a hard fault.
    #[error("assignment-incomplete")]
    Incomplete,
    /// File unreadable / oversized / malformed JSON.
    #[error("assignment-incomplete")]
    Malformed,
    /// `podId` is not this pod.
    #[error("assignment-mismatch")]
    WrongPod,
    /// `revision` or `payloadDigest` disagrees with the verified
    /// snapshot — stale or cross-envelope pair; fail closed.
    #[error("assignment-mismatch")]
    Stale,
}

impl AssignmentError {
    /// Wire reason (typed, whitelist-stable).
    pub fn code(self) -> &'static str {
        match self {
            Self::Incomplete | Self::Malformed => "assignment-incomplete",
            Self::WrongPod | Self::Stale => "assignment-mismatch",
        }
    }
}

/// Load + validate the assignment against this pod id and the verified
/// snapshot's `(revision, payloadDigest)`. Returns the typed record or
/// a fail-closed reason — callers must NOT converge on error.
pub fn load_assignment(
    path: &Path,
    pod_id: &str,
    desired: &ComputeDesired,
    payload_digest: &str,
) -> Result<PodAssignment, AssignmentError> {
    let meta = std::fs::metadata(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AssignmentError::Incomplete
        } else {
            AssignmentError::Malformed
        }
    })?;
    if meta.len() as usize > MAX_ASSIGNMENT_BYTES {
        return Err(AssignmentError::Malformed);
    }
    let raw = std::fs::read(path).map_err(|_| AssignmentError::Malformed)?;
    let a: PodAssignment = serde_json::from_slice(&raw).map_err(|_| AssignmentError::Malformed)?;
    if a.pod_id != pod_id {
        return Err(AssignmentError::WrongPod);
    }
    if a.revision != desired.revision || a.payload_digest != payload_digest {
        return Err(AssignmentError::Stale);
    }
    Ok(a)
}

/// Adopt-or-block decision for one converge tick: load + validate the
/// assignment against the verified snapshot, then filter. The shared
/// entry point for `bin/vrt-host` and tests — the production path is
/// this function, not a re-implementation.
pub fn adopt(
    path: &Path,
    pod_id: &str,
    desired: &ComputeDesired,
    payload_digest: &str,
) -> Result<(ComputeDesired, Vec<(String, String)>), AssignmentError> {
    let a = load_assignment(path, pod_id, desired, payload_digest)?;
    let filtered = filter_to_assignment(desired, &a);
    let assigned = a
        .workers
        .iter()
        .map(|w| (w.tenant_id.clone(), w.worker_id.clone()))
        .collect();
    Ok((filtered, assigned))
}

/// Narrow the verified snapshot to this pod's assignment — every
/// tenant keeps only its assigned workers; unassigned tenants drop
/// out entirely (they drain via the normal remove path).
///
/// PREVIEW: a preview instance follows the assignment of the
/// production worker it previews — the controller assigns the
/// production worker, and the preview must not require a second
/// assignment entry (its instance id is derived and may change with
/// every preview). An unassigned production worker still keeps its
/// preview out, and previews never pull in a tenant on their own.
pub fn filter_to_assignment(desired: &ComputeDesired, a: &PodAssignment) -> ComputeDesired {
    let wanted: BTreeSet<(&str, &str)> = a
        .workers
        .iter()
        .map(|w| (w.tenant_id.as_str(), w.worker_id.as_str()))
        .collect();
    let tenants: Vec<TenantDesired> = desired
        .tenants
        .iter()
        .filter_map(|t| {
            let workers: Vec<_> = t
                .workers
                .iter()
                .filter(|w| {
                    wanted.contains(&(t.tenant_id.as_str(), w.worker_id.as_str()))
                        || w.preview.as_ref().is_some_and(|p| {
                            wanted.contains(&(t.tenant_id.as_str(), p.worker_id.as_str()))
                        })
                })
                .cloned()
                .collect();
            (!workers.is_empty()).then(|| TenantDesired {
                tenant_id: t.tenant_id.clone(),
                workers,
                flagship_apps: t.flagship_apps.clone(),
            })
        })
        .collect();
    let mut filtered = desired.clone();
    filtered.tenants = tenants;
    filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    use snapshot_protocol::compute_desired::{
        PreviewDesired, TenantDesired, VersionDesired, WorkerDesired, WorkerQuotas,
    };

    fn worker(id: &str, preview: Option<PreviewDesired>) -> WorkerDesired {
        WorkerDesired {
            worker_id: id.into(),
            name: id.replace('.', "-"),
            active_version: VersionDesired {
                version_id: format!("v-{id}"),
                artifact_digest: "a".repeat(64),
                compatibility_date: "2024-01-01".into(),
                compatibility_flags: vec![],
                tag: String::new(),
                created_at: String::new(),
            },
            bindings: vec![],
            routes: vec![],
            crons: vec![],
            queue_consumers: vec![],
            durable_object_classes: vec![],
            workflows: vec![],
            tail_consumers: vec![],
            streaming_tail_consumers: vec![],
            observability: None,
            quotas: WorkerQuotas::platform_default(),
            preview,
        }
    }

    fn desired() -> ComputeDesired {
        ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 2,
            tenants: vec![
                TenantDesired {
                    tenant_id: "t1".into(),
                    workers: vec![
                        worker("w1", None),
                        worker(
                            "w1.pv00000000",
                            Some(PreviewDesired {
                                worker_id: "w1".into(),
                                version_id: "v-w1.pv00000000".into(),
                            }),
                        ),
                    ],
                    flagship_apps: vec![],
                },
                TenantDesired {
                    tenant_id: "t2".into(),
                    workers: vec![worker("w2", None)],
                    flagship_apps: vec![],
                },
            ],
            deletions: vec![],
        }
    }

    fn assignment(pairs: &[(&str, &str)]) -> PodAssignment {
        PodAssignment {
            pod_id: "pod-0".into(),
            revision: 2,
            payload_digest: "d".into(),
            envelope_digest: String::new(),
            generated_at: String::new(),
            workers: pairs
                .iter()
                .map(|(t, w)| AssignedWorker {
                    tenant_id: (*t).into(),
                    worker_id: (*w).into(),
                })
                .collect(),
        }
    }

    /// PREVIEW: a preview instance follows its producer's assignment and
    /// never pulls a tenant into a pod on its own.
    #[test]
    fn preview_instances_inherit_the_producer_assignment() {
        let filtered = filter_to_assignment(&desired(), &assignment(&[("t1", "w1")]));
        assert_eq!(filtered.tenants.len(), 1, "t2 is not assigned");
        let ids: Vec<&str> = filtered.tenants[0]
            .workers
            .iter()
            .map(|w| w.worker_id.as_str())
            .collect();
        assert_eq!(ids, ["w1", "w1.pv00000000"]);

        // Assigning ONLY the preview id does not admit the producer (and
        // the preview cannot drag an unassigned tenant into the pod).
        let only_preview =
            filter_to_assignment(&desired(), &assignment(&[("t1", "w1.pv00000000")]));
        let ids: Vec<&str> = only_preview.tenants[0]
            .workers
            .iter()
            .map(|w| w.worker_id.as_str())
            .collect();
        assert_eq!(ids, ["w1.pv00000000"]);
        assert!(
            filter_to_assignment(&desired(), &assignment(&[("t2", "w2.pv99999999")]))
                .tenants
                .is_empty(),
            "a dangling preview id assigns nothing"
        );
    }
}
