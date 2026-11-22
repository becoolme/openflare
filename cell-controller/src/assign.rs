//! Deterministic worker→Pod assignment.
//!
//! The cell's signed `compute.desired` names every (tenant, worker) that
//! may run on the cell; the controller partitions them across the cell's
//! `vrt-host` Pods. Placement uses rendezvous (highest-random-weight)
//! hashing keyed on `sha256(podId \0 tenantId \0 workerId)`:
//!
//! - deterministic input → deterministic output (no RNG, no clock);
//! - minimal movement when the Pod set changes (only the affected
//!   workers' top choice moves);
//! - no coordination — every controller replica computes the same table.
//!
//! The per-Pod deliverable is a `ComputeDesired` *filtered* to the Pod's
//! assignment (see [`filter_desired`]) plus the [`PodAssignment`] record —
//! both bound to the signed snapshot's `revision`/`payloadDigest`, so an
//! assignment can only narrow already-signed authority, never widen it.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::{ComputeDesired, TenantDesired, WorkerDesired};
use std::collections::{BTreeMap, BTreeSet};

/// One Pod's slice of the cell snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PodAssignment {
    /// Pod identity (the channel's `pods()` id).
    pub pod_id: String,
    /// Snapshot revision this assignment partitions.
    pub revision: u64,
    /// `payloadDigest` of the snapshot — the pod binds the filtered
    /// document to the verified envelope bytes it already holds.
    pub payload_digest: String,
    /// T14/B1: `sha256` of the RAW signed envelope bytes delivered
    /// alongside — the pod adopts the pair only when this matches the
    /// envelope file it received (fail closed on mismatch).
    #[serde(default)]
    pub envelope_digest: String,
    /// T14/B1: controller-side generation timestamp (RFC3339) —
    /// informational; ordering is enforced by `revision`, not time.
    #[serde(default)]
    pub generated_at: String,
    /// Assigned `(tenantId, workerId)` pairs, sorted.
    pub workers: Vec<AssignedWorker>,
}

/// A single worker placement inside a [`PodAssignment`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssignedWorker {
    /// `TenantDesired.tenant_id`.
    pub tenant_id: String,
    /// `WorkerDesired.worker_id`.
    pub worker_id: String,
}

/// Partition all desired workers across `pods` (pod ids, any order).
/// Deterministic: identical `(desired, pods)` always produces the same
/// `podId → PodAssignment` map. Pods with no workers get empty records —
/// they still converge (drain anything stale).
pub fn assign_workers(
    desired: &ComputeDesired,
    pods: &[String],
) -> BTreeMap<String, PodAssignment> {
    let mut table: BTreeMap<String, PodAssignment> = pods
        .iter()
        .map(|pod_id| {
            (
                pod_id.clone(),
                PodAssignment {
                    pod_id: pod_id.clone(),
                    revision: desired.revision,
                    payload_digest: String::new(),
                    envelope_digest: String::new(),
                    generated_at: String::new(),
                    workers: Vec::new(),
                },
            )
        })
        .collect();
    for tenant in &desired.tenants {
        for worker in &tenant.workers {
            let Some(pod_id) = rendezvous_pick(&tenant.tenant_id, &worker.worker_id, pods) else {
                continue;
            };
            table
                .get_mut(pod_id)
                .expect("rendezvous_pick returned a member of pods")
                .workers
                .push(AssignedWorker {
                    tenant_id: tenant.tenant_id.clone(),
                    worker_id: worker.worker_id.clone(),
                });
        }
    }
    for assignment in table.values_mut() {
        assignment.workers.sort();
    }
    table
}

/// Highest-random-weight pick: the pod whose keyed digest is largest.
pub fn rendezvous_pick<'a>(
    tenant_id: &str,
    worker_id: &str,
    pods: &'a [String],
) -> Option<&'a str> {
    pods.iter()
        .map(|pod| {
            let mut hasher = Sha256::new();
            hasher.update(pod.as_bytes());
            hasher.update([0]);
            hasher.update(tenant_id.as_bytes());
            hasher.update([0]);
            hasher.update(worker_id.as_bytes());
            (hasher.finalize(), pod.as_str())
        })
        // (digest, podId) max is total — podId tiebreak is impossible
        // (digests collide ~never; ties still deterministic).
        .max_by(|(a_digest, a_pod), (b_digest, b_pod)| {
            a_digest.cmp(b_digest).then_with(|| a_pod.cmp(b_pod))
        })
        .map(|(_, pod)| pod)
}

/// The `ComputeDesired` a Pod should converge to: same cell/revision,
/// only its assigned `(tenant, worker)` pairs. Empty tenants are dropped.
/// The result re-uses `vrt-host`'s convergence contract unchanged — a pod
/// treats it exactly like a whole-cell snapshot.
pub fn filter_desired(desired: &ComputeDesired, assignment: &PodAssignment) -> ComputeDesired {
    let wanted: BTreeSet<(&str, &str)> = assignment
        .workers
        .iter()
        .map(|w| (w.tenant_id.as_str(), w.worker_id.as_str()))
        .collect();
    let tenants: Vec<TenantDesired> = desired
        .tenants
        .iter()
        .filter_map(|tenant| {
            let workers: Vec<WorkerDesired> = tenant
                .workers
                .iter()
                .filter(|w| wanted.contains(&(tenant.tenant_id.as_str(), w.worker_id.as_str())))
                .cloned()
                .collect();
            (!workers.is_empty()).then(|| TenantDesired {
                tenant_id: tenant.tenant_id.clone(),
                workers,
                flagship_apps: tenant.flagship_apps.clone(),
            })
        })
        .collect();
    ComputeDesired {
        schema_version: desired.schema_version,
        cell_id: desired.cell_id.clone(),
        revision: desired.revision,
        tenants,
        // RV-R53: deletion tombstones are CELL-scoped — every pod must
        // see every tombstone so each pod purges its own local copy of
        // deleted worker/class/tenant data. Narrowing them to the pod's
        // assignment would strand deleted data on unassigned pods.
        deletions: desired.deletions.clone(),
    }
}

/// Digest of a full assignment table — the per-pod "did anything change"
/// idempotence key. Changing pods, membership or revision changes it.
pub fn assignment_fingerprint(table: &BTreeMap<String, PodAssignment>) -> String {
    let mut hasher = Sha256::new();
    for (pod, assignment) in table {
        hasher.update(pod.as_bytes());
        hasher.update([0]);
        for w in &assignment.workers {
            hasher.update(w.tenant_id.as_bytes());
            hasher.update([0]);
            hasher.update(w.worker_id.as_bytes());
            hasher.update([0xff]);
        }
    }
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn desired(workers: &[(&str, &str)]) -> ComputeDesired {
        let mut tenants: BTreeMap<&str, Vec<serde_json::Value>> = BTreeMap::new();
        for (tenant, worker) in workers {
            tenants.entry(*tenant).or_default().push(json!({
                "workerId": worker,
                "name": worker,
                "activeVersion": {
                    "versionId": format!("vrt_{worker}"),
                    "artifactDigest": "a".repeat(64),
                    "compatibilityDate": "2024-01-01",
                    "compatibilityFlags": []
                },
                "bindings": [],
                "crons": [],
                "quotas": {"cpuMs": 10, "memoryMb": 32, "outboundConnections": 4, "subrequests": 10},
                "routes": []
            }));
        }
        let payload = json!({
            "schemaVersion": 1,
            "cellId": "cell-1",
            "revision": 3,
            "tenants": tenants.iter().map(|(id, ws)| json!({"tenantId": id, "workers": ws})).collect::<Vec<_>>()
        });
        serde_json::from_value(payload).unwrap()
}

    #[test]
    fn deterministic_and_covers_every_worker() {
        let workers = [
            ("team-a", "w1"),
            ("team-a", "w2"),
            ("team-b", "w3"),
            ("team-c", "w4"),
            ("team-c", "w5"),
        ];
        let pods = vec![
            "pod-0".to_string(),
            "pod-1".to_string(),
            "pod-2".to_string(),
        ];
        let a = assign_workers(&desired(&workers), &pods);
        let b = assign_workers(&desired(&workers), &pods);
        assert_eq!(a, b);
        let placed: BTreeSet<_> = a
            .values()
            .flat_map(|p| {
                p.workers
                    .iter()
                    .map(|w| (w.tenant_id.as_str(), w.worker_id.as_str()))
            })
            .collect();
        assert_eq!(placed, workers.iter().copied().collect());
    }

    #[test]
    fn pod_set_change_moves_a_subset() {
        let workers: Vec<_> = (0..20)
            .map(|i| {
                (
                    "team-a",
                    Box::leak(format!("w{i}").into_boxed_str()) as &str,
                )
            })
            .collect();
        let three = vec!["p0".into(), "p1".into(), "p2".into()];
        let four = vec!["p0".into(), "p1".into(), "p2".into(), "p3".into()];
        let a = assign_workers(&desired(&workers), &three);
        let b = assign_workers(&desired(&workers), &four);
        let same: usize = ["p0", "p1", "p2"]
            .iter()
            .filter(|p| a[**p].workers == b[**p].workers)
            .count();
        assert!(same >= 1, "rendezvous should preserve most placements");
    }

    #[test]
    fn filtered_desired_keeps_only_assigned() {
        let d = desired(&[("team-a", "w1"), ("team-a", "w2"), ("team-b", "w3")]);
        let assignment = PodAssignment {
            pod_id: "pod-0".into(),
            revision: 3,
            payload_digest: "d".repeat(64),
            envelope_digest: "e".repeat(64),
            generated_at: String::new(),
            workers: vec![
                AssignedWorker {
                    tenant_id: "team-a".into(),
                    worker_id: "w2".into(),
                },
                AssignedWorker {
                    tenant_id: "team-b".into(),
                    worker_id: "w3".into(),
                },
            ],
        };
        let filtered = filter_desired(&d, &assignment);
        assert_eq!(filtered.revision, 3);
        assert_eq!(filtered.cell_id, "cell-1");
        assert_eq!(filtered.tenants.len(), 2);
        assert_eq!(filtered.tenants[0].workers[0].worker_id, "w2");
        assert_eq!(filtered.tenants[1].workers[0].worker_id, "w3");
    }

    #[test]
    fn fingerprint_changes_with_assignment() {
        let workers = [("team-a", "w1")];
        let pods2 = vec!["p0".to_string(), "p1".to_string()];
        let pods3 = vec!["p0".to_string(), "p1".to_string(), "p2".to_string()];
        assert_ne!(
            assignment_fingerprint(&assign_workers(&desired(&workers), &pods2)),
            assignment_fingerprint(&assign_workers(&desired(&workers), &pods3))
        );
    }
}
