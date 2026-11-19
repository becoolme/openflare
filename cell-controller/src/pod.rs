//! `PodChannel` — delivery + probe boundary between the controller and
//! the cell's `vrt-host` Pods.
//!
//! The control contract (T14/B1): the controller delivers the RAW
//! signed `compute.desired` envelope bytes plus a typed
//! [`PodAssignment`]; the pod (vrt-host) re-verifies the signature
//! itself and runs only the workers named in its assignment — the
//! controller's filtering is advisory, never authoritative.

use crate::assign::PodAssignment;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Mutex;
use thiserror::Error;

/// Why a pod-side operation failed — typed so the orchestrator can
/// report `PARTIAL` honestly instead of claiming `APPLIED`.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PodError {
    /// Pod is not part of the current inventory.
    #[error("unknown pod")]
    UnknownPod,
    /// Pod refused the assignment (e.g. revision fence, bad document).
    #[error("pod rejected the assignment")]
    Rejected,
    /// Transport/unreachable pod.
    #[error("pod unreachable")]
    Unreachable,
    /// Pod probe failed.
    #[error("pod probe failed")]
    ProbeFailed,
    /// Delivery mechanism is not implemented in this build (fail loud —
    /// never pretend an apply succeeded).
    #[error("pod channel unavailable")]
    Unavailable,
}

/// Result of delivering one pod's filtered desired + assignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodApplyResult {
    /// Revision the pod reports as loaded after the delivery.
    pub loaded_revision: u64,
}

/// MIG-1: one tenant's apply/switch failure INSIDE a pod — a typed,
/// bounded diagnostic carried BESIDE pod-level readiness, never AS it.
/// A `ready` pod may still report tenant failures (old generation keeps
/// serving); a pod-level failure carries `ready=false`/`reason` as
/// before. `code` is whitelist-stable; `detail` is sanitized + bounded
/// at the channel boundary (it never becomes an ACK `reason`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantFailure {
    /// Tenant id (as assigned).
    pub tenant: String,
    /// Worker ids of this tenant that hit the failure — MIG-4: empty
    /// serializes as ABSENT so the control plane applies its
    /// tenant-wide rule (changed-set ∩ tenant); a non-empty set marks
    /// exactly the named (attributed) workers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worker_ids: Vec<String>,
    /// Typed failure code (whitelist-stable).
    pub code: String,
    /// Sanitized bounded diagnostic, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Per-pod readiness + loaded revision, used by `/readyz`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodProbeResult {
    /// Pod reports ready (its own `/readyz` equivalent).
    pub ready: bool,
    /// Revision the pod currently has loaded, if any.
    pub loaded_revision: Option<u64>,
    /// T14/B1: `payloadDigest` the pod reports for its loaded revision
    /// — the controller only ACKs APPLIED when this matches the
    /// published snapshot (stale/forged loads never count).
    pub loaded_digest: Option<String>,
    /// T14/B1: typed reason when `ready == false` — whitelist codes
    /// only (`assignment-mismatch`, `switch-failed`, ...); free-form
    /// pod strings must be mapped to `None` by the channel.
    pub reason: Option<String>,
    /// MIG-1: tenant-level failures the pod processed while loading
    /// `loaded_revision` — empty on old hosts (absence is not pod-level
    /// evidence either way).
    pub tenant_failures: Vec<TenantFailure>,
}

/// Reasons a pod may report that are INFORMATIONAL (ready=true, e.g.
/// `hibernated`): they must never become an ACK failure reason nor mark
/// a pod not-applied on their own.
pub fn is_informational_reason(reason: &str) -> bool {
    reason == "hibernated"
}

/// Failure reason for a pod that is NOT applied: a typed failure reason
/// wins; an informational one (or none) collapses to `pods-not-applied`.
pub fn failure_reason(reason: Option<&str>) -> String {
    match reason {
        Some(r) if !is_informational_reason(r) => r.to_string(),
        _ => "pods-not-applied".to_string(),
    }
}

/// The pod-facing delivery + probe surface.
#[async_trait::async_trait]
pub trait PodChannel: Send + Sync {
    /// Current pod inventory ids (sorted not required — callers sort).
    fn pods(&self) -> Vec<String>;
    /// Deliver the RAW signed envelope bytes + this pod's assignment
    /// (T14/B1). The pod verifies the signature itself — the controller
    /// never sends a filtered, unsigned document. Implementations must
    /// be idempotent: re-delivering the same revision/digest is a no-op.
    async fn apply(
        &self,
        pod_id: &str,
        envelope_raw: &[u8],
        assignment: &PodAssignment,
    ) -> Result<PodApplyResult, PodError>;
    /// Readiness + loaded revision for `/readyz`.
    async fn probe(&self, pod_id: &str) -> Result<PodProbeResult, PodError>;
}

/// In-memory `PodChannel` for tests and local wiring. A pod "applies" by
/// recording the document; probes reflect the last applied revision and
/// the scripted readiness flag. Failure injection is per-pod call count.
#[derive(Debug, Default)]
pub struct MemoryPodChannel {
    pods: Mutex<Vec<String>>,
    applied: Mutex<BTreeMap<String, AppliedRecord>>,
    ready: Mutex<BTreeMap<String, bool>>,
    /// pod -> remaining apply attempts that fail (scripted).
    fail_applies: Mutex<BTreeMap<String, usize>>,
    apply_calls: Mutex<BTreeMap<String, usize>>,
    /// pod -> tenant failures the scripted probe reports (MIG-1).
    tenant_failures: Mutex<BTreeMap<String, Vec<TenantFailure>>>,
    /// pod -> pinned (revision, digest) the probe reports — models a
    /// pod still running an OLDER generation while the controller
    /// converges a newer one (MIG-6 tests). Absent ⇒ probe answers
    /// from the applied record as usual.
    probe_loaded: Mutex<BTreeMap<String, (Option<u64>, Option<String>)>>,
    /// pod -> probe should fail with this error — models a pod that
    /// is restarting/unreachable: it reports NOTHING, so the
    /// controller must not read "no failures reported" as "failures
    /// cleared" (MIG-8 tests).
    probe_error: Mutex<BTreeMap<String, PodError>>,
    /// pod -> typed reason the scripted probe reports (e.g. the
    /// informational `hibernated`). Absent ⇒ `None`.
    probe_reason: Mutex<BTreeMap<String, String>>,
}

#[derive(Debug)]
struct AppliedRecord {
    revision: u64,
    payload_digest: String,
    worker_count: usize,
}

impl MemoryPodChannel {
    /// Channel over a fixed pod inventory.
    pub fn new(pods: &[&str]) -> Self {
        let channel = Self::default();
        *channel.pods.lock().unwrap_or_else(|e| e.into_inner()) =
            pods.iter().map(|p| p.to_string()).collect();
        channel
    }

    /// Total `apply` calls for a pod (idempotence assertions).
    pub fn apply_calls(&self, pod_id: &str) -> usize {
        self.apply_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(pod_id)
            .copied()
            .unwrap_or(0)
    }

    /// Make the next `n` `apply` calls to `pod_id` fail with `Unreachable`.
    pub fn fail_next_applies(&self, pod_id: &str, n: usize) {
        self.fail_applies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pod_id.to_string(), n);
    }

    /// What the pod last applied (revision, digest, worker count) —
    /// test assertions on delivered intent.
    pub fn applied(&self, pod_id: &str) -> Option<(u64, String, usize)> {
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(pod_id)
            .map(|r| (r.revision, r.payload_digest.clone(), r.worker_count))
    }

    /// Script a pod's readiness flag (`/readyz` input).
    pub fn set_ready(&self, pod_id: &str, ready: bool) {
        self.ready
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pod_id.to_string(), ready);
    }

    /// MIG-1: script tenant-level failures for a pod's probe.
    pub fn set_tenant_failures(&self, pod_id: &str, failures: Vec<TenantFailure>) {
        self.tenant_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pod_id.to_string(), failures);
    }

    /// Script the `reason` a pod's probe reports (e.g. `hibernated`,
    /// `switch-failed`); independent of readiness.
    pub fn set_probe_reason(&self, pod_id: &str, reason: &str) {
        self.probe_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pod_id.to_string(), reason.to_string());
    }

    /// MIG-6: pin the (revision, digest) a pod's probe reports — the
    /// pod still processes deliveries normally but PROVES nothing
    /// about the new revision (stuck on an older generation, digest
    /// mismatch). `clear_probe_loaded` restores the applied record.
    pub fn set_probe_loaded(
        &self,
        pod_id: &str,
        revision: Option<u64>,
        digest: Option<String>,
    ) {
        self.probe_loaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pod_id.to_string(), (revision, digest));
    }

    /// Restore the natural applied-record probe answer for a pod.
    pub fn clear_probe_loaded(&self, pod_id: &str) {
        self.probe_loaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(pod_id);
    }

    /// MIG-8: make a pod's probe fail — the pod is unobservable
    /// (restarting, network partition). `clear_probe_error` restores
    /// the scripted result.
    pub fn set_probe_error(&self, pod_id: &str, error: PodError) {
        self.probe_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pod_id.to_string(), error);
    }

    /// Restore probing for a pod previously failed via
    /// [`MemoryPodChannel::set_probe_error`].
    pub fn clear_probe_error(&self, pod_id: &str) {
        self.probe_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(pod_id);
    }
}

#[async_trait::async_trait]
impl PodChannel for MemoryPodChannel {
    fn pods(&self) -> Vec<String> {
        self.pods.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    async fn apply(
        &self,
        pod_id: &str,
        envelope_raw: &[u8],
        assignment: &PodAssignment,
    ) -> Result<PodApplyResult, PodError> {
        let envelope_digest = hex::encode(Sha256::digest(envelope_raw));
        if !self.pods().iter().any(|p| p == pod_id) {
            return Err(PodError::UnknownPod);
        }
        *self
            .apply_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(pod_id.to_string())
            .or_insert(0) += 1;
        let mut failures = self.fail_applies.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(remaining) = failures.get_mut(pod_id) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(PodError::Unreachable);
            }
        }
        drop(failures);
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                pod_id.to_string(),
                AppliedRecord {
                    revision: assignment.revision,
                    payload_digest: format!("{}:{}", assignment.payload_digest, envelope_digest),
                    worker_count: assignment.workers.len(),
                },
            );
        Ok(PodApplyResult {
            loaded_revision: assignment.revision,
        })
    }

    async fn probe(&self, pod_id: &str) -> Result<PodProbeResult, PodError> {
        if !self.pods().iter().any(|p| p == pod_id) {
            return Err(PodError::UnknownPod);
        }
        if let Some(err) = self
            .probe_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(pod_id)
            .cloned()
        {
            return Err(err);
        }
        let applied = self.applied.lock().unwrap_or_else(|e| e.into_inner());
        let record = applied.get(pod_id);
        let pinned = self
            .probe_loaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(pod_id)
            .cloned();
        let ready = *self
            .ready
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(pod_id)
            .unwrap_or(&true);
        let tenant_failures = self
            .tenant_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(pod_id)
            .cloned()
            .unwrap_or_default();
        let (loaded_revision, loaded_digest) = match pinned {
            Some((rev, digest)) => (rev, digest),
            None => (
                record.map(|r| r.revision),
                record.map(|r| r.payload_digest.split(':').next().unwrap_or("").to_string()),
            ),
        };
        Ok(PodProbeResult {
            ready,
            loaded_revision,
            loaded_digest,
            reason: self
                .probe_reason
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(pod_id)
                .cloned(),
            tenant_failures,
        })
    }
}

/// Stub channel for the binary until the production pod transport is
/// decided — every call fails with [`PodError::Unavailable`] so the
/// controller reports `PARTIAL`/not-ready instead of a false `APPLIED`.
pub struct UnavailablePodChannel(pub &'static str);

#[async_trait::async_trait]
impl PodChannel for UnavailablePodChannel {
    fn pods(&self) -> Vec<String> {
        Vec::new()
    }
    async fn apply(
        &self,
        _pod_id: &str,
        _envelope_raw: &[u8],
        _assignment: &PodAssignment,
    ) -> Result<PodApplyResult, PodError> {
        Err(PodError::Unavailable)
    }
    async fn probe(&self, _pod_id: &str) -> Result<PodProbeResult, PodError> {
        Err(PodError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn informational_reason_never_becomes_failure_reason() {
        assert_eq!(super::failure_reason(Some("hibernated")), "pods-not-applied");
        assert_eq!(super::failure_reason(None), "pods-not-applied");
        assert_eq!(super::failure_reason(Some("switch-failed")), "switch-failed");
    }

    use super::*;

    fn raw() -> Vec<u8> {
        b"signed-envelope-bytes".to_vec()
    }

    fn assignment(pod: &str, revision: u64) -> PodAssignment {
        PodAssignment {
            pod_id: pod.into(),
            revision,
            payload_digest: "d".repeat(64),
            envelope_digest: "e".repeat(64),
            generated_at: "2026-11-08T00:00:00Z".into(),
            workers: vec![],
        }
    }

    #[tokio::test]
    async fn memory_channel_records_applies_and_probes() {
        let channel = MemoryPodChannel::new(&["p0", "p1"]);
        channel
            .apply("p0", &raw(), &assignment("p0", 7))
            .await
            .unwrap();
        let probe = channel.probe("p0").await.unwrap();
        assert_eq!(probe.loaded_revision, Some(7));
        assert!(probe.ready);
        assert_eq!(channel.apply_calls("p0"), 1);
        channel.set_ready("p0", false);
        assert!(!channel.probe("p0").await.unwrap().ready);
        assert!(matches!(
            channel
                .apply("ghost", &raw(), &assignment("ghost", 7))
                .await,
            Err(PodError::UnknownPod)
        ));
    }

    #[tokio::test]
    async fn scripted_apply_failures_recover() {
        let channel = MemoryPodChannel::new(&["p0"]);
        channel.fail_next_applies("p0", 2);
        assert!(channel
            .apply("p0", &raw(), &assignment("p0", 7))
            .await
            .is_err());
        assert!(channel
            .apply("p0", &raw(), &assignment("p0", 7))
            .await
            .is_err());
        assert!(channel
            .apply("p0", &raw(), &assignment("p0", 7))
            .await
            .is_ok());
    }
}
