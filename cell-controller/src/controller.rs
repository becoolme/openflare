//! `CellController` — the reconcile loop tying every trait together.
//!
//! One [`reconcile_once`] cycle:
//! pull → verify → assign → deliver (bounded retries per pod) → ACK.
//!
//! Fail-closed rules enforced here:
//! - any verification failure applies nothing, keeps the last applied
//!   snapshot, and ACKs `REJECTED` with a typed reason;
//! - a digest-identical replay is a no-op (no pod calls, no new ACK);
//! - `APPLIED` is reported only when EVERY assigned pod applied AND no
//!   pod reports `tenant_failures` (MIG-4) — otherwise `PARTIAL` with
//!   the failed pod ids and/or the typed tenant failures. One caveat:
//!   once an `APPLIED` for a revision LANDED server-side it is terminal
//!   there, so tenant failures surfacing afterwards still ride an
//!   `APPLIED` status (a `PARTIAL` replay would only conflict).
//! - the applied snapshot's `expiresAt` bounds routing authority: once it
//!   lapses with no newer publication the cell stops accepting new work
//!   (`/readyz` fails + cron triggers stop — the already-running pods own
//!   their own fail-closed enforcement; see the design doc).

use crate::ack::{AckError, AckReport, AckSink, AckStatus, FenceProof};
use crate::assign::{assign_workers, PodAssignment};
use crate::cron::{CronRunner, CronTickOutcome};
use crate::fence::{FenceAttestation, FenceState, FenceStore};
use crate::health::{new_health, PodHealth, ReadyState, SharedHealth};
use crate::lease::LeaseStore;
use crate::manifest::{ManifestApplier, ManifestError};
use crate::pod::{PodChannel, PodError, PodProbeResult, TenantFailure};
use crate::retry::RetryPlan;
use crate::source::{DesiredSource, SourceError};
use crate::verify::{extract_claimed, CellVerifier, RejectReason};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use snapshot_protocol::compute_desired::ComputeDesired;
use snapshot_protocol::envelope::Verified;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Injectable boundaries for [`CellController`].
pub struct CellControllerDeps {
    /// Latest-envelope pull.
    pub source: Arc<dyn DesiredSource>,
    /// Pod delivery + probe.
    pub channel: Arc<dyn PodChannel>,
    /// ACK reporting.
    pub ack: Arc<dyn AckSink>,
    /// Bounded retries for per-pod delivery.
    pub deliver_retry: RetryPlan,
    /// Bounded backoff for ACK RE-DELIVERY across reconcile ticks (T12/G1):
    /// an applied-but-unacked snapshot must keep retrying until the report
    /// lands or the plan is exhausted — then the failure is latched as an
    /// observable error instead of silently leaving the publication
    /// PENDING forever. The sink's own `RetryPlan` bounds attempts WITHIN
    /// one `report` call; this plan paces re-delivery BETWEEN ticks.
    pub ack_retry: RetryPlan,
    /// NetworkPolicy manifest sink — the T6 `render_network_policy`
    /// output reaches the cluster through this boundary ("delivered by T8").
    /// `None` for deployments without a cluster writer (MVP tests);
    /// a configured-but-failing applier degrades `readyz`.
    pub manifests: Option<Arc<dyn ManifestApplier>>,
    /// T12/G5: when `true`, a missing applier is a fail-closed egress
    /// gap — `apply_netpolicy` reports `ManifestError::Unavailable` and
    /// health exposes `egress-policy-unavailable` instead of silently
    /// no-op'ing. Production config must set this; tests may opt out.
    pub egress_required: bool,
    /// T13/H1: the runtime-pod NetworkPolicy spec to enforce. When
    /// `egress_required` is set, EVERY reconcile first ensures this
    /// policy is applied AND read-back verified — otherwise the cell
    /// ACKs `PARTIAL egress-policy-unavailable` and no tenant is
    /// delivered. `None` is only legal when egress is not required.
    pub netpolicy: Option<vrt_config::netpolicy::RuntimeNetPolicy>,
    /// Durable revision fence — `None` is permitted for tests but
    /// production must persist (restart without it re-opens the replay
    /// window). Persist failures are transient rejections, never ACKed
    /// terminal.
    pub fence: Option<Box<dyn FenceStore>>,
    /// T21: capability bits reported in every ACK — the control plane
    /// gates `crons`/ASSETS deploys on the cell's declared support.
    pub capabilities: crate::ack::AckCapabilities,
}

/// One reconcile cycle's verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// Control plane has no publication yet.
    NoPublication,
    /// T13/H1: egress policy not confirmed — tenants held back, a
    /// non-terminal PARTIAL ack with `egress-policy-unavailable` was sent.
    EgressUnavailable { revision: u64 },
    /// Pull failed after bounded retries — last applied state kept.
    PullFailed(SourceError),
    /// Envelope rejected — nothing applied, `REJECTED` ACK attempted.
    Rejected {
        /// Typed rejection class.
        reason: RejectReason,
        /// Whether the rejection ACK was delivered.
        acked: bool,
    },
    /// Identical replay — no pod calls, no new state.
    AlreadyApplied {
        /// Replayed revision.
        revision: u64,
    },
    /// Every assigned pod applied — `APPLIED` ACK attempted.
    Applied {
        /// Applied revision.
        revision: u64,
        /// Whether the ACK was delivered.
        acked: bool,
    },
    /// ≥1 pod failed delivery — `PARTIAL` ACK attempted; succeeded pods
    /// converged, failed pods keep their previous state.
    Partial {
        /// Attempted revision.
        revision: u64,
        /// Pods that did not apply.
        failed_pods: Vec<String>,
        /// Whether the ACK was delivered.
        acked: bool,
    },
}

#[derive(Debug)]
struct AppliedState {
    desired: ComputeDesired,
    envelope_digest: String,
    payload_digest: String,
    expires_at: DateTime<Utc>,
    /// Every assigned pod acknowledged the delivery. While false a
    /// digest-identical replay must re-enter convergence (per-pod
    /// `delivered` marks make it cheap) — otherwise a PARTIAL cell would
    /// never finish.
    fully_applied: bool,
    /// Whether the APPLIED/PARTIAL ack for THIS state reached the
    /// control plane (T12/G1). `AlreadyApplied` may only short-circuit
    /// when this is true — otherwise the cell re-sends the ack (and only
    /// the ack, no pod calls) until delivered or bounded out.
    ack_delivered: bool,
    /// Failed ack RE-delivery attempts so far (paces `next_ack_after`).
    ack_attempts: usize,
    /// Earliest tick allowed to re-send the ack (bounded backoff).
    next_ack_after: DateTime<Utc>,
    /// A 4xx made delivery terminal (server-side conflict that retries
    /// cannot fix) — latched here so the failure stays observable in
    /// health instead of being retried forever or silently dropped.
    ack_terminal: bool,
    /// MIG-1: tenant failures carried by the last DELIVERED report —
    /// `probe_tick` re-reports when a pod's set changes (a switch
    /// recovered, a new tenant failure appeared), so per-worker deploy
    /// status converges without a new revision.
    tenant_failures: Vec<TenantFailure>,
    /// MIG-4: an `APPLIED` report for THIS revision already reached the
    /// control plane. APPLIED is terminal server-side — afterwards a
    /// `PARTIAL` for the same revision only 409s, so late-surfacing
    /// tenant failures keep reporting under `APPLIED` + failures.
    applied_landed: bool,
    /// MIG-4: the report most recently ATTEMPTED for this revision —
    /// `resend_ack` replays it verbatim so a lost ack re-issues the
    /// verdict it carried (a pending `PARTIAL` + failures is never
    /// silently upgraded to `APPLIED` by resend).
    pending_report: Option<PendingReport>,
}

/// MIG-4: a report queued for delivery/replay.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingReport {
    /// Verdict attempted (`PARTIAL` while tenant failures / pod gaps
    /// exist; `APPLIED` on a clean converged state).
    status: AckStatus,
    /// Bounded reason string as sent.
    reason: Option<String>,
    /// Tenant failures the report carried (bounded).
    tenant_failures: Vec<TenantFailure>,
    /// Whether every assigned pod had applied — the APPLIED-fallback
    /// after a 4xx is only allowed when the PARTIAL was tenant-only.
    pods_ok: bool,
}

/// MIG-4 ack-status rule: `PARTIAL` whenever the revision did not fully
/// converge — pods failed OR tenant failures are still observed —
/// UNLESS an `APPLIED` for this revision already landed server-side
/// (terminal there; a `PARTIAL` replay only 409s). `pods_ok` means every
/// assigned pod applied the revision.
fn ack_status(pods_ok: bool, failures: &[TenantFailure], applied_landed: bool) -> AckStatus {
    if pods_ok && (failures.is_empty() || applied_landed) {
        AckStatus::Applied
    } else {
        AckStatus::Partial
    }
}

/// Per-pod delivery idempotence record.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Delivered {
    revision: u64,
    fingerprint: String,
}

#[derive(Debug, Default)]
struct ControllerState {
    applied: Option<AppliedState>,
    delivered: BTreeMap<String, Delivered>,
    assignments: BTreeMap<String, PodAssignment>,
    /// sha256 hex of the last successfully applied NetworkPolicy
    /// manifest — identical re-renders are a no-op (idempotent apply).
    applied_manifest_digest: Option<String>,
    /// MIG-8: per-pod LAST PROVEN tenant-failure contribution for the
    /// current applied revision — an unobservable pod (probe error,
    /// not-ready, wrong generation) is "unknown", NEVER "cleared":
    /// its previously-proven failures keep riding the aggregate until
    /// it proves the new state or leaves the inventory. Without this
    /// a vrt-host restart made the reported set shrink to [] and the
    /// control plane recorded a false clean recovery (2026-11-17).
    last_pod_failures: BTreeMap<String, Vec<TenantFailure>>,
    /// The (revision, payload_digest) `last_pod_failures` is scoped to
    /// — a new desired revision resets it so an older generation's
    /// failures are never attributed to the new one (MIG-6 invariant).
    last_pod_failures_key: Option<(u64, String)>,
}

/// The cell-side orchestrator.
pub struct CellController {
    cell_id: String,
    verifier: CellVerifier,
    deps: CellControllerDeps,
    state: Arc<Mutex<ControllerState>>,
    health: SharedHealth,
}

impl CellController {
    /// `verifier` holds the cell scope + trusted keys + revision fence.
    pub fn new(
        cell_id: impl Into<String>,
        verifier: CellVerifier,
        deps: CellControllerDeps,
    ) -> Self {
        let health = new_health();
        // T14/S3: running without egress isolation is an EXPLICIT,
        // visible posture — /healthz carries it forever; never silent.
        if !deps.egress_required {
            health
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .egress_policy_disabled = true;
        }
        Self {
            cell_id: cell_id.into(),
            verifier,
            deps,
            state: Arc::new(Mutex::new(ControllerState::default())),
            health,
        }
    }

    /// Shared `/healthz` + `/readyz` view.
    pub fn health(&self) -> SharedHealth {
        self.health.clone()
    }

    /// Converged readiness at `now` — every pod loaded the applied
    /// revision (the `/readyz/converged` predicate).
    pub fn readyz(&self, now: DateTime<Utc>) -> ReadyState {
        let health = self
            .health
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        crate::health::readiness(&health, now)
    }

    /// Serving readiness at `now` — the kubelet-facing `/readyz`
    /// predicate, which tolerates a bounded previous-revision window.
    pub fn serving_readyz(&self, now: DateTime<Utc>) -> ReadyState {
        let health = self
            .health
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        crate::health::serving_readiness(&health, now)
    }

    /// Cell id this controller serves.
    pub fn cell_id(&self) -> &str {
        &self.cell_id
    }

    /// `payloadDigest` of the last applied snapshot (ACK re-report after
    /// restart / fence restore diagnostics).
    pub fn applied_payload_digest(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .applied
            .as_ref()
            .map(|a| a.payload_digest.clone())
    }

    /// The last applied snapshot (for `cron_tick` / diagnostics).
    pub fn applied_revision(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .applied
            .as_ref()
            .map(|a| a.desired.revision)
    }

    /// Full reconcile cycle at `now`.
    pub async fn reconcile_once(&mut self, now: DateTime<Utc>) -> ReconcileOutcome {
        let fetched = match self.deps.source.fetch_detailed().await {
            Ok(Some(fetched)) => fetched,
            Ok(None) => {
                self.clear_error();
                return ReconcileOutcome::NoPublication;
            }
            Err(error) => {
                self.set_error("pull-failed");
                return ReconcileOutcome::PullFailed(error);
            }
        };
        let raw = fetched.body.as_slice();

        let verified = match self.verifier.verify(raw, now) {
            Ok(v) => v,
            Err(reason) => {
                crate::metrics::verify_failure(reason.code());
                // L9: transient rejections (expired, clock skew,
                // fence-persist) must NOT become terminal `REJECTED`
                // ACKs — the publication stays PENDING so a retried or
                // renewed envelope can still converge. Only
                // deterministic protocol violations are reported.
                let acked = if reason.is_transient() {
                    false
                } else {
                    let claimed = extract_claimed(raw);
                    match (
                        claimed.revision,
                        claimed.payload_digest,
                        claimed.envelope_digest,
                    ) {
                        (Some(revision), Some(digest), Some(envelope_digest)) => self
                            .report(
                                revision,
                                digest,
                                envelope_digest,
                                AckStatus::Rejected,
                                Some(reason.code().to_string()),
                                Vec::new(),
                                false,
                                now,
                            )
                            .await
                            .is_ok(),
                        _ => false,
                    }
                };
                self.set_error(reason.code());
                return ReconcileOutcome::Rejected { reason, acked };
            }
        };
        crate::metrics::desired_revision(verified.revision);

        // L10: persist the advanced revision fence before applying —
        // a restart without it would re-accept replayed older
        // snapshots. Failure is transient: nothing applied, no
        // terminal ACK; the in-memory fence already advanced
        // (monotone), so stale replays stay rejected regardless.
        // Issue #63: the attestation the control plane delivered out-of-band
        // belongs to this revision; the digests come from the verified doc.
        let attestation = fetched.fence_attestation.and_then(|header| {
            if header.revision != verified.revision {
                tracing::warn!(
                    revision = verified.revision,
                    header_revision = header.revision,
                    "fence attestation revision does not match the verified document — ignored"
                );
                return None;
            }
            Some(FenceAttestation {
                revision: verified.revision,
                payload_digest: verified.payload_digest.clone(),
                envelope_digest: verified.envelope_digest.clone(),
                attestation: header.attestation,
            })
        });
        if let Some(fence) = &self.deps.fence {
            let state = FenceState {
                revision: verified.revision,
                envelope_digest: verified.envelope_digest.clone(),
                attestation: attestation.clone(),
            };
            if let Err(e) = fence.store_full(&state) {
                let reason = RejectReason::FencePersist;
                self.set_error(reason.code());
                tracing::warn!(error = %e, "revision fence persist failed");
                return ReconcileOutcome::Rejected {
                    reason,
                    acked: false,
                };
            }
        }
        // In-memory copy used when building acks; identical to the persisted one.
        self.verifier.set_fence_attestation(attestation);

        // T13/H1: the NetworkPolicy must be APPLIED AND CONFIRMED before
        // any tenant traffic flows — including on replays that would
        // otherwise short-circuit. A missing/unconfirmed policy means
        // zero egress isolation: no APPLIED ack, no pod delivery.
        if let Err(()) = self.ensure_netpolicy(&verified, now).await {
            return ReconcileOutcome::EgressUnavailable {
                revision: verified.revision,
            };
        }

        // Idempotent replay: identical revision + envelope AND fully
        // delivered — replaying then produces zero pod calls. A PARTIAL
        // applied state deliberately falls through to convergence so the
        // failed pods get retried on every tick.
        // G1: an identical replay may only short-circuit when its ack
        // already LANDED. `fully_applied` alone is not enough — a lost
        // ack would otherwise leave the control-plane publication
        // PENDING forever. The resend path below re-issues ONLY the ack
        // (zero pod calls) under a bounded backoff; 4xx latches an
        // observable terminal error instead of retrying forever.
        {
            enum Replay {
                /// Delivered — clear any stale error and short-circuit.
                Short,
                /// Terminal ack failure — keep the latched error visible.
                Terminal,
                /// Re-issue the ack (bounded backoff reached).
                Resend,
                /// Not an identical replay, or backoff not yet reached.
                Skip,
            }
            let verdict = {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                match &state.applied {
                    Some(applied)
                        if applied.fully_applied
                            && applied.desired.revision == verified.revision
                            && applied.envelope_digest == verified.envelope_digest =>
                    {
                        if applied.ack_terminal {
                            Replay::Terminal
                        } else if applied.ack_delivered {
                            Replay::Short
                        } else if now >= applied.next_ack_after {
                            Replay::Resend
                        } else {
                            Replay::Skip
                        }
                    }
                    _ => Replay::Skip,
                }
            };
            match verdict {
                Replay::Short => {
                    self.clear_error();
                    return ReconcileOutcome::AlreadyApplied {
                        revision: verified.revision,
                    };
                }
                Replay::Terminal => {
                    return ReconcileOutcome::AlreadyApplied {
                        revision: verified.revision,
                    };
                }
                Replay::Resend => {
                    self.resend_ack(now).await;
                    return ReconcileOutcome::AlreadyApplied {
                        revision: verified.revision,
                    };
                }
                Replay::Skip => {}
            }
        }

        let outcome = self.converge(&verified, &raw, now).await;
        self.apply_ack(&verified, outcome.0, outcome.1, outcome.2, outcome.3, now)
            .await
    }

    /// Deliver the verified snapshot's assignment to every pod, then
    /// probe each pod for confirmation. Returns `(all_ok, failed_pods,
    /// reason, tenant_failures)` — T14/B1: APPLIED requires every
    /// assigned pod to report `ready && loaded_revision == target &&
    /// loaded_digest == payloadDigest`; delivery alone never suffices.
    /// MIG-1: `tenant_failures` aggregates every probe's tenant-level
    /// diagnostics (bounded + sanitized at the channel) — a ready pod
    /// can carry them.
    async fn converge(
        &self,
        verified: &Verified<ComputeDesired>,
        envelope_raw: &[u8],
        _now: DateTime<Utc>,
    ) -> (bool, Vec<String>, Option<String>, Vec<TenantFailure>) {
        let mut pods = self.deps.channel.pods();
        pods.sort();
        let has_workers = verified
            .payload
            .tenants
            .iter()
            .any(|t| !t.workers.is_empty());
        let mut table = assign_workers(&verified.payload, &pods);
        for assignment in table.values_mut() {
            assignment.payload_digest = verified.payload_digest.clone();
            // T14/B1: the host pairs envelope+assignment by this digest.
            assignment.envelope_digest = crate::file_channel::envelope_digest_of(envelope_raw);
            assignment.generated_at = _now.to_rfc3339();
        }

        // Record intent + update health view FIRST — even when PARTIAL,
        // the verified snapshot is the cell's current authority (readyz
        // still gates on per-pod loaded_revision == applied).
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            crate::metrics::applied_revision(verified.revision);
            state.applied = Some(AppliedState {
                desired: verified.payload.clone(),
                envelope_digest: verified.envelope_digest.clone(),
                payload_digest: verified.payload_digest.clone(),
                expires_at: verified.expires_at,
                fully_applied: false,
                ack_delivered: false,
                ack_attempts: 0,
                next_ack_after: DateTime::<Utc>::MIN_UTC,
                ack_terminal: false,
                tenant_failures: Vec::new(),
                applied_landed: false,
                pending_report: None,
            });
            let pods: std::collections::BTreeSet<&String> = table.keys().collect();
            state.delivered.retain(|pod, _| pods.contains(pod));
            state.assignments = table.clone();
            let key = (verified.revision, verified.payload_digest.clone());
            if state.last_pod_failures_key.as_ref() != Some(&key) {
                state.last_pod_failures.clear();
                state.last_pod_failures_key = Some(key);
            }
        }
        {
            let mut health = self.health.write().unwrap_or_else(|e| e.into_inner());
            health.applied_revision = Some(verified.revision);
            health.applied_expires_at = Some(verified.expires_at);
            health.assigned_pods = table.keys().cloned().collect();
        }

        if pods.is_empty() && has_workers {
            // Zero pods can never run the desired workers — report
            // PARTIAL honestly instead of a vacuous APPLIED.
            return (
                false,
                vec!["<no-pods>".to_string()],
                Some("pods-not-applied".to_string()),
                Vec::new(),
            );
        }

        let mut failed = Vec::new();
        for (pod_id, assignment) in &table {
            let fingerprint = pod_fingerprint(assignment);
            {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.delivered.get(pod_id)
                    == Some(&Delivered {
                        revision: verified.revision,
                        fingerprint: fingerprint.clone(),
                    })
                {
                    continue; // identical delivery — no downward action
                }
            }
            // T14/B1: deliver the RAW signed envelope + assignment —
            // the pod re-verifies; we never send an unsigned filtered
            // document.
            match self.deliver(pod_id, envelope_raw, assignment).await {
                Ok(()) => {
                    self.state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .delivered
                        .insert(
                            pod_id.clone(),
                            Delivered {
                                revision: verified.revision,
                                fingerprint,
                            },
                        );
                }
                Err(_) => failed.push(pod_id.clone()),
            }
        }
        // T14/B1: even a fully delivered cell is not APPLIED until
        // every pod PROVES it — probe confirms loaded revision +
        // payload digest + per-worker readiness. A pod reporting a
        // typed reason (assignment-mismatch, switch-failed, ...) is
        // surfaced through the PARTIAL ack reason instead of the
        // generic pods-not-applied.
        let mut reason: Option<String> = None;
        // MIG-1: tenant-level failures reported by pods — they ride the
        // ACK beside the pod-level verdict; a `ready` pod may still
        // carry them (old generation keeps serving).
        // MIG-4: a non-empty set forces the ACK to `PARTIAL` — a tenant
        // stuck in `SwitchFailed`/retrying is by definition not applied.
        // MIG-6: failures are only attributed to THIS revision when the
        // pod PROVED it loaded it (ready + loaded_revision + digest all
        // match). A pod still on an older generation reports that
        // generation's failure set — forwarding it blamed the new
        // revision for old failures (test-cluster repro 2026-11-16).
        let mut tenant_failures: Vec<TenantFailure> = Vec::new();
        // MIG-8: confirmed pods' fresh sets also update the retained
        // map so a later probe tick can distinguish "proven clear"
        // from "not proven this tick".
        let mut confirmed: Vec<(String, Vec<TenantFailure>)> = Vec::new();
        for (pod_id, _assignment) in &table {
            if failed.contains(pod_id) {
                continue;
            }
            match self.deps.channel.probe(pod_id).await {
                Ok(result)
                    if result.ready
                        && result.loaded_revision == Some(verified.revision)
                        && result.loaded_digest.as_deref()
                            == Some(verified.payload_digest.as_str()) =>
                {
                    // confirmed — pod-level; tenant failures still flow.
                    tenant_failures.extend(result.tenant_failures.iter().cloned());
                    confirmed.push((pod_id.clone(), result.tenant_failures));
                }
                Ok(result) => {
                    // MIG-6: this pod never proved it loaded the target
                    // revision — its `tenant_failures` belong to an
                    // older generation and are discarded. The pod
                    // itself still counts as not-applied.
                    if !result.tenant_failures.is_empty() {
                        tracing::warn!(
                            pod = %pod_id,
                            loaded_revision = ?result.loaded_revision,
                            target_revision = verified.revision,
                            "discarding tenant failures from a pod that did not load this revision"
                        );
                    }
                    failed.push(pod_id.clone());
                    // An informational reason (`hibernated`, ready=true)
                    // never masks the real failure: it collapses to
                    // `pods-not-applied`, and a later typed failure
                    // reason may still replace that generic one.
                    let this = crate::pod::failure_reason(result.reason.as_deref());
                    if reason.is_none() || reason.as_deref() == Some("pods-not-applied") {
                        reason = Some(this);
                    }
                }
                Err(_) => {
                    failed.push(pod_id.clone());
                    reason.get_or_insert_with(|| "pods-not-applied".to_string());
                }
            }
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for (pod_id, failures) in confirmed {
                state.last_pod_failures.insert(pod_id, failures);
            }
            // MIG-8: a pod that proved this revision's failures earlier
            // and is now unobservable (delivery or probe failure)
            // contributes its RETAINED set — same rule as probe_tick:
            // it proved nothing new, but it also did not prove
            // "cleared" (the ack stays PARTIAL pods-not-applied
            // regardless; this only keeps the diagnostics honest).
            for pod_id in &failed {
                if let Some(known) = state.last_pod_failures.get(pod_id) {
                    tenant_failures.extend(known.iter().cloned());
                }
            }
            if failed.is_empty() {
                if let Some(applied) = &mut state.applied {
                    applied.fully_applied = true;
                }
            }
        }
        (failed.is_empty(), failed, reason, tenant_failures)
    }

    async fn deliver(
        &self,
        pod_id: &str,
        envelope_raw: &[u8],
        assignment: &PodAssignment,
    ) -> Result<(), PodError> {
        let mut attempts = 0usize;
        let mut last = PodError::Unavailable;
        while self.deps.deliver_retry.can_retry(attempts) {
            if attempts > 0 {
                tokio::time::sleep(self.deps.deliver_retry.delay_for(attempts)).await;
            }
            attempts += 1;
            match self
                .deps
                .channel
                .apply(pod_id, envelope_raw, assignment)
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    last = e;
                    tracing::warn!(pod = %pod_id, attempts, "pod apply failed; retrying");
                }
            }
        }
        Err(last)
    }

    async fn apply_ack(
        &self,
        verified: &Verified<ComputeDesired>,
        all_ok: bool,
        failed_pods: Vec<String>,
        converge_reason: Option<String>,
        tenant_failures: Vec<TenantFailure>,
        now: DateTime<Utc>,
    ) -> ReconcileOutcome {
        // MIG-4: a converged pod set whose probes still report tenant
        // failures is NOT `APPLIED` — the ack stays `PARTIAL` (fresh
        // revision state ⇒ `applied_landed` is always false here).
        let failures_for_report =
            crate::ack::bounded_tenant_failures(tenant_failures);
        let status = ack_status(all_ok, &failures_for_report, false);
        let reason = match status {
            AckStatus::Applied => None,
            AckStatus::Partial if all_ok => Some("tenant-failed".to_string()),
            // T14/B1: prefer the pod-reported typed reason (still
            // bounded — reasons come back over the typed probe
            // contract; free-form strings are mapped to
            // `pods-not-applied` upstream).
            _ => Some(match converge_reason {
                Some(r) if r == "pods-not-applied" => {
                    format!("pods-not-applied:{}", failed_pods.join(","))
                }
                Some(r) => r,
                None => format!("pods-not-applied:{}", failed_pods.join(",")),
            }),
        };
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(applied) = &mut state.applied {
                if applied.desired.revision == verified.revision {
                    applied.pending_report = Some(PendingReport {
                        status,
                        reason: reason.clone(),
                        tenant_failures: failures_for_report.clone(),
                        pods_ok: all_ok,
                    });
                }
            }
        }
        let (acked, delivered_status) = match self
            .report(
                verified.revision,
                verified.payload_digest.clone(),
                verified.envelope_digest.clone(),
                status,
                reason,
                failures_for_report.clone(),
                all_ok,
                now,
            )
            .await
        {
            Ok(delivered) => (true, Some(delivered)),
            Err(e) => {
                self.note_ack_failure(verified.revision, e, now);
                (false, None)
            }
        };
        if acked {
            // Remember the last reported set + landed verdict —
            // `probe_tick` re-reports only when the tenant-failure set
            // actually CHANGES; `applied_landed` latches the terminal
            // APPLIED the control plane recorded.
            if let Some(applied) =
                &mut self.state.lock().unwrap_or_else(|e| e.into_inner()).applied
            {
                if applied.desired.revision == verified.revision {
                    applied.tenant_failures = failures_for_report.clone();
                    applied.ack_delivered = true;
                    applied.applied_landed |= delivered_status == Some(AckStatus::Applied);
                }
            }
        }
        if status == AckStatus::Applied {
            // Clear only when the ack LANDED — a terminal/exhausted ack
            // failure latches an observable error that must survive.
            if acked {
                self.clear_error();
            }
            ReconcileOutcome::Applied {
                revision: verified.revision,
                acked,
            }
        } else {
            if all_ok {
                // Pods applied but tenants report failures (MIG-4).
                self.set_error("tenant-failures");
            } else {
                self.set_error("partial-delivery");
            }
            ReconcileOutcome::Partial {
                revision: verified.revision,
                failed_pods,
                acked,
            }
        }
    }

    /// G1: record an ack-delivery failure against the CURRENT applied
    /// state — attempt count paces `next_ack_after` (bounded backoff);
    /// 4xx latches a terminal, observable error.
    fn note_ack_failure(&self, revision: u64, error: AckError, now: DateTime<Utc>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(applied) = &mut state.applied else {
            return;
        };
        if applied.desired.revision != revision {
            return;
        }
        if error.is_terminal() {
            applied.ack_terminal = true;
            drop(state);
            self.set_error("ack-terminal-rejected");
            tracing::error!(revision, "ack delivery returned 4xx — terminal, latched");
            return;
        }
        applied.ack_attempts = applied.ack_attempts.saturating_add(1);
        if !self.deps.ack_retry.can_retry(applied.ack_attempts) {
            // Bounded out: keep the error observable, stop hammering —
            // the publication stays PENDING server-side and ops sees a
            // latched cell error rather than silence.
            applied.ack_terminal = true;
            drop(state);
            self.set_error("ack-delivery-exhausted");
            tracing::error!(revision, "ack re-delivery attempts exhausted");
            return;
        }
        applied.next_ack_after = now
            + chrono::Duration::from_std(self.deps.ack_retry.delay_for(applied.ack_attempts))
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
        drop(state);
        tracing::warn!(revision, error = %error, "ack delivery failed; will re-send after backoff");
    }

    /// G1: re-issue the ack for an already-applied snapshot — no pod
    /// calls. MIG-4: replays the most recently ATTEMPTED report
    /// verbatim — a `PARTIAL` that failed mid-flight stays `PARTIAL`;
    /// it is never silently upgraded to `APPLIED`.
    async fn resend_ack(&self, now: DateTime<Utc>) {
        let (revision, payload_digest, envelope_digest, pending) = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match &state.applied {
                Some(a) => (
                    a.desired.revision,
                    a.payload_digest.clone(),
                    a.envelope_digest.clone(),
                    a.pending_report.clone(),
                ),
                None => return,
            }
        };
        let Some(pending) = pending else {
            return;
        };
        match self
            .report(
                revision,
                payload_digest,
                envelope_digest,
                pending.status,
                pending.reason,
                pending.tenant_failures.clone(),
                pending.pods_ok,
                now,
            )
            .await
        {
            Ok(delivered) => {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(applied) = &mut state.applied {
                    if applied.desired.revision == revision {
                        applied.ack_delivered = true;
                        applied.tenant_failures = pending.tenant_failures;
                        applied.applied_landed |= delivered == AckStatus::Applied;
                    }
                }
            }
            Err(e) => self.note_ack_failure(revision, e, now),
        }
    }

    /// Returns the status the server actually ACCEPTED (a 4xx may
    /// fall back to a degraded shape — see below).
    #[allow(clippy::too_many_arguments)]
    async fn report(
        &self,
        revision: u64,
        payload_digest: String,
        envelope_digest: String,
        status: AckStatus,
        reason: Option<String>,
        tenant_failures: Vec<TenantFailure>,
        pods_ok: bool,
        now: DateTime<Utc>,
    ) -> Result<AckStatus, AckError> {
        crate::metrics::ack(match status {
            AckStatus::Applied => "applied",
            AckStatus::Partial => "partial",
            AckStatus::Rejected => "rejected",
        });
        // MIG-6: the wire field is `Option` — `Some(set)` always
        // serializes `tenantFailures` (an explicit `[]` is the MIG-6
        // clear; the server persists it as "no reported failures")
        // while `None` omits the key entirely for the pre-MIG-1
        // strip-and-retry below.
        // Issue #63: when the persisted fence is AHEAD of the revision being
        // acked (typically after a control-plane restore), prove it with the
        // attestation the plane delivered for the fence document.
        let fence_proof = {
            let (fence_revision, _) = self.verifier.fence();
            if fence_revision > revision {
                self.verifier
                    .fence_attestation()
                    .map(|attestation| FenceProof {
                        revision: attestation.revision,
                        payload_digest: attestation.payload_digest.clone(),
                        envelope_digest: attestation.envelope_digest.clone(),
                        attestation: attestation.attestation.clone(),
                    })
            } else {
                None
            }
        };
        let build = |tenant_failures: Option<Vec<TenantFailure>>| AckReport {
            revision,
            payload_digest: payload_digest.clone(),
            envelope_digest: envelope_digest.clone(),
            // F5: report the persisted fence so a restored DB lifts
            // its counter — `verifier.fence()` is the in-memory one;
            // identical once `store()` succeeded.
            fence_revision: self.verifier.fence().0,
            fence_proof: fence_proof.clone(),
            status,
            // Bounded — the server rejects a whole report whose
            // reason exceeds 256 units (RV-R68).
            reason: crate::ack::bounded_reason(reason.clone()),
            applied_at: now,
            capabilities: Some(self.deps.capabilities),
            tenant_failures,
        };
        let report = build(Some(tenant_failures));
        match self.deps.ack.report(&report).await {
            // MIG-1/MIG-6: a pre-MIG-1 control plane's STRICT schema
            // rejects the unknown `tenantFailures` key with a 400 — a
            // verdict the cell must not latch as terminal over a
            // feature bit. Strip the field and deliver the classic
            // shape ONCE (bounded — the stripped report either lands
            // or returns the verdict the retry would have produced
            // anyway). The strip is gated on EXACTLY 400: a 409 is
            // `VRT_ACK_TERMINAL`/digest mismatch — dropping the
            // diagnostics there would fail open and lose the real
            // failure set.
            Err(AckError::Status(400)) if report.tenant_failures.is_some() => {
                tracing::warn!(
                    revision,
                    "control plane rejected tenantFailures (400) — retrying ack without them (pre-MIG-1 peer)"
                );
                match self.deps.ack.report(&build(None)).await {
                    Ok(()) => Ok(report.status),
                    // MIG-4: still 4xx after stripping means the
                    // publication is already terminal server-side —
                    // if an APPLIED was latched before this controller
                    // learned about the failure, a tenant-only PARTIAL
                    // can still deliver its diagnostics under APPLIED
                    // (never claim APPLIED when PODS failed — that
                    // would be a false success).
                    Err(_) if pods_ok => {
                        self.applied_fallback(build, report.tenant_failures.clone())
                            .await
                    }
                    Err(e) => Err(e),
                }
            }
            Err(AckError::Status(code))
                if (400..500).contains(&code)
                    && status == AckStatus::Partial
                    && pods_ok =>
            {
                self.applied_fallback(build, report.tenant_failures.clone())
                    .await
            }
            Ok(()) => Ok(report.status),
            Err(e) => Err(e),
        }
    }

    /// MIG-4: a tenant-only `PARTIAL` bounced off a 4xx — the likely
    /// cause is a server-side APPLIED already latched for this revision
    /// (e.g. recorded before this controller observed the failure, or
    /// delivered before a restart). APPLIED is terminal server-side, so
    /// the only way to still deliver the tenant diagnostics is under an
    /// `APPLIED` status — the failure set keeps it honest. Bounded: at
    /// most two more attempts (with failures, then stripped ONLY when a
    /// pre-MIG-1 peer rejects the key with a 400 — MIG-6).
    /// `build` already closes over revision/digests.
    async fn applied_fallback(
        &self,
        build: impl Fn(Option<Vec<TenantFailure>>) -> AckReport,
        tenant_failures: Option<Vec<TenantFailure>>,
    ) -> Result<AckStatus, AckError> {
        let mut applied = build(tenant_failures);
        applied.status = AckStatus::Applied;
        applied.reason = None;
        tracing::warn!(
            revision = applied.revision,
            "tenant-only PARTIAL ack hit a terminal server state — retrying as APPLIED + tenantFailures"
        );
        match self.deps.ack.report(&applied).await {
            Ok(()) => Ok(AckStatus::Applied),
            // MIG-6: only a 400 schema rejection gets the stripped
            // retry — a 409 must surface as the terminal verdict it is,
            // never degrade into a false clean APPLIED.
            Err(AckError::Status(400)) => {
                let mut stripped = applied;
                stripped.tenant_failures = None;
                match self.deps.ack.report(&stripped).await {
                    Ok(()) => Ok(AckStatus::Applied),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Cron scheduling cycle: lease-gated trigger dispatch over the last
    /// applied snapshot (expired desired ⇒ no authority ⇒ no triggers).
    pub async fn cron_tick<L: LeaseStore>(
        &self,
        runner: &mut CronRunner<L>,
        now: DateTime<Utc>,
    ) -> CronTickOutcome {
        let (desired, expires_at, assignments) = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            (
                state.applied.as_ref().map(|a| a.desired.clone()),
                state.applied.as_ref().map(|a| a.expires_at),
                state.assignments.clone(),
            )
        };
        runner
            .tick(desired.as_ref(), expires_at, &assignments, now)
            .await
    }

    /// T37b: hand the queue-dispatch task a cheap handle into the
    /// applied-state lock (the reconcile loop needs `&mut self`, so
    /// the controller itself is not `Clone`). One `snapshot()` per
    /// ~1s tick; `poll_interval_secs` is too coarse for
    /// `maxBatchTimeout` ≥ 1s semantics.
    pub fn dispatch_handle(&self) -> QueueDispatchHandle {
        QueueDispatchHandle {
            state: self.state.clone(),
        }
    }

    /// Apply a rendered NetworkPolicy manifest through
    /// [`ManifestApplier`]. Idempotent per content: an identical
    /// re-render produces zero applier calls. A failed apply is
    /// recorded in health (`netpolicy_error` → `readyz` 503) and
    /// returned to the caller — it never produces a fake success and
    /// does not touch reconcile state.
    ///
    /// No applier configured → `Ok(())` with nothing recorded (the
    /// caller's `None` means "no cluster writer by design").
    /// T13/H1: ensure the configured NetworkPolicy is applied AND
    /// confirmed cluster-side before tenants are delivered. Reports a
    /// non-terminal `PARTIAL egress-policy-unavailable` ACK so the
    /// control plane sees the typed condition and keeps retrying;
    /// returns `Err(())` when the environment is not confirmed safe.
    async fn ensure_netpolicy(
        &self,
        verified: &Verified<ComputeDesired>,
        now: DateTime<Utc>,
    ) -> Result<(), ()> {
        let Some(spec) = &self.deps.netpolicy else {
            // No policy configured at all.
            if self.deps.egress_required {
                self.health
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .netpolicy_error = Some("egress-policy-unavailable".to_string());
                let _ = self
                    .report(
                        verified.revision,
                        verified.payload_digest.clone(),
                        verified.envelope_digest.clone(),
                        AckStatus::Partial,
                        Some("egress-policy-unavailable".to_string()),
                        Vec::new(),
                        false,
                        now,
                    )
                    .await;
                self.set_error("egress-policy-unavailable");
                return Err(());
            }
            return Ok(());
        };
        let manifest = match vrt_config::netpolicy::render_network_policy(spec) {
            Ok(m) => m,
            Err(_) => {
                // Spec is validated at config load; a render failure here
                // is a bug — fail closed either way.
                self.health
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .netpolicy_error = Some("egress-policy-unavailable".to_string());
                return Err(());
            }
        };
        // apply (digest-idempotent) then READ BACK — write success is
        // not proof the policy exists cluster-side.
        if self.apply_netpolicy(&manifest).await.is_err() {
            let _ = self
                .report(
                    verified.revision,
                    verified.payload_digest.clone(),
                    verified.envelope_digest.clone(),
                    AckStatus::Partial,
                    Some("egress-policy-unavailable".to_string()),
                    Vec::new(),
                    false,
                        now,
                )
                .await;
            self.set_error("egress-policy-unavailable");
            return Err(());
        }
        let digest = hex::encode(Sha256::digest(manifest.as_bytes()));
        let confirmed = match &self.deps.manifests {
            Some(applier) => matches!(applier.verify(&digest).await, Ok(true)),
            None => false,
        };
        if !confirmed {
            // RV-R48: the digest only memoizes "applied AND verified". When
            // read-back fails (policy deleted / drifted cluster-side) the
            // memo is stale — clear it so the NEXT reconcile re-applies
            // instead of returning Ok on an identical digest forever.
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .applied_manifest_digest = None;
            self.health
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .netpolicy_error = Some("egress-policy-unconfirmed".to_string());
            let _ = self
                .report(
                    verified.revision,
                    verified.payload_digest.clone(),
                    verified.envelope_digest.clone(),
                    AckStatus::Partial,
                    Some("egress-policy-unavailable".to_string()),
                    Vec::new(),
                    false,
                        now,
                )
                .await;
            self.set_error("egress-policy-unavailable");
            return Err(());
        }
        self.health
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .netpolicy_error = None;
        Ok(())
    }

    pub async fn apply_netpolicy(&self, manifest: &str) -> Result<(), ManifestError> {
        let digest = hex::encode(Sha256::digest(manifest.as_bytes()));
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.applied_manifest_digest.as_deref() == Some(digest.as_str()) {
                return Ok(()); // already applied — idempotent replay
            }
        }
        let Some(applier) = &self.deps.manifests else {
            // T12/G5 fail-closed: production requires an applier; none
            // wired means egress isolation is NOT in place — surface it
            // as a typed error + health diagnostic, never a fake Ok.
            if self.deps.egress_required {
                self.health
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .netpolicy_error = Some("egress-policy-unavailable".to_string());
                return Err(ManifestError::Unavailable);
            }
            return Ok(());
        };
        match applier.apply(manifest).await {
            Ok(()) => {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .applied_manifest_digest = Some(digest);
                self.health
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .netpolicy_error = None;
                Ok(())
            }
            Err(e) => {
                self.health
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .netpolicy_error = Some("apply-failed".to_string());
                Err(e)
            }
        }
    }

    /// Probe every known pod and refresh the shared health view.
    /// MIG-1: also folds each pod's tenant-failure set into the cell
    /// aggregate — when it CHANGES for a fully-applied, ack-delivered
    /// revision the APPLIED report is re-issued (bounded: one report
    /// per change, identical sets are a no-op) so per-worker deploy
    /// status converges to truth without a new revision.
    pub async fn probe_tick(&self, now: DateTime<Utc>) {
        // MIG-6: only pods that PROVED they loaded the current revision
        // may attribute tenant failures to it — same rule as `converge`.
        let current = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .applied
                .as_ref()
                .map(|a| (a.desired.revision, a.payload_digest.clone()))
        };
        let pod_ids = self.deps.channel.pods();
        let mut results = Vec::with_capacity(pod_ids.len());
        for pod_id in &pod_ids {
            results.push((pod_id.clone(), self.deps.channel.probe(pod_id).await));
        }
        // `Some(set)` = the pod PROVED the applied revision this tick
        // (its fresh failure set is authoritative); `None` = the pod
        // is unobservable — probe error, not-ready, or still on an
        // older generation — its failure set is UNKNOWN, never empty.
        let mut observed: Vec<(String, Option<Vec<TenantFailure>>)> =
            Vec::with_capacity(results.len());
        {
            let mut health = self.health.write().unwrap_or_else(|e| e.into_inner());
            for (pod_id, result) in results {
                // Proof of the applied revision only moves forward; an
                // unproven tick keeps the previous anchor so the
                // stale-serving grace ages from the last real proof.
                let previous_current_at = health.pods.get(&pod_id).and_then(|p| p.last_current_at);
                let entry = match result {
                    Ok(PodProbeResult {
                        ready,
                        loaded_revision,
                        loaded_digest,
                        tenant_failures: failures,
                        ..
                    }) => {
                        let loaded_current = ready
                            && matches!(&current, Some((rev, digest))
                                if loaded_revision == Some(*rev)
                                    && loaded_digest.as_deref() == Some(digest.as_str()));
                        observed.push((pod_id.clone(), loaded_current.then_some(failures)));
                        PodHealth {
                            pod_id: pod_id.clone(),
                            ready,
                            loaded_revision,
                            probe_failed: false,
                            last_current_at: if loaded_current {
                                Some(now)
                            } else {
                                previous_current_at
                            },
                        }
                    }
                    Err(_) => {
                        observed.push((pod_id.clone(), None));
                        PodHealth {
                            pod_id: pod_id.clone(),
                            ready: false,
                            loaded_revision: None,
                            probe_failed: true,
                            last_current_at: previous_current_at,
                        }
                    }
                };
                health.pods.insert(pod_id, entry);
            }
        }
        // MIG-8: fold confirmed + retained sets into the aggregate —
        // an unconfirmed pod contributes its LAST PROVEN set, so a
        // restarting vrt-host can never masquerade as "failures
        // cleared" (false APPLIED + tenantFailures: [] cleared real
        // exclusion marks in the incident).
        let mut tenant_failures = Vec::new();
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            // The applied revision may have advanced while this tick's
            // probes were in flight (a stale `current`): folding now
            // would attribute the OLD revision's sets to the NEW
            // revision's report — the MIG-6 misattribution class.
            // The health map above is already honest pod-level truth;
            // skip the fold and let the NEXT tick judge the new
            // revision — delay, never a wrong answer.
            let applied_key = state
                .applied
                .as_ref()
                .map(|a| (a.desired.revision, a.payload_digest.clone()));
            if applied_key != current {
                return;
            }
            if state.last_pod_failures_key != current {
                state.last_pod_failures.clear();
                state.last_pod_failures_key = current.clone();
            }
            // Pods that left the inventory cannot still be blamed.
            state
                .last_pod_failures
                .retain(|id, _| pod_ids.iter().any(|p| p == id));
            for (pod_id, confirmed) in observed {
                match confirmed {
                    Some(failures) => {
                        tenant_failures.extend(failures.iter().cloned());
                        state.last_pod_failures.insert(pod_id, failures);
                    }
                    None => {
                        if let Some(known) = state.last_pod_failures.get(&pod_id) {
                            tenant_failures.extend(known.iter().cloned());
                        }
                    }
                }
            }
        }
        self.report_tenant_failure_change(tenant_failures, now).await;
    }

    /// MIG-1/MIG-4: re-report when the cell's tenant-failure set changed
    /// since the last delivered report. Only meaningful for a
    /// fully-applied revision — a PARTIAL state re-converges (and
    /// re-reports) on every reconcile anyway. MIG-4: the re-reported
    /// status is derived — `PARTIAL` while failures exist, `APPLIED`
    /// only when the set cleared (or an `APPLIED` for this revision
    /// already landed server-side, which is terminal there). This is
    /// also the PARTIAL→APPLIED recovery path for a tenant that
    /// recovered.
    async fn report_tenant_failure_change(
        &self,
        tenant_failures: Vec<TenantFailure>,
        now: DateTime<Utc>,
    ) {
        // Bound FIRST — the comparison is against the last reported
        // (already bounded) set; an over-limit raw set must not
        // re-report on every tick.
        let failures_for_report = crate::ack::bounded_tenant_failures(tenant_failures);
        let params = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match &state.applied {
                Some(a)
                    if a.fully_applied
                        && a.ack_delivered
                        && !a.ack_terminal
                        && a.tenant_failures != failures_for_report =>
                {
                    Some((
                        a.desired.revision,
                        a.payload_digest.clone(),
                        a.envelope_digest.clone(),
                        ack_status(true, &failures_for_report, a.applied_landed),
                    ))
                }
                _ => None,
            }
        };
        let Some((revision, payload_digest, envelope_digest, status)) = params else {
            return;
        };
        let reason = (status == AckStatus::Partial).then(|| "tenant-failed".to_string());
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(applied) = &mut state.applied {
                if applied.desired.revision == revision {
                    applied.pending_report = Some(PendingReport {
                        status,
                        reason: reason.clone(),
                        tenant_failures: failures_for_report.clone(),
                        pods_ok: true,
                    });
                }
            }
        }
        match self
            .report(
                revision,
                payload_digest,
                envelope_digest,
                status,
                reason,
                failures_for_report.clone(),
                true,
                now,
            )
            .await
        {
            Ok(delivered) => {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(applied) = &mut state.applied {
                    if applied.desired.revision == revision {
                        applied.tenant_failures = failures_for_report;
                        applied.applied_landed |= delivered == AckStatus::Applied;
                    }
                }
            }
            Err(e) => self.note_ack_failure(revision, e, now),
        }
    }

    fn set_error(&self, code: &'static str) {
        self.health
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .last_error = Some(code.to_string());
    }

    fn clear_error(&self) {
        self.health
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .last_error = None;
    }
}

/// Per-pod delivery idempotence key — revision + worker-set digest.
fn pod_fingerprint(assignment: &PodAssignment) -> String {
    let mut hasher = Sha256::new();
    hasher.update(assignment.revision.to_be_bytes());
    for w in &assignment.workers {
        hasher.update(w.tenant_id.as_bytes());
        hasher.update([0]);
        hasher.update(w.worker_id.as_bytes());
        hasher.update([0xff]);
    }
    hex::encode(hasher.finalize())
}

/// T37b: cloneable view of the applied-state lock for the dedicated
/// queue-dispatch task.
pub struct QueueDispatchHandle {
    state: Arc<Mutex<ControllerState>>,
}

impl QueueDispatchHandle {
    /// Applied desired + expiry + pod table under one lock —
    /// `None`/expired desired ⇒ the dispatcher has no authority
    /// (lanes clear, deliveries stop).
    pub fn snapshot(
        &self,
    ) -> (
        Option<snapshot_protocol::compute_desired::ComputeDesired>,
        Option<DateTime<Utc>>,
        BTreeMap<String, crate::assign::PodAssignment>,
    ) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (
            state.applied.as_ref().map(|a| a.desired.clone()),
            state.applied.as_ref().map(|a| a.expires_at),
            state.assignments.clone(),
        )
    }
}
