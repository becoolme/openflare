//! In-memory end-to-end: publish → pull → verify → assign → ACK.
//! Plus the failure matrix the task requires (see each test name).

use cell_controller::ack::{AckStatus, MemoryAckSink};
use cell_controller::controller::{CellController, CellControllerDeps, ReconcileOutcome};
use cell_controller::cron::{CronRunner, MemoryCronInvoker, MemoryTriggerLog};
use cell_controller::health::ReadyState;
use cell_controller::lease::MemoryLeaseStore;
use cell_controller::pod::MemoryPodChannel;
use cell_controller::retry::RetryPlan;
use cell_controller::source::{MemoryDesiredSource, SourceError};
use cell_controller::verify::{CellVerifier, RejectReason};
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use snapshot_protocol::envelope::{sign, SignParams, SignedSnapshot, SigningKey};
use std::sync::Arc;

fn key() -> SigningKey {
    SigningKey::from_seed(&[0x42; 32]).unwrap()
}

fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-08T00:00:00.000Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn worker(worker_id: &str, cron_expr: Option<&str>) -> serde_json::Value {
    json!({
        "workerId": worker_id,
        "name": worker_id,
        "activeVersion": {
            "versionId": format!("vrt_{worker_id}"),
            "artifactDigest": "a".repeat(64),
            "compatibilityDate": "2024-01-01",
            "compatibilityFlags": []
        },
        "bindings": [],
        "crons": cron_expr.map(|e| vec![json!({"scheduleId": format!("c:{worker_id}"), "expression": e})]).unwrap_or_default(),
        "quotas": {"cpuMs": 10, "memoryMb": 32, "outboundConnections": 4, "subrequests": 10},
        "routes": []
    })
}

fn publish(
    cell: &str,
    revision: u64,
    workers: &[serde_json::Value],
    ttl_secs: i64,
) -> SignedSnapshot {
    let payload = json!({
        "schemaVersion": 1,
        "cellId": cell,
        "revision": revision,
        "tenants": [{"tenantId": "team-a", "workers": workers}]
    });
    sign(
        &SignParams {
            kind: "compute.desired",
            scope: &format!("cell:{cell}"),
            revision,
            issued_at: now(),
            expires_at: now() + Duration::seconds(ttl_secs),
            payload: &payload,
        },
        &key(),
    )
    .unwrap()
}

struct Rig {
    controller: CellController,
    source: Arc<MemoryDesiredSource>,
    channel: Arc<MemoryPodChannel>,
    ack: Arc<MemoryAckSink>,
}

fn rig(pods: &[&str]) -> Rig {
    let source = Arc::new(MemoryDesiredSource::new(Ok(None)));
    let channel = Arc::new(MemoryPodChannel::new(pods));
    let ack = Arc::new(MemoryAckSink::default());
    Rig {
        controller: CellController::new(
            "cell-1",
            CellVerifier::new("cell-1", vec![key().trusted_key()]),
            CellControllerDeps {
                source: source.clone(),
                channel: channel.clone(),
                ack: ack.clone(),
                deliver_retry: RetryPlan {
                    max_attempts: 2,
                    base_delay: std::time::Duration::ZERO,
                    max_delay: std::time::Duration::ZERO,
                    jitter_seed: 0,
                },
                ack_retry: RetryPlan {
                    max_attempts: 5,
                    base_delay: std::time::Duration::ZERO,
                    max_delay: std::time::Duration::ZERO,
                    jitter_seed: 0,
                },
                manifests: None,
                egress_required: false,
                netpolicy: None,
                fence: None,
                capabilities: Default::default(),
            },
        ),
        source,
        channel,
        ack,
    }
}

#[tokio::test]
async fn end_to_end_publish_pull_verify_assign_ack() {
    let mut rig = rig(&["p0", "p1"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None), worker("w2", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));

    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Applied {
            revision: 7,
            acked: true
        }
    );

    // Both pods delivered exactly once; workers partitioned.
    let total_calls: usize = ["p0", "p1"]
        .iter()
        .map(|p| rig.channel.apply_calls(p))
        .sum();
    assert_eq!(total_calls, 2);
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, AckStatus::Applied);
    assert_eq!(sent[0].revision, 7);
    assert_eq!(sent[0].payload_digest, snapshot.payload_digest);
    // T11/F11+F5: the ack binds the full envelope digest AND reports the
    // persisted fence — the control plane compares the former to its stored
    // publication and uses the latter to lift a restored counter.
    assert_eq!(
        Some(sent[0].envelope_digest.as_str()),
        snapshot_protocol::compute_envelope_digest(&snapshot.bytes).as_deref()
    );
    assert_eq!(sent[0].fence_revision, 7);
}

/// MIG-1/MIG-4: a pod that landed the revision but reports tenant-level
/// failures stays pod-Ready (one bad tenant never drains the cell) —
/// but the ACK must NOT claim APPLIED: a latched `SwitchFailed` tenant
/// is by definition not applied. MIG-4 reports `PARTIAL` + the typed
/// `tenant_failures` so the control plane marks exactly the failed
/// workers instead of celebrating a false success.
#[tokio::test]
async fn tenant_failure_makes_ack_partial_and_rides_the_ack() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0", "p1"]);
    let snapshot = publish(
        "cell-1",
        7,
        &[worker("w1", None), worker("w2", None)],
        3600,
    );
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    // p1 reports the revision loaded (ready) but one tenant failed to
    // switch — old generation keeps serving on the pod.
    rig.channel.set_tenant_failures(
        "p1",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w2".to_string()],
            code: "switch-failed".to_string(),
            detail: Some("config load error".to_string()),
        }],
    );
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Partial {
            revision: 7,
            failed_pods: Vec::new(),
            acked: true
        },
        "tenant-level failure must report PARTIAL, never APPLIED (MIG-4)"
    );
    // Readiness is still Ready: the pod processed the revision (probe
    // first — health derives from the latest probe results).
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.controller.readyz(now()), ReadyState::Ready);
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, AckStatus::Partial);
    assert_eq!(sent[0].reason.as_deref(), Some("tenant-failed"));
    assert_eq!(sent[0].tenant_failures.as_deref().unwrap_or(&[]).len(), 1);
    let f = &sent[0].tenant_failures.as_ref().expect("failures present")[0];
    assert_eq!(f.tenant, "team-a");
    assert_eq!(f.worker_ids, vec!["w2".to_string()]);
    assert_eq!(f.code, "switch-failed");
    // The identical replay short-circuits — no resend while the
    // reported set is unchanged (PARTIAL is already delivered).
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    assert_eq!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::AlreadyApplied { revision: 7 }
    );
    assert_eq!(rig.ack.sent().len(), 1, "unchanged failure set must not resend");
}

/// MIG-4: probe_tick is the PARTIAL→APPLIED recovery path — when the
/// pod's tenant-failure set clears, the next probe re-reports APPLIED
/// (the pod was never re-converged: same revision, zero pod calls).
#[tokio::test]
async fn tenant_failure_recovery_re_reports_applied_via_probe_tick() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None), worker("w2", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w2".to_string()],
            code: "config-load".to_string(),
            detail: None,
        }],
    );
    assert!(matches!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::Partial { acked: true, .. }
    ));
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Partial);
    // Failure clears on the pod (the tenant recovered or converged) —
    // the next probe tick must lift the ack to APPLIED.
    rig.channel.set_tenant_failures("p0", Vec::new());
    rig.controller.probe_tick(now()).await;
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].status, AckStatus::Applied);
    assert!(sent[1].tenant_failures.as_deref().unwrap_or(&[]).is_empty());
    // No new pod deliveries happened for the recovery report.
    assert_eq!(rig.channel.apply_calls("p0"), 1);
}

/// MIG-4: an unchanged failure set must not re-report every tick, and
/// a CHANGED set re-reports as PARTIAL (never APPLIED while failures
/// remain).
#[tokio::test]
async fn probe_tick_re_reports_partial_for_changed_failure_sets() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None), worker("w2", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec![],
            code: "switch-failed".to_string(),
            detail: None,
        }],
    );
    rig.controller.reconcile_once(now()).await;
    // Identical set → no report.
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.ack.sent().len(), 1);
    // Changed set → PARTIAL again (failure detail changed).
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "switch-failed".to_string(),
            detail: Some("still broken".to_string()),
        }],
    );
    rig.controller.probe_tick(now()).await;
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].status, AckStatus::Partial);
    assert_eq!(sent[1].tenant_failures.as_ref().expect("failures present")[0].worker_ids, vec!["w1".to_string()]);
}

/// MIG-4: once an APPLIED for the revision LANDED server-side the
/// verdict is terminal there — a tenant failure surfacing afterwards
/// (e.g. a switch that broke on the next desired-independent attempt)
/// still rides an APPLIED + failures report; a PARTIAL replay would
/// only 409.
#[tokio::test]
async fn post_applied_tenant_failure_stays_applied_with_failures() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    // Clean APPLIED first.
    assert_eq!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::Applied { revision: 7, acked: true }
    );
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Applied);
    // A tenant failure surfaces after APPLIED — the report stays
    // APPLIED (server-side terminal) but carries the failure set.
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "crashed".to_string(),
            detail: None,
        }],
    );
    rig.controller.probe_tick(now()).await;
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].status, AckStatus::Applied);
    assert_eq!(sent[1].tenant_failures.as_deref().unwrap_or(&[]).len(), 1);
    // And clears back to a bare APPLIED when the failure heals.
    rig.channel.set_tenant_failures("p0", Vec::new());
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.ack.sent().len(), 3);
    assert_eq!(rig.ack.sent()[2].status, AckStatus::Applied);
    assert!(rig.ack.sent()[2].tenant_failures.as_deref().unwrap_or(&[]).is_empty());
}

/// MIG-4: a PARTIAL ack lost mid-flight must be resent AS PARTIAL —
/// never silently upgraded to APPLIED by the resend path.
#[tokio::test]
async fn lost_partial_ack_resends_as_partial() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None), worker("w2", None)], 3600);
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w2".to_string()],
            code: "switch-failed".to_string(),
            detail: None,
        }],
    );
    rig.ack.fail_next(1); // first ack lost in transport
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Partial { revision: 7, failed_pods: Vec::new(), acked: false }
    );
    assert_eq!(rig.ack.sent().len(), 0);
    // Next tick replays the SAME envelope → resend replays the pending
    // PARTIAL verbatim (pods untouched).
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(outcome, ReconcileOutcome::AlreadyApplied { revision: 7 });
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, AckStatus::Partial);
    assert_eq!(sent[0].tenant_failures.as_deref().unwrap_or(&[]).len(), 1);
    // Recovery: the failure set clears → probe re-reports APPLIED.
    rig.channel.set_tenant_failures("p0", Vec::new());
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.ack.sent()[1].status, AckStatus::Applied);
}

/// MIG-4: a tenant-only PARTIAL that 409s against a server which
/// already latched APPLIED falls back to APPLIED + failures — bounded,
/// and only ever for pods-ok reports (a pod-level PARTIAL never
/// flips).
#[tokio::test]
async fn partial_bouncing_off_terminal_applied_retries_as_applied() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "switch-failed".to_string(),
            detail: None,
        }],
    );
    // The server already latched APPLIED for this revision — the
    // PARTIAL+failures report 409s. MIG-6: a 409 is NOT a schema
    // rejection, so no stripped retry is attempted; the bounded
    // APPLIED-with-failures fallback lands instead.
    rig.ack.fail_terminal_next(1);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Partial { revision: 7, failed_pods: Vec::new(), acked: true }
    );
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, AckStatus::Applied);
    assert_eq!(sent[0].tenant_failures.as_deref().unwrap_or(&[]).len(), 1, "failure diagnostics must survive the fallback");
    // The landed APPLIED latches: the next probe re-report (set
    // changed) must issue APPLIED, not PARTIAL — a PARTIAL would 409.
    rig.channel.set_tenant_failures("p0", Vec::new());
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.ack.sent()[1].status, AckStatus::Applied);
}

/// MIG-6/A: a pod that has NOT proved it loaded the new revision
/// reports the OLD generation's tenant-failure set — those failures
/// must be discarded, never attributed to the new revision's ack
/// (test-cluster repro: a stale `config-load` marked the fix revision
/// FAILED and poisoned the rollback baseline).
#[tokio::test]
async fn stale_pod_failures_never_ride_a_new_revisions_ack() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0", "p1"]);
    // rev7 applies cleanly on both pods.
    let snap7 = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.source.push(Ok(Some(snap7.bytes.clone())));
    assert_eq!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::Applied { revision: 7, acked: true }
    );

    // p1 now carries generation-rev7 tenant failures and — crucially —
    // still reports rev7 as loaded while rev8 rolls out (the pod never
    // proved it loaded rev8).
    rig.channel.set_tenant_failures(
        "p1",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "config-load".to_string(),
            detail: None,
        }],
    );
    let snap8 = publish(
        "cell-1",
        8,
        &[worker("w1", None), worker("w2", None)],
        3600,
    );
    rig.channel.set_probe_loaded("p1", Some(7), Some(snap7.payload_digest.clone()));
    rig.source.push(Ok(Some(snap8.bytes.clone())));

    let outcome = rig.controller.reconcile_once(now()).await;
    match outcome {
        ReconcileOutcome::Partial { revision, acked, .. } => {
            assert_eq!(revision, 8);
            assert!(acked, "the PARTIAL itself must be delivered")
        }
        other => panic!("lagging pod must keep the ack PARTIAL, got {other:?}"),
    }
    let sent = rig.ack.sent();
    let last = sent.last().expect("one ack delivered");
    assert_eq!(last.revision, 8);
    assert_eq!(last.status, AckStatus::Partial);
    // MIG-6: the stale failure set was discarded — the wire carries an
    // explicit empty set (the MIG-6 "no failures on THIS revision"
    // clear), not the old generation's diagnostics.
    assert_eq!(
        last.tenant_failures.as_deref(),
        Some(&[][..]),
        "old-generation pod failures must not ride the new revision's ack"
    );
    assert!(
        last.reason.as_deref().unwrap_or("").contains("p1"),
        "the lagging pod is still named in the PARTIAL reason"
    );
}

/// A tenant deploy bumps the applied revision before the host loads it.
/// The kubelet-facing serving predicate must keep the single-replica
/// cell in the Service endpoints through that switch (every other
/// tenant would otherwise get a 503), while the converged predicate
/// still reports the lag — and the tolerance ages out if the pod never
/// catches up.
#[tokio::test]
async fn previous_revision_keeps_cell_serving_for_a_bounded_window() {
    let mut rig = rig(&["p0"]);
    let snap7 = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.source.push(Ok(Some(snap7.bytes.clone())));
    assert_eq!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::Applied { revision: 7, acked: true }
    );
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.controller.serving_readyz(now()), ReadyState::Ready);

    let snap8 = publish("cell-1", 8, &[worker("w1", None), worker("w2", None)], 3600);
    rig.channel.set_probe_loaded("p0", Some(7), Some(snap7.payload_digest.clone()));
    rig.source.push(Ok(Some(snap8.bytes.clone())));
    let t = now() + Duration::seconds(10);
    assert!(matches!(
        rig.controller.reconcile_once(t).await,
        ReconcileOutcome::Partial { revision: 8, .. }
    ));
    rig.controller.probe_tick(t).await;
    assert_eq!(rig.controller.serving_readyz(t), ReadyState::Ready);
    assert_eq!(
        rig.controller.readyz(t),
        ReadyState::NotReady("pod-stale-revision")
    );

    // Still stuck on rev7 well past the grace: fail closed.
    let late = now() + Duration::seconds(300);
    rig.controller.probe_tick(late).await;
    assert_eq!(
        rig.controller.serving_readyz(late),
        ReadyState::NotReady("pod-stale-revision")
    );
}

/// MIG-6/B: a clean APPLIED serializes `tenantFailures: []` explicitly
/// — the server persists the empty set as a clear instead of leaving
/// a PARTIAL-era diagnostic row behind.
#[tokio::test]
async fn clean_applied_serializes_explicit_empty_tenant_failures() {
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    assert_eq!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::Applied { revision: 7, acked: true }
    );
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, AckStatus::Applied);
    assert_eq!(
        sent[0].tenant_failures.as_deref(),
        Some(&[][..]),
        "MIG-6 cells always emit the key — [] is the explicit clear"
    );
    let wire = serde_json::to_value(&sent[0]).unwrap();
    assert_eq!(wire["tenantFailures"], json!([]));
}

/// MIG-6/B: a pre-MIG-1 control plane's strict schema rejects the
/// unknown `tenantFailures` key with a 400 — the cell strips the field
/// and retries ONCE with the classic shape (bounded degrade).
#[tokio::test]
async fn pre_mig1_server_400_strips_tenant_failures_once() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "switch-failed".to_string(),
            detail: None,
        }],
    );
    // First attempt rejected by a strict (unknown-key) schema.
    rig.ack.fail_status_next(&[400]);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    match outcome {
        ReconcileOutcome::Partial { revision, acked, .. } => {
            assert_eq!(revision, 7);
            assert!(acked, "the stripped retry must land the PARTIAL")
        }
        other => panic!("expected PARTIAL, got {other:?}"),
    }
    let attempted = rig.ack.attempted();
    assert_eq!(attempted.len(), 2, "exactly one stripped retry");
    assert_eq!(attempted[0].status, AckStatus::Partial);
    assert_eq!(
        attempted[0].tenant_failures.as_deref().unwrap_or(&[]).len(),
        1,
        "the first attempt carried the diagnostics"
    );
    assert_eq!(attempted[1].status, AckStatus::Partial);
    assert!(
        attempted[1].tenant_failures.is_none(),
        "the strip retry omits the key entirely — pre-MIG-1 wire shape"
    );
    assert_eq!(rig.ack.sent().len(), 1);
}

/// MIG-6/B: a 409 (terminal publication / digest conflict) must NEVER
/// strip `tenantFailures` — degrading there would fail open and turn
/// a real failure into a clean ack. The diagnostics survive on every
/// attempt, including the APPLIED fallback.
#[tokio::test]
async fn terminal_409_never_strips_tenant_failures() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "config-load".to_string(),
            detail: None,
        }],
    );
    // PARTIAL 409s (terminal latched) and so does the APPLIED
    // fallback — the cell must NOT degrade to a bare APPLIED.
    rig.ack.fail_status_next(&[409, 409]);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    match outcome {
        ReconcileOutcome::Partial { revision, acked, .. } => {
            assert_eq!(revision, 7);
            assert!(!acked, "a persistent 409 latches — nothing landed")
        }
        other => panic!("expected PARTIAL, got {other:?}"),
    }
    let attempted = rig.ack.attempted();
    assert_eq!(
        attempted.len(),
        2,
        "PARTIAL + APPLIED fallback — never a stripped attempt"
    );
    assert_eq!(attempted[0].status, AckStatus::Partial);
    assert_eq!(attempted[1].status, AckStatus::Applied);
    assert!(
        attempted
            .iter()
            .all(|r| r.tenant_failures.as_deref().map_or(0, |f| f.len()) == 1),
        "diagnostics ride every attempt — a 409 never degrades the set"
    );
    assert_eq!(rig.ack.sent().len(), 0);
}

/// MIG-6/B: inside the APPLIED fallback the strip-and-retry is likewise
/// gated on 400 — PARTIAL 409s, APPLIED+failures hits a strict
/// pre-MIG-1 schema (400), and the bounded stripped APPLIED lands.
#[tokio::test]
async fn applied_fallback_strips_only_on_400() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "config-load".to_string(),
            detail: None,
        }],
    );
    rig.ack.fail_status_next(&[409, 400]);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    match outcome {
        ReconcileOutcome::Partial { acked, .. } => {
            assert!(acked, "the stripped APPLIED fallback lands")
        }
        other => panic!("expected PARTIAL outcome, got {other:?}"),
    }
    let attempted = rig.ack.attempted();
    assert_eq!(attempted.len(), 3);
    assert_eq!(attempted[0].status, AckStatus::Partial);
    assert_eq!(attempted[0].tenant_failures.as_deref().unwrap_or(&[]).len(), 1);
    assert_eq!(attempted[1].status, AckStatus::Applied);
    assert_eq!(attempted[1].tenant_failures.as_deref().unwrap_or(&[]).len(), 1);
    assert_eq!(attempted[2].status, AckStatus::Applied);
    assert!(
        attempted[2].tenant_failures.is_none(),
        "the final degrade omits the key — pre-MIG-1 wire shape"
    );
    assert_eq!(rig.ack.sent().len(), 1);
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Applied);
}

/// MIG-4: a pod-level PARTIAL (real pod failure) NEVER flips to
/// APPLIED through the terminal fallback — that would be the false
/// success this fix removes.
#[tokio::test]
async fn pod_level_partial_never_flips_to_applied_on_4xx() {
    let mut rig = rig(&["p0", "p1"]);
    rig.channel.fail_next_applies("p1", 99);
    rig.ack.fail_terminal_next(3); // every ack attempt 409s
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert!(matches!(
        outcome,
        ReconcileOutcome::Partial { acked: false, .. }
    ));
    assert!(
        rig.ack.sent().is_empty(),
        "a pod-level PARTIAL may never be reported as APPLIED"
    );
}

/// MIG-1: a pod-LEVEL failure (unreachable) still fails the pod — the
/// tenant-failure path never downgrades real pod evidence.
#[tokio::test]
async fn pod_level_failure_still_reports_partial_not_ready() {
    let mut rig = rig(&["p0", "p1"]);
    rig.channel.fail_next_applies("p1", 99);
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    match outcome {
        ReconcileOutcome::Partial {
            revision,
            failed_pods,
            acked,
        } => {
            assert_eq!(revision, 7);
            assert_eq!(failed_pods, vec!["p1".to_string()]);
            assert!(acked);
        }
        other => panic!("expected PARTIAL, got {other:?}"),
    }
    rig.controller.probe_tick(now()).await;
    assert_ne!(rig.controller.readyz(now()), ReadyState::Ready);
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Partial);
}

async fn ack_reason_with_pod_reasons(pods: [(&str, &str); 2]) -> (ReconcileOutcome, Option<String>) {
    let mut rig = rig(&["p0", "p1"]);
    for (pod, reason) in pods {
        rig.channel.set_ready(pod, false);
        rig.channel.set_probe_reason(pod, reason);
    }
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    let reason = rig.ack.sent()[0].reason.clone();
    (outcome, reason)
}

/// A not-applied pod carrying the informational `hibernated` reason
/// collapses to the generic `pods-not-applied`.
#[tokio::test]
async fn hibernated_not_applied_pod_acks_pods_not_applied() {
    let mut rig = rig(&["p0"]);
    rig.channel.set_ready("p0", false);
    rig.channel.set_probe_reason("p0", "hibernated");
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert!(matches!(outcome, ReconcileOutcome::Partial { .. }), "{outcome:?}");
    let sent = rig.ack.sent();
    assert_eq!(sent[0].status, AckStatus::Partial);
    // The generic reason gets the failed-pod list appended on the ack.
    assert_eq!(sent[0].reason.as_deref(), Some("pods-not-applied:p0"));
}

/// A typed failure reason replaces the generic one from a hibernated
/// pod, whichever pod is probed first.
#[tokio::test]
async fn typed_reason_replaces_hibernated_generic_in_either_order() {
    let (o, reason) =
        ack_reason_with_pod_reasons([("p0", "hibernated"), ("p1", "switch-failed")]).await;
    assert!(matches!(o, ReconcileOutcome::Partial { .. }), "{o:?}");
    assert_eq!(reason.as_deref(), Some("switch-failed"));
    let (o, reason) =
        ack_reason_with_pod_reasons([("p0", "switch-failed"), ("p1", "hibernated")]).await;
    assert!(matches!(o, ReconcileOutcome::Partial { .. }), "{o:?}");
    assert_eq!(reason.as_deref(), Some("switch-failed"));
}

/// A ready, applied pod that still reports `hibernated` is applied.
#[tokio::test]
async fn ready_applied_pod_with_hibernated_reason_is_applied() {
    let mut rig = rig(&["p0", "p1"]);
    rig.channel.set_probe_reason("p0", "hibernated");
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Applied {
            revision: 7,
            acked: true
        }
    );
    let sent = rig.ack.sent();
    assert_eq!(sent[0].status, AckStatus::Applied);
    assert_eq!(sent[0].reason, None);
}

#[tokio::test]
async fn replay_is_idempotent_noop() {
    let mut rig = rig(&["p0"]);
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    rig.controller.reconcile_once(now()).await;
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(outcome, ReconcileOutcome::AlreadyApplied { revision: 7 });
    assert_eq!(
        rig.channel.apply_calls("p0"),
        1,
        "replay must not re-deliver"
    );
}

#[tokio::test]
async fn partial_pod_failure_never_acks_applied() {
    let mut rig = rig(&["p0", "p1"]);
    rig.channel.fail_next_applies("p1", 99);
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    let ReconcileOutcome::Partial {
        revision,
        failed_pods,
        acked,
    } = outcome
    else {
        panic!("expected PARTIAL, got {outcome:?}")
    };
    assert_eq!(revision, 7);
    assert_eq!(failed_pods, vec!["p1".to_string()]);
    assert!(acked);
    let sent = rig.ack.sent();
    assert_eq!(sent[0].status, AckStatus::Partial);
}

#[tokio::test]
async fn rejected_envelope_keeps_state_and_acks_rejected() {
    let mut rig = rig(&["p0"]);
    // Good apply first.
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    rig.controller.reconcile_once(now()).await;
    // Rollback attempt at rev 5.
    rig.source.push(Ok(Some(
        publish("cell-1", 5, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    let ReconcileOutcome::Rejected { reason, acked } = outcome else {
        panic!("expected rejection, got {outcome:?}")
    };
    assert_eq!(reason, RejectReason::RevisionRollback);
    assert!(acked);
    assert_eq!(
        rig.controller.applied_revision(),
        Some(7),
        "state preserved"
    );
    assert_eq!(
        rig.channel.apply_calls("p0"),
        1,
        "rejected revision must not deliver"
    );
    let sent = rig.ack.sent();
    assert_eq!(sent[1].status, AckStatus::Rejected);
}

#[tokio::test]
async fn pull_timeout_bounded_retries_keep_state() {
    let mut rig = rig(&["p0"]);
    rig.source.push(Err(SourceError::Timeout));
    // MemoryDesiredSource answers each scripted error once; to model a
    // bounded retry AT THE SOURCE level we push N errors then a success.
    let outcome = rig.controller.reconcile_once(now()).await;
    assert!(matches!(
        outcome,
        ReconcileOutcome::PullFailed(SourceError::Timeout)
    ));
    assert_eq!(rig.source.calls(), 1);
}

#[tokio::test]
async fn expired_applied_desired_fails_ready_and_stops_cron() {
    let mut rig = rig(&["p0"]);
    // Snapshot expires 60s after issue — apply at issue time.
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", Some("* * * * *"))], 60).bytes,
    )));
    rig.controller.reconcile_once(now()).await;
    // Still fresh: probe + ready.
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.controller.readyz(now()), ReadyState::Ready);
    // After expiry: not ready, and cron must not fire.
    let later = now() + Duration::seconds(120);
    let lease = MemoryLeaseStore::default();
    let mut cron = CronRunner::new(
        lease,
        "ctrl-0",
        Duration::minutes(3),
        Box::new(MemoryTriggerLog::default()),
        Box::new(MemoryCronInvoker::default()),
    );
    let outcome = rig.controller.cron_tick(&mut cron, later).await;
    assert!(
        outcome.fired.is_empty(),
        "expired desired must revoke cron authority"
    );
    assert_eq!(
        rig.controller.readyz(later),
        ReadyState::NotReady("desired-expired")
    );
}

#[tokio::test]
async fn cron_fires_through_controller() {
    let mut rig = rig(&["p0"]);
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", Some("30 12 * * *"))], 86000).bytes,
    )));
    rig.controller.reconcile_once(now()).await;
    let lease = MemoryLeaseStore::default();
    let mut cron = CronRunner::new(
        lease,
        "ctrl-0",
        Duration::minutes(3),
        Box::new(MemoryTriggerLog::default()),
        Box::new(MemoryCronInvoker::default()),
    );
    let due = DateTime::parse_from_rfc3339("2026-11-08T12:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    // Take leadership the minute BEFORE the due slot — the takeover
    // minute itself is a deliberate blackout (at-most-once failover).
    let pre = rig
        .controller
        .cron_tick(&mut cron, due - Duration::minutes(1))
        .await;
    assert!(pre.post_takeover_blackout && pre.fired.is_empty());
    let outcome = rig.controller.cron_tick(&mut cron, due).await;
    assert_eq!(outcome.fired.len(), 1);
    let again = rig.controller.cron_tick(&mut cron, due).await;
    assert!(again.fired.is_empty(), "same key must not refire");
}

#[tokio::test]
async fn partial_recovers_on_replay_and_acks_applied() {
    let mut rig = rig(&["p0", "p1"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    // Exhaust the per-reconcile retry budget (2 attempts) so the first
    // reconcile is genuinely PARTIAL; the next replay then succeeds.
    rig.channel.fail_next_applies("p1", 2);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let first = rig.controller.reconcile_once(now()).await;
    assert!(matches!(first, ReconcileOutcome::Partial { .. }));
    // Same envelope replays (control plane unchanged): must retry p1,
    // NOT short-circuit as AlreadyApplied.
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let second = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        second,
        ReconcileOutcome::Applied {
            revision: 7,
            acked: true
        }
    );
    assert_eq!(rig.channel.apply_calls("p0"), 1, "p0 was already delivered");
    assert_eq!(
        rig.channel.apply_calls("p1"),
        3,
        "2 failed + 1 retry success"
    );
    assert_eq!(rig.ack.sent()[1].status, AckStatus::Applied);
    // Third replay: now truly idempotent.
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    assert_eq!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::AlreadyApplied { revision: 7 }
    );
}

#[tokio::test]
async fn zero_pods_with_workers_is_partial_not_applied() {
    let mut rig = rig(&[]);
    rig.source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    let outcome = rig.controller.reconcile_once(now()).await;
    let ReconcileOutcome::Partial { failed_pods, .. } = outcome else {
        panic!("expected PARTIAL, got {outcome:?}")
    };
    assert_eq!(failed_pods, vec!["<no-pods>".to_string()]);
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Partial);
}

#[tokio::test]
async fn netpolicy_apply_is_idempotent_and_failure_degrades_readyz() {
    use cell_controller::health::ReadyState;
    use cell_controller::manifest::{ManifestApplier, ManifestError};
    struct FailOnce {
        applied: std::sync::Mutex<Vec<String>>,
        fail: std::sync::Mutex<bool>,
    }
    #[async_trait::async_trait]
    impl ManifestApplier for FailOnce {
        async fn apply(&self, manifest: &str) -> Result<(), ManifestError> {
            let mut f = self.fail.lock().unwrap();
            if *f {
                *f = false;
                return Err(ManifestError::Unavailable);
            }
            self.applied.lock().unwrap().push(manifest.to_string());
            Ok(())
        }
        async fn verify(&self, digest: &str) -> Result<bool, ManifestError> {
            Ok(self.applied.lock().unwrap().iter().any(|m| {
                use sha2::Digest;
                hex::encode(sha2::Sha256::digest(m.as_bytes())) == digest
            }))
        }
    }
    let applier = std::sync::Arc::new(FailOnce {
        applied: Default::default(),
        fail: std::sync::Mutex::new(true),
    });
    let mut rig = rig(&["p0"]);
    rig.controller = CellController::new(
        "cell-1",
        CellVerifier::new("cell-1", vec![key().trusted_key()]),
        CellControllerDeps {
            source: rig.source.clone(),
            channel: rig.channel.clone(),
            ack: rig.ack.clone(),
            deliver_retry: RetryPlan {
                max_attempts: 1,
                base_delay: std::time::Duration::ZERO,
                max_delay: std::time::Duration::ZERO,
                jitter_seed: 0,
            },
            ack_retry: RetryPlan::default(),
            manifests: Some(applier.clone()),
            egress_required: false,
            netpolicy: None,
            fence: None,
            capabilities: Default::default(),
        },
    );
    // First apply fails → error surfaced + readyz degrades to
    // 'netpolicy' (isolation guarantee not in place).
    assert!(rig.controller.apply_netpolicy("policy: v1").await.is_err());
    assert_eq!(
        rig.controller.readyz(now()),
        ReadyState::NotReady("netpolicy")
    );
    // A successful apply is idempotent: identical content → no 2nd call.
    assert!(rig.controller.apply_netpolicy("policy: v1").await.is_ok());
    assert!(rig.controller.apply_netpolicy("policy: v1").await.is_ok());
    assert_eq!(applier.applied.lock().unwrap().len(), 1);
    // Changed content re-applies.
    assert!(rig.controller.apply_netpolicy("policy: v2").await.is_ok());
    assert_eq!(applier.applied.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn egress_required_missing_applier_fails_closed() {
    use cell_controller::manifest::ManifestError;
    // T12/G5: production requires a NetworkPolicy applier — none wired
    // + egress_required must fail closed (typed error + readyz marks
    // 'netpolicy'), never pretend isolation is in place.
    let controller = CellController::new(
        "cell-1",
        CellVerifier::new("cell-1", vec![key().trusted_key()]),
        CellControllerDeps {
            source: Arc::new(MemoryDesiredSource::new(Ok(None))),
            channel: Arc::new(MemoryPodChannel::new(&["p0"])),
            ack: Arc::new(MemoryAckSink::default()),
            deliver_retry: RetryPlan::default(),
            ack_retry: RetryPlan::default(),
            manifests: None,
            egress_required: true,
            netpolicy: None,
            fence: None,
            capabilities: Default::default(),
        },
    );
    let err = controller.apply_netpolicy("policy: v1").await.unwrap_err();
    assert_eq!(err, ManifestError::Unavailable);
    assert_eq!(
        controller.readyz(Utc::now()),
        ReadyState::NotReady("netpolicy")
    );
    let health = controller.health().read().unwrap().clone();
    assert_eq!(
        health.netpolicy_error.as_deref(),
        Some("egress-policy-unavailable")
    );
}

// ---- L9: transient rejections never become terminal REJECTED ACKs ----

#[tokio::test]
async fn transient_rejection_sends_no_terminal_ack() {
    let mut rig = rig(&["p0"]);
    // An already-EXPIRED envelope: deterministic signature is fine but
    // `expiresAt` is in the past — a renewed publication may still fix
    // this, so the ack must NOT be terminal REJECTED.
    let payload = json!({
        "schemaVersion": 1, "cellId": "cell-1", "revision": 7,
        "tenants": [{"tenantId": "team-a", "workers": [worker("w1", None)]}]
    });
    let snap = sign(
        &SignParams {
            kind: "compute.desired",
            scope: "cell:cell-1",
            revision: 7,
            issued_at: now() - Duration::seconds(120),
            expires_at: now() - Duration::seconds(60),
            payload: &payload,
        },
        &key(),
    )
    .unwrap();
    rig.source.push(Ok(Some(snap.bytes)));
    let outcome = rig.controller.reconcile_once(now()).await;
    match outcome {
        ReconcileOutcome::Rejected { reason, acked } => {
            assert!(reason.is_transient());
            assert!(!acked, "transient rejection must not ACK");
        }
        other => panic!("expected transient rejection, got {other:?}"),
    }
    assert!(
        rig.ack.sent().is_empty(),
        "no ACK attempt on transient rejection"
    );
    assert_eq!(
        rig.controller.readyz(now()),
        ReadyState::NotReady("no-desired")
    );
}

#[tokio::test]
async fn deterministic_rejection_still_acks_rejected() {
    let mut rig = rig(&["p0"]);
    let snap = publish("cell-1", 7, &[worker("w1", None)], 3600);
    // Tamper: deterministic signature violation → terminal REJECTED.
    let mut raw: serde_json::Value = serde_json::from_slice(&snap.bytes).unwrap();
    raw["payload"]["revision"] = json!(8);
    let tampered = serde_json::to_vec(&raw).unwrap();
    rig.source.push(Ok(Some(tampered)));
    match rig.controller.reconcile_once(now()).await {
        ReconcileOutcome::Rejected { reason, acked } => {
            assert!(!reason.is_transient());
            assert!(acked);
        }
        other => panic!("expected deterministic rejection, got {other:?}"),
    }
    assert_eq!(
        rig.ack.sent()[0].status,
        cell_controller::ack::AckStatus::Rejected
    );
}

// ---- L10: durable revision fence ----

#[tokio::test]
async fn fence_persists_and_restart_blocks_replay() {
    use cell_controller::fence::MemoryFenceStore;
    let fence = std::sync::Arc::new(MemoryFenceStore::default());
    struct Shared(std::sync::Arc<MemoryFenceStore>);
    impl cell_controller::fence::FenceStore for Shared {
        fn load(&self) -> Result<Option<(u64, String)>, cell_controller::fence::FenceError> {
            self.0.load()
        }
        fn store(&self, r: u64, d: &str) -> Result<(), cell_controller::fence::FenceError> {
            self.0.store(r, d)
        }
    }
    // "Process 1": applies rev 7, fence persisted.
    let source1 = Arc::new(MemoryDesiredSource::new(Ok(None)));
    let channel1 = Arc::new(MemoryPodChannel::new(&["p0"]));
    let mut c1 = CellController::new(
        "cell-1",
        CellVerifier::new("cell-1", vec![key().trusted_key()]),
        CellControllerDeps {
            source: source1.clone(),
            channel: channel1,
            ack: Arc::new(MemoryAckSink::default()),
            deliver_retry: RetryPlan {
                max_attempts: 1,
                base_delay: std::time::Duration::ZERO,
                max_delay: std::time::Duration::ZERO,
                jitter_seed: 0,
            },
            ack_retry: RetryPlan::default(),
            manifests: None,
            egress_required: false,
            netpolicy: None,
            fence: Some(Box::new(Shared(fence.clone()))),
            capabilities: Default::default(),
        },
    );
    source1.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    match c1.reconcile_once(now()).await {
        ReconcileOutcome::Applied { .. } => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(fence.value().unwrap().0, 7);

    // "Process 2" restarts with a restored verifier — replaying rev 7
    // must be rejected by the fence (idempotent equal-revision conflict
    // or rollback), not silently re-applied.
    let (rev, digest) = fence.value().unwrap();
    let source2 = Arc::new(MemoryDesiredSource::new(Ok(None)));
    let mut c2 = CellController::new(
        "cell-1",
        CellVerifier::restore("cell-1", vec![key().trusted_key()], rev, &digest).unwrap(),
        CellControllerDeps {
            source: source2.clone(),
            channel: Arc::new(MemoryPodChannel::new(&["p0"])),
            ack: Arc::new(MemoryAckSink::default()),
            deliver_retry: RetryPlan {
                max_attempts: 1,
                base_delay: std::time::Duration::ZERO,
                max_delay: std::time::Duration::ZERO,
                jitter_seed: 0,
            },
            ack_retry: RetryPlan::default(),
            manifests: None,
            egress_required: false,
            netpolicy: None,
            fence: Some(Box::new(Shared(fence.clone()))),
            capabilities: Default::default(),
        },
    );
    source2.push(Ok(Some(
        publish("cell-1", 5, &[worker("w1", None)], 3600).bytes,
    )));
    match c2.reconcile_once(now()).await {
        ReconcileOutcome::Rejected { reason, .. } => {
            assert_eq!(
                reason,
                cell_controller::verify::RejectReason::RevisionRollback
            )
        }
        other => panic!("restart must fence out stale revision, got {other:?}"),
    }
}

#[tokio::test]
async fn fence_persist_failure_is_transient_and_applies_nothing() {
    struct Fail;
    impl cell_controller::fence::FenceStore for Fail {
        fn load(&self) -> Result<Option<(u64, String)>, cell_controller::fence::FenceError> {
            Ok(None)
        }
        fn store(&self, _r: u64, _d: &str) -> Result<(), cell_controller::fence::FenceError> {
            Err(cell_controller::fence::FenceError("disk full".into()))
        }
    }
    let source = Arc::new(MemoryDesiredSource::new(Ok(None)));
    let channel = Arc::new(MemoryPodChannel::new(&["p0"]));
    let ack = Arc::new(MemoryAckSink::default());
    let mut c = CellController::new(
        "cell-1",
        CellVerifier::new("cell-1", vec![key().trusted_key()]),
        CellControllerDeps {
            source: source.clone(),
            channel: channel.clone(),
            ack: ack.clone(),
            deliver_retry: RetryPlan {
                max_attempts: 1,
                base_delay: std::time::Duration::ZERO,
                max_delay: std::time::Duration::ZERO,
                jitter_seed: 0,
            },
            ack_retry: RetryPlan::default(),
            manifests: None,
            egress_required: false,
            netpolicy: None,
            fence: Some(Box::new(Fail)),
            capabilities: Default::default(),
        },
    );
    source.push(Ok(Some(
        publish("cell-1", 7, &[worker("w1", None)], 3600).bytes,
    )));
    match c.reconcile_once(now()).await {
        ReconcileOutcome::Rejected { reason, acked } => {
            assert_eq!(reason, cell_controller::verify::RejectReason::FencePersist);
            assert!(!acked);
        }
        other => panic!("{other:?}"),
    }
    assert!(
        channel.applied("p0").is_none(),
        "nothing applied on fence persist failure"
    );
    assert!(ack.sent().is_empty());
}

#[tokio::test]
async fn lost_applied_ack_is_resent_without_pod_calls() {
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.ack.fail_next(1); // first ack lost in transport
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Applied {
            revision: 7,
            acked: false
        }
    );
    assert_eq!(rig.ack.sent().len(), 0);
    let pod_calls = rig.channel.apply_calls("p0");

    // Next tick: identical envelope — controller must re-send ONLY the ack.
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(outcome, ReconcileOutcome::AlreadyApplied { revision: 7 });
    assert_eq!(rig.ack.sent().len(), 1, "ack must be re-delivered");
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Applied);
    assert_eq!(
        rig.channel.apply_calls("p0"),
        pod_calls,
        "ack resend must not touch pods"
    );

    // Ack landed — a further replay short-circuits with no resend.
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.controller.reconcile_once(now()).await;
    assert_eq!(rig.ack.sent().len(), 1);
}

#[tokio::test]
async fn terminal_4xx_ack_latches_observable_error_and_stops_retrying() {
    let mut rig = rig(&["p0"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.ack.fail_terminal_next(3); // server keeps saying 409
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Applied {
            revision: 7,
            acked: false
        }
    );

    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.controller.reconcile_once(now()).await;
    // Terminal: latched — health exposes the failure, no infinite resend.
    let health = rig.controller.health().read().unwrap().clone();
    assert_eq!(health.last_error.as_deref(), Some("ack-terminal-rejected"));
    let sent_after_latch = rig.ack.sent().len();
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.controller.reconcile_once(now()).await;
    assert_eq!(rig.ack.sent().len(), sent_after_latch);
}

// ---------- T13/H1: netpolicy enforced inside reconcile, via the REAL
// production construction path (`prod::build_controller`) ----------

fn t13_policy() -> vrt_config::netpolicy::RuntimeNetPolicy {
    use vrt_config::netpolicy::*;
    let peer = |ns: &str, k: &str, v: &str| SelectorPeer {
        namespace_labels: [("kubernetes.io/metadata.name".to_string(), ns.to_string())]
            .into_iter()
            .collect(),
        pod_labels: [(k.to_string(), v.to_string())].into_iter().collect(),
    };
    RuntimeNetPolicy {
        policy_name: "vrt-runtime-egress".into(),
        namespace: "vrt-runtime".into(),
        pod_selector: [("app".to_string(), "vrt-host".to_string())]
            .into_iter()
            .collect(),
        ingress_peers: vec![peer("traefik", "app.kubernetes.io/name", "traefik")],
        ingress_port: NetPort {
            protocol: Protocol::Tcp,
            port: 8080,
        },
        dns_peer: peer("kube-system", "k8s-app", "kube-dns"),
        backends: vec![EgressBackend::Selector {
            peer: peer("vrt-system", "app", "kvrocks"),
            port: NetPort {
                protocol: Protocol::Tcp,
                port: 6666,
            },
        }],
        public_ports: vec![NetPort {
            protocol: Protocol::Tcp,
            port: 443,
        }],
        pod_cidrs: vec![vrt_config::netpolicy::Cidr::parse("10.42.0.0/16").unwrap()],
        service_cidrs: vec![vrt_config::netpolicy::Cidr::parse("10.43.0.0/16").unwrap()],
        node_excepts: vec![vrt_config::netpolicy::Cidr::parse("203.0.113.10/32").unwrap()],
        extra_excepts: vec![],
        ipv6_enabled: false,
    }
}

fn t13_config(netpolicy: bool) -> cell_controller::prod::ProdConfig {
    cell_controller::prod::ProdConfig {
        cell_id: "cell-1".into(),
        control_plane_url: "https://cp.internal".into(),
        render_inputs_dir: None,
        token_file: "/nonexistent".into(),
        health_listen: "0.0.0.0:9090".into(),
        metrics_listen: None,
        poll_interval_secs: 10,
        cron_holder: "test".into(),
        fence_file: None,
        allow_insecure_http: false,
        egress_required: true,
        netpolicy: netpolicy.then(t13_policy),
        trusted_keys: vec![key().trusted_key()],
        pods: vec![],
        replicas: 1,
        cron_state_dir: None,
        cron_lease: None,
        d1sql: None,
        queue_dispatch: None,
        storage_purge: None,
        cache_purge: None,
        quotas: true,
        report: None,
    }
}

#[tokio::test]
async fn egress_unconfirmed_blocks_delivery_and_never_acks_applied() {
    use cell_controller::manifest::MemoryManifestApplier;
    let snapshot = publish("cell-1", 1, &[worker("w1", None)], 3600);

    // 1) egress_required + no applier → reconcile stops BEFORE pods.
    let source = Arc::new(MemoryDesiredSource::new(Ok(Some(snapshot.bytes.clone()))));
    let channel = Arc::new(MemoryPodChannel::new(&["p0"]));
    let ack = Arc::new(MemoryAckSink::default());
    let mut controller = cell_controller::prod::build_controller(
        &t13_config(true),
        source.clone(),
        channel.clone(),
        ack.clone(),
        None,
        Default::default(),
    )
    .unwrap();
    let outcome = controller.reconcile_once(now()).await;
    assert_eq!(outcome, ReconcileOutcome::EgressUnavailable { revision: 1 });
    // Zero pod calls — nothing delivered without confirmed isolation.
    assert!(channel.applied("p0").is_none());
    // The publication is NOT APPLIED — a typed PARTIAL reason went out.
    let sent = ack.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].status, AckStatus::Partial);
    assert_eq!(sent[0].reason.as_deref(), Some("egress-policy-unavailable"));
    assert_eq!(controller.readyz(now()), ReadyState::NotReady("netpolicy"));

    // 2) Same config + working applier → apply + read-back confirm →
    // tenants flow and the APPLIED ack lands.
    let applier = Arc::new(MemoryManifestApplier::default());
    let mut controller = cell_controller::prod::build_controller(
        &t13_config(true),
        source,
        channel.clone(),
        ack.clone(),
        Some(applier.clone()),
        Default::default(),
    )
    .unwrap();
    let outcome = controller.reconcile_once(now()).await;
    assert!(matches!(outcome, ReconcileOutcome::Applied { .. }));
    assert!(channel.applied("p0").is_some());
    assert_eq!(applier.applied().len(), 1);
    assert_eq!(ack.sent()[1].status, AckStatus::Applied);

    // 3) Policy deleted cluster-side afterwards → next reconcile fails
    // closed again even though the snapshot was already applied.
    applier.forget();
    let outcome = controller.reconcile_once(now()).await;
    assert_eq!(outcome, ReconcileOutcome::EgressUnavailable { revision: 1 });
    assert_eq!(
        ack.sent().last().unwrap().reason.as_deref(),
        Some("egress-policy-unavailable")
    );

    // 4) RV-R48 recovery: the failed verification cleared the applied
    // digest, so the next reconcile RE-APPLIES the manifest (the memo no
    // longer short-circuits), read-back confirms, and APPLIED is acked.
    let applied_before = applier.applied().len();
    assert_eq!(applied_before, 0, "forget() emptied the applier");
    let outcome = controller.reconcile_once(now()).await;
    // Revision 1 is already fully applied, so the replay path reports
    // AlreadyApplied (or Applied if it re-converges) — never EgressUnavailable.
    assert!(
        matches!(
            outcome,
            ReconcileOutcome::AlreadyApplied { .. } | ReconcileOutcome::Applied { .. }
        ),
        "got {outcome:?}"
    );
    assert_eq!(applier.applied().len(), 1, "manifest re-applied exactly once");
    // Pod health is refreshed by the probe tick (not by reconcile).
    controller.probe_tick(now()).await;
    assert_eq!(controller.readyz(now()), ReadyState::Ready);
}

/// MIG-8: a pod that PROVED tenant failures and then becomes
/// UNOBSERVABLE (vrt-host restart → probe unreachable) must not be
/// read as "failures cleared" — the aggregate keeps the pod's last
/// proven contribution, no false `APPLIED + tenantFailures: []` goes
/// out, and the control plane's marks stay (2026-11-17 incident: the
/// restart window emitted a clean APPLIED and cleared real exclusion
/// marks). Recovery only lands when the pod PROVES the cleared set.
#[tokio::test]
async fn unobservable_pod_retains_failures_no_false_recovery() {
    use cell_controller::pod::{PodError, TenantFailure};
    let mut rig = rig(&["p0", "p1"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None), worker("w2", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.channel.set_tenant_failures(
        "p1",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w2".to_string()],
            code: "config-load".to_string(),
            detail: None,
        }],
    );
    assert!(matches!(
        rig.controller.reconcile_once(now()).await,
        ReconcileOutcome::Partial { acked: true, .. }
    ));
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Partial);
    assert_eq!(rig.ack.sent()[0].reason.as_deref(), Some("tenant-failed"));

    // vrt-host restarts — p1 is in the inventory but probes fail.
    rig.channel.set_probe_error("p1", PodError::Unreachable);
    rig.controller.probe_tick(now()).await;
    // The retained set is unchanged ⇒ NO report at all — critically
    // never the incident's `APPLIED + tenantFailures: []`.
    assert_eq!(
        rig.ack.sent().len(),
        1,
        "unobservable pod must not emit a clean recovery: {:?}",
        rig.ack.sent()
    );

    // The pod comes back still failing — still no report.
    rig.channel.clear_probe_error("p1");
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.ack.sent().len(), 1);

    // Legit recovery: the pod PROVES the revision with a cleared set —
    // now (and only now) the APPLIED re-report is honest.
    rig.channel.set_tenant_failures("p1", Vec::new());
    rig.controller.probe_tick(now()).await;
    let sent = rig.ack.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1].status, AckStatus::Applied);
    assert!(sent[1].tenant_failures.as_deref().unwrap_or(&[]).is_empty());
}

/// MIG-8: same retention when the pod is REACHABLE but not-proven —
/// a restarting host reports `ready=false` (or a stale generation's
/// revision/digest). Its failures are unknown, never cleared.
#[tokio::test]
async fn not_ready_or_stale_pod_retains_failures() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0", "p1"]);
    let snapshot = publish("cell-1", 7, &[worker("w1", None), worker("w2", None)], 3600);
    rig.source.push(Ok(Some(snapshot.bytes.clone())));
    rig.channel.set_tenant_failures(
        "p1",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w2".to_string()],
            code: "switch-failed".to_string(),
            detail: None,
        }],
    );
    rig.controller.reconcile_once(now()).await;
    assert_eq!(rig.ack.sent().len(), 1);

    // Restarting: reachable but not-ready.
    rig.channel.set_ready("p1", false);
    rig.controller.probe_tick(now()).await;
    assert_eq!(
        rig.ack.sent().len(),
        1,
        "not-ready pod must not clear its set"
    );

    // Back up but still proving an OLD generation (stale revision) —
    // its current failures are unknown to the applied revision.
    rig.channel.set_ready("p1", true);
    rig.channel
        .set_probe_loaded("p1", Some(6), Some("old-digest".to_string()));
    rig.controller.probe_tick(now()).await;
    assert_eq!(
        rig.ack.sent().len(),
        1,
        "stale-generation pod must not clear the applied revision's set"
    );

    // Back on the applied revision, still failing — no report.
    rig.channel.clear_probe_loaded("p1");
    rig.controller.probe_tick(now()).await;
    assert_eq!(rig.ack.sent().len(), 1);
}

/// MIG-8: a NEW revision resets the retained map — the older
/// generation's failures must never be attributed to the new one
/// (the MIG-6 provenance invariant holds across probe ticks).
#[tokio::test]
async fn new_revision_resets_retained_failures() {
    use cell_controller::pod::TenantFailure;
    let mut rig = rig(&["p0"]);
    let snap7 = publish("cell-1", 7, &[worker("w1", None)], 3600);
    rig.source.push(Ok(Some(snap7.bytes.clone())));
    rig.channel.set_tenant_failures(
        "p0",
        vec![TenantFailure {
            tenant: "team-a".to_string(),
            worker_ids: vec!["w1".to_string()],
            code: "config-load".to_string(),
            detail: None,
        }],
    );
    rig.controller.reconcile_once(now()).await;
    assert_eq!(rig.ack.sent()[0].status, AckStatus::Partial);

    // r8 lands cleanly — the pod proves it loaded r8 with NO failures
    // while still (spuriously) reporting its r7 set: the r8 probe is
    // authoritative and the ack is APPLIED.
    rig.channel.set_tenant_failures("p0", Vec::new());
    let snap8 = publish("cell-1", 8, &[worker("w1", None), worker("w2", None)], 3600);
    rig.source.push(Ok(Some(snap8.bytes.clone())));
    let outcome = rig.controller.reconcile_once(now()).await;
    assert_eq!(
        outcome,
        ReconcileOutcome::Applied {
            revision: 8,
            acked: true
        }
    );
    // A probe error under r8 has nothing retained for this revision —
    // the aggregate is legitimately empty and no stale r7 set revives.
    rig.channel
        .set_probe_error("p0", cell_controller::pod::PodError::Unreachable);
    rig.controller.probe_tick(now()).await;
    let sent = rig.ack.sent();
    assert!(
        sent.iter()
            .all(|a| a.tenant_failures.as_deref().unwrap_or(&[]).is_empty()
                || a.status == AckStatus::Partial),
        "r7 failures must not be attributed to r8: {sent:?}"
    );
}

// ---- Issue #63: fence attestation persistence + echo ----

#[tokio::test]
async fn fence_attestation_is_persisted_and_echoed_when_fence_leads() {
    use cell_controller::ack::AckStatus;
    use cell_controller::fence::{FenceError, FenceState, FenceStore, MemoryFenceStore};
    use cell_controller::source::FenceAttestationHeader;

    struct Shared(std::sync::Arc<MemoryFenceStore>);
    impl FenceStore for Shared {
        fn load(&self) -> Result<Option<(u64, String)>, FenceError> {
            self.0.load()
        }
        fn store(&self, revision: u64, digest: &str) -> Result<(), FenceError> {
            self.0.store(revision, digest)
        }
        fn load_full(&self) -> Result<Option<FenceState>, FenceError> {
            self.0.load_full()
        }
        fn store_full(&self, state: &FenceState) -> Result<(), FenceError> {
            self.0.store_full(state)
        }
    }

    let fence = std::sync::Arc::new(MemoryFenceStore::default());
    let source = Arc::new(MemoryDesiredSource::new(Ok(None)));
    let ack = Arc::new(MemoryAckSink::default());
    let mut controller = CellController::new(
        "cell-1",
        CellVerifier::new("cell-1", vec![key().trusted_key()]),
        CellControllerDeps {
            source: source.clone(),
            channel: Arc::new(MemoryPodChannel::new(&["p0"])),
            ack: ack.clone(),
            deliver_retry: RetryPlan {
                max_attempts: 1,
                base_delay: std::time::Duration::ZERO,
                max_delay: std::time::Duration::ZERO,
                jitter_seed: 0,
            },
            ack_retry: RetryPlan::default(),
            manifests: None,
            egress_required: false,
            netpolicy: None,
            fence: Some(Box::new(Shared(fence.clone()))),
            capabilities: Default::default(),
        },
    );

    // Rev 7 arrives with the plane's attestation → persisted with the fence.
    source.set_attestation(Some(FenceAttestationHeader {
        revision: 7,
        attestation: "sig-7".to_string(),
    }));
    let snapshot7 = publish("cell-1", 7, &[worker("w1", None)], 3600);
    let envelope7 = snapshot_protocol::compute_envelope_digest(&snapshot7.bytes).unwrap();
    source.push(Ok(Some(snapshot7.bytes.clone())));
    match controller.reconcile_once(now()).await {
        ReconcileOutcome::Applied { revision: 7, .. } => {}
        other => panic!("{other:?}"),
    }
    let stored = fence.load_full().unwrap().expect("fence persisted");
    let stored_attestation = stored.attestation.expect("attestation persisted");
    assert_eq!(stored_attestation.revision, 7);
    assert_eq!(stored_attestation.attestation, "sig-7");
    assert_eq!(stored_attestation.payload_digest, snapshot7.payload_digest);
    assert_eq!(stored_attestation.envelope_digest, envelope7);

    // A restored plane publishes rev 5. The REJECTED ack (fence ahead) must
    // carry the proof for rev 7 so the plane can lift its counter.
    source.set_attestation(None);
    source.push(Ok(Some(
        publish("cell-1", 5, &[worker("w1", None)], 3600).bytes,
    )));
    match controller.reconcile_once(now()).await {
        ReconcileOutcome::Rejected { .. } => {}
        other => panic!("{other:?}"),
    }
    let sent = ack.sent();
    let rejected = sent
        .iter()
        .find(|a| a.revision == 5 && a.status == AckStatus::Rejected)
        .expect("rejected ack for rev 5");
    assert_eq!(rejected.fence_revision, 7);
    let proof = rejected.fence_proof.clone().expect("fence proof echoed");
    assert_eq!(proof.revision, 7);
    assert_eq!(proof.attestation, "sig-7");
    assert_eq!(proof.payload_digest, snapshot7.payload_digest);
    assert_eq!(proof.envelope_digest, envelope7);
}
