//! `/healthz` + `/readyz` surface — fail-closed readiness.
//!
//! `healthz` is process liveness (always 200 while running).
//!
//! Two readiness predicates share one fail-closed core:
//!
//! - `/readyz` ([`serving_readiness`]) answers "can this cell serve
//!   traffic right now?" and is what the kubelet probes. A pod that is
//!   ready but still on the PREVIOUS revision keeps the cell Ready for a
//!   bounded [`STALE_SERVING_GRACE`] after it last proved the applied
//!   revision. Without this, every tenant deploy bumped the applied
//!   revision, the single-replica host Pod left the Service endpoints
//!   until it loaded the new snapshot, and every tenant on the cell got
//!   a 503 during any one tenant's deploy.
//! - `/readyz/converged` ([`readiness`]) answers "has every pod loaded
//!   the applied revision?" for operators and rollout checks.
//!
//! The view carries no secrets, payloads, env or tenant data — only
//! revision numbers, timestamps and pod ids, so it can be scraped freely.

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::RwLock;

/// One pod's last probe result (revision + readiness only).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PodHealth {
    /// Pod identity.
    pub pod_id: String,
    /// Pod-reported readiness.
    pub ready: bool,
    /// Revision the pod reports loaded.
    pub loaded_revision: Option<u64>,
    /// Probe failed entirely (unreachable) — treated as not ready.
    pub probe_failed: bool,
    /// Last probe time at which this pod was ready AND had loaded the
    /// revision applied at that moment (`None` = never proven). Anchors
    /// the bounded stale-serving grace; it only moves forward on proof,
    /// so a pod stuck on an old revision ages out even while revisions
    /// keep churning.
    #[serde(default)]
    pub last_current_at: Option<DateTime<Utc>>,
}

/// Shared readiness view — updated by `probe_tick`/`reconcile_once`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CellHealth {
    /// Revision the controller last applied (`None` = never applied).
    pub applied_revision: Option<u64>,
    /// `expiresAt` of the applied snapshot.
    pub applied_expires_at: Option<DateTime<Utc>>,
    /// Pods assigned under the applied revision.
    pub assigned_pods: Vec<String>,
    /// Latest probe results per assigned pod.
    pub pods: BTreeMap<String, PodHealth>,
    /// Last reconcile error class (typed code only — no content).
    pub last_error: Option<String>,
    /// Typed code when the last `ManifestApplier` attempt failed
    /// (`None` = no applier configured, or the last apply succeeded).
    /// A failed NetworkPolicy apply means pod isolation is NOT what the
    /// generated manifest requires — readiness must reflect that.
    pub netpolicy_error: Option<String>,
    /// T14/S3: the cell is running with `egress_required=false` —
    /// documented MVP escape hatch. Serialized on /healthz (visible,
    /// structured) but does NOT gate readiness: it is an explicit
    /// configuration, not a failure.
    #[serde(default)]
    pub egress_policy_disabled: bool,
}

/// Readiness verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyState {
    /// Serving.
    Ready,
    /// Not serving; the static code is safe to expose.
    NotReady(&'static str),
}

/// The shared health state handle wired into both the orchestrator and
/// the axum router.
pub type SharedHealth = std::sync::Arc<RwLock<CellHealth>>;

/// Fresh empty health state.
pub fn new_health() -> SharedHealth {
    std::sync::Arc::new(RwLock::new(CellHealth::default()))
}

/// How long a ready pod that is still on an older revision keeps the
/// cell serving-ready, measured from the pod's `last_current_at`. A
/// generation switch takes tens of seconds (preflight alone is ~20 s);
/// a pod that has not proven any applied revision for this long is
/// treated as stuck and the cell fails closed.
pub const STALE_SERVING_GRACE: chrono::Duration = chrono::Duration::seconds(120);

/// Converged readiness at `now`: applied desired unexpired AND every
/// assigned pod ready AND loaded the applied revision. Served on
/// `/readyz/converged`.
pub fn readiness(health: &CellHealth, now: DateTime<Utc>) -> ReadyState {
    evaluate(health, now, false)
}

/// Serving readiness at `now`, served on `/readyz` (the kubelet probe).
/// Same fail-closed rules as [`readiness`] — netpolicy failure, missing
/// or expired desired, probe failure and not-ready pods all refuse —
/// except that a ready pod on an OLDER revision is tolerated while it
/// proved an applied revision within [`STALE_SERVING_GRACE`]. A pod
/// reporting no revision, or a revision NEWER than applied, never is.
pub fn serving_readiness(health: &CellHealth, now: DateTime<Utc>) -> ReadyState {
    evaluate(health, now, true)
}

fn evaluate(health: &CellHealth, now: DateTime<Utc>, tolerate_stale: bool) -> ReadyState {
    if health.netpolicy_error.is_some() {
        // Isolation guarantee not in place — surface this before the
        // desired-state axis (both fail closed; the cause matters).
        return ReadyState::NotReady("netpolicy");
    }
    let Some(revision) = health.applied_revision else {
        return ReadyState::NotReady("no-desired");
    };
    match health.applied_expires_at {
        Some(expires) if now < expires => {}
        Some(_) => return ReadyState::NotReady("desired-expired"),
        None => return ReadyState::NotReady("no-desired"),
    }
    for pod_id in &health.assigned_pods {
        match health.pods.get(pod_id) {
            Some(pod)
                if pod.ready && !pod.probe_failed && pod.loaded_revision == Some(revision) => {}
            Some(pod) if pod.probe_failed => return ReadyState::NotReady("pod-probe-failed"),
            Some(pod)
                if tolerate_stale
                    && pod.ready
                    && matches!(pod.loaded_revision, Some(loaded) if loaded < revision)
                    && matches!(pod.last_current_at, Some(at)
                        if at <= now && now - at < STALE_SERVING_GRACE) => {}
            Some(pod) if pod.loaded_revision != Some(revision) => {
                return ReadyState::NotReady("pod-stale-revision")
            }
            _ => return ReadyState::NotReady("pod-not-ready"),
        }
    }
    ReadyState::Ready
}

/// Axum router serving `/healthz` and `/readyz` over `shared`.
pub fn router(shared: SharedHealth) -> axum::Router {
    use axum::{extract::State, http::StatusCode, Json};
    async fn healthz() -> &'static str {
        "ok"
    }
    async fn readyz(
        State(shared): State<SharedHealth>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let health = shared.read().unwrap_or_else(|e| e.into_inner()).clone();
        verdict(serving_readiness(&health, Utc::now()))
    }
    async fn readyz_converged(
        State(shared): State<SharedHealth>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let health = shared.read().unwrap_or_else(|e| e.into_inner()).clone();
        verdict(readiness(&health, Utc::now()))
    }
    fn verdict(state: ReadyState) -> Result<Json<serde_json::Value>, StatusCode> {
        match state {
            ReadyState::Ready => Ok(Json(serde_json::json!({ "status": "ready" }))),
            ReadyState::NotReady(reason) => {
                let _ = reason; // static code only; never echoed
                Err(StatusCode::SERVICE_UNAVAILABLE)
            }
        }
    }
    axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(readyz))
        .route("/readyz/converged", axum::routing::get(readyz_converged))
        .with_state(shared)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-08T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::seconds(secs)
    }

    #[test]
    fn readyz_matrix() {
        let mut h = CellHealth::default();
        assert_eq!(readiness(&h, at(0)), ReadyState::NotReady("no-desired"));
        h.applied_revision = Some(7);
        h.applied_expires_at = Some(at(60));
        h.assigned_pods = vec!["p0".into(), "p1".into()];
        h.pods.insert(
            "p0".into(),
            PodHealth {
                pod_id: "p0".into(),
                ready: true,
                loaded_revision: Some(7),
                probe_failed: false,
                last_current_at: None,
            },
        );
        // missing probe for p1 → not ready
        assert!(matches!(readiness(&h, at(0)), ReadyState::NotReady(_)));
        h.pods.insert(
            "p1".into(),
            PodHealth {
                pod_id: "p1".into(),
                ready: true,
                loaded_revision: Some(7),
                probe_failed: false,
                last_current_at: None,
            },
        );
        assert_eq!(readiness(&h, at(0)), ReadyState::Ready);
        // expired desired
        assert_eq!(
            readiness(&h, at(61)),
            ReadyState::NotReady("desired-expired")
        );
        // stale pod revision
        h.pods.get_mut("p1").unwrap().loaded_revision = Some(6);
        assert_eq!(
            readiness(&h, at(0)),
            ReadyState::NotReady("pod-stale-revision")
        );
        // probe failure
        h.pods.get_mut("p1").unwrap().loaded_revision = Some(7);
        h.pods.get_mut("p1").unwrap().probe_failed = true;
        assert_eq!(
            readiness(&h, at(0)),
            ReadyState::NotReady("pod-probe-failed")
        );
    }

    fn serving_cell() -> CellHealth {
        let mut h = CellHealth::default();
        h.applied_revision = Some(8);
        h.applied_expires_at = Some(at(600));
        h.assigned_pods = vec!["p0".into()];
        h.pods.insert(
            "p0".into(),
            PodHealth {
                pod_id: "p0".into(),
                ready: true,
                loaded_revision: Some(7),
                probe_failed: false,
                last_current_at: Some(at(0)),
            },
        );
        h
    }

    #[test]
    fn serving_tolerates_previous_revision_within_grace() {
        let h = serving_cell();
        assert_eq!(serving_readiness(&h, at(30)), ReadyState::Ready);
        // converged view still reports the switch in flight
        assert_eq!(
            readiness(&h, at(30)),
            ReadyState::NotReady("pod-stale-revision")
        );
        // grace is bounded from the last proof, not from the apply
        assert_eq!(
            serving_readiness(&h, at(120)),
            ReadyState::NotReady("pod-stale-revision")
        );
    }

    #[test]
    fn serving_stays_fail_closed() {
        // never proven current (fresh boot) → not tolerated
        let mut h = serving_cell();
        h.pods.get_mut("p0").unwrap().last_current_at = None;
        assert_eq!(
            serving_readiness(&h, at(1)),
            ReadyState::NotReady("pod-stale-revision")
        );
        // proof timestamp in the future is not trusted
        let mut h = serving_cell();
        h.pods.get_mut("p0").unwrap().last_current_at = Some(at(50));
        assert!(matches!(serving_readiness(&h, at(1)), ReadyState::NotReady(_)));
        // no loaded revision, or a revision newer than applied
        for loaded in [None, Some(9)] {
            let mut h = serving_cell();
            h.pods.get_mut("p0").unwrap().loaded_revision = loaded;
            assert!(matches!(serving_readiness(&h, at(1)), ReadyState::NotReady(_)));
        }
        // stale AND not ready
        let mut h = serving_cell();
        h.pods.get_mut("p0").unwrap().ready = false;
        assert!(matches!(serving_readiness(&h, at(1)), ReadyState::NotReady(_)));
        // probe failure, netpolicy, expired desired all win over grace
        let mut h = serving_cell();
        h.pods.get_mut("p0").unwrap().probe_failed = true;
        assert_eq!(
            serving_readiness(&h, at(1)),
            ReadyState::NotReady("pod-probe-failed")
        );
        let mut h = serving_cell();
        h.netpolicy_error = Some("apply-failed".into());
        assert_eq!(serving_readiness(&h, at(1)), ReadyState::NotReady("netpolicy"));
        let h = serving_cell();
        assert_eq!(
            serving_readiness(&h, at(601)),
            ReadyState::NotReady("desired-expired")
        );
        // unassigned-but-missing probe stays not ready
        let mut h = serving_cell();
        h.assigned_pods.push("p1".into());
        assert_eq!(
            serving_readiness(&h, at(1)),
            ReadyState::NotReady("pod-not-ready")
        );
    }
}
