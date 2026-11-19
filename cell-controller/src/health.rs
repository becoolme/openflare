//! `/healthz` + `/readyz` surface — fail-closed readiness.
//!
//! `healthz` is process liveness (always 200 while running). `readyz` is
//! only ready when the controller's applied desired is unexpired AND every
//! assigned pod probed ready with the applied revision loaded.
//!
//! The view carries no secrets, payloads, env or tenant data — only
//! revision numbers and pod ids, so it can be scraped freely.

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

/// Evaluate readiness at `now`: applied desired unexpired AND every
/// assigned pod ready AND loaded the applied revision.
pub fn readiness(health: &CellHealth, now: DateTime<Utc>) -> ReadyState {
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
        match readiness(&health, Utc::now()) {
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
}
