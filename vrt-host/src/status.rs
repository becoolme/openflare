//! Pod status surface — `GET /readyz` for the cell-controller probe
//! and `GET /healthz` for liveness (T14/B1). The status object is a
//! shared snapshot written by the converge loop; the router only
//! renders it — no locks held across awaits.

use crate::assignment::AssignmentError;
use crate::state::TenantState;
use crate::supervisor::TenantRegistry;
use serde::Serialize;
use std::sync::{Arc, RwLock};

/// One worker's readiness line in the `/readyz` payload.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatus {
    /// Tenant id.
    pub tenant: String,
    /// Worker id.
    pub worker: String,
    /// Endpoint installed and open for requests.
    pub ready: bool,
    /// Spec fingerprint of the generation actually serving, when a
    /// runtime exists (T15/R2 — "loaded", not merely "assigned").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving_digest: Option<String>,
    /// Typed reason when not ready (whitelist-stable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// T25/L16: sanitized stderr tail of the last launch failure —
    /// free-text diagnostic (already secret-redacted), never a
    /// reason code. Skipped when empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// MIG-1: one tenant's apply/switch failure, reported beside — never
/// THROUGH — pod readiness. `code` is whitelist-stable; `detail` is the
/// sanitized, bounded diagnostic the worker line already carries.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantFailure {
    /// Tenant id.
    pub tenant: String,
    /// Worker ids of this tenant that hit the failure — MIG-4: only
    /// the workers the failure was ATTRIBUTED to are named; empty
    /// serializes as absent, which the control plane reads as
    /// tenant-wide (changed-set ∩ tenant).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub worker_ids: Vec<String>,
    /// Typed failure code (whitelist-stable).
    pub code: String,
    /// Sanitized bounded diagnostic, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// `/readyz` payload — the controller's probe contract
/// (`PodReadyz` in cell-controller/file_channel.rs).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodReadyz {
    /// POD-LEVEL progress only (MIG-1): the target revision is adopted,
    /// unblocked and every assigned worker either serves the expected
    /// spec or reached a terminal tenant-level verdict listed under
    /// `tenant_failures`. A tenant failure — old generation still
    /// serving, or no prior generation at all — must NOT make the pod
    /// NotReady: one bad worker can never drain the whole cell.
    pub ready: bool,
    /// Snapshot revision this pod has loaded, if any.
    pub revision: Option<u64>,
    /// `payloadDigest` bound to the loaded revision.
    #[serde(rename = "payloadDigest")]
    pub payload_digest: Option<String>,
    /// Per-worker readiness detail.
    pub workers: Vec<WorkerStatus>,
    /// MIG-1: tenant-level failures observed while applying the loaded
    /// revision. Optional — old hosts simply omit it; the controller
    /// must not treat its absence as pod failure.
    #[serde(rename = "tenantFailures", skip_serializing_if = "Vec::is_empty")]
    pub tenant_failures: Vec<TenantFailure>,
    /// Aggregate pod state (`running`/`degraded`/`blocked`).
    pub state: &'static str,
    /// Typed reason when `ready == false` — whitelist codes only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// What the converge loop records after each tick.
#[derive(Clone, Debug, Default)]
pub struct HostStatusInner {
    /// Verified snapshot revision ADOPTED (assignment validated and a
    /// converge pass driven), if any. Never rewound or pre-set — a
    /// blocked adoption keeps reporting the previously adopted value
    /// plus `blocked_reason` (T15/R2).
    pub revision: Option<u64>,
    /// `payloadDigest` of the verified snapshot.
    pub payload_digest: Option<String>,
    /// Fail-closed reason when the assignment/snapshot pair could not
    /// drive convergence — `None` when the pair is adopted.
    pub blocked_reason: Option<String>,
    /// Assigned `(tenant, worker)` pairs the pod is accountable for.
    pub assigned: Vec<(String, String)>,
    /// MIG-10: startup CA trust-store probe (`true` = tenant HTTPS
    /// egress can verify certs). Diagnostics only — never gates
    /// readiness (inbound traffic does not depend on the CA bundle).
    pub egress_trust_store_ok: bool,
}

/// Shared handle — converge writes, readyz reads.
pub type HostStatus = Arc<RwLock<HostStatusInner>>;

/// Fresh status handle.
pub fn new_status() -> HostStatus {
    Arc::new(RwLock::new(HostStatusInner::default()))
}

/// Whitelisted reason codes — keep in sync with
/// `cell-controller/file_channel.rs::POD_REASON_WHITELIST` and the
/// T12 API whitelist (`vrtAckReasonForApi`).
pub const REASON_WHITELIST: &[&str] = &[
    "assignment-mismatch",
    "assignment-incomplete",
    "snapshot-unavailable",
    "workers-not-ready",
    "switch-failed",
    "egress-policy-unavailable",
    "bundle-unavailable",
    "render-inputs-unavailable",
];

fn whitelist(reason: Option<String>) -> Option<String> {
    reason.filter(|r| REASON_WHITELIST.contains(&r.as_str()))
}

/// Caps for the `tenantFailures` list — the probe body must stay small
/// enough for the controller's bounded read.
const MAX_TENANT_FAILURES: usize = 128;
const MAX_FAILURE_WORKER_IDS: usize = 64;

/// Record one worker's tenant-level failure, grouped by (tenant, code)
/// so a tenant with N broken workers yields one entry, not N.
/// MIG-4: `named` is the failure's attributed/excluded worker set —
/// when non-empty the entry names exactly those workers; when empty
/// `workerIds` stays absent so the control plane applies its
/// tenant-wide (changed ∩ tenant) rule.
fn push_tenant_failure(
    out: &mut Vec<TenantFailure>,
    tenant: &str,
    named: &std::collections::BTreeSet<String>,
    code: &'static str,
    detail: Option<String>,
) {
    if let Some(entry) = out
        .iter_mut()
        .find(|e| e.tenant == tenant && e.code == code)
    {
        for id in named.iter().take(MAX_FAILURE_WORKER_IDS) {
            if !entry.worker_ids.contains(id) {
                entry.worker_ids.push(id.clone());
            }
        }
        if entry.detail.is_none() {
            entry.detail = detail;
        }
        return;
    }
    if out.len() >= MAX_TENANT_FAILURES {
        // Over the cap the probe still reports the bounded prefix —
        // never grows unboundedly.
        return;
    }
    out.push(TenantFailure {
        tenant: tenant.to_string(),
        worker_ids: named.iter().take(MAX_FAILURE_WORKER_IDS).cloned().collect(),
        code: code.to_string(),
        detail,
    });
}

/// Compute the current readyz view.
///
/// MIG-1 fault domains: `ready` is a POD-LEVEL verdict — the snapshot is
/// adopted, nothing blocks convergence, and every assigned worker is
/// either serving its expected spec or has a terminal tenant-level
/// verdict. Tenant-level outcomes ride in `tenant_failures` (typed code
/// + bounded sanitized detail) so the control plane can mark exactly the
/// failing workers without the cell going dark. A worker that is still
/// converging (launching, mid-switch, draining between applies) is
/// pod-level pending and DOES keep `ready` false — bounded by the
/// launch/ready timeouts, after which it lands in `tenant_failures`.
/// MIG-8: …but ONLY when its tenant has never served. Once a tenant
/// installed a generation, a later switch/drain window is a
/// tenant-level gap, not pod progress — the pod stays Ready, requests
/// to that worker get the typed `worker_unavailable` 503, and a
/// mid-switch tenant can never evict every other tenant's traffic
/// from the EndpointSlice (the 2026-11-17 incident).
pub fn readyz_view(status: &HostStatus, registry: &Arc<TenantRegistry>) -> PodReadyz {
    /// Per-worker verdict for one assigned (tenant, worker) pair.
    enum Verdict {
        /// Expected spec installed and endpoint open.
        Serving,
        /// Tenant-level terminal verdict — reported, never pod-gating.
        Failed(&'static str),
        /// In flight — launch, switch, drain. Pod-gating ONLY while the
        /// tenant has never served (typed `ever_served`, MIG-8).
        Converging,
        /// Scale-to-zero: deliberately asleep (or waking). Healthy and
        /// never pod-gating — the next wake source launches it. The
        /// worker line reports `ready` with the typed `hibernated`
        /// reason so the control plane does not treat it as an outage.
        Hibernated,
    }
    let inner = status.read().unwrap_or_else(|e| e.into_inner()).clone();
    let mut workers = Vec::new();
    let mut tenant_failures: Vec<TenantFailure> = Vec::new();
    let mut pending = false;
    let mut blocked = inner.blocked_reason.clone();
    for (tenant_id, worker_id) in &inner.assigned {
        let mut serving_digest = None;
        let mut detail = None;
        // MIG-4: the tenant's named failure set — attributed (stderr)
        // or excluded workers; empty ⇒ unattributed (tenant-wide).
        let mut named = std::collections::BTreeSet::new();
        // MIG-8: whether the tenant installed ≥1 generation — a
        // converging verdict only gates the pod while this is false.
        let mut ever_served = false;
        let verdict = match registry.get(tenant_id) {
            Some(rt) => {
                ever_served = rt.ever_served();
                let state = rt.state();
                serving_digest = rt.serving_digest();
                named = rt.named_failure_workers();
                // T25/L16: the launch-failure stderr tail is diagnostic
                // detail — surface it alongside (never AS) the reason.
                detail = rt.last_error();
                match state {
                    // T15/R1: SwitchFailed serves the OLD generation —
                    // a tenant-level verdict, not pod-level evidence.
                    TenantState::SwitchFailed => Verdict::Failed("switch-failed"),
                    TenantState::Crashed => Verdict::Failed("crashed"),
                    TenantState::CircuitOpen => Verdict::Failed("circuit-open"),
                    TenantState::Hibernated | TenantState::Waking => Verdict::Hibernated,
                    _ if rt.excluded_workers().contains(worker_id) =>
                    {
                        // MIG-4: excluded from the rebuilt generation —
                        // serves nothing here; the precise `config-load`
                        // tenant failure marks exactly this worker.
                        Verdict::Failed("config-load")
                    }
                    _ => {
                        let open = rt
                            .endpoint_slot(worker_id)
                            .map(|s| !s.is_closed())
                            .unwrap_or(false);
                        // T15/R2: ready requires the INSTALLED generation
                        // to be the spec the converger last enqueued —
                        // not merely a Ready state left over from the
                        // previous revision.
                        let loaded = rt.expected_digest().is_some()
                            && rt.serving_digest() == rt.expected_digest();
                        if matches!(state, TenantState::Ready) && open && loaded {
                            Verdict::Serving
                        } else {
                            // An assemble failure for the expected spec
                            // is the proximate cause — surface its typed
                            // reason. The map is deliberately a fn, not
                            // exhaustive: converge writes only
                            // whitelist-stable codes (see
                            // `set_assemble_error` call sites).
                            match rt.assemble_error().as_deref() {
                                Some("bundle-unavailable") => {
                                    Verdict::Failed("bundle-unavailable")
                                }
                                Some("isolation-uid-collision") => {
                                    Verdict::Failed("isolation-uid-collision")
                                }
                                Some("render-inputs-unavailable") => {
                                    Verdict::Failed("render-inputs-unavailable")
                                }
                                Some(_) => Verdict::Failed("workers-not-ready"),
                                None if detail.is_some() => {
                                    Verdict::Failed("launch-failed")
                                }
                                None => Verdict::Converging,
                            }
                        }
                    }
                }
            }
            // Assignment recorded but converge has not adopted the
            // tenant yet — pod-level pending.
            None => Verdict::Converging,
        };
        let (ready, reason) = match verdict {
            Verdict::Serving => (true, None),
            Verdict::Hibernated => (true, Some("hibernated".to_string())),
            Verdict::Failed(code) => {
                push_tenant_failure(
                    &mut tenant_failures,
                    tenant_id,
                    &named,
                    code,
                    detail.clone(),
                );
                (false, Some(code.to_string()))
            }
            Verdict::Converging => {
                // MIG-8: only a NEVER-SERVED tenant gates pod
                // readiness (cold start must not take traffic). A
                // served tenant switching/draining between
                // generations is a tenant-level gap — the pod stays
                // ready and the per-worker line still reports
                // `workers-not-ready` for its own requests.
                if !ever_served {
                    pending = true;
                }
                (false, Some("workers-not-ready".to_string()))
            }
        };
        workers.push(WorkerStatus {
            tenant: tenant_id.clone(),
            worker: worker_id.clone(),
            ready,
            serving_digest,
            reason,
            detail,
        });
    }
    // MIG-1: `ready` counts POD progress only — a tenant-level failure
    // does not hold the pod (and every other tenant on it) off the
    // EndpointSlice.
    let all_ready = inner.revision.is_some() && blocked.is_none() && !pending;
    if all_ready {
        blocked = None;
    } else if blocked.is_none() {
        blocked = if inner.revision.is_none() {
            Some("snapshot-unavailable".to_string())
        } else {
            // Pod-level pending always reports `workers-not-ready`;
            // tenant failures are diagnostics, not pod reasons (they
            // are carried in `tenant_failures` instead).
            Some("workers-not-ready".to_string())
        };
    }
    let degraded = !tenant_failures.is_empty();
    PodReadyz {
        ready: all_ready,
        revision: inner.revision,
        payload_digest: inner.payload_digest,
        tenant_failures,
        state: if all_ready && !degraded {
            "running"
        } else if inner.revision.is_some() {
            "degraded"
        } else {
            "blocked"
        },
        reason: whitelist(blocked),
        workers,
    }
}

/// axum router for the control listener: `/readyz` (503 until ready),
/// `/healthz` (liveness — always 200 while the process runs) and
/// `POST /v1/cron/trigger` (T21 cell → pod scheduled dispatch).
///
/// MIG-11: `hold` carries the serialized-switch wait ceilings shared
/// with ingress (production wires `[ingress].switchHold*`); cron,
/// queue and wfops dispatches all wait out a switch window under the
/// same bounds instead of mis-reporting the worker as absent.
pub fn control_router(
    status: HostStatus,
    registry: Arc<TenantRegistry>,
    hold: crate::supervisor::SwitchHoldParams,
) -> axum::Router {
    let status2 = status.clone();
    let registry2 = registry.clone();
    axum::Router::new()
        .merge(crate::cron_trigger::router_with_hold(registry.clone(), hold))
        .merge(crate::queue_dispatch::router_with_hold(registry.clone(), hold))
        .merge(crate::wfops::router_with_hold(registry.clone(), hold))
        // Control-plane stats report: read-only instance listing for the
        // declared class dirs (workflow + DO instance indexes).
        .merge(crate::introspect::router(registry.clone()))
        // B12: `GET /metrics` shares the control listener (standalone
        // / `of serve`); k8s additionally gets a dedicated
        // `metricsListen` port in vrt-host.rs.
        .merge(crate::metrics::router(registry))
        .route(
            "/readyz",
            axum::routing::get(move || {
                let view = readyz_view(&status2, &registry2);
                let code = if view.ready {
                    axum::http::StatusCode::OK
                } else {
                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                };
                std::future::ready((code, axum::Json(view)))
            }),
        )
        .route(
            "/healthz",
            axum::routing::get(|| std::future::ready(axum::http::StatusCode::OK)),
        )
}

/// Map an assignment load failure onto the blocked reason — kept as a
/// free function so the converge path stays a one-liner.
pub fn assignment_reason(e: AssignmentError) -> String {
    e.code().to_string()
}

// ---- T25/M2: on-disk host status for `of status` (standalone) ----

/// One tenant's live state as serialized into `host-status.json`.
/// Fields are diagnostic only — names/digests/reasons, never secrets
/// or env values.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantStatusFile {
    /// Supervisor state machine name (`Ready`, `CircuitOpen`, …).
    pub state: String,
    /// Spec fingerprint the converger last enqueued.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_digest: Option<String>,
    /// Spec fingerprint of the generation actually serving.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving_digest: Option<String>,
    /// Typed assemble failure, when the expected spec could not build.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assemble_error: Option<String>,
    /// Last launch/ready failure reason (+ sanitized stderr tail when
    /// captured). Always Some after a failure — never a bare reason
    /// less state (T25/M2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Per-worker endpoint openness (diagnostics).
    pub workers: Vec<String>,
}

/// The on-disk host status document.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostStatusFile {
    /// Write time — readers treat older files as stale.
    pub ts: String,
    /// Adopted snapshot revision, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    /// Per-tenant live state.
    pub tenants: std::collections::BTreeMap<String, TenantStatusFile>,
    /// MIG-10: CA trust-store probe result (egress TLS capability).
    pub egress_trust_store_ok: bool,
    /// Per-worker quotas: this host renders the workerd `limits` block
    /// (`render.quotas`). `of status` merges this so an operator can
    /// tell "declared but unenforced" from "enforced" without reading
    /// the host config.
    pub quotas_enforced: bool,
}

/// Snapshot the registry + adopted revision into a serializable file.
pub fn status_file_view(
    status: &HostStatus,
    registry: &Arc<TenantRegistry>,
    quotas_enforced: bool,
) -> HostStatusFile {
    let inner = status.read().unwrap_or_else(|e| e.into_inner());
    let mut tenants = std::collections::BTreeMap::new();
    for id in registry.tenant_ids() {
        if let Some(rt) = registry.get(&id) {
            tenants.insert(
                id,
                TenantStatusFile {
                    state: format!("{:?}", rt.state()),
                    expected_digest: rt.expected_digest(),
                    serving_digest: rt.serving_digest(),
                    assemble_error: rt.assemble_error(),
                    last_error: rt.last_error(),
                    workers: rt.worker_ids(),
                },
            );
        }
    }
    HostStatusFile {
        ts: chrono::Utc::now().to_rfc3339(),
        revision: inner.revision,
        tenants,
        egress_trust_store_ok: inner.egress_trust_store_ok,
        quotas_enforced,
    }
}

/// Atomically write `<work_dir>/host-status.json` (0600) — the same
/// file `of status --root <root>` reads at `<root>/work/host-status.json`.
/// Write failures are logged, never fatal (diagnostics must not kill
/// the host).
pub fn write_status_file(
    work_dir: &std::path::Path,
    status: &HostStatus,
    registry: &Arc<TenantRegistry>,
    quotas_enforced: bool,
) {
    let view = status_file_view(status, registry, quotas_enforced);
    let body = match serde_json::to_vec_pretty(&view) {
        Ok(b) => b,
        Err(_) => return,
    };
    let path = work_dir.join("host-status.json");
    if let Err(e) = vrt_securefs::atomic_write(&path, &body, 0o600) {
        tracing::warn!(error = %e, "host-status.json write failed");
    }
}
