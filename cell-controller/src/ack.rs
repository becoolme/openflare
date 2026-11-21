//! `AckSink` — the typed acknowledgement the cell reports back to the
//! control plane (`POST /api/internal/clusters/<clusterId>/vrt/desired/ack`).
//!
//! Wire shape (zod-validated server-side):
//! `{revision, payloadDigest, status: APPLIED|REJECTED|PARTIAL, reason?,
//!   appliedAt}`. `cellId` is NOT sent — the server derives it from the
//! authenticated cluster identity; a forged body can never claim another
//! cell's publication.

use crate::retry::RetryPlan;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use thiserror::Error;

/// Terminal convergence verdict for one publication revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckStatus {
    /// Every assigned pod applied the revision.
    #[serde(rename = "APPLIED")]
    Applied,
    /// Envelope failed verification — nothing was applied.
    #[serde(rename = "REJECTED")]
    Rejected,
    /// Verified, but at least one assigned pod did not apply.
    #[serde(rename = "PARTIAL")]
    Partial,
}

/// One acknowledgement report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AckReport {
    /// Publication revision the report binds to.
    pub revision: u64,
    /// `payloadDigest` of the (claimed or verified) envelope — the server
    /// only accepts the report when it matches the stored publication.
    pub payload_digest: String,
    /// sha256 of the full signed envelope (canonical form) — the server
    /// binds the ack to BOTH digests (T11/F11).
    pub envelope_digest: String,
    /// The cell's persisted revision fence (T11/F5) — lets a restored
    /// control-plane DB lift its counter back above the fence.
    pub fence_revision: u64,
    /// Convergence verdict.
    pub status: AckStatus,
    /// Typed reason code (see [`crate::verify::RejectReason::code`]) or a
    /// short free-form PARTIAL detail — never secrets/envelope content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Cell-side apply completion instant (server clock authoritative).
    pub applied_at: DateTime<Utc>,
    /// T21: runtime capabilities the cell declares — the control plane
    /// only schedules `crons`/ASSETS onto cells that claim support; an
    /// old cell (field absent) is treated as unsupported (fail closed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<AckCapabilities>,
    /// MIG-1: tenant-level failures observed while applying this
    /// revision — the control plane marks exactly these workers failed.
    /// MIG-6: `Some(set)` ALWAYS serializes the key — an explicit `[]`
    /// is the "this revision carries no tenant failures" clear that
    /// lets the server persist a clean diagnosis instead of retaining
    /// a stale one; `None` omits the key entirely and is used ONLY by
    /// the bounded strip-and-retry for a pre-MIG-1 control plane whose
    /// strict schema rejects the unknown key (400).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_failures: Option<Vec<crate::pod::TenantFailure>>,
}

/// Runtime capability bits carried in every ACK (T21). Reported per
/// revision; the latest ack wins server-side.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AckCapabilities {
    /// Cron delivery wired (pods + single-replica lease).
    pub cron: bool,
    /// ASSETS materialization available (host asset sync + provider).
    pub assets: bool,
    /// T35: D1 ops executor wired ([d1sql] config present) — the
    /// control plane only admits D1 bindings onto cells that claim it.
    #[serde(default)]
    pub d1: bool,
    /// T37b: queue consumer dispatcher wired — the control plane only
    /// admits queue consumers onto cells that claim it. Same `[d1sql]`
    /// authority; absent on pre-T37b cells ⇒ consumer deploys 409.
    #[serde(default)]
    pub queue: bool,
    /// W2: `WorkerDesired.workflows` render wired — the control plane
    /// only admits workflow declarations onto cells that claim it.
    /// Absent on pre-W2 cells ⇒ workflow deploys 409 (fail closed).
    #[serde(default)]
    pub workflows: bool,
    /// W3: workflow instance-op executor wired (`/vrt/cell-ops` →
    /// vrt-host `/v1/wfops` → facade). Absent on pre-W3 cells (and on
    /// cells without `[[pods]]`) ⇒ instance ops 409 (fail closed).
    #[serde(default)]
    pub workflows_ops: bool,
    /// W4: D1 ops (EXECUTE/MIGRATIONS_APPLY/DROP) pulled from the
    /// generic `/vrt/cell-ops` channel instead of `/vrt/d1-ops`.
    /// Absent on pre-W4 cells ⇒ dispatch stays on `vrt_d1_ops`
    /// (fail closed).
    #[serde(default)]
    pub d1_cell_ops: bool,
    /// W7: `WorkflowDesired.service_worker_id` understood — the
    /// control plane only admits cross-worker (`script_name`) workflow
    /// declarations onto cells that claim it. Absent ⇒ 409 (fail
    /// closed); same-worker decls remain under `workflows`.
    #[serde(default)]
    pub workflows_cross_worker: bool,
    /// RV-R70: STORAGE_PURGE executor wired (`[storage_purge]` backend
    /// built successfully) — the control plane only dispatches
    /// KV/R2 purge ops onto cells that claim it. Absent ⇒ the purge
    /// op is never queued for this cell (fail closed).
    #[serde(default)]
    pub storage_purge: bool,
    /// KV backend wired on this cell (`[storage_purge]` carries the KV
    /// backend config mirroring the provider's `[kv]`/`[kvrocks]`). The
    /// control plane rejects KV bindings (409 VRT_CAPABILITY_UNSUPPORTED)
    /// unless this is claimed. Omitted from the wire while false so an
    /// older strict control plane keeps accepting acks (rolling upgrade).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub kv: bool,
    /// R2 backend wired on this cell (`R2_LIST/HEAD/GET/PUT/DELETE`
    /// object data ops). The control plane rejects R2 object data ops
    /// (409 VRT_CAPABILITY_UNSUPPORTED) unless claimed. Omitted from the
    /// wire while false (rolling-upgrade compatibility, like `kv`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub r2: bool,
    /// `ServiceBinding.entrypoint` (named `WorkerEntrypoint` export) is
    /// understood by this generation. A pre-entrypoint host rejects the
    /// field via `deny_unknown_fields`, which would refuse the WHOLE
    /// snapshot, so the control plane only admits named entrypoints onto
    /// cells that claim it. Omitted while false (rolling upgrade).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub service_entrypoint: bool,
    /// `workerLoader` bindings render to the pinned fork's
    /// `workerLoader = (maxWorkers=N)` capnp — a cell on an older
    /// workerd pin would fail to parse the whole worker config, so the
    /// control plane only admits loader bindings onto cells that claim
    /// it. Omitted while false (rolling upgrade).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub worker_loader: bool,
    /// Per-worker quotas: this cell's vrt-host renders the workerd
    /// `limits` block (`cpuMs`/`memoryMb`/`subRequests`) for declared
    /// quotas, i.e. its workerd is the VRT fork with the limits patch.
    /// The control plane rejects deploys that declare a non-default
    /// quota (409 `VRT_CAPABILITY_UNSUPPORTED`) unless this is claimed.
    /// Omitted while false so an older strict control plane keeps
    /// accepting acks (rolling upgrade); the control plane must learn
    /// the bit BEFORE any cell enables it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub quotas: bool,
    /// `analyticsEngine` bindings render AND the per-tenant AE ingest
    /// socket is served by this generation's vrt-host (`AeService`
    /// ships in the same release). A pre-AE cell would reject the
    /// `analyticsEngine` wire field via `deny_unknown_fields` — the
    /// control plane only admits dataset bindings onto cells that
    /// claim it. Omitted while false (rolling upgrade).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub analytics_engine: bool,
    /// `tailConsumers` renders to workerd `tails` designators — a
    /// pre-tail cell rejects the `tailConsumers` wire field via
    /// `deny_unknown_fields`, so the control plane only admits
    /// consumer declarations onto cells that claim it. Omitted while
    /// false (rolling upgrade).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tail_consumers: bool,
    /// `{"type":"json"}` bindings (version_metadata, typed vars)
    /// render to workerd `json` — a pre-json cell rejects the unknown
    /// binding variant via `deny_unknown_fields`, so the control plane
    /// only admits them onto cells that claim it. Omitted while false
    /// (rolling upgrade).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub version_metadata: bool,
}

/// Server-side bound for `reason` (zod `.max(256)`) — an oversized
/// reason would reject the WHOLE ack report and stall convergence
/// (RV-R68). Bound at construction, in UTF-16 code units like the
/// server counts.
pub const MAX_REASON_CHARS: usize = 256;

/// Truncate `reason` to the server bound — pod-id lists in
/// `pods-not-applied:<ids>` can grow past 256 units on large cells.
pub fn bounded_reason(reason: Option<String>) -> Option<String> {
    reason.map(|r| crate::execset::truncate_utf16(&r, MAX_REASON_CHARS).0)
}

/// MIG-1: server-side bounds for `tenantFailures` (zod: ≤128 entries,
/// ≤64 workerIds each, names ≤191, code ≤64 lowercase-dash, detail
/// ≤512 UTF-16 units). Enforced at construction so a report can never
/// reject itself over a bound.
pub const MAX_TENANT_FAILURE_ENTRIES: usize = 128;
pub const MAX_FAILURE_WORKER_IDS: usize = 64;
pub const MAX_FAILURE_NAME_CHARS: usize = 191;
pub const MAX_FAILURE_DETAIL_CHARS: usize = 512;
pub const MAX_FAILURE_CODE_CHARS: usize = 64;

/// Bound `tenant_failures` to the wire contract — drops entries that
/// could not be represented rather than risking a whole-report reject.
pub fn bounded_tenant_failures(
    failures: Vec<crate::pod::TenantFailure>,
) -> Vec<crate::pod::TenantFailure> {
    failures
        .into_iter()
        .filter(|f| !f.tenant.is_empty())
        .take(MAX_TENANT_FAILURE_ENTRIES)
        .map(|mut f| {
            f.tenant = crate::execset::truncate_utf16(&f.tenant, MAX_FAILURE_NAME_CHARS).0;
            f.worker_ids = f
                .worker_ids
                .into_iter()
                .filter(|w| !w.is_empty())
                .map(|w| crate::execset::truncate_utf16(&w, MAX_FAILURE_NAME_CHARS).0)
                .take(MAX_FAILURE_WORKER_IDS)
                .collect();
            f.code = crate::execset::truncate_utf16(&f.code, MAX_FAILURE_CODE_CHARS).0;
            f.detail = f
                .detail
                .map(|d| crate::execset::truncate_utf16(&d, MAX_FAILURE_DETAIL_CHARS).0);
            f
        })
        .collect()
}

/// ACK delivery failure — a single `report` retries per [`RetryPlan`];
/// the CONTROLLER additionally re-sends an undelivered APPLIED ack
/// across reconcile ticks under `deps.ack_retry` (T12/G1).
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum AckError {
    /// Attempt timed out.
    #[error("ack delivery timed out")]
    Timeout,
    /// Server rejected the report (409 digest mismatch, 4xx, 5xx).
    #[error("ack endpoint returned HTTP {0}")]
    Status(u16),
    /// Transport failure.
    #[error("ack transport error")]
    Transport,
}

impl AckError {
    /// 4xx is a deterministic server verdict (digest mismatch, terminal
    /// state conflict, auth) — retrying the SAME report cannot change it.
    /// 5xx/timeout/transport are transient. T12/G1 uses this to latch a
    /// terminal state instead of retrying forever.
    pub fn is_terminal(&self) -> bool {
        matches!(self, AckError::Status(code) if (400..500).contains(code))
    }
}

/// Typed sink for acknowledgement reports.
#[async_trait::async_trait]
pub trait AckSink: Send + Sync {
    /// Deliver one report; must be idempotent server-side.
    async fn report(&self, ack: &AckReport) -> Result<(), AckError>;
}

/// HTTP POST implementation (bearer, bounded retries).
pub struct HttpAckSink {
    client: reqwest::Client,
    url: String,
    token: String,
    plan: RetryPlan,
}

impl HttpAckSink {
    /// `timeout` bounds each attempt; `plan` bounds attempts.
    pub fn new(
        url: impl Into<String>,
        token: impl Into<String>,
        timeout: std::time::Duration,
        plan: RetryPlan,
    ) -> Result<Self, AckError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|_| AckError::Transport)?;
        Ok(Self {
            client,
            url: url.into(),
            token: token.into(),
            plan,
        })
    }
}

#[async_trait::async_trait]
impl AckSink for HttpAckSink {
    async fn report(&self, ack: &AckReport) -> Result<(), AckError> {
        let mut attempts = 0usize;
        let mut last = AckError::Transport;
        while self.plan.can_retry(attempts) {
            if attempts > 0 {
                tokio::time::sleep(self.plan.delay_for(attempts)).await;
            }
            attempts += 1;
            match self
                .client
                .post(&self.url)
                .bearer_auth(&self.token)
                .json(ack)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    // 4xx (digest mismatch / validation) is terminal — the
                    // report itself will never be accepted; no retry.
                    if response.status().is_client_error() {
                        return Err(AckError::Status(response.status().as_u16()));
                    }
                    last = AckError::Status(response.status().as_u16());
                }
                Err(e) => {
                    last = if e.is_timeout() {
                        AckError::Timeout
                    } else {
                        AckError::Transport
                    };
                }
            }
        }
        Err(last)
    }
}

/// In-memory sink for tests: records every delivered report; optional
/// scripted failures.
#[derive(Debug, Default)]
pub struct MemoryAckSink {
    sent: Mutex<Vec<AckReport>>,
    /// Every report ATTEMPTED (delivered or rejected) — retry/strip
    /// assertions read the exact sequence the controller emitted.
    attempted: Mutex<Vec<AckReport>>,
    fail_next: Mutex<usize>,
    fail_terminal_next: Mutex<usize>,
    /// Queue of scripted HTTP status rejections consumed in order —
    /// `fail_terminal_next` is the legacy 409 alias kept for older
    /// tests; MIG-6 tests script 400 vs 409 explicitly.
    fail_status_next: Mutex<std::collections::VecDeque<u16>>,
}

impl MemoryAckSink {
    /// Reports delivered so far.
    pub fn sent(&self) -> Vec<AckReport> {
        self.sent.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Reports attempted so far — including the ones scripted to fail.
    pub fn attempted(&self) -> Vec<AckReport> {
        self.attempted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Make the next `n` `report` calls fail with `Transport`.
    pub fn fail_next(&self, n: usize) {
        *self.fail_next.lock().unwrap_or_else(|e| e.into_inner()) = n;
    }

    /// Make the next `n` `report` calls fail with a TERMINAL 409 — the
    /// server-side conflict shape that must latch an observable error
    /// instead of being retried forever (T12/G1).
    pub fn fail_terminal_next(&self, n: usize) {
        *self
            .fail_terminal_next
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = n;
    }

    /// Make the next `report` call fail with the given HTTP status
    /// (queue of one per call; MIG-6 distinguishes 400 schema rejects
    /// from 409 terminal conflicts).
    pub fn fail_status_next(&self, codes: &[u16]) {
        self.fail_status_next
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(codes.iter().copied());
    }
}

#[async_trait::async_trait]
impl AckSink for MemoryAckSink {
    async fn report(&self, ack: &AckReport) -> Result<(), AckError> {
        self.attempted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(ack.clone());
        {
            let mut codes = self
                .fail_status_next
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(code) = codes.pop_front() {
                return Err(AckError::Status(code));
            }
        }
        {
            let mut term = self
                .fail_terminal_next
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if *term > 0 {
                *term -= 1;
                return Err(AckError::Status(409));
            }
        }
        let mut fails = self.fail_next.lock().unwrap_or_else(|e| e.into_inner());
        if *fails > 0 {
            *fails -= 1;
            return Err(AckError::Transport);
        }
        drop(fails);
        self.sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(ack.clone());
        Ok(())
    }
}
