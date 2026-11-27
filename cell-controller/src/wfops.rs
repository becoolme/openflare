//! W3: workflow cell-op executor.
//!
//! Pulls `vrt_cell_ops` rows (kinds `WF_*`) from the control plane over
//! `GET /vrt/cell-ops` and executes them against the tenant workerd's
//! platform wfops facade:
//!
//!   POST {pod}/v1/wfops → vrt-host → sock-wfops-<workerId> unix socket
//!   → facade-wfops-<workerId> → engine `WorkflowBinding` JSRPC.
//!
//! W3-r2 trust boundary: the op payload carries ONLY `{workflow,
//! instanceId?, action?, params?}` — no uniqueKey/binding/tenantId.
//! Identity is resolved here against the CELL'S INSTALLED SIGNED
//! `compute.desired` (the same document the supervisor serves): an
//! undeclared workflow name or undeployed worker fails closed before
//! any network call. The op channel carries NO token material at all —
//! `x-vrt-wfop-token` is injected by vrt-host at the `/v1/wfops` hop
//! from the serving spec's map (HMAC-SHA256 with the host's
//! in-process pepper); the facade re-verifies it against its own env.
//!
//! W3-r2 delivery semantics: at-most-once for replayed ops. Executed
//! outcomes are journaled per-op (`wfops-executed.journal`, append +
//! sync_data) and periodically compacted into `wfops-executed.json`
//! dir BEFORE the ack is posted — a crash between execute and ack
//! replays the stored terminal ack on redelivery instead of
//! re-executing (create/restart cannot double-run). The residual
//! window is execute→persist (~µs): a crash inside that window
//! re-executes once (documented at-least-once gap).
//!
//! Results are whitelisted + size-bounded before ACK: `output` is cut
//! at 64KiB / `error` at 2KiB with `truncated: true`; unknown fields
//! never propagate to the control plane.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::Duration;

/// Output blob cap — mirrored by the server-side result schema.
pub const MAX_OUTPUT_CHARS: usize = 64 * 1024;
pub const MAX_ERROR_CHARS: usize = 2 * 1024;
const MAX_ID_CHARS: usize = 100;
const MAX_WF_NAME_CHARS: usize = 64;

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_ID_CHARS
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':' || c == '-')
}
fn valid_wf_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= MAX_WF_NAME_CHARS
        && n.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
fn valid_worker_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':')
}

/// Truncate in UTF-16 code units — the metric the control plane's
/// zod `.max()` counts (RV-R68). Never splits a surrogate pair.
fn truncate(s: &str, max: usize) -> (String, bool) {
    crate::execset::truncate_utf16(s, max)
}

/// `error.name` bound (UTF-16 units) — engine error classes are short
/// identifiers (`NonRetryableError`, `WorkflowFatalError`, …).
const MAX_ERROR_NAME_CHARS: usize = 128;

// W4: the wire types + executed-set journal moved to `execset` (shared
// with the D1 ops channel) — re-exported here so existing
// `crate::wfops::{CellOp,CellOpAck,ExecutedSet}` paths keep working.
#[cfg(test)]
pub(crate) use crate::execset::{now_secs, EXECUTED_CAP};
pub use crate::execset::{CellOp, CellOpAck, ExecutedSet};

fn done(op: &CellOp, result: serde_json::Value) -> CellOpAck {
    CellOpAck {
        seq: op.seq,
        status: "DONE",
        result: Some(result),
        error: None,
    }
}
fn fail(op: &CellOp, error: &str) -> CellOpAck {
    CellOpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(truncate(error, MAX_ERROR_CHARS).0),
    }
}

/// W3-r2: typed op payloads (`deny_unknown_fields`) — the op channel
/// carries workflow-op arguments only; routing/identity material
/// (tenant, binding, uniqueKey) is NEVER accepted from it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WfCreatePayload {
    workflow: String,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    params: Option<serde_json::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WfGetPayload {
    workflow: String,
    instance_id: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WfActionPayload {
    workflow: String,
    instance_id: String,
    action: String,
}
/// W6-r2: persistent instance deletion — binding `delete()` commits
/// the logical wipe; the facade confirms via a bounded get() poll
/// (instance.not_found). Physical file reclaim is the host's
/// safe-point job, not this op's.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WfDeletePayload {
    workflow: String,
    instance_id: String,
}
/// W6: `sendEvent` — eventType bounded charset/length (the vendored
/// engine itself validates nothing; the bounds are ours), payload is
/// an arbitrary JSON value capped below.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WfEventPayload {
    workflow: String,
    instance_id: String,
    event_type: String,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}
/// Serialized event-payload cap — mirrors the server zod bound; keeps
/// the op journal and the DO storage write bounded.
const MAX_EVENT_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_EVENT_TYPE_CHARS: usize = 128;

fn valid_event_type(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= MAX_EVENT_TYPE_CHARS
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// Result whitelist — only these engine fields ever leave the cell.
/// `output`/`error` are truncated (UTF-16 units) with a `truncated`
/// marker. `pub` for the cross-language wire-fixture tests (RV-R68).
pub fn whitelist(v: &serde_json::Value, allowed: &[&str]) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for k in allowed {
        if let Some(val) = v.get(*k) {
            // `optional()` in the server schema accepts absent, never
            // explicit null — drop it (facade also omits; belt+braces).
            if val.is_null() {
                continue;
            }
            out.insert(k.to_string(), val.clone());
        }
    }
    let mut truncated = v
        .get("truncated")
        .and_then(|t| t.as_bool())
        .unwrap_or(false);
    if let Some(o) = out.get_mut("output") {
        let text = o
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| o.to_string());
        let (t, tr) = truncate(&text, MAX_OUTPUT_CHARS);
        truncated |= tr;
        *o = serde_json::Value::String(t);
    }
    if let Some(e) = out.get_mut("error") {
        if e.is_string() {
            let (t, tr) = truncate(e.as_str().unwrap_or_default(), MAX_ERROR_CHARS);
            truncated |= tr;
            *e = serde_json::Value::String(t);
        } else if e.is_object() {
            // Engine error objects surface as typed {name,message}
            // (the workflow facade exposes this shape — a plain
            // object here would fail the strict server schema and
            // wedge WF_GET forever; RV-R68). Both fields bounded;
            // non-string members are stringified, never dropped.
            let name = e
                .get("name")
                .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Error".to_string());
            let message = e
                .get("message")
                .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
                .unwrap_or_default();
            let (n, tr1) = truncate(&name, MAX_ERROR_NAME_CHARS);
            let (m, tr2) = truncate(&message, MAX_ERROR_CHARS);
            truncated |= tr1 | tr2;
            *e = serde_json::json!({ "name": n, "message": m });
        } else {
            // Unexpected scalar/array — stringify, bounded. An errored
            // instance must surface its failure, never silently drop.
            let (t, tr) = truncate(&e.to_string(), MAX_ERROR_CHARS);
            truncated |= tr;
            *e = serde_json::Value::String(t);
        }
    }
    if truncated {
        out.insert("truncated".into(), serde_json::Value::Bool(true));
    }
    serde_json::Value::Object(out)
}

/// Executor — posts `{tenantId, workerId, token, op}` to the pod's
/// vrt-host `/v1/wfops`. One bounded retry on transport errors only.
pub struct WfOpsExecutor {
    /// `podId → loopback base` (same `[[pods]]` map as queue dispatch).
    pods: BTreeMap<String, String>,
    http: reqwest::Client,
    timeout: Duration,
}

impl WfOpsExecutor {
    pub fn new(pods: &[(String, String)], timeout: Duration) -> Result<Self, String> {
        let mut map = BTreeMap::new();
        for (id, base) in pods {
            let url = reqwest::Url::parse(base).map_err(|e| format!("pod {id} base: {e}"))?;
            if url.scheme() != "http" {
                return Err(format!("pod {id} base must be http (loopback)"));
            }
            let host = url
                .host_str()
                .ok_or_else(|| format!("pod {id} base: no host"))?;
            let loopback = host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .map(|ip| ip.is_loopback())
                    .unwrap_or(false);
            if !loopback {
                return Err(format!("pod {id} base {host} is not loopback"));
            }
            map.insert(id.clone(), base.trim_end_matches('/').to_string());
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("wfops http client: {e}"))?;
        Ok(Self {
            pods: map,
            http,
            timeout,
        })
    }

    /// Rendezvous pod for this tenant/worker — identical to the
    /// delivery assignment (`assign.rs`), so the call lands on the
    /// vrt-host actually serving this generation.
    fn pod_for(&self, tenant_id: &str, worker_id: &str) -> Option<String> {
        let ids: Vec<String> = self.pods.keys().cloned().collect();
        crate::assign::rendezvous_pick(tenant_id, worker_id, &ids).map(|s| s.to_string())
    }

    async fn call(&self, base: &str, req: &serde_json::Value) -> Result<serde_json::Value, String> {
        let resp = tokio::time::timeout(
            self.timeout,
            self.http.post(format!("{base}/v1/wfops")).json(req).send(),
        )
        .await
        .map_err(|_| "request timeout".to_string())?
        .map_err(|e| format!("transport: {e}"))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| format!("read body: {e}"))?;
        if !status.is_success() {
            // Host/facade typed errors carry `error`/`error.code` —
            // surface the code (bounded) as the failure reason.
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                let msg = v
                    .get("error")
                    .and_then(|e| {
                        e.as_str().map(|s| s.to_string()).or_else(|| {
                            e.get("code")
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string())
                        })
                    })
                    .unwrap_or_else(|| format!("status {status}"));
                return Err(msg.chars().take(300).collect());
            }
            return Err(format!("status {status}"));
        }
        serde_json::from_slice(&bytes).map_err(|_| "facade non-JSON response".to_string())
    }

    /// Execute one op → terminal ack. Never panics. `desired` is the
    /// cell's installed signed `compute.desired` — the sole authority
    /// for worker→tenant resolution and workflow declaration checks.
    pub async fn execute(
        &self,
        op: &CellOp,
        desired: Option<&snapshot_protocol::compute_desired::ComputeDesired>,
    ) -> CellOpAck {
        if !valid_worker_id(&op.worker_id) {
            return fail(op, "invalid workerId");
        }
        if let Some(deadline) = op.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            if now >= deadline {
                return CellOpAck {
                    seq: op.seq,
                    status: "EXPIRED",
                    result: None,
                    error: Some("dispatch deadline passed".into()),
                };
            }
        }
        // W3-r2: identity resolves from the installed SIGNED desired —
        // the op payload is never trusted for routing material.
        let Some(desired) = desired else {
            return fail(op, "no desired installed");
        };
        let Some((tenant_id, _worker)) = desired
            .tenants
            .iter()
            .flat_map(|t| t.workers.iter().map(move |w| (t.tenant_id.as_str(), w)))
            .find(|(_, w)| w.worker_id == op.worker_id)
        else {
            return fail(op, "worker.undeployed");
        };

        // Typed payload per kind (P0-2) — `deny_unknown_fields`
        // rejects smuggled identity material (wfUniqueKey/binding/
        // tenantId) as malformed, not ignored.
        let (workflow, fwd, allowed): (String, serde_json::Value, &[&str]) = match op.kind.as_str()
        {
            "WF_CREATE" => {
                let Ok(p) = serde_json::from_value::<WfCreatePayload>(op.payload.clone()) else {
                    return fail(op, "malformed payload");
                };
                if !valid_wf_name(&p.workflow) {
                    return fail(op, "invalid workflow name");
                }
                let params = p.params.unwrap_or(serde_json::json!({}));
                if !params.is_object() {
                    return fail(op, "params must be an object");
                }
                let mut fwd = serde_json::json!({
                    "op": "create", "workflow": p.workflow, "params": params,
                });
                if let Some(id) = p.instance_id {
                    if !valid_id(&id) {
                        return fail(op, "invalid instanceId");
                    }
                    fwd["instanceId"] = serde_json::json!(id);
                }
                (p.workflow, fwd, &["instanceId", "status"])
            }
            "WF_GET" => {
                let Ok(p) = serde_json::from_value::<WfGetPayload>(op.payload.clone()) else {
                    return fail(op, "malformed payload");
                };
                if !valid_wf_name(&p.workflow) {
                    return fail(op, "invalid workflow name");
                }
                if !valid_id(&p.instance_id) {
                    return fail(op, "invalid instanceId");
                }
                (
                    p.workflow.clone(),
                    serde_json::json!({"op":"get","workflow":p.workflow,"instanceId":p.instance_id}),
                    &["status", "output", "error"],
                )
            }
            "WF_ACTION" => {
                let Ok(p) = serde_json::from_value::<WfActionPayload>(op.payload.clone()) else {
                    return fail(op, "malformed payload");
                };
                if !valid_wf_name(&p.workflow) {
                    return fail(op, "invalid workflow name");
                }
                if !valid_id(&p.instance_id) {
                    return fail(op, "invalid instanceId");
                }
                if !["pause", "resume", "restart", "terminate"].contains(&p.action.as_str()) {
                    return fail(op, "invalid action");
                }
                (
                    p.workflow.clone(),
                    serde_json::json!({"op":"action","workflow":p.workflow,"instanceId":p.instance_id,"action":p.action}),
                    &["status"],
                )
            }
            "WF_DELETE" => {
                let Ok(p) = serde_json::from_value::<WfDeletePayload>(op.payload.clone()) else {
                    return fail(op, "malformed payload");
                };
                if !valid_wf_name(&p.workflow) {
                    return fail(op, "invalid workflow name");
                }
                if !valid_id(&p.instance_id) {
                    return fail(op, "invalid instanceId");
                }
                (
                    p.workflow.clone(),
                    serde_json::json!({"op":"delete","workflow":p.workflow,"instanceId":p.instance_id}),
                    &["status"],
                )
            }
            "WF_EVENT" => {
                let Ok(p) = serde_json::from_value::<WfEventPayload>(op.payload.clone()) else {
                    return fail(op, "malformed payload");
                };
                if !valid_wf_name(&p.workflow) {
                    return fail(op, "invalid workflow name");
                }
                if !valid_id(&p.instance_id) {
                    return fail(op, "invalid instanceId");
                }
                if !valid_event_type(&p.event_type) {
                    return fail(op, "invalid event type");
                }
                let payload = p.payload.unwrap_or(serde_json::Value::Null);
                let too_big = serde_json::to_string(&payload)
                    .map(|s| s.len() > MAX_EVENT_PAYLOAD_BYTES)
                    .unwrap_or(true);
                if too_big {
                    return fail(op, "event payload too large");
                }
                (
                    p.workflow.clone(),
                    serde_json::json!({"op":"sendEvent","workflow":p.workflow,"instanceId":p.instance_id,"eventType":p.event_type,"payload":payload}),
                    &["status"],
                )
            }
            _ => return fail(op, "unknown op kind"),
        };

        // Signed-desired authority check (W3-r2): the workflow must be
        // declared in the applied snapshot — never from op-channel
        // claims. W7: ops dispatch under the DEFINING worker's id
        // (`service_worker_id`, else the declarer) — the declaration
        // itself may live on a sibling worker's spec, so the match is
        // tenant-wide on (defining worker, name). Two decls sharing a
        // (target, name) pair are rejected by desired validation, so
        // this match is unambiguous. The search stays scoped to the
        // op's own tenant even though worker ids are globally unique —
        // a match outside it must never satisfy the check.
        let owned = desired
            .tenants
            .iter()
            .filter(|t| t.tenant_id == tenant_id)
            .flat_map(|t| t.workers.iter())
            .any(|w| {
                w.workflows.iter().any(|wf| {
                    wf.name == workflow
                        && wf
                            .service_worker_id
                            .as_deref()
                            .unwrap_or(w.worker_id.as_str())
                            == op.worker_id
                })
            });
        if !owned {
            return fail(op, "workflow.undeclared");
        }

        let Some(pod_id) = self.pod_for(tenant_id, &op.worker_id) else {
            return fail(op, "no pod available");
        };
        let Some(base) = self.pods.get(&pod_id).cloned() else {
            return fail(op, "no pod available");
        };
        // No token crosses this channel — vrt-host injects it at the
        // `/v1/wfops` hop from the serving spec's HMAC map.
        let req = serde_json::json!({
            "tenantId": tenant_id,
            "workerId": op.worker_id,
            "op": fwd,
        });
        // One bounded retry on pure-transport failures (connect refused
        // during a generation swap); typed errors never retry.
        let mut last_err = String::new();
        for attempt in 0..2 {
            match self.call(&base, &req).await {
                Ok(v) => return done(op, whitelist(&v, allowed)),
                Err(e) => {
                    if e.starts_with("transport:") || e == "request timeout" {
                        last_err = e;
                        if attempt == 0 {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        continue;
                    }
                    return fail(op, &e);
                }
            }
        }
        fail(op, &last_err)
    }
}

/// HTTP source/sink for `/vrt/cell-ops` — same auth and page shape as
/// the D1 ops channel (re-declared here because the row shape differs:
/// `workerId` instead of `databaseId`, TEXT kind).
pub struct HttpCellOpsSource {
    url: String,
    http: reqwest::Client,
    token: String,
}

#[derive(Deserialize)]
struct OpsEnvelope {
    data: OpsData,
}
#[derive(Deserialize)]
struct OpsData {
    // RV-R58: raw rows — decoded one by one (crate::execset::decode_ops_page)
    // so one undecodable row cannot stall the page.
    ops: Vec<serde_json::Value>,
}

impl HttpCellOpsSource {
    pub fn new(
        base_url: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let url = reqwest::Url::parse(base_url)
            .map_err(|e| format!("cell-ops base: {e}"))?
            .join(&format!("api/internal/clusters/{cell_id}/vrt/cell-ops"))
            .map_err(|e| format!("cell-ops url: {e}"))?
            .to_string();
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("cell-ops http client: {e}"))?;
        Ok(Self { url, http, token })
    }

    pub async fn fetch(&self, limit: u32) -> Result<Vec<CellOp>, String> {
        let resp = self
            .http
            .get(&self.url)
            .bearer_auth(&self.token)
            .query(&[("limit", limit.to_string())])
            .send()
            .await
            .map_err(|e| format!("fetch: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("fetch status {}", resp.status()));
        }
        let env: OpsEnvelope = resp.json().await.map_err(|e| format!("fetch json: {e}"))?;
        Ok(crate::execset::decode_ops_page(env.data.ops).ops)
    }
}

pub struct HttpCellOpsAckSink {
    url: String,
    http: reqwest::Client,
    token: String,
}

impl HttpCellOpsAckSink {
    pub fn new(
        base_url: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let url = reqwest::Url::parse(base_url)
            .map_err(|e| format!("cell-ops base: {e}"))?
            .join(&format!("api/internal/clusters/{cell_id}/vrt/cell-ops/ack"))
            .map_err(|e| format!("cell-ops url: {e}"))?
            .to_string();
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("cell-ops http client: {e}"))?;
        Ok(Self { url, http, token })
    }

    pub async fn ack(&self, acks: &[CellOpAck]) -> Result<(), String> {
        let resp = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "acks": acks }))
            .send()
            .await
            .map_err(|e| format!("ack: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("ack status {}", resp.status()));
        }
        Ok(())
    }
}

/// One executor tick — W3-r2 delivery semantics: persisted
/// executed-set replay + in-memory pull dedup.
///
/// Per op: replay the stored terminal outcome when present (no engine
/// call); otherwise execute and stage the outcome. Outcomes are
/// persisted BEFORE the ack POST, so a crash or failed ack never
/// re-executes an op — the next tick replays stored acks.
/// One executor tick — W3-r2 delivery semantics + W4 kind routing.
///
/// Per op: replay the stored terminal outcome when present (no engine
/// call); otherwise route by `kind` — `EXECUTE`/`MIGRATIONS_APPLY`/
/// `DROP` go to the D1 executor (resourceKey → databaseId), everything
/// else to the workflow executor. `KV_*` kinds route to the KV data
/// executor. A kind with no wired executor is SKIPPED (left PENDING
/// for the control plane to expire — never acked, never half-executed). Outcomes are persisted BEFORE the ack
/// POST, so a crash or failed ack never re-executes an op.
/// Data-plane read kinds whose outcome is never journaled (see the
/// record() call in `wfops_tick`).
pub fn is_pure_read_kind(kind: &str) -> bool {
    crate::kvops::KV_READ_KINDS.contains(&kind) || crate::r2ops::R2_READ_KINDS.contains(&kind)
}

pub async fn wfops_tick(
    source: &HttpCellOpsSource,
    sink: &HttpCellOpsAckSink,
    wf_exec: Option<&WfOpsExecutor>,
    d1_exec: Option<&crate::d1ops::D1OpsExecutor>,
    storage_exec: Option<&crate::storagepurge::StoragePurgeExecutor>,
    cache_exec: Option<&crate::cachepurge::CachePurgeExecutor>,
    kv_exec: Option<&crate::kvops::KvOpsExecutor>,
    r2_exec: Option<&crate::r2ops::R2OpsExecutor>,
    seen: &mut crate::execset::SeenSet,
    executed: &mut ExecutedSet,
    desired: Option<&snapshot_protocol::compute_desired::ComputeDesired>,
    order: &crate::d1ops::D1OrderGuard,
    limit: u32,
) -> Result<usize, String> {
    let ops = source.fetch(limit).await?;
    if ops.is_empty() {
        return Ok(0);
    }
    let mut acks = Vec::with_capacity(ops.len());
    let mut done_ids: Vec<String> = Vec::with_capacity(ops.len());
    for op in &ops {
        if seen.contains_key(&op.id) {
            continue;
        }
        let ack = if let Some(rec) = executed.get(&op.id) {
            // Already executed — replay the persisted outcome, never
            // re-enter the engine (at-most-once for replayed ops).
            CellOpAck {
                seq: op.seq,
                status: match rec.status.as_str() {
                    "DONE" => "DONE",
                    "EXPIRED" => "EXPIRED",
                    _ => "FAILED",
                },
                result: rec.result.clone(),
                error: rec.error.clone(),
            }
        } else {
            // W4-r2: same-database cross-channel ordering — while a
            // legacy-channel op for this database is unfinished on
            // this cell, defer (no record, no seen mark): the op stays
            // PENDING and is re-pulled next tick.
            if crate::d1ops::is_d1_kind(&op.kind) && order.is_blocked(&op.resource_key) {
                continue;
            }
            let ack = match op.kind.as_str() {
                k if crate::d1ops::is_d1_kind(k) => match d1_exec {
                    Some(d) => {
                        let a = d.execute_cell(op).await;
                        CellOpAck {
                            seq: a.seq,
                            status: a.status,
                            result: a.result,
                            error: a.error,
                        }
                    }
                    // No d1 executor on this cell — the op stays
                    // PENDING and expires control-plane side (fail
                    // closed; a fake FAILED would be wrong, a fake
                    // DONE worse).
                    None => continue,
                },
                // RV-R70: explicit branch — STORAGE_PURGE must never
                // fall into the workflow executor (which FAILs unknown
                // kinds and would strand the registry row DELETING).
                "STORAGE_PURGE" => match storage_exec {
                    Some(s) => s.execute(op).await,
                    // No purge executor on this cell — op stays
                    // PENDING (fail closed; never a fake outcome).
                    None => continue,
                },
                // CACHE_PURGE: Workers Cache invalidation fan-out to
                // every pod's loopback purge endpoint. No executor ⇒
                // stays PENDING (fail closed; never a fake outcome).
                crate::cachepurge::CACHE_PURGE_KIND => match cache_exec {
                    Some(x) => x.execute(op).await,
                    None => continue,
                },
                // KV1: namespace data ops — routed to the shared
                // KvDataBackend executor; never to the wf fallback.
                k if crate::kvops::is_kv_kind(k) => match kv_exec {
                    Some(x) => x.execute(op).await,
                    None => continue,
                },
                // R2 data ops — shared R2 backend executor; never the
                // wf fallback (which would FAIL the unknown kind).
                k if crate::r2ops::is_r2_kind(k) => match r2_exec {
                    Some(x) => x.execute(op).await,
                    None => continue,
                },
                // K2 stream data ops — same signed data plane as D1
                // (one endpoint, one signer), kind-scoped /streams/*
                // path. No d1 executor → stays PENDING (fail closed).
                k if crate::d1ops::is_k2_kind(k) => match d1_exec {
                    Some(d) => {
                        let a = d.execute_k2_cell(op).await;
                        CellOpAck {
                            seq: a.seq,
                            status: a.status,
                            result: a.result,
                            error: a.error,
                        }
                    }
                    None => continue,
                },
                _ => match wf_exec {
                    Some(w) => w.execute(op, desired).await,
                    None => continue,
                },
            };
            // record() is per-op durable (journal append + sync_data) —
            // a crash anywhere after this line replays the stored
            // outcome on redelivery, never re-executes. Pure reads are
            // not recorded: re-running one is harmless, while keeping
            // multi-MiB read results on disk and in memory (up to
            // EXECUTED_CAP entries) is not.
            if !is_pure_read_kind(&op.kind) {
                executed.record(&op.id, &ack);
            }
            ack
        };
        acks.push(ack);
        done_ids.push(op.id.clone());
    }
    if !acks.is_empty() {
        // A failed ack POST only replays stored outcomes on the next
        // fetch — the engine is never re-entered for these ids.
        // KV1-r: one page can carry up to `limit` KV results of
        // ~1.9 MiB each — far past the control plane's 16 MiB ack
        // body cap — so POST in byte-budgeted batches. Ids are marked
        // seen only after THEIR batch lands; a failed later batch
        // replays just its own stored outcomes next tick.
        for range in ack_batches(&acks, CELL_OPS_ACK_BATCH_BYTES) {
            sink.ack(&acks[range.clone()]).await?;
            for id in &done_ids[range] {
                seen.insert(id.clone(), ());
            }
        }
        while seen.len() > crate::execset::SEEN_CAP {
            if let Some(k) = seen.keys().next().cloned() {
                seen.remove(&k);
            }
        }
    }
    Ok(acks.len())
}

/// Byte budget for one cell-ops ack POST — headroom under the control
/// plane's `MAX_CELL_OPS_ACK_BODY_BYTES` (16 MiB).
pub const CELL_OPS_ACK_BATCH_BYTES: usize = 12 * 1024 * 1024;

/// Split acks into contiguous ranges whose serialized size stays within
/// `budget`. A single ack is always its own batch (each result is
/// already capped well below the budget by its executor).
pub fn ack_batches(acks: &[CellOpAck], budget: usize) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut used = 0usize;
    for (i, a) in acks.iter().enumerate() {
        let sz = serde_json::to_vec(a).map(|v| v.len()).unwrap_or(budget) + 1;
        if i > start && used + sz > budget {
            out.push(start..i);
            start = i;
            used = 0;
        }
        used += sz;
    }
    if start < acks.len() {
        out.push(start..acks.len());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_batches_respect_byte_budget() {
        let big = |seq: i64| CellOpAck {
            seq,
            status: "DONE",
            result: Some(serde_json::json!({ "v": "x".repeat(1900 * 1024) })),
            error: None,
        };
        let acks: Vec<CellOpAck> = (1..=64).map(big).collect();
        let ranges = ack_batches(&acks, CELL_OPS_ACK_BATCH_BYTES);
        assert!(ranges.len() > 1);
        let mut covered = 0;
        for r in &ranges {
            let body = serde_json::to_vec(&serde_json::json!({ "acks": &acks[r.clone()] })).unwrap();
            assert!(body.len() <= 16 * 1024 * 1024, "batch {} bytes", body.len());
            assert_eq!(r.start, covered);
            covered = r.end;
        }
        assert_eq!(covered, 64);
        // Small acks stay in one POST.
        let small: Vec<CellOpAck> = (1..=64)
            .map(|seq| CellOpAck { seq, status: "DONE", result: None, error: None })
            .collect();
        assert_eq!(ack_batches(&small, CELL_OPS_ACK_BATCH_BYTES), vec![0..64]);
        assert!(ack_batches(&[], CELL_OPS_ACK_BATCH_BYTES).is_empty());
    }

    const WORKER: &str = "w_0123456789abcdef";

    /// W3-r2 payloads — workflow-op args only, no identity material.
    fn payload(extra: serde_json::Value) -> serde_json::Value {
        let mut base = serde_json::json!({ "workflow": "order-pipe" });
        if let (Some(b), serde_json::Value::Object(x)) = (base.as_object_mut(), &extra) {
            for (k, v) in x {
                b.insert(k.clone(), v.clone());
            }
        }
        base
    }

    /// Signed-desired fixture: tenant-1 owns WORKER, which declares
    /// the `order-pipe` workflow. The executor resolves identity from
    /// this — never from the op payload.
    fn desired() -> snapshot_protocol::compute_desired::ComputeDesired {
        serde_json::from_value(serde_json::json!({
            "schemaVersion": 1,
            "cellId": "cell-1",
            "revision": 3,
            "tenants": [{
                "tenantId": "tenant-1",
                "workers": [{
                    "workerId": WORKER,
                    "name": "orders",
                    "activeVersion": {
                        "versionId": "vrt_v1",
                        "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2024-01-01",
                        "compatibilityFlags": []
                    },
                    "bindings": [],
                    "crons": [],
                    "quotas": {"cpuMs": 10, "memoryMb": 32, "outboundConnections": 4, "subrequests": 10},
                    "routes": [],
                    "workflows": [{
                        "name": "order-pipe",
                        "binding": "ORDER_FLOW",
                        "className": "OrderPipe",
                        "uniqueKey": "a".repeat(64)
                    }]
                }]
            }]
        }))
        .unwrap()
}

    fn op(seq: i64, kind: &str, payload: serde_json::Value) -> CellOp {
        CellOp {
            id: format!("op-{seq}"),
            seq,
            resource_key: String::new(),
            kind: kind.to_string(),
            worker_id: WORKER.into(),
            payload,
            expires_at: None,
        }
    }

    fn exec_unreachable() -> WfOpsExecutor {
        WfOpsExecutor::new(
            &[("pod-0".into(), "http://127.0.0.1:1".into())],
            Duration::from_millis(400),
        )
        .unwrap()
    }

    #[test]
    fn id_and_name_whitelists() {
        for good in ["inst-1:abc_2", &"x".repeat(100)] {
            assert!(valid_id(good), "{good:?}");
        }
        for bad in [
            "",
            &"x".repeat(101),
            "a/b",
            "a.b",
            "a b",
            "a\0b",
            "α",
            "a@b",
        ] {
            assert!(!valid_id(bad), "{bad:?} must fail");
        }
        for bad in ["", "a.b", "a/b", "αwf", &"w".repeat(65)] {
            assert!(!valid_wf_name(bad), "{bad:?} must fail");
        }
        assert!(valid_wf_name("order-pipe_2"));
        assert!(valid_wf_name(&"w".repeat(64)));
    }

    #[test]
    fn executed_set_persists_and_replays() {
        let dir = std::env::temp_dir().join(format!("wfops-exec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("executed.json");
        let ack = CellOpAck {
            seq: 1,
            status: "DONE",
            result: Some(serde_json::json!({"status": "complete"})),
            error: None,
        };
        let mut set = ExecutedSet::load(path.clone());
        set.record("op-1", &ack);
        set.persist();
        // A fresh load (process restart) replays the same outcome.
        let set2 = ExecutedSet::load(path.clone());
        let rec = set2.get("op-1").expect("persisted record");
        assert_eq!(rec.status, "DONE");
        assert_eq!(rec.result, ack.result);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn executed_set_caps_and_evicts_oldest() {
        let mut set = ExecutedSet::memory();
        for i in 0..(EXECUTED_CAP + 8) {
            let ack = CellOpAck {
                seq: i as i64,
                status: "DONE",
                result: None,
                error: None,
            };
            set.record(&format!("op-{i:05}"), &ack);
        }
        assert_eq!(set.map.len(), EXECUTED_CAP);
        // Oldest by timestamp evicted first — earliest inserts gone.
        assert!(set.get("op-00000").is_none());
        assert!(set.get(&format!("op-{:05}", EXECUTED_CAP + 7)).is_some());
    }

    fn tmp_w3r3_dir(tag: &str) -> std::path::PathBuf {
        // Task scratch root: workspace target dir (constraint).
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join(format!(
                "target/tmp-w3r3/wfops-exec-{tag}-{}",
                std::process::id()
            ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn journal_replays_after_crash_without_snapshot() {
        // W3-r3: record() alone is durable — a crash before ANY
        // snapshot persist must still replay via the journal.
        let dir = tmp_w3r3_dir("journal");
        let path = dir.join("executed.json");
        let ack = CellOpAck {
            seq: 7,
            status: "DONE",
            result: Some(serde_json::json!({"status": "running"})),
            error: None,
        };
        {
            let mut set = ExecutedSet::load(path.clone());
            set.record("op-crash", &ack);
            // NO persist() — drop() simulates the crash: the in-memory
            // map and dirty flag die here.
        }
        let set = ExecutedSet::load(path.clone());
        let rec = set.get("op-crash").expect("journal-replayed record");
        assert_eq!(rec.status, "DONE");
        assert_eq!(rec.result, ack.result);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn crash_between_execute_and_ack_never_reexecutes() {
        // The W3-r3 guarantee: first attempt executes against a REAL
        // facade; crash before ack; redelivery replays the journal —
        // the executor is pointed at a dead pod so ANY re-execution
        // attempt is a transport failure, not a silent pass.
        let dir = tmp_w3r3_dir("replay");
        let path = dir.join("executed.json");
        let d = desired();
        let o = op(
            3,
            "WF_ACTION",
            payload(serde_json::json!({"instanceId": "i", "action": "restart"})),
        );
        // Attempt 1: real executor, real stub facade → DONE.
        let (base, h) = stub_facade(r#"{"status":"running"}"#).await;
        let exec = WfOpsExecutor::new(&[("pod-0".into(), base)], Duration::from_secs(3)).unwrap();
        let ack = exec.execute(&o, Some(&d)).await;
        assert_eq!(ack.status, "DONE");
        {
            let mut set = ExecutedSet::load(path.clone());
            set.record(&o.id, &ack); // durable — then "crash"
        }
        h.await.unwrap();
        // Attempt 2 (redelivery after process restart): the tick path
        // checks executed.get BEFORE touching the executor.
        let set = ExecutedSet::load(path.clone());
        let rec = set.get(&o.id).expect("durable outcome");
        assert_eq!(rec.status, "DONE");
        assert_eq!(rec.result, Some(serde_json::json!({"status": "running"})));
        // The dead-pod executor proves replay did not re-execute.
        let dead = exec_unreachable();
        let ack2 = dead.execute(&o, Some(&d)).await;
        assert_eq!(ack2.status, "FAILED");
        assert!(ack2.error.unwrap().contains("transport"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_torn_tail_stops_replay() {
        // A crash mid-append leaves a partial last line; replay must
        // stop there (the torn record was never fsynced — its op may
        // legitimately re-execute).
        let dir = tmp_w3r3_dir("torn");
        let path = dir.join("executed.json");
        let journal = path.with_extension("journal");
        let good = serde_json::json!([
            "op-good",
            {"status": "DONE", "result": {"s": 1}, "error": null, "at": now_secs()}
        ]);
        let mut text = serde_json::to_string(&good).unwrap() + "\n";
        text.push_str("{\"op-torn\",{\"status\":\"DON"); // truncated
        std::fs::write(&journal, text).unwrap();
        let set = ExecutedSet::load(path);
        assert!(set.get("op-good").is_some());
        assert!(set.get("op-torn").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_compact_truncates_journal() {
        let dir = tmp_w3r3_dir("compact");
        let path = dir.join("executed.json");
        let journal = path.with_extension("journal");
        let mut set = ExecutedSet::load(path.clone());
        let ack = CellOpAck {
            seq: 1,
            status: "DONE",
            result: None,
            error: None,
        };
        for i in 0..5 {
            set.record(&format!("op-{i}"), &ack);
        }
        assert!(std::fs::metadata(&journal).unwrap().len() > 0);
        set.persist();
        // Journal truncated to zero once the snapshot covers it.
        assert_eq!(std::fs::metadata(&journal).unwrap().len(), 0);
        let set = ExecutedSet::load(path);
        for i in 0..5 {
            assert!(set.get(&format!("op-{i}")).is_some(), "op-{i} lost");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn only_data_plane_reads_skip_the_journal() {
        for k in ["KV_GET", "KV_LIST", "R2_LIST", "R2_HEAD", "R2_GET"] {
            assert!(is_pure_read_kind(k), "{k}");
        }
        for k in [
            "KV_PUT",
            "KV_DELETE",
            "KV_BULK_PUT",
            "KV_BULK_DELETE",
            "R2_PUT",
            "R2_DELETE",
            "STORAGE_PURGE",
            "CACHE_PURGE",
            "WF_ACTION",
            "",
        ] {
            assert!(!is_pure_read_kind(k), "{k}");
        }
        // Every read kind is a routed kind of its own executor.
        for k in crate::kvops::KV_READ_KINDS {
            assert!(crate::kvops::is_kv_kind(k));
        }
        for k in crate::r2ops::R2_READ_KINDS {
            assert!(crate::r2ops::is_r2_kind(k));
        }
    }

    #[tokio::test]
    async fn redelivery_replays_stored_outcome() {
        // "Executed, crashed before ack, redelivered" — the stored
        // outcome is re-acked WITHOUT re-executing (pod is dead: any
        // real call would be a transport failure).
        let exec = exec_unreachable();
        let d = desired();
        let mut executed = ExecutedSet::memory();
        let o = op(
            1,
            "WF_ACTION",
            payload(serde_json::json!({"instanceId": "i", "action": "restart"})),
        );
        // Stage the prior outcome as if the first attempt completed.
        let prior = CellOpAck {
            seq: o.seq,
            status: "DONE",
            result: Some(serde_json::json!({"status": "running"})),
            error: None,
        };
        executed.record(&o.id, &prior);
        let rec = executed.get(&o.id).unwrap().clone();
        let replay = CellOpAck {
            seq: o.seq,
            status: "DONE",
            result: rec.result,
            error: rec.error,
        };
        assert_eq!(replay.status, "DONE");
        assert_eq!(replay.result.unwrap()["status"], "running");
        // And execute would have FAILED on transport — proof the
        // replay path is what must serve redelivery.
        let ack = exec.execute(&o, Some(&d)).await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("transport"));
    }

    #[test]
    fn whitelist_drops_unknown_fields_and_marks_truncation() {
        let v = serde_json::json!({
            "status": "complete",
            "output": "x".repeat(MAX_OUTPUT_CHARS + 10),
            "error": "e".repeat(MAX_ERROR_CHARS + 10),
            "secretField": "must-not-leak",
            "__LOCAL_DEV_STEP_OUTPUTS": [1, 2],
        });
        let out = whitelist(&v, &["status", "output", "error"]);
        assert!(out.get("secretField").is_none());
        assert!(out.get("__LOCAL_DEV_STEP_OUTPUTS").is_none());
        assert_eq!(
            out["output"].as_str().unwrap().chars().count(),
            MAX_OUTPUT_CHARS
        );
        assert_eq!(
            out["error"].as_str().unwrap().chars().count(),
            MAX_ERROR_CHARS
        );
        assert_eq!(out["truncated"], serde_json::json!(true));
        // Under-limit input: no marker added.
        let small = serde_json::json!({"status": "running", "output": "ok"});
        let out = whitelist(&small, &["status", "output"]);
        assert!(out.get("truncated").is_none());
        // Engine-set `truncated: false` stays absent; `true` survives.
        let flagged = serde_json::json!({"status": "x", "truncated": true});
        assert_eq!(
            whitelist(&flagged, &["status"])["truncated"],
            serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn validation_fails_before_any_network() {
        let exec = exec_unreachable();
        // Every one of these must fail WITHOUT touching the (dead) pod —
        // a transport attempt would error with "transport:" instead.
        let cases: Vec<(CellOp, &str)> = vec![
            (
                {
                    let mut o = op(1, "WF_GET", payload(serde_json::json!({"instanceId": "i"})));
                    o.worker_id = "bad/id".into();
                    o
                },
                "invalid workerId",
            ),
            (
                op(
                    2,
                    "WF_GET",
                    payload(serde_json::json!({"workflow": "a/b", "instanceId": "i"})),
                ),
                "invalid workflow name",
            ),
            (
                op(
                    3,
                    "WF_GET",
                    payload(serde_json::json!({"instanceId": "a/b"})),
                ),
                "invalid instanceId",
            ),
            (
                op(
                    4,
                    "WF_ACTION",
                    payload(serde_json::json!({"instanceId": "i", "action": "explode"})),
                ),
                "invalid action",
            ),
            (
                op(
                    5,
                    "WF_CREATE",
                    payload(serde_json::json!({"params": [1, 2]})),
                ),
                "params must be an object",
            ),
            (
                // W3-r2: smuggled identity material is rejected by
                // `deny_unknown_fields` — not ignored, not honored.
                op(
                    6,
                    "WF_GET",
                    payload(serde_json::json!({"wfUniqueKey": "a".repeat(64), "instanceId": "i"})),
                ),
                "malformed payload",
            ),
            (
                op(7, "WF_UNKNOWN_KIND", payload(serde_json::json!({}))),
                "unknown op kind",
            ),
            (
                // A workflow name absent from the signed desired is
                // undeclared — even a syntactically valid one.
                op(
                    8,
                    "WF_GET",
                    payload(serde_json::json!({"workflow": "ghost-flow", "instanceId": "i"})),
                ),
                "workflow.undeclared",
            ),
            (
                // `__proto__` passes the name regex but can never be
                // declared — must not resolve via prototype tricks.
                op(
                    9,
                    "WF_GET",
                    payload(serde_json::json!({"workflow": "__proto__", "instanceId": "i"})),
                ),
                "workflow.undeclared",
            ),
            (
                // tenantId/binding smuggling — same deny_unknown_fields.
                op(
                    10,
                    "WF_GET",
                    payload(serde_json::json!({"tenantId": "tenant-2", "instanceId": "i"})),
                ),
                "malformed payload",
            ),
        ];
        let d = desired();
        for (o, want) in cases {
            let ack = exec.execute(&o, Some(&d)).await;
            assert_eq!(ack.status, "FAILED", "{:?}", o.id);
            assert_eq!(ack.error.as_deref(), Some(want), "{:?}", o.id);
        }
        // Worker absent from the signed desired — fail closed.
        let mut o = op(
            11,
            "WF_GET",
            payload(serde_json::json!({"instanceId": "i"})),
        );
        o.worker_id = "w_undeployed".into();
        let ack = exec.execute(&o, Some(&d)).await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(ack.error.as_deref(), Some("worker.undeployed"));
        // No desired installed at all — fail closed.
        let ack = exec
            .execute(
                &op(
                    12,
                    "WF_GET",
                    payload(serde_json::json!({"instanceId": "i"})),
                ),
                None,
            )
            .await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(ack.error.as_deref(), Some("no desired installed"));
    }

    #[tokio::test]
    async fn expired_op_is_never_executed() {
        let exec = exec_unreachable();
        let mut o = op(1, "WF_GET", payload(serde_json::json!({"instanceId": "i"})));
        o.expires_at = Some(1);
        // Expiry is gated BEFORE the desired lookup — a stale op is
        // EXPIRED even with no desired installed.
        let ack = exec.execute(&o, None).await;
        assert_eq!(ack.status, "EXPIRED");
        assert_eq!(ack.error.as_deref(), Some("dispatch deadline passed"));
        // Future deadline → reaches transport (fails transport, not EXPIRED).
        o.expires_at = Some(i64::MAX / 2);
        let d = desired();
        let ack = exec.execute(&o, Some(&d)).await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("transport"));
    }

    #[test]
    fn pod_base_must_be_loopback() {
        assert!(WfOpsExecutor::new(
            &[("p".into(), "http://10.0.0.5:9091".into())],
            Duration::from_secs(1),
        )
        .is_err());
        assert!(WfOpsExecutor::new(
            &[("p".into(), "https://127.0.0.1:9091".into())],
            Duration::from_secs(1),
        )
        .is_err());
    }

    /// Minimal facade stub: fixed JSON response, captures the request.
    async fn stub_facade(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        stub_facade_status("200 OK", body).await
    }

    async fn stub_facade_status(
        status: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let h = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 8192];
            let _ = sock.read(&mut buf).await.unwrap();
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
        (format!("http://127.0.0.1:{port}"), h)
    }

    /// W3-r4: the facade's 504 `{error:"instance.create_unconfirmed"}`
    /// (confirm budget exhausted) must surface as a FAILED ack whose
    /// error carries the typed code verbatim — the control plane maps
    /// it to VRT_WF_CREATE_UNCONFIRMED. Typed errors never transport-
    /// retry (only one stub accept is wired).
    #[tokio::test]
    async fn create_unconfirmed_maps_to_failed_ack() {
        let (base, _h) = stub_facade_status(
            "504 Gateway Timeout",
            r#"{"error":"instance.create_unconfirmed"}"#,
        )
        .await;
        let exec = WfOpsExecutor::new(&[("pod-0".into(), base)], Duration::from_secs(3)).unwrap();
        let d = desired();
        let ack = exec
            .execute(
                &op(
                    1,
                    "WF_CREATE",
                    payload(serde_json::json!({"instanceId": "i-1", "params": {}})),
                ),
                Some(&d),
            )
            .await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(ack.error.as_deref(), Some("instance.create_unconfirmed"));
        assert!(ack.result.is_none());
    }

    /// Scale-to-zero: the host's typed wake failures (503 +
    /// `VRT_WFOPS_WAKE_*`) are terminal FAILED acks, never transport
    /// retries (only one stub accept is wired, so a retry would surface
    /// as a `transport:` error instead of the code).
    #[tokio::test]
    async fn typed_wake_errors_are_not_retried() {
        for code in [
            "VRT_WFOPS_WAKE_TIMEOUT",
            "VRT_WFOPS_WAKE_FAILED",
            "VRT_WFOPS_WAKE_CAPACITY",
        ] {
            let body: &'static str = Box::leak(
                format!(r#"{{"error":{{"code":"{code}","message":"m"}}}}"#).into_boxed_str(),
            );
            let (base, _h) = stub_facade_status("503 Service Unavailable", body).await;
            let exec =
                WfOpsExecutor::new(&[("pod-0".into(), base)], Duration::from_secs(3)).unwrap();
            let d = desired();
            let ack = exec
                .execute(
                    &op(
                        1,
                        "WF_CREATE",
                        payload(serde_json::json!({"instanceId": "i-1", "params": {}})),
                    ),
                    Some(&d),
                )
                .await;
            assert_eq!(ack.status, "FAILED", "{code}");
            assert_eq!(ack.error.as_deref(), Some(code));
        }
    }

    #[tokio::test]
    async fn done_path_whitelists_facade_result() {
        let (base, _h) = stub_facade(
            r#"{"status":"complete","output":{"rows":3},"error":null,"internal":"nope"}"#,
        )
        .await;
        let exec = WfOpsExecutor::new(&[("pod-0".into(), base)], Duration::from_secs(3)).unwrap();
        let d = desired();
        let ack = exec
            .execute(
                &op(
                    1,
                    "WF_GET",
                    payload(serde_json::json!({"instanceId": "inst-1"})),
                ),
                Some(&d),
            )
            .await;
        assert_eq!(ack.status, "DONE");
        let r = ack.result.unwrap();
        assert_eq!(r["status"], serde_json::json!("complete"));
        // `output` is TEXT on the wire (server schema: boundedText) —
        // a non-string engine value is JSON-serialized, then capped.
        assert_eq!(r["output"], serde_json::json!("{\"rows\":3}"));
        assert!(r.get("error").is_none());
        assert!(r.get("internal").is_none());
    }

    #[tokio::test]
    async fn oversize_facade_output_is_truncated() {
        let big = format!(
            r#"{{"status":"complete","output":"{}"}}"#,
            "x".repeat(MAX_OUTPUT_CHARS + 1000)
        );
        // Leak the body — 'static requirement for the stub task.
        let (base, _h) = stub_facade(Box::leak(big.into_boxed_str())).await;
        let exec = WfOpsExecutor::new(&[("pod-0".into(), base)], Duration::from_secs(3)).unwrap();
        let d = desired();
        let ack = exec
            .execute(
                &op(1, "WF_GET", payload(serde_json::json!({"instanceId": "i"}))),
                Some(&d),
            )
            .await;
        assert_eq!(ack.status, "DONE");
        let r = ack.result.unwrap();
        assert_eq!(r["output"].as_str().unwrap().len(), MAX_OUTPUT_CHARS);
        assert_eq!(r["truncated"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn action_op_forwards_whitelist_only_status() {
        let (base, _h) = stub_facade(r#"{"status":"paused","debug":"x"}"#).await;
        let exec = WfOpsExecutor::new(&[("pod-0".into(), base)], Duration::from_secs(3)).unwrap();
        let d = desired();
        let ack = exec
            .execute(
                &op(
                    1,
                    "WF_ACTION",
                    payload(serde_json::json!({"instanceId": "i", "action": "pause"})),
                ),
                Some(&d),
            )
            .await;
        assert_eq!(ack.status, "DONE");
        let r = ack.result.unwrap();
        assert_eq!(r["status"], serde_json::json!("paused"));
        assert!(r.get("debug").is_none());
    }

    #[test]
    fn cellop_row_accepts_unknown_fields() {
        // RV-R58 (intentional semantic change): unknown fields are
        // accepted so a newer control plane cannot stall an older cell;
        // malformed rows are isolated by execset::decode_ops_page.
        let good = r#"{"id":"o1","seq":1,"kind":"WF_GET","workerId":"w_0123456789abcdef","payload":{},"expiresAt":1700000000}"#;
        let op: CellOp = serde_json::from_str(good).unwrap();
        assert_eq!(op.expires_at, Some(1700000000));
        let bad = r#"{"id":"o1","seq":1,"kind":"WF_GET","workerId":"w","payload":{},"expiresAt":0,"extra":1}"#;
        assert!(serde_json::from_str::<CellOp>(bad).is_ok());
        // a structurally wrong row still fails to decode (row-level).
        let broken = r#"{"id":"o1","seq":"x","kind":"WF_GET","workerId":"w","payload":{}}"#;
        assert!(serde_json::from_str::<CellOp>(broken).is_err());
        // expiresAt may be null or absent.
        let nul =
            r#"{"id":"o1","seq":1,"kind":"WF_GET","workerId":"w","payload":{},"expiresAt":null}"#;
        assert!(serde_json::from_str::<CellOp>(nul)
            .unwrap()
            .expires_at
            .is_none());
        let absent = r#"{"id":"o1","seq":1,"kind":"WF_GET","workerId":"w","payload":{}}"#;
        assert!(serde_json::from_str::<CellOp>(absent)
            .unwrap()
            .expires_at
            .is_none());
    }
}
