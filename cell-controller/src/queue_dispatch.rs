//! Queue consumer dispatcher (T37b, `doc/platform/workers-runtime-sql-storage.md` §8).
//!
//! The cell-controller is the dispatcher: it alone holds the vrt-sql
//! ServiceAuth secret, the pod assignment table and the pod bases —
//! a per-pod dispatcher could not route a queue to the pod that runs
//! its consumer worker, and vrt-provider holds none of the lease/
//! assignment machinery.
//!
//! Pipeline per queue (`queueConsumers` in `compute.desired`):
//!   1. `claim` — the dispatch lease is CAS'd inside the SAME
//!      transaction as the message claim (vrt-sql
//!      `_vrt_q_dispatch`): at most one dispatcher per queue per
//!      cell; a contender gets `VRT_QUEUE_DISPATCH_LEASE` + the
//!      holder's expiry and backs off until then.
//!   2. Batch assembly — messages accumulate until `maxBatchSize`
//!      or `maxBatchTimeout` since the first pending message.
//!   3. Delivery — `POST {pod}/v1/queue/dispatch` → vrt-host →
//!      `sock-qconsumer-<workerId>` → `facade-qconsumer` →
//!      `env.CONSUMER.queue()` (workerd `Fetcher.queue` RPC — the
//!      only internal delivery channel; capnp :406 `queue @15` is a
//!      producer binding only).
//!   4. Settlement — miniflare `processResponse` parity
//!      (broker.worker.js:220-235,354-374): `outcome!="ok"` ∨
//!      `retryBatch.retry` ⇒ ALL retry; else `explicitRetries` ∪
//!      `retryMessages` retry (message delay > batch delay >
//!      `retryDelay`); the rest ack. Messages failing their
//!      `maxRetries+1`-th delivery go to the DLQ (send with the
//!      ORIGINAL id — dedupe makes replays idempotent — then
//!      `dead-letter` the source row) or drop+audit when no DLQ.
//!
//! Layered leases: taking over the dispatch lease does NOT touch
//! claimed messages — a superseded dispatcher's in-flight batch still
//! settles correctly (its message leases are valid), and the
//! successor simply cannot re-claim those ids until expiry. Delivery
//! is at-least-once by construction: a crash between delivery and
//! settlement re-delivers after `msg_lease_ms` (section 8 of the doc).
//!
//! Bounds (startup-validated, fail closed): `msg_lease_ms` must
//! exceed `delivery_timeout + SETTLE_MARGIN` — a message lease that
//! expires mid-delivery would let the successor double-deliver
//! without the attempt budget the lease was meant to buy.

use crate::assign::PodAssignment;
use serde::Deserialize;
use snapshot_protocol::compute_desired::{ComputeDesired, QueueConsumerDesired};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use vrt_provider::d1::{SqlAuthSigner, SQL_AUTH_HEADER, SQL_EPOCH_HEADER, SQL_SEQ_HEADER};

// The vrt-sql epoch/seq header names reuse the vrt_provider::d1 constants (`x-vrt-epoch`/
// `x-vrt-seq`) -- the signature covers the header values, so a wrong name fails verification with a 401 (T37b self-review
// fix: an earlier version wrongly used an x-vrt-sql-* prefix). Queue ops have no per-db epoch,
// so it is always 1; seq must still be monotonic.

/// Default global cap on in-flight batches -- a slow consumer must not starve the whole cell.
pub const GLOBAL_INFLIGHT_MAX: usize = 32;
/// Margin between delivery and settlement (msg lease must be > delivery timeout + this margin).
const SETTLE_MARGIN: Duration = Duration::from_secs(15);
/// Default dispatch holder TTL is 15s (claim renews it every tick; takeover
/// delay after a crash is <= TTL).
pub const DISPATCH_TTL_DEFAULT_MS: i64 = 15_000;
/// Default message lease is 90s (> delivery timeout 60s + margin 15s).
pub const MSG_LEASE_DEFAULT_MS: i64 = 90_000;
/// Default batch delivery timeout -- below the upstream 70s cap that sits above the facade's internal 55s,
/// and must be < msg_lease (enforced by startup validation).
const DELIVERY_TIMEOUT_DEFAULT: Duration = Duration::from_secs(60);
/// T37b-r: default claim raw body byte budget is 4MB -- with 32 global concurrency the per-hop
/// worst-case buffering is ~4.1MB x2 (hex) + envelope ~= 8.5MB, replacing the old
/// 100x128KB ~= 25.7MB sizing.
pub const CLAIM_BUDGET_DEFAULT_BYTES: u64 = 4 * 1024 * 1024;
/// Upper bound of the budget -- aligned with vrt-sql `CLAIM_BYTES_MAX`; a larger budget has no
/// point (producer batch is 288KB, claim cap is 100x128KB=12.8MB).
pub const CLAIM_BUDGET_MAX_BYTES: u64 = 32 * 1024 * 1024;
fn unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn unix_ms_now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Dispatcher startup config -- built from `[queue_dispatch]` (platform) or
/// `vrt serve` arguments (standalone); `validate` fails closed.
#[derive(Clone, Debug)]
pub struct DispatchConfig {
    /// `_vrt_q_dispatch.holder` identity (`[a-zA-Z0-9_.:-]{1,64}` --
    /// strictly validated on the vrt-sql side).
    pub holder_id: String,
    /// Dispatch lease TTL (written via CAS embedded in claim).
    pub dispatch_ttl_ms: i64,
    /// Message lease TTL -- must be > delivery_timeout + SETTLE_MARGIN.
    pub msg_lease_ms: i64,
    /// Batch delivery timeout (the full HTTP -> facade -> queue() chain).
    pub delivery_timeout: Duration,
    /// Global cap on in-flight batches.
    pub global_inflight_max: usize,
    /// T37b-r: per-lane raw body byte budget for pending messages (the claim request's
    /// `maxBytes`). Once pending reaches the budget, stop claiming and form a batch immediately.
    pub claim_max_bytes: u64,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            holder_id: "cell-controller-0".into(),
            dispatch_ttl_ms: DISPATCH_TTL_DEFAULT_MS,
            msg_lease_ms: MSG_LEASE_DEFAULT_MS,
            delivery_timeout: DELIVERY_TIMEOUT_DEFAULT,
            global_inflight_max: GLOBAL_INFLIGHT_MAX,
            claim_max_bytes: CLAIM_BUDGET_DEFAULT_BYTES,
        }
    }
}

impl DispatchConfig {
    /// Section 8 hard invariant: `msg_lease > delivery_timeout + settle_margin`
    /// -- if the lease expires before settlement, a successor re-delivers while the original delivery is still in flight,
    /// and the double delivery exceeds the design margin of at-least-once semantics.
    pub fn validate(&self) -> Result<(), String> {
        if self.holder_id.is_empty()
            || self.holder_id.len() > 64
            || !self
                .holder_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
        {
            return Err("queue_dispatch.holder_id charset/len".into());
        }
        if self.dispatch_ttl_ms < 5_000 || self.dispatch_ttl_ms > 300_000 {
            return Err("queue_dispatch.dispatch_ttl_ms out of 5s..300s".into());
        }
        let min_lease = (self.delivery_timeout + SETTLE_MARGIN).as_millis() as i64;
        if self.msg_lease_ms <= min_lease {
            return Err(format!(
                "queue_dispatch.msg_lease_ms must exceed delivery_timeout+{SETTLE_MARGIN:?} ({min_lease}ms)"
            ));
        }
        if self.msg_lease_ms > 300_000 {
            return Err("queue_dispatch.msg_lease_ms out of bounds (<=300s)".into());
        }
        if self.claim_max_bytes == 0 || self.claim_max_bytes > CLAIM_BUDGET_MAX_BYTES {
            return Err(format!(
                "queue_dispatch.claim_max_bytes out of bounds (1..={CLAIM_BUDGET_MAX_BYTES})"
            ));
        }
        if self.global_inflight_max == 0 || self.global_inflight_max > 1024 {
            return Err("queue_dispatch.global_inflight_max out of bounds".into());
        }
        Ok(())
    }
}

/// vrt-sql call failure -- the classification decides dispatcher behavior (back off vs abandon the batch).
#[derive(Debug)]
pub enum SqlCallError {
    /// `VRT_QUEUE_DISPATCH_LEASE` -- held by someone else; payload = lease expiry in ms.
    DispatchHeld { expires_at_ms: i64 },
    /// `VRT_QUEUE_LEASE_LOST` -- this batch's lease_id is invalid (the lease expired or, after
    /// takeover, the messages were re-claimed); abandon settlement of this batch.
    LeaseLost,
    /// Other wire codes such as `VRT_FENCED`/`VRT_QUEUE_DROPPED`/`VRT_UNAVAILABLE`
    /// (`retryable` passed through).
    Backend {
        code: String,
        status: u16,
        retryable: bool,
    },
    /// Connection / timeout / JSON parse failure.
    Transport,
}

/// Signed client for `/queues/{id}/{op}` -- same wire as `vrt_provider::d1::D1Client`
/// (`x-vrt-sql-auth` v1 signature + epoch + seq); queue ops
/// have no database epoch, so it is always 1.
pub struct QueueSqlClient {
    http: reqwest::Client,
    addr: String,
    signer: SqlAuthSigner,
    timeout: Duration,
    seq: AtomicU64,
}

impl QueueSqlClient {
    /// `addr` must pass `d1ops::validate_data_plane_endpoint` (loopback
    /// or an in-cluster Service DNS name, http + explicit port; authentication relies on the HMAC signature,
    /// same constraint as D1OpsExecutor).
    pub fn new(addr: &str, secret: Vec<u8>, timeout: Duration) -> Result<Self, String> {
        Self::with_policy(addr, secret, timeout, true)
    }

    /// `require_loopback=false` is for standalone only: the compose internal
    /// network hostname (e.g. `http://vrt-sql:8090`) is a trust boundary configured by the
    /// operator, same channel / same signing model as provider->vrt-sql.
    pub fn with_policy(
        addr: &str,
        secret: Vec<u8>,
        timeout: Duration,
        require_loopback: bool,
    ) -> Result<Self, String> {
        if require_loopback {
            // Shared shape check (loopback OR in-cluster Service name,
            // http only, explicit port) — same rule as D1OpsExecutor.
            crate::d1ops::validate_data_plane_endpoint(addr)
                .map_err(|e| format!("vrt-sql addr {e}"))?;
        } else {
            let url = reqwest::Url::parse(addr).map_err(|e| format!("vrt-sql addr: {e}"))?;
            url.host_str().ok_or("vrt-sql addr: no host")?;
            if url.scheme() != "http" && url.scheme() != "https" {
                return Err("vrt-sql addr scheme must be http(s)".into());
            }
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("vrt-sql client: {e}"))?;
        Ok(Self {
            http,
            addr: addr.trim_end_matches('/').to_string(),
            signer: SqlAuthSigner::new(secret),
            timeout,
            seq: AtomicU64::new(unix_nanos()),
        })
    }

    fn next_seq(&self) -> u64 {
        let now = unix_nanos();
        let mut cur = self.seq.load(Ordering::Relaxed);
        loop {
            let next = cur.max(now) + 1;
            match self
                .seq
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return next,
                Err(c) => cur = c,
            }
        }
    }

    /// `POST /queues/{id}/{op}` -- a single attempt, never retried (write-path idempotency
    /// is guaranteed by the caller's settlement semantics).
    async fn call(
        &self,
        queue_id: &str,
        op: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, SqlCallError> {
        let body_bytes = serde_json::to_vec(body).map_err(|_| SqlCallError::Transport)?;
        let seq = self.next_seq();
        let path_query = format!("/queues/{queue_id}/{op}");
        let auth = self.signer.sign("POST", &path_query, &body_bytes, 1, seq);
        let resp = self
            .http
            .post(format!("{}{}", self.addr, path_query))
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(SQL_EPOCH_HEADER, "1")
            .header(SQL_SEQ_HEADER, seq.to_string())
            .header(SQL_AUTH_HEADER, auth)
            .body(body_bytes)
            .send()
            .await
            .map_err(|_| SqlCallError::Transport)?;
        let status = resp.status().as_u16();
        let text = resp.bytes().await.map_err(|_| SqlCallError::Transport)?;
        let doc: serde_json::Value =
            serde_json::from_slice(&text).map_err(|_| SqlCallError::Transport)?;
        if doc.get("success").and_then(|v| v.as_bool()) == Some(true) {
            return Ok(doc);
        }
        let code = doc
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let retryable = doc
            .get("retryable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        match code.as_str() {
            "VRT_QUEUE_DISPATCH_LEASE" => {
                // "queue dispatch lease held by <h> until <ms>"
                let msg = doc.get("error").and_then(|v| v.as_str()).unwrap_or("");
                let expires = msg
                    .rsplit(" until ")
                    .next()
                    .and_then(|t| t.trim().parse::<i64>().ok())
                    .unwrap_or(0);
                Err(SqlCallError::DispatchHeld {
                    expires_at_ms: expires,
                })
            }
            "VRT_QUEUE_LEASE_LOST" => Err(SqlCallError::LeaseLost),
            _ => Err(SqlCallError::Backend {
                code,
                status,
                retryable,
            }),
        }
    }

    async fn claim(
        &self,
        queue_id: &str,
        holder: &str,
        holder_ttl_ms: i64,
        max: u32,
        max_bytes: Option<u64>,
        lease_ttl_ms: i64,
    ) -> Result<serde_json::Value, SqlCallError> {
        let mut req = serde_json::json!({
            "holderId": holder,
            "holderTtlMs": holder_ttl_ms,
            "max": max,
            "leaseTtlMs": lease_ttl_ms,
        });
        if let Some(b) = max_bytes {
            req["maxBytes"] = b.into();
        }
        self.call(queue_id, "claim", &req).await
    }

    async fn ack(
        &self,
        queue_id: &str,
        lease_id: &str,
        ids: &[String],
    ) -> Result<(), SqlCallError> {
        self.call(
            queue_id,
            "ack",
            &serde_json::json!({"leaseId": lease_id, "ids": ids}),
        )
        .await
        .map(|_| ())
    }

    async fn retry_msgs(
        &self,
        queue_id: &str,
        lease_id: &str,
        items: &[(String, u32)],
    ) -> Result<(), SqlCallError> {
        let items: Vec<serde_json::Value> = items
            .iter()
            .map(|(id, d)| serde_json::json!({"id": id, "delaySecs": d}))
            .collect();
        self.call(
            queue_id,
            "retry",
            &serde_json::json!({"leaseId": lease_id, "items": items}),
        )
        .await
        .map(|_| ())
    }

    /// T37b-r5: platform-side failure give-back (consumer did not run) -- `visible_at=
    /// now+delay`, clear the lease, `attempts-1` compensates the +1 from claim; does not consume
    /// the retry budget, so messages stay in the queue and are not lost during a platform outage.
    async fn release(
        &self,
        queue_id: &str,
        lease_id: &str,
        items: &[(String, u32)],
    ) -> Result<(), SqlCallError> {
        let items: Vec<serde_json::Value> = items
            .iter()
            .map(|(id, d)| serde_json::json!({"id": id, "delaySecs": d}))
            .collect();
        self.call(
            queue_id,
            "release",
            &serde_json::json!({"leaseId": lease_id, "items": items}),
        )
        .await
        .map(|_| ())
    }

    async fn dead_letter(
        &self,
        queue_id: &str,
        lease_id: &str,
        ids: &[String],
        reason: &str,
    ) -> Result<(), SqlCallError> {
        self.call(
            queue_id,
            "dead-letter",
            &serde_json::json!({"leaseId": lease_id, "ids": ids, "reason": reason}),
        )
        .await
        .map(|_| ())
    }

    /// DLQ forwarding enqueue (same original msg id => idempotent).
    async fn dlq_send(&self, dlq_queue_id: &str, msgs: &[&PendingMsg]) -> Result<(), SqlCallError> {
        let body = serde_json::json!({
            "messages": msgs.iter().map(|m| serde_json::json!({
                "id": m.id,
                "body": m.body_hex,
                "contentType": m.content_type,
                "delaySecs": 0,
                "timestampMs": m.timestamp_ms,
            })).collect::<Vec<_>>()
        });
        self.call(dlq_queue_id, "send", &body).await.map(|_| ())
    }
}

/// QueueResult returned by the facade (the shape of the workerFetch queue() RPC result after
/// normalization by facade-qconsumer).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct QueueOutcome {
    pub outcome: String,
    pub ack_all: bool,
    pub retry_batch: RetryBatch,
    pub explicit_acks: Vec<String>,
    pub explicit_retries: Vec<String>,
    pub retry_messages: Vec<RetryMessage>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RetryBatch {
    pub retry: bool,
    pub delay_seconds: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RetryMessage {
    pub msg_id: String,
    pub delay_seconds: Option<u32>,
}

/// Delivery failure classification. `reason` only goes into metrics and logs (bounded 'static label);
/// `detail` is a bounded diagnostic (HTTP status + <=256B sanitized snippet -- vrt-host's
/// typed error body only contains code/message and never echoes the message body).
/// Every DeliveryError is a platform-side failure: the consumer code did not run.
#[derive(Debug)]
pub struct DeliveryError {
    pub reason: &'static str,
    pub detail: Option<String>,
}

impl DeliveryError {
    fn of(reason: &'static str) -> Self {
        Self {
            reason,
            detail: None,
        }
    }
}

/// A single claimed message (the dispatcher's internal representation).
#[derive(Debug, Clone)]
pub struct PendingMsg {
    pub id: String,
    pub lease_id: String,
    pub attempts: u32,
    pub body_hex: String,
    pub content_type: String,
    pub timestamp_ms: i64,
}

/// Delivery request -- the vrt-host `/v1/queue/dispatch` body.
#[derive(Debug)]
pub struct DispatchRequest {
    pub tenant_id: String,
    pub worker_id: String,
    /// facade payload `{queue, messages[], metadata}`.
    pub dispatch: serde_json::Value,
}

/// Delivery transport boundary (production: HTTP -> pod control; tests: memory).
#[async_trait::async_trait]
pub trait QueueInvoker: Send + Sync {
    async fn dispatch(
        &self,
        pod_id: &str,
        req: DispatchRequest,
    ) -> Result<QueueOutcome, DeliveryError>;
}

/// `POST {pod}/v1/queue/dispatch` -- the pod base reuses the `[[pods]]`
/// loopback readyz endpoint (same validation as HttpCronInvoker).
pub struct HttpQueueInvoker {
    pods: BTreeMap<String, String>,
    client: reqwest::Client,
}

impl HttpQueueInvoker {
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
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("queue invoker client: {e}"))?;
        Ok(Self { pods: map, client })
    }
}

#[async_trait::async_trait]
impl QueueInvoker for HttpQueueInvoker {
    async fn dispatch(
        &self,
        pod_id: &str,
        req: DispatchRequest,
    ) -> Result<QueueOutcome, DeliveryError> {
        let Some(base) = self.pods.get(pod_id) else {
            return Err(DeliveryError::of("unassigned"));
        };
        let resp = self
            .client
            .post(format!("{base}/v1/queue/dispatch"))
            .json(&serde_json::json!({
                "tenantId": req.tenant_id,
                "workerId": req.worker_id,
                "dispatch": req.dispatch,
            }))
            .send()
            .await
            .map_err(|e| DeliveryError {
                reason: if e.is_timeout() {
                    "timeout"
                } else {
                    "transport"
                },
                detail: Some(e.to_string()),
            })?;
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.map_err(|e| DeliveryError {
            reason: "transport",
            detail: Some(format!("status={status} body-read: {e}")),
        })?;
        // Judge the vrt-host typed error first (the `{delivered:false,error:{code}}`
        // shape would also be swallowed by QueueOutcome's all-default parse -> it must be checked first).
        if let Some(code) = platform_error_code(&bytes) {
            return Err(DeliveryError {
                reason: platform_reason(&code),
                detail: Some(format!("status={status} body={}", snippet(&bytes))),
            });
        }
        serde_json::from_slice::<QueueOutcome>(&bytes).map_err(|e| DeliveryError {
            reason: "malformed",
            detail: Some(format!("status={status} body={} ({e})", snippet(&bytes))),
        })
    }
}

/// vrt-host `/v1/queue/dispatch` typed error body (queue_dispatch.rs::
/// `err()`) -- `{delivered:false, error:{code}}` -> bounded code.
fn platform_error_code(bytes: &[u8]) -> Option<String> {
    let doc: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    if doc.get("delivered").and_then(|v| v.as_bool()) != Some(false) {
        return None;
    }
    let code = doc
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())?;
    (code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))
    .then(|| code.to_string())
}

/// Bounded 'static reason label (the metric label set must be closed) -- an unknown
/// code is normalized to `platform`, and the code itself goes in detail/log.
fn platform_reason(code: &str) -> &'static str {
    match code {
        "VRT_QDISP_NO_CONSUMER" => "no_consumer",
        "VRT_QDISP_UNKNOWN_WORKER" => "unknown_worker",
        "VRT_QDISP_WORKER_NOT_READY" => "not_ready",
        // MIG-11: tenant mid-serialized-switch — platform error like
        // not_ready; the message leases release, attempts unspent.
        "VRT_QDISP_SWITCHING" => "switching",
        // Scale-to-zero wake failures — all pre-delivery (consumer never
        // ran), platform errors with attempts unspent.
        "VRT_QDISP_WAKE_TIMEOUT" => "wake_timeout",
        "VRT_QDISP_WAKE_FAILED" => "wake_failed",
        "VRT_QDISP_WAKE_CAPACITY" => "wake_capacity",
        "VRT_QDISP_DELIVERY_FAILED" => "delivery_failed",
        "VRT_QDISP_TIMEOUT" => "host_timeout",
        "VRT_QDISP_OVERSIZED" => "oversized",
        "VRT_QDISP_MALFORMED" => "bad_request",
        "VRT_QDISP_INTERNAL" => "internal",
        _ => "platform",
    }
}

/// <=256B log snippet: keep printable ASCII only (control characters -> '.'). The response body is
/// generated/passed through by vrt-host and does not contain the message body; still sanitized as untrusted input.
fn snippet(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(256)
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}

/// Resolved per-lane consumer config (Option fields -> concrete values).
#[derive(Debug, Clone)]
struct ResolvedConsumer {
    queue_name: String,
    batch_size: u32,
    batch_timeout_ms: i64,
    max_retries: u32,
    retry_delay_secs: u32,
    max_concurrency: u32,
    dlq_queue_id: Option<String>,
}

impl ResolvedConsumer {
    fn of(c: &QueueConsumerDesired) -> Self {
        Self {
            queue_name: c.queue_name.clone(),
            batch_size: c.max_batch_size.unwrap_or(5),
            batch_timeout_ms: c.max_batch_timeout_secs.unwrap_or(1) as i64 * 1000,
            max_retries: c.max_retries.unwrap_or(2),
            retry_delay_secs: c.retry_delay_secs.unwrap_or(0),
            max_concurrency: c.max_concurrency.unwrap_or(10),
            dlq_queue_id: c.dead_letter_queue_id.clone(),
        }
    }
}

struct Lane {
    tenant_id: String,
    worker_id: String,
    pod_id: String,
    cfg: ResolvedConsumer,
    /// Messages claimed but not yet dispatched (lease_ids may be mixed across claims --
    /// settlement groups by lease_id).
    pending: Vec<PendingMsg>,
    /// Enqueue time of the first pending message (start of the batch timeout clock).
    pending_since_ms: Option<i64>,
    /// T37b-r: accumulated raw body bytes of pending (hex length/2) -- the deduction
    /// term of the claim budget.
    pending_bytes: u64,
    /// Number of in-flight batches (<= cfg.max_concurrency).
    inflight: usize,
    /// Dispatch lease backoff deadline (DispatchLeaseLost payload).
    not_before_ms: i64,
    /// T37b-r6 release retry queue: messages whose `release` failed on the backend are
    /// parked grouped by lease and retried on the next tick (once per group per tick); after the lease
    /// expires the backend naturally returns LeaseLost -> the group is removed (the messages were taken over by a successor).
    /// Bounded: <= inflight x batch.
    release_retry: Vec<(String, Vec<PendingMsg>)>,
    /// T37b-r5 lane-level backoff after a platform-side failure (consumer did not run): no
    /// claim before this instant (messages were released back to the queue, attempts not consumed) -- avoids a
    /// hot loop on transport/malformed cycles.
    platform_not_before_ms: i64,
    /// Consecutive platform failure count (determines backoff duration; any well-formed response resets it).
    platform_streak: u32,
    /// Dispatch epoch of the most recent claim (observability).
    dispatch_epoch: u64,
    /// Earliest retry time after the previous batch's settlement failed (settlement failure backoff).
    settle_not_before_ms: i64,
    /// Consecutive backend settlement failures; advances the bounded claim backoff.
    settle_streak: u32,
    /// Fences concurrent completions from resetting a newer settlement failure backoff.
    settle_generation: u64,
}

/// Queue dispatcher -- `tick` is driven by the controller loop about every ~1s.
pub struct QueueDispatcher {
    sql: Arc<QueueSqlClient>,
    invoker: Arc<dyn QueueInvoker>,
    cfg: DispatchConfig,
    lanes: Arc<Mutex<BTreeMap<String, Lane>>>,
    global_sem: Arc<tokio::sync::Semaphore>,
    /// T37b-r4: rate limiting of claim/deliver/settle error logs -- "at most one per key per
    /// 10s", so repeated failures do not flood the log but are never silent (in the field there was once a
    /// "lease acquired but completely silent afterwards" case, and the root cause was found only by guessing).
    log_throttle: Arc<LogThrottle>,
    /// Test hook: counts spawned delivery batches.
    #[cfg(test)]
    pub dispatched: AtomicU64,
}

/// Rate limiter -- std Mutex (synchronous fast path, no lock held across await).
struct LogThrottle(std::sync::Mutex<BTreeMap<String, std::time::Instant>>);

impl Default for LogThrottle {
    fn default() -> Self {
        Self(std::sync::Mutex::new(BTreeMap::new()))
    }
}

impl LogThrottle {
    /// True and records the time if the key is >= `iv` since its last pass (or first time).
    fn allow(&self, key: &str, iv: Duration) -> bool {
        let now = std::time::Instant::now();
        let mut m = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match m.get_mut(key) {
            Some(t) if now.duration_since(*t) < iv => false,
            Some(t) => {
                *t = now;
                true
            }
            None => {
                m.insert(key.to_string(), now);
                true
            }
        }
    }
}

/// Interval for rate limiting warn logs -- persistent failures log once per 10s; after recovery the next one is normal.
const LOG_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Observable summary of one tick (aggregated by controller/metrics).
#[derive(Debug, Default)]
pub struct DispatchTickOutcome {
    pub claimed: usize,
    pub dispatched: usize,
    /// Number of queues rejected this tick because someone else holds the lease.
    pub lease_lost: usize,
    /// Number of claims with vrt-sql/transport errors.
    pub claim_errors: usize,
    /// Number of queues where this side holds the lease (claim succeeded, including empty batches) -- basis for the T37b-r leader-election log /
    /// `vrt_queue_dispatcher_lease`.
    pub lease_held: usize,
}

impl QueueDispatcher {
    pub fn new(
        sql: Arc<QueueSqlClient>,
        invoker: Arc<dyn QueueInvoker>,
        cfg: DispatchConfig,
    ) -> Result<Self, String> {
        cfg.validate()?;
        Ok(Self {
            sql,
            invoker,
            global_sem: Arc::new(tokio::sync::Semaphore::new(cfg.global_inflight_max)),
            cfg,
            lanes: Arc::new(Mutex::new(BTreeMap::new())),
            log_throttle: Arc::new(LogThrottle::default()),
            #[cfg(test)]
            dispatched: AtomicU64::new(0),
        })
    }

    /// One scheduling tick: `desired` = the verified applied snapshot
    /// (`None`/expired = no dispatch authority -- same authority gate as cron);
    /// `assignments` = the pod table (worker -> pod).
    pub async fn tick(
        &self,
        desired: Option<&ComputeDesired>,
        desired_expires_at: Option<chrono::DateTime<chrono::Utc>>,
        assignments: &BTreeMap<String, PodAssignment>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> DispatchTickOutcome {
        let mut out = DispatchTickOutcome::default();
        let now_ms = now.timestamp_millis();
        let authorized = desired
            .zip(desired_expires_at)
            .is_some_and(|(_, exp)| exp > now);
        if !authorized {
            // No authoritative snapshot => no dispatch authority (fail closed -- an expired desired
            // must never keep consuming). The lease gauge is also emitted -- this is the
            // observable baseline for "dispatcher alive but idle" (the standalone
            // e2e diag depends on this series existing).
            crate::metrics::queue_lease(false, 0);
            self.lanes.lock().await.clear();
            return out;
        }
        let desired = desired.expect("authorized above");
        // (tenant,worker) → pod
        let mut pod_of: BTreeMap<(&str, &str), &str> = BTreeMap::new();
        for (pod, a) in assignments {
            for w in &a.workers {
                pod_of.insert((w.tenant_id.as_str(), w.worker_id.as_str()), pod.as_str());
            }
        }
        // queue_id -> lane spec (snapshot-level uniqueness is already enforced by validate).
        let mut wanted: BTreeMap<String, (String, String, String, ResolvedConsumer)> =
            BTreeMap::new();
        for tenant in &desired.tenants {
            for worker in &tenant.workers {
                for consumer in &worker.queue_consumers {
                    let Some(pod) =
                        pod_of.get(&(tenant.tenant_id.as_str(), worker.worker_id.as_str()))
                    else {
                        continue; // consumer worker not assigned -- this cell does not dispatch
                    };
                    wanted.insert(
                        consumer.queue_id.clone(),
                        (
                            tenant.tenant_id.clone(),
                            worker.worker_id.clone(),
                            (*pod).to_string(),
                            ResolvedConsumer::of(consumer),
                        ),
                    );
                }
            }
        }
        // Remove lanes that are not in desired -- their pending messages stay leased
        // (they become visible and are re-delivered after the lease expires), and a removed consumer is never dispatched to.
        {
            let mut lanes = self.lanes.lock().await;
            lanes.retain(|qid, _| wanted.contains_key(qid));
            for (qid, (tenant, worker, pod, cfg)) in wanted {
                let lane = lanes.entry(qid.clone()).or_insert_with(|| Lane {
                    tenant_id: tenant.clone(),
                    worker_id: worker.clone(),
                    pod_id: pod.clone(),
                    cfg: cfg.clone(),
                    pending: Vec::new(),
                    pending_since_ms: None,
                    pending_bytes: 0,
                    inflight: 0,
                    not_before_ms: 0,
                    platform_not_before_ms: 0,
                    platform_streak: 0,
                    release_retry: Vec::new(),
                    dispatch_epoch: 0,
                    settle_not_before_ms: 0,
                    settle_streak: 0,
                    settle_generation: 0,
                });
                // Refresh in place when desired changes (pod migration / config update) -- inflight
                // batches keep settling against the old pod (lease semantics guarantee correctness).
                lane.tenant_id = tenant;
                lane.worker_id = worker;
                lane.pod_id = pod;
                lane.cfg = cfg;
            }
            // claim/dispatch decisions are made within the same lane lock (ticks are driven serially).
            for (qid, lane) in lanes.iter_mut() {
                // T37b-r6 release retry: groups whose backend call failed on the previous tick are retried once
                // per group per tick (delay=0 -- the lane backoff has passed, make visible again immediately);
                // if the lease expired -> LeaseLost clears itself (messages were taken over by a successor).
                if !lane.release_retry.is_empty() {
                    let groups = std::mem::take(&mut lane.release_retry);
                    for (lease, msgs) in groups {
                        let items: Vec<(String, u32)> =
                            msgs.iter().map(|m| (m.id.clone(), 0)).collect();
                        let n = items.len();
                        match self.sql.release(qid, &lease, &items).await {
                            Ok(()) => {
                                crate::metrics::queue_released(n);
                                tracing::debug!(queue = %qid, n,
                                    "queue release retry succeeded");
                            }
                            Err(SqlCallError::LeaseLost) => {
                                crate::metrics::queue_settle_blocked("lease_lost", n);
                                if self
                                    .log_throttle
                                    .allow(&format!("settle:{qid}"), LOG_WARN_INTERVAL)
                                {
                                    tracing::info!(queue = %qid, op = "release_retry", n,
                                        "queue release retry dropped — lease lost");
                                }
                            }
                            Err(e) => {
                                crate::metrics::queue_settle_blocked("backend", n);
                                if self
                                    .log_throttle
                                    .allow(&format!("settle:{qid}"), LOG_WARN_INTERVAL)
                                {
                                    tracing::warn!(queue = %qid, op = "release_retry", n,
                                        error = ?e,
                                        "queue release retry failed — next tick");
                                }
                                lane.release_retry.push((lease, msgs));
                            }
                        }
                    }
                }
                if now_ms < lane.not_before_ms {
                    continue;
                }
                // T37b-r5: during platform-side backoff the whole lane sleeps -- released
                // messages become visible again after the delay, new messages wait in order; letting the dispatch lease
                // expire is harmless (the same holder takes it over via CAS on the next tick).
                if now_ms < lane.platform_not_before_ms {
                    continue;
                }
                // 1) claim to top up pending (every claim also renews the dispatch lease).
                //    T37b-r: the byte budget deducts the existing amount -- once pending has filled the budget,
                //    do not claim more (bytes>=budget in the batching condition dispatches immediately).
                let byte_room = self.cfg.claim_max_bytes.saturating_sub(lane.pending_bytes);
                if lane.pending.len() < lane.cfg.batch_size as usize
                    && lane.inflight < lane.cfg.max_concurrency as usize
                    && now_ms >= lane.settle_not_before_ms
                    && byte_room > 0
                {
                    let want = lane.cfg.batch_size - lane.pending.len() as u32;
                    let pending_before = lane.pending.len();
                    match self
                        .sql
                        .claim(
                            qid,
                            &self.cfg.holder_id,
                            self.cfg.dispatch_ttl_ms,
                            want,
                            Some(byte_room),
                            self.cfg.msg_lease_ms,
                        )
                        .await
                    {
                        Ok(doc) => {
                            out.lease_held += 1;
                            lane.dispatch_epoch = doc
                                .get("dispatchEpoch")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            if let Some(msgs) = doc.get("messages").and_then(|v| v.as_array()) {
                                for m in msgs {
                                    if let Some(pm) = parse_claimed(m) {
                                        lane.pending_bytes += pm.body_hex.len() as u64 / 2;
                                        lane.pending.push(pm);
                                    }
                                }
                            }
                            let got = lane.pending.len() - pending_before;
                            out.claimed += got;
                            // T37b-r4: every claim round must leave a trace (debug) --
                            // "lease held but completely silent" must not happen again.
                            tracing::debug!(
                                queue = %qid,
                                got,
                                pending = lane.pending.len(),
                                pending_bytes = lane.pending_bytes,
                                inflight = lane.inflight,
                                epoch = lane.dispatch_epoch,
                                "queue claim round"
                            );
                            if lane.pending_since_ms.is_none() && !lane.pending.is_empty() {
                                lane.pending_since_ms = Some(now_ms);
                            }
                        }
                        Err(SqlCallError::DispatchHeld { expires_at_ms }) => {
                            lane.not_before_ms = expires_at_ms;
                            out.lease_lost += 1;
                            crate::metrics::queue_lease(false, 0);
                            if self
                                .log_throttle
                                .allow(&format!("held:{qid}"), LOG_WARN_INTERVAL)
                            {
                                tracing::info!(
                                    queue = %qid,
                                    not_before_ms = expires_at_ms,
                                    "queue dispatch lease held by another holder"
                                );
                            }
                            continue;
                        }
                        Err(e) => {
                            out.claim_errors += 1;
                            if self
                                .log_throttle
                                .allow(&format!("claim:{qid}"), LOG_WARN_INTERVAL)
                            {
                                tracing::warn!(
                                    queue = %qid,
                                    error = ?e,
                                    "queue claim failed — bounded retry next tick"
                                );
                            }
                            continue;
                        }
                    }
                }
                // 2) batch ready: full batch, timeout (batch timeout counts from the first pending)
                //    or byte budget full (T37b-r -- a bytes batch does not idly wait for the timeout).
                let ready = !lane.pending.is_empty()
                    && lane.inflight < lane.cfg.max_concurrency as usize
                    && (lane.pending.len() >= lane.cfg.batch_size as usize
                        || lane.pending_bytes >= self.cfg.claim_max_bytes
                        || lane
                            .pending_since_ms
                            .is_some_and(|s| now_ms - s >= lane.cfg.batch_timeout_ms));
                if ready {
                    let batch = std::mem::take(&mut lane.pending);
                    lane.pending_bytes = 0;
                    lane.pending_since_ms = None;
                    lane.inflight += 1;
                    self.spawn_deliver(qid.clone(), lane, batch, now_ms);
                    out.dispatched += 1;
                }
            }
            crate::metrics::queue_lease(out.lease_held > 0, 0);
            if out.claimed > 0 || out.dispatched > 0 {
                tracing::info!(
                    claimed = out.claimed,
                    dispatched = out.dispatched,
                    lease_held = out.lease_held,
                    "queue dispatch tick"
                );
            }
        }
        out
    }

    /// Asynchronous delivery + settlement -- tick does not block; global semaphore + lane inflight
    /// double gate.
    fn spawn_deliver(&self, queue_id: String, lane: &Lane, batch: Vec<PendingMsg>, _now_ms: i64) {
        let sql = self.sql.clone();
        let invoker = self.invoker.clone();
        let sem = self.global_sem.clone();
        let lanes = self.lanes.clone();
        let pod = lane.pod_id.clone();
        let tenant = lane.tenant_id.clone();
        let worker = lane.worker_id.clone();
        let cfg = lane.cfg.clone();
        let settle_generation = lane.settle_generation;
        let delivery_timeout = self.cfg.delivery_timeout;
        let throttle = self.log_throttle.clone();
        #[cfg(test)]
        self.dispatched.fetch_add(1, Ordering::Relaxed);
        let qid2 = queue_id.clone();
        tokio::spawn(async move {
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let started = std::time::Instant::now();
            let dispatch = serde_json::json!({
                "queue": cfg.queue_name,
                "messages": batch.iter().map(|m| serde_json::json!({
                    "id": m.id,
                    "contentType": m.content_type,
                    "timestampMs": m.timestamp_ms,
                    "attempts": m.attempts,
                    "bodyHex": m.body_hex,
                })).collect::<Vec<_>>(),
            });
            let outcome = tokio::time::timeout(
                delivery_timeout,
                invoker.dispatch(
                    &pod,
                    DispatchRequest {
                        tenant_id: tenant.clone(),
                        worker_id: worker.clone(),
                        dispatch,
                    },
                ),
            )
            .await;
            let latency = started.elapsed().as_secs_f64();
            crate::metrics::queue_latency(latency);
            // T37b-r4: deliver failures used to be completely silent (transport/timeout only went into
            // metrics) -- connection refused / timeout / invalid endpoint must warn (rate limited).
            match &outcome {
                Ok(Err(e)) => {
                    if throttle.allow(&format!("deliver:{qid2}"), LOG_WARN_INTERVAL) {
                        tracing::warn!(
                            queue = %qid2,
                            pod = %pod,
                            reason = %e.reason,
                            "queue dispatch invoke failed — batch will retry"
                        );
                    }
                }
                Err(_) => {
                    if throttle.allow(&format!("deliver:{qid2}"), LOG_WARN_INTERVAL) {
                        tracing::warn!(
                            queue = %qid2,
                            pod = %pod,
                            "queue dispatch delivery timed out — batch will retry"
                        );
                    }
                }
                Ok(Ok(_)) => {}
            }
            // T37b-r5: platform-side vs user-side judgment. DeliveryError is entirely platform-
            // side (consumer did not run: transport/timeout/malformed/
            // unassigned/typed host errors) -> release, attempts not consumed;
            // QueueOutcome goes into normal settlement only when it can prove the consumer ran.
            enum Verdict {
                Consumer(QueueOutcome),
                Platform {
                    code: &'static str,
                    detail: Option<String>,
                },
            }
            let verdict = match outcome {
                Ok(Ok(r)) => {
                    if consumer_ran(&r) {
                        Verdict::Consumer(r)
                    } else {
                        // outcome="error" (facade envelope rejection, consumer
                        // did not run) or an unknown outcome -- treated as platform-side, never
                        // burn attempts on an unrecognized contract.
                        Verdict::Platform {
                            code: "consumer_rejected",
                            detail: Some(r.outcome.clone()),
                        }
                    }
                }
                Ok(Err(e)) => Verdict::Platform {
                    code: e.reason,
                    detail: e.detail,
                },
                Err(_) => Verdict::Platform {
                    code: "timeout",
                    detail: None,
                },
            };
            match verdict {
                Verdict::Platform { code, detail } => {
                    crate::metrics::queue_consumer_error(code);
                    // Give back the pending ones too -- the lease of a partial batch would expire during
                    // the backoff window, the backend would make it visible again, and a duplicate claim would burn
                    // attempts / cause double delivery; a platform failure always releases everything.
                    let (delay_secs, rest) = {
                        let mut lanes = lanes.lock().await;
                        match lanes.get_mut(&qid2) {
                            Some(l) => {
                                l.platform_streak = l.platform_streak.saturating_add(1);
                                let d = platform_backoff_secs(l.platform_streak, &qid2);
                                l.platform_not_before_ms = unix_ms_now() + i64::from(d) * 1000;
                                let rest = std::mem::take(&mut l.pending);
                                l.pending_bytes = 0;
                                l.pending_since_ms = None;
                                (d, rest)
                            }
                            None => (platform_backoff_secs(1, &qid2), Vec::new()),
                        }
                    };
                    let mut all = rest;
                    all.extend_from_slice(&batch);
                    if throttle.allow(&format!("platform:{qid2}"), LOG_WARN_INTERVAL) {
                        tracing::warn!(
                            queue = %qid2,
                            code,
                            detail = detail.as_deref().unwrap_or(""),
                            delay_secs,
                            batch = batch.len(),
                            released = all.len(),
                            "queue platform delivery failure — batch+pending released, attempts not consumed"
                        );
                    }
                    let failed = settle_release(&sql, &throttle, &queue_id, &all, delay_secs).await;
                    let mut lanes = lanes.lock().await;
                    if let Some(l) = lanes.get_mut(&qid2) {
                        l.inflight = l.inflight.saturating_sub(1);
                        if !failed.is_empty() {
                            l.release_retry.extend(failed);
                        }
                    }
                    return;
                }
                Verdict::Consumer(result) => {
                    if result.outcome != "ok"
                        && throttle.allow(&format!("outcome:{qid2}"), LOG_WARN_INTERVAL)
                    {
                        tracing::info!(
                            queue = %qid2,
                            outcome = %result.outcome,
                            batch = batch.len(),
                            "queue batch outcome not ok — settling retries"
                        );
                    }
                    // The outcome source is untrusted (facade passes through workerd fields) --
                    // Bounded label set: known outcomes pass through,
                    // rest fold to "other".
                    crate::metrics::queue_consumer_error(match result.outcome.as_str() {
                        "ok" => "ok",
                        "exception" => "exception",
                        "timeout" => "timeout",
                        "transport" => "transport",
                        "malformed" => "malformed",
                        "unassigned" => "unassigned",
                        "error" => "error",
                        _ => "other",
                    });
                    let settle_failure =
                        settle(&sql, &throttle, &queue_id, &cfg, &batch, &result).await;
                    let mut lanes = lanes.lock().await;
                    if let Some(l) = lanes.get_mut(&qid2) {
                        l.inflight = l.inflight.saturating_sub(1);
                        if settle_failure.backs_off_source_lane() {
                            l.settle_generation = l.settle_generation.wrapping_add(1);
                            l.settle_streak = l.settle_streak.saturating_add(1);
                            let delay_secs = settlement_backoff_secs(l.settle_streak, &qid2);
                            l.settle_not_before_ms = unix_ms_now() + i64::from(delay_secs) * 1000;
                        } else if l.settle_generation == settle_generation {
                            // Includes a DLQ-only failure: the source queue answered, so
                            // unrelated claims must not pause (the exhausted message's own
                            // lease bounds its forwarding retry).
                            l.settle_streak = 0;
                            l.settle_not_before_ms = 0;
                        }
                        // Well-formed response = platform reachable -- reset the backoff.
                        l.platform_streak = 0;
                        l.platform_not_before_ms = 0;
                    }
                }
            }
        });
    }
}

/// T37b-r5 user-side judgment -- only when QueueOutcome can prove the consumer code ran
/// does it go through normal settlement (attempts consumed, maxRetries, DLQ semantics). In CF/workerd
/// semantics "ok"|"exception"|"timeout" are all outcomes where the consumer ran
/// (the facade's timeout = handler exceeded 55s, which is user-side); an explicit ack/
/// retry mark also shows the consumer touched the message. Everything else (the facade envelope
/// rejection "error", unknown outcomes) is platform-side -- never
/// burn attempts on an unrecognized contract.
fn consumer_ran(r: &QueueOutcome) -> bool {
    matches!(r.outcome.as_str(), "ok" | "exception" | "timeout")
        || r.ack_all
        || r.retry_batch.retry
        || !r.explicit_acks.is_empty()
        || !r.explicit_retries.is_empty()
        || !r.retry_messages.is_empty()
}

/// Bounded exponential backoff: starts at 1s, doubles to a 60s base, then
/// adds <=base/4 jitter (seed+nanos mix). The final cap is 64s.
fn bounded_backoff_secs(streak: u32, seed: &str) -> u32 {
    let base = 1u64
        .checked_shl(streak.saturating_sub(1).min(6))
        .map(|v| v.min(60))
        .unwrap_or(60);
    let jitter_span = (base / 4).max(1);
    let mut h = unix_nanos();
    for b in seed.bytes() {
        h = h.wrapping_mul(31).wrapping_add(u64::from(b));
    }
    (base + h % (jitter_span + 1)).min(64) as u32
}

/// T37b-r5 platform-side lane backoff. Any well-formed response resets it.
fn platform_backoff_secs(streak: u32, seed: &str) -> u32 {
    bounded_backoff_secs(streak, seed)
}

/// Settlement errors leave claimed rows leased until expiry. Pause new
/// claims briefly so a failing backend does not receive a hot loop of writes.
fn settlement_backoff_secs(streak: u32, seed: &str) -> u32 {
    bounded_backoff_secs(streak, seed)
}

/// Which source-queue settlement calls of a batch failed. A dead-letter
/// *forward* failure is kept apart from a source-backend failure on purpose:
/// the exhausted message keeps its lease (expiry bounds the retry), while the
/// batch's acks and retries reached the source queue -- pausing the whole lane
/// would stall unrelated healthy traffic behind an unavailable DLQ.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum SettleFailure {
    #[default]
    None,
    /// `dlq_send` failed (the DLQ is a different queue id).
    DeadLetter,
    /// `ack` / `retry` / `dead_letter` on the source queue failed.
    Source,
}

impl SettleFailure {
    /// Only a source-backend failure justifies the claim backoff.
    fn backs_off_source_lane(self) -> bool {
        matches!(self, Self::Source)
    }

    /// A batch can fail on both sides: the source failure is the one that
    /// gates claims, so it wins.
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Source, _) | (_, Self::Source) => Self::Source,
            (Self::DeadLetter, _) | (_, Self::DeadLetter) => Self::DeadLetter,
            _ => Self::None,
        }
    }
}

/// T37b-r5 platform-side give-back: calls `release` grouped by lease_id -- visible_at
/// delayed to the backoff point, attempts-1 compensation; LeaseLost abandons that group (a successor has already
/// re-claimed); other errors warn -- the message lease expires and the message is naturally re-delivered (attempts
/// are not compensated, a conservative drift documented in the doc).
/// Returns the (lease_id, msgs) groups whose backend call failed and await retry -- the caller stores them in
/// `lane.release_retry` for retry on the next tick; messages do not
/// lose visibility before the lease expires, and the retry window is bounded by the lease TTL.
async fn settle_release(
    sql: &QueueSqlClient,
    throttle: &Arc<LogThrottle>,
    queue_id: &str,
    batch: &[PendingMsg],
    delay_secs: u32,
) -> Vec<(String, Vec<PendingMsg>)> {
    let mut groups: BTreeMap<&str, Vec<&PendingMsg>> = BTreeMap::new();
    for m in batch {
        groups.entry(m.lease_id.as_str()).or_default().push(m);
    }
    let mut failed: Vec<(String, Vec<PendingMsg>)> = Vec::new();
    for (lease, msgs) in groups {
        let items: Vec<(String, u32)> = msgs.iter().map(|m| (m.id.clone(), delay_secs)).collect();
        let n = items.len();
        match sql.release(queue_id, lease, &items).await {
            Ok(()) => {
                crate::metrics::queue_released(n);
                tracing::debug!(queue = %queue_id, n, delay_secs, "queue batch released (platform failure)");
            }
            Err(SqlCallError::LeaseLost) => {
                crate::metrics::queue_settle_blocked("lease_lost", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::info!(
                        queue = %queue_id, op = "release", n,
                        "queue settle skipped — lease lost (batch re-claimed elsewhere)"
                    );
                }
            }
            Err(e) => {
                crate::metrics::queue_settle_blocked("backend", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::warn!(
                        queue = %queue_id, op = "release", n, error = ?e,
                        "queue release failed — bounded retry each tick until lease expiry"
                    );
                }
                failed.push((
                    lease.to_string(),
                    msgs.iter().map(|m| (*m).clone()).collect(),
                ));
            }
        }
    }
    failed
}

fn parse_claimed(m: &serde_json::Value) -> Option<PendingMsg> {
    Some(PendingMsg {
        id: m.get("id")?.as_str()?.to_string(),
        lease_id: m.get("leaseId")?.as_str()?.to_string(),
        attempts: m.get("attempts")?.as_u64()? as u32,
        body_hex: m.get("body")?.as_str()?.to_string(),
        content_type: m.get("contentType")?.as_str()?.to_string(),
        timestamp_ms: m.get("timestampMs")?.as_i64()?,
    })
}

/// Settle a batch -- explicit per-message/batch acknowledgements take
/// precedence over batch failure/retry; otherwise miniflare
/// `processResponse` semantics (broker.worker.js:220-235,354-374): call
/// ack/retry/dead-letter grouped by lease_id; LeaseLost abandons that group
/// (the messages were already re-claimed by a successor -- another's batch
/// must never be touched by mistake).
/// Returns which calls failed: a source-backend failure pauses the lane's
/// claims, a dead-letter forward failure does not (see [`SettleFailure`]).
async fn settle(
    sql: &QueueSqlClient,
    throttle: &Arc<LogThrottle>,
    queue_id: &str,
    cfg: &ResolvedConsumer,
    batch: &[PendingMsg],
    result: &QueueOutcome,
) -> SettleFailure {
    // workerd keeps `ackAll`/`retryAll` mutually exclusive (whichever is
    // called first wins, queue.c++ QueueEvent::ackAll / retryAll), so
    // `ack_all` only has to beat the *implicit* retry of a failed outcome --
    // the batch the handler acknowledged and then threw on. An explicit
    // `retryBatch` still wins, which is also the fail-closed reading of a
    // contradictory envelope (never drop a message on ambiguity).
    let retry_all = result.retry_batch.retry || (result.outcome != "ok" && !result.ack_all);
    let explicit_ack_ids: BTreeSet<&str> =
        result.explicit_acks.iter().map(|s| s.as_str()).collect();
    let explicit_retry: BTreeSet<&str> =
        result.explicit_retries.iter().map(|s| s.as_str()).collect();
    let msg_delay: BTreeMap<&str, u32> = result
        .retry_messages
        .iter()
        .filter_map(|r| r.delay_seconds.map(|d| (r.msg_id.as_str(), d)))
        .collect();
    let explicit_retry_ids: BTreeSet<&str> = result
        .retry_messages
        .iter()
        .map(|r| r.msg_id.as_str())
        .chain(explicit_retry.iter().copied())
        .collect();
    // Grouped per lease_id -- one table each for ack/retry/dlq.
    let mut acks: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    let mut retries: BTreeMap<&str, Vec<(String, u32)>> = BTreeMap::new();
    let mut dlq: BTreeMap<&str, Vec<&PendingMsg>> = BTreeMap::new();
    let mut failure = SettleFailure::default();
    for m in batch {
        let lease = m.lease_id.as_str();
        // Cloudflare Queues preserves a per-message ack even when a later
        // message fails or the handler returns an error. Apply it before
        // batch-level retryAll/outcome handling. `ack_all` needs no per-message
        // branch: it clears the implicit failure retry above, so every message
        // without an explicit retry mark falls through to the ack below.
        if explicit_ack_ids.contains(m.id.as_str()) {
            acks.entry(lease).or_default().push(m.id.clone());
            continue;
        }
        let retry_this = retry_all
            || explicit_retry_ids.contains(m.id.as_str())
            || msg_delay.contains_key(m.id.as_str());
        if !retry_this {
            acks.entry(lease).or_default().push(m.id.clone());
            continue;
        }
        // attempts = this delivery's sequence number (+1 on claim) -- over the limit goes to the DLQ.
        if m.attempts >= cfg.max_retries + 1 {
            dlq.entry(lease).or_default().push(m);
            continue;
        }
        let delay = msg_delay
            .get(m.id.as_str())
            .copied()
            .or(result.retry_batch.delay_seconds)
            .unwrap_or(cfg.retry_delay_secs)
            .min(86400);
        retries
            .entry(lease)
            .or_default()
            .push((m.id.clone(), delay));
    }
    for (lease, ids) in &acks {
        let n = ids.len();
        match sql.ack(queue_id, lease, ids).await {
            Ok(()) => {
                crate::metrics::queue_delivered("ack", n);
                tracing::debug!(queue = %queue_id, n, "queue batch acked");
            }
            Err(SqlCallError::LeaseLost) => {
                crate::metrics::queue_settle_blocked("lease_lost", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::info!(
                        queue = %queue_id, op = "ack", n,
                        "queue settle skipped — lease lost (batch re-claimed elsewhere)"
                    );
                }
            }
            Err(e) => {
                failure = failure.merge(SettleFailure::Source);
                crate::metrics::queue_settle_blocked("backend", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::warn!(
                        queue = %queue_id, op = "ack", n, error = ?e,
                        "queue settle failed — messages redeliver on lease expiry"
                    );
                }
            }
        }
    }
    for (lease, items) in &retries {
        let n = items.len();
        match sql.retry_msgs(queue_id, lease, items).await {
            Ok(()) => {
                crate::metrics::queue_delivered("retry", n);
                tracing::debug!(queue = %queue_id, n, "queue batch retried");
            }
            Err(SqlCallError::LeaseLost) => {
                crate::metrics::queue_settle_blocked("lease_lost", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::info!(
                        queue = %queue_id, op = "retry", n,
                        "queue settle skipped — lease lost (batch re-claimed elsewhere)"
                    );
                }
            }
            Err(e) => {
                failure = failure.merge(SettleFailure::Source);
                crate::metrics::queue_settle_blocked("backend", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::warn!(
                        queue = %queue_id, op = "retry", n, error = ?e,
                        "queue settle failed — messages redeliver on lease expiry"
                    );
                }
            }
        }
    }
    for (lease, msgs) in &dlq {
        let n = msgs.len();
        if let Some(dlq_id) = &cfg.dlq_queue_id {
            // Write to the DLQ first (same msg id => idempotent dedup), then dead-letter the source
            // queue -- on failure this batch is re-delivered after lease expiry, never double-written and never lost.
            match sql.dlq_send(dlq_id, msgs).await {
                Ok(()) => {}
                // Any DLQ write failure skips dead_letter -- the source row keeps its lease and is
                // re-delivered after expiry => takes the DLQ path again and is deduplicated idempotently by id.
                // "When the DLQ is unavailable the message stays in the original queue" (task acceptance semantics).
                Err(e) => {
                    failure = failure.merge(SettleFailure::DeadLetter);
                    crate::metrics::queue_settle_blocked("dlq_send", n);
                    if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                        tracing::warn!(
                            queue = %queue_id, op = "dlq_send", n, error = ?e,
                            "queue dead-letter forward failed — source batch retained"
                        );
                    }
                    continue;
                }
            }
        }
        let ids: Vec<String> = msgs.iter().map(|m| m.id.clone()).collect();
        match sql.dead_letter(queue_id, lease, &ids, "max-retries").await {
            Ok(()) => {
                let mode = if cfg.dlq_queue_id.is_some() {
                    "dlq"
                } else {
                    "dropped"
                };
                crate::metrics::queue_delivered(mode, n);
                // A dead letter means dropping the message -- leave a trace for every batch (info, naturally rate limited per batch).
                tracing::info!(
                    queue = %queue_id, mode, n,
                    "queue batch dead-lettered after max retries"
                );
            }
            Err(SqlCallError::LeaseLost) => {
                crate::metrics::queue_settle_blocked("lease_lost", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::info!(
                        queue = %queue_id, op = "dead_letter", n,
                        "queue settle skipped — lease lost (batch re-claimed elsewhere)"
                    );
                }
            }
            Err(e) => {
                failure = failure.merge(SettleFailure::Source);
                crate::metrics::queue_settle_blocked("backend", n);
                if throttle.allow(&format!("settle:{queue_id}"), LOG_WARN_INTERVAL) {
                    tracing::warn!(
                        queue = %queue_id, op = "dead_letter", n, error = ?e,
                        "queue settle failed — messages redeliver on lease expiry"
                    );
                }
            }
        }
    }
    failure
}

// ---------------------------------------------------------------------------
// Tests -- in-process fake vrt-sql (implements semantics per the wire.rs endpoint shapes: dispatch lease
// CAS, message lease_id/attempts/lease_expiry, idempotent send) + memory invoker,
// covering T37b acceptance: outcome branches / delay precedence / DLQ forwarding and unreachability /
// dispatch lease exclusivity / stale settlement rejected / slow consumer fairness / redelivery after a crash before settlement.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::assign::AssignedWorker;
    use axum::extract::{Path, State};
    use axum::routing::post;
    use axum::Json;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[derive(Default)]
    struct FakeMsg {
        body_hex: String,
        content_type: String,
        timestamp_ms: i64,
        visible_at: i64,
        attempts: u32,
        lease_id: Option<String>,
        lease_expiry: i64,
    }

    #[derive(Default)]
    struct FakeQueue {
        msgs: BTreeMap<String, FakeMsg>,
        dead: Vec<String>,
        holder: Option<(String, i64)>,
        epoch: u64,
        seq: u64,
    }

    #[derive(Default)]
    struct FakeWorld {
        queues: HashMap<String, FakeQueue>,
        sent: Vec<(String, String)>, // (queue_id, msg_id) audit
        fail_ack: bool,
        /// Queue id whose `send` (the DLQ forward) fails with a retryable 503.
        fail_send_for: Option<String>,
        /// Forward attempts that actually failed -- lets a test wait for the
        /// failed `dlq_send` before inspecting the lane.
        failed_sends: u64,
    }

    type Shared = Arc<StdMutex<FakeWorld>>;

    async fn fake_op(
        State(w): State<Shared>,
        Path((qid, op)): Path<(String, String)>,
        Json(body): Json<Value>,
    ) -> (axum::http::StatusCode, Json<Value>) {
        let mut world = w.lock().unwrap();
        let now = now_ms();
        if op == "ack" && world.fail_ack {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "success": false,
                    "code": "VRT_UNAVAILABLE",
                    "retryable": true
                })),
            );
        }
        if op == "send" && world.fail_send_for.as_deref() == Some(qid.as_str()) {
            world.failed_sends += 1;
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "success": false,
                    "code": "VRT_UNAVAILABLE",
                    "retryable": true
                })),
            );
        }
        let q = world.queues.entry(qid.clone()).or_default();
        match op.as_str() {
            "send" => {
                let mut enq = 0usize;
                let mut dups = 0usize;
                let mut sent_ids: Vec<String> = Vec::new();
                for m in body["messages"].as_array().cloned().unwrap_or_default() {
                    let id = m["id"].as_str().unwrap_or("").to_string();
                    if q.msgs.contains_key(&id) {
                        dups += 1;
                        continue;
                    }
                    q.msgs.insert(
                        id.clone(),
                        FakeMsg {
                            body_hex: m["body"].as_str().unwrap_or("").to_string(),
                            content_type: m["contentType"].as_str().unwrap_or("text").to_string(),
                            timestamp_ms: m["timestampMs"].as_i64().unwrap_or(now),
                            visible_at: now + m["delaySecs"].as_i64().unwrap_or(0) * 1000,
                            ..Default::default()
                        },
                    );
                    enq += 1;
                    // sent audit is appended below after the queue borrow ends
                    sent_ids.push(id);
                }
                for id in sent_ids {
                    world.sent.push((qid.clone(), id));
                }
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"success": true, "enqueued": enq, "duplicates": dups})),
                )
            }
            "claim" => {
                let holder = body["holderId"].as_str().unwrap_or("");
                let ttl = body["holderTtlMs"].as_i64().unwrap_or(0);
                let max = body["max"].as_u64().unwrap_or(0) as usize;
                let lttl = body["leaseTtlMs"].as_i64().unwrap_or(0);
                let max_bytes = body["maxBytes"].as_u64();
                // dispatch lease CAS
                if let Some((h, exp)) = &q.holder {
                    if h != holder && *exp > now {
                        return (
                            axum::http::StatusCode::CONFLICT,
                            Json(json!({
                                "success": false,
                                "code": "VRT_QUEUE_DISPATCH_LEASE",
                                "error": format!("queue dispatch lease held by {h} until {exp}"),
                                "retryable": false
                            })),
                        );
                    }
                    if h != holder && *exp <= now {
                        q.epoch += 1;
                    }
                } else {
                    q.epoch = 1;
                }
                q.holder = Some((holder.to_string(), now + ttl));
                q.seq += 1;
                let lease = format!("L{}", q.seq);
                let mut msgs = Vec::new();
                let mut acc_bytes = 0u64;
                for (id, m) in q.msgs.iter_mut() {
                    if msgs.len() >= max {
                        break;
                    }
                    // T37b-r: same as the real implementation -- raw bytes budget + a >=1 floor.
                    let nb = acc_bytes.saturating_add(m.body_hex.len() as u64 / 2);
                    if !msgs.is_empty() && max_bytes.is_some_and(|b| nb > b) {
                        break;
                    }
                    let leased = m.lease_id.is_some() && m.lease_expiry > now;
                    if leased || m.visible_at > now {
                        continue;
                    }
                    acc_bytes = nb;
                    m.attempts += 1;
                    m.lease_id = Some(lease.clone());
                    m.lease_expiry = now + lttl;
                    msgs.push(json!({
                        "id": id,
                        "leaseId": lease,
                        "attempts": m.attempts,
                        "body": m.body_hex,
                        "contentType": m.content_type,
                        "timestampMs": m.timestamp_ms,
                    }));
                }
                (
                    axum::http::StatusCode::OK,
                    Json(
                        json!({"success": true, "leaseId": lease, "dispatchEpoch": q.epoch, "messages": msgs}),
                    ),
                )
            }
            "ack" | "retry" | "release" | "dead-letter" => {
                let lease = body["leaseId"].as_str().unwrap_or("");
                let ids: Vec<String> = match op.as_str() {
                    "retry" | "release" => body["items"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|i| i["id"].as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    _ => body["ids"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|i| i.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                };
                for id in &ids {
                    match q.msgs.get(id) {
                        Some(m) if m.lease_id.as_deref() == Some(lease) && m.lease_expiry > now => {
                        }
                        _ => {
                            return (
                                axum::http::StatusCode::CONFLICT,
                                Json(
                                    json!({"success": false, "code": "VRT_QUEUE_LEASE_LOST", "retryable": false}),
                                ),
                            )
                        }
                    }
                }
                match op.as_str() {
                    "ack" => {
                        for id in &ids {
                            q.msgs.remove(id);
                        }
                    }
                    "dead-letter" => {
                        for id in &ids {
                            q.msgs.remove(id);
                            q.dead.push(id.clone());
                        }
                    }
                    "release" => {
                        // T37b-r5: same as the real implementation -- lease cleared, visible delay,
                        // attempts-1 compensates the +1 from claim.
                        for (i, item) in body["items"].as_array().unwrap().iter().enumerate() {
                            let id = &ids[i];
                            let delay = item["delaySecs"].as_i64().unwrap_or(0);
                            let m = q.msgs.get_mut(id).unwrap();
                            m.lease_id = None;
                            m.visible_at = now + delay * 1000;
                            m.attempts = m.attempts.saturating_sub(1);
                        }
                    }
                    _ => {
                        for (i, item) in body["items"].as_array().unwrap().iter().enumerate() {
                            let id = &ids[i];
                            let delay = item["delaySecs"].as_i64().unwrap_or(0);
                            let m = q.msgs.get_mut(id).unwrap();
                            m.lease_id = None;
                            m.visible_at = now + delay * 1000;
                        }
                    }
                }
                (axum::http::StatusCode::OK, Json(json!({"success": true})))
            }
            _ => (
                axum::http::StatusCode::NOT_FOUND,
                Json(json!({"success": false, "code": "VRT_NOOP"})),
            ),
        }
    }

    async fn spawn_fake_sql() -> (String, Shared) {
        let world: Shared = Arc::new(StdMutex::new(FakeWorld::default()));
        let app = axum::Router::new()
            .route("/queues/{id}/{op}", post(fake_op))
            .with_state(world.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (addr, world)
    }

    fn cfg(holder: &str) -> DispatchConfig {
        DispatchConfig {
            holder_id: holder.into(),
            dispatch_ttl_ms: 5_000,
            msg_lease_ms: 30_000,
            delivery_timeout: Duration::from_secs(5),
            global_inflight_max: 8,
            claim_max_bytes: CLAIM_BUDGET_DEFAULT_BYTES,
        }
    }

    const QID: &str = "qqqqqqqqqqqqqqqqqqqqqqqqqq";
    const DLQID: &str = "dddddddddddddddddddddddddd";

    fn desired_with(consumers: Value) -> ComputeDesired {
        serde_json::from_value(json!({
            "schemaVersion": 1,
            "cellId": "cell-1",
            "revision": 1,
            "tenants": [{
                "tenantId": "t1",
                "workers": [{
                    "workerId": "w1",
                    "name": "consumer",
                    "activeVersion": {
                        "versionId": "v1",
                        "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2025-01-01",
                        "compatibilityFlags": []
                    },
                    "bindings": [],
                    "routes": [],
                    "crons": [],
                    "queueConsumers": consumers,
                    "quotas": {"cpuMs": 50, "memoryMb": 128, "subrequests": 0, "outboundConnections": 0}
                }]
            }]
        }))
        .unwrap()
}

    fn assignments() -> BTreeMap<String, PodAssignment> {
        let mut m = BTreeMap::new();
        m.insert(
            "pod-1".to_string(),
            PodAssignment {
                pod_id: "pod-1".into(),
                revision: 1,
                payload_digest: "x".into(),
                envelope_digest: "y".into(),
                generated_at: String::new(),
                workers: vec![AssignedWorker {
                    tenant_id: "t1".into(),
                    worker_id: "w1".into(),
                }],
            },
        );
        m
    }

    struct FakeInvoker {
        result: StdMutex<Result<QueueOutcome, DeliveryError>>,
        calls: AtomicU64,
        delay: Option<Duration>,
        last: StdMutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl QueueInvoker for FakeInvoker {
        async fn dispatch(
            &self,
            _pod: &str,
            req: DispatchRequest,
        ) -> Result<QueueOutcome, DeliveryError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.last.lock().unwrap().push(req.dispatch);
            if let Some(d) = self.delay {
                tokio::time::sleep(d).await;
            }
            let r = std::mem::replace(
                &mut *self.result.lock().unwrap(),
                Ok(QueueOutcome::default()),
            );
            r
        }
    }

    /// Returns the same outcome on every call (raw JSON re-decoded) -- user-side failures
    /// need to persist across many calls (after T37b-r5 transport no longer consumes attempts, so it can
    /// no longer be used to simulate a handler throw).
    struct StickyInvoker {
        raw: Value,
        calls: AtomicU64,
        last: StdMutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl QueueInvoker for StickyInvoker {
        async fn dispatch(
            &self,
            _pod: &str,
            req: DispatchRequest,
        ) -> Result<QueueOutcome, DeliveryError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.last.lock().unwrap().push(req.dispatch);
            serde_json::from_value(self.raw.clone()).map_err(|_| DeliveryError::of("malformed"))
        }
    }

    fn outcome(v: Value) -> QueueOutcome {
        serde_json::from_value(v).unwrap()
    }

    /// Always-ok invoker -- the result of `FakeInvoker` is consume-once,
    /// so any test needing multi-batch delivery must use this one.
    struct AlwaysOk {
        calls: AtomicU64,
    }
    #[async_trait::async_trait]
    impl QueueInvoker for AlwaysOk {
        async fn dispatch(
            &self,
            _pod: &str,
            _req: DispatchRequest,
        ) -> Result<QueueOutcome, DeliveryError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(QueueOutcome {
                outcome: "ok".into(),
                ack_all: true,
                ..Default::default()
            })
        }
    }

    /// Counts concurrently running dispatches and blocks them until released --
    /// deterministic `max_concurrency` observation, no wall-clock sleeps.
    struct GatedInvoker {
        started: AtomicU64,
        active: AtomicUsize,
        max_active: AtomicUsize,
        gate: tokio::sync::Semaphore,
    }

    impl GatedInvoker {
        fn new() -> Self {
            Self {
                started: AtomicU64::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                gate: tokio::sync::Semaphore::new(0),
            }
        }
        fn release_all(&self) {
            self.gate.add_permits(64);
        }
    }

    #[async_trait::async_trait]
    impl QueueInvoker for GatedInvoker {
        async fn dispatch(
            &self,
            _pod: &str,
            _req: DispatchRequest,
        ) -> Result<QueueOutcome, DeliveryError> {
            self.started.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            let permit = self.gate.acquire().await.expect("gate semaphore open");
            permit.forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(QueueOutcome {
                outcome: "ok".into(),
                ack_all: true,
                ..Default::default()
            })
        }
    }

    #[test]
    fn bug2_sql_client_endpoint_policy() {
        let mk = |a: &str| QueueSqlClient::new(a, b"s".to_vec(), Duration::from_secs(1));
        assert!(mk("http://vrt-sql.vrt-system.svc:8082").is_ok());
        assert!(mk("http://127.0.0.1:8082").is_ok());
        assert!(mk("http://example.com:8082").is_err());
        assert!(mk("http://127.0.0.1.evil.test:8082").is_err());
        assert!(mk("http://10.1.2.3:8082").is_err());
        // standalone policy unchanged (operator-configured internal name)
        assert!(
            QueueSqlClient::with_policy("http://vrt-sql:8090", b"s".to_vec(), Duration::from_secs(1), false)
                .is_ok()
        );
    }

    fn dispatcher(
        sql_addr: &str,
        inv: Arc<dyn QueueInvoker>,
        cfg: DispatchConfig,
    ) -> QueueDispatcher {
        QueueDispatcher::new(
            Arc::new(
                QueueSqlClient::new(sql_addr, b"test-secret".to_vec(), Duration::from_secs(5))
                    .unwrap(),
            ),
            inv,
            cfg,
        )
        .unwrap()
    }

    /// The batch timeout is a real 1s -- the first tick only claims; the second tick after 1.1s
    /// fills up / times out and dispatches.
    async fn tick_pair(
        d: &QueueDispatcher,
        desired: &ComputeDesired,
        exp: chrono::DateTime<chrono::Utc>,
        assigns: &BTreeMap<String, PodAssignment>,
    ) -> DispatchTickOutcome {
        let mut out = d
            .tick(Some(desired), Some(exp), assigns, chrono::Utc::now())
            .await;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let o2 = d
            .tick(Some(desired), Some(exp), assigns, chrono::Utc::now())
            .await;
        out.dispatched += o2.dispatched;
        out.claimed += o2.claimed;
        out.lease_lost += o2.lease_lost;
        out.claim_errors += o2.claim_errors;
        out.lease_held += o2.lease_held;
        out
    }

    async fn settle_until(world: &Shared, f: impl Fn(&FakeWorld) -> bool) {
        for _ in 0..200 {
            if f(&world.lock().unwrap()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("settle condition not reached");
    }

    #[test]
    fn config_validate_bounds() {
        assert!(cfg("h").validate().is_ok());
        let mut bad = cfg("h");
        bad.holder_id = "bad holder!".into();
        assert!(bad.validate().is_err());
        let mut bad = cfg("h");
        bad.dispatch_ttl_ms = 1_000;
        assert!(bad.validate().is_err());
        let mut bad = cfg("h");
        bad.msg_lease_ms = bad.delivery_timeout.as_millis() as i64 + 1_000;
        assert!(bad.validate().is_err()); // <= timeout+margin
        let mut bad = cfg("h");
        bad.global_inflight_max = 0;
        assert!(bad.validate().is_err());
    }

    #[tokio::test]
    async fn tick_without_desired_dispatches_nothing() {
        let (addr, world) = spawn_fake_sql().await;
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({"outcome":"ok","ackAll":true})))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let d0 = desired_with(json!([]));
        let assigns = assignments();
        let out = d.tick(None, None, &assigns, chrono::Utc::now()).await;
        assert_eq!(out.claimed, 0);
        // An expired desired likewise has no dispatch authority
        let out = d
            .tick(
                Some(&d0),
                Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
                &assigns,
                chrono::Utc::now(),
            )
            .await;
        assert_eq!(out.claimed, 0);
        assert!(world.lock().unwrap().queues.is_empty());
    }

    #[tokio::test]
    async fn ack_all_delivers_and_acks() {
        let (addr, world) = spawn_fake_sql().await;
        // Preload one message
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({"outcome":"ok","ackAll":true})))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q1"}]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        let out = tick_pair(&d, &desired, exp, &assigns).await;
        assert_eq!(out.claimed, 1);
        assert_eq!(out.dispatched, 1);
        settle_until(&world, |w| {
            w.queues
                .get(QID)
                .map(|q| q.msgs.is_empty())
                .unwrap_or(false)
        })
        .await;
        // facade payload: queue display name + message shape
        let calls = inv.last.lock().unwrap();
        assert_eq!(calls[0]["queue"], "q1");
        assert_eq!(calls[0]["messages"][0]["id"], "m1");
        assert_eq!(calls[0]["messages"][0]["attempts"], 1);
    }

    #[tokio::test]
    async fn max_concurrency_caps_inflight_batches_per_lane() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            for i in 0..6 {
                q.msgs.insert(
                    format!("m{i}"),
                    FakeMsg {
                        body_hex: "aa".into(),
                        ..Default::default()
                    },
                );
            }
        }
        let inv = Arc::new(GatedInvoker::new());
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        // One message per batch so every claim is immediately dispatchable
        // (no reliance on the batch timeout), two in-flight batches allowed.
        let desired = desired_with(json!([{
            "queueId": QID,
            "queueName": "q",
            "maxBatchSize": 1,
            "maxConcurrency": 2
        }]));
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        let assigns = assignments();

        // Drive ticks until both allowed deliveries are running.
        for _ in 0..100 {
            if inv.started.load(Ordering::SeqCst) >= 2 {
                break;
            }
            d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            inv.started.load(Ordering::SeqCst),
            2,
            "exactly max_concurrency deliveries may start"
        );
        // Both slots busy: the lane must not claim or dispatch a third batch.
        let out = d
            .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
            .await;
        assert_eq!(out.dispatched, 0);
        assert_eq!(out.claimed, 0);
        {
            let lanes = d.lanes.lock().await;
            assert_eq!(lanes.get(QID).unwrap().inflight, 2);
        }
        for _ in 0..100 {
            if inv.active.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(inv.max_active.load(Ordering::SeqCst), 2);

        // Open the gate and keep ticking: all six messages drain, and the cap
        // holds for the whole run, not just the first two batches.
        inv.release_all();
        for _ in 0..200 {
            d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            if world.lock().unwrap().queues[QID].msgs.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            world.lock().unwrap().queues[QID].msgs.is_empty(),
            "all messages must drain"
        );
        assert_eq!(inv.started.load(Ordering::SeqCst), 6);
        assert_eq!(
            inv.max_active.load(Ordering::SeqCst),
            2,
            "in-flight batches must never exceed max_concurrency"
        );
    }

    #[tokio::test]
    async fn settlement_backend_failure_backs_off_new_claims() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.fail_ack = true;
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({"outcome":"ok"})))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q1"}]));
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        tick_pair(&d, &desired, exp, &assignments()).await;

        let deadline = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                {
                    let lanes = d.lanes.lock().await;
                    if let Some(lane) = lanes.get(QID) {
                        if lane.inflight == 0 && lane.settle_streak == 1 {
                            return lane.settle_not_before_ms;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("settlement failure should update the lane backoff");
        let remaining_ms = deadline - now_ms();
        assert!(remaining_ms > 0, "backoff deadline must be in the future");
        assert!(remaining_ms <= 64_000, "backoff must remain bounded");
        assert!(world.lock().unwrap().queues[QID].msgs.contains_key("m1"));

        let seq_before = world.lock().unwrap().queues[QID].seq;
        let out = d
            .tick(
                Some(&desired),
                Some(exp),
                &assignments(),
                chrono::Utc::now(),
            )
            .await;
        assert_eq!(
            out.claimed, 0,
            "the lane must wait through settlement backoff"
        );
        assert_eq!(world.lock().unwrap().queues[QID].seq, seq_before);
    }

    #[tokio::test]
    async fn explicit_retry_mixed_and_delay_precedence() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            for id in ["a", "b", "c"] {
                q.msgs.insert(
                    id.into(),
                    FakeMsg {
                        body_hex: "aa".into(),
                        ..Default::default()
                    },
                );
            }
        }
        // a explicit retry (message-level delay 3s), b explicit ack, c batch-level default -> ack
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({
                "outcome": "ok",
                "explicitAcks": ["b"],
                "retryMessages": [{"msgId": "a", "delaySeconds": 3}]
            })))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q", "maxBatchSize": 3}]));
        d.tick(
            Some(&desired),
            Some(chrono::Utc::now() + chrono::Duration::minutes(5)),
            &assignments(),
            chrono::Utc::now(),
        )
        .await;
        settle_until(&world, |w| {
            w.queues
                .get(QID)
                .map(|q| q.msgs.len() == 1)
                .unwrap_or(false)
        })
        .await;
        let w = world.lock().unwrap();
        let q = w.queues.get(QID).unwrap();
        assert!(q.msgs.contains_key("a"));
        let a = &q.msgs["a"];
        assert!(a.visible_at > now_ms() + 2_000, "msg-level delay honored");
        assert_eq!(a.attempts, 1);
    }

    #[tokio::test]
    async fn explicit_ack_survives_batch_failure() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "acked".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({
                "outcome": "exception",
                "retryBatch": {"retry": true},
                "explicitAcks": ["acked"]
            })))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID,
            "queueName": "q",
            "maxBatchSize": 1,
            "retryDelaySecs": 0
        }]));
        tick_pair(
            &d,
            &desired,
            chrono::Utc::now() + chrono::Duration::minutes(5),
            &assignments(),
        )
        .await;
        settle_until(&world, |w| {
            w.queues.get(QID).is_some_and(|q| q.msgs.is_empty())
        })
        .await;
        let w = world.lock().unwrap();
        let q = &w.queues[QID];
        assert!(q.msgs.is_empty(), "explicit ack survives the batch failure");
    }

    /// workerd's `QueueEvent::ackAll` sets only `ackAll`; a handler that acks
    /// the whole batch and then throws yields
    /// `{outcome:"exception", ackAll:true, explicitAcks:[]}`. The implicit
    /// retryAll of the failed outcome must not resurrect those messages.
    #[tokio::test]
    async fn ack_all_survives_handler_exception() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            for id in ["m1", "m2"] {
                q.msgs.insert(
                    id.into(),
                    FakeMsg {
                        body_hex: "aa".into(),
                        ..Default::default()
                    },
                );
            }
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({
                "outcome": "exception",
                "ackAll": true
            })))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID,
            "queueName": "q",
            "maxBatchSize": 2,
            "retryDelaySecs": 0
        }]));
        tick_pair(
            &d,
            &desired,
            chrono::Utc::now() + chrono::Duration::minutes(5),
            &assignments(),
        )
        .await;
        settle_until(&world, |w| {
            w.queues.get(QID).is_some_and(|q| q.msgs.is_empty())
        })
        .await;
        let w = world.lock().unwrap();
        let q = &w.queues[QID];
        assert!(
            q.msgs.is_empty(),
            "ackAll survives the handler exception: {:?}",
            q.msgs.keys().collect::<Vec<_>>()
        );
        assert!(
            q.dead.is_empty(),
            "acked messages must not be dead-lettered"
        );
    }

    /// `retry()` marks recorded before `ackAll()` still win: workerd's
    /// `ackAll()` does not clear `result->retries`, and every other message in
    /// the batch is acknowledged (outcome=exception, ackAll=true).
    #[tokio::test]
    async fn ack_all_preserves_explicit_retries_before_it() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            for id in ["a1", "a2", "kept"] {
                q.msgs.insert(
                    id.into(),
                    FakeMsg {
                        body_hex: "aa".into(),
                        ..Default::default()
                    },
                );
            }
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({
                "outcome": "exception",
                "ackAll": true,
                "retryMessages": [{"msgId": "kept", "delaySeconds": 30}]
            })))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID,
            "queueName": "q",
            "maxBatchSize": 3,
            "retryDelaySecs": 0
        }]));
        tick_pair(
            &d,
            &desired,
            chrono::Utc::now() + chrono::Duration::minutes(5),
            &assignments(),
        )
        .await;
        settle_until(&world, |w| {
            w.queues.get(QID).is_some_and(|q| q.msgs.len() == 1)
        })
        .await;
        let w = world.lock().unwrap();
        let q = &w.queues[QID];
        assert_eq!(
            q.msgs.keys().map(|s| s.as_str()).collect::<Vec<_>>(),
            vec!["kept"],
            "only the pre-ackAll retry stays"
        );
        assert!(
            q.msgs["kept"].visible_at > now_ms() + 25_000,
            "the explicit retry delay is honored"
        );
        assert_eq!(q.msgs["kept"].attempts, 1);
    }

    #[tokio::test]
    async fn handler_throw_retries_all_then_redelivers() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        // T37b-r5: transport is platform-side and no longer consumes attempts -- handler
        // throw now uses the user-side outcome=exception (the consumer ran).
        let inv = Arc::new(StickyInvoker {
            raw: json!({"outcome":"exception"}),
            calls: AtomicU64::new(0),
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired =
            desired_with(json!([{"queueId": QID, "queueName": "q", "retryDelaySecs": 0}]));
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        tick_pair(&d, &desired, exp, &assignments()).await;
        settle_until(&world, |w| {
            w.queues
                .get(QID)
                .map(|q| {
                    q.msgs
                        .get("m1")
                        .map(|m| m.lease_id.is_none())
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        })
        .await;
        // Second round claim re-delivers, attempts incremented
        let out = tick_pair(&d, &desired, exp, &assignments()).await;
        assert!(out.claimed >= 1);
        settle_until(&world, |w| {
            w.queues
                .get(QID)
                .map(|q| q.msgs.get("m1").map(|m| m.attempts == 2).unwrap_or(false))
                .unwrap_or(false)
        })
        .await;
    }

    #[tokio::test]
    async fn max_retries_boundary_goes_to_dlq() {
        let (addr, world) = spawn_fake_sql().await;
        // The DLQ queue exists
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(DLQID.into()).or_default();
            let q = w.queues.entry(QID.into()).or_default();
            // attempts=2 means 3 deliveries are used up (maxRetries=2) -> this batch settles into the DLQ
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    attempts: 2,
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({
                "outcome": "ok", "explicitRetries": ["m1"]
            })))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID, "queueName": "q", "maxRetries": 2,
            "deadLetterQueueId": DLQID
        }]));
        tick_pair(
            &d,
            &desired,
            chrono::Utc::now() + chrono::Duration::minutes(5),
            &assignments(),
        )
        .await;
        settle_until(&world, |w| {
            w.queues
                .get(QID)
                .map(|q| q.msgs.is_empty())
                .unwrap_or(false)
        })
        .await;
        let w = world.lock().unwrap();
        // On claim attempts is already +1 -> 3 = maxRetries+1 -> DLQ forwarding + dead-letter
        assert!(
            w.queues[DLQID].msgs.contains_key("m1"),
            "transferred to DLQ"
        );
        assert!(w.queues[QID].dead.contains(&"m1".to_string()));
        assert!(w.queues[QID].msgs.is_empty());
    }

    #[tokio::test]
    async fn dlq_unavailable_keeps_source_message() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            // The DLQ forward (a `send` to a different queue id) fails for real.
            w.fail_send_for = Some(DLQID.into());
            let q = w.queues.entry(QID.into()).or_default();
            // attempts=2 + claim +1 => 3 = maxRetries+1 -> the DLQ path.
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    attempts: 2,
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(
                json!({"outcome":"ok","explicitRetries":["m1"]}),
            ))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID, "queueName": "q", "maxRetries": 2,
            "deadLetterQueueId": DLQID
        }]));
        tick_pair(
            &d,
            &desired,
            chrono::Utc::now() + chrono::Duration::minutes(5),
            &assignments(),
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while world.lock().unwrap().failed_sends == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the DLQ forward was never attempted"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let w = world.lock().unwrap();
        // No dead-letter happened: the source row keeps its lease and the next
        // attempt after expiry forwards again (idempotent by msg id).
        assert!(
            w.queues[QID].msgs.contains_key("m1"),
            "source message retained"
        );
        assert!(
            !w.queues[QID].dead.contains(&"m1".to_string()),
            "no dead-letter after a failed forward"
        );
    }

    /// The DLQ is a different queue: a failing forward must not pause claims of
    /// unrelated healthy messages on the source lane, and must not advance the
    /// settlement-failure streak. The exhausted message's own lease bounds its
    /// forwarding retry.
    #[tokio::test]
    async fn dlq_forward_failure_does_not_pause_source_claims() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.fail_send_for = Some(DLQID.into());
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "exhausted".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    attempts: 2,
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({
                "outcome": "ok",
                "explicitRetries": ["exhausted"]
            })))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID, "queueName": "q", "maxRetries": 2,
            "deadLetterQueueId": DLQID
        }]));
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        tick_pair(&d, &desired, exp, &assignments()).await;

        // Wait for the failed forward *and* the lane update that follows settle.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let failed_send = world.lock().unwrap().failed_sends >= 1;
            let inflight = d
                .lanes
                .lock()
                .await
                .get(QID)
                .map(|l| l.inflight)
                .unwrap_or(0);
            if failed_send && inflight == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the DLQ forward never completed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        {
            let lanes = d.lanes.lock().await;
            let lane = lanes.get(QID).expect("lane exists");
            assert_eq!(
                lane.settle_streak, 0,
                "a DLQ-only failure must not advance the settlement streak"
            );
            assert_eq!(
                lane.settle_not_before_ms, 0,
                "a DLQ-only failure must not pause source claims"
            );
        }

        // A message that arrives after the failed forward is still claimed on the
        // very next tick (the exhausted one stays leased until expiry).
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "later".into(),
                FakeMsg {
                    body_hex: "bb".into(),
                    ..Default::default()
                },
            );
        }
        let out = d
            .tick(
                Some(&desired),
                Some(exp),
                &assignments(),
                chrono::Utc::now(),
            )
            .await;
        assert_eq!(
            out.claimed, 1,
            "healthy traffic must not be paused by an unavailable DLQ"
        );
    }

    #[tokio::test]
    async fn dispatch_lease_excludes_second_holder() {
        let (addr, _world) = spawn_fake_sql().await;
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({"outcome":"ok"})))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let a = dispatcher(&addr, inv.clone(), cfg("holder-a"));
        let b = dispatcher(&addr, inv.clone(), cfg("holder-b"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q"}]));
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        let assigns = assignments();
        let outa = a
            .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
            .await;
        let outb = b
            .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
            .await;
        assert_eq!(outa.lease_lost, 0);
        assert_eq!(outb.lease_lost, 1, "second holder fenced by dispatch lease");
    }

    #[tokio::test]
    async fn stale_settle_rejected_by_lease_guard() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let sql = QueueSqlClient::new(&addr, b"s".to_vec(), Duration::from_secs(5)).unwrap();
        // holder claims with the lease -- leaseTtl uses the smallest positive value so the lease expires quickly
        let doc = sql.claim(QID, "h", 5_000, 1, None, 1).await.unwrap();
        let old_lease = doc["messages"][0]["leaseId"].as_str().unwrap().to_string();
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Re-claim -- the old lease expired, the new lease_id takes over
        let doc2 = sql.claim(QID, "h", 5_000, 1, None, 30_000).await.unwrap();
        assert_ne!(doc2["messages"][0]["leaseId"].as_str().unwrap(), old_lease);
        // Settling with the old lease -> LeaseLost (never touch another's batch by mistake)
        let err = sql
            .ack(QID, &old_lease, &["m1".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(err, SqlCallError::LeaseLost));
    }

    #[tokio::test]
    async fn slow_consumer_does_not_starve_other_queues() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "slow1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
            w.queues.entry(DLQID.into()).or_default().msgs.insert(
                "fast1".into(),
                FakeMsg {
                    body_hex: "bb".into(),
                    ..Default::default()
                },
            );
        }
        // invoker: QID hangs for 300ms, DLQID returns immediately
        struct Mixed;
        #[async_trait::async_trait]
        impl QueueInvoker for Mixed {
            async fn dispatch(
                &self,
                _p: &str,
                req: DispatchRequest,
            ) -> Result<QueueOutcome, DeliveryError> {
                if req.dispatch["queue"] == "slowq" {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                Ok(outcome(json!({"outcome":"ok","ackAll":true})))
            }
        }
        let inv = Arc::new(Mixed);
        let d = dispatcher(&addr, inv, cfg("h1"));
        // Two consumers: QID (maxConcurrency=1, hangs) + DLQID (fast)
        let desired = desired_with(json!([
            {"queueId": QID, "queueName": "slowq", "maxConcurrency": 1},
            {"queueId": DLQID, "queueName": "fastq"}
        ]));
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        let assigns = assignments();
        let out = tick_pair(&d, &desired, exp, &assigns).await;
        assert_eq!(out.dispatched, 2, "both lanes dispatch in parallel");
        // The fast queue has already completed its ack within the slow queue's 300ms
        settle_until(&world, |w| {
            w.queues
                .get(DLQID)
                .map(|q| q.msgs.is_empty())
                .unwrap_or(false)
        })
        .await;
        settle_until(&world, |w| {
            w.queues
                .get(QID)
                .map(|q| q.msgs.is_empty())
                .unwrap_or(false)
        })
        .await;
    }

    #[tokio::test]
    async fn unassigned_consumer_is_skipped() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(FakeInvoker {
            result: StdMutex::new(Ok(outcome(json!({"outcome":"ok"})))),
            calls: AtomicU64::new(0),
            delay: None,
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv, cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q"}]));
        // Empty assignments -- consumer worker not assigned => this cell does not dispatch
        let out = d
            .tick(
                Some(&desired),
                Some(chrono::Utc::now() + chrono::Duration::minutes(5)),
                &BTreeMap::new(),
                chrono::Utc::now(),
            )
            .await;
        assert_eq!(out.claimed, 0);
        assert_eq!(out.dispatched, 0);
        assert_eq!(world.lock().unwrap().queues[QID].msgs["m1"].attempts, 0);
    }

    /// T37b-r: dispatcher must recover after the vrt-sql process
    /// restarts — claim errors during the outage, delivery resumes
    /// with the SAME durable state once the listener is back.
    #[tokio::test]
    async fn recovers_after_sql_restart() {
        // Bind a fixed port once; the "process" is killed by dropping
        // the listener, then restarted on the same port with the SAME
        // Shared world (vrt-sql persists queue state on disk).
        let world: Shared = Arc::new(StdMutex::new(FakeWorld::default()));
        let mk_app = |w: &Shared| {
            axum::Router::new()
                .route("/queues/{id}/{op}", post(fake_op))
                .with_state(w.clone())
        };
        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l0.local_addr().unwrap().port();
        let addr = format!("http://127.0.0.1:{port}");
        let app0 = mk_app(&world);
        let srv = tokio::spawn(async move { axum::serve(l0, app0).await.unwrap() });
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(AlwaysOk {
            calls: AtomicU64::new(0),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q"}]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);

        // Phase 1 — live sql: claim+deliver succeeds.
        let out = tick_pair(&d, &desired, exp, &assigns).await;
        assert!(out.claim_errors == 0, "{out:?}");
        settle_until(&world, |w| {
            w.queues.get(QID).is_none_or(|q| q.msgs.is_empty())
        })
        .await;
        assert_eq!(inv.calls.load(Ordering::Relaxed), 1);

        // Phase 2 — kill "sql": claims fail, dispatcher keeps ticking
        // (bounded errors, no panic, no permanent stall).
        srv.abort();
        let mut errored = 0usize;
        for _ in 0..30 {
            let o = d
                .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            errored += o.claim_errors;
            if errored > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(errored > 0, "claims should fail while sql is down");

        // Phase 3 — restart on the same port: recovery within bounded
        // ticks; new sends deliver, nothing dead-lettered spuriously.
        let l1 = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let app1 = mk_app(&world);
        let _srv2 = tokio::spawn(async move { axum::serve(l1, app1).await.unwrap() });
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "m2".into(),
                FakeMsg {
                    body_hex: "bb".into(),
                    ..Default::default()
                },
            );
        }
        // Recovery = claims succeed again AND m2 is acked (AlwaysOk
        // invoker ⇒ no legit dead-letter; the outage must not strand
        // or kill anything).
        let mut recovered = false;
        for _ in 0..60 {
            let o = tick_pair(&d, &desired, exp, &assigns).await;
            let terminal = {
                let w = world.lock().unwrap();
                let q = &w.queues[QID];
                q.msgs.is_empty()
            };
            if o.claim_errors == 0 && terminal {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(recovered, "dispatcher did not recover after sql restart");
        assert!(inv.calls.load(Ordering::Relaxed) >= 2);
        assert!(
            world.lock().unwrap().queues[QID].dead.is_empty(),
            "restart must not dead-letter messages"
        );
    }

    /// T37b-r: 100 x 128KB messages must be split into multiple batches under the 4MB claim budget
    /// -- all delivered, zero dead letters (batch splitting itself must never be judged fatal).
    #[tokio::test]
    async fn byte_budget_splits_large_backlog() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            let hex_body = "ab".repeat(128_000); // 128KB raw
            for i in 0..100u32 {
                q.msgs.insert(
                    format!("m{i:03}"),
                    FakeMsg {
                        body_hex: hex_body.clone(),
                        visible_at: 0,
                        ..Default::default()
                    },
                );
            }
        }
        let inv = Arc::new(AlwaysOk {
            calls: AtomicU64::new(0),
        });
        // batch_size 100: the first 3 batches form immediately by byte budget; the last batch of 4
        // does not fill the budget and relies on the 1s batch timeout as the fallback (CF batchTimeout semantics).
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired = desired_with(json!([{
            "queueId": QID, "queueName": "q",
            "maxBatchSize": 100, "maxBatchTimeoutSecs": 1,
            "maxRetries": 3
        }]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        for _ in 0..30 {
            tick_pair(&d, &desired, exp, &assigns).await;
            if world.lock().unwrap().queues[QID].msgs.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let w = world.lock().unwrap();
        assert!(
            w.queues[QID].msgs.is_empty(),
            "undelivered: {}",
            w.queues[QID].msgs.len()
        );
        assert!(
            w.queues[QID].dead.is_empty(),
            "batching must never dead-letter: {:?}",
            w.queues[QID].dead
        );
        // 4MB/128KB ~= 32 -> >=4 batches; sending everything in one batch would be 1.
        assert!(
            inv.calls.load(Ordering::Relaxed) >= 2,
            "byte budget must split into multiple batches"
        );
    }

    /// T37b-r: a single 128KB message must still be delivered under a tiny budget (1KB) --
    /// claim's >=1 floor guarantees large messages do not starve.
    #[tokio::test]
    async fn tiny_budget_still_delivers_oversize() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            w.queues.entry(QID.into()).or_default().msgs.insert(
                "big".into(),
                FakeMsg {
                    body_hex: "ab".repeat(128_000),
                    visible_at: 0,
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(AlwaysOk {
            calls: AtomicU64::new(0),
        });
        let mut c = cfg("h1");
        c.claim_max_bytes = 1_024; // 1KB — far under the single message
        let d = dispatcher(&addr, inv.clone(), c);
        let desired = desired_with(json!([{
            "queueId": QID, "queueName": "q", "maxRetries": 3
        }]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        for _ in 0..30 {
            tick_pair(&d, &desired, exp, &assigns).await;
            if world.lock().unwrap().queues[QID].msgs.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            world.lock().unwrap().queues[QID].msgs.is_empty(),
            "≥1-claim guarantee: oversize message must still deliver"
        );
        assert!(world.lock().unwrap().queues[QID].dead.is_empty());
        assert_eq!(inv.calls.load(Ordering::Relaxed), 1);
    }

    /// T37b-r4: real vrt-sql process + SIGTERM restart -- e2e field timing:
    /// dispatcher already holds the dispatch lease -> `restart vrt-sql` -> producer send ->
    /// delivery within a bounded time. The fake in `recovers_after_sql_restart` only simulates the
    /// wire shape and does not cover the real engine/registry/lease file/ServiceAuth
    /// rebuild path -- this test assembles the production `QueueSqlClient` + `QueueDispatcher`
    /// (the host/workerd delivery side is covered separately by the real_workerd_queue_consumer
    /// smoke). The vrt-sql binary is located with the same convention as d1_smoke
    /// (`VRT_SQL_BIN` or ../target/debug/vrt-sql).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_vrt_sql_restart_mid_dispatch_recovers() {
        use std::os::unix::process::CommandExt;
        use std::path::{Path, PathBuf};
        use std::process::{Child, Command, Stdio};

        fn vrt_sql_bin() -> PathBuf {
            if let Ok(p) = std::env::var("VRT_SQL_BIN") {
                return PathBuf::from(p);
            }
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/debug/vrt-sql")
        }
        fn free_port() -> u16 {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }
        fn spawn_sql(dir: &Path, port: u16) -> Child {
            let secrets = dir.join("keys");
            std::fs::create_dir_all(&secrets).unwrap();
            for (name, val) in [
                // bm secret >=32B (validated in service.rs); the auth secret must match
                // the QueueSqlClient signing secret.
                ("bm.secret", "bm-secret-bm-secret-bm-secret-0001"),
                ("auth.secret", "test-secret-test-secret-test-secret-0002"),
            ] {
                let p = secrets.join(name);
                std::fs::write(&p, val).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
                }
            }
            let cfg = serde_json::json!({
                "dbRoot": dir.join("db"),
                "listenTcp": format!("127.0.0.1:{port}"),
                "bookmarkSecretFile": secrets.join("bm.secret"),
                "authSecretFile": secrets.join("auth.secret"),
                "persistence": {
                    "backend": "fs",
                    "fs": { "root": dir.join("objstore") },
                    // Test-only: objstore and dbRoot share one tmp device.
                    "allowSameDeviceDurability": true
                }
            });
            let cfg_path = dir.join("vrt-sql.json");
            std::fs::write(&cfg_path, cfg.to_string()).unwrap();
            Command::new(vrt_sql_bin())
                .args(["serve", "--config"])
                .arg(&cfg_path)
                .process_group(0)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn vrt-sql")
        }
        fn wait_ready(port: u16) -> bool {
            for _ in 0..200 {
                if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            false
        }
        struct Reap(Child);
        impl Drop for Reap {
            fn drop(&mut self) {
                let pid = self.0.id() as i32;
                // Process-group KILL -- same reclaim semantics as d1_smoke's Proc::drop.
                unsafe { libc::kill(-pid, libc::SIGKILL) };
                let _ = self.0.wait();
            }
        }

        let bin = vrt_sql_bin();
        assert!(
            bin.exists(),
            "vrt-sql binary missing at {}; run `cargo build -p vrt-sql` or set VRT_SQL_BIN",
            bin.display()
        );
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().join("sql");
        std::fs::create_dir_all(&dir).unwrap();
        let port = free_port();
        let addr = format!("http://127.0.0.1:{port}");

        let mut sql1 = Reap(spawn_sql(&dir, port));
        assert!(wait_ready(port), "vrt-sql never listened on {port}");

        let client =
            QueueSqlClient::new(&addr, b"test-secret-test-secret-test-secret-0002".to_vec(), Duration::from_secs(5)).unwrap();
        let inv = Arc::new(AlwaysOk {
            calls: AtomicU64::new(0),
        });
        let d = QueueDispatcher::new(
            Arc::new(
                QueueSqlClient::new(&addr, b"test-secret-test-secret-test-secret-0002".to_vec(), Duration::from_secs(5))
                    .unwrap(),
            ),
            inv.clone(),
            cfg("vrt-serve"),
        )
        .unwrap();
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q"}]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(10);

        // 1) dispatcher claim -> dispatch lease CAS persisted to disk (production wire+auth).
        let out = d
            .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
            .await;
        assert_eq!(out.lease_held, 1, "first claim must acquire dispatch lease");

        // 2) SIGTERM (e2e `restart vrt-sql`) -- ticks within the shutdown window may
        //    fail, but after the process recovers the dispatcher must continue and not hang.
        unsafe { libc::kill(-(sql1.0.id() as i32), libc::SIGTERM) };
        let _ = sql1.0.wait();
        let down = d
            .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
            .await;
        assert!(
            down.lease_held == 0,
            "claim during outage must not fake lease"
        );
        let mut sql2 = Reap(spawn_sql(&dir, port));
        assert!(wait_ready(port), "vrt-sql never re-listened on {port}");

        // 3) producer send -- real signed wire (arrives after the restart).
        let send = client
            .call(
                QID,
                "send",
                &json!({"messages":[
                    {"body": "6d31", "contentType": "text"},
                    {"body": "6d32", "contentType": "text"},
                    {"body": "6d33", "contentType": "text"}
                ]}),
            )
            .await;
        assert!(send.is_ok(), "send after restart failed: {send:?}");

        // 4) the full claim->dispatch->settle chain recovers within a bounded time.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut claimed = 0usize;
        loop {
            let out = d
                .tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            claimed += out.claimed;
            if inv.calls.load(Ordering::Relaxed) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no dispatch after restart — e2e failure signature                  (claimed={claimed}, lease_held={})",
                out.lease_held
            );
            tokio::time::sleep(Duration::from_millis(1_100)).await;
        }
        assert_eq!(claimed, 3, "all 3 messages must be claimed");

        // 5) T37b-r6: the release op must go through the real wire route (the r5 implementation
        //    forgot to mount the route -> axum 404 -> client Transport -- the actual error seen in the standalone
        //    e2e). Production client + real process verification: claim's
        //    attempts+1 -> release -> atomically compensated back to 0 -> immediately visible again.
        client
            .call(
                QID,
                "send",
                &json!({"messages":[
                    {"id": "rel-1", "body": "72656c31", "contentType": "text"}
                ]}),
            )
            .await
            .expect("send rel-1");
        let c = client
            .call(
                QID,
                "claim",
                &json!({"holderId":"vrt-serve","holderTtlMs":15000,
                        "max":10,"leaseTtlMs":60000}),
            )
            .await
            .expect("claim rel-1");
        let msgs = c["messages"].as_array().unwrap();
        let rel = msgs
            .iter()
            .find(|m| m["id"] == "rel-1")
            .expect("rel-1 must be claimed");
        assert_eq!(rel["attempts"], 1);
        let lease = c["leaseId"].as_str().unwrap().to_string();
        client
            .release(QID, &lease, &[("rel-1".to_string(), 0u32)])
            .await
            .expect("release over real wire must not be Transport/404");
        let c2 = client
            .call(
                QID,
                "claim",
                &json!({"holderId":"vrt-serve","holderTtlMs":15000,
                        "max":10,"leaseTtlMs":60000}),
            )
            .await
            .expect("re-claim rel-1");
        let rel2 = c2["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["id"] == "rel-1")
            .expect("released msg must be immediately visible");
        assert_eq!(
            rel2["attempts"], 1,
            "release compensates attempts: reclaim lands at 1, not 2"
        );
        let _ = (&mut sql1, &mut sql2);
    }

    // ---------- T37b-r5: platform-side failure release + backoff ----------

    /// Invoker that can toggle platform failure: fail=true -> transport error;
    /// fail=false -> ok+ackAll.
    struct SwitchInvoker {
        calls: AtomicU64,
        fail: AtomicBool,
        last: StdMutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl QueueInvoker for SwitchInvoker {
        async fn dispatch(
            &self,
            _pod: &str,
            req: DispatchRequest,
        ) -> Result<QueueOutcome, DeliveryError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.last.lock().unwrap().push(req.dispatch);
            if self.fail.load(Ordering::Relaxed) {
                Err(DeliveryError::of("transport"))
            } else {
                Ok(outcome(json!({"outcome":"ok","ackAll":true})))
            }
        }
    }

    async fn wait_world(world: &Shared, dur: Duration, f: impl Fn(&FakeWorld) -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + dur;
        while tokio::time::Instant::now() < deadline {
            if f(&world.lock().unwrap()) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    #[tokio::test]
    async fn platform_release_preserves_attempts_then_recovers() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(SwitchInvoker {
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(true),
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q1"}]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        // claim(attempts=1) -> dispatch -> transport -> release(attempts=0, visible after a delay)
        tick_pair(&d, &desired, exp, &assigns).await;
        assert!(
            wait_world(&world, Duration::from_secs(4), |w| {
                w.queues
                    .get(QID)
                    .map(|q| {
                        let m = &q.msgs["m1"];
                        m.attempts == 0 && m.lease_id.is_none() && m.visible_at > now_ms()
                    })
                    .unwrap_or(false)
            })
            .await,
            "message must be released with attempts compensated back to 0"
        );
        // During the failure: nothing lost, nothing dead, attempts stays 0.
        tokio::time::sleep(Duration::from_millis(400)).await;
        {
            let w = world.lock().unwrap();
            let q = w.queues.get(QID).unwrap();
            assert_eq!(q.msgs["m1"].attempts, 0);
            assert!(q.dead.is_empty(), "platform failure must never dead-letter");
        }
        // Recovery: attempts=1 delivery succeeds -> ack.
        inv.fail.store(false, Ordering::Relaxed);
        for _ in 0..40 {
            d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            if world
                .lock()
                .unwrap()
                .queues
                .get(QID)
                .map(|q| q.msgs.is_empty())
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let w = world.lock().unwrap();
        assert!(
            w.queues.get(QID).unwrap().msgs.is_empty(),
            "must ack after recovery"
        );
        assert!(w.queues.get(QID).unwrap().dead.is_empty());
        let calls = inv.last.lock().unwrap();
        let last = calls.last().unwrap();
        assert_eq!(
            last["messages"][0]["attempts"], 1,
            "recovery delivery carries attempts=1"
        );
    }

    #[tokio::test]
    async fn platform_backoff_bounds_dispatch_rate() {
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(SwitchInvoker {
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(true),
            last: StdMutex::new(vec![]),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q1"}]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        // Persistent platform failure for ~4.5s, ticking every second: backoff 1,2,4s... => dispatch count
        // is bounded (no hot loop), and the message always stays in the queue, not dead-lettered.
        for _ in 0..4 {
            d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            tokio::time::sleep(Duration::from_millis(1_050)).await;
        }
        d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
            .await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let calls = inv.calls.load(Ordering::Relaxed);
        assert!(
            calls <= 3,
            "platform backoff must bound dispatch rate, got {calls} calls in ~4.5s"
        );
        assert!(calls >= 1);
        let w = world.lock().unwrap();
        let q = w.queues.get(QID).unwrap();
        assert_eq!(
            q.msgs["m1"].attempts, 0,
            "attempts never consumed by platform failure"
        );
        assert!(q.dead.is_empty());
    }

    #[tokio::test]
    async fn malformed_detail_carries_status_and_body() {
        // Non-JSON 500 -> DeliveryError{malformed, detail carries status code + truncated
        // snippet} (diagnostic requirement: the warn can locate which segment of the wire).
        let app = axum::Router::new().route(
            "/v1/queue/dispatch",
            axum::routing::post(|| async {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "not json at all",
                )
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let inv = HttpQueueInvoker::new(&[("local".into(), addr)], Duration::from_secs(5)).unwrap();
        let err = inv
            .dispatch(
                "local",
                DispatchRequest {
                    tenant_id: "t1".into(),
                    worker_id: "w1".into(),
                    dispatch: json!({"queue":"q","messages":[]}),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.reason, "malformed");
        let detail = err.detail.unwrap();
        assert!(detail.contains("status=500"), "{detail}");
        assert!(detail.contains("not json at all"), "{detail}");
    }

    #[tokio::test]
    async fn typed_host_error_maps_to_platform_reason() {
        // vrt-host typed error body ({delivered:false,error:{code}}) ->
        // bounded platform reason + detail carries the code -- not parsed as
        // QueueOutcome (the all-default parse would swallow the error body).
        let code = "VRT_QDISP_NO_CONSUMER";
        let app = axum::Router::new().route(
            "/v1/queue/dispatch",
            axum::routing::post(move || async move {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    axum::Json(
                        json!({"delivered":false,"error":{"code":code,"message":"no consumer"}}),
                    ),
                )
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let inv = HttpQueueInvoker::new(&[("local".into(), addr)], Duration::from_secs(5)).unwrap();
        let err = inv
            .dispatch(
                "local",
                DispatchRequest {
                    tenant_id: "t1".into(),
                    worker_id: "w1".into(),
                    dispatch: json!({"queue":"q","messages":[]}),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.reason, "no_consumer");
        assert!(err.detail.unwrap().contains(code));
    }

    /// Contract vector: QueueOutcome classification table (consumer_ran vs platform) --
    /// semantic drift between the dispatcher and the facade/vrt-host sides is caught by this table.
    #[test]
    fn outcome_classification_vectors() {
        // consumer ran -> normal settlement
        for v in [
            json!({"outcome":"ok"}),
            json!({"outcome":"ok","ackAll":true}),
            json!({"outcome":"exception"}),
            json!({"outcome":"timeout"}),
            json!({"outcome":"ok","explicitAcks":["a"]}),
            json!({"outcome":"ok","retryMessages":[{"msgId":"a"}]}),
            json!({"outcome":"ok","retryBatch":{"retry":true}}),
        ] {
            assert!(consumer_ran(&outcome(v.clone())), "{v}");
        }
        // consumer did not run -> platform-side release
        for v in [
            json!({"outcome":"error"}),
            json!({"outcome":"bogus-unknown"}),
            json!({}),
        ] {
            assert!(!consumer_ran(&outcome(v.clone())), "{v}");
        }
        // vrt-host typed error code -> bounded reason (closed label set)
        assert_eq!(platform_reason("VRT_QDISP_NO_CONSUMER"), "no_consumer");
        // MIG-11: switching is classed as a platform error (attempts not consumed).
        assert_eq!(platform_reason("VRT_QDISP_SWITCHING"), "switching");
        assert_eq!(platform_reason("VRT_QDISP_WAKE_TIMEOUT"), "wake_timeout");
        assert_eq!(platform_reason("VRT_QDISP_WAKE_FAILED"), "wake_failed");
        assert_eq!(platform_reason("VRT_QDISP_WAKE_CAPACITY"), "wake_capacity");
        assert_eq!(
            platform_reason("VRT_QDISP_UNKNOWN_WORKER"),
            "unknown_worker"
        );
        assert_eq!(platform_reason("VRT_QDISP_ANYTHING_ELSE"), "platform");
        // platform_error_code only recognizes the {delivered:false,error:{code}} shape
        assert_eq!(
            platform_error_code(
                br#"{"delivered":false,"error":{"code":"VRT_QDISP_TIMEOUT","message":"x"}}"#
            )
            .as_deref(),
            Some("VRT_QDISP_TIMEOUT")
        );
        assert!(platform_error_code(br#"{"outcome":"ok","ackAll":true}"#).is_none());
        assert!(platform_error_code(b"not json").is_none());
        assert!(platform_error_code(
            br#"{"delivered":false,"error":{"code":"BAD CODE WITH SPACES"}}"#
        )
        .is_none());
    }

    /// MIG-11: vrt-host typed 503 `{code:"VRT_QDISP_SWITCHING"}` →
    /// `DeliveryError.reason == "switching"` -- platform error semantics,
    /// the dispatcher releases the lease without consuming attempts.
    #[tokio::test]
    async fn typed_switching_error_maps_to_switching_reason() {
        let app = axum::Router::new().route(
            "/v1/queue/dispatch",
            axum::routing::post(move || async move {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    [("retry-after", "1")],
                    axum::Json(json!({
                        "delivered": false,
                        "error": {"code": "VRT_QDISP_SWITCHING", "message": "switching"}
                    })),
                )
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let inv = HttpQueueInvoker::new(&[("local".into(), addr)], Duration::from_secs(5)).unwrap();
        let err = inv
            .dispatch(
                "local",
                DispatchRequest {
                    tenant_id: "t1".into(),
                    worker_id: "w1".into(),
                    dispatch: json!({"queue":"q","messages":[]}),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.reason, "switching");
        assert!(err.detail.unwrap().contains("VRT_QDISP_SWITCHING"));
    }

    /// MIG-11: dispatcher-level proof -- a DeliveryError with `reason:"switching"`
    /// takes the platform release path: the message's attempts are given back to 0,
    /// it does not go to dead-letter, and it acks normally after recovery.
    #[tokio::test]
    async fn switching_release_preserves_attempts() {
        /// Invoker that always returns a switching platform error (back to ok after the flip).
        struct SwitchingInvoker {
            fail: AtomicBool,
        }
        #[async_trait::async_trait]
        impl QueueInvoker for SwitchingInvoker {
            async fn dispatch(
                &self,
                _pod: &str,
                _req: DispatchRequest,
            ) -> Result<QueueOutcome, DeliveryError> {
                if self.fail.load(Ordering::Relaxed) {
                    Err(DeliveryError {
                        reason: "switching",
                        detail: Some("status=503 code=VRT_QDISP_SWITCHING".into()),
                    })
                } else {
                    Ok(outcome(json!({"outcome":"ok","ackAll":true})))
                }
            }
        }
        let (addr, world) = spawn_fake_sql().await;
        {
            let mut w = world.lock().unwrap();
            let q = w.queues.entry(QID.into()).or_default();
            q.msgs.insert(
                "m1".into(),
                FakeMsg {
                    body_hex: "aa".into(),
                    ..Default::default()
                },
            );
        }
        let inv = Arc::new(SwitchingInvoker {
            fail: AtomicBool::new(true),
        });
        let d = dispatcher(&addr, inv.clone(), cfg("h1"));
        let desired = desired_with(json!([{"queueId": QID, "queueName": "q1"}]));
        let assigns = assignments();
        let exp = chrono::Utc::now() + chrono::Duration::minutes(5);
        tick_pair(&d, &desired, exp, &assigns).await;
        assert!(
            wait_world(&world, Duration::from_secs(4), |w| {
                w.queues
                    .get(QID)
                    .map(|q| {
                        let m = &q.msgs["m1"];
                        m.attempts == 0 && m.lease_id.is_none() && m.visible_at > now_ms()
                    })
                    .unwrap_or(false)
            })
            .await,
            "switching must release the claim with attempts preserved (0)"
        );
        {
            let w = world.lock().unwrap();
            assert!(
                w.queues.get(QID).unwrap().dead.is_empty(),
                "switching must never dead-letter"
            );
        }
        inv.fail.store(false, Ordering::Relaxed);
        for _ in 0..40 {
            d.tick(Some(&desired), Some(exp), &assigns, chrono::Utc::now())
                .await;
            if world
                .lock()
                .unwrap()
                .queues
                .get(QID)
                .map(|q| q.msgs.is_empty())
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        assert!(
            world
                .lock()
                .unwrap()
                .queues
                .get(QID)
                .unwrap()
                .msgs
                .is_empty(),
            "message must ack after the switch window closes"
        );
    }
}
