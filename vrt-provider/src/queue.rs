//! Queue producer data plane (T37): facade-queue -> provider -> vrt-sql.
//!
//! The provider's responsibilities mirror D1: authenticate (signed props -> Authorizer) -> from the snapshot
//! map `bindingId` to `queueId` (the Worker cannot choose -- any queue
//! name/ID in the request is never trusted) -> validate per section 8 / miniflare broker semantics
//! the workerd `queue` binding wire format (`POST /message`, `POST /batch`,
//! `GET /metrics`) -> forward to the vrt-sql internal wire.
//!
//! Limit values = measured on the miniflare broker (B:50,82,379) -- **decimal** bytes:
//! single message <=128_000B, batch <=100 messages, batch total <=288_000B, delay <=86_400s.
//! Message bodies are opaque bytes (v8-serialized bodies are not interpreted).

use crate::context::{BindingContext, BindingResource};
use crate::d1::SqlAuthSigner;
use crate::error::ProviderError;
use axum::body::Bytes;
use serde::Deserialize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// miniflare broker limits (section 8, decimal -- B:50 verified).
pub const QUEUE_MSG_MAX_BYTES: usize = 128_000;
pub const QUEUE_BATCH_MAX_MSGS: usize = 100;
pub const QUEUE_BATCH_MAX_BYTES: usize = 288_000;
pub const QUEUE_DELAY_MAX_SECS: u32 = 86_400;
/// Provider ingress body-read cap (batch base64 ~= 384KB + header slack).
pub const QUEUE_MAX_BODY_BYTES: usize = 1024 * 1024;
/// Upstream response cap (RV-R8 #8): queue replies are small JSON envelopes (ack/metrics),
/// 1MiB is far above any legal response; exceeding it fails closed.
pub const QUEUE_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Header fields of the workerd `queue` binding.
pub const HDR_MSG_FMT: &str = "x-msg-fmt";
pub const HDR_MSG_DELAY: &str = "x-msg-delay-secs";
pub const HDR_BATCH_COUNT: &str = "cf-queue-batch-count";
pub const HDR_BATCH_LARGEST: &str = "cf-queue-largest-msg";
pub const HDR_BATCH_BYTES: &str = "cf-queue-batch-bytes";

/// vrt-sql `/queues/{id}/send` request message (body hex).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SendMsg<'a> {
    /// T37r: ids are always server-generated (uuid v4 32-hex) -- the regular workerd path
    /// does not send an id (the miniflare broker `/message` only sends {contentType,
    /// delaySecs,body}; `/batch` id/timestamp are optional fields only for DLQ
    /// forwarding). An id/timestamp supplied by the client gets 400 at ingress; after the provider generates
    /// the id, transport retries are naturally idempotent (vrt-sql dedups duplicate ids).
    id: String,
    body: String,
    content_type: &'a str,
    delay_secs: u32,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct SendReq<'a> {
    messages: Vec<SendMsg<'a>>,
}

/// `/batch` inbound message (miniflare `QueueIncomingMessageSchema`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchMsg {
    content_type: String,
    #[serde(default)]
    delay_secs: Option<u32>,
    body: String, // base64
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    timestamp: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct BatchReq {
    messages: Vec<BatchMsg>,
}

/// Error surface of a vrt-sql queue op response (typed code).
#[derive(Debug, Deserialize)]
pub struct QueueWireError {
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub retryable: Option<bool>,
}

/// The metrics shape shared by vrt-sql `/queues/{id}/metrics` and the send response.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireMetrics {
    #[serde(default)]
    pub backlog_count: i64,
    #[serde(default)]
    pub backlog_bytes: i64,
    #[serde(default)]
    pub oldest_message_timestamp_ms: Option<i64>,
}

/// broker `metrics` response shape: `oldestMessageTimestamp` is an epoch-ms
/// **number** -- measured from miniflare `getMetricsResponseObject()`
/// `this.#messages[0]?.timestamp.getTime() ?? 0`
/// (broker.worker.js:307-312), empty queue = **0**. workerd parses this field as type `date`:
/// null throws `TypeError ... not of type 'date'`,
/// and a string is likewise invalid (measured on real workerd in T37r2). Never emit null/a string.
pub fn broker_metrics_json(m: &WireMetrics) -> serde_json::Value {
    serde_json::json!({
        "backlogCount": m.backlog_count,
        "backlogBytes": m.backlog_bytes,
        "oldestMessageTimestamp": m.oldest_message_timestamp_ms.unwrap_or(0),
    })
}

const CONTENT_TYPES: [&str; 4] = ["text", "json", "bytes", "v8"];

/// Ingress-side `X-Msg-Fmt` parsing (miniflare `validateContentType`):
/// default = `v8`; invalid -> 400.
fn parse_msg_fmt(headers: &axum::http::HeaderMap) -> Result<&'static str, String> {
    let fmt = match headers.get(HDR_MSG_FMT).and_then(|v| v.to_str().ok()) {
        Some(f) => f,
        None => return Ok("v8"),
    };
    CONTENT_TYPES
        .iter()
        .copied()
        .find(|t| *t == fmt)
        .ok_or_else(|| {
            format!(
                "message content type {fmt} is invalid; if specified, must be one of 'text', 'json', 'bytes', or 'v8'"
            )
        })
}

/// `X-Msg-Delay-Secs` (miniflare `validateMessageDelay`): int 0..=86400.
fn parse_delay(headers: &axum::http::HeaderMap) -> Result<Option<u32>, String> {
    let v = match headers.get(HDR_MSG_DELAY).and_then(|v| v.to_str().ok()) {
        Some(v) => v,
        None => return Ok(None),
    };
    let d: u32 = v
        .parse()
        .map_err(|_| format!("message delay {v} is invalid"))?;
    if d > QUEUE_DELAY_MAX_SECS {
        return Err(format!("message delay {v} is invalid"));
    }
    Ok(Some(d))
}

/// `POST /message`: raw body + headers -> vrt-sql send request.
/// Returns (send_json, rejection reason for metrics).
pub fn producer_message(
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<serde_json::Value, (u16, String)> {
    // miniflare: Content-Length is checked first (over the limit -> 413).
    if let Some(cl) = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > QUEUE_MSG_MAX_BYTES {
            return Err((
                413,
                format!("message length of {cl} bytes exceeds limit of {QUEUE_MSG_MAX_BYTES}"),
            ));
        }
    }
    if body.len() > QUEUE_MSG_MAX_BYTES {
        return Err((
            413,
            format!(
                "message length of {} bytes exceeds limit of {QUEUE_MSG_MAX_BYTES}",
                body.len()
            ),
        ));
    }
    let content_type = parse_msg_fmt(headers).map_err(|m| (400u16, m))?;
    let delay = parse_delay(headers).map_err(|m| (400u16, m))?.unwrap_or(0);
    Ok(serde_json::to_value(SendReq {
        messages: vec![SendMsg {
            id: uuid::Uuid::new_v4().simple().to_string(),
            body: hex::encode(body),
            content_type,
            delay_secs: delay,
        }],
    })
    .unwrap())
}

/// `POST /batch`: batch headers (validated only if present, same convention as miniflare) + JSON body.
pub fn producer_batch(
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<serde_json::Value, (u16, String)> {
    if let Some(count) = headers.get(HDR_BATCH_COUNT).and_then(|v| v.to_str().ok()) {
        if count.parse::<usize>().unwrap_or(usize::MAX) > QUEUE_BATCH_MAX_MSGS {
            return Err((
                413,
                format!("batch message count of {count} exceeds limit of {QUEUE_BATCH_MAX_MSGS}"),
            ));
        }
    }
    if let Some(largest) = headers.get(HDR_BATCH_LARGEST).and_then(|v| v.to_str().ok()) {
        if largest.parse::<usize>().unwrap_or(usize::MAX) > QUEUE_MSG_MAX_BYTES {
            return Err((
                413,
                format!(
                    "message in batch has length {largest} bytes which exceeds single message size limit of {QUEUE_MSG_MAX_BYTES}"
                ),
            ));
        }
    }
    if let Some(size) = headers.get(HDR_BATCH_BYTES).and_then(|v| v.to_str().ok()) {
        if size.parse::<usize>().unwrap_or(usize::MAX) > QUEUE_BATCH_MAX_BYTES {
            return Err((
                413,
                format!("batch size of {size} bytes exceeds limit of {QUEUE_BATCH_MAX_BYTES}"),
            ));
        }
    }
    let delay = parse_delay(headers).map_err(|m| (400u16, m))?.unwrap_or(0);
    let req: BatchReq =
        serde_json::from_slice(body).map_err(|e| (400, format!("batch body: {e}")))?;
    if req.messages.len() > QUEUE_BATCH_MAX_MSGS {
        return Err((
            413,
            format!(
                "batch message count of {} exceeds limit of {QUEUE_BATCH_MAX_MSGS}",
                req.messages.len()
            ),
        ));
    }
    let mut total = 0usize;
    let mut msgs = Vec::with_capacity(req.messages.len());
    for m in &req.messages {
        let raw =
            base64_decode(&m.body).map_err(|_| (400, "message body must be base64".into()))?;
        if raw.len() > QUEUE_MSG_MAX_BYTES {
            return Err((
                413,
                format!(
                    "message in batch has length {} bytes which exceeds single message size limit of {QUEUE_MSG_MAX_BYTES}",
                    raw.len()
                ),
            ));
        }
        total = total.saturating_add(raw.len());
        if !CONTENT_TYPES.contains(&m.content_type.as_str()) {
            return Err((
                400,
                format!(
                    "message content type {} is invalid; if specified, must be one of 'text', 'json', 'bytes', or 'v8'",
                    m.content_type
                ),
            ));
        }
        let d = m.delay_secs.unwrap_or(delay);
        if d > QUEUE_DELAY_MAX_SECS {
            return Err((400, format!("message delay {d} is invalid")));
        }
        // T37r: the regular workerd send/sendBatch does not send id/timestamp (only
        // optional for DLQ forwarding) -- when the client supplies either it is rejected rather than passed through: ids are all
        // generated by the server (retry idempotence + observable dedup), and the timestamp is untrusted
        // (scheduling/metrics use only the server clock).
        if m.id.is_some() {
            return Err((400, "client-provided message id is not accepted".into()));
        }
        if m.timestamp.is_some() {
            return Err((
                400,
                "client-provided message timestamp is not accepted".into(),
            ));
        }
        msgs.push(SendMsg {
            id: uuid::Uuid::new_v4().simple().to_string(),
            body: hex::encode(&raw),
            content_type: &m.content_type,
            delay_secs: d,
        });
    }
    if total > QUEUE_BATCH_MAX_BYTES {
        return Err((
            413,
            format!("batch size of {total} bytes exceeds limit of {QUEUE_BATCH_MAX_BYTES}"),
        ));
    }
    Ok(serde_json::to_value(SendReq { messages: msgs }).unwrap())
}

fn base64_decode(s: &str) -> Result<Vec<u8>, ()> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| ())
}

/// vrt-sql downstream client (queue-specific -- does not borrow D1 types; paths/no session headers).
pub struct QueueClient {
    http: reqwest::Client,
    addr: String,
    signer: SqlAuthSigner,
    timeout: Duration,
    seq: AtomicU64,
}

impl std::fmt::Debug for QueueClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueClient")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl QueueClient {
    pub fn new(addr: String, secret: Vec<u8>, timeout: Duration) -> Result<Self, ProviderError> {
        if addr.is_empty()
            || addr.len() > 256
            || !(addr.starts_with("http://") || addr.starts_with("https://"))
            || addr.ends_with('/')
        {
            return Err(ProviderError::Protocol);
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .build()
            .map_err(|_| ProviderError::Protocol)?;
        Ok(Self {
            http,
            addr,
            signer: SqlAuthSigner::new(secret),
            timeout,
            seq: AtomicU64::new(0),
        })
    }

    fn next_seq(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
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

    /// Forward one queue op (`/queues/{id}/{op}`, GET for metrics).
    /// Returns (status, body); **never retries**.
    pub async fn forward(
        &self,
        ctx: &BindingContext,
        op: &str,
        method: &str,
        body: Bytes,
    ) -> Result<(u16, Bytes), ProviderError> {
        let queue_id = match ctx.resource() {
            BindingResource::Queue { id } => id.clone(),
            _ => {
                return Err(ProviderError::Forbidden(
                    crate::error::ForbiddenReason::BindingKindMismatch,
                ))
            }
        };
        // Same convention as d1.rs: allowlist-validate the id before rendering (defense in depth).
        if queue_id.len() != 26
            || !queue_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(ProviderError::Forbidden(
                crate::error::ForbiddenReason::ResourceMismatch,
            ));
        }
        let path_query = format!("/queues/{queue_id}/{op}");
        let url = format!("{}{}", self.addr, path_query);
        let seq = self.next_seq();
        let auth = self.signer.sign(method, &path_query, &body, 1, seq);
        let req = match method {
            "GET" => self.http.get(&url),
            _ => self.http.post(&url),
        };
        let req = req
            .timeout(self.timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(crate::d1::SQL_EPOCH_HEADER, "1")
            .header(crate::d1::SQL_SEQ_HEADER, seq.to_string())
            .header(crate::d1::SQL_AUTH_HEADER, auth)
            .body(body);
        let resp = req.send().await.map_err(|e| {
            tracing::warn!(
                worker = ctx.worker_id(),
                binding = ctx.binding_name(),
                error = %e,
                "vrt-sql queue backend call failed (no retry)"
            );
            ProviderError::ResourceUnavailable
        })?;
        let status = resp.status().as_u16();
        // RV-R8 #8: chunked capped read (same convention as d1).
        let bytes = crate::upstream_body::read_body_capped(resp, QUEUE_MAX_RESPONSE_BYTES)
            .await
            .map_err(|e| {
                match e {
                    crate::upstream_body::UpstreamBodyErr::TooLarge => tracing::warn!(
                        worker = ctx.worker_id(),
                        binding = ctx.binding_name(),
                        cap = QUEUE_MAX_RESPONSE_BYTES,
                        "queue upstream response exceeded byte cap"
                    ),
                    crate::upstream_body::UpstreamBodyErr::Transport => {}
                }
                ProviderError::ResourceUnavailable
            })?;
        Ok((status, bytes))
    }
}
