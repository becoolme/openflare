//! K2 produce data plane: facade-k2 -> provider -> vrt-sql K2Backend.
//!
//! Same responsibilities as D1/queue: authenticate (signed props ->
//! Authorizer) -> map `bindingId` to `streamId` from the signed desired
//! state (the Worker cannot choose a stream) -> validate the CF K2
//! `send()` contract (record <=1MB, batch <=5MB, headers shape) ->
//! forward to the vrt-sql internal wire (`/streams/{id}/produce`).
//!
//! The inbound wire is JSON `{records:[{content:<base64>,headers?}]}`
//! (the shim base64s binary content); the vrt-sql wire hex-encodes.
//! The response surface is CF K2's non-throwing `{success:true}` /
//! `{error:{code,message,retryable}}` envelope — produced by the
//! backend and relayed verbatim; ingress-side rejections use the same
//! shape so the shim never has to distinguish provider layers.

use crate::context::{BindingContext, BindingResource};
use crate::d1::SqlAuthSigner;
use crate::error::ProviderError;
use axum::body::Bytes;
use serde::Deserialize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// CF K2 public limits (docs): record <=1MB, batch total <=5MB.
pub const K2_RECORD_MAX_BYTES: usize = 1_000_000;
pub const K2_BATCH_MAX_BYTES: usize = 5_000_000;
/// Header bounds mirrored from vrt-sql (K2_HEADER_*).
pub const K2_HEADERS_MAX: usize = 64;
pub const K2_HEADER_KV_MAX_BYTES: usize = 1024;
/// Provider ingress body-read cap: JSON base64 inflates ~4/3
/// (5MB -> ~6.7MB) plus headers/field slack.
pub const K2_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
/// Upstream response cap: produce replies are small JSON envelopes.
pub const K2_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Inbound `/send` record (`content` is standard base64).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendRecord {
    content: String,
    #[serde(default)]
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SendReq {
    records: Vec<SendRecord>,
}

/// vrt-sql produce record (`content` hex — same wire as queue).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct WireRecord<'a> {
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<&'a serde_json::Map<String, serde_json::Value>>,
}

/// CF K2 error code table (docs): stable `code` strings the shim maps
/// onto `send()`'s `{error:{code,retryable}}` result. Backend codes are
/// relayed verbatim; these cover provider-side rejections.
pub fn error_json(code: &str, message: &str, retryable: bool) -> serde_json::Value {
    serde_json::json!({
        "success": false,
        "error": {"code": code, "message": message, "retryable": retryable},
    })
}

/// Validate one inbound `/send` body and build the vrt-sql produce
/// request. Returns the wire JSON or `(status, error_json)` — every
/// rejection is already in the CF K2 envelope shape.
pub fn validate_send_body(body: &[u8]) -> Result<serde_json::Value, (u16, serde_json::Value)> {
    let req: SendReq = serde_json::from_slice(body).map_err(|_| {
        (
            400,
            error_json("INVALID_BATCH", "send body must be {records:[…]} JSON", false),
        )
    })?;
    if req.records.is_empty() {
        return Err((
            400,
            error_json("INVALID_BATCH", "send batch must contain at least one record", false),
        ));
    }
    let mut total = 0usize;
    let mut records = Vec::with_capacity(req.records.len());
    for rec in &req.records {
        let raw = base64_decode(&rec.content).map_err(|_| {
            (
                400,
                error_json("INVALID_RECORD", "record content must be base64", false),
            )
        })?;
        if raw.len() > K2_RECORD_MAX_BYTES {
            return Err((
                413,
                error_json(
                    "RECORD_TOO_LARGE",
                    "record content exceeds the 1MB limit",
                    false,
                ),
            ));
        }
        total = total.saturating_add(raw.len());
        if let Some(headers) = &rec.headers {
            if headers.len() > K2_HEADERS_MAX {
                return Err((
                    400,
                    error_json("INVALID_HEADERS", "headers map exceeds the 64-entry limit", false),
                ));
            }
            for (k, v) in headers {
                if k.len() > K2_HEADER_KV_MAX_BYTES {
                    return Err((
                        400,
                        error_json("INVALID_HEADERS", "header key exceeds the 1KB limit", false),
                    ));
                }
                let Some(s) = v.as_str() else {
                    return Err((
                        400,
                        error_json("INVALID_HEADERS", "header values must be strings", false),
                    ));
                };
                if s.len() > K2_HEADER_KV_MAX_BYTES {
                    return Err((
                        400,
                        error_json("INVALID_HEADERS", "header value exceeds the 1KB limit", false),
                    ));
                }
            }
        }
        records.push(WireRecord {
            content: hex::encode(&raw),
            headers: rec.headers.as_ref(),
        });
    }
    if total > K2_BATCH_MAX_BYTES {
        return Err((
            413,
            error_json(
                "BATCH_TOO_LARGE",
                "record batch exceeds the 5MB limit",
                false,
            ),
        ));
    }
    Ok(serde_json::json!({ "records": records }))
}

fn base64_decode(s: &str) -> Result<Vec<u8>, ()> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| ())
}

/// vrt-sql downstream client (k2-specific — does not borrow D1/queue
/// types; `/streams/{id}/…` paths).
pub struct K2Client {
    http: reqwest::Client,
    addr: String,
    signer: SqlAuthSigner,
    timeout: Duration,
    seq: AtomicU64,
}

impl std::fmt::Debug for K2Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K2Client")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl K2Client {
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

    /// Forward one k2 op (`/streams/{id}/{op}`) — POST only, **never
    /// retries** (produce is not idempotent at the transport layer;
    /// the shim decides retryability from the typed `retryable` flag).
    pub async fn forward(
        &self,
        ctx: &BindingContext,
        op: &str,
        body: Bytes,
    ) -> Result<(u16, Bytes), ProviderError> {
        let stream_id = match ctx.resource() {
            BindingResource::K2Stream { id } => id.clone(),
            _ => {
                return Err(ProviderError::Forbidden(
                    crate::error::ForbiddenReason::BindingKindMismatch,
                ))
            }
        };
        // Allowlist-validate the id before rendering (defense in depth;
        // the desired validator already enforces `^[0-9a-f]{32}$`).
        if stream_id.len() != 32
            || !stream_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ProviderError::Forbidden(
                crate::error::ForbiddenReason::ResourceMismatch,
            ));
        }
        let path_query = format!("/streams/{stream_id}/{op}");
        let url = format!("{}{}", self.addr, path_query);
        let seq = self.next_seq();
        let auth = self.signer.sign("POST", &path_query, &body, 1, seq);
        let req = self
            .http
            .post(&url)
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
                "vrt-sql k2 backend call failed (no retry)"
            );
            ProviderError::ResourceUnavailable
        })?;
        let status = resp.status().as_u16();
        let bytes = crate::upstream_body::read_body_capped(resp, K2_MAX_RESPONSE_BYTES)
            .await
            .map_err(|e| {
                if let crate::upstream_body::UpstreamBodyErr::TooLarge = e {
                    tracing::warn!(
                        worker = ctx.worker_id(),
                        binding = ctx.binding_name(),
                        cap = K2_MAX_RESPONSE_BYTES,
                        "k2 upstream response exceeded byte cap"
                    );
                }
                ProviderError::ResourceUnavailable
            })?;
        Ok((status, bytes))
    }
}
