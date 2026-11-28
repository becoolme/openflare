//! Q-PUB: `QUEUE_SEND` cell-op — control-plane-initiated message publish
//! into a queue, dispatched over the generic cell-ops channel for the
//! console "send message" surface and the CLI.
//!
//! Trust boundary (same shape as KV1): the control plane owns the queue
//! registry and resolves `(tenantId, queueId)` from the signed row; the
//! cell NEVER takes queue identity from the payload — it validates the
//! shape only and uses the op's `resource_key` (the physical queue id)
//! as the vrt-sql routing key. Message ids are deterministic per
//! (op, index): a redelivered op re-sends the SAME ids, and vrt-sql
//! dedupe makes the replay idempotent — at-least-once at the channel
//! becomes effectively-once at the queue.
//!
//! Bounds: a publish op carries at most 100 messages and 256 KiB of
//! decoded payload (the producer batch cap is 288 KB; the op channel is
//! the tighter budget). Bodies are hex-encoded on the wire exactly like
//! the DLQ path so binary payloads survive the JSON channel.

use serde::Deserialize;

use crate::execset::{CellOp, CellOpAck};
use crate::queue_dispatch::QueueSqlClient;

pub const QUEUE_SEND_KIND: &str = "QUEUE_SEND";
const MAX_MESSAGES: usize = 100;
const MAX_TOTAL_BYTES: usize = 256 * 1024;

pub fn is_queueop_kind(kind: &str) -> bool {
    kind == QUEUE_SEND_KIND
}

fn failed(op: &CellOp, error: &str) -> CellOpAck {
    CellOpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(crate::execset::truncate_utf16(error, 512).0),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendPayload {
    tenant_id: String,
    queue_id: String,
    messages: Vec<SendMessage>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendMessage {
    /// Hex-encoded body, same wire shape as the DLQ forward path.
    body: String,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    delay_secs: u64,
}

/// Queue publish executor — wraps the signed vrt-sql client shared
/// shape with the dispatcher. Every call returns a TERMINAL ack.
pub struct QueueOpsExecutor {
    sql: QueueSqlClient,
}

impl QueueOpsExecutor {
    pub fn new(sql: QueueSqlClient) -> Self {
        Self { sql }
    }

    pub async fn execute(&self, op: &CellOp) -> CellOpAck {
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
        let Ok(p) = serde_json::from_value::<SendPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        // Scope is control-plane-attested via the op resource_key AND
        // repeated in the payload — they must agree, or the row was
        // minted wrong; fail closed rather than publish cross-queue.
        if op.resource_key.is_empty() || p.queue_id != op.resource_key {
            return failed(op, "scope mismatch");
        }
        if p.tenant_id.is_empty() || p.tenant_id.len() > 128 {
            return failed(op, "malformed tenantId");
        }
        if p.messages.is_empty() || p.messages.len() > MAX_MESSAGES {
            return failed(op, "message count out of bounds");
        }
        let mut total = 0usize;
        let mut msgs = Vec::with_capacity(p.messages.len());
        for (i, m) in p.messages.iter().enumerate() {
            let bytes = match hex::decode(&m.body) {
                Ok(b) => b,
                Err(_) => return failed(op, "message body is not hex"),
            };
            total += bytes.len();
            if total > MAX_TOTAL_BYTES {
                return failed(op, "aggregate payload too large");
            }
            if m.delay_secs > 86_400 {
                return failed(op, "delaySecs out of bounds");
            }
            // Deterministic id: sha256(opId | index) hex — a re-pulled
            // op re-sends identical ids and vrt-sql dedupes the replay.
            let digest = hex::encode(sha2_256(format!("{}:{}", op.id, i).as_bytes()));
            msgs.push(serde_json::json!({
                "id": digest,
                "body": m.body,
                "contentType": m.content_type,
                "delaySecs": m.delay_secs,
                "timestampMs": chrono::Utc::now().timestamp_millis(),
            }));
        }
        let body = serde_json::json!({ "messages": msgs });
        match self.sql.send_for_op(&op.resource_key, &body).await {
            Ok(()) => CellOpAck {
                seq: op.seq,
                status: "DONE",
                result: Some(serde_json::json!({ "sent": p.messages.len() })),
                error: None,
            },
            Err(e) => failed(op, &format!("QUEUE_SEND_FAILED: {e:?}")),
        }
    }
}

/// Minimal SHA-256 hex digest — the executor has no other hash need,
/// and pulling in sha2 for one call is not worth it; implement over
/// the existing workspace dep if present. (See note in Cargo.toml.)
fn sha2_256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(data);
    h.finalize().into()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn op(payload: serde_json::Value) -> CellOp {
        CellOp {
            id: "op-1".into(),
            seq: 1,
            kind: QUEUE_SEND_KIND.into(),
            worker_id: String::new(),
            resource_key: "q-physical".into(),
            payload,
            expires_at: None,
        }
    }

    fn valid_payload() -> serde_json::Value {
        serde_json::json!({
            "tenantId": "team",
            "queueId": "q-physical",
            "messages": [{ "body": hex::encode("hello"), "contentType": "text", "delaySecs": 0 }],
        })
    }

    // Validation paths run before any sql call, so the executor need
    // not reach vrt-sql: construct it against a loopback nothing and
    // exercise the fail-fast shapes.
    fn exec() -> QueueOpsExecutor {
        QueueOpsExecutor::new(
            QueueSqlClient::new("http://127.0.0.1:1", vec![0u8; 32], std::time::Duration::from_millis(50)).unwrap(),
        )
    }

    #[test]
    fn kind_matching() {
        assert!(is_queueop_kind("QUEUE_SEND"));
        assert!(!is_queueop_kind("KV_PUT"));
    }

    #[tokio::test]
    async fn scope_mismatch_fails_closed() {
        let mut p = valid_payload();
        p["queueId"] = serde_json::json!("other-queue");
        let ack = exec().execute(&op(p)).await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("scope mismatch"));
    }

    #[tokio::test]
    async fn non_hex_body_fails_closed() {
        let mut p = valid_payload();
        p["messages"][0]["body"] = serde_json::json!("zz-not-hex");
        let ack = exec().execute(&op(p)).await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("not hex"));
    }

    #[tokio::test]
    async fn empty_and_oversized_message_sets_fail_closed() {
        let mut p = valid_payload();
        p["messages"] = serde_json::json!([]);
        let ack = exec().execute(&op(p)).await;
        assert_eq!(ack.status, "FAILED");
        let mut p = valid_payload();
        p["messages"] = serde_json::json!(vec![p["messages"][0].clone(); 101]);
        let ack = exec().execute(&op(p)).await;
        assert_eq!(ack.status, "FAILED");
    }

    #[tokio::test]
    async fn expired_op_returns_expired() {
        let mut o = op(valid_payload());
        o.expires_at = Some(1);
        let ack = exec().execute(&o).await;
        assert_eq!(ack.status, "EXPIRED");
    }

    #[tokio::test]
    async fn valid_publish_attempts_transport() {
        // Nothing listens on 127.0.0.1:1 — a well-formed op must reach
        // the transport and fail there (never a shape error), proving
        // validation passed end to end.
        let ack = exec().execute(&op(valid_payload())).await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("QUEUE_SEND_FAILED"));
    }
}
