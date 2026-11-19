//! KV1: KV data cell-ops — `KV_GET` / `KV_PUT` / `KV_DELETE` /
//! `KV_LIST` / `KV_BULK_PUT` / `KV_BULK_DELETE`, dispatched over the
//! generic cell-ops channel for the namespace data API and the
//! `vibeploy workers kv` CLI.
//!
//! Trust boundary (same shape as STORAGE_PURGE): the control plane is
//! the only writer of op rows and resolves `(tenantId, namespaceId)`
//! from the signed registry row — the cell NEVER takes a prefix from
//! the payload; it only validates id SHAPES (`purge::valid_*`) and
//! derives the physical layout inside the backend (segment-encoded).
//! A namespace need not be bound to any worker — there is no desired
//! state binding to cross-check, so identity is control-plane-attested
//! plus shape-validated, never client-influenced.
//!
//! The tighter op-channel limits (single value ≤1 MiB, bulk ≤1000
//! entries / ≤8 MiB total) are enforced HERE too — the provider's own
//! ceilings (25 MiB values) are binding-runtime budgets, not channel
//! budgets. Results are whitelisted and bounded before ACK: an entry
//! stored out-of-band beyond the channel budget fails the op honestly
//! (`RESULT_TOO_LARGE`) rather than being truncated or faked.

use serde::Deserialize;
use std::sync::Arc;
use vrt_provider::error::ProviderError;
use vrt_provider::kv::{frame, KvDataBackend, KvPutOptions};
use vrt_provider::purge;

use crate::execset::{CellOp, CellOpAck};

/// Op-channel single-value budget (server `vrtLimits.ts` mirrors this).
const KV_OP_MAX_VALUE_BYTES: usize = 1024 * 1024;
/// Bulk op budgets — entry count and DECODED aggregate bytes.
const KV_OP_BULK_MAX_ENTRIES: usize = 1000;
const KV_OP_BULK_MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
/// Serialized ack-result budget — comfortably under the control
/// plane's per-item cap (2 MiB), so a ≤1 MiB value (≈1.4 MB base64)
/// plus metadata always fits.
const KV_OP_RESULT_MAX_BYTES: usize = 1900 * 1024;
/// `ZRANGEBYLEX`-style list page cap (provider KV_MAX_LIST_LIMIT).
const KV_OP_LIST_MAX_LIMIT: u16 = 1000;
/// Fixed unix-second granularity required by `expiration`.
const MIN_EXPIRATION_SECS: u64 = 60;

pub const KV_OP_KINDS: &[&str] = &[
    "KV_GET",
    "KV_PUT",
    "KV_DELETE",
    "KV_LIST",
    "KV_BULK_PUT",
    "KV_BULK_DELETE",
];

pub fn is_kv_kind(kind: &str) -> bool {
    KV_OP_KINDS.contains(&kind)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn failed(op: &CellOp, error: &str) -> CellOpAck {
    CellOpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(crate::execset::truncate_utf16(error, 512).0),
    }
}

fn provider_failed(op: &CellOp, e: ProviderError) -> CellOpAck {
    // Codes are the fixed `KV_*` wire vocabulary — never internals.
    failed(op, e.code())
}

/// Build a DONE ack; the serialized result must fit the ack budget or
/// the op FAILS honestly — never a truncated/corrupt value.
fn done(op: &CellOp, result: serde_json::Value) -> CellOpAck {
    match serde_json::to_vec(&result) {
        Ok(bytes) if bytes.len() <= KV_OP_RESULT_MAX_BYTES => CellOpAck {
            seq: op.seq,
            status: "DONE",
            result: Some(result),
            error: None,
        },
        _ => failed(op, "RESULT_TOO_LARGE"),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvScope {
    tenant_id: String,
    namespace_id: String,
}

fn valid_scope(scope: &KvScope) -> bool {
    purge::valid_purge_tenant(&scope.tenant_id) && purge::valid_kv_purge_id(&scope.namespace_id)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvGetPayload {
    #[serde(flatten)]
    scope: KvScope,
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvPutPayload {
    #[serde(flatten)]
    scope: KvScope,
    key: String,
    value_base64: String,
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvDeletePayload {
    #[serde(flatten)]
    scope: KvScope,
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvListPayload {
    #[serde(flatten)]
    scope: KvScope,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvBulkEntry {
    key: String,
    value_base64: String,
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvBulkPutPayload {
    #[serde(flatten)]
    scope: KvScope,
    entries: Vec<KvBulkEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvBulkDeletePayload {
    #[serde(flatten)]
    scope: KvScope,
    keys: Vec<String>,
}

fn decode_value(op: &CellOp, value_base64: &str) -> Result<Vec<u8>, CellOpAck> {
    use base64::Engine;
    // Reject early on encoded length before decoding megabytes of
    // garbage (1 MiB raw ≈ 1.4 MB base64).
    if value_base64.len() > KV_OP_MAX_VALUE_BYTES * 2 {
        return Err(failed(op, "KV_VALUE_TOO_LARGE"));
    }
    let value = base64::engine::general_purpose::STANDARD
        .decode(value_base64)
        .map_err(|_| failed(op, "valueBase64 malformed"))?;
    if value.len() > KV_OP_MAX_VALUE_BYTES {
        return Err(failed(op, "KV_VALUE_TOO_LARGE"));
    }
    Ok(value)
}

/// `expiration`/`expiration_ttl`/`metadata` → provider options, the
/// same canonicalization the binding-runtime path applies.
fn put_options(
    op: &CellOp,
    key: &str,
    expiration: Option<u64>,
    expiration_ttl: Option<u64>,
    metadata: Option<serde_json::Value>,
) -> Result<KvPutOptions, CellOpAck> {
    let header = frame::FramePutHeader {
        key: key.to_string(),
        expiration,
        expiration_ttl,
        metadata: metadata.clone(),
        metadata_present: metadata.is_some(),
    };
    if let (Some(ttl), Some(at)) = (expiration_ttl, expiration) {
        if ttl < MIN_EXPIRATION_SECS && at == 0 {
            return Err(failed(op, "KV_INVALID_OPTIONS"));
        }
    }
    frame::prepare_put_options(now_ms(), &header).map_err(|e| provider_failed(op, e))
}

/// Namespace-scoped KV data executor — wraps the backend shared with
/// STORAGE_PURGE. Every call returns a TERMINAL ack; nothing panics.
pub struct KvOpsExecutor {
    backend: Arc<dyn KvDataBackend>,
}

impl KvOpsExecutor {
    pub fn new(backend: Arc<dyn KvDataBackend>) -> Self {
        Self { backend }
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
        match op.kind.as_str() {
            "KV_GET" => self.get(op).await,
            "KV_PUT" => self.put(op).await,
            "KV_DELETE" => self.delete(op).await,
            "KV_LIST" => self.list(op).await,
            "KV_BULK_PUT" => self.bulk_put(op).await,
            "KV_BULK_DELETE" => self.bulk_delete(op).await,
            _ => failed(op, "unknown kv op kind"),
        }
    }

    async fn get(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<KvGetPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid namespace scope");
        }
        let key = match frame::validate_key(&p.key) {
            Ok(k) => k,
            Err(e) => return provider_failed(op, e),
        };
        match self
            .backend
            .data_get(&p.scope.tenant_id, &p.scope.namespace_id, &key)
            .await
        {
            Ok(None) => done(op, serde_json::json!({ "found": false })),
            Ok(Some(entry)) => {
                use base64::Engine;
                let mut out = serde_json::json!({
                    "found": true,
                    "valueBase64": base64::engine::general_purpose::STANDARD.encode(&entry.value),
                });
                if let Some(meta) = &entry.metadata_json {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(meta) {
                        out["metadata"] = v;
                    }
                }
                if let Some(ms) = entry.expires_at_ms {
                    out["expiresAtMs"] = serde_json::json!(ms);
                }
                done(op, out)
            }
            Err(e) => provider_failed(op, e),
        }
    }

    async fn put(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<KvPutPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid namespace scope");
        }
        let key = match frame::validate_key(&p.key) {
            Ok(k) => k,
            Err(e) => return provider_failed(op, e),
        };
        let value = match decode_value(op, &p.value_base64) {
            Ok(v) => v,
            Err(a) => return a,
        };
        let options = match put_options(op, &p.key, p.expiration, p.expiration_ttl, p.metadata) {
            Ok(o) => o,
            Err(a) => return a,
        };
        match self
            .backend
            .data_put(&p.scope.tenant_id, &p.scope.namespace_id, &key, &value, &options)
            .await
        {
            Ok(()) => done(op, serde_json::json!({ "stored": true })),
            Err(e) => provider_failed(op, e),
        }
    }

    async fn delete(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<KvDeletePayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid namespace scope");
        }
        let key = match frame::validate_key(&p.key) {
            Ok(k) => k,
            Err(e) => return provider_failed(op, e),
        };
        match self
            .backend
            .data_delete(&p.scope.tenant_id, &p.scope.namespace_id, &key)
            .await
        {
            Ok(()) => done(op, serde_json::json!({ "deleted": true })),
            Err(e) => provider_failed(op, e),
        }
    }

    async fn list(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<KvListPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid namespace scope");
        }
        let prefix = p.prefix.unwrap_or_default();
        if prefix.len() > 512 {
            return failed(op, "KV_KEY_TOO_LARGE");
        }
        let limit = p.limit.unwrap_or(1000);
        if limit == 0 || limit > KV_OP_LIST_MAX_LIMIT {
            return failed(op, "KV_INVALID_OPTIONS");
        }
        let after = match p.cursor.as_deref() {
            None | Some("") => None,
            Some(c) => {
                if c.len() > 512 {
                    return failed(op, "KV_CURSOR_INVALID");
                }
                Some(c.as_bytes().to_vec())
            }
        };
        match self
            .backend
            .data_list(
                &p.scope.tenant_id,
                &p.scope.namespace_id,
                &prefix,
                after.as_deref(),
                limit,
            )
            .await
        {
            Ok(page) => {
                let mut keys = Vec::with_capacity(page.rows.len());
                let mut last: Option<String> = None;
                for row in page.rows {
                    let name = match String::from_utf8(row.key) {
                        Ok(s) => s,
                        Err(_) => return failed(op, "KV_CURSOR_INVALID"),
                    };
                    let mut k = serde_json::json!({ "name": name });
                    if let Some(meta) = &row.metadata_json {
                        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(meta) {
                            k["metadata"] = v;
                        }
                    }
                    if let Some(ms) = row.expires_at_ms {
                        k["expiration"] = serde_json::json!(ms / 1000);
                    }
                    last = Some(name);
                    keys.push(k);
                }
                done(
                    op,
                    serde_json::json!({
                        "keys": keys,
                        "complete": page.complete,
                        "cursor": if page.complete { serde_json::Value::Null } else { last.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null) },
                    }),
                )
            }
            Err(e) => provider_failed(op, e),
        }
    }

    async fn bulk_put(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<KvBulkPutPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid namespace scope");
        }
        if p.entries.is_empty() || p.entries.len() > KV_OP_BULK_MAX_ENTRIES {
            return failed(op, "KV_INVALID_OPTIONS");
        }
        // Validate + decode EVERY entry before writing any — a partial
        // bulk write on malformed input would strand half a batch.
        let mut total = 0usize;
        let mut staged: Vec<(Vec<u8>, Vec<u8>, KvPutOptions)> = Vec::with_capacity(p.entries.len());
        for entry in &p.entries {
            let key = match frame::validate_key(&entry.key) {
                Ok(k) => k,
                Err(e) => return provider_failed(op, e),
            };
            let value = match decode_value(op, &entry.value_base64) {
                Ok(v) => v,
                Err(a) => return a,
            };
            total = match total.checked_add(value.len()) {
                Some(t) if t <= KV_OP_BULK_MAX_TOTAL_BYTES => t,
                _ => return failed(op, "KV_VALUE_TOO_LARGE"),
            };
            let options = match put_options(
                op,
                &entry.key,
                entry.expiration,
                entry.expiration_ttl,
                entry.metadata.clone(),
            ) {
                Ok(o) => o,
                Err(a) => return a,
            };
            staged.push((key, value, options));
        }
        let mut stored = 0u64;
        for (key, value, options) in staged {
            match self
                .backend
                .data_put(
                    &p.scope.tenant_id,
                    &p.scope.namespace_id,
                    &key,
                    &value,
                    &options,
                )
                .await
            {
                Ok(()) => stored += 1,
                Err(e) => return provider_failed(op, e),
            }
        }
        done(op, serde_json::json!({ "stored": stored }))
    }

    async fn bulk_delete(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<KvBulkDeletePayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid namespace scope");
        }
        if p.keys.is_empty() || p.keys.len() > KV_OP_BULK_MAX_ENTRIES {
            return failed(op, "KV_INVALID_OPTIONS");
        }
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(p.keys.len());
        for k in &p.keys {
            match frame::validate_key(k) {
                Ok(kb) => keys.push(kb),
                Err(e) => return provider_failed(op, e),
            }
        }
        let mut deleted = 0u64;
        for key in keys {
            match self
                .backend
                .data_delete(&p.scope.tenant_id, &p.scope.namespace_id, &key)
                .await
            {
                Ok(()) => deleted += 1,
                Err(e) => return provider_failed(op, e),
            }
        }
        done(op, serde_json::json!({ "deleted": deleted }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    const TENANT: &str = "team_abc123";
    const NS: &str = "vrtkv_testns123";

    fn exec() -> KvOpsExecutor {
        KvOpsExecutor::new(Arc::new(vrt_provider::kv::memory::MemoryKvProvider::new()))
    }

    fn op(kind: &str, payload: serde_json::Value) -> CellOp {
        CellOp {
            id: format!("op-{}", kind),
            seq: 7,
            resource_key: NS.into(),
            kind: kind.into(),
            worker_id: String::new(),
            payload,
            expires_at: None,
        }
    }

    fn b64(v: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(v)
    }

    fn put_op(key: &str, value: &[u8]) -> CellOp {
        op(
            "KV_PUT",
            serde_json::json!({
                "tenantId": TENANT, "namespaceId": NS,
                "key": key, "valueBase64": b64(value),
            }),
        )
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let e = exec();
        let a = e.execute(&put_op("k1", b"hello")).await;
        assert_eq!(a.status, "DONE");
        let a = e
            .execute(&op(
                "KV_GET",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"k1"}),
            ))
            .await;
        assert_eq!(a.status, "DONE");
        assert_eq!(a.result.unwrap()["valueBase64"], b64(b"hello"));
        // Missing key reads as found:false, never an error.
        let a = e
            .execute(&op(
                "KV_GET",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"nope"}),
            ))
            .await;
        assert_eq!(a.result.unwrap()["found"], false);
        let a = e
            .execute(&op(
                "KV_DELETE",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"k1"}),
            ))
            .await;
        assert_eq!(a.status, "DONE");
        let a = e
            .execute(&op(
                "KV_GET",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"k1"}),
            ))
            .await;
        assert_eq!(a.result.unwrap()["found"], false);
    }

    #[tokio::test]
    async fn rejects_malformed_and_foreign_scope() {
        let e = exec();
        for bad in [
            serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"k","extra":1}),
            serde_json::json!({"tenantId":"../escape","namespaceId":NS,"key":"k"}),
            serde_json::json!({"tenantId":TENANT,"namespaceId":"other-ns","key":"k"}),
            serde_json::json!({"tenantId":TENANT,"namespaceId":"vrtkv_","key":"k"}),
            serde_json::json!({"namespaceId":NS,"key":"k"}),
        ] {
            let a = e.execute(&op("KV_GET", bad)).await;
            assert_eq!(a.status, "FAILED", "{:?}", a);
        }
    }

    #[tokio::test]
    async fn rejects_bad_keys_and_oversize() {
        let e = exec();
        for key in ["", ".", "..", &"k".repeat(513)] {
            let a = e.execute(&put_op(key, b"v")).await;
            assert_eq!(a.status, "FAILED", "{key:?}");
        }
        // Encoded-length early reject + decoded-size reject.
        let a = e
            .execute(&put_op("big", &vec![b'x'; KV_OP_MAX_VALUE_BYTES + 1]))
            .await;
        assert_eq!(a.status, "FAILED");
        let a = e.execute(&op(
            "KV_PUT",
            serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"k","valueBase64":"!!!"}),
        ))
        .await;
        assert_eq!(a.status, "FAILED");
    }

    #[tokio::test]
    async fn expiry_and_ttl_options() {
        let e = exec();
        let a = e
            .execute(&op(
                "KV_PUT",
                serde_json::json!({
                    "tenantId":TENANT,"namespaceId":NS,"key":"t",
                    "valueBase64":b64(b"v"),"expirationTtl":30
                }),
            ))
            .await;
        assert_eq!(a.status, "FAILED");
        // ttl ≥60 accepted.
        let a = e
            .execute(&op(
                "KV_PUT",
                serde_json::json!({
                    "tenantId":TENANT,"namespaceId":NS,"key":"t",
                    "valueBase64":b64(b"v"),"expirationTtl":60,
                    "metadata":{"m":1}
                }),
            ))
            .await;
        assert_eq!(a.status, "DONE");
        let a = e
            .execute(&op(
                "KV_GET",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"t"}),
            ))
            .await;
        let r = a.result.unwrap();
        assert_eq!(r["metadata"], serde_json::json!({"m":1}));
        assert!(r["expiresAtMs"].as_i64().unwrap() > now_ms());
    }

    #[tokio::test]
    async fn list_pages_with_cursor() {
        let e = exec();
        for k in ["a1", "a2", "b1"] {
            assert_eq!(e.execute(&put_op(k, b"v")).await.status, "DONE");
        }
        let a = e
            .execute(&op(
                "KV_LIST",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"prefix":"a","limit":1}),
            ))
            .await;
        let r = a.result.unwrap();
        assert_eq!(r["keys"].as_array().unwrap().len(), 1);
        assert_eq!(r["complete"], false);
        let cursor = r["cursor"].as_str().unwrap().to_string();
        let a = e
            .execute(&op(
                "KV_LIST",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"prefix":"a","limit":10,"cursor":cursor}),
            ))
            .await;
        let r = a.result.unwrap();
        let names: Vec<&str> = r["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["a2"]);
        assert_eq!(r["complete"], true);
    }

    #[tokio::test]
    async fn bulk_paths_validate_all_then_apply() {
        let e = exec();
        let a = e
            .execute(&op(
                "KV_BULK_PUT",
                serde_json::json!({
                    "tenantId":TENANT,"namespaceId":NS,
                    "entries":[
                        {"key":"x","valueBase64":b64(b"1")},
                        {"key":"..","valueBase64":b64(b"bad")}
                    ]
                }),
            ))
            .await;
        assert_eq!(a.status, "FAILED");
        // Nothing from the batch landed (all-or-nothing validation).
        let a = e
            .execute(&op(
                "KV_GET",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"x"}),
            ))
            .await;
        assert_eq!(a.result.unwrap()["found"], false);
        let a = e
            .execute(&op(
                "KV_BULK_PUT",
                serde_json::json!({
                    "tenantId":TENANT,"namespaceId":NS,
                    "entries":[
                        {"key":"x","valueBase64":b64(b"1")},
                        {"key":"y","valueBase64":b64(b"2")}
                    ]
                }),
            ))
            .await;
        assert_eq!(a.status, "DONE");
        assert_eq!(a.result.unwrap()["stored"], 2);
        let a = e
            .execute(&op(
                "KV_BULK_DELETE",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"keys":["x","y","z"]}),
            ))
            .await;
        assert_eq!(a.result.unwrap()["deleted"], 3);
        // Empty/oversized batches reject.
        for keys in [
            serde_json::json!([]),
            serde_json::json!(vec!["k"; KV_OP_BULK_MAX_ENTRIES + 1]),
        ] {
            let a = e
                .execute(&op(
                    "KV_BULK_DELETE",
                    serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"keys":keys}),
                ))
                .await;
            assert_eq!(a.status, "FAILED");
        }
    }

    #[tokio::test]
    async fn expired_op_never_executes() {
        let e = exec();
        let mut o = put_op("k", b"v");
        o.expires_at = Some(1);
        let a = e.execute(&o).await;
        assert_eq!(a.status, "EXPIRED");
        let a = e
            .execute(&op(
                "KV_GET",
                serde_json::json!({"tenantId":TENANT,"namespaceId":NS,"key":"k"}),
            ))
            .await;
        assert_eq!(a.result.unwrap()["found"], false);
    }
}
