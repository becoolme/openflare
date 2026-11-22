//! R2 data cell-ops — `R2_LIST` / `R2_HEAD` / `R2_GET` / `R2_PUT` /
//! `R2_DELETE` / `R2_STATS`, dispatched over the generic cell-ops channel for the
//! bucket data API and the `vibeploy workers r2` CLI (CF R2 import/export).
//!
//! Trust boundary (same shape as `kvops.rs` / STORAGE_PURGE): the control
//! plane is the only writer of op rows and resolves `(tenantId, bucketId)`
//! from the signed registry row. The cell NEVER takes a physical key or
//! prefix from the payload: it validates id SHAPES
//! (`purge::valid_purge_tenant` / `valid_r2_purge_id`) and the backend
//! derives `vrt/{tenant}/{bucket}/…` (segment-encoded) itself. Payloads are
//! `deny_unknown_fields`, so a forged `prefix`/`physicalKey` is rejected.
//!
//! Inline object cap (4 MiB) is a CHANNEL budget (the op body and the ack
//! result are JSONB, base64-inflated); larger objects fail typed with
//! `R2_OBJECT_TOO_LARGE` — GET never truncates. The same R2 backend that
//! STORAGE_PURGE uses serves these ops.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use vrt_provider::error::ProviderError;
use vrt_provider::purge;
use vrt_provider::r2::{
    validate_object_key, R2DataBackend, R2GetResult, R2HttpMetadata, R2ObjectMetadata,
    R2PutOptions,
};

use crate::execset::{CellOp, CellOpAck};

/// Single-object inline cap (server `vrtLimits.ts` mirrors this).
pub const R2_OP_MAX_OBJECT_BYTES: usize = 4 * 1024 * 1024;
/// Batch delete / list page bounds.
const R2_OP_MAX_DELETE_KEYS: usize = 1000;
const R2_OP_LIST_MAX_LIMIT: u16 = 1000;
/// Serialized ack-result budget: a 4 MiB body is ~5.6 MiB base64; the
/// control plane's per-item cap (`MAX_CELL_OP_RESULT_BYTES`) is 6 MiB.
const R2_OP_RESULT_MAX_BYTES: usize = 5_900 * 1024;
/// Per backend call budget.
const R2_OP_BUDGET: Duration = Duration::from_secs(15);
const MAX_CONTENT_TYPE_BYTES: usize = 256;

pub const R2_OP_KINDS: &[&str] = &[
    "R2_LIST",
    "R2_HEAD",
    "R2_GET",
    "R2_PUT",
    "R2_DELETE",
    "R2_STATS",
];

pub fn is_r2_kind(kind: &str) -> bool {
    R2_OP_KINDS.contains(&kind)
}

/// Kinds that never mutate the bucket: re-executing one on redelivery
/// is safe, so its (up to ~5.6 MiB) result is never journaled.
pub const R2_READ_KINDS: &[&str] = &["R2_LIST", "R2_HEAD", "R2_GET", "R2_STATS"];
/// Stats scan bounds: the control plane shows `truncated` when a bucket
/// exceeds these; it never blocks an op forever on a huge bucket.
const R2_STATS_MAX_OBJECTS: usize = 20_000;
const R2_STATS_MAX_PAGES: usize = 100;

fn failed(op: &CellOp, error: &str) -> CellOpAck {
    CellOpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(crate::execset::truncate_utf16(error, 512).0),
    }
}

fn provider_failed(op: &CellOp, e: ProviderError) -> CellOpAck {
    // Fixed `R2_*` wire vocabulary — never backend internals.
    failed(op, e.code())
}

fn done(op: &CellOp, result: serde_json::Value) -> CellOpAck {
    match serde_json::to_vec(&result) {
        Ok(bytes) if bytes.len() <= R2_OP_RESULT_MAX_BYTES => CellOpAck {
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
struct R2Scope {
    tenant_id: String,
    bucket_id: String,
}

fn valid_scope(scope: &R2Scope) -> bool {
    purge::valid_purge_tenant(&scope.tenant_id) && purge::valid_r2_purge_id(&scope.bucket_id)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct R2KeyPayload {
    #[serde(flatten)]
    scope: R2Scope,
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct R2ListPayload {
    #[serde(flatten)]
    scope: R2Scope,
    prefix: Option<String>,
    cursor: Option<String>,
    limit: Option<u16>,
    delimiter: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct R2PutPayload {
    #[serde(flatten)]
    scope: R2Scope,
    key: String,
    body_base64: String,
    content_type: Option<String>,
    custom_metadata: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct R2DeletePayload {
    #[serde(flatten)]
    scope: R2Scope,
    keys: Vec<String>,
}

/// Logical key shape: provider rules (≤1024 bytes, no `.`/`..` segments)
/// plus non-empty and no NUL/control bytes.
fn check_key(key: &str) -> Result<(), ProviderError> {
    if key.is_empty() || key.bytes().any(|b| b == 0 || b < 0x20 || b == 0x7f) {
        return Err(ProviderError::R2InvalidOptions);
    }
    validate_object_key(key).map(|_| ())
}

fn http_json(m: &R2HttpMetadata, only_content_type: bool) -> serde_json::Value {
    let mut o = serde_json::Map::new();
    if let Some(v) = &m.content_type {
        o.insert("contentType".into(), v.clone().into());
    }
    if !only_content_type {
        if let Some(v) = &m.content_language {
            o.insert("contentLanguage".into(), v.clone().into());
        }
        if let Some(v) = &m.content_disposition {
            o.insert("contentDisposition".into(), v.clone().into());
        }
        if let Some(v) = &m.content_encoding {
            o.insert("contentEncoding".into(), v.clone().into());
        }
        if let Some(v) = &m.cache_control {
            o.insert("cacheControl".into(), v.clone().into());
        }
    }
    serde_json::Value::Object(o)
}

/// Whitelisted metadata projection (never the raw DTO: no version,
/// checksums, ssec md5 …). `full` adds custom metadata + all HTTP fields.
fn meta_json(m: &R2ObjectMetadata, full: bool) -> serde_json::Value {
    let mut o = serde_json::json!({
        "key": m.key,
        "size": m.size,
        "etag": m.etag,
        "uploaded": m.uploaded,
    });
    if let Some(h) = &m.http_metadata {
        o["httpMetadata"] = http_json(h, !full);
    }
    if full {
        if let Some(c) = &m.custom_metadata {
            o["customMetadata"] = serde_json::json!(c);
        }
    }
    o
}

/// Bucket-scoped R2 data executor — wraps the backend shared with
/// STORAGE_PURGE. Every call returns a TERMINAL ack; nothing panics.
pub struct R2OpsExecutor {
    backend: Arc<dyn R2DataBackend>,
}

impl R2OpsExecutor {
    pub fn new(backend: Arc<dyn R2DataBackend>) -> Self {
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
            "R2_LIST" => self.list(op).await,
            "R2_HEAD" => self.head(op).await,
            "R2_GET" => self.get(op).await,
            "R2_PUT" => self.put(op).await,
            "R2_DELETE" => self.delete(op).await,
            "R2_STATS" => self.stats(op).await,
            _ => failed(op, "unknown r2 op kind"),
        }
    }

    /// Read-only bucket aggregate: object count, bytes and a truncation flag.
    /// Pages the backend list API so a large bucket stays bounded by
    /// `R2_STATS_MAX_OBJECTS`/`R2_STATS_MAX_PAGES` instead of an unbounded scan.
    async fn stats(&self, op: &CellOp) -> CellOpAck {
        let Ok(scope) = serde_json::from_value::<R2Scope>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&scope) {
            return failed(op, "invalid bucket scope");
        }
        let mut objects: usize = 0;
        let mut size_bytes: u64 = 0;
        let mut after: Option<String> = None;
        let mut truncated = false;
        for _ in 0..R2_STATS_MAX_PAGES {
            let page = match self
                .backend
                .data_list(
                    &scope.tenant_id,
                    &scope.bucket_id,
                    "",
                    None,
                    after.as_deref(),
                    R2_OP_LIST_MAX_LIMIT,
                    R2_OP_BUDGET,
                )
                .await
            {
                Ok(p) => p,
                Err(e) => return provider_failed(op, e),
            };
            for object in page.objects {
                objects += 1;
                size_bytes = size_bytes.saturating_add(object.size);
                if objects >= R2_STATS_MAX_OBJECTS {
                    truncated = true;
                    break;
                }
            }
            if truncated {
                break;
            }
            match page.next_after {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        done(
            op,
            serde_json::json!({
                "objects": objects,
                "sizeBytes": size_bytes,
                "truncated": truncated,
                "sampledAtMs": crate::execset::now_secs() * 1000,
            }),
        )
    }

    async fn list(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<R2ListPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid bucket scope");
        }
        let prefix = p.prefix.unwrap_or_default();
        if prefix.len() > 1024 || prefix.bytes().any(|b| b == 0) {
            return failed(op, "R2_KEY_TOO_LARGE");
        }
        let limit = p.limit.unwrap_or(R2_OP_LIST_MAX_LIMIT);
        if limit == 0 || limit > R2_OP_LIST_MAX_LIMIT {
            return failed(op, "R2_INVALID_OPTIONS");
        }
        if p
            .delimiter
            .as_deref()
            .is_some_and(|d| d.is_empty() || d.len() > 16 || d.bytes().any(|b| b == 0))
        {
            return failed(op, "R2_INVALID_OPTIONS");
        }
        let after = match p.cursor.as_deref() {
            None | Some("") => None,
            Some(c) if c.len() > 1024 || c.bytes().any(|b| b == 0) => {
                return failed(op, "R2_CURSOR_INVALID")
            }
            Some(c) => Some(c),
        };
        match self
            .backend
            .data_list(
                &p.scope.tenant_id,
                &p.scope.bucket_id,
                &prefix,
                p.delimiter.as_deref(),
                after,
                limit,
                R2_OP_BUDGET,
            )
            .await
        {
            Ok(page) => {
                let objects: Vec<_> = page.objects.iter().map(|m| meta_json(m, false)).collect();
                done(
                    op,
                    serde_json::json!({
                        "objects": objects,
                        "delimitedPrefixes": page.delimited_prefixes,
                        "truncated": page.next_after.is_some(),
                        "cursor": page.next_after,
                    }),
                )
            }
            Err(e) => provider_failed(op, e),
        }
    }

    async fn head(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<R2KeyPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid bucket scope");
        }
        if let Err(e) = check_key(&p.key) {
            return provider_failed(op, e);
        }
        match self
            .backend
            .data_head(&p.scope.tenant_id, &p.scope.bucket_id, &p.key, R2_OP_BUDGET)
            .await
        {
            Ok(None) => done(op, serde_json::json!({ "found": false })),
            Ok(Some(m)) => done(
                op,
                serde_json::json!({ "found": true, "object": meta_json(&m, true) }),
            ),
            Err(e) => provider_failed(op, e),
        }
    }

    async fn get(&self, op: &CellOp) -> CellOpAck {
        use base64::Engine;
        let Ok(p) = serde_json::from_value::<R2KeyPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid bucket scope");
        }
        if let Err(e) = check_key(&p.key) {
            return provider_failed(op, e);
        }
        // HEAD first: an oversize object is rejected typed WITHOUT
        // pulling its body (S3 backend would otherwise download it all).
        match self
            .backend
            .data_head(&p.scope.tenant_id, &p.scope.bucket_id, &p.key, R2_OP_BUDGET)
            .await
        {
            Ok(None) => return done(op, serde_json::json!({ "found": false })),
            Ok(Some(m)) if m.size > R2_OP_MAX_OBJECT_BYTES as u64 => {
                return failed(op, "R2_OBJECT_TOO_LARGE")
            }
            Ok(Some(_)) => {}
            Err(e) => return provider_failed(op, e),
        }
        match self
            .backend
            .data_get(
                &p.scope.tenant_id,
                &p.scope.bucket_id,
                &p.key,
                R2_OP_MAX_OBJECT_BYTES as u64,
                R2_OP_BUDGET,
            )
            .await
        {
            Ok(R2GetResult::Missing) => done(op, serde_json::json!({ "found": false })),
            Ok(R2GetResult::Precondition(_)) => failed(op, "R2_PRECONDITION_FAILED"),
            Ok(R2GetResult::Body(m, body)) => {
                // Re-check after the read: the object may have been
                // replaced between HEAD and GET — never truncate.
                if body.len() > R2_OP_MAX_OBJECT_BYTES {
                    return failed(op, "R2_OBJECT_TOO_LARGE");
                }
                done(
                    op,
                    serde_json::json!({
                        "found": true,
                        "object": meta_json(&m, true),
                        "bodyBase64": base64::engine::general_purpose::STANDARD.encode(&body),
                    }),
                )
            }
            Err(e) => provider_failed(op, e),
        }
    }

    async fn put(&self, op: &CellOp) -> CellOpAck {
        use base64::Engine;
        let Ok(p) = serde_json::from_value::<R2PutPayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid bucket scope");
        }
        if let Err(e) = check_key(&p.key) {
            return provider_failed(op, e);
        }
        // Encoded-length early reject before decoding megabytes.
        if p.body_base64.len() > (R2_OP_MAX_OBJECT_BYTES / 3 + 1) * 4 {
            return failed(op, "R2_OBJECT_TOO_LARGE");
        }
        let body = match base64::engine::general_purpose::STANDARD.decode(&p.body_base64) {
            Ok(b) => b,
            Err(_) => return failed(op, "bodyBase64 malformed"),
        };
        if body.len() > R2_OP_MAX_OBJECT_BYTES {
            return failed(op, "R2_OBJECT_TOO_LARGE");
        }
        if p.content_type
            .as_deref()
            .is_some_and(|c| c.len() > MAX_CONTENT_TYPE_BYTES || c.bytes().any(|b| b < 0x20))
        {
            return failed(op, "R2_INVALID_OPTIONS");
        }
        let options = R2PutOptions {
            http_metadata: R2HttpMetadata {
                content_type: p.content_type,
                ..Default::default()
            },
            custom_metadata: p.custom_metadata.unwrap_or_default(),
            ..Default::default()
        };
        match self
            .backend
            .data_put(
                &p.scope.tenant_id,
                &p.scope.bucket_id,
                &p.key,
                &body,
                &options,
                R2_OP_BUDGET,
            )
            .await
        {
            Ok(Some(m)) => done(
                op,
                serde_json::json!({ "stored": true, "etag": m.etag, "size": m.size }),
            ),
            // No `onlyIf` is ever sent, so a `None` is a backend bug —
            // fail honestly rather than claim success.
            Ok(None) => failed(op, "R2_PRECONDITION_FAILED"),
            Err(e) => provider_failed(op, e),
        }
    }

    async fn delete(&self, op: &CellOp) -> CellOpAck {
        let Ok(p) = serde_json::from_value::<R2DeletePayload>(op.payload.clone()) else {
            return failed(op, "malformed payload");
        };
        if !valid_scope(&p.scope) {
            return failed(op, "invalid bucket scope");
        }
        if p.keys.is_empty() || p.keys.len() > R2_OP_MAX_DELETE_KEYS {
            return failed(op, "R2_INVALID_OPTIONS");
        }
        // Validate EVERY key before deleting any.
        for k in &p.keys {
            if let Err(e) = check_key(k) {
                return provider_failed(op, e);
            }
        }
        match self
            .backend
            .data_delete(&p.scope.tenant_id, &p.scope.bucket_id, &p.keys, R2_OP_BUDGET)
            .await
        {
            Ok(()) => done(op, serde_json::json!({ "deleted": p.keys.len() })),
            Err(e) => provider_failed(op, e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    const TENANT: &str = "team_abc123";
    const BUCKET: &str = "vrt-0123456789abcdef";

    fn mem_exec() -> R2OpsExecutor {
        R2OpsExecutor::new(Arc::new(vrt_provider::r2::MemoryR2Provider::new()))
    }

    fn op(kind: &str, payload: serde_json::Value) -> CellOp {
        CellOp {
            id: format!("op-{kind}"),
            seq: 3,
            resource_key: BUCKET.into(),
            kind: kind.into(),
            worker_id: String::new(),
            payload,
            expires_at: None,
        }
    }

    fn b64(v: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(v)
    }

    fn scope() -> serde_json::Value {
        serde_json::json!({"tenantId": TENANT, "bucketId": BUCKET})
    }

    fn with(extra: serde_json::Value) -> serde_json::Value {
        let mut s = scope();
        for (k, v) in extra.as_object().unwrap() {
            s[k] = v.clone();
        }
        s
    }

    fn put_op(key: &str, body: &[u8]) -> CellOp {
        op(
            "R2_PUT",
            with(serde_json::json!({"key": key, "bodyBase64": b64(body), "contentType": "text/plain",
                "customMetadata": {"a": "b"}})),
        )
    }

    async fn roundtrip(e: &R2OpsExecutor) {
        assert_eq!(e.execute(&put_op("dir/a.txt", b"hello")).await.status, "DONE");
        assert_eq!(e.execute(&put_op("dir/b.txt", b"world")).await.status, "DONE");
        assert_eq!(e.execute(&put_op("top.txt", b"t")).await.status, "DONE");
        let a = e
            .execute(&op("R2_GET", with(serde_json::json!({"key": "dir/a.txt"}))))
            .await;
        let r = a.result.unwrap();
        assert_eq!(r["bodyBase64"], b64(b"hello"));
        assert_eq!(r["object"]["httpMetadata"]["contentType"], "text/plain");
        assert_eq!(r["object"]["customMetadata"]["a"], "b");
        let a = e
            .execute(&op("R2_HEAD", with(serde_json::json!({"key": "dir/a.txt"}))))
            .await;
        assert_eq!(a.result.unwrap()["object"]["size"], 5);
        let a = e
            .execute(&op("R2_GET", with(serde_json::json!({"key": "nope"}))))
            .await;
        assert_eq!(a.result.unwrap()["found"], false);
        // list: paged + delimiter.
        let a = e
            .execute(&op("R2_LIST", with(serde_json::json!({"limit": 2}))))
            .await;
        let r = a.result.unwrap();
        assert_eq!(r["objects"].as_array().unwrap().len(), 2);
        assert_eq!(r["truncated"], true);
        let cursor = r["cursor"].as_str().unwrap().to_string();
        let a = e
            .execute(&op("R2_LIST", with(serde_json::json!({"cursor": cursor}))))
            .await;
        assert_eq!(a.result.unwrap()["objects"].as_array().unwrap().len(), 1);
        let a = e
            .execute(&op("R2_LIST", with(serde_json::json!({"delimiter": "/"}))))
            .await;
        let r = a.result.unwrap();
        assert_eq!(r["delimitedPrefixes"], serde_json::json!(["dir/"]));
        assert_eq!(r["objects"][0]["key"], "top.txt");
        // stats: real aggregate over the full bucket.
        let a = e.execute(&op("R2_STATS", scope())).await;
        let r = a.result.unwrap();
        assert_eq!(r["objects"], 3, "{r}");
        assert_eq!(r["sizeBytes"], 11, "{r}");
        assert_eq!(r["truncated"], false, "{r}");
        // bad scope is refused before any backend call.
        let a = e
            .execute(&op("R2_STATS", with(serde_json::json!({"tenantId": "../x"}))))
            .await;
        assert_eq!(a.status, "FAILED");
        // delete batch (+ idempotent on missing).
        let a = e
            .execute(&op(
                "R2_DELETE",
                with(serde_json::json!({"keys": ["dir/a.txt", "dir/b.txt", "ghost"]})),
            ))
            .await;
        assert_eq!(a.result.unwrap()["deleted"], 3);
        let a = e
            .execute(&op("R2_LIST", with(serde_json::json!({"prefix": "dir/"}))))
            .await;
        assert_eq!(a.result.unwrap()["objects"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn memory_backend_roundtrip() {
        roundtrip(&mem_exec()).await;
    }

    #[tokio::test]
    async fn fs_backend_roundtrip_and_tenant_isolation() {
        let dir = std::env::temp_dir().join(format!("r2ops-fs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let provider = vrt_provider::r2::fs::FsR2Provider::new(dir.clone(), 8 * 1024 * 1024).unwrap();
        let e = R2OpsExecutor::new(Arc::new(provider));
        roundtrip(&e).await;
        // Another tenant sharing the bucket id sees nothing.
        assert_eq!(e.execute(&put_op("secret", b"s")).await.status, "DONE");
        let other = serde_json::json!({"tenantId": "team_other", "bucketId": BUCKET, "key": "secret"});
        let a = e.execute(&op("R2_GET", other)).await;
        assert_eq!(a.result.unwrap()["found"], false);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rejects_unknown_fields_and_bad_scope() {
        let e = mem_exec();
        for bad in [
            serde_json::json!({"tenantId":TENANT,"bucketId":BUCKET,"key":"k","extra":1}),
            serde_json::json!({"tenantId":TENANT,"bucketId":BUCKET,"key":"k","prefix":"vrt/other/"}),
            serde_json::json!({"tenantId":"../x","bucketId":BUCKET,"key":"k"}),
            serde_json::json!({"tenantId":TENANT,"bucketId":"vrtkv_abc","key":"k"}),
            serde_json::json!({"tenantId":TENANT,"bucketId":"vrt-","key":"k"}),
            serde_json::json!({"tenantId":TENANT,"bucketId":"vrt-a/b","key":"k"}),
            serde_json::json!({"bucketId":BUCKET,"key":"k"}),
        ] {
            for kind in ["R2_GET", "R2_HEAD"] {
                let a = e.execute(&op(kind, bad.clone())).await;
                assert_eq!(a.status, "FAILED", "{kind} {bad}");
            }
        }
        let a = e
            .execute(&op("R2_LIST", with(serde_json::json!({"physicalPrefix": "x"}))))
            .await;
        assert_eq!(a.status, "FAILED");
    }

    #[tokio::test]
    async fn rejects_bad_keys_and_limits() {
        let e = mem_exec();
        for key in ["", "..", "a/../b", "a/./b", "nul\0key", &"k".repeat(1025)] {
            let a = e.execute(&put_op(key, b"v")).await;
            assert_eq!(a.status, "FAILED", "{key:?}");
        }
        for limit in [0, 1001] {
            let a = e
                .execute(&op("R2_LIST", with(serde_json::json!({"limit": limit}))))
                .await;
            assert_eq!(a.status, "FAILED");
        }
        let a = e
            .execute(&op("R2_LIST", with(serde_json::json!({"delimiter": ""}))))
            .await;
        assert_eq!(a.status, "FAILED");
        let a = e
            .execute(&op("R2_DELETE", with(serde_json::json!({"keys": []}))))
            .await;
        assert_eq!(a.status, "FAILED");
        let a = e
            .execute(&op(
                "R2_DELETE",
                with(serde_json::json!({"keys": vec!["k"; R2_OP_MAX_DELETE_KEYS + 1]})),
            ))
            .await;
        assert_eq!(a.status, "FAILED");
        // One bad key in a batch deletes nothing.
        assert_eq!(e.execute(&put_op("keep", b"v")).await.status, "DONE");
        let a = e
            .execute(&op("R2_DELETE", with(serde_json::json!({"keys": ["keep", ".."]}))))
            .await;
        assert_eq!(a.status, "FAILED");
        let a = e.execute(&op("R2_HEAD", with(serde_json::json!({"key": "keep"})))).await;
        assert_eq!(a.result.unwrap()["found"], true);
    }

    #[tokio::test]
    async fn size_cap_is_typed_never_truncated() {
        let e = mem_exec();
        let a = e
            .execute(&put_op("big", &vec![b'x'; R2_OP_MAX_OBJECT_BYTES + 1]))
            .await;
        assert_eq!(a.status, "FAILED");
        assert_eq!(a.error.as_deref(), Some("R2_OBJECT_TOO_LARGE"));
        // Exactly the cap is accepted and round-trips intact.
        let at_cap = vec![b'y'; R2_OP_MAX_OBJECT_BYTES];
        assert_eq!(e.execute(&put_op("cap", &at_cap)).await.status, "DONE");
        let a = e.execute(&op("R2_GET", with(serde_json::json!({"key": "cap"})))).await;
        assert_eq!(a.status, "DONE");
        let a = e
            .execute(&op(
                "R2_PUT",
                with(serde_json::json!({"key": "k", "bodyBase64": "!!!"})),
            ))
            .await;
        assert_eq!(a.status, "FAILED");
    }

    #[tokio::test]
    async fn get_rejects_object_stored_out_of_band_beyond_cap() {
        // Object written straight through the provider (e.g. a larger
        // binding-runtime put) must make GET fail typed, not truncate.
        let backend = Arc::new(vrt_provider::r2::MemoryR2Provider::new());
        let big = vec![b'z'; R2_OP_MAX_OBJECT_BYTES + 10];
        backend
            .data_put(TENANT, BUCKET, "huge", &big, &R2PutOptions::default(), R2_OP_BUDGET)
            .await
            .unwrap();
        let e = R2OpsExecutor::new(backend);
        let a = e.execute(&op("R2_GET", with(serde_json::json!({"key": "huge"})))).await;
        assert_eq!(a.status, "FAILED");
        assert_eq!(a.error.as_deref(), Some("R2_OBJECT_TOO_LARGE"));
        // HEAD still works for it (size visible → CLI can report it).
        let a = e.execute(&op("R2_HEAD", with(serde_json::json!({"key": "huge"})))).await;
        assert_eq!(a.result.unwrap()["object"]["size"], R2_OP_MAX_OBJECT_BYTES + 10);
    }

    #[tokio::test]
    async fn expired_op_never_executes() {
        let e = mem_exec();
        let mut o = put_op("k", b"v");
        o.expires_at = Some(1);
        assert_eq!(e.execute(&o).await.status, "EXPIRED");
        let a = e.execute(&op("R2_GET", with(serde_json::json!({"key": "k"})))).await;
        assert_eq!(a.result.unwrap()["found"], false);
    }
}
