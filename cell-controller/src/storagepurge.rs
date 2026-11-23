//! RV-R70: `STORAGE_PURGE` cell-op executor — bounded provider-side
//! KV (kvrocks / fs) and R2 (S3 / fs) prefix cleanup.
//!
//! Design: the executor holds DIRECT backend handles built from
//! `[storage_purge]` — the same `[kvrocks]`/`[s3]`/`[kv]`/`[r2]`
//! config shapes the provider binary uses (fields reused verbatim
//! from `vrt_provider::config`). The alternative (an HTTP hop through
//! the provider's internal binding API) would add an auth+route
//! surface for a purely local cleanup; direct backend access keeps
//! the trust boundary identical to the provider's.
//!
//! Safety invariants:
//! - **identity, never a prefix**: the op payload carries
//!   `{storage, storageId, tenantId}`; the physical prefix is derived
//!   inside the provider backend (`vrt:kv:{t}:{ns}:*`,
//!   `vrt/{t}/{bucket}/*`) — a forged payload can never name an
//!   arbitrary prefix. `vrt_provider::purge::valid_*` rejects empty /
//!   metachar / wrong-kind ids before any command runs.
//! - **bounded passes**: one op = one bounded pass
//!   (`max_items_per_op`, default 2000). `complete:false` tells the
//!   control plane to dispatch the next-generation op — THAT is the
//!   continuation; the executor never runs an unbounded job per op.
//!   `complete:true` is returned only when the backend's own final
//!   pass confirmed zero remaining targets.
//! - **fail closed**: malformed payload, unknown storage kind,
//!   unconfigured backend, or backend error → FAILED with a bounded
//!   fixed-phrase error (never a fake DONE, never a panic).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::execset::{CellOp, CellOpAck};
use vrt_provider::kv::kvrocks::KvrocksKvProvider;
use vrt_provider::error::ProviderError;
use vrt_provider::purge::PurgeOutcome;
use vrt_provider::r2::s3::{S3Config, S3R2Provider, SecretString};

/// `[storage_purge]` — executor wiring. Absent ⇒ `storagePurge`
/// capability false and purge ops are never pulled to this cell.
/// Each backend mirrors the provider's own section so an operator
/// configures one source of truth per site.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoragePurgeConfig {
    /// Kvrocks backend (`[storage_purge.kvrocks]`) — same fields as
    /// the provider's `[kvrocks]` (url/urlEnv, timeouts).
    #[serde(default)]
    pub kvrocks: Option<vrt_provider::config::KvrocksConfig>,
    /// S3 backend (`[storage_purge.s3]`) — same fields as the
    /// provider's `[s3]` (creds via env vars only).
    #[serde(default)]
    pub s3: Option<vrt_provider::config::S3Config>,
    /// Filesystem KV backend (`[storage_purge.kv]`) — the provider's
    /// standalone data root (`dir` shared with `[kv]`).
    #[serde(default)]
    pub kv: Option<vrt_provider::config::FsKvConfig>,
    /// Filesystem R2 backend (`[storage_purge.r2]`) — the provider's
    /// standalone data root (`dir` shared with `[r2]`).
    #[serde(default)]
    pub r2: Option<vrt_provider::config::FsR2Config>,
    /// Filesystem Vectorize backend (`[storage_purge.vectorize]`) — the
    /// provider's index store root (`dir` shared with `[vectorize]`).
    #[serde(default)]
    pub vectorize: Option<vrt_provider::config::FsVectorizeConfig>,
    /// Entries/objects bound per purge pass (default 2000, ≤10_000).
    /// Continuation is control-plane redispatch, not an in-op loop.
    #[serde(default = "default_max_items_per_op")]
    pub max_items_per_op: usize,
    /// Per-backend-call timeout seconds (default 15).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_max_items_per_op() -> usize {
    2_000
}
fn default_timeout_secs() -> u64 {
    15
}
/// Hard cap on the per-op bound — a config typo must not turn one op
/// into an unbounded keyspace walk.
const MAX_ITEMS_CEILING: usize = 10_000;

/// Strict op payload — the server-side `purgePayloadSchema` mirrored.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PurgePayload {
    storage: String,
    storage_id: String,
    tenant_id: String,
}

#[derive(Debug)]
pub enum KvBackend {
    Kvrocks(KvrocksKvProvider),
    Fs(vrt_provider::kv::fs::FsKvProvider),
}

/// KV1: both KV backends expose the namespace-scoped data-plane
/// surface (provider `KvDataBackend`); the cell ops executor shares
/// this via Arc — one connection, purge + data ops.
#[async_trait::async_trait]
impl vrt_provider::kv::KvDataBackend for KvBackend {
    async fn data_get(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<Option<vrt_provider::kv::KvEntry>, vrt_provider::error::ProviderError> {
        match self {
            KvBackend::Kvrocks(p) => p.data_get(tenant_id, namespace_id, key).await,
            KvBackend::Fs(p) => p.data_get(tenant_id, namespace_id, key).await,
        }
    }
    async fn data_put(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
        value: &[u8],
        options: &vrt_provider::kv::KvPutOptions,
    ) -> Result<(), vrt_provider::error::ProviderError> {
        match self {
            KvBackend::Kvrocks(p) => p.data_put(tenant_id, namespace_id, key, value, options).await,
            KvBackend::Fs(p) => p.data_put(tenant_id, namespace_id, key, value, options).await,
        }
    }
    async fn data_delete(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<(), vrt_provider::error::ProviderError> {
        match self {
            KvBackend::Kvrocks(p) => p.data_delete(tenant_id, namespace_id, key).await,
            KvBackend::Fs(p) => p.data_delete(tenant_id, namespace_id, key).await,
        }
    }
    async fn data_list(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<vrt_provider::kv::KvListPage, vrt_provider::error::ProviderError> {
        match self {
            KvBackend::Kvrocks(p) => p.data_list(tenant_id, namespace_id, prefix, after, limit).await,
            KvBackend::Fs(p) => p.data_list(tenant_id, namespace_id, prefix, after, limit).await,
        }
    }
}
#[derive(Debug)]
pub enum R2Backend {
    S3(S3R2Provider),
    Fs(vrt_provider::r2::fs::FsR2Provider),
}

/// R2 data ops share this backend (one client, purge + data ops);
/// every method delegates to the provider's validated `R2DataBackend`.
#[async_trait::async_trait]
impl vrt_provider::r2::R2DataBackend for R2Backend {
    async fn data_head(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        budget: Duration,
    ) -> Result<Option<vrt_provider::r2::R2ObjectMetadata>, ProviderError> {
        match self {
            R2Backend::S3(p) => p.data_head(tenant_id, bucket_id, key, budget).await,
            R2Backend::Fs(p) => p.data_head(tenant_id, bucket_id, key, budget).await,
        }
    }
    async fn data_get(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        max_bytes: u64,
        budget: Duration,
    ) -> Result<vrt_provider::r2::R2GetResult, ProviderError> {
        match self {
            R2Backend::S3(p) => p.data_get(tenant_id, bucket_id, key, max_bytes, budget).await,
            R2Backend::Fs(p) => p.data_get(tenant_id, bucket_id, key, max_bytes, budget).await,
        }
    }
    async fn data_put(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        body: &[u8],
        options: &vrt_provider::r2::R2PutOptions,
        budget: Duration,
    ) -> Result<Option<vrt_provider::r2::R2ObjectMetadata>, ProviderError> {
        match self {
            R2Backend::S3(p) => p.data_put(tenant_id, bucket_id, key, body, options, budget).await,
            R2Backend::Fs(p) => p.data_put(tenant_id, bucket_id, key, body, options, budget).await,
        }
    }
    async fn data_delete(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        keys: &[String],
        budget: Duration,
    ) -> Result<(), ProviderError> {
        match self {
            R2Backend::S3(p) => p.data_delete(tenant_id, bucket_id, keys, budget).await,
            R2Backend::Fs(p) => p.data_delete(tenant_id, bucket_id, keys, budget).await,
        }
    }
    async fn data_list(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
        budget: Duration,
    ) -> Result<vrt_provider::r2::R2ListPage, ProviderError> {
        match self {
            R2Backend::S3(p) => {
                p.data_list(tenant_id, bucket_id, prefix, delimiter, after, limit, budget).await
            }
            R2Backend::Fs(p) => {
                p.data_list(tenant_id, bucket_id, prefix, delimiter, after, limit, budget).await
            }
        }
    }
}

/// Wired backends; `None` fields mean that kind fails closed.
pub struct StoragePurgeExecutor {
    kv: Option<std::sync::Arc<KvBackend>>,
    r2: Option<std::sync::Arc<R2Backend>>,
    vectorize: Option<PathBuf>,
    max_items: usize,
    timeout: Duration,
}

impl StoragePurgeExecutor {
    /// Build all configured backends. Returns Err when the section is
    /// present but nothing usable was configured — the caller logs
    /// the failure and reports `storagePurge:false` (fail closed:
    /// the capability bit must mean "an executor is actually wired",
    /// never "a config section exists").
    pub async fn connect(cfg: &StoragePurgeConfig) -> Result<Self, String> {
        let timeout = Duration::from_secs(cfg.timeout_secs.max(1));
        let mut kv: Option<std::sync::Arc<KvBackend>> = None;
        if let Some(kvc) = &cfg.kvrocks {
            let url = kvc
                .url_env
                .as_deref()
                .and_then(|name| std::env::var(name).ok())
                .or_else(|| kvc.url.clone())
                .ok_or_else(|| "storage_purge.kvrocks url missing (url or urlEnv)".to_string())?;
            let conn_timeout = Duration::from_millis(kvc.connect_timeout_ms.max(1));
            let cmd_timeout = Duration::from_millis(kvc.timeout_ms.max(1));
            let provider = KvrocksKvProvider::connect(&url, conn_timeout, cmd_timeout)
                .await
                .map_err(|e| format!("storage_purge.kvrocks connect failed: {}", e.code()))?;
            kv = Some(std::sync::Arc::new(KvBackend::Kvrocks(provider)));
        } else if let Some(fsc) = &cfg.kv {
            let provider = vrt_provider::kv::fs::FsKvProvider::new(fsc.dir.clone())
                .map_err(|_| "storage_purge.kv dir is not a usable 0700 directory".to_string())?;
            kv = Some(std::sync::Arc::new(KvBackend::Fs(provider)));
        }
        let mut r2: Option<std::sync::Arc<R2Backend>> = None;
        if let Some(s3c) = &cfg.s3 {
            let access_key = std::env::var(&s3c.access_key_env)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "storage_purge.s3 access key env missing".to_string())?;
            let secret_key = std::env::var(&s3c.secret_key_env)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "storage_purge.s3 secret key env missing".to_string())?;
            let provider = S3R2Provider::new(S3Config {
                endpoint: s3c.endpoint.clone(),
                region: s3c.region.clone(),
                bucket: s3c.bucket.clone(),
                access_key,
                secret_key: SecretString::new(secret_key),
                timeout,
                max_object_bytes: s3c.max_object_bytes,
                conditional_writes: s3c.conditional_writes,
                multipart_ttl_ms: s3c.multipart_ttl_ms,
            })
            .map_err(|_| "storage_purge.s3 client build failed".to_string())?;
            r2 = Some(std::sync::Arc::new(R2Backend::S3(provider)));
        } else if let Some(fsc) = &cfg.r2 {
            let provider =
                vrt_provider::r2::fs::FsR2Provider::new(fsc.dir.clone(), fsc.max_object_bytes)
                    .map_err(|_| {
                        "storage_purge.r2 dir is not a usable 0700 directory".to_string()
                    })?;
            r2 = Some(std::sync::Arc::new(R2Backend::Fs(provider)));
        }
        let vectorize = match &cfg.vectorize {
            Some(v) => {
                std::fs::create_dir_all(&v.dir)
                    .map_err(|_| "storage_purge.vectorize dir is not usable".to_string())?;
                Some(v.dir.clone())
            }
            None => None,
        };
        if kv.is_none() && r2.is_none() && vectorize.is_none() {
            return Err("storage_purge configured with no usable backend".to_string());
        }
        Ok(Self {
            kv,
            r2,
            vectorize,
            max_items: cfg.max_items_per_op.clamp(1, MAX_ITEMS_CEILING),
            timeout,
        })
    }

    /// True when a KV backend (kvrocks or fs) was built — the cell's
    /// `kv` capability claim derives from this (never from config
    /// presence alone).
    pub fn has_kv(&self) -> bool {
        self.kv.is_some()
    }

    /// True when an R2 backend (S3 or fs) was built — the `r2`
    /// capability claim derives from this.
    pub fn has_r2(&self) -> bool {
        self.r2.is_some()
    }

    /// True when a Vectorize index store was wired for purge.
    pub fn has_vectorize(&self) -> bool {
        self.vectorize.is_some()
    }

    /// Shared R2 backend for the data cell-ops executor (`None` → kinds
    /// stay unrouted and expire PENDING; fail closed).
    pub fn r2_backend(&self) -> Option<std::sync::Arc<R2Backend>> {
        self.r2.clone()
    }

    /// KV1: the shared KV backend for the data cell-ops executor —
    /// `None` when no KV backend wired (op kinds stay unrouted, the
    /// ops expire PENDING server-side; fail closed, never faked).
    pub fn kv_backend(&self) -> Option<std::sync::Arc<KvBackend>> {
        self.kv.clone()
    }

    /// Execute one bounded purge pass. DONE with `{complete:false}`
    /// means "deleted up to the bound, more may remain" — the control
    /// plane redispatches; `{complete:true}` is the backend's own
    /// drained-verified verdict. FAILURES never fake completion.
    pub async fn execute(&self, op: &CellOp) -> CellOpAck {
        let payload: PurgePayload = match serde_json::from_value(op.payload.clone()) {
            Ok(p) => p,
            Err(_) => return failed(op, "malformed storage purge payload"),
        };
        let outcome = match payload.storage.as_str() {
            "kv" => {
                if !vrt_provider::purge::valid_kv_purge_id(&payload.storage_id) {
                    return failed(op, "invalid kv storageId");
                }
                match self.kv.as_deref() {
                    Some(KvBackend::Kvrocks(p)) => {
                        p.purge_namespace(&payload.tenant_id, &payload.storage_id, self.max_items)
                            .await
                    }
                    Some(KvBackend::Fs(p)) => {
                        p.purge_namespace(&payload.tenant_id, &payload.storage_id, self.max_items)
                            .await
                    }
                    None => return failed(op, "kv purge backend unconfigured"),
                }
            }
            "r2" => {
                if !vrt_provider::purge::valid_r2_purge_id(&payload.storage_id) {
                    return failed(op, "invalid r2 storageId");
                }
                match self.r2.as_deref() {
                    Some(R2Backend::S3(p)) => {
                        p.purge_prefix(
                            &payload.tenant_id,
                            &payload.storage_id,
                            self.max_items,
                            self.timeout,
                        )
                        .await
                    }
                    Some(R2Backend::Fs(p)) => {
                        p.purge_prefix(&payload.tenant_id, &payload.storage_id, self.max_items)
                            .await
                    }
                    None => return failed(op, "r2 purge backend unconfigured"),
                }
            }
            "vectorize" => {
                if !vrt_provider::purge::valid_purge_tenant(&payload.tenant_id)
                    || !vrt_provider::purge::valid_purge_target_id(&payload.storage_id)
                {
                    return failed(op, "invalid vectorize identity");
                }
                match self.vectorize.as_deref() {
                    Some(root) => vrt_provider::vectorize::purge_index_files(
                        root,
                        &payload.tenant_id,
                        &payload.storage_id,
                        self.max_items,
                    ),
                    None => return failed(op, "vectorize purge backend unconfigured"),
                }
            }
            _ => return failed(op, "unknown storage kind"),
        };
        match outcome {
            Ok(PurgeOutcome { deleted, complete }) => CellOpAck {
                seq: op.seq,
                status: "DONE",
                result: Some(serde_json::json!({
                    "complete": complete,
                    "deletedKeys": deleted,
                })),
                error: None,
            },
            Err(e) => failed(op, &format!("purge backend error: {}", e.code())),
        }
    }
}

fn failed(op: &CellOp, error: &str) -> CellOpAck {
    // Bounded fixed-phrase errors — the server persists them on the
    // registry row; backend detail never leaks beyond the error code.
    CellOpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(crate::execset::truncate_utf16(error, 512).0),
    }
}

/// Shareable handle for the ops loop.
pub type SharedStoragePurgeExecutor = Arc<StoragePurgeExecutor>;

#[cfg(test)]
mod tests {
    use super::*;

    fn op(payload: serde_json::Value) -> CellOp {
        CellOp {
            id: "o1".into(),
            seq: 7,
            kind: "STORAGE_PURGE".into(),
            worker_id: String::new(),
            resource_key: "vrtkv_t_ns".into(),
            payload,
            expires_at: None,
        }
    }

    fn fs_executor(root: &std::path::Path) -> StoragePurgeExecutor {
        StoragePurgeExecutor {
            kv: Some(std::sync::Arc::new(KvBackend::Fs(
                vrt_provider::kv::fs::FsKvProvider::new(root.join("kv")).unwrap(),
            ))),
            r2: Some(std::sync::Arc::new(R2Backend::Fs(
                vrt_provider::r2::fs::FsR2Provider::new(root.join("r2"), 1024).unwrap(),
            ))),
            vectorize: Some(root.join("vectorize")),
            max_items: 3,
            timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn malformed_and_unvalidated_payloads_fail_not_execute() {
        let dir = std::env::temp_dir().join(format!("sp-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let exec = fs_executor(&dir);
        // Unknown fields are rejected (deny_unknown_fields).
        let ack = exec
            .execute(&op(serde_json::json!({
                "storage": "kv", "storageId": "vrtkv_t_ns", "tenantId": "t", "x": 1
            })))
            .await;
        assert_eq!(ack.status, "FAILED");
        // Empty / metachar / wrong-kind ids all reject before any I/O.
        for id in ["", "vrtkv_t_ns:*", "vrtkv_", "vrt-abc", "vrtkv_a/b"] {
            let ack = exec
                .execute(&op(serde_json::json!({
                    "storage": "kv", "storageId": id, "tenantId": "t"
                })))
                .await;
            assert_eq!(ack.status, "FAILED", "id {id:?} accepted");
        }
        // Unknown storage kind fails closed.
        let ack = exec
            .execute(&op(serde_json::json!({
                "storage": "s3", "storageId": "vrtkv_x", "tenantId": "t"
            })))
            .await;
        assert_eq!(ack.status, "FAILED");
        let _ = std::fs::remove_dir_all(&dir);
    }


    #[tokio::test]
    async fn fs_vectorize_purge_is_scoped_and_complete() {
        let dir = std::env::temp_dir().join(format!("sp-vx-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let exec = fs_executor(&dir);
        let indexes = dir.join("vectorize").join("t1");
        std::fs::create_dir_all(&indexes).unwrap();
        let id = "abcdefghijklmnopqrstuvwxyz";
        for suffix in [".vec.db", ".vec.db-wal", ".vec.db-shm"] {
            std::fs::write(indexes.join(format!("{id}{suffix}")), b"x").unwrap();
        }
        let keep = indexes.join("keep.vec.db");
        std::fs::write(&keep, b"x").unwrap();

        let ack = exec
            .execute(&op(serde_json::json!({
                "storage": "vectorize", "storageId": id, "tenantId": "t1"
            })))
            .await;
        assert_eq!(ack.status, "DONE");
        assert_eq!(ack.result.as_ref().unwrap()["complete"], true);
        assert_eq!(ack.result.as_ref().unwrap()["deletedKeys"], 3);
        assert!(keep.exists());

        let ack = exec
            .execute(&op(serde_json::json!({
                "storage": "vectorize", "storageId": "../x", "tenantId": "t1"
            })))
            .await;
        assert_eq!(ack.status, "FAILED");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fs_kv_purge_is_bounded_then_completes() {
        let dir = std::env::temp_dir().join(format!("sp-kv-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let exec = fs_executor(&dir);
        let ns_dir = dir.join("kv").join("t").join("vrtkv_t_ns");
        std::fs::create_dir_all(&ns_dir).unwrap();
        for i in 0..5 {
            std::fs::write(ns_dir.join(format!("k{i}")), b"v").unwrap();
        }
        // Sibling namespace `vrtkv_t_nsab` must survive — boundary is
        // the segment-encoded directory level.
        let sibling = dir.join("kv").join("t").join("vrtkv_t_nsab");
        std::fs::create_dir_all(&sibling).unwrap();
        let keep = sibling.join("k0");
        std::fs::write(&keep, b"v").unwrap();

        let good = serde_json::json!({
            "storage": "kv", "storageId": "vrtkv_t_ns", "tenantId": "t"
        });
        // max_items=3 < 5 files — first pass incomplete.
        let ack = exec.execute(&op(good.clone())).await;
        assert_eq!(ack.status, "DONE");
        assert_eq!(ack.result.as_ref().unwrap()["complete"], false);
        assert_eq!(ack.result.as_ref().unwrap()["deletedKeys"], 3);
        // Second pass drains the rest + removes the dir → complete.
        let ack = exec.execute(&op(good)).await;
        assert_eq!(ack.status, "DONE");
        assert_eq!(ack.result.as_ref().unwrap()["complete"], true);
        assert_eq!(ack.result.as_ref().unwrap()["deletedKeys"], 2);
        assert!(!ns_dir.exists());
        assert!(keep.exists(), "sibling prefix was touched");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fs_r2_purge_removes_objects_and_staging() {
        let dir = std::env::temp_dir().join(format!("sp-r2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let exec = fs_executor(&dir);
        let bucket = dir.join("r2").join("t").join("vrt-bkt1");
        std::fs::create_dir_all(bucket.join(".multipart").join("u1")).unwrap();
        for i in 0..4 {
            std::fs::write(bucket.join(format!("o{i}")), b"v").unwrap();
        }
        // 4 objects + `.multipart` = 5 entries > max_items(3) — the
        // first pass must report incomplete, the second completes.
        let payload = serde_json::json!({
            "storage": "r2", "storageId": "vrt-bkt1", "tenantId": "t"
        });
        let ack = exec.execute(&op(payload.clone())).await;
        assert_eq!(ack.status, "DONE");
        assert_eq!(ack.result.as_ref().unwrap()["complete"], false);
        let ack = exec.execute(&op(payload)).await;
        assert_eq!(ack.status, "DONE");
        assert_eq!(ack.result.as_ref().unwrap()["complete"], true);
        assert!(!bucket.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KV1/Task A5: a kvrocks backend that cannot be dialed makes
    /// `connect` Err — the caller (cell-controller.rs) logs a structured
    /// error and builds NO executor, so `kv`/`storagePurge` capability
    /// is never ACKed. Fail closed, never "configured ⇒ capable".
    #[tokio::test]
    async fn connect_fails_closed_on_unreachable_kvrocks() {
        let cfg = StoragePurgeConfig {
            kvrocks: Some(vrt_provider::config::KvrocksConfig {
                url: Some("redis://127.0.0.1:1/0".to_string()),
                url_env: None,
                timeout_ms: 100,
                connect_timeout_ms: 100,
                disable_read_retry: false,
            }),
            s3: None,
            kv: None,
            r2: None,
            vectorize: None,
            max_items_per_op: 100,
            timeout_secs: 1,
        };
        match StoragePurgeExecutor::connect(&cfg).await {
            Err(msg) => assert!(msg.contains("kvrocks"), "error names the backend"),
            Ok(_) => panic!("unreachable kvrocks must fail connect"),
        }
    }
}
