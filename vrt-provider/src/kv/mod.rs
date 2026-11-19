//! Typed Workers KV provider boundary plus backends.
//!
//! - [`frame`]: binding-backend wire DTOs, KVS1/KVB1 encoders, key /
//!   metadata / put-option validation matching the upstream engine.
//! - [`memory`]: deterministic in-memory provider (tests, single-node dev).
//! - [`kvrocks`]: Redis-protocol backend for Kvrocks (value + metadata
//!   envelope + sorted index, atomic via MULTI).

pub mod frame;
pub mod fs;
pub mod kvrocks;
pub mod memory;

pub use frame::{
    canonical_metadata, decode_entry, encode_entry, encode_kvb1, encode_kvs1, encode_list_response,
    parse_put_frame, prepare_put_options, validate_key, FrameGetRequest, FrameListRequest,
    FramePutHeader, KeyRequest,
};
pub use kvrocks::KvrocksKvProvider;
pub use memory::MemoryKvProvider;

use crate::context::BindingContext;
use crate::error::ProviderError;
use async_trait::async_trait;
use std::fmt::Debug;

/// A stored KV entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvEntry {
    /// Raw value bytes.
    pub value: Vec<u8>,
    /// Canonical metadata JSON, if present.
    pub metadata_json: Option<Vec<u8>>,
    /// Absolute expiration in unix ms (`None` = persistent).
    pub expires_at_ms: Option<i64>,
}

/// One row in a KV list page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvListRow {
    /// Key bytes (validated UTF-8 upstream grammar).
    pub key: Vec<u8>,
    /// Canonical metadata JSON, if present.
    pub metadata_json: Option<Vec<u8>>,
    /// Absolute expiration in unix ms.
    pub expires_at_ms: Option<i64>,
}

/// A keyset-paginated list page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvListPage {
    /// Rows in ascending key order.
    pub rows: Vec<KvListRow>,
    /// `true` when no further rows exist after this page.
    pub complete: bool,
}

/// Resolved put options (after expiration/ttl and metadata canonicalization).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvPutOptions {
    /// Absolute expiration in unix ms.
    pub expires_at_ms: Option<i64>,
    /// Canonical metadata JSON (`metadataPresent` semantics upstream:
    /// `Some(b"null")` when present-but-absent).
    pub metadata_json: Option<Vec<u8>>,
}

/// RV-R8 #4: multi-get aggregate accounting shared by every backend.
/// Cost is charged against the DECLARED size BEFORE the value bytes
/// move — an oversized batch must trip on metadata alone, never after
/// buffering megabytes of values.
pub(crate) fn multi_get_budget_add(
    aggregate: usize,
    value_len: usize,
    meta_len: usize,
) -> Result<usize, ProviderError> {
    let next = aggregate
        .checked_add(value_len)
        .and_then(|v| v.checked_add(meta_len))
        .and_then(|v| v.checked_add(32))
        .ok_or(ProviderError::KvResponseTooLarge)?;
    if next > crate::limits::KV_MAX_MULTI_GET_RESPONSE_BYTES {
        return Err(ProviderError::KvResponseTooLarge);
    }
    Ok(next)
}

/// Typed KV provider. Every call is scoped by a sealed [`BindingContext`]
/// and must honor `ctx.deadline()`; implementations map backend failures
/// to the `KV_*` family of [`ProviderError`].
#[async_trait]
pub trait KvProvider: Send + Sync + Debug {
    /// Point read; expired entries read as `None`.
    async fn get(&self, ctx: &BindingContext, key: &[u8])
        -> Result<Option<KvEntry>, ProviderError>;
    /// Ordered multi-get; result length equals `keys.len()`.
    async fn get_many(
        &self,
        ctx: &BindingContext,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError>;
    /// Atomic value+metadata write.
    async fn put(
        &self,
        ctx: &BindingContext,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError>;
    /// Delete a key (idempotent — deleting a missing key succeeds).
    async fn delete(&self, ctx: &BindingContext, key: &[u8]) -> Result<(), ProviderError>;
    /// Keyset page: keys `> after` matching `prefix`, ascending, `limit`
    /// already validated `1..=1000`.
    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError>;
}

/// KV1: data-plane surface for the cell-op channel (`KV_*` cell ops) —
/// the same operations as [`KvProvider`] but scoped by an explicit
/// `(tenant_id, namespace_id)` pair instead of a sealed binding
/// context. Cell ops are NAMESPACE-scoped, not worker-scoped: a
/// namespace may hold data while bound to no live worker version, so
/// no binding context exists to grant.
///
/// Callers MUST validate both ids before calling — `tenant_id` via
/// `crate::purge::valid_purge_tenant`, `namespace_id` via
/// `crate::purge::valid_kv_purge_id` (exactly like STORAGE_PURGE);
/// backends still segment-encode both ids before they touch a physical
/// key/dir, so a forged payload can never widen the addressed range.
#[async_trait]
pub trait KvDataBackend: Send + Sync + Debug {
    /// Point read; expired entries read as `None`.
    async fn data_get(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError>;
    /// Atomic value+metadata write.
    async fn data_put(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError>;
    /// Delete a key (idempotent — deleting a missing key succeeds).
    async fn data_delete(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<(), ProviderError>;
    /// Keyset page: keys `> after` matching `prefix`, ascending.
    async fn data_list(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError>;
}
