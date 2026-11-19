//! In-memory [`KvProvider`] — deterministic fake for tests and single-node
//! development. Namespaces are created lazily per `(tenant, namespace)`.

use crate::context::BindingContext;
use crate::error::ProviderError;
use crate::limits::{KV_MAX_KEY_BYTES, KV_MAX_METADATA_BYTES};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use super::{KvEntry, KvListPage, KvListRow, KvProvider, KvPutOptions};

#[derive(Clone, Debug)]
struct StoredEntry {
    value: Vec<u8>,
    metadata_json: Option<Vec<u8>>,
    expires_at_ms: Option<i64>,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn namespace_key(ctx: &BindingContext) -> (String, String) {
    match ctx.resource() {
        crate::context::BindingResource::KvNamespace { id } => {
            ns_key(ctx.tenant_id(), id)
        }
        // The authorizer guarantees kind-correct resources; anything else
        // is a provider bug, not attacker input.
        _ => unreachable!("KvProvider invoked with non-KV resource"),
    }
}

/// Raw `(tenant, namespace)` map key — the memory backend stores ids
/// verbatim (no encoding needed; the map is not a flat namespace).
fn ns_key(tenant_id: &str, namespace_id: &str) -> (String, String) {
    (tenant_id.to_string(), namespace_id.to_string())
}

fn live(entry: &StoredEntry, now: i64) -> Option<&StoredEntry> {
    match entry.expires_at_ms {
        Some(expires) if expires <= now => None,
        _ => Some(entry),
    }
}

/// `(tenant, namespace) -> key -> entry`.
type NamespaceMap = HashMap<(String, String), BTreeMap<Vec<u8>, StoredEntry>>;

/// Deterministic in-memory KV provider.
#[derive(Debug, Default)]
pub struct MemoryKvProvider {
    namespaces: Mutex<NamespaceMap>,
}

impl MemoryKvProvider {
    /// Empty provider; namespaces materialize on first write.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl KvProvider for MemoryKvProvider {
    async fn get(
        &self,
        ctx: &BindingContext,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError> {
        let ns = namespace_key(ctx);
        self.get_scoped(ns, key).await
    }

    async fn get_many(
        &self,
        ctx: &BindingContext,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError> {
        let ns = namespace_key(ctx);
        self.get_many_scoped(ns, keys).await
    }

    async fn put(
        &self,
        ctx: &BindingContext,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError> {
        let ns = namespace_key(ctx);
        self.put_scoped(ns, key, value, options).await
    }

    async fn delete(&self, ctx: &BindingContext, key: &[u8]) -> Result<(), ProviderError> {
        let ns = namespace_key(ctx);
        self.delete_scoped(ns, key).await
    }

    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError> {
        let ns = namespace_key(ctx);
        self.list_scoped(ns, prefix, after, limit).await
    }
}

impl MemoryKvProvider {
    async fn get_scoped(
        &self,
        ns: (String, String),
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError>{
        let guard = self.namespaces.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        let entry = guard
            .get(&ns)
            .and_then(|ns| ns.get(key))
            .and_then(|entry| live(entry, now));
        Ok(entry.map(|entry| KvEntry {
            value: entry.value.clone(),
            metadata_json: entry.metadata_json.clone(),
            expires_at_ms: entry.expires_at_ms,
        }))
    }

    async fn get_many_scoped(
        &self,
        ns: (String, String),
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError>{
        let guard = self.namespaces.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        let ns = guard.get(&ns);
        let mut aggregate = 0usize;
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            let entry = ns
                .and_then(|ns| ns.get(key.as_slice()))
                .and_then(|e| live(e, now));
            if let Some(entry) = entry {
                aggregate = super::multi_get_budget_add(
                    aggregate,
                    entry.value.len(),
                    entry.metadata_json.as_ref().map_or(0, Vec::len),
                )?;
            }
            entries.push(entry.map(|entry| KvEntry {
                value: entry.value.clone(),
                metadata_json: entry.metadata_json.clone(),
                expires_at_ms: entry.expires_at_ms,
            }));
        }
        Ok(entries)
    }

    async fn put_scoped(
        &self,
        ns: (String, String),
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError>{
        // Stored metadata must be canonical; verify defensively.
        if let Some(metadata) = &options.metadata_json {
            if metadata.len() > KV_MAX_METADATA_BYTES {
                return Err(ProviderError::KvCorrupt);
            }
            let parsed: serde_json::Value =
                serde_json::from_slice(metadata).map_err(|_| ProviderError::KvCorrupt)?;
            if super::frame::canonical_metadata(&parsed).map_err(|_| ProviderError::KvCorrupt)?
                != *metadata
            {
                return Err(ProviderError::KvCorrupt);
            }
        }
        let mut guard = self.namespaces.lock().unwrap_or_else(|e| e.into_inner());
        guard.entry(ns).or_default().insert(
            key.to_vec(),
            StoredEntry {
                value: value.to_vec(),
                metadata_json: options.metadata_json.clone(),
                expires_at_ms: options.expires_at_ms,
            },
        );
        Ok(())
    }

    async fn delete_scoped(&self, ns: (String, String), key: &[u8]) -> Result<(), ProviderError>{
        let mut guard = self.namespaces.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ns) = guard.get_mut(&ns) {
            ns.remove(key);
        }
        Ok(())
    }

    async fn list_scoped(
        &self,
        ns: (String, String),
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError>{
        if limit == 0 || limit > crate::limits::KV_MAX_LIST_LIMIT {
            return Err(ProviderError::KvInvalidOptions);
        }
        if prefix.len() > KV_MAX_KEY_BYTES {
            return Err(ProviderError::KvKeyTooLarge);
        }
        if let Some(after) = after {
            if after.len() > KV_MAX_KEY_BYTES || std::str::from_utf8(after).is_err() {
                return Err(ProviderError::KvCursorInvalid);
            }
        }
        let guard = self.namespaces.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        let empty = BTreeMap::new();
        let ns = guard.get(&ns).unwrap_or(&empty);
        let mut rows = Vec::with_capacity(usize::from(limit) + 1);
        let start = match after {
            Some(after) => std::ops::Bound::Excluded(after.to_vec()),
            None => std::ops::Bound::Included(prefix.as_bytes().to_vec()),
        };
        for (key, entry) in ns.range((start, std::ops::Bound::Unbounded)) {
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            if live(entry, now).is_none() {
                continue;
            }
            if let Some(metadata) = &entry.metadata_json {
                if metadata.len() > KV_MAX_METADATA_BYTES
                    || serde_json::from_slice::<serde_json::Value>(metadata).is_err()
                {
                    return Err(ProviderError::KvCorrupt);
                }
            }
            rows.push(KvListRow {
                key: key.clone(),
                metadata_json: entry.metadata_json.clone(),
                expires_at_ms: entry.expires_at_ms,
            });
            if rows.len() > usize::from(limit) {
                break;
            }
        }
        let complete = rows.len() <= usize::from(limit);
        rows.truncate(usize::from(limit));
        Ok(KvListPage { rows, complete })
    }
}

#[async_trait]
impl super::KvDataBackend for MemoryKvProvider {
    async fn data_get(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError> {
        self.get_scoped(ns_key(tenant_id, namespace_id), key).await
    }

    async fn data_put(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError> {
        self.put_scoped(ns_key(tenant_id, namespace_id), key, value, options)
            .await
    }

    async fn data_delete(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<(), ProviderError> {
        self.delete_scoped(ns_key(tenant_id, namespace_id), key).await
    }

    async fn data_list(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError> {
        self.list_scoped(ns_key(tenant_id, namespace_id), prefix, after, limit)
            .await
    }
}

