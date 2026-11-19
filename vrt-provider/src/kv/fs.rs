//! Filesystem [`KvProvider`] — the standalone-mode backend (T17).
//!
//! Layout: `<root>/<enc(tenant)>/<enc(namespace)>/<sha256(key).hex>`
//! where `enc` is `vrt_securefs::segment_encode` and the filename is a
//! raw sha256 — path traversal is impossible by construction, and
//! tenant/namespace isolation is a directory boundary.
//!
//! Entry file: `[u32 LE header_len][header JSON][value bytes]`
//! `{key_b64, metadata_b64?, expiresAtMs?}` — written via
//! `vrt_securefs::atomic_write` (0600). List reads headers only and
//! applies the SAME keyset ordering as `MemoryKvProvider`.

use crate::context::BindingContext;
use crate::error::ProviderError;
use crate::limits::{KV_MAX_KEY_BYTES, KV_MAX_VALUE_BYTES};
use async_trait::async_trait;
use std::io::{Read, Seek};
use std::path::PathBuf;

use super::{KvEntry, KvListPage, KvListRow, KvProvider, KvPutOptions};

const MAX_HEADER_BYTES: u64 = 64 * 1024;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn ns_dir(root: &std::path::Path, ctx: &BindingContext) -> PathBuf {
    match ctx.resource() {
        crate::context::BindingResource::KvNamespace { id } => {
            ns_dir_for(root, ctx.tenant_id(), id)
        }
        _ => unreachable!("KvProvider invoked with non-KV resource"),
    }
}

/// `<root>/<seg(t)>/<seg(ns)>` — the same boundary the binding-scoped
/// path derives (segment_encode keeps `..`/separators out).
fn ns_dir_for(root: &std::path::Path, tenant_id: &str, namespace_id: &str) -> PathBuf {
    root.join(vrt_securefs::segment_encode(tenant_id))
        .join(vrt_securefs::segment_encode(namespace_id))
}

fn entry_path(dir: &std::path::Path, key: &[u8]) -> PathBuf {
    use sha2::Digest;
    dir.join(hex::encode(sha2::Sha256::digest(key)))
}

fn write_entry_for(path: &std::path::Path, key: &[u8], entry: &KvEntry) -> std::io::Result<()> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let header = serde_json::json!({
        "key": b64.encode(key),
        "metadata": entry.metadata_json.as_ref().map(|m| b64.encode(m)),
        "expiresAtMs": entry.expires_at_ms,
    });
    let hb = serde_json::to_vec(&header).unwrap_or_default();
    let mut out = Vec::with_capacity(4 + hb.len() + entry.value.len());
    out.extend_from_slice(&(hb.len() as u32).to_le_bytes());
    out.extend_from_slice(&hb);
    out.extend_from_slice(&entry.value);
    vrt_securefs::atomic_write(path, &out, 0o600)
}

/// Read the entry header only, leaving `f` positioned at the value.
fn read_header(f: &mut std::fs::File) -> std::io::Result<(Vec<u8>, Option<Vec<u8>>, Option<i64>)> {
    let mut len_buf = [0u8; 4];
    f.read_exact(&mut len_buf)?;
    let header_len = u32::from_le_bytes(len_buf) as u64;
    if header_len > MAX_HEADER_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "header",
        ));
    }
    let mut hb = vec![0u8; header_len as usize];
    f.read_exact(&mut hb)?;
    let header: serde_json::Value = serde_json::from_slice(&hb)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let key = b64
        .decode(header["key"].as_str().unwrap_or_default())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let metadata = header["metadata"]
        .as_str()
        .map(|m| b64.decode(m))
        .transpose()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let expires = header["expiresAtMs"].as_i64();
    Ok((key, metadata, expires))
}

/// Read the entry header + value; `body=false` reads the header only
/// (list/head paths never pull value bytes).
fn read_entry(
    path: &std::path::Path,
    body: bool,
) -> std::io::Result<Option<(Vec<u8>, Option<Vec<u8>>, Option<i64>, Option<Vec<u8>>)>> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let (key, metadata, expires) = read_header(&mut f)?;
    let value = if body {
        let mut v = Vec::new();
        // Cap: a corrupt/oversized entry fails closed rather than
        // blowing memory.
        f.take((KV_MAX_VALUE_BYTES + 1) as u64)
            .read_to_end(&mut v)?;
        Some(v)
    } else {
        None
    };
    Ok(Some((key, metadata, expires, value)))
}

/// RV-R8 #4: open an entry for the `get_many` two-phase read — parses
/// the header and returns the file (positioned at the value) plus the
/// DECLARED value length, so the aggregate budget can trip before a
/// single value byte is read.
fn open_entry_head(
    path: &std::path::Path,
) -> std::io::Result<Option<(std::fs::File, Vec<u8>, Option<Vec<u8>>, Option<i64>, u64)>> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let (key, metadata, expires) = read_header(&mut f)?;
    let declared = f.metadata()?.len().saturating_sub(f.stream_position()?);
    Ok(Some((f, key, metadata, expires, declared)))
}

/// Local-filesystem KV backend (standalone mode).
#[derive(Debug)]
pub struct FsKvProvider {
    root: PathBuf,
}

impl FsKvProvider {
    /// `root` must be `0700` — created if absent (service-owned dir).
    pub fn new(root: PathBuf) -> std::io::Result<Self> {
        vrt_securefs::ensure_secure_dir(&root)?;
        Ok(Self { root })
    }
}

fn live(expires: Option<i64>, now: i64) -> bool {
    expires.is_none_or(|e| e > now)
}

#[async_trait]
impl KvProvider for FsKvProvider {    async fn get(
        &self,
        ctx: &BindingContext,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError> {
        let dir = ns_dir(&self.root, ctx);
        self.get_scoped(&dir, key).await
    }

    async fn get_many(
        &self,
        ctx: &BindingContext,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError> {
        let dir = ns_dir(&self.root, ctx);
        self.get_many_scoped(&dir, keys).await
    }

    async fn put(
        &self,
        ctx: &BindingContext,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError> {
        let dir = ns_dir(&self.root, ctx);
        self.put_scoped(&dir, key, value, options).await
    }

    async fn delete(&self, ctx: &BindingContext, key: &[u8]) -> Result<(), ProviderError> {
        let dir = ns_dir(&self.root, ctx);
        self.delete_scoped(&dir, key).await
    }

    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError> {
        let dir = ns_dir(&self.root, ctx);
        self.list_scoped(&dir, prefix, after, limit, ctx.deadline()).await
    }
}
impl FsKvProvider {
    async fn get_scoped(
        &self,
        dir: &std::path::Path,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError>{
        let path = entry_path(&dir.to_path_buf(), key);
        let Some((_k, metadata, expires, value)) =
            read_entry(&path, true).map_err(|_| ProviderError::KvUnavailable)?
        else {
            return Ok(None);
        };
        if !live(expires, now_ms()) {
            return Ok(None);
        }
        Ok(Some(KvEntry {
            value: value.unwrap_or_default(),
            metadata_json: metadata,
            expires_at_ms: expires,
        }))
    }

    async fn get_many_scoped(
        &self,
        dir: &std::path::Path,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<KvEntry>>, ProviderError>{
        let dir = dir.to_path_buf();
        let now = now_ms();
        let mut aggregate = 0usize;
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            // RV-R8 #4: header + declared size first — an over-budget
            // entry aborts on its header alone, before its value bytes
            // are read from the same open handle.
            let Some((f, _k, metadata, expires, declared)) =
                open_entry_head(&entry_path(&dir, key))
                    .map_err(|_| ProviderError::KvUnavailable)?
            else {
                entries.push(None);
                continue;
            };
            if !live(expires, now) {
                entries.push(None);
                continue;
            }
            let meta_len = metadata.as_ref().map_or(0, Vec::len);
            aggregate = super::multi_get_budget_add(aggregate, declared as usize, meta_len)?;
            // Value read is bounded by the charge: take(declared+1)
            // can never pull more than the accounted size — a file
            // rewritten mid-read fails closed instead of slipping past
            // the budget.
            let mut value = Vec::new();
            f.take(declared + 1)
                .read_to_end(&mut value)
                .map_err(|_| ProviderError::KvUnavailable)?;
            if value.len() as u64 != declared {
                return Err(ProviderError::KvUnavailable);
            }
            entries.push(Some(KvEntry {
                value,
                metadata_json: metadata,
                expires_at_ms: expires,
            }));
        }
        Ok(entries)
    }

    async fn put_scoped(
        &self,
        dir: &std::path::Path,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError>{
        if value.len() > KV_MAX_VALUE_BYTES {
            return Err(ProviderError::KvValueTooLarge);
        }
        let dir = dir.to_path_buf();
        vrt_securefs::ensure_secure_dir(&dir).map_err(|_| ProviderError::KvUnavailable)?;
        write_entry_for(
            &entry_path(&dir, key),
            key,
            &KvEntry {
                value: value.to_vec(),
                metadata_json: options.metadata_json.clone(),
                expires_at_ms: options.expires_at_ms,
            },
        )
        .map_err(|_| ProviderError::KvUnavailable)
    }

    async fn delete_scoped(&self, dir: &std::path::Path, key: &[u8]) -> Result<(), ProviderError>{
        let path = entry_path(&dir.to_path_buf(), key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(ProviderError::KvUnavailable),
        }
    }

    async fn list_scoped(
        &self,
        dir: &std::path::Path,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
        deadline: tokio::time::Instant,
    ) -> Result<KvListPage, ProviderError>{
        if prefix.len() > KV_MAX_KEY_BYTES {
            return Err(ProviderError::KvKeyTooLarge);
        }
        let dir = dir.to_path_buf();
        let now = now_ms();
        let mut rows = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            // Collect (key, meta) pairs, sort by raw key bytes — the
            // on-disk digest order is unrelated, so we order in memory.
            let mut keyed: Vec<(Vec<u8>, Option<Vec<u8>>, Option<i64>)> = Vec::new();
            for e in entries.flatten() {
                if e.file_name().to_str().is_some_and(|n| n.len() != 64) {
                    continue;
                }
                let Some((key, metadata, expires, _)) =
                    read_entry(&e.path(), false).map_err(|_| ProviderError::KvUnavailable)?
                else {
                    continue;
                };
                if live(expires, now) {
                    keyed.push((key, metadata, expires));
                }
                if deadline <= tokio::time::Instant::now() {
                    return Err(ProviderError::KvBusy);
                }
            }
            keyed.sort();
            for (key, metadata, expires) in keyed {
                if !key.starts_with(prefix.as_bytes()) {
                    continue;
                }
                if let Some(after) = after {
                    if key.as_slice() <= after {
                        continue;
                    }
                }
                if rows.len() == usize::from(limit) {
                    return Ok(KvListPage {
                        rows,
                        complete: false,
                    });
                }
                rows.push(KvListRow {
                    key,
                    metadata_json: metadata,
                    expires_at_ms: expires,
                });
                if deadline <= tokio::time::Instant::now() {
                    return Err(ProviderError::KvBusy);
                }
            }
        }
        Ok(KvListPage {
            rows,
            complete: true,
        })
    }
}

#[async_trait]
impl super::KvDataBackend for FsKvProvider {
    async fn data_get(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<Option<KvEntry>, ProviderError> {
        self.get_scoped(&ns_dir_for(&self.root, tenant_id, namespace_id), key)
            .await
    }

    async fn data_put(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
        value: &[u8],
        options: &KvPutOptions,
    ) -> Result<(), ProviderError> {
        self.put_scoped(
            &ns_dir_for(&self.root, tenant_id, namespace_id),
            key,
            value,
            options,
        )
        .await
    }

    async fn data_delete(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        key: &[u8],
    ) -> Result<(), ProviderError> {
        self.delete_scoped(&ns_dir_for(&self.root, tenant_id, namespace_id), key)
            .await
    }

    async fn data_list(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        prefix: &str,
        after: Option<&[u8]>,
        limit: u16,
    ) -> Result<KvListPage, ProviderError> {
        // No binding deadline on the op path — the cell bounds the op
        // end-to-end instead; far-future keeps list semantics identical.
        self.list_scoped(
            &ns_dir_for(&self.root, tenant_id, namespace_id),
            prefix,
            after,
            limit,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .await
    }
}

impl FsKvProvider {
    /// RV-R70: bounded purge of the namespace directory
    /// `<root>/<enc(t)>/<enc(ns)>/` — entries are removed in
    /// `max_files`-bounded passes; the directory itself is removed on
    /// the clean pass. `complete` means the directory is GONE —
    /// never a budget cut.
    pub async fn purge_namespace(
        &self,
        tenant_id: &str,
        namespace_id: &str,
        max_files: usize,
    ) -> Result<crate::purge::PurgeOutcome, ProviderError> {
        if !crate::purge::valid_kv_purge_id(namespace_id)
            || !crate::purge::valid_purge_tenant(tenant_id)
            || max_files == 0
        {
            return Err(ProviderError::KvInvalidOptions);
        }
        let dir = self
            .root
            .join(vrt_securefs::segment_encode(tenant_id))
            .join(vrt_securefs::segment_encode(namespace_id));
        crate::purge::purge_dir_bounded(&dir, max_files).await
    }
}
