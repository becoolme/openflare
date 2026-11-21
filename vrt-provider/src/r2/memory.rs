//! In-memory [`R2Provider`] — deterministic fake for tests and
//! single-node development. Buckets are created lazily per
//! `(tenant, bucket)`; list mirrors the upstream repository's raw-key
//! scan + delimiter collapse so `next_after` is always the last raw key.

use crate::context::BindingContext;
use crate::error::ProviderError;
use crate::limits::{R2_MAX_KEY_BYTES, R2_MAX_LIST_LIMIT};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use super::wire::{
    canonical_custom_metadata, check_min_part_sizes, etag_matches, hash_bytes, multipart_etag,
    part_md5_hex, resolve_range, validate_complete_parts, validate_part_number,
    MultipartCreateResult, R2Condition, R2ListEntry, R2ListPage, R2MultipartCreateOptions,
    R2ObjectMetadata, R2PutOptions, R2Range, R2UploadedPart, R2_MULTIPART_DEFAULT_TTL_MS,
    R2_MULTIPART_MAX_ACTIVE_UPLOADS,
};
use super::{R2GetResult, R2Provider};

#[derive(Clone, Debug)]
struct StoredObject {
    body: Vec<u8>,
    metadata: R2ObjectMetadata,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn bucket_key(ctx: &BindingContext) -> (String, String) {
    match ctx.resource() {
        crate::context::BindingResource::R2Bucket { name } => {
            (ctx.tenant_id().to_string(), name.clone())
        }
        _ => unreachable!("R2Provider invoked with non-R2 resource"),
    }
}

fn list_entry_key(entry: &R2ListEntry) -> &str {
    match entry {
        R2ListEntry::Object(meta) => &meta.key,
        R2ListEntry::DelimitedPrefix(prefix) => prefix,
    }
}

/// One unfinished multipart upload (reference semantics: terminal
/// ops remove the entry — later ops fail `R2_MULTIPART_INVALID`).
#[derive(Debug)]
struct MemoryUpload {
    key: String,
    options: R2MultipartCreateOptions,
    expires_ms: i64,
    /// Running staged total — enforces the per-upload cap at write.
    total_bytes: u64,
    /// `partNumber → (part etag, body)`; re-uploading replaces.
    parts: BTreeMap<i32, (String, Vec<u8>)>,
}

/// Deterministic in-memory R2 provider.
#[derive(Debug)]
pub struct MemoryR2Provider {
    buckets: Mutex<HashMap<(String, String), BTreeMap<String, StoredObject>>>,
    /// `(tenant, bucket) → internal upload id → upload`.
    uploads: Mutex<HashMap<(String, String), HashMap<String, MemoryUpload>>>,
    /// Single-part object byte cap (mirrors `bucket.max_object_bytes`).
    /// Multipart caps each part AND the completed object at this bound.
    max_object_bytes: u64,
    /// Per-tenant (cross-bucket) staged-bytes cap — one tenant cannot
    /// pin unbounded body bytes in unfinished uploads.
    staging_max_bytes_per_tenant: u64,
    /// Per-tenant (cross-bucket) active-upload cap.
    max_active_uploads: usize,
}

impl Default for MemoryR2Provider {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryR2Provider {
    /// Empty provider; buckets materialize on first write.
    pub fn new() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            uploads: Mutex::new(HashMap::new()),
            max_object_bytes: 256 * 1024 * 1024,
            // 4× the object cap: a tenant may hold a few max-size
            // uploads in flight, never a disk/RAM-filling backlog.
            staging_max_bytes_per_tenant: 4 * 256 * 1024 * 1024,
            max_active_uploads: R2_MULTIPART_MAX_ACTIVE_UPLOADS,
        }
    }

    /// Override the per-object byte cap (tests).
    pub fn with_max_object_bytes(mut self, max: u64) -> Self {
        self.max_object_bytes = max;
        self
    }

    /// Override the per-tenant staging byte cap (tests).
    pub fn with_staging_max_bytes_per_tenant(mut self, max: u64) -> Self {
        self.staging_max_bytes_per_tenant = max;
        self
    }

    /// Override the per-tenant active-upload cap (tests).
    pub fn with_max_active_uploads(mut self, max: usize) -> Self {
        self.max_active_uploads = max;
        self
    }
}

#[async_trait]
impl R2Provider for MemoryR2Provider {
    async fn head(
        &self,
        ctx: &BindingContext,
        key: &str,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let guard = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        Ok(guard
            .get(&bucket_key(ctx))
            .and_then(|b| b.get(key))
            .map(|o| o.metadata.clone()))
    }

    async fn get(
        &self,
        ctx: &BindingContext,
        key: &str,
        range: Option<R2Range>,
        only_if: Option<&R2Condition>,
    ) -> Result<R2GetResult, ProviderError> {
        let guard = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let Some(object) = guard.get(&bucket_key(ctx)).and_then(|b| b.get(key)) else {
            return Ok(R2GetResult::Missing);
        };
        if let Some(condition) = only_if {
            if !condition.matches_object(&object.metadata.etag, object.metadata.uploaded) {
                return Ok(R2GetResult::Precondition(object.metadata.clone()));
            }
        }
        let (body, returned_range) = match range {
            Some(range) => {
                let (start, end) = resolve_range(range, object.body.len() as u64)?;
                (
                    object.body[start as usize..=end as usize].to_vec(),
                    Some(R2Range {
                        offset: Some(start),
                        length: Some(end - start + 1),
                        suffix: None,
                    }),
                )
            }
            None => (object.body.clone(), None),
        };
        let mut metadata = object.metadata.clone();
        metadata.range = returned_range;
        Ok(R2GetResult::Body(metadata, body))
    }

    async fn put(
        &self,
        ctx: &BindingContext,
        key: &str,
        body: &[u8],
        options: &R2PutOptions,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        canonical_custom_metadata(&options.custom_metadata)?;
        if body.len() as u64 > self.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let mut guard = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let bucket = guard.entry(bucket_key(ctx)).or_default();
        let current = bucket.get(key);
        let condition_ok = match (&options.only_if, current) {
            (Some(condition), Some(object)) => {
                condition.matches_object(&object.metadata.etag, object.metadata.uploaded)
            }
            (Some(condition), None) => condition.matches_missing(),
            (None, _) => true,
        };
        if !condition_ok {
            return Ok(None);
        }
        let checksums = hash_bytes(body);
        if let Some(expected) = &options.checksum {
            if !checksums.matches(expected) {
                return Err(ProviderError::R2ChecksumMismatch);
            }
        }
        let version = uuid::Uuid::now_v7().hyphenated().to_string();
        let etag = hex::encode(checksums.md5);
        let metadata = R2ObjectMetadata {
            key: key.to_string(),
            version,
            size: body.len() as u64,
            etag: etag.clone(),
            http_etag: format!("\"{etag}\""),
            uploaded: now_ms(),
            http_metadata: Some(options.http_metadata.clone()),
            custom_metadata: Some(options.custom_metadata.clone()),
            range: None,
            checksums: checksums.exposed(options.checksum.as_ref()),
            storage_class: options.storage_class.as_str().to_string(),
            ssec_key_md5: None,
        };
        bucket.insert(
            key.to_string(),
            StoredObject {
                body: body.to_vec(),
                metadata: metadata.clone(),
            },
        );
        Ok(Some(metadata))
    }

    async fn delete(&self, ctx: &BindingContext, keys: &[String]) -> Result<(), ProviderError> {
        let mut guard = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(bucket) = guard.get_mut(&bucket_key(ctx)) {
            for key in keys {
                bucket.remove(key);
            }
        }
        Ok(())
    }

    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
    ) -> Result<R2ListPage, ProviderError> {
        if limit == 0 || limit > R2_MAX_LIST_LIMIT || delimiter.is_some_and(str::is_empty) {
            return Err(ProviderError::R2InvalidOptions);
        }
        if prefix.len() > R2_MAX_KEY_BYTES {
            return Err(ProviderError::R2KeyTooLarge);
        }
        let guard = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let empty = BTreeMap::new();
        let bucket = guard.get(&bucket_key(ctx)).unwrap_or(&empty);
        let start = match after {
            Some(after) => std::ops::Bound::Excluded(after.to_string()),
            None => std::ops::Bound::Included(prefix.to_string()),
        };
        let mut selected: Vec<(R2ListEntry, String)> = Vec::with_capacity(limit.into());
        let mut truncated = false;
        for (raw_key, object) in bucket.range((start, std::ops::Bound::Unbounded)) {
            if !raw_key.starts_with(prefix) {
                break;
            }
            let entry = delimiter
                .and_then(|separator| {
                    raw_key[prefix.len()..].find(separator).map(|position| {
                        R2ListEntry::DelimitedPrefix(
                            raw_key[..prefix.len() + position + separator.len()].to_owned(),
                        )
                    })
                })
                .unwrap_or_else(|| R2ListEntry::Object(object.metadata.clone()));
            if let Some((last_entry, last_raw)) = selected.last_mut() {
                if list_entry_key(last_entry) == list_entry_key(&entry) {
                    *last_raw = raw_key.clone();
                    continue;
                }
            }
            if selected.len() == usize::from(limit) {
                truncated = true;
                break;
            }
            selected.push((entry, raw_key.clone()));
        }
        let next_after = truncated
            .then(|| selected.last().map(|(_, raw)| raw.clone()))
            .flatten();
        let mut objects = Vec::new();
        let mut delimited_prefixes = Vec::new();
        for (entry, _) in selected {
            match entry {
                R2ListEntry::Object(meta) => objects.push(meta),
                R2ListEntry::DelimitedPrefix(prefix) => delimited_prefixes.push(prefix),
            }
        }
        Ok(R2ListPage {
            objects,
            delimited_prefixes,
            next_after,
        })
    }

    async fn create_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        options: &R2MultipartCreateOptions,
    ) -> Result<MultipartCreateResult, ProviderError> {
        canonical_custom_metadata(&options.custom_metadata)?;
        let mut guard = self.uploads.lock().unwrap_or_else(|e| e.into_inner());
        // Bounded lazy GC — expired uploads are reaped on create so a
        // tenant leaking uploads cannot grow the map without bound.
        // Scoped to the caller's tenant (its own leak is what the
        // per-tenant caps meter anyway).
        let now = now_ms();
        let tenant = ctx.tenant_id();
        for ((t, _), uploads) in guard.iter_mut() {
            if t == tenant {
                uploads.retain(|_, u| u.expires_ms > now);
            }
        }
        // Per-tenant caps (cross-bucket): one tenant cannot pin the
        // shared map/body bytes. Counted under the same mutex as the
        // insert — create concurrency can never exceed the cap.
        let active: usize = guard
            .iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, uploads)| uploads.len())
            .sum();
        if active >= self.max_active_uploads {
            return Err(ProviderError::R2Overloaded);
        }
        let staged: u64 = guard
            .iter()
            .filter(|((t, _), _)| t == tenant)
            .flat_map(|(_, uploads)| uploads.values())
            .map(|u| u.total_bytes)
            .sum();
        if staged > self.staging_max_bytes_per_tenant {
            return Err(ProviderError::R2Overloaded);
        }
        let expires_ms = now + R2_MULTIPART_DEFAULT_TTL_MS;
        let upload_id = uuid::Uuid::now_v7().hyphenated().to_string();
        guard.entry(bucket_key(ctx)).or_default().insert(
            upload_id.clone(),
            MemoryUpload {
                key: key.to_string(),
                options: options.clone(),
                expires_ms,
                total_bytes: 0,
                parts: BTreeMap::new(),
            },
        );
        Ok(MultipartCreateResult {
            upload_id,
            expires_at_ms: expires_ms,
        })
    }

    async fn upload_part(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: &[u8],
    ) -> Result<R2UploadedPart, ProviderError> {
        validate_part_number(part_number as i64)?;
        if body.len() as u64 > self.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let mut guard = self.uploads.lock().unwrap_or_else(|e| e.into_inner());
        // Scoped mutable borrow: validate + compute ledgers, then drop
        // before the cross-bucket immutable scan.
        let total = {
            let upload = guard
                .get_mut(&bucket_key(ctx))
                .and_then(|u| u.get_mut(upload_id))
                .filter(|u| u.expires_ms > now_ms())
                .ok_or(ProviderError::R2MultipartInvalid)?;
            if upload.key != key {
                return Err(ProviderError::R2MultipartInvalid);
            }
            // Re-uploading the same partNumber replaces the prior part.
            let replaced = upload
                .parts
                .get(&part_number)
                .map(|(_, b)| b.len() as u64)
                .unwrap_or(0);
            let total = upload.total_bytes - replaced + body.len() as u64;
            if total > self.max_object_bytes {
                return Err(ProviderError::R2ObjectTooLarge);
            }
            total
        };
        // Per-tenant staging byte cap — same fail-closed semantics as
        // the fs backend so the conformance suite sees one contract.
        let tenant_staged: u64 = guard
            .iter()
            .filter(|((t, _), _)| t == ctx.tenant_id())
            .flat_map(|(_, uploads)| uploads.iter())
            .map(|(id, u)| {
                if id == upload_id {
                    total
                } else {
                    u.total_bytes
                }
            })
            .sum();
        if tenant_staged > self.staging_max_bytes_per_tenant {
            return Err(ProviderError::R2Overloaded);
        }
        let upload = guard
            .get_mut(&bucket_key(ctx))
            .and_then(|u| u.get_mut(upload_id))
            .ok_or(ProviderError::R2MultipartInvalid)?;
        upload.total_bytes = total;
        let etag = part_md5_hex(body);
        upload
            .parts
            .insert(part_number, (etag.clone(), body.to_vec()));
        Ok(R2UploadedPart { part_number, etag })
    }

    async fn complete_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
        parts: &[R2UploadedPart],
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        validate_complete_parts(parts)?;
        let mut guard = self.uploads.lock().unwrap_or_else(|e| e.into_inner());
        let upload = guard
            .get_mut(&bucket_key(ctx))
            .and_then(|m| m.get(upload_id))
            .filter(|u| u.expires_ms > now_ms());
        let Some(upload) = upload else {
            // W5-r4 retry idempotency: the upload is gone — consumed by
            // a previous successful complete (local ops are atomic; a
            // retry reaching here means the first call committed).
            // Answer from the object store: same composite etag ⇒ the
            // exact content requested is what was committed.
            let expected = multipart_etag(
                &parts
                    .iter()
                    .map(|p| p.etag.trim_matches('"').to_string())
                    .collect::<Vec<_>>(),
            )?;
            drop(guard);
            let objects = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
            return match objects.get(&bucket_key(ctx)).and_then(|m| m.get(key)) {
                Some(o) if etag_matches(&o.metadata.etag, &expected) => {
                    Ok(Some(o.metadata.clone().completed_multipart()))
                }
                _ => Err(ProviderError::R2MultipartInvalid),
            };
        };
        if upload.key != key {
            return Err(ProviderError::R2MultipartInvalid);
        }
        let mut md5s = Vec::with_capacity(parts.len());
        let mut sizes = Vec::with_capacity(parts.len());
        let mut body = Vec::new();
        for listed in parts {
            let Some((stored_etag, bytes)) = upload.parts.get(&listed.part_number) else {
                return Err(ProviderError::R2MultipartInvalid);
            };
            if !etag_matches(&listed.etag, stored_etag) {
                return Err(ProviderError::R2MultipartInvalid);
            }
            md5s.push(stored_etag.clone());
            sizes.push(bytes.len() as u64);
            body.extend_from_slice(bytes);
        }
        check_min_part_sizes(&sizes)?;
        if body.len() as u64 > self.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let etag = multipart_etag(&md5s)?;
        let checksums = hash_bytes(&body);
        let version = uuid::Uuid::now_v7().hyphenated().to_string();
        let metadata = R2ObjectMetadata {
            key: key.to_string(),
            version,
            size: body.len() as u64,
            etag: etag.clone(),
            http_etag: format!("\"{etag}\""),
            uploaded: now_ms(),
            http_metadata: Some(upload.options.http_metadata.clone()),
            custom_metadata: Some(upload.options.custom_metadata.clone()),
            range: None,
            checksums: checksums.exposed(None),
            storage_class: upload.options.storage_class.as_str().to_string(),
            ssec_key_md5: None,
        };
        let upload = guard
            .get_mut(&bucket_key(ctx))
            .and_then(|m| m.remove(upload_id))
            .expect("upload held above");
        drop(upload);
        drop(guard);
        let mut objects = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        objects.entry(bucket_key(ctx)).or_default().insert(
            key.to_string(),
            StoredObject {
                body,
                metadata: metadata.clone(),
            },
        );
        Ok(Some(metadata.completed_multipart()))
    }

    async fn abort_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ProviderError> {
        let mut guard = self.uploads.lock().unwrap_or_else(|e| e.into_inner());
        let Some(uploads) = guard.get_mut(&bucket_key(ctx)) else {
            return Err(ProviderError::R2MultipartInvalid);
        };
        let upload = uploads
            .get(upload_id)
            .filter(|u| u.expires_ms > now_ms())
            .ok_or(ProviderError::R2MultipartInvalid)?;
        if upload.key != key {
            return Err(ProviderError::R2MultipartInvalid);
        }
        uploads.remove(upload_id);
        Ok(())
    }
}
