//! Filesystem [`R2Provider`] — the standalone-mode backend (T17).
//!
//! Layout: `<root>/<enc(tenant)>/<enc(bucket)>/<sha256(key).hex>` —
//! the digest filename makes traversal impossible; bucket isolation
//! is a directory boundary. Entry file:
//! `[u32 LE header_len][metadata JSON][body bytes]`, where the header
//! is the full `R2ObjectMetadata` document — `head`/`list` never
//! touch body bytes. Writes are `atomic_write` (0600); conditional
//! (`onlyIf`) and checksum semantics mirror `MemoryR2Provider`.

use crate::context::BindingContext;
use crate::error::ProviderError;
use crate::limits::{R2_MAX_KEY_BYTES, R2_MAX_LIST_LIMIT};
use async_trait::async_trait;
use std::io::Read;
use std::path::PathBuf;

use super::wire::{
    canonical_custom_metadata, check_min_part_sizes, etag_matches, hash_bytes, multipart_etag,
    part_md5_hex, resolve_range, validate_complete_parts, validate_part_number,
    MultipartCreateResult, R2Condition, R2ListEntry, R2ListPage, R2MultipartCreateOptions,
    R2ObjectMetadata, R2PutOptions, R2Range, R2UploadedPart, R2_MULTIPART_DEFAULT_TTL_MS,
    R2_MULTIPART_MAX_ACTIVE_UPLOADS,
};
use super::{R2GetResult, R2Provider};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MAX_HEADER_BYTES: u64 = 128 * 1024;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn bucket_dir(root: &std::path::Path, ctx: &BindingContext) -> PathBuf {
    match ctx.resource() {
        crate::context::BindingResource::R2Bucket { name } => root
            .join(vrt_securefs::segment_encode(ctx.tenant_id()))
            .join(vrt_securefs::segment_encode(name)),
        _ => unreachable!("R2Provider invoked with non-R2 resource"),
    }
}

fn object_path(dir: &std::path::Path, key: &str) -> PathBuf {
    use sha2::Digest;
    dir.join(hex::encode(sha2::Sha256::digest(key.as_bytes())))
}

fn write_object(
    path: &std::path::Path,
    metadata: &R2ObjectMetadata,
    body: &[u8],
) -> std::io::Result<()> {
    let hb = serde_json::to_vec(metadata)?;
    let mut out = Vec::with_capacity(4 + hb.len() + body.len());
    out.extend_from_slice(&(hb.len() as u32).to_le_bytes());
    out.extend_from_slice(&hb);
    out.extend_from_slice(body);
    vrt_securefs::atomic_write(path, &out, 0o600)
}

/// `(metadata, body)` — `body=None` reads the header only.
fn read_object(
    path: &std::path::Path,
    body: bool,
) -> std::io::Result<Option<(R2ObjectMetadata, Option<Vec<u8>>)>> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
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
    let metadata: R2ObjectMetadata = serde_json::from_slice(&hb)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let body = if body {
        let mut v = Vec::new();
        f.read_to_end(&mut v)?;
        Some(v)
    } else {
        None
    };
    Ok(Some((metadata, body)))
}

fn list_entry_key(entry: &R2ListEntry) -> &str {
    match entry {
        R2ListEntry::Object(meta) => &meta.key,
        R2ListEntry::DelimitedPrefix(prefix) => prefix,
    }
}

/// Persisted staging metadata — survives provider restarts; ops
/// resume against the on-disk state. `total_bytes` tracks staged
/// capacity so the per-upload cap is enforced incrementally.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FsUploadMeta {
    key: String,
    http_metadata: super::wire::R2HttpMetadata,
    custom_metadata: std::collections::BTreeMap<String, String>,
    storage_class: String,
    expires_ms: i64,
    total_bytes: u64,
}

/// Multipart staging lives in `<bucket>/.multipart/<upload_id>/`:
/// `meta.json` + `parts/<NNNNNNNN>` (raw bytes; etag recomputed on
/// read). `.multipart` never collides with object names (sha256 hex).
const MULTIPART_DIR: &str = ".multipart";

/// Internal upload ids are uuid v7 — strict shape check before ANY
/// path join keeps the staging namespace traversal-proof.
fn valid_upload_id(id: &str) -> bool {
    id.len() == 36
        && id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase() || c == '-')
}

fn staging_dir(bucket: &std::path::Path) -> PathBuf {
    bucket.join(MULTIPART_DIR)
}

fn upload_dir(bucket: &std::path::Path, id: &str) -> PathBuf {
    staging_dir(bucket).join(id)
}

fn read_meta(dir: &std::path::Path) -> std::io::Result<Option<FsUploadMeta>> {
    match std::fs::read(dir.join("meta.json")) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_meta(dir: &std::path::Path, meta: &FsUploadMeta) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(meta)?;
    vrt_securefs::atomic_write(&dir.join("meta.json"), &bytes, 0o600)
}

/// Tenant data root (`<root>/<enc(tenant)>`) — the per-tenant quota
/// and create-count accounting scope (cross-bucket).
fn tenant_dir(root: &std::path::Path, ctx: &BindingContext) -> PathBuf {
    root.join(vrt_securefs::segment_encode(ctx.tenant_id()))
}

/// Sum of part-file sizes under `<upload>/parts` — the on-disk truth
/// used for every byte ledger (meta.total_bytes is only a hint
/// written back for the gauge; a part whose meta write failed can
/// never desynchronise the cap check).
fn parts_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir.join("parts"))
        .map(|parts| {
            parts
                .flatten()
                .filter_map(|p| p.metadata().ok().map(|m| m.len()))
                .sum()
        })
        .unwrap_or(0)
}

/// Shared mutable state — an `Arc` so `spawn_blocking` closures can
/// own a handle without borrowing the provider across `.await`.
#[derive(Debug)]
struct FsR2Inner {
    root: PathBuf,
    max_object_bytes: u64,
    /// Unfinished-upload lifetime (configurable; default 7 days).
    upload_ttl_ms: i64,
    /// Per-tenant (cross-bucket) staged-bytes ceiling enforced at
    /// `uploadPart` (default `4 × max_object_bytes` — a few max-size
    /// uploads in flight, never a disk-filling backlog).
    staging_max_bytes_per_tenant: u64,
    /// Per-tenant (cross-bucket) active-upload ceiling enforced at
    /// `create` (default 64).
    max_active_uploads: usize,
    /// Per-upload serialisation for complete/abort/part ops.
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Per-tenant serialisation for quota accounting (create count +
    /// staged-bytes checks). Lock order is strictly tenant → upload;
    /// nothing holding an upload lock may acquire a tenant lock.
    tenant_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl FsR2Inner {
    /// Lock serialising ops on one upload; removed on terminal ops.
    fn upload_lock(&self, bucket: &std::path::Path, id: &str) -> Arc<Mutex<()>> {
        let mut map = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(format!("{}/{}", bucket.display(), id))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn drop_upload_lock(&self, bucket: &std::path::Path, id: &str) {
        let mut map = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(&format!("{}/{}", bucket.display(), id));
    }

    /// Lock serialising per-tenant quota decisions (create count and
    /// staged-bytes checks are atomic under it).
    fn tenant_lock(&self, tdir: &std::path::Path) -> Arc<Mutex<()>> {
        let mut map = self.tenant_locks.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(format!("{}", tdir.display()))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Staged bytes for the whole tenant across every bucket —
    /// disk truth, same ledger as the part/quota checks.
    fn tenant_staging_bytes(&self, tdir: &std::path::Path) -> u64 {
        let mut total = 0u64;
        let Ok(buckets) = std::fs::read_dir(tdir) else {
            return 0;
        };
        for bucket in buckets.flatten() {
            let Ok(uploads) = std::fs::read_dir(staging_dir(&bucket.path())) else {
                continue;
            };
            for entry in uploads.flatten() {
                total = total.saturating_add(parts_bytes(&entry.path()));
            }
        }
        total
    }
}

/// Local-filesystem R2 backend (standalone mode).
///
/// Multipart ops run inside `spawn_blocking` (all staging IO and both
/// lock domains live in the blocking closure) so slow disks can never
/// stall tokio worker threads; a dropped join handle still lets the
/// closure finish — every write is atomic-rename, so a cancelled
/// upload_part either commits whole or not at all.
#[derive(Debug)]
pub struct FsR2Provider {
    inner: Arc<FsR2Inner>,
}

impl FsR2Provider {
    /// `root` must be `0700` — created if absent (service-owned dir).
    pub fn new(root: PathBuf, max_object_bytes: u64) -> std::io::Result<Self> {
        vrt_securefs::ensure_secure_dir(&root)?;
        Ok(Self {
            inner: Arc::new(FsR2Inner {
                root,
                max_object_bytes,
                upload_ttl_ms: R2_MULTIPART_DEFAULT_TTL_MS,
                staging_max_bytes_per_tenant: max_object_bytes.saturating_mul(4),
                max_active_uploads: R2_MULTIPART_MAX_ACTIVE_UPLOADS,
                locks: Mutex::new(HashMap::new()),
                tenant_locks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Override the unfinished-upload TTL (GC horizon).
    pub fn with_upload_ttl_ms(mut self, ttl_ms: i64) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("builder runs before sharing")
            .upload_ttl_ms = ttl_ms;
        self
    }

    /// Override the per-tenant staged-bytes ceiling.
    pub fn with_staging_max_bytes_per_tenant(mut self, max: u64) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("builder runs before sharing")
            .staging_max_bytes_per_tenant = max;
        self
    }

    /// Override the per-tenant active-upload ceiling.
    pub fn with_max_active_uploads(mut self, max: usize) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("builder runs before sharing")
            .max_active_uploads = max;
        self
    }

    /// Bounded staging GC: remove `.multipart/<id>` dirs whose
    /// `expires_ms` passed, plus orphans — dirs with a missing or
    /// corrupt `meta.json` (or a non-upload-id name) whose directory
    /// mtime is older than the TTL. Without the mtime fallback an
    /// upload whose meta write failed at create would pin staging
    /// bytes forever. Returns a per-reason [`SweepReport`]. Safe to
    /// run concurrently — each removal takes the per-upload lock and
    /// re-checks under it.
    pub fn sweep_staging(&self) -> std::io::Result<SweepReport> {
        self.inner.sweep_staging()
    }

    /// Current staged byte totals `(tenant, bucket, bytes)` — disk
    /// truth (sums `parts/` files), emitted by the binary as the
    /// `staging_bytes` gauge.
    pub fn staging_bytes(&self) -> std::io::Result<Vec<(String, String, u64)>> {
        self.inner.staging_bytes()
    }
}

/// Staging-GC outcome split by reclamation reason.
#[derive(Clone, Copy, Debug, Default)]
pub struct SweepReport {
    /// Uploads removed because `meta.expires_ms` passed.
    pub expired: u64,
    /// Uploads removed because meta was absent/corrupt (or the dir
    /// name was never a valid upload id) and dir mtime aged past TTL.
    pub orphans: u64,
    /// Part bytes freed by all removals.
    pub bytes_freed: u64,
}

impl FsR2Inner {
    fn sweep_staging(&self) -> std::io::Result<SweepReport> {
        let now = now_ms();
        let mut report = SweepReport::default();
        let Ok(tenants) = std::fs::read_dir(&self.root) else {
            return Ok(report);
        };
        for tenant in tenants.flatten() {
            let Ok(buckets) = std::fs::read_dir(tenant.path()) else {
                continue;
            };
            for bucket in buckets.flatten() {
                let staging = staging_dir(&bucket.path());
                let Ok(uploads) = std::fs::read_dir(&staging) else {
                    continue;
                };
                for entry in uploads.flatten() {
                    let dir = entry.path();
                    let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                        continue;
                    };
                    enum Stale {
                        Expired,
                        Orphan,
                    }
                    // A dir is reclaimable when its meta says expired,
                    // or when it can never be a live upload (bad name /
                    // unreadable meta) and nothing has touched it
                    // within the TTL. mtime tracks staging activity —
                    // a stalled-but-written upload keeps refreshing it.
                    let stale = match read_meta(&dir) {
                        Ok(Some(meta)) if meta.expires_ms <= now => Some(Stale::Expired),
                        Ok(Some(_)) => None,
                        _ if !valid_upload_id(&name)
                            || matches!(read_meta(&dir), Ok(None) | Err(_)) =>
                        {
                            let aged = entry
                                .metadata()
                                .and_then(|m| m.modified())
                                .ok()
                                .and_then(|mtime| {
                                    mtime
                                        .elapsed()
                                        .ok()
                                        .map(|e| e.as_millis() as i64 > self.upload_ttl_ms)
                                })
                                .unwrap_or(false);
                            aged.then_some(Stale::Orphan)
                        }
                        _ => None,
                    };
                    let Some(stale) = stale else {
                        continue;
                    };
                    let lock = self.upload_lock(&bucket.path(), &name);
                    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                    let bytes = parts_bytes(&dir);
                    // Re-check under the lock — a concurrent op may have
                    // completed or refreshed the upload since listing.
                    let still_stale = match read_meta(&dir) {
                        Ok(Some(meta)) => meta.expires_ms <= now,
                        Ok(None) | Err(_) => matches!(stale, Stale::Orphan),
                    };
                    if still_stale && std::fs::remove_dir_all(&dir).is_ok() {
                        match stale {
                            Stale::Expired => report.expired += 1,
                            Stale::Orphan => report.orphans += 1,
                        }
                        report.bytes_freed += bytes;
                        self.drop_upload_lock(&bucket.path(), &name);
                    }
                }
            }
        }
        Ok(report)
    }

    fn staging_bytes(&self) -> std::io::Result<Vec<(String, String, u64)>> {
        let mut out = Vec::new();
        let Ok(tenants) = std::fs::read_dir(&self.root) else {
            return Ok(out);
        };
        for tenant in tenants.flatten() {
            let Some(tname) = tenant.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(buckets) = std::fs::read_dir(tenant.path()) else {
                continue;
            };
            for bucket in buckets.flatten() {
                let staging = staging_dir(&bucket.path());
                let Ok(uploads) = std::fs::read_dir(&staging) else {
                    continue;
                };
                let mut total = 0u64;
                for entry in uploads.flatten() {
                    total = total.saturating_add(parts_bytes(&entry.path()));
                }
                if let (Some(t), Some(b)) = (
                    vrt_securefs::segment_decode(&tname),
                    bucket
                        .file_name()
                        .to_str()
                        .and_then(vrt_securefs::segment_decode),
                ) {
                    out.push((t, b, total));
                }
            }
        }
        Ok(out)
    }
}

/// Incremental object checksum state — the streaming equivalent of
/// `hash_bytes`, so `complete` never materialises the whole object.
struct ObjHasher {
    md5: md5::Context,
    sha1: sha1::Sha1,
    sha256: sha2::Sha256,
    sha384: sha2::Sha384,
    sha512: sha2::Sha512,
}

impl ObjHasher {
    fn new() -> Self {
        use sha2::Digest;
        Self {
            md5: md5::Context::new(),
            sha1: sha1::Sha1::new(),
            sha256: sha2::Sha256::new(),
            sha384: sha2::Sha384::new(),
            sha512: sha2::Sha512::new(),
        }
    }
    fn update(&mut self, chunk: &[u8]) {
        use sha2::Digest;
        self.md5.consume(chunk);
        self.sha1.update(chunk);
        self.sha256.update(chunk);
        self.sha384.update(chunk);
        self.sha512.update(chunk);
    }
    fn finish(self) -> super::wire::R2ComputedChecksums {
        use sha2::Digest;
        super::wire::R2ComputedChecksums {
            md5: self.md5.compute().0,
            sha1: self.sha1.finalize().into(),
            sha256: self.sha256.finalize().into(),
            sha384: self.sha384.finalize().into(),
            sha512: self.sha512.finalize().into(),
        }
    }
}

/// `write_object` layout (`[u32 hdr][JSON hdr][body]`) without
/// buffering the body: header+json first, then a streamed copy from
/// `body_src`. Same atomic contract as `vrt_securefs::atomic_write`
/// (unique tmp → fsync → mode → rename → dir fsync).
fn write_object_streaming(
    path: &std::path::Path,
    metadata: &R2ObjectMetadata,
    body_src: &std::path::Path,
) -> std::io::Result<()> {
    use std::io::Write;
    let hb = serde_json::to_vec(metadata)?;
    let dir = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
    })?;
    let tmp = dir.join(format!(
        ".{}.tmp-mp-{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        uuid::Uuid::now_v7().simple()
    ));
    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(&(hb.len() as u32).to_le_bytes())?;
        file.write_all(&hb)?;
        let mut src = std::fs::File::open(body_src)?;
        std::io::copy(&mut src, &mut file)?;
        file.sync_all()?;
        drop(file);
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result?;
    let dir_file = std::fs::File::open(dir)?;
    dir_file.sync_all()
}

/// Streaming concat of listed part files into `tmp_out`: verifies
/// each supplied etag against the streamed bytes, returns per-part
/// md5 hex, sizes, and the object checksums. Early-aborts when the
/// running total exceeds `cap`.
fn concat_parts(
    dir: &std::path::Path,
    tmp_out: &std::path::Path,
    parts: &[R2UploadedPart],
    cap: u64,
) -> Result<(Vec<String>, Vec<u64>, super::wire::R2ComputedChecksums), ProviderError> {
    use std::io::{Read, Write};
    let mut tmp = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp_out)
        .map_err(|_| ProviderError::R2ProviderUnavailable)?;
    let mut md5s = Vec::with_capacity(parts.len());
    let mut sizes = Vec::with_capacity(parts.len());
    let mut hasher = ObjHasher::new();
    let mut total = 0u64;
    let result = (|| -> Result<_, ProviderError> {
        let mut buf = vec![0u8; 64 * 1024];
        for listed in parts {
            let part_path = dir.join("parts").join(format!("{:08}", listed.part_number));
            let mut file = std::fs::File::open(&part_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    ProviderError::R2MultipartInvalid
                } else {
                    ProviderError::R2ProviderUnavailable
                }
            })?;
            let mut part_md5 = md5::Context::new();
            let mut part_size = 0u64;
            loop {
                let n = file
                    .read(&mut buf)
                    .map_err(|_| ProviderError::R2ProviderUnavailable)?;
                if n == 0 {
                    break;
                }
                let chunk = &buf[..n];
                tmp.write_all(chunk)
                    .map_err(|_| ProviderError::R2ProviderUnavailable)?;
                part_md5.consume(chunk);
                hasher.update(chunk);
                part_size += n as u64;
                total += n as u64;
                if total > cap {
                    return Err(ProviderError::R2ObjectTooLarge);
                }
            }
            let etag = format!("{:x}", part_md5.compute());
            if !etag_matches(&listed.etag, &etag) {
                return Err(ProviderError::R2MultipartInvalid);
            }
            md5s.push(etag);
            sizes.push(part_size);
        }
        tmp.sync_all()
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        Ok((md5s, sizes, hasher.finish()))
    })();
    if result.is_err() {
        drop(tmp);
        let _ = std::fs::remove_file(tmp_out);
    }
    result
}

#[async_trait]
impl R2Provider for FsR2Provider {
    async fn head(
        &self,
        ctx: &BindingContext,
        key: &str,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let path = object_path(&bucket_dir(&self.inner.root, ctx), key);
        Ok(read_object(&path, false)
            .map_err(|_| ProviderError::R2ProviderUnavailable)?
            .map(|(m, _)| m))
    }

    async fn get(
        &self,
        ctx: &BindingContext,
        key: &str,
        range: Option<R2Range>,
        only_if: Option<&R2Condition>,
    ) -> Result<R2GetResult, ProviderError> {
        let path = object_path(&bucket_dir(&self.inner.root, ctx), key);
        // Read metadata first; body only when needed (after onlyIf
        // passes and a body is actually requested).
        let Some((meta, _)) =
            read_object(&path, false).map_err(|_| ProviderError::R2ProviderUnavailable)?
        else {
            return Ok(R2GetResult::Missing);
        };
        if let Some(condition) = only_if {
            if !condition.matches_object(&meta.etag, meta.uploaded) {
                return Ok(R2GetResult::Precondition(meta));
            }
        }
        if range.is_none() {
            // Whole-body read.
            let Some((meta, body)) =
                read_object(&path, true).map_err(|_| ProviderError::R2ProviderUnavailable)?
            else {
                return Ok(R2GetResult::Missing);
            };
            return Ok(R2GetResult::Body(meta, body.unwrap_or_default()));
        }
        let Some((_m, body)) =
            read_object(&path, true).map_err(|_| ProviderError::R2ProviderUnavailable)?
        else {
            return Ok(R2GetResult::Missing);
        };
        let body = body.unwrap_or_default();
        let (start, end) = resolve_range(range.unwrap(), body.len() as u64)?;
        let mut meta = meta;
        meta.range = Some(R2Range {
            offset: Some(start),
            length: Some(end - start + 1),
            suffix: None,
        });
        Ok(R2GetResult::Body(
            meta,
            body[start as usize..=end as usize].to_vec(),
        ))
    }

    async fn put(
        &self,
        ctx: &BindingContext,
        key: &str,
        body: &[u8],
        options: &R2PutOptions,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        canonical_custom_metadata(&options.custom_metadata)?;
        if body.len() as u64 > self.inner.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let dir = bucket_dir(&self.inner.root, ctx);
        vrt_securefs::ensure_secure_dir(&dir).map_err(|_| ProviderError::R2ProviderUnavailable)?;
        let path = object_path(&dir, key);
        let current = read_object(&path, false)
            .map_err(|_| ProviderError::R2ProviderUnavailable)?
            .map(|(m, _)| m);
        let condition_ok = match (&options.only_if, &current) {
            (Some(condition), Some(meta)) => condition.matches_object(&meta.etag, meta.uploaded),
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
        write_object(&path, &metadata, body).map_err(|_| ProviderError::R2ProviderUnavailable)?;
        Ok(Some(metadata))
    }

    async fn delete(&self, ctx: &BindingContext, keys: &[String]) -> Result<(), ProviderError> {
        let dir = bucket_dir(&self.inner.root, ctx);
        for key in keys {
            match std::fs::remove_file(object_path(&dir, key)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(ProviderError::R2ProviderUnavailable),
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
        let dir = bucket_dir(&self.inner.root, ctx);
        // Enumerate headers, sort raw keys, then apply the same
        // delimiter-collapse + next_after semantics as the memory
        // backend.
        let mut metas: Vec<R2ObjectMetadata> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                if e.file_name().to_str().is_some_and(|n| n.len() != 64) {
                    continue;
                }
                if let Some((meta, _)) = read_object(&e.path(), false)
                    .map_err(|_| ProviderError::R2ProviderUnavailable)?
                {
                    metas.push(meta);
                }
                if ctx.deadline() <= tokio::time::Instant::now() {
                    return Err(ProviderError::R2Overloaded);
                }
            }
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        let start = after.map(|a| a.to_string());
        let mut selected: Vec<(R2ListEntry, String)> = Vec::with_capacity(limit.into());
        let mut truncated = false;
        for meta in metas {
            let raw_key = meta.key.clone();
            if !raw_key.starts_with(prefix) {
                continue;
            }
            if let Some(after) = &start {
                if raw_key.as_str() <= after.as_str() {
                    continue;
                }
            }
            let entry = delimiter
                .and_then(|separator| {
                    raw_key[prefix.len()..].find(separator).map(|position| {
                        R2ListEntry::DelimitedPrefix(
                            raw_key[..prefix.len() + position + separator.len()].to_owned(),
                        )
                    })
                })
                .unwrap_or_else(|| R2ListEntry::Object(meta));
            if let Some((last_entry, last_raw)) = selected.last_mut() {
                if list_entry_key(last_entry) == list_entry_key(&entry) {
                    *last_raw = raw_key;
                    continue;
                }
            }
            if selected.len() == usize::from(limit) {
                truncated = true;
                break;
            }
            selected.push((entry, raw_key));
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
        let inner = self.inner.clone();
        let tdir = tenant_dir(&inner.root, ctx);
        let bdir = bucket_dir(&inner.root, ctx);
        let key = key.to_string();
        let options = options.clone();
        tokio::task::spawn_blocking(move || {
            inner.create_multipart_block(&tdir, &bdir, &key, &options)
        })
        .await
        .map_err(|_| ProviderError::R2ProviderUnavailable)?
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
        if body.len() as u64 > self.inner.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        if !valid_upload_id(upload_id) {
            return Err(ProviderError::R2MultipartInvalid);
        }
        let inner = self.inner.clone();
        let tdir = tenant_dir(&inner.root, ctx);
        let bdir = bucket_dir(&inner.root, ctx);
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let body = body.to_vec();
        tokio::task::spawn_blocking(move || {
            inner.upload_part_block(&tdir, &bdir, &key, &upload_id, part_number, &body)
        })
        .await
        .map_err(|_| ProviderError::R2ProviderUnavailable)?
    }

    async fn complete_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
        parts: &[R2UploadedPart],
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        validate_complete_parts(parts)?;
        if !valid_upload_id(upload_id) {
            return Err(ProviderError::R2MultipartInvalid);
        }
        let inner = self.inner.clone();
        let bdir = bucket_dir(&inner.root, ctx);
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let parts = parts.to_vec();
        tokio::task::spawn_blocking(move || {
            inner.complete_multipart_block(&bdir, &key, &upload_id, &parts)
        })
        .await
        .map_err(|_| ProviderError::R2ProviderUnavailable)?
    }

    async fn abort_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ProviderError> {
        if !valid_upload_id(upload_id) {
            return Err(ProviderError::R2MultipartInvalid);
        }
        let inner = self.inner.clone();
        let bdir = bucket_dir(&inner.root, ctx);
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        tokio::task::spawn_blocking(move || inner.abort_multipart_block(&bdir, &key, &upload_id))
            .await
            .map_err(|_| ProviderError::R2ProviderUnavailable)?
    }
}

/// Blocking multipart bodies — every fs IO and both lock domains run
/// on the blocking threadpool inside `spawn_blocking`.
impl FsR2Inner {
    fn create_multipart_block(
        &self,
        tdir: &std::path::Path,
        bdir: &std::path::Path,
        key: &str,
        options: &R2MultipartCreateOptions,
    ) -> Result<MultipartCreateResult, ProviderError> {
        // Per-tenant lock makes the active-upload count + dir create
        // atomic across buckets — concurrent creates can never slip
        // past the ceiling.
        let tlock = self.tenant_lock(tdir);
        let _tguard = tlock.lock().unwrap_or_else(|e| e.into_inner());
        vrt_securefs::ensure_secure_dir(bdir).map_err(|_| ProviderError::R2ProviderUnavailable)?;
        let staging = staging_dir(bdir);
        vrt_securefs::ensure_secure_dir(&staging)
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        // Cross-bucket count under the tenant lock; expired entries
        // are reaped inline.
        let now = now_ms();
        let mut active = 0usize;
        let Ok(buckets) = std::fs::read_dir(tdir) else {
            return Err(ProviderError::R2ProviderUnavailable);
        };
        for bucket in buckets.flatten() {
            let Ok(entries) = std::fs::read_dir(staging_dir(&bucket.path())) else {
                continue;
            };
            for entry in entries.flatten() {
                match read_meta(&entry.path()) {
                    Ok(Some(meta)) if meta.expires_ms <= now => {
                        // Inline reaping must not violate the tenant →
                        // upload lock order nor race an in-flight op:
                        // a complete holding this upload's lock may be
                        // mid-concat (expiry is checked before concat
                        // but concat can cross the deadline). try_lock
                        // never blocks — a busy upload is skipped here
                        // and reaped by the periodic sweep instead.
                        let name = entry.file_name().to_string_lossy().into_owned();
                        let lock = self.upload_lock(&bucket.path(), &name);
                        let Ok(guard) = lock.try_lock() else {
                            continue;
                        };
                        // Re-check under the upload lock — the entry
                        // may have been removed or refreshed since the
                        // directory listing.
                        let still_expired = matches!(
                            read_meta(&entry.path()),
                            Ok(Some(m)) if m.expires_ms <= now_ms()
                        );
                        if still_expired {
                            let _ = std::fs::remove_dir_all(entry.path());
                        }
                        drop(guard);
                        self.drop_upload_lock(&bucket.path(), &name);
                    }
                    Ok(Some(_)) => active += 1,
                    _ => {}
                }
                if active >= self.max_active_uploads {
                    return Err(ProviderError::R2Overloaded);
                }
            }
        }
        let expires_ms = now + self.upload_ttl_ms;
        let upload_id = uuid::Uuid::now_v7().hyphenated().to_string();
        let dir = upload_dir(bdir, &upload_id);
        std::fs::create_dir_all(dir.join("parts"))
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        write_meta(
            &dir,
            &FsUploadMeta {
                key: key.to_string(),
                http_metadata: options.http_metadata.clone(),
                custom_metadata: options.custom_metadata.clone(),
                storage_class: options.storage_class.as_str().to_string(),
                expires_ms,
                total_bytes: 0,
            },
        )
        .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        Ok(MultipartCreateResult {
            upload_id,
            expires_at_ms: expires_ms,
        })
    }

    fn upload_part_block(
        &self,
        tdir: &std::path::Path,
        bdir: &std::path::Path,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: &[u8],
    ) -> Result<R2UploadedPart, ProviderError> {
        // Lock order tenant → upload: the staged-bytes decision must
        // see a stable cross-bucket ledger while the per-upload lock
        // serialises part-file replacement.
        let tlock = self.tenant_lock(tdir);
        let _tguard = tlock.lock().unwrap_or_else(|e| e.into_inner());
        let dir = upload_dir(bdir, upload_id);
        let lock = self.upload_lock(bdir, upload_id);
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let Some(mut meta) = read_meta(&dir).map_err(|_| ProviderError::R2ProviderUnavailable)?
        else {
            return Err(ProviderError::R2MultipartInvalid);
        };
        if meta.key != key || meta.expires_ms <= now_ms() {
            return Err(ProviderError::R2MultipartInvalid);
        }
        let part_path = dir.join("parts").join(format!("{part_number:08}"));
        let replaced = part_path.metadata().map(|m| m.len()).unwrap_or(0);
        // P1-1: both ledgers read DISK truth under the locks — a part
        // file written but whose meta write failed can never make
        // `replaced > total` (u64 underflow) or desynchronise the cap.
        let staged = parts_bytes(&dir);
        let total = staged - replaced + body.len() as u64;
        if total > self.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let tenant_staged = self.tenant_staging_bytes(tdir);
        if tenant_staged - replaced + body.len() as u64 > self.staging_max_bytes_per_tenant {
            return Err(ProviderError::R2Overloaded);
        }
        // tmp write + fsync + rename via atomic_write; re-uploading a
        // partNumber replaces the previous part file.
        vrt_securefs::atomic_write(&part_path, body, 0o600)
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        // meta.total_bytes is kept as a gauge hint only — the ledger
        // above is always disk-derived, so this write-back also
        // self-heals any drift left by an earlier partial failure.
        meta.total_bytes = total;
        write_meta(&dir, &meta).map_err(|_| ProviderError::R2ProviderUnavailable)?;
        Ok(R2UploadedPart {
            part_number,
            etag: part_md5_hex(body),
        })
    }

    fn complete_multipart_block(
        &self,
        bdir: &std::path::Path,
        key: &str,
        upload_id: &str,
        parts: &[R2UploadedPart],
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let dir = upload_dir(bdir, upload_id);
        let lock = self.upload_lock(bdir, upload_id);
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let Some(meta) = read_meta(&dir).map_err(|_| ProviderError::R2ProviderUnavailable)? else {
            // W5-r4 retry idempotency: staging gone means a previous
            // complete already committed atomically (or an abort/GC
            // ran) — the committed object decides. Its stored etag
            // matching the composite derived from THIS request's part
            // list ⇒ the exact requested content is what was
            // committed → idempotent success; otherwise the typed
            // invalid stands.
            let expected = multipart_etag(
                &parts
                    .iter()
                    .map(|p| p.etag.trim_matches('"').to_string())
                    .collect::<Vec<_>>(),
            )?;
            let committed = read_object(&object_path(bdir, key), false)
                .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            return match committed {
                Some((m, _)) if etag_matches(&m.etag, &expected) => {
                    Ok(Some(m.completed_multipart()))
                }
                _ => Err(ProviderError::R2MultipartInvalid),
            };
        };
        if meta.key != key || meta.expires_ms <= now_ms() {
            return Err(ProviderError::R2MultipartInvalid);
        }
        // Stream-concat inside the staging dir: per-part md5 + object
        // checksums computed on the fly; memory stays O(64KiB), never
        // O(2×object).
        // Unique tmp name per attempt — a crash between concat and
        // cleanup must not wedge the upload: the stale file rides out
        // with the staging dir on the next complete/abort/GC.
        let tmp_body = dir.join(format!(".concat-{}.tmp", uuid::Uuid::now_v7().simple()));
        let (md5s, sizes, checksums) = concat_parts(&dir, &tmp_body, parts, self.max_object_bytes)?;
        check_min_part_sizes(&sizes)?;
        let size = sizes.iter().sum::<u64>();
        let etag = multipart_etag(&md5s)?;
        let version = uuid::Uuid::now_v7().hyphenated().to_string();
        let metadata = R2ObjectMetadata {
            key: key.to_string(),
            version,
            size,
            etag: etag.clone(),
            http_etag: format!("\"{etag}\""),
            uploaded: now_ms(),
            http_metadata: Some(meta.http_metadata.clone()),
            custom_metadata: Some(meta.custom_metadata.clone()),
            range: None,
            checksums: checksums.exposed(None),
            storage_class: meta.storage_class.clone(),
            ssec_key_md5: None,
        };
        // Header + streamed body into a tmp object then atomic rename —
        // the final file appears whole or not at all.
        write_object_streaming(&object_path(bdir, key), &metadata, &tmp_body)
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        let _ = std::fs::remove_file(&tmp_body);
        let _ = std::fs::remove_dir_all(&dir);
        self.drop_upload_lock(bdir, upload_id);
        Ok(Some(metadata.completed_multipart()))
    }

    fn abort_multipart_block(
        &self,
        bdir: &std::path::Path,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ProviderError> {
        let dir = upload_dir(bdir, upload_id);
        let lock = self.upload_lock(bdir, upload_id);
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let Some(meta) = read_meta(&dir).map_err(|_| ProviderError::R2ProviderUnavailable)? else {
            return Err(ProviderError::R2MultipartInvalid);
        };
        if meta.key != key {
            return Err(ProviderError::R2MultipartInvalid);
        }
        std::fs::remove_dir_all(&dir).map_err(|_| ProviderError::R2ProviderUnavailable)?;
        self.drop_upload_lock(bdir, upload_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// W5-r3/P1-1: `create`'s inline reap holds only the tenant lock —
    /// it must skip (not delete) an expired upload whose per-upload
    /// lock is held by an in-flight op (e.g. a complete mid-concat
    /// that crossed the expiry point). Once the lock frees, the next
    /// create reaps it and the lock map entry is dropped.
    #[test]
    fn create_inline_reap_skips_locked_expired_upload() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../target/tmp-w5r3")
            .join(uuid::Uuid::now_v7().hyphenated().to_string());
        std::fs::create_dir_all(&base).unwrap();
        let root = base.join("r2");
        let provider = FsR2Provider::new(root.clone(), 64 * 1024 * 1024).unwrap();
        let inner = &provider.inner;

        let tdir = root.join(vrt_securefs::segment_encode("t1"));
        let bdir = tdir.join(vrt_securefs::segment_encode("b1"));
        // Stage an upload through the real create (secure dir chain,
        // meta shape), then expire it in place via meta.json.
        let upload_id = inner
            .create_multipart_block(&tdir, &bdir, "k", &Default::default())
            .unwrap()
            .upload_id;
        let dir = upload_dir(&bdir, &upload_id);
        let mut meta = read_meta(&dir).unwrap().unwrap();
        meta.expires_ms = now_ms() - 1;
        write_meta(&dir, &meta).unwrap();

        // Hold the upload's lock (as an in-flight complete would) —
        // create must not delete the directory.
        let lock = inner.upload_lock(&bdir, &upload_id);
        let guard = lock.lock().unwrap();
        inner
            .create_multipart_block(&tdir, &bdir, "new", &Default::default())
            .expect("create succeeds while an expired upload is locked");
        assert!(
            dir.exists(),
            "locked expired upload must survive inline reap"
        );
        drop(guard);

        // Freed: the next create reaps it and drops the lock entry.
        inner
            .create_multipart_block(&tdir, &bdir, "new2", &Default::default())
            .expect("create succeeds after lock release");
        assert!(!dir.exists(), "unlocked expired upload must be reaped");
        assert!(
            !inner
                .locks
                .lock()
                .unwrap()
                .contains_key(&format!("{}/{}", bdir.display(), upload_id)),
            "reaped upload's lock entry must be dropped"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}

impl FsR2Provider {
    /// RV-R70: bounded purge of the bucket directory
    /// `<root>/<enc(t)>/<enc(bucket)>/` — objects AND the `.multipart`
    /// staging subtree are removed in `max_files`-bounded passes; the
    /// directory itself is removed on the clean pass. `complete`
    /// means the directory is GONE — never a budget cut.
    pub async fn purge_prefix(
        &self,
        tenant_id: &str,
        bucket_name: &str,
        max_files: usize,
    ) -> Result<crate::purge::PurgeOutcome, ProviderError> {
        if !crate::purge::valid_r2_purge_id(bucket_name)
            || !crate::purge::valid_purge_tenant(tenant_id)
            || max_files == 0
        {
            return Err(ProviderError::R2InvalidOptions);
        }
        let dir = self
            .inner
            .root
            .join(vrt_securefs::segment_encode(tenant_id))
            .join(vrt_securefs::segment_encode(bucket_name));
        crate::purge::purge_dir_bounded(&dir, max_files).await
    }
}
