//! vrt-objstore: typed object-store abstraction (T36 / section 4).
//!
//! Three implementations: `memory` (tests), `fs` (single-node standalone default), `s3`
//! (S3-compatible; conditional writes via If-Match / If-None-Match, compatibility is an unproven item in design doc section 12).
//! Conditional writes are a **real CAS**: fs uses flock + read-verify-write +
//! atomic rename, never a "read then write" race.
//!
//! Three error types: `PreconditionFailed` (CAS failed -- a semantic
//! signal to the caller: lease lost/conflict), `NotFound`, `Unavailable{retryable}`.
//! Every call has a bounded timeout + bounded retry (retry applies only to idempotent operations and connection-level failures).

use std::future::Future;

pub mod deadline;
pub mod drive;
pub mod endpoint;
pub mod fs;
pub mod memory;
pub mod s3;
pub mod self_check;
pub mod sigv4;

pub use deadline::{retry_until, Deadline, RetryPlan};
pub use drive::block_on_store;
pub use fs::FsStore;
pub use memory::MemoryStore;
pub use s3::{S3Store, S3StoreConfig};

/// Object metadata (returned by read/list).
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Backend-specific version token (used for CAS). fs/memory: content sha256; s3: ETag.
    pub etag: String,
    /// Object size in bytes.
    pub size: u64,
}

/// Return value of `get`.
#[derive(Debug, Clone)]
pub struct StoredObject {
    pub meta: ObjectMeta,
    pub body: Vec<u8>,
}

/// A list entry.
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
}

/// `list` result (RV-R8 #7): the implementation's internal pagination has an upper bound, and `truncated == true`
/// means `entries` is **not** the full set -- it stopped at the bound. Callers that need the complete list
/// (WAL replay, delete convergence) must check this flag and fail closed; callers that only need
/// a sample (GC) can use `entries` directly.
#[derive(Debug)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    /// Hit the implementation-level pagination bound; the list is incomplete.
    pub truncated: bool,
}

/// `list_children` result (RV-R28 #3): `common_prefixes` are the collapsed
/// first-level "directories" (full prefix, including the trailing delimiter), and `entries` are the objects directly under `prefix`
/// whose rest contains no delimiter. `truncated` has the same semantics as [`ListPage`]
/// -- callers that need the full set must fail closed.
#[derive(Debug, Default)]
pub struct ListChildrenPage {
    pub entries: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
}

/// Upper bound of the default `put_file` implementation (whole-file buffering) -- larger files must first be
/// rejected by the caller's own `snapshot_max_bytes` limit; this fails closed here
/// as a backstop, to avoid reading a whole file into memory (RV-R8 #7). Streaming backends (fs) override
/// that method and are not subject to this bound.
pub const MAX_PUT_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// Storage error classification (the basis for section 4 decisions).
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Conditional write failed: If-Match etag mismatch / If-None-Match hit an existing object.
    /// Caller semantics: lease lost or concurrent write conflict.
    #[error("precondition failed")]
    PreconditionFailed,
    /// Object does not exist (some backends also return PreconditionFailed when a get/CAS target is missing).
    #[error("object not found")]
    NotFound,
    /// Unavailable: network/5xx/timeout/backend lacks a capability. `retryable=false` means retrying
    /// is pointless (e.g. the backend does not support conditional writes -- fail closed, never degrade to a non-atomic write).
    #[error("store unavailable (retryable={retryable}): {reason}")]
    Unavailable { retryable: bool, reason: String },
    /// Local data corruption (checksum failure / format error).
    #[error("object corrupt: {0}")]
    Corrupt(String),
}

impl StoreError {
    /// Convenience constructor.
    pub fn unavailable(retryable: bool, reason: impl Into<String>) -> Self {
        Self::Unavailable {
            retryable,
            reason: reason.into(),
        }
    }
}

/// `put_file` result: new etag + streaming content sha256 + byte count.
#[derive(Debug, Clone)]
pub struct FilePutOutcome {
    pub etag: String,
    pub sha256: String,
    pub len: u64,
}

/// `get_to_file` result: object etag + content sha256 + byte count.
#[derive(Debug, Clone)]
pub struct FileGetOutcome {
    pub etag: String,
    pub sha256: String,
    pub len: u64,
}

/// Typed object store. All methods are async; when a synchronous call is needed use
/// [`block_on_store`] to bridge (vrt-sql calls it inside the single-writer lock).
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Read an object; returns `Ok(None)` if it does not exist.
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, StoreError>;
    /// Metadata only; returns `Ok(None)` if it does not exist.
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError>;
    /// Write only if the key does not exist. Conflict -> `PreconditionFailed`.
    /// Returns the new object etag on success.
    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError>;
    /// Write only if the current etag == `expected`. Mismatch/missing object ->
    /// `PreconditionFailed`. Returns the new etag on success.
    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError>;
    /// Unconditional write (use when the key is naturally unique, e.g. `{epoch}/{lsn}.seg`).
    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError>;
    /// List keys by prefix (bounded: the implementation paginates/truncates internally). When `page.truncated`
    /// is true, `entries` is a prefix -- callers that need the full set must fail closed
    /// (RV-R8 #7).
    async fn list(&self, prefix: &str) -> Result<ListPage, StoreError>;
    /// RV-R28 #3: list objects under `prefix` whose key is strictly greater than `start_after`
    /// (cursor semantics, exclusive) -- a resumable scan over bounded-pagination stores. The default
    /// implementation is `list` + client-side filtering (correct for complete implementations); bounded backends
    /// (S3 `start-after`) override natively to keep the pagination-bound semantics.
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        let mut page = self.list(prefix).await?;
        if let Some(after) = start_after {
            page.entries.retain(|e| e.key.as_str() > after);
        }
        Ok(page)
    }
    /// RV-R28 #3: delimiter listing -- after `prefix`, keys whose rest contains
    /// `delimiter` collapse into `common_prefixes` (S3
    /// CommonPrefixes), the rest land in `entries`. Used to enumerate by first-level "directory"
    /// (e.g. WAL stems), avoiding a flat full-prefix list hitting the pagination bound.
    /// The default implementation is `list` + collapsing (correct for complete implementations; truncation is reported as usual);
    /// bounded backends override natively.
    async fn list_children(
        &self,
        prefix: &str,
        delimiter: &str,
    ) -> Result<ListChildrenPage, StoreError> {
        let page = self.list(prefix).await?;
        let mut out = ListChildrenPage {
            truncated: page.truncated,
            ..Default::default()
        };
        let mut seen = std::collections::BTreeSet::new();
        for e in page.entries {
            let Some(rest) = e.key.strip_prefix(prefix) else {
                continue;
            };
            if let Some(i) = rest.find(delimiter) {
                let cp = format!("{}{}", prefix, &rest[..i + delimiter.len()]);
                if seen.insert(cp.clone()) {
                    out.common_prefixes.push(cp);
                }
            } else {
                out.entries.push(e);
            }
        }
        out.common_prefixes.sort();
        Ok(out)
    }
    /// Delete; a missing object counts as success (idempotent).
    async fn delete(&self, key: &str) -> Result<(), StoreError>;
    /// RV-R31 #5: stream an object to a local file (without holding it all in memory), returning
    /// etag + content sha256 + byte count -- lets the restore path finish
    /// integrity verification in the same pass as writing to disk. The default implementation is `get` + `fs::write` (fine for small objects;
    /// fs/s3 override with true streaming for large snapshots). Writing `dest` is a **non-atomic**
    /// direct write -- the caller is responsible for tmp+rename.
    async fn get_to_file(
        &self,
        key: &str,
        dest: &std::path::Path,
    ) -> Result<Option<FileGetOutcome>, StoreError> {
        let Some(obj) = self.get(key).await? else {
            return Ok(None);
        };
        let sha256 = crate::sigv4::sha256_hex(&obj.body);
        let len = obj.body.len() as u64;
        let meta = obj.meta;
        std::fs::write(dest, &obj.body)
            .map_err(|e| StoreError::unavailable(true, format!("get_to_file write: {e}")))?;
        Ok(Some(FileGetOutcome {
            etag: meta.etag,
            sha256,
            len,
        }))
    }

    /// T36b: write from a local file as the source (large objects such as snapshots -- avoids putting the whole file
    /// into memory). With `if_none_match=true` it is equivalent to `put_if_none_match`.
    /// Default implementation: `fs::read` + `put*` (memory/S3 path -- large objects are backstopped by the
    /// caller's `snapshot_max_bytes` cap; fs overrides with a streaming copy).
    async fn put_file(
        &self,
        key: &str,
        src: &std::path::Path,
        if_none_match: bool,
    ) -> Result<FilePutOutcome, StoreError> {
        // RV-R8 #7: stat first -- files over the single-object bound are refused a whole read,
        // without allocating equivalent memory; streaming backends override this method and bring their own path.
        let meta = std::fs::metadata(src)
            .map_err(|e| StoreError::unavailable(false, format!("put_file stat: {e}")))?;
        if meta.len() > MAX_PUT_FILE_BYTES {
            return Err(StoreError::unavailable(
                false,
                format!("put_file source {} > MAX_PUT_FILE_BYTES", meta.len()),
            ));
        }
        let body = std::fs::read(src)
            .map_err(|e| StoreError::unavailable(false, format!("put_file read: {e}")))?;
        let sha256 = crate::sigv4::sha256_hex(&body);
        let etag = if if_none_match {
            self.put_if_none_match(key, &body).await?
        } else {
            self.put(key, &body).await?
        };
        Ok(FilePutOutcome {
            etag,
            sha256,
            len: body.len() as u64,
        })
    }
}

/// Key validity: reject empty, absolute, traversal, control characters (shared by all implementations).
pub fn validate_key(key: &str) -> Result<(), StoreError> {
    let ok = !key.is_empty()
        && key.len() <= 1024
        && !key.starts_with('/')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
        && !key
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..");
    if ok {
        Ok(())
    } else {
        Err(StoreError::unavailable(false, "invalid object key"))
    }
}

/// RV-R71: compatibility entry point -- the old linear jitter-free `retry` has been superseded by [`deadline::retry_until`]
/// (exponential backoff + jitter + overall deadline). The `attempts` count bound is kept; the total budget takes
/// `attempts` default backoff caps; callers should prefer using `retry_until` directly.
pub async fn retry<F, Fut, T>(attempts: u32, backoff_ms: u64, f: F) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    let plan = deadline::RetryPlan {
        max_attempts: attempts.max(1),
        base_delay: std::time::Duration::from_millis(backoff_ms.max(1)),
        ..deadline::RetryPlan::default()
    };
    deadline::retry_until(
        &plan,
        deadline::Deadline::after(std::time::Duration::from_secs(60)),
        f,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> MemoryStore {
        MemoryStore::new()
    }

    /// RV-R28 #3: the default `list_after` filters strictly past the
    /// cursor (exclusive resume); `list_children` folds deeper keys
    /// into sorted, deduplicated common prefixes while direct
    /// children land in `entries`.
    #[test]
    fn default_list_after_and_children_on_memory() {
        let store = mem();
        block_on_store(async {
            for k in ["w/a/1", "w/a/2", "w/b/1", "w/top", "w/z-deep/x/y"] {
                store.put(k, b"x").await?;
            }
            // list_after — exclusive cursor.
            let page = store.list_after("w/", Some("w/b/1")).await?;
            let keys: Vec<&str> = page.entries.iter().map(|e| e.key.as_str()).collect();
            assert_eq!(keys, vec!["w/top", "w/z-deep/x/y"]);
            let all = store.list_after("w/", None).await?;
            assert_eq!(all.entries.len(), 5);
            // list_children — fold at the first '/' after the prefix.
            let page = store.list_children("w/", "/").await?;
            assert_eq!(
                page.common_prefixes,
                vec!["w/a/", "w/b/", "w/z-deep/"]
            );
            assert_eq!(page.entries.len(), 1);
            assert_eq!(page.entries[0].key, "w/top");
            assert!(!page.truncated);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    }
}
