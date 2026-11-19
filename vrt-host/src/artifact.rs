//! Content-addressed bundle/artifact cache.
//!
//! Two artifact kinds live under `<root>/<kind>/<digest>`:
//!
//! - `bundles/` — version stored documents, addressed by the **manifest
//!   digest** (`VersionBundle::digest()`), which is what
//!   `compute.desired.artifactDigest` pins. Verification on read is
//!   `VersionBundle::parse_stored` (full re-derive of the manifest digest
//!   from content) plus a key equality check.
//! - `objects/` — raw byte artifacts (system worker modules), addressed
//!   by `sha256(content)`.
//!
//! In both kinds the name IS the digest: a corrupted or swapped object
//! can never be served under the wrong identity, mismatched files are
//! deleted on sight, and only strict 64-char lowercase hex digests may
//! become path segments — path traversal is impossible.
//!
//! Writes are atomic (`vrt-securefs::atomic_write`: tmp + fsync +
//! rename). [`BundleSource`] abstracts where bytes come from on a cache
//! miss: in-memory and local-directory sources ship for tests/dev; the
//! object-storage source is T7/T8 scope (documented in
//! `workers-runtime-host.md`).

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use vrt_config::VersionBundle;

/// Strict lowercase-hex sha256 validation — the only accepted path
/// segment, so traversal cannot occur.
pub fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// sha256 of `bytes` as lowercase hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Artifact cache/source errors — constant field names only.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    /// Digest failed the strict format check.
    #[error("artifact digest invalid")]
    BadDigest,
    /// Content does not verify against the digest it was requested under
    /// (corrupted artifact — stale cache entries are deleted on read).
    #[error("artifact digest mismatch")]
    DigestMismatch,
    /// Artifact not present in the source.
    #[error("artifact missing")]
    Missing,
    /// Remote source unavailable after bounded retries (timeout/5xx).
    #[error("artifact source unavailable: {0}")]
    Unavailable(String),
    /// Object exceeds the configured fetch bound.
    #[error("artifact too large")]
    TooLarge,
    /// Stored bundle document failed `VersionBundle` verification.
    #[error("bundle invalid")]
    BundleInvalid,
    /// Filesystem failure.
    #[error("artifact io: {0}")]
    Io(#[source] io::Error),
}

/// Where bytes come from on a cache miss. Implementations return raw
/// artifact bytes for `digest`; the caller verifies the digest.
pub trait BundleSource: Send + Sync {
    /// Fetch raw artifact bytes for `digest`.
    fn fetch(&self, digest: &str) -> Result<Vec<u8>, ArtifactError>;
}

/// In-memory source for tests/dev.
#[derive(Debug, Default)]
pub struct MemoryBundleSource {
    objects: RwLock<HashMap<String, Vec<u8>>>,
}

impl MemoryBundleSource {
    /// Insert bytes under their real sha256.
    pub fn insert(&self, bytes: &[u8]) -> String {
        let digest = sha256_hex(bytes);
        self.insert_aliased(&digest, bytes);
        digest
    }

    /// Insert bytes under an arbitrary key — for bundles use the bundle
    /// digest; also used by corruption tests.
    pub fn insert_aliased(&self, digest: &str, bytes: &[u8]) {
        self.objects
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(digest.to_string(), bytes.to_vec());
    }
}

impl BundleSource for MemoryBundleSource {
    fn fetch(&self, digest: &str) -> Result<Vec<u8>, ArtifactError> {
        self.objects
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(digest)
            .cloned()
            .ok_or(ArtifactError::Missing)
    }
}

/// Local-directory source: `<dir>/<digest>` files (dev/edge fixtures).
#[derive(Debug)]
pub struct DirBundleSource {
    root: PathBuf,
}

impl DirBundleSource {
    /// Source rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl BundleSource for DirBundleSource {
    fn fetch(&self, digest: &str) -> Result<Vec<u8>, ArtifactError> {
        if !valid_digest(digest) {
            return Err(ArtifactError::BadDigest);
        }
        match std::fs::read(self.root.join(digest)) {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(ArtifactError::Missing),
            Err(e) => Err(ArtifactError::Io(e)),
        }
    }
}

/// Content-addressed on-disk cache. Same-digest writers serialize on a
/// per-digest lock; last write wins (verified bytes are identical anyway).
#[derive(Debug)]
pub struct BundleCache {
    root: PathBuf,
    locks: Mutex<HashMap<String, std::sync::Arc<Mutex<()>>>>,
}

impl BundleCache {
    /// Cache rooted at `root` (callers `vrt_securefs::ensure_secure_dir`
    /// it at startup).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Root path (diagnostics).
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn kind_dir(&self, kind: &str) -> PathBuf {
        self.root.join(kind)
    }

    fn ensure_kind_dir(&self, kind: &str) -> Result<(), ArtifactError> {
        vrt_securefs::ensure_secure_dir(&self.kind_dir(kind)).map_err(ArtifactError::Io)
    }

    fn read_verified(
        &self,
        kind: &str,
        digest: &str,
        verify: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>, ArtifactError> {
        if !valid_digest(digest) {
            return Err(ArtifactError::BadDigest);
        }
        let path = self.kind_dir(kind).join(digest);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(ArtifactError::Missing);
            }
            Err(e) => return Err(ArtifactError::Io(e)),
        };
        if !verify(&bytes) {
            let _ = std::fs::remove_file(&path);
            tracing::warn!(kind, "corrupt cached artifact deleted (digest mismatch)");
            return Err(ArtifactError::DigestMismatch);
        }
        Ok(bytes)
    }

    fn write_verified(
        &self,
        kind: &str,
        digest: &str,
        bytes: &[u8],
        verify: impl Fn(&[u8]) -> bool,
    ) -> Result<(), ArtifactError> {
        if !valid_digest(digest) {
            return Err(ArtifactError::BadDigest);
        }
        if !verify(bytes) {
            return Err(ArtifactError::DigestMismatch);
        }
        self.ensure_kind_dir(kind)?;
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(|e| e.into_inner());
            locks
                .entry(format!("{kind}/{digest}"))
                .or_insert_with(|| std::sync::Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        vrt_securefs::atomic_write(&self.kind_dir(kind).join(digest), bytes, 0o600)
            .map_err(ArtifactError::Io)
    }

    fn fetch_or_cached(
        &self,
        kind: &str,
        digest: &str,
        source: &dyn BundleSource,
        verify: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<u8>, ArtifactError> {
        // T15/B2 single-flight: hold the per-digest lock across the
        // read+fetch+write so concurrent same-digest pulls serialize —
        // the second caller hits the cache the first one populated
        // instead of issuing a duplicate remote fetch.
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(|e| e.into_inner());
            locks
                .entry(format!("{kind}/{digest}"))
                .or_insert_with(|| std::sync::Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        match self.read_verified(kind, digest, &verify) {
            Ok(bytes) => return Ok(bytes),
            Err(ArtifactError::Missing) | Err(ArtifactError::DigestMismatch) => {}
            Err(e) => return Err(e),
        }
        let bytes = source.fetch(digest)?;
        // Reuse the same lock for the write — drop the outer guard is
        // unnecessary since write_verified takes its own lock; keep the
        // guard held so the whole miss→fill stays atomic.
        let _ = self.ensure_kind_dir(kind)?;
        if !verify(&bytes) {
            return Err(ArtifactError::DigestMismatch);
        }
        if !valid_digest(digest) {
            return Err(ArtifactError::BadDigest);
        }
        vrt_securefs::atomic_write(&self.kind_dir(kind).join(digest), &bytes, 0o600)
            .map_err(ArtifactError::Io)?;
        Ok(bytes)
    }

    /// Cached raw object (`objects/`), verified by `sha256 == digest`.
    pub fn get_raw(&self, digest: &str) -> Result<Vec<u8>, ArtifactError> {
        self.read_verified("objects", digest, |b| sha256_hex(b) == digest)
    }

    /// Store a raw object; refuses content that does not hash to `digest`.
    pub fn put_raw(&self, digest: &str, bytes: &[u8]) -> Result<(), ArtifactError> {
        self.write_verified("objects", digest, bytes, |b| sha256_hex(b) == digest)
    }

    /// `get_raw` + `source.fetch` + `put_raw` on miss.
    pub fn get_or_fetch_raw(
        &self,
        digest: &str,
        source: &dyn BundleSource,
    ) -> Result<Vec<u8>, ArtifactError> {
        self.fetch_or_cached("objects", digest, source, |b| sha256_hex(b) == digest)
    }

    /// Cached bundle, verified via `VersionBundle::parse_stored` and a
    /// manifest-digest equality check against `digest`.
    pub fn get_bundle(&self, digest: &str) -> Result<VersionBundle, ArtifactError> {
        let bytes = self.read_verified("bundles", digest, |b| bundle_matches(digest, b))?;
        VersionBundle::parse_stored(&bytes).map_err(|_| ArtifactError::BundleInvalid)
    }

    /// Store bundle bytes under `digest`; refuses content whose
    /// re-derived manifest digest differs.
    pub fn put_bundle(&self, digest: &str, bytes: &[u8]) -> Result<(), ArtifactError> {
        self.write_verified("bundles", digest, bytes, |b| bundle_matches(digest, b))
    }

    /// `get_bundle` + `source.fetch` + `put_bundle` on miss.
    pub fn get_or_fetch_bundle(
        &self,
        digest: &str,
        source: &dyn BundleSource,
    ) -> Result<VersionBundle, ArtifactError> {
        let bytes =
            self.fetch_or_cached("bundles", digest, source, |b| bundle_matches(digest, b))?;
        VersionBundle::parse_stored(&bytes).map_err(|_| ArtifactError::BundleInvalid)
    }
}

fn bundle_matches(digest: &str, bytes: &[u8]) -> bool {
    VersionBundle::parse_stored(bytes)
        .map(|b| b.digest() == digest)
        .unwrap_or(false)
}
