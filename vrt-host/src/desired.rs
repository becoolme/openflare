//! Host-side desired-state access: the verified `compute.desired`
//! snapshot, reused through `vrt-provider::FileDesiredStore` — signature
//! verification, revision fencing and expiry are implemented there once;
//! this module adds only the convergence-facing read surface.

use snapshot_protocol::compute_desired::ComputeDesired;
use std::sync::Arc;
use vrt_provider::desired::DesiredUnavailable;
use vrt_provider::FileDesiredStore;

/// Verified-snapshot read access for convergence + the route table.
#[async_trait::async_trait]
pub trait DesiredSource: Send + Sync {
    /// The last verified snapshot, or fail closed.
    fn snapshot(&self) -> Result<Arc<ComputeDesired>, DesiredUnavailable>;
    /// Re-verify + swap from disk; keeps the previous good snapshot on
    /// any failure (handled inside `FileDesiredStore::reload`).
    async fn reload(&self) -> Result<u64, vrt_provider::desired_file::FileLoadError>;
    /// `payloadDigest` of the last verified envelope (T14/B1) — the
    /// host binds `assignment.json` to this digest; `None` until a
    /// snapshot is loaded.
    fn payload_digest(&self) -> Option<String>;
}

/// File-backed snapshot store — wraps the provider's implementation.
#[derive(Debug)]
pub struct HostDesiredStore {
    inner: FileDesiredStore,
}

impl HostDesiredStore {
    /// Create over `inner`.
    pub fn new(inner: FileDesiredStore) -> Self {
        Self { inner }
    }

    /// Inner store (e.g. for `vrt-provider::desired_file::spawn_reload_task`-style
    /// polling loops).
    pub fn inner(&self) -> &FileDesiredStore {
        &self.inner
    }
}

#[async_trait::async_trait]
impl DesiredSource for HostDesiredStore {
    fn snapshot(&self) -> Result<Arc<ComputeDesired>, DesiredUnavailable> {
        self.inner.snapshot(chrono::Utc::now())
    }

    async fn reload(&self) -> Result<u64, vrt_provider::desired_file::FileLoadError> {
        self.inner.reload(chrono::Utc::now())
    }

    fn payload_digest(&self) -> Option<String> {
        self.inner.payload_digest()
    }
}

/// In-memory snapshot source for tests.
#[derive(Debug, Default)]
pub struct MemoryDesiredSource {
    desired: std::sync::RwLock<Option<Arc<ComputeDesired>>>,
    digest: std::sync::RwLock<Option<String>>,
}

impl MemoryDesiredSource {
    /// Install a snapshot (already verified by the caller's fixture —
    /// verification itself lives in `snapshot-protocol`).
    pub fn install(&self, desired: ComputeDesired) {
        *self.desired.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(desired));
    }

    /// T14/B1: fixture-side digest the source reports — assignment
    /// tests set this to the envelope's real `payloadDigest`.
    pub fn install_with_digest(&self, desired: ComputeDesired, digest: &str) {
        self.install(desired);
        *self.digest.write().unwrap_or_else(|e| e.into_inner()) = Some(digest.to_string());
    }
}

#[async_trait::async_trait]
impl DesiredSource for MemoryDesiredSource {
    fn snapshot(&self) -> Result<Arc<ComputeDesired>, DesiredUnavailable> {
        self.desired
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or(DesiredUnavailable::Missing)
    }

    async fn reload(&self) -> Result<u64, vrt_provider::desired_file::FileLoadError> {
        Ok(self
            .desired
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|d| d.revision)
            .unwrap_or(0))
    }

    fn payload_digest(&self) -> Option<String> {
        self.digest
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
