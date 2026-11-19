//! `FileDesiredStore` — loads and periodically reloads a signed
//! `compute.desired` envelope from disk through `snapshot-protocol`
//! verification. Reload failures keep the previous snapshot until it
//! expires; lookups then fail closed.

use crate::desired::{build_views, DesiredStore, DesiredUnavailable, StoreState, TenantView};
use chrono::{DateTime, Utc};
use snapshot_protocol::compute_desired::{
    verify_compute_desired, ComputeDesired, ComputeDesiredError,
};
use snapshot_protocol::envelope::{RevisionGuard, TrustedKey};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Desired-store backed by a signed snapshot file.
///
/// - [`load_bytes`](Self::load_bytes) verifies kind/scope/signature/
///   time-window/monotonicity through [`verify_compute_desired`] before
///   swapping views.
/// - A failed reload leaves the last good snapshot serving until its own
///   expiry, after which every lookup returns
///   [`DesiredUnavailable::Expired`].
#[derive(Debug)]
pub struct FileDesiredStore {
    path: PathBuf,
    cell_id: String,
    trusted_keys: Vec<TrustedKey>,
    guard: Mutex<RevisionGuard>,
    state: Mutex<Option<StoreState>>,
    grace_cap: usize,
}

impl FileDesiredStore {
    /// Create a store bound to `path`, the expected cell id and the
    /// verifier's trusted-key set.
    pub fn new(
        path: impl Into<PathBuf>,
        cell_id: impl Into<String>,
        trusted_keys: Vec<TrustedKey>,
        grace_cap: usize,
    ) -> Self {
        Self {
            path: path.into(),
            cell_id: cell_id.into(),
            trusted_keys,
            guard: Mutex::new(RevisionGuard::new()),
            state: Mutex::new(None),
            grace_cap,
        }
    }

    /// Verify and install one snapshot document. Fails closed: any
    /// verification error leaves the previous snapshot in place.
    pub fn load_bytes(&self, raw: &[u8], now: DateTime<Utc>) -> Result<u64, ComputeDesiredError> {
        let mut guard = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let verified =
            verify_compute_desired(raw, &self.cell_id, &self.trusted_keys, &mut guard, now)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let views = build_views(
            &verified.payload,
            verified.expires_at,
            state.as_ref().map(|s| &s.views),
            self.grace_cap,
        );
        let revision = verified.payload.revision;
        *state = Some(StoreState {
            views,
            expires_at: verified.expires_at,
            revision,
            desired: Arc::new(verified.payload),
            payload_digest: verified.payload_digest,
        });
        Ok(revision)
    }

    /// Read the configured file and attempt [`load_bytes`](Self::load_bytes).
    /// Errors are returned for logging; the old snapshot is kept.
    pub fn reload(&self, now: DateTime<Utc>) -> Result<u64, FileLoadError> {
        let raw = std::fs::read(&self.path).map_err(FileLoadError::Io)?;
        self.load_bytes(&raw, now).map_err(FileLoadError::Verify)
    }

    /// Path this store watches.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Tenant ids present in the current snapshot (for listener setup).
    pub fn tenant_ids(&self) -> Option<Vec<String>> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.as_ref().map(|s| s.views.keys().cloned().collect())
    }

    /// The last verified snapshot payload, fail-closed on expiry — for
    /// consumers (vrt-host) that need the full desired state rather than
    /// the per-tenant authorization view. Verification is not repeated:
    /// this returns the payload stored by [`load_bytes`](Self::load_bytes).
    pub fn snapshot(&self, now: DateTime<Utc>) -> Result<Arc<ComputeDesired>, DesiredUnavailable> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let state = guard.as_ref().ok_or(DesiredUnavailable::Missing)?;
        if now >= state.expires_at {
            return Err(DesiredUnavailable::Expired);
        }
        Ok(state.desired.clone())
    }

    /// `payloadDigest` of the last verified envelope (T14/B1) — the
    /// host binds its assignment to this, never to unverified bytes.
    pub fn payload_digest(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.payload_digest.clone())
    }

    /// Current snapshot revision, if any.
    pub fn revision(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.revision)
    }
}

impl DesiredStore for FileDesiredStore {
    fn tenant_view(
        &self,
        tenant_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Arc<TenantView>, DesiredUnavailable> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let state = guard.as_ref().ok_or(DesiredUnavailable::Missing)?;
        if now >= state.expires_at {
            return Err(DesiredUnavailable::Expired);
        }
        state
            .views
            .get(tenant_id)
            .cloned()
            .ok_or(DesiredUnavailable::Missing)
    }
}

/// [`FileDesiredStore::reload`] failure modes.
#[derive(Debug, thiserror::Error)]
pub enum FileLoadError {
    /// Snapshot file could not be read.
    #[error("read snapshot file: {0}")]
    Io(#[source] std::io::Error),
    /// Snapshot verification failed.
    #[error("verify snapshot: {0}")]
    Verify(#[source] ComputeDesiredError),
}

/// Spawn the periodic snapshot-reload loop used by the binary.
///
/// Ticks `store.reload` every `interval` (failures keep the last good
/// snapshot — logged, never fatal) and exits as soon as `shutdown`
/// resolves, so listener draining is never blocked by this task.
pub fn spawn_reload_task(
    store: Arc<FileDesiredStore>,
    interval: std::time::Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let interval = interval.max(std::time::Duration::from_millis(100));
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticks.tick() => {
                    match store.reload(chrono::Utc::now()) {
                        Ok(revision) => {
                            tracing::debug!(revision, "desired snapshot reloaded")
                        }
                        Err(error) => {
                            tracing::warn!(%error, "desired snapshot reload failed; keeping previous")
                        }
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
    })
}
