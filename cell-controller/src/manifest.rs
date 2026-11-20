//! `ManifestApplier` — typed sink for `vrt-config` generated
//! NetworkPolicy YAML (T6 output lands here -- "delivered by T8").
//!
//! This task ships only the trait + in-memory implementation; the
//! production `kubectl`/`client-go` apply path is unverified and deliberately
//! not wired (no cluster access from tests or the binary in this task).

use std::sync::Mutex;
use thiserror::Error;

/// Manifest delivery failure.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ManifestError {
    /// The applier rejected the manifest (malformed/unsupported).
    #[error("manifest rejected")]
    Rejected,
    /// Apply mechanism unavailable.
    #[error("manifest applier unavailable")]
    Unavailable,
}

/// Typed apply boundary for generated manifests.
#[async_trait::async_trait]
pub trait ManifestApplier: Send + Sync {
    /// Apply one rendered manifest (idempotent — re-applying the same
    /// document must be a no-op on the cluster side).
    async fn apply(&self, manifest: &str) -> Result<(), ManifestError>;
    /// T13/H1: READ-BACK — is the manifest with this content digest
    /// actually present cluster-side? `apply` alone cannot distinguish
    /// "written" from "adopted"; the reconcile loop refuses APPLIED
    /// acks until this returns `Ok(true)`. An applier that cannot
    /// confirm must return `Err(Unavailable)` — never guess.
    async fn verify(&self, digest: &str) -> Result<bool, ManifestError>;
}

/// In-memory applier recording every applied manifest for assertions.
#[derive(Debug, Default)]
pub struct MemoryManifestApplier {
    applied: Mutex<Vec<String>>,
}

impl MemoryManifestApplier {
    /// Manifests applied so far.
    pub fn applied(&self) -> Vec<String> {
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Test hook: forget recorded digests so `verify` reports absent
    /// (simulates the policy being deleted cluster-side).
    pub fn forget(&self) {
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

fn manifest_digest(manifest: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(manifest.as_bytes()))
}

#[async_trait::async_trait]
impl ManifestApplier for MemoryManifestApplier {
    async fn apply(&self, manifest: &str) -> Result<(), ManifestError> {
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(manifest.to_string());
        Ok(())
    }

    async fn verify(&self, digest: &str) -> Result<bool, ManifestError> {
        Ok(self
            .applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|m| manifest_digest(m) == digest))
    }
}
