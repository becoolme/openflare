//! Persistent revision fence (L10).
//!
//! The in-memory [`snapshot_protocol::envelope::RevisionGuard`] resets
//! on process restart — without persistence a replayed *older* signed
//! snapshot would be accepted after a crash (`revision` fence starts at
//! 0). `FenceStore` records `(lastAcceptedRevision, envelopeDigest)`
//! after every successful verify; startup restores it via
//! `CellVerifier::restore`.
//!
//! `FileFenceStore` writes atomically: tmp file → `fsync` → `rename`
//! (+ parent-dir fsync where the platform allows). A corrupt or
//! malformed file **fails closed** (`load` errors; the operator must
//! remove it manually after confirming the cell is drained — see
//! workers-runtime-cell-controller.md section "Recovery").

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

/// Fence persistence failure — always treated as transient (disk may
/// recover) but never silently ignored.
#[derive(Debug)]
pub struct FenceError(pub String);

impl std::fmt::Display for FenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for FenceError {}

/// Durable revision-fence backend.
pub trait FenceStore: Send + Sync {
    /// Last accepted `(revision, envelopeDigest)`, `None` on first boot.
    fn load(&self) -> Result<Option<(u64, String)>, FenceError>;
    /// Persist a newly accepted fence position.
    fn store(&self, revision: u64, envelope_digest: &str) -> Result<(), FenceError>;
}

#[derive(Serialize, Deserialize)]
struct FenceFile {
    /// `schemaVersion` guards future format changes.
    schema_version: u32,
    revision: u64,
    envelope_digest: String,
}

/// JSON fence file persisted atomically under `path`.
pub struct FileFenceStore {
    path: PathBuf,
}

impl FileFenceStore {
    /// `path` is the durable fence file (e.g.
    /// `/var/lib/vibeploy-cell/revision-fence.json`).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl FenceStore for FileFenceStore {
    fn load(&self) -> Result<Option<(u64, String)>, FenceError> {
        let raw = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(FenceError(format!("fence read: {e}"))),
        };
        let file: FenceFile = serde_json::from_slice(&raw)
            .map_err(|e| FenceError(format!("fence file corrupt: {e}")))?;
        if file.schema_version != 1 {
            return Err(FenceError(format!(
                "fence schemaVersion {} unsupported",
                file.schema_version
            )));
        }
        if file.revision == 0
            || file.revision > (1u64 << 53) - 1
            || file.envelope_digest.len() != 64
            || !file.envelope_digest.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(FenceError("fence file fails validation".into()));
        }
        Ok(Some((file.revision, file.envelope_digest)))
    }

    fn store(&self, revision: u64, envelope_digest: &str) -> Result<(), FenceError> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| FenceError("fence path has no parent".into()))?;
        fs::create_dir_all(parent).map_err(|e| FenceError(format!("fence mkdir: {e}")))?;
        let tmp = parent.join(format!(
            ".{}.tmp-{}",
            self.path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("fence"),
            std::process::id()
        ));
        let body = serde_json::to_vec(&FenceFile {
            schema_version: 1,
            revision,
            envelope_digest: envelope_digest.to_string(),
        })
        .map_err(|e| FenceError(format!("fence encode: {e}")))?;
        let write = || -> std::io::Result<()> {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&body)?;
            f.sync_all()?;
            drop(f);
            fs::rename(&tmp, &self.path)?;
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
            Ok(())
        };
        write().map_err(|e| {
            let _ = fs::remove_file(&tmp);
            FenceError(format!("fence write: {e}"))
        })
    }
}

/// In-memory fence for tests.
#[derive(Default)]
pub struct MemoryFenceStore {
    inner: std::sync::Mutex<Option<(u64, String)>>,
}

impl MemoryFenceStore {
    /// Current value (assertions).
    pub fn value(&self) -> Option<(u64, String)> {
        self.inner.lock().unwrap().clone()
    }
}

impl FenceStore for MemoryFenceStore {
    fn load(&self) -> Result<Option<(u64, String)>, FenceError> {
        Ok(self.inner.lock().unwrap().clone())
    }
    fn store(&self, revision: u64, envelope_digest: &str) -> Result<(), FenceError> {
        *self.inner.lock().unwrap() = Some((revision, envelope_digest.to_string()));
        Ok(())
    }
}

/// Control-plane URL policy: HTTPS required. Plain HTTP is accepted only
/// when the deployment explicitly opts into local/test mode
/// (`allow_insecure_http = true` in the controller config).
pub fn validate_control_plane_url(url: &str, allow_insecure_http: bool) -> Result<(), String> {
    if url.starts_with("https://") {
        return Ok(());
    }
    if url.starts_with("http://") && allow_insecure_http {
        return Ok(());
    }
    Err(format!(
        "control_plane_url must be https:// (got '{}...'); set allow_insecure_http = true only for local/test",
        url.split("://").next().unwrap_or(url)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "vrt-fence-test-{}-{}.json",
            std::process::id(),
            tag
        ))
    }

    #[test]
    fn roundtrip_and_missing() {
        let path = tmp_path("rt");
        let _ = fs::remove_file(&path);
        let store = FileFenceStore::new(&path);
        assert_eq!(store.load().unwrap(), None);
        store.store(41, &"a".repeat(64)).unwrap();
        assert_eq!(store.load().unwrap(), Some((41, "a".repeat(64))));
        store.store(42, &"b".repeat(64)).unwrap();
        assert_eq!(store.load().unwrap().unwrap().0, 42);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn corrupt_fails_closed() {
        let path = tmp_path("bad");
        fs::write(&path, b"{not json").unwrap();
        assert!(FileFenceStore::new(&path).load().is_err());
        fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1, "revision": 0, "envelope_digest": "x".repeat(64)
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(FileFenceStore::new(&path).load().is_err());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn url_policy() {
        assert!(validate_control_plane_url("https://cp.internal", false).is_ok());
        assert!(validate_control_plane_url("http://cp.internal", false).is_err());
        assert!(validate_control_plane_url("http://127.0.0.1:3000", true).is_ok());
        assert!(validate_control_plane_url("ftp://x", true).is_err());
    }
}
