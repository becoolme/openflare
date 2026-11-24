//! Object-store persistence surface (T36 / section 4): manifest CAS lease + epoch fencing +
//! pre-ack segment persistence + takeover/restore + tombstone deletion + segment GC.
//!
//! All methods have synchronous signatures (internally `block_on_store` bridges the async ObjectStore),
//! called by the engine under the single-writer lock -- ordering is naturally serial.

use crate::clock::Clock;
use crate::paths;
use crate::seg;
use crate::timeline::{self, TimelineEntry, TimelineIndex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vrt_objstore::{ObjectStore, StoreError};

/// Persistence config (the runtime form of `[persistence]`).
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Lease duration.
    pub lease_ms: u64,
    /// Clock-skew safety margin: a lease is only considered usable when `expires - margin > now`
    /// (same gate for reads and writes; section 4.3).
    pub clock_skew_margin_ms: u64,
    /// Holder identifier of this instance (unique per process).
    pub holder: String,
    /// Run a checkpoint/snapshot every N persisted commits (0 = disabled).
    pub checkpoint_every_commits: u64,
    /// Retention window (ms) for tombstone/segment objects; GC runs lazily.
    pub retention_ms: u64,
    /// Snapshot object cap (defensive gate).
    pub snapshot_max_bytes: u64,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            lease_ms: 15_000,
            clock_skew_margin_ms: 500,
            holder: String::new(),
            checkpoint_every_commits: 64,
            retention_ms: 86_400_000,
            snapshot_max_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

/// Typed serde (v1) of the manifest object `db/{id}/manifest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version (=1).
    pub v: u32,
    /// Current writer epoch (+1 on each takeover).
    pub epoch: u64,
    /// Last confirmed commit sequence number (monotonic across epochs, never rolls back).
    pub lsn: u64,
    /// Writer identifier.
    pub holder: String,
    /// Lease expiry (unix ms).
    pub expires_at_ms: u64,
    /// Deletion tombstone: any writer's renewal/CAS fails -> everyone is fenced.
    #[serde(default)]
    pub tombstone: bool,
    /// Latest snapshot baseline (may be empty = segment chain only, lsn must be contiguous from 1).
    #[serde(default)]
    pub snapshot: Option<SnapshotRef>,
    /// epoch chain: `{epoch, first_lsn}` -- a deterministic mapping from lsn to the
    /// owning epoch's key space (orphan segments never enter the chain). Ascending by epoch.
    #[serde(default)]
    pub chain: Vec<EpochSpan>,
    /// Follow a `vrt-sql promote` CAS: sha256 of the follower generation the
    /// new epoch was promoted from. Used only to resume a promotion whose CAS
    /// succeeded but whose local adoption did not. Cleared on a normal takeover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion: Option<String>,
}

/// Snapshot reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub key: String,
    /// The lsn (inclusive) the snapshot covers up to.
    pub lsn: u64,
    pub sha256: String,
    pub size: u64,
    /// Wall clock (unix ms) when the snapshot was uploaded. Absent in older manifests (0).
    #[serde(default)]
    pub committed_at_ms: u64,
}

/// Sidecar metadata for a snapshot object, used by Time Travel to pick a baseline without
/// downloading every snapshot. Older snapshots have no sidecar and are not eligible as a Time
/// Travel baseline (the current manifest snapshot is always usable).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotMeta {
    pub v: u32,
    pub database_id: String,
    pub epoch: u64,
    pub lsn: u64,
    pub sha256: String,
    pub size: u64,
    pub committed_at_ms: u64,
}

impl SnapshotMeta {
    pub fn as_ref(&self) -> SnapshotRef {
        SnapshotRef {
            key: String::new(), // filled by the caller from the object key
            lsn: self.lsn,
            sha256: self.sha256.clone(),
            size: self.size,
            committed_at_ms: self.committed_at_ms,
        }
    }
}

/// epoch chain entry: the segments of that epoch's writer start at `first_lsn`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochSpan {
    pub epoch: u64,
    pub first_lsn: u64,
}

/// Lease state held on the engine side (maintained under the Inner lock).
#[derive(Debug, Clone)]
pub struct ManifestHandle {
    pub epoch: u64,
    /// Persisted lsn (== manifest.lsn, also mirrors engine.commit_lsn).
    pub lsn: u64,
    pub expires_at_ms: u64,
    /// etag of the most recent successful CAS (used by put_if_match for both renewals and advances).
    pub etag: String,
    pub holder: String,
    pub snapshot: Option<SnapshotRef>,
    pub chain: Vec<EpochSpan>,
    /// Promotion marker (see [`Manifest::promotion`]).
    pub promotion: Option<String>,
    /// Number of persisted commits since the last checkpoint.
    pub commits_since_ckpt: u64,
    /// Renewal failed but the lease is still within `expiry-margin` -> degraded, reads allowed.
    pub degraded: bool,
}

impl ManifestHandle {
    /// Whether the lease is usable right now (including margin; the unified read/write gate of section 4.3).
    pub fn usable(&self, now_ms: u64, margin_ms: u64) -> bool {
        holder_usable(self.expires_at_ms, now_ms, margin_ms)
    }
    /// Time to renew (lead time = 1/4 of the lease, leaving a window for failure retries).
    pub fn due_renew(&self, now_ms: u64, margin_ms: u64, lease_ms: u64) -> bool {
        now_ms >= self.expires_at_ms.saturating_sub(margin_ms + lease_ms / 4)
    }
    fn to_manifest(&self, tombstone: bool) -> Manifest {
        Manifest {
            v: 1,
            epoch: self.epoch,
            lsn: self.lsn,
            holder: self.holder.clone(),
            expires_at_ms: self.expires_at_ms,
            tombstone,
            snapshot: self.snapshot.clone(),
            chain: self.chain.clone(),
            promotion: self.promotion.clone(),
        }
    }
    fn from_manifest(m: &Manifest, etag: String) -> Self {
        Self {
            epoch: m.epoch,
            lsn: m.lsn,
            expires_at_ms: m.expires_at_ms,
            etag,
            holder: m.holder.clone(),
            snapshot: m.snapshot.clone(),
            chain: m.chain.clone(),
            promotion: m.promotion.clone(),
            commits_since_ckpt: 0,
            degraded: false,
        }
    }
}

/// `.vrt-lsn` progress marker (JSON, v1) -- a durability statement that the local file state has reached
/// `manifest.lsn`.
/// - `file_frames`: the **physical** frame count of `-wal` when the marker was written (including the uncommitted
///   dead-frame tail). At claim time the frame count parsed from the whole local wal chain must equal it.
/// - `intent=true`: the pre-commit intent marker -- never used for trust decisions
///   (an un-acked commit in a crash window leaves an intent lsn > manifest.lsn
///   or the intent marker itself; both force recovery).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sidecar {
    pub v: u32,
    pub lsn: u64,
    #[serde(default)]
    pub file_frames: u64,
    #[serde(default)]
    pub intent: bool,
}

impl Sidecar {
    /// Whether the marker permits trusting the local file as the manifest `lsn` state.
    fn trusted(&self, manifest_lsn: u64) -> bool {
        self.v == 1 && !self.intent && self.lsn == manifest_lsn
    }
}

/// Restore plan: snapshot + segment range to replay + epoch chain.
#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub snapshot: Option<SnapshotRef>,
    /// Restore up to this lsn (inclusive).
    pub up_to_lsn: u64,
    pub chain: Vec<EpochSpan>,
}

/// claim result.
#[derive(Debug)]
pub enum ClaimOutcome {
    /// Write authority obtained: lease state + whether the local file must be restored first.
    Ready {
        handle: ManifestHandle,
        restore: Option<RestorePlan>,
    },
    /// The manifest is held by another live writer (within `expiry-margin`) -- refuse to open.
    HeldByOther { expires_at_ms: u64 },
    /// manifest.tombstone -- the db has been deleted.
    Tombstoned,
}

/// Persistence error.
#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("manifest/object corrupt: {0}")]
    Corrupt(String),
    #[error("lease held by another writer")]
    HeldByOther,
    #[error("database tombstoned")]
    Tombstoned,
    /// Snapshot exceeds `snapshot_max_bytes` (size is judged by stat first, without reading the file).
    /// Restore can still use the segment chain (cost grows linearly with chain length, see section 4.5).
    #[error("snapshot over cap: {0} bytes")]
    SnapshotTooLarge(u64),
    /// CAS contention failed (lease lost) -- the caller must fence.
    #[error("lease lost")]
    Fenced,
    /// RV-R31 #2: local on-disk I/O failure (sidecar/restore tmp, etc.) --
    /// a marker promise that cannot be written to disk means the local persistence layer is untrustworthy; propagate upward, do not ignore.
    #[error("local io: {0}")]
    LocalIo(String),
    /// The manifest moved between the follower snapshot and the promotion CAS.
    /// The caller must re-sync the follower and retry against the new head.
    #[error(
        "manifest changed during promotion (expected epoch {expected_epoch} lsn {expected_lsn})"
    )]
    ManifestChanged {
        expected_epoch: u64,
        expected_lsn: u64,
    },
    /// The follower generation is unreadable or does not describe the expected
    /// commit point. Promotion never guesses.
    #[error("promotion generation invalid: {0}")]
    InvalidGeneration(String),
}

/// A writer promoted from a verified follower generation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromotedWriter {
    /// Epoch of the manifest the follower was caught up to.
    pub previous_epoch: u64,
    /// lsn of the manifest the follower was caught up to.
    pub previous_lsn: u64,
    /// New writer epoch (the promotion CAS).
    pub epoch: u64,
    /// Committed lsn carried over unchanged (the snapshot baseline).
    pub lsn: u64,
    /// Holder recorded by the CAS.
    pub holder: String,
    /// Baseline snapshot published from the follower generation.
    pub snapshot: SnapshotRef,
    /// Local writer image adopted from the follower generation.
    pub db_file: PathBuf,
    /// True when a previous attempt already CASed this promotion and only the
    /// local adoption was completed by this call.
    pub resumed: bool,
    /// Lease handle at the promoted epoch (for release / hand-off).
    #[serde(skip)]
    pub handle: ManifestHandle,
}

/// RV-R66: max number of objects a single `gc_orphans` deletes (including failed attempts).
pub const MAX_GC_DELETES_PER_PASS: usize = 256;

/// How many baseline snapshots GC keeps (the newest N). More than one lets a reader/follower that
/// is one checkpoint behind continue with the segment chain instead of a full re-bootstrap.
/// This is bounded retention, not PITR: GC still deletes everything older than the window.
pub const SNAP_RETENTION: usize = 2;

/// RV-R66: result of one GC (observable, errors not swallowed).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcReport {
    pub deleted: usize,
    pub failed: usize,
    /// Number of objects to delete that exceeded the per-run cap and are left for the next round.
    pub deferred: usize,
    pub list_failed: bool,
}

/// MIG-9/MIG-10: the self-check retry policy and URL sanitization now live in the shared crate
/// `vrt-objstore::self_check` (the vrt-host placement self-check reuses the same
/// primitives) -- re-exported here to keep existing call paths unchanged.
pub use vrt_objstore::self_check::{sanitize_self_check_error, SelfCheckRetry};

/// MIG-9: whether a failed self-check is worth retrying -- only `Unavailable(retryable)`
/// (network/5xx/timeout/429) is transient not-ready; 403/auth-class
/// (`retryable=false`), PreconditionFailed, NotFound, Corrupt, and
/// local IO will not self-heal within 60 s, so fail closed immediately.
fn self_check_retryable(e: &PersistError) -> bool {
    matches!(
        e,
        PersistError::Store(vrt_objstore::StoreError::Unavailable {
            retryable: true,
            ..
        })
    )
}


/// Persistence surface.
pub struct Persister {
    store: Arc<dyn ObjectStore>,
    cfg: PersistConfig,
    clock: Arc<dyn Clock>,
    db_root: PathBuf,
    /// T37r: resource kind -- the object-key prefix (db/ or q/) and local root are bound when the
    /// Persister is constructed; on the same store each of the two kinds holds one instance.
    kind: crate::paths::ResourceKind,
    /// Time Travel retention window (ms). 0 = disabled (bounded two-snapshot GC only).
    time_travel_retention_ms: u64,
}

impl std::fmt::Debug for Persister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persister")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

/// RV-R71: message of the `StoreError::Unavailable` produced when a
/// request deadline cuts a store round trip.
pub const DEADLINE_MSG: &str = "deadline exceeded";

/// RV-R71: true when `e` is a request-deadline cut (not a store fault).
pub fn is_deadline(e: &PersistError) -> bool {
    matches!(
        e,
        PersistError::Store(StoreError::Unavailable { reason, .. }) if reason == DEADLINE_MSG
    )
}

fn manifest_key(kind: crate::paths::ResourceKind, id: &str) -> String {
    seg::manifest_key(kind, id)
}

/// lsn -> epoch key-space resolution (chain ascending).
/// Epoch that owns `lsn` according to the manifest chain (None when no span covers it).
pub fn epoch_for_lsn(chain: &[EpochSpan], lsn: u64) -> Option<u64> {
    let mut hit = None;
    for span in chain {
        if span.first_lsn <= lsn {
            hit = Some(span.epoch);
        } else {
            break;
        }
    }
    hit
}

impl Persister {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        cfg: PersistConfig,
        clock: Arc<dyn Clock>,
        db_root: PathBuf,
        kind: crate::paths::ResourceKind,
    ) -> Self {
        Self {
            store,
            cfg,
            clock,
            db_root,
            kind,
            time_travel_retention_ms: 0,
        }
    }

    /// Enable Time Travel retention (ms). GC keeps snapshots/segments inside the window and one
    /// baseline snapshot before it. 0 keeps the bounded two-snapshot policy only.
    pub fn with_time_travel_retention(mut self, ms: u64) -> Self {
        self.time_travel_retention_ms = ms;
        self
    }

    /// The resource kind bound to this instance (determines the object-key prefix + local root).
    pub fn kind(&self) -> crate::paths::ResourceKind {
        self.kind
    }

    pub fn config(&self) -> &PersistConfig {
        &self.cfg
    }
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
    /// Test observation hook.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// T36b: startup self-check -- write/read/delete a unique probe object, exercising the complete
    /// CAS path (fs: lockfile+flock+tmp->rename+directory fsync; s3:
    /// If-None-Match conditional write). Failure = object store unavailable/unwritable,
    /// and the caller must refuse to start (fail closed -- better than a 503 on every write at runtime).
    /// The probe key contains pid+nanos, so concurrent self-checks of multiple instances do not interfere; content is empty.
    pub fn self_check(&self) -> Result<(), PersistError> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let key = format!("probe/self-check-{}-{nonce:x}", std::process::id());
        self.store_put_inm(&key, b"")?;
        let back = self.store_get(&key)?;
        let _ = self.store_delete(&key);
        if back.is_none() {
            return Err(PersistError::Corrupt(
                "self-check probe vanished before delete".into(),
            ));
        }
        Ok(())
    }

    /// MIG-9: bounded in-process self-check retry -- exponential backoff (initial->x2->cap),
    /// returns the last error when the total `budget` is exhausted (the caller exits non-zero).
    /// Auth-class / non-retryable errors (403, PreconditionFailed, Corrupt, etc.) return
    /// immediately without retry -- only transient not-ready is worth waiting for; config errors do not self-heal.
    /// The budget gates the sleep between attempts: total wall clock <= budget + one self-check duration.
    /// `log(attempt, elapsed, next_backoff, err)` is written by the caller as structured
    /// logs -- error text is first stripped of URLs via [`sanitize_self_check_error`].
    /// The process should not take traffic during retries: in K8s readiness is gated by the startupProbe
    /// (port not bound = not-ready), and liveness does not declare it dead.
    pub fn self_check_with_retry(
        &self,
        retry: &SelfCheckRetry,
        log: impl FnMut(u32, std::time::Duration, std::time::Duration, &PersistError),
    ) -> Result<(), PersistError> {
        vrt_objstore::self_check::run_with_retry(
            retry,
            self_check_retryable,
            || self.self_check(),
            log,
        )
    }

    fn store_get(&self, key: &str) -> Result<Option<vrt_objstore::StoredObject>, PersistError> {
        Ok(vrt_objstore::block_on_store(self.store.get(key))?)
    }
    /// Unconditional put (timeline chunks / snapshot metadata: immutable-by-content, safe to rewrite).
    fn store_put(&self, key: &str, body: &[u8]) -> Result<(), PersistError> {
        vrt_objstore::block_on_store(self.store.put(key, body))?;
        Ok(())
    }
    fn store_put_inm(&self, key: &str, body: &[u8]) -> Result<String, PersistError> {
        Ok(vrt_objstore::block_on_store(
            self.store.put_if_none_match(key, body),
        )?)
    }
    fn store_put_im(&self, key: &str, etag: &str, body: &[u8]) -> Result<String, PersistError> {
        Ok(vrt_objstore::block_on_store(
            self.store.put_if_match(key, etag, body),
        )?)
    }
    fn store_list(&self, prefix: &str) -> Result<vrt_objstore::ListPage, PersistError> {
        Ok(vrt_objstore::block_on_store(self.store.list(prefix))?)
    }
    fn store_delete(&self, key: &str) -> Result<(), PersistError> {
        Ok(vrt_objstore::block_on_store(self.store.delete(key))?)
    }

    /// RV-R71: drive one store future from sync code, cut at `dl`
    /// (absolute deadline of the inbound request). `None` = legacy
    /// unbounded-by-request behaviour (the store's own per-request
    /// timeout still applies). Expiry maps to the retryable
    /// `Unavailable("deadline exceeded")` that [`is_deadline`] detects.
    fn bridge<F, T>(&self, dl: Option<std::time::Instant>, fut: F) -> Result<T, StoreError>
    where
        F: std::future::Future<Output = Result<T, StoreError>> + Send,
        T: Send,
    {
        match dl {
            None => vrt_objstore::block_on_store(fut),
            Some(d) => vrt_objstore::block_on_store(async move {
                match tokio::time::timeout_at(tokio::time::Instant::from_std(d), fut).await {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::unavailable(true, DEADLINE_MSG)),
                }
            }),
        }
    }
    fn store_get_dl(
        &self,
        key: &str,
        dl: Option<std::time::Instant>,
    ) -> Result<Option<vrt_objstore::StoredObject>, PersistError> {
        Ok(self.bridge(dl, self.store.get(key))?)
    }
    fn store_put_inm_dl(
        &self,
        key: &str,
        body: &[u8],
        dl: Option<std::time::Instant>,
    ) -> Result<String, PersistError> {
        Ok(self.bridge(dl, self.store.put_if_none_match(key, body))?)
    }
    fn store_put_im_dl(
        &self,
        key: &str,
        etag: &str,
        body: &[u8],
        dl: Option<std::time::Instant>,
    ) -> Result<String, PersistError> {
        Ok(self.bridge(dl, self.store.put_if_match(key, etag, body))?)
    }
    fn read_manifest_dl(
        &self,
        id: &str,
        dl: Option<std::time::Instant>,
    ) -> Result<Option<(Manifest, String)>, PersistError> {
        match self.store_get_dl(&manifest_key(self.kind, id), dl)? {
            None => Ok(None),
            Some(obj) => {
                let m: Manifest = serde_json::from_slice(&obj.body)
                    .map_err(|e| PersistError::Corrupt(format!("manifest json: {e}")))?;
                if m.v != 1 {
                    return Err(PersistError::Corrupt(format!("manifest v{}", m.v)));
                }
                Ok(Some((m, obj.meta.etag)))
            }
        }
    }
    fn write_manifest_im_dl(
        &self,
        id: &str,
        etag: &str,
        m: &Manifest,
        dl: Option<std::time::Instant>,
    ) -> Result<String, PersistError> {
        let body = serde_json::to_vec(m).map_err(|e| PersistError::Corrupt(e.to_string()))?;
        self.store_put_im_dl(&manifest_key(self.kind, id), etag, &body, dl)
    }

    fn read_manifest(&self, id: &str) -> Result<Option<(Manifest, String)>, PersistError> {
        match self.store_get(&manifest_key(self.kind, id))? {
            None => Ok(None),
            Some(obj) => {
                let m: Manifest = serde_json::from_slice(&obj.body)
                    .map_err(|e| PersistError::Corrupt(format!("manifest json: {e}")))?;
                if m.v != 1 {
                    return Err(PersistError::Corrupt(format!("manifest v{}", m.v)));
                }
                Ok(Some((m, obj.meta.etag)))
            }
        }
    }

    fn write_manifest_inm(&self, id: &str, m: &Manifest) -> Result<String, PersistError> {
        let body = serde_json::to_vec(m).map_err(|e| PersistError::Corrupt(e.to_string()))?;
        self.store_put_inm(&manifest_key(self.kind, id), &body)
    }
    fn write_manifest_im(
        &self,
        id: &str,
        etag: &str,
        m: &Manifest,
    ) -> Result<String, PersistError> {
        let body = serde_json::to_vec(m).map_err(|e| PersistError::Corrupt(e.to_string()))?;
        self.store_put_im(&manifest_key(self.kind, id), etag, &body)
    }

    /// `.vrt-lsn` sidecar (the lsn last acked to the manifest locally +
    /// the WAL physical frame count at that time -- all the credentials of the RV-R31 #2 trust decision).
    fn lsn_sidecar(&self, db_dir: &Path) -> PathBuf {
        db_dir.join(".vrt-lsn")
    }
    fn read_sidecar(&self, db_dir: &Path) -> Option<Sidecar> {
        let raw = std::fs::read(self.lsn_sidecar(db_dir)).ok()?;
        // Old-version hex text / truncated JSON all yield None -> untrusted -> go through restore
        // (upgrade compatibility: old markers are conservatively invalidated, at the cost of one extra restore).
        serde_json::from_slice(&raw).ok()
    }
    /// tmp -> fsync file -> rename -> fsync parent directory (vrt_securefs::
    /// atomic_write), errors propagate upward -- RV-R31 #2: a marker promise that cannot be written
    /// to disk means the local persistence layer is untrustworthy.
    fn write_sidecar(&self, db_dir: &Path, sc: &Sidecar) -> Result<(), PersistError> {
        let body = serde_json::to_vec(sc)
            .map_err(|e| PersistError::Corrupt(format!("sidecar json: {e}")))?;
        vrt_securefs::atomic_write(&self.lsn_sidecar(db_dir), &body, 0o600)
            .map_err(|e| PersistError::LocalIo(format!("sidecar write: {e}")))
    }

    /// T36: write the "intent lsn" before commit -- the sidecar records the highest commit position the **local file may
    /// contain that is not yet confirmed by the manifest**. If an un-acked commit
    /// appears locally in a crash window, the sidecar is necessarily > manifest.lsn -> claim goes through the
    /// restore path and clears it. There is never a hole of "local unpersisted data being trusted".
    /// RV-R31 #2: a write failure is returned upward -- the caller rolls back the batch and refuses to ack.
    pub fn mark_intent(&self, database_id: &str, lsn: u64) -> Result<(), PersistError> {
        self.write_sidecar(
            &self.db_dir(database_id),
            &Sidecar {
                v: 1,
                lsn,
                file_frames: 0,
                intent: true,
            },
        )
    }

    /// RV-R31 #2: full parse of the local `-wal` (header + salt + cumulative
    /// checksum over the whole chain, **consumed through end of file**) -> return the physical frame count;
    /// any torn / other-generation / broken-chain / non-frame-aligned tail -> None (local not trusted).
    /// File missing / length 0 -> Some(0).
    fn local_wal_frames(&self, dir: &Path) -> Option<u64> {
        let wal_path = dir.join("db.sqlite-wal");
        let buf = match std::fs::read(&wal_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(0),
            Err(_) => return None,
        };
        if buf.is_empty() {
            return Some(0);
        }
        if buf.len() < crate::wal::WAL_HDR {
            return None;
        }
        let hdr = crate::wal::wal_header(&buf[..crate::wal::WAL_HDR])?;
        let ps = hdr.page_size();
        let fsz = (crate::wal::WAL_FRAME_HDR + ps as usize) as u64;
        let body_len = buf.len() as u64 - crate::wal::WAL_HDR as u64;
        if body_len % fsz != 0 {
            return None; // half frame / torn tail
        }
        let seed = (
            u32::from_be_bytes(buf[24..28].try_into().unwrap()),
            u32::from_be_bytes(buf[28..32].try_into().unwrap()),
        );
        let (vend, heads, _) = crate::wal::scan_frames(
            &buf[crate::wal::WAL_HDR..],
            0,
            ps,
            hdr.cksum_be(),
            (hdr.salt1, hdr.salt2),
            seed,
        );
        if vend != body_len {
            return None; // chain broken / generations mixed -> file state incomplete
        }
        Some(heads.len() as u64)
    }

    /// Local db directory (`{shard}/{id}`). derive_db_file does mkdir --
    /// call only when we really are about to open/restore; it must not be called before claim (otherwise an empty directory
    /// would make a "nonexistent db" look "locally existing" -- though the directory has no db
    /// file and local_exists judges by db.sqlite, so it is safe).
    fn db_dir(&self, database_id: &str) -> PathBuf {
        match paths::derive_db_file(&self.db_root, database_id) {
            Ok(f) => f.parent().unwrap_or(&self.db_root).to_path_buf(),
            Err(_) => self.db_root.clone(),
        }
    }

    /// Claim write authority (open cold path). Retries CAS contention for at most 3 rounds.
    pub fn claim(&self, database_id: &str) -> Result<ClaimOutcome, PersistError> {
        for _ in 0..3 {
            match self.claim_once(database_id)? {
                ClaimStep::Retry => continue,
                ClaimStep::Done(out) => return Ok(out),
            }
        }
        Err(PersistError::Store(StoreError::unavailable(
            true,
            "manifest claim races exhausted",
        )))
    }

    fn claim_once(&self, database_id: &str) -> Result<ClaimStep, PersistError> {
        let now = self.clock.now_ms();
        let margin = self.cfg.clock_skew_margin_ms;
        let dir = self.db_dir(database_id);
        let db_file = dir.join("db.sqlite");
        let local_exists = db_file.exists();
        match self.read_manifest(database_id)? {
            None => {
                // No manifest: brand new, or an old-version local db being migrated in (upgrade compatibility).
                if local_exists {
                    // RV-R31 #3: before ADOPT, fold the leftover `-wal` into the main
                    // file -- a wal left by SIGKILL may hold committed but
                    // un-checkpointed pages; snapshotting the main file directly would lose data.
                    // busy (another opener / read pin) -> fail closed and refuse adoption;
                    // an integrity_check failure after the merge is likewise refused.
                    let wal_side = dir.join("db.sqlite-wal");
                    let wal_pending = std::fs::metadata(&wal_side)
                        .map(|m| m.len() > 0)
                        .unwrap_or(false);
                    if wal_pending {
                        match crate::wal::fold_journal(&db_file) {
                            Ok(true) => {}
                            Ok(false) => {
                                return Err(PersistError::Store(StoreError::unavailable(
                                    true,
                                    "adopt: wal checkpoint busy — refuse to adopt partial image",
                                )));
                            }
                            Err(e) => {
                                return Err(PersistError::Corrupt(format!(
                                    "adopt fold journal: {e}"
                                )))
                            }
                        }
                    }
                    match crate::wal::integrity_ok(&db_file) {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(PersistError::Corrupt(
                                "adopt: integrity_check failed".into(),
                            ))
                        }
                        Err(e) => {
                            return Err(PersistError::Corrupt(format!(
                                "adopt integrity_check: {e}"
                            )))
                        }
                    }
                    let snap = self.upload_snapshot(database_id, 1, 0, &db_file)?;
                    let m = Manifest {
                        v: 1,
                        epoch: 1,
                        lsn: 0,
                        holder: self.cfg.holder.clone(),
                        expires_at_ms: now + self.cfg.lease_ms,
                        tombstone: false,
                        snapshot: Some(snap.clone()),
                        chain: vec![EpochSpan {
                            epoch: 1,
                            first_lsn: 1,
                        }],
                        promotion: None,
                    };
                    match self.write_manifest_inm(database_id, &m) {
                        Ok(etag) => {
                            // The snapshot already holds the full post-fold content; the wal was TRUNCATEd
                            // to zero -> file_frames=0.
                            self.write_sidecar(
                                &dir,
                                &Sidecar {
                                    v: 1,
                                    lsn: 0,
                                    file_frames: 0,
                                    intent: false,
                                },
                            )?;
                            Ok(ClaimStep::Done(ClaimOutcome::Ready {
                                handle: ManifestHandle::from_manifest(&m, etag),
                                restore: None,
                            }))
                        }
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                            // Concurrent adopt/create: re-read to decide ownership.
                            Ok(ClaimStep::Retry)
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    let m = Manifest {
                        v: 1,
                        epoch: 1,
                        lsn: 0,
                        holder: self.cfg.holder.clone(),
                        expires_at_ms: now + self.cfg.lease_ms,
                        tombstone: false,
                        snapshot: None,
                        chain: vec![EpochSpan {
                            epoch: 1,
                            first_lsn: 1,
                        }],
                        promotion: None,
                    };
                    match self.write_manifest_inm(database_id, &m) {
                        Ok(etag) => Ok(ClaimStep::Done(ClaimOutcome::Ready {
                            handle: ManifestHandle::from_manifest(&m, etag),
                            restore: None,
                        })),
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                            Ok(ClaimStep::Retry)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
            Some((m, etag)) => {
                if m.tombstone {
                    return Ok(ClaimStep::Done(ClaimOutcome::Tombstoned));
                }
                // RV-R56: the taker judges the other side stale only when now >= expires + margin
                // (more conservative than the holder), avoiding a window where both sides serve reads.
                if m.holder != self.cfg.holder && !taker_may_claim(m.expires_at_ms, now, margin) {
                    return Ok(ClaimStep::Done(ClaimOutcome::HeldByOther {
                        expires_at_ms: m.expires_at_ms,
                    }));
                }
                // We hold it (renewal) or the other side has expired (takeover) -> CAS.
                let takeover =
                    m.holder != self.cfg.holder || !holder_usable(m.expires_at_ms, now, margin);
                let mut next = m.clone();
                next.expires_at_ms = now + self.cfg.lease_ms;
                if takeover {
                    next.epoch = m.epoch + 1;
                    next.holder = self.cfg.holder.clone();
                    // A real writer claim supersedes an unfinished promotion:
                    // never let a stale promote replay adopt over it.
                    next.promotion = None;
                    // The epoch+1 writer continues segment numbering from manifest.lsn+1.
                    next.chain.push(EpochSpan {
                        epoch: next.epoch,
                        first_lsn: m.lsn + 1,
                    });
                }
                match self.write_manifest_im(database_id, &etag, &next) {
                    Ok(new_etag) => {
                        let handle = ManifestHandle::from_manifest(&next, new_etag);
                        // RV-R31 #2: trusting the local file requires triple evidence -- the sidecar
                        // marker has reached manifest.lsn and is not intent; the local
                        // `-wal` is fully parseable from header to end of file;
                        // the parsed frame count equals the physical frame count recorded in the marker. Missing any one means
                        // the local copy is incomplete -> go through restore (fail closed).
                        let sidecar_ok = local_exists
                            && match self.read_sidecar(&dir) {
                                Some(sc) if sc.trusted(m.lsn) => {
                                    self.local_wal_frames(&dir) == Some(sc.file_frames)
                                }
                                _ => false,
                            };
                        let restore = if sidecar_ok || (m.lsn == 0 && m.snapshot.is_none()) {
                            // No restore needed; but a local file exists while the manifest is entirely empty ->
                            // local data was never acked -> treat as uncommitted and clear it.
                            if local_exists && m.lsn == 0 && m.snapshot.is_none() && !sidecar_ok {
                                Some(RestorePlan {
                                    snapshot: None,
                                    up_to_lsn: 0,
                                    chain: next.chain.clone(),
                                })
                            } else {
                                None
                            }
                        } else {
                            Some(RestorePlan {
                                snapshot: m.snapshot.clone(),
                                up_to_lsn: m.lsn,
                                chain: m.chain.clone(),
                            })
                        };
                        Ok(ClaimStep::Done(ClaimOutcome::Ready { handle, restore }))
                    }
                    Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                        Ok(ClaimStep::Retry)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Renew (CAS): on success update handle.expires/etag/degraded=false.
    /// `PreconditionFailed` -> re-read: if the holder is no longer us -> Fenced;
    /// if it is still us (etag drift) -> absorb the new etag and CAS once more.
    pub fn renew(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        self.renew_until(database_id, handle, None)
    }

    /// RV-R71: [`Self::renew`] with the request deadline — every store
    /// round trip is cut at `dl`, so a hung object store can't pin the
    /// single-writer lock (and a tokio blocking thread) past the call.
    pub fn renew_until(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        dl: Option<std::time::Instant>,
    ) -> Result<(), PersistError> {
        let now = self.clock.now_ms();
        let mut attempt = handle.clone();
        attempt.expires_at_ms = now + self.cfg.lease_ms;
        match self.write_manifest_im_dl(database_id, &handle.etag, &attempt.to_manifest(false), dl)
        {
            Ok(etag) => {
                handle.expires_at_ms = attempt.expires_at_ms;
                handle.etag = etag;
                handle.degraded = false;
                Ok(())
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                match self.read_manifest_dl(database_id, dl)? {
                    Some((m, etag))
                        if m.holder == self.cfg.holder
                            && m.epoch == handle.epoch
                            && !m.tombstone =>
                    {
                        let mut attempt2 = ManifestHandle::from_manifest(&m, etag.clone());
                        attempt2.expires_at_ms = now + self.cfg.lease_ms;
                        let etag2 = self.write_manifest_im_dl(
                            database_id,
                            &etag,
                            &attempt2.to_manifest(false),
                            dl,
                        )?;
                        handle.expires_at_ms = attempt2.expires_at_ms;
                        handle.etag = etag2;
                        handle.degraded = false;
                        Ok(())
                    }
                    _ => Err(PersistError::Fenced),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Pre-ack persistence (section 4.2 option 1, called under the single-writer lock):
    /// put seg (key unique -> If-None-Match) -> CAS manifest lsn.
    /// `wal_file_frames` = the `-wal` physical frame count after commit (including the dead-frame tail) --
    /// written into the sidecar so claim can verify local file integrity.
    /// Any step failing -> Err (the caller fences + returns retryable).
    pub fn publish(
        &self,
        database_id: &str,
        seg_bytes: &[u8],
        new_lsn: u64,
        wal_file_frames: u64,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        self.publish_until(
            database_id,
            seg_bytes,
            new_lsn,
            wal_file_frames,
            handle,
            None,
        )
    }

    /// RV-R71: [`Self::publish`] with the request deadline. A timeout
    /// leaves the outcome of the in-flight put/CAS unknown, so callers
    /// MUST treat [`is_deadline`] errors as "commit outcome unknown"
    /// (fence the writer; never ack).
    pub fn publish_until(
        &self,
        database_id: &str,
        seg_bytes: &[u8],
        new_lsn: u64,
        wal_file_frames: u64,
        handle: &mut ManifestHandle,
        dl: Option<std::time::Instant>,
    ) -> Result<(), PersistError> {
        let key = seg::seg_key(self.kind, database_id, handle.epoch, new_lsn);
        self.store_put_inm_dl(&key, seg_bytes, dl)
            .map_err(|e| match e {
                // A segment with the same epoch+lsn already exists = we cannot collide with ourselves (serial) -> semantic conflict.
                PersistError::Store(StoreError::PreconditionFailed) => PersistError::Fenced,
                other => other,
            })?;
        let mut next = handle.clone();
        next.lsn = new_lsn;
        next.commits_since_ckpt = handle.commits_since_ckpt + 1;
        // RV-R31: lsn=0 baseline -- the open-time TRUNCATE has already folded the init/dedup/ADOPT
        // pages into the main file, and the segment chain records only frames after the watermark; replaying onto an empty image misses main-file pages
        // and necessarily yields a malformed db (integrity_check refuses to open). On publishing the first segment the current
        // db.sqlite (exactly the post-TRUNCATE baseline) is uploaded as snap-0, landing in the same batch as
        // the manifest CAS; an upload failure follows publish semantics -> Err.
        if new_lsn == 1 && handle.snapshot.is_none() {
            let snap = self.upload_snapshot_dl(
                database_id,
                handle.epoch,
                0,
                &self.db_dir(database_id).join("db.sqlite"),
                dl,
            )?;
            next.snapshot = Some(snap);
        }
        match self.write_manifest_im_dl(database_id, &handle.etag, &next.to_manifest(false), dl) {
            Ok(etag) => {
                handle.lsn = new_lsn;
                handle.etag = etag;
                handle.commits_since_ckpt += 1;
                handle.snapshot = next.snapshot;
                // RV-R31 #2: the marker must be durably on disk (tmp->fsync->rename->
                // dir fsync) and errors must propagate -- at this point the commit is on the path of being acked
                // to the caller; a sidecar failure = local state and marker
                // inconsistent -> Err makes the engine fence (fail closed).
                self.write_sidecar(
                    &self.db_dir(database_id),
                    &Sidecar {
                        v: 1,
                        lsn: new_lsn,
                        file_frames: wal_file_frames,
                        intent: false,
                    },
                )?;
                Ok(())
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => Err(PersistError::Fenced),
            Err(e) => Err(e),
        }
    }

    /// checkpoint: after TRUNCATE the caller passes the db image path; upload snapshot +
    /// CAS manifest.snapshot. **This function does no GC** (RV-R66 corrects the old comment):
    /// segments / old snapshots covered by the new snapshot are cleaned by `gc_orphans` (lazily, in bounded
    /// fashion at the end of recovery), otherwise retained. No PITR; for the retention policy see the sql-storage doc.
    /// Failure only returns Err (data is still protected by the segment chain) -- the caller just warns.
    pub fn checkpoint(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        db_file: &Path,
    ) -> Result<(), PersistError> {
        let snap = self.upload_snapshot(database_id, handle.epoch, handle.lsn, db_file)?;
        let mut next = handle.clone();
        next.snapshot = Some(snap.clone());
        match self.write_manifest_im(database_id, &handle.etag, &next.to_manifest(false)) {
            Ok(etag) => {
                handle.snapshot = Some(snap);
                handle.etag = etag;
                handle.commits_since_ckpt = 0;
                // RV-R31 #2: TRUNCATE has zeroed `-wal` -- file_frames in the marker
                // must be zeroed in sync, otherwise the frame-count check at claim time fails
                // -> permanently forced recovery (self-inflicted). Failures propagate upward.
                self.write_sidecar(
                    &self.db_dir(database_id),
                    &Sidecar {
                        v: 1,
                        lsn: handle.lsn,
                        file_frames: 0,
                        intent: false,
                    },
                )?;
                Ok(())
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => Err(PersistError::Fenced),
            Err(e) => Err(e),
        }
    }

    fn upload_snapshot(
        &self,
        database_id: &str,
        epoch: u64,
        lsn: u64,
        db_file: &Path,
    ) -> Result<SnapshotRef, PersistError> {
        self.upload_snapshot_dl(database_id, epoch, lsn, db_file, None)
    }

    fn upload_snapshot_dl(
        &self,
        database_id: &str,
        epoch: u64,
        lsn: u64,
        db_file: &Path,
        dl: Option<std::time::Instant>,
    ) -> Result<SnapshotRef, PersistError> {
        // T36b: stat first to judge the cap -- over the cap the file is not read (the pitfall of pathologically re-reading the whole db,
        // section 4.5); the fs backend's put_file streams the copy and does not materialize either.
        let len = std::fs::metadata(db_file)
            .map_err(|e| PersistError::Corrupt(format!("snapshot stat: {e}")))?
            .len();
        if len > self.cfg.snapshot_max_bytes {
            return Err(PersistError::SnapshotTooLarge(len));
        }
        let key = seg::snap_key(self.kind, database_id, epoch, lsn);
        let out = self
            .bridge(dl, self.store.put_file(&key, db_file, true))
            .map_err(|e| match e {
                StoreError::PreconditionFailed => PersistError::Fenced,
                other => PersistError::Store(other),
            })?;
        let committed_at_ms = self.clock.now_ms();
        // Time Travel sidecar. Best-effort: the snapshot+manifest path does not depend on it, but
        // without it an older snapshot is not eligible as a Time Travel baseline.
        let meta = SnapshotMeta {
            v: 1,
            database_id: database_id.into(),
            epoch,
            lsn,
            sha256: out.sha256.clone(),
            size: out.len,
            committed_at_ms,
        };
        if let Ok(bytes) = serde_json::to_vec(&meta) {
            let meta_key = seg::snap_meta_key(self.kind, database_id, epoch, lsn);
            if let Err(e) = self.store_put(&meta_key, &bytes) {
                tracing::warn!(db = %database_id, lsn, error = %e,
                    "snapshot metadata write failed; Time Travel cannot use this baseline");
            }
        }
        Ok(SnapshotRef {
            key,
            lsn,
            sha256: out.sha256,
            size: out.len,
            committed_at_ms,
        })
    }

    /// RV-R31 #5: lease renewal during restore -- renew as soon as the remaining lease drops below half a period
    /// (leaving a retry window for renewal failures). Renewal failure -> Err -> restore aborts,
    /// and the caller fails closed (the db is not opened).
    fn restore_renew(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        let now = self.clock.now_ms();
        if handle
            .expires_at_ms
            .saturating_sub(self.cfg.clock_skew_margin_ms + self.cfg.lease_ms / 2)
            <= now
        {
            self.renew(database_id, handle)?;
        }
        Ok(())
    }

    /// Restore: stream-download the snapshot + replay segments along the chain -> rebuild the local db file.
    /// Any corruption / gap / sha mismatch -> Err (fail closed).
    ///
    /// On-disk order (RV-R31 #2/#5; the marker is not written if any step is missing):
    /// snapshot/replay writes `.restore.tmp` -> fsync tmp -> integrity_check ->
    /// clear stale wal/shm -> rename -> fsync directory -> write sidecar.
    /// During restore the lease is renewed every half lease period; a renewal failure aborts.
    pub fn restore(
        &self,
        database_id: &str,
        plan: &RestorePlan,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        let dir = self.db_dir(database_id);
        std::fs::create_dir_all(&dir).map_err(|e| PersistError::LocalIo(format!("db dir: {e}")))?;
        let db_file = dir.join("db.sqlite");
        let tmp = dir.join(".restore.tmp");
        // Snapshot baseline (or empty file) -- streamed to tmp, no longer held whole in memory.
        match &plan.snapshot {
            Some(snap) => {
                let out = vrt_objstore::block_on_store(self.store.get_to_file(&snap.key, &tmp))?
                    .ok_or_else(|| PersistError::Corrupt("snapshot object missing".into()))?;
                if out.len != snap.size {
                    return Err(PersistError::Corrupt("snapshot size mismatch".into()));
                }
                if out.sha256 != snap.sha256 {
                    return Err(PersistError::Corrupt("snapshot sha256 mismatch".into()));
                }
                std::fs::File::open(&tmp)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| PersistError::LocalIo(format!("restore fsync: {e}")))?;
                self.restore_renew(database_id, handle)?;
            }
            None => {
                // No snapshot baseline: replay all segments from an empty file (1..=up_to). Segment frames are
                // full page images + commit_db_size truncation, equivalent to a SQLite process replaying the WAL from
                // zero -- chain verification guarantees 1..up_to is contiguous with no gaps.
                std::fs::write(&tmp, b"")
                    .map_err(|e| PersistError::LocalIo(format!("restore write: {e}")))?;
            }
        }
        let base_lsn = plan.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0);
        if base_lsn > plan.up_to_lsn {
            return Err(PersistError::Corrupt("snapshot beyond manifest lsn".into()));
        }
        let mut page_size = 0u32;
        {
            // Read page_size from the snapshot (offset 16, 2 bytes BE; 1 means 65536).
            use std::io::{Read, Seek, SeekFrom};
            if plan.snapshot.is_some() {
                let mut f = std::fs::File::open(&tmp)
                    .map_err(|e| PersistError::LocalIo(format!("snap open: {e}")))?;
                let mut hdr = [0u8; 2];
                f.seek(SeekFrom::Start(16))
                    .and_then(|_| f.read_exact(&mut hdr))
                    .map_err(|_| PersistError::Corrupt("snapshot page_size read".into()))?;
                let v = u16::from_be_bytes(hdr) as u32;
                page_size = if v == 1 { 65536 } else { v };
                if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
                    return Err(PersistError::Corrupt("snapshot page_size invalid".into()));
                }
            }
        }
        // Replay (base_lsn, up_to_lsn] in order -- one lease-renewal check after each segment,
        // so the more segments, the longer the restore, yet the lease is not lost (RV-R31 #5).
        if plan.up_to_lsn > base_lsn {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp)
                .map_err(|e| PersistError::LocalIo(format!("restore open: {e}")))?;
            let mut prev = base_lsn;
            for lsn in (base_lsn + 1)..=plan.up_to_lsn {
                let epoch = epoch_for_lsn(&plan.chain, lsn)
                    .ok_or_else(|| PersistError::Corrupt(format!("no epoch covers lsn {lsn}")))?;
                let key = seg::seg_key(self.kind, database_id, epoch, lsn);
                let obj = self
                    .store_get(&key)?
                    .ok_or_else(|| PersistError::Corrupt(format!("segment lsn {lsn} missing")))?;
                let s = seg::decode(&obj.body)
                    .map_err(|e| PersistError::Corrupt(format!("seg {lsn}: {e}")))?;
                if s.database_id != database_id {
                    return Err(PersistError::Corrupt("seg database_id mismatch".into()));
                }
                if s.lsn != lsn || s.prev_lsn != prev {
                    return Err(PersistError::Corrupt(format!(
                        "seg chain break at lsn {lsn} (prev={prev}, seg.prev={})",
                        s.prev_lsn
                    )));
                }
                if page_size == 0 {
                    page_size = s.page_size;
                } else if s.page_size != page_size {
                    return Err(PersistError::Corrupt("seg page_size mismatch".into()));
                }
                seg::apply_frames(&mut f, &s)
                    .map_err(|e| PersistError::Corrupt(format!("seg {lsn} apply: {e}")))?;
                prev = lsn;
                self.restore_renew(database_id, handle)?;
            }
        }
        // After all of tmp is written -> fsync -> integrity_check (RV-R31 #1: refuse to open a
        // corrupt image -- integrity_check creates sidecar files in WAL mode, so verify first, then
        // clean). fsync before rename; fsync the parent directory after rename.
        std::fs::OpenOptions::new()
            .read(true)
            .open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| PersistError::LocalIo(format!("restore fsync: {e}")))?;
        match crate::wal::integrity_ok(&tmp) {
            Ok(true) => {}
            Ok(false) => {
                tracing::error!(
                    db = %database_id,
                    "restore image failed integrity_check — refusing to open"
                );
                return Err(PersistError::Corrupt(
                    "restored image failed integrity_check".into(),
                ));
            }
            Err(e) => {
                tracing::error!(db = %database_id, error = %e, "restore integrity_check errored");
                return Err(PersistError::Corrupt(format!("integrity_check: {e}")));
            }
        }
        // integrity_check may create wal/shm beside .restore.tmp -- clear them together with the
        // stale wal/shm (the restored state is a complete image). Must be cleared before
        // rename, otherwise an old wal beside the new image would be treated as live data.
        for stale in [
            dir.join("db.sqlite-wal"),
            dir.join("db.sqlite-shm"),
            dir.join(".restore.tmp-wal"),
            dir.join(".restore.tmp-shm"),
        ] {
            let _ = std::fs::remove_file(stale);
        }
        std::fs::rename(&tmp, &db_file)
            .map_err(|e| PersistError::LocalIo(format!("restore rename: {e}")))?;
        let df = std::fs::File::open(&dir)
            .map_err(|e| PersistError::LocalIo(format!("restore dir open: {e}")))?;
        df.sync_all()
            .map_err(|e| PersistError::LocalIo(format!("restore dir fsync: {e}")))?;
        // The marker is written only after all durable steps complete -- the wal has been rebuilt and zeroed.
        self.write_sidecar(
            &dir,
            &Sidecar {
                v: 1,
                lsn: plan.up_to_lsn,
                file_frames: 0,
                intent: false,
            },
        )?;
        // Lazy orphan segment GC (best-effort).
        self.gc_orphans(
            database_id,
            plan.up_to_lsn,
            &plan.chain,
            plan.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0),
        );
        Ok(())
    }

    /// Promote a verified follower generation to writer.
    ///
    /// Stages (all fail closed, in this order):
    /// 1. verify the generation file (shape, size cap, integrity, sha256) and
    ///    that its `(epoch, lsn)` is the manifest head;
    /// 2. upload the generation as the new epoch's baseline snapshot (an
    ///    object-store put, idempotent by content);
    /// 3. CAS the manifest to `epoch + 1` with a fresh lease, the promoted
    ///    snapshot and a new epoch span starting at `lsn + 1`;
    /// 4. adopt the generation as this writer's local `db.sqlite` plus the
    ///    `.vrt-lsn` sidecar, so a later claim needs no restore.
    ///
    /// A crash after step 3 is resumable: a rerun observes the promoted
    /// manifest (same lsn, same snapshot sha256, same epoch span) and only
    /// completes step 4. A writer still holding the old epoch cannot commit
    /// after step 3 because its manifest CAS carries the old etag.
    pub fn promote_from_generation(
        &self,
        database_id: &str,
        expected_epoch: u64,
        expected_lsn: u64,
        generation: &Path,
    ) -> Result<PromotedWriter, PersistError> {
        if expected_lsn == 0 {
            return Err(PersistError::InvalidGeneration(
                "an empty database cannot be promoted".into(),
            ));
        }
        let meta = std::fs::metadata(generation).map_err(|e| {
            PersistError::InvalidGeneration(format!(
                "generation stat {}: {e}",
                generation.display()
            ))
        })?;
        if !meta.is_file() || meta.len() == 0 {
            return Err(PersistError::InvalidGeneration(
                "generation is not a non-empty regular file".into(),
            ));
        }
        if meta.len() > self.cfg.snapshot_max_bytes {
            return Err(PersistError::SnapshotTooLarge(meta.len()));
        }
        match crate::wal::integrity_ok(generation) {
            Ok(true) => {}
            Ok(false) => {
                return Err(PersistError::InvalidGeneration(
                    "generation failed integrity_check".into(),
                ))
            }
            Err(e) => {
                return Err(PersistError::InvalidGeneration(format!(
                    "generation integrity_check: {e}"
                )))
            }
        }
        let generation_sha = sha256_file(generation)?;

        for _ in 0..3 {
            let Some((m, etag)) = self.read_manifest(database_id)? else {
                return Err(PersistError::InvalidGeneration(
                    "manifest does not exist".into(),
                ));
            };
            if m.tombstone {
                return Err(PersistError::Tombstoned);
            }

            // A rerun may observe the manifest BEFORE the promotion (the
            // caller's expected epoch) or AFTER it (the manifest already
            // carries the promoted epoch because the first attempt crashed
            // between CAS and local adoption). The promotion marker is the
            // only unambiguous signal: a normal writer takeover clears it.
            let prompted_epoch = m.epoch;
            let same_promotion = m.lsn == expected_lsn
                && m.promotion.as_deref() == Some(generation_sha.as_str())
                && (prompted_epoch == expected_epoch
                    || prompted_epoch == expected_epoch.saturating_add(1))
                && m.snapshot
                    .as_ref()
                    .is_some_and(|s| s.lsn == expected_lsn && s.sha256 == generation_sha)
                && m.chain.iter().any(|span| {
                    span.epoch == prompted_epoch && span.first_lsn == expected_lsn.saturating_add(1)
                });
            if same_promotion {
                let snapshot = m
                    .snapshot
                    .clone()
                    .ok_or_else(|| PersistError::InvalidGeneration("missing snapshot".into()))?;
                let db_file = self.adopt_generation(database_id, generation, expected_lsn)?;
                let handle = ManifestHandle::from_manifest(&m, etag);
                return Ok(PromotedWriter {
                    previous_epoch: prompted_epoch.saturating_sub(1),
                    previous_lsn: expected_lsn,
                    epoch: prompted_epoch,
                    lsn: m.lsn,
                    holder: m.holder,
                    snapshot,
                    db_file,
                    resumed: true,
                    handle,
                });
            }
            if m.epoch != expected_epoch || m.lsn != expected_lsn {
                return Err(PersistError::ManifestChanged {
                    expected_epoch,
                    expected_lsn,
                });
            }
            let owner = epoch_for_lsn(&m.chain, m.lsn);
            if owner != Some(m.epoch) {
                return Err(PersistError::InvalidGeneration(format!(
                    "manifest chain owner {owner:?} != epoch {}",
                    m.epoch
                )));
            }
            if m.chain
                .last()
                .is_some_and(|s| s.first_lsn > expected_lsn + 1)
            {
                return Err(PersistError::InvalidGeneration(
                    "manifest epoch chain already covers the next lsn".into(),
                ));
            }

            let next_epoch = m.epoch.saturating_add(1);
            let snapshot =
                self.upload_snapshot(database_id, next_epoch, expected_lsn, generation)?;
            if snapshot.sha256 != generation_sha {
                return Err(PersistError::Corrupt(
                    "uploaded snapshot sha256 differs from the generation".into(),
                ));
            }
            let now = self.clock.now_ms();
            let mut next = m.clone();
            next.epoch = next_epoch;
            next.lsn = expected_lsn;
            next.holder = self.cfg.holder.clone();
            next.expires_at_ms = now + self.cfg.lease_ms;
            next.tombstone = false;
            next.snapshot = Some(snapshot.clone());
            next.promotion = Some(generation_sha.clone());
            next.chain.push(EpochSpan {
                epoch: next_epoch,
                first_lsn: expected_lsn + 1,
            });

            match self.write_manifest_im(database_id, &etag, &next) {
                Ok(new_etag) => {
                    let db_file = self.adopt_generation(database_id, generation, expected_lsn)?;
                    let handle = ManifestHandle::from_manifest(&next, new_etag);
                    return Ok(PromotedWriter {
                        previous_epoch: expected_epoch,
                        previous_lsn: expected_lsn,
                        epoch: next_epoch,
                        lsn: expected_lsn,
                        holder: self.cfg.holder.clone(),
                        snapshot,
                        db_file,
                        resumed: false,
                        handle,
                    });
                }
                Err(PersistError::Store(StoreError::PreconditionFailed)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(PersistError::Store(StoreError::unavailable(
            true,
            "promotion CAS races exhausted",
        )))
    }

    /// Install a verified generation as this writer's local image.
    ///
    /// The generation is copied to a temp file in the writer's own db dir,
    /// fsynced and integrity-checked before it can replace `db.sqlite`. An
    /// existing local image is preserved as `db.sqlite.promote-backup-<id>`
    /// instead of being deleted. The sidecar is written last (at the promoted
    /// lsn with zero WAL frames, matching a fully checkpointed main file), so
    /// a later claim trusts the image without restoring.
    fn adopt_generation(
        &self,
        database_id: &str,
        generation: &Path,
        lsn: u64,
    ) -> Result<PathBuf, PersistError> {
        let db_file = paths::derive_db_file(&self.db_root, database_id)
            .map_err(|e| PersistError::LocalIo(format!("db file path: {e}")))?;
        let dir = db_file
            .parent()
            .ok_or_else(|| PersistError::LocalIo("db file has no parent".into()))?
            .to_path_buf();
        if db_file.exists() {
            if let Some(sc) = self.read_sidecar(&dir) {
                if sc.trusted(lsn) {
                    return Ok(db_file);
                }
            }
        }

        let tmp = dir.join(format!(".promote-{}.tmp", unique_suffix()));
        std::fs::copy(generation, &tmp)
            .map_err(|e| PersistError::LocalIo(format!("promotion copy: {e}")))?;
        std::fs::File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| PersistError::LocalIo(format!("promotion fsync: {e}")))?;
        match crate::wal::integrity_ok(&tmp) {
            Ok(true) => {}
            Ok(false) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(PersistError::InvalidGeneration(
                    "adopted image failed integrity_check".into(),
                ));
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(PersistError::InvalidGeneration(format!(
                    "adopted image integrity_check: {e}"
                )));
            }
        }
        clear_sqlite_sidecars(&tmp);

        if db_file.exists() {
            let backup = dir.join(format!("db.sqlite.promote-backup-{}", unique_suffix()));
            std::fs::rename(&db_file, &backup)
                .map_err(|e| PersistError::LocalIo(format!("promotion backup existing db: {e}")))?;
        }
        clear_sqlite_sidecars(&db_file);
        std::fs::rename(&tmp, &db_file)
            .map_err(|e| PersistError::LocalIo(format!("promotion rename: {e}")))?;
        let df = std::fs::File::open(&dir)
            .map_err(|e| PersistError::LocalIo(format!("promotion dir open: {e}")))?;
        df.sync_all()
            .map_err(|e| PersistError::LocalIo(format!("promotion dir fsync: {e}")))?;
        self.write_sidecar(
            &dir,
            &Sidecar {
                v: 1,
                lsn,
                file_frames: 0,
                intent: false,
            },
        )?;
        Ok(db_file)
    }

    /// Orphan object GC: list `db/{id}/`.
    /// - `.seg`: not on the chain / beyond committed / already covered by the baseline snapshot (`lsn <=
    ///   baseline_lsn`, snapshot semantics equivalent) -> delete.
    /// - `.snap`: only delete old baseline snapshots with `lsn < baseline_lsn` -- the baseline itself
    ///   and an in-flight snapshot (lsn >= baseline) are never touched (an in-flight uploader is finalized by the manifest CAS;
    ///   deleting it would leave restore without a baseline).
    /// RV-R66: per-run deletion cap (bounded); the remainder is left for the next round (deletion is idempotent).
    /// Deletion failures are no longer silent -- counted in `GcReport.failed` with a warn, retried next round.
    /// Note: this GC keeps only a bounded window (the newest [`SNAP_RETENTION`] snapshots and the
    /// segments above the older one). It is not a time-window PITR and does not retain history for
    /// an arbitrarily lagging follower; beyond the window recovery is a fresh bootstrap.
    pub fn gc_orphans(
        &self,
        database_id: &str,
        committed_lsn: u64,
        chain: &[EpochSpan],
        baseline_lsn: u64,
    ) -> GcReport {
        let mut rep = GcReport::default();
        let prefix = format!("{}/{database_id}/", self.kind.object_prefix());
        // best-effort GC: work with whatever page was obtained, a truncated page is used as normal too
        // (unlisted objects are collected next round; deleting a missing key is idempotent anyway).
        let page = match self.store_list(&prefix) {
            Ok(p) => p,
            Err(e) => {
                rep.list_failed = true;
                tracing::warn!(db = %database_id, error = %e, "gc_orphans list failed; retry next pass");
                return rep;
            }
        };
        // Keep the newest SNAP_RETENTION snapshots and every segment above the oldest kept
        // snapshot. A follower that is one checkpoint behind can then continue with the segment
        // chain instead of re-downloading the whole database. `baseline_lsn` remains the upper
        // bound (never delete a segment the current snapshot still needs).
        let mut snapshots: Vec<(u64, String)> = page
            .entries
            .iter()
            .filter_map(|e| parse_snap_name(&e.key).map(|lsn| (lsn, e.key.clone())))
            .collect();
        snapshots.sort_by(|a, b| b.0.cmp(&a.0));
        let kept_snapshots: HashSet<&str> = snapshots
            .iter()
            .take(SNAP_RETENTION)
            .map(|(_, key)| key.as_str())
            .collect();
        let mut protected_lsn = snapshots
            .iter()
            .take(SNAP_RETENTION)
            .map(|(lsn, _)| *lsn)
            .min()
            .unwrap_or(baseline_lsn)
            .min(baseline_lsn);
        // Time Travel retention: keep every snapshot inside the window, plus one baseline before
        // it so the oldest retained point remains restorable. Without metadata (older databases)
        // fall back to the bounded two-snapshot policy above.
        let mut time_travel_keep: Option<HashSet<u64>> = None;
        if self.time_travel_retention_ms > 0 {
            match self.snapshot_metas(database_id) {
                Ok(metas) if !metas.is_empty() => {
                    let cutoff = self
                        .clock
                        .now_ms()
                        .saturating_sub(self.time_travel_retention_ms);
                    let mut kept: Vec<&(String, SnapshotMeta)> = metas
                        .iter()
                        .filter(|(_, m)| m.committed_at_ms >= cutoff)
                        .collect();
                    if kept.is_empty() {
                        if let Some(newest) = metas.first() {
                            kept.push(newest);
                        }
                    }
                    let oldest_kept = kept.iter().map(|(_, m)| m.lsn).min().unwrap();
                    let baseline = metas.iter().find(|(_, m)| m.lsn < oldest_kept);
                    let mut keep: HashSet<u64> = kept.iter().map(|(_, m)| m.lsn).collect();
                    if let Some((_, m)) = baseline {
                        keep.insert(m.lsn);
                        protected_lsn = m.lsn.min(baseline_lsn);
                    } else {
                        protected_lsn = oldest_kept.saturating_sub(1).min(baseline_lsn);
                    }
                    // The manifest snapshot is always live even if its sidecar is missing/late.
                    keep.insert(baseline_lsn);
                    time_travel_keep = Some(keep);
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(db = %database_id, error = %e,
                    "time-travel GC could not read snapshot metadata; using bounded retention"),
            }
        }
        for e in page.entries {
            let doomed = if e.key.ends_with(".seg") {
                match parse_seg_name(&e.key) {
                    Some((ep, lsn)) => {
                        let covered =
                            epoch_for_lsn(chain, lsn) == Some(ep) && lsn <= committed_lsn;
                        !covered || lsn <= protected_lsn
                    }
                    None => false,
                }
            } else if e.key.ends_with(".snap") {
                match parse_snap_name(&e.key) {
                    Some(lsn) => match &time_travel_keep {
                        // Never touch a snapshot beyond the current manifest baseline (in-flight upload).
                        Some(keep) => lsn <= baseline_lsn && !keep.contains(&lsn),
                        None => lsn < baseline_lsn && !kept_snapshots.contains(e.key.as_str()),
                    },
                    None => false,
                }
            } else if e.key.contains("/snap-") && e.key.ends_with(".json") {
                match (parse_snap_meta_lsn(&e.key), &time_travel_keep) {
                    (Some(lsn), Some(keep)) => lsn <= baseline_lsn && !keep.contains(&lsn),
                    // Without Time Travel metadata is not managed by the legacy policy: it is
                    // tiny, and keeping it lets a later retention change use the history.
                    _ => false,
                }
            } else {
                false
            };
            if !doomed {
                continue;
            }
            if rep.deleted + rep.failed >= MAX_GC_DELETES_PER_PASS {
                rep.deferred += 1;
                continue;
            }
            match self.store_delete(&e.key) {
                Ok(_) => rep.deleted += 1,
                Err(err) => {
                    rep.failed += 1;
                    tracing::warn!(db = %database_id, error = %err, "gc_orphans delete failed; retry next pass");
                }
            }
        }
        rep
    }

    /// Append a Time Travel timeline chunk (`last_lsn` derives the key). Safe to re-upload.
    pub fn upload_timeline(
        &self,
        database_id: &str,
        entries: &[TimelineEntry],
    ) -> Result<(), PersistError> {
        let Some(last) = entries.last() else {
            return Ok(());
        };
        let bytes = timeline::encode_chunk(database_id, entries);
        self.store_put(&timeline::chunk_key(self.kind, database_id, last.lsn), &bytes)
    }

    /// Read and merge every timeline chunk for `database_id`.
    ///
    /// A truncated listing fails closed: resolving a time against a partial index could pick a
    /// commit that is not the newest at/before the target.
    pub fn read_timeline(&self, database_id: &str) -> Result<TimelineIndex, PersistError> {
        let prefix = format!("{}/{database_id}/", self.kind.object_prefix());
        let page = self.store_list(&prefix)?;
        if page.truncated {
            return Err(PersistError::Corrupt("timeline listing truncated".into()));
        }
        let mut idx = TimelineIndex::default();
        for e in page.entries {
            if !timeline::is_chunk_key(&e.key) {
                continue;
            }
            let Some(obj) = self.store_get(&e.key)? else {
                continue; // concurrent cleanup; the remaining chunks are still consistent
            };
            if obj.body.len() > timeline::MAX_CHUNK_BYTES {
                return Err(PersistError::Corrupt("timeline chunk oversize".into()));
            }
            let chunk: timeline::TimelineChunk = serde_json::from_slice(&obj.body)
                .map_err(|e| PersistError::Corrupt(format!("timeline chunk json: {e}")))?;
            if chunk.v != 1 || chunk.database_id != database_id {
                return Err(PersistError::Corrupt(
                    "timeline chunk version/database mismatch".into(),
                ));
            }
            for entry in chunk.entries {
                idx.insert(entry)
                    .map_err(|e| PersistError::Corrupt(format!("timeline: {e}")))?;
            }
        }
        Ok(idx)
    }

    /// Snapshot metadata sidecars (newest first).
    pub fn snapshot_metas(
        &self,
        database_id: &str,
    ) -> Result<Vec<(String, SnapshotMeta)>, PersistError> {
        let prefix = format!("{}/{database_id}/", self.kind.object_prefix());
        let page = self.store_list(&prefix)?;
        if page.truncated {
            return Err(PersistError::Corrupt("snapshot listing truncated".into()));
        }
        let mut out = Vec::new();
        for e in page.entries {
            if !e.key.ends_with(".json") || !e.key.contains("/snap-") {
                continue;
            }
            let Some(obj) = self.store_get(&e.key)? else {
                continue;
            };
            let meta: SnapshotMeta = serde_json::from_slice(&obj.body)
                .map_err(|e| PersistError::Corrupt(format!("snapshot meta json: {e}")))?;
            if meta.v != 1 || meta.database_id != database_id {
                continue;
            }
            let snap_key = format!("{}.snap", e.key.trim_end_matches(".json"));
            out.push((snap_key, meta));
        }
        out.sort_by(|a, b| b.1.lsn.cmp(&a.1.lsn));
        Ok(out)
    }

    /// Read the manifest (read-only helper for Time Travel; the writer path keeps the etag handle).
    pub fn read_manifest_doc(&self, database_id: &str) -> Result<Option<Manifest>, PersistError> {
        Ok(self.read_manifest(database_id)?.map(|(m, _)| m))
    }

    /// Local DB file path for a database.
    pub fn db_file_path(&self, database_id: &str) -> PathBuf {
        self.db_dir(database_id).join("db.sqlite")
    }

    /// Pick a restore baseline at or before `up_to_lsn`: the manifest snapshot when it qualifies,
    /// else the newest snapshot metadata sidecar. Returns `None` when the chain must start at 0.
    pub fn restore_plan(
        &self,
        database_id: &str,
        up_to_lsn: u64,
        chain: &[EpochSpan],
    ) -> Result<RestorePlan, PersistError> {
        let manifest = self.read_manifest_doc(database_id)?;
        let mut best: Option<SnapshotRef> = manifest
            .as_ref()
            .and_then(|m| m.snapshot.clone())
            .filter(|s| s.lsn <= up_to_lsn);
        for (key, meta) in self.snapshot_metas(database_id)? {
            if meta.lsn > up_to_lsn {
                continue;
            }
            if best.as_ref().is_some_and(|b| b.lsn >= meta.lsn) {
                continue;
            }
            best = Some(SnapshotRef {
                key,
                lsn: meta.lsn,
                sha256: meta.sha256,
                size: meta.size,
                committed_at_ms: meta.committed_at_ms,
            });
        }
        Ok(RestorePlan {
            snapshot: best,
            up_to_lsn,
            chain: chain.to_vec(),
        })
    }

    /// Publish the restored image as a **new** commit at `new_lsn` on the current epoch.
    ///
    /// The restored content becomes a fresh snapshot; lsn never rolls back. The epoch span is
    /// adjusted so the next commit is `new_lsn + 1`.
    pub fn publish_restored_state(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        db_file: &Path,
        new_lsn: u64,
    ) -> Result<(), PersistError> {
        let snap = self.upload_snapshot(database_id, handle.epoch, new_lsn, db_file)?;
        let mut next = handle.clone();
        next.lsn = new_lsn;
        next.snapshot = Some(snap.clone());
        next.commits_since_ckpt = 0;
        next.expires_at_ms = self.clock.now_ms() + self.cfg.lease_ms;
        match next.chain.last_mut() {
            Some(last) if last.epoch == handle.epoch => {
                // The restored state itself belongs to the new epoch; the next commit is new_lsn+1.
                last.first_lsn = new_lsn;
            }
            _ => next.chain.push(EpochSpan {
                epoch: handle.epoch,
                first_lsn: new_lsn,
            }),
        }
        match self.write_manifest_im(database_id, &handle.etag, &next.to_manifest(false)) {
            Ok(etag) => {
                handle.lsn = new_lsn;
                handle.snapshot = Some(snap);
                handle.commits_since_ckpt = 0;
                handle.expires_at_ms = next.expires_at_ms;
                handle.etag = etag;
                // Index the restored point immediately: a later `--at now` must resolve to the
                // state we just published, not to the pre-restore head.
                if let Err(e) = self.upload_timeline(
                    database_id,
                    &[TimelineEntry {
                        lsn: new_lsn,
                        epoch: handle.epoch,
                        committed_at_ms: self.clock.now_ms(),
                    }],
                ) {
                    tracing::warn!(db = %database_id, lsn = new_lsn, error = %e,
                        "restored-state timeline write failed (bookmark restore still works)");
                }
                self.write_sidecar(
                    &self.db_dir(database_id),
                    &Sidecar {
                        v: 1,
                        lsn: new_lsn,
                        file_frames: 0,
                        intent: false,
                    },
                )
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                Err(PersistError::Fenced)
            }
            Err(e) => Err(e),
        }
    }

    /// T36b graceful shutdown: set `expires_at` to now-margin (epoch/holder/lsn
    /// unchanged, not a tombstone) -- the next writer takes over immediately without waiting out the lease.
    /// CAS failure = lease already taken (someone took over or it expired and was seized) -> idempotent Ok;
    /// real errors (store unavailable, etc.) propagate upward for the caller to warn.
    pub fn release(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        now_ms: u64,
    ) -> Result<(), PersistError> {
        // RV-R56: the taker needs `now >= expires + margin` to take over, so release
        // winds expires back by one margin (saturating) so the next writer can take over at once; this
        // engine is then forcibly fenced and will not keep serving inside the wound-back window.
        handle.expires_at_ms = now_ms.saturating_sub(self.cfg.clock_skew_margin_ms);
        match self.write_manifest_im(database_id, &handle.etag, &handle.to_manifest(false)) {
            Ok(etag) => {
                handle.etag = etag;
                Ok(())
            }
            // Authority already lost (including after tombstone): nothing to release.
            Err(PersistError::Store(StoreError::PreconditionFailed)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// manifest tombstone (step one of section 7 delete): fence all old writers.
    /// Idempotent; no manifest -> directly create a tombstoned manifest.
    pub fn tombstone(&self, database_id: &str) -> Result<(), PersistError> {
        for _ in 0..3 {
            match self.read_manifest(database_id)? {
                None => {
                    let m = Manifest {
                        v: 1,
                        epoch: 0,
                        lsn: 0,
                        holder: String::new(),
                        expires_at_ms: 0,
                        tombstone: true,
                        snapshot: None,
                        chain: Vec::new(),
                        promotion: None,
                    };
                    match self.write_manifest_inm(database_id, &m) {
                        Ok(_) => return Ok(()),
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Some((mut m, etag)) => {
                    if m.tombstone {
                        return Ok(());
                    }
                    m.tombstone = true;
                    match self.write_manifest_im(database_id, &etag, &m) {
                        Ok(_) => return Ok(()),
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Err(PersistError::Store(StoreError::unavailable(
            true,
            "tombstone races exhausted",
        )))
    }

    /// Reclaim segments/snapshots/manifest after the tombstone retention period (called lazily).
    /// RV-R53.3: returns a typed result -- the caller decides from it whether to write the `.gc-done`
    /// side sentinel, so finished items do not occupy each round's scan quota forever (starvation fix).
    pub fn gc_tombstoned(&self, database_id: &str) -> Result<GcTombstonedOutcome, PersistError> {
        let Some((m, _)) = self.read_manifest(database_id)? else {
            // The manifest is already reclaimed (or a marker with no manifest) -- the object prefix
            // may still hold leftover segments; the listing must be walked through before declaring done.
            return self.gc_tombstoned_prefix(database_id, true);
        };
        if !m.tombstone {
            // The marker exists but the manifest is not yet tombstoned -- state has not converged,
            // keep it in the candidate set to re-check next round (do not write done).
            return Ok(GcTombstonedOutcome::Retained);
        }
        let now = self.clock.now_ms();
        if now.saturating_sub(m.expires_at_ms) < self.cfg.retention_ms {
            return Ok(GcTombstonedOutcome::Retained);
        }
        self.gc_tombstoned_prefix(database_id, false)
    }

    /// List and delete one page of `db/{id}/` (or `q/{id}/`). A truncated listing reports a retryable error,
    /// never pretending it is all deleted.
    fn gc_tombstoned_prefix(
        &self,
        database_id: &str,
        manifest_absent: bool,
    ) -> Result<GcTombstonedOutcome, PersistError> {
        let mut freed = 0u64;
        let page = self.store_list(&format!("{}/{database_id}/", self.kind.object_prefix()))?;
        for e in page.entries {
            let _ = self.store_delete(&e.key);
            freed += 1;
        }
        if page.truncated {
            // RV-R8 #7: one page cannot delete everything -- report progress and let the caller retry to converge,
            // never pretend everything was deleted.
            return Err(PersistError::Store(StoreError::unavailable(
                true,
                "tombstoned gc: listing truncated — retry to converge",
            )));
        }
        if freed == 0 && manifest_absent {
            return Ok(GcTombstonedOutcome::Empty);
        }
        Ok(GcTombstonedOutcome::Reclaimed(freed))
    }
}

/// RV-R53.3: result of one `gc_tombstoned` round. `Empty`/`Reclaimed` mean the
/// marker's objects are fully deleted (a `.gc-done` side sentinel may be written to leave the candidate set);
/// `Retained` stays in the set to be re-checked next round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcTombstonedOutcome {
    /// No manifest and the object prefix is already empty -- terminal.
    Empty,
    /// Retention period has passed; this round deleted `freed` objects -- terminal (prefix empty).
    Reclaimed(u64),
    /// Retention period not reached or manifest not tombstoned -- re-check next round.
    Retained,
}

enum ClaimStep {
    Retry,
    Done(ClaimOutcome),
}

fn sha256_file(path: &Path) -> Result<String, PersistError> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| PersistError::LocalIo(format!("sha256 open: {e}")))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| PersistError::LocalIo(format!("sha256 read: {e}")))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn clear_sqlite_sidecars(path: &Path) {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut side = path.as_os_str().to_os_string();
        side.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(side));
    }
}

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

/// Parse the epoch/lsn of `db/{id}/{epoch}/{lsn}.seg`.
/// `snap-{lsn:016x}.snap` -> lsn (a snapshot needs no epoch -- only the baseline remains in the directory).
fn parse_snap_name(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let rest = name.strip_prefix("snap-")?;
    let lsn = rest.strip_suffix(".snap")?;
    u64::from_str_radix(lsn, 16).ok()
}

fn parse_snap_meta_lsn(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let rest = name.strip_prefix("snap-")?;
    let lsn = rest.strip_suffix(".json")?;
    u64::from_str_radix(lsn, 16).ok()
}

fn parse_seg_name(key: &str) -> Option<(u64, u64)> {
    let name = key.rsplit('/').next()?;
    let lsn = name.strip_suffix(".seg")?;
    let lsn = u64::from_str_radix(lsn, 16).ok()?;
    let mut it = key.rsplit('/');
    let _ = it.next();
    let ep = it.next()?;
    let ep = u64::from_str_radix(ep, 16).ok()?;
    Some((ep, lsn))
}

/// RV-R56: whether the holder may still serve -- `now < expires - margin`.
/// Paired with [`taker_may_claim`]: the holder stops serving before `expires - margin`,
/// and the taker only takes over after `expires + margin`, leaving a gap of 2*margin between them;
/// with clock skew < margin both sides can never serve at the same time.
pub(crate) fn holder_usable(expires_at_ms: u64, now_ms: u64, margin_ms: u64) -> bool {
    expires_at_ms.saturating_sub(margin_ms) > now_ms
}

/// RV-R56: whether the taker may take over someone else's lease -- `now >= expires + margin` (saturating add).
pub(crate) fn taker_may_claim(expires_at_ms: u64, now_ms: u64, margin_ms: u64) -> bool {
    now_ms >= expires_at_ms.saturating_add(margin_ms)
}

#[cfg(test)]
mod rv_r56_tests {
    use super::*;

    /// Verified (2026-11-15): within the boundary [expires-margin, expires+margin) neither side is usable.
    #[test]
    fn lease_margin_inequalities() {
        let (exp, m) = (10_000u64, 500u64);
        assert!(holder_usable(exp, exp - m - 1, m));
        assert!(!holder_usable(exp, exp - m, m));
        assert!(!taker_may_claim(exp, exp - m, m));
        assert!(!taker_may_claim(exp, exp, m));
        assert!(!taker_may_claim(exp, exp + m - 1, m));
        assert!(taker_may_claim(exp, exp + m, m));
        for now in (exp - m)..(exp + m) {
            assert!(!(holder_usable(exp, now, m) && taker_may_claim(exp, now, m)));
        }
        assert!(!taker_may_claim(u64::MAX, u64::MAX - 1, 5));
    }
}
