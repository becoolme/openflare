//! Offline D1 Time Travel: restore a database to a historical commit.
//!
//! The writer must be stopped: the command takes the same local flock as the engine, so a live
//! `vrt-sql serve` makes it fail closed instead of racing a writer.
//!
//! Semantics:
//! - `--at <unix-ms>` resolves against the commit timeline (written at checkpoints and every
//!   [`crate::timeline::FLUSH_COMMITS`] commits) to the newest commit at or before the target;
//! - `--bookmark` restores exactly the signed `(epoch, lsn)` point (undo path);
//! - the restored image is published as a **new commit** (`newLsn = head + 1`, epoch from the
//!   takeover claim), never by rolling the manifest lsn back;
//! - the pre-restore head remains addressable through `previousBookmark` as long as its
//!   snapshot/segments are retained (see `timeTravelRetentionMs`).
//!
//! This is not an online API: run it while the data plane is stopped, then restart the writer.

use crate::bookmark::BookmarkSigner;
use crate::persist::{epoch_for_lsn, PersistError, Persister, RestorePlan};
use crate::paths;
use crate::timeline::TimelineEntry;
use serde::Serialize;
use std::path::{Path, PathBuf};

/// Time Travel operation error (all fail closed).
#[derive(Debug, thiserror::Error)]
pub enum TimeTravelError {
    #[error("writer appears to be running (database lock is held)")]
    LockBusy,
    #[error("writer lease is still active until {0}")]
    LeaseActive(u64),
    #[error("database is tombstoned")]
    Tombstoned,
    #[error("no indexed commit at or before the target time")]
    NoTarget,
    #[error("target is outside the retained window: {0}")]
    OutsideWindow(String),
    #[error("bookmark invalid")]
    Bookmark,
    #[error("inconsistent state: {0}")]
    Inconsistent(String),
    #[error("persistence: {0}")]
    Persist(#[from] PersistError),
    #[error("io: {0}")]
    Io(String),
    #[error("local io: {0}")]
    Local(String),
}

/// Dry-run / apply result.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeTravelOutcome {
    pub database_id: String,
    /// Requested time, when the target was time-based.
    pub target_ms: Option<u64>,
    /// Resolved commit.
    pub resolved_epoch: u64,
    pub resolved_lsn: u64,
    pub resolved_at_ms: u64,
    /// Snapshot baseline used (None = replay from an empty image).
    pub snapshot_lsn: Option<u64>,
    /// Segments replayed after the snapshot.
    pub segments_replayed: u64,
    pub previous_epoch: u64,
    pub previous_lsn: u64,
    pub new_epoch: u64,
    pub new_lsn: u64,
    pub previous_bookmark: Option<String>,
    pub new_bookmark: Option<String>,
    pub dry_run: bool,
}

/// Restore `database_id` to `target_ms` (or a signed `bookmark`).
///
/// The caller owns the `Persister` (it must use the same db root and persistence config as the
/// writer). `signer` is optional; without it bookmarks are neither accepted nor returned.
pub fn restore(
    persister: &Persister,
    database_id: &str,
    target_ms: Option<u64>,
    bookmark: Option<&str>,
    signer: Option<&BookmarkSigner>,
    dry_run: bool,
) -> Result<TimeTravelOutcome, TimeTravelError> {
    if paths::validate_database_id(database_id).is_err() {
        return Err(TimeTravelError::Inconsistent(
            "database id must be 26 lowercase alphanumerics".into(),
        ));
    }
    if target_ms.is_none() && bookmark.is_none() {
        return Err(TimeTravelError::Inconsistent(
            "provide --at <unix-ms> or --bookmark".into(),
        ));
    }
    // Serialize with a live writer: same lock file the engine uses.
    let db_file = persister.db_file_path(database_id);
    if let Some(parent) = db_file.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| TimeTravelError::Local(format!("db dir: {e}")))?;
    }
    let _lock = DbLock::acquire(&db_file)?;

    let before = persister
        .read_manifest_doc(database_id)?
        .ok_or_else(|| TimeTravelError::OutsideWindow("no manifest".into()))?;
    if before.tombstone {
        return Err(TimeTravelError::Tombstoned);
    }
    let previous_epoch = before.epoch;
    let previous_lsn = before.lsn;
    let timeline = persister.read_timeline(database_id)?;

    // Resolve the target to a commit point.
    let resolved = if let Some(bm) = bookmark {
        let signer = signer.ok_or(TimeTravelError::Bookmark)?;
        let (epoch, lsn) = signer.verify(database_id, bm).map_err(|_| TimeTravelError::Bookmark)?;
        if lsn == 0 || lsn > previous_lsn {
            return Err(TimeTravelError::Bookmark);
        }
        if epoch_for_lsn(&before.chain, lsn) != Some(epoch) {
            return Err(TimeTravelError::Bookmark);
        }
        let at = timeline
            .at_or_before_lsn(lsn)
            .map(|e| e.committed_at_ms)
            .unwrap_or(0);
        TimelineEntry {
            lsn,
            epoch,
            committed_at_ms: at,
        }
    } else {
        let target = target_ms.expect("checked");
        timeline
            .resolve_at(target)
            .filter(|e| e.lsn <= previous_lsn)
            .ok_or(TimeTravelError::NoTarget)?
    };
    if resolved.lsn == 0 {
        return Err(TimeTravelError::NoTarget);
    }

    let plan: RestorePlan =
        persister.restore_plan(database_id, resolved.lsn, &before.chain)?;
    let snapshot_lsn = plan.snapshot.as_ref().map(|s| s.lsn);
    let segments = resolved
        .lsn
        .saturating_sub(snapshot_lsn.unwrap_or(0));
    let previous_bookmark = signer.map(|s| s.sign(database_id, previous_epoch, previous_lsn));

    if dry_run {
        return Ok(TimeTravelOutcome {
            database_id: database_id.into(),
            target_ms,
            resolved_epoch: resolved.epoch,
            resolved_lsn: resolved.lsn,
            resolved_at_ms: resolved.committed_at_ms,
            snapshot_lsn,
            segments_replayed: segments,
            previous_epoch,
            previous_lsn,
            new_epoch: previous_epoch,
            new_lsn: previous_lsn,
            previous_bookmark,
            new_bookmark: None,
            dry_run: true,
        });
    }

    // Claim a fresh epoch (this also rejects a live writer / tombstone).
    let (mut handle, _) = match persister.claim(database_id)? {
        crate::persist::ClaimOutcome::Ready { handle, restore } => (handle, restore),
        crate::persist::ClaimOutcome::HeldByOther { expires_at_ms } => {
            return Err(TimeTravelError::LeaseActive(expires_at_ms))
        }
        crate::persist::ClaimOutcome::Tombstoned => return Err(TimeTravelError::Tombstoned),
    };
    let new_epoch = handle.epoch;
    let new_lsn = previous_lsn.saturating_add(1);

    // Rebuild the historical image into the local db, then publish it as a new commit.
    let result = (|| -> Result<(), TimeTravelError> {
        persister.restore(database_id, &plan, &mut handle)?;
        let db_file = persister.db_file_path(database_id);
        persister.publish_restored_state(database_id, &mut handle, &db_file, new_lsn)?;
        Ok(())
    })();
    let now = persister.clock().now_ms();
    let _ = persister.release(database_id, &mut handle, now);
    result?;

    Ok(TimeTravelOutcome {
        database_id: database_id.into(),
        target_ms,
        resolved_epoch: resolved.epoch,
        resolved_lsn: resolved.lsn,
        resolved_at_ms: resolved.committed_at_ms,
        snapshot_lsn,
        segments_replayed: segments,
        previous_epoch,
        previous_lsn,
        new_epoch,
        new_lsn,
        previous_bookmark,
        new_bookmark: signer.map(|s| s.sign(database_id, new_epoch, new_lsn)),
        dry_run: false,
    })
}

/// Local advisory lock matching `RusqliteEngine`'s `db.lock`.
struct DbLock {
    file: std::fs::File,
}

impl DbLock {
    fn acquire(db_file: &Path) -> Result<Self, TimeTravelError> {
        let path = db_file.with_extension("lock");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| TimeTravelError::Local(format!("lock open: {e}")))?;
        // SAFETY: flock only needs a valid fd; the File keeps it open for the guard's lifetime.
        let rc = unsafe { libc::flock(std::os::unix::io::AsRawFd::as_raw_fd(&file), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(TimeTravelError::LockBusy);
        }
        Ok(Self { file })
    }
}

impl Drop for DbLock {
    fn drop(&mut self) {
        // SAFETY: fd is still open; best-effort unlock before close.
        unsafe {
            libc::flock(std::os::unix::io::AsRawFd::as_raw_fd(&self.file), libc::LOCK_UN);
        }
    }
}

/// Config for the offline command (JSON/TOML).
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimeTravelConfig {
    pub db_root: PathBuf,
    pub persistence: crate::service::PersistenceConfig,
    /// Bookmark signing key (0600). Required to use or return bookmarks.
    #[serde(default)]
    pub bookmark_secret_file: Option<PathBuf>,
}

impl TimeTravelConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.persistence.validate()?;
        if self.persistence.backend == "none" {
            return Err("time travel requires persistence.backend fs|s3");
        }
        Ok(())
    }
}


