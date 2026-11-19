//! Bounded in-process wait for the FIRST `compute.desired` snapshot.
//!
//! The host/provider and the cell-controller start concurrently inside one pod;
//! the controller writes the snapshot file only after it pulled and
//! verified it. Exiting on "file not there yet" turned a normal start
//! race into a CrashLoopBackOff (up to 5 min of dead time). This waits
//! in-process instead — but ONLY for the file being absent:
//!
//! * `NotFound` → keep waiting (bounded by `deadline`, then fail closed);
//! * any other read error, or ANY verification failure (bad signature,
//!   wrong cell, expired, malformed) → fail immediately. A snapshot that
//!   exists but is untrustworthy must never be waited out and must never
//!   degrade into an empty/unverified start.
//!
//! The caller's listeners are not bound until this returns, so the
//! readiness probe stays NOT READY for the whole wait (the pod needs a
//! startupProbe covering the wait so liveness is held off meanwhile).
//!
//! Shared by vrt-host and vrt-provider (vrt-host re-exports this module).

use std::time::{Duration, Instant};
use crate::desired_file::FileLoadError;

/// Total wait before giving up (the pod then restarts as before).
pub const INITIAL_SNAPSHOT_WAIT: Duration = Duration::from_secs(10 * 60);
/// First poll delay (doubles up to the cap).
pub const INITIAL_SNAPSHOT_POLL_MIN: Duration = Duration::from_millis(500);
/// Poll delay cap.
pub const INITIAL_SNAPSHOT_POLL_MAX: Duration = Duration::from_secs(5);

/// Why the initial snapshot could not be installed.
#[derive(Debug, thiserror::Error)]
pub enum InitialSnapshotError {
    /// The snapshot file never appeared within the deadline.
    #[error("no initial snapshot within {0:?}")]
    Timeout(Duration),
    /// The snapshot exists but could not be read/verified — fail closed.
    #[error("initial snapshot rejected: {0}")]
    Rejected(#[source] FileLoadError),
}

/// Poll `reload` until it succeeds, the file stays absent past
/// `deadline`, or a non-`NotFound` failure occurs.
pub async fn wait_for_initial_snapshot<F>(
    mut reload: F,
    deadline: Duration,
    poll_min: Duration,
    poll_max: Duration,
) -> Result<u64, InitialSnapshotError>
where
    F: FnMut() -> Result<u64, FileLoadError>,
{
    let started = Instant::now();
    let mut delay = poll_min;
    let mut next_log = Duration::ZERO;
    loop {
        match reload() {
            Ok(revision) => return Ok(revision),
            Err(FileLoadError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                let elapsed = started.elapsed();
                if elapsed >= deadline {
                    return Err(InitialSnapshotError::Timeout(deadline));
                }
                if elapsed >= next_log {
                    tracing::warn!(
                        waited_secs = elapsed.as_secs(),
                        "waiting for the first compute.desired snapshot (not ready)"
                    );
                    next_log = elapsed + Duration::from_secs(30);
                }
                let remaining = deadline.saturating_sub(elapsed);
                tokio::time::sleep(delay.min(remaining)).await;
                delay = (delay * 2).min(poll_max);
            }
            Err(other) => return Err(InitialSnapshotError::Rejected(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const MS: Duration = Duration::from_millis(1);

    fn not_found() -> FileLoadError {
        FileLoadError::Io(std::io::Error::from(std::io::ErrorKind::NotFound))
    }

    #[tokio::test]
    async fn waits_for_absent_file_then_loads() {
        let calls = Cell::new(0u32);
        let out = wait_for_initial_snapshot(
            || {
                calls.set(calls.get() + 1);
                if calls.get() < 3 {
                    Err(not_found())
                } else {
                    Ok(7)
                }
            },
            Duration::from_secs(5),
            MS,
            MS,
        )
        .await
        .unwrap();
        assert_eq!(out, 7);
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn absent_past_deadline_fails_closed() {
        let err = wait_for_initial_snapshot(|| Err(not_found()), 20 * MS, MS, 2 * MS)
            .await
            .unwrap_err();
        assert!(matches!(err, InitialSnapshotError::Timeout(_)));
    }

    #[tokio::test]
    async fn non_notfound_io_error_is_not_waited_out() {
        let calls = Cell::new(0u32);
        let err = wait_for_initial_snapshot(
            || {
                calls.set(calls.get() + 1);
                Err(FileLoadError::Io(std::io::Error::from(
                    std::io::ErrorKind::PermissionDenied,
                )))
            },
            Duration::from_secs(5),
            MS,
            MS,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, InitialSnapshotError::Rejected(_)));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn existing_but_unverifiable_snapshot_fails_immediately() {
        use crate::desired_file::FileDesiredStore;
        let dir = vrt_securefs::test_short_root("vrt-initsnap").unwrap();
        let path = dir.join("desired.json");
        std::fs::write(&path, b"{\"not\":\"a signed envelope\"}").unwrap();
        let store = FileDesiredStore::new(&path, "cell-1", vec![], 2);
        let calls = Cell::new(0u32);
        let err = wait_for_initial_snapshot(
            || {
                calls.set(calls.get() + 1);
                store.reload(chrono::Utc::now())
            },
            Duration::from_secs(5),
            MS,
            MS,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            InitialSnapshotError::Rejected(FileLoadError::Verify(_))
        ));
        assert_eq!(calls.get(), 1);
        // The store stayed empty — no silent empty-set start.
        assert!(store.revision().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn file_appearing_late_is_picked_up_from_disk() {
        use crate::desired_file::FileDesiredStore;
        let dir = vrt_securefs::test_short_root("vrt-initsnap-late").unwrap();
        let path = dir.join("envelope.json");
        let store = FileDesiredStore::new(&path, "cell-1", vec![], 2);
        let writer_path = path.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            // present but unverifiable -> must be rejected immediately, not waited out
            std::fs::write(&writer_path, b"garbage").unwrap();
        });
        let err = wait_for_initial_snapshot(
            || store.reload(chrono::Utc::now()),
            Duration::from_secs(5),
            MS,
            2 * MS,
        )
        .await
        .unwrap_err();
        writer.await.unwrap();
        assert!(matches!(
            err,
            InitialSnapshotError::Rejected(FileLoadError::Verify(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
