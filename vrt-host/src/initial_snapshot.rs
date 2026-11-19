//! Bounded in-process wait for the FIRST `compute.desired` snapshot.
//!
//! The implementation lives in `vrt_provider::initial_snapshot` (the
//! provider sidecar needs the identical start-race handling); this
//! module re-exports it so existing host call sites and the host-side
//! contract tests keep working unchanged.
//!
//! * `NotFound` -> keep waiting (bounded, then fail closed);
//! * any other read error or ANY verification failure -> fail at once.

pub use vrt_provider::initial_snapshot::{
    wait_for_initial_snapshot, InitialSnapshotError, INITIAL_SNAPSHOT_POLL_MAX,
    INITIAL_SNAPSHOT_POLL_MIN, INITIAL_SNAPSHOT_WAIT,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The host keeps the exact start-race contract through the shared
    /// implementation (full behavioural tests live in vrt-provider).
    #[tokio::test]
    async fn reexport_keeps_notfound_wait_then_fail_closed() {
        let nf = || {
            Err(vrt_provider::desired_file::FileLoadError::Io(
                std::io::Error::from(std::io::ErrorKind::NotFound),
            ))
        };
        let err = wait_for_initial_snapshot(
            nf,
            Duration::from_millis(20),
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, InitialSnapshotError::Timeout(_)));
    }
}
