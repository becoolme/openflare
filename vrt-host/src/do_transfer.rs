//! DO1 `transferred_classes` cell-side handoff (doc §6 transfer
//! state machine).
//!
//! A class row moved `srcWorker → dstWorker` keeping `uniqueKey`.
//! On-disk that is `<tenant>/<src>/<key>` → `<tenant>/<dst>/<key>`.
//! The launch path runs the handoff AFTER the placement lease for
//! the destination scope is acquired and BEFORE workerd opens the
//! class dir:
//!
//! ```text
//!   lease acquired (dst scope, epoch N+1; src writer fenced/dead)
//!        │
//!        ├─ <src>/<key> exists, <dst>/<key> absent/empty
//!        │      → atomic rename (+ epoch siblings)      [same cell]
//!        ├─ <src>/<key> exists, <dst>/<key> non-empty
//!        │      → dst wins (written under ≥ epoch);
//!        │        src quarantined                       [crash residue]
//!        └─ <src>/<key> absent
//!               → cross-cell: snapshot restore decides;
//!                 no committed snapshot ⇒ FAIL CLOSED
//!                 (retry; never open an empty dir)
//! ```
//!
//! Every failure keeps the source dir untouched — the move is either
//! a single atomic rename or nothing.

use std::path::{Path, PathBuf};

/// DO1-r2 P1-1: quarantine retention marker — written next to a
/// quarantined source dir as `<name>.since` containing the unix-ms
/// quarantine time. The stale sweep keeps the dir until the marker's
/// age exceeds the configured retention; a missing/corrupt marker is
/// treated as "just quarantined" (fail closed — keep, never early-
/// delete data whose provenance is in doubt).
pub const QUARANTINE_SINCE_SUFFIX: &str = ".since";
pub const QUARANTINE_SUFFIX: &str = ".xfer-quarantine";

/// Write the retention marker for `quarantine_name` inside
/// `worker_dir`; the marker is best-effort (a failed write means the
/// sweep keeps the dir — the conservative outcome).
pub fn mark_quarantine(worker_dir: &Path, quarantine_name: &str) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let _ = std::fs::write(
        worker_dir.join(format!("{quarantine_name}{QUARANTINE_SINCE_SUFFIX}")),
        now_ms.to_string(),
    );
}

/// Read a quarantine marker's age in ms; `None` when absent/corrupt.
pub fn quarantine_age_ms(worker_dir: &Path, quarantine_name: &str, now_ms: u64) -> Option<u64> {
    let body = std::fs::read_to_string(
        worker_dir.join(format!("{quarantine_name}{QUARANTINE_SINCE_SUFFIX}")),
    )
    .ok()?;
    let at: u64 = body.trim().parse().ok()?;
    Some(now_ms.saturating_sub(at))
}

/// Sibling lease/epoch bookkeeping files that must move WITH the
/// class dir — `crate::do_placement::LEASE_FILE_SUFFIXES` is the
/// single name contract the sweep maps through.
pub fn sibling_paths(worker_dir: &Path, class_key: &str) -> Vec<PathBuf> {
    crate::do_placement::LEASE_FILE_SUFFIXES
        .iter()
        .map(|s| worker_dir.join(format!("{class_key}{s}")))
        .collect()
}

/// Non-empty-dir probe shared with the launch handoff gate.
pub fn dir_nonempty(dir: &Path) -> bool {
    dir.is_dir()
        && std::fs::read_dir(dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffMove {
    /// Source dir (+ epoch siblings) renamed into the destination.
    Moved,
    /// Destination already has data — the source dir was fenced
    /// provenance; it is renamed aside as `<key>.xfer-quarantine`
    /// for the stale-sweep to reclaim (never silently dual-served).
    Quarantined,
    /// No local source dir — cross-cell path; the snapshot restore
    /// owns the decision.
    NoLocalSource,
}

#[derive(Debug)]
pub enum HandoffError {
    Io(std::io::Error),
}

impl std::fmt::Display for HandoffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "handoff io: {e}"),
        }
    }
}
impl std::error::Error for HandoffError {}

/// Move `<tenant_dir>/<src_worker>/<class_key>` → `dst_dir`
/// (`<tenant_dir>/<dst_worker>/<class_key>`) including the sibling
/// epoch files. Caller MUST hold the destination's placement lease —
/// the acquire is what fenced the old owner, so after this rename the
/// dst dir is the sole writable copy. Atomicity: each entry moves by
/// one `rename(2)` on the same filesystem (both workers share the
/// tenant root); a failure mid-sibling-move leaves the class dir
/// itself already placed — the remaining siblings are bookkeeping,
/// retried on the next attempt (the dir no longer exists at src so
/// `NoLocalSource` and dst non-empty wins).
pub fn move_class_dir(
    tenant_dir: &Path,
    src_worker: &str,
    class_key: &str,
    dst_dir: &Path,
) -> Result<HandoffMove, HandoffError> {
    let src_dir = tenant_dir.join(src_worker).join(class_key);
    if !src_dir.is_dir() {
        return Ok(HandoffMove::NoLocalSource);
    }
    if dir_nonempty(dst_dir) {
        // dst wins — it exists non-empty, meaning it was written
        // under a lease epoch ≥ the source's fenced one (or already
        // restored). Quarantine the stale source aside; the sweep
        // reclaims it only after the retention window (P1-1 — the
        // dst data may itself be a stale restore, so the source is
        // evidence, not garbage).
        quarantine_src(tenant_dir, src_worker, class_key).map_err(HandoffError::Io)?;
        return Ok(HandoffMove::Quarantined);
    }
    // dst missing or an empty stub (an interrupted earlier attempt /
    // a rolled-back old cell that created the dir) — remove the empty
    // stub so the rename lands cleanly. A NON-empty dst is handled
    // above; an empty one carries no data.
    if dst_dir.exists() {
        std::fs::remove_dir(dst_dir).map_err(HandoffError::Io)?;
    }
    let src_worker_dir = tenant_dir.join(src_worker);
    let dst_worker_dir = dst_dir.parent().unwrap_or(tenant_dir);
    std::fs::create_dir_all(dst_worker_dir).map_err(HandoffError::Io)?;
    std::fs::rename(&src_dir, dst_dir).map_err(HandoffError::Io)?;
    // Sibling lease/epoch files follow the dir — `<key>.epoch`
    // (LocalPlacement durable epoch) and `<key>.lease-epoch` (the
    // provenance marker the stale-dir guard reads).
    for sib in sibling_paths(&src_worker_dir, class_key) {
        if !sib.exists() {
            continue;
        }
        let Some(name) = sib.file_name() else {
            continue;
        };
        let _ = std::fs::rename(&sib, dst_worker_dir.join(name));
    }
    Ok(HandoffMove::Moved)
}

/// DO1-r4: the target-side restore gate for a `transferred_classes`
/// class — one pure predicate over (committed snapshot, persisted
/// acceptance, the epoch WE acquired, the record's `writer_epoch`).
/// Returns true iff serving this snapshot cannot drop provable writes:
///
/// - `Final` is accepted ONLY when committed under the epoch of the
///   LAST REAL WRITER (`snap.epoch == writer_epoch`). `writer_epoch`
///   is claimed by CAS right before workerd opens the dir, so an
///   acquire-only epoch (transient restore failure, host restart,
///   sibling class's fail-closed releasing every lease) NEVER
///   advances it — a retry at a later acquire epoch still matches
///   the same Final. This is the r4 liveness fix over r3's strict
///   `epoch+1` rule, which made any such retry permanently stuck.
/// - `writer_epoch == 0` (pre-r4 record or never-launched): fall
///   back to the r3 rule `snap.epoch + 1 == acquired_epoch`. Legacy
///   records keep r3 semantics for the rolling-upgrade window;
///   correctness is preserved (an old-epoch Final still can't stand
///   in when an intermediate holder wrote), only the r4 liveness is
///   unavailable until an r4 host first claims the writer epoch.
/// - ANY kind (in practice `Periodic`) is accepted when the record's
///   `accepted_baseline` pins THIS snapshot's exact {epoch, seq} —
///   the operator's explicit RPO acceptance (DO1-r3 P0-1: bound to
///   the snapshot, cleared by every later commit, never sticky).
pub fn transfer_baseline_ok(
    snap: &crate::do_placement_store::SnapRef,
    accepted: Option<crate::do_placement_store::AcceptedBaseline>,
    acquired_epoch: u64,
    writer_epoch: u64,
) -> bool {
    if accepted.is_some_and(|a| a.epoch == snap.epoch && a.seq == snap.seq) {
        return true;
    }
    if !matches!(snap.kind, crate::do_placement_store::SnapKind::Final) {
        return false;
    }
    if writer_epoch == 0 {
        snap.epoch + 1 == acquired_epoch // legacy fallback — see above
    } else {
        snap.epoch == writer_epoch
    }
}

/// Move `<tenant>/<src_worker>/<key>` (+ siblings) aside as
/// `<key>.xfer-quarantine` and stamp the retention marker. Shared by
/// the dst-wins handoff path AND the supervisor's terminal-fenced
/// pending path — both mean "this copy is fenced provenance, keep it
/// for operator recovery, never silently delete".
pub fn quarantine_src(
    tenant_dir: &Path,
    src_worker: &str,
    class_key: &str,
) -> Result<(), std::io::Error> {
    let src_dir = tenant_dir.join(src_worker).join(class_key);
    let src_worker_dir = tenant_dir.join(src_worker);
    let qname = format!("{class_key}{QUARANTINE_SUFFIX}");
    let quarantine = src_worker_dir.join(&qname);
    let _ = std::fs::remove_dir_all(&quarantine);
    if src_dir.is_dir() {
        std::fs::rename(&src_dir, &quarantine)?;
    }
    for sib in sibling_paths(&src_worker_dir, class_key) {
        if sib.exists() {
            let _ = std::fs::rename(
                &sib,
                src_worker_dir.join(format!(
                    "{qname}-sib-{}",
                    sib.file_name().and_then(|n| n.to_str()).unwrap_or("x")
                )),
            );
        }
    }
    mark_quarantine(&src_worker_dir, &qname);
    Ok(())
}
