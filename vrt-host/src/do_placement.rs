//! Durable Objects placement lease — the P3c-2 seam.
//!
//! A placement lease decides WHICH generation may write a class's
//! storage dir. P3c-1 runs a single host: the supervisor's serialized
//! generation switch (`start_pending_launch`'s DO gate) is the real
//! mutual exclusion, and this module provides the strongly-typed
//! interface plus a durable, monotonically-increasing local epoch so
//! P3c-2's cross-cell fencing has state to inherit.
//!
//! K8s path (P3c-2, TODO): replace [`LocalPlacement`] with an impl
//! backed by a shared lease store (DB CAS row keyed by
//! `(tenant, worker, class_key)`); `acquire` returns the winning
//! epoch, `fence` rejects writers at `<=` a fenced epoch so a stale
//! cell that still believes it holds the lease fails closed. Object
//! snapshots / freeze for migration build on the same epoch — the
//! file naming here (`<uniqueKey>.epoch` beside the class dir) is
//! deliberately independent of class name so `renamed_classes` keeps
//! the lease with the data.

use std::path::{Path, PathBuf};

/// What a lease is taken on — a durable-object class's storage dir.
///
/// `class_key` is the capnp `uniqueKey` (server-derived, stable across
/// renames) — never a user-supplied class name.
#[derive(Debug, Clone)]
pub struct PlacementScope {
    /// Owning tenant segment.
    pub tenant: String,
    /// Worker id owning the class.
    pub worker: String,
    /// Class identity — the namespace `uniqueKey`.
    pub class_key: String,
    /// Class storage dir (`<tenantDir>/<worker>/<uniqueKey>`) —
    /// file-backed impls store the epoch next to it; networked impls
    /// may ignore it.
    pub class_dir: PathBuf,
}

/// A fencing token — strictly increasing per (worker, class).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PlacementEpoch(pub u64);

/// Lease acquisition failure — fail closed (caller must not open the
/// class dir for writes) unless the impl documents advisory semantics.
#[derive(Debug)]
pub enum PlacementError {
    /// Local store unavailable (I/O).
    Store(std::io::Error),
    /// A newer epoch already exists — this caller is fenced off.
    /// Deterministic terminal state for THIS epoch-holder pair:
    /// unlike `Store` (transient unreachability), there is nothing to
    /// retry — the supervisor must stop the writer immediately.
    /// `until_ms` is the holding lease's wall-clock expiry (unix ms)
    /// when the store record carried it — a remaining-TTL estimate
    /// for throttled wait logs; 0 means unknown.
    Fenced { current: u64, until_ms: u64 },
    /// P3c-2-r1: the record is a migration-reserved dead-marker and
    /// this caller is not the named taker. Distinct from `Fenced`:
    /// the class WILL be acquirable after `until_ms` (or by explicit
    /// reservation release) — callers keep retrying on backoff but
    /// must not serve meanwhile.
    Reserved { until_ms: u64 },
    /// Caller-supplied coordinates violate the protocol (defensive —
    /// never a retryable condition).
    Precondition(String),
}

impl std::fmt::Display for PlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => write!(f, "placement store: {e}"),
            Self::Fenced { current, until_ms } => {
                if *until_ms > 0 {
                    write!(
                        f,
                        "fenced: a newer epoch ({current}) holds the class until {until_ms}"
                    )
                } else {
                    write!(f, "fenced: a newer epoch ({current}) holds the class")
                }
            }
            Self::Reserved { until_ms } => {
                write!(f, "reserved for migration until {until_ms}")
            }
            Self::Precondition(s) => write!(f, "precondition: {s}"),
        }
    }
}

impl std::error::Error for PlacementError {}

/// Placement lease operations. All methods are scoped to ONE class.
/// A granted lease: fencing epoch + wall-clock expiry (unix ms).
/// `expires_at_ms == u64::MAX/2` marks a non-expiring lease
/// (LocalPlacement) — the renewal loop treats it as never-due.
#[derive(Debug, Clone)]
pub struct PlacementLease {
    pub epoch: PlacementEpoch,
    pub expires_at_ms: u64,
}

pub trait DoPlacement: Send + Sync {
    /// Whether an acquire/renew failure MUST block writes (shared
    /// store backends) or is advisory (local-only standalone). When
    /// authoritative, the supervisor refuses to launch and stops the
    /// writer before the lease lapses — never degraded writes.
    fn authoritative(&self) -> bool {
        false
    }

    /// Wall-clock lead time (ms) before expiry at which the renewal
    /// loop starts trying `renew` (retries each monitor tick).
    fn renew_margin_ms(&self) -> u64 {
        u64::MAX
    }

    /// Wall-clock lead time (ms) before expiry at which a still-failing
    /// renewal forces the writer down — the margin is the bounded
    /// shutdown budget so the process is dead BEFORE expiry.
    fn kill_margin_ms(&self) -> u64 {
        u64::MAX
    }

    /// Acquire (or reaffirm) the write lease for `scope`, returning
    /// the current fencing epoch — strictly increasing per class.
    fn acquire(&self, scope: &PlacementScope) -> Result<PlacementLease, PlacementError>;

    /// MIG-17: re-validate a lease this holder CARRIED across a
    /// same-holder serialized generation switch — the store record
    /// must still name this holder (at any live epoch; `expected` is
    /// the epoch the carry stashed, impls may verify it). The expiry
    /// is refreshed when it already fell inside `renew_margin_ms`, so
    /// the common path costs ONE read and no write instead of the
    /// release + re-acquire pair a fresh `acquire` would CAS.
    /// Anything else (foreign holder, lapsed record, missing object)
    /// takes the exact `acquire` path — the caller never has to
    /// distinguish a carried lease from a first acquisition.
    /// Default delegates to `acquire`: advisory impls keep identical
    /// semantics (LocalPlacement bumps its durable epoch as before).
    fn reaffirm(
        &self,
        scope: &PlacementScope,
        expected: PlacementEpoch,
    ) -> Result<PlacementLease, PlacementError> {
        let _ = expected;
        self.acquire(scope)
    }

    /// P3c-2: renew the lease held at `epoch`, returning the new
    /// expiry (unix ms). Impls without expiring leases return a
    /// far-future expiry so callers share one code path.
    fn renew(
        &self,
        _scope: &PlacementScope,
        _epoch: PlacementEpoch,
    ) -> Result<u64, PlacementError> {
        Ok(u64::MAX / 2)
    }

    /// Fence: declare every epoch `<= epoch` stale for `scope`.
    ///
    /// Single-host impl: the serialized generation switch is the
    /// fence, so this only validates the caller is not already behind.
    /// K8s impl (P3c-2): CAS the shared lease row so other cells'
    /// stale writers are rejected.
    fn fence(&self, scope: &PlacementScope, epoch: PlacementEpoch) -> Result<(), PlacementError>;

    /// DO1-r4: stamp `writer_epoch = epoch` on the record — proof
    /// this epoch actually launched a writer, consumed by the
    /// transfer-restore gate (`Final` must be stamped by the last
    /// real writer, not by an acquire-only epoch). Default no-op:
    /// non-authoritative placements keep no durable record, so the
    /// gate's legacy fallback applies.
    fn claim_writer(
        &self,
        _scope: &PlacementScope,
        _epoch: PlacementEpoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

/// `<uniqueKey>.epoch` — `LocalPlacement`'s durable epoch file.
pub const PLACEMENT_EPOCH_SUFFIX: &str = ".epoch";
/// `<uniqueKey>.lease-epoch` — the local-dir marker
/// `do_snapshot::sibling_epoch_file` records the acquiring epoch in.
pub const LEASE_EPOCH_SUFFIX: &str = ".lease-epoch";
/// `<uniqueKey>.wal-replay-pending` — the MIG-17 restore+WAL-replay
/// pending marker (`do_snapshot::sibling_replay_pending_file`).
pub const REPLAY_PENDING_SUFFIX: &str = ".wal-replay-pending";
/// Every sibling-file suffix that belongs to a class's lease/epoch
/// bookkeeping. Longest-first so `.lease-epoch` never degrades to
/// `<key>.lease`. The sweep maps an entry name back to its class key
/// through [`lease_entry_class_key`] — the ONLY place the
/// file-name → class-key contract lives; the writers above build
/// their names from these same constants so naming cannot desync.
/// MIG-17c: `REPLAY_PENDING_SUFFIX` joined so a marker left behind
/// sweeps (or is retained) WITH its class instead of orphaning as a
/// foreign entry.
pub const LEASE_FILE_SUFFIXES: &[&str] = &[
    REPLAY_PENDING_SUFFIX,
    LEASE_EPOCH_SUFFIX,
    PLACEMENT_EPOCH_SUFFIX,
];

/// The class key an entry name belongs to after removing any
/// lease-file suffix. A bare `<uniqueKey>` maps to itself.
pub fn lease_entry_class_key(name: &str) -> &str {
    LEASE_FILE_SUFFIXES
        .iter()
        .find_map(|s| name.strip_suffix(s))
        .unwrap_or(name)
}

/// Single-host lease: a durable `<uniqueKey>.epoch` file beside the
/// class dir. The file survives workerd restarts and generation
/// switches (it lives in the stable storage root, not gen dirs).
/// Mutual exclusion across generations is the supervisor's
/// serialized-switch gate — this impl only maintains the durable
/// epoch counter P3c-2 fencing will consume.
#[derive(Debug, Default)]
pub struct LocalPlacement;

impl LocalPlacement {
    fn epoch_path(scope: &PlacementScope) -> PathBuf {
        scope
            .class_dir
            .parent()
            .unwrap_or(&scope.class_dir)
            .join(format!("{}{PLACEMENT_EPOCH_SUFFIX}", scope.class_key))
    }

    fn read_epoch(path: &Path) -> Result<u64, PlacementError> {
        match std::fs::read_to_string(path) {
            Ok(s) => s.trim().parse::<u64>().map_err(|_| {
                PlacementError::Store(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "corrupt epoch file",
                ))
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(PlacementError::Store(e)),
        }
    }
}

impl DoPlacement for LocalPlacement {
    fn acquire(&self, scope: &PlacementScope) -> Result<PlacementLease, PlacementError> {
        let path = Self::epoch_path(scope);
        let cur = Self::read_epoch(&path)?;
        let next = cur + 1;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(PlacementError::Store)?;
        }
        // tmp+rename so a torn write never yields a corrupt epoch.
        let tmp = path.with_extension("epoch.tmp");
        std::fs::write(&tmp, next.to_string()).map_err(PlacementError::Store)?;
        std::fs::rename(&tmp, &path).map_err(PlacementError::Store)?;
        Ok(PlacementLease {
            epoch: PlacementEpoch(next),
            expires_at_ms: u64::MAX / 2,
        })
    }

    fn fence(&self, scope: &PlacementScope, epoch: PlacementEpoch) -> Result<(), PlacementError> {
        let path = Self::epoch_path(scope);
        let cur = Self::read_epoch(&path)?;
        if cur > epoch.0 {
            return Err(PlacementError::Fenced {
                current: cur,
                until_ms: 0,
            });
        }
        // Single host: the serialized switch already fenced older
        // writers — nothing else to persist.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(dir: &Path) -> PlacementScope {
        PlacementScope {
            tenant: "t1".into(),
            worker: "w1".into(),
            class_key: "a".repeat(64),
            class_dir: dir.join("w1").join("a".repeat(64)),
        }
    }

    #[test]
    fn lease_entry_class_key_maps_every_lease_file_to_its_class() {
        let k = "a".repeat(64);
        // Bare class dir → itself.
        assert_eq!(lease_entry_class_key(&k), k);
        // Both lease sibling files → the SAME class key.
        assert_eq!(lease_entry_class_key(&format!("{k}.epoch")), k);
        assert_eq!(lease_entry_class_key(&format!("{k}.lease-epoch")), k);
        // MIG-17c: the restore+replay pending marker is class
        // bookkeeping too — it maps back to its class so the sweep
        // retains/removes it WITH the class.
        assert_eq!(
            lease_entry_class_key(&format!("{k}.wal-replay-pending")),
            k
        );
        // `.lease-epoch` must never degrade to `<k>.lease` — longest-
        // suffix-first ordering is load-bearing here.
        assert_eq!(
            lease_entry_class_key(&format!("{k}.lease-epoch")),
            k,
            "must not strip only .epoch leaving <k>.lease"
        );
        // Atomic-write temp + unrelated entries → themselves (swept).
        assert_eq!(
            lease_entry_class_key(&format!("{k}.lease-epoch.tmp")),
            format!("{k}.lease-epoch.tmp")
        );
        assert_eq!(lease_entry_class_key("unrelated"), "unrelated");
        // The writers build their names from the same constants.
        let scope = PlacementScope {
            tenant: "t".into(),
            worker: "w".into(),
            class_key: k.clone(),
            class_dir: Path::new("/x").join(&k),
        };
        assert_eq!(
            LocalPlacement::epoch_path(&scope),
            Path::new("/x").join(format!("{k}{PLACEMENT_EPOCH_SUFFIX}"))
        );
    }

    #[test]
    fn local_epoch_is_monotone_and_durable() {
        let dir = std::env::temp_dir().join(format!("vrt-epoch-{}", std::process::id()));
        let s = scope(&dir);
        let p = LocalPlacement;
        let e1 = p.acquire(&s).unwrap().epoch;
        let e2 = p.acquire(&s).unwrap().epoch;
        assert!(e2.0 > e1.0);
        // A fresh impl instance reads the persisted epoch.
        let e3 = LocalPlacement.acquire(&s).unwrap().epoch;
        assert!(e3.0 > e2.0);
        // fencing: a staler epoch than current is refused.
        assert!(matches!(
            p.fence(&s, e1),
            Err(PlacementError::Fenced { .. })
        ));
        assert!(p.fence(&s, e3).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
