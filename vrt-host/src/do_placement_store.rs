//! P3c-2: object-store-CAS-backed DO placement lease (§4.1 / §6.4).
//!
//! Reuses the T36 `ObjectStore` conditional-write primitives
//! (`put_if_none_match`/`put_if_match`, vrt-objstore/src/lib.rs:88-109)
//! — the same CAS surface the D1 `Persister` manifests on, not a
//! second ad-hoc mechanism. Lease object key:
//! `do/{uniqueKey}/placement` holding
//! `{v, epoch, holder, expires_at_ms, snapshot}`.
//!
//! Semantics:
//! - `acquire`: no object → `put_if_none_match` {epoch:1}; held and
//!   `expires-margin > now` → `HeldByOther`; expired → CAS epoch+1.
//! - `renew`: CAS refresh `expires_at_ms` under the SAME epoch — a
//!   stale etag or epoch mismatch → `Fenced` (caller kills workerd
//!   BEFORE the lease lapses: no lease, no writes).
//! - `fence` (migration source): CAS `{epoch: epoch+1, holder:"",
//!   expires:0}` — every writer at `<= epoch` fails its next CAS;
//!   the next acquirer takes `epoch+1` with a fresh lease.
//! - `commit_snapshot`: CAS the manifest's `snapshot` ref ONLY while
//!   `manifest.epoch == caller_epoch` — uploads committed under a
//!   fenced epoch can never be referenced (orphan objects GC later).

use crate::do_placement::{
    DoPlacement, PlacementEpoch, PlacementError, PlacementLease, PlacementScope,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use vrt_objstore::{ObjectStore, StoreError};

/// Object key for the placement record of a class.
pub fn placement_key(class_key: &str) -> String {
    format!("do/{class_key}/placement")
}
/// Prefix for snapshot objects of one epoch.
pub fn snap_prefix(class_key: &str, epoch: u64) -> String {
    format!("do/{class_key}/{epoch}/snap")
}
/// Manifest object for snapshot `seq`.
pub fn snap_manifest_key(class_key: &str, epoch: u64, seq: u64) -> String {
    format!("{}-{seq}.json", snap_prefix(class_key, epoch))
}
/// Data-object key for one snapshot file.
pub fn snap_file_key(class_key: &str, epoch: u64, seq: u64, rel: &str) -> String {
    format!("{}-{seq}/files/{rel}", snap_prefix(class_key, epoch))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementRecord {
    pub v: u32,
    pub epoch: u64,
    pub holder: String,
    pub expires_at_ms: u64,
    /// Latest committed snapshot ref (restore baseline).
    #[serde(default)]
    pub snapshot: Option<SnapRef>,
    /// Migration reservation (P3c-2-r1): set only on dead-marker
    /// records by `fence_for_migration`. While `reserve_until_ms >
    /// now`, ONLY `reserved_for` may take `epoch+1` — a fenced
    /// source's respawn is `Reserved`-rejected instead of stealing
    /// the class back mid-migration.
    #[serde(default)]
    pub reserved_for: Option<String>,
    #[serde(default)]
    pub reserve_until_ms: u64,
    /// DO1-r3 admin escape hatch (P0-2, hardened from r2's bare
    /// bool): the operator accepted EXACTLY ONE snapshot — pinned by
    /// `{epoch, seq}` — as the handoff baseline for a transferred
    /// class whose `Final` never committed. Set ONLY through the
    /// trusted admin channel (`/v1/do/accept-baseline`); the target
    /// cell's fail-closed gate accepts a snapshot iff it matches the
    /// pinned identity exactly. Carried across acquire/fence (the
    /// acceptance is a statement about the DATA, not a lease
    /// generation) but CLEARED by every snapshot commit — a newer
    /// baseline always requires a fresh decision.
    /// Pre-r3 records carrying `baseline_accepted: true` decode with
    /// this field ABSENT (unknown-key ignored) — i.e. fail closed:
    /// a blanket r2 acceptance can never widen to a future snapshot.
    #[serde(default)]
    pub accepted_baseline: Option<AcceptedBaseline>,
    /// DO1-r4 P0: epoch of the last holder that actually LAUNCHED a
    /// writer for this class — claimed by CAS immediately before
    /// workerd opens the class dir (`claim_writer`), so an epoch that
    /// was merely acquired (launch failed / host restart) does NOT
    /// advance it. The transfer-restore gate requires
    /// `snap.epoch == writer_epoch`: the Final must come from the
    /// last real writer, and a transient acquire-fail-release cycle
    /// can never strand the handoff (the retry's new epoch does not
    /// move the required snapshot epoch). Carried across acquire/
    /// fence like `snapshot` — it describes the DATA's lineage, not
    /// the lease. `#[serde(default)]` keeps pre-r4 records readable
    /// as 0 → the gate falls back to the r3 `epoch+1` rule for those
    /// (bounded to the rolling-upgrade window; see §6.10).
    #[serde(default)]
    pub writer_epoch: u64,
}

/// DO1-r3: identity of the ONE snapshot an operator accepted as the
/// transfer baseline, plus the accept time for diagnostics/audit.
/// `epoch`/`seq` pin the snapshot exactly; `at_ms` is when the
/// `/v1/do/accept-baseline` call landed (persisted audit — P1-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedBaseline {
    pub epoch: u64,
    pub seq: u64,
    pub at_ms: u64,
}

/// DO1-r2: provenance class of a committed snapshot. `Periodic`
/// baselines are taken while a live writer still owns the class and
/// can be arbitrarily stale; `Final` is committed by the source
/// holder AFTER its writer provably exited (drain/fence-time) and is
/// the ONLY baseline a `transferred_classes` target may serve from.
/// `#[serde(default)]` keeps pre-r2 records readable — they decode
/// as `Periodic`, which is the honest (conservative) answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapKind {
    #[default]
    Periodic,
    Final,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapRef {
    /// Manifest object key (`do/{key}/{epoch}/snap-{seq}.json`).
    pub manifest_key: String,
    pub epoch: u64,
    pub seq: u64,
    pub files: u32,
    pub bytes: u64,
    pub at_ms: u64,
    #[serde(default)]
    pub kind: SnapKind,
}

pub struct ManifestDoPlacement {
    store: Arc<dyn ObjectStore>,
    /// Unique holder id (cell/pod id) — never trusted from callers.
    holder: String,
    /// Lease duration.
    pub lease_ms: u64,
    /// A lease counts as held only while `expires-margin > now`.
    pub margin_ms: u64,
    /// Clock — injectable for tests.
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// Bounded CAS retries on precondition races.
    retries: u32,
}

impl std::fmt::Debug for ManifestDoPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestDoPlacement")
            .field("holder", &self.holder)
            .field("lease_ms", &self.lease_ms)
            .field("margin_ms", &self.margin_ms)
            .finish_non_exhaustive()
    }
}

impl ManifestDoPlacement {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        holder: impl Into<String>,
        lease_ms: u64,
        margin_ms: u64,
    ) -> Self {
        Self {
            store,
            holder: holder.into(),
            lease_ms,
            margin_ms,
            now_ms: Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0)
            }),
            retries: 4,
        }
    }

    /// This host's placement identity — the value recorded as
    /// `holder` on every acquire (also the default `reserved_for`
    /// basis for the standalone self-reserved export path).
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Lease parameters (ms) — exposed so callers derive
    /// migration-time bounds from the SAME numbers the fencing
    /// protocol uses, never a parallel constant.
    pub fn lease_params(&self) -> (u64, u64) {
        (self.lease_ms, self.margin_ms)
    }

    /// P3c-2-r1: cross-cell stop-write upper bound for a fenced
    /// writer — worst case the fenced holder JUST renewed, so its
    /// next renew lands `lease_ms - margin` out, returns `Fenced`,
    /// and the writer dies within the kill margin: bound ≈
    /// `lease_ms + margin`; we use `lease_ms + 2*margin` to also
    /// absorb takeover-side clock skew and kill latency.
    pub fn stop_write_bound_ms(&self) -> u64 {
        self.lease_ms.saturating_add(2 * self.margin_ms)
    }

    /// Injectable wall clock (ms) — the skew/takeover-boundary tests
    /// drive two placement instances on deliberately different
    /// clocks. Not test-gated: integration tests (tests/) need the
    /// same seam, and production code never calls it.
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.now_ms = now_ms;
        self
    }

    fn get(
        &self,
        scope: &PlacementScope,
    ) -> Result<Option<(PlacementRecord, String)>, PlacementError> {
        let key = placement_key(&scope.class_key);
        match vrt_objstore::block_on_store(self.store.get(&key)) {
            Ok(Some(obj)) => {
                let rec: PlacementRecord = serde_json::from_slice(&obj.body).map_err(|e| {
                    PlacementError::Store(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("placement record: {e}"),
                    ))
                })?;
                Ok(Some((rec, obj.meta.etag)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PlacementError::Store(io(e))),
        }
    }

    /// Whether the record still counts as held. The holder kills its
    /// writer at `expires - margin` (kill margin, bounded stop); the
    /// acquirer must NOT treat the lease as free until `expires +
    /// margin` — the 2×margin gap is what absorbs clock skew (±margin)
    /// plus the bounded kill delay. Both sides of the handoff
    /// therefore reference DIFFERENT instants:
    /// `kill_due + kill_delay < expires < takeover_earliest - skew`.
    fn usable(&self, rec: &PlacementRecord) -> bool {
        rec.expires_at_ms.saturating_add(self.margin_ms) > (self.now_ms)() && !rec.holder.is_empty()
    }

    /// Defensive SnapRef checks (P2): the ref must claim the SAME
    /// epoch the commit targets and live under that epoch's snapshot
    /// keyspace (`do/{key}/{epoch}/snap-`) — a mismatched ref is a
    /// caller bug, never committed.
    fn check_snap_ref(
        &self,
        scope: &PlacementScope,
        epoch: u64,
        snap: &SnapRef,
    ) -> Result<(), PlacementError> {
        if snap.epoch != epoch {
            return Err(PlacementError::Precondition(format!(
                "snap.epoch {} != commit epoch {epoch}",
                snap.epoch
            )));
        }
        let prefix = snap_prefix(&scope.class_key, epoch);
        if !snap.manifest_key.starts_with(&format!("{prefix}-")) {
            return Err(PlacementError::Precondition(format!(
                "manifest key {} outside {prefix}-*",
                snap.manifest_key
            )));
        }
        Ok(())
    }

    /// The snapshot ref a writer may commit under `epoch` — checked
    /// CAS-side: returns the record+etag if the caller still owns the
    /// epoch, else `Fenced`.
    pub fn commit_snapshot(
        &self,
        scope: &PlacementScope,
        epoch: PlacementEpoch,
        snap: SnapRef,
    ) -> Result<(), PlacementError> {
        self.check_snap_ref(scope, epoch.0, &snap)?;
        for _ in 0..self.retries {
            let Some((mut rec, etag)) = self.get(scope)? else {
                return Err(PlacementError::Fenced {
                current: 0,
                until_ms: 0,
            });
            };
            if rec.epoch != epoch.0 || rec.holder != self.holder {
                return Err(PlacementError::Fenced {
                    current: rec.epoch,
                    until_ms: 0,
                });
            }
            // DO1-r3: a NEW baseline invalidates any prior admin
            // acceptance — the pinned {epoch,seq} no longer refers
            // to the record's snapshot, and an explicit field clear
            // documents that stale acceptances cannot ride forward.
            rec.accepted_baseline = None;
            rec.snapshot = Some(snap.clone());
            let body = serde_json::to_vec(&rec).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(()),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Fenced {
                current: epoch.0,
                until_ms: 0,
            })
    }

    /// Read the committed snapshot ref for restore (any holder may
    /// read; only the current epoch's writes are referenced).
    pub fn snapshot_ref(&self, scope: &PlacementScope) -> Result<Option<SnapRef>, PlacementError> {
        Ok(self.get(scope)?.and_then(|(r, _)| r.snapshot))
    }

    /// Raw record by class key — lifecycle cleanup uses it to
    /// refuse deleting objects another cell still owns.
    pub fn record_for_class(
        &self,
        class_key: &str,
    ) -> Result<Option<PlacementRecord>, PlacementError> {
        let key = placement_key(class_key);
        match vrt_objstore::block_on_store(self.store.get(&key)) {
            Ok(Some(obj)) => {
                let rec: PlacementRecord = serde_json::from_slice(&obj.body).map_err(|e| {
                    PlacementError::Store(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("placement record: {e}"),
                    ))
                })?;
                Ok(Some(rec))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(PlacementError::Store(io(e))),
        }
    }

    /// Read the raw placement record (migration coordinator —
    /// epoch holder/epoch inspection only).
    pub fn record(
        &self,
        scope: &PlacementScope,
    ) -> Result<Option<PlacementRecord>, PlacementError> {
        Ok(self.get(scope)?.map(|(r, _)| r))
    }

    /// Migration-commit: attach `snap` to a FENCED (dead-marker)
    /// record. The fenced epoch keeps holder="" and expires=0 — the
    /// record stays dead, only the restore baseline is added. CAS
    /// requires `manifest.epoch == at_epoch && holder.is_empty()` so
    /// a coordinator can never graft onto a live epoch.
    pub fn commit_snapshot_fenced(
        &self,
        scope: &PlacementScope,
        at_epoch: u64,
        snap: SnapRef,
    ) -> Result<(), PlacementError> {
        self.check_snap_ref(scope, at_epoch, &snap)?;
        for _ in 0..self.retries {
            let Some((mut rec, etag)) = self.get(scope)? else {
                return Err(PlacementError::Fenced {
                current: 0,
                until_ms: 0,
            });
            };
            if rec.epoch != at_epoch || !rec.holder.is_empty() {
                return Err(PlacementError::Fenced {
                    current: rec.epoch,
                    until_ms: 0,
                });
            }
            rec.accepted_baseline = None;
            rec.snapshot = Some(snap.clone());
            let body = serde_json::to_vec(&rec).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(()),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Fenced {
            current: at_epoch,
            until_ms: 0,
        })
    }

    /// DO1-r3: operator-accepted stale baseline for a transferred
    /// class whose `Final` snapshot never committed (lost source
    /// cell, exhausted retry). CAS pins `{epoch, seq, at_ms}` of the
    /// CURRENT committed snapshot — the acceptance is bound to that
    /// exact object, and every later snapshot commit clears it, so
    /// one acceptance can never silently bless a newer baseline
    /// (r2's sticky bool could). Idempotent on exact identity.
    /// Returns the snapshot ref that was accepted so the caller can
    /// audit exactly which baseline is now trusted (RPO = its seq).
    /// Refuses when no snapshot exists at all — accepting nothing
    /// would open an EMPTY store, precisely what the gate forbids.
    pub fn accept_baseline(&self, scope: &PlacementScope) -> Result<SnapRef, PlacementError> {
        for _ in 0..self.retries {
            let Some((mut rec, etag)) = self.get(scope)? else {
                return Err(PlacementError::Precondition("no placement record".into()));
            };
            let Some(snap) = rec.snapshot.clone() else {
                return Err(PlacementError::Precondition(
                    "no committed snapshot to accept".into(),
                ));
            };
            let pin = AcceptedBaseline {
                epoch: snap.epoch,
                seq: snap.seq,
                at_ms: (self.now_ms)(),
            };
            if rec.accepted_baseline == Some(pin) {
                return Ok(snap); // idempotent — same snapshot already accepted
            }
            rec.accepted_baseline = Some(pin);
            let body = serde_json::to_vec(&rec).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(snap),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Store(std::io::Error::new(
            std::io::ErrorKind::Other,
            "accept_baseline CAS retries exhausted",
        )))
    }

    /// DO1-r4: claim `epoch` as a REAL writer epoch. CAS requires
    /// the record still be the live lease we hold (`epoch` match +
    /// `holder == self.holder`) — same fencing precondition as
    /// `renew`/`commit_snapshot`, so a claim can never stamp a dead
    /// or foreign epoch. Called by the supervisor AFTER the restore
    /// pass, immediately before workerd opens the class dir: a
    /// failure is fail-closed (no launch), and an epoch that only
    /// acquired never advances `writer_epoch`. Idempotent — a
    /// reaffirmed same-holder launch re-stamps the same value.
    pub fn claim_writer(
        &self,
        scope: &PlacementScope,
        epoch: PlacementEpoch,
    ) -> Result<(), PlacementError> {
        for _ in 0..self.retries {
            let Some((mut rec, etag)) = self.get(scope)? else {
                return Err(PlacementError::Fenced {
                current: 0,
                until_ms: 0,
            });
            };
            if rec.epoch != epoch.0 || rec.holder != self.holder {
                return Err(PlacementError::Fenced {
                    current: rec.epoch,
                    until_ms: 0,
                });
            }
            if rec.writer_epoch >= epoch.0 {
                // `==` — already claimed (reaffirm/retry), idempotent
                // Ok. `>` — a field we never downgrade: a stamped
                // epoch beyond ours is anomalous state; refuse the
                // launch rather than mask it (fail closed).
                return if rec.writer_epoch == epoch.0 {
                    Ok(())
                } else {
                    Err(PlacementError::Fenced {
                    current: rec.epoch,
                    until_ms: 0,
                })
                };
            }
            rec.writer_epoch = epoch.0;
            let body = serde_json::to_vec(&rec).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(()),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Fenced {
                current: epoch.0,
                until_ms: 0,
            })
    }

    /// P3c-2-r1: fence + migration reservation in ONE CAS — the
    /// dead-marker records `reserved_for`/`reserve_until_ms` so only
    /// the named target (or `release_reservation`) may take epoch+1.
    /// Without it, a fenced source's respawn would win the next
    /// acquire and either break `commit_snapshot_fenced` or split the
    /// class across cells.
    pub fn fence_for_migration(
        &self,
        scope: &PlacementScope,
        epoch: PlacementEpoch,
        reserved_for: &str,
        reserve_ms: u64,
    ) -> Result<(), PlacementError> {
        let now = (self.now_ms)();
        for _ in 0..self.retries {
            let Some((rec, etag)) = self.get(scope)? else {
                return Ok(());
            };
            // Expectation CAS: the caller pre-conditioned on THIS
            // epoch — a moved-on record (live E+1 or a re-driven
            // dead-marker at another epoch) is a precondition
            // failure, never a silent no-op: fencing a live foreign
            // epoch would kill an innocent writer.
            if rec.epoch != epoch.0 {
                return Err(PlacementError::Precondition(format!(
                    "record at epoch {} != fence target {}",
                    rec.epoch, epoch.0
                )));
            }
            let next = PlacementRecord {
                v: 1,
                epoch: epoch.0,
                holder: String::new(),
                expires_at_ms: 0,
                snapshot: rec.snapshot.clone(),
                reserved_for: Some(reserved_for.to_string()),
                reserve_until_ms: now + reserve_ms,
                accepted_baseline: rec.accepted_baseline,
                writer_epoch: rec.writer_epoch,
            };
            let body = serde_json::to_vec(&next).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(()),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Fenced {
                current: epoch.0,
                until_ms: 0,
            })
    }

    /// Release a migration reservation (rollback path): clears
    /// `reserved_for`/`reserve_until_ms` on the dead-marker at
    /// `at_epoch`, so the NEXT acquire takes epoch+1 freely.
    /// Idempotent — a record already past `at_epoch` is a no-op.
    pub fn release_reservation(
        &self,
        scope: &PlacementScope,
        at_epoch: u64,
    ) -> Result<(), PlacementError> {
        for _ in 0..self.retries {
            let Some((mut rec, etag)) = self.get(scope)? else {
                return Ok(());
            };
            if rec.epoch != at_epoch || !rec.holder.is_empty() || rec.reserved_for.is_none() {
                return Ok(()); // moved on / never reserved — nothing to do
            }
            rec.reserved_for = None;
            rec.reserve_until_ms = 0;
            let body = serde_json::to_vec(&rec).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(()),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Store(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "release_reservation CAS retries exhausted",
        )))
    }

    /// MIG-17: carried-lease re-validation for the serialized
    /// same-holder switch (see `DoPlacement::reaffirm`). When the
    /// record still names this holder at the carried epoch and is
    /// usable, the lease reaffirms for the cost of ONE read — plus a
    /// single CAS renew only when the expiry already reached the
    /// renew window. Every other shape (foreign holder, dead-marker,
    /// lapsed, missing) defers to `acquire`, so fencing / epoch+1 /
    /// reservation semantics are identical to a fresh acquisition.
    pub fn reaffirm(
        &self,
        scope: &PlacementScope,
        expected: PlacementEpoch,
    ) -> Result<PlacementLease, PlacementError> {
        let key = placement_key(&scope.class_key);
        for _ in 0..self.retries {
            // Refresh `now` per CAS retry — a contended loop must not
            // renew with a stale expiry anchor.
            let now = (self.now_ms)();
            match self.get(scope)? {
                Some((mut rec, etag))
                    if rec.holder == self.holder
                        && rec.epoch == expected.0
                        && self.usable(&rec) =>
                {
                    // Not yet inside the renew window — reaffirm with
                    // no write; the renew loop owns the next refresh.
                    if rec
                        .expires_at_ms
                        .saturating_sub(<Self as DoPlacement>::renew_margin_ms(self))
                        > now
                    {
                        return Ok(PlacementLease {
                            epoch: expected,
                            expires_at_ms: rec.expires_at_ms,
                        });
                    }
                    rec.expires_at_ms = now + self.lease_ms;
                    let body = serde_json::to_vec(&rec).map_err(|e| {
                        PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                    match vrt_objstore::block_on_store(self.store.put_if_match(
                        &key, &etag, &body,
                    )) {
                        Ok(_) => {
                            return Ok(PlacementLease {
                                epoch: expected,
                                expires_at_ms: rec.expires_at_ms,
                            })
                        }
                        Err(StoreError::PreconditionFailed) => continue,
                        Err(e) => return Err(PlacementError::Store(io(e))),
                    }
                }
                // Same-holder record at a DIFFERENT live epoch (a
                // previous attempt's acquire landed after the carry
                // stashed) — the record is the truth, adopt its epoch.
                Some((rec, _)) if rec.holder == self.holder && self.usable(&rec) => {
                    return Ok(PlacementLease {
                        epoch: PlacementEpoch(rec.epoch),
                        expires_at_ms: rec.expires_at_ms,
                    });
                }
                // Foreign, dead-marker, lapsed or missing — identical
                // to a first acquisition (fencing/reservation rules
                // unchanged).
                _ => return self.acquire(scope),
            }
        }
        Err(PlacementError::Fenced {
            current: u64::MAX,
            until_ms: 0,
        })
    }
}

fn io(e: StoreError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
}

impl DoPlacement for ManifestDoPlacement {
    fn authoritative(&self) -> bool {
        true
    }

    fn renew_margin_ms(&self) -> u64 {
        // Start renewing at 2×margin before expiry — leaves the kill
        // margin intact for the bounded shutdown if the store stays
        // unreachable.
        self.margin_ms * 2
    }

    fn kill_margin_ms(&self) -> u64 {
        self.margin_ms
    }

    fn acquire(&self, scope: &PlacementScope) -> Result<PlacementLease, PlacementError> {
        let key = placement_key(&scope.class_key);
        let now = (self.now_ms)();
        for _ in 0..self.retries {
            match self.get(scope)? {
                None => {
                    let rec = PlacementRecord {
                        v: 1,
                        epoch: 1,
                        holder: self.holder.clone(),
                        expires_at_ms: now + self.lease_ms,
                        snapshot: None,
                        reserved_for: None,
                        reserve_until_ms: 0,
                        accepted_baseline: None,
                        writer_epoch: 0,
                    };
                    let body = serde_json::to_vec(&rec).map_err(|e| {
                        PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                    match vrt_objstore::block_on_store(self.store.put_if_none_match(&key, &body)) {
                        Ok(_) => {
                            return Ok(PlacementLease {
                                epoch: PlacementEpoch(1),
                                expires_at_ms: rec.expires_at_ms,
                            })
                        }
                        Err(StoreError::PreconditionFailed) => continue,
                        Err(e) => return Err(PlacementError::Store(io(e))),
                    }
                }
                Some((rec, etag)) => {
                    if self.usable(&rec) {
                        if rec.holder == self.holder {
                            // Reaffirm own lease (restart dedupe).
                            return Ok(PlacementLease {
                                epoch: PlacementEpoch(rec.epoch),
                                expires_at_ms: rec.expires_at_ms,
                            });
                        }
                        return Err(PlacementError::Fenced {
                            current: rec.epoch,
                            until_ms: rec.expires_at_ms,
                        });
                    }
                    // Expired or released — take epoch+1. `snapshot`
                    // survives: it is the restore baseline for the NEW
                    // epoch's first snapshot decision.
                    // P3c-2-r1 reservation gate: a dead-marker carrying
                    // an unexpired `reserved_for` admits only the named
                    // taker; every other acquirer (incl. the fenced
                    // source's own respawn) is rejected `Reserved`.
                    // Expired reservations lapse — bounded outage,
                    // never a stuck class.
                    if rec.holder.is_empty()
                        && rec
                            .reserved_for
                            .as_deref()
                            .is_some_and(|t| t != self.holder)
                        && rec.reserve_until_ms > now
                    {
                        return Err(PlacementError::Reserved {
                            until_ms: rec.reserve_until_ms,
                        });
                    }
                    let next = PlacementRecord {
                        v: 1,
                        epoch: rec.epoch + 1,
                        holder: self.holder.clone(),
                        expires_at_ms: now + self.lease_ms,
                        snapshot: rec.snapshot.clone(),
                        reserved_for: None,
                        reserve_until_ms: 0,
                        accepted_baseline: rec.accepted_baseline,
                        writer_epoch: rec.writer_epoch,
                    };
                    let body = serde_json::to_vec(&next).map_err(|e| {
                        PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
                    })?;
                    match vrt_objstore::block_on_store(self.store.put_if_match(&key, &etag, &body))
                    {
                        Ok(_) => {
                            return Ok(PlacementLease {
                                epoch: PlacementEpoch(rec.epoch + 1),
                                expires_at_ms: next.expires_at_ms,
                            })
                        }
                        Err(StoreError::PreconditionFailed) => continue,
                        Err(e) => return Err(PlacementError::Store(io(e))),
                    }
                }
            }
        }
        Err(PlacementError::Fenced {
            current: u64::MAX,
            until_ms: 0,
        })
    }

    fn renew(&self, scope: &PlacementScope, epoch: PlacementEpoch) -> Result<u64, PlacementError> {
        let now = (self.now_ms)();
        for _ in 0..self.retries {
            let Some((mut rec, etag)) = self.get(scope)? else {
                return Err(PlacementError::Fenced {
                current: 0,
                until_ms: 0,
            });
            };
            if rec.epoch != epoch.0 || rec.holder != self.holder {
                return Err(PlacementError::Fenced {
                    current: rec.epoch,
                    until_ms: 0,
                });
            }
            rec.expires_at_ms = now + self.lease_ms;
            let body = serde_json::to_vec(&rec).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(rec.expires_at_ms),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Fenced {
                current: epoch.0,
                until_ms: 0,
            })
    }

    fn fence(&self, scope: &PlacementScope, epoch: PlacementEpoch) -> Result<(), PlacementError> {
        for _ in 0..self.retries {
            let Some((rec, etag)) = self.get(scope)? else {
                return Ok(()); // nothing to fence — never held
            };
            if rec.epoch > epoch.0 {
                return Ok(()); // already fenced by a newer claim
            }
            if rec.epoch == epoch.0 && rec.holder.is_empty() {
                // Already a dead-marker at this epoch — PRESERVE any
                // migration reservation (`reserved_for`): rewriting a
                // plain dead-marker here would strip the reservation
                // and let a fenced source respawn steal epoch+1
                // mid-migration (P3c-2-r1 self-review).
                return Ok(());
            }
            // Dead marker AT the fenced epoch: `holder:""` + expired
            // makes every writer at `<= epoch` fail its next CAS
            // (holder mismatch / epoch check), while the next
            // `acquire` is what mints `epoch+1` — a single increment
            // per takeover, never a double hop.
            let next = PlacementRecord {
                v: 1,
                epoch: epoch.0,
                holder: String::new(),
                expires_at_ms: 0,
                snapshot: rec.snapshot.clone(),
                reserved_for: None,
                reserve_until_ms: 0,
                accepted_baseline: rec.accepted_baseline,
                writer_epoch: rec.writer_epoch,
            };
            let body = serde_json::to_vec(&next).map_err(|e| {
                PlacementError::Store(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?;
            match vrt_objstore::block_on_store(self.store.put_if_match(
                &placement_key(&scope.class_key),
                &etag,
                &body,
            )) {
                Ok(_) => return Ok(()),
                Err(StoreError::PreconditionFailed) => continue,
                Err(e) => return Err(PlacementError::Store(io(e))),
            }
        }
        Err(PlacementError::Fenced {
                current: epoch.0,
                until_ms: 0,
            })
    }

    // DO1-r4: delegate to the inherent CAS — without this override
    // the trait default no-ops and `writer_epoch` would never be
    // stamped on a real (shared-store) placement.
    fn claim_writer(
        &self,
        scope: &PlacementScope,
        epoch: PlacementEpoch,
    ) -> Result<(), PlacementError> {
        ManifestDoPlacement::claim_writer(self, scope, epoch)
    }

    // MIG-17: delegate to the inherent carried-lease fast path —
    // without the override the trait default re-runs `acquire` and a
    // serialized switch would pay the full GET+CAS it just avoided.
    fn reaffirm(
        &self,
        scope: &PlacementScope,
        expected: PlacementEpoch,
    ) -> Result<PlacementLease, PlacementError> {
        ManifestDoPlacement::reaffirm(self, scope, expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scope() -> PlacementScope {
        PlacementScope {
            tenant: "t".into(),
            worker: "w".into(),
            class_key: "a".repeat(64),
            class_dir: PathBuf::from("/x"),
        }
    }
    use std::path::PathBuf;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(vrt_objstore::MemoryStore::new())
    }

    #[test]
    fn two_instances_single_winner() {
        let s = store();
        let a = ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000);
        let b = ManifestDoPlacement::new(s, "cell-b", 10_000, 1_000);
        let sc = scope();
        assert_eq!(a.acquire(&sc).unwrap().epoch.0, 1);
        assert!(matches!(b.acquire(&sc), Err(PlacementError::Fenced { .. })));
    }

    #[test]
    fn expiry_lets_next_holder_take_epoch_plus_one() {
        let s = store();
        let now = Arc::new(AtomicU64::new(1_000));
        let clock = {
            let n = now.clone();
            Arc::new(move || n.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let a =
            ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000).with_clock(clock.clone());
        let b = ManifestDoPlacement::new(s, "cell-b", 10_000, 1_000).with_clock(clock);
        let sc = scope();
        assert_eq!(a.acquire(&sc).unwrap().epoch.0, 1);
        now.store(1_000 + 8_999, Ordering::SeqCst); // inside usable window → still held
        assert!(b.acquire(&sc).is_err());
        // P0-1: the takeover barrier is expires + ACQUIRER margin —
        // expires+1 is still inside the safety window (holder's kill
        // deadline is expires - ITS margin; the 2*margin gap absorbs
        // skew + kill latency).
        now.store(1_000 + 10_001, Ordering::SeqCst);
        assert!(b.acquire(&sc).is_err(), "expires+1 must still be held");
        now.store(1_000 + 12_001, Ordering::SeqCst); // past expires+margin → take epoch 2
        assert_eq!(b.acquire(&sc).unwrap().epoch.0, 2);
        // Old holder's renew now fenced.
        assert!(matches!(
            a.renew(&sc, PlacementEpoch(1)),
            Err(PlacementError::Fenced { .. })
        ));
    }

    #[test]
    fn renew_only_by_holder_and_commit_epoch_checked() {
        let s = store();
        let now = Arc::new(AtomicU64::new(0));
        let clock = {
            let n = now.clone();
            Arc::new(move || n.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let a =
            ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000).with_clock(clock.clone());
        let b = ManifestDoPlacement::new(s, "cell-b", 10_000, 1_000).with_clock(clock);
        let sc = scope();
        a.acquire(&sc).unwrap();
        // Non-holder can't renew someone else's epoch.
        assert!(b.renew(&sc, PlacementEpoch(1)).is_err());
        now.store(5_000, Ordering::SeqCst);
        assert_eq!(a.renew(&sc, PlacementEpoch(1)).unwrap(), 15_000);
        // Snapshot commit only under owned epoch — well-formed ref
        // (manifest_key must live under `do/{key}/{epoch}/snap-*`).
        a.commit_snapshot(
            &sc,
            PlacementEpoch(1),
            SnapRef {
                manifest_key: snap_manifest_key(&sc.class_key, 1, 7),
                epoch: 1,
                seq: 1,
                files: 2,
                bytes: 10,
                at_ms: 5_000,
                kind: SnapKind::Periodic,
            },
        )
        .unwrap();
        assert_eq!(a.snapshot_ref(&sc).unwrap().unwrap().seq, 1);
        // fence → old commit rejected, next acquire = epoch 2.
        a.fence(&sc, PlacementEpoch(1)).unwrap();
        assert!(matches!(
            a.commit_snapshot(
                &sc,
                PlacementEpoch(1),
                SnapRef {
                    manifest_key: snap_manifest_key(&sc.class_key, 1, 9),
                    epoch: 1,
                    seq: 2,
                    files: 0,
                    bytes: 0,
                    at_ms: 0,
                    kind: SnapKind::Periodic,
                }
            ),
            Err(PlacementError::Fenced { .. })
        ));
        assert_eq!(b.acquire(&sc).unwrap().epoch.0, 2);
        // Snapshot ref carried across the fence — restore baseline.
        assert_eq!(b.snapshot_ref(&sc).unwrap().unwrap().seq, 1);
    }

    /// MIG-10: graceful release — fencing A's epoch leaves a dead
    /// marker; B's NEXT acquire takes epoch+1 immediately (no TTL
    /// wait). The 70 s real-cluster fence was this path NOT running
    /// before SIGKILL; the bounded Drop pass covers it now.
    #[test]
    fn fenced_dead_marker_admits_new_owner_immediately() {
        let s = store();
        let now = Arc::new(AtomicU64::new(1_000));
        let clock = {
            let n = now.clone();
            Arc::new(move || n.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let a = ManifestDoPlacement::new(s.clone(), "cell-a", 60_000, 1_000)
            .with_clock(clock.clone());
        let b = ManifestDoPlacement::new(s, "cell-b", 60_000, 1_000).with_clock(clock);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap();
        // Without release, B is fenced for the whole 60 s lease —
        // and the error reports the remaining TTL for the throttled
        // wait log.
        match b.acquire(&sc) {
            Err(PlacementError::Fenced { until_ms, .. }) => {
                assert_eq!(until_ms, 1_000 + 60_000);
            }
            other => panic!("live foreign lease must fence: {other:?}"),
        }
        // Graceful release (actor Drop / drain-exit fence): the dead
        // marker admits B at once even though the TTL is far out.
        a.fence(&sc, lease.epoch).unwrap();
        assert_eq!(b.acquire(&sc).unwrap().epoch.0, 2);
    }

    /// MIG-10: release/acquire CAS race — two threads fence+take
    /// concurrently. Every interleaving is linearizable: B either
    /// loses to the still-live lease (Fenced — retry) or wins
    /// epoch+1 (dead-marker first). No torn record, no panic, and a
    /// follow-up acquire always converges.
    #[test]
    fn concurrent_release_and_acquire_cas_race_is_safe() {
        for _ in 0..32 {
            let s = store();
            let a = Arc::new(ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000));
            let b = Arc::new(ManifestDoPlacement::new(s.clone(), "cell-b", 10_000, 1_000));
            let sc = scope();
            let lease = a.acquire(&sc).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let (a2, b2) = (a.clone(), b.clone());
            let bar = barrier.clone();
            let releaser = std::thread::spawn(move || {
                bar.wait();
                a2.fence(&sc, lease.epoch)
            });
            let sc2 = scope();
            barrier.wait();
            let first = b2.acquire(&sc2);
            releaser.join().unwrap().unwrap();
            match first {
                Ok(l) => assert_eq!(l.epoch.0, 2, "acquire after fence wins epoch+1"),
                Err(PlacementError::Fenced { .. }) => {
                    // Lease still live when B read it — the fence has
                    // now landed, so the retry converges.
                    assert_eq!(b.acquire(&sc2).unwrap().epoch.0, 2);
                }
                Err(e) => panic!("unexpected acquire error: {e}"),
            }
        }
    }

    #[test]
    fn renew_reaffirms_after_restart_by_same_holder() {
        let s = store();
        let a = ManifestDoPlacement::new(s, "cell-a", 10_000, 1_000);
        let sc = scope();
        assert_eq!(a.acquire(&sc).unwrap().epoch.0, 1);
        // Same holder re-acquires its own live lease (cell restart).
        assert_eq!(a.acquire(&sc).unwrap().epoch.0, 1);
    }

    // ---- MIG-17: `reaffirm` — the serialized-switch carried-lease
    // fast path. Same holder + same live epoch reaffirms for ONE
    // read and no write; every other shape defers to `acquire` so
    // fencing is identical to a fresh acquisition. ----

    /// The common carry: record still names us at the carried epoch,
    /// expiry outside the renew window → reaffirmed with ZERO writes.
    #[test]
    fn reaffirm_live_same_epoch_costs_one_read_no_write() {
        let s = Arc::new(vrt_objstore::MemoryStore::new());
        let a = ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap();
        let puts_after_acquire = s.put_calls();
        let re = DoPlacement::reaffirm(&a, &sc, lease.epoch).unwrap();
        assert_eq!(re.epoch, lease.epoch, "carried epoch kept");
        assert_eq!(
            re.expires_at_ms, lease.expires_at_ms,
            "no renew needed outside the window"
        );
        assert_eq!(
            s.put_calls(),
            puts_after_acquire,
            "carry write count must be 0 (one read only)"
        );
    }

    /// Carry arriving already inside the renew window → the reaffirm
    /// refreshes expiry with ONE CAS (still no release+re-acquire,
    /// epoch kept).
    #[test]
    fn reaffirm_inside_renew_window_writes_one_cas() {
        let s = Arc::new(vrt_objstore::MemoryStore::new());
        let now = Arc::new(AtomicU64::new(1_000));
        let clock = {
            let n = now.clone();
            Arc::new(move || n.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let a = ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000)
            .with_clock(clock);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap(); // expires = 1_000 + 10_000
        // Inside renew window (margin*2 = 2_000): now = 11_000 - 2_000 + 1.
        now.store(9_001, Ordering::SeqCst);
        let puts_before = s.put_calls();
        let re = DoPlacement::reaffirm(&a, &sc, lease.epoch).unwrap();
        assert_eq!(re.epoch, lease.epoch);
        assert!(re.expires_at_ms > lease.expires_at_ms);
        assert_eq!(s.put_calls(), puts_before + 1, "exactly one renew CAS");
    }

    /// Holder mismatch — the record moved to a foreign live holder:
    /// reaffirm must take the plain acquire path and fence, never
    /// "adopt" someone else's epoch.
    #[test]
    fn reaffirm_foreign_holder_fences_via_acquire() {
        let s = store();
        let a = ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000);
        let b = ManifestDoPlacement::new(s, "cell-b", 10_000, 1_000);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap();
        // B (carrying a stale claim to the class) reaffirms — the
        // record names cell-a, so B's reaffirm is just an acquire.
        match DoPlacement::reaffirm(&b, &sc, lease.epoch) {
            Err(PlacementError::Fenced { .. }) => {}
            other => panic!("foreign live holder must fence: {other:?}"),
        }
    }

    /// Epoch mismatch under the SAME live holder (a prior attempt's
    /// acquire landed after the carry stashed): adopt the record's
    /// epoch — the record is the truth.
    #[test]
    fn reaffirm_epoch_mismatch_adopts_record_epoch() {
        let s = store();
        let a = ManifestDoPlacement::new(s, "cell-a", 10_000, 1_000);
        let sc = scope();
        let l1 = a.acquire(&sc).unwrap();
        // Simulate the drift: fence then re-acquire under the same
        // holder → record now lives at epoch 2.
        a.fence(&sc, l1.epoch).unwrap();
        let l2 = a.acquire(&sc).unwrap();
        assert_eq!(l2.epoch.0, 2);
        // A carry stashed at epoch 1 reaffirms → adopts epoch 2.
        let re = DoPlacement::reaffirm(&a, &sc, l1.epoch).unwrap();
        assert_eq!(re.epoch.0, 2, "record epoch is the truth");
    }

    /// Carry arrived after the record lapsed — identical to a first
    /// acquisition: CAS epoch+1 (a real write, correctly).
    #[test]
    fn reaffirm_lapsed_record_acquires_epoch_plus_one() {
        let s = store();
        let now = Arc::new(AtomicU64::new(1_000));
        let clock = {
            let n = now.clone();
            Arc::new(move || n.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let a = ManifestDoPlacement::new(s, "cell-a", 10_000, 1_000)
            .with_clock(clock);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap(); // expires 11_000
        now.store(1_000 + 12_001, Ordering::SeqCst); // past expires+margin
        let re = DoPlacement::reaffirm(&a, &sc, lease.epoch).unwrap();
        assert_eq!(re.epoch.0, 2, "lapsed record → epoch+1 like acquire");
    }

    /// Etag contention on the renew CAS → the loop retries cleanly
    /// (bounded, deterministic — injected once).
    #[test]
    fn reaffirm_renew_cas_contention_retries() {
        let s = Arc::new(vrt_objstore::MemoryStore::new());
        let now = Arc::new(AtomicU64::new(1_000));
        let clock = {
            let n = now.clone();
            Arc::new(move || n.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
        };
        let a = ManifestDoPlacement::new(s.clone(), "cell-a", 10_000, 1_000)
            .with_clock(clock);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap();
        now.store(9_001, Ordering::SeqCst); // inside renew window → CAS needed
        s.inject_put_failures(vec![vrt_objstore::StoreError::PreconditionFailed]);
        let re = DoPlacement::reaffirm(&a, &sc, lease.epoch).unwrap();
        assert_eq!(re.epoch, lease.epoch);
    }

    /// The carry is void on a dead-marker record (released by the
    /// drain path mid-switch) → reaffirm = fresh acquire at epoch+1.
    #[test]
    fn reaffirm_dead_marker_takes_epoch_plus_one() {
        let s = store();
        let a = ManifestDoPlacement::new(s, "cell-a", 10_000, 1_000);
        let sc = scope();
        let lease = a.acquire(&sc).unwrap();
        a.fence(&sc, lease.epoch).unwrap();
        let re = DoPlacement::reaffirm(&a, &sc, lease.epoch).unwrap();
        assert_eq!(re.epoch.0, 2);
    }
}
