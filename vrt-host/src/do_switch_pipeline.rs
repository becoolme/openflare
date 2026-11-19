//! MIG-17: bounded-concurrency per-class DO placement pipeline — the
//! serialized-switch critical path.
//!
//! Background: a serialized DO switch previously ran `acquire →
//! handoff → restore → claim → record` **serially per class** on the
//! tenant actor, each step one or more object-store round trips
//! (~8/class on the S3 backend). With 10 classes that was ~6.5 s
//! inside the request-hold window.
//!
//! This module runs the same stage chain per class on a bounded pool
//! of scoped OS threads (`std::thread::scope` — chosen over
//! spawn_blocking + event-reply so actor state can never be installed
//! stale, and no abort/drop path can strand a half-acquired lease
//! set: the whole call still completes synchronously inside the
//! actor turn). Different classes' scopes are fully independent —
//! placement records key on `class_key`, handoff/restore touch only
//! that class's dir — so per-class fan-out preserves semantics.
//!
//! Invariants kept identical to the serial implementation:
//! - phase 2 (writer-epoch `claim` + WAL seq-label read) starts ONLY
//!   after every phase-1 result is in — a failed launch never
//!   advances `writer_epoch` (WAL1 rule), and `wal.start` still runs
//!   after all claims;
//! - any failure releases every lease the pipeline took (the
//!   "partial set") — never a partial launch;
//! - per-op soft timeout + whole-pipeline wall budget fail the class
//!   `Budget` instead of pinning the switch hold.
//!
//! MIG-17 lease carry: classes the just-exited generation still held
//! under the SAME worker arrive as [`Carry::Lease`] and are
//! revalidated by `DoPlacement::reaffirm` (one read, a renew CAS only
//! when already due) instead of release+re-acquire.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::do_placement::{
    DoPlacement, PlacementEpoch, PlacementError, PlacementLease, PlacementScope,
};
use crate::do_placement_store::ManifestDoPlacement;
use crate::do_wal::DoWalReplicator;
use crate::supervisor::DoLeaseEntry;
use vrt_objstore::ObjectStore;

/// Hard ceiling on the fan-out regardless of configured value — the
/// pool is OS threads doing blocking store calls; unbounded fan-out
/// would pin the shared store runtime instead of helping.
pub const MAX_CONCURRENCY: usize = 64;
/// Default per-class pipeline fan-out.
pub const DEFAULT_CONCURRENCY: usize = 8;

/// Pipeline tuning — `SupervisorParams::do_pipeline` carries it from
/// `[supervisor]` config; validation lives in `config.rs`.
#[derive(Clone, Copy, Debug)]
pub struct PipeCfg {
    /// Max per-class pipelines in flight (clamped `[1, MAX_CONCURRENCY]`).
    pub concurrency: usize,
    /// Soft per-op bound: a placement/restore op RETURNING past this
    /// fails its class (`PipeFail::Budget`). Sync store calls cannot
    /// be aborted, so the bound is enforced on completion and before
    /// each subsequent op.
    pub op_timeout: Duration,
    /// Whole-pipeline wall budget measured from `PipeDeps::t0` —
    /// checked before every op; past it, unstarted ops fail `Budget`.
    pub total_budget: Duration,
    /// MIG-17-r2 (B1): bound on the WHOLE restore unit (snapshot
    /// download + epoch mark + WAL replay) — judged only after the
    /// unit completes, never mid-unit. Wider than `op_timeout`:
    /// snapshot pulls legitimately outlive a placement round trip.
    pub restore_timeout: Duration,
}

impl Default for PipeCfg {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            op_timeout: Duration::from_secs(10),
            total_budget: Duration::from_secs(30),
            restore_timeout: Duration::from_secs(120),
        }
    }
}

/// How a class's lease arrives at the pipeline.
#[derive(Clone, Copy, Debug)]
pub enum Carry {
    /// Fresh acquire.
    None,
    /// MIG-17: lease carried from the just-exited generation —
    /// `reaffirm` revalidates instead of `acquire`.
    Lease(PlacementEpoch),
    /// MIG-17: a carried lease whose renewal FAILED during the carry
    /// window — fail closed (no launch), exactly like an acquire
    /// failure; the entry was already fenced best-effort when voided.
    Lost,
}

/// One class's launch inputs — immutable across both phases.
#[derive(Debug)]
pub struct PipeIn {
    pub scope: PlacementScope,
    pub carry: Carry,
    /// DO1 handoff: source worker when the class is a
    /// `transferred_classes` target.
    pub transfer_src: Option<String>,
}

/// The stage a failure belongs to — the supervisor's `last_error`
/// maps 1:1 to the serial implementation's per-stage errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailStage {
    Acquire,
    Handoff,
    Restore,
    Claim,
    /// MIG-17-r2 (H3): skipped before an op because a sibling class
    /// already failed — never the `first_fail` winner when a real
    /// failure exists (it sorts last).
    Aborted,
}

#[derive(Debug)]
pub enum PipeFail {
    /// Placement acquire/reaffirm refused — carries the typed error
    /// for reason metrics and the actor's throttled fenced log.
    Acquire(PlacementError),
    /// `move_class_dir` handoff error (rendered — `HandoffError` is
    /// not `Clone`).
    Handoff(String),
    /// Restore / transfer-baseline gate / WAL replay / manifest read
    /// failure — all share the `do-restore-failed` last_error.
    Restore(String),
    /// `claim_writer` refusal.
    Claim(PlacementError),
    /// Per-op timeout or total-budget exhaustion (carries the op or
    /// `total`) — fail closed like an acquire failure.
    Budget(&'static str),
    /// MIG-17: the class's carried lease failed renewal inside the
    /// carry window — the consume must not launch on an unverifiable
    /// lease; maps to the acquire-failure path.
    CarryLost,
    /// MIG-17-r2 (H3): another class already failed — this op was
    /// never started (fail-fast: no new acquire/download after the
    /// first failure). Carries the skipped op name.
    Aborted(&'static str),
}

impl PipeFail {
    pub fn stage(&self) -> FailStage {
        match self {
            Self::Acquire(_) | Self::Budget(_) | Self::CarryLost => FailStage::Acquire,
            Self::Handoff(_) => FailStage::Handoff,
            Self::Restore(_) => FailStage::Restore,
            Self::Claim(_) => FailStage::Claim,
            Self::Aborted(_) => FailStage::Aborted,
        }
    }

    /// Stable error string for the supervisor's log line.
    pub fn detail(&self) -> String {
        match self {
            Self::Acquire(e) | Self::Claim(e) => e.to_string(),
            Self::Handoff(s) | Self::Restore(s) => s.clone(),
            Self::Budget(op) => format!("placement pipeline budget exceeded at {op}"),
            Self::CarryLost => "carried lease lost during switch window".into(),
            Self::Aborted(op) => format!("pipeline aborted by sibling failure before {op}"),
        }
    }
}

/// Per-class pipeline output.
#[derive(Debug)]
pub struct PipeOk {
    /// WAL stream label (committed snapshot seq) for `wal.start`.
    /// 0 = genesis/no-snapshot (or a tolerated label-read failure —
    /// the serial `.ok()` path degraded identically).
    pub seq_label: u64,
}

pub struct PipeOutcome {
    pub scope: PlacementScope,
    /// The lease this class still holds after the pipeline,
    /// WHATEVER the outcome — the failure path releases exactly this
    /// set ("the partial set" of the serial code).
    pub acquired: Option<PlacementLease>,
    /// The lease was consumed via the carried-lease path.
    pub carried: bool,
    /// MIG-17c: reaffirm adopted a DIFFERENT epoch than the carried
    /// one — the retained WAL tail still belongs to THIS epoch.
    /// Surfaced for metrics/logging only: `wal.start` replaces the
    /// tail outright (old-epoch segments are unadoptable at the new
    /// epoch by namespacing; unparsed frames fold into the db on
    /// the next checkpoint and are captured by the new tail's
    /// immediate first baseline — no drain needed).
    pub stale_tail_epoch: Option<u64>,
    pub result: Result<PipeOk, PipeFail>,
}

/// Snapshot/restore/WAL view the pipeline borrows — the actor's
/// `DoSnapCtx` minus the fields the launch path never reads.
pub struct SnapView<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    pub placement: &'a ManifestDoPlacement,
    pub wal: Option<&'a DoWalReplicator>,
}

/// Shared read-only pipeline context (one launch attempt).
pub struct PipeDeps<'a> {
    pub tenant: &'a str,
    /// `do_spec.tenant_dir` — transfer handoff root.
    pub tenant_dir: &'a std::path::Path,
    pub placement: &'a dyn DoPlacement,
    /// `None` = no object-store surface (standalone LocalPlacement):
    /// record reads, restore and WAL all skip, identical to the
    /// serial path with `deps.do_snap == None`.
    pub snap: Option<SnapView<'a>>,
    pub cfg: PipeCfg,
    /// Wall anchor the total budget measures from — per launch
    /// ATTEMPT (the caller passes `Instant::now()` at pipeline
    /// start; the serialized switch measures exit→completion
    /// separately via `stage="placement"`, and a +5 s retry gets a
    /// fresh budget rather than a permanently-spent window).
    pub t0: Instant,
}

/// Time one placement/restore op and record
/// `vrt_host_do_placement_op_seconds{op}` — every store round trip in
/// the pipeline is observable per op type.
fn timed<R>(tenant: &str, op: &'static str, f: impl FnOnce() -> R) -> (R, Duration) {
    let t = Instant::now();
    let r = f();
    let e = t.elapsed();
    crate::metrics::registry().observe(
        "vrt_host_do_placement_op_seconds",
        &[("tenant", tenant), ("op", op)],
        e.as_secs_f64(),
    );
    (r, e)
}

/// Total-budget gate run BEFORE an op — past `total_budget` nothing
/// new starts (fail closed; the class never launches).
fn budget_gate(deps: &PipeDeps<'_>, op: &'static str) -> Result<(), PipeFail> {
    if deps.t0.elapsed() > deps.cfg.total_budget {
        tracing::error!(
            tenant = %deps.tenant,
            %op,
            "do placement pipeline budget exhausted; refusing op (fail closed)"
        );
        return Err(PipeFail::Budget(op));
    }
    Ok(())
}

/// MIG-17-r2 (H3): the shared fail-fast flag. Any class's real
/// failure — acquire/refusal, store error, budget, panic — flips it
/// once; every class checks it before EVERY external op and skips
/// with `PipeFail::Aborted` instead of starting new acquire/restore/
/// download work after the launch is already dead. Leases already
/// taken still ride `Phase1Out::acquired` into the unified release.
type Abort = AtomicBool;

/// Abort+budget gate run BEFORE an op — a dead attempt starts no new
/// work; a spent budget fails closed. Order: `Aborted` wins over
/// `Budget` (the report should name the real cause).
fn gate(deps: &PipeDeps<'_>, abort: &Abort, op: &'static str) -> Result<(), PipeFail> {
    if abort.load(Ordering::SeqCst) {
        return Err(PipeFail::Aborted(op));
    }
    budget_gate(deps, op)
}

/// Uniform phase-1 failure: mark the attempt dead (siblings stop
/// starting work) and return the outcome — the lease taken so far is
/// preserved inside `out.acquired`.
fn fail_out(out: Phase1Out, abort: &Abort, f: PipeFail) -> Phase1Out {
    abort.store(true, Ordering::SeqCst);
    Phase1Out {
        result: Err(f),
        ..out
    }
}

/// Per-op soft bound — enforced on completion (a sync op cannot be
/// aborted; the lease it may have minted is still released via
/// `PipeOutcome::acquired`).
fn op_gate(deps: &PipeDeps<'_>, op: &'static str, elapsed: Duration) -> Result<(), PipeFail> {
    if elapsed > deps.cfg.op_timeout {
        tracing::error!(
            tenant = %deps.tenant,
            %op,
            elapsed_ms = elapsed.as_millis() as u64,
            "do placement op exceeded soft timeout; failing class (fail closed)"
        );
        return Err(PipeFail::Budget(op));
    }
    Ok(())
}

/// `acquire`/`reaffirm` error → the serial reason label for
/// `vrt_host_do_placement_failures_total`.
fn acquire_reason(e: &PlacementError) -> &'static str {
    match e {
        PlacementError::Fenced { .. } => "fenced",
        PlacementError::Reserved { .. } => "reserved",
        PlacementError::Precondition(_) => "precondition",
        PlacementError::Store(_) => "store",
    }
}

struct Phase1Out {
    /// Lease taken this phase (released by the caller on ANY failure
    /// in the launch — see `PipeOutcome::acquired`).
    acquired: Option<PlacementLease>,
    /// `reaffirm` kept the carried epoch.
    carried: bool,
    /// See `PipeOutcome::stale_tail_epoch`.
    stale_tail_epoch: Option<u64>,
    result: Result<(), PipeFail>,
}

impl Phase1Out {
    fn fail(f: PipeFail) -> Self {
        Self {
            acquired: None,
            carried: false,
            stale_tail_epoch: None,
            result: Err(f),
        }
    }
}

/// Bounded fan-out: `k` scoped OS threads pull work items off a
/// shared atomic cursor. `f` must never panic (the phase wrappers
/// catch their own panics into typed failures).
fn fan_out<T: Send>(
    n: usize,
    k: usize,
    f: impl Fn(usize) -> T + Sync,
) -> Vec<T> {
    let next = AtomicUsize::new(0);
    let out: Mutex<Vec<Option<T>>> = Mutex::new((0..n).map(|_| None).collect());
    std::thread::scope(|s| {
        for _ in 0..k {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    return;
                }
                out.lock().unwrap()[i] = Some(f(i));
            });
        }
    });
    out.into_inner()
        .unwrap()
        .into_iter()
        .map(|o| o.expect("fan_out slot"))
        .collect()
}

/// Phase 1 for one class: acquire (or carried reaffirm) → record
/// read → transfer handoff → snapshot/WAL restore. Never panics.
///
/// B2: `lease_slot` is a guard-external escape hatch — the moment a
/// lease is taken it is ALSO dropped into the slot, so a panic later
/// in the phase still surfaces it to `phase_one_guard` for the
/// release set (otherwise the lease would vanish with the poisoned
/// stack frame — and a carried lease taken from `carried_map` would
/// live in NO list at all).
fn phase_one(
    deps: &PipeDeps<'_>,
    input: &PipeIn,
    abort: &Abort,
    lease_slot: &Mutex<Option<PlacementLease>>,
) -> Phase1Out {
    let scope = &input.scope;
    let tenant = deps.tenant;
    // A voided carry must NOT touch the store at all — the entry was
    // already fenced when `service_do_leases` voided it; this attempt
    // fails closed and the +5 s retry's plain acquire decides cleanly.
    if let Carry::Lost = input.carry {
        abort.store(true, Ordering::SeqCst);
        return Phase1Out::fail(PipeFail::CarryLost);
    }
    if let Err(f) = gate(deps, abort, "acquire") {
        abort.store(true, Ordering::SeqCst);
        return Phase1Out::fail(f);
    }
    // ---- acquire — carried leases reaffirm (one read; a renew CAS
    // only when already due) instead of release + re-acquire ----
    let (acq, acq_elapsed) = timed(tenant, "acquire", || match input.carry {
        Carry::Lease(epoch) => deps.placement.reaffirm(scope, epoch),
        _ => deps.placement.acquire(scope),
    });
    let mut out = Phase1Out {
        acquired: None,
        carried: false,
        stale_tail_epoch: None,
        result: Ok(()),
    };
    match acq {
        Ok(lease) => {
            crate::metrics::registry().inc(
                "vrt_host_do_lease_acquired_total",
                &[("tenant", tenant), ("worker", scope.worker.as_str())],
                1.0,
            );
            crate::metrics::registry().set(
                "vrt_host_do_placement_epoch",
                &[("tenant", tenant), ("worker", scope.worker.as_str())],
                lease.epoch.0 as f64,
            );
            if let Carry::Lease(expected) = input.carry {
                if lease.epoch != expected {
                    // MIG-17c: the record advanced under the same
                    // holder — `reaffirm` adopted the newer epoch,
                    // which is a legit lease but NOT the carried
                    // one: the retained tail is still bound to
                    // `expected`. Flagged for observability; the
                    // fresh-epoch tail's immediate first baseline
                    // covers the remnant frames.
                    out.stale_tail_epoch = Some(expected.0);
                    tracing::warn!(
                        tenant = %tenant,
                        class = %scope.class_key,
                        carried_epoch = expected.0,
                        adopted_epoch = lease.epoch.0,
                        "do reaffirm adopted a different epoch — carried tail will be drained before wal.start"
                    );
                }
            }
            if input
                .carry
                .matches_lease(&lease)
            {
                out.carried = true;
                crate::metrics::registry().inc(
                    "vrt_host_do_lease_carried_total",
                    &[("tenant", tenant), ("worker", scope.worker.as_str())],
                    1.0,
                );
            }
            // B2: mirror into the panic slot BEFORE any further work —
            // a panic below must still surface this lease for release.
            *lease_slot.lock().unwrap() = Some(lease.clone());
            out.acquired = Some(lease);
            if let Err(f) = op_gate(deps, "acquire", acq_elapsed) {
                return fail_out(out, abort, f);
            }
        }
        Err(e) => {
            crate::metrics::registry().inc(
                "vrt_host_do_placement_failures_total",
                &[("tenant", tenant), ("reason", acquire_reason(&e))],
                1.0,
            );
            if matches!(e, PlacementError::Fenced { .. }) {
                crate::metrics::registry().inc(
                    "vrt_host_do_placement_fenced_total",
                    &[("tenant", tenant), ("worker", scope.worker.as_str())],
                    1.0,
                );
            }
            if deps.placement.authoritative() {
                // Logged on the actor through the fenced throttle —
                // the pipeline returns the typed error untouched.
                return fail_out(out, abort, PipeFail::Acquire(e));
            }
            tracing::warn!(
                tenant = %tenant,
                worker = %scope.worker,
                %e,
                "do placement acquire failed (advisory)"
            );
            // Advisory impls keep the serial warn-and-continue: no
            // lease, and the class skips every later stage exactly
            // like a lease that never entered `leases`.
            return out;
        }
    }
    let epoch = out.acquired.as_ref().map(|l| l.epoch.0).unwrap_or(0);
    // ---- record read — ONE GET folds the serial path's
    // `snapshot_ref` + gate `record` + WAL `writer_epoch` reads
    // (the snapshot/accepted_baseline/writer_epoch fields cannot
    // change mid-pipeline: only our epoch commits them) ----
    let mut rec_snapshot = None;
    let mut rec_accepted = None;
    let mut rec_writer_epoch = 0u64;
    if let Some(snap) = &deps.snap {
        if let Err(f) = gate(deps, abort, "record_read") {
            return fail_out(out, abort, f);
        }
        let (rec, el) = timed(tenant, "record_read", || snap.placement.record(scope));
        if let Err(f) = op_gate(deps, "record_read", el) {
            return fail_out(out, abort, f);
        }
        match rec {
            Ok(rec) => {
                if let Some(rec) = rec {
                    rec_snapshot = rec.snapshot;
                    rec_accepted = rec.accepted_baseline;
                    rec_writer_epoch = rec.writer_epoch;
                }
            }
            Err(e) => {
                tracing::error!(
                    tenant = %tenant,
                    class = %scope.class_key,
                    %e,
                    "do snapshot manifest read failed (fail closed)"
                );
                return fail_out(out, abort, PipeFail::Restore(e.to_string()));
            }
        }
    }
    // ---- DO1 `transferred_classes` handoff — under the freshly held
    // lease (the acquire fenced the old owner; the serialized gate
    // proved its writer dead), before workerd opens the dir ----
    let mut transfer_pending = false;
    if let Some(src_worker) = &input.transfer_src {
        if let Err(f) = gate(deps, abort, "handoff") {
            return fail_out(out, abort, f);
        }
        match crate::do_transfer::move_class_dir(
            deps.tenant_dir,
            src_worker,
            &scope.class_key,
            &scope.class_dir,
        ) {
            Ok(crate::do_transfer::HandoffMove::Moved) => {
                crate::metrics::registry().inc(
                    "vrt_host_do_transfer_total",
                    &[("tenant", tenant), ("outcome", "moved")],
                    1.0,
                );
                tracing::info!(
                    tenant = %tenant,
                    worker = %scope.worker,
                    from_worker = %src_worker,
                    class = %scope.class_key,
                    "do transfer: source dir moved into destination segment"
                );
            }
            Ok(crate::do_transfer::HandoffMove::Quarantined) => {
                crate::metrics::registry().inc(
                    "vrt_host_do_transfer_total",
                    &[("tenant", tenant), ("outcome", "quarantined-src")],
                    1.0,
                );
                tracing::error!(
                    tenant = %tenant,
                    worker = %scope.worker,
                    from_worker = %src_worker,
                    class = %scope.class_key,
                    "do transfer: dst already populated — fenced source dir quarantined"
                );
            }
            Ok(crate::do_transfer::HandoffMove::NoLocalSource) => {
                if !crate::do_transfer::dir_nonempty(&scope.class_dir) {
                    transfer_pending = true;
                }
            }
            Err(e) => {
                crate::metrics::registry().inc(
                    "vrt_host_do_transfer_total",
                    &[("tenant", tenant), ("outcome", "failed")],
                    1.0,
                );
                tracing::error!(
                    tenant = %tenant,
                    worker = %scope.worker,
                    from_worker = %src_worker,
                    class = %scope.class_key,
                    %e,
                    "do transfer handoff failed; refusing to launch (fail closed)"
                );
                return fail_out(out, abort, PipeFail::Handoff(e.to_string()));
            }
        }
    }
    // ---- restore committed snapshots into empty class dirs BEFORE
    // workerd opens them — a non-empty local dir wins (written under
    // a held lease, ≥ any snapshot) ----
    let Some(snap) = &deps.snap else {
        return out;
    };
    match rec_snapshot {
        Some(r) => {
            if transfer_pending
                && !crate::do_transfer::transfer_baseline_ok(
                    &r,
                    rec_accepted,
                    epoch,
                    rec_writer_epoch,
                )
            {
                crate::metrics::registry().inc(
                    "vrt_host_do_transfer_total",
                    &[("tenant", tenant), ("outcome", "awaiting-final-snapshot")],
                    1.0,
                );
                tracing::error!(
                    tenant = %tenant,
                    class = %scope.class_key,
                    snapshot_epoch = r.epoch,
                    "do transfer: only a periodic snapshot exists; refusing to launch (fail closed — awaiting Final baseline or admin accept)"
                );
                return fail_out(
                    out,
                    abort,
                    PipeFail::Restore("transfer baseline not provable".into()),
                );
            }
            // Stale-dir guard (unchanged): a dir written under an
            // epoch OLDER than the committed snapshot's epoch is
            // fenced-away provenance — replace it.
            let local_epoch = crate::do_snapshot::local_dir_epoch(scope);
            let allow_replace = local_epoch.is_some_and(|e| e < r.epoch);
            // MIG-17-r2 (B1): restore + epoch mark + WAL replay are
            // ONE atomic preparation step — no budget/op verdict may
            // split them (a timeout between restore and replay used
            // to leave an Intact dir that skipped replay forever → a
            // silent baseline-only launch dropping committed writes).
            // The unit is gated BEFORE it and judged AFTER it, and it
            // owns a WIDER bound (`restore_timeout`) than the per-op
            // timeout — snapshot pulls legitimately outlive placement
            // round trips. `replay_pending` is the durable "tail not
            // applied" flag beside the dir: written as soon as the
            // dir is a restore product, cleared only on replay
            // success — every retry re-runs the tail.
            if let Err(f) = gate(deps, abort, "restore_read") {
                return fail_out(out, abort, f);
            }
            let pending_ctx = crate::do_snapshot::replay_pending(scope);
            let want_ctx = crate::do_snapshot::ReplayPending {
                snap_epoch: r.epoch,
                snap_seq: r.seq,
                writer_epoch: rec_writer_epoch,
            };
            // MIG-17-r3 (B1a): the marker is armed INSIDE
            // `restore_snapshot` via `pre_place` — only when the dir
            // is actually about to become a restore product. A Live
            // or Intact dir NEVER gets one here: a transient manifest
            // failure or a pre-clear crash can no longer leave a
            // marker bound to newer-than-baseline live state.
            let mark = move || {
                crate::do_snapshot::mark_replay_pending(scope, want_ctx).map_err(|e| {
                    crate::metrics::registry().inc(
                        "vrt_host_do_wal_replay_marker_failures_total",
                        &[("tenant", tenant)],
                        1.0,
                    );
                    tracing::error!(
                        tenant = %tenant,
                        class = %scope.class_key,
                        %e,
                        "do wal replay-pending marker write failed; fail closed"
                    );
                    e
                })
            };
            let unit_t0 = Instant::now();
            let (res, _el) = timed(tenant, "restore_read", || {
                crate::do_snapshot::restore_snapshot(
                    snap.store,
                    &r,
                    scope,
                    allow_replace,
                    snap.wal.is_some().then_some(
                        &mark as &dyn Fn() -> std::io::Result<()>,
                    ),
                )
            });
            match res {
                Ok(restored) => {
                    // Replay due iff this attempt restored the dir, or
                    // an interrupted attempt's marker binds the dir to
                    // THIS baseline + writer epoch.
                    // B1a guard: a residual marker (e.g. written by an
                    // older build) on a dir holding live `*-wal`/`-shm`
                    // journal files is STALE — live commits are newer
                    // than the baseline; replaying would regress them.
                    let marker_match = pending_ctx == Some(want_ctx);
                    let live_dir = marker_match
                        && !restored
                        && crate::do_snapshot::has_live_wal_frames(&scope.class_dir);
                    let needs_replay = snap.wal.is_some()
                        && (restored || (marker_match && !live_dir));
                    if live_dir {
                        crate::metrics::registry().inc(
                            "vrt_host_do_wal_replay_marker_stale_total",
                            &[("tenant", tenant)],
                            1.0,
                        );
                        tracing::warn!(
                            tenant = %tenant,
                            class = %scope.class_key,
                            "do wal replay-pending marker on a live dir — dropped, no replay"
                        );
                    }
                    if needs_replay {
                        // B1(b): the marker must be durably written —
                        // a failed write is a typed retryable error,
                        // never a silent proceed.
                        if let Err(e) = mark() {
                            return fail_out(
                                out,
                                abort,
                                PipeFail::Restore(format!("replay marker write: {e}")),
                            );
                        }
                        let wal = snap.wal.unwrap();
                        // WAL1: extend the baseline with the last
                        // writer's uploaded chain — only when the
                        // dir actually came FROM the snapshot (or is
                        // a marker-proven unreplayed product of it).
                        let (rep, _el2) = timed(tenant, "restore_read", || {
                            crate::do_wal::replay_for_restore(
                                snap.store,
                                &scope.class_key,
                                &scope.class_dir,
                                Some(&r),
                                rec_writer_epoch,
                                wal.replay_max_bytes(),
                                tenant,
                            )
                        });
                        match rep {
                            Ok(rep) if rep.stems_rejected > 0 => {
                                // MIG-17c: stems whose chain was
                                // selected but then failed
                                // fold/apply/integrity — those frames
                                // SHOULD have been adopted. Refuse to
                                // launch: the marker stays so a retry
                                // re-runs replay after repair.
                                crate::metrics::registry().inc(
                                    "vrt_host_do_wal_replay_total",
                                    &[("tenant", tenant), ("outcome", "rejected")],
                                    1.0,
                                );
                                tracing::error!(
                                    tenant = %tenant,
                                    class = %scope.class_key,
                                    stems_rejected = rep.stems_rejected,
                                    stems_applied = rep.stems_applied,
                                    "do wal replay rejected selected stems; refusing to launch (fail closed — marker kept for retry)"
                                );
                                return fail_out(
                                    out,
                                    abort,
                                    PipeFail::Restore(format!(
                                        "wal replay rejected {} stem(s)",
                                        rep.stems_rejected
                                    )),
                                );
                            }
                            Ok(rep) => {
                                crate::do_snapshot::clear_replay_pending(scope);
                                crate::metrics::registry().inc(
                                    "vrt_host_do_wal_replay_total",
                                    &[("tenant", tenant), ("outcome", "ok")],
                                    1.0,
                                );
                                tracing::info!(
                                    tenant = %tenant,
                                    class = %scope.class_key,
                                    segs = rep.segs_applied,
                                    frames = rep.frames_applied,
                                    last_commit_frame = rep.last_commit_frame,
                                    segs_skipped = rep.segs_skipped,
                                    truncated = rep.truncated,
                                    "do wal replay applied post-baseline chain"
                                );
                            }
                            Err(e) => {
                                crate::metrics::registry().inc(
                                    "vrt_host_do_wal_replay_total",
                                    &[("tenant", tenant), ("outcome", "failed")],
                                    1.0,
                                );
                                tracing::error!(
                                    tenant = %tenant,
                                    class = %scope.class_key,
                                    %e,
                                    "do wal replay failed; refusing to launch (fail closed — baseline alone would drop committed writes)"
                                );
                                // Marker stays → retry re-runs replay.
                                return fail_out(
                                    out,
                                    abort,
                                    PipeFail::Restore(e.to_string()),
                                );
                            }
                        }
                    } else {
                        // No replay needed — clear either our own
                        // pre-mark (Intact dir, no prior marker) or a
                        // stale marker for a superseded baseline /
                        // dead writer epoch.
                        crate::do_snapshot::clear_replay_pending(scope);
                    }
                    crate::do_snapshot::mark_local_dir_epoch(scope, epoch);
                    // The ONE verdict for the whole unit, judged only
                    // now — an overrun still fails the attempt but the
                    // retry converges fast (dir Intact, marker clear).
                    if unit_t0.elapsed() > deps.cfg.restore_timeout {
                        tracing::error!(
                            tenant = %tenant,
                            class = %scope.class_key,
                            elapsed_ms = unit_t0.elapsed().as_millis() as u64,
                            "do restore+replay unit exceeded restore timeout; failing class (fail closed)"
                        );
                        return fail_out(out, abort, PipeFail::Budget("restore_read"));
                    }
                    crate::metrics::registry().inc(
                        "vrt_host_do_restore_total",
                        &[
                            ("tenant", tenant),
                            (
                                "outcome",
                                if restored {
                                    "restored"
                                } else {
                                    "skipped-local"
                                },
                            ),
                        ],
                        1.0,
                    );
                }
                Err(e) => {
                    // Restore left the dir untouched (atomic place).
                    // Our pre-mark survives → the retry re-runs the
                    // whole unit (restore again, then replay).
                    crate::metrics::registry().inc(
                        "vrt_host_do_restore_total",
                        &[("tenant", tenant), ("outcome", "failed")],
                        1.0,
                    );
                    tracing::error!(
                        tenant = %tenant,
                        class = %scope.class_key,
                        %e,
                        "do snapshot restore failed; refusing to launch (fail closed)"
                    );
                    return fail_out(out, abort, PipeFail::Restore(e.to_string()));
                }
            }
        }
        None => {
            if transfer_pending {
                crate::metrics::registry().inc(
                    "vrt_host_do_transfer_total",
                    &[("tenant", tenant), ("outcome", "awaiting-snapshot")],
                    1.0,
                );
                tracing::error!(
                    tenant = %tenant,
                    class = %scope.class_key,
                    "do transfer: no local source dir and no committed snapshot; refusing to launch (fail closed)"
                );
                return fail_out(
                    out,
                    abort,
                    PipeFail::Restore("transfer source missing and no baseline".into()),
                );
            }
            // WAL1: genesis rebuild — no baseline but the writer
            // epoch's genesis segments may reconstruct the db. Same
            // B1 atomicity: a genesis replay interrupted mid-way can
            // leave a partially-applied dir that `dir_nonempty` would
            // otherwise gate OUT of the retry's replay — the pending
            // marker (ctx 0,0 + writer epoch) forces it back in.
            if let Some(wal) = snap.wal {
                let pending_ctx = crate::do_snapshot::replay_pending(scope);
                let pending_match = pending_ctx.is_some_and(|p| {
                    p.snap_epoch == 0 && p.snap_seq == 0 && p.writer_epoch == rec_writer_epoch
                });
                // B1a parity: a genesis marker on a dir holding live
                // journal files is stale — never replay genesis frames
                // over live commits.
                let live_dir = pending_match
                    && crate::do_snapshot::has_live_wal_frames(&scope.class_dir);
                if !crate::do_transfer::dir_nonempty(&scope.class_dir)
                    || (pending_match && !live_dir)
                {
                    if let Err(f) = gate(deps, abort, "restore_read") {
                        return fail_out(out, abort, f);
                    }
                    // B1(b): fail closed on the marker write — same
                    // contract as the baseline arm.
                    if let Err(e) = crate::do_snapshot::mark_replay_pending(
                        scope,
                        crate::do_snapshot::ReplayPending {
                            snap_epoch: 0,
                            snap_seq: 0,
                            writer_epoch: rec_writer_epoch,
                        },
                    ) {
                        crate::metrics::registry().inc(
                            "vrt_host_do_wal_replay_marker_failures_total",
                            &[("tenant", tenant)],
                            1.0,
                        );
                        tracing::error!(
                            tenant = %tenant,
                            class = %scope.class_key,
                            %e,
                            "do wal replay marker write failed — refusing genesis replay (fail closed)"
                        );
                        return fail_out(
                            out,
                            abort,
                            PipeFail::Restore(format!("replay marker write: {e}")),
                        );
                    }
                    let unit_t0 = Instant::now();
                    let (rep, _el) = timed(tenant, "restore_read", || {
                        crate::do_wal::replay_for_restore(
                            snap.store,
                            &scope.class_key,
                            &scope.class_dir,
                            None,
                            rec_writer_epoch,
                            wal.replay_max_bytes(),
                            tenant,
                        )
                    });
                    match rep {
                        // `stems_rejected` is unreachable in genesis
                        // mode: `replay_for_restore` skips every stem
                        // pre-selection (P0-C genesis `continue`), so
                        // no rejected arm is needed here.
                        Ok(_) => crate::do_snapshot::clear_replay_pending(scope),
                        Err(e) => {
                            crate::metrics::registry().inc(
                                "vrt_host_do_wal_replay_total",
                                &[("tenant", tenant), ("outcome", "genesis-failed")],
                                1.0,
                            );
                            tracing::error!(
                                tenant = %tenant,
                                class = %scope.class_key,
                                %e,
                                "do wal genesis replay failed; refusing to launch (fail closed)"
                            );
                            // Marker stays → retry re-runs replay.
                            return fail_out(out, abort, PipeFail::Restore(e.to_string()));
                        }
                    }
                    if unit_t0.elapsed() > deps.cfg.restore_timeout {
                        return fail_out(out, abort, PipeFail::Budget("restore_read"));
                    }
                } else if pending_ctx.is_some() {
                    // Stale genesis marker for a dead writer epoch.
                    crate::do_snapshot::clear_replay_pending(scope);
                }
            }
            crate::do_snapshot::mark_local_dir_epoch(scope, epoch);
        }
    }
    out
}

/// Phase 2 for one class: writer-epoch claim → WAL seq-label read.
/// Runs only once EVERY class's phase 1 succeeded (WAL1 invariant).
fn phase_two(
    deps: &PipeDeps<'_>,
    input: &PipeIn,
    p1: &Phase1Out,
    abort: &Abort,
) -> Result<u64, PipeFail> {
    let Some(lease) = &p1.acquired else {
        return Ok(0);
    };
    gate(deps, abort, "claim")?;
    if deps.placement.authoritative() {
        let (res, el) = timed(deps.tenant, "claim", || {
            deps.placement.claim_writer(&input.scope, lease.epoch)
        });
        match res {
            Ok(()) => {
                if let Err(f) = op_gate(deps, "claim", el) {
                    abort.store(true, Ordering::SeqCst);
                    return Err(f);
                }
            }
            Err(e) => {
                crate::metrics::registry().inc(
                    "vrt_host_do_writer_claim_total",
                    &[("tenant", deps.tenant), ("outcome", "failed")],
                    1.0,
                );
                tracing::error!(
                    tenant = %deps.tenant,
                    worker = %input.scope.worker,
                    class = %input.scope.class_key,
                    %e,
                    "do writer-epoch claim failed; refusing to launch (fail closed)"
                );
                abort.store(true, Ordering::SeqCst);
                return Err(PipeFail::Claim(e));
            }
        }
    }
    // WAL stream label — the committed snapshot seq AFTER the claim.
    // A read failure degrades to label 0 (the serial `.ok()` path was
    // identically tolerant); a slow read still hits the op bound.
    if let Some(snap) = &deps.snap {
        if snap.wal.is_some() {
            gate(deps, abort, "record_read")?;
            let (rec, el) = timed(deps.tenant, "record_read", || {
                snap.placement.record(&input.scope)
            });
            op_gate(deps, "record_read", el)?;
            return Ok(rec
                .ok()
                .flatten()
                .and_then(|x| x.snapshot)
                .map(|x| x.seq)
                .unwrap_or(0));
        }
    }
    Ok(0)
}

fn phase_one_guard(deps: &PipeDeps<'_>, input: &PipeIn, abort: &Abort) -> Phase1Out {
    // B2: the lease slot lives OUTSIDE the unwind boundary — the
    // moment `phase_one` takes a lease it drops a copy here, so a
    // panic mid-phase cannot strand it (the poisoned stack frame
    // dies, the slot survives, `release_all` still fences it).
    let slot: Mutex<Option<PlacementLease>> = Mutex::new(None);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        phase_one(deps, input, abort, &slot)
    })) {
        Ok(mut o) => {
            // Belt & braces: if a store path ever set the slot without
            // `out.acquired` (a future edit), prefer the slot — a
            // reported lease is always releasable.
            if o.acquired.is_none() {
                o.acquired = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
            }
            o
        }
        Err(_) => {
            abort.store(true, Ordering::SeqCst);
            let acquired = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
            let carried = acquired
                .as_ref()
                .is_some_and(|l| input.carry.matches_lease(l));
            let stale_tail_epoch = match (&input.carry, &acquired) {
                (Carry::Lease(expected), Some(l)) if l.epoch != *expected => {
                    Some(expected.0)
                }
                _ => None,
            };
            Phase1Out {
                acquired,
                carried,
                stale_tail_epoch,
                result: Err(PipeFail::Restore("pipeline panic".into())),
            }
        }
    }
}

fn phase_two_guard(
    deps: &PipeDeps<'_>,
    input: &PipeIn,
    p1: &Phase1Out,
    abort: &Abort,
) -> Result<u64, PipeFail> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        phase_two(deps, input, p1, abort)
    })) {
        Ok(r) => r,
        Err(_) => {
            abort.store(true, Ordering::SeqCst);
            Err(PipeFail::Restore("pipeline panic".into()))
        }
    }
}

impl Carry {
    /// The lease a reaffirm produced still IS the carried epoch (a
    /// reaffirm that fell back to `acquire` — lapsed record, epoch
    /// drift — returns a different epoch and does not count as a
    /// carried lease).
    fn matches_lease(&self, lease: &PlacementLease) -> bool {
        matches!(self, Carry::Lease(e) if *e == lease.epoch)
    }
}

/// Run the per-class pipeline: phase 1 (acquire → record → handoff →
/// restore) for every class under `cfg.concurrency` fan-out; then —
/// only when every phase-1 result is `Ok` — phase 2 (claim →
/// seq-label) under the same bound. Outcomes arrive in input order.
pub fn run(deps: &PipeDeps<'_>, inputs: &[PipeIn]) -> Vec<PipeOutcome> {
    let n = inputs.len();
    if n == 0 {
        return Vec::new();
    }
    let k = deps.cfg.concurrency.clamp(1, MAX_CONCURRENCY).min(n);
    // MIG-17-r2 (H3): one failure kills the attempt — siblings check
    // the flag before every op and skip instead of starting new
    // acquire/download work (fail-fast, never a partial launch).
    let abort = AtomicBool::new(false);
    let p1: Vec<Phase1Out> = fan_out(n, k, |i| phase_one_guard(deps, &inputs[i], &abort));
    let all_ok = p1.iter().all(|o| o.result.is_ok());
    // WAL1 invariant: no writer-epoch claim until EVERY class's
    // acquire+handoff+restore succeeded — a failed launch never
    // advances `writer_epoch`. Phase 2 is skipped wholesale on any
    // phase-1 failure.
    let p2: Vec<Result<u64, PipeFail>> = if all_ok {
        fan_out(n, k, |i| phase_two_guard(deps, &inputs[i], &p1[i], &abort))
    } else {
        Vec::new()
    };
    let mut p2 = p2.into_iter();
    inputs
        .iter()
        .zip(p1)
        .map(|(input, o1)| {
            let result = match (o1.result, all_ok) {
                (Err(e), _) => Err(e),
                (Ok(()), true) => match p2.next().expect("phase2 slot") {
                    Ok(seq_label) => Ok(PipeOk { seq_label }),
                    Err(e) => Err(e),
                },
                // Phase 2 skipped wholesale — this class's phase-1
                // lease is still reported via `acquired` so the
                // partial set releases exactly like the serial path.
                (Ok(()), false) => Ok(PipeOk { seq_label: 0 }),
            };
            PipeOutcome {
                scope: input.scope.clone(),
                acquired: o1.acquired,
                carried: o1.carried,
                stale_tail_epoch: o1.stale_tail_epoch,
                result,
            }
        })
        .collect()
}

/// Bounded-parallel fence for the launch-failure release — same
/// "release the partial set" semantics as `release_leases`, ~ceil(N/K)
/// round trips instead of N. Errors log-and-continue per class (the
/// remainder still lapses by TTL — crash semantics unchanged).
pub(crate) fn release_all(
    placement: &dyn DoPlacement,
    tenant: &str,
    leases: &[DoLeaseEntry],
    concurrency: usize,
) {
    if leases.is_empty() {
        return;
    }
    let k = concurrency.clamp(1, MAX_CONCURRENCY).min(leases.len());
    fan_out(leases.len(), k, |i| {
        let l = &leases[i];
        let (_r, _e) = timed(tenant, "release", || placement.fence(&l.scope, l.epoch));
        if let Err(e) = _r {
            tracing::warn!(
                class = %l.scope.class_key,
                %e,
                "do lease release (fence) failed; lease lapses by TTL"
            );
        }
    });
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::do_placement::PlacementScope;
    use std::sync::atomic::Ordering as AOrd;
    use std::time::Duration;

    fn scope(worker: &str, key: &str) -> PlacementScope {
        PlacementScope {
            tenant: "t".into(),
            worker: worker.into(),
            class_key: key.into(),
            class_dir: std::path::PathBuf::from("/x"),
        }
    }

    /// Injected-delay + outcome fake: `sleep` per acquire/reaffirm,
    /// per-class_key failure injection, concurrency watermark and
    /// per-op call counters.
    struct SpyPlacement {
        sleep: Duration,
        acquires: AtomicUsize,
        reaffirms: AtomicUsize,
        claims: AtomicUsize,
        fences: AtomicUsize,
        renews: AtomicUsize,
        inflight: AtomicUsize,
        max_inflight: AtomicUsize,
        fail: Mutex<std::collections::BTreeMap<String, &'static str>>,
    }

    impl SpyPlacement {
        fn new(sleep: Duration) -> Self {
            Self {
                sleep,
                acquires: AtomicUsize::new(0),
                reaffirms: AtomicUsize::new(0),
                claims: AtomicUsize::new(0),
                fences: AtomicUsize::new(0),
                renews: AtomicUsize::new(0),
                inflight: AtomicUsize::new(0),
                max_inflight: AtomicUsize::new(0),
                fail: Mutex::new(Default::default()),
            }
        }

        fn gate(&self, s: &PlacementScope) -> Result<(), PlacementError> {
            let n = self.inflight.fetch_add(1, AOrd::SeqCst) + 1;
            self.max_inflight.fetch_max(n, AOrd::SeqCst);
            std::thread::sleep(self.sleep);
            self.inflight.fetch_sub(1, AOrd::SeqCst);
            match self.fail.lock().unwrap().get(&s.class_key) {
                Some(&"fenced") => Err(PlacementError::Fenced {
                    current: 9,
                    until_ms: 0,
                }),
                Some(&"reserved") => Err(PlacementError::Reserved { until_ms: 0 }),
                Some(&"store") => Err(PlacementError::Store(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "store down",
                ))),
                _ => Ok(()),
            }
        }

        fn fail_on(&self, class_key: &str, kind: &'static str) {
            self.fail
                .lock()
                .unwrap()
                .insert(class_key.to_string(), kind);
        }
    }

    impl DoPlacement for SpyPlacement {
        fn authoritative(&self) -> bool {
            true
        }
        fn acquire(
            &self,
            s: &PlacementScope,
        ) -> Result<PlacementLease, PlacementError> {
            self.acquires.fetch_add(1, AOrd::SeqCst);
            self.gate(s)?;
            Ok(PlacementLease {
                epoch: PlacementEpoch(3),
                expires_at_ms: u64::MAX / 2,
            })
        }
        fn reaffirm(
            &self,
            s: &PlacementScope,
            expected: PlacementEpoch,
        ) -> Result<PlacementLease, PlacementError> {
            self.reaffirms.fetch_add(1, AOrd::SeqCst);
            self.gate(s)?;
            Ok(PlacementLease {
                epoch: expected,
                expires_at_ms: u64::MAX / 2,
            })
        }
        fn renew(
            &self,
            _s: &PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<u64, PlacementError> {
            self.renews.fetch_add(1, AOrd::SeqCst);
            Ok(u64::MAX / 2)
        }
        fn fence(
            &self,
            _s: &PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            std::thread::sleep(self.sleep);
            self.fences.fetch_add(1, AOrd::SeqCst);
            Ok(())
        }
        fn claim_writer(
            &self,
            _s: &PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            self.claims.fetch_add(1, AOrd::SeqCst);
            Ok(())
        }
    }

    fn mk_deps<'a>(
        placement: &'a SpyPlacement,
        tenant_dir: &'a std::path::Path,
        cfg: PipeCfg,
    ) -> PipeDeps<'a> {
        PipeDeps {
            tenant: "t",
            tenant_dir,
            placement,
            snap: None,
            cfg,
            t0: Instant::now(),
        }
    }

    fn pipe_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "vrt-pipe-{tag}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn inputs(n: usize, carry: Carry) -> Vec<PipeIn> {
        (0..n)
            .map(|i| PipeIn {
                scope: scope("w", &format!("ck{i:02}")),
                carry,
                transfer_src: None,
            })
            .collect()
    }

    fn cfg(k: usize) -> PipeCfg {
        PipeCfg {
            concurrency: k,
            op_timeout: Duration::from_secs(30),
            total_budget: Duration::from_secs(30),
            restore_timeout: Duration::from_secs(120),
        }
    }

    /// THE MIG-17 bound: N classes at concurrency K cost
    /// ~ceil(N/K) single-class times — not N×. 9 classes × 80 ms at
    /// K=4 ≈ 3 rounds ≈ 240 ms wall, nowhere near the serial 720 ms.
    #[test]
    fn pipeline_bounded_wall_time_scales_with_ceil_n_over_k() {
        let p = SpyPlacement::new(Duration::from_millis(80));
        let dir = pipe_dir("t4");
        let deps = mk_deps(&p, &dir, cfg(4));
        let ins = inputs(9, Carry::None);
        let t0 = Instant::now();
        let outcomes = run(&deps, &ins);
        let el = t0.elapsed();
        assert!(
            outcomes.iter().all(|o| o.result.is_ok()),
            "all classes ok"
        );
        assert_eq!(p.acquires.load(AOrd::SeqCst), 9);
        assert_eq!(p.claims.load(AOrd::SeqCst), 9, "phase 2 ran for all");
        assert!(
            p.max_inflight.load(AOrd::SeqCst) <= 4,
            "fan-out bound respected"
        );
        assert!(
            el < Duration::from_millis(450),
            "wall {el:?} must be ~3×80ms not 9×80ms"
        );
        assert!(el >= Duration::from_millis(160), "work actually ran");
    }

    /// Concurrency 1 keeps the serial shape — proves the bound is the
    /// fan-out, not the work.
    #[test]
    fn pipeline_concurrency_one_is_serial() {
        let p = SpyPlacement::new(Duration::from_millis(40));
        let dir = pipe_dir("t1");
        let deps = mk_deps(&p, &dir, cfg(1));
        let ins = inputs(3, Carry::None);
        let t0 = Instant::now();
        run(&deps, &ins);
        assert_eq!(p.max_inflight.load(AOrd::SeqCst), 1);
        assert!(t0.elapsed() >= Duration::from_millis(120));
    }

    /// Carried leases go through `reaffirm` — zero acquire calls —
    /// and keep their carried epoch.
    #[test]
    fn carried_leases_reaffirm_not_acquire() {
        let p = SpyPlacement::new(Duration::ZERO);
        let dir = pipe_dir("t8");
        let deps = mk_deps(&p, &dir, cfg(8));
        let ins = inputs(4, Carry::Lease(PlacementEpoch(7)));
        let outcomes = run(&deps, &ins);
        assert_eq!(p.reaffirms.load(AOrd::SeqCst), 4);
        assert_eq!(p.acquires.load(AOrd::SeqCst), 0, "no acquire writes");
        for o in &outcomes {
            assert!(o.carried, "carried flag set");
            assert_eq!(o.acquired.as_ref().unwrap().epoch.0, 7, "epoch kept");
        }
    }

    /// A reaffirm that lands a DIFFERENT epoch (the fallback path)
    /// doesn't count as carried.
    #[test]
    fn reaffirm_epoch_drift_is_not_carried() {
        struct Drift;
        impl DoPlacement for Drift {
            fn authoritative(&self) -> bool {
                true
            }
            fn acquire(
                &self,
                _s: &PlacementScope,
            ) -> Result<PlacementLease, PlacementError> {
                unreachable!()
            }
            fn reaffirm(
                &self,
                _s: &PlacementScope,
                _e: PlacementEpoch,
            ) -> Result<PlacementLease, PlacementError> {
                Ok(PlacementLease {
                    epoch: PlacementEpoch(9), // record moved: +1 path
                    expires_at_ms: u64::MAX / 2,
                })
            }
            fn fence(
                &self,
                _s: &PlacementScope,
                _e: PlacementEpoch,
            ) -> Result<(), PlacementError> {
                Ok(())
            }
        }
        let p = Drift;
        let dir = std::env::temp_dir().join(format!("vrt-pipe-drift-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let deps = PipeDeps {
            tenant: "t",
            tenant_dir: &dir,
            placement: &p,
            snap: None,
            cfg: cfg(4),
            t0: Instant::now(),
        };
        let ins = inputs(2, Carry::Lease(PlacementEpoch(7)));
        let outcomes = run(&deps, &ins);
        assert!(outcomes.iter().all(|o| o.result.is_ok() && !o.carried));
        // MIG-17c: the drift is flagged so the consumer drains the
        // still-bound-old-epoch tail before `wal.start` replaces it.
        assert!(
            outcomes
                .iter()
                .all(|o| o.stale_tail_epoch == Some(7) && o.acquired.as_ref().unwrap().epoch.0 == 9),
            "carried=7 adopted=9 → stale tail flagged: {:?}",
            outcomes.iter().map(|o| o.stale_tail_epoch).collect::<Vec<_>>()
        );
    }

    /// Fail closed on a mid-set acquire failure: the partial set is
    /// released (every acquired lease still reachable via
    /// `outcome.acquired` → `release_all`), phase-2 claims never ran
    /// (WAL1), and per-class errors keep their typed shape.
    #[test]
    fn acquire_failure_keeps_partial_set_releasable_and_skips_claims() {
        let p = SpyPlacement::new(Duration::ZERO);
        p.fail_on("ck04", "fenced");
        p.fail_on("ck07", "store");
        let dir = pipe_dir("t8");
        let deps = mk_deps(&p, &dir, cfg(8));
        let ins = inputs(9, Carry::None);
        let outcomes = run(&deps, &ins);
        assert_eq!(p.claims.load(AOrd::SeqCst), 0, "WAL1: no claims on a failed launch");
        // r2 fail-fast: the two real failures keep their typed shape;
        // classes that hadn't passed the acquire gate when the abort
        // fired report `Aborted` — both are valid skip forms.
        // Whichever injected failure fired first aborts the other
        // before its own gate — so only ONE typed failure is
        // guaranteed to survive; that one owns `last_error`.
        assert!(
            outcomes.iter().any(|o| matches!(
                o.result,
                Err(PipeFail::Acquire(PlacementError::Fenced { .. }))
                    | Err(PipeFail::Acquire(PlacementError::Store(_)))
            )),
            "at least one injected failure kept its typed shape"
        );
        for o in outcomes
            .iter()
            .filter(|o| o.scope.class_key == "ck04" || o.scope.class_key == "ck07")
        {
            assert!(o.result.is_err(), "injected class never succeeds");
            assert!(o.acquired.is_none(), "injected class never holds a lease");
        }
        assert!(
            outcomes
                .iter()
                .all(|o| o.result.is_err() || o.acquired.is_some()),
            "no class may be both Ok and unreleased"
        );
        // The partial set: every lease taken before the abort fired
        // is releasable — however many that was.
        let mut rel: Vec<DoLeaseEntry> = Vec::new();
        for o in &outcomes {
            if let Some(lease) = &o.acquired {
                rel.push(DoLeaseEntry {
                    scope: o.scope.clone(),
                    epoch: lease.epoch,
                    expires_at_ms: lease.expires_at_ms,
                });
            }
        }
        // 0..=7 — if a failing class runs first it aborts every
        // sibling before acquire; if it runs last all seven slip in.
        assert!(rel.len() <= 7);
        release_all(&p, "t", &rel, 8);
        assert_eq!(
            p.fences.load(AOrd::SeqCst) as usize,
            rel.len(),
            "partial set fully fenced"
        );
    }

    /// A carried lease voided during the carry window must NOT
    /// launch — the consume fails at the acquire stage exactly like
    /// a fenced acquire.
    #[test]
    fn carry_lost_fails_closed_at_acquire_stage() {
        let p = SpyPlacement::new(Duration::ZERO);
        let dir = pipe_dir("t8");
        let deps = mk_deps(&p, &dir, cfg(8));
        let ins = inputs(3, Carry::Lost);
        let outcomes = run(&deps, &ins);
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o.result, Err(PipeFail::CarryLost))
                    && o.result.as_ref().err().unwrap().stage() == FailStage::Acquire
                    && o.acquired.is_none()),
            "every lost carry fails closed, nothing acquired"
        );
        assert_eq!(p.acquires.load(AOrd::SeqCst), 0);
        assert_eq!(p.reaffirms.load(AOrd::SeqCst), 0);
    }

    /// Whole-pipeline budget: once spent, unstarted ops refuse —
    /// every class fails `Budget` instead of pinning the hold open.
    #[test]
    fn total_budget_gates_unstarted_ops() {
        let p = SpyPlacement::new(Duration::ZERO);
        let dir = pipe_dir("tb");
        let deps = mk_deps(
            &p,
            &dir,
            PipeCfg {
                total_budget: Duration::ZERO,
                ..cfg(8)
            },
        );
        let ins = inputs(4, Carry::None);
        let outcomes = run(&deps, &ins);
        assert!(
            outcomes.iter().all(|o| matches!(
                o.result,
                Err(PipeFail::Budget("acquire")) | Err(PipeFail::Aborted("acquire"))
            )),
            "budget exhaustion fails every class closed (Budget or sibling-Aborted)"
        );
        assert!(
            outcomes
                .iter()
                .any(|o| matches!(o.result, Err(PipeFail::Budget(_)))),
            "at least the first gate reports the real Budget cause"
        );
        assert_eq!(p.acquires.load(AOrd::SeqCst), 0, "no op ran past budget");
    }

    /// Bounded-parallel release: 6 leases × 40 ms fences at K=6 ≈
    /// 40 ms wall — the same "partial set" release, parallel.
    #[test]
    fn release_all_is_bounded_parallel() {
        let p = SpyPlacement::new(Duration::from_millis(40));
        let rel: Vec<DoLeaseEntry> = (0..6)
            .map(|i| DoLeaseEntry {
                scope: scope("w", &format!("ck{i:02}")),
                epoch: PlacementEpoch(1),
                expires_at_ms: 0,
            })
            .collect();
        let t0 = Instant::now();
        release_all(&p, "t", &rel, 6);
        assert!(
            t0.elapsed() < Duration::from_millis(200),
            "parallel release ~1×40ms not 6×"
        );
        assert_eq!(p.fences.load(AOrd::SeqCst), 6);
    }

    /// End-to-end with the REAL manifest placement + memory store:
    /// proves `vrt_objstore::block_on_store` works inside the
    /// pipeline's scoped threads (the actor thread is a Tokio task,
    /// but the pipeline's workers are plain OS threads — the store
    /// future must drive on the shared objstore runtime) and that a
    /// same-holder carry reaffirms the epoch with no CAS churn.
    #[test]
    fn real_manifest_placement_carries_inside_scoped_threads() {
        let store = std::sync::Arc::new(vrt_objstore::MemoryStore::new());
        let placement =
            crate::do_placement_store::ManifestDoPlacement::new(store.clone(), "cell-a", 10_000, 1_000);
        let classes: Vec<String> = (0..4).map(|i| format!("cls{i}")).collect();
        // Pre-acquire every class — the old generation's held set.
        let inputs: Vec<PipeIn> = classes
            .iter()
            .map(|k| {
                let sc = scope("w", k);
                let l = placement.acquire(&sc).unwrap();
                PipeIn {
                    scope: sc,
                    carry: Carry::Lease(l.epoch),
                    transfer_src: None,
                }
            })
            .collect();
        let puts = store.put_calls();
        let dir = std::env::temp_dir().join("pipe-real-manifest");
        let deps = PipeDeps {
            tenant: "t",
            tenant_dir: &dir,
            placement: &placement,
            snap: None,
            cfg: PipeCfg {
                concurrency: 4,
                op_timeout: Duration::from_secs(30),
                total_budget: Duration::from_secs(60),
                restore_timeout: Duration::from_secs(120),
            },
            t0: Instant::now(),
        };
        let outcomes = run(&deps, &inputs);
        assert!(outcomes.iter().all(|o| o.result.is_ok()));
        assert!(outcomes.iter().all(|o| o.carried));
        for (o, inp) in outcomes.iter().zip(&inputs) {
            let Carry::Lease(e) = inp.carry else {
                unreachable!()
            };
            assert_eq!(o.acquired.as_ref().unwrap().epoch, e, "epoch kept");
        }
        // Exactly ONE write per class — the writer-epoch claim. A
        // reaffirm outside the renew window costs zero CAS (the
        // serial path paid fence + re-acquire = two).
        assert_eq!(
            store.put_calls(),
            puts + inputs.len(),
            "carry adds zero CAS on top of the per-class claim"
        );
    }

    // ==================== MIG-17-r2 adversarial-review tests ====================

    fn wal_cfg() -> crate::do_wal::WalCfg {
        crate::do_wal::WalCfg {
            enabled: true,
            tick_ms: 0,
            max_dbs: 8,
            max_pending_bytes: 256 << 20,
            max_segment_bytes: 1 << 20,
            upload_attempts: 2,
            tick_upload_budget_ms: 500,
            idle_stop_ms: u64::MAX,
            flush_max_ms: 5_000,
            gc_max_per_tick: 64,
            replay_max_bytes: 1 << 30,
            rebaseline_chain_bytes: 1 << 29,
            baseline_backoff_max_ms: 300_000,
        }
    }

    fn mk_db(dir: &std::path::Path, stem: &str) -> rusqlite::Connection {
        let c = rusqlite::Connection::open(dir.join(format!("{stem}.sqlite"))).unwrap();
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .unwrap();
        c.execute_batch("CREATE TABLE IF NOT EXISTS t(k TEXT PRIMARY KEY, v TEXT)")
            .unwrap();
        c
    }
    fn put(c: &rusqlite::Connection, k: &str, v: &str) {
        c.execute("INSERT OR REPLACE INTO t VALUES (?1,?2)", [k, v])
            .unwrap();
    }
    fn get_val(db: &std::path::Path, k: &str) -> Option<String> {
        let c = rusqlite::Connection::open(db).unwrap();
        c.query_row("SELECT v FROM t WHERE k=?1", [k], |r| r.get(0))
            .ok()
    }

    /// A real `ManifestDoPlacement` + `MemoryStore` + committed
    /// snapshot (seq 42, baseline row 'a') + ONE post-baseline WAL
    /// segment chain (row 'b') under `{E}/42` — the exact shape a
    /// serialized switch must restore + replay.
    struct B1Fx {
        tenant_dir: std::path::PathBuf,
        scope: PlacementScope,
        epoch: PlacementEpoch,
        store: std::sync::Arc<vrt_objstore::MemoryStore>,
        dyn_store: std::sync::Arc<dyn ObjectStore>,
        placement: crate::do_placement_store::ManifestDoPlacement,
        wal: crate::do_wal::DoWalReplicator,
    }

    fn b1_fixture(tag: &str) -> B1Fx {
        b1_fixture_mode(tag, false)
    }

    /// `keep_live`: the dir stays LIVE — writer conn leaked so
    /// `obj1.sqlite-wal` persists, one post-baseline commit ('b')
    /// plus one never-uploaded live commit ('live') sit in it.
    fn b1_fixture_mode(tag: &str, keep_live: bool) -> B1Fx {
        let dir = pipe_dir(tag);
        let tenant_dir = dir.join("do-t");
        let ck = "f".repeat(64);
        let class_dir = tenant_dir.join("w").join(&ck);
        std::fs::create_dir_all(&class_dir).unwrap();
        let conn = mk_db(&class_dir, "obj1");
        put(&conn, "a", "1");
        let store = std::sync::Arc::new(vrt_objstore::MemoryStore::new());
        let dyn_store: std::sync::Arc<dyn ObjectStore> = store.clone();
        let placement = crate::do_placement_store::ManifestDoPlacement::new(
            dyn_store.clone(),
            "cell-a",
            60_000,
            1_000,
        );
        let wal = crate::do_wal::DoWalReplicator::new(
            wal_cfg(),
            dyn_store.clone(),
            dir.join("wtmp"),
            std::sync::Arc::new(|| 1_000_000u64),
        );
        let scope = PlacementScope {
            tenant: "t".into(),
            worker: "w".into(),
            class_key: ck.clone(),
            class_dir: class_dir.clone(),
        };
        let epoch = placement.acquire(&scope).unwrap().epoch;
        wal.start(&scope, epoch.0, 0);
        // First tick: the tail discovers obj1 under the PRE-begin
        // label (0) — pins + anchors + re-parses 'a'. Matching the
        // live sequence (and the do_wal tests): `on_snapshot_begin`
        // seals the label a tail was DISCOVERED under — a tail first
        // seen inside begin(42) would seal 42 itself and can_emit
        // could never pass.
        wal.tick(&ck);
        wal.on_snapshot_begin(&ck, 42);
        crate::do_snapshot::upload_snapshot(
            &dyn_store,
            &placement,
            &scope,
            epoch,
            42,
            &dir.join("snap-tmp"),
            crate::do_placement_store::SnapKind::Periodic,
            Some(&|| wal.on_snapshot_image_taken(&ck, 42)),
        )
        .unwrap();
        wal.on_snapshot_committed(&ck, 42);
        put(&conn, "b", "2");
        wal.tick(&ck);
        wal.tick(&ck);
        placement.claim_writer(&scope, epoch).unwrap();
        if keep_live {
            // A live commit the chain NEVER saw — replaying over this
            // dir would fold it away (the B1a regression shape).
            put(&conn, "live", "3");
            std::mem::forget(conn); // keep -wal/-shm alive — dir stays Live
        } else {
            conn.close().ok();
            // The "new cell" starts empty — the pipeline restores the
            // baseline and must replay the committed tail on top of it.
            std::fs::remove_dir_all(&class_dir).unwrap();
        }
        B1Fx {
            tenant_dir,
            scope,
            epoch,
            store,
            dyn_store,
            placement,
            wal,
        }
    }

    fn b1_deps(fx: &B1Fx, cfg: PipeCfg) -> PipeDeps<'_> {
        PipeDeps {
            tenant: "t",
            tenant_dir: &fx.tenant_dir,
            placement: &fx.placement,
            snap: Some(SnapView {
                store: &fx.dyn_store,
                placement: &fx.placement,
                wal: Some(&fx.wal),
            }),
            cfg,
            t0: Instant::now(),
        }
    }

    /// B1: a mid-unit replay failure must NOT strand a restored dir
    /// that suppresses the retry's replay. Attempt 1 fails the replay
    /// (injected list error); the `wal-replay-pending` marker survives
    /// beside the Intact baseline; attempt 2 re-runs the replay even
    /// though `restore_snapshot` returns Ok(false) — the post-baseline
    /// committed write lands.
    #[test]
    fn mig17r2_replay_failure_marker_forces_replay_on_retry() {
        let fx = b1_fixture("b1-fail");
        let ins = vec![PipeIn {
            scope: fx.scope.clone(),
            carry: Carry::Lease(fx.epoch),
            transfer_src: None,
        }];
        // Fail the replay's first store op (its chain listing).
        fx.store.inject_list_failures(vec![vrt_objstore::StoreError::unavailable(
            true,
            "injected",
        )]);
        let out1 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(
            matches!(out1[0].result, Err(PipeFail::Restore(_))),
            "attempt 1 fails at the replay: {:?}",
            out1[0].result
        );
        assert!(
            crate::do_snapshot::sibling_replay_pending_file(&fx.scope).exists(),
            "pending marker survives the failed unit"
        );
        let db = fx.scope.class_dir.join("obj1.sqlite");
        assert_eq!(get_val(&db, "a").as_deref(), Some("1"), "baseline restored");
        assert_eq!(get_val(&db, "b"), None, "the bug's silent-loss state");

        // Retry: dir verifies Intact — the PRE-r2 shape skipped the
        // replay here. The marker forces it back in.
        let out2 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(out2[0].result.is_ok(), "retry: {:?}", out2[0].result);
        assert!(
            !crate::do_snapshot::sibling_replay_pending_file(&fx.scope).exists(),
            "marker cleared after the successful replay"
        );
        assert_eq!(
            get_val(&db, "b").as_deref(),
            Some("2"),
            "committed tail applied on the retry — no silent loss"
        );
    }

    /// B1: the restore unit is judged only AS A WHOLE — even a
    /// zero-width `restore_timeout` verdict lands AFTER
    /// restore+mark+replay completed (marker already cleared), and
    /// the retry converges immediately (Intact, no marker).
    #[test]
    fn mig17r2_restore_unit_bound_judged_only_after_completion() {
        let fx = b1_fixture("b1-slow");
        let ins = vec![PipeIn {
            scope: fx.scope.clone(),
            carry: Carry::Lease(fx.epoch),
            transfer_src: None,
        }];
        let slow = PipeCfg {
            restore_timeout: Duration::ZERO,
            ..cfg(4)
        };
        let out1 = run(&b1_deps(&fx, slow), &ins);
        assert!(
            matches!(out1[0].result, Err(PipeFail::Budget("restore_read"))),
            "post-unit verdict: {:?}",
            out1[0].result
        );
        assert!(
            !crate::do_snapshot::sibling_replay_pending_file(&fx.scope).exists(),
            "replay completed before the verdict — marker cleared"
        );
        let db = fx.scope.class_dir.join("obj1.sqlite");
        assert_eq!(
            get_val(&db, "b").as_deref(),
            Some("2"),
            "the unit fully completed before its bound was judged"
        );
        // A normal retry skips restore AND replay — the dir is Intact
        // and provably replayed.
        let out2 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(out2[0].result.is_ok(), "converged: {:?}", out2[0].result);
    }

    /// MIG-17c: a segment that decodes + applies but leaves a corrupt
    /// image is a HARD rejection (`stems_rejected`), never a skip —
    /// the stem was selected for the current epoch's chain, so its
    /// frames are committed state the baseline alone would drop.
    /// Restore fails closed, the pending marker survives, and after
    /// the segment is repaired the retry adopts complete data.
    #[test]
    fn mig17c_rejected_stem_fails_closed_until_repaired() {
        let fx = b1_fixture("17c-rej");
        let ins = vec![PipeIn {
            scope: fx.scope.clone(),
            carry: Carry::Lease(fx.epoch),
            transfer_src: None,
        }];
        // Corrupt the 'b' chain segment: mangle every frame's page
        // image then RE-encode — the seg-level sha256 passes, the
        // post-apply integrity_check is what rejects it.
        // The 'b' chain lives under seq label 42 (0x2a) — a seq-0
        // pre-anchor seg would be a legitimate POLICY skip, not a
        // rejection.
        let seg_key = fx
            .store
            .keys()
            .into_iter()
            .find(|k| k.contains("/wal/obj1/") && k.contains("/000000000000002a/") && k.ends_with(".seg"))
            .expect("fixture emitted one seg");
        let orig = vrt_objstore::block_on_store(fx.store.get(&seg_key))
            .unwrap()
            .unwrap()
            .body;
        let mut seg = vrt_sql::seg::decode(&orig).unwrap();
        assert!(!seg.frames.is_empty());
        for f in &mut seg.frames {
            for b in f.data.iter_mut().take(64) {
                *b = 0xAA;
            }
        }
        fx.store.corrupt(&seg_key, vrt_sql::seg::encode(&seg));

        let out1 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(
            matches!(out1[0].result, Err(PipeFail::Restore(_))),
            "rejected stem ⇒ restore failure, not a clean restore: {:?}",
            out1[0].result
        );
        assert!(
            crate::do_snapshot::sibling_replay_pending_file(&fx.scope).exists(),
            "marker kept — a retry must re-run replay after repair"
        );

        // Repair the segment (restore the original bytes) and retry:
        // complete data, marker cleared.
        fx.store.corrupt(&seg_key, orig);
        let out2 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(out2[0].result.is_ok(), "retry: {:?}", out2[0].result);
        assert!(
            !crate::do_snapshot::sibling_replay_pending_file(&fx.scope).exists(),
            "marker cleared on the successful retry"
        );
        let db = fx.scope.class_dir.join("obj1.sqlite");
        assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
        assert_eq!(
            get_val(&db, "b").as_deref(),
            Some("2"),
            "repaired segment adopted — complete data, nothing skipped"
        );
    }

    /// MIG-17-r3 (B1a): a residual marker that happens to MATCH the
    /// current baseline+writer context on a LIVE dir is stale by
    /// definition — replaying genesis/baseline segments would fold
    /// the live `-wal` away and regress commits. The pipeline must
    /// drop it, clear the marker, and never replay. Then a MISMATCHED
    /// marker (older writer epoch, e.g. a departed same-name class's
    /// leftover) is cleared without replay either.
    #[test]
    fn mig17r3_residual_marker_on_live_dir_never_replays() {
        let fx = b1_fixture_mode("b1-live", true);
        let want = crate::do_snapshot::ReplayPending {
            snap_epoch: fx.epoch.0,
            snap_seq: 42,
            writer_epoch: fx.epoch.0,
        };
        crate::do_snapshot::mark_replay_pending(&fx.scope, want).unwrap();
        let marker = crate::do_snapshot::sibling_replay_pending_file(&fx.scope);
        assert!(marker.exists(), "residual marker planted");
        assert!(
            crate::do_snapshot::has_live_wal_frames(&fx.scope.class_dir),
            "fixture must be live — -wal present"
        );
        let ins = vec![PipeIn {
            scope: fx.scope.clone(),
            carry: Carry::Lease(fx.epoch),
            transfer_src: None,
        }];
        let out = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(out[0].result.is_ok(), "live dir: {:?}", out[0].result);
        let db = fx.scope.class_dir.join("obj1.sqlite");
        assert_eq!(get_val(&db, "b").as_deref(), Some("2"), "chain commit kept");
        assert_eq!(
            get_val(&db, "live").as_deref(),
            Some("3"),
            "unpublished live commit survived — no replay folded it away"
        );
        assert!(!marker.exists(), "stale marker cleared");

        // Mismatched ctx (same key, older writer epoch — e.g. a
        // departed same-name class): also cleared, still no replay.
        crate::do_snapshot::mark_replay_pending(
            &fx.scope,
            crate::do_snapshot::ReplayPending {
                snap_epoch: fx.epoch.0,
                snap_seq: 42,
                writer_epoch: fx.epoch.0 - 1,
            },
        )
        .unwrap();
        let out2 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(out2[0].result.is_ok(), "retry: {:?}", out2[0].result);
        assert_eq!(get_val(&db, "live").as_deref(), Some("3"));
        assert!(!marker.exists(), "mismatched marker cleared — no extra replay");
    }

    /// MIG-17-r3 (B1b): a marker that cannot be durably written is a
    /// typed fail-closed error — the restore must NOT proceed into a
    /// product dir. The marker path pre-created as a DIRECTORY makes
    /// `atomic_write`'s rename fail (file → dir), exercising exactly
    /// the write-failure arm inside `restore_snapshot`'s `pre_place`.
    #[test]
    fn mig17r3_marker_write_failure_fails_closed() {
        let fx = b1_fixture("b1-markfail");
        let marker = crate::do_snapshot::sibling_replay_pending_file(&fx.scope);
        std::fs::create_dir(&marker).unwrap(); // rename-over-dir → io error
        let ins = vec![PipeIn {
            scope: fx.scope.clone(),
            carry: Carry::Lease(fx.epoch),
            transfer_src: None,
        }];
        let out = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(
            matches!(out[0].result, Err(PipeFail::Restore(_))),
            "marker write failure = typed retryable failure: {:?}",
            out[0].result
        );
        assert!(
            !fx.scope.class_dir.join("obj1.sqlite").exists(),
            "fail closed: no restore product without a durable marker"
        );
        // Retry after the fs fault heals converges normally.
        std::fs::remove_dir(&marker).unwrap();
        let out2 = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(out2[0].result.is_ok(), "retry converges: {:?}", out2[0].result);
        let db = fx.scope.class_dir.join("obj1.sqlite");
        assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
    }

    /// Store wrapper: forwards to `MemoryStore`; `get` panics once the
    /// per-key call count reaches `panic_at` — used to fault the
    /// record read AFTER acquire/reaffirm (B2 panic-leak path).
    struct PanicStore {
        inner: vrt_objstore::MemoryStore,
        panic_key: String,
        panic_at: usize,
        gets: Mutex<std::collections::BTreeMap<String, usize>>,
    }

    impl PanicStore {
        fn new(panic_key: String, panic_at: usize) -> Self {
            Self {
                inner: vrt_objstore::MemoryStore::new(),
                panic_key,
                panic_at,
                gets: Mutex::new(Default::default()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for PanicStore {
        async fn get(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
            let n = {
                let mut g = self.gets.lock().unwrap();
                let e = g.entry(k.to_string()).or_default();
                *e += 1;
                *e
            };
            if k == self.panic_key && n == self.panic_at {
                panic!("injected panic on get({k})");
            }
            self.inner.get(k).await
        }
        async fn head(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
            self.inner.head(k).await
        }
        async fn put_if_none_match(
            &self,
            k: &str,
            b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put_if_none_match(k, b).await
        }
        async fn put_if_match(
            &self,
            k: &str,
            e: &str,
            b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put_if_match(k, e, b).await
        }
        async fn put(&self, k: &str, b: &[u8]) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put(k, b).await
        }
        async fn list(&self, p: &str) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            self.inner.list(p).await
        }
        async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
            self.inner.delete(k).await
        }
    }

    /// B2: a panic AFTER the lease was taken (here: the phase-1 record
    /// read) must still surface the lease for the release set — for a
    /// plain acquire and a carried reaffirm alike. Proven against the
    /// REAL manifest placement: after `release_all`, the record is a
    /// dead marker (holder empty, expiry zeroed).
    #[test]
    fn mig17r2_panic_after_acquire_still_releases_the_lease() {
        for carry in [Carry::None, Carry::Lost] {
            let _ = carry; // (carried variant covered below)
        }
        let dir = pipe_dir("b2-acq");
        let tenant_dir = dir.join("do-t");
        let ck = "a".repeat(64);
        // Panic on the SECOND placement-record read: get#1 = acquire,
        // get#2 = phase-1 record read.
        let store = std::sync::Arc::new(PanicStore::new(
            crate::do_placement_store::placement_key(&ck),
            2,
        ));
        let dyn_store: std::sync::Arc<dyn ObjectStore> = store.clone();
        let placement = crate::do_placement_store::ManifestDoPlacement::new(
            dyn_store.clone(),
            "cell-a",
            60_000,
            1_000,
        );
        let sc = PlacementScope {
            tenant: "t".into(),
            worker: "w".into(),
            class_key: ck.clone(),
            class_dir: tenant_dir.join("w").join(&ck),
        };
        std::fs::create_dir_all(&sc.class_dir).unwrap();
        let deps = PipeDeps {
            tenant: "t",
            tenant_dir: &tenant_dir,
            placement: &placement,
            snap: Some(SnapView {
                store: &dyn_store,
                placement: &placement,
                wal: None,
            }),
            cfg: cfg(4),
            t0: Instant::now(),
        };
        let ins = vec![PipeIn {
            scope: sc.clone(),
            carry: Carry::None,
            transfer_src: None,
        }];
        let outcomes = run(&deps, &ins);
        assert!(
            outcomes[0].result.is_err(),
            "panic surfaces as a typed failure"
        );
        let lease = outcomes[0]
            .acquired
            .clone()
            .expect("B2: the panicked phase still surfaces its lease");
        release_all(
            &placement,
            "t",
            &[DoLeaseEntry {
                scope: outcomes[0].scope.clone(),
                epoch: lease.epoch,
                expires_at_ms: lease.expires_at_ms,
            }],
            4,
        );
        let rec = placement.record(&sc).unwrap().unwrap();
        assert!(
            rec.holder.is_empty() && rec.expires_at_ms == 0,
            "lease fenced — not left live past a panicked attempt"
        );
    }

    /// B2: same guarantee on the CARRIED path — a panic after
    /// reaffirm must not strand the carried lease (it was already
    /// `take`n out of `carried_map` — no list owned it anymore).
    #[test]
    fn mig17r2_panic_after_reaffirm_still_releases_carried_lease() {
        let dir = pipe_dir("b2-carry");
        let tenant_dir = dir.join("do-t");
        let ck = "b".repeat(64);
        // get#1 = the fixture's old-gen acquire, get#2 = reaffirm,
        // get#3 = the phase-1 record read (the panic lands AFTER the
        // carried lease was reaffirmed into the slot).
        let store = std::sync::Arc::new(PanicStore::new(
            crate::do_placement_store::placement_key(&ck),
            3,
        ));
        let dyn_store: std::sync::Arc<dyn ObjectStore> = store.clone();
        let placement = crate::do_placement_store::ManifestDoPlacement::new(
            dyn_store.clone(),
            "cell-a",
            60_000,
            1_000,
        );
        let sc = PlacementScope {
            tenant: "t".into(),
            worker: "w".into(),
            class_key: ck.clone(),
            class_dir: tenant_dir.join("w").join(&ck),
        };
        std::fs::create_dir_all(&sc.class_dir).unwrap();
        let epoch = placement.acquire(&sc).unwrap().epoch; // old gen's lease
        let deps = PipeDeps {
            tenant: "t",
            tenant_dir: &tenant_dir,
            placement: &placement,
            snap: Some(SnapView {
                store: &dyn_store,
                placement: &placement,
                wal: None,
            }),
            cfg: cfg(4),
            t0: Instant::now(),
        };
        let ins = vec![PipeIn {
            scope: sc.clone(),
            carry: Carry::Lease(epoch),
            transfer_src: None,
        }];
        let outcomes = run(&deps, &ins);
        assert!(outcomes[0].result.is_err());
        let lease = outcomes[0]
            .acquired
            .clone()
            .expect("B2: carried lease recovered from the panic slot");
        assert_eq!(lease.epoch, epoch, "carried epoch surfaced intact");
        assert!(outcomes[0].carried, "carried flag recomputed on panic");
        release_all(
            &placement,
            "t",
            &[DoLeaseEntry {
                scope: outcomes[0].scope.clone(),
                epoch: lease.epoch,
                expires_at_ms: lease.expires_at_ms,
            }],
            4,
        );
        let rec = placement.record(&sc).unwrap().unwrap();
        assert!(rec.holder.is_empty() && rec.expires_at_ms == 0);
    }

    /// H3: once one class fails, siblings skip every further op —
    /// proven by counting placement-record GETs on the real store: an
    /// aborted class's acquire GET ran (lease taken → released) but
    /// its record-read GET never did. Uses a gating placement that
    /// fails ck00 instantly while ck01+ sleep inside acquire — the
    /// abort flag is necessarily set before they reach record_read.
    #[test]
    fn mig17r2_fail_fast_siblings_never_start_the_record_read() {
        /// Delegating placement: per-key acquire delay + failure —
        /// everything else forwards to the real manifest impl.
        struct GatePl {
            inner: crate::do_placement_store::ManifestDoPlacement,
            fail_key: String,
            sleep: Duration,
        }
        impl DoPlacement for GatePl {
            fn authoritative(&self) -> bool {
                true
            }
            fn acquire(
                &self,
                s: &PlacementScope,
            ) -> Result<PlacementLease, PlacementError> {
                if s.class_key == self.fail_key {
                    return Err(PlacementError::Store(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "store down",
                    )));
                }
                std::thread::sleep(self.sleep);
                self.inner.acquire(s)
            }
            fn reaffirm(
                &self,
                s: &PlacementScope,
                e: PlacementEpoch,
            ) -> Result<PlacementLease, PlacementError> {
                self.inner.reaffirm(s, e)
            }
            fn renew(
                &self,
                s: &PlacementScope,
                e: PlacementEpoch,
            ) -> Result<u64, PlacementError> {
                self.inner.renew(s, e)
            }
            fn fence(
                &self,
                s: &PlacementScope,
                e: PlacementEpoch,
            ) -> Result<(), PlacementError> {
                self.inner.fence(s, e)
            }
            fn claim_writer(
                &self,
                s: &PlacementScope,
                e: PlacementEpoch,
            ) -> Result<(), PlacementError> {
                self.inner.claim_writer(s, e)
            }
        }

        /// Counting store — placement-record GETs per key.
        struct CountStore {
            inner: vrt_objstore::MemoryStore,
            gets: Mutex<std::collections::BTreeMap<String, usize>>,
        }
        #[async_trait::async_trait]
        impl ObjectStore for CountStore {
            async fn get(
                &self,
                k: &str,
            ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
                *self
                    .gets
                    .lock()
                    .unwrap()
                    .entry(k.to_string())
                    .or_default() += 1;
                self.inner.get(k).await
            }
            async fn head(
                &self,
                k: &str,
            ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
                self.inner.head(k).await
            }
            async fn put_if_none_match(
                &self,
                k: &str,
                b: &[u8],
            ) -> Result<String, vrt_objstore::StoreError> {
                self.inner.put_if_none_match(k, b).await
            }
            async fn put_if_match(
                &self,
                k: &str,
                e: &str,
                b: &[u8],
            ) -> Result<String, vrt_objstore::StoreError> {
                self.inner.put_if_match(k, e, b).await
            }
            async fn put(&self, k: &str, b: &[u8]) -> Result<String, vrt_objstore::StoreError> {
                self.inner.put(k, b).await
            }
            async fn list(
                &self,
                p: &str,
            ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
                self.inner.list(p).await
            }
            async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
                self.inner.delete(k).await
            }
        }

        let store = std::sync::Arc::new(CountStore {
            inner: vrt_objstore::MemoryStore::new(),
            gets: Mutex::new(Default::default()),
        });
        let dyn_store: std::sync::Arc<dyn ObjectStore> = store.clone();
        let manifest = crate::do_placement_store::ManifestDoPlacement::new(
            dyn_store.clone(),
            "cell-a",
            60_000,
            1_000,
        );
        let dir = pipe_dir("h3-ff");
        let tenant_dir = dir.join("do-t");
        let fail_ck = "0".repeat(64);
        let gate = GatePl {
            inner: crate::do_placement_store::ManifestDoPlacement::new(
                dyn_store.clone(),
                "cell-a",
                60_000,
                1_000,
            ),
            fail_key: fail_ck.clone(),
            sleep: Duration::from_millis(150),
        };
        let mut ins = vec![PipeIn {
            scope: PlacementScope {
                tenant: "t".into(),
                worker: "w".into(),
                class_key: fail_ck.clone(),
                class_dir: tenant_dir.join("w").join(&fail_ck),
            },
            carry: Carry::None,
            transfer_src: None,
        }];
        let mut others = Vec::new();
        for i in 1..=3 {
            let ck = format!("{:0>64}", i);
            let sc = PlacementScope {
                tenant: "t".into(),
                worker: "w".into(),
                class_key: ck.clone(),
                class_dir: tenant_dir.join("w").join(&ck),
            };
            std::fs::create_dir_all(&sc.class_dir).unwrap();
            others.push(ck);
            ins.push(PipeIn {
                scope: sc,
                carry: Carry::None,
                transfer_src: None,
            });
        }
        let deps = PipeDeps {
            tenant: "t",
            tenant_dir: &tenant_dir,
            placement: &gate,
            snap: Some(SnapView {
                store: &dyn_store,
                placement: &manifest,
                wal: None,
            }),
            cfg: cfg(4),
            t0: Instant::now(),
        };
        let outcomes = run(&deps, &ins);
        assert!(
            matches!(
                outcomes[0].result,
                Err(PipeFail::Acquire(PlacementError::Store(_)))
            ),
            "the failing class keeps its typed error: {:?}",
            outcomes[0].result
        );
        for (o, ck) in outcomes[1..].iter().zip(&others) {
            assert!(
                matches!(o.result, Err(PipeFail::Aborted(_))),
                "{ck}: skipped with typed failure, got {:?}",
                o.result
            );
            // If a lease was taken before the abort fired it must
            // surface for the unified release; either way the
            // record_read GET (placement-key get #2) never ran.
            let gets = store
                .gets
                .lock()
                .unwrap()
                .get(&crate::do_placement_store::placement_key(ck))
                .copied()
                .unwrap_or(0);
            assert!(
                gets <= 1,
                "{ck}: record_read GET skipped (gets={gets}, acquired={})",
                o.acquired.is_some()
            );
        }
    }

    /// Carried + plain classes mixed when one fails: the carried
    /// lease still lands in the release set (reaffirmed, then fenced
    /// with the rest) — never left held by a dead attempt.
    #[test]
    fn mig17r2_mixed_carry_and_failure_releases_carried_lease() {
        let p = SpyPlacement::new(Duration::ZERO);
        p.fail_on("ck02", "fenced");
        let dir = pipe_dir("h3-mix");
        let deps = mk_deps(&p, &dir, cfg(4));
        let mut ins = inputs(4, Carry::None);
        ins[0].carry = Carry::Lease(PlacementEpoch(7));
        let outcomes = run(&deps, &ins);
        let rel: Vec<DoLeaseEntry> = outcomes
            .iter()
            .filter_map(|o| {
                o.acquired.as_ref().map(|l| DoLeaseEntry {
                    scope: o.scope.clone(),
                    epoch: l.epoch,
                    expires_at_ms: l.expires_at_ms,
                })
            })
            .collect();
        assert_eq!(p.reaffirms.load(AOrd::SeqCst), 1);
        release_all(&p, "t", &rel, 4);
        // Carried lease + the other two successful acquires fenced.
        assert!(rel.iter().any(|l| l.epoch.0 == 7));
        assert_eq!(p.fences.load(AOrd::SeqCst), rel.len());
    }

    /// Transfer handoff: a class whose key moved to a different
    /// worker gets its source dir moved into the destination under
    /// the fresh lease — before any restore work.
    #[test]
    fn mig17r2_transfer_handoff_moves_source_dir() {
        let p = SpyPlacement::new(Duration::ZERO);
        let dir = pipe_dir("xfer");
        let tenant_dir = dir.join("do-t");
        let ck = "c".repeat(64);
        // Source cell dir under the OLD worker — the handoff moves it.
        let src_dir = tenant_dir.join("w-old").join(&ck);
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("obj1.sqlite"), b"live-data").unwrap();
        let deps = PipeDeps {
            tenant: "t",
            tenant_dir: &tenant_dir,
            placement: &p,
            snap: None,
            cfg: cfg(4),
            t0: Instant::now(),
        };
        let ins = vec![PipeIn {
            scope: PlacementScope {
                tenant: "t".into(),
                worker: "w-new".into(),
                class_key: ck.clone(),
                class_dir: tenant_dir.join("w-new").join(&ck),
            },
            carry: Carry::None,
            transfer_src: Some("w-old".into()),
        }];
        let outcomes = run(&deps, &ins);
        assert!(outcomes[0].result.is_ok(), "{:?}", outcomes[0].result);
        assert_eq!(
            std::fs::read(outcomes[0].scope.class_dir.join("obj1.sqlite")).unwrap(),
            b"live-data",
            "source dir moved into the destination segment"
        );
        assert!(
            !src_dir.join("obj1.sqlite").exists(),
            "source vacated"
        );
    }

    /// Phase-2 seq-label read with a REAL committed snapshot: the
    /// outcome carries the committed seq for `wal.start`.
    #[test]
    fn mig17r2_phase2_seq_label_from_committed_snapshot() {
        let fx = b1_fixture("p2-label");
        let ins = vec![PipeIn {
            scope: fx.scope.clone(),
            carry: Carry::Lease(fx.epoch),
            transfer_src: None,
        }];
        let outcomes = run(&b1_deps(&fx, cfg(4)), &ins);
        assert!(outcomes[0].result.is_ok());
        assert_eq!(
            outcomes[0].result.as_ref().unwrap().seq_label,
            42,
            "wal.start gets the committed snapshot seq"
        );
        // And the baseline+tail both landed on disk.
        let db = fx.scope.class_dir.join("obj1.sqlite");
        assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
    }
}
