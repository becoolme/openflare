//! WAL1 — §6.11 continuous WAL replication for Durable Object sqlite
//! storage. Shrinks crash-loss RPO from the snapshot interval to
//! seconds by streaming WAL frames to object storage while the
//! writer is active.
//!
//! Mechanism (PoC-verified on real workerd, `tests/do_wal_poc.rs`):
//! an external reader holds a read transaction on each object db —
//! checkpoint cannot recycle WAL past the reader's mark (a mark-0
//! pin is the STRONGEST: it blocks everything) — parses frames out
//! of the `-wal` file, verifies the cumulative checksum chain, cuts
//! segments at commit boundaries, and uploads with
//! `put_if_none_match`.
//!
//! Consistency invariants (WAL1-r2, §6.11):
//!
//! - **I1** a segment chain `{epoch E}/{seq S}` is adoptable ONLY on
//!   top of a baseline E committed itself (`snapshot.epoch ==
//!   writer_epoch`). A writer never emits adoptable segments before
//!   its own baseline's anchor (`anchor_written`); a break
//!   a genesis stream whose WAL provably never checkpointed
//!   (`ckpt_seq == 0` at first anchor).
//! - **I2** the segment index space must cover the baseline's image
//!   boundary continuously. The image provably covers every commit
//!   that happened BEFORE the backup began, so `on_snapshot_begin`
//!   flushes pending frames under the OLD label, records the
//!   boundary `B = emitted`, and writes a per-db `anchor` object —
//!   the chain under the new label then starts exactly at `B`.
//! - **I3** replay applies only frames with logical index `>= B`,
//!   contiguously, in order — result = the committed state at the
//!   end of the chain. A chain that cannot contain `B` (missing
//!   anchor, hole before/at `B`, epoch mismatch) is never applied;
//!   the baseline image alone is served.
//!
//! Any observed discontinuity (salt change, file shrink, a complete
//! frame whose checksum fails — i.e. a same-salt rewrite, a pending
//! cap breach, a segment-content conflict) marks the db
//! `needs_baseline`: because a held pin makes resets physically
//! impossible, ANY observed reset means frames were lost while
//! un-pinned and the loss extent is unverifiable — the ONLY safe
//! recovery is a fresh baseline image (re-arm inside the next
//! `on_snapshot_begin`, before the boundary is recorded).
//!
//! Recovery: replay ONLY segments of `record.writer_epoch` under the
//! restored baseline's own seq/epoch, in strictly consecutive `lsn`
//! order with continuous `wal_begin`; the first gap/corrupt segment
//! stops the chain and frames apply only through the last commit.
//! `Final` snapshots never adopt a chain (the writer was already
//! dead — the image is the complete truth).
//!
//! Off-by-default: `None` in `DoSnapCtx` disables every hook — the
//! snapshot-only path is byte-identical to pre-WAL1.

use crate::do_placement::PlacementScope;
use crate::do_placement_store::SnapRef;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use vrt_objstore::{ObjectStore, StoreError};
use vrt_sql::seg::{self, SegFrame, Segment};
use vrt_sql::wal::{self, WalPin};

// ---------- config ----------

/// Resolved (clamped) replication bounds — see `[doWal]` in config.
#[derive(Debug, Clone)]
pub struct WalCfg {
    /// Master switch. `false` → no pinning/tailing/upload/replay.
    pub enabled: bool,
    /// Minimum interval between tail passes per class (ms).
    pub tick_ms: u64,
    /// Max concurrently-tailed dbs per cell.
    pub max_dbs: usize,
    /// Bound on un-uploaded WAL bytes per db — BOTH the parsed
    /// pending+queue region AND the pinned-but-unparsed span
    /// (`wal_len - mark_pos`). Breach → drop pending, release pin,
    /// needs_baseline.
    pub max_pending_bytes: u64,
    /// Target segment cut size; cut happens at last commit ≤ cap,
    /// mid-txn only when a single txn exceeds 4× cap.
    pub max_segment_bytes: u64,
    /// Per-segment upload attempts (linear backoff 50ms×n).
    pub upload_attempts: u32,
    /// Per-tick wall budget for segment uploads — the tail shares
    /// the supervisor actor thread with lease renewals; leftover
    /// queue drains on subsequent ticks.
    pub tick_upload_budget_ms: u64,
    /// Idle db stop-tailing window (pin released; resumes on growth).
    pub idle_stop_ms: u64,
    /// Wall budget for drain flush before Final snapshot.
    pub flush_max_ms: u64,
    /// Segment GC deletes per call.
    pub gc_max_per_tick: usize,
    /// Total replay bytes bound across all stems of a class.
    pub replay_max_bytes: u64,
    /// MIG-17d(-r2): proactive rebaseline trigger — when the SUM of
    /// every stem's uploaded frame bytes under current labels
    /// reaches this, the tail asks for a fresh baseline so the
    /// class's combined chains can never outgrow `replay_max_bytes`
    /// (restore charges `total_bytes` across ALL stems). Resolved
    /// default = `replay_max_bytes / 2`; the
    /// `replay - threshold >= max_pending_bytes` invariant is a
    /// HEURISTIC lower bound: EVERY db may add a full pending cap
    /// inside one snapshot window, so the true safe sustained write
    /// rate is `headroom / (30s + snapshot duration)`.
    pub rebaseline_chain_bytes: u64,
    /// Ceiling of the exponential backoff between full baselines
    /// while SOME db stays uncovered (P1-r5: 30s → 60s → … cap).
    /// Clamped [30s, 1h] by config; default 30min.
    pub baseline_backoff_max_ms: u64,
}

impl WalCfg {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            tick_ms: 2_000,
            max_dbs: 32,
            max_pending_bytes: 256 << 20,
            max_segment_bytes: 8 << 20,
            upload_attempts: 3,
            tick_upload_budget_ms: 500,
            idle_stop_ms: 300_000,
            flush_max_ms: 5_000,
            gc_max_per_tick: 64,
            replay_max_bytes: 1 << 30,
            rebaseline_chain_bytes: 1 << 29,
            baseline_backoff_max_ms: 1_800_000,
        }
    }
}

// ---------- key layout ----------

/// `do/{class_key}/wal/` — under the class's own prefix so
/// `cleanup_class_objects` reclaims segments on class deletion.
fn wal_prefix(class_key: &str) -> String {
    format!("do/{class_key}/wal/")
}

fn seg_key(class_key: &str, stem: &str, epoch: u64, seq: u64, lsn: u64) -> String {
    format!(
        "{}{}/{:016x}/{:016x}/{:016x}.seg",
        wal_prefix(class_key),
        stem,
        epoch,
        seq,
        lsn
    )
}

/// Per-db coverage boundary object: `wal/{stem}/{epoch}/{seq}/anchor`
/// holds the be64 image boundary `B` recorded at the seq's own
/// `on_snapshot_begin` — replay only applies frames `>= B` (I3).
fn anchor_key(class_key: &str, stem: &str, epoch: u64, seq: u64) -> String {
    format!(
        "{}{}/{:016x}/{:016x}/anchor",
        wal_prefix(class_key),
        stem,
        epoch,
        seq
    )
}

/// Deterministic per-db id — bound into every segment so a key
/// mismatch is detectable even past the key layout itself.
fn database_id(class_key: &str, stem: &str) -> String {
    let h = vrt_objstore::sigv4::sha256_hex(format!("{class_key}/{stem}").as_bytes());
    h[..32].to_string()
}

// ---------- errors ----------

#[derive(Debug, thiserror::Error)]
pub enum WalErr {
    #[error("store: {0}")]
    Store(#[from] vrt_objstore::StoreError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("seg: {0}")]
    Seg(#[from] seg::SegError),
    #[error("integrity: {0}")]
    Integrity(String),
}

// ---------- tail state ----------

struct DbTail {
    db_path: PathBuf,
    wal_path: PathBuf,
    pin: Option<WalPin>,
    /// Bounded backoff after a failed pin acquire (P1-4).
    pin_retry_at_ms: u64,
    pin_fail_streak: u32,
    /// Header anchored: salt/pos/cksum reflect the current gen.
    anchored: bool,
    salt: (u32, u32),
    cksum: (u32, u32),
    /// Next WAL byte offset to parse (current generation).
    read_pos: u64,
    /// WAL file length at last stat — reset detection.
    wal_len: u64,
    /// Page size from the anchored WAL header (drives segment field).
    page_size: u32,
    /// Logical index of the NEXT emitted frame — the segment
    /// `wal_begin` source. Monotonic per tail; anchor boundaries are
    /// recorded in this index space (I2).
    emitted: u64,
    /// Verified frames awaiting emit (indices `emitted..`).
    pending: Vec<SegFrame>,
    pending_bytes: u64,
    /// Encoded+uploaded seg queue (never re-ordered): key, body,
    /// frame bytes (Σ `data.len()` — the `chain_bytes` unit).
    queue: std::collections::VecDeque<(String, Vec<u8>, u64)>,
    queue_bytes: u64,
    /// Segment ordinal within `labeled_seq`.
    lsn: u64,
    /// Per-seq lsn continuity across rollback (P1-1).
    lsn_by_seq: BTreeMap<u64, u64>,
    /// Seq label this tail's lsn stream counts under.
    labeled_seq: u64,
    /// MIG-17d(-r2): frame bytes UPLOADED under `labeled_seq` since
    /// this label became current — the SAME unit `replay_for_restore`
    /// charges to `total_bytes` (Σ `SegFrame.data.len()`, headers
    /// excluded). Counted only after the seg's durable put.
    /// Per-label: saved/restored through `chain_bytes_by_seq` so a
    /// P1-1 rollback resumes the resumed label's count. The
    /// `rebaseline_chain_bytes` trigger sums this across ALL dbs of
    /// the class — replay's cap is class-wide too (conservative: a
    /// db at any label counts, even one just rolled back). A db
    /// discovered mid-label undercounts (segs uploaded before
    /// discovery aren't visible).
    chain_bytes: u64,
    /// Per-seq `chain_bytes` continuity across rollback (P1-1) —
    /// mirrors `lsn_by_seq`.
    chain_bytes_by_seq: BTreeMap<u64, u64>,

    /// Seq labels with a durably-written `anchor` object (P0-B: v2
    /// `{B,E}` — emission under a label is allowed only once ITS
    /// anchor landed, i.e. after `on_snapshot_image_taken`).
    anchor_written: HashSet<u64>,
    /// WAL1-r5: monotonically incremented on every successful
    /// `WalPin::acquire` — a release+reacquire is observable.
    pin_gen: u64,
    /// The pin generation recorded by `on_snapshot_begin`. An
    /// anchor's E ≥ M proof needs the pin to have been held
    /// CONTINUOUSLY across the backup window.
    pin_gen_at_begin: u64,
    /// WAL1-r4: this tick's scan consumed the whole valid region
    /// (stopped at frontier/torn tail or file end) — used by repin
    /// and by `on_snapshot_image_taken` (E = caught-up point).
    caught_up: bool,
    /// P0-A: labels permanently sealed by `mark_broken` — a broken
    /// chain's tail is unknowable, so NOTHING may ever be appended
    /// under a sealed label again (not even via rollback).
    sealed: HashSet<u64>,
    /// Boundary `B` for the in-flight baseline (set at begin).
    pending_boundary: u64,
    /// WAL byte length when the current pin was (re)acquired —
    /// pinned span = wal_len - mark_pos.
    mark_pos: u64,
    /// MIG-17c-r2: `mark_broken` set this — the stream is a
    /// data-risk event, so the next `take_needs_baseline` poll
    /// grants IMMEDIATELY (bypassing `baseline_backoff_ms`) instead
    /// of letting the RPO window (last-uploaded → next baseline)
    /// ride out the backoff. Consumed on grant.
    urgent_baseline: bool,
    /// Last time the WAL grew (idle detection).
    last_growth_ms: u64,
    /// Discontinuity / cap breach — wait for a new baseline.
    needs_baseline: bool,
    /// W6-r4: discovered while the class label was still ≤
    /// `ClassTail::retired_stem_seal_ceiling` — the stem's mark may
    /// have been cap-evicted (its seal set is gone, but every
    /// evicted label is ≤ the ceiling). Dormant once `labeled_seq`
    /// provably outruns the ceiling; until then every emission path
    /// treats it like a marked retired stem.
    evicted_suspect: bool,
}

impl DbTail {
    /// Emit gate (I1/P0-A): segments under `labeled_seq` only after
    /// its own anchor landed AND the label was never broken.
    fn can_emit(&self) -> bool {
        self.anchor_written.contains(&self.labeled_seq) && !self.sealed.contains(&self.labeled_seq)
    }

    fn new(db_path: PathBuf, seq: u64, now_ms: u64) -> Self {
        Self {
            wal_path: PathBuf::from(format!("{}-wal", db_path.display())),
            db_path,
            pin: None,
            pin_retry_at_ms: 0,
            pin_fail_streak: 0,
            anchored: false,
            salt: (0, 0),
            cksum: (0, 0),
            read_pos: wal::WAL_HDR as u64,
            wal_len: 0,
            page_size: 4096,
            emitted: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            queue: std::collections::VecDeque::new(),
            queue_bytes: 0,
            lsn: 1,
            lsn_by_seq: BTreeMap::new(),
            labeled_seq: seq,
            chain_bytes: 0,
            chain_bytes_by_seq: BTreeMap::new(),
            anchor_written: HashSet::new(),
            sealed: HashSet::new(),
            caught_up: false,
            pin_gen: 0,
            pin_gen_at_begin: 0,
            pending_boundary: 0,
            mark_pos: wal::WAL_HDR as u64,
            last_growth_ms: now_ms,
            urgent_baseline: false,
            needs_baseline: false,
            evicted_suspect: false,
        }
    }
}

struct ClassTail {
    scope: PlacementScope,
    epoch: u64,
    /// Snapshot seq this class's segments follow (the label dbs are
    /// switched to at `on_snapshot_begin` — per-db lazy state lives
    /// in `DbTail::labeled_seq`).
    seq_label: u64,
    /// Label before the in-flight backup's begin — the rollback
    /// target of `on_snapshot_failed` (P1-1).
    prev_seq_label: Option<u64>,
    dbs: BTreeMap<String, DbTail>,
    next_tick_ms: u64,
    wants_baseline: bool,
    /// First request is immediate (takeover must baseline its OWN
    /// stream before producing adoptable segments — I1); subsequent
    /// requests are rate-limited to one per 30s.
    baseline_requested_ms: Option<u64>,
    /// P1-r5: consecutive baselines that left some db uncovered →
    /// exponential backoff on re-requests (reset on full coverage).
    baseline_streak: u32,
    /// Current re-request interval (starts at 30s, ≤ cfg cap).
    baseline_backoff_ms: u64,
    /// MIG-17d-r2: the CLASS-level chain-byte trigger fired — Σ
    /// `DbTail.chain_bytes` over every stem reached
    /// `cfg.rebaseline_chain_bytes` (the replay cap bounds the sum,
    /// not any single stem). Set once per crossing; re-arms when the
    /// sum drops back below the threshold (new label cycle).
    chain_rebaseline_wanted: bool,
    /// W6-r3: seals lifted to class level when a retired (or broken)
    /// tail is reaped — the DbTail dies with the inode but a same-stem
    /// recreate must STILL never emit under a label the deleted
    /// incarnation already used (its `{epoch,seq,lsn}` keys exist in
    /// the store: a same-LSN append reads as a content conflict, and
    /// a gap-LSN append would splice two incarnations on replay).
    /// Consumed by `discover_dbs` on rediscovery.
    retired_stems: BTreeMap<String, HashSet<u64>>,
    /// W6-r4: upper bound over every seal dropped by a cap eviction
    /// (`max` of each evicted mark's label set). `None` = nothing
    /// evicted yet — a plain 0 would falsely catch every seq-0
    /// discovery before the first baseline. A stem rediscovered
    /// at-or-below it can no longer be PROVED collision-free — its
    /// mark is gone — so the tail turns `evicted_suspect` and holds
    /// for a baseline until the live label outruns every evicted
    /// mark. O(1) memory; resets with the epoch.
    retired_stem_seal_ceiling: Option<u64>,
    /// RV-R28 #3: resumable GC cursor — a bounded store page stops
    /// at a lexicographic bound; without this the sweep would
    /// permanently miss every key past it. `None` = next pass scans
    /// from the head (a complete pass resets it). In-memory: a
    /// restart just rescans — deletes are idempotent.
    gc_cursor: Option<String>,
}

/// W6-r4: hard bound on `retired_stems` — a mark matters only while
/// the class's live label can still collide with the dead
/// incarnation's segments (the prune at `on_snapshot_committed`
/// removes provably-dead marks; churn on never-recreated stems is
/// what this cap exists for). At the cap the LEAST dangerous mark —
/// smallest max label, furthest behind the live seq — is evicted,
/// its label-max folds into `retired_stem_seal_ceiling`, and the
/// class is pulled toward a new baseline. The ceiling keeps
/// eviction fail-CLOSED: a rediscovery at-or-below it can no longer
/// be proved collision-free (unanchored orphan segments the dead
/// incarnation flushed without an anchor could otherwise splice on
/// replay), so the tail stays sealed+baselined until labels outrun
/// every evicted mark — delay, never silent overlap.
const MAX_RETIRED_STEMS: usize = 4096;

/// W6-r4: every label a dying/dead incarnation may have emitted
/// segments under — the superset used both for sealing at
/// rediscovery and for the committed-seq prune bound. `lsn_by_seq`
/// records every label segments were cut under; `anchor_written`
/// every label whose anchor landed (gate for tick-path emission);
/// `sealed` every permanently-broken label; `labeled_seq` is the
/// live label at death (the ungated begin flush drains under it).
fn dead_labels(d: &DbTail) -> HashSet<u64> {
    d.lsn_by_seq
        .keys()
        .copied()
        .chain(d.sealed.iter().copied())
        .chain(d.anchor_written.iter().copied())
        .chain(std::iter::once(d.labeled_seq))
        .collect()
}

/// Per-call view of the owning class (split-borrow friendly).
struct TailInfo<'a> {
    tenant: &'a str,
    class_key: &'a str,
    epoch: u64,
}

// ---------- report ----------

/// Outcome of one replay pass (restore path).
#[derive(Debug, Default)]
pub struct ReplayReport {
    /// `{epoch}/0/` stems skipped by policy (P0-C — never adoptable).
    pub genesis_segments_skipped: u64,
    pub stems_seen: usize,
    pub stems_applied: usize,
    pub segs_applied: usize,
    pub frames_applied: usize,
    /// Logical frame index of the last applied commit.
    pub last_commit_frame: u64,
    /// Listed segments dropped past the chain break (loss visible).
    pub segs_skipped: usize,
    /// Chain ended early (gap / corrupt / missing head).
    pub truncated: bool,
    /// Adoption vetoed by invariant I1 — the baseline was committed
    /// by a different epoch or is a `Final` image; segments were
    /// never consulted.
    pub epoch_mismatch: bool,
    /// Stems whose chain was SELECTED (anchor proved coverage,
    /// frames above the image boundary existed) but whose
    /// fold/apply/integrity step then FAILED — i.e. data that should
    /// have been adopted was not. Callers must treat `>0` as a
    /// restore failure (retryable), never as a clean restore: the
    /// image alone is self-consistent but silently NEWER-state-
    /// dropping. Distinct from `segs_skipped`, which counts
    /// policy-legitimate skips — foreign epochs/seqs, empty chains,
    /// frames below the image floor, torn tails, and the I3
    /// coverage-proven "serve the image" shapes.
    pub stems_rejected: usize,
    /// MIG-17c-r2: listed segs that broke the chain while LATER
    /// segments at the same `{epoch}/{seq}` label provably exist in
    /// the listing — an interior hole or a corrupt interior object
    /// (bad sha/magic, wrong epoch/id/lsn, broken wal_begin link),
    /// NOT a torn tail. Interior corruption is repairable state loss
    /// → every stem it hits counts into `stems_rejected` (fail
    /// closed) and `vrt_host_do_wal_replay_segs_corrupt_total`. A
    /// break with NO later segments stays `truncated` — the classic
    /// writer-died-mid-upload tail, a legitimate skip.
    pub segs_corrupt: usize,
    /// MIG-17c-r3: listed segs that could not be adopted because the
    /// cumulative frame bytes crossed `replay_max_bytes`. NOT
    /// corruption — a configured bound, unfixable by retry alone
    /// (needs a larger `doWalReplayMaxBytes` or chain abandonment
    /// per the §13 runbook). Every stem it hits still counts into
    /// `stems_rejected`: the capped-out segments ARE committed
    /// writes, and serving image+prefix would silently drop them —
    /// even when the cap lands exactly on the chain's tail (that
    /// last seg is committed data, not a torn tail).
    pub cap_exceeded: usize,
}

// ---------- replicator ----------

/// Typed fault counters for unit tests (cfg-gated — not reachable
/// from production code paths). Countdown semantics: N > 0 → the
/// next N operations fail.
#[cfg(test)]
#[derive(Default)]
pub struct WalTestHooks {
    /// Fail the next N `WalPin::acquire` calls (SQLITE_BUSY).
    pub pin_fail: std::sync::atomic::AtomicUsize,
    /// Fail the next N frame-region `read_at` calls (io error).
    pub frame_read_fail: std::sync::atomic::AtomicUsize,
}

pub struct DoWalReplicator {
    cfg: WalCfg,
    store: Arc<dyn ObjectStore>,
    #[allow(dead_code)]
    tmp_root: PathBuf,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    inner: Mutex<BTreeMap<String, ClassTail>>,
    /// Typed fault-injection hooks — test-only, zero prod surface.
    #[cfg(test)]
    pub hooks: WalTestHooks,
}

impl DoWalReplicator {
    pub fn new(
        cfg: WalCfg,
        store: Arc<dyn ObjectStore>,
        tmp_root: PathBuf,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            cfg,
            store,
            tmp_root,
            now_ms,
            #[cfg(test)]
            hooks: WalTestHooks::default(),
            inner: Mutex::new(BTreeMap::new()),
        }
    }

    fn now(&self) -> u64 {
        (self.now_ms)()
    }

    /// Replay bound surfaced to the restore path.
    pub fn replay_max_bytes(&self) -> u64 {
        self.cfg.replay_max_bytes
    }

    /// Register a class for tailing. `seq_label` = the placement
    /// record's committed snapshot seq (0 when none — genesis
    /// segments then rebuild the db from frame 0).
    ///
    /// A non-genesis takeover immediately wants its OWN baseline:
    /// until it commits, `anchor_written`/`sealed` keep this epoch from
    /// producing adoptable segments (I1).
    pub fn start(&self, scope: &PlacementScope, epoch: u64, seq_label: u64) {
        if !self.cfg.enabled {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let t = g
            .entry(scope.class_key.clone())
            .or_insert_with(|| ClassTail {
                scope: scope.clone(),
                epoch,
                seq_label,
                prev_seq_label: None,
                dbs: BTreeMap::new(),
                next_tick_ms: 0,
                // P0-C: every takeover wants its own baseline.
                wants_baseline: true,
                baseline_requested_ms: None,
                baseline_streak: 0,
                baseline_backoff_ms: 30_000,
                chain_rebaseline_wanted: false,
                retired_stems: BTreeMap::new(),
                retired_stem_seal_ceiling: None,
                gc_cursor: None,
            });
        // Re-acquire of the same class: adopt the NEW epoch and its
        // baseline label — streams are per-epoch namespaced so no
        // carry-over state is safe to keep.
        //
        // MIG-17c-r2: no drain of the outgoing tail — by design.
        // Epoch-namespaced adoption means a restore at the NEW epoch
        // can NEVER replay the old epoch's segments, so uploading
        // the remnant serves only forensics while blocking the
        // actor up to `flush_max_ms` PER drifted class (N classes
        // would stack past the switch budget). RPO is preserved by
        // the SNAPSHOT path instead: the dropped pin lets the next
        // checkpoint fold unparsed frames into the db, and the new
        // tail's first `take_needs_baseline` fires immediately —
        // the baseline image carries db+`-wal` either way.
        if t.epoch != epoch || t.scope.class_dir != scope.class_dir {
            *t = ClassTail {
                scope: scope.clone(),
                epoch,
                seq_label,
                prev_seq_label: None,
                dbs: BTreeMap::new(),
                next_tick_ms: 0,
                // P0-C: every takeover wants its own baseline.
                wants_baseline: true,
                baseline_requested_ms: None,
                baseline_streak: 0,
                baseline_backoff_ms: 30_000,
                chain_rebaseline_wanted: false,
                retired_stems: BTreeMap::new(),
                retired_stem_seal_ceiling: None,
                gc_cursor: None,
            };
        }
    }

    /// Drop tails for classes no longer leased (undeploy / release /
    /// epoch loss). Connections close → pins release.
    pub fn retain(&self, active: &HashSet<String>) {
        let mut g = self.inner.lock().unwrap();
        g.retain(|k, _| active.contains(k));
    }

    /// Snapshot backup is STARTING for OUR epoch — per db:
    ///
    /// 1. a `needs_baseline`/unanchored db is re-armed FIRST (pin +
    ///    anchor + parse) so its continuity boundary sits at-or-
    ///    before the image point (I2);
    /// 2. pending frames are cut under the OLD label at the last
    ///    commit boundary — every emitted frame was provably
    ///    committed before the backup started, hence inside the
    ///    image (I2);
    /// 3. the boundary `B = emitted` is recorded in a per-db
    ///    `anchor` object, then the tail switches to `seq`: every
    ///    later frame (index >= B) lands under the new label, and
    ///    replay adopts them only past `B` (I3).
    pub fn on_snapshot_begin(&self, class_key: &str, seq: u64) {
        if !self.cfg.enabled {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let Some(t) = g.get_mut(class_key) else {
            return;
        };
        t.prev_seq_label = Some(t.seq_label);
        t.seq_label = seq;
        let now = self.now();
        self.discover_dbs(t, now);
        let info = TailInfo {
            tenant: &t.scope.tenant,
            class_key: &t.scope.class_key,
            epoch: t.epoch,
        };
        // `None` ceiling ⇒ no evictions ⇒ no suspect can exist —
        // unwrap_or(0) is then dead code (flag is false everywhere).
        let seal_ceiling = t.retired_stem_seal_ceiling.unwrap_or(0);
        let stems: Vec<String> = t.dbs.keys().cloned().collect();
        for stem in stems {
            let Some(d) = t.dbs.get_mut(&stem) else {
                continue;
            };
            // r6-P0: an anchored-but-unpinned db MUST run one
            // normal tail under its OLD label before any rearm.
            // tail_db re-acquires the pin (pin_gen+1) and runs the
            // header salt/shrink check — a cross-generation reset
            // during the un-pinned gap fires mark_broken which SEALS
            // the old label. Blindly re-arming here would mask that
            // reset: rearm discards pending/queue and re-anchors
            // from the new generation's head with `emitted=parsed` —
            // if the new gen happens to hold exactly the old emitted
            // count, a later rollback to the unsealed label would
            // append new-gen frames at wal_begin=emitted, faking lsn
            // and wal_begin continuity across the lost interval →
            // page-level mix on replay.
            if !d.needs_baseline && d.anchored && d.pin.is_none() {
                let _ = self.tail_db(&info, d, &stem, now, None, true);
            }
            // W6-r4: an evicted-mark suspect whose live label is
            // still inside the evicted range must never take the
            // continuous path — that flush emits under `labeled_seq`
            // ungated, and a cap-evicted mark may hold dead segments
            // under exactly that label (same-LSN → conflict, gap-LSN
            // → splice on replay).
            let mut was_unanchored = d.needs_baseline
                || !d.anchored
                || (d.evicted_suspect && d.labeled_seq <= seal_ceiling);
            if !was_unanchored && d.pin.is_none() {
                // The tail could not re-acquire the pin — continuity
                // across the un-pinned gap is unprovable. Seal the
                // old label, then re-arm from the current head.
                d.sealed.insert(d.labeled_seq);
                was_unanchored = true;
            }
            if was_unanchored {
                // Re-arm: re-pin + anchor + parse into pending. Frames
                // between the break and now are all pre-begin commits
                // → inside the new image; they are DROPPED below —
                // appending them under any existing label would fake
                // lsn/wal_begin continuity across the lost interval.
                // needs_baseline must clear BEFORE rearm — tail_db
                // early-returns on the flag, so re-arming while it
                // is still set can never anchor (the db would loop
                // needs_baseline forever).
                d.sealed.insert(d.labeled_seq);
                // W6-r4: the suspect must not rearm under a label
                // still inside the evicted range either — anchoring
                // would succeed whenever the dead incarnation never
                // anchored that label itself, then adopt its orphan
                // segments on replay. Hold (needs_baseline stays)
                // until seq outruns the ceiling — bounded: every
                // committed baseline advances it.
                if d.evicted_suspect && seq <= seal_ceiling {
                    continue;
                }
                d.needs_baseline = false;
                self.rearm(&info, d, &stem, now);
                if !d.anchored {
                    // Couldn't establish continuity — no adoptable
                    // chain for seq; retry at the next begin.
                    d.needs_baseline = true;
                    continue;
                }
                // Parsed frames are all pre-begin commits → inside
                // the new image → DROPPED (never emitted under the
                // possibly-sealed old label). The new chain's
                // boundary B = re-parsed count of this WAL gen.
                let parsed = d.pending.len() as u64;
                d.pending.clear();
                d.pending_bytes = 0;
                d.emitted = parsed;
            } else {
                // Continuous pin (or a successful re-tail): pending =
                // pre-begin commits → emit under the OLD label
                // (keeps that chain complete).
                self.cut_segments(&info, d, &stem, Cut::CommitBoundary);
                self.drain_queue(&info, d, None);
            }
            // r5: remember the pin generation the anchor's E ≥ M
            // proof will be checked against at image_taken.
            d.pin_gen_at_begin = d.pin_gen;
            d.pending_boundary = d.emitted;
            d.lsn_by_seq.insert(d.labeled_seq, d.lsn);
            d.chain_bytes_by_seq.insert(d.labeled_seq, d.chain_bytes);
            d.labeled_seq = seq;
            d.lsn = *d.lsn_by_seq.get(&seq).unwrap_or(&1);
            d.chain_bytes = *d.chain_bytes_by_seq.get(&seq).unwrap_or(&0);
        }
    }

    /// The snapshot image is durably taken (online backup's read
    /// txn closed) and about to be committed — record `{B,E}` in the
    /// per-db anchor (P0-B). The pin has been held since begin, so
    /// the WAL end now is ≥ the image's mxFrame M → `E = emitted +
    /// pending` is a logical frame index ≥ M; replay may only apply
    /// a chain whose last commit reaches ≥ E (image is a FLOOR).
    pub fn on_snapshot_image_taken(&self, class_key: &str, seq: u64) {
        if !self.cfg.enabled {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let Some(t) = g.get_mut(class_key) else {
            return;
        };
        let now = self.now();
        self.discover_dbs(t, now);
        let info = TailInfo {
            tenant: &t.scope.tenant,
            class_key: &t.scope.class_key,
            epoch: t.epoch,
        };
        let stems: Vec<String> = t.dbs.keys().cloned().collect();
        for stem in stems {
            let Some(d) = t.dbs.get_mut(&stem) else {
                continue;
            };
            if d.needs_baseline || !d.anchored {
                continue;
            }
            // Parse ALL the way to the WAL end — pin held → end ≥ M
            // (I2). tail_db caps a single pass at 64MiB; a longer
            // tail needs several passes, otherwise E would
            // understate the true end and could fall BELOW M —
            // breaking the image-floor proof. Bounded passes; if we
            // still can't reach the end, the anchor is unreliable →
            // rebaseline (fail closed).
            // `caught_up` = this pass consumed every commit frame in
            // the valid region — E is then the index just past the
            // last commit (≥ M: the image's mxFrame is itself a
            // commit frame ≤ the wal end while pinned; residual
            // old-generation bytes never count toward E).
            // WAL1-r5: reached_end requires THIS call's scan to be
            // caught up AND the pin held CONTINUOUSLY since begin
            // (same pin_gen) — a release+reacquire gap can hide a
            // reset (or the scan may never have pinned at all), so
            // it cannot prove E ≥ M → fail closed.
            let mut reached_end = false;
            for _ in 0..8 {
                if self.tail_db(&info, d, &stem, now, None, true) {
                    t.wants_baseline = true;
                }
                if d.needs_baseline || !d.anchored {
                    break;
                }
                let pin_ok = d.pin.is_some() && d.pin_gen == d.pin_gen_at_begin;
                if d.caught_up && pin_ok {
                    reached_end = true;
                    break;
                }
                if !pin_ok {
                    break; // can never recover within this hook
                }
            }
            if d.needs_baseline || !d.anchored {
                continue;
            }
            if !reached_end {
                d.needs_baseline = true;
                t.wants_baseline = true;
                continue;
            }
            let e = d.emitted + d.pending.len() as u64;
            let mut body = Vec::with_capacity(17);
            body.push(2u8);
            body.extend_from_slice(&d.pending_boundary.to_be_bytes());
            body.extend_from_slice(&e.to_be_bytes());
            let key = anchor_key(info.class_key, &stem, info.epoch, seq);
            if !self.put_durable(&key, &body) {
                // No anchor → chain never adoptable → rebaseline.
                d.needs_baseline = true;
                t.wants_baseline = true;
                continue;
            }
            d.anchor_written.insert(seq);
            // Post-B frames (parsed during the image window) emit
            // under the NEW label — the chain crosses E by itself.
            self.cut_segments(&info, d, &stem, Cut::CommitBoundary);
            self.drain_queue(&info, d, None);
        }
    }

    /// Baseline committed — clear rebaseline requests.
    pub fn on_snapshot_committed(&self, class_key: &str, seq: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some(t) = g.get_mut(class_key) {
            t.seq_label = seq;
            // W6-r4: prune retired marks the committed seq has
            // outlived. A mark can only ever matter while the LIVE
            // label may still collide with the dead incarnation's
            // segments: a rediscovered tail always starts at
            // `labeled_seq >= seq_label` (labels only advance), and a
            // committed seq can never be rolled back below itself
            // (rollback only undoes a FAILED in-flight begin). Once
            // every recorded emission label is < seq, a future
            // rediscovery's lsn stream cannot touch the old keys —
            // the mark gates nothing, so it is dropped. Empty marks
            // (never emitted under this epoch) drop too.
            let retired_before = t.retired_stems.len();
            t.retired_stems
                .retain(|_, seals| seals.iter().max().is_some_and(|m| *m >= seq));
            if t.retired_stems.len() != retired_before {
                crate::metrics::registry().set(
                    "vrt_host_do_wal_retired_stems",
                    &[("tenant", t.scope.tenant.as_str()), ("class", class_key)],
                    t.retired_stems.len() as f64,
                );
            }
            // WAL1-r4 P1: clear needs_baseline ONLY for dbs that
            // anchored this seq. A db whose image_taken anchor write
            // failed, never caught up, or arrived after the baseline
            // remains uncovered: keep its flag so the next baseline
            // is still requested (rate-limited) and count it as
            // uncovered. `wants_baseline` clears only when every
            // tracked db holds seq's anchor.
            let mut uncovered = 0u64;
            for d in t.dbs.values_mut() {
                // MIG-17d-r2: labels < the committed seq can never
                // be rolled back — prune the byte-continuity map.
                // (`lsn_by_seq` is deliberately NOT pruned: it feeds
                // `dead_labels`, whose superset-of-emitted-labels
                // semantic seals rediscovered stems — shrinking it
                // would weaken that guard for zero benefit.)
                d.chain_bytes_by_seq.retain(|s, _| *s >= seq);
                if d.anchor_written.contains(&seq) {
                    d.needs_baseline = false;
                } else {
                    d.needs_baseline = true;
                    uncovered += 1;
                }
            }
            t.wants_baseline = uncovered > 0;
            if uncovered > 0 {
                // P1-r5: every baseline that still leaves a db
                // uncovered doubles the next re-request interval,
                // capped by cfg — resets to 30s on full coverage.
                t.baseline_streak = t.baseline_streak.saturating_add(1);
                t.baseline_backoff_ms =
                    (30_000u64 << t.baseline_streak.min(7)).min(self.cfg.baseline_backoff_max_ms);
                crate::metrics::registry().set(
                    "vrt_host_do_wal_baseline_backoff_ms",
                    &[("tenant", &t.scope.tenant), ("class", class_key)],
                    t.baseline_backoff_ms as f64,
                );
                tracing::warn!(
                    class = class_key,
                    seq,
                    uncovered,
                    "wal replication: baseline committed with uncovered dbs"
                );
                crate::metrics::registry().inc(
                    "vrt_host_do_wal_uncovered_dbs",
                    &[("tenant", &t.scope.tenant)],
                    uncovered as f64,
                );
            } else if t.baseline_streak > 0 {
                t.baseline_streak = 0;
                t.baseline_backoff_ms = 30_000;
                crate::metrics::registry().set(
                    "vrt_host_do_wal_baseline_backoff_ms",
                    &[("tenant", &t.scope.tenant), ("class", class_key)],
                    0.0,
                );
            }
        }
    }

    /// A snapshot attempt FAILED after `on_snapshot_begin` switched
    /// labels (P1-1): roll every db back onto the still-committed
    /// label — segments emitted under the orphan label are never
    /// adopted (seq never becomes the baseline; GC reclaims them)
    /// and the rolled-back chain resumes its own lsn numbering so
    /// no collision occurs. The backup window's frames
    /// `(boundary..failure]` were already emitted under the orphan
    /// label; a fresh baseline covers them next pass.
    pub fn on_snapshot_failed(&self, class_key: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some(t) = g.get_mut(class_key) {
            let failed_seq = t.seq_label;
            let committed_seq = t.prev_seq_label.unwrap_or(failed_seq);
            t.seq_label = committed_seq;
            t.prev_seq_label = None;
            t.wants_baseline = true;
            for d in t.dbs.values_mut() {
                if d.labeled_seq == failed_seq {
                    d.lsn_by_seq.insert(d.labeled_seq, d.lsn);
                    d.chain_bytes_by_seq.insert(d.labeled_seq, d.chain_bytes);
                    d.labeled_seq = committed_seq;
                    d.lsn = *d.lsn_by_seq.get(&committed_seq).unwrap_or(&1);
                    d.chain_bytes =
                        *d.chain_bytes_by_seq.get(&committed_seq).unwrap_or(&0);
                    // P0-A: rolling back onto a SEALED label must not
                    // resume its poisoned chain — keep it silenced
                    // and re-baseline instead.
                    if d.sealed.contains(&committed_seq) {
                        d.needs_baseline = true;
                    }
                }
            }
        }
    }

    /// Supervisor poll: any db waiting for a fresh baseline →
    /// force the periodic snapshot due. The first request after a
    /// (re)start fires immediately (I1 — a takeover must not wait a
    /// snapshot interval before it can emit adoptable segments);
    /// subsequent requests are limited to one per 30s so a
    /// persistently-failing snapshot neither hot-loops nor
    /// permanently starves the stream.
    pub fn take_needs_baseline(&self, class_key: &str) -> bool {
        let now = self.now();
        let mut g = self.inner.lock().unwrap();
        g.get_mut(class_key)
            .map(|t| {
                // MIG-17d-r2: the replay cap bounds the SUM of all
                // stems' chains — the trigger is the CLASS total.
                // Conservative: a db at ANY label counts (a
                // just-rolled-back db's old-label bytes still
                // represent live un-baselined writes its next chain
                // must cover).
                let class_bytes: u64 =
                    t.dbs.values().map(|d| d.chain_bytes).sum();
                let chain_fire =
                    class_bytes >= self.cfg.rebaseline_chain_bytes;
                if chain_fire && !t.chain_rebaseline_wanted {
                    // NORMAL rebaseline — NOT mark_broken: the
                    // chains are healthy, they just need a fresh
                    // baseline before their SUM can outgrow
                    // `replay_max_bytes`. Once per crossing.
                    t.chain_rebaseline_wanted = true;
                    crate::metrics::registry().inc(
                        "vrt_host_do_wal_chain_rebaseline_total",
                        &[("tenant", &t.scope.tenant)],
                        1.0,
                    );
                    tracing::info!(
                        tenant = %t.scope.tenant,
                        class = %t.scope.class_key,
                        class_bytes,
                        dbs = t.dbs.len(),
                        threshold = self.cfg.rebaseline_chain_bytes,
                        "do wal: class chain bytes over rebaseline threshold — requesting baseline"
                    );
                } else if !chain_fire {
                    t.chain_rebaseline_wanted = false;
                }
                if chain_fire {
                    t.wants_baseline = true;
                }
                if !t.wants_baseline {
                    return false;
                }
                // MIG-17c-r2: a broken stream jumps the backoff —
                // the flag is one-shot (set by mark_broken, consumed
                // here), so a permanently-broken db still throttles
                // on the escalated interval after the urgent grant.
                if t.dbs.values_mut().any(|d| {
                    let u = d.urgent_baseline;
                    d.urgent_baseline = false;
                    u
                }) {
                    t.baseline_requested_ms = Some(now);
                    return true;
                }
                match t.baseline_requested_ms {
                    None => {
                        t.baseline_requested_ms = Some(now);
                        true
                    }
                    // P1-r5: interval escalates with consecutive
                    // uncovered baselines (capped by cfg) instead of
                    // a flat 30s — a permanently-unpinnable db must
                    // not force a full snapshot every 30s forever.
                    // MIG-17d: a chain-rebaseline request caps the
                    // wait at the BASE 30s — an escalated backoff
                    // (from unrelated uncovered dbs) could outlast
                    // the chain's headroom to `replay_max_bytes`;
                    // this is a normal baseline and does NOT count
                    // into the uncovered-escalation streak.
                    Some(last)
                        if now.saturating_sub(last)
                            >= (if chain_fire {
                                t.baseline_backoff_ms.min(30_000)
                            } else {
                                t.baseline_backoff_ms
                            }) =>
                    {
                        t.baseline_requested_ms = Some(now);
                        true
                    }
                    _ => false,
                }
            })
            .unwrap_or(false)
    }

    /// Drive one class's tails if its throttle window elapsed.
    pub fn tick(&self, class_key: &str) {
        if !self.cfg.enabled {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let Some(t) = g.get_mut(class_key) else {
            return;
        };
        let now = self.now();
        if now < t.next_tick_ms {
            return;
        }
        t.next_tick_ms = now + self.cfg.tick_ms;
        self.discover_dbs(t, now);
        // Reap tails whose db vanished (an unlinked inode would be
        // kept alive by the held connection — a slow disk leak).
        // W6-r3: lift the mark to class level FIRST — a same-stem
        // recreate rediscovered later must never emit under a label
        // the deleted incarnation already used. W6-r4: the lifted
        // set is `dead_labels` (every label segments may have been
        // emitted under), and the mark goes through the bounded
        // `mark_retired_stem`.
        let mut reaped: Vec<(String, HashSet<u64>)> = Vec::new();
        t.dbs.retain(|stem, d| {
            if d.db_path.exists() {
                return true;
            }
            if !d.sealed.is_empty()
                || !d.anchor_written.is_empty()
                || !d.lsn_by_seq.is_empty()
                || d.needs_baseline
            {
                reaped.push((stem.clone(), dead_labels(d)));
            }
            false
        });
        for (stem, labels) in reaped {
            self.mark_retired_stem(t, &stem, labels);
        }
        let info = TailInfo {
            tenant: &t.scope.tenant,
            class_key: &t.scope.class_key,
            epoch: t.epoch,
        };
        let stems: Vec<String> = t.dbs.keys().cloned().collect();
        let upload_deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(self.cfg.tick_upload_budget_ms);
        let mut broke = false;
        for stem in stems {
            if let Some(d) = t.dbs.get_mut(&stem) {
                broke |= self.tail_db(&info, d, &stem, now, Some(upload_deadline), false);
            }
        }
        t.wants_baseline |= broke;
    }

    /// Discover `*.sqlite` files (≤ max_dbs; skip when over).
    fn discover_dbs(&self, t: &mut ClassTail, now: u64) {
        let Ok(rd) = std::fs::read_dir(&t.scope.class_dir) else {
            return;
        };
        for e in rd.flatten() {
            if t.dbs.len() >= self.cfg.max_dbs {
                break;
            }
            let p = e.path();
            let name = e.file_name().to_string_lossy().to_string();
            if p.extension().map(|x| x == "sqlite").unwrap_or(false) {
                let stem = name.trim_end_matches(".sqlite").to_string();
                let seq = t.seq_label;
                if let std::collections::btree_map::Entry::Vacant(e) = t.dbs.entry(stem) {
                    let stem = e.key().clone();
                    let mut d = DbTail::new(p.clone(), seq, now);
                    // W6-r3: a retired stem rediscovered under the
                    // still-current label must not emit under it —
                    // the deleted incarnation's segments occupy that
                    // key space. Inherit the lifted seals, seal the
                    // current label too (this tail arrived after the
                    // label's image point — it can never anchor under
                    // it anyway), and demand a baseline: the next
                    // begin rearms it under a fresh label instead of
                    // the continuous old-label flush.
                    if let Some(seals) = t.retired_stems.remove(&stem) {
                        d.sealed.extend(seals);
                        d.sealed.insert(seq);
                        d.needs_baseline = true;
                    } else if t.retired_stem_seal_ceiling.is_some_and(|c| seq <= c) {
                        // W6-r4: this stem's mark may have been
                        // cap-evicted — the seal set is lost but
                        // every evicted label is ≤ the ceiling, and
                        // the current label is still inside that
                        // range. Treat it like a marked stem: seal
                        // this label, demand a baseline, and keep the
                        // suspect guard until `labeled_seq` outruns
                        // every evicted mark.
                        d.sealed.insert(seq);
                        d.needs_baseline = true;
                        d.evicted_suspect = true;
                    }
                    e.insert(d);
                    // A db discovered after the current baseline's
                    // anchor landed can never be covered by it — its
                    // segments under this label stay unadoptable
                    // (no anchor → replay skips the stem). Count it
                    // and pull the next baseline forward (bounded).
                    t.wants_baseline = true;
                    // P2-r6: during a backoff window the request is
                    // BOUNDED (≤ baseline_backoff_ms ≤
                    // baselineBackoffMaxMs), not immediate — log so
                    // the delayed coverage is observable. A
                    // per-db bypass was rejected: churning new stems
                    // would defeat the class-level cap.
                    if let Some(last) = t.baseline_requested_ms {
                        if now.saturating_sub(last) < t.baseline_backoff_ms {
                            tracing::info!(
                                tenant = %t.scope.tenant,
                                class = %t.scope.class_key,
                                stem = %stem,
                                backoff_ms = t.baseline_backoff_ms,
                                "wal replication: new db uncovered — baseline request waits out current backoff"
                            );
                        }
                    }
                    crate::metrics::registry().inc(
                        "vrt_host_do_wal_uncovered_dbs",
                        &[("tenant", &t.scope.tenant)],
                        1.0,
                    );
                }
            }
        }
    }

    /// Re-pin + re-anchor + parse for a db that broke (or never
    /// anchored). Called inside `on_snapshot_begin` so the
    /// subsequently-recorded boundary provably precedes the image
    /// point (I2). Parsed frames land in `pending`; the caller's
    /// old-label flush emits the committed prefix under the previous
    /// label — all pre-begin commits are inside the new image anyway.
    fn rearm(&self, info: &TailInfo, d: &mut DbTail, stem: &str, now: u64) {
        // r6-P0: ANY rearm drops pending/queue and re-anchors from
        // the current generation's head — the outgoing label's chain
        // tail is unprovable from here on → seal it, no exceptions.
        d.sealed.insert(d.labeled_seq);
        d.pin = None;
        d.anchored = false;
        d.pending.clear();
        d.pending_bytes = 0;
        d.queue.clear();
        d.queue_bytes = 0;
        d.pin_retry_at_ms = 0;
        d.pin_fail_streak = 0;
        let _ = self.tail_db(info, d, stem, now, None, true);
    }

    /// Returns true when the db broke and needs a new baseline.
    /// `for_image`: called inside `on_snapshot_image_taken`/`rearm`
    /// — the caller owns pin lifecycle there; skip repin/idle-release
    /// (a release inside the image window is meaningless and would
    /// break the pin-continuity check).
    fn tail_db(
        &self,
        info: &TailInfo,
        d: &mut DbTail,
        stem: &str,
        now: u64,
        upload_deadline: Option<std::time::Instant>,
        for_image: bool,
    ) -> bool {
        // WAL1-r5: caught_up is a THIS-CALL result — never let a
        // stale value leak across early returns (an anchor's E must
        // be computed from a scan that actually happened, pinned,
        // with the current header verified).
        d.caught_up = false;
        if d.needs_baseline {
            return false;
        }
        // --- pin lifecycle (bounded retry — P1-4) ---
        if d.pin.is_none() {
            if now < d.pin_retry_at_ms {
                return false;
            }
            #[cfg(test)]
            let injected = self
                .hooks
                .pin_fail
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |n| n.checked_sub(1),
                )
                .is_ok();
            #[cfg(not(test))]
            let injected = false;
            // String err keeps the test hook independent of the
            // rusqlite type (dev-dependency only).
            let acq: Result<WalPin, String> = if injected {
                Err("injected pin acquire failure".to_string())
            } else {
                WalPin::acquire(&d.db_path).map_err(|e| e.to_string())
            };
            match acq {
                Ok(p) => {
                    d.pin = Some(p);
                    d.pin_fail_streak = 0;
                    d.pin_gen += 1;
                    d.mark_pos = d.wal_len.max(wal::WAL_HDR as u64);
                }
                Err(e) => {
                    // EXCLUSIVE lock / missing db — cannot pin yet.
                    // Bounded exponential backoff (1s → 60s cap),
                    // never sticky-fatal: a WAL-mode db may just be
                    // mid-recreate. Metric exposes the degraded path.
                    d.pin_fail_streak = d.pin_fail_streak.saturating_add(1);
                    let back = (1_000u64 << d.pin_fail_streak.min(6)).min(60_000);
                    d.pin_retry_at_ms = now + back;
                    crate::metrics::registry().inc(
                        "vrt_host_do_wal_pin_unavailable",
                        &[("tenant", info.tenant)],
                        1.0,
                    );
                    tracing::warn!(
                        tenant = %info.tenant, class = %info.class_key, stem,
                        err = %e, "do wal pin acquire failed — retrying (snapshot-only meanwhile)"
                    );
                    return false;
                }
            }
        }
        // --- observe wal ---
        let len = match std::fs::metadata(&d.wal_path) {
            Ok(m) => m.len(),
            Err(_) => 0,
        };
        if len > d.wal_len {
            d.last_growth_ms = now;
        }
        if len < wal::WAL_HDR as u64 {
            // No/empty header. If previously anchored the wal was
            // reset wholesale while unverifiable → break (I2).
            if d.anchored {
                self.mark_broken(info, d, "wal file shrank/reset");
                return true;
            }
            return false;
        }
        d.wal_len = len;
        // --- MIG-17c: bound the pinned-but-unparsed span HERE ---
        // `max_pending_bytes` has always been documented as "memory +
        // pinned span", and every early return below (torn header,
        // scan error, stalled deadline) must not strand it: a scanner
        // that stalls while the writer floods would otherwise let the
        // pin protect UNBOUNDED -wal growth — a checkpoint can never
        // reclaim below the mark. Past the cap the stream is
        // unverifiable-in-practice → break it the same way: pin
        // released, baseline re-armed, committed frames re-derived
        // from the next snapshot image (fail closed — never silently
        // dropped).
        if len.saturating_sub(d.mark_pos) > self.cfg.max_pending_bytes {
            d.pending.clear();
            d.pending_bytes = 0;
            d.queue.clear();
            d.queue_bytes = 0;
            self.mark_broken(info, d, "pinned wal span over cap");
            crate::metrics::registry().inc(
                "vrt_host_do_wal_broken_total",
                &[("tenant", info.tenant), ("reason", "pin_span_cap")],
                1.0,
            );
            return true;
        }
        let hdr_bytes = match read_at(&d.wal_path, 0, wal::WAL_HDR) {
            Ok(b) => b,
            Err(_) => return false,
        };
        let Some(hdr) = wal::wal_header(&hdr_bytes) else {
            // r7: an ANCHORED db whose header no longer parses cannot
            // verify ANY part of its stream — treating it as a
            // transient torn read would loop forever on persistent
            // corruption and could let a same-salt rewrite slip past
            // the salt check. Fail closed (break); an unanchored db
            // keeps the transient-retry behavior.
            if d.anchored {
                self.mark_broken(info, d, "wal header unreadable");
                return true;
            }
            return false; // torn header — next tick
        };
        if !d.anchored {
            d.page_size = hdr.page_size();
            d.salt = (hdr.salt1, hdr.salt2);
            d.cksum = (
                u32::from_be_bytes(hdr_bytes[24..28].try_into().unwrap()),
                u32::from_be_bytes(hdr_bytes[28..32].try_into().unwrap()),
            );
            d.read_pos = wal::WAL_HDR as u64;
            d.anchored = true;
        } else if (hdr.salt1, hdr.salt2) != d.salt || len < d.read_pos {
            // I2: while our pin is held a reset is physically
            // impossible — any observed reset therefore happened
            // while un-pinned, and the lost extent is unverifiable.
            // ALWAYS break; never re-anchor mid-stream.
            self.mark_broken(info, d, "wal reset (salt/shrink)");
            return true;
        }
        // Header verified this call, nothing unconsumed beyond
        // read_pos (no frames at all) → caught up by definition.
        if len == d.read_pos {
            d.caught_up = true;
        }
        // --- parse new frames ---
        if len > d.read_pos {
            let want = (len - d.read_pos).min(64 << 20) as usize;
            let buf_r = {
                #[cfg(test)]
                if self
                    .hooks
                    .frame_read_fail
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |n| n.checked_sub(1),
                    )
                    .is_ok()
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "injected read failure",
                    ))
                } else {
                    read_at(&d.wal_path, d.read_pos, want)
                }
                #[cfg(not(test))]
                {
                    read_at(&d.wal_path, d.read_pos, want)
                }
            };
            match buf_r {
                Ok(buf) => {
                    let fsz = wal::WAL_FRAME_HDR as u64 + hdr.page_size() as u64;
                    let (_vend, heads, _ck) =
                        wal::scan_frames(&buf, 0, hdr.page_size(), hdr.cksum_be(), d.salt, d.cksum);
                    // WAL1-r4: the first invalid frame ends the valid
                    // region — never a break. It is either a torn
                    // in-flight tail or OLD-GENERATION residue after a
                    // restart (restart does NOT truncate the file —
                    // PoC §6.2/9). Resets are detected only via the
                    // 32B header above.
                    // `caught_up` = the scan consumed everything valid
                    // this tick: it stopped before exhausting the buf
                    // (frontier/torn tail) OR reached the file end.
                    let consumed_bytes = (heads.len() as u64) * fsz;
                    d.caught_up =
                        consumed_bytes < buf.len() as u64 || d.read_pos + buf.len() as u64 >= len;
                    // Consume ONLY through the last commit frame.
                    // Uncommitted tail frames never enter pending:
                    // rolled-back/spill frames may be overwritten
                    // in-place by a same-generation later txn —
                    // keeping read_pos behind them means we always
                    // re-read the CURRENT bytes (P0 proof in §6.11).
                    let last_commit = heads.iter().rposition(|h| h.commit_db_size != 0);
                    if let Some(lc) = last_commit {
                        for (i, h) in heads.iter().enumerate().take(lc + 1) {
                            let off = i * fsz as usize + wal::WAL_FRAME_HDR;
                            d.pending.push(SegFrame {
                                page_no: h.page_no,
                                commit_db_size: h.commit_db_size,
                                data: buf[off..off + hdr.page_size() as usize].to_vec(),
                            });
                            d.pending_bytes += fsz;
                        }
                        d.cksum = heads[lc].cksum;
                        d.read_pos += (lc as u64 + 1) * fsz;
                    }
                }
                Err(_) => return false,
            }
        }
        // --- cut + upload (I1 gate) ---
        if d.can_emit() {
            self.cut_segments(info, d, stem, Cut::CommitBoundary);
            self.drain_queue(&info, d, upload_deadline);
        }
        // --- bounds: pending+queue vs cap ---
        let unuploaded = d.pending_bytes + d.queue_bytes;
        if unuploaded > self.cfg.max_pending_bytes {
            d.pending.clear();
            d.pending_bytes = 0;
            d.queue.clear();
            d.queue_bytes = 0;
            d.pin = None; // release → checkpoint reclaims disk
            self.mark_broken(info, d, "pending wal over cap");
            crate::metrics::registry().inc(
                "vrt_host_do_wal_broken_total",
                &[("tenant", info.tenant), ("reason", "pending_cap")],
                1.0,
            );
            return true;
        }
        // --- repin / idle release ---
        let pinned_span = d.wal_len.saturating_sub(d.mark_pos);
        // Repin only when FULLY caught up — a repin asserts "nothing
        // below wal_len is needed"; repinning past unparsed bytes
        // would let a checkpoint reset strand them.
        // WAL1-r4: repin when pending+queue drained AND this tick's
        // scan consumed the whole valid region — a repin asserts
        // "no unconsumed frame below the new mark". It can never
        // release frames that were merely scanned-but-unconsumed:
        // caught_up means the last commit of the region is at-or-
        // below read_pos, and read_pos only advances past COMMITS.
        if !for_image {
            if d.pending.is_empty() && d.queue.is_empty() && d.caught_up && pinned_span > 0 {
                if let Some(p) = d.pin.as_mut() {
                    if p.repin().is_ok() {
                        d.mark_pos = d.wal_len;
                    }
                }
            }
            if now.saturating_sub(d.last_growth_ms) > self.cfg.idle_stop_ms {
                d.pin = None; // idle — stop pinning; re-anchor on growth
            }
        }
        crate::metrics::registry().set(
            "vrt_host_do_wal_lag_bytes",
            &[("tenant", info.tenant), ("class", info.class_key)],
            (d.pending_bytes + d.queue_bytes) as f64,
        );
        crate::metrics::registry().set(
            "vrt_host_do_wal_pinned_bytes",
            &[("tenant", info.tenant), ("class", info.class_key)],
            pinned_span as f64,
        );
        false
    }

    /// Cut pending frames into segments. `CommitBoundary` emits at
    /// the last commit ≤ cap each pass (RPO ≈ tick cadence);
    /// `Tail` additionally emits a mid-txn remainder (departing
    /// flush — the Final image covers it anyway).
    fn cut_segments(&self, info: &TailInfo, d: &mut DbTail, stem: &str, mode: Cut) {
        loop {
            if d.pending.is_empty() {
                return;
            }
            // `seg_cap`: never emit a segment the decoder would
            // reject (SEG_MAX_BYTES) even when cfg is looser.
            let seg_cap = self
                .cfg
                .max_segment_bytes
                .min(seg::SEG_MAX_BYTES as u64 - (128 << 10));
            let mut last_commit: Option<usize> = None;
            let mut commit_bytes = 0u64;
            let mut bytes = 0u64;
            let mut cap_idx = d.pending.len();
            for (i, f) in d.pending.iter().enumerate() {
                bytes += f.data.len() as u64 + wal::WAL_FRAME_HDR as u64;
                if f.commit_db_size != 0 {
                    last_commit = Some(i);
                    commit_bytes = bytes;
                }
                if bytes > seg_cap {
                    cap_idx = i + 1;
                    break;
                }
            }
            let take = if let Some(ci) = last_commit {
                if commit_bytes <= seg_cap {
                    ci + 1
                } else {
                    cap_idx
                }
            } else if d.pending_bytes > seg_cap * 4 || (mode == Cut::Tail && d.pending_bytes > 0) {
                // No commit yet — only cut mid-txn when oversized
                // (segment bound) or flushing a partial tail.
                cap_idx
            } else {
                return;
            };
            let take = take.min(cap_idx).min(d.pending.len());
            if take == 0 {
                return;
            }
            let frames: Vec<SegFrame> = d.pending.drain(..take).collect();
            let segn = Segment {
                database_id: database_id(info.class_key, stem),
                epoch: info.epoch,
                lsn: d.lsn,
                prev_lsn: d.lsn.saturating_sub(1),
                page_size: d.page_size,
                wal_begin: d.emitted,
                frames,
            };
            let nbytes: u64 = segn
                .frames
                .iter()
                .map(|f| f.data.len() as u64 + wal::WAL_FRAME_HDR as u64)
                .sum();
            d.pending_bytes -= nbytes.min(d.pending_bytes);
            d.emitted += segn.frames.len() as u64;
            let key = seg_key(info.class_key, stem, info.epoch, d.labeled_seq, d.lsn);
            d.lsn += 1;
            let fbytes: u64 = segn.frames.iter().map(|f| f.data.len() as u64).sum();
            let body = seg::encode(&segn);
            d.queue.push_back((key, body, fbytes));
            d.queue_bytes += nbytes;
            if d.queue_bytes > self.cfg.max_pending_bytes {
                return; // cap check in caller breaks it
            }
        }
    }

    /// FIFO upload of queued segments; bounded attempts each and an
    /// optional per-pass wall budget (P1-2: the tail shares the
    /// supervisor actor thread — leftover work resumes next tick).
    ///
    /// `PreconditionFailed` (key exists) is split: identical body
    /// means a same-epoch re-emit (host restart re-parsed the same
    /// WAL) → already uploaded; a DIFFERENT body is unexplainable
    /// under our naming (unverifiable) → break.
    fn drain_queue(&self, info: &TailInfo, d: &mut DbTail, deadline: Option<std::time::Instant>) {
        while let Some((key, body, _fb)) = d.queue.front() {
            if let Some(dl) = deadline {
                if std::time::Instant::now() >= dl {
                    return;
                }
            }
            let nbytes = body.len() as u64;
            let mut ok = false;
            for attempt in 0..self.cfg.upload_attempts {
                // Budget covers retry sleeps too — a blocking store
                // must not stretch the tick past the configured cap.
                if let Some(dl) = deadline {
                    if attempt > 0 && std::time::Instant::now() >= dl {
                        break;
                    }
                }
                match vrt_objstore::block_on_store(self.store.put_if_none_match(key, body)) {
                    Ok(_) => {
                        ok = true;
                        break;
                    }
                    Err(StoreError::PreconditionFailed) => {
                        // Same-epoch restart re-emit? Verify content.
                        let existing = vrt_objstore::block_on_store(self.store.get(key));
                        match existing {
                            Ok(Some(obj)) if obj.body == *body => {
                                ok = true;
                            }
                            Ok(Some(_)) => {
                                self.mark_broken(info, d, "segment content conflict");
                                return;
                            }
                            _ => {}
                        }
                        break;
                    }
                    Err(e) => {
                        crate::metrics::registry().inc(
                            "vrt_host_do_wal_upload_failures_total",
                            &[("tenant", info.tenant)],
                            1.0,
                        );
                        if attempt + 1 < self.cfg.upload_attempts {
                            std::thread::sleep(std::time::Duration::from_millis(
                                50 * (attempt as u64 + 1),
                            ));
                        }
                        let _ = e;
                    }
                }
            }
            if !ok {
                return; // keep queue ordered — retry next tick
            }
            let (_, body, fb) = d.queue.pop_front().unwrap();
            d.queue_bytes = d.queue_bytes.saturating_sub(nbytes);
            let _ = body;
            // MIG-17d-r2: accrue uploaded frame bytes to the
            // current label's chain (replay counts the same unit).
            // The trigger itself is evaluated at CLASS level in
            // `take_needs_baseline` — `replay_max_bytes` bounds the
            // Σ over all stems, not any single db.
            d.chain_bytes = d.chain_bytes.saturating_add(fb);
            crate::metrics::registry().inc(
                "vrt_host_do_wal_segments_uploaded_total",
                &[("tenant", info.tenant)],
                1.0,
            );
        }
    }

    /// Bounded synchronous write (anchor objects) — fail closed.
    fn put_durable(&self, key: &str, body: &[u8]) -> bool {
        for attempt in 0..self.cfg.upload_attempts {
            match vrt_objstore::block_on_store(self.store.put_if_none_match(key, body)) {
                Ok(_) => return true,
                Err(StoreError::PreconditionFailed) => {
                    // P1-1: an existing anchor is OK only with the
                    // SAME body — a different {B,E} is unexplainable
                    // → fail closed.
                    match vrt_objstore::block_on_store(self.store.get(key)) {
                        Ok(Some(o)) if o.body == body => return true,
                        _ => return false,
                    }
                }
                Err(_) => {
                    if attempt + 1 < self.cfg.upload_attempts {
                        std::thread::sleep(std::time::Duration::from_millis(
                            50 * (attempt as u64 + 1),
                        ));
                    }
                }
            }
        }
        false
    }

    fn mark_broken(&self, info: &TailInfo, d: &mut DbTail, why: &'static str) {
        d.needs_baseline = true;
        // MIG-17c-r2: a broken stream is a data-risk event — the gap
        // between last-uploaded and the next baseline is real RPO.
        // Flag urgency so the next poll bypasses the backoff floor.
        d.urgent_baseline = true;
        // P0-A: seal the current label FOREVER — its chain's tail is
        // unknowable (unobserved frames may be lost), so appending
        // post-rearm frames under it would fake continuity and
        // produce a page-level mix on replay.
        d.sealed.insert(d.labeled_seq);
        d.pin = None;
        tracing::error!(
            tenant = %info.tenant, class = %info.class_key, db = %d.db_path.display(),
            why, "do wal stream broken — rebaseline via snapshot"
        );
    }

    /// Bounded drain of every pending/cuttable segment under the
    /// tail's CURRENT labels — sole caller is `flush` (departing).
    /// (`start` does NOT drain: old-epoch segments are unadoptable
    /// at the new epoch by namespacing; remnant frames fold into
    /// the db on the next checkpoint and ride the fresh tail's
    /// immediate first baseline — MIG-17c-r2.) Best-effort; a
    /// broken stem re-arms `wants_baseline`.
    fn drain_pending(
        &self,
        t: &mut ClassTail,
        now: u64,
        deadline: std::time::Instant,
    ) {
        self.discover_dbs(t, now);
        let info = TailInfo {
            tenant: &t.scope.tenant,
            class_key: &t.scope.class_key,
            epoch: t.epoch,
        };
        let stems: Vec<String> = t.dbs.keys().cloned().collect();
        let mut broke = false;
        for stem in stems {
            if std::time::Instant::now() >= deadline {
                break;
            }
            if let Some(d) = t.dbs.get_mut(&stem) {
                // one more read pass then cut+drain
                broke |= self.tail_db(&info, d, &stem, now, Some(deadline), false);
                if d.can_emit() {
                    self.cut_segments(&info, d, &stem, Cut::Tail);
                    self.drain_queue(&info, d, Some(deadline));
                }
            }
        }
        t.wants_baseline |= broke;
    }

    /// Departing flush: cut all pending (incl. partial tail) and
    /// drain with a wall budget — ALWAYS under the db's CURRENT
    /// label (the Final image is the handoff authority; its own seq
    /// adopts no chain — I3/Final rule). Best-effort.
    pub fn flush(&self, scope: &PlacementScope) {
        if !self.cfg.enabled {
            return;
        }
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(self.cfg.flush_max_ms);
        let mut g = self.inner.lock().unwrap();
        let Some(t) = g.get_mut(&scope.class_key) else {
            return;
        };
        let now = self.now();
        self.drain_pending(t, now, deadline);
    }

    /// Stop tailing a class entirely (undeploy / epoch loss).
    pub fn stop(&self, class_key: &str) {
        self.inner.lock().unwrap().remove(class_key);
    }

    /// W6-r4: insert/merge a retired-stem mark under the hard cap —
    /// keeps the mark gauge fresh and, at the cap, evicts the LEAST
    /// dangerous entry (smallest max label = furthest behind the live
    /// seq) while pulling the class toward a new baseline. Never
    /// evicts the stem being inserted (its delete just happened under
    /// the live label — it is the most dangerous entry by
    /// construction).
    fn mark_retired_stem(&self, t: &mut ClassTail, stem: &str, labels: HashSet<u64>) {
        t.retired_stems
            .entry(stem.to_string())
            .or_default()
            .extend(labels);
        if t.retired_stems.len() > MAX_RETIRED_STEMS {
            let victim = t
                .retired_stems
                .iter()
                .filter(|(k, _)| k.as_str() != stem)
                .min_by_key(|(_, seals)| seals.iter().max().copied().unwrap_or(0))
                .map(|(k, seals)| (k.clone(), seals.iter().max().copied().unwrap_or(0)));
            if let Some((victim, victim_max)) = victim {
                t.retired_stems.remove(&victim);
                // Fail-closed: the evicted mark's label-max joins the
                // ceiling so a rediscovery inside the evicted range
                // becomes `evicted_suspect` instead of emitting blind.
                t.retired_stem_seal_ceiling =
                    Some(t.retired_stem_seal_ceiling.unwrap_or(0).max(victim_max));
                t.wants_baseline = true;
                crate::metrics::registry().inc(
                    "vrt_host_do_wal_retired_evicted_total",
                    &[("tenant", t.scope.tenant.as_str())],
                    1.0,
                );
                tracing::warn!(
                    tenant = %t.scope.tenant,
                    class = %t.scope.class_key,
                    victim = %victim,
                    cap = MAX_RETIRED_STEMS,
                    "wal retired-stem mark evicted at cap — rediscovery below the seal ceiling holds for a baseline"
                );
            }
        }
        crate::metrics::registry().set(
            "vrt_host_do_wal_retired_stems",
            &[
                ("tenant", t.scope.tenant.as_str()),
                ("class", t.scope.class_key.as_str()),
            ],
            t.retired_stems.len() as f64,
        );
    }

    /// W6: retire a db whose actor file was deleted (workflows
    /// `delete()` → loopback storage unlink). Seals the live label
    /// and demands a fresh baseline — the same seal rules as
    /// `mark_broken`, minus the error log (an admin delete is
    /// expected lifecycle, not corruption):
    ///
    /// - pending/queue frames of the deleted incarnation can never
    ///   be adopted (post-delete anchors only reference labels from
    ///   later baselines), so they are dropped eagerly;
    /// - a recreated instance reuses the same filename. Keeping the
    ///   DbTail tombstone means the new incarnation rearms at the
    ///   NEXT snapshot begin and emits under a NEW label — never
    ///   under a label the old incarnation already used, which would
    ///   collide on `put_if_none_match` LSN keys in the store.
    /// - Idempotent: absent class/stem → no-op.
    pub fn retire_db(&self, class_key: &str, stem: &str) {
        if !self.cfg.enabled {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let Some(c) = g.get_mut(class_key) else {
            return;
        };
        // W6-r3: mark the stem at class level (entry presence = this
        // stem was deleted under this epoch) — if the tail is later
        // reaped before a recreate lands, the mark survives the drop
        // and the rediscovered tail inherits it in `discover_dbs`.
        // The mark+count fire even when no live tail exists yet
        // (`wal.start` registers the class before the first tick
        // discovers its dbs — reclaim runs in that window).
        let dead = c.dbs.get(stem).map(dead_labels).unwrap_or_default();
        self.mark_retired_stem(c, stem, dead);
        c.wants_baseline = true;
        crate::metrics::registry().inc(
            "vrt_host_do_wal_retired_total",
            &[("tenant", c.scope.tenant.as_str()), ("class", class_key)],
            1.0,
        );
        tracing::info!(
            tenant = %c.scope.tenant,
            class = %class_key,
            db = %stem,
            "do wal db retired — storage deleted; rebaseline on next begin"
        );
        let Some(d) = c.dbs.get_mut(stem) else { return };
        d.sealed.insert(d.labeled_seq);
        d.needs_baseline = true;
        d.anchored = false;
        d.caught_up = false;
        d.pin = None;
        d.pending.clear();
        d.pending_bytes = 0;
        d.queue.clear();
        d.queue_bytes = 0;
    }

    /// Bounded GC: delete segments/anchors strictly older than the
    /// committed baseline seq UNDER OUR EPOCH, or produced by an
    /// older epoch — both unreachable for restore. Segments of a
    /// NEWER epoch (`ep > our_epoch`) belong to a successor that
    /// fenced us out and are NEVER touched: seq labels are wall-clock
    /// (RV-R24 — a new holder on a slower clock emits tail seqs below
    /// our `committed_seq`; a bare `sq < committed_seq` test would
    /// delete them and leave the restored chain with missing frames).
    /// Never deletes the live tail (seq == committed or the in-flight
    /// label, epoch == ours).
    ///
    /// `holds_lease` is evaluated before any delete — the epoch guard
    /// alone cannot stop a stale holder from racing deletes against a
    /// same-epoch re-commit window, so a caller that lost the lease
    /// must skip the whole pass.
    pub fn gc_segments(
        &self,
        class_key: &str,
        our_epoch: u64,
        committed_seq: u64,
        holds_lease: impl Fn() -> bool,
    ) {
        if !self.cfg.enabled || !holds_lease() {
            return;
        }
        let prefix = wal_prefix(class_key);
        // RV-R28 #3: resumable sweep — a bounded store page ends at
        // a lexicographic bound; a fresh `list()` next round would
        // restart at the head and keys beyond the bound are never
        // reached. Resume from the last examined key instead; when a
        // pass completes the cursor resets so the next round sweeps
        // the head again.
        let cursor = {
            let g = self.inner.lock().unwrap();
            g.get(class_key).and_then(|t| t.gc_cursor.clone())
        };
        let Ok(page) = vrt_objstore::block_on_store(
            self.store.list_after(&prefix, cursor.as_deref()),
        ) else {
            return;
        };
        let mut n = 0usize;
        let mut last_examined: Option<String> = None;
        for e in page.entries {
            last_examined = Some(e.key.clone());
            if n >= self.cfg.gc_max_per_tick {
                break;
            }
            if let Some((_, ep, sq, _)) = parse_wal_key(&e.key) {
                if ep < our_epoch || (ep == our_epoch && sq < committed_seq) {
                    // RV-R24 re-check: the lease may have flipped
                    // mid-sweep — a stale holder must not delete a
                    // successor's stream.
                    if holds_lease()
                        && vrt_objstore::block_on_store(self.store.delete(&e.key)).is_ok()
                    {
                        n += 1;
                    }
                }
            }
        }
        // More to sweep (store page truncated, or the delete budget
        // cut the pass short) → resume at the last examined key;
        // a complete pass resets to the head for the next round.
        let next_cursor = if page.truncated || n >= self.cfg.gc_max_per_tick {
            last_examined
        } else {
            None
        };
        if let Some(t) = self.inner.lock().unwrap().get_mut(class_key) {
            t.gc_cursor = next_cursor;
        }
    }
}

/// Cut mode for `cut_segments`.
#[derive(PartialEq)]
enum Cut {
    /// Emit through the last commit boundary.
    CommitBoundary,
    /// Additionally flush a mid-txn partial tail (departing only —
    /// replay still truncates at the last commit).
    Tail,
}

/// Parse `{stem}/{epoch16}/{seq16}/({lsn16}.seg|anchor)` — returns
/// (stem, epoch, seq, lsn; lsn == u64::MAX marks an anchor object).
fn parse_wal_key(key: &str) -> Option<(String, u64, u64, u64)> {
    let mut parts = key.rsplitn(4, '/');
    let last = parts.next()?;
    let lsn = if last == "anchor" {
        u64::MAX
    } else {
        u64::from_str_radix(last.strip_suffix(".seg")?, 16).ok()?
    };
    let seq = u64::from_str_radix(parts.next()?, 16).ok()?;
    let epoch = u64::from_str_radix(parts.next()?, 16).ok()?;
    let stem = parts.next()?.rsplit('/').next()?.to_string();
    Some((stem, epoch, seq, lsn))
}

fn read_at(p: &Path, off: u64, len: usize) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(p)?;
    let mut buf = vec![0u8; len];
    let n = f.read_at(&mut buf, off)?;
    buf.truncate(n);
    Ok(buf)
}

/// Restore-time replay: apply `writer_epoch` segments on top of the
/// restored baseline in `class_dir`.
///
/// Chain rules (§6.11, I1–I3):
/// - `Final` baselines adopt NO chain — the image is complete (the
///   writer had already exited); likewise a baseline committed by a
///   different epoch (`snap.epoch != writer_epoch`) adopts nothing —
///   segments under another epoch's seq are unprovable;
/// - genesis (`baseline = None`) requires the epoch's `{epoch}/0/`
///   chain to contain the full stream; a stem that listed segments
///   but applied zero frames is a dead chain → `Err` (fail closed —
///   an empty db is NOT an acceptable outcome for a db that had
///   committed data);
/// - non-genesis stems need their per-db `anchor` object — `B`
///   (the image coverage boundary recorded at the backup's begin);
///   the chain must contain index `B` contiguously and replay
///   applies only frames `>= B` through the last commit;
/// - `database_id`/`epoch` re-verified on decode; post-apply
///   `integrity_check` must return true or the staged file is
///   dropped (baseline preserved, never half-applied).
pub fn replay_for_restore(
    store: &Arc<dyn ObjectStore>,
    class_key: &str,
    class_dir: &Path,
    baseline: Option<&SnapRef>,
    writer_epoch: u64,
    replay_max_bytes: u64,
    tenant: &str,
) -> Result<ReplayReport, WalErr> {
    let mut report = ReplayReport::default();
    if let Some(s) = baseline {
        // I1: segments are adoptable only on top of a baseline the
        // LAST writer committed itself — and a Final image is already
        // complete (writer dead → nothing could follow it).
        if s.kind == crate::do_placement_store::SnapKind::Final || s.epoch != writer_epoch {
            report.epoch_mismatch = true;
            if s.epoch != writer_epoch {
                tracing::warn!(
                    tenant = %tenant, class = %class_key,
                    snap_epoch = s.epoch, writer_epoch,
                    "do wal replay skipped — baseline epoch != writer epoch (I1)"
                );
            }
            return Ok(report);
        }
    }
    let snap_seq = baseline.map(|s| s.seq).unwrap_or(0);
    let genesis = baseline.is_none();
    // Stems = union of local *.sqlite files and stems present in the
    // wal listing (a genesis db may exist only as segments).
    let mut stems: BTreeMap<String, bool> = BTreeMap::new();
    if let Ok(rd) = std::fs::read_dir(class_dir) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().to_string();
            if let Some(s) = n.strip_suffix(".sqlite") {
                stems.insert(s.to_string(), true);
            }
        }
    }
    let wpfx = wal_prefix(class_key);
    // RV-R8 #7 + RV-R28 #3: stem discovery enumerates the per-stem
    // "directories" — NOT a flat list of the whole class prefix. A
    // class can hold >65k WAL keys (every stem × epoch × seq —
    // including not-yet-GC'd old segments): a flat list truncates on
    // S3 and either silently drops instances or fails every restore
    // forever. `list_children` folds keys into `{wpfx}{stem}/`
    // CommonPrefixes — bounded by the STEM count, not the segment
    // count. Err or truncation still fails closed.
    let stem_page = match vrt_objstore::block_on_store(store.list_children(&wpfx, "/")) {
        Ok(page) => page,
        Err(e) => return Err(WalErr::Store(e)),
    };
    if stem_page.truncated {
        return Err(WalErr::Store(vrt_objstore::StoreError::unavailable(
            true,
            "wal stem listing truncated",
        )));
    }
    for cp in &stem_page.common_prefixes {
        // `{wpfx}{stem}/` — a nested remainder would mean a '/'
        // inside a stem name, which the key layout never produces:
        // ignore rather than replay an unprovable chain.
        let Some(rest) = cp.strip_prefix(&wpfx) else {
            continue;
        };
        let stem = rest.strip_suffix('/').unwrap_or(rest);
        if stem.is_empty() || stem.contains('/') {
            continue;
        }
        stems.insert(stem.to_string(), true);
    }
    for e in &stem_page.entries {
        // Objects directly under `do/{class}/wal/` — none expected;
        // keep the old stem-discovery semantics for any odd layout.
        if let Some((stem, ep, sq, lsn)) = parse_wal_key(&e.key) {
            if ep == writer_epoch && sq == snap_seq && lsn != u64::MAX {
                stems.insert(stem, true);
            }
        }
    }
    let mut total_bytes = 0u64;
    for stem in stems.keys() {
        report.stems_seen += 1;
        // P0-C: genesis segments (`{epoch}/0/`) are NEVER adoptable
        // — a fresh WAL's ckpt_seq==0 cannot prove completeness
        // (WAL delete+recreate restarts it), so `{}/0` chains are
        // inherently unverifiable. Warn + metric; never apply, never
        // refuse launch — flag-off parity (empty dir → empty db).
        if genesis {
            report.genesis_segments_skipped += 1;
            tracing::warn!(
                tenant = %tenant, class = %class_key, stem = %stem,
                "do wal replay: genesis-era segments present but never adoptable — serving empty image"
            );
            continue;
        }
        // RV-R28 #3: the chain listing is scoped to THIS stem's own
        // `{stem}/{epoch}/{seq}/` prefix — bounded by that chain's
        // segment count, never the whole class's key space.
        let pfx = format!(
            "{}{stem}/{writer_epoch:016x}/{snap_seq:016x}/",
            wal_prefix(class_key)
        );
        let page = match vrt_objstore::block_on_store(store.list(&pfx)) {
            Ok(v) => v,
            Err(e) => return Err(WalErr::Store(e)),
        };
        if page.truncated {
            // 链前缀不完整 → 无法证明链连续性 → fail closed。
            return Err(WalErr::Store(vrt_objstore::StoreError::unavailable(
                true,
                "wal chain listing truncated",
            )));
        }
        let mut entries = page.entries;
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        // Decode + chain-verify; keep (idx, frame) pairs so the
        // `>= image_frame` prefix strip is exact.
        let mut frames: Vec<(u64, SegFrame)> = Vec::new();
        let mut expect_lsn = 1u64;
        let mut expect_wal_begin: Option<u64> = None;
        let mut page_size = 0u32;
        let applied_before = report.segs_applied;
        let mut listed = 0usize;
        let mut stem_corrupt = 0usize;
        let mut stem_capped = 0usize;
        let mut cap_at: Option<(u64, u64)> = None; // (lsn, total_bytes)
        // MIG-17c-r2: is the break INTERIOR corruption or a torn
        // tail? `later(idx)` = the listing still contains chain segs
        // for this `{epoch}/{seq}` label after `idx` — the gap is
        // interior (repairable corruption → reject), not a tail.
        let later = |at: usize| -> bool {
            entries[at + 1..].iter().any(|x| {
                matches!(
                    parse_wal_key(&x.key),
                    Some((_, ep, sq, l))
                        if ep == writer_epoch && sq == snap_seq && l != u64::MAX
                )
            })
        };
        for (ei, e) in entries.iter().enumerate() {
            let Some((_, ep, sq, lsn)) = parse_wal_key(&e.key) else {
                continue;
            };
            if ep != writer_epoch || sq != snap_seq || lsn == u64::MAX {
                continue;
            }
            listed += 1;
            if lsn != expect_lsn {
                // Missing head/interior seg — the entry itself IS a
                // later chain seg, so the hole is always interior.
                stem_corrupt += 1;
                break;
            }
            let Some(obj) =
                vrt_objstore::block_on_store(store.get(&e.key)).map_err(WalErr::Store)?
            else {
                if later(ei) {
                    stem_corrupt += 1;
                } else {
                    report.truncated = true;
                }
                break;
            };
            let s = match seg::decode(&obj.body) {
                Ok(s) => s,
                Err(se) => {
                    if later(ei) {
                        stem_corrupt += 1;
                        tracing::error!(
                            tenant = %tenant, class = %class_key, stem = %stem,
                            lsn, err = %se, key = %e.key,
                            "do wal replay: interior segment undecodable — stem rejected"
                        );
                    } else {
                        report.truncated = true;
                    }
                    break;
                }
            };
            if s.epoch != writer_epoch
                || s.database_id != database_id(class_key, stem)
                || s.lsn != lsn
            {
                if later(ei) {
                    stem_corrupt += 1;
                    tracing::error!(
                        tenant = %tenant, class = %class_key, stem = %stem,
                        lsn, seg_epoch = s.epoch, seg_db = %s.database_id,
                        "do wal replay: interior segment failed identity re-verify — stem rejected"
                    );
                } else {
                    report.truncated = true;
                }
                break;
            }
            if let Some(wb) = expect_wal_begin {
                if s.wal_begin != wb {
                    if later(ei) {
                        stem_corrupt += 1;
                        tracing::error!(
                            tenant = %tenant, class = %class_key, stem = %stem,
                            lsn, seg_wal_begin = s.wal_begin, expect_wal_begin = wb,
                            "do wal replay: interior chain link broken — stem rejected"
                        );
                    } else {
                        report.truncated = true;
                    }
                    break;
                }
            }
            let b: u64 = s.frames.iter().map(|f| f.data.len() as u64).sum();
            if total_bytes + b > replay_max_bytes {
                // MIG-17c-r3: a configured bound, not corruption —
                // counted separately and ALWAYS rejects the stem:
                // the capped-out seg is committed data whether it
                // sits mid-chain or on the tail.
                stem_capped += 1;
                cap_at = Some((lsn, total_bytes));
                break;
            }
            total_bytes += b;
            expect_wal_begin = Some(s.wal_begin + s.frames.len() as u64);
            expect_lsn += 1;
            report.segs_applied += 1;
            if page_size == 0 {
                page_size = s.page_size;
            }
            for (i, f) in s.frames.into_iter().enumerate() {
                frames.push((s.wal_begin + i as u64, f));
            }
        }
        // Loss visibility: listed-but-unadopted segments for THIS
        // stem (segs_applied is cumulative — diff per stem).
        // Interior-corrupt segs count separately, not as skips.
        report.segs_skipped += listed
            .saturating_sub(report.segs_applied - applied_before)
            .saturating_sub(stem_corrupt + stem_capped);
        report.segs_corrupt += stem_corrupt;
        report.cap_exceeded += stem_capped;
        if stem_capped > 0 {
            // Not repairable by retry alone — needs a bigger
            // `doWalReplayMaxBytes` or §13 chain abandonment.
            report.stems_rejected += 1;
            let (lsn, tb) = cap_at.unwrap_or((0, total_bytes));
            crate::metrics::registry().inc(
                "vrt_host_do_wal_replay_cap_exceeded_total",
                &[("tenant", tenant)],
                1.0,
            );
            tracing::error!(
                tenant = %tenant, class = %class_key, stem = %stem,
                lsn, total_bytes = tb, cap = replay_max_bytes,
                "do wal replay: replay_max_bytes exceeded — stem rejected (needs larger cap or chain abandonment; retry alone won't converge)"
            );
            continue;
        }
        if stem_corrupt > 0 {
            // Fail closed: the hole means the chain's tail is
            // unprovable — adopting the intact prefix would silently
            // drop the missing range, and retry-after-repair is the
            // only honest path. The stem is rejected wholesale (its
            // collected prefix is NOT applied).
            report.stems_rejected += 1;
            crate::metrics::registry().inc(
                "vrt_host_do_wal_replay_segs_corrupt_total",
                &[("tenant", tenant)],
                stem_corrupt as f64,
            );
            tracing::error!(
                tenant = %tenant, class = %class_key, stem = %stem,
                corrupt = stem_corrupt,
                "do wal replay: interior corrupt segment(s) — stem rejected (fail closed)"
            );
            continue;
        }
        // RV-R28 #3: a stem with zero segments at our (epoch,
        // snap_seq) label has nothing adoptable — skip before the
        // anchor read so dead/foreign stems never spam warns or GETs.
        if listed == 0 {
            continue;
        }
        // I3/P0-B: anchor v2 {B,E} — B = boundary below which the
        // image provably covers; E = WAL end at image-taken (≥ M).
        let (image_frame, image_end) = {
            let akey = anchor_key(class_key, stem, writer_epoch, snap_seq);
            match vrt_objstore::block_on_store(store.get(&akey)) {
                Ok(Some(o)) if o.body.len() == 17 && o.body[0] == 2 => (
                    u64::from_be_bytes(o.body[1..9].try_into().unwrap()),
                    u64::from_be_bytes(o.body[9..17].try_into().unwrap()),
                ),
                Ok(_) => {
                    // A chain EXISTS at our label but the anchor is
                    // missing/legacy → cannot prove coverage →
                    // serve the image alone (I3). Acceptable: the
                    // image is self-consistent and the chain's
                    // coverage boundary is unprovable — adopting
                    // unbound frames could regress the image.
                    crate::metrics::registry().inc(
                        "vrt_host_do_wal_replay_anchor_missing_total",
                        &[("tenant", tenant)],
                        1.0,
                    );
                    tracing::warn!(
                        tenant = %tenant, class = %class_key, stem = %stem,
                        epoch = writer_epoch, seq = snap_seq,
                        "do wal replay: no v2 anchor for baseline seq — stem skipped"
                    );
                    continue;
                }
                Err(e) => return Err(WalErr::Store(e)),
            }
        };
        // I3: drop every frame below the image boundary, then require
        // the chain to actually CONTAIN the boundary — a chain that
        // starts past `image_frame` leaves a hole the image does not
        // cover → never applied.
        frames.retain(|(idx, _)| *idx >= image_frame);
        if frames.is_empty() {
            continue;
        }
        if frames[0].0 > image_frame {
            // The image covers up to `image_frame` and the chain
            // starts ABOVE it — the `(image_frame, chain_start)`
            // range exists in neither → serving the image alone is
            // the only honest restore (I3). Metric + warn expose it.
            report.segs_skipped += 1;
            crate::metrics::registry().inc(
                "vrt_host_do_wal_replay_hole_total",
                &[("tenant", tenant)],
                1.0,
            );
            tracing::warn!(
                tenant = %tenant, class = %class_key, stem = %stem,
                image_frame, chain_start = frames[0].0,
                "do wal replay: chain starts above image boundary — serving baseline"
            );
            continue;
        }
        // Apply only through the last commit boundary — a torn tail
        // is never installed.
        let Some(lc) = frames.iter().rposition(|(_, f)| f.commit_db_size != 0) else {
            continue;
        };
        // P0-B (I3): the image is a FLOOR. The applied cut must reach
        // ≥ E (WAL end at image-taken ≥ image mxFrame) — a chain cut
        // short inside (B,E) would revert pages the image already
        // holds a NEWER version of → apply NOTHING for this stem.
        // Frame idx = wal_begin + position (0-based); E = count of
        // consumed frames = the NEXT unconsumed idx. Coverage is
        // complete iff the last commit's idx reaches E-1 — i.e.
        // `lc + 1 < E` means the chain stopped below the last
        // consumed frame → truncated inside (B,E) → never apply.
        if frames[lc].0 + 1 < image_end {
            report.segs_skipped += listed;
            tracing::warn!(
                tenant = %tenant, class = %class_key, stem = %stem,
                last_commit = frames[lc].0, image_end,
                "do wal replay: chain cut short of image end — serving baseline"
            );
            continue;
        }
        // Commit position = the applied stream's last commit idx.
        report.last_commit_frame = frames[lc].0;
        frames.truncate(lc + 1);
        report.frames_applied += frames.len();
        // Stage: copy existing db (or start empty), apply, verify,
        // swap — baseline stays intact on any failure.
        let dst = class_dir.join(format!("{stem}.sqlite"));
        let stage = class_dir.with_extension(format!("wal-replay-{stem}-tmp"));
        let _ = std::fs::remove_dir_all(&stage);
        std::fs::create_dir_all(&stage).map_err(WalErr::Io)?;
        let staged = stage.join(format!("{stem}.sqlite"));
        if dst.exists() {
            // The image may carry a `-wal` (backup-style restore).
            // A raw copy would drop it → fold it into the main file
            // first; a busy/corrupt journal means the image state is
            // unverifiable → skip apply, serve the image untouched.
            if Path::new(&format!("{}-wal", dst.display())).exists() {
                match vrt_sql::wal::fold_journal(&dst) {
                    Ok(true) => {}
                    r => {
                        // MIG-17c: an unfoldable image journal means
                        // the base image can't be proven — rejecting
                        // is retryable (busy can clear), skipping is
                        // silent loss of every selected frame.
                        report.stems_rejected += 1;
                        match r {
                            Ok(false) => tracing::error!(
                                tenant = %tenant, class = %class_key, stem = %stem,
                                reason = "busy",
                                "do wal replay: destination image journal won't fold — stem rejected"
                            ),
                            Err(e) => tracing::error!(
                                tenant = %tenant, class = %class_key, stem = %stem,
                                err = %e,
                                "do wal replay: destination image journal won't fold — stem rejected"
                            ),
                            Ok(true) => unreachable!(),
                        }
                        continue;
                    }
                }
            }
            std::fs::copy(&dst, &staged).map_err(WalErr::Io)?;
        }
        let apply_seg = Segment {
            database_id: String::new(),
            epoch: writer_epoch,
            lsn: 0,
            prev_lsn: 0,
            page_size,
            wal_begin: 0,
            frames: frames.into_iter().map(|(_, f)| f).collect(),
        };
        let res = (|| -> Result<(), WalErr> {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&staged)
                .map_err(WalErr::Io)?;
            seg::apply_frames(&mut f, &apply_seg)?;
            // P0-4: `Ok(false)` IS a failure — `?` alone only
            // surfaces the io error, not a failed check.
            if !vrt_sql::wal::integrity_ok(&staged).map_err(|e| WalErr::Integrity(e.to_string()))? {
                return Err(WalErr::Integrity(format!(
                    "integrity_check failed for {stem}"
                )));
            }
            Ok(())
        })();
        match res {
            Ok(()) => {
                // A stray `-wal`/`-shm` beside dst (image remnant or
                // hot-journal junk) would hot-recover ITS frames over
                // the swapped file → page-level revert. The swapped
                // main file is already complete; drop journal files
                // before the swap (P0-B hardening).
                let _ = std::fs::remove_file(format!("{}-wal", dst.display()));
                let _ = std::fs::remove_file(format!("{}-shm", dst.display()));
                std::fs::rename(&staged, &dst).map_err(WalErr::Io)?;
                report.stems_applied += 1;
            }
            Err(e) => {
                // MIG-17c: counted — the caller converts this into a
                // typed restore failure (marker stays, retry re-runs)
                // instead of silently serving the older image.
                report.stems_rejected += 1;
                tracing::error!(
                    tenant = %tenant, class = %class_key, stem = %stem, err = %e,
                    "do wal replay rejected db — stem counted rejected"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&stage);
    }
    Ok(report)
}

// ---------- WAL1-r5 unit tests (typed fault injection) ----------
#[cfg(test)]
mod r5_tests {
    use super::*;
    use crate::do_placement::PlacementScope;
    use crate::do_placement_store::SnapRef;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use vrt_objstore::{MemoryStore, ObjectStore};

    const CK: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn tmpdir(tag: &str) -> PathBuf {
        let d = Path::new("target/tmp-wal1r5").join(tag);
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }
    fn clock() -> (Arc<AtomicU64>, Arc<dyn Fn() -> u64 + Send + Sync>) {
        let t = Arc::new(AtomicU64::new(1_000_000));
        let t2 = t.clone();
        (t, Arc::new(move || t2.load(Ordering::SeqCst)))
    }
    fn cfg() -> WalCfg {
        WalCfg {
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
            rebaseline_chain_bytes: 1 << 29,
            replay_max_bytes: 1 << 30,
            baseline_backoff_max_ms: 300_000,
        }
    }
    fn scope(dir: PathBuf) -> PlacementScope {
        scope_as(dir, "team", CK)
    }
    /// F1: tests asserting on the GLOBAL metric registry need an
    /// unshared (tenant, class) pair — parallel tests reusing "team"/CK
    /// would pollute each other's deltas.
    fn scope_as(dir: PathBuf, tenant: &str, class_key: &str) -> PlacementScope {
        PlacementScope {
            tenant: tenant.into(),
            worker: "w".into(),
            class_key: class_key.into(),
            class_dir: dir,
        }
    }
    fn mk_db(dir: &Path, stem: &str) -> rusqlite::Connection {
        let p = dir.join(format!("{stem}.sqlite"));
        let c = rusqlite::Connection::open(&p).unwrap();
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
    fn get_val(db: &Path, k: &str) -> Option<String> {
        let c = rusqlite::Connection::open(db).unwrap();
        c.query_row("SELECT v FROM t WHERE k=?1", [k], |r| r.get(0))
            .ok()
    }
    fn img_copy(src: &Path, img: &Path, stem: &str) {
        std::fs::create_dir_all(img).unwrap();
        std::fs::copy(
            src.join(format!("{stem}.sqlite")),
            img.join(format!("{stem}.sqlite")),
        )
        .unwrap();
        let w = src.join(format!("{stem}.sqlite-wal"));
        if w.exists() {
            std::fs::copy(&w, img.join(format!("{stem}.sqlite-wal"))).unwrap();
        }
    }
    fn img_restore(img: &Path, dst: &Path, stem: &str) {
        std::fs::create_dir_all(dst).unwrap();
        std::fs::copy(
            img.join(format!("{stem}.sqlite")),
            dst.join(format!("{stem}.sqlite")),
        )
        .unwrap();
        let w = img.join(format!("{stem}.sqlite-wal"));
        if w.exists() {
            std::fs::copy(&w, dst.join(format!("{stem}.sqlite-wal"))).unwrap();
        }
    }
    fn anchor_exists(store: &Arc<dyn ObjectStore>, stem: &str, seq: u64) -> bool {
        let k = anchor_key(CK, stem, 5, seq);
        vrt_objstore::block_on_store(store.get(&k))
            .unwrap()
            .is_some()
    }
    fn snapref(seq: u64) -> SnapRef {
        SnapRef {
            manifest_key: "m".into(),
            epoch: 5,
            seq,
            files: 1,
            bytes: 0,
            at_ms: 0,
            kind: crate::do_placement_store::SnapKind::Periodic,
        }
    }

    /// P0-1: pin acquire failure during image_taken → NO anchor,
    /// needs_baseline; replay serves the image only.
    #[test]
    fn r5_pin_acquire_fail_during_image_taken_fails_closed() {
        let dir = tmpdir("pinfail");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 42);
        put(&conn, "b", "2"); // post-begin write → M passes begin
        let img = dir.join("img42");
        img_copy(&cls, &img, "obj1");
        // Drop the held pin, then make every acquire fail.
        {
            let mut g = rep.inner.lock().unwrap();
            let d = g
                .get_mut(&sc.class_key)
                .unwrap()
                .dbs
                .get_mut("obj1")
                .unwrap();
            d.pin = None;
        }
        rep.hooks.pin_fail.store(64, Ordering::SeqCst);
        rep.on_snapshot_image_taken(&sc.class_key, 42);
        rep.on_snapshot_committed(&sc.class_key, 42);
        assert!(!anchor_exists(&store, "obj1", 42), "anchor must not land");
        // Replay on baseline-42 serves the image only.
        let dst = dir.join("takeover");
        img_restore(&img, &dst, "obj1");
        let r =
            replay_for_restore(&store, CK, &dst, Some(&snapref(42)), 5, 1 << 30, "team").unwrap();
        assert_eq!(r.segs_applied, 0);
        assert_eq!(get_val(&dst.join("obj1.sqlite"), "a").as_deref(), Some("1"));
        conn.close().ok();
    }

    /// P0-2: a read error mid-scan during image_taken → same fail
    /// closed (caught_up must not retain a stale value).
    #[test]
    fn r5_frame_read_error_during_image_taken_fails_closed() {
        let dir = tmpdir("readfail");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 42);
        put(&conn, "b", "2");
        let img = dir.join("img42");
        img_copy(&cls, &img, "obj1");
        rep.hooks.frame_read_fail.store(64, Ordering::SeqCst);
        rep.on_snapshot_image_taken(&sc.class_key, 42);
        rep.on_snapshot_committed(&sc.class_key, 42);
        assert!(!anchor_exists(&store, "obj1", 42));
        let dst = dir.join("takeover");
        img_restore(&img, &dst, "obj1");
        let r =
            replay_for_restore(&store, CK, &dst, Some(&snapref(42)), 5, 1 << 30, "team").unwrap();
        assert_eq!(r.segs_applied, 0);
        conn.close().ok();
    }

    /// P0-3: idle db (len == read_pos, nothing new) anchors fine —
    /// caught_up must be set by the "no new data" path.
    #[test]
    fn r5_idle_db_anchors() {
        let dir = tmpdir("idleok");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key); // parses a, caught_up=true, pinned
        rep.on_snapshot_begin(&sc.class_key, 42);
        let img = dir.join("img42");
        img_copy(&cls, &img, "obj1"); // no writes in the window
        rep.on_snapshot_image_taken(&sc.class_key, 42);
        rep.on_snapshot_committed(&sc.class_key, 42);
        assert!(anchor_exists(&store, "obj1", 42), "idle db must anchor");
        conn.close().ok();
    }

    /// P0-4: pin released+reacquired between begin and image_taken →
    /// pin_gen mismatch → fail closed (never E < M).
    #[test]
    fn r5_pin_reacquire_between_begin_and_image_fails_closed() {
        let dir = tmpdir("reacq");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 42);
        put(&conn, "b", "2");
        let img = dir.join("img42");
        img_copy(&cls, &img, "obj1");
        // Simulate idle release: pin dropped; image_taken's tail_db
        // reacquires (real acquire) → pin_gen bump → mismatch.
        {
            let mut g = rep.inner.lock().unwrap();
            let d = g
                .get_mut(&sc.class_key)
                .unwrap()
                .dbs
                .get_mut("obj1")
                .unwrap();
            d.pin = None;
        }
        rep.on_snapshot_image_taken(&sc.class_key, 42);
        rep.on_snapshot_committed(&sc.class_key, 42);
        assert!(
            !anchor_exists(&store, "obj1", 42),
            "release+reacquire across the window must fail closed"
        );
        conn.close().ok();
    }

    /// P0-4 (success branch): pin released BEFORE begin → begin
    /// re-arms and records pin_gen → writes → anchor lands, E ≥ M.
    #[test]
    fn r5_unpinned_before_begin_anchors() {
        let dir = tmpdir("unpinb");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        {
            let mut g = rep.inner.lock().unwrap();
            let d = g
                .get_mut(&sc.class_key)
                .unwrap()
                .dbs
                .get_mut("obj1")
                .unwrap();
            d.pin = None; // released before begin
        }
        rep.on_snapshot_begin(&sc.class_key, 42); // re-arms
        put(&conn, "b", "2"); // post-begin commit → M
        let img = dir.join("img42");
        img_copy(&cls, &img, "obj1");
        put(&conn, "c", "3"); // post-image commit
        rep.on_snapshot_image_taken(&sc.class_key, 42);
        rep.on_snapshot_committed(&sc.class_key, 42);
        assert!(
            anchor_exists(&store, "obj1", 42),
            "re-pinned db must anchor"
        );
        rep.tick(&sc.class_key);
        // Replay: image(a,b) + c.
        let dst = dir.join("takeover");
        img_restore(&img, &dst, "obj1");
        let r =
            replay_for_restore(&store, CK, &dst, Some(&snapref(42)), 5, 1 << 30, "team").unwrap();
        assert_eq!(r.segs_applied, 1);
        let db = dst.join("obj1.sqlite");
        assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
        assert_eq!(
            get_val(&db, "c").as_deref(),
            Some("3"),
            "post-image commit must apply"
        );
        conn.close().ok();
    }

    /// P1: consecutive uncovered baselines back off exponentially
    /// (capped), reset on full coverage; first request immediate.
    #[test]
    fn r5_baseline_backoff_escalates_and_resets() {
        struct FailAnchor {
            inner: MemoryStore,
        }
        #[async_trait::async_trait]
        impl ObjectStore for FailAnchor {
            async fn get(
                &self,
                k: &str,
            ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
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
                if k.ends_with("/anchor") && k.contains("obj2") {
                    return Err(vrt_objstore::StoreError::unavailable(true, "down"));
                }
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
        let dir = tmpdir("backoff");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let c1 = mk_db(&cls, "obj1");
        let c2 = mk_db(&cls, "obj2");
        put(&c1, "a", "1");
        put(&c2, "x", "9");
        let store: Arc<dyn ObjectStore> = Arc::new(FailAnchor {
            inner: MemoryStore::new(),
        });
        let (tclk, ck) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        // First request immediate.
        assert!(rep.take_needs_baseline(&sc.class_key));
        // Baseline 1: obj2 anchor fails → uncovered → backoff 60s.
        rep.on_snapshot_begin(&sc.class_key, 1);
        rep.on_snapshot_image_taken(&sc.class_key, 1);
        rep.on_snapshot_committed(&sc.class_key, 1);
        let backoff = |r: &DoWalReplicator| -> u64 {
            r.inner.lock().unwrap()[&sc.class_key].baseline_backoff_ms
        };
        assert_eq!(backoff(&rep), 60_000);
        tclk.fetch_add(31_000, Ordering::SeqCst);
        assert!(
            !rep.take_needs_baseline(&sc.class_key),
            "60s backoff must gate"
        );
        tclk.fetch_add(30_000, Ordering::SeqCst);
        assert!(rep.take_needs_baseline(&sc.class_key));
        // Baseline 2: uncovered again → 120s.
        rep.on_snapshot_begin(&sc.class_key, 2);
        rep.on_snapshot_image_taken(&sc.class_key, 2);
        rep.on_snapshot_committed(&sc.class_key, 2);
        assert_eq!(backoff(&rep), 120_000);
        tclk.fetch_add(61_000, Ordering::SeqCst);
        assert!(!rep.take_needs_baseline(&sc.class_key));
        tclk.fetch_add(60_000, Ordering::SeqCst);
        assert!(rep.take_needs_baseline(&sc.class_key));
        // Baseline 3: uncovered → 240s; 4th would cap at 300s.
        rep.on_snapshot_begin(&sc.class_key, 3);
        rep.on_snapshot_image_taken(&sc.class_key, 3);
        rep.on_snapshot_committed(&sc.class_key, 3);
        assert_eq!(backoff(&rep), 240_000);
        // Covered baseline (fail only obj2 anchors — swap store? the
        // store still fails obj2 anchors… instead: remove obj2 from
        // tracking by deleting its file? simplest: drop the FailAnchor
        // by making it succeed — use a new rep on the same store?
        // FailAnchor always fails obj2 — simulate coverage by
        // removing obj2's db file then discovering: simpler to just
        // verify reset via a covered commit with a fresh replicator
        // is not possible. Instead: emulate full coverage by
        // clearing obj2's needs_baseline + writing its anchor body
        // directly so anchor_written contains the seq — do it via a
        // second MemoryStore-free path: mark db anchored manually.
        {
            let mut g = rep.inner.lock().unwrap();
            let t = g.get_mut(&sc.class_key).unwrap();
            let d = t.dbs.get_mut("obj2").unwrap();
            d.anchor_written.insert(4);
            d.needs_baseline = false;
        }
        rep.on_snapshot_begin(&sc.class_key, 4);
        rep.on_snapshot_image_taken(&sc.class_key, 4);
        rep.on_snapshot_committed(&sc.class_key, 4);
        assert_eq!(backoff(&rep), 30_000, "full coverage resets backoff");
        c1.close().ok();
        c2.close().ok();
    }

    // ---------- WAL1-r6 unit tests ----------

    fn tmpdir6(tag: &str) -> PathBuf {
        let d = Path::new("target/tmp-wal1r6").join(tag);
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Count VALID frames of the CURRENT wal generation (scan stops
    /// at the first salt/checksum mismatch — old-gen residue is not
    /// counted, exactly like tail_db's frontier rule).
    fn gen_frames(walp: &Path) -> u64 {
        let b = std::fs::read(walp).unwrap();
        if b.len() < wal::WAL_HDR {
            return 0;
        }
        let hdr = wal::wal_header(&b[..wal::WAL_HDR]).unwrap();
        let seed = (
            u32::from_be_bytes(b[24..28].try_into().unwrap()),
            u32::from_be_bytes(b[28..32].try_into().unwrap()),
        );
        let (_v, heads, _ck) = wal::scan_frames(
            &b[wal::WAL_HDR..],
            0,
            hdr.page_size(),
            hdr.cksum_be(),
            (hdr.salt1, hdr.salt2),
            seed,
        );
        heads.len() as u64
    }

    fn d_state(rep: &DoWalReplicator, ck: &str, stem: &str) -> (u64, bool, Vec<u64>) {
        let g = rep.inner.lock().unwrap();
        let d = &g[ck].dbs[stem];
        (
            d.emitted,
            d.needs_baseline,
            d.sealed.iter().copied().collect(),
        )
    }

    /// P0-1: idle release → RESTART → new gen lands EXACTLY on old
    /// `emitted` → begin must NOT mask the reset via rearm — the old
    /// label is sealed via the re-tail's header check; rollback and
    /// crash-before-commit both stay safe (no new-gen frames ever
    /// appended under the old label).
    #[test]
    fn r6_idle_release_reset_tail_seals_rollback_and_crash_safe() {
        for crash in [false, true] {
            let dir = tmpdir6(if crash { "xgen2" } else { "xgen" });
            let cls = dir.join("cls");
            std::fs::create_dir_all(&cls).unwrap();
            let conn = mk_db(&cls, "obj1");
            put(&conn, "a", "1");
            let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
            let (_t, ck2) = clock();
            let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck2);
            let sc = scope(cls.clone());
            rep.start(&sc, 5, 0);
            rep.tick(&sc.class_key);
            rep.on_snapshot_begin(&sc.class_key, 5);
            let img5 = dir.join("img5");
            img_copy(&cls, &img5, "obj1"); // baseline-5 image has a
            rep.on_snapshot_image_taken(&sc.class_key, 5);
            rep.on_snapshot_committed(&sc.class_key, 5);
            put(&conn, "b", "2");
            rep.tick(&sc.class_key); // b emitted under 5
            let (n, _, _) = d_state(&rep, &sc.class_key, "obj1");
            assert!(n >= 2, "a+b frames consumed");
            // Idle-release the pin, then RESTART + write so the NEW
            // generation holds exactly n valid frames — the bug's
            // coincidence: parsed == old emitted.
            {
                let mut g = rep.inner.lock().unwrap();
                let d = g
                    .get_mut(&sc.class_key)
                    .unwrap()
                    .dbs
                    .get_mut("obj1")
                    .unwrap();
                d.pin = None;
            }
            let (busy, _log, _ckpt): (i64, i64, i64) = conn
                .query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .unwrap();
            assert_eq!(busy, 0, "restart checkpoint must complete (pin released)");
            // RESTART resets the wal but the salt bump happens on the
            // NEXT WRITE (verified with sqlite3). One put starts the
            // new generation; the file keeps its length (old-gen
            // residue beyond the new valid region — r4 PoC).
            let walp = cls.join("obj1.sqlite-wal");
            put(&conn, "x0", "v"); // first write of the NEW generation
                                   // Pad to exactly n with one-frame writes (PRAGMA
                                   // user_version touches only page 1 → exactly 1 frame).
            let mut uv = 0u32;
            while gen_frames(&walp) < n {
                conn.pragma_update(None, "user_version", uv).unwrap();
                uv += 1;
                assert!(gen_frames(&walp) <= n, "pragma must add exactly one frame");
            }
            assert_eq!(gen_frames(&walp), n, "new gen must land on emitted");
            {
                let b = std::fs::read(&walp).unwrap();
                let hdr = wal::wal_header(&b[..wal::WAL_HDR]).unwrap();
                let g = rep.inner.lock().unwrap();
                let d = &g[&sc.class_key].dbs["obj1"];
                eprintln!(
                    "DBG file salt={:?} d.salt={:?} anchored={} pin={} needs={} labeled={}",
                    (hdr.salt1, hdr.salt2),
                    d.salt,
                    d.anchored,
                    d.pin.is_some(),
                    d.needs_baseline,
                    d.labeled_seq
                );
            }
            // begin(T): fix re-tails under 5 first — header detects
            // the salt change → mark_broken → 5 sealed → rearm.
            rep.on_snapshot_begin(&sc.class_key, 7);
            let (_e, _needs, sealed) = d_state(&rep, &sc.class_key, "obj1");
            assert!(
                sealed.contains(&5),
                "old label must be sealed, got {sealed:?}"
            );
            put(&conn, "c", "3"); // post-reset write
            if crash {
                drop(rep); // takeover replays under 5 — never under 7
            } else {
                rep.on_snapshot_failed(&sc.class_key);
                // Post-reset writes keep flowing; nothing may ever
                // be appended under sealed 5.
                rep.tick(&sc.class_key);
                rep.on_snapshot_begin(&sc.class_key, 9);
                rep.on_snapshot_image_taken(&sc.class_key, 9);
                rep.on_snapshot_committed(&sc.class_key, 9);
                drop(rep);
            }
            // Replay under baseline 5 = image(a) + old chain(b).
            // x*/c (post-reset) MUST NOT appear.
            let dst = dir.join("takeover");
            img_restore(&img5, &dst, "obj1");
            let r = replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team")
                .unwrap();
            let db = dst.join("obj1.sqlite");
            assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
            assert_eq!(
                get_val(&db, "b").as_deref(),
                Some("2"),
                "pre-reset chain must apply"
            );
            assert_eq!(get_val(&db, "x0"), None, "post-reset data must not appear");
            assert_eq!(get_val(&db, "c"), None);
            assert!(r.stems_applied <= 1);
            conn.close().ok();
        }
    }

    /// P0-2: same-generation idle release with queued-but-unuploaded
    /// segments — begin's re-tail must drain the queue under the OLD
    /// label so the chain stays complete (no gap-bridging).
    #[test]
    fn r6_idle_release_queued_old_label_drains_at_begin() {
        let dir = tmpdir6("queued");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck2) = clock();
        let mut c = cfg();
        c.tick_upload_budget_ms = 0; // tick drains nothing → queue backs up
        let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), ck2);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 5);
        let img5 = dir.join("img5");
        img_copy(&cls, &img5, "obj1");
        rep.on_snapshot_image_taken(&sc.class_key, 5);
        rep.on_snapshot_committed(&sc.class_key, 5);
        put(&conn, "b", "2");
        rep.tick(&sc.class_key); // cut under 5, drain skipped (budget 0)
        {
            let g = rep.inner.lock().unwrap();
            let d = &g[&sc.class_key].dbs["obj1"];
            assert!(!d.queue.is_empty(), "queue must hold an un-uploaded seg");
        }
        {
            let mut g = rep.inner.lock().unwrap();
            let d = g
                .get_mut(&sc.class_key)
                .unwrap()
                .dbs
                .get_mut("obj1")
                .unwrap();
            d.pin = None; // idle release — SAME generation (no reset)
        }
        // begin(T): re-tail under 5 → re-acquire, salt same, scan,
        // cut+drain under 5 → queue flushed BEFORE label switch.
        rep.on_snapshot_begin(&sc.class_key, 7);
        rep.on_snapshot_failed(&sc.class_key); // rollback to 5
        put(&conn, "c", "3");
        rep.tick(&sc.class_key); // emits under 5 continuing lsn
        rep.flush(&sc); // tick budget is 0 — drain the queued seg

        // Replay under 5 = image(a) + b + c — chain complete, no gap.
        let dst = dir.join("takeover");
        img_restore(&img5, &dst, "obj1");
        let _r =
            replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team").unwrap();
        let db = dst.join("obj1.sqlite");
        assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
        assert_eq!(
            get_val(&db, "b").as_deref(),
            Some("2"),
            "queued seg must have drained"
        );
        assert_eq!(
            get_val(&db, "c").as_deref(),
            Some("3"),
            "rollback continuation must land"
        );
        conn.close().ok();
    }

    /// r6/r7: keys of uploaded segs for a stem (any label).
    fn seg_keys(store: &Arc<dyn ObjectStore>, stem: &str) -> Vec<String> {
        let page = vrt_objstore::block_on_store(store.list(&format!("do/{CK}/wal/{stem}/")))
            .unwrap();
        assert!(!page.truncated, "test stem listing must be complete");
        page.entries
            .iter()
            .filter(|e| e.key.ends_with(".seg"))
            .map(|e| e.key.clone())
            .collect()
    }

    /// r7: decode anchor v2 body → (B, E).
    fn anchor_be(store: &Arc<dyn ObjectStore>, stem: &str, seq: u64) -> (u64, u64) {
        let o = vrt_objstore::block_on_store(store.get(&anchor_key(CK, stem, 5, seq)))
            .unwrap()
            .expect("anchor must exist");
        assert_eq!(o.body[0], 2, "anchor v2");
        let b = u64::from_be_bytes(o.body[1..9].try_into().unwrap());
        let e = u64::from_be_bytes(o.body[9..17].try_into().unwrap());
        (b, e)
    }

    /// §1: re-tail acquire failure → seal old label → rearm.
    /// pin_fail=1: only the re-tail acquire fails, rearm succeeds.
    /// pin_fail=2: rearm's acquire also fails → fail closed.
    #[test]
    fn r6_retail_acquire_fail_seals_then_rearms() {
        for rearm_ok in [true, false] {
            let dir = tmpdir6(if rearm_ok { "acqok" } else { "acqfail" });
            let cls = dir.join("cls");
            std::fs::create_dir_all(&cls).unwrap();
            let conn = mk_db(&cls, "obj1");
            put(&conn, "a", "1");
            let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
            let (_t, ck2) = clock();
            let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck2);
            let sc = scope(cls.clone());
            rep.start(&sc, 5, 0);
            rep.tick(&sc.class_key);
            rep.on_snapshot_begin(&sc.class_key, 5);
            let img5 = dir.join("img5");
            img_copy(&cls, &img5, "obj1");
            rep.on_snapshot_image_taken(&sc.class_key, 5);
            rep.on_snapshot_committed(&sc.class_key, 5);
            put(&conn, "b", "2");
            rep.tick(&sc.class_key); // b emitted+uploaded under 5
                                     // Idle release, then same-gen writes (no reset).
            {
                let mut g = rep.inner.lock().unwrap();
                let d = g
                    .get_mut(&sc.class_key)
                    .unwrap()
                    .dbs
                    .get_mut("obj1")
                    .unwrap();
                d.pin = None;
            }
            put(&conn, "c", "3");
            let walp = cls.join("obj1.sqlite-wal");
            // Inject: re-tail acquire fails (1); optionally rearm too (2).
            rep.hooks
                .pin_fail
                .store(if rearm_ok { 1 } else { 2 }, Ordering::SeqCst);
            rep.on_snapshot_begin(&sc.class_key, 7);
            let (emitted, needs, sealed) = d_state(&rep, &sc.class_key, "obj1");
            assert!(sealed.contains(&5), "old label must be sealed: {sealed:?}");
            if !rearm_ok {
                // Fail-closed branch (:517-520): could not anchor —
                // needs_baseline, labeled stays 5, no anchor.
                assert!(needs, "rearm acquire failure must fail closed");
                let g = rep.inner.lock().unwrap();
                let d = &g[&sc.class_key].dbs["obj1"];
                assert!(d.pin.is_none());
                drop(g);
                assert!(!anchor_exists(&store, "obj1", 7));
                conn.close().ok();
                continue;
            }
            // Rearm succeeded: dropped parsed frames, emitted=parsed.
            assert_eq!(emitted, gen_frames(&walp), "emitted must equal parsed");
            {
                let g = rep.inner.lock().unwrap();
                let d = &g[&sc.class_key].dbs["obj1"];
                assert!(d.pending.is_empty());
                assert_eq!(d.pin_gen_at_begin, d.pin_gen);
                assert!(d.pin.is_some());
                assert!(!needs);
            }
            put(&conn, "d", "4");
            let img7 = dir.join("img7");
            img_copy(&cls, &img7, "obj1"); // image has a,b,c,d
            let m = gen_frames(&walp);
            rep.on_snapshot_image_taken(&sc.class_key, 7);
            assert!(anchor_exists(&store, "obj1", 7));
            let (_b, e) = anchor_be(&store, "obj1", 7);
            assert!(e >= m, "E={e} must reach image end M={m}");
            rep.on_snapshot_failed(&sc.class_key); // rollback to 5
            let n5 = seg_keys(&store, "obj1")
                .iter()
                .filter(|k| k.contains("/0000000000000005/"))
                .count();
            let (_, needs, _) = d_state(&rep, &sc.class_key, "obj1");
            assert!(needs, "rollback onto sealed label must needs_baseline");
            put(&conn, "e", "5");
            rep.tick(&sc.class_key); // must NOT append under sealed 5
            let n5b = seg_keys(&store, "obj1")
                .iter()
                .filter(|k| k.contains("/0000000000000005/"))
                .count();
            assert_eq!(n5, n5b, "no segment may be appended under sealed 5");
            // Replay under 5 = image(a) + pre-seal chain(b) only.
            let dst = dir.join("takeover5");
            img_restore(&img5, &dst, "obj1");
            let _r = replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team")
                .unwrap();
            let db5 = dst.join("obj1.sqlite");
            assert_eq!(get_val(&db5, "a").as_deref(), Some("1"));
            assert_eq!(get_val(&db5, "b").as_deref(), Some("2"));
            assert_eq!(get_val(&db5, "c"), None, "post-seal data must not appear");
            assert_eq!(get_val(&db5, "d"), None);
            // Next committed round: replay under the NEW label = all.
            rep.on_snapshot_begin(&sc.class_key, 9);
            let img9 = dir.join("img9");
            img_copy(&cls, &img9, "obj1"); // has a,b,c,d,e
            rep.on_snapshot_image_taken(&sc.class_key, 9);
            rep.on_snapshot_committed(&sc.class_key, 9);
            assert!(anchor_exists(&store, "obj1", 9));
            let dst9 = dir.join("takeover9");
            img_restore(&img9, &dst9, "obj1");
            let _r9 = replay_for_restore(&store, CK, &dst9, Some(&snapref(9)), 5, 1 << 30, "team")
                .unwrap();
            let db9 = dst9.join("obj1.sqlite");
            for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
                assert_eq!(get_val(&db9, k).as_deref(), Some(v), "{k} missing");
            }
            conn.close().ok();
        }
    }

    /// §2: wal header damage while unpinned — (a) truncate < 32B,
    /// (b) bad magic, (c) shrink >32B same salt. Each must break +
    /// seal (never fake continuity); rollback appends nothing.
    #[test]
    fn r6_idle_release_header_damage_breaks_and_seals() {
        for (tag, mode) in [
            ("trunc32", "trunc"),
            ("badmagic", "magic"),
            ("shrink", "shrink"),
        ] {
            let dir = tmpdir6(tag);
            let cls = dir.join("cls");
            std::fs::create_dir_all(&cls).unwrap();
            let conn = mk_db(&cls, "obj1");
            put(&conn, "a", "1");
            let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
            let (_t, ck2) = clock();
            let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck2);
            let sc = scope(cls.clone());
            rep.start(&sc, 5, 0);
            rep.tick(&sc.class_key);
            rep.on_snapshot_begin(&sc.class_key, 5);
            let img5 = dir.join("img5");
            img_copy(&cls, &img5, "obj1");
            rep.on_snapshot_image_taken(&sc.class_key, 5);
            rep.on_snapshot_committed(&sc.class_key, 5);
            put(&conn, "b", "2");
            rep.tick(&sc.class_key);
            {
                let mut g = rep.inner.lock().unwrap();
                let d = g
                    .get_mut(&sc.class_key)
                    .unwrap()
                    .dbs
                    .get_mut("obj1")
                    .unwrap();
                d.pin = None;
            }
            let walp = cls.join("obj1.sqlite-wal");
            match mode {
                // Direct byte ops: sqlite may self-heal an open wal,
                // so we damage the FILE — the point under test is
                // tail_db's header detection, not sqlite recovery.
                "trunc" => std::fs::File::options()
                    .write(true)
                    .open(&walp)
                    .unwrap()
                    .set_len(16)
                    .unwrap(),
                "magic" => {
                    use std::io::{Seek, SeekFrom, Write};
                    let mut f = std::fs::File::options().write(true).open(&walp).unwrap();
                    f.seek(SeekFrom::Start(0)).unwrap();
                    f.write_all(&[0xde, 0xad, 0xbe, 0xef]).unwrap();
                }
                "shrink" => std::fs::File::options()
                    .write(true)
                    .open(&walp)
                    .unwrap()
                    .set_len(48)
                    .unwrap(),
                _ => unreachable!(),
            }
            rep.on_snapshot_begin(&sc.class_key, 7);
            let (_e, needs1, sealed) = d_state(&rep, &sc.class_key, "obj1");
            assert!(sealed.contains(&5), "{mode}: old label must be sealed");
            rep.on_snapshot_failed(&sc.class_key);
            let n5 = seg_keys(&store, "obj1")
                .iter()
                .filter(|k| k.contains("/0000000000000005/"))
                .count();
            let (_e, needs2, _s2) = d_state(&rep, &sc.class_key, "obj1");
            assert!(needs1 || needs2, "{mode}: must fail closed at some point");
            put(&conn, "c", "3");
            rep.tick(&sc.class_key);
            let n5b = seg_keys(&store, "obj1")
                .iter()
                .filter(|k| k.contains("/0000000000000005/"))
                .count();
            assert_eq!(n5, n5b, "{mode}: nothing appended under sealed 5");
            let dst = dir.join("takeover");
            img_restore(&img5, &dst, "obj1");
            let _r = replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team")
                .unwrap();
            let db = dst.join("obj1.sqlite");
            assert_eq!(get_val(&db, "a").as_deref(), Some("1"), "{mode}");
            assert_eq!(get_val(&db, "b").as_deref(), Some("2"), "{mode}");
            assert_eq!(
                get_val(&db, "c"),
                None,
                "{mode}: post-damage data must not appear"
            );
            conn.close().ok();
        }
    }

    /// W6: `retire_db` — seal + rebaseline on file delete; a
    /// recreated instance under the same stem emits under a NEW
    /// label only, never colliding with the old incarnation's
    /// segments. Replay at the pre-delete seq still serves the old
    /// chain (point-in-time); post-delete seq has the fresh db.
    #[test]
    fn w6_retire_db_seals_then_recreate_rearms_new_label() {
        let dir = tmpdir6("retire");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck2) = clock();
        let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), ck2);
        let sc = scope(cls.clone());
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 5);
        let img5 = dir.join("img5");
        img_copy(&cls, &img5, "obj1");
        rep.on_snapshot_image_taken(&sc.class_key, 5);
        rep.on_snapshot_committed(&sc.class_key, 5);
        put(&conn, "b", "2");
        rep.tick(&sc.class_key); // b uploaded under 5
        conn.close().ok();

        // Engine delete: retire the tail, then unlink the files.
        rep.retire_db(&sc.class_key, "obj1");
        {
            let g = rep.inner.lock().unwrap();
            let d = &g[&sc.class_key].dbs["obj1"];
            assert!(d.needs_baseline);
            assert!(d.sealed.contains(&5));
            assert!(!d.anchored);
            assert!(d.pin.is_none());
            assert!(d.pending.is_empty() && d.queue.is_empty());
        }
        let segs5_before = seg_keys(&store, "obj1");
        for suf in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", cls.join("obj1.sqlite").display(), suf));
        }
        rep.tick(&sc.class_key); // file gone — nothing may append
        let segs5_after = seg_keys(&store, "obj1");
        assert_eq!(segs5_before, segs5_after, "no upload after retire");

        // Recreate same id: a FRESH db file at the same path (new
        // salt), carrying only the new incarnation's writes.
        let conn2 = mk_db(&cls, "obj1");
        put(&conn2, "c", "3");
        rep.on_snapshot_begin(&sc.class_key, 7);
        let (emitted, needs, _s) = d_state(&rep, &sc.class_key, "obj1");
        let walp = cls.join("obj1.sqlite-wal");
        assert_eq!(emitted, gen_frames(&walp), "rearmed to fresh wal");
        assert!(!needs, "rearm anchored the new incarnation");
        let img7 = dir.join("img7");
        img_copy(&cls, &img7, "obj1");
        rep.on_snapshot_image_taken(&sc.class_key, 7);
        rep.on_snapshot_committed(&sc.class_key, 7);
        assert!(anchor_exists(&store, "obj1", 7));
        put(&conn2, "d", "4");
        rep.tick(&sc.class_key);
        conn2.close().ok();

        // Replay at the pre-delete seq: old chain serves a+b (PITR —
        // the instance existed at snapshot 5's image time).
        let dst5 = dir.join("takeover5");
        img_restore(&img5, &dst5, "obj1");
        let _r5 =
            replay_for_restore(&store, CK, &dst5, Some(&snapref(5)), 5, 1 << 30, "team").unwrap();
        let db5 = dst5.join("obj1.sqlite");
        assert_eq!(get_val(&db5, "a").as_deref(), Some("1"));
        assert_eq!(get_val(&db5, "b").as_deref(), Some("2"));
        assert_eq!(
            get_val(&db5, "c"),
            None,
            "new incarnation never leaks into old seq"
        );

        // Replay at the post-delete seq: the NEW incarnation's chain
        // only — old a/b are gone (clean new instance).
        let dst7 = dir.join("takeover7");
        img_restore(&img7, &dst7, "obj1");
        let _r7 =
            replay_for_restore(&store, CK, &dst7, Some(&snapref(7)), 5, 1 << 30, "team").unwrap();
        let db7 = dst7.join("obj1.sqlite");
        assert_eq!(get_val(&db7, "c").as_deref(), Some("3"));
        // `d` was committed AFTER anchor(7)'s image end — the E gate
        // must keep it out of a seq-7 restore.
        assert_eq!(get_val(&db7, "d"), None);
        assert_eq!(get_val(&db7, "a"), None);
        assert_eq!(get_val(&db7, "b"), None);
        // Old label still sealed — nothing appended under 5 since retire.
        let segs5_final = seg_keys(&store, "obj1")
            .iter()
            .filter(|k| k.contains("/0000000000000005/"))
            .count();
        let segs5_pre = segs5_before
            .iter()
            .filter(|k| k.contains("/0000000000000005/"))
            .count();
        assert_eq!(segs5_pre, segs5_final);
    }

    /// W6-r3 P0: the reviewer's hole — reap drops the tail (and its
    /// `sealed`), then a same-stem recreate is discovered at TICK
    /// time under the still-current label S (not inside a begin). At
    /// the NEXT begin the tail is `anchored && !needs_baseline` → the
    /// continuous branch cuts+drains its pending under label S with
    /// no `can_emit` gate — colliding with the OLD incarnation's
    /// segments under S (lsn conflict → mark_broken, or an orphan
    /// under-S chain a restore could splice). The retired stem must
    /// be carried at CLASS level so a rediscovered tail can never
    /// emit under a label from before its deletion.
    #[test]
    fn w6r3_tick_discovered_recreate_never_emits_under_old_label() {
        for variant in ["tick-discovered", "pre-reap-recreate"] {
            let dir = tmpdir6(&format!("w6r3-{variant}"));
            let cls = dir.join("cls");
            std::fs::create_dir_all(&cls).unwrap();
            let conn = mk_db(&cls, "obj1");
            put(&conn, "a", "1");
            let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
            let (_t, ck2) = clock();
            let rep = Arc::new(DoWalReplicator::new(
                cfg(),
                store.clone(),
                dir.join("tmp"),
                ck2,
            ));
            let sc = scope(cls.clone());
            rep.start(&sc, 5, 0);
            rep.tick(&sc.class_key);
            rep.on_snapshot_begin(&sc.class_key, 5);
            let img5 = dir.join("img5");
            img_copy(&cls, &img5, "obj1");
            rep.on_snapshot_image_taken(&sc.class_key, 5);
            rep.on_snapshot_committed(&sc.class_key, 5);
            put(&conn, "b", "2");
            rep.tick(&sc.class_key); // b uploaded under 5
            conn.close().ok();
            let segs5_committed = seg_keys(&store, "obj1")
                .into_iter()
                .filter(|k| k.contains("/0000000000000005/0000000000000005/"))
                .collect::<Vec<_>>();
            assert!(!segs5_committed.is_empty(), "old chain under 5 exists");

            // Tombstone → reclaim with the REAL replicator: retire
            // (seals 5, drops the WalPin) THEN unlink.
            let tomb_dir = dir.join("wf-del");
            let tdir = tomb_dir.join("team").join(CK);
            std::fs::create_dir_all(&tdir).unwrap();
            std::fs::write(
                tdir.join("obj1.json"),
                serde_json::json!({
                    "v": 1, "tenant": "team", "class_key": CK,
                    "hex_id": "obj1", "class_dir": cls,
                })
                .to_string(),
            )
            .unwrap();
            let stats =
                crate::wf_delete::reclaim_tenant("team", &tomb_dir, &Some(rep.clone()), None);
            assert_eq!(stats.reclaimed, 1, "{variant}: {stats:?}");
            assert!(!cls.join("obj1.sqlite").exists(), "file unlinked");

            if variant == "tick-discovered" {
                // Reap runs while the file is GONE — the tail (and
                // its `sealed`) is dropped — THEN the recreate lands.
                rep.tick(&sc.class_key);
                {
                    let g = rep.inner.lock().unwrap();
                    assert!(
                        !g[&sc.class_key].dbs.contains_key("obj1"),
                        "reap must have dropped the tail"
                    );
                }
            }
            // Same-stem recreate — FRESH db file, new incarnation.
            let conn2 = mk_db(&cls, "obj1");
            put(&conn2, "c", "3");
            rep.tick(&sc.class_key); // discovery under still-current label 5
            {
                // The rediscovered tail must carry the retired mark:
                // `needs_baseline` blocks all tailing/emission until
                // the next begin rearms it under a FRESH label.
                let g = rep.inner.lock().unwrap();
                let d = &g[&sc.class_key].dbs["obj1"];
                assert!(
                    d.needs_baseline,
                    "{variant}: rediscovered retired stem must demand a baseline"
                );
                assert!(
                    d.sealed.contains(&5),
                    "{variant}: deleted incarnation's label must stay sealed: {:?}",
                    d.sealed
                );
            }
            rep.on_snapshot_begin(&sc.class_key, 7);
            {
                let g = rep.inner.lock().unwrap();
                let d = &g[&sc.class_key].dbs["obj1"];
                assert_eq!(d.labeled_seq, 7, "{variant}: rearmed under fresh label");
                assert!(d.anchored && !d.needs_baseline, "{variant}: rearm resolved");
            }
            let img7 = dir.join("img7");
            img_copy(&cls, &img7, "obj1");
            rep.on_snapshot_image_taken(&sc.class_key, 7);
            rep.on_snapshot_committed(&sc.class_key, 7);

            // DESIRED: nothing from the new incarnation ever emits
            // under label 5 — the old chain's keys are untouched and
            // no under-5 lsn conflict/break fired.
            let segs5_now: Vec<String> = seg_keys(&store, "obj1")
                .into_iter()
                .filter(|k| k.contains("/0000000000000005/0000000000000005/"))
                .collect();
            assert_eq!(
                segs5_committed.len(),
                segs5_now.len(),
                "{variant}: new incarnation must not emit under old label 5: {segs5_now:?}"
            );
            assert_eq!(stats.failed, 0, "{variant}: reclaim clean");
            assert!(
                anchor_exists(&store, "obj1", 7),
                "{variant}: new incarnation anchors under 7"
            );
            put(&conn2, "d", "4");
            rep.tick(&sc.class_key);
            conn2.close().ok();

            // Restore at baseline 7: only the new incarnation's data —
            // the deleted incarnation's chain can never be spliced in.
            let dst7 = dir.join("takeover7");
            img_restore(&img7, &dst7, "obj1");
            let _r = replay_for_restore(&store, CK, &dst7, Some(&snapref(7)), 5, 1 << 30, "team")
                .unwrap();
            let db7 = dst7.join("obj1.sqlite");
            assert_eq!(get_val(&db7, "c").as_deref(), Some("3"), "{variant}");
            assert_eq!(
                get_val(&db7, "a"),
                None,
                "{variant}: deleted gen never spliced"
            );
            assert_eq!(
                get_val(&db7, "b"),
                None,
                "{variant}: deleted gen never spliced"
            );
        }
    }

    /// W6-r4: `retired_stems` is BOUNDED —
    /// (a) the committed-seq prune drops marks whose every recorded
    ///     emission label is < the durable committed seq (a future
    ///     rediscovery starts on `labeled_seq >= seq_label` ≥ that
    ///     seq by construction, so it still can never emit under the
    ///     dead incarnation's labels — now by construction, the mark
    ///     being gone changes nothing);
    /// (b) the hard cap evicts the least dangerous mark and counts it.
    #[test]
    fn w6r4_retired_stems_bounded_by_prune_and_cap() {
        let dir = tmpdir6("w6r4");
        let cls = dir.join("cls");
        std::fs::create_dir_all(&cls).unwrap();
        let conn = mk_db(&cls, "obj1");
        put(&conn, "a", "1");
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let (_t, ck2) = clock();
        let rep = Arc::new(DoWalReplicator::new(
            cfg(),
            store.clone(),
            dir.join("tmp"),
            ck2,
        ));
        // F1: unique TENANT — this test reads the global
        // `retired_evicted_total` counter (tenant label only) so the
        // label must be owned; the class key stays CK because every
        // store-key helper in this module derives paths from it.
        let sc = scope_as(cls.clone(), "team-w6r4", CK);
        rep.start(&sc, 5, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 5);
        let img5 = dir.join("img5");
        img_copy(&cls, &img5, "obj1");
        rep.on_snapshot_image_taken(&sc.class_key, 5);
        rep.on_snapshot_committed(&sc.class_key, 5);
        put(&conn, "b", "2");
        rep.tick(&sc.class_key); // b under 5
        conn.close().ok();
        let segs5_committed: Vec<String> = seg_keys(&store, "obj1")
            .into_iter()
            .filter(|k| k.contains("/0000000000000005/0000000000000005/"))
            .collect();
        assert!(!segs5_committed.is_empty());

        // Retire via the real tombstone path → unlink → reap lifts
        // the mark to class level.
        let tomb_dir = dir.join("wf-del");
        let tdir = tomb_dir.join("team").join(CK);
        std::fs::create_dir_all(&tdir).unwrap();
        std::fs::write(
            tdir.join("obj1.json"),
            serde_json::json!({
                "v": 1, "tenant": "team", "class_key": CK,
                "hex_id": "obj1", "class_dir": cls,
            })
            .to_string(),
        )
        .unwrap();
        let stats = crate::wf_delete::reclaim_tenant("team", &tomb_dir, &Some(rep.clone()), None);
        assert_eq!(stats.reclaimed, 1, "{stats:?}");
        rep.tick(&sc.class_key); // reap → mark lifted
        {
            let g = rep.inner.lock().unwrap();
            let t = &g[&sc.class_key];
            assert_eq!(
                t.retired_stems.len(),
                1,
                "reap must lift the retired mark: {:?}",
                t.retired_stems
            );
            assert!(
                t.retired_stems["obj1"].contains(&5),
                "mark carries the dead incarnation's emission labels"
            );
        }

        // committed(7) outlives every recorded label (max=5) → prune.
        rep.on_snapshot_begin(&sc.class_key, 7);
        rep.on_snapshot_image_taken(&sc.class_key, 7);
        rep.on_snapshot_committed(&sc.class_key, 7);
        {
            let g = rep.inner.lock().unwrap();
            assert!(
                g[&sc.class_key].retired_stems.is_empty(),
                "mark must prune once committed seq outlives all seals"
            );
        }

        // Post-prune recreate: the tail starts on labeled_seq=7 BY
        // CONSTRUCTION — it can never emit under 5 even without the
        // mark, and its baseline-8 image carries only the new gen.
        let conn2 = mk_db(&cls, "obj1");
        put(&conn2, "c", "3");
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 8);
        let img8 = dir.join("img8");
        img_copy(&cls, &img8, "obj1");
        rep.on_snapshot_image_taken(&sc.class_key, 8);
        rep.on_snapshot_committed(&sc.class_key, 8);
        put(&conn2, "d", "4");
        rep.tick(&sc.class_key);
        conn2.close().ok();
        let segs5_now: Vec<String> = seg_keys(&store, "obj1")
            .into_iter()
            .filter(|k| k.contains("/0000000000000005/0000000000000005/"))
            .collect();
        assert_eq!(
            segs5_committed.len(),
            segs5_now.len(),
            "post-prune recreate still never emits under 5: {segs5_now:?}"
        );
        let dst8 = dir.join("takeover8");
        img_restore(&img8, &dst8, "obj1");
        let _r =
            replay_for_restore(&store, CK, &dst8, Some(&snapref(8)), 5, 1 << 30, "team").unwrap();
        let db8 = dst8.join("obj1.sqlite");
        assert_eq!(get_val(&db8, "c").as_deref(), Some("3"));
        assert_eq!(get_val(&db8, "a"), None, "deleted gen never spliced");

        // Hard cap: retire MAX+16 distinct stems — the map stays ≤
        // cap, every overflow evicts the least dangerous mark and
        // counts it; the class is pulled toward a new baseline.
        let pre_evicted: f64 = crate::metrics::registry()
            .render()
            .lines()
            .filter(|l| {
                l.starts_with("vrt_host_do_wal_retired_evicted_total") && l.contains("team-w6r4")
            })
            .map(|l| l.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
            .sum();
        for i in 0..(super::MAX_RETIRED_STEMS + 16) {
            rep.retire_db(&sc.class_key, &format!("flood{i:05}"));
        }
        {
            let g = rep.inner.lock().unwrap();
            let t = &g[&sc.class_key];
            assert!(
                t.retired_stems.len() <= super::MAX_RETIRED_STEMS,
                "retired_stems must stay bounded: {}",
                t.retired_stems.len()
            );
            assert!(t.wants_baseline, "cap eviction pulls a baseline");
        }
        let evicted: f64 = crate::metrics::registry()
            .render()
            .lines()
            .filter(|l| {
                l.starts_with("vrt_host_do_wal_retired_evicted_total") && l.contains("team-w6r4")
            })
            .map(|l| l.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
            .sum();
        assert_eq!(evicted - pre_evicted, 16.0, "every overflow evicts one");
        let gauge: f64 = crate::metrics::registry()
            .render()
            .lines()
            .filter(|l| {
                l.starts_with("vrt_host_do_wal_retired_stems")
                    && l.contains("team-w6r4")
                    && l.contains(sc.class_key.as_str())
            })
            .map(|l| l.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
            .sum();
        assert_eq!(
            gauge,
            super::MAX_RETIRED_STEMS as f64,
            "gauge mirrors map size"
        );

        // --- cap eviction must stay fail-CLOSED ---
        // The flood marks all carried empty seal sets (vmax=0), so
        // the ceiling is still 0. Force real evictions: refill the
        // table with marks sealed at 100, then overflow once — the
        // victim's label-max folds into `retired_stem_seal_ceiling`.
        {
            let mut g = rep.inner.lock().unwrap();
            let t = g.get_mut(&sc.class_key).unwrap();
            t.retired_stems.clear();
            for i in 0..super::MAX_RETIRED_STEMS {
                rep.mark_retired_stem(t, &format!("victim{i:05}"), [100u64].into_iter().collect());
            }
            rep.mark_retired_stem(t, "victim-overflow", [100u64].into_iter().collect());
            assert_eq!(t.retired_stems.len(), super::MAX_RETIRED_STEMS);
            assert_eq!(
                t.retired_stem_seal_ceiling,
                Some(100),
                "evicted mark's seal max must bound rediscovery"
            );
            assert!(
                !t.retired_stems.contains_key("victim00000")
                    || !t.retired_stems.contains_key("victim-overflow"),
                "one mark was evicted"
            );
        }
        // Recreate an evicted stem while the live label (8) is still
        // inside the evicted range → the tail must turn
        // `evicted_suspect`: sealed at 8, needs_baseline, never
        // emitted under 8 (or ANY label ≤ the ceiling).
        let victim_stem = {
            let g = rep.inner.lock().unwrap();
            let t = &g[&sc.class_key];
            // pick a stem that is NOT in the table (it was evicted)
            (0..=super::MAX_RETIRED_STEMS)
                .map(|i| {
                    if i == super::MAX_RETIRED_STEMS {
                        "victim-overflow".to_string()
                    } else {
                        format!("victim{i:05}")
                    }
                })
                .find(|stem| !t.retired_stems.contains_key(stem))
                .expect("exactly one victim was evicted")
        };
        let vconn = mk_db(&cls, &victim_stem);
        put(&vconn, "vz", "1");
        rep.tick(&sc.class_key);
        {
            let g = rep.inner.lock().unwrap();
            let d = &g[&sc.class_key].dbs[&victim_stem];
            assert!(d.evicted_suspect, "rediscovery below ceiling must suspect");
            assert!(d.needs_baseline);
            assert!(d.sealed.contains(&8), "discovery label sealed");
            assert_eq!(d.labeled_seq, 8);
        }
        // Begins at labels still ≤ the ceiling must NOT rearm the
        // suspect — anchoring could adopt the dead incarnation's
        // unanchored orphan segments on replay.
        for s in 9..=100u64 {
            rep.on_snapshot_begin(&sc.class_key, s);
            rep.on_snapshot_image_taken(&sc.class_key, s);
            rep.on_snapshot_committed(&sc.class_key, s);
            let g = rep.inner.lock().unwrap();
            let d = &g[&sc.class_key].dbs[&victim_stem];
            assert!(
                d.needs_baseline && d.labeled_seq == 8 && !d.anchored,
                "suspect must hold at seq {s} ≤ ceiling"
            );
        }
        // seq 101 outruns every evicted mark → rearm is finally
        // allowed; the new incarnation emits ONLY under 101.
        rep.on_snapshot_begin(&sc.class_key, 101);
        rep.on_snapshot_image_taken(&sc.class_key, 101);
        rep.on_snapshot_committed(&sc.class_key, 101);
        put(&vconn, "vw", "2");
        rep.tick(&sc.class_key);
        {
            let g = rep.inner.lock().unwrap();
            let d = &g[&sc.class_key].dbs[&victim_stem];
            assert_eq!(d.labeled_seq, 101, "rearmed past the ceiling");
            assert!(d.anchor_written.contains(&101));
        }
        vconn.close().ok();
        let vsegs = seg_keys(&store, &victim_stem);
        assert!(!vsegs.is_empty(), "suspect eventually emits under 101");
        assert!(
            vsegs.iter().all(|k| k.contains("/0000000000000065/")),
            "suspect must never emit at-or-below the ceiling: {vsegs:?}"
        );
    }

    /// RV-R8 #7：list 截断 = 可能有未列出的段——回放必须 fail
    /// closed 拒绝，绝不把短前缀当全集、静默丢尾部写入。
    struct TruncWhen {
        inner: MemoryStore,
        /// 只把含该子串的 prefix 标成截断；"" = 全部截断。
        marker: &'static str,
    }
    #[async_trait::async_trait]
    impl ObjectStore for TruncWhen {
        async fn get(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
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
            pfx: &str,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            let mut page = self.inner.list(pfx).await?;
            if pfx.contains(self.marker) {
                page.truncated = true;
            }
            Ok(page)
        }
        async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
            self.inner.delete(k).await
        }
    }

    #[test]
    fn r8_truncated_stem_listing_fails_replay_closed() {
        let dir = tmpdir("r8trunc-stem");
        let dst = dir.join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(TruncWhen {
            inner: MemoryStore::new(),
            marker: "",
        });
        let err = replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team")
            .expect_err("truncated stem listing must fail closed");
        assert!(matches!(err, WalErr::Store(_)), "err: {err:?}");
    }

    #[test]
    fn r8_truncated_chain_listing_fails_replay_closed() {
        let dir = tmpdir("r8trunc-chain");
        let dst = dir.join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        let _conn = mk_db(&dst, "obj1"); // 本地 stem → 走 per-stem 路径
        // v2 anchor {B,E}——让回放推进到 chain list 一步。
        let inner = MemoryStore::new();
        let mut ab = vec![2u8];
        ab.extend_from_slice(&1u64.to_be_bytes());
        ab.extend_from_slice(&2u64.to_be_bytes());
        vrt_objstore::block_on_store(inner.put(&anchor_key(CK, "obj1", 5, 5), &ab)).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(TruncWhen {
            inner,
            marker: "obj1",
        });
        let err = replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team")
            .expect_err("truncated chain listing must fail closed");
        assert!(matches!(err, WalErr::Store(_)), "err: {err:?}");
    }

    /// RV-R28 #3: bounded-page store — `list`/`list_after` stop at
    /// `bound` keys and report `truncated`, while `list_children`
    /// folds the COMPLETE inner list (like S3's CommonPrefixes,
    /// whose bound is the prefix count, not the flat key count).
    /// This is the honest model of an S3 class prefix that has
    /// grown past LIST_KEY_BOUND.
    struct BoundedList {
        inner: MemoryStore,
        bound: usize,
    }
    #[async_trait::async_trait]
    impl ObjectStore for BoundedList {
        async fn get(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
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
            pfx: &str,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            let mut page = self.inner.list(pfx).await?;
            page.truncated = page.entries.len() > self.bound;
            page.entries.truncate(self.bound);
            Ok(page)
        }
        async fn list_after(
            &self,
            pfx: &str,
            after: Option<&str>,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            let mut page = self.inner.list_after(pfx, after).await?;
            page.truncated = page.entries.len() > self.bound;
            page.entries.truncate(self.bound);
            Ok(page)
        }
        async fn list_children(
            &self,
            pfx: &str,
            delim: &str,
        ) -> Result<vrt_objstore::ListChildrenPage, vrt_objstore::StoreError> {
            self.inner.list_children(pfx, delim).await
        }
        async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
            self.inner.delete(k).await
        }
    }

    /// RV-R28 #3: stem discovery must survive a flat class-prefix
    /// listing that cannot enumerate the key space — the delimiter
    /// path sees every stem anyway and replay still applies.
    #[test]
    fn r28_replay_discovers_stems_past_flat_list_bound() {
        let dir = tmpdir("r28-delim");
        let dst = dir.join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        let inner = MemoryStore::new();
        // A v2 anchor + one real-looking seg key for our stem so the
        // chain path is exercised (the seg itself need not decode —
        // this test targets STEM DISCOVERY: the flat list bound must
        // not be what enumerates stems).
        let mut ab = vec![2u8];
        ab.extend_from_slice(&1u64.to_be_bytes());
        ab.extend_from_slice(&2u64.to_be_bytes());
        vrt_objstore::block_on_store(inner.put(&anchor_key(CK, "obj1", 5, 5), &ab)).unwrap();
        // Pad the class prefix with thousands of keys across MANY
        // other stems — the flat list is 8 deep while >8k keys exist.
        for i in 0..80 {
            let key = format!(
                "{}pad{i:05}/0000000000000004/0000000000000001/0000000000000001.seg",
                wal_prefix(CK)
            );
            vrt_objstore::block_on_store(inner.put(&key, b"x")).unwrap();
        }
        let store: Arc<dyn ObjectStore> = Arc::new(BoundedList {
            inner,
            bound: 8,
        });
        // Flat prefix listing IS truncated — the old code would have
        // errored here on the first `list(wpfx)` (or worse, silently
        // dropped stems when the bound hid them).
        let flat = vrt_objstore::block_on_store(store.list(&wal_prefix(CK))).unwrap();
        assert!(flat.truncated, "fixture: flat listing must truncate");
        // Discovery via children still finds our stem → replay runs
        // its chain path (anchor exists, zero segs → skipped cleanly).
        let rep = replay_for_restore(&store, CK, &dst, Some(&snapref(5)), 5, 1 << 30, "team")
            .expect("delimiter stem discovery must not die on the flat bound");
        assert!(rep.stems_seen >= 81, "all stems discovered: {rep:?}");
    }

    /// RV-R28 #3: the GC cursor must sweep old segments beyond one
    /// bounded page across successive rounds — while never deleting
    /// current-epoch or live-tail objects (RV-R24 guard intact).
    #[test]
    fn r28_gc_cursor_converges_past_page_bound() {
        let dir = tmpdir("r28-gc");
        let inner = MemoryStore::new();
        let (_t, clock) = clock();
        let mut c = cfg();
        c.gc_max_per_tick = 1000; // bound is the PAGE size, not deletes
        let sc_key = CK;
        // 30 deletable old-epoch/covered keys + keep-list: current
        // epoch tail and a NEWER epoch segment (RV-R24).
        let mk = |stem: &str, ep: u64, sq: u64, lsn: u64| {
            format!(
                "{}{stem}/{ep:016x}/{sq:016x}/{lsn:016x}.seg",
                wal_prefix(sc_key)
            )
        };
        for i in 0..30 {
            let k = mk(&format!("z{i:03}"), 4, 1, 1);
            vrt_objstore::block_on_store(inner.put(&k, b"x")).unwrap();
        }
        for k in [mk("zlive1", 5, 200, 1), mk("zlive2", 6, 1, 1)] {
            vrt_objstore::block_on_store(inner.put(&k, b"x")).unwrap();
        }
        let store: Arc<dyn ObjectStore> = Arc::new(BoundedList {
            inner,
            bound: 7,
        });
        let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
        rep.start(&scope(dir.join("cls")), 5, 0);
        // Without the cursor each call would delete only the first
        // page's deletable keys and the tail would stay forever.
        for _ in 0..12 {
            rep.gc_segments(sc_key, 5, 200, || true);
        }
        let left = vrt_objstore::block_on_store(store.list(&wal_prefix(sc_key))).unwrap();
        let keys: Vec<String> = left.entries.iter().map(|e| e.key.clone()).collect();
        assert_eq!(
            keys,
            vec![mk("zlive1", 5, 200, 1), mk("zlive2", 6, 1, 1)],
            "cursor must converge over the whole prefix; live keys kept: {keys:?}"
        );
    }
}
