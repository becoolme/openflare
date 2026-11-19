//! P3c-2: epoch-fenced DO snapshot upload / verified restore /
//! lifecycle cleanup over the shared object store (§6.4).
//!
//! Object layout (all under the class's own `do/{uniqueKey}/` prefix —
//! cleanup can never reach another class or tenant):
//!   `do/{uniqueKey}/placement`                    placement record (CAS)
//!   `do/{uniqueKey}/{epoch}/snap-{seq}.json`      snapshot manifest
//!   `do/{uniqueKey}/{epoch}/snap-{seq}/files/…`   snapshot payload
//!
//! Fencing rule: a snapshot is only referenced by `commit_snapshot`
//! while the placement manifest still records the uploading epoch —
//! objects uploaded under a fenced epoch are unreachable orphans
//! (the existing bounded object-store GC owns reclamation; a stale
//! uploader can never resurrect them through the manifest).
//!
//! Consistency: `vrt_sql::do_snap::snapshot_dir` copies each
//! `*.sqlite` through the rusqlite ONLINE BACKUP API — WAL-safe
//! cross-process copies while workerd writes (SQLite page-lock
//! semantics; if a future workerd pinned `locking_mode=EXCLUSIVE`
//! the backup/open simply fails → observable error, never a torn
//! copy). Non-sqlite files are byte copies with sha256.

use crate::do_placement::{PlacementEpoch, PlacementError, PlacementScope};
use crate::do_placement_store::{snap_file_key, snap_manifest_key, SnapRef};
use std::path::Path;
use std::sync::Arc;
use vrt_objstore::ObjectStore;
use vrt_sql::do_snap::{self, SnapError, SnapshotMeta};

#[derive(Debug)]
pub enum DoSnapOp {
    Io(std::io::Error),
    Store(String),
    Snap(SnapError),
    Fenced,
    Corrupt(String),
}

impl std::fmt::Display for DoSnapOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Store(e) => write!(f, "store: {e}"),
            Self::Snap(e) => write!(f, "snap: {e}"),
            Self::Fenced => write!(f, "placement fenced"),
            Self::Corrupt(e) => write!(f, "corrupt: {e}"),
        }
    }
}
impl std::error::Error for DoSnapOp {}

fn serr(e: vrt_objstore::StoreError) -> DoSnapOp {
    DoSnapOp::Store(e.to_string())
}

/// Snapshot `class_dir` and commit it to the placement manifest under
/// `epoch`/`seq` (CAS-checked: a fenced epoch can never commit).
/// Returns the committed ref. Caller holds the lease.
pub fn upload_snapshot(
    store: &Arc<dyn ObjectStore>,
    placement: &crate::do_placement_store::ManifestDoPlacement,
    scope: &PlacementScope,
    epoch: PlacementEpoch,
    seq: u64,
    tmp_root: &Path,
    kind: crate::do_placement_store::SnapKind,
    // WAL1-r3 P0-B: invoked right after the image is durably taken
    // (the backup's read txn has closed inside `snapshot_dir`) and
    // BEFORE objects upload / placement commits — the WAL tailer
    // records its {B,E} anchor here so E provably ≥ the image's
    // mxFrame. `None` for snapshots that adopt no chain (Final).
    image_hook: Option<&dyn Fn()>,
) -> Result<SnapRef, DoSnapOp> {
    let stage = tmp_root.join(format!("snap-{}-{seq}", scope.class_key));
    let _ = std::fs::remove_dir_all(&stage);
    let meta =
        do_snap::snapshot_dir(&scope.class_dir, &stage, epoch.0, seq).map_err(DoSnapOp::Snap)?;
    if let Some(h) = image_hook {
        h();
    }
    // Upload payload objects first, manifest LAST — a manifest is
    // only valid once every listed object exists.
    for f in &meta.files {
        let data = std::fs::read(stage.join(&f.path)).map_err(DoSnapOp::Io)?;
        let key = snap_file_key(&scope.class_key, epoch.0, seq, &f.path);
        vrt_objstore::block_on_store(store.put(&key, &data)).map_err(serr)?;
    }
    let mkey = snap_manifest_key(&scope.class_key, epoch.0, seq);
    let mbody = serde_json::to_vec(&meta)
        .map_err(|e| DoSnapOp::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
    vrt_objstore::block_on_store(store.put(&mkey, &mbody)).map_err(serr)?;
    let bytes: u64 = meta.files.iter().map(|f| f.size).sum();
    let snap = SnapRef {
        manifest_key: mkey,
        epoch: epoch.0,
        seq,
        files: meta.files.len() as u32,
        bytes,
        at_ms: meta.created_ms,
        kind,
    };
    placement
        .commit_snapshot(scope, epoch, snap.clone())
        .map_err(|e| match e {
            PlacementError::Fenced { .. } => DoSnapOp::Fenced,
            _ => DoSnapOp::Store(e.to_string()),
        })?;
    let _ = std::fs::remove_dir_all(&stage);
    Ok(snap)
}

/// Restore the manifest-referenced snapshot into `class_dir` — ONLY
/// when the local dir is missing or empty (a live local dir is
/// at-least-as-fresh as the last snapshot: it was written under a
/// held lease; overwriting it would LOSE post-snapshot commits —
/// fail closed instead of clobbering).
///
/// RV-R31 #4：本地非空且 `allow_replace=false` 时不是无条件放行
/// ——先用快照 meta 判定本地目录是否"恰是某次恢复的产物"：本地
/// 文件集合 ⊆ meta.files 且无 sha256/size/integrity 不符 → 放行
/// （本地数据就是该快照）；任何缺失/不符 → 掉电幽灵，必须重拉
/// （结构化 warn + `vrt_host_do_restore_local_corrupt_total`）；
/// 含快照外文件（-wal/-shm/外来写入、目录无法枚举）→ 活体目录
/// → local-wins 不动。校验失败的目录从不被删除——restore 只走
/// "验证过的 stage 整体换入" 的原子路径（r24 sweep 语义不变：
/// 这里不产孤儿判定，只产"本地不完整"判定）。
///
/// Stages into `<class_dir>.restore-tmp`, verifies sha256 + size +
/// `PRAGMA integrity_check`, then atomically renames into place.
/// Any failure leaves existing local data untouched.
/// `allow_replace` permits replacing a non-empty local dir (used by
/// the supervisor ONLY when the dir's recorded lease-epoch is older
/// than the snapshot's epoch — see `sibling_epoch_file`: a fenced-
/// away writer's stale dir must yield to the newer committed
/// baseline, never silently serve it).
/// MIG-17-r3 (B1a): `pre_place` is invoked ONCE, only when the
/// local dir will actually be replaced (missing/corrupt/stale-epoch
/// paths) — never on the Intact/Live early return. Callers use it to
/// arm the `wal-replay-pending` marker so it exists ONLY beside
/// restore products; a transient manifest failure or a crash before
/// this point can never leave one bound to a live dir.
pub fn restore_snapshot(
    store: &Arc<dyn ObjectStore>,
    snap: &SnapRef,
    scope: &PlacementScope,
    allow_replace: bool,
    pre_place: Option<&dyn Fn() -> std::io::Result<()>>,
) -> Result<bool, DoSnapOp> {
    let local_nonempty = scope.class_dir.exists()
        && std::fs::read_dir(&scope.class_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(true);
    // 1. manifest object → SnapshotMeta (schema + caps enforced).
    let obj = vrt_objstore::block_on_store(store.get(&snap.manifest_key))
        .map_err(serr)?
        .ok_or_else(|| DoSnapOp::Corrupt("manifest object missing".into()))?;
    let meta: SnapshotMeta = serde_json::from_slice(&obj.body)
        .map_err(|e| DoSnapOp::Corrupt(format!("manifest parse: {e}")))?;
    if meta.epoch != snap.epoch || meta.seq != snap.seq {
        return Err(DoSnapOp::Corrupt("manifest epoch/seq mismatch".into()));
    }
    if local_nonempty && !allow_replace {
        match local_restore_state(&scope.class_dir, &meta) {
            LocalDirState::Intact | LocalDirState::Live => {
                return Ok(false); // local data wins — nothing to restore
            }
            LocalDirState::Corrupt(detail) => {
                crate::metrics::registry().inc(
                    "vrt_host_do_restore_local_corrupt_total",
                    &[("tenant", &scope.tenant)],
                    1.0,
                );
                tracing::warn!(
                    tenant = %scope.tenant,
                    class_key = %scope.class_key,
                    detail = %detail,
                    "do local dir failed snapshot-meta verification; restoring"
                );
                // fall through：走与 allow_replace 相同的 stage →
                // verify → backup-rename → place 原子换入路径。
            }
        }
    }
    // 2. stage files: download → verify → atomic place.
    let stage = scope.class_dir.with_extension("restore-tmp");
    let _ = std::fs::remove_dir_all(&stage);
    std::fs::create_dir_all(&stage).map_err(DoSnapOp::Io)?;
    for f in &meta.files {
        if !vrt_sql::do_snap::valid_relpath_pub(&f.path) {
            let _ = std::fs::remove_dir_all(&stage);
            return Err(DoSnapOp::Corrupt(format!("unsafe path {}", f.path)));
        }
        let key = snap_file_key(&scope.class_key, snap.epoch, snap.seq, &f.path);
        let obj = vrt_objstore::block_on_store(store.get(&key))
            .map_err(serr)?
            .ok_or_else(|| DoSnapOp::Corrupt(format!("missing object {key}")))?;
        let dst = stage.join(&f.path);
        if let Some(p) = dst.parent() {
            std::fs::create_dir_all(p).map_err(DoSnapOp::Io)?;
        }
        std::fs::write(&dst, &obj.body).map_err(DoSnapOp::Io)?;
    }
    if let Err(e) = do_snap::verify_snapshot_files(&meta, &stage) {
        let _ = std::fs::remove_dir_all(&stage);
        return Err(DoSnapOp::Snap(e));
    }
    // 3. atomic install: place_verified renames stage → class_dir.
    //    Replacing keeps the old dir until the verified one lands —
    //    a post-verify failure restores the backup, never an empty
    //    hole. `pre_place` arms durable state (the replay-pending
    //    marker) that must exist BEFORE the dir becomes a restore
    //    product — its failure aborts the whole restore (typed,
    //    retryable), so a marker is never missing beside a placed dir.
    if let Some(h) = pre_place {
        h().map_err(DoSnapOp::Io)?;
    }
    if local_nonempty {
        let backup = scope.class_dir.with_extension("replaced-bak");
        let _ = std::fs::remove_dir_all(&backup);
        std::fs::rename(&scope.class_dir, &backup).map_err(DoSnapOp::Io)?;
        if let Err(e) = do_snap::place_verified(&stage, &scope.class_dir) {
            let _ = std::fs::remove_dir_all(&scope.class_dir);
            let _ = std::fs::rename(&backup, &scope.class_dir);
            let _ = std::fs::remove_dir_all(&stage);
            return Err(DoSnapOp::Snap(e));
        }
        let _ = std::fs::remove_dir_all(&backup);
    } else if let Err(e) = do_snap::place_verified(&stage, &scope.class_dir) {
        let _ = std::fs::remove_dir_all(&stage);
        return Err(DoSnapOp::Snap(e));
    }
    Ok(true)
}

/// 本地目录相对某次快照的判定（RV-R31 #4）。
enum LocalDirState {
    /// 本地文件集合 ⊆ 快照清单且 sha256/size/integrity 全部相符——
    /// 该目录就是某次恢复的产物，放行不拉。
    Intact,
    /// 含快照清单之外的文件（-wal/-shm/外来文件/symlink/枚举失败）
    /// ——活体目录，local-wins，绝不覆盖。
    Live,
    /// ⊆ 清单但校验失败（缺文件/大小/sha/integrity 不符）——掉电
    /// 幽灵，必须恢复。
    Corrupt(String),
}

fn local_restore_state(dir: &Path, meta: &SnapshotMeta) -> LocalDirState {
    let mut files = Vec::new();
    if collect_local_files(dir, dir, &mut files).is_err() {
        return LocalDirState::Live; // 枚举不了 → 无法判定 → 按活体保守处理
    }
    let meta_paths: std::collections::HashSet<&str> =
        meta.files.iter().map(|f| f.path.as_str()).collect();
    for rel in &files {
        if !meta_paths.contains(rel.as_str()) {
            return LocalDirState::Live;
        }
    }
    match vrt_sql::do_snap::verify_snapshot_files(meta, dir) {
        Ok(()) => LocalDirState::Intact,
        Err(e) => LocalDirState::Corrupt(e.to_string()),
    }
}

/// 收集目录下全部非目录条目的相对路径（`/` 分隔）——symlink、
/// socket、fifo 一律按"文件"计入（不在 meta 清单 → 活体）。
fn collect_local_files(
    root: &Path,
    dir: &Path,
    out: &mut Vec<String>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_local_files(root, &path, out)?;
        } else {
            let rel = path
                .strip_prefix(root)
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "relpath strip")
                })?
                .to_string_lossy()
                .replace('\\', "/");
            out.push(rel);
        }
    }
    Ok(())
}

/// Sibling marker recording which lease epoch wrote this class dir —
/// `<uniqueKey>.lease-epoch` next to the dir (kept OUT of it so a
/// snapshot never carries another epoch's marker).
pub fn sibling_epoch_file(scope: &PlacementScope) -> std::path::PathBuf {
    scope
        .class_dir
        .parent()
        .unwrap_or(&scope.class_dir)
        .join(format!(
            "{}{}",
            scope.class_key,
            crate::do_placement::LEASE_EPOCH_SUFFIX
        ))
}

/// Epoch recorded beside `class_dir`; `None` = pre-P3c-2 dir or
/// absent marker (treated as "unknown provenance" by callers).
pub fn local_dir_epoch(scope: &PlacementScope) -> Option<u64> {
    let s = std::fs::read_to_string(sibling_epoch_file(scope)).ok()?;
    serde_json::from_str::<serde_json::Value>(&s)
        .ok()?
        .get("epoch")?
        .as_u64()
}

/// Persist the acquiring epoch beside `class_dir`（tmp → fsync →
/// rename → fsync 目录；RV-R31 #4——标记是启动期信任判定凭据，
/// 必须耐久；仍是 best-effort：写不进盘时按"未知来源"处理，
/// fail closed 方向不变）。
pub fn mark_local_dir_epoch(scope: &PlacementScope, epoch: u64) {
    let p = sibling_epoch_file(scope);
    let body = format!("{{\"epoch\":{epoch}}}");
    let _ = vrt_securefs::atomic_write(&p, body.as_bytes(), 0o600);
}

/// MIG-17-r2 (B1): `<uniqueKey>.wal-replay-pending` beside `class_dir`
/// — "the dir's WAL tail replay has not yet completed". Restore and
/// replay are ONE preparation step: the marker is written the moment a
/// restored (or genesis-replayable) dir exists and cleared only after
/// `replay_for_restore` succeeds. A later attempt that finds the dir
/// `Intact` but the marker still present must re-run the replay — the
/// previous attempt could have timed out or failed between the two,
/// and launching on the bare snapshot baseline would silently drop
/// every committed post-baseline write.
pub fn sibling_replay_pending_file(scope: &PlacementScope) -> std::path::PathBuf {
    scope
        .class_dir
        .parent()
        .unwrap_or(&scope.class_dir)
        .join(format!(
            "{}{}",
            scope.class_key,
            crate::do_placement::REPLAY_PENDING_SUFFIX
        ))
}

/// Context the pending marker binds the replay to — the baseline the
/// local dir was restored from plus the writer epoch whose chain must
/// apply. `(0, 0)` = genesis replay (no committed snapshot yet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplayPending {
    pub snap_epoch: u64,
    pub snap_seq: u64,
    pub writer_epoch: u64,
}

/// Mark "replay incomplete" beside `class_dir` BEFORE the replay runs
/// — MIG-17-r3 (B1b): fail-CLOSED durable, unlike `mark_local_dir_epoch`.
/// A marker that can't be written must not let the unit proceed — the
/// caller converts `Err` into a retryable typed failure rather than
/// risk an unmarked restore product.
pub fn mark_replay_pending(scope: &PlacementScope, ctx: ReplayPending) -> std::io::Result<()> {
    let file = sibling_replay_pending_file(scope);
    // The worker seg dir may not exist yet (genesis onto a missing
    // class dir) — the marker is tenant bookkeeping, so an absent
    // seg root is created rather than reported as a write failure.
    if let Some(parent) = file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec(&ctx).unwrap_or_default();
    vrt_securefs::atomic_write(&file, &body, 0o600)
}

/// The pending marker's context. A corrupt/undecodable marker reads
/// as `None` — same "unknown provenance" contract as
/// `local_dir_epoch` (the atomic_write tmp→rename dance makes partial
/// bodies impossible short of fs-level corruption).
pub fn replay_pending(scope: &PlacementScope) -> Option<ReplayPending> {
    let s = std::fs::read_to_string(sibling_replay_pending_file(scope)).ok()?;
    serde_json::from_str(&s).ok()
}

/// Clear the marker — only after `replay_for_restore` succeeded (or
/// the marker provably belongs to a different baseline/writer epoch).
pub fn clear_replay_pending(scope: &PlacementScope) {
    let _ = std::fs::remove_file(sibling_replay_pending_file(scope));
}

/// MIG-17-r3 (B1a): does `dir` hold live WAL frames — a `*-wal`
/// file with content beyond the 32-byte header? Snapshots never
/// carry `-wal`, so frames there mean live writer commits beyond any
/// baseline — replaying onto such a dir would regress state, and a
/// stray replay-pending marker there is stale by definition.
/// (`-shm` is NOT evidence: any connection — including our own
/// `integrity_check`/verify — materializes it; a header-only or
/// empty `-wal` holds no frames and folds harmlessly.)
pub fn has_live_wal_frames(dir: &std::path::Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    rd.flatten().any(|e| {
        e.file_name().to_string_lossy().ends_with("-wal")
            && e.metadata().map(|m| m.len() > 32).unwrap_or(true)
    })
}

/// RV-R53.4: typed outcome of a prefix cleanup. `complete == false`
/// means the prefix is NOT confirmed empty (the `max_keys` cap was hit,
/// or the final listing was still truncated) — the caller MUST requeue
/// instead of treating the deletion as done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupOutcome {
    /// Objects deleted in this call.
    pub deleted: usize,
    /// `true` only when a post-deletion listing proved the prefix empty.
    pub complete: bool,
}

/// Delete every object under `do/{class_key}/` — placement record,
/// all snapshots across epochs. Idempotent + bounded: at most
/// `max_keys` objects are deleted per call; [`CleanupOutcome::complete`]
/// is `false` while the prefix may still hold objects (the caller's
/// ledger/queue re-drives the next round). Deletion of a missing key
/// is a no-op. MUST only be called with the class's own uniqueKey —
/// the prefix IS the tenant boundary.
pub fn cleanup_class_objects(
    store: &Arc<dyn ObjectStore>,
    class_key: &str,
    max_keys: usize,
) -> Result<CleanupOutcome, DoSnapOp> {
    // uniqueKey is a 64-hex namespace mint — a key outside that shape
    // is a caller bug; refuse rather than prefix-delete `do/`.
    if class_key.len() != 64 || !class_key.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(DoSnapOp::Corrupt("class_key shape".into()));
    }
    let prefix = format!("do/{class_key}/");
    let mut deleted = 0usize;
    loop {
        let page = vrt_objstore::block_on_store(store.list(&prefix)).map_err(serr)?;
        if page.entries.is_empty() {
            // A truncated page claiming no entries is contradictory —
            // fail closed: not confirmed empty, retry next round.
            return Ok(CleanupOutcome {
                deleted,
                complete: !page.truncated,
            });
        }
        if deleted >= max_keys {
            return Ok(CleanupOutcome {
                deleted,
                complete: false,
            });
        }
        for entry in page.entries {
            if deleted >= max_keys {
                return Ok(CleanupOutcome {
                    deleted,
                    complete: false,
                });
            }
            vrt_objstore::block_on_store(store.delete(&entry.key)).map_err(serr)?;
            deleted += 1;
        }
    }
}
