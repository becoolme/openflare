//! W6-r2: tombstone-based `MINIFLARE_LOOPBACK` workflow-storage delete.
//!
//! The vendored workflows Engine calls two endpoints around
//! `ctx.storage.deleteAll()` + `abort()`:
//!
//!   DELETE /core/workflow-storage/<uniqueKey>/<hexId>?defer=1
//!       → persist a tombstone → 204. NO file is unlinked here.
//!   GET    /core/workflow-storage/<uniqueKey>/<hexId>?waitForPendingDelete=1
//!       → 204 (always — after the same fail-closed validation).
//!
//! ## Why no unlink while workerd lives
//!
//! W6 shipped a convergence loop that unlinked `<hex>.sqlite*` and
//! re-deleted reappearing files. That is UNSAFE: the Engine namespace
//! is `preventEviction = true`, so any `get()`/`status()`/alarm inside
//! the window re-instantiates the actor and has it open the sqlite
//! while the loop is still deleting. The actor then commits into an
//! inode no path reaches — a subsequent `create()` confirms from that
//! in-memory state and the instance is lost at the next workerd
//! restart (proven by `real_workerd_wf_delete_get_race_loses_recreate`
//! — it FAILED on the pre-r2 implementation). Deleting `-shm`/`-wal`
//! under open connections also desyncs the shared-memory index.
//!
//! ## Logical delete is already durable
//!
//! `deleteInstance` runs `ctx.storage.deleteAll()` INSIDE the actor's
//! storage transaction before calling us — the wipe is a set of
//! ordinary WAL frames that WAL1 replicates like any other write.
//! Restore/replay therefore yields the same EMPTY database (no
//! resurrection), and a same-id recreate starts clean BECAUSE the
//! emptied state is what got copied. A tombstone + safe-point reclaim
//! is thus sufficient; nothing needs the file gone eagerly.
//!
//! ## Tombstone
//!
//! Atomic write (tmp + rename + dir fsync) under the host-owned
//! `<work_dir>/wf-del/<tenant>/<classKey>/<hexId>.json`, recording the
//! resolved storage identity (class dir + instance path) so reclaim
//! needs no registry. Bounded: `MAX_TOMBSTONES` per tenant triggers a
//! warn — DELETE is NEVER rejected (the logical delete already
//! committed; refusing would strand it invisibly).
//!
//! ## Safe-point reclaim (`reclaim_tenant`)
//!
//! Called from the supervisor ONLY where `active.is_none() &&
//! draining.is_empty()` — inside the serialized DO switch the old
//! generation is provably dead before the new one opens class dirs,
//! and a cold start has no generation at all. No workerd actor can
//! hold the sqlite; `wal.retire_db` is invoked BEFORE each unlink to
//! drop the replicator's own open read connection (WalPin) AND seal
//! the deleted incarnation's label.
//!
//! Per tombstone: re-verify the recorded instance path inside the
//! recorded class dir (canonicalize), open the db with rusqlite and
//! apply the ENGINE'S OWN liveness marker — `_cf_KV` row
//! `INSTANCE_METADATA` (vendored `get()` keys existence on exactly
//! this): absent → `retire_db` + unlink `.sqlite{,-shm,-wal}` + drop
//! tombstone; present → drop tombstone, KEEP the file (a same-id
//! recreate must never be unlinked — this is the only gate that
//! prevents it); missing file → drop tombstone; I/O error → keep
//! tombstone + `reclaim_failed_total` + structured log.
//!
//! VRT-hardened validation (fail closed, never silent) — unchanged:
//!   - `uniqueKey` must be declared in the SERVING spec's
//!     `DoStorageView` (signed desired);
//!   - `hexId` must be exactly 64 lowercase hex;
//!   - the resolved `<hexId>.sqlite` must canonicalize inside the
//!     class dir (symlink/traversal escape → explicit 400);
//!   - bare-name DELETE → explicit 501 (the engine never calls it).

use crate::do_wal::DoWalReplicator;
use crate::supervisor::TenantRegistry;
use axum::extract::{Path as AxPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::delete;
use axum::Router;
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Per-file unlink retry budget (transient fs errors only — at the
/// safe point no actor holds these files).
const UNLINK_ATTEMPTS: u32 = 3;
const UNLINK_RETRY_MS: u64 = 50;
/// Tombstone ceiling per tenant — warn above, NEVER reject a delete
/// (the logical wipe already committed; dropping the record would
/// strand the file invisibly).
const MAX_TOMBSTONES: usize = 10_000;
/// Reclaim work cap per safe-point pass — the scan runs inline on
/// the supervisor's serialized launch path, so each pass must stay
/// bounded; leftovers keep their tombstones for the next pass.
const MAX_RECLAIM_PER_PASS: u32 = 512;
/// Deleted-file suffixes (miniflare `WORKFLOW_STORAGE_EXTENSIONS`):
/// `<hexId>.sqlite{,-shm,-wal}` on disk.
const SQLITE_EXT: [&str; 3] = [".sqlite", ".sqlite-shm", ".sqlite-wal"];
/// Engine liveness marker — the vendored engine keys instance
/// existence on `storage.get("INSTANCE_METADATA")`, which lands in
/// `_cf_KV` in every workerd DO sqlite file.
const INSTANCE_METADATA_KEY: &str = "INSTANCE_METADATA";
const CF_KV_TABLE: &str = "_cf_KV";

pub struct WfDelState {
    /// Serving-spec tenant registry — storage-identity resolution
    /// (uniqueKey → class dir) for incoming requests.
    registry: Arc<TenantRegistry>,
    /// Durable tombstone root `<work_dir>/wf-del` (host-owned 0700).
    tomb_dir: PathBuf,
}

impl WfDelState {
    pub fn new(registry: Arc<TenantRegistry>, tomb_dir: PathBuf) -> Self {
        Self { registry, tomb_dir }
    }
}

pub fn router(state: WfDelState) -> Router {
    Router::new()
        .route(
            "/core/workflow-storage/{uk}/{hex}",
            delete(del_instance).get(wait_instance),
        )
        .route("/core/workflow-storage/{uk}", delete(del_namespace))
        .with_state(Arc::new(state))
}

fn err(status: StatusCode, code: &str) -> Response {
    (
        status,
        axum::Json(serde_json::json!({"error": {"code": code}})),
    )
        .into_response()
}

fn valid_hex64(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// uk → (tenant, worker_id, class_dir): exactly one serving worker
/// may declare this storage identity; ambiguity is a hard error.
fn resolve_class_dir(
    registry: &TenantRegistry,
    uk: &str,
) -> Result<Option<(String, String, PathBuf)>, ()> {
    let mut found: Option<(String, String, PathBuf)> = None;
    for tid in registry.tenant_ids() {
        let Some(rt) = registry.get(&tid) else {
            continue;
        };
        let Some(view) = rt.do_storage_view() else {
            continue;
        };
        for (wid, keys) in &view.worker_classes {
            if keys.iter().any(|k| k == uk) {
                let Some(dir) = view.class_dir(wid, uk) else {
                    continue;
                };
                if found.is_some() {
                    return Err(());
                }
                found = Some((tid.clone(), wid.clone(), dir));
            }
        }
    }
    Ok(found)
}

struct ResolvedInstance {
    tenant: String,
    class_key: String,
    /// The class dir AS RESOLVED AT DELETE TIME — recorded into the
    /// tombstone so reclaim needs no registry.
    class_dir: PathBuf,
    /// `<class_dir>/<hexId>` — extension-less instance path
    /// (canonicalized inside the verified class dir).
    instance_path: PathBuf,
    hex_id: String,
}

/// Shared validation: uniqueKey + hexId shape + declared-class +
/// canonicalization inside the class dir.
fn resolve(state: &WfDelState, uk: &str, hex_id: &str) -> Result<ResolvedInstance, Response> {
    if !valid_hex64(uk) {
        return Err(err(StatusCode::BAD_REQUEST, "workflow.undeclared"));
    }
    if !valid_hex64(hex_id) {
        return Err(err(StatusCode::BAD_REQUEST, "instance.id_invalid"));
    }
    let (tenant, _wid, class_dir) = match resolve_class_dir(&state.registry, uk) {
        Ok(Some(r)) => r,
        Ok(None) => return Err(err(StatusCode::BAD_REQUEST, "workflow.undeclared")),
        Err(()) => return Err(err(StatusCode::INTERNAL_SERVER_ERROR, "workflow.ambiguous")),
    };
    // `class_dir` is registry-trusted; canonicalize it once, then
    // verify each resolved target stays inside — a hostile symlink
    // at <hexId>.sqlite must not redirect anything outside the
    // tenant's DO storage.
    let canon_dir = std::fs::canonicalize(&class_dir)
        .map_err(|_| err(StatusCode::NOT_FOUND, "workflow.storage_gone"))?;
    let instance_path = canon_dir.join(hex_id);
    if let Ok(real) = std::fs::canonicalize(format!("{}.sqlite", instance_path.display())) {
        if !real.starts_with(&canon_dir) {
            return Err(err(StatusCode::BAD_REQUEST, "instance.path_escape"));
        }
    }
    Ok(ResolvedInstance {
        tenant,
        class_key: uk.to_string(),
        class_dir,
        instance_path,
        hex_id: hex_id.to_string(),
    })
}

// ---- tombstones ----

fn tomb_json_path(tomb_dir: &FsPath, tenant: &str, class_key: &str, hex_id: &str) -> PathBuf {
    tomb_dir
        .join(tenant)
        .join(class_key)
        .join(format!("{hex_id}.json"))
}

/// Live tombstone count for one tenant (bounded scan).
fn tomb_count(tomb_dir: &FsPath, tenant: &str) -> usize {
    let mut n = 0usize;
    let tdir = tomb_dir.join(tenant);
    let Ok(classes) = std::fs::read_dir(&tdir) else {
        return 0;
    };
    for c in classes.flatten() {
        if c.file_type().is_ok_and(|t| t.is_dir()) {
            if let Ok(rd) = std::fs::read_dir(c.path()) {
                n += rd
                    .flatten()
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".json"))
                    .count();
            }
        }
    }
    n
}

fn set_tomb_gauge(tomb_dir: &FsPath, tenant: &str) {
    crate::metrics::registry().set(
        "vrt_host_wf_storage_tombstones",
        &[("tenant", tenant)],
        tomb_count(tomb_dir, tenant) as f64,
    );
}

/// Persist the tombstone: tmp-write + fsync + atomic rename + dir
/// fsync. A tombstone surviving vrt-host crash is the WHOLE point —
/// the pending physical delete must outlive this process.
fn write_tombstone(state: &WfDelState, inst: &ResolvedInstance) -> std::io::Result<()> {
    let target = tomb_json_path(&state.tomb_dir, &inst.tenant, &inst.class_key, &inst.hex_id);
    let dir = target.parent().unwrap().to_path_buf();
    std::fs::create_dir_all(&dir)?;
    let body = serde_json::json!({
        "v": 1,
        "tenant": inst.tenant,
        "class_key": inst.class_key,
        "hex_id": inst.hex_id,
        "class_dir": inst.class_dir,
        // Informational only — reclaim recomputes the target from
        // class_dir + hex_id so a tampered path can never redirect
        // the unlink.
        "instance_path": inst.instance_path,
        "ts_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    })
    .to_string();
    let tmp = dir.join(format!("{}.tmp", inst.hex_id));
    {
        let mut f = std::fs::File::create(&tmp)?;
        use std::io::Write;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &target)?;
    // Directory fsync so the rename itself is durable (unix semantics).
    // Best-effort: some filesystems reject dir fsync — the file fsync +
    // atomic rename already protects the record's content; a dropped
    // dir fsync only widens the crash window to dentry-loss, which the
    // next safe-point reclaim would surface as an absent tombstone.
    match std::fs::File::open(&dir).and_then(|d| d.sync_all()) {
        Ok(()) => {}
        Err(e) => {
            tracing::warn!(
                tenant = %inst.tenant,
                class = %inst.class_key,
                instance = %inst.hex_id,
                error = %e,
                "tombstone dir fsync failed — record may not survive a host crash"
            );
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct Tombstone {
    tenant: String,
    class_key: String,
    hex_id: String,
    class_dir: PathBuf,
}

/// `DELETE /core/workflow-storage/{uk}/{hex}[?defer=1]` — records the
/// tombstone only. `defer` is accepted for contract parity but is a
/// no-op: nothing is unlinked online, so there is nothing to defer.
async fn del_instance(
    State(state): State<Arc<WfDelState>>,
    AxPath((uk, hex)): AxPath<(String, String)>,
    Query(_q): Query<HashMap<String, String>>,
) -> Response {
    let inst = match resolve(&state, &uk, &hex) {
        Ok(i) => i,
        Err(r) => return r,
    };
    if let Err(e) = write_tombstone(&state, &inst) {
        crate::metrics::registry().inc(
            "vrt_host_wf_storage_reclaim_failed_total",
            &[("tenant", &inst.tenant)],
            1.0,
        );
        tracing::error!(
            tenant = %inst.tenant,
            class = %inst.class_key,
            instance = %inst.hex_id,
            error = %e,
            "workflow delete tombstone write failed — files leak until next spawn"
        );
        return err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "instance.tombstone_failed",
        );
    }
    let n = tomb_count(&state.tomb_dir, &inst.tenant);
    crate::metrics::registry().set(
        "vrt_host_wf_storage_tombstones",
        &[("tenant", &inst.tenant)],
        n as f64,
    );
    if n > MAX_TOMBSTONES {
        tracing::warn!(
            tenant = %inst.tenant,
            tombstones = n,
            "workflow delete tombstones exceed MAX_TOMBSTONES"
        );
    }
    tracing::info!(
        tenant = %inst.tenant,
        class = %inst.class_key,
        instance = %inst.hex_id,
        "workflow instance delete tombstoned — reclaim at next safe point"
    );
    (StatusCode::NO_CONTENT, "").into_response()
}

/// `GET /core/workflow-storage/{uk}/{hex}?waitForPendingDelete=1` —
/// always 204 after validation. Miniflare parity for the no-pending
/// case AND the only correct answer now: the vendored `create()`
/// polls this before every create and throws on non-ok; the physical
/// unlink is owned by the safe-point reclaimer, not by any wait here.
async fn wait_instance(
    State(state): State<Arc<WfDelState>>,
    AxPath((uk, hex)): AxPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if !q.contains_key("waitForPendingDelete") {
        return err(StatusCode::BAD_REQUEST, "query.unsupported");
    }
    // Shape validation still applies — a malformed caller must not
    // observe "204" for an id that could never exist.
    let _inst = match resolve(&state, &uk, &hex) {
        Ok(i) => i,
        Err(r) => return r,
    };
    (StatusCode::NO_CONTENT, "").into_response()
}

/// `DELETE /core/workflow-storage/{uk}` — miniflare's namespace wipe.
/// The vendored engine never calls it; refuse explicitly rather than
/// silently succeeding (fail closed).
async fn del_namespace() -> Response {
    err(StatusCode::NOT_IMPLEMENTED, "workflow.namespace_delete")
}

// ---- safe-point reclaim ----

#[derive(Default, Debug)]
pub struct ReclaimStats {
    pub scanned: u32,
    pub reclaimed: u32,
    /// Tombstone dropped because the file holds a LIVE instance
    /// (`_cf_KV.INSTANCE_METADATA` present) — the recreate survives.
    pub skipped_nonempty: u32,
    /// File already gone — tombstone dropped.
    pub missing: u32,
    /// Error — tombstone KEPT for the next safe point.
    pub failed: u32,
}

/// Reclaim tombstoned instance files for one tenant.
///
/// SAFE-POINT CONTRACT — call ONLY where no workerd process of this
/// tenant can hold these files: inside the serialized DO switch after
/// the old generation fully exits (and before the new one spawns), or
/// on a cold start. The caller additionally gates on
/// `active.is_none() && draining.is_empty()`. `wal.retire_db` runs
/// BEFORE each unlink to drop the replicator's own open read
/// connection (WalPin holds `-wal` fds in-process) and seal the dead
/// incarnation's label.
pub fn reclaim_tenant(
    tenant: &str,
    tomb_dir: &FsPath,
    wal: &Option<Arc<DoWalReplicator>>,
    leased: Option<&std::collections::BTreeSet<String>>,
) -> ReclaimStats {
    let mut stats = ReclaimStats::default();
    if tomb_dir.as_os_str().is_empty() {
        return stats;
    }
    let tdir = tomb_dir.join(tenant);
    let Ok(classes) = std::fs::read_dir(&tdir) else {
        return stats;
    };
    'outer: for class in classes.flatten() {
        // W6-r3: ownership gate — only classes whose placement lease
        // THIS host holds may be reclaimed. An unleased class dir
        // belongs to another cell (authoritative acquire failure) or
        // to no live spec; its tombstones and files stay untouched.
        if let Some(set) = leased {
            let key = class.file_name().to_string_lossy().to_string();
            if !set.contains(&key) {
                continue;
            }
        }
        let Ok(rd) = std::fs::read_dir(class.path()) else {
            continue;
        };
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".json") {
                // Crash-orphaned `*.tmp` from an interrupted
                // tombstone write (create→fsync ok, rename never
                // ran). Only touch files older than a minute — a
                // live writer's tmp must never be pulled mid-write.
                if name.ends_with(".tmp") {
                    if let Ok(meta) = entry.metadata() {
                        let old = meta
                            .modified()
                            .ok()
                            .and_then(|t| t.elapsed().ok())
                            .map(|d| d.as_secs() > 60)
                            .unwrap_or(false);
                        if old {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
                continue;
            }
            if stats.scanned >= MAX_RECLAIM_PER_PASS {
                tracing::warn!(
                    tenant = %tenant,
                    scanned = stats.scanned,
                    "wf reclaim pass hit MAX_RECLAIM_PER_PASS — leftovers retry next safe point"
                );
                break 'outer;
            }
            stats.scanned += 1;
            let tpath = entry.path();
            let tomb = match std::fs::read_to_string(&tpath)
                .ok()
                .and_then(|s| serde_json::from_str::<Tombstone>(&s).ok())
            {
                Some(t) if t.tenant == tenant => t,
                _ => {
                    // Corrupt or foreign record — cannot trust its
                    // paths; keep it (retried at every safe point) and
                    // make it loud rather than silently leaking.
                    stats.failed += 1;
                    tracing::warn!(path = %tpath.display(), "wf tombstone unreadable — kept");
                    continue;
                }
            };
            reclaim_one(&tomb, &tpath, wal, &mut stats);
        }
    }
    set_tomb_gauge(tomb_dir, tenant);
    if stats.reclaimed + stats.skipped_nonempty + stats.failed > 0 {
        tracing::info!(
            tenant = %tenant,
            reclaimed = stats.reclaimed,
            skipped_nonempty = stats.skipped_nonempty,
            missing = stats.missing,
            failed = stats.failed,
            "workflow storage reclaim pass complete"
        );
    }
    stats
}

fn reclaim_one(
    tomb: &Tombstone,
    tpath: &FsPath,
    wal: &Option<Arc<DoWalReplicator>>,
    stats: &mut ReclaimStats,
) {
    let r = crate::metrics::registry();
    // Re-verify the recorded path: the recorded class_dir and the
    // resolved instance file must still sit inside it. A tombstone is
    // host-written, but defense-in-depth costs one canonicalize.
    let canon_dir = match std::fs::canonicalize(&tomb.class_dir) {
        Ok(d) => d,
        Err(_) => {
            // Class dir gone entirely — nothing left to unlink.
            let _ = std::fs::remove_file(tpath);
            stats.missing += 1;
            return;
        }
    };
    let instance_path = canon_dir.join(&tomb.hex_id);
    let sqlite = format!("{}.sqlite", instance_path.display());
    if !FsPath::new(&sqlite).exists() {
        let _ = std::fs::remove_file(tpath);
        stats.missing += 1;
        return;
    }
    match std::fs::canonicalize(&sqlite) {
        Ok(real) if real.starts_with(&canon_dir) => {}
        _ => {
            stats.failed += 1;
            r.inc(
                "vrt_host_wf_storage_reclaim_failed_total",
                &[("tenant", &tomb.tenant)],
                1.0,
            );
            tracing::error!(
                tenant = %tomb.tenant,
                class = %tomb.class_key,
                instance = %tomb.hex_id,
                "wf reclaim: instance path escaped recorded class dir — kept"
            );
            return;
        }
    }
    match instance_db_is_empty(FsPath::new(&sqlite)) {
        Ok(true) => {
            // WAL1: retire BEFORE unlink — drops the replicator's own
            // open read connection (WalPin) and seals the deleted
            // incarnation's label so a same-name recreate emits under
            // a fresh baseline, never colliding on old LSN keys. This
            // is the ONLY place retire_db runs for deletes — once per
            // reclaimed file at a workerd stop, so `wants_baseline` is
            // bounded by stop events, never by delete rate (and it is
            // a bool: N reclaims still request ≤1 class baseline).
            if let Some(w) = wal {
                w.retire_db(&tomb.class_key, &tomb.hex_id);
            }
            match unlink_instance(&instance_path) {
                Ok(()) => {
                    let _ = std::fs::remove_file(tpath);
                    stats.reclaimed += 1;
                    r.inc(
                        "vrt_host_wf_storage_reclaimed_total",
                        &[("tenant", &tomb.tenant)],
                        1.0,
                    );
                }
                Err(e) => {
                    stats.failed += 1;
                    r.inc(
                        "vrt_host_wf_storage_reclaim_failed_total",
                        &[("tenant", &tomb.tenant)],
                        1.0,
                    );
                    tracing::error!(
                        tenant = %tomb.tenant,
                        class = %tomb.class_key,
                        instance = %tomb.hex_id,
                        error = %e,
                        "wf reclaim unlink failed — tombstone kept"
                    );
                }
            }
        }
        Ok(false) => {
            // INSTANCE_METADATA present → a same-id recreate wrote a
            // live instance over this path. Drop the tombstone and
            // KEEP the file — this check is the only thing standing
            // between a tombstone and a live instance's storage.
            let _ = std::fs::remove_file(tpath);
            stats.skipped_nonempty += 1;
            r.inc(
                "vrt_host_wf_storage_reclaim_skipped_nonempty_total",
                &[("tenant", &tomb.tenant)],
                1.0,
            );
            tracing::info!(
                tenant = %tomb.tenant,
                class = %tomb.class_key,
                instance = %tomb.hex_id,
                "wf reclaim skipped — instance recreated (nonempty); file kept"
            );
        }
        Err(e) => {
            stats.failed += 1;
            r.inc(
                "vrt_host_wf_storage_reclaim_failed_total",
                &[("tenant", &tomb.tenant)],
                1.0,
            );
            tracing::error!(
                tenant = %tomb.tenant,
                class = %tomb.class_key,
                instance = %tomb.hex_id,
                error = %e,
                "wf reclaim empty-check failed — tombstone kept"
            );
        }
    }
}

/// Empty ⟺ the engine's own liveness marker is absent: `_cf_KV` has
/// no `INSTANCE_METADATA` row (or the kv table does not exist at all —
/// a file that never became a real DO db). Opened RW so any `-wal`
/// folds into the visible state; at the safe point nothing else
/// holds the file.
fn instance_db_is_empty(path: &FsPath) -> Result<bool, String> {
    let conn = rusqlite::Connection::open(path).map_err(|e| format!("open: {e}"))?;
    let has_kv: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [CF_KV_TABLE],
            |r: &rusqlite::Row| r.get(0),
        )
        .map_err(|e| format!("schema: {e}"))?;
    if has_kv == 0 {
        return Ok(true);
    }
    let n: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {CF_KV_TABLE} WHERE key=?1"),
            [INSTANCE_METADATA_KEY],
            |r: &rusqlite::Row| r.get(0),
        )
        .map_err(|e| format!("marker: {e}"))?;
    Ok(n == 0)
}

/// Unlink `<instance>.sqlite` + `-shm` + `-wal`; bounded per-file
/// retry for transient errors only.
fn unlink_instance(instance_path: &FsPath) -> std::io::Result<()> {
    for ext in SQLITE_EXT {
        let f = format!("{}{}", instance_path.display(), ext);
        let p = FsPath::new(&f);
        if !p.exists() {
            continue;
        }
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match std::fs::remove_file(p) {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) if attempt < UNLINK_ATTEMPTS => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(UNLINK_RETRY_MS));
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

// ---- tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervisor::{DoStorageView, TenantRuntime};
    use axum::body::Body;
    use axum::http::Request;
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    const UK: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HEX: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("vrt-wfdel-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Registry where `w1` declares `UK` under `<dir>/do`; tombstones
    /// under `<dir>/wf-del`.
    fn setup(dir: &FsPath) -> (Router, Arc<TenantRegistry>) {
        let reg = TenantRegistry::new();
        let rt = TenantRuntime::new("t1");
        rt.set_do_storage_view(Some(DoStorageView {
            tenant_dir: dir.join("do"),
            worker_classes: BTreeMap::from([("w1".to_string(), vec![UK.to_string()])]),
        }));
        reg.insert("t1".into(), rt);
        let class_dir = dir.join("do").join("w1").join(UK);
        std::fs::create_dir_all(&class_dir).unwrap();
        (
            router(WfDelState::new(reg.clone(), dir.join("wf-del"))),
            reg,
        )
    }

    fn req(method: &str, path: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    fn class_dir(dir: &FsPath) -> PathBuf {
        dir.join("do").join("w1").join(UK)
    }

    /// Real empty DO sqlite: `_cf_KV`/`_cf_METADATA` but NO
    /// `INSTANCE_METADATA` row — the post-deleteAll shape.
    fn write_empty_instance(dir: &FsPath) -> PathBuf {
        let p = class_dir(dir).join(format!("{HEX}.sqlite"));
        let conn = rusqlite::Connection::open(&p).unwrap();
        conn.execute(
            "CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID",
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE _cf_METADATA (k INTEGER PRIMARY KEY, v INTEGER)",
            [],
        )
        .unwrap();
        p
    }

    fn write_live_instance(dir: &FsPath) -> PathBuf {
        let p = write_empty_instance(dir);
        let conn = rusqlite::Connection::open(&p).unwrap();
        conn.execute(
            "INSERT INTO _cf_KV (key, value) VALUES ('INSTANCE_METADATA', X'7B7D')",
            [],
        )
        .unwrap();
        p
    }

    fn tomb_path(dir: &FsPath) -> PathBuf {
        tomb_json_path(&dir.join("wf-del"), "t1", UK, HEX)
    }

    #[tokio::test]
    async fn delete_writes_tombstone_and_keeps_files() {
        let dir = tmpdir("tomb");
        let (app, _r) = setup(&dir);
        let db = write_live_instance(&dir);
        let res = app
            .clone()
            .oneshot(req(
                "DELETE",
                &format!("/core/workflow-storage/{UK}/{HEX}?defer=1"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        // The file MUST remain — reclaim is safe-point only.
        assert!(db.exists(), "delete must not unlink");
        assert!(tomb_path(&dir).exists(), "tombstone persisted");
        assert!(
            !class_dir(&dir).join(format!("{HEX}.tmp")).exists(),
            "tmp cleaned by rename"
        );
        // Second delete idempotent — same 204, still one tombstone.
        let res = app
            .oneshot(req("DELETE", &format!("/core/workflow-storage/{UK}/{HEX}")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(tomb_count(&dir.join("wf-del"), "t1"), 1);
    }

    #[tokio::test]
    async fn wait_is_204_and_validation_still_fail_closed() {
        let dir = tmpdir("wait");
        let (app, _r) = setup(&dir);
        // No pending, no file — still 204 (create() requires ok).
        let res = app
            .clone()
            .oneshot(req(
                "GET",
                &format!("/core/workflow-storage/{UK}/{HEX}?waitForPendingDelete=1"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        // Unknown param → 400; bad hex → 400; undeclared uk → 400.
        let res = app
            .clone()
            .oneshot(req("GET", &format!("/core/workflow-storage/{UK}/{HEX}")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let res = app
            .oneshot(req(
                "GET",
                &format!("/core/workflow-storage/{UK}/zz?waitForPendingDelete=1"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn validation_fail_closed_matrix() {
        let dir = tmpdir("valid");
        let (app, _r) = setup(&dir);
        // uppercase hex → 400
        let bad = "B".repeat(64);
        let res = app
            .clone()
            .oneshot(req("DELETE", &format!("/core/workflow-storage/{UK}/{bad}")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        // undeclared uk → 400
        let res = app
            .clone()
            .oneshot(req(
                "DELETE",
                &format!("/core/workflow-storage/{}/{HEX}", "9".repeat(64)),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        // symlink escape → 400: <hex>.sqlite pointing outside class dir
        let outside = dir.join("outside.sqlite");
        std::fs::write(&outside, b"x").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, class_dir(&dir).join(format!("{HEX}.sqlite")))
            .unwrap();
        let res = app
            .clone()
            .oneshot(req(
                "DELETE",
                &format!("/core/workflow-storage/{UK}/{HEX}?defer=1"),
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        // bare namespace delete → 501
        let res = app
            .oneshot(req("DELETE", &format!("/core/workflow-storage/{UK}")))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn reclaim_unlinks_empty_and_drops_tombstone() {
        let dir = tmpdir("recl-empty");
        let (_app, _r) = setup(&dir);
        write_empty_instance(&dir);
        for ext in [".sqlite-shm", ".sqlite-wal"] {
            std::fs::write(class_dir(&dir).join(format!("{HEX}{ext}")), b"x").unwrap();
        }
        // Tombstone via the real write path.
        let st = WfDelState::new(TenantRegistry::new(), dir.join("wf-del"));
        let inst = ResolvedInstance {
            tenant: "t1".into(),
            class_key: UK.into(),
            class_dir: class_dir(&dir),
            instance_path: class_dir(&dir).join(HEX),
            hex_id: HEX.into(),
        };
        write_tombstone(&st, &inst).unwrap();

        let stats = reclaim_tenant("t1", &dir.join("wf-del"), &None, None);
        assert_eq!(stats.reclaimed, 1, "{stats:?}");
        for ext in SQLITE_EXT {
            assert!(
                !class_dir(&dir).join(format!("{HEX}{ext}")).exists(),
                "{ext} must be unlinked"
            );
        }
        assert!(!tomb_path(&dir).exists(), "tombstone dropped");
        assert_eq!(tomb_count(&dir.join("wf-del"), "t1"), 0);
    }

    #[test]
    fn reclaim_keeps_recreated_and_drops_tombstone() {
        let dir = tmpdir("recl-live");
        let (_app, _r) = setup(&dir);
        let db = write_live_instance(&dir);
        let st = WfDelState::new(TenantRegistry::new(), dir.join("wf-del"));
        let inst = ResolvedInstance {
            tenant: "t1".into(),
            class_key: UK.into(),
            class_dir: class_dir(&dir),
            instance_path: class_dir(&dir).join(HEX),
            hex_id: HEX.into(),
        };
        write_tombstone(&st, &inst).unwrap();

        let stats = reclaim_tenant("t1", &dir.join("wf-del"), &None, None);
        assert_eq!(stats.skipped_nonempty, 1, "{stats:?}");
        assert!(db.exists(), "live instance file MUST be kept");
        assert!(!tomb_path(&dir).exists(), "tombstone dropped");
    }

    #[test]
    fn reclaim_drops_tombstone_when_file_missing() {
        let dir = tmpdir("recl-miss");
        let (_app, _r) = setup(&dir);
        let st = WfDelState::new(TenantRegistry::new(), dir.join("wf-del"));
        let inst = ResolvedInstance {
            tenant: "t1".into(),
            class_key: UK.into(),
            class_dir: class_dir(&dir),
            instance_path: class_dir(&dir).join(HEX),
            hex_id: HEX.into(),
        };
        write_tombstone(&st, &inst).unwrap();
        let stats = reclaim_tenant("t1", &dir.join("wf-del"), &None, None);
        assert_eq!(stats.missing, 1, "{stats:?}");
        assert!(!tomb_path(&dir).exists());
    }

    #[test]
    fn reclaim_keeps_corrupt_tombstone() {
        let dir = tmpdir("recl-corrupt");
        let (_app, _r) = setup(&dir);
        let tp = tomb_path(&dir);
        std::fs::create_dir_all(tp.parent().unwrap()).unwrap();
        std::fs::write(&tp, b"{not json").unwrap();
        let stats = reclaim_tenant("t1", &dir.join("wf-del"), &None, None);
        assert_eq!(stats.failed, 1, "{stats:?}");
        assert!(tp.exists(), "corrupt tombstone kept for next point");
    }

    /// Counter value from the rendered registry (f64 parse of the
    /// `name{...labels} v` line for the given tenant).
    fn metric_counter(name: &str, tenant: &str) -> f64 {
        let rendered = crate::metrics::registry().render();
        rendered
            .lines()
            .filter(|l| l.starts_with(name) && l.contains(&format!("tenant=\"{tenant}\"")))
            .filter_map(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
            .sum()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn churn_1000_deletes_no_baseline_then_one_reclaim_pass() {
        // 1003 real HTTP DELETEs → tombstones only. The delete path
        // NEVER touches the WAL layer, so `retired_total` (the call
        // count that drives `wants_baseline`) must not move at all
        // during the delete storm. ONE safe-point reclaim then calls
        // retire_db at most once per reclaimed file — and the flag is
        // a per-class bool, so N retires still collapse to ≤1
        // baseline emission per class per snapshot.
        let dir = tmpdir("recl-churn");
        let (app, _r) = setup(&dir);
        let class = class_dir(&dir);
        let mut live: Vec<String> = Vec::new();
        for i in 0..3u32 {
            let hex = format!("{:064x}", i + 1);
            let p = class.join(format!("{hex}.sqlite"));
            let conn = rusqlite::Connection::open(&p).unwrap();
            conn.execute(
                "CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID",
                [],
            )
            .unwrap();
            live.push(hex);
        }
        let before = metric_counter("vrt_host_do_wal_retired_total", "t1");
        for i in 0..1003u32 {
            let hex = if i < 3 {
                live[i as usize].clone()
            } else {
                format!("{:064x}", i + 100)
            };
            let res = app
                .clone()
                .oneshot(req(
                    "DELETE",
                    &format!("/core/workflow-storage/{UK}/{hex}?defer=1"),
                ))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::NO_CONTENT);
        }
        assert_eq!(tomb_count(&dir.join("wf-del"), "t1"), 1003);
        assert_eq!(
            metric_counter("vrt_host_do_wal_retired_total", "t1"),
            before,
            "delete storm must not touch the WAL layer at all"
        );
        // Reclaim is capped at MAX_RECLAIM_PER_PASS per pass — loop
        // passes like successive safe points do; bounded count of
        // passes MUST fully drain.
        let mut reclaimed = 0u32;
        let mut missing = 0u32;
        for pass in 0..8 {
            let stats = reclaim_tenant("t1", &dir.join("wf-del"), &None, None);
            reclaimed += stats.reclaimed;
            missing += stats.missing;
            assert_eq!(stats.failed, 0, "{stats:?}");
            if tomb_count(&dir.join("wf-del"), "t1") == 0 {
                break;
            }
            assert!(pass < 7, "tombstones must drain in bounded passes");
        }
        assert_eq!(reclaimed, 3);
        assert_eq!(missing, 1000);
        assert_eq!(tomb_count(&dir.join("wf-del"), "t1"), 0);
        for hex in &live {
            assert!(!class.join(format!("{hex}.sqlite")).exists());
        }
    }
}
