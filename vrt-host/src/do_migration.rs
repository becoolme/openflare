//! P3c-2: frozen cross-cell DO class migration (§6.4 state machine).
//!
//! Source-side coordinator — runs on the pod currently holding the
//! class. Stages (each bounded + retried by the caller's deadline):
//!
//! ```text
//!   read record ──fence──▶ dead-marker(epoch N)      [writers ≤N fenced]
//!        │                wait writer-dead            [registry pid==0]
//!        │                final snapshot → upload     [do/{key}/{N}/snap-*]
//!        │                commit_snapshot_fenced      [manifest N gets ref]
//!        ▼                                         
//!   target cell's OWN converge acquires epoch N+1 → restores the ref
//!   → cutover by desired assignment. Rollback = source re-acquires.
//! ```
//!
//! Safety: after `fence` CAS-commits, NO cell can hold epoch ≤N —
//! the old writer's next renew fails → the supervisor kills it inside
//! `kill_margin_ms` (bounded stop-write window). The final snapshot
//! is taken only after the local writer is confirmed dead, so no
//! committed write is ever lost. If any stage fails BEFORE commit,
//! the source re-acquires (epoch N+1, local data intact) and keeps
//! serving — never a false success, never dual-write.
//!
//! Idempotency: `(uniqueKey, targetEpoch)` — re-driving a completed
//! fence/snapshot for the same epoch is a no-op; a DIFFERENT target
//! epoch is a fresh migration.

use crate::do_placement::{DoPlacement, PlacementEpoch, PlacementError, PlacementScope};
use crate::do_placement_store::{ManifestDoPlacement, SnapRef};
use crate::do_snapshot::DoSnapOp;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use vrt_objstore::ObjectStore;

#[derive(Debug)]
pub enum MigError {
    Placement(PlacementError),
    Snap(DoSnapOp),
    /// Bounded wait exceeded — stage did not complete in time.
    Timeout(&'static str),
    /// Caller-supplied coordinates don't match the manifest.
    Precondition(String),
}
impl std::fmt::Display for MigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Placement(e) => write!(f, "placement: {e}"),
            Self::Snap(e) => write!(f, "snapshot: {e}"),
            Self::Timeout(s) => write!(f, "timeout: {s}"),
            Self::Precondition(s) => write!(f, "precondition: {s}"),
        }
    }
}
impl std::error::Error for MigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigStage {
    Fenced,
    WriterStopped,
    SnapshotUploaded,
    Committed,
    /// Migration object finalized — target cutover is the
    /// control-plane's desired-assignment change (out of band).
    Done,
}

#[derive(Debug)]
pub struct MigrationOutcome {
    pub stage: MigStage,
    pub fenced_epoch: u64,
    pub snapshot: Option<SnapRef>,
    /// Elapsed since the fence — the class's stop-write window.
    pub freeze_ms: u64,
}

/// Source-side frozen-migration driver.
pub struct Migrator {
    pub store: Arc<dyn ObjectStore>,
    pub placement: Arc<ManifestDoPlacement>,
    /// Staging dir root for snapshots.
    pub tmp_root: PathBuf,
    /// Per-stage deadline — MUST be derived from the lease params
    /// (`lease_ms + 2*margin`): the cross-cell stop-write bound is
    /// "time until the fenced writer's next renew + kill margin",
    /// i.e. one renewal cadence plus shutdown — never a magic number.
    pub stage_timeout: Duration,
    /// Reservation window on the fenced dead-marker (ms) — bounds the
    /// source-outage; expiry auto-releases so a crashed coordinator
    /// can't wedge the class.
    pub reserve_ms: u64,
    /// Poll cadence for writer-death.
    pub poll: Duration,
}

impl Migrator {
    /// Stage 1 — fence + RESERVE the current manifest epoch for
    /// `reserved_for` (P3c-2-r1). Idempotent for `(uniqueKey, epoch)`
    /// when the record is already a reserved dead-marker at that
    /// epoch. Without the reservation a fenced source's respawn wins
    /// the next `acquire` — breaking `commit_snapshot_fenced` or
    /// splitting the class across cells. `reserve_ms` bounds the
    /// reservation: expiry auto-releases so an abandoned migration
    /// can never wedge the class.
    pub fn fence(&self, scope: &PlacementScope, reserved_for: &str) -> Result<u64, MigError> {
        let rec = self
            .placement
            .record(scope)
            .map_err(MigError::Placement)?
            .ok_or_else(|| MigError::Precondition("no placement record".into()))?;
        if rec.holder.is_empty() {
            return Ok(rec.epoch); // already fenced (idempotent re-drive)
        }
        self.placement
            .fence_for_migration(
                scope,
                PlacementEpoch(rec.epoch),
                reserved_for,
                self.reserve_ms,
            )
            .map_err(MigError::Placement)?;
        Ok(rec.epoch)
    }

    /// Stage 2 — wait until `alive` reports the local writer is dead
    /// (supervisor kills it inside kill_margin once renew fails).
    /// Bounded: returns Timeout rather than proceed with a live writer.
    pub fn wait_writer_dead(
        &self,
        mut alive: impl FnMut() -> bool,
        deadline: Instant,
    ) -> Result<(), MigError> {
        while Instant::now() < deadline {
            if !alive() {
                return Ok(());
            }
            std::thread::sleep(self.poll.min(Duration::from_millis(200)));
        }
        Err(MigError::Timeout("writer-stop"))
    }

    /// Stage 3+4 — snapshot the (now-frozen) class dir, upload under
    /// the FENCED epoch's keyspace, and commit onto the dead-marker
    /// record via `commit_snapshot_fenced`.
    pub fn snapshot_commit(
        &self,
        scope: &PlacementScope,
        fenced_epoch: u64,
        seq: u64,
        kind: crate::do_placement_store::SnapKind,
    ) -> Result<SnapRef, MigError> {
        // Upload objects + manifest object (plain puts — immutable keys).
        let stage = self.tmp_root.join(format!("mig-{}-{seq}", scope.class_key));
        let _ = std::fs::remove_dir_all(&stage);
        let meta = vrt_sql::do_snap::snapshot_dir(&scope.class_dir, &stage, fenced_epoch, seq)
            .map_err(|e| MigError::Snap(DoSnapOp::Snap(e)))?;
        for f in &meta.files {
            let data =
                std::fs::read(stage.join(&f.path)).map_err(|e| MigError::Snap(DoSnapOp::Io(e)))?;
            let key = crate::do_placement_store::snap_file_key(
                &scope.class_key,
                fenced_epoch,
                seq,
                &f.path,
            );
            vrt_objstore::block_on_store(self.store.put(&key, &data))
                .map_err(|e| MigError::Snap(DoSnapOp::Store(e.to_string())))?;
        }
        let mkey =
            crate::do_placement_store::snap_manifest_key(&scope.class_key, fenced_epoch, seq);
        let mbody = serde_json::to_vec(&meta).map_err(|e| {
            MigError::Snap(DoSnapOp::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e,
            )))
        })?;
        vrt_objstore::block_on_store(self.store.put(&mkey, &mbody))
            .map_err(|e| MigError::Snap(DoSnapOp::Store(e.to_string())))?;
        let _ = std::fs::remove_dir_all(&stage);
        let snap = SnapRef {
            manifest_key: mkey,
            epoch: fenced_epoch,
            seq,
            files: meta.files.len() as u32,
            bytes: meta.files.iter().map(|f| f.size).sum(),
            at_ms: meta.created_ms,
            kind,
        };
        self.placement
            .commit_snapshot_fenced(scope, fenced_epoch, snap.clone())
            .map_err(MigError::Placement)?;
        Ok(snap)
    }

    /// Full source-side drive: precondition → fence+reserve →
    /// wait-dead → snapshot → commit.
    ///
    /// `target_epoch` MUST equal `rec.epoch + 1` — the fenced epoch
    /// plus exactly one takeover (P2); anything else is a caller bug
    /// rejected BEFORE fencing (409). `seq` is taken from the wall
    /// clock (monotone-enough unique per attempt — decoupled from
    /// epoch numbering). `freeze_notify` runs right after the fence
    /// CAS lands — the local path uses it to stop the writer
    /// immediately instead of waiting out a renew cadence (P0-2).
    /// On any failure BEFORE commit the caller may `rollback_source`;
    /// after commit the snapshot IS the restore baseline.
    pub fn migrate(
        &self,
        scope: &PlacementScope,
        alive: impl FnMut() -> bool,
        target_epoch: u64,
        reserved_for: &str,
        freeze_notify: impl FnOnce(),
    ) -> Result<MigrationOutcome, MigError> {
        let t0 = Instant::now();
        // P2: target_epoch precondition BEFORE fencing — a wrong
        // expectation must not disturb the live record.
        let rec = self
            .placement
            .record(scope)
            .map_err(MigError::Placement)?
            .ok_or_else(|| MigError::Precondition("no placement record".into()))?;
        if target_epoch != rec.epoch + 1 {
            return Err(MigError::Precondition(format!(
                "targetEpoch {target_epoch} != fenced epoch + 1 ({})",
                rec.epoch + 1
            )));
        }
        // Fence atomically AT the preconditioned epoch — the inner
        // CAS rejects any record that moved (another acquirer took
        // epoch+1 between read and fence), so a live foreign lease
        // can never be fenced by a stale drive.
        self.placement
            .fence_for_migration(
                scope,
                PlacementEpoch(rec.epoch),
                reserved_for,
                self.reserve_ms,
            )
            .map_err(MigError::Placement)?;
        let epoch = rec.epoch;
        freeze_notify();
        let deadline = t0 + self.stage_timeout;
        self.wait_writer_dead(alive, deadline)?;
        let seq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let snap = self.snapshot_commit(
            scope,
            epoch,
            seq,
            crate::do_placement_store::SnapKind::Final,
        )?;
        Ok(MigrationOutcome {
            stage: MigStage::Committed,
            fenced_epoch: epoch,
            snapshot: Some(snap),
            freeze_ms: t0.elapsed().as_millis() as u64,
        })
    }

    /// Rollback / standalone-resume — release the reservation and
    /// re-acquire the class (epoch+1) so the source resumes from its
    /// (untouched) local dir.
    pub fn rollback_source(
        &self,
        scope: &PlacementScope,
        at_epoch: u64,
    ) -> Result<crate::do_placement::PlacementLease, MigError> {
        self.placement
            .release_reservation(scope, at_epoch)
            .map_err(MigError::Placement)?;
        self.placement.acquire(scope).map_err(MigError::Placement)
    }
}

/// Standalone admin trigger — `POST /v1/do/migrate` on the pod's
/// loopback control listener, same trust model + idempotency shape
/// as `/v1/cron/trigger` (header key must equal
/// `{tenantId}:{workerId}:{uniqueKey}:{targetEpoch}`; bounded LRU
/// dedupe makes replays no-ops).
pub mod admin {
    use super::*;
    use crate::supervisor::TenantRegistry;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::Json;
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Clone)]
    pub struct AdminState {
        pub migrator: Arc<Migrator>,
        pub registry: Arc<TenantRegistry>,
        /// Bounded idempotency LRU (constructed by callers).
        pub seen: Arc<Mutex<HashMap<String, ()>>>,
    }

    /// Strict id shapes — request ids are lookup keys into the
    /// trusted desired view, never path segments. Same whitelist as
    /// the config-side id rules (alnum/`-`/`_`, ≤64).
    pub fn valid_id(s: &str) -> bool {
        !s.is_empty()
            && s.len() <= 64
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }

    /// Resolve the trusted `PlacementScope` for a request triple.
    /// `class_dir` comes ONLY from the tenant's desired DO view —
    /// `tenant_dir/worker_id/unique_key` as `prepare_do_storage`
    /// created it. Any miss is a 404, never a constructed path.
    pub fn resolve_scope(
        st: &AdminState,
        tenant_id: &str,
        worker_id: &str,
        unique_key: &str,
    ) -> Result<PlacementScope, (StatusCode, serde_json::Value)> {
        if !valid_id(tenant_id) || !valid_id(worker_id) {
            return Err((
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error":"id shape"}),
            ));
        }
        if unique_key.len() != 64 || !unique_key.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err((
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error":"uniqueKey shape"}),
            ));
        }
        let rt = st.registry.get(tenant_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                serde_json::json!({"error":"unknown tenant"}),
            )
        })?;
        let view = rt.do_storage_view().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                serde_json::json!({"error":"tenant declares no DO storage"}),
            )
        })?;
        let class_dir = view.class_dir(worker_id, unique_key).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                serde_json::json!({"error":"class not in desired"}),
            )
        })?;
        Ok(PlacementScope {
            tenant: tenant_id.to_string(),
            worker: worker_id.to_string(),
            class_key: unique_key.to_string(),
            class_dir,
        })
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct MigrateReq {
        pub tenant_id: String,
        pub worker_id: String,
        /// Class uniqueKey — strict 64-hex (the tenant boundary).
        pub unique_key: String,
        /// Expected `fenced_epoch + 1` — caller's epoch view must be
        /// current (409 on mismatch, checked BEFORE fencing).
        pub target_epoch: u64,
        /// Who the reservation names — the TARGET cell's holder id
        /// for cross-cell drives. Default `{holder}:mig` =
        /// self-reserved (standalone export path).
        #[serde(default)]
        pub reserved_for: Option<String>,
        /// After commit, release the reservation and let the source
        /// re-acquire epoch+1 (standalone cold-backup semantics —
        /// there is no remote target on a single host). Set false
        /// for a genuine cross-cell handoff.
        #[serde(default)]
        pub resume: Option<bool>,
    }

    pub fn router(state: AdminState) -> axum::Router {
        axum::Router::new()
            .route("/v1/do/migrate", post(migrate))
            .route("/v1/do/snapshot", post(snapshot_now))
            .route("/v1/do/accept-baseline", post(accept_baseline))
            .with_state(state)
    }

    /// `POST /v1/do/accept-baseline` — DO1-r2 operator escape hatch.
    /// A transferred class whose `Final` snapshot never committed
    /// (lost source cell, exhausted retries) refuses to launch on the
    /// target forever by design; this call explicitly accepts the
    /// LATEST committed snapshot — even a periodic one — as the
    /// handoff baseline. RPO = the accepted seq's age; the response
    /// returns it so the operator sees exactly what was trusted.
    /// Trusted-channel only (same loopback admin listener as
    /// /v1/do/migrate); audited via warn-level log + metric.
    async fn accept_baseline(
        State(st): State<AdminState>,
        Json(req): Json<SnapshotReq>,
    ) -> impl IntoResponse {
        let scope = match resolve_scope(&st, &req.tenant_id, &req.worker_id, &req.unique_key) {
            Ok(s) => s,
            Err((code, body)) => return (code, Json(body)),
        };
        let placement = st.migrator.placement.clone();
        let scope2 = scope.clone();
        match tokio::task::spawn_blocking(move || {
            let snap = placement.accept_baseline(&scope)?;
            // DO1-r3 P1-1: the acceptance is PERSISTED inside the
            // placement record (`accepted_baseline{epoch,seq,at_ms}`)
            // — read it back so the response reports what durable
            // state diagnostics will show, not just what we asked for.
            let pin = placement
                .record(&scope2)
                .ok()
                .flatten()
                .and_then(|r| r.accepted_baseline);
            Ok::<_, PlacementError>((snap, pin))
        })
        .await
        {
            Ok(Ok((snap, pin))) => {
                crate::metrics::registry().inc("vrt_do_baseline_accepts_total", &[], 1.0);
                tracing::warn!(
                    tenant = %req.tenant_id,
                    worker = %req.worker_id,
                    class = %req.unique_key,
                    epoch = snap.epoch,
                    seq = snap.seq,
                    bytes = snap.bytes,
                    accepted_at_ms = pin.map(|p| p.at_ms),
                    "do baseline ACCEPTED by admin — periodic snapshot may serve a transferred class (RPO loss accepted)"
                );
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "ok": true,
                        "acceptedEpoch": snap.epoch,
                        "acceptedSeq": snap.seq,
                        "acceptedAtMs": pin.map(|p| p.at_ms),
                        "manifest": snap.manifest_key,
                        "bytes": snap.bytes,
                    })),
                )
            }
            Ok(Err(PlacementError::Precondition(m))) => {
                (StatusCode::CONFLICT, Json(serde_json::json!({"error": m})))
            }
            Ok(Err(e)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": e.to_string()})),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            ),
        }
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SnapshotReq {
        pub tenant_id: String,
        pub worker_id: String,
        pub unique_key: String,
    }

    /// `POST /v1/do/snapshot` — on-demand epoch-fenced snapshot of a
    /// locally-held class. Requires the placement record to still
    /// show THIS host as holder at a usable epoch (fail closed if
    /// the lease moved on). Same loopback control-listener trust
    /// model as `/v1/do/migrate`.
    async fn snapshot_now(
        State(st): State<AdminState>,
        Json(req): Json<SnapshotReq>,
    ) -> impl IntoResponse {
        let scope = match resolve_scope(&st, &req.tenant_id, &req.worker_id, &req.unique_key) {
            Ok(s) => s,
            Err((code, body)) => return (code, Json(body)),
        };
        let migrator = st.migrator.clone();
        let t0 = Instant::now();
        // `block_on_store` inside a runtime thread already offloads to
        // a scoped std thread (drive.rs:39-44 — verified: detects the
        // ambient runtime and spawns a scoped ordinary thread, so no
        // nested block_on panic/deadlock); the remaining hazard is
        // starving a tokio WORKER for the whole upload —
        // spawn_blocking moves it to the elastic blocking pool.
        let outcome = match tokio::task::spawn_blocking(move || {
            let rec = migrator.placement.record(&scope)?;
            let rec =
                rec.ok_or_else(|| PlacementError::Precondition("no placement record".into()))?;
            // Holder must be THIS host — a foreign or empty holder
            // means the lease moved on; only the writer may snapshot.
            if rec.holder.is_empty() {
                return Err(PlacementError::Precondition("class fenced".into()));
            }
            if rec.holder != migrator.placement.holder() {
                return Err(PlacementError::Precondition("not lease holder".into()));
            }
            let seq = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            crate::do_snapshot::upload_snapshot(
                &migrator.store,
                &migrator.placement,
                &scope,
                crate::do_placement::PlacementEpoch(rec.epoch),
                seq,
                &migrator.tmp_root,
                crate::do_placement_store::SnapKind::Periodic,
                // Transfer-side snapshot: the WAL tailer records its
                // own {B,E} anchor at the supervisor's periodic path;
                // a missing anchor just means the chain isn't
                // adoptable for THIS seq (next baseline covers).
                None,
            )
            .map_err(|e| {
                PlacementError::Store(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))
            })
        })
        .await
        {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(PlacementError::Precondition(s))) => Err((StatusCode::CONFLICT, s)),
            Ok(Err(e)) => Err((StatusCode::SERVICE_UNAVAILABLE, e.to_string())),
            Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
        };
        match outcome {
            Ok(r) => {
                crate::metrics::registry().observe(
                    "vrt_do_migration_duration_seconds",
                    &[("op", "snapshot")],
                    t0.elapsed().as_secs_f64(),
                );
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "ok": true,
                        "epoch": r.epoch,
                        "seq": r.seq,
                        "manifest": r.manifest_key,
                        "files": r.files,
                        "bytes": r.bytes,
                    })),
                )
            }
            Err((code, msg)) => (code, Json(serde_json::json!({"error": msg}))),
        }
    }

    async fn migrate(
        State(st): State<AdminState>,
        headers: HeaderMap,
        Json(req): Json<MigrateReq>,
    ) -> impl IntoResponse {
        let idem = format!(
            "{}:{}:{}:{}",
            req.tenant_id, req.worker_id, req.unique_key, req.target_epoch
        );
        let hdr = headers
            .get("x-vrt-do-migrate-key")
            .and_then(|v| v.to_str().ok());
        if hdr != Some(idem.as_str()) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error":"idempotency key mismatch"})),
            );
        }
        {
            let mut seen = st.seen.lock().unwrap();
            if seen.contains_key(&idem) {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({"duplicate":true,"delivered":false})),
                );
            }
            if seen.len() > 4096 {
                seen.clear();
            }
        }
        let scope = match resolve_scope(&st, &req.tenant_id, &req.worker_id, &req.unique_key) {
            Ok(s) => s,
            Err((code, body)) => return (code, Json(body)),
        };
        // P3c-2-r1 standalone semantics: there is no remote target
        // cell in a single-host deployment, so the reservation is
        // SELF-reserved (`{holder}:mig`) and the handler releases it
        // after commit — the source re-acquires E+1 and resumes
        // (cold-backup / fence-export semantics). A platform-driven
        // cross-cell drive passes the TARGET cell's holder id and
        // `resume:false`; the target's own acquire completes the
        // handoff.
        let rt = match st.registry.get(&req.tenant_id) {
            Some(rt) => rt,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error":"unknown tenant"})),
                )
            }
        };
        let reserved_for = match &req.reserved_for {
            // Caller bug must surface, not silently self-reserve —
            // a mistyped target id would otherwise fence the class
            // for a reservation nobody can take.
            Some(s) if !valid_id(s) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error":"reservedFor shape"})),
                )
            }
            Some(s) => s.clone(),
            None => format!("{}:mig", st.migrator.placement.holder()),
        };
        let resume = req.resume.unwrap_or(true);
        let registry = st.registry.clone();
        let migrator = st.migrator.clone();
        let tenant = req.tenant_id.clone();
        let freeze_key = req.unique_key.clone();
        let scope_bg = scope.clone();
        let started = Instant::now();
        // spawn_blocking: the drive blocks on `block_on_store` + poll
        // sleeps — never on a tokio worker thread (P1-2).
        let result = tokio::task::spawn_blocking(move || {
            migrator
                .migrate(
                    &scope_bg,
                    move || {
                        registry
                            .get(&tenant)
                            .map(|r| r.active_pid() != 0)
                            .unwrap_or(false)
                    },
                    req.target_epoch,
                    &reserved_for,
                    // Fence committed → immediate local freeze instead
                    // of waiting out a renew cadence (P0-2).
                    move || rt.request_do_freeze(freeze_key),
                )
                .and_then(|out| {
                    if resume {
                        migrator.rollback_source(&scope_bg, out.fenced_epoch)?;
                    }
                    Ok(out)
                })
        })
        .await;
        let secs = started.elapsed().as_secs_f64();
        let result = match result {
            Ok(r) => r,
            Err(e) => Err(MigError::Precondition(format!("join: {e}"))),
        };
        match result {
            Ok(out) => {
                crate::metrics::registry().observe("vrt_do_migration_duration_seconds", &[], secs);
                st.seen.lock().unwrap().insert(idem, ());
                (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "delivered": true,
                        "fencedEpoch": out.fenced_epoch,
                        "snapshot": out.snapshot.map(|s| s.manifest_key),
                        "freezeMs": out.freeze_ms,
                    })),
                )
            }
            Err(e) => {
                let stage = match &e {
                    MigError::Timeout(s) => *s,
                    MigError::Placement(_) => "fence",
                    MigError::Snap(_) => "snapshot",
                    MigError::Precondition(_) => "precondition",
                };
                crate::metrics::registry().inc(
                    "vrt_do_migration_failures_total",
                    &[("stage", stage)],
                    1.0,
                );
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "error": e.to_string(),
                        "stage": stage,
                    })),
                )
            }
        }
    }
}
