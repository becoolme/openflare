//! Read-only follower (replica) for object-store persisted databases.
//!
//! A writer publishes `snapshot + vrt-seg` objects and a manifest; the follower tails that chain,
//! verifies it, materializes an **immutable generation file** and publishes its applied watermark.
//! It never takes the writer lease, never CASes the manifest and never writes to the object store,
//! so any number of followers can run beside one writer.
//!
//! Crash/consistency model:
//! - every sync builds a *new* `gen-{epoch}-{lsn}.sqlite` (copy of the previous generation plus the
//!   new page images), fsyncs it, runs `PRAGMA integrity_check`, then atomically renames it into
//!   place and only afterwards updates `follower-state.json`;
//! - a crash before the state update leaves an unreferenced generation (pruned next round), and the
//!   previous state + generation remain readable;
//! - readers open a generation with `immutable=1`, so they see one complete commit point for the
//!   lifetime of the connection. Reopen after a sync to observe newer commits.
//!
//! Cost model (P0): one full file copy per applied batch. This is intentionally simple and safe;
//! reflink/incremental page application is a later optimization. Batches are coalesced to the
//! manifest head, so a follower that polls every second copies once per poll, not once per commit.
//!
//! GC interaction: the writer's GC deletes segments covered by the latest snapshot. If a follower
//! falls behind that window, the missing segment is detected and the follower re-bootstraps from
//! the current snapshot instead of serving a partial chain. Bounded retries prevent a loop against
//! a persistently broken store.

use crate::clock::Clock;
use crate::paths::{self, ResourceKind};
use crate::persist::{epoch_for_lsn, EpochSpan, Manifest, PersistError, Persister, PromotedWriter};
use crate::seg;
use crate::service::PersistenceConfig;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use vrt_objstore::{block_on_store, ObjectStore, StoreError};

/// State file name inside a follower root.
pub const STATE_FILE: &str = "follower-state.json";
/// Generation file prefix.
pub const GEN_PREFIX: &str = "gen-";
/// Generation file suffix.
pub const GEN_SUFFIX: &str = ".sqlite";

const STATE_VERSION: u32 = 1;
const MAX_BOOTSTRAP_ATTEMPTS: u32 = 2;

/// Follower configuration (`follower` in the config file).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FollowerConfig {
    /// Database id to replicate (26 lowercase alphanumerics).
    pub database_id: String,
    /// Tail poll interval (ms).
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Number of materialized generations to keep (>= 1). The current one is always kept; the
    /// previous one is kept for readers that still hold an open connection.
    #[serde(default = "default_retain_generations")]
    pub retain_generations: usize,
}

fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_retain_generations() -> usize {
    2
}

impl FollowerConfig {
    /// Fail-closed validation.
    pub fn validate(&self) -> Result<(), &'static str> {
        if paths::validate_database_id(&self.database_id).is_err() {
            return Err("follower.databaseId must be 26 lowercase alphanumerics");
        }
        if !(50..=3_600_000).contains(&self.poll_interval_ms) {
            return Err("follower.pollIntervalMs out of range [50, 3600000]");
        }
        if !(1..=16).contains(&self.retain_generations) {
            return Err("follower.retainGenerations out of range [1, 16]");
        }
        Ok(())
    }
}

/// Follower process config: the same `[persistence]` section as the writer plus the follower block.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FollowerServiceConfig {
    /// Local root for generations (disposable cache; may be removed and rebuilt at any time).
    pub db_root: PathBuf,
    /// Object-store source (must be `fs` or `s3`; `none` has nothing to replicate).
    pub persistence: PersistenceConfig,
    /// Follower settings.
    pub follower: FollowerConfig,
    /// Optional read-only D1 wire listener (exactly one of `unixDir` / `listenTcp`).
    #[serde(default)]
    pub unix_dir: Option<PathBuf>,
    #[serde(default)]
    pub listen_tcp: Option<String>,
    /// Service-to-service auth secret (0600). Required when a listener is configured.
    #[serde(default)]
    pub auth_secret_file: Option<PathBuf>,
    /// Bookmark signing key (0600, the writer's key). Optional: without it the replica serves
    /// reads but returns no bookmark and rejects bookmark constraints.
    #[serde(default)]
    pub bookmark_secret_file: Option<PathBuf>,
    /// Opt-in: local writer `dbRoot` for `POST /internal/dbs/{id}/promote`.
    /// The route is absent unless this is set; when set, the follower process
    /// may CAS the manifest and adopt a generation into this root.
    #[serde(default)]
    pub promote_writer_db_root: Option<PathBuf>,
}

impl FollowerServiceConfig {
    /// Fail-closed validation.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.persistence.validate()?;
        if self.persistence.backend == "none" {
            return Err("follower requires persistence.backend fs|s3");
        }
        self.follower.validate()?;
        match (self.unix_dir.is_some(), self.listen_tcp.is_some()) {
            (true, true) => return Err("follower unixDir|listenTcp: at most one"),
            (false, false) => {
                if self.promote_writer_db_root.is_some() {
                    return Err("follower listenTcp|unixDir required when promote is enabled");
                }
                return Ok(());
            }
            _ => {}
        }
        if self.auth_secret_file.is_none() {
            return Err("follower authSecretFile required when serving");
        }
        if let Some(addr) = &self.listen_tcp {
            if addr.parse::<std::net::SocketAddr>().is_err() {
                return Err("follower.listenTcp invalid");
            }
        }
        if self.promote_writer_db_root.is_some() && self.auth_secret_file.is_none() {
            return Err("follower authSecretFile required when promote is enabled");
        }
        Ok(())
    }
}

/// Follower phase reported by [`Follower::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FollowerPhase {
    /// No manifest yet (database not created).
    Empty,
    /// The local generation is older than the manifest head.
    CatchingUp,
    /// The local generation is exactly at the manifest head.
    CaughtUp,
    /// The local generation is ahead of the manifest (should not happen; requires re-bootstrap).
    Stale,
}

/// Persisted watermark. It describes the generation readers can open.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FollowerState {
    pub v: u32,
    pub database_id: String,
    /// Epoch that owns `lsn`.
    pub epoch: u64,
    /// Applied commit lsn (inclusive).
    pub lsn: u64,
    /// Snapshot baseline this generation was built from (0 = chain from empty).
    pub snapshot_lsn: u64,
    /// Generation file name inside the follower root.
    pub generation: String,
    /// Manifest head observed at the last successful sync.
    pub head_epoch: u64,
    pub head_lsn: u64,
    /// Wall clock (unix ms) of the last successful sync.
    pub updated_at_ms: u64,
}

impl FollowerState {
    /// Whether this applied point satisfies a `(epoch, lsn)` bookmark.
    ///
    /// Bookmarks are `(epoch, lsn)` ordered lexicographically (the epoch precedes every lsn of
    /// the next epoch), so a replica at `(self.epoch, self.lsn)` can serve every earlier point.
    pub fn servable(&self, epoch: u64, lsn: u64) -> bool {
        (self.epoch, self.lsn) >= (epoch, lsn)
    }
}

/// Live status (state + current manifest head).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowerStatus {
    pub database_id: String,
    pub phase: FollowerPhase,
    pub applied_epoch: u64,
    pub applied_lsn: u64,
    pub head_epoch: u64,
    pub head_lsn: u64,
    pub snapshot_lsn: Option<u64>,
    pub generation: Option<String>,
    pub generation_bytes: Option<u64>,
    pub updated_at_ms: Option<u64>,
    pub last_error: Option<String>,
}

/// Read-only promotion preflight. This never CASes the manifest or mutates
/// local state; it only tells the operator whether a manual promotion would
/// start from a readable, caught-up generation.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromotionReadiness {
    pub database_id: String,
    pub ready: bool,
    pub reason: String,
    pub phase: FollowerPhase,
    pub applied_epoch: u64,
    pub applied_lsn: u64,
    pub head_epoch: u64,
    pub head_lsn: u64,
    pub generation: Option<String>,
    pub generation_exists: bool,
}

/// Read-only CAS inputs for a future manual promotion. The actual promote
/// command must still compare `expected_epoch`/`expected_lsn`, write the next
/// epoch manifest with CAS, and only then start the writer.
#[derive(Debug, Clone)]
pub struct PromotionPlan {
    pub database_id: String,
    pub expected_epoch: u64,
    pub expected_lsn: u64,
    pub next_epoch: u64,
    pub generation: String,
    pub chain: Vec<EpochSpan>,
}

impl FollowerStatus {
    /// Commits between the applied watermark and the manifest head (0 when caught up).
    pub fn lag_commits(&self) -> u64 {
        self.head_lsn.saturating_sub(self.applied_lsn)
    }
}

/// Result of one [`Follower::sync_once`] call.
#[derive(Debug, Clone)]
pub struct SyncReport {
    /// A full bootstrap (snapshot + chain, or chain from empty) was performed.
    pub bootstrapped: bool,
    /// Segments applied in this call.
    pub segments_applied: u32,
    /// Applied lsn before this call.
    pub from_lsn: u64,
    /// Applied lsn after this call.
    pub to_lsn: u64,
    /// Manifest head at the start of this call.
    pub head_epoch: u64,
    pub head_lsn: u64,
    /// Phase after the call.
    pub phase: FollowerPhase,
}

impl SyncReport {
    /// Whether the follower is now at the head captured for this call.
    pub fn caught_up(&self) -> bool {
        self.phase == FollowerPhase::CaughtUp
    }
    /// Whether this call changed the local generation.
    pub fn progressed(&self) -> bool {
        self.bootstrapped || self.segments_applied > 0
    }
}

/// Follower error (fail closed: no partially applied generation is ever published).
#[derive(Debug, thiserror::Error)]
pub enum FollowerError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("manifest/object corrupt: {0}")]
    Corrupt(String),
    #[error("database tombstoned (deleted)")]
    Tombstoned,
    #[error("local io: {0}")]
    LocalIo(String),
    #[error("follower has no readable generation yet")]
    NotReady,
    /// Internal: re-read the manifest and bootstrap from the current snapshot.
    #[error("re-bootstrap required: {0}")]
    RetryBootstrap(String),
    /// The promotion CAS or local adoption failed (fail closed).
    #[error("promotion failed: {0}")]
    Promotion(#[from] PersistError),
}

/// Read-only follower for one database.
pub struct Follower {
    store: Arc<dyn ObjectStore>,
    kind: ResourceKind,
    database_id: String,
    root: PathBuf,
    clock: Arc<dyn Clock>,
    retain_generations: usize,
    sync_lock: Mutex<()>,
    last_error: Mutex<Option<String>>,
}

impl std::fmt::Debug for Follower {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Follower")
            .field("database_id", &self.database_id)
            .field("kind", &self.kind)
            .field("root", &self.root)
            .field("retain_generations", &self.retain_generations)
            .finish_non_exhaustive()
    }
}

impl Follower {
    /// Create a follower. The root directory is created with mode 0700.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        kind: ResourceKind,
        database_id: impl Into<String>,
        root: PathBuf,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, FollowerError> {
        let database_id = database_id.into();
        paths::validate_database_id(&database_id)
            .map_err(|e| FollowerError::Corrupt(format!("database id: {e}")))?;
        create_private_dir(&root)?;
        Ok(Self {
            store,
            kind,
            database_id,
            root,
            clock,
            retain_generations: default_retain_generations(),
            sync_lock: Mutex::new(()),
            last_error: Mutex::new(None),
        })
    }

    /// Set how many generations to keep (values below 1 are clamped to 1).
    pub fn with_retain_generations(mut self, n: usize) -> Self {
        self.retain_generations = n.max(1);
        self
    }

    /// Database id this follower replicates.
    pub fn database_id(&self) -> &str {
        &self.database_id
    }

    /// Follower root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Path of the currently published generation, if any.
    pub fn current_db_path(&self) -> Result<Option<PathBuf>, FollowerError> {
        let Some(state) = self.read_state()? else {
            return Ok(None);
        };
        let path = self.root.join(&state.generation);
        Ok(path.exists().then_some(path))
    }

    /// Open the current generation read-only.
    ///
    /// The connection is pinned to one immutable commit point (SQLite `immutable=1`): it always
    /// sees a complete database and never observes a generation being built. Reopen after
    /// [`Follower::sync_once`] to read newer commits.
    pub fn open_readonly(&self) -> Result<Connection, FollowerError> {
        let Some(state) = self.read_state()? else {
            return Err(FollowerError::NotReady);
        };
        if state.lsn == 0 {
            return Err(FollowerError::NotReady);
        }
        let path = self.root.join(&state.generation);
        let path = std::fs::canonicalize(&path)
            .map_err(|e| FollowerError::LocalIo(format!("generation canonicalize: {e}")))?;
        // `immutable=1` is correct here: generation files are never modified after publication.
        // It also lets SQLite read a WAL-mode header without requiring a writable `-shm`.
        let uri = format!("file:{}?immutable=1", sqlite_uri_path(&path));
        Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(|e| FollowerError::LocalIo(format!("open generation read-only: {e}")))
    }

    /// Read the persisted watermark.
    pub fn state(&self) -> Result<Option<FollowerState>, FollowerError> {
        self.read_state()
    }

    /// Preflight for a manual writer promotion. Fail closed: any missing or
    /// stale state is reported, never inferred as ready.
    pub fn promotion_readiness(&self) -> Result<PromotionReadiness, FollowerError> {
        let status = self.status()?;
        let state = self.state()?;
        let generation_exists = status
            .generation
            .as_ref()
            .and_then(|g| std::fs::metadata(self.root.join(g)).ok())
            .is_some();
        let (ready, reason) = if state.is_none() {
            (false, "no-state")
        } else if status.applied_lsn > status.head_lsn {
            (false, "ahead-of-manifest")
        } else if status.applied_lsn < status.head_lsn {
            (false, "catching-up")
        } else if status.applied_lsn == 0 {
            (false, "empty")
        } else if !generation_exists {
            (false, "generation-missing")
        } else {
            (true, "ready")
        };
        Ok(PromotionReadiness {
            database_id: self.database_id.clone(),
            ready,
            reason: reason.to_string(),
            phase: status.phase,
            applied_epoch: status.applied_epoch,
            applied_lsn: status.applied_lsn,
            head_epoch: status.head_epoch,
            head_lsn: status.head_lsn,
            generation: status.generation,
            generation_exists,
        })
    }

    /// CAS inputs for a future manual promotion. Fail closed unless
    /// `promotion_readiness()` is ready.
    pub fn promotion_plan(&self) -> Result<PromotionPlan, FollowerError> {
        let readiness = self.promotion_readiness()?;
        if !readiness.ready {
            return Err(FollowerError::NotReady);
        }
        let manifest = self.read_manifest()?.ok_or(FollowerError::NotReady)?;
        let generation = readiness.generation.ok_or(FollowerError::NotReady)?;
        if epoch_for_lsn(&manifest.chain, manifest.lsn).is_none() {
            return Err(FollowerError::Corrupt(
                "manifest epoch chain does not cover its head lsn".into(),
            ));
        }
        Ok(PromotionPlan {
            database_id: self.database_id.clone(),
            expected_epoch: manifest.epoch,
            expected_lsn: manifest.lsn,
            next_epoch: manifest.epoch.saturating_add(1),
            generation,
            chain: manifest.chain,
        })
    }

    /// Promote this follower's verified generation to writer.
    ///
    /// The `persister` must be configured with the promoted writer's local
    /// `db_root` (and the shared object store). This performs the manifest CAS
    /// and adopts the generation as the writer's local image. It does not
    /// start the serving process; callers must release or keep the returned
    /// lease according to how the writer is launched.
    pub fn promote(&self, persister: &Persister) -> Result<PromotedWriter, FollowerError> {
        let plan = self.promotion_plan()?;
        let generation = self.root.join(&plan.generation);
        Ok(persister.promote_from_generation(
            &self.database_id,
            plan.expected_epoch,
            plan.expected_lsn,
            &generation,
        )?)
    }

    /// Compute live status (reads the manifest).
    pub fn status(&self) -> Result<FollowerStatus, FollowerError> {
        let manifest = self.read_manifest()?;
        let state = self.read_state()?;
        let head_lsn = manifest.as_ref().map(|m| m.lsn).unwrap_or(0);
        let head_epoch = manifest
            .as_ref()
            .map(|m| epoch_or_zero(&m.chain, m.lsn))
            .unwrap_or(0);
        let applied_lsn = state.as_ref().map(|s| s.lsn).unwrap_or(0);
        let phase = if state.is_none() {
            FollowerPhase::Empty
        } else if applied_lsn > head_lsn {
            FollowerPhase::Stale
        } else if applied_lsn == head_lsn {
            FollowerPhase::CaughtUp
        } else {
            FollowerPhase::CatchingUp
        };
        let generation_bytes = state
            .as_ref()
            .and_then(|s| std::fs::metadata(self.root.join(&s.generation)).ok())
            .map(|m| m.len());
        Ok(FollowerStatus {
            database_id: self.database_id.clone(),
            phase,
            applied_epoch: state.as_ref().map(|s| s.epoch).unwrap_or(0),
            applied_lsn,
            head_epoch,
            head_lsn,
            snapshot_lsn: manifest
                .as_ref()
                .and_then(|m| m.snapshot.as_ref().map(|s| s.lsn)),
            generation: state.as_ref().map(|s| s.generation.clone()),
            generation_bytes,
            updated_at_ms: state.as_ref().map(|s| s.updated_at_ms),
            last_error: self.last_error.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        })
    }

    /// Tail once: bootstrap if needed, otherwise apply every segment up to the current head.
    ///
    /// The whole batch is coalesced into one generation rename, so readers never observe a
    /// partially applied chain. A missing segment (writer GC advanced past this follower) triggers
    /// one bounded re-bootstrap from the current snapshot.
    pub fn sync_once(&self) -> Result<SyncReport, FollowerError> {
        let _guard = self.sync_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut attempt = 0;
        loop {
            match self.sync_inner() {
                Ok(report) => {
                    *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = None;
                    return Ok(report);
                }
                Err(FollowerError::RetryBootstrap(reason)) if attempt + 1 < MAX_BOOTSTRAP_ATTEMPTS => {
                    attempt += 1;
                    tracing::warn!(
                        db = %self.database_id,
                        reason = %reason,
                        "follower re-bootstrap"
                    );
                }
                Err(FollowerError::RetryBootstrap(reason)) => {
                    let err = FollowerError::Corrupt(reason);
                    *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(err.to_string());
                    return Err(err);
                }
                Err(e) => {
                    *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.to_string());
                    return Err(e);
                }
            }
        }
    }

    fn sync_inner(&self) -> Result<SyncReport, FollowerError> {
        let Some(manifest) = self.read_manifest()? else {
            return Ok(SyncReport {
                bootstrapped: false,
                segments_applied: 0,
                from_lsn: 0,
                to_lsn: 0,
                head_epoch: 0,
                head_lsn: 0,
                phase: FollowerPhase::Empty,
            });
        };
        if manifest.tombstone {
            return Err(FollowerError::Tombstoned);
        }
        let head_epoch = epoch_or_zero(&manifest.chain, manifest.lsn);
        let head_lsn = manifest.lsn;
        let snapshot_lsn = manifest.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0);
        let state = self.read_state()?;

        let rebootstrap = match &state {
            None => true,
            Some(st) => {
                st.lsn > head_lsn
                    || snapshot_lsn > st.lsn
                    || (st.lsn > 0 && epoch_for_lsn(&manifest.chain, st.lsn) != Some(st.epoch))
                    || !self.root.join(&st.generation).exists()
            }
        };
        if rebootstrap {
            return self.bootstrap(&manifest, head_epoch);
        }

        let state = state.expect("checked above");
        if state.lsn == head_lsn {
            // Nothing to apply. Refresh head/snapshot fields only when they changed, to keep the
            // steady-state poll read-only and avoid a write every second.
            if state.head_lsn != head_lsn
                || state.head_epoch != head_epoch
                || state.snapshot_lsn != snapshot_lsn
            {
                self.write_state(&FollowerState {
                    head_lsn,
                    head_epoch,
                    snapshot_lsn,
                    updated_at_ms: self.clock.now_ms(),
                    ..state.clone()
                })?;
            }
            return Ok(SyncReport {
                bootstrapped: false,
                segments_applied: 0,
                from_lsn: state.lsn,
                to_lsn: state.lsn,
                head_epoch,
                head_lsn,
                phase: FollowerPhase::CaughtUp,
            });
        }

        self.incremental(&manifest, &state, head_epoch)
    }

    fn bootstrap(&self, manifest: &Manifest, head_epoch: u64) -> Result<SyncReport, FollowerError> {
        let tmp = TempFile::new(self.tmp_path("bootstrap"));
        let mut page_size = 0u32;
        let base_lsn = match &manifest.snapshot {
            Some(snap) => {
                if snap.lsn > manifest.lsn {
                    return Err(FollowerError::Corrupt(
                        "snapshot beyond manifest lsn".into(),
                    ));
                }
                let out = block_on_store(self.store.get_to_file(&snap.key, &tmp.path))?;
                let Some(out) = out else {
                    return Err(FollowerError::RetryBootstrap(format!(
                        "snapshot {} missing",
                        snap.key
                    )));
                };
                if out.len != snap.size {
                    return Err(FollowerError::Corrupt(format!(
                        "snapshot size mismatch: manifest {} actual {}",
                        snap.size, out.len
                    )));
                }
                if out.sha256 != snap.sha256 {
                    return Err(FollowerError::Corrupt(
                        "snapshot sha256 mismatch".into(),
                    ));
                }
                page_size = read_page_size(&tmp.path)?;
                snap.lsn
            }
            None => {
                std::fs::File::create(&tmp.path)
                    .map_err(|e| FollowerError::LocalIo(format!("bootstrap create: {e}")))?;
                0
            }
        };

        if manifest.lsn > base_lsn {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp.path)
                .map_err(|e| FollowerError::LocalIo(format!("bootstrap open: {e}")))?;
            self.apply_range(
                &mut f,
                &manifest.chain,
                base_lsn + 1,
                manifest.lsn,
                &mut page_size,
            )?;
            f.sync_all()
                .map_err(|e| FollowerError::LocalIo(format!("bootstrap fsync: {e}")))?;
        }

        if manifest.lsn > 0 {
            verify_integrity(&tmp.path)?;
        }
        clear_sidecars(&tmp.path);

        let generation = gen_name(head_epoch, manifest.lsn);
        let target = self.root.join(&generation);
        std::fs::rename(&tmp.path, &target)
            .map_err(|e| FollowerError::LocalIo(format!("bootstrap rename: {e}")))?;
        fsync_dir(&self.root)?;

        let state = FollowerState {
            v: STATE_VERSION,
            database_id: self.database_id.clone(),
            epoch: epoch_or_zero(&manifest.chain, manifest.lsn),
            lsn: manifest.lsn,
            snapshot_lsn: manifest.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0),
            generation,
            head_epoch,
            head_lsn: manifest.lsn,
            updated_at_ms: self.clock.now_ms(),
        };
        self.write_state(&state)?;
        self.prune_generations(&state.generation);

        let applied = manifest.lsn.saturating_sub(base_lsn) as u32;
        Ok(SyncReport {
            bootstrapped: true,
            segments_applied: applied,
            from_lsn: base_lsn,
            to_lsn: manifest.lsn,
            head_epoch,
            head_lsn: manifest.lsn,
            phase: FollowerPhase::CaughtUp,
        })
    }

    fn incremental(
        &self,
        manifest: &Manifest,
        state: &FollowerState,
        head_epoch: u64,
    ) -> Result<SyncReport, FollowerError> {
        let from_lsn = state.lsn;
        let current = self.root.join(&state.generation);
        let tmp = TempFile::new(self.tmp_path("apply"));
        std::fs::copy(&current, &tmp.path)
            .map_err(|e| FollowerError::LocalIo(format!("generation copy: {e}")))?;
        let mut page_size = read_page_size(&tmp.path)?;

        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp.path)
                .map_err(|e| FollowerError::LocalIo(format!("apply open: {e}")))?;
            self.apply_range(
                &mut f,
                &manifest.chain,
                from_lsn + 1,
                manifest.lsn,
                &mut page_size,
            )?;
            f.sync_all()
                .map_err(|e| FollowerError::LocalIo(format!("apply fsync: {e}")))?;
        }

        verify_integrity(&tmp.path)?;
        clear_sidecars(&tmp.path);

        let generation = gen_name(head_epoch, manifest.lsn);
        let target = self.root.join(&generation);
        std::fs::rename(&tmp.path, &target)
            .map_err(|e| FollowerError::LocalIo(format!("apply rename: {e}")))?;
        fsync_dir(&self.root)?;

        let next = FollowerState {
            v: STATE_VERSION,
            database_id: self.database_id.clone(),
            epoch: epoch_or_zero(&manifest.chain, manifest.lsn),
            lsn: manifest.lsn,
            snapshot_lsn: manifest
                .snapshot
                .as_ref()
                .map(|s| s.lsn)
                .unwrap_or(0)
                .max(state.snapshot_lsn),
            generation,
            head_epoch,
            head_lsn: manifest.lsn,
            updated_at_ms: self.clock.now_ms(),
        };
        self.write_state(&next)?;
        self.prune_generations(&next.generation);

        Ok(SyncReport {
            bootstrapped: false,
            segments_applied: (manifest.lsn - from_lsn) as u32,
            from_lsn,
            to_lsn: manifest.lsn,
            head_epoch,
            head_lsn: manifest.lsn,
            phase: FollowerPhase::CaughtUp,
        })
    }

    /// Verify and apply segments `from_lsn..=to_lsn` on top of an existing image.
    fn apply_range(
        &self,
        file: &mut std::fs::File,
        chain: &[EpochSpan],
        from_lsn: u64,
        to_lsn: u64,
        page_size: &mut u32,
    ) -> Result<(), FollowerError> {
        let mut prev = from_lsn - 1;
        for lsn in from_lsn..=to_lsn {
            let epoch = epoch_for_lsn(chain, lsn).ok_or_else(|| {
                FollowerError::RetryBootstrap(format!("no epoch covers lsn {lsn}"))
            })?;
            let key = seg::seg_key(self.kind, &self.database_id, epoch, lsn);
            let Some(obj) = block_on_store(self.store.get(&key))? else {
                return Err(FollowerError::RetryBootstrap(format!(
                    "segment lsn {lsn} missing (writer GC advanced past this follower)"
                )));
            };
            let segment = seg::decode(&obj.body)
                .map_err(|e| FollowerError::Corrupt(format!("segment lsn {lsn}: {e}")))?;
            if segment.database_id != self.database_id {
                return Err(FollowerError::Corrupt(format!(
                    "segment lsn {lsn}: database id mismatch"
                )));
            }
            if segment.lsn != lsn || segment.epoch != epoch {
                return Err(FollowerError::Corrupt(format!(
                    "segment lsn {lsn}: header lsn/epoch mismatch ({} / {})",
                    segment.lsn, segment.epoch
                )));
            }
            if segment.prev_lsn != prev {
                return Err(FollowerError::RetryBootstrap(format!(
                    "segment chain break at lsn {lsn}: expected prev {prev}, got {}",
                    segment.prev_lsn
                )));
            }
            if *page_size == 0 {
                validate_page_size(segment.page_size)?;
                *page_size = segment.page_size;
            } else if segment.page_size != *page_size {
                return Err(FollowerError::Corrupt(format!(
                    "segment lsn {lsn}: page size {} != {}",
                    segment.page_size, *page_size
                )));
            }
            seg::apply_frames(file, &segment)
                .map_err(|e| FollowerError::Corrupt(format!("segment lsn {lsn} apply: {e}")))?;
            prev = lsn;
        }
        Ok(())
    }

    fn read_manifest(&self) -> Result<Option<Manifest>, FollowerError> {
        let key = seg::manifest_key(self.kind, &self.database_id);
        let Some(obj) = block_on_store(self.store.get(&key))? else {
            return Ok(None);
        };
        serde_json::from_slice::<Manifest>(&obj.body)
            .map(Some)
            .map_err(|e| FollowerError::Corrupt(format!("manifest json: {e}")))
    }

    fn state_path(&self) -> PathBuf {
        self.root.join(STATE_FILE)
    }

    fn read_state(&self) -> Result<Option<FollowerState>, FollowerError> {
        let path = self.state_path();
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(FollowerError::LocalIo(format!("state read: {e}"))),
        };
        match serde_json::from_slice::<FollowerState>(&raw) {
            Ok(state) => Ok(Some(state)),
            Err(e) => {
                // A corrupt/unreadable watermark is safe to rebuild from the object store.
                tracing::warn!(
                    db = %self.database_id,
                    error = %e,
                    "follower state unreadable; rebuilding from the object store"
                );
                Ok(None)
            }
        }
    }

    fn write_state(&self, state: &FollowerState) -> Result<(), FollowerError> {
        let bytes = serde_json::to_vec_pretty(state)
            .map_err(|e| FollowerError::Corrupt(format!("state json: {e}")))?;
        let path = self.state_path();
        let tmp = self.tmp_path("state");
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| FollowerError::LocalIo(format!("state create: {e}")))?;
            f.write_all(&bytes)
                .map_err(|e| FollowerError::LocalIo(format!("state write: {e}")))?;
            f.sync_all()
                .map_err(|e| FollowerError::LocalIo(format!("state fsync: {e}")))?;
        }
        std::fs::rename(&tmp, &path)
            .map_err(|e| FollowerError::LocalIo(format!("state rename: {e}")))?;
        fsync_dir(&self.root)
    }

    /// Keep the current generation plus `retain_generations - 1` previous ones.
    fn prune_generations(&self, current: &str) {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return;
        };
        let mut generations: Vec<String> = entries
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| {
                name.starts_with(GEN_PREFIX)
                    && name.ends_with(GEN_SUFFIX)
                    && parse_gen_name(name).is_some()
            })
            .collect();
        // Fixed-width names: lexical order equals (epoch, lsn) order.
        generations.sort_by(|a, b| b.cmp(a));
        for name in generations.into_iter().skip(self.retain_generations) {
            if name == current {
                continue;
            }
            if let Err(e) = std::fs::remove_file(self.root.join(&name)) {
                tracing::warn!(db = %self.database_id, file = %name, error = %e,
                    "follower generation prune failed (retried next round)");
            }
        }
    }

    fn tmp_path(&self, purpose: &str) -> PathBuf {
        self.root.join(format!(".{purpose}-{}", unique_suffix()))
    }
}

fn epoch_or_zero(chain: &[EpochSpan], lsn: u64) -> u64 {
    if lsn == 0 {
        0
    } else {
        epoch_for_lsn(chain, lsn).unwrap_or(0)
    }
}

fn gen_name(epoch: u64, lsn: u64) -> String {
    format!("{GEN_PREFIX}{epoch:016x}-{lsn:016x}{GEN_SUFFIX}")
}

fn parse_gen_name(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix(GEN_PREFIX)?.strip_suffix(GEN_SUFFIX)?;
    let (epoch, lsn) = rest.split_once('-')?;
    if epoch.len() != 16 || lsn.len() != 16 {
        return None;
    }
    Some((
        u64::from_str_radix(epoch, 16).ok()?,
        u64::from_str_radix(lsn, 16).ok()?,
    ))
}

fn create_private_dir(path: &Path) -> Result<(), FollowerError> {
    std::fs::create_dir_all(path)
        .map_err(|e| FollowerError::LocalIo(format!("follower dir {}: {e}", path.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| FollowerError::LocalIo(format!("follower dir mode: {e}")))?;
    }
    Ok(())
}

fn read_page_size(path: &Path) -> Result<u32, FollowerError> {
    let len = std::fs::metadata(path)
        .map_err(|e| FollowerError::LocalIo(format!("image stat: {e}")))?
        .len();
    if len < 18 {
        return Ok(0);
    }
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)
        .map_err(|e| FollowerError::LocalIo(format!("image open: {e}")))?;
    let mut hdr = [0u8; 2];
    f.seek(SeekFrom::Start(16))
        .and_then(|_| f.read_exact(&mut hdr))
        .map_err(|e| FollowerError::LocalIo(format!("image header: {e}")))?;
    let v = u16::from_be_bytes(hdr) as u32;
    let page_size = if v == 1 { 65_536 } else { v };
    validate_page_size(page_size)?;
    Ok(page_size)
}

fn validate_page_size(page_size: u32) -> Result<(), FollowerError> {
    if (512..=65_536).contains(&page_size) && page_size.is_power_of_two() {
        Ok(())
    } else {
        Err(FollowerError::Corrupt(format!(
            "invalid page size {page_size}"
        )))
    }
}

fn verify_integrity(path: &Path) -> Result<(), FollowerError> {
    match crate::wal::integrity_ok(path) {
        Ok(true) => Ok(()),
        Ok(false) => Err(FollowerError::Corrupt(
            "replica image failed integrity_check".into(),
        )),
        Err(e) => Err(FollowerError::Corrupt(format!("integrity_check: {e}"))),
    }
}

fn clear_sidecars(path: &Path) {
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut p = path.as_os_str().to_os_string();
        p.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(p));
    }
}

fn fsync_dir(path: &Path) -> Result<(), FollowerError> {
    let dir = std::fs::File::open(path)
        .map_err(|e| FollowerError::LocalIo(format!("dir open: {e}")))?;
    dir.sync_all()
        .map_err(|e| FollowerError::LocalIo(format!("dir fsync: {e}")))
}

fn unique_suffix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "{}-{nanos}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

pub(crate) fn sqlite_uri_path(path: &Path) -> String {
    let s = path.to_string_lossy();
    let mut out = String::with_capacity(s.len() + 16);
    for ch in s.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '?' => out.push_str("%3f"),
            '#' => out.push_str("%23"),
            other => out.push(other),
        }
    }
    out
}

/// Removes a partially written file (and SQLite sidecars) unless it was renamed into place.
struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        clear_sidecars(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> FollowerConfig {
        FollowerConfig {
            database_id: "abcdefghijklmnopqrstuvwxyz".into(),
            poll_interval_ms: 1000,
            retain_generations: 2,
        }
    }

    #[test]
    fn config_validation() {
        assert!(cfg().validate().is_ok());
        let mut bad = cfg();
        bad.database_id = "SHORT".into();
        assert!(bad.validate().is_err());
        let mut bad = cfg();
        bad.poll_interval_ms = 0;
        assert!(bad.validate().is_err());
        let mut bad = cfg();
        bad.retain_generations = 0;
        assert!(bad.validate().is_err());
    }

    #[test]
    fn generation_names_roundtrip_and_sort() {
        assert_eq!(parse_gen_name(&gen_name(1, 2)), Some((1, 2)));
        assert!(gen_name(1, 9) < gen_name(1, 10));
        assert!(parse_gen_name("gen-1-2.sqlite").is_none());
        assert!(parse_gen_name("other.sqlite").is_none());
    }

    #[test]
    fn service_config_parses_toml_and_json_with_defaults() {
        let toml_cfg = r#"
dbRoot = "/tmp/replica"
[persistence]
backend = "fs"
[persistence.fs]
root = "/tmp/persistence"
[follower]
databaseId = "abcdefghijklmnopqrstuvwxyz"
"#;
        let parsed: FollowerServiceConfig = toml::from_str(toml_cfg).unwrap();
        assert_eq!(parsed.follower.poll_interval_ms, 1000);
        assert_eq!(parsed.follower.retain_generations, 2);
        assert!(parsed.validate().is_ok());

        let json_cfg = r#"{
            "dbRoot": "/tmp/replica",
            "persistence": {"backend": "fs", "fs": {"root": "/tmp/persistence"}},
            "follower": {"databaseId": "abcdefghijklmnopqrstuvwxyz", "pollIntervalMs": 250}
        }"#;
        let parsed: FollowerServiceConfig = serde_json::from_str(json_cfg).unwrap();
        assert_eq!(parsed.follower.poll_interval_ms, 250);
        assert!(parsed.validate().is_ok());

        // A listener requires the service auth secret; both listener kinds at once are rejected.
        let json_serving = r#"{
            "dbRoot": "/tmp/replica",
            "persistence": {"backend": "fs", "fs": {"root": "/tmp/persistence"}},
            "follower": {"databaseId": "abcdefghijklmnopqrstuvwxyz"},
            "listenTcp": "127.0.0.1:8091"
        }"#;
        let mut serving: FollowerServiceConfig = serde_json::from_str(json_serving).unwrap();
        assert!(serving.validate().is_err());
        serving.auth_secret_file = Some("/tmp/auth".into());
        assert!(serving.validate().is_ok());
        serving.unix_dir = Some("/tmp/replica.sock".into());
        assert!(serving.validate().is_err());

        // `none` has no source to tail.
        let none_cfg = FollowerServiceConfig {
            db_root: "/tmp/replica".into(),
            persistence: PersistenceConfig {
                backend: "none".into(),
                allow_dev_backend: true,
                fs: None,
                s3: None,
                lease_ms: 15_000,
                clock_skew_margin_ms: 500,
                holder: None,
                checkpoint_every_commits: 64,
                retention_ms: 86_400_000,
                snapshot_max_bytes: 1024,
                self_check_budget_ms: 60_000,
                self_check_initial_backoff_ms: 500,
                self_check_max_backoff_ms: 8_000,
                allow_same_device_durability: false,
                time_travel_retention_ms: 0,
            },
            follower: cfg().clone(),
            unix_dir: None,
            listen_tcp: None,
            auth_secret_file: None,
            bookmark_secret_file: None,
        };
        assert!(none_cfg.validate().is_err());
    }

    #[test]
    fn sqlite_uri_escapes_reserved_characters() {
        assert_eq!(
            sqlite_uri_path(Path::new("/tmp/a b/c%d?e#f")),
            "/tmp/a b/c%25d%3fe%23f"
        );
    }
}
