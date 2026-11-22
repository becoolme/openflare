//! K2Backend — Cloudflare K2 (durable ordered event log) storage core.
//!
//! One independent vrt-sql database per stream, reusing all SqlEngine invariants
//! (lease/fencing/tombstone/pre-ack persistence, committed as a single
//! transaction via `exec_internal`, **never bypassing persistence**) — the same
//! architecture as `queue.rs` (one db per queue).
//!
//! Semantics follow the CF K2 public contract
//! (developers.cloudflare.com/k2 — public beta):
//! - Records `{content ≤1MiB, headers map<string,string>}`; batches are atomic;
//!   timestamps are server-assigned.
//! - A subscription is an independent read pointer (`earliest` | `latest`),
//!   immutable after creation; creating with the same name AND start_at is
//!   idempotent, same name with different start_at conflicts (422).
//! - `consume` leases a batch to a `worker_id` for LEASE_MS (5 min); one live
//!   lease per worker; re-consume by the holder replays the same batch and
//!   refreshes the lease; expiry/nack returns the records to the pending set
//!   (at-least-once). ≤128 live leases per subscription.
//! - `ack` is idempotent (unknown/already-acked batch → success); `extend`
//!   requires the lease holder and never shortens; `nack` releases immediately.
//! - TTL deletes expired records in the background (`reap`); records may
//!   remain readable briefly past expiry — matches CF's documented behavior.
//!
//! Delivery coverage is explicit, not pointer arithmetic: `_vrt_k2_dlv` holds
//! one row per delivered seq (`acked` flag). A consume returns the earliest
//! `max_records` records at/after the subscription's `start_seq` with no
//! coverage row — nack/expiry simply deletes coverage, so the same seqs are
//! re-picked in order. Acked coverage rows persist while their record does
//! (they are the "already consumed" tombstone) and are reaped with it.

use crate::engine::{EngineError, EngineRegistry, RusqliteEngine, TxnCtx};
use rusqlite::params;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

/// CF limits (decimal bytes / counts).
pub const RECORD_MAX_BYTES: usize = 1_000_000;
pub const BATCH_MAX_BYTES: usize = 5_000_000;
pub const HEADERS_MAX: usize = 64;
pub const HEADER_KV_MAX_BYTES: usize = 1024;
pub const SUBSCRIPTION_NAME_MAX: usize = 128;
pub const SUBSCRIPTIONS_MAX: usize = 100;
pub const LEASES_MAX: usize = 128;
pub const WORKER_ID_MAX: usize = 256;
pub const MAX_RECORDS_MAX: u32 = 10_000;
/// CF lease period: five minutes.
pub const LEASE_MS: i64 = 300_000;
/// Retention bounds: 1h..30d, default 7d.
pub const RETENTION_MIN_MS: i64 = 3_600_000;
pub const RETENTION_MAX_MS: i64 = 2_592_000_000;
pub const RETENTION_DEFAULT_MS: i64 = 604_800_000;
/// Internal transaction budget (same as queue ops).
pub const OP_BUDGET_MS: u64 = 5_000;

/// streamId shape = CF `^[0-9a-f]{32}$`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(String);

impl StreamId {
    pub fn parse(id: &str) -> Result<Self, K2Error> {
        crate::paths::validate_stream_id(id)
            .map_err(|_| K2Error::InvalidInput(format!("invalid stream id: {id:?}")))?;
        Ok(Self(id.to_string()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug)]
pub struct K2Record {
    pub content: Vec<u8>,
    /// Opaque JSON object `map<string,string>` — shape-checked at the edge.
    pub headers: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct K2Subscription {
    pub id: String,
    pub name: String,
    pub start_at: String,
    pub created_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ConsumedRecord {
    pub seq: i64,
    pub timestamp_ms: i64,
    pub content: Vec<u8>,
    pub headers: String,
}

#[derive(Clone, Debug)]
pub struct ConsumeBatch {
    /// `None` when nothing was pending (CF `{batch_id: null, records: []}`).
    pub batch_id: Option<String>,
    pub leased_until_ms: Option<i64>,
    pub records: Vec<ConsumedRecord>,
}

#[derive(Debug, Error)]
pub enum K2Error {
    /// 10204-class invalid request (400).
    #[error("invalid k2 input: {0}")]
    InvalidInput(String),
    /// Stream/subscription missing or input disabled (404).
    #[error("k2 not found")]
    NotFound,
    /// Subscription name taken with different settings, or cap hit (422).
    #[error("k2 conflict: {0}")]
    Conflict(String),
    /// All parallel read slots for the subscription are in use (429, code 10216).
    #[error("k2 read slots exhausted")]
    SlotsFull,
    /// Lease expired/released/reassigned (409, code 10218).
    #[error("k2 lease lost")]
    LeaseLost,
    /// Batch > 5MB or record > 1MB (413).
    #[error("k2 payload too large")]
    TooLarge,
    #[error("k2 backend unavailable: {reason}")]
    Unavailable { retryable: bool, reason: String },
    /// Writer fenced (lease lost at the persistence layer).
    #[error("k2 writer fenced at epoch {0}")]
    Fenced(u64),
    /// Stream dropped.
    #[error("k2 stream dropped")]
    Dropped,
}

/// wire error codes (typed, for provider mapping) — same contract as QueueError.
impl K2Error {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "VRT_K2_INVALID",
            Self::NotFound => "VRT_K2_NOT_FOUND",
            Self::Conflict(_) => "VRT_K2_CONFLICT",
            Self::SlotsFull => "VRT_K2_SLOTS_FULL",
            Self::LeaseLost => "VRT_K2_LEASE_LOST",
            Self::TooLarge => "VRT_K2_TOO_LARGE",
            Self::Unavailable { .. } => "VRT_UNAVAILABLE",
            Self::Fenced(_) => "VRT_FENCED",
            Self::Dropped => "VRT_K2_DROPPED",
        }
    }
    pub fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode as S;
        match self {
            Self::InvalidInput(_) => S::BAD_REQUEST,
            Self::NotFound => S::NOT_FOUND,
            Self::Conflict(_) => S::UNPROCESSABLE_ENTITY,
            Self::SlotsFull => S::TOO_MANY_REQUESTS,
            Self::LeaseLost => S::CONFLICT,
            Self::TooLarge => S::PAYLOAD_TOO_LARGE,
            Self::Unavailable { .. } | Self::Fenced(_) => S::SERVICE_UNAVAILABLE,
            Self::Dropped => S::GONE,
        }
    }
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable {
                retryable: true,
                ..
            } | Self::Fenced(_)
        )
    }
}

/// exec_internal discriminant sentinels (Message payload → typed K2Error).
const KIND_MISMATCH: &str = "VRT_K2_KIND_MISMATCH";
const LEASE_LOST: &str = "VRT_K2_LEASE_LOST";
const SLOTS_FULL: &str = "VRT_K2_SLOTS_FULL";
const CONFLICT: &str = "VRT_K2_CONFLICT|";
const NOT_FOUND: &str = "VRT_K2_NOT_FOUND";
const TOO_LARGE: &str = "VRT_K2_TOO_LARGE";

fn map_engine_error(e: EngineError) -> K2Error {
    match e {
        EngineError::Dropped => K2Error::Dropped,
        EngineError::Fenced(epoch) => K2Error::Fenced(epoch),
        EngineError::Timeout => K2Error::Unavailable {
            retryable: true,
            reason: "op timed out".into(),
        },
        EngineError::Unavailable(reason) => K2Error::Unavailable {
            retryable: true,
            reason: reason.to_string(),
        },
        EngineError::Message(m) if m == KIND_MISMATCH => {
            K2Error::InvalidInput("database is not a k2 stream (kind mismatch)".into())
        }
        EngineError::Message(m) if m == LEASE_LOST => K2Error::LeaseLost,
        EngineError::Message(m) if m == SLOTS_FULL => K2Error::SlotsFull,
        EngineError::Message(m) if m == NOT_FOUND => K2Error::NotFound,
        EngineError::Message(m) if m == TOO_LARGE => K2Error::TooLarge,
        EngineError::Message(m) if m.starts_with(CONFLICT) => {
            K2Error::Conflict(m[CONFLICT.len()..].to_string())
        }
        EngineError::Message(m) => K2Error::Unavailable {
            retryable: false,
            reason: m,
        },
        other => K2Error::Unavailable {
            retryable: true,
            reason: format!("{other}"),
        },
    }
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS _vrt_k2_stream(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _vrt_k2_records(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  content BLOB NOT NULL,
  headers TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS _vrt_k2_subs(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL COLLATE NOCASE,
  start_type TEXT NOT NULL,
  start_seq INTEGER NOT NULL,
  created_ms INTEGER NOT NULL
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS _vrt_k2_subs_name ON _vrt_k2_subs(name);
CREATE TABLE IF NOT EXISTS _vrt_k2_leases(
  batch_id TEXT PRIMARY KEY,
  sub_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  leased_until_ms INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _vrt_k2_leases_sub ON _vrt_k2_leases(sub_id);
CREATE TABLE IF NOT EXISTS _vrt_k2_dlv(
  seq INTEGER PRIMARY KEY,
  batch_id TEXT NOT NULL,
  acked INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _vrt_k2_dlv_batch ON _vrt_k2_dlv(batch_id);
";

/// Database type gate + schema initialization (same transaction) — same
/// contract as `queue.rs`: `kind='k2'` passes; an unkinded database with no
/// user tables initializes; anything else fails closed.
fn ensure_k2_db(tx: &rusqlite::Transaction<'_>) -> Result<(), EngineError> {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS _vrt_k2_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;",
    )
    .map_err(EngineError::Sqlite)?;
    let kind: Option<String> = tx
        .query_row("SELECT v FROM _vrt_k2_meta WHERE k='kind'", [], |r| {
            r.get(0)
        })
        .ok();
    match kind.as_deref() {
        Some("k2") => Ok(()),
        Some(_) => Err(EngineError::Message(KIND_MISMATCH.into())),
        None => {
            let mut st = tx
                .prepare("SELECT name FROM sqlite_master WHERE type='table'")
                .map_err(EngineError::Sqlite)?;
            let names = st
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(EngineError::Sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(EngineError::Sqlite)?;
            if names
                .iter()
                .any(|n| !n.to_ascii_lowercase().starts_with("_vrt_"))
            {
                return Err(EngineError::Message(KIND_MISMATCH.into()));
            }
            tx.execute_batch(SCHEMA).map_err(EngineError::Sqlite)?;
            tx.execute("INSERT INTO _vrt_k2_meta(k,v) VALUES('kind','k2')", [])
                .map_err(EngineError::Sqlite)?;
            Ok(())
        }
    }
}

/// batch/subscription id generation: nanos + pid + monotonic counter (32-hex,
/// same shape as CF ids; no rand dependency).
static ID_SEQ: AtomicU64 = AtomicU64::new(0);

fn gen_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let seq = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:016x}{:04x}{seq:012x}", std::process::id() & 0xffff)
}

/// Run one `exec_internal` transaction on a blocking thread (same contract as
/// `queue.rs::run_internal`).
async fn run_internal<T, F>(engine: Arc<RusqliteEngine>, ctx: TxnCtx, f: F) -> Result<T, K2Error>
where
    T: Send + 'static,
    F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, EngineError> + Send + 'static,
{
    let probe = crate::engine::CommitProbe::new();
    let cutoff = tokio::time::Instant::from_std(ctx.deadline) + std::time::Duration::from_secs(2);
    let task = {
        let probe = probe.clone();
        tokio::task::spawn_blocking(move || {
            crate::engine::with_commit_probe(&probe, || engine.exec_internal(&ctx, f))
        })
    };
    match tokio::time::timeout_at(cutoff, task).await {
        Ok(Ok(r)) => r.map_err(map_engine_error),
        Ok(Err(_)) => Err(K2Error::Unavailable {
            retryable: true,
            reason: "k2 op task failed".into(),
        }),
        Err(_) => Err(map_engine_error(if probe.committed() {
            EngineError::ResultUnknown
        } else {
            EngineError::DeadlineExceeded
        })),
    }
}

fn sub_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<K2Subscription> {
    Ok(K2Subscription {
        id: r.get(0)?,
        name: r.get(1)?,
        start_at: r.get(2)?,
        created_ms: r.get(3)?,
    })
}

fn validate_worker_id(id: &str) -> Result<(), K2Error> {
    if id.is_empty() || id.len() > WORKER_ID_MAX {
        return Err(K2Error::InvalidInput(
            "worker_id must be 1-256 chars".into(),
        ));
    }
    Ok(())
}

fn validate_sub_name(name: &str) -> Result<(), K2Error> {
    let ok = !name.is_empty()
        && name.len() <= SUBSCRIPTION_NAME_MAX
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if !ok {
        return Err(K2Error::InvalidInput(
            "subscription name must be 1-128 letters, numbers, underscores, or hyphens".into(),
        ));
    }
    Ok(())
}

/// SQLite implementation — one independent db per stream, lazily claimed via
/// the EngineRegistry (same lease/fencing/recovery path as D1/queue).
pub struct SqliteK2Backend {
    registry: Arc<EngineRegistry>,
    op_budget_ms: u64,
    lease_ms: i64,
    clock: Arc<dyn crate::clock::Clock>,
}

impl SqliteK2Backend {
    pub fn new(registry: Arc<EngineRegistry>) -> Self {
        Self {
            registry,
            op_budget_ms: OP_BUDGET_MS,
            lease_ms: LEASE_MS,
            clock: Arc::new(crate::clock::SystemClock),
        }
    }
    pub fn with_op_budget_ms(mut self, ms: u64) -> Self {
        self.op_budget_ms = ms;
        self
    }
    /// Lease period override (CF default is LEASE_MS; shorter values let
    /// callers/tests exercise the real expiry path instead of waiting 5 min).
    pub fn with_lease_ms(mut self, ms: i64) -> Self {
        self.lease_ms = ms;
        self
    }
    /// Injectable clock (T36): timestamps, lease expiry and TTL decisions all
    /// go through it so tests advance time deterministically.
    pub fn with_clock(mut self, clock: Arc<dyn crate::clock::Clock>) -> Self {
        self.clock = clock;
        self
    }

    async fn engine(&self, stream: &StreamId) -> Result<Arc<RusqliteEngine>, K2Error> {
        let registry = self.registry.clone();
        let id = stream.as_str().to_string();
        tokio::task::spawn_blocking(move || registry.get(crate::paths::ResourceKind::K2, &id))
            .await
            .map_err(|_| K2Error::Unavailable {
                retryable: true,
                reason: "engine open task failed".into(),
            })?
            .map_err(map_engine_error)
    }

    fn ctx(&self, engine: &RusqliteEngine) -> TxnCtx {
        TxnCtx {
            epoch: engine.lease_epoch(),
            deadline: Instant::now() + std::time::Duration::from_millis(self.op_budget_ms),
            dedup: None,
        }
    }

    /// Initialize the stream's config row (create-time). `name` is checked
    /// for CF's `1-128 [A-Za-z0-9_]` shape at the resource layer; the db only
    /// stores it. Idempotent — an already-configured stream keeps its values.
    pub async fn configure(
        &self,
        stream: &StreamId,
        name: &str,
        retention_ms: i64,
    ) -> Result<(), K2Error> {
        if !(RETENTION_MIN_MS..=RETENTION_MAX_MS).contains(&retention_ms) {
            return Err(K2Error::InvalidInput(format!(
                "retention_ms must be {RETENTION_MIN_MS}..={RETENTION_MAX_MS}"
            )));
        }
        let name = name.to_string();
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            tx.execute(
                "INSERT OR IGNORE INTO _vrt_k2_stream(k,v) VALUES('name',?1),('retention_ms',?2)",
                params![name, retention_ms.to_string()],
            )
            .map_err(EngineError::Sqlite)?;
            Ok(())
        })
        .await
    }

    /// Stream config (`retention_ms`, `name`) — `None` when unconfigured.
    pub async fn stream_config(&self, stream: &StreamId) -> Result<Option<(String, i64)>, K2Error> {
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let get = |k: &str| -> Option<String> {
                tx.query_row("SELECT v FROM _vrt_k2_stream WHERE k=?1", params![k], |r| {
                    r.get(0)
                })
                .ok()
            };
            match (get("name"), get("retention_ms")) {
                (Some(n), Some(r)) => Ok(Some((n, r.parse().unwrap_or(RETENTION_DEFAULT_MS)))),
                _ => Ok(None),
            }
        })
        .await
    }

    /// Atomic batch append (CF `/produce`): all records or none. Timestamps
    /// are server-assigned (single `now` for the whole batch — CF writes one
    /// receive time per batch).
    pub async fn produce(&self, stream: &StreamId, records: &[K2Record]) -> Result<u32, K2Error> {
        if records.is_empty() {
            return Err(K2Error::InvalidInput("empty record batch".into()));
        }
        let mut total = 0usize;
        for r in records {
            if r.content.len().saturating_add(r.headers.len()) > RECORD_MAX_BYTES {
                return Err(K2Error::TooLarge);
            }
            total = total
                .saturating_add(r.content.len())
                .saturating_add(r.headers.len());
        }
        if total > BATCH_MAX_BYTES {
            return Err(K2Error::TooLarge);
        }
        let rows: Vec<(Vec<u8>, String)> = records
            .iter()
            .map(|r| (r.content.clone(), r.headers.clone()))
            .collect();
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let ts = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let n = rows.len() as u32;
            {
                let mut st = tx
                    .prepare("INSERT INTO _vrt_k2_records(ts_ms,content,headers) VALUES(?1,?2,?3)")
                    .map_err(EngineError::Sqlite)?;
                for (content, headers) in &rows {
                    st.execute(params![ts, content, headers])
                        .map_err(EngineError::Sqlite)?;
                }
            }
            Ok(n)
        })
        .await
    }

    /// Create a subscription. Idempotent per CF: same name + same start_at
    /// returns the existing id; same name + different start_at → Conflict.
    /// `start_seq`: `earliest` → 0 (all retained), `latest` → max(seq)+1.
    pub async fn create_subscription(
        &self,
        stream: &StreamId,
        name: &str,
        start_at: &str,
    ) -> Result<K2Subscription, K2Error> {
        validate_sub_name(name)?;
        if start_at != "earliest" && start_at != "latest" {
            return Err(K2Error::InvalidInput(
                "start_at.type must be 'earliest' or 'latest'".into(),
            ));
        }
        let name = name.to_string();
        let start_at = start_at.to_string();
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let created_now = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            // Idempotency check by (case-insensitive) name.
            let existing: Option<(String, String, i64)> = tx
                .query_row(
                    "SELECT id, start_type, created_ms FROM _vrt_k2_subs WHERE name=?1",
                    params![name],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .ok();
            if let Some((id, ty, created)) = existing {
                if ty == start_at {
                    return Ok(K2Subscription {
                        id,
                        name: name.clone(),
                        start_at: ty,
                        created_ms: created,
                    });
                }
                return Err(EngineError::Message(format!(
                    "{CONFLICT}subscription name {name:?} exists with different start_at"
                )));
            }
            let count: i64 = tx
                .query_row("SELECT COUNT(*) FROM _vrt_k2_subs", [], |r| r.get(0))
                .map_err(EngineError::Sqlite)?;
            if count >= SUBSCRIPTIONS_MAX as i64 {
                return Err(EngineError::Message(format!(
                    "{CONFLICT}subscription limit {SUBSCRIPTIONS_MAX} reached"
                )));
            }
            let start_seq: i64 = if start_at == "earliest" {
                0
            } else {
                tx.query_row(
                    "SELECT COALESCE(MAX(seq),0)+1 FROM _vrt_k2_records",
                    [],
                    |r| r.get(0),
                )
                .map_err(EngineError::Sqlite)?
            };
            let id = gen_id();
            let created = created_now;
            tx.execute(
                "INSERT INTO _vrt_k2_subs(id,name,start_type,start_seq,created_ms) VALUES(?1,?2,?3,?4,?5)",
                params![id, name, start_at, start_seq, created],
            )
            .map_err(EngineError::Sqlite)?;
            Ok(K2Subscription {
                id,
                name,
                start_at,
                created_ms: created,
            })
        })
        .await
    }

    /// List subscriptions (oldest first).
    pub async fn list_subscriptions(
        &self,
        stream: &StreamId,
    ) -> Result<Vec<K2Subscription>, K2Error> {
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let mut st = tx
                .prepare("SELECT id,name,start_type,created_ms FROM _vrt_k2_subs ORDER BY created_ms, id")
                .map_err(EngineError::Sqlite)?;
            let out = st
                .query_map([], sub_from_row)
                .map_err(EngineError::Sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(EngineError::Sqlite)?;
            Ok(out)
        })
        .await
    }

    /// Delete a subscription and every lease/coverage row it owns.
    pub async fn delete_subscription(
        &self,
        stream: &StreamId,
        sub_id: &str,
    ) -> Result<bool, K2Error> {
        let sub = sub_id.to_string();
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let existed = tx
                .execute("DELETE FROM _vrt_k2_subs WHERE id=?1", params![sub])
                .map_err(EngineError::Sqlite)?
                > 0;
            if existed {
                tx.execute(
                    "DELETE FROM _vrt_k2_dlv WHERE batch_id IN (SELECT batch_id FROM _vrt_k2_leases WHERE sub_id=?1)",
                    params![sub],
                )
                .map_err(EngineError::Sqlite)?;
                tx.execute("DELETE FROM _vrt_k2_leases WHERE sub_id=?1", params![sub])
                    .map_err(EngineError::Sqlite)?;
            }
            Ok(existed)
        })
        .await
    }

    /// CF `/consume`: lease a batch to `worker_id`. Holder re-consume replays
    /// the same batch and refreshes the lease (CF recovery semantics).
    pub async fn consume(
        &self,
        stream: &StreamId,
        sub_id: &str,
        worker_id: &str,
        max_records: u32,
    ) -> Result<ConsumeBatch, K2Error> {
        validate_worker_id(worker_id)?;
        if max_records == 0 || max_records > MAX_RECORDS_MAX {
            return Err(K2Error::InvalidInput(format!(
                "max_records must be 1..={MAX_RECORDS_MAX}"
            )));
        }
        let sub = sub_id.to_string();
        let worker = worker_id.to_string();
        let lease_ms = self.lease_ms;
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let now = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let start_seq: i64 = tx
                .query_row(
                    "SELECT start_seq FROM _vrt_k2_subs WHERE id=?1",
                    params![sub],
                    |r| r.get(0),
                )
                .optional()
                .map_err(EngineError::Sqlite)?
                .ok_or_else(|| EngineError::Message(NOT_FOUND.into()))?;

            // Expire stale leases: coverage returns to the pending set.
            tx.execute(
                "DELETE FROM _vrt_k2_dlv WHERE acked=0 AND batch_id IN (SELECT batch_id FROM _vrt_k2_leases WHERE sub_id=?1 AND leased_until_ms<?2)",
                params![sub, now],
            )
            .map_err(EngineError::Sqlite)?;
            tx.execute(
                "DELETE FROM _vrt_k2_leases WHERE sub_id=?1 AND leased_until_ms<?2",
                params![sub, now],
            )
            .map_err(EngineError::Sqlite)?;

            // Holder replay: same batch, refreshed lease (CF).
            let held: Option<(String, i64)> = tx
                .query_row(
                    "SELECT batch_id, leased_until_ms FROM _vrt_k2_leases WHERE sub_id=?1 AND worker_id=?2",
                    params![sub, worker],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(EngineError::Sqlite)?;
            if let Some((batch_id, _)) = held {
                let until = now + lease_ms;
                tx.execute(
                    "UPDATE _vrt_k2_leases SET leased_until_ms=?1 WHERE batch_id=?2",
                    params![until, batch_id],
                )
                .map_err(EngineError::Sqlite)?;
                let records = read_batch(tx, &batch_id)?;
                return Ok(ConsumeBatch {
                    batch_id: Some(batch_id),
                    leased_until_ms: Some(until),
                    records,
                });
            }

            let live: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM _vrt_k2_leases WHERE sub_id=?1",
                    params![sub],
                    |r| r.get(0),
                )
                .map_err(EngineError::Sqlite)?;
            if live >= LEASES_MAX as i64 {
                return Err(EngineError::Message(SLOTS_FULL.into()));
            }

            // Read-time TTL: records past retention are never delivered even
            // before the reaper physically deletes them.
            let retention: i64 = tx
                .query_row(
                    "SELECT v FROM _vrt_k2_stream WHERE k='retention_ms'",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(RETENTION_DEFAULT_MS);
            let cutoff = now - retention;

            // Earliest uncovered records at/after the subscription start.
            let mut st = tx
                .prepare(
                    "SELECT r.seq, r.ts_ms, r.content, r.headers FROM _vrt_k2_records r \
                     WHERE r.seq >= ?1 AND r.ts_ms >= ?3 \
                     AND NOT EXISTS(SELECT 1 FROM _vrt_k2_dlv d WHERE d.seq = r.seq) \
                     ORDER BY r.seq LIMIT ?2",
                )
                .map_err(EngineError::Sqlite)?;
            let pending: Vec<ConsumedRecord> = st
                .query_map(params![start_seq, max_records as i64, cutoff], |r| {
                    Ok(ConsumedRecord {
                        seq: r.get(0)?,
                        timestamp_ms: r.get(1)?,
                        content: r.get(2)?,
                        headers: r.get(3)?,
                    })
                })
                .map_err(EngineError::Sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(EngineError::Sqlite)?;
            if pending.is_empty() {
                return Ok(ConsumeBatch {
                    batch_id: None,
                    leased_until_ms: None,
                    records: Vec::new(),
                });
            }
            let batch_id = gen_id();
            let until = now + lease_ms;
            tx.execute(
                "INSERT INTO _vrt_k2_leases(batch_id,sub_id,worker_id,leased_until_ms) VALUES(?1,?2,?3,?4)",
                params![batch_id, sub, worker, until],
            )
            .map_err(EngineError::Sqlite)?;
            {
                let mut ins = tx
                    .prepare("INSERT INTO _vrt_k2_dlv(seq,batch_id) VALUES(?1,?2)")
                    .map_err(EngineError::Sqlite)?;
                for r in &pending {
                    ins.execute(params![r.seq, batch_id])
                        .map_err(EngineError::Sqlite)?;
                }
            }
            Ok(ConsumeBatch {
                batch_id: Some(batch_id),
                leased_until_ms: Some(until),
                records: pending,
            })
        })
        .await
    }

    /// CF `/ack`: mark coverage acked + release the lease. Idempotent —
    /// unknown or already-acked batches succeed silently.
    pub async fn ack(
        &self,
        stream: &StreamId,
        sub_id: &str,
        batch_id: &str,
    ) -> Result<(), K2Error> {
        let sub = sub_id.to_string();
        let batch = batch_id.to_string();
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let now = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            // An expired-but-not-yet-reaped lease is already lost: treat it as
            // an unknown batch (idempotent success) rather than tombstoning
            // records that are owed a redelivery.
            let has = tx
                .query_row(
                    "SELECT 1 FROM _vrt_k2_leases WHERE sub_id=?1 AND batch_id=?2 AND leased_until_ms>=?3",
                    params![sub, batch, now],
                    |_| Ok(()),
                )
                .optional()
                .map_err(EngineError::Sqlite)?
                .is_some();
            if has {
                tx.execute(
                    "UPDATE _vrt_k2_dlv SET acked=1 WHERE batch_id=?1",
                    params![batch],
                )
                .map_err(EngineError::Sqlite)?;
                tx.execute(
                    "DELETE FROM _vrt_k2_leases WHERE batch_id=?1",
                    params![batch],
                )
                .map_err(EngineError::Sqlite)?;
            }
            Ok(())
        })
        .await
    }

    /// CF `/nack`: release the batch — coverage deletes, so the same seqs
    /// return to the pending set for the next consume (any worker).
    pub async fn nack(
        &self,
        stream: &StreamId,
        sub_id: &str,
        batch_id: &str,
    ) -> Result<(), K2Error> {
        let sub = sub_id.to_string();
        let batch = batch_id.to_string();
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let now = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let has = tx
                .query_row(
                    "SELECT 1 FROM _vrt_k2_leases WHERE sub_id=?1 AND batch_id=?2 AND leased_until_ms>=?3",
                    params![sub, batch, now],
                    |_| Ok(()),
                )
                .optional()
                .map_err(EngineError::Sqlite)?
                .is_some();
            if has {
                tx.execute("DELETE FROM _vrt_k2_dlv WHERE batch_id=?1", params![batch])
                    .map_err(EngineError::Sqlite)?;
                tx.execute(
                    "DELETE FROM _vrt_k2_leases WHERE batch_id=?1",
                    params![batch],
                )
                .map_err(EngineError::Sqlite)?;
            }
            Ok(())
        })
        .await
    }

    /// CF `/extend`: holder-only, never shortens (new expiry is now+LEASE_MS
    /// only when later than the current one).
    pub async fn extend(
        &self,
        stream: &StreamId,
        sub_id: &str,
        batch_id: &str,
        worker_id: &str,
    ) -> Result<i64, K2Error> {
        validate_worker_id(worker_id)?;
        let sub = sub_id.to_string();
        let batch = batch_id.to_string();
        let worker = worker_id.to_string();
        let lease_ms = self.lease_ms;
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let now = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let cur: Option<(String, i64)> = tx
                .query_row(
                    "SELECT worker_id, leased_until_ms FROM _vrt_k2_leases WHERE sub_id=?1 AND batch_id=?2 AND leased_until_ms>=?3",
                    params![sub, batch, now],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(EngineError::Sqlite)?;
            match cur {
                Some((holder, until)) if holder == worker => {
                    let next = until.max(now + lease_ms);
                    tx.execute(
                        "UPDATE _vrt_k2_leases SET leased_until_ms=?1 WHERE batch_id=?2",
                        params![next, batch],
                    )
                    .map_err(EngineError::Sqlite)?;
                    Ok(next)
                }
                _ => Err(EngineError::Message(LEASE_LOST.into())),
            }
        })
        .await
    }

    /// TTL reaper: delete expired records and every coverage row whose record
    /// no longer exists. Bounded single transaction.
    pub async fn reap(&self, stream: &StreamId) -> Result<(u64, u64), K2Error> {
        let engine = self.engine(stream).await?;
        let ctx = self.ctx(&engine);
        let now = self.clock.now_ms() as i64;
        run_internal(engine, ctx, move |tx| {
            ensure_k2_db(tx)?;
            let retention: i64 = tx
                .query_row(
                    "SELECT v FROM _vrt_k2_stream WHERE k='retention_ms'",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(RETENTION_DEFAULT_MS);
            let cutoff = now - retention;
            let records = tx
                .execute(
                    "DELETE FROM _vrt_k2_records WHERE ts_ms < ?1",
                    params![cutoff],
                )
                .map_err(EngineError::Sqlite)? as u64;
            let coverage = tx
                .execute(
                    "DELETE FROM _vrt_k2_dlv WHERE seq NOT IN (SELECT seq FROM _vrt_k2_records)",
                    [],
                )
                .map_err(EngineError::Sqlite)? as u64;
            Ok((records, coverage))
        })
        .await
    }

    /// Delete the stream (same T35c/T36 drop path as `drop_queue`: manifest
    /// tombstone -> removal -> rename; idempotent).
    pub fn drop_stream(&self, stream: &StreamId) -> Result<bool, K2Error> {
        self.registry
            .drop_db(crate::paths::ResourceKind::K2, stream.as_str())
            .map_err(map_engine_error)
    }
}

/// Read the records pinned by a live batch (join coverage → record).
fn read_batch(
    tx: &rusqlite::Transaction<'_>,
    batch_id: &str,
) -> Result<Vec<ConsumedRecord>, EngineError> {
    let mut st = tx
        .prepare(
            "SELECT r.seq, r.ts_ms, r.content, r.headers FROM _vrt_k2_dlv d \
             JOIN _vrt_k2_records r ON r.seq = d.seq WHERE d.batch_id=?1 ORDER BY r.seq",
        )
        .map_err(EngineError::Sqlite)?;
    let out = st
        .query_map(params![batch_id], |r| {
            Ok(ConsumedRecord {
                seq: r.get(0)?,
                timestamp_ms: r.get(1)?,
                content: r.get(2)?,
                headers: r.get(3)?,
            })
        })
        .map_err(EngineError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(EngineError::Sqlite);
    out
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineRegistry, RusqliteEngine};
    use tempfile::TempDir;

    fn backend() -> (TempDir, SqliteK2Backend) {
        let tmp = TempDir::new().unwrap();
        let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
        (tmp, SqliteK2Backend::new(reg))
    }

    fn sid() -> StreamId {
        StreamId::parse("0123456789abcdef0123456789abcdef").unwrap()
    }

    fn rec(b: &[u8]) -> K2Record {
        K2Record {
            content: b.to_vec(),
            headers: "{}".into(),
        }
    }

    #[tokio::test]
    async fn produce_atomic_and_limits() {
        let (_t, b) = backend();
        let s = sid();
        b.configure(&s, "orders", RETENTION_DEFAULT_MS)
            .await
            .unwrap();
        assert_eq!(b.produce(&s, &[rec(b"a"), rec(b"b")]).await.unwrap(), 2);
        assert!(b.produce(&s, &[]).await.is_err());
        assert!(matches!(
            b.produce(&s, &[rec(&vec![0u8; RECORD_MAX_BYTES + 1])])
                .await,
            Err(K2Error::TooLarge)
        ));
    }

    #[tokio::test]
    async fn subscription_idempotent_and_conflicting() {
        let (_t, b) = backend();
        let s = sid();
        b.configure(&s, "orders", RETENTION_DEFAULT_MS)
            .await
            .unwrap();
        let a = b.create_subscription(&s, "sub", "earliest").await.unwrap();
        let again = b.create_subscription(&s, "sub", "earliest").await.unwrap();
        assert_eq!(a.id, again.id);
        // Case-insensitive name uniqueness.
        assert!(matches!(
            b.create_subscription(&s, "Sub", "latest").await,
            Err(K2Error::Conflict(_))
        ));
        assert!(matches!(
            b.create_subscription(&s, "sub", "latest").await,
            Err(K2Error::Conflict(_))
        ));
        assert!(b.create_subscription(&s, "sub", "bogus").await.is_err());
    }

    #[tokio::test]
    async fn consume_lease_ack_nack_cycle() {
        let (_t, b) = backend();
        let s = sid();
        b.configure(&s, "orders", RETENTION_DEFAULT_MS)
            .await
            .unwrap();
        b.produce(&s, &[rec(b"r1"), rec(b"r2"), rec(b"r3")])
            .await
            .unwrap();
        let sub = b.create_subscription(&s, "c", "earliest").await.unwrap();

        // w1 consumes 2 → leased batch.
        let b1 = b.consume(&s, &sub.id, "w1", 2).await.unwrap();
        assert_eq!(b1.records.len(), 2);
        assert_eq!(b1.records[0].content, b"r1");
        // Same worker re-consume replays the same batch (CF recovery).
        let replay = b.consume(&s, &sub.id, "w1", 10).await.unwrap();
        assert_eq!(replay.batch_id, b1.batch_id);
        // w2 gets the NEXT batch, not the leased records.
        let b2 = b.consume(&s, &sub.id, "w2", 10).await.unwrap();
        assert_eq!(b2.records.len(), 1);
        assert_eq!(b2.records[0].content, b"r3");
        // ack b1 → never redelivered; nack b2 → r3 redelivers.
        b.ack(&s, &sub.id, b1.batch_id.as_deref().unwrap())
            .await
            .unwrap();
        b.nack(&s, &sub.id, b2.batch_id.as_deref().unwrap())
            .await
            .unwrap();
        let b3 = b.consume(&s, &sub.id, "w3", 10).await.unwrap();
        assert_eq!(b3.records.len(), 1);
        assert_eq!(b3.records[0].content, b"r3");
        b.ack(&s, &sub.id, b3.batch_id.as_deref().unwrap())
            .await
            .unwrap();
        // Everything acked → empty batch.
        let empty = b.consume(&s, &sub.id, "w4", 10).await.unwrap();
        assert!(empty.batch_id.is_none() && empty.records.is_empty());
    }

    #[tokio::test]
    async fn latest_starts_after_existing_records() {
        let (_t, b) = backend();
        let s = sid();
        b.configure(&s, "orders", RETENTION_DEFAULT_MS)
            .await
            .unwrap();
        b.produce(&s, &[rec(b"old")]).await.unwrap();
        let sub = b.create_subscription(&s, "c", "latest").await.unwrap();
        b.produce(&s, &[rec(b"new")]).await.unwrap();
        let batch = b.consume(&s, &sub.id, "w", 10).await.unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].content, b"new");
    }

    #[tokio::test]
    async fn extend_requires_holder_and_never_shortens() {
        let (_t, b) = backend();
        let s = sid();
        b.configure(&s, "orders", RETENTION_DEFAULT_MS)
            .await
            .unwrap();
        b.produce(&s, &[rec(b"r")]).await.unwrap();
        let sub = b.create_subscription(&s, "c", "earliest").await.unwrap();
        let batch = b.consume(&s, &sub.id, "w1", 10).await.unwrap();
        let bid = batch.batch_id.clone().unwrap();
        // Other worker / unknown batch → LeaseLost.
        assert!(matches!(
            b.extend(&s, &sub.id, &bid, "w2").await,
            Err(K2Error::LeaseLost)
        ));
        let until = b.extend(&s, &sub.id, &bid, "w1").await.unwrap();
        assert!(until >= batch.leased_until_ms.unwrap());
    }

    #[tokio::test]
    async fn kind_gate_and_stream_id() {
        assert!(StreamId::parse("xyz").is_err());
        assert!(StreamId::parse(&"a".repeat(32)).is_ok());
        let (_t, b) = backend();
        // Unconfigured stream is still usable (config only gates retention).
        let s = sid();
        assert!(b.stream_config(&s).await.unwrap().is_none());
    }

    #[test]
    fn rusqlite_engine_kind_gate() {
        let tmp = TempDir::new().unwrap();
        // A db with user tables (created outside the engine's internal window,
        // which correctly denies user-shaped DDL) is not claimable as k2.
        {
            let raw = rusqlite::Connection::open(tmp.path().join("x.db")).unwrap();
            raw.execute_batch("CREATE TABLE user_t(x)").unwrap();
        }
        let e = RusqliteEngine::open_file(&tmp.path().join("x.db"), "x").unwrap();
        let err = e
            .exec_internal(
                &TxnCtx {
                    epoch: 0,
                    deadline: Instant::now() + std::time::Duration::from_secs(5),
                    dedup: None,
                },
                |tx| ensure_k2_db(tx),
            )
            .unwrap_err();
        assert!(matches!(map_engine_error(err), K2Error::InvalidInput(_)));
    }
}
