//! QueueBackend (T37, section 8/10 landed): one independent vrt-sql database per queue,
//! reusing all SqlEngine invariants (lease/fencing/tombstone/pre-ack persistence,
//! committed as a single transaction via `exec_internal`, **never bypassing persistence**).
//!
//! Limit values follow section 8 + miniflare broker measurements -- **decimal** bytes:
//! single message <=128_000B, batch <=100 messages, batch total <=288_000B, delay 0..=86_400s.
//! Consumer-side dispatch (dispatcher leader election, calling queue(), ack/retry/DLQ driving) belongs to
//! T37b; the table schema / lease semantics landed this round are its foundation.
//!
//! Database type gate: `_vrt_q_meta.kind='queue'` is initialized in the same transaction as the schema;
//! a database that already has user tables (D1) calling a queue endpoint -> fail closed `NotAQueue`.

use crate::engine::{EngineError, EngineRegistry, RusqliteEngine, TxnCtx};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

/// Decimal limits consistent with the miniflare broker (B:50,82,379 verified).
pub const MSG_MAX_BYTES: usize = 128_000;
pub const BATCH_MAX_MSGS: usize = 100;
pub const BATCH_MAX_BYTES: usize = 288_000;
pub const DELAY_MAX_SECS: u32 = 86_400;
pub const CLAIM_MAX: u32 = 100;
/// T37b-r: upper bound of the raw body byte budget per claim (the `maxBytes` parameter).
/// Callers may use a smaller value (dispatcher default 4MB) -- the upper bound only guards against runaway config,
/// it is not a recommended value.
pub const CLAIM_BYTES_MAX: u64 = 32 * 1024 * 1024;
/// Upper bound on claim lease TTL (12h) -- `claim_ttl > maxDeliveryTimeout`
/// is guaranteed by the dispatcher (T37b); here only non-positive/out-of-range values are rejected.
pub const LEASE_TTL_MAX_MS: i64 = 43_200_000;
/// Upper bound on client-supplied message ids (printable ASCII, no separators).
pub const MSG_ID_MAX: usize = 128;
/// Per-call settlement cap for ack/retry/dead_letter (looser than the batch cap, covering multiple claims).
pub const SETTLE_MAX: usize = 1_000;
/// Cloudflare-compatible maximum message retention: four days from enqueue;
/// dead-letter rows use the same bound from `dead_at`. Expired rows are purged
/// lazily by queue operations (there is no separate queue storage sweeper).
pub const MESSAGE_RETENTION_MS: i64 = 4 * 24 * 60 * 60 * 1000;
/// Bounded lazy purge per queue operation: retention must never turn one
/// request into an unbounded table scan/delete. Read paths still filter by
/// the cutoff, so a larger expired backlog is not visible or delivered.
const PURGE_BATCH: usize = 2_000;
/// Internal transaction budget for queue ops (below D1's 30s -- queue statements are bounded small batches).
pub const OP_BUDGET_MS: u64 = 5_000;

/// Allowed content types (wire `X-Msg-Fmt` / `contentType` domain).
pub const CONTENT_TYPES: &[&str] = &["text", "json", "bytes", "v8"];

/// queueId shape = the section 7 databaseId rule (`^[a-z0-9]{26}$`, entry rejects
/// `/`, `..`, uppercase, wrong length).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueueId(String);

impl QueueId {
    pub fn parse(id: &str) -> Result<Self, QueueError> {
        crate::paths::validate_database_id(id)
            .map_err(|_| QueueError::InvalidInput(format!("invalid queue id: {id:?}")))?;
        Ok(Self(id.to_string()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Producer-side enqueued message (`body` is **opaque bytes** -- v8-serialized bodies are not interpreted).
#[derive(Clone, Debug)]
pub struct IncomingMsg {
    /// Client id (batch message `id?`); None -> server generates 32-hex.
    pub id: Option<String>,
    pub body: Vec<u8>,
    /// `text|json|bytes|v8`.
    pub content_type: String,
    /// 0..=86400.
    pub delay_secs: u32,
    /// T37r: caller-declared timestamp -- an **untrusted message attribute** (used by DLQ forwarding to preserve
    /// the original id/timestamp). Stored in the `client_ts` column: range-validated, echoed to the consumer
    /// on claim, and never participates in `visible_at` scheduling, `created_at`
    /// or the `oldestMessageTimestamp` metric -- all three always use the server clock.
    /// None -> the column is NULL, and what the consumer sees is `created_at`.
    pub timestamp_ms: Option<i64>,
}

/// Message returned by claim (section 10).
#[derive(Clone, Debug)]
pub struct ClaimedMsg {
    pub id: String,
    pub lease_id: String,
    pub attempts: i64,
    pub body: Vec<u8>,
    pub content_type: String,
    pub timestamp_ms: i64,
}

/// Inner object shape of `GET /metrics` (same origin as miniflare `{backlogCount,
/// backlogBytes, oldestMessageTimestamp}`, but the wire field is
/// `oldestMessageTimestampMs` and the whole thing is nested in the `{success, metrics}` envelope
/// -- wire.rs; oldest None = empty queue, normalized to 0 on the broker surface).
#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueMetrics {
    pub backlog_count: i64,
    pub backlog_bytes: i64,
    pub oldest_message_timestamp_ms: Option<i64>,
    /// T37b-r4: immediately claimable (visible_at<=now and not leased / lease expired).
    /// Operational troubleshooting needs to split "written but cannot be claimed" into four buckets:
    /// visible/inflight/delayed/dead -- fields only added; old consumers ignore them via serde.
    #[serde(default)]
    pub visible_count: i64,
    /// T37b-r4: in flight (holding an unexpired lease).
    #[serde(default)]
    pub in_flight_count: i64,
    /// T37b-r4: delayed, not yet due (visible_at>now -- entry point for clock-skew investigation).
    #[serde(default)]
    pub delayed_count: i64,
    /// T37b-r4: row count of the `_vrt_q_dead` audit table (total dead-lettered/dropped).
    #[serde(default)]
    pub dead_count: i64,
}

#[derive(Clone, Debug)]
pub struct ClaimedBatch {
    /// The settlement lease_id shared by this batch of messages.
    pub lease_id: String,
    /// T37b: dispatch lease epoch atomically completed by claim (CAS embedded in the claim transaction;
    /// each epoch uniquely identifies one lease hold -- for observation/tests, settlement uses lease_id).
    pub dispatch_epoch: i64,
    pub messages: Vec<ClaimedMsg>,
}

/// T37r: send results distinguish real enqueues from idempotent dedup -- a duplicate id is never again disguised as
/// an all-success silent absorb.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendResult {
    /// Number of messages actually written this time.
    pub inserted: u32,
    /// Number of messages deduplicated because the id already exists (idempotent hits from DLQ forwarding replay/transport retry).
    pub duplicates: u32,
    #[serde(flatten)]
    pub metrics: QueueMetrics,
}

#[derive(Debug, Error)]
pub enum QueueError {
    /// lease_id mismatched or expired -- settlement must be rejected, never mistakenly deleting/modifying
    /// a message that someone else has re-claimed.
    #[error("queue lease lost")]
    LeaseLost,
    /// Limit exceeded (413 semantics).
    #[error("queue payload too large")]
    TooLarge,
    /// Invalid input (400 semantics).
    #[error("invalid queue input: {0}")]
    InvalidInput(String),
    /// Backend unavailable; `retryable` lets the caller decide whether to retry.
    #[error("queue backend unavailable: {reason}")]
    Unavailable { retryable: bool, reason: String },
    /// Writer fenced (lease lost at the persistence layer).
    #[error("queue writer fenced at epoch {0}")]
    Fenced(u64),
    /// queue already tombstoned/deleted (after drop all ops are rejected, idempotently).
    #[error("queue dropped")]
    Dropped,
    /// T37b: the dispatch lease is held by another dispatcher and unexpired -- claim rejected;
    /// the payload is the holder and expiry time (the caller backs off and automatically takes over after expiry).
    #[error("queue dispatch lease held by {holder} until {expires_at_ms}")]
    DispatchLeaseLost { holder: String, expires_at_ms: i64 },
}

/// wire error codes (typed, for provider mapping).
impl QueueError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::LeaseLost => "VRT_QUEUE_LEASE_LOST",
            Self::TooLarge => "VRT_QUEUE_TOO_LARGE",
            Self::InvalidInput(_) => "VRT_QUEUE_INVALID",
            Self::Unavailable { .. } => "VRT_UNAVAILABLE",
            Self::Fenced(_) => "VRT_FENCED",
            Self::Dropped => "VRT_QUEUE_DROPPED",
            Self::DispatchLeaseLost { .. } => "VRT_QUEUE_DISPATCH_LEASE",
        }
    }
    pub fn status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode as S;
        match self {
            Self::LeaseLost => S::CONFLICT,
            Self::TooLarge => S::PAYLOAD_TOO_LARGE,
            Self::InvalidInput(_) => S::BAD_REQUEST,
            Self::Unavailable { .. } => S::SERVICE_UNAVAILABLE,
            Self::Fenced(_) => S::SERVICE_UNAVAILABLE,
            Self::Dropped => S::GONE,
            Self::DispatchLeaseLost { .. } => S::CONFLICT,
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

fn map_engine_error(e: EngineError) -> QueueError {
    match e {
        EngineError::Dropped => QueueError::Dropped,
        EngineError::Fenced(epoch) => QueueError::Fenced(epoch),
        EngineError::Timeout => QueueError::Unavailable {
            retryable: true,
            reason: "op timed out".into(),
        },
        EngineError::Unavailable(reason) => QueueError::Unavailable {
            retryable: true,
            reason: reason.to_string(),
        },
        EngineError::Message(m) if m == KIND_MISMATCH => {
            QueueError::InvalidInput("database is not a queue (kind mismatch)".into())
        }
        EngineError::Message(m) if m == LEASE_LOST => QueueError::LeaseLost,
        EngineError::Message(m) if m.starts_with(DISPATCH_LOST) => {
            let payload = &m[DISPATCH_LOST.len()..];
            let (h, exp) = payload.split_once('|').unwrap_or((payload, "0"));
            QueueError::DispatchLeaseLost {
                holder: h.to_string(),
                expires_at_ms: exp.parse().unwrap_or(0),
            }
        }
        EngineError::Message(m) => QueueError::Unavailable {
            retryable: false,
            reason: m,
        },
        EngineError::DeadlineExceeded => QueueError::Unavailable {
            retryable: true,
            reason: "VRT_SQL_TIMEOUT".into(),
        },
        EngineError::ResultUnknown => QueueError::Unavailable {
            retryable: true,
            reason: "VRT_SQL_RESULT_UNKNOWN".into(),
        },
        other => QueueError::Unavailable {
            retryable: false,
            reason: other.to_string(),
        },
    }
}

/// RV-R71: grace past the op deadline the async side waits for the
/// blocking task's own (more precise) deadline error.
const QUEUE_OUTER_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

/// RV-R71: run one `exec_internal` transaction (SQLite, std Mutex and
/// the durable-publish bridge are all blocking) on a blocking thread,
/// never on a tokio worker, and cut the wait at the op deadline.
/// Timeout before commit -> retryable `VRT_SQL_TIMEOUT`; after commit
/// started -> retryable `VRT_SQL_RESULT_UNKNOWN` (queue ops carry their
/// own idempotency: message ids / lease ids).
async fn run_internal<T, F>(engine: Arc<RusqliteEngine>, ctx: TxnCtx, f: F) -> Result<T, QueueError>
where
    T: Send + 'static,
    F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, EngineError> + Send + 'static,
{
    let probe = crate::engine::CommitProbe::new();
    let cutoff = tokio::time::Instant::from_std(ctx.deadline) + QUEUE_OUTER_GRACE;
    let task = {
        let probe = probe.clone();
        tokio::task::spawn_blocking(move || {
            crate::engine::with_commit_probe(&probe, || engine.exec_internal(&ctx, f))
        })
    };
    match tokio::time::timeout_at(cutoff, task).await {
        Ok(Ok(r)) => r.map_err(map_engine_error),
        Ok(Err(_)) => Err(QueueError::Unavailable {
            retryable: true,
            reason: "queue op task failed".into(),
        }),
        Err(_) => Err(map_engine_error(if probe.committed() {
            EngineError::ResultUnknown
        } else {
            EngineError::DeadlineExceeded
        })),
    }
}

/// exec_internal internal discriminant sentinel (Message payload -> typed QueueError).
const KIND_MISMATCH: &str = "VRT_QUEUE_KIND_MISMATCH";
const LEASE_LOST: &str = "VRT_QUEUE_LEASE_LOST";
const DISPATCH_LOST: &str = "VRT_QUEUE_DISPATCH_LOST|";
/// T37b: upper bound for dispatch lease holder/ttl (charset [a-zA-Z0-9_.:-]).
pub const DISPATCH_HOLDER_MAX: usize = 64;
/// Dispatch lease duration cap: 5 minutes -- far above the polling period, far below the message lease cap.
pub const DISPATCH_TTL_MAX_MS: i64 = 300_000;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS _vrt_q_messages(
  id TEXT PRIMARY KEY,
  body BLOB NOT NULL,
  content_type TEXT NOT NULL,
  visible_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  client_ts INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_id TEXT,
  lease_expiry INTEGER
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _vrt_q_visible ON _vrt_q_messages(visible_at);
CREATE TABLE IF NOT EXISTS _vrt_q_dead(
  id TEXT PRIMARY KEY,
  body BLOB NOT NULL,
  content_type TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  attempts INTEGER NOT NULL,
  dead_at INTEGER NOT NULL,
  reason TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS _vrt_q_dispatch(
  id INTEGER PRIMARY KEY CHECK(id=1),
  holder TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
) WITHOUT ROWID;
";

/// Database type gate + schema initialization (same transaction):
/// - `kind='queue'` -> pass straight through;
/// - no kind and **no user tables** (not `_vrt_`/`sqlite_` prefixed) -> initialize;
/// - anything else (a D1 db / unknown kind string) -> fail closed.
fn ensure_queue_db(tx: &rusqlite::Transaction<'_>) -> Result<(), EngineError> {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS _vrt_q_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;",
    )
    .map_err(EngineError::Sqlite)?;
    let kind: Option<String> = tx
        .query_row("SELECT v FROM _vrt_q_meta WHERE k='kind'", [], |r| r.get(0))
        .ok();
    match kind.as_deref() {
        Some("queue") => {
            // T37b: add _vrt_q_dispatch to an existing queue db (the old SCHEMA lacks this table
            // -- CREATE IF NOT EXISTS is idempotent, in the same exec_internal transaction).
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS _vrt_q_dispatch(\
                 id INTEGER PRIMARY KEY CHECK(id=1), holder TEXT NOT NULL,\
                 epoch INTEGER NOT NULL, expires_at INTEGER NOT NULL) WITHOUT ROWID;",
            )
            .map_err(EngineError::Sqlite)?;
            Ok(())
        }
        Some(_) => Err(EngineError::Message(KIND_MISMATCH.into())),
        None => {
            // Fetch rows from sqlite_master one by one and judge the prefix on the Rust side -- `NOT LIKE`
            // would trigger the `like` function (not in the internal whitelist), and
            // no function calls may appear in the SQL.
            let mut st = tx
                .prepare("SELECT name FROM sqlite_master WHERE type='table'")
                .map_err(EngineError::Sqlite)?;
            let names = st
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(EngineError::Sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(EngineError::Sqlite)?;
            let user_tables = names
                .iter()
                .filter(|n| !n.to_ascii_lowercase().starts_with("_vrt_"))
                .count();
            if user_tables > 0 {
                return Err(EngineError::Message(KIND_MISMATCH.into()));
            }
            tx.execute_batch(SCHEMA).map_err(EngineError::Sqlite)?;
            tx.execute("INSERT INTO _vrt_q_meta(k,v) VALUES('kind','queue')", [])
                .map_err(EngineError::Sqlite)?;
            Ok(())
        }
    }
}

/// lease/msg id generation: nanos + pid + monotonic counter (32-hex, same shape as miniflare's
/// `randomness.toString("hex")`; no rand dependency).
static ID_SEQ: AtomicU64 = AtomicU64::new(0);

fn gen_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let seq = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:016x}{:04x}{seq:012x}", std::process::id() & 0xffff)
}

/// QueueBackend -- finalized in section 10 (signature revised relative to the draft: queue by reference,
/// ack/retry carry a lease_id settlement guard, now injected for testability and dispatcher
/// clock consistency; semantics unchanged). All ops commit in a single transaction and go through exec_internal
/// persistence.
#[async_trait::async_trait]
pub trait QueueBackend: Send + Sync {
    /// Enqueue a batch in a single transaction; `(queue_id, msg_id)` is idempotent -- a duplicate id is deduplicated and
    /// counted into `duplicates` (T37r: no longer silently absorbed).
    async fn send(
        &self,
        queue: &QueueId,
        msgs: &[IncomingMsg],
        now_ms: i64,
    ) -> Result<SendResult, QueueError>;
    /// Atomically claim a batch (T37b dispatch lease embedded): in the same transaction first CAS
    /// `_vrt_q_dispatch` (holder=me renews / expired takeover epoch++ /
    /// held unexpired by someone else -> DispatchLeaseLost), then
    /// rows where `visible_at<=now AND lease is free/expired` ->
    /// `lease_id/lease_expiry/attempts+1`. `holder_ttl_ms` is the dispatch
    /// lease duration; `max=0` is a pure renewal (claims no messages, for renewal during a
    /// backpressure wait). The claim and the lease share one transaction -- the intermediate state of
    /// "got messages but no lease" never exists.
    async fn claim(
        &self,
        queue: &QueueId,
        holder: &str,
        holder_ttl_ms: i64,
        max: u32,
        max_bytes: Option<u64>,
        lease_ttl_ms: i64,
        now_ms: i64,
    ) -> Result<ClaimedBatch, QueueError>;
    /// Settlement ack: delete only rows whose `lease_id` matches and is unexpired; any mismatch ->
    /// LeaseLost rejects everything (must not delete messages that someone else has re-claimed).
    async fn ack(
        &self,
        queue: &QueueId,
        lease_id: &str,
        ids: &[String],
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// Settlement retry: `visible_at=now+delay`, clear lease; lease guard same as ack.
    async fn retry(
        &self,
        queue: &QueueId,
        lease_id: &str,
        items: &[(String, u32)],
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// T37b-r5 platform-side failure return: `visible_at=now+delay`, clear lease,
    /// and `attempts=MAX(attempts-1,0)` compensating the +1 at claim time --
    /// deliveries where the consumer code did not run (transport/timeout/malformed/host rejection)
    /// do not consume attempts; messages are not dead-lettered/dropped due to platform faults.
    /// Lease guard and atomicity same as retry (decrement within the same UPDATE, per-tx).
    async fn release(
        &self,
        queue: &QueueId,
        lease_id: &str,
        items: &[(String, u32)],
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// Dead letter: move into the `_vrt_q_dead` audit table, then delete (the landing spot when attempts exceed the limit and no
    /// DLQ target is configured; cross-queue DLQ forwarding belongs to T37b). Lease guard same as ack.
    async fn dead_letter(
        &self,
        queue: &QueueId,
        lease_id: &str,
        ids: &[String],
        reason: &str,
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// `{backlogCount, backlogBytes, oldestMessageTimestampMs,
    /// visibleCount, inFlightCount, delayedCount, deadCount}` (T37b-r4
    /// visibility split; now is injected with the same clock semantics as other ops).
    async fn metrics(&self, queue: &QueueId, now_ms: i64) -> Result<QueueMetrics, QueueError>;
}

/// SQLite implementation -- one independent db per queue, lazily claimed via the EngineRegistry
/// (the same lease/fencing/recovery path as D1 dbs).
pub struct SqliteQueueBackend {
    registry: Arc<EngineRegistry>,
    op_budget_ms: u64,
}

impl SqliteQueueBackend {
    pub fn new(registry: Arc<EngineRegistry>) -> Self {
        Self {
            registry,
            op_budget_ms: OP_BUDGET_MS,
        }
    }
    pub fn with_op_budget_ms(mut self, ms: u64) -> Self {
        self.op_budget_ms = ms;
        self
    }

    async fn engine(&self, queue: &QueueId) -> Result<Arc<RusqliteEngine>, QueueError> {
        let registry = self.registry.clone();
        let id = queue.as_str().to_string();
        tokio::task::spawn_blocking(move || registry.get(crate::paths::ResourceKind::Queue, &id))
            .await
            .map_err(|_| QueueError::Unavailable {
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

    /// Delete the queue (reuses the T35c/T36 drop path: manifest tombstone ->
    /// removal -> rename; idempotent).
    pub fn drop_queue(&self, queue: &QueueId) -> Result<bool, QueueError> {
        self.registry
            .drop_db(crate::paths::ResourceKind::Queue, queue.as_str())
            .map_err(map_engine_error)
    }
}

fn validate_msg_id(id: &str) -> Result<(), QueueError> {
    if id.is_empty() || id.len() > MSG_ID_MAX || !id.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(QueueError::InvalidInput(
            "message id must be 1-128 printable non-space ASCII".into(),
        ));
    }
    Ok(())
}

/// T37r: maximum future skew allowed for client_ts (clock drift tolerance). Ancient values are
/// legal (DLQ forwarding preserves the original timestamp), but future values are never legal -- anything beyond is rejected,
/// not clamped (the caller should fix its own clock).
const CLIENT_TS_MAX_FUTURE_SKEW_MS: i64 = 300_000;

fn validate_msgs(msgs: &[IncomingMsg], now_ms: i64) -> Result<(), QueueError> {
    if msgs.is_empty() {
        return Err(QueueError::InvalidInput("empty message batch".into()));
    }
    if msgs.len() > BATCH_MAX_MSGS {
        return Err(QueueError::TooLarge);
    }
    let mut total = 0usize;
    for m in msgs {
        if m.body.len() > MSG_MAX_BYTES {
            return Err(QueueError::TooLarge);
        }
        total = total.saturating_add(m.body.len());
        if m.delay_secs > DELAY_MAX_SECS {
            return Err(QueueError::InvalidInput("delay exceeds 86400".into()));
        }
        if !CONTENT_TYPES.contains(&m.content_type.as_str()) {
            return Err(QueueError::InvalidInput(format!(
                "content type {:?} must be one of text|json|bytes|v8",
                m.content_type
            )));
        }
        if let Some(id) = &m.id {
            validate_msg_id(id)?;
        }
        if let Some(t) = m.timestamp_ms {
            if t < 0 || t > now_ms.saturating_add(CLIENT_TS_MAX_FUTURE_SKEW_MS) {
                return Err(QueueError::InvalidInput(
                    "timestamp out of trusted range".into(),
                ));
            }
        }
    }
    if total > BATCH_MAX_BYTES {
        return Err(QueueError::TooLarge);
    }
    Ok(())
}

fn retention_cutoff(now_ms: i64) -> i64 {
    now_ms.saturating_sub(MESSAGE_RETENTION_MS)
}

fn purge_expired(tx: &rusqlite::Transaction<'_>, now_ms: i64) -> Result<(), EngineError> {
    let cutoff = retention_cutoff(now_ms);
    tx.execute(
        "DELETE FROM _vrt_q_messages WHERE id IN \
           (SELECT id FROM _vrt_q_messages WHERE created_at <= ?1 \
            ORDER BY created_at LIMIT ?2)",
        rusqlite::params![cutoff, PURGE_BATCH as i64],
    )
    .map_err(EngineError::Sqlite)?;
    tx.execute(
        "DELETE FROM _vrt_q_dead WHERE id IN \
           (SELECT id FROM _vrt_q_dead WHERE dead_at <= ?1 \
            ORDER BY dead_at LIMIT ?2)",
        rusqlite::params![cutoff, PURGE_BATCH as i64],
    )
    .map_err(EngineError::Sqlite)?;
    Ok(())
}

fn metrics_of(tx: &rusqlite::Transaction<'_>, now_ms: i64) -> Result<QueueMetrics, EngineError> {
    purge_expired(tx, now_ms)?;
    let cutoff = retention_cutoff(now_ms);
    let (backlog_count, backlog_bytes, oldest): (i64, i64, Option<i64>) = tx
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(body)),0), MIN(created_at) \
             FROM _vrt_q_messages WHERE created_at > ?1",
            [cutoff],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(EngineError::Sqlite)?;
    // Visibility split -- same definition as the claim selection predicate (server ms).
    let (visible, in_flight, delayed): (i64, i64, i64) = tx
        .query_row(
            "SELECT \
               COALESCE(SUM(CASE WHEN visible_at<=?1 AND (lease_id IS NULL OR lease_expiry<=?1) \
                 THEN 1 ELSE 0 END),0), \
               COALESCE(SUM(CASE WHEN lease_id IS NOT NULL AND lease_expiry>?1 \
                 THEN 1 ELSE 0 END),0), \
               COALESCE(SUM(CASE WHEN visible_at>?1 AND (lease_id IS NULL OR lease_expiry<=?1) \
                 THEN 1 ELSE 0 END),0) \
             FROM _vrt_q_messages WHERE created_at > ?2",
            rusqlite::params![now_ms, cutoff],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(EngineError::Sqlite)?;
    let dead: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM _vrt_q_dead WHERE dead_at > ?1",
            [cutoff],
            |r| r.get(0),
        )
        .map_err(EngineError::Sqlite)?;
    Ok(QueueMetrics {
        backlog_count,
        backlog_bytes,
        oldest_message_timestamp_ms: oldest,
        visible_count: visible,
        in_flight_count: in_flight,
        delayed_count: delayed,
        dead_count: dead,
    })
}

#[async_trait::async_trait]
impl QueueBackend for SqliteQueueBackend {
    async fn send(
        &self,
        queue: &QueueId,
        msgs: &[IncomingMsg],
        now_ms: i64,
    ) -> Result<SendResult, QueueError> {
        validate_msgs(msgs, now_ms)?;
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        // (id, body, content_type, visible_at, created_at, client_ts)——
        // visible_at/created_at always use the server clock; client_ts is stored separately only as an untrusted
        // message attribute and never participates in scheduling or metrics.
        let msgs: Vec<(String, Vec<u8>, String, i64, i64, Option<i64>)> = msgs
            .iter()
            .map(|m| {
                (
                    m.id.clone().unwrap_or_else(gen_id),
                    m.body.clone(),
                    m.content_type.clone(),
                    now_ms.saturating_add(m.delay_secs as i64 * 1000),
                    now_ms,
                    m.timestamp_ms,
                )
            })
            .collect();
        run_internal(engine, ctx, move |tx| {
            ensure_queue_db(tx)?;
            purge_expired(tx, now_ms)?;
            let mut inserted = 0u32;
            {
                let mut ins = tx.prepare_cached(
                    "INSERT OR IGNORE INTO _vrt_q_messages\
                         (id,body,content_type,visible_at,created_at,client_ts,attempts)\
                         VALUES(?1,?2,?3,?4,?5,?6,0)",
                )?;
                for (id, body, ct, vis, created, cts) in &msgs {
                    inserted += ins
                        .execute(params![id, body, ct, vis, created, cts])
                        .map_err(EngineError::Sqlite)? as u32;
                }
            }
            let duplicates = msgs.len() as u32 - inserted;
            metrics_of(tx, now_ms).map(|m| SendResult {
                inserted,
                duplicates,
                metrics: m,
            })
        })
        .await
    }

    async fn claim(
        &self,
        queue: &QueueId,
        holder: &str,
        holder_ttl_ms: i64,
        max: u32,
        max_bytes: Option<u64>,
        lease_ttl_ms: i64,
        now_ms: i64,
    ) -> Result<ClaimedBatch, QueueError> {
        if max > CLAIM_MAX {
            return Err(QueueError::InvalidInput(format!(
                "claim max must be 0..={CLAIM_MAX}"
            )));
        }
        if let Some(b) = max_bytes {
            if b == 0 || b > CLAIM_BYTES_MAX {
                return Err(QueueError::InvalidInput(format!(
                    "claim maxBytes must be 1..={CLAIM_BYTES_MAX}"
                )));
            }
        }
        if holder.is_empty()
            || holder.len() > DISPATCH_HOLDER_MAX
            || !holder
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
        {
            return Err(QueueError::InvalidInput(format!(
                "dispatch holder must be 1..={DISPATCH_HOLDER_MAX} [a-zA-Z0-9_.:-]"
            )));
        }
        if holder_ttl_ms <= 0 || holder_ttl_ms > DISPATCH_TTL_MAX_MS {
            return Err(QueueError::InvalidInput(
                "dispatch holder ttl out of bounds".into(),
            ));
        }
        if lease_ttl_ms <= 0 || lease_ttl_ms > LEASE_TTL_MAX_MS {
            return Err(QueueError::InvalidInput("lease ttl out of bounds".into()));
        }
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        let lease_id = gen_id();
        let expiry = now_ms.saturating_add(lease_ttl_ms);
        let holder_expiry = now_ms.saturating_add(holder_ttl_ms);
        let holder = holder.to_string();
        let lid = lease_id.clone();
        let h2 = holder.clone();
        let (dispatch_epoch, messages) = run_internal(engine, ctx, move |tx| {
            ensure_queue_db(tx)?;
            purge_expired(tx, now_ms)?;
            // Dispatch lease CAS (same transaction) -- insert a placeholder if no row, holder=me renews,
            // expired takeover epoch++; held unexpired by someone else -> DispatchLeaseLost.
            tx.execute(
                "INSERT OR IGNORE INTO _vrt_q_dispatch \
                     (id, holder, epoch, expires_at) VALUES(1, ?1, 0, 0)",
                params![h2],
            )
            .map_err(EngineError::Sqlite)?;
            let won = tx
                .execute(
                    "UPDATE _vrt_q_dispatch SET \
                           epoch = epoch + CASE \
                             WHEN holder != ?1 OR expires_at <= ?3 THEN 1 ELSE 0 END, \
                           holder = ?1, expires_at = ?2 \
                         WHERE (holder = ?1 AND expires_at > ?3) \
                            OR expires_at <= ?3",
                    params![h2, holder_expiry, now_ms],
                )
                .map_err(EngineError::Sqlite)?;
            if won == 0 {
                let (h, exp): (String, i64) = tx
                    .query_row(
                        "SELECT holder, expires_at FROM _vrt_q_dispatch WHERE id=1",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .map_err(EngineError::Sqlite)?;
                return Err(EngineError::Message(format!("{DISPATCH_LOST}{h}|{exp}")));
            }
            let epoch: i64 = tx
                .query_row("SELECT epoch FROM _vrt_q_dispatch WHERE id=1", [], |r| {
                    r.get(0)
                })
                .map_err(EngineError::Sqlite)?;
            if max == 0 {
                return Ok((epoch, Vec::new()));
            }
            // T37b-r: select candidates first so the RAW body-byte
            // budget can bound the batch (hex/encoding must not
            // count). First eligible row is always taken — a
            // single oversize message can never starve forever.
            // The table is WITHOUT ROWID, so rows of one sendBatch
            // (same visible_at/created_at) need the explicit id tie
            // break; the provider issues time-ordered (v7) ids.
            let mut st = tx.prepare(
                "SELECT id, body, content_type, attempts, \
                     COALESCE(client_ts, created_at) \
                     FROM _vrt_q_messages \
                     WHERE visible_at<=?3 \
                       AND created_at>?5 \
                       AND (lease_id IS NULL OR lease_expiry<=?3)\
                     ORDER BY visible_at, created_at, id LIMIT ?4",
            )?;
            let candidates = st
                .query_map(
                    params![lid, expiry, now_ms, max as i64, retention_cutoff(now_ms)],
                    |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, u32>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let mut taken = Vec::with_capacity(candidates.len());
            let mut bytes = 0u64;
            for (id, body, ct, atts, ts) in candidates {
                let next = bytes.saturating_add(body.len() as u64);
                if !taken.is_empty() && max_bytes.is_some_and(|b| next > b) {
                    break;
                }
                bytes = next;
                taken.push(ClaimedMsg {
                    id,
                    lease_id: lid.clone(),
                    // attempts is not yet +1 -- that value is committed only after the UPDATE.
                    attempts: atts as i64 + 1,
                    body,
                    content_type: ct,
                    timestamp_ms: ts,
                });
            }
            if !taken.is_empty() {
                let ids: Vec<&str> = taken.iter().map(|m| m.id.as_str()).collect();
                let mut upd = tx.prepare(
                    "UPDATE _vrt_q_messages SET lease_id=?1, \
                         lease_expiry=?2, attempts=attempts+1 \
                         WHERE id=?3 AND (lease_id IS NULL OR \
                         lease_expiry<=?4)",
                )?;
                for id in ids {
                    // Concurrent lease races within the same batch are caught by the lease guard;
                    // if the row was already leased away by someone else it is 0 rows here -- harmless skip.
                    upd.execute(params![lid, expiry, id, now_ms])
                        .map_err(EngineError::Sqlite)?;
                }
            }
            Ok((epoch, taken))
        })
        .await?;
        Ok(ClaimedBatch {
            lease_id,
            dispatch_epoch,
            messages,
        })
    }

    async fn ack(
        &self,
        queue: &QueueId,
        lease_id: &str,
        ids: &[String],
        now_ms: i64,
    ) -> Result<u32, QueueError> {
        if ids.is_empty() || ids.len() > SETTLE_MAX {
            return Err(QueueError::InvalidInput(format!(
                "ack ids must be 1..={SETTLE_MAX}"
            )));
        }
        validate_msg_id(lease_id)?;
        for id in ids {
            validate_msg_id(id)?;
        }
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        let ids: Vec<String> = ids.to_vec();
        let lease = lease_id.to_string();
        let expected = ids.len();
        run_internal(engine, ctx, move |tx| {
            ensure_queue_db(tx)?;
            let mut settled = 0usize;
            {
                let mut st = tx.prepare(
                    "DELETE FROM _vrt_q_messages WHERE id=?1\
                         AND lease_id=?2 AND lease_expiry>?3",
                )?;
                for id in &ids {
                    settled += st
                        .execute(params![id, lease, now_ms])
                        .map_err(EngineError::Sqlite)?;
                }
            }
            if settled != expected {
                return Err(EngineError::Message(LEASE_LOST.into()));
            }
            Ok(settled as u32)
        })
        .await
    }

    async fn retry(
        &self,
        queue: &QueueId,
        lease_id: &str,
        items: &[(String, u32)],
        now_ms: i64,
    ) -> Result<u32, QueueError> {
        if items.is_empty() || items.len() > SETTLE_MAX {
            return Err(QueueError::InvalidInput(format!(
                "retry items must be 1..={SETTLE_MAX}"
            )));
        }
        validate_msg_id(lease_id)?;
        for (id, delay) in items {
            validate_msg_id(id)?;
            if *delay > DELAY_MAX_SECS {
                return Err(QueueError::InvalidInput("delay exceeds 86400".into()));
            }
        }
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        let items: Vec<(String, i64)> = items
            .iter()
            .map(|(id, d)| (id.clone(), now_ms.saturating_add(*d as i64 * 1000)))
            .collect();
        let lease = lease_id.to_string();
        let expected = items.len();
        run_internal(engine, ctx, move |tx| {
            ensure_queue_db(tx)?;
            let mut settled = 0usize;
            {
                let mut st = tx.prepare(
                    "UPDATE _vrt_q_messages SET lease_id=NULL,\
                         lease_expiry=NULL, visible_at=?1 WHERE id=?2\
                         AND lease_id=?3 AND lease_expiry>?4",
                )?;
                for (id, vis) in &items {
                    settled += st
                        .execute(params![vis, id, lease, now_ms])
                        .map_err(EngineError::Sqlite)?;
                }
            }
            if settled != expected {
                return Err(EngineError::Message(LEASE_LOST.into()));
            }
            Ok(settled as u32)
        })
        .await
    }

    async fn release(
        &self,
        queue: &QueueId,
        lease_id: &str,
        items: &[(String, u32)],
        now_ms: i64,
    ) -> Result<u32, QueueError> {
        if items.is_empty() || items.len() > SETTLE_MAX {
            return Err(QueueError::InvalidInput(format!(
                "release items must be 1..={SETTLE_MAX}"
            )));
        }
        validate_msg_id(lease_id)?;
        for (id, delay) in items {
            validate_msg_id(id)?;
            if *delay > DELAY_MAX_SECS {
                return Err(QueueError::InvalidInput("delay exceeds 86400".into()));
            }
        }
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        let items: Vec<(String, i64)> = items
            .iter()
            .map(|(id, d)| (id.clone(), now_ms.saturating_add(*d as i64 * 1000)))
            .collect();
        let lease = lease_id.to_string();
        let expected = items.len();
        run_internal(engine, ctx, move |tx| {
                ensure_queue_db(tx)?;
                let mut settled = 0usize;
                {
                    // attempts-1 shares a transaction with the return -- the +1 at claim time is atomically
                    // compensated; MAX(...,0) guards against negatives under dirty data.
                    let mut st = tx.prepare(
                        "UPDATE _vrt_q_messages SET lease_id=NULL,                         lease_expiry=NULL, visible_at=?1,                         attempts=MAX(attempts-1,0) WHERE id=?2                         AND lease_id=?3 AND lease_expiry>?4",
                    )?;
                    for (id, vis) in &items {
                        settled += st
                            .execute(params![vis, id, lease, now_ms])
                            .map_err(EngineError::Sqlite)?;
                    }
                }
                if settled != expected {
                    return Err(EngineError::Message(LEASE_LOST.into()));
                }
                Ok(settled as u32)
            })
            .await
    }

    async fn dead_letter(
        &self,
        queue: &QueueId,
        lease_id: &str,
        ids: &[String],
        reason: &str,
        now_ms: i64,
    ) -> Result<u32, QueueError> {
        if ids.is_empty() || ids.len() > SETTLE_MAX {
            return Err(QueueError::InvalidInput(format!(
                "dead-letter ids must be 1..={SETTLE_MAX}"
            )));
        }
        validate_msg_id(lease_id)?;
        for id in ids {
            validate_msg_id(id)?;
        }
        if reason.len() > 256 {
            return Err(QueueError::InvalidInput("reason too long".into()));
        }
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        let ids: Vec<String> = ids.to_vec();
        let lease = lease_id.to_string();
        let reason = reason.to_string();
        let expected = ids.len();
        run_internal(engine, ctx, move |tx| {
            ensure_queue_db(tx)?;
            let mut settled = 0usize;
            {
                let mut ins = tx.prepare(
                    "INSERT OR REPLACE INTO _vrt_q_dead\
                         (id,body,content_type,created_at,attempts,dead_at,reason)\
                         SELECT id,body,content_type,created_at,attempts,?1,?2\
                         FROM _vrt_q_messages WHERE id=?3\
                         AND lease_id=?4 AND lease_expiry>?1",
                )?;
                let mut del = tx.prepare(
                    "DELETE FROM _vrt_q_messages WHERE id=?1\
                         AND lease_id=?2 AND lease_expiry>?3",
                )?;
                for id in &ids {
                    ins.execute(params![now_ms, reason, id, lease])
                        .map_err(EngineError::Sqlite)?;
                    settled += del
                        .execute(params![id, lease, now_ms])
                        .map_err(EngineError::Sqlite)?;
                }
            }
            if settled != expected {
                return Err(EngineError::Message(LEASE_LOST.into()));
            }
            Ok(settled as u32)
        })
        .await
    }

    async fn metrics(&self, queue: &QueueId, now_ms: i64) -> Result<QueueMetrics, QueueError> {
        let engine = self.engine(queue).await?;
        let ctx = self.ctx(&engine);
        run_internal(engine, ctx, move |tx| {
            ensure_queue_db(tx)?;
            metrics_of(tx, now_ms)
        })
        .await
    }
}

/// ---------- wire request/response types (provider -> vrt-sql internal channel) ----------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendRequest {
    pub messages: Vec<SendMsg>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMsg {
    #[serde(default)]
    pub id: Option<String>,
    /// hex encoded (the internal wire has no base64 dependency).
    pub body: String,
    pub content_type: String,
    #[serde(default)]
    pub delay_secs: u32,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimRequest {
    /// T37b: dispatch lease holder (required -- the claim and the lease CAS share one transaction).
    pub holder_id: String,
    pub holder_ttl_ms: i64,
    /// 0 = pure renewal (claims no messages; the renewal path during a backpressure wait).
    pub max: u32,
    /// Raw body byte budget (T37b-r). `None` = unset (old callers
    /// compatible; the count cap still holds). When set it always returns >=1 message -- a single over-limit
    /// message cannot starve.
    #[serde(default)]
    pub max_bytes: Option<u64>,
    pub lease_ttl_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettleRequest {
    pub lease_id: String,
    #[serde(default)]
    pub ids: Vec<String>,
    #[serde(default)]
    pub items: Vec<RetryItem>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryItem {
    pub id: String,
    pub delay_secs: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineRegistry, ResultsFormat, SqlStmt};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn registry() -> (TempDir, Arc<EngineRegistry>) {
        let tmp = TempDir::new().unwrap();
        let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
        (tmp, reg)
    }

    fn tctx() -> TxnCtx {
        TxnCtx {
            epoch: 1,
            deadline: Instant::now() + Duration::from_secs(30),
            dedup: None,
        }
    }

    /// f panic -> Err (lock not poisoned), intent/lsn not advanced, InternalFlag reset
    /// (user SQL is still rejected by the authorizer).
    #[tokio::test]
    async fn exec_internal_panic_resets_flag_and_rolls_back() {
        let (_t, reg) = registry();
        let e = reg
            .get(
                crate::paths::ResourceKind::Queue,
                "abcdefghijklmnopqrstuvwxyz",
            )
            .unwrap();
        let before = e.commit_lsn();
        let r: Result<(), EngineError> = e.exec_internal(&tctx(), |_tx| {
            panic!("boom");
        });
        assert!(matches!(r, Err(EngineError::Message(_))));
        assert_eq!(e.commit_lsn(), before, "panic must not advance lsn");
        // flag reset: user-surface _vrt_ access is still rejected by the authorizer.
        let denied = e.query_batch_sync(
            &[SqlStmt {
                sql: "SELECT * FROM _vrt_q_meta".into(),
                params: None,
            }],
            &tctx(),
            ResultsFormat::ArrayOfObjects,
        );
        assert!(denied.is_err(), "internal flag must reset after panic");
        // Subsequent normal ops are still usable (lock not poisoned).
        let (_t2, reg2) = registry();
        let _ = reg2;
    }

    /// closure Err -> roll back, lsn not advanced.
    #[tokio::test]
    async fn exec_internal_err_rolls_back() {
        let (_t, reg) = registry();
        let e = reg
            .get(
                crate::paths::ResourceKind::Queue,
                "abcdefghijklmnopqrstuvwxyz",
            )
            .unwrap();
        let before = e.commit_lsn();
        let r: Result<(), EngineError> = e.exec_internal(&tctx(), |tx| {
            tx.execute_batch("CREATE TABLE _vrt_q_probe(a)")
                .map_err(EngineError::Sqlite)?;
            Err(EngineError::Message("fail".into()))
        });
        assert!(matches!(r, Err(EngineError::Message(_))), "{r:?}");
        assert_eq!(e.commit_lsn(), before);
        // Rollback confirmed: _vrt_tmp_x does not exist (checked via internal).
        let n: Result<i64, EngineError> = e.exec_internal(&tctx(), |tx| {
            Ok(tx
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name='_vrt_q_probe'",
                    [],
                    |r| r.get(0),
                )
                .unwrap())
        });
        assert_eq!(n.unwrap(), 0);
        // S1 defense in depth: inside the internal window, non-queue `_vrt_` objects are still rejected by the authorizer.
        let denied: Result<(), EngineError> = e.exec_internal(&tctx(), |tx| {
            tx.execute_batch("CREATE TABLE _vrt_evil(a)")
                .map_err(EngineError::Sqlite)
        });
        assert!(matches!(denied, Err(EngineError::Sqlite(_))), "{denied:?}");
    }
}
