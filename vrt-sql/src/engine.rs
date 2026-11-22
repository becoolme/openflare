//! SqlEngine trait + RusqliteEngine (the P3a implementation of the section 10 trait draft).
//!
//! Durability level (T33): local fsync only, no replication; the epoch field is reserved.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use thiserror::Error;

use crate::paths::{self, PathError};
use crate::security;

// ---------- wire types (aligned with the D1 protocol, section 2 table) ----------

/// `Value = number | string | null | number[]` (number[] -> BLOB).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqlValue {
    fn to_rusqlite(&self) -> Value {
        match self {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(i) => Value::Integer(*i),
            SqlValue::Real(f) => Value::Real(*f),
            SqlValue::Text(t) => Value::Text(t.clone()),
            SqlValue::Blob(b) => Value::Blob(b.clone()),
        }
    }
}

impl<'de> Deserialize<'de> for SqlValue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        match v {
            serde_json::Value::Null => Ok(SqlValue::Null),
            serde_json::Value::Bool(_) => Err(serde::de::Error::custom("D1_TYPE_ERROR")),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(SqlValue::Integer(i))
                } else {
                    Ok(SqlValue::Real(n.as_f64().unwrap_or(0.0)))
                }
            }
            serde_json::Value::String(t) => Ok(SqlValue::Text(t)),
            serde_json::Value::Array(items) => {
                // number[] -> BLOB; a non-number element is a schema violation
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    match it.as_u64().filter(|u| *u <= 255) {
                        Some(u) => out.push(u as u8),
                        None => {
                            return Err(serde::de::Error::custom(
                                "params array items must be numbers 0-255",
                            ))
                        }
                    }
                }
                Ok(SqlValue::Blob(out))
            }
            _ => Err(serde::de::Error::custom("unsupported param type")),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SqlStmt {
    pub sql: String,
    #[serde(default)]
    pub params: Option<Vec<SqlValue>>,
}

/// `?resultsFormat=`: invalid/missing falls back to ARRAY_OF_OBJECTS (miniflare `.catch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultsFormat {
    ArrayOfObjects,
    RowsAndColumns,
    None,
}

impl ResultsFormat {
    pub fn parse(param: Option<&str>) -> Self {
        match param {
            Some("ROWS_AND_COLUMNS") => Self::RowsAndColumns,
            Some("NONE") => Self::None,
            _ => Self::ArrayOfObjects,
        }
    }
}

/// Cloudflare's documented maximum query result size is 50 MB
/// (https://developers.cloudflare.com/d1/platform/limits/), enforced
/// since 2026-11-04. D1 counts MB as 10^6 bytes on the same page
/// (100 KB = 100,000 bytes, 2 MB = 2,000,000 bytes), so the cap is
/// expressed the same way here.
pub const D1_RESULT_MAX_BYTES: usize = 50_000_000;

/// Documented error text for an oversized query result
/// (https://developers.cloudflare.com/d1/observability/debug-d1/#error-list).
/// workerd's `d1-api` adds the `D1_ERROR: ` prefix when it throws.
pub const D1_RESULT_TOO_LARGE: &str =
    "D1 query result exceeded the maximum response size limit of 50 MB";

/// In-process ceiling for `dump()`: unlike query results, D1 export is
/// documented as *not* subject to the 50 MB query-result limit, but the
/// provider's transport cap is 64 MiB and the whole export is buffered
/// in memory. Exceeding this fails closed with a bounded error instead
/// of an unbounded allocation.
pub const D1_DUMP_MAX_BYTES: usize = 64 * 1024 * 1024;
const D1_DUMP_TOO_LARGE: &str = "D1 dump exceeded the local 64 MiB export limit";

#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    pub served_by: String,
    pub duration: f64,
    pub changes: i64,
    pub last_row_id: i64,
    pub changed_db: bool,
    pub size_after: i64,
    pub rows_read: i64,
    pub rows_written: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub results: serde_json::Value,
    pub meta: Meta,
}

// ---------- errors ----------

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("{0}")]
    Message(String),
    #[error("request timed out")]
    Timeout,
    #[error("writer is fenced (epoch {0} expired)")]
    Fenced(u64),
    #[error("database already open (single writer)")]
    AlreadyOpen,
    /// T35c: the id was tombstoned by a DROP op (in-memory set + on-disk `.dropped`
    /// marker). Fail closed; the wire surface maps it to `{success:false}`.
    #[error("database dropped")]
    Dropped,
    /// T36: transiently unavailable (lease alive elsewhere / persistence layer unreachable / restore failed).
    /// Wire maps it to 503 + `code:"VRT_UNAVAILABLE"` + retryable.
    #[error("unavailable: {0}")]
    Unavailable(&'static str),
    /// RV-R71: the call's deadline expired BEFORE anything was
    /// committed (lock wait / lease renew / cold open / outer cutoff).
    /// wire: 503 + `code:"VRT_SQL_TIMEOUT"` + retryable (safe to retry).
    #[error("deadline exceeded before commit")]
    DeadlineExceeded,
    /// RV-R71: the deadline expired AFTER the local commit started
    /// (durable-publish or outer cutoff) — whether the write took
    /// effect is unknown. wire: 503 + `code:"VRT_SQL_RESULT_UNKNOWN"`;
    /// retry only with the SAME `(epoch, seq)` idempotency key (the
    /// dedup ledger commits in the same transaction).
    #[error("commit outcome unknown (deadline exceeded after commit)")]
    ResultUnknown,
    /// Read-only replica refused a statement that would write. The wire maps this to
    /// HTTP 409 + `code:"VRT_REPLICA_READ_ONLY"` so the provider can retry on the writer.
    /// Never produced by the writer engine.
    #[error("attempt to write a readonly database")]
    ReadOnly,
    #[error("path: {0}")]
    Path(#[from] PathError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl EngineError {
    /// Wire `{error}` text (the object golden compares against).
    pub fn wire_message(&self) -> String {
        match self {
            EngineError::Sqlite(e) => e.to_string(),
            other => other.to_string(),
        }
    }
}

// ---------- trait (section 10 draft, trimmed for P3a: synchronous engine) ----------

/// RV-R71: set by the engine the moment `tx.commit()` returns Ok, so a
/// caller that abandons a blocking call at its deadline can tell
/// "nothing committed" (retry-safe) from "commit outcome unknown".
/// Scoped per blocking task via [`with_commit_probe`] — no `TxnCtx`
/// field churn, no global state.
#[derive(Clone, Default, Debug)]
pub struct CommitProbe(Arc<std::sync::atomic::AtomicBool>);

impl CommitProbe {
    pub fn new() -> Self {
        Self::default()
    }
    /// True once a local commit happened under this probe.
    pub fn committed(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

thread_local! {
    static COMMIT_PROBE: std::cell::RefCell<Option<CommitProbe>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` (synchronously, same thread) with `probe` armed.
pub fn with_commit_probe<R>(probe: &CommitProbe, f: impl FnOnce() -> R) -> R {
    struct Reset(Option<CommitProbe>);
    impl Drop for Reset {
        fn drop(&mut self) {
            COMMIT_PROBE.with(|c| *c.borrow_mut() = self.0.take());
        }
    }
    let prev = COMMIT_PROBE.with(|c| c.borrow_mut().replace(probe.clone()));
    let _reset = Reset(prev);
    f()
}

fn mark_committed() {
    COMMIT_PROBE.with(|c| {
        if let Some(p) = c.borrow().as_ref() {
            p.0.store(true, Ordering::SeqCst);
        }
    });
}

pub struct TxnCtx {
    pub epoch: u64,
    pub deadline: Instant,
    /// `(epoch, seq)` dedup key; None = no dedup (compatible with direct-connect tests).
    pub dedup: Option<(u64, u64)>,
}

pub struct LtxSegment;

pub struct Snapshot;

#[async_trait::async_trait]
pub trait SqlEngine: Send + Sync {
    /// Execute the whole batch in a single transaction; on failure the whole batch rolls back and only the first error is returned.
    /// resultsFormat is a wire concern; the trait defaults to the array-of-objects form.
    async fn query_batch(
        &self,
        batch: &[SqlStmt],
        ctx: &TxnCtx,
    ) -> Result<Vec<QueryResult>, EngineError>;
    /// Current commit position (bare lsn; HMAC/fixed-width wrapping lives in the wire-layer BookmarkSigner).
    async fn bookmark(&self) -> Result<u64, EngineError>;
    /// Force a checkpoint and return the uploadable LTX segment (replication is not implemented in P3a -> error).
    async fn checkpoint(&self) -> Result<LtxSegment, EngineError>;
    /// Restore from a snapshot (not implemented in P3a -> error).
    async fn restore(&self, snap: &Snapshot) -> Result<(), EngineError>;
    fn size_bytes(&self) -> u64;
}

// ---------- engine ----------

/// Canonical digest of the request payload (used for dedup key-collision detection; covers sql+params).
fn req_digest(stmts: &[&SqlStmt]) -> String {
    let mut h = sha2::Sha256::new();
    for q in stmts {
        h.update((q.sql.len() as u64).to_be_bytes());
        h.update(q.sql.as_bytes());
        for p in q.params.as_deref().unwrap_or_default() {
            match p {
                SqlValue::Null => h.update([0u8]),
                SqlValue::Integer(i) => {
                    h.update([1u8]);
                    h.update(i.to_be_bytes());
                }
                SqlValue::Real(f) => {
                    h.update([2u8]);
                    h.update(f.to_bits().to_be_bytes());
                }
                SqlValue::Text(t) => {
                    h.update([3u8]);
                    h.update((t.len() as u64).to_be_bytes());
                    h.update(t.as_bytes());
                }
                SqlValue::Blob(b) => {
                    h.update([4u8]);
                    h.update((b.len() as u64).to_be_bytes());
                    h.update(b);
                }
            }
        }
        h.update([0xffu8]); // statement separator
    }
    hex::encode(h.finalize())
}

const DEDUP_DDL: &str = "CREATE TABLE IF NOT EXISTS _vrt_dedup(
    epoch INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    req_sha TEXT NOT NULL DEFAULT '',
    committed_at INTEGER NOT NULL,
    PRIMARY KEY (epoch, seq)
)";

/// The same non-empty check as miniflare: `sql.replace(/^\s+--.*/gm, "").trim().len() > 0`
/// (database.worker.js:175-176). Note `\s+` can swallow newlines: "blank line + comment line"
/// is stripped as a whole; but `--` directly at line start (no leading whitespace on the line) is not stripped -- a string-initial
/// `"-- comment"` reaches SQLite unchanged and reports "did not contain a statement".
fn is_nonempty_sql(sql: &str) -> bool {
    !strip_comment_lines(sql).iter().all(|&b| is_sql_space(b))
}

fn is_sql_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// Hand-written equivalent of `/^\s+--.*/gm`: starting at a "line-start position" (pos0 or after a newline),
/// if consecutive whitespace is followed by `--`, strip from line start to line end (newline excluded).
fn strip_comment_lines(sql: &str) -> Vec<u8> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::with_capacity(n);
    let mut i = 0;
    let mut at_line_start = true;
    while i < n {
        if at_line_start {
            let mut j = i;
            while j < n && is_sql_space(b[j]) {
                j += 1;
            }
            if j > i && j + 1 < n && b[j] == b'-' && b[j + 1] == b'-' {
                // Hit: discard [i, next newline)
                let mut k = j;
                while k < n && b[k] != b'\n' {
                    k += 1;
                }
                i = k;
                at_line_start = false;
                continue;
            }
        }
        out.push(b[i]);
        at_line_start = b[i] == b'\n';
        i += 1;
    }
    out
}

struct EngineInner {
    conn: Connection,
    /// Authorizer bypass for engine-internal statements (set only by engine code)
    internal: security::InternalFlag,
    /// T36: page size (segment encoding / snapshot parsing).
    page_size: u32,
    /// T36: committed WAL frame index watermark (mxFrame). Read and written only
    /// under the single-writer lock; observed by PASSIVE checkpoint, zeroed after TRUNCATE.
    wal_frames: u64,
    /// RV-R31 #1: current WAL generation salt pair -- frame capture only accepts this generation.
    /// `None` = not seeded (the first capture after open/TRUNCATE seeds from the then-current
    /// header; when the watermark is 0 no generation-switch distinction is needed).
    wal_salt: Option<(u32, u32)>,
    /// RV-R31 #1: cumulative checksum after the watermark frame -- the
    /// verification seed for the next frame chain. Cleared to 0 with the generation after TRUNCATE.
    wal_cksum: (u32, u32),
    /// RV-R31 #1: WAL checksum byte order (last bit of the header magic).
    wal_be: bool,
    /// T36: lease + persistence slot (when a persistence backend exists).
    persist: Option<PersistSlot>,
    /// Time Travel index: commits not yet flushed to a timeline chunk. Best-effort history --
    /// a crash loses at most [`crate::timeline::FLUSH_COMMITS`] entries.
    timeline: Vec<crate::timeline::TimelineEntry>,
}

/// T36: persistence state held by the engine (maintained under the inner lock -- all persistence
/// operations and the local commit share the single-writer critical section, so ordering is naturally serial).
struct PersistSlot {
    handle: crate::persist::ManifestHandle,
    persister: std::sync::Arc<crate::persist::Persister>,
    /// T36b: dynamic checkpoint interval (commits) -- doubles on failure,
    /// capped at `every*64`; resets to `every` on success. Prevents the pathological loop of
    /// "snapshot over limit / store flapping -> whole db re-uploaded on every commit".
    ckpt_gap: u64,
}

pub struct RusqliteEngine {
    inner: Mutex<EngineInner>,
    commit_lsn: AtomicU64,
    served_by: String,
    #[allow(dead_code)]
    database_id: String,
    /// T35c: registry drop_db sets this inside the same critical section that removes the engine.
    /// Checked once at query_batch entry and once before commit -- new writes in the window between removal and directory rename/
    /// deletion can no longer "succeed and then get deleted".
    dropped: AtomicBool,
    /// T36: lease lost / persistence failed -> fenced; afterwards all reads and writes are rejected,
    /// and the registry rebuilds via the recovery path after removal.
    fenced: AtomicBool,
    /// T36b: the latest checkpoint was rejected for exceeding the snapshot limit (diagnostics/alerting surface).
    snapshot_over_cap: AtomicBool,
    /// T36: writer epoch (the epoch component of bookmarks; 1 without persistence,
    /// equivalent to the old behavior -- the old bookmark's epoch field came from the caller).
    lease_epoch: AtomicU64,
    /// T36: the last renewal failed but is still within the margin (read for diagnostics/metrics).
    degraded: AtomicBool,
    /// T36: db file path (to locate the WAL sidecar).
    db_file: PathBuf,
    /// Writer engines hold an flock for their lifetime; a read-only replica generation is
    /// immutable and has no lock.
    _lock_file: Option<std::fs::File>,
    /// Feature follow-up (D1 follower): read-only replica engine. Skips commit-lsn accounting
    /// and maps SQLITE_READONLY to [`EngineError::ReadOnly`].
    read_only: bool,
}

impl RusqliteEngine {
    /// Open (creating if necessary) the database file; the Registry guarantees a single open within the process,
    /// and flock(LOCK_EX|LOCK_NB) covers duplicate opens across processes (fail closed).
    pub fn open(db_root: &Path, database_id: &str) -> Result<Self, EngineError> {
        // T35c tombstone: check the `.dropped` marker first, then derive --
        // derive_db_file creates directories; the reverse order would "resurrect" a deleted id.
        if paths::marker_exists(db_root, database_id)? {
            return Err(EngineError::Dropped);
        }
        let db_file = paths::derive_db_file(db_root, database_id)?;
        Self::open_file_opts(&db_file, database_id, false)
    }

    /// T36: when `wal_capture=true` (persistence enabled), disable auto-checkpoint
    /// and TRUNCATE-checkpoint at open: from then on the WAL file only shrinks on our explicit
    /// checkpoint, which makes byte-watermark capture valid (`persist_commit`).
    pub fn open_opts(
        db_root: &Path,
        database_id: &str,
        wal_capture: bool,
    ) -> Result<Self, EngineError> {
        if paths::marker_exists(db_root, database_id)? {
            return Err(EngineError::Dropped);
        }
        let db_file = paths::derive_db_file(db_root, database_id)?;
        Self::open_file_opts(&db_file, database_id, wal_capture)
    }

    /// Tests/embedded use: specify the file path directly (still goes through flock + safe initialization).
    pub fn open_file(db_file: &Path, database_id: &str) -> Result<Self, EngineError> {
        Self::open_file_opts(db_file, database_id, false)
    }

    pub fn open_file_opts(
        db_file: &Path,
        database_id: &str,
        wal_capture: bool,
    ) -> Result<Self, EngineError> {
        if let Some(parent) = db_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // File lock: sidecar {db}.lock (the db itself is not locked -- on macOS flock goes through fcntl
        // and would conflict with SQLite's own POSIX locks, producing SQLITE_BUSY).
        let lock_path = db_file.with_extension("lock");
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)?;
// SAFETY: flock(2) only requires an fd open on the lock file; the fd is a live owned or borrowed File.
        if unsafe { libc::flock(lock_file_fd(&lock_file), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(EngineError::AlreadyOpen);
        }

        let conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )?;
        let internal = security::configure(&conn)?;
        internal.set(true);
        // T36: wal_capture disables auto-checkpoint -- the WAL may only shrink on our explicit
        // checkpoint, making the file byte watermark a monotonic sequence (capture correctness).
        // RV-R31 #2: a persistent db must fsync the WAL before commit returns --
        // the `.vrt-lsn` marker promises "the local file has reached manifest.lsn";
        // under synchronous=NORMAL commit returns once data is in OS buffers; after power loss the
        // WAL tail is lost while the marker remains -> at claim time the missing data is trusted (fail
        // closed: switched to FULL; the cost is one fsync per commit, far below the object-store RTT of
        // synchronously uploading a segment). Dbs without persistence make no such promise and are untouched.
        let init_sql = if wal_capture {
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=FULL;"
        } else {
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"
        };
        let init = conn
            .execute_batch(init_sql)
            .and_then(|_| conn.execute_batch(DEDUP_DDL));
        let page_size = conn
            .query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
            .unwrap_or(4096);
        // T36: in capture mode TRUNCATE at open -- committed frames are merged into the db
        // file, dead frames left by a crash are cleared too, the wal file is zeroed, and the watermark starts from 0.
        // busy -> fail closed (a single connection should not be busy during open).
        let wal_frames = if wal_capture {
            match conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                r.get::<_, i64>(0)
            }) {
                Ok(0) => 0,
                Ok(_) => {
                    internal.set(false);
                    return Err(EngineError::Message("wal checkpoint busy at open".into()));
                }
                Err(e) => {
                    internal.set(false);
                    return Err(EngineError::Sqlite(e));
                }
            }
        } else {
            0
        };
        internal.set(false);
        init?;
        Ok(Self {
            inner: Mutex::new(EngineInner {
                conn,
                internal,
                page_size,
                wal_frames,
                wal_salt: None,
                wal_cksum: (0, 0),
                wal_be: false,
                persist: None,
                timeline: Vec::new(),
            }),
            commit_lsn: AtomicU64::new(0),
            served_by: "vrt-sql".into(),
            database_id: database_id.into(),
            dropped: AtomicBool::new(false),
            fenced: AtomicBool::new(false),
            snapshot_over_cap: AtomicBool::new(false),
            lease_epoch: AtomicU64::new(1),
            degraded: AtomicBool::new(false),
            db_file: db_file.to_path_buf(),
            _lock_file: Some(lock_file),
            read_only: false,
        })
    }

    /// Open a published follower generation as a read-only engine.
    ///
    /// The file is immutable by construction (the follower renames a completed generation into
    /// place and never writes it again), so `immutable=1` is safe and avoids requiring a writable
    /// `-shm`. No lock file is created: readers do not exclude the follower.
    ///
    /// `epoch`/`lsn` are the follower watermark that produced this generation; they seed the
    /// engine position for diagnostics and bookmark minting.
    pub fn open_immutable(
        db_file: &Path,
        database_id: &str,
        epoch: u64,
        lsn: u64,
    ) -> Result<Self, EngineError> {
        let path = std::fs::canonicalize(db_file)?;
        let uri = format!("file:{}?immutable=1", crate::follower::sqlite_uri_path(&path));
        let conn = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )?;
        let internal = security::configure(&conn)?;
        let page_size = conn
            .query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
            .unwrap_or(4096);
        Ok(Self {
            inner: Mutex::new(EngineInner {
                conn,
                internal,
                page_size,
                wal_frames: 0,
                wal_salt: None,
                wal_cksum: (0, 0),
                wal_be: false,
                persist: None,
                timeline: Vec::new(),
            }),
            commit_lsn: AtomicU64::new(lsn),
            served_by: "vrt-sql-replica".into(),
            database_id: database_id.into(),
            dropped: AtomicBool::new(false),
            fenced: AtomicBool::new(false),
            snapshot_over_cap: AtomicBool::new(false),
            lease_epoch: AtomicU64::new(epoch),
            degraded: AtomicBool::new(false),
            db_file: path,
            _lock_file: None,
            read_only: true,
        })
    }

    /// Whether this engine is a read-only follower generation.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// T36: the registry installs persistence state after a successful claim; commit_lsn and
    /// the writer epoch are aligned with the manifest (never rolled back across takeovers).
    pub fn install_persistence(
        &self,
        handle: crate::persist::ManifestHandle,
        persister: std::sync::Arc<crate::persist::Persister>,
    ) {
        self.commit_lsn.store(handle.lsn, Ordering::SeqCst);
        self.lease_epoch.store(handle.epoch, Ordering::SeqCst);
        let mut inner = self.inner.lock().unwrap();
        inner.persist = Some(PersistSlot {
            ckpt_gap: persister.config().checkpoint_every_commits,
            handle,
            persister,
        });
    }

    /// T36: whether this engine is already fenced (lease lost or persistence failed).
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }
    /// T36: mark fenced (all later reads and writes are rejected). The swap ensures the same engine
    /// fencing multiple times counts only +1 (paired with the -1 in Drop, so the gauge does not leak).
    fn fence(&self, epoch: u64) -> EngineError {
        if !self.fenced.swap(true, Ordering::SeqCst) {
            crate::metrics::set_fenced_dbs_delta(1);
        }
        EngineError::Fenced(epoch)
    }
    /// T36b: graceful shutdown -- CAS `expires_at=now` under the inner lock (epoch/
    /// holder/lsn unchanged), then force fence: this engine must not write again
    /// (even if release did not complete due to a store error -- fencing is the baseline).
    /// Called by `EngineRegistry::release_leases`.
    pub fn release_lease(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let epoch = self.lease_epoch();
        // Best effort: persist the timeline tail before the next writer takes over.
        self.flush_timeline(&mut inner);
        if let Some(slot) = inner.persist.as_mut() {
            let p = slot.persister.clone();
            let now = p.clock().now_ms();
            match p.release(&self.database_id, &mut slot.handle, now) {
                Ok(()) => {
                    tracing::info!(db = %self.database_id, epoch, "lease released");
                }
                Err(e) => {
                    tracing::warn!(db = %self.database_id, epoch, error = %e,
                        "lease release failed (next holder waits out the lease)");
                }
            }
        }
        let _ = self.fence(epoch);
    }

    /// T36: degraded (renewal failed but lease valid) observation hook.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    /// T36: lease gate (section 4.3) -- usable only if `expiry-margin > now`;
    /// renew when due; renewal failure but not yet expired -> degraded (reads allowed, writes proceed,
    /// publish has its own CAS backstop); already unusable -> renew once, fenced on failure.
    /// Precondition: holds the inner lock.
    /// RV-R71: take the single-writer lock but give up at `deadline`
    /// (poll `try_lock`; std `Mutex` has no timed lock). A hung
    /// persistence call holding the lock can therefore pin at most the
    /// owner, never every queued request's blocking thread.
    fn lock_inner(
        &self,
        deadline: Instant,
    ) -> Result<std::sync::MutexGuard<'_, EngineInner>, EngineError> {
        let mut pause = std::time::Duration::from_micros(200);
        loop {
            match self.inner.try_lock() {
                Ok(g) => return Ok(g),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(EngineError::Message("engine lock poisoned".into()))
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let now = Instant::now();
                    if now >= deadline {
                        crate::metrics::persistence_failure("lock_wait_timeout");
                        return Err(EngineError::DeadlineExceeded);
                    }
                    std::thread::sleep(pause.min(deadline - now));
                    pause = (pause * 2).min(std::time::Duration::from_millis(5));
                }
            }
        }
    }

    fn ensure_lease(&self, inner: &mut EngineInner, deadline: Instant) -> Result<(), EngineError> {
        let Some(slot) = inner.persist.as_mut() else {
            return Ok(());
        };
        let p = slot.persister.clone();
        let cfg = p.config().clone();
        let now = p.clock().now_ms();
        let epoch = slot.handle.epoch;
        if slot.handle.usable(now, cfg.clock_skew_margin_ms) {
            if slot
                .handle
                .due_renew(now, cfg.clock_skew_margin_ms, cfg.lease_ms)
            {
                match p.renew_until(&self.database_id, &mut slot.handle, Some(deadline)) {
                    Ok(()) => {
                        self.degraded.store(false, Ordering::SeqCst);
                    }
                    Err(crate::persist::PersistError::Fenced) => {
                        return Err(self.fence(epoch));
                    }
                    Err(e) => {
                        // Renewal failed but lease valid -> degraded.
                        slot.handle.degraded = true;
                        self.degraded.store(true, Ordering::SeqCst);
                        crate::metrics::lease_renew_failure("store");
                        tracing::warn!(db = %self.database_id, error = %e, "lease renew failed (degraded)");
                    }
                }
            }
            return Ok(());
        }
        // Lease unusable -> renewal is the only self-rescue; any failure fences.
        match p.renew_until(&self.database_id, &mut slot.handle, Some(deadline)) {
            Ok(()) => {
                self.degraded.store(false, Ordering::SeqCst);
                Ok(())
            }
            Err(e) => {
                crate::metrics::lease_renew_failure("expired");
                tracing::warn!(db = %self.database_id, error = %e, "lease renew failed (fencing)");
                Err(self.fence(epoch))
            }
        }
    }

    /// T36 section 4.2 option 1: the WAL frames produced by this commit -> vrt-seg v1 -> synchronous
    /// upload -> CAS manifest lsn -> return the new lsn. Read-only transactions return None.
    /// Precondition: tx is committed and the inner lock is held (single-writer critical section).
    ///
    /// RV-R31 #1: the capture upper bound = the last **commit frame** in the verified chain
    /// (`commit_db_size!=0`), not the physical end of file. Rolled-back spill dead frames
    /// remain after mxFrame, and their checksum is still a legitimate continuation of this chain -- chain verification
    /// cannot exclude them, only the commit boundary can; conversely a new commit may also fall entirely within the old
    /// file length (zero physical growth; the old `len==0` read-only check -> dropped confirmed
    /// writes). "Are there new frames" is judged by the verified frame count, not the file length.
    ///
    /// Generation anchoring: when `wal_frames>0` the header salt must equal the seeded
    /// generation, otherwise the WAL was rebuilt outside the lock -> fail closed; watermark 0 seeds from the current
    /// header (the correct starting point for the first write after open/TRUNCATE). If not even one frame
    /// can be verified at the watermark while the file does have tail bytes = bookkeeping diverged from reality ->
    /// fail closed.
    ///
    /// Do not read mxFrame via `PRAGMA wal_checkpoint` -- after a PASSIVE checkpoint
    /// completes SQLite resets the WAL (mxFrame goes to 0), invalidating the watermark's self-proof.
    fn persist_commit(
        &self,
        inner: &mut EngineInner,
        deadline: Instant,
    ) -> Result<Option<u64>, EngineError> {
        let slot_epoch = inner.persist.as_ref().map(|s| s.handle.epoch).unwrap_or(0);
        let prev_lsn = inner.persist.as_ref().map(|s| s.handle.lsn).unwrap_or(0);
        let ps = inner.page_size;
        let wal_path = format!("{}-wal", self.db_file.display());
        let wal_len = match std::fs::metadata(&wal_path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(EngineError::Message(format!("wal stat: {e}"))),
        };
        if wal_len < crate::wal::WAL_HDR as u64 {
            if wal_len == 0 && inner.wal_frames == 0 {
                // After TRUNCATE the wal has not been rebuilt yet (read-only batch) -- no new frames.
                return Ok(None);
            }
            // Watermark non-zero but the file is gone/torn -> outside interference, fail closed.
            return Err(EngineError::Message("wal header torn".into()));
        }
        let f = std::fs::File::open(&wal_path)
            .map_err(|e| EngineError::Message(format!("wal open: {e}")))?;
        use std::os::unix::fs::FileExt;
        let mut hbuf = [0u8; crate::wal::WAL_HDR];
        f.read_exact_at(&mut hbuf, 0)
            .map_err(|e| EngineError::Message(format!("wal header read: {e}")))?;
        let Some(hdr) = crate::wal::wal_header(&hbuf) else {
            return Err(EngineError::Message("wal header invalid".into()));
        };
        if hdr.page_size() != ps {
            return Err(EngineError::Message(format!(
                "wal page_size drift: {} != {ps}",
                hdr.page_size()
            )));
        }
        let (salt, cksum_seed, be) = if inner.wal_frames == 0 {
            (
                (hdr.salt1, hdr.salt2),
                (
                    u32::from_be_bytes(hbuf[24..28].try_into().unwrap()),
                    u32::from_be_bytes(hbuf[28..32].try_into().unwrap()),
                ),
                hdr.cksum_be(),
            )
        } else {
            let Some(s0) = inner.wal_salt else {
                return Err(EngineError::Message("wal salt not seeded".into()));
            };
            if s0 != (hdr.salt1, hdr.salt2) {
                return Err(EngineError::Message(
                    "wal generation changed under live watermark".into(),
                ));
            }
            (s0, inner.wal_cksum, inner.wal_be)
        };
        let fsz = (crate::wal::WAL_FRAME_HDR + ps as usize) as u64;
        let start = crate::wal::WAL_HDR as u64 + inner.wal_frames * fsz;
        if wal_len < start {
            return Err(EngineError::Message("wal watermark regressed".into()));
        }
        if wal_len == start {
            return Ok(None); // physical tail exactly at the watermark -> no new bytes
        }
        let len = wal_len - start;
        let mut buf = vec![0u8; len as usize];
        f.read_exact_at(&mut buf, start)
            .map_err(|e| EngineError::Message(format!("wal read: {e}")))?;
        // Verify salt + cumulative checksum frame by frame (to keep torn/other-generation frames out of the segment);
        // capture upper bound = the last commit frame in the chain -- the legitimate continuation frames after it are
        // rolled-back spill dead frames; they are neither encoded nor counted into the watermark (SQLite's next write overwrites
        // that region, so the watermark is not polluted by dead frames).
        let (_vend, heads, _ck) = crate::wal::scan_frames(&buf, 0, ps, be, salt, cksum_seed);
        if heads.is_empty() {
            return Err(EngineError::Message(
                "wal unreadable at capture watermark".into(),
            ));
        }
        let Some(last_commit) = heads.iter().rposition(|h| h.commit_db_size != 0) else {
            // Only non-commit frames on the chain = a pure dead-frame tail -- no new commit.
            return Ok(None);
        };
        let take = last_commit + 1;
        let frames_before = inner.wal_frames;
        let frames: Vec<crate::seg::SegFrame> = heads[..take]
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let off = i * fsz as usize + crate::wal::WAL_FRAME_HDR;
                crate::seg::SegFrame {
                    page_no: h.page_no,
                    commit_db_size: h.commit_db_size,
                    data: buf[off..off + ps as usize].to_vec(),
                }
            })
            .collect();
        inner.wal_frames += take as u64;
        inner.wal_salt = Some(salt);
        inner.wal_cksum = heads[last_commit].cksum;
        inner.wal_be = be;
        let new_lsn = prev_lsn + 1;
        let segment = crate::seg::Segment {
            database_id: self.database_id.clone(),
            epoch: slot_epoch,
            lsn: new_lsn,
            prev_lsn,
            page_size: ps,
            wal_begin: frames_before,
            frames,
        };
        let bytes = crate::seg::encode(&segment);
        // The sidecar records the WAL physical frame count (including the dead-frame tail) -- the baseline for
        // the local-file integrity check at claim time (truncation / junk bytes both deviate from it).
        let file_frames = (wal_len - crate::wal::WAL_HDR as u64) / fsz;
        let slot = inner.persist.as_mut().expect("persist slot");
        let p = slot.persister.clone();
        match p.publish_until(
            &self.database_id,
            &bytes,
            new_lsn,
            file_frames,
            &mut slot.handle,
            Some(deadline),
        ) {
            Ok(()) => {
                self.commit_lsn.store(new_lsn, Ordering::SeqCst);
                self.degraded.store(slot.handle.degraded, Ordering::SeqCst);
                inner.timeline.push(crate::timeline::TimelineEntry {
                    lsn: new_lsn,
                    epoch: slot_epoch,
                    committed_at_ms: slot.persister.clock().now_ms(),
                });
            }
            Err(e) => {
                crate::metrics::persistence_failure("publish");
                tracing::warn!(db = %self.database_id, error = %e, "publish failed; fencing writer");
                // RV-R71: always fence (fail closed — the in-flight put /
                // CAS may or may not land); but a deadline cut is reported
                // as an UNKNOWN outcome, not a plain fence.
                let fenced = self.fence(slot_epoch);
                if crate::persist::is_deadline(&e) {
                    crate::metrics::persistence_failure("publish_deadline");
                    return Err(EngineError::ResultUnknown);
                }
                return Err(fenced);
            }
        }
        if inner.timeline.len() >= crate::timeline::FLUSH_COMMITS {
            self.flush_timeline(inner);
        }
        // Periodic checkpoint: TRUNCATE wal + upload snapshot + GC old segments.
        // T36b: trigger on the dynamic `ckpt_gap` (exponential backoff on failure) rather than the fixed
        // `every` -- over-limit/flapping dbs do not retry a whole-db upload on every commit.
        let every = p.config().checkpoint_every_commits;
        let gap_due = every > 0
            && inner
                .persist
                .as_ref()
                .map(|s| s.handle.commits_since_ckpt >= s.ckpt_gap)
                .unwrap_or(false);
        if gap_due {
            if let Err(e) = self.checkpoint_now(inner) {
                // Checkpoint failure is not fatal (the segment chain is still complete); lease loss -> fenced.
                let (reason, is_cap) = match &e {
                    EngineError::Message(m) if m.starts_with("checkpoint: snapshot over cap") => {
                        ("snapshot_over_cap", true)
                    }
                    _ => ("checkpoint", false),
                };
                crate::metrics::persistence_failure(reason);
                tracing::warn!(db = %self.database_id, error = %e, "checkpoint failed");
                if is_cap {
                    self.snapshot_over_cap.store(true, Ordering::SeqCst);
                }
                if matches!(e, EngineError::Fenced(_)) {
                    return Err(e);
                }
                // Backoff: gap doubles, capped at every*64 -- this failure already consumed this
                // commits_since_ckpt trigger bit; the next trigger comes after +gap commits.
                if let Some(slot) = inner.persist.as_mut() {
                    slot.handle.commits_since_ckpt = 0;
                    slot.ckpt_gap =
                        (slot.ckpt_gap.saturating_mul(2)).min(every.saturating_mul(64).max(every));
                }
            } else if let Some(slot) = inner.persist.as_mut() {
                slot.ckpt_gap = every;
                self.snapshot_over_cap.store(false, Ordering::SeqCst);
            }
        }
        Ok(Some(new_lsn))
    }

    /// T36: TRUNCATE checkpoint + snapshot upload (under the inner lock).
    fn checkpoint_now(&self, inner: &mut EngineInner) -> Result<(), EngineError> {
        let Some(slot) = inner.persist.as_mut() else {
            return Ok(());
        };
        let p = slot.persister.clone();
        let epoch = slot.handle.epoch;
        inner.internal.clone().set(true);
        let ck = inner
            .conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                r.get::<_, i64>(0)
            });
        inner.internal.clone().set(false);
        match ck {
            Ok(0) => {
                // TRUNCATE resets mxFrame and switches the WAL generation (salt1++) --
                // the watermark is zeroed and the generation anchor is cleared to 0 as well; the next capture
                // reseeds from the new header.
                inner.wal_frames = 0;
                inner.wal_salt = None;
                inner.wal_cksum = (0, 0);
            }
            Ok(_) => {
                // busy: there are un-checkpointed frames -- skip this time (retry next time).
                return Ok(());
            }
            Err(e) => return Err(EngineError::Sqlite(e)),
        }
        match p.checkpoint(
            &self.database_id,
            &mut inner.persist.as_mut().unwrap().handle,
            &self.db_file,
        ) {
            Ok(()) => {
                self.flush_timeline(inner);
                Ok(())
            }
            Err(crate::persist::PersistError::Fenced) => Err(self.fence(epoch)),
            Err(e) => Err(EngineError::Message(format!("checkpoint: {e}"))),
        }
    }

    /// Persist the buffered Time Travel timeline (best effort: history must never fail a commit).
    fn flush_timeline(&self, inner: &mut EngineInner) {
        if inner.timeline.is_empty() {
            return;
        }
        let Some(slot) = inner.persist.as_ref() else {
            inner.timeline.clear();
            return;
        };
        let p = slot.persister.clone();
        match p.upload_timeline(&self.database_id, &inner.timeline) {
            Ok(()) => inner.timeline.clear(),
            Err(e) => {
                tracing::warn!(db = %self.database_id, error = %e,
                    "timeline flush failed; keeping entries for the next attempt");
                if inner.timeline.len() > crate::timeline::MAX_BUFFERED_ENTRIES {
                    let drop = inner.timeline.len() - crate::timeline::MAX_BUFFERED_ENTRIES;
                    inner.timeline.drain(..drop);
                }
            }
        }
    }

    /// Read-only size/shape snapshot for the control-plane stats report.
    /// Uses the engine-internal authorizer bypass (pragma_* functions are
    /// rejected for tenant SQL) and never changes data.
    pub fn stats_sync(&self) -> Result<DbStats, EngineError> {
        if self.dropped.load(Ordering::SeqCst) {
            return Err(EngineError::Dropped);
        }
        if self.fenced.load(Ordering::SeqCst) {
            return Err(EngineError::Fenced(self.lease_epoch()));
        }
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.internal.set(true);
        let out = (|| -> rusqlite::Result<DbStats> {
            let page_count: i64 =
                inner
                    .conn
                    .query_row("SELECT page_count FROM pragma_page_count()", [], |r| {
                        r.get(0)
                    })?;
            let page_size: i64 =
                inner
                    .conn
                    .query_row("SELECT page_size FROM pragma_page_size()", [], |r| {
                        r.get(0)
                    })?;
            let user_version: i64 = inner
                .conn
                .query_row("SELECT user_version FROM pragma_user_version()", [], |r| {
                    r.get(0)
                })?;
            let tables: Vec<String> = {
                let mut stmt = inner.conn.prepare(
                    "select name from sqlite_master
                      where type='table'
                        and name not like 'sqlite_%'
                        and name not like '\\_vrt\\_%' escape '\\'
                      order by name",
                )?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
                rows.filter_map(|r| r.ok()).collect()
            };
            let index_count: i64 = inner.conn.query_row(
                "select count(*) from sqlite_master
                  where type='index' and name not like 'sqlite_%'",
                [],
                |r| r.get(0),
            )?;
            Ok(DbStats {
                database_bytes: page_count.saturating_mul(page_size),
                page_count,
                page_size,
                table_count: tables.len() as i64,
                tables,
                index_count,
                user_version,
            })
        })();
        inner.internal.set(false);
        out.map_err(EngineError::Sqlite)
    }

    /// size_after: pragma_*() functions are rejected by the authorizer; go through the internal bypass.
    fn size_after(conn: &Connection, internal: &security::InternalFlag) -> i64 {
        internal.set(true);
        let r = conn
            .query_row(
                "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        internal.set(false);
        r
    }

    /// Execute the whole batch in a single transaction; any failure rolls back the whole batch and only the first error is returned (section 3).
    /// `format` affects only the shape of results, not execution semantics.
    pub fn query_batch_sync(
        &self,
        batch: &[SqlStmt],
        ctx: &TxnCtx,
        format: ResultsFormat,
    ) -> Result<Vec<QueryResult>, EngineError> {
        // T35c: drop has already removed this engine -> reject directly (prevents "write succeeded but directory already deleted").
        if self.dropped.load(Ordering::SeqCst) {
            return Err(EngineError::Dropped);
        }
        // T36: a fenced engine rejects all reads and writes (recovery is rebuilt by the registry).
        if self.fenced.load(Ordering::SeqCst) {
            return Err(EngineError::Fenced(self.lease_epoch.load(Ordering::SeqCst)));
        }
        let mut inner = self.lock_inner(ctx.deadline)?;
        // T36: lease gate (section 4.3) -- reads and writes use the same decision.
        self.ensure_lease(&mut inner, ctx.deadline)?;
        let internal = inner.internal.clone();
        // T36: take the intent lsn before the tx is created (while tx holds a mutable borrow of inner.conn,
        // inner cannot be accessed again).
        let persist_intent = inner
            .persist
            .as_ref()
            .map(|s| (s.persister.clone(), s.handle.lsn + 1));

        // Same filter semantics as miniflare
        let stmts: Vec<&SqlStmt> = batch.iter().filter(|q| is_nonempty_sql(&q.sql)).collect();
        if stmts.is_empty() {
            return Err(EngineError::Message("No SQL statements detected.".into()));
        }

        // Dedup hit -> no replay (the ledger commits in the same transaction inside the db, see section 3 R4;
        // _vrt_dedup is unreadable by users, the engine goes through the internal bypass)
        // S4: the dedup epoch must equal the current writer epoch -- replaying a seq
        // across epochs could commit an old position's result as a new position's, fail closed.
        if let Some((depoch, _)) = ctx.dedup {
            if depoch != ctx.epoch {
                return Err(EngineError::Message(format!(
                    "dedup epoch {depoch} != writer epoch {}",
                    ctx.epoch
                )));
            }
        }
        // T34b: the dedup key validates the payload with idempotency-key semantics -- same
        // (epoch,seq) but different payload => seq allocation collision (provider restart wraparound,
        // etc.), fail closed rather than replay the old result. Discriminated byte encoding avoids serialization ambiguity.
        let req_sha = ctx.dedup.map(|_| req_digest(&stmts));
        if let Some((depoch, dseq)) = ctx.dedup {
            internal.set(true);
            let hit = inner.conn.query_row(
                "SELECT result_json, req_sha FROM _vrt_dedup WHERE epoch=?1 AND seq=?2",
                (depoch as i64, dseq as i64),
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            );
            internal.set(false);
            if let Ok((json, stored_sha)) = hit {
                if stored_sha != *req_sha.as_ref().unwrap() {
                    return Err(EngineError::Message(
                        "dedup key collision: (epoch,seq) reused with different payload".into(),
                    ));
                }
                return serde_json::from_str::<Vec<QueryResult>>(&json)
                    .map_err(|e| EngineError::Message(e.to_string()));
            }
        }

        let tx = inner.conn.transaction().map_err(EngineError::Sqlite)?;
        let deadline = ctx.deadline;

        // Per-statement timeout: the progress handler periodically checks the deadline (section 3 R5)
        security::arm_deadline(&tx, deadline);

        let mut out = Vec::with_capacity(stmts.len());
        let mut failed: Option<EngineError> = None;

        for q in &stmts {
            if Instant::now() >= deadline {
                failed = Some(EngineError::Timeout);
                break;
            }
            // Text pre-check for `VACUUM`/`VACUUM INTO` statements (the authorizer has no VACUUM
            // action code; a bare VACUUM's internal Attach{''} is indistinguishable from a user ATTACH)
            if security::looks_like_vacuum(&q.sql) {
                failed = Some(EngineError::Message(
                    "not authorized: VACUUM is disabled".into(),
                ));
                break;
            }
            let t0 = Instant::now();
            let before_changes = tx.total_changes() as i64;
            let before_size = Self::size_after(&tx, &internal);
            let before_rowid = tx.last_insert_rowid();

            let run = (|| -> Result<(serde_json::Value, i64), EngineError> {
                let mut st = tx.prepare(&q.sql).map_err(EngineError::Sqlite)?;
                let cols: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
                let params: Vec<Value> = q
                    .params
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(SqlValue::to_rusqlite)
                    .collect();
                let mut rows_cur = st
                    .query(params_from_iter(params.iter()))
                    .map_err(EngineError::Sqlite)?;
                // D1's documented 50 MB cap is per statement result. Count
                // the serialized shape while rows are produced so a huge
                // result fails closed before the whole set is materialized.
                let mut result_bytes: usize = match format {
                    ResultsFormat::RowsAndColumns => serde_json::to_vec(&cols)
                        .map_err(|e| EngineError::Message(e.to_string()))?
                        .len()
                        .saturating_add(32),
                    _ => 2,
                };
                let mut rows: Vec<serde_json::Value> = Vec::new();
                while let Some(row) = rows_cur.next().map_err(EngineError::Sqlite)? {
                    let mut r = Vec::with_capacity(cols.len());
                    for i in 0..cols.len() {
                        r.push(value_to_json(row.get_ref(i)?)?);
                    }
                    let row_value = format_row(format, &cols, r);
                    result_bytes = result_bytes.saturating_add(
                        serde_json::to_vec(&row_value)
                            .map_err(|e| EngineError::Message(e.to_string()))?
                            .len()
                            .saturating_add(1),
                    );
                    if result_bytes > D1_RESULT_MAX_BYTES {
                        return Err(EngineError::Message(D1_RESULT_TOO_LARGE.into()));
                    }
                    rows.push(row_value);
                }
                let nrows = rows.len() as i64;
                let results = match format {
                    ResultsFormat::RowsAndColumns => {
                        serde_json::json!({ "columns": cols, "rows": rows })
                    }
                    _ => serde_json::Value::Array(rows),
                };
                Ok((results, nrows))
            })();

            match run {
                Ok((results, nrows)) => {
                    let after_changes = tx.total_changes() as i64;
                    let after_size = Self::size_after(&tx, &internal);
                    let after_rowid = tx.last_insert_rowid();
                    let changes = after_changes - before_changes;
                    out.push(QueryResult {
                        success: true,
                        results,
                        meta: Meta {
                            served_by: self.served_by.clone(),
                            duration: t0.elapsed().as_secs_f64() * 1000.0,
                            changes,
                            last_row_id: after_rowid,
                            changed_db: changes != 0
                                || after_rowid != before_rowid
                                || after_size != before_size,
                            size_after: after_size,
                            rows_read: nrows,
                            rows_written: changes,
                        },
                    });
                }
                Err(e) => {
                    failed = Some(self.normalize_error(e));
                    break;
                }
            }
        }

        let result = match failed {
            Some(e) => {
                drop(tx); // roll back the whole batch (including dedup rows -- the ledger of a failed business op is not persisted)
                Err(e)
            }
            None => {
                // The dedup ledger commits in the same transaction as the business writes
                if let Some((depoch, dseq)) = ctx.dedup {
                    let result_json = serde_json::to_string(&out)
                        .map_err(|e| EngineError::Message(e.to_string()))?;
                    internal.set(true);
                    let r = tx.execute(
                        "INSERT INTO _vrt_dedup(epoch,seq,result_json,req_sha,committed_at) VALUES(?,?,?,?,?)",
                        (
                            depoch as i64,
                            dseq as i64,
                            result_json,
                            req_sha.clone().unwrap_or_default(),
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs() as i64,
                        ),
                    );
                    internal.set(false);
                    r.map_err(EngineError::Sqlite)?;
                }
                // T35c: final check before commit -- if dropped during batch execution, roll back and report an error,
                // so writes never land on a file already renamed into .trash and still return success.
                if self.dropped.load(Ordering::SeqCst) {
                    drop(tx);
                    return Err(EngineError::Dropped);
                }
                // T36: write the intent lsn before commit (prevents "committed locally but not acked
                // being trusted" -- an unpersisted commit in the crash window would leave the sidecar ahead of
                // manifest.lsn; claim forces recovery to clear it).
                if let Some((p, lsn)) = &persist_intent {
                    if p.mark_intent(&self.database_id, *lsn).is_err() {
                        // RV-R31 #2: if the intent marker cannot be written to disk = the local persistence layer
                        // is untrustworthy -- roll back this batch and refuse to ack (fail closed).
                        drop(tx);
                        return Err(EngineError::Unavailable("local intent marker write failed"));
                    }
                }
                tx.commit().map_err(EngineError::Sqlite)?;
                mark_committed();
                // T36 section 4.2: with persistence -> segment upload + manifest CAS before ack
                // (serial under the lock; failure fences and does not ack); without persistence the old semantics are kept.
                if inner.persist.is_some() {
                    // the conn borrow was released along with tx -- call directly on inner.
                    match self.persist_commit(&mut inner, ctx.deadline) {
                        Ok(_) => Ok(out),
                        Err(e) => Err(e),
                    }
                } else if self.read_only {
                    // A read transaction commits as a no-op; the replica watermark does not move.
                    Ok(out)
                } else {
                    self.commit_lsn.fetch_add(1, Ordering::SeqCst);
                    Ok(out)
                }
            }
        };
        security::disarm_deadline(&mut inner.conn);
        result
    }

    /// Map a statement error on a read-only engine to a typed routing signal.
    fn normalize_error(&self, e: EngineError) -> EngineError {
        if self.read_only {
            if let EngineError::Sqlite(ref sqlite) = e {
                if sqlite.sqlite_error_code() == Some(rusqlite::ErrorCode::ReadOnly) {
                    return EngineError::ReadOnly;
                }
            }
        }
        e
    }

    /// `dump()`: export the user database as a SQLite-compatible SQL
    /// script. Read-only: it takes the same dropped/fenced/lease gates as
    /// a query but opens a deferred (snapshot) transaction and never
    /// commits, so no lsn advances and no dedup row is written.
    pub fn dump_sync(&self, ctx: &TxnCtx) -> Result<Vec<u8>, EngineError> {
        if self.dropped.load(Ordering::SeqCst) {
            return Err(EngineError::Dropped);
        }
        if self.fenced.load(Ordering::SeqCst) {
            return Err(EngineError::Fenced(self.lease_epoch.load(Ordering::SeqCst)));
        }
        let mut inner = self.lock_inner(ctx.deadline)?;
        self.ensure_lease(&mut inner, ctx.deadline)?;
        let tx = inner
            .conn
            .unchecked_transaction()
            .map_err(EngineError::Sqlite)?;
        security::arm_deadline(&tx, ctx.deadline);
        let result = dump_sql(&tx, ctx.deadline);
        drop(tx); // read-only: always rolled back
        security::disarm_deadline(&mut inner.conn);
        // T35c: a concurrent DROP while the export ran must not return a
        // dump of a database the control plane already removed.
        if self.dropped.load(Ordering::SeqCst) {
            return Err(EngineError::Dropped);
        }
        result
    }

    /// T37: engine-internal transaction -- the only commit path for non-user-SQL surfaces
    /// such as QueueBackend (pub(crate): unreachable from the tenant `/query` path). Shares all invariants with
    /// `query_batch_sync`: dropped/fenced gate, lease
    /// gate, deadline progress handle, final dropped check before commit, intent lsn,
    /// commit -> persist_commit persistence before ack; the whole thing runs under the InternalFlag bypass
    /// (the `_vrt_q_*` internal tables are reachable), with no dedup ledger and no result-set materialization.
    ///
    /// `f` runs synchronously and must not cross an await (this method itself holds the lock synchronously);
    /// the `&Transaction` borrow does not escape the closure (T is separate from the handle type, so a
    /// handle cannot be returned); f Err -> whole batch rolled back, lsn not advanced, no persist.
    /// InternalFlag is reset via an RAII guard -- reset on f panic too,
    /// and the panic is caught by catch_unwind and collected as Err (does not poison the inner lock).
    pub(crate) fn exec_internal<T>(
        &self,
        ctx: &TxnCtx,
        f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        if self.dropped.load(Ordering::SeqCst) {
            return Err(EngineError::Dropped);
        }
        if self.fenced.load(Ordering::SeqCst) {
            return Err(EngineError::Fenced(self.lease_epoch.load(Ordering::SeqCst)));
        }
        let mut inner = self.lock_inner(ctx.deadline)?;
        self.ensure_lease(&mut inner, ctx.deadline)?;
        let internal = inner.internal.clone();
        let persist_intent = inner
            .persist
            .as_ref()
            .map(|s| (s.persister.clone(), s.handle.lsn + 1));

        let tx = inner.conn.transaction().map_err(EngineError::Sqlite)?;
        security::arm_deadline(&tx, ctx.deadline);
        // Drop guard: the bypass flag is reset whether f returns early / Errs / panics.
        struct FlagReset(security::InternalFlag);
        impl Drop for FlagReset {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        internal.set(true);
        let _guard = FlagReset(internal);
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&tx)));
        let result = match out {
            Err(_panic) => {
                drop(tx);
                Err(EngineError::Message("internal op panicked".into()))
            }
            Ok(Err(e)) => {
                drop(tx);
                Err(e)
            }
            Ok(Ok(v)) => {
                if self.dropped.load(Ordering::SeqCst) {
                    drop(tx);
                    return Err(EngineError::Dropped);
                }
                if let Some((p, lsn)) = &persist_intent {
                    if p.mark_intent(&self.database_id, *lsn).is_err() {
                        // RV-R31 #2: same as query_batch_sync -- marker failure
                        // means the local persistence layer is untrustworthy; roll back and refuse to ack.
                        drop(tx);
                        return Err(EngineError::Unavailable("local intent marker write failed"));
                    }
                }
                match tx.commit() {
                    Err(e) => Err(EngineError::Sqlite(e)),
                    Ok(()) => {
                        mark_committed();
                        if inner.persist.is_some() {
                            match self.persist_commit(&mut inner, ctx.deadline) {
                                Ok(_) => Ok(v),
                                Err(e) => Err(e),
                            }
                        } else {
                            self.commit_lsn.fetch_add(1, Ordering::SeqCst);
                            Ok(v)
                        }
                    }
                }
            }
        };
        drop(_guard);
        security::disarm_deadline(&mut inner.conn);
        result
    }

    /// Current commit position (the source of the bookmark's lsn).
    pub fn commit_lsn(&self) -> u64 {
        self.commit_lsn.load(Ordering::SeqCst)
    }

    /// T36: writer lease epoch (the epoch component of bookmarks).
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch.load(Ordering::SeqCst)
    }

    /// T36b: segment chain length (lsn - base snapshot lsn); no persist -> None.
    pub fn chain_len(&self) -> Option<u64> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.persist.as_ref().map(|s| {
            s.handle
                .lsn
                .saturating_sub(s.handle.snapshot.as_ref().map(|x| x.lsn).unwrap_or(0))
        })
    }

    /// T36: whether this engine has persistence attached.
    pub fn persistence_enabled(&self) -> bool {
        self.inner.lock().unwrap().persist.is_some()
    }
}

#[async_trait::async_trait]
impl SqlEngine for RusqliteEngine {
    async fn query_batch(
        &self,
        batch: &[SqlStmt],
        ctx: &TxnCtx,
    ) -> Result<Vec<QueryResult>, EngineError> {
        self.query_batch_sync(batch, ctx, ResultsFormat::ArrayOfObjects)
    }

    async fn bookmark(&self) -> Result<u64, EngineError> {
        Ok(self.commit_lsn())
    }

    async fn checkpoint(&self) -> Result<LtxSegment, EngineError> {
        Err(EngineError::Message(
            "P3a: LTX replication not implemented".into(),
        ))
    }

    async fn restore(&self, _snap: &Snapshot) -> Result<(), EngineError> {
        Err(EngineError::Message(
            "P3a: snapshot restore not implemented".into(),
        ))
    }

    fn size_bytes(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        let internal = inner.internal.clone();
        Self::size_after(&inner.conn, &internal) as u64
    }
}

/// Build one result row in the wire's result shape. Counting the
/// serialized row is what enforces [`D1_RESULT_MAX_BYTES`].
fn format_row(
    format: ResultsFormat,
    cols: &[String],
    values: Vec<serde_json::Value>,
) -> serde_json::Value {
    match format {
        ResultsFormat::RowsAndColumns => serde_json::Value::Array(values),
        _ => {
            let mut obj = serde_json::Map::with_capacity(cols.len());
            for (name, value) in cols.iter().zip(values) {
                obj.insert(name.clone(), value);
            }
            serde_json::Value::Object(obj)
        }
    }
}

/// `dump()`: a SQLite-compatible SQL script for the whole database.
///
/// The statement order, quoting and escaping follow workerd's own D1
/// export (`src/workerd/server/d1/d1.js`, ported from workers-sdk's
/// `dumpSql`), whose golden vectors live in
/// `src/workerd/server/tests/d1/d1-export-test.js`. Engine-internal
/// tables (`_vrt_*`) are skipped: they are not part of the user
/// database and must never leak through an export.
fn dump_sql(conn: &Connection, deadline: Instant) -> Result<Vec<u8>, EngineError> {
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(b"PRAGMA defer_foreign_keys=TRUE;\n");
    let tables: Vec<(String, String)> = {
        let mut st = conn
            .prepare(
                "SELECT name, sql FROM sqlite_schema AS o \
                 WHERE (true) AND type=='table' AND sql NOT NULL \
                 ORDER BY tbl_name='sqlite_sequence', rowid",
            )
            .map_err(EngineError::Sqlite)?;
        let rows = st
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(EngineError::Sqlite)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(EngineError::Sqlite)?
    };
    for (table, sql) in tables {
        check_dump_deadline(deadline)?;
        if table == "sqlite_sequence" {
            out.extend_from_slice(b"DELETE FROM sqlite_sequence;\n");
        } else if table.starts_with("sqlite_stat") {
            // As SQLite's shell dump: before the table's contents.
            out.extend_from_slice(b"ANALYZE sqlite_schema;\n");
        } else if sql.starts_with("CREATE VIRTUAL TABLE") {
            return Err(EngineError::Message(
                "D1 Export error: cannot export databases with Virtual Tables (fts5)".into(),
            ));
        } else if table.starts_with("_cf_")
            || table.starts_with("sqlite_")
            || table.starts_with("_vrt_")
        {
            continue;
        } else if sql.starts_with("CREATE TABLE \"") || sql.starts_with("CREATE TABLE '") {
            // As SQLite's dump: a quoted table name gets IF NOT EXISTS.
            out.extend_from_slice(b"CREATE TABLE IF NOT EXISTS ");
            out.extend_from_slice(sql[13..].as_bytes());
            out.push(b';');
            out.push(b'\n');
        } else {
            out.extend_from_slice(sql.as_bytes());
            out.push(b';');
            out.push(b'\n');
        }
        if out.len() > D1_DUMP_MAX_BYTES {
            return Err(EngineError::Message(D1_DUMP_TOO_LARGE.into()));
        }

        let cols: Vec<String> = {
            let mut st = conn
                .prepare(&format!("PRAGMA table_info={}", dump_escape_id(&table)))
                .map_err(EngineError::Sqlite)?;
            let names = st
                .query_map([], |r| r.get::<_, String>(1))
                .map_err(EngineError::Sqlite)?;
            names
                .collect::<Result<Vec<_>, _>>()
                .map_err(EngineError::Sqlite)?
        };
        let names = cols
            .iter()
            .map(|c| dump_escape_id(c))
            .collect::<Vec<_>>()
            .join(",");
        let select = format!("SELECT {names} FROM {};", dump_escape_id(&table));
        let mut st = conn.prepare(&select).map_err(EngineError::Sqlite)?;
        let mut it = st.query([]).map_err(EngineError::Sqlite)?;
        while let Some(row) = it.next().map_err(EngineError::Sqlite)? {
            check_dump_deadline(deadline)?;
            let mut cells: Vec<String> = Vec::with_capacity(cols.len());
            for i in 0..cols.len() {
                cells.push(dump_cell(row.get_ref(i).map_err(EngineError::Sqlite)?));
            }
            out.extend_from_slice(
                format!(
                    "INSERT INTO {} ({names}) VALUES({});\n",
                    dump_escape_id(&table),
                    cells.join(",")
                )
                .as_bytes(),
            );
            if out.len() > D1_DUMP_MAX_BYTES {
                return Err(EngineError::Message(D1_DUMP_TOO_LARGE.into()));
            }
        }
    }

    let rest: Vec<String> = {
        let mut st = conn
            .prepare(
                "SELECT sql FROM sqlite_schema AS o WHERE (true) AND sql NOT NULL \
                 AND type IN ('index', 'trigger', 'view') ORDER BY type COLLATE NOCASE",
            )
            .map_err(EngineError::Sqlite)?;
        let rows = st
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(EngineError::Sqlite)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(EngineError::Sqlite)?
    };
    for sql in rest {
        check_dump_deadline(deadline)?;
        out.extend_from_slice(sql.as_bytes());
        out.push(b';');
        out.push(b'\n');
        if out.len() > D1_DUMP_MAX_BYTES {
            return Err(EngineError::Message(D1_DUMP_TOO_LARGE.into()));
        }
    }
    // Statements are newline-separated; drop the trailing separator so an
    // empty database dumps exactly the leading pragma statement.
    if out.last() == Some(&b'\n') {
        out.pop();
    }
    Ok(out)
}

fn check_dump_deadline(deadline: Instant) -> Result<(), EngineError> {
    if Instant::now() >= deadline {
        Err(EngineError::Timeout)
    } else {
        Ok(())
    }
}

fn dump_escape_id(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

fn dump_cell(v: rusqlite::types::ValueRef<'_>) -> String {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => "NULL".into(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(f) => dump_real(f),
        // shell.c.in output_quoted_escaped_string: a literal newline/CR
        // cannot be embedded in a dump line, so they are written as
        // `\n`/`\r` and restored with replace(...,char(10|13)).
        ValueRef::Text(t) => dump_quote(&String::from_utf8_lossy(t)),
        ValueRef::Blob(b) => {
            let mut s = String::with_capacity(3 + b.len() * 2);
            s.push_str("X'");
            for byte in b {
                s.push_str(&format!("{byte:02x}"));
            }
            s.push('\'');
            s
        }
    }
}

fn dump_real(f: f64) -> String {
    if f.is_finite() {
        f.to_string()
    } else if f > 0.0 {
        "9e999".into()
    } else {
        "-9e999".into()
    }
}

fn dump_quote(text: &str) -> String {
    let mut lf = false;
    let mut cr = false;
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('\'');
    for ch in text.chars() {
        match ch {
            '\'' => quoted.push_str("''"),
            '\n' => {
                lf = true;
                quoted.push_str("\\n");
            }
            '\r' => {
                cr = true;
                quoted.push_str("\\r");
            }
            _ => quoted.push(ch),
        }
    }
    quoted.push('\'');
    if cr {
        quoted = format!("replace({quoted},'\\r',char(13))");
    }
    if lf {
        quoted = format!("replace({quoted},'\\n',char(10))");
    }
    quoted
}

fn value_to_json(v: rusqlite::types::ValueRef) -> Result<serde_json::Value, EngineError> {
    Ok(match v {
        rusqlite::types::ValueRef::Null => serde_json::Value::Null,
        rusqlite::types::ValueRef::Integer(i) => serde_json::json!(i),
        rusqlite::types::ValueRef::Real(f) => serde_json::json!(f),
        rusqlite::types::ValueRef::Text(t) => serde_json::json!(String::from_utf8_lossy(t)),
        rusqlite::types::ValueRef::Blob(b) => {
            // miniflare convertRows: BLOB -> byte array
            serde_json::json!(b.iter().map(|x| *x).collect::<Vec<u8>>())
        }
    })
}

#[cfg(unix)]
fn lock_file_fd(f: &std::fs::File) -> libc::c_int {
    use std::os::unix::io::AsRawFd;
    f.as_raw_fd()
}

// ---------- Registry: in-process single-writer map + drop tombstone ----------

/// engines and tombstones share one lock: drop_db's marker->tombstone->evict
/// ->flag->rename and get()'s tombstone/marker check complete in the same critical section,
/// so there is no window between "removed" and "deleted" where the db can be reopened (T35c).
/// T37r: Registry key = (kind, databaseId). A D1 db and a Queue
/// db with the same id are two disjoint engine/db/marker namespaces.
pub type EngineKey = (crate::paths::ResourceKind, String);

#[derive(Default)]
struct RegistryState {
    engines: HashMap<EngineKey, Arc<RusqliteEngine>>,
    /// (kind,id) pairs already DROPped within this process; the on-disk `.dropped` marker is the persistent
    /// copy (after restart it is pre-filled by each of the two roots' scan_dropped_ids +
    /// the get() cold-path stat fallback).
    tombstones: HashSet<EngineKey>,
    /// T36: diagnostics view (`/internal/dbs`) -- per-db lease/position/state.
    diag: HashMap<EngineKey, DbDiag>,
    /// T36b: shutting down -- reject new get/claim (prevents the same process re-acquiring after release).
    closing: bool,
    /// RV-R71: single-flight cold opens. One in-progress open per key;
    /// same-key callers wait on the slot (with a deadline) instead of
    /// re-running claim/restore, and no caller holds the global lock
    /// while the object store is being talked to.
    opening: HashMap<EngineKey, Arc<OpenSlot>>,
    /// 2026-11-15: per-db bounded exponential backoff for restore failures (fail-closed cases such as
    /// corrupt snapshot/segment or sha mismatch): (consecutive failure count, next allowed retry time). Within the window
    /// `get` returns 503 directly and no longer re-fetches the snapshot/segments (before the fix every request for the same db
    /// re-downloaded and recomputed everything, 143 tight-loop iterations). Cleared on successful restore.
    restore_backoff: HashMap<EngineKey, (u32, std::time::Instant)>,
}

/// Default base/cap of the restore-failure backoff: 1s, 2s, 4s ... capped at 5min.
pub const RESTORE_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_secs(1);
pub const RESTORE_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(300);

/// RV-R71: how long a same-key waiter will wait for someone else's
/// in-flight cold open before failing closed with a retryable 503.
pub const ENGINE_OPEN_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// RV-R71: cloneable projection of an open failure shared with waiters.
#[derive(Clone, Debug)]
enum SharedOpenErr {
    Dropped,
    Unavailable(&'static str),
    Message(String),
}

impl SharedOpenErr {
    fn from_engine(e: &EngineError) -> Self {
        match e {
            EngineError::Dropped => Self::Dropped,
            EngineError::Unavailable(m) => Self::Unavailable(m),
            other => Self::Message(other.to_string()),
        }
    }
    fn into_engine(self) -> EngineError {
        match self {
            Self::Dropped => EngineError::Dropped,
            Self::Unavailable(m) => EngineError::Unavailable(m),
            Self::Message(m) => EngineError::Message(m),
        }
    }
}

/// RV-R71: the shared result cell of one in-flight cold open.
struct OpenSlot {
    done: Mutex<Option<Result<Arc<RusqliteEngine>, SharedOpenErr>>>,
    cv: std::sync::Condvar,
}

impl OpenSlot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Mutex::new(None),
            cv: std::sync::Condvar::new(),
        })
    }
    fn publish(&self, r: Result<Arc<RusqliteEngine>, SharedOpenErr>) {
        let mut g = self.done.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            *g = Some(r);
        }
        self.cv.notify_all();
    }
    /// Wait for the opener's result until `deadline`.
    fn wait(&self, deadline: std::time::Instant) -> Result<Arc<RusqliteEngine>, EngineError> {
        let mut g = self.done.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(r) = g.as_ref() {
                return r.clone().map_err(SharedOpenErr::into_engine);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(EngineError::Unavailable("database open timed out"));
            }
            let (ng, _) = self
                .cv
                .wait_timeout(g, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            g = ng;
        }
    }
}

/// RV-R71: removes the opening slot and wakes waiters even if the
/// opener panics (a forgotten slot would wedge the key forever).
struct OpenGuard<'a> {
    reg: &'a EngineRegistry,
    key: EngineKey,
    slot: Arc<OpenSlot>,
}

impl Drop for OpenGuard<'_> {
    fn drop(&mut self) {
        let mut st = self.reg.state.lock().unwrap_or_else(|e| e.into_inner());
        if st
            .opening
            .get(&self.key)
            .is_some_and(|s| Arc::ptr_eq(s, &self.slot))
        {
            st.opening.remove(&self.key);
        }
        drop(st);
        // No-op when the opener already published a real result.
        self.slot
            .publish(Err(SharedOpenErr::Unavailable("database open aborted")));
    }
}

/// Read-only size/shape snapshot of one D1 database (control-plane stats).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DbStats {
    pub database_bytes: i64,
    pub page_count: i64,
    pub page_size: i64,
    pub table_count: i64,
    pub tables: Vec<String>,
    pub index_count: i64,
    pub user_version: i64,
}

/// T36: read-only diagnostic entry (JSON camelCase -- the outward shape of `/internal/dbs`).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbDiag {
    /// T37r: resource kind (`/internal/dbs` outputs `"kind":"d1"|"queue"`).
    pub kind: crate::paths::ResourceKind,
    pub database_id: String,
    pub epoch: u64,
    pub lsn: u64,
    pub expires_at_ms: u64,
    pub degraded: bool,
    pub fenced: bool,
    pub dropped: bool,
    /// T36b: the latest checkpoint was rejected for exceeding `snapshotMaxBytes`
    /// (this db's restore cost grows linearly with segment chain length, see section 4.5).
    pub snapshot_over_cap: bool,
    /// T36b: current segment chain length (lsn - snapshot.lsn) -- the number of segments
    /// a restore must replay; zeroed as the snapshot advances.
    pub chain_len: u64,
}

impl Drop for RusqliteEngine {
    fn drop(&mut self) {
        if self.fenced.load(Ordering::SeqCst) {
            crate::metrics::set_fenced_dbs_delta(-1);
        }
    }
}

/// T36: Registry is Clone (shares the same state/db_root; persist is only
/// assembled at startup -- `with_persister` returns a new instance that overrides the registry inside AppState,
/// semantics unchanged).
#[derive(Clone)]
pub struct EngineRegistry {
    state: std::sync::Arc<Mutex<RegistryState>>,
    /// D1 local root (`.../dbs`, T36 layout unchanged).
    db_root: PathBuf,
    /// T37r: Queue local root (`.../queues`, derived as a sibling of db_root).
    queue_root: PathBuf,
    k2_root: PathBuf,
    /// T36/T37r: object-store persistence surface -- one Persister per kind
    /// (object-key prefix and local root each bound), assembled when `[persistence]` != none.
    persist: Option<PersisterPair>,
    restore_backoff_base: std::time::Duration,
    restore_backoff_max: std::time::Duration,
}

/// T37r: Persister pair split by kind -- D1 writes `db/{id}`/`dbs/`,
/// Queue writes `q/{id}`/`queues/`.
/// RV-R53.3: per-round bound on tombstoned-database GC — one round
/// never scans more than this many local markers, so a big backlog
/// can't wedge the periodic task.
pub const GC_TOMBSTONED_MAX_PER_ROUND: usize = 32;
/// RV-R53.3: how often the sweeper runs (constant per spec — the
/// retention bound that matters is already configured per Persister).
pub const GC_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// RV-R53.3: counters from one bounded GC pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneGcReport {
    /// Local `.dropped` markers inspected this round.
    pub scanned: usize,
    /// Databases fully reclaimed this round (objects freed).
    pub reclaimed: usize,
    /// Total objects deleted.
    pub objects_freed: u64,
    /// Tombstones still inside retention or already empty.
    pub retained: usize,
    /// Per-database failures (isolated, retried next round).
    pub failures: usize,
}

#[derive(Clone)]
/// One Persister per resource kind -- D1 writes `db/{id}`/`dbs/`, Queue
/// `q/{id}`/`queues/`, K2 `s/{id}`/`streams/` (structural namespace separation).
pub struct PersisterPair {
    pub d1: std::sync::Arc<crate::persist::Persister>,
    pub queue: std::sync::Arc<crate::persist::Persister>,
    pub k2: std::sync::Arc<crate::persist::Persister>,
}

impl PersisterPair {
    fn for_kind(
        &self,
        kind: crate::paths::ResourceKind,
    ) -> &std::sync::Arc<crate::persist::Persister> {
        match kind {
            crate::paths::ResourceKind::D1 => &self.d1,
            crate::paths::ResourceKind::Queue => &self.queue,
            crate::paths::ResourceKind::K2 => &self.k2,
        }
    }
}

/// Upper bound on waiting for in-flight Arcs to be returned after an engine is removed (several times the default transaction budget;
/// deletion proceeds after a timeout -- flock/fd do not block unlink+rename).
const DROP_DRAIN: std::time::Duration = std::time::Duration::from_secs(10);

impl EngineRegistry {
    pub fn new(db_root: PathBuf) -> Self {
        let queue_root = paths::queue_root_for(&db_root);
        // Startup sweep of .trash (already-tombstoned debris left by a previous crash),
        // and pre-fill the in-memory tombstone set from on-disk .dropped markers -- the two
        // roots are scanned separately, and tombstone keys carry kind (a D1 drop of the same id does not
        // affect Queue).
        paths::sweep_trash(&db_root);
        paths::sweep_trash(&queue_root);
        let k2_root = paths::k2_root_for(&db_root);
        paths::sweep_trash(&k2_root);
        let tombstones: HashSet<EngineKey> = paths::scan_dropped_ids(&db_root)
            .into_iter()
            .map(|id| (crate::paths::ResourceKind::D1, id))
            .chain(
                paths::scan_dropped_ids(&queue_root)
                    .into_iter()
                    .map(|id| (crate::paths::ResourceKind::Queue, id)),
            )
            .chain(
                paths::scan_dropped_ids(&k2_root)
                    .into_iter()
                    .map(|id| (crate::paths::ResourceKind::K2, id)),
            )
            .collect();
        Self {
            state: std::sync::Arc::new(Mutex::new(RegistryState {
                engines: HashMap::new(),
                tombstones,
                diag: HashMap::new(),
                closing: false,
                opening: HashMap::new(),
                restore_backoff: HashMap::new(),
            })),
            db_root,
            queue_root,
            k2_root,
            persist: None,
            restore_backoff_base: RESTORE_BACKOFF_BASE,
            restore_backoff_max: RESTORE_BACKOFF_MAX,
        }
    }

    /// Override the restore-failure backoff parameters (tests and operational tuning).
    pub fn with_restore_backoff(
        mut self,
        base: std::time::Duration,
        max: std::time::Duration,
    ) -> Self {
        self.restore_backoff_base = base;
        self.restore_backoff_max = max;
        self
    }

    /// Record a restore failure and return this backoff duration (base*2^(n-1), capped at max).
    fn note_restore_failure(&self, key: &EngineKey) -> (u32, std::time::Duration) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let n = state
            .restore_backoff
            .get(key)
            .map(|(n, _)| n.saturating_add(1))
            .unwrap_or(1);
        let shift = (n - 1).min(20);
        let wait = self
            .restore_backoff_base
            .saturating_mul(1u32 << shift)
            .min(self.restore_backoff_max);
        state
            .restore_backoff
            .insert(key.clone(), (n, std::time::Instant::now() + wait));
        (n, wait)
    }

    /// T36/T37r: install the persistence surface (called at service startup; not installed for the none backend).
    /// Persisters for both kinds must be installed together -- installing only D1 would make queue
    /// silently take the pure-local path (rejected by the fail-closed principle on persistence gaps).
    pub fn with_persisters(mut self, pair: PersisterPair) -> Self {
        self.persist = Some(pair);
        self
    }

    /// kind -> local root.
    fn root_for(&self, kind: crate::paths::ResourceKind) -> &Path {
        match kind {
            crate::paths::ResourceKind::D1 => &self.db_root,
            crate::paths::ResourceKind::Queue => &self.queue_root,
            crate::paths::ResourceKind::K2 => &self.k2_root,
        }
    }

    /// T36: diagnostics snapshot (read-only, for the auth-protected endpoint).
    pub fn diagnostics(&self) -> Vec<DbDiag> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<DbDiag> = state.diag.values().cloned().collect();
        // T36b: enrich with live engines (degraded/over_cap/chain_len change
        // with commits; the diag map is snapshotted only once at claim time).
        for d in out.iter_mut() {
            if let Some(e) = state.engines.get(&(d.kind, d.database_id.clone())) {
                d.degraded = e.is_degraded();
                d.fenced = d.fenced || e.is_fenced();
                d.snapshot_over_cap = e.snapshot_over_cap.load(Ordering::SeqCst);
                if let Some(cl) = e.chain_len() {
                    d.chain_len = cl;
                }
            }
        }
        out
    }

    /// Get (or lazily open) the write handle for databaseId. Repeated opens of the same (kind,id)
    /// go through the same handle; duplicate opens across processes fail closed via flock; a DROPped
    /// (kind,id) is rejected forever (the other kind is unaffected).
    /// T36: with persistence on, a cold open = manifest claim (lease claim/
    /// takeover/tombstone/held-elsewhere) -> snapshot+segment restore if needed -> install lease.
    /// A fenced engine self-heals via the same cold path after being removed under the lock.
    pub fn get(
        &self,
        kind: crate::paths::ResourceKind,
        database_id: &str,
    ) -> Result<Arc<RusqliteEngine>, EngineError> {
        self.get_until(
            kind,
            database_id,
            std::time::Instant::now() + ENGINE_OPEN_WAIT,
        )
    }

    /// RV-R71: single-flight cold open. The global state lock is only
    /// held for map bookkeeping — never across claim / S3 restore — so
    /// a slow restore of one database cannot block cached or other
    /// cold databases. Same-key callers share the opener's result and
    /// wait at most until `wait_deadline` (then 503, retryable). A
    /// failed open clears the slot, so the next call retries cleanly.
    pub fn get_until(
        &self,
        kind: crate::paths::ResourceKind,
        database_id: &str,
        wait_deadline: std::time::Instant,
    ) -> Result<Arc<RusqliteEngine>, EngineError> {
        let key: EngineKey = (kind, database_id.into());
        let slot = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            // T36b: shutting down -- all new access is rejected as 503.
            if state.closing {
                return Err(EngineError::Unavailable("service shutting down"));
            }
            if let Some(e) = state.engines.get(&key) {
                if e.is_fenced() {
                    state.engines.remove(&key);
                } else {
                    return Ok(e.clone());
                }
            }
            // The tombstone check is under the lock: serialized with drop_db's marker/tombstone writes.
            if state.tombstones.contains(&key)
                || paths::marker_exists(self.root_for(kind), database_id)
                    .map_err(|e| EngineError::Message(format!("tombstone check: {e}")))?
            {
                state.tombstones.insert(key.clone());
                if let Some(d) = state.diag.get_mut(&key) {
                    d.dropped = true;
                    d.fenced = true;
                }
                return Err(EngineError::Dropped);
            }
            if let Some(existing) = state.opening.get(&key).cloned() {
                drop(state);
                return existing.wait(wait_deadline);
            }
            // Within the restore-failure backoff window: 503 directly (fail closed, no repeated download).
            if let Some((_, until)) = state.restore_backoff.get(&key) {
                if std::time::Instant::now() < *until {
                    return Err(EngineError::Unavailable(
                        "database restore failing; backing off",
                    ));
                }
            }
            let slot = OpenSlot::new();
            state.opening.insert(key.clone(), slot.clone());
            slot
        };
        // We are the opener. The guard clears the slot on every exit.
        let _guard = OpenGuard {
            reg: self,
            key: key.clone(),
            slot: slot.clone(),
        };
        let opened = self.open_claimed(kind, database_id);
        let result = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            match opened {
                Err(EngineError::Dropped) => {
                    state.tombstones.insert(key.clone());
                    if let Some(d) = state.diag.get_mut(&key) {
                        d.dropped = true;
                        d.fenced = true;
                    }
                    Err(EngineError::Dropped)
                }
                Err(e) => Err(e),
                // A DROP or shutdown raced the (lock-free) open: the
                // fresh engine is discarded, never published. DROP's
                // manifest tombstone already fenced its lease.
                Ok(_) if state.tombstones.contains(&key) => Err(EngineError::Dropped),
                Ok(_) if state.closing => Err(EngineError::Unavailable("service shutting down")),
                Ok((e, diag)) => {
                    state.restore_backoff.remove(&key);
                    if let Some(d) = diag {
                        state.diag.insert(key.clone(), d);
                    }
                    state.engines.insert(key.clone(), e.clone());
                    Ok(e)
                }
            }
        };
        slot.publish(match &result {
            Ok(e) => Ok(e.clone()),
            Err(e) => Err(SharedOpenErr::from_engine(e)),
        });
        result
    }

    /// Cold open (**without holding the global lock**): persistence -> claim/restore/install.
    /// Returns the engine and the diag entry to be written (published by the caller under the lock).
    /// T37r: kind decides the Persister (object-key prefix) and the local root.
    fn open_claimed(
        &self,
        kind: crate::paths::ResourceKind,
        database_id: &str,
    ) -> Result<(Arc<RusqliteEngine>, Option<DbDiag>), EngineError> {
        let Some(p) = self.persist.as_ref().map(|pp| pp.for_kind(kind).clone()) else {
            return Ok((
                Arc::new(RusqliteEngine::open(self.root_for(kind), database_id)?),
                None,
            ));
        };
        let outcome = p.claim(database_id).map_err(|e| {
            crate::metrics::persistence_failure("claim");
            match e {
                crate::persist::PersistError::Tombstoned => EngineError::Dropped,
                crate::persist::PersistError::HeldByOther => {
                    EngineError::Unavailable("database lease held by another writer")
                }
                crate::persist::PersistError::Store(se) => match se {
                    vrt_objstore::StoreError::Unavailable { .. } => {
                        EngineError::Unavailable("persistence unavailable")
                    }
                    _ => EngineError::Message(format!("claim: {se}")),
                },
                other => EngineError::Message(format!("claim: {other}")),
            }
        })?;
        let (mut handle, restore) = match outcome {
            crate::persist::ClaimOutcome::Ready { handle, restore } => (handle, restore),
            crate::persist::ClaimOutcome::HeldByOther { .. } => {
                return Err(EngineError::Unavailable(
                    "database lease held by another writer",
                ));
            }
            crate::persist::ClaimOutcome::Tombstoned => {
                // Persistence-layer tombstone -> write the local marker too (offline opens are likewise refused);
                // the in-memory tombstone is added by the caller under the lock.
                let _ = paths::write_marker(self.root_for(kind), database_id);
                return Err(EngineError::Dropped);
            }
        };
        if let Some(plan) = &restore {
            // RV-R31 #5: during restore the lease is renewed every half period -- pass &mut
            // handle; a renewal failure aborts the restore (fail closed, the db is not opened).
            p.restore(database_id, plan, &mut handle).map_err(|e| {
                crate::metrics::persistence_failure("restore");
                // Only "data corruption / verification mismatch" failures enter bounded backoff: they do not self-heal,
                // retrying merely repeats the download; transient store/renewal errors keep retrying immediately.
                if matches!(e, crate::persist::PersistError::Corrupt(_)) {
                    let (n, wait) = self.note_restore_failure(&(kind, database_id.to_string()));
                    crate::metrics::restore_corrupt_backoff();
                    tracing::error!(
                        db = %database_id,
                        kind = ?kind,
                        error = %e,
                        consecutive_failures = n,
                        backoff_ms = wait.as_millis() as u64,
                        "restore failed closed (corrupt snapshot/segment); backing off — see ops runbook"
                    );
                } else {
                    tracing::warn!(db = %database_id, error = %e, "restore failed");
                }
                EngineError::Unavailable("database restore failed")
            })?;
        }
        let e = Arc::new(RusqliteEngine::open_opts(
            self.root_for(kind),
            database_id,
            true,
        )?);
        e.install_persistence(handle.clone(), p.clone());
        let diag = DbDiag {
            kind,
            database_id: database_id.into(),
            epoch: handle.epoch,
            lsn: handle.lsn,
            expires_at_ms: handle.expires_at_ms,
            degraded: false,
            fenced: false,
            dropped: false,
            snapshot_over_cap: false,
            chain_len: handle
                .lsn
                .saturating_sub(handle.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0)),
        };
        Ok((e, Some(diag)))
    }

    /// T36b: shutdown starting point -- from then on `get()` always returns `Unavailable` (on top of the section 4.3 read
    /// rule: shutdown stops service). diag keeps its last state for queries during shutdown.
    pub fn begin_shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closing = true;
    }

    /// T36b: gracefully release all held leases -- CAS `expires_at=
    /// now` under the inner lock, each engine fails independently (warn and continue), total time bounded by `deadline`
    /// (one store round trip per db, stop at the deadline -- remaining dbs wait for their leases to expire naturally).
    pub fn release_leases(&self, deadline: std::time::Instant) {
        let engines: Vec<(EngineKey, Arc<RusqliteEngine>)> = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .engines
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        for (key, e) in engines {
            if std::time::Instant::now() >= deadline {
                tracing::warn!(remaining = ?key, "lease release deadline reached; skip rest");
                break;
            }
            e.release_lease();
        }
    }

    /// T36: remove a fenced engine (called by wire after Fenced -- the next get goes through the
    /// recovery path). diag is marked fenced.
    pub fn evict_fenced(&self, kind: crate::paths::ResourceKind, database_id: &str, epoch: u64) {
        let key: EngineKey = (kind, database_id.into());
        let mut state = self.state.lock().unwrap();
        if let Some(e) = state.engines.get(&key) {
            if e.is_fenced() {
                state.engines.remove(&key);
            }
        }
        let d = state.diag.entry(key).or_insert(DbDiag {
            kind,
            database_id: database_id.into(),
            epoch,
            lsn: 0,
            expires_at_ms: 0,
            degraded: false,
            fenced: false,
            dropped: false,
            snapshot_over_cap: false,
            chain_len: 0,
        });
        if !d.fenced {
            d.fenced = true;
        }
    }

    /// DROP (T35b interface / T35c concurrency-safe rewrite).
    ///
    /// Atomic section under the lock: write the persistent marker -> set the in-memory tombstone -> remove the engine and set its
    /// dropped flag -> atomically rename the leaf directory into `{db_root}/.trash/`.
    /// Outside the lock: bounded wait for in-flight Arcs to be returned (DROP_DRAIN), then remove_dir_all.
    /// Failure in any phase: marker/tombstone are already committed, retry resumes safely (idempotent).
    /// Ok(false) only when the id never existed and is not tombstoned.
    pub fn drop_db(
        &self,
        kind: crate::paths::ResourceKind,
        database_id: &str,
    ) -> Result<bool, EngineError> {
        crate::paths::validate_database_id(database_id)
            .map_err(|_| EngineError::Message("invalid database id".into()))?;

        // T36 section 7: first CAS the manifest tombstone (fencing every leased/expired
        // writer), then clean up locally on success. Failure -> retryable error, nothing deleted locally
        // (the control plane DROP op is redispatched). Idempotent: already tombstoned passes straight through.
        // T37r: tombstone/marker/removal are all split by kind -- a db of the same id in the other
        // kind is unaffected.
        if let Some(p) = self.persist.as_ref().map(|pp| pp.for_kind(kind)) {
            p.tombstone(database_id).map_err(|e| {
                crate::metrics::persistence_failure("tombstone");
                match e {
                    crate::persist::PersistError::Store(
                        vrt_objstore::StoreError::Unavailable { .. },
                    ) => EngineError::Unavailable("manifest tombstone failed"),
                    other => EngineError::Message(format!("manifest tombstone: {other}")),
                }
            })?;
        }

        let root = self.root_for(kind);
        let key: EngineKey = (kind, database_id.into());
        let (evicted, existed);
        {
            let mut state = self.state.lock().unwrap();
            let tombstoned = state.tombstones.contains(&key)
                || paths::marker_exists(root, database_id)
                    .map_err(|e| EngineError::Message(format!("drop tombstone check: {e}")))?;
            if !tombstoned {
                // Write the persistent marker first -- on failure return before any state changes.
                paths::write_marker(root, database_id)
                    .map_err(|e| EngineError::Message(format!("drop marker: {e}")))?;
                state.tombstones.insert(key.clone());
                if let Some(d) = state.diag.get_mut(&key) {
                    d.dropped = true;
                    d.fenced = true;
                }
            }
            let engine = state.engines.remove(&key);
            if let Some(e) = &engine {
                // Set the flag in the same critical section: from then on this engine's new batches and its pre-commit final check
                // are rejected.
                e.dropped.store(true, Ordering::SeqCst);
            }
            // leaf -> .trash/{id}-{nonce} (atomic rename on the same filesystem). Missing
            // leaf: not "half deleted"; it means the db file was never created or has already gone into .trash/ or been deleted.
            let leaf = crate::paths::derive_db_dir_readonly(root, database_id)
                .map_err(|e| EngineError::Message(format!("drop path derive: {e}")))?;
            let mut trashed = false;
            if let Some(leaf) = leaf {
                let tdir = paths::ensure_trash_dir(root)
                    .map_err(|e| EngineError::Message(format!("drop trash dir: {e}")))?;
                let nonce = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let dest = tdir.join(format!("{database_id}-{nonce}"));
                std::fs::rename(&leaf, &dest)
                    .map_err(|e| EngineError::Message(format!("drop rename to trash: {e}")))?;
                trashed = true;
            }
            existed = engine.is_some() || trashed;
            // `dropped` reports whether this call deleted an entity -- a retry on an already
            // tombstoned id returns false idempotently (consistent with the T35b contract), and the tombstone still
            // holds: any later get() is always Dropped.
            evicted = engine;
        } // lock released: the tombstone is in effect, get() is always Dropped from this moment on

        if let Some(e) = evicted {
            // In-flight requests may still hold an Arc -- wait for it to be returned with a bound, never indefinitely.
            // The blocking sleep happens only on the caller's spawn_blocking worker thread.
            let deadline = std::time::Instant::now() + DROP_DRAIN;
            while Arc::strong_count(&e) > 1 && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            drop(e);
        }
        // Remove .trash as a whole (including leftovers from drain timeout; the next drop/restart sweep is the backstop).
        if let Ok(tdir) = paths::ensure_trash_dir(root) {
            if let Ok(entries) = std::fs::read_dir(&tdir) {
                for e in entries.flatten() {
                    if e.file_name().to_string_lossy().starts_with(database_id) {
                        std::fs::remove_dir_all(e.path()).map_err(|e| {
                            EngineError::Message(format!("drop remove_dir_all: {e}"))
                        })?;
                    }
                }
            }
        }
        Ok(existed)
    }

    /// RV-R53.3: one bounded tombstone-GC pass over the local
    /// `.dropped` markers of EVERY kind. For each id whose tombstone
    /// aged past `retention_ms` the Persister reclaims the object
    /// prefix; the LOCAL marker stays — it is the permanent
    /// open-refusal record, and a finished reclaim makes later rounds
    /// a cheap no-manifest no-op. Per-database failures are isolated
    /// (logged + counted, never block the rest) and the whole round is
    /// bounded by [`GC_TOMBSTONED_MAX_PER_ROUND`].
    pub fn gc_tombstoned_round(&self) -> TombstoneGcReport {
        let mut out = TombstoneGcReport::default();
        let Some(pair) = &self.persist else {
            return out;
        };
        'outer: for kind in [
            crate::paths::ResourceKind::D1,
            crate::paths::ResourceKind::Queue,
            crate::paths::ResourceKind::K2,
        ] {
            let root = self.root_for(kind);
            for id in crate::paths::scan_gc_pending_ids(root) {
                if out.scanned >= GC_TOMBSTONED_MAX_PER_ROUND {
                    break 'outer;
                }
                out.scanned += 1;
                match pair.for_kind(kind).gc_tombstoned(&id) {
                    Ok(crate::persist::GcTombstonedOutcome::Reclaimed(freed)) => {
                        out.reclaimed += 1;
                        out.objects_freed += freed;
                        // best-effort sentinel; a failed write only means one more rescan next round,
                        // idempotent and cheap.
                        let _ = crate::paths::write_gc_done(root, &id);
                        tracing::info!(
                            kind = ?kind,
                            database = %id,
                            freed,
                            "tombstoned database reclaimed past retention"
                        );
                    }
                    Ok(crate::persist::GcTombstonedOutcome::Empty) => {
                        out.reclaimed += 1;
                        let _ = crate::paths::write_gc_done(root, &id);
                    }
                    Ok(crate::persist::GcTombstonedOutcome::Retained) => {
                        out.retained += 1;
                    }
                    Err(e) => {
                        out.failures += 1;
                        tracing::warn!(
                            kind = ?kind,
                            database = %id,
                            %e,
                            "tombstoned gc failed — retried next round"
                        );
                    }
                }
            }
        }
        crate::metrics::tombstone_gc_round(out.reclaimed, out.objects_freed, out.failures);
        out
    }

    /// Test probe: whether the id is tombstoned (in-memory set or on-disk marker).
    /// Concurrency tests use it to decide "DROP has completed", after which get() must always fail.
    #[doc(hidden)]
    pub fn state_tombstoned_for_test(
        &self,
        kind: crate::paths::ResourceKind,
        database_id: &str,
    ) -> bool {
        let state = self.state.lock().unwrap();
        state.tombstones.contains(&(kind, database_id.into()))
            || paths::marker_exists(self.root_for(kind), database_id).unwrap_or(false)
    }
}
