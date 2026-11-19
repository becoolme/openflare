//! SqlEngine trait + RusqliteEngine（§10 trait 草案的 P3a 实现）。
//!
//! 持久化等级（T33）：仅本地 fsync，无复制；epoch 字段已预留。

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

// ---------- wire 类型（与 D1 协议对齐，§2 表） ----------

/// `Value = number | string | null | number[]`（number[] → BLOB）。
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
                // number[] → BLOB；元素非 number 即 schema 违规
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

/// `?resultsFormat=`：非法/缺省回落 ARRAY_OF_OBJECTS（miniflare `.catch`）。
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

// ---------- 错误 ----------

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
    /// T35c：id 已被 DROP op tombstone（内存集 + 磁盘 `.dropped`
    /// marker）。fail closed，wire 面映射为 `{success:false}`。
    #[error("database dropped")]
    Dropped,
    /// T36：暂态不可用（lease 在别处存活/持久层不可达/恢复失败）。
    /// wire 映射 503 + `code:"VRT_UNAVAILABLE"` + retryable。
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
    #[error("path: {0}")]
    Path(#[from] PathError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl EngineError {
    /// wire `{error}` 文本（golden 比对对象）。
    pub fn wire_message(&self) -> String {
        match self {
            EngineError::Sqlite(e) => e.to_string(),
            other => other.to_string(),
        }
    }
}

// ---------- trait（§10 草案 P3a 裁剪：同步引擎） ----------

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
    /// `(epoch, seq)` 去重键；None = 不去重（兼容直连测试）。
    pub dedup: Option<(u64, u64)>,
}

pub struct LtxSegment;

pub struct Snapshot;

#[async_trait::async_trait]
pub trait SqlEngine: Send + Sync {
    /// 单事务执行整批；失败全批回滚只回首错。
    /// resultsFormat 属 wire 关注点，trait 默认对象数组口径。
    async fn query_batch(
        &self,
        batch: &[SqlStmt],
        ctx: &TxnCtx,
    ) -> Result<Vec<QueryResult>, EngineError>;
    /// 当前提交位点（裸 lsn；HMAC/定宽封装在 wire 层 BookmarkSigner）。
    async fn bookmark(&self) -> Result<u64, EngineError>;
    /// 强制 checkpoint，返回可上传的 LTX 段（P3a 未实现复制→报错）。
    async fn checkpoint(&self) -> Result<LtxSegment, EngineError>;
    /// 从快照恢复（P3a 未实现→报错）。
    async fn restore(&self, snap: &Snapshot) -> Result<(), EngineError>;
    fn size_bytes(&self) -> u64;
}

// ---------- 引擎 ----------

/// 请求负载的定式 digest（dedup 撞键检测用，含 sql+params）。
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
        h.update([0xffu8]); // 语句分隔
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

/// miniflare 同款非空判定：`sql.replace(/^\s+--.*/gm, "").trim().len() > 0`
/// （database.worker.js:175-176）。注意 `\s+` 可吞换行："空白行+注释行"
/// 整体剥；而 `--` 直接贴在行首（行内无前置空白）不剥——串首的
/// `"-- comment"` 会原样到达 SQLite 报 "did not contain a statement"。
fn is_nonempty_sql(sql: &str) -> bool {
    !strip_comment_lines(sql).iter().all(|&b| is_sql_space(b))
}

fn is_sql_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// `/^\s+--.*/gm` 的手工等价物：在"行首位置"(pos0 或换行之后)起，
/// 若连续空白字符后是 `--`，则剥掉从行首到该行尾（不含换行符）。
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
                // 命中：丢弃 [i, 下一个换行符)
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
    /// 引擎内部语句的 authorizer 旁路（仅引擎代码置位）
    internal: security::InternalFlag,
    /// T36：页面大小（段编码/快照解析）。
    page_size: u32,
    /// T36：已提交的 WAL 帧下标水位（mxFrame）。只在单写者锁内
    /// 读写；PASSIVE checkpoint 观测，TRUNCATE 后归零。
    wal_frames: u64,
    /// RV-R31 #1：当前 WAL 代际盐对——帧截取只认本代际。
    /// `None` = 未播种（open/TRUNCATE 后的第一次捕获按当时
    /// header 播种，水位为 0 时代际切换无需区分）。
    wal_salt: Option<(u32, u32)>,
    /// RV-R31 #1：水位帧之后的累计 checksum——下一段帧链的
    /// 校验种子。TRUNCATE 后随代际清 0。
    wal_cksum: (u32, u32),
    /// RV-R31 #1：WAL 校验和字节序（header magic 末位）。
    wal_be: bool,
    /// T36：lease + 持久化槽（persistence backend 存在时）。
    persist: Option<PersistSlot>,
}

/// T36：引擎持有的持久化状态（在 inner 锁内维护——所有持久化
/// 操作与本地提交同属单写者临界区，顺序天然串行）。
struct PersistSlot {
    handle: crate::persist::ManifestHandle,
    persister: std::sync::Arc<crate::persist::Persister>,
    /// T36b：checkpoint 动态间隔（commits）——失败指数翻倍，
    /// 封顶 `every*64`；成功归零时回到 `every`。防"快照超限/
    /// store 抖动 → 每次提交都重传整库"的病态循环。
    ckpt_gap: u64,
}

pub struct RusqliteEngine {
    inner: Mutex<EngineInner>,
    commit_lsn: AtomicU64,
    served_by: String,
    #[allow(dead_code)]
    database_id: String,
    /// T35c：registry drop_db 在摘除引擎的同一临界区内置位。
    /// query_batch 入口与 commit 前各查一次——摘除后到目录 rename/
    /// 删除窗口内的新写入不再可能"成功但随即被删"。
    dropped: AtomicBool,
    /// T36：lease 丢失/持久化失败 → fenced；此后一切读写被拒，
    /// registry 摘除后经恢复路径重建。
    fenced: AtomicBool,
    /// T36b：最近一次 checkpoint 因快照超限被拒（诊断/告警面）。
    snapshot_over_cap: AtomicBool,
    /// T36：写者 epoch（bookmark 的 epoch 分量；无持久化时为 1，
    /// 等价旧行为——旧 bookmark 的 epoch 字段来自调用方）。
    lease_epoch: AtomicU64,
    /// T36：上次续租失败但仍在 margin 内（诊断/metric 读取）。
    degraded: AtomicBool,
    /// T36：db 文件路径（WAL 旁车定位）。
    db_file: PathBuf,
    _lock_file: std::fs::File, // 保持 flock 至 drop
}

impl RusqliteEngine {
    /// 打开（必要时创建）数据库文件；进程内由 Registry 保证唯一打开，
    /// flock(LOCK_EX|LOCK_NB) 覆盖跨进程重复打开（fail closed）。
    pub fn open(db_root: &Path, database_id: &str) -> Result<Self, EngineError> {
        // T35c tombstone：先查 `.dropped` marker 再 derive——
        // derive_db_file 会建目录，顺序反了会让已删除的 id "复活"。
        if paths::marker_exists(db_root, database_id)? {
            return Err(EngineError::Dropped);
        }
        let db_file = paths::derive_db_file(db_root, database_id)?;
        Self::open_file_opts(&db_file, database_id, false)
    }

    /// T36：`wal_capture=true`（持久化启用）时关闭 auto-checkpoint
    /// 并在 open 时 TRUNCATE checkpoint：此后 WAL 文件只在我们的显式
    /// checkpoint 时回缩，字节水位捕获才成立（`persist_commit`）。
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

    /// 测试/嵌入式直接指定文件路径（仍走 flock + 安全初始化）。
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
        // 文件锁：旁车 {db}.lock（不锁 db 本体——macOS 上 flock 走 fcntl
        // 实现，会与 SQLite 自身 POSIX 锁冲突产生 SQLITE_BUSY）。
        let lock_path = db_file.with_extension("lock");
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)?;
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
        // T36：wal_capture 关 auto-checkpoint——WAL 只允许被我们的显式
        // checkpoint 回缩，文件字节水位成为单调序列（capture 正确性）。
        // RV-R31 #2：持久化库 commit 返回前必须 fsync WAL——
        // `.vrt-lsn` 标记承诺"本地文件已达 manifest.lsn"；
        // synchronous=NORMAL 下 commit 落 OS 缓冲即返回，掉电后
        // WAL 尾部丢失而标记仍在 → 认领时少数据被信任（fail
        // closed 改为 FULL；代价是每提交一次 fsync，远小于同步
        // 上传段的对象存储 RTT）。无持久化的库无此承诺，不动。
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
        // T36：capture 模式下 open 即 TRUNCATE——已提交帧归并进 db
        // 文件、崩溃残留的死帧一并清除，wal 文件归零，水位从 0 起算。
        // busy → fail closed（单连接 open 期不应 busy）。
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
            _lock_file: lock_file,
        })
    }

    /// T36：registry 在 claim 成功后安装持久化状态；commit_lsn 与
    /// 写者 epoch 对齐 manifest（跨接管不回退）。
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

    /// T36：本引擎是否已 fenced（lease 丢失或持久化失败）。
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }
    /// T36：标记 fenced（后续读写全拒）。swap 保证同一引擎多次
    /// fence 只 +1 一次（配合 Drop 的 -1，gauge 不泄漏）。
    fn fence(&self, epoch: u64) -> EngineError {
        if !self.fenced.swap(true, Ordering::SeqCst) {
            crate::metrics::set_fenced_dbs_delta(1);
        }
        EngineError::Fenced(epoch)
    }
    /// T36b：优雅停机——inner 锁内 CAS `expires_at=now`（epoch/
    /// holder/lsn 不变），随后强制 fence：本引擎此后不得再写
    /// （即使 release 因 store 错误未完成——fencing 是底线）。
    /// 由 `EngineRegistry::release_leases` 调用。
    pub fn release_lease(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let epoch = self.lease_epoch();
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

    /// T36：degraded（续租失败但 lease 有效）观察口。
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }

    /// T36：lease 门槛（§4.3）——`expiry-margin > now` 才可用；
    /// 到点先续租；续租失败但未到期 → degraded（读放行写照走，
    /// publish 自带 CAS 兜底）；已不可用 → 续一次，失败 fenced。
    /// 前置：持有 inner 锁。
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
                        // 续租失败但 lease 有效 → degraded。
                        slot.handle.degraded = true;
                        self.degraded.store(true, Ordering::SeqCst);
                        crate::metrics::lease_renew_failure("store");
                        tracing::warn!(db = %self.database_id, error = %e, "lease renew failed (degraded)");
                    }
                }
            }
            return Ok(());
        }
        // lease 不可用 → 续租是唯一自救；任何失败都 fenced。
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

    /// T36 §4.2 方案 1：本提交产生的 WAL 帧 → vrt-seg v1 → 同步
    /// 上传 → CAS manifest lsn → 返回新 lsn。只读事务返回 None。
    /// 前置：tx 已 commit，持有 inner 锁（单写者临界区）。
    ///
    /// RV-R31 #1：截取上限 = 验证链中最后一个 **commit 帧**
    /// （`commit_db_size≠0`），不是物理文件末尾。回滚 spill 死帧
    /// 留在 mxFrame 之后，其 checksum 仍是本链合法延续——链验证
    /// 无法剔除，只有 commit 边界能；反之新提交也可能完全落在旧
    /// 文件长度内（物理零增长，旧 `len==0` 判只读 → 丢已确认
    /// 写）。判定"有无新帧"用验证后的帧数而非文件长度。
    ///
    /// 代际锚定：`wal_frames>0` 时 header salt 必须等于已播种
    /// 代际，否则 WAL 在锁外被重建 → fail closed；水位 0 按当前
    /// header 播种（open/TRUNCATE 后第一写的正确起点）。水位处
    /// 连一帧都验证不了而文件确有尾部字节 = 簿记与现实脱节 →
    /// fail closed。
    ///
    /// 不用 `PRAGMA wal_checkpoint` 读 mxFrame——PASSIVE checkpoint
    /// 完成后 SQLite 重置 WAL（mxFrame 归 0），水位自证失效。
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
                // TRUNCATE 后 wal 尚未重建（纯读批次）——无新帧。
                return Ok(None);
            }
            // 水位非 0 而文件消失/撕裂 → 锁外干预，fail closed。
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
            return Ok(None); // 物理尾部恰在水位 → 无新字节
        }
        let len = wal_len - start;
        let mut buf = vec![0u8; len as usize];
        f.read_exact_at(&mut buf, start)
            .map_err(|e| EngineError::Message(format!("wal read: {e}")))?;
        // 逐帧验证 salt + 累计 checksum（防撕裂/异代际帧入段）；
        // 截取上限 = 链中最后一个 commit 帧——其后合法延续帧是
        // 回滚 spill 死帧，不编码、不进水位（SQLite 下次写会覆写
        // 该区域，水位不被死帧污染）。
        let (_vend, heads, _ck) = crate::wal::scan_frames(&buf, 0, ps, be, salt, cksum_seed);
        if heads.is_empty() {
            return Err(EngineError::Message(
                "wal unreadable at capture watermark".into(),
            ));
        }
        let Some(last_commit) = heads.iter().rposition(|h| h.commit_db_size != 0) else {
            // 链上全是非 commit 帧 = 纯死帧尾巴——无新提交。
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
        // sidecar 记 WAL 物理帧数（含死帧尾）——认领时本地文件
        // 完整性核对基准（截短/塞垃圾都偏离它）。
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
        // 周期 checkpoint：TRUNCATE wal + 上传快照 + GC 旧段。
        // T36b：触发用动态 `ckpt_gap`（失败指数退避）而非固定
        // `every`——超限/抖动库不随每次提交重试整库上传。
        let every = p.config().checkpoint_every_commits;
        let gap_due = every > 0
            && inner
                .persist
                .as_ref()
                .map(|s| s.handle.commits_since_ckpt >= s.ckpt_gap)
                .unwrap_or(false);
        if gap_due {
            if let Err(e) = self.checkpoint_now(inner) {
                // checkpoint 失败不致命（段链仍完整）；lease 丢失则 fenced。
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
                // 退避：gap 翻倍封顶 every*64——本次失败已消费本次
                // commits_since_ckpt 触发位，下一次触发在 +gap 提交后。
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

    /// T36：TRUNCATE checkpoint + 快照上传（inner 锁内）。
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
                // TRUNCATE 重置 mxFrame 并换 WAL 代际（salt1++）——
                // 水位归零的同时代际锚定也清 0，下一次捕获按新
                // header 重新播种。
                inner.wal_frames = 0;
                inner.wal_salt = None;
                inner.wal_cksum = (0, 0);
            }
            Ok(_) => {
                // busy：有未 checkpoint 帧——本次跳过（下次再试）。
                return Ok(());
            }
            Err(e) => return Err(EngineError::Sqlite(e)),
        }
        p.checkpoint(
            &self.database_id,
            &mut inner.persist.as_mut().unwrap().handle,
            &self.db_file,
        )
        .map_err(|e| match e {
            crate::persist::PersistError::Fenced => self.fence(epoch),
            _ => EngineError::Message(format!("checkpoint: {e}")),
        })
    }

    /// size_after：pragma_*() 函数被 authorizer 拒，走 internal 旁路。
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

    /// 单事务执行整批；任一失败全批回滚只回首错（§3）。
    /// `format` 只影响 results 形状，不影响执行语义。
    pub fn query_batch_sync(
        &self,
        batch: &[SqlStmt],
        ctx: &TxnCtx,
        format: ResultsFormat,
    ) -> Result<Vec<QueryResult>, EngineError> {
        // T35c：drop 已摘除本引擎 → 直接拒（防"写成功但目录已删"）。
        if self.dropped.load(Ordering::SeqCst) {
            return Err(EngineError::Dropped);
        }
        // T36：fenced 引擎一切读写拒（恢复由 registry 重建）。
        if self.fenced.load(Ordering::SeqCst) {
            return Err(EngineError::Fenced(self.lease_epoch.load(Ordering::SeqCst)));
        }
        let mut inner = self.lock_inner(ctx.deadline)?;
        // T36：lease 门槛（§4.3）——读与写同一判定。
        self.ensure_lease(&mut inner, ctx.deadline)?;
        let internal = inner.internal.clone();
        // T36：intent lsn 在 tx 创建前取出（tx 持有 inner.conn 可变
        // 借用期间无法再访问 inner）。
        let persist_intent = inner
            .persist
            .as_ref()
            .map(|s| (s.persister.clone(), s.handle.lsn + 1));

        // 与 miniflare 一致的过滤语义
        let stmts: Vec<&SqlStmt> = batch.iter().filter(|q| is_nonempty_sql(&q.sql)).collect();
        if stmts.is_empty() {
            return Err(EngineError::Message("No SQL statements detected.".into()));
        }

        // 去重命中 → 不重放（账本在库内同事务提交，见 §3 R4；
        // _vrt_dedup 对用户拒读，引擎走 internal 旁路）
        // S4：dedup epoch 必须等于当前写者 epoch——跨 epoch 的 seq
        // 重放可能把旧位点的结果当新位点提交，fail closed。
        if let Some((depoch, _)) = ctx.dedup {
            if depoch != ctx.epoch {
                return Err(EngineError::Message(format!(
                    "dedup epoch {depoch} != writer epoch {}",
                    ctx.epoch
                )));
            }
        }
        // T34b: dedup 键按 idempotency-key 语义校验负载——同
        // (epoch,seq) 但负载不同 ⇒ seq 分配撞键（provider 重启回绕
        // 等），fail closed 而非回放旧结果。判别式字节编码，免序列化歧义。
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

        // 每语句超时：progress handler 周期性检查 deadline（§3 R5）
        security::arm_deadline(&tx, deadline);

        let mut out = Vec::with_capacity(stmts.len());
        let mut failed: Option<EngineError> = None;

        for q in &stmts {
            if Instant::now() >= deadline {
                failed = Some(EngineError::Timeout);
                break;
            }
            // `VACUUM`/`VACUUM INTO` 语句文本预检（authorizer 无 VACUUM
            // action code；裸 VACUUM 内部 Attach{''} 与用户 ATTACH 不可分）
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
                let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
                while let Some(row) = rows_cur.next().map_err(EngineError::Sqlite)? {
                    let mut r = Vec::with_capacity(cols.len());
                    for i in 0..cols.len() {
                        r.push(value_to_json(row.get_ref(i)?)?);
                    }
                    rows.push(r);
                }
                let nrows = rows.len() as i64;
                Ok((format_rows(format, &cols, rows), nrows))
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
                    failed = Some(e);
                    break;
                }
            }
        }

        let result = match failed {
            Some(e) => {
                drop(tx); // 回滚整批（含 dedup 行——业务失败账本不落盘）
                Err(e)
            }
            None => {
                // dedup 账本与业务同事务提交
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
                // T35c：commit 前终检——批次执行期间被 drop 时回滚并报错，
                // 不让写入落在已 rename 进 .trash 的文件上还返回成功。
                if self.dropped.load(Ordering::SeqCst) {
                    drop(tx);
                    return Err(EngineError::Dropped);
                }
                // T36：commit 前写 intent lsn（防"本地已提交未 ack
                // 被信任"——崩溃窗口的未持久化提交会让 sidecar 超出
                // manifest.lsn，claim 强制恢复清掉）。
                if let Some((p, lsn)) = &persist_intent {
                    if p.mark_intent(&self.database_id, *lsn).is_err() {
                        // RV-R31 #2：意图标记写不进盘 = 本地持久层
                        // 不可信——回滚本批并拒 ack（fail closed）。
                        drop(tx);
                        return Err(EngineError::Unavailable("local intent marker write failed"));
                    }
                }
                tx.commit().map_err(EngineError::Sqlite)?;
                mark_committed();
                // T36 §4.2：有持久化 → ack 前段上传+manifest CAS
                // （锁内串行，失败 fenced 不 ack）；无持久化保持旧语义。
                if inner.persist.is_some() {
                    // conn 借用已随 tx 释放——直接以 inner 调。
                    match self.persist_commit(&mut inner, ctx.deadline) {
                        Ok(_) => Ok(out),
                        Err(e) => Err(e),
                    }
                } else {
                    self.commit_lsn.fetch_add(1, Ordering::SeqCst);
                    Ok(out)
                }
            }
        };
        security::disarm_deadline(&mut inner.conn);
        result
    }

    /// T37：引擎内部事务 —— QueueBackend 等非用户 SQL 面的唯一
    /// 提交路径（pub(crate)：租户 `/query` 路径不可达）。与
    /// `query_batch_sync` 共享全部不变式：dropped/fenced 门槛、lease
    /// 门槛、deadline 进度句柄、commit 前 dropped 终检、intent lsn、
    /// commit → persist_commit ack 前持久化；整段在 InternalFlag 旁路
    /// 下执行（`_vrt_q_*` 内部表可达），无 dedup 账本、无结果集物化。
    ///
    /// `f` 同步执行、不得跨 await（本方法本身同步持锁）；
    /// `&Transaction` 借用不逃出闭包（T 与句柄类型分离，无法返回
    /// 句柄）；f Err → 全批回滚、不推进 lsn、不 persist。
    /// InternalFlag 经 RAII guard 复位——f panic 时同样复位，
    /// panic 被 catch_unwind 收拢为 Err（不毒化 inner 锁）。
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
        // Drop-guard：f 提前 return / Err / panic 都复位旁路标志。
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
                        // RV-R31 #2：同 query_batch_sync——标记失败
                        // 即本地持久层不可信，回滚拒 ack。
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

    /// 当前提交位点（bookmark 的 lsn 来源）。
    pub fn commit_lsn(&self) -> u64 {
        self.commit_lsn.load(Ordering::SeqCst)
    }

    /// T36：写者 lease epoch（bookmark 的 epoch 分量）。
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch.load(Ordering::SeqCst)
    }

    /// T36b：段链长度（lsn - 基线快照 lsn）；无 persist → None。
    pub fn chain_len(&self) -> Option<u64> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.persist.as_ref().map(|s| {
            s.handle
                .lsn
                .saturating_sub(s.handle.snapshot.as_ref().map(|x| x.lsn).unwrap_or(0))
        })
    }

    /// T36：本引擎是否挂了持久化。
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

fn format_rows(
    format: ResultsFormat,
    cols: &[String],
    rows: Vec<Vec<serde_json::Value>>,
) -> serde_json::Value {
    match format {
        // miniflare：ROWS_AND_COLUMNS → {columns, rows}；其余（含 NONE）→ 对象数组
        ResultsFormat::RowsAndColumns => serde_json::json!({ "columns": cols, "rows": rows }),
        _ => serde_json::Value::Array(
            rows.iter()
                .map(|r| {
                    cols.iter()
                        .cloned()
                        .zip(r.iter().cloned())
                        .collect::<serde_json::Map<String, serde_json::Value>>()
                })
                .map(serde_json::Value::Object)
                .collect(),
        ),
    }
}

fn value_to_json(v: rusqlite::types::ValueRef) -> Result<serde_json::Value, EngineError> {
    Ok(match v {
        rusqlite::types::ValueRef::Null => serde_json::Value::Null,
        rusqlite::types::ValueRef::Integer(i) => serde_json::json!(i),
        rusqlite::types::ValueRef::Real(f) => serde_json::json!(f),
        rusqlite::types::ValueRef::Text(t) => serde_json::json!(String::from_utf8_lossy(t)),
        rusqlite::types::ValueRef::Blob(b) => {
            // miniflare convertRows：BLOB → 字节数组
            serde_json::json!(b.iter().map(|x| *x).collect::<Vec<u8>>())
        }
    })
}

#[cfg(unix)]
fn lock_file_fd(f: &std::fs::File) -> libc::c_int {
    use std::os::unix::io::AsRawFd;
    f.as_raw_fd()
}

// ---------- Registry：进程内单写者 map + drop tombstone ----------

/// engines 与 tombstones 共享一把锁：drop_db 的 marker→tombstone→evict
/// →flag→rename 与 get() 的 tombstone/marker 检查在同一临界区完成，
/// 不存在"摘除后、删除前"可重新打开的窗口（T35c）。
/// T37r：Registry 键 = (kind, databaseId)。同一 id 的 D1 库与 Queue
/// 库是两个互不相交的引擎/库/marker 命名空间。
pub type EngineKey = (crate::paths::ResourceKind, String);

#[derive(Default)]
struct RegistryState {
    engines: HashMap<EngineKey, Arc<RusqliteEngine>>,
    /// 本进程内已 DROP 的 (kind,id)；磁盘 `.dropped` marker 是持久
    /// 副本（重启后由两个 root 各自的 scan_dropped_ids 预灌 +
    /// get() 冷路径 stat 兜底）。
    tombstones: HashSet<EngineKey>,
    /// T36：诊断视图（`/internal/dbs`）——每库的 lease/位点/状态。
    diag: HashMap<EngineKey, DbDiag>,
    /// T36b：停机中——拒绝新 get/claim（防释放后同进程重夺）。
    closing: bool,
    /// RV-R71: single-flight cold opens. One in-progress open per key;
    /// same-key callers wait on the slot (with a deadline) instead of
    /// re-running claim/restore, and no caller holds the global lock
    /// while the object store is being talked to.
    opening: HashMap<EngineKey, Arc<OpenSlot>>,
    /// 2026-11-15：恢复失败（快照/段损坏、sha 不符等 fail-closed）的
    /// 每库有界指数退避：(连续失败次数, 下次允许重试时刻)。窗口内的
    /// `get` 直接 503，不再重复拉取快照/段（修复前对同一库每个请求
    /// 都全量重下 + 重算，143 次紧循环）。成功恢复即清除。
    restore_backoff: HashMap<EngineKey, (u32, std::time::Instant)>,
}

/// 恢复失败退避的默认基数/上限：1s、2s、4s … 封顶 5min。
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

/// T36：只读诊断条目（JSON camelCase——`/internal/dbs` 对外形状）。
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbDiag {
    /// T37r：资源种类（`/internal/dbs` 输出 `"kind":"d1"|"queue"`）。
    pub kind: crate::paths::ResourceKind,
    pub database_id: String,
    pub epoch: u64,
    pub lsn: u64,
    pub expires_at_ms: u64,
    pub degraded: bool,
    pub fenced: bool,
    pub dropped: bool,
    /// T36b：最近一次 checkpoint 因 `snapshotMaxBytes` 超限被拒
    /// （该库恢复成本随段链长线性增长，见 §4.5）。
    pub snapshot_over_cap: bool,
    /// T36b：当前段链长度（lsn - snapshot.lsn）——恢复需重放的
    /// 段数；随 snapshot 推进归零。
    pub chain_len: u64,
}

impl Drop for RusqliteEngine {
    fn drop(&mut self) {
        if self.fenced.load(Ordering::SeqCst) {
            crate::metrics::set_fenced_dbs_delta(-1);
        }
    }
}

/// T36：Registry 可 Clone（共享同一 state/db_root，persist 仅
/// 启动时装配——`with_persister` 返回新实例覆盖 AppState 内 registry，
/// 语义不变）。
#[derive(Clone)]
pub struct EngineRegistry {
    state: std::sync::Arc<Mutex<RegistryState>>,
    /// D1 本地根（`…/dbs`，T36 布局不变）。
    db_root: PathBuf,
    /// T37r：Queue 本地根（`…/queues`，与 db_root 同级派生）。
    queue_root: PathBuf,
    /// T36/T37r：对象存储持久化面——每种 kind 一个 Persister
    /// （对象键前缀与本地 root 各绑定），`[persistence]` ≠ none 时装配。
    persist: Option<PersisterPair>,
    restore_backoff_base: std::time::Duration,
    restore_backoff_max: std::time::Duration,
}

/// T37r：按 kind 分面的 Persister 对——D1 写 `db/{id}`/`dbs/`，
/// Queue 写 `q/{id}`/`queues/`。
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
pub struct PersisterPair {
    pub d1: std::sync::Arc<crate::persist::Persister>,
    pub queue: std::sync::Arc<crate::persist::Persister>,
}

impl PersisterPair {
    fn for_kind(
        &self,
        kind: crate::paths::ResourceKind,
    ) -> &std::sync::Arc<crate::persist::Persister> {
        match kind {
            crate::paths::ResourceKind::D1 => &self.d1,
            crate::paths::ResourceKind::Queue => &self.queue,
        }
    }
}

/// 引擎摘除后等待在途 Arc 归还的上限（覆盖默认事务预算数倍；
/// 超时仍继续删——flock/fd 不阻塞 unlink+rename）。
const DROP_DRAIN: std::time::Duration = std::time::Duration::from_secs(10);

impl EngineRegistry {
    pub fn new(db_root: PathBuf) -> Self {
        let queue_root = paths::queue_root_for(&db_root);
        // 启动清扫 .trash（上次崩溃留下的已 tombstone 残骸），
        // 并把盘上 .dropped marker 预灌进内存 tombstone set——两个
        // root 各自扫描，tombstone 键带 kind（同一 id 的 D1 drop 不
        // 影响 Queue）。
        paths::sweep_trash(&db_root);
        paths::sweep_trash(&queue_root);
        let tombstones: HashSet<EngineKey> = paths::scan_dropped_ids(&db_root)
            .into_iter()
            .map(|id| (crate::paths::ResourceKind::D1, id))
            .chain(
                paths::scan_dropped_ids(&queue_root)
                    .into_iter()
                    .map(|id| (crate::paths::ResourceKind::Queue, id)),
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
            persist: None,
            restore_backoff_base: RESTORE_BACKOFF_BASE,
            restore_backoff_max: RESTORE_BACKOFF_MAX,
        }
    }

    /// 覆盖恢复失败退避参数（测试与运维调参）。
    pub fn with_restore_backoff(
        mut self,
        base: std::time::Duration,
        max: std::time::Duration,
    ) -> Self {
        self.restore_backoff_base = base;
        self.restore_backoff_max = max;
        self
    }

    /// 记录一次恢复失败并返回本次退避时长（base·2^(n-1)，封顶 max）。
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

    /// T36/T37r：安装持久化面（service 启动时调用；none backend 不装）。
    /// 两种 kind 的 Persister 必须同时装配——只装 D1 会让 queue
    /// 静默走纯本地路径（持久化缺口 fail closed 原则拒绝）。
    pub fn with_persisters(mut self, pair: PersisterPair) -> Self {
        self.persist = Some(pair);
        self
    }

    /// kind → 本地 root。
    fn root_for(&self, kind: crate::paths::ResourceKind) -> &Path {
        match kind {
            crate::paths::ResourceKind::D1 => &self.db_root,
            crate::paths::ResourceKind::Queue => &self.queue_root,
        }
    }

    /// T36：诊断快照（只读，auth 保护端点用）。
    pub fn diagnostics(&self) -> Vec<DbDiag> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<DbDiag> = state.diag.values().cloned().collect();
        // T36b：活引擎实时充实（degraded/over_cap/chain_len 随提交
        // 变化，diag map 只在 claim 时快照一次）。
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

    /// 取得（或惰性打开）databaseId 的写句柄。重复打开同 (kind,id)
    /// 走同一句柄；跨进程重复打开由 flock fail closed；已 DROP 的
    /// (kind,id) 永远拒绝（另一 kind 不受影响）。
    /// T36：persistence 开启时，冷打开 = manifest claim（lease 认领/
    /// 接管/tombstone/held-elsewhere）→ 必要时快照+段恢复 → 装 lease。
    /// fenced 引擎在锁内摘除后走同一冷路径自愈。
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
            // T36b：停机中——全部新访问拒为 503。
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
            // tombstone 检查在锁内：与 drop_db 的写 marker/tombstone 串行。
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
            // 恢复失败退避窗口内：直接 503（fail closed，不重复下载）。
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

    /// 冷打开（**不持全局锁**）：persistence → claim/restore/install。
    /// 返回引擎与待写入的 diag 条目（由调用方在锁内发布）。
    /// T37r：kind 决定 Persister（对象键前缀）与本地 root。
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
                // 持久层 tombstone → 本地 marker 也落（离线同样拒开）；
                // 内存 tombstone 由调用方在锁内补。
                let _ = paths::write_marker(self.root_for(kind), database_id);
                return Err(EngineError::Dropped);
            }
        };
        if let Some(plan) = &restore {
            // RV-R31 #5：恢复期间租约按半周期续租——传 &mut
            // handle，续租失败即中止恢复（fail closed，库不打开）。
            p.restore(database_id, plan, &mut handle).map_err(|e| {
                crate::metrics::persistence_failure("restore");
                // 仅"数据损坏/校验不符"类失败进入有界退避：它们不会自愈，
                // 重试只是重复下载；瞬时存储/续租错误保持即时重试。
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

    /// T36b：停机起点——此后 `get()` 一律 `Unavailable`（§4.3 读
    /// 规则之上：停机即停服务）。diag 保留末态供停机中查询。
    pub fn begin_shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closing = true;
    }

    /// T36b：优雅释放全部在持 lease——inner 锁内 CAS `expires_at=
    /// now`，各引擎独立失败（warn 继续），总耗时受 `deadline` 上限
    /// （每库一次 store 往返，超截止即停——剩余库等 lease 自然过期）。
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

    /// T36：摘除 fenced 引擎（wire 在 Fenced 后调用——下次 get 走
    /// 恢复路径）。diag 标记 fenced。
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

    /// DROP（T35b 接口 / T35c 并发安全重写）。
    ///
    /// 锁内原子段：写持久 marker → 置内存 tombstone → 摘除引擎并置其
    /// dropped 标志 → leaf 目录原子 rename 到 `{db_root}/.trash/`。
    /// 锁外：有界等在途 Arc 归还（DROP_DRAIN），再 remove_dir_all。
    /// 任一阶段失败：marker/tombstone 已提交，重试安全续跑（幂等）。
    /// Ok(false) 仅当该 id 从未存在且未 tombstone。
    pub fn drop_db(
        &self,
        kind: crate::paths::ResourceKind,
        database_id: &str,
    ) -> Result<bool, EngineError> {
        crate::paths::validate_database_id(database_id)
            .map_err(|_| EngineError::Message("invalid database id".into()))?;

        // T36 §7：先 CAS manifest tombstone（fence 所有在租/过期
        // 写者），成功后再本地清理。失败 → retryable 错误，本地不删
        // （控制面 DROP op 会重派）。幂等：已 tombstone 直接过。
        // T37r：tombstone/marker/摘除全部按 kind 分面——同一 id 另
        // 一 kind 的库不受影响。
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
                // 先写持久 marker——失败则在任何状态改变前返回。
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
                // 同一临界区置标志：此后该引擎新批次与 commit 前终检
                // 都会被拒。
                e.dropped.store(true, Ordering::SeqCst);
            }
            // leaf → .trash/{id}-{nonce}（同文件系统原子 rename）。leaf
            // 缺失：非"删一半"，表示 db 文件从未创建或已进 .trash/已删。
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
            // `dropped` 报告的是本次调用是否删到实体——已 tombstone
            // 的重试幂等返回 false（与 T35b 契约一致），tombstone 仍
            // 生效：任何后续 get() 永远 Dropped。
            evicted = engine;
        } // 锁释放：tombstone 已生效，get() 从此刻起一律 Dropped

        if let Some(e) = evicted {
            // 在途请求可能仍持有 Arc —— 有界等待其归还，绝不无限等。
            // 阻塞睡眠只发生在调用方 spawn_blocking 的工作线程上。
            let deadline = std::time::Instant::now() + DROP_DRAIN;
            while Arc::strong_count(&e) > 1 && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            drop(e);
        }
        // .trash 整体清除（含 drain 超时残留；下一次 drop/重启清扫兜底）。
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
                        // best-effort 哨兵；写失败只是下轮重扫一次，
                        // 幂等且便宜。
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

    /// 测试探针：id 是否已 tombstone（内存集或盘上 marker）。
    /// 并发测试用它判定"DROP 已完成"，之后 get() 必须一律失败。
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
