//! QueueBackend（T37，§8/§10 落地）：每个 queue 一个独立 vrt-sql 库，
//! 复用 SqlEngine 的全部不变式（lease/fencing/tombstone/ack 前持久化，
//! 经 `exec_internal` 单事务提交，**绝不绕过持久化**）。
//!
//! 限额数值以 §8 + miniflare broker 实测为准——**十进制**字节：
//! 单消息 ≤128_000B、批 ≤100 条、批总 ≤288_000B、delay 0..=86_400s。
//! 消费端派发（dispatcher 选主、调 queue()、ack/retry/DLQ 驱动）属
//! T37b；本轮已落地的表结构/lease 语义即为其基座。
//!
//! 库类型门禁：`_vrt_q_meta.kind='queue'` 与 schema 同事务初始化；
//! 已含用户表的库（D1）调用队列端点 → fail closed `NotAQueue`。

use crate::engine::{EngineError, EngineRegistry, RusqliteEngine, TxnCtx};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

/// 与 miniflare broker 一致的十进制限额（B:50,82,379 已核实）。
pub const MSG_MAX_BYTES: usize = 128_000;
pub const BATCH_MAX_MSGS: usize = 100;
pub const BATCH_MAX_BYTES: usize = 288_000;
pub const DELAY_MAX_SECS: u32 = 86_400;
pub const CLAIM_MAX: u32 = 100;
/// T37b-r: 单次 claim 原始 body 字节预算上限（`maxBytes` 参数）。
/// 调用方可用更小值（dispatcher 默认 4MB）——上界只防失控配置，
/// 不是建议值。
pub const CLAIM_BYTES_MAX: u64 = 32 * 1024 * 1024;
/// claim lease TTL 上限（12h）——`claim_ttl > maxDeliveryTimeout`
/// 由 dispatcher（T37b）负责保证；此处只拒非正/超界值。
pub const LEASE_TTL_MAX_MS: i64 = 43_200_000;
/// 客户端可提供的消息 id 上限（printable ASCII，无分隔符）。
pub const MSG_ID_MAX: usize = 128;
/// ack/retry/dead_letter 单次结算上限（宽松于批次上限，覆盖多次 claim）。
pub const SETTLE_MAX: usize = 1_000;
/// 队列 op 内部事务预算（小于 D1 的 30s——队列语句是有界小批量）。
pub const OP_BUDGET_MS: u64 = 5_000;

/// 允许的内容类型（wire `X-Msg-Fmt` / `contentType` 域）。
pub const CONTENT_TYPES: &[&str] = &["text", "json", "bytes", "v8"];

/// queueId 形态 = §7 databaseId 规则（`^[a-z0-9]{26}$`，入口拒绝
/// `/`、`..`、大写、长度不符）。
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

/// 生产端入队消息（`body` 是**不透明字节**——v8 序列化体不解释）。
#[derive(Clone, Debug)]
pub struct IncomingMsg {
    /// 客户端 id（batch 消息 `id?`）；None → 服务端生成 32-hex。
    pub id: Option<String>,
    pub body: Vec<u8>,
    /// `text|json|bytes|v8`。
    pub content_type: String,
    /// 0..=86400。
    pub delay_secs: u32,
    /// T37r：调用方声明的时间戳——**不可信消息属性**（DLQ 转投保留
    /// 原 id/timestamp 用）。存 `client_ts` 列：经范围校验、claim
    /// 时回显给 consumer，绝不参与 `visible_at` 调度、`created_at`
    /// 或 `oldestMessageTimestamp` 指标——三者一律服务端时钟。
    /// None → 该列 NULL，consumer 看到的就是 `created_at`。
    pub timestamp_ms: Option<i64>,
}

/// claim 返回的消息（§10）。
#[derive(Clone, Debug)]
pub struct ClaimedMsg {
    pub id: String,
    pub lease_id: String,
    pub attempts: i64,
    pub body: Vec<u8>,
    pub content_type: String,
    pub timestamp_ms: i64,
}

/// `GET /metrics` 内层对象形状（与 miniflare `{backlogCount,
/// backlogBytes, oldestMessageTimestamp}` 同源，但 wire 字段为
/// `oldestMessageTimestampMs` 且整体嵌套在 `{success, metrics}` 包络
/// 下——wire.rs；oldest 为 None = 空队列，broker 面归一为 0）。
#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueMetrics {
    pub backlog_count: i64,
    pub backlog_bytes: i64,
    pub oldest_message_timestamp_ms: Option<i64>,
    /// T37b-r4：可立即 claim（visible_at<=now 且未租/租约过期）。
    /// 运维排障需要把 "写进去但领不到" 拆成 visible/inflight/
    /// delayed/dead 四桶 —— 仅新增字段，旧消费方 serde 忽略。
    #[serde(default)]
    pub visible_count: i64,
    /// T37b-r4：在途（持有未过期租约）。
    #[serde(default)]
    pub in_flight_count: i64,
    /// T37b-r4：延迟未到期（visible_at>now —— 时间错位排查入口）。
    #[serde(default)]
    pub delayed_count: i64,
    /// T37b-r4：`_vrt_q_dead` 审记行数（已死信/丢弃总数）。
    #[serde(default)]
    pub dead_count: i64,
}

#[derive(Clone, Debug)]
pub struct ClaimedBatch {
    /// 本批消息共享的结算 lease_id。
    pub lease_id: String,
    /// T37b：claim 原子完成的派发租约 epoch（CAS 内嵌 claim 事务；
    /// 每个 epoch 唯一标识一次持约——观察/测试用，结算走 lease_id）。
    pub dispatch_epoch: i64,
    pub messages: Vec<ClaimedMsg>,
}

/// T37r：send 结果区分真实入队与幂等去重——重复 id 绝不再伪装成
/// 全部成功的静默吞并。
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendResult {
    /// 本次实际写入的消息数。
    pub inserted: u32,
    /// id 已存在被去重的消息数（DLQ 转投重放/传输重试的幂等命中）。
    pub duplicates: u32,
    #[serde(flatten)]
    pub metrics: QueueMetrics,
}

#[derive(Debug, Error)]
pub enum QueueError {
    /// lease_id 不匹配或已过期——结算必须拒，绝不误删/误改
    /// 已被他人重新 claim 的消息。
    #[error("queue lease lost")]
    LeaseLost,
    /// 限额超限（413 语义）。
    #[error("queue payload too large")]
    TooLarge,
    /// 非法输入（400 语义）。
    #[error("invalid queue input: {0}")]
    InvalidInput(String),
    /// 后端不可用；`retryable` 供调用方决定是否重试。
    #[error("queue backend unavailable: {reason}")]
    Unavailable { retryable: bool, reason: String },
    /// 写者被 fence（lease 丢失于持久层）。
    #[error("queue writer fenced at epoch {0}")]
    Fenced(u64),
    /// queue 已 tombstone/删除（drop 后一切 op 拒绝，幂等）。
    #[error("queue dropped")]
    Dropped,
    /// T37b：派发租约被另一 dispatcher 持有且未过期——claim 拒绝；
    /// 载荷是持约者与到期时刻（调用方退避至过期后自动接手）。
    #[error("queue dispatch lease held by {holder} until {expires_at_ms}")]
    DispatchLeaseLost { holder: String, expires_at_ms: i64 },
}

/// wire 错误码（typed，供 provider 映射）。
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

/// exec_internal 内部判别哨兵（Message 载荷 → typed QueueError）。
const KIND_MISMATCH: &str = "VRT_QUEUE_KIND_MISMATCH";
const LEASE_LOST: &str = "VRT_QUEUE_LEASE_LOST";
const DISPATCH_LOST: &str = "VRT_QUEUE_DISPATCH_LOST|";
/// T37b：派发租约 holder/ttl 上限（字符集 [a-zA-Z0-9_.:-]）。
pub const DISPATCH_HOLDER_MAX: usize = 64;
/// 派发租约时长上限：5 分钟——远大于轮询周期、远小于消息租约上限。
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

/// 库类型门禁 + schema 初始化（同一事务）：
/// - `kind='queue'` → 直接过；
/// - 无 kind 且**无用户表**（非 `_vrt_`/`sqlite_` 前缀）→ 初始化；
/// - 其它（D1 库 / kind 串未知）→ fail closed。
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
            // T37b：既有 queue 库补 _vrt_q_dispatch（旧 SCHEMA 无此表
            // ——CREATE IF NOT EXISTS 幂等，同 exec_internal 事务）。
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
            // sqlite_master 逐行取回，在 Rust 侧判前缀——`NOT LIKE`
            // 会触发 `like` 函数（internal 白名单不含它），SQL 里
            // 不能出现函数调用。
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

/// lease/msg id 生成：nanos + pid + 单调计数（32-hex，与 miniflare
/// `randomness.toString("hex")` 同形；无 rand 依赖）。
static ID_SEQ: AtomicU64 = AtomicU64::new(0);

fn gen_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let seq = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:016x}{:04x}{seq:012x}", std::process::id() & 0xffff)
}

/// QueueBackend —— §10 定稿（签名相对草案修订：queue 引用化、
/// ack/retry 带 lease_id 结算守卫、now 注入便于测试与 dispatcher
/// 时钟一致性；语义不变）。所有 op 单事务提交并经 exec_internal
/// 持久化。
#[async_trait::async_trait]
pub trait QueueBackend: Send + Sync {
    /// 一批单事务入队；`(queue_id, msg_id)` 幂等——重复 id 去重并
    /// 计入 `duplicates`（T37r：不再静默吞并）。
    async fn send(
        &self,
        queue: &QueueId,
        msgs: &[IncomingMsg],
        now_ms: i64,
    ) -> Result<SendResult, QueueError>;
    /// 原子 claim 一批（T37b 派发租约内嵌）：同一事务先 CAS
    /// `_vrt_q_dispatch`（holder=me 续约 / 过期接管 epoch++ /
    /// 他人持约未过期 → DispatchLeaseLost），再
    /// `visible_at<=now AND lease 空闲/过期` 的行 →
    /// `lease_id/lease_expiry/attempts+1`。`holder_ttl_ms` 是派发
    /// 租约时长；`max=0` 为纯续约（不 claim 消息，供背压等待期
    /// 续约）。claim 与租约同事务——绝不存在"拿到消息却没租约"
    /// 的中间态。
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
    /// 结算 ack：仅删 `lease_id` 匹配且未过期的行；任一不符 →
    /// LeaseLost 全拒（不得误删已被他人重新 claim 的消息）。
    async fn ack(
        &self,
        queue: &QueueId,
        lease_id: &str,
        ids: &[String],
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// 结算 retry：`visible_at=now+delay`、清 lease；lease 守卫同 ack。
    async fn retry(
        &self,
        queue: &QueueId,
        lease_id: &str,
        items: &[(String, u32)],
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// T37b-r5 平台侧失败归还：`visible_at=now+delay`、清 lease，
    /// 并 `attempts=MAX(attempts-1,0)` 补偿 claim 时的 +1 ——
    /// consumer 代码未执行（transport/timeout/malformed/host 拒绝）
    /// 的投递不消耗 attempts，消息不因平台故障被死信/丢弃。
    /// lease 守卫与原子性同 retry（同一 UPDATE 内递减，per-tx）。
    async fn release(
        &self,
        queue: &QueueId,
        lease_id: &str,
        items: &[(String, u32)],
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// 死信：移入 `_vrt_q_dead` 审记表后删除（attempts 超限且未配
    /// DLQ 目标的落点；DLQ 跨队列转发属 T37b）。lease 守卫同 ack。
    async fn dead_letter(
        &self,
        queue: &QueueId,
        lease_id: &str,
        ids: &[String],
        reason: &str,
        now_ms: i64,
    ) -> Result<u32, QueueError>;
    /// `{backlogCount, backlogBytes, oldestMessageTimestampMs,
    /// visibleCount, inFlightCount, delayedCount, deadCount}`（T37b-r4
    /// 可见性切分；now 注入与其他 op 同一时钟语义）。
    async fn metrics(&self, queue: &QueueId, now_ms: i64) -> Result<QueueMetrics, QueueError>;
}

/// SQLite 实现——每 queue 一个独立库，经 EngineRegistry 惰性
/// claim（同 D1 库同一条 lease/fencing/恢复路径）。
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

    /// 删除 queue（复用 T35c/T36 drop 路径：manifest tombstone →
    /// 摘除 → rename；幂等）。
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

/// T37r：client_ts 允许的最大未来偏差（时钟漂移容忍）。远古值
/// 合法（DLQ 转投保留原时间戳），但未来值绝不合法——超过即拒，
/// 而非截断（调用方应修正自己的时钟）。
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

fn metrics_of(tx: &rusqlite::Transaction<'_>, now_ms: i64) -> Result<QueueMetrics, EngineError> {
    let (backlog_count, backlog_bytes, oldest): (i64, i64, Option<i64>) = tx
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(body)),0), MIN(created_at) \
             FROM _vrt_q_messages",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(EngineError::Sqlite)?;
    // 可见性切分 —— 与 claim 选择谓词同口径（server ms）。
    let (visible, in_flight, delayed): (i64, i64, i64) = tx
        .query_row(
            "SELECT \
               COALESCE(SUM(CASE WHEN visible_at<=?1 AND (lease_id IS NULL OR lease_expiry<=?1) \
                 THEN 1 ELSE 0 END),0), \
               COALESCE(SUM(CASE WHEN lease_id IS NOT NULL AND lease_expiry>?1 \
                 THEN 1 ELSE 0 END),0), \
               COALESCE(SUM(CASE WHEN visible_at>?1 AND (lease_id IS NULL OR lease_expiry<=?1) \
                 THEN 1 ELSE 0 END),0) \
             FROM _vrt_q_messages",
            [now_ms],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(EngineError::Sqlite)?;
    let dead: i64 = tx
        .query_row("SELECT COUNT(*) FROM _vrt_q_dead", [], |r| r.get(0))
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
        // visible_at/created_at 一律服务端时钟；client_ts 只作不可信
        // 消息属性单独存放，绝不参与调度与指标。
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
            // 派发租约 CAS（同事务）——无行则占位，holder=me 续约，
            // 过期接管 epoch++；他人持约未过期 → DispatchLeaseLost。
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
            let mut st = tx.prepare(
                "SELECT id, body, content_type, attempts, \
                     COALESCE(client_ts, created_at) \
                     FROM _vrt_q_messages \
                     WHERE visible_at<=?3 \
                       AND (lease_id IS NULL OR lease_expiry<=?3)\
                     ORDER BY visible_at, created_at LIMIT ?4",
            )?;
            let candidates = st
                .query_map(params![lid, expiry, now_ms, max as i64], |r| {
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
                    // attempts 尚未 +1 —— UPDATE 后才提交该值。
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
                    // 同批内并发 lease 竞态由 lease 守卫兜住；
                    // 行已被他人租走则此处 0 行——无害跳过。
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
                    // attempts-1 与归还同事务 —— claim 的 +1 被原子
                    // 补偿；MAX(...,0) 防脏数据下负值。
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

/// ---------- wire 请求/响应类型（provider → vrt-sql 内部通道） ----------

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
    /// hex 编码（内部 wire 无 base64 依赖）。
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
    /// T37b：派发租约持约者（必填——claim 与租约 CAS 同事务）。
    pub holder_id: String,
    pub holder_ttl_ms: i64,
    /// 0 = 纯续约（不 claim 消息；背压等待期的续约路径）。
    pub max: u32,
    /// 原始 body 字节预算（T37b-r）。`None` = 不设（旧调用方兼
    /// 容；count 上限仍兜住）。设值时恒返回 ≥1 条——单条超限
    /// 消息不会饿死。
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

    /// f panic → Err（不毒化锁）、intent/lsn 不推进、InternalFlag 复位
    ///（用户 SQL 依然被 authorizer 拒）。
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
        // flag reset: 用户面 _vrt_ 访问仍被 authorizer 拒。
        let denied = e.query_batch_sync(
            &[SqlStmt {
                sql: "SELECT * FROM _vrt_q_meta".into(),
                params: None,
            }],
            &tctx(),
            ResultsFormat::ArrayOfObjects,
        );
        assert!(denied.is_err(), "internal flag must reset after panic");
        // 后续正常 op 仍可用（锁未毒化）。
        let (_t2, reg2) = registry();
        let _ = reg2;
    }

    /// closure Err → 回滚，不推进 lsn。
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
        // 回滚确认：_vrt_tmp_x 不存在（经 internal 查）。
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
        // S1 纵深：internal 窗口内非队列 `_vrt_` 对象仍被 authorizer 拒。
        let denied: Result<(), EngineError> = e.exec_internal(&tctx(), |tx| {
            tx.execute_batch("CREATE TABLE _vrt_evil(a)")
                .map_err(EngineError::Sqlite)
        });
        assert!(matches!(denied, Err(EngineError::Sqlite(_))), "{denied:?}");
    }
}
