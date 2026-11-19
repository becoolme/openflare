//! vrt-sql 服务层（T34）：服务间认证、配置、有界并发与指标。
//!
//! 认证模型：只有 provider 能调用 vrt-sql。双方共享 0600 secret 文件；
//! 客户端对
//!   `"{ts}|{method}|{path}?{query}|{sha256hex(body)}|{epoch}|{seq}"`
//! 做 HMAC-SHA256，放 `x-vrt-sql-auth: v1:<ts>:<mac-hex>`。
//! 覆盖 epoch/seq/query 是为了让中间人无法改写 resultsFormat 与
//! 去重语义；ts 窗口 ±30s（容忍时钟漂移），超窗拒绝，常量时间比较。
//! 验签通过之前绝不读取 `x-vrt-epoch`/`x-vrt-seq`——这两个值只在
//! 认证后才被采信（wire.rs `trusted_ctx_from_headers`）。
//!
//! 重放：窗口内同签名重放的写由 `_vrt_dedup`(epoch,seq) 兜住；读
//! 重放可接受（文档已写明）。body 长度先验后读，超限直接拒——防
//! 未认证大包耗内存；认证失败计 `outcome="unauthorized"`。

use axum::http::{HeaderMap, Method};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

/// 认证请求头。
pub const AUTH_HEADER: &str = "x-vrt-sql-auth";
/// 认证时钟容差（秒）——±30s。
pub const AUTH_SKEW_SECS: i64 = 30;

type HmacSha256 = Hmac<Sha256>;

/// 服务配置（JSON/TOML，deny_unknown_fields）。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceConfig {
    /// 数据库根目录（0700，内部派生 sha256 shard）。
    pub db_root: PathBuf,
    /// unix socket：`{unixDir}/vrt-sql.sock`。
    #[serde(default)]
    pub unix_dir: Option<PathBuf>,
    /// TCP 监听 `ip:port`（内部网络）。unixDir 与 listenTcp 恰设一个。
    #[serde(default)]
    pub listen_tcp: Option<String>,
    /// bookmark HMAC key 文件（0600，≤4KiB）。
    pub bookmark_secret_file: PathBuf,
    /// 服务间认证 secret 文件（0600，≤4KiB；provider 持有同一份）。
    pub auth_secret_file: PathBuf,
    /// 内部 metrics TCP 监听；不公开。
    #[serde(default)]
    pub metrics_listen: Option<String>,
    /// 全局在途并发上限（try_acquire，超限 429）。
    #[serde(default = "default_global_concurrency")]
    pub global_concurrency: usize,
    /// 单请求事务预算（ms）。
    #[serde(default = "default_operation_timeout_ms")]
    pub operation_timeout_ms: u64,
    /// 优雅退出排空上限（ms）。
    #[serde(default = "default_drain_ms")]
    pub drain_timeout_ms: u64,
    /// 请求体上限（字节）——读 body 前先验。
    #[serde(default = "default_body_max")]
    pub body_max_bytes: usize,
    /// T36 §4：对象存储持久化。**必填**（fail closed：缺失 → 启动
    /// 失败）。`backend="none"` 仅在 `allowDevBackend=true` 时允许（RV-R72）。
    pub persistence: PersistenceConfig,
}

/// T36 `[persistence]`（§4）。键名 camelCase 与 ServiceConfig 一致。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PersistenceConfig {
    /// `fs`（standalone 默认）| `s3` | `none`（测试/dev，WARN）。
    pub backend: String,
    /// RV-R72：显式 dev 开关。`backend="none"`（无复制、D1 写入只做本地
    /// fsync）与 s3 非 loopback `http://` endpoint 仅在此为 true 时允许。
    /// 生产 overlay 禁止出现该键（`vrt-k8s-check` 校验）。
    #[serde(default)]
    pub allow_dev_backend: bool,
    /// RV-R66：`backend="fs"` 的持久层根与本地库 `dbRoot` 位于同一块
    /// 设备（`st_dev` 相同）时，"持久层"与本地库同生共死——D1/Queue
    /// 实际单份。默认 false → 启动 fail closed
    /// （[`check_fs_durability_distinct`]）；仅 dev/internal-test 显式置
    /// true 以承认"无灾备"。生产 overlay 禁止出现（`vrt-k8s-check`）。
    #[serde(default)]
    pub allow_same_device_durability: bool,
    /// `backend="fs"` 必填。
    #[serde(default)]
    pub fs: Option<FsPersistence>,
    /// `backend="s3"` 必填。
    #[serde(default)]
    pub s3: Option<S3Persistence>,
    /// lease 时长（ms）。
    #[serde(default = "default_lease_ms")]
    pub lease_ms: u64,
    /// 时钟漂移安全边际（ms）：lease 仅在 `expiry-margin > now`
    /// 时视作可用（§4.3 读写同一门槛）。
    #[serde(default = "default_margin_ms")]
    pub clock_skew_margin_ms: u64,
    /// 写者 holder 标识；缺省 = `hostname:pid:start_nanos`。
    #[serde(default)]
    pub holder: Option<String>,
    /// 每 N 个已持久化提交做一次 checkpoint/snapshot（0=禁用）。
    #[serde(default = "default_ckpt_commits")]
    pub checkpoint_every_commits: u64,
    /// tombstone/段保留窗（ms），GC 惰性执行。
    #[serde(default = "default_retention_ms")]
    pub retention_ms: u64,
    /// 快照字节上限（T36b）：超限库靠段链恢复（成本随链长线性）。
    /// 默认 256MiB——覆盖常规 D1 库量级；s3 后端上传在内存中
    /// 进行，校验强制 ≤256MiB（fail closed）；fs 流式不受内存约束。
    #[serde(default = "default_snapshot_max_bytes")]
    pub snapshot_max_bytes: u64,
    /// MIG-9：启动自检重试总预算（ms）。瞬时对端未就绪（R2/DNS/
    /// 网络慢启动）在预算内指数退避；预算尽仍败 → 进程退出非 0。
    /// 认证类/非重试错误不受预算控制，立即 fail closed。
    /// 须 < K8s startupProbe 的 periodSeconds×failureThreshold。
    #[serde(default = "default_self_check_budget_ms")]
    pub self_check_budget_ms: u64,
    /// MIG-9：首次退避（ms）。
    #[serde(default = "default_self_check_initial_backoff_ms")]
    pub self_check_initial_backoff_ms: u64,
    /// MIG-9：退避上限（ms），×2 增长到此封顶。
    #[serde(default = "default_self_check_max_backoff_ms")]
    pub self_check_max_backoff_ms: u64,
}

/// RV-R66：同设备持久层的 typed 启动错误（fail closed）。
#[derive(Debug, PartialEq, Eq)]
pub enum DurabilityError {
    /// persistence root 与 dbRoot 在同一 `st_dev`：无独立副本。
    SameDevice {
        db_root: PathBuf,
        persist_root: PathBuf,
    },
    /// 无法确认两路径所在设备（stat 失败）——无法证明独立即拒绝。
    Unverifiable(String),
}

impl std::fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SameDevice {
                db_root,
                persist_root,
            } => write!(
                f,
                "VRT_SQL_DURABILITY_SAME_DEVICE: persistence fs root {} shares a device with dbRoot {} \
                 (single copy; use backend=\"s3\", or set persistence.allowSameDeviceDurability=true for dev only)",
                persist_root.display(),
                db_root.display()
            ),
            Self::Unverifiable(m) => write!(f, "VRT_SQL_DURABILITY_UNVERIFIABLE: {m}"),
        }
    }
}

impl std::error::Error for DurabilityError {}

/// 最近的已存在祖先（含自身）——root 目录可能尚未创建。
fn nearest_existing(p: &std::path::Path) -> Option<PathBuf> {
    let mut cur = Some(p);
    while let Some(c) = cur {
        if c.exists() {
            return Some(c.to_path_buf());
        }
        cur = c.parent();
    }
    None
}

/// RV-R66：`fs` 持久层必须与本地库不同设备，除非显式 dev 豁免。
/// 豁免生效时返回 `Ok(true)`（调用方须告警"无灾备"），独立设备
/// `Ok(false)`。无法确认设备 → `Err`（fail closed，不当作独立）。
#[cfg(unix)]
pub fn check_fs_durability_distinct(
    db_root: &std::path::Path,
    persist_root: &std::path::Path,
    allow_same_device: bool,
) -> Result<bool, DurabilityError> {
    use std::os::unix::fs::MetadataExt;
    let dev = |p: &std::path::Path| -> Result<u64, DurabilityError> {
        let anc = nearest_existing(p)
            .ok_or_else(|| DurabilityError::Unverifiable("no existing ancestor".into()))?;
        std::fs::metadata(&anc)
            .map(|m| m.dev())
            .map_err(|e| DurabilityError::Unverifiable(format!("stat: {e}")))
    };
    let (a, b) = (dev(db_root)?, dev(persist_root)?);
    if a != b {
        return Ok(false);
    }
    if allow_same_device {
        return Ok(true);
    }
    Err(DurabilityError::SameDevice {
        db_root: db_root.to_path_buf(),
        persist_root: persist_root.to_path_buf(),
    })
}

/// `[persistence.fs]`。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FsPersistence {
    /// 对象根目录（独立卷建议）。
    pub root: PathBuf,
}

/// `[persistence.s3]`。凭据走 0600 文件（绝不内联 secret）。
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3Persistence {
    /// Base endpoint `https://host[:port]`（path-style）。
    pub endpoint: String,
    /// Signing region。
    pub region: String,
    /// Physical bucket。
    pub bucket: String,
    /// access key 文件（0600）。
    pub access_key_file: PathBuf,
    /// secret key 文件（0600）。
    pub secret_key_file: PathBuf,
    /// 后端支持 S3 条件写（If-Match/If-None-Match）。false → CAS
    /// fail closed（§12 未证实项：R2/MinIO 兼容性需真机核实）。
    #[serde(default = "default_conditional_writes")]
    pub conditional_writes: bool,
    /// 请求超时（ms）。
    #[serde(default = "default_s3_timeout_ms")]
    pub timeout_ms: u64,
    /// 对象 key 前缀（多部署共 bucket 时隔离）。
    #[serde(default)]
    pub prefix: String,
}

fn default_lease_ms() -> u64 {
    15_000
}
fn default_margin_ms() -> u64 {
    500
}
/// T36b 快照上限默认：256MiB（s3 内存上传的硬顶；fs 流式可放宽）。
fn default_snapshot_max_bytes() -> u64 {
    256 * 1024 * 1024
}
/// s3 后端快照字节硬上限——上传路径把对象读进内存签名。
const S3_SNAPSHOT_MAX_BYTES: u64 = 256 * 1024 * 1024;
fn default_ckpt_commits() -> u64 {
    64
}
fn default_retention_ms() -> u64 {
    86_400_000
}
fn default_conditional_writes() -> bool {
    true
}
fn default_s3_timeout_ms() -> u64 {
    10_000
}
fn default_self_check_budget_ms() -> u64 {
    60_000
}
fn default_self_check_initial_backoff_ms() -> u64 {
    500
}
fn default_self_check_max_backoff_ms() -> u64 {
    8_000
}

fn default_global_concurrency() -> usize {
    64
}
fn default_operation_timeout_ms() -> u64 {
    5_000
}
fn default_drain_ms() -> u64 {
    30_000
}
fn default_body_max() -> usize {
    1024 * 1024
}

impl ServiceConfig {
    /// fail-closed 校验。
    pub fn validate(&self) -> Result<(), &'static str> {
        match (self.unix_dir.is_some(), self.listen_tcp.is_some()) {
            (true, false) | (false, true) => {}
            _ => return Err("unixDir|listenTcp: exactly one required"),
        }
        if self.global_concurrency == 0 || self.operation_timeout_ms == 0 {
            return Err("concurrency|timeout");
        }
        if self.body_max_bytes == 0 || self.body_max_bytes > 64 * 1024 * 1024 {
            return Err("bodyMaxBytes");
        }
        if let Some(addr) = &self.listen_tcp {
            if addr.parse::<std::net::SocketAddr>().is_err() {
                return Err("listenTcp");
            }
        }
        if let Some(addr) = &self.metrics_listen {
            if addr.parse::<std::net::SocketAddr>().is_err() {
                return Err("metricsListen");
            }
        }
        // T36 persistence 校验（fail closed）。
        match self.persistence.backend.as_str() {
            "none" => {
                if !self.persistence.allow_dev_backend {
                    return Err(
                        "persistence.backend=none requires persistence.allowDevBackend=true (dev only)",
                    );
                }
            }
            "fs" => {
                if self.persistence.fs.is_none() {
                    return Err("persistence.fs.root required");
                }
            }
            "s3" => {
                let Some(s3) = &self.persistence.s3 else {
                    return Err("persistence.s3 required");
                };
                if s3.endpoint.is_empty() || s3.bucket.is_empty() || s3.region.is_empty() {
                    return Err("persistence.s3 endpoint/bucket/region");
                }
                // RV-R72：endpoint 解析校验（https；http 仅 loopback/dev）。
                if vrt_objstore::endpoint::validate_object_store_endpoint(
                    &s3.endpoint,
                    self.persistence.allow_dev_backend,
                )
                .is_err()
                {
                    return Err("persistence.s3.endpoint must be https (http only loopback, <svc>.<ns>.svc:port, or allowDevBackend)");
                }
                // T36b：s3 快照上传为内存路径——超限 fail closed。
                if self.persistence.snapshot_max_bytes > S3_SNAPSHOT_MAX_BYTES {
                    return Err("persistence.snapshotMaxBytes exceeds s3 in-memory cap (256MiB)");
                }
            }
            _ => return Err("persistence.backend must be fs|s3|none"),
        }
        if self.persistence.lease_ms <= self.persistence.clock_skew_margin_ms * 4 {
            // margin 必须显著小于 lease（默认 500ms vs 15s）。
            return Err("persistence.leaseMs too small vs clockSkewMarginMs");
        }
        // MIG-9：自检重试参数边界——预算/退避必须为正且自洽；
        // 预算封顶 10 分钟（更长应靠 pod restart policy，不是
        // 单次启动自检）。
        let p = &self.persistence;
        if p.self_check_initial_backoff_ms == 0 || p.self_check_max_backoff_ms == 0 {
            return Err("persistence.selfCheck* backoff must be > 0");
        }
        if p.self_check_max_backoff_ms < p.self_check_initial_backoff_ms {
            return Err("persistence.selfCheckMaxBackoffMs < selfCheckInitialBackoffMs");
        }
        if p.self_check_budget_ms == 0 || p.self_check_budget_ms > 600_000 {
            return Err("persistence.selfCheckBudgetMs out of range (0, 600000]");
        }
        Ok(())
    }
}

/// 0600 secret 文件加载（RV-R72：委托 `vrt_securefs::load_service_secret`，
/// 三个 binary 共用同一规范化：仅去尾部一个换行、内部空白拒绝、最小长度、
/// 占位值拒绝）。绝不打印内容。
pub fn load_secret_0600(
    path: &std::path::Path,
    purpose: vrt_securefs::SecretPurpose,
) -> io::Result<Vec<u8>> {
    Ok(vrt_securefs::load_service_secret(path, purpose)?.into_bytes())
}

/// 签名 token，双向共用：provider 调 [`ServiceAuth::sign`]，vrt-sql
/// 调 [`ServiceAuth::verify`]。
#[derive(Clone)]
pub struct ServiceAuth {
    secret: Vec<u8>,
}

impl std::fmt::Debug for ServiceAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAuth").finish_non_exhaustive()
    }
}

fn mac(secret: &[u8], msg: &str) -> [u8; 32] {
    let mut h = <HmacSha256 as Mac>::new_from_slice(secret).expect("hmac any len");
    h.update(msg.as_bytes());
    h.finalize().into_bytes().into()
}

fn body_sha_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

impl ServiceAuth {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret }
    }

    /// 被签名消息（epoch/seq 缺省为 `-`）。
    fn message(
        ts: i64,
        method: &str,
        path_query: &str,
        body: &[u8],
        epoch: &str,
        seq: &str,
    ) -> String {
        format!(
            "{ts}|{method}|{path_query}|{}|{epoch}|{seq}",
            body_sha_hex(body)
        )
    }

    /// 客户端签名：`v1:<ts>:<mac64>`。`epoch`/`seq` 为将发送的头值
    /// （或 None），必须与实际上送一致——签名覆盖它们。
    pub fn sign(
        &self,
        method: &str,
        path_query: &str,
        body: &[u8],
        epoch: Option<u64>,
        seq: Option<u64>,
    ) -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let e = epoch.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        let s = seq.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        let m = mac(
            &self.secret,
            &Self::message(ts, method, path_query, body, &e, &s),
        );
        format!("v1:{ts}:{}", hex::encode(m))
    }

    /// 服务端验签：格式/时钟/HMAC 任一不符 → false（fail closed）。
    /// `epoch`/`seq` 传原始头值（缺失 → "-"），签名之后由调用方解析。
    pub fn verify(
        &self,
        headers: &HeaderMap,
        method: &Method,
        path_query: &str,
        body: &[u8],
    ) -> bool {
        let Some(v) = headers.get(AUTH_HEADER).and_then(|v| v.to_str().ok()) else {
            return false;
        };
        let mut it = v.splitn(3, ':');
        let (Some("v1"), Some(ts_s), Some(mac_hex)) = (it.next(), it.next(), it.next()) else {
            return false;
        };
        let Ok(ts) = ts_s.parse::<i64>() else {
            return false;
        };
        let Ok(expected) = hex::decode(mac_hex) else {
            return false;
        };
        if expected.len() != 32 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        if (now - ts).abs() > AUTH_SKEW_SECS {
            return false;
        }
        let epoch = headers
            .get(crate::wire::EPOCH_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        let seq = headers
            .get(crate::wire::SEQ_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        let actual = mac(
            &self.secret,
            &Self::message(ts, method.as_str(), path_query, body, epoch, seq),
        );
        let mut diff = 0u8;
        for (a, b) in actual.iter().zip(expected.iter()) {
            diff |= a ^ b;
        }
        diff == 0
    }
}

/// 有界 databaseId → metrics label：≤256 个活跃 id，其余折叠
/// `_overflow`（workers-runtime-metrics.md 基数规则；非法值 `_unknown`）。
#[derive(Default)]
pub struct DbLabels {
    seen: Mutex<std::collections::HashSet<String>>,
}

impl DbLabels {
    const CAP: usize = 256;

    pub fn label(&self, database_id: &str) -> String {
        let valid = database_id.len() == 26
            && database_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        if !valid {
            return "_unknown".to_string();
        }
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        if seen.contains(database_id) || seen.len() < Self::CAP {
            seen.insert(database_id.to_string());
            return database_id.to_string();
        }
        "_overflow".to_string()
    }
}
