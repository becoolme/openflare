//! 对象存储持久化面（T36 / §4）：manifest CAS lease + epoch fencing +
//! ack 前段持久化 + 接管/恢复 + tombstone 删除 + 段 GC。
//!
//! 所有方法同步签名（内部 `block_on_store` 桥接 async ObjectStore），
//! 由引擎在单写者锁内调用——顺序天然串行。

use crate::clock::Clock;
use crate::paths;
use crate::seg;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vrt_objstore::{ObjectStore, StoreError};

/// 持久化配置（`[persistence]` 的运行时形态）。
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// lease 时长。
    pub lease_ms: u64,
    /// 时钟漂移安全边际：lease 只有在 `expires - margin > now` 时
    /// 视作可用（读与写同门槛；§4.3）。
    pub clock_skew_margin_ms: u64,
    /// 本实例持有者标识（进程级唯一）。
    pub holder: String,
    /// 每 N 个已持久化提交做一次 checkpoint/snapshot（0=禁用）。
    pub checkpoint_every_commits: u64,
    /// tombstone/段对象的保留窗（ms）；GC 惰性执行。
    pub retention_ms: u64,
    /// 快照对象上限（防御闸）。
    pub snapshot_max_bytes: u64,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            lease_ms: 15_000,
            clock_skew_margin_ms: 500,
            holder: String::new(),
            checkpoint_every_commits: 64,
            retention_ms: 86_400_000,
            snapshot_max_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

/// manifest 对象 `db/{id}/manifest` 的 typed serde（v1）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// 格式版本（=1）。
    pub v: u32,
    /// 当前写者 epoch（每次接管 +1）。
    pub epoch: u64,
    /// 已确认的最后提交序号（跨 epoch 单调，不回退）。
    pub lsn: u64,
    /// 写者标识。
    pub holder: String,
    /// lease 到期（unix ms）。
    pub expires_at_ms: u64,
    /// 删除墓碑：任何写者续租/CAS 都会失败 → 全员 fenced。
    #[serde(default)]
    pub tombstone: bool,
    /// 最新快照基线（可为空 = 仅靠段链，lsn 必须从 1 连续）。
    #[serde(default)]
    pub snapshot: Option<SnapshotRef>,
    /// epoch 链：`{epoch, first_lsn}`——lsn → 所属 epoch 键空间的
    /// 确定性映射（孤儿段永不进链）。按 epoch 升序。
    #[serde(default)]
    pub chain: Vec<EpochSpan>,
}

/// 快照引用。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub key: String,
    /// 快照覆盖到的 lsn（含）。
    pub lsn: u64,
    pub sha256: String,
    pub size: u64,
}

/// epoch 链条目：该 epoch 写者的段从 `first_lsn` 开始。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochSpan {
    pub epoch: u64,
    pub first_lsn: u64,
}

/// 引擎侧持有的 lease 状态（在 Inner 锁内维护）。
#[derive(Debug, Clone)]
pub struct ManifestHandle {
    pub epoch: u64,
    /// 已持久化的 lsn（== manifest.lsn，也镜像 engine.commit_lsn）。
    pub lsn: u64,
    pub expires_at_ms: u64,
    /// 最近一次 CAS 成功的 etag（续租/推进都用它做 put_if_match）。
    pub etag: String,
    pub holder: String,
    pub snapshot: Option<SnapshotRef>,
    pub chain: Vec<EpochSpan>,
    /// 距上次 checkpoint 的已持久化提交数。
    pub commits_since_ckpt: u64,
    /// 续租失败但 lease 仍在 `expiry-margin` 内 → degraded 读放行。
    pub degraded: bool,
}

impl ManifestHandle {
    /// lease 此刻是否可用（含 margin；§4.3 读/写统一门槛）。
    pub fn usable(&self, now_ms: u64, margin_ms: u64) -> bool {
        holder_usable(self.expires_at_ms, now_ms, margin_ms)
    }
    /// 该续租了（提前量 = lease 1/4，给失败重试留窗）。
    pub fn due_renew(&self, now_ms: u64, margin_ms: u64, lease_ms: u64) -> bool {
        now_ms >= self.expires_at_ms.saturating_sub(margin_ms + lease_ms / 4)
    }
    fn to_manifest(&self, tombstone: bool) -> Manifest {
        Manifest {
            v: 1,
            epoch: self.epoch,
            lsn: self.lsn,
            holder: self.holder.clone(),
            expires_at_ms: self.expires_at_ms,
            tombstone,
            snapshot: self.snapshot.clone(),
            chain: self.chain.clone(),
        }
    }
    fn from_manifest(m: &Manifest, etag: String) -> Self {
        Self {
            epoch: m.epoch,
            lsn: m.lsn,
            expires_at_ms: m.expires_at_ms,
            etag,
            holder: m.holder.clone(),
            snapshot: m.snapshot.clone(),
            chain: m.chain.clone(),
            commits_since_ckpt: 0,
            degraded: false,
        }
    }
}

/// `.vrt-lsn` 进度标记（JSON，v1）——本地文件状态已达
/// `manifest.lsn` 的耐久声明。
/// - `file_frames`：写标记时 `-wal` 的**物理**帧数（含未提交
///   死帧尾）。认领时本地 wal 全链解析出的帧数必须与之相等。
/// - `intent=true`：commit 前的意图标记——永不作信任判定
///   （崩溃窗口的未 ack 提交会留下 intent lsn > manifest.lsn
///   或 intent 标记本身，两者都强制走恢复）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sidecar {
    pub v: u32,
    pub lsn: u64,
    #[serde(default)]
    pub file_frames: u64,
    #[serde(default)]
    pub intent: bool,
}

impl Sidecar {
    /// 标记是否允许信任本地文件为 manifest `lsn` 状态。
    fn trusted(&self, manifest_lsn: u64) -> bool {
        self.v == 1 && !self.intent && self.lsn == manifest_lsn
    }
}

/// 恢复计划：快照 + 待重放段区间 + epoch 链。
#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub snapshot: Option<SnapshotRef>,
    /// 恢复到该 lsn（含）。
    pub up_to_lsn: u64,
    pub chain: Vec<EpochSpan>,
}

/// claim 结果。
#[derive(Debug)]
pub enum ClaimOutcome {
    /// 拿到写权：lease 状态 + 是否需要先恢复本地文件。
    Ready {
        handle: ManifestHandle,
        restore: Option<RestorePlan>,
    },
    /// manifest 被另一存活写者持有（`expiry-margin` 内）——拒开。
    HeldByOther { expires_at_ms: u64 },
    /// manifest.tombstone —— 库已删除。
    Tombstoned,
}

/// 持久化错误。
#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("manifest/object corrupt: {0}")]
    Corrupt(String),
    #[error("lease held by another writer")]
    HeldByOther,
    #[error("database tombstoned")]
    Tombstoned,
    /// 快照超 `snapshot_max_bytes`（先 stat 判大小，不读文件）。
    /// 恢复仍可用段链（代价随链长线性增长，见 §4.5）。
    #[error("snapshot over cap: {0} bytes")]
    SnapshotTooLarge(u64),
    /// CAS 竞争失败（lease 丢失）——调用方必须 fence。
    #[error("lease lost")]
    Fenced,
    /// RV-R31 #2：本地落盘 I/O 失败（sidecar/restore tmp 等）——
    /// 标记承诺写不进盘即本地持久层不可信，向上传播不忽略。
    #[error("local io: {0}")]
    LocalIo(String),
}

/// RV-R66：`gc_orphans` 单次最多删除（含失败尝试）的对象数。
pub const MAX_GC_DELETES_PER_PASS: usize = 256;

/// RV-R66：一次 GC 的结果（可观测，不吞错）。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcReport {
    pub deleted: usize,
    pub failed: usize,
    /// 超过单次上限、留给下一轮的待删对象数。
    pub deferred: usize,
    pub list_failed: bool,
}

/// MIG-9/MIG-10：自检重试策略与 URL 净化现在住在共享 crate
/// `vrt-objstore::self_check`（vrt-host placement 自检复用同一
/// 原语）——此处 re-export 保持既有调用路径不变。
pub use vrt_objstore::self_check::{sanitize_self_check_error, SelfCheckRetry};

/// MIG-9：一次自检失败是否值得重试——仅 `Unavailable(retryable)`
///（网络/5xx/超时/429）是瞬时未就绪；403/认证类
/// （`retryable=false`）、PreconditionFailed、NotFound、Corrupt、
/// 本地 IO 都不会在 60 s 内自愈，立即 fail closed。
fn self_check_retryable(e: &PersistError) -> bool {
    matches!(
        e,
        PersistError::Store(vrt_objstore::StoreError::Unavailable {
            retryable: true,
            ..
        })
    )
}


/// 持久化面。
pub struct Persister {
    store: Arc<dyn ObjectStore>,
    cfg: PersistConfig,
    clock: Arc<dyn Clock>,
    db_root: PathBuf,
    /// T37r：资源种类——对象键前缀（db/ 或 q/）与本地 root 由
    /// Persister 构造时绑定，同一 store 上两种 kind 各持一个实例。
    kind: crate::paths::ResourceKind,
}

impl std::fmt::Debug for Persister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persister")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

/// RV-R71: message of the `StoreError::Unavailable` produced when a
/// request deadline cuts a store round trip.
pub const DEADLINE_MSG: &str = "deadline exceeded";

/// RV-R71: true when `e` is a request-deadline cut (not a store fault).
pub fn is_deadline(e: &PersistError) -> bool {
    matches!(
        e,
        PersistError::Store(StoreError::Unavailable { reason, .. }) if reason == DEADLINE_MSG
    )
}

fn manifest_key(kind: crate::paths::ResourceKind, id: &str) -> String {
    seg::manifest_key(kind, id)
}

/// lsn → epoch 键空间解析（chain 升序）。
fn epoch_for_lsn(chain: &[EpochSpan], lsn: u64) -> Option<u64> {
    let mut hit = None;
    for span in chain {
        if span.first_lsn <= lsn {
            hit = Some(span.epoch);
        } else {
            break;
        }
    }
    hit
}

impl Persister {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        cfg: PersistConfig,
        clock: Arc<dyn Clock>,
        db_root: PathBuf,
        kind: crate::paths::ResourceKind,
    ) -> Self {
        Self {
            store,
            cfg,
            clock,
            db_root,
            kind,
        }
    }

    /// 本实例绑定的资源种类（对象键前缀 + 本地 root 的决定者）。
    pub fn kind(&self) -> crate::paths::ResourceKind {
        self.kind
    }

    pub fn config(&self) -> &PersistConfig {
        &self.cfg
    }
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
    /// 测试观察口。
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// T36b：启动自检——写/读/删一个唯一 probe 对象，走通完整
    /// CAS 路径（fs：lockfile+flock+tmp→rename+目录 fsync；s3：
    /// If-None-Match 条件写）。失败 = 对象存储不可用/不可写，
    /// 调用方必须拒绝启动（fail closed——优于运行期每次写 503）。
    /// probe key 含 pid+nanos，多实例并发自检互不干扰；内容空。
    pub fn self_check(&self) -> Result<(), PersistError> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let key = format!("probe/self-check-{}-{nonce:x}", std::process::id());
        self.store_put_inm(&key, b"")?;
        let back = self.store_get(&key)?;
        let _ = self.store_delete(&key);
        if back.is_none() {
            return Err(PersistError::Corrupt(
                "self-check probe vanished before delete".into(),
            ));
        }
        Ok(())
    }

    /// MIG-9：进程内有界自检重试——指数退避（initial→×2→cap），
    /// 总预算 `budget` 耗尽返回最后一次错误（调用方退出非 0）。
    /// 认证类/非重试错误（403、PreconditionFailed、Corrupt 等）立即
    /// 返回不重试——瞬时未就绪才值得等，配置错误不会自愈。
    /// 预算门控的是尝试之间的 sleep：总墙钟 ≤ budget + 单次自检耗时。
    /// `log(attempt, elapsed, next_backoff, err)` 由调用方写结构化
    /// 日志——错误文本先经 [`sanitize_self_check_error`] 剥离 URL。
    /// 重试期间进程不应接流量：K8s 里 readiness 由 startupProbe
    /// 门控（端口未绑定即 not-ready），liveness 不判死。
    pub fn self_check_with_retry(
        &self,
        retry: &SelfCheckRetry,
        log: impl FnMut(u32, std::time::Duration, std::time::Duration, &PersistError),
    ) -> Result<(), PersistError> {
        vrt_objstore::self_check::run_with_retry(
            retry,
            self_check_retryable,
            || self.self_check(),
            log,
        )
    }

    fn store_get(&self, key: &str) -> Result<Option<vrt_objstore::StoredObject>, PersistError> {
        Ok(vrt_objstore::block_on_store(self.store.get(key))?)
    }
    fn store_put_inm(&self, key: &str, body: &[u8]) -> Result<String, PersistError> {
        Ok(vrt_objstore::block_on_store(
            self.store.put_if_none_match(key, body),
        )?)
    }
    fn store_put_im(&self, key: &str, etag: &str, body: &[u8]) -> Result<String, PersistError> {
        Ok(vrt_objstore::block_on_store(
            self.store.put_if_match(key, etag, body),
        )?)
    }
    fn store_list(&self, prefix: &str) -> Result<vrt_objstore::ListPage, PersistError> {
        Ok(vrt_objstore::block_on_store(self.store.list(prefix))?)
    }
    fn store_delete(&self, key: &str) -> Result<(), PersistError> {
        Ok(vrt_objstore::block_on_store(self.store.delete(key))?)
    }

    /// RV-R71: drive one store future from sync code, cut at `dl`
    /// (absolute deadline of the inbound request). `None` = legacy
    /// unbounded-by-request behaviour (the store's own per-request
    /// timeout still applies). Expiry maps to the retryable
    /// `Unavailable("deadline exceeded")` that [`is_deadline`] detects.
    fn bridge<F, T>(&self, dl: Option<std::time::Instant>, fut: F) -> Result<T, StoreError>
    where
        F: std::future::Future<Output = Result<T, StoreError>> + Send,
        T: Send,
    {
        match dl {
            None => vrt_objstore::block_on_store(fut),
            Some(d) => vrt_objstore::block_on_store(async move {
                match tokio::time::timeout_at(tokio::time::Instant::from_std(d), fut).await {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::unavailable(true, DEADLINE_MSG)),
                }
            }),
        }
    }
    fn store_get_dl(
        &self,
        key: &str,
        dl: Option<std::time::Instant>,
    ) -> Result<Option<vrt_objstore::StoredObject>, PersistError> {
        Ok(self.bridge(dl, self.store.get(key))?)
    }
    fn store_put_inm_dl(
        &self,
        key: &str,
        body: &[u8],
        dl: Option<std::time::Instant>,
    ) -> Result<String, PersistError> {
        Ok(self.bridge(dl, self.store.put_if_none_match(key, body))?)
    }
    fn store_put_im_dl(
        &self,
        key: &str,
        etag: &str,
        body: &[u8],
        dl: Option<std::time::Instant>,
    ) -> Result<String, PersistError> {
        Ok(self.bridge(dl, self.store.put_if_match(key, etag, body))?)
    }
    fn read_manifest_dl(
        &self,
        id: &str,
        dl: Option<std::time::Instant>,
    ) -> Result<Option<(Manifest, String)>, PersistError> {
        match self.store_get_dl(&manifest_key(self.kind, id), dl)? {
            None => Ok(None),
            Some(obj) => {
                let m: Manifest = serde_json::from_slice(&obj.body)
                    .map_err(|e| PersistError::Corrupt(format!("manifest json: {e}")))?;
                if m.v != 1 {
                    return Err(PersistError::Corrupt(format!("manifest v{}", m.v)));
                }
                Ok(Some((m, obj.meta.etag)))
            }
        }
    }
    fn write_manifest_im_dl(
        &self,
        id: &str,
        etag: &str,
        m: &Manifest,
        dl: Option<std::time::Instant>,
    ) -> Result<String, PersistError> {
        let body = serde_json::to_vec(m).map_err(|e| PersistError::Corrupt(e.to_string()))?;
        self.store_put_im_dl(&manifest_key(self.kind, id), etag, &body, dl)
    }

    fn read_manifest(&self, id: &str) -> Result<Option<(Manifest, String)>, PersistError> {
        match self.store_get(&manifest_key(self.kind, id))? {
            None => Ok(None),
            Some(obj) => {
                let m: Manifest = serde_json::from_slice(&obj.body)
                    .map_err(|e| PersistError::Corrupt(format!("manifest json: {e}")))?;
                if m.v != 1 {
                    return Err(PersistError::Corrupt(format!("manifest v{}", m.v)));
                }
                Ok(Some((m, obj.meta.etag)))
            }
        }
    }

    fn write_manifest_inm(&self, id: &str, m: &Manifest) -> Result<String, PersistError> {
        let body = serde_json::to_vec(m).map_err(|e| PersistError::Corrupt(e.to_string()))?;
        self.store_put_inm(&manifest_key(self.kind, id), &body)
    }
    fn write_manifest_im(
        &self,
        id: &str,
        etag: &str,
        m: &Manifest,
    ) -> Result<String, PersistError> {
        let body = serde_json::to_vec(m).map_err(|e| PersistError::Corrupt(e.to_string()))?;
        self.store_put_im(&manifest_key(self.kind, id), etag, &body)
    }

    /// `.vrt-lsn` sidecar（本地上次 ack 到 manifest 的 lsn +
    /// 当时 WAL 物理帧数——RV-R31 #2 信任判定的全部凭据）。
    fn lsn_sidecar(&self, db_dir: &Path) -> PathBuf {
        db_dir.join(".vrt-lsn")
    }
    fn read_sidecar(&self, db_dir: &Path) -> Option<Sidecar> {
        let raw = std::fs::read(self.lsn_sidecar(db_dir)).ok()?;
        // 旧版 hex 文本 / 截断 JSON 一律 None → 不信任 → 走恢复
        // （升级兼容：旧标记保守失效，多付一次恢复的代价）。
        serde_json::from_slice(&raw).ok()
    }
    /// tmp → fsync 文件 → rename → fsync 父目录（vrt_securefs::
    /// atomic_write），错误向上传播——RV-R31 #2：标记承诺写不进
    /// 盘即本地持久层不可信。
    fn write_sidecar(&self, db_dir: &Path, sc: &Sidecar) -> Result<(), PersistError> {
        let body = serde_json::to_vec(sc)
            .map_err(|e| PersistError::Corrupt(format!("sidecar json: {e}")))?;
        vrt_securefs::atomic_write(&self.lsn_sidecar(db_dir), &body, 0o600)
            .map_err(|e| PersistError::LocalIo(format!("sidecar write: {e}")))
    }

    /// T36：commit 前写"意图 lsn"——sidecar 记录的是**本地文件可能
    /// 包含的、未经 manifest 证实**的最高提交位点。崩溃窗口里本地
    /// 若出现未 ack 提交，sidecar 必然 > manifest.lsn → claim 走
    /// 恢复路径清掉。绝不出现"本地有未持久化数据却被信任"的洞。
    /// RV-R31 #2：写失败向上返回——调用方回滚本批拒 ack。
    pub fn mark_intent(&self, database_id: &str, lsn: u64) -> Result<(), PersistError> {
        self.write_sidecar(
            &self.db_dir(database_id),
            &Sidecar {
                v: 1,
                lsn,
                file_frames: 0,
                intent: true,
            },
        )
    }

    /// RV-R31 #2：本地 `-wal` 全量解析（header + salt + 累计
    /// checksum 全链且**消费到文件末尾**）→ 返回物理帧数；
    /// 任何撕裂/异代际/坏链/非帧对齐尾部 → None（不信任本地）。
    /// 文件不存在/长度 0 → Some(0)。
    fn local_wal_frames(&self, dir: &Path) -> Option<u64> {
        let wal_path = dir.join("db.sqlite-wal");
        let buf = match std::fs::read(&wal_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Some(0),
            Err(_) => return None,
        };
        if buf.is_empty() {
            return Some(0);
        }
        if buf.len() < crate::wal::WAL_HDR {
            return None;
        }
        let hdr = crate::wal::wal_header(&buf[..crate::wal::WAL_HDR])?;
        let ps = hdr.page_size();
        let fsz = (crate::wal::WAL_FRAME_HDR + ps as usize) as u64;
        let body_len = buf.len() as u64 - crate::wal::WAL_HDR as u64;
        if body_len % fsz != 0 {
            return None; // 半帧/撕裂尾
        }
        let seed = (
            u32::from_be_bytes(buf[24..28].try_into().unwrap()),
            u32::from_be_bytes(buf[28..32].try_into().unwrap()),
        );
        let (vend, heads, _) = crate::wal::scan_frames(
            &buf[crate::wal::WAL_HDR..],
            0,
            ps,
            hdr.cksum_be(),
            (hdr.salt1, hdr.salt2),
            seed,
        );
        if vend != body_len {
            return None; // 链中断/代际混入 → 文件状态不完整
        }
        Some(heads.len() as u64)
    }

    /// 本地 db 目录（`{shard}/{id}`）。derive_db_file 会 mkdir——
    /// 仅在确实要打开/恢复时调用；claim 之前不得调用（否则空目录
    /// 会让"不存在的库"看起来"本地已存在"——不过目录本身无 db
    /// 文件，local_exists 按 db.sqlite 判定，安全）。
    fn db_dir(&self, database_id: &str) -> PathBuf {
        match paths::derive_db_file(&self.db_root, database_id) {
            Ok(f) => f.parent().unwrap_or(&self.db_root).to_path_buf(),
            Err(_) => self.db_root.clone(),
        }
    }

    /// 认领写权（open 冷路径）。最多重试 3 轮 CAS 竞态。
    pub fn claim(&self, database_id: &str) -> Result<ClaimOutcome, PersistError> {
        for _ in 0..3 {
            match self.claim_once(database_id)? {
                ClaimStep::Retry => continue,
                ClaimStep::Done(out) => return Ok(out),
            }
        }
        Err(PersistError::Store(StoreError::unavailable(
            true,
            "manifest claim races exhausted",
        )))
    }

    fn claim_once(&self, database_id: &str) -> Result<ClaimStep, PersistError> {
        let now = self.clock.now_ms();
        let margin = self.cfg.clock_skew_margin_ms;
        let dir = self.db_dir(database_id);
        let db_file = dir.join("db.sqlite");
        let local_exists = db_file.exists();
        match self.read_manifest(database_id)? {
            None => {
                // 无 manifest：全新 或 旧版本地库迁入（升级兼容）。
                if local_exists {
                    // RV-R31 #3：ADOPT 前先把残留 `-wal` 合进主
                    // 文件——SIGKILL 残留的 wal 里可能有已提交未
                    // checkpoint 的页，直接快照主文件会丢数据。
                    // busy（别的打开者/读钉位）→ fail closed 拒收；
                    // 合并后 integrity_check 不通过同样拒收。
                    let wal_side = dir.join("db.sqlite-wal");
                    let wal_pending = std::fs::metadata(&wal_side)
                        .map(|m| m.len() > 0)
                        .unwrap_or(false);
                    if wal_pending {
                        match crate::wal::fold_journal(&db_file) {
                            Ok(true) => {}
                            Ok(false) => {
                                return Err(PersistError::Store(StoreError::unavailable(
                                    true,
                                    "adopt: wal checkpoint busy — refuse to adopt partial image",
                                )));
                            }
                            Err(e) => {
                                return Err(PersistError::Corrupt(format!(
                                    "adopt fold journal: {e}"
                                )))
                            }
                        }
                    }
                    match crate::wal::integrity_ok(&db_file) {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(PersistError::Corrupt(
                                "adopt: integrity_check failed".into(),
                            ))
                        }
                        Err(e) => {
                            return Err(PersistError::Corrupt(format!(
                                "adopt integrity_check: {e}"
                            )))
                        }
                    }
                    let snap = self.upload_snapshot(database_id, 1, 0, &db_file)?;
                    let m = Manifest {
                        v: 1,
                        epoch: 1,
                        lsn: 0,
                        holder: self.cfg.holder.clone(),
                        expires_at_ms: now + self.cfg.lease_ms,
                        tombstone: false,
                        snapshot: Some(snap.clone()),
                        chain: vec![EpochSpan {
                            epoch: 1,
                            first_lsn: 1,
                        }],
                    };
                    match self.write_manifest_inm(database_id, &m) {
                        Ok(etag) => {
                            // 快照已含 fold 后全量；wal 被 TRUNCATE
                            // 归零 → file_frames=0。
                            self.write_sidecar(
                                &dir,
                                &Sidecar {
                                    v: 1,
                                    lsn: 0,
                                    file_frames: 0,
                                    intent: false,
                                },
                            )?;
                            Ok(ClaimStep::Done(ClaimOutcome::Ready {
                                handle: ManifestHandle::from_manifest(&m, etag),
                                restore: None,
                            }))
                        }
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                            // 并发 adopt/create：重读决定归属。
                            Ok(ClaimStep::Retry)
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    let m = Manifest {
                        v: 1,
                        epoch: 1,
                        lsn: 0,
                        holder: self.cfg.holder.clone(),
                        expires_at_ms: now + self.cfg.lease_ms,
                        tombstone: false,
                        snapshot: None,
                        chain: vec![EpochSpan {
                            epoch: 1,
                            first_lsn: 1,
                        }],
                    };
                    match self.write_manifest_inm(database_id, &m) {
                        Ok(etag) => Ok(ClaimStep::Done(ClaimOutcome::Ready {
                            handle: ManifestHandle::from_manifest(&m, etag),
                            restore: None,
                        })),
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                            Ok(ClaimStep::Retry)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
            Some((m, etag)) => {
                if m.tombstone {
                    return Ok(ClaimStep::Done(ClaimOutcome::Tombstoned));
                }
                // RV-R56: 取租方判定对方失效须 now >= expires + margin
                // （比持有方更保守），避免双方同时服务读的窗口。
                if m.holder != self.cfg.holder && !taker_may_claim(m.expires_at_ms, now, margin) {
                    return Ok(ClaimStep::Done(ClaimOutcome::HeldByOther {
                        expires_at_ms: m.expires_at_ms,
                    }));
                }
                // 我们持有（续租）或对方已过期（接管）→ CAS。
                let takeover =
                    m.holder != self.cfg.holder || !holder_usable(m.expires_at_ms, now, margin);
                let mut next = m.clone();
                next.expires_at_ms = now + self.cfg.lease_ms;
                if takeover {
                    next.epoch = m.epoch + 1;
                    next.holder = self.cfg.holder.clone();
                    // epoch+1 写者从 manifest.lsn+1 继续段编号。
                    next.chain.push(EpochSpan {
                        epoch: next.epoch,
                        first_lsn: m.lsn + 1,
                    });
                }
                match self.write_manifest_im(database_id, &etag, &next) {
                    Ok(new_etag) => {
                        let handle = ManifestHandle::from_manifest(&next, new_etag);
                        // RV-R31 #2：信任本地文件需三重证据——sidecar
                        // 标记已达 manifest.lsn 且非 intent；本地
                        // `-wal` 从 header 到文件末尾全链可解析；
                        // 解析帧数恰为标记时记录的物理帧数。缺一即
                        // 本地不完整 → 走恢复（fail closed）。
                        let sidecar_ok = local_exists
                            && match self.read_sidecar(&dir) {
                                Some(sc) if sc.trusted(m.lsn) => {
                                    self.local_wal_frames(&dir) == Some(sc.file_frames)
                                }
                                _ => false,
                            };
                        let restore = if sidecar_ok || (m.lsn == 0 && m.snapshot.is_none()) {
                            // 无恢复必要；但本地有文件而 manifest 全空 →
                            // 本地数据从未 ack → 当作未提交清掉。
                            if local_exists && m.lsn == 0 && m.snapshot.is_none() && !sidecar_ok {
                                Some(RestorePlan {
                                    snapshot: None,
                                    up_to_lsn: 0,
                                    chain: next.chain.clone(),
                                })
                            } else {
                                None
                            }
                        } else {
                            Some(RestorePlan {
                                snapshot: m.snapshot.clone(),
                                up_to_lsn: m.lsn,
                                chain: m.chain.clone(),
                            })
                        };
                        Ok(ClaimStep::Done(ClaimOutcome::Ready { handle, restore }))
                    }
                    Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                        Ok(ClaimStep::Retry)
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// 续租（CAS）：成功更新 handle.expires/etag/degraded=false。
    /// `PreconditionFailed` → 重读：若 holder 不再是我们 → Fenced；
    /// 若仍是我们（etag 漂移）→ 吸收新 etag 再 CAS 一次。
    pub fn renew(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        self.renew_until(database_id, handle, None)
    }

    /// RV-R71: [`Self::renew`] with the request deadline — every store
    /// round trip is cut at `dl`, so a hung object store can't pin the
    /// single-writer lock (and a tokio blocking thread) past the call.
    pub fn renew_until(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        dl: Option<std::time::Instant>,
    ) -> Result<(), PersistError> {
        let now = self.clock.now_ms();
        let mut attempt = handle.clone();
        attempt.expires_at_ms = now + self.cfg.lease_ms;
        match self.write_manifest_im_dl(database_id, &handle.etag, &attempt.to_manifest(false), dl)
        {
            Ok(etag) => {
                handle.expires_at_ms = attempt.expires_at_ms;
                handle.etag = etag;
                handle.degraded = false;
                Ok(())
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => {
                match self.read_manifest_dl(database_id, dl)? {
                    Some((m, etag))
                        if m.holder == self.cfg.holder
                            && m.epoch == handle.epoch
                            && !m.tombstone =>
                    {
                        let mut attempt2 = ManifestHandle::from_manifest(&m, etag.clone());
                        attempt2.expires_at_ms = now + self.cfg.lease_ms;
                        let etag2 = self.write_manifest_im_dl(
                            database_id,
                            &etag,
                            &attempt2.to_manifest(false),
                            dl,
                        )?;
                        handle.expires_at_ms = attempt2.expires_at_ms;
                        handle.etag = etag2;
                        handle.degraded = false;
                        Ok(())
                    }
                    _ => Err(PersistError::Fenced),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// ack 前持久化（§4.2 方案 1，单写者锁内调用）：
    /// put seg（key 唯一 → If-None-Match）→ CAS manifest lsn。
    /// `wal_file_frames` = 提交后 `-wal` 物理帧数（含死帧尾）——
    /// 写进 sidecar 供认领时核对本地文件完整性。
    /// 任何一步失败 → Err（调用方 fence + 返回 retryable）。
    pub fn publish(
        &self,
        database_id: &str,
        seg_bytes: &[u8],
        new_lsn: u64,
        wal_file_frames: u64,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        self.publish_until(
            database_id,
            seg_bytes,
            new_lsn,
            wal_file_frames,
            handle,
            None,
        )
    }

    /// RV-R71: [`Self::publish`] with the request deadline. A timeout
    /// leaves the outcome of the in-flight put/CAS unknown, so callers
    /// MUST treat [`is_deadline`] errors as "commit outcome unknown"
    /// (fence the writer; never ack).
    pub fn publish_until(
        &self,
        database_id: &str,
        seg_bytes: &[u8],
        new_lsn: u64,
        wal_file_frames: u64,
        handle: &mut ManifestHandle,
        dl: Option<std::time::Instant>,
    ) -> Result<(), PersistError> {
        let key = seg::seg_key(self.kind, database_id, handle.epoch, new_lsn);
        self.store_put_inm_dl(&key, seg_bytes, dl)
            .map_err(|e| match e {
                // 同 epoch+lsn 段已存在 = 我们不可能撞自己（串行）→ 语义冲突。
                PersistError::Store(StoreError::PreconditionFailed) => PersistError::Fenced,
                other => other,
            })?;
        let mut next = handle.clone();
        next.lsn = new_lsn;
        next.commits_since_ckpt = handle.commits_since_ckpt + 1;
        // RV-R31：lsn=0 基线——open 期 TRUNCATE 已把 init/dedup/ADOPT
        // 页折进主文件，段链只记水位之后的帧；空镜像重放缺主文件页
        // 必得畸形库（integrity_check 拒开）。首个段发布时把当前
        // db.sqlite（恰是 post-TRUNCATE 基线）传为 snap-0，与
        // manifest CAS 同批落定；上传失败同 publish 语义 → Err。
        if new_lsn == 1 && handle.snapshot.is_none() {
            let snap = self.upload_snapshot_dl(
                database_id,
                handle.epoch,
                0,
                &self.db_dir(database_id).join("db.sqlite"),
                dl,
            )?;
            next.snapshot = Some(snap);
        }
        match self.write_manifest_im_dl(database_id, &handle.etag, &next.to_manifest(false), dl) {
            Ok(etag) => {
                handle.lsn = new_lsn;
                handle.etag = etag;
                handle.commits_since_ckpt += 1;
                handle.snapshot = next.snapshot;
                // RV-R31 #2：标记必须耐久落盘（tmp→fsync→rename→
                // dir fsync）且错误向上传播——此刻 commit 已 ack
                // 给调用方的路径上，sidecar 失败 = 本地状态与标记
                // 不一致 → Err 让引擎 fence（fail closed）。
                self.write_sidecar(
                    &self.db_dir(database_id),
                    &Sidecar {
                        v: 1,
                        lsn: new_lsn,
                        file_frames: wal_file_frames,
                        intent: false,
                    },
                )?;
                Ok(())
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => Err(PersistError::Fenced),
            Err(e) => Err(e),
        }
    }

    /// checkpoint：TRUNCATE 后调用方传入 db 镜像路径；上传快照 +
    /// CAS manifest.snapshot。**本函数不做 GC**（RV-R66 更正旧注释）：
    /// 被新快照覆盖的段/旧快照由 `gc_orphans`（恢复收尾时惰性、有界
    /// 地）清理，否则保留。无 PITR，保留策略见 sql-storage 文档。
    /// 失败仅返回 Err（数据仍由段链保护）——调用方 warn 即可。
    pub fn checkpoint(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        db_file: &Path,
    ) -> Result<(), PersistError> {
        let snap = self.upload_snapshot(database_id, handle.epoch, handle.lsn, db_file)?;
        let mut next = handle.clone();
        next.snapshot = Some(snap.clone());
        match self.write_manifest_im(database_id, &handle.etag, &next.to_manifest(false)) {
            Ok(etag) => {
                handle.snapshot = Some(snap);
                handle.etag = etag;
                handle.commits_since_ckpt = 0;
                // RV-R31 #2：TRUNCATE 已把 `-wal` 归零——标记里的
                // file_frames 必须同步归零，否则认领时帧数核对失
                // 败 → 永久强制恢复（自损）。失败向上传播。
                self.write_sidecar(
                    &self.db_dir(database_id),
                    &Sidecar {
                        v: 1,
                        lsn: handle.lsn,
                        file_frames: 0,
                        intent: false,
                    },
                )?;
                Ok(())
            }
            Err(PersistError::Store(StoreError::PreconditionFailed)) => Err(PersistError::Fenced),
            Err(e) => Err(e),
        }
    }

    fn upload_snapshot(
        &self,
        database_id: &str,
        epoch: u64,
        lsn: u64,
        db_file: &Path,
    ) -> Result<SnapshotRef, PersistError> {
        self.upload_snapshot_dl(database_id, epoch, lsn, db_file, None)
    }

    fn upload_snapshot_dl(
        &self,
        database_id: &str,
        epoch: u64,
        lsn: u64,
        db_file: &Path,
        dl: Option<std::time::Instant>,
    ) -> Result<SnapshotRef, PersistError> {
        // T36b：先 stat 判上限——超限不读文件（病态重读整库的坑，
        // §4.5）；fs 后端 put_file 流式拷贝也不 materialize。
        let len = std::fs::metadata(db_file)
            .map_err(|e| PersistError::Corrupt(format!("snapshot stat: {e}")))?
            .len();
        if len > self.cfg.snapshot_max_bytes {
            return Err(PersistError::SnapshotTooLarge(len));
        }
        let key = seg::snap_key(self.kind, database_id, epoch, lsn);
        let out = self
            .bridge(dl, self.store.put_file(&key, db_file, true))
            .map_err(|e| match e {
                StoreError::PreconditionFailed => PersistError::Fenced,
                other => PersistError::Store(other),
            })?;
        Ok(SnapshotRef {
            key,
            lsn,
            sha256: out.sha256,
            size: out.len,
        })
    }

    /// RV-R31 #5：恢复期间的租约续租——剩余租期跌破半个周期
    /// 即续（给续租失败留重试窗）。续租失败 → Err → 恢复中止，
    /// 调用方 fail closed（库不打开）。
    fn restore_renew(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        let now = self.clock.now_ms();
        if handle
            .expires_at_ms
            .saturating_sub(self.cfg.clock_skew_margin_ms + self.cfg.lease_ms / 2)
            <= now
        {
            self.renew(database_id, handle)?;
        }
        Ok(())
    }

    /// 恢复：流式下载快照 + 按链重放段 → 重建本地 db 文件。
    /// 任何损坏/断档/sha 不符 → Err（fail closed）。
    ///
    /// 落盘顺序（RV-R31 #2/#5，缺一步都不写标记）：
    /// 快照/重放写 `.restore.tmp` → fsync tmp → integrity_check →
    /// 清陈旧 wal/shm → rename → fsync 目录 → 写 sidecar。
    /// 恢复期间按半个租约周期续租；续租失败即中止。
    pub fn restore(
        &self,
        database_id: &str,
        plan: &RestorePlan,
        handle: &mut ManifestHandle,
    ) -> Result<(), PersistError> {
        let dir = self.db_dir(database_id);
        std::fs::create_dir_all(&dir).map_err(|e| PersistError::LocalIo(format!("db dir: {e}")))?;
        let db_file = dir.join("db.sqlite");
        let tmp = dir.join(".restore.tmp");
        // 快照基线（或空文件）——流式落 tmp，不再整体进内存。
        match &plan.snapshot {
            Some(snap) => {
                let out = vrt_objstore::block_on_store(self.store.get_to_file(&snap.key, &tmp))?
                    .ok_or_else(|| PersistError::Corrupt("snapshot object missing".into()))?;
                if out.len != snap.size {
                    return Err(PersistError::Corrupt("snapshot size mismatch".into()));
                }
                if out.sha256 != snap.sha256 {
                    return Err(PersistError::Corrupt("snapshot sha256 mismatch".into()));
                }
                std::fs::File::open(&tmp)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| PersistError::LocalIo(format!("restore fsync: {e}")))?;
                self.restore_renew(database_id, handle)?;
            }
            None => {
                // 无快照基线：从空文件重放全部段（1..=up_to）。段帧是
                // 完整页镜像 + commit_db_size 截断，等价 SQLite 进程从
                // 零重放 WAL——链校验保证 1..up_to 连续无断档。
                std::fs::write(&tmp, b"")
                    .map_err(|e| PersistError::LocalIo(format!("restore write: {e}")))?;
            }
        }
        let base_lsn = plan.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0);
        if base_lsn > plan.up_to_lsn {
            return Err(PersistError::Corrupt("snapshot beyond manifest lsn".into()));
        }
        let mut page_size = 0u32;
        {
            // 从快照读 page_size（偏移 16,2 字节 BE;1 表示 65536）。
            use std::io::{Read, Seek, SeekFrom};
            if plan.snapshot.is_some() {
                let mut f = std::fs::File::open(&tmp)
                    .map_err(|e| PersistError::LocalIo(format!("snap open: {e}")))?;
                let mut hdr = [0u8; 2];
                f.seek(SeekFrom::Start(16))
                    .and_then(|_| f.read_exact(&mut hdr))
                    .map_err(|_| PersistError::Corrupt("snapshot page_size read".into()))?;
                let v = u16::from_be_bytes(hdr) as u32;
                page_size = if v == 1 { 65536 } else { v };
                if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
                    return Err(PersistError::Corrupt("snapshot page_size invalid".into()));
                }
            }
        }
        // 顺序重放 (base_lsn, up_to_lsn]——每段后做一次续租判定，
        // 段越多恢复越久，租约不丢（RV-R31 #5）。
        if plan.up_to_lsn > base_lsn {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp)
                .map_err(|e| PersistError::LocalIo(format!("restore open: {e}")))?;
            let mut prev = base_lsn;
            for lsn in (base_lsn + 1)..=plan.up_to_lsn {
                let epoch = epoch_for_lsn(&plan.chain, lsn)
                    .ok_or_else(|| PersistError::Corrupt(format!("no epoch covers lsn {lsn}")))?;
                let key = seg::seg_key(self.kind, database_id, epoch, lsn);
                let obj = self
                    .store_get(&key)?
                    .ok_or_else(|| PersistError::Corrupt(format!("segment lsn {lsn} missing")))?;
                let s = seg::decode(&obj.body)
                    .map_err(|e| PersistError::Corrupt(format!("seg {lsn}: {e}")))?;
                if s.database_id != database_id {
                    return Err(PersistError::Corrupt("seg database_id mismatch".into()));
                }
                if s.lsn != lsn || s.prev_lsn != prev {
                    return Err(PersistError::Corrupt(format!(
                        "seg chain break at lsn {lsn} (prev={prev}, seg.prev={})",
                        s.prev_lsn
                    )));
                }
                if page_size == 0 {
                    page_size = s.page_size;
                } else if s.page_size != page_size {
                    return Err(PersistError::Corrupt("seg page_size mismatch".into()));
                }
                seg::apply_frames(&mut f, &s)
                    .map_err(|e| PersistError::Corrupt(format!("seg {lsn} apply: {e}")))?;
                prev = lsn;
                self.restore_renew(database_id, handle)?;
            }
        }
        // tmp 全部写完 → fsync → integrity_check（RV-R31 #1：拒开
        // 损坏镜像——integrity_check 会以 WAL 模式建旁车文件，先
        // 校验再清）。fsync 在 rename 前；rename 后 fsync 父目录。
        std::fs::OpenOptions::new()
            .read(true)
            .open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| PersistError::LocalIo(format!("restore fsync: {e}")))?;
        match crate::wal::integrity_ok(&tmp) {
            Ok(true) => {}
            Ok(false) => {
                tracing::error!(
                    db = %database_id,
                    "restore image failed integrity_check — refusing to open"
                );
                return Err(PersistError::Corrupt(
                    "restored image failed integrity_check".into(),
                ));
            }
            Err(e) => {
                tracing::error!(db = %database_id, error = %e, "restore integrity_check errored");
                return Err(PersistError::Corrupt(format!("integrity_check: {e}")));
            }
        }
        // integrity_check 可能在 .restore.tmp 旁建 wal/shm——连同
        // 陈旧 wal/shm 一并清掉（恢复态是完整镜像）。必须在 rename
        // 前清，否则新镜像旁挂旧 wal 会被当活数据。
        for stale in [
            dir.join("db.sqlite-wal"),
            dir.join("db.sqlite-shm"),
            dir.join(".restore.tmp-wal"),
            dir.join(".restore.tmp-shm"),
        ] {
            let _ = std::fs::remove_file(stale);
        }
        std::fs::rename(&tmp, &db_file)
            .map_err(|e| PersistError::LocalIo(format!("restore rename: {e}")))?;
        let df = std::fs::File::open(&dir)
            .map_err(|e| PersistError::LocalIo(format!("restore dir open: {e}")))?;
        df.sync_all()
            .map_err(|e| PersistError::LocalIo(format!("restore dir fsync: {e}")))?;
        // 全部耐久步骤完成后才写标记——wal 已被重建清零。
        self.write_sidecar(
            &dir,
            &Sidecar {
                v: 1,
                lsn: plan.up_to_lsn,
                file_frames: 0,
                intent: false,
            },
        )?;
        // 惰性孤儿段 GC（best-effort）。
        self.gc_orphans(
            database_id,
            plan.up_to_lsn,
            &plan.chain,
            plan.snapshot.as_ref().map(|s| s.lsn).unwrap_or(0),
        );
        Ok(())
    }

    /// 孤儿对象 GC：list `db/{id}/`。
    /// - `.seg`：不在链上 / 超出已提交 / 已被基线快照覆盖（`lsn <=
    ///   baseline_lsn`，快照语义等价）→ delete。
    /// - `.snap`：仅删 `lsn < baseline_lsn` 的旧基线快照——基线本身
    ///   与在传快照（lsn ≥ baseline）绝不动（在传者由 manifest CAS
    ///   收尾，删它会让 restore 缺基线）。
    /// RV-R66：单次删除上限（有界）；余量留给下一轮（删除幂等）。
    /// 删除失败不再静默——计入 `GcReport.failed` 并 warn，下一轮重试。
    /// 注意：本 GC 只清"被基线快照覆盖/不在链上"的对象，**没有**
    /// 按时间窗/N 代的保留链，因此不提供 PITR（见 sql-storage 文档）。
    pub fn gc_orphans(
        &self,
        database_id: &str,
        committed_lsn: u64,
        chain: &[EpochSpan],
        baseline_lsn: u64,
    ) -> GcReport {
        let mut rep = GcReport::default();
        let prefix = format!("{}/{database_id}/", self.kind.object_prefix());
        // best-effort GC：取得到的一页即干活，截断页也照常用
        // （未列出的下一轮再收；删缺失 key 本来就是幂等）。
        let page = match self.store_list(&prefix) {
            Ok(p) => p,
            Err(e) => {
                rep.list_failed = true;
                tracing::warn!(db = %database_id, error = %e, "gc_orphans list failed; retry next pass");
                return rep;
            }
        };
        for e in page.entries {
            let doomed = if e.key.ends_with(".seg") {
                match parse_seg_name(&e.key) {
                    Some((ep, lsn)) => {
                        let covered =
                            epoch_for_lsn(chain, lsn) == Some(ep) && lsn <= committed_lsn;
                        !covered || lsn <= baseline_lsn
                    }
                    None => false,
                }
            } else if e.key.ends_with(".snap") {
                matches!(parse_snap_name(&e.key), Some(lsn) if lsn < baseline_lsn)
            } else {
                false
            };
            if !doomed {
                continue;
            }
            if rep.deleted + rep.failed >= MAX_GC_DELETES_PER_PASS {
                rep.deferred += 1;
                continue;
            }
            match self.store_delete(&e.key) {
                Ok(_) => rep.deleted += 1,
                Err(err) => {
                    rep.failed += 1;
                    tracing::warn!(db = %database_id, error = %err, "gc_orphans delete failed; retry next pass");
                }
            }
        }
        rep
    }

    /// T36b 优雅停机：把 `expires_at` 置为 now-margin（epoch/holder/lsn
    /// 不变、非 tombstone）——下一个写者立即接管，不必等满 lease。
    /// CAS 失败 = lease 已被夺（他人接管或已过期被抢）→ 幂等 Ok；
    /// 真错误（store 不可用等）向上传播由调用方 warn。
    pub fn release(
        &self,
        database_id: &str,
        handle: &mut ManifestHandle,
        now_ms: u64,
    ) -> Result<(), PersistError> {
        // RV-R56: 取租方须 `now >= expires + margin` 才能接管，故释放
        // 把 expires 回拨一个 margin（饱和），下一个写者立即可接；本
        // 引擎随后被强制 fence，不会在回拨窗口内继续服务。
        handle.expires_at_ms = now_ms.saturating_sub(self.cfg.clock_skew_margin_ms);
        match self.write_manifest_im(database_id, &handle.etag, &handle.to_manifest(false)) {
            Ok(etag) => {
                handle.etag = etag;
                Ok(())
            }
            // 已丢权（含 tombstone 后）：无需释放。
            Err(PersistError::Store(StoreError::PreconditionFailed)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// manifest tombstone（§7 delete 第一步）：旧写者全部 fencing。
    /// 幂等；无 manifest → 直接建 tombstoned manifest。
    pub fn tombstone(&self, database_id: &str) -> Result<(), PersistError> {
        for _ in 0..3 {
            match self.read_manifest(database_id)? {
                None => {
                    let m = Manifest {
                        v: 1,
                        epoch: 0,
                        lsn: 0,
                        holder: String::new(),
                        expires_at_ms: 0,
                        tombstone: true,
                        snapshot: None,
                        chain: Vec::new(),
                    };
                    match self.write_manifest_inm(database_id, &m) {
                        Ok(_) => return Ok(()),
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Some((mut m, etag)) => {
                    if m.tombstone {
                        return Ok(());
                    }
                    m.tombstone = true;
                    match self.write_manifest_im(database_id, &etag, &m) {
                        Ok(_) => return Ok(()),
                        Err(PersistError::Store(StoreError::PreconditionFailed)) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Err(PersistError::Store(StoreError::unavailable(
            true,
            "tombstone races exhausted",
        )))
    }

    /// tombstone 保留期后回收段/快照/manifest（惰性调用）。
    /// RV-R53.3：返回 typed 结果——调用方据此决定是否写 `.gc-done`
    /// 旁挂哨兵，避免已完成项永久占用每轮扫描名额（饥饿修复）。
    pub fn gc_tombstoned(&self, database_id: &str) -> Result<GcTombstonedOutcome, PersistError> {
        let Some((m, _)) = self.read_manifest(database_id)? else {
            // manifest 已回收（或无 manifest 的 marker）——对象前缀
            // 可能仍有残留段，继续走完 listing 才能宣告 done。
            return self.gc_tombstoned_prefix(database_id, true);
        };
        if !m.tombstone {
            // marker 在但 manifest 尚未 tombstone ——状态未收敛，
            // 保留在候选集里下轮重查（不写 done）。
            return Ok(GcTombstonedOutcome::Retained);
        }
        let now = self.clock.now_ms();
        if now.saturating_sub(m.expires_at_ms) < self.cfg.retention_ms {
            return Ok(GcTombstonedOutcome::Retained);
        }
        self.gc_tombstoned_prefix(database_id, false)
    }

    /// 列出并删除一页 `db/{id}/`（或 `q/{id}/`）。截断报可重试错误，
    /// 绝不假装删完。
    fn gc_tombstoned_prefix(
        &self,
        database_id: &str,
        manifest_absent: bool,
    ) -> Result<GcTombstonedOutcome, PersistError> {
        let mut freed = 0u64;
        let page = self.store_list(&format!("{}/{database_id}/", self.kind.object_prefix()))?;
        for e in page.entries {
            let _ = self.store_delete(&e.key);
            freed += 1;
        }
        if page.truncated {
            // RV-R8 #7：一页删不完——报出进度并让调用方重试收敛，
            // 绝不假装已删完。
            return Err(PersistError::Store(StoreError::unavailable(
                true,
                "tombstoned gc: listing truncated — retry to converge",
            )));
        }
        if freed == 0 && manifest_absent {
            return Ok(GcTombstonedOutcome::Empty);
        }
        Ok(GcTombstonedOutcome::Reclaimed(freed))
    }
}

/// RV-R53.3：`gc_tombstoned` 一轮结果。`Empty`/`Reclaimed` 表示该
/// marker 的对象已删净（可写 `.gc-done` 旁挂哨兵退出候选集）；
/// `Retained` 留在集合里下轮重查。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcTombstonedOutcome {
    /// 无 manifest 且对象前缀已空——终态。
    Empty,
    /// 已过保留期，本轮删除 freed 个对象——终态（前缀已空）。
    Reclaimed(u64),
    /// 未到保留期或 manifest 未 tombstone——下轮再查。
    Retained,
}

enum ClaimStep {
    Retry,
    Done(ClaimOutcome),
}

/// 解析 `db/{id}/{epoch}/{lsn}.seg` 的 epoch/lsn。
/// `snap-{lsn:016x}.snap` → lsn（快照无需 epoch——同目录只留基线）。
fn parse_snap_name(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let rest = name.strip_prefix("snap-")?;
    let lsn = rest.strip_suffix(".snap")?;
    u64::from_str_radix(lsn, 16).ok()
}

fn parse_seg_name(key: &str) -> Option<(u64, u64)> {
    let name = key.rsplit('/').next()?;
    let lsn = name.strip_suffix(".seg")?;
    let lsn = u64::from_str_radix(lsn, 16).ok()?;
    let mut it = key.rsplit('/');
    let _ = it.next();
    let ep = it.next()?;
    let ep = u64::from_str_radix(ep, 16).ok()?;
    Some((ep, lsn))
}

/// RV-R56: 持有方是否仍可服务——`now < expires - margin`。
/// 与 [`taker_may_claim`] 配对：持有方在 `expires - margin` 前停止服务，
/// 取租方在 `expires + margin` 后才接管，两者之间留出 2*margin 的
/// 空档，时钟偏差 < margin 时不可能双方同时服务。
pub(crate) fn holder_usable(expires_at_ms: u64, now_ms: u64, margin_ms: u64) -> bool {
    expires_at_ms.saturating_sub(margin_ms) > now_ms
}

/// RV-R56: 取租方是否可接管他人租约——`now >= expires + margin`（饱和加）。
pub(crate) fn taker_may_claim(expires_at_ms: u64, now_ms: u64, margin_ms: u64) -> bool {
    now_ms >= expires_at_ms.saturating_add(margin_ms)
}

#[cfg(test)]
mod rv_r56_tests {
    use super::*;

    /// 已验证（2026-11-15）：边界 [expires-margin, expires+margin) 内双方都不可用。
    #[test]
    fn lease_margin_inequalities() {
        let (exp, m) = (10_000u64, 500u64);
        assert!(holder_usable(exp, exp - m - 1, m));
        assert!(!holder_usable(exp, exp - m, m));
        assert!(!taker_may_claim(exp, exp - m, m));
        assert!(!taker_may_claim(exp, exp, m));
        assert!(!taker_may_claim(exp, exp + m - 1, m));
        assert!(taker_may_claim(exp, exp + m, m));
        for now in (exp - m)..(exp + m) {
            assert!(!(holder_usable(exp, now, m) && taker_may_claim(exp, now, m)));
        }
        assert!(!taker_may_claim(u64::MAX, u64::MAX - 1, 5));
    }
}
