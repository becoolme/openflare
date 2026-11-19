//! vrt-objstore：typed 对象存储抽象（T36 / §4）。
//!
//! 三个实现：`memory`（测试）、`fs`（单节点 standalone 默认）、`s3`
//! （兼容 S3；条件写 If-Match / If-None-Match，兼容性见设计文档 §12
//! 未证实项）。条件写是**真 CAS**：fs 用 flock + read-verify-write +
//! atomic rename，绝非"先读后写"竞态。
//!
//! 错误类型三分：`PreconditionFailed`（CAS 失败——对调用方是语义
//! 信号：lease 丢失/冲突）、`NotFound`、`Unavailable{retryable}`。
//! 所有调用有界超时 + 有界重试（重试仅作用于幂等操作与连接级失败）。

use std::future::Future;

pub mod deadline;
pub mod drive;
pub mod endpoint;
pub mod fs;
pub mod memory;
pub mod s3;
pub mod self_check;
pub mod sigv4;

pub use deadline::{retry_until, Deadline, RetryPlan};
pub use drive::block_on_store;
pub use fs::FsStore;
pub use memory::MemoryStore;
pub use s3::{S3Store, S3StoreConfig};

/// 对象元数据（读/列返回）。
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// 后端相关的版本令牌（CAS 用）。fs/memory: 内容 sha256；s3: ETag。
    pub etag: String,
    /// 对象字节数。
    pub size: u64,
}

/// `get` 的返回值。
#[derive(Debug, Clone)]
pub struct StoredObject {
    pub meta: ObjectMeta,
    pub body: Vec<u8>,
}

/// list 条目。
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
}

/// `list` 结果（RV-R8 #7）：实现内部分页有上界，`truncated == true`
/// 表示 `entries` **不是**全集——在上界处停住了。需要完整列表的
/// 调用方（WAL 回放、删除收敛）必须检查该位并 fail closed；只需
/// 取样的调用方（GC）可直接用 `entries`。
#[derive(Debug)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    /// 命中实现级分页上界，列表不完整。
    pub truncated: bool,
}

/// `list_children` 结果（RV-R28 #3）：`common_prefixes` 是折叠出的
/// 一级"目录"（完整前缀，含结尾分隔符），`entries` 是 `prefix`
/// 直下、rest 中不含分隔符的对象。`truncated` 语义同 [`ListPage`]
/// ——需要全集的调用方必须 fail closed。
#[derive(Debug, Default)]
pub struct ListChildrenPage {
    pub entries: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
}

/// `put_file` 默认（整读缓冲）实现的上限——大于此的文件必须先被
/// 调用方自己的 `snapshot_max_bytes` 约束拒绝；这里 fail closed
/// 兜底，避免把整个文件读进内存（RV-R8 #7）。流式后端（fs）覆盖
/// 该方法、不受此界。
pub const MAX_PUT_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// 存储错误分类（§4 决策依据）。
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// 条件写失败：If-Match etag 不符 / If-None-Match 撞已存在。
    /// 调用方语义：lease 丢失或并发写冲突。
    #[error("precondition failed")]
    PreconditionFailed,
    /// 对象不存在（get/CAS 目标缺失时部分后端也返回 PreconditionFailed）。
    #[error("object not found")]
    NotFound,
    /// 不可用：网络/5xx/超时/后端缺能力。`retryable=false` 表示重试
    /// 无意义（如后端不支持条件写——fail closed，绝不降级为非原子写）。
    #[error("store unavailable (retryable={retryable}): {reason}")]
    Unavailable { retryable: bool, reason: String },
    /// 本地数据损坏（校验失败/格式错误）。
    #[error("object corrupt: {0}")]
    Corrupt(String),
}

impl StoreError {
    /// 便捷构造。
    pub fn unavailable(retryable: bool, reason: impl Into<String>) -> Self {
        Self::Unavailable {
            retryable,
            reason: reason.into(),
        }
    }
}

/// `put_file` 结果：新 etag + 流式内容 sha256 + 字节数。
#[derive(Debug, Clone)]
pub struct FilePutOutcome {
    pub etag: String,
    pub sha256: String,
    pub len: u64,
}

/// `get_to_file` 结果：对象 etag + 内容 sha256 + 字节数。
#[derive(Debug, Clone)]
pub struct FileGetOutcome {
    pub etag: String,
    pub sha256: String,
    pub len: u64,
}

/// Typed 对象存储。所有方法 async；需要同步调用时用
/// [`block_on_store`] 桥接（vrt-sql 在单写者锁内调用）。
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// 读对象；不存在返回 `Ok(None)`。
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, StoreError>;
    /// 仅元数据；不存在返回 `Ok(None)`。
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError>;
    /// 仅当 key 不存在时写入。冲突 → `PreconditionFailed`。
    /// 成功返回新对象 etag。
    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError>;
    /// 仅当当前 etag == `expected` 时写入。不符/对象缺失 →
    /// `PreconditionFailed`。成功返回新 etag。
    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError>;
    /// 无条件写（key 天然唯一时使用，如 `{epoch}/{lsn}.seg`）。
    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError>;
    /// 按前缀列 key（有界：实现内部分页/截断）。`page.truncated`
    /// 为真时 `entries` 是前缀——需要全集者必须 fail closed
    /// （RV-R8 #7）。
    async fn list(&self, prefix: &str) -> Result<ListPage, StoreError>;
    /// RV-R28 #3：列 `prefix` 下 key 严格大于 `start_after` 的对象
    /// （游标语义，exclusive）——有界分页存储的可续传扫描。默认
    /// 实现是 `list` + 客户端过滤（对完整实现正确）；有界后端
    /// （S3 `start-after`）原生覆写以保持分页上界语义。
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        let mut page = self.list(prefix).await?;
        if let Some(after) = start_after {
            page.entries.retain(|e| e.key.as_str() > after);
        }
        Ok(page)
    }
    /// RV-R28 #3：delimiter 列举——`prefix` 之后 rest 中含
    /// `delimiter` 的 key 折叠进 `common_prefixes`（S3
    /// CommonPrefixes），不含的落 `entries`。用于按一级"目录"
    /// 枚举（如 WAL stem），避免整前缀 flat 列表撞上分页上界。
    /// 默认实现是 `list` + 折叠（对完整实现正确，截断照常上报）；
    /// 有界后端原生覆写。
    async fn list_children(
        &self,
        prefix: &str,
        delimiter: &str,
    ) -> Result<ListChildrenPage, StoreError> {
        let page = self.list(prefix).await?;
        let mut out = ListChildrenPage {
            truncated: page.truncated,
            ..Default::default()
        };
        let mut seen = std::collections::BTreeSet::new();
        for e in page.entries {
            let Some(rest) = e.key.strip_prefix(prefix) else {
                continue;
            };
            if let Some(i) = rest.find(delimiter) {
                let cp = format!("{}{}", prefix, &rest[..i + delimiter.len()]);
                if seen.insert(cp.clone()) {
                    out.common_prefixes.push(cp);
                }
            } else {
                out.entries.push(e);
            }
        }
        out.common_prefixes.sort();
        Ok(out)
    }
    /// 删除；缺失视为成功（幂等）。
    async fn delete(&self, key: &str) -> Result<(), StoreError>;
    /// RV-R31 #5：把对象流式写到本地文件（不整体进内存），返回
    /// etag + 内容 sha256 + 字节数——供恢复路径在落盘同趟完成
    /// 完整性校验。默认实现 `get` + `fs::write`（小对象可用；
    /// 大快照由 fs/s3 覆写为真流式）。写 `dest` 是**非原子**的
    /// 直写——调用方负责 tmp+rename。
    async fn get_to_file(
        &self,
        key: &str,
        dest: &std::path::Path,
    ) -> Result<Option<FileGetOutcome>, StoreError> {
        let Some(obj) = self.get(key).await? else {
            return Ok(None);
        };
        let sha256 = crate::sigv4::sha256_hex(&obj.body);
        let len = obj.body.len() as u64;
        let meta = obj.meta;
        std::fs::write(dest, &obj.body)
            .map_err(|e| StoreError::unavailable(true, format!("get_to_file write: {e}")))?;
        Ok(Some(FileGetOutcome {
            etag: meta.etag,
            sha256,
            len,
        }))
    }

    /// T36b：以本地文件为源写入（快照等大对象——避免整文件进
    /// 内存）。`if_none_match=true` 时等价 `put_if_none_match`。
    /// 默认实现：`fs::read` + `put*`（内存/S3 路径——大对象由
    /// 调用方 `snapshot_max_bytes` 上限兜底；fs 覆盖为流式拷贝）。
    async fn put_file(
        &self,
        key: &str,
        src: &std::path::Path,
        if_none_match: bool,
    ) -> Result<FilePutOutcome, StoreError> {
        // RV-R8 #7: stat 先行——超出单对象界的文件拒绝整读，
        // 不分配等价内存；流式后端覆写本方法、自带路径。
        let meta = std::fs::metadata(src)
            .map_err(|e| StoreError::unavailable(false, format!("put_file stat: {e}")))?;
        if meta.len() > MAX_PUT_FILE_BYTES {
            return Err(StoreError::unavailable(
                false,
                format!("put_file source {} > MAX_PUT_FILE_BYTES", meta.len()),
            ));
        }
        let body = std::fs::read(src)
            .map_err(|e| StoreError::unavailable(false, format!("put_file read: {e}")))?;
        let sha256 = crate::sigv4::sha256_hex(&body);
        let etag = if if_none_match {
            self.put_if_none_match(key, &body).await?
        } else {
            self.put(key, &body).await?
        };
        Ok(FilePutOutcome {
            etag,
            sha256,
            len: body.len() as u64,
        })
    }
}

/// key 合法性：拒绝空、绝对、穿越、控制字符（所有实现共用）。
pub fn validate_key(key: &str) -> Result<(), StoreError> {
    let ok = !key.is_empty()
        && key.len() <= 1024
        && !key.starts_with('/')
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
        && !key
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..");
    if ok {
        Ok(())
    } else {
        Err(StoreError::unavailable(false, "invalid object key"))
    }
}

/// RV-R71：兼容入口——旧的线性无抖动 `retry` 已被 [`deadline::retry_until`]
/// 取代（指数退避 + 抖动 + 总截止）。`attempts` 次数上界保留；总预算取
/// `attempts` 个默认退避上限，调用方应优先直接使用 `retry_until`。
pub async fn retry<F, Fut, T>(attempts: u32, backoff_ms: u64, f: F) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    let plan = deadline::RetryPlan {
        max_attempts: attempts.max(1),
        base_delay: std::time::Duration::from_millis(backoff_ms.max(1)),
        ..deadline::RetryPlan::default()
    };
    deadline::retry_until(
        &plan,
        deadline::Deadline::after(std::time::Duration::from_secs(60)),
        f,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> MemoryStore {
        MemoryStore::new()
    }

    /// RV-R28 #3: the default `list_after` filters strictly past the
    /// cursor (exclusive resume); `list_children` folds deeper keys
    /// into sorted, deduplicated common prefixes while direct
    /// children land in `entries`.
    #[test]
    fn default_list_after_and_children_on_memory() {
        let store = mem();
        block_on_store(async {
            for k in ["w/a/1", "w/a/2", "w/b/1", "w/top", "w/z-deep/x/y"] {
                store.put(k, b"x").await?;
            }
            // list_after — exclusive cursor.
            let page = store.list_after("w/", Some("w/b/1")).await?;
            let keys: Vec<&str> = page.entries.iter().map(|e| e.key.as_str()).collect();
            assert_eq!(keys, vec!["w/top", "w/z-deep/x/y"]);
            let all = store.list_after("w/", None).await?;
            assert_eq!(all.entries.len(), 5);
            // list_children — fold at the first '/' after the prefix.
            let page = store.list_children("w/", "/").await?;
            assert_eq!(
                page.common_prefixes,
                vec!["w/a/", "w/b/", "w/z-deep/"]
            );
            assert_eq!(page.entries.len(), 1);
            assert_eq!(page.entries[0].key, "w/top");
            assert!(!page.truncated);
            Ok::<(), StoreError>(())
        })
        .unwrap();
    }
}
