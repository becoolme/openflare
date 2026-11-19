//! 文件系统对象存储（standalone 默认）。
//!
//! 布局：`{root}/{key}` 直接映射为文件。条件写是**真 CAS**：
//! 1. 进程内 `cas_lock` Mutex 互斥（**必须**——macOS/BSD flock 锁按
//!    进程合并，同进程多 fd 的 LOCK_EX 不互斥；Linux per-fd 才互斥）。
//! 2. 打开 `{key}.lock` 锁文件 → `flock(LOCK_EX)` 跨进程互斥。
//! 2. 锁内读当前内容 → 比 etag（sha256）→ 写 `{key}.tmp-{nonce}`
//!    → fsync → atomic rename → fsync 目录。
//! 3. flock 在 fd 关闭时自动释放。
//!
//! 不是"先读后写"——校验与提交全程在排他锁内。

use super::{
    FileGetOutcome, FilePutOutcome, ListEntry, ListPage, ObjectMeta, ObjectStore, StoreError,
    StoredObject,
};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NONCE: AtomicU64 = AtomicU64::new(0);

fn etag_of(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

/// 文件系统 store。单节点 standalone 默认后端。
pub struct FsStore {
    root: PathBuf,
    /// 进程内 CAS 互斥：flock 在 macOS 下同进程不互斥（见文件头注）。
    cas_lock: std::sync::Mutex<()>,
}

impl std::fmt::Debug for FsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsStore").field("root", &self.root).finish()
    }
}

impl FsStore {
    /// 打开（不存在则创建 root）。
    pub fn new(root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            cas_lock: std::sync::Mutex::new(()),
        })
    }

    fn path_of(&self, key: &str) -> Result<PathBuf, StoreError> {
        super::validate_key(key)?;
        Ok(self.root.join(key))
    }

    fn lock_path_of(&self, key: &str) -> Result<PathBuf, StoreError> {
        super::validate_key(key)?;
        Ok(self.root.join(format!("{key}.lock")))
    }

    /// 排他锁 guard：flock 文件 + fd 关闭即释放。
    fn lock_key(&self, key: &str) -> Result<std::fs::File, StoreError> {
        let lp = self.lock_path_of(key)?;
        if let Some(d) = lp.parent() {
            std::fs::create_dir_all(d)
                .map_err(|e| StoreError::unavailable(false, format!("lock dir: {e}")))?;
        }
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&lp)
            .map_err(|e| StoreError::unavailable(true, format!("lock open: {e}")))?;
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(StoreError::unavailable(
                true,
                format!("flock: {}", std::io::Error::last_os_error()),
            ));
        }
        Ok(f)
    }

    fn read_current(&self, path: &Path) -> Result<Option<Vec<u8>>, StoreError> {
        match std::fs::read(path) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::unavailable(true, format!("read: {e}"))),
        }
    }

    /// tmp 写入 + fsync + rename + 目录 fsync（锁内调用）。
    fn atomic_commit(&self, path: &Path, body: &[u8]) -> Result<String, StoreError> {
        let dir = path
            .parent()
            .ok_or_else(|| StoreError::unavailable(false, "key path has no parent"))?;
        std::fs::create_dir_all(dir)
            .map_err(|e| StoreError::unavailable(true, format!("mkdir: {e}")))?;
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!(
            ".{}.tmp-{}-{nonce}",
            path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id()
        ));
        let res = (|| -> Result<(), StoreError> {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| StoreError::unavailable(true, format!("tmp create: {e}")))?;
            f.write_all(body)
                .map_err(|e| StoreError::unavailable(true, format!("tmp write: {e}")))?;
            f.sync_all()
                .map_err(|e| StoreError::unavailable(true, format!("tmp fsync: {e}")))?;
            std::fs::rename(&tmp, path)
                .map_err(|e| StoreError::unavailable(true, format!("rename: {e}")))?;
            // 目录 fsync：rename 持久化。
            let d = std::fs::File::open(dir)
                .map_err(|e| StoreError::unavailable(true, format!("dir open: {e}")))?;
            d.sync_all()
                .map_err(|e| StoreError::unavailable(true, format!("dir fsync: {e}")))?;
            Ok(())
        })();
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res.map(|_| etag_of(body))
    }

    /// 同步 CAS：锁内 read-verify-write（ObjectStore async 方法内部直接
    /// 调它——文件 I/O 就绪即返回，无需 reactor）。
    pub fn put_cas_sync(
        &self,
        key: &str,
        expected: Option<&str>,
        body: &[u8],
    ) -> Result<String, StoreError> {
        let path = self.path_of(key)?;
        let _guard = self.cas_lock.lock().unwrap_or_else(|e| e.into_inner());
        let _lock = self.lock_key(key)?;
        let cur = self.read_current(&path)?;
        match (expected, &cur) {
            (Some(exp), Some(bytes)) if etag_of(bytes) == exp => {}
            (None, None) => {}
            // expected=Some + 缺失 / etag 不符 / expected=None + 已存在
            _ => return Err(StoreError::PreconditionFailed),
        }
        self.atomic_commit(&path, body)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FsStore {
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, StoreError> {
        let path = self.path_of(key)?;
        match self.read_current(&path)? {
            Some(body) => Ok(Some(StoredObject {
                meta: ObjectMeta {
                    etag: etag_of(&body),
                    size: body.len() as u64,
                },
                body,
            })),
            None => Ok(None),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError> {
        Ok(self.get(key).await?.map(|o| o.meta))
    }

    /// RV-R31 #5：流式下载——打开的 fd 读到的是 rename 提交的完整
    /// 快照（对象写入本身走 tmp→rename），边拷边算 sha256。
    async fn get_to_file(
        &self,
        key: &str,
        dest: &std::path::Path,
    ) -> Result<Option<FileGetOutcome>, StoreError> {
        let path = self.path_of(key)?;
        let mut r = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(StoreError::unavailable(true, format!("get_to_file src: {e}")))
            }
        };
        let mut w = std::fs::File::create(dest)
            .map_err(|e| StoreError::unavailable(true, format!("get_to_file dst: {e}")))?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 65536];
        let mut len = 0u64;
        use std::io::Read;
        loop {
            let n = r
                .read(&mut buf)
                .map_err(|e| StoreError::unavailable(true, format!("get_to_file read: {e}")))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            w.write_all(&buf[..n])
                .map_err(|e| StoreError::unavailable(true, format!("get_to_file write: {e}")))?;
            len += n as u64;
        }
        // fs 后端 etag 即内容 sha256（与 etag_of/put_file 同口径）。
        let sha256 = hex::encode(hasher.finalize());
        Ok(Some(FileGetOutcome {
            etag: sha256.clone(),
            sha256,
            len,
        }))
    }

    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.put_cas_sync(key, None, body)
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError> {
        self.put_cas_sync(key, Some(expected), body)
    }

    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        let path = self.path_of(key)?;
        let _guard = self.cas_lock.lock().unwrap_or_else(|e| e.into_inner());
        let _lock = self.lock_key(key)?;
        self.atomic_commit(&path, body)
    }

    /// T36b：流式快照写入——不整读源文件进内存。
    /// 两阶段：无锁流拷 src→tmp（含流式 sha256+fsync）→ CAS 锁内
    /// 校验前置条件 + rename + 目录 fsync（锁只持快速段，不阻塞
    /// 其它 key 的 CAS）。
    async fn put_file(
        &self,
        key: &str,
        src: &std::path::Path,
        if_none_match: bool,
    ) -> Result<FilePutOutcome, StoreError> {
        let path = self.path_of(key)?;
        let dir = path
            .parent()
            .ok_or_else(|| StoreError::unavailable(false, "key path has no parent"))?;
        std::fs::create_dir_all(dir)
            .map_err(|e| StoreError::unavailable(true, format!("mkdir: {e}")))?;
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!(
            ".{}.tmp-{}-{nonce}",
            path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id()
        ));
        // Phase 1（无锁）：流式拷贝 + sha256 + fsync。
        let streamed = (|| -> Result<(String, u64), StoreError> {
            use sha2::{Digest, Sha256};
            use std::io::{Read, Write};
            let mut r = std::fs::File::open(src)
                .map_err(|e| StoreError::unavailable(false, format!("put_file src: {e}")))?;
            let mut w = std::fs::File::create(&tmp)
                .map_err(|e| StoreError::unavailable(true, format!("tmp create: {e}")))?;
            let mut hasher = Sha256::new();
            let mut buf = [0u8; 65536];
            let mut len = 0u64;
            loop {
                let n = r
                    .read(&mut buf)
                    .map_err(|e| StoreError::unavailable(true, format!("src read: {e}")))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                w.write_all(&buf[..n])
                    .map_err(|e| StoreError::unavailable(true, format!("tmp write: {e}")))?;
                len += n as u64;
            }
            w.sync_all()
                .map_err(|e| StoreError::unavailable(true, format!("tmp fsync: {e}")))?;
            Ok((hex::encode(hasher.finalize()), len))
        })();
        let (sha256, len) = match streamed {
            Ok(v) => v,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        };
        // Phase 2（锁内）：前置条件 + rename + 目录 fsync。
        let _guard = self.cas_lock.lock().unwrap_or_else(|e| e.into_inner());
        let _lock = self.lock_key(key)?;
        if if_none_match && path.exists() {
            let _ = std::fs::remove_file(&tmp);
            return Err(StoreError::PreconditionFailed);
        }
        if let Err(e) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(StoreError::unavailable(true, format!("rename: {e}")));
        }
        // 目录 fsync：rename 持久化（与 atomic_commit 同口径）。
        let d = std::fs::File::open(dir)
            .map_err(|e| StoreError::unavailable(true, format!("dir open: {e}")))?;
        d.sync_all()
            .map_err(|e| StoreError::unavailable(true, format!("dir fsync: {e}")))?;
        // fs 后端 etag = 内容 sha256（与 etag_of 同口径）。
        Ok(FilePutOutcome {
            etag: sha256.clone(),
            sha256,
            len,
        })
    }

    async fn list(&self, prefix: &str) -> Result<ListPage, StoreError> {
        // T36b：从最长目录前缀开始遍历（O(匹配子树) 而非 O(全树)）。
        // "prefix 可落目录边界中间"（`db/ab` 须匹配 `db/abc/x`）→
        // 遍历锚点是最后一个 '/' 之前部分；无 '/' → 全树。
        if prefix.starts_with('/')
            || prefix.split('/').any(|seg| seg == "..")
            || prefix.contains('\0')
        {
            return Err(StoreError::unavailable(false, "invalid list prefix"));
        }
        let walk_base = match prefix.rfind('/') {
            Some(i) => self.root.join(&prefix[..i + 1]),
            None => self.root.clone(),
        };
        let mut out = Vec::new();
        let mut stack = vec![walk_base];
        while let Some(dir) = stack.pop() {
            let rd = match std::fs::read_dir(&dir) {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StoreError::unavailable(true, format!("readdir: {e}"))),
            };
            for ent in rd {
                let ent =
                    ent.map_err(|e| StoreError::unavailable(true, format!("readdir: {e}")))?;
                let p = ent.path();
                let ft = ent
                    .file_type()
                    .map_err(|e| StoreError::unavailable(true, format!("filetype: {e}")))?;
                if ft.is_dir() {
                    stack.push(p);
                } else if ft.is_file() {
                    let rel = p
                        .strip_prefix(&self.root)
                        .map_err(|_| StoreError::unavailable(false, "strip prefix"))?;
                    let key = rel.to_string_lossy().replace('\\', "/");
                    if key.starts_with(prefix) && !key.ends_with(".lock") && !key.contains(".tmp-")
                    {
                        out.push(ListEntry {
                            key,
                            size: ent.metadata().map(|m| m.len()).unwrap_or(0),
                        });
                    }
                }
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        // fs 遍历一次走完整棵子树——无分页界，truncated 恒假。
        Ok(ListPage {
            entries: out,
            truncated: false,
        })
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let path = self.path_of(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::unavailable(true, format!("delete: {e}"))),
        }
    }
}
