//! 内存对象存储（测试/离线）。真 CAS：全局锁内 compare+swap。
//! 附带失败注入钩子（`fail_next_*`）供对抗测试。

use super::{ListEntry, ListPage, ObjectMeta, ObjectStore, StoreError, StoredObject};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone)]
struct Entry {
    body: Vec<u8>,
    /// etag = sha256(body) hex —— CAS 以内容哈希为版本。
    etag: String,
}

/// 内存 store。`Clone` 共享同一张表（两 registry 共享场景）。
#[derive(Default)]
pub struct MemoryStore {
    inner: std::sync::Arc<Mutex<MemInner>>,
}

#[derive(Default)]
struct MemInner {
    objs: BTreeMap<String, Entry>,
    /// 失败注入：下一次匹配操作返回该错误一次。
    inject_get: Vec<StoreError>,
    inject_put: Vec<StoreError>,
    inject_list: Vec<StoreError>,
    /// T36b：put* 调用计数（退避/节流测试观察口）。
    put_calls: usize,
}

fn etag_of(body: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(body))
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注入：接下来 `n` 次 put* 调用返回该错误。
    pub fn inject_put_failures(&self, errs: Vec<StoreError>) {
        self.inner.lock().unwrap().inject_put = errs;
    }
    /// 注入：接下来 `n` 次 get 调用返回该错误。
    pub fn inject_get_failures(&self, errs: Vec<StoreError>) {
        self.inner.lock().unwrap().inject_get = errs;
    }
    /// 注入：接下来 `n` 次 list 调用返回该错误。
    pub fn inject_list_failures(&self, errs: Vec<StoreError>) {
        self.inner.lock().unwrap().inject_list = errs;
    }

    /// 测试观察口：put* 累计调用数（含注入失败）。
    pub fn put_calls(&self) -> usize {
        self.inner.lock().unwrap().put_calls
    }

    /// 测试观察口：对象数。
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().objs.len()
    }
    /// 测试观察口：所有 key。
    pub fn keys(&self) -> Vec<String> {
        self.inner.lock().unwrap().objs.keys().cloned().collect()
    }
    /// 测试篡改口：直接覆盖对象内容（绕过 CAS，制造坏段）。
    pub fn corrupt(&self, key: &str, body: Vec<u8>) {
        let etag = etag_of(&body);
        self.inner
            .lock()
            .unwrap()
            .objs
            .insert(key.to_string(), Entry { body, etag });
    }
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ObjectStore for MemoryStore {
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, StoreError> {
        super::validate_key(key)?;
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.inject_get.pop() {
            return Err(e);
        }
        Ok(g.objs.get(key).map(|e| StoredObject {
            meta: ObjectMeta {
                etag: e.etag.clone(),
                size: e.body.len() as u64,
            },
            body: e.body.clone(),
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError> {
        super::validate_key(key)?;
        let g = self.inner.lock().unwrap();
        Ok(g.objs.get(key).map(|e| ObjectMeta {
            etag: e.etag.clone(),
            size: e.body.len() as u64,
        }))
    }

    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        super::validate_key(key)?;
        let mut g = self.inner.lock().unwrap();
        g.put_calls += 1;
        if let Some(e) = g.inject_put.pop() {
            return Err(e);
        }
        if g.objs.contains_key(key) {
            return Err(StoreError::PreconditionFailed);
        }
        let etag = etag_of(body);
        g.objs.insert(
            key.to_string(),
            Entry {
                body: body.to_vec(),
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError> {
        super::validate_key(key)?;
        let mut g = self.inner.lock().unwrap();
        g.put_calls += 1;
        if let Some(e) = g.inject_put.pop() {
            return Err(e);
        }
        match g.objs.get(key) {
            Some(e) if e.etag == expected => {
                let etag = etag_of(body);
                g.objs.insert(
                    key.to_string(),
                    Entry {
                        body: body.to_vec(),
                        etag: etag.clone(),
                    },
                );
                Ok(etag)
            }
            Some(_) => Err(StoreError::PreconditionFailed),
            None => Err(StoreError::PreconditionFailed),
        }
    }

    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        super::validate_key(key)?;
        let mut g = self.inner.lock().unwrap();
        g.put_calls += 1;
        if let Some(e) = g.inject_put.pop() {
            return Err(e);
        }
        let etag = etag_of(body);
        g.objs.insert(
            key.to_string(),
            Entry {
                body: body.to_vec(),
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    async fn list(&self, prefix: &str) -> Result<ListPage, StoreError> {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.inject_list.pop() {
            return Err(e);
        }
        let entries = g
            .objs
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, e)| ListEntry {
                key: k.clone(),
                size: e.body.len() as u64,
            })
            .collect();
        Ok(ListPage {
            entries,
            truncated: false,
        })
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        super::validate_key(key)?;
        self.inner.lock().unwrap().objs.remove(key);
        Ok(())
    }
}
