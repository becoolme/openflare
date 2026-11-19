//! S3 兼容对象存储：共享 [`crate::sigv4`] 签名，path-style 寻址，
//! If-Match / If-None-Match 条件写，ListObjectsV2 有界分页。
//!
//! 兼容性（§12 未证实项）：S3 条件写在不同后端（R2/MinIO/AWS 新版）
//! 语义不一；`conditional_writes=false` 时 CAS 调用直接
//! `Unavailable{retryable:false}`——fail closed，绝不降级非原子写。

use super::{
    sigv4::{sha256_hex, uri_escape, SecretString, SignedRequest, Signer},
    FileGetOutcome, ListEntry, ListPage, ObjectMeta, ObjectStore, StoreError, StoredObject,
};
use quick_xml::de::from_str as xml_from_str;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

/// S3 存储配置。
#[derive(Clone)]
pub struct S3StoreConfig {
    /// Base endpoint, e.g. `https://s3.internal:9000` (no path)。
    pub endpoint: String,
    /// Signing region。
    pub region: String,
    /// Physical bucket。
    pub bucket: String,
    /// Access key id。
    pub access_key: String,
    /// Secret access key。
    pub secret_key: SecretString,
    /// Per-request timeout。
    pub timeout: Duration,
    /// 后端支持 S3 条件写 headers；`false` → CAS fail closed。
    pub conditional_writes: bool,
    /// 幂等操作重试次数（GET/HEAD/list/delete）。
    pub retries: u32,
    /// 可选 key 前缀（隔离不同部署共用 bucket）。
    pub prefix: String,
}

impl std::fmt::Debug for S3StoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3StoreConfig")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("timeout", &self.timeout)
            .field("conditional_writes", &self.conditional_writes)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// S3 store。
pub struct S3Store {
    client: Client,
    signer: Signer,
    cfg: S3StoreConfig,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store").field("cfg", &self.cfg).finish()
    }
}

// RV-R8 #7: 分页上界之上限本身只是 DoS 护栏——命中时通过
// `ListPage.truncated` 显式告知调用方，绝不静默丢尾部 key。
// 64 页 × 1000 keys 覆盖单个 class/库的合理规模。
const LIST_FETCH_BOUND: usize = 64;
const LIST_KEY_BOUND: usize = 65_536;
const MAX_OBJECT_BYTES: usize = 256 * 1024 * 1024;

impl S3Store {
    /// 构建（timeout 由 client 级强制）。
    pub fn new(cfg: S3StoreConfig) -> Result<Self, StoreError> {
        if cfg.endpoint.is_empty() || cfg.bucket.is_empty() {
            return Err(StoreError::unavailable(false, "s3 endpoint/bucket empty"));
        }
        if super::sigv4::host_of(&cfg.endpoint).is_err() {
            return Err(StoreError::unavailable(false, "s3 endpoint has no host"));
        }
        let client = Client::builder()
            .timeout(cfg.timeout)
            .connect_timeout(cfg.timeout.min(Duration::from_secs(5)))
            .build()
            .map_err(|_| StoreError::unavailable(false, "reqwest client build"))?;
        let signer = Signer {
            endpoint: cfg.endpoint.trim_end_matches('/').to_string(),
            region: cfg.region.clone(),
            access_key: cfg.access_key.clone(),
            secret_key: cfg.secret_key.clone(),
        };
        Ok(Self {
            client,
            signer,
            cfg,
        })
    }

    fn object_path(&self, key: &str) -> Result<String, StoreError> {
        super::validate_key(key)?;
        let full = if self.cfg.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.cfg.prefix.trim_matches('/'), key)
        };
        // path-style：/{bucket}/{key}，key 逐段 escape 保 slash。
        let mut p = String::with_capacity(full.len() + self.cfg.bucket.len() + 2);
        p.push('/');
        p.push_str(&self.cfg.bucket);
        for seg in full.split('/') {
            p.push('/');
            p.push_str(&uri_escape(seg, true));
        }
        Ok(p)
    }

    async fn send(&self, req: SignedRequest) -> Result<reqwest::Response, StoreError> {
        let headers = self
            .signer
            .sign(&req)
            .map_err(|_| StoreError::unavailable(false, "sign"))?;
        let url = self.signer.url_of(&req);
        // T12/G7 fail-closed（与 provider send 同款）：签名 path 必须
        // 等于上线 path；key 已由 validate_key 拒 `.`/`..`，任何分叉
        // 是 bug 而非输入。
        let parsed =
            reqwest::Url::parse(&url).map_err(|_| StoreError::unavailable(false, "url parse"))?;
        if parsed.path() != req.path {
            return Err(StoreError::unavailable(false, "signed path diverged"));
        }
        let mut b = self.client.request(req.method.clone(), url);
        for (name, value) in headers.iter() {
            b = b.header(name, value);
        }
        // RV-R8 #1: 签名里承诺的 payload hash 就是这些字节——必须
        // 真的附到请求上；漏掉会让所有 PUT 上传空对象。
        // R2 对缺少 Content-Length 的空 body PUT/POST 回 411：带 body 的
        // 方法（含空 body）一律显式声明长度（空为 0）。content-length
        // 不进 SigV4 SignedHeaders（与 payload hash 无关，签名不变）。
        if matches!(req.method, reqwest::Method::PUT | reqwest::Method::POST) {
            b = b.header(reqwest::header::CONTENT_LENGTH, req.body.len().to_string());
        }
        b.body(req.body).send().await.map_err(|e| {
            StoreError::unavailable(e.is_timeout() || e.is_connect(), format!("send: {e}"))
        })
    }

    async fn head_raw(&self, key: &str) -> Result<Option<(u16, ObjectMeta)>, StoreError> {
        let path = self.object_path(key)?;
        let req = SignedRequest {
            method: reqwest::Method::HEAD,
            path,
            query: Vec::new(),
            headers: Vec::new(),
            body: Vec::new(),
        };
        let resp = self.send(req).await?;
        let status = resp.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            return Err(status_err(status));
        }
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let size = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        Ok(Some((status, ObjectMeta { etag, size })))
    }

    async fn put_raw(
        &self,
        key: &str,
        body: &[u8],
        cond_header: Option<(String, String)>,
    ) -> Result<String, StoreError> {
        if body.len() > MAX_OBJECT_BYTES {
            return Err(StoreError::unavailable(false, "object too large"));
        }
        let path = self.object_path(key)?;
        let headers: Vec<(String, String)> = match cond_header {
            Some((k, v)) => vec![(k, v)],
            None => Vec::new(),
        };
        let req = SignedRequest {
            method: reqwest::Method::PUT,
            path,
            query: Vec::new(),
            headers,
            body: body.to_vec(),
        };
        let resp = self.send(req).await?;
        let status = resp.status().as_u16();
        match status {
            200..=299 => {
                let etag = resp
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.trim_matches('"').to_string())
                    .unwrap_or_else(|| sha256_hex(body));
                Ok(etag)
            }
            404 | 412 => Err(StoreError::PreconditionFailed),
            s => Err(status_err(s)),
        }
    }
}

fn status_err(status: u16) -> StoreError {
    match status {
        // 5xx/429 可重试；其余 4xx 属配置/语义错误。
        429 | 500..=599 => StoreError::unavailable(true, format!("s3 status {status}")),
        s => StoreError::unavailable(false, format!("s3 status {s}")),
    }
}

impl S3Store {
    /// Shared ListObjectsV2 walk (RV-R28 #3): `start_after` maps to
    /// the S3 `start-after` param (exclusive resume cursor) and
    /// `delimiter` folds deeper keys into `CommonPrefixes`. Both are
    /// bounded by the same LIST_* guards and report `truncated`.
    async fn list_v2(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<super::ListChildrenPage, StoreError> {
        #[derive(Deserialize)]
        struct ListBucketResult {
            #[serde(rename = "Contents", default)]
            contents: Vec<Contents>,
            // S3 emits one <CommonPrefixes><Prefix>…</Prefix></CommonPrefixes>
            // element per folded prefix (repeated siblings).
            #[serde(rename = "CommonPrefixes", default)]
            common_prefixes: Vec<CommonPrefixEntry>,
            #[serde(rename = "IsTruncated", default)]
            truncated: bool,
            #[serde(rename = "NextContinuationToken")]
            next_token: Option<String>,
        }
        #[derive(Deserialize)]
        struct CommonPrefixEntry {
            #[serde(rename = "Prefix")]
            prefix: String,
        }
        #[derive(Deserialize)]
        struct Contents {
            #[serde(rename = "Key")]
            key: String,
            #[serde(rename = "Size", default)]
            size: u64,
        }
        let full_prefix = if self.cfg.prefix.is_empty() {
            prefix.to_string()
        } else {
            format!("{}/{}", self.cfg.prefix.trim_matches('/'), prefix)
        };
        let strip = self.cfg.prefix.trim_matches('/');
        let strip_logical = |physical: String| -> Option<String> {
            if strip.is_empty() {
                Some(physical)
            } else {
                physical
                    .strip_prefix(format!("{strip}/").as_str())
                    .map(|s| s.to_string())
            }
        };
        let full_start_after = start_after.map(|a| {
            if self.cfg.prefix.is_empty() {
                a.to_string()
            } else {
                format!("{}/{}", self.cfg.prefix.trim_matches('/'), a)
            }
        });
        let mut out: Vec<ListEntry> = Vec::new();
        let mut commons: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        let mut fetches = 0usize;
        // 默认假定截断：只有当某页声明"无更多"且我们未触上界时
        // 才清零——任一路径命中界值都会保留 truncated=true 交给
        // 调用方 fail closed（RV-R8 #7）。
        let mut truncated = true;
        while fetches < LIST_FETCH_BOUND && out.len() + commons.len() < LIST_KEY_BOUND {
            fetches += 1;
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), full_prefix.clone()),
                ("max-keys".to_string(), "1000".to_string()),
            ];
            if let Some(d) = delimiter {
                query.push(("delimiter".to_string(), d.to_string()));
            }
            if let Some(a) = &full_start_after {
                query.push(("start-after".to_string(), a.clone()));
            }
            if let Some(t) = &token {
                query.push(("continuation-token".to_string(), t.clone()));
            }
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path: format!("/{}", self.cfg.bucket),
                query,
                headers: Vec::new(),
                body: Vec::new(),
            };
            let resp = self.send(req).await?;
            if !resp.status().is_success() {
                return Err(status_err(resp.status().as_u16()));
            }
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| StoreError::unavailable(true, format!("list body: {e}")))?;
            let page: ListBucketResult = xml_from_str(
                std::str::from_utf8(&bytes)
                    .map_err(|_| StoreError::Corrupt("list xml not utf8".into()))?,
            )
            .map_err(|_| StoreError::Corrupt("list xml parse".into()))?;
            for c in page.contents {
                let Some(logical) = strip_logical(c.key) else {
                    continue;
                };
                if logical.starts_with(prefix) {
                    out.push(ListEntry {
                        key: logical,
                        size: c.size,
                    });
                }
                if out.len() + commons.len() >= LIST_KEY_BOUND {
                    break;
                }
            }
            for cp in page.common_prefixes {
                if let Some(logical) = strip_logical(cp.prefix) {
                    if logical.starts_with(prefix) && !commons.contains(&logical) {
                        commons.push(logical);
                    }
                }
                if out.len() + commons.len() >= LIST_KEY_BOUND {
                    break;
                }
            }
            if out.len() + commons.len() >= LIST_KEY_BOUND {
                break;
            }
            token = page.next_token;
            if !page.truncated || token.is_none() {
                truncated = false;
                break;
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        commons.sort();
        Ok(super::ListChildrenPage {
            entries: out,
            common_prefixes: commons,
            truncated,
        })
    }

}

impl S3Store {
    /// RV-R71: bounded, jittered, deadline-limited retry for IDEMPOTENT
    /// requests only (GET/HEAD/DELETE/streamed GET). Conditional writes
    /// (`put_if_*`) and plain `put` never go through here — a retried
    /// CAS could mask a lost-race and is single-shot by design.
    /// Total window = 2 x per-request timeout (cap 60 s), so one slow
    /// object store cannot pin a caller for `retries x timeout`.
    async fn idempotent<F, Fut, T>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, StoreError>>,
    {
        let plan = super::deadline::RetryPlan {
            max_attempts: self.cfg.retries.max(1),
            ..super::deadline::RetryPlan::default()
        };
        let budget = self
            .cfg
            .timeout
            .saturating_mul(2)
            .min(Duration::from_secs(60));
        super::deadline::retry_until(&plan, super::deadline::Deadline::after(budget), f).await
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3Store {
    async fn get(&self, key: &str) -> Result<Option<StoredObject>, StoreError> {
        self.idempotent(|| async {
            let path = self.object_path(key)?;
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path,
                query: Vec::new(),
                headers: Vec::new(),
                body: Vec::new(),
            };
            let resp = self.send(req).await?;
            let status = resp.status().as_u16();
            if status == 404 {
                return Ok(None);
            }
            if !(200..300).contains(&status) {
                return Err(status_err(status));
            }
            let etag = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            let body = resp
                .bytes()
                .await
                .map_err(|e| StoreError::unavailable(true, format!("body: {e}")))?
                .to_vec();
            Ok(Some(StoredObject {
                meta: ObjectMeta {
                    etag,
                    size: body.len() as u64,
                },
                body,
            }))
        })
        .await
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, StoreError> {
        self.idempotent(|| async {
            Ok(self.head_raw(key).await?.map(|(_, m)| m))
        })
        .await
    }

    /// RV-R31 #5：流式下载——body 边到边写边算 sha256，快照不再
    /// 整体进内存（MAX_OBJECT_BYTES 约束的是 put；GET 上限由
    /// 调用方按快照预算校验）。
    async fn get_to_file(
        &self,
        key: &str,
        dest: &std::path::Path,
    ) -> Result<Option<FileGetOutcome>, StoreError> {
        self.idempotent(|| async {
            let path = self.object_path(key)?;
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path,
                query: Vec::new(),
                headers: Vec::new(),
                body: Vec::new(),
            };
            let resp = self.send(req).await?;
            let status = resp.status().as_u16();
            if status == 404 {
                return Ok(None);
            }
            if !(200..300).contains(&status) {
                return Err(status_err(status));
            }
            let etag = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            use futures_util::StreamExt;
            use sha2::Digest;
            use std::io::Write;
            let mut w = std::fs::File::create(dest)
                .map_err(|e| StoreError::unavailable(true, format!("get_to_file dst: {e}")))?;
            let mut hasher = sha2::Sha256::new();
            let mut len = 0u64;
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let c = chunk
                    .map_err(|e| StoreError::unavailable(true, format!("body stream: {e}")))?;
                hasher.update(&c);
                w.write_all(&c).map_err(|e| {
                    StoreError::unavailable(true, format!("get_to_file write: {e}"))
                })?;
                len += c.len() as u64;
            }
            Ok(Some(FileGetOutcome {
                etag,
                // 2026-11-15：finalize() 已是摘要——不得再过 sha256_hex
                // （那会算 sha256(sha256(x))，restore 永远 sha 不符）。
                sha256: hex::encode(hasher.finalize()),
                len,
            }))
        })
        .await
    }

    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        if !self.cfg.conditional_writes {
            return Err(StoreError::unavailable(
                false,
                "s3 conditional writes unsupported",
            ));
        }
        // If-None-Match: * —— key 不存在才写。
        self.put_raw(key, body, Some(("if-none-match".into(), "*".into())))
            .await
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError> {
        if !self.cfg.conditional_writes {
            return Err(StoreError::unavailable(
                false,
                "s3 conditional writes unsupported",
            ));
        }
        self.put_raw(
            key,
            body,
            Some(("if-match".into(), format!("\"{expected}\""))),
        )
        .await
    }

    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.put_raw(key, body, None).await
    }

    async fn list(&self, prefix: &str) -> Result<ListPage, StoreError> {
        let page = self.list_v2(prefix, None, None).await?;
        Ok(ListPage {
            entries: page.entries,
            truncated: page.truncated,
        })
    }

    /// RV-R28 #3: native `start-after` cursor — the GC sweep resumes
    /// past a truncated page instead of restarting at the head.
    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        let page = self.list_v2(prefix, start_after, None).await?;
        Ok(ListPage {
            entries: page.entries,
            truncated: page.truncated,
        })
    }

    /// RV-R28 #3: native delimiter listing — WAL stem discovery
    /// enumerates `{wpfx}{stem}/` CommonPrefixes, bounded by stem
    /// count, not by total segment count.
    async fn list_children(
        &self,
        prefix: &str,
        delimiter: &str,
    ) -> Result<super::ListChildrenPage, StoreError> {
        self.list_v2(prefix, None, Some(delimiter)).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.idempotent(|| async {
            let path = self.object_path(key)?;
            let req = SignedRequest {
                method: reqwest::Method::DELETE,
                path,
                query: Vec::new(),
                headers: Vec::new(),
                body: Vec::new(),
            };
            let resp = self.send(req).await?;
            let status = resp.status().as_u16();
            match status {
                200..=299 | 404 => Ok(()),
                s => Err(status_err(s)),
            }
        })
        .await
    }
}
