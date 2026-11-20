//! S3-compatible object store: shares the [`crate::sigv4`] signing, path-style addressing,
//! If-Match / If-None-Match conditional writes, ListObjectsV2 with bounded pagination.
//!
//! Compatibility (section 12 unproven item): S3 conditional-write semantics differ across backends (R2/MinIO/newer AWS);
//! when `conditional_writes=false`, CAS calls return
//! `Unavailable{retryable:false}` directly -- fail closed, never degrade to a non-atomic write.

use super::{
    sigv4::{sha256_hex, uri_escape, SecretString, SignedRequest, Signer},
    FileGetOutcome, ListEntry, ListPage, ObjectMeta, ObjectStore, StoreError, StoredObject,
};
use quick_xml::de::from_str as xml_from_str;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

/// S3 store configuration.
#[derive(Clone)]
pub struct S3StoreConfig {
    /// Base endpoint, e.g. `https://s3.internal:9000` (no path).
    pub endpoint: String,
    /// Signing region.
    pub region: String,
    /// Physical bucket.
    pub bucket: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: SecretString,
    /// Per-request timeout.
    pub timeout: Duration,
    /// The backend supports S3 conditional-write headers; `false` -> CAS fails closed.
    pub conditional_writes: bool,
    /// Retry count for idempotent operations (GET/HEAD/list/delete).
    pub retries: u32,
    /// Optional key prefix (isolates deployments sharing a bucket).
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

/// S3 store.
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

// RV-R8 #7: the cap on the pagination bound is only a DoS guardrail -- when hit, the caller is told explicitly via
// `ListPage.truncated`, and tail keys are never silently dropped.
// 64 pages x 1000 keys covers a reasonable size for a single class/db.
const LIST_FETCH_BOUND: usize = 64;
const LIST_KEY_BOUND: usize = 65_536;
const MAX_OBJECT_BYTES: usize = 256 * 1024 * 1024;

impl S3Store {
    /// Build (timeout is enforced at the client level).
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
        // path-style: /{bucket}/{key}, escape key segment by segment to keep slashes.
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
        // T12/G7 fail-closed (same as provider send): the signed path must
        // equal the on-the-wire path; key already has `.`/`..` rejected by validate_key, so any divergence
        // is a bug rather than input.
        let parsed =
            reqwest::Url::parse(&url).map_err(|_| StoreError::unavailable(false, "url parse"))?;
        if parsed.path() != req.path {
            return Err(StoreError::unavailable(false, "signed path diverged"));
        }
        let mut b = self.client.request(req.method.clone(), url);
        for (name, value) in headers.iter() {
            b = b.header(name, value);
        }
        // RV-R8 #1: the payload hash committed to in the signature is exactly these bytes -- they must
        // really be attached to the request; omitting them would make every PUT upload an empty object.
        // R2 answers 411 to an empty-body PUT/POST lacking Content-Length: methods with a body
        // (including empty bodies) always declare the length explicitly (0 for empty). content-length
        // is not in the SigV4 SignedHeaders (unrelated to the payload hash, the signature is unchanged).
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
        // 5xx/429 are retryable; other 4xx are configuration/semantic errors.
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
        // Assume truncation by default: only clear it when a page declares "no more" and we did not hit the bound
        // -- any path that hits the bound keeps truncated=true and hands it to the
        // caller to fail closed (RV-R8 #7).
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

    /// RV-R31 #5: streaming download -- the body is written and hashed with sha256 as it arrives, so snapshots no longer
    /// go wholly into memory (MAX_OBJECT_BYTES constrains put; the GET cap is
/// validated by the caller against the snapshot budget).
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
                // 2026-11-15: finalize() is already a digest -- it must not go through sha256_hex again
                // (that would compute sha256(sha256(x)), and restore would never match the sha).
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
        // If-None-Match: * -- write only if the key does not exist.
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
