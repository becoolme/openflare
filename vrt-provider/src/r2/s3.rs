//! S3-compatible [`R2Provider`] over `reqwest` + AWS SigV4 (path-style
//! addressing, `https://{endpoint}/{bucket}/{key}`).
//!
//! Layout: `vrt/{enc(tenantId)}/{enc(bucket)}/{logical key}` — tenant and
//! bucket segments are percent-escape encoded (`:`/`/` cannot be
//! injected); the logical key is stored verbatim.
//!
//! Object state lives in `x-amz-meta-vrt-*` headers: schema marker,
//! uuidv7 version, upload ms, canonical custom metadata (base64), HTTP
//! metadata fields, all five checksums, storage class. Reads fail closed
//! (`R2_OBJECT_METADATA_INVALID`) when the schema marker or integrity
//! fields are missing/corrupt — foreign objects under the prefix are
//! never reinterpreted as ours.
//!
//! Conditional semantics mirror upstream: `onlyIf` is evaluated against
//! the authoritative head before get/put; one conditional-read retry is
//! allowed when the backend reports a precondition failure mid-flight.
//! List performs a raw-key scan (S3 delimiter collapsing is NOT used so
//! `next_after` stays a raw-key resume point, identical to the memory
//! provider); the scan is bounded at 4 fetches / 4000 keys per call — a
//! page may under-fill inside very large delimiter groups, which is
//! documented and safe (keyset resume never repeats or skips keys).

use crate::context::BindingContext;
use crate::encoding::segment_encode;
use crate::error::ProviderError;
use crate::limits::{R2_MAX_KEY_BYTES, R2_MAX_LIST_LIMIT};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use quick_xml::de::from_str as xml_from_str;
use reqwest::header::HeaderMap;
use reqwest::Client;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use super::wire::{
    canonical_custom_metadata, check_min_part_sizes, etag_matches, hash_bytes, multipart_etag,
    resolve_range, validate_complete_parts, MultipartCreateResult, R2Condition, R2HttpMetadata,
    R2ListEntry, R2ListPage, R2MultipartCreateOptions, R2ObjectMetadata, R2PutOptions, R2Range,
    R2SsecKey, R2UploadedPart,
};
use super::{R2GetResult, R2Provider};
use vrt_objstore::sigv4::{self, SignedRequest, Signer};

/// T36: SecretString/SignedRequest/SigV4 are shared from vrt-objstore (extracted,
/// not copied). `vrt_provider::r2::s3::SecretString` is re-exported to keep the original path.
pub use vrt_objstore::sigv4::SecretString;

const META_PREFIX: &str = "x-amz-meta-vrt-";
const LIST_FETCH_BOUND: usize = 4;
const LIST_KEY_BOUND: usize = 4000;

/// Tail of the request budget reserved for the post-Complete
/// confirmation HEAD (see `complete_multipart`).
const CONFIRM_HEAD_RESERVE: Duration = Duration::from_secs(3);

/// S3 backend configuration.
#[derive(Clone)]
pub struct S3Config {
    /// Base endpoint, e.g. `https://s3.internal:9000` (no path).
    pub endpoint: String,
    /// Signing region.
    pub region: String,
    /// Physical bucket name.
    pub bucket: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: SecretString,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Single-part object byte cap.
    pub max_object_bytes: u64,
    /// Backend honors S3 conditional-write headers; `false` + `onlyIf`
    /// → `CapabilityUnsupported` (T13/M3, never non-atomic).
    pub conditional_writes: bool,
    /// Horizon for unfinished multipart uploads — determines the
    /// `expires_at_ms` the tenant token inherits. S3 itself reaps
    /// uploads only via bucket `AbortIncompleteMultipartUpload`
    /// lifecycle rules; this value MUST mirror the configured rule
    /// (default 7 days, matching upstream R2).
    pub multipart_ttl_ms: i64,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("timeout", &self.timeout)
            .field("max_object_bytes", &self.max_object_bytes)
            .finish_non_exhaustive()
    }
}

/// S3-compatible object provider.
#[derive(Debug)]
pub struct S3R2Provider {
    client: Client,
    config: S3Config,
    /// T36: shared SigV4 signer (vrt-objstore).
    signer: Signer,
    /// SSE-C keys for in-flight multipart uploads. The Workers API has no
    /// key on `complete()`, so the backend remembers the key supplied at
    /// create (same model as workerd/R2, where the upload session owns
    /// it). Keys are never written to object metadata.
    multipart_ssec: Mutex<BTreeMap<String, R2SsecKey>>,
}

/// S3 SSE-C headers for a request that must present the customer key.
/// Empty for plain requests — never emitted speculatively.
fn ssec_headers(key: Option<&R2SsecKey>) -> Vec<(String, String)> {
    let Some(key) = key else {
        return Vec::new();
    };
    vec![
        (
            "x-amz-server-side-encryption-customer-algorithm".to_string(),
            "AES256".to_string(),
        ),
        (
            "x-amz-server-side-encryption-customer-key".to_string(),
            key.s3_key_b64(),
        ),
        (
            "x-amz-server-side-encryption-customer-key-MD5".to_string(),
            key.s3_key_md5_b64(),
        ),
    ]
}

/// S3 reports SSE-C trouble as a 400/403 on the object request. When the
/// request carried a key that is the typed SSE-C failure; without one it
/// stays a generic backend failure.
fn classify_ssec_failure(status: u16, ssec: Option<&R2SsecKey>) -> Option<ProviderError> {
    (ssec.is_some() && matches!(status, 400 | 403)).then_some(ProviderError::R2SsecInvalid)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl S3R2Provider {
    /// Build a provider from a prepared reqwest client + config.
    pub fn new(config: S3Config) -> Result<Self, ProviderError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|_| ProviderError::Internal)?;
        let signer = Signer {
            endpoint: config.endpoint.trim_end_matches('/').to_string(),
            region: config.region.clone(),
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
        };
        Ok(Self {
            client,
            config,
            signer,
            multipart_ssec: Mutex::new(BTreeMap::new()),
        })
    }

    /// Drop the remembered SSE-C key for a consumed/aborted upload.
    fn forget_upload_key(&self, upload_id: &str) {
        self.multipart_ssec
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(upload_id);
    }

    fn physical_key(&self, ctx: &BindingContext, logical: &str) -> String {
        let (tenant, bucket) = match ctx.resource() {
            crate::context::BindingResource::R2Bucket { name } => {
                (segment_encode(ctx.tenant_id()), segment_encode(name))
            }
            _ => unreachable!("R2Provider invoked with non-R2 resource"),
        };
        format!("vrt/{tenant}/{bucket}/{logical}")
    }

    /// T12/G7: the PHYSICAL key (prefix + logical) is what R2 stores —
    /// bound it by the same 1024-byte object-key cap so a short logical
    /// key cannot exceed the backend limit via the tenant prefix.
    /// Typed 4xx (`R2KeyTooLarge`), not provider-unavailable.
    fn checked_physical_key(
        &self,
        ctx: &BindingContext,
        logical: &str,
    ) -> Result<String, ProviderError> {
        let physical = self.physical_key(ctx, logical);
        if physical.len() > R2_MAX_KEY_BYTES {
            return Err(ProviderError::R2KeyTooLarge);
        }
        Ok(physical)
    }

    async fn send(
        &self,
        req: SignedRequest,
        deadline: std::time::Duration,
    ) -> Result<reqwest::Response, ProviderError> {
        let headers = self
            .signer
            .sign(&req)
            .map_err(|_| ProviderError::Internal)?;
        let url = self.signer.url_of(&req);
        // T12/G7 fail-closed: the path we SIGN must equal the path that
        // actually goes on the wire. `reqwest` normalizes `.`/`..` URL
        // segments — logical keys are rejected upstream for exactly this
        // reason, so any divergence here is a bug, not user input.
        // Internal (no request details leak) — never a fake success.
        if let Ok(parsed) = reqwest::Url::parse(&url) {
            if parsed.path() != req.path {
                return Err(ProviderError::Internal);
            }
        } else {
            return Err(ProviderError::Internal);
        }
        let send = self
            .client
            .request(req.method.clone(), &url)
            .headers(headers)
            .body(req.body.clone());
        tokio::time::timeout(deadline, send.send())
            .await
            .map_err(|_| ProviderError::R2ProviderUnavailable)?
            .map_err(|_| ProviderError::R2ProviderUnavailable)
    }

    /// HEAD the physical key; returns raw headers + status. `ssec` adds
    /// the customer-key headers required by an encrypted object.
    async fn s3_head(
        &self,
        physical: &str,
        ssec: Option<&R2SsecKey>,
        deadline: Duration,
    ) -> Result<Option<HeaderMap>, ProviderError> {
        let req = SignedRequest {
            method: reqwest::Method::HEAD,
            path: format!(
                "/{}/{}",
                self.config.bucket,
                sigv4::uri_escape(physical, false)
            ),
            query: Vec::new(),
            headers: ssec_headers(ssec),
            body: Vec::new(),
        };
        let response = self.send(req, deadline).await?;
        match response.status().as_u16() {
            200 => Ok(Some(response.headers().clone())),
            404 => Ok(None),
            status => {
                Err(classify_ssec_failure(status, ssec)
                    .unwrap_or(ProviderError::R2ProviderUnavailable))
            }
        }
    }

    fn decode_head(
        &self,
        logical: &str,
        headers: &HeaderMap,
        range: Option<R2Range>,
    ) -> Result<R2ObjectMetadata, ProviderError> {
        let meta = |name: &str| -> Option<String> {
            headers
                .get(format!("{META_PREFIX}{name}"))
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        if meta("schema").as_deref() != Some("1") {
            return Err(ProviderError::R2ObjectMetadataInvalid);
        }
        let version = meta("version").ok_or(ProviderError::R2ObjectMetadataInvalid)?;
        let uploaded: i64 = meta("uploaded")
            .and_then(|v| v.parse().ok())
            .ok_or(ProviderError::R2ObjectMetadataInvalid)?;
        let size: u64 = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or(ProviderError::R2ObjectMetadataInvalid)?;
        let etag = headers
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_matches('"').to_string())
            .ok_or(ProviderError::R2ObjectMetadataInvalid)?;
        let custom_metadata = match meta("custom") {
            Some(b64) => {
                let raw = B64
                    .decode(&b64)
                    .map_err(|_| ProviderError::R2ObjectMetadataInvalid)?;
                Some(
                    serde_json::from_slice::<BTreeMap<String, String>>(&raw)
                        .map_err(|_| ProviderError::R2ObjectMetadataInvalid)?,
                )
            }
            None => None,
        };
        let http_metadata = {
            let content_type = meta("ct");
            let content_language = meta("cl");
            let content_disposition = meta("cd");
            let content_encoding = meta("ce");
            let cache_control = meta("cc");
            let cache_expiry = meta("cxp").and_then(|v| v.parse::<i64>().ok());
            if [
                &content_type,
                &content_language,
                &content_disposition,
                &content_encoding,
                &cache_control,
            ]
            .iter()
            .any(|v| v.is_some())
                || cache_expiry.is_some()
            {
                Some(R2HttpMetadata {
                    content_type,
                    content_language,
                    content_disposition,
                    content_encoding,
                    cache_control,
                    cache_expiry,
                })
            } else {
                None
            }
        };
        let checksums = super::wire::R2Checksums {
            md5: meta("md5"),
            sha1: meta("sha1"),
            sha256: meta("sha256"),
            sha384: meta("sha384"),
            sha512: meta("sha512"),
        };
        // Multipart objects carry NO object checksums — S3 computes
        // only the composite etag; integrity was verified per part.
        let composite = etag.contains('-');
        if !composite && (checksums.md5.is_none() || checksums.sha256.is_none()) {
            return Err(ProviderError::R2ObjectMetadataInvalid);
        }
        Ok(R2ObjectMetadata {
            key: logical.to_string(),
            version,
            size,
            etag: etag.clone(),
            http_etag: format!("\"{etag}\""),
            uploaded,
            http_metadata,
            custom_metadata,
            range,
            checksums,
            storage_class: meta("class").unwrap_or_else(|| "Standard".to_string()),
            ssec_key_md5: meta("ssec-md5"),
        })
    }

    fn meta_headers(&self, metadata: &PutMetadata<'_>) -> Vec<(String, String)> {
        let mut headers = vec![
            ("x-amz-meta-vrt-schema".to_string(), "1".to_string()),
            (
                "x-amz-meta-vrt-version".to_string(),
                metadata.version.to_string(),
            ),
            (
                "x-amz-meta-vrt-uploaded".to_string(),
                metadata.uploaded.to_string(),
            ),
            ("x-amz-meta-vrt-md5".to_string(), metadata.md5.clone()),
            ("x-amz-meta-vrt-sha1".to_string(), metadata.sha1.clone()),
            ("x-amz-meta-vrt-sha256".to_string(), metadata.sha256.clone()),
            ("x-amz-meta-vrt-sha384".to_string(), metadata.sha384.clone()),
            ("x-amz-meta-vrt-sha512".to_string(), metadata.sha512.clone()),
            (
                "x-amz-meta-vrt-class".to_string(),
                metadata.class.to_string(),
            ),
        ];
        if let Some(custom) = &metadata.custom_b64 {
            headers.push(("x-amz-meta-vrt-custom".to_string(), custom.to_string()));
        }
        for (suffix, value) in &metadata.http_fields {
            headers.push((format!("x-amz-meta-vrt-{suffix}"), value.to_string()));
        }
        headers
    }

    /// Extract `<Code>` from an S3 `<Error>` XML body (best-effort).
    fn xml_error_code(body: &str) -> String {
        let start = body.find("<Code>").map(|i| i + 6);
        let end = body.find("</Code>");
        match (start, end) {
            (Some(s), Some(e)) if e > s => body[s..e].to_string(),
            _ => String::new(),
        }
    }

    /// Map an S3 error code to the typed family — client/multipart
    /// errors (`NoSuchUpload`, `InvalidPart`, `EntityTooSmall`,
    /// `InvalidPartOrder`) → `R2_MULTIPART_INVALID`; service faults →
    /// `R2_PROVIDER_UNAVAILABLE`.
    fn classify_s3_error_code(code: &str) -> ProviderError {
        match code {
            "NoSuchUpload" | "InvalidPart" | "InvalidPartOrder" | "EntityTooSmall"
            | "MalformedXML" | "InvalidArgument" => ProviderError::R2MultipartInvalid,
            "InternalError" | "ServiceUnavailable" | "SlowDown" | "" => {
                ProviderError::R2ProviderUnavailable
            }
            _ => ProviderError::R2ProviderUnavailable,
        }
    }

    /// `ListParts` — S3's authoritative per-upload part table
    /// (`partNumber → (etag, size)`), paginated at 1000/page, bounded
    /// to the 10 pages the 10k-part cap allows. 404/NoSuchUpload →
    /// `R2_MULTIPART_INVALID`.
    async fn s3_list_parts(
        &self,
        physical: &str,
        upload_id: &str,
        deadline: Duration,
    ) -> Result<BTreeMap<i32, (String, u64)>, ProviderError> {
        #[derive(Deserialize)]
        struct ListPartsResult {
            #[serde(rename = "Part", default)]
            parts: Vec<ListedPart>,
            #[serde(rename = "IsTruncated", default)]
            truncated: bool,
            #[serde(rename = "NextPartNumberMarker")]
            next_marker: Option<i32>,
        }
        #[derive(Deserialize)]
        struct ListedPart {
            #[serde(rename = "PartNumber")]
            part_number: i32,
            #[serde(rename = "ETag")]
            etag: String,
            #[serde(rename = "Size")]
            size: u64,
        }
        let mut out = BTreeMap::new();
        let mut marker = 0i32;
        for _ in 0..10 {
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path: format!(
                    "/{}/{}",
                    self.config.bucket,
                    sigv4::uri_escape(physical, false)
                ),
                query: vec![
                    ("uploadId".to_string(), upload_id.to_string()),
                    ("part-number-marker".to_string(), marker.to_string()),
                    ("max-parts".to_string(), "1000".to_string()),
                ],
                headers: Vec::new(),
                body: Vec::new(),
            };
            let response = self.send(req, deadline).await?;
            if response.status().as_u16() == 404 {
                return Err(ProviderError::R2MultipartInvalid);
            }
            if !response.status().is_success() {
                return Err(self.s3_error(response, deadline).await);
            }
            let bytes = tokio::time::timeout(deadline, response.bytes())
                .await
                .map_err(|_| ProviderError::R2ProviderUnavailable)?
                .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            let page: ListPartsResult = xml_from_str(
                std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
            )
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            for part in page.parts {
                out.insert(
                    part.part_number,
                    (part.etag.trim_matches('"').to_string(), part.size),
                );
            }
            if !page.truncated {
                return Ok(out);
            }
            marker = match page.next_marker {
                Some(m) => m,
                None => return Ok(out),
            };
        }
        Ok(out)
    }

    /// Post-failure verification for an ambiguous
    /// `CompleteMultipartUpload`: HEAD the object; commit is confirmed
    /// only when its etag equals the composite derived from the
    /// request's own part etags (same model as `put`'s version check).
    /// Anything else → `R2_RESULT_UNKNOWN`, never a false success.
    async fn confirm_ambiguous_complete(
        &self,
        ctx: &BindingContext,
        physical: &str,
        key: &str,
        expected_etag: &str,
        ssec: Option<&R2SsecKey>,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        match self.s3_head(physical, ssec, ctx.remaining()).await {
            Ok(Some(headers)) => {
                let head_etag = headers
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim_matches('"').to_string())
                    .unwrap_or_default();
                if etag_matches(&head_etag, expected_etag) {
                    Ok(Some(
                        self.decode_head(key, &headers, None)?.completed_multipart(),
                    ))
                } else {
                    // Object exists but under a different etag — the
                    // state is real but not ours to explain.
                    Err(ProviderError::R2ResultUnknown)
                }
            }
            // HEAD 404: the object definitively did NOT commit — a
            // retryable service failure, not an unknown.
            Ok(None) => Err(ProviderError::R2ProviderUnavailable),
            Err(_) => Err(ProviderError::R2ResultUnknown),
        }
    }

    /// W5-r4 retry idempotency: `NoSuchUpload` on a complete RETRY is
    /// ambiguous in the opposite direction — the upload is gone, but
    /// the previous attempt may have committed it before its response
    /// faulted. HEAD the object and compare against the composite etag
    /// derived from THIS request's part list (ListParts is useless
    /// here — the upload is already consumed). Match → the exact
    /// content requested is the committed object → idempotent success.
    /// Absent or mismatched → the typed invalid stands; unreachable →
    /// `R2_RESULT_UNKNOWN` (never a false failure, never a false
    /// success).
    async fn confirm_committed_retry(
        &self,
        ctx: &BindingContext,
        physical: &str,
        key: &str,
        expected_etag: &str,
        ssec: Option<&R2SsecKey>,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        match self.s3_head(physical, ssec, ctx.remaining()).await {
            Ok(Some(headers)) => {
                let head_etag = headers
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim_matches('"').to_string())
                    .unwrap_or_default();
                if etag_matches(&head_etag, expected_etag) {
                    Ok(Some(
                        self.decode_head(key, &headers, None)?.completed_multipart(),
                    ))
                } else {
                    Err(ProviderError::R2MultipartInvalid)
                }
            }
            Ok(None) => Err(ProviderError::R2MultipartInvalid),
            Err(_) => Err(ProviderError::R2ResultUnknown),
        }
    }

    /// Read a non-2xx S3 error body (bounded) and map to a typed error.
    async fn s3_error(&self, response: reqwest::Response, deadline: Duration) -> ProviderError {
        let status = response.status().as_u16();
        if status == 404 {
            return ProviderError::R2MultipartInvalid;
        }
        let bytes = match tokio::time::timeout(deadline, response.bytes()).await {
            Ok(Ok(b)) => b,
            _ => return ProviderError::R2ProviderUnavailable,
        };
        let code = Self::xml_error_code(std::str::from_utf8(&bytes).unwrap_or(""));
        let _ = code; // classify via code table below
        Self::classify_s3_error_code(&code)
    }
}

struct PutMetadata<'a> {
    version: &'a str,
    uploaded: i64,
    md5: String,
    sha1: String,
    sha256: String,
    sha384: String,
    sha512: String,
    class: &'a str,
    custom_b64: Option<String>,
    http_fields: Vec<(&'a str, &'a str)>,
}

#[async_trait]
impl R2Provider for S3R2Provider {
    async fn head(
        &self,
        ctx: &BindingContext,
        key: &str,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let physical = self.checked_physical_key(ctx, key)?;
        let headers = self.s3_head(&physical, None, ctx.remaining()).await?;
        headers
            .map(|headers| self.decode_head(key, &headers, None))
            .transpose()
    }

    async fn get(
        &self,
        ctx: &BindingContext,
        key: &str,
        range: Option<R2Range>,
        only_if: Option<&R2Condition>,
        ssec_key: Option<&R2SsecKey>,
    ) -> Result<R2GetResult, ProviderError> {
        let physical = self.checked_physical_key(ctx, key)?;
        for attempt in 0..2 {
            let headers = match self.s3_head(&physical, ssec_key, ctx.remaining()).await? {
                Some(headers) => headers,
                None => return Ok(R2GetResult::Missing),
            };
            let head_meta = self.decode_head(key, &headers, None)?;
            if let Some(condition) = only_if {
                if !condition.matches_object(&head_meta.etag, head_meta.uploaded) {
                    return Ok(R2GetResult::Precondition(head_meta));
                }
            }
            let resolved = range
                .map(|r| resolve_range(r, head_meta.size))
                .transpose()?;
            let mut request_headers = ssec_headers(ssec_key);
            if let Some((start, end)) = resolved {
                request_headers.push(("range".to_string(), format!("bytes={start}-{end}")));
            }
            if only_if.is_some() {
                request_headers.push(("if-match".to_string(), head_meta.http_etag.clone()));
            }
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path: format!(
                    "/{}/{}",
                    self.config.bucket,
                    sigv4::uri_escape(&physical, false)
                ),
                query: Vec::new(),
                headers: request_headers,
                body: Vec::new(),
            };
            let response = self.send(req, ctx.remaining()).await?;
            match response.status().as_u16() {
                200 | 206 => {
                    // T13/H3: never buffer an unbounded body — Content-
                    // Length precheck, then stream chunk-wise up to the
                    // configured cap.
                    let cap = self.config.max_object_bytes;
                    if let Some(cl) = response
                        .headers()
                        .get(reqwest::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                    {
                        if cl > cap {
                            return Err(ProviderError::R2ObjectTooLarge);
                        }
                    }
                    let mut metadata = self.decode_head(key, response.headers(), None)?;
                    metadata.range = resolved.map(|(start, end)| R2Range {
                        offset: Some(start),
                        length: Some(end - start + 1),
                        suffix: None,
                    });
                    let mut bytes = Vec::new();
                    let mut resp = response;
                    loop {
                        let chunk = tokio::time::timeout(ctx.remaining(), resp.chunk())
                            .await
                            .map_err(|_| ProviderError::R2ProviderUnavailable)?
                            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
                        let Some(chunk) = chunk else { break };
                        if bytes.len() as u64 + chunk.len() as u64 > cap {
                            return Err(ProviderError::R2ObjectTooLarge);
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    return Ok(R2GetResult::Body(metadata, bytes));
                }
                404 => return Ok(R2GetResult::Missing),
                412 if only_if.is_some() && attempt == 0 => continue,
                status => {
                    return Err(classify_ssec_failure(status, ssec_key)
                        .unwrap_or(ProviderError::R2ProviderUnavailable))
                }
            }
        }
        Err(ProviderError::R2ProviderUnavailable)
    }

    async fn put(
        &self,
        ctx: &BindingContext,
        key: &str,
        body: &[u8],
        options: &R2PutOptions,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let custom_b64 = if options.custom_metadata.is_empty() {
            None
        } else {
            Some(B64.encode(canonical_custom_metadata(&options.custom_metadata)?))
        };
        if body.len() as u64 > self.config.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let physical = self.checked_physical_key(ctx, key)?;
        // T13/M3: `onlyIf` is evaluated on a HEAD snapshot AND pushed
        // down atomically — the PUT carries `If-Match: <etag-seen>` (or
        // `If-None-Match: *` for a missing object), so a concurrent
        // write between HEAD and PUT yields 412 instead of a silent
        // TOCTOU overwrite. One bounded retry re-reads and re-evaluates.
        if options.only_if.is_some() && !self.config.conditional_writes {
            return Err(ProviderError::CapabilityUnsupported);
        }
        let checksums = hash_bytes(body);
        if let Some(expected) = &options.checksum {
            if !checksums.matches(expected) {
                return Err(ProviderError::R2ChecksumMismatch);
            }
        }
        // Bounded conditional-write attempts: evaluate on HEAD, write
        // atomically via the etag anchor, retry once on a 412 race.
        for attempt in 0..2u8 {
            let current = match self
                .s3_head(&physical, options.ssec_key.as_ref(), ctx.remaining())
                .await?
            {
                Some(headers) => Some(self.decode_head(key, &headers, None)?),
                None => None,
            };
            let allowed = match (&options.only_if, &current) {
                (Some(condition), Some(meta)) => {
                    condition.matches_object(&meta.etag, meta.uploaded)
                }
                (Some(condition), None) => condition.matches_missing(),
                (None, _) => true,
            };
            if !allowed {
                return Ok(None);
            }
            let version = uuid::Uuid::now_v7().hyphenated().to_string();
            let uploaded = now_ms();
            let http_fields: Vec<(&str, &str)> = [
                ("ct", options.http_metadata.content_type.as_deref()),
                ("cl", options.http_metadata.content_language.as_deref()),
                ("cd", options.http_metadata.content_disposition.as_deref()),
                ("ce", options.http_metadata.content_encoding.as_deref()),
                ("cc", options.http_metadata.cache_control.as_deref()),
            ]
            .into_iter()
            .filter_map(|(suffix, value)| value.map(|v| (suffix, v)))
            .collect();
            let mut extra = self.meta_headers(&PutMetadata {
                version: &version,
                uploaded,
                md5: hex::encode(checksums.md5),
                sha1: hex::encode(checksums.sha1),
                sha256: hex::encode(checksums.sha256),
                sha384: hex::encode(checksums.sha384),
                sha512: hex::encode(checksums.sha512),
                class: options.storage_class.as_str(),
                custom_b64: custom_b64.clone(),
                http_fields: http_fields.clone(),
            });
            if let Some(expiry) = options.http_metadata.cache_expiry {
                extra.push(("x-amz-meta-vrt-cxp".to_string(), expiry.to_string()));
            }
            if let Some(ssec) = &options.ssec_key {
                // Stored for metadata reads; the key itself never is.
                extra.push(("x-amz-meta-vrt-ssec-md5".to_string(), ssec.md5_hex()));
                extra.extend(ssec_headers(Some(ssec)));
            }
            if options.only_if.is_some() {
                // Atomic anchor: object must still be exactly what HEAD
                // saw (or still absent).
                match &current {
                    Some(meta) => extra.push(("If-Match".to_string(), meta.http_etag.clone())),
                    None => extra.push(("If-None-Match".to_string(), "*".to_string())),
                }
            }
            let req = SignedRequest {
                method: reqwest::Method::PUT,
                path: format!(
                    "/{}/{}",
                    self.config.bucket,
                    sigv4::uri_escape(&physical, false)
                ),
                query: Vec::new(),
                headers: extra,
                body: body.to_vec(),
            };
            let response = self.send(req, ctx.remaining()).await.map_err(|e| {
                if e == ProviderError::R2ProviderUnavailable {
                    ProviderError::R2ResultUnknown
                } else {
                    e
                }
            })?;
            if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
                // Raced change between HEAD and PUT — re-evaluate once.
                if attempt == 0 {
                    continue;
                }
                return Ok(None);
            }
            if !response.status().is_success() {
                // S3 PutObject is atomic — a rejection is a known failure,
                // never an ambiguous commit.
                return Err(classify_ssec_failure(
                    response.status().as_u16(),
                    options.ssec_key.as_ref(),
                )
                .unwrap_or(ProviderError::R2ProviderUnavailable));
            }
            // Verify the committed object like upstream's verify_put:
            // head and compare version/size/checksum integrity.
            let headers = self
                .s3_head(&physical, options.ssec_key.as_ref(), ctx.remaining())
                .await?
                .ok_or(ProviderError::R2ProviderUnavailable)?;
            let committed = self.decode_head(key, &headers, None)?;
            if committed.version != version || committed.size != body.len() as u64 {
                return Err(ProviderError::R2ObjectMetadataInvalid);
            }
            return Ok(Some(committed));
        }
        unreachable!("conditional-write retry loop is bounded")
    }

    async fn delete(&self, ctx: &BindingContext, keys: &[String]) -> Result<(), ProviderError> {
        if keys.is_empty() {
            return Ok(());
        }
        #[derive(serde::Serialize)]
        #[serde(rename = "Delete")]
        struct DeleteBody<'a> {
            #[serde(rename = "Object")]
            objects: Vec<DeleteObject<'a>>,
            #[serde(rename = "Quiet")]
            quiet: bool,
        }
        #[derive(serde::Serialize)]
        struct DeleteObject<'a> {
            #[serde(rename = "Key")]
            key: &'a str,
        }
        #[derive(Deserialize)]
        struct DeleteResult {
            #[serde(rename = "Error", default)]
            errors: Vec<DeleteError>,
        }
        #[derive(Deserialize)]
        struct DeleteError {
            #[serde(rename = "Code")]
            code: String,
        }
        let physical: Vec<String> = keys
            .iter()
            .map(|k| self.checked_physical_key(ctx, k))
            .collect::<Result<_, _>>()?;
        let body = DeleteBody {
            objects: physical
                .iter()
                .map(|k| DeleteObject { key: k.as_str() })
                .collect(),
            quiet: true,
        };
        let xml = quick_xml::se::to_string(&body).map_err(|_| ProviderError::Internal)?;
        let req = SignedRequest {
            method: reqwest::Method::POST,
            path: format!("/{}", self.config.bucket),
            query: vec![("delete".to_string(), String::new())],
            headers: vec![
                ("content-type".to_string(), "application/xml".to_string()),
                (
                    "content-md5".to_string(),
                    B64.encode(md5::compute(xml.as_bytes()).0),
                ),
            ],
            body: xml.into_bytes(),
        };
        let response = self.send(req, ctx.remaining()).await.map_err(|e| {
            if e == ProviderError::R2ProviderUnavailable {
                ProviderError::R2ResultUnknown
            } else {
                e
            }
        })?;
        if !response.status().is_success() {
            // DeleteObject failures are per-key and deterministic —
            // the object was not deleted.
            return Err(ProviderError::R2ProviderUnavailable);
        }
        let bytes = tokio::time::timeout(ctx.remaining(), response.bytes())
            .await
            .map_err(|_| ProviderError::R2ResultUnknown)?
            .map_err(|_| ProviderError::R2ResultUnknown)?;
        let parsed: DeleteResult = xml_from_str(
            std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
        )
        .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        if parsed.errors.iter().any(|e| e.code != "NoSuchKey") {
            return Err(ProviderError::R2ProviderUnavailable);
        }
        Ok(())
    }

    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
    ) -> Result<R2ListPage, ProviderError> {
        if limit == 0 || limit > R2_MAX_LIST_LIMIT || delimiter.is_some_and(str::is_empty) {
            return Err(ProviderError::R2InvalidOptions);
        }
        if prefix.len() > R2_MAX_KEY_BYTES {
            return Err(ProviderError::R2KeyTooLarge);
        }
        let (tenant, bucket) = match ctx.resource() {
            crate::context::BindingResource::R2Bucket { name } => {
                (segment_encode(ctx.tenant_id()), segment_encode(name))
            }
            _ => unreachable!("R2Provider invoked with non-R2 resource"),
        };
        let physical_prefix = format!("vrt/{tenant}/{bucket}/{prefix}");
        let physical_after = after.map(|k| format!("vrt/{tenant}/{bucket}/{k}"));
        #[derive(Deserialize)]
        struct ListBucketResult {
            #[serde(rename = "Contents", default)]
            contents: Vec<Contents>,
            #[serde(rename = "IsTruncated", default)]
            truncated: bool,
            #[serde(rename = "NextContinuationToken")]
            next_token: Option<String>,
        }
        #[derive(Deserialize)]
        struct Contents {
            #[serde(rename = "Key")]
            key: String,
        }
        let mut selected: Vec<(R2ListEntry, String)> = Vec::with_capacity(limit.into());
        let mut fetch_after = physical_after.clone();
        let mut fetches = 0usize;
        let mut scanned = 0usize;
        let mut truncated = false;
        let mut complete = false;
        'outer: while fetches < LIST_FETCH_BOUND && scanned < LIST_KEY_BOUND {
            fetches += 1;
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), physical_prefix.clone()),
                ("max-keys".to_string(), "1000".to_string()),
            ];
            if let Some(after_key) = &fetch_after {
                query.push(("start-after".to_string(), after_key.clone()));
            }
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path: format!("/{}", self.config.bucket),
                query,
                headers: Vec::new(),
                body: Vec::new(),
            };
            let response = self.send(req, ctx.remaining()).await?;
            if !response.status().is_success() {
                return Err(ProviderError::R2ProviderUnavailable);
            }
            let bytes = tokio::time::timeout(ctx.remaining(), response.bytes())
                .await
                .map_err(|_| ProviderError::R2ProviderUnavailable)?
                .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            let page: ListBucketResult = xml_from_str(
                std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
            )
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            scanned += page.contents.len();
            for object in &page.contents {
                let Some(logical) = object.key.strip_prefix(&format!("vrt/{tenant}/{bucket}/"))
                else {
                    continue;
                };
                if !logical.starts_with(prefix) {
                    complete = true;
                    break 'outer;
                }
                let entry = delimiter
                    .and_then(|sep| {
                        logical[prefix.len()..].find(sep).map(|pos| {
                            R2ListEntry::DelimitedPrefix(
                                logical[..prefix.len() + pos + sep.len()].to_owned(),
                            )
                        })
                    })
                    .unwrap_or_else(|| {
                        R2ListEntry::Object(R2ObjectMetadata {
                            key: logical.to_string(),
                            version: String::new(),
                            size: 0,
                            etag: String::new(),
                            http_etag: String::new(),
                            uploaded: 0,
                            http_metadata: None,
                            custom_metadata: None,
                            range: None,
                            checksums: Default::default(),
                            storage_class: "Standard".into(),
                            ssec_key_md5: None,
                        })
                    });
                if let Some((last_entry, last_raw)) = selected.last_mut() {
                    if crate::r2::wire::list_entry_key(last_entry)
                        == crate::r2::wire::list_entry_key(&entry)
                    {
                        *last_raw = logical.to_string();
                        continue;
                    }
                }
                if selected.len() == usize::from(limit) {
                    truncated = true;
                    break 'outer;
                }
                selected.push((entry, logical.to_string()));
            }
            fetch_after = page
                .next_token
                .or_else(|| page.contents.last().map(|c| c.key.clone()));
            if !page.truncated || fetch_after.is_none() {
                complete = true;
                break;
            }
        }
        let truncated = truncated || !complete;
        let next_after = truncated
            .then(|| selected.last().map(|(_, raw)| raw.clone()))
            .flatten();
        // Resolve object metadata via HEAD fanout (bounded by limit).
        let mut objects = Vec::new();
        let mut delimited_prefixes = Vec::new();
        for (entry, _) in &selected {
            match entry {
                R2ListEntry::DelimitedPrefix(p) => delimited_prefixes.push(p.clone()),
                R2ListEntry::Object(meta) => {
                    let physical = self.checked_physical_key(ctx, &meta.key)?;
                    let headers = self
                        .s3_head(&physical, None, ctx.remaining())
                        .await?
                        .ok_or(ProviderError::R2ProviderUnavailable)?;
                    objects.push(self.decode_head(&meta.key, &headers, None)?);
                }
            }
        }
        Ok(R2ListPage {
            objects,
            delimited_prefixes,
            next_after,
        })
    }

    async fn create_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        options: &R2MultipartCreateOptions,
    ) -> Result<MultipartCreateResult, ProviderError> {
        let custom_b64 = if options.custom_metadata.is_empty() {
            None
        } else {
            Some(B64.encode(canonical_custom_metadata(&options.custom_metadata)?))
        };
        let physical = self.checked_physical_key(ctx, key)?;
        let version = uuid::Uuid::now_v7().hyphenated().to_string();
        let http_fields: Vec<(&str, &str)> = [
            ("ct", options.http_metadata.content_type.as_deref()),
            ("cl", options.http_metadata.content_language.as_deref()),
            ("cd", options.http_metadata.content_disposition.as_deref()),
            ("ce", options.http_metadata.content_encoding.as_deref()),
            ("cc", options.http_metadata.cache_control.as_deref()),
        ]
        .into_iter()
        .filter_map(|(suffix, value)| value.map(|v| (suffix, v)))
        .collect();
        // Object checksums are unknowable at create — S3 stores object
        // metadata now, so multipart objects carry no vrt-md5/sha*;
        // `decode_head` relaxes the checksum requirement for composite
        // etags (`<hex>-<N>`) which S3 itself computes at complete.
        let mut headers = vec![
            ("x-amz-meta-vrt-schema".to_string(), "1".to_string()),
            ("x-amz-meta-vrt-version".to_string(), version),
            ("x-amz-meta-vrt-uploaded".to_string(), now_ms().to_string()),
            (
                "x-amz-meta-vrt-class".to_string(),
                options.storage_class.as_str().to_string(),
            ),
        ];
        if let Some(custom) = &custom_b64 {
            headers.push(("x-amz-meta-vrt-custom".to_string(), custom.clone()));
        }
        for (suffix, value) in &http_fields {
            headers.push((format!("x-amz-meta-vrt-{suffix}"), value.to_string()));
        }
        if let Some(expiry) = options.http_metadata.cache_expiry {
            headers.push(("x-amz-meta-vrt-cxp".to_string(), expiry.to_string()));
        }
        if let Some(ssec) = &options.ssec_key {
            // Persist only the key hash; the key is remembered in memory
            // for complete() (the Workers API has no key there).
            headers.push(("x-amz-meta-vrt-ssec-md5".to_string(), ssec.md5_hex()));
            headers.extend(ssec_headers(Some(ssec)));
        }
        #[derive(Deserialize)]
        struct InitiateResult {
            #[serde(rename = "UploadId")]
            upload_id: String,
        }
        let req = SignedRequest {
            method: reqwest::Method::POST,
            path: format!(
                "/{}/{}",
                self.config.bucket,
                sigv4::uri_escape(&physical, false)
            ),
            query: vec![("uploads".to_string(), String::new())],
            headers,
            body: Vec::new(),
        };
        let response = self.send(req, ctx.remaining()).await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            if let Some(err) = classify_ssec_failure(status, options.ssec_key.as_ref()) {
                return Err(err);
            }
            return Err(self.s3_error(response, ctx.remaining()).await);
        }
        let bytes = tokio::time::timeout(ctx.remaining(), response.bytes())
            .await
            .map_err(|_| ProviderError::R2ProviderUnavailable)?
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        let parsed: InitiateResult = xml_from_str(
            std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
        )
        .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        if parsed.upload_id.is_empty() {
            return Err(ProviderError::R2ProviderUnavailable);
        }
        if let Some(ssec) = &options.ssec_key {
            self.multipart_ssec
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(parsed.upload_id.clone(), ssec.clone());
        }
        Ok(MultipartCreateResult {
            upload_id: parsed.upload_id,
            expires_at_ms: now_ms() + self.config.multipart_ttl_ms,
        })
    }

    async fn upload_part(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: &[u8],
        ssec_key: Option<&R2SsecKey>,
    ) -> Result<R2UploadedPart, ProviderError> {
        super::wire::validate_part_number(part_number as i64)?;
        if body.len() as u64 > self.config.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        let physical = self.checked_physical_key(ctx, key)?;
        // (uploadId, partNumber) is idempotent on S3 — one bounded
        // transport retry is safe.
        for attempt in 0..2u8 {
            let req = SignedRequest {
                method: reqwest::Method::PUT,
                path: format!(
                    "/{}/{}",
                    self.config.bucket,
                    sigv4::uri_escape(&physical, false)
                ),
                query: vec![
                    ("partNumber".to_string(), part_number.to_string()),
                    ("uploadId".to_string(), upload_id.to_string()),
                ],
                headers: ssec_headers(ssec_key),
                body: body.to_vec(),
            };
            let response = match self.send(req, ctx.remaining()).await {
                Ok(r) => r,
                Err(e) if attempt == 0 && e == ProviderError::R2ProviderUnavailable => {
                    continue;
                }
                Err(e) => return Err(e),
            };
            match response.status().as_u16() {
                200 => {
                    let etag = response
                        .headers()
                        .get(reqwest::header::ETAG)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| v.trim_matches('"').to_string())
                        .ok_or(ProviderError::R2ProviderUnavailable)?;
                    return Ok(R2UploadedPart { part_number, etag });
                }
                status => {
                    if let Some(err) = classify_ssec_failure(status, ssec_key) {
                        return Err(err);
                    }
                    let err = self.s3_error(response, ctx.remaining()).await;
                    if err == ProviderError::R2ProviderUnavailable && attempt == 0 {
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(ProviderError::R2ProviderUnavailable)
    }

    async fn complete_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
        parts: &[R2UploadedPart],
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        validate_complete_parts(parts)?;
        // Etag hex-format check + expected composite — derivable from
        // the request itself, so an ambiguous complete can be confirmed
        // by comparing HEAD's etag to this value.
        let expected_etag = multipart_etag(
            &parts
                .iter()
                .map(|p| p.etag.trim_matches('"').to_string())
                .collect::<Vec<_>>(),
        )?;
        let physical = self.checked_physical_key(ctx, key)?;
        // The Workers API supplies no key on complete(); reuse the key
        // remembered at create for this upload session.
        let ssec = self
            .multipart_ssec
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(upload_id)
            .cloned();
        let ssec = ssec.as_ref();
        // Upstream parity: every listed part must EXIST and its etag
        // must match the etag S3 recorded at UploadPart (S3 itself does
        // not check supplied etags — we verify via ListParts so the
        // s3 backend keeps the same typed semantics as memory/fs).
        let stored = match self
            .s3_list_parts(&physical, upload_id, ctx.remaining())
            .await
        {
            Ok(stored) => stored,
            // W5-r4 retry idempotency: a consumed upload (the previous
            // attempt committed after its response faulted) reports
            // NoSuchUpload — HEAD may prove this request's exact
            // content is already the committed object.
            Err(ProviderError::R2MultipartInvalid) => {
                let out = self
                    .confirm_committed_retry(ctx, &physical, key, &expected_etag, ssec)
                    .await;
                self.forget_upload_key(upload_id);
                return out;
            }
            Err(e) => return Err(e),
        };
        let mut sizes = Vec::with_capacity(parts.len());
        for listed in parts {
            let Some((stored_etag, size)) = stored.get(&listed.part_number) else {
                return Err(ProviderError::R2MultipartInvalid);
            };
            if !etag_matches(&listed.etag, stored_etag) {
                return Err(ProviderError::R2MultipartInvalid);
            }
            sizes.push(*size);
        }
        check_min_part_sizes(&sizes)?;
        if sizes.iter().sum::<u64>() > self.config.max_object_bytes {
            return Err(ProviderError::R2ObjectTooLarge);
        }
        #[derive(serde::Serialize)]
        #[serde(rename = "CompleteMultipartUpload")]
        struct CompleteBody<'a> {
            #[serde(rename = "Part")]
            parts: Vec<CompletePart<'a>>,
        }
        #[derive(serde::Serialize)]
        struct CompletePart<'a> {
            #[serde(rename = "PartNumber")]
            part_number: i32,
            #[serde(rename = "ETag")]
            etag: &'a str,
        }
        let xml = quick_xml::se::to_string(&CompleteBody {
            parts: parts
                .iter()
                .map(|p| CompletePart {
                    part_number: p.part_number,
                    etag: &p.etag,
                })
                .collect(),
        })
        .map_err(|_| ProviderError::Internal)?;
        let req = SignedRequest {
            method: reqwest::Method::POST,
            path: format!(
                "/{}/{}",
                self.config.bucket,
                sigv4::uri_escape(&physical, false)
            ),
            query: vec![("uploadId".to_string(), upload_id.to_string())],
            headers: {
                let mut headers = vec![("content-type".to_string(), "application/xml".to_string())];
                headers.extend(ssec_headers(ssec));
                headers
            },
            body: xml.into_bytes(),
        };
        // Reserve the tail of the request budget for the ambiguous
        // case: if Complete itself burns the whole deadline, the
        // confirmation HEAD would have no room to run and the caller
        // would see UNKNOWN for what HEAD could have classified.
        let send_budget = ctx
            .remaining()
            .saturating_sub(CONFIRM_HEAD_RESERVE)
            .max(Duration::from_millis(50));
        let response = match self.send(req, send_budget).await {
            Ok(r) => r,
            Err(e) => {
                return match e {
                    ProviderError::R2ProviderUnavailable => {
                        self.confirm_ambiguous_complete(ctx, &physical, key, &expected_etag, ssec)
                            .await
                    }
                    _ => Err(e),
                };
            }
        };
        let status = response.status().as_u16();
        let bytes = tokio::time::timeout(ctx.remaining(), response.bytes())
            .await
            .map_err(|_| ProviderError::R2ResultUnknown)?
            .map_err(|_| ProviderError::R2ResultUnknown)?;
        let text = std::str::from_utf8(&bytes).unwrap_or("");
        // Commit semantics on Complete (W5-r3/P0-1): AWS documents that
        // InternalError — AND its 200-embedded variant — may still have
        // committed the object. So on this path only definite client
        // errors stay typed `R2_MULTIPART_INVALID`; every service-side
        // or ambiguous outcome (5xx, embedded service/unknown/empty
        // codes, unparseable body, empty ETag) routes through the
        // confirmation HEAD before we answer anything.
        if !(200..300).contains(&status) || text.contains("<Error") {
            if let Some(err) = classify_ssec_failure(status, ssec) {
                return Err(err);
            }
            let code = Self::xml_error_code(text);
            return if code == "NoSuchUpload" {
                // W5-r4: on a retry the consumed upload is the sign of a
                // prior successful commit — verify via HEAD against the
                // request-derived composite etag before answering.
                let out = self
                    .confirm_committed_retry(ctx, &physical, key, &expected_etag, ssec)
                    .await;
                self.forget_upload_key(upload_id);
                out
            } else if matches!(
                Self::classify_s3_error_code(&code),
                ProviderError::R2MultipartInvalid
            ) {
                Err(ProviderError::R2MultipartInvalid)
            } else {
                self.confirm_ambiguous_complete(ctx, &physical, key, &expected_etag, ssec)
                    .await
            };
        }
        #[derive(Deserialize)]
        struct CompleteResult {
            #[serde(rename = "ETag")]
            etag: String,
        }
        // 200 with garbage XML or an empty ETag is equally ambiguous —
        // the complete may have committed before the body got mangled.
        let result_etag = xml_from_str::<CompleteResult>(text)
            .ok()
            .map(|p| p.etag.trim_matches('"').to_string())
            .filter(|e| !e.is_empty());
        let Some(result_etag) = result_etag else {
            return self
                .confirm_ambiguous_complete(ctx, &physical, key, &expected_etag, ssec)
                .await;
        };
        // Verify commit: HEAD must show the object with the etag the
        // complete response reported. Anything else is unconfirmed.
        match self.s3_head(&physical, ssec, ctx.remaining()).await? {
            Some(headers) => {
                let head_etag = headers
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim_matches('"').to_string())
                    .unwrap_or_default();
                if !etag_matches(&head_etag, &result_etag) {
                    return Err(ProviderError::R2ResultUnknown);
                }
                let committed = self.decode_head(key, &headers, None)?.completed_multipart();
                self.forget_upload_key(upload_id);
                Ok(Some(committed))
            }
            None => Err(ProviderError::R2ResultUnknown),
        }
    }

    async fn abort_multipart(
        &self,
        ctx: &BindingContext,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ProviderError> {
        let physical = self.checked_physical_key(ctx, key)?;
        let req = SignedRequest {
            method: reqwest::Method::DELETE,
            path: format!(
                "/{}/{}",
                self.config.bucket,
                sigv4::uri_escape(&physical, false)
            ),
            query: vec![("uploadId".to_string(), upload_id.to_string())],
            headers: Vec::new(),
            body: Vec::new(),
        };
        let response = self.send(req, ctx.remaining()).await?;
        match response.status().as_u16() {
            204 => {
                self.forget_upload_key(upload_id);
                Ok(())
            }
            404 => {
                self.forget_upload_key(upload_id);
                Err(ProviderError::R2MultipartInvalid)
            }
            _ => Err(self.s3_error(response, ctx.remaining()).await),
        }
    }
}

impl S3R2Provider {
    /// RV-R70: bounded purge of EVERY object + unfinished multipart
    /// upload under `vrt/{enc(t)}/{enc(bucket)}/`. The trailing `/` in
    /// the physical prefix is the structural boundary — bucket `a`
    /// (`vrt/t/a/`) can never touch `ab` (`vrt/t/ab/`).
    ///
    /// Passes are bounded by `max_objects`: each loop turn lists ≤1000
    /// keys (S3 `list-type=2` page cap) then issues one `DeleteObjects`
    /// batch (≤1000/request) — plus one `ListMultipartUploads` +
    /// per-upload `AbortMultipartUpload` pass. `complete` is true ONLY
    /// when a list page reported `IsTruncated=false` and every key it
    /// returned was deleted — never on a budget cut.
    pub async fn purge_prefix(
        &self,
        tenant_id: &str,
        bucket_name: &str,
        max_objects: usize,
        deadline: Duration,
    ) -> Result<crate::purge::PurgeOutcome, ProviderError> {
        if !crate::purge::valid_r2_purge_id(bucket_name)
            || !crate::purge::valid_purge_tenant(tenant_id)
            || max_objects == 0
        {
            return Err(ProviderError::R2InvalidOptions);
        }
        let t = segment_encode(tenant_id);
        let b = segment_encode(bucket_name);
        let physical_prefix = format!("vrt/{t}/{b}/");
        let mut deleted = 0u64;

        // Abort unfinished multipart uploads under the prefix — they
        // hold real storage bytes and are keyed under the same physical
        // prefix (ListMultipartUploads prefix filter).
        self.purge_multipart_uploads(&physical_prefix, max_objects, deadline)
            .await?;

        #[derive(Deserialize)]
        struct ListBucketResult {
            #[serde(rename = "Contents", default)]
            contents: Vec<Contents>,
            #[serde(rename = "IsTruncated", default)]
            truncated: bool,
            #[serde(rename = "NextContinuationToken")]
            next_token: Option<String>,
        }
        #[derive(Deserialize)]
        struct Contents {
            #[serde(rename = "Key")]
            key: String,
        }
        let mut start_after: Option<String> = None;
        loop {
            if deleted as usize >= max_objects {
                return Ok(crate::purge::PurgeOutcome {
                    deleted,
                    complete: false,
                });
            }
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), physical_prefix.clone()),
                ("max-keys".to_string(), "1000".to_string()),
            ];
            if let Some(after) = &start_after {
                query.push(("start-after".to_string(), after.clone()));
            }
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path: format!("/{}", self.config.bucket),
                query,
                headers: Vec::new(),
                body: Vec::new(),
            };
            let response = self.send(req, deadline).await?;
            if !response.status().is_success() {
                return Err(ProviderError::R2ProviderUnavailable);
            }
            let bytes = tokio::time::timeout(deadline, response.bytes())
                .await
                .map_err(|_| ProviderError::R2ProviderUnavailable)?
                .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            let page: ListBucketResult = xml_from_str(
                std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
            )
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            let batch: Vec<String> = page
                .contents
                .iter()
                .map(|c| c.key.clone())
                .take(max_objects - deleted as usize)
                .collect();
            if !batch.is_empty() {
                let n = batch.len();
                self.purge_delete_objects(&batch, deadline).await?;
                deleted += n as u64;
            }
            // Done when the enumeration was complete (not truncated)
            // AND the page carried nothing beyond what we deleted.
            if !page.truncated && batch.len() >= page.contents.len() {
                return Ok(crate::purge::PurgeOutcome {
                    deleted,
                    complete: true,
                });
            }
            start_after = page
                .next_token
                .or_else(|| page.contents.last().map(|c| c.key.clone()))
                .or(start_after);
        }
    }

    /// One `DeleteObjects` batch (≤1000 keys/request). A racing
    /// `NoSuchKey` is tolerated; any other per-key error fails closed.
    async fn purge_delete_objects(
        &self,
        physical: &[String],
        deadline: Duration,
    ) -> Result<(), ProviderError> {
        #[derive(serde::Serialize)]
        #[serde(rename = "Delete")]
        struct DeleteBody<'a> {
            #[serde(rename = "Object")]
            objects: Vec<DeleteObject<'a>>,
            #[serde(rename = "Quiet")]
            quiet: bool,
        }
        #[derive(serde::Serialize)]
        struct DeleteObject<'a> {
            #[serde(rename = "Key")]
            key: &'a str,
        }
        #[derive(Deserialize)]
        struct DeleteResult {
            #[serde(rename = "Error", default)]
            errors: Vec<DeleteError>,
        }
        #[derive(Deserialize)]
        struct DeleteError {
            #[serde(rename = "Code")]
            code: String,
        }
        let body = DeleteBody {
            objects: physical
                .iter()
                .map(|k| DeleteObject { key: k.as_str() })
                .collect(),
            quiet: true,
        };
        let xml = quick_xml::se::to_string(&body).map_err(|_| ProviderError::Internal)?;
        let req = SignedRequest {
            method: reqwest::Method::POST,
            path: format!("/{}", self.config.bucket),
            query: vec![("delete".to_string(), String::new())],
            headers: vec![
                ("content-type".to_string(), "application/xml".to_string()),
                (
                    "content-md5".to_string(),
                    B64.encode(md5::compute(xml.as_bytes()).0),
                ),
            ],
            body: xml.into_bytes(),
        };
        let response = self.send(req, deadline).await.map_err(|e| {
            if e == ProviderError::R2ProviderUnavailable {
                ProviderError::R2ResultUnknown
            } else {
                e
            }
        })?;
        if !response.status().is_success() {
            return Err(ProviderError::R2ProviderUnavailable);
        }
        let bytes = tokio::time::timeout(deadline, response.bytes())
            .await
            .map_err(|_| ProviderError::R2ResultUnknown)?
            .map_err(|_| ProviderError::R2ResultUnknown)?;
        let parsed: DeleteResult = xml_from_str(
            std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
        )
        .map_err(|_| ProviderError::R2ProviderUnavailable)?;
        if parsed.errors.iter().any(|e| e.code != "NoSuchKey") {
            return Err(ProviderError::R2ProviderUnavailable);
        }
        Ok(())
    }

    /// Abort every unfinished multipart upload under the physical
    /// prefix — bounded by `cap`; a leftover upload means incomplete.
    async fn purge_multipart_uploads(
        &self,
        physical_prefix: &str,
        cap: usize,
        deadline: Duration,
    ) -> Result<(), ProviderError> {
        #[derive(Deserialize)]
        struct ListUploadsResult {
            #[serde(rename = "Upload", default)]
            uploads: Vec<Upload>,
            #[serde(rename = "IsTruncated", default)]
            truncated: bool,
            #[serde(rename = "NextKeyMarker")]
            next_key_marker: Option<String>,
            #[serde(rename = "NextUploadIdMarker")]
            next_upload_id_marker: Option<String>,
        }
        #[derive(Deserialize)]
        struct Upload {
            #[serde(rename = "Key")]
            key: String,
            #[serde(rename = "UploadId")]
            upload_id: String,
        }
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        let mut aborted = 0usize;
        loop {
            let mut query = vec![
                ("uploads".to_string(), String::new()),
                ("prefix".to_string(), physical_prefix.to_string()),
                ("max-uploads".to_string(), "100".to_string()),
            ];
            if let Some(k) = &key_marker {
                query.push(("key-marker".to_string(), k.clone()));
            }
            if let Some(u) = &upload_id_marker {
                query.push(("upload-id-marker".to_string(), u.clone()));
            }
            let req = SignedRequest {
                method: reqwest::Method::GET,
                path: format!("/{}", self.config.bucket),
                query,
                headers: Vec::new(),
                body: Vec::new(),
            };
            let response = self.send(req, deadline).await?;
            if !response.status().is_success() {
                return Err(ProviderError::R2ProviderUnavailable);
            }
            let bytes = tokio::time::timeout(deadline, response.bytes())
                .await
                .map_err(|_| ProviderError::R2ProviderUnavailable)?
                .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            let page: ListUploadsResult = xml_from_str(
                std::str::from_utf8(&bytes).map_err(|_| ProviderError::R2ProviderUnavailable)?,
            )
            .map_err(|_| ProviderError::R2ProviderUnavailable)?;
            for upload in &page.uploads {
                if aborted >= cap {
                    return Err(ProviderError::R2ProviderUnavailable);
                }
                let req = SignedRequest {
                    method: reqwest::Method::DELETE,
                    path: format!(
                        "/{}/{}",
                        self.config.bucket,
                        sigv4::uri_escape(&upload.key, false)
                    ),
                    query: vec![("uploadId".to_string(), upload.upload_id.clone())],
                    headers: Vec::new(),
                    body: Vec::new(),
                };
                let response = self.send(req, deadline).await?;
                if !(response.status().is_success() || response.status().as_u16() == 404) {
                    return Err(ProviderError::R2ProviderUnavailable);
                }
                self.forget_upload_key(&upload.upload_id);
                aborted += 1;
            }
            if !page.truncated {
                return Ok(());
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
            if key_marker.is_none() && upload_id_marker.is_none() {
                // Truncated but no resume markers — backend contract
                // violation; fail closed, never silently incomplete.
                return Err(ProviderError::R2ProviderUnavailable);
            }
        }
    }
}

#[cfg(test)]
mod ssec_tests {
    use super::*;

    #[test]
    fn customer_key_headers_use_s3_base64_form() {
        let key = R2SsecKey::parse(&"ab".repeat(32)).unwrap();
        let headers = ssec_headers(Some(&key));
        assert_eq!(
            headers,
            vec![
                (
                    "x-amz-server-side-encryption-customer-algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key".to_string(),
                    B64.encode([0xabu8; 32]),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-MD5".to_string(),
                    B64.encode(md5::compute([0xabu8; 32]).0),
                ),
            ]
        );
        // Tenant-visible form is hex, not base64 (Workers API docs).
        assert_eq!(key.md5_hex(), hex::encode(md5::compute([0xabu8; 32]).0));
        assert!(ssec_headers(None).is_empty());
    }

    #[test]
    fn ssec_failure_classification_requires_a_supplied_key() {
        let key = R2SsecKey::parse(&"cd".repeat(32)).unwrap();
        assert_eq!(
            classify_ssec_failure(400, Some(&key)),
            Some(ProviderError::R2SsecInvalid)
        );
        assert_eq!(
            classify_ssec_failure(403, Some(&key)),
            Some(ProviderError::R2SsecInvalid)
        );
        assert_eq!(classify_ssec_failure(500, Some(&key)), None);
        assert_eq!(classify_ssec_failure(400, None), None);
    }
}
