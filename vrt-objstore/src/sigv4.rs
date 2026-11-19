//! AWS SigV4 签名 + path-style 请求 —— 从 vrt-provider::r2::s3
//! **逐字抽取**为共享实现（T36）。vrt-provider 与 vrt-sql persistence
//! 的 S3 后端共用；签名算法、canonical request、header 集合与原版
//! 逐位一致，provider 行为不变。

use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

/// Secret key material wrapper — never Debug/Display'd.
pub struct SecretString(String);

impl SecretString {
    /// Wrap raw secret material.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    /// 暴露明文（仅签名用途；绝不进日志）。
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

impl Clone for SecretString {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// 待签名请求（与 provider 原 `SignedRequest` 字段一致）。
#[derive(Debug)]
pub struct SignedRequest {
    pub method: reqwest::Method,
    /// `/bucket/key` 形态绝对路径（调用方已拼好、已校验无穿越）。
    pub path: String,
    /// query 键值对（签名内部排序+编码）。
    pub query: Vec<(String, String)>,
    /// 业务 headers（小写名，签名时 trim 值）。
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// SigV4 签名错误（仅内部失败，绝不含 secret）。
#[derive(Debug, thiserror::Error)]
#[error("sigv4 signing failed")]
pub struct SignError;

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// sha256 → lowercase hex。
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// RFC3986 unreserved-only percent encoding（与 provider 原版一致）。
pub fn uri_escape(value: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else if byte == b'/' && !encode_slash {
            out.push('/');
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// 从 endpoint URL 提取 `host[:port]`（与 provider 原版一致）。
pub fn host_of(endpoint: &str) -> Result<String, SignError> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| SignError)?;
    let host = url.host_str().ok_or(SignError)?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

/// 签名器：endpoint + region + 凭据（等价 provider S3Config 的
/// 签名相关字段子集）。
pub struct Signer {
    /// Base endpoint, e.g. `https://s3.internal:9000` (no path).
    pub endpoint: String,
    /// Signing region.
    pub region: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: SecretString,
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signer")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key", &self.access_key)
            .finish_non_exhaustive()
    }
}

impl Signer {
    /// 与 provider 原 `sign()` 逐位一致（canonical request/scope/
    /// key derivation/输出 headers 相同）。
    pub fn sign(&self, req: &SignedRequest) -> Result<HeaderMap, SignError> {
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let payload_hash = sha256_hex(&req.body);

        let mut headers: BTreeMap<String, String> = BTreeMap::new();
        headers.insert("host".into(), host_of(&self.endpoint)?);
        headers.insert("x-amz-date".into(), amz_date.clone());
        headers.insert("x-amz-content-sha256".into(), payload_hash.clone());
        for (name, value) in &req.headers {
            headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
        }
        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>();
        let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_query = {
            let mut pairs = req.query.clone();
            pairs.sort();
            pairs
                .iter()
                .map(|(k, v)| format!("{}={}", uri_escape(k, true), uri_escape(v, true)))
                .collect::<Vec<_>>()
                .join("&")
        };
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            req.method.as_str(),
            req.path,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let k_date = hmac(
            format!("AWS4{}", self.secret_key.expose()).as_bytes(),
            &date,
        );
        let k_region = hmac(&k_date, &self.region);
        let k_service = hmac(&k_region, "s3");
        let k_signing = hmac(&k_service, "aws4_request");
        let signature = hex::encode(hmac(&k_signing, &string_to_sign));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        let mut map = HeaderMap::new();
        for (name, value) in &headers {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| SignError)?,
                HeaderValue::from_str(value).map_err(|_| SignError)?,
            );
        }
        map.insert(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_str(&authorization).map_err(|_| SignError)?,
        );
        Ok(map)
    }

    /// 上线路径 URL（与 provider `send()` 内构造一致；调用方负责
    /// path-divergence 校验）。
    pub fn url_of(&self, req: &SignedRequest) -> String {
        let url = format!(
            "{}{}?{}",
            self.endpoint.trim_end_matches('/'),
            req.path,
            req.query
                .iter()
                .map(|(k, v)| format!("{}={}", uri_escape(k, true), uri_escape(v, true)))
                .collect::<Vec<_>>()
                .join("&")
        );
        url.trim_end_matches('?').to_string()
    }
}
