//! AWS SigV4 signing + path-style requests -- extracted **verbatim** from vrt-provider::r2::s3
//! as a shared implementation (T36). Shared by vrt-provider and the vrt-sql persistence
//! S3 backend; the signing algorithm, canonical request and header set are bit-for-bit identical
//! to the original, so provider behavior is unchanged.

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
    /// Expose the plaintext (for signing only; never into logs).
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

/// Request to be signed (fields match the provider's original `SignedRequest`).
#[derive(Debug)]
pub struct SignedRequest {
    pub method: reqwest::Method,
    /// Absolute path of the `/bucket/key` form (already assembled by the caller, already verified free of traversal).
    pub path: String,
    /// Query key-value pairs (sorted + encoded internally during signing).
    pub query: Vec<(String, String)>,
    /// Business headers (lowercase names, values trimmed when signing).
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// SigV4 signing errors (internal failures only, never contain the secret).
#[derive(Debug, thiserror::Error)]
#[error("sigv4 signing failed")]
pub struct SignError;

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// sha256 -> lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// RFC3986 unreserved-only percent encoding (identical to the provider original).
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

/// Extract `host[:port]` from an endpoint URL (identical to the provider original).
pub fn host_of(endpoint: &str) -> Result<String, SignError> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| SignError)?;
    let host = url.host_str().ok_or(SignError)?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

/// Signer: endpoint + region + credentials (equivalent to the signing-related
/// field subset of the provider's S3Config).
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
    /// Bit-for-bit identical to the provider's original `sign()` (canonical request/scope/
    /// key derivation/output headers are the same).
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

    /// On-the-wire path URL (identical to the construction inside provider `send()`; the caller is responsible for
    /// the path-divergence check).
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
