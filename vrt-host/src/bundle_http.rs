//! `HttpBundleSource` — control-plane bundle pull (T15/B2).
//!
//! The host fetches `GET {base}/vrt/bundles/{digest}` with the scoped
//! cell token (`Authorization: Bearer`, loaded from a `0600` file,
//! never logged). The response is untrusted until verified:
//!
//! - `Content-Length` is pre-checked against `max_bytes` and the body
//!   is accumulated chunked with the same cap — an oversized object is
//!   aborted before it can exhaust memory.
//! - Content verification is deliberately NOT done here: the address
//!   is a bundle-manifest digest (`sha256(canonical manifest)`), not
//!   `sha256(body)` — only `VersionBundle::parse_stored` can check it.
//!   `BundleCache::write_verified` re-derives it on write and deletes
//!   mismatches, so a forged body can never be cached or launched.
//! - Retries are bounded: exponential backoff, transport errors and
//!   5xx/429 only — 4xx answers are authoritative and never retried.
//!   `404` maps to `Missing`; other 4xx to `Unauthorized`.
//!
//! `BundleSource::fetch` is synchronous; the reqwest call is async. On
//! a multi-thread tokio runtime the fetch uses `block_in_place`; in
//! sync contexts (tests, tools) it runs on a scoped thread with its
//! own current-thread runtime — either way the contract stays sync.

use crate::artifact::{valid_digest, ArtifactError, BundleSource};
use futures_util::StreamExt;
use std::time::{Duration, Instant};

/// Configuration for [`HttpBundleSource`].
#[derive(Clone)]
pub struct HttpBundleConfig {
    /// Route base, e.g. `https://cp.internal/api/internal/clusters/<id>`
    /// — https, or loopback http only (validated at config load).
    pub base_url: String,
    /// Scoped cell token (kept out of logs — passed only as a header).
    pub token: String,
    /// Per-attempt request timeout.
    pub request_timeout: Duration,
    /// Hard deadline for the whole retry loop.
    pub deadline: Duration,
    /// Total attempts including the first.
    pub max_attempts: u32,
    /// Hard cap on the object body (same bound as the upload cap).
    pub max_bytes: usize,
}

impl std::fmt::Debug for HttpBundleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Token must never reach logs — redact it unconditionally.
        f.debug_struct("HttpBundleConfig")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("request_timeout", &self.request_timeout)
            .field("deadline", &self.deadline)
            .field("max_attempts", &self.max_attempts)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

/// Bundle pull over HTTP(S) — see module docs for the trust model.
pub struct HttpBundleSource {
    client: reqwest::Client,
    config: HttpBundleConfig,
}

impl HttpBundleSource {
    /// New source; builds a redirect-refusing client.
    pub fn new(config: HttpBundleConfig) -> Result<Self, ArtifactError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ArtifactError::Unavailable(format!("client: {e}")))?;
        Ok(Self { client, config })
    }

    async fn fetch_once(&self, digest: &str) -> Result<Vec<u8>, FetchOutcome> {
        let url = format!("{}/vrt/bundles/{digest}", self.config.base_url);
        let res = self
            .client
            .get(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.config.token),
            )
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .timeout(self.config.request_timeout)
            .send()
            .await
            .map_err(|e| {
                if e.is_status() {
                    FetchOutcome::status(e.status().unwrap_or_default())
                } else {
                    // timeouts/connect/request errors are retryable
                    FetchOutcome::Retryable
                }
            })?;
        let status = res.status();
        if !status.is_success() {
            return Err(FetchOutcome::status(status));
        }
        // Content-Length pre-check — a declared-oversized object is
        // refused without consuming a byte (T15/B2).
        if let Some(len) = res.content_length() {
            if len as usize > self.config.max_bytes {
                return Err(FetchOutcome::TooLarge);
            }
        }
        // Bounded chunked accumulation — an undeclared or lying
        // Content-Length cannot bypass the cap.
        let mut body = Vec::new();
        let mut stream = res.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| FetchOutcome::Retryable)?;
            if body.len() + chunk.len() > self.config.max_bytes {
                return Err(FetchOutcome::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn fetch_bounded(&self, digest: &str) -> Result<Vec<u8>, ArtifactError> {
        let deadline = Instant::now() + self.config.deadline;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if Instant::now() >= deadline {
                return Err(ArtifactError::Unavailable("deadline exceeded".into()));
            }
            match self.fetch_once(digest).await {
                Ok(bytes) => return Ok(bytes),
                Err(FetchOutcome::TooLarge) => return Err(ArtifactError::TooLarge),
                Err(FetchOutcome::Missing) => return Err(ArtifactError::Missing),
                Err(FetchOutcome::Unauthorized) => {
                    return Err(ArtifactError::Unavailable("unauthorized".into()));
                }
                Err(FetchOutcome::Retryable) => {
                    if attempt >= self.config.max_attempts {
                        return Err(ArtifactError::Unavailable("attempts exhausted".into()));
                    }
                    // Exponential backoff bounded by the deadline —
                    // never sleep past it.
                    let backoff = Duration::from_millis(100 << attempt.saturating_sub(1).min(4))
                        .min(deadline.saturating_duration_since(Instant::now()));
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

/// Internal attempt classification.
enum FetchOutcome {
    Retryable,
    TooLarge,
    Missing,
    Unauthorized,
}

impl FetchOutcome {
    fn status(s: reqwest::StatusCode) -> Self {
        match s.as_u16() {
            404 => Self::Missing,
            _ if s.is_server_error() || s.as_u16() == 429 => Self::Retryable,
            _ => Self::Unauthorized, // every other 4xx — authoritative, no retry
        }
    }
}

/// Run an async future to completion from sync call sites.
/// Multi-thread tokio → `block_in_place`; otherwise a scoped thread
/// with a private current-thread runtime.
fn run_blocking<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| h.block_on(fut))
        }
        Ok(_) => panic!("HttpBundleSource requires a multi-thread runtime or sync context"),
        Err(_) => {
            // No runtime — drive the future on a scoped thread with its
            // own tiny runtime (sync/test contexts).
            std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("mini runtime")
                            .block_on(fut)
                    })
                    .join()
                    .expect("fetch thread")
            })
        }
    }
}

impl BundleSource for HttpBundleSource {
    fn fetch(&self, digest: &str) -> Result<Vec<u8>, ArtifactError> {
        if !valid_digest(digest) {
            return Err(ArtifactError::BadDigest);
        }
        run_blocking(self.fetch_bounded(digest))
    }
}
