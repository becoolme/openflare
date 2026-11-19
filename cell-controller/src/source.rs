//! `DesiredSource` — pull the latest `compute.desired` envelope for the
//! local cell from the control plane.
//!
//! The production implementation (`HttpDesiredSource`) is a bounded,
//! jittered GET against the internal cluster endpoint:
//! `GET /api/internal/clusters/<clusterId>/vrt/desired` authenticated with
//! the per-cluster ingest-scoped bearer token (the same mechanism the
//! tenant-usage / cluster-events collectors use — see
//! `server/src/modules/clusters/clusterEvents.ts`). `404` means "no
//! publication yet" and is NOT an error.
//!
//! Everything is bounded: per-attempt timeout, `RetryPlan` attempts, and a
//! hard response-size cap (`snapshot_protocol::envelope::MAX_SNAPSHOT_BYTES`
//! + envelope slack). The token is a `secrecy`-style opaque string — it is
//! never logged.

use crate::retry::RetryPlan;
use snapshot_protocol::envelope::MAX_SNAPSHOT_BYTES;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

/// Why a pull failed — typed so the orchestrator can distinguish a
/// transient outage (retry next tick, keep last-good) from a permanent
/// misconfiguration.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SourceError {
    /// Every attempt timed out.
    #[error("desired fetch timed out after bounded retries")]
    Timeout,
    /// Control plane answered with a non-success status (not 404).
    #[error("control plane returned HTTP {0}")]
    Status(u16),
    /// Transport failure (DNS, connect, reset).
    #[error("transport error")]
    Transport,
    /// Response exceeded the snapshot size bound — fail closed.
    #[error("response exceeds the snapshot size bound")]
    TooLarge,
    /// All attempts exhausted for a reason that does not classify above.
    #[error("desired fetch exhausted retries")]
    Exhausted,
}

/// Pull boundary for the latest signed `compute.desired` envelope.
#[async_trait::async_trait]
pub trait DesiredSource: Send + Sync {
    /// Latest envelope bytes for this cell, `None` when the control plane
    /// has no publication yet (404). Must never panic.
    async fn fetch(&self) -> Result<Option<Vec<u8>>, SourceError>;
}

/// HTTP pull implementation over `reqwest` (rustls). Authorization uses
/// `Bearer <token>` exactly like the other cluster ingest endpoints.
pub struct HttpDesiredSource {
    client: reqwest::Client,
    url: String,
    token: String,
    plan: RetryPlan,
}

impl HttpDesiredSource {
    /// `timeout` bounds every attempt; `plan` bounds the attempt count.
    pub fn new(
        url: impl Into<String>,
        token: impl Into<String>,
        timeout: Duration,
        plan: RetryPlan,
    ) -> Result<Self, SourceError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|_| SourceError::Transport)?;
        Ok(Self {
            client,
            url: url.into(),
            token: token.into(),
            plan,
        })
    }
}

#[async_trait::async_trait]
impl DesiredSource for HttpDesiredSource {
    async fn fetch(&self) -> Result<Option<Vec<u8>>, SourceError> {
        let mut attempts = 0usize;
        let mut last = SourceError::Exhausted;
        while self.plan.can_retry(attempts) {
            if attempts > 0 {
                tokio::time::sleep(self.plan.delay_for(attempts)).await;
            }
            attempts += 1;
            let response = self
                .client
                .get(&self.url)
                .bearer_auth(&self.token)
                .header(reqwest::header::ACCEPT, "application/json")
                .send()
                .await;
            let response = match response {
                Ok(r) => r,
                Err(e) => {
                    last = if e.is_timeout() {
                        SourceError::Timeout
                    } else {
                        SourceError::Transport
                    };
                    continue;
                }
            };
            let status = response.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                last = SourceError::Status(status.as_u16());
                continue;
            }
            let limit = MAX_SNAPSHOT_BYTES + 4096;
            // Preflight: a truthful Content-Length beyond the cap fails
            // closed without reading a byte (a lying one is still caught
            // by the streaming cap below — no bypass).
            if let Some(len) = response.content_length() {
                if len as usize > limit {
                    return Err(SourceError::TooLarge); // permanent — no retry
                }
            }
            // Stream the body and abort at the cap instead of buffering
            // the whole response first — an unbounded body is cut at
            // `limit + 1` bytes of memory.
            let mut response = response;
            let mut body = Vec::new();
            let mut oversized = false;
            let mut mid_err: Option<SourceError> = None;
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        body.extend_from_slice(&chunk);
                        if body.len() > limit {
                            oversized = true;
                            break; // drop `response` → connection aborted
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        mid_err = Some(if e.is_timeout() {
                            SourceError::Timeout
                        } else {
                            SourceError::Transport
                        });
                        break;
                    }
                }
            }
            if oversized {
                return Err(SourceError::TooLarge); // permanent — no retry
            }
            if let Some(e) = mid_err {
                last = e;
                continue;
            }
            return Ok(Some(body));
        }
        Err(last)
    }
}

/// Scripted in-memory source for tests. Each `fetch` pops the next
/// scripted result; once the script is empty it replays `fallback`.
#[derive(Debug)]
pub struct MemoryDesiredSource {
    script: Mutex<std::collections::VecDeque<Result<Option<Vec<u8>>, SourceError>>>,
    fallback: Result<Option<Vec<u8>>, SourceError>,
    calls: AtomicUsize,
}

impl MemoryDesiredSource {
    /// Source that always answers `fallback`.
    pub fn new(fallback: Result<Option<Vec<u8>>, SourceError>) -> Self {
        Self {
            script: Mutex::new(std::collections::VecDeque::new()),
            fallback,
            calls: AtomicUsize::new(0),
        }
    }

    /// Queue a one-shot scripted result (consumed FIFO).
    pub fn push(&self, next: Result<Option<Vec<u8>>, SourceError>) {
        self.script
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(next);
    }

    /// Number of `fetch` calls so far (retry-count assertions).
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl DesiredSource for MemoryDesiredSource {
    async fn fetch(&self) -> Result<Option<Vec<u8>>, SourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.script
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
            .unwrap_or_else(|| self.fallback.clone())
    }
}

/// `MemoryDesiredSource` shared handle — `CellController` stores trait
/// objects, tests keep the concrete handle for scripting.
pub fn shared_source(source: MemoryDesiredSource) -> Arc<MemoryDesiredSource> {
    Arc::new(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T12/G10: an oversized body must abort the stream at the cap —
    /// never buffer-then-reject. The test server streams ~2× the limit
    /// and asserts the fetch returns TooLarge (a pre-buffer read would
    /// hang or OOM instead of failing fast).
    #[tokio::test]
    async fn http_source_aborts_oversized_body_streaming() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await; // request
            sock.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            // Stream ~2× MAX_SNAPSHOT_BYTES in chunked frames.
            let chunk = vec![b'x'; 64 * 1024];
            for _ in 0..((MAX_SNAPSHOT_BYTES * 2) / chunk.len() + 2) {
                let head = format!("{:x}\r\n", chunk.len());
                if sock.write_all(head.as_bytes()).await.is_err()
                    || sock.write_all(&chunk).await.is_err()
                    || sock.write_all(b"\r\n").await.is_err()
                {
                    break; // client aborted — expected
                }
            }
        });
        let src = HttpDesiredSource::new(
            format!("http://127.0.0.1:{port}/desired"),
            "token".to_string(),
            std::time::Duration::from_secs(5),
            crate::retry::RetryPlan::default(),
        )
        .unwrap();
        let result = src.fetch().await;
        assert_eq!(result, Err(SourceError::TooLarge));
        server.abort();
    }

    /// G10: a truthful Content-Length over the cap is rejected BEFORE any
    /// body byte is consumed (preflight short-circuit).
    #[tokio::test]
    async fn http_source_rejects_oversized_content_length_preflight() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let len = MAX_SNAPSHOT_BYTES + 4096 + 1;
            sock.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {len}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            // Keep the socket open without sending the body — if the
            // client actually read, fetch would block/timeout instead
            // of returning immediately.
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        });
        let src = HttpDesiredSource::new(
            format!("http://127.0.0.1:{port}/desired"),
            "token".to_string(),
            std::time::Duration::from_secs(5),
            crate::retry::RetryPlan::default(),
        )
        .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), src.fetch())
            .await
            .expect("preflight reject should be immediate");
        assert_eq!(result, Err(SourceError::TooLarge));
        server.abort();
    }

    #[tokio::test]
    async fn memory_source_replays_script_then_fallback() {
        let src = shared_source(MemoryDesiredSource::new(Ok(Some(b"env".to_vec()))));
        src.push(Err(SourceError::Timeout));
        src.push(Ok(None));
        assert!(matches!(src.fetch().await, Err(SourceError::Timeout)));
        assert_eq!(src.fetch().await.unwrap(), None);
        assert_eq!(src.fetch().await.unwrap().as_deref(), Some(&b"env"[..]));
        assert_eq!(src.calls(), 3);
    }
}
