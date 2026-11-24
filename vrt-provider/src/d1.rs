//! D1 data plane (T34): facade-d1 -> provider -> vrt-sql.
//!
//! The provider's responsibilities: authenticate (signed props -> Authorizer) -> map
//! `bindingId` to `databaseId` from the snapshot (the Worker cannot choose) -> bounded rate limiting
//! -> quota pre-check -> allocate seq (globally monotonic, hence monotonic per databaseId subsequence) ->
//! send a signed request to vrt-sql. **A write with an uncertain outcome is never retried** (section 3 R4).
//! When vrt-sql is unavailable, returns the D1 error surface `{success:false,error:"D1_ERROR: ..."}`,
//! writes structured logs and metrics, and never hangs.

use crate::context::{BindingContext, BindingResource};
use crate::error::ProviderError;
use axum::body::Bytes;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Same auth headers as vrt-sql::service (shared wire format, see service.rs).
pub const SQL_AUTH_HEADER: &str = "x-vrt-sql-auth";
pub const SQL_EPOCH_HEADER: &str = "x-vrt-epoch";
pub const SQL_SEQ_HEADER: &str = "x-vrt-seq";
/// RV-R71: remaining request budget in ms (shrink-only on the vrt-sql side).
pub const SQL_DEADLINE_MS_HEADER: &str = "x-vrt-deadline-ms";
/// bookmark/session headers (passed through to the caller).
pub const BOOKMARK_HEADER: &str = "x-cf-d1-session-commit-token";

/// Pre-check limits: consistent with vrt-sql::security's limits (the engine remains authoritative).
pub const D1_MAX_SQL_BYTES: usize = 100 * 1024;
pub const D1_MAX_PARAMS: usize = 100;
/// Per-request body cap (checked before reading the body).
pub const D1_MAX_BODY_BYTES: usize = 1024 * 1024;
/// Upstream response cap (RV-R8 #8): a single statement result is
/// capped at 50 MB by vrt-sql's documented D1 limit, but a
/// multi-statement batch can legally serialize larger. This remains the
/// transport backstop: over it the provider fails closed (never
/// truncates) instead of growing the buffer without bound.
pub const D1_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

type HmacSha256 = Hmac<Sha256>;

fn unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// provider->vrt-sql signer (same wire format as vrt-sql::service::ServiceAuth:
/// `v1:<ts>:<hmac(ts|method|path?query|sha256(body)|epoch|seq)>`).
/// Deliberately does not reuse the vrt-sql crate -- keeps the service boundary; the format comments are synced on both sides.
pub struct SqlAuthSigner {
    secret: Vec<u8>,
}

impl std::fmt::Debug for SqlAuthSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlAuthSigner").finish_non_exhaustive()
    }
}

impl SqlAuthSigner {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret }
    }

    /// `epoch`/`seq` match the header values actually sent (the signature covers them).
    pub fn sign(
        &self,
        method: &str,
        path_query: &str,
        body: &[u8],
        epoch: u64,
        seq: u64,
    ) -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let msg = format!(
            "{ts}|{method}|{path_query}|{}|{epoch}|{seq}",
            hex::encode(Sha256::digest(body))
        );
        let mut h = <HmacSha256 as Mac>::new_from_slice(&self.secret).expect("hmac any len");
        h.update(msg.as_bytes());
        let mac: [u8; 32] = h.finalize().into_bytes().into();
        format!("v1:{ts}:{}", hex::encode(mac))
    }
}

/// vrt-sql downstream client.
pub struct D1Client {
    http: reqwest::Client,
    /// e.g. `http://127.0.0.1:8090` (no trailing slash).
    addr: String,
    /// Optional read-only followers (`vrt-sql follower` -> `/dbs/{id}/query` only).
    replicas: Vec<String>,
    /// Round-robin cursor across `replicas`.
    replica_cursor: AtomicU64,
    signer: SqlAuthSigner,
    timeout: Duration,
    /// Globally monotonic seq (monotonic across databaseIds => the per-database subsequence is monotonic too).
    /// Seeded from unix-nanos: after a provider restart seq keeps climbing instead of wrapping back to 1
    /// -- in this round where epoch is fixed at 1, wrapping back would let a **new write** collide with an old `_vrt_dedup`
    /// row (the engine now also checks the payload, a double safeguard). Assumption: the wall clock does not move back below a historical
    /// high-water mark (the same clock trust base as the +-30s auth window).
    seq: AtomicU64,
}

impl std::fmt::Debug for D1Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("D1Client")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl D1Client {
    pub fn new(addr: String, secret: Vec<u8>, timeout: Duration) -> Result<Self, ProviderError> {
        if addr.is_empty()
            || addr.len() > 256
            || !(addr.starts_with("http://") || addr.starts_with("https://"))
            || addr.ends_with('/')
        {
            return Err(ProviderError::Protocol);
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .build()
            .map_err(|_| ProviderError::Protocol)?;
        Ok(Self {
            http,
            addr,
            replicas: Vec::new(),
            replica_cursor: AtomicU64::new(0),
            signer: SqlAuthSigner::new(secret),
            timeout,
            seq: AtomicU64::new(unix_nanos()),
        })
    }

    /// Attach read-only followers. Addresses are validated by the provider config loader.
    pub fn with_replicas(mut self, replicas: Vec<String>) -> Self {
        self.replicas = replicas;
        self
    }

    /// Configured read-only followers (diagnostics/tests).
    pub fn replicas(&self) -> &[String] {
        &self.replicas
    }

    /// Allocate the next seq (public for metering/tests; called internally by forward).
    pub fn next_seq(&self) -> u64 {
        // CAS-max(cur+1, now+1): strictly increasing within the process, and the counter rises
        // with the wall clock (a restart seed is always higher than the seq issued in the previous run).
        let now = unix_nanos();
        let mut cur = self.seq.load(Ordering::Relaxed);
        loop {
            let next = cur.max(now) + 1;
            match self
                .seq
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return next,
                Err(c) => cur = c,
            }
        }
    }

    /// Quota pre-check: sql length and parameter count (the authoritative limits remain on the engine side).
    pub fn precheck(body: &[u8]) -> Result<(), &'static str> {
        let v: serde_json::Value = serde_json::from_slice(body).map_err(|_| "body json")?;
        let stmts: &[serde_json::Value] = match &v {
            serde_json::Value::Array(a) => a,
            serde_json::Value::Object(_) => std::slice::from_ref(&v),
            _ => return Err("body shape"),
        };
        for st in stmts {
            let sql = st.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            if sql.len() > D1_MAX_SQL_BYTES {
                return Err("sql too long");
            }
            if let Some(p) = st.get("params").and_then(|v| v.as_array()) {
                if p.len() > D1_MAX_PARAMS {
                    return Err("too many params");
                }
            }
        }
        Ok(())
    }

    /// Forward one D1 request. Returns (status, bookmark, body bytes).
    /// **Never retries**: a single send, any failure (connect/timeout/5xx) returns Err.
    pub async fn forward(
        &self,
        ctx: &BindingContext,
        op: &str,
        path_query_suffix: &str,
        body: Bytes,
        session_headers: Vec<(String, String)>,
    ) -> Result<(u16, Option<String>, Bytes), ProviderError> {
        let (database_id, epoch, writer_addr) = match ctx.resource() {
            BindingResource::D1Database {
                id,
                epoch,
                writer_addr,
            } => (id.clone(), *epoch, writer_addr.clone()),
            _ => {
                return Err(ProviderError::Forbidden(
                    crate::error::ForbiddenReason::BindingKindMismatch,
                ))
            }
        };
        // T35: whitelist the id BEFORE URL interpolation — the snapshot
        // validator enforces ^[a-z0-9]{26}$ upstream, but the provider
        // treats rendered inputs as untrusted (defense in depth against
        // path/URL injection into the data plane address).
        if database_id.len() != 26
            || !database_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(ProviderError::Forbidden(
                crate::error::ForbiddenReason::ResourceMismatch,
            ));
        }
        // RV-R71: one deadline covers send + body read. The effective
        // budget is min(client timeout, remaining request budget); an
        // already-expired budget fails closed WITHOUT sending (nothing
        // was submitted, so it is safe for the caller to retry).
        let budget = self.timeout.min(ctx.remaining());
        if budget.is_zero() {
            tracing::warn!(
                worker = ctx.worker_id(),
                binding = ctx.binding_name(),
                "d1 request budget exhausted before forward"
            );
            return Err(ProviderError::ResourceUnavailable);
        }
        let deadline = std::time::Instant::now() + budget;
        let seq = self.next_seq();
        let path_query = format!("/dbs/{database_id}/{op}{path_query_suffix}");
        let auth = self.signer.sign("POST", &path_query, &body, epoch, seq);
        let first_primary = session_headers.iter().any(|(k, v)| {
            k.eq_ignore_ascii_case(BOOKMARK_HEADER)
                && v.trim().eq_ignore_ascii_case("first-primary")
        });
        // Only reads are routed to followers. `/execute` and `/dump` must stay on the writer;
        // `/query` may still attempt a write, which the replica rejects with
        // `VRT_REPLICA_READ_ONLY` and we retry here (the replica never executes it).
        if op == "query" && !self.replicas.is_empty() && !first_primary {
            let start = self.replica_cursor.fetch_add(1, Ordering::Relaxed) as usize;
            for i in 0..self.replicas.len() {
                let addr = &self.replicas[(start + i) % self.replicas.len()];
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                // Reserve at least half the remaining budget for the writer fallback: a hung
                // replica must not consume the entire request and turn a retryable read into 503.
                let replica_budget = remaining
                    .min(budget / 2)
                    .max(Duration::from_millis(1))
                    .min(remaining);
                match self
                    .attempt(
                        addr,
                        &path_query,
                        &auth,
                        &session_headers,
                        &body,
                        epoch,
                        seq,
                        replica_budget,
                    )
                    .await
                {
                    Ok((status, bookmark, bytes)) => {
                        if let Some(reason) = replica_fallback_code(status, &bytes) {
                            tracing::warn!(
                                worker = ctx.worker_id(),
                                binding = ctx.binding_name(),
                                replica = %addr,
                                status,
                                reason,
                                "d1 replica cannot serve the read; falling back to the writer"
                            );
                            continue;
                        }
                        return Ok((status, bookmark, bytes));
                    }
                    Err(e) => {
                        tracing::warn!(
                            worker = ctx.worker_id(),
                            binding = ctx.binding_name(),
                            replica = %addr,
                            error = %e,
                            "d1 replica call failed; falling back to the writer"
                        );
                        continue;
                    }
                }
            }
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            tracing::warn!(
                worker = ctx.worker_id(),
                binding = ctx.binding_name(),
                "d1 request budget exhausted before the writer attempt"
            );
            return Err(ProviderError::ResourceUnavailable);
        }
        let writer = writer_addr.as_deref().unwrap_or(&self.addr);
        self.attempt(
            writer,
            &path_query,
            &auth,
            &session_headers,
            &body,
            epoch,
            seq,
            remaining,
        )
        .await
    }

    /// One send + capped body read against one vrt-sql address. Never retries internally.
    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        addr: &str,
        path_query: &str,
        auth: &str,
        session_headers: &[(String, String)],
        body: &Bytes,
        epoch: u64,
        seq: u64,
        budget: Duration,
    ) -> Result<(u16, Option<String>, Bytes), ProviderError> {
        let url = format!("{addr}{path_query}");
        let mut req = self
            .http
            .post(&url)
            .timeout(budget)
            .header(
                SQL_DEADLINE_MS_HEADER,
                budget.as_millis().max(1).to_string(),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(SQL_EPOCH_HEADER, epoch.to_string())
            .header(SQL_SEQ_HEADER, seq.to_string())
            .header(SQL_AUTH_HEADER, auth)
            .body(body.clone());
        for (k, v) in session_headers {
            if let (Ok(name), Ok(value)) = (
                k.parse::<reqwest::header::HeaderName>(),
                reqwest::header::HeaderValue::from_str(v),
            ) {
                req = req.header(name, value);
            }
        }
        let started = std::time::Instant::now();
        let overall = tokio::time::timeout(budget, async {
            let resp = req.send().await.map_err(|e| {
                tracing::warn!(error = %e, addr = %addr, "vrt-sql backend call failed");
                ProviderError::ResourceUnavailable
            })?;
            let status = resp.status().as_u16();
            let bookmark = resp
                .headers()
                .get(BOOKMARK_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            // RV-R8 #8: chunked capped read — a misbehaving backend can
            // never grow the buffer past D1_MAX_RESPONSE_BYTES.
            let bytes = crate::upstream_body::read_body_capped(resp, D1_MAX_RESPONSE_BYTES)
                .await
                .map_err(|e| {
                    if matches!(e, crate::upstream_body::UpstreamBodyErr::TooLarge) {
                        tracing::warn!(
                            addr = %addr,
                            cap = D1_MAX_RESPONSE_BYTES,
                            "d1 upstream response exceeded byte cap"
                        );
                    }
                    ProviderError::ResourceUnavailable
                })?;
            Ok::<_, ProviderError>((status, bookmark, bytes))
        })
        .await;
        match overall {
            Ok(r) => r,
            Err(_) => {
                tracing::warn!(
                    addr = %addr,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "d1 forward cut at request deadline"
                );
                Err(ProviderError::ResourceUnavailable)
            }
        }
    }
}

/// Replica responses that mean "ask the writer" rather than a tenant-visible SQL result.
/// The codes are defined in `vrt-sql/src/replica.rs`.
fn replica_fallback_code(status: u16, body: &[u8]) -> Option<&'static str> {
    if status == 429 || status >= 500 {
        return Some("replica_unavailable");
    }
    if status != 409 {
        return None;
    }
    let code = serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("code")?
        .as_str()?
        .to_string();
    match code.as_str() {
        "VRT_REPLICA_STALE" => Some("stale"),
        "VRT_REPLICA_PRIMARY_REQUIRED" => Some("primary_required"),
        "VRT_REPLICA_READ_ONLY" => Some("read_only"),
        "VRT_REPLICA_BOOKMARK_INVALID" => Some("bookmark_invalid"),
        "VRT_REPLICA_UNKNOWN_DB" => Some("unknown_db"),
        "VRT_REPLICA_NOT_READY" => Some("not_ready"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{BindingContext, BindingResource, Permissions};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn ctx(budget_ms: u64) -> BindingContext {
        ctx_with_writer(budget_ms, 1, None)
    }

    fn ctx_with_writer(
        budget_ms: u64,
        epoch: u64,
        writer_addr: Option<String>,
    ) -> BindingContext {
        BindingContext::grant(
            "t".into(),
            "w".into(),
            "v".into(),
            "w~DB".into(),
            "DB".into(),
            BindingResource::D1Database {
                id: "a".repeat(26),
                epoch,
                writer_addr,
            },
            Permissions {
                read: true,
                write: true,
            },
            1,
            1,
            tokio::time::Instant::now() + Duration::from_millis(budget_ms),
        )
    }

    /// RV-R71 item 3: a backend that never answers is cut at the
    /// REQUEST deadline, not the (much larger) client timeout.
    #[tokio::test]
    async fn forward_is_cut_at_request_deadline() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).await;
            tokio::time::sleep(Duration::from_secs(60)).await; // never replies
        });
        let c = D1Client::new(addr, b"s".to_vec(), Duration::from_secs(30)).unwrap();
        let t = std::time::Instant::now();
        let r = c
            .forward(&ctx(300), "query", "", Bytes::from_static(b"{}"), vec![])
            .await;
        assert!(matches!(r, Err(ProviderError::ResourceUnavailable)));
        assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    }

    /// The remaining budget is forwarded as a shrink-only header, and an
    /// exhausted budget fails closed without touching the network.
    #[tokio::test]
    async fn forward_sends_remaining_budget_and_refuses_expired() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = s.read(&mut buf).await.unwrap();
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_lowercase());
            let _ = s
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\n[]")
                .await;
        });
        let c = D1Client::new(addr.clone(), b"s".to_vec(), Duration::from_secs(30)).unwrap();
        let (st, _, body) = c
            .forward(&ctx(2_000), "query", "", Bytes::from_static(b"{}"), vec![])
            .await
            .unwrap();
        assert_eq!(st, 200);
        assert_eq!(&body[..], b"[]");
        let req = rx.await.unwrap();
        let line = req
            .lines()
            .find(|l| l.starts_with("x-vrt-deadline-ms:"))
            .expect("deadline header present");
        let ms: u64 = line.split(':').nth(1).unwrap().trim().parse().unwrap();
        assert!(ms > 0 && ms <= 2_000, "{ms}");

        // Expired budget: refuse before connecting (nothing listens now).
        let dead = D1Client::new("http://127.0.0.1:1".into(), b"s".to_vec(), Duration::from_secs(30))
            .unwrap();
        let expired = ctx(0);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let r = dead
            .forward(&expired, "query", "", Bytes::from_static(b"{}"), vec![])
            .await;
        assert!(matches!(r, Err(ProviderError::ResourceUnavailable)));
    }

    /// Sessions (D1 `withSession`): the tenant's `x-cf-d1-*` session
    /// header must reach vrt-sql, and vrt-sql's commit bookmark must come
    /// back on the response — that header is what workerd's d1-api turns
    /// into `session.getBookmark()`. Neither direction is optional: a
    /// dropped request header loses the session constraint, a dropped
    /// response header leaves `getBookmark()` null forever.
    #[tokio::test]
    async fn forward_round_trips_session_bookmark() {
        const BM: &str = "00000000-0000002a-0123456789abcdef";
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = s.read(&mut buf).await.unwrap();
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_lowercase());
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n{BOOKMARK_HEADER}: {BM}\r\nconnection: close\r\n\r\n[]"
            );
            let _ = s.write_all(reply.as_bytes()).await;
        });
        let c = D1Client::new(addr, b"s".to_vec(), Duration::from_secs(30)).unwrap();
        let (status, bookmark, body) = c
            .forward(
                &ctx(2_000),
                "query",
                "?resultsFormat=ROWS_AND_COLUMNS",
                Bytes::from_static(br#"{"sql":"select 1"}"#),
                vec![(
                    BOOKMARK_HEADER.to_string(),
                    "first-unconstrained".to_string(),
                )],
            )
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(bookmark.as_deref(), Some(BM));
        assert_eq!(&body[..], b"[]");
        let req = rx.await.unwrap();
        assert!(
            req.contains("x-cf-d1-session-commit-token: first-unconstrained"),
            "session header not forwarded to vrt-sql: {req}"
        );
    }

    struct FakeBackend {
        addr: String,
        hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    async fn spawn_fake(status: u16, body: impl Into<String>) -> FakeBackend {
        let body = body.into();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", l.local_addr().unwrap());
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    break;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let mut buf = vec![0u8; 16 * 1024];
                let _ = s.read(&mut buf).await;
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(reply.as_bytes()).await;
            }
        });
        FakeBackend { addr, hits }
    }

    fn client(primary: &str, replicas: Vec<String>) -> D1Client {
        D1Client::new(primary.to_string(), b"s".to_vec(), Duration::from_secs(5))
            .unwrap()
            .with_replicas(replicas)
    }

    #[tokio::test]
    async fn query_uses_replica_but_execute_stays_on_the_writer() {
        let primary = spawn_fake(200, "PRIMARY").await;
        let replica = spawn_fake(200, "REPLICA").await;
        let c = client(&primary.addr, vec![replica.addr.clone()]);
        let body = Bytes::from_static(b"[]");

        let (status, _, out) = c
            .forward(&ctx(2_000), "query", "", body.clone(), vec![])
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(&out[..], b"REPLICA");
        assert_eq!(replica.hits.load(Ordering::SeqCst), 1);
        assert_eq!(primary.hits.load(Ordering::SeqCst), 0);

        let (_, _, out) = c
            .forward(&ctx(2_000), "execute", "", body, vec![])
            .await
            .unwrap();
        assert_eq!(&out[..], b"PRIMARY");
        assert_eq!(replica.hits.load(Ordering::SeqCst), 1);
        assert_eq!(primary.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_or_read_only_replica_falls_back_to_the_writer() {
        for code in ["VRT_REPLICA_STALE", "VRT_REPLICA_READ_ONLY"] {
            let primary = spawn_fake(200, "PRIMARY").await;
            let replica = spawn_fake(409, format!(r#"{{"code":"{code}"}}"#)).await;
            let c = client(&primary.addr, vec![replica.addr.clone()]);
            let (status, _, out) = c
                .forward(&ctx(2_000), "query", "", Bytes::from_static(b"[]"), vec![])
                .await
                .unwrap();
            assert_eq!((status, &out[..]), (200, &b"PRIMARY"[..]), "{code}");
            assert_eq!(replica.hits.load(Ordering::SeqCst), 1, "{code}");
            assert_eq!(primary.hits.load(Ordering::SeqCst), 1, "{code}");
        }
    }

    #[tokio::test]
    async fn writer_addr_override_routes_to_the_promoted_writer() {
        let primary = spawn_fake(200, "STATIC").await;
        let promoted = spawn_fake(200, "PROMOTED").await;
        let c = client(&primary.addr, vec![]);
        let ctx = ctx_with_writer(2_000, 2, Some(promoted.addr.clone()));
        let (status, _, out) = c
            .forward(&ctx, "execute", "", Bytes::from_static(b"[]"), vec![])
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(&out[..], b"PROMOTED");
        assert_eq!(primary.hits.load(Ordering::SeqCst), 0);
        assert_eq!(promoted.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn writer_addr_override_applies_after_the_replica_falls_back() {
        let primary = spawn_fake(200, "STATIC").await;
        let promoted = spawn_fake(200, "PROMOTED").await;
        let replica = spawn_fake(409, r#"{"code":"VRT_REPLICA_STALE"}"#).await;
        let c = client(&primary.addr, vec![replica.addr.clone()]);
        let ctx = ctx_with_writer(2_000, 2, Some(promoted.addr.clone()));
        let (_, _, out) = c
            .forward(&ctx, "query", "", Bytes::from_static(b"[]"), vec![])
            .await
            .unwrap();
        assert_eq!(&out[..], b"PROMOTED");
        assert_eq!(replica.hits.load(Ordering::SeqCst), 1);
        assert_eq!(primary.hits.load(Ordering::SeqCst), 0);
        assert_eq!(promoted.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_primary_skips_the_replica() {
        let primary = spawn_fake(200, "PRIMARY").await;
        let replica = spawn_fake(200, "REPLICA").await;
        let c = client(&primary.addr, vec![replica.addr.clone()]);
        let (_, _, out) = c
            .forward(
                &ctx(2_000),
                "query",
                "",
                Bytes::from_static(b"[]"),
                vec![(BOOKMARK_HEADER.to_string(), "first-primary".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(&out[..], b"PRIMARY");
        assert_eq!(replica.hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unreachable_replica_falls_back() {
        let primary = spawn_fake(200, "PRIMARY").await;
        let c = client(&primary.addr, vec!["http://127.0.0.1:1".to_string()]);
        let (_, _, out) = c
            .forward(&ctx(2_000), "query", "", Bytes::from_static(b"[]"), vec![])
            .await
            .unwrap();
        assert_eq!(&out[..], b"PRIMARY");
    }

    #[tokio::test]
    async fn replica_sql_error_is_returned_without_retry() {
        let primary = spawn_fake(200, "PRIMARY").await;
        let replica = spawn_fake(200, r#"{"success":false,"error":"no such table"}"#).await;
        let c = client(&primary.addr, vec![replica.addr.clone()]);
        let (status, _, out) = c
            .forward(&ctx(2_000), "query", "", Bytes::from_static(b"[]"), vec![])
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert!(out.starts_with(br#"{"success":false"#));
        assert_eq!(primary.hits.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fallback_codes_are_exactly_the_routing_signals() {
        for (code, want) in [
            ("VRT_REPLICA_STALE", Some("stale")),
            ("VRT_REPLICA_READ_ONLY", Some("read_only")),
            ("VRT_REPLICA_PRIMARY_REQUIRED", Some("primary_required")),
            ("VRT_REPLICA_BOOKMARK_INVALID", Some("bookmark_invalid")),
            ("VRT_REPLICA_UNKNOWN_DB", Some("unknown_db")),
            ("VRT_REPLICA_NOT_READY", Some("not_ready")),
            ("SOMETHING_ELSE", None),
        ] {
            let body = format!(r#"{{"code":"{code}"}}"#);
            assert_eq!(replica_fallback_code(409, body.as_bytes()), want, "{code}");
        }
        assert_eq!(replica_fallback_code(200, b"[]"), None);
        assert_eq!(replica_fallback_code(503, b"{}"), Some("replica_unavailable"));
    }

    /// End-to-end: provider -> replica for eligible reads, writer for writes and stale bookmarks.
    #[tokio::test]
    async fn replica_routing_round_trip_over_tcp() {
        use crate::context::Permissions;
        use vrt_objstore::{MemoryStore, ObjectStore};
        use vrt_sql::bookmark::BookmarkSigner;
        use vrt_sql::clock::{Clock, TestClock};
        use vrt_sql::engine::{
            EngineRegistry, PersisterPair, ResultsFormat, RusqliteEngine, SqlStmt, TxnCtx,
        };
        use vrt_sql::follower::Follower;
        use vrt_sql::paths::queue_root_for;
        use vrt_sql::paths::ResourceKind::D1;
        use vrt_sql::persist::{PersistConfig, Persister};
        use vrt_sql::replica::{self, ReplicaState};
        use vrt_sql::service::ServiceAuth;

        const DB: &str = "abcdefghijklmnopqrstuvwxyz";
        const SECRET: &[u8] = b"replica-e2e-secret";

        fn write(e: &RusqliteEngine, sql: &str) {
            e.query_batch_sync(
                &[SqlStmt {
                    sql: sql.into(),
                    params: None,
                }],
                &TxnCtx {
                    epoch: 1,
                    deadline: std::time::Instant::now() + Duration::from_secs(30),
                    dedup: None,
                },
                ResultsFormat::ArrayOfObjects,
            )
            .unwrap();
        }

        fn served_by(body: &[u8]) -> String {
            let v: serde_json::Value = serde_json::from_slice(body).unwrap();
            v[0]["meta"]["served_by"].as_str().unwrap_or("").to_string()
        }

        async fn serve(app: axum::Router) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            addr
        }

        let root = std::env::temp_dir().join(format!(
            "vrt-d1-replica-e2e-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(MemoryStore::new());
        let clock = std::sync::Arc::new(TestClock::new(1_000_000));
        let writer_root = root.join("writer");
        let d1 = std::sync::Arc::new(Persister::new(
            store.clone(),
            PersistConfig {
                lease_ms: 10_000,
                clock_skew_margin_ms: 500,
                holder: "writer".into(),
                checkpoint_every_commits: 0,
                retention_ms: 60_000,
                snapshot_max_bytes: 1 << 30,
            },
            clock.clone(),
            writer_root.clone(),
            D1,
        ));
        let queue = std::sync::Arc::new(Persister::new(
            d1.store().clone(),
            d1.config().clone(),
            d1.clock().clone(),
            queue_root_for(&writer_root),
            vrt_sql::paths::ResourceKind::Queue,
        ));
        let k2 = std::sync::Arc::new(Persister::new(
            d1.store().clone(),
            d1.config().clone(),
            d1.clock().clone(),
            vrt_sql::paths::k2_root_for(&writer_root),
            vrt_sql::paths::ResourceKind::K2,
        ));
        let reg = EngineRegistry::new(writer_root.clone())
            .with_persisters(PersisterPair { d1, queue, k2 });
        let engine = reg.get(D1, DB).unwrap();
        write(&engine, "CREATE TABLE t(v INTEGER)");
        write(&engine, "INSERT INTO t VALUES (1)");

        let auth = ServiceAuth::new(SECRET.to_vec());
        let bookmark_key = [7u8; 32];
        let mut writer_state =
            vrt_sql::wire::AppState::new(writer_root, bookmark_key).with_auth(auth.clone());
        writer_state.registry = std::sync::Arc::new(reg);
        let writer_addr = serve(vrt_sql::wire::app(writer_state)).await;

        let follower = std::sync::Arc::new(
            Follower::new(
                store.clone(),
                D1,
                DB,
                root.join("replica"),
                clock.clone() as std::sync::Arc<dyn Clock>,
            )
            .unwrap(),
        );
        follower.sync_once().unwrap();
        let replica_addr = serve(replica::app(
            ReplicaState::new(follower.clone())
                .with_auth(auth)
                .with_signer(BookmarkSigner::new(bookmark_key)),
        ))
        .await;

        let client = D1Client::new(writer_addr, SECRET.to_vec(), Duration::from_secs(5))
            .unwrap()
            .with_replicas(vec![replica_addr]);

        let request = || {
            crate::context::BindingContext::grant(
                "tenant".into(),
                "worker".into(),
                "version".into(),
                "worker~DB".into(),
                "DB".into(),
                BindingResource::D1Database {
                    id: DB.into(),
                    epoch: 1,
                    writer_addr: None,
                },
                Permissions {
                    read: true,
                    write: true,
                },
                1,
                1,
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
        };
        let query = br#"[{"sql":"SELECT count(*) AS n FROM t"}]"#;

        let (status, _, body) = client
            .forward(
                &request(),
                "query",
                "",
                Bytes::from_static(query),
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(served_by(&body), "vrt-sql-replica");

        let (_, _, body) = client
            .forward(
                &request(),
                "query",
                "",
                Bytes::from_static(query),
                vec![(BOOKMARK_HEADER.to_string(), "first-primary".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(served_by(&body), "vrt-sql");

        let (status, write_bookmark, _) = client
            .forward(
                &request(),
                "execute",
                "",
                Bytes::from_static(br#"[{"sql":"INSERT INTO t VALUES (2)"}]"#),
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(status, 200);
        let write_bookmark = write_bookmark.expect("writer mints a bookmark");

        let (_, _, body) = client
            .forward(
                &request(),
                "query",
                "",
                Bytes::from_static(query),
                vec![(BOOKMARK_HEADER.to_string(), write_bookmark.clone())],
            )
            .await
            .unwrap();
        assert_eq!(served_by(&body), "vrt-sql");
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v[0]["results"][0]["n"], 2);

        follower.sync_once().unwrap();
        let (_, _, body) = client
            .forward(
                &request(),
                "query",
                "",
                Bytes::from_static(query),
                vec![(BOOKMARK_HEADER.to_string(), write_bookmark)],
            )
            .await
            .unwrap();
        assert_eq!(served_by(&body), "vrt-sql-replica");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn t35_database_id_shape() {
        let ok = |id: &str| {
            id.len() == 26
                && id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        };
        assert!(ok(&"a".repeat(26)));
        for bad in [
            "",
            &"a".repeat(25),
            &"A".repeat(26),
            "../..",
            "abc/../../etc/passwdzz",
        ] {
            assert!(!ok(bad));
        }
    }
}
