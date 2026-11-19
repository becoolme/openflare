//! D1 数据面（T34）：facade-d1 → provider → vrt-sql。
//!
//! provider 的职责：鉴权（签名 props→Authorizer）→ 按快照把
//! `bindingId` 映射到 `databaseId`（Worker 不可指定）→ 有界限流
//! → 限额预检 → 分配 seq（全局单调，按 databaseId 子序列亦单调）→
//! 向 vrt-sql 发签名请求。**结果不确定的写绝不重试**（§3 R4）。
//! vrt-sql 不可用时返回 D1 错误面 `{success:false,error:"D1_ERROR: …"}`，
//! 写结构化日志与指标，绝不挂起。

use crate::context::{BindingContext, BindingResource};
use crate::error::ProviderError;
use axum::body::Bytes;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// 与 vrt-sql::service 同款的认证头（共享 wire 格式，见 service.rs）。
pub const SQL_AUTH_HEADER: &str = "x-vrt-sql-auth";
pub const SQL_EPOCH_HEADER: &str = "x-vrt-epoch";
pub const SQL_SEQ_HEADER: &str = "x-vrt-seq";
/// RV-R71: remaining request budget in ms (shrink-only on the vrt-sql side).
pub const SQL_DEADLINE_MS_HEADER: &str = "x-vrt-deadline-ms";
/// bookmark/session 头（透传给调用方）。
pub const BOOKMARK_HEADER: &str = "x-cf-d1-session-commit-token";

/// 预检上限：与 vrt-sql::security 的 limit 口径一致（引擎仍是权威）。
pub const D1_MAX_SQL_BYTES: usize = 100 * 1024;
pub const D1_MAX_PARAMS: usize = 100;
/// 单请求体上限（先于读 body 校验）。
pub const D1_MAX_BODY_BYTES: usize = 1024 * 1024;
/// 上游响应上限（RV-R8 #8）：与 vrt-sql 自身 `body_max_bytes` 上界
/// （64MiB）一致——vrt-sql 不可能产生更大的合法响应。
pub const D1_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

type HmacSha256 = Hmac<Sha256>;

fn unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// provider→vrt-sql 签名器（与 vrt-sql::service::ServiceAuth 同一 wire
/// 格式：`v1:<ts>:<hmac(ts|method|path?query|sha256(body)|epoch|seq)>`）。
/// 有意不复用 vrt-sql crate —— 保持服务边界，格式注释双侧同步。
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

    /// `epoch`/`seq` 与实际上送的头值一致（签名覆盖它们）。
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

/// vrt-sql 下游客户端。
pub struct D1Client {
    http: reqwest::Client,
    /// 例 `http://127.0.0.1:8090`（无尾斜杠）。
    addr: String,
    signer: SqlAuthSigner,
    timeout: Duration,
    /// 全局单调 seq（跨 databaseId 单调 ⇒ 按库子序列也单调）。
    /// 种子取 unix-nanos：provider 重启后 seq 继续走高而非回绕到 1
    /// ——epoch 固定为 1 的本轮里，回绕会让**新写**撞上 `_vrt_dedup`
    /// 旧行（引擎现亦校验负载，双保险）。假设：墙钟不回退到历史
    /// 高水位以下（与 ±30s 认证窗口同一时钟信任基）。
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
            signer: SqlAuthSigner::new(secret),
            timeout,
            seq: AtomicU64::new(unix_nanos()),
        })
    }

    /// 分配下一个 seq（公开供计量/测试；内部由 forward 调用）。
    pub fn next_seq(&self) -> u64 {
        // CAS-max(cur+1, now+1)：进程内严格递增，且计数器随墙钟
        // 抬升（重启种子必高于上一轮已发出的 seq）。
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

    /// 限额预检：sql 长度与参数数（权威限额仍在引擎侧）。
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

    /// 转发一次 D1 请求。返回（status, bookmark, body 字节）。
    /// **绝不重试**：单次发送，任何失败（连接/超时/5xx）返回 Err。
    pub async fn forward(
        &self,
        ctx: &BindingContext,
        op: &str,
        path_query_suffix: &str,
        body: Bytes,
        session_headers: Vec<(String, String)>,
    ) -> Result<(u16, Option<String>, Bytes), ProviderError> {
        let (database_id, epoch) = match ctx.resource() {
            BindingResource::D1Database { id, epoch } => (id.clone(), *epoch),
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
        let seq = self.next_seq();
        let path_query = format!("/dbs/{database_id}/{op}{path_query_suffix}");
        let url = format!("{}{}", self.addr, path_query);
        let auth = self.signer.sign("POST", &path_query, &body, epoch, seq);
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
            .body(body);
        for (k, v) in session_headers {
            if let (Ok(name), Ok(value)) = (
                k.parse::<reqwest::header::HeaderName>(),
                reqwest::header::HeaderValue::from_str(&v),
            ) {
                req = req.header(name, value);
            }
        }
        let started = std::time::Instant::now();
        let overall = tokio::time::timeout(budget, async {
            let resp = req.send().await.map_err(|e| {
                tracing::warn!(
                    worker = ctx.worker_id(),
                    binding = ctx.binding_name(),
                    error = %e,
                    "vrt-sql backend call failed (no retry)"
                );
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
                    match e {
                        crate::upstream_body::UpstreamBodyErr::TooLarge => tracing::warn!(
                            worker = ctx.worker_id(),
                            binding = ctx.binding_name(),
                            cap = D1_MAX_RESPONSE_BYTES,
                            "d1 upstream response exceeded byte cap"
                        ),
                        crate::upstream_body::UpstreamBodyErr::Transport => {}
                    }
                    ProviderError::ResourceUnavailable
                })?;
            Ok::<_, ProviderError>((status, bookmark, bytes))
        })
        .await;
        match overall {
            Ok(r) => r,
            Err(_) => {
                // Deadline cut: the write may or may not have committed
                // upstream; surface as unavailable and NEVER retry here.
                tracing::warn!(
                    worker = ctx.worker_id(),
                    binding = ctx.binding_name(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "d1 forward cut at request deadline"
                );
                Err(ProviderError::ResourceUnavailable)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{BindingContext, BindingResource, Permissions};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn ctx(budget_ms: u64) -> BindingContext {
        BindingContext::grant(
            "t".into(),
            "w".into(),
            "v".into(),
            "w~DB".into(),
            "DB".into(),
            BindingResource::D1Database {
                id: "a".repeat(26),
                epoch: 1,
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
