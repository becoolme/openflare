//! `CACHE_PURGE` cell-op executor — fan-out Workers Cache invalidation
//! to every vrt-host pod's loopback purge endpoint.
//!
//! Why fan-out instead of rendezvous: cache entries are pod-local and
//! ingress routing may serve the same worker from any pod, so a purge
//! must reach ALL pods. Purge is idempotent (purging an absent entry
//! is a no-op), which makes the op model's at-least-once redelivery
//! safe: a partial fan-out retried later converges.
//!
//! Trust boundary (same shape as STORAGE_PURGE): the payload carries
//! `{tenantId, workerId?|tags?|pathPrefix?|all?}` — identities and
//! bounds are re-validated here with the same rules the vrt-host
//! endpoint applies (`wc_admin.rs`), so a malformed op is FAILED
//! before any network IO and a forged payload can never reach the
//! wire in a shape the host would accept.
//!
//! Fail closed: malformed payload, no wired executor, token absent,
//! or any pod failing → terminal FAILED with a bounded fixed-phrase
//! error. DONE requires every pod to confirm 200.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::execset::{CellOp, CellOpAck};

pub const CACHE_PURGE_KIND: &str = "CACHE_PURGE";

/// `[cache_purge]` — executor wiring. The token is the SAME secret the
/// pods' vrt-host processes read from `VRT_CACHE_PURGE_TOKEN` (in k8s
/// one Secret mounted into both containers of the cell pod). Absent
/// section or empty/unset token ⇒ `cachePurge` capability false and
/// purge ops are never dispatched to this cell.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CachePurgeConfig {
    /// Env var holding the shared purge token. Default
    /// `VRT_CACHE_PURGE_TOKEN` — the same name vrt-host reads, so one
    /// Secret projection serves both containers.
    #[serde(default = "default_token_env")]
    pub token_env: String,
    /// Per-pod request timeout in milliseconds (fan-out is concurrent;
    /// the op latency is the slowest pod, not the sum).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_token_env() -> String {
    "VRT_CACHE_PURGE_TOKEN".to_string()
}

fn default_timeout_ms() -> u64 {
    5_000
}

/// Op payload — exactly one purge mode, mirroring `wc_admin.rs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PurgePayload {
    tenant_id: String,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    path_prefix: Option<String>,
    #[serde(default)]
    all: Option<bool>,
}

/// `wc_admin.rs::valid_id` — keep the charset/bounds byte-identical:
/// this executor is the only legitimate caller, so rejecting anything
/// the host would reject must happen here, before the wire.
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

/// Validate the payload exactly as the host endpoint will. Returns the
/// canonicalized body to forward, or a fixed-phrase error.
fn validate(payload: &PurgePayload) -> Result<serde_json::Value, &'static str> {
    if !valid_id(&payload.tenant_id) {
        return Err("invalid tenantId");
    }
    let modes = [
        payload.worker_id.is_some(),
        payload.tags.is_some(),
        payload.path_prefix.is_some(),
        payload.all.unwrap_or(false),
    ]
    .into_iter()
    .filter(|v| *v)
    .count();
    if modes != 1 {
        return Err("exactly one of workerId/tags/pathPrefix/all");
    }
    if let Some(worker) = &payload.worker_id {
        if !valid_id(worker) {
            return Err("invalid workerId");
        }
    }
    if let Some(tags) = &payload.tags {
        // Mirror vrt_host::wc bounds — a tag set the host would reject
        // must never leave this process.
        if tags.is_empty() || tags.len() > 1000 {
            return Err("invalid tags");
        }
    }
    if let Some(prefix) = &payload.path_prefix {
        if !prefix.starts_with('/') || prefix.len() > 2048 {
            return Err("pathPrefix must start with /");
        }
    }
    // Forward exactly the validated fields — never the raw payload, so
    // a future host-side deny_unknown_fields tightening can never be
    // bypassed through this executor.
    let mut body = serde_json::Map::new();
    body.insert(
        "tenantId".to_string(),
        serde_json::Value::String(payload.tenant_id.clone()),
    );
    if let Some(worker) = &payload.worker_id {
        body.insert(
            "workerId".to_string(),
            serde_json::Value::String(worker.clone()),
        );
    }
    if let Some(tags) = &payload.tags {
        body.insert(
            "tags".to_string(),
            serde_json::Value::Array(
                tags.iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            ),
        );
    }
    if let Some(prefix) = &payload.path_prefix {
        body.insert(
            "pathPrefix".to_string(),
            serde_json::Value::String(prefix.clone()),
        );
    }
    if payload.all.unwrap_or(false) {
        body.insert("all".to_string(), serde_json::Value::Bool(true));
    }
    Ok(serde_json::Value::Object(body))
}

pub struct CachePurgeExecutor {
    /// `podId → loopback base` — the SAME `[[pods]].readyz` map the
    /// wfops executor consumes; the purge route is mounted on the same
    /// control listener.
    pods: Vec<(String, String)>,
    token: Arc<str>,
    http: reqwest::Client,
}

impl CachePurgeExecutor {
    /// Build from config + the `[[pods]]` loopback bases. Returns Err
    /// when the token env is unset/empty or a pod base is not loopback
    /// http — the caller treats Err as capability-off (fail closed).
    pub fn connect(
        config: &CachePurgeConfig,
        pods: &[(String, String)],
    ) -> Result<Self, String> {
        if pods.is_empty() {
            return Err("cache purge requires [[pods]] loopback bases".to_string());
        }
        let token = std::env::var(&config.token_env)
            .map_err(|_| format!("{} is not set", config.token_env))?;
        if token.is_empty() {
            return Err(format!("{} is empty", config.token_env));
        }
        for (id, base) in pods {
            let url = reqwest::Url::parse(base).map_err(|e| format!("pod {id} base: {e}"))?;
            if url.scheme() != "http" {
                return Err(format!("pod {id} base must be http (loopback)"));
            }
            let host = url
                .host_str()
                .ok_or_else(|| format!("pod {id} base: no host"))?;
            let loopback = host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .map(|ip| ip.is_loopback())
                    .unwrap_or(false);
            if !loopback {
                return Err(format!("pod {id} base must be loopback"));
            }
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| format!("cache purge http client: {e}"))?;
        Ok(Self {
            pods: pods.to_vec(),
            token: Arc::from(token.as_str()),
            http,
        })
    }

    pub async fn execute(&self, op: &CellOp) -> CellOpAck {
        let payload: PurgePayload = match serde_json::from_value(op.payload.clone()) {
            Ok(p) => p,
            Err(_) => return failed(op, "malformed cache purge payload"),
        };
        let body = match validate(&payload) {
            Ok(b) => b,
            Err(e) => return failed(op, e),
        };
        // Fan out concurrently — entries are pod-local, every pod must
        // confirm. JoinSet over one future per pod; the op latency is
        // the slowest pod (bounded by the client timeout).
        let mut set = tokio::task::JoinSet::new();
        for (id, base) in &self.pods {
            let url = format!("{}/internal/cache/purge", base.trim_end_matches('/'));
            let http = self.http.clone();
            let token = self.token.clone();
            let body = body.clone();
            let id = id.clone();
            set.spawn(async move {
                let result = http
                    .post(&url)
                    .header("x-vrt-cache-token", token.as_ref())
                    .json(&body)
                    .send()
                    .await;
                (id, result)
            });
        }
        let mut total_purged: u64 = 0;
        let mut pod_count: u64 = 0;
        let mut transport_failed = false;
        let mut rejected = false;
        while let Some(joined) = set.join_next().await {
            let (_id, result) = match joined {
                Ok(v) => v,
                Err(_) => {
                    transport_failed = true;
                    continue;
                }
            };
            match result {
                Ok(resp) if resp.status().is_success() => {
                    pod_count += 1;
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        if let Some(n) = json.get("purged").and_then(|v| v.as_u64()) {
                            total_purged += n;
                        }
                    }
                }
                // A 4xx from the host means OUR validated body was
                // rejected — retrying cannot help, and the fixed phrase
                // must not leak host detail beyond the status class.
                Ok(resp) if resp.status().is_client_error() => {
                    rejected = true;
                }
                // 5xx / transport: retry may converge — the op FAILEDs
                // terminally either way (the control plane decides on
                // redispatch; purge is idempotent so replay is safe).
                Ok(_) => transport_failed = true,
                Err(_) => transport_failed = true,
            }
        }
        if rejected {
            return failed(op, "cache purge rejected by pod");
        }
        if transport_failed {
            return failed(op, "cache purge fan-out incomplete");
        }
        CellOpAck {
            seq: op.seq,
            status: "DONE",
            result: Some(serde_json::json!({
                "purged": total_purged,
                "pods": pod_count,
            })),
            error: None,
        }
    }
}

fn failed(op: &CellOp, error: &str) -> CellOpAck {
    // Bounded fixed-phrase errors — host/backend detail never leaks
    // into the control-plane registry row.
    CellOpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(crate::execset::truncate_utf16(error, 512).0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener as TokListener;

    fn op(payload: serde_json::Value) -> CellOp {
        CellOp {
            id: "op-1".to_string(),
            seq: 7,
            kind: CACHE_PURGE_KIND.to_string(),
            worker_id: String::new(),
            resource_key: String::new(),
            payload,
            expires_at: None,
        }
    }

    struct StubState {
        token: &'static str,
        status: StatusCode,
        hits: Arc<AtomicUsize>,
    }

    async fn stub_handler(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        body: Json<serde_json::Value>,
    ) -> impl IntoResponse {
        state.hits.fetch_add(1, Ordering::SeqCst);
        let got = headers
            .get("x-vrt-cache-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if got != state.token {
            return (StatusCode::FORBIDDEN, Json(serde_json::json!({"error":"forbidden"})));
        }
        if state.status != StatusCode::OK {
            return (state.status, Json(serde_json::json!({"error":"x"})));
        }
        let _ = body;
        (StatusCode::OK, Json(serde_json::json!({"purged": 3})))
    }

    /// Spawn a stub purge endpoint on loopback; returns its base URL.
    async fn spawn_stub(token: &'static str, status: StatusCode) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(StubState {
            token,
            status,
            hits: hits.clone(),
        });
        let app = Router::new()
            .route("/internal/cache/purge", post(stub_handler))
            .with_state(state);
        let listener = TokListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://127.0.0.1:{}", addr.port()), hits)
    }

    fn executor(pods: &[(String, String)]) -> CachePurgeExecutor {
        std::env::set_var("VRT_CACHE_PURGE_TOKEN", "test-token");
        CachePurgeExecutor::connect(
            &CachePurgeConfig {
                token_env: "VRT_CACHE_PURGE_TOKEN".to_string(),
                timeout_ms: 3_000,
            },
            pods,
        )
        .expect("connect")
    }

    #[tokio::test]
    async fn fans_out_to_all_pods_and_sums_purged() {
        let (base_a, hits_a) = spawn_stub("test-token", StatusCode::OK).await;
        let (base_b, hits_b) = spawn_stub("test-token", StatusCode::OK).await;
        let pods = vec![
            ("pod-a".to_string(), base_a),
            ("pod-b".to_string(), base_b),
        ];
        let exec = executor(&pods);
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"t1","all":true})))
            .await;
        assert_eq!(ack.status, "DONE");
        assert_eq!(ack.result.as_ref().unwrap()["purged"], 6);
        assert_eq!(ack.result.as_ref().unwrap()["pods"], 2);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn malformed_payload_fails_before_any_io() {
        let (base, hits) = spawn_stub("test-token", StatusCode::OK).await;
        let pods = vec![("pod-a".to_string(), base)];
        let exec = executor(&pods);
        // zero modes
        let ack = exec.execute(&op(serde_json::json!({"tenantId":"t1"}))).await;
        assert_eq!(ack.status, "FAILED");
        // two modes
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"t1","all":true,"tags":["x"]})))
            .await;
        assert_eq!(ack.status, "FAILED");
        // bad tenant charset
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"../etc","all":true})))
            .await;
        assert_eq!(ack.status, "FAILED");
        // prefix not absolute
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"t1","pathPrefix":"etc"})))
            .await;
        assert_eq!(ack.status, "FAILED");
        // unknown key rejected by deny_unknown_fields
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"t1","all":true,"hack":1})))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn any_pod_failure_fails_the_op() {
        let (base_ok, _) = spawn_stub("test-token", StatusCode::OK).await;
        let (base_err, _) = spawn_stub("test-token", StatusCode::INTERNAL_SERVER_ERROR).await;
        let pods = vec![
            ("pod-ok".to_string(), base_ok),
            ("pod-err".to_string(), base_err),
        ];
        let exec = executor(&pods);
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"t1","workerId":"w1"})))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(ack.error.as_deref(), Some("cache purge fan-out incomplete"));
    }

    #[tokio::test]
    async fn wrong_token_surfaces_as_rejected() {
        // Stub expects a different token than the executor sends.
        let (base, _) = spawn_stub("other-token", StatusCode::OK).await;
        let pods = vec![("pod-a".to_string(), base)];
        let exec = executor(&pods);
        let ack = exec
            .execute(&op(serde_json::json!({"tenantId":"t1","all":true})))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(ack.error.as_deref(), Some("cache purge rejected by pod"));
    }

    #[tokio::test]
    async fn connect_fails_closed_without_token_or_loopback() {
        std::env::remove_var("VRT_CACHE_PURGE_TOKEN_UNSET_TEST");
        let result = CachePurgeExecutor::connect(
            &CachePurgeConfig {
                token_env: "VRT_CACHE_PURGE_TOKEN_UNSET_TEST".to_string(),
                timeout_ms: 1_000,
            },
            &[("pod-a".to_string(), "http://127.0.0.1:9091".to_string())],
        );
        assert!(result.is_err());
        std::env::set_var("VRT_CACHE_PURGE_TOKEN_UNSET_TEST", "x");
        let result = CachePurgeExecutor::connect(
            &CachePurgeConfig {
                token_env: "VRT_CACHE_PURGE_TOKEN_UNSET_TEST".to_string(),
                timeout_ms: 1_000,
            },
            &[("pod-a".to_string(), "http://10.0.0.1:9091".to_string())],
        );
        assert!(result.is_err());
        let result = CachePurgeExecutor::connect(
            &CachePurgeConfig {
                token_env: "VRT_CACHE_PURGE_TOKEN_UNSET_TEST".to_string(),
                timeout_ms: 1_000,
            },
            &[],
        );
        assert!(result.is_err());
        std::env::remove_var("VRT_CACHE_PURGE_TOKEN_UNSET_TEST");
    }
}
