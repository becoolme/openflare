//! D1 wire 层（§2 协议规格）：`POST /dbs/{id}/query` 与 `POST /dbs/{id}/execute`
//! 同一 handler。两个错误面：SQL 失败 = HTTP 200 + `{success:false,error}`；
//! 畸形请求（schema 不合）= HTTP 400 + 非 `{success:false}` 形态。

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::queue::QueueBackend;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use serde_json::json;

use crate::bookmark::BookmarkSigner;
use crate::engine::{EngineError, EngineRegistry, ResultsFormat, RusqliteEngine, SqlStmt, TxnCtx};

pub const BOOKMARK_HEADER: &str = "x-cf-d1-session-commit-token";
/// provider→vrt-sql 内部扩展头（§3 R4 的 (epoch, seq) 去重键）。
/// **信任边界（S4）**：handler 不直接读原始头——由调用方在内部认证
/// 通过后构造 `TrustedCallCtx`。用户 Worker 无法控制这两个值
/// （facade/provider 会覆写）。T34 接入真实 provider 认证。
pub const EPOCH_HEADER: &str = "x-vrt-epoch";
pub const SEQ_HEADER: &str = "x-vrt-seq";
/// RV-R71: remaining request budget (ms) forwarded by vrt-provider.
/// Shrink-only: the effective budget is `min(txn_budget, header)`;
/// missing / non-numeric / zero values are ignored and a value larger
/// than `txn_budget` can never enlarge it.
pub const DEADLINE_MS_HEADER: &str = "x-vrt-deadline-ms";

/// RV-R71: pure helper so the clamp rule is unit-testable.
pub fn clamp_budget(own: Duration, header: Option<&str>) -> Duration {
    match header.and_then(|v| v.trim().parse::<u64>().ok()) {
        Some(ms) if ms > 0 => own.min(Duration::from_millis(ms)),
        _ => own,
    }
}

const DEFAULT_TXN_BUDGET_MS: u64 = 30_000;

/// 可信调用上下文（S4）：只能由已通过内部认证的调用方构造。
/// T34：vrt-provider 侧服务间认证（mTLS/HMAC）通过后填充；
/// 用户 Worker 流量永远到不了这一步。
pub struct TrustedCallCtx {
    /// 当前写者 epoch（租约位点）。
    pub epoch: u64,
    /// 调用方提供的去重序列号；None = 不去重。
    pub seq: Option<u64>,
    /// 本批 deadline。
    pub deadline: Instant,
}

impl TrustedCallCtx {
    /// 测试/内部直通构造（无去重）。
    pub fn test(deadline: Instant) -> Self {
        Self {
            epoch: 0,
            seq: None,
            deadline,
        }
    }
}

pub struct WireResponse {
    pub status: StatusCode,
    pub bookmark: Option<String>,
    pub body: serde_json::Value,
    /// 本批累计读取行数（§10 近似口径；指标用）。
    pub rows_read: u64,
    /// 本批累计写入行数。
    pub rows_written: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<EngineRegistry>,
    pub signer: BookmarkSigner,
    pub txn_budget: Duration,
    /// T34：provider→vrt-sql 服务间认证。`None` 仅限单测直通。
    pub auth: Option<crate::service::ServiceAuth>,
    /// 有界并发（try_acquire，超限拒绝）。
    pub permits: Arc<tokio::sync::Semaphore>,
    /// 请求体上限——读 body 前校验 Content-Length。
    pub body_max: usize,
    /// 有界 databaseId → label。
    pub db_labels: Arc<crate::service::DbLabels>,
    /// 排空计数（优雅退出）。
    pub inflight: Arc<tokio::sync::Semaphore>,
}

impl AppState {
    pub fn new(db_root: std::path::PathBuf, hmac_key: [u8; 32]) -> Self {
        Self {
            registry: Arc::new(EngineRegistry::new(db_root)),
            signer: BookmarkSigner::new(hmac_key),
            txn_budget: Duration::from_millis(DEFAULT_TXN_BUDGET_MS),
            auth: None,
            permits: Arc::new(tokio::sync::Semaphore::new(64)),
            body_max: 1024 * 1024,
            db_labels: Arc::new(crate::service::DbLabels::default()),
            inflight: Arc::new(tokio::sync::Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    /// 挂服务间认证（生产路径必设）。
    pub fn with_auth(mut self, auth: crate::service::ServiceAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_permits(mut self, n: usize) -> Self {
        self.permits = Arc::new(tokio::sync::Semaphore::new(n));
        self
    }

    pub fn with_body_max(mut self, n: usize) -> Self {
        self.body_max = n;
        self
    }

    /// 优雅退出：等待所有在途许可归还。
    pub async fn drain(&self) {
        // inflight 满额信号量：等待能一次性拿满 ⇒ 在途为零。
        let _ = self
            .inflight
            .clone()
            .acquire_many_owned(Semaphore::MAX_PERMITS as u32)
            .await;
    }
}

use tokio::sync::Semaphore;

/// 由原始头构造 TrustedCallCtx。**仅在服务间认证通过后调用**
/// （T34 已落地：query_handler 先验签再进这里）——epoch/seq 头
/// 值已被签名覆盖，篡改在验签阶段即被拒。
fn trusted_ctx_from_headers(headers: &HeaderMap, deadline: Instant) -> Result<TrustedCallCtx, ()> {
    let epoch = headers.get(EPOCH_HEADER);
    let seq = headers.get(SEQ_HEADER);
    let (epoch, seq) = match (epoch, seq) {
        (None, None) => (0, None),
        (Some(e), Some(s)) => {
            let e = e
                .to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(())?;
            let s = s
                .to_str()
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(())?;
            (e, Some(s))
        }
        _ => return Err(()),
    };
    Ok(TrustedCallCtx {
        epoch,
        seq,
        deadline,
    })
}

/// T36：错误面扩展——`code`/`retryable` 附加字段（向后兼容，
/// D1 `{success,error}` 形态不变）。Fenced/Unavailable 供 provider
/// 映射为 D1 retryable。`pub`：跨语言 wire fixture 直接取真实输出
/// (RV-R68)。
pub fn error_body(e: &EngineError) -> serde_json::Value {
    match e {
        EngineError::Fenced(_) => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_FENCED",
            "retryable": true,
        }),
        EngineError::Unavailable(_) => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_UNAVAILABLE",
            "retryable": true,
        }),
        // RV-R71: deadline expired before anything committed — safe retry.
        EngineError::DeadlineExceeded => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_SQL_TIMEOUT",
            "retryable": true,
        }),
        // RV-R71: deadline expired after commit started — outcome unknown;
        // retry only with the same (epoch, seq) idempotency key.
        EngineError::ResultUnknown => json!({
            "success": false,
            "error": e.wire_message(),
            "code": "VRT_SQL_RESULT_UNKNOWN",
            "retryable": true,
        }),
        _ => json!({ "success": false, "error": e.wire_message() }),
    }
}

/// 暂态不可用 → 503（其余错误面照旧 HTTP 200）。
/// `pub`：wire fixture 同时记录状态码与 body (RV-R68)。
pub fn error_status(e: &EngineError) -> StatusCode {
    match e {
        EngineError::Unavailable(_)
        | EngineError::DeadlineExceeded
        | EngineError::ResultUnknown => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::OK,
    }
}

/// 纯函数 handler：`/query` 与 `/execute` 语义完全一致（§2）。
/// `results_format` 由调用方解析 URL 参数传入（非法值回落 ARRAY_OF_OBJECTS）。
pub async fn handle_d1(
    state: &AppState,
    database_id: &str,
    results_format: Option<&str>,
    call: TrustedCallCtx,
    body: &[u8],
) -> WireResponse {
    // 畸形请求面（非 {success:false}）：JSON 解析失败或 schema 不合
    let parsed: Result<Vec<SqlStmt>, ()> = (|| {
        let v: serde_json::Value = serde_json::from_slice(body).map_err(|_| ())?;
        let one_or_many = match v {
            serde_json::Value::Array(a) => serde_json::Value::Array(a),
            o @ serde_json::Value::Object(_) => json!([o]),
            _ => return Err(()),
        };
        serde_json::from_value::<Vec<SqlStmt>>(one_or_many).map_err(|_| ())
    })();
    let stmts = match parsed {
        Ok(s) => s,
        Err(()) => {
            return WireResponse {
                status: StatusCode::BAD_REQUEST,
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: json!({ "error": "invalid request: expected {sql, params?} or array of such" }),
            }
        }
    };
    let ctx = TxnCtx {
        epoch: call.epoch,
        deadline: call.deadline,
        dedup: call.seq.map(|s| (call.epoch, s)),
    };
    let format = ResultsFormat::parse(results_format);

    // RV-R71: engine open + SQLite + durable publish are all blocking
    // (SQLite, std Mutex, object-store bridge). Run them on a blocking
    // thread, never on a tokio worker, and cut the wait at the call
    // deadline (+ small grace so the engine's own, more precise, deadline
    // errors win the race). The blocking task itself is bounded too:
    // lock wait, lease renew, publish and SQLite's progress handler all
    // honour `ctx.deadline`.
    let probe = crate::engine::CommitProbe::new();
    let task = {
        let registry = state.registry.clone();
        let signer = state.signer.clone();
        let probe = probe.clone();
        let database_id = database_id.to_string();
        let call_epoch = call.epoch;
        tokio::task::spawn_blocking(move || {
            crate::engine::with_commit_probe(&probe, || {
                let engine = registry
                    .get_until(crate::paths::ResourceKind::D1, &database_id, ctx.deadline)
                    .map_err(|e| (e, false))?;
                match run_query(&engine, &stmts, &ctx, format) {
                    Ok(results) => {
                        // T36：degraded（lease 有效但续租失败）→ 读仍放行但计数。
                        if engine.is_degraded() {
                            crate::metrics::degraded_read();
                        }
                        // T36：bookmark epoch = 写者 lease epoch（有持久化时），
                        // 与 manifest epoch 对齐；无持久化保持调用方 epoch。
                        let book_epoch = if engine.persistence_enabled() {
                            engine.lease_epoch()
                        } else {
                            call_epoch
                        };
                        let bookmark = signer.sign(&database_id, book_epoch, engine.commit_lsn());
                        Ok((results, bookmark))
                    }
                    Err(e) => {
                        // T36：fenced → 摘除引擎，下次 get 走 claim/restore 自愈。
                        if let EngineError::Fenced(ep) = e {
                            registry.evict_fenced(crate::paths::ResourceKind::D1, &database_id, ep);
                        }
                        Err((e, true))
                    }
                }
            })
        })
    };
    let cutoff = tokio::time::Instant::from_std(call.deadline) + D1_OUTER_GRACE;
    let joined = match tokio::time::timeout_at(cutoff, task).await {
        Ok(j) => j,
        Err(_) => {
            // The blocking task is not cancellable; it will unwind at its
            // own deadline checks. The caller must not be told "success".
            let e = if probe.committed() {
                EngineError::ResultUnknown
            } else {
                EngineError::DeadlineExceeded
            };
            tracing::warn!(db = %database_id, code = %error_body(&e)["code"], "d1 call cut at deadline");
            return WireResponse {
                status: error_status(&e),
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: error_body(&e),
            };
        }
    };
    let outcome = match joined {
        Ok(r) => r,
        Err(_) => {
            // spawn_blocking task panicked: fail closed, retryable, and
            // never claim success. (No SQL text / params are logged.)
            tracing::error!(db = %database_id, "d1 blocking task panicked");
            let e = EngineError::Unavailable("internal task failed");
            return WireResponse {
                status: error_status(&e),
                bookmark: None,
                rows_read: 0,
                rows_written: 0,
                body: error_body(&e),
            };
        }
    };
    match outcome {
        Ok((results, bookmark)) => {
            let rows_read = results.iter().map(|r| r.meta.rows_read.max(0) as u64).sum();
            let rows_written = results
                .iter()
                .map(|r| r.meta.rows_written.max(0) as u64)
                .sum();
            WireResponse {
                status: StatusCode::OK,
                bookmark: Some(bookmark),
                rows_read,
                rows_written,
                body: json!(results),
            }
        }
        // 路径/锁/打开失败 → {success:false} 错误面（与 SQL 失败同形）；
        // T36: Unavailable → 503（held-elsewhere/restore 失败可重试）。
        Err((e, _ran)) => WireResponse {
            status: error_status(&e),
            bookmark: None,
            rows_read: 0,
            rows_written: 0,
            body: error_body(&e),
        },
    }
}

/// RV-R71: how long past the call deadline the async side keeps waiting
/// for the blocking task's own (more precise) deadline error.
const D1_OUTER_GRACE: Duration = Duration::from_millis(500);

fn run_query(
    engine: &Arc<RusqliteEngine>,
    stmts: &[SqlStmt],
    ctx: &TxnCtx,
    format: ResultsFormat,
) -> Result<Vec<crate::engine::QueryResult>, EngineError> {
    engine.query_batch_sync(stmts, ctx, format)
}

// ---------- axum 装配 ----------

#[derive(serde::Deserialize)]
struct FormatParam {
    #[serde(rename = "resultsFormat")]
    results_format: Option<String>,
}

fn endpoint_label(path: &str) -> &'static str {
    if path.ends_with("/query") {
        "query"
    } else {
        "execute"
    }
}

/// T34 装配顺序：并发许可 → Content-Length 先验 → body 收集
/// （DefaultBodyLimit 兜底）→ 服务间验签 → 之后才解析
/// `x-vrt-epoch`/`x-vrt-seq` 构造 TrustedCallCtx。认证失败一律
/// 401 + `outcome="unauthorized"`，绝不读 x-vrt-*。
async fn query_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let endpoint = endpoint_label(request.uri().path());
    let fail = |outcome: &'static str, status: StatusCode, body: serde_json::Value| {
        crate::metrics::query(endpoint, outcome, started.elapsed().as_secs_f64());
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    // 有界并发（try_acquire，绝不排队）。
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "rate_limited",
                StatusCode::TOO_MANY_REQUESTS,
                json!({"success": false, "error": "vrt-sql overloaded"}),
            )
        }
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"success": false, "error": "vrt-sql draining"}),
            )
        }
    };
    // Content-Length 先验：超限直接拒，不读 body（防未认证大包）。
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            );
        }
    }
    let database_id = request
        .uri()
        .path()
        .trim_start_matches("/dbs/")
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string();
    let db_id = request
        .uri()
        .path()
        .strip_prefix("/dbs/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_else(|| database_id.clone());
    let results_format = Query::<FormatParam>::try_from_uri(request.uri())
        .ok()
        .and_then(|q| q.0.results_format);
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            )
        }
    };
    // 服务间认证：验签覆盖 method/path+query/body/epoch/seq。
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return fail(
                "unauthorized",
                StatusCode::UNAUTHORIZED,
                json!({"error": "unauthorized"}),
            );
        }
    }
    // 认证通过后才允许采信 x-vrt-*。
    let budget = clamp_budget(
        state.txn_budget,
        headers
            .get(DEADLINE_MS_HEADER)
            .and_then(|v| v.to_str().ok()),
    );
    let deadline = Instant::now() + budget;
    let call = match trusted_ctx_from_headers(&headers, deadline) {
        Ok(c) => c,
        Err(()) => {
            return fail(
                "protocol_error",
                StatusCode::BAD_REQUEST,
                json!({ "error": "invalid request: x-vrt-epoch/x-vrt-seq must be u64 pair" }),
            )
        }
    };
    let r = handle_d1(&state, &db_id, results_format.as_deref(), call, &body).await;
    // 指标：endpoint×outcome + rows（有界 db label）。
    let outcome = if r.status != StatusCode::OK {
        if r.body.get("code") == Some(&serde_json::Value::String("VRT_UNAVAILABLE".into())) {
            "unavailable"
        } else if matches!(
            r.body.get("code").and_then(|c| c.as_str()),
            Some("VRT_SQL_TIMEOUT" | "VRT_SQL_RESULT_UNKNOWN")
        ) {
            "timeout"
        } else {
            "protocol_error"
        }
    } else if r.body.get("success") == Some(&serde_json::Value::Bool(false)) {
        match r.body.get("code").and_then(|c| c.as_str()) {
            Some("VRT_FENCED") => "fenced",
            _ => "sql_error",
        }
    } else {
        "ok"
    };
    crate::metrics::query(endpoint, outcome, started.elapsed().as_secs_f64());
    let db_label = state.db_labels.label(&db_id);
    crate::metrics::rows(&db_label, r.rows_read, r.rows_written);
    let mut b = Response::builder().status(r.status);
    if let Some(bm) = r.bookmark {
        b = b.header(BOOKMARK_HEADER, bm);
    }
    b.header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r.body.to_string()))
        .unwrap()
}

/// T36：只读诊断 `GET /internal/dbs`——fenced/degraded 库与最后
/// 已上传 lsn。与查询同一 ServiceAuth 验签（GET + 空 body，
/// epoch=0 seq=none 由签名串覆盖）。未配置 auth 时（单测）放行。
async fn diag_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, b"") {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"error": "unauthorized"}).to_string()))
                .unwrap();
        }
    }
    let dbs = state.registry.diagnostics();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(json!({"databases": dbs}).to_string()))
        .unwrap()
}

async fn healthz() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({"ok": true}).to_string()))
        .unwrap()
}

/// T35b：受限数据面删除 —— `POST /dbs/{id}/drop`。
/// 与 query_handler 同一条信任链：并发许可 → Content-Length →
/// 服务间验签（覆盖 method/path/body/epoch/seq）→ 白名单 id 派生
/// 目录 → registry 摘引擎 + 删目录。幂等：不存在返回 dropped:false。
/// 路径只由验签后且白名单校验过的 databaseId 组成，绝不读 body 里的
/// 路径/目标字段。
///
/// T35c：整个 registry.drop_db（内含在途 Arc 排空等待与目录删除）
/// 移到 spawn_blocking——同步阻塞不再卡住 tokio worker 线程、不影响
/// 同 worker 上其它 db 的查询；外加 DROP_TOTAL_TIMEOUT 有界超时，
/// 超时回 503 可重试（控制面 DROP op 会重派，幂等续跑）。阻塞任务
/// 本身不可取消，但 tombstone 已提交，重试立刻走幂等路径返回。
const DROP_TOTAL_TIMEOUT: Duration = Duration::from_secs(15);

async fn drop_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    drop_impl(state, request, crate::paths::ResourceKind::D1, "/dbs/").await
}

/// T37r：queue 删除走独立命名空间端点。`/dbs/{id}/drop` 只触 D1
/// kind、`/queues/{id}/drop` 只触 Queue kind——命名空间分离后跨
/// kind drop 会 tombstone 错误命名空间并泄漏另一边的库与对象
/// （甚至误删同 id 的真 D1 库）。控制面 queue DROP op 的 payload
/// 携带 {"kind":"queue"}，cell-controller 据其路由到本端点。
async fn queue_drop_handler(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> Response {
    drop_impl(
        state,
        request,
        crate::paths::ResourceKind::Queue,
        "/queues/",
    )
    .await
}

async fn drop_impl(
    state: AppState,
    request: axum::extract::Request,
    kind: crate::paths::ResourceKind,
    prefix: &'static str,
) -> Response {
    let started = Instant::now();
    let fail = |outcome: &'static str, status: StatusCode, body: serde_json::Value| {
        crate::metrics::query("drop", outcome, started.elapsed().as_secs_f64());
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return fail(
                "rate_limited",
                StatusCode::TOO_MANY_REQUESTS,
                json!({"success": false, "error": "vrt-sql overloaded"}),
            )
        }
    };
    if let Some(cl) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if cl > state.body_max {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            );
        }
    }
    let db_id = request
        .uri()
        .path()
        .strip_prefix(prefix)
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body = match axum::body::to_bytes(request.into_body(), state.body_max).await {
        Ok(b) => b,
        Err(_) => {
            return fail(
                "protocol_error",
                StatusCode::PAYLOAD_TOO_LARGE,
                json!({"error": "request body too large"}),
            )
        }
    };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return fail(
                "unauthorized",
                StatusCode::UNAUTHORIZED,
                json!({"error": "unauthorized"}),
            );
        }
    }
    let registry = state.registry.clone();
    let outcome = tokio::time::timeout(DROP_TOTAL_TIMEOUT, async move {
        tokio::task::spawn_blocking(move || registry.drop_db(kind, &db_id)).await
    })
    .await;
    match outcome {
        Ok(Ok(Ok(dropped))) => {
            crate::metrics::query("drop", "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"success": true, "dropped": dropped}).to_string(),
                ))
                .unwrap()
        }
        Ok(Ok(Err(e))) => fail(
            "drop_error",
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"success": false, "error": e.wire_message()}),
        ),
        Ok(Err(_join)) => fail(
            "drop_error",
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"success": false, "error": "drop task failed"}),
        ),
        Err(_elapsed) => fail(
            "timeout",
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"success": false, "error": "drop timed out; retry"}),
        ),
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/dbs/{id}/query", post(query_handler))
        .route("/dbs/{id}/execute", post(query_handler))
        .route("/dbs/{id}/drop", post(drop_handler))
        .route("/queues/{id}/drop", post(queue_drop_handler))
        .route("/queues/{id}/send", post(queue_handler))
        .route("/queues/{id}/claim", post(queue_handler))
        .route("/queues/{id}/ack", post(queue_handler))
        .route("/queues/{id}/retry", post(queue_handler))
        // T37b-r6：release 的 handler/op matcher 早已就位（r5），
        // 唯独 route 漏挂 → axum 404 → client Transport ——补上。
        .route("/queues/{id}/release", post(queue_handler))
        .route("/queues/{id}/dead-letter", post(queue_handler))
        .route(
            "/queues/{id}/metrics",
            axum::routing::get(queue_metrics_handler),
        )
        .route("/internal/dbs", axum::routing::get(diag_handler))
        .route("/healthz", axum::routing::get(healthz))
        .route("/readyz", axum::routing::get(healthz))
        .with_state(state)
}

/// ---------- T37：Queues 内部 wire（§8）。与 D1 端点同一
/// ServiceAuth 模型；queueId 入口按 §7 databaseId 规则校验。
/// `/dbs/{id}/drop` 仅删 D1 命名空间；queue 删除走 `/queues/{id}/drop`
///（T37r——cell-controller 依 op payload.kind 选择路径）。

/// 队列请求体上限（T37r 精确化，只作用于 /queues/* 路由——D1
/// 端点上限不受放大影响）：恰好覆盖"最大合法批次 + JSON 开销"。
///   hex(BATCH_MAX_BYTES=288_000)            = 576_000
///   每消息 JSON 骨架 ≤256B×100（id 128 + 键名/引号/数值）
///   + envelope（{"messages":[…]} + 逗号）   =    +256
/// 超过即 413——发生在 JSON 解析之前（content-length 预检 +
/// to_bytes 限长）。
const QUEUE_BODY_MAX: usize =
    crate::queue::BATCH_MAX_BYTES * 2 + crate::queue::BATCH_MAX_MSGS as usize * 256 + 256;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn queue_fail(
    op: &'static str,
    outcome: &'static str,
    started: Instant,
    e: &crate::queue::QueueError,
) -> Response {
    crate::metrics::query(op, outcome, started.elapsed().as_secs_f64());
    Response::builder()
        .status(e.status())
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "success": false,
                "error": e.to_string(),
                "code": e.code(),
                "retryable": e.retryable(),
            })
            .to_string(),
        ))
        .unwrap()
}

fn queue_bad(
    op: &'static str,
    outcome: &'static str,
    started: Instant,
    status: StatusCode,
    msg: &str,
) -> Response {
    crate::metrics::query(op, outcome, started.elapsed().as_secs_f64());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({"success": false, "error": msg, "code": "VRT_QUEUE_INVALID",
                   "retryable": false})
            .to_string(),
        ))
        .unwrap()
}

async fn queue_handler(State(state): State<AppState>, request: axum::extract::Request) -> Response {
    let started = Instant::now();
    let op: &'static str = match request.uri().path().rsplit('/').next() {
        Some("send") => "send",
        Some("claim") => "claim",
        Some("ack") => "ack",
        Some("retry") => "retry",
        Some("release") => "release",
        Some("dead-letter") => "dead-letter",
        _ => "bad_op",
    };
    let label: &'static str = op;
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return queue_bad(
                label,
                "rate_limited",
                started,
                StatusCode::TOO_MANY_REQUESTS,
                "vrt-sql overloaded",
            )
        }
    };
    let _inflight = match state.inflight.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return queue_bad(
                label,
                "unavailable",
                started,
                StatusCode::SERVICE_UNAVAILABLE,
                "vrt-sql draining",
            )
        }
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .is_some_and(|n| n > QUEUE_BODY_MAX.min(state.body_max))
    {
        return queue_bad(
            label,
            "protocol_error",
            started,
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large",
        );
    }
    let id_raw = request
        .uri()
        .path()
        .strip_prefix("/queues/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let body =
        match axum::body::to_bytes(request.into_body(), QUEUE_BODY_MAX.min(state.body_max)).await {
            Ok(b) => b,
            Err(_) => {
                return queue_bad(
                    label,
                    "protocol_error",
                    started,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body too large",
                )
            }
        };
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, &body) {
            return queue_bad(
                label,
                "unauthorized",
                started,
                StatusCode::UNAUTHORIZED,
                "unauthorized",
            );
        }
    }
    let queue = match crate::queue::QueueId::parse(&id_raw) {
        Ok(q) => q,
        Err(e) => return queue_fail(label, "invalid_id", started, &e),
    };
    let backend = crate::queue::SqliteQueueBackend::new(state.registry.clone());
    let now = now_ms();
    let result: Result<serde_json::Value, crate::queue::QueueError> = (|| async {
        match op {
            "send" => {
                let req: crate::queue::SendRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let mut msgs = Vec::with_capacity(req.messages.len());
                for m in req.messages {
                    let body_bytes = hex::decode(&m.body).map_err(|_| {
                        crate::queue::QueueError::InvalidInput("message body must be hex".into())
                    })?;
                    msgs.push(crate::queue::IncomingMsg {
                        id: m.id,
                        body: body_bytes,
                        content_type: m.content_type,
                        delay_secs: m.delay_secs,
                        timestamp_ms: m.timestamp_ms,
                    });
                }
                let r = backend.send(&queue, &msgs, now).await?;
                Ok(json!({
                    "success": true,
                    "enqueued": r.inserted,
                    "duplicates": r.duplicates,
                    "metrics": r.metrics,
                }))
            }
            "claim" => {
                let req: crate::queue::ClaimRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let batch = backend
                    .claim(
                        &queue,
                        &req.holder_id,
                        req.holder_ttl_ms,
                        req.max,
                        req.max_bytes,
                        req.lease_ttl_ms,
                        now,
                    )
                    .await?;
                let msgs: Vec<serde_json::Value> = batch
                    .messages
                    .iter()
                    .map(|m| {
                        json!({
                            "id": m.id,
                            "leaseId": m.lease_id,
                            "attempts": m.attempts,
                            "body": hex::encode(&m.body),
                            "contentType": m.content_type,
                            "timestampMs": m.timestamp_ms,
                        })
                    })
                    .collect();
                Ok(json!({"success": true, "leaseId": batch.lease_id,
                          "dispatchEpoch": batch.dispatch_epoch, "messages": msgs}))
            }
            "ack" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let n = backend.ack(&queue, &req.lease_id, &req.ids, now).await?;
                Ok(json!({"success": true, "acked": n}))
            }
            "retry" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let items: Vec<(String, u32)> = req
                    .items
                    .iter()
                    .map(|i| (i.id.clone(), i.delay_secs))
                    .collect();
                let n = backend.retry(&queue, &req.lease_id, &items, now).await?;
                Ok(json!({"success": true, "retried": n}))
            }
            "release" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let items: Vec<(String, u32)> = req
                    .items
                    .iter()
                    .map(|i| (i.id.clone(), i.delay_secs))
                    .collect();
                let n = backend.release(&queue, &req.lease_id, &items, now).await?;
                Ok(json!({"success": true, "released": n}))
            }
            "dead-letter" => {
                let req: crate::queue::SettleRequest =
                    serde_json::from_slice(&body).map_err(|e| {
                        crate::queue::QueueError::InvalidInput(format!("body json: {e}"))
                    })?;
                let n = backend
                    .dead_letter(
                        &queue,
                        &req.lease_id,
                        &req.ids,
                        req.reason.as_deref().unwrap_or("dead-letter"),
                        now,
                    )
                    .await?;
                Ok(json!({"success": true, "deadLettered": n}))
            }
            _ => Err(crate::queue::QueueError::InvalidInput(format!(
                "unsupported queue op {op:?}"
            ))),
        }
    })()
    .await;
    match result {
        Ok(body) => {
            crate::metrics::query(label, "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        }
        Err(e) => queue_fail(
            label,
            if e.retryable() {
                "unavailable"
            } else {
                "error"
            },
            started,
            &e,
        ),
    }
}

/// `GET /queues/{id}/metrics`——同 ServiceAuth（GET + 空 body）。
async fn queue_metrics_handler(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> Response {
    let started = Instant::now();
    let label = "queue_metrics";
    let _permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return queue_bad(
                label,
                "rate_limited",
                started,
                StatusCode::TOO_MANY_REQUESTS,
                "vrt-sql overloaded",
            )
        }
    };
    let id_raw = request
        .uri()
        .path()
        .strip_prefix("/queues/")
        .and_then(|rest| rest.rsplit_once('/'))
        .map(|(id, _)| id.to_string())
        .unwrap_or_default();
    let method = request.method().clone();
    let path_query = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    if let Some(auth) = &state.auth {
        if !auth.verify(&headers, &method, &path_query, b"") {
            return queue_bad(
                label,
                "unauthorized",
                started,
                StatusCode::UNAUTHORIZED,
                "unauthorized",
            );
        }
    }
    let queue = match crate::queue::QueueId::parse(&id_raw) {
        Ok(q) => q,
        Err(e) => return queue_fail(label, "invalid_id", started, &e),
    };
    let backend = crate::queue::SqliteQueueBackend::new(state.registry.clone());
    match backend.metrics(&queue, now_ms()).await {
        Ok(m) => {
            crate::metrics::query(label, "ok", started.elapsed().as_secs_f64());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::from(
                    json!({"success": true, "metrics": m}).to_string(),
                ))
                .unwrap()
        }
        Err(e) => queue_fail(
            label,
            if e.retryable() {
                "unavailable"
            } else {
                "error"
            },
            started,
            &e,
        ),
    }
}
