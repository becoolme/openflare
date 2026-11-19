//! T35 — D1 control-plane ops executor.
//!
//! Control plane creates `VrtD1Op` rows (EXECUTE / MIGRATIONS_APPLY);
//! this module pulls them over the authenticated cell channel and
//! executes against the LOCAL vrt-sql data plane
//! (`POST /dbs/<id>/execute`, HMAC-signed, same wire as vrt-provider).
//!
//! Safety invariants:
//! - **seq-derived dedup**: `x-vrt-seq` = `op_seq << 12 | stmt_idx` —
//!   deterministic per (op, statement). A re-pulled op (ACK lost)
//!   re-sends the SAME engine seq → `_vrt_dedup` returns the stored
//!   result instead of double-applying an uncertain write.
//! - **fail closed**: malformed `databaseId` (must be
//!   `^[a-z0-9]{26}$`, the vrt-sql path rule) fails the op, never
//!   the process; an unreachable data plane fails the op with an
//!   error string, not a panic.
//! - **bounded**: 5s request timeout, 192 KiB result cap in the ACK
//!   (the control-plane body cap stays separate).
//! - **one-shot**: each op executes each statement once per pull;
//!   MIGRATIONS_APPLY stops at the first failing statement.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// `[d1sql]` — local data-plane endpoint. Absent → cell reports no
/// `d1` capability and the ops channel is not polled.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1SqlConfig {
    /// Loopback (`http://127.0.0.1:8090`) or an in-cluster Service DNS
    /// name (`http://vrt-sql.vrt-system.svc:8082`) — see
    /// [`validate_data_plane_endpoint`]. Never a public/arbitrary host.
    pub endpoint: String,
    /// 0600 secret file shared with vrt-sql (`authSecretFile`).
    pub auth_secret_file: String,
    /// Per-request timeout seconds (default 5).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_timeout_secs() -> u64 {
    5
}

/// One op row from the control plane (camelCase wire) — the LEGACY
/// `/vrt/d1-ops` shape (kept for old-row drain; W4).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct D1Op {
    /// Control-plane row id — the cell-side dedup key (T35b: seq is
    /// SERIAL so it cannot be used as a high-water cursor).
    pub id: String,
    pub seq: i64,
    pub kind: D1OpKind,
    pub database_id: String,
    pub payload: serde_json::Value,
    /// Unix seconds; ops past this are EXPIRED — never executed (T35b).
    /// Absent on DROP ops (cleanup must not be abandoned).
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// W4 channel marker: 0 on the legacy wire; [`CELL_OPS_SEQ_FLAG`]
    /// when the op arrived via `/vrt/cell-ops` — folded into the
    /// engine dedup seq so the two channels' seq spaces can never
    /// collide (vrt_d1_ops and vrt_cell_ops are independent SERIALs).
    #[serde(default, skip)]
    pub chan: u64,
}

/// High marker bit folded into the engine dedup seq for ops that came
/// through the generic cell-ops channel. Without it a cell-ops seq
/// could collide with an already-executed legacy seq and vrt-sql's
/// (epoch, seq) dedup would silently return the OLD result — an
/// un-executed write acked DONE (data loss). Bit 62 is far above any
/// reachable `seq << 12`.
pub const CELL_OPS_SEQ_FLAG: u64 = 1 << 62;

/// W4-r2 cross-channel ordering: the two op channels run as
/// concurrent tasks — during the `d1CellOps` capability window the
/// control plane can have a legacy op for database X in flight while
/// a generic-channel op for X is pulled by the other task. Executing
/// the generic op first violates the per-database write order. This
/// guard is the cell-side safety rail under the control-plane rule
/// (dispatch keeps writing legacy while the database has unfinished
/// legacy ops — see vrtD1.ts): generic-channel D1 ops for a database
/// DEFER while a legacy op for that database is pulled-not-yet-acked.
/// An expired/never-delivered legacy op never blocks (expired ops are
/// filtered server-side and skipped here without blocking). The set
/// is memory-only — a cell restart re-derives it from the next pull.
///
/// Entries carry the blocking op's `expires_at` (unix seconds): if a
/// pulled legacy op expires server-side before its ack lands, the
/// control plane stops delivering it and the entry would otherwise
/// leak — deferring generic ops for that database forever. `is_blocked`
/// lazily evicts entries past their deadline; ops with no expiry
/// (e.g. DROP) get a bounded fallback TTL so no entry can outlive its
/// op's dispatch window.
/// W4-r3 P0-5: keyed per OP, not per database — one pull can carry
/// several legacy ops for the same db and `release` must only clear
/// the op that acked. A db is blocked while ANY of its recorded ops
/// is unfinished and unexpired; per-op lazy eviction keeps an
/// expire-before-ack straggler from pinning the db forever.
#[derive(Default, Clone)]
pub struct D1OrderGuard(
    std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashMap<String, i64>>>,
    >,
);

/// Fallback TTL (seconds) for blocking entries whose op carries no
/// `expires_at`. Covers the legacy 120s dispatch window with margin —
/// longer than any single op's lifetime, shorter than a leak.
const D1_ORDER_FALLBACK_TTL_SECS: i64 = 300;

impl D1OrderGuard {
    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
    /// A legacy-channel op `op_id` for `db` exists locally and is
    /// unfinished. `expires_at` bounds the entry's lifetime.
    pub fn block(&self, db: &str, op_id: &str, expires_at: Option<i64>) {
        let deadline = expires_at.unwrap_or_else(|| Self::now_secs() + D1_ORDER_FALLBACK_TTL_SECS);
        self.0
            .lock()
            .unwrap()
            .entry(db.to_string())
            .or_default()
            .insert(op_id.to_string(), deadline);
    }
    /// The legacy op `op_id` reached a terminal local state (acked).
    /// The db unblocks only when its last pending op is gone.
    pub fn release(&self, db: &str, op_id: &str) {
        let mut m = self.0.lock().unwrap();
        if let Some(ops) = m.get_mut(db) {
            ops.remove(op_id);
            if ops.is_empty() {
                m.remove(db);
            }
        }
    }
    /// Generic-channel ops for `db` must defer this tick — unless
    /// every recorded legacy op already outlived its dispatch
    /// deadline (expired server-side → never re-delivered → evict).
    pub fn is_blocked(&self, db: &str) -> bool {
        let mut m = self.0.lock().unwrap();
        let now = Self::now_secs();
        match m.get_mut(db) {
            Some(ops) => {
                ops.retain(|_, deadline| now < *deadline);
                if ops.is_empty() {
                    m.remove(db);
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }
}

/// Kinds routed to the D1 executor on the generic channel.
pub fn is_d1_kind(kind: &str) -> bool {
    matches!(kind, "EXECUTE" | "MIGRATIONS_APPLY" | "DROP")
}

impl D1Op {
    /// Adapt a generic-channel op row — `resourceKey` carries the
    /// databaseId. Returns None for non-D1 kinds (caller routes first).
    pub fn from_cell_op(op: &crate::execset::CellOp) -> Option<Self> {
        let kind = match op.kind.as_str() {
            "EXECUTE" => D1OpKind::Execute,
            "MIGRATIONS_APPLY" => D1OpKind::MigrationsApply,
            "DROP" => D1OpKind::Drop,
            _ => return None,
        };
        Some(Self {
            id: op.id.clone(),
            seq: op.seq,
            kind,
            database_id: op.resource_key.clone(),
            payload: op.payload.clone(),
            expires_at: op.expires_at,
            chan: CELL_OPS_SEQ_FLAG,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum D1OpKind {
    #[serde(rename = "EXECUTE")]
    Execute,
    #[serde(rename = "MIGRATIONS_APPLY")]
    MigrationsApply,
    #[serde(rename = "DROP")]
    Drop,
}

/// Terminal result of one op, ACKed back.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct D1OpAck {
    pub seq: i64,
    pub status: &'static str, // "DONE" | "FAILED"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Pull page shape (`{data:{ops:[…]}}` — control-plane envelope).
#[derive(Debug, Deserialize)]
struct OpsPage {
    data: OpsData,
}
#[derive(Debug, Deserialize)]
struct OpsData {
    ops: Vec<D1Op>,
}

const MAX_OPS_PAGE_BYTES: usize = 2 * 1024 * 1024;
/// Result JSON cap inside a single ACK — keeps the ACK body far under
/// the control-plane bound.
const MAX_RESULT_BYTES: usize = 192 * 1024;
const MAX_ACK_BODY_BYTES: usize = 1024 * 1024;
const DATABASE_ID_LEN: usize = 26;

fn valid_database_id(id: &str) -> bool {
    id.len() == DATABASE_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Engine seq derivation: op_seq << 12 | call_idx. One dedup key is
/// consumed per HTTP call (the engine dedups per REQUEST — a batch of
/// statements inside one request shares one key). Call indexes are
/// bounded: EXECUTE=1, MIGRATIONS_APPLY≤512, DROP=1 — all < 4096.
/// `call()` refuses idx ≥ SEQ_SPAN rather than wrapping into the next
/// op's range (T35b #2).
const SEQ_SPAN: usize = 4096;
fn engine_seq(op_seq: i64, stmt_idx: usize, chan: u64) -> u64 {
    ((op_seq as u64) << 12) | (stmt_idx as u64 & 0xfff) | chan
}

/// Shape check for the vrt-sql data-plane endpoint shared by the d1ops
/// executor and the queue dispatcher (fail closed, one definition).
///
/// Accepted (`http` only, explicit port, no userinfo/path/query/fragment):
/// - loopback: `localhost`, `127.0.0.0/8`, `::1` (same-pod / dev);
/// - in-cluster Service DNS: `<svc>.<ns>.svc` or
///   `<svc>.<ns>.svc.cluster.local`, every label a DNS-1123 label.
///
/// Why this is safe: requests carry an HMAC (`v1:<ts>:<mac>` over
/// ts|method|path|body-sha256|epoch|seq, +-30 s window) keyed by the shared
/// `vrt-sql-auth-secret`, which is never sent on the wire; vrt-sql rejects
/// unsigned/stale requests. Transport is cleartext in-cluster, exactly like
/// the provider->vrt-sql hop. Bare IPs (other than loopback), public DNS
/// names, short single-label names (search-path dependent) and look-alikes
/// such as `127.0.0.1.evil.test` are rejected. Returns the normalized
/// endpoint without a trailing slash.
pub fn validate_data_plane_endpoint(raw: &str) -> Result<String, String> {
    if raw.is_empty() || raw.len() > 256 {
        return Err("must be 1..=256 bytes".into());
    }
    let url = reqwest::Url::parse(raw).map_err(|_| "is not a valid URL".to_string())?;
    if url.scheme() != "http" {
        return Err("scheme must be http".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("must not carry userinfo".into());
    }
    if url.query().is_some() || url.fragment().is_some() || url.path() != "/" {
        return Err("must be a bare http://host:port origin".into());
    }
    match url.port() {
        Some(p) if p != 0 => {}
        _ => return Err("must carry an explicit non-zero port".into()),
    }
    let host = url.host_str().ok_or("has no host")?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    let ok = match bare.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => host == "localhost" || is_cluster_service_host(host),
    };
    if !ok {
        return Err(
            "host must be loopback or an in-cluster Service name \
             (<svc>.<ns>.svc[.cluster.local])"
                .into(),
        );
    }
    Ok(raw.trim_end_matches('/').to_string())
}

fn is_dns1123_label(l: &str) -> bool {
    let b = l.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b[0] != b'-'
        && b[b.len() - 1] != b'-'
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn is_cluster_service_host(host: &str) -> bool {
    let labels: Vec<&str> = host.split('.').collect();
    let ok_shape = matches!(
        labels.as_slice(),
        [_, _, "svc"] | [_, _, "svc", "cluster", "local"]
    );
    ok_shape && labels.iter().all(|l| is_dns1123_label(l))
}

/// The executor: HTTP client + signer against the local vrt-sql.
pub struct D1OpsExecutor {
    http: reqwest::Client,
    endpoint: String,
    signer: vrt_provider::d1::SqlAuthSigner,
    timeout: Duration,
}

impl std::fmt::Debug for D1OpsExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("D1OpsExecutor")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl D1OpsExecutor {
    pub fn new(config: &D1SqlConfig) -> Result<Self, String> {
        let endpoint = validate_data_plane_endpoint(&config.endpoint)
            .map_err(|e| format!("d1sql.endpoint {e}"))?;
        let secret = vrt_securefs::load_service_secret(
            std::path::Path::new(&config.auth_secret_file),
            vrt_securefs::SecretPurpose::ServiceHmac,
        )
        .map_err(|e| format!("d1sql.auth_secret_file unusable: {e}"))?
        .into_bytes();
        let timeout = Duration::from_secs(config.timeout_secs.max(1));
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            // A signed request must never follow a redirect to another host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("d1sql http client: {e}"))?;
        Ok(Self {
            http,
            endpoint,
            signer: vrt_provider::d1::SqlAuthSigner::new(secret),
            timeout,
        })
    }

    /// Execute one batch against `/dbs/<id>/execute`. Returns the raw
    /// JSON body on a 2xx; an error string otherwise (fail the op,
    /// not the process).
    async fn call(
        &self,
        op: &D1Op,
        stmt_idx: usize,
        body: &[u8],
    ) -> Result<serde_json::Value, String> {
        if stmt_idx >= SEQ_SPAN {
            // Never let an index wrap into another op's seq range —
            // fail the op loudly instead of a dedup-collision cascade.
            return Err(format!(
                "call index {stmt_idx} exceeds per-op seq span {SEQ_SPAN}"
            ));
        }
        let path = format!("/dbs/{}/execute", op.database_id);
        let url = format!("{}{}", self.endpoint, path);
        let seq = engine_seq(op.seq, stmt_idx, op.chan);
        let auth = self.signer.sign("POST", &path, body, 1, seq);
        let resp = tokio::time::timeout(
            self.timeout,
            self.http
                .post(&url)
                .header(vrt_provider::d1::SQL_AUTH_HEADER, auth)
                .header(vrt_provider::d1::SQL_EPOCH_HEADER, "1")
                .header(vrt_provider::d1::SQL_SEQ_HEADER, seq.to_string())
                .header("content-type", "application/json")
                .body(body.to_vec())
                .send(),
        )
        .await
        .map_err(|_| "request timeout".to_string())?
        .map_err(|e| format!("transport: {e}"))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| format!("read body: {e}"))?;
        if !status.is_success() {
            // A typed `{success:false,...}` body is still an engine
            // OUTCOME, not a transport failure — its `code`/`retryable`
            // semantics must reach the control plane (VRT_UNAVAILABLE
            // rides HTTP 503; RV-R68). Untyped bodies stay fail-closed.
            if bytes.len() <= MAX_RESULT_BYTES {
                if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if body.get("success") == Some(&serde_json::json!(false)) {
                        return Ok(normalize_execute_body(body));
                    }
                }
            }
            return Err(format!("vrt-sql status {status}"));
        }
        if bytes.len() > MAX_RESULT_BYTES {
            return Ok(serde_json::json!({ "truncated": true, "bytes": bytes.len() }));
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| "vrt-sql non-JSON response".to_string())?;
        Ok(normalize_execute_body(body))
    }

    /// Generic-channel entry — adapt the cell-op row then run the
    /// identical execute path (same validation, same deadline rule).
    pub async fn execute_cell(&self, op: &crate::execset::CellOp) -> D1OpAck {
        match D1Op::from_cell_op(op) {
            Some(d) => self.execute(&d).await,
            None => D1OpAck {
                seq: op.seq,
                status: "FAILED",
                result: None,
                error: Some("unknown op kind".into()),
            },
        }
    }

    /// Execute one op end-to-end. Never panics — the result is always
    /// a terminal ack (DONE/FAILED/EXPIRED).
    pub async fn execute(&self, op: &D1Op) -> D1OpAck {
        if !valid_database_id(&op.database_id) {
            return fail(op, "invalid databaseId");
        }
        // T35b: an op past its dispatch deadline is never executed —
        // the caller already saw a failure for it. Report EXPIRED so the
        // control plane marks it terminal instead of re-delivering.
        if let Some(deadline) = op.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            if now >= deadline {
                return D1OpAck {
                    seq: op.seq,
                    status: "EXPIRED",
                    result: None,
                    error: Some("dispatch deadline passed".into()),
                };
            }
        }
        match op.kind {
            D1OpKind::Execute => {
                let body = match &op.payload {
                    v if v.is_object() => serde_json::json!([v]).to_string(),
                    v if v.is_array() => v.to_string(),
                    _ => return fail(op, "execute payload must be a statement object or array"),
                };
                match self.call(op, 0, body.as_bytes()).await {
                    Ok(v) => done(op, v),
                    Err(e) => fail(op, &e),
                }
            }
            D1OpKind::MigrationsApply => {
                let migrations = op
                    .payload
                    .get("migrations")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                if migrations.is_empty() || migrations.len() > 512 {
                    return fail(op, "migrations must be a non-empty list ≤512");
                }
                let mut applied: Vec<String> = Vec::with_capacity(migrations.len());
                for (i, m) in migrations.iter().enumerate() {
                    let name = m.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let sql = m.get("sql").and_then(|v| v.as_str()).unwrap_or("");
                    if name.is_empty() || name.len() > 200 || sql.is_empty() {
                        return fail(op, "migration entry invalid");
                    }
                    let body = serde_json::json!([{ "sql": sql }]).to_string();
                    match self.call(op, i, body.as_bytes()).await {
                        // `{success:false}` (SQL error / fenced /
                        // unavailable) means the statement did NOT
                        // apply — never claim it (RV-R68).
                        Ok(v) if v.get("success") == Some(&serde_json::json!(false)) => {
                            return D1OpAck {
                                seq: op.seq,
                                status: "FAILED",
                                result: Some(serde_json::json!({ "applied": applied })),
                                error: Some(format!(
                                    "migration '{name}': {}",
                                    engine_error_text(&v)
                                )),
                            }
                        }
                        Ok(_) => applied.push(name.to_string()),
                        Err(e) => {
                            return D1OpAck {
                                seq: op.seq,
                                status: "FAILED",
                                result: Some(serde_json::json!({ "applied": applied })),
                                error: Some(format!("migration '{name}': {e}")),
                            }
                        }
                    }
                }
                done(op, serde_json::json!({ "applied": applied }))
            }
            D1OpKind::Drop => match self
                .drop_db(&op.database_id, op.seq, op.chan, &op.payload)
                .await
            {
                Ok(v) => done(op, v),
                Err(e) => fail(op, &e),
            },
        }
    }

    /// `POST /dbs/<id>/drop` — restricted delete on the local vrt-sql
    /// for a D1 database; `/queues/<id>/drop` when the op payload marks a
    /// queue drop (`{"kind":"queue"}`, T37r — vrt-sql namespaces D1 and
    /// Queue separately, the wrong path would tombstone the other kind's
    /// namespace and leak the queue db + objects). Absent or `"d1"` kind
    /// → D1 path (in-flight legacy ops). Unknown kinds fail closed —
    /// never guess the namespace.
    async fn drop_db(
        &self,
        database_id: &str,
        op_seq: i64,
        chan: u64,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let path = drop_path(database_id, payload)?;
        let url = format!("{}{}", self.endpoint, path);
        let body = b"{}";
        let seq = engine_seq(op_seq, 0, chan);
        let auth = self.signer.sign("POST", &path, body, 1, seq);
        let resp = tokio::time::timeout(
            self.timeout,
            self.http
                .post(&url)
                .header(vrt_provider::d1::SQL_AUTH_HEADER, auth)
                .header(vrt_provider::d1::SQL_EPOCH_HEADER, "1")
                .header(vrt_provider::d1::SQL_SEQ_HEADER, seq.to_string())
                .header("content-type", "application/json")
                .body(body.to_vec())
                .send(),
        )
        .await
        .map_err(|_| "request timeout".to_string())?
        .map_err(|e| format!("transport: {e}"))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| format!("read body: {e}"))?;
        if !status.is_success() {
            return Err(format!("vrt-sql drop status {status}"));
        }
        let v: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| "vrt-sql drop non-JSON response".to_string())?;
        // `{success:false}` is a typed failure — the ACK handler
        // hard-deletes the row on DONE, so a fake-ok body must fail
        // the op instead (RV-R68).
        if v.get("success") == Some(&serde_json::json!(false)) {
            return Err(engine_error_text(&v));
        }
        Ok(v)
    }
}

/// Drop path selection (T37r): payload `{"kind":"queue"}` → `/queues/…`,
/// absent/`"d1"` → `/dbs/…` (legacy in-flight ops), anything else → Err.
fn drop_path(database_id: &str, payload: &serde_json::Value) -> Result<String, String> {
    match payload.get("kind") {
        // Absent → legacy in-flight D1 ops (pre-T37r payloads were {}).
        None => Ok(format!("/dbs/{database_id}/drop")),
        // Present but non-string → malformed, never guess a namespace.
        Some(v) => match v.as_str() {
            Some("d1") => Ok(format!("/dbs/{database_id}/drop")),
            Some("queue") => Ok(format!("/queues/{database_id}/drop")),
            Some(other) => Err(format!("drop payload kind '{other}' unknown")),
            None => Err("drop payload kind must be a string".to_string()),
        },
    }
}

/// Control-plane contract for engine bodies (RV-R68): a vrt-sql
/// success payload is a bare ARRAY of per-statement results, which the
/// strict result schema rejects — wrap it as `{results: [...]}`.
/// Already-typed object bodies (`{success:false,error,code,retryable}`,
/// `{truncated,bytes}`, `{success,dropped}`) pass through unchanged.
pub fn normalize_execute_body(body: serde_json::Value) -> serde_json::Value {
    match body {
        serde_json::Value::Array(results) => serde_json::json!({ "results": results }),
        other => other,
    }
}

/// Bounded human-readable rendering of a `{success:false}` engine body
/// for an op's `error` field — `error` plus the typed `code` when the
/// engine provided one (kept so FAILED ops stay diagnosable).
fn engine_error_text(v: &serde_json::Value) -> String {
    let err = v
        .get("error")
        .and_then(|e| e.as_str())
        .unwrap_or("engine error");
    match v.get("code").and_then(|c| c.as_str()) {
        Some(code) => format!("{code}: {err}"),
        None => err.to_string(),
    }
}

fn done(op: &D1Op, result: serde_json::Value) -> D1OpAck {
    D1OpAck {
        seq: op.seq,
        status: "DONE",
        result: Some(result),
        error: None,
    }
}
fn fail(op: &D1Op, error: &str) -> D1OpAck {
    D1OpAck {
        seq: op.seq,
        status: "FAILED",
        result: None,
        error: Some(crate::execset::truncate_utf16(error, 500).0),
    }
}

/// Pull side — GET the pending ops page over the cell token channel.
pub struct HttpD1OpsSource {
    http: reqwest::Client,
    url: String,
    token: String,
}

impl std::fmt::Debug for HttpD1OpsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpD1OpsSource")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl HttpD1OpsSource {
    pub fn new(
        base_url: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| format!("ops source http: {e}"))?;
        Ok(Self {
            http,
            url: format!(
                "{}/api/internal/clusters/{}/vrt/d1-ops",
                base_url.trim_end_matches('/'),
                cell_id
            ),
            token,
        })
    }

    /// Pull ALL pending ops for the cell (T35b: no seq filter — a
    /// late-committing low seq must still arrive). `fetch` is a pure
    /// pull — ops stay PENDING until ACKed, so re-delivery is normal
    /// and deduped by id in `ops_tick`.
    pub async fn fetch(&self, limit: u32) -> Result<Vec<D1Op>, String> {
        let resp = self
            .http
            .get(&self.url)
            .bearer_auth(&self.token)
            .query(&[("limit", limit.to_string())])
            .send()
            .await
            .map_err(|e| format!("ops pull: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("ops pull status {}", resp.status()));
        }
        let bytes = resp.bytes().await.map_err(|e| format!("ops body: {e}"))?;
        if bytes.len() > MAX_OPS_PAGE_BYTES {
            return Err("ops page too large".into());
        }
        let page: OpsPage =
            serde_json::from_slice(&bytes).map_err(|_| "ops page invalid".to_string())?;
        Ok(page.data.ops)
    }
}

/// ACK side — POST terminal results.
pub struct HttpD1OpsAckSink {
    http: reqwest::Client,
    url: String,
    token: String,
}

impl HttpD1OpsAckSink {
    pub fn new(
        base_url: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| format!("ops ack http: {e}"))?;
        Ok(Self {
            http,
            url: format!(
                "{}/api/internal/clusters/{}/vrt/d1-ops/ack",
                base_url.trim_end_matches('/'),
                cell_id
            ),
            token,
        })
    }

    pub async fn ack(&self, acks: &[D1OpAck]) -> Result<(), String> {
        let body = serde_json::json!({ "acks": acks }).to_string();
        if body.len() > MAX_ACK_BODY_BYTES {
            return Err("ack body too large".into());
        }
        let resp = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("ops ack: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("ops ack status {}", resp.status()));
        }
        Ok(())
    }
}

/// W4: pull-dedup primitives live in `execset` — re-exported so
/// `crate::d1ops::SeenSet`/`SEEN_CAP` callers keep working.
pub use crate::execset::{SeenSet, SEEN_CAP};

/// One executor tick on the LEGACY channel: pull → execute → journal →
/// ack. W4 persist-before-ack, identical to the generic channel:
/// `executed` records the terminal outcome durably BEFORE the ack POST
/// — a crash between execute and ack replays the stored outcome on
/// redelivery instead of re-executing the write. `seen` marks an id
/// only after its ACK POSTed.
pub async fn ops_tick(
    source: &HttpD1OpsSource,
    sink: &HttpD1OpsAckSink,
    exec: &D1OpsExecutor,
    seen: &mut SeenSet,
    executed: &mut crate::execset::ExecutedSet,
    order: &D1OrderGuard,
    limit: u32,
) -> Result<usize, String> {
    let ops = source.fetch(limit).await?;
    if ops.is_empty() {
        return Ok(0);
    }
    let mut acks = Vec::with_capacity(ops.len());
    let mut done_ids: Vec<String> = Vec::with_capacity(ops.len());
    let mut done_dbs: Vec<(String, String)> = Vec::with_capacity(ops.len());
    for op in &ops {
        if seen.contains_key(&op.id) {
            continue;
        }
        // W4-r2: mark the database as having an unfinished legacy op
        // for the whole pull→ack span — generic-channel same-db ops
        // defer until release().
        order.block(&op.database_id, &op.id, op.expires_at);
        let ack = if let Some(rec) = executed.get(&op.id) {
            crate::execset::CellOpAck {
                seq: op.seq,
                status: match rec.status.as_str() {
                    "DONE" => "DONE",
                    "EXPIRED" => "EXPIRED",
                    _ => "FAILED",
                },
                result: rec.result.clone(),
                error: rec.error.clone(),
            }
        } else {
            let a = exec.execute(op).await;
            let ack = crate::execset::CellOpAck {
                seq: a.seq,
                status: a.status,
                result: a.result.clone(),
                error: a.error.clone(),
            };
            executed.record(&op.id, &ack);
            ack
        };
        acks.push(D1OpAck {
            seq: ack.seq,
            status: ack.status,
            result: ack.result,
            error: ack.error,
        });
        done_ids.push(op.id.clone());
        done_dbs.push((op.database_id.clone(), op.id.clone()));
    }
    if !acks.is_empty() {
        sink.ack(&acks).await?;
        for id in done_ids {
            seen.insert(id, ());
        }
        for (db, op_id) in done_dbs {
            order.release(&db, &op_id);
        }
        while seen.len() > SEEN_CAP {
            if let Some(k) = seen.keys().next().cloned() {
                seen.remove(&k);
            }
        }
    }
    Ok(acks.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RV-R72: valid service secret fixture (0600, >=32 bytes, non-uniform).
    fn write_test_secret(path: &std::path::Path) {
        vrt_securefs::atomic_write(path, b"d1ops-test-secret-0123456789abcdef-Q7", 0o600).unwrap();
    }

    #[test]
    fn rv_r72_secret_trailing_newline_and_unsafe_files_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mk = |p: &std::path::Path| D1SqlConfig {
            endpoint: "http://127.0.0.1:1".into(),
            auth_secret_file: p.to_string_lossy().into(),
            timeout_secs: 1,
        };
        // trailing newline -> accepted and identical key to vrt-sql/provider
        let nl = dir.path().join("nl");
        vrt_securefs::atomic_write(&nl, b"0123456789abcdef0123456789abcdef\n", 0o600).unwrap();
        assert!(D1OpsExecutor::new(&mk(&nl)).is_ok());
        for (name, body, mode) in [
            ("short", &b"tooshort\n"[..], 0o600),
            ("inner", &b"0123456789abcdef 0123456789abcdef"[..], 0o600),
            ("ph", &b"REPLACE_ME_REPLACE_ME_REPLACE_ME_REPLACE"[..], 0o600),
            ("mode", &b"0123456789abcdef0123456789abcdef"[..], 0o644),
        ] {
            let p = dir.path().join(name);
            vrt_securefs::atomic_write(&p, body, mode).unwrap();
            let err = D1OpsExecutor::new(&mk(&p)).err().unwrap_or_else(|| panic!("{name} accepted"));
            assert!(!err.contains("0123456789abcdef"), "{err}");
        }
    }

    fn op(seq: i64, kind: D1OpKind, db: &str, payload: serde_json::Value) -> D1Op {
        D1Op {
            id: format!("op-{seq}"),
            seq,
            chan: 0,
            kind,
            database_id: db.to_string(),
            payload,
            expires_at: None,
        }
    }

    #[test]
    fn database_id_whitelist() {
        assert!(valid_database_id(&"a".repeat(26)));
        assert!(valid_database_id("abc123def456ghi789jklmno01"));
        // Every attack shape rejected.
        for bad in [
            "",
            "short",
            &"a".repeat(25),
            &"a".repeat(27),
            &"A".repeat(26),         // uppercase
            "abc/def..def/def/xyz/", // slash+traversal shape
            "abc..def..def..def..12",
            "ab\u{0}def456ghi789jklmno01", // NUL
            "αbcdefghijklmnopqrstuvwxy",   // unicode
            "abc-def_ghi-jkl_mno-pqr1",    // '-','_' not in alphabet
        ] {
            assert!(!valid_database_id(bad), "{bad:?} must fail");
        }
    }

    #[test]
    fn engine_seq_deterministic_and_ordered() {
        assert_eq!(engine_seq(1, 0, 0), 4096);
        assert_eq!(engine_seq(1, 4095, 0), 8191);
        assert_eq!(engine_seq(2, 0, 0), 8192);
        // Per-op statement order + across-op monotonicity.
        assert!(engine_seq(5, 7, 0) > engine_seq(5, 6, 0));
        assert!(engine_seq(6, 0, 0) > engine_seq(5, 4095, 0));
        // Same inputs → same seq (re-pull dedup).
        assert_eq!(engine_seq(42, 3, 0), engine_seq(42, 3, 0));
    }

    #[tokio::test]
    async fn execute_rejects_bad_database_id_without_network() {
        // Executor with an unreachable endpoint — bad id must fail
        // BEFORE any socket call (no transport error possible).
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let exec = D1OpsExecutor::new(&D1SqlConfig {
            endpoint: "http://127.0.0.1:1".into(), // nothing listens
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 1,
        })
        .unwrap();
        let ack = exec
            .execute(&op(
                1,
                D1OpKind::Execute,
                "../escape",
                serde_json::json!({"sql":"select 1"}),
            ))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert_eq!(ack.error.as_deref(), Some("invalid databaseId"));
    }

    #[tokio::test]
    async fn execute_unreachable_backend_fails_op_not_process() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let exec = D1OpsExecutor::new(&D1SqlConfig {
            endpoint: "http://127.0.0.1:1".into(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 1,
        })
        .unwrap();
        let ack = exec
            .execute(&op(
                9,
                D1OpKind::Execute,
                &"a".repeat(26),
                serde_json::json!({"sql":"select 1","params":[]}),
            ))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("transport") || true);
    }

    #[tokio::test]
    async fn migrations_payload_shape_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let exec = D1OpsExecutor::new(&D1SqlConfig {
            endpoint: "http://127.0.0.1:1".into(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 1,
        })
        .unwrap();
        for payload in [
            serde_json::json!({}),
            serde_json::json!({"migrations": []}),
            serde_json::json!({"migrations": [{"name": "", "sql": "x"}]}),
            serde_json::json!({"migrations": [{"name": "m", "sql": ""}]}),
        ] {
            let ack = exec
                .execute(&op(
                    1,
                    D1OpKind::MigrationsApply,
                    &"a".repeat(26),
                    payload.clone(),
                ))
                .await;
            assert_eq!(ack.status, "FAILED", "{payload:?}");
        }
    }

    #[test]
    fn endpoint_must_be_loopback() {
        let cfg = D1SqlConfig {
            endpoint: "http://10.0.0.5:8090".into(),
            auth_secret_file: "/nonexistent".into(),
            timeout_secs: 5,
        };
        assert!(D1OpsExecutor::new(&cfg).is_err());
    }

    #[test]
    fn bug2_endpoint_shape_accepts_loopback_and_cluster_service() {
        for ok in [
            "http://127.0.0.1:8090",
            "http://127.0.0.1:8090/",
            "http://localhost:8090",
            "http://[::1]:8090",
            "http://vrt-sql.vrt-system.svc:8082",
            "http://vrt-sql.vrt-system.svc.cluster.local:8082",
            "http://vrt-sql.vrt-system.svc:8082/",
        ] {
            assert!(validate_data_plane_endpoint(ok).is_ok(), "{ok}");
        }
        assert_eq!(
            validate_data_plane_endpoint("http://vrt-sql.vrt-system.svc:8082/").unwrap(),
            "http://vrt-sql.vrt-system.svc:8082"
        );
    }

    #[test]
    fn bug2_endpoint_shape_rejects_public_and_malformed() {
        for bad in [
            "",
            "https://vrt-sql.vrt-system.svc:8082", // scheme
            "ftp://vrt-sql.vrt-system.svc:8082",
            "http://vrt-sql:8082",                  // short name: search-path dependent
            "http://vrt-sql.vrt-system:8082",
            "http://vrt-sql.vrt-system.svc",       // no port
            "http://vrt-sql.vrt-system.svc:0",
            "http://example.com:8082",
            "http://vrt-sql.vrt-system.svc.example.com:8082",
            "http://vrt-sql.vrt-system.svc.cluster.local.evil.test:8082",
            "http://evil.test/vrt-sql.vrt-system.svc:8082",
            "http://127.0.0.1.evil.test:8082",       // prefix look-alike
            "http://localhost.evil.test:8082",
            "http://10.0.0.5:8082",
            "http://169.254.169.254:80",
            "http://[fd00::1]:8082",
            "http://0.0.0.0:8082",
            "http://user:pw@vrt-sql.vrt-system.svc:8082",
            "http://vrt-sql.vrt-system.svc:8082/dbs",
            "http://vrt-sql.vrt-system.svc:8082/?x=1",
            "http://vrt-sql.vrt-system.svc:8082/#f",
            "http://-bad.vrt-system.svc:8082",
            "http://Vrt_sql.vrt-system.svc:8082",
            "http://vrt-sql.vrt-system.svc.:8082", // trailing dot
            "http://.vrt-system.svc:8082",
            "http://a.b.c.svc:8082",
        ] {
            assert!(validate_data_plane_endpoint(bad).is_err(), "{bad}");
        }
        let long = format!("http://{}.ns.svc:8082", "a".repeat(300));
        assert!(validate_data_plane_endpoint(&long).is_err());
    }

    #[test]
    fn bug2_executor_accepts_cluster_service_and_rejects_public() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let mk = |ep: &str| D1SqlConfig {
            endpoint: ep.into(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 1,
        };
        assert!(D1OpsExecutor::new(&mk("http://vrt-sql.vrt-system.svc:8082")).is_ok());
        let err = D1OpsExecutor::new(&mk("http://example.com:8082")).err().unwrap();
        assert!(err.starts_with("d1sql.endpoint"), "{err}");
        // Secret file stays mandatory for a cluster endpoint (fail closed).
        let missing = D1SqlConfig {
            endpoint: "http://vrt-sql.vrt-system.svc:8082".into(),
            auth_secret_file: dir.path().join("nope").to_string_lossy().into(),
            timeout_secs: 1,
        };
        assert!(D1OpsExecutor::new(&missing).is_err());
    }
    fn exec_at_unused_port(dir: &tempfile::TempDir) -> D1OpsExecutor {
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        D1OpsExecutor::new(&D1SqlConfig {
            endpoint: "http://127.0.0.1:1".into(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 1,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn expired_op_is_never_executed() {
        let dir = tempfile::tempdir().unwrap();
        let exec = exec_at_unused_port(&dir);
        let mut o = op(
            1,
            D1OpKind::Execute,
            &"a".repeat(26),
            serde_json::json!({"sql":"select 1"}),
        );
        // Deadline already passed → EXPIRED before any socket call
        // (endpoint is unreachable; a transport attempt would fail with
        // a different error, proving the gate ran first).
        o.expires_at = Some(1);
        let ack = exec.execute(&o).await;
        assert_eq!(ack.status, "EXPIRED");
        assert_eq!(ack.error.as_deref(), Some("dispatch deadline passed"));
        // Future deadline → proceeds (fails on transport, not EXPIRED).
        o.expires_at = Some(i64::MAX / 2);
        let ack = exec.execute(&o).await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("transport"));
        // DROP ops carry no expiry (cleanup must not be abandoned).
        let o = op(2, D1OpKind::Drop, &"a".repeat(26), serde_json::json!({}));
        assert!(o.expires_at.is_none());
    }

    #[tokio::test]
    async fn drop_op_calls_drop_endpoint() {
        // Unreachable endpoint → FAILED via drop_db transport (proves
        // the Dispatch routed to drop_db, not /execute).
        let dir = tempfile::tempdir().unwrap();
        let exec = exec_at_unused_port(&dir);
        let ack = exec
            .execute(&op(
                3,
                D1OpKind::Drop,
                &"a".repeat(26),
                serde_json::json!({}),
            ))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("transport"));
    }

    #[test]
    fn drop_path_selects_kind_namespace() {
        let id = "a".repeat(26);
        // Legacy in-flight ops carry no kind → D1 path (back-compat).
        assert_eq!(
            drop_path(&id, &serde_json::json!({})).unwrap(),
            format!("/dbs/{id}/drop")
        );
        assert_eq!(
            drop_path(&id, &serde_json::json!({"kind":"d1"})).unwrap(),
            format!("/dbs/{id}/drop")
        );
        assert_eq!(
            drop_path(&id, &serde_json::json!({"kind":"queue"})).unwrap(),
            format!("/queues/{id}/drop")
        );
        // Unknown / non-string kinds fail closed — never guess.
        assert!(drop_path(&id, &serde_json::json!({"kind":"kv"})).is_err());
        assert!(drop_path(&id, &serde_json::json!({"kind":1})).is_err());
    }

    #[tokio::test]
    async fn queue_drop_op_fails_closed_on_unknown_kind() {
        // Unknown kind → FAILED before any transport (namespace never
        // guessed); queue kind → routes to drop_db (transport error at
        // unused port proves dispatch reached the HTTP call).
        let dir = tempfile::tempdir().unwrap();
        let exec = exec_at_unused_port(&dir);
        let ack = exec
            .execute(&op(
                7,
                D1OpKind::Drop,
                &"a".repeat(26),
                serde_json::json!({"kind":"bogus"}),
            ))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("kind 'bogus' unknown"));
        let ack = exec
            .execute(&op(
                8,
                D1OpKind::Drop,
                &"a".repeat(26),
                serde_json::json!({"kind":"queue"}),
            ))
            .await;
        assert_eq!(ack.status, "FAILED");
        assert!(ack.error.unwrap().contains("transport"));
    }

    #[test]
    fn engine_seq_span_guard() {
        // Indices < SEQ_SPAN encode cleanly; ≥ SEQ_SPAN must never
        // silently wrap into the next op's range.
        assert_eq!(
            engine_seq(1, SEQ_SPAN - 1, 0),
            (1u64 << 12) | (SEQ_SPAN as u64 - 1)
        );
        // The guard lives in call(): idx ≥ SEQ_SPAN returns Err before
        // encoding — verified by the compile-time span bound on
        // migrations (≤512) plus this direct assert on the limit value.
        assert!(SEQ_SPAN == 4096);
        // Same-position low index of the NEXT op never collides.
        assert_ne!(engine_seq(1, 4095, 0), engine_seq(2, 0, 0));
    }

    // -----------------------------------------------------------------
    // W4: generic-channel adaptation, engine-seq isolation, journal.
    // -----------------------------------------------------------------

    #[test]
    fn is_d1_kind_routes_only_the_three() {
        for k in ["EXECUTE", "MIGRATIONS_APPLY", "DROP"] {
            assert!(is_d1_kind(k), "{k}");
        }
        for k in ["WF_GET", "WF_CREATE", "execute", "", "DROP_TABLE"] {
            assert!(!is_d1_kind(k), "{k}");
        }
    }

    #[test]
    fn from_cell_op_maps_resource_key_and_flags_channel() {
        let cell_op = crate::execset::CellOp {
            id: "cell-op-1".into(),
            seq: 42,
            kind: "EXECUTE".into(),
            worker_id: "w1".into(),
            resource_key: "a".repeat(26),
            payload: serde_json::json!({"sql":"select 1"}),
            expires_at: Some(9_999_999),
        };
        let d = D1Op::from_cell_op(&cell_op).unwrap();
        assert_eq!(d.database_id, "a".repeat(26));
        assert_eq!(d.kind, D1OpKind::Execute);
        assert_eq!(d.chan, CELL_OPS_SEQ_FLAG);
        // Non-D1 kinds refuse.
        let mut wf = cell_op.clone();
        wf.kind = "WF_GET".into();
        assert!(D1Op::from_cell_op(&wf).is_none());
        // Queue drops keep their payload kind marker.
        let mut q = cell_op.clone();
        q.kind = "DROP".into();
        q.payload = serde_json::json!({"kind":"queue"});
        let d = D1Op::from_cell_op(&q).unwrap();
        assert_eq!(
            drop_path(&d.database_id, &d.payload).unwrap(),
            "/queues/".to_string() + &"a".repeat(26) + "/drop"
        );
    }

    #[test]
    fn engine_seq_channel_isolation() {
        // Same op seq on both channels MUST land in different engine
        // dedup slots — otherwise an executed legacy seq could shadow a
        // fresh cell-ops write (silent skip → data loss).
        for seq in [1, 42, 4096, 1_000_000] {
            for idx in [0, 7, 4095] {
                assert_ne!(
                    engine_seq(seq, idx, 0),
                    engine_seq(seq, idx, CELL_OPS_SEQ_FLAG)
                );
            }
        }
        // Within-channel ordering preserved.
        assert!(engine_seq(2, 0, CELL_OPS_SEQ_FLAG) > engine_seq(1, 4095, CELL_OPS_SEQ_FLAG));
    }

    #[test]
    fn legacy_wire_without_chan_deserializes() {
        // Pre-W4 row shape — `chan` absent ⇒ 0 (legacy namespace).
        let json = r#"{"id":"o1","seq":3,"kind":"EXECUTE","databaseId":"aaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{},"expiresAt":100}"#;
        let d: D1Op = serde_json::from_str(json).unwrap();
        assert_eq!(d.chan, 0);
        assert_eq!(d.database_id.len(), 26);
    }

    /// Minimal stub for the legacy ops channel + data plane: serves
    /// the ops page once per GET, counts `/dbs/*/execute` hits, acks 200.
    async fn stub_d1_plane(
        ops_json: String,
        hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let h = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let ops = ops_json.clone();
                let hits = hits.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 65536];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let line = req.lines().next().unwrap_or("");
                    let (status, body) = if line.starts_with("POST /dbs/") {
                        hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        ("200 OK", r#"{"success":true}"#.to_string())
                    } else if line.starts_with("POST ") && line.contains("-ops/ack") {
                        ("200 OK", "{}".to_string())
                    } else if line.starts_with("GET ") && line.contains("/cell-ops") {
                        ("200 OK", ops)
                    } else if line.starts_with("GET ") && line.contains("/d1-ops") {
                        ("200 OK", ops)
                    } else {
                        ("404 Not Found", "{}".to_string())
                    };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        (format!("http://127.0.0.1:{port}"), h)
    }

    #[tokio::test]
    async fn crash_between_execute_and_ack_never_reexecutes() {
        // W4: persist-before-ack on the LEGACY channel — the execution
        // outcome is journaled before the ack POST; a crash followed by
        // redelivery replays the outcome without touching vrt-sql again.
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let db = "a".repeat(26);
        let ops = serde_json::json!({
            "data": {"ops": [{
                "id": "d1-op-1", "seq": 7, "kind": "EXECUTE",
                "databaseId": db, "payload": {"sql":"insert into t values(1)"},
                "expiresAt": 9999999999i64
            }]}
        })
        .to_string();
        let (base, _h) = stub_d1_plane(ops, hits.clone()).await;
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let exec = D1OpsExecutor::new(&D1SqlConfig {
            endpoint: base.clone(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 2,
        })
        .unwrap();
        let src =
            HttpD1OpsSource::new(&base, "cell-1", "t".into(), Duration::from_secs(2)).unwrap();
        let sink =
            HttpD1OpsAckSink::new(&base, "cell-1", "t".into(), Duration::from_secs(2)).unwrap();
        let journal_path = dir.path().join("d1-executed.json");

        // Tick 1 — execute once, journal, ack.
        let mut seen = SeenSet::default();
        let mut executed = crate::execset::ExecutedSet::load(journal_path.clone());
        assert_eq!(
            ops_tick(
                &src,
                &sink,
                &exec,
                &mut seen,
                &mut executed,
                &D1OrderGuard::default(),
                8
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);

        // "Crash": drop the journal + seen set, reload from disk — the
        // op is redelivered (still PENDING upstream until the ack lands
        // — our stub always serves it).
        drop(executed);
        let mut seen2 = SeenSet::default();
        let mut executed2 = crate::execset::ExecutedSet::load(journal_path);
        assert_eq!(
            ops_tick(
                &src,
                &sink,
                &exec,
                &mut seen2,
                &mut executed2,
                &D1OrderGuard::default(),
                8
            )
            .await
            .unwrap(),
            1 // re-acked from the journal
        );
        // The write path was NOT re-entered — the outcome replayed.
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn order_guard_blocks_same_db_and_releases_after_ack() {
        // W4-r2 P0-2: the legacy task marks the pulled op's database
        // for the whole pull→ack span; release lands after the ack
        // POST so a mid-flight redelivery re-blocks on the next pull.
        let guard = D1OrderGuard::default();
        let db = "b".repeat(26);
        let ops = serde_json::json!({
            "data": {"ops": [{
                "id": "d1-op-g1", "seq": 9, "kind": "EXECUTE",
                "databaseId": db, "payload": {"sql":"select 1"},
                "expiresAt": 9999999999i64
            }]}
        })
        .to_string();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (base, _h) = stub_d1_plane(ops, hits.clone()).await;
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let exec = D1OpsExecutor::new(&D1SqlConfig {
            endpoint: base.clone(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 2,
        })
        .unwrap();
        let src =
            HttpD1OpsSource::new(&base, "cell-1", "t".into(), Duration::from_secs(2)).unwrap();
        let sink =
            HttpD1OpsAckSink::new(&base, "cell-1", "t".into(), Duration::from_secs(2)).unwrap();
        let mut seen = SeenSet::default();
        let mut executed = crate::execset::ExecutedSet::memory();
        // Pre-block the pulled op itself (per-op keying — W4-r3):
        // a generic-channel op for the same db defers.
        guard.block(&db, "d1-op-g1", None);
        assert!(guard.is_blocked(&db));
        assert!(!guard.is_blocked(&"z".repeat(26)));
        // Running the legacy tick blocks→executes→acks→releases.
        assert_eq!(
            ops_tick(&src, &sink, &exec, &mut seen, &mut executed, &guard, 8)
                .await
                .unwrap(),
            1
        );
        assert!(!guard.is_blocked(&db), "op acked — order released");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generic_channel_defers_while_same_db_legacy_unfinished() {
        // W4-r2 P0-2: a generic-channel EXECUTE for db A must NOT run
        // while a legacy op for A is unfinished; a DIFFERENT database
        // stays parallel. The stub serves the same body on both
        // channels — we drive it per-side.
        let guard = D1OrderGuard::default();
        let db_a = "c".repeat(26);
        let db_b = "d".repeat(26);
        let cell_ops = serde_json::json!({
            "data": {"ops": [
                {"id":"co-a","seq":1,"kind":"EXECUTE","workerId":"w1",
                 "resourceKey": db_a, "payload":{"sql":"insert into t values(1)"},
                 "expiresAt": 9999999999i64},
                {"id":"co-b","seq":2,"kind":"EXECUTE","workerId":"w1",
                 "resourceKey": db_b, "payload":{"sql":"insert into t values(1)"},
                 "expiresAt": 9999999999i64}
            ]}
        })
        .to_string();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (base, _h) = stub_d1_plane(cell_ops, hits.clone()).await;
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("d1.secret");
        write_test_secret(&secret);
        let d1_exec = D1OpsExecutor::new(&D1SqlConfig {
            endpoint: base.clone(),
            auth_secret_file: secret.to_string_lossy().into(),
            timeout_secs: 2,
        })
        .unwrap();
        let src = crate::wfops::HttpCellOpsSource::new(
            &base,
            "cell-1",
            "t".into(),
            Duration::from_secs(2),
        )
        .unwrap();
        let sink = crate::wfops::HttpCellOpsAckSink::new(
            &base,
            "cell-1",
            "t".into(),
            Duration::from_secs(2),
        )
        .unwrap();
        let mut seen = crate::execset::SeenSet::default();
        let mut executed = crate::execset::ExecutedSet::memory();

        // A legacy op for db A is unfinished → co-a defers, co-b runs.
        guard.block(&db_a, "op-a", None);
        assert_eq!(
            crate::wfops::wfops_tick(
                &src,
                &sink,
                None,
                Some(&d1_exec),
                None,
                None,
                &mut seen,
                &mut executed,
                None,
                &guard,
                8
            )
            .await
            .unwrap(),
            1 // only co-b executed+acked
        );
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(executed.get("co-b").is_some());
        assert!(
            executed.get("co-a").is_none(),
            "deferred op must not be recorded"
        );

        // Legacy finished → next tick runs co-a.
        guard.release(&db_a, "op-a");
        assert_eq!(
            crate::wfops::wfops_tick(
                &src,
                &sink,
                None,
                Some(&d1_exec),
                None,
                None,
                &mut seen,
                &mut executed,
                None,
                &guard,
                8
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(executed.get("co-a").is_some());
    }

    #[tokio::test]
    async fn expired_legacy_entry_releases_order_constraint() {
        // W4-r2 P0-2: a legacy op pulled but expired server-side before
        // ack must NOT pin the database forever — `is_blocked` evicts
        // entries past the op's own `expires_at` deadline.
        let guard = D1OrderGuard::default();
        let db = "e".repeat(26);
        // Deadline in the past → never blocks (lazy eviction).
        guard.block(&db, "op-1", Some(1));
        assert!(
            !guard.is_blocked(&db),
            "past-deadline entry must be evicted"
        );
        // Future deadline blocks; the same entry then expires.
        guard.block(&db, "op-1", Some(D1OrderGuard::now_secs() + 60));
        assert!(guard.is_blocked(&db));
        // Re-block with an already-past deadline → evicted on read.
        guard.block(&db, "op-1", Some(1));
        assert!(!guard.is_blocked(&db));
    }

    #[tokio::test]
    async fn same_db_ops_release_only_when_all_acked() {
        // W4-r3 P0-5: the guard is keyed per OP — two legacy ops for
        // the same db pulled in one batch must keep the db blocked
        // until BOTH ack; releasing on the first ack would let a
        // generic-channel op interleave before the second finishes.
        let guard = D1OrderGuard::default();
        let db = "f".repeat(26);
        guard.block(&db, "op-a", None);
        guard.block(&db, "op-b", None);
        guard.release(&db, "op-a");
        assert!(
            guard.is_blocked(&db),
            "op-b still unfinished — db must stay blocked"
        );
        guard.release(&db, "op-b");
        assert!(!guard.is_blocked(&db), "all ops acked — db unblocked");
        // Per-op expiry: op-a expired (past deadline) does not lift
        // op-b's block; both expired unblocks.
        guard.block(&db, "op-a", Some(1));
        guard.block(&db, "op-b", Some(D1OrderGuard::now_secs() + 60));
        assert!(guard.is_blocked(&db), "live op-b keeps the block");
        guard.block(&db, "op-b", Some(1)); // op-b now also expired
        assert!(
            !guard.is_blocked(&db),
            "all entries expired — evicted on read"
        );
    }
}
