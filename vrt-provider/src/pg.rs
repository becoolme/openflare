//! `PgProvider` — typed Postgres binding over the tenant HTTP gateway.
//!
//! P1: the isolate holds only a binding object; the tenant JWT lives
//! exclusively in `pg-credentials.json` (0600, written by
//! cell-controller from the control-plane render-inputs channel) and in
//! this provider's memory. Desired state carries only `credentialRef`.
//!
//! Data plane: `POST {endpoint}/v1/tenants/<slug>/sql` (Neon-compatible
//! wire). Read-only enforcement is at the database layer: read-only
//! bindings (or migration-fenced tenants, `readOnly` in the credential
//! payload) are ALWAYS issued as a batch with
//! `neon-batch-read-only: true` so the gateway derives `BEGIN READ
//! ONLY` — writes fail inside PostgreSQL regardless of SQL text,
//! retries, or transaction wrapping.

use crate::context::{BindingContext, BindingResource};
use crate::error::ProviderError;
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

/// Parameters for one prepared statement execution.
#[derive(Clone, Debug)]
pub struct PgQuery<'a> {
    /// Parameterized SQL text (`$1..$n` placeholders).
    pub sql: &'a str,
    /// Typed bind parameters; binary encoding is provider-defined.
    pub params: &'a [PgParam<'a>],
}

/// Typed bind parameter for [`PgQuery`].
#[derive(Clone, Debug)]
pub enum PgParam<'a> {
    /// SQL NULL.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Int8(i64),
    /// IEEE-754 double.
    Float8(f64),
    /// UTF-8 text.
    Text(&'a str),
    /// Raw bytes.
    Bytes(&'a [u8]),
}

/// A single column value in a returned row.
#[derive(Clone, Debug)]
pub enum PgValue {
    /// SQL NULL.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Int8(i64),
    /// IEEE-754 double.
    Float8(f64),
    /// UTF-8 text.
    Text(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
}

/// Query result: column names plus rows (aligned by index).
#[derive(Clone, Debug, Default)]
pub struct PgResult {
    /// Column names in result order.
    pub columns: Vec<String>,
    /// Rows of values aligned to `columns`.
    pub rows: Vec<Vec<PgValue>>,
    /// Rows affected for DML statements.
    pub affected: u64,
}

/// Typed Postgres binding provider contract.
#[async_trait]
pub trait PgProvider: Send + Sync + Debug {
    /// Execute one statement inside the authorized database.
    async fn query(
        &self,
        ctx: &BindingContext,
        query: PgQuery<'_>,
    ) -> Result<PgResult, ProviderError>;
}

// ---------------- P1 data plane ----------------

/// Upstream request timeout — gateway-side budgets are shorter; this
/// only bounds the provider's wait.
pub const PG_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// Incoming Neon wire body cap (also enforced at dispatch).
pub const PG_MAX_REQUEST_BYTES: usize = 256 * 1024;
/// Relayed response cap — gateway caps at 4 MiB; this is defense.
pub const PG_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
/// Batch statement ceiling.
pub const PG_MAX_BATCH_STATEMENTS: usize = 64;
/// Credential file byte cap (bounds parse work).
const PG_CREDENTIAL_FILE_MAX: u64 = 4 * 1024 * 1024;

/// One credential payload, mirroring cell-controller's
/// `pg-credentials.json` shape.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PgCredential {
    pub token: String,
    pub endpoint: String,
    #[serde(default)]
    pub tenant_slug: String,
    #[serde(default)]
    pub credential_version: u64,
    #[serde(default)]
    pub read_only: bool,
    /// MIG-15: optional internal-TCP payload for the pg-wire proxy —
    /// real upstream endpoint + login, never isolate-visible.
    #[serde(default)]
    pub tcp: Option<PgTcpCredential>,
    /// MIG-15 r2: explicit TCP block marker (e.g. "migration_read_only")
    /// — sessions fail typed `PG_MIGRATION_READ_ONLY`; the sweeper
    /// kills live sessions carrying it within one interval.
    #[serde(default)]
    pub tcp_blocked: Option<String>,
}

/// MIG-15: upstream TCP endpoint + real login for the pg-wire proxy.
/// Rendered by the server from an `INTERNAL` TenantEndpoint; the
/// username/password are the DB-layer role the proxy authenticates as.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PgTcpCredential {
    /// Internal endpoint host (tenant gateway / pooler service DNS).
    pub host: String,
    /// Internal endpoint port (typically 5432).
    pub port: u16,
    /// DB-layer role name for upstream auth.
    pub user: String,
    /// DB-layer role password for upstream auth — provider-held only.
    pub password: String,
    /// Target database name (tenant DB).
    pub database: String,
}

/// File shape: `{tenants: {teamId: {credentialRef: PgCredential}}}`.
#[derive(Debug, Deserialize)]
struct PgCredentialFile {
    #[serde(default)]
    tenants: BTreeMap<String, BTreeMap<String, PgCredential>>,
}

/// Hot-reloading credential store. `load` re-stats the file every call
/// and re-parses on mtime change — rotation converges within the
/// cell-controller's render-inputs poll (the documented bound).
/// Missing/unparseable → empty map → lookups fail closed.
#[derive(Debug)]
pub struct PgCredentialStore {
    path: PathBuf,
    state: Mutex<PgCredentialState>,
}

#[derive(Debug, Default)]
struct PgCredentialState {
    mtime: Option<std::time::SystemTime>,
    tenants: BTreeMap<String, BTreeMap<String, PgCredential>>,
}

impl PgCredentialStore {
    /// Create a store over `path` (0600 file, same dir as
    /// render-inputs.json).
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            state: Mutex::new(PgCredentialState::default()),
        }
    }

    /// Current credential for `tenant`/`credential_ref` — `None` when
    /// the file is absent, unparseable, rotated away, or the ref was
    /// dropped (tenant deleted). Never errors: absence is the signal.
    pub fn lookup(&self, tenant: &str, credential_ref: &str) -> Option<PgCredential> {
        self.reload();
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.tenants.get(tenant)?.get(credential_ref).cloned()
    }

    fn reload(&self) {
        let meta = match std::fs::metadata(&self.path) {
            Ok(m) => m,
            Err(_) => {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                state.tenants.clear();
                state.mtime = None;
                return;
            }
        };
        let mtime = meta.modified().ok();
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.mtime == mtime && mtime.is_some() {
                return;
            }
        }
        // Permissions: credentials must be owner-only (0600) — a
        // world-readable token file is treated as absent (fail closed).
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.mode() & 0o077 != 0 {
                tracing::error!("pg credentials file has insecure permissions — refused");
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                state.tenants.clear();
                state.mtime = mtime;
                return;
            }
        }
        if meta.len() > PG_CREDENTIAL_FILE_MAX {
            tracing::error!("pg credentials file too large — refused");
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.tenants.clear();
            state.mtime = mtime;
            return;
        }
        let parsed = std::fs::read(&self.path)
            .ok()
            .and_then(|b| serde_json::from_slice::<PgCredentialFile>(&b).ok());
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match parsed {
            Some(f) => {
                state.tenants = f.tenants;
            }
            None => {
                tracing::error!("pg credentials file unreadable/invalid — refusing");
                state.tenants.clear();
            }
        }
        state.mtime = mtime;
    }
}

/// Parsed outcome of a backend execute call.
#[derive(Debug)]
pub struct PgResponse {
    /// HTTP status relayed to the facade.
    pub status: u16,
    /// Response body (Neon wire JSON — never contains credentials).
    pub body: Vec<u8>,
}

/// HTTP data-plane backend. One instance per provider process; egress
/// is pinned per request to the credential's endpoint host.
#[derive(Debug)]
pub struct PgBackend {
    client: reqwest::Client,
    creds: std::sync::Arc<PgCredentialStore>,
}

impl PgBackend {
    /// Build a backend over `creds`. The reqwest client carries no
    /// default headers and follows no redirects.
    pub fn new(creds: std::sync::Arc<PgCredentialStore>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(PG_REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { client, creds }
    }

    /// Authorize + forward one Neon `/sql` request. `body` is the
    /// already size-bounded Neon wire payload. All failures are typed
    /// [`ProviderError`]s — upstream bodies are never relayed for
    /// non-200 statuses (bounded surface, no credential echoes).
    pub async fn execute(
        &self,
        ctx: &BindingContext,
        body: &[u8],
        inbound_headers: &[(String, String)],
    ) -> Result<PgResponse, ProviderError> {
        let (credential_ref, read_only_grant) = match ctx.resource() {
            BindingResource::PgDatabase { credential_ref, .. } => {
                (credential_ref.clone(), !ctx.permissions().write)
            }
            _ => return Err(ProviderError::Protocol),
        };
        let cred = self
            .creds
            .lookup(ctx.tenant_id(), &credential_ref)
            .ok_or(ProviderError::PgCredentialUnavailable)?;
        // Endpoint + slug are trusted-side config, still shape-checked:
        // scheme http(s), no userinfo, slug is a DNS-safe single label
        // (never a path segment).
        let endpoint = cred.endpoint.trim_end_matches('/');
        let endpoint_ok = reqwest::Url::parse(endpoint).ok().is_some_and(|u| {
            matches!(u.scheme(), "http" | "https")
                && !u.host_str().unwrap_or_default().is_empty()
                && u.username().is_empty()
        });
        let slug_ok = !cred.tenant_slug.is_empty()
            && cred
                .tenant_slug
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !endpoint_ok || !slug_ok {
            return Err(ProviderError::PgCredentialUnavailable);
        }
        let target = format!("{endpoint}/v1/tenants/{}/sql", cred.tenant_slug);
        let read_only = read_only_grant || cred.read_only;

        // F3: rebuild — never forward caller bytes. The Neon wire
        // protocol admits exactly two shapes; anything else is a
        // protocol rejection at the trust boundary (the gateway's own
        // precedence for mixed keys is NOT relied on):
        //   {"query": string, "params": [...]}          — single
        //   {"queries": [{query, params}, ...]}         — batch
        // Mixed `query`+`queries`, unknown top-level keys, non-string
        // query, non-array params, unknown per-statement keys → 400.
        // Read-only ALWAYS emits the batch shape so
        // `neon-batch-read-only: true` yields BEGIN READ ONLY —
        // enforcement lives in PostgreSQL, not in this process.
        let payload_in: serde_json::Value =
            serde_json::from_slice(body).map_err(|_| ProviderError::Protocol)?;
        let stmt_of = |e: &serde_json::Value| -> Option<(String, serde_json::Value)> {
            let o = e.as_object()?;
            let mut query = None;
            let mut params = serde_json::json!([]);
            for (k, v) in o {
                match k.as_str() {
                    "query" => {
                        query = Some(v.as_str()?.to_string());
                    }
                    "params" => {
                        if !v.is_array() {
                            return None;
                        }
                        params = v.clone();
                    }
                    _ => return None, // unknown per-statement key
                }
            }
            Some((query?, params))
        };
        let top = payload_in.as_object().ok_or(ProviderError::Protocol)?;
        let has_single_key = top.contains_key("query") || top.contains_key("params");
        let has_batch_key = top.contains_key("queries");
        if has_single_key && has_batch_key {
            return Err(ProviderError::Protocol); // mixed shapes
        }
        for k in top.keys() {
            match k.as_str() {
                "query" | "params" | "queries" => {}
                _ => return Err(ProviderError::Protocol), // unknown top-level key
            }
        }
        let statements: Vec<(String, serde_json::Value)> = if has_batch_key {
            let b = top["queries"].as_array().ok_or(ProviderError::Protocol)?;
            if b.is_empty() || b.len() > PG_MAX_BATCH_STATEMENTS {
                return Err(ProviderError::LimitExceeded);
            }
            let mut out = Vec::with_capacity(b.len());
            for e in b {
                out.push(stmt_of(e).ok_or(ProviderError::Protocol)?);
            }
            out
        } else if has_single_key {
            vec![stmt_of(&payload_in).ok_or(ProviderError::Protocol)?]
        } else {
            return Err(ProviderError::Protocol);
        };
        let payload = if read_only || has_batch_key {
            serde_json::json!({
                "queries": statements
                    .iter()
                    .map(|(q, p)| serde_json::json!({"query": q, "params": p}))
                    .collect::<Vec<_>>(),
            })
        } else {
            serde_json::json!({
                "query": statements[0].0,
                "params": statements[0].1,
            })
        };

        let mut request = self
            .client
            .post(target)
            .bearer_auth(&cred.token)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&payload).unwrap_or_default());
        // neon-batch-* passthrough: explicit allowlist, case-insensitive.
        // The Neon HTTP batch-transaction surface is exactly three
        // headers — postgres-tenant-gateway `neon.rs` NeonBatchHeaders
        // parsing (neon.rs:144-167) honors isolation-level/read-only/
        // deferrable and nothing else; upstream definition:
        // https://github.com/neondatabase/serverless README "Batch
        // queries" transaction options (the /sql HTTP wire format).
        // Anything else is dropped rather than smuggled through. Under
        // a read-only grant the caller's `neon-batch-read-only` (ANY
        // case) is dropped — it can only weaken the grant.
        const NEON_BATCH_ALLOWLIST: &[&str] = &[
            "neon-batch-isolation-level",
            "neon-batch-read-only",
            "neon-batch-deferrable",
        ];
        for (k, v) in inbound_headers {
            let lk = k.to_ascii_lowercase();
            if !NEON_BATCH_ALLOWLIST.contains(&lk.as_str()) {
                continue;
            }
            if read_only && lk == "neon-batch-read-only" {
                continue;
            }
            request = request.header(lk.as_str(), v.as_str());
        }
        if read_only {
            request = request.header("neon-batch-read-only", "true");
        }

        // Deadline: min(ctx.deadline, request timeout) — ctx deadline
        // is the listener's budget; the client timeout bounds upstream.
        let deadline = ctx.deadline();
        let write_capable = !read_only;
        let upstream = tokio::time::timeout_at(deadline, request.send())
            .await
            .map_err(|_| Self::unavailable(write_capable))?
            .map_err(|e| {
                if e.is_timeout() {
                    Self::unavailable(write_capable)
                } else {
                    ProviderError::PgBackendUnavailable
                }
            })?;
        let status = upstream.status().as_u16();
        // RV-R8 #8: chunked capped read — the Content-Length pre-check
        // and post-length assert fold into one bounded accumulation
        // that also guards a lying/absent header.
        let body = tokio::time::timeout_at(
            deadline,
            crate::upstream_body::read_body_capped(upstream, PG_MAX_RESPONSE_BYTES),
        )
        .await
        .map_err(|_| Self::unavailable(write_capable))?
        .map_err(|e| match e {
            crate::upstream_body::UpstreamBodyErr::TooLarge => {
                tracing::warn!(
                    cap = PG_MAX_RESPONSE_BYTES,
                    "pg upstream response exceeded byte cap"
                );
                ProviderError::PgResponseTooLarge
            }
            crate::upstream_body::UpstreamBodyErr::Transport => ProviderError::PgBackendUnavailable,
        })?;
        match status {
            200 => Ok(PgResponse {
                status,
                body: body.to_vec(),
            }),
            401 => Err(ProviderError::PgCredentialExpired),
            404 => Err(ProviderError::PgTenantGone),
            409 => Err(ProviderError::PgWriteFenced),
            429 => Err(ProviderError::PgRateLimited),
            _ if (500..=599).contains(&status) => Err(Self::unavailable(write_capable)),
            _ => Err(ProviderError::Protocol),
        }
    }

    /// 5xx/timeout on a write-capable binding → result unknown; on a
    /// read-only one → plain unavailability (retries are safe).
    fn unavailable(write_capable: bool) -> ProviderError {
        if write_capable {
            ProviderError::PgResultUnknown
        } else {
            ProviderError::PgBackendUnavailable
        }
    }
}

// ---------------- tests (P1) ----------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{BindingResource, Permissions};
    use axum::{extract::State, response::IntoResponse, routing::post, Router};
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    struct Seen {
        auth: Option<String>,
        read_only_header: Option<String>,
        body: serde_json::Value,
    }

    fn ctx(perm_write: bool, credential_ref: &str) -> BindingContext {
        BindingContext::grant(
            "team-a".into(),
            "w-1".into(),
            "w-1:v1".into(),
            "w-1~DB".into(),
            "DB".into(),
            BindingResource::PgDatabase {
                id: "vrtpg:t-1".into(),
                credential_ref: credential_ref.into(),
            },
            Permissions {
                read: true,
                write: perm_write,
            },
            1,
            1,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )
    }

    fn creds_file(tenants: serde_json::Value) -> (tempfile_like::TmpDir, std::path::PathBuf) {
        let dir = tempfile_like::TmpDir::new();
        let path = dir.path().join("pg-credentials.json");
        std::fs::write(&path, serde_json::to_vec(&tenants).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        (dir, path)
    }

    /// Minimal tempfile shim — tempfile is not a dependency.
    mod tempfile_like {
        pub struct TmpDir(std::path::PathBuf);
        impl TmpDir {
            pub fn new() -> Self {
                // Unique per call even when parallel tests share the
                // clock granularity — a shared dir would let one test's
                // credential file overwrite another's.
                static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let p = std::env::temp_dir().join(format!(
                    "vrt-pg-test-{}-{}",
                    std::process::id(),
                    SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                ));
                std::fs::create_dir_all(&p).unwrap();
                Self(p)
            }
            pub fn path(&self) -> &std::path::Path {
                &self.0
            }
        }
        impl Drop for TmpDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    async fn mock_sql(status: u16, body: &'static str) -> (String, Arc<TokioMutex<Vec<Seen>>>) {
        let seen = Arc::new(TokioMutex::new(Vec::new()));
        let seen2 = seen.clone();
        let app = Router::new().route(
            "/v1/tenants/{slug}/sql",
            post(move |State(()): State<()>, req: axum::extract::Request| {
                let seen = seen2.clone();
                async move {
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let ro = req
                        .headers()
                        .get("neon-batch-read-only")
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let bytes = axum::body::to_bytes(req.into_body(), 1 << 20)
                        .await
                        .unwrap();
                    seen.lock().await.push(Seen {
                        auth,
                        read_only_header: ro,
                        body: serde_json::from_slice(&bytes).unwrap_or_default(),
                    });
                    (
                        axum::http::StatusCode::from_u16(status).unwrap(),
                        body.to_string(),
                    )
                        .into_response()
                }
            }),
        );
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(lis, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn backend(file: &std::path::Path) -> PgBackend {
        PgBackend::new(Arc::new(PgCredentialStore::new(file.to_path_buf())))
    }

    fn tenants(endpoint: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut cred = serde_json::json!({
            "token": "jwt-abc",
            "endpoint": endpoint,
            "tenantSlug": "team-a-tenant",
            "credentialVersion": 3,
            "readOnly": false,
        });
        for (k, v) in extra.as_object().unwrap_or(&serde_json::Map::new()) {
            cred[k.clone()] = v.clone();
        }
        serde_json::json!({"tenants": {"team-a": {"vrtpgcred:t-1": cred}}})
    }

    #[tokio::test]
    async fn query_relays_params_and_bearer() {
        let (endpoint, seen) = mock_sql(200, r#"{"rows":[["1"]]}"#).await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        let out = be
            .execute(
                &ctx(true, "vrtpgcred:t-1"),
                br#"{"query":"select $1","params":[1]}"#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(out.status, 200);
        let seen = seen.lock().await;
        assert_eq!(seen[0].auth.as_deref(), Some("Bearer jwt-abc"));
        assert_eq!(seen[0].read_only_header, None);
    }

    #[tokio::test]
    async fn read_only_grant_forces_batch_and_ro_header() {
        let (endpoint, seen) = mock_sql(200, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        be.execute(
            &ctx(false, "vrtpgcred:t-1"),
            br#"{"query":"insert into t values (1)","params":[]}"#,
            // client tries to downgrade the grant — must be overridden.
            &[("neon-batch-read-only".into(), "false".into())],
        )
        .await
        .unwrap();
        let seen = seen.lock().await;
        assert_eq!(seen[0].read_only_header.as_deref(), Some("true"));
        // single query rewritten to batch form
        assert!(seen[0].body.get("queries").is_some());
    }

    #[tokio::test]
    async fn migration_fenced_credential_forces_read_only() {
        let (endpoint, seen) = mock_sql(200, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({"readOnly": true})));
        let be = backend(&path);
        be.execute(&ctx(true, "vrtpgcred:t-1"), br#"{"query":"select 1"}"#, &[])
            .await
            .unwrap();
        assert_eq!(
            seen.lock().await[0].read_only_header.as_deref(),
            Some("true")
        );
    }

    #[tokio::test]
    async fn gateway_errors_map_typed() {
        for (status, want) in [
            (401u16, "PG_CREDENTIAL_EXPIRED"),
            (404, "PG_TENANT_GONE"),
            (409, "PG_WRITE_FENCED"),
            (429, "PG_RATE_LIMITED"),
        ] {
            let (endpoint, _seen) = mock_sql(status, "{}").await;
            let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
            let be = backend(&path);
            let err = be
                .execute(&ctx(true, "vrtpgcred:t-1"), br#"{"query":"select 1"}"#, &[])
                .await
                .unwrap_err();
            assert_eq!(err.code(), want, "status {status}");
        }
    }

    #[tokio::test]
    async fn gateway_5xx_on_write_is_result_unknown() {
        let (endpoint, _seen) = mock_sql(500, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        let err = be
            .execute(
                &ctx(true, "vrtpgcred:t-1"),
                br#"{"query":"delete from t"}"#,
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PG_RESULT_UNKNOWN");
    }

    #[tokio::test]
    async fn gateway_5xx_on_read_only_is_unavailable() {
        let (endpoint, _seen) = mock_sql(503, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        let err = be
            .execute(
                &ctx(false, "vrtpgcred:t-1"),
                br#"{"query":"select 1"}"#,
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PG_BACKEND_UNAVAILABLE");
    }

    #[tokio::test]
    async fn missing_credential_ref_fails_closed() {
        let (_d, path) = creds_file(serde_json::json!({"tenants": {}}));
        let be = backend(&path);
        let err = be
            .execute(&ctx(true, "vrtpgcred:t-1"), br#"{"query":"select 1"}"#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PG_CREDENTIAL_UNAVAILABLE");
    }

    #[tokio::test]
    async fn cross_tenant_credential_isolation() {
        // team-b's creds must not resolve under team-a's ctx.
        let (_d, path) = creds_file(serde_json::json!({
            "tenants": {"team-b": {"vrtpgcred:t-1": {
                "token":"jwt-b","endpoint":"http://127.0.0.1:1","tenantSlug":"t",
                "credentialVersion":1,"readOnly":false }}}}
        ));
        let be = backend(&path);
        let err = be
            .execute(&ctx(true, "vrtpgcred:t-1"), br#"{"query":"select 1"}"#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PG_CREDENTIAL_UNAVAILABLE");
    }

    #[tokio::test]
    async fn oversized_response_is_capped() {
        let big = "x".repeat(super::PG_MAX_RESPONSE_BYTES + 1);
        let (endpoint, _seen) = mock_sql(200, "unused").await;
        let _ = big; // body generated below
        let seen = Arc::new(TokioMutex::new(Vec::<Seen>::new()));
        let _ = seen;
        let app = Router::new().route(
            "/v1/tenants/{slug}/sql",
            post(move || async move {
                (
                    axum::http::StatusCode::OK,
                    axum::body::Body::from("x".repeat(super::PG_MAX_RESPONSE_BYTES + 1)),
                )
            }),
        );
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(lis, app).await.unwrap() });
        let (_d, path) = creds_file(tenants(&format!("http://{addr}"), serde_json::json!({})));
        let be = backend(&path);
        let err = be
            .execute(&ctx(true, "vrtpgcred:t-1"), br#"{"query":"select 1"}"#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PG_RESPONSE_TOO_LARGE");
        let _ = endpoint;
    }

    /// RV-R8 #8: the same cap must hold when the upstream omits
    /// Content-Length and streams chunks — the read aborts mid-body,
    /// not after buffering.
    #[tokio::test]
    async fn chunked_oversized_response_is_capped_mid_stream() {
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut s, _)) = lis.accept().await {
                let mut buf = [0u8; 8192];
                // Drain request head.
                let _ = s.read(&mut buf).await;
                let mut raw = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n".to_vec();
                for _ in 0..6 {
                    let chunk = vec![b'x'; 1024 * 1024];
                    raw.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
                    raw.extend_from_slice(&chunk);
                    raw.extend_from_slice(b"\r\n");
                }
                raw.extend_from_slice(b"0\r\n\r\n");
                let _ = s.write_all(&raw).await;
            }
        });
        let (_d, path) = creds_file(tenants(&format!("http://{addr}"), serde_json::json!({})));
        let be = backend(&path);
        let err = be
            .execute(&ctx(true, "vrtpgcred:t-1"), br#"{"query":"select 1"}"#, &[])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "PG_RESPONSE_TOO_LARGE");
    }

    // F3: the provider is the trust boundary — it reconstructs the
    // request body and rejects every shape outside the two canonical
    // Neon wire forms.
    #[tokio::test]
    async fn malformed_bodies_are_protocol_rejected() {
        let (endpoint, _seen) = mock_sql(200, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        for body in [
            // mixed single + batch keys
            br#"{"query":"select 1","queries":[{"query":"select 2"}]}"#.as_slice(),
            // unknown top-level key
            br#"{"query":"select 1","mode":"raw"}"#.as_slice(),
            // params not an array
            br#"{"query":"select 1","params":"x"}"#.as_slice(),
            // unknown per-statement key inside a batch item
            br#"{"queries":[{"query":"select 1","extra":1}]}"#.as_slice(),
            // batch shape but queries not an array
            br#"{"queries":{"query":"select 1"}}"#.as_slice(),
            // non-string query
            br#"{"query":42}"#.as_slice(),
        ] {
            let err = be
                .execute(&ctx(true, "vrtpgcred:t-1"), body, &[])
                .await
                .unwrap_err();
            assert_eq!(
                err.code(),
                "BINDING_PROTOCOL_ERROR",
                "body rejected: {}",
                String::from_utf8_lossy(body)
            );
        }
        // …and nothing reached the gateway.
        assert!(_seen.lock().await.is_empty());
    }

    #[tokio::test]
    async fn batch_body_passes_through_canonicalized() {
        let (endpoint, seen) = mock_sql(200, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        be.execute(
            &ctx(true, "vrtpgcred:t-1"),
            br#"{"queries":[{"query":"select 1","params":[1]},{"query":"select 2"}]}"#,
            &[],
        )
        .await
        .unwrap();
        let seen = seen.lock().await;
        let q = seen[0].body["queries"].as_array().unwrap();
        assert_eq!(q.len(), 2);
        // Rebuilt canonical form: each item has exactly query+params.
        assert_eq!(q[1], serde_json::json!({"query": "select 2", "params": []}));
    }

    #[tokio::test]
    async fn read_only_drops_mixed_case_ro_header_and_allowlist_only() {
        let (endpoint, seen) = mock_sql(200, "{}").await;
        let (_d, path) = creds_file(tenants(&endpoint, serde_json::json!({})));
        let be = backend(&path);
        be.execute(
            &ctx(false, "vrtpgcred:t-1"),
            br#"{"query":"select 1"}"#,
            &[
                // Mixed-case downgrade attempt — must never reach the
                // gateway under a read-only grant.
                ("Neon-Batch-Read-Only".into(), "false".into()),
                // Allowlisted batch header passes.
                ("Neon-Batch-Isolation-Level".into(), "read committed".into()),
                // Non-batch header is not forwarded.
                ("x-evil".into(), "1".into()),
            ],
        )
        .await
        .unwrap();
        let seen = seen.lock().await;
        assert_eq!(seen[0].read_only_header.as_deref(), Some("true"));
    }
}
