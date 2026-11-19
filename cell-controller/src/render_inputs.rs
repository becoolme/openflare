//! B7 — platform render-inputs pull (vars + secrets) for the cell.
//!
//! Contract with `vrt-host`'s `FileRenderInputs` (combined mode): ONE
//! file `render-inputs.json` under a private dir —
//! `{ "vars": {varRef: value}, "secrets": {tenantId: {secretRef:
//! value}} }`, 0600, atomically replaced. A single file is the only
//! shape giving the host a consistent snapshot: two files written
//! independently could leave a mixed generation behind (B4).
//!
//! Pull semantics (B1): `fetch()` NEVER mutates committed state —
//! `commit(fetched)` is called only after `write_render_inputs`
//! succeeds. A failed write therefore re-fetches next tick (no
//! permanent skip), and a 304 keeps everything untouched.
//!
//! Transport security (B2): HTTPS + scoped token + field whitelist.
//! `http` is accepted ONLY for loopback hosts (127.0.0.1/::1/localhost)
//! — plaintext secrets over the network are never allowed.
//! Bounded: RetryPlan backoff per fetch, body byte cap, field caps.
//!
//! Failure keeps the previous file — stale render inputs serve old
//! generations; nothing deletes it (fail closed, same rule as desired).

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use serde::Deserialize;

use crate::retry::RetryPlan;

/// `tenants[teamId] = { vars, secrets, unavailable }` — wire shape of
/// the platform render-inputs endpoint (B5: `unavailable` lists typed
/// failure codes for a tenant whose inputs could not be assembled;
/// its secrets are omitted entirely so referencing generations hit
/// the host's render-inputs-unavailable path instead of a half state).
#[derive(Debug, Deserialize)]
pub struct RenderInputsPayload {
    pub fingerprint: String,
    #[serde(default)]
    pub tenants: std::collections::BTreeMap<String, TenantInputs>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TenantInputs {
    #[serde(default)]
    pub vars: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub secrets: std::collections::BTreeMap<String, String>,
    /// P1: `credentialRef → provider credential payload` for PG
    /// bindings. Written to `pg-credentials.json` — provider-side only,
    /// never enters an isolate's env.
    #[serde(default)]
    pub pg: std::collections::BTreeMap<String, PgCredential>,
    #[serde(default)]
    pub unavailable: Vec<String>,
}

/// P1: one PG credential payload — trusted-side only (token is the
/// tenant's HTTP gateway JWT).
#[derive(Debug, Deserialize)]
pub struct PgCredential {
    pub token: String,
    pub endpoint: String,
    #[serde(rename = "tenantSlug", default)]
    pub tenant_slug: String,
    #[serde(rename = "credentialVersion", default)]
    pub credential_version: u64,
    #[serde(rename = "readOnly", default)]
    pub read_only: bool,
    /// MIG-15 / hyperdrive database target: upstream pg-wire entry +
    /// login for the provider's pg proxy. Previously dropped here, which
    /// made every TCP session fail closed `PG_CREDENTIAL_UNAVAILABLE`.
    /// `deny_unknown_fields` on the inner struct keeps a future wire
    /// addition from being silently discarded (fail closed instead).
    #[serde(default)]
    pub tcp: Option<PgTcpCredential>,
    /// Explicit TCP block marker (e.g. `migration_read_only`).
    #[serde(rename = "tcpBlocked", default)]
    pub tcp_blocked: Option<String>,
}

/// Upstream pg-wire target + real login — trusted-side only; mirrors
/// `vrt_provider::pg::PgTcpCredential` field for field.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgTcpCredential {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Bounds (B3): generous ceilings that stop abuse/pathology without
/// shrinking real payloads.
pub const DEFAULT_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_TENANTS: usize = 1024;
const MAX_REF_LEN: usize = 256;
const MAX_VALUE_LEN: usize = 64 * 1024;
const MAX_FINGERPRINT_LEN: usize = 128;

impl RenderInputsPayload {
    fn validate(&self) -> Result<(), String> {
        if self.fingerprint.is_empty() || self.fingerprint.len() > MAX_FINGERPRINT_LEN {
            return Err("fingerprint invalid".into());
        }
        if self.tenants.len() > MAX_TENANTS {
            return Err("too many tenants".into());
        }
        for (team, t) in &self.tenants {
            if team.len() > MAX_REF_LEN {
                return Err("tenant id too long".into());
            }
            for (k, v) in t.vars.iter().chain(t.secrets.iter()) {
                if k.len() > MAX_REF_LEN {
                    return Err("ref too long".into());
                }
                if v.len() > MAX_VALUE_LEN {
                    return Err("value too large".into());
                }
            }
        }
        Ok(())
    }
}

/// A fetched payload awaiting commit — holds the ETag to store ONLY
/// after the write lands (B1).
pub struct Fetched {
    pub payload: RenderInputsPayload,
    etag: Option<String>,
}

#[derive(Default)]
struct Committed {
    etag: Option<String>,
    fingerprint: Option<String>,
}

/// HTTP puller with ETag caching — same token/timeout posture as
/// `HttpDesiredSource`, plus a real RetryPlan (B3).
pub struct HttpRenderInputsSource {
    client: reqwest::Client,
    url: reqwest::Url,
    token: String,
    committed: Mutex<Committed>,
    retry: RetryPlan,
    max_body_bytes: usize,
}

fn is_loopback_host(host: &str) -> bool {
    let h = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    h == "localhost" || h == "127.0.0.1" || h == "::1" || h.ends_with(".localhost")
}

impl HttpRenderInputsSource {
    pub fn new(
        base: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
        retry: RetryPlan,
    ) -> Result<Self, String> {
        Self::with_body_cap(base, cell_id, token, timeout, retry, DEFAULT_MAX_BODY_BYTES)
    }

    pub fn with_body_cap(
        base: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
        retry: RetryPlan,
        max_body_bytes: usize,
    ) -> Result<Self, String> {
        let url = reqwest::Url::parse(&format!(
            "{}/api/internal/clusters/{}/vrt/render-inputs",
            base.trim_end_matches('/'),
            cell_id
        ))
        .map_err(|e| format!("render-inputs url: {e}"))?;
        // B2: secrets must travel TLS-only. http is allowed solely for
        // loopback (local dev/tests) — anything else fails closed.
        match url.scheme() {
            "https" => {}
            "http" => {
                let host = url.host_str().unwrap_or_default();
                if !is_loopback_host(host) {
                    return Err("render-inputs url must be https (http only for loopback)".into());
                }
            }
            other => return Err(format!("render-inputs url scheme {other:?} unsupported")),
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| format!("client: {e}"))?;
        Ok(Self {
            client,
            url,
            token,
            committed: Mutex::new(Committed::default()),
            retry,
            max_body_bytes,
        })
    }

    /// Fetch with bounded retry → `Some(Fetched)` when new content
    /// arrived. Committed state is NEVER touched here — the caller
    /// must `commit` only after `write_render_inputs` succeeds (B1).
    pub async fn fetch(&self) -> Result<Option<Fetched>, String> {
        let mut attempt = 0usize;
        loop {
            match self.try_fetch().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    attempt += 1;
                    if !self.retry.can_retry(attempt) {
                        return Err(e);
                    }
                    tokio::time::sleep(self.retry.delay_for(attempt)).await;
                }
            }
        }
    }

    async fn try_fetch(&self) -> Result<Option<Fetched>, String> {
        let mut req = self.client.get(self.url.clone()).bearer_auth(&self.token);
        if let Some(tag) = self
            .committed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .etag
            .clone()
        {
            req = req.header("if-none-match", tag);
        }
        let res = req.send().await.map_err(|e| format!("send: {e}"))?;
        if res.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(None);
        }
        if !res.status().is_success() {
            return Err(format!("status {}", res.status()));
        }
        let new_etag = res
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = res.bytes().await.map_err(|e| format!("body: {e}"))?;
        if body.len() > self.max_body_bytes {
            return Err(format!("body exceeds {} bytes", self.max_body_bytes));
        }
        let payload: RenderInputsPayload =
            serde_json::from_slice(&body).map_err(|e| format!("json: {e}"))?;
        payload.validate()?;
        if self
            .committed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .fingerprint
            .as_deref()
            == Some(payload.fingerprint.as_str())
        {
            return Ok(None); // same content, different ETag — skip write
        }
        Ok(Some(Fetched {
            payload,
            etag: new_etag,
        }))
    }

    /// Record the fetch as applied — call ONLY after the write landed.
    pub fn commit(&self, fetched: &Fetched) {
        let mut c = self.committed.lock().unwrap_or_else(|e| e.into_inner());
        c.etag = fetched.etag.clone();
        c.fingerprint = Some(fetched.payload.fingerprint.clone());
    }
}

/// The single combined file the host reads (B4).
pub const RENDER_INPUTS_FILE: &str = "render-inputs.json";
/// P1: provider-facing PG credentials file — `{tenantId: {credentialRef:
/// {token, endpoint, tenantSlug, credentialVersion, readOnly}}}`, 0600,
/// atomically replaced (same rules as render-inputs.json).
pub const PG_CREDENTIALS_FILE: &str = "pg-credentials.json";

/// Materialize a payload into `<dir>/render-inputs.json` — ONE atomic
/// write, so the host never observes a mixed vars/secrets generation.
/// Secrets for `unavailable` tenants are dropped (B5): referencing
/// generations then hit host's render-inputs-unavailable, unaffected
/// tenants keep working.
pub fn write_render_inputs(dir: &Path, payload: &RenderInputsPayload) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir: {e}"))?;
    let mut vars = serde_json::Map::new();
    let mut secrets = serde_json::Map::new();
    for (team, t) in &payload.tenants {
        for (k, v) in &t.vars {
            vars.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        if t.unavailable.is_empty() {
            secrets.insert(
                team.clone(),
                serde_json::Value::Object(
                    t.secrets
                        .iter()
                        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                        .collect(),
                ),
            );
        }
    }
    let doc = serde_json::json!({ "vars": vars, "secrets": secrets });
    vrt_securefs::atomic_write(
        &dir.join(RENDER_INPUTS_FILE),
        serde_json::to_string(&doc).unwrap_or_default().as_bytes(),
        0o600,
    )
    .map_err(|e| format!("{RENDER_INPUTS_FILE}: {e}"))?;

    // P1: provider credentials in a SEPARATE file — the host's render
    // path must stay byte-compatible (it knows only vars/secrets).
    // Unavailable tenants lose their pg map entirely (same B5 rule).
    let mut pg = serde_json::Map::new();
    for (team, t) in &payload.tenants {
        if t.unavailable.is_empty() && !t.pg.is_empty() {
            pg.insert(
                team.clone(),
                serde_json::Value::Object(
                    t.pg
                        .iter()
                        .map(|(k, v)| {
                            let mut cred = serde_json::json!({
                                "token": v.token,
                                "endpoint": v.endpoint,
                                "tenantSlug": v.tenant_slug,
                                "credentialVersion": v.credential_version,
                                "readOnly": v.read_only,
                            });
                            if let Some(tcp) = &v.tcp {
                                cred["tcp"] = serde_json::json!({
                                    "host": tcp.host,
                                    "port": tcp.port,
                                    "user": tcp.user,
                                    "password": tcp.password,
                                    "database": tcp.database,
                                });
                            }
                            if let Some(marker) = &v.tcp_blocked {
                                cred["tcpBlocked"] = serde_json::Value::String(marker.clone());
                            }
                            (k.clone(), cred)
                        })
                        .collect(),
                ),
            );
        }
    }
    let pg_doc = serde_json::json!({ "tenants": pg });
    vrt_securefs::atomic_write(
        &dir.join(PG_CREDENTIALS_FILE),
        serde_json::to_string(&pg_doc).unwrap_or_default().as_bytes(),
        0o600,
    )
    .map_err(|e| format!("{PG_CREDENTIALS_FILE}: {e}"))
}

/// Default render-inputs dir under the host work dir (the config maps
/// host's `renderInputsFile` at `<dir>/render-inputs.json`).
pub fn default_dir(host_work_dir: &Path) -> PathBuf {
    host_work_dir.join("render-inputs")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> RenderInputsPayload {
        serde_json::from_str(
            r#"{"fingerprint":"abc","tenants":{"t1":{"vars":{"vrtvar:w:k":"val"},"secrets":{"vrtsec:w:s":"pw"}},"t2":{"vars":{},"secrets":{"vrtsec:w2:s2":"x"}}}}"#,
        )
        .unwrap()
    }

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ri-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn write_produces_single_combined_file() {
        let dir = tmp("single");
        write_render_inputs(&dir, &payload()).unwrap();
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join(RENDER_INPUTS_FILE)).unwrap())
                .unwrap();
        assert_eq!(doc["vars"]["vrtvar:w:k"], "val");
        assert_eq!(doc["secrets"]["t1"]["vrtsec:w:s"], "pw");
        assert_eq!(doc["secrets"]["t2"]["vrtsec:w2:s2"], "x");
        // exactly ONE file — no vars.json/secrets.json split (B4)
        assert!(!dir.join("vars.json").exists());
        assert!(!dir.join("secrets.json").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join(RENDER_INPUTS_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unavailable_tenant_secrets_are_omitted() {
        let dir = tmp("unavail");
        let p: RenderInputsPayload = serde_json::from_str(
            r#"{"fingerprint":"fp","tenants":{"bad":{"vars":{"vrtvar:w:k":"v"},"secrets":{"vrtsec:w:s":"pw"},"unavailable":["secret-decrypt-failed"]},"good":{"vars":{},"secrets":{"vrtsec:g:s":"ok"}}}}"#,
        )
        .unwrap();
        write_render_inputs(&dir, &p).unwrap();
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join(RENDER_INPUTS_FILE)).unwrap())
                .unwrap();
        assert!(doc["secrets"].get("bad").is_none());
        assert_eq!(doc["secrets"]["good"]["vrtsec:g:s"], "ok");
        assert_eq!(doc["vars"]["vrtvar:w:k"], "v");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_failure_leaves_previous_file_untouched() {
        // B1/B6: an actual failing write (read-only dir) must leave the
        // previously committed file intact — and since commit() is
        // skipped by the caller, the next fetch retries for real.
        let dir = tmp("keep");
        write_render_inputs(&dir, &payload()).unwrap();
        let before = std::fs::read(dir.join(RENDER_INPUTS_FILE)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
            let changed: RenderInputsPayload = serde_json::from_str(
                r#"{"fingerprint":"new","tenants":{"t1":{"vars":{"vrtvar:w:k":"v2"},"secrets":{}}}}"#,
            )
            .unwrap();
            let res = write_render_inputs(&dir, &changed);
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            // root (tests may run as root) can still write — if the write
            // somehow succeeded the content must at least be consistent.
            if res.is_err() {
                assert_eq!(std::fs::read(dir.join(RENDER_INPUTS_FILE)).unwrap(), before);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn payload_validation_bounds() {
        assert!(
            serde_json::from_str::<RenderInputsPayload>(r#"{"fingerprint":"","tenants":{}}"#)
                .unwrap_or_else(|_| panic!())
                .validate()
                .is_err()
        );
        let big = format!(
            r#"{{"fingerprint":"x","tenants":{{"t":{{"vars":{{"k":"{}"}},"secrets":{{}}}}}}}}"#,
            "v".repeat(MAX_VALUE_LEN + 1)
        );
        let p: RenderInputsPayload = serde_json::from_str(&big).unwrap();
        assert!(p.validate().is_err());
    }

    #[test]
    fn https_required_except_loopback() {
        assert!(HttpRenderInputsSource::new(
            "http://10.0.0.5:3000",
            "c1",
            "t".into(),
            Duration::from_secs(1),
            RetryPlan::default(),
        )
        .is_err());
        assert!(HttpRenderInputsSource::new(
            "http://127.0.0.1:3000",
            "c1",
            "t".into(),
            Duration::from_secs(1),
            RetryPlan::default(),
        )
        .is_ok());
        assert!(HttpRenderInputsSource::new(
            "https://cp.example.com",
            "c1",
            "t".into(),
            Duration::from_secs(1),
            RetryPlan::default(),
        )
        .is_ok());
        assert!(HttpRenderInputsSource::new(
            "ftp://127.0.0.1",
            "c1",
            "t".into(),
            Duration::from_secs(1),
            RetryPlan::default(),
        )
        .is_err());
    }

    #[test]
    fn pg_credentials_landed_in_separate_0600_file() {
        let dir = tmp("pgcred");
        let p: RenderInputsPayload = serde_json::from_str(
            r#"{"fingerprint":"fp","tenants":{"good":{"vars":{},"secrets":{},
               "pg":{"vrtpgcred:t-1":{"token":"jwt","endpoint":"http://gw",
                 "tenantSlug":"good-x","credentialVersion":3,"readOnly":false}}},
               "bad":{"vars":{},"secrets":{},"unavailable":["decrypt-failed"],
               "pg":{"vrtpgcred:t-2":{"token":"jwt2","endpoint":"http://gw2",
                 "tenantSlug":"bad-x","credentialVersion":1,"readOnly":false}}}}}"#,
        )
        .unwrap();
        write_render_inputs(&dir, &p).unwrap();
        let doc: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(PG_CREDENTIALS_FILE)).unwrap(),
        )
        .unwrap();
        // available tenant lands with full payload
        let cred = &doc["tenants"]["good"]["vrtpgcred:t-1"];
        assert_eq!(cred["token"], "jwt");
        assert_eq!(cred["endpoint"], "http://gw");
        assert_eq!(cred["tenantSlug"], "good-x");
        assert_eq!(cred["credentialVersion"], 3);
        assert_eq!(cred["readOnly"], false);
        // unavailable tenant loses the whole pg map (B5 parity)
        assert!(doc["tenants"].get("bad").is_none());
        // credentials never enter the isolate-facing render file
        let ri: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(RENDER_INPUTS_FILE)).unwrap(),
        )
        .unwrap();
        assert!(!ri.to_string().contains("jwt"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.join(PG_CREDENTIALS_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pg_tcp_payload_passes_through_to_provider_file() {
        let dir = tmp("pgtcp");
        let p: RenderInputsPayload = serde_json::from_str(
            r#"{"fingerprint":"fp","tenants":{"t":{"vars":{},"secrets":{},
               "pg":{"vrtdbcred:db-1":{"token":"","endpoint":"",
                 "credentialVersion":7,"readOnly":false,
                 "tcp":{"host":"c-rw.ns.svc.cluster.local","port":5432,
                        "user":"app","password":"pw-xyz","database":"app"}},
                     "vrtpgcred:t-1":{"token":"jwt","endpoint":"http://gw",
                 "tcpBlocked":"migration_read_only"}}}}}"#,
        )
        .unwrap();
        write_render_inputs(&dir, &p).unwrap();
        let doc: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(PG_CREDENTIALS_FILE)).unwrap(),
        )
        .unwrap();
        let tcp = &doc["tenants"]["t"]["vrtdbcred:db-1"]["tcp"];
        assert_eq!(tcp["host"], "c-rw.ns.svc.cluster.local");
        assert_eq!(tcp["port"], 5432);
        assert_eq!(tcp["user"], "app");
        assert_eq!(tcp["password"], "pw-xyz");
        assert_eq!(tcp["database"], "app");
        assert_eq!(doc["tenants"]["t"]["vrtdbcred:db-1"]["credentialVersion"], 7);
        // a ref without tcp carries no tcp key; the block marker survives
        assert!(doc["tenants"]["t"]["vrtpgcred:t-1"].get("tcp").is_none());
        assert_eq!(
            doc["tenants"]["t"]["vrtpgcred:t-1"]["tcpBlocked"],
            "migration_read_only"
        );
        // the password never enters the isolate-facing render file
        let ri = std::fs::read_to_string(dir.join(RENDER_INPUTS_FILE)).unwrap();
        assert!(!ri.contains("pw-xyz"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pg_tcp_unknown_field_fails_closed() {
        // A future wire addition must not be silently discarded.
        let r: Result<RenderInputsPayload, _> = serde_json::from_str(
            r#"{"fingerprint":"fp","tenants":{"t":{"pg":{"r":{"token":"","endpoint":"",
               "tcp":{"host":"h","port":5432,"user":"u","password":"p","database":"d","sslmode":"require"}}}}}}"#,
        );
        assert!(r.is_err());
    }
}
