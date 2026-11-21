//! Email send data plane: facade-email -> provider -> upstream adapter.
//!
//! Unlike kv/r2/d1/k2, there is no VRT storage behind this binding —
//! the provider relays `env.<name>.send(msg)` to a configured upstream
//! (`[email] backend = resend|smtp|cf|log`). The CF send-bindings
//! restrictions (`destination_address`, `allowed_destination_addresses`,
//! `allowed_sender_addresses`) are enforced HERE from the signed
//! desired state — the worker only learns the outcome as a thrown
//! Error with `.code` (CF contract: TypeError for local misuse,
//! E_* codes for service rejections).
//!
//! Inbound wire (from the shim, JSON):
//!   {from:{email,name?}, to:[addr], cc:[addr], bcc:[addr],
//!    subject, html?, text?, replyTo?: addr,
//!    attachments:[{filename,type,disposition,contentId?,content(b64)}],
//!    headers?}
//! `addr` = {email, name?} — the shim canonicalizes string|object.
//!
//! Outbound wire (to the shim): `{success:true, messageId}` or
//! `{success:false, error:{code, message, retryable}}` — the shim
//! throws `Error(message)` carrying `.code` on failure.

use crate::context::BindingResource;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// ---- CF public limits (email-service/platform/limits) ----
/// Combined to+cc+bcc recipients per send.
pub const EMAIL_MAX_RECIPIENTS: usize = 50;
/// Attachments per send.
pub const EMAIL_MAX_ATTACHMENTS: usize = 32;
/// Total message size (CF 25MiB); enforced on decoded bytes.
pub const EMAIL_MAX_MESSAGE_BYTES: usize = 25 * 1024 * 1024;
/// Provider ingress body cap: base64 inflates ~4/3 (25MiB -> ~34MiB).
pub const EMAIL_MAX_BODY_BYTES: usize = 36 * 1024 * 1024;
/// Custom header bounds.
pub const EMAIL_MAX_HEADERS: usize = 20;
pub const EMAIL_HEADER_NAME_MAX: usize = 100;
pub const EMAIL_HEADER_VALUE_MAX: usize = 2048;
pub const EMAIL_HEADERS_MAX_BYTES: usize = 16 * 1024;
/// Reply envelope is `{messageId}`-sized.
pub const EMAIL_MAX_RESPONSE_BYTES: usize = 64 * 1024;

// ---- inbound wire types ----

/// Canonical address (`{email, name?}`) — the shim already folded
/// `string | EmailAddress` into this shape.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Addr {
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Attachment {
    filename: String,
    #[serde(rename = "type")]
    mime_type: String,
    disposition: String,
    #[serde(default)]
    content_id: Option<String>,
    /// Base64 content (the shim encodes binary; strings pass as-is).
    content: String,
}

/// The normalized builder shape (EmailMessageBuilder post-shim).
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendReq {
    from: Addr,
    #[serde(default)]
    to: Vec<Addr>,
    #[serde(default)]
    cc: Vec<Addr>,
    #[serde(default)]
    bcc: Vec<Addr>,
    subject: String,
    #[serde(default)]
    html: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    reply_to: Option<Addr>,
    #[serde(default)]
    attachments: Vec<Attachment>,
    #[serde(default)]
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

/// What the adapters consume — fully validated, restrictions applied.
#[derive(Clone, Debug)]
pub struct NormalizedEmail {
    pub from: Addr,
    pub to: Vec<Addr>,
    pub cc: Vec<Addr>,
    pub bcc: Vec<Addr>,
    pub subject: String,
    pub html: Option<String>,
    pub text: Option<String>,
    pub reply_to: Option<Addr>,
    pub attachments: Vec<NormalizedAttachment>,
    /// Allowlisted custom headers (reserved names already stripped).
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct NormalizedAttachment {
    pub filename: String,
    pub mime_type: String,
    pub disposition: String,
    pub content_id: Option<String>,
    pub content: Vec<u8>,
}

// ---- typed error (CF E_* codes) ----

/// Backend/validation failure carrying a CF error code; the dispatch
/// maps it to the `{success:false,error:{code,...}}` envelope.
#[derive(Clone, Debug)]
pub struct EmailError {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl EmailError {
    fn code(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
        }
    }
    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: "E_INTERNAL_SERVER_ERROR",
            message: message.into(),
            retryable: true,
        }
    }
}

// ---- validation ----

fn valid_addr(addr: &Addr) -> bool {
    let (local, domain) = match addr.email.split_once('@') {
        Some(t) => t,
        None => return false,
    };
    !local.is_empty()
        && local.len() <= 64
        && addr.email.len() <= 254
        && domain.contains('.')
        && addr.email.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'@' | b'.' | b'-' | b'_' | b'+' | b'%')
        })
        // Header-injection: no control bytes anywhere in the display name.
        && addr
            .name
            .as_ref()
            .is_none_or(|n| n.len() <= 998 && !n.bytes().any(|b| b < 0x20 || b == 0x7f))
}

/// Headers the platform controls — CF rejects them in the custom
/// `headers` map (E_HEADER_USE_API_FIELD / E_HEADER_NOT_ALLOWED).
const RESERVED_HEADERS: &[&str] = &[
    "from",
    "to",
    "cc",
    "bcc",
    "subject",
    "reply-to",
    "message-id",
    "date",
    "mime-version",
    "content-type",
    "content-transfer-encoding",
    "dkim-signature",
    "return-path",
    "received",
];

fn decode_base64(s: &str) -> Result<Vec<u8>, ()> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .map_err(|_| ())
}

/// Validate the inbound body into a `NormalizedEmail`. Returns the
/// typed rejection envelope payload `(code, message)` on failure —
/// every bad-input path maps to a CF E_* code, never a bare 400.
pub fn validate_send(body: &[u8]) -> Result<NormalizedEmail, EmailError> {
    let req: SendReq = serde_json::from_slice(body)
        .map_err(|_| EmailError::code("E_VALIDATION_ERROR", "malformed send body"))?;
    if req.subject.len() > 998 {
        return Err(EmailError::code(
            "E_VALIDATION_ERROR",
            "subject exceeds 998 bytes",
        ));
    }
    if req.html.is_none() && req.text.is_none() {
        return Err(EmailError::code(
            "E_VALIDATION_ERROR",
            "one of text or html content is required",
        ));
    }
    let mut total_bytes = req.subject.len()
        + req.html.as_deref().map_or(0, str::len)
        + req.text.as_deref().map_or(0, str::len);
    if req.to.len() + req.cc.len() + req.bcc.len() == 0 {
        return Err(EmailError::code(
            "E_VALIDATION_ERROR",
            "at least one of to, cc, or bcc is required",
        ));
    }
    if req.to.len() + req.cc.len() + req.bcc.len() > EMAIL_MAX_RECIPIENTS {
        return Err(EmailError::code(
            "E_TOO_MANY_RECIPIENTS",
            "combined recipients exceed 50",
        ));
    }
    if req.attachments.len() > EMAIL_MAX_ATTACHMENTS {
        return Err(EmailError::code(
            "E_TOO_MANY_ATTACHMENTS",
            "attachments array exceeds 32 entries",
        ));
    }
    if !valid_addr(&req.from) {
        return Err(EmailError::code(
            "E_VALIDATION_ERROR",
            "invalid from address",
        ));
    }
    for addr in req
        .to
        .iter()
        .chain(req.cc.iter())
        .chain(req.bcc.iter())
        .chain(req.reply_to.iter())
    {
        if !valid_addr(addr) {
            return Err(EmailError::code(
                "E_VALIDATION_ERROR",
                "invalid recipient/reply-to address",
            ));
        }
    }
    let mut attachments = Vec::with_capacity(req.attachments.len());
    for att in &req.attachments {
        if att.filename.is_empty()
            || att.filename.len() > 256
            || !matches!(att.disposition.as_str(), "attachment" | "inline")
        {
            return Err(EmailError::code(
                "E_VALIDATION_ERROR",
                "attachment filename/disposition invalid",
            ));
        }
        let content = decode_base64(&att.content)
            .map_err(|_| EmailError::code("E_VALIDATION_ERROR", "attachment not base64"))?;
        total_bytes += content.len();
        if total_bytes > EMAIL_MAX_MESSAGE_BYTES {
            return Err(EmailError::code(
                "E_CONTENT_TOO_LARGE",
                "message exceeds 25MiB",
            ));
        }
        attachments.push(NormalizedAttachment {
            filename: att.filename.clone(),
            mime_type: att.mime_type.clone(),
            disposition: att.disposition.clone(),
            content_id: att.content_id.clone(),
            content,
        });
    }
    let mut headers = Vec::new();
    let mut headers_bytes = 0usize;
    if let Some(h) = req.headers {
        for (name, value) in &h {
            let Some(value) = value.as_str() else {
                return Err(EmailError::code(
                    "E_HEADER_VALUE_INVALID",
                    "header values must be strings",
                ));
            };
            let lname = name.to_ascii_lowercase();
            if name.is_empty()
                || name.len() > EMAIL_HEADER_NAME_MAX
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            {
                return Err(EmailError::code(
                    "E_HEADER_NAME_INVALID",
                    "header name invalid",
                ));
            }
            if RESERVED_HEADERS.contains(&lname.as_str()) {
                return Err(EmailError::code(
                    "E_HEADER_USE_API_FIELD",
                    "reserved header must use its API field",
                ));
            }
            if value.is_empty()
                || value.len() > EMAIL_HEADER_VALUE_MAX
                || value.bytes().any(|b| b < 0x20 || b == 0x7f)
            {
                return Err(EmailError::code(
                    "E_HEADER_VALUE_INVALID",
                    "header value invalid",
                ));
            }
            headers_bytes += name.len() + value.len() + 4;
            headers.push((name.clone(), value.to_string()));
        }
        if headers
            .iter()
            .filter(|(n, _)| !n.to_ascii_lowercase().starts_with("x-"))
            .count()
            > EMAIL_MAX_HEADERS
        {
            return Err(EmailError::code(
                "E_HEADERS_TOO_MANY",
                "more than 20 non-X custom headers",
            ));
        }
        if headers_bytes > EMAIL_HEADERS_MAX_BYTES {
            return Err(EmailError::code(
                "E_HEADERS_TOO_LARGE",
                "custom headers exceed 16KiB",
            ));
        }
    }
    Ok(NormalizedEmail {
        from: req.from,
        to: req.to,
        cc: req.cc,
        bcc: req.bcc,
        subject: req.subject,
        html: req.html,
        text: req.text,
        reply_to: req.reply_to,
        attachments,
        headers,
    })
}

/// CF send-bindings restrictions, enforced from the signed binding —
/// `destination_address` pins every send; `allowed_*` are allowlists.
/// Exact address match (CF semantics), case-insensitive on domain.
pub fn enforce_restrictions(
    resource: &BindingResource,
    msg: &NormalizedEmail,
) -> Result<(), EmailError> {
    let BindingResource::SendEmail {
        destination_address,
        allowed_destination_addresses,
        allowed_sender_addresses,
        ..
    } = resource
    else {
        return Err(EmailError::unavailable("binding is not sendEmail"));
    };
    let same = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    if !allowed_sender_addresses.is_empty()
        && !allowed_sender_addresses
            .iter()
            .any(|a| same(a, &msg.from.email))
    {
        return Err(EmailError::code(
            "E_SENDER_NOT_VERIFIED",
            "sender address not in allowed_sender_addresses",
        ));
    }
    let all_recipients = || {
        msg.to
            .iter()
            .chain(msg.cc.iter())
            .chain(msg.bcc.iter())
            .map(|a| a.email.as_str())
    };
    if let Some(pin) = destination_address {
        if !all_recipients().all(|e| same(e, pin)) {
            return Err(EmailError::code(
                "E_RECIPIENT_NOT_ALLOWED",
                "binding destination_address pins every recipient",
            ));
        }
    } else if !allowed_destination_addresses.is_empty()
        && !all_recipients().all(|e| allowed_destination_addresses.iter().any(|a| same(a, e)))
    {
        return Err(EmailError::code(
            "E_RECIPIENT_NOT_ALLOWED",
            "recipient not in allowed_destination_addresses",
        ));
    }
    Ok(())
}

// ---- backend adapters ----

/// One upstream implementation. Configured per provider instance via
/// `[email]` — per-binding routing can layer on later without
/// changing this trait.
#[async_trait::async_trait]
pub trait EmailBackend: Send + Sync {
    /// Deliver the message; return the upstream message id.
    async fn send(&self, msg: &NormalizedEmail) -> Result<String, EmailError>;
}

/// Dev adapter: logs + returns a local id — never delivers.
pub struct LogBackend;
#[async_trait::async_trait]
impl EmailBackend for LogBackend {
    async fn send(&self, msg: &NormalizedEmail) -> Result<String, EmailError> {
        tracing::info!(
            to = ?msg.to.iter().map(|a| &a.email).collect::<Vec<_>>(),
            subject = %msg.subject,
            "email backend=log: captured send (not delivered)"
        );
        Ok(format!(
            "log-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64)
        ))
    }
}

/// Resend HTTPS adapter — `POST {endpoint}/emails` with
/// `Authorization: Bearer <apiKeyEnv>`.
pub struct ResendBackend {
    client: reqwest::Client,
    endpoint: String,
    api_key: String,
}

impl ResendBackend {
    pub fn new(endpoint: String, api_key: String, timeout: Duration) -> Result<Self, EmailError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| EmailError::unavailable(format!("resend client: {e}")))?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_key,
        })
    }
}

fn resend_addr(a: &Addr) -> String {
    match &a.name {
        Some(n) => format!("{n} <{}>", a.email),
        None => a.email.clone(),
    }
}

#[async_trait::async_trait]
impl EmailBackend for ResendBackend {
    async fn send(&self, msg: &NormalizedEmail) -> Result<String, EmailError> {
        use base64::Engine;
        let body = serde_json::json!({
            "from": resend_addr(&msg.from),
            "to": msg.to.iter().map(resend_addr).collect::<Vec<_>>(),
            "cc": msg.cc.iter().map(resend_addr).collect::<Vec<_>>(),
            "bcc": msg.bcc.iter().map(resend_addr).collect::<Vec<_>>(),
            "subject": msg.subject,
            "html": msg.html,
            "text": msg.text,
            "reply_to": msg.reply_to.as_ref().map(resend_addr),
            "attachments": msg.attachments.iter().map(|a| serde_json::json!({
                "filename": a.filename,
                "content": base64::engine::general_purpose::STANDARD.encode(&a.content),
                "content_type": a.mime_type,
                "content_id": a.content_id,
            })).collect::<Vec<_>>(),
            "headers": msg.headers.iter().cloned().map(|(k,v)| (k, serde_json::Value::String(v))).collect::<serde_json::Map<String, serde_json::Value>>(),
        });
        let resp = self
            .client
            .post(format!("{}/emails", self.endpoint))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| EmailError::unavailable(format!("resend unreachable: {e}")))?;
        let status = resp.status();
        let bytes = resp.bytes().await.unwrap_or_default();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        if status.is_success() {
            let id = v
                .get("id")
                .and_then(|x| x.as_str())
                .unwrap_or("resend-accepted")
                .to_string();
            return Ok(id);
        }
        // Upstream rejections map onto the CF code surface.
        let msg_up = v
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("resend rejected the send")
            .to_string();
        Err(match status.as_u16() {
            401 | 403 => EmailError::unavailable("resend credential rejected"),
            422 => EmailError::code("E_VALIDATION_ERROR", msg_up),
            429 => EmailError {
                code: "E_RATE_LIMIT_EXCEEDED",
                message: msg_up,
                retryable: true,
            },
            _ => EmailError::code("E_DELIVERY_FAILED", msg_up),
        })
    }
}

/// Cloudflare Email Service REST adapter —
/// `POST {endpoint}/accounts/{accountId}/email/send` (API token auth).
pub struct CfEmailBackend {
    client: reqwest::Client,
    endpoint: String,
    api_token: String,
}

impl CfEmailBackend {
    pub fn new(endpoint: String, api_token: String, timeout: Duration) -> Result<Self, EmailError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| EmailError::unavailable(format!("cf client: {e}")))?;
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            api_token,
        })
    }
}

#[async_trait::async_trait]
impl EmailBackend for CfEmailBackend {
    async fn send(&self, msg: &NormalizedEmail) -> Result<String, EmailError> {
        use base64::Engine;
        let body = serde_json::json!({
            "from": resend_addr(&msg.from),
            "to": msg.to.iter().map(resend_addr).collect::<Vec<_>>(),
            "cc": msg.cc.iter().map(resend_addr).collect::<Vec<_>>(),
            "bcc": msg.bcc.iter().map(resend_addr).collect::<Vec<_>>(),
            "subject": msg.subject,
            "html": msg.html,
            "text": msg.text,
            "replyTo": msg.reply_to.as_ref().map(resend_addr),
            "attachments": msg.attachments.iter().map(|a| serde_json::json!({
                "filename": a.filename,
                "content": base64::engine::general_purpose::STANDARD.encode(&a.content),
                "type": a.mime_type,
                "disposition": a.disposition,
                "contentId": a.content_id,
            })).collect::<Vec<_>>(),
            "headers": msg.headers.iter().cloned().map(|(k,v)| (k, serde_json::Value::String(v))).collect::<serde_json::Map<String, serde_json::Value>>(),
        });
        let resp = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_token)
            .json(&body)
            .send()
            .await
            .map_err(|e| EmailError::unavailable(format!("cf email unreachable: {e}")))?;
        let status = resp.status();
        let bytes = resp.bytes().await.unwrap_or_default();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        if status.is_success() {
            let id = v
                .get("result")
                .and_then(|r| r.get("messageId"))
                .and_then(|m| m.as_str())
                .or_else(|| v.get("messageId").and_then(|m| m.as_str()))
                .unwrap_or("cf-accepted")
                .to_string();
            return Ok(id);
        }
        // CF's own errors already carry codes — pass them through.
        let code = v
            .pointer("/errors/0/code_name")
            .and_then(|c| c.as_str())
            .or_else(|| v.pointer("/errors/0/code").and_then(|c| c.as_str()))
            .unwrap_or("E_DELIVERY_FAILED");
        let message = v
            .pointer("/errors/0/message")
            .and_then(|m| m.as_str())
            .unwrap_or("cloudflare email rejected the send")
            .to_string();
        Err(EmailError {
            code: Box::leak(code.to_string().into_boxed_str()),
            message,
            retryable: status.is_server_error() || status.as_u16() == 429,
        })
    }
}

/// Standard SMTP relay adapter — minimal client over
/// TcpStream/{plain,starttls,implicit}. `AUTH LOGIN` when credentials
/// are configured; no PIPELINING/8BITMIME beyond what the greeting
/// advertises (ASCII-quoted bodies stay 7-bit-safe via base64/qp).
pub struct SmtpBackend {
    host: String,
    port: u16,
    tls: SmtpTls,
    username: Option<String>,
    password: Option<String>,
    helo: String,
    timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub enum SmtpTls {
    Plain,
    StartTls,
    Implicit,
}

impl SmtpBackend {
    pub fn new(
        host: String,
        port: u16,
        tls: SmtpTls,
        username: Option<String>,
        password: Option<String>,
        helo: String,
        timeout: Duration,
    ) -> Self {
        Self {
            host,
            port,
            tls,
            username,
            password,
            helo,
            timeout,
        }
    }
}

#[async_trait::async_trait]
impl EmailBackend for SmtpBackend {
    async fn send(&self, msg: &NormalizedEmail) -> Result<String, EmailError> {
        crate::smtp::relay(
            &self.host,
            self.port,
            self.tls,
            self.username.as_deref(),
            self.password.as_deref(),
            &self.helo,
            self.timeout,
            msg,
        )
        .await
    }
}

/// Resolved `[email]` backend selection (built once at startup).
pub enum EmailAdapter {
    Log(LogBackend),
    Resend(ResendBackend),
    Cf(CfEmailBackend),
    Smtp(SmtpBackend),
}

#[async_trait::async_trait]
impl EmailBackend for EmailAdapter {
    async fn send(&self, msg: &NormalizedEmail) -> Result<String, EmailError> {
        match self {
            Self::Log(b) => b.send(msg).await,
            Self::Resend(b) => b.send(msg).await,
            Self::Cf(b) => b.send(msg).await,
            Self::Smtp(b) => b.send(msg).await,
        }
    }
}

/// Build the adapter from config; secrets resolve from the named env
/// vars (never inline values in the config file).
pub fn adapter_from_config(
    cfg: &crate::config::EmailConfig,
) -> Result<Arc<dyn EmailBackend>, String> {
    let timeout = Duration::from_millis(cfg.timeout_ms);
    let env_secret = |name: &str, what: &str| -> Result<String, String> {
        std::env::var(name)
            .map_err(|_| format!("email {what}: env {name} unset"))
            .and_then(|v| {
                if v.is_empty() {
                    Err(format!("email {what}: env {name} empty"))
                } else {
                    Ok(v)
                }
            })
    };
    let backend: Arc<dyn EmailBackend> = match cfg.backend.as_str() {
        "log" => Arc::new(LogBackend),
        "resend" => {
            let key = env_secret(&cfg.api_key_env, "resend apiKey")?;
            Arc::new(
                ResendBackend::new(
                    cfg.endpoint
                        .clone()
                        .unwrap_or_else(|| "https://api.resend.com".into()),
                    key,
                    timeout,
                )
                .map_err(|e| e.message)?,
            )
        }
        "cf" => {
            let token = env_secret(&cfg.api_key_env, "cf apiToken")?;
            let endpoint = cfg.endpoint.clone().ok_or_else(|| {
                "email backend=cf requires `endpoint` (the account send URL)".to_string()
            })?;
            Arc::new(CfEmailBackend::new(endpoint, token, timeout).map_err(|e| e.message)?)
        }
        "smtp" => {
            let host = cfg
                .host
                .clone()
                .ok_or_else(|| "email backend=smtp requires `host`".to_string())?;
            let tls = match cfg.tls.as_str() {
                "plain" => SmtpTls::Plain,
                "starttls" => SmtpTls::StartTls,
                "implicit" => SmtpTls::Implicit,
                other => {
                    return Err(format!(
                        "email tls `{other}` invalid (plain|starttls|implicit)"
                    ))
                }
            };
            let (user, pass) = match (&cfg.username_env, &cfg.password_env) {
                (Some(u), Some(p)) => (
                    Some(env_secret(u, "smtp username")?),
                    Some(env_secret(p, "smtp password")?),
                ),
                (None, None) => (None, None),
                _ => {
                    return Err(
                        "email smtp: usernameEnv and passwordEnv must be set together".into(),
                    )
                }
            };
            Arc::new(SmtpBackend::new(
                host,
                cfg.port.unwrap_or(match tls {
                    SmtpTls::Implicit => 465,
                    _ => 587,
                }),
                tls,
                user,
                pass,
                cfg.helo
                    .clone()
                    .unwrap_or_else(|| "vrt-provider.local".into()),
                timeout,
            ))
        }
        other => {
            return Err(format!(
                "email backend `{other}` invalid (log|resend|cf|smtp)"
            ))
        }
    };
    Ok(backend)
}
