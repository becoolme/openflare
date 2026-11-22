//! Minimal SMTP relay for the email `smtp` adapter: EHLO → optional
//! STARTTLS/implicit TLS → `AUTH LOGIN` → MAIL/RCPT/DATA → QUIT.
//!
//! Deliberately small: no PIPELINING, no 8BITMIME negotiation —
//! bodies are quoted-printable/base64-encoded 7-bit-safe text, so any
//! RFC5321 relay accepts them. Replies are checked per-line; multi-line
//! replies consume until the `250 `-terminated line. All socket I/O is
//! bounded by the adapter timeout and replies are capped (4KiB/line,
//! 64 lines) — a hostile or hung server cannot stall or flood us.

use crate::email::{EmailError, NormalizedEmail, SmtpTls};
use base64::Engine;
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const MAX_REPLY_LINE: usize = 4096;
const MAX_REPLY_LINES: usize = 64;

enum Stream {
    Plain(BufReader<tokio::net::TcpStream>),
    Tls(BufReader<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>),
}

impl Stream {
    async fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read_line(buf).await,
            Self::Tls(s) => s.read_line(buf).await,
        }
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.get_mut().write_all(bytes).await,
            Self::Tls(s) => s.get_mut().write_all(bytes).await,
        }
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.get_mut().flush().await,
            Self::Tls(s) => s.get_mut().flush().await,
        }
    }
}

fn io_err(e: io::Error) -> EmailError {
    EmailError::unavailable(format!("smtp io: {e}"))
}

fn smtp_reject(code: u16, line: &str) -> EmailError {
    let retryable = (400..500).contains(&code);
    EmailError {
        code: "E_DELIVERY_FAILED",
        message: format!("smtp rejected ({code}): {}", line.trim()),
        retryable,
    }
}

/// Read one (possibly multi-line) reply; return the final code + text.
async fn reply(s: &mut Stream) -> Result<(u16, String), EmailError> {
    let mut last = String::new();
    for _ in 0..MAX_REPLY_LINES {
        last.clear();
        let mut line = String::new();
        let n = s.read_line(&mut line).await.map_err(io_err)?;
        if n == 0 || n > MAX_REPLY_LINE {
            return Err(EmailError::unavailable("smtp reply line lost/oversized"));
        }
        last = line.trim_end().to_string();
        if last.len() < 4 || !last[..3].bytes().all(|b| b.is_ascii_digit()) {
            return Err(EmailError::unavailable("smtp malformed reply"));
        }
        // "250-text" continues; "250 text" terminates.
        if last.as_bytes()[3] == b' ' {
            let code: u16 = last[..3].parse().unwrap_or(500);
            return Ok((code, last));
        }
    }
    Err(EmailError::unavailable("smtp reply never terminated"))
}

async fn expect(s: &mut Stream, sent: &str, want: &[u16]) -> Result<String, EmailError> {
    s.write_all(sent.as_bytes()).await.map_err(io_err)?;
    s.flush().await.map_err(io_err)?;
    let (code, line) = reply(s).await?;
    if !want.contains(&code) {
        return Err(smtp_reject(code, &line));
    }
    Ok(line)
}

fn dot_stuff(body: &str) -> String {
    let mut out = String::with_capacity(body.len() + 64);
    for (i, line) in body.split_inclusive('\n').enumerate() {
        if line.starts_with('.') || (i == 0 && line.starts_with('.')) {
            out.push('.');
        }
        out.push_str(line);
    }
    if !out.ends_with("\r\n") {
        if out.ends_with('\n') {
            out.push('\r');
        } else {
            out.push_str("\r\n");
        }
    }
    out
}

fn encode_header_value(v: &str) -> String {
    // ASCII passes through; anything else is RFC2047 UTF-8 base64.
    if v.is_ascii() {
        return v.replace(['\r', '\n'], " ").to_string();
    }
    format!(
        "=?UTF-8?B?{}?=",
        base64::engine::general_purpose::STANDARD.encode(v.as_bytes())
    )
}

fn mime_addr(a: &crate::email::Addr) -> String {
    match &a.name {
        Some(n) => format!("{} <{}>", encode_header_value(n), a.email),
        None => a.email.clone(),
    }
}

fn boundary() -> String {
    format!(
        "vrt-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    )
}

fn build_mime(msg: &NormalizedEmail) -> String {
    let mut out = String::with_capacity(4096);
    let mut h = |k: &str, v: &str| {
        out.push_str(k);
        out.push_str(": ");
        out.push_str(v);
        out.push_str("\r\n");
    };
    h("From", &mime_addr(&msg.from));
    h(
        "To",
        &msg.to.iter().map(mime_addr).collect::<Vec<_>>().join(", "),
    );
    if !msg.cc.is_empty() {
        h(
            "Cc",
            &msg.cc.iter().map(mime_addr).collect::<Vec<_>>().join(", "),
        );
    }
    if let Some(rt) = &msg.reply_to {
        h("Reply-To", &mime_addr(rt));
    }
    h("Subject", &encode_header_value(&msg.subject));
    h("MIME-Version", "1.0");
    h(
        "Message-ID",
        &format!("<{}.vrt@vrt-provider.local>", boundary()),
    );
    for (k, v) in &msg.headers {
        h(k, &encode_header_value(v));
    }

    let b_mixed = boundary();
    let b_alt = boundary();
    let has_atts = !msg.attachments.is_empty();
    let both = msg.text.is_some() && msg.html.is_some();
    if has_atts {
        h(
            "Content-Type",
            &format!("multipart/mixed; boundary=\"{b_mixed}\""),
        );
    } else if both {
        h(
            "Content-Type",
            &format!("multipart/alternative; boundary=\"{b_alt}\""),
        );
    } else {
        let (ct, body) = match (&msg.text, &msg.html) {
            (Some(t), _) => ("text/plain; charset=utf-8", t.clone()),
            (_, Some(h)) => ("text/html; charset=utf-8", h.clone()),
            _ => unreachable!("validated: one of text/html"),
        };
        h("Content-Type", ct);
        h("Content-Transfer-Encoding", "base64");
        out.push_str("\r\n");
        out.push_str(&wrap_b64(
            &base64::engine::general_purpose::STANDARD.encode(body),
        ));
        return out;
    }
    out.push_str("\r\n");

    let emit_part = |out: &mut String, b: &str, ct: &str, body: &str, extra: &[(&str, &str)]| {
        out.push_str(&format!("--{b}\r\nContent-Type: {ct}\r\n"));
        for (k, v) in extra {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str("Content-Transfer-Encoding: base64\r\n\r\n");
        out.push_str(&wrap_b64(
            &base64::engine::general_purpose::STANDARD.encode(body),
        ));
        out.push_str("\r\n");
    };

    if both {
        // alternative part nested inside mixed (or alone).
        let outer = if has_atts { &b_mixed } else { &b_alt };
        if has_atts {
            out.push_str(&format!(
                "--{b_mixed}\r\nContent-Type: multipart/alternative; boundary=\"{b_alt}\"\r\n\r\n"
            ));
        }
        emit_part(
            &mut out,
            &b_alt,
            "text/plain; charset=utf-8",
            msg.text.as_deref().unwrap_or(""),
            &[],
        );
        emit_part(
            &mut out,
            &b_alt,
            "text/html; charset=utf-8",
            msg.html.as_deref().unwrap_or(""),
            &[],
        );
        out.push_str(&format!("--{outer}--\r\n"));
        let _ = outer;
    } else if let Some(t) = &msg.text {
        emit_part(&mut out, &b_mixed, "text/plain; charset=utf-8", t, &[]);
    } else if let Some(h) = &msg.html {
        emit_part(&mut out, &b_mixed, "text/html; charset=utf-8", h, &[]);
    }

    for att in &msg.attachments {
        let cd = match &att.content_id {
            Some(id) => format!("; content-id=<{id}>"),
            None => String::new(),
        };
        let body = wrap_b64(&base64::engine::general_purpose::STANDARD.encode(&att.content));
        out.push_str(&format!(
            "--{b_mixed}\r\nContent-Type: {mt}; name=\"{fn_}\"\r\nContent-Disposition: {disp}; filename=\"{fn_}\"{cd}\r\nContent-Transfer-Encoding: base64\r\n\r\n{body}\r\n",
            b_mixed = b_mixed,
            mt = att.mime_type,
            fn_ = att.filename.replace('"', ""),
            disp = att.disposition,
            cd = cd,
        ));
    }
    out.push_str(&format!("--{b_mixed}--\r\n"));
    out
}

fn wrap_b64(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + s.len() / 38);
    for chunk in s.as_bytes().chunks(76) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or(""));
        out.push_str("\r\n");
    }
    out
}

/// One relay round-trip. The `mail_from`/`rcpt` lists come from the
/// validated builder (restrictions already applied upstream).
#[allow(clippy::too_many_arguments)]
pub async fn relay(
    host: &str,
    port: u16,
    tls: SmtpTls,
    username: Option<&str>,
    password: Option<&str>,
    helo: &str,
    timeout: std::time::Duration,
    msg: &NormalizedEmail,
) -> Result<String, EmailError> {
    tokio::time::timeout(
        timeout,
        relay_inner(host, port, tls, username, password, helo, msg),
    )
    .await
    .map_err(|_| EmailError::unavailable("smtp relay timed out"))?
}

async fn relay_inner(
    host: &str,
    port: u16,
    tls: SmtpTls,
    username: Option<&str>,
    password: Option<&str>,
    helo: &str,
    msg: &NormalizedEmail,
) -> Result<String, EmailError> {
    let tcp = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(io_err)?;
    tcp.set_nodelay(true).ok();
    let mut s = match tls {
        SmtpTls::Implicit => Stream::Tls(BufReader::new(tls_wrap(tcp, host).await?)),
        _ => Stream::Plain(BufReader::new(tcp)),
    };
    let (code, line) = reply(&mut s).await?;
    if code != 220 {
        return Err(smtp_reject(code, &line));
    }
    let caps = expect(&mut s, &format!("EHLO {helo}\r\n"), &[250]).await?;
    if matches!(tls, SmtpTls::StartTls) {
        if !caps.to_ascii_uppercase().contains("STARTTLS") {
            return Err(EmailError::unavailable("smtp server lacks STARTTLS"));
        }
        expect(&mut s, "STARTTLS\r\n", &[220]).await?;
        let Stream::Plain(reader) = s else {
            return Err(EmailError::unavailable("smtp tls state"));
        };
        let tls_stream = tls_wrap(reader.into_inner(), host).await?;
        s = Stream::Tls(BufReader::new(tls_stream));
        expect(&mut s, &format!("EHLO {helo}\r\n"), &[250]).await?;
    }
    if let (Some(u), Some(p)) = (username, password) {
        let b = &base64::engine::general_purpose::STANDARD;
        expect(&mut s, "AUTH LOGIN\r\n", &[334]).await?;
        expect(&mut s, &format!("{}\r\n", b.encode(u)), &[334]).await?;
        expect(&mut s, &format!("{}\r\n", b.encode(p)), &[235]).await?;
    }
    expect(
        &mut s,
        &format!("MAIL FROM:<{}>\r\n", msg.from.email),
        &[250],
    )
    .await?;
    for a in msg.to.iter().chain(msg.cc.iter()).chain(msg.bcc.iter()) {
        expect(&mut s, &format!("RCPT TO:<{}>\r\n", a.email), &[250, 251]).await?;
    }
    expect(&mut s, "DATA\r\n", &[354]).await?;
    let body = dot_stuff(&build_mime(msg));
    s.write_all(body.as_bytes()).await.map_err(io_err)?;
    s.write_all(b".\r\n").await.map_err(io_err)?;
    s.flush().await.map_err(io_err)?;
    let (code, line) = reply(&mut s).await?;
    let _ = expect(&mut s, "QUIT\r\n", &[221, 250]).await;
    if code != 250 {
        return Err(smtp_reject(code, &line));
    }
    // Queue id out of the 250 reply text when present.
    let id = line
        .split_whitespace()
        .nth(1)
        .filter(|t| t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "smtp-{:x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos() as u64)
            )
        });
    Ok(id)
}

async fn tls_wrap(
    tcp: tokio::net::TcpStream,
    host: &str,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, EmailError> {
    use rustls::pki_types::ServerName;
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from(host.to_string())
        .map_err(|_| EmailError::unavailable("smtp host not a valid TLS name"))?;
    tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
        .connect(name, tcp)
        .await
        .map_err(|e| EmailError::unavailable(format!("smtp tls: {e}")))
}
