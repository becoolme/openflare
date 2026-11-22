//! Read-only worker-log tail client (`of logs`, `of dev` output).
//!
//! The host log hub (`vrt-host/src/logs.rs`) exposes
//! `POST /v1/logs/tail` on its loopback/pod control listener —
//! non-consuming, per-worker, already redacted at ingest. This client
//! resolves the `controlListen` address from `config/vrt-host.toml`
//! and polls with a `(bootId, nextSeq)` cursor: a host restart resets
//! the cursor to 0 instead of silently skipping records.

use std::io::{Read, Write};
use std::path::Path;

use serde::Deserialize;

use crate::LocalError;

/// One buffered record as the hub serializes it.
#[derive(Debug, Clone, serde::Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TailRecord {
    pub seq: u64,
    pub worker_id: String,
    pub ts_ms: i64,
    /// debug | info | log | warn | error | platform
    pub level: String,
    /// log | exception | outcome | platform
    pub kind: String,
    pub message: String,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub status: Option<u16>,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub truncated: bool,
}

/// Wire body of `POST /v1/logs/tail`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailResponse {
    pub boot_id: String,
    pub records: Vec<TailRecord>,
    pub next_seq: u64,
}

/// `controlListen = "addr"` out of `root/config/vrt-host.toml`
/// (minimal line parse — the same convention
/// `serve::host_control_base` uses internally).
pub fn control_listen_addr(root: &Path) -> Result<String, LocalError> {
    let text = std::fs::read_to_string(root.join("config").join("vrt-host.toml"))
        .map_err(|_| LocalError::Layout("vrt-host.toml"))?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("controlListen") {
            let v = rest
                .trim_start_matches([' ', '='])
                .split('#')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"');
            if !v.is_empty() {
                return Ok(v.to_string());
            }
        }
    }
    Err(LocalError::Layout("controlListen"))
}

/// One tail poll. `(boot_id, after)` is the caller's cursor — echo
/// the last response's `bootId` so a host restart resets to 0
/// server-side instead of silently skipping the new buffer. `None`
/// → worker not registered yet (404); callers treat that as "no
/// records", not an error.
pub fn tail_once(
    base: &str,
    worker_id: &str,
    boot_id: Option<&str>,
    after: u64,
    limit: usize,
) -> Result<Option<TailResponse>, LocalError> {
    let body = serde_json::json!({
        "workerId": worker_id,
        "bootId": boot_id,
        "afterSeq": after,
        "limit": limit,
    })
    .to_string();
    let mut stream = std::net::TcpStream::connect(base)
        .map_err(|_| LocalError::Op("logs", "control listener unreachable".into()))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(|_| LocalError::Op("logs", "control listener".into()))?;
    let req = format!(
        "POST /v1/logs/tail HTTP/1.0\r\nhost: {base}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(req.as_bytes())
        .and_then(|_| {
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).map(|_| buf)
        })
        .map_err(|_| LocalError::Op("logs", "control listener io".into()))
        .map(|buf| String::from_utf8_lossy(&buf).to_string())
        .and_then(|text| {
            let (head, body_out) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
            let status: u16 = head
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if status == 404 {
                return Ok(None);
            }
            if !(200..300).contains(&status) {
                return Err(LocalError::Op("logs", format!("tail status {status}")));
            }
            let resp: TailResponse = serde_json::from_str(body_out)
                .map_err(|_| LocalError::Op("logs", "tail body".into()))?;
            Ok(Some(resp))
        })
}

/// `--since` duration → milliseconds. Accepts `Ns`, `Nm`, `Nh`,
/// `Nd` (bare number = seconds).
pub fn parse_since(s: &str) -> Result<i64, LocalError> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1_000i64),
        Some('m') => (&s[..s.len() - 1], 60_000),
        Some('h') => (&s[..s.len() - 1], 3_600_000),
        Some('d') => (&s[..s.len() - 1], 86_400_000),
        _ => (s, 1_000),
    };
    let n: i64 = num
        .trim()
        .parse()
        .map_err(|_| LocalError::Worker("since"))?;
    if n < 0 {
        return Err(LocalError::Worker("since"));
    }
    Ok(n.saturating_mul(mult))
}

/// ANSI color per level — `of dev` output and `of logs` share this.
fn level_color(level: &str, color: bool) -> (&'static str, &'static str) {
    if !color {
        return ("", "");
    }
    let c = match level {
        "error" => "\x1b[31m",
        "warn" => "\x1b[33m",
        "debug" => "\x1b[2m",
        "platform" => "\x1b[35m",
        _ => "",
    };
    (c, if c.is_empty() { "" } else { "\x1b[0m" })
}

/// Human line: `HH:MM:SS.mmm LEVEL message` (local time via chrono —
/// the only formatter in-tree; no new dep).
pub fn format_record(r: &TailRecord, color: bool) -> String {
    let ts = chrono::DateTime::from_timestamp_millis(r.ts_ms)
        .map(|t| {
            t.with_timezone(&chrono::Local)
                .format("%H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_else(|| r.ts_ms.to_string());
    let (c, reset) = level_color(&r.level, color);
    let lvl = format!("{c}{}{reset}", r.level.to_uppercase());
    let mut line = format!("{ts} {lvl} {}", r.message);
    if let Some(code) = &r.code {
        line.push_str(&format!(" [{code}]"));
    }
    if let (Some(outcome), Some(status)) = (&r.outcome, r.status) {
        line.push_str(&format!(" ({outcome} {status})"));
    }
    if r.truncated {
        line.push_str(" [truncated]");
    }
    line
}

/// Client-side `--level` filter.
pub fn level_matches(r: &TailRecord, level: Option<&str>) -> bool {
    match level {
        None => true,
        Some(l) => r.level == l,
    }
}

/// Client-side `--since` filter (wall-clock ms cutoff).
pub fn since_matches(r: &TailRecord, cutoff_ms: i64) -> bool {
    r.ts_ms >= cutoff_ms
}

/// `of logs` / dev-output options.
#[derive(Debug, Clone)]
pub struct LogsOpts {
    /// Keep polling (until killed / `max_polls`).
    pub follow: bool,
    /// Emit one JSON object per record instead of human lines.
    pub json: bool,
    /// `--level` filter (exact match).
    pub level: Option<String>,
    /// `--since` cutoff in wall-clock ms (`i64::MIN` = no filter).
    pub since_ms: i64,
    /// ANSI colors on the human format.
    pub color: bool,
    /// Interval between follow polls.
    pub poll: std::time::Duration,
    /// Cooperative stop for embedded callers (`of dev` sets this on
    /// session shutdown; `of logs` leaves it `None` — Ctrl+C exits).
    pub stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// Test knob — bound the follow loop so it terminates.
    pub max_polls: Option<u64>,
}

impl Default for LogsOpts {
    fn default() -> Self {
        Self {
            follow: false,
            json: false,
            level: None,
            since_ms: i64::MIN,
            color: false,
            poll: std::time::Duration::from_millis(800),
            stop: None,
            max_polls: None,
        }
    }
}

/// The shared print+poll loop — `of logs` runs it on the main
/// thread, `of dev` on a std::thread. `max_polls` bounds it in
/// tests; production callers pass `None` and rely on Ctrl+C.
pub fn logs_run(
    out: &mut dyn Write,
    base: &str,
    worker_id: &str,
    opts: &LogsOpts,
) -> Result<(), LocalError> {
    let mut seq = 0u64;
    let mut boot: Option<String> = None;
    let mut polls = 0u64;
    loop {
        if let Some(resp) = tail_once(base, worker_id, boot.as_deref(), seq, 200)? {
            seq = resp.next_seq;
            boot = Some(resp.boot_id);
            for rec in &resp.records {
                if !since_matches(rec, opts.since_ms) || !level_matches(rec, opts.level.as_deref())
                {
                    continue;
                }
                let line = if opts.json {
                    serde_json::to_string(rec).unwrap_or_default()
                } else {
                    format_record(rec, opts.color)
                };
                let _ = writeln!(out, "{line}");
            }
            let _ = out.flush();
        }
        polls += 1;
        let stopped = opts
            .stop
            .as_ref()
            .is_some_and(|s| s.load(std::sync::atomic::Ordering::Relaxed));
        if !opts.follow || stopped || opts.max_polls.is_some_and(|m| polls >= m) {
            return Ok(());
        }
        std::thread::sleep(opts.poll);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn since_parses_all_units() {
        assert_eq!(parse_since("30s").unwrap(), 30_000);
        assert_eq!(parse_since("5m").unwrap(), 300_000);
        assert_eq!(parse_since("2h").unwrap(), 7_200_000);
        assert_eq!(parse_since("1d").unwrap(), 86_400_000);
        assert_eq!(parse_since("42").unwrap(), 42_000);
        assert!(parse_since("nope").is_err());
        assert!(parse_since("-5s").is_err());
    }

    #[test]
    fn format_includes_level_message_and_flags() {
        let r = TailRecord {
            seq: 1,
            worker_id: "w".into(),
            ts_ms: 0,
            level: "error".into(),
            kind: "exception".into(),
            message: "boom".into(),
            outcome: Some("exception".into()),
            status: Some(500),
            code: Some("x".into()),
            truncated: true,
        };
        let line = format_record(&r, false);
        assert!(line.contains("ERROR"));
        assert!(line.contains("boom"));
        assert!(line.contains("[x]"));
        assert!(line.contains("(exception 500)"));
        assert!(line.contains("[truncated]"));
        assert!(!line.contains('\x1b'));
        let colored = format_record(&r, true);
        assert!(colored.contains("\x1b[31m"));
    }

    #[test]
    fn filters_are_exact() {
        let r = TailRecord {
            seq: 1,
            worker_id: "w".into(),
            ts_ms: 100,
            level: "warn".into(),
            kind: "log".into(),
            message: "m".into(),
            outcome: None,
            status: None,
            code: None,
            truncated: false,
        };
        assert!(level_matches(&r, None));
        assert!(level_matches(&r, Some("warn")));
        assert!(!level_matches(&r, Some("error")));
        assert!(since_matches(&r, 100));
        assert!(!since_matches(&r, 101));
    }
}
