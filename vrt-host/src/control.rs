//! Bounded control-fd event parsing.
//!
//! The tenant process reports socket readiness as JSON lines on its
//! control fd (fd 3). Real workerd **1.20261103.1** was measured on
//! B8/T25 (arm64 container): for a `sock-<workerId>` unix socket the
//! exact emitted line is
//!
//! ```text
//! {"event":"listen","socket":"sock-w_2cf24dba5fb0a30e","port":0}
//! ```
//!
//! — NO `address` field, and `port` is 0 for unix sockets. `address`
//! is therefore optional: when absent the host resolves the endpoint
//! from its own rendered spec (never guessed from the event); when
//! present it must match the spec exactly. Unknown fields are
//! tolerated (workerd may add fields across releases); every known
//! field is still whitelist-validated.

use serde::Deserialize;

/// Hard bounds copied from the upstream parser: a single line is at most
/// 4 KiB, total control output at most 16 KiB.
pub const MAX_LINE: usize = 4 * 1024;
/// Total control output bound.
pub const MAX_TOTAL: usize = 16 * 1024;

/// One socket listen report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenEvent {
    /// Socket name from the capnp (`sock-<workerId>` for tenant workers).
    pub socket: String,
    /// Reported port — 0 for unix sockets (measured on real workerd).
    pub port: u16,
    /// Raw `address` field — OPTIONAL: real workerd omits it for unix
    /// sockets. `None` means "the named socket is listening"; the
    /// endpoint comes from the spec, never from the event.
    pub address: Option<String>,
}

/// Replaceable parser for control-fd bytes. `push` consumes one chunk and
/// returns the events completed by it; malformed input is a hard error
/// (fail closed).
pub trait ControlEventParser: Send {
    /// Feed a chunk; returns events completed by this chunk.
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<ListenEvent>, ControlError>;
}

/// Control channel failures — payloads are constant, never input data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    /// Line or cumulative bound exceeded.
    #[error("control-fd output exceeded the size bound")]
    Oversized,
    /// A line is not a well-formed listen event.
    #[error("control-fd line is not a valid listen event")]
    Malformed,
    /// The same socket reported `listen` twice.
    #[error("control-fd emitted a duplicate listen event")]
    Duplicate,
}

#[derive(Debug, Deserialize)]
struct ListenMessage {
    event: String,
    socket: String,
    port: u64,
    /// Optional — absent on real workerd unix sockets.
    #[serde(default)]
    address: Option<String>,
}

/// JSON-lines parser matching the real workerd format measured on
/// 1.20261103.1: `{"event":"listen","socket":"<name>","port":<n>}` with
/// an OPTIONAL `"address":"<addr>"`. Validation: `event == "listen"`,
/// a sane socket token, `port` fits u16, and a present `address` parses
/// via [`crate::endpoint::parse_endpoint_address`] (`unix:` absolute
/// path or loopback `tcp:`).
#[derive(Debug, Default)]
pub struct JsonLineControlParser {
    buf: Vec<u8>,
    seen: std::collections::HashSet<String>,
    total: usize,
}

impl JsonLineControlParser {
    /// Fresh parser.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlEventParser for JsonLineControlParser {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<ListenEvent>, ControlError> {
        self.total = self.total.saturating_add(chunk.len());
        if self.total > MAX_TOTAL {
            return Err(ControlError::Oversized);
        }
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        loop {
            let Some(idx) = self.buf.iter().position(|b| *b == b'\n') else {
                if self.buf.len() > MAX_LINE {
                    return Err(ControlError::Oversized);
                }
                break;
            };
            let mut line: Vec<u8> = self.buf.drain(..=idx).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                continue;
            }
            if line.len() > MAX_LINE {
                return Err(ControlError::Oversized);
            }
            let event = parse_line(&line)?;
            if !self.seen.insert(event.socket.clone()) {
                return Err(ControlError::Duplicate);
            }
            events.push(event);
        }
        Ok(events)
    }
}

fn parse_line(line: &[u8]) -> Result<ListenEvent, ControlError> {
    let text = std::str::from_utf8(line).map_err(|_| ControlError::Malformed)?;
    let msg: ListenMessage = serde_json::from_str(text).map_err(|_| ControlError::Malformed)?;
    if msg.event != "listen" {
        return Err(ControlError::Malformed);
    }
    // Worker ids are `<teamId>:<workerName>` on the control plane, and the
    // socket name is `sock-<workerId>` — allow the separator and a bound that
    // covers two maximum-length identifiers plus the prefix.
    if msg.socket.is_empty()
        || msg.socket.len() > 256
        || !msg
            .socket
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b':')
    {
        return Err(ControlError::Malformed);
    }
    if msg.port > u16::MAX as u64 {
        return Err(ControlError::Malformed);
    }
    if let Some(address) = &msg.address {
        if crate::endpoint::parse_endpoint_address(address).is_none() {
            return Err(ControlError::Malformed);
        }
    }
    Ok(ListenEvent {
        socket: msg.socket,
        port: msg.port as u16,
        address: msg.address,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(socket: &str, address: &str) -> String {
        format!(
            "{{\"event\":\"listen\",\"socket\":\"{socket}\",\"port\":0,\"address\":\"{address}\"}}\n"
        )
    }

    /// Real workerd 1.20261103.1 output — verbatim B8 fixture.
    fn real_line(socket: &str) -> String {
        format!("{{\"event\":\"listen\",\"socket\":\"{socket}\",\"port\":0}}\n")
    }

    #[test]
    fn parses_listen_events_across_chunks() {
        let mut p = JsonLineControlParser::new();
        let data = line("sock-w1", "tcp:127.0.0.1:1234") + &line("sock-w2", "unix:/tmp/x.sock");
        let mut events = Vec::new();
        for chunk in data.as_bytes().chunks(7) {
            events.extend(p.push(chunk).unwrap());
        }
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].socket, "sock-w1");
        assert_eq!(events[1].socket, "sock-w2");
    }

    #[test]
    fn accepts_platform_worker_ids_with_colons() {
        let mut p = JsonLineControlParser::new();
        let events = p
            .push(line("sock-team-1:edge", "tcp:127.0.0.1:1").as_bytes())
            .unwrap();
        assert_eq!(events[0].socket, "sock-team-1:edge");
    }

    #[test]
    fn rejects_malformed_and_dupes() {
        let mut p = JsonLineControlParser::new();
        assert_eq!(p.push(b"not-json\n"), Err(ControlError::Malformed));
        let mut p = JsonLineControlParser::new();
        assert_eq!(
            p.push(line("sock-w1", "tcp:127.0.0.1:1").as_bytes()),
            Ok(vec![ListenEvent {
                socket: "sock-w1".into(),
                port: 0,
                address: Some("tcp:127.0.0.1:1".into())
            }])
        );
        assert_eq!(
            p.push(line("sock-w1", "tcp:127.0.0.1:1").as_bytes()),
            Err(ControlError::Duplicate)
        );
    }

    #[test]
    fn rejects_oversize_and_bad_fields() {
        let mut p = JsonLineControlParser::new();
        // One line > 4 KiB without newline → Oversized.
        let big = vec![b'x'; MAX_LINE + 1];
        assert_eq!(p.push(&big), Err(ControlError::Oversized));

        let mut p = JsonLineControlParser::new();
        // >16 KiB cumulative.
        let mut line1 = line("sock-w1", "tcp:127.0.0.1:1").into_bytes();
        line1.extend(vec![b' '; MAX_TOTAL]);
        line1.push(b'\n');
        assert_eq!(p.push(&line1), Err(ControlError::Oversized));

        // Wrong event type / socket charset / non-loopback address.
        let mut p = JsonLineControlParser::new();
        assert_eq!(
            p.push(b"{\"event\":\"other\",\"socket\":\"sock-w1\",\"port\":0,\"address\":\"tcp:127.0.0.1:1\"}\n"),
            Err(ControlError::Malformed)
        );
        let mut p = JsonLineControlParser::new();
        assert_eq!(
            p.push(line("sock w1", "tcp:127.0.0.1:1").as_bytes()),
            Err(ControlError::Malformed)
        );
        let mut p = JsonLineControlParser::new();
        assert_eq!(
            p.push(line("sock-w1", "tcp:10.0.0.1:80").as_bytes()),
            Err(ControlError::Malformed)
        );
    }

    // ---- T25/M1: real workerd format (no `address`) ----

    #[test]
    fn accepts_real_workerd_line_without_address() {
        // Verbatim bytes emitted by real workerd 1.20261103.1 on fd 3
        // for a unix socket (B8 evidence, control-fd.txt).
        let mut p = JsonLineControlParser::new();
        let events = p
            .push(b"{\"event\":\"listen\",\"socket\":\"sock-w_2cf24dba5fb0a30e\",\"port\":0}\n")
            .unwrap();
        assert_eq!(
            events,
            vec![ListenEvent {
                socket: "sock-w_2cf24dba5fb0a30e".into(),
                port: 0,
                address: None,
            }]
        );
    }

    #[test]
    fn mixed_real_and_addressed_lines_parse() {
        let mut p = JsonLineControlParser::new();
        let data = real_line("sock-w1") + &line("sock-w2", "unix:/tmp/x.sock");
        let events = p.push(data.as_bytes()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].address, None);
        assert_eq!(events[1].address.as_deref(), Some("unix:/tmp/x.sock"));
    }

    #[test]
    fn unknown_fields_tolerated_known_fields_validated() {
        // Future workerd may add fields — tolerated; known fields stay
        // whitelist-checked.
        let mut p = JsonLineControlParser::new();
        let events = p
            .push(b"{\"event\":\"listen\",\"socket\":\"sock-w1\",\"port\":0,\"futureField\":{\"x\":1}}\n")
            .unwrap();
        assert_eq!(events.len(), 1);
        // …but a bad KNOWN field still fails closed.
        let mut p = JsonLineControlParser::new();
        assert_eq!(
            p.push(b"{\"event\":\"listen\",\"socket\":\"sock-w1\",\"port\":70000}\n"),
            Err(ControlError::Malformed)
        );
        let mut p = JsonLineControlParser::new();
        assert_eq!(
            p.push(b"{\"event\":\"listen\",\"socket\":\"\",\"port\":0}\n"),
            Err(ControlError::Malformed)
        );
    }
}
