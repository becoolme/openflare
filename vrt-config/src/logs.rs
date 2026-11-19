//! Worker log collection: the platform-owned per-worker tail worker.
//!
//! workerd (`Worker.tails`) delivers every invocation's `TraceItem`
//! (console logs, uncaught exceptions, request outcome) to a tail
//! handler AFTER the tenant request finished — a slow or broken
//! collector can never delay or fail a tenant request. Each tenant
//! worker gets its own `vrt-tail-<workerId>` service whose
//! `WORKER_ID` / `TOKEN` bindings are rendered from trusted config
//! (never derived from tenant-controlled data), so a log record can
//! only ever be attributed to the worker the tail was attached to.
//!
//! The tail worker has a deny-all `globalOutbound` and exactly one
//! service binding: the `vrt-logsink` external → vrt-host control
//! listener `/v1/logs/ingest`.

/// External service name for the vrt-host log sink.
pub const LOGSINK_SERVICE: &str = "vrt-logsink";

/// Hard per-message cap (bytes) applied by BOTH the tail worker and
/// the host hub (the hub is authoritative; the worker cap only keeps
/// the batch small).
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024;

/// Max records the tail worker puts into a single ingest call.
pub const MAX_BATCH_RECORDS: usize = 100;

/// Fail-closed sink parameters. Empty `sink_addr` => log collection
/// disabled and the render is byte-identical to the legacy output.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct LogTailParams {
    /// `host:port` of the vrt-host control listener.
    pub sink_addr: String,
    /// Host-process pepper for [`log_token`]. Required when
    /// `sink_addr` is set (validated at render).
    pub pepper: String,
}

impl LogTailParams {
    /// True when tail workers should be rendered.
    pub fn enabled(&self) -> bool {
        !self.sink_addr.is_empty()
    }
}

impl std::fmt::Debug for LogTailParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogTailParams")
            .field("sink_addr", &self.sink_addr)
            .field("pepper", &"<redacted>")
            .finish()
    }
}

/// `vrt-tail-<workerId>` capnp service name.
pub fn tail_service_name(worker_id: &str) -> String {
    format!("vrt-tail-{worker_id}")
}

/// Ingest token = HMAC-SHA256(pepper, "vrt-log:" + workerId), hex.
/// Per-process random pepper => rotates on host restart and cannot be
/// derived from any DB row.
pub fn log_token(pepper: &str, worker_id: &str) -> String {
    hex::encode(crate::workflows::hmac_sha256(
        pepper.as_bytes(),
        format!("vrt-log:{worker_id}").as_bytes(),
    ))
}

/// The tail handler. Never throws, bounds every string, and posts one
/// batch per invocation group. Identity comes from the config-rendered
/// `WORKER_ID` / `TOKEN` bindings only.
pub const TAIL_WORKER_JS: &str = r#"const MAX = 8192;
const MAX_RECORDS = 100;
function cut(s) {
  s = String(s);
  return s.length > MAX ? s.slice(0, MAX) : s;
}
function fmt(v) {
  if (typeof v === "string") return v;
  try {
    const j = JSON.stringify(v);
    return j === undefined ? String(v) : j;
  } catch (_) {
    return "[unserializable]";
  }
}
export default {
  async tail(events, env) {
    try {
      const records = [];
      for (const ev of events || []) {
        const base = ev.eventTimestamp || Date.now();
        for (const l of ev.logs || []) {
          if (records.length >= MAX_RECORDS) break;
          const msg = Array.isArray(l.message) ? l.message.map(fmt).join(" ") : fmt(l.message);
          records.push({ kind: "log", level: String(l.level || "log"), ts: l.timestamp || base, message: cut(msg) });
        }
        for (const x of ev.exceptions || []) {
          if (records.length >= MAX_RECORDS) break;
          const stack = typeof x.stack === "string" ? "\n" + x.stack : "";
          records.push({ kind: "exception", level: "error", ts: x.timestamp || base, message: cut(String(x.name || "Error") + ": " + String(x.message || "") + stack) });
        }
        if (records.length < MAX_RECORDS) {
          const req = ev.event && ev.event.request;
          const res = ev.event && ev.event.response;
          const m = req ? String(req.method || "") + " " + String(req.url || "") : "";
          records.push({
            kind: "outcome",
            level: ev.outcome === "ok" ? "info" : "error",
            ts: base,
            message: cut(m),
            outcome: String(ev.outcome || ""),
            status: res && typeof res.status === "number" ? res.status : null,
          });
        }
      }
      if (records.length === 0) return;
      await env.SINK.fetch("http://vrt-logsink/v1/logs/ingest", {
        method: "POST",
        headers: { "content-type": "application/json", "x-vrt-log-worker": env.WORKER_ID, "x-vrt-log-token": env.TOKEN },
        body: JSON.stringify({ records }),
      });
    } catch (_) {
      /* collection failure must never surface */
    }
  },
};
"#;

#[cfg(test)]
mod tail_js_tests {
    use super::*;

    #[test]
    fn missing_response_yields_null_status_never_a_fabricated_one() {
        // Uncaught exception / timeout / cancel: workerd gives no
        // `event.response`; the tail must report `status: null`.
        assert!(TAIL_WORKER_JS
            .contains("status: res && typeof res.status === \"number\" ? res.status : null"));
        // The raw workerd outcome is forwarded, not remapped.
        assert!(TAIL_WORKER_JS.contains("outcome: String(ev.outcome || \"\")"));
    }
}
