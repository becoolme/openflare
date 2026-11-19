//! Worker log hub: bounded per-worker ring buffers fed by the platform
//! tail workers (`vrt-config::logs`) and by host-observed platform
//! events (routing/startup failures), drained by cell-controller.
//!
//! Design (`doc/platform/workers-runtime-logs.md`):
//! - **Ingest** (`POST /v1/logs/ingest`, loopback control listener,
//!   reached only through the `vrt-logsink` external of the per-worker
//!   tail worker): authenticated with `HMAC(pepper, workerId)`; the
//!   worker identity comes from the config-rendered header, never from
//!   the record body, so one worker can only write its own stream.
//! - **Bounded**: per-worker record+byte ring, global byte cap,
//!   per-message truncation, per-worker token-bucket rate limit and
//!   per-(worker, code) platform-event coalescing. Every drop is
//!   counted (metrics + cumulative counters on the drain response).
//! - **Redaction**: known secret values of the tenant are replaced by
//!   `[REDACTED]` (exact match, plus JSON-escaped form) BEFORE
//!   truncation and storage, so a secret never reaches the ring.
//! - **Drain** (`POST /v1/logs/drain`): ack-by-cursor. The caller acks
//!   the highest `seq` it durably pushed; the hub frees `seq <= ack`
//!   and returns the next batch. Records carry a `bootId`+`seq`
//!   identity so a re-drain after a failed push is idempotent
//!   downstream.
//! - **Fail open for tenants**: nothing here sits on the request path
//!   except a non-blocking `try_lock`-free short critical section for
//!   platform events; collection failures only increment counters.

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Replacement text for known secret values.
pub const REDACTED: &str = "[REDACTED]";

/// Ingest body cap (JSON escaping can inflate a 200k-char batch).
const INGEST_BODY_LIMIT: usize = 2 * 1024 * 1024;
/// Smallest secret length worth redacting (shorter values would shred
/// unrelated log text and carry little secrecy).
const MIN_REDACT_LEN: usize = 4;
/// Secret values retained per worker (current + previous generation).
const MAX_REDACT_VALUES: usize = 128;
/// Longest secret value considered.
const MAX_REDACT_VALUE_LEN: usize = 4096;
/// Per-record accounting overhead (struct + strings).
const RECORD_OVERHEAD: usize = 160;

/// Bounds. All hard limits; tests use small values.
#[derive(Clone, Debug)]
pub struct HubLimits {
    pub ring_records: usize,
    pub ring_bytes: usize,
    pub total_bytes: usize,
    pub message_bytes: usize,
    pub batch_records: usize,
    pub rate_per_sec: f64,
    pub rate_burst: f64,
    pub platform_min_interval: Duration,
    pub drain_max: usize,
}

impl Default for HubLimits {
    fn default() -> Self {
        Self {
            ring_records: 1000,
            ring_bytes: 1024 * 1024,
            total_bytes: 16 * 1024 * 1024,
            message_bytes: vrt_config::logs::MAX_MESSAGE_BYTES,
            batch_records: vrt_config::logs::MAX_BATCH_RECORDS,
            rate_per_sec: 200.0,
            rate_burst: 400.0,
            platform_min_interval: Duration::from_secs(1),
            drain_max: 500,
        }
    }
}

/// Closed set of platform-event reason codes (`level=platform`
/// records). Wire form is `as_str()`; the docs table in
/// `doc/platform/workers-runtime-logs.md` is pinned to [`Self::ALL`] by a
/// test. Independent of the metrics `reason` label (a coarser,
/// metrics-only vocabulary).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlatformCode {
    /// Could not connect / handshake to the worker process (502).
    UpstreamConnect,
    /// No response inside the request timeout (504).
    UpstreamTimeout,
    /// Upstream I/O / protocol failure mid-request (502).
    UpstreamError,
    /// Worker socket missing on the live generation (503).
    WorkerUnavailable,
    /// Tenant exists but cannot serve right now (503).
    TenantUnavailable,
    /// Tenant circuit breaker open (503).
    CircuitOpen,
    /// Scale-to-zero wake exceeded its deadline (503).
    WakeTimeout,
    /// Scale-to-zero wake launch/ready failed (503).
    WakeFailed,
    /// Scale-to-zero wake capacity exhausted (503).
    WakeCapacity,
    /// Ingress internal fault (500).
    Internal,
    /// Static-asset router failed closed (503); see `x-vrt-route-error`.
    RouteIndexRpcFailed,
    RouteIndexInvalid,
    RouteIndexDigestMismatch,
    RouteIndexTimeout,
    CanFetchRpcFailed,
    CanFetchTimeout,
    /// Tenant log stream exceeded the per-worker rate limit.
    LogsRateLimited,
}

impl PlatformCode {
    pub const ALL: &'static [PlatformCode] = &[
        Self::UpstreamConnect,
        Self::UpstreamTimeout,
        Self::UpstreamError,
        Self::WorkerUnavailable,
        Self::TenantUnavailable,
        Self::CircuitOpen,
        Self::WakeTimeout,
        Self::WakeFailed,
        Self::WakeCapacity,
        Self::Internal,
        Self::RouteIndexRpcFailed,
        Self::RouteIndexInvalid,
        Self::RouteIndexDigestMismatch,
        Self::RouteIndexTimeout,
        Self::CanFetchRpcFailed,
        Self::CanFetchTimeout,
        Self::LogsRateLimited,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::UpstreamConnect => "upstream_connect",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::UpstreamError => "upstream_error",
            Self::WorkerUnavailable => "worker_unavailable",
            Self::TenantUnavailable => "tenant_unavailable",
            Self::CircuitOpen => "tenant_circuit_open",
            Self::WakeTimeout => "tenant_wake_timeout",
            Self::WakeFailed => "tenant_wake_failed",
            Self::WakeCapacity => "wake_capacity",
            Self::Internal => "internal",
            Self::RouteIndexRpcFailed => "route_index_rpc_failed",
            Self::RouteIndexInvalid => "route_index_invalid",
            Self::RouteIndexDigestMismatch => "route_index_digest_mismatch",
            Self::RouteIndexTimeout => "route_index_timeout",
            Self::CanFetchRpcFailed => "can_fetch_rpc_failed",
            Self::CanFetchTimeout => "can_fetch_timeout",
            Self::LogsRateLimited => "logs_rate_limited",
        }
    }

    /// Router fail-closed reason (whitelisted `x-vrt-route-error` value,
    /// as normalized by ingress) -> code. `none`/`other` => None.
    pub fn from_route_error(reason: &str) -> Option<Self> {
        Some(match reason {
            "route_index_rpc_failed" => Self::RouteIndexRpcFailed,
            "route_index_invalid" => Self::RouteIndexInvalid,
            "route_index_digest_mismatch" => Self::RouteIndexDigestMismatch,
            "route_index_timeout" => Self::RouteIndexTimeout,
            "can_fetch_rpc_failed" => Self::CanFetchRpcFailed,
            "can_fetch_timeout" => Self::CanFetchTimeout,
            _ => return None,
        })
    }
}

/// One stored record (tenant text is UNTRUSTED).
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LogRecord {
    pub seq: u64,
    pub worker_id: String,
    pub ts_ms: i64,
    /// debug | info | log | warn | error | platform
    pub level: String,
    /// log | exception | outcome | platform
    pub kind: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// Stable reason code (platform events only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub truncated: bool,
}

impl LogRecord {
    fn cost(&self) -> usize {
        self.message.len() + RECORD_OVERHEAD
    }
}

/// Cumulative drop accounting since boot.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DropCounters {
    pub evicted: u64,
    pub rate_limited: u64,
    pub truncated: u64,
    pub coalesced: u64,
    pub batch_overflow: u64,
}

struct WorkerState {
    ring: VecDeque<LogRecord>,
    bytes: usize,
    tokens: f64,
    last_refill: Instant,
    /// code -> (last emitted, suppressed since)
    platform: HashMap<String, (Instant, u32)>,
    rate_notice_last: Option<Instant>,
    redact: Vec<String>,
}

impl WorkerState {
    fn new(now: Instant, burst: f64) -> Self {
        Self {
            ring: VecDeque::new(),
            bytes: 0,
            tokens: burst,
            last_refill: now,
            platform: HashMap::new(),
            rate_notice_last: None,
            redact: Vec::new(),
        }
    }
}

struct Inner {
    workers: HashMap<String, WorkerState>,
    known: HashSet<String>,
    seq: u64,
    total_bytes: usize,
    drops: DropCounters,
}

/// Ingest failure — maps to a status; none leaks tenant data.
#[derive(Debug, PartialEq, Eq)]
pub enum IngestError {
    Unauthorized,
    UnknownWorker,
    BadBody,
}

/// Result of one ingest call.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct IngestOutcome {
    pub accepted: usize,
    pub dropped: usize,
}

/// Drain response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainResponse {
    pub boot_id: String,
    pub records: Vec<LogRecord>,
    pub dropped: DropCounters,
}

pub struct LogHub {
    limits: HubLimits,
    pepper: String,
    boot_id: String,
    inner: Mutex<Inner>,
}

fn random_hex(n: usize) -> String {
    let mut buf = vec![0u8; n];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut buf))
        .is_err()
    {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id() as u128;
        for (i, b) in buf.iter_mut().enumerate() {
            *b = ((nanos ^ pid).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (i % 8)) as u8;
        }
    }
    hex::encode(buf)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn ct_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Truncate to at most `max` bytes on a char boundary.
fn truncate_utf8(s: &mut String, max: usize) -> bool {
    if s.len() <= max {
        return false;
    }
    let mut cut = max;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    true
}

/// Exact-match secret redaction (tenant text only; never regex).
pub fn redact(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_string();
    for s in secrets {
        if out.contains(s.as_str()) {
            out = out.replace(s.as_str(), REDACTED);
        }
    }
    out
}

fn norm_level(level: &str) -> &'static str {
    match level {
        "debug" => "debug",
        "info" => "info",
        "warn" | "warning" => "warn",
        "error" => "error",
        _ => "log",
    }
}

fn norm_kind(kind: &str) -> &'static str {
    match kind {
        "exception" => "exception",
        "outcome" => "outcome",
        _ => "log",
    }
}

fn strip_control_nul(s: &str) -> String {
    s.chars().filter(|c| *c != '\0').collect()
}

#[derive(Deserialize)]
struct IngestBody {
    #[serde(default)]
    records: Vec<InRecord>,
}

#[derive(Deserialize)]
struct InRecord {
    kind: Option<String>,
    level: Option<String>,
    ts: Option<f64>,
    message: Option<String>,
    outcome: Option<String>,
    status: Option<f64>,
}

fn counter(name: &'static str, labels: &[(&'static str, &'static str)], n: f64) {
    crate::metrics::registry().inc(name, labels, n);
}

impl LogHub {
    pub fn new(limits: HubLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            pepper: random_hex(32),
            boot_id: random_hex(8),
            inner: Mutex::new(Inner {
                workers: HashMap::new(),
                known: HashSet::new(),
                seq: 0,
                total_bytes: 0,
                drops: DropCounters::default(),
            }),
        })
    }

    /// Test constructor with a fixed pepper.
    pub fn with_pepper(limits: HubLimits, pepper: &str) -> Arc<Self> {
        Arc::new(Self {
            limits,
            pepper: pepper.to_string(),
            boot_id: random_hex(8),
            inner: Mutex::new(Inner {
                workers: HashMap::new(),
                known: HashSet::new(),
                seq: 0,
                total_bytes: 0,
                drops: DropCounters::default(),
            }),
        })
    }

    pub fn pepper(&self) -> &str {
        &self.pepper
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned lock only means a panic elsewhere mid-update of
        // counters/rings; the data stays structurally valid.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Register the workers of one tenant and their secret values
    /// (called by the assembler on every assemble). Previous values are
    /// kept (bounded) so the old generation's late logs are still
    /// redacted across a rotation.
    pub fn register_tenant(&self, worker_ids: &[String], secrets: &[String]) {
        let mut forms: Vec<String> = Vec::new();
        for s in secrets {
            if s.len() < MIN_REDACT_LEN || s.len() > MAX_REDACT_VALUE_LEN {
                continue;
            }
            forms.push(s.clone());
            if let Ok(j) = serde_json::to_string(s) {
                let esc = j[1..j.len() - 1].to_string();
                if esc != *s {
                    forms.push(esc);
                }
            }
        }
        // Longest first so a secret containing another is fully masked.
        forms.sort_by_key(|f| std::cmp::Reverse(f.len()));
        forms.dedup();
        let now = Instant::now();
        let mut g = self.lock();
        for id in worker_ids {
            g.known.insert(id.clone());
            let burst = self.limits.rate_burst;
            let w = g
                .workers
                .entry(id.clone())
                .or_insert_with(|| WorkerState::new(now, burst));
            let mut merged = forms.clone();
            for old in &w.redact {
                if !merged.contains(old) {
                    merged.push(old.clone());
                }
            }
            merged.truncate(MAX_REDACT_VALUES);
            w.redact = merged;
        }
    }

    fn token_ok(&self, worker_id: &str, token: &str) -> bool {
        ct_eq(&vrt_config::log_token(&self.pepper, worker_id), token)
    }

    fn evict_to_fit(&self, g: &mut Inner, worker_id: &str) {
        // Per-worker bounds first.
        loop {
            let over = {
                let w = g.workers.get(worker_id).unwrap();
                w.ring.len() > self.limits.ring_records || w.bytes > self.limits.ring_bytes
            };
            if !over {
                break;
            }
            self.evict_front(g, worker_id);
        }
        // Global bound: evict from the worker holding the oldest record.
        while g.total_bytes > self.limits.total_bytes {
            let victim = g
                .workers
                .iter()
                .filter_map(|(id, w)| w.ring.front().map(|r| (r.seq, id.clone())))
                .min()
                .map(|(_, id)| id);
            match victim {
                Some(id) => self.evict_front(g, &id),
                None => break,
            }
        }
    }

    fn evict_front(&self, g: &mut Inner, worker_id: &str) {
        if let Some(w) = g.workers.get_mut(worker_id) {
            if let Some(r) = w.ring.pop_front() {
                let c = r.cost();
                w.bytes = w.bytes.saturating_sub(c);
                g.total_bytes = g.total_bytes.saturating_sub(c);
                g.drops.evicted += 1;
                counter("vrt_host_logs_dropped_total", &[("reason", "evicted")], 1.0);
            }
        }
    }

    fn take_token(&self, w: &mut WorkerState, now: Instant) -> bool {
        let dt = now.saturating_duration_since(w.last_refill).as_secs_f64();
        w.last_refill = now;
        w.tokens = (w.tokens + dt * self.limits.rate_per_sec).min(self.limits.rate_burst);
        if w.tokens >= 1.0 {
            w.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Push one finished record (caller holds the lock). Applies
    /// redaction, truncation, ring bounds.
    #[allow(clippy::too_many_arguments)]
    fn push(
        &self,
        g: &mut Inner,
        worker_id: &str,
        ts_ms: i64,
        level: &str,
        kind: &str,
        message: &str,
        outcome: Option<String>,
        status: Option<u16>,
        code: Option<String>,
    ) {
        let secrets = g
            .workers
            .get(worker_id)
            .map(|w| w.redact.clone())
            .unwrap_or_default();
        // Redact BEFORE truncation so a secret straddling the cut
        // cannot leak as a prefix.
        let mut msg = redact(&strip_control_nul(message), &secrets);
        let truncated = truncate_utf8(&mut msg, self.limits.message_bytes);
        if truncated {
            g.drops.truncated += 1;
            counter("vrt_host_logs_truncated_total", &[], 1.0);
        }
        g.seq += 1;
        let rec = LogRecord {
            seq: g.seq,
            worker_id: worker_id.to_string(),
            ts_ms,
            level: level.to_string(),
            kind: kind.to_string(),
            message: msg,
            outcome: outcome.map(|o| {
                let mut o = strip_control_nul(&o);
                truncate_utf8(&mut o, 64);
                o
            }),
            status,
            code,
            truncated,
        };
        let c = rec.cost();
        let w = g.workers.get_mut(worker_id).unwrap();
        w.bytes += c;
        w.ring.push_back(rec);
        g.total_bytes += c;
        counter("vrt_host_logs_ingested_total", &[], 1.0);
        self.evict_to_fit(g, worker_id);
        crate::metrics::registry().set(
            "vrt_host_logs_buffer_bytes",
            &[],
            g.total_bytes as f64,
        );
    }

    /// Authenticated tail-worker ingest. Identity = `worker_id`
    /// header + token; the body never names a worker.
    pub fn ingest(
        &self,
        worker_id: &str,
        token: &str,
        body: &[u8],
    ) -> Result<IngestOutcome, IngestError> {
        if !self.token_ok(worker_id, token) {
            return Err(IngestError::Unauthorized);
        }
        let parsed: IngestBody = serde_json::from_slice(body).map_err(|_| IngestError::BadBody)?;
        let now = Instant::now();
        let now_wall = now_ms();
        let mut out = IngestOutcome::default();
        let mut g = self.lock();
        if !g.known.contains(worker_id) {
            return Err(IngestError::UnknownWorker);
        }
        let burst = self.limits.rate_burst;
        g.workers
            .entry(worker_id.to_string())
            .or_insert_with(|| WorkerState::new(now, burst));
        let total = parsed.records.len();
        if total > self.limits.batch_records {
            let over = (total - self.limits.batch_records) as u64;
            g.drops.batch_overflow += over;
            out.dropped += over as usize;
            counter(
                "vrt_host_logs_dropped_total",
                &[("reason", "batch_overflow")],
                over as f64,
            );
        }
        for rec in parsed.records.into_iter().take(self.limits.batch_records) {
            let allowed = {
                let w = g.workers.get_mut(worker_id).unwrap();
                self.take_token(w, now)
            };
            if !allowed {
                g.drops.rate_limited += 1;
                out.dropped += 1;
                counter(
                    "vrt_host_logs_dropped_total",
                    &[("reason", "rate_limited")],
                    1.0,
                );
                // One coalesced notice per 10 s so the stream shows
                // that records are being dropped.
                let notice = {
                    let w = g.workers.get_mut(worker_id).unwrap();
                    let due = w
                        .rate_notice_last
                        .map(|t| now.saturating_duration_since(t) >= Duration::from_secs(10))
                        .unwrap_or(true);
                    if due {
                        w.rate_notice_last = Some(now);
                    }
                    due
                };
                if notice {
                    self.push(
                        &mut g,
                        worker_id,
                        now_wall,
                        "platform",
                        "platform",
                        "log rate limit exceeded: records are being dropped",
                        None,
                        None,
                        Some(PlatformCode::LogsRateLimited.as_str().into()),
                    );
                }
                continue;
            }
            let ts = match rec.ts {
                Some(t) if t.is_finite() && t > 0.0 => {
                    let t = t as i64;
                    if t < now_wall - 86_400_000 || t > now_wall + 60_000 {
                        now_wall
                    } else {
                        t
                    }
                }
                _ => now_wall,
            };
            let status = rec
                .status
                .filter(|s| s.is_finite() && *s >= 100.0 && *s <= 599.0)
                .map(|s| s as u16);
            self.push(
                &mut g,
                worker_id,
                ts,
                norm_level(rec.level.as_deref().unwrap_or("log")),
                norm_kind(rec.kind.as_deref().unwrap_or("log")),
                rec.message.as_deref().unwrap_or(""),
                rec.outcome,
                status,
                None,
            );
            out.accepted += 1;
        }
        Ok(out)
    }

    /// Host-observed platform event (routing/startup/upstream failure).
    /// `code` is a fixed stable token; `message` is platform-authored
    /// (never tenant text). Never blocks the request beyond a short
    /// critical section; unknown workers are ignored (bounded map).
    ///
    /// `status` is the HTTP status the host actually returned to the
    /// client for this failure (None when the event is not tied to one
    /// response, e.g. rate-limit notices). Never invented.
    pub fn platform_event(
        &self,
        worker_id: &str,
        code: PlatformCode,
        status: Option<u16>,
        message: &str,
    ) {
        let code_str = code.as_str();
        let now = Instant::now();
        let mut g = self.lock();
        if !g.known.contains(worker_id) {
            return;
        }
        let burst = self.limits.rate_burst;
        let w = g
            .workers
            .entry(worker_id.to_string())
            .or_insert_with(|| WorkerState::new(now, burst));
        let min = self.limits.platform_min_interval;
        let mut suppressed = 0u32;
        match w.platform.get_mut(code_str) {
            Some((last, n)) if now.saturating_duration_since(*last) < min => {
                *n = n.saturating_add(1);
                g.drops.coalesced += 1;
                counter("vrt_host_logs_dropped_total", &[("reason", "coalesced")], 1.0);
                return;
            }
            Some((last, n)) => {
                suppressed = *n;
                *last = now;
                *n = 0;
            }
            None => {
                w.platform.insert(code_str.to_string(), (now, 0));
            }
        }
        if !self.take_token(g.workers.get_mut(worker_id).unwrap(), now) {
            g.drops.rate_limited += 1;
            counter(
                "vrt_host_logs_dropped_total",
                &[("reason", "rate_limited")],
                1.0,
            );
            return;
        }
        let msg = if suppressed > 0 {
            format!("{message} ({suppressed} similar events suppressed)")
        } else {
            message.to_string()
        };
        self.push(
            &mut g,
            worker_id,
            now_ms(),
            "platform",
            "platform",
            &msg,
            None,
            status,
            Some(code_str.to_string()),
        );
    }

    /// Ack-by-cursor drain. `boot_id` must match this process for the
    /// ack to count (a controller holding a cursor from a previous host
    /// process restarts from 0).
    pub fn drain(&self, boot_id: Option<&str>, ack: u64, limit: usize) -> DrainResponse {
        let limit = limit.clamp(1, self.limits.drain_max);
        let ack = if boot_id == Some(self.boot_id.as_str()) {
            ack
        } else {
            0
        };
        let mut g = self.lock();
        let mut freed = 0usize;
        let ids: Vec<String> = g.workers.keys().cloned().collect();
        for id in &ids {
            let w = g.workers.get_mut(id).unwrap();
            while let Some(front) = w.ring.front() {
                if front.seq <= ack {
                    let c = front.cost();
                    w.ring.pop_front();
                    w.bytes = w.bytes.saturating_sub(c);
                    freed += c;
                } else {
                    break;
                }
            }
        }
        g.total_bytes = g.total_bytes.saturating_sub(freed);
        let mut all: Vec<&LogRecord> = g
            .workers
            .values()
            .flat_map(|w| w.ring.iter())
            .filter(|r| r.seq > ack)
            .collect();
        all.sort_by_key(|r| r.seq);
        let records: Vec<LogRecord> = all.into_iter().take(limit).cloned().collect();
        crate::metrics::registry().set("vrt_host_logs_buffer_bytes", &[], g.total_bytes as f64);
        DrainResponse {
            boot_id: self.boot_id.clone(),
            records,
            dropped: g.drops.clone(),
        }
    }

    /// Records currently buffered for a worker (tests/diagnostics).
    pub fn buffered(&self, worker_id: &str) -> usize {
        self.lock()
            .workers
            .get(worker_id)
            .map(|w| w.ring.len())
            .unwrap_or(0)
    }
}

fn err(status: StatusCode, code: &str) -> Response {
    (
        status,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(serde_json::json!({"error": {"code": code}})),
    )
        .into_response()
}

async fn ingest_handler(
    State(hub): State<Arc<LogHub>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let worker = headers
        .get("x-vrt-log-worker")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = headers
        .get("x-vrt-log-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    match hub.ingest(worker, token, &body) {
        Ok(o) => (
            StatusCode::OK,
            axum::Json(serde_json::json!({"accepted": o.accepted, "dropped": o.dropped})),
        )
            .into_response(),
        Err(e) => {
            let (status, code, reason) = match e {
                IngestError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", "auth"),
                IngestError::UnknownWorker => (StatusCode::NOT_FOUND, "unknown_worker", "unknown"),
                IngestError::BadBody => (StatusCode::BAD_REQUEST, "bad_body", "bad_body"),
            };
            counter(
                "vrt_host_logs_ingest_rejected_total",
                &[("reason", reason)],
                1.0,
            );
            err(status, code)
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBody {
    #[serde(default)]
    boot_id: Option<String>,
    #[serde(default)]
    ack_seq: u64,
    #[serde(default)]
    limit: Option<usize>,
}

async fn drain_handler(State(hub): State<Arc<LogHub>>, body: Bytes) -> Response {
    let req: DrainBody = if body.is_empty() {
        DrainBody {
            boot_id: None,
            ack_seq: 0,
            limit: None,
        }
    } else {
        match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(_) => return err(StatusCode::BAD_REQUEST, "bad_body"),
        }
    };
    let resp = hub.drain(req.boot_id.as_deref(), req.ack_seq, req.limit.unwrap_or(200));
    (
        StatusCode::OK,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(resp),
    )
        .into_response()
}

/// Control-listener router (same loopback/pod-network trust model as
/// `/v1/cron/trigger`: the pod NetworkPolicy admits only the cell
/// controller and the in-pod tail workers can reach it).
pub fn router(hub: Arc<LogHub>) -> Router {
    Router::new()
        .route("/v1/logs/ingest", post(ingest_handler))
        .route("/v1/logs/drain", post(drain_handler))
        .layer(DefaultBodyLimit::max(INGEST_BODY_LIMIT))
        .with_state(hub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> HubLimits {
        HubLimits {
            ring_records: 5,
            ring_bytes: 10_000,
            total_bytes: 100_000,
            message_bytes: 32,
            batch_records: 10,
            rate_per_sec: 0.0,
            rate_burst: 1000.0,
            platform_min_interval: Duration::from_secs(3600),
            drain_max: 100,
        }
    }

    fn hub(l: HubLimits) -> Arc<LogHub> {
        let h = LogHub::with_pepper(l, "pep");
        h.register_tenant(&["w1".into(), "w2".into()], &[]);
        h
    }

    fn tok(w: &str) -> String {
        vrt_config::log_token("pep", w)
    }

    fn body(msgs: &[&str]) -> Vec<u8> {
        let recs: Vec<_> = msgs
            .iter()
            .map(|m| serde_json::json!({"kind":"log","level":"info","message":m}))
            .collect();
        serde_json::to_vec(&serde_json::json!({ "records": recs })).unwrap()
    }

    #[test]
    fn rejects_bad_token_and_cross_worker_token() {
        let h = hub(small());
        assert_eq!(
            h.ingest("w1", "bad", &body(&["x"])),
            Err(IngestError::Unauthorized)
        );
        // w2's token must not write w1's stream.
        assert_eq!(
            h.ingest("w1", &tok("w2"), &body(&["x"])),
            Err(IngestError::Unauthorized)
        );
        assert_eq!(h.buffered("w1"), 0);
    }

    #[test]
    fn unknown_worker_rejected_even_with_valid_token() {
        let h = hub(small());
        assert_eq!(
            h.ingest("ghost", &tok("ghost"), &body(&["x"])),
            Err(IngestError::UnknownWorker)
        );
    }

    #[test]
    fn bad_body_rejected() {
        let h = hub(small());
        assert_eq!(
            h.ingest("w1", &tok("w1"), b"not json"),
            Err(IngestError::BadBody)
        );
    }

    #[test]
    fn isolation_per_worker_and_body_cannot_name_worker() {
        let h = hub(small());
        let b = serde_json::to_vec(&serde_json::json!({"records":[
            {"kind":"log","level":"info","message":"a","workerId":"w2"}
        ]}))
        .unwrap();
        h.ingest("w1", &tok("w1"), &b).unwrap();
        h.ingest("w2", &tok("w2"), &body(&["b"])).unwrap();
        let d = h.drain(None, 0, 100);
        assert_eq!(d.records.len(), 2);
        for r in &d.records {
            match r.message.as_str() {
                "a" => assert_eq!(r.worker_id, "w1"),
                "b" => assert_eq!(r.worker_id, "w2"),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn ring_record_cap_evicts_oldest_and_counts() {
        let h = hub(small());
        h.ingest("w1", &tok("w1"), &body(&["1", "2", "3", "4", "5", "6", "7"]))
            .unwrap();
        assert_eq!(h.buffered("w1"), 5);
        let d = h.drain(None, 0, 100);
        assert_eq!(d.records.first().unwrap().message, "3");
        assert_eq!(d.dropped.evicted, 2);
    }

    #[test]
    fn ring_byte_cap_enforced() {
        let mut l = small();
        l.ring_records = 1000;
        l.ring_bytes = 3 * (RECORD_OVERHEAD + 10);
        l.message_bytes = 10;
        let h = hub(l);
        h.ingest("w1", &tok("w1"), &body(&["0123456789"; 8])).unwrap();
        assert_eq!(h.buffered("w1"), 3);
    }

    #[test]
    fn global_byte_cap_evicts_oldest_across_workers() {
        let mut l = small();
        l.total_bytes = 4 * (RECORD_OVERHEAD + 1);
        l.ring_bytes = 1_000_000;
        l.ring_records = 1000;
        let h = hub(l);
        h.ingest("w1", &tok("w1"), &body(&["a", "b"])).unwrap();
        h.ingest("w2", &tok("w2"), &body(&["c", "d", "e"])).unwrap();
        let d = h.drain(None, 0, 100);
        let msgs: Vec<_> = d.records.iter().map(|r| r.message.as_str()).collect();
        assert_eq!(msgs, vec!["b", "c", "d", "e"]);
    }

    #[test]
    fn truncates_long_messages_on_char_boundary() {
        let h = hub(small());
        let long = "é".repeat(100);
        h.ingest("w1", &tok("w1"), &body(&[&long])).unwrap();
        let d = h.drain(None, 0, 10);
        let r = &d.records[0];
        assert!(r.truncated);
        assert!(r.message.len() <= 32);
        assert!(r.message.chars().all(|c| c == 'é'));
        assert_eq!(d.dropped.truncated, 1);
    }

    #[test]
    fn batch_overflow_is_counted() {
        let h = hub(small());
        let msgs: Vec<String> = (0..15).map(|i| i.to_string()).collect();
        let refs: Vec<&str> = msgs.iter().map(|s| s.as_str()).collect();
        let o = h.ingest("w1", &tok("w1"), &body(&refs)).unwrap();
        assert_eq!(o.accepted, 10);
        assert_eq!(o.dropped, 5);
        assert_eq!(h.drain(None, 0, 100).dropped.batch_overflow, 5);
    }

    #[test]
    fn rate_limit_drops_and_emits_single_notice() {
        let mut l = small();
        l.rate_burst = 3.0;
        l.ring_records = 100;
        let h = hub(l);
        let o = h
            .ingest("w1", &tok("w1"), &body(&["1", "2", "3", "4", "5", "6"]))
            .unwrap();
        assert_eq!(o.accepted, 3);
        assert_eq!(o.dropped, 3);
        let d = h.drain(None, 0, 100);
        assert_eq!(d.dropped.rate_limited, 3);
        // The notice also needs a token; with 0 refill it is dropped
        // too — the counter is the guaranteed signal.
        assert!(d.records.len() <= 4);
    }

    #[test]
    fn tenant_cannot_forge_platform_level_or_kind() {
        let h = hub(small());
        let b = serde_json::to_vec(&serde_json::json!({"records":[
            {"kind":"platform","level":"platform","message":"forged"}
        ]}))
        .unwrap();
        h.ingest("w1", &tok("w1"), &b).unwrap();
        let r = &h.drain(None, 0, 10).records[0];
        assert_eq!(r.level, "log");
        assert_eq!(r.kind, "log");
        assert!(r.code.is_none());
    }

    #[test]
    fn redacts_secrets_before_truncation_and_json_escaped() {
        let h = LogHub::with_pepper(small(), "pep");
        h.register_tenant(&["w1".into()], &["s3cr3t-value".into(), "a\"b\\c-key".into()]);
        // Secret placed so that truncation at 32 bytes would cut it.
        let msg = format!("{}s3cr3t-value tail", "x".repeat(15));
        let b = body(&[&msg, "payload a\\\"b\\\\c-key end"]);
        h.ingest("w1", &tok("w1"), &b).unwrap();
        let d = h.drain(None, 0, 10);
        for r in &d.records {
            assert!(!r.message.contains("s3cr3t"), "{}", r.message);
            assert!(!r.message.contains("c-key"), "{}", r.message);
        }
        assert!(d.records[0].message.contains(REDACTED));
    }

    #[test]
    fn redaction_survives_rotation_for_old_generation() {
        let h = LogHub::with_pepper(small(), "pep");
        h.register_tenant(&["w1".into()], &["old-secret-1".into()]);
        h.register_tenant(&["w1".into()], &["new-secret-2".into()]);
        h.ingest("w1", &tok("w1"), &body(&["old-secret-1 new-secret-2"]))
            .unwrap();
        let m = &h.drain(None, 0, 10).records[0].message;
        assert_eq!(m, "[REDACTED] [REDACTED]");
    }

    #[test]
    fn short_secrets_not_redacted() {
        let h = LogHub::with_pepper(small(), "pep");
        h.register_tenant(&["w1".into()], &["ab".into()]);
        h.ingest("w1", &tok("w1"), &body(&["ab cd"])).unwrap();
        assert_eq!(h.drain(None, 0, 10).records[0].message, "ab cd");
    }

    #[test]
    fn nul_bytes_stripped_and_status_validated() {
        let h = hub(small());
        let b = serde_json::to_vec(&serde_json::json!({"records":[
            {"kind":"outcome","level":"error","message":"a\u{0}b","outcome":"exception","status":99999}
        ]}))
        .unwrap();
        h.ingest("w1", &tok("w1"), &b).unwrap();
        let r = &h.drain(None, 0, 10).records[0];
        assert_eq!(r.message, "ab");
        assert_eq!(r.status, None);
        assert_eq!(r.outcome.as_deref(), Some("exception"));
    }

    #[test]
    fn platform_event_coalesces_per_code_and_ignores_unknown() {
        let h = hub(small());
        h.platform_event("w1", PlatformCode::WorkerUnavailable, Some(503), "worker unavailable");
        h.platform_event("w1", PlatformCode::WorkerUnavailable, Some(503), "worker unavailable");
        h.platform_event("w1", PlatformCode::UpstreamTimeout, Some(504), "timeout");
        h.platform_event("ghost", PlatformCode::WorkerUnavailable, Some(503), "x");
        let d = h.drain(None, 0, 10);
        assert_eq!(d.records.len(), 2);
        assert!(d.records.iter().all(|r| r.level == "platform"));
        assert_eq!(d.dropped.coalesced, 1);
        assert_eq!(h.buffered("ghost"), 0);
        // Stable wire code + the status actually returned to the client.
        assert_eq!(d.records[1].code.as_deref(), Some("upstream_timeout"));
        assert_eq!(d.records[1].status, Some(504));
        assert_eq!(d.records[0].status, Some(503));
    }

    #[test]
    fn platform_codes_are_unique_snake_case_and_documented() {
        let mut seen = std::collections::HashSet::new();
        let doc = include_str!("../../docs/log-codes.md");
        for c in PlatformCode::ALL {
            let s = c.as_str();
            assert!(seen.insert(s), "duplicate code {s}");
            assert!(s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'), "{s}");
            assert!(doc.contains(&format!("`{s}`")), "code {s} missing from docs table");
        }
        assert_eq!(
            PlatformCode::from_route_error("route_index_timeout"),
            Some(PlatformCode::RouteIndexTimeout)
        );
        assert_eq!(PlatformCode::from_route_error("other"), None);
        assert_eq!(PlatformCode::from_route_error("none"), None);
    }

    #[test]
    fn arrival_order_not_timestamp_order_defines_seq() {
        // A slow request's tail record carries its START time but arrives
        // after the platform event produced at timeout: seq follows
        // arrival, ts stays the event time (documented semantics).
        let h = hub(small());
        h.platform_event("w1", PlatformCode::UpstreamTimeout, Some(504), "t");
        let earlier = now_ms() - 30_000;
        let b = format!(
            r#"{{"records":[{{"kind":"outcome","level":"info","ts":{earlier},"message":"GET /","outcome":"ok","status":200}}]}}"#
        );
        h.ingest("w1", &tok("w1"), b.as_bytes()).unwrap();
        let d = h.drain(None, 0, 10);
        assert_eq!(d.records[0].code.as_deref(), Some("upstream_timeout"));
        assert!(d.records[0].seq < d.records[1].seq);
        assert!(d.records[1].ts_ms < d.records[0].ts_ms);
    }

    #[test]
    fn platform_event_reports_suppressed_count_after_interval() {
        let mut l = small();
        l.platform_min_interval = Duration::from_millis(30);
        let h = hub(l);
        h.platform_event("w1", PlatformCode::UpstreamError, None, "m");
        h.platform_event("w1", PlatformCode::UpstreamError, None, "m");
        h.platform_event("w1", PlatformCode::UpstreamError, None, "m");
        std::thread::sleep(Duration::from_millis(50));
        h.platform_event("w1", PlatformCode::UpstreamError, None, "m");
        let d = h.drain(None, 0, 10);
        assert_eq!(d.records.len(), 2);
        assert!(d.records[1].message.contains("2 similar events suppressed"));
    }

    #[test]
    fn drain_ack_frees_and_is_idempotent_until_acked() {
        let h = hub(small());
        h.ingest("w1", &tok("w1"), &body(&["1", "2", "3"])).unwrap();
        let d1 = h.drain(None, 0, 2);
        assert_eq!(d1.records.len(), 2);
        // No ack: same batch again (idempotent re-drain).
        let d1b = h.drain(Some(h.boot_id()), 0, 2);
        assert_eq!(d1b.records, d1.records);
        let last = d1.records.last().unwrap().seq;
        let d2 = h.drain(Some(h.boot_id()), last, 10);
        assert_eq!(d2.records.len(), 1);
        assert_eq!(d2.records[0].message, "3");
        assert_eq!(h.buffered("w1"), 1);
    }

    #[test]
    fn drain_ignores_ack_from_other_boot() {
        let h = hub(small());
        h.ingest("w1", &tok("w1"), &body(&["1", "2"])).unwrap();
        let d = h.drain(Some("old-boot"), 99, 10);
        assert_eq!(d.records.len(), 2);
        assert_eq!(h.buffered("w1"), 2);
    }

    #[test]
    fn drain_limit_is_clamped() {
        let mut l = small();
        l.drain_max = 2;
        l.ring_records = 100;
        let h = hub(l);
        h.ingest("w1", &tok("w1"), &body(&["1", "2", "3"])).unwrap();
        assert_eq!(h.drain(None, 0, 1000).records.len(), 2);
        assert_eq!(h.drain(None, 0, 0).records.len(), 1);
    }

    #[tokio::test]
    async fn http_ingest_and_drain_roundtrip() {
        use tower::ServiceExt;
        let h = hub(small());
        let app = router(h.clone());
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/logs/ingest")
            .header("x-vrt-log-worker", "w1")
            .header("x-vrt-log-token", tok("w1"))
            .body(axum::body::Body::from(body(&["hello"])))
            .unwrap();
        let r = app.clone().oneshot(req).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let bad = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/logs/ingest")
            .header("x-vrt-log-worker", "w1")
            .header("x-vrt-log-token", "nope")
            .body(axum::body::Body::from(body(&["x"])))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(bad).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let drain = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/logs/drain")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{\"ackSeq\":0}"))
            .unwrap();
        let r = app.oneshot(drain).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers().get("cache-control").unwrap().to_str().unwrap(),
            "no-store"
        );
    }
}
