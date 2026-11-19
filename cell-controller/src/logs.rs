//! Worker log shipper: drains each vrt-host pod's bounded log ring
//! (`POST {base}/v1/logs/drain`) and pushes the records to the control
//! plane (`POST /api/internal/clusters/{cell}/vrt/logs`) over the same
//! scoped cell token as every other cell->control-plane channel.
//!
//! Delivery contract (`doc/platform/workers-runtime-logs.md`):
//! - **Ack-by-cursor**: the pod keeps records until the controller acks
//!   the highest `seq` that the control plane accepted. A failed push
//!   leaves the cursor unchanged, so the next tick re-drains the same
//!   records; the control plane dedupes by `(workerId, bootId, seq)`.
//! - **Bounded**: one drain page per pod per tick (page + body capped),
//!   exponential backoff on failure (cap 60 s). No unbounded retry
//!   queue exists in the controller: if the control plane stays down the
//!   pod ring evicts its oldest records and counts them
//!   (`dropped.evicted`, surfaced in the next successful push).
//! - **Fail soft**: a broken pod/control plane only moves counters;
//!   nothing here touches the tenant request path or the desired/ack
//!   channels.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Records requested per drain call.
pub const DRAIN_PAGE: usize = 200;
/// Hard cap on a drain response / push body.
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Backoff bounds after a failed pod drain or control-plane push.
pub const BACKOFF_BASE: Duration = Duration::from_secs(2);
pub const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// One record as served by the pod (opaque tenant text: never logged
/// by the controller).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LogRecord {
    pub seq: u64,
    pub worker_id: String,
    pub ts_ms: i64,
    pub level: String,
    pub kind: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default)]
    pub truncated: bool,
}

/// Cumulative pod-side drop counters (since pod boot).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DropCounters {
    #[serde(default)]
    pub evicted: u64,
    #[serde(default)]
    pub rate_limited: u64,
    #[serde(default)]
    pub truncated: u64,
    #[serde(default)]
    pub coalesced: u64,
    #[serde(default)]
    pub batch_overflow: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainPage {
    pub boot_id: String,
    #[serde(default)]
    pub records: Vec<LogRecord>,
    #[serde(default)]
    pub dropped: DropCounters,
}

/// Body pushed to the control plane.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PushBatch<'a> {
    pub pod_id: &'a str,
    pub boot_id: &'a str,
    pub records: &'a [LogRecord],
    pub dropped: &'a DropCounters,
}

#[async_trait]
pub trait LogSource: Send + Sync {
    /// Drain one page from `pod`, acking `ack_seq` for `boot_id`.
    async fn drain(
        &self,
        pod: &str,
        boot_id: Option<&str>,
        ack_seq: u64,
    ) -> Result<DrainPage, String>;
}

#[async_trait]
pub trait LogSink: Send + Sync {
    async fn push(&self, batch: &PushBatch<'_>) -> Result<(), String>;
}

/// Loopback-only HTTP source over the `[[pods]]` bases.
pub struct HttpLogSource {
    pods: BTreeMap<String, String>,
    http: reqwest::Client,
}

impl HttpLogSource {
    pub fn new(pods: &[(String, String)], timeout: Duration) -> Result<Self, String> {
        let mut map = BTreeMap::new();
        for (id, base) in pods {
            let url = reqwest::Url::parse(base).map_err(|e| format!("pod {id} base: {e}"))?;
            if url.scheme() != "http" {
                return Err(format!("pod {id} base must be http"));
            }
            let host = url
                .host_str()
                .ok_or_else(|| format!("pod {id} base: no host"))?;
            let loopback = host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .map(|ip| ip.is_loopback())
                    .unwrap_or(false);
            if !loopback {
                return Err(format!("pod {id} base {host} is not loopback"));
            }
            map.insert(id.clone(), base.trim_end_matches('/').to_string());
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("logs http client: {e}"))?;
        Ok(Self { pods: map, http })
    }

    pub fn pod_ids(&self) -> Vec<String> {
        self.pods.keys().cloned().collect()
    }
}

#[async_trait]
impl LogSource for HttpLogSource {
    async fn drain(
        &self,
        pod: &str,
        boot_id: Option<&str>,
        ack_seq: u64,
    ) -> Result<DrainPage, String> {
        let base = self.pods.get(pod).ok_or("unknown pod")?;
        let resp = self
            .http
            .post(format!("{base}/v1/logs/drain"))
            .json(&serde_json::json!({
                "bootId": boot_id,
                "ackSeq": ack_seq,
                "limit": DRAIN_PAGE,
            }))
            .send()
            .await
            .map_err(|e| format!("drain: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("drain status {}", resp.status()));
        }
        if resp
            .content_length()
            .map(|l| l as usize > MAX_BODY_BYTES)
            .unwrap_or(false)
        {
            return Err("drain body too large".into());
        }
        let bytes = resp.bytes().await.map_err(|e| format!("drain body: {e}"))?;
        if bytes.len() > MAX_BODY_BYTES {
            return Err("drain body too large".into());
        }
        serde_json::from_slice(&bytes).map_err(|e| format!("drain json: {e}"))
    }
}

/// Control-plane push.
pub struct HttpLogSink {
    http: reqwest::Client,
    url: String,
    token: String,
}

impl HttpLogSink {
    pub fn new(
        base_url: &str,
        cell_id: &str,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .build()
            .map_err(|e| format!("logs push http: {e}"))?;
        Ok(Self {
            http,
            url: format!(
                "{}/api/internal/clusters/{}/vrt/logs",
                base_url.trim_end_matches('/'),
                cell_id
            ),
            token,
        })
    }
}

#[async_trait]
impl LogSink for HttpLogSink {
    async fn push(&self, batch: &PushBatch<'_>) -> Result<(), String> {
        let body = serde_json::to_vec(batch).map_err(|e| format!("logs encode: {e}"))?;
        if body.len() > MAX_BODY_BYTES {
            return Err("logs push body too large".into());
        }
        let resp = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("logs push: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("logs push status {}", resp.status()));
        }
        Ok(())
    }
}

#[derive(Default)]
struct PodCursor {
    boot_id: Option<String>,
    ack_seq: u64,
    failures: u32,
    /// Skip this pod until the tick count reaches this value.
    not_before_tick: u64,
}

/// Drain+push state machine (one per cell).
pub struct LogShipper<S: LogSource, K: LogSink> {
    source: S,
    sink: K,
    pods: Vec<String>,
    cursors: BTreeMap<String, PodCursor>,
    tick_no: u64,
    tick_every: Duration,
}

/// Outcome of one tick, for logs/tests (never tenant text).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    pub shipped: usize,
    pub failed_pods: usize,
    pub skipped_backoff: usize,
}

fn backoff_ticks(failures: u32, tick_every: Duration) -> u64 {
    let shift = failures.saturating_sub(1).min(6);
    let d = BACKOFF_BASE
        .saturating_mul(1u32 << shift)
        .min(BACKOFF_MAX);
    let t = tick_every.as_millis().max(1);
    ((d.as_millis() + t - 1) / t).max(1) as u64
}

impl<S: LogSource, K: LogSink> LogShipper<S, K> {
    pub fn new(source: S, sink: K, pods: Vec<String>, tick_every: Duration) -> Self {
        Self {
            source,
            sink,
            pods,
            cursors: BTreeMap::new(),
            tick_no: 0,
            tick_every,
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        self.tick_no += 1;
        let mut rep = TickReport::default();
        for pod in self.pods.clone() {
            let cur = self.cursors.entry(pod.clone()).or_default();
            if cur.not_before_tick > self.tick_no {
                rep.skipped_backoff += 1;
                continue;
            }
            let boot = cur.boot_id.clone();
            let ack = cur.ack_seq;
            let page = match self.source.drain(&pod, boot.as_deref(), ack).await {
                Ok(p) => p,
                Err(e) => {
                    fail(cur, self.tick_no, self.tick_every);
                    crate::metrics::logs_event("drain_failed", 1);
                    // Bounded: first failure + every power-of-two streak
                    // length, and the retry cadence is already capped by
                    // the backoff. Only a fixed error class is logged:
                    // drain bodies come from the pod (tenant-derived
                    // data could be echoed by a parser error message).
                    if should_log_failure(cur.failures) {
                        tracing::warn!(
                            pod = %pod,
                            consecutive_failures = cur.failures,
                            error_class = drain_error_class(&e),
                            "worker log drain failed; will retry with backoff"
                        );
                    }
                    rep.failed_pods += 1;
                    continue;
                }
            };
            // New pod process: the cursor from the old boot is void.
            if cur.boot_id.as_deref() != Some(page.boot_id.as_str()) {
                cur.boot_id = Some(page.boot_id.clone());
                cur.ack_seq = 0;
            }
            if page.records.is_empty() {
                cur.failures = 0;
                continue;
            }
            let max_seq = page.records.iter().map(|r| r.seq).max().unwrap_or(0);
            let batch = PushBatch {
                pod_id: &pod,
                boot_id: &page.boot_id,
                records: &page.records,
                dropped: &page.dropped,
            };
            match self.sink.push(&batch).await {
                Ok(()) => {
                    // Ack is sent with the NEXT drain call.
                    cur.ack_seq = max_seq;
                    cur.failures = 0;
                    cur.not_before_tick = 0;
                    rep.shipped += page.records.len();
                    crate::metrics::logs_event("shipped", page.records.len());
                }
                Err(_) => {
                    // Cursor unchanged: records stay in the pod ring and
                    // are re-drained (control plane dedupes).
                    fail(cur, self.tick_no, self.tick_every);
                    crate::metrics::logs_event("push_failed", 1);
                    rep.failed_pods += 1;
                }
            }
        }
        rep
    }
}

/// Rate limit for the drain-failure warning: streak length 1, 2, 4, 8...
fn should_log_failure(consecutive: u32) -> bool {
    consecutive >= 1 && consecutive.is_power_of_two()
}

/// Fixed, content-free classification of a drain error string.
fn drain_error_class(e: &str) -> &'static str {
    if e.starts_with("drain status") {
        "status"
    } else if e.starts_with("drain json") {
        "decode"
    } else if e.contains("too large") {
        "too_large"
    } else if e.starts_with("drain body") {
        "body"
    } else if e.starts_with("drain:") {
        "transport"
    } else {
        "other"
    }
}

fn fail(cur: &mut PodCursor, tick_no: u64, tick_every: Duration) {
    cur.failures = cur.failures.saturating_add(1);
    cur.not_before_tick = tick_no + 1 + backoff_ticks(cur.failures, tick_every);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn rec(seq: u64, w: &str) -> LogRecord {
        LogRecord {
            seq,
            worker_id: w.into(),
            ts_ms: 1,
            level: "log".into(),
            kind: "log".into(),
            message: "m".into(),
            outcome: None,
            status: None,
            code: None,
            truncated: false,
        }
    }

    #[derive(Clone, Default)]
    struct FakeSource {
        /// Records the fake pod holds (seq ascending) and its boot id.
        state: Arc<Mutex<(String, Vec<LogRecord>)>>,
        acks: Arc<Mutex<Vec<(Option<String>, u64)>>>,
        fail: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl LogSource for FakeSource {
        async fn drain(
            &self,
            _pod: &str,
            boot_id: Option<&str>,
            ack_seq: u64,
        ) -> Result<DrainPage, String> {
            if *self.fail.lock().unwrap() {
                return Err("down".into());
            }
            self.acks
                .lock()
                .unwrap()
                .push((boot_id.map(str::to_string), ack_seq));
            let mut g = self.state.lock().unwrap();
            if boot_id == Some(g.0.as_str()) {
                g.1.retain(|r| r.seq > ack_seq);
            }
            Ok(DrainPage {
                boot_id: g.0.clone(),
                records: g.1.clone(),
                dropped: DropCounters::default(),
            })
        }
    }

    #[derive(Clone, Default)]
    struct FakeSink {
        pushed: Arc<Mutex<Vec<(String, Vec<u64>)>>>,
        fail: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl LogSink for FakeSink {
        async fn push(&self, b: &PushBatch<'_>) -> Result<(), String> {
            if *self.fail.lock().unwrap() {
                return Err("cp down".into());
            }
            self.pushed
                .lock()
                .unwrap()
                .push((b.boot_id.to_string(), b.records.iter().map(|r| r.seq).collect()));
            Ok(())
        }
    }

    fn setup() -> (FakeSource, FakeSink, LogShipper<FakeSource, FakeSink>) {
        let src = FakeSource::default();
        *src.state.lock().unwrap() = ("boot1".into(), vec![rec(1, "w"), rec(2, "w")]);
        let sink = FakeSink::default();
        let sh = LogShipper::new(
            src.clone(),
            sink.clone(),
            vec!["p0".into()],
            Duration::from_secs(2),
        );
        (src, sink, sh)
    }

    #[tokio::test]
    async fn ships_then_acks_on_next_drain() {
        let (src, sink, mut sh) = setup();
        let r = sh.tick().await;
        assert_eq!(r.shipped, 2);
        assert_eq!(sink.pushed.lock().unwrap().len(), 1);
        // Next tick acks seq 2 (pod frees), nothing more to push.
        let r = sh.tick().await;
        assert_eq!(r.shipped, 0);
        let acks = src.acks.lock().unwrap().clone();
        assert_eq!(acks.last().unwrap(), &(Some("boot1".to_string()), 2));
        assert!(src.state.lock().unwrap().1.is_empty());
    }

    #[tokio::test]
    async fn push_failure_keeps_cursor_and_backs_off_then_redelivers() {
        let (src, sink, mut sh) = setup();
        *sink.fail.lock().unwrap() = true;
        let r = sh.tick().await;
        assert_eq!(r.failed_pods, 1);
        // Backoff: the next tick skips the pod.
        let r = sh.tick().await;
        assert_eq!(r.skipped_backoff, 1);
        *sink.fail.lock().unwrap() = false;
        // Records were never acked, so they are still in the pod.
        assert_eq!(src.state.lock().unwrap().1.len(), 2);
        // After the backoff window the same records are re-pushed.
        let mut shipped = 0;
        for _ in 0..5 {
            shipped += sh.tick().await.shipped;
        }
        assert_eq!(shipped, 2);
        assert_eq!(sink.pushed.lock().unwrap().last().unwrap().1, vec![1, 2]);
    }

    #[tokio::test]
    async fn drain_failure_backs_off_without_panicking() {
        let (src, _sink, mut sh) = setup();
        *src.fail.lock().unwrap() = true;
        assert_eq!(sh.tick().await.failed_pods, 1);
        assert_eq!(sh.tick().await.skipped_backoff, 1);
    }

    #[test]
    fn drain_failure_warning_is_rate_limited_and_content_free() {
        let logged: Vec<u32> = (1..=100).filter(|n| should_log_failure(*n)).collect();
        assert_eq!(logged, vec![1, 2, 4, 8, 16, 32, 64]);
        assert!(!should_log_failure(0));
        assert_eq!(drain_error_class("drain json: invalid type: string \"SECRET\""), "decode");
        assert_eq!(drain_error_class("drain status 500 Internal Server Error"), "status");
        assert_eq!(drain_error_class("drain: error sending request"), "transport");
        assert_eq!(drain_error_class("drain body too large"), "too_large");
        assert_eq!(drain_error_class("weird SECRET"), "other");
    }

    #[tokio::test]
    async fn pod_restart_resets_cursor() {
        let (src, _sink, mut sh) = setup();
        sh.tick().await;
        sh.tick().await;
        // Pod restarted: new boot id, seq restarts at 1.
        *src.state.lock().unwrap() = ("boot2".into(), vec![rec(1, "w")]);
        let r = sh.tick().await;
        assert_eq!(r.shipped, 1);
    }

    #[test]
    fn backoff_is_bounded_and_monotonic() {
        let t = Duration::from_secs(2);
        let mut prev = 0;
        for f in 1..40 {
            let b = backoff_ticks(f, t);
            assert!(b >= prev);
            assert!(b <= 30, "cap 60s / 2s tick");
            prev = b;
        }
    }

    #[test]
    fn http_source_rejects_non_loopback_and_https() {
        let t = Duration::from_secs(1);
        assert!(HttpLogSource::new(&[("p".into(), "http://10.0.0.1:9091".into())], t).is_err());
        assert!(HttpLogSource::new(&[("p".into(), "https://127.0.0.1:9091".into())], t).is_err());
        assert!(HttpLogSource::new(&[("p".into(), "http://127.0.0.1:9091".into())], t).is_ok());
    }

    #[test]
    fn page_decodes_pod_wire_shape() {
        let j = r#"{"bootId":"b","records":[{"seq":1,"workerId":"w","tsMs":5,"level":"platform","kind":"platform","message":"x","code":"worker_unavailable","truncated":false}],"dropped":{"evicted":3,"rateLimited":0,"truncated":1,"coalesced":0,"batchOverflow":0}}"#;
        let p: DrainPage = serde_json::from_str(j).unwrap();
        assert_eq!(p.records[0].code.as_deref(), Some("worker_unavailable"));
        assert_eq!(p.dropped.evicted, 3);
    }
}
