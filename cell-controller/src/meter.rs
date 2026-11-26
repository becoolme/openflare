//! Worker usage meter: scrapes each vrt-host pod's Prometheus text
//! (`GET {base}/metrics`), computes per-scrape deltas, and pushes a bounded
//! batch to the control plane
//! (`POST /api/internal/clusters/{cell}/vrt/meter/batch`) over the same scoped
//! cell token as every other cell->control-plane channel.
//!
//! See `doc/platform/workers-runtime-metering.md`.
//!
//! Delivery contract:
//! - **Delta, not cumulative**: counters are process-local and reset on
//!   vrt-host restart. A sample lower than the previous one is treated as a
//!   reset and its full value becomes the delta; otherwise `new - old`.
//! - **Commit-on-push**: the previous sample is only advanced after the
//!   control plane accepts the batch. A failed push replays the same delta on
//!   the next tick with the SAME `(bootId, seq)`; the control plane dedupes by
//!   that key, so a partially-delivered tick can never double count.
//! - **Bounded**: one scrape + one push per pod per tick, body capped, no
//!   unbounded retry queue. Scrape/push failures only move counters.
//! - **Fail soft**: nothing here touches the tenant request path.
//!
//! Trust boundary: the counters are incremented on the platform-trusted path
//! (vrt-host ingress proxy + provider dispatch), never by worker code. Only
//! `worker_id`/binding labels are read; tenant/version attribution is done by
//! the control plane.

use async_trait::async_trait;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;

/// Hard cap on a scraped metrics body.
pub const MAX_SCRAPE_BYTES: usize = 8 * 1024 * 1024;
/// Hard cap on a push body.
pub const MAX_PUSH_BYTES: usize = 4 * 1024 * 1024;
/// Entries per batch (one per worker).
pub const MAX_ENTRIES: usize = 5_000;
/// Binding types tracked per worker (defensive; the control plane caps too).
pub const MAX_BINDINGS: usize = 32;

const M_REQUESTS: &str = "vrt_worker_requests_total";
const M_REQ_BYTES: &str = "vrt_worker_request_bytes_total";
const M_RESP_BYTES: &str = "vrt_worker_response_bytes_total";
const M_SUBREQ: &str = "vrt_worker_subrequests_total";
const M_CRON: &str = "vrt_worker_cron_invocations_total";
const M_ERRORS: &str = "vrt_worker_exceptions_total";

/// One parsed Prometheus text sample.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Parse the subset of Prometheus text we need. Unknown families are dropped
/// later; `#` comments, blank lines and non-finite values are skipped.
pub fn parse_metrics(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((head, value_str)) = split_value(line) else {
            continue;
        };
        let Ok(value) = value_str.parse::<f64>() else {
            continue;
        };
        if !value.is_finite() {
            continue;
        }
        let Some((name, labels)) = split_name_labels(head) else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        out.push(Sample {
            name: name.to_string(),
            labels,
            value,
        });
    }
    out
}

/// Split `metric<labels> value` at the last whitespace before the value.
fn split_value(line: &str) -> Option<(&str, &str)> {
    // Values never contain spaces; skip a trailing timestamp if present.
    let mut parts = line.rsplitn(3, ' ');
    let value = parts.next()?;
    let head = parts.next()?;
    // If head still looks like a number it was a timestamp: drop it and retry.
    if head.parse::<f64>().is_ok() {
        let head = parts.next()?;
        return Some((head, value));
    }
    Some((head, value))
}

/// Split `name{k="v",...}` into the metric name and decoded labels.
fn split_name_labels(head: &str) -> Option<(&str, BTreeMap<String, String>)> {
    match head.find('{') {
        None => Some((head, BTreeMap::new())),
        Some(open) => {
            if !head.ends_with('}') {
                return None;
            }
            let name = &head[..open];
            let inner = &head[open + 1..head.len() - 1];
            let mut labels = BTreeMap::new();
            for pair in split_labels(inner) {
                let eq = pair.find('=')?;
                let key = pair[..eq].trim();
                let raw = pair[eq + 1..].trim();
                let value = raw
                    .strip_prefix('"')
                    .and_then(|v| v.strip_suffix('"'))
                    .unwrap_or(raw);
                if key.is_empty() {
                    return None;
                }
                labels.insert(key.to_string(), unescape_label(value));
            }
            Some((name, labels))
        }
    }
}

/// Split a label list on commas that are not inside quoted values.
fn split_labels(inner: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let mut escaped = false;
    for (i, ch) in inner.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                out.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < inner.len() {
        out.push(inner[start..].trim());
    }
    out.into_iter().filter(|s| !s.is_empty()).collect()
}

fn unescape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('\\') => out.push('\\'),
            Some('"') => out.push('"'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

fn sample_key(sample: &Sample) -> String {
    let mut key = sample.name.clone();
    for (k, v) in &sample.labels {
        key.push('\u{1f}');
        key.push_str(k);
        key.push('=');
        key.push_str(v);
    }
    key
}

/// Per-worker accumulator built from one tick's deltas.
#[derive(Default, Clone, Debug)]
pub struct WorkerUsage {
    pub requests: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub subrequests: BTreeMap<String, u64>,
    pub cron_invocations: u64,
    pub errors: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MeterEntry {
    pub worker_id: String,
    pub requests: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub subrequests: BTreeMap<String, u64>,
    pub cron_invocations: u64,
    pub errors: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MeterBatch {
    pub schema_version: u32,
    pub pod_id: String,
    pub boot_id: String,
    pub seq: u64,
    pub window_start: String,
    pub window_end: String,
    pub entries: Vec<MeterEntry>,
}

#[async_trait]
pub trait MeterSource: Send + Sync {
    /// Scrape one pod's Prometheus text.
    async fn scrape(&self, pod: &str) -> Result<Vec<Sample>, String>;
}

#[async_trait]
pub trait MeterSink: Send + Sync {
    async fn push(&self, batch: &MeterBatch) -> Result<(), String>;
}

/// Loopback-only HTTP source over `[[pods]]` bases, mirroring the log source.
pub struct HttpMeterSource {
    pods: BTreeMap<String, String>,
    http: reqwest::Client,
}

impl HttpMeterSource {
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
            .map_err(|e| format!("meter http client: {e}"))?;
        Ok(Self { pods: map, http })
    }

    pub fn pod_ids(&self) -> Vec<String> {
        self.pods.keys().cloned().collect()
    }
}

#[async_trait]
impl MeterSource for HttpMeterSource {
    async fn scrape(&self, pod: &str) -> Result<Vec<Sample>, String> {
        let base = self.pods.get(pod).ok_or("unknown pod")?;
        let resp = self
            .http
            .get(format!("{base}/metrics"))
            .send()
            .await
            .map_err(|e| format!("metrics scrape: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("metrics scrape status {}", resp.status()));
        }
        if resp
            .content_length()
            .map(|l| l as usize > MAX_SCRAPE_BYTES)
            .unwrap_or(false)
        {
            return Err("metrics body too large".into());
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("metrics body: {e}"))?;
        if bytes.len() > MAX_SCRAPE_BYTES {
            return Err("metrics body too large".into());
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| "metrics not utf8".to_string())?;
        Ok(parse_metrics(text))
    }
}

/// Control-plane push.
pub struct HttpMeterSink {
    http: reqwest::Client,
    url: String,
    token: String,
}

impl HttpMeterSink {
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
            .map_err(|e| format!("meter push http: {e}"))?;
        Ok(Self {
            http,
            url: format!(
                "{}/api/internal/clusters/{}/vrt/meter/batch",
                base_url.trim_end_matches('/'),
                cell_id
            ),
            token,
        })
    }
}

#[async_trait]
impl MeterSink for HttpMeterSink {
    async fn push(&self, batch: &MeterBatch) -> Result<(), String> {
        let body = serde_json::to_vec(batch).map_err(|e| format!("meter encode: {e}"))?;
        if body.len() > MAX_PUSH_BYTES {
            return Err("meter push body too large".into());
        }
        let resp = self
            .http
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("meter push: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("meter push status {}", resp.status()));
        }
        Ok(())
    }
}

/// Fold a tick's per-sample deltas into per-worker usage.
pub fn accumulate(out: &mut BTreeMap<String, WorkerUsage>, sample: &Sample, delta: f64) {
    let Some(worker) = sample.labels.get("worker_id") else {
        return;
    };
    // vrt-metrics folds cardinality overflow into the literal `_overflow`
    // worker id; it is not a real worker, so it must never reach the control
    // plane as a usage entry.
    if worker.is_empty() || worker == "_overflow" || !delta.is_finite() || delta <= 0.0 {
        return;
    }
    if out.len() >= MAX_ENTRIES && !out.contains_key(worker) {
        return;
    }
    let d = delta.round() as u64;
    if d == 0 {
        return;
    }
    let acc = out.entry(worker.clone()).or_default();
    match sample.name.as_str() {
        M_REQUESTS => acc.requests += d,
        M_REQ_BYTES => acc.request_bytes += d,
        M_RESP_BYTES => acc.response_bytes += d,
        M_CRON => acc.cron_invocations += d,
        M_ERRORS => acc.errors += d,
        M_SUBREQ => {
            let binding = sample
                .labels
                .get("binding_type")
                .filter(|b| !b.is_empty())
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            if acc.subrequests.len() < MAX_BINDINGS || acc.subrequests.contains_key(&binding) {
                *acc.subrequests.entry(binding).or_default() += d;
            }
        }
        _ => {}
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickResult {
    pub entries: usize,
    pub scraped: usize,
    pub scrape_failed: usize,
    pub pushed: bool,
}

/// Per-pod delta shipper with commit-on-push semantics.
pub struct MeterShipper<S: MeterSource, K: MeterSink> {
    source: S,
    sink: K,
    pods: Vec<String>,
    prev: BTreeMap<String, BTreeMap<String, f64>>,
    boot_id: String,
    seq: u64,
}

impl<S: MeterSource, K: MeterSink> MeterShipper<S, K> {
    pub fn new(source: S, sink: K, pods: Vec<String>, boot_id: String) -> Self {
        Self {
            source,
            sink,
            pods,
            prev: BTreeMap::new(),
            boot_id,
            seq: 0,
        }
    }

    /// Scrape every pod, push one batch per pod that produced deltas. The
    /// previous sample and `seq` advance only after a successful push.
    pub async fn tick(&mut self, window_start_ms: i64, window_end_ms: i64) -> TickResult {
        let mut result = TickResult::default();
        for pod in self.pods.clone() {
            let samples = match self.source.scrape(&pod).await {
                Ok(s) => s,
                Err(e) => {
                    result.scrape_failed += 1;
                    tracing::warn!(pod = %pod, error = %e, "meter scrape failed");
                    crate::metrics::meter_event("scrape_failed", 0);
                    continue;
                }
            };
            result.scraped += 1;

            let prev = self.prev.get(&pod).cloned().unwrap_or_default();
            let mut current = BTreeMap::new();
            let mut usage: BTreeMap<String, WorkerUsage> = BTreeMap::new();
            let mut overflow_dropped: u64 = 0;
            for sample in &samples {
                let key = sample_key(sample);
                current.insert(key.clone(), sample.value);
                if sample
                    .labels
                    .get("worker_id")
                    .map(|w| w == "_overflow")
                    .unwrap_or(false)
                {
                    // Baseline still advances above, but the folded series is
                    // not attributable to a worker.
                    overflow_dropped += 1;
                    continue;
                }
                let old = prev.get(&key).copied().unwrap_or(0.0);
                let delta = if sample.value >= old {
                    sample.value - old
                } else {
                    // Counter reset (vrt-host restart): the new value is the delta.
                    sample.value
                };
                accumulate(&mut usage, sample, delta);
            }
            if overflow_dropped > 0 {
                crate::metrics::meter_overflow_dropped(overflow_dropped);
            }
            if usage.is_empty() {
                // No deltas: advance the baseline without pushing.
                self.prev.insert(pod.clone(), current);
                continue;
            }

            let entries: Vec<MeterEntry> = usage
                .into_iter()
                .map(|(worker_id, u)| MeterEntry {
                    worker_id,
                    requests: u.requests,
                    request_bytes: u.request_bytes,
                    response_bytes: u.response_bytes,
                    subrequests: u.subrequests,
                    cron_invocations: u.cron_invocations,
                    errors: u.errors,
                })
                .collect();

            let seq = self.seq + 1;
            let batch = MeterBatch {
                schema_version: 1,
                pod_id: pod.clone(),
                boot_id: self.boot_id.clone(),
                seq,
                window_start: rfc3339_ms(window_start_ms),
                window_end: rfc3339_ms(window_end_ms),
                entries,
            };
            match self.sink.push(&batch).await {
                Ok(()) => {
                    // Commit: the same deltas must never be pushed again.
                    self.prev.insert(pod.clone(), current);
                    self.seq = seq;
                    result.entries += batch.entries.len();
                    result.pushed = true;
                    crate::metrics::meter_event("pushed", batch.entries.len());
                }
                Err(e) => {
                    // Keep the old baseline and seq: the next tick re-sends the
                    // same (larger) delta with the same idempotency key.
                    tracing::warn!(pod = %pod, error = %e, "meter push failed");
                    crate::metrics::meter_event("push_failed", 0);
                }
            }
        }
        result
    }
}

fn rfc3339_ms(ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn parses_names_labels_escapes_and_comments() {
        let text = r#"
# HELP vrt_worker_requests_total x
vrt_worker_requests_total{worker_id="w1",status_class="2xx"} 12
vrt_worker_request_bytes_total{worker_id="w1"} 1024
vrt_worker_subrequests_total{worker_id="w1",binding_type="kv"} 3
bad_metric{worker_id="x" 5
vrt_worker_requests_total{worker_id="w2",status_class="5xx"} NaN
"#;
        let samples = parse_metrics(text);
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].name, M_REQUESTS);
        assert_eq!(samples[0].labels.get("worker_id").unwrap(), "w1");
        assert_eq!(samples[0].value, 12.0);
        assert_eq!(samples[2].labels.get("binding_type").unwrap(), "kv");
    }

    #[test]
    fn label_escapes_are_decoded_and_commas_in_quotes_do_not_split() {
        let text = r#"m{a="x,\"y\"",b="line\nnext"} 1"#;
        let samples = parse_metrics(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("a").unwrap(), "x,\"y\"");
        assert_eq!(samples[0].labels.get("b").unwrap(), "line\nnext");
    }

    #[test]
    fn accumulates_per_worker_and_binding() {
        let mut out = BTreeMap::new();
        let s = |name: &str, labels: &[(&str, &str)], value: f64| Sample {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            value,
        };
        accumulate(&mut out, &s(M_REQUESTS, &[("worker_id", "w1")], 2.0), 2.0);
        accumulate(&mut out, &s(M_REQUESTS, &[("worker_id", "w1")], 3.0), 3.0);
        accumulate(
            &mut out,
            &s(
                M_SUBREQ,
                &[("worker_id", "w1"), ("binding_type", "kv")],
                5.0,
            ),
            5.0,
        );
        let w = out.get("w1").unwrap();
        assert_eq!(w.requests, 5);
        assert_eq!(w.subrequests.get("kv"), Some(&5));
    }

    struct FakeSource {
        samples: Mutex<Vec<Vec<Sample>>>,
    }

    #[async_trait]
    impl MeterSource for FakeSource {
        async fn scrape(&self, _pod: &str) -> Result<Vec<Sample>, String> {
            let mut g = self.samples.lock().unwrap();
            if g.is_empty() {
                Ok(vec![])
            } else {
                Ok(g.remove(0))
            }
        }
    }

    struct FakeSink {
        fail: Mutex<bool>,
        batches: Mutex<Vec<MeterBatch>>,
    }

    #[async_trait]
    impl MeterSink for FakeSink {
        async fn push(&self, batch: &MeterBatch) -> Result<(), String> {
            if *self.fail.lock().unwrap() {
                return Err("down".into());
            }
            self.batches.lock().unwrap().push(MeterBatch {
                schema_version: batch.schema_version,
                pod_id: batch.pod_id.clone(),
                boot_id: batch.boot_id.clone(),
                seq: batch.seq,
                window_start: batch.window_start.clone(),
                window_end: batch.window_end.clone(),
                entries: batch
                    .entries
                    .iter()
                    .map(|e| MeterEntry {
                        worker_id: e.worker_id.clone(),
                        requests: e.requests,
                        request_bytes: e.request_bytes,
                        response_bytes: e.response_bytes,
                        subrequests: e.subrequests.clone(),
                        cron_invocations: e.cron_invocations,
                        errors: e.errors,
                    })
                    .collect(),
            });
            Ok(())
        }
    }

    fn sample(worker: &str, value: f64) -> Sample {
        Sample {
            name: M_REQUESTS.to_string(),
            labels: BTreeMap::from([("worker_id".to_string(), worker.to_string())]),
            value,
        }
    }

    #[tokio::test]
    async fn first_scrape_sends_full_value_then_only_the_delta() {
        let src = FakeSource {
            samples: Mutex::new(vec![vec![sample("w1", 10.0)], vec![sample("w1", 14.0)]]),
        };
        let sink = FakeSink {
            fail: Mutex::new(false),
            batches: Mutex::new(vec![]),
        };
        let mut shipper = MeterShipper::new(src, sink, vec!["p1".into()], "boot".into());
        let r1 = shipper.tick(0, 1000).await;
        assert!(r1.pushed);
        let r2 = shipper.tick(1000, 2000).await;
        assert!(r2.pushed);
        let batches = shipper.sink.batches.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].entries[0].requests, 10);
        assert_eq!(batches[1].entries[0].requests, 4);
        assert_eq!(batches[1].seq, 2);
    }

    #[tokio::test]
    async fn counter_reset_sends_the_new_value_once() {
        let src = FakeSource {
            samples: Mutex::new(vec![vec![sample("w1", 50.0)], vec![sample("w1", 3.0)]]),
        };
        let sink = FakeSink {
            fail: Mutex::new(false),
            batches: Mutex::new(vec![]),
        };
        let mut shipper = MeterShipper::new(src, sink, vec!["p1".into()], "boot".into());
        shipper.tick(0, 1000).await;
        shipper.tick(1000, 2000).await;
        let batches = shipper.sink.batches.lock().unwrap();
        assert_eq!(batches[0].entries[0].requests, 50);
        assert_eq!(batches[1].entries[0].requests, 3);
    }

    #[tokio::test]
    async fn failed_push_keeps_baseline_and_seq_for_replay() {
        let src = FakeSource {
            samples: Mutex::new(vec![vec![sample("w1", 10.0)], vec![sample("w1", 10.0)]]),
        };
        let sink = FakeSink {
            fail: Mutex::new(true),
            batches: Mutex::new(vec![]),
        };
        let mut shipper = MeterShipper::new(src, sink, vec!["p1".into()], "boot".into());
        let r1 = shipper.tick(0, 1000).await;
        assert!(!r1.pushed);
        // First scrape delivered 10 but the push failed; the second scrape is
        // identical, so replaying with the unchanged baseline still yields 10.
        *shipper.sink.fail.lock().unwrap() = false;
        let r2 = shipper.tick(1000, 2000).await;
        assert!(r2.pushed);
        let batches = shipper.sink.batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].entries[0].requests, 10);
        // seq never advanced across the failure, so the retry is idempotent.
        assert_eq!(batches[0].seq, 1);
    }

    #[tokio::test]
    async fn overflow_worker_id_is_skipped_but_baseline_advances() {
        let overflow = Sample {
            name: M_REQUESTS.to_string(),
            labels: BTreeMap::from([("worker_id".to_string(), "_overflow".to_string())]),
            value: 9.0,
        };
        let src = FakeSource {
            samples: Mutex::new(vec![
                vec![sample("w1", 4.0), overflow.clone()],
                vec![sample("w1", 4.0), overflow],
            ]),
        };
        let sink = FakeSink {
            fail: Mutex::new(false),
            batches: Mutex::new(vec![]),
        };
        let mut shipper = MeterShipper::new(src, sink, vec!["p1".into()], "boot".into());

        let r1 = shipper.tick(0, 1_000).await;
        assert!(r1.pushed);
        {
            let batches = shipper.sink.batches.lock().unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].entries.len(), 1);
            assert_eq!(batches[0].entries[0].worker_id, "w1");
            assert_eq!(batches[0].entries[0].requests, 4);
        }
        // Same counters next tick: the overflow baseline advanced, so nothing
        // is re-reported and no push happens.
        let r2 = shipper.tick(1_000, 2_000).await;
        assert!(!r2.pushed);
    }
}
