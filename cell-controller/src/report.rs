//! Periodic cell report: the runtime-side collection that turns pod
//! Prometheus exposition + data-plane stats + class-directory instance files
//! into one wire document for the control plane.
//!
//! Design:
//! - **One push per tick**, containing every pod's *deltas* since the previous
//!   scrape plus cell-wide resource stats. Counters are differenced here so the
//!   control plane never has to guess a reset; a newly seen series after the
//!   first sample contributes `0` (its baseline is unknown), never a fake burst.
//! - **Fail soft**: one broken pod/data-plane/class dir only adds an entry to
//!   `errors`; the rest of the report is still pushed. The next tick retries.
//! - **Bounded**: response bodies, per-class instance lists and the whole
//!   report are capped; bad input never panics.
//!
//! The wire shape is mirrored by `plane/src/routes.ts` and
//! `apps/console/app/pages/kit/api.vue`; see `fixtures/vrt-wire/cell-report.json`.

use crate::alarm_index::{AlarmClass, AlarmIndex, AlarmObservation};
use crate::assign::PodAssignment;
use serde::{Deserialize, Serialize};
use snapshot_protocol::compute_desired::{BindingDesired, ComputeDesired};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

/// Bumped when the wire shape changes incompatibly.
pub const REPORT_SCHEMA_VERSION: u32 = 1;
/// Hard cap on one pod `/metrics` body.
pub const MAX_SCRAPE_BYTES: usize = 4 * 1024 * 1024;
/// Hard cap on one instance response (the host caps its own scan).
pub const MAX_INSTANCE_BYTES: usize = 4 * 1024 * 1024;
/// Instances retained per class in the report (the API can show `truncated`).
pub const MAX_INSTANCES_PER_CLASS: usize = 1000;
/// Total instance entries across every class in one report. A cell with many
/// classes must not build an 8 MiB+ document (the push would be lost).
pub const MAX_TOTAL_INSTANCES: usize = 5000;
/// Table names retained per D1 database (the count stays exact).
pub const MAX_TABLES_PER_DB: usize = 500;
/// Total table names across every D1 database in one report.
pub const MAX_TOTAL_TABLES: usize = 5000;
/// Error strings carried in a report / returned by a tick (the plane caps
/// each at 512 chars; an unbounded error list would fail schema validation).
pub const MAX_REPORT_ERRORS: usize = 500;
pub const MAX_ERROR_CHARS: usize = 512;
/// Hard cap on the report body pushed to the control plane.
pub const MAX_REPORT_BYTES: usize = 8 * 1024 * 1024;
/// How long a host trusts one alarm gate before treating DO sleep as unsafe.
pub const ALARM_GATE_TTL_MS: u64 = 90_000;
/// An alarm due inside this window keeps the tenant resident, covering the
/// wake loop period + scan/report latency + clock skew.
pub const ALARM_SAFETY_LEAD_MS: u64 = 60_000;
/// Default collection cadence.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
/// Windows longer than this are clamped (a pod that vanished for days).
pub const MAX_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

// ======================= Prometheus parsing =======================

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Parse the Prometheus text exposition used by `vrt-host` `/metrics`.
/// Comments/blank lines are skipped; malformed lines are ignored (one bad
/// line must never drop the whole scrape).
pub fn parse_exposition(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, labels, rest) = match line.find('{') {
            Some(open) => {
                let Some(close) = matching_brace(line, open) else {
                    continue;
                };
                let name = line[..open].trim();
                let labels = parse_labels(&line[open + 1..close]);
                (name, labels, line[close + 1..].trim())
            }
            None => {
                let mut it = line.splitn(2, char::is_whitespace);
                let name = it.next().unwrap_or("").trim();
                (name, BTreeMap::new(), it.next().unwrap_or("").trim())
            }
        };
        if name.is_empty() {
            continue;
        }
        let Some(value) = rest.split_whitespace().next() else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        if !value.is_finite() {
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

fn matching_brace(line: &str, open: usize) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut quoted = false;
    let mut escaped = false;
    for (i, b) in bytes.iter().enumerate().skip(open + 1) {
        if escaped {
            escaped = false;
            continue;
        }
        match *b {
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            b'}' if !quoted => return Some(i),
            _ => {}
        }
    }
    None
}

fn parse_labels(input: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] == b',' || bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && bytes[i] != b',' {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            // Skip the rest of a malformed pair; the next comma starts over.
            while i < bytes.len() && bytes[i] != b',' {
                i += 1;
            }
            continue;
        }
        let key = input[key_start..i].trim();
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'"' {
            continue;
        }
        i += 1;
        let mut value = String::new();
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            i += 1;
            if escaped {
                match b {
                    b'n' => value.push('\n'),
                    b'\\' => value.push('\\'),
                    b'"' => value.push('"'),
                    other => value.push(other as char),
                }
                escaped = false;
                continue;
            }
            match b {
                b'\\' => escaped = true,
                b'"' => break,
                other => value.push(other as char),
            }
        }
        if !key.is_empty() {
            out.insert(key.to_string(), value);
        }
    }
    out
}

fn sample_key(s: &Sample) -> String {
    let mut out = String::with_capacity(s.name.len() + 32);
    out.push_str(&s.name);
    for (k, v) in &s.labels {
        out.push('\u{1f}');
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out
}

fn is_gauge(name: &str) -> bool {
    matches!(
        name,
        "vrt_worker_inflight_requests"
            | "vrt_tenant_memory_rss_bytes"
            | "vrt_host_do_objects"
            | "vrt_host_do_storage_bytes"
            | "vrt_metrics_unsupported"
            | "process_start_time_seconds"
    )
}

// ======================= wire types =======================

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub le: f64,
    pub count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Histogram {
    pub count: u64,
    pub sum_seconds: f64,
    #[serde(default)]
    pub buckets: Vec<Bucket>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkerReport {
    pub worker_id: String,
    pub requests: u64,
    /// 5xx responses + proxy errors before headers.
    pub error_requests: u64,
    /// 5xx responses only.
    pub server_errors: u64,
    /// `vrt_worker_exceptions_total` (proxy/upstream failures).
    pub exceptions: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub inflight: u64,
    pub request_duration: Histogram,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DoStorage {
    pub worker_id: String,
    pub objects: u64,
    pub storage_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TenantReport {
    pub tenant_id: String,
    pub cpu_seconds: f64,
    pub memory_rss_bytes: u64,
    #[serde(default)]
    pub do_storage: Vec<DoStorage>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Launches {
    pub ready: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Wakes {
    pub ok: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodReport {
    pub pod_id: String,
    /// Process-start identity + pod id (diagnostic; counter resets are
    /// signalled by `reset`/`firstSample`).
    pub boot_id: String,
    pub window_ms: i64,
    pub first_sample: bool,
    pub reset: bool,
    #[serde(default)]
    pub workers: Vec<WorkerReport>,
    #[serde(default)]
    pub tenants: Vec<TenantReport>,
    pub launches: Launches,
    pub wakes: Wakes,
    pub wake_seconds: Histogram,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_start_time_seconds: Option<f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct D1Stats {
    pub size_bytes: i64,
    pub page_count: i64,
    pub page_size: i64,
    pub table_count: i64,
    pub tables: Vec<String>,
    pub index_count: i64,
    pub user_version: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QueueStats {
    pub backlog: i64,
    pub backlog_bytes: i64,
    pub oldest_message_timestamp_ms: Option<i64>,
    pub visible: i64,
    pub in_flight: i64,
    pub delayed: i64,
    pub dead: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    pub id: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSet {
    /// `false` when the pod's serving desired does not declare the class.
    pub declared: bool,
    pub instances: Vec<Instance>,
    pub instance_count: u64,
    pub storage_bytes: u64,
    pub truncated: bool,
    pub sampled_at_ms: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesReport {
    pub sampled_at_ms: i64,
    #[serde(default)]
    pub d1: BTreeMap<String, D1Stats>,
    #[serde(default)]
    pub queues: BTreeMap<String, QueueStats>,
    /// `workerId → workflowName → instances`.
    #[serde(default)]
    pub workflows: BTreeMap<String, BTreeMap<String, InstanceSet>>,
    /// `workerId → className → instances`.
    #[serde(default)]
    pub durable_objects: BTreeMap<String, BTreeMap<String, InstanceSet>>,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CellReport {
    pub schema_version: u32,
    pub cell_id: String,
    pub sampled_at_ms: i64,
    pub window_ms: i64,
    #[serde(default)]
    pub pods: Vec<PodReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesReport>,
}

// ======================= desired → plan =======================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstanceTarget {
    pub tenant_id: String,
    pub worker_id: String,
    pub name: String,
    pub class_name: String,
    pub class_key: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReportPlan {
    pub d1: Vec<String>,
    pub queues: Vec<String>,
    pub workflows: Vec<InstanceTarget>,
    pub do_classes: Vec<InstanceTarget>,
}

impl ReportPlan {
    /// Derive every resource the cell can report on from the signed desired.
    /// Bindings are the deployment's own declaration; this never invents a
    /// resource the control plane did not deploy here.
    pub fn from_desired(desired: &ComputeDesired) -> Self {
        let mut d1 = BTreeSet::new();
        let mut queues = BTreeSet::new();
        let mut workflows = Vec::new();
        let mut do_classes = Vec::new();
        for tenant in &desired.tenants {
            for worker in &tenant.workers {
                for binding in &worker.bindings {
                    match binding {
                        BindingDesired::D1(b) => {
                            d1.insert(b.database_id.clone());
                        }
                        BindingDesired::Queue(b) => {
                            queues.insert(b.queue_id.clone());
                        }
                        _ => {}
                    }
                }
                for consumer in &worker.queue_consumers {
                    queues.insert(consumer.queue_id.clone());
                }
                for wf in &worker.workflows {
                    let defining = wf
                        .service_worker_id
                        .clone()
                        .unwrap_or_else(|| worker.worker_id.clone());
                    workflows.push(InstanceTarget {
                        tenant_id: tenant.tenant_id.clone(),
                        worker_id: defining,
                        name: wf.name.clone(),
                        class_name: wf.class_name.clone(),
                        class_key: wf.unique_key.clone(),
                    });
                }
                for class in &worker.durable_object_classes {
                    do_classes.push(InstanceTarget {
                        tenant_id: tenant.tenant_id.clone(),
                        worker_id: worker.worker_id.clone(),
                        name: class.class_name.clone(),
                        class_name: class.class_name.clone(),
                        class_key: class.unique_key.clone(),
                    });
                }
            }
        }
        workflows.sort();
        workflows.dedup();
        do_classes.sort();
        do_classes.dedup();
        Self {
            d1: d1.into_iter().collect(),
            queues: queues.into_iter().collect(),
            workflows,
            do_classes,
        }
    }
}

// ======================= delta builder =======================

#[derive(Clone, Debug, Default)]
pub struct PodDelta {
    sampled_at_ms: i64,
    process_start_s: Option<f64>,
    last: BTreeMap<String, f64>,
}

/// Build one pod's delta report and the state for the next tick.
pub fn build_pod_report(
    pod_id: &str,
    samples: &[Sample],
    prev: Option<&PodDelta>,
    now_ms: i64,
) -> (PodReport, PodDelta) {
    let process_start = samples
        .iter()
        .find(|s| s.name == "process_start_time_seconds")
        .map(|s| s.value);
    let first_sample = prev.is_none();
    let reset = prev.is_some_and(|p| match (p.process_start_s, process_start) {
        (Some(a), Some(b)) => (a - b).abs() > 0.5,
        _ => false,
    });
    let window_ms = if first_sample || reset {
        match process_start {
            Some(start) => {
                let start_ms = (start * 1000.0).round() as i64;
                (now_ms - start_ms).clamp(0, MAX_WINDOW_MS)
            }
            None => 0,
        }
    } else {
        (now_ms - prev.map(|p| p.sampled_at_ms).unwrap_or(now_ms)).clamp(0, MAX_WINDOW_MS)
    };

    // One pass: current values + deltas. A series first seen after the
    // initial sample has no baseline: contribution 0, never its lifetime
    // value.
    let mut last = BTreeMap::new();
    let mut values: Vec<(&Sample, f64)> = Vec::with_capacity(samples.len());
    for s in samples {
        let key = sample_key(s);
        let previous = prev.and_then(|p| p.last.get(&key)).copied();
        let delta = if is_gauge(&s.name) {
            s.value
        } else if reset || first_sample {
            s.value
        } else if let Some(p) = previous {
            if s.value >= p {
                s.value - p
            } else {
                // A counter went backwards without a process start change
                // (manual reset/overflow): count the new value, bounded.
                s.value
            }
        } else {
            0.0
        };
        last.insert(key, s.value);
        values.push((s, delta));
    }

    let mut workers: BTreeMap<String, WorkerAcc> = BTreeMap::new();
    let mut tenants: BTreeMap<String, TenantAcc> = BTreeMap::new();
    let mut launches = Launches::default();
    let mut wakes = Wakes::default();
    let mut wake_seconds = HistogramAcc::default();

    for (s, delta) in values {
        match s.name.as_str() {
            "vrt_worker_requests_total" => {
                let Some(w) = s.labels.get("worker_id") else {
                    continue;
                };
                let acc = workers.entry(w.clone()).or_default();
                acc.requests += delta;
                match s.labels.get("status_class").map(String::as_str) {
                    Some("5xx") => {
                        acc.server_errors += delta;
                        acc.error_requests += delta;
                    }
                    Some("error") => acc.error_requests += delta,
                    _ => {}
                }
            }
            "vrt_worker_exceptions_total" => {
                if let Some(w) = s.labels.get("worker_id") {
                    workers.entry(w.clone()).or_default().exceptions += delta;
                }
            }
            "vrt_worker_request_bytes_total" => {
                if let Some(w) = s.labels.get("worker_id") {
                    workers.entry(w.clone()).or_default().request_bytes += delta;
                }
            }
            "vrt_worker_response_bytes_total" => {
                if let Some(w) = s.labels.get("worker_id") {
                    workers.entry(w.clone()).or_default().response_bytes += delta;
                }
            }
            "vrt_worker_inflight_requests" => {
                if let Some(w) = s.labels.get("worker_id") {
                    workers.entry(w.clone()).or_default().inflight += delta;
                }
            }
            "vrt_worker_request_duration_seconds_bucket" => {
                if let (Some(w), Some(le)) = (s.labels.get("worker_id"), s.labels.get("le")) {
                    if let Ok(le) = le.parse::<f64>() {
                        workers
                            .entry(w.clone())
                            .or_default()
                            .duration
                            .bucket(le, delta);
                    }
                }
            }
            "vrt_worker_request_duration_seconds_sum" => {
                if let Some(w) = s.labels.get("worker_id") {
                    workers.entry(w.clone()).or_default().duration.sum += delta;
                }
            }
            "vrt_worker_request_duration_seconds_count" => {
                if let Some(w) = s.labels.get("worker_id") {
                    workers.entry(w.clone()).or_default().duration.count += delta;
                }
            }
            "vrt_tenant_cpu_seconds_total" => {
                if let Some(t) = s.labels.get("tenant_id") {
                    tenants.entry(t.clone()).or_default().cpu_seconds += delta;
                }
            }
            "vrt_tenant_memory_rss_bytes" => {
                if let Some(t) = s.labels.get("tenant_id") {
                    tenants.entry(t.clone()).or_default().memory_rss_bytes += delta;
                }
            }
            "vrt_host_do_objects" | "vrt_host_do_storage_bytes" => {
                let (Some(t), Some(w)) = (s.labels.get("tenant"), s.labels.get("worker")) else {
                    continue;
                };
                let acc = tenants.entry(t.clone()).or_default();
                let entry = acc
                    .do_storage
                    .entry(w.clone())
                    .or_insert_with(|| DoStorage {
                        worker_id: w.clone(),
                        ..Default::default()
                    });
                if s.name == "vrt_host_do_objects" {
                    entry.objects = delta.round().max(0.0) as u64;
                } else {
                    entry.storage_bytes = delta.round().max(0.0) as u64;
                }
            }
            "vrt_host_launches_total" => {
                if s.labels.get("outcome").map(String::as_str) == Some("ready") {
                    launches.ready += delta.round().max(0.0) as u64;
                } else {
                    launches.failed += delta.round().max(0.0) as u64;
                }
            }
            "vrt_host_wake_total" => {
                if s.labels.get("outcome").map(String::as_str) == Some("ok") {
                    wakes.ok += delta.round().max(0.0) as u64;
                } else {
                    wakes.failed += delta.round().max(0.0) as u64;
                }
            }
            "vrt_host_wake_seconds_bucket" => {
                if let Some(le) = s.labels.get("le").and_then(|l| l.parse::<f64>().ok()) {
                    wake_seconds.bucket(le, delta);
                }
            }
            "vrt_host_wake_seconds_sum" => wake_seconds.sum += delta,
            "vrt_host_wake_seconds_count" => wake_seconds.count += delta,
            _ => {}
        }
    }

    let mut worker_reports: Vec<WorkerReport> = workers
        .into_iter()
        .map(|(worker_id, acc)| WorkerReport {
            worker_id,
            requests: acc.requests.round().max(0.0) as u64,
            error_requests: acc.error_requests.round().max(0.0) as u64,
            server_errors: acc.server_errors.round().max(0.0) as u64,
            exceptions: acc.exceptions.round().max(0.0) as u64,
            request_bytes: acc.request_bytes.round().max(0.0) as u64,
            response_bytes: acc.response_bytes.round().max(0.0) as u64,
            inflight: acc.inflight.round().max(0.0) as u64,
            request_duration: acc.duration.finish(),
        })
        .collect();
    worker_reports.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));

    let tenant_reports: Vec<TenantReport> = tenants
        .into_iter()
        .map(|(tenant_id, acc)| {
            let mut do_storage: Vec<DoStorage> = acc.do_storage.into_values().collect();
            do_storage.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
            TenantReport {
                tenant_id,
                cpu_seconds: acc.cpu_seconds.max(0.0),
                memory_rss_bytes: acc.memory_rss_bytes.round().max(0.0) as u64,
                do_storage,
            }
        })
        .collect();

    let start_label = process_start
        .map(|s| format!("{s:.0}"))
        .unwrap_or_else(|| "unknown".into());
    let report = PodReport {
        pod_id: pod_id.to_string(),
        boot_id: format!("{start_label}:{pod_id}"),
        window_ms,
        first_sample,
        reset,
        workers: worker_reports,
        tenants: tenant_reports,
        launches,
        wakes,
        wake_seconds: wake_seconds.finish(),
        process_start_time_seconds: process_start,
    };
    let state = PodDelta {
        sampled_at_ms: now_ms,
        process_start_s: process_start,
        last,
    };
    (report, state)
}

#[derive(Default)]
struct WorkerAcc {
    requests: f64,
    error_requests: f64,
    server_errors: f64,
    exceptions: f64,
    request_bytes: f64,
    response_bytes: f64,
    inflight: f64,
    duration: HistogramAcc,
}

#[derive(Default)]
struct HistogramAcc {
    count: f64,
    sum: f64,
    buckets: BTreeMap<String, (f64, f64)>,
}

impl HistogramAcc {
    fn bucket(&mut self, le: f64, delta: f64) {
        let entry = self.buckets.entry(format!("{le:?}")).or_insert((le, 0.0));
        entry.1 += delta;
    }
    fn finish(self) -> Histogram {
        let mut buckets: Vec<Bucket> = self
            .buckets
            .into_values()
            .map(|(le, count)| Bucket {
                le,
                count: count.round().max(0.0) as u64,
            })
            .collect();
        buckets.sort_by(|a, b| a.le.total_cmp(&b.le));
        Histogram {
            count: self.count.round().max(0.0) as u64,
            sum_seconds: self.sum.max(0.0),
            buckets,
        }
    }
}

#[derive(Default)]
struct TenantAcc {
    cpu_seconds: f64,
    memory_rss_bytes: f64,
    do_storage: BTreeMap<String, DoStorage>,
}

// ======================= collection IO =======================

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsPayload {
    #[serde(default)]
    pub databases: Vec<StatsDatabase>,
    #[serde(default)]
    pub queues: Vec<StatsQueue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsDatabase {
    pub database_id: String,
    #[serde(default)]
    pub stats: Option<VrtDbStats>,
    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VrtDbStats {
    pub database_bytes: i64,
    pub page_count: i64,
    pub page_size: i64,
    pub table_count: i64,
    #[serde(default)]
    pub tables: Vec<String>,
    pub index_count: i64,
    pub user_version: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsQueue {
    pub queue_id: String,
    #[serde(default)]
    pub metrics: Option<VrtQueueMetrics>,
    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VrtQueueMetrics {
    pub backlog_count: i64,
    pub backlog_bytes: i64,
    #[serde(default)]
    pub oldest_message_timestamp_ms: Option<i64>,
    #[serde(default)]
    pub visible_count: i64,
    #[serde(default)]
    pub in_flight_count: i64,
    #[serde(default)]
    pub delayed_count: i64,
    #[serde(default)]
    pub dead_count: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstancesView {
    pub declared: bool,
    #[serde(default)]
    pub instances: Vec<Instance>,
    #[serde(default)]
    pub storage_bytes: u64,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub sampled_at_ms: i64,
}

/// One object's alarm fields from the host alarm scan.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmInstance {
    pub id: String,
    #[serde(default)]
    pub alarm_at_ms: Option<u64>,
    #[serde(default)]
    pub alarm_known: bool,
}

/// `/v1/introspect/alarms` view (same class resolution as instances).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmsView {
    pub declared: bool,
    #[serde(default)]
    pub instances: Vec<AlarmInstance>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub sampled_at_ms: i64,
}

/// One tenant's DO sleep verdict pushed to its pod.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlarmGate {
    pub tenant_id: String,
    pub safe: bool,
    pub ttl_ms: u64,
}

/// Every external dependency of the periodic report. Tests implement this
/// with in-memory data; production uses [`HttpReportIo`].
#[async_trait::async_trait]
pub trait ReportIo: Send + Sync {
    async fn scrape(&self, pod_id: &str) -> Result<String, String>;
    async fn instances(
        &self,
        pod_id: &str,
        target: &InstanceTarget,
    ) -> Result<InstancesView, String>;
    /// Alarm scan for the persistent cell alarm index. Defaults to
    /// unsupported so older hosts/tests fail closed (class marked unknown).
    async fn alarms(&self, _pod_id: &str, _target: &InstanceTarget) -> Result<AlarmsView, String> {
        Err("alarm scan unsupported".into())
    }
    async fn stats(&self, d1: &[String], queues: &[String]) -> Result<StatsPayload, String>;
    async fn push(&self, report: &CellReport) -> Result<(), String>;
    /// Push the tenant's DO alarm-sleep verdict to the pod. Default no-op:
    /// older hosts/tests simply keep DO tenants resident.
    async fn push_alarm_gate(&self, _pod_id: &str, _gate: &AlarmGate) -> Result<(), String> {
        Ok(())
    }
}

fn instance_set(mut view: InstancesView, budget: usize) -> InstanceSet {
    let limit = budget.min(MAX_INSTANCES_PER_CLASS);
    if view.instances.len() > limit {
        view.instances.truncate(limit);
        view.truncated = true;
    }
    InstanceSet {
        declared: view.declared,
        instance_count: view.instances.len() as u64,
        instances: view.instances,
        storage_bytes: view.storage_bytes,
        truncated: view.truncated,
        sampled_at_ms: view.sampled_at_ms,
    }
}

/// Last-resort shrink for an oversize report: drop the point-in-time resource
/// lists (metrics are the important part) and mark them truncated. If the
/// document is still too large, the caller drops `resources` entirely.
fn bounded_error(value: impl std::fmt::Display) -> String {
    value.to_string().chars().take(MAX_ERROR_CHARS).collect()
}

fn shrink_report(mut report: CellReport) -> CellReport {
    if let Some(resources) = &mut report.resources {
        for classes in resources
            .workflows
            .values_mut()
            .chain(resources.durable_objects.values_mut())
        {
            for set in classes.values_mut() {
                set.instances.clear();
                set.instance_count = 0;
                set.truncated = true;
            }
        }
        for stats in resources.d1.values_mut() {
            stats.tables.clear();
        }
    }
    report
}

// ======================= reporter =======================

#[derive(Debug, Default)]
pub struct TickReport {
    pub pods: usize,
    pub d1: usize,
    pub queues: usize,
    pub workflows: usize,
    pub do_classes: usize,
    pub errors: Vec<String>,
}

pub struct Reporter<T: ReportIo> {
    io: T,
    cell_id: String,
    state: BTreeMap<String, PodDelta>,
    last_tick_ms: Option<i64>,
    /// Optional Durable Object alarm index fed by the same pod scans.
    alarm_index: Option<Arc<AlarmIndex>>,
}

impl<T: ReportIo> Reporter<T> {
    pub fn new(io: T, cell_id: impl Into<String>) -> Self {
        Self {
            io,
            cell_id: cell_id.into(),
            state: BTreeMap::new(),
            last_tick_ms: None,
            alarm_index: None,
        }
    }

    /// Fold DO alarm scans into the persistent cell index on every tick.
    pub fn with_alarm_index(mut self, index: Arc<AlarmIndex>) -> Self {
        self.alarm_index = Some(index);
        self
    }

    /// Update the index from one pod's alarm view. A failed scan, undeclared
    /// class or truncated listing marks the class unknown (fail closed).
    async fn observe_alarms(&self, pod: &str, target: &InstanceTarget, now_ms: i64) {
        let Some(index) = &self.alarm_index else {
            return;
        };
        let class = AlarmClass {
            tenant_id: target.tenant_id.clone(),
            worker_id: target.worker_id.clone(),
            class_key: target.class_key.clone(),
        };
        let now = now_ms.max(0) as u64;
        match self.io.alarms(pod, target).await {
            Ok(view) if view.declared && !view.truncated => {
                let observations: Vec<AlarmObservation> = view
                    .instances
                    .into_iter()
                    .map(|instance| AlarmObservation {
                        object_id: instance.id,
                        at_ms: instance.alarm_at_ms,
                        known: instance.alarm_known,
                    })
                    .collect();
                if let Err(e) = index.observe_class(&class, &observations, now) {
                    tracing::warn!(tenant = %class.tenant_id, class = %class.class_key,
                        error = %e, "do alarm index persist failed");
                }
            }
            Ok(view) => {
                tracing::debug!(
                    tenant = %class.tenant_id,
                    class = %class.class_key,
                    declared = view.declared,
                    truncated = view.truncated,
                    "do alarm scan incomplete; marking class unknown"
                );
                if let Err(e) = index.mark_unknown(&class, now) {
                    tracing::warn!(tenant = %class.tenant_id, class = %class.class_key,
                        error = %e, "do alarm index persist failed");
                }
            }
            Err(e) => {
                tracing::debug!(tenant = %class.tenant_id, class = %class.class_key,
                    error = %e, "do alarm scan failed; marking class unknown");
                if let Err(e) = index.mark_unknown(&class, now) {
                    tracing::warn!(tenant = %class.tenant_id, class = %class.class_key,
                        error = %e, "do alarm index persist failed");
                }
            }
        }
    }

    /// Derive and push one tenant gate per pod from the alarm index. A tenant
    /// with any declared DO class that is not fully scanned, unknown, or has an
    /// alarm inside the lead window is published as unsafe.
    async fn publish_alarm_gates(
        &self,
        plan: &ReportPlan,
        assignments: &BTreeMap<String, PodAssignment>,
        now_ms: i64,
    ) {
        let Some(index) = &self.alarm_index else {
            return;
        };
        let now = now_ms.max(0) as u64;
        for assignment in assignments.values() {
            let mut by_tenant: BTreeMap<String, Vec<&InstanceTarget>> = BTreeMap::new();
            for target in &plan.do_classes {
                if assignment
                    .workers
                    .iter()
                    .any(|w| w.tenant_id == target.tenant_id && w.worker_id == target.worker_id)
                {
                    by_tenant
                        .entry(target.tenant_id.clone())
                        .or_default()
                        .push(target);
                }
            }
            for (tenant_id, targets) in by_tenant {
                let safe = targets.iter().all(|target| {
                    let class = AlarmClass {
                        tenant_id: target.tenant_id.clone(),
                        worker_id: target.worker_id.clone(),
                        class_key: target.class_key.clone(),
                    };
                    index.is_safe_to_sleep_stage2(&class, now, ALARM_SAFETY_LEAD_MS)
                });
                let gate = AlarmGate {
                    tenant_id,
                    safe,
                    ttl_ms: ALARM_GATE_TTL_MS,
                };
                if let Err(e) = self.io.push_alarm_gate(&assignment.pod_id, &gate).await {
                    tracing::debug!(pod = %assignment.pod_id, tenant = %gate.tenant_id,
                        error = %e, "do alarm gate push failed; tenant stays resident");
                }
            }
        }
    }

    /// Collect one report for the pod assignments the controller currently
    /// holds, then push it. A failed push keeps counter state, so the next
    /// successful push covers the lost window (eventual, not lost metrics).
    pub async fn tick(
        &mut self,
        desired: &ComputeDesired,
        assignments: &BTreeMap<String, PodAssignment>,
        now_ms: i64,
    ) -> Result<TickReport, String> {
        let plan = ReportPlan::from_desired(desired);
        let mut report = TickReport::default();
        let mut pods = Vec::new();
        for pod_id in assignments.keys() {
            match self.io.scrape(pod_id).await {
                Ok(text) => {
                    let samples = parse_exposition(&text);
                    let (pod, state) =
                        build_pod_report(pod_id, &samples, self.state.get(pod_id), now_ms);
                    self.state.insert(pod_id.clone(), state);
                    report.pods += 1;
                    pods.push(pod);
                }
                Err(e) => report.errors.push(format!("scrape {pod_id}: {e}")),
            }
        }

        let mut resources = ResourcesReport {
            sampled_at_ms: now_ms,
            ..Default::default()
        };
        let mut instance_budget = MAX_TOTAL_INSTANCES;
        let mut table_budget = MAX_TOTAL_TABLES;
        if !plan.d1.is_empty() || !plan.queues.is_empty() {
            match self.io.stats(&plan.d1, &plan.queues).await {
                Ok(stats) => {
                    for entry in stats.databases {
                        match entry.stats {
                            Some(s) => {
                                report.d1 += 1;
                                let mut tables = s.tables;
                                let allowed = table_budget.min(MAX_TABLES_PER_DB);
                                if tables.len() > allowed {
                                    tables.truncate(allowed);
                                }
                                table_budget = table_budget.saturating_sub(tables.len());
                                resources.d1.insert(
                                    entry.database_id,
                                    D1Stats {
                                        size_bytes: s.database_bytes,
                                        page_count: s.page_count,
                                        page_size: s.page_size,
                                        table_count: s.table_count,
                                        tables,
                                        index_count: s.index_count,
                                        user_version: s.user_version,
                                    },
                                );
                            }
                            None => report.errors.push(format!(
                                "d1 {}: {}",
                                entry.database_id,
                                entry
                                    .error
                                    .map(bounded_error)
                                    .unwrap_or_else(|| "unknown error".into())
                            )),
                        }
                    }
                    for entry in stats.queues {
                        match entry.metrics {
                            Some(m) => {
                                report.queues += 1;
                                resources.queues.insert(
                                    entry.queue_id,
                                    QueueStats {
                                        backlog: m.backlog_count,
                                        backlog_bytes: m.backlog_bytes,
                                        oldest_message_timestamp_ms: m.oldest_message_timestamp_ms,
                                        visible: m.visible_count,
                                        in_flight: m.in_flight_count,
                                        delayed: m.delayed_count,
                                        dead: m.dead_count,
                                    },
                                );
                            }
                            None => report.errors.push(format!(
                                "queue {}: {}",
                                entry.queue_id,
                                entry
                                    .error
                                    .map(bounded_error)
                                    .unwrap_or_else(|| "unknown error".into())
                            )),
                        }
                    }
                }
                Err(e) => report.errors.push(format!("stats: {e}")),
            }
        }

        let pod_of = |worker_id: &str| -> Option<String> {
            assignments
                .values()
                .find(|a| a.workers.iter().any(|w| w.worker_id == worker_id))
                .map(|a| a.pod_id.clone())
        };
        for target in &plan.workflows {
            if instance_budget == 0 {
                report
                    .errors
                    .push("instances: report budget exhausted; remaining classes skipped".into());
                break;
            }
            let Some(pod) = pod_of(&target.worker_id) else {
                report
                    .errors
                    .push(format!("workflow {}: worker not assigned", target.name));
                continue;
            };
            match self.io.instances(&pod, target).await {
                Ok(view) => {
                    let set = instance_set(view, instance_budget);
                    instance_budget = instance_budget.saturating_sub(set.instances.len());
                    report.workflows += 1;
                    resources
                        .workflows
                        .entry(target.worker_id.clone())
                        .or_default()
                        .insert(target.name.clone(), set);
                }
                Err(e) => report.errors.push(format!("workflow {}: {e}", target.name)),
            }
            self.observe_alarms(&pod, target, now_ms).await;
        }
        for target in &plan.do_classes {
            if instance_budget == 0 {
                report
                    .errors
                    .push("instances: report budget exhausted; remaining classes skipped".into());
                break;
            }
            let Some(pod) = pod_of(&target.worker_id) else {
                report
                    .errors
                    .push(format!("do class {}: worker not assigned", target.name));
                continue;
            };
            match self.io.instances(&pod, target).await {
                Ok(view) => {
                    let set = instance_set(view, instance_budget);
                    instance_budget = instance_budget.saturating_sub(set.instances.len());
                    report.do_classes += 1;
                    resources
                        .durable_objects
                        .entry(target.worker_id.clone())
                        .or_default()
                        .insert(target.class_name.clone(), set);
                }
                Err(e) => report.errors.push(format!("do class {}: {e}", target.name)),
            }
            self.observe_alarms(&pod, target, now_ms).await;
        }
        self.publish_alarm_gates(&plan, assignments, now_ms).await;
        if report.errors.len() > MAX_REPORT_ERRORS {
            report.errors.truncate(MAX_REPORT_ERRORS - 1);
            report.errors.push("errors truncated".into());
        }
        resources.errors = report.errors.clone();

        let window_ms = self
            .last_tick_ms
            .map(|prev| (now_ms - prev).clamp(0, MAX_WINDOW_MS))
            .unwrap_or(0);
        let mut report_body = CellReport {
            schema_version: REPORT_SCHEMA_VERSION,
            cell_id: self.cell_id.clone(),
            sampled_at_ms: now_ms,
            window_ms,
            pods,
            resources: Some(resources),
        };
        let mut bytes =
            serde_json::to_vec(&report_body).map_err(|e| format!("encode report: {e}"))?;
        if bytes.len() > MAX_REPORT_BYTES {
            tracing::warn!(
                bytes = bytes.len(),
                "cell report oversize — dropping point-in-time resource lists"
            );
            report_body = shrink_report(report_body);
            bytes = serde_json::to_vec(&report_body).map_err(|e| format!("encode report: {e}"))?;
        }
        if bytes.len() > MAX_REPORT_BYTES {
            report_body.resources = None;
            bytes = serde_json::to_vec(&report_body).map_err(|e| format!("encode report: {e}"))?;
        }
        if bytes.len() > MAX_REPORT_BYTES {
            return Err(format!("report too large ({} bytes)", bytes.len()));
        }
        self.io.push(&report_body).await?;
        self.last_tick_ms = Some(now_ms);
        Ok(report)
    }
}

// ======================= production HTTP =======================

pub struct HttpReportIo {
    http: reqwest::Client,
    pods: BTreeMap<String, String>,
    control_plane_url: String,
    cell_id: String,
    token: String,
    stats: Option<Box<crate::d1ops::D1OpsExecutor>>,
    timeout: Duration,
}

impl HttpReportIo {
    pub fn new(
        pods: &[(String, String)],
        control_plane_url: &str,
        cell_id: &str,
        token: String,
        stats: Option<crate::d1ops::D1OpsExecutor>,
        timeout: Duration,
    ) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(2)))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("report http: {e}"))?;
        let mut map = BTreeMap::new();
        for (id, base) in pods {
            let base = base.trim_end_matches('/').to_string();
            let url = reqwest::Url::parse(&base).map_err(|e| format!("pod {id}: {e}"))?;
            if url.scheme() != "http" {
                return Err(format!("pod {id} must be http"));
            }
            map.insert(id.clone(), base);
        }
        Ok(Self {
            http,
            pods: map,
            control_plane_url: control_plane_url.trim_end_matches('/').to_string(),
            cell_id: cell_id.to_string(),
            token,
            stats: stats.map(Box::new),
            timeout,
        })
    }

    fn pod(&self, pod_id: &str) -> Result<&str, String> {
        self.pods
            .get(pod_id)
            .map(String::as_str)
            .ok_or_else(|| format!("unknown pod {pod_id}"))
    }
}

#[async_trait::async_trait]
impl ReportIo for HttpReportIo {
    async fn scrape(&self, pod_id: &str) -> Result<String, String> {
        let url = format!("{}/metrics", self.pod(pod_id)?);
        let resp = tokio::time::timeout(self.timeout, self.http.get(&url).send())
            .await
            .map_err(|_| "metrics timeout".to_string())?
            .map_err(|e| format!("metrics transport: {e}"))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("metrics body: {e}"))?;
        if !status.is_success() {
            return Err(format!("metrics status {status}"));
        }
        if bytes.len() > MAX_SCRAPE_BYTES {
            return Err("metrics body too large".into());
        }
        String::from_utf8(bytes.to_vec()).map_err(|_| "metrics body is not UTF-8".into())
    }

    async fn instances(
        &self,
        pod_id: &str,
        target: &InstanceTarget,
    ) -> Result<InstancesView, String> {
        let url = format!("{}/v1/introspect/instances", self.pod(pod_id)?);
        let body = serde_json::json!({
            "tenantId": target.tenant_id,
            "workerId": target.worker_id,
            "classKey": target.class_key,
        });
        let resp = tokio::time::timeout(
            self.timeout,
            self.http
                .post(&url)
                .header("content-type", "application/json")
                .json(&body)
                .send(),
        )
        .await
        .map_err(|_| "instances timeout".to_string())?
        .map_err(|e| format!("instances transport: {e}"))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("instances body: {e}"))?;
        if !status.is_success() {
            return Err(format!("instances status {status}"));
        }
        if bytes.len() > MAX_INSTANCE_BYTES {
            return Err("instances body too large".into());
        }
        serde_json::from_slice(&bytes).map_err(|e| format!("instances decode: {e}"))
    }

    async fn alarms(&self, pod_id: &str, target: &InstanceTarget) -> Result<AlarmsView, String> {
        let url = format!("{}/v1/introspect/alarms", self.pod(pod_id)?);
        let body = serde_json::json!({
            "tenantId": target.tenant_id,
            "workerId": target.worker_id,
            "classKey": target.class_key,
        });
        let resp = tokio::time::timeout(
            self.timeout,
            self.http
                .post(&url)
                .header("content-type", "application/json")
                .json(&body)
                .send(),
        )
        .await
        .map_err(|_| "alarms timeout".to_string())?
        .map_err(|e| format!("alarms transport: {e}"))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("alarms body: {e}"))?;
        if !status.is_success() {
            return Err(format!("alarms status {status}"));
        }
        if bytes.len() > MAX_INSTANCE_BYTES {
            return Err("alarms body too large".into());
        }
        serde_json::from_slice(&bytes).map_err(|e| format!("alarms decode: {e}"))
    }

    async fn push_alarm_gate(&self, pod_id: &str, gate: &AlarmGate) -> Result<(), String> {
        let url = format!("{}/v1/do-alarm/gate", self.pod(pod_id)?);
        let resp = tokio::time::timeout(
            self.timeout,
            self.http
                .post(&url)
                .header("content-type", "application/json")
                .json(gate)
                .send(),
        )
        .await
        .map_err(|_| "alarm gate timeout".to_string())?
        .map_err(|e| format!("alarm gate transport: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("alarm gate status {}", resp.status()));
        }
        Ok(())
    }

    async fn stats(&self, d1: &[String], queues: &[String]) -> Result<StatsPayload, String> {
        let Some(stats) = &self.stats else {
            return Ok(StatsPayload::default());
        };
        let value = stats.stats(d1, queues).await?;
        serde_json::from_value(value).map_err(|e| format!("stats decode: {e}"))
    }

    async fn push(&self, report: &CellReport) -> Result<(), String> {
        let url = format!(
            "{}/api/internal/clusters/{}/vrt/metrics",
            self.control_plane_url, self.cell_id
        );
        let bytes = serde_json::to_vec(report).map_err(|e| format!("encode: {e}"))?;
        if bytes.len() > MAX_REPORT_BYTES {
            return Err("report too large".into());
        }
        let resp = tokio::time::timeout(
            self.timeout,
            self.http
                .post(&url)
                .header("authorization", format!("Bearer {}", self.token))
                .header("content-type", "application/json")
                .body(bytes)
                .send(),
        )
        .await
        .map_err(|_| "report push timeout".to_string())?
        .map_err(|e| format!("report push transport: {e}"))?;
        let status = resp.status();
        // Drain a bounded body so the connection is reusable; the body is
        // diagnostic only.
        let _ = resp.bytes().await;
        if !status.is_success() {
            return Err(format!("report push status {status}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_labels_escapes_and_values() {
        let text = r#"
# HELP vrt_worker_requests_total Requests
vrt_worker_requests_total{cell_id="c",worker_id="team:w",status_class="2xx"} 12
vrt_worker_request_duration_seconds_bucket{le="0.005",worker_id="team:w"} 3
vrt_host_build_info{version="1.2.3"} 1 1760000000
bad line
vrt_worker_inflight_requests{worker_id="a\"b"} 2
"#;
        let samples = parse_exposition(text);
        assert_eq!(samples.len(), 4, "{samples:?}");
        assert_eq!(samples[0].name, "vrt_worker_requests_total");
        assert_eq!(samples[0].labels["status_class"], "2xx");
        assert_eq!(samples[0].value, 12.0);
        assert_eq!(samples[1].labels["le"], "0.005");
        assert_eq!(samples[2].labels["version"], "1.2.3");
        assert_eq!(samples[2].value, 1.0);
        assert_eq!(samples[3].labels["worker_id"], "a\"b");
    }

    fn samples(counter: f64) -> Vec<Sample> {
        parse_exposition(&format!(
            r#"
process_start_time_seconds 1000
vrt_worker_requests_total{{worker_id="team:w",status_class="2xx"}} {counter}
vrt_worker_requests_total{{worker_id="team:w",status_class="5xx"}} 1
vrt_worker_exceptions_total{{worker_id="team:w",reason="upstream_timeout"}} 1
vrt_worker_request_duration_seconds_bucket{{worker_id="team:w",le="0.005"}} 2
vrt_worker_request_duration_seconds_bucket{{worker_id="team:w",le="0.05"}} 3
vrt_worker_request_duration_seconds_sum{{worker_id="team:w"}} 0.06
vrt_worker_request_duration_seconds_count{{worker_id="team:w"}} 3
vrt_worker_inflight_requests{{worker_id="team:w"}} 2
vrt_tenant_cpu_seconds_total{{tenant_id="team"}} {counter}
vrt_tenant_memory_rss_bytes{{tenant_id="team"}} 1024
vrt_host_do_objects{{tenant="team",worker="team:w"}} 4
vrt_host_do_storage_bytes{{tenant="team",worker="team:w"}} 8192
vrt_host_launches_total{{outcome="ready",reason="none"}} 2
vrt_host_wake_total{{source="request",outcome="ok"}} 1
vrt_host_wake_seconds_bucket{{le="0.05"}} 1
vrt_host_wake_seconds_sum 0.02
vrt_host_wake_seconds_count 1
"#
        ))
    }

    #[test]
    fn first_sample_uses_process_uptime_and_next_uses_delta() {
        let (first, state) = build_pod_report("pod-0", &samples(10.0), None, 1_060_000);
        assert!(first.first_sample);
        assert!(!first.reset);
        assert_eq!(first.window_ms, 60_000);
        assert_eq!(first.workers[0].requests, 11);
        assert_eq!(first.workers[0].server_errors, 1);
        assert_eq!(first.workers[0].error_requests, 1);
        assert_eq!(first.workers[0].request_duration.count, 3);
        assert_eq!(first.workers[0].request_duration.buckets.len(), 2);
        assert_eq!(first.workers[0].inflight, 2);
        assert_eq!(first.tenants[0].cpu_seconds, 10.0);
        assert_eq!(first.tenants[0].do_storage[0].objects, 4);
        assert_eq!(first.tenants[0].do_storage[0].storage_bytes, 8192);
        assert_eq!(first.launches.ready, 2);
        assert_eq!(first.wakes.ok, 1);
        assert_eq!(first.wake_seconds.count, 1);

        let (second, _) = build_pod_report("pod-0", &samples(16.0), Some(&state), 1_090_000);
        assert!(!second.first_sample);
        assert_eq!(second.window_ms, 30_000);
        assert_eq!(second.workers[0].requests, 6); // 2xx delta only; the 5xx series did not move
        assert_eq!(second.tenants[0].cpu_seconds, 6.0);
        assert_eq!(second.launches.ready, 0); // unchanged counter
        assert_eq!(second.workers[0].inflight, 2); // gauge, not a delta
    }

    #[test]
    fn process_restart_marks_reset_and_rebaselines() {
        let (_, state) = build_pod_report("pod-0", &samples(10.0), None, 1_060_000);
        let mut restarted = samples(1.0);
        for s in &mut restarted {
            if s.name == "process_start_time_seconds" {
                s.value = 2000.0;
            }
        }
        let (report, _) = build_pod_report("pod-0", &restarted, Some(&state), 1_100_000);
        assert!(report.reset);
        assert_eq!(report.window_ms, 0.max(1_100_000 - 2_000_000));
        assert_eq!(report.workers[0].requests, 2); // current values counted again
    }

    #[test]
    fn new_series_after_first_sample_contributes_zero() {
        let (_, state) = build_pod_report("pod-0", &samples(10.0), None, 1_060_000);
        let mut later = samples(10.0);
        later.push(Sample {
            name: "vrt_worker_requests_total".into(),
            labels: BTreeMap::from([
                ("worker_id".into(), "team:new".into()),
                ("status_class".into(), "2xx".into()),
            ]),
            value: 99.0,
        });
        let (report, _) = build_pod_report("pod-0", &later, Some(&state), 1_090_000);
        let new = report
            .workers
            .iter()
            .find(|w| w.worker_id == "team:new")
            .unwrap();
        assert_eq!(new.requests, 0);
    }

    #[test]
    fn wire_fixture_matches_types_exactly() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../fixtures/vrt-wire/cell-report.json")).unwrap();
        let report: CellReport = serde_json::from_value(fixture.clone()).unwrap();
        assert_eq!(report.schema_version, REPORT_SCHEMA_VERSION);
        assert_eq!(serde_json::to_value(&report).unwrap(), fixture);
    }

    #[test]
    fn instance_budget_truncates_and_marks_the_set() {
        let view = InstancesView {
            declared: true,
            instances: vec![
                Instance {
                    id: "a".repeat(64),
                    bytes: 1,
                },
                Instance {
                    id: "b".repeat(64),
                    bytes: 2,
                },
                Instance {
                    id: "c".repeat(64),
                    bytes: 3,
                },
            ],
            storage_bytes: 6,
            truncated: false,
            sampled_at_ms: 1,
        };
        let set = instance_set(view, 2);
        assert_eq!(set.instance_count, 2);
        assert_eq!(set.instances.len(), 2);
        assert!(set.truncated);
    }

    #[test]
    fn errors_are_bounded() {
        let long = "x".repeat(MAX_ERROR_CHARS * 10);
        assert_eq!(bounded_error(long).chars().count(), MAX_ERROR_CHARS);
    }

    #[test]
    fn shrink_report_clears_point_in_time_lists() {
        let report: CellReport =
            serde_json::from_str(include_str!("../../fixtures/vrt-wire/cell-report.json")).unwrap();
        let shrunk = shrink_report(report);
        let resources = shrunk.resources.unwrap();
        for set in resources
            .workflows
            .values()
            .flat_map(|m| m.values())
            .chain(resources.durable_objects.values().flat_map(|m| m.values()))
        {
            assert!(set.instances.is_empty());
            assert_eq!(set.instance_count, 0);
            assert!(set.truncated);
        }
        for stats in resources.d1.values() {
            assert!(stats.tables.is_empty());
        }
    }

    #[test]
    fn plan_derives_resources_from_desired() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../snapshot-protocol/fixtures/ts_compute_desired.json"
        ))
        .unwrap();
        let envelope: serde_json::Value =
            serde_json::from_str(fixture["envelope"].as_str().unwrap()).unwrap();
        let desired: ComputeDesired = serde_json::from_value(envelope["payload"].clone()).unwrap();
        let plan = ReportPlan::from_desired(&desired);
        assert!(plan.workflows.iter().any(|w| w.name == "notify-flow"));
        assert!(plan.workflows.iter().any(|w| w.name == "order-pipeline"));
        assert!(plan
            .do_classes
            .iter()
            .any(|d| d.class_name == "Counter" && d.worker_id == "worker-a"));
        assert!(plan
            .do_classes
            .iter()
            .any(|d| d.class_name == "Shared" && d.worker_id == "worker-b"));
        assert!(plan.d1.is_empty());
        assert!(plan.queues.is_empty());
    }

    struct MemoryIo {
        pushes: std::sync::Arc<std::sync::Mutex<Vec<CellReport>>>,
        gates: std::sync::Arc<std::sync::Mutex<Vec<(String, AlarmGate)>>>,
    }

    #[async_trait::async_trait]
    impl ReportIo for MemoryIo {
        async fn scrape(&self, _pod_id: &str) -> Result<String, String> {
            Ok("process_start_time_seconds 1000\nvrt_worker_requests_total{worker_id=\"team:w\",status_class=\"2xx\"} 3".into())
        }
        async fn instances(
            &self,
            _pod_id: &str,
            _target: &InstanceTarget,
        ) -> Result<InstancesView, String> {
            Ok(InstancesView {
                declared: true,
                instances: vec![Instance {
                    id: "a".repeat(64),
                    bytes: 4096,
                }],
                storage_bytes: 4096,
                truncated: false,
                sampled_at_ms: 1,
            })
        }
        async fn alarms(
            &self,
            _pod_id: &str,
            _target: &InstanceTarget,
        ) -> Result<AlarmsView, String> {
            Ok(AlarmsView {
                declared: true,
                instances: vec![AlarmInstance {
                    id: "a".repeat(64),
                    alarm_at_ms: Some(42),
                    alarm_known: true,
                }],
                truncated: false,
                sampled_at_ms: 1,
            })
        }
        async fn stats(&self, d1: &[String], queues: &[String]) -> Result<StatsPayload, String> {
            Ok(StatsPayload {
                databases: d1
                    .iter()
                    .map(|id| StatsDatabase {
                        database_id: id.clone(),
                        stats: Some(VrtDbStats {
                            database_bytes: 4096,
                            page_count: 1,
                            page_size: 4096,
                            table_count: 2,
                            tables: vec!["t".into()],
                            index_count: 0,
                            user_version: 0,
                        }),
                        error: None,
                    })
                    .collect(),
                queues: queues
                    .iter()
                    .map(|id| StatsQueue {
                        queue_id: id.clone(),
                        metrics: Some(VrtQueueMetrics {
                            backlog_count: 2,
                            backlog_bytes: 20,
                            oldest_message_timestamp_ms: Some(1000),
                            visible_count: 1,
                            in_flight_count: 1,
                            delayed_count: 0,
                            dead_count: 0,
                        }),
                        error: None,
                    })
                    .collect(),
            })
        }
        async fn push(&self, report: &CellReport) -> Result<(), String> {
            self.pushes.lock().unwrap().push(report.clone());
            Ok(())
        }
        async fn push_alarm_gate(&self, pod_id: &str, gate: &AlarmGate) -> Result<(), String> {
            self.gates
                .lock()
                .unwrap()
                .push((pod_id.to_string(), gate.clone()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn reporter_collects_pods_resources_and_pushes_once() {
        let desired = {
            let fixture: serde_json::Value = serde_json::from_str(include_str!(
                "../../snapshot-protocol/fixtures/ts_compute_desired.json"
            ))
            .unwrap();
            let envelope: serde_json::Value =
                serde_json::from_str(fixture["envelope"].as_str().unwrap()).unwrap();
            serde_json::from_value::<ComputeDesired>(envelope["payload"].clone()).unwrap()
        };
        let assignments = BTreeMap::from([(
            "pod-0".to_string(),
            PodAssignment {
                pod_id: "pod-0".into(),
                revision: 1,
                payload_digest: "a".repeat(64),
                envelope_digest: String::new(),
                generated_at: String::new(),
                workers: vec![
                    crate::assign::AssignedWorker {
                        tenant_id: "team-001".into(),
                        worker_id: "worker-a".into(),
                    },
                    crate::assign::AssignedWorker {
                        tenant_id: "team-001".into(),
                        worker_id: "worker-b".into(),
                    },
                ],
            },
        )]);
        let pushes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut reporter = Reporter::new(
            MemoryIo {
                pushes: pushes.clone(),
                gates: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            },
            "cell-001",
        );
        let tick = reporter
            .tick(&desired, &assignments, 1_060_000)
            .await
            .unwrap();
        assert_eq!(tick.pods, 1);
        assert_eq!(tick.workflows, 2);
        assert_eq!(tick.do_classes, 2);
        assert_eq!(tick.d1, 0);
        let pushed = pushes.lock().unwrap();
        assert_eq!(pushed.len(), 1);
        let report = &pushed[0];
        assert_eq!(report.cell_id, "cell-001");
        assert_eq!(report.pods[0].workers[0].requests, 3);
        assert_eq!(
            report.resources.as_ref().unwrap().workflows["worker-a"]
                .get("notify-flow")
                .unwrap()
                .instance_count,
            1
        );
    }

    #[tokio::test]
    async fn reporter_feeds_the_persistent_do_alarm_index() {
        let desired = {
            let fixture: serde_json::Value = serde_json::from_str(include_str!(
                "../../snapshot-protocol/fixtures/ts_compute_desired.json"
            ))
            .unwrap();
            let envelope: serde_json::Value =
                serde_json::from_str(fixture["envelope"].as_str().unwrap()).unwrap();
            serde_json::from_value::<ComputeDesired>(envelope["payload"].clone()).unwrap()
        };
        let assignments = BTreeMap::from([(
            "pod-0".to_string(),
            PodAssignment {
                pod_id: "pod-0".into(),
                revision: 1,
                payload_digest: "a".repeat(64),
                envelope_digest: String::new(),
                generated_at: String::new(),
                workers: vec![
                    crate::assign::AssignedWorker {
                        tenant_id: "team-001".into(),
                        worker_id: "worker-a".into(),
                    },
                    crate::assign::AssignedWorker {
                        tenant_id: "team-001".into(),
                        worker_id: "worker-b".into(),
                    },
                ],
            },
        )]);
        let tmp = tempfile::tempdir().unwrap();
        let index = std::sync::Arc::new(AlarmIndex::open(Some(crate::alarm_index::index_path(
            tmp.path(),
        ))));
        let pushes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let gates = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut reporter = Reporter::new(
            MemoryIo {
                pushes: pushes.clone(),
                gates: gates.clone(),
            },
            "cell-001",
        )
        .with_alarm_index(index.clone());
        reporter
            .tick(&desired, &assignments, 1_060_000)
            .await
            .unwrap();
        assert_eq!(index.earliest().unwrap().at_ms, 42);
        assert!(!index.snapshot().is_empty());
        let gates = gates.lock().unwrap();
        assert!(!gates.is_empty());
        assert!(
            gates.iter().all(|(_, g)| !g.safe),
            "a due alarm must publish an unsafe gate"
        );
        // The file is written in the cell state dir and reloads.
        let reopened = AlarmIndex::open(Some(crate::alarm_index::index_path(tmp.path())));
        assert_eq!(reopened.earliest().unwrap().at_ms, 42);
    }

    #[tokio::test]
    async fn reporter_fails_soft_when_one_pod_is_down() {
        struct DownIo;
        #[async_trait::async_trait]
        impl ReportIo for DownIo {
            async fn scrape(&self, _: &str) -> Result<String, String> {
                Err("boom".into())
            }
            async fn instances(
                &self,
                _: &str,
                _: &InstanceTarget,
            ) -> Result<InstancesView, String> {
                Err("boom".into())
            }
            async fn stats(&self, _: &[String], _: &[String]) -> Result<StatsPayload, String> {
                Err("boom".into())
            }
            async fn push(&self, _: &CellReport) -> Result<(), String> {
                Ok(())
            }
        }
        let desired: ComputeDesired = serde_json::from_value(serde_json::json!({
            "schemaVersion": 1,
            "cellId": "cell-001",
            "revision": 1,
            "tenants": [{
                "tenantId": "team",
                "workers": [{
                    "workerId": "worker-a",
                    "name": "a",
                    "activeVersion": {
                        "versionId": "v", "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2024-09-25", "compatibilityFlags": []
                    },
                    "bindings": [{"type":"d1","name":"DB","databaseId":"b".repeat(26),"epoch":1,"permissions":{"read":true,"write":true}}],
                    "routes": [], "crons": [], "quotas": {
                        "cpuMs":50,"memoryMb":128,"outboundConnections":16,"subrequests":50
                    }
                }]
            }]
        }))
        .unwrap();
        let assignments = BTreeMap::from([(
            "pod-0".into(),
            PodAssignment {
                pod_id: "pod-0".into(),
                revision: 1,
                payload_digest: "a".repeat(64),
                envelope_digest: String::new(),
                generated_at: String::new(),
                workers: vec![crate::assign::AssignedWorker {
                    tenant_id: "team".into(),
                    worker_id: "worker-a".into(),
                }],
            },
        )]);
        let mut reporter = Reporter::new(DownIo, "cell-001");
        let tick = reporter.tick(&desired, &assignments, 1_000).await.unwrap();
        assert_eq!(tick.pods, 0);
        assert!(!tick.errors.is_empty());
    }
}
