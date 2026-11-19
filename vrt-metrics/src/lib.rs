//! B12: minimal Prometheus text-exposition registry — pure std, no
//! external crates (none vendored in Cargo.lock).
//!
//! Design rules enforced here:
//! - **Bounded cardinality**: label *names* are compile-time string
//!   literals at call sites; each metric name caps at
//!   [`MAX_SERIES_PER_METRIC`] distinct label sets — beyond that the
//!   series folds into `worker_id="_overflow"` (or `tenant_id`/`_other`
//!   when the name lacks a worker label) and the process overflow
//!   counter increments once per folded series creation.
//! - **Fixed buckets** — no summaries.
//! - Label values are escaped on encode (`\`, `"`, newline).
//!
//! Callers must only ever pass bounded label values: platform
//! `worker_id`/`tenant_id` from the signed desired, `&'static str`
//! enums for status/outcome/reason, NEVER request paths, hosts or
//! bodies.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

/// Distinct series allowed per metric name before folding to
/// `_overflow` (per B12 cardinality budget).
pub const MAX_SERIES_PER_METRIC: usize = 1024;

pub mod sys;

/// One label pair; values may be dynamic but must be bounded upstream.
pub type LabelSet<'a> = &'a [(&'static str, &'a str)];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    metric: &'static str,
    labels: Vec<(String, String)>,
}

/// Monotonic counter (f64 accumulate so sub-second sums work too).
#[derive(Debug)]
pub struct Counter {
    v: AtomicU64, // stores f64 bits
}
impl Counter {
    fn new() -> Self {
        Self {
            v: AtomicU64::new(0f64.to_bits()),
        }
    }
    /// Add `n` (must be ≥ 0 and finite).
    pub fn inc(&self, n: f64) {
        debug_assert!(n >= 0.0 && n.is_finite());
        let mut cur = self.v.load(Ordering::Relaxed);
        loop {
            let next = f64::from_bits(cur) + n;
            match self.v.compare_exchange_weak(
                cur,
                next.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
    }
    fn get(&self) -> f64 {
        f64::from_bits(self.v.load(Ordering::Relaxed))
    }
}

/// Gauge (may go up and down).
#[derive(Debug)]
pub struct Gauge {
    v: AtomicU64,
}
impl Gauge {
    fn new() -> Self {
        Self {
            v: AtomicU64::new(0f64.to_bits()),
        }
    }
    /// Set to `v`.
    pub fn set(&self, v: f64) {
        self.v.store(v.to_bits(), Ordering::Relaxed);
    }
    /// Add `n`.
    pub fn add(&self, n: f64) {
        let mut cur = self.v.load(Ordering::Relaxed);
        loop {
            let next = f64::from_bits(cur) + n;
            match self.v.compare_exchange_weak(
                cur,
                next.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
    }
    fn get(&self) -> f64 {
        f64::from_bits(self.v.load(Ordering::Relaxed))
    }
}

/// Fixed-bucket histogram (cumulative counters + sum + count).
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    buckets: Vec<AtomicU64>, // count per bound (non-cumulative)
    count: AtomicU64,
    sum: AtomicU64, // f64 bits
}
impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: bounds.iter().map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0f64.to_bits()),
        }
    }
    /// Observe one sample.
    pub fn observe(&self, v: f64) {
        if !v.is_finite() {
            return;
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let mut cur = self.sum.load(Ordering::Relaxed);
        loop {
            let next = f64::from_bits(cur) + v;
            match self.sum.compare_exchange_weak(
                cur,
                next.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
        for (i, b) in self.bounds.iter().enumerate() {
            if v <= *b {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
        // +Inf bucket = count (implicit on encode).
    }
}

enum Series {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
}

struct MetricMeta {
    kind: &'static str, // "counter" | "gauge" | "histogram"
    help: &'static str,
    /// For histograms: bounds (needed when a series is created lazily).
    buckets: Option<&'static [f64]>,
}

/// The process-wide metric store. One `Registry` per process; share
/// via `Arc` or a `static`.
pub struct Registry {
    /// `# HELP`/`# TYPE` declarations by metric name.
    decls: RwLock<HashMap<&'static str, MetricMeta>>,
    /// Live series keyed by (metric, sorted labels).
    series: RwLock<HashMap<Key, Series>>,
    /// `<overflow_metric>` count — folded series creations.
    overflow: AtomicU64,
    /// Name of the overflow counter metric (e.g.
    /// `vrt_host_series_overflow_total`).
    overflow_metric: &'static str,
    /// T28/H2: const labels merged into EVERY emitted series at
    /// render time — process identity (e.g. `cell_id`) sourced from
    /// the process's own config, never from scrape-time relabels.
    const_labels: RwLock<Vec<(String, String)>>,
}

impl Registry {
    /// New registry; `overflow_metric` names the folded-series counter.
    pub fn new(overflow_metric: &'static str) -> Self {
        Self {
            decls: RwLock::new(HashMap::new()),
            series: RwLock::new(HashMap::new()),
            overflow: AtomicU64::new(0),
            overflow_metric,
            const_labels: RwLock::new(Vec::new()),
        }
    }

    /// T28/H2: attach a constant label to every series this process
    /// emits. Bounded: name must be a valid Prom label name, value ≤
    /// 256 chars without `\n`/`"`/`\\` — silently IGNORED otherwise
    /// (a malformed identity must not corrupt exposition).
    pub fn set_const_label(&self, name: &'static str, value: &str) {
        let name_ok = !name.is_empty()
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !name.chars().next().unwrap().is_ascii_digit();
        let value_ok = value.len() <= 256 && !value.chars().any(|c| matches!(c, '\n' | '"' | '\\'));
        if !(name_ok && value_ok) {
            return;
        }
        let mut cl = self.const_labels.write().unwrap_or_else(|e| e.into_inner());
        cl.retain(|(n, _)| n != name);
        cl.push((name.to_string(), value.to_string()));
        cl.sort();
    }

    /// Const labels merged into a series' label set (sorted union).
    fn merged_labels(&self, labels: &[(String, String)]) -> Vec<(String, String)> {
        let cl = self.const_labels.read().unwrap_or_else(|e| e.into_inner());
        if cl.is_empty() {
            return labels.to_vec();
        }
        let mut out = labels.to_vec();
        out.extend(cl.iter().cloned());
        out.sort();
        out
    }

    /// Declare a counter; subsequent `counter(...)` calls on the same
    /// name return the same series.
    pub fn declare_counter(&self, name: &'static str, help: &'static str) {
        self.declare(name, "counter", help, None);
    }

    /// Declare a gauge.
    pub fn declare_gauge(&self, name: &'static str, help: &'static str) {
        self.declare(name, "gauge", help, None);
    }

    /// Declare a histogram with fixed `buckets` (upper bounds,
    /// excluding +Inf which is implicit).
    pub fn declare_histogram(
        &self,
        name: &'static str,
        help: &'static str,
        buckets: &'static [f64],
    ) {
        self.declare(name, "histogram", help, Some(buckets));
    }

    fn declare(
        &self,
        name: &'static str,
        kind: &'static str,
        help: &'static str,
        buckets: Option<&'static [f64]>,
    ) {
        let mut d = self.decls.write().unwrap_or_else(|e| e.into_inner());
        d.entry(name).or_insert(MetricMeta {
            kind,
            help,
            buckets,
        });
    }

    fn key(&self, metric: &'static str, labels: LabelSet<'_>) -> Key {
        let mut ls: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ls.sort();
        Key { metric, labels: ls }
    }

    /// Fold `labels` when the metric's series cap is hit: collapse the
    /// identity label to `_overflow`.
    fn folded(key: Key) -> Key {
        let mut folded = false;
        let labels = key
            .labels
            .into_iter()
            .map(|(k, v)| {
                if !folded && (k == "worker_id" || k == "tenant_id") {
                    folded = true;
                    (k, "_overflow".to_string())
                } else {
                    (k, v)
                }
            })
            .collect();
        Key {
            metric: key.metric,
            labels,
        }
    }

    /// Fetch-or-create a series; enforces the per-name cap.
    fn series_or<F: FnOnce() -> Series>(&self, key: Key, make: F) -> SeriesRef<'_> {
        // Fast path: read lock.
        {
            let map = self.series.read().unwrap_or_else(|e| e.into_inner());
            if map.contains_key(&key) {
                drop(map);
                return SeriesRef::Read(self.series.read().unwrap_or_else(|e| e.into_inner()), key);
            }
        }
        let mut map = self.series.write().unwrap_or_else(|e| e.into_inner());
        if !map.contains_key(&key) {
            let count = map.keys().filter(|k| k.metric == key.metric).count();
            let key = if count >= MAX_SERIES_PER_METRIC {
                let folded = Self::folded(key);
                if !map.contains_key(&folded) {
                    map.insert(folded.clone(), make());
                    self.overflow.fetch_add(1, Ordering::Relaxed);
                }
                folded
            } else {
                map.insert(key.clone(), make());
                key
            };
            drop(map);
            let map = self.series.read().unwrap_or_else(|e| e.into_inner());
            return SeriesRef::Read(map, key);
        }
        drop(map);
        SeriesRef::Read(self.series.read().unwrap_or_else(|e| e.into_inner()), key)
    }

    /// Increment counter `name` by `n` with `labels`.
    pub fn inc(&self, name: &'static str, labels: LabelSet<'_>, n: f64) {
        let r = self.series_or(self.key(name, labels), || Series::Counter(Counter::new()));
        if let Some(Series::Counter(c)) = r.get(&key_ref(&r)) {
            c.inc(n)
        }
    }

    /// Set gauge `name` to `v`.
    pub fn set(&self, name: &'static str, labels: LabelSet<'_>, v: f64) {
        let r = self.series_or(self.key(name, labels), || Series::Gauge(Gauge::new()));
        if let Some(Series::Gauge(g)) = r.get(&key_ref(&r)) {
            g.set(v)
        }
    }

    /// Add `n` to gauge `name`.
    pub fn add(&self, name: &'static str, labels: LabelSet<'_>, n: f64) {
        let r = self.series_or(self.key(name, labels), || Series::Gauge(Gauge::new()));
        if let Some(Series::Gauge(g)) = r.get(&key_ref(&r)) {
            g.add(n)
        }
    }

    /// Observe `v` into histogram `name` (declared buckets apply).
    pub fn observe(&self, name: &'static str, labels: LabelSet<'_>, v: f64) {
        let bounds = {
            let d = self.decls.read().unwrap_or_else(|e| e.into_inner());
            d.get(name).and_then(|m| m.buckets).unwrap_or(&[])
        };
        let bounds_static: &'static [f64] = bounds;
        let r = self.series_or(self.key(name, labels), || {
            Series::Histogram(Histogram::new(bounds_static))
        });
        if let Some(Series::Histogram(h)) = r.get(&key_ref(&r)) {
            h.observe(v)
        }
    }

    /// Render the whole registry in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let decls = self.decls.read().unwrap_or_else(|e| e.into_inner());
        let map = self.series.read().unwrap_or_else(|e| e.into_inner());
        let mut names: Vec<&&'static str> = decls.keys().collect();
        names.sort();
        let mut out = String::with_capacity(4096);
        for name in names {
            let meta = &decls[*name];
            let _ = writeln!(out, "# HELP {} {}", name, meta.help);
            let _ = writeln!(out, "# TYPE {} {}", name, meta.kind);
            let mut keys: Vec<&Key> = map.keys().filter(|k| k.metric == *name).collect();
            keys.sort_by(|a, b| a.labels.cmp(&b.labels));
            for key in keys {
                let series = &map[key];
                let merged = self.merged_labels(&key.labels);
                let ls = render_labels(&merged);
                match (series, meta.kind) {
                    (Series::Counter(c), "counter") => {
                        let _ = writeln!(out, "{}{} {}", name, ls, fmt_f64(c.get()));
                    }
                    (Series::Gauge(g), "gauge") => {
                        let _ = writeln!(out, "{}{} {}", name, ls, fmt_f64(g.get()));
                    }
                    (Series::Histogram(h), "histogram") => {
                        let mut cum = 0u64;
                        for (i, b) in h.bounds.iter().enumerate() {
                            cum += h.buckets[i].load(Ordering::Relaxed);
                            let _ = writeln!(
                                out,
                                "{}_bucket{{{}le=\"{}\"}} {}",
                                name,
                                label_prefix(&merged),
                                fmt_f64(*b),
                                cum
                            );
                        }
                        let _ = writeln!(
                            out,
                            "{}_bucket{{{}le=\"+Inf\"}} {}",
                            name,
                            label_prefix(&merged),
                            h.count.load(Ordering::Relaxed)
                        );
                        let _ = writeln!(
                            out,
                            "{}_sum{} {}",
                            name,
                            ls,
                            fmt_f64(f64::from_bits(h.sum.load(Ordering::Relaxed)))
                        );
                        let _ = writeln!(
                            out,
                            "{}_count{} {}",
                            name,
                            ls,
                            h.count.load(Ordering::Relaxed)
                        );
                    }
                    _ => {}
                }
            }
        }
        let _ = writeln!(
            out,
            "# HELP {0} Series folded into an `_overflow` label value after the per-metric cardinality cap.\n# TYPE {0} counter\n{0}{2} {1}",
            self.overflow_metric,
            self.overflow.load(Ordering::Relaxed),
            render_labels(&self.merged_labels(&[])),
        );
        out
    }
}

/// Read guard wrapper exposing the series for a key.
enum SeriesRef<'a> {
    Read(RwLockReadGuard<'a, HashMap<Key, Series>>, Key),
}
impl<'a> SeriesRef<'a> {
    fn get(&'a self, key: &Key) -> Option<&'a Series> {
        match self {
            SeriesRef::Read(map, _) => map.get(key),
        }
    }
}
fn key_ref<'a>(r: &'a SeriesRef<'a>) -> &'a Key {
    match r {
        SeriesRef::Read(_, k) => k,
    }
}

/// Escape a label value per the exposition format (`\`, `"`, `\n`).
pub fn escape_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

fn render_labels(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let inner: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect();
    format!("{{{}}}", inner.join(","))
}

/// `le` label appended last inside braces for histogram buckets.
fn label_prefix(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(k, v)| format!("{}=\"{}\",", k, escape_label_value(v)))
        .collect()
}

/// Format a float like Prometheus does (integers without `.0`).
fn fmt_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

/// Standard request-duration buckets (B12, fixed).
pub const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_renders_sorted_labels_and_escapes() {
        let r = Registry::new("vrt_test_overflow_total");
        r.declare_counter("vrt_x_total", "x");
        r.inc(
            "vrt_x_total",
            &[("worker_id", "w1\"evil\nline"), ("status_class", "2xx")],
            3.0,
        );
        let out = r.render();
        assert!(out.contains("# TYPE vrt_x_total counter"));
        // Labels sort by name; values escape quote and newline.
        assert!(out.contains("vrt_x_total{status_class=\"2xx\",worker_id=\"w1\\\"evil\\nline\"} 3"));
    }

    #[test]
    fn histogram_renders_cumulative_buckets() {
        let r = Registry::new("vrt_test_overflow_total");
        r.declare_histogram("vrt_h_seconds", "h", &[0.1, 1.0]);
        for v in [0.05, 0.2, 0.9, 5.0] {
            r.observe("vrt_h_seconds", &[("worker_id", "w1")], v);
        }
        let out = r.render();
        assert!(out.contains("vrt_h_seconds_bucket{worker_id=\"w1\",le=\"0.1\"} 1"));
        assert!(out.contains("vrt_h_seconds_bucket{worker_id=\"w1\",le=\"1\"} 3"));
        assert!(out.contains("vrt_h_seconds_bucket{worker_id=\"w1\",le=\"+Inf\"} 4"));
        assert!(out.contains("vrt_h_seconds_count{worker_id=\"w1\"} 4"));
        assert!(out.contains("vrt_h_seconds_sum{worker_id=\"w1\"} 6.15"));
    }

    #[test]
    fn series_cap_folds_to_overflow_label() {
        let r = Registry::new("vrt_test_overflow_total");
        r.declare_counter("vrt_y_total", "y");
        for i in 0..MAX_SERIES_PER_METRIC {
            r.inc("vrt_y_total", &[("worker_id", &format!("w{i}"))], 1.0);
        }
        // Beyond the cap → folded under `_overflow`.
        r.inc("vrt_y_total", &[("worker_id", "w-extra")], 5.0);
        r.inc("vrt_y_total", &[("worker_id", "w-extra2")], 7.0);
        let out = r.render();
        assert!(out.contains("worker_id=\"_overflow\"} 12"), "{out}");
        assert!(out.contains("vrt_test_overflow_total 1"), "{out}");
    }

    #[test]
    fn concurrent_increments_are_exact() {
        let r = std::sync::Arc::new(Registry::new("vrt_test_overflow_total"));
        r.declare_counter("vrt_c_total", "c");
        let mut hs = Vec::new();
        for _ in 0..8 {
            let r = r.clone();
            hs.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    r.inc("vrt_c_total", &[("worker_id", "w1")], 1.0);
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let out = r.render();
        assert!(out.contains("vrt_c_total{worker_id=\"w1\"} 8000"), "{out}");
    }

    #[test]
    fn gauge_set_and_add() {
        let r = Registry::new("vrt_test_overflow_total");
        r.declare_gauge("vrt_g", "g");
        r.set("vrt_g", &[], 3.0);
        r.add("vrt_g", &[], -1.0);
        let out = r.render();
        assert!(out.contains("vrt_g 2"), "{out}");
    }

    #[test]
    fn const_labels_merge_into_every_series() {
        let r = Registry::new("ovf");
        r.declare_counter("reqs", "r");
        r.declare_gauge("up", "u");
        r.declare_histogram("lat", "l", &[0.5, 1.0]);
        r.inc("reqs", &[("worker_id", "w1")], 1.0);
        r.set("up", &[], 1.0);
        r.observe("lat", &[("worker_id", "w1")], 0.5);
        r.set_const_label("cell_id", "cell-a");
        let out = r.render();
        // Every emitted series carries the const label — including
        // histogram buckets/sum/count.
        for line in out.lines().filter(|l| !l.starts_with('#')) {
            assert!(line.contains("cell_id=\"cell-a\""), "{line}");
        }
        // Malformed identity is ignored, never corrupts output.
        r.set_const_label("bad name!", "x");
        r.set_const_label(
            "cell_id", "ok
nope",
        );
        assert!(!r.render().contains("bad name!"));
        assert!(r.render().contains("cell_id=\"cell-a\""));
    }
}
