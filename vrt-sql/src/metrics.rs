//! vrt-sql Prometheus metrics (T34). Labels use only fixed enums and
//! the bounded databaseId from [`crate::service::DbLabels`] -- never SQL/error text.

use std::sync::OnceLock;
use vrt_metrics::Registry;

fn reg() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| {
        let r = Registry::new("vrt_sql_series_overflow_total");
        r.declare_counter(
            "vrt_sql_queries_total",
            "D1 wire requests by endpoint/outcome.",
        );
        r.declare_histogram(
            "vrt_sql_query_duration_seconds",
            "D1 wire request duration.",
            &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
        );
        r.declare_counter("vrt_sql_rows_read_total", "Rows read, per databaseId.");
        r.declare_counter(
            "vrt_sql_rows_written_total",
            "Rows written, per databaseId.",
        );
        r.declare_gauge("vrt_sql_open_databases", "Open single-writer handles.");
        r.declare_counter(
            "vrt_sql_persistence_failures_total",
            "Object-store persistence failures by stage (T36).",
        );
        r.declare_counter(
            "vrt_sql_restore_corrupt_backoff_total",
            "Restores that failed closed on corrupt/mismatched snapshot or segment and entered bounded backoff (2026-11-15).",
        );
        r.declare_counter(
            "vrt_sql_degraded_reads_total",
            "Reads served while lease valid but renew failing (T36).",
        );
        r.declare_counter(
            "vrt_sql_lease_renew_failures_total",
            "Lease renewal CAS failures (T36).",
        );
        r.declare_gauge(
            "vrt_sql_fenced_dbs",
            "Databases currently fenced in this process (T36).",
        );
        r.declare_counter(
            "vrt_sql_tombstone_gc_rounds_total",
            "Bounded tombstone-GC passes run by the periodic sweeper (RV-R53.3).",
        );
        r.declare_counter(
            "vrt_sql_tombstone_gc_reclaimed_total",
            "Tombstoned databases whose objects were reclaimed (RV-R53.3).",
        );
        r.declare_counter(
            "vrt_sql_tombstone_gc_objects_deleted_total",
            "Objects deleted by tombstone GC (RV-R53.3).",
        );
        r.declare_counter(
            "vrt_sql_tombstone_gc_failures_total",
            "Per-database tombstone-GC failures (isolated, retried next round).",
        );
        r
    })
}

pub fn registry() -> &'static Registry {
    reg()
}

/// `endpoint` ∈ `query|execute`; `outcome` ∈
/// `ok|sql_error|protocol_error|unauthorized|rate_limited|unavailable`.
pub fn query(endpoint: &'static str, outcome: &'static str, seconds: f64) {
    let r = reg();
    r.inc(
        "vrt_sql_queries_total",
        &[("endpoint", endpoint), ("outcome", outcome)],
        1.0,
    );
    r.observe(
        "vrt_sql_query_duration_seconds",
        &[("endpoint", endpoint)],
        seconds,
    );
}

/// `db` must be a product of DbLabels::label (bounded).
pub fn rows(db: &str, read: u64, written: u64) {
    let r = reg();
    if read > 0 {
        r.inc(
            "vrt_sql_rows_read_total",
            &[("database_id", db)],
            read as f64,
        );
    }
    if written > 0 {
        r.inc(
            "vrt_sql_rows_written_total",
            &[("database_id", db)],
            written as f64,
        );
    }
}

pub fn set_open_databases(n: usize) {
    reg().set("vrt_sql_open_databases", &[], n as f64);
}

/// `reason` ∈ `publish|checkpoint|claim|restore|tombstone|gc`.
pub fn persistence_failure(reason: &'static str) {
    reg().inc(
        "vrt_sql_persistence_failures_total",
        &[("reason", reason)],
        1.0,
    );
}

/// Restore failed closed on corruption/sha mismatch and entered backoff.
pub fn restore_corrupt_backoff() {
    reg().inc("vrt_sql_restore_corrupt_backoff_total", &[], 1.0);
}

/// degraded read (lease valid but renewal failed).
pub fn degraded_read() {
    reg().inc("vrt_sql_degraded_reads_total", &[], 1.0);
}

/// `stage` ∈ `store|expired` (the two contexts of a renewal failure).
/// RV-R53.3: one bounded tombstone-GC round's counters.
pub fn tombstone_gc_round(reclaimed: usize, objects_freed: u64, failures: usize) {
    let r = reg();
    r.inc("vrt_sql_tombstone_gc_rounds_total", &[], 1.0);
    if reclaimed > 0 {
        r.inc(
            "vrt_sql_tombstone_gc_reclaimed_total",
            &[],
            reclaimed as f64,
        );
    }
    if objects_freed > 0 {
        r.inc(
            "vrt_sql_tombstone_gc_objects_deleted_total",
            &[],
            objects_freed as f64,
        );
    }
    if failures > 0 {
        r.inc("vrt_sql_tombstone_gc_failures_total", &[], failures as f64);
    }
}

pub fn lease_renew_failure(stage: &'static str) {
    reg().inc(
        "vrt_sql_lease_renew_failures_total",
        &[("stage", stage)],
        1.0,
    );
}

/// Fenced-db counter inc/dec (fence +1; the caller adds -1 on recovery/removal).
pub fn set_fenced_dbs_delta(d: i64) {
    static N: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let v = N.fetch_add(d, std::sync::atomic::Ordering::Relaxed) + d;
    reg().set("vrt_sql_fenced_dbs", &[], v.max(0) as f64);
}

/// `GET /metrics` handler.
pub async fn metrics_handler() -> axum::response::Response {
    axum::response::IntoResponse::into_response((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        reg().render(),
    ))
}

/// metrics listener router (internal port, not public).
pub fn router() -> axum::Router {
    axum::Router::new().route("/metrics", axum::routing::get(metrics_handler))
}
