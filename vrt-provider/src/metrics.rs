//! B12: provider-side metric accounting for binding subrequests and
//! assets fetches. Series live in the shared [`vrt_metrics::Registry`];
//! `worker_id` comes from the `x-open-compute-worker-id` header — the
//! same header the [`crate::authorize::Authorizer`] verifies against
//! the signed `compute.desired` snapshot before the binding executes.
//! Malformed/absent headers fold to `_unknown`; cardinality is capped
//! per metric and folds into `worker_id="_overflow"`. Fixed enum
//! labels only — never paths, methods or tenant data.

use axum::response::{IntoResponse, Response};
use axum::routing::get;
use std::sync::OnceLock;
use vrt_metrics::Registry;

pub(crate) fn reg() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| {
        let r = Registry::new("vrt_provider_series_overflow_total");
        r.declare_counter(
            "vrt_worker_subrequests_total",
            "Binding subrequests executed by the provider.",
        );
        r.declare_counter(
            "vrt_worker_subrequests_errors_total",
            "Binding subrequests that errored.",
        );
        r.declare_counter(
            "vrt_worker_assets_requests_total",
            "Assets fetches by outcome.",
        );
        r.declare_counter("vrt_worker_d1_requests_total", "D1 dispatches by outcome.");
        // RV-R28 #2: response bodies cut before completion —
        // `{tenant, reason}` with reason ∈ budget_exceeded |
        // body_idle_timeout | body_deadline (bounded static set).
        r.declare_counter(
            "vrt_provider_response_body_aborted_total",
            "Dispatch response bodies aborted before completion by cause.",
        );
        // W5: per-tenant multipart accounting — `{tenant, op, outcome}`
        // with op ∈ create|part|complete|abort and outcome ∈ ok|error
        // (bounded static sets; raw upload ids never label).
        r.declare_counter(
            "vrt_provider_r2_multipart_ops_total",
            "R2 multipart operations by tenant/op/outcome.",
        );
        r.declare_gauge(
            "vrt_provider_r2_staging_bytes",
            "Filesystem multipart staging bytes by tenant/bucket.",
        );
        r.declare_counter(
            "vrt_provider_quota_exceeded_total",
            "Requests refused by the per-tenant QPS/bytes quota by tenant/reason.",
        );
        r.declare_counter(
            "vrt_provider_r2_staging_gc_uploads_reclaimed_total",
            "Expired multipart staging dirs reclaimed by the fs GC sweep.",
        );
        r.declare_counter(
            "vrt_provider_r2_staging_gc_bytes_freed_total",
            "Staging bytes freed by the fs GC sweep.",
        );
        r.declare_counter("process_cpu_seconds_total", "Self process CPU seconds.");
        // MIG-15: pg-wire proxy accounting. Fixed labels only —
        // `reason` is the typed Deny enum, `binding`/`dir` bounded.
        r.declare_counter(
            "vrt_provider_pgproxy_sessions_total",
            "PG proxy sessions admitted.",
        );
        r.declare_gauge(
            "vrt_provider_pgproxy_sessions_active",
            "PG proxy sessions currently relaying.",
        );
        r.declare_counter(
            "vrt_provider_pgproxy_denied_total",
            "PG proxy session denials by typed reason.",
        );
        r.declare_counter(
            "vrt_provider_pgproxy_bytes_total",
            "PG proxy relayed bytes by direction.",
        );
        r.declare_histogram(
            "vrt_provider_pgproxy_upstream_connect_ms",
            "PG proxy upstream TCP connect latency.",
            &[50.0, 200.0, 1_000.0, 5_000.0],
        );
        r.declare_gauge("process_resident_memory_bytes", "Self process RSS.");
        r.declare_gauge("process_start_time_seconds", "Self process start epoch.");
        r.declare_gauge("process_open_fds", "Self process open fds.");
        r.declare_gauge(
            "vrt_metrics_unsupported",
            "Metrics that cannot be sampled on this platform (1 = unsupported).",
        );
        r
    })
}

pub fn registry() -> &'static Registry {
    reg()
}

/// Bounded `worker_id` label: the `x-open-compute-worker-id` header
/// validated by [`crate::identity::is_valid_identifier`] — the same
/// rule authorization applies — else `_unknown` (never raw request
/// data in a label).
pub(crate) fn worker_label(request: &axum::http::Request<axum::body::Body>) -> String {
    request
        .headers()
        .get(crate::limits::WORKER_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| crate::identity::is_valid_identifier(v))
        .unwrap_or("_unknown")
        .to_string()
}

/// `binding_type` ∈ `kv|r2|assets|pg|egress|unknown`; `is_error` =
/// response rejected at the binding boundary (denial/5xx).
pub(crate) fn subrequest(worker_id: &str, binding_type: &'static str, is_error: bool) {
    let r = reg();
    r.inc(
        "vrt_worker_subrequests_total",
        &[("worker_id", worker_id), ("binding_type", binding_type)],
        1.0,
    );
    if is_error {
        r.inc(
            "vrt_worker_subrequests_errors_total",
            &[("worker_id", worker_id), ("binding_type", binding_type)],
            1.0,
        );
    }
}

/// RV-R28 #2: a response body aborted before completion.
/// `reason` ∈ `budget_exceeded|body_idle_timeout|body_deadline`
/// (bounded static set; tenant is the listener tenant id).
pub(crate) fn body_aborted(tenant: &str, reason: &'static str) {
    reg().inc(
        "vrt_provider_response_body_aborted_total",
        &[("tenant", tenant), ("reason", reason)],
        1.0,
    );
}

/// RV-R67: requests refused by the per-tenant quota; `reason` ∈
/// `qps|bytes` (bounded static set, [`crate::quota::QuotaExceeded::as_str`]).
pub(crate) fn quota_exceeded(tenant: &str, reason: &'static str) {
    reg().inc(
        "vrt_provider_quota_exceeded_total",
        &[("tenant", tenant), ("reason", reason)],
        1.0,
    );
}

/// `outcome` ∈ `hit|not_modified|not_found|spa_fallback|redirect|error`.
/// T34: vrt-sql downstream failure counter (the outcome is embedded in the metric name, avoiding a new label dimension).
pub(crate) fn d1_backend_unavailable(worker_id: &str) {
    reg().inc(
        "vrt_worker_subrequests_errors_total",
        &[("worker_id", worker_id), ("binding_type", "d1_backend")],
        1.0,
    );
}

/// `outcome` ∈ `ok|bad_path|oversize|denied_identity|denied_authz|
/// denied_permission|backend_unconfigured|body_error|precheck|
/// backend_error` — bounded static set (T34d).
pub(crate) fn d1_outcome(worker_id: &str, outcome: &'static str) {
    reg().inc(
        "vrt_worker_d1_requests_total",
        &[("worker_id", worker_id), ("outcome", outcome)],
        1.0,
    );
}

/// T37 queue metrics (section 11 P3b four-piece set):
/// `vrt_queue_messages_sent_total{tenant,worker_id}`,
/// `vrt_queue_send_rejected_total{tenant,worker_id,reason}` (reason is a bounded enum),
/// `vrt_queue_backlog_messages{tenant,queue}`,
/// `vrt_queue_oldest_age_seconds{tenant,queue}` (approximate: the provider's sampling instant).
/// The queue label is truncated to prevent high cardinality (ULID is bounded at 26 chars).
pub(crate) fn queue_sent(tenant: &str, worker_id: &str, n: u64) {
    reg().inc(
        "vrt_queue_messages_sent_total",
        &[("tenant", tenant), ("worker_id", worker_id)],
        n as f64,
    );
}

/// `reason` ∈ `bad_path|oversize|invalid|denied_identity|denied_authz|
/// denied_permission|backend_unconfigured|body_error|dropped|fenced|
/// backend_unavailable|backend_error|duplicate_id` (a bounded static set).
pub(crate) fn queue_rejected(tenant: &str, worker_id: &str, reason: &'static str) {
    reg().inc(
        "vrt_queue_send_rejected_total",
        &[
            ("tenant", tenant),
            ("worker_id", worker_id),
            ("reason", reason),
        ],
        1.0,
    );
}

/// T37r: count of idempotent dedups reported back by vrt-sql -- `reason="duplicate_id"`,
/// 0 on the normal path (the provider already rejects client ids; only DLQ forwarding/transport retries hit it).
pub(crate) fn queue_duplicate_ids(tenant: &str, worker_id: &str, n: u64) {
    if n == 0 {
        return;
    }
    reg().inc(
        "vrt_queue_send_rejected_total",
        &[
            ("tenant", tenant),
            ("worker_id", worker_id),
            ("reason", "duplicate_id"),
        ],
        n as f64,
    );
}

/// After each successful op, the metrics response refreshes the backlog level and the oldest-message age.
/// The `queue` label = the authorized binding's queue id (ULID, bounded cardinality).
/// When `oldest_message_timestamp_ms=None` (empty queue) the age is set to 0.
pub(crate) fn queue_backlog(tenant: &str, queue: &str, m: &crate::queue::WireMetrics) {
    let labels = [("tenant", tenant), ("queue", queue)];
    reg().set(
        "vrt_queue_backlog_messages",
        &labels,
        m.backlog_count as f64,
    );
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let age = m
        .oldest_message_timestamp_ms
        .map(|t| ((now_ms - t).max(0)) as f64 / 1000.0)
        .unwrap_or(0.0);
    reg().set("vrt_queue_oldest_age_seconds", &labels, age);
}

pub(crate) fn assets_outcome(worker_id: &str, outcome: &'static str) {
    reg().inc(
        "vrt_worker_assets_requests_total",
        &[("worker_id", worker_id), ("outcome", outcome)],
        1.0,
    );
}

fn publish_self(r: &Registry) {
    const NAMES: [&str; 4] = [
        "process_cpu_seconds_total",
        "process_resident_memory_bytes",
        "process_start_time_seconds",
        "process_open_fds",
    ];
    match vrt_metrics::sys::self_usage() {
        Some(u) => {
            r.set("process_cpu_seconds_total", &[], u.cpu_seconds);
            r.set("process_resident_memory_bytes", &[], u.rss_bytes as f64);
            r.set("process_start_time_seconds", &[], u.start_time_seconds);
            r.set("process_open_fds", &[], u.open_fds as f64);
            for n in NAMES {
                r.set("vrt_metrics_unsupported", &[("metric", n)], 0.0);
            }
        }
        None => {
            for n in NAMES {
                r.set("vrt_metrics_unsupported", &[("metric", n)], 1.0);
            }
        }
    }
}

/// `GET /metrics` for the provider's internal metrics listener.
pub async fn metrics_handler() -> Response {
    publish_self(reg());
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        reg().render(),
    )
        .into_response()
}

/// Router fragment mounted on the provider metrics listener.
/// Liveness/readiness probe — static 200 on the internal metrics
/// listener only (never on the tenant data plane).
async fn health_probe() -> &'static str {
    "ok"
}

pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_probe))
        .route("/readyz", get(health_probe))
}

/// Label pairs present in `previous` but absent from `current` —
/// these stale gauge series must be explicitly zeroed each sweep (a
/// set-only gauge leaks the last value forever once a tenant/bucket
/// drains or disappears). Extracted pure so the zeroing discipline is
/// unit-testable.
pub fn vanished_series(
    previous: &std::collections::HashSet<(String, String)>,
    current: &std::collections::HashSet<(String, String)>,
) -> Vec<(String, String)> {
    previous.difference(current).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::vanished_series;
    use std::collections::HashSet;

    #[test]
    fn vanished_series_zeroes_only_disappeared_pairs() {
        let prev: HashSet<(String, String)> = [
            ("t1".to_string(), "b1".to_string()),
            ("t1".to_string(), "b2".to_string()),
            ("t2".to_string(), "b1".to_string()),
        ]
        .into_iter()
        .collect();
        let current: HashSet<(String, String)> =
            [("t1".to_string(), "b2".to_string())].into_iter().collect();
        let gone = vanished_series(&prev, &current);
        assert_eq!(gone.len(), 2);
        assert!(gone.contains(&("t1".to_string(), "b1".to_string())));
        assert!(gone.contains(&("t2".to_string(), "b1".to_string())));
        // Empty previous round → nothing to zero.
        assert!(vanished_series(&HashSet::new(), &current).is_empty());
        // Identical rounds → nothing to zero.
        assert!(vanished_series(&prev, &prev).is_empty());
    }
}
