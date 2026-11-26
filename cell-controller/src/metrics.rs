//! B12: cell-controller metrics — lease, revision/apply lag, ACK
//! outcomes, snapshot verification failures, cron dispatch, poll
//! errors. All labels are fixed enums; revisions are counters/gauges
//! with no label cardinality.

use axum::response::{IntoResponse, Response};
use axum::routing::get;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use vrt_metrics::Registry;

fn reg() -> &'static Registry {
    static REG: OnceLock<Registry> = OnceLock::new();
    REG.get_or_init(|| {
        let r = Registry::new("vrt_cell_series_overflow_total");
        r.declare_gauge("vrt_cell_desired_revision", "Latest verified desired revision seen.");
        r.declare_gauge("vrt_cell_applied_revision", "Latest revision applied to pods.");
        r.declare_gauge("vrt_cell_apply_lag_seconds", "Seconds from first seeing a desired revision to its apply completing.");
        r.declare_counter("vrt_cell_ack_total", "ACK reports by outcome (applied|partial|rejected|failed).");
        r.declare_counter("vrt_cell_snapshot_verify_failures_total", "Envelope verification rejections by reason.");
        r.declare_gauge("vrt_cell_lease_held", "Cron lease currently held (1/0).");
        r.declare_gauge("vrt_cell_lease_epoch", "Cron lease fencing epoch.");
        r.declare_counter("vrt_cell_cron_dispatch_total", "Cron dispatch outcomes (delivered|failed|retried_switching|skipped_lease_lost|skipped_no_lease|blackout|missed_deadline).");
        r.declare_counter("vrt_cell_logs_total", "Worker log shipping events by outcome (shipped records|drain_failed|push_failed).");
        r.declare_counter("vrt_cell_meter_push_total", "Usage meter pushes by outcome (pushed|scrape_failed|push_failed).");
        r.declare_counter("vrt_cell_meter_entries_total", "Worker usage entries accepted by the control plane.");
        r.declare_counter("vrt_cell_poll_errors_total", "Pull/fetch errors by source (desired|render_inputs).");
        r.declare_counter("vrt_cell_d1_promote_total", "D1 promotion outcomes (ok|failed).");
        r.declare_counter("vrt_cell_d1_auto_total", "D1 automatic-failover outcomes (probe_failed|promoted|promote_failed|cooling).");
        r.declare_counter("vrt_cell_do_alarm_due_total", "Durable Object alarms due during a wake tick.");
        r.declare_counter("vrt_cell_do_alarm_wake_total", "Durable Object alarm wake requests by outcome (ok|failed|unassigned).");
        r.declare_counter("vrt_cell_do_alarm_gate_total", "DO sleep gates published by verdict (safe|unsafe).");
        r
    })
}

pub fn registry() -> &'static Registry {
    reg()
}

static DESIRED_SEEN: Mutex<Option<(u64, Instant)>> = Mutex::new(None);

/// Record a verified desired revision (starts the apply-lag clock).
pub fn desired_revision(revision: u64) {
    reg().set("vrt_cell_desired_revision", &[], revision as f64);
    let mut seen = DESIRED_SEEN.lock().unwrap_or_else(|e| e.into_inner());
    match *seen {
        Some((r, _)) if r >= revision => {}
        _ => *seen = Some((revision, Instant::now())),
    }
}

/// Record an applied revision; publishes lag when it reaches the
/// newest desired revision.
pub fn applied_revision(revision: u64) {
    let r = reg();
    r.set("vrt_cell_applied_revision", &[], revision as f64);
    let mut seen = DESIRED_SEEN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((desired, ts)) = *seen {
        if revision >= desired {
            r.set(
                "vrt_cell_apply_lag_seconds",
                &[],
                ts.elapsed().as_secs_f64(),
            );
            *seen = None;
        }
    }
}

/// ACK funnel — call from `report()` with the ack status.
pub fn ack(status: &'static str) {
    reg().inc("vrt_cell_ack_total", &[("outcome", status)], 1.0);
}

/// T28/H2: export the cell identity on every series.
pub fn set_cell_label(cell_id: &str) {
    reg().set_const_label("cell_id", cell_id);
}

/// Envelope verification rejection (`reason` = `RejectReason::code()`).
pub fn verify_failure(reason: &'static str) {
    reg().inc(
        "vrt_cell_snapshot_verify_failures_total",
        &[("reason", reason)],
        1.0,
    );
}

/// Lease state from the cron runner each tick.
pub fn lease(held: bool, epoch: u64) {
    let r = reg();
    r.set("vrt_cell_lease_held", &[], if held { 1.0 } else { 0.0 });
    if held {
        r.set("vrt_cell_lease_epoch", &[], epoch as f64);
    }
}

/// Cron tick outcome aggregate.
pub fn cron_tick(
    fired: usize,
    failed: usize,
    skipped_lease_lost: usize,
    not_leader: bool,
    blackout: bool,
    skipped_deadline: usize,
    retried_switching: usize,
) {
    let r = reg();
    for (outcome, n) in [
        ("delivered", fired),
        ("failed", failed),
        ("skipped_lease_lost", skipped_lease_lost),
        ("skipped_no_lease", usize::from(not_leader)),
        ("skipped_blackout", usize::from(blackout)),
        ("missed_deadline", skipped_deadline),
        // MIG-11: confirmed-undelivered switching retries (attempts).
        ("retried_switching", retried_switching),
    ] {
        if n > 0 {
            r.inc(
                "vrt_cell_cron_dispatch_total",
                &[("outcome", outcome)],
                n as f64,
            );
        }
    }
}

/// D1 manual PROMOTE outcome.
pub fn d1_promote(outcome: &'static str) {
    reg().inc("vrt_cell_d1_promote_total", &[("outcome", outcome)], 1.0);
}

/// D1 automatic-failover outcome.
pub fn d1_auto(outcome: &'static str) {
    reg().inc("vrt_cell_d1_auto_total", &[("outcome", outcome)], 1.0);
}

/// One alarm-wake tick (counts are already aggregated).
pub fn alarm_wake(due: usize, ok: usize, failed: usize, unassigned: usize) {
    let r = reg();
    if due > 0 {
        r.inc("vrt_cell_do_alarm_due_total", &[], due as f64);
    }
    for (outcome, n) in [("ok", ok), ("failed", failed), ("unassigned", unassigned)] {
        if n > 0 {
            r.inc(
                "vrt_cell_do_alarm_wake_total",
                &[("outcome", outcome)],
                n as f64,
            );
        }
    }
}

/// One tenant DO sleep gate.
pub fn alarm_gate(safe: bool) {
    reg().inc(
        "vrt_cell_do_alarm_gate_total",
        &[("verdict", if safe { "safe" } else { "unsafe" })],
        1.0,
    );
}

/// Pull/fetch failure (`source` ∈ `desired|render_inputs`).
pub fn poll_error(source: &'static str) {
    reg().inc("vrt_cell_poll_errors_total", &[("reason", source)], 1.0);
}

/// T37b: queue dispatch — per-message settle outcome
/// (`ack|retry|dlq|dropped`).
pub fn queue_delivered(outcome: &'static str, n: usize) {
    if n > 0 {
        reg().inc(
            "vrt_queue_delivered_total",
            &[("outcome", outcome)],
            n as f64,
        );
    }
}

/// T37b: settle blocked (lease lost / backend / dlq send failure).
pub fn queue_settle_blocked(reason: &'static str, n: usize) {
    if n > 0 {
        reg().inc(
            "vrt_queue_settle_blocked_total",
            &[("reason", reason)],
            n as f64,
        );
    }
}

/// T37b-r5: platform-side release — batch returned without consuming
/// attempts (consumer never ran). `vrt_queue_released_total`.
pub fn queue_released(n: usize) {
    if n > 0 {
        reg().inc("vrt_queue_released_total", &[], n as f64);
    }
}

/// T37b: delivery latency (claim→settle decision) histogram.
pub fn queue_latency(seconds: f64) {
    reg().observe("vrt_queue_delivery_latency_seconds", &[], seconds);
}

/// T37b: consumer invocation outcome/error (`ok|timeout|transport|…`).
pub fn queue_consumer_error(reason: &str) {
    reg().inc(
        "vrt_queue_consumer_errors_total",
        &[("reason", reason)],
        1.0,
    );
}

/// T37b: dispatch lease state — held=1/lost=0 (+epoch when held).
pub fn queue_lease(held: bool, epoch: u64) {
    let r = reg();
    r.set(
        "vrt_queue_dispatcher_lease",
        &[("state", "held")],
        if held { 1.0 } else { 0.0 },
    );
    if held {
        r.set("vrt_queue_dispatcher_epoch", &[], epoch as f64);
    }
}

/// `GET /metrics` for the internal metrics listener.
pub async fn metrics_handler() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        reg().render(),
    )
        .into_response()
}

pub fn router() -> axum::Router {
    axum::Router::new().route("/metrics", get(metrics_handler))
}

/// Worker log shipper outcome (`shipped|drain_failed|push_failed`).
pub fn logs_event(outcome: &'static str, n: usize) {
    if n > 0 {
        reg().inc("vrt_cell_logs_total", &[("outcome", outcome)], n as f64);
    }
}

pub fn meter_event(outcome: &'static str, entries: usize) {
    reg().inc("vrt_cell_meter_push_total", &[("outcome", outcome)], 1.0);
    if entries > 0 {
        reg().inc("vrt_cell_meter_entries_total", &[], entries as f64);
    }
}
