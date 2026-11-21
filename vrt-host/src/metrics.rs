//! B12: per-worker + per-instance Prometheus metrics, counted on the
//! platform-trusted path (ingress proxy, supervisor, asset sync,
//! cron trigger) — never from worker-reported data.
//!
//! Label discipline (enforced by construction):
//! - `worker_id`/`tenant_id`: platform ids from the signed desired —
//!   never worker names, Host headers or request data. Series are
//!   capped per metric by [`vrt_metrics::MAX_SERIES_PER_METRIC`] and
//!   fold into `worker_id="_overflow"` beyond the cap.
//! - `status_class`/`outcome`/`reason`/`binding_type`: fixed enums.
//! - No path, hostname, method or body labels anywhere.

use std::sync::Arc;
use std::time::Instant;

use vrt_metrics::{Registry, DURATION_BUCKETS};

use crate::state::TenantState;
use crate::supervisor::TenantRegistry;

/// Process-wide registry (`vrt_host_*` prefix).
static REGISTRY: std::sync::OnceLock<Arc<Registry>> = std::sync::OnceLock::new();

/// Shared registry for the whole host process.
pub fn registry() -> Arc<Registry> {
    REGISTRY
        .get_or_init(|| {
            let r = Registry::new("vrt_host_series_overflow_total");
            declare_all(&r);
            Arc::new(r)
        })
        .clone()
}

fn declare_all(r: &Registry) {
    // ---- worker logs (no worker-id labels: bounded cardinality) ----
    r.declare_counter(
        "vrt_host_logs_ingested_total",
        "Log records accepted into the host log ring buffers.",
    );
    r.declare_counter(
        "vrt_host_logs_dropped_total",
        "Log records dropped by reason (evicted, rate_limited, coalesced, batch_overflow).",
    );
    r.declare_counter(
        "vrt_host_logs_truncated_total",
        "Log records truncated to the per-message cap.",
    );
    r.declare_counter(
        "vrt_host_logs_ingest_rejected_total",
        "Log ingest calls rejected by reason (auth, unknown, bad_body).",
    );
    r.declare_gauge(
        "vrt_host_logs_buffer_bytes",
        "Bytes currently buffered in the host log ring buffers.",
    );
    // ---- per-worker ----
    r.declare_counter(
        "vrt_worker_requests_total",
        "Requests proxied to the worker.",
    );
    // T31c: eyeball requests the in-workerd router answered from static
    // assets — counted as ASSET requests, never worker invocations.
    // The marker header is written only by the facade-router system
    // worker (it strips the header from user-worker responses), so a
    // user worker cannot reclassify its own invocations.
    r.declare_counter(
        "vrt_worker_assets_route_requests_total",
        "Requests answered by the assets-first router from static assets.",
    );
    r.declare_histogram(
        "vrt_worker_request_duration_seconds",
        "Worker request wall time (accept → response headers) as observed by the host.",
        DURATION_BUCKETS,
    );
    r.declare_counter(
        "vrt_worker_response_bytes_total",
        "Response body bytes streamed to the client.",
    );
    r.declare_counter(
        "vrt_worker_request_bytes_total",
        "Request body bytes streamed upstream.",
    );
    r.declare_gauge(
        "vrt_worker_inflight_requests",
        "In-flight proxied requests.",
    );
    // ---- WebSocket passthrough (ingress) ----
    r.declare_counter(
        "vrt_host_ws_sessions_total",
        "WebSocket upgrade attempts by outcome (upgraded, rejected_capacity, rejected_upstream, client_upgrade_failed, upstream_upgrade_failed).",
    );
    r.declare_counter(
        "vrt_host_ws_session_end_total",
        "Ended WebSocket sessions by reason (closed, idle_timeout, max_duration, error).",
    );
    r.declare_gauge(
        "vrt_host_ws_active_sessions",
        "WebSocket sessions currently tunnelled through the ingress.",
    );
    r.declare_counter(
        "vrt_host_ws_bytes_total",
        "WebSocket tunnel bytes by direction (client_to_worker, worker_to_client).",
    );
    r.declare_counter(
        "vrt_worker_exceptions_total",
        "Worker-surfaced errors by reason.",
    );
    r.declare_counter(
        "vrt_worker_cron_invocations_total",
        "Cron trigger deliveries by outcome.",
    );
    // ---- per-tenant (one process may serve several workers) ----
    r.declare_gauge(
        "vrt_tenant_cpu_seconds_total",
        "Tenant process CPU seconds (utime+stime, cumulative per pid; resets across generations).",
    );
    r.declare_gauge(
        "vrt_tenant_memory_rss_bytes",
        "Tenant process resident memory.",
    );
    r.declare_gauge(
        "vrt_metrics_unsupported",
        "Metrics that cannot be sampled on this platform (1 = unsupported).",
    );
    r.declare_counter(
        "vrt_host_runaway_kills_total",
        "Tenant process groups SIGKILLed by the runaway watchdog.",
    );
    r.declare_counter(
        "vrt_host_runaway_heartbeat_failures_total",
        "Heartbeat probe failures observed by the runaway watchdog.",
    );
    r.declare_counter(
        "vrt_host_waituntil_dropped_total",
        "MIG-12: generations signalled with pending or unknown ctx.waitUntil work, by reason (grace_expired, do_switch_grace_expired, probe_failed, drain_deadline).",
    );
    r.declare_counter(
        "vrt_host_runaway_cpu_unavailable_total",
        "Monitor ticks where cpu_seconds was unreadable (signal 1 unprovable, never zero).",
    );
    r.declare_counter(
        "vrt_host_runaway_probes_total",
        "Heartbeat probes spawned by the runaway watchdog (CPU-gated; a cold tenant costs zero).",
    );
    r.declare_counter(
        "vrt_host_runaway_kill_stuck_total",
        "Runaway kills that failed to converge within the stuck deadline (re-sends counted).",
    );
    r.declare_gauge(
        "vrt_tenant_runaway_last_verdict",
        "Last runaway verdict code: 0 healthy, 1 cpu-only, 2 heartbeat-only, 3 cpu-unavailable, 4 runaway.",
    );
    // ---- instance ----
    r.declare_gauge(
        "vrt_host_tenant_processes",
        "Tenant processes currently running.",
    );
    r.declare_counter(
        "vrt_host_launches_total",
        "Generation launches by outcome+reason.",
    );
    r.declare_counter(
        "vrt_host_generation_switches_total",
        "Generation switches by outcome.",
    );
    // MIG-8: pre-drain `workerd test` config preflight — outcome-coded
    // (ok/rejected/timeout/admission-timeout) so a stuck or wedged
    // check is alertable before it ever touches a drain.
    r.declare_counter(
        "vrt_host_preflight_checks_total",
        "Config preflight checks by outcome (ok|rejected|timeout|admission-timeout).",
    );
    r.declare_gauge(
        "vrt_host_preflight_checks_in_flight",
        "Preflight checks running or queued on the global concurrency bound.",
    );
    r.declare_histogram(
        "vrt_host_preflight_check_seconds",
        "Preflight check wall time (admission queue + subprocess).",
        DURATION_BUCKETS,
    );
    // W4-r2: serialized-switch request hold (tenant-scoped — the
    // gate lives on TenantRuntime, not a worker).
    r.declare_counter(
        "vrt_host_switch_held_requests_total",
        "Requests held during a serialized DO switch window.",
    );
    r.declare_counter(
        "vrt_host_switch_hold_rejects_total",
        "Requests refused during a switch hold (capacity|timeout).",
    );
    r.declare_gauge(
        "vrt_host_switch_held_inflight",
        "Requests currently held for a serialized switch.",
    );
    r.declare_gauge("vrt_host_ready", "Tenant ready to serve (1/0).");
    // MIG-10: CA trust-store probe — 0 means every tenant HTTPS egress
    // will fail TLS verification (image missing ca-certificates).
    r.declare_gauge(
        "vrt_host_egress_trust_store_ok",
        "CA trust store workerd egress TLS uses exists and is non-empty (1/0).",
    );
    r.declare_counter(
        "vrt_host_asset_sync_total",
        "Asset materialization by outcome.",
    );
    r.declare_counter(
        "vrt_host_render_inputs_unavailable",
        "Assembles failed on render-inputs load.",
    );
    r.declare_gauge("vrt_host_build_info", "Build info.");
    r.declare_counter(
        "process_cpu_seconds_total",
        "Self process CPU seconds (standalone host basics).",
    );
    r.declare_gauge("process_resident_memory_bytes", "Self process RSS.");
    r.declare_gauge("process_start_time_seconds", "Self process start epoch.");
    r.declare_gauge("process_open_fds", "Self process open fds.");
    // WAL1: DO WAL continuous replication (feature-flagged).
    r.declare_counter(
        "vrt_host_do_wal_segments_uploaded_total",
        "WAL segments uploaded per cell (WAL1).",
    );
    r.declare_counter(
        "vrt_host_do_wal_upload_failures_total",
        "WAL segment upload failures after per-attempt retries.",
    );
    r.declare_counter(
        "vrt_host_do_wal_broken_total",
        "WAL streams broken (discontinuity/pending cap) by reason.",
    );
    r.declare_gauge(
        "vrt_host_do_wal_lag_bytes",
        "Un-uploaded WAL bytes buffered per class.",
    );
    r.declare_gauge(
        "vrt_host_do_wal_lag_seconds",
        "WAL upload lag — oldest un-uploaded commit age.",
    );
    r.declare_gauge(
        "vrt_host_do_wal_pinned_bytes",
        "WAL bytes pinned below the tailer's read mark.",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_total",
        "WAL replay passes at restore by outcome.",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_marker_failures_total",
        "Restore+replay pending-marker write failures — the restore was       refused before touching the dir (fail closed, retryable).",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_marker_stale_total",
        "Replay-pending markers cleared as stale — matched a live dir or       a superseded baseline/writer epoch; never replayed.",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_segs_corrupt_total",
        "WAL replay segments that broke the chain INTERIORLY (later        segments provably exist) — repairable corruption; the stem is        rejected and restore fails closed.",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_cap_exceeded_total",
        "WAL replay stems rejected because the chain crossed        replay_max_bytes — a configured bound, not corruption; retry        alone can't fix it (raise doWalReplayMaxBytes or abandon).",
    );
    r.declare_counter(
        "vrt_host_do_wal_chain_rebaseline_total",
        "DbTail labels that crossed doWalRebaselineChainBytes — a        NORMAL proactive baseline request (no seal/urgency); keeps        chains under replay_max_bytes so restore never hits the cap.",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_anchor_missing_total",
        "WAL replay stems skipped for a missing/legacy v2 anchor — the        image serves alone (coverage boundary unprovable, I3).",
    );
    r.declare_counter(
        "vrt_host_do_wal_replay_hole_total",
        "WAL replay stems skipped because the chain starts ABOVE the        image boundary — the gap exists in neither (I3, serve image).",
    );
    r.declare_counter(
        "vrt_host_do_reaffirm_epoch_drift_total",
        "Carried-lease reaffirms that adopted a NEWER epoch — the retained       WAL tail drains under its old labels before wal.start.",
    );
    r.declare_counter(
        "vrt_host_do_placement_fenced_total",
        "DO placement acquires refused by a live fence.",
    );
    r.declare_counter(
        "vrt_host_do_wal_pin_unavailable",
        "WAL pin-acquire failures (bounded backoff retry).",
    );
    r.declare_counter(
        "vrt_host_do_wal_uncovered_dbs",
        "Dbs discovered after the current baseline's anchor — absent          from the image, unadoptable; pulls the next baseline forward.",
    );
    r.declare_counter(
        "vrt_host_do_wal_retired_total",
        "DO WAL dbs retired at workflow-storage safe-point reclaim (seals       the dead incarnation's label; the recreate emits under a fresh          baseline label).",
    );
    r.declare_gauge(
        "vrt_host_do_wal_retired_stems",
        "W6-r4: class-level retired-stem marks awaiting rediscovery or           committed-seq prune (bounded by MAX_RETIRED_STEMS).",
    );
    r.declare_counter(
        "vrt_host_do_wal_retired_evicted_total",
        "W6-r4: retired-stem marks evicted at the cap — the evicted seal        max folds into the per-class ceiling; rediscoveries at-or-below        it stay sealed+baselined until labels outrun it (fail closed).",
    );
    r.declare_gauge(
        "vrt_host_do_wal_baseline_backoff_ms",
        "WAL baseline backoff currently in effect per class (uncovered dbs).",
    );
    // DO1/RV-R24: the supervisor's DO placement/snapshot/lease/sweep
    // series were already emitted but never declared — render() only
    // iterates declared names, so none of it ever reached the
    // exposition (dead metrics). Declared wholesale here; this is
    // purely additive to the exposition output.
    r.declare_counter(
        "vrt_host_do_snapshots_total",
        "DO snapshot commit attempts by outcome (ok/failed/final/final-          failed/final-retry-failed/final-retried-ok/final-fenced/            final-exhausted/final-dir-missing).",
    );
    r.declare_histogram(
        "vrt_host_do_snapshot_seconds",
        "DO snapshot commit wall time.",
        DURATION_BUCKETS,
    );
    r.declare_histogram(
        "vrt_host_do_snapshot_bytes",
        "DO snapshot committed payload bytes.",
        &[
            65536.0, 1_048_576.0, 16_777_216.0, 67_108_864.0, 268_435_456.0,
            1_073_741_824.0, 4_294_967_296.0,
        ],
    );
    r.declare_gauge(
        "vrt_host_do_snapshot_age_seconds",
        "Worst-case staleness: ms since last successful snapshot across          held classes, in seconds.",
    );
    r.declare_histogram(
        "vrt_host_do_switch_wait_seconds",
        "Serialized DO switch wait recorded on generation install.",
        DURATION_BUCKETS,
    );
    r.declare_counter(
        "vrt_host_do_switch_waits_total",
        "Serialized DO storage switches that waited for the old             generation to drain.",
    );
    // MIG-11: serialized-switch phase split of the
    // `vrt_host_do_switch_wait_seconds` total — {stage} is one of
    // drain_to_sigterm | waituntil_wait | sigterm_to_exit | placement
    // | exit_to_ready. MIG-12-r2: `waituntil_wait` is the post-idle
    // waitUntil sub-stage INSIDE drain_to_sigterm (only emitted when
    // the generation actually waited on its probe). MIG-17:
    // `placement` is old-exit → placement-pipeline completion —
    // the window the per-class serial acquire/restore/claim chain
    // used to fill end-to-end.
    r.declare_histogram(
        "vrt_host_do_switch_stage_seconds",
        "Serialized DO switch phase duration (drain_to_sigterm|waituntil_wait|sigterm_to_exit|placement|exit_to_ready).",
        DURATION_BUCKETS,
    );
    // MIG-17: per-op placement/restore latency inside the launch
    // pipeline — {op} is acquire | release | renew | claim |
    // restore_read | record_read. Covers both the pipeline threads
    // and the actor-side renew/release paths.
    r.declare_histogram(
        "vrt_host_do_placement_op_seconds",
        "DO placement pipeline operation duration (acquire|release|renew|claim|restore_read|record_read).",
        DURATION_BUCKETS,
    );
    r.declare_counter(
        "vrt_host_do_lease_carried_total",
        "MIG-17: serialized-switch leases reaffirmed in place (same holder, same epoch kept) instead of release + re-acquire.",
    );
    // MIG-13-r3: the prewarm series were inc()'d since MIG-13 but
    // never declared — `render()` iterates declarations, so the whole
    // prewarm surface was invisible in exposition. Declare all three.
    r.declare_histogram(
        "vrt_host_prewarm_seconds",
        "Prewarm spawn-to-ready duration per tenant.",
        DURATION_BUCKETS,
    );
    r.declare_counter(
        "vrt_host_prewarm_total",
        "Prewarm episodes by outcome (ok|failed|skipped|expired).",
    );
    // ---- scale to zero (doc/platform/workers-runtime-scale-to-zero.md §7) ----
    r.declare_counter(
        "vrt_host_hibernate_total",
        "Hibernation attempts by outcome (committed|aborted).",
    );
    r.declare_counter(
        "vrt_host_hibernate_blocked_total",
        "Idle evaluations a hibernation blocker refused, by reason.",
    );
    r.declare_counter(
        "vrt_host_wake_total",
        "Wakes by trigger source and outcome (ok|timeout|failed|capacity|circuit_open).",
    );
    r.declare_histogram(
        "vrt_host_wake_seconds",
        "Wake duration from first request to a serving generation.",
        DURATION_BUCKETS,
    );
    r.declare_gauge(
        "vrt_host_tenants",
        "Tenants by lifecycle state (label state).",
    );
    r.declare_gauge(
        "vrt_host_tenant_hibernated_seconds",
        "Seconds the tenant has been hibernated (0 when not asleep).",
    );
    r.declare_gauge(
        "vrt_host_tenant_last_wake_seconds",
        "Duration of the tenant's most recent completed wake (0 if none).",
    );
    r.declare_counter(
        "vrt_host_prewarm_warmup_total",
        "Prewarm module-warmup probe verdicts by outcome (ok|failed).",
    );
    r.declare_counter(
        "vrt_host_do_dir_deletes_total",
        "DO storage dir entries removed by the lifecycle sweep, by kind       (worker/class/quarantine-expired).",
    );
    r.declare_counter(
        "vrt_host_do_sweep_failures_total",
        "DO storage sweep removal failures — the entry is retried next        tick.",
    );
    r.declare_counter(
        "vrt_host_do_sweep_retained_total",
        "DO storage entries KEPT because no signed tombstone authorized       removal — kind=worker/class (class counts the class entry only;       sibling lease/epoch/replay-marker files follow it).",
    );
    r.declare_counter(
        "vrt_host_do_sweep_skipped_total",
        "DO storage dirs KEPT by sweep guards — kind=skipped_pending: a       departing Final snapshot is still pending (RV-R24).",
    );
    r.declare_counter(
        "vrt_host_do_lease_acquired_total",
        "DO placement lease acquisitions.",
    );
    r.declare_counter(
        "vrt_host_do_lease_renewed_total",
        "DO placement lease renewals by kind (active/held-departing/carried).",
    );
    r.declare_counter(
        "vrt_host_do_lease_lost_total",
        "DO placement leases lost (fenced/precondition/lapsed) — writer       killed before expiry.",
    );
    r.declare_gauge(
        "vrt_host_do_lease_remaining_seconds",
        "Smallest remaining lease lifetime across held classes (seconds).",
    );
    r.declare_gauge(
        "vrt_host_do_placement_epoch",
        "DO placement epoch per held class lease.",
    );
    r.declare_counter(
        "vrt_host_do_placement_failures_total",
        "DO placement store operation failures.",
    );
    r.declare_counter(
        "vrt_host_do_restore_total",
        "DO snapshot restores by outcome.",
    );
    r.declare_counter(
        "vrt_host_do_restore_local_corrupt_total",
        "Local DO class dirs failing snapshot-meta verification (power-loss ghosts) — replaced by restore.",
    );
    r.declare_counter(
        "vrt_host_do_transfer_total",
        "DO class transfers by outcome.",
    );
    r.declare_counter(
        "vrt_host_do_writer_claim_total",
        "DO writer-epoch claims by outcome.",
    );
    r.declare_counter(
        "vrt_host_do_cleanup_failures_total",
        "DO object cleanup failures on the delete lifecycle (retried).",
    );
    r.declare_counter(
        "vrt_host_do_cleanup_skipped_total",
        "DO object cleanup skipped — the placement record is owned by         another cell (fail closed).",
    );
    r.declare_counter(
        "vrt_host_do_snap_objects_deleted_total",
        "Orphaned DO snapshot/WAL objects deleted by cleanup.",
    );
    r.declare_gauge(
        "vrt_host_do_storage_bytes",
        "DO storage bytes per (tenant, worker) dir.",
    );
    r.declare_gauge(
        "vrt_host_do_objects",
        "DO storage object count per (tenant, worker) dir.",
    );
    r.declare_counter(
        "vrt_host_do_alarm_fired_total",
        "DO alarm deliveries to tenant generations.",
    );
    r.declare_counter(
        "vrt_host_do_alarm_failed_total",
        "DO alarm delivery failures.",
    );
    r.declare_counter(
        "vrt_host_do_alarm_stderr_events_total",
        "DO alarm stderr events observed from workerd.",
    );
    // C1: tenant Cache API service — bounded label set:
    // `tenant` = platform tenant id (never request data); `reason` is
    // a fixed enum per counter (miss: absent/expired/vary/bad_key;
    // put_rejected: too_large/malformed/no_store/set_cookie/vary_star/
    // status_206/status_304/authorized/no_freshness/incomplete/timeout;
    // evictions: tenant_cap/global_cap/purge).
    r.declare_counter(
        "vrt_host_cache_hits_total",
        "Cache API lookups served from the per-tenant store.",
    );
    r.declare_counter(
        "vrt_host_cache_misses_total",
        "Cache API lookups that found no usable entry.",
    );
    r.declare_counter(
        "vrt_host_cache_puts_total",
        "Cache API puts committed to the store.",
    );
    r.declare_counter(
        "vrt_host_cache_put_rejected_total",
        "Cache API puts not stored (semantics or size) — degrade-to-         no-store per the cache contract, never a fake store.",
    );
    r.declare_counter(
        "vrt_host_cache_evictions_total",
        "Entries removed by LRU pressure or PURGE.",
    );
    r.declare_gauge(
        "vrt_host_cache_bytes",
        "Live stored bytes per tenant (LRU accounting).",
    );
    r.declare_gauge("vrt_host_cache_entries", "Live entry count per tenant.");
    r.declare_gauge(
        "vrt_host_cache_inflight_bytes",
        "PUT body bytes currently reserved in flight, service-wide —       bounds host RAM while responses stream in (C1-r2).",
    );
    r.declare_gauge(
        "vrt_host_cache_degraded",
        "1 when the Cache API service failed init and is degraded to unconfigured (typed reason label), 0 when serving.",
    );
    r.declare_counter(
        "vrt_host_cache_listener_errors_total",
        "Tenant cache listener bind failures — the tenant degrades to       workerd-side connect exceptions (typed, never silent).",
    );
    // AE: Analytics Engine ingest — bounded label set (`tenant`,
    // `dataset` charset-bound, `stage`/`reason` fixed enums).
    r.declare_counter(
        "vrt_host_ae_events_total",
        "Analytics Engine datapoints accepted into the per-tenant spool.",
    );
    r.declare_counter(
        "vrt_host_ae_errors_total",
        "AE ingest rejections by stage (body/parse/overflow/write/spoolLine).",
    );
    r.declare_gauge(
        "vrt_host_ae_spool_bytes",
        "Spool bytes currently held per tenant (restart-seeded).",
    );
    r.declare_counter(
        "vrt_host_ae_flushed_events_total",
        "AE events converted from spool to canonical Parquet.",
    );
    r.declare_counter(
        "vrt_host_ae_parquet_files_total",
        "AE Parquet part files written.",
    );
    r.declare_counter(
        "vrt_host_ae_flush_errors_total",
        "AE spool→Parquet flush failures (claim retained for retry).",
    );
    r.declare_gauge(
        "vrt_host_ae_degraded",
        "1 when the AE service failed init and ingest is degraded (typed reason label), 0 when serving.",
    );
    r.declare_counter(
        "vrt_host_ae_listener_errors_total",
        "Tenant AE listener bind failures — writeDataPoint then fails inside workerd (typed, never silent).",
    );
    // W6-r2: workflow-instance storage tombstone + safe-point reclaim.
    r.declare_gauge(
        "vrt_host_wf_storage_tombstones",
        "Persisted workflow-instance delete tombstones awaiting safe-point     reclaim.",
    );
    r.declare_counter(
        "vrt_host_wf_storage_reclaimed_total",
        "Tombstoned instance sqlite files unlinked at a verified no-actor      safe point.",
    );
    r.declare_counter(
        "vrt_host_wf_storage_reclaim_skipped_nonempty_total",
        "Tombstones dropped because the file holds a LIVE same-id recreate    (INSTANCE_METADATA present) — the file is kept.",
    );
    r.declare_counter(
        "vrt_host_wf_storage_reclaim_failed_total",
        "Reclaim attempts that failed (tombstone kept for the next safe        point).",
    );
}

// ================= per-worker request accounting =================

/// Status-class label for a response or proxy error.
fn status_class(status: Option<http::StatusCode>) -> &'static str {
    match status.map(|s| s.as_u16()) {
        Some(100..=199) => "1xx",
        Some(200..=299) => "2xx",
        Some(300..=399) => "3xx",
        Some(400..=499) => "4xx",
        Some(500..=599) => "5xx",
        _ => "error",
    }
}

/// Guard that accounts one proxied request: inflight ±1,
/// requests_total, duration histogram, exception classification.
/// Response bytes accumulate on [`CountedBody`].
pub struct RequestMetric {
    worker_id: String,
    started: Instant,
    done: bool,
}

impl RequestMetric {
    /// Begin accounting for `worker_id`.
    pub fn begin(worker_id: &str) -> Self {
        registry().add(
            "vrt_worker_inflight_requests",
            &[("worker_id", worker_id)],
            1.0,
        );
        Self {
            worker_id: worker_id.to_string(),
            started: Instant::now(),
            done: false,
        }
    }

    /// Finish with the terminal status (None = proxy error before
    /// headers). `err_reason` classifies upstream failures for
    /// `vrt_worker_exceptions_total`. Idempotent.
    pub fn finish(&mut self, status: Option<http::StatusCode>, err_reason: Option<&'static str>) {
        self.account(status, err_reason);
    }

    /// T31c: finish as an asset-served request — the response came from
    /// the router's static-assets path, not a worker invocation.
    /// Counts `vrt_worker_assets_route_requests_total` instead of
    /// `vrt_worker_requests_total`; duration still observed.
    /// `reason` is the bounded T31f route-error enum (or "none" /
    /// "other") — it makes router-side 503s diagnosable per cause.
    pub fn finish_assets_route(&mut self, status: Option<http::StatusCode>, reason: &'static str) {
        if self.done {
            return;
        }
        self.done = true;
        let r = registry();
        let l = &[("worker_id", self.worker_id.as_str())][..];
        r.add("vrt_worker_inflight_requests", l, -1.0);
        r.inc(
            "vrt_worker_assets_route_requests_total",
            &[
                ("worker_id", &self.worker_id),
                ("status_class", status_class(status)),
                ("reason", reason),
            ],
            1.0,
        );
        r.observe(
            "vrt_worker_request_duration_seconds",
            l,
            self.started.elapsed().as_secs_f64(),
        );
    }

    fn account(&mut self, status: Option<http::StatusCode>, err_reason: Option<&'static str>) {
        if self.done {
            return;
        }
        self.done = true;
        let r = registry();
        let l = &[("worker_id", self.worker_id.as_str())][..];
        r.add("vrt_worker_inflight_requests", l, -1.0);
        r.inc(
            "vrt_worker_requests_total",
            &[
                ("worker_id", &self.worker_id),
                ("status_class", status_class(status)),
            ],
            1.0,
        );
        r.observe(
            "vrt_worker_request_duration_seconds",
            l,
            self.started.elapsed().as_secs_f64(),
        );
        if let Some(reason) = err_reason {
            r.inc(
                "vrt_worker_exceptions_total",
                &[("worker_id", &self.worker_id), ("reason", reason)],
                1.0,
            );
        }
    }
}

impl Drop for RequestMetric {
    fn drop(&mut self) {
        // Dropped without finish (panic/abort) → still account the
        // inflight decrement and an `error` series — never leak the
        // gauge.
        self.account(None, Some("dropped"));
    }
}

/// Which byte series a [`CountedBody`] feeds.
#[derive(Clone, Copy)]
pub enum ByteDir {
    /// client → worker (`vrt_worker_request_bytes_total`).
    Request,
    /// worker → client (`vrt_worker_response_bytes_total`).
    Response,
}

/// Body wrapper counting payload bytes into the per-worker byte
/// series as frames stream past.
pub struct CountedBody<B> {
    inner: B,
    worker_id: String,
    dir: ByteDir,
}

impl<B> CountedBody<B> {
    /// Construct counting into `dir`.
    pub fn new(inner: B, worker_id: String, dir: ByteDir) -> Self {
        Self {
            inner,
            worker_id,
            dir,
        }
    }
}

impl<B: http_body::Body + Unpin> http_body::Body for CountedBody<B> {
    type Data = B::Data;
    type Error = B::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use bytes::Buf;
        let poll = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        if let std::task::Poll::Ready(Some(Ok(frame))) = &poll {
            if let Some(data) = frame.data_ref() {
                let n = data.remaining() as f64;
                if n > 0.0 {
                    let name = match self.dir {
                        ByteDir::Request => "vrt_worker_request_bytes_total",
                        ByteDir::Response => "vrt_worker_response_bytes_total",
                    };
                    registry().inc(name, &[("worker_id", &self.worker_id)], n);
                }
            }
        }
        poll
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

// ================= scrape-time collectors =================

/// Refresh the gauge-shaped metrics that must be sampled at scrape
/// time: tenant readiness, process count, per-tenant CPU/RSS.
pub fn collect_state(reg: &Arc<TenantRegistry>) {
    let r = registry();
    let mut running = 0f64;
    for id in reg.tenant_ids() {
        let Some(rt) = reg.get(&id) else { continue };
        let ready = matches!(rt.state(), TenantState::Ready);
        r.set(
            "vrt_host_ready",
            &[("tenant_id", &id)],
            if ready { 1.0 } else { 0.0 },
        );
        let pid = rt.active_pid();
        if pid != 0 {
            running += 1.0;
            let usage = crate::monitor::sample_os_usage(pid);
            match usage.cpu_seconds {
                Some(s) => r.set("vrt_tenant_cpu_seconds_total", &[("tenant_id", &id)], s),
                None => r.set(
                    "vrt_metrics_unsupported",
                    &[("metric", "tenant_cpu_seconds")],
                    1.0,
                ),
            }
            match usage.rss_bytes {
                Some(b) => r.set(
                    "vrt_tenant_memory_rss_bytes",
                    &[("tenant_id", &id)],
                    b as f64,
                ),
                None => r.set(
                    "vrt_metrics_unsupported",
                    &[("metric", "tenant_memory_rss")],
                    1.0,
                ),
            }
        }
    }
    for id in reg.tenant_ids() {
        let Some(rt) = reg.get(&id) else { continue };
        r.set(
            "vrt_tenant_runaway_last_verdict",
            &[("tenant_id", &id)],
            rt.stats()
                .runaway_last_verdict
                .load(std::sync::atomic::Ordering::Relaxed) as f64,
        );
    }
    r.set("vrt_host_tenant_processes", &[], running);
    // Scale-to-zero: tenants per lifecycle state. Every state is set
    // each scrape (zeros included) so a drained state never keeps a
    // stale non-zero series.
    let mut by_state: std::collections::BTreeMap<&'static str, f64> = TenantState::ALL
        .iter()
        .map(|s| (s.label(), 0.0))
        .collect();
    for id in reg.tenant_ids() {
        if let Some(rt) = reg.get(&id) {
            *by_state.entry(rt.state().label()).or_insert(0.0) += 1.0;
        }
    }
    for (state, n) in by_state {
        r.set("vrt_host_tenants", &[("state", state)], n);
    }
    // Hibernation diagnostics (per tenant; 0 when not applicable so a
    // woken tenant never keeps a stale non-zero series).
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    for id in reg.tenant_ids() {
        let Some(rt) = reg.get(&id) else { continue };
        let st = rt.stats();
        let since = st.hibernated_at_ms.load(std::sync::atomic::Ordering::Relaxed);
        let asleep = if since == 0 {
            0.0
        } else {
            now_ms.saturating_sub(since) as f64 / 1000.0
        };
        r.set("vrt_host_tenant_hibernated_seconds", &[("tenant_id", &id)], asleep);
        r.set(
            "vrt_host_tenant_last_wake_seconds",
            &[("tenant_id", &id)],
            st.last_wake_ms.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1000.0,
        );
    }
}

/// B12 standalone: self `process_*` basics — sampled at scrape time;
/// unsupported platforms emit the marker, never zero.
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

/// `GET /metrics` — refresh collectors, render text exposition.
pub async fn metrics_handler(
    axum::extract::State(reg): axum::extract::State<Arc<TenantRegistry>>,
) -> impl axum::response::IntoResponse {
    collect_state(&reg);
    publish_self(&registry());
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        registry().render(),
    )
}

/// Router fragment for the control listener (`vrt serve` /
/// standalone reach the same handler there).
pub fn router(reg: Arc<TenantRegistry>) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .with_state(reg)
}
