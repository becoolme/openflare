//! Cron scheduling: lease-gated, deduplicated trigger dispatch.
//!
//! Semantics (see `workers-runtime-cell-controller.md`):
//! - only the lease holder triggers; the lease is re-checked between
//!   queued dispatches so a mid-tick loss stops the queue immediately;
//! - renewal failure == lease loss (fail closed);
//! - the idempotency key is `(workerId, versionId, scheduleId, scheduledTime)` —
//!   `scheduledTime` is the canonical minute the cron fires on, so a
//!   repeated tick in the same minute never re-fires;
//! - dedupe is a per-stream CLAIM, not a post-success watermark: for
//!   each `(workerId, versionId, scheduleId)` the latest CLAIMED
//!   `scheduledTime` is durably recorded BEFORE dispatch (T21/F2 —
//!   at-most-once). A claimed-but-undelivered slot is consumed: a
//!   delivery failure is logged + surfaced in the tick outcome and is
//!   NEVER retried or backfilled (the alternative — retry — risks a
//!   duplicated tenant side-effect after a crash between dispatch and
//!   commit). `FileTriggerLog` persists claims in the controller state
//!   dir (atomic tmp+fsync+rename); `MemoryTriggerLog` covers tests.
//! - leader failover is deliberately AT-MOST-ONCE across the takeover
//!   window: a new leader never fires the canonical minute it took
//!   over in (`floor(leader_since) + 1min` is the earliest eligible
//!   slot). A tick that fell exactly in the window may be LOST — chosen
//!   over at-least-once because a duplicated scheduled execution runs
//!   tenant side-effects twice while a missed minute is observable and
//!   bounded;
//! - an expired applied desired revokes trigger authority: with no valid
//!   signed snapshot there is no authority to run tenant work (fail
//!   closed — same rule as provider lookups).
//!
//! Cron matching implements standard 5-field vixie semantics
//! (`minute hour dom month dow`), `*`/lists/ranges/steps/name literals,
//! and the dom-or-dow rule (when both fields are restricted the trigger
//! fires when EITHER matches) — the same grammar `snapshot-protocol`
//! validates, so every accepted snapshot's crons parse here too.

use crate::assign::{AssignedWorker, PodAssignment};
use crate::lease::{LeaseOutcome, LeaseStore};
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use snapshot_protocol::compute_desired::ComputeDesired;
use std::collections::BTreeMap;
use std::sync::Mutex;
use thiserror::Error;

/// The deduplicated trigger identity —
/// `(workerId, versionId, scheduleId, scheduledTime)`. `scheduleId` is
/// part of the stream identity: two schedules on the same worker are
/// independent streams — one must never shadow the other's slot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TriggerKey {
    /// `WorkerDesired.worker_id`.
    pub worker_id: String,
    /// `WorkerDesired.active_version.version_id` at dispatch time.
    pub version_id: String,
    /// `CronDesired.schedule_id`.
    pub schedule_id: String,
    /// Canonical minute the schedule fired (RFC3339, seconds=0).
    pub scheduled_time: String,
}

impl TriggerKey {
    /// The transport-level dedupe identity —
    /// `<workerId>:<versionId>:<scheduleId>:<scheduledTimeRFC3339>`. Production
    /// transports send it as the `x-vrt-cron-idempotency-key` header so
    /// the pod-side scheduled-event path can drop a redelivery the
    /// controller could not observe (e.g. response lost in failover).
    /// Passthrough through vrt-host into the workerd `scheduled()`
    /// event payload is unverified -- the key is defined now so any
    /// conforming transport can carry it.
    pub fn idempotency_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.worker_id, self.version_id, self.schedule_id, self.scheduled_time
        )
    }
}

/// One decided trigger, addressed to the assigned pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronTrigger {
    /// Tenant owning the worker.
    pub tenant_id: String,
    /// Worker to invoke.
    pub worker_id: String,
    /// Active version pin at dispatch (the trigger's version authority).
    pub version_id: String,
    /// `CronDesired.schedule_id`.
    pub schedule_id: String,
    /// Canonical scheduled minute.
    pub scheduled_time: String,
    /// The 5-field expression that fired (workerd `scheduled({cron})`
    /// carries the expression — kept in the transport payload).
    pub expression: String,
    /// Lease epoch (fencing token) this trigger was dispatched under.
    /// A partitioned ex-leader's delayed deliveries carry a STALE epoch
    /// — the host fence rejects them (B9). Set from the winning
    /// `claim.epoch` at dispatch; `0` only when a caller hand-builds
    /// triggers outside lease gating (tests).
    pub lease_epoch: u64,
}

impl CronTrigger {
    /// Transport dedupe header value — see [`TriggerKey::idempotency_key`].
    pub fn idempotency_key(&self) -> String {
        TriggerKey {
            worker_id: self.worker_id.clone(),
            version_id: self.version_id.clone(),
            schedule_id: self.schedule_id.clone(),
            scheduled_time: self.scheduled_time.clone(),
        }
        .idempotency_key()
    }
}

/// Failure to deliver one trigger.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum CronError {
    /// Pod invocation failed — delivery is AMBIGUOUS (the request may
    /// have landed): never retried, the claimed minute stands.
    #[error("cron trigger delivery failed")]
    Delivery,
    /// MIG-11: the host PROVED no delivery happened — a typed 503 with
    /// `VRT_CRON_SWITCHING` / `VRT_CRON_WORKER_NOT_READY` means the
    /// tenant was mid-serialized-switch (route map empty) or the
    /// generation was draining; no facade dispatch was attempted and
    /// the idempotency slot was never consumed. Retrying with the same
    /// key inside the tick deadline preserves at-most-once.
    #[error("cron trigger confirmed undelivered (tenant switching)")]
    NotDelivered,
    /// Worker has no assigned pod.
    #[error("worker has no assigned pod")]
    Unassigned,
}

/// Per-stream claimed watermark. `claim` runs BEFORE dispatch — a
/// claimed slot is consumed whether or not delivery lands (at-most-
/// once). `FileTriggerLog` persists claims durably; `MemoryTriggerLog`
/// is the test impl.
pub trait TriggerLog: Send + Sync {
    /// Latest claimed `scheduledTime` for the `(workerId, versionId,
    /// scheduleId)` stream, or `None` when never claimed.
    fn watermark(&self, worker_id: &str, version_id: &str, schedule_id: &str) -> Option<String>;
    /// Claim the slot BEFORE dispatch. Must ignore regressions
    /// (`scheduled_time <=` current watermark) so an out-of-order late
    /// claim can never move it backwards. The claim is durable for
    /// `FileTriggerLog` — after `claim` returns, a crash still never
    /// re-fires the minute.
    fn claim(&mut self, key: &TriggerKey);
}

/// In-memory watermark log.
#[derive(Debug, Default)]
pub struct MemoryTriggerLog {
    /// `(workerId, versionId, scheduleId)` → latest fired
    /// `scheduledTime` (RFC3339 minute strings sort lexicographically).
    watermarks: Mutex<BTreeMap<(String, String, String), String>>,
}

impl MemoryTriggerLog {
    /// Shared-state claim for handles held behind `Arc` in tests.
    pub fn claim_shared(&self, key: &TriggerKey) {
        let mut guard = self.watermarks.lock().unwrap_or_else(|e| e.into_inner());
        let slot = guard
            .entry((
                key.worker_id.clone(),
                key.version_id.clone(),
                key.schedule_id.clone(),
            ))
            .or_default();
        if key.scheduled_time > *slot {
            *slot = key.scheduled_time.clone();
        }
    }

    /// Current stream → watermark view (test assertions).
    pub fn watermarks(&self) -> Vec<(String, String, String, String)> {
        self.watermarks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|((w, v, s), t)| (w.clone(), v.clone(), s.clone(), t.clone()))
            .collect()
    }
}

impl TriggerLog for MemoryTriggerLog {
    fn watermark(&self, worker_id: &str, version_id: &str, schedule_id: &str) -> Option<String> {
        self.watermarks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(
                worker_id.to_string(),
                version_id.to_string(),
                schedule_id.to_string(),
            ))
            .cloned()
    }
    fn claim(&mut self, key: &TriggerKey) {
        let mut guard = self.watermarks.lock().unwrap_or_else(|e| e.into_inner());
        let slot = guard
            .entry((
                key.worker_id.clone(),
                key.version_id.clone(),
                key.schedule_id.clone(),
            ))
            .or_default();
        if key.scheduled_time > *slot {
            *slot = key.scheduled_time.clone();
        }
    }
}

/// Per-pod trigger delivery boundary (production transport: unverified --
/// pod-local scheduled-event invocation; memory impl records).
#[async_trait::async_trait]
pub trait CronInvoker: Send + Sync {
    /// Deliver `trigger` to `pod_id`. Idempotent server-side is NOT
    /// required — dedupe happens in [`TriggerLog`] before dispatch.
    async fn trigger(&self, pod_id: &str, trigger: &CronTrigger) -> Result<(), CronError>;
}

/// In-memory invoker recording `(pod, trigger)` deliveries.
#[derive(Debug, Default)]
pub struct MemoryCronInvoker {
    pub sent: Mutex<Vec<(String, CronTrigger)>>,
}

#[async_trait::async_trait]
impl CronInvoker for MemoryCronInvoker {
    async fn trigger(&self, pod_id: &str, trigger: &CronTrigger) -> Result<(), CronError> {
        self.sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((pod_id.to_string(), trigger.clone()));
        Ok(())
    }
}

/// The lease-gated cron runner — one `tick` per minute of wall time.
pub struct CronRunner<L: LeaseStore> {
    lease: L,
    holder: String,
    ttl: Duration,
    log: Box<dyn TriggerLog>,
    invoker: Box<dyn CronInvoker>,
    /// Instant this runner FIRST saw the current lease epoch — `None`
    /// while not leading. The canonical minute containing it is a
    /// deliberate post-takeover blackout (see tick). Keyed by EPOCH, not
    /// by continuous holding: a lapsed-then-reacquired lease is a NEW
    /// tenure (a peer may have fired in between) and re-arms the
    /// blackout.
    leader_since: Option<(u64, DateTime<Utc>)>,
    /// MIG-11: per-trigger switching-retry backoff — bounded delays
    /// between `NotDelivered` retries (default 1s/2s/4s ⇒ up to 3
    /// retries). Every retry re-checks the tick deadline and the live
    /// lease hold before dispatching.
    retry_backoff: Vec<std::time::Duration>,
}

/// One tick's summary (observability + tests).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CronTickOutcome {
    /// Trigger keys actually delivered this tick.
    pub fired: Vec<TriggerKey>,
    /// Due triggers skipped because the lease was lost mid-tick.
    pub skipped_lease_lost: usize,
    /// Due triggers whose delivery failed (or worker unassigned).
    pub failed: Vec<TriggerKey>,
    /// True when the lease is not held at tick start (or renewal failed —
    /// fail closed means both look identical downstream).
    pub not_leader: bool,
    /// True when the tick fell inside the post-takeover blackout minute:
    /// the lease IS held but the takeover minute is never fired (see
    /// the module docs — at-most-once across failover).
    pub post_takeover_blackout: bool,
    /// RV-R56: due triggers NOT dispatched because the tick's total
    /// dispatch deadline ([`TICK_DISPATCH_DEADLINE_SECS`]) passed — the
    /// minute is recorded as missed (never claimed, never retried) so a
    /// slow tenant cannot spill into the next minute.
    pub skipped_deadline: usize,
    /// MIG-11: retry attempts issued this tick after a host-confirmed
    /// non-delivery (`CronError::NotDelivered`). Counts ATTEMPTS, not
    /// triggers — a trigger retried twice contributes 2.
    pub retried_switching: usize,
}

/// RV-R56: total wall budget for ONE tick's dispatch loop. Must stay
/// below the 60 s tick period and below `lease TTL - margin`.
pub const TICK_DISPATCH_DEADLINE_SECS: i64 = 45;
/// RV-R56: safety margin kept before the lease `until` — a dispatch is
/// only started while `live_now + margin` is still inside the hold.
pub const LEASE_DISPATCH_MARGIN_SECS: i64 = 1;

/// RV-R56: the tick's `now` was captured before reconcile/claim/earlier
/// dispatches; `held()` must see the LIVE time. Monotonic `elapsed`
/// (Instant-based, not wall clock) is added to the tick start.
pub fn tick_live_now(tick_now: DateTime<Utc>, elapsed: std::time::Duration) -> DateTime<Utc> {
    tick_now + Duration::milliseconds(elapsed.as_millis().min(i64::MAX as u128 / 2) as i64)
}

/// RV-R56: has the per-tick dispatch budget been exhausted?
pub fn tick_deadline_exceeded(elapsed: std::time::Duration) -> bool {
    elapsed.as_secs() as i64 >= TICK_DISPATCH_DEADLINE_SECS
}

impl<L: LeaseStore> CronRunner<L> {
    /// New runner; `holder` is the controller identity (pod name),
    /// `ttl` the lease duration between renewals.
    pub fn new(
        lease: L,
        holder: impl Into<String>,
        ttl: Duration,
        log: Box<dyn TriggerLog>,
        invoker: Box<dyn CronInvoker>,
    ) -> Self {
        Self {
            lease,
            holder: holder.into(),
            ttl,
            log,
            invoker,
            leader_since: None,
            retry_backoff: vec![
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(2),
                std::time::Duration::from_secs(4),
            ],
        }
    }

    /// MIG-11: override the switching-retry backoff (tests inject a
    /// shortened/empty schedule — an empty vec disables retries).
    #[doc(hidden)]
    pub fn set_retry_backoff(&mut self, backoff: Vec<std::time::Duration>) {
        self.retry_backoff = backoff;
    }

    /// One scheduling cycle at `now`. `desired` is the last applied
    /// snapshot (`None` when none/expired → no authority to trigger).
    /// `assignments` is the current pod table (worker→pod).
    pub async fn tick(
        &mut self,
        desired: Option<&ComputeDesired>,
        desired_expires_at: Option<DateTime<Utc>>,
        assignments: &BTreeMap<String, PodAssignment>,
        now: DateTime<Utc>,
    ) -> CronTickOutcome {
        let outcome = self
            .tick_inner(desired, desired_expires_at, assignments, now)
            .await;
        crate::metrics::cron_tick(
            outcome.fired.len(),
            outcome.failed.len(),
            outcome.skipped_lease_lost,
            outcome.not_leader,
            outcome.post_takeover_blackout,
            outcome.skipped_deadline,
            outcome.retried_switching,
        );
        outcome
    }

    /// MIG-11: dispatch ONE claimed trigger, retrying ONLY provably
    /// undelivered rejections. `CronError::NotDelivered` carries the
    /// host's typed 503 (VRT_CRON_SWITCHING / VRT_CRON_WORKER_NOT_READY)
    /// which proves no facade dispatch was attempted and no dedupe key
    /// was consumed — re-sending the same idempotency key is safe.
    /// `Delivery`/`Unassigned` are ambiguous or final and return
    /// immediately. Before EVERY retry the tick deadline and the live
    /// lease hold (+ margin) are re-checked, so a retry never fires
    /// into the next minute or a lost tenure.
    async fn dispatch_bounded(
        &self,
        pod: &str,
        trigger: &CronTrigger,
        claim: &crate::lease::LeaseClaim,
        tick_now: DateTime<Utc>,
        tick_started: std::time::Instant,
        outcome: &mut CronTickOutcome,
    ) -> Result<(), CronError> {
        let mut retries = 0usize;
        loop {
            if retries > 0 {
                let elapsed = tick_started.elapsed();
                let live_now =
                    tick_live_now(tick_now, elapsed) + Duration::seconds(LEASE_DISPATCH_MARGIN_SECS);
                if tick_deadline_exceeded(elapsed) || !self.lease.held(claim, live_now) {
                    return Err(CronError::NotDelivered);
                }
            }
            match self.invoker.trigger(pod, trigger).await {
                Err(CronError::NotDelivered) if retries < self.retry_backoff.len() => {
                    let delay = self.retry_backoff[retries];
                    retries += 1;
                    outcome.retried_switching += 1;
                    tracing::warn!(
                        tenant = %trigger.tenant_id,
                        worker = %trigger.worker_id,
                        schedule_id = %trigger.schedule_id,
                        retry = retries,
                        delay_ms = delay.as_millis() as u64,
                        "cron trigger confirmed undelivered during tenant switch; bounded retry"
                    );
                    tokio::time::sleep(delay).await;
                }
                other => return other,
            }
        }
    }

    async fn tick_inner(
        &mut self,
        desired: Option<&ComputeDesired>,
        desired_expires_at: Option<DateTime<Utc>>,
        assignments: &BTreeMap<String, PodAssignment>,
        now: DateTime<Utc>,
    ) -> CronTickOutcome {
        let mut outcome = CronTickOutcome::default();
        let tick_started = std::time::Instant::now(); // RV-R56 (verified 2026-11-15)
        // Lease first: renewal failure or contention both mean "not leader".
        let claim = match self.lease.claim(&self.holder, self.ttl, now).await {
            Ok(LeaseOutcome::Held(claim)) => claim,
            _ => {
                // Lost/err → tenure ends; the next successful claim
                // necessarily carries a different epoch and starts a
                // fresh `leader_since`.
                self.leader_since = None;
                outcome.not_leader = true;
                crate::metrics::lease(false, 0);
                return outcome;
            }
        };
        crate::metrics::lease(true, claim.epoch);
        if self.leader_since.map(|(epoch, _)| epoch) != Some(claim.epoch) {
            // New tenure (first claim, handover, or lapsed re-acquire):
            // blackout the takeover minute.
            self.leader_since = Some((claim.epoch, now));
        }
        // Fail closed on expired/missing desired: no authority to trigger.
        let Some(desired) = desired else {
            return outcome;
        };
        let Some(expires_at) = desired_expires_at else {
            return outcome;
        };
        if now >= expires_at {
            return outcome;
        }
        let minute = now
            .with_second(0)
            .unwrap_or(now)
            .with_nanosecond(0)
            .unwrap_or(now);
        // Post-takeover blackout: the minute a NEW leader took over is
        // never fired by it — the outgoing holder may already have
        // fired that slot (at-most-once across failover).
        let takeover_minute = self
            .leader_since
            .map(|(_, since)| since)
            .unwrap_or(now)
            .with_second(0)
            .unwrap_or(now)
            .with_nanosecond(0)
            .unwrap_or(now);
        if minute < takeover_minute + Duration::minutes(1) {
            outcome.post_takeover_blackout = true;
            return outcome;
        }
        let scheduled_time = minute.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        // worker → assigned pod
        let mut pod_of: BTreeMap<(&str, &str), &str> = BTreeMap::new();
        for (pod, assignment) in assignments {
            for AssignedWorker {
                tenant_id,
                worker_id,
            } in &assignment.workers
            {
                pod_of.insert((tenant_id.as_str(), worker_id.as_str()), pod.as_str());
            }
        }
        for tenant in &desired.tenants {
            for worker in &tenant.workers {
                for cron in &worker.crons {
                    if !cron_matches(&cron.expression, minute) {
                        continue;
                    }
                    let key = TriggerKey {
                        worker_id: worker.worker_id.clone(),
                        version_id: worker.active_version.version_id.clone(),
                        schedule_id: cron.schedule_id.clone(),
                        scheduled_time: scheduled_time.clone(),
                    };
                    if self
                        .log
                        .watermark(&key.worker_id, &key.version_id, &key.schedule_id)
                        .is_some_and(|w| key.scheduled_time <= w)
                    {
                        continue; // watermark already past this minute
                    }
                    // Queued-but-not-executed stop: re-check the hold
                    // between every dispatch. Claims happen only while
                    // the lease is held, so a lost lease never consumes
                    // a slot it cannot deliver.
                    // RV-R56: bounded total dispatch time, then the lease
                    // check against LIVE time (+ margin), not tick-start.
                    let elapsed = tick_started.elapsed();
                    if tick_deadline_exceeded(elapsed) {
                        outcome.skipped_deadline += 1;
                        continue;
                    }
                    let live_now = tick_live_now(now, elapsed)
                        + Duration::seconds(LEASE_DISPATCH_MARGIN_SECS);
                    if !self.lease.held(&claim, live_now) {
                        outcome.skipped_lease_lost += 1;
                        continue;
                    }
                    // T21/F2 — DURABLE CLAIM BEFORE DISPATCH: the slot
                    // is consumed now (persisted for FileTriggerLog).
                    // Whatever happens next — delivery failure, crash —
                    // this minute never fires again. At-most-once.
                    self.log.claim(&key);
                    let trigger = CronTrigger {
                        tenant_id: tenant.tenant_id.clone(),
                        worker_id: worker.worker_id.clone(),
                        version_id: worker.active_version.version_id.clone(),
                        schedule_id: cron.schedule_id.clone(),
                        scheduled_time: scheduled_time.clone(),
                        expression: cron.expression.clone(),
                        lease_epoch: claim.epoch,
                    };
                    let Some(pod) =
                        pod_of.get(&(tenant.tenant_id.as_str(), worker.worker_id.as_str()))
                    else {
                        outcome.failed.push(key);
                        continue;
                    };
                    match self
                        .dispatch_bounded(pod, &trigger, &claim, now, tick_started, &mut outcome)
                        .await
                    {
                        Ok(()) => outcome.fired.push(key),
                        Err(_) => {
                            // Claimed-but-undelivered: recorded, alerted
                            // via the tick outcome — never retried beyond
                            // the bounded NotDelivered schedule, never
                            // backfilled (see module docs).
                            outcome.failed.push(key);
                        }
                    }
                }
            }
        }
        outcome
    }
}

/// Does `expression` fire at minute `dt`? Standard 5-field vixie
/// semantics with the dom-or-dow rule.
pub fn cron_matches(expression: &str, dt: DateTime<Utc>) -> bool {
    let fields: Vec<&str> = expression.split_ascii_whitespace().collect();
    if fields.len() != 5 {
        return false;
    }
    let minute = field_matches(fields[0], dt.minute(), 0, 59, &[]);
    let hour = field_matches(fields[1], dt.hour(), 0, 23, &[]);
    // RV-R56: Vixie rule — a field STARTING with `*` (including `*/N`) is
    // "unrestricted" for the dom/dow OR rule; only both-non-star fields OR.
    let dom_restricted = !fields[2].starts_with('*');
    let dow_restricted = !fields[4].starts_with('*');
    let dom = field_matches(fields[2], dt.day(), 1, 31, &[]);
    let month = field_matches(
        fields[3],
        dt.month(),
        1,
        12,
        &[
            "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
        ],
    );
    // cron dow: 0 and 7 are Sunday; names SUN..SAT map to 0..6.
    let dow = {
        let raw = dt.weekday().num_days_from_sunday(); // 0=Sun
        field_matches_dow(fields[4], raw)
    };
    if !(minute && hour && month) {
        return false;
    }
    match (dom_restricted, dow_restricted) {
        (true, true) => dom || dow,
        _ => dom && dow,
    }
}

fn field_matches(field: &str, value: u32, min: u32, max: u32, names: &[&str]) -> bool {
    field
        .split(',')
        .any(|part| part_matches(part, value, min, max, names))
}

fn part_matches(part: &str, value: u32, min: u32, max: u32, names: &[&str]) -> bool {
    let (base, step) = match part.split_once('/') {
        Some((base, step)) => match step.parse::<u32>() {
            Ok(s) if s >= 1 => (base, s),
            _ => return false,
        },
        None => (part, 1),
    };
    let in_step = |v: u32, lo: u32| (v - lo) % step == 0;
    match base.split_once('-') {
        Some((lo, hi)) => {
            let (lo, hi) = match (atom(lo, min, max, names), atom(hi, min, max, names)) {
                (Some(l), Some(h)) => (l, h),
                _ => return false,
            };
            value >= lo && value <= hi && in_step(value, lo)
        }
        None => {
            if base == "*" {
                in_step(value, min)
            } else {
                atom(base, min, max, names).is_some_and(|v| value == v && in_step(value, v))
            }
        }
    }
}

fn atom(value: &str, min: u32, max: u32, names: &[&str]) -> Option<u32> {
    if let Ok(n) = value.parse::<u32>() {
        return (min..=max).contains(&n).then_some(n);
    }
    names
        .iter()
        .position(|n| value.eq_ignore_ascii_case(n))
        .map(|i| i as u32 + min)
}

/// Day-of-week matcher (RV-R69). Accepts `0-7` (7 = Sunday) and
/// SUN–SAT; `raw_sunday0` is 0=Sun..6=Sat.
///
/// Ranges are expanded on the 0..=7 axis and folded with `% 7`, so an
/// upper bound of `7` (or the name `SUN` after a non-Sunday start)
/// covers Sunday at the end of the week — vixie semantics:
///   * `5-7`, `FRI-SUN` → Fri, Sat, Sun
///   * `1-7/2`          → Mon, Wed, Fri, Sun (steps count from the lower bound)
///   * `*/2`            → Sun, Tue, Thu, Sat (`*` is 0..=6 here)
///   * `0-7`            → every day
///   * `SUN-SUN`, `7-7` → Sunday only
fn field_matches_dow(field: &str, raw_sunday0: u32) -> bool {
    const NAMES: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
    field.split(',').any(|part| {
        let (base, step) = match part.split_once('/') {
            Some((b, s)) => (b, s.parse::<u32>().unwrap_or(0)),
            None => (part, 1),
        };
        if step == 0 {
            return false;
        }
        // (value on the 0..=7 axis, was a name)
        let resolve = |a: &str| -> Option<(u32, bool)> {
            match a.parse::<u32>() {
                Ok(n) if n <= 7 => Some((n, false)),
                Ok(_) => None,
                Err(_) => NAMES
                    .iter()
                    .position(|n| a.eq_ignore_ascii_case(n))
                    .map(|i| (i as u32, true)),
            }
        };
        match base.split_once('-') {
            Some((lo, hi)) => match (resolve(lo), resolve(hi)) {
                (Some((l, _)), Some((mut h, hi_is_name))) => {
                    if hi_is_name && h == 0 && l > 0 {
                        h = 7; // `FRI-SUN`
                    }
                    if l > h {
                        return false;
                    }
                    (l..=h)
                        .filter(|v| (v - l) % step == 0)
                        .any(|v| v % 7 == raw_sunday0)
                }
                _ => false,
            },
            None => {
                if base == "*" {
                    (raw_sunday0 % step) == 0
                } else {
                    resolve(base).is_some_and(|(v, _)| v % 7 == raw_sunday0)
                }
            }
        }
    })
}

// ------------------------------------------------------------------ T21

/// Upper bound on remembered streams inside [`FileTriggerLog`] — the
/// file stays bounded; oldest claimed times are evicted first. A stream
/// firing once per minute would take >11 years to age out, so eviction
/// can never reopen a live slot.
const MAX_CLAIM_STREAMS: usize = 16_384;

/// Durable per-stream claim log at `<dir>/cron-claims.json` (T21/F2).
///
/// Every `claim` persists the full map atomically (tmp + fsync +
/// rename via `vrt_securefs::atomic_write`), so a crash between
/// dispatch and response still never re-fires the minute — the claim
/// landed first. Load failures fail closed: a corrupt claim file
/// means the "which minutes already fired" record is unverifiable,
/// and guessing would risk at-least-once — the caller must refuse to
/// run cron rather than silently reset.
pub struct FileTriggerLog {
    path: std::path::PathBuf,
    inner: MemoryTriggerLog,
}

impl FileTriggerLog {
    /// Open (or create on first claim) the log under `dir`. `dir` must
    /// already exist as a `0700` state directory (the controller's
    /// state dir contract) — it is created when absent so single-node
    /// dev runs still work.
    pub fn open(dir: &std::path::Path) -> Result<Self, String> {
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|e| format!("cron state dir: {e}"))?;
        }
        let path = dir.join("cron-claims.json");
        let inner = MemoryTriggerLog::default();
        let log = Self { path, inner };
        if log.path.exists() {
            let raw =
                std::fs::read(&log.path).map_err(|e| format!("cron claims unreadable: {e}"))?;
            // Tolerate a torn final write: the file is rewritten on
            // every claim, but a crash between tmp write and rename
            // leaves only the tmp file — the final name is either the
            // previous complete map or absent. A CORRUPT final name
            // (partial JSON) therefore means real corruption → Err.
            let doc: serde_json::Value = serde_json::from_slice(&raw)
                .map_err(|_| {
                    "cron-claims.json corrupt — refusing to guess claim state                      (at-least-once risk); restore from backup or delete the file                      to reset all schedules".to_string()
                })?;
            let streams = doc
                .get("streams")
                .and_then(|s| s.as_object())
                .ok_or_else(|| "cron-claims.json: missing streams".to_string())?;
            let mut guard = log
                .inner
                .watermarks
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for (k, v) in streams {
                let Some(t) = v.as_str() else {
                    return Err("cron-claims.json: malformed stream value".into());
                };
                let parts: Vec<&str> = k.splitn(3, '\u{1f}').collect();
                if parts.len() != 3 {
                    return Err("cron-claims.json: malformed stream key".into());
                }
                guard.insert(
                    (
                        parts[0].to_string(),
                        parts[1].to_string(),
                        parts[2].to_string(),
                    ),
                    t.to_string(),
                );
            }
        }
        Ok(log)
    }

    fn persist(&self) -> Result<(), String> {
        let guard = self
            .inner
            .watermarks
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut streams = serde_json::Map::new();
        // Bound the file: drop the oldest claimed times first.
        let mut entries: Vec<(&(String, String, String), &String)> = guard.iter().collect();
        if entries.len() > MAX_CLAIM_STREAMS {
            entries.sort_by(|a, b| a.1.cmp(b.1));
            let drop = entries.len() - MAX_CLAIM_STREAMS;
            entries.drain(..drop);
        }
        for ((w, v, s), t) in entries {
            streams.insert(
                format!("{w}\u{1f}{v}\u{1f}{s}"),
                serde_json::Value::String(t.clone()),
            );
        }
        let body = serde_json::json!({ "schemaVersion": 1, "streams": streams });
        let bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
        drop(guard);
        vrt_securefs::atomic_write(&self.path, &bytes, 0o600)
            .map_err(|e| format!("cron claims persist failed: {e}"))
    }
}

impl TriggerLog for FileTriggerLog {
    fn watermark(&self, worker_id: &str, version_id: &str, schedule_id: &str) -> Option<String> {
        self.inner.watermark(worker_id, version_id, schedule_id)
    }
    fn claim(&mut self, key: &TriggerKey) {
        self.inner.claim(key);
        if let Err(e) = self.persist() {
            // Persist failure is NOT silent — the claim is only in
            // memory now. A crash could re-fire the minute. Log it
            // loudly; the next claim will retry the write (the file
            // self-heals on the next successful persist).
            tracing::error!(error = %e, "cron claim persistence failed — at-most-once degraded for this slot");
        }
    }
}

/// Production trigger transport (T21/F1): POST the typed trigger to the
/// pod's loopback control listener `POST /v1/cron/trigger` carrying the
/// `x-vrt-cron-idempotency-key` header. The host dedupes by key (a lost
/// response retried at the TCP/HTTP layer can never double-deliver).
///
/// Pod bases reuse the `[[pods]]` `readyz` loopback endpoints — every
/// base is validated `http://loopback:<port>` exactly like
/// [`crate::file_channel`], so a misconfigured remote URL fails closed
/// at construction.
pub struct HttpCronInvoker {
    pods: std::collections::BTreeMap<String, String>,
    client: reqwest::Client,
}

impl HttpCronInvoker {
    /// Build from `(pod_id, readyz_base)` pairs (the same `[[pods]]`
    /// specs the file channel validates). Any non-loopback base → Err.
    pub fn new(pods: &[(String, String)], timeout: std::time::Duration) -> Result<Self, String> {
        let mut map = std::collections::BTreeMap::new();
        for (id, base) in pods {
            let url = reqwest::Url::parse(base).map_err(|e| format!("pod {id} base: {e}"))?;
            if url.scheme() != "http" {
                return Err(format!("pod {id} base must be http (loopback)"));
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
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(std::time::Duration::from_secs(2)))
            .build()
            .map_err(|e| format!("cron client: {e}"))?;
        Ok(Self { pods: map, client })
    }
}

#[async_trait::async_trait]
impl CronInvoker for HttpCronInvoker {
    async fn trigger(&self, pod_id: &str, trigger: &CronTrigger) -> Result<(), CronError> {
        let Some(base) = self.pods.get(pod_id) else {
            return Err(CronError::Unassigned);
        };
        let url = format!("{base}/v1/cron/trigger");
        let body = serde_json::json!({
            "tenantId": trigger.tenant_id,
            "workerId": trigger.worker_id,
            "versionId": trigger.version_id,
            "scheduleId": trigger.schedule_id,
            "scheduledTime": trigger.scheduled_time,
            "cron": trigger.expression,
            "leaseEpoch": trigger.lease_epoch,
        });
        let resp = self
            .client
            .post(&url)
            .header("x-vrt-cron-idempotency-key", trigger.idempotency_key())
            .json(&body)
            .send()
            .await
            .map_err(|_| CronError::Delivery)?;
        // Any 2xx = delivered (200 delivered, 200 duplicate — the host
        // reports `duplicate:true` on replays which is still "done").
        if resp.status().is_success() {
            return Ok(());
        }
        // MIG-11: the ONLY retryable failure is a typed 503 proving the
        // host never dispatched — VRT_CRON_SWITCHING (route map empty
        // during a serialized DO switch) or VRT_CRON_WORKER_NOT_READY
        // (generation draining) — both surface BEFORE the dedupe slot
        // is consumed. Everything else (404 absent, 409 epoch, 5xx
        // upstream, transport errors in `send()` above) is ambiguous
        // or final → `Delivery`, never retried.
        let retryable = resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE
            && resp
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|j| {
                    j.pointer("/error/code")
                        .and_then(|c| c.as_str())
                        .map(str::to_string)
                })
                .is_some_and(|c| c == "VRT_CRON_SWITCHING" || c == "VRT_CRON_WORKER_NOT_READY");
        Err(if retryable {
            CronError::NotDelivered
        } else {
            CronError::Delivery
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::MemoryLeaseStore;

    fn at(iso: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(iso)
            .unwrap()
            .with_timezone(&Utc)
    }

    /// RV-R56 (verified 2026-11-15): `*`-prefixed dom/dow are unrestricted.
    #[test]
    fn dom_dow_star_prefix_is_unrestricted() {
        let count = |expr: &str, start_day: u32, days: u32| -> Vec<u32> {
            (start_day..start_day + days)
                .filter(|d| {
                    let iso = format!("2026-11-{:02}T00:00:00Z", d);
                    cron_matches(expr, at(&iso))
                })
                .collect()
        };
        // Nov 2026: Mondays = 2, 9, 16, 23, 30. `*/2` dom = odd days 1,3,..29.
        // AND semantics → Mondays that are odd days: none in 1..=30 except... 9, 23.
        assert_eq!(count("0 0 */2 * MON", 1, 30), vec![9, 23]);
        // `1-15 * MON` both restricted → OR: days 1..=15 plus every Monday.
        let or = count("0 0 1-15 * MON", 1, 30);
        assert_eq!(or.len(), 15 + 3); // 16, 23, 30 extra
        assert!(or.contains(&16) && or.contains(&30) && or.contains(&1));
        // all-star and */1 fire every day.
        assert_eq!(count("0 0 * * *", 1, 30).len(), 30);
        assert_eq!(count("0 0 */1 * *", 1, 30).len(), 30);
        // dow `*/2` with dom restricted → AND (dom=15 only if dow matches).
        assert!(count("0 0 15 * */1", 1, 30) == vec![15]);
    }

    #[test]
    fn tick_live_now_and_deadline() {
        let t0 = at("2026-11-08T12:00:00Z");
        assert_eq!(tick_live_now(t0, std::time::Duration::from_secs(31)), at("2026-11-08T12:00:31Z"));
        assert!(!tick_deadline_exceeded(std::time::Duration::from_secs(44)));
        assert!(tick_deadline_exceeded(std::time::Duration::from_secs(45)));
        assert!(TICK_DISPATCH_DEADLINE_SECS < 60);
    }

    #[test]
    fn matcher_basic() {
        assert!(cron_matches("*/15 * * * *", at("2026-11-08T00:30:00Z")));
        assert!(!cron_matches("*/15 * * * *", at("2026-11-08T00:31:00Z")));
        assert!(cron_matches("0 9 * * mon-fri", at("2026-11-09T09:00:00Z"))); // Mon
        assert!(!cron_matches("0 9 * * mon-fri", at("2026-11-08T09:00:00Z"))); // Sun
        assert!(cron_matches("0 9 * * sun", at("2026-11-08T09:00:00Z")));
        // dom-or-dow when both restricted:
        assert!(cron_matches("0 0 1 * sun", at("2026-11-08T00:00:00Z"))); // sun, not 1st
        assert!(cron_matches("0 0 1 * *", at("2026-11-08T00:00:00Z")) == false);
        assert!(cron_matches("0 0 1 1 *", at("2026-01-01T00:00:00Z")));
        assert!(!cron_matches("bad", at("2026-11-08T00:00:00Z")));
    }

    #[test]
    fn matcher_month_end_leap_and_utc_edges() {
        // Month-end: a restricted dom fires only when the month HAS
        // that day — no "nearest-day" drift (matches CF/vixie: an
        // impossible date simply never triggers).
        assert!(cron_matches("0 0 31 * *", at("2026-01-31T00:00:00Z")));
        assert!(!cron_matches("0 0 31 * *", at("2026-11-30T00:00:00Z")));
        assert!(!cron_matches("0 0 31 * *", at("2026-02-28T00:00:00Z")));
        assert!(!cron_matches("0 0 30 2 *", at("2026-02-28T00:00:00Z")));
        // Leap year: `0 0 29 2 *` fires on 2028-02-29 — a real day —
        // and never on non-leap 2026-02-28 or leap-year 2028-03-01.
        assert!(cron_matches("0 0 29 2 *", at("2028-02-29T00:00:00Z")));
        assert!(!cron_matches("0 0 29 2 *", at("2026-02-28T00:00:00Z")));
        assert!(!cron_matches("0 0 29 2 *", at("2028-03-01T00:00:00Z")));
        // UTC boundaries: all fields are UTC verbatim — 23:59→00:00
        // day rollover, Dec-31 23:59 → Jan-1 year rollover. DST does
        // not exist in UTC: no skipped/repeated wall minutes.
        assert!(cron_matches("59 23 * * *", at("2026-11-08T23:59:00Z")));
        assert!(!cron_matches("59 23 * * *", at("2026-11-09T00:00:00Z")));
        assert!(cron_matches("0 0 * * *", at("2026-11-09T00:00:00Z")));
        assert!(cron_matches("59 23 31 12 *", at("2026-12-31T23:59:00Z")));
        assert!(!cron_matches("59 23 31 12 *", at("2027-01-01T00:00:00Z")));
        assert!(cron_matches("0 0 1 1 *", at("2027-01-01T00:00:00Z")));
        // dow 0 and 7 are both Sunday; names case-insensitive.
        assert!(cron_matches("0 0 * * 0", at("2026-11-08T00:00:00Z")));
        assert!(cron_matches("0 0 * * 7", at("2026-11-08T00:00:00Z")));
        // Step at a range edge: `1-31/2` = odd days only.
        assert!(cron_matches("0 0 1-31/2 * *", at("2026-11-29T00:00:00Z")));
        assert!(!cron_matches("0 0 1-31/2 * *", at("2026-11-30T00:00:00Z")));
    }

    /// RV-R69 #4: DOW ranges whose upper bound is 7 / `SUN` expand across
    /// Sunday (vixie) instead of silently never firing.
    #[test]
    fn matcher_dow_upper_bound_seven_and_sun() {
        // 2026-11-08 is Sunday; Mon 09 .. Sat 14.
        let days = [
            "2026-11-08T00:00:00Z", // Sun
            "2026-11-09T00:00:00Z", // Mon
            "2026-11-10T00:00:00Z",
            "2026-11-11T00:00:00Z", // Wed
            "2026-11-12T00:00:00Z",
            "2026-11-13T00:00:00Z", // Fri
            "2026-11-14T00:00:00Z", // Sat
        ];
        let fired = |expr: &str| -> Vec<bool> {
            days.iter().map(|d| cron_matches(expr, at(d))).collect()
        };
        // order: Sun Mon Tue Wed Thu Fri Sat
        assert_eq!(
            fired("0 0 * * 5-7"),
            [true, false, false, false, false, true, true]
        );
        assert_eq!(fired("0 0 * * FRI-SUN"), fired("0 0 * * 5-7"));
        assert_eq!(fired("0 0 * * fri-sun"), fired("0 0 * * 5-7"));
        // Steps count from the lower bound: 1,3,5,7(=Sun).
        assert_eq!(
            fired("0 0 * * 1-7/2"),
            [true, true, false, true, false, true, false]
        );
        // `*/2` = 0,2,4,6.
        assert_eq!(
            fired("0 0 * * */2"),
            [true, false, true, false, true, false, true]
        );
        assert_eq!(fired("0 0 * * 0-7"), [true; 7]);
        assert_eq!(
            fired("0 0 * * SUN-SUN"),
            [true, false, false, false, false, false, false]
        );
        assert_eq!(
            fired("0 0 * * 7-7"),
            [true, false, false, false, false, false, false]
        );
        assert_eq!(
            fired("0 0 * * 6-7"),
            [true, false, false, false, false, false, true]
        );
    }

    /// RV-R69 #4 structural property: whatever `ComputeDesired` accepts
    /// as a cron expression must be able to fire (validate ⇒ matches at
    /// least once in a 366-day window). Deterministic pseudo-random
    /// generator over the full grammar; impossible calendar dates
    /// (`31` in 30-day months, Feb 30/31) are valid-but-never by design
    /// (vixie/CF) and are excluded from generation.
    #[test]
    fn validate_accepts_implies_cron_fires_within_a_year() {
        fn desired_with(expr: &str) -> Result<ComputeDesired, serde_json::Error> {
            let mut p = serde_json::json!({
                "schemaVersion": 1, "cellId": "cell-1", "revision": 1,
                "tenants": [{"tenantId": "t", "workers": [{
                    "workerId": "w1", "name": "w1",
                    "activeVersion": {"versionId": "v1", "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2024-01-01", "compatibilityFlags": []},
                    "bindings": [], "routes": [],
                    "crons": [{"scheduleId": "c1", "expression": "* * * * *"}],
                    "quotas": {"cpuMs": 10, "memoryMb": 32, "outboundConnections": 4, "subrequests": 10}
                }]}]
            });
            p["tenants"][0]["workers"][0]["crons"][0]["expression"] = expr.into();
            serde_json::from_value(p)
        }
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move |n: usize| -> usize {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        let minute = ["*", "0", "30", "59", "*/15", "5-10", "0,30", "1-59/7"];
        let hour = ["*", "0", "12", "23", "*/6", "9-17", "1,13", "0-23/5"];
        let dom = ["*", "1", "15", "28", "*/10", "1-7", "1,15", "1-28/3"];
        let month = ["*", "1", "6", "12", "JAN", "dec", "*/3", "JAN-MAR", "2-11/2"];
        let dow = [
            "*", "0", "1", "6", "7", "SUN", "MON", "*/2", "1-5", "MON-FRI", "5-7", "FRI-SUN",
            "1-7/2", "0-7", "SUN-SUN", "6-7", "SAT-SUN", "0,7", "3-7/3", "*/7",
        ];
        let mut accepted = 0;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3000 {
            let expr = format!(
                "{} {} {} {} {}",
                minute[next(minute.len())],
                hour[next(hour.len())],
                dom[next(dom.len())],
                month[next(month.len())],
                dow[next(dow.len())]
            );
            if !seen.insert(expr.clone()) || desired_with(&expr).is_err() {
                continue;
            }
            accepted += 1;
            let start = at("2026-01-01T00:00:00Z");
            // minute/hour and the date fields are conjunctive and
            // independent, so scan them separately (cheap in debug):
            // some (hour,minute) pair on one day, and some day of the
            // year at 00:00 under the date fields with minute/hour `0 0`.
            let f: Vec<&str> = expr.split_ascii_whitespace().collect();
            let time_expr = format!("{} {} * * *", f[0], f[1]);
            let date_expr = format!("0 0 {} {} {}", f[2], f[3], f[4]);
            let time_ok =
                (0..24 * 60).any(|m| cron_matches(&time_expr, start + Duration::minutes(m)));
            // A star-prefixed dom step (`*/10`) is ANDed with a restricted
            // dow (vixie), so e.g. `0 0 */10 12 6` fires only in years
            // where Dec 1/11/21/31 is a Saturday. Scan the 366-day window
            // first, then fall back to one full 28-year Gregorian
            // weekday/date cycle — "never fires" still means never.
            let date_ok = (0..366).any(|d| cron_matches(&date_expr, start + Duration::days(d)))
                || (366..366 * 28).any(|d| cron_matches(&date_expr, start + Duration::days(d)));
            let fires = time_ok && date_ok;
            assert!(fires, "validate accepted `{expr}` but it never fires in a 28-year cycle");
        }
        assert!(accepted > 300, "generator accepted too few: {accepted}");
    }

    #[test]
    fn matcher_rejected_syntax_never_fires() {
        // Deploy-time `valid_cron_expression` typed-rejects these;
        // the runtime matcher stays fail closed — a rejected
        // expression must evaluate to "no match" on every instant,
        // never error-as-true. Choices: CF-unsupported grammar
        // (L/W/#/?/@aliases/6-field seconds) is rejected outright
        // rather than approximated; `*/0` step is invalid (division
        // by zero); out-of-range atoms never match.
        for bad in [
            "0 0 ? * *",     // Quartz day-wildcard — unsupported
            "0 0 L * *",     // last-day-of-month — unsupported
            "0 0 LW * *",    // last weekday — unsupported
            "0 0 15W * *",   // nearest weekday — unsupported
            "0 0 * * MON#2", // nth weekday — unsupported
            "0 0 * * 5#3",   // nth weekday numeric — unsupported
            "@daily",        // macro aliases — not 5-field
            "@hourly",
            "0 0 0 * * *",     // 6-field seconds — unsupported
            "0 0 * * * 2026",  // 6-field year — unsupported
            "*/0 * * * *",     // zero step — invalid
            "0 0 32 * *",      // dom out of range
            "0 0 * 13 *",      // month out of range
            "0 24 * * *",      // hour out of range
            "60 * * * *",      // minute out of range
            "0 0 * * 8",       // dow out of range (0..7)
            "",                // empty
            "* * *",           // too few fields
            "* * * * * * * *", // too many
        ] {
            assert!(!cron_matches(bad, at("2026-11-08T00:00:00Z")), "{bad}");
            assert!(!cron_matches(bad, at("2028-02-29T23:59:00Z")), "{bad}");
        }
        // `*/0` must not panic (div-by-zero guarded in part_matches).
        assert!(!cron_matches("*/0 * * * *", at("2026-11-08T00:00:00Z")));
    }

    fn desired_with_cron() -> ComputeDesired {
        let payload = serde_json::json!({
            "schemaVersion": 1,
            "cellId": "cell-1",
            "revision": 3,
            "tenants": [{
                "tenantId": "team-a",
                "workers": [{
                    "workerId": "w1", "name": "w1",
                    "activeVersion": {
                        "versionId": "vrt_v1",
                        "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2024-01-01",
                        "compatibilityFlags": []
                    },
                    "bindings": [], "crons": [{"scheduleId": "c1", "expression": "* * * * *"}],
                    "quotas": {"cpuMs": 10, "memoryMb": 32, "outboundConnections": 4, "subrequests": 10},
                    "routes": []
                }]
            }]
        });
        serde_json::from_value(payload).unwrap()
}

    fn assignments() -> BTreeMap<String, PodAssignment> {
        [(
            "pod-0".to_string(),
            PodAssignment {
                pod_id: "pod-0".into(),
                revision: 3,
                payload_digest: "d".repeat(64),
                envelope_digest: "e".repeat(64),
                generated_at: String::new(),
                workers: vec![AssignedWorker {
                    tenant_id: "team-a".into(),
                    worker_id: "w1".into(),
                }],
            },
        )]
        .into_iter()
        .collect()
    }

    fn runner(
        lease: MemoryLeaseStore,
        log: Box<dyn TriggerLog>,
        inv: Box<dyn CronInvoker>,
    ) -> CronRunner<MemoryLeaseStore> {
        CronRunner::new(lease, "ctrl-0", Duration::minutes(3), log, inv)
    }

    #[tokio::test]
    async fn cron_dedup_and_lease_loss_and_expiry() {
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();

        // dedup
        let lease = MemoryLeaseStore::default();
        let log = std::sync::Arc::new(Mutex::new(
            BTreeMap::<(String, String, String), String>::new(),
        ));
        struct SharedLog(std::sync::Arc<Mutex<BTreeMap<(String, String, String), String>>>);
        impl TriggerLog for SharedLog {
            fn watermark(
                &self,
                worker_id: &str,
                version_id: &str,
                schedule_id: &str,
            ) -> Option<String> {
                self.0
                    .lock()
                    .unwrap()
                    .get(&(
                        worker_id.to_string(),
                        version_id.to_string(),
                        schedule_id.to_string(),
                    ))
                    .cloned()
            }
            fn claim(&mut self, key: &TriggerKey) {
                let mut g = self.0.lock().unwrap();
                let slot = g
                    .entry((
                        key.worker_id.clone(),
                        key.version_id.clone(),
                        key.schedule_id.clone(),
                    ))
                    .or_default();
                if key.scheduled_time > *slot {
                    *slot = key.scheduled_time.clone();
                }
            }
        }
        let sent = std::sync::Arc::new(Mutex::new(Vec::new()));
        struct SharedInv(std::sync::Arc<Mutex<Vec<(String, CronTrigger)>>>);
        #[async_trait::async_trait]
        impl CronInvoker for SharedInv {
            async fn trigger(&self, pod: &str, t: &CronTrigger) -> Result<(), CronError> {
                self.0.lock().unwrap().push((pod.into(), t.clone()));
                Ok(())
            }
        }
        let mut r = runner(
            lease,
            Box::new(SharedLog(log.clone())),
            Box::new(SharedInv(sent.clone())),
        );
        // Leadership taken the minute BEFORE the first due tick — the
        // takeover minute itself is a deliberate blackout (tested
        // separately below).
        r.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:33:00Z"),
        )
        .await;
        let t0 = at("2026-11-08T12:34:00Z");
        let o1 = r.tick(Some(&desired), Some(expires), &assigns, t0).await;
        assert_eq!(o1.fired.len(), 1);
        let o2 = r.tick(Some(&desired), Some(expires), &assigns, t0).await;
        assert!(
            o2.fired.is_empty(),
            "same (worker,version,minute) must dedupe"
        );
        // next minute fires again (different scheduled_time)
        let o3 = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:35:00Z"),
            )
            .await;
        assert_eq!(o3.fired.len(), 1);
        // renewal failure → not leader → nothing fires
        // (fail_next_claims on the lease — need fresh runner to inject)
        let lease2 = MemoryLeaseStore::default();
        lease2.fail_next_claims(1);
        let mut r2 = runner(
            lease2,
            Box::new(SharedLog(log.clone())),
            Box::new(SharedInv(sent.clone())),
        );
        let o4 = r2
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:36:00Z"),
            )
            .await;
        assert!(o4.not_leader && o4.fired.is_empty());
        // expired desired → no authority (leadership warmed up so the
        // assertion measures expiry, not the takeover blackout).
        let lease3 = MemoryLeaseStore::default();
        let mut r3 = runner(
            lease3,
            Box::new(SharedLog(log.clone())),
            Box::new(SharedInv(sent.clone())),
        );
        r3.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:36:00Z"),
        )
        .await;
        let o5 = r3
            .tick(
                Some(&desired),
                Some(at("2026-11-08T00:00:00Z")),
                &assigns,
                at("2026-11-08T12:37:00Z"),
            )
            .await;
        assert!(o5.fired.is_empty());
    }

    #[tokio::test]
    async fn lease_loss_mid_queue_stops_pending() {
        // Lease stolen BETWEEN claim and the in-loop held() re-check:
        // the 2nd claim succeeds normally (same tenure renewal), then
        // the underlying hold is expired — held() inside the dispatch
        // loop reports loss and the queued trigger stops.
        let lease = MemoryLeaseStore::default();
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        struct StealAfterRenewal(MemoryLeaseStore, std::sync::atomic::AtomicUsize);
        #[async_trait::async_trait]
        impl LeaseStore for StealAfterRenewal {
            async fn claim(
                &self,
                h: &str,
                ttl: Duration,
                now: DateTime<Utc>,
            ) -> Result<crate::lease::LeaseOutcome, crate::lease::LeaseError> {
                let out = self.0.claim(h, ttl, now).await;
                // Expire right after the SECOND claim: the returned
                // claim looks valid but held() immediately disagrees —
                // exactly a mid-queue steal.
                if self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                    self.0.expire_now(now);
                }
                out
            }
            fn held(&self, c: &crate::lease::LeaseClaim, now: DateTime<Utc>) -> bool {
                self.0.held(c, now)
            }
        }
        let mut r = CronRunner::new(
            StealAfterRenewal(lease, std::sync::atomic::AtomicUsize::new(0)),
            "ctrl-0",
            Duration::minutes(3),
            Box::new(MemoryTriggerLog::default()),
            Box::new(MemoryCronInvoker::default()),
        );
        // 11:59: first claim — takeover minute, blackout by design.
        let warm = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T11:59:00Z"),
            )
            .await;
        assert!(warm.post_takeover_blackout && warm.fired.is_empty());
        // 12:00: renewal same tenure → past blackout; the steal flips
        // held() → the queued trigger is skipped, not fired.
        let o = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:00:00Z"),
            )
            .await;
        assert!(!o.post_takeover_blackout);
        assert!(o.fired.is_empty() && o.skipped_lease_lost == 1);
    }

    #[tokio::test]
    async fn takeover_minute_is_a_blackout_then_normal_firing() {
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let lease = MemoryLeaseStore::default();
        let mut r = runner(
            lease,
            Box::new(MemoryTriggerLog::default()),
            Box::new(MemoryCronInvoker::default()),
        );
        // First claim at 12:34 → the 12:34 slot is deliberately never
        // fired by the new leader (at-most-once across failover).
        let o1 = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:34:00Z"),
            )
            .await;
        assert!(o1.post_takeover_blackout && o1.fired.is_empty());
        // The next canonical minute fires normally.
        let o2 = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:35:00Z"),
            )
            .await;
        assert_eq!(o2.fired.len(), 1);
        assert!(!o2.post_takeover_blackout);
        // Losing + re-winning the lease inside the SAME minute re-arms
        // the blackout — a flap cannot double-fire the takeover slot.
        let lease2 = MemoryLeaseStore::default();
        lease2.fail_next_claims(1);
        let mut r2 = runner(
            lease2,
            Box::new(MemoryTriggerLog::default()),
            Box::new(MemoryCronInvoker::default()),
        );
        let o3 = r2
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:36:00Z"),
            )
            .await;
        assert!(o3.not_leader);
        let o4 = r2
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:36:30Z"),
            )
            .await;
        assert!(o4.post_takeover_blackout && o4.fired.is_empty());
        let o5 = r2
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:37:00Z"),
            )
            .await;
        assert_eq!(o5.fired.len(), 1);
    }

    #[tokio::test]
    async fn two_schedules_same_minute_are_independent_streams() {
        // One worker, two cron entries firing in the same minute: the
        // first success must not shadow the second (the old
        // (worker,version) watermark bug — H0).
        let payload = serde_json::json!({
            "schemaVersion": 1, "cellId": "cell-1", "revision": 3,
            "tenants": [{
                "tenantId": "team-a",
                "workers": [{
                    "workerId": "w1", "name": "w1",
                    "activeVersion": {
                        "versionId": "vrt_v1",
                        "artifactDigest": "a".repeat(64),
                        "compatibilityDate": "2024-01-01",
                        "compatibilityFlags": []
                    },
                    "bindings": [],
                    "crons": [
                        {"scheduleId": "c-1", "expression": "* * * * *"},
                        {"scheduleId": "c-2", "expression": "* * * * *"}
                    ],
                    "quotas": {"cpuMs": 10, "memoryMb": 32, "outboundConnections": 4, "subrequests": 10},
                    "routes": []
                }]
            }]
        });
        let desired: ComputeDesired = serde_json::from_value(payload).unwrap();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let lease = MemoryLeaseStore::default();
        let log = std::sync::Arc::new(MemoryTriggerLog::default());
        struct LogHandle(std::sync::Arc<MemoryTriggerLog>);
        impl TriggerLog for LogHandle {
            fn watermark(&self, w: &str, v: &str, s: &str) -> Option<String> {
                self.0.watermark(w, v, s)
            }
            fn claim(&mut self, key: &TriggerKey) {
                self.0.claim_shared(key)
            }
        }
        // c-2 fails permanently; c-1 must still fire.
        struct FailOne(String);
        #[async_trait::async_trait]
        impl CronInvoker for FailOne {
            async fn trigger(&self, _pod: &str, t: &CronTrigger) -> Result<(), CronError> {
                if t.schedule_id == self.0 {
                    return Err(CronError::Delivery);
                }
                Ok(())
            }
        }
        let mut r = runner(
            lease,
            Box::new(LogHandle(log.clone())),
            Box::new(FailOne("c-2".into())),
        );
        r.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:33:00Z"),
        )
        .await;
        let o = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:34:00Z"),
            )
            .await;
        assert_eq!(o.fired.len(), 1, "c-1 fires even though c-2 failed");
        assert_eq!(o.failed.len(), 1, "c-2 recorded as failed");
        // Watermarks are per-schedule claims: BOTH streams consumed
        // their 12:34 slot — c-1 fired, c-2 claimed-then-failed
        // (at-most-once: the failed slot is NOT retryable).
        assert_eq!(
            log.watermark("w1", "vrt_v1", "c-1").as_deref(),
            Some("2026-11-08T12:34:00Z")
        );
        assert_eq!(
            log.watermark("w1", "vrt_v1", "c-2").as_deref(),
            Some("2026-11-08T12:34:00Z")
        );
        // Replay: both deduped — a claimed-but-undelivered minute is
        // consumed, never retried, never backfilled (T21/F2).
        let o2 = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:34:30Z"),
            )
            .await;
        assert!(o2.fired.is_empty() && o2.failed.is_empty());
    }

    #[tokio::test]
    async fn lapsed_reacquire_re_arms_blackout() {
        // Same holder whose lease LAPSES then re-wins is a NEW tenure —
        // epoch changed, so the re-takeover minute is blacked out too.
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let lease = MemoryLeaseStore::default();
        let mut r = runner(
            lease.clone(),
            Box::new(MemoryTriggerLog::default()),
            Box::new(MemoryCronInvoker::default()),
        );
        // Takeover at 12:34 (blackout) → fires 12:35.
        r.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:34:00Z"),
        )
        .await;
        let o = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:35:00Z"),
            )
            .await;
        assert_eq!(o.fired.len(), 1);
        // Lease lapses and the SAME holder re-wins inside 12:36 → new
        // epoch → the 12:36 slot is blacked out, 12:37 fires again.
        lease.expire_now(at("2026-11-08T12:35:30Z"));
        let o2 = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:36:00Z"),
            )
            .await;
        assert!(o2.post_takeover_blackout && o2.fired.is_empty());
        let o3 = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:37:00Z"),
            )
            .await;
        assert_eq!(o3.fired.len(), 1);
    }

    #[test]
    fn trigger_idempotency_key_is_stable_and_complete() {
        let key = TriggerKey {
            worker_id: "w-1".into(),
            version_id: "vrt_v1".into(),
            schedule_id: "c1".into(),
            scheduled_time: "2026-11-08T12:34:00Z".into(),
        };
        assert_eq!(key.idempotency_key(), "w-1:vrt_v1:c1:2026-11-08T12:34:00Z");
        let trigger = CronTrigger {
            tenant_id: "team-a".into(),
            worker_id: "w-1".into(),
            version_id: "vrt_v1".into(),
            schedule_id: "c1".into(),
            scheduled_time: "2026-11-08T12:34:00Z".into(),
            expression: "* * * * *".into(),
            lease_epoch: 1,
        };
        assert_eq!(trigger.idempotency_key(), key.idempotency_key());
    }

    #[test]
    fn claim_ignores_regressions() {
        let mut log = MemoryTriggerLog::default();
        let later = TriggerKey {
            worker_id: "w".into(),
            version_id: "v".into(),
            schedule_id: "c".into(),
            scheduled_time: "2026-11-08T12:35:00Z".into(),
        };
        let earlier = TriggerKey {
            scheduled_time: "2026-11-08T12:34:00Z".into(),
            ..later.clone()
        };
        log.claim(&later);
        log.claim(&earlier); // late/duplicated claim must not regress
        assert_eq!(
            log.watermark("w", "v", "c").as_deref(),
            Some("2026-11-08T12:35:00Z")
        );
        assert!(log.watermark("w", "other-v", "c").is_none());
        assert!(log.watermark("w", "v", "other-c").is_none());
    }
    // ---------------------------------------------------------- T21/F2
    // Crash-window proofs for the claim-before-dispatch contract.

    /// A runner whose claim persists then "crashes": we drop the whole
    /// runner and rebuild from the same state dir — exactly a process
    /// restart. `FileTriggerLog` must rehydrate claims.
    #[tokio::test]
    async fn file_log_claim_survives_restart_and_blocks_redelivery() {
        let dir = std::env::temp_dir().join(format!(
            "vrt-cronlog-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let t_fire = at("2026-11-08T12:40:00Z");

        // Process 1: warm leadership, then fire 12:40.
        {
            let log = FileTriggerLog::open(&dir).unwrap();
            let inv = std::sync::Arc::new(MemoryCronInvoker::default());
            struct Inv(std::sync::Arc<MemoryCronInvoker>);
            #[async_trait::async_trait]
            impl CronInvoker for Inv {
                async fn trigger(&self, pod: &str, t: &CronTrigger) -> Result<(), CronError> {
                    self.0.trigger(pod, t).await
                }
            }
            let mut r = runner(
                MemoryLeaseStore::default(),
                Box::new(log),
                Box::new(Inv(inv.clone())),
            );
            r.tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:39:00Z"),
            )
            .await;
            let o = r
                .tick(Some(&desired), Some(expires), &assigns, t_fire)
                .await;
            assert_eq!(o.fired.len(), 1);
            assert_eq!(inv.sent.lock().unwrap().len(), 1);
        } // "crash": runner + in-memory state dropped; file persists.

        // Process 2: fresh runner over the same dir. Same minute tick
        // must NOT re-deliver even though the new process has an empty
        // memory — the durable claim blocks it.
        {
            let log = FileTriggerLog::open(&dir).unwrap();
            let inv = std::sync::Arc::new(MemoryCronInvoker::default());
            struct Inv(std::sync::Arc<MemoryCronInvoker>);
            #[async_trait::async_trait]
            impl CronInvoker for Inv {
                async fn trigger(&self, pod: &str, t: &CronTrigger) -> Result<(), CronError> {
                    self.0.trigger(pod, t).await
                }
            }
            let mut r = runner(
                MemoryLeaseStore::default(),
                Box::new(log),
                Box::new(Inv(inv.clone())),
            );
            // New leader takeover at 12:40 = blackout, but 12:41 would
            // dedupe anyway via watermark; the KEY assertion is replay
            // of 12:40 never reaches the invoker. To bypass blackout
            // masking, claim leadership one minute earlier first.
            r.tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:39:30Z"),
            )
            .await;
            let o = r
                .tick(Some(&desired), Some(expires), &assigns, t_fire)
                .await;
            assert!(
                o.fired.is_empty(),
                "durable claim must block re-delivery across restart"
            );
            assert!(o.failed.is_empty());
            assert_eq!(inv.sent.lock().unwrap().len(), 0);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Crash AFTER the durable claim but BEFORE dispatch: the minute is
    /// consumed and never retried (at-most-once, no backfill).
    #[tokio::test]
    async fn claim_then_crash_never_delivers_or_retries() {
        let dir = std::env::temp_dir().join(format!(
            "vrt-cronlog2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        // Invoker that "crashes" the process before it can respond:
        // delivery never happens; the claim already landed on disk.
        struct CrashBeforeDeliver;
        #[async_trait::async_trait]
        impl CronInvoker for CrashBeforeDeliver {
            async fn trigger(&self, _pod: &str, _t: &CronTrigger) -> Result<(), CronError> {
                Err(CronError::Delivery)
            }
        }
        {
            let mut r = runner(
                MemoryLeaseStore::default(),
                Box::new(FileTriggerLog::open(&dir).unwrap()),
                Box::new(CrashBeforeDeliver),
            );
            r.tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:39:00Z"),
            )
            .await;
            let o = r
                .tick(
                    Some(&desired),
                    Some(expires),
                    &assigns,
                    at("2026-11-08T12:40:00Z"),
                )
                .await;
            assert_eq!(o.failed.len(), 1); // claimed, delivery failed
        } // crash
        {
            let log = FileTriggerLog::open(&dir).unwrap();
            // Claim survived: watermark already at 12:40.
            assert_eq!(
                log.watermark("w1", "vrt_v1", "c1").as_deref(),
                Some("2026-11-08T12:40:00Z")
            );
            let inv = std::sync::Arc::new(MemoryCronInvoker::default());
            struct Inv(std::sync::Arc<MemoryCronInvoker>);
            #[async_trait::async_trait]
            impl CronInvoker for Inv {
                async fn trigger(&self, pod: &str, t: &CronTrigger) -> Result<(), CronError> {
                    self.0.trigger(pod, t).await
                }
            }
            let mut r = runner(
                MemoryLeaseStore::default(),
                Box::new(log),
                Box::new(Inv(inv.clone())),
            );
            r.tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:40:30Z"),
            )
            .await;
            // 12:41 fires fresh (failed 12:40 does NOT retry), and the
            // new process's invoker sees exactly ONE delivery.
            let o = r
                .tick(
                    Some(&desired),
                    Some(expires),
                    &assigns,
                    at("2026-11-08T12:41:00Z"),
                )
                .await;
            assert_eq!(o.fired.len(), 1);
            assert_eq!(inv.sent.lock().unwrap().len(), 1);
            assert_eq!(
                inv.sent.lock().unwrap()[0].1.scheduled_time,
                "2026-11-08T12:41:00Z"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Corrupt claim file → open fails closed (at-least-once risk must
    /// never be silently reset).
    #[test]
    fn file_log_corrupt_claims_fails_closed() {
        let dir = std::env::temp_dir().join(format!(
            "vrt-cronlog3-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("cron-claims.json"), b"{not-json").unwrap();
        assert!(FileTriggerLog::open(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// HttpCronInvoker rejects non-loopback bases at construction.
    #[test]
    fn http_invoker_rejects_non_loopback_base() {
        assert!(HttpCronInvoker::new(
            &[("p".to_string(), "http://169.254.169.254:80".to_string())],
            std::time::Duration::from_secs(1),
        )
        .is_err());
        assert!(HttpCronInvoker::new(
            &[("p".to_string(), "https://127.0.0.1:1".to_string())],
            std::time::Duration::from_secs(1),
        )
        .is_err());
        assert!(HttpCronInvoker::new(
            &[("p".to_string(), "http://localhost:0".to_string())],
            std::time::Duration::from_secs(1),
        )
        .is_ok());
    }

    // ---- MIG-11: confirmed-undelivered switching retries ----

    /// MIG-11: scripted invoker — pops one scripted result per call,
    /// counting invocations.
    struct ScriptInv {
        script: std::sync::Mutex<std::collections::VecDeque<Result<(), CronError>>>,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl CronInvoker for ScriptInv {
        async fn trigger(&self, _pod: &str, _t: &CronTrigger) -> Result<(), CronError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.script
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()))
        }
    }

    fn script_inv(
        results: Vec<Result<(), CronError>>,
    ) -> (Box<ScriptInv>, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Box::new(ScriptInv {
                script: std::sync::Mutex::new(results.into()),
                calls: calls.clone(),
            }),
            calls,
        )
    }

    /// MIG-11: a confirmed-undelivered 503 (VRT_CRON_SWITCHING /
    /// VRT_CRON_WORKER_NOT_READY → `CronError::NotDelivered`) retries
    /// inside the minute — first attempt refused, second delivers.
    /// One `fired`, zero `failed`: at-most-once preserved because the
    /// host never consumed the idempotency key on a SWITCHING reject.
    #[tokio::test]
    async fn not_delivered_retries_within_tick_and_succeeds() {
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let (inv, calls) = script_inv(vec![Err(CronError::NotDelivered), Ok(())]);
        let mut r = runner(
            MemoryLeaseStore::default(),
            Box::new(MemoryTriggerLog::default()),
            inv,
        );
        r.set_retry_backoff(vec![std::time::Duration::from_millis(1)]);
        // Takeover warmup (blackout minute), then the due tick.
        r.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:33:00Z"),
        )
        .await;
        let o = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:34:00Z"),
            )
            .await;
        assert_eq!(o.fired.len(), 1, "switching retry delivered");
        assert!(o.failed.is_empty());
        assert_eq!(o.retried_switching, 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// MIG-11: an AMBIGUOUS failure (transport/timeout →
    /// `CronError::Delivery`) is NEVER retried — the event may have
    /// landed. Exactly one call, one failure; the claimed minute stands.
    #[tokio::test]
    async fn ambiguous_delivery_failure_is_never_retried() {
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let (inv, calls) = script_inv(vec![Err(CronError::Delivery)]);
        let mut r = runner(
            MemoryLeaseStore::default(),
            Box::new(MemoryTriggerLog::default()),
            inv,
        );
        r.set_retry_backoff(vec![
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
        ]);
        r.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:33:00Z"),
        )
        .await;
        let o = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:34:00Z"),
            )
            .await;
        assert_eq!(o.failed.len(), 1);
        assert!(o.fired.is_empty());
        assert_eq!(o.retried_switching, 0);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "ambiguous failure must not retry — delivery may have landed"
        );
    }

    /// MIG-11: a switching window outliving the bounded schedule stops
    /// after `1 + backoff.len()` calls — the trigger is `failed` (the
    /// minute is claimed, never backfilled), never an unbounded retry.
    #[tokio::test]
    async fn not_delivered_retries_are_bounded() {
        let desired = desired_with_cron();
        let expires = at("2026-11-09T00:00:00Z");
        let assigns = assignments();
        let (inv, calls) = script_inv(vec![Err(CronError::NotDelivered); 16]);
        let mut r = runner(
            MemoryLeaseStore::default(),
            Box::new(MemoryTriggerLog::default()),
            inv,
        );
        r.set_retry_backoff(vec![
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(1),
        ]);
        r.tick(
            Some(&desired),
            Some(expires),
            &assigns,
            at("2026-11-08T12:33:00Z"),
        )
        .await;
        let o = r
            .tick(
                Some(&desired),
                Some(expires),
                &assigns,
                at("2026-11-08T12:34:00Z"),
            )
            .await;
        assert_eq!(o.failed.len(), 1);
        assert!(o.fired.is_empty());
        assert_eq!(o.retried_switching, 3);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            4,
            "1 attempt + 3 bounded retries"
        );
    }

    /// MIG-11: `HttpCronInvoker` maps ONLY a typed 503 carrying
    /// VRT_CRON_SWITCHING / VRT_CRON_WORKER_NOT_READY to
    /// `NotDelivered`; 404 absence, other 503 codes and malformed
    /// bodies stay ambiguous `Delivery` (never retried).
    #[tokio::test]
    async fn http_invoker_switching_maps_to_not_delivered() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        async fn serve_once(status_line: &str, body: &str) -> String {
            let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = lis.local_addr().unwrap();
            let resp = format!(
                "HTTP/1.1 {status_line}
content-type: application/json
content-length: {}

{body}",
                body.len()
            );
            tokio::spawn(async move {
                let (mut s, _) = lis.accept().await.unwrap();
                let mut buf = [0u8; 8192];
                let _ = s.read(&mut buf).await;
                let _ = s.write_all(resp.as_bytes()).await;
            });
            format!("http://{addr}")
        }
        let trig = CronTrigger {
            tenant_id: "t".into(),
            worker_id: "w1".into(),
            version_id: "v1".into(),
            schedule_id: "s1".into(),
            scheduled_time: "2026-01-01T00:00:00Z".into(),
            expression: "* * * * *".into(),
            lease_epoch: 1,
        };
        // 503 + VRT_CRON_SWITCHING → NotDelivered (retry-safe)
        let base = serve_once(
            "503 Service Unavailable",
            r#"{"delivered":false,"error":{"code":"VRT_CRON_SWITCHING","message":"switching"}}"#,
        )
        .await;
        let inv = HttpCronInvoker::new(
            &[("p".to_string(), base)],
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            inv.trigger("p", &trig).await.unwrap_err(),
            CronError::NotDelivered
        );
        // 503 + VRT_CRON_WORKER_NOT_READY → NotDelivered
        let base = serve_once(
            "503 Service Unavailable",
            r#"{"delivered":false,"error":{"code":"VRT_CRON_WORKER_NOT_READY","message":"draining"}}"#,
        )
        .await;
        let inv = HttpCronInvoker::new(
            &[("p".to_string(), base)],
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            inv.trigger("p", &trig).await.unwrap_err(),
            CronError::NotDelivered
        );
        // 404 genuine absence → Delivery (never retried)
        let base = serve_once(
            "404 Not Found",
            r#"{"delivered":false,"error":{"code":"VRT_CRON_UNKNOWN_WORKER","message":"absent"}}"#,
        )
        .await;
        let inv = HttpCronInvoker::new(
            &[("p".to_string(), base)],
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            inv.trigger("p", &trig).await.unwrap_err(),
            CronError::Delivery
        );
        // 503 with an unrelated code → Delivery
        let base = serve_once(
            "503 Service Unavailable",
            r#"{"delivered":false,"error":{"code":"VRT_CRON_WHATEVER","message":"x"}}"#,
        )
        .await;
        let inv = HttpCronInvoker::new(
            &[("p".to_string(), base)],
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            inv.trigger("p", &trig).await.unwrap_err(),
            CronError::Delivery
        );
        // 503 with a malformed body → Delivery (cannot prove
        // non-delivery → ambiguous → never retried)
        let base = serve_once("503 Service Unavailable", "not-json").await;
        let inv = HttpCronInvoker::new(
            &[("p".to_string(), base)],
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            inv.trigger("p", &trig).await.unwrap_err(),
            CronError::Delivery
        );
    }
}
