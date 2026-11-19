//! Per-tenant supervisor actor.
//!
//! One actor task owns the tenant lifecycle (see [`crate::state`]):
//! launching generations, watching exits, bounded restarts with a
//! failure-window circuit breaker, resource-limit graceful restarts,
//! version switching without a service gap, and SIGTERM-driven drains.
//!
//! Communication is one-way: `SupervisorCmd` into the actor plus shared
//! read state for the ingress fast path ([`TenantRuntime`]) — `state`
//! (atomic), `routing` (`ArcSwap<BTreeMap>` worker→slot, swapped
//! atomically on generation install), per-slot in-flight counters, and
//! `drain_notify` so drain completions wake the actor without polling.

use crate::backoff::{FailureWindow, RestartBackoff};
use crate::control::{ControlEventParser, JsonLineControlParser};
use crate::endpoint::WorkerEndpoint;
use crate::launch::{signal_process_group, terminate_child, LaunchError, WorkerdLauncher};
use crate::monitor::{over_limit, ResourceSampler};
use crate::ready::{launch_until_ready, ReadyError};
use crate::spec::TenantLaunchSpec;
use crate::state::{TenantState, Transition};
use arc_swap::ArcSwap;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

/// Tunables for the supervisor actor.
#[derive(Clone, Debug)]
pub struct SupervisorParams {
    /// Deadline for a generation to report all worker sockets (default 10 s).
    pub ready_timeout: Duration,
    /// Per-socket connect-probe slice of the ready deadline (default 2 s).
    pub probe_timeout: Duration,
    /// SIGTERM→SIGKILL escalation grace (default 5 s).
    pub kill_grace: Duration,
    /// Restart backoff bounds `(base, max)` (default 100 ms–5 s).
    pub backoff: (Duration, Duration),
    /// Rolling failure window (default 60 s).
    pub fail_window: Duration,
    /// Failures inside the window that open the circuit (default 5).
    pub fail_threshold: usize,
    /// Circuit cooldown before the half-open attempt (default 30 s).
    pub circuit_cooldown: Duration,
    /// In-flight drain cap per generation (default 30 s).
    pub drain_timeout: Duration,
    /// MIG-12: bound on the post-idle waitUntil wait — once a draining
    /// generation's HTTP in-flight reaches zero, SIGTERM is deferred
    /// while its aggregate probe reports pending `ctx.waitUntil`
    /// work, up to this grace (default 30 s, the CF ceiling). `0`
    /// disables the wait entirely.
    pub wait_until_grace: Duration,
    /// MIG-12-r2: the same bound for generations draining on the
    /// serialized-DO-switch path — their waitUntil wait extends the
    /// switch-hold window directly (the new generation cannot launch
    /// until the old one exits), so it is capped separately and
    /// smaller (default 2 s, validated `< ingress.switchHoldMs`).
    /// Effective grace for a do_switch generation is
    /// `min(wait_until_grace, do_switch_wait_until_grace)`; `0`
    /// disables the wait there (idle → same-turn SIGTERM).
    pub do_switch_wait_until_grace: Duration,
    /// Resource sampling interval (default 5 s).
    pub monitor_interval: Duration,
    /// T13/M4: consecutive switch-launch failures allowed before the
    /// tenant latches `SwitchFailed` and retries STOP (default 5). The
    /// old generation keeps serving; a new Apply re-arms.
    pub max_switch_failures: u32,
    /// MIG-8: bound on ONE `workerd test` config preflight — covers
    /// global-admission queueing AND the child itself (default 15 s,
    /// matching the shared classifier's historical constant).
    pub preflight_timeout: Duration,
    /// MIG-13-r2: cap on the WHOLE prewarm episode — a warm
    /// generation (or an in-flight prewarm spawn) that never reaches
    /// install past this bound is killed and the switch falls back to
    /// the classic serialized path (`prewarm_total{outcome="expired"}`).
    /// MIG-13-r3: sized `max(2 * ingress.switchHoldMs, drain_timeout +
    /// do_switch_wait_until_grace + kill_grace + 5 s margin)` — the
    /// floor lives in `SupervisorConfig::params()`, the hold-based
    /// raise in `bin/vrt-host.rs` — and `on_tick` never fires it while
    /// the switch gate is open (the armed warm generation is
    /// load-bearing there; the drain/swap bounds own the episode).
    pub prewarm_ttl: Duration,
    /// MIG-17: bounded per-class DO placement pipeline (acquire →
    /// handoff → restore → claim → record fan-out + per-op/total
    /// budgets). See [`crate::do_switch_pipeline::PipeCfg`].
    pub do_pipeline: crate::do_switch_pipeline::PipeCfg,
    /// Tenant-level scale-to-zero (host-local, NOT part of the signed
    /// snapshot / desired spec). `None` (default) = feature OFF — the
    /// supervisor behaves byte-for-byte as before.
    pub scale_to_zero: Option<crate::hibernate::ScaleToZero>,
}

impl Default for SupervisorParams {
    fn default() -> Self {
        Self {
            ready_timeout: Duration::from_secs(10),
            probe_timeout: Duration::from_secs(2),
            kill_grace: Duration::from_secs(5),
            backoff: (Duration::from_millis(100), Duration::from_secs(5)),
            fail_window: Duration::from_secs(60),
            fail_threshold: 5,
            circuit_cooldown: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
            wait_until_grace: Duration::from_secs(30),
            do_switch_wait_until_grace: Duration::from_secs(2),
            monitor_interval: Duration::from_secs(5),
            max_switch_failures: 5,
            preflight_timeout: Duration::from_secs(15),
            prewarm_ttl: Duration::from_secs(20),
            do_pipeline: crate::do_switch_pipeline::PipeCfg::default(),
            scale_to_zero: None,
        }
    }
}

/// One worker's routable slot inside a generation. Requests hold an
/// [`InFlightGuard`] so a draining generation knows when its last request
/// finished. `closed` is set when the generation starts draining so new
/// acquisitions fail fast instead of landing on a dying process.
pub struct EndpointSlot {
    endpoint: WorkerEndpoint,
    in_flight: AtomicUsize,
    closed: AtomicBool,
    drain_notify: Arc<Notify>,
}

impl EndpointSlot {
    /// The endpoint this slot connects to.
    pub fn endpoint(&self) -> &WorkerEndpoint {
        &self.endpoint
    }

    /// Acquire an in-flight slot; `None` once the generation is draining.
    /// Released on guard drop (also on client disconnect — the response
    /// body owns the guard).
    pub fn acquire(self: &Arc<Self>) -> Option<InFlightGuard> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.closed.load(Ordering::Acquire) {
            self.release();
            return None;
        }
        Some(InFlightGuard { slot: self.clone() })
    }

    fn release(&self) {
        if self.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.drain_notify.notify_waiters();
        }
    }

    /// Whether the slot still accepts new requests (T14 readyz).
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Stop new acquisitions (drain begins).
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Scale-to-zero only: undo a `close()` when a hibernation attempt
    /// is abandoned before commit (the generation is still alive and
    /// its routes were never withdrawn). Never used for a slot of a
    /// generation that entered draining.
    pub(crate) fn reopen(&self) {
        self.closed.store(false, Ordering::Release);
    }

    /// Requests currently holding this slot.
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }
}

/// W4-r2: ingress request-hold gate for the serialized DO switch
/// window. `supervisor` empties `routing` while the old generation
/// drains — without a gate every request in that (~seconds) window
/// 503s, INCLUDING workers with no DO at all. While `active`, the
/// ingress holds requests under a bounded count/bytes/budget and
/// forwards them once `end_switch_hold` lands the new routes.
/// Cleared on install AND on every path that abandons the switch —
/// a stuck-true gate would hold requests until their own budget
/// instead of failing fast.
#[derive(Default)]
pub struct SwitchGate {
    active: AtomicBool,
    notify: Notify,
    /// In-flight held requests — per-tenant bound enforced by ingress.
    held_reqs: AtomicUsize,
    /// Sum of declared Content-Length across held requests.
    held_bytes: AtomicUsize,
    /// Test seam (`#[doc(hidden)]`, MIG-11): fires inside the shared
    /// hold loop AFTER the gate was found active and the permit
    /// acquired, but BEFORE the notified wait — the exact slice where
    /// a lost `end()` used to cost the whole budget. Lives on the
    /// gate (not IngressState) so every held entry point — ingress,
    /// cron trigger, queue dispatch, wfops — exercises the same wake
    /// ordering. Never set outside tests.
    #[doc(hidden)]
    pub pre_wait_hook: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl SwitchGate {
    /// Whether the serialized-switch window is open.
    pub fn active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
    /// Wake reservation for the window closing (install or abandon).
    /// W4-r3: `notify_waiters` stores NO permit — the caller MUST
    /// `enable()` the returned [`Notified`], THEN re-check `active()`,
    /// and only then await it. Enabling first makes an `end()` that
    /// lands between the `endpoint_slot` miss and the wait reachable
    /// to us — otherwise the request would sleep out its whole budget.
    pub fn cleared(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
    /// Try to account one held request; `false` when a bound is hit.
    /// The caller MUST release with [`Self::unhold`] — normally via
    /// [`HoldPermit`]'s Drop so a cancelled request future cannot
    /// leak the slot (W4-r3 P0-1). Both ceilings CAS — a
    /// load-then-add pair can overshoot under concurrent holds
    /// (W4-r3 P0-3).
    pub fn try_hold(&self, bytes: usize, max_reqs: usize, max_bytes: usize) -> bool {
        if self
            .held_reqs
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < max_reqs).then_some(n + 1)
            })
            .is_err()
        {
            return false;
        }
        if self
            .held_bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |b| {
                b.checked_add(bytes).filter(|v| *v <= max_bytes)
            })
            .is_err()
        {
            self.held_reqs.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        true
    }
    /// Release a held-request accounting slot.
    fn unhold(&self, bytes: usize) {
        self.held_reqs.fetch_sub(1, Ordering::SeqCst);
        self.held_bytes.fetch_sub(bytes, Ordering::SeqCst);
    }
    /// Held-request count (metrics gauge).
    pub fn held(&self) -> usize {
        self.held_reqs.load(Ordering::Acquire)
    }
    #[doc(hidden)] // tests drive the window without a full supervisor
    pub fn begin(&self) {
        self.active.store(true, Ordering::Release);
    }
    #[doc(hidden)]
    pub fn end(&self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }
}

/// MIG-11: why a bounded serialized-switch hold did NOT wait out the
/// window — the routed entry points map each variant onto a typed
/// 503 (ingress `worker_unavailable`; control-plane `*_SWITCHING`
/// + Retry-After).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchHoldReject {
    /// Per-tenant hold ceilings (count/bytes) were exhausted.
    Capacity,
    /// The wait budget expired with the switch window still open.
    Timeout,
}

/// MIG-11-r2: outcome of [`TenantRuntime::resolve_through_switch`]
/// once the switch window is over — the typed split between "the
/// target genuinely does not exist on the serving generation" (404)
/// and "the tenant is deployed but no generation is serving right
/// now" (retryable 503). Review found the pre-fix `Ok(None)` folded
/// both into the caller's 404 path: after a failed serialized-switch
/// launch (gate `end()` + empty route map + state still `Ready`)
/// every cron trigger answered `VRT_CRON_UNKNOWN_WORKER`, which the
/// controller treats as non-retryable — silently losing that
/// minute's trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchResolve<T> {
    /// Route lookup hit — a live slot (or one landed during the wait).
    Hit(T),
    /// A generation IS serving but its route map does not declare the
    /// target — genuine absence, the caller's normal 404 path.
    NotDeclared,
    /// The tenant is deployed but NO generation is serving right now
    /// (launch failed / crashed / serialized-switch gap / drain) —
    /// retryable not-ready, NEVER a 404.
    NoServing,
}

/// MIG-11: bounded serialized-switch hold ceilings shared by every
/// routed entry point — mirrors `[ingress]` `switchHold*` config so
/// the control-plane dispatches (cron/queue/wfops) wait under the
/// same per-tenant bounds as ingress requests.
#[derive(Clone, Copy, Debug)]
pub struct SwitchHoldParams {
    /// Per-waiter hold budget (default 10 s — the measured
    /// drain+launch+ready gap with margin).
    pub budget: Duration,
    /// Per-tenant bound on simultaneously held waiters.
    pub max_reqs: usize,
    /// Per-tenant bound on declared bytes held.
    pub max_bytes: usize,
}

impl Default for SwitchHoldParams {
    fn default() -> Self {
        Self {
            budget: Duration::from_secs(10),
            max_reqs: 256,
            max_bytes: 32 * 1024 * 1024,
        }
    }
}

/// W4-r3 P0-1: RAII lease on one held-request accounting slot. A
/// request future can be dropped at ANY await point (client
/// disconnect, hyper task abort, caller timeout) — manual `unhold`
/// branches at each exit inevitably leak on the cancellation paths
/// nobody wrote. `Drop` returns the slot and refreshes the tenant
/// gauge unconditionally; the only way to leak is `mem::forget`
/// (never used).
pub struct HoldPermit {
    runtime: Arc<TenantRuntime>,
    /// Bytes this permit charged against `held_bytes` (declared
    /// Content-Length — chunked bodies count 0; the request body is
    /// never read while held so memory does not grow with the body —
    /// TCP backpressure bounds the socket buffer).
    bytes: usize,
}

impl Drop for HoldPermit {
    fn drop(&mut self) {
        self.runtime.switch_gate.unhold(self.bytes);
        crate::metrics::registry().set(
            "vrt_host_switch_held_inflight",
            &[("tenant", self.runtime.tenant_id())],
            self.runtime.switch_gate.held() as f64,
        );
    }
}

/// RAII in-flight counter; notifies the supervisor when a generation's
/// count reaches zero.
pub struct InFlightGuard {
    slot: Arc<EndpointSlot>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.slot.release();
    }
}

/// Per-tenant counters published for diagnostics and tests.
#[derive(Debug, Default)]
pub struct TenantStats {
    /// Successful ready installs.
    pub readies: AtomicU64,
    /// Launch/ready failures.
    pub launch_fails: AtomicU64,
    /// Ready-deadline failures specifically.
    pub ready_timeouts: AtomicU64,
    /// Unexpected exits of the serving generation.
    pub crashes: AtomicU64,
    /// Circuit trips.
    pub circuit_opens: AtomicU64,
    /// Half-open attempts started.
    pub half_open_attempts: AtomicU64,
    /// Successful version switches.
    pub switch_ok: AtomicU64,
    /// Failed version switches (old generation kept serving).
    pub switch_fail: AtomicU64,
    /// Switches that exhausted the retry cap and latched
    /// `SwitchFailed` (T13/M4).
    pub switch_latched: AtomicU64,
    /// Forced kills after drain deadline (`drain_forced_total`).
    pub drain_forced: AtomicU64,
    /// Restarts triggered by resource-limit violations.
    pub over_limit_restarts: AtomicU64,
    /// Last sampled RSS (bytes), 0 when unknown.
    pub last_rss_bytes: AtomicU64,
    /// Last sampled fd count, 0 when unknown.
    pub last_fds: AtomicU64,
    /// PT1: process groups SIGKILLed by the runaway watchdog.
    pub runaway_kills: AtomicU64,
    /// PT1-r2: heartbeat probes actually spawned (CPU-gated — a cold
    /// tenant costs zero probes).
    pub runaway_probes: AtomicU64,
    /// PT1-r2: watchdog kills that did not converge (no exit event)
    /// past the stuck deadline — each re-evaluation counts once.
    pub runaway_kill_stuck: AtomicU64,
    /// PT1: heartbeat probe failures observed by the watchdog.
    pub runaway_heartbeat_failures: AtomicU64,
    /// PT1: monitor ticks where `cpu_seconds` was unreadable —
    /// signal 1 unprovable, never counted as zero.
    pub runaway_cpu_unavailable: AtomicU64,
    /// PT1: last verdict code (`RunawayVerdict::code`).
    pub runaway_last_verdict: AtomicU64,
    /// Scale-to-zero: hibernations committed (generation drained).
    pub hibernations: AtomicU64,
    /// Scale-to-zero: attempts abandoned before commit (traffic,
    /// timeout, pending waitUntil, blocker).
    pub hibernate_aborts: AtomicU64,
    /// Scale-to-zero: idle evaluations a blocker refused.
    pub hibernate_blocked: AtomicU64,
    /// Scale-to-zero: wakes that installed a generation.
    pub wakes: AtomicU64,
    /// Scale-to-zero: wakes that failed (launch, timeout, capacity,
    /// circuit).
    pub wake_failures: AtomicU64,
    /// Scale-to-zero: duration of the last successful wake (ms).
    pub last_wake_ms: AtomicU64,
    /// Scale-to-zero: unix ms the tenant went to sleep (0 = awake).
    pub hibernated_at_ms: AtomicU64,
}

impl std::fmt::Debug for TenantRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantRuntime")
            .field("tenant_id", &self.tenant_id)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Shared read-side tenant state for the ingress and the converger.
pub struct TenantRuntime {
    tenant_id: String,
    state: AtomicU8,
    routing: ArcSwap<BTreeMap<String, Arc<EndpointSlot>>>,
    /// T37b: `workerId → sock-qconsumer-<id>` slot map (empty for
    /// workers without `queueConsumers`).
    qconsumer_routing: ArcSwap<BTreeMap<String, Arc<EndpointSlot>>>,
    /// W3: `workerId → sock-wfops-<id>` slots (mirrors qconsumer_routing).
    wfops_routing: ArcSwap<BTreeMap<String, Arc<EndpointSlot>>>,
    /// W3-r2: `workerId → workflowName → facade token` of the serving
    /// generation — arc-swapped with `wfops_routing` on install.
    wfop_token_routing: ArcSwap<BTreeMap<String, BTreeMap<String, String>>>,
    /// CR1: `workerId → sock-cron-<id>` slots (mirrors qconsumer_routing).
    cron_routing: ArcSwap<BTreeMap<String, Arc<EndpointSlot>>>,
    drain_notify: Arc<Notify>,
    stats: TenantStats,
    /// `config_digest` of the spec most recently enqueued by the
    /// converger — what this tenant is EXPECTED to serve under the
    /// adopted desired (T15/R2). `None` until the first Apply.
    expected_digest: arc_swap::ArcSwapOption<String>,
    /// `config_digest` of the generation currently installed/active —
    /// what this tenant is actually serving (T15/R2).
    serving_digest: arc_swap::ArcSwapOption<String>,
    /// MIG-8: this tenant INSTALLED at least one generation since the
    /// runtime was built — "has served" vs "is serving now". Pod
    /// readiness gates only on tenants that never served: a served
    /// tenant mid-switch/drain is a tenant-level gap (its own
    /// requests 503 `worker_unavailable`), never a pod-level one.
    ever_served: AtomicBool,
    /// B12: pid of the currently serving generation's leader process
    /// (0 = none) — scrape-time cpu/rss sampling joins here.
    active_pid: AtomicU64,
    /// MIG-11-r2: a generation's routes are LIVE and serving — the
    /// typed criterion `resolve_through_switch` uses to split a route
    /// miss into `NotDeclared` (a generation serves but never declared
    /// this target) vs `NoServing` (tenant deployed but NOTHING is
    /// serving right now: launch failed / crashed / serialized-switch
    /// window ended without an install). Written ONLY by the actor at
    /// the route-map swap points: `true` AFTER the new map is stored
    /// on install, `false` BEFORE the map is emptied on exit /
    /// serialized drain / lease-kill / stop / drop — so
    /// `serving == true && lookup miss` can never observe a torn
    /// half-state and falsely report "not declared".
    serving: AtomicBool,
    /// Typed reason of the last assembly failure for the CURRENT
    /// expected spec (`bundle-unavailable` etc.) — surfaced on readyz
    /// so a pod can never look ready for a revision it cannot build.
    assemble_error: arc_swap::ArcSwapOption<String>,
    /// T25/L16: bounded sanitized tail of the last failed launch's
    /// stderr — diagnostic text (already secret-redacted at capture),
    /// surfaced on readyz as `detail` for not-ready workers.
    last_error: arc_swap::ArcSwapOption<String>,
    /// P3c-2-r1: the tenant's CURRENT desired DO storage declaration
    /// (`do_storage.tenant_dir` + `workerId → uniqueKeys`), published
    /// by the actor on every Apply — the ONLY trusted source admin
    /// endpoints may resolve class dirs from (requests are lookup
    /// keys, never path segments).
    do_storage_view: arc_swap::ArcSwapOption<DoStorageView>,
    /// P3c-2-r1: freeze requests from the local migration admin path —
    /// the actor services them on the next loop turn and kills the
    /// writer immediately (a fence without an immediate stop is the
    /// P0-2 dual-write window).
    do_freeze_requests: std::sync::Mutex<Vec<String>>,
    /// Wakes the actor when a freeze request lands (notify_one — a
    /// stored permit survives ordering vs `notified()` registration).
    do_freeze_notify: Notify,
    /// W4-r2: serialized-switch request-hold gate (see SwitchGate).
    pub switch_gate: SwitchGate,
    /// MIG-4: worker ids the last launch failure was attributed to
    /// (`service worker-<id>` in sanitized stderr, validated against
    /// the desired worker set). Drives precise `workerIds` on tenant
    /// failures; empty ⇒ tenant-wide reporting.
    attributed_workers: arc_swap::ArcSwap<Vec<String>>,
    /// MIG-4: worker ids the serving generation deliberately EXCLUDES
    /// (config-load attribution + exclusion plan). They serve nothing
    /// on this generation — surfaced as `config-load` tenant failures
    /// so the control plane marks exactly them.
    excluded_workers: arc_swap::ArcSwap<Vec<String>>,
    /// Scale-to-zero: runtime construction instant — the anchor of
    /// `last_activity_ms`.
    born: Instant,
    /// Scale-to-zero: monotone count of request arrivals
    /// ([`Self::touch`]) — the quiesce uses it to prove no request
    /// arrived between "idle" and "commit".
    activity_seq: AtomicU64,
    /// Scale-to-zero: ms since `born` of the latest arrival.
    last_activity_ms: AtomicU64,
    /// Scale-to-zero: single-flight wake cell (ingress joins, the actor
    /// resolves). Inert when the feature is off.
    pub wake: crate::hibernate::WakeFlight,
    /// Scale-to-zero: how long a waiter holds for one wake (ms).
    wake_wait_ms: AtomicU64,
}

impl TenantRuntime {
    /// Fresh runtime in `Starting`.
    pub fn new(tenant_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            tenant_id: tenant_id.into(),
            state: AtomicU8::new(TenantState::Starting.as_u8()),
            routing: ArcSwap::from_pointee(BTreeMap::new()),
            qconsumer_routing: ArcSwap::from_pointee(BTreeMap::new()),
            wfops_routing: ArcSwap::from_pointee(BTreeMap::new()),
            wfop_token_routing: ArcSwap::from_pointee(BTreeMap::new()),
            cron_routing: ArcSwap::from_pointee(BTreeMap::new()),
            drain_notify: Arc::new(Notify::new()),
            stats: TenantStats::default(),
            expected_digest: arc_swap::ArcSwapOption::from(None),
            serving_digest: arc_swap::ArcSwapOption::from(None),
            ever_served: AtomicBool::new(false),
            assemble_error: arc_swap::ArcSwapOption::from(None),
            active_pid: AtomicU64::new(0),
            serving: AtomicBool::new(false),
            last_error: arc_swap::ArcSwapOption::from(None),
            do_storage_view: arc_swap::ArcSwapOption::from(None),
            do_freeze_requests: std::sync::Mutex::new(Vec::new()),
            do_freeze_notify: Notify::new(),
            switch_gate: SwitchGate::default(),
            attributed_workers: arc_swap::ArcSwap::from_pointee(Vec::new()),
            excluded_workers: arc_swap::ArcSwap::from_pointee(Vec::new()),
            born: Instant::now(),
            activity_seq: AtomicU64::new(0),
            last_activity_ms: AtomicU64::new(0),
            wake: crate::hibernate::WakeFlight::new(Duration::from_secs(5)),
            wake_wait_ms: AtomicU64::new(30_000),
        })
    }

    /// Scale-to-zero: record a request arrival (ingress, before slot
    /// acquisition). Two relaxed-cost atomics — free when the feature
    /// is off.
    pub fn touch(&self) {
        self.activity_seq.fetch_add(1, Ordering::SeqCst);
        self.last_activity_ms.store(
            self.born.elapsed().as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    /// Scale-to-zero: arrivals seen so far.
    pub fn activity_seq(&self) -> u64 {
        self.activity_seq.load(Ordering::SeqCst)
    }

    /// Scale-to-zero: instant of the latest arrival (`born` if none).
    pub fn last_activity(&self) -> Instant {
        self.born + Duration::from_millis(self.last_activity_ms.load(Ordering::Relaxed))
    }

    /// Scale-to-zero: publish the host wake parameters (actor spawn).
    pub fn set_wake_params(&self, wait: Duration, cooldown: Duration) {
        self.wake_wait_ms
            .store(wait.as_millis().min(u64::MAX as u128) as u64, Ordering::Relaxed);
        self.wake.set_cooldown(cooldown);
    }

    /// Scale-to-zero: the longest a request waits for one wake.
    pub fn wake_wait(&self) -> Duration {
        Duration::from_millis(self.wake_wait_ms.load(Ordering::Relaxed))
    }

    /// Scale-to-zero: hold ONE request until the tenant is awake. The
    /// waiter is accounted against the SAME per-tenant held-request
    /// ceilings as the serialized-switch hold (256 / 32 MiB by default)
    /// through a cancel-safe [`HoldPermit`]; the wake itself is owned by
    /// the actor, so a disconnecting client never strands the others.
    /// Never reports success without the actor's verdict.
    pub async fn await_wake(
        self: &Arc<Self>,
        declared_bytes: usize,
        max_reqs: usize,
        max_bytes: usize,
        source: crate::hibernate::WakeSourceKind,
    ) -> Result<(), crate::hibernate::WakeError> {
        use crate::hibernate::{WakeError, WakeJoin};
        let _permit = self
            .try_switch_hold(declared_bytes, max_reqs, max_bytes)
            .ok_or(WakeError::Capacity)?;
        crate::metrics::registry().set(
            "vrt_host_switch_held_inflight",
            &[("tenant", self.tenant_id())],
            self.switch_gate.held() as f64,
        );
        match self.wake.join(source) {
            WakeJoin::Ticket(t) => t.wait(source.max_wait(self.wake_wait())).await,
            WakeJoin::CoolingDown(e) => Err(e),
        }
    }

    /// Tenant id.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Current lifecycle state.
    pub fn state(&self) -> TenantState {
        TenantState::from_u8(self.state.load(Ordering::SeqCst))
    }

    /// MIG-4: worker ids the current launch failures were attributed
    /// to (validated stderr `service worker-<id>` ∩ desired workers).
    pub fn attributed_workers(&self) -> Vec<String> {
        self.attributed_workers.load().as_ref().clone()
    }

    /// Publish the attribution set for the current desired.
    pub fn set_attributed_workers(&self, ids: Vec<String>) {
        self.attributed_workers.store(Arc::new(ids));
    }

    /// MIG-4: worker ids excluded from the serving generation
    /// (`config-load` exclusion plan) — they serve nothing here.
    pub fn excluded_workers(&self) -> Vec<String> {
        self.excluded_workers.load().as_ref().clone()
    }

    /// Publish the exclusion set installed with the latest generation.
    pub fn set_excluded_workers(&self, ids: Vec<String>) {
        self.excluded_workers.store(Arc::new(ids));
    }

    /// MIG-4: union of attributed + excluded ids — the "named" set a
    /// tenant failure may list; empty ⇒ the failure is tenant-wide
    /// (`workerIds` omitted on the wire).
    pub fn named_failure_workers(&self) -> std::collections::BTreeSet<String> {
        let mut out: std::collections::BTreeSet<String> =
            self.attributed_workers.load().iter().cloned().collect();
        out.extend(self.excluded_workers.load().iter().cloned());
        out
    }

    /// Atomic snapshot lookup of one worker's slot.
    pub fn endpoint_slot(&self, worker_id: &str) -> Option<Arc<EndpointSlot>> {
        self.routing.load().get(worker_id).cloned()
    }

    /// W4-r3: account one held request against the switch gate's
    /// ceilings; the returned [`HoldPermit`] frees the slot on Drop —
    /// cancel-safe by construction.
    pub fn try_switch_hold(
        self: &Arc<Self>,
        bytes: usize,
        max_reqs: usize,
        max_bytes: usize,
    ) -> Option<HoldPermit> {
        self.switch_gate
            .try_hold(bytes, max_reqs, max_bytes)
            .then(|| HoldPermit {
                runtime: self.clone(),
                bytes,
            })
    }

    /// MIG-11: resolve `lookup` to a slot, holding through the
    /// serialized-switch window while the gate is active — bounded by
    /// `budget` and the shared per-tenant hold ceilings. This is the
    /// ONE implementation of the gate wait every routed entry point
    /// uses (ingress, cron trigger, queue dispatch, wfops).
    ///
    /// - `Ok(Hit(_))` — resolved (immediately, or after the window
    ///   closed and routes landed);
    /// - `Ok(NotDeclared)` — a generation IS serving (`serving`) yet
    ///   `lookup` misses: the target was never declared — the
    ///   caller's normal 404 path;
    /// - `Ok(NoServing)` — the gate is NOT active and nothing serves
    ///   (`serving == false`): the tenant has no live generation —
    ///   retryable not-ready, never a 404;
    /// - `Err(Capacity|Timeout)` — the window was open but the wait
    ///   could not complete within bounds.
    ///
    /// The wake is registered+enabled BEFORE `active()` is re-checked
    /// (`notify_waiters` keeps no permit), so an `end()` landing
    /// between the lookup miss and the wait can never be lost. The
    /// [`HoldPermit`] drops with the call — the waiter slot frees
    /// before the caller dispatches.
    pub async fn resolve_through_switch<T>(
        self: &Arc<Self>,
        declared_bytes: usize,
        budget: Duration,
        max_reqs: usize,
        max_bytes: usize,
        lookup: impl Fn(&TenantRuntime) -> Option<T>,
    ) -> Result<SwitchResolve<T>, SwitchHoldReject> {
        let deadline = tokio::time::Instant::now() + budget;
        let mut permit: Option<HoldPermit> = None;
        loop {
            if let Some(t) = lookup(self) {
                return Ok(SwitchResolve::Hit(t));
            }
            let notified = self.switch_gate.cleared();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.switch_gate.active() {
                // `end()` may have landed routes just before our miss
                // — one last lookup before the genuine-absence answer.
                // `serving` is the typed split between "declared
                // nowhere" (404) and "nothing serving" (retryable
                // 503): the actor writes it `false` BEFORE emptying
                // the maps and `true` only AFTER a live install, so
                // `true` here proves a full generation's routes stand
                // behind the miss.
                return Ok(match lookup(self) {
                    Some(t) => SwitchResolve::Hit(t),
                    None if self.serving() => SwitchResolve::NotDeclared,
                    None => SwitchResolve::NoServing,
                });
            }
            if permit.is_none() {
                permit = Some(
                    self.try_switch_hold(declared_bytes, max_reqs, max_bytes)
                        .ok_or(SwitchHoldReject::Capacity)?,
                );
                let r = crate::metrics::registry();
                r.inc(
                    "vrt_host_switch_held_requests_total",
                    &[("tenant", self.tenant_id())],
                    1.0,
                );
                r.set(
                    "vrt_host_switch_held_inflight",
                    &[("tenant", self.tenant_id())],
                    self.switch_gate.held() as f64,
                );
            }
            if let Some(hook) = self
                .switch_gate
                .pre_wait_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                hook();
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(SwitchHoldReject::Timeout);
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// T37b: dispatch-endpoint slot of a consumer worker
    /// (`sock-qconsumer-<id>`); `None` for non-consumer workers.
    pub fn qconsumer_endpoint_slot(&self, worker_id: &str) -> Option<Arc<EndpointSlot>> {
        self.qconsumer_routing.load().get(worker_id).cloned()
    }

    /// W3: wfops facade endpoint slot of a workflow worker
    /// (`sock-wfops-<id>`); `None` when the worker declares no workflows.
    pub fn wfops_endpoint_slot(&self, worker_id: &str) -> Option<Arc<EndpointSlot>> {
        self.wfops_routing.load().get(worker_id).cloned()
    }

    /// CR1: cron dispatcher endpoint slot of a worker carrying `crons`
    /// (`sock-cron-<id>`); `None` when the worker declares no schedules.
    pub fn cron_endpoint_slot(&self, worker_id: &str) -> Option<Arc<EndpointSlot>> {
        self.cron_routing.load().get(worker_id).cloned()
    }

    /// W3-r2: facade token for `(workerId, workflowName)` of the
    /// serving generation. `None` ⇒ the workflow is not declared on
    /// the installed spec — the signed-desired authority check.
    pub fn wfop_token(&self, worker_id: &str, wf_name: &str) -> Option<String> {
        self.wfop_token_routing
            .load()
            .get(worker_id)
            .and_then(|m| m.get(wf_name))
            .cloned()
    }

    /// Spec fingerprint the converger last enqueued — `None` before
    /// the first Apply.
    pub fn expected_digest(&self) -> Option<String> {
        self.expected_digest
            .load()
            .as_deref()
            .map(|s| s.to_string())
    }

    /// Spec fingerprint of the installed (serving) generation.
    pub fn serving_digest(&self) -> Option<String> {
        self.serving_digest.load().as_deref().map(|s| s.to_string())
    }

    /// MIG-8: whether this tenant ever installed a generation — the
    /// distinction between initial convergence (gates pod readiness)
    /// and a generation switch on a previously-serving tenant (never
    /// pod-gating).
    pub fn ever_served(&self) -> bool {
        self.ever_served.load(Ordering::Acquire)
    }

    /// Actor write side — set inside `install_gen` when a generation
    /// goes live (crate-visible so status tests can stage the flag).
    pub(crate) fn mark_served(&self) {
        self.ever_served.store(true, Ordering::Release);
    }

    /// Publish the expected fingerprint (converger write side).
    pub fn set_expected_digest(&self, digest: String) {
        self.expected_digest
            .store(Some(std::sync::Arc::new(digest)));
    }

    /// Typed reason of the last assemble failure against the expected
    /// spec, if any (whitelist code — `bundle-unavailable`,
    /// `workers-not-ready`).
    pub fn assemble_error(&self) -> Option<String> {
        self.assemble_error.load().as_deref().map(|s| s.to_string())
    }

    /// Record/clear the assemble-failure reason (converger write side).
    pub fn set_assemble_error(&self, reason: Option<String>) {
        self.assemble_error.store(reason.map(std::sync::Arc::new));
    }

    /// Sanitized stderr tail of the last launch failure, if any
    /// (T25/L16 — diagnostic detail, never a reason code).
    pub fn last_error(&self) -> Option<String> {
        self.last_error.load().as_deref().map(|s| s.to_string())
    }

    /// Record/clear the launch-failure diagnostic (actor write side).
    /// Input is ALREADY sanitized + secret-redacted at capture.
    pub fn set_last_error(&self, detail: Option<String>) {
        self.last_error.store(detail.map(std::sync::Arc::new));
    }

    /// Publish the serving fingerprint (actor write side, on install).
    fn set_serving_digest(&self, digest: String) {
        self.serving_digest.store(Some(std::sync::Arc::new(digest)));
    }

    /// Worker ids currently routable.
    pub fn worker_ids(&self) -> Vec<String> {
        self.routing.load().keys().cloned().collect()
    }

    /// Stats for diagnostics/tests.
    pub fn stats(&self) -> &TenantStats {
        &self.stats
    }

    /// Pid of the serving generation's leader (0 = none) — B12 scrape
    /// sampling.
    pub fn active_pid(&self) -> u32 {
        self.active_pid.load(Ordering::Relaxed) as u32
    }

    /// Actor-side: publish the serving pid (0 clears).
    pub fn set_active_pid(&self, pid: u32) {
        self.active_pid.store(pid as u64, Ordering::Relaxed);
    }

    /// MIG-11-r2: is a generation's route map LIVE and serving? The
    /// typed split between a retryable "tenant has no serving
    /// generation" and a genuine "this generation never declared the
    /// target" at resolve time — see the `serving` field.
    pub fn serving(&self) -> bool {
        self.serving.load(Ordering::Acquire)
    }

    /// Actor-side writer for [`serving`](Self::serving) — every call
    /// site pairs it with a route-map swap (see the field docs for the
    /// ordering contract).
    pub(crate) fn set_serving(&self, serving: bool) {
        self.serving.store(serving, Ordering::Release);
    }

    /// P3c-2-r1: publish the tenant's desired DO storage view
    /// (`None` when the applied spec declares no DO classes).
    /// Actor write side.
    pub fn set_do_storage_view(&self, view: Option<DoStorageView>) {
        self.do_storage_view.store(view.map(std::sync::Arc::new));
    }

    /// Read side — admin endpoints resolve class dirs through this
    /// (desired-derived, never request-constructed).
    pub fn do_storage_view(&self) -> Option<std::sync::Arc<DoStorageView>> {
        self.do_storage_view.load().clone()
    }

    /// P3c-2-r1: request an immediate writer freeze for `class_key`
    /// (local migration path — the fence is already CAS-committed;
    /// this makes the local writer die NOW instead of at next renew).
    pub fn request_do_freeze(&self, class_key: String) {
        self.do_freeze_requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(class_key);
        self.do_freeze_notify.notify_one();
    }

    /// Actor write side — drain queued freeze class keys.
    fn take_do_freeze_requests(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .do_freeze_requests
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }

    /// Test hook: install worker endpoints and mark Ready without a
    /// child process — ingress tests use it for dead-endpoint and
    /// state-matrix cases.
    #[doc(hidden)]
    pub fn test_install(&self, endpoints: BTreeMap<String, WorkerEndpoint>) {
        let slots: BTreeMap<String, Arc<EndpointSlot>> = endpoints
            .into_iter()
            .map(|(worker, endpoint)| {
                (
                    worker,
                    Arc::new(EndpointSlot {
                        endpoint,
                        in_flight: AtomicUsize::new(0),
                        closed: AtomicBool::new(false),
                        drain_notify: self.drain_notify.clone(),
                    }),
                )
            })
            .collect();
        self.routing.store(Arc::new(slots));
        self.mark_served();
        self.set_serving(true);
        self.set_state(TenantState::Ready);
    }

    /// Test hook: install a qconsumer slot map directly (mirrors
    /// `test_install` for `sock-qconsumer-<id>` routing; T37b-r5).
    #[doc(hidden)]
    pub fn test_install_qconsumer(&self, endpoints: BTreeMap<String, WorkerEndpoint>) {
        let slots: BTreeMap<String, Arc<EndpointSlot>> = endpoints
            .into_iter()
            .map(|(worker, endpoint)| {
                (
                    worker,
                    Arc::new(EndpointSlot {
                        endpoint,
                        in_flight: AtomicUsize::new(0),
                        closed: AtomicBool::new(false),
                        drain_notify: self.drain_notify.clone(),
                    }),
                )
            })
            .collect();
        self.qconsumer_routing.store(Arc::new(slots));
        self.wfops_routing.store(Arc::new(BTreeMap::new()));
        self.wfop_token_routing.store(Arc::new(BTreeMap::new()));
        self.cron_routing.store(Arc::new(BTreeMap::new()));
        self.mark_served();
        self.set_serving(true);
        self.set_state(TenantState::Ready);
    }

    /// Test hook: install a wfops slot map directly (mirrors
    /// `test_install_qconsumer` for `sock-wfops-<id>` routing; W3).
    /// `tokens` seeds the generation's `wfop_tokens` map (W3-r2 hop
    /// injection source).
    #[doc(hidden)]
    pub fn test_install_wfops(
        &self,
        endpoints: BTreeMap<String, WorkerEndpoint>,
        tokens: BTreeMap<String, BTreeMap<String, String>>,
    ) {
        let slots: BTreeMap<String, Arc<EndpointSlot>> = endpoints
            .into_iter()
            .map(|(worker, endpoint)| {
                (
                    worker,
                    Arc::new(EndpointSlot {
                        endpoint,
                        in_flight: AtomicUsize::new(0),
                        closed: AtomicBool::new(false),
                        drain_notify: self.drain_notify.clone(),
                    }),
                )
            })
            .collect();
        self.wfops_routing.store(Arc::new(slots));
        self.wfop_token_routing.store(Arc::new(tokens));
        self.cron_routing.store(Arc::new(BTreeMap::new()));
        self.mark_served();
        self.set_serving(true);
        self.set_state(TenantState::Ready);
    }

    /// Test hook: install a cron slot map directly (mirrors
    /// `test_install_qconsumer` for `sock-cron-<id>` routing; CR1).
    #[doc(hidden)]
    pub fn test_install_cron(&self, endpoints: BTreeMap<String, WorkerEndpoint>) {
        let slots: BTreeMap<String, Arc<EndpointSlot>> = endpoints
            .into_iter()
            .map(|(worker, endpoint)| {
                (
                    worker,
                    Arc::new(EndpointSlot {
                        endpoint,
                        in_flight: AtomicUsize::new(0),
                        closed: AtomicBool::new(false),
                        drain_notify: self.drain_notify.clone(),
                    }),
                )
            })
            .collect();
        self.cron_routing.store(Arc::new(slots));
        self.mark_served();
        self.set_serving(true);
        self.set_state(TenantState::Ready);
    }

    /// Test hook: force a lifecycle state without a supervisor.
    #[doc(hidden)]
    pub fn test_set_state(&self, state: TenantState) {
        self.set_state(state);
    }

    /// Hibernation commit: empty every route map (the actor already
    /// dropped `serving`). Actor write side.
    pub(crate) fn clear_routes(&self) {
        self.routing.store(Arc::new(BTreeMap::new()));
        self.qconsumer_routing.store(Arc::new(BTreeMap::new()));
        self.wfops_routing.store(Arc::new(BTreeMap::new()));
        self.wfop_token_routing.store(Arc::new(BTreeMap::new()));
        self.cron_routing.store(Arc::new(BTreeMap::new()));
    }

    /// Test hook (MIG-11-r2): model "deployed but NOTHING serving" —
    /// every route map empties and the serving marker drops, exactly
    /// the state a failed serialized-switch launch / crash leaves
    /// behind (state may still read `Ready`: the transition table
    /// keeps `Ready` for a failed switch). `resolve_through_switch`
    /// must answer `NoServing`, never `NotDeclared`.
    #[doc(hidden)]
    pub fn test_uninstall(&self) {
        self.set_serving(false);
        self.routing.store(Arc::new(BTreeMap::new()));
        self.qconsumer_routing.store(Arc::new(BTreeMap::new()));
        self.wfops_routing.store(Arc::new(BTreeMap::new()));
        self.wfop_token_routing.store(Arc::new(BTreeMap::new()));
        self.cron_routing.store(Arc::new(BTreeMap::new()));
    }

    /// Total in-flight requests across current slots (diagnostics).
    pub fn in_flight(&self) -> usize {
        self.routing
            .load()
            .values()
            .map(|s| s.in_flight.load(Ordering::SeqCst))
            .sum()
    }

    fn set_state(&self, state: TenantState) {
        self.state.store(state.as_u8(), Ordering::SeqCst);
    }
}

/// RV-R53: one signed deletion tombstone scoped to this actor's
/// tenant (worker/class level — tenant-level tombstones are handled
/// host-side as `Undeploy`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DoDeletion {
    /// Worker whose DO dirs may be destroyed.
    pub worker_id: String,
    /// `Some` → exactly that class; `None` → the whole worker dir.
    pub class_key: Option<String>,
}

/// Commands into the supervisor actor.
pub enum SupervisorCmd {
    /// New launch spec (config/version/param change).
    Apply(Box<TenantLaunchSpec>),
    /// Drain everything, terminate, end the actor (host shutdown OR
    /// plain absence from desired — data-plane state such as DO
    /// storage MUST survive it; only a signed tombstone authorizes
    /// destruction).
    Stop,
    /// P3c-1: tenant removal CONFIRMED by a signed deletion tombstone —
    /// same drain path as [`Stop`], but the actor then runs the
    /// `delete` lifecycle: the tenant's DO storage dir is removed
    /// (idempotent, bounded, never another tenant's data). Host
    /// shutdown and tombstone-free absence use `Stop` — DO storage
    /// always survives them.
    Undeploy,
    /// RV-R53: the cell's currently-signed worker/class tombstones for
    /// this tenant. The actor destroys ONLY dirs named here; a worker
    /// or class that merely left the spec is retained.
    Deletions(Vec<DoDeletion>),
}

/// Owning handle of a running supervisor.
pub struct SupervisorHandle {
    runtime: Arc<TenantRuntime>,
    cmd: mpsc::Sender<SupervisorCmd>,
    task: JoinHandle<()>,
}

impl SupervisorHandle {
    /// Shared runtime (for the registry/ingress).
    pub fn runtime(&self) -> Arc<TenantRuntime> {
        self.runtime.clone()
    }

    /// Send a command; a dead actor swallows it.
    pub async fn send(&self, cmd: SupervisorCmd) {
        let _ = self.cmd.send(cmd).await;
    }

    /// Non-blocking command send — `Err` returns the command (full
    /// channel or dead actor) so the caller can retry next tick.
    /// T13/M4: the failure was previously swallowed while the tenant
    /// was still recorded as applied.
    pub fn try_send(
        &self,
        cmd: SupervisorCmd,
    ) -> Result<(), mpsc::error::TrySendError<SupervisorCmd>> {
        self.cmd.try_send(cmd)
    }

    /// Whether the actor task finished.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Await actor completion.
    pub async fn join(mut self) {
        let _ = (&mut self.task).await;
    }
}

/// Why a launch attempt failed — a stable reason token for metrics.
#[derive(Debug)]
pub struct SpawnFailure {
    /// `launch`, `timeout`, `exited`, `control`, `unexpected_socket`,
    /// `endpoint_mismatch`, `io`, `swap-io`, `swap-preclear`,
    /// `swap-child-exit`, `warm-exited`, `assemble`, `spawn`,
    /// `ready-timeout`, `config-load`, `unexpected_port`.
    pub reason: &'static str,
    /// T25/L16: bounded sanitized tail of the child's stderr (already
    /// secret-redacted at capture — safe for logs + status surface).
    pub detail: Option<String>,
}

impl SpawnFailure {
    fn launch(_: &LaunchError) -> Self {
        Self {
            reason: "launch",
            detail: None,
        }
    }

    fn ready(e: &ReadyError) -> Self {
        Self {
            reason: e.reason(),
            detail: None,
        }
    }

    /// Attach the bounded sanitized stderr tail.
    fn with_detail(mut self, detail: Option<String>) -> Self {
        self.detail = detail.filter(|d| !d.is_empty());
        self
    }
}

// ---- internal actor structures ----

struct PendingLaunch {
    seq: u64,
    task: JoinHandle<()>,
    /// The attempt's own work dir — removal guards check this so an
    /// exiting generation can never delete an in-flight launch's files.
    work_dir: std::path::PathBuf,
    /// `desired_spec.config_digest` AT LAUNCH TIME — install requires
    /// the current desired to still match it (T13/H2: the launched
    /// spec's own digest may differ when the assembler re-stamps it).
    desired_digest: String,
    /// P3c-2: leases acquired for the in-flight launch.
    leases: Vec<DoLeaseEntry>,
    /// MIG-13-r3: this pending IS a warm generation's swap — a failure
    /// counts `prewarm_total{outcome="failed"}` and retires the spent
    /// gen dir; a success installs the warm process itself.
    prewarm_swap: bool,
}

/// MIG-9: an off-actor `workerd test` preflight in flight. `attempt`
/// is the spec actually checked (possibly an exclusion-reduced
/// rebuild); `desired` is the FULL desired spec — both stay parked on
/// the actor while the check runs on a blocking-pool thread, then
/// travel to the verdict handler so the launch proceeds on exactly
/// the checked bytes. `cancel` group-kills + reaps the child when the
/// actor abandons the check (stop/supersede/drain).
struct PreflightCtx {
    seq: u64,
    attempt: Box<TenantLaunchSpec>,
    desired: Box<TenantLaunchSpec>,
    cancel: Arc<AtomicBool>,
}

/// MIG-13: a generation spawned against the prewarm twin config while
/// the OLD generation still serves — `Ev::PrewarmDone` lands the
/// spawn/ready verdict; `warm` then holds the live process until the
/// post-exit config swap. The twin repoints every `disk` service at
/// scratch dirs and strips the `vrt-switch` marker socket, so the
/// warm process can never open the shared class dirs (or fire their
/// alarms) while the old writer lives.
struct PrewarmCtx {
    /// Launch seq binding `Ev::PrewarmDone` to this ctx.
    seq: u64,
    /// The attempt spec (post-exclusion) and the full desired —
    /// launch re-entry inputs once the warm generation proves ready.
    attempt: Box<TenantLaunchSpec>,
    desired: Box<TenantLaunchSpec>,
    /// In-flight spawn task; `None` once `Ev::PrewarmDone` landed.
    task: Option<JoinHandle<()>>,
    /// The warm generation once it proved ready.
    warm: Option<WarmGen>,
    /// `<work_dir>/config.live.capnp` — the watched live file.
    live_config: PathBuf,
    /// The attempt's own work dir — `dir_referenced` covers it.
    work_dir: PathBuf,
    /// Prewarm start — `vrt_host_prewarm_seconds` anchor.
    started_at: Instant,
    /// MIG-13-r2/L2: end of the episode's lifetime — the swap must
    /// finish (or be abandoned) by this instant; `on_tick` kills the
    /// warm process and falls back to the serialized switch. MIG-13-r3:
    /// inert while the switch gate is open (early-flip routes live).
    expires_at: Instant,
}

/// A ready-but-uninstalled prewarmed generation.
struct WarmGen {
    /// Spawned process (spec/pid/child/control/endpoints).
    gen: PendingGen,
    /// `true` = the final config differs (real DO dirs) — the live
    /// file must be rewritten post-exit and the `--watch` reload
    /// reopens real storage in-process. `false` = the new spec has no
    /// DO storage; the warm process is already final.
    swap_needed: bool,
    /// Early-flip routes for workers that cannot reach DO storage
    /// through the binding graph — they serve from the warm
    /// generation during the drain window instead of holding.
    early: EarlyFlip,
    /// When the warm generation proved ready.
    warmed_at: Instant,
}

/// Route sets for the early-flipped (non-DO-reachable) workers.
#[derive(Default)]
struct EarlyFlip {
    routing: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    qconsumer: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    wfops: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    cron: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    /// `wfop_token_routing` subset for the flipped workers.
    wfop_tokens: BTreeMap<String, BTreeMap<String, String>>,
}

struct PendingGen {
    spec: Box<TenantLaunchSpec>,
    pid: u32,
    child: crate::launch::GroupChild,
    control: tokio::net::UnixStream,
    endpoints: BTreeMap<String, WorkerEndpoint>,
    /// T37b: `workerId → sock-qconsumer-<id>` resolved endpoints.
    qconsumer_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// W3: `workerId → sock-wfops-<id>` resolved endpoints.
    wfops_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// CR1: `workerId → sock-cron-<id>` resolved endpoints.
    cron_endpoints: BTreeMap<String, WorkerEndpoint>,
    /// P3c-2: leases acquired pre-spawn for this attempt; released if
    /// the launch never installs.
    leases: Vec<DoLeaseEntry>,
}

struct LiveGen {
    id: u64,
    pid: u32,
    spec: Box<TenantLaunchSpec>,
    slots: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    /// T37b qconsumer dispatch slots (empty for non-consumer workers).
    qconsumer_slots: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    /// W3 wfops facade slots (empty for workflow-less workers).
    wfops_slots: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    /// CR1 cron dispatcher slots (empty for cron-less workers).
    cron_slots: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    watcher: JoinHandle<()>,
    /// Control channel kept open for the generation's lifetime — whether
    /// real workerd reacts to control-fd EOF is unverified.
    _control: tokio::net::UnixStream,
    work_dir: PathBuf,
    /// P3c-2: this generation's held DO placement leases.
    leases: Vec<DoLeaseEntry>,
}

/// MIG-12: verdict of one aggregate `vrt-wu-probe` request — the
/// isolate-side pending `ctx.waitUntil` count, or a failed channel
/// (probe error / unreachable entrypoint / unwrapped worker).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WuVerdict {
    /// Sum of `ctx.waitUntil` promises still unsettled.
    Pending(u64),
    /// The probe failed — the count is unknown and must not be
    /// trusted (a half-dead generation SIGTERMs immediately, fail
    /// closed, never a drain hang).
    Failed,
}

struct DrainingGen {
    id: u64,
    pid: u32,
    slots: Arc<BTreeMap<String, Arc<EndpointSlot>>>,
    /// MIG-11: when the generation entered draining — the
    /// `drain → SIGTERM` stage anchor for the switch histogram.
    entered_at: Instant,
    deadline: Instant,
    sigterm_at: Option<Instant>,
    killed: bool,
    watcher: JoinHandle<()>,
    work_dir: PathBuf,
    /// P3c-1: this generation's spec carried DO storage — it may still
    /// be writing class dirs until it exits (serialized-switch gate
    /// and delete-sweep both key on this).
    had_do_storage: bool,
    /// MIG-11: drained as part of a serialized DO switch wait — the
    /// per-stage histogram (`vrt_host_do_switch_stage_seconds`) only
    /// observes generations carrying this flag; tenant drains and
    /// plain (post-install) switches don't belong to that series.
    do_switch: bool,
    /// P3c-2: held leases — fenced at exit so a successor can acquire
    /// without waiting out the lease TTL.
    leases: Vec<DoLeaseEntry>,
    /// MIG-12: this generation's `vrt-wu-probe` endpoint (from its
    /// launch spec) — `None` = no waitUntil accounting, the drain
    /// SIGTERMs at HTTP idle exactly as before.
    waituntil: Option<WorkerEndpoint>,
    /// First tick HTTP idle was observed — anchors `wait_until_grace`
    /// (cleared if the gen goes busy again, though slots stay closed
    /// so this can only flap through in-flight stragglers).
    wu_idle_since: Option<Instant>,
    /// In-flight aggregate probe `(seq, sent_at)` — bounded by
    /// `probe_timeout`; an expired probe resolves to
    /// `WuVerdict::Failed` rather than stalling the drain.
    wu_probe: Option<(u64, Instant)>,
    /// Last probe verdict — `Pending(0)` SIGTERMs clean,
    /// `Pending(n>0)` keeps waiting, `Failed` SIGTERMs dropped.
    wu_last: Option<WuVerdict>,
    /// Earliest next probe time — bounds probe rate so the drain
    /// never busy-polls a busy worker.
    wu_next_at: Instant,
}

/// P3c-2: one held DO placement lease, owned by the generation that
/// acquired it. Released (fenced) only after that generation's
/// process is confirmed dead — never while it can still write.
#[derive(Debug)]
/// DO1-r2: a departing class whose post-exit `Final` snapshot has
/// not committed. Bounded exponential backoff; `first_failed_ms`
/// arms the escalation ceiling; `exhausted` latches the error log
/// emission so it fires once, not per retry.
#[derive(Clone)]
struct DepartingPending {
    scope: crate::do_placement::PlacementScope,
    /// Lease epoch the commit must reference (held or fenced).
    epoch: u64,
    first_failed_ms: u64,
    next_retry_at: Instant,
    attempts: u32,
    exhausted: bool,
    /// True when the class reappears under a DIFFERENT worker in
    /// desired (transfer) — those hold the lease until committed.
    /// False = delete (best-effort backup; lease already released).
    is_transfer: bool,
    /// RV-R24: the pending source dir VANISHED before its `Final`
    /// committed. Latches the one-shot error log/metric — the entry
    /// stays (lease held, fail closed) so a restored dir can still
    /// resume the normal retry path.
    dir_missing: bool,
}

#[derive(Clone)]
pub(crate) struct DoLeaseEntry {
    pub(crate) scope: crate::do_placement::PlacementScope,
    pub(crate) epoch: crate::do_placement::PlacementEpoch,
    /// Wall-clock expiry (unix ms) as returned by acquire/renew.
    pub(crate) expires_at_ms: u64,
}

fn wall_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// PT1-r2: bookkeeping for a watchdog-ordered kill. Carries enough
/// identity to retry the SIGKILL safely — never signal a pid the
/// kernel may have recycled for someone else — plus a deadline after
/// which detection resumes rather than stalling the watchdog forever.
#[derive(Clone, Copy, Debug)]
struct RunawayKillMark {
    /// Generation the kill was ordered on.
    gen: u64,
    /// Pid (== process-group leader) signalled.
    pid: u32,
    /// When the last SIGKILL was sent.
    since: Instant,
    /// SIGKILL re-sends issued so far (the initial kill isn't counted).
    resends: u32,
}

/// PT1-r2: bounded SIGKILL re-sends for an unconverged runaway kill —
/// a live process cannot refuse SIGKILL, so non-convergence means a
/// lost/late exit event or an unkillable D-state; past the cap the
/// marker is released and normal detection resumes (the breaker
/// streak is already recorded).
const RUNAWAY_KILL_MAX_RESENDS: u32 = 3;

/// MIG-12: min interval between aggregate waitUntil probes on one
/// draining generation — bounds probe cost on a busy isolate while
/// staying well under the grace (never a busy-wait: `next_wake`
/// schedules off this).
const WU_PROBE_INTERVAL: Duration = Duration::from_millis(500);
/// MIG-13-r2/H2: the parked warm generation is death-polled on actor
/// ticks (no exit watcher owns its child — the swap needs it) — this
/// bounds how long early-flip routes can point at a dead socket.
const WARM_WATCH_INTERVAL: Duration = Duration::from_millis(200);
/// Scale-to-zero: after a failed wake, new waiters fail fast for this
/// long instead of each starting another launch (bounds relaunch
/// storms against a tenant that cannot start).
const WAKE_FAIL_COOLDOWN: Duration = Duration::from_secs(5);

/// MIG-12: cap on the bytes the waitUntil probe will buffer.
const WU_PROBE_MAX_BYTES: usize = 16384;

/// MIG-12: framing state of the (possibly partial) probe response.
#[derive(Debug, PartialEq, Eq)]
enum WuProgress {
    /// Header not complete yet, or no Content-Length (EOF delimits).
    NeedMore,
    /// Header + Content-Length body are all buffered; the value is the
    /// total byte length of the response (trailing bytes ignored).
    Complete(usize),
    /// Unsupported / invalid framing — fail closed.
    Reject,
}

/// Pure framing check over the bytes read so far. `Transfer-Encoding`
/// (chunked) is deliberately unsupported — the private probe endpoint
/// only ever answers with Content-Length — so it is `Reject`, fail
/// closed. An unparsable, conflicting or over-cap Content-Length is
/// `Reject` too.
fn wu_response_progress(buf: &[u8]) -> WuProgress {
    let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return WuProgress::NeedMore;
    };
    let head = String::from_utf8_lossy(&buf[..head_end]);
    let mut content_length: Option<usize> = None;
    for line in head.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return WuProgress::Reject;
        }
        if name.eq_ignore_ascii_case("content-length") {
            let Ok(n) = value.trim().parse::<usize>() else {
                return WuProgress::Reject;
            };
            if content_length.is_some_and(|prev| prev != n) {
                return WuProgress::Reject;
            }
            content_length = Some(n);
        }
    }
    let Some(cl) = content_length else {
        return WuProgress::NeedMore;
    };
    if cl > WU_PROBE_MAX_BYTES {
        return WuProgress::Reject;
    }
    let total = head_end + 4 + cl;
    if buf.len() >= total {
        WuProgress::Complete(total)
    } else {
        WuProgress::NeedMore
    }
}

/// MIG-12: one bounded HTTP GET on the generation's private
/// `vrt-wu` socket → `vrt-wu-probe` aggregate `pending`.
/// `None` = probe failed (any io/status/parse problem); `Some(n)` =
/// the isolate's pending `ctx.waitUntil` count. Runs blocking under
/// `spawn_blocking` — the per-call socket timeouts keep it inside
/// `timeout`.
fn wu_pending_probe(endpoint: &WorkerEndpoint, timeout: Duration) -> Option<u64> {
    use std::io::{Read, Write};
    trait Rw: Read + Write {}
    impl<T: Read + Write> Rw for T {}
    let mut s: Box<dyn Rw> = match endpoint {
        WorkerEndpoint::Unix(path) => {
            let st = std::os::unix::net::UnixStream::connect(path).ok()?;
            st.set_read_timeout(Some(timeout)).ok()?;
            st.set_write_timeout(Some(timeout)).ok()?;
            Box::new(st)
        }
        #[cfg(feature = "dev-stub")]
        WorkerEndpoint::Tcp(addr) => {
            let st = std::net::TcpStream::connect_timeout(addr, timeout).ok()?;
            st.set_read_timeout(Some(timeout)).ok()?;
            st.set_write_timeout(Some(timeout)).ok()?;
            Box::new(st)
        }
    };
    s.write_all(
        format!(
            "GET {} HTTP/1.1\r\nhost: vrt-wu-probe\r\nconnection: close\r\n\r\n",
            vrt_config::waituntil::PROBE_PATH
        )
        .as_bytes(),
    )
    .ok()?;
    // Bounded read. workerd answers with a complete Content-Length
    // response but does NOT close on `connection: close`, so once the
    // header + Content-Length bytes are in we stop (never wait for
    // EOF / the read timeout). Without Content-Length, EOF delimits.
    // A pathological peer can never grow this past the cap.
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    while buf.len() <= WU_PROBE_MAX_BYTES {
        match wu_response_progress(&buf) {
            WuProgress::Complete(total) => {
                buf.truncate(total);
                break;
            }
            WuProgress::Reject => return None,
            WuProgress::NeedMore => {}
        }
        match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => return None,
        }
    }
    // The last read may have completed (or invalidated) the framing.
    match wu_response_progress(&buf) {
        WuProgress::Complete(total) => buf.truncate(total),
        WuProgress::Reject => return None,
        WuProgress::NeedMore => {}
    }
    let text = String::from_utf8_lossy(&buf);
    let (head, body) = text.split_once("\r\n\r\n")?;
    if !head
        .lines()
        .next()
        .is_some_and(|l| l.split_whitespace().nth(1) == Some("200"))
    {
        return None;
    }
    #[derive(serde::Deserialize)]
    struct WuReply {
        pending: u64,
        #[serde(default)]
        failed: u64,
    }
    let reply: WuReply = serde_json::from_str(body.trim()).ok()?;
    if reply.failed > 0 {
        return None;
    }
    Some(reply.pending)
}

enum Ev {
    Launched {
        seq: u64,
        result: Result<PendingGen, SpawnFailure>,
    },
    Exited {
        id: u64,
    },
    /// PT1-r2: heartbeat probe completed off-actor. `seq` disambiguates
    /// a late result from a re-issued probe for the same generation.
    ProbeResult {
        gen: u64,
        seq: u64,
        ok: bool,
    },
    /// MIG-12: an aggregate waitUntil probe landed off-actor. `seq`
    /// binds the result to the in-flight `wu_probe` slot — a stale
    /// seq (probe re-issued, or the generation already reaped) is
    /// dropped.
    WuProbeResult {
        gen: u64,
        seq: u64,
        verdict: WuVerdict,
    },
    /// MIG-9: the bounded `workerd test` preflight finished OFF the
    /// actor thread (spawn_blocking). `seq` binds the verdict to the
    /// registered [`PreflightCtx`] — a stale seq (superseded/cancelled
    /// ctx) drops the result without touching launch state.
    PreflightDone {
        seq: u64,
        result: Result<(), vrt_config::check::CheckFailure>,
        elapsed_ms: u64,
    },
    /// MIG-13: the prewarmed generation's spawn+ready verdict landed
    /// off-actor (twin config: scratch DO dirs, no marker socket,
    /// `--watch`). `seq` binds it to the registered `PrewarmCtx`.
    PrewarmDone {
        seq: u64,
        result: Result<PendingGen, SpawnFailure>,
    },
    /// Scale-to-zero: the pre-commit aggregate waitUntil probe of the
    /// serving generation landed off-actor. `seq` binds it to the
    /// current `HibAttempt` — a stale result is dropped.
    HibProbeResult {
        gen: u64,
        seq: u64,
        verdict: WuVerdict,
    },
}

/// Injectable dependencies (tests provide fakes).
pub struct SupervisorDeps {
    /// Process launcher.
    pub launcher: Arc<dyn WorkerdLauncher>,
    /// Control-event parser factory — a fresh parser per launch attempt.
    pub probe_factory: Arc<dyn Fn() -> Box<dyn ControlEventParser> + Send + Sync>,
    /// Resource sampler.
    pub sampler: Arc<dyn ResourceSampler>,
    /// Restart jitter source in milliseconds (`0..=max`).
    pub jitter: Arc<dyn Fn(u64) -> u64 + Send + Sync>,
    /// T13/H2: re-assembler for RETRY launches — every attempt gets its
    /// own generation directory so an exiting predecessor can never
    /// delete files the new generation needs. `None` (unit tests)
    /// falls back to identical-spec reuse.
    pub assembler: Option<Arc<dyn TenantAssembler>>,
    /// Shared monotone generation counter — host converge and actor
    /// retries draw from it so `gen-<n>` dirs never collide.
    pub gen_counter: Arc<std::sync::atomic::AtomicU64>,
    /// P3c-2 seam: DO placement lease — `acquire` runs before a
    /// generation opens class dirs. P3c-1 ships [`LocalPlacement`]
    /// (durable epoch file, advisory); K8s swaps in a shared-store
    /// impl for cross-cell fencing.
    pub placement: Arc<dyn crate::do_placement::DoPlacement>,
    /// P3c-2: DO snapshot/restore surface — `Some` only when the
    /// placement backend is object-store-authoritative (local-only
    /// standalone keeps no remote snapshots: the local dir IS the
    /// data). Restore runs right after `acquire`, before workerd
    /// opens the dir.
    pub do_snap: Option<Arc<DoSnapCtx>>,
    /// W6-r2: workflow-instance delete tombstone root
    /// (`<work_dir>/wf-del`). Empty → reclaim is a no-op (unit tests
    /// and hosts without the wf-delete endpoint). Independent of
    /// `do_snap`: tombstones must exist on standalone hosts too,
    /// where `wal`/`do_snap` are `None`.
    pub wf_tomb_dir: std::path::PathBuf,
    /// C1: shared Cache API service — `Some` iff the host config has
    /// `[cache]` (the SAME condition under which assemble emits
    /// `cacheApiOutbound`). `None` ⇒ workers get no cache binding and
    /// `caches.*` throws `No Cache was configured` — explicit, never
    /// a fake store.
    pub cache: Option<Arc<crate::cache::CacheService>>,
    /// PT1: bounded event-loop liveness probe for the runaway
    /// watchdog. Injected so unit tests never touch sockets; the
    /// production impl requires a response byte, not just a connect.
    pub heartbeat: Arc<dyn crate::runaway::HeartbeatProbe>,
    /// RV-R53: DO storage root — the host needs it to purge dirs for
    /// tombstoned tenants/workers that have no live actor. `None`
    /// (standalone without DO storage) makes host-level purges a no-op.
    pub do_storage_root: Option<PathBuf>,
    /// RV-R53.4: durable object-store purge ledger — tombstone-named
    /// class prefixes are deleted through it (persisted, bounded,
    /// restart-resumable). `None` when no object store is configured.
    pub purge: Option<Arc<crate::do_purge::DoPurge>>,
    /// MIG-13-r2/L3: OPTIONAL admission hook for the dual-generation
    /// window — a prewarm runs two workerd processes for one tenant
    /// while it warms. The host has no memory/admission budget for
    /// generation spawns today (`ResourceLimits` are sampled
    /// post-launch, never pre-admission), so this is the typed
    /// extension point rather than an invented budget: `Some` ⇒
    /// `admit_prewarm` may refuse the extra process (the switch then
    /// falls back to the serialized path, `prewarm_total{skipped}`);
    /// `None` ⇒ prewarm is never admission-gated.
    pub prewarm_admission: Option<Arc<dyn PrewarmAdmission>>,
}

/// MIG-13-r2/L3: see [`SupervisorDeps::prewarm_admission`].
pub trait PrewarmAdmission: Send + Sync {
    /// `true` ⇒ the second (warm) process may spawn for this spec.
    /// Must be non-blocking — it runs on the actor's launch path.
    fn admit_prewarm(&self, spec: &TenantLaunchSpec) -> bool;
}

/// P3c-2: object-store surface for DO snapshot upload/restore and
/// lifecycle cleanup. `placement` is the CONCRETE impl so snapshot
/// commits can CAS-check the epoch (the `DoPlacement` trait object
/// alone can't see the manifest).
pub struct DoSnapCtx {
    pub store: Arc<dyn vrt_objstore::ObjectStore>,
    pub placement: Arc<crate::do_placement_store::ManifestDoPlacement>,
    /// Staging root for snapshot materialization (per-attempt dirs
    /// under it; bounded by `vrt_sql::do_snap` caps).
    pub tmp_root: std::path::PathBuf,
    /// Periodic snapshot interval (ms) + jitter bound (ms).
    pub interval_ms: u64,
    pub jitter_ms: u64,
    /// DO1-r2 P0-2a: max wall-clock time a departing transfer's
    /// final-snapshot retry may run while still holding its lease
    /// before the failure escalates to error-level observability.
    /// The lease is HELD past this bound regardless — the bound
    /// only changes log level/metric, never releases data unsafely.
    /// Clamped to [60s, 24h].
    pub departing_max_ms: u64,
    /// DO1-r2 P1-1: quarantined transfer-source dirs are kept this
    /// long for operator recovery before the stale sweep reclaims
    /// them. Clamped to [1h, 30d].
    pub xfer_quarantine_ms: u64,
    /// DO1-r2 P1-2: departing-final-snapshot retry backoff — base
    /// delay doubles per attempt, capped at `departing_retry_cap_ms`.
    pub departing_retry_base_ms: u64,
    pub departing_retry_cap_ms: u64,
    /// WAL1: continuous WAL replication — `None` (feature off or no
    /// object store) leaves the snapshot-only path byte-identical.
    pub wal: Option<Arc<crate::do_wal::DoWalReplicator>>,
}

impl Default for SupervisorDeps {
    fn default() -> Self {
        Self {
            launcher: Arc::new(crate::launch::ProcessWorkerdLauncher),
            probe_factory: Arc::new(|| Box::new(JsonLineControlParser::new())),
            sampler: Arc::new(crate::monitor::OsSampler),
            assembler: None,
            gen_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            placement: Arc::new(crate::do_placement::LocalPlacement),
            do_snap: None,
            jitter: Arc::new(|max| {
                if max == 0 {
                    return 0;
                }
                // Time-derived jitter — enough to decohere restart storms
                // across tenants without a crypto RNG dependency.
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos() as u64)
                    .unwrap_or(0);
                nanos % (max + 1)
            }),
            wf_tomb_dir: std::path::PathBuf::new(),
            cache: None,
            heartbeat: Arc::new(crate::runaway::OsHeartbeatProbe),
            do_storage_root: None,
            purge: None,
            prewarm_admission: None,
        }
    }
}

/// Spawn the per-tenant actor. `spec` is launched immediately.
pub fn spawn_supervisor(
    runtime: Arc<TenantRuntime>,
    spec: TenantLaunchSpec,
    deps: SupervisorDeps,
    params: SupervisorParams,
) -> SupervisorHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let (ev_tx, ev_rx) = mpsc::channel(32);
    // First-boot spec may already declare DO — capture the tenant dir
    // so the undeploy/sweep lifecycle can locate it.
    let do_tenant_dir = spec.do_storage.as_ref().map(|d| d.tenant_dir.clone());
    // First-boot DO declaration is part of the trusted admin view too.
    runtime.set_do_storage_view(spec.do_storage.as_ref().map(|d| DoStorageView {
        tenant_dir: d.tenant_dir.clone(),
        worker_classes: d.worker_classes.clone(),
    }));
    let actor = Actor {
        tenant_id: runtime.tenant_id.clone(),
        runtime: runtime.clone(),
        cmd_rx,
        ev_tx: ev_tx.clone(),
        ev_rx,
        deps,
        ready_timeout: params.ready_timeout,
        probe_timeout: params.probe_timeout,
        kill_grace: params.kill_grace,
        fail_threshold: params.fail_threshold,
        circuit_cooldown: params.circuit_cooldown,
        drain_timeout: params.drain_timeout,
        monitor_interval: params.monitor_interval,
        max_switch_failures: params.max_switch_failures,
        preflight_timeout: params.preflight_timeout,
        backoff: RestartBackoff::new(params.backoff.0, params.backoff.1),
        window: FailureWindow::new(params.fail_window, params.fail_threshold),
        state: TenantState::Starting,
        desired_spec: Some(Box::new(spec)),
        active: None,
        draining: Vec::new(),
        pending: None,
        seq: 0,
        cooldown_until: None,
        half_open: false,
        retry_at: None,
        switch_fail_streak: 0,
        latched_digest: None,
        attributed: std::collections::BTreeSet::new(),
        excluded: std::collections::BTreeSet::new(),
        attempted_once: false,
        next_monitor: Instant::now() + params.monitor_interval,
        seg_dir: None,
        do_wait_since: None,
        do_switch_exit_at: None,
        preflight_ok_digest: None,
        preflight: None,
        prewarm: None,
        prewarm_ttl: params.prewarm_ttl,
        prewarm_burned: false,
        preflight_seq: 0,
        pending_do_leases: Vec::new(),
        do_lease_wake: None,
        do_class_keys: std::collections::BTreeSet::new(),
        do_deletions: std::collections::BTreeMap::new(),
        departing_snap_pending: std::collections::BTreeMap::new(),
        held_departing: Vec::new(),
        carried_leases: Vec::new(),
        carried_lost: Default::default(),
        do_pipeline: params.do_pipeline,
        do_snap_due: std::collections::BTreeMap::new(),
        do_snap_ok_at: std::collections::BTreeMap::new(),
        do_tenant_dir,
        undeploy: false,
        cache_listener: None,
        runaway: None,
        runaway_kill: None,
        probe_in_flight: None,
        probe_seq: 0,
        wait_until_grace: params.wait_until_grace,
        do_switch_wait_until_grace: params.do_switch_wait_until_grace,
        wu_probe_seq: 0,
        runaway_window: FailureWindow::new(params.fail_window, params.fail_threshold),
        runaway_breaker_pending: false,
        fenced_log: (None, Duration::from_secs(5)),
        s2z: params.scale_to_zero.clone(),
        ready_since: Instant::now(),
        hib_next_eval: Instant::now(),
        hib_backoff: Default::default(),
        hib: None,
        wake_run: None,
        hib_probe_seq: 0,
    };
    if let Some(cfg) = &params.scale_to_zero {
        actor
            .runtime
            .set_wake_params(cfg.wake_deadline + Duration::from_secs(1), WAKE_FAIL_COOLDOWN);
    }
    let task = tokio::spawn(actor.run());
    SupervisorHandle {
        runtime,
        cmd: cmd_tx,
        task,
    }
}

struct Actor {
    tenant_id: String,
    runtime: Arc<TenantRuntime>,
    cmd_rx: mpsc::Receiver<SupervisorCmd>,
    ev_tx: mpsc::Sender<Ev>,
    ev_rx: mpsc::Receiver<Ev>,
    deps: SupervisorDeps,
    ready_timeout: Duration,
    probe_timeout: Duration,
    kill_grace: Duration,
    fail_threshold: usize,
    circuit_cooldown: Duration,
    drain_timeout: Duration,
    monitor_interval: Duration,
    backoff: RestartBackoff,
    window: FailureWindow,
    state: TenantState,
    desired_spec: Option<Box<TenantLaunchSpec>>,
    active: Option<LiveGen>,
    draining: Vec<DrainingGen>,
    pending: Option<PendingLaunch>,
    seq: u64,
    cooldown_until: Option<Instant>,
    half_open: bool,
    retry_at: Option<Instant>,
    next_monitor: Instant,
    seg_dir: Option<PathBuf>,

    /// Whether at least one launch attempt already ran — the FIRST
    /// attempt uses the converge-assembled spec (its dir is already
    /// unique); only retries reassemble (T13/H2) so converge specs
    /// never orphan.
    attempted_once: bool,
    /// Consecutive switch-launch failures with an
    /// active generation — capped by `max_switch_failures`
    /// (T13/M4); reset on install or a new Apply.
    switch_fail_streak: u32,
    /// MIG-1: the desired config_digest whose switch retry budget was
    /// exhausted. `SwitchFailed` latches PER CONTENT, not per spec version:
    /// an Apply whose desired digest still equals this value keeps the
    /// latch (no infinite retries); an Apply carrying ANY different digest
    /// — including deleting the bad worker or reverting to known-good code
    /// — re-arms the switch. Cleared on successful install and when the
    /// desired digest reverts to the currently serving one.
    latched_digest: Option<String>,
    /// Retry cap from `SupervisorParams` (T13/M4).
    max_switch_failures: u32,
    /// MIG-4: workers the current desired's config-load failures were
    /// attributed to (`service worker-<id>` ∩ desired workers).
    /// Reporting only — never authorizes touching other workers.
    attributed: std::collections::BTreeSet<String>,
    /// MIG-4: workers excluded from the launch attempt by the
    /// exclusion plan (bounded per desired). Empty ⇒ full generation.
    /// Cleared when the desired digest changes.
    excluded: std::collections::BTreeSet<String>,
    /// PT1: runaway watchdog state for the ACTIVE generation —
    /// reset whenever a new generation installs (cpu_seconds and the
    /// consecutive-failure counter are per-pid).
    runaway: Option<crate::runaway::RunawayDetector>,
    /// PT1: set when the watchdog SIGKILLed the active group — the
    /// crash path turns the next relaunch into an immediate retry
    /// instead of backoff so a fresh generation takes over fast.
    /// PT1-r2: bookkeeping for a watchdog-ordered kill — enough
    /// identity to safely retry the SIGKILL (never signal a recycled
    /// pid) and a deadline after which detection resumes rather than
    /// stalling the tenant's watchdog forever.
    runaway_kill: Option<RunawayKillMark>,
    /// PT1-r2: (gen, seq, since) of the in-flight off-actor probe.
    /// One at a time — never stacks probes.
    probe_in_flight: Option<(u64, u64, Instant)>,
    probe_seq: u64,
    /// MIG-12: post-idle waitUntil grace, from
    /// `SupervisorParams::wait_until_grace`.
    wait_until_grace: Duration,
    /// MIG-12-r2: do-switch-bounded waitUntil grace, from
    /// `SupervisorParams::do_switch_wait_until_grace`.
    do_switch_wait_until_grace: Duration,
    /// MIG-12: monotonically increasing waitUntil-probe id —
    /// separates a stale `Ev::WuProbeResult` from the in-flight probe
    /// registered on the draining generation.
    wu_probe_seq: u64,
    /// PT1: runaway-kill streak in `fail_window` — a SECOND window,
    /// because `window` resets on every successful Ready and a
    /// kill→reinstall→serve cycle would otherwise never trip the
    /// breaker. Events here only prune by time, so N runaway kills
    /// inside the window open the circuit.
    runaway_window: crate::backoff::FailureWindow,
    /// PT1: the NEXT exit trips the breaker — set when the kill-side
    /// runaway_window reached `fail_threshold`.
    runaway_breaker_pending: bool,
    /// MIG-8: bound on one preflight subprocess (admission queue +
    /// run), from `SupervisorParams::preflight_timeout`.
    preflight_timeout: Duration,
    /// P3c-1: serialized DO switch — when `Some`, a launch is waiting
    /// for every DO-capable generation to fully exit (bounded by
    /// drain_timeout + kill_grace); elapsed lands in
    /// `vrt_host_do_switch_wait_seconds` on install.
    do_wait_since: Option<Instant>,
    /// MIG-11: Instant the last serialized-switch generation exited —
    /// the `exit → ready` stage anchor, consumed by `install_gen`.
    do_switch_exit_at: Option<Instant>,
    /// W1-r2: config digest already proven loadable by the pre-launch
    /// `workerd test` preflight — serialized re-entry and retry
    /// re-assembly skip the subprocess (same desired ⇒ identical
    /// rendered bytes ⇒ same verdict).
    preflight_ok_digest: Option<String>,
    /// MIG-9: the in-flight off-actor preflight, if any. While `Some`
    /// the actor still services cmds/evs; `abort_pending` flips
    /// `ctx.cancel` → the check group-kills and reaps the child.
    preflight: Option<PreflightCtx>,
    /// MIG-13: the serialized-switch prewarm — a spawned-but-
    /// uninstalled generation warming on scratch DO dirs while the
    /// old generation still serves (see `start_prewarm`).
    prewarm: Option<PrewarmCtx>,
    /// MIG-13-r2/L2: warm-generation lifetime cap, from
    /// `SupervisorParams::prewarm_ttl`.
    prewarm_ttl: Duration,
    /// MIG-13-r2/L2: prewarm already ran (or expired) for the CURRENT
    /// desired — further attempts for the same desired take the
    /// serialized path instead of parking a second warm process.
    /// Reset when an Apply carries different desired content.
    prewarm_burned: bool,
    /// MIG-9: monotonically increasing preflight id — separates a stale
    /// `Ev::PreflightDone` from the currently-registered ctx.
    preflight_seq: u64,
    /// P3c-1: tenant DO storage root — remembered across applies so a
    /// DO→none switch or tenant undeploy can still locate the dir for
    /// the `delete` lifecycle (sweep / removal).
    do_tenant_dir: Option<PathBuf>,
    /// P3c-1: `Undeploy` was received — the Stopped cleanup removes
    /// `do_tenant_dir` (delete lifecycle) instead of only gen dirs.
    undeploy: bool,
    /// P3c-2: leases acquired by `start_pending_launch`, moved into
    /// the `PendingLaunch`/`PendingGen`/`LiveGen` chain at spawn and
    /// released at every abandonment path.
    pending_do_leases: Vec<DoLeaseEntry>,
    /// P3c-2: next wall-clock deadline at which lease renewal/loss
    /// must be evaluated (fed into `next_wake`).
    do_lease_wake: Option<Instant>,
    /// P3c-2: every class key this actor has ever acquired — drives
    /// object-store cleanup on undeploy (leases themselves may be
    /// gone by then; the key list is the lifecycle record).
    do_class_keys: std::collections::BTreeSet<String>,
    /// RV-R53: signed deletion intents delivered by the host —
    /// `worker_id → class keys`, where the empty key `""` marks the
    /// WHOLE worker dir. The sweep destroys only tombstoned entries;
    /// everything else is retained (exclusion/eviction ≠ deletion).
    do_deletions: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// DO1: departing-class final snapshots that FAILED after the
    /// writer exited — retried with exponential backoff until the
    /// baseline commits; the sweep keeps those dirs so the source
    /// copy survives a store outage.
    departing_snap_pending: std::collections::BTreeMap<(String, String), DepartingPending>,
    /// DO1-r2 P0-2a: leases of TRANSFERRED classes whose `Final`
    /// snapshot has not committed yet. The writer is dead — these
    /// are renewed like normal leases (no writer runs on them) and
    /// released ONLY when the baseline lands; an unreleased hold is
    /// what keeps the target cell from fencing into a stale restore.
    held_departing: Vec<DoLeaseEntry>,
    /// MIG-17: leases CARRIED across a serialized DO switch — the
    /// class is still desired under the SAME worker, so the
    /// just-exited generation's lease was never released. Renewed
    /// like `held_departing` (no writer runs on them) until the
    /// pending launch's placement pipeline consumes them; leftovers
    /// take the normal departing path.
    carried_leases: Vec<DoLeaseEntry>,
    /// MIG-17: `(worker, class_key)` whose carried lease failed
    /// renewal inside the carry window — the pending launch must not
    /// open the class on an unverifiable lease (fail closed); the
    /// pipeline sees `Carry::Lost` and takes the acquire-failure
    /// path.
    carried_lost: std::collections::BTreeSet<(String, String)>,
    /// MIG-17: bounded per-class placement pipeline tuning, from
    /// `SupervisorParams::do_pipeline`.
    do_pipeline: crate::do_switch_pipeline::PipeCfg,
    /// P3c-2: `class_key → next snapshot due (unix ms)`.
    do_snap_due: std::collections::BTreeMap<String, u64>,
    /// P3c-2: `class_key → last successful snapshot (unix ms)` — the
    /// age gauge's numerator.
    do_snap_ok_at: std::collections::BTreeMap<String, u64>,
    /// C1: this tenant's Cache API unix listener — bound once per
    /// ACTOR (data and socket survive generation switches; the cache
    /// is keyed by listener scope, so identity cannot leak). Dropped
    /// with the actor → socket unlinked + tenant key space purged.
    cache_listener: Option<crate::cache::CacheListener>,
    /// MIG-10: throttled fenced-acquire ERROR logging — the first
    /// event logs at once; repeats emit only when `next` passes, with
    /// the interval doubling to a 60 s cap (a held foreign lease used
    /// to spam one ERROR per 5 s retry for the whole TTL — ~70 s of
    /// noise on the real cluster). `(next_log_at, next_interval)`;
    /// reset when an acquire succeeds.
    fenced_log: (Option<Instant>, Duration),
    /// Scale-to-zero configuration (`None` = OFF: every hook below is
    /// inert and the actor behaves exactly as before).
    s2z: Option<crate::hibernate::ScaleToZero>,
    /// When the active generation was installed (residency anchor for
    /// the idle policy).
    ready_since: Instant,
    /// Earliest instant of the next idle evaluation.
    hib_next_eval: Instant,
    /// Backoff for consecutive non-traffic hibernation aborts.
    hib_backoff: crate::hibernate::HibernateRetryBackoff,
    /// The in-progress hibernation attempt, if any.
    hib: Option<HibAttempt>,
    /// The wake this actor is currently serving, if any.
    wake_run: Option<WakeRun>,
    /// Monotone id of waitUntil pre-commit probes.
    hib_probe_seq: u64,
}

/// Where the waitUntil pre-commit probe stands.
#[derive(Clone, Copy, Debug)]
enum HibWu {
    /// The generation has no accounting endpoint (same semantics as
    /// the existing drain — SIGTERM at HTTP idle).
    NotNeeded,
    /// Not yet fired.
    NotStarted,
    /// Fired; `seq` binds the result.
    InFlight { seq: u64 },
    /// Landed.
    Done(WuVerdict),
}

/// One hibernation attempt (slots closed, waiting to prove quiescence).
struct HibAttempt {
    /// Hard bound — past it the attempt is abandoned (never force-kills).
    deadline: Instant,
    /// `TenantRuntime::activity_seq` when the attempt began: any change
    /// proves a request arrived and aborts the attempt.
    activity_seq: u64,
    /// The generation under attempt.
    gen_id: u64,
    wu: HibWu,
}

/// One wake being served by the actor.
struct WakeRun {
    /// Flight id (idempotency key).
    flight: u64,
    /// Trigger kind (bounds the launch retry budget).
    source: crate::hibernate::WakeSourceKind,
    /// Flight start — anchors the deadline.
    started: Instant,
    deadline: Instant,
    /// Launch attempts made so far (bounded by the source's budget).
    attempt: u32,
    /// Coordinator slot, held for the whole launch.
    permit: Option<crate::hibernate::WakePermit>,
}

/// T34f2/R2: the exit watchers own the `GroupChild` kill guards and
/// the pending launch task owns the in-flight child — but dropping a
/// `JoinHandle` DETACHES its task. An actor dropped via task abort
/// (shutdown deadline) must abort them explicitly, or the children
/// outlive the guard until runtime teardown. Aborting a completed
/// task is a no-op, so this also runs harmlessly on the normal path.
impl Drop for Actor {
    fn drop(&mut self) {
        // MIG-11-r2: an actor dropped mid-switch-window (task abort
        // past the shutdown deadline) would otherwise pin the gate
        // `active` on a runtime the registry still hands out — held
        // waiters would burn their whole budget, and a post-window
        // miss would read as the false "not declared" 404. `end` is
        // idempotent; `serving=false` is the same not-ready marker
        // every other teardown path writes. One runtime belongs to
        // exactly one live actor — `reap_finished`/`shutdown_all`
        // remove the registry entry, so no successor can be clobbered.
        self.runtime.set_serving(false);
        self.runtime.switch_gate.end();
        // Scale-to-zero: waiters on a wake flight must not hang on a
        // vanished actor (never a fake success), and a queued wake slot
        // is withdrawn. The permit (if any) frees with the field.
        if let Some(s) = &self.s2z {
            s.coordinator.cancel(&self.tenant_id);
            self.runtime
                .wake
                .complete_current(Err(crate::hibernate::WakeError::Failed("tenant-gone")));
        }
        if let Some(ctx) = &self.preflight {
            ctx.cancel.store(true, Ordering::SeqCst);
        }
        if let Some(p) = &self.pending {
            p.task.abort();
        }
        if let Some(p) = &self.prewarm {
            if let Some(t) = &p.task {
                t.abort();
            }
        }
        if let Some(live) = &self.active {
            live.watcher.abort();
        }
        for g in &self.draining {
            g.watcher.abort();
        }
        // MIG-10: ALL still-held leases release in ONE bounded pass —
        // before, each generation's fences serialized behind its own
        // CAS retries, so a slow/unreachable store could pin the drop
        // past the pod's SIGKILL grace and leave every lease to wait
        // out the TTL anyway (the real-cluster ~70 s fence). Fencing
        // here is safe: the writers' GroupChild guards die with this
        // drop, so nothing can still dual-write; whatever the 10 s
        // budget cannot reach simply lapses by TTL — crash semantics
        // are never shortened.
        let mut leases: Vec<DoLeaseEntry> = Vec::new();
        if let Some(p) = &self.pending {
            leases.extend(p.leases.iter().cloned());
        }
        if let Some(g) = &self.active {
            leases.extend(g.leases.iter().cloned());
        }
        for g in &self.draining {
            leases.extend(g.leases.iter().cloned());
        }
        leases.extend(self.pending_do_leases.iter().cloned());
        // DO1-r2: a held departing lease becomes a dead-marker here —
        // the on-disk `.final-snap-pending` marker re-seeds the retry
        // on the next host, which then commits via the fenced path.
        leases.extend(self.held_departing.iter().cloned());
        // MIG-17: a dropped mid-carry lease likewise — never leave it
        // to TTL when the holder can fence it now.
        leases.extend(self.carried_leases.iter().cloned());
        Self::release_leases_bounded(
            &*self.deps.placement,
            &self.tenant_id,
            &leases,
            Some(Duration::from_secs(10)),
        );
        // C1: listener drop unlinks the socket and aborts the accept
        // loop; purging the tenant key space keeps the shared byte
        // cap honest — cache semantics permit loss on stop/undeploy.
        drop(self.cache_listener.take());
        if let Some(svc) = &self.deps.cache {
            svc.remove_tenant(&self.tenant_id);
        }
    }
}

impl Actor {
    async fn run(mut self) {
        self.set_state(TenantState::Starting);
        self.start_pending_launch(true);
        loop {
            let wake = self.next_wake();
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.on_cmd(cmd),
                        None => self.on_cmd(SupervisorCmd::Stop),
                    }
                }
                ev = self.ev_rx.recv() => {
                    if let Some(ev) = ev { self.on_ev(ev); }
                }
                _ = self.runtime.drain_notify.notified() => {
                    self.service_draining(Instant::now());
                    self.service_hibernate(Instant::now());
                    self.check_stopped();
                }
                _ = self.runtime.wake.notify.notified() => {
                    self.service_wake();
                }
                _ = self.runtime.do_freeze_notify.notified() => {
                    self.service_do_freeze();
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {
                    self.on_tick();
                }
            }
            if self.state == TenantState::Stopped {
                break;
            }
        }
        self.runtime.set_active_pid(0);
        self.runtime.set_serving(false);
        self.runtime.set_state(TenantState::Stopped);
        if let Some(dir) = self.seg_dir.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
        // P3c-1: `delete` lifecycle — only for UNDEPLOY (desired
        // removal / zero-worker desired), never for host shutdown
        // (`Stop`): storage must survive restarts. The tenant's own
        // dir is removed wholesale — workerd is dead by now (the
        // drain above only completes when every generation exited),
        // idempotent, and the tenant_id-segmented path cannot reach
        // another tenant's data.
        if self.undeploy {
            // P3c-2: object-store lifecycle cleanup — every class this
            // tenant ever acquired OR was tombstoned for,
            // `do/{uniqueKey}/` prefix only (per-class boundary;
            // idempotent + bounded). RV-R53: undeploy is only ever
            // reached after a signed tenant tombstone, so the union is
            // scoped to confirmed-deleted data.
            let mut cleanup_keys = self.do_class_keys.clone();
            for set in self.do_deletions.values() {
                for k in set {
                    if !k.is_empty() {
                        cleanup_keys.insert(k.clone());
                    }
                }
            }
            // RV-R53.4: route through the durable purge ledger when
            // configured — partial/truncated cleanups stay queued and
            // resume across restarts instead of being lost with the
            // in-memory actor.
            if let Some(purge) = &self.deps.purge {
                for key in &cleanup_keys {
                    purge.queue(&self.tenant_id, "", key);
                }
                let d = purge.drain(
                    crate::do_purge::DRAIN_MAX_ENTRIES,
                    crate::do_purge::DRAIN_MAX_KEYS,
                );
                crate::metrics::registry().set(
                    "vrt_host_do_purge_pending",
                    &[],
                    d.pending as f64,
                );
            }
            if self.deps.purge.is_none() {
            if let Some(snap) = &self.deps.do_snap {
                for key in &cleanup_keys {
                    // P3c-2-r1: a migrated-away or mid-migration class
                    // is NOT ours to delete — a live foreign holder
                    // still reads its placement record + snapshots,
                    // and a reserved dead-marker means a coordinator
                    // owns the handoff. Deleting either splits the
                    // class's fencing/restore state across cells.
                    match snap.placement.record_for_class(key) {
                        Ok(Some(rec))
                            if (!rec.holder.is_empty()
                                && rec.holder != snap.placement.holder())
                                || rec.reserved_for.is_some() =>
                        {
                            tracing::warn!(
                                tenant = %self.tenant_id,
                                class = %key,
                                holder = %rec.holder,
                                "skipping object cleanup — class owned elsewhere or mid-migration"
                            );
                            crate::metrics::registry().inc(
                                "vrt_host_do_cleanup_skipped_total",
                                &[("tenant", &self.tenant_id)],
                                1.0,
                            );
                            continue;
                        }
                        Err(e) => {
                            // Fail closed: an unreadable record must
                            // never let us delete possibly-live data.
                            tracing::warn!(
                                tenant = %self.tenant_id,
                                class = %key,
                                %e,
                                "placement record unreadable; skipping cleanup"
                            );
                            crate::metrics::registry().inc(
                                "vrt_host_do_cleanup_skipped_total",
                                &[("tenant", &self.tenant_id)],
                                1.0,
                            );
                            continue;
                        }
                        _ => {}
                    }
                    match crate::do_snapshot::cleanup_class_objects(&snap.store, key, 10_000) {
                        Ok(o) => {
                            crate::metrics::registry().inc(
                                "vrt_host_do_snap_objects_deleted_total",
                                &[("tenant", &self.tenant_id)],
                                o.deleted as f64,
                            );
                            if !o.complete {
                                crate::metrics::registry().inc(
                                    "vrt_host_do_cleanup_failures_total",
                                    &[("tenant", &self.tenant_id)],
                                    1.0,
                                );
                                tracing::warn!(
                                    tenant = %self.tenant_id,
                                    class = %key,
                                    "do object cleanup partial — no purge ledger configured; retried on next undeploy"
                                );
                            }
                        }
                        Err(e) => {
                            crate::metrics::registry().inc(
                                "vrt_host_do_cleanup_failures_total",
                                &[("tenant", &self.tenant_id)],
                                1.0,
                            );
                            tracing::warn!(
                                tenant = %self.tenant_id,
                                class = %key,
                                %e,
                                "do object cleanup incomplete; retried on next undeploy/host restart"
                            );
                        }
                    }
                }
            }
            }
            if let Some(dir) = self.do_tenant_dir.take() {
                if dir.exists() {
                    match std::fs::remove_dir_all(&dir) {
                        Ok(()) => {
                            crate::metrics::registry().inc(
                                "vrt_host_do_dir_deletes_total",
                                &[("kind", "tenant")],
                                1.0,
                            );
                            tracing::info!(
                                tenant = %self.tenant_id,
                                "do storage tenant dir removed on undeploy"
                            );
                        }
                        Err(e) => {
                            crate::metrics::registry().inc(
                                "vrt_host_do_sweep_failures_total",
                                &[],
                                1.0,
                            );
                            tracing::warn!(
                                tenant = %self.tenant_id,
                                %e,
                                "do storage tenant dir removal failed"
                            );
                        }
                    }
                }
            }
        }
        tracing::info!(tenant = %self.tenant_id, "tenant supervisor stopped");
    }

    // ---- state helpers ----

    fn set_state(&mut self, state: TenantState) {
        if self.state != state {
            self.state = state;
            self.runtime.set_state(state);
            tracing::info!(tenant = %self.tenant_id, ?state, "tenant state changed");
        }
    }

    fn transit(&mut self, event: Transition) {
        match self.state.transition(event) {
            Ok(next) => self.set_state(next),
            Err(error) => {
                tracing::warn!(tenant = %self.tenant_id, %error, "ignored illegal tenant transition");
            }
        }
    }

    fn jitter(&self) -> u64 {
        (self.deps.jitter)(1_000)
    }

    // ---- commands ----

    fn on_cmd(&mut self, cmd: SupervisorCmd) {
        match cmd {
            SupervisorCmd::Apply(spec) => self.on_apply(*spec),
            SupervisorCmd::Stop => self.begin_drain(false),
            SupervisorCmd::Undeploy => self.begin_drain(true),
            SupervisorCmd::Deletions(list) => self.on_deletions(list),
        }
    }

    /// RV-R53: merge the signed tombstone set, queue object-store
    /// purges for named class keys, then let the sweep do the local
    /// work. Tombstones are monotonically unioned — a later revision
    /// that drops a tombstone entry (already consumed by the control
    /// plane) must not resurrect deleted state, and a not-yet-acked
    /// revision always re-carries it.
    fn on_deletions(&mut self, list: Vec<DoDeletion>) {
        for d in list {
            let key = d.class_key.clone().unwrap_or_default();
            self.do_deletions
                .entry(d.worker_id.clone())
                .or_default()
                .insert(key.clone());
            if key.is_empty() {
                // Whole-worker tombstone: enumerate its class dirs so
                // object cleanup is queued for every class key.
                if let (Some(purge), Some(dir)) =
                    (&self.deps.purge, &self.do_tenant_dir)
                {
                    let worker_dir = dir.join(&d.worker_id);
                    if let Ok(entries) = std::fs::read_dir(&worker_dir) {
                        for e in entries.flatten() {
                            let n = e.file_name();
                            if let Some(k) = n.to_str() {
                                if k.len() == 64 && k.bytes().all(|b| b.is_ascii_hexdigit()) {
                                    purge.queue(&self.tenant_id, &d.worker_id, k);
                                }
                            }
                        }
                    }
                }
            } else if let Some(purge) = &self.deps.purge {
                purge.queue(&self.tenant_id, &d.worker_id, &key);
            }
        }
        self.maybe_sweep_do_dirs();
    }

    fn on_apply(&mut self, spec: TenantLaunchSpec) {
        if matches!(self.state, TenantState::Draining | TenantState::Stopped) {
            tracing::warn!(tenant = %self.tenant_id, "ignoring apply while draining/stopped");
            return;
        }
        let digest = spec.config_digest.clone();
        // MIG-4: exclusions + stderr attributions are keyed to ONE
        // desired content — a different digest (fix, revert, worker
        // deletion) re-derives them from scratch.
        if self
            .desired_spec
            .as_ref()
            .is_some_and(|s| s.config_digest != digest)
        {
            self.excluded.clear();
            self.attributed.clear();
            self.runtime.set_excluded_workers(Vec::new());
            self.runtime.set_attributed_workers(Vec::new());
            // L2: a new desired earns a fresh prewarm attempt — the
            // burn flag only suppresses repeats of the SAME content.
            self.prewarm_burned = false;
        }
        // P3c-1: remember the tenant DO dir across applies — a later
        // DO→none spec or tenant undeploy still needs it for the
        // delete lifecycle.
        // P3c-2-r1: the desired DO declaration is the admin path's
        // trusted lookup source — publish BEFORE the launch attempt
        // so a resolve can't observe a stale view.
        self.runtime
            .set_do_storage_view(spec.do_storage.as_ref().map(|d| DoStorageView {
                tenant_dir: d.tenant_dir.clone(),
                worker_classes: d.worker_classes.clone(),
            }));
        if let Some(d) = &spec.do_storage {
            self.do_tenant_dir = Some(d.tenant_dir.clone());
        }
        self.desired_spec = Some(Box::new(spec));
        // Scale-to-zero: a redeploy cancels a hibernation in progress
        // (the Ready logic below then switches normally) and re-arms
        // the idle evaluation — structural blocks are per-content.
        if self.state == TenantState::Hibernating {
            self.abort_hibernate("apply");
        }
        self.hib_next_eval = Instant::now();
        self.hib_backoff.reset();
        match self.state {
            TenantState::Hibernated => {
                // Lazy: no process runs, so nothing to switch — the
                // next wake launches the newest desired. The Apply's
                // freshly assembled dir is unspent again.
                self.attempted_once = false;
            }
            TenantState::Waking => {
                // Restart the in-flight wake launch against the newest
                // desired (same shape as the Starting arm).
                self.abort_pending();
                self.attempted_once = false;
                self.start_pending_launch(false);
            }
            TenantState::Hibernating => {
                // abort_hibernate always leaves Ready; defensive only.
            }
            TenantState::Ready | TenantState::SwitchFailed => {
                if self
                    .active
                    .as_ref()
                    .is_some_and(|g| g.spec.config_digest == digest)
                {
                    // Already serving this exact config — drop any stale
                    // switch in flight.
                    self.abort_pending();
                    if self.state == TenantState::SwitchFailed {
                        // MIG-1: desired reverted to the serving spec —
                        // release the latch; no switch is needed at all.
                        self.switch_fail_streak = 0;
                        self.latched_digest = None;
                        self.transit(Transition::Ready);
                    }
                    return;
                }
                self.abort_pending();
                if self.state == TenantState::SwitchFailed
                    && self.latched_digest.as_deref() == Some(digest.as_str())
                {
                    // MIG-1: the desired content is the exact digest that
                    // already exhausted the switch retry budget — keep the
                    // latch; do not burn a workerd spawn per re-publish of
                    // identical bytes.
                    self.switch_fail_streak = 0;
                    return;
                }
                // A new Apply carrying DIFFERENT desired content re-arms a
                // SwitchFailed tenant — the retry budget is per-content,
                // not per-tenant-lifetime.
                self.switch_fail_streak = 0;
                self.start_pending_launch(false);
            }
            TenantState::Starting => {
                self.abort_pending();
                self.start_pending_launch(true);
            }
            TenantState::Crashed | TenantState::CircuitOpen => {
                // The scheduled retry / half-open attempt picks up the
                // newest spec.
            }
            TenantState::Draining | TenantState::Stopped => {}
        }
    }

    // ---- events ----

    fn on_ev(&mut self, ev: Ev) {
        match ev {
            Ev::Launched { seq, result } => self.on_launched(seq, result),
            Ev::Exited { id } => self.on_exited(id),
            Ev::ProbeResult { gen, seq, ok } => self.on_probe_result(gen, seq, ok),
            Ev::WuProbeResult { gen, seq, verdict } => {
                self.on_wu_probe_result(gen, seq, verdict)
            }
            Ev::PreflightDone {
                seq,
                result,
                elapsed_ms,
            } => self.on_preflight_done(seq, result, elapsed_ms),
            Ev::PrewarmDone { seq, result } => self.on_prewarm_done(seq, result),
            Ev::HibProbeResult { gen, seq, verdict } => {
                self.on_hib_probe_result(gen, seq, verdict)
            }
        }
        self.check_stopped();
    }

    fn on_launched(&mut self, seq: u64, result: Result<PendingGen, SpawnFailure>) {
        let pending = if self.pending.as_ref().is_some_and(|p| p.seq == seq) {
            self.pending.take()
        } else {
            None
        };
        let launched_for = pending
            .as_ref()
            .map(|p| p.desired_digest.clone())
            .unwrap_or_default();
        match result {
            Ok(mut gen) => {
                let Some(p) = pending else {
                    // Superseded attempt that still completed — kill.
                    self.kill_uninstalled(gen);
                    return;
                };
                // P3c-2/W1-r4: the pre-spawn leases ride the pending
                // launch — hand them to the generation so renew/kill
                // -on-loss and the drain-exit release see them. They
                // used to be dropped here with `p`, leaving every
                // LiveGen leaseless: no renewals → the object record
                // expired at TTL under a still-writing gen (dual-write
                // window), and restarts fenced the full leaseMs.
                gen.leases = p.leases;
                self.install_gen(gen, &launched_for);
            }
            Err(failure) => {
                let Some(p) = pending else {
                    return; // stale failure, ignored
                };
                // The attempt is dead — release its leases now rather
                // than letting the retry's acquire fence on the TTL.
                Self::release_leases(&*self.deps.placement, &self.tenant_id, &p.leases);
                if p.prewarm_swap {
                    // MIG-13-r3: a failed swap IS the prewarm
                    // episode's failure — count it and retire the
                    // spent gen dir (`prewarm_burned` was set when the
                    // warm generation was consumed, so the retry is
                    // already on the serialized path).
                    crate::metrics::registry().inc(
                        "vrt_host_prewarm_total",
                        &[("tenant", &self.tenant_id), ("outcome", "failed")],
                        1.0,
                    );
                    self.retire_prewarm_dir(&p.work_dir);
                }
                self.on_launch_failed(failure);
            }
        }
    }

    /// MIG-13: spawn the incoming generation NOW — on the prewarm
    /// twin config (every `disk` service at scratch, `vrt-switch`
    /// marker stripped) under `--watch` — while the old generation
    /// keeps serving. `Ev::PrewarmDone` re-drives the launch. Any
    /// prep failure degrades to the classic serialized launch:
    /// prewarm is an optimization, never a correctness dependency.
    fn start_prewarm(
        &mut self,
        attempt: &TenantLaunchSpec,
        desired: &TenantLaunchSpec,
    ) -> Result<(), String> {
        if attempt.config_path.file_name().and_then(|n| n.to_str()) != Some("config.capnp") {
            return Err("non-assembled spec".into());
        }
        // L3: the optional admission hook may refuse the second
        // process — prewarm is an optimization, never a dependency.
        if let Some(adm) = &self.deps.prewarm_admission {
            if !adm.admit_prewarm(attempt) {
                return Err("prewarm admission refused".into());
            }
        }
        let text = std::fs::read_to_string(&attempt.config_path)
            .map_err(|e| format!("config read: {e}"))?;
        let scratch = attempt.work_dir.join("prewarm-disks");
        let (variant, marker) = vrt_config::check::prewarm_variant(&text, &scratch)
            .map_err(|e| format!("prewarm variant: {e}"))?;
        if attempt.do_storage.is_some() && !marker {
            // A render without the waitUntil probe socket carries no
            // marker (MIG-13-r2) — the swap can never prove reload
            // completion, so the serialized path owns the switch.
            return Err("config lacks the vrt-switch marker".into());
        }
        // H1: the warm process runs as the tenant uid under isolation
        // — the scratch disk dirs and the live config must be
        // reachable by it (same ownership/mode logic as an assembled
        // generation dir), and the secret-bearing config lands 0600
        // via the shared atomic writer, tmp-in-dir + rename.
        crate::assemble::chown_gen_dir(&scratch, attempt.isolation)
            .map_err(|e| format!("prewarm scratch: {e}"))?;
        if let Ok(entries) = std::fs::read_dir(&scratch) {
            for e in entries.flatten() {
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    crate::assemble::chown_gen_dir(&e.path(), attempt.isolation)
                        .map_err(|e| format!("prewarm scratch: {e}"))?;
                }
            }
        }
        let live_config = attempt.work_dir.join("config.live.capnp");
        vrt_securefs::atomic_write(&live_config, variant.as_bytes(), 0o600)
            .map_err(|e| format!("live config write: {e}"))?;
        crate::assemble::chown_isolated(&live_config, attempt.isolation)
            .map_err(|e| format!("live config chown: {e}"))?;
        let mut spec = attempt.clone();
        spec.config_path = live_config.clone();
        spec.extra_args.push("--watch".to_string());
        self.seq += 1;
        let seq = self.seq;
        let ev_tx = self.ev_tx.clone();
        let launcher = self.deps.launcher.clone();
        let probe = (self.deps.probe_factory)();
        let ready_timeout = self.ready_timeout;
        let probe_timeout = self.probe_timeout;
        let kill_grace = self.kill_grace;
        let work_dir = spec.work_dir.clone();
        let task = tokio::spawn(async move {
            let result = launch_one(
                seq,
                Box::new(spec),
                launcher,
                probe,
                ready_timeout,
                probe_timeout,
                kill_grace,
            )
            .await;
            let _ = ev_tx.send(Ev::PrewarmDone { seq, result }).await;
        });
        self.prewarm = Some(PrewarmCtx {
            seq,
            attempt: Box::new(attempt.clone()),
            desired: Box::new(desired.clone()),
            task: Some(task),
            warm: None,
            live_config,
            work_dir,
            started_at: Instant::now(),
            expires_at: Instant::now() + self.prewarm_ttl,
        });
        Ok(())
    }

    /// MIG-13: the prewarmed generation's spawn+ready verdict landed.
    /// Success arms the warm generation and re-drives the serialized
    /// switch — the gate/drain begins only now. Failure aborts the
    /// switch WITHOUT touching the old generation: requests were
    /// never held and the old writer still serves — strictly safer
    /// than the classic path, which learned the same verdict only
    /// after the old generation was dead.
    fn on_prewarm_done(&mut self, seq: u64, result: Result<PendingGen, SpawnFailure>) {
        if !self.prewarm.as_ref().is_some_and(|c| c.seq == seq) {
            // Stale/superseded attempt — a late Ok gen dies on its
            // own kill-on-drop guard; a live newer ctx is untouched.
            return;
        }
        let mut ctx = self.prewarm.take().expect("seq checked above");
        ctx.task = None;
        let elapsed = ctx.started_at.elapsed().as_secs_f64();
        crate::metrics::registry().observe(
            "vrt_host_prewarm_seconds",
            &[("tenant", &self.tenant_id)],
            elapsed,
        );
        match result {
            Ok(gen) => {
                crate::metrics::registry().inc(
                    "vrt_host_prewarm_total",
                    &[("tenant", &self.tenant_id), ("outcome", "ok")],
                    1.0,
                );
                // Module-warm fan-out: one aggregate waitUntil probe
                // compiles every wrapped worker's module graph inside
                // the warm process — DO classes included — without
                // running tenant fetch handlers or touching storage.
                // Best effort: a miss only moves compile cost back
                // onto the first real request.
                if let Some(endpoint) = gen.spec.waituntil_endpoint.clone() {
                    let tenant = self.tenant_id.clone();
                    let budget = self.probe_timeout;
                    tokio::spawn(async move {
                        let hit = tokio::task::spawn_blocking(move || {
                            wu_pending_probe(&endpoint, budget)
                        })
                        .await
                        .ok()
                        .flatten();
                        crate::metrics::registry().inc(
                            "vrt_host_prewarm_warmup_total",
                            &[
                                ("tenant", &tenant),
                                ("outcome", if hit.is_some() { "ok" } else { "failed" }),
                            ],
                            1.0,
                        );
                    });
                }
                let early = self.prewarm_early_maps(&gen);
                let swap_needed = ctx.attempt.do_storage.is_some();
                let attempt = ctx.attempt.clone();
                let desired = ctx.desired.clone();
                ctx.warm = Some(WarmGen {
                    swap_needed,
                    gen,
                    early,
                    warmed_at: Instant::now(),
                });
                self.prewarm = Some(ctx);
                self.launch_after_preflight(attempt, &desired);
            }
            Err(failure) => {
                crate::metrics::registry().inc(
                    "vrt_host_prewarm_total",
                    &[("tenant", &self.tenant_id), ("outcome", "failed")],
                    1.0,
                );
                tracing::warn!(
                    tenant = %self.tenant_id,
                    reason = failure.reason,
                    detail = ?failure.detail,
                    "do switch prewarm failed; old generation keeps serving"
                );
                // MIG-13-r3: a failed prewarm episode burns the flag —
                // the same desired's retries take the serialized path
                // end-to-end instead of re-warming every attempt — and
                // its spent gen dir is retired deterministically.
                self.prewarm_burned = true;
                self.retire_prewarm_dir(&ctx.work_dir);
                // M1-r2: ONE failure path — the classic switch-failure
                // handler owns attribution, the worker-exclusion step,
                // the switch-fail streak/latch, and the active=None
                // circuit accounting. A bespoke path here skipped
                // exclusion of a bad worker and reported inconsistent
                // state when the active generation had already exited.
                self.on_launch_failed(failure);
            }
        }
    }

    /// MIG-13: route sets for workers that cannot reach DO storage
    /// through the binding graph. They flip to the warm generation at
    /// gate-begin — the old writer still drains — and join the hold
    /// set only for the reload itself, which tears in-flight service
    /// instances down.
    fn prewarm_early_maps(&self, gen: &PendingGen) -> EarlyFlip {
        let Some(relaunch) = gen.spec.relaunch.as_ref() else {
            return EarlyFlip::default();
        };
        let tainted = do_tainted_workers(&relaunch.tenant);
        let notify = self.runtime.drain_notify.clone();
        let mk = |eps: &BTreeMap<String, WorkerEndpoint>| {
            Arc::new(
                eps.iter()
                    .filter(|(id, _)| !tainted.contains(id.as_str()))
                    .map(|(id, ep)| {
                        (
                            id.clone(),
                            Arc::new(EndpointSlot {
                                endpoint: ep.clone(),
                                in_flight: AtomicUsize::new(0),
                                closed: AtomicBool::new(false),
                                drain_notify: notify.clone(),
                            }),
                        )
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        EarlyFlip {
            routing: mk(&gen.endpoints),
            qconsumer: mk(&gen.qconsumer_endpoints),
            wfops: mk(&gen.wfops_endpoints),
            cron: mk(&gen.cron_endpoints),
            wfop_tokens: gen
                .spec
                .wfop_tokens
                .iter()
                .filter(|(id, _)| !tainted.contains(id.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    fn install_gen(&mut self, gen: PendingGen, launched_for: &str) {
        // Wanted iff the CURRENT desired still equals the desired this
        // attempt was launched for — a retry's reassembled spec may
        // carry a different stamp than `desired_spec` (T13/H2).
        let wanted = !matches!(self.state, TenantState::Draining | TenantState::Stopped)
            && self
                .desired_spec
                .as_ref()
                .is_some_and(|s| s.config_digest == launched_for);
        if !wanted {
            self.kill_uninstalled(gen);
            return;
        }
        let notify = self.runtime.drain_notify.clone();
        let mk_slots =
            |eps: &BTreeMap<String, WorkerEndpoint>| -> BTreeMap<String, Arc<EndpointSlot>> {
                eps.iter()
                    .map(|(worker, endpoint)| {
                        (
                            worker.clone(),
                            Arc::new(EndpointSlot {
                                endpoint: endpoint.clone(),
                                in_flight: AtomicUsize::new(0),
                                closed: AtomicBool::new(false),
                                drain_notify: notify.clone(),
                            }),
                        )
                    })
                    .collect()
            };
        let qconsumer_slots = Arc::new(mk_slots(&gen.qconsumer_endpoints));
        let wfops_slots = Arc::new(mk_slots(&gen.wfops_endpoints));
        let cron_slots = Arc::new(mk_slots(&gen.cron_endpoints));
        let slots: BTreeMap<String, Arc<EndpointSlot>> = mk_slots(&gen.endpoints);
        let slots = Arc::new(slots);
        let watcher = spawn_exit_watcher(self.seq, gen.child, self.ev_tx.clone());
        let work_dir = gen.spec.work_dir.clone();
        let live = LiveGen {
            id: self.seq,
            pid: gen.pid,
            spec: gen.spec,
            slots,
            qconsumer_slots,
            wfops_slots,
            cron_slots,
            watcher,
            _control: gen.control,
            work_dir,
            leases: gen.leases,
        };
        let had_old = self.active.is_some();
        if let Some(old) = self.active.take() {
            self.enter_draining(old, Instant::now(), false);
        }
        // Atomic route swap AFTER the new generation proved ready — no
        // gap where requests hit a dead process.
        self.runtime.routing.store(live.slots.clone());
        self.runtime
            .qconsumer_routing
            .store(live.qconsumer_slots.clone());
        self.runtime.wfops_routing.store(live.wfops_slots.clone());
        self.runtime
            .wfop_token_routing
            .store(std::sync::Arc::new(live.spec.wfop_tokens.clone()));
        self.runtime.cron_routing.store(live.cron_slots.clone());
        if self.seg_dir.is_none() {
            self.seg_dir = live.work_dir.parent().map(|p| p.to_path_buf());
        }
        // MIG-11-r2: the serving marker goes up only AFTER the full
        // route map is published — a resolver that misses between the
        // store and this flag still sees `serving == false` and
        // reports not-ready rather than a false "not declared" 404.
        self.runtime.set_serving(true);
        // W4-r2: routes are live — release held requests now, not at
        // the end of install bookkeeping.
        self.runtime.switch_gate.end();
        self.runtime
            .set_serving_digest(live.spec.config_digest.clone());
        self.runtime.mark_served();
        self.runtime.set_active_pid(live.pid);
        // MIG-4: publish which desired workers this generation does
        // NOT serve (empty for a full generation) — readyz reports
        // them as `config-load` tenant failures. Attribution clears:
        // the exclusions carry the reporting from here.
        self.runtime
            .set_excluded_workers(self.excluded.iter().cloned().collect());
        self.attributed.clear();
        self.runtime.set_attributed_workers(Vec::new());
        self.active = Some(live);
        // PT1: new process, new detector — cpu_prev and failure
        // streaks never carry across generation switches. PT1-r2: a
        // stale in-flight probe must not carry either (its result is
        // gen-tagged and ignored, but the flag must not block probes
        // for the new generation).
        self.runaway = None;
        self.runaway_kill = None;
        self.probe_in_flight = None;
        self.runtime.stats.readies.fetch_add(1, Ordering::Relaxed);
        // B12 instance metrics.
        let r = crate::metrics::registry();
        r.inc(
            "vrt_host_launches_total",
            &[("outcome", "ready"), ("reason", "none")],
            1.0,
        );
        if had_old {
            self.runtime.stats.switch_ok.fetch_add(1, Ordering::Relaxed);
            r.inc(
                "vrt_host_generation_switches_total",
                &[("outcome", "ok")],
                1.0,
            );
        }
        // P3c-1: serialized DO switch completed — account the bounded
        // wait + bump the switch counter (`had_old` is false here
        // because the old generation was drained pre-launch).
        if let Some(since) = self.do_wait_since.take() {
            self.runtime.stats.switch_ok.fetch_add(1, Ordering::Relaxed);
            r.inc(
                "vrt_host_generation_switches_total",
                &[("outcome", "ok")],
                1.0,
            );
            let waited = since.elapsed().as_secs_f64();
            r.observe(
                "vrt_host_do_switch_wait_seconds",
                &[("tenant", &self.tenant_id)],
                waited,
            );
            // MIG-11: last serialized-switch stage — old writer's exit
            // → new generation installed-and-serving.
            let exit_to_ready = self
                .do_switch_exit_at
                .take()
                .map(|t| t.elapsed().as_secs_f64());
            if let Some(v) = exit_to_ready {
                r.observe(
                    "vrt_host_do_switch_stage_seconds",
                    &[("tenant", &self.tenant_id), ("stage", "exit_to_ready")],
                    v,
                );
            }
            tracing::info!(
                tenant = %self.tenant_id,
                wait_ms = (waited * 1000.0) as u64,
                exit_to_ready_ms = exit_to_ready.map(|v| (v * 1000.0) as u64),
                "serialized DO switch complete"
            );
        }
        self.maybe_sweep_do_dirs();
        self.window.reset();
        self.backoff.reset();
        self.switch_fail_streak = 0;
        self.latched_digest = None;
        self.half_open = false;
        self.cooldown_until = None;
        self.retry_at = None;
        self.runtime.set_last_error(None);
        self.sweep_gen_dirs();
        if self.state != TenantState::Ready {
            self.transit(Transition::Ready);
        }
        // Scale-to-zero: residency anchor for the idle policy, and the
        // wake (if this install ended one) resolves only now — routes
        // are published and the state is Ready, so a waiter that
        // resumes resolves a serving generation.
        self.ready_since = Instant::now();
        self.hib_next_eval = Instant::now();
        self.hib_backoff.reset();
        self.finish_wake_ok();
        tracing::info!(
            tenant = %self.tenant_id,
            gen = self.seq,
            pid = self.active.as_ref().map(|g| g.pid).unwrap_or(0),
            "generation installed"
        );
    }

    /// Terminate a generation that never got installed (stale apply or
    /// drain during launch).
    fn kill_uninstalled(&mut self, mut gen: PendingGen) {
        // Any leases this gen acquired die with it — the writer is
        // being killed, never installed.
        Self::release_leases(&*self.deps.placement, &self.tenant_id, &gen.leases);
        let pid = gen.pid;
        let work_dir = gen.spec.work_dir.clone();
        let kill_grace = self.kill_grace;
        tokio::spawn(async move {
            terminate_child(&mut gen.child.inner, pid, kill_grace).await;
            let _ = std::fs::remove_dir_all(&work_dir);
        });
    }

    /// T13/H2: is `dir` still needed by anything live or about to
    /// launch? Generation dirs are per-attempt, so cleanup must never
    /// remove a directory a surviving generation or in-flight launch
    /// still references (e.g. the no-assembler identical-spec retry).
    fn dir_referenced(&self, dir: &std::path::Path) -> bool {
        let desired_reuses = self.deps.assembler.is_none()
            && self
                .desired_spec
                .as_ref()
                .is_some_and(|s| s.work_dir == dir);
        desired_reuses
            || self.active.as_ref().is_some_and(|g| g.work_dir == dir)
            || self.pending.as_ref().is_some_and(|p| p.work_dir == dir)
            || self.prewarm.as_ref().is_some_and(|p| p.work_dir == dir)
            || self.draining.iter().any(|g| g.work_dir == dir)
    }

    /// MIG-13-r3: the ONE teardown for every path that abandons a
    /// prewarmed generation — ttl expiry, parked-warm death,
    /// `abort_pending`, a failed swap and a failed `!swap_needed`
    /// install all land here instead of re-implementing the route
    /// cleanup. Semantics mirror `warm_dead`/the old `abort_pending`
    /// inline block: with a live generation the published maps belong
    /// to it and stay (the old generation keeps serving); with none,
    /// the only maps that can be live are early-flip slots (or a dead
    /// generation's stale map), so
    ///   - `serving` drops FIRST — no resolver may observe
    ///     "serving + empty map" (the false-404 window),
    ///   - every published slot is CLOSED — a resolver that already
    ///     holds its `Arc` can no longer acquire, so nothing new lands
    ///     on a killed socket,
    ///   - the maps empty and the gate releases held requests into the
    ///     typed `NoServing` surface (fail closed: with the switch
    ///     aborted there is nothing left to hold for — the serialized
    ///     fallback still runs, but worst-case drain+relaunch far
    ///     exceeds a request's hold budget, so requests get the
    ///     retryable 503 now instead of burning it).
    fn revoke_early_routes(&mut self) {
        if self.active.is_some() {
            return;
        }
        self.runtime.set_serving(false);
        for map in [
            &self.runtime.routing,
            &self.runtime.qconsumer_routing,
            &self.runtime.wfops_routing,
            &self.runtime.cron_routing,
        ] {
            for slot in map.load().values() {
                slot.close();
            }
            map.store(Arc::new(BTreeMap::new()));
        }
        self.runtime
            .wfop_token_routing
            .store(Arc::new(BTreeMap::new()));
        self.runtime.switch_gate.end();
    }

    /// MIG-13-r3: deterministic cleanup for an abandoned prewarm
    /// episode's gen dir — called AFTER the warm process is dead or
    /// SIGKILLed. An unreferenced dir is removed outright (the retry
    /// reassembles a FRESH `gen-<n>` dir rather than reusing the warm
    /// one's leftover sockets/scratch — the r2 reuse could hit stale
    /// `*.sock` files and EADDRINUSE, or lose the dir to
    /// `sweep_gen_dirs` mid-respawn). A dir the no-assembler retry
    /// will reuse keeps its spec files — only the episode's own
    /// artifacts (twin/live/preflight configs, scratch disks, stale
    /// socket files) are stripped so the next spawn binds clean.
    fn retire_prewarm_dir(&self, dir: &std::path::Path) {
        if !self.dir_referenced(dir) {
            let _ = std::fs::remove_dir_all(dir);
            return;
        }
        let _ = std::fs::remove_dir_all(dir.join("prewarm-disks"));
        let _ = std::fs::remove_dir_all(dir.join("preflight-disks"));
        let _ = std::fs::remove_file(dir.join("config.live.capnp"));
        let _ = std::fs::remove_file(dir.join("config.preflight.capnp"));
        if let Ok(entries) = std::fs::read_dir(dir.join("sock")) {
            for e in entries.flatten() {
                if e.file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with(".sock"))
                {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    }

    fn on_launch_failed(&mut self, failure: SpawnFailure) {
        let now = Instant::now();
        self.runtime
            .stats
            .launch_fails
            .fetch_add(1, Ordering::Relaxed);
        crate::metrics::registry().inc(
            "vrt_host_launches_total",
            &[("outcome", "failed"), ("reason", failure.reason)],
            1.0,
        );
        // MIG-11-r2: when the old generation was already drained
        // (`active` taken by the serialized-switch path) this failure
        // leaves NOTHING serving — the marker drops BEFORE the gate
        // releases waiters so their misses resolve `NoServing` (typed
        // 503), never the false "not declared" 404 that used to lose
        // cron triggers outright.
        if self.active.is_none() {
            // MIG-13-r3: with nothing installed, any published map can
            // only be a prewarmed generation's early-flip set — a
            // failed `!swap_needed` install left them routable to the
            // just-killed warm process. The one helper clears them
            // (serving=false first, slots closed, gate released).
            self.revoke_early_routes();
        }
        // W4-r2: a failed switch launch leaves NO serving generation
        // (the old one was already drained) — release held requests
        // so they 503 now instead of burning their full budget.
        self.runtime.switch_gate.end();
        // MIG-11: a failed switch has no "ready" — drop the
        // exit→ready anchor so a later unrelated install cannot
        // record a bogus multi-day stage.
        self.do_switch_exit_at = None;
        // T25/M2: EVERY launch/ready failure gets a structured warn —
        // a silent workerd (real workerd exits quietly on config
        // rejection) must still leave the typed reason. The sanitized
        // stderr tail rides along when present.
        match &failure.detail {
            Some(detail) => tracing::warn!(
                tenant = %self.tenant_id,
                reason = failure.reason,
                detail = %detail,
                "launch failed; workerd stderr tail"
            ),
            None => tracing::warn!(
                tenant = %self.tenant_id,
                reason = failure.reason,
                "launch failed"
            ),
        }
        // M2: last_error carries the reason even without a stderr
        // tail — `vrt status` must never read a bare None.
        self.runtime.set_last_error(Some(match &failure.detail {
            Some(d) => format!("{}: {}", failure.reason, d),
            None => failure.reason.to_string(),
        }));
        // MIG-4: mine the diagnostic for `service worker-<id>`
        // attribution — reporting only; a forged name can never move
        // workers outside this tenant's desired set.
        if let Some(detail) = &failure.detail {
            self.record_attribution(detail);
        }
        if failure.reason == "timeout" {
            self.runtime
                .stats
                .ready_timeouts
                .fetch_add(1, Ordering::Relaxed);
        }
        if self.state == TenantState::Waking {
            // Scale-to-zero: a failed WAKE launch never trips into the
            // crash/backoff machinery silently — bounded retry, then a
            // typed failure to the waiters (see `on_wake_launch_failed`).
            self.on_wake_launch_failed(failure.reason, now);
            return;
        }
        if self.half_open {
            // The single half-open attempt failed — reopen the circuit.
            self.half_open = false;
            self.window.reset();
            self.cooldown_until = Some(now + self.circuit_cooldown);
            self.set_state(TenantState::CircuitOpen);
            tracing::warn!(
                tenant = %self.tenant_id,
                reason = failure.reason,
                "half-open attempt failed; circuit reopened"
            );
            return;
        }
        let count = self.window.record(now);
        let tripped = count >= self.fail_threshold;
        if self.active.is_some() {
            // A failed switch never trips the circuit: the old generation
            // keeps serving and we retry with backoff — but BOUNDED
            // (T13/M4): after `max_switch_failures` consecutive failures
            // the tenant latches `SwitchFailed`, retries stop, and a new
            // Apply re-arms. Previously this retried forever silently.
            self.runtime
                .stats
                .switch_fail
                .fetch_add(1, Ordering::Relaxed);
            crate::metrics::registry().inc(
                "vrt_host_generation_switches_total",
                &[("outcome", "failed")],
                1.0,
            );
            self.switch_fail_streak += 1;
            if self.switch_fail_streak >= self.max_switch_failures {
                self.retry_at = None;
                self.runtime
                    .stats
                    .switch_latched
                    .fetch_add(1, Ordering::Relaxed);
                // MIG-1: key the latch by the desired content digest — an
                // Apply of the same bytes stays latched, any other digest
                // (fix, revert, deletion) re-arms.
                self.latched_digest =
                    self.desired_spec.as_ref().map(|s| s.config_digest.clone());
                self.transit(Transition::SwitchFailed);
                // MIG-7: the sanitized stderr tail rides along — the
                // 2026-11-16 incident needed pod-shell access only
                // because this line dropped `failure.detail` (the same
                // bounded tail `last_error` already carries).
                match &failure.detail {
                    Some(detail) => tracing::warn!(
                        tenant = %self.tenant_id,
                        reason = failure.reason,
                        streak = self.switch_fail_streak,
                        detail = %detail,
                        "switch retry budget exhausted; latched SwitchFailed, old generation keeps serving; workerd stderr tail"
                    ),
                    None => tracing::warn!(
                        tenant = %self.tenant_id,
                        reason = failure.reason,
                        streak = self.switch_fail_streak,
                        "switch retry budget exhausted; latched SwitchFailed, old generation keeps serving"
                    ),
                }
                return;
            }
            self.retry_at = Some(now + self.backoff.next_delay(self.jitter()));
            self.transit(Transition::LaunchFailed);
            match &failure.detail {
                Some(detail) => tracing::warn!(
                    tenant = %self.tenant_id,
                    reason = failure.reason,
                    streak = self.switch_fail_streak,
                    count,
                    detail = %detail,
                    "generation switch failed; old generation keeps serving; workerd stderr tail"
                ),
                None => tracing::warn!(
                    tenant = %self.tenant_id,
                    reason = failure.reason,
                    streak = self.switch_fail_streak,
                    count,
                    "generation switch failed; old generation keeps serving"
                ),
            }
            return;
        }
        self.sweep_gen_dirs();
        if tripped {
            self.cooldown_until = Some(now + self.circuit_cooldown);
            self.runtime
                .stats
                .circuit_opens
                .fetch_add(1, Ordering::Relaxed);
            self.transit(Transition::CircuitTrip);
            tracing::warn!(
                tenant = %self.tenant_id,
                count,
                reason = failure.reason,
                "failure threshold reached; circuit open"
            );
        } else {
            self.transit(Transition::LaunchFailed);
            self.retry_at = Some(now + self.backoff.next_delay(self.jitter()));
        }
    }

    fn on_exited(&mut self, id: u64) {
        if self.active.as_ref().is_some_and(|g| g.id == id) {
            // Scale-to-zero: the serving generation died while a
            // hibernation was quiescing — that is a REAL crash (the
            // expected-exit path goes through the draining branch), so
            // return to Ready first and let the crash accounting below
            // run unchanged.
            if self.state == TenantState::Hibernating {
                self.hib = None;
                self.transit(Transition::HibernateAbort);
            }
            let gen = self.active.take().expect("checked above");
            gen.watcher.abort();
            let now = Instant::now();
            // PT1: per-process detector state dies with the gen; a
            // watchdog-ordered exit takes the immediate retry (its
            // replacement starts while the kill is still reaping).
            self.runaway = None;
            self.probe_in_flight = None;
            let runaway_exit = self.runaway_kill.take().is_some();
            self.runtime.stats.crashes.fetch_add(1, Ordering::Relaxed);
            // MIG-11-r2: `serving` drops BEFORE the gate releases
            // waiters and BEFORE the maps empty — a resolver can then
            // never observe "serving + empty routes" and answer the
            // false "not declared" 404; the overlap window resolves
            // `NoServing` instead.
            self.runtime.set_serving(false);
            // W4-r2: a mid-switch crash abandons the window — release.
            self.runtime.switch_gate.end();
            // MIG-11: the abandoned window's stage anchor must not
            // leak into a later install's exit→ready observation.
            self.do_switch_exit_at = None;
            self.runtime.routing.store(Arc::new(BTreeMap::new()));
            self.runtime
                .qconsumer_routing
                .store(Arc::new(BTreeMap::new()));
            self.runtime.wfops_routing.store(Arc::new(BTreeMap::new()));
            self.runtime
                .wfop_token_routing
                .store(Arc::new(BTreeMap::new()));
            self.runtime.cron_routing.store(Arc::new(BTreeMap::new()));
            let count = self.window.record(now);
            // The runaway streak fuses the same breaker: `window`
            // resets on Ready, `runaway_window` does not.
            if count >= self.fail_threshold || std::mem::take(&mut self.runaway_breaker_pending) {
                self.cooldown_until = Some(now + self.circuit_cooldown);
                self.runtime
                    .stats
                    .circuit_opens
                    .fetch_add(1, Ordering::Relaxed);
                self.transit(Transition::Crash);
                self.transit(Transition::CircuitTrip);
                tracing::warn!(
                    tenant = %self.tenant_id,
                    count,
                    "crash threshold reached; circuit open"
                );
            } else {
                self.transit(Transition::Crash);
                // PT1: watchdog-ordered exits skip backoff — a spin
                // tenant's replacement must take over immediately.
                self.retry_at = Some(if runaway_exit {
                    now
                } else {
                    now + self.backoff.next_delay(self.jitter())
                });
                tracing::warn!(tenant = %self.tenant_id, gen = id, runaway = runaway_exit, "serving generation crashed");
            }
            for slot in gen.slots.values() {
                slot.close();
            }
            // The work dir is removed only when nothing references it:
            // per-attempt reassembly gives retries a fresh dir, but the
            // no-assembler fallback reuses this one.
            if !self.dir_referenced(&gen.work_dir) {
                let _ = std::fs::remove_dir_all(&gen.work_dir);
            }
            self.sweep_gen_dirs();
            return;
        }
        if let Some(pos) = self.draining.iter().position(|g| g.id == id) {
            let gen = self.draining.remove(pos);
            // MIG-11: `SIGTERM → exit` stage + `exit → ready` anchor
            // (consumed by install_gen).
            if gen.do_switch {
                if let Some(t) = gen.sigterm_at {
                    crate::metrics::registry().observe(
                        "vrt_host_do_switch_stage_seconds",
                        &[("tenant", &self.tenant_id), ("stage", "sigterm_to_exit")],
                        t.elapsed().as_secs_f64(),
                    );
                }
                self.do_switch_exit_at = Some(Instant::now());
            }
            gen.watcher.abort();
            // P3c-2/W1-r4: the exit watcher proved this writer DEAD —
            // its leases can never dual-write again. DO1-r2 P0-1/P0-2:
            // departing classes get their `Final` snapshot NOW (post-
            // exit ⇒ alarm/in-flight writes are included); a class the
            // desired spec TRANSFERS away keeps its lease until the
            // commit lands — released early, the target could fence in
            // and restore a stale periodic baseline over newer data.
            // Ordering is strict: writer dead → Final commit → fence,
            // then `start_pending_launch` sees the markers immediately.
            let held = self.final_snapshot_departing(&gen.leases);
            // MIG-17: serialized-switch lease carry. A class the
            // desired spec still wants under the SAME worker keeps
            // its lease across the switch — the placement record
            // still names this holder, so releasing + re-acquiring
            // would burn two CAS writes (plus fencing the epoch the
            // carry could have kept) for zero fencing benefit. The
            // carry is restricted to `gen.do_switch` (a serialized
            // switch where the old writer provably died before the
            // pending launch opens dirs) — crash/restart paths keep
            // the classic release-everything order. Carried leases
            // renew via `service_do_leases` until the launch consumes
            // them; EVERYTHING else (held-departing, transfer,
            // delete) takes the original path untouched.
            let mut carried: Vec<DoLeaseEntry> = Vec::new();
            let release: Vec<DoLeaseEntry> = gen
                .leases
                .into_iter()
                .filter(|l| {
                    if held.iter().any(|h| {
                        h.scope.worker == l.scope.worker && h.scope.class_key == l.scope.class_key
                    }) {
                        return false;
                    }
                    if gen.do_switch
                        && self.do_wait_since.is_some()
                        && self.desired_still_wants(&l.scope)
                    {
                        carried.push(l.clone());
                        return false;
                    }
                    true
                })
                .collect();
            Self::release_leases(&*self.deps.placement, &self.tenant_id, &release);
            self.held_departing.extend(held);
            self.carried_leases.extend(carried);
            if !self.dir_referenced(&gen.work_dir) {
                let _ = std::fs::remove_dir_all(&gen.work_dir);
            }
            // P3c-1: serialized DO switch — old generation is fully
            // dead; the pending launch may now open the class dirs.
            if self.draining.is_empty() && self.do_wait_since.is_some() {
                self.start_pending_launch(false);
            }
        }
        self.sweep_gen_dirs();
        // Scale-to-zero: a wake waits for the hibernated generation to
        // be fully reaped (same work dir / sockets) — resume it now.
        if self.draining.is_empty() && self.wake_run.is_some() {
            self.service_wake();
        }
        // Otherwise a stale/killed generation's exit — already accounted.
    }

    // ---- timers ----

    fn on_tick(&mut self) {
        let now = Instant::now();
        // Scale-to-zero hooks (all inert while `s2z` is None).
        if self.hib.is_some() {
            self.service_hibernate(now);
        }
        if self.wake_run.as_ref().is_some_and(|w| now >= w.deadline) {
            self.on_wake_deadline();
        }
        self.evaluate_idle(now);
        if self.state == TenantState::Ready && now >= self.next_monitor {
            self.next_monitor = now + self.monitor_interval;
            self.sample_active(now);
        }
        if matches!(
            self.state,
            TenantState::Crashed | TenantState::Starting | TenantState::Ready
        ) && self.pending.is_none()
            && self.retry_at.is_some_and(|t| now >= t)
        {
            self.retry_at = None;
            self.transit(Transition::RetryBegin);
            self.start_pending_launch(true);
        }
        if self.state == TenantState::CircuitOpen && self.cooldown_until.is_some_and(|t| now >= t) {
            self.cooldown_until = None;
            self.half_open = true;
            self.runtime
                .stats
                .half_open_attempts
                .fetch_add(1, Ordering::Relaxed);
            self.transit(Transition::CooldownEnd);
            self.start_pending_launch(true);
        }
        // MIG-13-r2/H2: the parked warm generation has no exit
        // watcher — its child lives inside `prewarm` so the swap can
        // still terminate it — so this tick poll IS its death
        // detection. A dead warm process must never keep serving
        // early-flip routes.
        let warm_dead = self
            .prewarm
            .as_mut()
            .and_then(|ctx| ctx.warm.as_mut())
            .and_then(|w| w.gen.child.inner.try_wait().ok().flatten())
            .is_some();
        if warm_dead {
            tracing::warn!(
                tenant = %self.tenant_id,
                "prewarmed generation exited while parked"
            );
            // Drop the ctx — the kill-on-drop guard is a no-op on an
            // already-dead group; the gen dir is retired below once
            // unreferenced.
            let ctx = self.prewarm.take();
            // MIG-13-r3: a parked-then-dead warm generation is a
            // FAILED prewarm episode — count it, burn the flag so the
            // same desired retries on the serialized path, and revoke
            // every early-flip route BEFORE a held request can resolve
            // to the dead socket (fail closed — see `revoke_early_routes`).
            self.prewarm_burned = true;
            crate::metrics::registry().inc(
                "vrt_host_prewarm_total",
                &[("tenant", &self.tenant_id), ("outcome", "failed")],
                1.0,
            );
            self.revoke_early_routes();
            if let Some(ctx) = ctx {
                self.retire_prewarm_dir(&ctx.work_dir);
            }
            // H2-r2: the SAME failure handling as a launch failure —
            // a still-serving old generation keeps its routes and the
            // bounded streak/latch applies; with none live this is
            // the fail-closed circuit path.
            self.on_launch_failed(SpawnFailure {
                reason: "warm-exited",
                detail: None,
            });
        }
        // MIG-13-r2/L2: the whole prewarm episode is capped — a warm
        // process (or in-flight spawn) older than `prewarm_ttl` is
        // killed and the switch continues on the serialized path.
        // MIG-13-r3: NEVER inside the switch-hold window — once the
        // gate is open the armed warm generation is load-bearing (its
        // early-flip routes are live); killing it mid-drain pointed
        // those routes at a dead socket (the r2 502). The episode is
        // still bounded: `drain_timeout` caps the gate window and the
        // swap's own deadlines bound it after consume.
        if self
            .prewarm
            .as_ref()
            .is_some_and(|ctx| now >= ctx.expires_at)
            && !self.runtime.switch_gate.active()
        {
            let mut ctx = self.prewarm.take().expect("expires checked");
            if let Some(t) = ctx.task.take() {
                t.abort();
            }
            let work_dir = ctx.work_dir.clone();
            // `ctx.warm` drops here — the GroupChild guard SIGKILLs
            // the parked process group.
            drop(ctx);
            self.prewarm_burned = true;
            crate::metrics::registry().inc(
                "vrt_host_prewarm_total",
                &[("tenant", &self.tenant_id), ("outcome", "expired")],
                1.0,
            );
            // MIG-13-r3: early-flip routes may already be live —
            // revoke them (fail closed) and retire the spent gen dir
            // deterministically, same as every other abandon path.
            self.revoke_early_routes();
            self.retire_prewarm_dir(&work_dir);
            tracing::warn!(
                tenant = %self.tenant_id,
                ttl_ms = self.prewarm_ttl.as_millis() as u64,
                "prewarm expired; falling back to serialized switch"
            );
            // Same-turn retry — the serialized path owns the switch.
            self.retry_at = Some(now);
        }
        self.service_do_leases(now);
        self.service_do_snapshots();
        self.service_do_wal();
        self.service_departing_snapshots();
        self.service_draining(now);
        self.check_stopped();
    }

    fn sample_active(&mut self, now: Instant) {
        let Some(active) = &self.active else { return };
        let active_pid = active.pid;
        let active_gen_id = active.id;
        let limits = active.spec.limits;
        let rw_cfg = limits.runaway;
        let hb_ep = active.spec.heartbeat_endpoint.clone();
        let usage = self.deps.sampler.sample(active_pid);
        if let Some(rss) = usage.rss_bytes {
            self.runtime
                .stats
                .last_rss_bytes
                .store(rss, Ordering::Relaxed);
        }
        if let Some(fds) = usage.fds {
            self.runtime.stats.last_fds.store(fds, Ordering::Relaxed);
        }
        let over_limit = over_limit(&usage, &limits);
        // P3c-1: bounded DO storage stats + lifecycle sweep on the
        // monitor cadence (idempotent; stat entries capped).
        if self
            .active
            .as_ref()
            .is_some_and(|g| g.spec.do_storage.is_some())
            || self.do_tenant_dir.is_some()
        {
            self.stat_do_dirs();
            self.maybe_sweep_do_dirs();
        }
        if over_limit && self.pending.is_none() {
            self.runtime
                .stats
                .over_limit_restarts
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                tenant = %self.tenant_id,
                gen = active_gen_id,
                "resource limit exceeded; starting graceful restart"
            );
            self.start_pending_launch(false);
        }

        // PT1/PT1-r2 runaway watchdog — dual-signal kill. Only
        // evaluated while Ready (monitor tick); single-signal states
        // are metrics, never kills. SIGKILL the whole group: a
        // spinning event loop cannot drain, and the crash path owns
        // the reap + breaker accounting + immediate replacement.
        // Probes run OFF the actor (spawn_blocking + total-budget
        // timeout, result as Ev::ProbeResult) so a slow wedge never
        // stalls this tenant's commands — or, with many runaway
        // tenants, the runtime's worker threads.
        if self.runaway_kill.is_some() {
            self.check_runaway_kill_stuck(now);
        } else if rw_cfg.is_some() && hb_ep.is_some() {
            let rw_cfg = rw_cfg.unwrap();
            let hb_ep = hb_ep.unwrap();
            let verdict = self
                .runaway
                .get_or_insert_with(|| crate::runaway::RunawayDetector::new(rw_cfg))
                .observe_cpu(now, usage.cpu_seconds);
            self.runtime
                .stats
                .runaway_last_verdict
                .store(verdict.code(), Ordering::Relaxed);
            if matches!(verdict, crate::runaway::RunawayVerdict::CpuUnavailable) {
                self.runtime
                    .stats
                    .runaway_cpu_unavailable
                    .fetch_add(1, Ordering::Relaxed);
            }
            if verdict == crate::runaway::RunawayVerdict::Runaway {
                self.runtime
                    .stats
                    .runaway_kills
                    .fetch_add(1, Ordering::Relaxed);
                // Repeated runaways inside fail_window must fuse the
                // breaker — the crash window alone resets on every
                // successful reinstall, so the streak lives in its
                // own window.
                if self.runaway_window.record(now) >= self.fail_threshold {
                    self.runaway_breaker_pending = true;
                }
                tracing::warn!(
                    tenant = %self.tenant_id,
                    gen = active_gen_id,
                    pid = active_pid,
                    "runaway tenant: sustained CPU saturation + dead event loop; SIGKILL process group"
                );
                crate::launch::signal_process_group(active_pid, libc::SIGKILL);
                self.runaway_kill = Some(RunawayKillMark {
                    gen: active_gen_id,
                    pid: active_pid,
                    since: now,
                    resends: 0,
                });
                // No pending launch here: the SIGKILLed generation's
                // exit event drives the normal crash path — the kill
                // marker makes its retry immediate rather than
                // backoff-paced.
            } else {
                // PT1-r2: an in-flight probe whose result never lands
                // (task lost, channel full, join error) must not gate
                // detection forever — expiry counts as a failed probe
                // (fail closed) and frees the slot.
                if let Some((gen, seq, since)) = self.probe_in_flight {
                    if gen == active_gen_id
                        && now.duration_since(since) >= rw_cfg.heartbeat_timeout * 2
                    {
                        self.probe_in_flight = None;
                        let _ = seq;
                        if let Some(det) = &mut self.runaway {
                            det.note_probe(false);
                        }
                        self.runtime
                            .stats
                            .runaway_heartbeat_failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                // CPU gate: probe only when the detector already has a
                // saturation candidate (or an open failure streak).
                let should = self.runaway.as_ref().is_some_and(|d| d.should_probe());
                if should
                    && self.probe_in_flight.is_none()
                    && tokio::runtime::Handle::try_current().is_ok()
                {
                    self.probe_seq += 1;
                    let seq = self.probe_seq;
                    self.probe_in_flight = Some((active_gen_id, seq, now));
                    self.runtime
                        .stats
                        .runaway_probes
                        .fetch_add(1, Ordering::Relaxed);
                    let probe = self.deps.heartbeat.clone();
                    let tx = self.ev_tx.clone();
                    let budget = rw_cfg.heartbeat_timeout;
                    tokio::spawn(async move {
                        let ok = match tokio::time::timeout(
                            budget,
                            tokio::task::spawn_blocking(move || probe.probe(&hb_ep, budget)),
                        )
                        .await
                        {
                            // Join error or probe panic — fail closed.
                            Ok(r) => r.unwrap_or(false),
                            Err(_) => false,
                        };
                        let _ = tx.try_send(Ev::ProbeResult {
                            gen: active_gen_id,
                            seq,
                            ok,
                        });
                    });
                }
            }
        }
    }

    /// PT1-r2: the probe landed — clear the in-flight slot for THIS
    /// probe and fold the result into the streak, but only for the
    /// generation it actually probed: a stale result must never
    /// poison the fresh detector of a replacement generation.
    fn on_probe_result(&mut self, gen: u64, seq: u64, ok: bool) {
        if matches!(self.probe_in_flight, Some((g, s, _)) if g == gen && s == seq) {
            self.probe_in_flight = None;
        }
        if self.active.as_ref().is_some_and(|g| g.id == gen) {
            if !ok {
                self.runtime
                    .stats
                    .runaway_heartbeat_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let Some(det) = &mut self.runaway {
                det.note_probe(ok);
            }
        }
    }

    /// PT1-r2: a watchdog-ordered kill must converge (the exit event
    /// owns the crash path). If it doesn't — lost event, recycled
    /// pid, unkillable D-state — re-SIGKILL a bounded number of times,
    /// then release the marker so detection resumes. The breaker
    /// streak was already recorded at the first kill.
    fn check_runaway_kill_stuck(&mut self, now: Instant) {
        let Some(mark) = self.runaway_kill else {
            return;
        };
        // Bounded by 2 monitor ticks — long enough for a real exit
        // event to beat it by a wide margin; 10s floor keeps tiny
        // test intervals honest.
        let limit = (self.monitor_interval * 2).max(Duration::from_secs(10));
        if now.duration_since(mark.since) < limit {
            return;
        }
        // Never signal a pid the kernel may have recycled: resend
        // only while the ACTIVE generation is still the exact (gen,
        // pid) the kill was ordered on.
        let same = self
            .active
            .as_ref()
            .is_some_and(|g| g.id == mark.gen && g.pid == mark.pid);
        self.runtime
            .stats
            .runaway_kill_stuck
            .fetch_add(1, Ordering::Relaxed);
        if !same || mark.resends >= RUNAWAY_KILL_MAX_RESENDS {
            tracing::error!(
                tenant = %self.tenant_id,
                gen = mark.gen,
                pid = mark.pid,
                resends = mark.resends,
                same,
                "runaway kill not converged; releasing watchdog marker, detection resumes"
            );
            self.runaway_kill = None;
            return;
        }
        tracing::error!(
            tenant = %self.tenant_id,
            gen = mark.gen,
            pid = mark.pid,
            resends = mark.resends,
            "runaway generation still live after SIGKILL; resending"
        );
        crate::launch::signal_process_group(mark.pid, libc::SIGKILL);
        if let Some(m) = &mut self.runaway_kill {
            m.resends += 1;
            m.since = now;
        }
    }

    /// P3c-1 `delete` stage: sweep stale entries under the tenant DO
    /// dir — class dirs no live spec declares (deleted_classes or
    /// DO→none) and worker dirs of workers no longer in the spec.
    /// Invariants: never runs while a DO-capable generation is still
    /// draining (its sqlite files would be deleted under it); keep-set
    /// is the SERVING spec's declared uniqueKeys — never desired's, so
    /// a failed switch never orphans the live classes' data. Entries
    /// named `<uniqueKey>.epoch` are the P3c-2 placement leases —
    /// kept iff their class survives. Idempotent; failures are
    /// metric'd and retried every monitor tick.
    fn maybe_sweep_do_dirs(&mut self) {
        if self.state != TenantState::Ready || self.draining.iter().any(|g| g.had_do_storage) {
            return;
        }
        let Some(dir) = self.do_tenant_dir.clone() else {
            return;
        };
        let Some(active) = &self.active else {
            return;
        };
        let live_workers: std::collections::BTreeSet<&str> = active
            .spec
            .worker_endpoints
            .keys()
            .map(|s| s.as_str())
            .collect();
        let keep: &std::collections::BTreeMap<String, Vec<String>> = active
            .spec
            .do_storage
            .as_ref()
            .map(|d| &d.worker_classes)
            .unwrap_or_else(|| {
                static EMPTY: std::sync::OnceLock<std::collections::BTreeMap<String, Vec<String>>> =
                    std::sync::OnceLock::new();
                EMPTY.get_or_init(std::collections::BTreeMap::new)
            });
        let Ok(workers) = std::fs::read_dir(&dir) else {
            return;
        };
        for entry in workers.flatten() {
            let worker_dir = entry.path();
            let name = entry.file_name();
            let Some(worker_id) = name.to_str() else {
                continue;
            };
            if !entry.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            if !live_workers.contains(worker_id) {
                // RV-R53: a worker that left the spec is destroyed ONLY
                // when the control plane signed a tombstone for it —
                // capacity eviction, exclusion, poisoning and stale
                // specs all RETAIN the dir (undeploy ≠ not-serving).
                let tombstoned = self
                    .do_deletions
                    .get(worker_id)
                    .is_some_and(|s| s.contains(""));
                if !tombstoned {
                    crate::metrics::registry().inc(
                        "vrt_host_do_sweep_retained_total",
                        &[("kind", "worker")],
                        1.0,
                    );
                    continue;
                }
                // Worker removed from the spec — its DO data is the
                // `delete` lifecycle: remove the whole worker dir
                // (idempotent; the worker's own segment prevents
                // touching another worker).
                // RV-R24: EXCEPT while a departing `Final` snapshot is
                // still pending — that class dir is the ONLY remaining
                // copy of the data. Bulk-deleting it here would strand
                // the transfer target on a stale or missing baseline
                // and let `service_departing_snapshots` release the
                // held lease into a fence-in. An in-memory pending
                // entry OR the on-disk `.final-snap-pending` marker
                // both count (read_dir order/restart windows), and
                // markers also re-seed the retry set — same contract
                // as the live-worker branch below. Skipping the WHOLE
                // worker dir is deliberate fail-closed: the pending
                // entry bounds the retention (the dir leaves once the
                // Final commits), while partial deletion could orphan
                // lease siblings or marker evidence.
                let mut has_pending = self
                    .departing_snap_pending
                    .keys()
                    .any(|(w, _)| w == worker_id);
                let now_ms = wall_now_ms();
                if let Ok(entries) = std::fs::read_dir(&worker_dir) {
                    for e in entries.flatten() {
                        let n = e.file_name();
                        let Some(cname) = n.to_str() else {
                            continue;
                        };
                        if let Some(key) = cname.strip_suffix(FINAL_PENDING_SUFFIX) {
                            has_pending = true;
                            Self::reseed_final_pending(
                                &mut self.departing_snap_pending,
                                &self.tenant_id,
                                &worker_dir,
                                worker_id,
                                key,
                                &e.path(),
                                now_ms,
                            );
                        }
                    }
                }
                if has_pending {
                    crate::metrics::registry().inc(
                        "vrt_host_do_sweep_skipped_total",
                        &[("kind", "skipped_pending")],
                        1.0,
                    );
                    tracing::warn!(
                        tenant = %self.tenant_id,
                        worker = worker_id,
                        "do storage worker dir kept — departing final snapshot still pending"
                    );
                    continue;
                }
                match std::fs::remove_dir_all(&worker_dir) {
                    Ok(()) => {
                        crate::metrics::registry().inc(
                            "vrt_host_do_dir_deletes_total",
                            &[("kind", "worker")],
                            1.0,
                        );
                        tracing::info!(
                            tenant = %self.tenant_id,
                            worker = worker_id,
                            "do storage worker dir removed (worker undeployed)"
                        );
                    }
                    Err(e) => {
                        crate::metrics::registry().inc(
                            "vrt_host_do_sweep_failures_total",
                            &[],
                            1.0,
                        );
                        tracing::warn!(
                            tenant = %self.tenant_id,
                            worker = worker_id,
                            %e,
                            "do storage worker dir removal failed"
                        );
                    }
                }
                continue;
            }
            let keep_keys: &[String] = keep.get(worker_id).map(|v| v.as_slice()).unwrap_or(&[]);
            let Ok(classes) = std::fs::read_dir(&worker_dir) else {
                continue;
            };
            for class_entry in classes.flatten() {
                let cname = class_entry.file_name();
                let Some(cname) = cname.to_str() else {
                    continue;
                };
                let now_ms = wall_now_ms();
                // DO1-r2 P1-1: quarantined transfer sources carry a
                // `.since` retention marker — the dir stays until the
                // configured window lapses (operator recovery), and a
                // missing/corrupt marker RESTARTS the window rather
                // than deleting evidence early.
                if let Some(qbase) = cname.strip_suffix(crate::do_transfer::QUARANTINE_SUFFIX) {
                    let retention = self
                        .deps
                        .do_snap
                        .as_ref()
                        .map(|s| s.xfer_quarantine_ms)
                        .unwrap_or(7 * 24 * 3600 * 1000);
                    let age = crate::do_transfer::quarantine_age_ms(&worker_dir, cname, now_ms);
                    match age {
                        Some(a) if a >= retention => {
                            let _ = qbase;
                            let path = class_entry.path();
                            let _ = std::fs::remove_dir_all(&path);
                            let _ = std::fs::remove_file(worker_dir.join(format!(
                                "{cname}{}",
                                crate::do_transfer::QUARANTINE_SINCE_SUFFIX
                            )));
                            for e in std::fs::read_dir(&worker_dir)
                                .into_iter()
                                .flatten()
                                .flatten()
                            {
                                let n = e.file_name();
                                if n.to_str()
                                    .is_some_and(|n| n.starts_with(&format!("{cname}-sib-")))
                                {
                                    let _ = std::fs::remove_file(e.path());
                                }
                            }
                            crate::metrics::registry().inc(
                                "vrt_host_do_dir_deletes_total",
                                &[("kind", "quarantine-expired")],
                                1.0,
                            );
                            tracing::warn!(
                                tenant = %self.tenant_id,
                                worker = worker_id,
                                entry = cname,
                                "do quarantine retention expired; fenced source removed"
                            );
                        }
                        Some(_) => {}
                        None => {
                            crate::do_transfer::mark_quarantine(&worker_dir, cname);
                        }
                    }
                    continue;
                }
                if cname.contains(crate::do_transfer::QUARANTINE_SUFFIX) {
                    // `.since` marker / `-sib-*` siblings — their life
                    // is decided with the parent quarantine entry.
                    continue;
                }
                // DO1-r2: `.final-snap-pending` markers re-seed the
                // retry set after a restart AND keep themselves — the
                // marker is cleared only when the baseline commits.
                if let Some(key) = cname.strip_suffix(FINAL_PENDING_SUFFIX) {
                    Self::reseed_final_pending(
                        &mut self.departing_snap_pending,
                        &self.tenant_id,
                        &worker_dir,
                        worker_id,
                        key,
                        &class_entry.path(),
                        now_ms,
                    );
                    continue;
                }
                // `<uniqueKey>` dirs and their sibling lease files
                // (`<key>.epoch`, `<key>.lease-epoch`) survive iff the
                // class is still declared — the suffix table is the
                // single source shared with both writers.
                let key = crate::do_placement::lease_entry_class_key(cname);
                if keep_keys.iter().any(|k| k == key) {
                    continue;
                }
                // DO1: a departing class whose final snapshot is still
                // pending is NOT garbage — it is the only remaining
                // copy until a baseline lands. The on-disk marker is
                // checked too: read_dir order is arbitrary, so the
                // class dir may be visited BEFORE the marker that
                // would seed the pending entry.
                if self
                    .departing_snap_pending
                    .contains_key(&(worker_id.to_string(), key.to_string()))
                    || worker_dir
                        .join(format!("{key}{FINAL_PENDING_SUFFIX}"))
                        .exists()
                {
                    continue;
                }
                // RV-R53: only a signed tombstone authorizes destroying
                // a class entry — anything else that left the spec
                // (excluded worker, stale snapshot) is retained.
                let tombstoned = self
                    .do_deletions
                    .get(worker_id)
                    .is_some_and(|s| s.contains("") || s.contains(key));
                if !tombstoned {
                    // MIG-17c: count the CLASS entry once — sibling
                    // lease/epoch/replay-marker files (`cname != key`
                    // after suffix mapping) follow the class's fate
                    // and are not classes themselves.
                    if cname == key {
                        crate::metrics::registry().inc(
                            "vrt_host_do_sweep_retained_total",
                            &[("kind", "class")],
                            1.0,
                        );
                    }
                    continue;
                }
                let path = class_entry.path();
                let removed = if path.is_dir() {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_file(&path)
                };
                match removed {
                    Ok(()) => {
                        crate::metrics::registry().inc(
                            "vrt_host_do_dir_deletes_total",
                            &[("kind", "class")],
                            1.0,
                        );
                        tracing::info!(
                            tenant = %self.tenant_id,
                            worker = worker_id,
                            entry = cname,
                            "do storage stale class entry removed"
                        );
                    }
                    Err(e) => {
                        crate::metrics::registry().inc(
                            "vrt_host_do_sweep_failures_total",
                            &[],
                            1.0,
                        );
                        tracing::warn!(
                            tenant = %self.tenant_id,
                            worker = worker_id,
                            %e,
                            "do storage stale class entry removal failed"
                        );
                    }
                }
            }
        }
    }

    /// DO1-r2/RV-R24: seed the bounded retry entry for a
    /// `<key>.final-snap-pending` marker found by the sweep — shared
    /// by the live-worker and removed-worker branches. The marker is
    /// cleared only when the baseline commits; a missing class dir
    /// does NOT seed an entry (there is no data left to upload —
    /// `service_departing_snapshots` owns the vanished-dir
    /// escalation for entries already pending).
    fn reseed_final_pending(
        pending: &mut std::collections::BTreeMap<(String, String), DepartingPending>,
        tenant_id: &str,
        worker_dir: &Path,
        worker_id: &str,
        key: &str,
        marker_path: &Path,
        now_ms: u64,
    ) {
        let pair = (worker_id.to_string(), key.to_string());
        let dir = worker_dir.join(key);
        if pending.contains_key(&pair) || !dir.is_dir() {
            return;
        }
        let body = std::fs::read_to_string(marker_path).unwrap_or_default();
        let mut it = body.split_whitespace();
        let epoch = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        let is_transfer = it.next() == Some("transfer");
        pending.insert(
            pair,
            DepartingPending {
                scope: crate::do_placement::PlacementScope {
                    tenant: tenant_id.to_string(),
                    worker: worker_id.to_string(),
                    class_key: key.to_string(),
                    class_dir: dir,
                },
                epoch,
                first_failed_ms: now_ms,
                next_retry_at: Instant::now(),
                attempts: 0,
                exhausted: false,
                is_transfer,
                dir_missing: false,
            },
        );
    }

    /// P3c-1: per-worker DO storage stats — bounded recursive stat of
    /// `<workerDir>/<uniqueKey>` (capped entry count so a pathological
    /// namespace can't stall the monitor loop).
    fn stat_do_dirs(&mut self) {
        const MAX_STAT_ENTRIES: usize = 100_000;
        let Some(active) = &self.active else {
            return;
        };
        let Some(do_spec) = &active.spec.do_storage else {
            return;
        };
        let r = crate::metrics::registry();
        for (worker_id, keys) in &do_spec.worker_classes {
            let mut bytes: u64 = 0;
            let mut objects: u64 = 0;
            let mut entries_seen: usize = 0;
            'outer: for key in keys {
                let class_dir = do_spec.tenant_dir.join(worker_id).join(key);
                let mut stack = vec![class_dir];
                while let Some(d) = stack.pop() {
                    let Ok(rd) = std::fs::read_dir(&d) else {
                        continue;
                    };
                    for e in rd.flatten() {
                        entries_seen += 1;
                        if entries_seen > MAX_STAT_ENTRIES {
                            break 'outer;
                        }
                        let Ok(meta) = e.metadata() else {
                            continue;
                        };
                        if meta.is_dir() {
                            stack.push(e.path());
                        } else {
                            bytes += meta.len();
                            if e.file_name().to_string_lossy().ends_with(".sqlite") {
                                objects += 1;
                            }
                        }
                    }
                }
            }
            r.set(
                "vrt_host_do_storage_bytes",
                &[("tenant", &self.tenant_id), ("worker", worker_id.as_str())],
                bytes as f64,
            );
            r.set(
                "vrt_host_do_objects",
                &[("tenant", &self.tenant_id), ("worker", worker_id.as_str())],
                objects as f64,
            );
        }
    }

    fn service_draining(&mut self, now: Instant) {
        // Probes to fire after the loop — `spawn_wu_probe` borrows
        // `self`, which the `&mut self.draining` iteration forbids.
        let mut probes: Vec<(u64, u64, WorkerEndpoint)> = Vec::new();
        // MIG-12-r2: hoisted out of the loop — a do_switch
        // generation's waitUntil wait is capped at the smaller
        // do-switch budget (see `effective_wu_grace`); `self` can't be
        // borrowed inside the `&mut self.draining` iteration.
        let wu_grace_do = self.wait_until_grace.min(self.do_switch_wait_until_grace);
        for gen in &mut self.draining {
            if gen.killed {
                continue;
            }
            if gen.sigterm_at.is_none() {
                let idle = gen
                    .slots
                    .values()
                    .all(|s| s.in_flight.load(Ordering::SeqCst) == 0);
                if !idle {
                    gen.wu_idle_since = None;
                    if now >= gen.deadline {
                        self.runtime
                            .stats
                            .drain_forced
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            tenant = %self.tenant_id,
                            gen = gen.id,
                            "drain deadline exceeded; SIGKILL"
                        );
                        // MIG-12: any waitUntil work still armed dies
                        // with the process — counted as dropped.
                        Self::note_waituntil_dropped(&self.tenant_id, gen, "drain_deadline");
                        signal_process_group(gen.pid, libc::SIGKILL);
                        gen.killed = true;
                    }
                    continue;
                }
                // HTTP idle. Without a probe surface (or with the
                // grace disabled) the generation SIGTERMs exactly as
                // before — same-turn, zero added latency (MIG-11).
                // MIG-12-r2: a serialized-DO-switch generation's wait
                // is bounded by the smaller do-switch budget — its
                // drain sits inside the switch-hold window, so a full
                // `wait_until_grace` stall would blow the hold into
                // 503s. `0` on EITHER bound disables the wait here.
                let wu_grace = if gen.do_switch {
                    wu_grace_do
                } else {
                    self.wait_until_grace
                };
                if gen.waituntil.is_none() || wu_grace.is_zero() {
                    signal_process_group(gen.pid, libc::SIGTERM);
                    if gen.do_switch {
                        crate::metrics::registry().observe(
                            "vrt_host_do_switch_stage_seconds",
                            &[("tenant", &self.tenant_id), ("stage", "drain_to_sigterm")],
                            now.duration_since(gen.entered_at).as_secs_f64(),
                        );
                    }
                    gen.sigterm_at = Some(now);
                    continue;
                }
                // MIG-12: defer SIGTERM while THIS generation's own
                // pending ctx.waitUntil work is unsettled — bounded by
                // `wu_grace` (the do-switch budget for do_switch
                // generations) from the first idle observation.
                let idle_since = *gen.wu_idle_since.get_or_insert(now);
                // `drain_timeout` is the TOTAL cap — the waitUntil wait
                // can never extend past it (spec: SIGKILL at deadline).
                if now >= gen.deadline {
                    self.runtime
                        .stats
                        .drain_forced
                        .fetch_add(1, Ordering::Relaxed);
                    Self::note_waituntil_dropped(&self.tenant_id, gen, "drain_deadline");
                    signal_process_group(gen.pid, libc::SIGKILL);
                    gen.killed = true;
                    continue;
                }
                // An in-flight probe that outlived its timeout is a
                // failure, not a stall — resolve it now (fail closed).
                if let Some((_, sent)) = gen.wu_probe {
                    if now.duration_since(sent) >= self.probe_timeout {
                        gen.wu_probe = None;
                        gen.wu_last = Some(WuVerdict::Failed);
                    }
                }
                let sigterm_clean = matches!(gen.wu_last, Some(WuVerdict::Pending(0)));
                let probe_failed = matches!(gen.wu_last, Some(WuVerdict::Failed));
                // The grace clock is capped at the drain deadline — a
                // generation that went idle just before it gets no
                // extra runway.
                let grace_deadline = (idle_since + wu_grace).min(gen.deadline);
                let grace_expired = now >= grace_deadline;
                if sigterm_clean || probe_failed || grace_expired {
                    if probe_failed {
                        Self::note_waituntil_dropped(&self.tenant_id, gen, "probe_failed");
                    } else if grace_expired && !sigterm_clean {
                        // MIG-12-r2: name the bound that fired — on a
                        // do_switch generation the smaller do-switch
                        // budget is what truncates the wait.
                        let reason = if gen.do_switch
                            && self.do_switch_wait_until_grace <= self.wait_until_grace
                        {
                            "do_switch_grace_expired"
                        } else {
                            "grace_expired"
                        };
                        Self::note_waituntil_dropped(&self.tenant_id, gen, reason);
                    }
                    signal_process_group(gen.pid, libc::SIGTERM);
                    if gen.do_switch {
                        crate::metrics::registry().observe(
                            "vrt_host_do_switch_stage_seconds",
                            &[("tenant", &self.tenant_id), ("stage", "drain_to_sigterm")],
                            now.duration_since(gen.entered_at).as_secs_f64(),
                        );
                        // MIG-12-r2: sub-stage — the post-idle
                        // waitUntil wait INSIDE drain_to_sigterm.
                        // Only observed when the generation actually
                        // entered the wait (wu_idle_since armed).
                        if let Some(idle_since) = gen.wu_idle_since {
                            crate::metrics::registry().observe(
                                "vrt_host_do_switch_stage_seconds",
                                &[("tenant", &self.tenant_id), ("stage", "waituntil_wait")],
                                now.duration_since(idle_since).as_secs_f64(),
                            );
                        }
                    }
                    gen.sigterm_at = Some(now);
                } else if gen.wu_probe.is_none() && now >= gen.wu_next_at {
                    self.wu_probe_seq += 1;
                    let seq = self.wu_probe_seq;
                    gen.wu_probe = Some((seq, now));
                    gen.wu_next_at = now + WU_PROBE_INTERVAL;
                    probes.push((
                        gen.id,
                        seq,
                        gen.waituntil
                            .clone()
                            .expect("waituntil checked non-empty above"),
                    ));
                }
            } else if now >= gen.sigterm_at.expect("checked") + self.kill_grace {
                signal_process_group(gen.pid, libc::SIGKILL);
                gen.killed = true;
            }
        }
        for (gen, seq, endpoint) in probes {
            self.spawn_wu_probe(gen, seq, endpoint);
        }
    }

    /// MIG-12-r2: the waitUntil grace that binds THIS draining
    /// generation — a serialized-DO-switch generation is capped at
    /// the smaller do-switch budget because its drain extends the
    /// switch-hold window; every other generation uses the global
    /// `wait_until_grace`. `0` on the applicable bound disables the
    /// wait (idle → same-turn SIGTERM).
    fn effective_wu_grace(&self, gen: &DrainingGen) -> Duration {
        if gen.do_switch {
            self.wait_until_grace.min(self.do_switch_wait_until_grace)
        } else {
            self.wait_until_grace
        }
    }

    /// MIG-12: count the drop + one structured WARN. `pending` is the
    /// last observed count — `pending_known=false` when none was ever
    /// established (never probed; last probe Failed), so an unknown
    /// count is never emitted as a real number. Never tenant payload.
    fn note_waituntil_dropped(tenant: &str, gen: &DrainingGen, reason: &'static str) {
        if gen.waituntil.is_none() {
            return;
        }
        crate::metrics::registry().inc(
            "vrt_host_waituntil_dropped_total",
            &[("reason", reason)],
            1.0,
        );
        let (pending, pending_known) = match gen.wu_last {
            Some(WuVerdict::Pending(n)) => (n, true),
            _ => (0, false),
        };
        tracing::warn!(
            tenant = %tenant,
            gen = gen.id,
            pending,
            pending_known,
            reason,
            "waitUntil work dropped: generation signalled with pending or unknown waitUntil"
        );
    }

    /// MIG-12: fire the bounded aggregate probe off-actor (same model
    /// as the heartbeat probe — spawn_blocking on a fixed timeout,
    /// result through `Ev::WuProbeResult`).
    fn spawn_wu_probe(&mut self, gen: u64, seq: u64, endpoint: WorkerEndpoint) {
        let tx = self.ev_tx.clone();
        let budget = self.probe_timeout;
        tokio::spawn(async move {
            let verdict = match tokio::time::timeout(
                budget,
                tokio::task::spawn_blocking(move || wu_pending_probe(&endpoint, budget)),
            )
            .await
            {
                Ok(Ok(Some(pending))) => WuVerdict::Pending(pending),
                // Join error, probe panic, IO error, bad reply —
                // everything non-Pending is a failed probe (fail
                // closed, never a hang).
                _ => WuVerdict::Failed,
            };
            let _ = tx.try_send(Ev::WuProbeResult { gen, seq, verdict });
        });
    }

    /// MIG-12: the probe landed — only the registered (gen, seq)
    /// accepts it; a stale result never moves a newer generation.
    /// Then re-service draining so `Pending(0)` SIGTERMs in the same
    /// turn rather than waiting out `WU_PROBE_INTERVAL`.
    fn on_wu_probe_result(&mut self, gen: u64, seq: u64, verdict: WuVerdict) {
        let now = Instant::now();
        let found = self.draining.iter_mut().any(|g| {
            g.id == gen && matches!(g.wu_probe, Some((s, _)) if s == seq)
        });
        if !found {
            return;
        }
        if let Some(g) = self.draining.iter_mut().find(|g| g.id == gen) {
            g.wu_probe = None;
            g.wu_last = Some(verdict);
            g.wu_next_at = now + WU_PROBE_INTERVAL;
        }
        self.service_draining(now);
    }

    fn check_stopped(&mut self) {
        if self.state == TenantState::Draining
            && self.active.is_none()
            && self.draining.is_empty()
            && self.pending.is_none()
            && self.preflight.is_none()
            && self.prewarm.is_none()
        {
            self.transit(Transition::Drained);
        }
    }

    fn next_wake(&self) -> Instant {
        let mut wake = Instant::now() + Duration::from_secs(3600);
        let mut consider = |t: Instant| {
            if t < wake {
                wake = t;
            }
        };
        consider(self.next_monitor);
        if let Some(cfg) = &self.s2z {
            if self.state == TenantState::Ready && self.hib.is_none() {
                consider(self.hib_next_eval);
            }
            if let Some(h) = &self.hib {
                consider(h.deadline);
                consider(Instant::now() + cfg.quiesce_poll);
            }
            if let Some(w) = &self.wake_run {
                consider(w.deadline);
            }
        }
        if let Some(t) = self.do_lease_wake {
            consider(t);
        }
        if let Some(t) = self.retry_at {
            consider(t);
        }
        if let Some(t) = self.cooldown_until {
            consider(t);
        }
        if let Some(ctx) = &self.prewarm {
            // L2: the episode's own deadline.
            consider(ctx.expires_at);
            // H2-r2: the parked warm generation is death-polled on
            // ticks — keep the period short so early-flip routes
            // never outlive the process for long.
            if ctx.warm.is_some() {
                consider(Instant::now() + WARM_WATCH_INTERVAL);
            }
        }
        for gen in &self.draining {
            // A SIGKILLed gen has nothing left to time: service_draining
            // skips it and only the exit watcher's event reaps it. Its
            // deadline / sigterm+grace are already in the past, so
            // considering them would busy-spin the actor until reap.
            if gen.killed {
                continue;
            }
            consider(gen.deadline);
            match gen.sigterm_at {
                Some(t) => consider(t + self.kill_grace),
                None => {
                    let idle = gen
                        .slots
                        .values()
                        .all(|s| s.in_flight.load(Ordering::SeqCst) == 0);
                    if !idle {
                        continue;
                    }
                    // MIG-12-r2: the bound that applies to THIS
                    // generation — do_switch drains use the smaller
                    // do-switch budget (`0` disables → immediate wake).
                    let wu_grace = self.effective_wu_grace(gen);
                    if gen.waituntil.is_none() || wu_grace.is_zero() {
                        // MIG-11: an idle unsignalled draining
                        // generation must not wait for the monitor
                        // tick — `enter_draining` signals it in the
                        // same turn, and this wake is the backstop for
                        // any path that reaches service_draining late
                        // (e.g. a slot's last guard released without a
                        // 1→0 transition observed).
                        consider(Instant::now());
                    } else {
                        // MIG-12: the waitUntil wait is bounded — wake
                        // for the next scheduled probe, the in-flight
                        // probe's timeout, and the grace deadline. An
                        // immediate `now` here would busy-spin until a
                        // verdict lands.
                        let idle_since = gen.wu_idle_since.unwrap_or_else(Instant::now);
                        consider(idle_since + wu_grace);
                        consider(gen.wu_next_at);
                        if let Some((_, sent)) = gen.wu_probe {
                            consider(sent + self.probe_timeout);
                        }
                    }
                }
            }
        }
        wake
    }

    // ---- drain ----

    fn begin_drain(&mut self, undeploy: bool) {
        if self.state == TenantState::Stopped {
            return;
        }
        self.undeploy |= undeploy;
        self.transit(Transition::BeginDrain);
        // Scale-to-zero: waiters get a typed failure (never a hang or a
        // fake success), the queue slot is withdrawn, and a quiescing
        // attempt is simply folded into the drain below.
        self.hib = None;
        if let Some(s) = &self.s2z {
            s.coordinator.cancel(&self.tenant_id);
        }
        self.finish_wake_err(crate::hibernate::WakeError::Failed("tenant-draining"));
        self.desired_spec = None;
        self.retry_at = None;
        self.cooldown_until = None;
        self.half_open = false;
        self.do_wait_since = None;
        self.do_switch_exit_at = None;
        // MIG-11-r2: drain means nothing keeps serving — held
        // waiters must resolve `NoServing`, not the false-404 branch.
        self.runtime.set_serving(false);
        // W4-r2: tenant drain abandons any open switch window —
        // held requests release into the honest not-ready surface.
        self.runtime.switch_gate.end();
        self.abort_pending();
        let now = Instant::now();
        if let Some(active) = self.active.take() {
            self.enter_draining(active, now, false);
        }
        self.check_stopped();
    }

    fn abort_pending(&mut self) {
        // MIG-9: an in-flight preflight is NOT a JoinHandle abort —
        // spawn_blocking ignores it once running. Flip the cancel flag
        // instead: the check's wait loop group-kills + reaps the child
        // inside one tick, then reports `Cancelled` (stale seq → the
        // late verdict is dropped when it lands).
        if let Some(ctx) = self.preflight.take() {
            ctx.cancel.store(true, Ordering::SeqCst);
        }
        if let Some(pending) = self.pending.take() {
            pending.task.abort();
            // Aborted mid-flight the future drops its `LaunchedWorkerd`
            // or `PendingGen`; kill-on-drop SIGKILLs and tokio's orphan
            // collector reaps. The generation was never routed.
            // P3c-2: kill-on-drop already fired → the pre-spawn leases
            // are releasable (fence now, not at TTL).
            Self::release_leases(&*self.deps.placement, &self.tenant_id, &pending.leases);
            // MIG-13-r3: an aborted swap leaves the warm gen's dir
            // spent — retire it deterministically.
            if pending.prewarm_swap {
                self.retire_prewarm_dir(&pending.work_dir);
            }
        }
        if let Some(mut ctx) = self.prewarm.take() {
            if let Some(t) = ctx.task.take() {
                t.abort();
            }
            // A spawned warm generation dies on its own kill-on-drop
            // guard — it was never installed or routed.
            let work_dir = ctx.work_dir.clone();
            drop(ctx);
            self.retire_prewarm_dir(&work_dir);
        }
        // MIG-13: with NO live generation, any still-published route
        // map can only be early-flip slots of a prewarmed generation
        // whose process just died (or a dead gen's stale map) —
        // revoked through the one helper so held requests never
        // resolve to a killed socket. When `active` is alive the maps
        // belong to it and stay untouched.
        self.revoke_early_routes();
        if !self.pending_do_leases.is_empty() {
            Self::release_leases(&*self.deps.placement, &self.tenant_id, &self.pending_do_leases);
            self.pending_do_leases.clear();
        }
        // MIG-17-r2 (H2): an aborted pending also retires the
        // serialized-switch carry — same departing/release path as
        // every other carry exit (no lease survives the attempt it
        // belonged to).
        self.drain_carried();
    }

    /// Start a launch attempt for `desired_spec`.
    ///
    /// MIG-9: this is now only the attempt prelude — pick the full
    /// desired or the exclusion-reduced re-assembly, then hand off to
    /// `preflight_or_launch`. The `workerd test` gate (when needed)
    /// runs on a blocking-pool thread and reports back via
    /// `Ev::PreflightDone`; everything after it lives in
    /// `launch_after_preflight`. A config workerd cannot load still
    /// never reaches the serialized DO switch below, and the bounded
    /// switch-retry budget still applies — only the actor thread is
    /// no longer pinned while the check runs.
    fn start_pending_launch(&mut self, _boot: bool) {
        if self.pending.is_some()
            || self.preflight.is_some()
            // MIG-13-r2/M3: a prewarm SPAWN in flight is already a
            // launch attempt for this desired — re-entering could
            // stack a third workerd on top of (old gen + warm spawn).
            // Once `warm` arms this call is exactly how the post-drain
            // consume runs, so it must pass then.
            || self.prewarm.as_ref().is_some_and(|p| p.warm.is_none())
        {
            return;
        }
        let Some(desired_spec) = self.desired_spec.clone() else {
            return;
        };
        // MIG-4/MIG-9: initial attempt = the full desired, or the
        // reduced re-assembly when exclusions are already recorded
        // (identical to the old launch_attempt_spec prelude).
        let attempt: Box<TenantLaunchSpec> = if self.excluded.is_empty() {
            Box::new((*desired_spec).clone())
        } else {
            match self.assemble_excluded(&desired_spec) {
                Some(spec) => spec,
                None => {
                    self.on_launch_failed(SpawnFailure {
                        reason: "assemble",
                        detail: None,
                    });
                    return;
                }
            }
        };
        self.preflight_or_launch(attempt, desired_spec);
    }

    /// W1-r2: whether `attempt` still needs the `workerd test` gate —
    /// only serialized capnp configs, and only once per content digest
    /// (a passing digest never re-runs: serialized re-entry and retry
    /// re-assembly render identical bytes ⇒ identical verdict).
    fn preflight_needed(&self, spec: &TenantLaunchSpec) -> bool {
        if spec.config_path.file_name().and_then(|n| n.to_str()) != Some("config.capnp") {
            return false;
        }
        self.preflight_ok_digest.as_deref() != Some(spec.config_digest.as_str())
    }

    /// Build the loopback-addressed twin config the check actually
    /// runs — `workerd test` BINDS declared sockets, so the probe uses
    /// the isolated variant (same dir ⇒ same embeds) and never
    /// collides with the live generation's `unix:` sockets.
    fn preflight_prep(spec: &TenantLaunchSpec) -> Result<PathBuf, String> {
        let text =
            std::fs::read_to_string(&spec.config_path).map_err(|e| format!("config read: {e}"))?;
        let scratch = spec.work_dir.join("preflight-disks");
        std::fs::create_dir_all(&scratch).map_err(|e| format!("preflight dir: {e}"))?;
        let twin = spec.work_dir.join("config.preflight.capnp");
        let isolated = vrt_config::check::isolated_variant(&text, &scratch)
            .map_err(|e| format!("preflight config: {e}"))?;
        std::fs::write(&twin, isolated).map_err(|e| format!("preflight config: {e}"))?;
        Ok(twin)
    }

    /// MIG-9: run the bounded config preflight OFF the actor thread
    /// when the digest needs it, else proceed straight to launch. The
    /// verdict arrives as [`Ev::PreflightDone`] — while it is in flight
    /// the actor still services cmds/evs, so Apply/Stop/Undeploy and
    /// sibling tenants are never pinned behind one `workerd test`.
    ///
    /// Failure handling is identical to the old synchronous loop:
    /// attribute the detail, try one bounded exclusion step, and either
    /// recurse on the reduced attempt or take the classic failure path.
    fn preflight_or_launch(
        &mut self,
        attempt: Box<TenantLaunchSpec>,
        desired: Box<TenantLaunchSpec>,
    ) {
        if !self.preflight_needed(&attempt) {
            self.launch_after_preflight(attempt, &desired);
            return;
        }
        match Self::preflight_prep(&attempt) {
            Ok(twin) => self.spawn_preflight(attempt, desired, twin),
            Err(detail) => {
                self.record_attribution(&detail);
                match self.exclusion_step(&desired, &detail) {
                    Ok(next) => self.preflight_or_launch(next, desired),
                    Err(failure) => self.on_launch_failed(failure),
                }
            }
        }
    }

    /// MIG-9: dispatch the check onto the blocking pool and forward the
    /// typed verdict through `ev_tx`. The inner check owns admission +
    /// exec deadlines AND the child's process-group kill/reap; the
    /// outer `tokio::time::timeout` only guards a wedged blocking task
    /// (pool starvation, unkillable sleep), flipping `cancel` so even
    /// that path kills the group instead of detaching it.
    fn spawn_preflight(
        &mut self,
        attempt: Box<TenantLaunchSpec>,
        desired: Box<TenantLaunchSpec>,
        twin: PathBuf,
    ) {
        self.preflight_seq += 1;
        let seq = self.preflight_seq;
        let cancel = Arc::new(AtomicBool::new(false));
        let binary = attempt.binary.clone();
        let external_addrs = attempt.external_addrs.clone();
        let extra_args = attempt.extra_args.clone();
        let needles: Vec<String> = attempt.env.values().map(|v| v.expose().to_string()).collect();
        let timeout = self.preflight_timeout;
        self.preflight = Some(PreflightCtx {
            seq,
            attempt,
            desired,
            cancel: cancel.clone(),
        });
        let ev_tx = self.ev_tx.clone();
        let r = crate::metrics::registry();
        r.add("vrt_host_preflight_checks_in_flight", &[], 1.0);
        tokio::spawn(async move {
            let t0 = Instant::now();
            let cancel_in_check = cancel.clone();
            let join = tokio::task::spawn_blocking(move || {
                preflight_workerd_cancellable(
                    &binary,
                    &twin,
                    &external_addrs,
                    &extra_args,
                    needles.into_iter(),
                    timeout,
                    cancel_in_check,
                )
            });
            // Outer bound = inner budget + slack — the inner check owns
            // the admission + exec deadlines, so a timeout here means
            // the blocking task itself is wedged. spawn_blocking cannot
            // be aborted once running: flip `cancel` and let the check
            // kill + reap the group, then drop the (detached) handle.
            let result = match tokio::time::timeout(timeout + Duration::from_secs(10), join).await {
                Ok(Ok(res)) => res,
                Ok(Err(e)) => Err(vrt_config::check::CheckFailure {
                    kind: vrt_config::check::CheckKind::Rejected,
                    detail: format!("preflight task join: {e}"),
                }),
                Err(_) => {
                    cancel.store(true, Ordering::SeqCst);
                    Err(vrt_config::check::CheckFailure {
                        kind: vrt_config::check::CheckKind::Timeout,
                        detail: "preflight wedged past actor-side bound".into(),
                    })
                }
            };
            let elapsed = t0.elapsed();
            let r = crate::metrics::registry();
            r.add("vrt_host_preflight_checks_in_flight", &[], -1.0);
            r.observe(
                "vrt_host_preflight_check_seconds",
                &[],
                elapsed.as_secs_f64(),
            );
            let outcome = match &result {
                Ok(()) => "ok",
                Err(f) => match f.kind {
                    vrt_config::check::CheckKind::Rejected => "rejected",
                    vrt_config::check::CheckKind::Timeout => "timeout",
                    vrt_config::check::CheckKind::AdmissionTimeout => "admission-timeout",
                    vrt_config::check::CheckKind::Cancelled => "cancelled",
                },
            };
            r.inc(
                "vrt_host_preflight_checks_total",
                &[("outcome", outcome)],
                1.0,
            );
            let _ = ev_tx
                .send(Ev::PreflightDone {
                    seq,
                    result,
                    elapsed_ms: elapsed.as_millis() as u64,
                })
                .await;
        });
    }

    /// MIG-9: land the off-actor verdict. `seq` must match the
    /// registered ctx — a superseded or actor-cancelled check is
    /// abandoned without touching launch state. The desired that
    /// produced `ctx.attempt` must still BE the desired (the
    /// Crashed/CircuitOpen Apply path can swap `desired_spec` without
    /// aborting an in-flight preflight; a stale verdict restarts
    /// against the current desired instead of launching old bytes).
    fn on_preflight_done(
        &mut self,
        seq: u64,
        result: Result<(), vrt_config::check::CheckFailure>,
        elapsed_ms: u64,
    ) {
        match self.preflight.as_ref() {
            Some(c) if c.seq == seq => {}
            _ => return,
        }
        let ctx = self.preflight.take().expect("ctx checked above");
        if !self
            .desired_spec
            .as_ref()
            .is_some_and(|d| d.config_digest == ctx.desired.config_digest)
        {
            self.start_pending_launch(false);
            return;
        }
        match result {
            Ok(()) => {
                tracing::info!(
                    tenant = %self.tenant_id,
                    digest = %ctx.attempt.config_digest,
                    elapsed_ms,
                    "preflight: config loads"
                );
                self.preflight_ok_digest = Some(ctx.attempt.config_digest.clone());
                self.launch_after_preflight(ctx.attempt, &ctx.desired);
            }
            Err(f) if f.kind == vrt_config::check::CheckKind::Cancelled => {
                // Actor-side abort (stop/supersede): the process group
                // is already dead — restart cleanly against the current
                // desired; never report a config verdict.
                self.start_pending_launch(false);
            }
            Err(f) => {
                tracing::warn!(
                    tenant = %self.tenant_id,
                    digest = %ctx.attempt.config_digest,
                    detail = %f.detail,
                    elapsed_ms,
                    "preflight: config rejected (old generation keeps serving, no drain)"
                );
                self.record_attribution(&f.detail);
                match self.exclusion_step(&ctx.desired, &f.detail) {
                    Ok(next) => self.preflight_or_launch(next, ctx.desired),
                    Err(failure) => self.on_launch_failed(failure),
                }
            }
        }
    }

    /// MIG-4: stderr `service worker-<id>` attribution for the
    /// current desired — validated ids only (charset + membership in
    /// the desired tenant). Stored on the runtime so readyz can name
    /// the blamed workers in `tenant_failures`. Parse result is
    /// untrusted input: it only ever labels THIS tenant's own workers.
    fn record_attribution(&mut self, detail: &str) {
        let Some(tenant) = self
            .desired_spec
            .as_ref()
            .and_then(|s| s.relaunch.as_ref().map(|rl| &rl.tenant))
        else {
            return;
        };
        let mut added = false;
        for id in crate::exclusion::parse_failed_services(detail) {
            if tenant.workers.iter().any(|w| w.worker_id == id)
                && self.attributed.insert(id)
            {
                added = true;
            }
        }
        if added {
            self.runtime
                .set_attributed_workers(self.attributed.iter().cloned().collect());
        }
    }

    /// One exclusion iteration: attribute the failure, gate the
    /// candidate(s) on [`crate::exclusion::exclusion_verdict`], and
    /// reassemble the reduced spec. `Err(SpawnFailure)` = give up —
    /// the caller records the launch failure normally.
    fn exclusion_step(
        &mut self,
        desired: &TenantLaunchSpec,
        detail: &str,
    ) -> Result<Box<TenantLaunchSpec>, SpawnFailure> {
        let give_up = |reason: &'static str| -> SpawnFailure {
            SpawnFailure {
                reason,
                detail: Some(detail.to_string()),
            }
        };
        let Some(rl) = desired.relaunch.clone() else {
            // No reassembly context (hand-built spec / unit tests) —
            // exclusion is impossible; classic failure path.
            return Err(give_up("config-load"));
        };
        if self.deps.assembler.is_none() {
            return Err(give_up("config-load"));
        }
        let mut added = false;
        for id in crate::exclusion::parse_failed_services(detail) {
            if self.excluded.contains(&id) {
                continue;
            }
            match crate::exclusion::exclusion_verdict(&rl.tenant, &id, &self.excluded) {
                Ok(()) => {
                    crate::metrics::registry().inc(
                        "vrt_host_generation_exclusions_total",
                        &[("tenant", &self.tenant_id)],
                        1.0,
                    );
                    tracing::warn!(
                        tenant = %self.tenant_id,
                        worker = %id,
                        "config-load attributed to worker; rebuilding generation without it (MIG-4)"
                    );
                    self.excluded.insert(id);
                    added = true;
                }
                Err(why) => {
                    crate::metrics::registry().inc(
                        "vrt_host_generation_exclusion_ineligible_total",
                        &[("tenant", &self.tenant_id), ("reason", why.code())],
                        1.0,
                    );
                    tracing::warn!(
                        tenant = %self.tenant_id,
                        worker = %id,
                        reason = why.code(),
                        "worker not excludable; keeping whole-tenant failure path"
                    );
                }
            }
        }
        if !added {
            return Err(give_up("config-load"));
        }
        self.assemble_excluded(desired)
            .ok_or_else(|| give_up("assemble"))
    }

    /// Reassemble `desired` minus `self.excluded` — the reduced spec
    /// keeps the FULL `relaunch` context (`self.excluded` is the
    /// single filter applied at every rebuild) and gets the desired
    /// `config_digest` stamped AFTER a successful preflight.
    fn assemble_excluded(&mut self, desired: &TenantLaunchSpec) -> Option<Box<TenantLaunchSpec>> {
        let asm = self.deps.assembler.clone()?;
        let rl = desired.relaunch.clone()?;
        let tenant = crate::exclusion::filtered_tenant(&rl.tenant, &self.excluded)?;
        let gen = self.deps.gen_counter.fetch_add(1, Ordering::Relaxed) + 1;
        match asm.assemble(&tenant, rl.revision, gen) {
            Ok(mut spec) => {
                spec.relaunch = desired.relaunch.clone();
                Some(Box::new(spec))
            }
            Err(error) => {
                tracing::warn!(
                    tenant = %self.tenant_id,
                    %error,
                    "exclusion reassembly failed"
                );
                None
            }
        }
    }

    /// MIG-9: everything `start_pending_launch` used to do AFTER the
    /// synchronous preflight — serialized DO switching (P3c-1 P0),
    /// lease acquisition, transfer handoff, snapshot/WAL restore,
    /// retry re-assembly, spawn. Runs inline when no preflight is
    /// needed, or from `on_preflight_done` once the check passes.
    ///
    /// P3c-1 P0 — serialized DO switching. A spec may only launch
    /// while NO live generation carries `do_storage` (a draining one
    /// may still be writing the shared class dirs). The ordering:
    ///   1. old generation → draining (SIGTERM → SIGKILL bound)
    ///   2. `draining` empty ⇒ old is DEAD (exit watchers fired)
    ///   3. only then the new generation launches and opens the dirs
    /// The wait is bounded (drain_timeout + kill_grace) and
    /// observable: `vrt_host_do_switch_waits_total` counts waiters and
    /// `vrt_host_do_switch_wait_seconds` records the wall-clock gap on
    /// install — during it, routing is empty (requests see the same
    /// not-ready surface as a crashed tenant).
    fn launch_after_preflight(
        &mut self,
        mut desired: Box<TenantLaunchSpec>,
        full_desired: &TenantLaunchSpec,
    ) {
        if !self.excluded.is_empty() {
            // The reduced generation serves the SAME desired — stamp
            // the full fingerprint so `expected == serving` holds for
            // the workers it does run (the excluded ones report
            // `config-load` via `excluded_workers`).
            desired.config_digest = full_desired.config_digest.clone();
            crate::metrics::registry().inc(
                "vrt_host_generation_excluded_workers",
                &[("tenant", &self.tenant_id)],
                self.excluded.len() as f64,
            );
        }
        // C1: bind this tenant's Cache API unix listener ONCE per
        // actor — data and socket survive generation switches (CF
        // per-zone semantics scoped to the tenant). A bind failure
        // DEGRADES instead of blocking the launch: the emitted
        // `cacheApiOutbound` then points at a missing socket and
        // workerd's `caches.*` ops throw explicitly (verified
        // upstream: connect failure → typed exception, never a fake
        // hit/store). Observable via metric + last_error.
        if self.cache_listener.is_none() {
            if let Some(svc) = &self.deps.cache {
                match svc.serve_tenant(&self.tenant_id, desired.isolation.map(|i| (i.uid, i.gid))) {
                    Ok(l) => self.cache_listener = Some(l),
                    Err(e) => {
                        crate::metrics::registry().inc(
                            "vrt_host_cache_listener_errors_total",
                            &[("tenant", self.tenant_id.as_str()), ("stage", "bind")],
                            1.0,
                        );
                        tracing::error!(
                            tenant = %self.tenant_id,
                            %e,
                            "cache listener bind failed; launching degraded (caches.* fails explicitly)"
                        );
                        self.runtime
                            .set_last_error(Some(format!("cache-bind: {e}")));
                    }
                }
            }
        }
        {
            let new_has_do = desired.do_storage.is_some();
            let old_has_do = self
                .active
                .as_ref()
                .is_some_and(|g| g.spec.do_storage.is_some())
                || self.draining.iter().any(|g| g.had_do_storage);
            if new_has_do || old_has_do {
                // Serialized: a live generation must be fully dead
                // before the new one opens the shared class dirs —
                // even when only ONE side carries DO (a non-DO old gen
                // never writes the dirs, but the delete-sweep needs the
                // same "nothing old alive" precondition; one ordering
                // rule, bounded gap, observable).
                //
                // MIG-13: prewarm the incoming generation while the
                // old one still serves — spawn, capnp load, socket
                // bind, readiness and module compile all complete
                // before the request hold begins; only the config
                // swap that opens real DO storage stays serialized.
                if self.prewarm.is_none() && !self.prewarm_burned && self.active.is_some() {
                    match self.start_prewarm(&desired, full_desired) {
                        Ok(()) => return,
                        Err(why) => {
                            // Prewarm is an optimization — degrade to
                            // the classic serialized launch, never a
                            // correctness dependency.
                            crate::metrics::registry().inc(
                                "vrt_host_prewarm_total",
                                &[("tenant", &self.tenant_id), ("outcome", "skipped")],
                                1.0,
                            );
                            tracing::info!(
                                tenant = %self.tenant_id,
                                %why,
                                "do switch: prewarm unavailable; classic serialized launch"
                            );
                        }
                    }
                }
                // A warm generation must exist before the gate opens —
                // otherwise the hold would re-expose the full cold
                // start this design removes.
                if self.prewarm.as_ref().is_some_and(|p| p.warm.is_none()) {
                    return;
                }
                if let Some(old) = self.active.take() {
                    // MIG-11: gate FIRST — a route-map miss between
                    // the clear and `begin()` would otherwise reach a
                    // caller as a genuine-absence 404. With the gate
                    // already active, every entry point sees either a
                    // live slot or a held window, never a false 404.
                    self.runtime.switch_gate.begin();
                    self.enter_draining(old, Instant::now(), true);
                    // MIG-11-r2: `serving` drops BEFORE the maps
                    // empty — inside the gate every waiter holds, but
                    // a resolve started between `begin` and here must
                    // never read "serving + empty map".
                    self.runtime.set_serving(false);
                    // MIG-13: workers that cannot reach DO storage
                    // through the binding graph already serve from the
                    // warm generation — they never hold through the
                    // drain window. DO-reachable workers stay off-map
                    // and hold through the swap.
                    let empty = EarlyFlip::default();
                    let early = self
                        .prewarm
                        .as_ref()
                        .and_then(|p| p.warm.as_ref())
                        .map(|w| &w.early)
                        .unwrap_or(&empty);
                    self.runtime.routing.store(early.routing.clone());
                    self.runtime
                        .qconsumer_routing
                        .store(early.qconsumer.clone());
                    self.runtime.wfops_routing.store(early.wfops.clone());
                    self.runtime
                        .wfop_token_routing
                        .store(Arc::new(early.wfop_tokens.clone()));
                    self.runtime.cron_routing.store(early.cron.clone());
                    self.runtime.set_active_pid(0);
                    tracing::info!(
                        tenant = %self.tenant_id,
                        "do serialized switch: old generation draining before new launch"
                    );
                }
                if !self.draining.is_empty() {
                    if self.do_wait_since.is_none() {
                        self.do_wait_since = Some(Instant::now());
                        crate::metrics::registry().inc("vrt_host_do_switch_waits_total", &[], 1.0);
                        tracing::info!(
                            tenant = %self.tenant_id,
                            "do serialized switch: waiting for old generation to exit"
                        );
                    }
                    return;
                }
            }
            // P3c-2: placement lease `acquire` for every class the
            // new generation will open, BEFORE launch. Authoritative
            // impls (object-store CAS) fail CLOSED — no lease, no
            // workerd (a held lease elsewhere means another cell owns
            // the class; launching would dual-write). Advisory impls
            // (LocalPlacement, standalone) keep the P3c-1 warn-and-
            // continue behavior: the serialized gate above is their
            // mutual exclusion.
            //
            // MIG-17: the per-class `acquire → record → handoff →
            // restore → claim → seq-label` chain runs through
            // `do_switch_pipeline::run` at bounded concurrency — N
            // classes cost ~ceil(N/K) serial store round trips
            // instead of N×~8 inside the switch hold. Classes the
            // just-exited generation still held under the SAME worker
            // arrive carried (`on_exited` never released them):
            // `reaffirm` revalidates the same epoch for one read —
            // plus a renew only when already due — instead of a
            // fence + re-acquire CAS pair. Semantics are unchanged:
            // any failure releases the partial set, `retry_at` +5 s,
            // never a partial launch; phase 2 (writer claims) runs
            // only once every phase-1 result is Ok (WAL1), and
            // `wal.start` still follows all claims.
            // MIG-17: a spec with no do_storage can never consume the
            // serialized-switch carry — the whole set takes the
            // normal departing path now (same handling the do_spec
            // block gives leftover entries below).
            if desired.do_storage.is_none() {
                self.drain_carried();
            }
            if let Some(do_spec) = &desired.do_storage {
                // Consume the serialized-switch carry — a leftover
                // here means the desired moved on mid-switch (or the
                // attempt is an exclusion-reduced re-assembly), so
                // the lease takes the normal departing path now.
                let mut carried_map: std::collections::BTreeMap<(String, String), DoLeaseEntry> =
                    std::mem::take(&mut self.carried_leases)
                        .into_iter()
                        .map(|l| ((l.scope.worker.clone(), l.scope.class_key.clone()), l))
                        .collect();
                let carried_lost = std::mem::take(&mut self.carried_lost);
                let mut forced_release: Vec<DoLeaseEntry> = Vec::new();
                let mut inputs: Vec<crate::do_switch_pipeline::PipeIn> = Vec::new();
                for (worker_id, keys) in &do_spec.worker_classes {
                    for class_key in keys {
                        let scope = crate::do_placement::PlacementScope {
                            tenant: self.tenant_id.clone(),
                            worker: worker_id.clone(),
                            class_key: class_key.clone(),
                            class_dir: do_spec.tenant_dir.join(worker_id).join(class_key),
                        };
                        let pair = (worker_id.clone(), class_key.clone());
                        let transfer_src = do_spec.transfer_sources.get(&pair).cloned();
                        // MIG-17: a TRANSFERRED class is never
                        // carried — the lease must cycle (fence +
                        // re-acquire + handoff) exactly like the
                        // serial path so the Final-baseline gate
                        // still orders against a fresh epoch. A
                        // carried entry here (desired re-declared
                        // `from_worker_id` on the same worker mid-
                        // switch) is released now — the pipeline's
                        // plain acquire mints a fresh epoch.
                        let carry = match (transfer_src.is_some(), carried_map.remove(&pair)) {
                            (false, Some(l)) => crate::do_switch_pipeline::Carry::Lease(l.epoch),
                            (true, Some(l)) => {
                                forced_release.push(l);
                                crate::do_switch_pipeline::Carry::None
                            }
                            _ if transfer_src.is_none() && carried_lost.contains(&pair) => {
                                crate::do_switch_pipeline::Carry::Lost
                            }
                            _ => crate::do_switch_pipeline::Carry::None,
                        };
                        inputs.push(crate::do_switch_pipeline::PipeIn {
                            scope,
                            carry,
                            transfer_src,
                        });
                    }
                }
                if !forced_release.is_empty() {
                    Self::release_leases(
                        &*self.deps.placement,
                        &self.tenant_id,
                        &forced_release,
                    );
                }
                if !carried_map.is_empty() {
                    self.carried_leases = carried_map.into_values().collect();
                    self.drain_carried();
                }
                let pipe_t0 = Instant::now();
                let pipe_cfg = self.do_pipeline;
                let outcomes = {
                    let deps = crate::do_switch_pipeline::PipeDeps {
                        tenant: &self.tenant_id,
                        tenant_dir: &do_spec.tenant_dir,
                        placement: &*self.deps.placement,
                        snap: self
                            .deps
                            .do_snap
                            .as_deref()
                            .map(|s| crate::do_switch_pipeline::SnapView {
                                store: &s.store,
                                placement: &s.placement,
                                wal: s.wal.as_deref(),
                            }),
                        cfg: pipe_cfg,
                        t0: pipe_t0,
                    };
                    crate::do_switch_pipeline::run(&deps, &inputs)
                };
                // Fold outcomes back into the serial shape: `leases`
                // is the held set — the partial set on failure — and
                // `first_fail` reproduces the serial pass's verdict
                // (it evaluated every acquire before any handoff,
                // every restore before any claim, so the reported
                // failure is the earliest STAGE, first in spec
                // order).
                let mut leases: Vec<DoLeaseEntry> = Vec::new();
                let mut carried_n = 0usize;
                for o in &outcomes {
                    if let Some(lease) = &o.acquired {
                        leases.push(DoLeaseEntry {
                            scope: o.scope.clone(),
                            epoch: lease.epoch,
                            expires_at_ms: lease.expires_at_ms,
                        });
                        self.do_class_keys.insert(o.scope.class_key.clone());
                    }
                    if o.carried {
                        carried_n += 1;
                    }
                }
                let mut first_fail: Option<(usize, crate::do_switch_pipeline::FailStage)> = None;
                for want in [
                    crate::do_switch_pipeline::FailStage::Acquire,
                    crate::do_switch_pipeline::FailStage::Handoff,
                    crate::do_switch_pipeline::FailStage::Restore,
                    crate::do_switch_pipeline::FailStage::Claim,
                    // H3: sibling-skipped classes sort LAST — a real
                    // failure always exists beside an `Aborted` and
                    // must own `last_error`.
                    crate::do_switch_pipeline::FailStage::Aborted,
                ] {
                    if let Some(pos) = outcomes.iter().position(|o| {
                        o.result.as_ref().err().is_some_and(|f| f.stage() == want)
                    }) {
                        first_fail = Some((pos, want));
                        break;
                    }
                }
                // MIG-17: placement-pipeline wall time inside the
                // switch hold — the serial path had no equivalent
                // stage (it WAS the whole gap).
                if let Some(t) = self.do_switch_exit_at {
                    crate::metrics::registry().observe(
                        "vrt_host_do_switch_stage_seconds",
                        &[("tenant", &self.tenant_id), ("stage", "placement")],
                        t.elapsed().as_secs_f64(),
                    );
                }
                tracing::info!(
                    tenant = %self.tenant_id,
                    classes = inputs.len(),
                    concurrency = pipe_cfg
                        .concurrency
                        .clamp(1, crate::do_switch_pipeline::MAX_CONCURRENCY)
                        .min(inputs.len().max(1)),
                    carried = carried_n,
                    elapsed_ms = pipe_t0.elapsed().as_millis() as u64,
                    ok = first_fail.is_none(),
                    "do placement pipeline complete"
                );
                if let Some((pos, stage)) = first_fail {
                    let o = &outcomes[pos];
                    let err = o.result.as_ref().err().unwrap();
                    match stage {
                        crate::do_switch_pipeline::FailStage::Acquire => {
                            // Identical to the serial acquire failure —
                            // throttled fenced log, release partial
                            // set, retry +5 s, never a partial launch.
                            let now = Instant::now();
                            if self.fenced_log.0.is_none_or(|t| now >= t) {
                                let remaining_ttl_secs = match err {
                                    crate::do_switch_pipeline::PipeFail::Acquire(
                                        crate::do_placement::PlacementError::Fenced {
                                            until_ms,
                                            ..
                                        },
                                    )
                                    | crate::do_switch_pipeline::PipeFail::Acquire(
                                        crate::do_placement::PlacementError::Reserved {
                                            until_ms,
                                        },
                                    ) if *until_ms > 0 => Some(
                                        until_ms.saturating_sub(wall_now_ms()) / 1000,
                                    ),
                                    _ => None,
                                };
                                tracing::error!(
                                    tenant = %self.tenant_id,
                                    worker = %o.scope.worker,
                                    class = %o.scope.class_key,
                                    remaining_ttl_secs,
                                    err = %err.detail(),
                                    "do placement acquire failed; refusing to launch (fail closed)"
                                );
                                let next = self.fenced_log.1.min(Duration::from_secs(60));
                                self.fenced_log.0 = Some(now + next);
                                self.fenced_log.1 =
                                    (next * 2).min(Duration::from_secs(60));
                            }
                            self.runtime
                                .set_last_error(Some("do-placement-unavailable".into()));
                        }
                        crate::do_switch_pipeline::FailStage::Handoff => {
                            self.runtime
                                .set_last_error(Some("do-transfer-handoff".into()));
                        }
                        crate::do_switch_pipeline::FailStage::Restore => {
                            self.runtime
                                .set_last_error(Some("do-restore-failed".into()));
                        }
                        crate::do_switch_pipeline::FailStage::Claim => {
                            self.runtime.set_last_error(Some("do-writer-claim".into()));
                        }
                        // Unreachable in practice — a real failure
                        // always precedes an Aborted in the want
                        // order — but keep a sane label if it ever
                        // wins alone.
                        crate::do_switch_pipeline::FailStage::Aborted => {
                            self.runtime
                                .set_last_error(Some("do-placement-unavailable".into()));
                        }
                    }
                    // Release the partial set, stay not-launched, retry
                    // via the restart backoff — the OLD generation is
                    // already gone (serialized gate), so the class is
                    // down but NEVER dual-writing.
                    crate::do_switch_pipeline::release_all(
                        &*self.deps.placement,
                        &self.tenant_id,
                        &leases,
                        pipe_cfg.concurrency,
                    );
                    self.retry_at = Some(Instant::now() + Duration::from_secs(5));
                    return;
                }
                // All leases held — next fenced episode logs fresh.
                self.fenced_log = (None, Duration::from_secs(5));
                // WAL1: start tailing only AFTER claims succeed —
                // segments are epoch-namespaced; a class whose claim
                // failed never produces a stream.
                if let Some(snap) = &self.deps.do_snap {
                    if let Some(wal) = &snap.wal {
                        for o in &outcomes {
                            let Some(lease) = &o.acquired else {
                                continue;
                            };
                            // MIG-17c: reaffirm adopted a NEWER
                            // epoch than the carried one — the
                            // retained tail is still bound to the
                            // carried epoch. `wal.start` replaces it
                            // outright (old-epoch segments are
                            // unadoptable at the new epoch; unparsed
                            // frames fold into the db and land in
                            // the fresh tail's immediate first
                            // baseline — see `PipeOutcome::
                            // stale_tail_epoch`).
                            if let Some(stale) = o.stale_tail_epoch {
                                crate::metrics::registry().inc(
                                    "vrt_host_do_reaffirm_epoch_drift_total",
                                    &[("tenant", &self.tenant_id)],
                                    1.0,
                                );
                                tracing::warn!(
                                    tenant = %self.tenant_id,
                                    class = %o.scope.class_key,
                                    carried_epoch = stale,
                                    adopted_epoch = lease.epoch.0,
                                    "do reaffirm epoch drift — outgoing tail drains under its own labels"
                                );
                            }
                            let seq_label =
                                o.result.as_ref().map(|r| r.seq_label).unwrap_or(0);
                            wal.start(&o.scope, lease.epoch.0, seq_label);
                        }
                    }
                }
                // W6-r3: workflow-instance delete tombstone reclaim —
                // moved AFTER lease acquire + transfer handoff +
                // snapshot/WAL restore + writer-epoch claim, and still
                // BEFORE spawn:
                //  - lease: only classes this host owns may lose files
                //    (an authoritative acquire failure returned early —
                //    reclaim is never reached for unowned classes);
                //  - restore: a resurrected pre-delete file lands
                //    BEFORE this pass so the `_cf_KV.INSTANCE_METADATA`
                //    gate sees the post-restore truth, not pre-restore
                //    absence;
                //  - wal.start: `retire_db` lands on a registered
                //    ClassTail (class-level mark survives discovery);
                //  - safe point: `active.is_none() && draining.is_empty()`
                //    still holds — the serialized gate above ran in the
                //    same synchronous pass; `retire_db` inside reclaim
                //    drops the WalPin before each unlink.
                // Unleased classes' tombstones stay untouched; a spec
                // without do_storage reclaims nothing.
                if self.active.is_none() && self.draining.is_empty() {
                    let leased: std::collections::BTreeSet<String> =
                        leases.iter().map(|l| l.scope.class_key.clone()).collect();
                    crate::wf_delete::reclaim_tenant(
                        &self.tenant_id,
                        &self.deps.wf_tomb_dir,
                        &self.deps.do_snap.as_ref().and_then(|d| d.wal.clone()),
                        Some(&leased),
                    );
                }
                self.pending_do_leases = leases;
            }
        }
        // MIG-13: a warm generation swaps its live config instead of
        // spawning a fresh process — and no retry reassembly either:
        // the warm process IS this attempt (own work dir/sockets).
        // M3-r2: consume BEFORE the retry reassembly — a reassembled
        // spec would allocate a fresh gen-<n> dir the warm process
        // never uses (a leaked dir per retry).
        if let Some(ctx) = self.prewarm.take() {
            // MIG-13-r3: once the episode produces its launch attempt
            // it is spent — a failed swap's retry takes the serialized
            // path end-to-end instead of warming a second process for
            // the same desired.
            self.prewarm_burned = true;
            if let Some(warm) = ctx.warm {
                let work_dir = warm.gen.spec.work_dir.clone();
                let isolation = warm.gen.spec.isolation;
                let desired_digest = self
                    .desired_spec
                    .as_ref()
                    .map(|s| s.config_digest.clone())
                    .unwrap_or_default();
                self.seq += 1;
                let seq = self.seq;
                let ev_tx = self.ev_tx.clone();
                let ready_timeout = self.ready_timeout;
                let probe_timeout = self.probe_timeout;
                let kill_grace = self.kill_grace;
                let tenant = self.tenant_id.clone();
                let live_config = ctx.live_config.clone();
                let final_config = ctx.attempt.config_path.clone();
                let swap_needed = warm.swap_needed;
                // M2-r2: the pre-reload waitUntil bound — same cap the
                // do-switch drain applies to the OLD generation.
                let wu_grace = self
                    .wait_until_grace
                    .min(self.do_switch_wait_until_grace);
                crate::metrics::registry().observe(
                    "vrt_host_do_switch_stage_seconds",
                    &[("tenant", &self.tenant_id), ("stage", "warm_idle")],
                    warm.warmed_at.elapsed().as_secs_f64(),
                );
                let early_slots: Vec<Arc<EndpointSlot>> = warm
                    .early
                    .routing
                    .values()
                    .chain(warm.early.qconsumer.values())
                    .chain(warm.early.wfops.values())
                    .chain(warm.early.cron.values())
                    .cloned()
                    .collect();
                if swap_needed {
                    // MIG-13-r3: close the early slots BEFORE the maps
                    // empty — a resolver that already grabbed a slot
                    // `Arc` can then never acquire after the settle
                    // saw in_flight==0, so `swap_finalize`'s settle is
                    // a real quiesce (the r2 race could land a request
                    // on the reloading process).
                    for slot in &early_slots {
                        slot.close();
                    }
                    // The reload tears live listeners down — every
                    // early-flipped worker joins the hold set until
                    // install re-maps its route.
                    self.runtime.routing.store(Arc::new(BTreeMap::new()));
                    self.runtime
                        .qconsumer_routing
                        .store(Arc::new(BTreeMap::new()));
                    self.runtime.wfops_routing.store(Arc::new(BTreeMap::new()));
                    self.runtime
                        .wfop_token_routing
                        .store(Arc::new(BTreeMap::new()));
                    self.runtime.cron_routing.store(Arc::new(BTreeMap::new()));
                }
                let task = tokio::spawn(async move {
                    let result = swap_finalize(
                        warm,
                        live_config,
                        final_config,
                        early_slots,
                        ready_timeout,
                        probe_timeout,
                        kill_grace,
                        wu_grace,
                        isolation,
                        tenant,
                    )
                    .await;
                    let _ = ev_tx.send(Ev::Launched { seq, result }).await;
                });
                self.pending = Some(PendingLaunch {
                    seq,
                    task,
                    work_dir,
                    desired_digest,
                    leases: std::mem::take(&mut self.pending_do_leases),
                    prewarm_swap: true,
                });
                return;
            }
            if let Some(t) = ctx.task {
                t.abort();
            }
            // MIG-13-r3: an in-flight prewarm spawn superseded by the
            // drain completing leaves its gen dir spent — retire it
            // before the serialized launch below picks a dir.
            self.retire_prewarm_dir(&ctx.work_dir);
        }
        // T13/H2: REASSEMBLE every RETRY with a fresh generation so
        // the launch gets its own work dir and socket namespace — an
        // exiting predecessor's dir cleanup can then never touch them.
        // The first attempt reuses the converge-assembled spec (already
        // a unique gen dir); reassembling it too would orphan that dir.
        // MIG-13-r3: a burned prewarm makes this a retry regardless of
        // `attempted_once` — the desired's own dir was spent by the
        // warm episode (stale `*.sock`, `prewarm-disks`, the twin/live
        // configs), so the serialized retry MUST assemble into a fresh
        // `gen-<n>` dir; the spent one is retired by the abandon path.
        let assembler = if self.attempted_once || self.prewarm_burned {
            self.deps.assembler.clone()
        } else {
            None
        };
        self.attempted_once = true;
        let spec = match (assembler, desired.relaunch.clone()) {
            (Some(asm), Some(rl)) => {
                let gen = self.deps.gen_counter.fetch_add(1, Ordering::Relaxed) + 1;
                // MIG-4: `rl.tenant` is always the FULL desired tenant
                // — `self.excluded` filters it on every rebuild so a
                // reduced generation stays reduced across retries.
                let tenant = crate::exclusion::filtered_tenant(&rl.tenant, &self.excluded)
                    .unwrap_or_else(|| rl.tenant.clone());
                match asm.assemble(&tenant, rl.revision, gen) {
                    Ok(mut s) => {
                        if !self.excluded.is_empty() {
                            s.config_digest = desired.config_digest.clone();
                            s.relaunch = desired.relaunch.clone();
                        }
                        Box::new(s)
                    }
                    Err(error) => {
                        tracing::warn!(
                            tenant = %self.tenant_id,
                            %error,
                            "retry reassembly failed"
                        );
                        self.on_launch_failed(SpawnFailure {
                            reason: "assemble",
                            detail: None,
                        });
                        return;
                    }
                }
            }
            _ => desired.clone(),
        };
        let work_dir = spec.work_dir.clone();
        let desired_digest = self
            .desired_spec
            .as_ref()
            .map(|s| s.config_digest.clone())
            .unwrap_or_default();
        self.seq += 1;
        let seq = self.seq;
        let ev_tx = self.ev_tx.clone();
        let launcher = self.deps.launcher.clone();
        let probe = (self.deps.probe_factory)();
        let ready_timeout = self.ready_timeout;
        let probe_timeout = self.probe_timeout;
        let kill_grace = self.kill_grace;
        let task = tokio::spawn(async move {
            let result = launch_one(
                seq,
                spec,
                launcher,
                probe,
                ready_timeout,
                probe_timeout,
                kill_grace,
            )
            .await;
            let _ = ev_tx.send(Ev::Launched { seq, result }).await;
        });
        self.pending = Some(PendingLaunch {
            seq,
            task,
            work_dir,
            desired_digest,
            leases: std::mem::take(&mut self.pending_do_leases),
            prewarm_swap: false,
        });
    }

    /// T25/L17: bound generation-dir accumulation under `seg_dir` —
    /// keep every dir still referenced (active / pending / draining /
    /// no-assembler desired reuse) plus the NEWEST unreferenced one
    /// (diagnostics for the most recent failure); remove the rest.
    /// Dirs are removed only when no live process references them —
    /// `dir_referenced` covers the no-assembler retry-dir reuse too.
    fn sweep_gen_dirs(&mut self) {
        let Some(seg) = &self.seg_dir else { return };
        let Ok(entries) = std::fs::read_dir(seg) else {
            return;
        };
        let mut unreferenced: Vec<PathBuf> = Vec::new();
        for e in entries.flatten() {
            let path = e.path();
            let is_gen = e
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("gen-"));
            if !is_gen || self.dir_referenced(&path) {
                continue;
            }
            unreferenced.push(path);
        }
        // Keep the newest unreferenced dir (sort by mtime desc, then
        // name) — the failed generation's config/sockets stay around
        // for post-mortem; older ones are removed.
        for dir in select_sweep_dirs(unreferenced) {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// P3c-2: release held leases by fencing them — the holder is
    /// dead or dying, so the manifest gets a dead-marker at its
    /// epoch and the next acquirer takes `epoch+1` immediately
    /// instead of waiting out the lease TTL. Fencing a dead writer
    /// is safe by construction: writers at `<= epoch` are stale.
    fn release_leases(
        placement: &dyn crate::do_placement::DoPlacement,
        tenant: &str,
        leases: &[DoLeaseEntry],
    ) {
        Self::release_leases_bounded(placement, tenant, leases, None);
    }

    /// MIG-10: bounded variant for the shutdown/abort path — stops
    /// fencing once `budget` is spent; whatever remains lapses by TTL
    /// (the unchanged crash semantic — TTL is never shortened to
    /// "optimize" recovery). Release failures are logged and never
    /// block exit.
    fn release_leases_bounded(
        placement: &dyn crate::do_placement::DoPlacement,
        tenant: &str,
        leases: &[DoLeaseEntry],
        budget: Option<Duration>,
    ) {
        let t0 = Instant::now();
        for (i, l) in leases.iter().enumerate() {
            if budget.is_some_and(|b| t0.elapsed() >= b) {
                tracing::warn!(
                    class = %l.scope.class_key,
                    remaining = leases.len() - i,
                    "do lease release budget spent; remainder lapses by TTL"
                );
                break;
            }
            let t = Instant::now();
            if let Err(e) = placement.fence(&l.scope, l.epoch) {
                tracing::warn!(
                    class = %l.scope.class_key,
                    %e,
                    "do lease release (fence) failed; lease lapses by TTL"
                );
            }
            crate::metrics::registry().observe(
                "vrt_host_do_placement_op_seconds",
                &[("tenant", tenant), ("op", "release")],
                t.elapsed().as_secs_f64(),
            );
        }
    }

    /// P3c-2: renew every held lease due within `renew_margin_ms`;
    /// a renewal still failing inside `kill_margin_ms` of expiry
    /// forces the writer down BEFORE the lease lapses — never run a
    /// DO writer without a verified lease. Every lease outcome arms
    /// `do_lease_wake` at that lease's next decision boundary
    /// (renew_due / kill_due) so the kill guarantee does NOT depend on
    /// `monitor_interval` being smaller than the margin.
    fn service_do_leases(&mut self, now: Instant) {
        let now_ms = wall_now_ms();
        let renew_margin = self.deps.placement.renew_margin_ms();
        let kill_margin = self.deps.placement.kill_margin_ms();
        let mut wake_at: Option<Instant> = None;
        let mut min_remaining_ms = u64::MAX;
        let mut lost_active = false;
        let mut lost_draining: Vec<usize> = Vec::new();
        // Renew active + draining (draining gens still write until exit).
        let mut renew_set = |leases: &mut Vec<DoLeaseEntry>,
                             is_active: bool,
                             gen_idx: usize,
                             tenant: &str,
                             placement: &dyn crate::do_placement::DoPlacement,
                             lost_draining: &mut Vec<usize>| {
            for l in leases.iter_mut() {
                if l.expires_at_ms >= u64::MAX / 4 {
                    continue; // non-expiring (LocalPlacement)
                }
                let renew_due = l.expires_at_ms.saturating_sub(renew_margin);
                let kill_due = l.expires_at_ms.saturating_sub(kill_margin);
                min_remaining_ms = min_remaining_ms.min(l.expires_at_ms.saturating_sub(now_ms));
                let mut wake_in = |ms: u64| {
                    let cand = now + Duration::from_millis(ms.min(60_000));
                    wake_at = Some(wake_at.map_or(cand, |w: Instant| w.min(cand)));
                };
                if now_ms < renew_due {
                    wake_in(renew_due - now_ms);
                    continue;
                }
                let t_renew = Instant::now();
                let renew_res = placement.renew(&l.scope, l.epoch);
                crate::metrics::registry().observe(
                    "vrt_host_do_placement_op_seconds",
                    &[("tenant", tenant), ("op", "renew")],
                    t_renew.elapsed().as_secs_f64(),
                );
                match renew_res {
                    Ok(new_expiry) => {
                        l.expires_at_ms = new_expiry;
                        crate::metrics::registry().inc(
                            "vrt_host_do_lease_renewed_total",
                            &[("tenant", tenant)],
                            1.0,
                        );
                        // Re-arm at the next renewal boundary — the
                        // deadline never rides on monitor cadence.
                        wake_in(
                            new_expiry
                                .saturating_sub(renew_margin)
                                .saturating_sub(now_ms),
                        );
                    }
                    Err(
                        e @ (crate::do_placement::PlacementError::Fenced { .. }
                        | crate::do_placement::PlacementError::Precondition(_)),
                    ) => {
                        // P3c-2-r1: Fenced is a TERMINAL verdict — the
                        // epoch/holder provably moved on, nothing to
                        // retry. Every fenced writer dies NOW (not at
                        // kill_due): a migration fence must bound the
                        // stop-write window by the notify/renew cadence,
                        // never by expiry-minus-margin.
                        tracing::error!(
                            tenant = %tenant,
                            class = %l.scope.class_key,
                            %e,
                            "do lease fenced; stopping writer immediately"
                        );
                        if is_active {
                            lost_active = true;
                        } else {
                            lost_draining.push(gen_idx);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            tenant = %tenant,
                            class = %l.scope.class_key,
                            %e,
                            "do lease renew failed"
                        );
                        if now_ms >= kill_due {
                            if is_active {
                                lost_active = true;
                            } else {
                                // A DRAINING writer is still a writer —
                                // past the kill margin it goes down too
                                // (unavailable over dual-write).
                                lost_draining.push(gen_idx);
                            }
                        } else {
                            // Deterministic re-check at the kill
                            // boundary; the ≤60s cap keeps a bounded
                            // retry cadence in between.
                            wake_in(kill_due - now_ms);
                        }
                    }
                }
            }
        };
        if let Some(g) = &mut self.active {
            renew_set(
                &mut g.leases,
                true,
                usize::MAX,
                &self.tenant_id,
                &*self.deps.placement,
                &mut lost_draining,
            );
        }
        for (i, g) in self.draining.iter_mut().enumerate() {
            renew_set(
                &mut g.leases,
                false,
                i,
                &self.tenant_id,
                &*self.deps.placement,
                &mut lost_draining,
            );
        }
        // DO1-r2 P0-2a: HELD departing leases — dead writers awaiting
        // their `Final` commit. Renewed like normal leases (a lapse
        // would let the target fence into a stale restore); a Fenced/
        // Precondition verdict means the epoch provably moved on —
        // the entry drops, the pending retry takes the fenced-commit
        // path instead. No kill-margin: there is no writer to stop.
        let mut i = 0;
        while i < self.held_departing.len() {
            let l = &mut self.held_departing[i];
            if l.expires_at_ms >= u64::MAX / 4
                || now_ms < l.expires_at_ms.saturating_sub(renew_margin)
            {
                i += 1;
                continue;
            }
            let t_renew = Instant::now();
            let renew_res = self.deps.placement.renew(&l.scope, l.epoch);
            crate::metrics::registry().observe(
                "vrt_host_do_placement_op_seconds",
                &[("tenant", &self.tenant_id), ("op", "renew")],
                t_renew.elapsed().as_secs_f64(),
            );
            match renew_res {
                Ok(new_expiry) => {
                    l.expires_at_ms = new_expiry;
                    crate::metrics::registry().inc(
                        "vrt_host_do_lease_renewed_total",
                        &[("tenant", &self.tenant_id), ("kind", "held-departing")],
                        1.0,
                    );
                    i += 1;
                }
                Err(
                    e @ (crate::do_placement::PlacementError::Fenced { .. }
                    | crate::do_placement::PlacementError::Precondition(_)),
                ) => {
                    tracing::error!(
                        tenant = %self.tenant_id,
                        class = %l.scope.class_key,
                        %e,
                        "held departing lease fenced; dropping hold (pending retry takes fenced-commit path)"
                    );
                    self.held_departing.remove(i);
                }
                Err(e) => {
                    tracing::warn!(
                        tenant = %self.tenant_id,
                        class = %l.scope.class_key,
                        %e,
                        "held departing lease renew failed"
                    );
                    i += 1;
                }
            }
        }
        // MIG-17: carried switch leases — same shape as
        // held_departing (dead writer, no kill-margin) but ANY
        // renewal failure voids the carry: the pending launch must
        // never open the class on an unverifiable lease (fail
        // closed). The entry drops into `carried_lost`, is fenced
        // best-effort, and the launch's pipeline consume takes the
        // classic acquire-failure path (release partial set, retry
        // +5 s); the retry's normal acquire then decides cleanly.
        let mut i = 0;
        while i < self.carried_leases.len() {
            let l = &mut self.carried_leases[i];
            if l.expires_at_ms >= u64::MAX / 4
                || now_ms < l.expires_at_ms.saturating_sub(renew_margin)
            {
                i += 1;
                continue;
            }
            let t_renew = Instant::now();
            let renew_res = self.deps.placement.renew(&l.scope, l.epoch);
            crate::metrics::registry().observe(
                "vrt_host_do_placement_op_seconds",
                &[("tenant", &self.tenant_id), ("op", "renew")],
                t_renew.elapsed().as_secs_f64(),
            );
            match renew_res {
                Ok(new_expiry) => {
                    l.expires_at_ms = new_expiry;
                    crate::metrics::registry().inc(
                        "vrt_host_do_lease_renewed_total",
                        &[("tenant", &self.tenant_id), ("kind", "carried")],
                        1.0,
                    );
                    i += 1;
                }
                Err(e) => {
                    let terminal = matches!(
                        e,
                        crate::do_placement::PlacementError::Fenced { .. }
                            | crate::do_placement::PlacementError::Precondition(_)
                    );
                    if terminal {
                        tracing::error!(
                            tenant = %self.tenant_id,
                            class = %l.scope.class_key,
                            %e,
                            "carried do lease fenced; voiding carry (launch fails closed)"
                        );
                    } else {
                        tracing::warn!(
                            tenant = %self.tenant_id,
                            class = %l.scope.class_key,
                            %e,
                            "carried do lease renew failed; voiding carry (launch fails closed)"
                        );
                    }
                    let l = self.carried_leases.remove(i);
                    self.carried_lost
                        .insert((l.scope.worker.clone(), l.scope.class_key.clone()));
                    crate::metrics::registry().inc(
                        "vrt_host_do_lease_lost_total",
                        &[("tenant", &self.tenant_id), ("gen", "carried")],
                        1.0,
                    );
                    Self::release_leases(&*self.deps.placement, &self.tenant_id, &[l]);
                }
            }
        }
        if min_remaining_ms != u64::MAX {
            crate::metrics::registry().set(
                "vrt_host_do_lease_remaining_seconds",
                &[("tenant", &self.tenant_id)],
                min_remaining_ms as f64 / 1000.0,
            );
        }
        self.do_lease_wake = wake_at;
        if lost_active {
            crate::metrics::registry().inc(
                "vrt_host_do_lease_lost_total",
                &[("tenant", &self.tenant_id)],
                1.0,
            );
            self.lease_lost_kill_active();
        }
        for idx in lost_draining.into_iter().rev() {
            crate::metrics::registry().inc(
                "vrt_host_do_lease_lost_total",
                &[("tenant", &self.tenant_id), ("gen", "draining")],
                1.0,
            );
            self.lease_lost_kill_draining(idx);
        }
    }

    /// P3c-2: a draining generation whose lease renewal is still
    /// failing at `kill_due` is force-killed — same kill-on-drop
    /// guard as the active path; its slots are already closed, so the
    /// only loss is in-flight requests (chosen over unfenced writes).
    fn lease_lost_kill_draining(&mut self, idx: usize) {
        if idx >= self.draining.len() {
            return;
        }
        let g = self.draining.remove(idx);
        tracing::error!(
            tenant = %self.tenant_id,
            pid = g.pid,
            "do lease lost inside kill margin; killing draining writer before expiry"
        );
        // Same rationale as `lease_lost_kill_active`: the failure was
        // observational — no fence; the TTL already bounds the lease.
        g.watcher.abort();
    }

    /// P3c-2: periodic epoch-fenced snapshot per active class lease.
    /// Upload + manifest commit are CAS-gated on the CURRENT epoch —
    /// a fenced/stale writer's upload can never be referenced (it
    /// becomes GC-eligible orphans). Interval + jitter come from
    /// `DoSnapCtx`; failures are metric'd and retried next tick —
    /// never block request serving on the snapshot path.
    fn service_do_snapshots(&mut self) {
        let Some(snap) = self.deps.do_snap.clone() else {
            return;
        };
        let now_ms = wall_now_ms();
        let Some(active) = &mut self.active else {
            return;
        };
        for l in active.leases.iter_mut() {
            if l.expires_at_ms >= u64::MAX / 4 {
                continue;
            }
            // WAL1: a broken stream (cap breach / lost frames) asks
            // for a new baseline NOW — the forced snapshot re-anchors
            // the chain; covered by snapshot bounds, not a hot loop.
            if let Some(wal) = &snap.wal {
                if wal.take_needs_baseline(&l.scope.class_key) {
                    self.do_snap_due.insert(l.scope.class_key.clone(), 0);
                }
            }
            let due = *self
                .do_snap_due
                .get(&l.scope.class_key)
                .unwrap_or(&u64::MAX);
            if now_ms < due {
                continue;
            }
            // Next due BEFORE the work — a slow/failed upload can't
            // hot-loop; jitter decoheres tenants sharing the store.
            let jitter = (self.deps.jitter)(snap.jitter_ms.max(1));
            self.do_snap_due.insert(
                l.scope.class_key.clone(),
                now_ms + snap.interval_ms + jitter,
            );
            let t0 = Instant::now();
            let seq = now_ms;
            // WAL1: re-label BEFORE the backup starts — frames past
            // image-start land under this baseline's seq.
            if let Some(wal) = &snap.wal {
                wal.on_snapshot_begin(&l.scope.class_key, seq);
            }
            // WAL1-r3 P0-B: {B,E} anchor recorded at image-taken —
            // after the backup's read txn closed, before commit.
            let wal_img_hook = snap.wal.as_ref().map(|w| {
                let k = l.scope.class_key.clone();
                let w = w.clone();
                move || w.on_snapshot_image_taken(&k, seq)
            });
            match crate::do_snapshot::upload_snapshot(
                &snap.store,
                &snap.placement,
                &l.scope,
                l.epoch,
                seq,
                &snap.tmp_root,
                crate::do_placement_store::SnapKind::Periodic,
                wal_img_hook.as_ref().map(|f| f as &dyn Fn()),
            ) {
                Ok(r) => {
                    // WAL1: confirm baseline + bounded GC of covered
                    // segments (seq < committed) and stale epochs.
                    if let Some(wal) = &snap.wal {
                        wal.on_snapshot_committed(&l.scope.class_key, r.seq);
                        // RV-R24: GC deletes objects — run it only
                        // while THIS host still provably holds the
                        // lease at this epoch. The just-committed
                        // manifest is CAS-checked, but the window
                        // between commit and delete is exactly where a
                        // fence could land; a stale holder must never
                        // garbage-collect a successor's stream.
                        let placement = snap.placement.clone();
                        let scope = l.scope.clone();
                        let epoch = l.epoch.0;
                        wal.gc_segments(&l.scope.class_key, epoch, r.seq, move || {
                            matches!(
                                placement.record(&scope),
                                Ok(Some(rec))
                                    if rec.epoch == epoch
                                        && rec.holder == placement.holder()
                            )
                        });
                    }
                    self.do_snap_ok_at.insert(l.scope.class_key.clone(), now_ms);
                    let r_m = crate::metrics::registry();
                    r_m.inc(
                        "vrt_host_do_snapshots_total",
                        &[("tenant", &self.tenant_id), ("outcome", "ok")],
                        1.0,
                    );
                    r_m.observe(
                        "vrt_host_do_snapshot_bytes",
                        &[("tenant", &self.tenant_id)],
                        r.bytes as f64,
                    );
                    r_m.observe(
                        "vrt_host_do_snapshot_seconds",
                        &[("tenant", &self.tenant_id)],
                        t0.elapsed().as_secs_f64(),
                    );
                }
                Err(e) => {
                    // WAL1-r2 P1-1: roll seg labels back to the still-
                    // committed baseline — frames emitted under the
                    // orphan label are never adopted, and a fresh
                    // baseline is requested immediately.
                    if let Some(wal) = &snap.wal {
                        wal.on_snapshot_failed(&l.scope.class_key);
                    }
                    crate::metrics::registry().inc(
                        "vrt_host_do_snapshots_total",
                        &[("tenant", &self.tenant_id), ("outcome", "failed")],
                        1.0,
                    );
                    tracing::warn!(
                        tenant = %self.tenant_id,
                        class = %l.scope.class_key,
                        %e,
                        "do periodic snapshot failed"
                    );
                }
            }
        }
        // Snapshot-age gauge (min across classes → worst-case staleness).
        let mut worst = u64::MAX;
        for (k, ok_at) in &self.do_snap_ok_at {
            if self
                .active
                .as_ref()
                .is_some_and(|a| a.leases.iter().any(|l| &l.scope.class_key == k))
            {
                worst = worst.min(now_ms.saturating_sub(*ok_at));
            }
        }
        if worst != u64::MAX {
            crate::metrics::registry().set(
                "vrt_host_do_snapshot_age_seconds",
                &[("tenant", &self.tenant_id)],
                worst as f64 / 1000.0,
            );
        }
    }

    /// DO1-r2 P0-2a/P1-2: bounded retry for departing-class `Final`
    /// snapshots. Each entry waits out its own exponential backoff
    /// (`base * 2^attempts`, capped); failures warn, a crossing of
    /// `departing_max_ms` escalates to error ONCE (`final-exhausted`
    /// metric) but the entry — and for transfers the HELD lease —
    /// stays until the baseline lands or an operator intervenes.
    /// A fenced epoch (someone else provably owns the class) is
    /// terminal: the entry leaves the retry set, the source dir is
    /// QUARANTINED (P1-1 retention — fenced provenance is evidence,
    /// not garbage), and the marker clears. Success releases the
    /// held lease: commit ⇒ fence is now safe, the target's gate
    /// sees `kind=Final`.
    fn service_departing_snapshots(&mut self) {
        if self.departing_snap_pending.is_empty() {
            return;
        }
        let Some(snap) = self.deps.do_snap.clone() else {
            self.departing_snap_pending.clear();
            return;
        };
        let now = Instant::now();
        let now_ms = wall_now_ms();
        let pending: Vec<(String, String)> = self.departing_snap_pending.keys().cloned().collect();
        for pair in pending {
            let Some(entry) = self.departing_snap_pending.get(&pair).cloned() else {
                continue;
            };
            if now < entry.next_retry_at {
                continue;
            }
            let mut entry = entry;
            if !entry.scope.class_dir.is_dir() {
                // RV-R24: the pending source dir VANISHED before its
                // `Final` committed — the only copy is gone, so this
                // is NOT a completed lifecycle. Releasing the held
                // lease would let an acquirer fence into a missing or
                // stale baseline. Fail closed: keep the lease AND the
                // pending entry, escalate ONCE (metric + error), and
                // wait for the operator — a restored dir resumes the
                // normal retry path below.
                if !entry.dir_missing {
                    if let Some(stored) = self.departing_snap_pending.get_mut(&pair) {
                        stored.dir_missing = true;
                    }
                    crate::metrics::registry().inc(
                        "vrt_host_do_snapshots_total",
                        &[
                            ("tenant", &self.tenant_id),
                            ("outcome", "final-dir-missing"),
                        ],
                        1.0,
                    );
                    tracing::error!(
                        tenant = %self.tenant_id,
                        worker = %entry.scope.worker,
                        class = %entry.scope.class_key,
                        transfer = entry.is_transfer,
                        "do departing-class source dir VANISHED with final snapshot still pending; held lease kept — operator action required (fail closed)"
                    );
                }
                continue;
            }
            // Escalation ceiling: error-level once, keep retrying —
            // the held lease must NOT ride on an operator's latency.
            // (A dir that reappeared after a vanish clears the flag
            // when the entry is written back or removed below.)
            entry.dir_missing = false;
            if !entry.exhausted
                && now_ms.saturating_sub(entry.first_failed_ms) > snap.departing_max_ms
            {
                entry.exhausted = true;
                crate::metrics::registry().inc(
                    "vrt_host_do_snapshots_total",
                    &[("tenant", &self.tenant_id), ("outcome", "final-exhausted")],
                    1.0,
                );
                tracing::error!(
                    tenant = %self.tenant_id,
                    worker = %entry.scope.worker,
                    class = %entry.scope.class_key,
                    transfer = entry.is_transfer,
                    held_ms = now_ms.saturating_sub(entry.first_failed_ms),
                    "do departing-class final snapshot retries EXHAUSTED the configured window; lease still held — operator action required (data preserved)"
                );
            }
            let seq = now_ms;
            let held = matches!(
                snap.placement.record(&entry.scope),
                Ok(Some(rec)) if rec.holder == snap.placement.holder()
            );
            let outcome: Result<(), String> = if held {
                crate::do_snapshot::upload_snapshot(
                    &snap.store,
                    &snap.placement,
                    &entry.scope,
                    crate::do_placement::PlacementEpoch(entry.epoch),
                    seq,
                    &snap.tmp_root,
                    crate::do_placement_store::SnapKind::Final,
                    None, // Final adopts no chain — no anchor
                )
                .map(|_| ())
                .map_err(|e| e.to_string())
            } else {
                let migrator = crate::do_migration::Migrator {
                    store: snap.store.clone(),
                    placement: snap.placement.clone(),
                    tmp_root: snap.tmp_root.clone(),
                    stage_timeout: Duration::from_secs(30),
                    reserve_ms: 0,
                    poll: Duration::from_millis(50),
                };
                migrator
                    .snapshot_commit(
                        &entry.scope,
                        entry.epoch,
                        seq,
                        crate::do_placement_store::SnapKind::Final,
                    )
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            };
            match outcome {
                Ok(()) => {
                    self.departing_snap_pending.remove(&pair);
                    clear_final_pending(&entry.scope);
                    self.release_held_departing(&pair);
                    crate::metrics::registry().inc(
                        "vrt_host_do_snapshots_total",
                        &[("tenant", &self.tenant_id), ("outcome", "final-retried-ok")],
                        1.0,
                    );
                    tracing::info!(
                        tenant = %self.tenant_id,
                        class = %entry.scope.class_key,
                        attempts = entry.attempts,
                        "do departing-class final snapshot committed on retry"
                    );
                }
                Err(e) => {
                    // Terminal-fenced check: the epoch moved past us or
                    // a DIFFERENT live holder owns it — no commit can
                    // ever land on this entry's epoch again.
                    let fenced_terminal = match snap.placement.record(&entry.scope) {
                        // The placement object itself is gone — this
                        // epoch can never commit again; keep the data
                        // in quarantine instead of retrying forever.
                        Ok(None) => true,
                        Ok(Some(rec)) => {
                            rec.epoch > entry.epoch
                                || (rec.epoch == entry.epoch
                                    && !rec.holder.is_empty()
                                    && rec.holder != snap.placement.holder())
                        }
                        Err(_) => false,
                    };
                    if fenced_terminal {
                        self.departing_snap_pending.remove(&pair);
                        clear_final_pending(&entry.scope);
                        self.release_held_departing(&pair);
                        crate::metrics::registry().inc(
                            "vrt_host_do_snapshots_total",
                            &[("tenant", &self.tenant_id), ("outcome", "final-fenced")],
                            1.0,
                        );
                        tracing::error!(
                            tenant = %self.tenant_id,
                            worker = %entry.scope.worker,
                            class = %entry.scope.class_key,
                            transfer = entry.is_transfer,
                            %e,
                            "do departing-class final snapshot fenced terminally; source dir quarantined for operator recovery"
                        );
                        // DO1-r3: quarantine ONLY when no live local
                        // generation re-owns this key. A delete→re-
                        // declare leaves a stale pending entry whose
                        // class_dir now IS the new writer's live dir —
                        // moving it would yank a live database out
                        // from under workerd. The reowned check is the
                        // same as `release_held_departing`'s.
                        if !self.do_class_key_reowned(&entry.scope.class_key) {
                            let worker_dir =
                                entry.scope.class_dir.parent().map(|p| p.to_path_buf());
                            if let Some(wd) = worker_dir {
                                let tenant_dir = wd.parent().map(|p| p.to_path_buf());
                                if let Some(td) = tenant_dir {
                                    let _ = crate::do_transfer::quarantine_src(
                                        &td,
                                        &entry.scope.worker,
                                        &entry.scope.class_key,
                                    );
                                }
                            }
                        }
                        continue;
                    }
                    entry.attempts += 1;
                    let backoff = snap
                        .departing_retry_base_ms
                        .saturating_mul(1u64 << entry.attempts.min(20))
                        .min(snap.departing_retry_cap_ms);
                    entry.next_retry_at = now + Duration::from_millis(backoff);
                    crate::metrics::registry().inc(
                        "vrt_host_do_snapshots_total",
                        &[
                            ("tenant", &self.tenant_id),
                            ("outcome", "final-retry-failed"),
                        ],
                        1.0,
                    );
                    // P1-2: retry failures are warn (escalating to the
                    // once-only error at the exhaustion ceiling above).
                    tracing::warn!(
                        tenant = %self.tenant_id,
                        class = %entry.scope.class_key,
                        attempts = entry.attempts,
                        backoff_ms = backoff,
                        %e,
                        "do departing-class snapshot retry failed (still pending)"
                    );
                    self.departing_snap_pending.insert(pair, entry);
                }
            }
        }
    }

    /// True when ANY live local generation (active, launching,
    /// awaiting DO leases, or still draining) holds a lease entry
    /// for `class_key` — i.e. the dir at that scope is (or will be)
    /// a live writer's storage and must never be fenced/quarantined.
    fn do_class_key_reowned(&self, class_key: &str) -> bool {
        self.active
            .as_ref()
            .is_some_and(|g| g.leases.iter().any(|x| x.scope.class_key == class_key))
            || self
                .pending
                .as_ref()
                .is_some_and(|p| p.leases.iter().any(|x| x.scope.class_key == class_key))
            || self
                .pending_do_leases
                .iter()
                .any(|x| x.scope.class_key == class_key)
            || self
                .draining
                .iter()
                .any(|g| g.leases.iter().any(|x| x.scope.class_key == class_key))
    }

    /// Release a held departing lease after its `Final` baseline
    /// committed (or the entry went terminal). Fences ONLY when no
    /// live/pending/draining generation re-owns the key: a same-
    /// holder `acquire` reaffirms the SAME epoch for a same-cell
    /// destination worker, and fencing then would kill that live
    /// generation. When nothing local owns it (cross-cell handoff),
    /// the prompt dead-marker lets the target acquire epoch+1 over
    /// the guaranteed `kind=Final` baseline instead of waiting out
    /// the TTL.
    fn release_held_departing(&mut self, pair: &(String, String)) {
        if let Some(pos) = self
            .held_departing
            .iter()
            .position(|l| l.scope.worker == pair.0 && l.scope.class_key == pair.1)
        {
            let l = self.held_departing.remove(pos);
            if !self.do_class_key_reowned(&pair.1) {
                Self::release_leases(&*self.deps.placement, &self.tenant_id, &[l]);
            }
        }
    }

    /// P3c-2-r1: local-migration freeze — the admin path already
    /// CAS-committed the fence; this drains the request queue and
    /// kills the writer NOW instead of waiting for the next renew
    /// failure (the P0-2 dual-write window). Collateral: one workerd
    /// hosts the tenant's whole worker set — freezing one class downs
    /// the generation, which is the bounded stop-write window frozen
    /// migration promises. The post-kill retry loop re-acquires only
    /// when the reservation releases it.
    /// WAL1: drive per-class tailers each service pass. The
    /// replicator self-throttles (`tick_ms`) and self-cleans tails
    /// whose leases are gone (`retain`).
    fn service_do_wal(&mut self) {
        let Some(snap) = self.deps.do_snap.clone() else {
            return;
        };
        let Some(wal) = snap.wal.clone() else {
            return;
        };
        let mut active = std::collections::HashSet::new();
        if let Some(a) = &self.active {
            for l in &a.leases {
                active.insert(l.scope.class_key.clone());
            }
        }
        // Departing classes keep their tails until `flush` runs
        // inside the final-snapshot path; pending-launch classes
        // were `start`ed right after the writer claim.
        for l in &self.held_departing {
            active.insert(l.scope.class_key.clone());
        }
        for l in &self.pending_do_leases {
            active.insert(l.scope.class_key.clone());
        }
        // MIG-17-r2 (H1): a CARRIED class keeps its tail across the
        // switch window — same epoch, same dir ⇒ the same-epoch
        // `(seq_label, lsn)` stream continues verbatim (no re-parse,
        // no coverage gap, no key collision). Dropping it here would
        // re-anchor a fresh tail that cannot emit under old labels
        // anyway (anchor gate), but keeping it preserves continuity.
        // A `carried_lost` entry stays OUT — its lease is voided.
        for l in &self.carried_leases {
            active.insert(l.scope.class_key.clone());
        }
        wal.retain(&active);
        for k in active {
            wal.tick(&k);
        }
    }

    fn service_do_freeze(&mut self) {
        let keys = self.runtime.take_do_freeze_requests();
        if keys.is_empty() {
            return;
        }
        let mut kill_active = false;
        let mut kill_draining: Vec<usize> = Vec::new();
        let mut kill_pending = false;
        for key in &keys {
            if self
                .active
                .as_ref()
                .is_some_and(|g| g.leases.iter().any(|l| &l.scope.class_key == key))
            {
                kill_active = true;
            }
            for (i, g) in self.draining.iter().enumerate() {
                if g.leases.iter().any(|l| &l.scope.class_key == key) {
                    kill_draining.push(i);
                }
            }
            // In-flight/pre-spawn writers hold the (now fenced) lease
            // and would open the class dir at launch-complete — the
            // freeze must abort them too, or the fenced writer comes
            // RIGHT BACK (a real dual-write window).
            // MIG-17-r2 (H2): carried + voided-carry pairs count too —
            // the pending launch would re-acquire (or fail closed on
            // `Carry::Lost`) a frozen class; aborting the attempt is
            // the enforced stop.
            if self
                .pending
                .as_ref()
                .is_some_and(|p| p.leases.iter().any(|l| &l.scope.class_key == key))
                || self
                    .pending_do_leases
                    .iter()
                    .any(|l| &l.scope.class_key == key)
                || self
                    .carried_leases
                    .iter()
                    .any(|l| &l.scope.class_key == key)
                || self
                    .carried_lost
                    .iter()
                    .any(|(_, k)| k == key)
            {
                kill_pending = true;
            }
        }
        if kill_pending {
            crate::metrics::registry().inc(
                "vrt_host_do_lease_lost_total",
                &[("tenant", &self.tenant_id), ("reason", "freeze")],
                1.0,
            );
            self.abort_pending();
            // Pre-spawn lease entries belong to the same aborted
            // attempt — release them so the reserved lease is not
            // held by a generation that will never exist.
            Self::release_leases(&*self.deps.placement, &self.tenant_id, &self.pending_do_leases);
            self.pending_do_leases.clear();
        }
        if kill_active {
            crate::metrics::registry().inc(
                "vrt_host_do_lease_lost_total",
                &[("tenant", &self.tenant_id), ("reason", "freeze")],
                1.0,
            );
            self.lease_lost_kill_active();
        }
        for idx in kill_draining.into_iter().rev() {
            self.lease_lost_kill_draining(idx);
        }
    }

    /// P3c-2: lease lost — stop the writer NOW (bounded by the kill
    /// margin remaining before expiry). Routing is cleared so the
    /// class stops accepting requests immediately; the retry path
    /// re-acquires (epoch+1) when the store recovers. We do NOT fence:
    /// the renew failure was observational — we may still be the
    /// legitimate holder, and fencing now would only race the expiry
    /// that already bounds the old writer's lifetime.
    fn lease_lost_kill_active(&mut self) {
        tracing::error!(
            tenant = %self.tenant_id,
            "do lease lost inside kill margin; stopping writer before expiry"
        );
        if let Some(gen) = self.active.take() {
            // Aborting the exit watcher drops the GroupChild — its
            // kill-on-drop guard SIGKILLs the process group.
            gen.watcher.abort();
            // MIG-11-r2: `serving` drops BEFORE the maps empty — the
            // fenced-kill overlap must resolve `NoServing`, never a
            // false "not declared" 404.
            self.runtime.set_serving(false);
            self.runtime.routing.store(Arc::new(BTreeMap::new()));
            self.runtime
                .qconsumer_routing
                .store(Arc::new(BTreeMap::new()));
            self.runtime.wfops_routing.store(Arc::new(BTreeMap::new()));
            self.runtime
                .wfop_token_routing
                .store(Arc::new(BTreeMap::new()));
            self.runtime.cron_routing.store(Arc::new(BTreeMap::new()));
            self.runtime.set_active_pid(0);
            self.runtime.stats.crashes.fetch_add(1, Ordering::Relaxed);
            self.transit(Transition::Crash);
            self.retry_at = Some(Instant::now() + Duration::from_secs(5));
        }
    }

    /// MIG-17: is this class still desired under the SAME worker in
    /// the pending spec — the carry condition (same host, same
    /// worker, same class_key).
    fn desired_still_wants(
        &self,
        scope: &crate::do_placement::PlacementScope,
    ) -> bool {
        self.desired_spec
            .as_ref()
            .and_then(|s| s.do_storage.as_ref())
            .and_then(|d| d.worker_classes.get(&scope.worker))
            .is_some_and(|keys| keys.iter().any(|k| k == &scope.class_key))
    }

    /// MIG-17-r2 (H2): drain the serialized-switch carry through the
    /// normal departing path — a `Final` snapshot attempt for classes
    /// that leave ownership, release for every entry not held back
    /// for a pending commit, `held_departing` for the rest. The
    /// `carried_lost` set only clears (its leases were already fenced
    /// when voided). Used by abort/freeze/do_storage-None paths —
    /// every exit from the carry leaves no lease dangling.
    fn drain_carried(&mut self) {
        if self.carried_leases.is_empty() && self.carried_lost.is_empty() {
            return;
        }
        let leftover = std::mem::take(&mut self.carried_leases);
        self.carried_lost.clear();
        let held = self.final_snapshot_departing(&leftover);
        let release: Vec<DoLeaseEntry> = leftover
            .into_iter()
            .filter(|l| {
                !held.iter().any(|h| {
                    h.scope.worker == l.scope.worker && h.scope.class_key == l.scope.class_key
                })
            })
            .collect();
        Self::release_leases(&*self.deps.placement, &self.tenant_id, &release);
        self.held_departing.extend(held);
    }

    /// DO1-r2: final epoch-fenced snapshot for leases that LEAVE this
    /// cell's ownership under the pending desired — a transferred
    /// class's key re-appears under a DIFFERENT worker (dst), and a
    /// deleted class disappears entirely. Runs from `on_exited`'s
    /// reap — AFTER the exit watcher proved the writer dead — so the
    /// snapshot includes every in-flight/alarm write, and commits
    /// while the generation still holds the lease (CAS-valid) so a
    /// target cell can never observe "released + no baseline".
    ///
    /// Returns the leases that must stay HELD (P0-2a): a transferred
    /// class whose `Final` commit failed keeps its lease (renewed,
    /// never fencing) until the baseline lands — fencing early would
    /// let the target restore a periodic snapshot older than the data
    /// still sitting un-uploaded in the source dir. Deleted classes
    /// never hold: an absent class has no target to fork, the pending
    /// retry is a best-effort backup only.
    ///
    /// A missing/empty class dir still commits a 0-file `Final`
    /// manifest when no baseline exists (honest "source had no data"
    /// — unblocks the target's fail-closed gate); an existing baseline
    /// is never clobbered by an empty commit.
    fn final_snapshot_departing(&mut self, leases: &[DoLeaseEntry]) -> Vec<DoLeaseEntry> {
        let mut hold = Vec::new();
        let Some(snap) = self.deps.do_snap.clone() else {
            return hold;
        };
        let desired_workers: std::collections::BTreeMap<String, Vec<String>> = self
            .desired_spec
            .as_ref()
            .and_then(|s| s.do_storage.as_ref())
            .map(|d| d.worker_classes.clone())
            .unwrap_or_default();
        for l in leases {
            // Still owned here under the SAME worker — not departing.
            if desired_workers
                .get(&l.scope.worker)
                .is_some_and(|keys| keys.iter().any(|k| k == &l.scope.class_key))
            {
                continue;
            }
            // MIG-17-r3 (marker cleanup): this class leaves the cell
            // — drop any `wal-replay-pending` sibling bound to an OLD
            // writer epoch so a later same-name class on this worker
            // never sees a stale bind. Best-effort, before the
            // snapshot staging below (uses l.scope, whose class_dir
            // still points at the real cell dir).
            crate::do_snapshot::clear_replay_pending(&l.scope);
            // Transfer = the KEY survives under a different worker;
            // delete = the key is gone entirely.
            let is_transfer = desired_workers.iter().any(|(w, keys)| {
                w != &l.scope.worker && keys.iter().any(|k| k == &l.scope.class_key)
            });
            let empty_dir = !crate::do_transfer::dir_nonempty(&l.scope.class_dir);
            if empty_dir {
                match snap.placement.snapshot_ref(&l.scope) {
                    Ok(Some(_)) => continue, // never clobber a real baseline with an empty commit
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            tenant = %self.tenant_id,
                            class = %l.scope.class_key,
                            %e,
                            "do final snapshot: placement read failed"
                        );
                        continue;
                    }
                }
            }
            let mut scope = l.scope.clone();
            let mut staged_empty = std::path::PathBuf::new();
            if empty_dir {
                staged_empty = snap
                    .tmp_root
                    .join(format!("empty-{}-{}", scope.class_key, l.epoch.0));
                let _ = std::fs::remove_dir_all(&staged_empty);
                if std::fs::create_dir_all(&staged_empty).is_err() {
                    continue;
                }
                scope.class_dir = staged_empty.clone();
            }
            // WAL1-r2: drain the tail BEFORE the Final snapshot under
            // the CURRENT (previous) label only — the Final image is
            // the complete truth and adopts no chain, so flushing
            // pre-image frames under its seq would produce segments
            // that must never be applied (P0-3). Best-effort +
            // wall-bounded; Final stays the ground truth.
            let seq = wall_now_ms();
            if let Some(wal) = &snap.wal {
                wal.flush(&scope);
            }
            match crate::do_snapshot::upload_snapshot(
                &snap.store,
                &snap.placement,
                &scope,
                l.epoch,
                seq,
                &snap.tmp_root,
                crate::do_placement_store::SnapKind::Final,
                None, // Final adopts no chain — no anchor needed
            ) {
                Ok(r) => {
                    crate::metrics::registry().inc(
                        "vrt_host_do_snapshots_total",
                        &[("tenant", &self.tenant_id), ("outcome", "final")],
                        1.0,
                    );
                    tracing::info!(
                        tenant = %self.tenant_id,
                        worker = %l.scope.worker,
                        class = %l.scope.class_key,
                        transfer = is_transfer,
                        bytes = r.bytes,
                        empty = empty_dir,
                        "do departing-class final snapshot committed"
                    );
                }
                Err(e) => {
                    crate::metrics::registry().inc(
                        "vrt_host_do_snapshots_total",
                        &[("tenant", &self.tenant_id), ("outcome", "final-failed")],
                        1.0,
                    );
                    tracing::error!(
                        tenant = %self.tenant_id,
                        worker = %l.scope.worker,
                        class = %l.scope.class_key,
                        transfer = is_transfer,
                        %e,
                        "do departing-class final snapshot failed; pending retry (source dir kept, lease held for transfers)"
                    );
                    // Failure preservation: keep the (worker,key) →
                    // pending entry so the monitor retries with
                    // backoff AND the sweep keeps the source dir; the
                    // on-disk marker re-seeds the entry after a host
                    // restart.
                    let now_ms = wall_now_ms();
                    self.departing_snap_pending.insert(
                        (l.scope.worker.clone(), l.scope.class_key.clone()),
                        DepartingPending {
                            scope: l.scope.clone(),
                            epoch: l.epoch.0,
                            first_failed_ms: now_ms,
                            next_retry_at: Instant::now()
                                + Duration::from_millis(snap.departing_retry_base_ms),
                            attempts: 0,
                            exhausted: false,
                            is_transfer,
                            dir_missing: false,
                        },
                    );
                    mark_final_pending(&l.scope, l.epoch.0, is_transfer);
                    // P0-2a: HOLD the lease — for EVERY departing
                    // class, not only visible transfers. At drain
                    // time this cell cannot distinguish "moved to
                    // another worker/tenant/cell" from a plain
                    // delete (a cross-tenant transfer is invisible in
                    // our desired map); holding is the safe default:
                    // the record stays a live epoch owned by this
                    // cell until the `Final` baseline lands, so no
                    // acquirer can fence in and serve a stale or
                    // missing snapshot. A deleted class simply holds
                    // a dead lease — bounded retry + TTL-bound lapse.
                    hold.push(l.clone());
                }
            }
            if !staged_empty.as_os_str().is_empty() {
                let _ = std::fs::remove_dir_all(&staged_empty);
            }
        }
        hold
    }

    fn push_draining(&mut self, gen: LiveGen, now: Instant, do_switch: bool) {
        for slot in gen.slots.values() {
            slot.close();
        }
        // DO1-r2 P0-1: NO final snapshot here — closing the slots only
        // stops NEW requests; in-flight handlers and workerd's alarm
        // scheduler can still write until the process actually exits.
        // The departing snapshot moved to `on_exited`'s reap so the
        // ordering is strictly: writer dead → Final commit → release.
        self.draining.push(DrainingGen {
            id: gen.id,
            pid: gen.pid,
            slots: gen.slots,
            entered_at: now,
            deadline: now + self.drain_timeout,
            sigterm_at: None,
            killed: false,
            watcher: gen.watcher,
            work_dir: gen.work_dir,
            had_do_storage: gen.spec.do_storage.is_some(),
            do_switch,
            leases: gen.leases,
            waituntil: gen.spec.waituntil_endpoint.clone(),
            wu_idle_since: None,
            wu_probe: None,
            wu_last: None,
            wu_next_at: now,
        });
    }

    /// MIG-11: the ONLY way a generation enters draining — push +
    /// immediate drain service in one call, so an idle generation is
    /// SIGTERMed in the same actor turn. The serialized DO switch and
    /// install paths used to push and then wait for `drain_notify`
    /// (never fires when in-flight is already 0) or the next monitor
    /// tick before signalling — that latency WAS most of the measured
    /// switch window.
    fn enter_draining(&mut self, gen: LiveGen, now: Instant, do_switch: bool) {
        self.push_draining(gen, now, do_switch);
        self.service_draining(now);
    }
}

// ======================= scale to zero (actor side) =======================
//
// The actor only ORCHESTRATES the fixed lifecycle stages; every
// decision is delegated to the typed mechanisms in `crate::hibernate`
// (`IdlePolicy`, `BlockerChain`, `WakeFlight`, `WakeCoordinator`).
impl Actor {
    fn gen_total_in_flight(gen: &LiveGen) -> usize {
        [&gen.slots, &gen.qconsumer_slots, &gen.wfops_slots, &gen.cron_slots]
            .iter()
            .flat_map(|m| m.values())
            .map(|s| s.in_flight())
            .sum()
    }

    fn gen_slot_maps(gen: &LiveGen) -> [&Arc<BTreeMap<String, Arc<EndpointSlot>>>; 4] {
        [&gen.slots, &gen.qconsumer_slots, &gen.wfops_slots, &gen.cron_slots]
    }

    fn structural_facts(gen: &LiveGen) -> crate::hibernate::StructuralFacts {
        crate::hibernate::StructuralFacts {
            durable_objects: gen.spec.do_storage.is_some(),
            cron: !gen.spec.cron_endpoints.is_empty(),
            queue_consumers: !gen.spec.qconsumer_endpoints.is_empty(),
            workflows: !gen.spec.wfops_endpoints.is_empty(),
        }
    }

    /// Monitor-tick idle evaluation (stage 1 + 2 of the lifecycle).
    fn evaluate_idle(&mut self, now: Instant) {
        use crate::hibernate::{BlockContext, BlockPhase, IdleInputs, IdleVerdict, WaitUntilFact};
        let Some(s2z) = self.s2z.clone() else {
            return;
        };
        if now < self.hib_next_eval {
            return;
        }
        let eligible = self.state == TenantState::Ready
            && self.active.is_some()
            && self.hib.is_none()
            && self.wake_run.is_none()
            && self.pending.is_none()
            && self.preflight.is_none()
            && self.prewarm.is_none()
            && self.draining.is_empty()
            && self.retry_at.is_none()
            && !self.runtime.switch_gate.active()
            && self.do_wait_since.is_none();
        if !eligible {
            self.hib_next_eval = now + s2z.blocked_recheck;
            return;
        }
        let Some(gen) = self.active.as_ref() else {
            return;
        };
        let in_flight = Self::gen_total_in_flight(gen);
        if in_flight > 0 {
            // In-flight work IS activity (approximation: completion
            // time is not tracked, so every evaluation touches).
            self.runtime.touch();
        }
        let inputs = IdleInputs {
            now,
            last_activity: self.runtime.last_activity(),
            ready_since: self.ready_since,
        };
        match s2z.policy.evaluate(&inputs) {
            IdleVerdict::Keep => {
                self.hib_next_eval = now + Duration::from_secs(3600);
            }
            IdleVerdict::DeferUntil(t) => {
                self.hib_next_eval = t.max(now + Duration::from_millis(50));
            }
            IdleVerdict::Hibernate => {
                let ctx = BlockContext {
                    in_flight,
                    structural: Self::structural_facts(gen),
                    wait_until: WaitUntilFact::NotProbed,
                };
                if let Some(reason) = s2z.blockers.first_block(BlockPhase::Admission, &ctx) {
                    self.record_hibernate_blocked(reason);
                    self.hib_next_eval = now
                        + if reason.is_structural() {
                            Duration::from_secs(3600)
                        } else {
                            s2z.blocked_recheck
                        };
                } else {
                    self.begin_hibernate(now);
                }
            }
        }
    }

    fn record_hibernate_blocked(&self, reason: crate::hibernate::BlockReason) {
        self.runtime
            .stats
            .hibernate_blocked
            .fetch_add(1, Ordering::Relaxed);
        crate::metrics::registry().inc(
            "vrt_host_hibernate_blocked_total",
            &[("reason", reason.label())],
            1.0,
        );
    }

    /// Stage 3: close every slot, then wait for quiescence.
    fn begin_hibernate(&mut self, now: Instant) {
        let Some(s2z) = self.s2z.clone() else {
            return;
        };
        // Read BEFORE closing: a request that touches after this read
        // is observed by `service_hibernate` as an activity change.
        let activity_seq = self.runtime.activity_seq();
        let Some(gen) = self.active.as_ref() else {
            return;
        };
        for m in Self::gen_slot_maps(gen) {
            for s in m.values() {
                s.close();
            }
        }
        let wu = if gen.spec.waituntil_endpoint.is_some() {
            HibWu::NotStarted
        } else {
            HibWu::NotNeeded
        };
        self.hib = Some(HibAttempt {
            deadline: now + s2z.drain_timeout,
            activity_seq,
            gen_id: gen.id,
            wu,
        });
        self.transit(Transition::HibernateBegin);
        self.service_hibernate(now);
    }

    /// Drive the in-progress attempt as far as it goes right now.
    fn service_hibernate(&mut self, now: Instant) {
        use crate::hibernate::WaitUntilFact;
        if self.s2z.is_none() || self.state != TenantState::Hibernating {
            self.hib = None;
            return;
        }
        let Some(att) = self.hib.as_ref() else {
            self.abort_hibernate("orphan");
            return;
        };
        let (deadline, seq0, gen_id, wu) = (att.deadline, att.activity_seq, att.gen_id, att.wu);
        let Some(gen) = self.active.as_ref().filter(|g| g.id == gen_id) else {
            self.abort_hibernate("gen_changed");
            return;
        };
        if self.runtime.wake.current().is_some() || self.runtime.activity_seq() != seq0 {
            self.abort_hibernate("traffic");
            return;
        }
        if Self::gen_total_in_flight(gen) > 0 {
            if now >= deadline {
                self.abort_hibernate("drain_timeout");
            }
            return;
        }
        match wu {
            HibWu::NotNeeded => self.try_commit(now, WaitUntilFact::NoAccounting),
            HibWu::NotStarted => {
                let Some(endpoint) = gen.spec.waituntil_endpoint.clone() else {
                    self.try_commit(now, WaitUntilFact::NoAccounting);
                    return;
                };
                self.hib_probe_seq += 1;
                let seq = self.hib_probe_seq;
                if let Some(a) = self.hib.as_mut() {
                    a.wu = HibWu::InFlight { seq };
                }
                self.spawn_hib_probe(gen_id, seq, endpoint);
            }
            HibWu::InFlight { .. } => {
                if now >= deadline {
                    self.abort_hibernate("probe_timeout");
                }
            }
            HibWu::Done(v) => {
                let fact = match v {
                    WuVerdict::Pending(0) => WaitUntilFact::Idle,
                    WuVerdict::Pending(n) => WaitUntilFact::Pending(n),
                    WuVerdict::Failed => WaitUntilFact::Unknown,
                };
                self.try_commit(now, fact);
            }
        }
    }

    /// Stage 4: commit-phase blockers, then release.
    fn try_commit(&mut self, now: Instant, wait_until: crate::hibernate::WaitUntilFact) {
        use crate::hibernate::{BlockContext, BlockPhase};
        let Some(s2z) = self.s2z.clone() else {
            return;
        };
        let Some(gen) = self.active.as_ref() else {
            self.abort_hibernate("gen_changed");
            return;
        };
        let ctx = BlockContext {
            in_flight: Self::gen_total_in_flight(gen),
            structural: Self::structural_facts(gen),
            wait_until,
        };
        if let Some(reason) = s2z.blockers.first_block(BlockPhase::Commit, &ctx) {
            self.record_hibernate_blocked(reason);
            self.abort_hibernate(reason.label());
            return;
        }
        self.commit_hibernate(now);
    }

    fn abort_hibernate(&mut self, reason: &'static str) {
        let now = Instant::now();
        self.hib = None;
        if let Some(gen) = self.active.as_ref() {
            for m in Self::gen_slot_maps(gen) {
                for s in m.values() {
                    s.reopen();
                }
            }
        }
        self.transit(Transition::HibernateAbort);
        self.runtime
            .stats
            .hibernate_aborts
            .fetch_add(1, Ordering::Relaxed);
        crate::metrics::registry().inc(
            "vrt_host_hibernate_total",
            &[("outcome", "aborted")],
            1.0,
        );
        let recheck = self
            .s2z
            .as_ref()
            .map(|s| s.blocked_recheck)
            .unwrap_or(Duration::from_secs(30));
        // Traffic = the tenant is active (the idle policy defers it
        // naturally): re-evaluate now and forget the backoff. Any other
        // abort (commit-phase block, probe/drain timeout) backs off
        // exponentially — each attempt closes admission.
        let next = if reason == "traffic" {
            self.hib_backoff.reset();
            Duration::ZERO
        } else {
            self.hib_backoff.on_blocked_abort(recheck)
        };
        self.hib_next_eval = now + next;
        tracing::info!(
            tenant = %self.tenant_id,
            reason,
            consecutive = self.hib_backoff.consecutive(),
            next_retry_ms = next.as_millis() as u64,
            "hibernation aborted; tenant stays Ready"
        );
    }

    /// Release the generation. Its exit goes through the DRAINING
    /// branch of `on_exited` — an expected exit, never a crash.
    fn commit_hibernate(&mut self, now: Instant) {
        self.hib = None;
        self.hib_backoff.reset();
        let Some(gen) = self.active.take() else {
            return;
        };
        self.runtime.set_serving(false);
        self.runtime.clear_routes();
        self.runtime.switch_gate.end();
        self.runaway = None;
        self.runaway_kill = None;
        self.probe_in_flight = None;
        self.runtime.set_active_pid(0);
        // The next launch reassembles (fresh work dir) instead of
        // reusing the hibernated generation's.
        self.attempted_once = true;
        self.transit(Transition::Hibernated);
        self.runtime
            .stats
            .hibernations
            .fetch_add(1, Ordering::Relaxed);
        self.runtime
            .stats
            .hibernated_at_ms
            .store(wall_now_ms(), Ordering::Relaxed);
        crate::metrics::registry().inc(
            "vrt_host_hibernate_total",
            &[("outcome", "committed")],
            1.0,
        );
        tracing::info!(tenant = %self.tenant_id, gen = gen.id, "tenant hibernated");
        self.enter_draining(gen, now, false);
        // A wake that raced the commit is picked up here.
        if self.runtime.wake.pending().is_some() {
            self.service_wake();
        }
    }

    fn spawn_hib_probe(&mut self, gen: u64, seq: u64, endpoint: WorkerEndpoint) {
        let tx = self.ev_tx.clone();
        let budget = self.probe_timeout;
        tokio::spawn(async move {
            let verdict = match tokio::time::timeout(
                budget,
                tokio::task::spawn_blocking(move || wu_pending_probe(&endpoint, budget)),
            )
            .await
            {
                Ok(Ok(Some(pending))) => WuVerdict::Pending(pending),
                _ => WuVerdict::Failed,
            };
            let _ = tx.try_send(Ev::HibProbeResult { gen, seq, verdict });
        });
    }

    fn on_hib_probe_result(&mut self, gen: u64, seq: u64, verdict: WuVerdict) {
        let ok = matches!(
            &self.hib,
            Some(a) if a.gen_id == gen && matches!(a.wu, HibWu::InFlight { seq: s } if s == seq)
        );
        if !ok {
            return;
        }
        if let Some(a) = self.hib.as_mut() {
            a.wu = HibWu::Done(verdict);
        }
        self.service_hibernate(Instant::now());
    }

    // ---- wake ----

    /// Adopt the pending flight (if any) and drive it.
    fn service_wake(&mut self) {
        use crate::hibernate::{WakeAdmission, WakeError};
        let Some(s2z) = self.s2z.clone() else {
            return;
        };
        if self.wake_run.is_none() {
            let Some(req) = self.runtime.wake.pending() else {
                return;
            };
            self.runtime.wake.claim(req.key.flight);
            self.wake_run = Some(WakeRun {
                flight: req.key.flight,
                source: req.key.source,
                started: req.started,
                deadline: req.started + s2z.wake_deadline,
                attempt: 0,
                permit: None,
            });
            match self.state {
                TenantState::Ready => {
                    self.finish_wake_ok();
                    return;
                }
                TenantState::Hibernating => {
                    self.abort_hibernate("traffic");
                    self.finish_wake_ok();
                    return;
                }
                TenantState::Hibernated | TenantState::Waking => {}
                TenantState::CircuitOpen => {
                    self.finish_wake_err(WakeError::CircuitOpen);
                    return;
                }
                TenantState::Draining | TenantState::Stopped => {
                    self.finish_wake_err(WakeError::Failed("tenant-draining"));
                    return;
                }
                _ => {
                    self.finish_wake_err(WakeError::Failed("not-asleep"));
                    return;
                }
            }
        }
        if self.state != TenantState::Hibernated {
            return;
        }
        // Wait for the hibernated generation to be fully reaped
        // (`on_exited` re-enters here).
        if !self.draining.is_empty() {
            return;
        }
        if self.desired_spec.is_none() {
            self.finish_wake_err(WakeError::Failed("no-spec"));
            return;
        }
        match s2z
            .coordinator
            .request(&self.tenant_id, &self.runtime.wake.notify)
        {
            WakeAdmission::Granted(permit) => {
                if let Some(w) = self.wake_run.as_mut() {
                    w.permit = Some(permit);
                    w.attempt = 1;
                }
                self.transit(Transition::WakeBegin);
                self.start_pending_launch(false);
            }
            WakeAdmission::Queued => {}
            WakeAdmission::Rejected => self.finish_wake_err(WakeError::Capacity),
        }
    }

    /// The launch installed — the wake succeeded.
    fn finish_wake_ok(&mut self) {
        let Some(run) = self.wake_run.take() else {
            return;
        };
        let stats = &self.runtime.stats;
        stats.wakes.fetch_add(1, Ordering::Relaxed);
        let elapsed = run.started.elapsed();
        stats.last_wake_ms.store(
            elapsed.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        stats.hibernated_at_ms.store(0, Ordering::Relaxed);
        let r = crate::metrics::registry();
        r.inc(
            "vrt_host_wake_total",
            &[("source", run.source.label()), ("outcome", "ok")],
            1.0,
        );
        r.observe("vrt_host_wake_seconds", &[], elapsed.as_secs_f64());
        self.runtime.wake.complete(run.flight, Ok(()));
        drop(run.permit);
    }

    /// The wake failed — typed error to every waiter, never fake success.
    fn finish_wake_err(&mut self, err: crate::hibernate::WakeError) {
        let run = self.wake_run.take();
        if let Some(s2z) = self.s2z.as_ref() {
            s2z.coordinator.cancel(&self.tenant_id);
        }
        let (completed, source) = match run {
            Some(run) => {
                let ok = self.runtime.wake.complete(run.flight, Err(err));
                let src = run.source.label();
                drop(run.permit);
                (ok, src)
            }
            None => (
                self.runtime.wake.complete_current(Err(err)).is_some(),
                crate::hibernate::WakeSourceKind::Http.label(),
            ),
        };
        if completed {
            self.runtime
                .stats
                .wake_failures
                .fetch_add(1, Ordering::Relaxed);
            crate::metrics::registry().inc(
                "vrt_host_wake_total",
                &[("source", source), ("outcome", err.label())],
                1.0,
            );
            tracing::warn!(tenant = %self.tenant_id, ?err, "tenant wake failed");
        }
    }

    /// A launch during `Waking` failed: bounded retry, then fail.
    fn on_wake_launch_failed(&mut self, reason: &'static str, now: Instant) {
        use crate::hibernate::WakeError;
        let count = self.window.record(now);
        let tripped = count >= self.fail_threshold;
        self.sweep_gen_dirs();
        let (attempt, deadline, max) = match self.wake_run.as_ref() {
            Some(w) => (w.attempt, w.deadline, w.source.max_attempts()),
            None => (u32::MAX, now, 0),
        };
        if !tripped && attempt < max && now < deadline {
            if let Some(w) = self.wake_run.as_mut() {
                w.attempt += 1;
            }
            self.transit(Transition::RetryBegin);
            self.start_pending_launch(false);
            return;
        }
        if tripped {
            self.cooldown_until = Some(now + self.circuit_cooldown);
            self.runtime
                .stats
                .circuit_opens
                .fetch_add(1, Ordering::Relaxed);
            self.transit(Transition::CircuitTrip);
            self.finish_wake_err(WakeError::CircuitOpen);
        } else {
            self.transit(Transition::LaunchFailed);
            self.finish_wake_err(WakeError::Failed(reason));
        }
    }

    /// The wake deadline passed.
    fn on_wake_deadline(&mut self) {
        self.abort_pending();
        if self.state == TenantState::Waking {
            self.transit(Transition::LaunchFailed);
        }
        self.finish_wake_err(crate::hibernate::WakeError::Timeout);
    }
}

/// DO1-r2: on-disk persistence for `departing_snap_pending` — a
/// `<key>.final-snap-pending` file in the source worker dir records
/// `epoch` (and `transfer` flag) so a restarted host re-seeds the
/// bounded retry AND the sweep keeps the dir until a baseline lands.
pub const FINAL_PENDING_SUFFIX: &str = ".final-snap-pending";

fn final_pending_path(scope: &crate::do_placement::PlacementScope) -> std::path::PathBuf {
    scope
        .class_dir
        .parent()
        .map(|w| w.join(format!("{}{FINAL_PENDING_SUFFIX}", scope.class_key)))
        .unwrap_or_else(|| scope.class_dir.join(format!("{FINAL_PENDING_SUFFIX}")))
}

fn mark_final_pending(scope: &crate::do_placement::PlacementScope, epoch: u64, is_transfer: bool) {
    let body = format!(
        "{epoch} {}",
        if is_transfer { "transfer" } else { "delete" }
    );
    let _ = std::fs::write(final_pending_path(scope), body);
}

fn clear_final_pending(scope: &crate::do_placement::PlacementScope) {
    let _ = std::fs::remove_file(final_pending_path(scope));
}

/// Wait for a child exit and report it — one task per installed
/// generation. `kill_on_drop` on the child means aborting this watcher
/// still SIGKILLs and reaps via the orphan collector.
fn spawn_exit_watcher(
    id: u64,
    mut child: crate::launch::GroupChild,
    ev_tx: mpsc::Sender<Ev>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _ = child.inner.wait().await;
        let _ = ev_tx.send(Ev::Exited { id }).await;
    })
}

/// One launch attempt: spawn, wait ready, return the pending generation
/// or terminate the child and report the failure.
/// W1-r2/P0-2: bounded `workerd test <config>` load check — the
/// verdict lives in [`vrt_config::check`] (ONE classifier shared with
/// vrt-local's deploy-time check); this shell supplies the spec's
/// argv shape and secret needles.
/// Non-cancellable wrapper kept for unit tests — the launch path
/// uses `preflight_workerd_cancellable` (MIG-9).
#[allow(dead_code)]
fn preflight_workerd<I: Iterator<Item = String>>(
    binary: &Path,
    config: &Path,
    external_addrs: &[vrt_config::ExternalAddr],
    extra_args: &[String],
    needles: I,
    timeout: Duration,
) -> Result<(), vrt_config::check::CheckFailure> {
    vrt_config::check::check_workerd_loads_bounded(
        binary,
        config,
        external_addrs,
        extra_args,
        &needles.collect::<Vec<_>>(),
        timeout,
    )
}

/// MIG-9: cancellable variant used by [`Actor::spawn_preflight`]'s
/// spawn_blocking task — identical check, plus the actor's cancel
/// flag. When it flips (stop/supersede), the check group-kills and
/// reaps the child inside one wait tick; `CheckKind::Cancelled` is
/// never a config verdict.
#[allow(clippy::too_many_arguments)]
fn preflight_workerd_cancellable<I: Iterator<Item = String>>(
    binary: &Path,
    config: &Path,
    external_addrs: &[vrt_config::ExternalAddr],
    extra_args: &[String],
    needles: I,
    timeout: Duration,
    cancel: Arc<AtomicBool>,
) -> Result<(), vrt_config::check::CheckFailure> {
    vrt_config::check::check_workerd_loads_cancellable(
        binary,
        config,
        external_addrs,
        extra_args,
        &needles.collect::<Vec<_>>(),
        timeout,
        cancel,
    )
}

async fn launch_one(
    _seq: u64,
    spec: Box<TenantLaunchSpec>,
    launcher: Arc<dyn WorkerdLauncher>,
    mut probe: Box<dyn ControlEventParser>,
    ready_timeout: Duration,
    probe_timeout: Duration,
    kill_grace: Duration,
) -> Result<PendingGen, SpawnFailure> {
    // T34f2/R1: NO orphan reaping here — at launch time a live
    // previous generation has the exact orphan shape (pidfile +
    // alive + cmdline match) and MUST keep serving through a
    // switch. Reaping runs once at vrt-host startup instead, when
    // this process provably owns no children.
    // T25/L15: drop stale sockets left by a crashed predecessor
    // BEFORE spawning — a socket file at our bind path makes the new
    // workerd fail with EADDRINUSE. Only socket files inside this
    // generation's own sock dir are touched; symlinks are never
    // followed and foreign file types are left alone.
    clean_stale_sockets(&spec.work_dir.join("sock"));
    let mut live = launcher
        .launch(&spec)
        .await
        .map_err(|e| SpawnFailure::launch(&e))?;
    let endpoints = match launch_until_ready(
        &mut live,
        &spec,
        &mut *probe,
        ready_timeout,
        probe_timeout,
        kill_grace,
    )
    .await
    {
        Ok(endpoints) => endpoints,
        Err(error) => {
            // T25/L16: surface the sanitized stderr tail — the only
            // place a real workerd's "why" is recorded.
            let detail = live
                .log_stats
                .stderr_tail
                .as_ref()
                .map(|t| t.tail_lines(crate::launch::LOG_TAIL_LINES));
            return Err(SpawnFailure::ready(&error).with_detail(detail));
        }
    };
    Ok(PendingGen {
        spec,
        pid: live.pid,
        child: crate::launch::GroupChild::wrap(live.child, live.pid),
        control: live.control,
        endpoints: endpoints.ingress,
        qconsumer_endpoints: endpoints.qconsumer,
        wfops_endpoints: endpoints.wfops,
        cron_endpoints: endpoints.cron,
        leases: Vec::new(),
    })
}

/// MIG-13: finish a prewarmed generation — rewrite the watched live
/// config to the final bytes, wait for the `vrt-switch` marker
/// listen event (the reload's applied signal), then re-probe every
/// endpoint before handing the generation to install. Fail-closed:
/// a reload that never applies (crash, control EOF, deadline) is a
/// launch failure — the gate releases held requests into
/// `NoServing`, the retry path respawns, and the warm process dies
/// on its own kill guard.
async fn swap_finalize(
    warm: WarmGen,
    live_config: PathBuf,
    final_config: PathBuf,
    early_slots: Vec<Arc<EndpointSlot>>,
    ready_timeout: Duration,
    probe_timeout: Duration,
    kill_grace: Duration,
    wu_grace: Duration,
    isolation: Option<crate::spec::TenantIsolation>,
    tenant: String,
) -> Result<PendingGen, SpawnFailure> {
    use tokio::io::AsyncReadExt;
    let WarmGen {
        mut gen,
        swap_needed,
        ..
    } = warm;
    if !swap_needed {
        // No DO storage in the new spec — the warm process already
        // runs the final config. H2-r2: it may have died while parked
        // (no exit watcher covers the parked window — the actor's
        // tick poll is the backstop, not the proof), so install only
        // after every endpoint re-probes; a dead warm process fails
        // closed exactly like a fresh spawn that never readied.
        for map in [
            &gen.endpoints,
            &gen.qconsumer_endpoints,
            &gen.wfops_endpoints,
            &gen.cron_endpoints,
        ] {
            if let Err(e) = crate::ready::probe_all(map, probe_timeout).await {
                let _ = terminate_child(&mut gen.child.inner, gen.pid, kill_grace).await;
                return Err(SpawnFailure::ready(&e));
            }
        }
        return Ok(gen);
    }
    let t_swap = Instant::now();
    // Bounded settle for early-flipped in-flight requests — the
    // reload tears workerd service instances down, so a request that
    // started on the warm generation would be aborted. Already off
    // the route maps → only drains, never grows; the cap reuses the
    // do-switch waitUntil bound so the whole reload window stays
    // inside the ingress hold budget.
    let settle_cap = wu_grace.min(Duration::from_millis(300));
    let settle_end = Instant::now() + settle_cap;
    while early_slots
        .iter()
        .any(|s| s.in_flight.load(Ordering::Relaxed) > 0)
        && Instant::now() < settle_end
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // M2-r2: bounded waitUntil drain on the warm generation — the
    // reload rebuilds service instances, so its pending
    // `ctx.waitUntil` work is truncated exactly like the old
    // generation's at a serialized switch. Wait, bounded by the SAME
    // do-switch grace; whatever is still pending at the cap (or a
    // probe that never answered) is dropped and COUNTED, never
    // silently. Declared limitation: streaming/WebSocket sessions on
    // early-flipped workers are cut at this reload — no re-handshake.
    if let Some(endpoint) = gen.spec.waituntil_endpoint.clone() {
        let mut cut = !wu_grace.is_zero();
        if cut {
            let deadline = Instant::now() + wu_grace;
            let mut last: Option<u64>;
            loop {
                last = tokio::task::spawn_blocking({
                    let ep = endpoint.clone();
                    let t = probe_timeout;
                    move || wu_pending_probe(&ep, t)
                })
                .await
                .ok()
                .flatten();
                match last {
                    Some(0) => {
                        cut = false;
                        break;
                    }
                    Some(_) => {
                        if Instant::now() >= deadline {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    // Probe failed — the count is unknown; the marker
                    // wait below decides the process's real fate.
                    None => break,
                }
            }
        }
        if cut {
            crate::metrics::registry().inc(
                "vrt_host_waituntil_dropped_total",
                &[("reason", "swap_reload")],
                1.0,
            );
            tracing::warn!(
                tenant = %tenant,
                "waitUntil work dropped: prewarm reload cut pending or unknown waitUntil"
            );
        }
    }
    let final_text = match std::fs::read_to_string(&final_config) {
        Ok(t) => t,
        Err(e) => {
            // L1-r2: EVERY swap early-return terminates the warm
            // child — no orphan workerd survives a failed swap.
            let _ = terminate_child(&mut gen.child.inner, gen.pid, kill_grace).await;
            return Err(SpawnFailure {
                reason: "swap-io",
                detail: Some(format!("{e}")),
            });
        }
    };
    // MIG-13-r5: `workerd --watch` reloads by execve (see
    // `clear_declared_sockets`) — the exec'd image binds every unix
    // socket the final config declares while the twin's socket FILES
    // still exist → EADDRINUSE → kj fatal → the marker never comes
    // (e2e24). Clear exactly the declared paths inside this
    // generation's own sock dir BEFORE the watched write; a refusal
    // fails the swap closed — never unlink anything unproven.
    let sock_dir = gen.spec.work_dir.join("sock");
    if let Err(e) = clear_declared_sockets(&final_text, &sock_dir) {
        let _ = terminate_child(&mut gen.child.inner, gen.pid, kill_grace).await;
        return Err(SpawnFailure {
            reason: "swap-preclear",
            detail: Some(format!("{e}")),
        });
    }
    // H1-r2: the secret-bearing live config lands via the shared
    // atomic writer — tmp-in-same-dir + rename (0600, dir fsync), the
    // same materialization the assembled configs get — then re-chown
    // to the tenant uid so the isolated child can re-read it.
    if let Err(e) = vrt_securefs::atomic_write(&live_config, final_text.as_bytes(), 0o600)
        .and_then(|()| {
            crate::assemble::chown_isolated(&live_config, isolation)
                .map_err(|e2| match e2 {
                    crate::assemble::AssembleError::Io(io) => io,
                    other => std::io::Error::new(std::io::ErrorKind::Other, format!("{other}")),
                })
        }) {
        let _ = terminate_child(&mut gen.child.inner, gen.pid, kill_grace).await;
        return Err(SpawnFailure {
            reason: "swap-io",
            detail: Some(format!("{e}")),
        });
    }
    // Marker wait — workerd re-emits every socket's listen event on
    // reload, but ONLY the final config carries `vrt-switch`, so the
    // marker can never arrive from a same-content reload of the
    // twin. Raw substring scan on the control stream — immune to a
    // partial line the warm spawn's parser may have buffered.
    const NEEDLE: &[u8] = b"\"socket\":\"vrt-switch\"";
    let deadline = Instant::now() + ready_timeout;
    let mut buf = [0u8; 4096];
    let mut carry: Vec<u8> = Vec::new();
    let mut total = 0usize;
    let mut child_died = false;
    let marker_seen = loop {
        if Instant::now() >= deadline {
            break false;
        }
        match gen.child.inner.try_wait() {
            // MIG-13-r5: a dead warm child can never emit the marker
            // — the execve'd image fatals fast on any bind/load error
            // (e.g. an EADDRINUSE the pre-clear refused to fix), so
            // stop NOW with a distinct reason instead of draining the
            // whole ready_timeout on a corpse (e2e24 burned ~9.6 s
            // and 20+ 503s per worker on exactly this).
            Ok(Some(_)) | Err(_) => {
                child_died = true;
                break false;
            }
            Ok(None) => {}
        }
        let remain = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(
            remain.min(Duration::from_millis(100)),
            gen.control.read(&mut buf),
        )
        .await
        {
            Ok(Ok(0)) => break false,
            Ok(Ok(n)) => {
                total += n;
                if total > 256 * 1024 {
                    break false;
                }
                carry.extend_from_slice(&buf[..n]);
                if find_subslice(&carry, NEEDLE) {
                    break true;
                }
                if carry.len() > 8192 {
                    carry = carry.split_off(carry.len() - 4096);
                }
            }
            Ok(Err(_)) => break false,
            Err(_) => {}
        }
    };
    if !marker_seen {
        // The EOF/oversized/timeout breaks can still mean a dead
        // child — the exit may have landed between the last poll and
        // the read that broke the loop. One final poll BEFORE
        // terminate keeps the reason honest.
        if !child_died {
            child_died = gen
                .child
                .inner
                .try_wait()
                .map(|s| s.is_some())
                .unwrap_or(true);
        }
        let _ = terminate_child(&mut gen.child.inner, gen.pid, kill_grace).await;
        if child_died {
            tracing::warn!(
                tenant = %tenant,
                pid = gen.pid,
                "warm generation exited during swap reload; failing fast"
            );
            return Err(SpawnFailure {
                reason: "swap-child-exit",
                detail: None,
            });
        }
        return Err(SpawnFailure::ready(&ReadyError::Timeout));
    }
    crate::metrics::registry().observe(
        "vrt_host_do_switch_stage_seconds",
        &[("tenant", &tenant), ("stage", "swap")],
        t_swap.elapsed().as_secs_f64(),
    );
    // Re-probe every endpoint — a reload rebuilds listeners whose
    // config changed; a torn socket must fail closed here, never at
    // the first held request.
    for map in [
        &gen.endpoints,
        &gen.qconsumer_endpoints,
        &gen.wfops_endpoints,
        &gen.cron_endpoints,
    ] {
        if let Err(e) = crate::ready::probe_all(map, probe_timeout).await {
            let _ = terminate_child(&mut gen.child.inner, gen.pid, kill_grace).await;
            return Err(SpawnFailure::ready(&e));
        }
    }
    Ok(gen)
}

/// MIG-13-r5: unlink every unix socket path the final config will
/// re-bind at reload — BEFORE the watched live-config rewrite.
///
/// `workerd serve --watch` reloads by `execve()` of the same
/// binary+argv (workerd.c++ `CliMain::reloadFromConfigChange`,
/// v1.20260916.1): the pid survives but the process image does not —
/// the exec'd image re-binds every `unix:` socket the config
/// declares, and kj's `SocketAddress::bind` never unlinks a stale
/// pathname first (verified: `kj/async-io-unix.c++` has no unlink;
/// the EADDRINUSE at line 941 is the e2e24 crash site). The twin
/// config bound exactly these paths, so every re-bind collides with
/// its own stale file.
///
/// Scope is fail-closed, two proofs per path:
/// 1. the path must appear as a `unix:` bind inside the final
///    config's own `sockets` block (`unix_socket_bind_paths` — never
///    a disk path, external address, or prose `unix:` literal), AND
/// 2. its lexical parent must be exactly `sock_dir` — this
///    generation's own `sock/`. Anything else (relative path, `..`
///    escape, another generation's dir, an absolute path elsewhere)
///    aborts the swap instead of unlinking an unproven file. Lexical
///    equality is deliberate: no canonicalize, so a symlinked sock
///    dir or a missing entry can never widen the scope.
/// `remove_stale_socket` then re-checks the inode type WITHOUT
/// following links — a planted regular file or any symlink errors
/// out and survives (a dangling link would still block the bind),
/// and `NotFound` (the `vrt-sw.sock` marker the twin never bound)
/// is fine.
///
/// Isolation: the pass runs with host credentials — root under
/// `[isolation]` (the `sock/` dir itself is always host-created
/// 0700) or the shared tenant uid otherwise; unlink needs only the
/// parent dir's write bit, never the socket file's ownership, so no
/// `chown_isolated` is needed here.
///
/// A partial failure still fails closed: earlier paths are unlinked,
/// the warm child is terminated by the caller, the gen dir is
/// retired with its stale sockets, and the burned-prewarm retry
/// assembles a FRESH `gen-<n>` — no shared state leaks forward.
pub fn clear_declared_sockets(
    final_text: &str,
    sock_dir: &std::path::Path,
) -> std::io::Result<()> {
    for path in vrt_config::check::unix_socket_bind_paths(final_text) {
        if path.parent() != Some(sock_dir) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "declared socket {} outside generation sock dir {}",
                    path.display(),
                    sock_dir.display()
                ),
            ));
        }
        vrt_securefs::remove_stale_socket(&path)?;
    }
    Ok(())
}

/// MIG-13: workers that can reach DO storage through the binding
/// graph — DO/workflow declarers and their storage hosts, DO-binding
/// callers, and every worker that service-binds (transitively) into
/// that set. A worker outside the set has no in-process path to an
/// actor namespace, so routing it to the warm generation can never
/// touch scratch storage.
fn do_tainted_workers(
    tenant: &snapshot_protocol::compute_desired::TenantDesired,
) -> std::collections::BTreeSet<String> {
    use snapshot_protocol::compute_desired::BindingDesired;
    let mut tainted: std::collections::BTreeSet<String> = Default::default();
    let mut callers: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
    for w in &tenant.workers {
        if !w.durable_object_classes.is_empty() || !w.workflows.is_empty() {
            tainted.insert(w.worker_id.clone());
        }
        for wf in &w.workflows {
            if let Some(t) = &wf.service_worker_id {
                tainted.insert(t.clone());
            }
        }
        for b in &w.bindings {
            match b {
                BindingDesired::DurableObject(d) => {
                    tainted.insert(w.worker_id.clone());
                    if let Some(t) = &d.service_worker_id {
                        tainted.insert(t.clone());
                    }
                }
                BindingDesired::Service(svc) => {
                    callers
                        .entry(svc.worker_id.as_str())
                        .or_default()
                        .push(w.worker_id.as_str());
                }
                _ => {}
            }
        }
    }
    // Reverse-propagate: a worker service-binding a tainted worker
    // reaches its namespace in-process — transitively tainted.
    let mut stack: Vec<String> = tainted.iter().cloned().collect();
    while let Some(t) = stack.pop() {
        if let Some(cs) = callers.get(t.as_str()) {
            for c in cs {
                if tainted.insert((*c).to_string()) {
                    stack.push((*c).to_string());
                }
            }
        }
    }
    tainted
}

/// Byte-slice `contains` — the control-stream marker scan.
fn find_subslice(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && hay.len() >= needle.len()
        && hay.windows(needle.len()).any(|w| w == needle)
}

/// T25/L17: from the unreferenced gen dirs under a tenant's seg dir,
/// return the ones safe to delete — everything except the NEWEST
/// (kept for post-mortem diagnostics). Sort key: mtime desc, then
/// name desc so a same-mtime pair still keeps the highest gen number.
fn select_sweep_dirs(mut unreferenced: Vec<PathBuf>) -> Vec<PathBuf> {
    unreferenced.sort_by(|a, b| {
        let key = |p: &PathBuf| {
            std::fs::metadata(p)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH)
        };
        key(b).cmp(&key(a)).then_with(|| b.cmp(a))
    });
    unreferenced.into_iter().skip(1).collect()
}

/// T34f2/R1: startup-only orphan sweep — call ONCE at vrt-host boot
/// (`vrt-host` bin, before any tenant launch), never mid-process.
/// Rationale: a live previous generation has the exact orphan shape
/// (pidfile + alive + cmdline match) and must keep serving through
/// blue/green switches — at startup this process provably owns no
/// children, so only the dead-owner rule below is needed to keep
/// foreign instances safe.
///
/// Per `<work>/<tenant>/gen-*/workerd.pid` (`"<w_pid> <h_pid>"`):
/// `h_pid` alive → a LIVE host instance owns that workerd — never
/// touch it (covers shared roots AND would have spared gen-1 in the
/// R1 bug even if the sweep ran mid-process). `h_pid` dead → the
/// workerd's parent is gone → orphan: verify the live cmdline
/// contains the gen dir (recycled pids are never killed), then
/// SIGKILL the whole group. Unverifiable entries are left alone —
/// an orphan is safer than a wrong kill.
pub fn reap_orphaned_children(work_root: &Path) {
    let Ok(entries) = std::fs::read_dir(work_root) else {
        return;
    };
    for e in entries.flatten() {
        let seg = e.path();
        if !seg.is_dir() {
            continue;
        }
        reap_seg_dir(&seg);
    }
}

fn reap_seg_dir(seg_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(seg_dir) else {
        return;
    };
    for e in entries.flatten() {
        let dir = e.path();
        let is_gen = dir
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("gen-"));
        if !is_gen {
            continue;
        }
        let pidfile = dir.join("workerd.pid");
        let Ok(raw) = std::fs::read_to_string(&pidfile) else {
            continue;
        };
        match classify_pidfile(&raw, std::process::id(), crate::launch::pid_alive) {
            ReapVerdict::Clean => {
                let _ = std::fs::remove_file(&pidfile);
            }
            ReapVerdict::Spare => {}
            ReapVerdict::Verify(worker_pid) => {
                if !cmdline_contains(worker_pid, &dir) {
                    continue;
                }
                tracing::warn!(
                    pid = worker_pid,
                    gen_dir = %dir.display(),
                    "reaping orphaned workerd from dead predecessor"
                );
                crate::launch::signal_process_group(worker_pid, libc::SIGKILL);
                let _ = std::fs::remove_file(&pidfile);
            }
        }
    }
}

/// What the startup sweep may do with a `workerd.pid` file.
#[derive(Debug, PartialEq, Eq)]
enum ReapVerdict {
    /// pidfile is stale garbage or its worker is already dead — delete.
    Clean,
    /// Leave it: unparsable, or the owning host pid is still alive
    /// (a live instance's generations are NEVER orphans).
    Spare,
    /// Owner host is dead but the worker lives — verify the cmdline
    /// against the gen dir before killing.
    Verify(u32),
}

/// Pure pidfile verdict — no ps/proc in the decision itself, so the
/// ownership rule is unit-testable end-to-end (R1: the ONLY killable
/// entry is dead-owner + live-worker).
fn classify_pidfile(raw: &str, self_pid: u32, alive: impl Fn(u32) -> bool) -> ReapVerdict {
    let mut it = raw.split_whitespace();
    let (Some(w), Some(h)) = (it.next(), it.next()) else {
        return ReapVerdict::Spare; // old/garbled format — never guess
    };
    if it.next().is_some() {
        return ReapVerdict::Spare; // extra fields — never guess
    }
    let (Ok(worker), Ok(host)) = (w.parse::<u32>(), h.parse::<u32>()) else {
        return ReapVerdict::Spare;
    };
    if host == self_pid || alive(host) {
        return ReapVerdict::Spare; // ours or a live instance's — not an orphan
    }
    if !alive(worker) {
        return ReapVerdict::Clean;
    }
    ReapVerdict::Verify(worker)
}

/// True when process `pid`'s cmdline references `dir` — the pidfile
/// ownership check. Linux reads `/proc/<pid>/cmdline`; elsewhere
/// `ps -o command=` (macOS has no /proc). Any lookup failure returns
/// false so the reaper never kills an unverified pid.
fn cmdline_contains(pid: u32, dir: &Path) -> bool {
    let Some(needle) = dir.as_os_str().to_str() else {
        return false;
    };
    if needle.is_empty() {
        return false;
    }
    #[cfg(target_os = "linux")]
    if let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) {
        return raw
            .split(|&b| b == 0)
            .any(|a| a == needle.as_bytes() || a.starts_with(needle.as_bytes()));
    }
    let Ok(out) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
    else {
        return false;
    };
    out.status.success() && String::from_utf8_lossy(&out.stdout).contains(needle)
}

/// T25/L15: remove stale unix socket FILES directly under `dir`.
/// Regular files/dirs/symlinks are untouched, nothing recurses —
/// a crashed predecessor only leaves socket inodes here.
fn clean_stale_sockets(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return; // missing/unreadable — spawn decides
    };
    use std::os::unix::fs::FileTypeExt;
    for e in entries.flatten() {
        // `symlink_metadata`-equivalent: DirEntry::metadata does NOT
        // follow symlinks — a symlinked socket is left alone.
        let Ok(meta) = e.metadata() else { continue };
        if meta.file_type().is_socket() {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

// ---- registry & convergence ----

/// Shared tenant table read by the ingress: `tenantId -> runtime`.
/// P3c-2-r1: trusted snapshot of a tenant's desired DO storage —
/// `do_storage.tenant_dir` + `workerId → declared uniqueKeys`,
/// published per Apply. Admin endpoints resolve `class_dir` ONLY
/// through this view so request fields can never construct paths.
#[derive(Debug, Clone)]
pub struct DoStorageView {
    pub tenant_dir: PathBuf,
    /// `workerId → uniqueKey[]` as declared by desired.
    pub worker_classes: std::collections::BTreeMap<String, Vec<String>>,
}

impl DoStorageView {
    /// Trusted resolution: `(worker_id, unique_key)` must be declared
    /// in desired — else `None` (admin maps it to 404). The returned
    /// dir is always `tenant_dir/worker_id/unique_key` — the exact
    /// layout `prepare_do_storage` creates (assemble.rs).
    pub fn class_dir(&self, worker_id: &str, unique_key: &str) -> Option<PathBuf> {
        let keys = self.worker_classes.get(worker_id)?;
        if !keys.iter().any(|k| k == unique_key) {
            return None;
        }
        Some(self.tenant_dir.join(worker_id).join(unique_key))
    }
}

#[derive(Debug, Default)]
pub struct TenantRegistry {
    tenants: std::sync::RwLock<std::collections::HashMap<String, Arc<TenantRuntime>>>,
}

impl TenantRegistry {
    /// Shared empty registry.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Lookup a tenant's runtime.
    pub fn get(&self, tenant_id: &str) -> Option<Arc<TenantRuntime>> {
        self.tenants
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(tenant_id)
            .cloned()
    }

    /// Tenant ids present (diagnostics).
    pub fn tenant_ids(&self) -> Vec<String> {
        self.tenants
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// Return the tenant's runtime, creating a `Starting` one when
    /// absent — converger needs it before launch to publish the
    /// expected fingerprint (T15/R2).
    pub fn get_or_create(&self, tenant_id: &str) -> Arc<TenantRuntime> {
        if let Some(rt) = self.get(tenant_id) {
            return rt;
        }
        let mut map = self.tenants.write().unwrap_or_else(|e| e.into_inner());
        map.entry(tenant_id.to_string())
            .or_insert_with(|| TenantRuntime::new(tenant_id))
            .clone()
    }

    /// Register a runtime (tests + `SupervisorHost`).
    #[doc(hidden)]
    pub fn insert(&self, tenant_id: String, runtime: Arc<TenantRuntime>) {
        self.tenants
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tenant_id, runtime);
    }

    fn remove(&self, tenant_id: &str) {
        self.tenants
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(tenant_id);
    }
}

/// Per-tenant assembly abstraction — turns a desired tenant fragment into
/// a launch spec (the concrete implementation is [`crate::assemble`]).
pub trait TenantAssembler: Send + Sync {
    /// H5-r2: refresh the per-desired-set uid collision verdict.
    /// Called once per converge BEFORE any tenant assembly — default
    /// no-op so non-isolating assemblers need no state.
    fn sync_desired_tenants(&self, _tenant_ids: &[String]) {}

    /// Render + materialize one generation for `tenant`.
    fn assemble(
        &self,
        tenant: &snapshot_protocol::compute_desired::TenantDesired,
        revision: u64,
        generation: u64,
    ) -> Result<TenantLaunchSpec, crate::assemble::AssembleError>;

    /// Fingerprint of generation-independent render params (vars/secrets
    /// fingerprints, system refs) — the converge dedup key includes it so
    /// a params-only change still re-assembles tenants.
    /// T17/F1: may fail when render inputs are unloadable — converge
    /// treats that as `render-inputs-unavailable` for every tenant.
    fn params_fingerprint(&self) -> Result<String, crate::assemble::AssembleError>;

    /// RV-R53.5: bounded asset-store mark-and-sweep. `keep` maps
    /// tenant → manifest digests still referenced by desired state.
    /// Default no-op (None) for assemblers without an asset store.
    fn sweep_assets(
        &self,
        _keep: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    ) -> Option<vrt_provider::assets::AssetGcReport> {
        None
    }
}

/// Owns the supervisor set and applies `compute.desired` convergence:
/// tenants in the snapshot get a spec assembled + applied; removed
/// tenants are drained and reaped.
pub struct SupervisorHost {
    deps: SupervisorDeps,
    params: SupervisorParams,
    registry: Arc<TenantRegistry>,
    supervisors: std::collections::HashMap<String, SupervisorHandle>,
    applied: std::collections::HashMap<
        String,
        (snapshot_protocol::compute_desired::TenantDesired, String),
    >,
}

/// Outcome of one converge pass.
#[derive(Debug, Default)]
pub struct ConvergeOutcome {
    /// Tenants whose spec was (re)applied.
    pub applied: Vec<String>,
    /// Tenants skipped because their desired fragment did not change.
    pub unchanged: Vec<String>,
    /// Tenants whose assembly failed (old generation keeps serving).
    pub failed: Vec<String>,
    /// Tenants drained because they left the snapshot.
    pub removed: Vec<String>,
    /// RV-R53.4: object-store purge-ledger entries still pending after
    /// this converge's bounded drain — surfaced to status/metrics.
    pub purges_pending: usize,
}

impl SupervisorHost {
    /// New host over an empty registry.
    pub fn new(
        registry: Arc<TenantRegistry>,
        deps: SupervisorDeps,
        params: SupervisorParams,
    ) -> Self {
        Self {
            deps,
            params,
            registry,
            supervisors: std::collections::HashMap::new(),
            applied: std::collections::HashMap::new(),
        }
    }

    /// Apply a verified `compute.desired`: assemble changed tenants,
    /// drain removed ones. Per-tenant assembly failures are isolated —
    /// a bad tenant never blocks or kills another tenant's service.
    pub fn converge(
        &mut self,
        desired: &snapshot_protocol::compute_desired::ComputeDesired,
        assembler: &Arc<dyn TenantAssembler>,
    ) -> ConvergeOutcome {
        let mut outcome = ConvergeOutcome::default();
        let mut seen = std::collections::HashSet::new();
        // RV-R53: signed deletion intents. Absence from `tenants` is
        // NEVER deletion — only these tombstones authorize destroying
        // durable data. `tomb_tenants` covers whole-tenant purges;
        // worker/class tombstones are routed to live actors or the
        // host-level purge below.
        let tomb_tenants: std::collections::HashSet<&str> = desired
            .deletions
            .iter()
            .filter(|d| d.worker_id.is_none())
            .map(|d| d.tenant_id.as_str())
            .collect();
        // Wire the shared assembler so actor retry-reassembly uses the
        // SAME assembler as converge (T13/H2) — set once, never swapped.
        if self.deps.assembler.is_none() {
            self.deps.assembler = Some(assembler.clone());
        }
        // T17/F1: a failed inputs load must NEVER fall back to the
        // previous fingerprint — that would keep `serving == expected`
        // while secrets are broken (a second "received ≠ loaded" lie).
        // Instead every tenant's expected spec is bound to a sentinel
        // it can never match, with the typed reason surfaced.
        // H5-r2: uid-collision verdict is a SET property — recomputed
        // every converge so a deleted tenant frees its uid and the
        // refusal set matches the provider's exactly.
        assembler.sync_desired_tenants(
            &desired
                .tenants
                .iter()
                .map(|t| t.tenant_id.clone())
                .collect::<Vec<_>>(),
        );
        let inputs_ok = assembler.params_fingerprint();
        let params_fp = match &inputs_ok {
            Ok(fp) => fp.clone(),
            Err(_) => "render-inputs-unavailable".to_string(),
        };
        for tenant in &desired.tenants {
            seen.insert(tenant.tenant_id.clone());
            // T15/R2: the expected fingerprint derives from the adopted
            // desired itself — computable WITHOUT a successful
            // assemble, so a failed build still leaves
            // `expected != serving` and readyz never lies.
            let runtime = self.registry.get_or_create(&tenant.tenant_id);
            runtime.set_expected_digest(crate::assemble::spec_fingerprint(tenant, &params_fp));
            if inputs_ok.is_err() {
                // Fresh vars/secrets could not be loaded — no tenant
                // may assemble against unknown inputs. Old generations
                // keep serving; readyz reports the typed reason.
                runtime.set_assemble_error(Some("render-inputs-unavailable".to_string()));
                outcome.failed.push(tenant.tenant_id.clone());
                continue;
            }
            // T25/L9: a tenant with ZERO workers is an explicit empty
            // state — the provider still binds its listener (bindings
            // only), host launches nothing. Not an assemble error.
            if tenant.workers.is_empty() {
                if let Some(handle) = self.supervisors.remove(&tenant.tenant_id) {
                    // RV-R53: an empty worker list is "not serving",
                    // NOT "deleted" — only a signed tenant tombstone
                    // authorizes the destructive Undeploy lifecycle;
                    // otherwise Stop (drain, retain all DO data).
                    if tomb_tenants.contains(tenant.tenant_id.as_str()) {
                        let _ = handle.try_send(SupervisorCmd::Undeploy);
                    } else {
                        let _ = handle.try_send(SupervisorCmd::Stop);
                    }
                }
                runtime.set_assemble_error(None);
                self.applied.insert(
                    tenant.tenant_id.clone(),
                    (tenant.clone(), params_fp.clone()),
                );
                outcome.applied.push(tenant.tenant_id.clone());
                continue;
            }
            if self
                .applied
                .get(&tenant.tenant_id)
                .is_some_and(|(old, fp)| old == tenant && *fp == params_fp)
                && self.supervisors.contains_key(&tenant.tenant_id)
            {
                outcome.unchanged.push(tenant.tenant_id.clone());
                continue;
            }
            let gen = self.next_gen(&tenant.tenant_id);
            match assembler.assemble(tenant, desired.revision, gen) {
                Ok(spec) => {
                    let mut spec = spec;
                    // T13/H2: the actor re-assembles retries from this —
                    // a fresh generation dir per attempt.
                    spec.relaunch = Some(crate::spec::RelaunchCtx {
                        tenant: tenant.clone(),
                        revision: desired.revision,
                    });
                    let enqueued = match self.supervisors.get(&tenant.tenant_id) {
                        Some(handle) => {
                            match handle.try_send(SupervisorCmd::Apply(Box::new(spec))) {
                                Ok(()) => true,
                                Err(e) => {
                                    // T13/M4: never reached the actor — do
                                    // NOT mark applied; next converge retries.
                                    tracing::warn!(
                                        tenant = %tenant.tenant_id,
                                        %e,
                                        "Apply not delivered (channel full or actor dead); will retry next converge"
                                    );
                                    outcome.failed.push(tenant.tenant_id.clone());
                                    false
                                }
                            }
                        }
                        None => {
                            let runtime = self.registry.get_or_create(&tenant.tenant_id);
                            let handle = spawn_supervisor(
                                runtime,
                                spec,
                                SupervisorDeps {
                                    launcher: self.deps.launcher.clone(),
                                    probe_factory: self.deps.probe_factory.clone(),
                                    sampler: self.deps.sampler.clone(),
                                    jitter: self.deps.jitter.clone(),
                                    assembler: self.deps.assembler.clone(),
                                    gen_counter: self.deps.gen_counter.clone(),
                                    placement: self.deps.placement.clone(),
                                    do_snap: self.deps.do_snap.clone(),
                                    wf_tomb_dir: self.deps.wf_tomb_dir.clone(),
                                    cache: self.deps.cache.clone(),
                                    heartbeat: self.deps.heartbeat.clone(),
                                    do_storage_root: self.deps.do_storage_root.clone(),
                                    purge: self.deps.purge.clone(),
                                    prewarm_admission: self.deps.prewarm_admission.clone(),
                                },
                                self.params.clone(),
                            );
                            self.supervisors.insert(tenant.tenant_id.clone(), handle);
                            true
                        }
                    };
                    if enqueued {
                        runtime.set_assemble_error(None);
                        self.applied.insert(
                            tenant.tenant_id.clone(),
                            (tenant.clone(), params_fp.clone()),
                        );
                        outcome.applied.push(tenant.tenant_id.clone());
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        tenant = %tenant.tenant_id,
                        %error,
                        "tenant assembly failed; previous generation keeps serving"
                    );
                    // T15: typed readyz reason — bundle fetches report
                    // `bundle-unavailable`, everything else stays
                    // `workers-not-ready` (whitelist-stable).
                    let reason = match &error {
                        crate::assemble::AssembleError::Artifact(_) => "bundle-unavailable",
                        crate::assemble::AssembleError::Isolation(_) => "isolation-uid-collision",
                        crate::assemble::AssembleError::Inputs(_) => "render-inputs-unavailable",
                        crate::assemble::AssembleError::Render(
                            vrt_config::ConfigError::UnresolvedReference("secretRef"),
                        )
                        | crate::assemble::AssembleError::Render(
                            vrt_config::ConfigError::UnresolvedReference("varRef"),
                        ) => "render-inputs-unavailable",
                        _ => "workers-not-ready",
                    };
                    runtime.set_assemble_error(Some(reason.to_string()));
                    outcome.failed.push(tenant.tenant_id.clone());
                }
            }
        }
        let removed: Vec<String> = self
            .supervisors
            .keys()
            .filter(|t| !seen.contains(*t))
            .cloned()
            .collect();
        for tenant_id in removed {
            if let Some(handle) = self.supervisors.get(&tenant_id) {
                // RV-R53: desired-set removal is "not serving" — a
                // signed tenant tombstone upgrades it to Undeploy
                // (destruction); without one, `Stop` drains and keeps
                // every byte of local + object-store DO data.
                if tomb_tenants.contains(tenant_id.as_str()) {
                    let _ = handle.try_send(SupervisorCmd::Undeploy);
                } else {
                    crate::metrics::registry().inc(
                        "vrt_host_tenant_stop_retained_total",
                        &[],
                        1.0,
                    );
                    tracing::info!(
                        tenant = %tenant_id,
                        "tenant absent from desired — stopping, DO data retained (no deletion tombstone)"
                    );
                    let _ = handle.try_send(SupervisorCmd::Stop);
                }
            }
            self.applied.remove(&tenant_id);
            outcome.removed.push(tenant_id);
        }
        // RV-R53.1/4: route worker/class tombstones to live actors and
        // host-level purge for absent tenants; drain the durable
        // object-store purge ledger (bounded per converge).
        self.process_deletions(desired, &seen);
        if let Some(purge) = &self.deps.purge {
            let d = purge.drain(
                crate::do_purge::DRAIN_MAX_ENTRIES,
                crate::do_purge::DRAIN_MAX_KEYS,
            );
            crate::metrics::registry().set(
                "vrt_host_do_purge_pending",
                &[],
                d.pending as f64,
            );
            outcome.purges_pending = d.pending;
        }
        // RV-R53.5: mark-and-sweep cell asset storage. Keep-set =
        // manifest digests referenced by this desired snapshot;
        // tenants absent from desired contribute no digests — their
        // unreferenced objects age out past the rollback-grace window.
        // Deleting assets is NOT tenant data destruction: manifests
        // remain re-uploadable from the control plane.
        {
            let mut keep: std::collections::BTreeMap<
                String,
                std::collections::BTreeSet<String>,
            > = std::collections::BTreeMap::new();
            for tenant in &desired.tenants {
                let set = keep.entry(tenant.tenant_id.clone()).or_default();
                for w in &tenant.workers {
                    for b in &w.bindings {
                        if let snapshot_protocol::compute_desired::BindingDesired::Assets(
                            a,
                        ) = b
                        {
                            set.insert(a.manifest_digest.clone());
                        }
                    }
                }
            }
            if let Some(rep) = assembler.sweep_assets(&keep) {
                crate::metrics::registry().set(
                    "vrt_host_assets_gc_deleted",
                    &[],
                    (rep.manifests_deleted + rep.objects_deleted) as f64,
                );
                if rep.manifests_deleted + rep.objects_deleted + rep.failures > 0 {
                    tracing::info!(
                        scanned = rep.scanned,
                        manifests_deleted = rep.manifests_deleted,
                        objects_deleted = rep.objects_deleted,
                        grace_kept = rep.grace_kept,
                        failures = rep.failures,
                        "asset store mark-and-sweep round"
                    );
                }
            }
        }
        self.reap_finished();
        outcome
    }

    /// RV-R53: apply signed deletion tombstones. Tenant-scope entries
    /// were already handled in `converge` (Undeploy/Stop); this routes
    /// worker/class entries to the live actor of their tenant, or to
    /// the host-level purge when no actor owns the tenant (tenant
    /// absent or actor already stopped).
    fn process_deletions(
        &mut self,
        desired: &snapshot_protocol::compute_desired::ComputeDesired,
        seen: &std::collections::HashSet<String>,
    ) {
        let mut per_tenant: std::collections::BTreeMap<String, Vec<DoDeletion>> =
            std::collections::BTreeMap::new();
        for d in &desired.deletions {
            match &d.worker_id {
                None => {
                    // Tenant-scope tombstone — actionable only while the
                    // tenant is absent and no actor owns it (an active
                    // actor gets `Undeploy` in the removed-tenants path).
                    if !seen.contains(&d.tenant_id)
                        && !self.supervisors.contains_key(&d.tenant_id)
                    {
                        self.purge_absent_tenant(&d.tenant_id);
                    }
                }
                Some(wid) => {
                    if self.supervisors.contains_key(&d.tenant_id) {
                        per_tenant
                            .entry(d.tenant_id.clone())
                            .or_default()
                            .push(DoDeletion {
                                worker_id: wid.clone(),
                                class_key: d.class_key.clone(),
                            });
                    } else {
                        match &d.class_key {
                            Some(key) => self.purge_absent_class(&d.tenant_id, wid, key),
                            None => self.purge_absent_worker(&d.tenant_id, wid),
                        }
                    }
                }
            }
        }
        for (tenant_id, list) in per_tenant {
            if let Some(handle) = self.supervisors.get(&tenant_id) {
                let _ = handle.try_send(SupervisorCmd::Deletions(list));
            }
        }
    }

    /// DO storage root for `tenant_id` (`<root>/<segment(tenant)>` —
    /// same derivation the assembler uses).
    fn do_tenant_dir(&self, tenant_id: &str) -> Option<PathBuf> {
        self.deps
            .do_storage_root
            .as_ref()
            .map(|r| r.join(vrt_securefs::segment_encode(tenant_id)))
    }

    /// Queue every 64-hex class dir under `worker_dir` for object-store
    /// cleanup. Returns the class keys found (tests inspect it).
    fn queue_class_dirs(&self, tenant_id: &str, worker_id: &str, worker_dir: &Path) -> usize {
        let Some(purge) = &self.deps.purge else {
            return 0;
        };
        let mut n = 0usize;
        if let Ok(entries) = std::fs::read_dir(worker_dir) {
            for e in entries.flatten() {
                if !e.file_type().is_ok_and(|t| t.is_dir()) {
                    continue;
                }
                let name = e.file_name();
                if let Some(k) = name.to_str() {
                    if k.len() == 64 && k.bytes().all(|b| b.is_ascii_hexdigit()) {
                        purge.queue(tenant_id, worker_id, k);
                        n += 1;
                    }
                }
            }
        }
        n
    }

    /// Host-level purge: whole tenant dir + every class prefix.
    /// Tombstone-authorized only — call sites verify `deletions` first.
    fn purge_absent_tenant(&mut self, tenant_id: &str) {
        let Some(dir) = self.do_tenant_dir(tenant_id) else {
            return;
        };
        if let Ok(workers) = std::fs::read_dir(&dir) {
            for w in workers.flatten() {
                if !w.file_type().is_ok_and(|t| t.is_dir()) {
                    continue;
                }
                let name = w.file_name();
                if let Some(wid) = name.to_str() {
                    self.queue_class_dirs(tenant_id, wid, &w.path());
                }
            }
        }
        if dir.exists() {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    crate::metrics::registry()
                        .inc("vrt_host_do_dir_deletes_total", &[("kind", "tenant")], 1.0);
                    tracing::info!(tenant = %tenant_id, "do storage tenant dir removed (deletion tombstone)");
                }
                Err(e) => {
                    crate::metrics::registry().inc("vrt_host_do_sweep_failures_total", &[], 1.0);
                    tracing::warn!(tenant = %tenant_id, %e, "do storage tenant dir removal failed");
                }
            }
        }
    }

    /// Host-level purge of one worker's DO dir (tombstone-authorized).
    fn purge_absent_worker(&mut self, tenant_id: &str, worker_id: &str) {
        let Some(tdir) = self.do_tenant_dir(tenant_id) else {
            return;
        };
        let wdir = tdir.join(worker_id);
        self.queue_class_dirs(tenant_id, worker_id, &wdir);
        if wdir.exists() {
            match std::fs::remove_dir_all(&wdir) {
                Ok(()) => {
                    crate::metrics::registry()
                        .inc("vrt_host_do_dir_deletes_total", &[("kind", "worker")], 1.0);
                    tracing::info!(
                        tenant = %tenant_id,
                        worker = worker_id,
                        "do storage worker dir removed (deletion tombstone)"
                    );
                }
                Err(e) => {
                    crate::metrics::registry().inc("vrt_host_do_sweep_failures_total", &[], 1.0);
                    tracing::warn!(tenant = %tenant_id, worker = worker_id, %e, "do storage worker dir removal failed");
                }
            }
        }
    }

    /// Host-level purge of one class — object prefix via the ledger,
    /// then the local dir + every sibling marker/lease file naming
    /// that key (`.epoch`, `.lease-epoch`, `.final-snap-pending`,
    /// `.xfer-quarantine[.since]`, `-sib-*`).
    fn purge_absent_class(&mut self, tenant_id: &str, worker_id: &str, class_key: &str) {
        if let Some(purge) = &self.deps.purge {
            purge.queue(tenant_id, worker_id, class_key);
        }
        let Some(tdir) = self.do_tenant_dir(tenant_id) else {
            return;
        };
        let wdir = tdir.join(worker_id);
        if let Ok(entries) = std::fs::read_dir(&wdir) {
            for e in entries.flatten() {
                let name = e.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                let base = name
                    .strip_suffix(crate::do_transfer::QUARANTINE_SINCE_SUFFIX)
                    .unwrap_or(name)
                    .strip_suffix(crate::do_transfer::QUARANTINE_SUFFIX)
                    .unwrap_or(name)
                    .strip_suffix(FINAL_PENDING_SUFFIX)
                    .unwrap_or(name);
                let owned = crate::do_placement::lease_entry_class_key(base) == class_key
                    || name.starts_with(&format!("{class_key}-sib-"));
                if !owned {
                    continue;
                }
                let path = e.path();
                let removed = if path.is_dir() {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_file(&path)
                };
                match removed {
                    Ok(()) => {
                        crate::metrics::registry()
                            .inc("vrt_host_do_dir_deletes_total", &[("kind", "class")], 1.0);
                        tracing::info!(
                            tenant = %tenant_id,
                            worker = worker_id,
                            entry = name,
                            "do storage class entry removed (deletion tombstone)"
                        );
                    }
                    Err(e) => {
                        crate::metrics::registry().inc("vrt_host_do_sweep_failures_total", &[], 1.0);
                        tracing::warn!(tenant = %tenant_id, worker = worker_id, %e, "do storage class entry removal failed");
                    }
                }
            }
        }
    }

    /// Remove finished (Stopped) supervisors from the registry and join
    /// their tasks.
    pub fn reap_finished(&mut self) {
        let finished: Vec<String> = self
            .supervisors
            .iter()
            .filter(|(_, h)| h.is_finished())
            .map(|(t, _)| t.clone())
            .collect();
        for tenant_id in finished {
            if let Some(handle) = self.supervisors.remove(&tenant_id) {
                self.registry.remove(&tenant_id);
                tokio::spawn(handle.join());
            }
        }
    }

    /// Stop every tenant and join all actors; each tenant's own drain cap
    /// applies, bounded globally by `deadline` (excess tenants are
    /// force-killed by their drain deadlines — the bound is structural).
    pub async fn shutdown_all(&mut self, deadline: Duration) {
        for handle in self.supervisors.values() {
            let _ = handle.try_send(SupervisorCmd::Stop);
        }
        let end = Instant::now() + deadline;
        let handles: Vec<(String, SupervisorHandle)> = self.supervisors.drain().collect();
        for (tenant_id, mut handle) in handles {
            let remain = end.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remain.max(Duration::from_millis(1)), &mut handle.task)
                .await
                .is_err()
            {
                // T34f2/R2: abort the wedged actor — dropping its task
                // drops the Actor's generations and their GroupChild
                // guards, which SIGKILL the process groups — children
                // die on OUR deadline instead of lingering to the
                // runtime teardown (or surviving it entirely).
                tracing::warn!(
                    tenant = %tenant_id,
                    "tenant supervisor did not stop within the shutdown deadline; aborting"
                );
                handle.task.abort();
                // Await the abort completing — the task's drop runs
                // GroupChild guards which SIGKILL the process groups;
                // returning before that drop could let the process
                // exit with children mid-flight.
                let _ = tokio::time::timeout(Duration::from_millis(500), &mut handle.task).await;
            }
            self.registry.remove(&tenant_id);
        }
        self.applied.clear();
    }

    /// Next generation from the SHARED monotone counter — actor retry
    /// reassembly draws from the same space so `gen-<n>` dirs can never
    /// collide across converge and retry paths (T13/H2).
    fn next_gen(&mut self, _tenant_id: &str) -> u64 {
        self.deps.gen_counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Registry accessor.
    pub fn registry(&self) -> Arc<TenantRegistry> {
        self.registry.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::do_placement::{DoPlacement, PlacementEpoch, PlacementError};
    use vrt_objstore::ObjectStore as _;

    fn tmpdir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "vrt-host-sup-{}-{tag}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ---- T34f: orphan reaper ----

    /// Spawn a stand-in "orphaned workerd": own process group (like
    /// the real launcher's process_group(0)), alive, cmdline
    /// containing `arg`. `tail -f` keeps argv literal — `sh -c` would
    /// exec away the marker.
    fn fake_orphan(arg: &Path) -> std::process::Child {
        use std::os::unix::process::CommandExt;
        let mut c = std::process::Command::new("tail");
        c.arg("-f").arg(arg).stdin(std::process::Stdio::null());
        c.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        c.process_group(0);
        c.spawn().unwrap()
    }

    /// `ps -p` readable? (sandboxed runs deny process queries → the
    /// reaper's cmdline check can never verify → Verify entries are
    /// all spared and the kill assertions would be unexercisable.)
    fn ps_probe() -> bool {
        std::process::Command::new("ps")
            .args(["-p", &std::process::id().to_string(), "-o", "command="])
            .output()
            .is_ok_and(|o| o.status.success())
    }

    /// A pid guaranteed dead: spawn, exit, reap — its pid cannot be
    /// ours and is overwhelmingly unlikely to be recycled mid-test.
    fn dead_pid() -> u32 {
        let mut c = std::process::Command::new("true").spawn().unwrap();
        c.wait().unwrap();
        c.id()
    }

    /// R1 pure-function coverage — the kill decision without ps/proc.
    #[test]
    fn classify_pidfile_only_verifies_dead_owner_live_worker() {
        let me = std::process::id();
        let alive = |p: u32| p == me || p == 4242;
        // Live owner host → always spare, worker state irrelevant.
        assert_eq!(
            classify_pidfile("4242 9999", me, |p| p == 9999),
            ReapVerdict::Spare
        );
        // Owner = self → spare (live instance's own generations).
        assert_eq!(
            classify_pidfile(format!("4242 {me}").as_str(), me, alive),
            ReapVerdict::Spare
        );
        // Dead owner + live worker → Verify (cmdline still gates kill).
        assert_eq!(
            classify_pidfile("4242 7777", me, alive),
            ReapVerdict::Verify(4242)
        );
        // Dead owner + dead worker → clean the stale file.
        assert_eq!(classify_pidfile("8888 7777", me, alive), ReapVerdict::Clean);
        // Old single-field or garbage → never guess.
        for bad in ["4242", "abc def", "", "1 2 3", "-1 2"] {
            assert_eq!(
                classify_pidfile(bad, me, alive),
                ReapVerdict::Spare,
                "{bad}"
            );
        }
    }

    /// R1 regression — NO ps involved: a workerd whose pidfile owner
    /// is THIS (live) process is never reaped, even though the file
    /// and the live cmdline both match the orphan shape. This is the
    /// "gen-1 survives while gen-2 launches" invariant.
    #[test]
    fn startup_reaper_spares_processes_owned_by_live_host() {
        let root = std::env::temp_dir().join(format!("vrt-reap-own-{}", std::process::id()));
        let seg = root.join("t1");
        let gen = seg.join("gen-1");
        std::fs::create_dir_all(&gen).unwrap();
        let keep = gen.join("keep");
        std::fs::write(&keep, "x").unwrap();
        let mut own = fake_orphan(&keep);
        // pidfile claims owner = US (a live host) — the R1 case.
        std::fs::write(
            gen.join("workerd.pid"),
            format!("{} {}\n", own.id(), std::process::id()),
        )
        .unwrap();
        reap_orphaned_children(&root);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            own.try_wait().unwrap().is_none(),
            "live-host-owned process was killed — the R1 bug"
        );
        assert!(gen.join("workerd.pid").exists());
        let _ = own.kill();
        let _ = own.wait();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Startup sweep kills a verified workerd whose owning host is
    /// dead; spares anything unverifiable. ps-dependent (cmdline
    /// check) — pure logic coverage lives in the classify test.
    #[test]
    fn startup_reaper_kills_dead_owner_verified_orphan() {
        if !ps_probe() {
            eprintln!("SKIP: ps unavailable — sandboxed run");
            return;
        }
        let root = std::env::temp_dir().join(format!("vrt-reap-{}", std::process::id()));
        let seg = root.join("t1");
        let gen_own = seg.join("gen-1");
        let gen_foreign = seg.join("gen-2");
        let other = seg.join("other");
        for d in [&gen_own, &gen_foreign, &other] {
            std::fs::create_dir_all(d).unwrap();
        }
        // Verified orphan: dead owner + live worker + cmdline match.
        let keep = gen_own.join("keep");
        std::fs::write(&keep, "x").unwrap();
        let mut own = fake_orphan(&keep);
        std::fs::write(
            gen_own.join("workerd.pid"),
            format!("{} {}\n", own.id(), dead_pid()),
        )
        .unwrap();
        // Foreign: dead owner + live worker but cmdline lacks gen dir.
        let keep2 = other.join("keep");
        std::fs::write(&keep2, "x").unwrap();
        let mut foreign = fake_orphan(&keep2);
        std::fs::write(
            gen_foreign.join("workerd.pid"),
            format!("{} {}\n", foreign.id(), dead_pid()),
        )
        .unwrap();
        // Dead-worker pidfile → cleaned. Garbage → skipped. Non-gen
        // dir pidfile → untouched.
        let gen_dead = seg.join("gen-3");
        std::fs::create_dir_all(&gen_dead).unwrap();
        std::fs::write(gen_dead.join("workerd.pid"), "999999 888888\n").unwrap();
        let gen_garbage = seg.join("gen-4");
        std::fs::create_dir_all(&gen_garbage).unwrap();
        std::fs::write(gen_garbage.join("workerd.pid"), "not-a-pid").unwrap();
        std::fs::write(
            other.join("workerd.pid"),
            format!("{} 1\n", std::process::id()),
        )
        .unwrap();

        reap_orphaned_children(&root);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let exited = loop {
            match own.try_wait() {
                Ok(Some(_)) => break true,
                _ if std::time::Instant::now() > deadline => break false,
                _ => std::thread::sleep(std::time::Duration::from_millis(20)),
            }
        };
        assert!(exited, "dead-owner verified orphan not killed");
        assert!(!gen_own.join("workerd.pid").exists());
        assert!(
            foreign.try_wait().unwrap().is_none(),
            "cmdline-mismatch killed!"
        );
        assert!(gen_foreign.join("workerd.pid").exists());
        assert!(!gen_dead.join("workerd.pid").exists());
        assert!(gen_garbage.join("workerd.pid").exists());
        assert!(other.join("workerd.pid").exists());
        let _ = foreign.kill();
        let _ = foreign.wait();
        let _ = std::fs::remove_dir_all(&root);
    }

    // ---- L15: stale-socket cleanup ----

    #[test]
    fn stale_socket_cleanup_leaves_foreign_types_alone() {
        // Regular files, dirs, symlinks inside the sock dir must never
        // be removed — only socket inodes are eligible.
        let dir = tmpdir("sock");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        let file = sock_dir.join("stray.txt");
        std::fs::write(&file, b"keep").unwrap();
        let nested = sock_dir.join("nested");
        std::fs::create_dir(&nested).unwrap();
        let link = sock_dir.join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        clean_stale_sockets(&sock_dir);
        assert!(file.exists(), "regular file kept");
        assert!(nested.exists(), "dir kept");
        assert!(
            link.exists() || link.symlink_metadata().is_ok(),
            "symlink kept"
        );
    }

    #[test]
    fn stale_socket_cleanup_removes_real_socket() {
        // unix bind() is denied in the dev sandbox — gate exactly like
        // the unix-listener tests do.
        if std::env::var("VRT_HOST_UNIX_TESTS").ok().as_deref() != Some("1") {
            eprintln!("skip: set VRT_HOST_UNIX_TESTS=1");
            return;
        }
        let dir = tmpdir("sock-real");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        let path = sock_dir.join("w1.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        drop(_listener);
        assert!(path.exists());
        clean_stale_sockets(&sock_dir);
        assert!(!path.exists(), "stale socket removed");
    }

    // ---- L17: gen-dir sweep ----

    #[test]
    fn sweep_keeps_newest_unreferenced_removes_rest() {
        let dir = tmpdir("sweep");
        let mut gens = Vec::new();
        for i in 1..=5 {
            let g = dir.join(format!("gen-{i}"));
            std::fs::create_dir(&g).unwrap();
            gens.push(g);
        }
        // Same-mtime fallback sorts by name desc — gen-5 is newest.
        let doomed = select_sweep_dirs(gens.clone());
        assert_eq!(doomed.len(), 4);
        assert!(!doomed.contains(&gens[4]), "newest kept for diagnostics");
        assert!(gens[1..4].iter().all(|g| doomed.contains(g)));
    }

    #[test]
    fn sweep_single_unreferenced_is_kept() {
        let dir = tmpdir("sweep-one");
        let g = dir.join("gen-1");
        std::fs::create_dir(&g).unwrap();
        assert!(select_sweep_dirs(vec![g.clone()]).is_empty());
    }

    // ---- P3c-2-r1: fenced lease stops the writer NOW (P0-2) ----

    /// `renew` always answers the injected verdict — `Fenced` is a
    /// terminal verdict (epoch/holder provably moved on), `Store` is
    /// transient unreachability.
    struct StubPlacement(PlacementError);
    impl DoPlacement for StubPlacement {
        fn authoritative(&self) -> bool {
            true
        }
        // renew_due = expiry - 120s < kill_due = expiry - 60s — with
        // expiry = now+120s the lease is DUE NOW but the kill boundary
        // is still a minute out: exactly the window where the old code
        // kept a fenced writer alive.
        fn renew_margin_ms(&self) -> u64 {
            120_000
        }
        fn kill_margin_ms(&self) -> u64 {
            60_000
        }
        fn acquire(
            &self,
            _s: &crate::do_placement::PlacementScope,
        ) -> Result<crate::do_placement::PlacementLease, PlacementError> {
            Ok(crate::do_placement::PlacementLease {
                epoch: PlacementEpoch(1),
                expires_at_ms: wall_now_ms() + 120_000,
            })
        }
        fn renew(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<u64, PlacementError> {
            Err(match &self.0 {
                PlacementError::Fenced { current, until_ms } => PlacementError::Fenced {
                current: *current,
                until_ms: *until_ms,
            },
                PlacementError::Reserved { until_ms } => PlacementError::Reserved {
                    until_ms: *until_ms,
                },
                PlacementError::Precondition(s) => PlacementError::Precondition(s.clone()),
                PlacementError::Store(_) => PlacementError::Store(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "store down",
                )),
            })
        }
        fn fence(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            Ok(())
        }
    }

    /// MIG-10: bounded lease release — a slow/wedged store must not
    /// pin the shutdown drop past its budget; remaining leases simply
    /// lapse by TTL (crash semantics preserved).
    struct SlowFencePlacement {
        calls: std::sync::Mutex<u32>,
        sleep: Duration,
    }
    impl DoPlacement for SlowFencePlacement {
        fn authoritative(&self) -> bool {
            true
        }
        fn acquire(
            &self,
            _s: &crate::do_placement::PlacementScope,
        ) -> Result<crate::do_placement::PlacementLease, PlacementError> {
            Ok(crate::do_placement::PlacementLease {
                epoch: PlacementEpoch(1),
                expires_at_ms: wall_now_ms() + 60_000,
            })
        }
        fn fence(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            *self.calls.lock().unwrap() += 1;
            std::thread::sleep(self.sleep);
            Ok(())
        }
    }

    #[test]
    fn release_leases_bounded_stops_at_budget() {
        let placement = SlowFencePlacement {
            calls: std::sync::Mutex::new(0),
            sleep: Duration::from_millis(30),
        };
        let dir = tmpdir("bounded-release");
        let leases: Vec<DoLeaseEntry> = (0..5)
            .map(|_| DoLeaseEntry {
                scope: lease_scope(&dir),
                epoch: PlacementEpoch(1),
                expires_at_ms: wall_now_ms() + 60_000,
            })
            .collect();
        let t0 = Instant::now();
        Actor::release_leases_bounded(&placement, "t", &leases, Some(Duration::from_millis(65)));
        let calls = *placement.calls.lock().unwrap();
        // Each fence costs >=30 ms; the 65 ms budget admits at most a
        // few of the 5 — never the whole set — and the pass returns
        // inside budget + one fence, nowhere near an unbounded wait.
        assert!(calls < 5, "budget must cut the pass early (calls={calls})");
        assert!(t0.elapsed() < Duration::from_secs(10));
        // A zero budget releases nothing at all.
        Actor::release_leases_bounded(&placement, "t", &leases, Some(Duration::ZERO));
        assert_eq!(*placement.calls.lock().unwrap(), calls);
    }

    fn lease_scope(dir: &Path) -> crate::do_placement::PlacementScope {
        crate::do_placement::PlacementScope {
            tenant: "t".into(),
            worker: "w".into(),
            class_key: "a".repeat(64),
            class_dir: dir.to_path_buf(),
        }
    }

    fn test_spec(dir: &Path) -> TenantLaunchSpec {
        TenantLaunchSpec {
            tenant_id: "t".into(),
            binary: PathBuf::from("/nonexistent"),
            config_path: dir.join("config.capnp"),
            config_digest: "d".into(),
            work_dir: dir.to_path_buf(),
            external_addrs: vec![],
            env: Default::default(),
            base_env: Default::default(),
            worker_endpoints: Default::default(),
            qconsumer_endpoints: Default::default(),
            wfops_endpoints: Default::default(),
            cron_endpoints: Default::default(),
            wfop_tokens: Default::default(),
            limits: crate::spec::ResourceLimits::default(),
            heartbeat_endpoint: None,
        waituntil_endpoint: None,
            extra_args: vec![],
            generation: 1,
            isolation: None,
            relaunch: None,
            do_storage: None,
        }
    }

    fn test_actor(deps: SupervisorDeps) -> Actor {
        let params = SupervisorParams::default();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (ev_tx, ev_rx) = mpsc::channel(32);
        drop(cmd_tx);
        let dir = tmpdir("actor");
        Actor {
            tenant_id: "t".into(),
            runtime: TenantRuntime::new("t"),
            cmd_rx,
            ev_tx,
            ev_rx,
            deps,
            ready_timeout: params.ready_timeout,
            probe_timeout: params.probe_timeout,
            kill_grace: params.kill_grace,
            fail_threshold: params.fail_threshold,
            circuit_cooldown: params.circuit_cooldown,
            drain_timeout: params.drain_timeout,
            monitor_interval: params.monitor_interval,
            backoff: RestartBackoff::new(params.backoff.0, params.backoff.1),
            window: FailureWindow::new(params.fail_window, params.fail_threshold),
            state: TenantState::Ready,
            desired_spec: Some(Box::new(test_spec(&dir))),
            active: None,
            draining: Vec::new(),
            pending: None,
            seq: 0,
            cooldown_until: None,
            half_open: false,
            retry_at: None,
            next_monitor: Instant::now(),
            seg_dir: None,
            attempted_once: false,
            switch_fail_streak: 0,
            latched_digest: None,
            attributed: std::collections::BTreeSet::new(),
            excluded: std::collections::BTreeSet::new(),
            max_switch_failures: params.max_switch_failures,
            runaway: None,
            runaway_kill: None,
            probe_in_flight: None,
            probe_seq: 0,
            wait_until_grace: params.wait_until_grace,
            do_switch_wait_until_grace: params.do_switch_wait_until_grace,
            wu_probe_seq: 0,
            runaway_window: FailureWindow::new(params.fail_window, params.fail_threshold),
            runaway_breaker_pending: false,
            do_wait_since: None,
            do_switch_exit_at: None,
            preflight_timeout: params.preflight_timeout,
            preflight_ok_digest: None,
            preflight: None,
            prewarm: None,
            prewarm_ttl: params.prewarm_ttl,
            prewarm_burned: false,
            preflight_seq: 0,
            do_tenant_dir: None,
            undeploy: false,
            pending_do_leases: Vec::new(),
            do_lease_wake: None,
            do_class_keys: Default::default(),
            do_deletions: Default::default(),
            departing_snap_pending: Default::default(),
            held_departing: Vec::new(),
            carried_leases: Vec::new(),
            carried_lost: Default::default(),
            do_pipeline: params.do_pipeline,
            do_snap_due: Default::default(),
            do_snap_ok_at: Default::default(),
            cache_listener: None,
            fenced_log: (None, Duration::from_secs(5)),
            s2z: None,
            ready_since: Instant::now(),
            hib_next_eval: Instant::now(),
            hib_backoff: Default::default(),
            hib: None,
            wake_run: None,
            hib_probe_seq: 0,
        }
    }

    async fn fake_live_gen(dir: &Path, lease_expiry_ms: u64) -> LiveGen {
        let (_a, b) = tokio::net::UnixStream::pair().unwrap();
        LiveGen {
            id: 1,
            pid: std::process::id(),
            spec: Box::new(test_spec(dir)),
            slots: Arc::new(BTreeMap::new()),
            qconsumer_slots: Arc::new(BTreeMap::new()),
            wfops_slots: Arc::new(BTreeMap::new()),
            cron_slots: Arc::new(BTreeMap::new()),
            watcher: tokio::spawn(std::future::pending::<()>()),
            _control: b,
            work_dir: dir.to_path_buf(),
            leases: vec![DoLeaseEntry {
                scope: lease_scope(dir),
                epoch: PlacementEpoch(1),
                expires_at_ms: lease_expiry_ms,
            }],
        }
    }

    /// P0-2 regression: `Fenced` at renew must stop the writer NOW —
    /// the pre-fix code only re-armed at kill_due and kept serving
    /// writes for up to `lease - 2*margin` after the fence.
    // ---- PT1: runaway watchdog wiring (sample_active) ----

    /// Scripted cpu_seconds source — each monitor tick pops one
    /// sample; `None` models a sampler failure.
    struct ScriptSampler(std::sync::Mutex<std::collections::VecDeque<Option<f64>>>);
    impl crate::monitor::ResourceSampler for ScriptSampler {
        fn sample(&self, _pid: u32) -> crate::monitor::ResourceUsage {
            crate::monitor::ResourceUsage {
                cpu_seconds: self.0.lock().unwrap().pop_front().flatten(),
                rss_bytes: None,
                fds: None,
            }
        }
    }

    struct FixedProbe(bool);
    impl crate::runaway::HeartbeatProbe for FixedProbe {
        fn probe(&self, _ep: &crate::endpoint::WorkerEndpoint, _t: Duration) -> bool {
            self.0
        }
    }

    /// A pid whose process GROUP provably does not exist (macOS/Linux
    /// pids stay far below 4e9) — `kill(-pgid)` returns ESRCH, so the
    /// kill path is exercised without signalling anything real.
    const DEAD_PID: u32 = 4_000_000_000;

    fn runaway_spec(dir: &Path) -> TenantLaunchSpec {
        let mut spec = test_spec(dir);
        spec.limits.runaway = Some(crate::runaway::RunawaySpec {
            window: Duration::from_millis(100),
            min_cpu_ratio: 0.5,
            heartbeat_timeout: Duration::from_millis(50),
            heartbeat_failures: 2,
        });
        spec.heartbeat_endpoint = Some(crate::endpoint::WorkerEndpoint::Tcp(
            "127.0.0.1:9".parse().unwrap(),
        ));
        spec
    }

    async fn runaway_actor(dir: &Path, cpus: Vec<Option<f64>>, hb_ok: bool) -> Actor {
        let deps = SupervisorDeps {
            sampler: Arc::new(ScriptSampler(std::sync::Mutex::new(cpus.into()))),
            heartbeat: Arc::new(FixedProbe(hb_ok)),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let mut gen = fake_live_gen(dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(runaway_spec(dir));
        actor.active = Some(gen);
        actor.runtime.test_set_state(TenantState::Ready);
        actor
    }

    /// Drive spawned probe tasks to completion and deliver their
    /// `Ev::ProbeResult` into the actor (the actor loop would do this
    /// in production; unit tests drive it explicitly).
    async fn flush_probe_events(a: &mut Actor) {
        tokio::time::sleep(Duration::from_millis(10)).await;
        while let Ok(ev) = a.ev_rx.try_recv() {
            a.on_ev(ev);
        }
    }

    /// Probe that sleeps in the blocking pool — proves the actor
    /// never waits on it (PT1-r2).
    struct SlowProbe(Duration);
    impl crate::runaway::HeartbeatProbe for SlowProbe {
        fn probe(&self, _ep: &crate::endpoint::WorkerEndpoint, _t: Duration) -> bool {
            std::thread::sleep(self.0);
            false
        }
    }

    /// Both signals held across the window → kill mark + kill
    /// counted; only one signal → no kill, metrics only.
    #[tokio::test]
    async fn runaway_kills_only_on_dual_signal() {
        let dir = tmpdir("rw-dual");
        let base = Instant::now();
        // cpu_seconds climbing 1.0/s (ratio 1.0 ≥ 0.5) every 50ms —
        // saturated since t=50ms; probes are CPU-gated so the failure
        // streak starts at t=50, completing at t=100; window 100ms
        // earns signal1 at t=150.
        let cpus: Vec<Option<f64>> = (0..=10).map(|i| Some(i as f64 * 0.05)).collect();
        let mut a = runaway_actor(&dir, cpus, false).await;
        for i in 0..5 {
            a.sample_active(base + Duration::from_millis(i * 50));
            flush_probe_events(&mut a).await;
        }
        assert!(a.runaway_kill.is_some(), "dual signal held must kill");
        // Deduped: one verdict cycle per generation counts exactly
        // one kill (production's gen exits right after the SIGKILL;
        // without the kill marker each extra tick would re-count).
        assert_eq!(a.runtime.stats.runaway_kills.load(Ordering::Relaxed), 1);
        assert!(
            a.runtime
                .stats
                .runaway_heartbeat_failures
                .load(Ordering::Relaxed)
                >= 2
        );
        assert!(a.runtime.stats.runaway_probes.load(Ordering::Relaxed) >= 2);
    }

    #[tokio::test]
    async fn runaway_no_kill_when_heartbeat_answers() {
        let dir = tmpdir("rw-hbok");
        let base = Instant::now();
        let cpus: Vec<Option<f64>> = (0..=10).map(|i| Some(i as f64)).collect();
        let mut a = runaway_actor(&dir, cpus, true).await;
        for i in 0..5 {
            a.sample_active(base + Duration::from_millis(i * 50));
            flush_probe_events(&mut a).await;
        }
        assert!(a.runaway_kill.is_none(), "live heartbeat must block kill");
        assert_eq!(a.runtime.stats.runaway_kills.load(Ordering::Relaxed), 0);
        // Signal1 alone is still a typed verdict, metric only.
        assert_eq!(
            a.runtime.stats.runaway_last_verdict.load(Ordering::Relaxed),
            crate::runaway::RunawayVerdict::CpuOnly.code()
        );
    }

    /// PT1-r2: missing CPU ⇒ never a candidate ⇒ no probes ever
    /// spawn; the verdict stays CpuUnavailable (signal 1 unprovable,
    /// never counted as zero).
    #[tokio::test]
    async fn runaway_no_kill_on_missing_cpu_sample() {
        let dir = tmpdir("rw-nocpu");
        let base = Instant::now();
        let cpus: Vec<Option<f64>> = vec![None, None, None];
        let mut a = runaway_actor(&dir, cpus, false).await;
        for i in 0..3 {
            a.sample_active(base + Duration::from_millis(i * 50));
            flush_probe_events(&mut a).await;
        }
        assert!(a.runaway_kill.is_none(), "unreadable cpu must not kill");
        assert_eq!(
            a.runtime.stats.runaway_probes.load(Ordering::Relaxed),
            0,
            "cold CPU must never spawn a probe"
        );
        assert_eq!(
            a.runtime
                .stats
                .runaway_cpu_unavailable
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            a.runtime.stats.runaway_last_verdict.load(Ordering::Relaxed),
            crate::runaway::RunawayVerdict::CpuUnavailable.code()
        );
    }

    /// fail_threshold=5 (SupervisorParams::default) → five kills
    /// inside the window fuse the breaker pending flag.
    #[tokio::test]
    async fn runaway_streak_sets_breaker_pending() {
        let dir = tmpdir("rw-breaker");
        let base = Instant::now();
        let cpus: Vec<Option<f64>> = (0..=40).map(|i| Some(i as f64)).collect();
        let mut a = runaway_actor(&dir, cpus, false).await;
        // Each kill cycle re-seeds the detector like an exit+install
        // does; five kills ~600ms apart arm the breaker at #5.
        for kill in 1..=5 {
            a.runaway = None;
            a.runaway_kill = None;
            a.probe_in_flight = None;
            for i in 0..5 {
                a.sample_active(base + Duration::from_millis(i * 50 + kill * 600));
                flush_probe_events(&mut a).await;
            }
            assert!(a.runaway_kill.is_some(), "kill {kill} should fire");
            let want_pending = kill >= 5;
            assert_eq!(
                a.runaway_breaker_pending, want_pending,
                "breaker pending must arm only at kill #{kill}=5"
            );
        }
    }

    /// PT1-r2: probes are CPU-gated — a cold tenant costs zero probes
    /// and never blocks on anything.
    #[tokio::test]
    async fn runaway_probe_gated_by_cpu() {
        let dir = tmpdir("rw-gate");
        let base = Instant::now();
        // ratio 0.2 < 0.5 — never a saturation candidate.
        let cpus: Vec<Option<f64>> = (0..=8).map(|i| Some(i as f64 * 0.01)).collect();
        let mut a = runaway_actor(&dir, cpus, false).await;
        for i in 0..6 {
            a.sample_active(base + Duration::from_millis(i * 50));
            flush_probe_events(&mut a).await;
        }
        assert_eq!(a.runtime.stats.runaway_probes.load(Ordering::Relaxed), 0);
        assert!(a.probe_in_flight.is_none());
        assert!(a.runaway_kill.is_none());
        assert_eq!(
            a.runtime.stats.runaway_last_verdict.load(Ordering::Relaxed),
            crate::runaway::RunawayVerdict::Healthy.code()
        );
    }

    /// PT1-r2: a slow probe cannot stall the actor — the tick returns
    /// immediately, never stacks a second probe, and the result lands
    /// asynchronously (timeout → fail closed).
    #[tokio::test(flavor = "current_thread")]
    async fn runaway_probe_off_actor_slow_probe() {
        let dir = tmpdir("rw-slow");
        let base = Instant::now();
        let cpus: Vec<Option<f64>> = (0..=20).map(|i| Some(i as f64)).collect();
        let deps = SupervisorDeps {
            sampler: Arc::new(ScriptSampler(std::sync::Mutex::new(cpus.into()))),
            heartbeat: Arc::new(SlowProbe(Duration::from_millis(300))),
            ..Default::default()
        };
        let mut a = test_actor(deps);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(runaway_spec(&dir));
        a.active = Some(gen);
        a.runtime.test_set_state(TenantState::Ready);

        a.sample_active(base); // seed
        let t0 = Instant::now();
        a.sample_active(base + Duration::from_millis(50)); // sat → probe
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "tick must not wait on the 300ms probe ({elapsed:?})"
        );
        assert!(a.probe_in_flight.is_some());
        assert_eq!(a.runtime.stats.runaway_probes.load(Ordering::Relaxed), 1);
        // Actor keeps working while the probe is in flight — the next
        // tick does NOT stack a second probe.
        a.sample_active(base + Duration::from_millis(100));
        assert_eq!(a.runtime.stats.runaway_probes.load(Ordering::Relaxed), 1);
        // Probe budget 50ms < sleep 300ms → timeout → fail closed.
        tokio::time::sleep(Duration::from_millis(150)).await;
        while let Ok(ev) = a.ev_rx.try_recv() {
            a.on_ev(ev);
        }
        assert_eq!(
            a.runtime
                .stats
                .runaway_heartbeat_failures
                .load(Ordering::Relaxed),
            1
        );
        assert!(a.probe_in_flight.is_none());
    }

    /// PT1-r2: a probe result is only meaningful for the generation
    /// it probed — a stale result must not poison the replacement
    /// generation's streak; a seq-mismatched result must not free the
    /// live probe slot.
    #[tokio::test]
    async fn runaway_probe_result_stale_gen_ignored() {
        let dir = tmpdir("rw-stale");
        let base = Instant::now();
        let cpus: Vec<Option<f64>> = (0..=10).map(|i| Some(i as f64)).collect();
        let mut a = runaway_actor(&dir, cpus, false).await;
        a.sample_active(base);
        a.sample_active(base + Duration::from_millis(50)); // sat → probe (gen 1)
        let (g, seq, _) = a.probe_in_flight.expect("probe should be in flight");
        assert_eq!(g, 1);
        // Gen-2 installs before the result lands.
        a.active.as_mut().unwrap().id = 2;
        a.runaway = None;
        a.on_ev(Ev::ProbeResult {
            gen: 1,
            seq,
            ok: false,
        });
        assert!(a.probe_in_flight.is_none(), "slot freed by its own result");
        assert_eq!(
            a.runtime
                .stats
                .runaway_heartbeat_failures
                .load(Ordering::Relaxed),
            0,
            "stale-generation result must not count"
        );
        // A seq-mismatched result for the CURRENT gen folds into the
        // streak (it is real liveness data) but must not free the live
        // probe slot.
        a.probe_in_flight = Some((2, 7, Instant::now()));
        a.on_ev(Ev::ProbeResult {
            gen: 2,
            seq: 8,
            ok: false,
        });
        assert!(a.probe_in_flight.is_some(), "seq mismatch keeps the slot");
        assert_eq!(
            a.runtime
                .stats
                .runaway_heartbeat_failures
                .load(Ordering::Relaxed),
            1
        );
    }

    /// PT1-r2: an unconverged kill re-sends SIGKILL (for real — the
    /// target here is a live `sleep`) a bounded number of times, then
    /// releases the marker so detection resumes; never signals a pid
    /// the kernel could have recycled.
    #[tokio::test]
    async fn runaway_kill_stuck_resends_then_resumes() {
        let dir = tmpdir("rw-stuck");
        let base = Instant::now();
        let cpus: Vec<Option<f64>> = (0..=80).map(|i| Some(i as f64)).collect();
        let mut a = runaway_actor(&dir, cpus, false).await;
        for i in 0..4 {
            a.sample_active(base + Duration::from_millis(i * 50));
            flush_probe_events(&mut a).await;
        }
        assert!(a.runaway_kill.is_some());
        // Aim the mark at a REAL live process (same gen identity) so
        // resends prove signal efficacy rather than ESRCH noise.
        // Own process group — mirrors workerd's `process_group(0)`
        // spawn; kill(-pgid) otherwise hits ESRCH.
        let mut child = {
            use std::os::unix::process::CommandExt;
            std::process::Command::new("sleep")
                .arg("60")
                .process_group(0)
                .spawn()
                .unwrap()
        };
        let live_pid = child.id();
        a.runaway_kill.as_mut().unwrap().pid = live_pid;
        a.active.as_mut().unwrap().pid = live_pid;
        // Stuck limit = max(2×5s, 10s) = 10s — each eval past it resends.
        for r in 1..=3u32 {
            a.sample_active(base + Duration::from_secs(11 + 11 * (r - 1) as u64));
            let m = a.runaway_kill.expect("mark should persist during resends");
            assert_eq!(m.resends, r, "resend #{r} should be counted");
        }
        // SIGKILL is asynchronous — waitpid can observe "still
        // running" for a few ms after the signal lands.
        let mut dead = false;
        for _ in 0..50 {
            if child.try_wait().unwrap().is_some() {
                dead = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(4));
        }
        assert!(
            dead,
            "the first resend must SIGKILL the live process for real"
        );
        // A pid the kernel could recycle is never signalled: mismatch
        // on either (gen, pid) releases the marker without resending.
        a.runaway_kill = Some(RunawayKillMark {
            gen: 1,
            pid: live_pid,
            since: base,
            resends: 0,
        });
        a.active.as_mut().unwrap().pid = DEAD_PID; // gen same, pid changed
        a.sample_active(base + Duration::from_secs(50));
        assert!(a.runaway_kill.is_none(), "pid mismatch releases marker");
        // Past the resend cap the marker is released and detection
        // resumes — the next saturated ticks produce a NEW kill.
        a.runaway_kill = Some(RunawayKillMark {
            gen: 1,
            pid: DEAD_PID,
            since: base,
            resends: RUNAWAY_KILL_MAX_RESENDS,
        });
        a.active.as_mut().unwrap().pid = DEAD_PID;
        a.sample_active(base + Duration::from_secs(60));
        assert!(a.runaway_kill.is_none(), "cap reached releases marker");
        assert!(a.runtime.stats.runaway_kill_stuck.load(Ordering::Relaxed) >= 5);
        // Detection resumes: cpu still saturating 1.0/s, streak still
        // open → fresh kill after re-seed + window.
        for s in 61..=63u64 {
            a.sample_active(base + Duration::from_secs(s));
            flush_probe_events(&mut a).await;
        }
        assert_eq!(a.runtime.stats.runaway_kills.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn fenced_renew_kills_active_immediately() {
        let deps = SupervisorDeps {
            placement: Arc::new(StubPlacement(PlacementError::Fenced {
                current: 2,
                until_ms: 0,
            })),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let dir = tmpdir("fenced");
        // Far-future expiry: now is LONG before kill_due — under the
        // old code the writer survived; now it must die.
        let expiry = wall_now_ms() + 120_000;
        actor.active = Some(fake_live_gen(&dir, expiry).await);
        actor.runtime.test_set_state(TenantState::Ready);
        actor.service_do_leases(Instant::now());
        assert!(actor.active.is_none(), "fenced writer must die now");
        assert!(
            !matches!(actor.runtime.state(), TenantState::Ready),
            "state must leave Ready, got {:?}",
            actor.runtime.state()
        );
        assert!(actor.retry_at.is_some());
    }

    /// Same verdict on a DRAINING writer — still a writer, same
    /// immediate stop.
    #[tokio::test]
    async fn fenced_renew_kills_draining_immediately() {
        let deps = SupervisorDeps {
            placement: Arc::new(StubPlacement(PlacementError::Fenced {
                current: 2,
                until_ms: 0,
            })),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let dir = tmpdir("fenced-drain");
        let expiry = wall_now_ms() + 120_000;
        let live = fake_live_gen(&dir, expiry).await;
        actor.draining.push(DrainingGen {
            id: live.id,
            pid: live.pid,
            slots: live.slots.clone(),
            entered_at: Instant::now(),
            deadline: Instant::now() + Duration::from_secs(60),
            sigterm_at: None,
            killed: false,
            watcher: live.watcher,
            work_dir: live.work_dir.clone(),
            had_do_storage: true,
            do_switch: false,
            leases: live.leases,
            waituntil: None,
            wu_idle_since: None,
            wu_last: None,
            wu_probe: None,
            wu_next_at: Instant::now(),
        });
        actor.service_do_leases(Instant::now());
        assert!(
            actor.draining.is_empty(),
            "fenced draining writer must die now"
        );
    }

    /// Transient `Store` failure is NOT terminal: writer survives
    /// inside the kill margin and a deterministic re-check is armed
    /// AT kill_due (never riding on monitor cadence).
    #[tokio::test]
    async fn store_error_survives_until_kill_due() {
        let deps = SupervisorDeps {
            placement: Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "x",
            )))),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let dir = tmpdir("store-err");
        let expiry = wall_now_ms() + 120_000;
        actor.active = Some(fake_live_gen(&dir, expiry).await);
        actor.service_do_leases(Instant::now());
        assert!(
            actor.active.is_some(),
            "transient store failure must not stop writes early"
        );
        assert!(
            actor.do_lease_wake.is_some(),
            "re-check at kill_due must be armed"
        );
    }

    /// …but past kill_due the same transient failure stops the
    /// writer (bounded shutdown before expiry).
    #[tokio::test]
    async fn store_error_past_kill_due_kills() {
        let deps = SupervisorDeps {
            placement: Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "x",
            )))),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let dir = tmpdir("store-kill");
        // Expiry inside the kill margin → now >= kill_due.
        let expiry = wall_now_ms() + 1_000;
        actor.active = Some(fake_live_gen(&dir, expiry).await);
        actor.service_do_leases(Instant::now());
        assert!(actor.active.is_none());
    }

    /// Local-migration freeze request → queued key kills the holding
    /// generation on the next service turn (admin path, P0-2).
    #[tokio::test]
    async fn freeze_request_kills_holding_generation() {
        let deps = SupervisorDeps {
            placement: Arc::new(StubPlacement(PlacementError::Fenced {
                current: 2,
                until_ms: 0,
            })),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let dir = tmpdir("freeze");
        let expiry = wall_now_ms() + 120_000;
        actor.active = Some(fake_live_gen(&dir, expiry).await);
        actor.runtime.request_do_freeze("a".repeat(64));
        actor.service_do_freeze();
        assert!(actor.active.is_none(), "freeze must kill the holder now");
    }

    /// An in-flight launch holding the frozen class's lease must be
    /// aborted — it would open the class dir at launch-complete and
    /// become a fenced writer that just came back.
    #[tokio::test]
    async fn freeze_request_aborts_pending_launch() {
        let deps = SupervisorDeps::default();
        let mut actor = test_actor(deps);
        let dir = tmpdir("freeze-pending");
        let expiry = wall_now_ms() + 120_000;
        actor.pending = Some(PendingLaunch {
            seq: 1,
            task: tokio::spawn(std::future::pending::<()>()),
            work_dir: dir.clone(),
            desired_digest: "d".into(),
            prewarm_swap: false,
            leases: vec![DoLeaseEntry {
                scope: lease_scope(&dir),
                epoch: PlacementEpoch(1),
                expires_at_ms: expiry,
            }],
        });
        actor.runtime.request_do_freeze("a".repeat(64));
        actor.service_do_freeze();
        assert!(actor.pending.is_none(), "frozen pending launch must abort");
    }

    /// Freeze for a key this actor doesn't hold is a no-op (an
    /// unrelated class's migration must not kill this tenant).
    #[tokio::test]
    async fn freeze_request_ignores_unheld_key() {
        let deps = SupervisorDeps::default();
        let mut actor = test_actor(deps);
        let dir = tmpdir("freeze-noop");
        actor.active = Some(fake_live_gen(&dir, wall_now_ms() + 120_000).await);
        actor.runtime.request_do_freeze("b".repeat(64));
        actor.service_do_freeze();
        assert!(actor.active.is_some());
    }

    // ---- W1-r2/P0-2: pre-launch config preflight ----

    /// Bounded wait for the preflight unit tests — fast, but long
    /// enough for a `sh` child to print and exit.
    const PREFLIGHT_TEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// A stand-in `workerd`: argv-compatible, prints `script` on
    /// stderr, exits `code`.
    fn fake_workerd(dir: &Path, script: &str, code: i32) -> PathBuf {
        let bin = dir.join("workerd");
        std::fs::write(
            &bin,
            format!("#!/bin/sh\ncat <<'EOF' >&2\n{script}\nEOF\nexit {code}\n"),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        bin
    }

    #[test]
    fn preflight_passes_on_sentinel_only_stderr() {
        let dir = tmpdir("pf-ok");
        let cfg = dir.join("config.capnp");
        std::fs::write(&cfg, b"(services = [])").unwrap();
        let bin = fake_workerd(
            &dir,
            "workerd/server/server.c++:7475: error: No tests found!\nTests failed!",
            1,
        );
        preflight_workerd(
            &bin,
            &cfg,
            &[],
            &[],
            std::iter::empty(),
            PREFLIGHT_TEST_TIMEOUT,
        )
        .expect("sentinel-only stderr = loadable");
    }

    #[test]
    fn preflight_rejects_load_errors() {
        let dir = tmpdir("pf-bad");
        let cfg = dir.join("config.capnp");
        std::fs::write(&cfg, b"(services = [])").unwrap();
        let bin = fake_workerd(
            &dir,
            "service w: Uncaught Error: No such module \"miniflare:zod\".\n  imported from \"workflows.mjs\"",
            1,
        );
        let err = preflight_workerd(
            &bin,
            &cfg,
            &[],
            &[],
            std::iter::empty(),
            PREFLIGHT_TEST_TIMEOUT,
        )
        .expect_err("load-error stderr must reject");
        assert_eq!(err.kind, vrt_config::check::CheckKind::Rejected, "{err:?}");
        assert!(
            err.detail.contains("miniflare:zod"),
            "tail carries the cause: {err:?}"
        );
    }

    #[test]
    fn preflight_rejects_unexplained_exit() {
        let dir = tmpdir("pf-empty");
        let cfg = dir.join("config.capnp");
        std::fs::write(&cfg, b"(services = [])").unwrap();
        let bin = fake_workerd(&dir, "", 1);
        preflight_workerd(
            &bin,
            &cfg,
            &[],
            &[],
            std::iter::empty(),
            PREFLIGHT_TEST_TIMEOUT,
        )
        .expect_err("silent non-zero exit must not pass as 'no tests'");
    }

    #[test]
    fn preflight_redacts_env_values_from_tail() {
        let dir = tmpdir("pf-secret");
        let cfg = dir.join("config.capnp");
        std::fs::write(&cfg, b"(services = [])").unwrap();
        let bin = fake_workerd(
            &dir,
            "service w: Uncaught Error: leaked hunter2-value in diagnostics",
            1,
        );
        let err = preflight_workerd(
            &bin,
            &cfg,
            &[],
            &[],
            std::iter::once("hunter2-value".to_string()),
            PREFLIGHT_TEST_TIMEOUT,
        )
        .expect_err("must reject");
        assert!(
            !err.detail.contains("hunter2-value"),
            "needle leaked: {err:?}"
        );
        assert!(
            err.detail.contains("***"),
            "redaction marker expected: {err:?}"
        );
    }

    /// MIG-9: await the off-actor preflight verdict and feed it back
    /// through `on_ev` — the async equivalent of the old synchronous
    /// `start_pending_launch` return. Bounded so a wedged check fails
    /// the test instead of hanging it.
    async fn drive_preflight(a: &mut Actor) {
        let ev = tokio::time::timeout(Duration::from_secs(30), a.ev_rx.recv())
            .await
            .expect("preflight verdict timed out")
            .expect("ev channel closed");
        a.on_ev(ev);
    }

    /// MIG-8: a config workerd refuses to load must NEVER reach the
    /// serialized DO drain — preflight runs before `active` is moved,
    /// the serving generation survives untouched, and the tenant takes
    /// the bounded switch-failure path (latching `SwitchFailed` here
    /// with `max_switch_failures = 1`). Regression for the
    /// 2026-11-17 incident: an unvalidated config drained the old
    /// generation first, blanking the whole tenant for ~30 s.
    #[tokio::test]
    async fn mig8_preflight_reject_never_drains_old_generation() {
        let dir = tmpdir("mig8-pf-reject");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "e5f6".repeat(16);
        let (mut actor, _wal) = launch_actor(
            Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unused",
            )))),
            &dir,
            &tenant_dir,
            &tomb_dir,
            &leased,
        );
        // Serving generation + an unproven digest → preflight must run.
        actor.active = Some(fake_live_gen(&dir, 60_000).await);
        actor.runtime.mark_served();
        actor.preflight_ok_digest = None;
        actor.max_switch_failures = 1;
        let spec_dir = actor.desired_spec.as_ref().unwrap().work_dir.clone();
        std::fs::write(spec_dir.join("config.capnp"), b"(services = [])").unwrap();
        actor.desired_spec.as_mut().unwrap().binary =
            fake_workerd(&dir, "service w: Uncaught Error: top-level boom", 1);
        actor.start_pending_launch(false);
        // MIG-9: the verdict now lands asynchronously via
        // `Ev::PreflightDone` — the call above only DISPATCHED the
        // check (actor not pinned); drive the verdict to see the
        // failure path.
        assert!(
            actor.active.is_some() && actor.draining.is_empty(),
            "dispatch alone must not drain"
        );
        drive_preflight(&mut actor).await;
        assert!(
            actor.active.is_some(),
            "rejected preflight must keep the old generation serving"
        );
        assert!(
            actor.draining.is_empty(),
            "rejected preflight must never drain"
        );
        assert_eq!(actor.runtime.state(), TenantState::SwitchFailed);
        let err = actor.runtime.last_error().unwrap_or_default();
        assert!(err.contains("config-load"), "typed reason: {err}");
        assert!(
            actor.preflight_ok_digest.is_none(),
            "a refused digest must never be cached as proven"
        );
    }

    /// MIG-8: a PASSING preflight reaches the serialized switch —
    /// the live DO generation moves to draining and the digest is
    /// cached as proven (so a retry of the same desired skips the
    /// subprocess entirely).
    #[tokio::test]
    async fn mig8_preflight_pass_then_serialized_drain() {
        let dir = tmpdir("mig8-pf-pass");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "e5f7".repeat(16);
        let (mut actor, _wal) = launch_actor(
            Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unused",
            )))),
            &dir,
            &tenant_dir,
            &tomb_dir,
            &leased,
        );
        actor.active = Some(fake_live_gen(&dir, 60_000).await);
        actor.runtime.mark_served();
        actor.preflight_ok_digest = None;
        let spec_dir = actor.desired_spec.as_ref().unwrap().work_dir.clone();
        std::fs::write(spec_dir.join("config.capnp"), b"(services = [])").unwrap();
        actor.desired_spec.as_mut().unwrap().binary = fake_workerd(
            &dir,
            "workerd/server/server.c++:7475: error: No tests found!\nTests failed!",
            1,
        );
        actor.start_pending_launch(false);
        assert!(
            actor.active.is_some(),
            "pre-verdict: old generation still serving"
        );
        drive_preflight(&mut actor).await;
        assert!(
            actor.active.is_none(),
            "passed preflight → old generation moves to draining"
        );
        assert_eq!(actor.draining.len(), 1, "serialized switch drains first");
        assert!(
            actor.do_wait_since.is_some(),
            "serialized switch waits for the drain"
        );
        assert_eq!(
            actor.preflight_ok_digest.as_deref(),
            Some("d"),
            "proven digest cached"
        );
    }

    /// MIG-8: a wedged `workerd test` must fail closed on the caller's
    /// bound — killed inside `preflight_timeout`, the old generation
    /// never drained, and the typed failure recorded. Covers the
    /// "preflight timeout" branch end-to-end (the kill/reap mechanics
    /// themselves are proven in vrt-config's check tests).
    #[tokio::test]
    async fn mig8_preflight_timeout_fails_closed_without_drain() {
        let dir = tmpdir("mig8-pf-timeout");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "e5f8".repeat(16);
        let (mut actor, _wal) = launch_actor(
            Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unused",
            )))),
            &dir,
            &tenant_dir,
            &tomb_dir,
            &leased,
        );
        actor.active = Some(fake_live_gen(&dir, 60_000).await);
        actor.runtime.mark_served();
        actor.preflight_ok_digest = None;
        actor.max_switch_failures = 1;
        actor.preflight_timeout = Duration::from_millis(300);
        let spec_dir = actor.desired_spec.as_ref().unwrap().work_dir.clone();
        std::fs::write(spec_dir.join("config.capnp"), b"(services = [])").unwrap();
        // A `workerd test` that never returns.
        let bin = dir.join("wedged-workerd");
        std::fs::write(&bin, "#!/bin/sh\nsleep 30\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        actor.desired_spec.as_mut().unwrap().binary = bin;
        let t0 = Instant::now();
        actor.start_pending_launch(false);
        assert!(
            actor.preflight.is_some(),
            "MIG-9: wedged check runs off-actor — dispatch returns instantly"
        );
        drive_preflight(&mut actor).await;
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "wedged child killed, not awaited to natural exit"
        );
        assert!(actor.active.is_some(), "timeout must not drain");
        assert!(actor.draining.is_empty());
        assert_eq!(actor.runtime.state(), TenantState::SwitchFailed);
        let err = actor.runtime.last_error().unwrap_or_default();
        assert!(err.contains("timeout"), "typed timeout reason: {err}");
    }

    /// MIG-9: the check must never pin the actor task — on a
    /// single-threaded runtime the OLD synchronous call would have
    /// frozen the ONLY worker thread for the whole timeout budget.
    /// Now `start_pending_launch` returns instantly and a peer task
    /// still completes.
    #[tokio::test(flavor = "current_thread")]
    async fn mig9_preflight_does_not_pin_the_actor_task() {
        let dir = tmpdir("mig9-pf-pin");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "e5fa".repeat(16);
        let (mut actor, _wal) = launch_actor(
            Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unused",
            )))),
            &dir,
            &tenant_dir,
            &tomb_dir,
            &leased,
        );
        actor.preflight_ok_digest = None;
        actor.preflight_timeout = Duration::from_secs(5);
        let spec_dir = actor.desired_spec.as_ref().unwrap().work_dir.clone();
        std::fs::write(spec_dir.join("config.capnp"), b"(services = [])").unwrap();
        let bin = dir.join("wedged-workerd");
        std::fs::write(&bin, "#!/bin/sh
sleep 10
").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        actor.desired_spec.as_mut().unwrap().binary = bin;
        let t0 = Instant::now();
        actor.start_pending_launch(false);
        assert!(
            t0.elapsed() < Duration::from_secs(1),
            "dispatch must return instantly on a 1-thread runtime: {:?}",
            t0.elapsed()
        );
        assert!(actor.preflight.is_some(), "preflight ctx registered");
        // A peer task on the same single-threaded runtime still runs —
        // the old synchronous check would have starved it for 10 s.
        let probe = tokio::spawn(async { 42 });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), probe)
                .await
                .unwrap()
                .unwrap(),
            42,
            "peer task starved — check is still on the actor thread"
        );
        // Cancel so the 10 s child dies now, not at deadline.
        actor.abort_pending();
        drive_preflight(&mut actor).await; // drains the Cancelled ev
    }

    /// MIG-9: stop mid-preflight — `abort_pending` flips the cancel
    /// flag, the check group-kills + reaps the child inside one tick,
    /// and the late `PreflightDone` verdict lands stale (ctx already
    /// taken) without reviving the launch. Proves both the prompt
    /// return and a dead grandchild pid.
    #[tokio::test]
    async fn mig9_stop_during_preflight_reaps_child_group() {
        let dir = tmpdir("mig9-pf-stop");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "e5fb".repeat(16);
        let (mut actor, _wal) = launch_actor(
            Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unused",
            )))),
            &dir,
            &tenant_dir,
            &tomb_dir,
            &leased,
        );
        actor.active = Some(fake_live_gen(&dir, 60_000).await);
        actor.runtime.mark_served();
        actor.preflight_ok_digest = None;
        actor.preflight_timeout = Duration::from_secs(30);
        let spec_dir = actor.desired_spec.as_ref().unwrap().work_dir.clone();
        std::fs::write(spec_dir.join("config.capnp"), b"(services = [])").unwrap();
        // Same grandchild-holds-pipes shape as the check tests: the
        // group must die, not just the leader.
        let pidfile = dir.join("child.pid");
        let bin = dir.join("wedged-workerd");
        std::fs::write(
            &bin,
            format!(
                "#!/bin/sh
sleep 30 & echo $! > {}; wait
",
                pidfile.display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        actor.desired_spec.as_mut().unwrap().binary = bin;
        actor.start_pending_launch(false);
        assert!(actor.preflight.is_some());
        // Wait for the grandchild to actually spawn.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !pidfile.exists() {
            assert!(Instant::now() < deadline, "child never started");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let t0 = Instant::now();
        actor.begin_drain(false); // the Stop path
        let _ = tokio::time::timeout(Duration::from_secs(10), actor.ev_rx.recv())
            .await
            .expect("cancelled check must still report its verdict");
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "cancelled check must return promptly, not at the 30 s deadline"
        );
        // The late verdict is stale — ctx already taken by abort_pending.
        assert!(actor.preflight.is_none());
        let pid: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let gone = {
            let mut dead = false;
            for _ in 0..50 {
                let up = std::process::Command::new("kill")
                    .args(["-0", &pid.to_string()])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false);
                if !up {
                    dead = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            dead
        };
        assert!(gone, "cancelled preflight child pid {pid} must be dead");
        // The stale verdict must not have revived a launch.
        assert!(actor.pending.is_none());
    }

    /// MIG-9: two tenants' slow preflights run CONCURRENTLY on the
    /// blocking pool — both dispatches return before either verdict,
    /// and both verdicts land inside one deadline (serialized would
    /// take 2×).
    #[tokio::test]
    async fn mig9_concurrent_preflights_run_in_parallel() {
        let mk = |tag: &str| {
            let dir = tmpdir(tag);
            let tenant_dir = dir.join("do-t");
            let tomb_dir = dir.join("wf-del");
            let leased = "e5fc".repeat(16);
            let (mut actor, wal) = launch_actor(
                Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "unused",
                )))),
                &dir,
                &tenant_dir,
                &tomb_dir,
                &leased,
            );
            actor.active = None;
            actor.runtime.mark_served();
            actor.preflight_ok_digest = None;
            actor.max_switch_failures = 1;
            actor.preflight_timeout = Duration::from_secs(2);
            let spec_dir = actor.desired_spec.as_ref().unwrap().work_dir.clone();
            std::fs::write(spec_dir.join("config.capnp"), b"(services = [])").unwrap();
            let bin = dir.join("wedged-workerd");
            std::fs::write(&bin, "#!/bin/sh
sleep 10
").unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
            actor.desired_spec.as_mut().unwrap().binary = bin;
            (actor, wal)
        };
        let (mut a, _wa) = mk("mig9-pf-conc-a");
        let (mut b, _wb) = mk("mig9-pf-conc-b");
        let t0 = Instant::now();
        a.start_pending_launch(false);
        b.start_pending_launch(false);
        assert!(a.preflight.is_some() && b.preflight.is_some());
        drive_preflight(&mut a).await;
        drive_preflight(&mut b).await;
        assert!(
            t0.elapsed() < Duration::from_secs(3),
            "2 s + 2 s checks must overlap on the blocking pool: {:?}",
            t0.elapsed()
        );
        let ea = a.runtime.last_error().unwrap_or_default();
        let eb = b.runtime.last_error().unwrap_or_default();
        assert!(ea.contains("timeout") && eb.contains("timeout"), "{ea} / {eb}");
    }

    #[tokio::test]
    async fn switch_gate_try_hold_cas_never_exceeds_caps() {
        // W4-r3 P0-3: 64 racing holds against a 32-slot ceiling —
        // fetch_update CAS must cap successes at exactly 32 (a
        // load-then-add pair would overshoot).
        let gate = std::sync::Arc::new(SwitchGate::default());
        let mut joins = Vec::new();
        for _ in 0..64 {
            let g = gate.clone();
            joins.push(tokio::spawn(
                async move { g.try_hold(1 << 20, 32, 32 << 20) },
            ));
        }
        let mut ok = 0usize;
        for j in joins {
            ok += j.await.unwrap() as usize;
        }
        assert_eq!(ok, 32, "req ceiling overshot under concurrency");
        assert_eq!(gate.held(), 32);
        // Byte ceiling CAS: 33rd req only fits until bytes run out —
        // bytes=1MiB against a 2MiB cap with 3 racers → exactly 2.
        let gate = SwitchGate::default();
        assert!(gate.try_hold(1 << 20, 8, 2 << 20));
        assert!(gate.try_hold(1 << 20, 8, 2 << 20));
        assert!(!gate.try_hold(1, 8, 2 << 20), "byte ceiling overshot");
        assert_eq!(gate.held(), 2);
        // Releasing one MiB admits a small hold again — but not one
        // that would push bytes back over the cap.
        gate.unhold(1 << 20);
        assert!(gate.try_hold(1, 8, 2 << 20));
        assert!(!gate.try_hold(1 << 20, 8, 2 << 20));
    }

    #[tokio::test]
    async fn switch_gate_clear_reaches_waiters_registered_before_end() {
        // W4-r3 P0-2: `notify_waiters` stores no permit — a waiter
        // must `enable()` its Notified BEFORE checking `active()`.
        // `end()` landing between registration and await MUST still
        // wake it (this is the lost-wake window).
        let gate = SwitchGate::default();
        gate.begin();
        let n = gate.cleared();
        tokio::pin!(n);
        n.as_mut().enable();
        gate.end(); // end AFTER registration but BEFORE the await
        tokio::time::timeout(std::time::Duration::from_millis(200), n)
            .await
            .expect("registered waiter missed the clear");
        // `end()` before the check: `active()` reads false — the
        // caller skips the wait entirely (no stale registration).
        gate.begin();
        gate.end();
        let n2 = gate.cleared();
        tokio::pin!(n2);
        n2.as_mut().enable();
        assert!(!gate.active(), "ended gate must read inactive");
    }

    // ---- MIG-11: same-tenant switch window ----

    /// A spec carrying DO storage → the serialized switch branch.
    fn do_spec(dir: &Path) -> TenantLaunchSpec {
        let mut spec = test_spec(dir);
        spec.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: dir.join("do-t"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        spec
    }

    fn busy_slot(runtime: &TenantRuntime) -> Arc<EndpointSlot> {
        Arc::new(EndpointSlot {
            endpoint: crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            drain_notify: runtime.drain_notify.clone(),
        })
    }

    /// MIG-11: an IDLE old generation must be SIGTERMed inside
    /// `enter_draining` itself — never deferred to a monitor tick or a
    /// drain_notify that cannot fire (in-flight is already 0). pid is
    /// DEAD_PID: kill(-pgid) → ESRCH, the path is exercised but nothing
    /// real is signalled.
    #[tokio::test]
    async fn enter_draining_sigterms_idle_generation_in_same_turn() {
        let dir = tmpdir("mig11-idle");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        let now = Instant::now();
        actor.enter_draining(gen, now, true);
        let g = &actor.draining[0];
        assert!(
            g.sigterm_at.is_some(),
            "idle gen must be SIGTERMed inside enter_draining — not on a monitor tick"
        );
        assert!(g.do_switch, "serialized-switch flag carried");
        assert!(
            g.sigterm_at.unwrap().duration_since(g.entered_at) < Duration::from_secs(1),
            "drain→sigterm must be sub-second"
        );
    }

    /// MIG-11: a BUSY old generation still waits for in-flight → 0
    /// before SIGTERM (drain semantics unchanged).
    #[tokio::test]
    async fn enter_draining_busy_generation_waits_for_idle() {
        let dir = tmpdir("mig11-busy-wait");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        let slot = busy_slot(&actor.runtime);
        let guard = slot.acquire().unwrap();
        gen.slots = Arc::new(BTreeMap::from([("w1".to_string(), slot.clone())]));
        actor.enter_draining(gen, Instant::now(), false);
        assert!(
            actor.draining[0].sigterm_at.is_none(),
            "busy gen must NOT be SIGTERMed while in-flight"
        );
        // In-flight 1→0 — the actor's drain_notify wake services the
        // queue; the next service turn signals.
        drop(guard);
        actor.service_draining(Instant::now());
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "idle-again gen gets SIGTERM on service"
        );
    }

    /// MIG-11: deadline breach on a still-busy draining generation
    /// escalates to SIGKILL (unchanged drain semantics).
    #[tokio::test]
    async fn draining_deadline_still_escalates_to_sigkill() {
        let dir = tmpdir("mig11-deadline");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        let slot = busy_slot(&actor.runtime);
        let _guard = slot.acquire().unwrap();
        gen.slots = Arc::new(BTreeMap::from([("w1".to_string(), slot)]));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        assert!(actor.draining[0].sigterm_at.is_none());
        actor.service_draining(now + actor.drain_timeout + Duration::from_secs(1));
        assert!(actor.draining[0].killed, "still-busy at deadline → SIGKILL");
    }

    /// MIG-11: `next_wake` must not leave an idle unsignalled draining
    /// generation asleep until the monitor tick — the backstop wake is
    /// immediate even if a future path skips `enter_draining`'s service.
    #[tokio::test]
    async fn next_wake_immediate_for_idle_unsignalled_draining() {
        let dir = tmpdir("mig11-wake");
        let mut actor = test_actor(SupervisorDeps::default());
        // test_actor starts with next_monitor = now; push it out so the
        // assertion measures the draining wake alone.
        actor.next_monitor = Instant::now() + Duration::from_secs(3600);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        // Bypass enter_draining on purpose: push only, no service —
        // models any path that reaches the queue late.
        actor.push_draining(gen, Instant::now(), false);
        let wake = actor.next_wake();
        assert!(
            wake.duration_since(Instant::now()) < Duration::from_secs(1),
            "idle draining gen must wake the actor now, not at the 5s monitor"
        );
    }

    /// Regression: a deadline-SIGKILLed gen has `killed == true` but
    /// `sigterm_at == None`. service_draining skips killed gens, so the
    /// immediate-wake backstop must not fire for it (busy loop).
    #[tokio::test]
    async fn next_wake_not_hot_for_killed_unsignalled_draining() {
        let dir = tmpdir("mig11-wake-killed");
        let mut actor = test_actor(SupervisorDeps::default());
        // test_actor starts with next_monitor = now; push it out so the
        // assertion measures the draining wake alone.
        actor.next_monitor = Instant::now() + Duration::from_secs(3600);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        actor.push_draining(gen, Instant::now(), false);
        for d in actor.draining.iter_mut() {
            d.killed = true;
            assert!(d.sigterm_at.is_none());
            // deadline-SIGKILL happens only once the deadline passed
            d.deadline = Instant::now() - Duration::from_secs(1);
        }
        let wake = actor.next_wake();
        assert!(
            wake.duration_since(Instant::now()) >= Duration::from_secs(1),
            "killed draining gen must not cause an immediate wake"
        );
    }

    /// MIG-11: the serialized DO switch path drains through
    /// `enter_draining` — idle old gen SIGTERMed in the same actor turn
    /// — and holds the gate BEFORE the routing maps empty, so a lookup
    /// miss while the gate is active is always "switching", never
    /// "absent" (the invariant the control-plane SWITCHING mapping
    /// relies on).
    #[tokio::test]
    async fn serialized_switch_sigterms_idle_gen_and_gates_routing() {
        let dir = tmpdir("mig11-serial");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        actor.active = Some(gen);
        actor.runtime.test_install(BTreeMap::from([(
            "w1".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        )]));
        let spec = Box::new(do_spec(&dir));
        actor.launch_after_preflight(spec.clone(), &spec);
        assert!(
            actor.runtime.switch_gate.active(),
            "serialized switch must hold the gate"
        );
        assert!(
            actor.runtime.endpoint_slot("w1").is_none(),
            "gate active ⇒ routing empty (SWITCHING invariant)"
        );
        let g = &actor.draining[0];
        assert!(g.do_switch);
        assert!(
            g.sigterm_at.is_some(),
            "idle old gen SIGTERMed in the same turn — not on a monitor tick"
        );
        assert!(actor.do_wait_since.is_some(), "switch wait armed");
    }

    /// MIG-11: a failed serialized-switch launch MUST release the
    /// gate — held waiters resolve into the not-ready surface, never
    /// hang for their full budget.
    #[tokio::test]
    async fn serialized_switch_launch_failure_releases_gate() {
        let dir = tmpdir("mig11-fail");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        actor.active = Some(gen);
        actor.runtime.test_install(BTreeMap::from([(
            "w1".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        )]));
        let spec = Box::new(do_spec(&dir));
        actor.launch_after_preflight(spec.clone(), &spec);
        assert!(actor.runtime.switch_gate.active());
        actor.on_launch_failed(SpawnFailure {
            reason: "ready-timeout",
            detail: None,
        });
        assert!(
            !actor.runtime.switch_gate.active(),
            "launch failure must release the switch gate"
        );
    }

    /// MIG-11: bounded wait semantics — with an empty map + active
    /// gate the waiter resolves once the gate clears with routes
    /// installed (install lands routes BEFORE ending the gate).
    #[tokio::test]
    async fn resolve_through_switch_waits_and_resolves_after_gate() {
        let rt = TenantRuntime::new("t");
        rt.switch_gate.begin();
        let rt2 = rt.clone();
        let waiter = tokio::spawn(async move {
            rt2.resolve_through_switch(0, Duration::from_secs(5), 8, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        rt.test_install(BTreeMap::from([(
            "w1".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        )]));
        rt.switch_gate.end();
        let res = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter hung past gate clear")
            .unwrap();
        assert!(
            matches!(res, Ok(SwitchResolve::Hit(_))),
            "resolved to the new slot"
        );
        assert_eq!(rt.switch_gate.held(), 0, "permit released on resolve");
    }

    /// MIG-11: waiters are bounded by `max_reqs` — an excess resolves
    /// `Capacity` immediately (bounded task accumulation, never an
    /// unbounded queue of held futures).
    #[tokio::test]
    async fn resolve_through_switch_caps_waiters() {
        let rt = TenantRuntime::new("t");
        rt.switch_gate.begin();
        let rt2 = rt.clone();
        let held = tokio::spawn(async move {
            rt2.resolve_through_switch(0, Duration::from_secs(5), 1, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let res = rt
            .resolve_through_switch(0, Duration::from_secs(5), 1, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await;
        assert!(
            matches!(res, Err(SwitchHoldReject::Capacity)),
            "second waiter exceeds the 1-permit ceiling"
        );
        rt.switch_gate.end();
        // Gate cleared + still no route AND nothing serving →
        // `NoServing`: the waiter's hold was released and the miss is
        // a retryable not-ready, never a 404.
        let res = held.await.unwrap();
        assert!(matches!(res, Ok(SwitchResolve::NoServing)));
        assert_eq!(rt.switch_gate.held(), 0);
    }

    /// MIG-11: budget expiry resolves `Timeout` — never a hang; and a
    /// miss with the gate INACTIVE returns `Ok(None)` immediately (the
    /// genuine-absence → 404 path).
    #[tokio::test]
    async fn resolve_through_switch_timeout_and_inactive_miss() {
        let rt = TenantRuntime::new("t");
        rt.switch_gate.begin();
        let res = rt
            .resolve_through_switch(0, Duration::from_millis(30), 1, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await;
        assert!(matches!(res, Err(SwitchHoldReject::Timeout)));
        rt.switch_gate.end();
        let res = rt
            .resolve_through_switch(0, Duration::from_secs(5), 1, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await;
        assert!(
            matches!(res, Ok(SwitchResolve::NoServing)),
            "inactive gate + miss + nothing serving → NoServing"
        );
    }

    /// MIG-11-r2: the typed split at the abstraction boundary —
    /// `serving` separates a genuine "never declared" 404
    /// (`NotDeclared`) from a retryable "no generation serving"
    /// (`NoServing`). Before the split both read as `Ok(None)` and
    /// the entry points answered 404 — silently losing cron minutes
    /// on a crashed/launch-failed tenant.
    #[tokio::test]
    async fn resolve_through_switch_no_serving_vs_not_declared() {
        let rt = TenantRuntime::new("t");
        // Deployed tenant, nothing ever installed → NoServing.
        let res = rt
            .resolve_through_switch(0, Duration::from_secs(1), 8, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await;
        assert!(matches!(res, Ok(SwitchResolve::NoServing)));
        // A serving generation that never declared `w2` → NotDeclared.
        rt.test_install(BTreeMap::from([(
            "w1".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        )]));
        let res = rt
            .resolve_through_switch(0, Duration::from_secs(1), 8, 1 << 20, |rt| {
                rt.endpoint_slot("w2")
            })
            .await;
        assert!(matches!(res, Ok(SwitchResolve::NotDeclared)));
        // Declared worker still hits.
        let res = rt
            .resolve_through_switch(0, Duration::from_secs(1), 8, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await;
        assert!(matches!(res, Ok(SwitchResolve::Hit(_))));
        // And after the serving generation is gone (crash / failed
        // switch) the SAME miss flips back to NoServing — the
        // launch-failure regression this enum exists to catch.
        rt.test_uninstall();
        let res = rt
            .resolve_through_switch(0, Duration::from_secs(1), 8, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await;
        assert!(matches!(res, Ok(SwitchResolve::NoServing)));
    }

    /// MIG-11-r2: a resolved future dropped mid-wait (client
    /// disconnect / task abort) must release its hold slot — the
    /// `HoldPermit` Drop is the only unhold path and it is
    /// cancellation-safe by construction.
    #[tokio::test]
    async fn resolve_through_switch_cancel_releases_hold() {
        let rt = TenantRuntime::new("t");
        rt.switch_gate.begin();
        let rt2 = rt.clone();
        let waiter = tokio::spawn(async move {
            rt2.resolve_through_switch(0, Duration::from_secs(60), 8, 1 << 20, |rt| {
                rt.endpoint_slot("w1")
            })
            .await
        });
        for _ in 0..50 {
            if rt.switch_gate.held() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(rt.switch_gate.held(), 1, "waiter took a hold slot");
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            rt.switch_gate.held(),
            0,
            "dropped future must release its hold"
        );
        rt.switch_gate.end();
    }

    /// MIG-11-r2: `Actor::drop` ends the switch gate and clears
    /// `serving` — an abort past the shutdown deadline cannot leave a
    /// permanently-active gate on a runtime the registry may still
    /// hand out.
    #[tokio::test]
    async fn actor_drop_ends_switch_gate_and_clears_serving() {
        let dir = tmpdir("mig11-drop");
        let actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_install(BTreeMap::from([(
            "w1".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        )]));
        actor.runtime.switch_gate.begin();
        let rt = actor.runtime.clone();
        assert!(rt.switch_gate.active() && rt.serving());
        drop(actor);
        let _ = dir;
        assert!(
            !rt.switch_gate.active(),
            "dropped actor must end the switch gate"
        );
        assert!(!rt.serving(), "dropped actor must clear the serving marker");
    }

    /// MIG-11-r2: `on_launch_failed` writes `serving=false` ONLY when
    /// no generation survives — a failed serialized switch (active
    /// taken at drain) reports no-serving; a failed normal switch
    /// keeps the old generation's marker so misses still split
    /// `NotDeclared` correctly.
    #[tokio::test]
    async fn launch_failed_marks_no_serving_only_without_active() {
        let dir = tmpdir("mig11-noserving");
        let mut actor = test_actor(SupervisorDeps::default());
        // No generation at all (first-launch failure / post-drain
        // serialized switch): the marker drops.
        actor.on_launch_failed(SpawnFailure {
            reason: "ready-timeout",
            detail: None,
        });
        assert!(
            !actor.runtime.serving(),
            "launch failure with no active generation clears serving"
        );
        // A serving generation + failed NORMAL switch: marker stays.
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        actor.active = Some(gen);
        actor.runtime.test_install(BTreeMap::from([(
            "w1".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        )]));
        actor.on_launch_failed(SpawnFailure {
            reason: "config-load",
            detail: None,
        });
        assert!(
            actor.runtime.serving(),
            "failed switch keeps the surviving generation's marker"
        );
    }

    // ---- W6-r3 P1: tombstone reclaim waits for lease + restore ----

    /// Authoritative placement that always refuses `acquire` — models
    /// "another cell owns this class". Reclaim must never be reached:
    /// nothing unlinked, tombstones survive, `retire_db` never fires.
    struct FailAcquire;
    impl DoPlacement for FailAcquire {
        fn authoritative(&self) -> bool {
            true
        }
        fn acquire(
            &self,
            _s: &crate::do_placement::PlacementScope,
        ) -> Result<crate::do_placement::PlacementLease, PlacementError> {
            Err(PlacementError::Reserved { until_ms: u64::MAX })
        }
        fn fence(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            Ok(())
        }
    }

    /// Stage an emptied actor db (`_cf_KV` present, no
    /// `INSTANCE_METADATA` → the reclaim empty-gate passes) plus its
    /// tombstone under `tomb_dir/t/<classKey>/`.
    fn stage_tombstoned(
        tenant_dir: &Path,
        tomb_dir: &Path,
        class_key: &str,
        hex: &str,
    ) -> (PathBuf, PathBuf) {
        let class_dir = tenant_dir.join("w").join(class_key);
        std::fs::create_dir_all(&class_dir).unwrap();
        let db = class_dir.join(format!("{hex}.sqlite"));
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute(
            "CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID",
            [],
        )
        .unwrap();
        drop(conn);
        let tdir = tomb_dir.join("t").join(class_key);
        std::fs::create_dir_all(&tdir).unwrap();
        let tomb = tdir.join(format!("{hex}.json"));
        std::fs::write(
            &tomb,
            serde_json::json!({
                "tenant": "t",
                "class_key": class_key,
                "hex_id": hex,
                "class_dir": class_dir,
            })
            .to_string(),
        )
        .unwrap();
        (db, tomb)
    }

    /// `retire_db` calls scoped to THIS test's class key — the
    /// global registry is shared across parallel tests, so each test
    /// asserts on ITS OWN unique label (F1: two tests sharing one
    /// class key pollute each other's delta).
    fn retired_total(ck: &str) -> f64 {
        crate::metrics::registry()
            .render()
            .lines()
            .filter(|l| {
                l.starts_with("vrt_host_do_wal_retired_total")
                    && l.contains(&format!("class=\"{ck}\""))
            })
            .map(|l| l.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
            .sum()
    }

    /// Launch-path ordering: acquire → (restore pass) → wal.start →
    /// RECLAIM → spawn. `start_pending_launch` is driven on a real
    /// Actor with a DO spec; the binary is `/nonexistent`, so spawn
    /// fails AFTER the assertions' window — the filesystem state is
    /// the evidence.
    fn launch_actor(
        placement: Arc<dyn DoPlacement>,
        dir: &Path,
        tenant_dir: &Path,
        tomb_dir: &Path,
        leased_ck: &str,
    ) -> (Actor, Arc<crate::do_wal::DoWalReplicator>) {
        let store: Arc<dyn vrt_objstore::ObjectStore> = Arc::new(vrt_objstore::MemoryStore::new());
        let wal = Arc::new(crate::do_wal::DoWalReplicator::new(
            crate::do_wal::WalCfg {
                enabled: true,
                tick_ms: 0,
                max_dbs: 8,
                max_pending_bytes: 1 << 20,
                max_segment_bytes: 1 << 20,
                upload_attempts: 1,
                tick_upload_budget_ms: 500,
                idle_stop_ms: u64::MAX,
                flush_max_ms: 1_000,
                gc_max_per_tick: 64,
                replay_max_bytes: 1 << 30,
            rebaseline_chain_bytes: 1 << 29,
                baseline_backoff_max_ms: 300_000,
            },
            store.clone(),
            dir.join("wal-tmp"),
            Arc::new(|| 0),
        ));
        let deps = SupervisorDeps {
            placement,
            wf_tomb_dir: tomb_dir.to_path_buf(),
            do_snap: Some(Arc::new(DoSnapCtx {
                store,
                placement: Arc::new(crate::do_placement_store::ManifestDoPlacement::new(
                    Arc::new(vrt_objstore::MemoryStore::new()),
                    "cell-test",
                    60_000,
                    30_000,
                )),
                tmp_root: dir.join("snap-tmp"),
                interval_ms: 60_000,
                jitter_ms: 0,
                departing_max_ms: 60_000,
                xfer_quarantine_ms: 3_600_000,
                departing_retry_base_ms: 1_000,
                departing_retry_cap_ms: 60_000,
                wal: Some(wal.clone()),
            })),
            ..Default::default()
        };
        let mut actor = test_actor(deps);
        let mut spec = *actor.desired_spec.take().unwrap();
        spec.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: tenant_dir.to_path_buf(),
            worker_classes: [("w".to_string(), vec![leased_ck.to_string()])]
                .into_iter()
                .collect(),
            transfer_sources: Default::default(),
        });
        // Skip the `workerd test` subprocess — the digest is marked
        // proven so the launch reaches the DO block deterministically.
        actor.preflight_ok_digest = Some(spec.config_digest.clone());
        actor.desired_spec = Some(Box::new(spec));
        (actor, wal)
    }

    /// Authoritative acquire failure → launch aborts BEFORE reclaim:
    /// the deleted instance's file AND tombstone survive (they belong
    /// to a class another cell owns — unlinking would be cross-cell
    /// destruction), and `retire_db` never fires.
    #[tokio::test]
    async fn w6r3_reclaim_skipped_when_lease_refused() {
        let dir = tmpdir("w6r3-lease-fail");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        // F1: unique class key per test — the metric is global and a
        // sibling test retires under ITS key concurrently.
        let leased = "f1a1".repeat(16);
        let (db, tomb) = stage_tombstoned(&tenant_dir, &tomb_dir, &leased, &"c".repeat(64));
        let pre = retired_total(&leased);
        let (mut actor, _wal) =
            launch_actor(Arc::new(FailAcquire), &dir, &tenant_dir, &tomb_dir, &leased);
        actor.start_pending_launch(false);
        assert!(db.exists(), "unleased class file must survive");
        assert!(tomb.exists(), "unleased class tombstone must survive");
        assert_eq!(
            retired_total(&leased) - pre,
            0.0,
            "retire_db must not fire for unleased classes"
        );
        assert!(actor.pending_do_leases.is_empty());
        assert_eq!(
            actor.runtime.last_error().as_deref(),
            Some("do-placement-unavailable")
        );
    }

    /// Lease held (authoritative stub acquires) → the launch's
    /// reclaim pass unlinks the emptied instance of the LEASED class
    /// and counts its `retire_db` — while an equal tombstone on a
    /// class the spec does not declare stays fully untouched.
    #[tokio::test]
    async fn w6r3_reclaim_after_lease_restore() {
        let dir = tmpdir("w6r3-lease-ok");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "f1b2".repeat(16); // F1: unique per test (global metric)
        let other = "f1c3".repeat(16); // tombstoned but NOT in the spec
        let (db, tomb) = stage_tombstoned(&tenant_dir, &tomb_dir, &leased, &"c".repeat(64));
        let (db2, tomb2) = stage_tombstoned(&tenant_dir, &tomb_dir, &other, &"d".repeat(64));
        let pre = retired_total(&leased);
        let (mut actor, _wal) = launch_actor(
            Arc::new(StubPlacement(PlacementError::Store(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unused",
            )))),
            &dir,
            &tenant_dir,
            &tomb_dir,
            &leased,
        );
        actor.start_pending_launch(false);
        assert!(!db.exists(), "leased emptied instance must be unlinked");
        assert!(!tomb.exists(), "leased tombstone consumed");
        assert!(
            db2.exists() && tomb2.exists(),
            "unleased class tombstone+file must stay untouched"
        );
        assert_eq!(
            retired_total(&leased) - pre,
            1.0,
            "exactly one retire for the reclaimed leased instance"
        );
    }

    // ---- RV-R24: departing-Final GC safety ----

    fn snap_deps(
        dir: &Path,
        store: Arc<dyn vrt_objstore::ObjectStore>,
        plc: Arc<crate::do_placement_store::ManifestDoPlacement>,
    ) -> SupervisorDeps {
        let mut deps = SupervisorDeps::default();
        deps.placement = plc.clone();
        deps.do_snap = Some(Arc::new(DoSnapCtx {
            store,
            placement: plc,
            tmp_root: dir.join("snap-tmp"),
            interval_ms: 3_600_000,
            jitter_ms: 0,
            departing_max_ms: 60_000,
            xfer_quarantine_ms: 3_600_000,
            departing_retry_base_ms: 10,
            departing_retry_cap_ms: 100,
            wal: None,
        }));
        deps
    }

    fn snap_outcome(outcome: &str) -> f64 {
        crate::metrics::registry()
            .render()
            .lines()
            .filter(|l| {
                l.starts_with("vrt_host_do_snapshots_total")
                    && l.contains(&format!("outcome=\"{outcome}\""))
            })
            .map(|l| l.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
            .sum()
    }

    /// A worker removed from the spec whose class still has a pending
    /// `Final` must keep its WHOLE storage dir — the source is the
    /// only remaining copy until the baseline lands. Covers both
    /// detection paths (in-memory pending entry; on-disk marker
    /// re-seed) and the post-commit reap.
    #[tokio::test]
    async fn sweep_keeps_removed_worker_dir_while_final_pending() {
        let dir = tmpdir("sweep-pending-final");
        let tenant_dir = dir.join("do-t");
        let key = "e7".repeat(32);
        let wdir = tenant_dir.join("w1");
        let class_dir = wdir.join(&key);
        std::fs::create_dir_all(&class_dir).unwrap();
        std::fs::write(class_dir.join("payload.bin"), b"sqlite-payload").unwrap();
        // A stale sibling class dir sharing the worker dir — the
        // pending class pins the whole dir (fail closed); it is
        // reclaimed together once nothing is pending.
        let decoy = wdir.join("f8".repeat(32));
        std::fs::create_dir_all(&decoy).unwrap();
        std::fs::write(decoy.join("stale.bin"), b"stale").unwrap();

        let store: Arc<dyn vrt_objstore::ObjectStore> =
            Arc::new(vrt_objstore::MemoryStore::new());
        let plc = Arc::new(crate::do_placement_store::ManifestDoPlacement::new(
            store.clone(),
            "cell-test",
            60_000,
            30_000,
        ));
        let mut actor = test_actor(snap_deps(&dir, store, plc.clone()));
        // Live gen has NO worker endpoints — w1 reads as "removed".
        actor.active = Some(fake_live_gen(&dir, u64::MAX / 2).await);
        actor.do_tenant_dir = Some(tenant_dir.clone());
        // RV-R53: the sweep's destructive path is tombstone-gated —
        // grant the worker tombstone so the pending-Final protection
        // (not the retention gate) is what each assertion exercises.
        actor
            .do_deletions
            .entry("w1".to_string())
            .or_default()
            .insert(String::new());

        let scope = crate::do_placement::PlacementScope {
            tenant: "t".into(),
            worker: "w1".into(),
            class_key: key.clone(),
            class_dir: class_dir.clone(),
        };
        let pair = ("w1".to_string(), key.clone());
        // Real held lease at epoch 1 — the retry path commits on it.
        let lease = plc.acquire(&scope).unwrap();
        assert_eq!(lease.epoch.0, 1);

        // (a) in-memory pending only — no marker yet (pre-marker-crash
        // window): the map alone must already protect the dir.
        actor.departing_snap_pending.insert(
            pair.clone(),
            DepartingPending {
                scope: scope.clone(),
                epoch: 1,
                first_failed_ms: wall_now_ms(),
                next_retry_at: Instant::now(),
                attempts: 0,
                exhausted: false,
                is_transfer: true,
                dir_missing: false,
            },
        );
        actor.held_departing.push(DoLeaseEntry {
            scope: scope.clone(),
            epoch: PlacementEpoch(1),
            expires_at_ms: u64::MAX / 2,
        });
        actor.maybe_sweep_do_dirs();
        assert!(
            class_dir.join("payload.bin").exists(),
            "RV-R24: pending-final class dir must survive worker removal"
        );
        assert!(decoy.join("stale.bin").exists(), "whole worker dir kept");
        assert!(actor.departing_snap_pending.contains_key(&pair));

        // (b) marker-file path: drop the in-memory entry, write the
        // marker — the sweep must re-seed the retry AND still keep
        // the dir (restart recovery contract).
        actor.departing_snap_pending.clear();
        let marker = wdir.join(format!("{key}{FINAL_PENDING_SUFFIX}"));
        std::fs::write(&marker, "1 transfer").unwrap();
        actor.maybe_sweep_do_dirs();
        assert!(
            actor.departing_snap_pending.contains_key(&pair),
            "marker on a removed worker must re-seed the pending retry"
        );
        assert!(class_dir.join("payload.bin").exists());
        assert!(marker.exists());

        // Final commits → pending clears → the NEXT sweep reaps the
        // whole worker dir (the delete lifecycle resumes) — but only
        // once a signed tombstone exists (RV-R53: undeploy ≠ removed
        // from spec). Without it the dir is retained.
        actor.service_departing_snapshots();
        assert!(!actor.departing_snap_pending.contains_key(&pair));
        assert!(!marker.exists(), "marker cleared on commit");
        assert!(actor.held_departing.is_empty(), "held lease released");
        let rec = plc.record(&scope).unwrap().unwrap();
        assert_eq!(
            rec.snapshot.as_ref().map(|s| s.kind),
            Some(crate::do_placement_store::SnapKind::Final)
        );
        assert!(rec.holder.is_empty(), "lease fenced after commit");
        actor.maybe_sweep_do_dirs();
        assert!(
            !wdir.exists(),
            "worker dir reaped once nothing is pending (tombstoned)"
        );
    }

    /// A pending `Final` whose source dir VANISHED is a data-loss
    /// event, not a completed lifecycle — the entry and the HELD
    /// lease stay (fail closed) so no acquirer can fence into a
    /// missing baseline; a restored dir resumes the retry path.
    #[tokio::test]
    async fn departing_final_missing_dir_holds_lease_fail_closed() {
        let dir = tmpdir("departing-dir-missing");
        let tenant_dir = dir.join("do-t");
        let key = "a9".repeat(32);
        let wdir = tenant_dir.join("w1");
        let class_dir = wdir.join(&key);
        // class_dir intentionally absent.

        let store: Arc<dyn vrt_objstore::ObjectStore> =
            Arc::new(vrt_objstore::MemoryStore::new());
        let plc = Arc::new(crate::do_placement_store::ManifestDoPlacement::new(
            store.clone(),
            "cell-test",
            60_000,
            30_000,
        ));
        let mut actor = test_actor(snap_deps(&dir, store, plc.clone()));
        actor.active = Some(fake_live_gen(&dir, u64::MAX / 2).await);
        actor.do_tenant_dir = Some(tenant_dir.clone());

        let scope = crate::do_placement::PlacementScope {
            tenant: "t".into(),
            worker: "w1".into(),
            class_key: key.clone(),
            class_dir: class_dir.clone(),
        };
        let pair = ("w1".to_string(), key.clone());
        plc.acquire(&scope).unwrap();
        actor.departing_snap_pending.insert(
            pair.clone(),
            DepartingPending {
                scope: scope.clone(),
                epoch: 1,
                first_failed_ms: wall_now_ms(),
                next_retry_at: Instant::now(),
                attempts: 0,
                exhausted: false,
                is_transfer: true,
                dir_missing: false,
            },
        );
        actor.held_departing.push(DoLeaseEntry {
            scope: scope.clone(),
            epoch: PlacementEpoch(1),
            expires_at_ms: u64::MAX / 2,
        });
        let missing_pre = snap_outcome("final-dir-missing");

        actor.service_departing_snapshots();
        // Fail closed: NOTHING released — pending kept, lease kept.
        assert!(actor.departing_snap_pending.contains_key(&pair));
        assert!(actor.departing_snap_pending[&pair].dir_missing);
        assert_eq!(actor.held_departing.len(), 1);
        let rec = plc.record(&scope).unwrap().unwrap();
        assert_eq!(
            rec.holder, "cell-test",
            "held lease must NOT be released on missing dir"
        );
        assert!(snap_outcome("final-dir-missing") > missing_pre);

        // Repeat pass: still held — the latch logs once and nothing
        // creeps toward release.
        actor.service_departing_snapshots();
        assert!(actor.departing_snap_pending.contains_key(&pair));
        assert_eq!(actor.held_departing.len(), 1);

        // Operator restores the dir → the normal retry resumes and
        // commits; pending and the held lease leave together.
        std::fs::create_dir_all(&class_dir).unwrap();
        std::fs::write(class_dir.join("payload.bin"), b"restored").unwrap();
        actor.service_departing_snapshots();
        assert!(!actor.departing_snap_pending.contains_key(&pair));
        assert!(actor.held_departing.is_empty());
        let rec = plc.record(&scope).unwrap().unwrap();
        assert_eq!(
            rec.snapshot.as_ref().map(|s| s.kind),
            Some(crate::do_placement_store::SnapKind::Final)
        );
    }

    // ---- RV-R53.1/4: tombstone-gated destruction ----

    /// Stub assembler — never actually called in these tests (they
    /// converge only `tenants: []` + deletions), but required by
    /// `converge`'s signature.
    struct NullAssembler;
    impl TenantAssembler for NullAssembler {
        fn assemble(
            &self,
            _tenant: &snapshot_protocol::compute_desired::TenantDesired,
            _revision: u64,
            _generation: u64,
        ) -> Result<TenantLaunchSpec, crate::assemble::AssembleError> {
            panic!("no tenants in these tests");
        }
        fn params_fingerprint(&self) -> Result<String, crate::assemble::AssembleError> {
            Ok("null".into())
        }
    }

    fn tombstone(
        tenant: &str,
        worker: Option<&str>,
        class_key: Option<&str>,
    ) -> snapshot_protocol::compute_desired::DeletionDesired {
        snapshot_protocol::compute_desired::DeletionDesired {
            tenant_id: tenant.into(),
            worker_id: worker.map(|s| s.into()),
            class_key: class_key.map(|s| s.into()),
            revision: 1,
        }
    }

    fn purge_host(dir: &Path) -> (SupervisorHost, Arc<vrt_objstore::MemoryStore>) {
        let store = Arc::new(vrt_objstore::MemoryStore::new());
        let plc = Arc::new(crate::do_placement_store::ManifestDoPlacement::new(
            store.clone(),
            "cell-test",
            60_000,
            30_000,
        ));
        let mut deps = SupervisorDeps::default();
        deps.do_storage_root = Some(dir.to_path_buf());
        deps.placement = plc.clone();
        deps.purge = Some(Arc::new(crate::do_purge::DoPurge::open(
            dir,
            store.clone(),
            plc,
        )));
        let host = SupervisorHost::new(
            TenantRegistry::new(),
            deps,
            SupervisorParams::default(),
        );
        (host, store)
    }

    /// Empty desired + no tombstones = "nothing serves here" — local
    /// DO dirs and object prefixes MUST be retained verbatim. This is
    /// the fail-closed contract behind capacity eviction, exclusion,
    /// poisoning and dependency-cycle removal.
    #[test]
    fn converge_empty_desired_retains_all_do_data() {
        let dir = tmpdir("r53-empty");
        let key = "ab".repeat(32);
        let class_dir = dir.join("t").join("w1").join(&key);
        std::fs::create_dir_all(&class_dir).unwrap();
        std::fs::write(class_dir.join("sqlite.db"), b"data").unwrap();
        let (mut host, store) = purge_host(&dir);
        vrt_objstore::block_on_store(store.put(&format!("do/{key}/manifest"), b"m")).unwrap();
        let desired = snapshot_protocol::compute_desired::ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 1,
            tenants: vec![],
            deletions: vec![],
        };
        let asm: Arc<dyn TenantAssembler> = Arc::new(NullAssembler);
        let outcome = host.converge(&desired, &asm);
        assert!(outcome.removed.is_empty());
        assert!(class_dir.join("sqlite.db").exists());
        assert_eq!(store.len(), 1, "no tombstone → objects retained");
    }

    /// A worker-scope tombstone for a tenant with no live actor
    /// destroys the worker dir AND queues the class prefix for
    /// object-store deletion — drained inside the same converge.
    #[test]
    fn converge_worker_tombstone_purges_local_and_objects() {
        let dir = tmpdir("r53-worker");
        let key = "cd".repeat(32);
        let keep = "ef".repeat(32);
        // w1 is the deleted worker; w2 is a different worker of the
        // same tenant — a worker tombstone must never reach across.
        let wdir = dir.join("t").join("w1");
        std::fs::create_dir_all(wdir.join(&key)).unwrap();
        std::fs::create_dir_all(dir.join("t").join("w2").join(&keep)).unwrap();
        std::fs::write(wdir.join(&key).join("sqlite.db"), b"data").unwrap();
        let (mut host, store) = purge_host(&dir);
        for k in [&key, &keep] {
            vrt_objstore::block_on_store(store.put(&format!("do/{k}/obj"), b"x")).unwrap();
        }
        let desired = snapshot_protocol::compute_desired::ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 1,
            tenants: vec![],
            deletions: vec![tombstone("t", Some("w1"), None)],
        };
        let asm: Arc<dyn TenantAssembler> = Arc::new(NullAssembler);
        host.converge(&desired, &asm);
        assert!(!wdir.exists(), "worker tombstone removes the worker dir");
        // The tombstoned class prefix is drained; the other worker's
        // (not-tombstoned) class is untouched.
        assert_eq!(store.len(), 1);
        assert!(dir.join("t").join("w2").join(&keep).exists());
        assert!(vrt_objstore::block_on_store(store.get(&format!("do/{keep}/obj")))
            .unwrap()
            .is_some());
    }

    /// A class-scope tombstone removes exactly that class dir + prefix;
    /// sibling classes and the worker shell stay.
    #[test]
    fn converge_class_tombstone_purges_only_that_class() {
        let dir = tmpdir("r53-class");
        let key = "12".repeat(32);
        let keep = "34".repeat(32);
        let wdir = dir.join("t").join("w1");
        std::fs::create_dir_all(wdir.join(&key)).unwrap();
        std::fs::create_dir_all(wdir.join(&keep)).unwrap();
        std::fs::write(wdir.join(&key).join("sqlite.db"), b"data").unwrap();
        // Lease + quarantine siblings naming the class are removed too.
        std::fs::write(wdir.join(format!("{key}.lease-epoch")), b"1").unwrap();
        let (mut host, store) = purge_host(&dir);
        for k in [&key, &keep] {
            vrt_objstore::block_on_store(store.put(&format!("do/{k}/obj"), b"x")).unwrap();
        }
        let desired = snapshot_protocol::compute_desired::ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 1,
            tenants: vec![],
            deletions: vec![tombstone("t", Some("w1"), Some(&key))],
        };
        let asm: Arc<dyn TenantAssembler> = Arc::new(NullAssembler);
        host.converge(&desired, &asm);
        assert!(!wdir.join(&key).exists(), "class dir removed");
        assert!(!wdir.join(format!("{key}.lease-epoch")).exists());
        assert!(wdir.join(&keep).exists(), "sibling class retained");
        assert_eq!(store.len(), 1);
    }

    /// A tenant-scope tombstone for an absent tenant removes the
    /// whole tenant dir + every class prefix it held.
    #[test]
    fn converge_tenant_tombstone_purges_whole_dir() {
        let dir = tmpdir("r53-tenant");
        let k1 = "aa".repeat(32);
        let k2 = "bb".repeat(32);
        std::fs::create_dir_all(dir.join("t").join("w1").join(&k1)).unwrap();
        std::fs::create_dir_all(dir.join("t").join("w2").join(&k2)).unwrap();
        let (mut host, store) = purge_host(&dir);
        for k in [&k1, &k2] {
            vrt_objstore::block_on_store(store.put(&format!("do/{k}/obj"), b"x")).unwrap();
        }
        let desired = snapshot_protocol::compute_desired::ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 1,
            tenants: vec![],
            deletions: vec![tombstone("t", None, None)],
        };
        let asm: Arc<dyn TenantAssembler> = Arc::new(NullAssembler);
        host.converge(&desired, &asm);
        assert!(!dir.join("t").exists(), "tenant dir removed");
        assert_eq!(store.len(), 0, "all class prefixes drained");
    }

    /// Actor sweep: worker gone from the serving spec is retained
    /// without a tombstone; with one, it is reaped. A dropped class on
    /// a live worker follows the same rule at class scope.
    #[tokio::test]
    async fn sweep_requires_tombstone_for_removed_worker_and_class() {
        let dir = tmpdir("r53-sweep");
        let tenant_dir = dir.join("do-t");
        let key = "56".repeat(32);
        let stale_key = "78".repeat(32);
        // w-gone: whole removed worker; w-live keeps one class, drops
        // the other.
        std::fs::create_dir_all(tenant_dir.join("w-gone").join(&key)).unwrap();
        std::fs::create_dir_all(tenant_dir.join("w-live").join(&key)).unwrap();
        std::fs::create_dir_all(tenant_dir.join("w-live").join(&stale_key)).unwrap();

        let mut actor = test_actor(SupervisorDeps::default());
        let mut live = fake_live_gen(&dir, u64::MAX / 2).await;
        live.spec.worker_endpoints.insert(
            "w-live".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        );
        live.spec.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: tenant_dir.clone(),
            worker_classes: std::collections::BTreeMap::from([(
                "w-live".to_string(),
                vec![key.clone()],
            )]),
            transfer_sources: Default::default(),
        });
        actor.active = Some(live);
        actor.do_tenant_dir = Some(tenant_dir.clone());

        // No tombstones: EVERYTHING is retained — this is the
        // eviction/exclusion/empty-desired failure mode.
        actor.maybe_sweep_do_dirs();
        assert!(tenant_dir.join("w-gone").join(&key).exists());
        assert!(tenant_dir.join("w-live").join(&stale_key).exists());

        // A class-scope tombstone hits only the named class — even
        // under a live worker; a worker-scope tombstone reaps the
        // removed worker.
        actor
            .do_deletions
            .entry("w-live".to_string())
            .or_default()
            .insert(stale_key.clone());
        actor.maybe_sweep_do_dirs();
        assert!(!tenant_dir.join("w-live").join(&stale_key).exists());
        assert!(tenant_dir.join("w-live").join(&key).exists());
        assert!(tenant_dir.join("w-gone").exists(), "still no worker tombstone");

        actor
            .do_deletions
            .entry("w-gone".to_string())
            .or_default()
            .insert(String::new());
        actor.maybe_sweep_do_dirs();
        assert!(!tenant_dir.join("w-gone").exists());
    }

    /// MIG-17c: `<uniqueKey>.wal-replay-pending` maps back to its
    /// class via `lease_entry_class_key` — a DECLARED class's marker
    /// is retained with the class (and never counts as a retained
    /// stale entry), while a TOMBSTONED class's marker is swept with
    /// it. Before the suffix joined `LEASE_FILE_SUFFIXES` the marker
    /// was an unnamed foreign entry: retained classes inflated
    /// `vrt_host_do_sweep_retained_total{kind="class"}` and removed
    /// classes orphaned their markers.
    #[tokio::test]
    async fn sweep_maps_replay_pending_marker_to_its_class() {
        let dir = tmpdir("17c-sweep-marker");
        let tenant_dir = dir.join("do-t");
        let keep_key = "56".repeat(32);
        let dead_key = "78".repeat(32);
        let w = tenant_dir.join("w-live");
        std::fs::create_dir_all(w.join(&keep_key)).unwrap();
        std::fs::create_dir_all(w.join(&dead_key)).unwrap();
        // Markers sit as sibling FILES next to the class dirs.
        std::fs::write(w.join(format!("{keep_key}.wal-replay-pending")), b"{}").unwrap();
        std::fs::write(w.join(format!("{dead_key}.wal-replay-pending")), b"{}").unwrap();
        // The registry is global across parallel tests — measure
        // per-SWEEP deltas instead of one before/after pair: our
        // fixture contributes EXACTLY 1 (the dead class dir; its
        // marker never counts) and concurrent tests can only ADD, so
        // the first delta==1 observation is provably unpolluted.
        let retained_class = || -> f64 {
            crate::metrics::registry()
                .render()
                .lines()
                .filter(|l| l.starts_with("vrt_host_do_sweep_retained_total") && l.contains("class"))
                .map(|l| l.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
                .sum()
        };

        let mut actor = test_actor(SupervisorDeps::default());
        let mut live = fake_live_gen(&dir, u64::MAX / 2).await;
        live.spec.worker_endpoints.insert(
            "w-live".to_string(),
            crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        );
        live.spec.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: tenant_dir.clone(),
            worker_classes: std::collections::BTreeMap::from([(
                "w-live".to_string(),
                vec![keep_key.clone()],
            )]),
            transfer_sources: Default::default(),
        });
        actor.active = Some(live);
        actor.do_tenant_dir = Some(tenant_dir.clone());

        // No tombstone for the undeclared class: dir AND its marker
        // are retained — and the marker must NOT inflate the
        // retained-class metric (it isn't a class).
        let mut measured = false;
        for _ in 0..50 {
            let before = retained_class();
            actor.maybe_sweep_do_dirs();
            if retained_class() - before == 1.0 {
                measured = true;
                break;
            }
        }
        assert!(
            measured,
            "exactly the dead class dir counts as a retained class — its marker does not"
        );
        assert!(w.join(&dead_key).exists());
        assert!(
            w.join(format!("{dead_key}.wal-replay-pending")).exists(),
            "untombstoned marker is retained with its class"
        );
        // The declared class's marker is retained silently too.
        assert!(w.join(format!("{keep_key}.wal-replay-pending")).exists());

        // Tombstone the undeclared class: dir and marker sweep away
        // together.
        actor
            .do_deletions
            .entry("w-live".to_string())
            .or_default()
            .insert(dead_key.clone());
        actor.maybe_sweep_do_dirs();
        assert!(!w.join(&dead_key).exists());
        assert!(
            !w.join(format!("{dead_key}.wal-replay-pending")).exists(),
            "tombstoned class's marker sweeps with it"
        );
        assert!(w.join(&keep_key).exists());
        assert!(w.join(format!("{keep_key}.wal-replay-pending")).exists());
    }

    /// `on_deletions` merges the tombstone set AND queues the object
    /// prefix into the durable ledger — the tombstone drives BOTH the
    /// local sweep and the object-store purge.
    #[test]
    fn on_deletions_merges_and_queues_purge() {
        let dir = tmpdir("r53-ondel");
        let key = "9a".repeat(32);
        let store = Arc::new(vrt_objstore::MemoryStore::new());
        let plc = Arc::new(crate::do_placement_store::ManifestDoPlacement::new(
            store.clone(),
            "cell-test",
            60_000,
            30_000,
        ));
        let mut deps = SupervisorDeps::default();
        deps.placement = plc.clone();
        deps.purge = Some(Arc::new(crate::do_purge::DoPurge::open(
            &dir.join("ledger"),
            store.clone(),
            plc,
        )));
        std::fs::create_dir_all(dir.join("ledger")).unwrap();
        let mut actor = test_actor(deps);
        actor.on_deletions(vec![super::DoDeletion {
            worker_id: "w1".into(),
            class_key: Some(key.clone()),
        }]);
        assert!(actor
            .do_deletions
            .get("w1")
            .is_some_and(|s| s.contains(&key)));
        assert_eq!(actor.deps.purge.as_ref().unwrap().pending(), 1);
    }
    // ---------- MIG-12: waitUntil drain ----------

    /// TCP aggregate-probe stub: one canned 200 reply per connection.
    async fn wu_tcp_stub(body: &'static str) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = s.write_all(resp.as_bytes()).await;
            }
        });
        addr
    }

    fn wu_spec(dir: &Path, ep: crate::endpoint::WorkerEndpoint) -> TenantLaunchSpec {
        let mut spec = test_spec(dir);
        spec.waituntil_endpoint = Some(ep);
        spec
    }

    /// Pull the next WuProbeResult the spawned probe delivers (bounded).
    async fn next_wu_ev(actor: &mut Actor) -> Ev {
        match tokio::time::timeout(Duration::from_secs(5), actor.ev_rx.recv())
            .await
            .expect("probe event must land")
            .expect("ev channel open")
        {
            ev @ Ev::WuProbeResult { .. } => ev,
            _ => panic!("expected WuProbeResult"),
        }
    }

    /// A draining generation whose probe reports pending>0 must NOT be
    /// SIGTERMed — the SIGTERM waits for a Pending(0) verdict or grace.
    #[tokio::test]
    async fn waituntil_pending_defers_sigterm_until_zero() {
        let dir = tmpdir("mig12-pending");
        let addr = wu_tcp_stub("{\"pending\":2,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        assert!(
            actor.draining[0].sigterm_at.is_none(),
            "unprobed waitUntil gen must not be SIGTERMed in the same turn"
        );
        let ev = next_wu_ev(&mut actor).await;
        actor.on_ev(ev);
        assert!(
            actor.draining[0].sigterm_at.is_none(),
            "pending>0 verdict must keep deferring SIGTERM"
        );
    }

    /// Pending(0) is a clean verdict — SIGTERM without waiting out grace.
    #[tokio::test]
    async fn waituntil_pending_zero_sigterms() {
        let dir = tmpdir("mig12-zero");
        let addr = wu_tcp_stub("{\"pending\":0,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        let ev = next_wu_ev(&mut actor).await;
        actor.on_ev(ev);
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "pending=0 verdict must SIGTERM immediately"
        );
    }

    /// Probe channel failure is a failed probe, never a drain stall:
    /// SIGTERM follows the Failed verdict and the drop is counted.
    #[tokio::test]
    async fn waituntil_probe_failure_sigterms_and_counts_drop() {
        let dir = tmpdir("mig12-probe-fail");
        // Nothing listens → connect refused → verdict Failed.
        let ep = crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap());
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-wu-fail".into();
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(&dir, ep));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        let ev = next_wu_ev(&mut actor).await;
        actor.on_ev(ev);
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "failed probe must SIGTERM, never stall the drain"
        );
        let rendered = crate::metrics::registry().render();
        assert!(
            rendered.contains("vrt_host_waituntil_dropped_total{reason=\"probe_failed\"}"),
            "probe_failed drop must be counted: {rendered}"
        );
    }

    /// Grace expiry while pending>0: SIGTERM + dropped metric with
    /// reason grace_expired (never a hang past wait_until_grace).
    #[tokio::test]
    async fn waituntil_grace_expiry_sigterms_and_counts_drop() {
        let dir = tmpdir("mig12-grace");
        let addr = wu_tcp_stub("{\"pending\":3,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-wu-grace".into();
        actor.wait_until_grace = Duration::from_millis(200);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        let ev = next_wu_ev(&mut actor).await;
        actor.on_ev(ev); // Pending(3) recorded
        assert!(actor.draining[0].sigterm_at.is_none());
        actor.service_draining(now + Duration::from_millis(500));
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "grace expiry must SIGTERM"
        );
        let rendered = crate::metrics::registry().render();
        assert!(
            rendered.contains("vrt_host_waituntil_dropped_total{reason=\"grace_expired\"}"),
            "grace_expired drop must be counted: {rendered}"
        );
    }

    /// An in-flight probe past probe_timeout resolves to Failed — the
    /// generation is SIGTERMed rather than parked on a dead channel.
    #[tokio::test]
    async fn waituntil_probe_timeout_is_failure_not_stall() {
        let dir = tmpdir("mig12-timeout");
        // Accept and never answer — the socket-level timeout bounds it.
        use tokio::io::AsyncReadExt;
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                let mut buf = [0u8; 256];
                let _ = s.read(&mut buf).await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        let mut actor = test_actor(SupervisorDeps::default());
        actor.probe_timeout = Duration::from_millis(80);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        assert!(actor.draining[0].sigterm_at.is_none());
        actor.service_draining(now + Duration::from_millis(200));
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "expired probe resolves Failed → SIGTERM, not a hang"
        );
    }

    /// drain_timeout is the TOTAL cap — a generation still waiting on
    /// waitUntil at the deadline is SIGKILLed, never allowed past it.
    #[tokio::test]
    async fn waituntil_drain_deadline_still_sigkills() {
        let dir = tmpdir("mig12-deadline");
        let ep = crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap());
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-wu-deadline".into();
        actor.wait_until_grace = Duration::from_secs(300); // grace > drain
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(&dir, ep));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        actor.service_draining(now + actor.drain_timeout + Duration::from_secs(1));
        let g = &actor.draining[0];
        assert!(g.killed, "still-waiting gen at drain deadline → SIGKILL");
        assert!(g.sigterm_at.is_none(), "deadline kill skips SIGTERM");
        let rendered = crate::metrics::registry().render();
        assert!(
            rendered.contains("vrt_host_waituntil_dropped_total{reason=\"drain_deadline\"}"),
            "drain_deadline drop must be counted: {rendered}"
        );
    }

    /// next_wake stays bounded while a generation waits on waitUntil:
    /// probe retry / probe timeout / grace — never the 5 s monitor tick.
    #[tokio::test]
    async fn next_wake_bounded_during_waituntil_wait() {
        let dir = tmpdir("mig12-wake");
        let addr = wu_tcp_stub("{\"pending\":1,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        actor.next_monitor = Instant::now() + Duration::from_secs(3600);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        let wake = actor.next_wake();
        assert!(
            wake <= now + actor.probe_timeout + Duration::from_millis(100),
            "waiting gen must wake at probe timeout at latest, got {wake:?}"
        );
    }

    /// wu_pending_probe: parse contract — pending counted, `failed`
    /// and malformed/closed channels all fail closed to None.
    #[tokio::test]
    async fn wu_pending_probe_parse_contract() {
        let addr = wu_tcp_stub("{\"pending\":4,\"failed\":0}").await;
        let ep = crate::endpoint::WorkerEndpoint::Tcp(addr);
        let n = tokio::task::spawn_blocking(move || {
            wu_pending_probe(&ep, Duration::from_secs(2))
        })
        .await
        .unwrap();
        assert_eq!(n, Some(4));

        // `failed` fan-out counts as a failed probe — never trusted.
        let addr = wu_tcp_stub("{\"pending\":4,\"failed\":1}").await;
        let ep = crate::endpoint::WorkerEndpoint::Tcp(addr);
        let n = tokio::task::spawn_blocking(move || {
            wu_pending_probe(&ep, Duration::from_secs(2))
        })
        .await
        .unwrap();
        assert_eq!(n, None);

        // Malformed body → None. Connection refused → None.
        let addr = wu_tcp_stub("not json").await;
        let ep = crate::endpoint::WorkerEndpoint::Tcp(addr);
        let n = tokio::task::spawn_blocking(move || {
            wu_pending_probe(&ep, Duration::from_secs(2))
        })
        .await
        .unwrap();
        assert_eq!(n, None);
        let ep = crate::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap());
        let n = tokio::task::spawn_blocking(move || {
            wu_pending_probe(&ep, Duration::from_millis(500))
        })
        .await
        .unwrap();
        assert_eq!(n, None);
    }

    /// Fake workerd on a unix socket: one connection, reads the request,
    /// writes `response`, then holds the connection open for `hold`
    /// (closes immediately when `hold` is zero).
    fn wu_unix_stub(
        name: &str,
        response: &'static str,
        hold: Duration,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        use std::io::{Read, Write};
        let dir = tempfile::Builder::new()
            .prefix("wu")
            .tempdir_in("/tmp")
            .unwrap();
        let path = dir.path().join(format!("{name}.sock"));
        let l = std::os::unix::net::UnixListener::bind(&path).unwrap();
        std::thread::spawn(move || {
            if let Ok((mut st, _)) = l.accept() {
                let mut req = [0u8; 1024];
                let _ = st.read(&mut req);
                let _ = st.write_all(response.as_bytes());
                let _ = st.flush();
                if !hold.is_zero() {
                    std::thread::sleep(hold);
                }
            }
        });
        (dir, path)
    }

    async fn wu_probe_unix(
        path: std::path::PathBuf,
        timeout: Duration,
    ) -> (Option<u64>, Duration) {
        let ep = crate::endpoint::WorkerEndpoint::Unix(path);
        let t0 = Instant::now();
        let n = tokio::task::spawn_blocking(move || wu_pending_probe(&ep, timeout))
            .await
            .unwrap();
        (n, t0.elapsed())
    }

    /// Real workerd keeps the connection open after a Content-Length
    /// response: the probe must return on the body, not the 2 s timeout.
    #[tokio::test]
    async fn wu_probe_content_length_returns_without_eof() {
        let (_d, path) = wu_unix_stub(
            "cl",
            "HTTP/1.1 200 OK\r\nContent-Length: 24\r\n\r\n{\"pending\":3,\"failed\":0}",
            Duration::from_secs(5),
        );
        let (n, took) = wu_probe_unix(path, Duration::from_secs(2)).await;
        assert_eq!(n, Some(3));
        assert!(took < Duration::from_millis(500), "took {took:?}");
    }

    /// Header name matching is case-insensitive.
    #[tokio::test]
    async fn wu_probe_content_length_case_insensitive() {
        let (_d, path) = wu_unix_stub(
            "cli",
            "HTTP/1.1 200 OK\r\ncontent-length: 24\r\n\r\n{\"pending\":0,\"failed\":0}",
            Duration::from_secs(5),
        );
        let (n, took) = wu_probe_unix(path, Duration::from_secs(2)).await;
        assert_eq!(n, Some(0));
        assert!(took < Duration::from_millis(500), "took {took:?}");
    }

    #[tokio::test]
    async fn wu_probe_oversized_content_length_fails_closed() {
        let (_d, path) = wu_unix_stub(
            "big",
            "HTTP/1.1 200 OK\r\nContent-Length: 99999\r\n\r\n{\"pending\":0}",
            Duration::from_secs(5),
        );
        let (n, took) = wu_probe_unix(path, Duration::from_secs(2)).await;
        assert_eq!(n, None);
        assert!(took < Duration::from_millis(500), "took {took:?}");
    }

    #[tokio::test]
    async fn wu_probe_chunked_fails_closed() {
        let (_d, path) = wu_unix_stub(
            "chk",
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n18\r\n{\"pending\":0,\"failed\":0}\r\n0\r\n\r\n",
            Duration::from_secs(5),
        );
        let (n, took) = wu_probe_unix(path, Duration::from_secs(2)).await;
        assert_eq!(n, None);
        assert!(took < Duration::from_millis(500), "took {took:?}");
    }

    #[tokio::test]
    async fn wu_probe_invalid_content_length_fails_closed() {
        let (_d, path) = wu_unix_stub(
            "bad",
            "HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\n{\"pending\":0}",
            Duration::from_secs(5),
        );
        let (n, took) = wu_probe_unix(path, Duration::from_secs(2)).await;
        assert_eq!(n, None);
        assert!(took < Duration::from_millis(500), "took {took:?}");
    }

    /// No Content-Length + peer closes: EOF still delimits (old behaviour).
    #[tokio::test]
    async fn wu_probe_no_content_length_reads_to_eof() {
        let (_d, path) = wu_unix_stub(
            "eof",
            "HTTP/1.1 200 OK\r\n\r\n{\"pending\":7,\"failed\":0}",
            Duration::ZERO,
        );
        let (n, _) = wu_probe_unix(path, Duration::from_secs(2)).await;
        assert_eq!(n, Some(7));
    }

    #[test]
    fn wu_response_progress_framing() {
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n";
        assert_eq!(
            wu_response_progress(b"HTTP/1.1 200 OK\r\nContent-Le"),
            WuProgress::NeedMore
        );
        assert_eq!(
            wu_response_progress(format!("{head}ab").as_bytes()),
            WuProgress::NeedMore
        );
        assert_eq!(
            wu_response_progress(format!("{head}abcdXX").as_bytes()),
            WuProgress::Complete(head.len() + 4)
        );
        assert_eq!(
            wu_response_progress(
                b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab"
            ),
            WuProgress::Reject
        );
    }

    /// No waitUntil endpoint on the spec (legacy launch) → the drain is
    /// byte-identical to before: idle → SIGTERM in the same turn.
    #[tokio::test]
    async fn waituntil_absent_endpoint_sigterms_immediately() {
        let dir = tmpdir("mig12-absent");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "gen without waituntil endpoint keeps the MIG-11 same-turn SIGTERM"
        );
    }

    // ---------- MIG-12-r2: do_switch-bounded waitUntil grace ----------

    /// A do_switch draining generation's waitUntil wait ends at the
    /// do-switch grace — NOT the global `wait_until_grace`: its drain
    /// sits inside the switch-hold window, so the SIGTERM lands at the
    /// shorter bound, the drop is `do_switch_grace_expired`, and the
    /// `waituntil_wait` stage records the idle→SIGTERM span.
    #[tokio::test]
    async fn waituntil_do_switch_grace_truncates_sigterm() {
        let dir = tmpdir("mig12r2-do-grace");
        let addr = wu_tcp_stub("{\"pending\":3,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-wu-do".into();
        actor.wait_until_grace = Duration::from_secs(30);
        actor.do_switch_wait_until_grace = Duration::from_millis(200);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, true);
        assert!(actor.draining[0].do_switch);
        assert!(actor.draining[0].sigterm_at.is_none());
        let ev = next_wu_ev(&mut actor).await;
        actor.on_ev(ev); // Pending(3) recorded
        assert!(
            actor.draining[0].sigterm_at.is_none(),
            "pending>0 inside the do-switch grace must still defer"
        );
        // Past the 200 ms do-switch bound but far inside the global
        // 30 s one — only the do-switch bound can explain a SIGTERM.
        actor.service_draining(now + Duration::from_millis(500));
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "do_switch gen must SIGTERM at the do-switch grace"
        );
        let rendered = crate::metrics::registry().render();
        assert!(
            rendered.contains(
                "vrt_host_waituntil_dropped_total{reason=\"do_switch_grace_expired\"}"
            ),
            "do_switch_grace_expired drop must be counted: {rendered}"
        );
        assert!(
            rendered.contains("stage=\"waituntil_wait\""),
            "waituntil_wait stage must be observed for do_switch gens: {rendered}"
        );
    }

    /// A NON-do_switch draining generation ignores the do-switch bound
    /// entirely — it still waits out the global `wait_until_grace`.
    #[tokio::test]
    async fn waituntil_non_do_switch_keeps_global_grace() {
        let dir = tmpdir("mig12r2-nondo");
        let addr = wu_tcp_stub("{\"pending\":1,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        actor.wait_until_grace = Duration::from_millis(600);
        actor.do_switch_wait_until_grace = Duration::from_millis(100);
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        let now = Instant::now();
        actor.enter_draining(gen, now, false);
        let ev = next_wu_ev(&mut actor).await;
        actor.on_ev(ev); // Pending(1)
        // Past the 100 ms do-switch bound — irrelevant here; inside
        // the 600 ms global grace, the gen must still be waiting.
        actor.service_draining(now + Duration::from_millis(300));
        assert!(
            actor.draining[0].sigterm_at.is_none(),
            "non-do_switch gen must wait out the global grace, not the do-switch one"
        );
        actor.service_draining(now + Duration::from_millis(700));
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "global grace expiry still SIGTERMs"
        );
    }

    /// `do_switch_wait_until_grace = 0` disables the wait for do_switch
    /// generations only — an idle one SIGTERMs in the same turn (the
    /// MIG-11 zero-added-latency path), probe never consulted.
    #[tokio::test]
    async fn waituntil_do_switch_grace_zero_sigterms_same_turn() {
        let dir = tmpdir("mig12r2-do-zero");
        let addr = wu_tcp_stub("{\"pending\":9,\"failed\":0}").await;
        let mut actor = test_actor(SupervisorDeps::default());
        actor.wait_until_grace = Duration::from_secs(30);
        actor.do_switch_wait_until_grace = Duration::ZERO;
        let mut gen = fake_live_gen(&dir, wall_now_ms() + 120_000).await;
        gen.pid = DEAD_PID;
        gen.spec = Box::new(wu_spec(
            &dir,
            crate::endpoint::WorkerEndpoint::Tcp(addr),
        ));
        actor.enter_draining(gen, Instant::now(), true);
        assert!(
            actor.draining[0].sigterm_at.is_some(),
            "do-switch grace = 0 → same-turn SIGTERM (no waitUntil wait)"
        );
        assert!(
            actor.draining[0].wu_probe.is_none(),
            "disabled wait must not spawn a probe"
        );
    }

    // ---- MIG-13: DO-reachability taint for early-flip ----

    fn taint_worker(
        worker_id: &str,
        bindings: Vec<snapshot_protocol::compute_desired::BindingDesired>,
        has_do: bool,
    ) -> snapshot_protocol::compute_desired::WorkerDesired {
        use snapshot_protocol::compute_desired::{
            DoClassDesired, RouteDesired, VersionDesired, WorkerDesired, WorkerQuotas,
        };
        WorkerDesired {
            worker_id: worker_id.into(),
            name: worker_id.into(),
            active_version: VersionDesired {
                version_id: "v1".into(),
                artifact_digest: "0".repeat(64),
                compatibility_date: "2024-01-01".into(),
                compatibility_flags: vec![],
            },
            bindings,
            routes: vec![RouteDesired {
                hostname: format!("{worker_id}.test"),
            }],
            crons: vec![],
            queue_consumers: vec![],
            durable_object_classes: if has_do {
                vec![DoClassDesired {
                    class_name: "C".into(),
                    unique_key: "a".repeat(64),
                    enable_sql: true,
                    from_worker_id: None,
                }]
            } else {
                vec![]
            },
            workflows: vec![],
            quotas: WorkerQuotas {
                cpu_ms: 50,
                memory_mb: 128,
                subrequests: 0,
                outbound_connections: 0,
            },
        }
    }

    fn taint_tenant(
        workers: Vec<snapshot_protocol::compute_desired::WorkerDesired>,
    ) -> std::collections::BTreeSet<String> {
        do_tainted_workers(&snapshot_protocol::compute_desired::TenantDesired {
            tenant_id: "t".into(),
            workers,
        })
    }

    #[test]
    fn taint_marks_do_declarers_and_do_binding_callers() {
        use snapshot_protocol::compute_desired::{BindingDesired, DoBindingDesired};
        // w0 declares a DO class; w1 binds it cross-worker; w2 plain.
        let tainted = taint_tenant(vec![
            taint_worker("w0", vec![], true),
            taint_worker(
                "w1",
                vec![BindingDesired::DurableObject(DoBindingDesired {
                    name: "D".into(),
                    class_name: "C".into(),
                    service_worker_id: Some("w0".into()),
                })],
                false,
            ),
            taint_worker("w2", vec![], false),
        ]);
        assert!(tainted.contains("w0"), "declarer tainted");
        assert!(tainted.contains("w1"), "cross-worker DO caller tainted");
        assert!(!tainted.contains("w2"), "unrelated worker flips early");
    }

    #[test]
    fn taint_propagates_through_service_bindings() {
        use snapshot_protocol::compute_desired::{BindingDesired, ServiceBinding};
        // w0 DO declarer <- w1 service-binds w0 <- w2 service-binds w1.
        let tainted = taint_tenant(vec![
            taint_worker("w0", vec![], true),
            taint_worker(
                "w1",
                vec![BindingDesired::Service(ServiceBinding {
                    name: "S".into(),
                    worker_id: "w0".into(),
                })],
                false,
            ),
            taint_worker(
                "w2",
                vec![BindingDesired::Service(ServiceBinding {
                    name: "S".into(),
                    worker_id: "w1".into(),
                })],
                false,
            ),
        ]);
        assert_eq!(
            tainted,
            ["w0", "w1", "w2"].into_iter().map(String::from).collect(),
            "transitive service-binding callers reach the namespace"
        );
    }

    /// MIG-13: aborting a switch with NO live generation must clear
    /// every published route map — otherwise early-flip slots of a
    /// killed warm generation stay routable and held requests resolve
    /// to dead sockets.
    #[tokio::test]
    async fn abort_pending_clears_early_maps_without_active() {
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_install(BTreeMap::from([(
            "w0".to_string(),
            crate::endpoint::WorkerEndpoint::Unix(PathBuf::from("/nonexistent/w0.sock")),
        )]));
        assert!(!actor.runtime.routing.load().is_empty());
        actor.abort_pending();
        assert!(
            actor.runtime.routing.load().is_empty(),
            "no live generation -> early-flip routes must not survive the abort"
        );
        assert!(actor.runtime.qconsumer_routing.load().is_empty());
        assert!(actor.runtime.cron_routing.load().is_empty());
    }

    #[test]
    fn taint_empty_when_no_do() {
        let tainted = taint_tenant(vec![
            taint_worker("w0", vec![], false),
            taint_worker("w1", vec![], false),
        ]);
        assert!(tainted.is_empty());
    }


    // ---- MIG-13-r2: prewarm hardening ----

    fn prewarm_ctx(dir: &Path, seq: u64, warm: Option<WarmGen>) -> PrewarmCtx {
        PrewarmCtx {
            seq,
            attempt: Box::new(test_spec(dir)),
            desired: Box::new(test_spec(dir)),
            task: None,
            warm,
            live_config: dir.join("config.live.capnp"),
            work_dir: dir.to_path_buf(),
            started_at: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(60),
        }
    }

    /// A parked warm generation: a real `sleep` stands in for workerd
    /// (own process group, like the launcher arranges) so exit polls
    /// and group kills exercise real process state.
    async fn fake_warm_gen(dir: &Path, swap_needed: bool) -> WarmGen {
        let (_a, b) = tokio::net::UnixStream::pair().unwrap();
        let child = {
            let mut cmd = tokio::process::Command::new("sleep");
            cmd.arg("60").process_group(0);
            cmd.spawn().unwrap()
        };
        let pid = child.id().expect("fresh spawn has pid");
        WarmGen {
            swap_needed,
            gen: PendingGen {
                spec: Box::new(test_spec(dir)),
                pid,
                child: crate::launch::GroupChild::wrap(child, pid),
                control: b,
                endpoints: Default::default(),
                qconsumer_endpoints: Default::default(),
                wfops_endpoints: Default::default(),
                cron_endpoints: Default::default(),
                leases: vec![],
            },
            early: EarlyFlip::default(),
            warmed_at: Instant::now(),
        }
    }

    /// H1: `start_prewarm` materializes the watched live config via
    /// the shared atomic writer — 0600 (the twin can carry
    /// secret-bearing text), tmp-in-dir + rename — strips the marker,
    /// and repoints every disk path at owned scratch dirs (chowned to
    /// the tenant uid when the host is root; the launcher's
    /// spawn-side ownership check is the hard fail otherwise).
    #[tokio::test]
    async fn start_prewarm_writes_live_config_secure() {
        let dir = tmpdir("prewarm-secure");
        let text = concat!(
            "using Workerd = import \"/workerd/workerd.capnp\";\n",
            "const config :Workerd.Config = (\n",
            "  services = [ (name = \"do-storage-w0\", disk = (path = \"/real/do/w0\", writable = true)) ],\n",
            "  sockets = [\n",
            "    (name = \"sock-w0\", address = \"unix:/x/sock-w0.sock\", http = (), service = \"worker-w0\"),\n",
            "    (name = \"vrt-wu\", address = \"unix:/x/vrt-wu.sock\", http = (), service = \"vrt-wu-probe\"),\n",
            "    (name = \"vrt-switch\", address = \"unix:/x/vrt-sw.sock\", http = (), service = \"vrt-wu-probe\"),\n",
            "  ],\n",
            ");\n",
        );
        let config_path = dir.join("config.capnp");
        std::fs::write(&config_path, text).unwrap();
        let mut spec = test_spec(&dir);
        spec.config_path = config_path;
        spec.isolation = Some(crate::spec::TenantIsolation { uid: 4242, gid: 4242 });
        spec.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        let mut actor = test_actor(SupervisorDeps::default());
        actor.start_prewarm(&spec, &spec).expect("prewarm starts");
        let ctx = actor.prewarm.take().expect("ctx parked");
        assert!(ctx.task.is_some(), "spawn task dispatched");
        if let Some(t) = ctx.task {
            t.abort();
        }
        let live = dir.join("config.live.capnp");
        let meta = std::fs::metadata(&live).unwrap();
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let twin = std::fs::read_to_string(&live).unwrap();
        assert!(!twin.contains("vrt-switch"), "marker stripped: {twin}");
        let scratch = dir.join("prewarm-disks").canonicalize().unwrap();
        assert!(scratch.join("disk-0").is_dir(), "scratch disk created");
        assert!(twin.contains(&scratch.join("disk-0").to_string_lossy().to_string()));
        assert!(!twin.contains("/real/do"), "no live path leaked: {twin}");
        assert!(!std::path::Path::new("/real/do").exists());
    }

    /// H0: a DO spec whose render carries no `vrt-switch` marker
    /// cannot prove reload completion — `start_prewarm` refuses and
    /// the serialized path owns the switch (never a false success).
    #[tokio::test]
    async fn prewarm_refused_without_marker() {
        let dir = tmpdir("prewarm-nomarker");
        let text = concat!(
            "const config :Workerd.Config = (\n",
            "  services = [ (name = \"do-storage-w0\", disk = (path = \"/real/do/w0\", writable = true)) ],\n",
            "  sockets = [\n",
            "    (name = \"sock-w0\", address = \"unix:/x/sock-w0.sock\", http = (), service = \"worker-w0\"),\n",
            "  ],\n",
            ");\n",
        );
        let config_path = dir.join("config.capnp");
        std::fs::write(&config_path, text).unwrap();
        let mut spec = test_spec(&dir);
        spec.config_path = config_path;
        spec.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        let mut actor = test_actor(SupervisorDeps::default());
        let err = actor.start_prewarm(&spec, &spec);
        assert!(err.is_err(), "no marker -> no prewarm");
        assert!(actor.prewarm.is_none());
    }

    /// M1: a prewarm failure lands on the SAME handler as a classic
    /// launch failure — attribution, active=None circuit accounting
    /// and the bounded retry are the shared semantics the bespoke
    /// branch skipped.
    #[tokio::test]
    async fn prewarm_failure_routes_through_launch_failed() {
        let dir = tmpdir("prewarm-m1");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        actor.prewarm = Some(prewarm_ctx(&dir, 7, None));
        actor.on_ev(Ev::PrewarmDone {
            seq: 7,
            result: Err(SpawnFailure {
                reason: "ready-timeout",
                detail: Some("worker-w0 blew up".into()),
            }),
        });
        assert!(actor.prewarm.is_none(), "ctx consumed");
        assert!(!actor.runtime.serving(), "no live gen -> not serving");
        assert!(actor.runtime.last_error().is_some());
        assert!(actor.retry_at.is_some(), "bounded retry armed");
        assert_eq!(actor.runtime.stats.launch_fails.load(Ordering::Relaxed), 1);
    }

    /// M3: a prewarm SPAWN in flight is a launch attempt — a stray
    /// `start_pending_launch` (retry tick, monitor restart) must not
    /// stack a third workerd on (old gen + warm spawn).
    #[tokio::test]
    async fn prewarm_in_flight_blocks_extra_launch() {
        let dir = tmpdir("prewarm-m3-guard");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        let mut ctx = prewarm_ctx(&dir, 3, None);
        ctx.task = Some(tokio::spawn(std::future::pending::<()>()));
        actor.prewarm = Some(ctx);
        actor.start_pending_launch(false);
        assert!(actor.pending.is_none() && actor.preflight.is_none());
    }

    /// M3: the armed warm generation is consumed BEFORE retry
    /// reassembly — a reassembled spec would allocate a fresh gen-<n>
    /// dir the already-running process never uses (the leak).
    #[tokio::test]
    async fn warm_consumed_before_retry_reassembly() {
        let dir = tmpdir("prewarm-m3-order");
        let calls = Arc::new(AtomicUsize::new(0));
        struct CountAsm(Arc<AtomicUsize>);
        impl TenantAssembler for CountAsm {
            fn assemble(
                &self,
                _t: &snapshot_protocol::compute_desired::TenantDesired,
                _r: u64,
                _g: u64,
            ) -> Result<TenantLaunchSpec, crate::assemble::AssembleError> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Err(crate::assemble::AssembleError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "must not assemble",
                )))
            }
            fn params_fingerprint(&self) -> Result<String, crate::assemble::AssembleError> {
                Ok("fp".into())
            }
        }
        let mut actor = test_actor(SupervisorDeps {
            assembler: Some(Arc::new(CountAsm(calls.clone()))),
            ..Default::default()
        });
        actor.attempted_once = true;
        actor.runtime.test_set_state(TenantState::Ready);
        let warm = fake_warm_gen(&dir, false).await;
        actor.prewarm = Some(prewarm_ctx(&dir, 1, Some(warm)));
        let mut desired = test_spec(&dir);
        desired.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        desired.relaunch = Some(crate::spec::RelaunchCtx {
            tenant: snapshot_protocol::compute_desired::TenantDesired {
                tenant_id: "t".into(),
                workers: vec![taint_worker("w0", vec![], true)],
            },
            revision: 9,
        });
        actor.desired_spec = Some(Box::new(desired.clone()));
        actor.launch_after_preflight(Box::new(desired.clone()), &desired);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "no reassembly (no fresh gen dir) while a warm gen exists"
        );
        assert!(actor.pending.is_some(), "swap consume registered");
        if let Some(p) = actor.pending.take() {
            p.task.abort();
        }
    }

    /// H2: the parked warm gen is death-polled on ticks — a dead warm
    /// process loses its early-flip routes in the same tick and the
    /// failure lands on the shared launch-failure path.
    #[tokio::test]
    async fn warm_death_clears_early_routes() {
        let dir = tmpdir("warm-dead");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        // Gate-window state: active taken, early maps published.
        actor.runtime.switch_gate.begin();
        actor.runtime.routing.store(Arc::new(BTreeMap::from([(
            "w0".to_string(),
            Arc::new(EndpointSlot {
                endpoint: WorkerEndpoint::Unix(PathBuf::from("/nonexistent/w0.sock")),
                in_flight: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                drain_notify: actor.runtime.drain_notify.clone(),
            }),
        )])));
        let warm = fake_warm_gen(&dir, false).await;
        signal_process_group(warm.gen.pid, libc::SIGKILL);
        tokio::time::sleep(Duration::from_millis(60)).await;
        actor.prewarm = Some(prewarm_ctx(&dir, 5, Some(warm)));
        actor.on_tick();
        assert!(actor.prewarm.is_none(), "dead warm gen dropped");
        assert!(
            actor.runtime.routing.load().is_empty(),
            "early routes cleared"
        );
        assert!(!actor.runtime.serving());
        assert!(actor.retry_at.is_some(), "fail-closed retry armed");
        assert_eq!(actor.runtime.stats.launch_fails.load(Ordering::Relaxed), 1);
    }

    /// L2: a warm generation parked past `prewarm_ttl` is killed and
    /// the same desired falls back to the serialized switch —
    /// `outcome="expired"` is counted and no second warm is spawned.
    #[tokio::test]
    async fn prewarm_expiry_kills_warm_and_falls_back() {
        let dir = tmpdir("prewarm-expire");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        let warm = fake_warm_gen(&dir, false).await;
        let mut ctx = prewarm_ctx(&dir, 9, Some(warm));
        ctx.expires_at = Instant::now() - Duration::from_millis(1);
        actor.prewarm = Some(ctx);
        actor.on_tick();
        assert!(actor.prewarm.is_none());
        assert!(actor.prewarm_burned, "same desired cannot re-prewarm");
        assert!(actor.retry_at.is_some(), "serialized retry armed");
    }

    /// L2: an in-flight prewarm spawn past TTL aborts identically —
    /// the cap binds the whole episode, not just the parked window.
    #[tokio::test]
    async fn prewarm_expiry_aborts_in_flight_spawn() {
        let dir = tmpdir("prewarm-expire2");
        let mut actor = test_actor(SupervisorDeps::default());
        let mut ctx = prewarm_ctx(&dir, 11, None);
        ctx.task = Some(tokio::spawn(std::future::pending::<()>()));
        ctx.expires_at = Instant::now() - Duration::from_millis(1);
        actor.prewarm = Some(ctx);
        actor.on_tick();
        assert!(actor.prewarm.is_none());
        assert!(actor.prewarm_burned);
        assert!(actor.retry_at.is_some());
    }

    /// M2+H1+L1: the swap drains pending waitUntil work only up to
    /// the do-switch bound (counting the cut), rewrites the live
    /// config via the atomic writer (0600), waits the marker, and
    /// re-probes before handing back a LIVE process.
    #[tokio::test]
    async fn swap_finalize_drains_then_swaps() {
        let dir = tmpdir("swap-final");
        let live_config = dir.join("config.live.capnp");
        std::fs::write(&live_config, "TWIN").unwrap();
        let final_config = dir.join("config.capnp");
        std::fs::write(&final_config, "FINAL").unwrap();
        // Probe endpoint that always reports pending work — the drain
        // must bound at wu_grace and count the truncation. TCP stub:
        // the sandbox forbids unix binds and the probe path is
        // address-family agnostic.
        let wu_addr = wu_tcp_stub("{\"pending\":3,\"failed\":0}").await;
        let mut warm = fake_warm_gen(&dir, true).await;
        warm.gen.spec.waituntil_endpoint = Some(WorkerEndpoint::Tcp(wu_addr));
        let (mut a, b) = tokio::net::UnixStream::pair().unwrap();
        warm.gen.control = b;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            use tokio::io::AsyncWriteExt;
            let _ = a
                .write_all(
                    b"{\"event\":\"listen\",\"socket\":\"vrt-switch\",\"address\":\"unix:/x\"}\n",
                )
                .await;
        });
        let gen = swap_finalize(
            warm,
            live_config.clone(),
            final_config,
            vec![],
            Duration::from_secs(5),
            Duration::from_millis(80),
            Duration::from_millis(120),
            Duration::from_millis(150),
            None,
            "t".into(),
        )
        .await
        .expect("swap completes");
        assert_eq!(std::fs::read_to_string(&live_config).unwrap(), "FINAL");
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&live_config).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let mut gen = gen;
        let _ = terminate_child(&mut gen.child.inner, gen.pid, Duration::from_millis(100)).await;
    }

    /// H2-r2: `!swap_needed` installs only after a re-probe — an
    /// already-dead warm process fails closed instead of installing.
    #[tokio::test]
    async fn swap_finalize_no_swap_fails_closed_on_dead_warm() {
        let dir = tmpdir("swap-dead");
        let mut warm = fake_warm_gen(&dir, false).await;
        warm
            .gen
            .endpoints
            .insert("w0".into(), WorkerEndpoint::Unix(dir.join("nope.sock")));
        signal_process_group(warm.gen.pid, libc::SIGKILL);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let res = swap_finalize(
            warm,
            dir.join("live.capnp"),
            dir.join("config.capnp"),
            vec![],
            Duration::from_secs(1),
            Duration::from_millis(60),
            Duration::from_millis(100),
            Duration::from_millis(50),
            None,
            "t".into(),
        )
        .await;
        assert!(res.is_err(), "dead warm process must not install");
    }

    // ---- MIG-13-r3: abandon-path hardening ----

    /// A live `sleep` standing in for an installed generation's
    /// workerd (same stand-in as `fake_warm_gen`, returned as the
    /// `PendingGen` `Ev::Launched` carries).
    async fn fake_pending_gen(dir: &Path) -> PendingGen {
        let (_a, b) = tokio::net::UnixStream::pair().unwrap();
        let child = {
            let mut cmd = tokio::process::Command::new("sleep");
            cmd.arg("60").process_group(0);
            cmd.spawn().unwrap()
        };
        let pid = child.id().expect("fresh spawn has pid");
        PendingGen {
            spec: Box::new(test_spec(dir)),
            pid,
            child: crate::launch::GroupChild::wrap(child, pid),
            control: b,
            endpoints: Default::default(),
            qconsumer_endpoints: Default::default(),
            wfops_endpoints: Default::default(),
            cron_endpoints: Default::default(),
            leases: vec![],
        }
    }

    /// Read one counter out of the shared registry's text exposition —
    /// `needle` pins the series (unique per-test tenant labels keep
    /// parallel tests out of each other's counts).
    fn rendered_counter(metric: &str, needle: &str) -> f64 {
        crate::metrics::registry()
            .render()
            .lines()
            .filter(|l| l.starts_with(metric) && l.contains(needle))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum()
    }

    /// L2-r3: the warm-generation TTL must NEVER fire inside the
    /// switch gate — the armed process is load-bearing there (the r2
    /// expiry killed it mid-drain and left early-flip routes pointed
    /// at a dead socket). Once the window closes the cap fires on the
    /// next tick: kill, burn, revoke, retire, serialized retry armed.
    #[tokio::test]
    async fn prewarm_expiry_suppressed_inside_gate_then_fires() {
        let dir = tmpdir("expire-gate");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-expire-gate".into();
        actor.runtime.test_set_state(TenantState::Ready);
        actor.runtime.switch_gate.begin();
        let slot = Arc::new(EndpointSlot {
            endpoint: WorkerEndpoint::Unix(dir.join("w0.sock")),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            drain_notify: actor.runtime.drain_notify.clone(),
        });
        actor
            .runtime
            .routing
            .store(Arc::new(BTreeMap::from([("w0".to_string(), slot.clone())])));
        let warm = fake_warm_gen(&dir, true).await;
        let warm_pid = warm.gen.pid;
        let mut ctx = prewarm_ctx(&dir, 9, Some(warm));
        ctx.expires_at = Instant::now() - Duration::from_millis(1);
        actor.prewarm = Some(ctx);
        actor.on_tick();
        assert!(
            actor.prewarm.is_some(),
            "ttl must not fire inside the gate window — the warm generation is load-bearing"
        );
        assert!(!actor.runtime.routing.load().is_empty(), "early routes stay live");
        assert!(crate::launch::pid_alive(warm_pid), "warm process still serving");
        assert!(!actor.prewarm_burned);
        // The window closed (abandon or install elsewhere) — the cap
        // is the first thing the next tick enforces.
        actor.runtime.switch_gate.end();
        actor.on_tick();
        assert!(actor.prewarm.is_none(), "expired ctx dropped");
        assert!(actor.prewarm_burned, "same desired cannot re-prewarm");
        assert!(
            actor.runtime.routing.load().is_empty(),
            "revoke cleared the early-flip routes"
        );
        assert!(actor.runtime.qconsumer_routing.load().is_empty());
        assert!(actor.runtime.wfops_routing.load().is_empty());
        assert!(actor.runtime.wfop_token_routing.load().is_empty());
        assert!(actor.runtime.cron_routing.load().is_empty());
        assert!(slot.is_closed(), "revoked slot refuses new acquisitions");
        assert!(slot.acquire().is_none());
        assert!(!actor.runtime.switch_gate.active(), "gate stays ended");
        assert!(!actor.runtime.serving());
        assert!(actor.retry_at.is_some(), "serialized retry armed");
        let deadline = Instant::now() + Duration::from_secs(3);
        while crate::launch::pid_alive(warm_pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !crate::launch::pid_alive(warm_pid),
            "expiry SIGKILLs the parked process group"
        );
        assert!(!dir.exists(), "spent warm gen dir retired");
    }

    /// r3: a parked warm generation dying mid-window is a FAILED
    /// prewarm episode — counted `outcome="failed"`, the flag burned
    /// so the same desired retries on the serialized path, the spent
    /// gen dir removed, and the serving old generation's routes never
    /// touched.
    #[tokio::test]
    async fn warm_death_burns_prewarm_and_retires_dir() {
        let dir = tmpdir("warm-dead-r3");
        std::fs::create_dir_all(dir.join("prewarm-disks")).unwrap();
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-warm-dead-r3".into();
        actor.runtime.test_set_state(TenantState::Ready);
        let old_dir = tmpdir("warm-dead-old");
        actor.active = Some(fake_live_gen(&old_dir, wall_now_ms() + 120_000).await);
        actor.runtime.set_serving(true);
        let warm = fake_warm_gen(&dir, false).await;
        let warm_pid = warm.gen.pid;
        signal_process_group(warm_pid, libc::SIGKILL);
        tokio::time::sleep(Duration::from_millis(60)).await;
        actor.prewarm = Some(prewarm_ctx(&dir, 5, Some(warm)));
        actor.on_tick();
        assert!(actor.prewarm.is_none(), "dead warm gen dropped");
        assert!(actor.prewarm_burned, "dead warm episode burns same-desired prewarm");
        assert!(!dir.exists(), "spent warm gen dir removed");
        assert_eq!(actor.switch_fail_streak, 1, "same bounded switch path");
        assert!(actor.retry_at.is_some());
        assert!(actor.active.is_some(), "old generation keeps serving");
        assert!(actor.runtime.serving());
        assert_ne!(actor.runtime.state(), TenantState::SwitchFailed);
        let failed = rendered_counter(
            "vrt_host_prewarm_total",
            "outcome=\"failed\",tenant=\"t-warm-dead-r3\"",
        );
        assert!(failed >= 1.0, "warm death counted as a failed prewarm episode");
    }

    /// r3: a `!swap_needed` install that fails its re-probe leaves
    /// nothing serving — the early-flip routes the warm generation
    /// published must be revoked (`on_launch_failed` used to only
    /// `set_serving(false)` + `gate.end()`, leaving routable slots at
    /// a killed process).
    #[tokio::test]
    async fn swap_failure_no_swap_needed_revokes_early_routes() {
        let dir = tmpdir("swapfail-noswap");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-swapfail-ns".into();
        actor.runtime.test_set_state(TenantState::Ready);
        actor.probe_timeout = Duration::from_millis(60);
        actor.preflight_ok_digest = Some("d".into());
        let old_dir = tmpdir("swapfail-old");
        actor.active = Some(fake_live_gen(&old_dir, wall_now_ms() + 120_000).await);
        let mut warm = fake_warm_gen(&dir, false).await;
        // A reachable early route AND a dead install endpoint — the
        // re-probe inside swap_finalize must fail closed.
        let slot = Arc::new(EndpointSlot {
            endpoint: WorkerEndpoint::Unix(dir.join("early.sock")),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            drain_notify: actor.runtime.drain_notify.clone(),
        });
        warm.early.routing = Arc::new(BTreeMap::from([("w0".to_string(), slot.clone())]));
        warm
            .gen
            .endpoints
            .insert("w0".into(), WorkerEndpoint::Unix(dir.join("nope.sock")));
        actor.prewarm = Some(prewarm_ctx(&dir, 4, Some(warm)));
        let desired_dir = tmpdir("swapfail-desired");
        let mut desired = test_spec(&desired_dir);
        desired.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: desired_dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        actor.desired_spec = Some(Box::new(desired.clone()));
        actor.launch_after_preflight(Box::new(desired.clone()), &desired);
        assert!(actor.runtime.switch_gate.active(), "serialized window open");
        assert_eq!(actor.draining.len(), 1, "old generation draining");
        assert!(
            actor.runtime.routing.load().contains_key("w0"),
            "early-flip route published"
        );
        actor.on_exited(1);
        let p = actor.pending.as_ref().expect("install consume dispatched");
        assert!(p.prewarm_swap, "pending marks the warm consume");
        let seq = p.seq;
        let ev = tokio::time::timeout(Duration::from_secs(10), actor.ev_rx.recv())
            .await
            .expect("swap task finishes")
            .expect("event received");
        let Ev::Launched { seq: ev_seq, result } = ev else {
            panic!("expected Ev::Launched");
        };
        assert_eq!(ev_seq, seq);
        assert!(result.is_err(), "dead endpoint must fail the install");
        actor.on_ev(Ev::Launched {
            seq,
            result: result.map(|_| unreachable!()),
        });
        assert!(actor.pending.is_none());
        assert!(actor.prewarm.is_none());
        assert!(actor.prewarm_burned);
        assert!(
            actor.runtime.routing.load().is_empty(),
            "early routes revoked on install failure"
        );
        assert!(actor.runtime.qconsumer_routing.load().is_empty());
        assert!(actor.runtime.cron_routing.load().is_empty());
        assert!(slot.is_closed(), "revoked slot refuses new acquisitions");
        assert!(!actor.runtime.switch_gate.active(), "gate ended");
        assert!(!actor.runtime.serving(), "nothing serves — typed NoServing");
        assert!(actor.retry_at.is_some(), "bounded relaunch armed");
        assert!(!dir.exists(), "spent warm gen dir retired");
        let failed = rendered_counter(
            "vrt_host_prewarm_total",
            "outcome=\"failed\",tenant=\"t-swapfail-ns\"",
        );
        assert!(failed >= 1.0, "failed install counted as a failed prewarm episode");
    }

    /// r3: the `swap_needed` consume closes every early-flip slot
    /// BEFORE the maps empty — a resolver holding a stale `Arc` can
    /// never acquire after the settle observed `in_flight == 0` (the
    /// r2 settle race could land a request on the reloading process).
    /// A swap failure then burns the flag, retires the dir and
    /// releases the gate.
    #[tokio::test]
    async fn swap_consume_closes_early_slots_before_clear() {
        let dir = tmpdir("swap-slots");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        actor.preflight_ok_digest = Some("d".into());
        let old_dir = tmpdir("swap-slots-old");
        actor.active = Some(fake_live_gen(&old_dir, wall_now_ms() + 120_000).await);
        let mut warm = fake_warm_gen(&dir, true).await;
        let slot = Arc::new(EndpointSlot {
            endpoint: WorkerEndpoint::Unix(dir.join("early.sock")),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            drain_notify: actor.runtime.drain_notify.clone(),
        });
        warm.early.routing = Arc::new(BTreeMap::from([("w0".to_string(), slot.clone())]));
        let mut ctx = prewarm_ctx(&dir, 6, Some(warm));
        // A missing final config fails the swap fast — after the
        // consume-time close/clear, deterministic enough to await.
        ctx.attempt.config_path = dir.join("missing.capnp");
        actor.prewarm = Some(ctx);
        let desired_dir = tmpdir("swap-slots-desired");
        let mut desired = test_spec(&desired_dir);
        desired.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: desired_dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        actor.desired_spec = Some(Box::new(desired.clone()));
        actor.launch_after_preflight(Box::new(desired.clone()), &desired);
        // A request holds the slot across the consume — the settle
        // sees it, but no NEW acquisition is possible once closed.
        let guard = slot.acquire().expect("slot live before consume");
        actor.on_exited(1);
        assert!(slot.is_closed(), "early slot closed at consume");
        assert!(slot.acquire().is_none(), "no new acquisition after close");
        assert!(
            actor.runtime.routing.load().is_empty(),
            "early routes lifted for the reload"
        );
        assert!(
            actor.pending.as_ref().is_some_and(|p| p.prewarm_swap),
            "swap dispatched"
        );
        let seq = actor.pending.as_ref().unwrap().seq;
        drop(guard);
        let ev = tokio::time::timeout(Duration::from_secs(10), actor.ev_rx.recv())
            .await
            .expect("swap task finishes")
            .expect("event received");
        let Ev::Launched { result, .. } = ev else {
            panic!("expected Ev::Launched");
        };
        assert!(result.is_err(), "missing final config fails the swap");
        actor.on_ev(Ev::Launched {
            seq,
            result: result.map(|_| unreachable!()),
        });
        assert!(actor.pending.is_none());
        assert!(actor.prewarm_burned, "failed swap burns same-desired prewarm");
        assert!(!actor.runtime.switch_gate.active(), "gate ended");
        assert!(!actor.runtime.serving());
        assert!(!dir.exists(), "spent warm gen dir retired");
    }

    /// r3: a burned prewarm makes the retry a CLASSIC serialized
    /// launch — the assembler runs again into a FRESH `gen-<n>` dir
    /// (the spent dir is gone), no second process is warmed, and the
    /// successful install resets the streak (no SwitchFailed from a
    /// prewarm-only failure).
    #[tokio::test]
    async fn burned_prewarm_retry_reassembles_fresh_dir() {
        let fresh_root = tmpdir("classic-fresh");
        let calls = Arc::new(AtomicUsize::new(0));
        struct FreshAsm {
            calls: Arc<AtomicUsize>,
            root: PathBuf,
        }
        impl TenantAssembler for FreshAsm {
            fn assemble(
                &self,
                _t: &snapshot_protocol::compute_desired::TenantDesired,
                _r: u64,
                g: u64,
            ) -> Result<TenantLaunchSpec, crate::assemble::AssembleError> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Ok(test_spec(&self.root.join(format!("gen-{g}"))))
            }
            fn params_fingerprint(&self) -> Result<String, crate::assemble::AssembleError> {
                Ok("fp".into())
            }
        }
        let mut actor = test_actor(SupervisorDeps {
            assembler: Some(Arc::new(FreshAsm {
                calls: calls.clone(),
                root: fresh_root.clone(),
            })),
            ..Default::default()
        });
        actor.runtime.test_set_state(TenantState::Ready);
        actor.preflight_ok_digest = Some("d".into());
        let old_dir = tmpdir("classic-old");
        actor.active = Some(fake_live_gen(&old_dir, wall_now_ms() + 120_000).await);
        let desired_dir = tmpdir("classic-desired");
        let mut desired = test_spec(&desired_dir);
        desired.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: desired_dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        desired.relaunch = Some(crate::spec::RelaunchCtx {
            tenant: snapshot_protocol::compute_desired::TenantDesired {
                tenant_id: "t".into(),
                workers: vec![taint_worker("w0", vec![], true)],
            },
            revision: 9,
        });
        actor.desired_spec = Some(Box::new(desired.clone()));
        // The prewarm episode's spawn verdict fails — the attempt is
        // burned before the gate ever opened.
        actor.prewarm = Some(prewarm_ctx(&desired_dir, 7, None));
        actor.on_ev(Ev::PrewarmDone {
            seq: 7,
            result: Err(SpawnFailure {
                reason: "spawn",
                detail: None,
            }),
        });
        assert!(actor.prewarm.is_none());
        assert!(actor.prewarm_burned, "failed episode burns the flag");
        assert!(!desired_dir.exists(), "spent prewarm dir retired");
        assert_eq!(actor.switch_fail_streak, 1);
        assert_ne!(actor.runtime.state(), TenantState::SwitchFailed);
        assert!(actor.active.is_some(), "old generation still serving");
        // The retry: classic serialized path end-to-end.
        actor.retry_at = Some(Instant::now() - Duration::from_millis(1));
        actor.on_tick();
        assert!(actor.prewarm.is_none(), "no second prewarm for the same desired");
        assert_eq!(actor.draining.len(), 1, "old generation draining");
        assert!(actor.runtime.switch_gate.active());
        actor.on_exited(1);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "retry reassembled exactly once"
        );
        let p = actor.pending.as_ref().expect("classic launch dispatched");
        assert!(!p.prewarm_swap, "classic retry is not a warm consume");
        assert_ne!(p.work_dir, desired_dir, "fresh gen dir, not the spent one");
        assert!(p.work_dir.starts_with(&fresh_root));
        p.task.abort(); // the fake binary would fail — feed the verdict directly
        let seq = p.seq;
        actor.on_ev(Ev::Launched {
            seq,
            result: Ok(fake_pending_gen(&fresh_root.join("gen-1")).await),
        });
        assert!(actor.active.is_some(), "classic retry installed");
        assert_eq!(
            actor.switch_fail_streak, 0,
            "a successful classic fallback resets the streak — no SwitchFailed"
        );
        assert_eq!(actor.runtime.state(), TenantState::Ready);
        assert!(actor.runtime.serving());
    }

    /// r3: the single-writer invariant — the warm generation can be
    /// armed and its early routes live, but `swap_finalize` MUST NOT
    /// run while the old generation still holds the class dirs. The
    /// consume only fires once `draining` is empty.
    #[tokio::test]
    async fn warm_never_swaps_while_old_gen_drains() {
        let dir = tmpdir("no-swap-drain");
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        actor.preflight_ok_digest = Some("d".into());
        let old_dir = tmpdir("no-swap-old");
        actor.active = Some(fake_live_gen(&old_dir, wall_now_ms() + 120_000).await);
        let warm = fake_warm_gen(&dir, true).await;
        actor.prewarm = Some(prewarm_ctx(&dir, 2, Some(warm)));
        let desired_dir = tmpdir("no-swap-desired");
        let mut desired = test_spec(&desired_dir);
        desired.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: desired_dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        actor.desired_spec = Some(Box::new(desired.clone()));
        actor.launch_after_preflight(Box::new(desired.clone()), &desired);
        assert_eq!(actor.draining.len(), 1, "old generation entered drain");
        assert!(actor.prewarm.is_some(), "warm parked — NOT consumed");
        assert!(
            actor.pending.is_none(),
            "swap_finalize must not run while the old generation drains"
        );
        assert!(actor.runtime.switch_gate.active());
        actor.on_exited(1);
        assert!(actor.draining.is_empty());
        assert!(actor.prewarm.is_none(), "warm consumed once the old gen is dead");
        let p = actor.pending.as_ref().expect("swap dispatched after drain");
        assert!(p.prewarm_swap);
        p.task.abort();
    }

    /// r3: abandoning an in-flight swap kills the warm process
    /// (kill-on-drop inside the aborted task's future) and retires
    /// the spent gen dir — no orphan workerd, no stale dir.
    #[tokio::test]
    async fn abort_pending_kills_in_flight_swap_and_retires_dir() {
        let dir = tmpdir("abort-swap");
        std::fs::write(dir.join("config.live.capnp"), "TWIN").unwrap();
        std::fs::write(dir.join("config.capnp"), "FINAL").unwrap();
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        // Hold the marker peer open — the swap task parks on the
        // `vrt-switch` listen line like a real workerd that never
        // reloads.
        let (_a, b) = tokio::net::UnixStream::pair().unwrap();
        let mut warm = fake_warm_gen(&dir, true).await;
        warm.gen.control = b;
        let warm_pid = warm.gen.pid;
        let task_dir = dir.clone();
        let task = tokio::spawn(async move {
            let _ = swap_finalize(
                warm,
                task_dir.join("config.live.capnp"),
                task_dir.join("config.capnp"),
                vec![],
                Duration::from_secs(60),
                Duration::from_millis(50),
                Duration::from_secs(5),
                Duration::from_millis(50),
                None,
                "t".into(),
            )
            .await;
        });
        actor.pending = Some(PendingLaunch {
            seq: 3,
            task,
            work_dir: dir.clone(),
            desired_digest: "d".into(),
            leases: vec![],
            prewarm_swap: true,
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        actor.abort_pending();
        assert!(actor.pending.is_none());
        let deadline = Instant::now() + Duration::from_secs(3);
        while crate::launch::pid_alive(warm_pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !crate::launch::pid_alive(warm_pid),
            "aborted swap must SIGKILL the warm process"
        );
        assert!(!dir.exists(), "aborted swap retires the spent warm dir");
    }

    /// r3: `abort_pending` also tears down a parked prewarm episode —
    /// warm process killed, early routes revoked through the shared
    /// helper (slot closed, gate released), gen dir removed.
    #[tokio::test]
    async fn abort_pending_retires_prewarm_episode() {
        let dir = tmpdir("abort-prewarm");
        std::fs::create_dir_all(dir.join("prewarm-disks")).unwrap();
        std::fs::write(dir.join("config.live.capnp"), "x").unwrap();
        let mut actor = test_actor(SupervisorDeps::default());
        actor.runtime.test_set_state(TenantState::Ready);
        actor.runtime.switch_gate.begin();
        let slot = Arc::new(EndpointSlot {
            endpoint: WorkerEndpoint::Unix(dir.join("w0.sock")),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            drain_notify: actor.runtime.drain_notify.clone(),
        });
        actor
            .runtime
            .routing
            .store(Arc::new(BTreeMap::from([("w0".to_string(), slot.clone())])));
        let warm = fake_warm_gen(&dir, false).await;
        let warm_pid = warm.gen.pid;
        actor.prewarm = Some(prewarm_ctx(&dir, 4, Some(warm)));
        actor.abort_pending();
        assert!(actor.prewarm.is_none());
        assert!(actor.runtime.routing.load().is_empty());
        assert!(slot.is_closed());
        assert!(!actor.runtime.switch_gate.active());
        assert!(!actor.runtime.serving());
        let deadline = Instant::now() + Duration::from_secs(3);
        while crate::launch::pid_alive(warm_pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !crate::launch::pid_alive(warm_pid),
            "aborted episode SIGKILLs the parked process group"
        );
        assert!(!dir.exists(), "aborted episode retires the warm dir");
    }

    // ---- MIG-13-r5: swap socket pre-clear + fast child-exit ----

    /// r5: the pre-clear's scope is exactly "declared by the final
    /// config's sockets block AND directly inside the generation's
    /// own sock dir" — out-of-dir paths, `..` escapes and undeclared
    /// entries are refused or ignored, never unlinked.
    #[test]
    fn swap_preclear_scope_is_declared_in_dir_only() {
        let dir = tmpdir("preclear-scope");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        // Undeclared file inside the dir — never touched.
        let stray = sock_dir.join("stray.sock");
        std::fs::write(&stray, b"keep").unwrap();
        // A sibling generation's socket path, still on disk.
        let other_gen = dir.join("gen-9").join("sock");
        std::fs::create_dir_all(&other_gen).unwrap();
        let foreign = other_gen.join("w0.sock");
        std::fs::write(&foreign, b"keep").unwrap();
        // Declared only the (absent) in-dir path — succeeds and
        // touches nothing else.
        let text = format!(
            "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n  ],\n",
            sock_dir.join("w0.sock").display()
        );
        clear_declared_sockets(&text, &sock_dir).unwrap();
        assert!(stray.exists(), "undeclared entry in sock dir kept");
        assert!(foreign.exists(), "other generation's file kept");

        // Same call but the declared bind is the OTHER generation's
        // socket — the helper must REFUSE, never unlink it.
        let bad = format!(
            "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n  ],\n",
            foreign.display()
        );
        let e = clear_declared_sockets(&bad, &sock_dir).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
        assert!(foreign.exists(), "out-of-dir declared path survives");

        // `..` escapes — the lexical parent can never equal sock_dir.
        let trav = format!(
            "  sockets = [\n    (name = \"w0\", address = \"unix:{}/../w0.sock\", http = (), service = \"w0\"),\n  ],\n",
            sock_dir.display()
        );
        let e = clear_declared_sockets(&trav, &sock_dir).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");

        // Relative declared path — same refusal.
        let e = clear_declared_sockets(
            "  sockets = [\n    (name = \"w0\", address = \"unix:sock/w0.sock\", http = (), service = \"w0\"),\n  ],\n",
            &sock_dir,
        )
        .unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied, "{e}");
    }

    /// r5: a declared path that exists but is NOT a socket fails the
    /// pre-clear closed (remove_stale_socket's type check) — and a
    /// PARTIAL pass leaves already-cleared paths unlinked while the
    /// offending entry survives; the caller aborts the swap before
    /// the watched write.
    #[test]
    fn swap_preclear_non_socket_fails_closed_partial() {
        let dir = tmpdir("preclear-nonsock");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        let planted = sock_dir.join("w1.sock");
        std::fs::write(&planted, b"i am not a socket").unwrap();
        // w0 (absent) is processed first and "cleared", then the
        // planted regular file aborts the pass.
        let text = format!(
            "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n    (name = \"w1\", address = \"unix:{}\", http = (), service = \"w1\"),\n    (name = \"w2\", address = \"unix:{}\", http = (), service = \"w2\"),\n  ],\n",
            sock_dir.join("w0.sock").display(),
            planted.display(),
            sock_dir.join("w2.sock").display()
        );
        let e = clear_declared_sockets(&text, &sock_dir).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists, "{e}");
        assert!(planted.exists(), "planted regular file must never be unlinked");
        // A directory entry is equally non-socket — same refusal.
        let subdir = sock_dir.join("w3.sock");
        std::fs::create_dir(&subdir).unwrap();
        let text = format!(
            "  sockets = [\n    (name = \"w3\", address = \"unix:{}\", http = (), service = \"w3\"),\n  ],\n",
            subdir.display()
        );
        assert!(clear_declared_sockets(&text, &sock_dir).is_err());
        assert!(subdir.exists());
        // A symlink — even a DANGLING one — is a non-socket dirent:
        // it would still block the exec'd image's bind, so it must
        // fail closed here, and neither it nor any target is touched.
        let link = sock_dir.join("w4.sock");
        std::os::unix::fs::symlink(sock_dir.join("ghost.sock"), &link).unwrap();
        let text = format!(
            "  sockets = [\n    (name = \"w4\", address = \"unix:{}\", http = (), service = \"w4\"),\n  ],\n",
            link.display()
        );
        assert!(clear_declared_sockets(&text, &sock_dir).is_err());
        assert!(link.symlink_metadata().is_ok(), "dangling symlink kept");
    }

    /// r5: REAL socket inodes — declared in-dir sockets are unlinked
    /// while an undeclared socket in the same dir and a declared
    /// out-of-dir path are never touched. unix `bind()` is denied in
    /// the dev sandbox — gate exactly like `stale_socket_cleanup_*`.
    #[test]
    fn clear_declared_sockets_removes_declared_sockets_only() {
        if std::env::var("VRT_HOST_UNIX_TESTS").ok().as_deref() != Some("1") {
            eprintln!("skip: set VRT_HOST_UNIX_TESTS=1");
            return;
        }
        let dir = tmpdir("preclear-unix");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        let declared = sock_dir.join("w0.sock");
        let undeclared = sock_dir.join("w9.sock");
        let _l0 = std::os::unix::net::UnixListener::bind(&declared).unwrap();
        let _l9 = std::os::unix::net::UnixListener::bind(&undeclared).unwrap();
        // Listeners stay open — unlink must still remove the pathnames
        // (that is precisely what makes the reload's re-bind succeed).
        let text = format!(
            "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n    (name = \"vrt-switch\", address = \"unix:{}\", http = (), service = \"vrt-wu-probe\"),\n  ],\n",
            declared.display(),
            sock_dir.join("vrt-sw.sock").display()
        );
        clear_declared_sockets(&text, &sock_dir).unwrap();
        assert!(!declared.exists(), "declared socket path unlinked");
        assert!(undeclared.exists(), "undeclared socket in same dir kept");
    }

    /// r5: the warm child dies INSIDE the marker wait — the swap must
    /// return `swap-child-exit` in well under `ready_timeout` instead
    /// of draining the whole bound on a corpse.
    #[tokio::test]
    async fn swap_marker_wait_fails_fast_on_child_exit() {
        let dir = tmpdir("swap-child-exit");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        let live_config = dir.join("config.live.capnp");
        std::fs::write(&live_config, "TWIN").unwrap();
        let final_config = dir.join("config.capnp");
        std::fs::write(
            &final_config,
            format!(
                "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n    (name = \"vrt-switch\", address = \"unix:{}\", http = (), service = \"vrt-wu-probe\"),\n  ],\n",
                sock_dir.join("w0.sock").display(),
                sock_dir.join("vrt-sw.sock").display()
            ),
        )
        .unwrap();
        // A short-lived child stands in for the execve'd image that
        // fatals on reload — it exits ~200 ms into the marker wait.
        let (keep, b) = tokio::net::UnixStream::pair().unwrap();
        let child = {
            let mut cmd = tokio::process::Command::new("sleep");
            cmd.arg("0.2").process_group(0);
            cmd.spawn().unwrap()
        };
        let pid = child.id().expect("pid");
        let warm = WarmGen {
            swap_needed: true,
            gen: PendingGen {
                spec: Box::new(test_spec(&dir)),
                pid,
                child: crate::launch::GroupChild::wrap(child, pid),
                control: b,
                endpoints: Default::default(),
                qconsumer_endpoints: Default::default(),
                wfops_endpoints: Default::default(),
                cron_endpoints: Default::default(),
                leases: vec![],
            },
            early: EarlyFlip::default(),
            warmed_at: Instant::now(),
        };
        let t0 = Instant::now();
        let res = swap_finalize(
            warm,
            live_config.clone(),
            final_config,
            vec![],
            Duration::from_secs(60),
            Duration::from_millis(50),
            Duration::from_millis(200),
            Duration::ZERO,
            None,
            "t".into(),
        )
        .await;
        let err = res.err().expect("dead child must fail the swap");
        assert_eq!(err.reason, "swap-child-exit", "{err:?}");
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "must not drain ready_timeout on a dead child: {:?}",
            t0.elapsed()
        );
        // The watched write DID land — the failure is the reload's
        // child death, not the pre-clear or the write.
        assert!(
            std::fs::read_to_string(&live_config)
                .unwrap()
                .contains("vrt-switch"),
            "final bytes were written before the marker wait"
        );
        drop(keep);
    }

    /// r5: a pre-clear refusal aborts the swap BEFORE the watched
    /// live config is rewritten — the twin bytes survive and the warm
    /// child is terminated (never a reload against an unclearable
    /// bind).
    #[tokio::test]
    async fn swap_preclear_refusal_aborts_before_write() {
        let dir = tmpdir("swap-preclear-abort");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        // A declared bind that is a planted regular file — the
        // pre-clear refuses rather than unlink it.
        let planted = sock_dir.join("w0.sock");
        std::fs::write(&planted, b"not a socket").unwrap();
        let live_config = dir.join("config.live.capnp");
        std::fs::write(&live_config, "TWIN").unwrap();
        let final_config = dir.join("config.capnp");
        std::fs::write(
            &final_config,
            format!(
                "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n  ],\n",
                planted.display()
            ),
        )
        .unwrap();
        let warm = fake_warm_gen(&dir, true).await;
        let warm_pid = warm.gen.pid;
        let res = swap_finalize(
            warm,
            live_config.clone(),
            final_config,
            vec![],
            Duration::from_secs(5),
            Duration::from_millis(50),
            Duration::from_millis(200),
            Duration::ZERO,
            None,
            "t".into(),
        )
        .await;
        let err = res.err().expect("unclearable bind must fail the swap");
        assert_eq!(err.reason, "swap-preclear", "{err:?}");
        assert_eq!(
            std::fs::read_to_string(&live_config).unwrap(),
            "TWIN",
            "watched config never rewritten on a refused pre-clear"
        );
        assert!(planted.exists(), "planted file survives");
        let deadline = Instant::now() + Duration::from_secs(3);
        while crate::launch::pid_alive(warm_pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!crate::launch::pid_alive(warm_pid), "warm child terminated");
    }

    /// r5: the consume path — a warm child that dies during the swap
    /// fails fast AND revokes every early-flip route through the
    /// shared helper (slot closed, maps empty, gate released), then
    /// burns the prewarm episode for a fresh-gen serialized retry.
    #[tokio::test]
    async fn swap_child_exit_revokes_early_routes() {
        let dir = tmpdir("swapkill-early");
        let sock_dir = dir.join("sock");
        std::fs::create_dir_all(&sock_dir).unwrap();
        std::fs::write(dir.join("config.live.capnp"), "TWIN").unwrap();
        std::fs::write(
            dir.join("config.capnp"),
            format!(
                "  sockets = [\n    (name = \"w0\", address = \"unix:{}\", http = (), service = \"w0\"),\n    (name = \"vrt-switch\", address = \"unix:{}\", http = (), service = \"vrt-wu-probe\"),\n  ],\n",
                sock_dir.join("w0.sock").display(),
                sock_dir.join("vrt-sw.sock").display()
            ),
        )
        .unwrap();
        let mut actor = test_actor(SupervisorDeps::default());
        actor.tenant_id = "t-swapkill".into();
        actor.runtime.test_set_state(TenantState::Ready);
        actor.preflight_ok_digest = Some("d".into());
        let old_dir = tmpdir("swapkill-old");
        actor.active = Some(fake_live_gen(&old_dir, wall_now_ms() + 120_000).await);
        let mut warm = fake_warm_gen(&dir, true).await;
        let slot = Arc::new(EndpointSlot {
            endpoint: WorkerEndpoint::Unix(sock_dir.join("early.sock")),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            drain_notify: actor.runtime.drain_notify.clone(),
        });
        warm.early.routing = Arc::new(BTreeMap::from([("w0".to_string(), slot.clone())]));
        // The execve'd image fatals on reload — same shape, cheaper:
        // SIGKILL the warm child so the marker wait observes the exit.
        signal_process_group(warm.gen.pid, libc::SIGKILL);
        tokio::time::sleep(Duration::from_millis(60)).await;
        actor.prewarm = Some(prewarm_ctx(&dir, 4, Some(warm)));
        let desired_dir = tmpdir("swapkill-desired");
        let mut desired = test_spec(&desired_dir);
        desired.do_storage = Some(crate::spec::DoStorageSpec {
            tenant_dir: desired_dir.join("do"),
            worker_classes: Default::default(),
            transfer_sources: Default::default(),
        });
        actor.desired_spec = Some(Box::new(desired.clone()));
        actor.launch_after_preflight(Box::new(desired.clone()), &desired);
        assert!(actor.runtime.routing.load().contains_key("w0"));
        actor.on_exited(1);
        let p = actor.pending.as_ref().expect("swap dispatched");
        assert!(p.prewarm_swap);
        let seq = p.seq;
        let ev = tokio::time::timeout(Duration::from_secs(10), actor.ev_rx.recv())
            .await
            .expect("swap task finishes fast on the dead child")
            .expect("event received");
        let Ev::Launched { seq: ev_seq, result } = ev else {
            panic!("expected Ev::Launched");
        };
        assert_eq!(ev_seq, seq);
        let err = result.err().expect("dead child fails the swap");
        assert_eq!(err.reason, "swap-child-exit", "{err:?}");
        actor.on_ev(Ev::Launched {
            seq,
            result: Err(err),
        });
        assert!(actor.pending.is_none());
        assert!(actor.prewarm.is_none());
        assert!(actor.prewarm_burned);
        assert!(
            actor.runtime.routing.load().is_empty(),
            "early routes revoked on the child-exit swap failure"
        );
        assert!(slot.is_closed(), "revoked slot refuses new acquisitions");
        assert!(!actor.runtime.switch_gate.active(), "gate ended");
        assert!(!actor.runtime.serving());
        assert!(actor.retry_at.is_some(), "bounded relaunch armed");
        assert!(!dir.exists(), "spent warm gen dir retired");
        assert!(
            actor
                .runtime
                .last_error()
                .unwrap_or_default()
                .contains("swap-child-exit"),
            "typed reason reaches last_error"
        );
    }

    // ---- MIG-17: serialized-switch lease carry + bounded pipeline ----

    /// Counting placement — the carry tests assert `reaffirm` (not
    /// `acquire`) consumed the lease and the exit path never fenced.
    #[derive(Default)]
    struct PipeSpy {
        acquires: AtomicUsize,
        reaffirms: AtomicUsize,
        claims: AtomicUsize,
        fences: AtomicUsize,
        renew_err: std::sync::Mutex<Option<&'static str>>,
    }
    impl DoPlacement for PipeSpy {
        fn authoritative(&self) -> bool {
            true
        }
        fn renew_margin_ms(&self) -> u64 {
            60_000
        }
        fn acquire(
            &self,
            _s: &crate::do_placement::PlacementScope,
        ) -> Result<crate::do_placement::PlacementLease, PlacementError> {
            self.acquires.fetch_add(1, Ordering::SeqCst);
            Ok(crate::do_placement::PlacementLease {
                epoch: PlacementEpoch(9),
                expires_at_ms: u64::MAX / 2,
            })
        }
        fn reaffirm(
            &self,
            _s: &crate::do_placement::PlacementScope,
            expected: PlacementEpoch,
        ) -> Result<crate::do_placement::PlacementLease, PlacementError> {
            self.reaffirms.fetch_add(1, Ordering::SeqCst);
            Ok(crate::do_placement::PlacementLease {
                epoch: expected,
                expires_at_ms: u64::MAX / 2,
            })
        }
        fn renew(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<u64, PlacementError> {
            match *self.renew_err.lock().unwrap() {
                Some("fenced") => Err(PlacementError::Fenced {
                    current: 9,
                    until_ms: 0,
                }),
                Some("store") => Err(PlacementError::Store(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "store down",
                ))),
                _ => Ok(u64::MAX / 2),
            }
        }
        fn fence(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            self.fences.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn claim_writer(
            &self,
            _s: &crate::do_placement::PlacementScope,
            _e: PlacementEpoch,
        ) -> Result<(), PlacementError> {
            self.claims.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn mig17_scope(
        tenant_dir: &Path,
        worker: &str,
        class_key: &str,
    ) -> crate::do_placement::PlacementScope {
        crate::do_placement::PlacementScope {
            tenant: "t".into(),
            worker: worker.into(),
            class_key: class_key.into(),
            class_dir: tenant_dir.join(worker).join(class_key),
        }
    }

    fn mig17_push_drain(
        actor: &mut Actor,
        live: LiveGen,
        do_switch: bool,
        leases: Vec<DoLeaseEntry>,
    ) {
        actor.draining.push(DrainingGen {
            id: live.id,
            pid: live.pid,
            slots: live.slots.clone(),
            entered_at: Instant::now(),
            deadline: Instant::now() + Duration::from_secs(60),
            sigterm_at: Some(Instant::now()),
            killed: false,
            watcher: live.watcher,
            work_dir: live.work_dir.clone(),
            had_do_storage: true,
            do_switch,
            leases,
            waituntil: None,
            wu_idle_since: None,
            wu_last: None,
            wu_probe: None,
            wu_next_at: Instant::now(),
        });
    }

    /// Serialized switch: the exiting gen's lease for a class still
    /// desired under the SAME worker is carried — never fenced at
    /// exit, `reaffirm`ed (not re-acquired) at consume, epoch kept.
    #[tokio::test]
    async fn mig17_switch_carry_reaffirms_same_worker_lease() {
        let dir = tmpdir("mig17-carry");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "a1b2".repeat(16);
        // A non-empty class dir so the genesis WAL arm skips replay —
        // the carry test only exercises acquire/claim paths.
        let class_dir = tenant_dir.join("w").join(&leased);
        std::fs::create_dir_all(&class_dir).unwrap();
        std::fs::write(class_dir.join("data.sqlite"), b"x").unwrap();
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        let expiry = wall_now_ms() + 60_000;
        let live = fake_live_gen(&dir, expiry).await;
        let gen_id = live.id;
        mig17_push_drain(
            &mut actor,
            live,
            true,
            vec![DoLeaseEntry {
                scope: mig17_scope(&tenant_dir, "w", &leased),
                epoch: PlacementEpoch(7),
                expires_at_ms: expiry,
            }],
        );
        actor.do_wait_since = Some(Instant::now());
        actor.on_exited(gen_id);
        assert!(
            actor.carried_leases.is_empty(),
            "carry consumed by the pending launch"
        );
        assert_eq!(spy.fences.load(Ordering::SeqCst), 0, "never fenced at exit");
        assert_eq!(spy.reaffirms.load(Ordering::SeqCst), 1);
        assert_eq!(spy.acquires.load(Ordering::SeqCst), 0, "no re-acquire");
        assert_eq!(spy.claims.load(Ordering::SeqCst), 1, "claim still ran");
        let pending = actor.pending.as_ref().expect("launch attempted");
        assert_eq!(pending.leases.len(), 1);
        assert_eq!(pending.leases[0].epoch.0, 7, "carried epoch kept");
        assert!(actor.retry_at.is_none(), "clean consume — no retry");
    }

    /// A carried lease whose renew fails inside the carry window is
    /// VOIDED (fenced best-effort + `carried_lost`); the launch then
    /// fails closed at the acquire stage — no launch, +5 s retry.
    #[tokio::test]
    async fn mig17_carried_renew_failure_voids_carry_fails_closed() {
        let dir = tmpdir("mig17-carry-void");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "c4d5".repeat(16);
        let class_dir = tenant_dir.join("w").join(&leased);
        std::fs::create_dir_all(&class_dir).unwrap();
        std::fs::write(class_dir.join("data.sqlite"), b"x").unwrap();
        let spy = Arc::new(PipeSpy::default());
        *spy.renew_err.lock().unwrap() = Some("store");
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        actor.carried_leases.push(DoLeaseEntry {
            scope: mig17_scope(&tenant_dir, "w", &leased),
            epoch: PlacementEpoch(7),
            // Inside the spy's 60 s renew margin → due NOW.
            expires_at_ms: wall_now_ms() + 1_000,
        });
        actor.service_do_leases(Instant::now());
        assert!(actor.carried_leases.is_empty(), "carry voided");
        assert!(
            actor
                .carried_lost
                .contains(&("w".to_string(), leased.clone())),
            "lost pair recorded for the consume"
        );
        assert_eq!(spy.fences.load(Ordering::SeqCst), 1, "voided carry fenced");
        actor.start_pending_launch(false);
        assert!(actor.pending.is_none(), "no launch on a lost carry");
        assert!(actor.pending_do_leases.is_empty());
        assert_eq!(
            actor.runtime.last_error().as_deref(),
            Some("do-placement-unavailable")
        );
        assert!(actor.retry_at.is_some(), "+5 s retry armed");
        assert_eq!(
            spy.acquires.load(Ordering::SeqCst),
            0,
            "lost carry never re-acquired"
        );
    }

    /// Regression: a non-switch draining exit still releases every
    /// lease — the carry gate requires `do_switch` AND the switch wait.
    #[tokio::test]
    async fn mig17_non_switch_exit_releases_everything() {
        let dir = tmpdir("mig17-nonswitch");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "e6f7".repeat(16);
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        let live = fake_live_gen(&dir, wall_now_ms() + 60_000).await;
        let gen_id = live.id;
        mig17_push_drain(
            &mut actor,
            live,
            false,
            vec![DoLeaseEntry {
                scope: mig17_scope(&tenant_dir, "w", &leased),
                epoch: PlacementEpoch(3),
                expires_at_ms: wall_now_ms() + 60_000,
            }],
        );
        actor.on_exited(gen_id);
        assert_eq!(spy.fences.load(Ordering::SeqCst), 1, "classic release");
        assert!(actor.carried_leases.is_empty());
    }

    /// A carry the desired moved away from before the launch consumes
    /// it takes the normal departing path — never a stale reaffirm.
    #[tokio::test]
    async fn mig17_carry_leftover_departing_when_desired_moved_on() {
        let dir = tmpdir("mig17-leftover");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "aabb".repeat(16);
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        actor.carried_leases.push(DoLeaseEntry {
            scope: mig17_scope(&tenant_dir, "w", &leased),
            epoch: PlacementEpoch(4),
            expires_at_ms: wall_now_ms() + 60_000,
        });
        // Desired drops the class entirely before the launch runs.
        actor
            .desired_spec
            .as_mut()
            .unwrap()
            .do_storage
            .as_mut()
            .unwrap()
            .worker_classes
            .clear();
        actor.start_pending_launch(false);
        assert!(actor.carried_leases.is_empty(), "no stale carry survives");
        assert!(actor.carried_lost.is_empty());
        assert_eq!(
            spy.reaffirms.load(Ordering::SeqCst),
            0,
            "dropped class never reaffirmed"
        );
    }

    /// A transferred class (same key under a DIFFERENT worker) is not
    /// carried — it stays on the departing path exactly as before.
    #[tokio::test]
    async fn mig17_transferred_class_not_carried() {
        let dir = tmpdir("mig17-xfer");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "bbcc".repeat(16);
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        // Desired wants the key under "w2"; the draining gen held "w".
        actor
            .desired_spec
            .as_mut()
            .unwrap()
            .do_storage
            .as_mut()
            .unwrap()
            .worker_classes = [("w2".to_string(), vec![leased.clone()])]
            .into_iter()
            .collect();
        let live = fake_live_gen(&dir, wall_now_ms() + 60_000).await;
        let gen_id = live.id;
        mig17_push_drain(
            &mut actor,
            live,
            true,
            vec![DoLeaseEntry {
                scope: mig17_scope(&tenant_dir, "w", &leased),
                epoch: PlacementEpoch(5),
                expires_at_ms: wall_now_ms() + 60_000,
            }],
        );
        actor.do_wait_since = Some(Instant::now());
        actor.on_exited(gen_id);
        assert!(
            actor.carried_leases.is_empty(),
            "a transferred class departs, never carries"
        );
    }

    /// MIG-17-r2 (H2): a freeze request landing INSIDE the carry
    /// window must not leave the carried lease held — the frozen
    /// class's pending launch aborts and the carry drains through
    /// the normal release path.
    #[tokio::test]
    async fn mig17r2_freeze_during_carry_releases_carried_lease() {
        let dir = tmpdir("mig17-freeze");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "dd01".repeat(16);
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        actor.carried_leases.push(DoLeaseEntry {
            scope: mig17_scope(&tenant_dir, "w", &leased),
            epoch: PlacementEpoch(7),
            expires_at_ms: wall_now_ms() + 60_000,
        });
        actor.carried_lost.insert(("w2".to_string(), "ef23".repeat(16)));
        actor.runtime.request_do_freeze(leased.clone());
        actor.service_do_freeze();
        assert!(actor.carried_leases.is_empty(), "carry drained on freeze");
        assert!(
            actor.carried_lost.is_empty(),
            "voided-carry bookkeeping cleared with it"
        );
        assert_eq!(
            spy.fences.load(Ordering::SeqCst),
            1,
            "carried lease fenced — the freeze semantics reach it"
        );
    }

    /// MIG-17-r2 (H2): `abort_pending` also retires the carry — a
    /// lease parked for the aborted attempt never survives it.
    #[tokio::test]
    async fn mig17r2_abort_pending_releases_carried_lease() {
        let dir = tmpdir("mig17-abort");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "ee02".repeat(16);
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        actor.carried_leases.push(DoLeaseEntry {
            scope: mig17_scope(&tenant_dir, "w", &leased),
            epoch: PlacementEpoch(9),
            expires_at_ms: wall_now_ms() + 60_000,
        });
        actor.carried_lost.insert(("w3".to_string(), "ab12".repeat(16)));
        actor.abort_pending();
        assert!(actor.carried_leases.is_empty());
        assert!(actor.carried_lost.is_empty());
        assert_eq!(spy.fences.load(Ordering::SeqCst), 1);
    }

    /// MIG-17-r3 (marker cleanup): a class departing this cell drops
    /// its `wal-replay-pending` sibling — a stale marker must never
    /// bind a later same-name class on this worker into a replay it
    /// doesn't need. Cleared at the departure decision, before any
    /// final-snapshot work (so it lands even on the held-for-retry
    /// path).
    #[tokio::test]
    async fn mig17r3_departing_clears_replay_pending_marker() {
        let dir = tmpdir("mig17-marker-clean");
        let tenant_dir = dir.join("do-t");
        let tomb_dir = dir.join("wf-del");
        let leased = "ab03".repeat(16);
        let class_dir = tenant_dir.join("w").join(&leased);
        std::fs::create_dir_all(&class_dir).unwrap();
        std::fs::write(class_dir.join("data.sqlite"), b"x").unwrap();
        let spy = Arc::new(PipeSpy::default());
        let (mut actor, _wal) =
            launch_actor(spy.clone(), &dir, &tenant_dir, &tomb_dir, &leased);
        let sc = mig17_scope(&tenant_dir, "w", &leased);
        crate::do_snapshot::mark_replay_pending(
            &sc,
            crate::do_snapshot::ReplayPending {
                snap_epoch: 1,
                snap_seq: 2,
                writer_epoch: 7,
            },
        )
        .unwrap();
        let marker = crate::do_snapshot::sibling_replay_pending_file(&sc);
        assert!(marker.exists(), "marker planted");
        // Desired drops the class → it departs this cell.
        let mut spec = *actor.desired_spec.take().unwrap();
        spec.do_storage.as_mut().unwrap().worker_classes = Default::default();
        actor.desired_spec = Some(Box::new(spec));
        let _hold = actor.final_snapshot_departing(&[DoLeaseEntry {
            scope: sc.clone(),
            epoch: PlacementEpoch(7),
            expires_at_ms: wall_now_ms() + 60_000,
        }]);
        assert!(
            !marker.exists(),
            "departure cleared the replay-pending marker"
        );
    }
}

