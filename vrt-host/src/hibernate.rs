//! Tenant-level scale to zero (P0): typed idle policy, hibernation
//! blockers and the wake machinery.
//!
//! Design: `doc/platform/workers-runtime-scale-to-zero.md`.
//!
//! This module holds the REUSABLE mechanisms; the supervisor actor only
//! orchestrates the fixed lifecycle stages (evaluate → admission check →
//! quiesce → commit check → release → wake):
//!
//! - [`IdlePolicy`] answers "does the tenant WANT to sleep" (pure);
//! - [`HibernationBlocker`] answers "MAY it sleep right now" — closed
//!   [`BlockReason`] enum, fail closed (an empty chain blocks);
//! - [`WakeSourceKind`] is the closed set of wake triggers (HTTP, cron);
//! - [`WakeFlight`] is the per-tenant single-flight: N concurrent
//!   requests share ONE launch, cancel-safe (the actor owns the work, a
//!   disconnecting client only drops its receiver);
//! - [`WakeCoordinator`] is the host-wide wake cap with a fair FIFO
//!   queue.
//!
//! Nothing here touches processes or sockets; every type is unit-testable
//! without a supervisor. No untyped JSON, no string events.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};

// ======================= parameters =======================

/// Effective scale-to-zero parameters (host-local — NOT part of the
/// signed snapshot or the desired spec). `None` in
/// `SupervisorParams::scale_to_zero` means the feature is OFF and the
/// supervisor behaves exactly as before.
#[derive(Clone)]
pub struct ScaleToZero {
    /// "Want to sleep" strategy (default [`FixedWindowPolicy`]).
    pub policy: Arc<dyn IdlePolicy>,
    /// `Some` when `policy` is an [`AutoPolicy`]: the actor keeps one
    /// [`AutoState`] per tenant with these params and feeds it
    /// [`AutoFacts`] at every evaluation.
    pub auto: Option<AutoParams>,
    /// Longest a tenant may stay hibernated before the host wakes it
    /// itself ([`WakeSourceKind::MaxSleep`]) — bounds spec/alarm-index
    /// staleness. `None` = sleep indefinitely.
    pub max_sleep: Option<Duration>,
    /// "May sleep" chain (default [`BlockerChain::p0`]).
    pub blockers: Arc<BlockerChain>,
    /// Host-wide wake cap + fair queue (shared by every tenant actor).
    pub coordinator: Arc<WakeCoordinator>,
    /// Bound on quiescing (in-flight + waitUntil) before the attempt is
    /// abandoned and the tenant returns to `Ready`. Never force-kills.
    pub drain_timeout: Duration,
    /// Bound on one whole wake (queue + launch + ready probe).
    pub wake_deadline: Duration,
    /// How long to wait before re-evaluating a tenant a blocker refused.
    pub blocked_recheck: Duration,
    /// Cadence at which quiescing polls in-flight counts.
    pub quiesce_poll: Duration,
    /// How long an alarm wake holds the gate unsafe after Ready.
    pub alarm_wake_grace: Duration,
}

impl fmt::Debug for ScaleToZero {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScaleToZero")
            .field("drain_timeout", &self.drain_timeout)
            .field("wake_deadline", &self.wake_deadline)
            .field("blocked_recheck", &self.blocked_recheck)
            .finish_non_exhaustive()
    }
}

// ======================= retry backoff =======================

/// Floor of the backoff ceiling: the delay never grows past
/// `max(base, BACKOFF_CAP_FLOOR)` (a `base` above the floor is itself
/// the ceiling, so the backoff never retries sooner than configured).
pub const BACKOFF_CAP_FLOOR: Duration = Duration::from_secs(30 * 60);

/// Exponential backoff for consecutive non-traffic hibernation aborts
/// (commit-phase blocks: waitUntil pending/unknown, in-flight at commit,
/// probe/drain timeouts). Each such attempt closes admission, so a
/// tenant whose background work never ends must not be retried at the
/// flat `blocked_recheck` cadence. Pure data; the actor resets it on
/// commit, wake, new generation, apply and on traffic aborts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HibernateRetryBackoff {
    consecutive: u32,
}

impl HibernateRetryBackoff {
    /// Consecutive non-traffic aborts since the last reset.
    pub fn consecutive(&self) -> u32 {
        self.consecutive
    }

    /// Record one more blocked abort; returns the delay before the next
    /// attempt: `base * 2^(consecutive-1)`, saturating, capped at
    /// `max(base, BACKOFF_CAP_FLOOR)`.
    pub fn on_blocked_abort(&mut self, base: Duration) -> Duration {
        self.consecutive = self.consecutive.saturating_add(1);
        let cap = base.max(BACKOFF_CAP_FLOOR);
        let shift = (self.consecutive - 1).min(63);
        let mult = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        base.saturating_mul(mult).min(cap)
    }

    pub fn reset(&mut self) {
        self.consecutive = 0;
    }
}

// ======================= idle policy =======================

/// Pure inputs of an idle decision.
#[derive(Clone, Copy, Debug)]
pub struct IdleInputs {
    /// Decision time.
    pub now: Instant,
    /// Last request activity (arrival, or last time in-flight was seen).
    pub last_activity: Instant,
    /// When the current generation became `Ready` (wake or boot).
    pub ready_since: Instant,
    /// Per-tenant auto-policy facts; `Some` only under
    /// `[scaleToZero.auto]`. Other policies ignore it.
    pub auto: Option<AutoFacts>,
}

/// Verdict of an [`IdlePolicy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleVerdict {
    /// Never hibernate (policy disabled for this tenant).
    Keep,
    /// The tenant wants to sleep now.
    Hibernate,
    /// Not yet — re-evaluate at this instant.
    DeferUntil(Instant),
}

/// "Does the tenant WANT to sleep?" — a pure function, no side effects.
/// It never answers "can it" (that is [`HibernationBlocker`]).
pub trait IdlePolicy: Send + Sync {
    /// Evaluate one tenant.
    fn evaluate(&self, inputs: &IdleInputs) -> IdleVerdict;
}

/// Default policy: idle for `idle_window` AND resident for at least
/// `min_residency` (anti-flap). A zero `idle_window` means Keep.
#[derive(Clone, Copy, Debug)]
pub struct FixedWindowPolicy {
    /// Required idle time.
    pub idle_window: Duration,
    /// Minimum time a generation stays resident after becoming Ready.
    pub min_residency: Duration,
}

impl IdlePolicy for FixedWindowPolicy {
    fn evaluate(&self, i: &IdleInputs) -> IdleVerdict {
        if self.idle_window.is_zero() {
            return IdleVerdict::Keep;
        }
        let idle_at = i.last_activity + self.idle_window;
        let resident_at = i.ready_since + self.min_residency;
        let at = idle_at.max(resident_at);
        if i.now >= at {
            IdleVerdict::Hibernate
        } else {
            IdleVerdict::DeferUntil(at)
        }
    }
}

// ======================= automatic policy (stage 4) =======================

/// Tunables of the automatic idle policy (`[scaleToZero.auto]`).
/// `AutoPolicy` (the shared [`IdlePolicy`]) decides from
/// [`AutoFacts`]; `AutoState` (per tenant, actor-owned) produces those
/// facts from observed sleep/wake history.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AutoParams {
    /// Adaptation floor: the configured idle window. The policy never
    /// sleeps a tenant sooner than the operator declared.
    pub floor: Duration,
    /// Adaptation ceiling.
    pub ceiling: Duration,
    /// A committed sleep shorter than this was not worth the cold
    /// start: the window doubles (bounded by `ceiling`).
    pub premature_wake: Duration,
    /// A committed sleep at least this long means the tenant was
    /// deeply idle and could have slept sooner: the window halves
    /// (bounded by `floor`). Must exceed `premature_wake` so
    /// adaptation cannot oscillate between grow and shrink.
    pub sound_sleep: Duration,
    /// Successful wakes allowed inside `wake_budget_window` before
    /// the tenant is kept resident (0 = unlimited). A thrashing
    /// tenant stays awake — the budget denies SLEEP, never traffic.
    pub wake_budget: u32,
    /// Sliding window the wake budget is counted over.
    pub wake_budget_window: Duration,
}

/// Facts the actor feeds [`AutoPolicy`] through [`IdleInputs::auto`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AutoFacts {
    /// The tenant's currently adapted idle window.
    pub window: Duration,
    /// `Some(t)` when the wake budget is exhausted: the tenant stays
    /// resident until `t` (the instant enough counted wakes age out
    /// of `wake_budget_window`).
    pub budget_refill_at: Option<Instant>,
}

/// Per-tenant bookkeeping behind [`AutoPolicy`]. Lives on the actor —
/// it describes the tenant's traffic shape and therefore survives
/// generations, sleeps and wakes (it is reset only with the tenant).
pub struct AutoState {
    params: AutoParams,
    window: Duration,
    /// When the current/last committed sleep began (set at
    /// `commit_hibernate`, consumed by `record_wake`).
    slept_at: Option<Instant>,
    /// Successful-wake timestamps inside `wake_budget_window`
    /// (oldest first).
    wakes: VecDeque<Instant>,
}

/// Window adaptation step: premature sleeps double it, sound sleeps
/// halve it. Fixed factors keep the policy predictable — the operator
/// tunes the thresholds, not the gain.
const AUTO_GROW: u32 = 2;
const AUTO_SHRINK: u32 = 2;

impl AutoState {
    /// New state at the adaptation floor.
    pub fn new(params: AutoParams) -> Self {
        Self {
            params,
            window: params.floor,
            slept_at: None,
            wakes: VecDeque::new(),
        }
    }

    /// Mark the start of a committed hibernation.
    pub fn record_sleep_commit(&mut self, now: Instant) {
        self.slept_at = Some(now);
    }

    /// Adapt the window from the just-ended sleep and count the wake
    /// against the budget. A wake with no committed sleep (the tenant
    /// was drained/applied instead of hibernated) only counts toward
    /// the budget.
    pub fn record_wake(&mut self, now: Instant) {
        if let Some(slept_at) = self.slept_at.take() {
            let slept = now.saturating_duration_since(slept_at);
            if slept < self.params.premature_wake {
                self.window = (self.window * AUTO_GROW).min(self.params.ceiling);
            } else if slept >= self.params.sound_sleep {
                self.window = (self.window / AUTO_SHRINK).max(self.params.floor);
            }
        }
        self.wakes.push_back(now);
        self.prune(now);
    }

    /// A hibernation attempt that aborted never became a sleep.
    pub fn clear_sleep(&mut self) {
        self.slept_at = None;
    }

    fn prune(&mut self, now: Instant) {
        // Underflow (now < window from the monotonic epoch) means no
        // instant can be before the horizon — nothing to drop.
        let Some(horizon) = now.checked_sub(self.params.wake_budget_window) else {
            return;
        };
        while self.wakes.front().is_some_and(|t| *t < horizon) {
            self.wakes.pop_front();
        }
    }

    /// Counted wakes remaining before the budget gates (u32::MAX when
    /// the budget is unlimited).
    pub fn budget_remaining(&mut self, now: Instant) -> u32 {
        self.prune(now);
        if self.params.wake_budget == 0 {
            return u32::MAX;
        }
        self.params
            .wake_budget
            .saturating_sub(self.wakes.len().min(u32::MAX as usize) as u32)
    }

    /// Snapshot for [`AutoPolicy::evaluate`].
    pub fn facts(&mut self, now: Instant) -> AutoFacts {
        self.prune(now);
        let budget = self.params.wake_budget as usize;
        let budget_refill_at = (budget > 0 && self.wakes.len() >= budget).then(|| {
            // To fall below `budget`, `len - budget + 1` counted wakes
            // must age out: the refill instant is when the
            // `len - budget`-th oldest wake exits the window.
            self.wakes[self.wakes.len() - budget] + self.params.wake_budget_window
        });
        AutoFacts {
            window: self.window,
            budget_refill_at,
        }
    }

    /// Current adapted window (diagnostics / metrics).
    pub fn window(&self) -> Duration {
        self.window
    }
}

/// Stage-4 policy: the fixed-window rule over a per-tenant adapted
/// window, gated by the wake budget. Shared per host like
/// [`FixedWindowPolicy`]; the per-tenant state arrives via
/// [`IdleInputs::auto`].
#[derive(Clone, Copy, Debug)]
pub struct AutoPolicy {
    /// Adaptation/budget tunables.
    pub params: AutoParams,
    /// Anti-flap residency floor (same meaning as
    /// [`FixedWindowPolicy::min_residency`]).
    pub min_residency: Duration,
}

impl IdlePolicy for AutoPolicy {
    fn evaluate(&self, i: &IdleInputs) -> IdleVerdict {
        // No facts means the actor does not track auto state — keep the
        // tenant rather than sleep without the inputs the verdict needs.
        let Some(auto) = i.auto else {
            return IdleVerdict::Keep;
        };
        if let Some(refill) = auto.budget_refill_at {
            // Budget exhausted: stay resident until it refills. Not
            // `Keep` — the budget does refill.
            return IdleVerdict::DeferUntil(refill);
        }
        let idle_at = i.last_activity + auto.window;
        let resident_at = i.ready_since + self.min_residency;
        let at = idle_at.max(resident_at);
        if i.now >= at {
            IdleVerdict::Hibernate
        } else {
            IdleVerdict::DeferUntil(at)
        }
    }
}

// ======================= blockers =======================

/// Closed set of reasons a tenant may not hibernate. Used as a metric
/// label and a log field — never a free string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockReason {
    /// Requests (including streaming bodies) are in flight.
    InFlight,
    /// `ctx.waitUntil` work is pending.
    WaitUntilPending,
    /// The waitUntil probe failed — count unknown (fail closed).
    WaitUntilUnknown,
    /// Tenant declares Durable Objects (alarms cannot wake the host).
    StructuralDurableObject,
    /// Tenant declares cron schedules. NO LONGER PRODUCED: a cron
    /// trigger wakes a hibernated tenant ([`WakeSourceKind::Cron`]), so
    /// cron is not structural. Kept so the metric/log label and any
    /// persisted/rolling-upgrade consumers stay stable.
    StructuralCron,
    /// Tenant consumes queues. NO LONGER PRODUCED: a queue dispatch
    /// wakes a hibernated tenant ([`WakeSourceKind::Queue`]), so queue
    /// consumers are not structural. Kept so the metric/log label stays
    /// stable.
    StructuralQueueConsumer,
    /// Tenant declares workflows (the engine is a DO whose alarms
    /// cannot wake the host; wfops wakes via [`WakeSourceKind::Workflow`]
    /// but a workflow tenant is never put to sleep).
    StructuralWorkflow,
    /// The blocker chain is empty — misconfiguration, fail closed.
    NoBlockerConfigured,
}

impl BlockReason {
    /// Stable metric/log label.
    pub fn label(self) -> &'static str {
        match self {
            Self::InFlight => "in-flight",
            Self::WaitUntilPending => "waituntil-pending",
            Self::WaitUntilUnknown => "waituntil-probe-failed",
            Self::StructuralDurableObject => "structural-do",
            Self::StructuralCron => "structural-cron",
            Self::StructuralQueueConsumer => "structural-queue",
            Self::StructuralWorkflow => "structural-workflow",
            Self::NoBlockerConfigured => "no-blocker",
        }
    }

    /// Structural blocks cannot clear without a redeploy — callers back
    /// off longer instead of re-polling.
    pub fn is_structural(self) -> bool {
        matches!(
            self,
            Self::StructuralDurableObject
                | Self::StructuralCron
                | Self::StructuralQueueConsumer
                | Self::StructuralWorkflow
        )
    }
}

/// When in the hibernation lifecycle a blocker runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockPhase {
    /// Before the tenant stops admitting traffic (cheap, in-memory).
    Admission,
    /// After quiescing, with the waitUntil probe verdict, right before
    /// the process is released.
    Commit,
}

/// Facts derived from the tenant's declared bindings (typed — never
/// guessed from strings).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StructuralFacts {
    /// Declares DO storage.
    pub durable_objects: bool,
    /// Declares cron schedules. Informational only — cron triggers wake
    /// a hibernated tenant, so this never blocks hibernation.
    pub cron: bool,
    /// Declares queue consumers. Informational only — a queue dispatch
    /// wakes a hibernated tenant, so this never blocks hibernation.
    pub queue_consumers: bool,
    /// Declares workflows.
    pub workflows: bool,
    /// The cell alarm index has confirmed every DO class is fully scanned and
    /// no alarm is due inside the safety lead. Default `false` (fail closed).
    pub alarm_safe: bool,
}

/// waitUntil accounting at commit time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitUntilFact {
    /// Not probed yet (admission phase).
    NotProbed,
    /// The generation has no waitUntil accounting endpoint; the same
    /// semantics as the existing drain (SIGTERM at HTTP idle).
    NoAccounting,
    /// Probe says zero pending.
    Idle,
    /// Probe says `n > 0` pending.
    Pending(u64),
    /// Probe failed — unknown.
    Unknown,
}

/// Read-only typed view handed to blockers — no supervisor internals.
#[derive(Clone, Copy, Debug)]
pub struct BlockContext {
    /// Sum of in-flight requests across every slot of the generation.
    pub in_flight: usize,
    /// Declared-binding facts.
    pub structural: StructuralFacts,
    /// waitUntil accounting (commit phase only).
    pub wait_until: WaitUntilFact,
}

/// "MAY the tenant sleep right now?" Failure semantics are FIXED: a
/// blocker that cannot decide returns `Some(reason)` (deny).
pub trait HibernationBlocker: Send + Sync {
    /// Stable name (logs / docs).
    fn name(&self) -> &'static str;
    /// Phase this blocker runs in.
    fn phase(&self) -> BlockPhase;
    /// `Some(reason)` = deny.
    fn check(&self, ctx: &BlockContext) -> Option<BlockReason>;
}

/// Denies while any request is in flight (admission AND commit — a
/// request can race the quiesce).
#[derive(Clone, Copy, Debug)]
pub struct InFlightBlocker(pub BlockPhase);

impl HibernationBlocker for InFlightBlocker {
    fn name(&self) -> &'static str {
        "in-flight"
    }
    fn phase(&self) -> BlockPhase {
        self.0
    }
    fn check(&self, ctx: &BlockContext) -> Option<BlockReason> {
        (ctx.in_flight > 0).then_some(BlockReason::InFlight)
    }
}

/// Denies on pending or unknown waitUntil work (commit phase).
#[derive(Clone, Copy, Debug)]
pub struct WaitUntilBlocker;

impl HibernationBlocker for WaitUntilBlocker {
    fn name(&self) -> &'static str {
        "waituntil"
    }
    fn phase(&self) -> BlockPhase {
        BlockPhase::Commit
    }
    fn check(&self, ctx: &BlockContext) -> Option<BlockReason> {
        match ctx.wait_until {
            WaitUntilFact::Idle | WaitUntilFact::NoAccounting => None,
            WaitUntilFact::Pending(_) => Some(BlockReason::WaitUntilPending),
            // Commit with no verdict is a supervisor bug — deny.
            WaitUntilFact::Unknown | WaitUntilFact::NotProbed => {
                Some(BlockReason::WaitUntilUnknown)
            }
        }
    }
}

/// P0: refuses tenants whose bindings the host cannot wake (DO alarms)
/// or has no wake hook for yet (cron / queue / workflow). Later phases
/// split this into one blocker per capability instead of touching the
/// supervisor.
#[derive(Clone, Copy, Debug)]
pub struct StructuralBlocker;

impl HibernationBlocker for StructuralBlocker {
    fn name(&self) -> &'static str {
        "structural"
    }
    fn phase(&self) -> BlockPhase {
        BlockPhase::Admission
    }
    fn check(&self, ctx: &BlockContext) -> Option<BlockReason> {
        let s = ctx.structural;
        if s.durable_objects && !s.alarm_safe {
            Some(BlockReason::StructuralDurableObject)
        } else if s.workflows {
            Some(BlockReason::StructuralWorkflow)
        } else {
            None
        }
    }
}

/// Ordered blocker list (explicit order: cheap in-memory checks first).
pub struct BlockerChain {
    blockers: Vec<Box<dyn HibernationBlocker>>,
}

impl BlockerChain {
    /// Chain from an explicit ordered list.
    pub fn new(blockers: Vec<Box<dyn HibernationBlocker>>) -> Self {
        Self { blockers }
    }

    /// The P0 chain: structural → in-flight → (commit) in-flight →
    /// waitUntil.
    pub fn p0() -> Self {
        Self::new(vec![
            Box::new(StructuralBlocker),
            Box::new(InFlightBlocker(BlockPhase::Admission)),
            Box::new(InFlightBlocker(BlockPhase::Commit)),
            Box::new(WaitUntilBlocker),
        ])
    }

    /// First denial in `phase`, in registration order. An empty chain
    /// denies (fail closed).
    pub fn first_block(&self, phase: BlockPhase, ctx: &BlockContext) -> Option<BlockReason> {
        if self.blockers.is_empty() {
            return Some(BlockReason::NoBlockerConfigured);
        }
        self.blockers
            .iter()
            .filter(|b| b.phase() == phase)
            .find_map(|b| b.check(ctx))
    }

    /// Number of registered blockers.
    pub fn len(&self) -> usize {
        self.blockers.len()
    }

    /// Whether the chain is empty (always denies).
    pub fn is_empty(&self) -> bool {
        self.blockers.is_empty()
    }
}

// ======================= wake sources =======================

/// Queue wake wait ceiling. The wake duration is deducted from the
/// facade's 55s delivery budget (`x-vrt-delivery-budget-ms`), so a 20s
/// cap leaves the consumer handler at least 35s.
pub const QUEUE_WAKE_MAX_WAIT: Duration = Duration::from_secs(20);

/// Workflow-op (wfops) wake wait ceiling. The cell-controller's wfops
/// call has a 15s per-call timeout and blindly retries `request
/// timeout` / transport errors once; if the host were still waking at
/// that point the op could execute twice. 8s leaves >= 6s for the
/// forwarded op so a typed result is returned before 15s.
pub const WORKFLOW_WAKE_MAX_WAIT: Duration = Duration::from_secs(8);


/// Closed set of wake triggers. `Http`, `Cron`, `Queue`, `Workflow` and
/// `Alarm` are all implemented; never a string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WakeSourceKind {
    /// An ingress HTTP request.
    Http,
    /// A cell-controller cron trigger (`/v1/cron/trigger`).
    Cron,
    /// A cell-controller queue dispatch (`/v1/queue/dispatch`).
    Queue,
    /// A cell-controller DO alarm wake (`/v1/do-alarm/wake`). The tenant
    /// already holds the alarm in workerd's class `metadata.sqlite`; starting
    /// the generation delivers it.
    Alarm,
    /// A client WebSocket frame arrived on a detached hibernated session;
    /// the ingress wakes the tenant and reattaches the tunnel.
    WebSocket,
    /// A cell-controller workflow op (`/v1/wfops`). The workflow engine
    /// is a DO inside workerd, so workflow tenants never hibernate
    /// (`StructuralWorkflow` stays); this wakes a tenant that slept
    /// BEFORE a workflow was added (lazy Apply) or any wfops that lands
    /// on a sleeping tenant.
    Workflow,
    /// The host's own max-sleep deadline (`[scaleToZero.auto]
    /// maxSleepMs`): the tenant has been asleep long enough that its
    /// spec/alarm-index view may be stale — wake it so the next eval
    /// sees a fresh resident tenant. No external waiter watches the
    /// flight.
    MaxSleep,
}

impl WakeSourceKind {
    /// Stable metric label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Cron => "cron",
            Self::Queue => "queue",
            Self::Alarm => "alarm",
            Self::WebSocket => "websocket",
            Self::Workflow => "workflow",
            Self::MaxSleep => "max-sleep",
        }
    }

    /// Bounded launch retry for this source: total launch attempts a
    /// single wake may use (>= 1).
    pub fn max_attempts(self) -> u32 {
        match self {
            Self::Http
            | Self::Cron
            | Self::Queue
            | Self::Workflow
            | Self::Alarm
            | Self::WebSocket
            | Self::MaxSleep => 2,
        }
    }

    /// Longest this source may wait for ONE wake, given the host-wide
    /// default. Http/Cron use the default. Queue is capped at
    /// [`QUEUE_WAKE_MAX_WAIT`]: wake time is charged to the 55s facade
    /// delivery budget, so the handler must keep at least 35s. Workflow
    /// is capped at [`WORKFLOW_WAKE_MAX_WAIT`] (controller 15s call
    /// timeout, see that constant).
    pub fn max_wait(self, default: Duration) -> Duration {
        match self {
            Self::Http | Self::Cron | Self::Alarm | Self::WebSocket | Self::MaxSleep => default,
            Self::Queue => default.min(QUEUE_WAKE_MAX_WAIT),
            Self::Workflow => default.min(WORKFLOW_WAKE_MAX_WAIT),
        }
    }
}

/// Idempotency key of one wake: a (source, flight) pair — a duplicate
/// or stale request for an already-handled flight is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WakeIdemKey {
    /// Trigger kind.
    pub source: WakeSourceKind,
    /// Per-tenant monotonically increasing flight id.
    pub flight: u64,
}

/// Why a wake did not produce a serving tenant. Each maps to a typed
/// 503 — never a fake success.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeError {
    /// Wake exceeded its deadline (`tenant_wake_timeout`).
    Timeout,
    /// Launch / ready probe failed (`tenant_wake_failed`).
    Failed(&'static str),
    /// Host wake capacity exhausted (`wake_capacity`).
    Capacity,
    /// Tenant circuit breaker open (existing `tenant_circuit_open`).
    CircuitOpen,
}

impl WakeError {
    /// Stable metric outcome label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Failed(_) => "failed",
            Self::Capacity => "capacity",
            Self::CircuitOpen => "circuit_open",
        }
    }
}

/// One pending wake as seen by the actor.
#[derive(Clone, Copy, Debug)]
pub struct WakeRequest {
    /// Idempotency key.
    pub key: WakeIdemKey,
    /// When the flight started (anchors the deadline).
    pub started: Instant,
}

type FlightResult = Option<Result<(), WakeError>>;

struct LiveFlight {
    key: WakeIdemKey,
    started: Instant,
    tx: watch::Sender<FlightResult>,
    /// The actor has seen this flight (idempotency).
    claimed: bool,
}

struct FlightInner {
    current: Option<LiveFlight>,
    /// Most recent failure — gates re-triggering for `cooldown`.
    last_failure: Option<(Instant, WakeError)>,
    /// Fail-fast window after a failed wake.
    cooldown: Duration,
}

/// Outcome of joining the flight.
pub enum WakeJoin {
    /// Joined (or started) the flight — wait on the ticket.
    Ticket(WakeTicket),
    /// A recent failure is cooling down — fail fast, do not launch.
    CoolingDown(WakeError),
}

/// Receiver side of a flight.
pub struct WakeTicket {
    /// This waiter started the flight.
    pub leader: bool,
    /// Flight id.
    pub flight: u64,
    rx: watch::Receiver<FlightResult>,
}

impl WakeTicket {
    /// Wait for the flight's outcome, at most `max_wait`. Dropping the
    /// future (client disconnect) abandons only this receiver — the
    /// actor keeps owning the wake, so a leader leaving never strands
    /// followers.
    pub async fn wait(mut self, max_wait: Duration) -> Result<(), WakeError> {
        let fut = async {
            loop {
                if let Some(r) = *self.rx.borrow_and_update() {
                    return r;
                }
                if self.rx.changed().await.is_err() {
                    // Sender dropped without a verdict (tenant removed
                    // mid-wake): never a success.
                    return Err(WakeError::Failed("tenant-gone"));
                }
            }
        };
        match tokio::time::timeout(max_wait, fut).await {
            Ok(r) => r,
            Err(_) => Err(WakeError::Timeout),
        }
    }
}

/// Per-tenant single-flight wake. Lives on `TenantRuntime`; ingress
/// calls [`WakeFlight::join`], the actor calls
/// [`WakeFlight::pending`] / [`WakeFlight::complete`].
pub struct WakeFlight {
    inner: Mutex<FlightInner>,
    seq: AtomicU64,
    /// Wakes the actor when a flight starts (stored permit survives
    /// ordering vs `notified()` registration).
    pub notify: Arc<Notify>,
    flights_started: AtomicU64,
}

impl WakeFlight {
    /// New flight cell; `cooldown` is the fail-fast window after a
    /// failed wake (bounds relaunch storms against a broken tenant).
    pub fn new(cooldown: Duration) -> Self {
        Self {
            inner: Mutex::new(FlightInner {
                current: None,
                last_failure: None,
                cooldown,
            }),
            seq: AtomicU64::new(0),
            notify: Arc::new(Notify::new()),
            flights_started: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FlightInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Re-arm the fail-fast window (host config lands after the runtime
    /// is built).
    pub fn set_cooldown(&self, cooldown: Duration) {
        self.lock().cooldown = cooldown;
    }

    /// Join the in-progress flight or start one.
    pub fn join(&self, source: WakeSourceKind) -> WakeJoin {
        let mut g = self.lock();
        if let Some(cur) = &g.current {
            return WakeJoin::Ticket(WakeTicket {
                leader: false,
                flight: cur.key.flight,
                rx: cur.tx.subscribe(),
            });
        }
        if let Some((at, err)) = g.last_failure {
            if at.elapsed() < g.cooldown {
                return WakeJoin::CoolingDown(err);
            }
        }
        let id = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let (tx, rx) = watch::channel(None);
        g.current = Some(LiveFlight {
            key: WakeIdemKey { source, flight: id },
            started: Instant::now(),
            tx,
            claimed: false,
        });
        self.flights_started.fetch_add(1, Ordering::Relaxed);
        drop(g);
        self.notify.notify_one();
        WakeJoin::Ticket(WakeTicket {
            leader: true,
            flight: id,
            rx,
        })
    }

    /// The flight the actor still has to handle (not yet claimed).
    pub fn pending(&self) -> Option<WakeRequest> {
        self.lock()
            .current
            .as_ref()
            .filter(|c| !c.claimed)
            .map(|c| WakeRequest {
                key: c.key,
                started: c.started,
            })
    }

    /// The in-progress flight regardless of claim state.
    pub fn current(&self) -> Option<WakeRequest> {
        self.lock().current.as_ref().map(|c| WakeRequest {
            key: c.key,
            started: c.started,
        })
    }

    /// Mark the flight handled by the actor (idempotency).
    pub fn claim(&self, flight: u64) {
        if let Some(c) = self.lock().current.as_mut() {
            if c.key.flight == flight {
                c.claimed = true;
            }
        }
    }

    /// Resolve the flight `flight`; a stale id is ignored. Failures arm
    /// the cooldown; success clears it.
    pub fn complete(&self, flight: u64, result: Result<(), WakeError>) -> bool {
        let mut g = self.lock();
        let matches = g
            .current
            .as_ref()
            .is_some_and(|c| c.key.flight == flight);
        if !matches {
            return false;
        }
        let cur = g.current.take().expect("checked");
        match result {
            Ok(()) => g.last_failure = None,
            Err(e) => g.last_failure = Some((Instant::now(), e)),
        }
        drop(g);
        let _ = cur.tx.send(Some(result));
        true
    }

    /// Resolve whatever flight is current (used when the outcome is
    /// tenant-wide, e.g. the circuit opened or the tenant is draining).
    pub fn complete_current(&self, result: Result<(), WakeError>) -> Option<u64> {
        let id = self.current().map(|c| c.key.flight)?;
        self.complete(id, result).then_some(id)
    }

    /// Flights ever started (diagnostics / single-flight tests).
    pub fn flights_started(&self) -> u64 {
        self.flights_started.load(Ordering::Relaxed)
    }
}

// ======================= coordinator =======================

struct CoordInner {
    active: usize,
    queue: VecDeque<(String, Arc<Notify>)>,
}

/// Host-wide cap on concurrent wakes with a fair FIFO queue. Process
/// spawns are expensive; a burst of cold tenants must not starve the
/// host or the already-awake tenants.
pub struct WakeCoordinator {
    max_concurrent: usize,
    max_queue: usize,
    inner: Mutex<CoordInner>,
}

/// Admission result.
pub enum WakeAdmission {
    /// Run the launch now; the permit frees the slot on drop.
    Granted(WakePermit),
    /// Queued behind earlier wakes — the tenant's notify fires when a
    /// slot may be free; call `request` again then.
    Queued,
    /// Queue full — fail with `wake_capacity`.
    Rejected,
}

/// RAII slot in the coordinator.
pub struct WakePermit {
    coord: Arc<WakeCoordinator>,
}

impl Drop for WakePermit {
    fn drop(&mut self) {
        let wake: Vec<Arc<Notify>> = {
            let mut g = self.coord.lock();
            g.active = g.active.saturating_sub(1);
            let free = self.coord.max_concurrent.saturating_sub(g.active);
            g.queue
                .iter()
                .take(free)
                .map(|(_, n)| n.clone())
                .collect()
        };
        for n in wake {
            n.notify_one();
        }
    }
}

impl WakeCoordinator {
    /// New coordinator; `max_concurrent >= 1`, `max_queue` bounds the
    /// waiting line.
    pub fn new(max_concurrent: usize, max_queue: usize) -> Arc<Self> {
        Arc::new(Self {
            max_concurrent: max_concurrent.max(1),
            max_queue,
            inner: Mutex::new(CoordInner {
                active: 0,
                queue: VecDeque::new(),
            }),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CoordInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Ask to run `tenant`'s wake. Idempotent per tenant (a tenant is
    /// queued at most once). Fair: a free slot goes to the queue head.
    pub fn request(self: &Arc<Self>, tenant: &str, notify: &Arc<Notify>) -> WakeAdmission {
        let mut g = self.lock();
        let free = self.max_concurrent.saturating_sub(g.active);
        match g.queue.iter().position(|(t, _)| t == tenant) {
            Some(p) if p < free => {
                g.queue.remove(p);
                g.active += 1;
                WakeAdmission::Granted(WakePermit {
                    coord: self.clone(),
                })
            }
            Some(_) => WakeAdmission::Queued,
            None => {
                if g.queue.len() < free {
                    g.active += 1;
                    WakeAdmission::Granted(WakePermit {
                        coord: self.clone(),
                    })
                } else if g.queue.len() >= self.max_queue {
                    WakeAdmission::Rejected
                } else {
                    g.queue.push_back((tenant.to_string(), notify.clone()));
                    WakeAdmission::Queued
                }
            }
        }
    }

    /// Withdraw a queued tenant (deadline, drain, abort).
    pub fn cancel(&self, tenant: &str) {
        self.lock().queue.retain(|(t, _)| t != tenant);
    }

    /// Wakes currently holding a permit.
    pub fn active(&self) -> usize {
        self.lock().active
    }

    /// Tenants waiting for a permit.
    pub fn queued(&self) -> usize {
        self.lock().queue.len()
    }
}

// ======================= tests =======================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_doubles_caps_and_resets() {
        let base = Duration::from_secs(60);
        let mut b = HibernateRetryBackoff::default();
        let seq: Vec<u64> = (0..7).map(|_| b.on_blocked_abort(base).as_secs()).collect();
        assert_eq!(seq, vec![60, 120, 240, 480, 960, 1800, 1800]);
        assert_eq!(b.consecutive(), 7);
        b.reset();
        assert_eq!(b.consecutive(), 0);
        assert_eq!(b.on_blocked_abort(base), base);
    }

    #[test]
    fn retry_backoff_survives_overflow_and_large_base() {
        let base = Duration::from_secs(60);
        let mut b = HibernateRetryBackoff { consecutive: u32::MAX - 1 };
        assert_eq!(b.on_blocked_abort(base), BACKOFF_CAP_FLOOR);
        assert_eq!(b.on_blocked_abort(base), BACKOFF_CAP_FLOOR);
        assert_eq!(b.consecutive(), u32::MAX);
        // base above the floor is its own ceiling.
        let big = Duration::from_secs(3 * 3600);
        let mut b = HibernateRetryBackoff::default();
        for _ in 0..5 {
            assert_eq!(b.on_blocked_abort(big), big);
        }
        // Huge base saturates instead of panicking.
        let mut b = HibernateRetryBackoff { consecutive: 40 };
        assert_eq!(b.on_blocked_abort(Duration::MAX), Duration::MAX);
    }

    fn at(base: Instant, s: u64) -> Instant {
        base + Duration::from_secs(s)
    }

    #[test]
    fn fixed_window_policy_idle_and_residency() {
        let p = FixedWindowPolicy {
            idle_window: Duration::from_secs(600),
            min_residency: Duration::from_secs(120),
        };
        let t0 = Instant::now();
        let mk = |now, last, ready| IdleInputs {
            now,
            last_activity: last,
            ready_since: ready,
            auto: None,
        };
        // Idle long enough and resident long enough.
        assert_eq!(
            p.evaluate(&mk(at(t0, 700), t0, t0)),
            IdleVerdict::Hibernate
        );
        // Recent activity defers to last_activity + window.
        assert_eq!(
            p.evaluate(&mk(at(t0, 700), at(t0, 400), t0)),
            IdleVerdict::DeferUntil(at(t0, 1000))
        );
        // Idle but freshly woken: min_residency wins.
        let long_idle = FixedWindowPolicy {
            idle_window: Duration::from_secs(10),
            min_residency: Duration::from_secs(120),
        };
        assert_eq!(
            long_idle.evaluate(&mk(at(t0, 50), t0, t0)),
            IdleVerdict::DeferUntil(at(t0, 120))
        );
        assert_eq!(
            long_idle.evaluate(&mk(at(t0, 120), t0, t0)),
            IdleVerdict::Hibernate
        );
        // Zero window = disabled.
        let off = FixedWindowPolicy {
            idle_window: Duration::ZERO,
            min_residency: Duration::ZERO,
        };
        assert_eq!(off.evaluate(&mk(at(t0, 9999), t0, t0)), IdleVerdict::Keep);
    }

    fn auto_params() -> AutoParams {
        AutoParams {
            floor: Duration::from_secs(600),
            ceiling: Duration::from_secs(3_600),
            premature_wake: Duration::from_secs(120),
            sound_sleep: Duration::from_secs(3_600),
            wake_budget: 0,
            wake_budget_window: Duration::from_secs(3_600),
        }
    }

    #[test]
    fn auto_state_adapts_window_on_wake() {
        let t0 = Instant::now();
        let mut s = AutoState::new(auto_params());
        assert_eq!(s.window(), Duration::from_secs(600));
        // A premature wake (slept < 2min) doubles the window.
        s.record_sleep_commit(t0);
        s.record_wake(at(t0, 60));
        assert_eq!(s.window(), Duration::from_secs(1_200));
        // Neutral sleep: between premature and sound — unchanged.
        s.record_sleep_commit(at(t0, 100));
        s.record_wake(at(t0, 1_300));
        assert_eq!(s.window(), Duration::from_secs(1_200));
        // A sound sleep halves it toward the floor.
        s.record_sleep_commit(at(t0, 1_400));
        s.record_wake(at(t0, 1_400 + 3_700));
        assert_eq!(s.window(), Duration::from_secs(600));
        // Ceiling cap: repeated premature wakes stop at ceiling.
        for i in 0..5 {
            let base = at(t0, 10_000 + i * 300);
            s.record_sleep_commit(base);
            s.record_wake(base + Duration::from_secs(10));
        }
        assert_eq!(s.window(), Duration::from_secs(3_600));
        // Floor cap: sound sleeps cannot push below the configured
        // idle window.
        for i in 0..5 {
            let base = at(t0, 20_000 + i * 4_000);
            s.record_sleep_commit(base);
            s.record_wake(base + Duration::from_secs(3_900));
        }
        assert_eq!(s.window(), Duration::from_secs(600));
    }

    #[test]
    fn auto_state_clear_sleep_ignores_uncommitted_wake() {
        let t0 = Instant::now();
        let mut s = AutoState::new(auto_params());
        s.record_sleep_commit(t0);
        s.clear_sleep();
        // Wake without a committed sleep: no adaptation (slept_at was
        // consumed), but the wake still counts toward the budget.
        s.record_wake(at(t0, 60));
        assert_eq!(s.window(), Duration::from_secs(600));
    }

    #[test]
    fn auto_state_wake_budget_gates_facts() {
        let t0 = Instant::now();
        let mut p = auto_params();
        p.wake_budget = 2;
        let mut s = AutoState::new(p);
        assert_eq!(s.facts(t0).budget_refill_at, None);
        assert_eq!(s.budget_remaining(t0), 2);
        s.record_wake(at(t0, 10));
        s.record_wake(at(t0, 20));
        // Exhausted: refill when the OLDEST counted wake ages out.
        let f = s.facts(at(t0, 30));
        assert_eq!(f.budget_refill_at, Some(at(t0, 10 + 3_600)));
        assert_eq!(s.budget_remaining(at(t0, 30)), 0);
        // Over-budget (3 wakes): refill when len-budget+1 = 2 wakes
        // age out — i.e. the wake at index len-budget = 1 (t=20).
        s.record_wake(at(t0, 25));
        let f = s.facts(at(t0, 30));
        assert_eq!(f.budget_refill_at, Some(at(t0, 20 + 3_600)));
        // After the first wake ages out, still exhausted (2 remain).
        let f = s.facts(at(t0, 3_611));
        assert_eq!(f.budget_refill_at, Some(at(t0, 20 + 3_600)));
        // After two age out: budget refilled.
        let f = s.facts(at(t0, 3_621));
        assert_eq!(f.budget_refill_at, None);
        assert_eq!(s.budget_remaining(at(t0, 3_621)), 1);
    }

    #[test]
    fn auto_policy_evaluates_facts() {
        let p = AutoPolicy {
            params: auto_params(),
            min_residency: Duration::from_secs(120),
        };
        let t0 = Instant::now();
        let facts = |window, refill| {
            Some(AutoFacts {
                window,
                budget_refill_at: refill,
            })
        };
        let mk = |now, last, ready, auto| IdleInputs {
            now,
            last_activity: last,
            ready_since: ready,
            auto,
        };
        // No facts = keep (policy cannot decide without state).
        assert_eq!(
            p.evaluate(&mk(at(t0, 9999), t0, t0, None)),
            IdleVerdict::Keep
        );
        // Idle past the adapted window hibernates.
        assert_eq!(
            p.evaluate(&mk(
                at(t0, 700),
                t0,
                t0,
                facts(Duration::from_secs(600), None)
            )),
            IdleVerdict::Hibernate
        );
        // A longer adapted window defers further out.
        assert_eq!(
            p.evaluate(&mk(
                at(t0, 700),
                t0,
                t0,
                facts(Duration::from_secs(1_200), None)
            )),
            IdleVerdict::DeferUntil(at(t0, 1_200))
        );
        // Exhausted budget defers to the refill instant — not Keep.
        assert_eq!(
            p.evaluate(&mk(
                at(t0, 700),
                t0,
                t0,
                facts(Duration::from_secs(600), Some(at(t0, 5_000)))
            )),
            IdleVerdict::DeferUntil(at(t0, 5_000))
        );
        // Residency floor still applies under the adapted window.
        assert_eq!(
            p.evaluate(&mk(
                at(t0, 50),
                t0,
                t0,
                facts(Duration::from_secs(10), None)
            )),
            IdleVerdict::DeferUntil(at(t0, 120))
        );
    }

    #[test]
    fn max_sleep_wake_source_is_bounded() {
        assert_eq!(WakeSourceKind::MaxSleep.label(), "max-sleep");
        assert_eq!(WakeSourceKind::MaxSleep.max_attempts(), 2);
        let d = Duration::from_secs(30);
        assert_eq!(WakeSourceKind::MaxSleep.max_wait(d), d);
    }

    fn ctx(in_flight: usize, s: StructuralFacts, wu: WaitUntilFact) -> BlockContext {
        BlockContext {
            in_flight,
            structural: s,
            wait_until: wu,
        }
    }

    #[test]
    fn structural_blocker_denies_each_binding() {
        let chain = BlockerChain::p0();
        let none = StructuralFacts::default();
        assert_eq!(
            chain.first_block(BlockPhase::Admission, &ctx(0, none, WaitUntilFact::NotProbed)),
            None
        );
        for (facts, want) in [
            (
                StructuralFacts {
                    durable_objects: true,
                    ..none
                },
                BlockReason::StructuralDurableObject,
            ),
            (
                StructuralFacts {
                    cron: true,
                    durable_objects: true,
                    ..none
                },
                BlockReason::StructuralDurableObject,
            ),
            (
                StructuralFacts {
                    queue_consumers: true,
                    durable_objects: true,
                    ..none
                },
                BlockReason::StructuralDurableObject,
            ),
            (
                StructuralFacts {
                    workflows: true,
                    ..none
                },
                BlockReason::StructuralWorkflow,
            ),
        ] {
            assert_eq!(
                chain.first_block(BlockPhase::Admission, &ctx(0, facts, WaitUntilFact::NotProbed)),
                Some(want)
            );
            assert!(want.is_structural());
        }
        // Cron / queue consumers alone never block: their triggers wake
        // the tenant.
        for facts in [
            StructuralFacts { cron: true, ..none },
            StructuralFacts {
                queue_consumers: true,
                ..none
            },
        ] {
            assert_eq!(
                chain.first_block(BlockPhase::Admission, &ctx(0, facts, WaitUntilFact::NotProbed)),
                None
            );
        }
        // A safe DO alarm gate releases the structural block.
        let safe_do = StructuralFacts {
            durable_objects: true,
            alarm_safe: true,
            ..none
        };
        assert_eq!(
            chain.first_block(BlockPhase::Admission, &ctx(0, safe_do, WaitUntilFact::NotProbed)),
            None
        );
    }

    #[test]
    fn in_flight_blocks_both_phases() {
        let chain = BlockerChain::p0();
        let none = StructuralFacts::default();
        assert_eq!(
            chain.first_block(BlockPhase::Admission, &ctx(1, none, WaitUntilFact::NotProbed)),
            Some(BlockReason::InFlight)
        );
        assert_eq!(
            chain.first_block(BlockPhase::Commit, &ctx(1, none, WaitUntilFact::Idle)),
            Some(BlockReason::InFlight)
        );
        assert!(!BlockReason::InFlight.is_structural());
    }

    #[test]
    fn waituntil_blocker_fails_closed() {
        let chain = BlockerChain::p0();
        let none = StructuralFacts::default();
        let commit = |wu| chain.first_block(BlockPhase::Commit, &ctx(0, none, wu));
        assert_eq!(commit(WaitUntilFact::Idle), None);
        assert_eq!(commit(WaitUntilFact::NoAccounting), None);
        assert_eq!(
            commit(WaitUntilFact::Pending(3)),
            Some(BlockReason::WaitUntilPending)
        );
        assert_eq!(
            commit(WaitUntilFact::Unknown),
            Some(BlockReason::WaitUntilUnknown)
        );
        // A commit check without a verdict is also a denial.
        assert_eq!(
            commit(WaitUntilFact::NotProbed),
            Some(BlockReason::WaitUntilUnknown)
        );
    }

    #[test]
    fn empty_chain_denies() {
        let chain = BlockerChain::new(vec![]);
        let none = StructuralFacts::default();
        for phase in [BlockPhase::Admission, BlockPhase::Commit] {
            assert_eq!(
                chain.first_block(phase, &ctx(0, none, WaitUntilFact::Idle)),
                Some(BlockReason::NoBlockerConfigured)
            );
        }
        assert!(chain.is_empty());
    }

    #[test]
    fn block_reason_labels_are_unique() {
        let all = [
            BlockReason::InFlight,
            BlockReason::WaitUntilPending,
            BlockReason::WaitUntilUnknown,
            BlockReason::StructuralDurableObject,
            BlockReason::StructuralCron,
            BlockReason::StructuralQueueConsumer,
            BlockReason::StructuralWorkflow,
            BlockReason::NoBlockerConfigured,
        ];
        let set: std::collections::BTreeSet<_> = all.iter().map(|r| r.label()).collect();
        assert_eq!(set.len(), all.len());
    }

    fn ticket(j: WakeJoin) -> WakeTicket {
        match j {
            WakeJoin::Ticket(t) => t,
            WakeJoin::CoolingDown(e) => panic!("unexpected cooldown {e:?}"),
        }
    }

    #[tokio::test]
    async fn wake_flight_is_single_flight() {
        let f = WakeFlight::new(Duration::from_secs(1));
        let a = ticket(f.join(WakeSourceKind::Http));
        let b = ticket(f.join(WakeSourceKind::Http));
        let c = ticket(f.join(WakeSourceKind::Http));
        assert!(a.leader && !b.leader && !c.leader);
        assert_eq!(a.flight, b.flight);
        assert_eq!(f.flights_started(), 1);
        let req = f.pending().expect("unclaimed flight");
        assert_eq!(req.key.flight, a.flight);
        f.claim(req.key.flight);
        assert!(f.pending().is_none(), "claimed flight is not re-delivered");
        assert!(f.complete(req.key.flight, Ok(())));
        assert_eq!(a.wait(Duration::from_secs(1)).await, Ok(()));
        assert_eq!(b.wait(Duration::from_secs(1)).await, Ok(()));
        assert_eq!(c.wait(Duration::from_secs(1)).await, Ok(()));
        // Completed: the next join is a NEW flight.
        let d = ticket(f.join(WakeSourceKind::Http));
        assert!(d.leader);
        assert_eq!(f.flights_started(), 2);
    }

    #[tokio::test]
    async fn wake_flight_stale_completion_is_ignored() {
        let f = WakeFlight::new(Duration::ZERO);
        let a = ticket(f.join(WakeSourceKind::Http));
        assert!(!f.complete(a.flight + 99, Ok(())));
        assert!(f.current().is_some());
        assert!(f.complete(a.flight, Err(WakeError::Timeout)));
        assert!(!f.complete(a.flight, Ok(())), "double completion ignored");
        assert_eq!(a.wait(Duration::from_secs(1)).await, Err(WakeError::Timeout));
    }

    #[tokio::test]
    async fn wake_flight_failure_cools_down_then_rearms() {
        let f = WakeFlight::new(Duration::from_millis(80));
        let a = ticket(f.join(WakeSourceKind::Http));
        f.complete(a.flight, Err(WakeError::Failed("spawn")));
        match f.join(WakeSourceKind::Http) {
            WakeJoin::CoolingDown(WakeError::Failed("spawn")) => {}
            _ => panic!("expected cooldown"),
        }
        assert_eq!(f.flights_started(), 1, "cooldown must not start a flight");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(ticket(f.join(WakeSourceKind::Http)).leader);
    }

    #[tokio::test]
    async fn wake_ticket_times_out_and_leader_drop_does_not_strand_followers() {
        let f = WakeFlight::new(Duration::ZERO);
        let leader = ticket(f.join(WakeSourceKind::Http));
        let follower = ticket(f.join(WakeSourceKind::Http));
        let id = leader.flight;
        // Leader's client disconnects: its wait future is dropped.
        let h = tokio::spawn(leader.wait(Duration::from_secs(5)));
        h.abort();
        let _ = h.await;
        f.complete(id, Ok(()));
        assert_eq!(follower.wait(Duration::from_secs(1)).await, Ok(()));
        let slow = ticket(f.join(WakeSourceKind::Http));
        assert_eq!(
            slow.wait(Duration::from_millis(30)).await,
            Err(WakeError::Timeout)
        );
    }

    #[tokio::test]
    async fn wake_ticket_sender_drop_is_failure_not_success() {
        let f = WakeFlight::new(Duration::ZERO);
        let t = ticket(f.join(WakeSourceKind::Http));
        drop(f);
        assert_eq!(
            t.wait(Duration::from_secs(1)).await,
            Err(WakeError::Failed("tenant-gone"))
        );
    }

    #[test]
    fn coordinator_caps_concurrency_and_queues_fairly() {
        let c = WakeCoordinator::new(2, 4);
        let n = |_: &str| Arc::new(Notify::new());
        let (na, nb, nc, nd) = (n("a"), n("b"), n("c"), n("d"));
        let pa = match c.request("a", &na) {
            WakeAdmission::Granted(p) => p,
            _ => panic!("a granted"),
        };
        let _pb = match c.request("b", &nb) {
            WakeAdmission::Granted(p) => p,
            _ => panic!("b granted"),
        };
        assert_eq!(c.active(), 2);
        assert!(matches!(c.request("c", &nc), WakeAdmission::Queued));
        assert!(matches!(c.request("d", &nd), WakeAdmission::Queued));
        // Re-asking while queued does not double-queue.
        assert!(matches!(c.request("c", &nc), WakeAdmission::Queued));
        assert_eq!(c.queued(), 2);
        // d cannot jump c.
        assert!(matches!(c.request("d", &nd), WakeAdmission::Queued));
        drop(pa);
        assert_eq!(c.active(), 1);
        assert!(matches!(c.request("d", &nd), WakeAdmission::Queued));
        let pc = match c.request("c", &nc) {
            WakeAdmission::Granted(p) => p,
            _ => panic!("c is head and gets the free slot"),
        };
        assert_eq!(c.active(), 2);
        assert_eq!(c.queued(), 1);
        drop(pc);
        assert!(matches!(c.request("d", &nd), WakeAdmission::Granted(_)));
    }

    #[tokio::test]
    async fn coordinator_permit_drop_notifies_head() {
        let c = WakeCoordinator::new(1, 2);
        let nb = Arc::new(Notify::new());
        let pa = match c.request("a", &Arc::new(Notify::new())) {
            WakeAdmission::Granted(p) => p,
            _ => panic!(),
        };
        assert!(matches!(c.request("b", &nb), WakeAdmission::Queued));
        drop(pa);
        tokio::time::timeout(Duration::from_secs(1), nb.notified())
            .await
            .expect("queue head must be notified when a permit frees");
    }

    #[test]
    fn coordinator_rejects_when_queue_full_and_cancel_frees() {
        let c = WakeCoordinator::new(1, 1);
        let n = Arc::new(Notify::new());
        let _p = match c.request("a", &n) {
            WakeAdmission::Granted(p) => p,
            _ => panic!(),
        };
        assert!(matches!(c.request("b", &n), WakeAdmission::Queued));
        assert!(matches!(c.request("c", &n), WakeAdmission::Rejected));
        c.cancel("b");
        assert_eq!(c.queued(), 0);
        assert!(matches!(c.request("c", &n), WakeAdmission::Queued));
    }

    #[test]
    fn wake_source_retry_is_bounded() {
        assert!(WakeSourceKind::Http.max_attempts() >= 1);
        assert!(WakeSourceKind::Http.max_attempts() <= 3);
        assert_eq!(WakeError::Capacity.label(), "capacity");
        assert_eq!(WakeSourceKind::Cron.label(), "cron");
        assert_eq!(WakeSourceKind::Cron.max_attempts(), 2);
        assert_eq!(WakeSourceKind::Queue.label(), "queue");
        assert_eq!(WakeSourceKind::Queue.max_attempts(), 2);
        let d = Duration::from_secs(30);
        assert_eq!(WakeSourceKind::Http.max_wait(d), d);
        assert_eq!(WakeSourceKind::Cron.max_wait(d), d);
        assert_eq!(WakeSourceKind::Queue.max_wait(d), Duration::from_secs(20));
        assert_eq!(WakeSourceKind::Workflow.label(), "workflow");
        assert_eq!(WakeSourceKind::Workflow.max_attempts(), 2);
        assert_eq!(WakeSourceKind::Workflow.max_wait(d), Duration::from_secs(8));
        assert_eq!(
            WakeSourceKind::Workflow.max_wait(Duration::from_secs(3)),
            Duration::from_secs(3)
        );
        // A shorter host default is never lengthened.
        assert_eq!(
            WakeSourceKind::Queue.max_wait(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }
}
