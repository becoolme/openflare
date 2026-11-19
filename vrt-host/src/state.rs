//! `TenantState` — the explicit per-tenant lifecycle machine.
//!
//! ```text
//!            ┌───────────────────────────────────────────┐
//!            │              launch ok                     ▼
//!   Starting ────────────────► Ready ──drain begin──► Draining ──► Stopped
//!      ▲  │                    │   ▲                       ▲          ▲
//!      │  │launch fail         │crash                      │          │
//!      │  ▼                    ▼   │retry begin            │drained / │
//!      └─ Crashed ◄──circuit trip──┘                       │forced    │
//!            │                                             │          │
//!   CircuitOpen ──cooldown──► Starting(half-open)          └──────────┘
//! ```
//!
//! Version switching is not a separate state: a Ready tenant keeps its
//! `active` generation serving while a `pending` generation launches, so
//! `Ready + switch` stays `Ready` and a failed switch leaves `Ready`
//! untouched (no service gap).

/// Per-tenant lifecycle. `#[repr(u8)]` lets [`supervisor::TenantRuntime`]
/// publish it atomically for the ingress fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TenantState {
    /// No live generation; a launch attempt is in flight or imminent.
    Starting = 0,
    /// A live generation is serving traffic.
    Ready = 1,
    /// Shutdown requested — in-flight requests drain, no new ones.
    Draining = 2,
    /// Terminal. No process, no attempts.
    Stopped = 3,
    /// Unexpected loss of the serving generation; a backoff-gated retry
    /// is pending.
    Crashed = 4,
    /// Failure-window breaker open; requests get a typed 503 until the
    /// cooldown elapses and one half-open attempt is allowed.
    CircuitOpen = 5,
    /// T13/M4: generation switch failed `max_switch_failures` times in a
    /// row — retries STOPPED (was previously unbounded). The old
    /// generation keeps serving; a new Apply re-arms switching.
    SwitchFailed = 6,
    /// Scale-to-zero: idle tenant is draining in-flight work before its
    /// process is released. Still routable until the drain gate closes;
    /// a wake request aborts it back to `Ready`.
    Hibernating = 7,
    /// Scale-to-zero: no process, desired spec retained. Healthy (not an
    /// outage) — the next wake source launches it.
    Hibernated = 8,
    /// Scale-to-zero: a wake launch is in flight.
    Waking = 9,
}

/// Transition inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transition {
    /// A generation became ready and installed.
    Ready,
    /// A launch attempt failed (spawn error or ready timeout).
    LaunchFailed,
    /// The serving generation exited unexpectedly.
    Crash,
    /// The failure-window threshold was reached — trip the breaker.
    CircuitTrip,
    /// Cooldown elapsed — begin one half-open attempt.
    CooldownEnd,
    /// A backoff-gated retry fires — the attempt starts.
    RetryBegin,
    /// Drain requested (tenant removal or host shutdown).
    BeginDrain,
    /// Everything drained and terminated.
    Drained,
    /// T13/M4: the bounded switch-retry budget was exhausted — the
    /// tenant moves to `SwitchFailed` (old generation still serves).
    SwitchFailed,
    /// Scale-to-zero: idle verdict reached, start draining.
    HibernateBegin,
    /// Scale-to-zero: drain timed out / a wake arrived — stay serving.
    HibernateAbort,
    /// Scale-to-zero: process released.
    Hibernated,
    /// Scale-to-zero: a wake source fired.
    WakeBegin,
}

/// Illegal transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("illegal tenant transition {from:?} + {event:?}")]
pub struct TransitionError {
    /// State the event was applied to.
    pub from: TenantState,
    /// Event that was rejected.
    pub event: Transition,
}

impl TenantState {
    /// Ingress fast-path: only `Ready` serves.
    pub fn can_serve(self) -> bool {
        // SwitchFailed keeps the OLD generation serving — it is a
        // stopped-retries marker, not an outage.
        matches!(self, Self::Ready | Self::SwitchFailed)
    }

    /// Scale-to-zero: no process is held for this tenant by design.
    pub fn is_asleep(self) -> bool {
        matches!(self, Self::Hibernated | Self::Waking)
    }

    /// Every state, for exhaustive metric emission.
    pub const ALL: [TenantState; 10] = [
        Self::Starting,
        Self::Ready,
        Self::Draining,
        Self::Stopped,
        Self::Crashed,
        Self::CircuitOpen,
        Self::SwitchFailed,
        Self::Hibernating,
        Self::Hibernated,
        Self::Waking,
    ];

    /// Stable lowercase metric label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Crashed => "crashed",
            Self::CircuitOpen => "circuit_open",
            Self::SwitchFailed => "switch_failed",
            Self::Hibernating => "hibernating",
            Self::Hibernated => "hibernated",
            Self::Waking => "waking",
        }
    }

    /// Whether a launch attempt may be started from this state.
    pub fn can_attempt_launch(self) -> bool {
        matches!(self, Self::Starting | Self::Crashed | Self::CircuitOpen)
    }

    /// Apply `event`; illegal combinations fail closed (the caller logs —
    /// a bad transition is a supervisor bug, not tenant input).
    pub fn transition(self, event: Transition) -> Result<Self, TransitionError> {
        let next = match (self, event) {
            (Self::Starting, Transition::Ready) => Self::Ready,
            (Self::Starting, Transition::RetryBegin) => Self::Starting,
            (Self::Starting, Transition::LaunchFailed) => Self::Starting,
            (Self::Starting, Transition::CircuitTrip) => Self::CircuitOpen,
            (Self::Ready, Transition::Crash) => Self::Crashed,
            (Self::Ready, Transition::LaunchFailed) => Self::Ready, // failed switch keeps serving
            (Self::Ready, Transition::RetryBegin) => Self::Ready,   // bounded switch retry fires
            (Self::Ready, Transition::SwitchFailed) => Self::SwitchFailed,
            // W1-r2: Ready + no live generation IS reachable — the
            // serialized DO switch drains `active` while the state
            // stays Ready. If the launch-failure threshold then trips
            // the circuit, the tenant is down; reporting Ready would
            // mask the outage (this exact warn shipped in the W1-r2
            // incident). Open the circuit — the cooldown→Starting
            // path retries with backoff like a crash storm.
            (Self::Ready, Transition::CircuitTrip) => Self::CircuitOpen,
            (Self::SwitchFailed, Transition::Ready) => Self::Ready,
            (Self::SwitchFailed, Transition::LaunchFailed) => Self::SwitchFailed,
            (Self::SwitchFailed, Transition::RetryBegin) => Self::Starting,
            (Self::Crashed, Transition::LaunchFailed) => Self::Crashed,
            (Self::Crashed, Transition::RetryBegin) => Self::Starting,
            (Self::Crashed, Transition::CircuitTrip) => Self::CircuitOpen,
            (Self::Waking, Transition::CircuitTrip) => Self::CircuitOpen,
            (Self::CircuitOpen, Transition::CooldownEnd) => Self::Starting,
            (Self::CircuitOpen, Transition::LaunchFailed) => Self::CircuitOpen,
            (
                Self::Ready | Self::Starting | Self::Crashed | Self::CircuitOpen,
                Transition::BeginDrain,
            ) => Self::Draining,
            (Self::Ready, Transition::HibernateBegin) => Self::Hibernating,
            (Self::Hibernating, Transition::HibernateAbort) => Self::Ready,
            (Self::Hibernating, Transition::Hibernated) => Self::Hibernated,
            (Self::Hibernated, Transition::WakeBegin) => Self::Waking,
            (Self::Waking, Transition::Ready) => Self::Ready,
            // A failed wake leaves the tenant asleep (typed 503 to the
            // caller) — never a crash/breaker episode of its own.
            (Self::Waking, Transition::LaunchFailed) => Self::Hibernated,
            (Self::Waking, Transition::RetryBegin) => Self::Waking,
            (
                Self::Hibernating | Self::Hibernated | Self::Waking,
                Transition::BeginDrain,
            ) => Self::Draining,
            (Self::Draining, Transition::Drained) => Self::Stopped,
            _ => return Err(TransitionError { from: self, event }),
        };
        Ok(next)
    }
}

impl TenantState {
    /// Lossy conversion for atomic storage.
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Ready,
            2 => Self::Draining,
            3 => Self::Stopped,
            4 => Self::Crashed,
            5 => Self::CircuitOpen,
            6 => Self::SwitchFailed,
            7 => Self::Hibernating,
            8 => Self::Hibernated,
            9 => Self::Waking,
            _ => Self::Starting,
        }
    }

    /// `self as u8` companion for `AtomicU8`.
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions_follow_the_table() {
        use TenantState::{CircuitOpen, Crashed, Draining, Starting, Stopped};
        use Transition::{
            BeginDrain, CircuitTrip, CooldownEnd, Crash, Drained, LaunchFailed, RetryBegin,
        };
        let cases = [
            (Starting, Transition::Ready, TenantState::Ready),
            (Starting, LaunchFailed, Starting),
            (Starting, CircuitTrip, CircuitOpen),
            (TenantState::Ready, Crash, Crashed),
            (TenantState::Ready, LaunchFailed, TenantState::Ready),
            // W1-r2: Ready + drained-active + circuit trip = honest
            // outage, not a masked "Ready".
            (TenantState::Ready, CircuitTrip, CircuitOpen),
            (Crashed, RetryBegin, Starting),
            (Crashed, CircuitTrip, CircuitOpen),
            (CircuitOpen, CooldownEnd, Starting),
            (CircuitOpen, LaunchFailed, CircuitOpen),
            (TenantState::Ready, BeginDrain, Draining),
            (Starting, BeginDrain, Draining),
            (Draining, Drained, Stopped),
        ];
        for (from, event, want) in cases {
            assert_eq!(from.transition(event).unwrap(), want, "{from:?} {event:?}");
        }
    }

    #[test]
    fn illegal_transitions_fail_closed() {
        use TenantState::{Draining, Starting, Stopped};
        use Transition::{
            BeginDrain, CircuitTrip, CooldownEnd, Crash, Drained, LaunchFailed, RetryBegin,
        };
        let ready = Transition::Ready;
        // Stopped is terminal — nothing leaves it.
        for event in [
            ready,
            LaunchFailed,
            Crash,
            CircuitTrip,
            CooldownEnd,
            RetryBegin,
            BeginDrain,
            Drained,
        ] {
            assert!(Stopped.transition(event).is_err(), "Stopped + {event:?}");
        }
        // No path skips the drain.
        assert!(TenantState::Ready.transition(Transition::Drained).is_err());
        assert!(TenantState::Ready.transition(ready).is_err());
        assert!(Starting.transition(Crash).is_err());
        assert!(TenantState::Crashed.transition(ready).is_err());
        assert!(TenantState::CircuitOpen.transition(ready).is_err());
        assert!(Draining.transition(ready).is_err());
        assert!(Draining.transition(BeginDrain).is_err());
    }

    #[test]
    fn hibernation_transitions() {
        use TenantState::{Hibernated, Hibernating, Waking};
        use Transition as T;
        assert_eq!(TenantState::Ready.transition(T::HibernateBegin).unwrap(), Hibernating);
        assert_eq!(Hibernating.transition(T::HibernateAbort).unwrap(), TenantState::Ready);
        assert_eq!(Hibernating.transition(T::Hibernated).unwrap(), Hibernated);
        assert_eq!(Hibernated.transition(T::WakeBegin).unwrap(), Waking);
        assert_eq!(Waking.transition(T::Ready).unwrap(), TenantState::Ready);
        assert_eq!(Waking.transition(T::LaunchFailed).unwrap(), Hibernated);
        for s in [Hibernating, Hibernated, Waking] {
            assert_eq!(s.transition(T::BeginDrain).unwrap(), TenantState::Draining);
            assert!(s.transition(T::Crash).is_err());
            assert_eq!(s.transition(T::CircuitTrip).is_ok(), s == Waking);
            assert_eq!(TenantState::from_u8(s.as_u8()), s);
        }
        // Only a Ready tenant may start hibernating; no skipping states.
        assert!(TenantState::Starting.transition(T::HibernateBegin).is_err());
        assert!(TenantState::Crashed.transition(T::HibernateBegin).is_err());
        assert!(Hibernated.transition(T::Ready).is_err());
        assert!(Hibernated.transition(T::HibernateAbort).is_err());
        assert!(Hibernating.transition(T::WakeBegin).is_err());
        assert!(Hibernated.is_asleep() && Waking.is_asleep());
        assert!(!Hibernating.is_asleep());
        for s in [Hibernating, Hibernated, Waking] {
            assert!(!s.can_serve());
            assert!(!s.can_attempt_launch());
        }
    }

    #[test]
    fn only_ready_serves() {
        assert!(TenantState::Ready.can_serve());
        for s in [
            TenantState::Starting,
            TenantState::Crashed,
            TenantState::CircuitOpen,
            TenantState::Draining,
            TenantState::Stopped,
        ] {
            assert!(!s.can_serve());
        }
    }
}
