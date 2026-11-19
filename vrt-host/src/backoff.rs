//! Bounded exponential restart backoff + rolling failure window.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Exponential backoff: `min(max, base * 2^attempt) + jitter`. Jitter is
/// injected (deterministic in tests); the cap is hard — retries never
/// grow unbounded.
#[derive(Clone, Debug)]
pub struct RestartBackoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl RestartBackoff {
    /// `base..=max` delay bounds.
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base: base.min(max),
            max,
            attempt: 0,
        }
    }

    /// Next delay including `jitter_ms` (caller-drawn, e.g. OS entropy or
    /// a test sequence). Saturates on overflow.
    pub fn next_delay(&mut self, jitter_ms: u64) -> Duration {
        let factor = 1u64.checked_shl(self.attempt.min(20)).unwrap_or(u64::MAX);
        let delay = self
            .base
            .saturating_mul(factor as u32)
            .min(self.max)
            .saturating_add(Duration::from_millis(jitter_ms % 1_000));
        self.attempt = self.attempt.saturating_add(1);
        delay
    }

    /// Clear attempt count after a successful ready.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Attempts consumed so far.
    pub fn attempts(&self) -> u32 {
        self.attempt
    }
}

/// Rolling window counting launch failures; when `record` reaches the
/// threshold the supervisor trips the circuit.
#[derive(Debug)]
pub struct FailureWindow {
    window: Duration,
    threshold: usize,
    events: VecDeque<Instant>,
}

impl FailureWindow {
    /// `threshold` failures inside `window` → trip.
    pub fn new(window: Duration, threshold: usize) -> Self {
        Self {
            window,
            threshold: threshold.max(1),
            events: VecDeque::new(),
        }
    }

    /// Record one failure at `now`; returns the in-window count.
    pub fn record(&mut self, now: Instant) -> usize {
        self.events.push_back(now);
        self.prune(now)
    }

    /// In-window failure count at `now`.
    pub fn count(&mut self, now: Instant) -> usize {
        self.prune(now)
    }

    /// Whether the threshold has been reached.
    pub fn tripped(&mut self, now: Instant) -> bool {
        self.count(now) >= self.threshold
    }

    /// Clear all events (after a successful ready or a fresh cooldown).
    pub fn reset(&mut self) {
        self.events.clear();
    }

    fn prune(&mut self, now: Instant) -> usize {
        while let Some(&front) = self.events.front() {
            if now.duration_since(front) > self.window {
                self.events.pop_front();
            } else {
                break;
            }
        }
        self.events.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded_and_exponential() {
        let mut b = RestartBackoff::new(Duration::from_millis(100), Duration::from_secs(2));
        assert_eq!(b.next_delay(0), Duration::from_millis(100));
        assert_eq!(b.next_delay(0), Duration::from_millis(200));
        assert_eq!(b.next_delay(0), Duration::from_millis(400));
        for _ in 0..30 {
            let d = b.next_delay(0);
            assert!(d <= Duration::from_secs(2) + Duration::from_millis(0));
        }
        b.reset();
        assert_eq!(b.next_delay(0), Duration::from_millis(100));
    }

    #[test]
    fn failure_window_trips_at_threshold() {
        let mut w = FailureWindow::new(Duration::from_secs(60), 3);
        let t0 = Instant::now();
        assert_eq!(w.record(t0), 1);
        assert_eq!(w.record(t0 + Duration::from_secs(10)), 2);
        assert!(!w.tripped(t0 + Duration::from_secs(10)));
        assert_eq!(w.record(t0 + Duration::from_secs(20)), 3);
        assert!(w.tripped(t0 + Duration::from_secs(20)));
        // Old events age out (only `> window` is pruned — t0 and t0+10
        // are still inside at t0+70? t0 is 70 s out → pruned, t0+10 is
        // exactly 60 s → kept, t0+20 is 50 s → kept).
        assert_eq!(w.count(t0 + Duration::from_secs(70)), 2);
        assert_eq!(w.count(t0 + Duration::from_secs(80)), 1);
        w.reset();
        assert_eq!(w.count(t0 + Duration::from_secs(80)), 0);
    }
}
