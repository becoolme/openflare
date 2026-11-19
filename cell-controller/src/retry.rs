//! Bounded retries with deterministic jittered backoff.
//!
//! No external RNG: jitter derives from a xorshift64 of `(seed, attempt)` so
//! a fixed seed + fixed attempt sequence is fully reproducible — the same
//! determinism contract `vrt-host::backoff` uses for restart jitter.

use std::time::Duration;

/// A bounded retry plan: at most `max_attempts` total attempts, exponential
/// backoff `base * 2^attempt` capped at `max_delay`, jittered ±25%.
#[derive(Clone, Debug)]
pub struct RetryPlan {
    /// Total attempts including the first (≥1).
    pub max_attempts: usize,
    /// First backoff slice.
    pub base_delay: Duration,
    /// Backoff ceiling before jitter.
    pub max_delay: Duration,
    /// Jitter seed — per-site distinct values decorrelate pollers.
    pub jitter_seed: u64,
}

impl Default for RetryPlan {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            jitter_seed: 0x5C311_D31BED,
        }
    }
}

impl RetryPlan {
    /// Backoff to sleep before attempt `failed_attempts + 1`
    /// (`failed_attempts` counts attempts already made and failed).
    pub fn delay_for(&self, failed_attempts: usize) -> Duration {
        let shift = (failed_attempts.saturating_sub(1)).min(16) as u32;
        let base_ms = self
            .base_delay
            .as_millis()
            .saturating_mul(1u128 << shift)
            .min(self.max_delay.as_millis()) as u64;
        let span = (base_ms / 4).max(1);
        let jitter = xorshift64(
            self.jitter_seed ^ (failed_attempts as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        ) % (2 * span + 1);
        Duration::from_millis(base_ms.saturating_add(jitter).saturating_sub(span))
    }

    /// Whether another attempt may be made after `failed_attempts` failures.
    pub fn can_retry(&self, failed_attempts: usize) -> bool {
        failed_attempts < self.max_attempts
    }
}

fn xorshift64(mut x: u64) -> u64 {
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_attempts() {
        let plan = RetryPlan {
            max_attempts: 3,
            ..RetryPlan::default()
        };
        assert!(plan.can_retry(0) && plan.can_retry(1) && plan.can_retry(2));
        assert!(!plan.can_retry(3));
    }

    #[test]
    fn deterministic_backoff_within_bounds() {
        let plan = RetryPlan::default();
        let a = plan.delay_for(2);
        let b = plan.delay_for(2);
        assert_eq!(a, b, "same seed + attempt must reproduce the same delay");
        for attempt in 0..6 {
            let d = plan.delay_for(attempt);
            assert!(d <= plan.max_delay + plan.max_delay / 2);
        }
    }

    #[test]
    fn backoff_grows_exponentially() {
        let plan = RetryPlan {
            jitter_seed: 0,
            ..RetryPlan::default()
        };
        assert!(plan.delay_for(2) >= plan.delay_for(1));
    }
}
