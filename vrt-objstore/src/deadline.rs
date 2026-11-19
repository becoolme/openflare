//! RV-R71: shared `Deadline` + jittered exponential `RetryPlan`.
//!
//! Lives in `vrt-objstore` because it is the lowest crate both
//! `vrt-sql` and `vrt-provider` already depend on (no new crate, no
//! cycle). `cell-controller` keeps its own deterministic `RetryPlan`
//! (different seeding contract for reproducible controller tests); see
//! doc/platform/workers-runtime-sql-storage.md "Timeouts and deadline
//! propagation".
//!
//! Rules: retries are only for idempotent operations; every retry loop
//! is bounded by BOTH an attempt count and a `Deadline`; a sleep that
//! would cross the deadline is never started.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::StoreError;

/// An absolute point in time by which a whole operation must finish.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    at: Instant,
}

impl Deadline {
    /// Deadline `budget` from now.
    pub fn after(budget: Duration) -> Self {
        Self {
            at: Instant::now() + budget,
        }
    }
    /// Deadline at an absolute instant.
    pub fn at(at: Instant) -> Self {
        Self { at }
    }
    /// Time left (zero once expired).
    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }
    /// True once the deadline has passed.
    pub fn expired(&self) -> bool {
        self.remaining().is_zero()
    }
    /// The earlier of two deadlines — a child budget can only shrink.
    pub fn min(self, other: Deadline) -> Deadline {
        Deadline {
            at: self.at.min(other.at),
        }
    }
    /// Clamp a per-attempt timeout to the remaining budget.
    pub fn clamp(&self, per_attempt: Duration) -> Duration {
        per_attempt.min(self.remaining())
    }
    /// The tokio instant form, for `timeout_at`.
    pub fn tokio_instant(&self) -> tokio::time::Instant {
        tokio::time::Instant::from_std(self.at)
    }
}

/// Bounded exponential backoff with jitter.
#[derive(Clone, Debug)]
pub struct RetryPlan {
    /// Total attempts including the first (>= 1).
    pub max_attempts: u32,
    /// First backoff slice.
    pub base_delay: Duration,
    /// Backoff ceiling before jitter.
    pub max_delay: Duration,
}

impl Default for RetryPlan {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
        }
    }
}

static JITTER_STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

fn next_random() -> u64 {
    // splitmix64 over a shared counter mixed with the clock: cheap,
    // lock-free, decorrelates concurrent retriers (no RNG dependency).
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut z = JITTER_STATE
        .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        .wrapping_add(nanos);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl RetryPlan {
    /// Capped exponential ceiling before jitter for the sleep that
    /// follows `failed_attempts` (>= 1) failures.
    pub fn ceiling_for(&self, failed_attempts: u32) -> Duration {
        let shift = failed_attempts.saturating_sub(1).min(16);
        let ms = self
            .base_delay
            .as_millis()
            .saturating_mul(1u128 << shift)
            .min(self.max_delay.as_millis()) as u64;
        Duration::from_millis(ms)
    }

    /// Backoff with "equal jitter": uniformly in [ceiling/2, ceiling].
    /// `rand` is injected so tests are deterministic.
    pub fn delay_with(&self, failed_attempts: u32, rand: u64) -> Duration {
        let ceil = self.ceiling_for(failed_attempts).as_millis() as u64;
        let half = ceil / 2;
        let span = ceil - half;
        Duration::from_millis(half + if span == 0 { 0 } else { rand % (span + 1) })
    }

    /// Production backoff (random jitter).
    pub fn delay_for(&self, failed_attempts: u32) -> Duration {
        self.delay_with(failed_attempts, next_random())
    }
}

/// Run an IDEMPOTENT `f` with bounded, jittered retries under a
/// deadline. Only `Unavailable{retryable:true}` is retried. Each
/// attempt is cut at the deadline; a backoff sleep that would not leave
/// time for another attempt ends the loop with the last error.
/// Never use for conditional writes (CAS) — those are single-shot.
pub async fn retry_until<F, Fut, T>(
    plan: &RetryPlan,
    deadline: Deadline,
    mut f: F,
) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    let mut last = StoreError::unavailable(true, "deadline exceeded");
    let attempts = plan.max_attempts.max(1);
    for i in 0..attempts {
        if deadline.expired() {
            return Err(last);
        }
        let outcome = match tokio::time::timeout_at(deadline.tokio_instant(), f()).await {
            Ok(r) => r,
            Err(_) => return Err(StoreError::unavailable(true, "deadline exceeded")),
        };
        match outcome {
            Ok(v) => return Ok(v),
            Err(
                e @ StoreError::Unavailable {
                    retryable: true, ..
                },
            ) => {
                last = e;
                if i + 1 >= attempts {
                    break;
                }
                let delay = plan.delay_for(i + 1);
                if delay >= deadline.remaining() {
                    // Not enough budget left for sleep + another attempt.
                    break;
                }
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    fn retryable() -> StoreError {
        StoreError::unavailable(true, "boom")
    }

    #[test]
    fn backoff_is_exponential_capped_and_jittered() {
        let plan = RetryPlan {
            max_attempts: 10,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(400),
        };
        // ceiling doubles then caps
        assert_eq!(plan.ceiling_for(1), Duration::from_millis(50));
        assert_eq!(plan.ceiling_for(2), Duration::from_millis(100));
        assert_eq!(plan.ceiling_for(4), Duration::from_millis(400));
        assert_eq!(plan.ceiling_for(30), Duration::from_millis(400));
        // jitter spans [ceil/2, ceil] and is not constant
        let lo = plan.delay_with(4, 0);
        let hi = plan.delay_with(4, u64::MAX);
        assert_eq!(lo, Duration::from_millis(200));
        assert!(hi <= Duration::from_millis(400) && hi > lo);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let d = plan.delay_for(4);
            assert!(d >= Duration::from_millis(200) && d <= Duration::from_millis(400));
            seen.insert(d);
        }
        assert!(seen.len() > 1, "production jitter must vary");
    }

    #[test]
    fn child_deadline_can_only_shrink() {
        let long = Deadline::after(Duration::from_secs(60));
        let short = Deadline::after(Duration::from_millis(10));
        assert!(long.min(short).remaining() <= Duration::from_millis(10));
        assert!(short.min(long).remaining() <= Duration::from_millis(10));
        assert!(long.clamp(Duration::from_secs(5)) <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn retries_then_succeeds_within_attempts() {
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let plan = RetryPlan {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
        };
        let r = retry_until(&plan, Deadline::after(Duration::from_secs(5)), || {
            let n = n2.clone();
            async move {
                if n.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(retryable())
                } else {
                    Ok(7)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 7);
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn attempt_count_is_bounded() {
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let plan = RetryPlan {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
        };
        let r: Result<(), _> = retry_until(&plan, Deadline::after(Duration::from_secs(5)), || {
            let n = n2.clone();
            async move {
                n.fetch_add(1, Ordering::SeqCst);
                Err(retryable())
            }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn non_retryable_and_semantic_errors_are_not_retried() {
        for err in [
            StoreError::unavailable(false, "nope"),
            StoreError::PreconditionFailed,
            StoreError::NotFound,
        ] {
            let n = Arc::new(AtomicU32::new(0));
            let n2 = n.clone();
            let e2 = err.to_string();
            let r: Result<(), _> = retry_until(
                &RetryPlan::default(),
                Deadline::after(Duration::from_secs(5)),
                || {
                    let n = n2.clone();
                    let err = match e2.as_str() {
                        "precondition failed" => StoreError::PreconditionFailed,
                        "object not found" => StoreError::NotFound,
                        _ => StoreError::unavailable(false, "nope"),
                    };
                    async move {
                        n.fetch_add(1, Ordering::SeqCst);
                        Err(err)
                    }
                },
            )
            .await;
            assert!(r.is_err());
            assert_eq!(n.load(Ordering::SeqCst), 1, "{e2}");
        }
    }

    #[tokio::test]
    async fn deadline_stops_retrying_before_attempts_run_out() {
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let plan = RetryPlan {
            max_attempts: 1000,
            base_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(20),
        };
        let start = Instant::now();
        let r: Result<(), _> = retry_until(&plan, Deadline::after(Duration::from_millis(120)), || {
            let n = n2.clone();
            async move {
                n.fetch_add(1, Ordering::SeqCst);
                Err(retryable())
            }
        })
        .await;
        assert!(r.is_err());
        assert!(start.elapsed() < Duration::from_millis(400));
        assert!(n.load(Ordering::SeqCst) < 20);
    }

    #[tokio::test]
    async fn hung_attempt_is_cut_at_the_deadline() {
        let start = Instant::now();
        let r: Result<(), _> = retry_until(
            &RetryPlan::default(),
            Deadline::after(Duration::from_millis(80)),
            || async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(())
            },
        )
        .await;
        assert!(matches!(
            r,
            Err(StoreError::Unavailable { retryable: true, .. })
        ));
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
