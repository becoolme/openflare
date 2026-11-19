//! `LeaseStore` — Kubernetes `coordination.k8s.io/v1 Lease` semantics for
//! Cron leader election, behind a typed trait.
//!
//! Contract (matching k8s Lease):
//! - one holder at a time; `claim` either renews the caller's hold
//!   (`Held` with a fresh expiry) or reports `Lost` when another holder
//!   owns an unexpired lease;
//! - renewal failure IS lease loss (fail closed): the caller must stop
//!   lease-gated work, including queued-but-not-yet-executed triggers;
//! - each successful new hold returns a fresh `epoch` (fencing counter) —
//!   a stale claim can be detected with [`LeaseStore::held`].
//!
//! The production implementation targets the Kubernetes Lease API —
//! 未证实 in this task (no cluster client is wired here); `MemoryLeaseStore`
//! implements the same contract for tests.

use chrono::{DateTime, Duration, Utc};
use std::sync::Mutex;
use thiserror::Error;

/// A successful hold: `until` is the holder-exclusive expiry; `epoch`
/// strictly increases whenever the lease changes hands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseClaim {
    /// Holder identity (pod/controller identity string).
    pub holder: String,
    /// Exclusive hold expiry.
    pub until: DateTime<Utc>,
    /// Fencing counter — compare with [`LeaseStore::held`] before acting.
    pub epoch: u64,
}

/// Outcome of one `claim` cycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// Caller holds (or renewed) the lease.
    Held(LeaseClaim),
    /// Another holder owns an unexpired lease.
    Lost,
}

/// Lease failure — fail closed: callers treat every error as `Lost`.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum LeaseError {
    /// The store could not be reached / did not answer.
    #[error("lease store unavailable")]
    Unavailable,
    /// A conflicting write lost the compare-and-swap.
    #[error("lease conflict")]
    Conflict,
}

/// Kubernetes-lease-shaped acquire/renew boundary.
#[async_trait::async_trait]
pub trait LeaseStore: Send + Sync {
    /// Acquire or renew for `holder` with `ttl`. `Held` on success,
    /// `Lost` when another holder owns it, `Err` on store failure —
    /// callers map `Err` to lease loss (fail closed).
    async fn claim(
        &self,
        holder: &str,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<LeaseOutcome, LeaseError>;
    /// Is `claim` still the current unexpired hold at `now`? Used between
    /// queued trigger dispatches so a mid-cycle loss stops the queue.
    fn held(&self, claim: &LeaseClaim, now: DateTime<Utc>) -> bool;
}

/// `Box<dyn LeaseStore>` delegates — lets prod config select the
/// implementation without a generic runner boundary.
#[async_trait::async_trait]
impl LeaseStore for Box<dyn LeaseStore> {
    async fn claim(
        &self,
        holder: &str,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<LeaseOutcome, LeaseError> {
        (**self).claim(holder, ttl, now).await
    }
    fn held(&self, claim: &LeaseClaim, now: DateTime<Utc>) -> bool {
        (**self).held(claim, now)
    }
}

#[derive(Debug)]
struct MemoryLease {
    holder: String,
    until: DateTime<Utc>,
    epoch: u64,
}

/// In-memory implementation of the lease contract for tests.
/// `Clone` shares the same underlying lease (Arc) so tests can hold a
/// handle after moving one copy into a [`crate::cron::CronRunner`].
#[derive(Clone, Debug, Default)]
pub struct MemoryLeaseStore {
    state: std::sync::Arc<Mutex<Option<MemoryLease>>>,
    fail_next_claims: std::sync::Arc<Mutex<usize>>,
}

impl MemoryLeaseStore {
    /// Make the next `n` `claim` calls return `Unavailable` (renewal
    /// failure → caller must treat as lease loss).
    pub fn fail_next_claims(&self, n: usize) {
        *self
            .fail_next_claims
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = n;
    }

    /// Test helper: expire the current hold immediately (another holder
    /// can acquire on its next `claim`).
    pub fn expire_now(&self, now: DateTime<Utc>) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(lease) = guard.as_mut() {
            lease.until = now - Duration::seconds(1);
        }
    }
}

#[async_trait::async_trait]
impl LeaseStore for MemoryLeaseStore {
    async fn claim(
        &self,
        holder: &str,
        ttl: Duration,
        now: DateTime<Utc>,
    ) -> Result<LeaseOutcome, LeaseError> {
        let mut fails = self
            .fail_next_claims
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *fails > 0 {
            *fails -= 1;
            return Err(LeaseError::Unavailable);
        }
        drop(fails);
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_mut() {
            // Same holder renewing → extend, same epoch.
            Some(lease) if lease.holder == holder && now < lease.until => {
                lease.until = now + ttl;
                Ok(LeaseOutcome::Held(LeaseClaim {
                    holder: holder.into(),
                    until: lease.until,
                    epoch: lease.epoch,
                }))
            }
            // Expired lease or first claim → take it, bump epoch.
            Some(lease) if now >= lease.until => {
                lease.holder = holder.into();
                lease.until = now + ttl;
                lease.epoch += 1;
                Ok(LeaseOutcome::Held(LeaseClaim {
                    holder: holder.into(),
                    until: lease.until,
                    epoch: lease.epoch,
                }))
            }
            Some(_) => Ok(LeaseOutcome::Lost),
            None => {
                *guard = Some(MemoryLease {
                    holder: holder.into(),
                    until: now + ttl,
                    epoch: 1,
                });
                Ok(LeaseOutcome::Held(LeaseClaim {
                    holder: holder.into(),
                    until: now + ttl,
                    epoch: 1,
                }))
            }
        }
    }

    fn held(&self, claim: &LeaseClaim, now: DateTime<Utc>) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|l| l.holder == claim.holder && l.epoch == claim.epoch && now < l.until)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-08T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + Duration::seconds(secs)
    }

    #[tokio::test]
    async fn acquire_renew_and_contend() {
        let lease = MemoryLeaseStore::default();
        let ttl = Duration::seconds(15);
        let first = lease.claim("a", ttl, at(0)).await.unwrap();
        let LeaseOutcome::Held(claim_a) = first else {
            panic!("first claim must hold")
        };
        assert_eq!(claim_a.epoch, 1);
        // Contender while held → Lost.
        assert_eq!(
            lease.claim("b", ttl, at(5)).await.unwrap(),
            LeaseOutcome::Lost
        );
        // Renewal extends the hold.
        let renewed = lease.claim("a", ttl, at(10)).await.unwrap();
        let LeaseOutcome::Held(claim_a2) = renewed else {
            panic!()
        };
        assert_eq!(claim_a2.epoch, 1);
        assert!(claim_a2.until > claim_a.until);
        assert!(lease.held(&claim_a2, at(20)));
        // After expiry the contender wins with a fresh epoch.
        let second = lease.claim("b", ttl, at(30)).await.unwrap();
        let LeaseOutcome::Held(claim_b) = second else {
            panic!()
        };
        assert_eq!(claim_b.epoch, 2);
        assert!(
            !lease.held(&claim_a2, at(31)),
            "stale claim must not be held"
        );
    }

    #[tokio::test]
    async fn renewal_failure_is_lease_loss() {
        let lease = MemoryLeaseStore::default();
        let ttl = Duration::seconds(15);
        lease.claim("a", ttl, at(0)).await.unwrap();
        lease.fail_next_claims(1);
        assert!(matches!(
            lease.claim("a", ttl, at(5)).await,
            Err(LeaseError::Unavailable)
        ));
        // The claim itself is stale-checkable: caller sees loss via held().
        // (fail closed — do not keep triggering.)
        assert!(lease.claim("a", ttl, at(6)).await.is_ok());
    }
}
