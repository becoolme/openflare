//! Injectable clock (T36): lease expiry/renewal/boundary decisions all go through `Clock`,
//! so tests can advance time deterministically.

use std::sync::atomic::{AtomicU64, Ordering};

/// Millisecond wall clock.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// Unix epoch milliseconds.
    fn now_ms(&self) -> u64;
}

/// System clock.
#[derive(Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// Test clock: manually advanced, for deterministic boundary tests.
#[derive(Debug)]
pub struct TestClock {
    t: AtomicU64,
}

impl TestClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            t: AtomicU64::new(start_ms),
        }
    }
    pub fn advance(&self, ms: u64) {
        self.t.fetch_add(ms, Ordering::SeqCst);
    }
    pub fn set(&self, ms: u64) {
        self.t.store(ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.t.load(Ordering::SeqCst)
    }
}
