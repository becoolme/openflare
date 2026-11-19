//! RV-R67: per-tenant QPS + byte/s quota applied ONCE at the provider's
//! shared orchestration entry ([`crate::server::tenant_router_with_quota`]),
//! so KV / R2 / D1 / Queue / PG / assets are all covered without any
//! per-product copy. One [`TenantQuota`] lives inside one tenant
//! listener (tenant identity is the unix-socket binding, never a request
//! field), so the bucket table is bounded by live listeners and dies with
//! them — no unbounded map keyed by request data.
//!
//! UNCOMPILED (RV-R67): written without a cargo run; first compile may
//! need small signature fixes. Tests below are the intended contract.

use std::sync::Mutex;
use std::time::Instant;

/// Default sustained requests/second per tenant (burst = 2 s worth).
pub const DEFAULT_TENANT_QPS: u32 = 2_000;
/// Default sustained declared request bytes/second per tenant
/// (burst = 2 s worth; always >= the R2 single-part cap default).
pub const DEFAULT_TENANT_BYTES_PER_SEC: u64 = 256 * 1024 * 1024;
const BURST_SECONDS: f64 = 2.0;

/// Which dimension ran out (typed `QuotaExceeded`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaExceeded {
    /// Request-rate bucket empty.
    Qps,
    /// Declared-bytes bucket empty.
    Bytes,
}

impl QuotaExceeded {
    /// Fixed, bounded metric/log label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Qps => "qps",
            Self::Bytes => "bytes",
        }
    }
}

#[derive(Debug)]
struct State {
    qps: f64,
    bytes: f64,
    last: Instant,
}

/// Token buckets for one tenant. `check` is atomic: both buckets must
/// admit the request or neither is charged.
#[derive(Debug)]
pub struct TenantQuota {
    enabled: bool,
    qps_rate: f64,
    qps_cap: f64,
    byte_rate: f64,
    byte_cap: f64,
    state: Mutex<State>,
}

impl TenantQuota {
    /// Quota with the given sustained rates (both must be >= 1;
    /// config validation enforces the range).
    pub fn new(qps: u32, bytes_per_sec: u64, now: Instant) -> Self {
        let qps_rate = f64::from(qps.max(1));
        let byte_rate = (bytes_per_sec.max(1)) as f64;
        let qps_cap = qps_rate * BURST_SECONDS;
        let byte_cap = byte_rate * BURST_SECONDS;
        Self {
            enabled: true,
            qps_rate,
            qps_cap,
            byte_rate,
            byte_cap,
            state: Mutex::new(State { qps: qps_cap, bytes: byte_cap, last: now }),
        }
    }

    /// No limits (library default for `tenant_router`; production
    /// wiring passes configured limits via `tenant_router_with_quota`).
    pub fn unlimited() -> Self {
        let mut q = Self::new(1, 1, Instant::now());
        q.enabled = false;
        q
    }

    /// Admit one request carrying `declared_bytes` (Content-Length; a
    /// chunked body declares 0 — it is still bounded by the per-family
    /// body caps, see doc). A request larger than the whole byte burst
    /// is charged the full burst (it can still pass when the bucket is
    /// full) rather than being unsatisfiable forever.
    pub fn check(&self, now: Instant, declared_bytes: u64) -> Result<(), QuotaExceeded> {
        if !self.enabled {
            return Ok(());
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if now > s.last {
            let dt = now.duration_since(s.last).as_secs_f64();
            s.qps = (s.qps + dt * self.qps_rate).min(self.qps_cap);
            s.bytes = (s.bytes + dt * self.byte_rate).min(self.byte_cap);
            s.last = now;
        }
        let charge = (declared_bytes as f64).min(self.byte_cap);
        if s.qps < 1.0 {
            return Err(QuotaExceeded::Qps);
        }
        if s.bytes < charge {
            return Err(QuotaExceeded::Bytes);
        }
        s.qps -= 1.0;
        s.bytes -= charge;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn qps_burst_then_refuse_then_refill() {
        let t0 = Instant::now();
        let q = TenantQuota::new(10, 1 << 30, t0);
        for _ in 0..20 {
            assert_eq!(q.check(t0, 0), Ok(()));
        }
        assert_eq!(q.check(t0, 0), Err(QuotaExceeded::Qps));
        // 0.5 s refills 5 tokens.
        let t1 = t0 + Duration::from_millis(500);
        for _ in 0..5 {
            assert_eq!(q.check(t1, 0), Ok(()));
        }
        assert_eq!(q.check(t1, 0), Err(QuotaExceeded::Qps));
    }

    #[test]
    fn bytes_exhaust_independently_and_are_not_charged_on_refusal() {
        let t0 = Instant::now();
        let q = TenantQuota::new(1000, 100, t0); // byte cap 200
        assert_eq!(q.check(t0, 150), Ok(()));
        assert_eq!(q.check(t0, 100), Err(QuotaExceeded::Bytes));
        // The refused request did not consume a qps token or bytes.
        assert_eq!(q.check(t0, 50), Ok(()));
    }

    #[test]
    fn oversized_request_is_clamped_to_burst_not_unsatisfiable() {
        let t0 = Instant::now();
        let q = TenantQuota::new(1000, 100, t0);
        assert_eq!(q.check(t0, u64::MAX), Ok(()));
        assert_eq!(q.check(t0, 1), Err(QuotaExceeded::Bytes));
    }

    #[test]
    fn tenants_are_isolated() {
        let t0 = Instant::now();
        let a = TenantQuota::new(1, 1 << 30, t0);
        let b = TenantQuota::new(1, 1 << 30, t0);
        while a.check(t0, 0).is_ok() {}
        assert_eq!(a.check(t0, 0), Err(QuotaExceeded::Qps));
        assert_eq!(b.check(t0, 0), Ok(()));
    }

    #[test]
    fn clock_going_backwards_never_refills_or_panics() {
        let t0 = Instant::now() + Duration::from_secs(10);
        let q = TenantQuota::new(1, 1 << 30, t0);
        while q.check(t0, 0).is_ok() {}
        assert_eq!(q.check(t0 - Duration::from_secs(5), 0), Err(QuotaExceeded::Qps));
    }

    #[test]
    fn unlimited_never_refuses() {
        let q = TenantQuota::unlimited();
        for _ in 0..10_000 {
            assert_eq!(q.check(Instant::now(), u64::MAX), Ok(()));
        }
    }
}
