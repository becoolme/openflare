//! PT1 runaway watchdog — dual-signal detector for CPU-spin tenants.
//!
//! Why two signals: CPU saturation alone cannot distinguish "runaway"
//! from "legitimately busy"; heartbeat failure alone cannot distinguish
//! "spinning" from "blocked on I/O". Only BOTH together — sustained
//! high CPU AND a dead in-process event loop — prove a runaway.
//!
//! Signal 1 (CPU saturation): consecutive `cpu_seconds` samples are
//! differenced; the ratio delta/elapsed must stay >= `min_cpu_ratio`
//! continuously for `window`. A missing sample (`None`) is NEVER
//! treated as zero — signal 1 is simply unproven that tick (the
//! process may also have died; the exit path owns that verdict).
//!
//! Signal 2 (event-loop dead): a bounded HTTP GET on the tenant's
//! in-process `vrt-heartbeat` worker — rendered into the SAME workerd
//! process on a private socket — must fail `heartbeat_failures` times
//! in a row. TCP handshakes prove nothing (kernel accepts), so the
//! probe requires a response byte; a spinning event loop never
//! services the connection.
//!
//! Action on `Runaway`: SIGKILL the process GROUP (`kill(-pgid)` —
//! the generation cannot drain a spin; graceful restart would just
//! leave it burning). The crash path then owns reaping, breaker
//! accounting, and (fast) replacement.

use crate::endpoint::WorkerEndpoint;
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

/// Resolved watchdog parameters (from `LimitsConfig::cpuRunaway`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RunawaySpec {
    /// CPU must stay >= `min_cpu_ratio` for this long before signal 1
    /// counts. Should cover >= 2 monitor ticks.
    pub window: Duration,
    /// Minimum CPU ratio (cores busy / wall time). A tight JS spin
    /// pins ~1 core => 0.9 catches it while tolerating sched noise.
    pub min_cpu_ratio: f64,
    /// Per-probe response timeout — the event-loop-dead detector.
    pub heartbeat_timeout: Duration,
    /// Consecutive probe failures required for signal 2.
    pub heartbeat_failures: u32,
}

/// What the detector concluded at one observation. Typed — metrics
/// carry the code, never a free-form string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunawayVerdict {
    /// Neither signal asserted.
    Healthy = 0,
    /// CPU saturated for the window but heartbeat alive — busy, not
    /// runaway; metric only.
    CpuOnly = 1,
    /// Heartbeat dead but CPU not saturated — blocked/stuck, not a
    /// CPU runaway; metric only.
    HeartbeatOnly = 2,
    /// The sampler could not read `cpu_seconds` — signal 1 unprovable
    /// this tick (never counted as zero).
    CpuUnavailable = 3,
    /// Both signals asserted — kill the process group.
    Runaway = 4,
}

impl RunawayVerdict {
    /// Stable wire code for the `vrt_tenant_runaway_last_verdict` gauge.
    pub fn code(self) -> u64 {
        self as u64
    }
}

/// Pure state machine — inject (now, cpu_seconds, heartbeat_ok) per
/// monitor tick; the supervisor supplies real samples, tests inject
/// synthetic sequences.
#[derive(Debug)]
pub struct RunawayDetector {
    cfg: RunawaySpec,
    /// Last (sample instant, cpu_seconds) — `None` until seeded.
    prev_cpu: Option<(Instant, f64)>,
    /// When continuous saturation began (`None` = not saturated).
    saturated_since: Option<Instant>,
    /// Consecutive heartbeat probe failures.
    hb_failures: u32,
}

impl RunawayDetector {
    /// Fresh detector for a resolved config.
    pub fn new(cfg: RunawaySpec) -> Self {
        Self {
            cfg,
            prev_cpu: None,
            saturated_since: None,
            hb_failures: 0,
        }
    }

    /// A probe result arrived (PT1-r2: probes run off-actor, results
    /// are delivered asynchronously). Updates ONLY the failure streak;
    /// verdicts come from `observe_cpu`.
    pub fn note_probe(&mut self, ok: bool) {
        if ok {
            self.hb_failures = 0;
        } else {
            self.hb_failures = self.hb_failures.saturating_add(1);
        }
    }

    /// CPU gate for probes (PT1-r2): only spend a probe when this
    /// interval already looks saturated — the runaway candidate — or
    /// when a failure streak is in progress and must keep measuring.
    /// A cold tenant is never probed.
    pub fn should_probe(&self) -> bool {
        self.saturated_since.is_some() || self.hb_failures > 0
    }

    /// CPU-side observation — called once per monitor tick. The
    /// heartbeat streak is advanced separately by `note_probe` as
    /// probe results land.
    pub fn observe_cpu(&mut self, now: Instant, cpu_seconds: Option<f64>) -> RunawayVerdict {
        let signal2 = self.hb_failures >= self.cfg.heartbeat_failures;

        // Signal 1 — sustained saturation. A `None` sample proves
        // nothing: unproven is not zero, and saturation cannot carry
        // across a measurement gap (the delta would span unmeasured
        // wall time).
        let signal1 = match cpu_seconds {
            None => {
                self.prev_cpu = None;
                self.saturated_since = None;
                false
            }
            Some(c) => match self.prev_cpu {
                Some((t0, c0)) if c >= c0 && now > t0 => {
                    let ratio = (c - c0) / now.duration_since(t0).as_secs_f64();
                    if ratio >= self.cfg.min_cpu_ratio {
                        self.saturated_since = Some(self.saturated_since.unwrap_or(now));
                    } else {
                        self.saturated_since = None;
                    }
                    self.prev_cpu = Some((now, c));
                    self.saturated_since
                        .is_some_and(|t| now.duration_since(t) >= self.cfg.window)
                }
                // First sample, cpu regression, or clock jitter —
                // re-seed; one interval of saturation never counts.
                _ => {
                    self.prev_cpu = Some((now, c));
                    false
                }
            },
        };

        match (signal1, signal2, cpu_seconds.is_none()) {
            (true, true, _) => RunawayVerdict::Runaway,
            (true, false, _) => RunawayVerdict::CpuOnly,
            (false, true, _) => RunawayVerdict::HeartbeatOnly,
            (false, false, true) => RunawayVerdict::CpuUnavailable,
            _ => RunawayVerdict::Healthy,
        }
    }

    /// Inline observation — feeds one probe result then observes.
    /// Kept for single-shot/unit-test paths where the probe answered
    /// within the tick; the actor path uses `observe_cpu` + `note_probe`.
    #[cfg(test)]
    pub fn observe(
        &mut self,
        now: Instant,
        cpu_seconds: Option<f64>,
        heartbeat_ok: bool,
    ) -> RunawayVerdict {
        self.note_probe(heartbeat_ok);
        self.observe_cpu(now, cpu_seconds)
    }
}

/// Bounded event-loop liveness probe — the supervisor injects it so
/// unit tests never touch sockets.
pub trait HeartbeatProbe: Send + Sync {
    /// `true` iff the endpoint answered the GET within `timeout`.
    fn probe(&self, endpoint: &WorkerEndpoint, timeout: Duration) -> bool;
}

/// Production probe — HTTP/1.1 GET on the private heartbeat socket.
/// A connect alone is NOT success (the kernel completes handshakes for
/// a stalled event loop); a response byte is required.
pub struct OsHeartbeatProbe;

impl HeartbeatProbe for OsHeartbeatProbe {
    fn probe(&self, endpoint: &WorkerEndpoint, timeout: Duration) -> bool {
        match endpoint {
            WorkerEndpoint::Unix(path) => unix_probe(path, timeout),
            #[cfg(feature = "dev-stub")]
            WorkerEndpoint::Tcp(addr) => tcp_probe(*addr, timeout),
        }
    }
}

fn unix_probe(path: &Path, timeout: Duration) -> bool {
    let Ok(s) = std::os::unix::net::UnixStream::connect(path) else {
        return false;
    };
    probe_stream(s, timeout)
}

#[cfg(feature = "dev-stub")]
fn tcp_probe(addr: std::net::SocketAddr, timeout: Duration) -> bool {
    let Ok(s) = std::net::TcpStream::connect_timeout(&addr, timeout) else {
        return false;
    };
    probe_stream(s, timeout)
}

fn probe_stream<S>(s: S, timeout: Duration) -> bool
where
    S: Read + Write + SetReadWriteTimeout,
{
    if s.set_read_timeout(Some(timeout)).is_err() || s.set_write_timeout(Some(timeout)).is_err() {
        return false;
    }
    let mut s = s;
    if s.write_all(b"GET / HTTP/1.1\r\nhost: vrt-heartbeat\r\nconnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut byte = [0u8; 1];
    matches!(s.read(&mut byte), Ok(1))
}

/// `set_read_timeout`/`set_write_timeout` shared by unix+tcp streams
/// (not a std trait).
trait SetReadWriteTimeout {
    fn set_read_timeout(&self, t: Option<Duration>) -> std::io::Result<()>;
    fn set_write_timeout(&self, t: Option<Duration>) -> std::io::Result<()>;
}

impl SetReadWriteTimeout for std::os::unix::net::UnixStream {
    fn set_read_timeout(&self, t: Option<Duration>) -> std::io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, t)
    }
    fn set_write_timeout(&self, t: Option<Duration>) -> std::io::Result<()> {
        std::os::unix::net::UnixStream::set_write_timeout(self, t)
    }
}

#[cfg(feature = "dev-stub")]
impl SetReadWriteTimeout for std::net::TcpStream {
    fn set_read_timeout(&self, t: Option<Duration>) -> std::io::Result<()> {
        std::net::TcpStream::set_read_timeout(self, t)
    }
    fn set_write_timeout(&self, t: Option<Duration>) -> std::io::Result<()> {
        std::net::TcpStream::set_write_timeout(self, t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RunawaySpec {
        RunawaySpec {
            window: Duration::from_secs(10),
            min_cpu_ratio: 0.9,
            heartbeat_timeout: Duration::from_millis(250),
            heartbeat_failures: 2,
        }
    }

    /// Deterministic virtual clock — every tick shares one base, so
    /// inter-tick `duration_since` is an EXACT number of seconds.
    /// (`Instant::now()` per call injects µs skew: `0.9/(1s+ε)` lands
    /// just under the 0.9 threshold and flips the edge verdicts.)
    fn tick(base: Instant, sec: u64) -> Instant {
        base + Duration::from_secs(sec)
    }

    #[test]
    fn runaway_requires_both_signals() {
        let b = Instant::now();
        let mut d = RunawayDetector::new(cfg());
        // Seed.
        assert_eq!(
            d.observe(tick(b, 0), Some(0.0), true),
            RunawayVerdict::Healthy
        );
        // Saturate +1.0cpu/+1.0s — saturated_since latches at t=1,
        // signal1 needs the full 10s window: Healthy until t=11.
        for s in 1..=10 {
            assert_eq!(
                d.observe(tick(b, s), Some(s as f64), true),
                RunawayVerdict::Healthy,
                "tick {s}: inside saturation window"
            );
        }
        // From t=11 the window is satisfied -> CpuOnly, and it stays
        // CpuOnly forever while the heartbeat answers (busy-but-alive
        // must never be killed).
        for s in 11..=30 {
            assert_eq!(
                d.observe(tick(b, s), Some(s as f64), true),
                RunawayVerdict::CpuOnly,
                "tick {s}: alive heartbeat must block the kill"
            );
        }
        // Event loop dies: first failure is below the streak -> still
        // CpuOnly; second consecutive failure trips BOTH -> Runaway.
        assert_eq!(
            d.observe(tick(b, 31), Some(31.0), false),
            RunawayVerdict::CpuOnly
        );
        assert_eq!(
            d.observe(tick(b, 32), Some(32.0), false),
            RunawayVerdict::Runaway
        );
    }

    #[test]
    fn heartbeat_dead_alone_never_kills() {
        let b = Instant::now();
        let mut d = RunawayDetector::new(cfg());
        assert_eq!(
            d.observe(tick(b, 0), Some(0.0), true),
            RunawayVerdict::Healthy
        );
        // Heartbeat fails while CPU idles (waiting on I/O) — signal 2
        // alone marks HeartbeatOnly, never a runaway.
        assert_eq!(
            d.observe(tick(b, 1), Some(0.001), false),
            RunawayVerdict::Healthy
        );
        for s in 2..=10 {
            assert_eq!(
                d.observe(tick(b, s), Some(0.001 * s as f64), false),
                RunawayVerdict::HeartbeatOnly,
                "tick {s}: low CPU must block the kill"
            );
        }
    }

    #[test]
    fn missing_sample_is_not_zero() {
        let b = Instant::now();
        let mut d = RunawayDetector::new(cfg());
        assert_eq!(
            d.observe(tick(b, 0), Some(10.0), true),
            RunawayVerdict::Healthy
        );
        // One saturated interval (ratio 1.0) — inside the window.
        assert_eq!(
            d.observe(tick(b, 1), Some(11.0), true),
            RunawayVerdict::Healthy
        );
        // Sampler fails — not counted as zero, saturation state clears.
        assert_eq!(
            d.observe(tick(b, 2), None, true),
            RunawayVerdict::CpuUnavailable
        );
        // Resume re-seeds: a fresh saturated run must re-earn the
        // window; this time heartbeat is dead too — verdicts walk
        // Healthy -> HeartbeatOnly -> Runaway once the window lands.
        assert_eq!(
            d.observe(tick(b, 3), Some(20.0), false),
            RunawayVerdict::Healthy
        );
        assert_eq!(
            d.observe(tick(b, 4), Some(21.0), false),
            RunawayVerdict::HeartbeatOnly
        );
        for s in 5..=13 {
            assert_eq!(
                d.observe(tick(b, s), Some(20.0 + (s - 3) as f64), false),
                RunawayVerdict::HeartbeatOnly,
                "tick {s}: signal2 but window not yet earned"
            );
        }
        // saturated_since=4 -> window satisfied at t=14; hb streak
        // long past 2 -> both signals -> Runaway.
        assert_eq!(
            d.observe(tick(b, 14), Some(31.0), false),
            RunawayVerdict::Runaway
        );
    }

    #[test]
    fn cpu_regression_reseeds() {
        let b = Instant::now();
        let mut d = RunawayDetector::new(cfg());
        assert_eq!(
            d.observe(tick(b, 0), Some(50.0), true),
            RunawayVerdict::Healthy
        );
        // Counter backwards (pid reuse/in-place restart) — re-seed,
        // never a negative ratio or panic.
        assert_eq!(
            d.observe(tick(b, 1), Some(3.0), true),
            RunawayVerdict::Healthy
        );
        assert_eq!(
            d.observe(tick(b, 2), Some(4.5), true),
            RunawayVerdict::Healthy
        );
    }

    #[test]
    fn window_and_ratio_edges() {
        let b = Instant::now();
        let mut d = RunawayDetector::new(cfg());
        assert_eq!(
            d.observe(tick(b, 0), Some(0.0), true),
            RunawayVerdict::Healthy
        );
        // Below-threshold ratio (0.8 < 0.9) — never saturates.
        for s in 1..=20 {
            assert_eq!(
                d.observe(tick(b, s), Some(0.8 * s as f64), true),
                RunawayVerdict::Healthy,
                "tick {s}: 0.8 ratio must not count as saturation"
            );
        }
        // Exactly-at-threshold ratio counts (>=). The t=0->t=1 delta
        // is the literal 0.9/1.0 — bit-identical to cfg.min_cpu_ratio,
        // so `>=` is genuinely exercised (accumulating 0.9*n literals
        // would drift below the threshold by ~1e-16). saturated_since
        // latches at t=1; signal1 flips at t=11; hb streak >=2 from
        // t=3 -> HeartbeatOnly inside the window, Runaway at t=11 —
        // which transitively proves the t=1 edge step counted: had
        // it not, sat_since=2 and t=11 would stay HeartbeatOnly.
        let mut d2 = RunawayDetector::new(cfg());
        d2.observe(tick(b, 0), Some(0.0), true);
        d2.observe(tick(b, 1), Some(0.9), true);
        assert_eq!(
            d2.observe(tick(b, 2), Some(1.9), false),
            RunawayVerdict::Healthy
        );
        for s in 3..=10 {
            assert_eq!(
                d2.observe(tick(b, s), Some(0.9 + (s - 1) as f64), false),
                RunawayVerdict::HeartbeatOnly,
                "tick {s}: hb dead but window not yet earned"
            );
        }
        assert_eq!(
            d2.observe(tick(b, 11), Some(10.9), false),
            RunawayVerdict::Runaway
        );
    }

    #[test]
    fn healthy_resets_streaks() {
        let b = Instant::now();
        let mut d = RunawayDetector::new(cfg());
        d.observe(tick(b, 0), Some(0.0), true);
        // One failed probe, then recovery — streak resets, so the next
        // two failures are ticks 3+4 (streak 1, 2 -> signal2 only at 4).
        d.observe(tick(b, 1), Some(1.0), false);
        d.observe(tick(b, 2), Some(2.0), true);
        assert_eq!(
            d.observe(tick(b, 3), Some(3.0), false),
            RunawayVerdict::Healthy
        );
        assert_eq!(
            d.observe(tick(b, 4), Some(4.0), false),
            RunawayVerdict::HeartbeatOnly,
            "non-consecutive failures must not accumulate"
        );
    }
}
