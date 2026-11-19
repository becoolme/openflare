//! B12: self-process usage sampling for `process_*` metrics.
//!
//! Linux only (`/proc/self`). On unsupported platforms
//! [`self_usage`] returns `None` — callers must then emit the
//! `vrt_metrics_unsupported{metric=...}` marker instead of
//! fabricating zeros (B12 fail-closed rule).

/// One self-process sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelfUsage {
    /// utime+stime converted to seconds via `CLK_TCK`.
    pub cpu_seconds: f64,
    /// Resident set size in bytes (`VmRSS`).
    pub rss_bytes: u64,
    /// Process start time as a unix-epoch seconds float
    /// (`btime + starttime/CLK_TCK`).
    pub start_time_seconds: f64,
    /// Open file descriptors (`/proc/self/fd` count).
    pub open_fds: u64,
}

/// Sample the current process. `None` on non-Linux platforms or
/// when `/proc` is unreadable — never a guessed value.
#[cfg(target_os = "linux")]
pub fn self_usage() -> Option<SelfUsage> {
    self_usage_from(
        &std::fs::read_to_string("/proc/self/stat").ok()?,
        &std::fs::read_to_string("/proc/self/status").ok()?,
        &std::fs::read_to_string("/proc/stat").ok()?,
        std::fs::read_dir("/proc/self/fd").ok()?.count() as u64,
    )
}

#[cfg(not(target_os = "linux"))]
pub fn self_usage() -> Option<SelfUsage> {
    None
}

/// Pure parser — fixture-testable without touching real `/proc`.
/// `stat` is `/proc/self/stat`; `status` `/proc/self/status`;
/// `proc_stat` `/proc/stat` (for `btime`); `fds` the fd count.
pub fn self_usage_from(stat: &str, status: &str, proc_stat: &str, fds: u64) -> Option<SelfUsage> {
    // /proc/self/stat: fields after the final ')' are space-split;
    // index 0-based: state(0) ppid(1) ... utime is field 14
    // overall → index 11 after comm; stime field 15 → 12;
    // starttime field 22 → 19.
    let after = stat.rsplit_once(')')?.1.trim();
    let f: Vec<&str> = after.split_whitespace().collect();
    let ticks = clock_ticks();
    let cpu_seconds =
        (f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?) as f64 / ticks;
    let start_ticks = f.get(19)?.parse::<u64>().ok()? as f64;
    let rss_bytes = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        * 1024;
    let btime = proc_stat
        .lines()
        .find_map(|l| l.strip_prefix("btime "))?
        .trim()
        .parse::<u64>()
        .ok()? as f64;
    Some(SelfUsage {
        cpu_seconds,
        rss_bytes,
        start_time_seconds: btime + start_ticks / ticks,
        open_fds: fds,
    })
}

fn clock_ticks() -> f64 {
    // POSIX CLK_TCK is 100 on every Linux arch we target.
    100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT: &str =
        "1234 (vrt-host) S 1 1234 1234 0 -1 4194304 100 0 0 0 250 150 0 0 20 0 1 0 9999 1000000 5000 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0";
    const STATUS: &str = "Name:\tvrt-host\nVmRSS:\t   20480 kB\n";
    const PROC_STAT: &str = "cpu  1 2 3\nbtime 1700000000\n";

    #[test]
    fn parses_self_usage() {
        let u = self_usage_from(STAT, STATUS, PROC_STAT, 42).unwrap();
        assert!((u.cpu_seconds - 4.0).abs() < 1e-9); // (250+150)/100
        assert_eq!(u.rss_bytes, 20480 * 1024);
        // starttime ticks 9999 → btime + 99.99
        assert!((u.start_time_seconds - (1700000000.0 + 99.99)).abs() < 1e-6);
        assert_eq!(u.open_fds, 42);
    }

    #[test]
    fn comm_with_spaces_and_parens_is_skipped() {
        let stat = STAT.replace("(vrt-host)", "(weird ) name)");
        assert!(self_usage_from(&stat, STATUS, PROC_STAT, 1).is_some());
    }

    #[test]
    fn malformed_inputs_return_none_not_zero() {
        assert!(self_usage_from("garbage", STATUS, PROC_STAT, 0).is_none());
        assert!(self_usage_from(STAT, "no rss", PROC_STAT, 0).is_none());
        assert!(self_usage_from(STAT, STATUS, "no btime", 0).is_none());
    }
}
