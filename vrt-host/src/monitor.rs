//! Portable RSS/fd sampling for the supervisor's resource watch.
//!
//! - Linux: `/proc/<pid>/status` `VmRSS` + `/proc/<pid>/fd` count.
//! - macOS: `proc_pidinfo(PROC_PIDTASKINFO)` + `proc_pidinfo(PROC_PIDLISTFDS)`.
//! - Other platforms / failures: `Unknown` — a missing reading is never
//!   treated as zero and never trips a limit.

/// One process sample. `None` means the axis could not be read.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ResourceUsage {
    /// Resident set size in bytes.
    pub rss_bytes: Option<u64>,
    /// Open file descriptor count.
    pub fds: Option<u64>,
    /// B12: cumulative CPU seconds (utime+stime) of the pid — Linux
    /// reads `/proc/<pid>/stat`, macOS `proc_pidinfo`. `None` on
    /// platforms without a sampler (never faked as 0).
    pub cpu_seconds: Option<f64>,
}

/// Whether `usage` violates `limits` — only known readings can trip.
pub fn over_limit(usage: &ResourceUsage, limits: &crate::spec::ResourceLimits) -> bool {
    if let (Some(rss), Some(max)) = (usage.rss_bytes, limits.max_rss_bytes) {
        if rss > max {
            return true;
        }
    }
    if let (Some(fds), Some(max)) = (usage.fds, limits.max_fds) {
        if fds > max {
            return true;
        }
    }
    false
}

/// Sampler abstraction (tests inject sequences).
pub trait ResourceSampler: Send + Sync {
    /// Sample `pid`; unknown axes return `None`.
    fn sample(&self, pid: u32) -> ResourceUsage;
}

/// Platform sampler: procfs on Linux, `proc_pidinfo` on macOS, `Unknown`
/// elsewhere.
#[derive(Debug, Default)]
pub struct OsSampler;

impl ResourceSampler for OsSampler {
    fn sample(&self, pid: u32) -> ResourceUsage {
        sample_os(pid)
    }
}

/// B12 scrape-side sampler — same readings the limit monitor uses.
/// `None` fields mean "unsupported/unknown", never fabricated zeros.
pub fn sample_os_usage(pid: u32) -> ResourceUsage {
    sample_os(pid)
}

/// `/proc/<pid>/status` `VmRSS:` kB → bytes. Pure — fixture-testable.
#[cfg(target_os = "linux")]
fn parse_proc_status_rss(text: &str) -> Option<u64> {
    text.lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|kb| kb * 1024)
}

/// `/proc/<pid>/stat` → cpu seconds (utime+stime ticks / CLK_TCK).
/// utime(14)+stime(15) sit after the parenthesized comm field, which
/// may itself contain spaces — parse from the LAST `)`.
#[cfg(target_os = "linux")]
fn parse_proc_stat_cpu(text: &str, ticks_per_sec: u64) -> Option<f64> {
    if ticks_per_sec == 0 {
        return None;
    }
    let after = text.rfind(')')? + 2;
    let fields: Vec<&str> = text[after..].split_whitespace().collect();
    // fields[0] is state; utime/stime are indices 11/12 here.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / ticks_per_sec as f64)
}

#[cfg(target_os = "linux")]
fn sample_os(pid: u32) -> ResourceUsage {
    let rss = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|text| parse_proc_status_rss(&text));
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .ok()
        .map(|entries| entries.count() as u64);
    let cpu_seconds = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|text| {
            let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
            if hz <= 0 {
                return None;
            }
            parse_proc_stat_cpu(&text, hz as u64)
        });
    ResourceUsage {
        rss_bytes: rss,
        fds,
        cpu_seconds,
    }
}

/// `mach_timebase_info_data_t` — declared locally because the libc
/// crate deprecates it in favour of the `mach2` crate (new deps are
/// off-limits here). The ABI is stable since Darwin 10.0.
#[cfg(target_os = "macos")]
#[repr(C)]
struct MachTimebaseInfo {
    numer: u32,
    denom: u32,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
}

#[cfg(target_os = "macos")]
fn sample_os(pid: u32) -> ResourceUsage {
    use std::mem::size_of;
    let rss = unsafe {
        let mut info: libc::proc_taskinfo = std::mem::zeroed();
        let wrote = libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size_of::<libc::proc_taskinfo>() as i32,
        );
        if wrote == size_of::<libc::proc_taskinfo>() as i32 {
            Some(info.pti_resident_size)
        } else {
            None
        }
    };
    let fds = unsafe {
        let needed = libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut(),
            0,
        );
        if needed <= 0 {
            None
        } else {
            let mut buf = vec![0u8; needed as usize];
            let wrote = libc::proc_pidinfo(
                pid as i32,
                libc::PROC_PIDLISTFDS,
                0,
                buf.as_mut_ptr() as *mut libc::c_void,
                needed,
            );
            if wrote <= 0 {
                None
            } else {
                Some((wrote as usize / size_of::<libc::proc_fdinfo>()) as u64)
            }
        }
    };
    let cpu_seconds = unsafe {
        let mut info: libc::proc_taskinfo = std::mem::zeroed();
        let wrote = libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size_of::<libc::proc_taskinfo>() as i32,
        );
        if wrote == size_of::<libc::proc_taskinfo>() as i32 {
            // `pti_total_*` are mach absolute-time ticks, NOT
            // nanoseconds: Apple-Silicon runs a 125/3 timebase (~41.7ns
            // per tick) so dividing raw ticks by 1e9 underreports CPU
            // by ~42x. Scale through `mach_timebase_info` first —
            // numer/denom==1/1 on Intel keeps behavior identical.
            let mut tb = MachTimebaseInfo { numer: 0, denom: 0 };
            if mach_timebase_info(&mut tb) == 0 && tb.denom != 0 {
                Some(
                    (info.pti_total_user + info.pti_total_system) as f64
                        * (tb.numer as f64 / tb.denom as f64)
                        / 1e9,
                )
            } else {
                None
            }
        } else {
            None
        }
    };
    ResourceUsage {
        rss_bytes: rss,
        fds,
        cpu_seconds,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn sample_os(_pid: u32) -> ResourceUsage {
    ResourceUsage::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::ResourceLimits;

    #[test]
    fn unknown_never_trips() {
        let limits = ResourceLimits {
            max_rss_bytes: Some(1),
            max_fds: Some(1),
            runaway: None,
        };
        // All-unknown sample — a sampler failure is not a violation.
        assert!(!over_limit(&ResourceUsage::default(), &limits));
        assert!(over_limit(
            &ResourceUsage {
                cpu_seconds: None,
                rss_bytes: Some(10),
                fds: None
            },
            &limits
        ));
        assert!(over_limit(
            &ResourceUsage {
                cpu_seconds: None,
                rss_bytes: None,
                fds: Some(10)
            },
            &limits
        ));
        // At-limit is not over-limit.
        assert!(!over_limit(
            &ResourceUsage {
                cpu_seconds: None,
                rss_bytes: Some(1),
                fds: Some(1)
            },
            &limits
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_fixtures_parse() {
        // Fixture: real /proc/<pid>/status excerpt.
        let status = "Name:\tworkerd\nVmRSS:\t   45324 kB\nVmSize:\t999999 kB\n";
        assert_eq!(parse_proc_status_rss(status), Some(45324 * 1024));
        assert_eq!(parse_proc_status_rss("Name:\tx\n"), None);
        // Fixture: stat with a spaced comm — utime=14, stime=15 ticks.
        let stat = "1234 (vrt workerd gen) R 1 2 3 4 5 6 7 8 9 10 14 15 16 17\n";
        assert_eq!(parse_proc_stat_cpu(stat, 10), Some(2.9));
        // No closing paren → unknown, never a shifted parse.
        assert_eq!(parse_proc_stat_cpu("1234 no-parens 1 2 3", 100), None);
    }

    #[test]
    fn unsupported_usage_maps_to_unsupported_metric() {
        // All-None (what sample_os returns on unsupported platforms)
        // must publish `vrt_metrics_unsupported`, never fabricated 0.
        let r = vrt_metrics::Registry::new("vrt_test_overflow_total");
        r.declare_gauge("vrt_metrics_unsupported", "m");
        r.declare_gauge("vrt_tenant_cpu_seconds_total", "c");
        r.declare_gauge("vrt_tenant_memory_rss_bytes", "r");
        let u = ResourceUsage::default();
        if u.cpu_seconds.is_none() {
            r.set(
                "vrt_metrics_unsupported",
                &[("metric", "tenant_cpu_seconds")],
                1.0,
            );
        }
        if u.rss_bytes.is_none() {
            r.set(
                "vrt_metrics_unsupported",
                &[("metric", "tenant_memory_rss")],
                1.0,
            );
        }
        let out = r.render();
        assert!(
            out.contains("vrt_metrics_unsupported{metric=\"tenant_cpu_seconds\"} 1"),
            "{out}"
        );
        assert!(!out.contains("vrt_tenant_cpu_seconds_total{"), "{out}");
        assert!(!out.contains("vrt_tenant_memory_rss_bytes{"), "{out}");
    }

    /// PT1: `cpu_seconds` unit check — a ~250ms busy loop must report
    /// ~0.25s of CPU. Off-by-1e3/1e6 unit errors (e.g. reporting
    /// macOS `pti_total_*` nanoseconds as seconds) are caught by the
    /// [0.05, 5.0] band, which also tolerates slow/loaded CI boxes.
    /// Skips the numeric band only on platforms where the sampler
    /// returns `None` (unsupported — asserted separately).
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn cpu_seconds_is_seconds_not_ticks_or_nanos() {
        let sampler = OsSampler;
        let pid = std::process::id();
        let before = sampler.sample(pid).cpu_seconds;
        // ~250ms of real CPU on THIS thread (no sleeps — those don't
        // burn cpu_seconds and would only widen the error band).
        let until = std::time::Instant::now() + std::time::Duration::from_millis(250);
        let mut acc = 0u64;
        while std::time::Instant::now() < until {
            acc = acc.wrapping_mul(1664525).wrapping_add(1013904223);
        }
        std::hint::black_box(acc);
        let after = sampler.sample(pid).cpu_seconds;
        let (Some(a), Some(b)) = (before, after) else {
            panic!("cpu_seconds unsupported on a supported platform");
        };
        let delta = b - a;
        assert!(
            (0.05..=5.0).contains(&delta),
            "cpu_seconds delta {delta}s for a ~0.25s spin — unit wrong?"
        );
    }

    #[test]
    fn os_sampler_reports_or_unknown() {
        let sampler = OsSampler;
        let usage = sampler.sample(std::process::id());
        // On supported platforms our own process reports both axes.
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert!(usage.rss_bytes.unwrap_or(0) > 0);
            assert!(usage.fds.is_some());
        }
        let _ = usage;
        // A dead pid yields Unknown, never a panic/zero.
        let dead = sampler.sample(u32::MAX - 1);
        let _ = dead;
    }
}
