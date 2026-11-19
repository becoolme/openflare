//! MIG-10: shared bounded self-check retry — extracted from vrt-sql's
//! MIG-9 implementation so vrt-host's placement self-check uses the
//! SAME primitive (no copy/paste).
//!
//! Contract: only errors the caller's `retryable` predicate marks
//! retryable are retried — authentication/permission failures
//! (401/403, `Unavailable{retryable:false}`), PreconditionFailed,
//! NotFound, corruption and local I/O return IMMEDIATELY (a
//! misconfiguration never heals inside a 60 s budget — fail closed).
//! The budget gates the SLEEP time between attempts: total wall
//! clock <= budget + one self-check duration. While retrying the
//! process must stay not-ready (startup binds no listener until the
//! check passes) and never be liveness-killed (startupProbe covers
//! the budget).

use std::time::{Duration, Instant};

/// Bounded self-check retry policy (config injects the ms fields).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelfCheckRetry {
    /// Total sleep budget across retries.
    pub budget: Duration,
    /// First backoff slice.
    pub initial_backoff: Duration,
    /// Backoff cap (doubles each attempt until this).
    pub max_backoff: Duration,
}

/// The only `StoreError` worth retrying: transient unavailability
/// (network/5xx/timeout/429 — `retryable: true`). Everything else —
/// 401/403/authz (`retryable: false`), PreconditionFailed, NotFound,
/// Corrupt, local I/O — fails closed at once.
pub fn retryable_store_error(e: &crate::StoreError) -> bool {
    matches!(
        e,
        crate::StoreError::Unavailable {
            retryable: true,
            ..
        }
    )
}

/// Strip embedded URLs from a store error's Display text before it
/// reaches a log line — reqwest `send:`/`connect:` errors carry the
/// full request URL (path contains the probe key, query may carry
/// params). Credentials/signatures travel in headers, never URLs,
/// but the whole URL is still redacted so no endpoint detail can
/// reach logs under ANY backend error shape.
pub fn sanitize_self_check_error(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find("http").filter(|&p| {
        rest[p..].starts_with("http://") || rest[p..].starts_with("https://")
    }) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos..];
        let end = after
            .find(|c: char| c.is_whitespace() || c == ')' || c == ']' || c == '"' || c == '\'')
            .unwrap_or(after.len());
        out.push_str("<url>");
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// Bounded in-process self-check retry — exponential backoff
/// (initial → ×2 → cap), the budget exhausts to the LAST error (the
/// caller exits non-zero / refuses to serve). `log(attempt, elapsed,
/// next_backoff, err)` is the caller's structured-log hook — the
/// error text MUST pass [`sanitize_self_check_error`] first.
pub fn run_with_retry<E>(
    retry: &SelfCheckRetry,
    retryable: impl Fn(&E) -> bool,
    mut check: impl FnMut() -> Result<(), E>,
    mut log: impl FnMut(u32, Duration, Duration, &E),
) -> Result<(), E> {
    let t0 = Instant::now();
    let mut backoff = retry.initial_backoff;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match check() {
            Ok(()) => return Ok(()),
            Err(e) => {
                if !retryable(&e) {
                    return Err(e);
                }
                let elapsed = t0.elapsed();
                if elapsed + backoff > retry.budget {
                    return Err(e);
                }
                log(attempt, elapsed, backoff, &e);
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(retry.max_backoff);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use crate::StoreError;

    fn plan(budget_ms: u64) -> SelfCheckRetry {
        SelfCheckRetry {
            budget: Duration::from_millis(budget_ms),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
        }
    }

    #[test]
    fn retryable_only_on_unavailable_retryable() {
        assert!(retryable_store_error(&StoreError::unavailable(true, "x")));
        assert!(!retryable_store_error(&StoreError::unavailable(false, "x")));
        assert!(!retryable_store_error(&StoreError::PreconditionFailed));
        assert!(!retryable_store_error(&StoreError::NotFound));
    }

    #[test]
    fn n_retryable_then_success() {
        let n = Arc::new(AtomicU32::new(0));
        let n2 = n.clone();
        let r = run_with_retry(
            &plan(10_000),
            retryable_store_error,
            move || {
                if n2.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(StoreError::unavailable(true, "boom"))
                } else {
                    Ok(())
                }
            },
            |_, _, _, _| {},
        );
        assert!(r.is_ok());
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn persistent_failure_exhausts_budget() {
        let r = run_with_retry(
            &plan(30),
            retryable_store_error,
            || Err::<(), _>(StoreError::unavailable(true, "down")),
            |_, _, _, _| {},
        );
        assert!(r.is_err());
    }

    #[test]
    fn non_retryable_returns_immediately() {
        let n = AtomicU32::new(0);
        let r = run_with_retry(
            &plan(10_000),
            retryable_store_error,
            || {
                n.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(StoreError::unavailable(false, "403"))
            },
            |_, _, _, _| {},
        );
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sanitize_strips_urls() {
        let raw = "put failed: send: error sending request for url (https://s3.example.com/b/probe/self-check-1?sig=abc) connection refused";
        let out = sanitize_self_check_error(raw);
        assert!(!out.contains("s3.example.com") && !out.contains("sig=abc"));
        assert!(out.contains("<url>"));
        assert_eq!(
            sanitize_self_check_error("a http://x/1 b https://y/2 c"),
            "a <url> b <url> c"
        );
        assert_eq!(sanitize_self_check_error("no url"), "no url");
    }
}
