//! MIG-10: bounded placement self-check — the SAME shared retry
//! primitive vrt-sql's MIG-9 persistence check runs
//! (`vrt_objstore::self_check::run_with_retry`), never a copy.
//!
//! Extracted from the vrt-host binary so the behavior is unit-testable
//! against the in-memory store's failure injection: N retryable
//! failures then success → Ok; persistent failure → budget exhausts
//! to the last error; authz (retryable=false) fails IMMEDIATELY —
//! a misconfigured credential never burns the 60 s budget.

use std::sync::Arc;
use vrt_objstore::self_check::{retryable_store_error, run_with_retry, SelfCheckRetry};
use vrt_objstore::{ObjectStore, StoreError};

/// One self-check round: conditional write + read-back + delete on a
/// fresh placement-namespaced probe key (pid+nanos — concurrent
/// self-checks never collide). Returns the store error verbatim so
/// the retry predicate sees the real `retryable` bit; a read-back
/// mismatch maps to Corrupt (non-retryable — the store answered but
/// lost the write, retrying hides the fault).
fn probe_once(store: &Arc<dyn ObjectStore>) -> Result<(), StoreError> {
    let probe_key = format!(
        "do/_self-check/{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    vrt_objstore::block_on_store(store.put_if_none_match(&probe_key, b"p"))?;
    let read = vrt_objstore::block_on_store(store.get(&probe_key))?;
    if read.map(|o| o.body) != Some(b"p".to_vec()) {
        let _ = vrt_objstore::block_on_store(store.delete(&probe_key));
        return Err(StoreError::Corrupt(
            "placement self-check read-back mismatch".into(),
        ));
    }
    vrt_objstore::block_on_store(store.delete(&probe_key))?;
    Ok(())
}

/// Run the bounded retry. `log(attempt, elapsed, next_backoff, err)`
/// is the caller's structured-log hook — error text MUST pass
/// `sanitize_self_check_error` first (backend URLs carry probe paths).
pub fn run(
    store: &Arc<dyn ObjectStore>,
    retry: &SelfCheckRetry,
    log: impl FnMut(u32, std::time::Duration, std::time::Duration, &StoreError),
) -> Result<(), StoreError> {
    run_with_retry(retry, retryable_store_error, || probe_once(store), log)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vrt_objstore::MemoryStore;
    use std::time::Duration;

    fn plan(budget_ms: u64) -> SelfCheckRetry {
        SelfCheckRetry {
            budget: Duration::from_millis(budget_ms),
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(15),
        }
    }

    fn mem() -> (Arc<MemoryStore>, Arc<dyn ObjectStore>) {
        let mem = Arc::new(MemoryStore::new());
        let store: Arc<dyn ObjectStore> = mem.clone();
        (mem, store)
    }

    #[test]
    fn n_retryable_failures_then_success() {
        let (mem, store) = mem();
        // inject_put pops from the END — two errors then success.
        mem.inject_put_failures(vec![
            StoreError::unavailable(true, "send: error sending request"),
            StoreError::unavailable(true, "send: error sending request"),
        ]);
        let mut logs = 0u32;
        run(&store, &plan(10_000), |a, _e, _b, err| {
            logs += 1;
            assert!(matches!(err, StoreError::Unavailable { retryable: true, .. }));
            assert_eq!(a, logs);
        })
        .expect("recovers inside budget");
        assert_eq!(logs, 2);
        assert_eq!(mem.len(), 0, "probe keys are deleted");
    }

    #[test]
    fn persistent_retryable_failure_exhausts_budget() {
        let (mem, store) = mem();
        mem.inject_put_failures(
            std::iter::repeat_with(|| StoreError::unavailable(true, "down"))
                .take(64)
                .collect(),
        );
        let t0 = std::time::Instant::now();
        let e = run(&store, &plan(50), |_, _, _, _| {}).unwrap_err();
        assert!(matches!(e, StoreError::Unavailable { retryable: true, .. }));
        // Retried (not single-shot), then the budget cut it off well
        // under the caller's exit path — wall clock stays bounded by
        // budget + one attempt, never the inject queue's 64.
        assert!(mem.put_calls() > 1 && mem.put_calls() < 64);
        assert!(t0.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn authorization_failure_returns_immediately() {
        let (mem, store) = mem();
        mem.inject_put_failures(vec![StoreError::unavailable(
            false,
            "403 SignatureDoesNotMatch",
        )]);
        let mut attempts = 0u32;
        let e = run(&store, &plan(60_000), |_, _, _, _| attempts += 1).unwrap_err();
        assert!(matches!(
            e,
            StoreError::Unavailable {
                retryable: false,
                ..
            }
        ));
        // Immediate: no retry-log calls, exactly one store attempt.
        assert_eq!(attempts, 0);
        assert_eq!(mem.put_calls(), 1);
    }

    #[test]
    fn precondition_failure_is_not_retried() {
        let (mem, store) = mem();
        mem.inject_put_failures(vec![StoreError::PreconditionFailed]);
        let e = run(&store, &plan(60_000), |_, _, _, _| {}).unwrap_err();
        assert!(matches!(e, StoreError::PreconditionFailed));
        assert_eq!(mem.put_calls(), 1);
    }

    #[test]
    fn sanitize_keeps_credentials_out_of_retry_logs() {
        let raw = "send: error sending request for url (https://AKID@r2.example.com/b/do/_self-check?x=1&sig=SECRET)";
        let clean = vrt_objstore::self_check::sanitize_self_check_error(raw);
        for needle in ["AKID", "SECRET", "x=1", "r2.example.com", "http"] {
            assert!(!clean.contains(needle), "{needle} leaked in {clean}");
        }
    }
}
