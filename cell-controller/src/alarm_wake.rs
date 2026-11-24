//! Consume the persistent DO alarm index and wake tenants whose alarm is due.
//!
//! The host does not need to "replay" the alarm: workerd loads
//! `metadata.sqlite:_cf_ALARM` at startup and fires overdue alarms on its own.
//! This module only asks the owning pod to start the tenant, then waits for the
//! next authoritative class scan to clear/replace the index entry.
//!
//! Wake is at-least-once by design: if a wake response is lost, the entry stays
//! in the index and is retried after a bounded cooldown. The host's
//! `WakeFlight` coalesces concurrent sources into one launch.

use crate::alarm_index::{AlarmEntry, AlarmIndex};
use crate::assign::PodAssignment;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::Client;

/// Default retry/cooldown after a wake attempt (successful or not).
pub const DEFAULT_WAKE_COOLDOWN: Duration = Duration::from_secs(30);
/// Default maximum entries processed in one tick.
pub const DEFAULT_MAX_PER_TICK: usize = 64;

/// Transport to one pod's `/v1/do-alarm/wake`.
#[async_trait]
pub trait AlarmWakeTransport: Send + Sync {
    async fn wake(&self, pod: &str, entry: &AlarmEntry) -> Result<(), String>;
}

/// Observable result of one tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WakeSummary {
    /// Due entries seen in the index.
    pub due: usize,
    /// Entries with no assigned pod (index unsafe; retried).
    pub unassigned: usize,
    /// Entries skipped by their cooldown.
    pub cooling: usize,
    /// Wake requests sent.
    pub sent: usize,
    /// Wake requests that returned success.
    pub ok: usize,
    /// Wake requests that failed (retried after backoff).
    pub failed: usize,
}

type Key = (String, String, String, String, u64);

struct Retry {
    next: Instant,
    failures: u32,
}

/// Production wake transport (`POST {pod}/v1/do-alarm/wake`).
pub struct HttpAlarmWake {
    client: Client,
    pods: BTreeMap<String, String>,
    timeout: Duration,
}

impl HttpAlarmWake {
    pub fn new(pods: &[(String, String)], timeout: Duration) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| format!("alarm wake client: {e}"))?;
        let pods = pods
            .iter()
            .map(|(id, base)| (id.clone(), base.trim_end_matches('/').to_string()))
            .collect();
        Ok(Self {
            client,
            pods,
            timeout,
        })
    }
}

#[async_trait]
impl AlarmWakeTransport for HttpAlarmWake {
    async fn wake(&self, pod: &str, entry: &AlarmEntry) -> Result<(), String> {
        let base = self
            .pods
            .get(pod)
            .ok_or_else(|| format!("unknown pod {pod}"))?;
        let url = format!("{base}/v1/do-alarm/wake");
        let body = serde_json::json!({
            "tenantId": entry.tenant_id,
            "workerId": entry.worker_id,
            "classKey": entry.class_key,
            "objectId": entry.object_id,
            "atMs": entry.at_ms,
        });
        let resp = tokio::time::timeout(
            self.timeout,
            self.client
                .post(&url)
                .header("content-type", "application/json")
                .json(&body)
                .send(),
        )
        .await
        .map_err(|_| "alarm wake timeout".to_string())?
        .map_err(|e| format!("alarm wake transport: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("alarm wake status {}", resp.status()));
        }
        Ok(())
    }
}

/// Controller-side alarm waker. In-memory cooldown state is intentionally
/// disposable: a restart re-wakes at most `MAX` due entries, which the host
/// single-flight coalesces.
pub struct AlarmWaker {
    index: Arc<AlarmIndex>,
    cooldown: Duration,
    max_per_tick: usize,
    retry: Mutex<BTreeMap<Key, Retry>>,
}

impl AlarmWaker {
    pub fn new(index: Arc<AlarmIndex>) -> Self {
        Self {
            index,
            cooldown: DEFAULT_WAKE_COOLDOWN,
            max_per_tick: DEFAULT_MAX_PER_TICK,
            retry: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    pub fn with_max_per_tick(mut self, max: usize) -> Self {
        self.max_per_tick = max.max(1);
        self
    }

    /// One wake pass. `assignments` decides which pod owns the tenant.
    pub async fn tick(
        &self,
        assignments: &BTreeMap<String, PodAssignment>,
        transport: &dyn AlarmWakeTransport,
        now: Instant,
        now_ms: u64,
    ) -> WakeSummary {
        let due = self.index.due(now_ms);
        let mut summary = WakeSummary {
            due: due.len(),
            ..Default::default()
        };
        crate::metrics::alarm_wake(due.len(), 0, 0, 0);
        if due.is_empty() {
            return summary;
        }
        let mut processed = 0usize;
        for entry in due {
            if processed >= self.max_per_tick {
                break;
            }
            let key: Key = (
                entry.tenant_id.clone(),
                entry.worker_id.clone(),
                entry.class_key.clone(),
                entry.object_id.clone(),
                entry.at_ms,
            );
            let cooling = {
                let retry = self.retry.lock().unwrap_or_else(|e| e.into_inner());
                retry.get(&key).is_some_and(|r| r.next > now)
            };
            if cooling {
                summary.cooling += 1;
                continue;
            }
            let pod = pod_for(assignments, &entry);
            let Some(pod) = pod else {
                summary.unassigned += 1;
                let mut retry = self.retry.lock().unwrap_or_else(|e| e.into_inner());
                retry.insert(
                    key,
                    Retry {
                        next: now + self.cooldown,
                        failures: 0,
                    },
                );
                continue;
            };
            processed += 1;
            summary.sent += 1;
            let result = transport.wake(&pod, &entry).await;
            let mut retry = self.retry.lock().unwrap_or_else(|e| e.into_inner());
            let failures = retry.get(&key).map_or(0, |r| r.failures);
            match result {
                Ok(()) => {
                    summary.ok += 1;
                    retry.insert(
                        key,
                        Retry {
                            next: now + self.cooldown,
                            failures: 0,
                        },
                    );
                }
                Err(e) => {
                    summary.failed += 1;
                    tracing::warn!(
                        tenant = %entry.tenant_id,
                        class = %entry.class_key,
                        object = %entry.object_id,
                        error = %e,
                        "do alarm wake failed"
                    );
                    let failures = failures.saturating_add(1);
                    let backoff = self
                        .cooldown
                        .saturating_mul(1 << failures.min(6))
                        .min(Duration::from_secs(600));
                    retry.insert(
                        key,
                        Retry {
                            next: now + backoff,
                            failures,
                        },
                    );
                }
            }
        }
        crate::metrics::alarm_wake(0, summary.ok, summary.failed, summary.unassigned);
        // Bound the ledger; entries for cleared alarms age out on reload.
        let mut retry = self.retry.lock().unwrap_or_else(|e| e.into_inner());
        if retry.len() > 10_000 {
            let keep: Vec<Key> = retry.keys().take(5_000).cloned().collect();
            for k in keep {
                retry.remove(&k);
            }
        }
        summary
    }
}

/// The pod assignment that owns this entry's worker, falling back to the
/// tenant for cross-worker classes (workflow engines declare on the parent).
fn pod_for(assignments: &BTreeMap<String, PodAssignment>, entry: &AlarmEntry) -> Option<String> {
    for assignment in assignments.values() {
        if assignment
            .workers
            .iter()
            .any(|w| w.tenant_id == entry.tenant_id && w.worker_id == entry.worker_id)
        {
            return Some(assignment.pod_id.clone());
        }
    }
    for assignment in assignments.values() {
        if assignment
            .workers
            .iter()
            .any(|w| w.tenant_id == entry.tenant_id)
        {
            return Some(assignment.pod_id.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alarm_index::{AlarmClass, AlarmIndex, AlarmObservation};
    use crate::assign::AssignedWorker;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Mock {
        calls: AtomicUsize,
        fail: bool,
    }

    #[async_trait]
    impl AlarmWakeTransport for Mock {
        async fn wake(&self, _pod: &str, _entry: &AlarmEntry) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err("boom".into())
            } else {
                Ok(())
            }
        }
    }

    fn assignments() -> BTreeMap<String, PodAssignment> {
        BTreeMap::from([(
            "pod-0".into(),
            PodAssignment {
                pod_id: "pod-0".into(),
                revision: 1,
                payload_digest: "a".repeat(64),
                envelope_digest: String::new(),
                generated_at: String::new(),
                workers: vec![AssignedWorker {
                    tenant_id: "team".into(),
                    worker_id: "team:api".into(),
                }],
            },
        )])
    }

    fn class() -> AlarmClass {
        AlarmClass {
            tenant_id: "team".into(),
            worker_id: "team:api".into(),
            class_key: "a".repeat(64),
        }
    }

    fn seed(index: &AlarmIndex, at_ms: Option<u64>) {
        index
            .observe_class(
                &class(),
                &[AlarmObservation {
                    object_id: "b".repeat(64),
                    at_ms,
                    known: true,
                }],
                0,
            )
            .unwrap();
    }

    #[tokio::test]
    async fn wakes_due_entries_once_per_cooldown() {
        let index = Arc::new(AlarmIndex::open(None));
        seed(&index, Some(100));
        let waker = AlarmWaker::new(index.clone()).with_cooldown(Duration::from_secs(30));
        let t = Mock {
            calls: AtomicUsize::new(0),
            fail: false,
        };
        let now = Instant::now();
        let s = waker.tick(&assignments(), &t, now, 100).await;
        assert_eq!((s.due, s.sent, s.ok), (1, 1, 1));
        assert_eq!(t.calls.load(Ordering::SeqCst), 1);
        // Still due (index is only cleared by an authoritative scan) but the
        // cooldown suppresses a second wake.
        let s = waker.tick(&assignments(), &t, now, 100).await;
        assert_eq!((s.due, s.sent, s.cooling), (1, 0, 1));
        assert_eq!(t.calls.load(Ordering::SeqCst), 1);
        // Future alarms are not due.
        seed(&index, Some(10_000));
        let s = waker.tick(&assignments(), &t, now, 200).await;
        assert_eq!(s.due, 0);
    }

    #[tokio::test]
    async fn failed_wakes_back_off_and_unassigned_entries_are_retried() {
        let index = Arc::new(AlarmIndex::open(None));
        seed(&index, Some(100));
        let waker = AlarmWaker::new(index.clone()).with_cooldown(Duration::from_secs(1));
        let t = Mock {
            calls: AtomicUsize::new(0),
            fail: true,
        };
        let now = Instant::now();
        let s = waker.tick(&assignments(), &t, now, 100).await;
        assert_eq!((s.sent, s.failed), (1, 1));
        let s = waker
            .tick(&assignments(), &t, now + Duration::from_secs(2), 100)
            .await;
        assert_eq!(s.sent, 1);
        assert_eq!(t.calls.load(Ordering::SeqCst), 2);

        // No pod assignment: not sent, not lost.
        let s = waker
            .tick(&BTreeMap::new(), &t, now + Duration::from_secs(60), 100)
            .await;
        assert_eq!(s.unassigned, 1);
        assert_eq!(t.calls.load(Ordering::SeqCst), 2);
    }
}
