//! RV-R53.4 — durable DO object-store purge ledger.
//!
//! Signed deletion tombstones (`compute.desired.deletions`) name the
//! class keys whose `do/{classKey}/` object prefix may be destroyed.
//! Deleting a prefix is a multi-round job (list → delete → relist);
//! a process restart must not lose pending work, and a truncated
//! listing must never be mistaken for completion. This module keeps a
//! small ledger file on the DO state disk (`<doRoot>/.do-purge-ledger.json`,
//! atomic write) and drains it in bounded rounds.
//!
//! Safety:
//! - Entries are only ever added for tombstone-named class keys
//!   (validated 64-hex at the call sites) — never for "not in spec"
//!   resources (exclusion/eviction retains data).
//! - The placement-record guard is identical to the undeploy path:
//!   a foreign/live holder or an in-flight migration reservation skips
//!   the entry (it stays queued; the owning pod drains it, and a
//!   permanently-foreign record leaks objects — the safe direction).
//! - Idempotent: deleting a missing key is a no-op; a partially
//!   deleted prefix resumes naturally.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use vrt_objstore::ObjectStore;

/// Ledger file name under the DO storage root. Dot-prefixed so it can
/// never collide with a segment-encoded tenant directory.
pub const LEDGER_FILE: &str = ".do-purge-ledger.json";
/// Default per-drain entry bound — keeps one converge bounded even if
/// the ledger accumulated a large backlog while the host was down.
pub const DRAIN_MAX_ENTRIES: usize = 32;
/// Per-entry object cap for one drain round (same bound the undeploy
/// path used; a partial round re-queues via the ledger).
pub const DRAIN_MAX_KEYS: usize = 10_000;

/// One queued prefix-deletion task. `worker_id`/`tenant_id` are
/// diagnostics + local-dir scope only — the work item is `class_key`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PurgeEntry {
    pub tenant_id: String,
    /// Empty for tenant-scoped purges (worker dir already gone).
    pub worker_id: String,
    /// DO class `uniqueKey` — the object-store prefix `do/{key}/`.
    pub class_key: String,
    pub queued_at_ms: u64,
}

/// Outcome of one `drain` pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PurgeDrain {
    /// Entries whose prefix is confirmed empty this pass.
    pub completed: usize,
    /// Entries still queued after the pass (partial work, skipped
    /// foreign-held classes, or transient failures).
    pub pending: usize,
    /// Objects deleted this pass.
    pub deleted: usize,
}

/// Durable purge ledger — shared between the host (absent-tenant
/// tombstones) and actors (worker/class tombstones + undeploy).
pub struct DoPurge {
    path: PathBuf,
    store: Arc<dyn ObjectStore>,
    placement: Arc<crate::do_placement_store::ManifestDoPlacement>,
    inner: Mutex<BTreeMap<String, PurgeEntry>>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl DoPurge {
    /// Open (or create) the ledger under `root`. A corrupt ledger is
    /// moved aside (`<name>.corrupt`) and starts empty — losing pending
    /// work only ever delays deletion (leak), never destroys data.
    pub fn open(
        root: &Path,
        store: Arc<dyn ObjectStore>,
        placement: Arc<crate::do_placement_store::ManifestDoPlacement>,
    ) -> Self {
        Self::with_clock(
            root,
            store,
            placement,
            Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0)
            }),
        )
    }

    /// Test seam — injectable clock.
    pub fn with_clock(
        root: &Path,
        store: Arc<dyn ObjectStore>,
        placement: Arc<crate::do_placement_store::ManifestDoPlacement>,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        let path = root.join(LEDGER_FILE);
        let mut entries = BTreeMap::new();
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<Vec<PurgeEntry>>(&bytes) {
                Ok(list) => {
                    for e in list {
                        if e.class_key.len() == 64
                            && e.class_key.bytes().all(|b| b.is_ascii_hexdigit())
                        {
                            entries.insert(e.class_key.clone(), e);
                        }
                    }
                }
                Err(e) => {
                    let _ = std::fs::rename(&path, path.with_extension("corrupt"));
                    crate::metrics::registry()
                        .inc("vrt_host_do_purge_ledger_corrupt_total", &[], 1.0);
                    tracing::warn!(%e, "do purge ledger corrupt — moved aside, restarting empty");
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                crate::metrics::registry().inc("vrt_host_do_purge_ledger_corrupt_total", &[], 1.0);
                tracing::warn!(%e, "do purge ledger unreadable — restarting empty");
            }
        }
        Self {
            path,
            store,
            placement,
            inner: Mutex::new(entries),
            now_ms,
        }
    }

    /// Queue a class-key prefix for deletion (dedup by key — the first
    /// queue wins the diagnostic fields; identical work regardless).
    pub fn queue(&self, tenant_id: &str, worker_id: &str, class_key: &str) {
        if class_key.len() != 64 || !class_key.bytes().all(|b| b.is_ascii_hexdigit()) {
            crate::metrics::registry().inc("vrt_host_do_purge_rejected_total", &[], 1.0);
            tracing::warn!(tenant = tenant_id, "refusing purge of non-class-key target");
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.contains_key(class_key) {
            return;
        }
        inner.insert(
            class_key.to_string(),
            PurgeEntry {
                tenant_id: tenant_id.to_string(),
                worker_id: worker_id.to_string(),
                class_key: class_key.to_string(),
                queued_at_ms: (self.now_ms)(),
            },
        );
        self.persist(&inner);
    }

    /// Current pending-entry count — surfaced as a gauge by callers.
    pub fn pending(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Bounded drain: at most `max_entries` queued prefixes are worked,
    /// each capped at `max_keys` object deletes. Entries that finish
    /// drop out durably; everything else stays queued for the next
    /// pass (converge tick, restart resume).
    pub fn drain(&self, max_entries: usize, max_keys: usize) -> PurgeDrain {
        let keys: Vec<String> = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.keys().take(max_entries).cloned().collect()
        };
        let mut out = PurgeDrain::default();
        for key in keys {
            let done = match self.clean_one(&key, max_keys) {
                CleanResult::Done(n) => {
                    out.deleted += n;
                    true
                }
                CleanResult::Partial(n) => {
                    out.deleted += n;
                    false
                }
                CleanResult::Skip | CleanResult::Fail => false,
            };
            if done {
                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                inner.remove(&key);
                self.persist(&inner);
                out.completed += 1;
            }
        }
        out.pending = self.pending();
        out
    }

    fn clean_one(&self, key: &str, max_keys: usize) -> CleanResult {
        // Same ownership guard as the undeploy path: a class held by
        // another pod or reserved by an in-flight migration is NOT ours
        // to delete — the owning pod drains its own ledger entry.
        match self.placement.record_for_class(key) {
            Ok(Some(rec))
                if (!rec.holder.is_empty() && rec.holder != self.placement.holder())
                    || rec.reserved_for.is_some() =>
            {
                crate::metrics::registry()
                    .inc("vrt_host_do_cleanup_skipped_total", &[("stage", "purge")], 1.0);
                return CleanResult::Skip;
            }
            Err(_) => {
                crate::metrics::registry()
                    .inc("vrt_host_do_cleanup_skipped_total", &[("stage", "purge")], 1.0);
                return CleanResult::Fail;
            }
            _ => {}
        }
        match crate::do_snapshot::cleanup_class_objects(&self.store, key, max_keys) {
            Ok(o) if o.complete => CleanResult::Done(o.deleted),
            Ok(o) => CleanResult::Partial(o.deleted),
            Err(_) => {
                crate::metrics::registry()
                    .inc("vrt_host_do_cleanup_failures_total", &[("stage", "purge")], 1.0);
                CleanResult::Fail
            }
        }
    }

    fn persist(&self, inner: &BTreeMap<String, PurgeEntry>) {
        let body = match serde_json::to_vec(&inner.values().collect::<Vec<_>>()) {
            Ok(b) => b,
            Err(_) => return,
        };
        if vrt_securefs::atomic_write(&self.path, &body, 0o600).is_err() {
            crate::metrics::registry()
                .inc("vrt_host_do_purge_ledger_write_failures_total", &[], 1.0);
            tracing::warn!("do purge ledger persist failed — entries remain in memory");
        }
    }
}

enum CleanResult {
    Done(usize),
    Partial(usize),
    Skip,
    Fail,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(ch: char) -> String {
        ch.to_string().repeat(64)
    }

    fn fixture(
        dir: &Path,
    ) -> (
        Arc<vrt_objstore::MemoryStore>,
        Arc<crate::do_placement_store::ManifestDoPlacement>,
        DoPurge,
    ) {
        let store = Arc::new(vrt_objstore::MemoryStore::new());
        let placement = Arc::new(crate::do_placement_store::ManifestDoPlacement::new(
            store.clone(),
            "pod-a",
            60_000,
            5_000,
        ));
        (
            store.clone(),
            placement.clone(),
            DoPurge::open(dir, store, placement),
        )
    }

    #[test]
    fn queue_persist_resume_and_drain() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _plc, purge) = fixture(dir.path());
        vrt_objstore::block_on_store(store.put(&format!("do/{}/snap", key('a')), b"x")).unwrap();
        vrt_objstore::block_on_store(store.put(&format!("do/{}/snap", key('b')), b"x")).unwrap();
        purge.queue("t1", "w1", &key('a'));
        purge.queue("t1", "w1", &key('b'));
        assert_eq!(purge.pending(), 2);
        // Restart: a fresh instance over the SAME store resumes the
        // persisted ledger (restart-resume contract).
        let purge2 = DoPurge::open(dir.path(), store.clone(), _plc.clone());
        assert_eq!(purge2.pending(), 2);
        let d = purge2.drain(1, 100);
        assert_eq!(d.completed, 1);
        assert_eq!(d.pending, 1);
        let d = purge2.drain(32, 100);
        assert_eq!(d.completed, 1);
        assert_eq!(d.pending, 0);
        // Every `do/` prefix is gone; the placement manifest itself
        // (outside `do/`) is not the purge's business.
        assert!(
            store.keys().iter().all(|k| !k.starts_with("do/")),
            "leftover keys: {:?}",
            store.keys()
        );
    }

    #[test]
    fn partial_cleanup_stays_queued_until_empty() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _plc, purge) = fixture(dir.path());
        for i in 0..5 {
            vrt_objstore::block_on_store(
                store.put(&format!("do/{}/o{i}", key('c')), b"x"),
            )
            .unwrap();
        }
        purge.queue("t1", "w1", &key('c'));
        // max_keys=2 → each round deletes at most 2 and reports partial.
        let d = purge.drain(32, 2);
        assert_eq!(d.completed, 0);
        assert_eq!(d.pending, 1);
        while purge.pending() > 0 {
            purge.drain(32, 2);
        }
        assert!(store.keys().iter().all(|k| !k.starts_with("do/")));
    }

    #[test]
    fn corrupt_ledger_restarts_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LEDGER_FILE), b"{{not json").unwrap();
        let (_s, _p, purge) = fixture(dir.path());
        assert_eq!(purge.pending(), 0);
        assert!(dir.path().join(".do-purge-ledger.corrupt").exists());
    }

    #[test]
    fn rejects_non_class_key() {
        let dir = tempfile::tempdir().unwrap();
        let (_s, _p, purge) = fixture(dir.path());
        purge.queue("t1", "w1", "not-a-key");
        assert_eq!(purge.pending(), 0);
    }
}
