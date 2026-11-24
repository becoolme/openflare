//! Persistent Durable Object alarm index.
//!
//! A DO alarm lives in the object's own SQLite file, which travels with the
//! tenant. That is enough while the host that owns the object is running, but
//! not after the tenant has been scaled to zero: nothing is left to fire the
//! alarm. This index is the cell-level memory of "which object has an alarm,
//! and when is the earliest one", persisted outside the pod so a survivor can
//! wake the tenant.
//!
//! The host is the only producer: it scans the declared class directories and
//! reads workerd's `_cf_METADATA` key 1 (unix nanoseconds). A class whose files
//! could not be read is recorded as *unknown* rather than "no alarm" — a
//! failed read must fail closed, never let a tenant sleep over a pending alarm.
//!
//! This module deliberately has no timer and no wake hook. It is the
//! persistence/index primitive; the wake loop consumes [`AlarmIndex::due`].

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// On-disk format version.
pub const INDEX_VERSION: u32 = 1;
/// Default file name inside the cell state directory.
pub const INDEX_FILE: &str = "do-alarm-index.json";
/// Defensive cap: a pathological class set must not grow the cell state file
/// without bound. Beyond it the index is marked `truncated` (consumers must
/// treat truncation as unsafe to sleep).
pub const MAX_ENTRIES: usize = 10_000;

/// Identity of one declared DO class.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmClass {
    pub tenant_id: String,
    pub worker_id: String,
    pub class_key: String,
}

/// One object's known alarm.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmEntry {
    pub tenant_id: String,
    pub worker_id: String,
    pub class_key: String,
    pub object_id: String,
    /// Alarm time (unix ms).
    pub at_ms: u64,
    /// When this host observation was folded into the index.
    pub observed_at_ms: u64,
}

/// Bounded export for the control-plane console.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AlarmSnapshot {
    pub entries: Vec<AlarmEntry>,
    pub unknown: Vec<AlarmClass>,
    pub truncated: Vec<AlarmClass>,
    pub observed: Vec<AlarmClass>,
}

/// One object as reported by a host scan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlarmObservation {
    pub object_id: String,
    #[serde(default)]
    pub at_ms: Option<u64>,
    /// `false` = the host could not read this object's metadata. The class is
    /// then unknown and must not be considered sleepable.
    #[serde(default)]
    pub known: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct IndexState {
    entries: BTreeMap<(String, String, String, String), AlarmEntry>,
    unknown: BTreeSet<AlarmClass>,
    /// Classes whose scan hit [`MAX_ENTRIES`] and may be missing alarms.
    truncated: BTreeSet<AlarmClass>,
    /// Classes observed since this process opened the index. Deliberately
    /// in-memory: a restart must re-scan before a class can be sleep-safe.
    observed: BTreeMap<AlarmClass, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexDoc {
    v: u32,
    #[serde(default)]
    entries: Vec<AlarmEntry>,
    #[serde(default)]
    unknown: Vec<AlarmClass>,
    #[serde(default)]
    truncated_classes: Vec<AlarmClass>,
}

/// The persistent index. Cheap to share (`Arc`); every mutation is serialized
/// under one lock and persisted with an atomic replace.
pub struct AlarmIndex {
    path: Option<PathBuf>,
    state: Mutex<IndexState>,
    max_entries: usize,
}

impl std::fmt::Debug for AlarmIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("AlarmIndex")
            .field("path", &self.path)
            .field("entries", &state.entries.len())
            .field("unknown", &state.unknown.len())
            .field("truncated", &state.truncated.len())
            .field("observed", &state.observed.len())
            .finish()
    }
}

impl AlarmIndex {
    /// Open (or create) the index at `path`. `None` keeps it in memory only
    /// (state-dir-less cells; log the at-least-once fallback at the call site).
    pub fn open(path: Option<PathBuf>) -> Self {
        let mut state = IndexState::default();
        if let Some(path) = &path {
            if let Some(dir) = path.parent() {
                if let Err(e) = std::fs::create_dir_all(dir) {
                    tracing::warn!(
                        dir = %dir.display(),
                        error = %e,
                        "alarm index: state dir unavailable; keeping the index in memory"
                    );
                }
            }
            match std::fs::read(path) {
                Ok(raw) => match serde_json::from_slice::<IndexDoc>(&raw) {
                    Ok(doc) if doc.v == INDEX_VERSION => {
                        for entry in doc.entries {
                            state.entries.insert(
                                (
                                    entry.tenant_id.clone(),
                                    entry.worker_id.clone(),
                                    entry.class_key.clone(),
                                    entry.object_id.clone(),
                                ),
                                entry,
                            );
                        }
                        state.unknown.extend(doc.unknown);
                        state.truncated.extend(doc.truncated_classes);
                    }
                    Ok(doc) => tracing::warn!(
                        path = %path.display(),
                        version = doc.v,
                        "alarm index: unsupported version; starting empty"
                    ),
                    Err(e) => tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "alarm index: unreadable; starting empty (next observation rewrites it)"
                    ),
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "alarm index: read failed; starting empty"
                ),
            }
        }
        Self {
            path,
            state: Mutex::new(state),
            max_entries: MAX_ENTRIES,
        }
    }

    /// Override the entry cap (tests).
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max.max(1);
        self
    }

    /// Fold one host scan of a class into the index.
    ///
    /// The observation is authoritative for the class: entries for objects no
    /// longer present (or whose alarm cleared) are dropped. Any `known:false`
    /// object marks the whole class unknown, which blocks sleep.
    pub fn observe_class(
        &self,
        class: &AlarmClass,
        observations: &[AlarmObservation],
        now_ms: u64,
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.entries.retain(|(tenant, worker, key, _), _| {
            tenant != &class.tenant_id || worker != &class.worker_id || key != &class.class_key
        });
        state.unknown.remove(class);
        state.truncated.remove(class);
        state.observed.insert(class.clone(), now_ms);

        let mut truncated = false;
        for observation in observations {
            if !observation.known {
                state.unknown.insert(class.clone());
                continue;
            }
            let Some(at_ms) = observation.at_ms else {
                continue;
            };
            if state.entries.len() >= self.max_entries
                && !state.entries.contains_key(&(
                    class.tenant_id.clone(),
                    class.worker_id.clone(),
                    class.class_key.clone(),
                    observation.object_id.clone(),
                ))
            {
                truncated = true;
                continue;
            }
            state.entries.insert(
                (
                    class.tenant_id.clone(),
                    class.worker_id.clone(),
                    class.class_key.clone(),
                    observation.object_id.clone(),
                ),
                AlarmEntry {
                    tenant_id: class.tenant_id.clone(),
                    worker_id: class.worker_id.clone(),
                    class_key: class.class_key.clone(),
                    object_id: observation.object_id.clone(),
                    at_ms,
                    observed_at_ms: now_ms,
                },
            );
        }
        if truncated {
            state.truncated.insert(class.clone());
        }
        let doc = IndexDoc {
            v: INDEX_VERSION,
            entries: state.entries.values().cloned().collect(),
            unknown: state.unknown.iter().cloned().collect(),
            truncated_classes: state.truncated.iter().cloned().collect(),
        };
        drop(state);
        self.persist(&doc)
    }

    /// Mark a class unreadable without replacing its known entries. Used when
    /// the host scan failed, was truncated, or the class was not declared:
    /// consumers must not let the class sleep until a complete scan clears it.
    pub fn mark_unknown(&self, class: &AlarmClass, now_ms: u64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.unknown.insert(class.clone());
        state.observed.insert(class.clone(), now_ms);
        state.truncated.remove(class);
        let doc = IndexDoc {
            v: INDEX_VERSION,
            entries: state.entries.values().cloned().collect(),
            unknown: state.unknown.iter().cloned().collect(),
            truncated_classes: state.truncated.iter().cloned().collect(),
        };
        drop(state);
        self.persist(&doc)
    }

    /// Earliest alarm across the whole cell (None when no object has one).
    pub fn earliest(&self) -> Option<AlarmEntry> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .entries
            .values()
            .min_by(|a, b| {
                a.at_ms
                    .cmp(&b.at_ms)
                    .then_with(|| a.tenant_id.cmp(&b.tenant_id))
                    .then_with(|| a.worker_id.cmp(&b.worker_id))
                    .then_with(|| a.class_key.cmp(&b.class_key))
                    .then_with(|| a.object_id.cmp(&b.object_id))
            })
            .cloned()
    }

    /// Earliest alarm for one class.
    pub fn earliest_for_class(&self, class: &AlarmClass) -> Option<AlarmEntry> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .entries
            .values()
            .filter(|e| {
                e.tenant_id == class.tenant_id
                    && e.worker_id == class.worker_id
                    && e.class_key == class.class_key
            })
            .min_by(|a, b| a.at_ms.cmp(&b.at_ms))
            .cloned()
    }

    /// Alarms that are due at `now_ms`, earliest first.
    pub fn due(&self, now_ms: u64) -> Vec<AlarmEntry> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut due: Vec<AlarmEntry> = state
            .entries
            .values()
            .filter(|e| e.at_ms <= now_ms)
            .cloned()
            .collect();
        due.sort_by(|a, b| {
            a.at_ms
                .cmp(&b.at_ms)
                .then_with(|| a.tenant_id.cmp(&b.tenant_id))
                .then_with(|| a.worker_id.cmp(&b.worker_id))
                .then_with(|| a.class_key.cmp(&b.class_key))
                .then_with(|| a.object_id.cmp(&b.object_id))
        });
        due
    }

    /// Whether the class may sleep from the alarm index's point of view:
    /// no unknown read, no pending alarm and no truncation for this class.
    pub fn is_safe_to_sleep(&self, class: &AlarmClass) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.truncated.contains(class) || state.unknown.contains(class) {
            return false;
        }
        !state.entries.values().any(|e| {
            e.tenant_id == class.tenant_id
                && e.worker_id == class.worker_id
                && e.class_key == class.class_key
        })
    }

    /// Stage-2 sleep gate: the class was fully observed since this process
    /// opened the index, is not unknown/truncated, and has no alarm due inside
    /// the lead window. Future alarms are allowed — the alarm waker handles
    /// them after the tenant sleeps.
    pub fn is_safe_to_sleep_stage2(&self, class: &AlarmClass, now_ms: u64, lead_ms: u64) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.unknown.contains(class) || state.truncated.contains(class) {
            return false;
        }
        if !state.observed.contains_key(class) {
            return false;
        }
        let horizon = now_ms.saturating_add(lead_ms);
        !state.entries.values().any(|e| {
            e.tenant_id == class.tenant_id
                && e.worker_id == class.worker_id
                && e.class_key == class.class_key
                && e.at_ms <= horizon
        })
    }

    /// Whether this class has been observed since the index process started.
    pub fn observed(&self, class: &AlarmClass) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observed
            .contains_key(class)
    }

    /// Bounded console export: known alarms + class health.
    pub fn export(&self) -> AlarmSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        AlarmSnapshot {
            entries: state.entries.values().cloned().collect(),
            unknown: state.unknown.iter().cloned().collect(),
            truncated: state.truncated.iter().cloned().collect(),
            observed: state.observed.keys().cloned().collect(),
        }
    }

    /// Snapshot of every known alarm (for diagnostics/tests).
    pub fn snapshot(&self) -> Vec<AlarmEntry> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.entries.values().cloned().collect()
    }

    /// Classes whose metadata could not be read at the last scan.
    pub fn unknown_classes(&self) -> Vec<AlarmClass> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.unknown.iter().cloned().collect()
    }

    /// Whether any class/entry was dropped past [`MAX_ENTRIES`].
    pub fn truncated(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .truncated
            .is_empty()
    }

    /// Persist the current state (used when the path was set after opening).
    pub fn save(&self) -> Result<(), String> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let doc = IndexDoc {
            v: INDEX_VERSION,
            entries: state.entries.values().cloned().collect(),
            unknown: state.unknown.iter().cloned().collect(),
            truncated_classes: state.truncated.iter().cloned().collect(),
        };
        drop(state);
        self.persist(&doc)
    }

    fn persist(&self, doc: &IndexDoc) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let bytes =
            serde_json::to_vec_pretty(doc).map_err(|e| format!("alarm index encode: {e}"))?;
        vrt_securefs::atomic_write(path, &bytes, 0o600)
            .map_err(|e| format!("alarm index write {}: {e}", path.display()))
    }
}

/// Standard file path inside a cell state directory.
pub fn index_path(state_dir: &Path) -> PathBuf {
    state_dir.join(INDEX_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class() -> AlarmClass {
        AlarmClass {
            tenant_id: "team".into(),
            worker_id: "team:api".into(),
            class_key: "a".repeat(64),
        }
    }

    fn other_class() -> AlarmClass {
        AlarmClass {
            tenant_id: "team".into(),
            worker_id: "team:api".into(),
            class_key: "b".repeat(64),
        }
    }

    fn obs(id: &str, at_ms: Option<u64>) -> AlarmObservation {
        AlarmObservation {
            object_id: id.into(),
            at_ms,
            known: true,
        }
    }

    #[test]
    fn observes_persists_and_reopens() {
        let tmp = tempfile::tempdir().unwrap();
        let path = index_path(tmp.path());
        let index = AlarmIndex::open(Some(path.clone()));
        index
            .observe_class(
                &class(),
                &[obs("01", Some(500)), obs("02", Some(100))],
                1_000,
            )
            .unwrap();
        assert_eq!(index.earliest().unwrap().at_ms, 100);
        assert_eq!(index.earliest().unwrap().object_id, "02");
        assert!(!index.is_safe_to_sleep(&class()));
        assert!(index.due(100).iter().any(|e| e.object_id == "02"));
        assert!(index.due(99).is_empty());

        let reopened = AlarmIndex::open(Some(path));
        assert_eq!(reopened.earliest().unwrap().at_ms, 100);
        assert_eq!(reopened.snapshot().len(), 2);
    }

    #[test]
    fn a_new_observation_replaces_the_class_and_clears_removed_alarms() {
        let tmp = tempfile::tempdir().unwrap();
        let index = AlarmIndex::open(Some(index_path(tmp.path())));
        index
            .observe_class(&class(), &[obs("01", Some(500)), obs("02", Some(100))], 0)
            .unwrap();
        index
            .observe_class(&class(), &[obs("01", None)], 10)
            .unwrap();
        assert!(index.snapshot().is_empty());
        assert!(index.is_safe_to_sleep(&class()));
        assert_eq!(index.earliest_for_class(&class()), None);
    }

    #[test]
    fn unknown_reads_block_sleep_and_clear_on_a_good_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let index = AlarmIndex::open(Some(index_path(tmp.path())));
        index
            .observe_class(
                &class(),
                &[AlarmObservation {
                    object_id: "01".into(),
                    at_ms: None,
                    known: false,
                }],
                0,
            )
            .unwrap();
        assert!(!index.is_safe_to_sleep(&class()));
        assert_eq!(index.unknown_classes(), vec![class()]);
        assert!(index.earliest().is_none());

        index
            .observe_class(&class(), &[obs("01", None)], 1)
            .unwrap();
        assert!(index.is_safe_to_sleep(&class()));
        assert!(index.unknown_classes().is_empty());
    }

    #[test]
    fn classes_are_isolated_and_truncation_blocks_sleep() {
        let tmp = tempfile::tempdir().unwrap();
        let index = AlarmIndex::open(Some(index_path(tmp.path()))).with_max_entries(1);
        index
            .observe_class(&class(), &[obs("01", Some(50))], 0)
            .unwrap();
        index
            .observe_class(&other_class(), &[obs("02", Some(60))], 0)
            .unwrap();
        assert!(index.truncated());
        assert!(!index.is_safe_to_sleep(&other_class()));
        assert_eq!(index.earliest_for_class(&class()).unwrap().at_ms, 50);
    }

    #[test]
    fn stage2_gate_requires_a_fresh_observation_and_allows_future_alarms() {
        let tmp = tempfile::tempdir().unwrap();
        let index = AlarmIndex::open(Some(index_path(tmp.path())));
        // Never observed -> unsafe even with no entries.
        assert!(!index.is_safe_to_sleep_stage2(&class(), 1_000, 10));
        index
            .observe_class(&class(), &[obs("01", Some(5_000))], 1_000)
            .unwrap();
        // Future alarm outside the lead window is sleep-safe at stage 2.
        assert!(index.is_safe_to_sleep_stage2(&class(), 1_000, 1_000));
        assert!(!index.is_safe_to_sleep(&class()));
        // Inside the lead window it is not.
        assert!(!index.is_safe_to_sleep_stage2(&class(), 4_500, 1_000));
    }

    #[test]
    fn truncation_is_per_class() {
        let tmp = tempfile::tempdir().unwrap();
        let index = AlarmIndex::open(Some(index_path(tmp.path()))).with_max_entries(1);
        index
            .observe_class(&class(), &[obs("01", Some(50))], 0)
            .unwrap();
        index
            .observe_class(&other_class(), &[obs("02", Some(60))], 0)
            .unwrap();
        assert!(index.truncated());
        assert!(!index.is_safe_to_sleep_stage2(&other_class(), 0, 0));
        // Observing a different class must not clear another class's mark.
        index
            .observe_class(&class(), &[obs("01", None)], 1)
            .unwrap();
        assert!(!index.is_safe_to_sleep_stage2(&other_class(), 0, 0));
        assert!(index.is_safe_to_sleep_stage2(&class(), 0, 0));
    }

    #[test]
    fn corrupt_state_starts_empty_and_the_next_observation_rewrites_it() {
        let tmp = tempfile::tempdir().unwrap();
        let path = index_path(tmp.path());
        std::fs::write(&path, b"{not json").unwrap();
        let index = AlarmIndex::open(Some(path.clone()));
        assert!(index.snapshot().is_empty());
        index
            .observe_class(&class(), &[obs("01", Some(7))], 0)
            .unwrap();
        let raw = std::fs::read(&path).unwrap();
        let doc: IndexDoc = serde_json::from_slice(&raw).unwrap();
        assert_eq!(doc.v, INDEX_VERSION);
        assert_eq!(doc.entries.len(), 1);
    }
}
