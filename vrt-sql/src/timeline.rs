//! D1 commit timeline: a small append-only index for Time Travel.
//!
//! vrt-seg objects are keyed by `(epoch, lsn)` and do not carry a human timestamp, so the writer
//! records `(epoch, lsn, committedAtMs)` entries in chunks. A chunk is written at most every
//! [`FLUSH_COMMITS`] commits and at every checkpoint; a crash can therefore lose the tail of the
//! index, in which case Time Travel resolves to the newest indexed commit at or before the target
//! (never a future or fabricated state).
//!
//! Object key: `{kind}/{databaseId}/timeline/{last_lsn:016x}.json`.
//! Chunks are self-describing and safe to re-upload (same last_lsn, same entries).

use crate::paths::ResourceKind;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Flush the timeline buffer after this many commits even if no checkpoint happens.
pub const FLUSH_COMMITS: usize = 16;
/// Defensive cap on an in-memory unflushed timeline buffer.
pub const MAX_BUFFERED_ENTRIES: usize = 4096;
/// Defensive cap on one decoded chunk (bytes).
pub const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// One indexed commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimelineEntry {
    /// Commit lsn (inclusive).
    pub lsn: u64,
    /// Epoch that owns `lsn`.
    pub epoch: u64,
    /// Wall clock (unix ms) when the commit was persisted.
    pub committed_at_ms: u64,
}

/// One timeline chunk (`v1`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimelineChunk {
    pub v: u32,
    pub database_id: String,
    pub entries: Vec<TimelineEntry>,
}

/// Object key for a chunk ending at `last_lsn`.
pub fn chunk_key(kind: ResourceKind, database_id: &str, last_lsn: u64) -> String {
    format!(
        "{}/{database_id}/timeline/{last_lsn:016x}.json",
        kind.object_prefix()
    )
}

/// Whether `key` is a timeline chunk key.
pub fn is_chunk_key(key: &str) -> bool {
    key.contains("/timeline/") && key.ends_with(".json")
}

/// Encode a chunk (compact JSON).
pub fn encode_chunk(database_id: &str, entries: &[TimelineEntry]) -> Vec<u8> {
    serde_json::to_vec(&TimelineChunk {
        v: 1,
        database_id: database_id.into(),
        entries: entries.to_vec(),
    })
    .expect("timeline chunk serializes")
}

/// Merge all chunks into an lsn-sorted index.
///
/// Duplicate lsns are accepted only when epoch/time agree (a re-uploaded chunk); conflicting
/// duplicates make the index unusable (`Err`), because the caller would otherwise resolve a
/// time to a commit that never happened.
#[derive(Debug, Clone, Default)]
pub struct TimelineIndex {
    entries: BTreeMap<u64, TimelineEntry>,
}

impl TimelineIndex {
    pub fn insert(&mut self, entry: TimelineEntry) -> Result<(), &'static str> {
        if let Some(prev) = self.entries.get(&entry.lsn) {
            if prev.epoch != entry.epoch || prev.committed_at_ms != entry.committed_at_ms {
                return Err("conflicting timeline entry for the same lsn");
            }
            return Ok(());
        }
        self.entries.insert(entry.lsn, entry);
        Ok(())
    }

    pub fn from_entries(entries: impl IntoIterator<Item = TimelineEntry>) -> Result<Self, &'static str> {
        let mut idx = Self::default();
        for e in entries {
            idx.insert(e)?;
        }
        Ok(idx)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn oldest(&self) -> Option<TimelineEntry> {
        self.entries.values().next().copied()
    }

    pub fn latest(&self) -> Option<TimelineEntry> {
        self.entries.values().next_back().copied()
    }

    /// Latest commit with `committedAtMs <= target_ms`.
    ///
    /// Ties on the timestamp resolve to the greatest lsn (the later commit at the same
    /// millisecond), which is deterministic across processes.
    pub fn resolve_at(&self, target_ms: u64) -> Option<TimelineEntry> {
        self.entries
            .values()
            .filter(|e| e.committed_at_ms <= target_ms)
            .next_back()
            .copied()
    }

    /// The newest entry at or before `lsn` (bookmark validation helper).
    pub fn at_or_before_lsn(&self, lsn: u64) -> Option<TimelineEntry> {
        self.entries
            .range(..=lsn)
            .next_back()
            .map(|(_, e)| *e)
    }

    /// All entries at or after `since_ms`, ascending.
    pub fn since(&self, since_ms: u64) -> impl Iterator<Item = TimelineEntry> + '_ {
        self.entries
            .values()
            .copied()
            .filter(move |e| e.committed_at_ms >= since_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(lsn: u64, at: u64) -> TimelineEntry {
        TimelineEntry {
            lsn,
            epoch: 1,
            committed_at_ms: at,
        }
    }

    #[test]
    fn resolves_to_the_latest_commit_not_after_the_target() {
        let idx = TimelineIndex::from_entries([e(1, 100), e(2, 200), e(3, 300)]).unwrap();
        assert_eq!(idx.resolve_at(99), None);
        assert_eq!(idx.resolve_at(100).unwrap().lsn, 1);
        assert_eq!(idx.resolve_at(250).unwrap().lsn, 2);
        assert_eq!(idx.resolve_at(300).unwrap().lsn, 3);
        assert_eq!(idx.resolve_at(u64::MAX).unwrap().lsn, 3);
        assert_eq!(idx.oldest().unwrap().lsn, 1);
        assert_eq!(idx.latest().unwrap().lsn, 3);
    }

    #[test]
    fn duplicate_entries_are_safe_but_conflicts_fail() {
        let mut idx = TimelineIndex::default();
        idx.insert(e(1, 100)).unwrap();
        idx.insert(e(1, 100)).unwrap();
        assert_eq!(idx.len(), 1);
        let mut conflict = e(1, 101);
        conflict.epoch = 2;
        assert!(idx.insert(conflict).is_err());
    }

    #[test]
    fn chunk_roundtrip_and_key_shape() {
        let key = chunk_key(ResourceKind::D1, "abc", 42);
        assert_eq!(key, "db/abc/timeline/000000000000002a.json");
        assert!(is_chunk_key(&key));
        let bytes = encode_chunk("abc", &[e(1, 100), e(2, 200)]);
        let chunk: TimelineChunk = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(chunk.v, 1);
        assert_eq!(chunk.database_id, "abc");
        assert_eq!(chunk.entries.len(), 2);
    }
}
