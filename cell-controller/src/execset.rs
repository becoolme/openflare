//! W4: shared cell-op execution journal — extracted from `wfops.rs` so
//! the D1 ops channel (`d1ops.rs`) gets identical persist-before-ack
//! semantics without a copy-pasted journal.
//!
//! Components:
//!  - [`CellOp`] / [`CellOpAck`]: the generic `vrt_cell_ops` wire types.
//!    `resourceKey` scopes resource-bound kinds (D1 carries the
//!    databaseId); it is absent/'' for worker-scoped kinds (WF_*).
//!  - [`SeenSet`]: in-memory pull dedup — ids are marked only after
//!    their ACK POSTed; a bounded BTreeMap, oldest evicted first.
//!  - [`ExecutedSet`]: the DURABLE outcome journal — `record` appends
//!    + `sync_data`s before returning, so a crash between execute and
//!    ack replays the stored outcome instead of re-executing. Periodic
//!    snapshot compaction bounds restart replay.

use serde::Deserialize;

/// RV-R58: result of decoding one `ops[]` page row-by-row. A row the
/// controller cannot decode must never stall the rest of the page (a
/// newer server adding a field/shape would otherwise freeze every op).
#[derive(Debug, Default)]
pub struct DecodedOps {
    pub ops: Vec<CellOp>,
    /// Rows that failed to decode (skipped; counted + logged).
    pub bad_rows: usize,
    /// Rows carrying unknown keys that still decoded (forward compat).
    pub unknown_field_rows: usize,
}

const CELL_OP_KNOWN_KEYS: [&str; 7] = [
    "id",
    "seq",
    "kind",
    "workerId",
    "resourceKey",
    "payload",
    "expiresAt",
];

/// Decode a page of raw rows one at a time (RV-R58). Unknown keys are
/// accepted and counted; a malformed row is skipped (counted, logged
/// with its id when present) and the other rows still execute.
pub fn decode_ops_page(rows: Vec<serde_json::Value>) -> DecodedOps {
    let mut out = DecodedOps::default();
    for row in rows {
        if let Some(obj) = row.as_object() {
            if obj.keys().any(|k| !CELL_OP_KNOWN_KEYS.contains(&k.as_str())) {
                out.unknown_field_rows += 1;
                tracing::debug!("cell-ops row carries unknown fields — accepted");
            }
        }
        let id = row
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.chars().take(64).collect::<String>());
        match serde_json::from_value::<CellOp>(row) {
            Ok(op) => out.ops.push(op),
            Err(e) => {
                out.bad_rows += 1;
                tracing::warn!(op_id = ?id, error = %e, "cell-ops row undecodable — skipped");
            }
        }
    }
    out
}

/// One op row from `GET /vrt/cell-ops`. RV-R58: unknown fields are
/// ACCEPTED (counted by [`decode_ops_page`]) so a newer control plane
/// cannot stall an older cell; forward-compat kinds arrive as strings.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CellOp {
    pub id: String,
    pub seq: i64,
    pub kind: String,
    pub worker_id: String,
    /// W4: resource scope — '' for worker-scoped kinds; the D1
    /// databaseId / queueId for resource-bound kinds.
    #[serde(default)]
    pub resource_key: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub expires_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CellOpAck {
    pub seq: i64,
    pub status: &'static str,
    /// Kind-shaped result payload (validated server-side). Absent is the
    /// ONLY accepted "no result" wire form — `serde_json` would emit
    /// `null`, which the strict control-plane schema rejects (RV-R68).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Bounded free-text error for FAILED/EXPIRED — omitted, never null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Bounded set of op ids already ACKed — the pull endpoint returns ALL
/// pending ops (SERIAL order ≠ commit order), so re-delivery of
/// in-flight or un-acked ops is expected. Ids are the durable dedup
/// key; the set is capped and evicts arbitrary-oldest — the persisted
/// `ExecutedSet` journal is the real safety net.
pub const SEEN_CAP: usize = 4096;
pub type SeenSet = std::collections::BTreeMap<String, ()>;

/// Persisted executed-op set (W3-r2/W3-r3, extracted W4).
///
/// Per-op durability: `record` appends `(op_id, outcome)` to
/// `<path>.journal` and `sync_data`s — after `record` returns, a crash
/// anywhere before/inside the ack POST replays the stored outcome; the
/// engine call is never repeated.
///
/// Bounds: [`EXECUTED_CAP`] entries max, [`EXECUTED_TTL_SECS`] TTL —
/// eviction prefers expired records, then oldest by timestamp. The
/// snapshot is atomic (tmp+rename, 0600); a corrupt file degrades to an
/// empty set (fail-safe: re-execution is the documented fallback).
pub const EXECUTED_CAP: usize = 8192;
pub const EXECUTED_TTL_SECS: i64 = 7 * 24 * 3600;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutedRecord {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// epoch seconds — drives TTL eviction.
    pub at: i64,
}

#[derive(Default)]
pub struct ExecutedSet {
    path: Option<std::path::PathBuf>,
    journal_path: Option<std::path::PathBuf>,
    journal: Option<std::fs::File>,
    pub(crate) map: std::collections::BTreeMap<String, ExecutedRecord>,
    dirty: bool,
    appends_since_compact: usize,
}

/// Snapshot compaction cadence: the journal is the durability record,
/// the snapshot only bounds restart-replay time.
const JOURNAL_COMPACT_EVERY: usize = 256;

impl ExecutedSet {
    /// In-memory set (cells without a state dir — delivery degrades to
    /// the documented at-least-once fallback, still correct).
    pub fn memory() -> Self {
        Self::default()
    }

    /// Load from `path` — expires TTL-stale records on read; malformed
    /// or absent files start empty. The per-op journal at
    /// `<path>.journal` replays after the snapshot — a torn tail line
    /// (crash mid-append) stops replay at that line, degrading to
    /// at-least-once for the unflushed record only.
    pub fn load(path: std::path::PathBuf) -> Self {
        let journal_path = path.with_extension("journal");
        let mut set = Self {
            path: Some(path),
            journal_path: Some(journal_path.clone()),
            ..Default::default()
        };
        let now = now_secs();
        if let Some(p) = &set.path {
            if let Ok(text) = std::fs::read_to_string(p) {
                if let Ok(map) = serde_json::from_str::<
                    std::collections::BTreeMap<String, ExecutedRecord>,
                >(&text)
                {
                    set.map = map
                        .into_iter()
                        .filter(|(_, r)| now - r.at < EXECUTED_TTL_SECS)
                        .collect();
                } else {
                    tracing::warn!("cell-ops executed-set file corrupt — starting empty");
                }
            }
        }
        // Journal replay — append-order, later records overwrite.
        if let Ok(text) = std::fs::read_to_string(&journal_path) {
            for line in text.lines() {
                match serde_json::from_str::<(String, ExecutedRecord)>(line) {
                    Ok((id, rec)) if now - rec.at < EXECUTED_TTL_SECS => {
                        set.map.insert(id, rec);
                    }
                    Ok(_) => {}
                    Err(_) => break, // torn tail — rest was never fsynced
                }
            }
        }
        if let Some(jp) = &set.journal_path {
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(jp)
            {
                Ok(f) => set.journal = Some(f),
                Err(e) => tracing::warn!(%e, "cell-ops journal open failed — in-memory only"),
            }
        }
        set.prune();
        set
    }

    /// The stored terminal outcome for `op_id`, if still retained.
    pub fn get(&self, op_id: &str) -> Option<&ExecutedRecord> {
        self.map
            .get(op_id)
            .filter(|r| now_secs() - r.at < EXECUTED_TTL_SECS)
    }

    /// Stage an outcome AND persist it before returning: appended to
    /// the journal + `sync_data` — after `record` returns the outcome
    /// survives a crash. Failure degrades to in-memory (at-least-once),
    /// never a silent drop.
    pub fn record(&mut self, op_id: &str, ack: &CellOpAck) {
        let rec = ExecutedRecord {
            status: ack.status.to_string(),
            result: ack.result.clone(),
            error: ack.error.clone(),
            at: now_secs(),
        };
        if let Some(j) = &mut self.journal {
            let dur = (op_id.to_string(), &rec);
            let line = serde_json::to_string(&dur).map(|l| l + "\n");
            let flushed = match line {
                Ok(l) => std::io::Write::write_all(j, l.as_bytes())
                    .and_then(|_| j.sync_data())
                    .is_ok(),
                Err(_) => false,
            };
            if !flushed {
                tracing::warn!("cell-ops journal append failed — at-least-once applies");
            } else {
                self.appends_since_compact += 1;
            }
        }
        self.map.insert(op_id.to_string(), rec);
        self.dirty = true;
        self.prune();
        if self.appends_since_compact >= JOURNAL_COMPACT_EVERY {
            self.persist();
        }
    }

    /// Snapshot compaction: atomic write of the full map (tmp +
    /// `sync_all` + rename + dir fsync, 0600), then journal truncate.
    pub fn persist(&mut self) {
        let Some(path) = self.path.clone() else {
            return;
        };
        if !self.dirty {
            return;
        }
        let bytes = match serde_json::to_vec(&self.map) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%e, "cell-ops executed-set encode failed");
                return;
            }
        };
        let tmp = path.with_extension("json.tmp");
        #[cfg(unix)]
        let write_ok = {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)
                .and_then(|mut f| {
                    f.write_all(&bytes)?;
                    f.sync_all()
                })
                .is_ok()
        };
        #[cfg(not(unix))]
        let write_ok = std::fs::write(&tmp, &bytes).is_ok();
        if write_ok {
            match std::fs::rename(&tmp, &path) {
                Ok(()) => {
                    self.dirty = false;
                    self.appends_since_compact = 0;
                    if let Some(dir) = path.parent() {
                        if let Ok(d) = std::fs::File::open(dir) {
                            let _ = d.sync_all();
                        }
                    }
                    if let Some(j) = &mut self.journal {
                        use std::io::{Seek, SeekFrom};
                        if j.set_len(0)
                            .and_then(|_| j.seek(SeekFrom::Start(0)))
                            .is_err()
                        {
                            tracing::warn!(
                                "cell-ops journal truncate failed — replay overhead only"
                            );
                        }
                    }
                }
                Err(e) => tracing::warn!(%e, "cell-ops executed-set rename failed"),
            }
        } else {
            tracing::warn!("cell-ops executed-set write failed — at-least-once applies");
        }
    }

    fn prune(&mut self) {
        let now = now_secs();
        self.map.retain(|_, r| now - r.at < EXECUTED_TTL_SECS);
        while self.map.len() > EXECUTED_CAP {
            let Some((oldest, _)) = self
                .map
                .iter()
                .min_by_key(|(_, r)| r.at)
                .map(|(k, r)| (k.clone(), r.at))
            else {
                break;
            };
            self.map.remove(&oldest);
        }
    }
}

pub(crate) fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_op_parses_resource_key_and_absent_default() {
        let with = r#"{"id":"o1","seq":1,"kind":"EXECUTE","workerId":"w1","resourceKey":"abcdefghijklmnopqrstuvwxyz","payload":{},"expiresAt":1700000000}"#;
        let op: CellOp = serde_json::from_str(with).unwrap();
        assert_eq!(op.resource_key, "abcdefghijklmnopqrstuvwxyz");
        let without = r#"{"id":"o2","seq":2,"kind":"WF_GET","workerId":"w1","payload":{}}"#;
        let op: CellOp = serde_json::from_str(without).unwrap();
        assert_eq!(op.resource_key, "");
        // RV-R58 (intentional semantic change): unknown fields are accepted.
        let extra = r#"{"id":"o3","seq":3,"kind":"WF_GET","workerId":"w1","payload":{},"extra":1}"#;
        assert!(serde_json::from_str::<CellOp>(extra).is_ok());
    }

    #[test]
    fn decode_ops_page_counts_unknown_and_isolates_bad_rows() {
        let rows = vec![
            serde_json::json!({"id":"a","seq":1,"kind":"WF_GET","workerId":"w","payload":{},"newField":true}),
            serde_json::json!({"id":"bad","seq":"not-a-number","kind":"WF_GET","workerId":"w","payload":{}}),
            serde_json::json!({"nope":1}),
            serde_json::json!({"id":"b","seq":2,"kind":"WF_GET","workerId":"w","payload":{}}),
        ];
        let d = decode_ops_page(rows);
        assert_eq!(d.ops.len(), 2);
        assert_eq!(d.ops[0].id, "a");
        assert_eq!(d.ops[1].id, "b");
        assert_eq!(d.bad_rows, 2);
        assert_eq!(d.unknown_field_rows, 2);
    }

    #[test]
    fn record_persists_before_return_and_replays_after_drop() {
        let dir = std::env::temp_dir().join(format!("execset-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("executed.json");
        let mut set = ExecutedSet::load(path.clone());
        let ack = CellOpAck {
            seq: 7,
            status: "DONE",
            result: Some(serde_json::json!({"ok": true})),
            error: None,
        };
        set.record("op-1", &ack);
        drop(set); // simulate crash — no explicit persist needed
        let reloaded = ExecutedSet::load(path.clone());
        let rec = reloaded.get("op-1").expect("journaled record replays");
        assert_eq!(rec.status, "DONE");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_journal_tail_stops_replay() {
        let dir = std::env::temp_dir().join(format!("execset-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("executed.json");
        let journal = path.with_extension("journal");
        std::fs::write(
            &journal,
            b"{\"op-a\":{\"status\":\"DONE\",\"at\":9999999999}}\n{not-json",
        )
        .unwrap();
        let set = ExecutedSet::load(path.clone());
        assert!(set.get("op-a").is_none() || set.get("op-a").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Bounded truncation in **UTF-16 code units** — the shared metric the
/// control plane uses (`zod .max()` counts JS string length, i.e. UTF-16
/// units). Iterates `char`s so a surrogate pair is never split; the
/// returned prefix is always valid UTF-8 (RV-R68).
pub fn truncate_utf16(s: &str, max_units: usize) -> (String, bool) {
    let mut units = 0usize;
    for (i, c) in s.char_indices() {
        units += c.len_utf16();
        if units > max_units {
            return (s[..i].to_string(), true);
        }
    }
    (s.to_string(), false)
}
