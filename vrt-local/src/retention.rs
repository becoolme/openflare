//! Preview-version retention (GC) policy.
//!
//! `deploy --preview` stores a version without moving traffic and, unlike
//! a normal deploy, never evicts anything: without a retention policy the
//! preview history of a worker grows forever. This module owns the typed
//! policy, the pure planning step and the state mutation, so the same
//! rules apply to the automatic pass at deploy time and to
//! `of versions prune`.
//!
//! Safety rules (see `docs/versioning.md`):
//!
//! - a worker's ACTIVE version is never a pruning candidate (it is not a
//!   preview record; the mutation re-checks this);
//! - the NEWEST preview of a worker is always kept, even when it is past
//!   the age limit, so the last previewed build stays inspectable;
//! - pruning only removes version RECORDS; `gc_objects` then deletes
//!   artifact objects no remaining version (of any worker) references, so
//!   a shared content-addressed bundle survives;
//! - the policy is explicit and stored in `state/retention.json`; an
//!   absent file means the defaults, a malformed file fails closed.
//!
//! The policy lives in its own file (not `state.json`) on purpose: an
//! older build must still be able to read and rewrite the state document
//! after a newer build customized retention.

use crate::state::{LocalState, VersionStatus};
use crate::LocalError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Default preview retention: 7 days.
pub const DEFAULT_MAX_AGE_DAYS: u64 = 7;
/// Default preview retention: 4 per worker — the same bound the signed
/// desired document uses for the previews it renders, so a stored
/// preview is normally also a servable one.
pub const DEFAULT_MAX_COUNT: u32 =
    snapshot_protocol::compute_desired::MAX_PREVIEWS_PER_WORKER as u32;
/// Hard ceiling for `maxAgeDays` (ten years) — rejects absurd values
/// that would overflow the millisecond arithmetic.
pub const MAX_AGE_DAYS_CEILING: u64 = 3_650;
/// Hard ceiling for `maxCount` — same bound as retained versions.
pub const MAX_COUNT_CEILING: u32 = crate::state::MAX_VERSIONS_CEILING;

/// One millisecond day.
const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// Explicit preview retention policy.
///
/// `max_age_days = 0` means "no age limit", `max_count = 0` means "no
/// count limit"; an ENABLED policy must keep at least one of them set
/// (a policy that can never remove anything is a configuration bug, not
/// a way to disable GC — use `enabled = false`).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviewRetention {
    /// Run the automatic pass at the end of every deploy and let
    /// `of versions prune` use this policy. A manual `prune` always
    /// runs, even when automatic pruning is disabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Remove preview versions older than this many days.
    #[serde(default = "default_max_age_days")]
    pub max_age_days: u64,
    /// Keep at most this many preview versions per worker (newest
    /// first). The newest preview is always kept.
    #[serde(default = "default_max_count")]
    pub max_count: u32,
}

fn default_enabled() -> bool {
    true
}
fn default_max_age_days() -> u64 {
    DEFAULT_MAX_AGE_DAYS
}
fn default_max_count() -> u32 {
    DEFAULT_MAX_COUNT
}

impl Default for PreviewRetention {
    fn default() -> Self {
        Self {
            enabled: true,
            max_age_days: DEFAULT_MAX_AGE_DAYS,
            max_count: DEFAULT_MAX_COUNT,
        }
    }
}

impl PreviewRetention {
    /// Structural bounds check — refuses values this crate will not
    /// apply instead of clamping them silently.
    pub fn validate(&self) -> Result<(), LocalError> {
        if self.max_age_days > MAX_AGE_DAYS_CEILING {
            return Err(LocalError::State("retentionMaxAgeDays"));
        }
        if self.max_count > MAX_COUNT_CEILING {
            return Err(LocalError::State("retentionMaxCount"));
        }
        if self.enabled && self.max_age_days == 0 && self.max_count == 0 {
            // Fail closed: this would look enabled while never removing
            // anything. Disabling is explicit (`enabled = false`).
            return Err(LocalError::State("retentionLimits"));
        }
        Ok(())
    }

    /// Apply a CLI/API patch (each `Some` field wins) and validate.
    pub fn patched(&self, update: &RetentionUpdate) -> Result<Self, LocalError> {
        let mut next = *self;
        if let Some(enabled) = update.enabled {
            next.enabled = enabled;
        }
        if let Some(days) = update.max_age_days {
            next.max_age_days = days;
        }
        if let Some(count) = update.max_count {
            next.max_count = count;
        }
        next.validate()?;
        Ok(next)
    }
}

/// Partial policy update (`of versions retention`). Omitted fields keep
/// the stored value.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetentionUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_days: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_count: Option<u32>,
}

impl RetentionUpdate {
    pub fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.max_age_days.is_none() && self.max_count.is_none()
    }
}

/// Why a preview version was selected for removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PruneReason {
    /// Older than `max_age_days`.
    Age,
    /// Beyond `max_count` previews (newest first).
    Count,
}

/// One version selected for removal — also the audit record written to
/// `state/oplog.jsonl`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrunedVersion {
    pub worker: String,
    pub version_id: String,
    pub artifact_digest: String,
    pub created_at_unix_ms: u64,
    pub reason: PruneReason,
}

/// The policy file under `<root>/state/`.
pub fn policy_path(root: &Path) -> std::path::PathBuf {
    crate::layout::state_dir(root).join("retention.json")
}

/// Read the stored policy. Absent ⇒ [`PreviewRetention::default`];
/// unreadable/malformed/out-of-bounds ⇒ fail closed (never "prune with
/// guessed limits").
pub fn read(root: &Path) -> Result<PreviewRetention, LocalError> {
    let raw = match std::fs::read(policy_path(root)) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(PreviewRetention::default()),
        Err(_) => return Err(LocalError::Layout("retention read")),
    };
    let policy: PreviewRetention =
        serde_json::from_slice(&raw).map_err(|_| LocalError::State("retention decode"))?;
    policy.validate()?;
    Ok(policy)
}

/// Persist a validated policy (`0600`, atomic).
pub fn write(root: &Path, policy: &PreviewRetention) -> Result<(), LocalError> {
    policy.validate()?;
    let bytes = serde_json::to_vec_pretty(policy).map_err(|_| LocalError::State("retention"))?;
    vrt_securefs::atomic_write(&policy_path(root), &bytes, 0o600)
        .map_err(|_| LocalError::Layout("retention write"))
}

/// Pure planning step. `worker` is a display name OR `workerId`; `None`
/// plans for every worker. The state is not mutated, so callers can use
/// this for `--dry-run` and for the audit-first auto pass.
///
/// Deterministic order: workers in state order, versions newest first.
pub fn plan(
    st: &LocalState,
    worker: Option<&str>,
    policy: PreviewRetention,
    now_unix_ms: u64,
) -> Vec<PrunedVersion> {
    let mut out = Vec::new();
    for (name, record) in &st.workers {
        if let Some(query) = worker {
            if name != query && record.worker_id != query {
                continue;
            }
        }
        let active = record.active_version_id.as_deref();
        // `(index, version)` pairs: the Vec is append-only for new
        // versions, so a HIGHER index means "created later" even when
        // two previews share a millisecond timestamp. Tie-breaking on
        // position (not on version id) guarantees the version a deploy
        // just created is treated as the newest.
        let mut previews: Vec<(usize, &crate::state::VersionRecord)> = record
            .versions
            .iter()
            .enumerate()
            .filter(|(_, v)| v.status == VersionStatus::Preview)
            .filter(|(_, v)| active != Some(v.version_id.as_str()))
            .collect();
        previews.sort_by(|(ia, a), (ib, b)| {
            b.created_at_unix_ms
                .cmp(&a.created_at_unix_ms)
                .then_with(|| ib.cmp(ia))
        });
        for (index, (_, version)) in previews.iter().enumerate() {
            // The newest preview always stays: it is the build an
            // operator just created (and normally the one projected by
            // the signed desired document).
            if index == 0 {
                continue;
            }
            let over_count = policy.max_count > 0 && index >= policy.max_count as usize;
            // `created_at_unix_ms == 0` is a pre-MVP record with unknown
            // age — never guess it is old.
            let too_old = policy.max_age_days > 0
                && version.created_at_unix_ms > 0
                && now_unix_ms.saturating_sub(version.created_at_unix_ms)
                    > policy.max_age_days.saturating_mul(DAY_MS);
            let reason = if over_count {
                Some(PruneReason::Count)
            } else if too_old {
                Some(PruneReason::Age)
            } else {
                None
            };
            if let Some(reason) = reason {
                out.push(PrunedVersion {
                    worker: name.clone(),
                    version_id: version.version_id.clone(),
                    artifact_digest: version.artifact_digest.clone(),
                    created_at_unix_ms: version.created_at_unix_ms,
                    reason,
                });
            }
        }
    }
    out
}

/// Remove the planned records. Returns exactly the entries that were
/// actually deleted, in plan order (the audit record must never claim a
/// removal that did not happen).
///
/// Re-checks both invariants at mutation time (status is `Preview` AND
/// the record is not selected by `activeVersionId`), so a stale plan can
/// never delete a version that a concurrent promote just activated. The
/// caller holds the writer lock.
pub fn apply(st: &mut LocalState, plan: &[PrunedVersion]) -> Vec<PrunedVersion> {
    if plan.is_empty() {
        return Vec::new();
    }
    let mut by_worker: std::collections::BTreeMap<&str, std::collections::BTreeSet<&str>> =
        Default::default();
    for p in plan {
        by_worker
            .entry(p.worker.as_str())
            .or_default()
            .insert(p.version_id.as_str());
    }
    let mut removed: std::collections::BTreeSet<(String, String)> = Default::default();
    for (name, record) in st.workers.iter_mut() {
        let Some(ids) = by_worker.get(name.as_str()) else {
            continue;
        };
        let active = record.active_version_id.as_deref();
        record.versions.retain(|version| {
            let candidate = version.status == VersionStatus::Preview
                && ids.contains(version.version_id.as_str())
                && active != Some(version.version_id.as_str());
            if candidate {
                removed.insert((name.clone(), version.version_id.clone()));
            }
            !candidate
        });
    }
    plan.iter()
        .filter(|p| removed.contains(&(p.worker.clone(), p.version_id.clone())))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn pkg(name: &str) -> crate::worker_pkg::PackedWorker {
        crate::worker_pkg::PackedWorker {
            name: name.into(),
            digest: "a".repeat(64),
            stored_document: Vec::new(),
            compatibility_date: "2025-01-01".into(),
            compatibility_flags: vec![],
            bindings: vec![],
            var_values: BTreeMap::new(),
            crons: vec![],
            queue_consumers: vec![],
            tail_consumers: vec![],
            observability: None,
            streaming_tail_consumers: vec![],
            flagship_apps: vec![],
            durable_object_classes: vec![],
            workflows: vec![],
            hostnames: vec![],
            quotas: snapshot_protocol::compute_desired::WorkerQuotas::platform_default(),
            first_deploy_id: "0123456789abcdef".into(),
            pending_assets: None,
            tag: String::new(),
        }
    }

    fn preview(local: &mut LocalState, name: &str, flag: &str, at: u64) -> String {
        let mut p = pkg(name);
        p.compatibility_flags = vec![flag.into()];
        let id = LocalState::version_id(&p);
        local.apply_preview(&p, at).unwrap();
        id
    }

    #[test]
    fn defaults_are_explicit_and_valid() {
        let policy = PreviewRetention::default();
        policy.validate().unwrap();
        assert!(policy.enabled);
        assert_eq!(policy.max_age_days, 7);
        assert_eq!(policy.max_count, 4);
    }

    #[test]
    fn an_enabled_policy_must_have_a_limit() {
        let policy = PreviewRetention {
            enabled: true,
            max_age_days: 0,
            max_count: 0,
        };
        assert!(matches!(
            policy.validate(),
            Err(LocalError::State("retentionLimits"))
        ));
        let disabled = PreviewRetention {
            enabled: false,
            max_age_days: 0,
            max_count: 0,
        };
        disabled.validate().unwrap();
    }

    #[test]
    fn the_newest_preview_and_the_active_version_are_never_planned() {
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg("svc"), 1).unwrap();
        let newest = preview(&mut st, "svc", "newest", 10_000);
        let older = preview(&mut st, "svc", "older", 5_000);
        let policy = PreviewRetention {
            enabled: true,
            max_age_days: 1,
            max_count: 1,
        };
        let candidates = plan(&st, None, policy, 10_000_000_000);
        assert_eq!(
            candidates
                .iter()
                .map(|p| p.version_id.as_str())
                .collect::<Vec<_>>(),
            vec![older.as_str()],
            "only the non-newest preview is a candidate"
        );
        assert!(!candidates.iter().any(|p| p.version_id == newest));
        // Promoting the older preview protects it too.
        st.promote("svc", &older).unwrap();
        let after_promote = plan(&st, None, policy, 10_000_000_000);
        let active = st.workers["svc"].active_version_id.clone().unwrap();
        assert!(after_promote.iter().all(|p| p.version_id != active));
        assert!(after_promote.iter().all(|p| p.version_id != newest));
    }

    #[test]
    fn count_policy_keeps_the_newest_n_and_ignores_archived() {
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg("svc"), 1).unwrap();
        let mut ids = Vec::new();
        for (i, flag) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            ids.push(preview(&mut st, "svc", flag, 100 + i as u64));
        }
        let policy = PreviewRetention {
            enabled: true,
            max_age_days: 0,
            max_count: 2,
        };
        let plan = plan(&st, Some("svc"), policy, 1_000);
        let planned: Vec<&str> = plan.iter().map(|p| p.version_id.as_str()).collect();
        assert_eq!(
            planned,
            vec![ids[2].as_str(), ids[1].as_str(), ids[0].as_str()],
            "everything older than the newest 2, newest first"
        );
        assert!(plan.iter().all(|p| p.reason == PruneReason::Count));
        assert_eq!(apply(&mut st, &plan).len(), 3);
        let stored: Vec<&str> = st.workers["svc"]
            .versions
            .iter()
            .map(|v| v.version_id.as_str())
            .collect();
        assert!(stored.contains(&ids[4].as_str()));
        assert!(stored.contains(&ids[3].as_str()));
        assert_eq!(stored.len(), 3, "active + 2 previews");
    }

    #[test]
    fn age_policy_uses_strictly_older_than_and_keeps_unknown_ages() {
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg("svc"), 1).unwrap();
        let newest = preview(&mut st, "svc", "newest", 9_000);
        let stale = preview(&mut st, "svc", "stale", 1_000);
        let policy = PreviewRetention {
            enabled: true,
            max_age_days: 1,
            max_count: 0,
        };
        // Exactly at the boundary: age == 1 day is NOT "older than".
        let at_boundary = plan(&st, None, policy, 1_000 + DAY_MS);
        assert!(
            at_boundary.is_empty(),
            "boundary is inclusive: {at_boundary:?}"
        );
        let past_boundary = plan(&st, None, policy, 1_000 + DAY_MS + 1);
        assert_eq!(
            past_boundary
                .iter()
                .map(|p| p.version_id.as_str())
                .collect::<Vec<_>>(),
            vec![stale.as_str()]
        );
        assert_eq!(past_boundary[0].reason, PruneReason::Age);
        assert!(!past_boundary.iter().any(|p| p.version_id == newest));
    }

    #[test]
    fn apply_is_idempotent_and_never_removes_non_previews() {
        let mut st = LocalState::new("local", "local");
        st.revision = 1;
        st.apply_deploy(&pkg("svc"), 1).unwrap();
        let a = preview(&mut st, "svc", "a", 100);
        preview(&mut st, "svc", "b", 200);
        let policy = PreviewRetention {
            enabled: true,
            max_age_days: 0,
            max_count: 1,
        };
        let plan = plan(&st, None, policy, 1_000);
        assert_eq!(apply(&mut st, &plan).len(), 1);
        assert!(!st.workers["svc"]
            .versions
            .iter()
            .any(|v| v.version_id == a));
        assert!(apply(&mut st, &plan).is_empty(), "replay removes nothing");
        // A stale plan naming the active version must be refused at
        // mutation time.
        let active = st.workers["svc"].active_version_id.clone().unwrap();
        let stale = vec![PrunedVersion {
            worker: "svc".into(),
            version_id: active.clone(),
            artifact_digest: String::new(),
            created_at_unix_ms: 1,
            reason: PruneReason::Count,
        }];
        assert!(apply(&mut st, &stale).is_empty());
        assert_eq!(st.workers["svc"].active_version_id.as_deref(), Some(active.as_str()));
    }
}
