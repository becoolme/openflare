//! MIG-4: bounded per-worker generation exclusion — blast-radius
//! shrink for tenant-level `SwitchFailed`.
//!
//! A team-level tenant packs many workers into ONE workerd process /
//! generation: a single worker whose code cannot load (top-level
//! throw) makes the WHOLE config fail `workerd test`, which used to
//! latch the tenant `SwitchFailed` and block every sibling worker's
//! publish. When the failure diagnostic attributes the failure to a
//! concrete `service worker-<id>` (workerd's own `service <name>:`
//! stderr prefix), the supervisor may instead rebuild the generation
//! WITHOUT that worker — siblings switch normally, the excluded
//! worker keeps its old generation / 503s, and the tenant reports the
//! precise `workerIds` under `tenant_failures`.
//!
//! Trust model:
//! - stderr is UNTRUSTED input — a worker's own output could forge a
//!   `service worker-<sibling>` line. Mitigations: only the bounded,
//!   sanitized tail is parsed; ids must match the VRT identifier
//!   charset AND be members of THIS tenant's desired worker set;
//!   exclusions are capped per desired (`MAX_EXCLUDED_PER_DESIRED`);
//!   and exclusion only ever shrinks the blast radius versus the
//!   whole-tenant latch it replaces.
//! - eligibility is structural: a worker that declares DO classes or
//!   workflows (its storage identity lives under the worker's own
//!   segment) or is referenced by a sibling (service binding, DO
//!   `serviceWorkerId`, workflow `script_name`, DO transfer source)
//!   is NEVER excluded — removing it would break innocent workers or
//!   orphan provisioned storage, i.e. GROW the blast radius.

use snapshot_protocol::compute_desired::{BindingDesired, TenantDesired};
use std::collections::BTreeSet;

/// Per-desired exclusion bound — a misattributed stderr line can only
/// ever shrink a generation by this many workers before the tenant
/// falls back to the classic `SwitchFailed` latch.
pub const MAX_EXCLUDED_PER_DESIRED: usize = 8;

/// Bytes of diagnostic text scanned for `service worker-<id>`
/// attributions. Input is the already-sanitized stderr tail — this is
/// a second, hard bound so an unbounded blob can never keep the parser
/// busy.
const MAX_ATTRIBUTION_SCAN_BYTES: usize = 64 * 1024;

/// Valid VRT worker-id charset — `[A-Za-z0-9_.:-]`, non-empty, ≤128
/// bytes (mirrors `compute_desired`'s identifier rule; the same shape
/// `worker-<id>` service names are minted under).
fn valid_worker_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':')
        })
}

/// Extract `service worker-<id>` candidates from a sanitized workerd
/// diagnostic blob — first-seen order, validated charset/length.
/// Membership in the desired worker set is the CALLER's trust check;
/// this function only locates syntactically valid names.
pub fn parse_failed_services(detail: &str) -> Vec<String> {
    const PREFIX: &str = "service worker-";
    let scan = &detail[..detail.len().min(MAX_ATTRIBUTION_SCAN_BYTES)];
    let mut out: Vec<String> = Vec::new();
    let mut idx = 0usize;
    while let Some(off) = scan[idx..].find(PREFIX) {
        let start = idx + off;
        // The match must begin a token — `xservice worker-a` or
        // `service worker-a` inside a larger identifier must not
        // attribute. Whitespace, quotes and brackets all count as
        // boundaries (workerd logs bare, quoted and parenthesized).
        let boundary = scan[..start]
            .chars()
            .next_back()
            .map(|c| {
                c.is_whitespace()
                    || matches!(c, '"' | '\'' | '(' | '[' | '{' | '<' | ':' | '=')
            })
            .unwrap_or(true);
        let name_start = start + PREFIX.len();
        let tail = &scan[name_start..];
        let name_len = tail
            .find(|c: char| {
                !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
            })
            .unwrap_or(tail.len());
        // `:` is a legal id char but it is ALSO workerd's
        // `service <name>:` log separator — a trailing colon is the
        // separator, never the id. Interior colons (rare but legal)
        // are preserved; membership in the desired set is the real
        // trust check anyway.
        let candidate = tail[..name_len].trim_end_matches(':');
        if boundary && valid_worker_id(candidate) && !out.iter().any(|x| x == candidate) {
            out.push(candidate.to_string());
            if out.len() >= MAX_EXCLUDED_PER_DESIRED {
                break;
            }
        }
        idx = name_start + name_len;
    }
    out
}

/// Why a candidate may NOT be dropped from the rebuilt generation —
/// each variant falls back to the whole-tenant `SwitchFailed` path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ineligible {
    /// Not a worker of the desired tenant (stale/forged attribution).
    NotInDesired,
    /// Declares DO classes — its storage identity lives under its own
    /// segment; excluding it would orphan class dirs and wedge the
    /// serialized DO switch gate.
    DeclaresDurableObjects,
    /// Declares workflows — the engine DO's unique_key is provisioned
    /// under the declaring worker's segment (same orphan risk).
    DeclaresWorkflows,
    /// A sibling references it (service binding, DO `serviceWorkerId`,
    /// workflow `script_name`, DO transfer source) — removing it
    /// would make the SIBLING's config fail too, growing the blast
    /// radius instead of shrinking it.
    ReferencedBySibling,
    /// Exclusion budget for this desired is already spent.
    CapReached,
}

impl Ineligible {
    /// Stable metric/log label.
    pub fn code(self) -> &'static str {
        match self {
            Self::NotInDesired => "not-in-desired",
            Self::DeclaresDurableObjects => "declares-do",
            Self::DeclaresWorkflows => "declares-workflows",
            Self::ReferencedBySibling => "referenced-by-sibling",
            Self::CapReached => "cap-reached",
        }
    }
}

/// Structural eligibility check: may `worker_id` be dropped from
/// `tenant`'s rebuild without breaking or orphaning anything else?
/// `already_excluded` workers are treated as absent (their references
/// die with them — they are already not served).
pub fn exclusion_verdict(
    tenant: &TenantDesired,
    worker_id: &str,
    already_excluded: &BTreeSet<String>,
) -> Result<(), Ineligible> {
    if already_excluded.len() >= MAX_EXCLUDED_PER_DESIRED {
        return Err(Ineligible::CapReached);
    }
    let Some(w) = tenant.workers.iter().find(|w| w.worker_id == worker_id) else {
        return Err(Ineligible::NotInDesired);
    };
    if !w.durable_object_classes.is_empty() {
        return Err(Ineligible::DeclaresDurableObjects);
    }
    if !w.workflows.is_empty() {
        return Err(Ineligible::DeclaresWorkflows);
    }
    let live_reference = |other: &snapshot_protocol::compute_desired::WorkerDesired| -> bool {
        other.bindings.iter().any(|b| match b {
            BindingDesired::Service(s) => s.worker_id == worker_id,
            BindingDesired::DurableObject(d) => {
                d.service_worker_id.as_deref() == Some(worker_id)
            }
            _ => false,
        }) || other
            .workflows
            .iter()
            .any(|wf| wf.service_worker_id.as_deref() == Some(worker_id))
            || other
                .durable_object_classes
                .iter()
                .any(|c| c.from_worker_id.as_deref() == Some(worker_id))
    };
    for other in &tenant.workers {
        if other.worker_id == worker_id || already_excluded.contains(&other.worker_id) {
            continue;
        }
        if live_reference(other) {
            return Err(Ineligible::ReferencedBySibling);
        }
    }
    Ok(())
}

/// The tenant fragment with `excluded` workers removed — `None` when
/// nothing would remain (an all-excluded generation serves nothing;
/// fall back to the latch rather than launching an empty process).
pub fn filtered_tenant(tenant: &TenantDesired, excluded: &BTreeSet<String>) -> Option<TenantDesired> {
    let mut t = tenant.clone();
    t.workers.retain(|w| !excluded.contains(&w.worker_id));
    (!t.workers.is_empty()).then_some(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_service_attribution() {
        let d = r#"config error: service worker-abc123: Uncaught Error: mig-bad top-level boom
            at worker-abc123:3:14"#;
        assert_eq!(parse_failed_services(d), vec!["abc123".to_string()]);
    }

    #[test]
    fn skips_invalid_and_injected_ids() {
        // `worker-` followed by a non-identifier char yields an empty
        // candidate; shell-ish injection stops the id at the boundary.
        assert!(parse_failed_services("service worker-; rm -rf /").is_empty());
        assert_eq!(
            parse_failed_services("service worker-a|evil"),
            vec!["a".to_string()]
        );
        assert!(parse_failed_services("service worker-").is_empty());
        // Oversized id rejected.
        assert!(parse_failed_services(&format!("service worker-{}", "x".repeat(200))).is_empty());
        // Non-boundary prefix does not attribute.
        assert!(parse_failed_services("xservice worker-a").is_empty());
        // Dedup + multiple services.
        assert_eq!(
            parse_failed_services("service worker-a fails; service worker-a again; service worker-b too"),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    use snapshot_protocol::compute_desired::{
        BindingDesired, DoClassDesired, ServiceBinding, TenantDesired, VersionDesired,
        WorkerDesired, WorkerQuotas, WorkflowDesired,
    };

    fn w(id: &str) -> WorkerDesired {
        WorkerDesired {
            worker_id: id.to_string(),
            name: id.to_string(),
            active_version: VersionDesired {
                version_id: format!("v-{id}"),
                artifact_digest: "a".repeat(64),
                compatibility_date: "2024-01-01".to_string(),
                compatibility_flags: Vec::new(),
            },
            bindings: Vec::new(),
            routes: Vec::new(),
            crons: Vec::new(),
            queue_consumers: Vec::new(),
            durable_object_classes: Vec::new(),
            workflows: Vec::new(),
            quotas: WorkerQuotas {
                cpu_ms: 10,
                memory_mb: 32,
                subrequests: 10,
                outbound_connections: 4,
            },
        }
    }

    fn tenant(ids: &[&str]) -> TenantDesired {
        TenantDesired {
            tenant_id: "team-a".to_string(),
            workers: ids.iter().map(|id| w(id)).collect(),
        }
    }

    #[test]
    fn verdict_accepts_unreferenced_plain_worker() {
        let t = tenant(&["good", "bad"]);
        assert_eq!(
            exclusion_verdict(&t, "bad", &BTreeSet::new()),
            Ok(())
        );
        // Excluding `bad` leaves `good` — the reduced generation
        // keeps every other worker.
        let excluded: BTreeSet<String> = ["bad".to_string()].into_iter().collect();
        let t2 = filtered_tenant(&t, &excluded).unwrap();
        assert_eq!(
            t2.workers.iter().map(|w| w.worker_id.as_str()).collect::<Vec<_>>(),
            vec!["good"]
        );
    }

    #[test]
    fn verdict_rejects_not_in_desired_and_cap() {
        let t = tenant(&["good"]);
        // A forged/stale attribution naming a worker outside this
        // tenant is refused outright.
        assert_eq!(
            exclusion_verdict(&t, "other-tenant-worker", &BTreeSet::new()),
            Err(Ineligible::NotInDesired)
        );
        // Budget exhausted → fall back to the whole-tenant latch.
        let full: BTreeSet<String> = (0..MAX_EXCLUDED_PER_DESIRED)
            .map(|i| format!("w{i}"))
            .collect();
        assert_eq!(
            exclusion_verdict(&t, "good", &full),
            Err(Ineligible::CapReached)
        );
    }

    #[test]
    fn verdict_rejects_do_workflow_and_referenced_workers() {
        // DO-declaring worker: its storage identity lives under its
        // own segment — excluding it would orphan provisioned data.
        let mut t = tenant(&["good", "dower"]);
        t.workers[1].durable_object_classes.push(DoClassDesired {
            class_name: "Counter".to_string(),
            unique_key: "b".repeat(64),
            enable_sql: true,
            from_worker_id: None,
        });
        assert_eq!(
            exclusion_verdict(&t, "dower", &BTreeSet::new()),
            Err(Ineligible::DeclaresDurableObjects)
        );
        // Workflow-declaring worker: same engine-storage orphan risk.
        let mut t = tenant(&["good", "wf"]);
        t.workers[1].workflows.push(WorkflowDesired {
            name: "wflow".to_string(),
            binding: "WF".to_string(),
            class_name: "Flow".to_string(),
            service_worker_id: None,
            unique_key: "c".repeat(64),
            limits_steps: None,
        });
        assert_eq!(
            exclusion_verdict(&t, "wf", &BTreeSet::new()),
            Err(Ineligible::DeclaresWorkflows)
        );
        // Service-binding reference: `caller` binds `bad` — removing
        // `bad` would break the sibling too.
        let mut t = tenant(&["caller", "bad"]);
        t.workers[0].bindings.push(BindingDesired::Service(ServiceBinding {
            name: "SVC".to_string(),
            worker_id: "bad".to_string(),
        }));
        assert_eq!(
            exclusion_verdict(&t, "bad", &BTreeSet::new()),
            Err(Ineligible::ReferencedBySibling)
        );
        // ...unless the referencing sibling is itself already excluded
        // (its references die with it).
        let excluded: BTreeSet<String> = ["caller".to_string()].into_iter().collect();
        assert_eq!(exclusion_verdict(&t, "bad", &excluded), Ok(()));
    }

    #[test]
    fn filtered_tenant_none_when_all_excluded() {
        let t = tenant(&["only"]);
        let excluded: BTreeSet<String> = ["only".to_string()].into_iter().collect();
        assert!(filtered_tenant(&t, &excluded).is_none());
    }

    #[test]
    fn id_charset() {
        assert!(valid_worker_id("wk_01.23:x"));
        assert!(!valid_worker_id(""));
        assert!(!valid_worker_id("a b"));
        assert!(!valid_worker_id("a/b"));
        assert!(!valid_worker_id(&"x".repeat(129)));
    }
}
