//! Preview retention / GC (PR: version GC and retention policy).
//!
//! These tests drive the real state files and object store:
//!
//! - a stored preview past the age limit is removed together with its
//!   now-unreferenced artifact;
//! - the ACTIVE version and the NEWEST preview are never removed;
//! - a dry run reports candidates without writing;
//! - a content-addressed artifact shared with a retained version
//!   survives (reference counting by digest, not by version);
//! - the automatic pass at deploy time bounds the preview history;
//! - prune and promote racing on the same version never leave a
//!   dangling active pointer or a missing artifact.

use std::path::{Path, PathBuf};
use std::sync::Barrier;
use vrt_local::{
    DeployOverrides, InitParams, LocalError, PruneRequest, RetentionUpdate, VrtLocal,
};
use vrt_local::VersionStatus;

struct TmpDir(PathBuf);

impl std::ops::Deref for TmpDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TmpDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn root() -> TmpDir {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-retention-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TmpDir(dir)
}

fn init(root: &Path) -> VrtLocal {
    VrtLocal::init(
        root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: None,
        },
    )
    .unwrap();
    VrtLocal::open(root).unwrap()
}

fn worker_dir(root: &Path, name: &str, body: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = root.join(format!(
        "worker-{name}-{}",
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), body).unwrap();
    dir
}

fn overrides(vars: &[(&str, &str)]) -> DeployOverrides {
    DeployOverrides {
        vars: vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    }
}

/// Rewrite `createdAtUnixMs` of `worker`'s records to a fixed pre-2002
/// timestamp — the only way to exercise the age rule without waiting.
/// `only_previews` leaves the active record alone.
fn age_versions(root: &Path, worker: &str, only_previews: bool) {
    let path = root.join("state/state.json");
    let mut doc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let versions = doc["workers"][worker]["versions"]
        .as_array_mut()
        .unwrap_or_else(|| panic!("worker {worker} missing from state"));
    let mut aged = 0;
    for v in versions.iter_mut() {
        if !only_previews || v["status"] == "preview" {
            v["createdAtUnixMs"] = serde_json::json!(1_000_000_000_000u64);
            aged += 1;
        }
    }
    assert!(aged > 0, "no versions to age");
    std::fs::write(&path, serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
}

fn age_previews(root: &Path, worker: &str) {
    age_versions(root, worker, true)
}

fn state(root: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(root.join("state/state.json")).unwrap()).unwrap()
}

fn versions_of<'a>(st: &'a serde_json::Value, worker: &str) -> &'a Vec<serde_json::Value> {
    st["workers"][worker]["versions"].as_array().unwrap()
}

fn oplog(root: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(root.join("state/oplog.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn object_exists(root: &Path, digest: &str) -> bool {
    root.join("objects").join(digest).is_file()
}

#[test]
fn expired_preview_is_pruned_with_its_artifact_and_audited() {
    let root = root();
    let local = init(&root);
    let prod = worker_dir(&root, "gc", "export default {v: 1}\n");
    let active = local.deploy(&prod, &overrides(&[])).unwrap();
    let old_dir = worker_dir(&root, "gc", "export default {v: 2}\n");
    let preview = local
        .deploy_preview(&old_dir, &overrides(&[("PREVIEW_ONLY", "1")]))
        .unwrap();
    // A second, NEWER preview: the newest preview is protected by
    // policy, so without it the aged preview would never be a candidate.
    let new_dir = worker_dir(&root, "gc", "export default {v: 3}\n");
    let newest = local.deploy_preview(&new_dir, &overrides(&[])).unwrap();
    assert_ne!(active.version_id, preview.version_id);
    assert_ne!(preview.artifact_digest, newest.artifact_digest);
    assert!(object_exists(&root, &preview.artifact_digest));

    age_previews(&root, "gc");
    let report = local.prune_versions(&PruneRequest::default()).unwrap();
    assert!(report.changed);
    assert!(!report.dry_run);
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].version_id, preview.version_id);
    assert_eq!(report.removed[0].worker, "gc");

    let st = state(&root);
    let ids: Vec<&str> = versions_of(&st, "gc")
        .iter()
        .map(|v| v["versionId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&active.version_id.as_str()), "active survives");
    assert!(!ids.contains(&preview.version_id.as_str()), "expired removed");
    assert!(
        !object_exists(&root, &preview.artifact_digest),
        "unreferenced artifact is collected"
    );
    assert!(
        object_exists(&root, &active.artifact_digest),
        "the active artifact is untouched"
    );

    // Desired still serves exactly the active version.
    let envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("desired/envelope.json")).unwrap()).unwrap();
    let workers = envelope["payload"]["tenants"][0]["workers"]
        .as_array()
        .unwrap();
    assert_eq!(
        workers.len(),
        2,
        "the active worker plus the protected newest preview"
    );
    let production = workers.iter().find(|w| w.get("preview").is_none()).unwrap();
    assert_eq!(
        production["activeVersion"]["versionId"].as_str().unwrap(),
        active.version_id
    );
    let preview_instance = workers.iter().find(|w| w.get("preview").is_some()).unwrap();
    assert_eq!(
        preview_instance["activeVersion"]["versionId"]
            .as_str()
            .unwrap(),
        newest.version_id
    );

    // Audit trail: one versions-prune event naming what was removed.
    let event = oplog(&root)
        .into_iter()
        .find(|e| e["op"] == "versions-prune")
        .expect("prune must be audited");
    assert_eq!(event["fields"]["trigger"], "manual");
    assert_eq!(event["fields"]["removed"][0]["versionId"], preview.version_id);
    assert_eq!(event["fields"]["removed"][0]["reason"], "age");

    // Idempotent replay: nothing left to remove, no revision churn.
    let revision = local.status().unwrap().revision;
    let again = local.prune_versions(&PruneRequest::default()).unwrap();
    assert!(!again.changed);
    assert!(again.removed.is_empty());
    assert_eq!(again.revision, revision);
    assert_eq!(local.status().unwrap().revision, revision);
}

#[test]
fn newest_preview_and_active_version_are_never_pruned() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "keep", "export default {v: 1}\n");
    local.deploy(&dir, &overrides(&[])).unwrap();
    let older = local
        .deploy_preview(&dir, &overrides(&[("P", "older")]))
        .unwrap();
    let newest = local
        .deploy_preview(&dir, &overrides(&[("P", "newest")]))
        .unwrap();

    // Age both previews: exactly the older one may go.
    age_previews(&root, "keep");
    let report = local
        .prune_versions(&PruneRequest {
            max_age_days: Some(1),
            max_count: Some(0),
            ..Default::default()
        })
        .unwrap();
    let removed: Vec<&str> = report
        .removed
        .iter()
        .map(|p| p.version_id.as_str())
        .collect();
    assert_eq!(removed, vec![older.version_id.as_str()]);
    assert!(!removed.contains(&newest.version_id.as_str()));

    // An active version is protected even when its age is past the limit.
    local.promote("keep", &newest.version_id).unwrap();
    age_versions(&root, "keep", false);
    let report = local
        .prune_versions(&PruneRequest {
            max_age_days: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert!(
        report
            .removed
            .iter()
            .all(|p| p.version_id != newest.version_id),
        "the active version is never a candidate"
    );
    let st = state(&root);
    assert_eq!(
        st["workers"]["keep"]["activeVersionId"].as_str().unwrap(),
        newest.version_id
    );
}

#[test]
fn dry_run_reports_candidates_without_writing() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "dry", "export default {v: 1}\n");
    local.deploy(&dir, &overrides(&[])).unwrap();
    let a_dir = worker_dir(&root, "dry", "export default {v: 2}\n");
    let a = local.deploy_preview(&a_dir, &overrides(&[])).unwrap();
    let b_dir = worker_dir(&root, "dry", "export default {v: 3}\n");
    local.deploy_preview(&b_dir, &overrides(&[])).unwrap();
    age_previews(&root, "dry");

    let before = std::fs::read(root.join("state/state.json")).unwrap();
    let revision = local.status().unwrap().revision;
    let report = local
        .prune_versions(&PruneRequest {
            dry_run: true,
            ..Default::default()
        })
        .unwrap();
    assert!(report.dry_run);
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].version_id, a.version_id);
    assert!(!report.changed);
    assert_eq!(report.revision, revision);
    assert_eq!(
        std::fs::read(root.join("state/state.json")).unwrap(),
        before,
        "dry run must not touch state"
    );
    assert!(object_exists(&root, &a.artifact_digest));
}

/// Content-addressed reuse: two versions of the SAME bundle (different
/// record identity via vars) share one artifact. Pruning one must not
/// delete the object while the other still references it.
#[test]
fn shared_artifact_survives_until_no_version_references_it() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "shared", "export default {v: 1}\n");
    local.deploy(&dir, &overrides(&[("P", "prod")])).unwrap();
    let one = local
        .deploy_preview(&dir, &overrides(&[("P", "one")]))
        .unwrap();
    let two = local
        .deploy_preview(&dir, &overrides(&[("P", "two")]))
        .unwrap();
    assert_ne!(one.version_id, two.version_id);
    assert_eq!(
        one.artifact_digest, two.artifact_digest,
        "same bundle bytes must share one content address"
    );

    age_previews(&root, "shared");
    let report = local
        .prune_versions(&PruneRequest {
            max_age_days: Some(1),
            max_count: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(report.removed.len(), 1);
    assert!(
        object_exists(&root, &one.artifact_digest),
        "the digest stays because a retained version references it"
    );

    // Cross-worker reference: another worker shipping the same bundle
    // keeps the same object alive, so pruning must recompute references
    // over ALL workers, not just the pruned one.
    let other_dir = worker_dir(&root, "shared-b", "export default {v: 1}\n");
    let other = local.deploy(&other_dir, &overrides(&[("P", "b")])).unwrap();
    assert_eq!(other.artifact_digest, one.artifact_digest);
    // Promote `two` so the worker has no prunable preview left, then
    // prune: the object must still exist for the other worker.
    local.promote("shared", &two.version_id).unwrap();
    let report = local
        .prune_versions(&PruneRequest {
            max_age_days: Some(1),
            max_count: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert!(report.removed.is_empty(), "{:?}", report.removed);
    assert!(object_exists(&root, &other.artifact_digest));
}

#[test]
fn deploy_runs_the_automatic_pass_and_can_be_disabled() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "auto", "export default {v: 1}\n");
    local.deploy(&dir, &overrides(&[])).unwrap();
    // Default count is 4: the 5th preview evicts the oldest automatically.
    let mut ids = Vec::new();
    for i in 0..5 {
        let r = local
            .deploy_preview(&dir, &overrides(&[("P", &format!("p{i}"))]))
            .unwrap();
        ids.push(r.version_id);
    }
    let st = state(&root);
    let previews: Vec<&str> = versions_of(&st, "auto")
        .iter()
        .filter(|v| v["status"] == "preview")
        .map(|v| v["versionId"].as_str().unwrap())
        .collect();
    assert_eq!(previews.len(), 4, "automatic pass bounds preview history");
    assert!(!previews.contains(&ids[0].as_str()), "oldest evicted");
    assert!(previews.contains(&ids[4].as_str()), "newest kept");
    let event = oplog(&root)
        .into_iter()
        .find(|e| e["op"] == "versions-prune" && e["fields"]["trigger"] == "deploy")
        .expect("automatic prune must be audited");
    assert_eq!(event["fields"]["removed"][0]["versionId"], ids[0]);

    // Disable automatic pruning: the same 5-deploy sequence keeps all
    // previews. A manual prune still works (explicit operator action).
    local
        .set_retention(&RetentionUpdate {
            enabled: Some(false),
            ..Default::default()
        })
        .unwrap();
    let dir2 = worker_dir(&root, "manual", "export default {v: 2}\n");
    local.deploy(&dir2, &overrides(&[])).unwrap();
    for i in 0..5 {
        local
            .deploy_preview(&dir2, &overrides(&[("P", &format!("m{i}"))]))
            .unwrap();
    }
    let st = state(&root);
    assert_eq!(
        versions_of(&st, "manual")
            .iter()
            .filter(|v| v["status"] == "preview")
            .count(),
        5,
        "automatic pruning is off"
    );
    let report = local
        .prune_versions(&PruneRequest {
            worker: Some("manual".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(report.removed.len(), 1, "manual prune still honors limits");
}

/// Re-previewing an existing (older) version refreshes its recency: it
/// becomes the newest preview, is projected by the desired document, and
/// survives the automatic retention pass instead of being pruned by the
/// very deploy that reported it.
#[test]
fn re_previewing_an_old_version_makes_it_the_newest_and_keeps_it() {
    let root = root();
    let local = init(&root);
    let prod = worker_dir(&root, "reprev", "export default {v: 0}\n");
    local.deploy(&prod, &overrides(&[])).unwrap();
    let a_dir = worker_dir(&root, "reprev", "export default {v: 1}\n");
    let a = local.deploy_preview(&a_dir, &overrides(&[])).unwrap();
    let b_dir = worker_dir(&root, "reprev", "export default {v: 2}\n");
    let b = local.deploy_preview(&b_dir, &overrides(&[])).unwrap();
    let c_dir = worker_dir(&root, "reprev", "export default {v: 3}\n");
    let c = local.deploy_preview(&c_dir, &overrides(&[])).unwrap();
    // Policy change alone prunes nothing; the next deploy applies it.
    local
        .set_retention(&RetentionUpdate {
            max_age_days: Some(0),
            max_count: Some(2),
            ..Default::default()
        })
        .unwrap();
    let a_created = state(&root)["workers"]["reprev"]["versions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["versionId"] == a.version_id)
        .unwrap()["createdAtUnixMs"]
        .as_u64()
        .unwrap();

    // A is the OLDEST preview. Re-previewing it must make it the newest.
    let again = local.deploy_preview(&a_dir, &overrides(&[])).unwrap();
    assert_eq!(again.version_id, a.version_id);
    let st = state(&root);
    let record = versions_of(&st, "reprev")
        .iter()
        .find(|v| v["versionId"] == a.version_id)
        .unwrap();
    assert_eq!(record["status"], "preview");
    assert!(
        record["createdAtUnixMs"].as_u64().unwrap() > a_created,
        "re-preview refreshes recency"
    );

    // Newest-first retention keeps A and C, drops B.
    let previews: Vec<&str> = versions_of(&st, "reprev")
        .iter()
        .filter(|v| v["status"] == "preview")
        .map(|v| v["versionId"].as_str().unwrap())
        .collect();
    assert!(previews.contains(&a.version_id.as_str()));
    assert!(previews.contains(&c.version_id.as_str()));
    assert!(!previews.contains(&b.version_id.as_str()));

    // The signed desired projects A as the newest preview, so the URL a
    // caller just received actually points at a served instance.
    let envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("desired/envelope.json")).unwrap()).unwrap();
    let projected: Vec<&str> = envelope["payload"]["tenants"][0]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|w| w.get("preview").is_some())
        .map(|w| w["activeVersion"]["versionId"].as_str().unwrap())
        .collect();
    assert_eq!(projected, vec![a.version_id.as_str(), c.version_id.as_str()]);
}

#[test]
fn policy_roundtrip_and_bounds_fail_closed() {
    let root = root();
    let local = init(&root);
    assert_eq!(local.retention().unwrap().max_age_days, 7);
    assert_eq!(local.retention().unwrap().max_count, 4);

    let stored = local
        .set_retention(&RetentionUpdate {
            max_age_days: Some(2),
            max_count: Some(3),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(stored.max_age_days, 2);
    assert_eq!(stored.max_count, 3);
    assert_eq!(local.retention().unwrap(), stored);

    // An enabled policy with no limits is refused (disable explicitly).
    assert!(matches!(
        local.set_retention(&RetentionUpdate {
            max_age_days: Some(0),
            max_count: Some(0),
            ..Default::default()
        }),
        Err(LocalError::State("retentionLimits"))
    ));
    // Out-of-bounds values are refused, not clamped.
    assert!(matches!(
        local.set_retention(&RetentionUpdate {
            max_count: Some(u32::MAX),
            ..Default::default()
        }),
        Err(LocalError::State("retentionMaxCount"))
    ));

    // A malformed policy file fails closed instead of guessing limits.
    std::fs::write(root.join("state/retention.json"), b"{not json").unwrap();
    assert!(local.retention().is_err());
    assert!(local.prune_versions(&PruneRequest::default()).is_err());
}

#[test]
fn concurrent_prune_and_promote_never_dangle() {
    for round in 0..6 {
        let root = root();
        let local = init(&root);
        let dir = worker_dir(&root, "race", "export default {v: 1}\n");
        let active = local.deploy(&dir, &overrides(&[])).unwrap();
        let mut previews = Vec::new();
        for i in 0..4 {
            previews.push(
                local
                    .deploy_preview(&dir, &overrides(&[("P", &format!("race{i}"))]))
                    .unwrap(),
            );
        }
        // The oldest preview is the one the count policy wants to drop.
        let target = previews[0].version_id.clone();
        let barrier = Barrier::new(2);
        let root_a = root.to_path_buf();
        let root_b = root.to_path_buf();
        let target_b = target.clone();
        let (pruned, promoted) = std::thread::scope(|scope| {
            let prune = scope.spawn(|| {
                barrier.wait();
                let request = PruneRequest {
                    max_count: Some(1),
                    max_age_days: Some(0),
                    ..Default::default()
                };
                // The peer may hold the writer lock for a whole
                // preflight; the library's bounded flock wait is
                // deliberately short, so a test retries instead of
                // treating "locked" as a product failure.
                for _ in 0..40 {
                    match VrtLocal::open(&root_a).unwrap().prune_versions(&request) {
                        Ok(result) => return result,
                        Err(LocalError::Locked) => {
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        Err(e) => panic!("prune failed: {e}"),
                    }
                }
                panic!("prune stayed locked");
            });
            let promote = scope.spawn(|| {
                barrier.wait();
                for _ in 0..40 {
                    match VrtLocal::open(&root_b)
                        .unwrap()
                        .promote("race", &target_b)
                    {
                        Err(LocalError::Locked) => {
                            std::thread::sleep(std::time::Duration::from_millis(25));
                        }
                        other => return other,
                    }
                }
                panic!("promote stayed locked");
            });
            (prune.join().unwrap(), promote.join().unwrap())
        });

        let st = state(&root);
        let stored: Vec<&str> = versions_of(&st, "race")
            .iter()
            .map(|v| v["versionId"].as_str().unwrap())
            .collect();
        let pointer = st["workers"]["race"]["activeVersionId"]
            .as_str()
            .unwrap()
            .to_string();
        // Whatever the order, the committed state is internally
        // consistent: the active pointer names a stored version and
        // every stored version's artifact exists.
        assert!(
            stored.contains(&pointer.as_str()),
            "round {round}: dangling pointer"
        );
        for v in versions_of(&st, "race") {
            let digest = v["artifactDigest"].as_str().unwrap();
            assert!(
                object_exists(&root, digest),
                "round {round}: stored version references a missing artifact"
            );
        }
        if promoted.is_ok() {
            assert_eq!(pointer, target, "round {round}: promote must win wholly");
            assert!(stored.contains(&target.as_str()));
            assert!(
                pruned.removed.iter().all(|p| p.version_id != target),
                "round {round}: a promoted version must not be reported as pruned"
            );
        } else {
            // Prune won: promote fails closed and the pointer is intact.
            // (`Locked` is a legitimate bounded-wait outcome too.)
            match &promoted {
                Err(LocalError::NotFound("version")) | Err(LocalError::Locked) => {}
                other => panic!("round {round}: unexpected promote outcome {other:?}"),
            }
            assert_eq!(pointer, active.version_id, "round {round}: pointer moved");
            assert!(!stored.contains(&target.as_str()));
        }
        // A promoted version is never in both states: exactly one
        // active record after both operations.
        let active_count = versions_of(&st, "race")
            .iter()
            .filter(|v| v["status"] == "active")
            .count();
        assert_eq!(active_count, 1, "round {round}");
        if let Some(record) = st["workers"]["race"]["versions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["versionId"] == target)
        {
            assert_eq!(record["status"], "active", "round {round}");
        }
    }
}

#[test]
fn cli_prune_and_retention() {
    use std::process::Command;
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "cli", "export default {v: 1}\n");
    local.deploy(&dir, &overrides(&[])).unwrap();
    let old_dir = worker_dir(&root, "cli", "export default {v: 2}\n");
    let old = local.deploy_preview(&old_dir, &overrides(&[])).unwrap();
    let new_dir = worker_dir(&root, "cli", "export default {v: 3}\n");
    local.deploy_preview(&new_dir, &overrides(&[])).unwrap();
    age_previews(&root, "cli");

    let exe = env!("CARGO_BIN_EXE_of");
    let out = Command::new(exe)
        .arg("versions")
        .arg("prune")
        .arg("--root")
        .arg(root.as_os_str())
        .arg("--dry-run")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["dryRun"], true);
    assert_eq!(json["removed"][0]["versionId"], old.version_id);

    let out = Command::new(exe)
        .arg("versions")
        .arg("prune")
        .arg("--root")
        .arg(root.as_os_str())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["changed"], true);
    assert_eq!(json["removed"][0]["versionId"], old.version_id);

    let out = Command::new(exe)
        .arg("versions")
        .arg("retention")
        .arg("--root")
        .arg(root.as_os_str())
        .arg("--no-auto")
        .arg("--max-count")
        .arg("2")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["enabled"], false);
    assert_eq!(json["maxCount"], 2);
    assert_eq!(local.retention().unwrap().max_count, 2);
    assert!(!local.retention().unwrap().enabled);
    assert_eq!(
        local.versions("cli").unwrap().versions.iter().filter(|v| v.status == VersionStatus::Preview).count(),
        1,
        "the remaining preview is still stored"
    );
}
