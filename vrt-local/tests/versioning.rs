//! Versioning MVP: `deploy --preview`, `promote`, `versions list`.
//!
//! The tests exercise real files and the signed desired document:
//! preview must not move `activeVersionId`, promote must switch it
//! atomically through the existing rollback-style pointer path, and
//! repeated promotes must be idempotent.

use std::path::{Path, PathBuf};
use vrt_local::{DeployOverrides, InitParams, LocalError, VersionStatus, VrtLocal};

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
        "vrt-versioning-{}-{}",
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
    let dir = root.join(format!("worker-{name}"));
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

fn desired_active(root: &Path, worker: &str) -> Option<String> {
    let raw = std::fs::read(root.join("desired/envelope.json")).ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    doc["payload"]["tenants"][0]["workers"]
        .as_array()?
        .iter()
        .find(|w| w["name"] == worker)
        .map(|w| {
            w["activeVersion"]["versionId"]
                .as_str()
                .unwrap()
                .to_string()
        })
}

fn desired_worker_count(root: &Path) -> usize {
    let raw = std::fs::read(root.join("desired/envelope.json")).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    doc["payload"]["tenants"][0]["workers"]
        .as_array()
        .unwrap()
        .len()
}

fn raw_state(root: &Path) -> serde_json::Value {
    let raw = std::fs::read(root.join("state/state.json")).unwrap();
    serde_json::from_slice(&raw).unwrap()
}

#[test]
fn preview_stores_version_without_moving_traffic() {
    let root = root();
    let local = init(&root);
    let v1 = worker_dir(&root, "svc", "export default {v: 1}\n");
    let r1 = local.deploy(&v1, &DeployOverrides::default()).unwrap();
    {
        let st = local.status().unwrap();
        let active = st.workers[0].active_version_id.clone().unwrap();
        assert_eq!(r1.version_id, active);
    }

    let v2 = worker_dir(&root, "svc", "export default {v: 2}\n");
    let p2 = local
        .deploy_preview(&v2, &DeployOverrides::default())
        .unwrap();
    assert_ne!(p2.version_id, r1.version_id);

    // Active pointer and signed desired are untouched by preview.
    assert_eq!(
        local.status().unwrap().workers[0]
            .active_version_id
            .as_deref(),
        Some(r1.version_id.as_str())
    );
    assert_eq!(
        desired_active(&root, "svc").as_deref(),
        Some(r1.version_id.as_str())
    );

    let list = local.versions("svc").unwrap();
    assert_eq!(list.versions.len(), 2);
    let active = list
        .versions
        .iter()
        .find(|v| v.version_id == r1.version_id)
        .unwrap();
    assert_eq!(active.status, VersionStatus::Active);
    assert!(active.active);
    let preview = list
        .versions
        .iter()
        .find(|v| v.version_id == p2.version_id)
        .unwrap();
    assert_eq!(preview.status, VersionStatus::Preview);
    assert!(!preview.active);
    assert!(preview.created_at_unix_ms > 0);
    assert!(preview.config_hash.starts_with("cfg_"));

    // Deploying the same bytes mints the SAME version id and activates
    // it; content identity is independent of preview/promote state.
    let r2 = local.deploy(&v2, &DeployOverrides::default()).unwrap();
    assert_eq!(r2.version_id, p2.version_id);
    assert_eq!(
        desired_active(&root, "svc").as_deref(),
        Some(r2.version_id.as_str())
    );
}

#[test]
fn preview_first_worker_is_absent_from_desired_until_promoted() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "fresh", "export default {fresh: true}\n");

    let preview = local
        .deploy_preview(&dir, &DeployOverrides::default())
        .unwrap();
    assert_eq!(
        desired_worker_count(&root),
        0,
        "nothing may serve a preview"
    );
    assert!(local.status().unwrap().workers[0]
        .active_version_id
        .is_none());
    assert_eq!(
        local.versions("fresh").unwrap().versions[0].status,
        VersionStatus::Preview
    );

    // Global version-id resolution works when exactly one worker owns
    // the id; the promoted version becomes active and enters desired.
    let promoted = local.promote_version(&preview.version_id, None).unwrap();
    assert_eq!(promoted.worker, "fresh");
    assert!(promoted.changed);
    assert_eq!(desired_worker_count(&root), 1);
    assert_eq!(
        desired_active(&root, "fresh").as_deref(),
        Some(preview.version_id.as_str())
    );
    let list = local.versions("fresh").unwrap();
    assert_eq!(list.versions[0].status, VersionStatus::Active);
    assert!(list.versions[0].active);

    // Repeated promote is a no-op: same revision, no new publish.
    let again = local.promote_version(&preview.version_id, None).unwrap();
    assert!(!again.changed);
    assert_eq!(again.revision, promoted.revision);
}

#[test]
fn cross_worker_promote_and_missing_artifact_fail_closed() {
    let root = root();
    let local = init(&root);
    let a = worker_dir(&root, "a", "export default {a: 1}\n");
    let b = worker_dir(&root, "b", "export default {b: 1}\n");
    local.deploy(&a, &DeployOverrides::default()).unwrap();
    local.deploy(&b, &DeployOverrides::default()).unwrap();

    let b_active = local
        .status()
        .unwrap()
        .workers
        .into_iter()
        .find(|w| w.name == "b")
        .unwrap()
        .active_version_id
        .unwrap();
    assert!(matches!(
        local.promote("a", &b_active),
        Err(LocalError::NotFound("version"))
    ));

    // A preview whose object was deleted must not be promotable.
    let a2 = worker_dir(&root, "a", "export default {a: 2}\n");
    let preview = local
        .deploy_preview(&a2, &DeployOverrides::default())
        .unwrap();
    std::fs::remove_file(root.join("objects").join(&preview.artifact_digest)).unwrap();
    assert!(matches!(
        local.promote("a", &preview.version_id),
        Err(LocalError::NotFound("artifact"))
    ));
    // The old active version keeps serving.
    assert!(desired_active(&root, "a").is_some());
}

#[test]
fn promote_repairs_a_stale_envelope_after_a_crash() {
    let root = root();
    let local = init(&root);
    let v1 = worker_dir(&root, "repair", "export default {v: 1}\n");
    local.deploy(&v1, &DeployOverrides::default()).unwrap();
    let v2 = worker_dir(&root, "repair", "export default {v: 2}\n");
    let preview = local
        .deploy_preview(&v2, &DeployOverrides::default())
        .unwrap();
    let promoted = local.promote_version(&preview.version_id, None).unwrap();
    assert!(promoted.changed);

    // Simulate the crash window: state already points at the new
    // version, but the signed envelope is stale/missing.
    std::fs::write(root.join("desired/envelope.json"), b"{\"revision\":0}").unwrap();
    let replay = local.promote_version(&preview.version_id, None).unwrap();
    assert!(!replay.changed, "state was already switched");
    assert_eq!(replay.revision, promoted.revision);
    assert_eq!(
        desired_active(&root, "repair").as_deref(),
        Some(preview.version_id.as_str()),
        "replay republished a valid desired document"
    );
}

#[test]
fn cli_preview_promote_and_versions_list() {
    use std::process::Command;

    let root = root();
    let _local = init(&root);
    let dir = worker_dir(&root, "cli", "export default {cli: true}\n");
    let exe = env!("CARGO_BIN_EXE_vrt");

    let out = Command::new(exe)
        .arg("deploy")
        .arg("--preview")
        .arg("--root")
        .arg(root.as_os_str())
        .arg(&dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "preview CLI failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let deployed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let version_id = deployed["versionId"].as_str().unwrap().to_string();

    let out = Command::new(exe)
        .arg("versions")
        .arg("list")
        .arg("--root")
        .arg(root.as_os_str())
        .arg("cli")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let listed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(listed["activeVersionId"], serde_json::Value::Null);
    assert_eq!(listed["versions"][0]["versionId"], version_id);
    assert_eq!(listed["versions"][0]["status"], "preview");

    let out = Command::new(exe)
        .arg("promote")
        .arg("--root")
        .arg(root.as_os_str())
        .arg(&version_id)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "promote CLI failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let promoted: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(promoted["versionId"], version_id);
    assert_eq!(promoted["changed"], true);
    assert_eq!(
        desired_active(&root, "cli").as_deref(),
        Some(version_id.as_str())
    );
}

#[test]
fn concurrent_promotes_never_produce_two_active_versions() {
    use std::sync::{Arc, Barrier};

    let root = root();
    let local = init(&root);
    let v1 = worker_dir(&root, "race", "export default {v: 1}\n");
    let r1 = local.deploy(&v1, &DeployOverrides::default()).unwrap();
    let v2 = worker_dir(&root, "race", "export default {v: 2}\n");
    let r2 = local
        .deploy_preview(&v2, &DeployOverrides::default())
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for id in [r1.version_id.clone(), r2.version_id.clone()] {
        let path = root.to_path_buf();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let local = VrtLocal::open(&path).unwrap();
            barrier.wait();
            // The writer lock serializes the two promotes. A bounded
            // 500ms wait may reject one under extreme load; that is a
            // typed `Locked`, never a second active pointer.
            local.promote("race", &id)
        }));
    }
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(
        results.iter().any(|r| r.is_ok()),
        "at least one promote must commit: {results:?}"
    );

    let st = raw_state(&root);
    let versions = st["workers"]["race"]["versions"].as_array().unwrap();
    let active_statuses: Vec<_> = versions
        .iter()
        .filter(|v| v["status"] == "active")
        .collect();
    assert_eq!(active_statuses.len(), 1, "exactly one active version");
    let active_id = st["workers"]["race"]["activeVersionId"].as_str().unwrap();
    assert_eq!(active_statuses[0]["versionId"], active_id);
    assert_eq!(desired_active(&root, "race").as_deref(), Some(active_id));
}
