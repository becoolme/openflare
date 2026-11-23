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
fn preview_first_worker_is_absent_from_desired_until_promoted() {    let root = root();
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
    let exe = env!("CARGO_BIN_EXE_of");

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

// ── PREVIEW URLs ─────────────────────────────────────────────────────

/// `previewUrl` is derived from the HOST config, never guessed, and it
/// is SIGNED: previews require a token, so a domain without a token
/// secret is a closed domain and reports null.
#[test]
fn preview_url_reflects_the_host_preview_config() {
    let root = root();
    let local = init(&root);
    let v1 = worker_dir(&root, "urls", "export default {v: 1}\n");
    let r1 = local.deploy(&v1, &DeployOverrides::default()).unwrap();
    // A production deploy never carries a preview URL.
    assert_eq!(r1.preview_url, None);

    let v2 = worker_dir(&root, "urls", "export default {v: 2}\n");
    // No host config at all → preview is off.
    let no_domain = local
        .deploy_preview(&v2, &DeployOverrides::default())
        .unwrap();
    assert_eq!(no_domain.preview_url, None);
    assert_eq!(
        local
            .versions("urls")
            .unwrap()
            .versions
            .iter()
            .find(|v| v.version_id == no_domain.version_id)
            .unwrap()
            .preview_url,
        None
    );

    // A domain WITHOUT `tokenSecretFile` keeps the domain closed: the
    // host installs no preview route, so no URL may be advertised.
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::write(
        root.join("config/vrt-host.toml"),
        "cellId = \"local\"\n\n[preview]\ndomain = \"Preview.Example.COM\"\n",
    )
    .unwrap();
    assert_eq!(
        vrt_local::preview::preview_domain(&root).as_deref(),
        Some("preview.example.com"),
        "case is normalized"
    );
    let v3 = worker_dir(&root, "urls", "export default {v: 3}\n");
    let closed = local
        .deploy_preview(&v3, &DeployOverrides::default())
        .unwrap();
    assert_eq!(
        closed.preview_url, None,
        "a domain without a token secret is closed"
    );

    // Configure the SAME knobs vrt-host routes and verifies with.
    let key_path = root.join("keys/preview-token.secret");
    std::fs::write(
        root.join("config/vrt-host.toml"),
        format!(
            "cellId = \"local\"\n\n[preview]\ndomain = \"Preview.Example.COM\"\n\
             tokenSecretFile = \"{}\"\n",
            key_path.display()
        ),
    )
    .unwrap();
    let v4 = worker_dir(&root, "urls", "export default {v: 4}\n");
    let p4 = local
        .deploy_preview(&v4, &DeployOverrides::default())
        .unwrap();
    let prefix = format!(
        "https://{}--urls.preview.example.com/?__vrt_preview_token=v1.",
        p4.version_id
    );
    let url = p4.preview_url.as_deref().expect("signed preview URL");
    assert!(url.starts_with(&prefix), "{url}");
    let expires = p4.preview_url_expires_at_unix_s.expect("expiry reported");
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    assert!(expires > now && expires <= now + 3600 + 5, "{expires} vs {now}");
    // The token verifies for this (version, worker) and nothing else.
    let key = snapshot_protocol::preview_token::PreviewTokenKey::from_hex(
        &std::fs::read_to_string(&key_path).unwrap(),
    )
    .unwrap();
    let token = url.split_once("__vrt_preview_token=").unwrap().1;
    let wid = local.versions("urls").unwrap().worker_id;
    assert_eq!(key.verify(&p4.version_id, &wid, token, now), Ok(()));
    assert!(
        key.verify(&closed.version_id, &wid, token, now).is_err(),
        "a token is bound to its version"
    );

    // Active versions keep a preview URL (the host serves them), archived
    // versions lose it.
    local.promote("urls", &p4.version_id).unwrap();
    let list = local.versions("urls").unwrap();
    let row = |id: &str| {
        list.versions
            .iter()
            .find(|v| v.version_id == id)
            .unwrap_or_else(|| panic!("version {id} missing"))
    };
    assert_eq!(row(&p4.version_id).status, VersionStatus::Active);
    assert!(row(&p4.version_id).preview_url.is_some());
    assert!(row(&p4.version_id).preview_url_expires_at_unix_s.is_some());
    // Earlier previews stay previews and become routable now that both
    // knobs are configured.
    assert_eq!(row(&no_domain.version_id).status, VersionStatus::Preview);
    assert!(row(&no_domain.version_id).preview_url.is_some());
    assert!(row(&closed.version_id).preview_url.is_some());
    // The version that was ACTIVE and got replaced loses its URL: it is
    // archived, and the host has no route for it.
    assert_eq!(row(&r1.version_id).status, VersionStatus::Archived);
    assert_eq!(row(&r1.version_id).preview_url, None);
    assert_eq!(row(&r1.version_id).preview_url_expires_at_unix_s, None);
}

/// `of versions share` mints a scoped, expiring URL for one version; it
/// never changes state and refuses an unusable TTL or a closed domain.
#[test]
fn share_version_mints_a_scoped_expiring_url() {
    let root = root();
    let local = init(&root);
    let v1 = worker_dir(&root, "share", "export default {v: 1}\n");
    let first = local.deploy(&v1, &DeployOverrides::default()).unwrap();
    let v2 = worker_dir(&root, "share", "export default {v: 2}\n");
    let preview = local
        .deploy_preview(&v2, &DeployOverrides::default())
        .unwrap();

    // No preview config → a loud failure (not a URL that 404s).
    assert!(matches!(
        local.share_version(&preview.version_id, None, 3600),
        Err(LocalError::Preview("previewAccess"))
    ));

    let key_path = root.join("keys/preview-token.secret");
    std::fs::write(
        root.join("config/vrt-host.toml"),
        format!(
            "cellId = \"local\"\n\n[preview]\ndomain = \"preview.example.com\"\n\
             tokenSecretFile = \"{}\"\n",
            key_path.display()
        ),
    )
    .unwrap();

    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let shared = local
        .share_version(&preview.version_id, None, 600)
        .unwrap();
    assert_eq!(shared.worker, "share");
    assert!(shared.url.contains(&preview.version_id));
    let expires = shared.expires_at_unix_s;
    assert!(expires >= now + 600 && expires <= now + 605, "{expires}");
    let token = shared
        .url
        .split_once("__vrt_preview_token=")
        .unwrap()
        .1;
    let key = snapshot_protocol::preview_token::PreviewTokenKey::from_hex(
        &std::fs::read_to_string(&key_path).unwrap(),
    )
    .unwrap();
    assert_eq!(
        key.verify(&preview.version_id, &shared.worker_id, token, now),
        Ok(())
    );
    assert!(key
        .verify(&preview.version_id, "w_other", token, now)
        .is_err());

    // CLI wiring: `of versions share` prints the same JSON shape.
    let exe = env!("CARGO_BIN_EXE_of");
    let out = std::process::Command::new(exe)
        .arg("versions")
        .arg("share")
        .arg("--root")
        .arg(root.as_os_str())
        .arg("--ttl-seconds")
        .arg("600")
        .arg(&preview.version_id)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "share CLI failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let cli: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(cli["workerId"], shared.worker_id);
    assert!(cli["url"]
        .as_str()
        .unwrap()
        .contains("__vrt_preview_token=v1."));

    // TTL is bounded, not clamped silently.
    assert!(matches!(
        local.share_version(&preview.version_id, None, 1),
        Err(LocalError::State("previewTtl"))
    ));
    assert!(matches!(
        local.share_version(&preview.version_id, None, 40 * 24 * 3600),
        Err(LocalError::State("previewTtl"))
    ));
    // Unknown version fails closed.
    assert!(matches!(
        local.share_version("vrt_missing", None, 600),
        Err(LocalError::NotFound("version"))
    ));

    // Archived versions have no route: refuse instead of a dead link.
    // (The first deploy was replaced by a promote, so it is archived now;
    // other previews stay previewable and keep their URLs.)
    let v3 = worker_dir(&root, "share", "export default {v: 3}\n");
    let third = local
        .deploy_preview(&v3, &DeployOverrides::default())
        .unwrap();
    local.promote("share", &third.version_id).unwrap();
    assert_eq!(
        local
            .versions("share")
            .unwrap()
            .versions
            .iter()
            .find(|v| v.version_id == first.version_id)
            .unwrap()
            .status,
        VersionStatus::Archived
    );
    assert!(matches!(
        local.share_version(&first.version_id, None, 600),
        Err(LocalError::Preview("versionNotRoutable"))
    ));

    // A preview-first worker is skipped by the desired document, so the
    // host projects no preview instance: no URL, and share refuses.
    let fresh = worker_dir(&root, "fresh", "export default {v: 1}\n");
    let fresh_preview = local
        .deploy_preview(&fresh, &DeployOverrides::default())
        .unwrap();
    assert_eq!(fresh_preview.preview_url, None);
    assert_eq!(fresh_preview.preview_url_expires_at_unix_s, None);
    assert!(matches!(
        local.share_version(&fresh_preview.version_id, Some("fresh"), 600),
        Err(LocalError::Preview("workerNotServing"))
    ));
}

/// Durable objects and workflows are not version-isolated yet: a preview
/// that declares them would share/duplicate production state, so the
/// preview is refused BEFORE anything is persisted.
#[test]
fn preview_refuses_durable_objects_and_workflows() {
    let root = root();
    let local = init(&root);
    let base = worker_dir(&root, "guarded", "export default {}\n");
    local.deploy(&base, &DeployOverrides::default()).unwrap();
    let rev_before = local.status().unwrap().revision;

    // Durable object declaration.
    let do_dir = root.join("worker-do");
    std::fs::create_dir_all(&do_dir).unwrap();
    std::fs::write(
        do_dir.join("vrt.toml"),
        "name = \"guarded\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\n\
         [[durableObjectBindings]]\nname = \"COUNTER\"\nclassName = \"Counter\"\n\n\
         [[durableObjectMigrations]]\ntag = \"v1\"\nnewSqliteClasses = [\"Counter\"]\n",
    )
    .unwrap();
    std::fs::write(do_dir.join("main.js"), "export class Counter {}\n").unwrap();
    let err = local
        .deploy_preview(&do_dir, &DeployOverrides::default())
        .expect_err("DO preview must fail closed");
    assert!(
        err.to_string().contains("durableObjects"),
        "unexpected error: {err}"
    );

    // Workflow declaration.
    let wf_dir = root.join("worker-wf");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("vrt.toml"),
        "name = \"guarded\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\n\
         [[workflows]]\nname = \"order\"\nbinding = \"ORDER\"\nclassName = \"OrderWorkflow\"\n",
    )
    .unwrap();
    std::fs::write(
        wf_dir.join("main.js"),
        "import { WorkflowEntrypoint } from \"cloudflare:workers\";\n         export class OrderWorkflow extends WorkflowEntrypoint {}\n         export default {}\n",
    )
    .unwrap();
    let err = local
        .deploy_preview(&wf_dir, &DeployOverrides::default())
        .expect_err("workflow preview must fail closed");
    assert!(
        err.to_string().contains("workflows"),
        "unexpected error: {err}"
    );

    // Nothing was persisted and the active pointer never moved.
    let st = local.status().unwrap();
    assert_eq!(st.revision, rev_before, "a refused preview writes nothing");
    assert_eq!(st.workers[0].versions.len(), 1);
    assert!(st.workers[0].active_version_id.is_some());
}
