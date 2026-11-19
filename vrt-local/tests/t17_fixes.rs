//! T17 Part-1 regression tests — F1 secret/var hot reload inputs,
//! F2 per-version vars, F3 no-persistence-before-validation, F4
//! version eviction + object GC, F5 atomic init, F6 layout checks.

use std::path::{Path, PathBuf};
use vrt_local::{DeployOverrides, InitParams, LocalError, VrtLocal};

/// T37b: drop guard — unique top-level dir per test; parallel tests
/// never share a parent on cleanup.
/// `path` is what tests see; `cleanup` is the unique parent removed on
/// drop (init staging lands in path's parent — must be per-test).
struct TmpDir {
    cleanup: PathBuf,
    path: PathBuf,
}

impl std::ops::Deref for TmpDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for TmpDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.cleanup) {
            eprintln!("tmp cleanup {}: {e}", self.cleanup.display());
        }
    }
}

fn root() -> TmpDir {
    // Staging dirs live in root's PARENT — it must be unique per
    // test or parallel inits' staging leaks into each other's view.
    // The guard owns the unique parent; `root` itself is the child.
    let parent = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-local-t17-{}-{}",
        std::process::id(),
        uuidish()
    ));
    let root = parent.join("root");
    std::fs::create_dir_all(&root).unwrap();
    TmpDir {
        cleanup: parent,
        path: root,
    }
}

fn uuidish() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{nanos:x}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn init(root: &Path) -> VrtLocal {
    init_with(root, None)
}

fn init_with(root: &Path, max_versions: Option<u32>) -> VrtLocal {
    VrtLocal::init(
        root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: max_versions,
        },
    )
    .unwrap();
    VrtLocal::open(root).unwrap()
}

fn worker_dir(root: &Path, name: &str, code: &str, vars: &[(&str, &str)]) -> PathBuf {
    let dir = root.join(format!("w-{name}-{}", uuidish()));
    std::fs::create_dir_all(&dir).unwrap();
    let vars_toml: String = vars
        .iter()
        .map(|(k, v)| format!("{k} = \"{v}\"\n"))
        .collect();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n[vars]\n{vars_toml}"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), code).unwrap();
    dir
}

fn state_json(root: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(root.join("state/state.json")).unwrap()).unwrap()
}

fn vars_json(root: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(root.join("vars/vars.json")).unwrap()).unwrap()
}

// ---------- F1: secret ops publish a new revision ----------

#[test]
fn secret_ops_publish_new_revision() {
    let root = root();
    let local = init(&root);
    let wdir = worker_dir(&root, "api", "export default {}", &[]);
    let r1 = local.deploy(&wdir, &DeployOverrides::default()).unwrap();
    // T25/L9: revision 1 is the init-published empty envelope.
    assert_eq!(r1.revision, 2);

    local.put_secret("local", "api-key", "s3cr3t").unwrap();
    let st = local.status().unwrap();
    assert_eq!(st.revision, 3, "put_secret bumps + publishes");
    assert_eq!(st.published_revision, Some(3));

    local.delete_secret("local", "api-key").unwrap();
    let st = local.status().unwrap();
    assert_eq!(st.revision, 4);
    assert_eq!(st.published_revision, Some(4));
}

// ---------- F2: vars are version-scoped ----------

#[test]
fn vars_are_part_of_version_identity() {
    let root = root();
    let local = init(&root);
    let v1 = worker_dir(&root, "api", "export default {v:1}", &[("LOG", "a")]);
    let r1 = local.deploy(&v1, &DeployOverrides::default()).unwrap();

    // Var-only change (same code): MUST produce a new version (F2 —
    // before the fix this silently reactivated the old version).
    let v2 = worker_dir(&root, "api", "export default {v:1}", &[("LOG", "b")]);
    let r2 = local.deploy(&v2, &DeployOverrides::default()).unwrap();
    assert_ne!(r1.version_id, r2.version_id, "var change = new version");
    assert_eq!(r1.artifact_digest, r2.artifact_digest, "same code");

    // Same code + same vars → identical version (dedup still works).
    let v1b = worker_dir(&root, "api", "export default {v:1}", &[("LOG", "a")]);
    let r1b = local.deploy(&v1b, &DeployOverrides::default()).unwrap();
    assert_eq!(r1b.version_id, r1.version_id);

    // Rollback restores THAT version's vars — both var sets coexist
    // in vars.json under content-scoped refs (union, F2).
    local.rollback("api", &r1.version_id).unwrap();
    let vars = vars_json(&root);
    let obj = vars.as_object().unwrap();
    assert_eq!(obj.len(), 2, "one ref per distinct var set");
    let mut values: Vec<_> = obj.values().map(|v| v.as_str().unwrap()).collect();
    values.sort();
    assert_eq!(values, ["a", "b"]);

    // Active version's desired resolves to the OLD var refs — verify
    // the state record carries per-version var values.
    let st = state_json(&root);
    let rec = &st["workers"]["api"];
    let active = rec["activeVersionId"].as_str().unwrap();
    let versions = rec["versions"].as_array().unwrap();
    let active_rec = versions
        .iter()
        .find(|v| v["versionId"].as_str().unwrap() == active)
        .unwrap();
    let var_values = active_rec["varValues"].as_object().unwrap();
    assert!(
        var_values.values().any(|v| v == "a"),
        "rollback restores v1 vars"
    );
}

// ---------- F3: no persistence before validation ----------

#[test]
fn failed_validation_persists_nothing() {
    let root = root();
    let local = init(&root);
    let w1 = worker_dir(&root, "api", "export default {v:1}", &[("LOG", "a")]);
    local.deploy(&w1, &DeployOverrides::default()).unwrap();
    let vars_before = std::fs::read(root.join("vars/vars.json")).unwrap();
    let state_before = std::fs::read(root.join("state/state.json")).unwrap();
    let rev_before = local.status().unwrap().revision;

    // A worker dir whose desired doc can't validate: `name` collides
    // with an existing worker but with a clashing hostname would also
    // do — simplest deterministic failure is an invalid hostname set
    // via overrides (desired validation rejects it, pack accepts).
    let w2 = worker_dir(&root, "api2", "export default {v:2}", &[("LOG", "z")]);
    let bad = DeployOverrides {
        name: None,
        bindings: vec![],
        hostnames: vec!["invalid_hostname!".to_string()],
    };
    let err = local.deploy(&w2, &bad);
    assert!(err.is_err(), "invalid desired must fail");

    // Byte-identical vars.json + state.json, no new object, revision
    // untouched — the failed op persisted NOTHING (F3).
    assert_eq!(
        std::fs::read(root.join("vars/vars.json")).unwrap(),
        vars_before
    );
    assert_eq!(
        std::fs::read(root.join("state/state.json")).unwrap(),
        state_before
    );
    assert_eq!(local.status().unwrap().revision, rev_before);
    // Retry with valid input still works (no poisoned state).
    let r = local.deploy(&w2, &DeployOverrides::default()).unwrap();
    assert_eq!(r.revision, rev_before + 1);
}

// ---------- F4: version eviction + object GC ----------

#[test]
fn version_cap_evicts_oldest_non_active_and_gcs_objects() {
    let root = root();
    let local = init_with(&root, Some(3));
    let mut ids = Vec::new();
    let mut digests = Vec::new();
    for i in 0..4u8 {
        let w = worker_dir(
            &root,
            "api",
            &format!("export default {{v:{i}}}"),
            &[("LOG", &format!("v{i}"))],
        );
        let r = local.deploy(&w, &DeployOverrides::default()).unwrap();
        ids.push(r.version_id);
        digests.push(r.artifact_digest);
    }
    let st = local.status().unwrap();
    let rec = st.workers.iter().find(|w| w.name == "api").unwrap();
    assert_eq!(rec.versions.len(), 3, "cap enforced by eviction, not error");
    assert!(!rec.versions.contains(&ids[0]), "oldest evicted");
    assert_eq!(rec.active_version_id.as_deref(), Some(ids[3].as_str()));

    // Evicted version's bundle object is GC'd (F4).
    assert!(
        !root.join("objects").join(&digests[0]).exists(),
        "unreferenced object removed"
    );
    assert!(root.join("objects").join(&digests[3]).exists());

    // Evicted version's var refs leave vars.json; the union holds
    // only the three retained versions.
    let vars = vars_json(&root);
    let obj = vars.as_object().unwrap();
    assert_eq!(obj.len(), 3);
    let mut values: Vec<_> = obj
        .values()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    values.sort();
    assert_eq!(values, ["v1", "v2", "v3"]);

    // Rollback to an evicted version fails closed.
    assert!(matches!(
        local.rollback("api", &ids[0]),
        Err(LocalError::NotFound("version"))
    ));
    // Deploys keep working — the cap no longer bricks the worker.
    let w = worker_dir(&root, "api", "export default {v:9}", &[("LOG", "v9")]);
    assert!(local.deploy(&w, &DeployOverrides::default()).is_ok());
}

// ---------- F5: atomic init ----------

#[test]
fn init_is_atomic_and_retries_after_crash() {
    let root = root();
    let _ = std::fs::remove_dir(&root); // absent root is the norm
                                        // T25/L1: staging lives INSIDE root (`root/.init-tmp-<pid>`) so a
                                        // mount-point root is never removed/renamed — a "crashed" earlier
                                        // init leaves residue there, which the next attempt must clean.
    std::fs::create_dir_all(&root).unwrap();
    let stale = root.join(".init-tmp-9999");
    std::fs::create_dir_all(stale.join("keys")).unwrap();
    std::fs::write(stale.join("keys/junk"), b"x").unwrap();
    let local = init(&root);
    assert!(local.status().is_ok());
    assert!(!stale.exists(), "stale staging cleaned");
    // No staging leftovers inside the committed root.
    let leftovers: Vec<_> = std::fs::read_dir(&root)
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.contains(".init-tmp-"))
        })
        .collect();
    assert!(leftovers.is_empty());

    // Non-empty root refuses AND leaves no staging behind.
    let r2 = root.parent().unwrap().join("occupied");
    std::fs::create_dir_all(&r2).unwrap();
    std::fs::write(r2.join("other.txt"), b"data").unwrap();
    assert!(matches!(
        VrtLocal::init(
            &r2,
            &InitParams {
                cell_id: "local".into(),
                tenant_id: "local".into(),
                facade_manifest: None,
                facade_dist_root: None,
                max_versions_per_worker: None,
            },
        ),
        Err(LocalError::Layout(_))
    ));
    assert!(r2.join("other.txt").exists(), "foreign content untouched");
    let leftovers: Vec<_> = std::fs::read_dir(root.parent().unwrap())
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.contains(".init-tmp-"))
        })
        .collect();
    assert!(leftovers.is_empty(), "refused init leaves no staging");
}

// ---------- F6: layout::check covers the root itself ----------

#[test]
fn open_rejects_insecure_root_dir() {
    use std::os::unix::fs::PermissionsExt;
    let root = root();
    let _local = init(&root);
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        VrtLocal::open(&root).is_err(),
        "root itself must be 0700 — F6"
    );
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(VrtLocal::open(&root).is_ok());
}

// ---- T18 D5/D7: mode detection + reconfigure ----

#[test]
fn init_rejects_stub_mode_with_real_workerd() {
    let root = root().join("d5-mismatch");
    std::fs::create_dir_all(&root).unwrap();
    let fake = root.join("fake-workerd");
    std::fs::write(&fake, "#!/bin/sh\necho 'workerd 2025.1.0'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let exe = env!("CARGO_BIN_EXE_vrt");
    let out = std::process::Command::new(exe)
        .args(["init", "--root"])
        .arg(&root)
        .args(["--mode", "stub", "--workerd"])
        .arg(&fake)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("not vrt-stub-workerd"), "got: {err}");
}

#[test]
fn init_rejects_missing_workerd_explicit_and_default() {
    let exe = env!("CARGO_BIN_EXE_vrt");
    // T34f: spawns real `vrt init` — deep CARGO_TARGET_TMPDIR roots
    // now fail the socket-path budget before reaching the workerd
    // check; a short root keeps the assertion about `--workerd`.
    let root_guard = match vrt_securefs::ShortRoot::new("vrt17k") {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            return;
        }
        Err(e) => panic!("short root: {e}"),
    };
    let root = root_guard.join("k2-missing-workerd");
    std::fs::create_dir_all(&root).unwrap();
    // Explicit --workerd pointing at a nonexistent path → fail closed,
    // naming the path (not a silent capnp config).
    let out = std::process::Command::new(exe)
        .args(["init", "--root"])
        .arg(&root)
        .args(["--workerd", "/nonexistent/definitely-missing-workerd"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("definitely-missing-workerd"), "got: {err}");
    assert!(!root.join("keys/trusted.json").exists(), "no partial init");
    // No --workerd at all → the capnp default /usr/local/bin/workerd is
    // absent on the test host → also fails closed.
    let out2 = std::process::Command::new(exe)
        .args(["init", "--root"])
        .arg(&root.join("b"))
        .output()
        .unwrap();
    if !std::path::Path::new("/usr/local/bin/workerd").exists() {
        assert!(!out2.status.success(), "default path missing must fail");
        let err2 = String::from_utf8_lossy(&out2.stderr);
        assert!(err2.contains("/usr/local/bin/workerd"), "got: {err2}");
    }
}

#[test]
fn init_reconfigure_rejects_on_drift_and_rewrites() {
    let exe = env!("CARGO_BIN_EXE_vrt");
    // T34e/T34f: `vrt init` refuses roots whose derived unix socket
    // path exceeds the AF_UNIX budget — this test spawns the real
    // binary, so its root must FIT the budget: the shared helper
    // gives /tmp/<p>-<pid>-<seq> (~30B; macOS temp_dir alone is ~50B
    // and CARGO_TARGET_TMPDIR ~150B — both proven too long).
    let root = match vrt_securefs::ShortRoot::new("vrt17d") {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            return;
        }
        Err(e) => panic!("short root: {e}"),
    };
    // T24/K2: init now requires a resolvable workerd — point it at a
    // harmless fake so these invocations exercise config semantics.
    // (outside `root` — init requires an EMPTY root dir)
    let fake = root.parent().unwrap().join("d7-fake-workerd");
    std::fs::write(&fake, "#!/bin/sh\necho 'workerd 2026.1.0'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let fake_s = fake.to_str().unwrap().to_string();
    let run = |args: &[&str]| std::process::Command::new(exe).args(args).output().unwrap();
    let w = fake_s.as_str();
    let out = run(&[
        "init",
        "--root",
        root.to_str().unwrap(),
        "--cell",
        "c1",
        "--workerd",
        w,
    ]);
    eprintln!(
        "status={:?} stderr={} stdout={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(out.status.success());
    // Same params → idempotent skip.
    let out = run(&[
        "init",
        "--root",
        root.to_str().unwrap(),
        "--cell",
        "c1",
        "--workerd",
        w,
    ]);
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("unchanged"));
    // Changed cell → refuse without --reconfigure.
    let out = run(&[
        "init",
        "--root",
        root.to_str().unwrap(),
        "--cell",
        "c2",
        "--workerd",
        w,
    ]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("differs") && err.contains("cell"),
        "got: {err}"
    );
    // --reconfigure rewrites config only — keys/state bytes identical.
    let key_before = std::fs::read(root.join("keys/trusted.json")).unwrap();
    let out = run(&[
        "init",
        "--root",
        root.to_str().unwrap(),
        "--cell",
        "c2",
        "--reconfigure",
        "--workerd",
        w,
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(root.join("keys/trusted.json")).unwrap(),
        key_before,
        "reconfigure must never regenerate keys"
    );
    assert!(std::fs::read_to_string(root.join("config/vrt-host.toml"))
        .unwrap()
        .contains("c2"));
}
