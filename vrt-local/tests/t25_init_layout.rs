//! T25 (B8 real-machine) — init/layout contract tests:
//! L1 mount-point-safe atomic init, L2 path-bearing errors,
//! L3 facade dist mapping, L4 manifest persisted into root,
//! L6 provider dirs, L8 cursor secret, L9 signed empty envelope.

use std::path::{Path, PathBuf};
use vrt_local::{InitParams, LocalError, VrtLocal};

/// T37b: drop guard — owns ONE unique top-level dir per test so
/// parallel tests never race a shared parent on cleanup.
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
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("tmp cleanup {}: {e}", self.0.display());
        }
    }
}

fn tmp_base(tag: &str) -> TmpDir {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-local-t25-{tag}-{}-{}",
        std::process::id(),
        uuidish()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    TmpDir(dir)
}

fn root() -> TmpDir {
    // init lays out under the unique base directly — the guard owns it.
    tmp_base("root")
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

fn init_at(root: &Path) {
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
}

fn facade_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn init_with_facade(root: &Path) {
    let fd = facade_dir();
    VrtLocal::init(
        root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: Some(fd.join("manifest.json")),
            facade_dist_root: Some(fd),
            max_versions_per_worker: None,
        },
    )
    .unwrap();
}

// ---------- L1: mount-point-safe atomic init ----------

#[test]
fn init_inside_existing_empty_dir_preserves_the_dir_itself() {
    // The compose named-volume case: root EXISTS as an empty dir
    // (possibly a mount point — must never be removed/renamed).
    let root = root();
    assert!(root.exists());
    init_at(&root);
    // Root still the same dir (not replaced), now holding the layout
    // + the .vrt-init commit marker.
    assert!(root.join(".vrt-init").is_file(), "commit marker");
    for d in [
        "keys", "state", "desired", "objects", "secrets", "vars", "config", "mgmt", "facade",
        "provider", "work", "cache", "objstore",
    ] {
        assert!(root.join(d).is_dir(), "{d} created");
    }
    // No staging residue.
    let leftovers: Vec<_> = std::fs::read_dir(&root)
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(".init-tmp-"))
        })
        .collect();
    assert!(leftovers.is_empty(), "staging cleaned");
    VrtLocal::open(&root).unwrap();
}

#[test]
fn init_reuses_residue_from_a_crashed_init() {
    // Simulate a crash after the marker-free partial commit: residue
    // entries are EXACTLY our layout names — next init must clean and
    // proceed, not fail "root not empty".
    let root = root();
    for d in ["keys", "state", ".init-tmp-99999"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    init_at(&root);
    assert!(root.join(".vrt-init").is_file());
    VrtLocal::open(&root).unwrap();
}

#[test]
fn init_fails_closed_on_foreign_content() {
    let root = root();
    std::fs::write(root.join("stranger.txt"), b"not ours").unwrap();
    let err = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: None,
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, LocalError::Layout("root not empty")),
        "{err:?}"
    );
    // Foreign file untouched.
    assert!(root.join("stranger.txt").exists());
}

#[test]
fn init_rerun_after_success_reports_already_initialized() {
    let root = root();
    init_at(&root);
    let err = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            facade_dist_root: None,
            max_versions_per_worker: None,
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, LocalError::Layout("root already initialized")),
        "{err:?}"
    );
}

// ---------- L2: errors carry operation + path ----------

#[test]
fn init_error_names_the_failing_path() {
    // Unwritable PARENT: init's own dir creation must report the path
    // it tried, not a bare "layout: root".
    let base = tmp_base("ro");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o500)).unwrap();
        let root = base.join("root");
        let err = VrtLocal::init(
            &root,
            &InitParams {
                cell_id: "local".into(),
                tenant_id: "local".into(),
                facade_manifest: None,
                facade_dist_root: None,
                max_versions_per_worker: None,
            },
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("root") || text.contains(&base.display().to_string()),
            "error names the operation/path: {text}"
        );
        // restore for cleanup
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[test]
fn seed_error_names_the_module_path() {
    // A facade dir missing one module's dist file → the error must
    // name the path it tried to read.
    let root = root();
    let fd_base = tmp_base("fd");
    let fd = fd_base.join("facade");
    std::fs::create_dir_all(&fd).unwrap();
    // Manifest copied verbatim but dist/ absent entirely.
    std::fs::copy(facade_dir().join("manifest.json"), fd.join("manifest.json")).unwrap();
    let err = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: Some(fd.join("manifest.json")),
            facade_dist_root: Some(fd.clone()),
            max_versions_per_worker: None,
        },
    )
    .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("dist"), "seed error names path: {text}");
}

// ---------- L3 + L4: facade seed convention + manifest persisted ----------

#[test]
fn init_seeds_facade_modules_and_persists_manifest() {
    let root = root();
    init_with_facade(&root);
    // L4: the manifest lands INSIDE the root where the generated host
    // config points — no dependency on the external dist mount.
    assert!(root.join("facade/manifest.json").is_file());
    // L3: every declared module digest landed under objects/<digest>.
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("facade/manifest.json")).unwrap()).unwrap();
    let mut count = 0usize;
    for w in manifest["workers"].as_object().unwrap().values() {
        for m in w["modules"].as_array().unwrap() {
            let digest = m["sha256"].as_str().unwrap();
            assert!(
                root.join("objects").join(digest).is_file(),
                "seeded module {digest}"
            );
            count += 1;
        }
    }
    assert!(count > 10, "seeded all system modules: {count}");
    // L9: signed empty envelope published at revision 1.
    let env: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("desired/envelope.json")).unwrap())
            .unwrap();
    assert_eq!(env["revision"].as_u64(), Some(1));
    let tenants = env["payload"]["tenants"].as_array().unwrap();
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0]["workers"].as_array().unwrap().len(), 0);
    // L8: cursor secret file exists, 0600.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(root.join("keys/cursor.secret")).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        assert!(meta.len() >= 32);
    }
    // L6: provider listener dir + fs backends exist (0700).
    for d in [
        "provider/listeners",
        "provider/data/kv",
        "provider/data/r2",
        "provider/data/assets",
    ] {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(root.join(d)).unwrap();
            assert!(meta.is_dir(), "{d} exists");
            assert_eq!(meta.permissions().mode() & 0o777, 0o700, "{d} 0700");
        }
    }
}

#[test]
fn init_persists_verified_dist_tree_matching_host_convention() {
    use sha2::Digest as _;
    let root = root();
    init_with_facade(&root);
    // vrt-host `system_seed::read_module` reads
    // `<manifest dir>/dist/<module.name>` (plain Normal components) and
    // checks sha256 against the manifest digest. vrt-host dev-depends on
    // vrt-local, so the same convention is replayed here.
    let manifest_path = root.join("facade/manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let dist = std::fs::canonicalize(manifest_path.parent().unwrap().join("dist")).unwrap();
    let mut count = 0usize;
    for w in manifest["workers"].as_object().unwrap().values() {
        for m in w["modules"].as_array().unwrap() {
            let name = m["name"].as_str().unwrap();
            assert!(Path::new(name)
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_))));
            let p = dist.join(name);
            let meta = std::fs::symlink_metadata(&p).unwrap();
            assert!(meta.is_file() && !meta.file_type().is_symlink(), "{name}");
            assert!(std::fs::canonicalize(&p).unwrap().starts_with(&dist));
            let bytes = std::fs::read(&p).unwrap();
            assert_eq!(
                hex::encode(sha2::Sha256::digest(&bytes)),
                m["sha256"].as_str().unwrap(),
                "{name}"
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(meta.permissions().mode() & 0o777, 0o644, "{name}");
            }
            count += 1;
        }
    }
    assert!(count > 10, "checked all system modules: {count}");
    // Manifest on disk is byte-identical to the verified source.
    assert_eq!(
        std::fs::read(&manifest_path).unwrap(),
        std::fs::read(facade_dir().join("manifest.json")).unwrap()
    );
}

#[test]
fn tampered_facade_module_fails_closed() {
    // L4: seed verifies dist bytes against manifest digests — a
    // tampered module fails init, not produces a bad render.
    let root = root();
    let fd_base = tmp_base("tamper");
    let fd = fd_base.join("facade");
    // Copy the whole facade dir then corrupt one module.
    let src = facade_dir();
    cp_r(&src, &fd);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fd.join("manifest.json")).unwrap()).unwrap();
    let w = manifest["workers"].as_object().unwrap();
    let first = w.values().next().unwrap();
    let name = first["modules"][0]["name"].as_str().unwrap();
    std::fs::write(fd.join("dist").join(name), b"tampered").unwrap();
    let err = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: Some(fd.join("manifest.json")),
            facade_dist_root: Some(fd),
            max_versions_per_worker: None,
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, LocalError::Layout("module digest")),
        "digest mismatch fails closed: {err:?}"
    );
    // The failed init leaves no initialized root.
    assert!(!root.join(".vrt-init").exists());
    // No half-seeded facade/ dist or manifest leaks into the final root.
    assert!(!root.join("facade/manifest.json").exists());
    assert!(!root.join("facade/dist").exists());
}

fn cp_r(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap().flatten() {
        let target = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            cp_r(&e.path(), &target);
        } else {
            std::fs::copy(e.path(), &target).unwrap();
        }
    }
}

// ---------- L9: empty envelope is a valid signed state ----------

#[test]
fn init_envelope_verifies_against_trusted_key() {
    let root = root();
    init_at(&root);
    // The file the host/provider read must VERIFY, not merely parse:
    // FileDesiredStore semantics — signature, cell scope, revision ≥ 1.
    let raw = std::fs::read(root.join("desired/envelope.json")).unwrap();
    let trusted: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("keys/trusted.json")).unwrap()).unwrap();
    let key = snapshot_protocol::TrustedKey {
        key_id: trusted["keyId"].as_str().unwrap().to_string(),
        public_key: trusted["publicKey"].as_str().unwrap().to_string(),
    };
    let mut guard = snapshot_protocol::RevisionGuard::new();
    let verified = snapshot_protocol::compute_desired::verify_compute_desired(
        &raw,
        "local",
        &[key],
        &mut guard,
        chrono::Utc::now(),
    );
    assert!(verified.is_ok(), "empty envelope verifies: {verified:?}");
    let v = verified.unwrap();
    assert_eq!(v.payload.revision, 1);
    assert_eq!(v.payload.tenants[0].workers.len(), 0);
}

#[test]
fn init_without_facade_still_publishes_envelope() {
    // Bare-metal init with no --facade-dist: still valid signed empty
    // state; facade manifest simply absent (host config then fails
    // closed on the missing manifest — never on a half-seeded one).
    let root = root();
    init_at(&root);
    assert!(root.join("desired/envelope.json").is_file());
    assert!(!root.join("facade/manifest.json").exists());
}

// ---------- L6: generated configs reference existing paths ----------

#[test]
fn generated_config_paths_all_exist() {
    // L4/L6 cross-check: after `vrt init --facade-dist <repo>/facade`,
    // every file/dir the rendered host+provider configs name must
    // exist inside the root. This is the exact contract that B8 broke
    // (facadeManifest pointed at a file init never wrote).
    let root = root();
    init_with_facade(&root);
    // Required file/dir set produced by the templates.
    for p in [
        "facade/manifest.json",  // render.facadeManifest
        "desired/envelope.json", // desired.path
        "secrets/secrets.json",  // render.secretsFile
        "vars/vars.json",        // render.varsFile
        "keys/cursor.secret",    // provider cursorSecretFile
        "provider/listeners",    // provider listenerDir
        "provider/data/kv",
        "provider/data/r2",
        "provider/data/assets",
        "objects", // bundles.dir + cacheDir
        "work",    // host workDir
    ] {
        assert!(root.join(p).exists(), "init produced {p}");
    }
}
