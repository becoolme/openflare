//! T16: standalone-mode artifacts must satisfy the SAME host read
//! paths platform mode uses — no second runtime.
//!
//!   vrt-local deploy ──▶ objects/<digest>        ──▶ DirBundleSource
//!                    └─▶ desired/envelope.json   ──▶ FileDesiredStore
//!                                                    (verify+fence)

use std::path::{Path, PathBuf};
use vrt_host::artifact::{BundleCache, DirBundleSource};
use vrt_local::{DeployOverrides, InitParams, VrtLocal};
use vrt_provider::FileDesiredStore;

fn fresh(dir: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(dir);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn worker_dir(root: &Path) -> PathBuf {
    let dir = root.join("worker-src");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"edge-api\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\
         hostnames = [\"api.example.com\"]\n\n[vars]\nLOG_LEVEL = \"info\"\n",
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    dir
}

#[test]
fn standalone_artifacts_load_through_real_host_paths() {
    let root = fresh("standalone-e2e");
    let report = VrtLocal::init(
        &root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: None,
            max_versions_per_worker: None,
            facade_dist_root: None,
        },
    )
    .unwrap();
    let local = VrtLocal::open(&root).unwrap();
    let src = worker_dir(&root);
    let r1 = local.deploy(&src, &DeployOverrides::default()).unwrap();
    let rev1_bytes = std::fs::read(root.join("desired/envelope.json")).unwrap();

    // --- FileDesiredStore: the real verification path ---
    let store = FileDesiredStore::new(
        root.join("desired/envelope.json"),
        "local",
        vec![report.trusted_key.clone()],
        1,
    );
    // T25/L9: init publishes a signed EMPTY envelope at revision 1 —
    // the first deploy is therefore revision 2 (monotonic fencing).
    let rev = store.reload(chrono::Utc::now()).expect("envelope verifies");
    assert_eq!(rev, 2);
    let snap = store.snapshot(chrono::Utc::now()).unwrap();
    assert_eq!(snap.tenants.len(), 1);
    assert_eq!(snap.tenants[0].workers.len(), 1);
    assert_eq!(snap.tenants[0].workers[0].name, "edge-api");
    assert_eq!(
        snap.tenants[0].workers[0].routes[0].hostname,
        "api.example.com"
    );

    // --- DirBundleSource + BundleCache: the real artifact path ---
    let cache_root = fresh("standalone-cache");
    let cache = BundleCache::new(&cache_root);
    let source = DirBundleSource::new(root.join("objects"));
    let bundle = cache
        .get_or_fetch_bundle(&r1.artifact_digest, &source)
        .expect("bundle resolves");
    assert_eq!(bundle.digest(), r1.artifact_digest);

    // --- fence: replaying an OLD envelope must not roll the store back ---
    let r2 = local.deploy(&src, &DeployOverrides::default()).unwrap();
    assert_eq!(r2.revision, 3);
    store.reload(chrono::Utc::now()).unwrap();
    std::fs::write(root.join("desired/envelope.json"), &rev1_bytes).unwrap();
    // Reload of the stale file fails the fence — store keeps rev 3.
    assert!(store.reload(chrono::Utc::now()).is_err());
    assert_eq!(
        store.snapshot(chrono::Utc::now()).unwrap().revision,
        3,
        "fenced reload keeps the newer snapshot"
    );

    // --- crash window: envelope removed, renew re-publishes state ---
    std::fs::remove_file(root.join("desired/envelope.json")).unwrap();
    assert!(store.reload(chrono::Utc::now()).is_err());
    assert_eq!(store.snapshot(chrono::Utc::now()).unwrap().revision, 3);
    local.renew().unwrap();
    assert!(store.reload(chrono::Utc::now()).is_ok());
}
