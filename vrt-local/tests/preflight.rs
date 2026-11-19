//! W1-r2/P0-2: deploy-time workerd load check — a config workerd
//! cannot load must fail `deploy` BEFORE publish (explicit error,
//! old version untouched), never reach the tenant process. Skipped
//! when the root is not a standalone host root or runs stub mode.

use std::path::{Path, PathBuf};
use vrt_local::{DeployOverrides, InitParams, VrtLocal};

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
fn root() -> TmpDir {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-local-pf-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
}

fn facade() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade")
}

fn init(root: &Path, with_facade: bool) -> VrtLocal {
    let facade = facade();
    VrtLocal::init(
        root,
        &InitParams {
            cell_id: "local".into(),
            tenant_id: "local".into(),
            facade_manifest: with_facade.then(|| facade.join("manifest.json")),
            facade_dist_root: with_facade.then_some(facade),
            max_versions_per_worker: None,
        },
    )
    .unwrap();
    VrtLocal::open(root).unwrap()
}

fn worker_dir(root: &Path, name: &str, main: &str) -> PathBuf {
    let dir = root.join(format!("w-{name}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), main).unwrap();
    dir
}

/// Write `config/vrt-host.toml` — the document the check keys off.
fn write_host_toml(root: &Path, mode: &str, workerd: &Path) {
    let cfg_dir = root.join("config");
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let manifest = facade().join("manifest.json");
    std::fs::write(
        cfg_dir.join("vrt-host.toml"),
        format!(
            "workerdBinary = \"{}\"\n\
             doStorageDir = \"{}\"\n\
             [render]\n\
             mode = \"{mode}\"\n\
             facadeManifest = \"{}\"\n\
             bindingBackendSocket = \"/nonexistent.sock\"\n\
             systemCompatDate = \"2024-09-25\"\n\
             systemCompatFlags = []\n",
            workerd.display(),
            root.join("do").display(),
            manifest.display(),
        ),
    )
    .unwrap();
}

const GOOD: &str = "export default { async fetch() { return new Response('ok'); } };\n";
const BAD: &str = "import \"nonexistent:mod\";\nexport default { async fetch() { return new Response('x'); } };\n";

#[test]
fn check_skips_when_root_has_no_host_config() {
    // Library/test roots never wrote config/vrt-host.toml — no host
    // will render, so nothing is checked (deploy proceeds).
    let root = root();
    let local = init(&root, false);
    local
        .deploy(&worker_dir(&root, "w1", GOOD), &DeployOverrides::default())
        .expect("no host config → check skipped");
}

#[test]
fn check_skips_in_stub_mode() {
    // Stub workerd never parses capnp — even an unloadable module is
    // deployable under stub semantics (documented W1 stub behavior).
    let root = root();
    let local = init(&root, false);
    write_host_toml(&root, "stub", Path::new("/nonexistent-stub"));
    local
        .deploy(&worker_dir(&root, "w1", BAD), &DeployOverrides::default())
        .expect("stub mode → check skipped");
}

#[test]
fn check_rejects_unloadable_config_keeps_old_version() {
    let Some(workerd) = std::env::var_os("VRT_WORKERD_BIN").map(PathBuf::from) else {
        eprintln!("VRT_WORKERD_BIN unset — real-workerd deploy check skipped");
        return;
    };
    let root = root();
    let local = init(&root, true);
    write_host_toml(&root, "capnp", &workerd);

    // v1 loads → published.
    local
        .deploy(&worker_dir(&root, "w1", GOOD), &DeployOverrides::default())
        .expect("loadable config deploys");

    // v2 cannot load → deploy fails BEFORE publish; v1 stays active.
    let err = local
        .deploy(&worker_dir(&root, "w1", BAD), &DeployOverrides::default())
        .expect_err("unloadable config must fail the deploy");
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent:mod"),
        "error names the cause: {msg}"
    );
    let st: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("state/state.json")).unwrap()).unwrap();
    let w = &st["workers"]["w1"];
    assert_eq!(
        w["versions"].as_array().unwrap().len(),
        1,
        "failed deploy must not persist the version: {w}"
    );
}

/// W1-r3 P0: the check must never run tenant `test()` handlers and
/// never touch the live DO `localDisk`. A worker whose `test` export
/// throws would fail the old check (handlers ran); under the
/// impossible filter it must deploy fine, and the host-toml
/// doStorageDir must stay empty.
#[test]
fn check_never_runs_test_handler_and_never_touches_live_storage() {
    let Some(workerd) = std::env::var_os("VRT_WORKERD_BIN").map(PathBuf::from) else {
        eprintln!("VRT_WORKERD_BIN unset — real-workerd deploy check skipped");
        return;
    };
    let root = root();
    let local = init(&root, true);
    write_host_toml(&root, "capnp", &workerd);
    let live_do = root.join("do");
    std::fs::create_dir_all(&live_do).unwrap();

    let dir = root.join("w-do");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("wrangler.toml"),
        "name = \"do1\"\nmain = \"main.js\"\ncompatibility_date = \"2024-09-25\"\n\
         [durable_objects]\nbindings = [{name=\"C\", class_name=\"Counter\"}]\n\
         [[migrations]]\ntag = \"v1\"\nnew_sqlite_classes = [\"Counter\"]\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        "import { DurableObject } from \"cloudflare:workers\";\n\
         export class Counter extends DurableObject {\n\
         \x20 async fetch() { return new Response(\"ok\"); }\n\
         }\n\
         export default {\n\
         \x20 async fetch() { return new Response(\"ok\"); },\n\
         \x20 async test() { throw new Error(\"tenant test handler must never run\"); },\n\
         };\n",
    )
    .unwrap();
    local
        .deploy(&dir, &DeployOverrides::default())
        .expect("deploy must pass — the test() handler can never run under the check filter");

    // The check ran a real `workerd test` on a DO-bearing config —
    // the live storage root must have ZERO artifacts.
    fn files(p: &Path) -> Vec<PathBuf> {
        let mut v = Vec::new();
        for e in std::fs::read_dir(p).unwrap() {
            let e = e.unwrap().path();
            if e.is_dir() {
                v.extend(files(&e));
            } else {
                v.push(e);
            }
        }
        v
    }
    assert!(
        files(&live_do).is_empty(),
        "live doStorageDir touched by the check: {:?}",
        files(&live_do)
    );
    // No check scratch survived either.
    let leftover: Vec<_> = std::fs::read_dir(root.join("work"))
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            e.file_name()
                .to_string_lossy()
                .starts_with("preflight-")
                .then_some(e.path())
        })
        .collect();
    assert!(
        leftover.is_empty(),
        "preflight scratch leaked: {leftover:?}"
    );
}
