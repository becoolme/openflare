//! W1: `[[workflows]]` pack validation + typed persistence +
//! read-only `workflows` query. Engine uniqueKey must be
//! server-derived (HMAC over tenant/worker/name), never user input.

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
    // pid + atomic counter — parallel tests in one process must get
    // distinct roots. (A nanos timestamp proved flaky: coarse macOS
    // clock granularity let two tests collide on the same root, and
    // `.init-tmp-{pid}` staging then raced with `File exists`.)
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-local-wf-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
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

fn wf_worker_dir(root: &Path, name: &str, wf_toml: &str) -> PathBuf {
    let dir = root.join(format!("worker-{name}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n{wf_toml}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        "import { WorkflowEntrypoint } from \"cloudflare:workers\";\n\
             export default {}\n\
             export class OrderFlow extends WorkflowEntrypoint { async run() {} }\n",
    )
    .unwrap();
    dir
}

const WF: &str =
    "[[workflows]]\nname = \"order-flow\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"";

#[test]
fn deploy_persists_workflows_with_derived_key() {
    let root = root();
    let local = init(&root);
    local
        .deploy(
            &wf_worker_dir(&root, "wf-demo", WF),
            &DeployOverrides::default(),
        )
        .unwrap();
    let wfs = local.workflows("wf-demo").unwrap();
    assert_eq!(wfs.len(), 1);
    let wf = &wfs[0];
    assert_eq!(wf.name, "order-flow");
    assert_eq!(wf.binding, "WF");
    assert_eq!(wf.class_name, "OrderFlow");
    assert_eq!(wf.unique_key.len(), 64);
    assert!(wf
        .unique_key
        .bytes()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    // Derived, not caller-chosen: same input → same key across redeploy.
    let v1 = local.resolve_worker_id("wf-demo").unwrap();
    local
        .deploy(
            &wf_worker_dir(&root, "wf-demo", WF),
            &DeployOverrides::default(),
        )
        .unwrap();
    assert_eq!(
        local.workflows("wf-demo").unwrap()[0].unique_key,
        wf.unique_key
    );
    assert_eq!(v1, local.resolve_worker_id("wf-demo").unwrap());
}

#[test]
fn deploy_rejects_bad_workflow_decls() {
    let root = root();
    let local = init(&root);
    let cases: Vec<(&str, &str)> = vec![
        // bad name shape
        ("name = \"bad name!\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"", "name"),
        // 65-char name
        ("__LONG65__", "name"),
        // non-identifier binding
        ("name = \"f\"\nbinding = \"9WF\"\nclassName = \"OrderFlow\"", "binding"),
        // steps bounds
        ("name = \"f\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"\nlimitsSteps = 0", "steps"),
        ("name = \"f\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"\nlimitsSteps = 100001", "steps"),
        // cross-worker script_name (W1 fail-closed)
        ("name = \"f\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"\nscriptName = \"other-worker\"", "script_name"),
    ];
    for (i, (body, _)) in cases.iter().enumerate() {
        let toml = body.replace("__LONG65__", &"x".repeat(65));
        let dir = wf_worker_dir(
            &root,
            &format!("bad-{i}"),
            &format!("[[workflows]]\n{toml}"),
        );
        assert!(
            local.deploy(&dir, &DeployOverrides::default()).is_err(),
            "case {i} ({toml:?}) must fail closed"
        );
    }
}

#[test]
fn deploy_rejects_duplicate_and_binding_collision() {
    let root = root();
    let local = init(&root);
    // two workflows, same name
    let dup = "[[workflows]]\nname = \"f\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"\n\
               [[workflows]]\nname = \"f\"\nbinding = \"WF2\"\nclassName = \"OrderFlow\"";
    assert!(local
        .deploy(
            &wf_worker_dir(&root, "dup", dup),
            &DeployOverrides::default()
        )
        .is_err());
    // workflow binding collides with a var name — desired-side
    // validate must also catch it (env namespace is shared).
    let collide =
        "[[workflows]]\nname = \"f\"\nbinding = \"SECRET_X\"\nclassName = \"OrderFlow\"\n\
                   [vars]\nSECRET_X = \"v\"";
    assert!(local
        .deploy(
            &wf_worker_dir(&root, "coll", collide),
            &DeployOverrides::default()
        )
        .is_err());
}

#[test]
fn workflows_query_typed_and_empty() {
    let root = root();
    let local = init(&root);
    // worker without workflows → empty vec (not error)
    local
        .deploy(
            &wf_worker_dir(&root, "plain", ""),
            &DeployOverrides::default(),
        )
        .unwrap();
    assert!(local.workflows("plain").unwrap().is_empty());
    // deploy with workflows → desired.validate() passed internally
    // (deploy fails closed pre-publish on invalid desired), query is typed.
    local
        .deploy(
            &wf_worker_dir(&root, "wf-demo", WF),
            &DeployOverrides::default(),
        )
        .unwrap();
    assert_eq!(local.workflows("wf-demo").unwrap().len(), 1);
    // unknown worker → not found
    assert!(local.workflows("ghost").is_err());
}

// W1-r1 regression: the deploy record MUST be identical whether the
// input is `vrt.toml` (camelCase) or `wrangler.toml` (snake_case).
// The wrangler path silently dropped `[[workflows]]` at the
// `wrangler_config_to_worker_toml` wire projection before the fix.
fn wrangler_worker_dir(root: &Path, name: &str, wf_toml: &str) -> PathBuf {
    let dir = root.join(format!("wrangler-{name}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("wrangler.toml"),
        format!(
            "name = \"{name}\"\nmain = \"main.js\"\n\
             compatibility_date = \"2024-09-25\"\n{wf_toml}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        "import { WorkflowEntrypoint } from \"cloudflare:workers\";\n\
             export default {}\n\
             export class OrderFlow extends WorkflowEntrypoint { async run() {} }\n",
    )
    .unwrap();
    dir
}

#[test]
fn wrangler_workflows_normalize_snake_case_fields() {
    let root = root();
    let local = init(&root);
    // Covers class_name, script_name (same-worker), limits.steps.
    let toml = "[[workflows]]\nname = \"order-flow\"\nbinding = \"WF\"\n\
                class_name = \"OrderFlow\"\nscript_name = \"wf-demo\"\n\
                [workflows.limits]\nsteps = 50\n";
    local
        .deploy(
            &wrangler_worker_dir(&root, "wf-demo", toml),
            &DeployOverrides::default(),
        )
        .unwrap();
    let wf = &local.workflows("wf-demo").unwrap()[0];
    assert_eq!(wf.name, "order-flow");
    assert_eq!(wf.binding, "WF");
    assert_eq!(wf.class_name, "OrderFlow");
    assert_eq!(wf.limits_steps, Some(50));
    assert_eq!(wf.unique_key.len(), 64);
}

#[test]
fn wrangler_and_vrt_toml_produce_identical_records() {
    // Same root: deploy wf-demo from vrt.toml, then redeploy the SAME
    // worker from wrangler.toml — the typed workflow record (incl.
    // the HMAC unique_key, which is per-root peppered) must be
    // byte-identical across input syntaxes.
    let root_a = root();
    let local = init(&root_a);
    local
        .deploy(
            &wf_worker_dir(&root_a, "wf-demo", WF),
            &DeployOverrides::default(),
        )
        .unwrap();
    let vrt_wf = local.workflows("wf-demo").unwrap();
    let toml = "[[workflows]]\nname = \"order-flow\"\nbinding = \"WF\"\n\
                class_name = \"OrderFlow\"\n";
    local
        .deploy(
            &wrangler_worker_dir(&root_a, "wf-demo", toml),
            &DeployOverrides::default(),
        )
        .unwrap();
    let wrangler_wf = local.workflows("wf-demo").unwrap();
    assert_eq!(vrt_wf, wrangler_wf);
}

#[test]
fn standalone_example_dir_deploys_and_queries_workflow() {
    // The real e2e fixture — wrangler.toml `[[workflows]]` must reach
    // the deploy record through the full chain (previously dropped
    // at the wire projection → `of workflows` returned []).
    let example = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../deploy/standalone/examples/standalone-workflow");
    let example = if example.exists() {
        example
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/standalone-workflow")
    };
    assert!(
        example.join("wrangler.toml").exists(),
        "fixture missing: {example:?}"
    );
    let root = root();
    let local = init(&root);
    local.deploy(&example, &DeployOverrides::default()).unwrap();
    let wfs = local.workflows("wf-demo").unwrap();
    assert_eq!(wfs.len(), 1, "order-flow must be in the deploy record");
    assert_eq!(wfs[0].name, "order-flow");
    assert_eq!(wfs[0].binding, "WF");
    assert_eq!(wfs[0].class_name, "OrderFlow");
}

// W7: deploy-gate — the bundle must export class_name AND the class
// must extend WorkflowEntrypoint (workerd loads a plain class fine;
// it only dies at engine init — rejected at pack instead).
#[test]
fn deploy_rejects_workflow_class_problems() {
    let root = root();
    let local = init(&root);

    // Exported but NOT a WorkflowEntrypoint subclass.
    let dir = root.join("worker-wf-plain");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"wf-plain\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n[[workflows]]\n\
         name = \"order-flow\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("main.js"),
        "export default {}\nexport class OrderFlow { async run() {} }\n",
    )
    .unwrap();
    let err = local.deploy(&dir, &DeployOverrides::default()).unwrap_err();
    assert!(format!("{err:?}").contains("WorkflowEntrypoint"), "{err:?}");

    // Not exported at all.
    let dir2 = root.join("worker-wf-ghost");
    std::fs::create_dir_all(&dir2).unwrap();
    std::fs::write(
        dir2.join("vrt.toml"),
        "name = \"wf-ghost\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n[[workflows]]\n\
         name = \"order-flow\"\nbinding = \"WF\"\nclassName = \"Ghost\"\n",
    )
    .unwrap();
    std::fs::write(dir2.join("main.js"), "export default {}\n").unwrap();
    let err = local
        .deploy(&dir2, &DeployOverrides::default())
        .unwrap_err();
    assert!(format!("{err:?}").contains("not exported"), "{err:?}");

    // Exported but not a class.
    let dir3 = root.join("worker-wf-fn");
    std::fs::create_dir_all(&dir3).unwrap();
    std::fs::write(
        dir3.join("vrt.toml"),
        "name = \"wf-fn\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n[[workflows]]\n\
         name = \"order-flow\"\nbinding = \"WF\"\nclassName = \"OrderFlow\"\n",
    )
    .unwrap();
    std::fs::write(
        dir3.join("main.js"),
        "export default {}\nexport const OrderFlow = () => {}\n",
    )
    .unwrap();
    let err = local
        .deploy(&dir3, &DeployOverrides::default())
        .unwrap_err();
    assert!(format!("{err:?}").contains("not a class"), "{err:?}");

    // A failed deploy publishes nothing — the worker record is never
    // created (NotFound) or holds no workflows.
    match local.workflows("wf-plain") {
        Ok(wfs) => assert!(wfs.is_empty()),
        Err(e) => assert!(format!("{e:?}").contains("NotFound"), "{e:?}"),
    }
}
