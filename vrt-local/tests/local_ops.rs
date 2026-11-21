//! vrt-local behaviour tests — all API-level, exercising real files.

use std::path::{Path, PathBuf};
use vrt_local::{DeployOverrides, InitParams, LocalError, VrtLocal};

/// T37b: drop guard — unique top-level dir per test; parallel tests
/// never share a parent on cleanup.
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
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "vrt-local-{}-{}",
        std::process::id(),
        uuidish()
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
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

fn worker_dir(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(format!("worker-{name}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"{name}\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\ncompatibilityFlags = [\"nodejs_compat\"]\n"
        ),
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    dir
}

/// T37b-r3: `queueConsumers` is part of version identity — a deploy
/// that changes ONLY the consumer section must mint a new version and
/// publish a desired WITHOUT the consumer (previously deduped onto the
/// old version → dispatcher kept consuming the removed queue).
#[test]
fn deploy_consumer_only_change_removes_it() {
    let root = root();
    let local = init(&root);
    let dir = root.join("wq");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    let base =
        "name = \"wq\"\nmainModule = \"main.js\"\n         compatibilityDate = \"2024-09-25\"\n";
    std::fs::write(
        dir.join("vrt.toml"),
        format!("{base}[[queueConsumers]]\nqueue = \"qq\"\n"),
    )
    .unwrap();
    let r1 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    // Same code, consumer section dropped → NEW version, empty desired.
    std::fs::write(dir.join("vrt.toml"), base).unwrap();
    let r2 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    assert_ne!(r1.version_id, r2.version_id);
    let st = local.status().unwrap();
    let desired: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("desired").join("envelope.json")).unwrap())
            .unwrap();
    let consumers = &desired["payload"]["tenants"][0]["workers"][0]["queueConsumers"];
    assert_eq!(consumers, &serde_json::json!([]));
    let _ = st;
}

/// `[observability]` is version-scoped state (stored on VersionRecord,
/// replayed by to_desired, restored by rollback) — so it MUST be part of
/// the deploy dedup identity. A redeploy that changes only the sampling
/// rate mints a new version id; before the fix the second deploy was
/// silently deduped and the old gates stayed active.
#[test]
fn observability_is_part_of_version_identity() {
    let root = root();
    let local = init(&root);
    let dir = root.join("worker-obs");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    let base =
        "name = \"obs\"\nmain = \"main.js\"\ncompatibility_date = \"2024-09-25\"\n";
    std::fs::write(
        dir.join("wrangler.toml"),
        format!("{base}[observability.logs]\nhead_sampling_rate = 0.3\n"),
    )
    .unwrap();
    let r1 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    std::fs::write(
        dir.join("wrangler.toml"),
        format!("{base}[observability.logs]\nhead_sampling_rate = 0.7\n"),
    )
    .unwrap();
    let r2 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    assert_ne!(
        r1.version_id, r2.version_id,
        "observability-only change must mint a new version"
    );
    // The active version carries the NEW rate on the wire.
    let desired: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("desired").join("envelope.json")).unwrap())
            .unwrap();
    assert_eq!(
        desired["payload"]["tenants"][0]["workers"][0]["observability"]
            ["headSamplingRatePpm"],
        serde_json::json!(700_000)
    );
}

/// P3c-2-r2: `vrt worker-id` / `vrt do-key` typed resolution — the
/// e2e used to grep `vrt status` JSON positionally and died silently
/// (alphabetical serialization puts `versions` between `name` and
/// `workerId`). Name OR workerId both resolve — same convention as
/// `versions`/`rollback`.
#[test]
fn worker_id_resolves_by_name_or_id() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "do-demo");
    local.deploy(&dir, &DeployOverrides::default()).unwrap();
    let wid = local.status().unwrap().workers[0].worker_id.clone();
    assert_eq!(local.resolve_worker_id("do-demo").unwrap(), wid);
    assert_eq!(local.resolve_worker_id(&wid).unwrap(), wid);
    assert!(matches!(
        local.resolve_worker_id("ghost"),
        Err(LocalError::NotFound(_))
    ));
}

#[test]
fn do_class_key_resolves_declared_class() {
    let root = root();
    let local = init(&root);
    let dir = mk_do_worker(
        &root,
        "[durable_objects]\nbindings = [{name=\"C\", class_name=\"Counter\"}]\n\
         [[migrations]]\ntag = \"v1\"\nnew_sqlite_classes = [\"Counter\"]\n",
    );
    local.deploy(&dir, &DeployOverrides::default()).unwrap();
    let key = local.do_class_key("do-worker", "Counter").unwrap();
    assert_eq!(key.len(), 64);
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
    // Resolves by workerId too; undeclared class / ghost worker 404.
    let wid = local.resolve_worker_id("do-worker").unwrap();
    assert_eq!(local.do_class_key(&wid, "Counter").unwrap(), key);
    assert!(matches!(
        local.do_class_key("do-worker", "Ghost"),
        Err(LocalError::NotFound(_))
    ));
    assert!(matches!(
        local.do_class_key("ghost", "Counter"),
        Err(LocalError::NotFound(_))
    ));
}

/// P3c-2-r2 regression: the REAL `vrt status` pretty output observed
/// in compose — serde_json::Value serializes map keys ALPHABETICALLY,
/// so `versions` sits between `name` and `workerId`. A positional
/// `"name":"X","workerId":"Y"` grep can never match this shape; any
/// consumer must navigate JSON semantically (or use the typed CLI
/// queries added above). Verbatim sample from the r1 review:
#[test]
fn status_pretty_output_worker_id_lookup_is_order_independent() {
    let sample = r#"{
  "local": {
    "cellId": "local",
    "publishedRevision": 3,
    "revision": 3,
    "secretRefs": [],
    "tenantId": "local",
    "workers": [
      {
        "activeVersionId": "vrt_4177f79f39fbaf00eb680e5e3107",
        "hostnames": [
          "do.vrt.local"
        ],
        "name": "do-demo",
        "versions": [
          "vrt_4177f79f39fbaf00eb680e5e3107"
        ],
        "workerId": "w_5f5f391765232dca"
      }
    ]
  },
  "host": { "running": true },
  "metrics": { "reachable": false }
}"#;
    let v: serde_json::Value = serde_json::from_str(sample).unwrap();
    let w = v["local"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "do-demo")
        .expect("do-demo present");
    assert_eq!(w["workerId"], "w_5f5f391765232dca");
    // Pin the failure mode: `name` is followed by `versions`, not
    // `workerId` — the positional grep shape is structurally absent.
    assert!(sample
        .find("\"name\": \"do-demo\",\n        \"workerId\"")
        .is_none());
    // And our emitted output really is this shape (same serializer).
    let root = root();
    let local = init(&root);
    local
        .deploy(&worker_dir(&root, "do-demo"), &DeployOverrides::default())
        .unwrap();
    let emitted = serde_json::to_value(local.status().unwrap()).unwrap();
    let keys: Vec<&str> = emitted["workers"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    // Alphabetical: `versions` interposes between `name`/`workerId`.
    assert_eq!(
        keys,
        [
            "activeVersionId",
            "hostnames",
            "name",
            "quotas",
            "quotasEnforced",
            "versions",
            "workerId"
        ]
    );
}

#[test]
fn init_deploy_status_roundtrip() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "edge-api");
    let r1 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    // T25/L9: init publishes the empty envelope at revision 1, so the
    // first deploy is revision 2.
    assert_eq!(r1.revision, 2);
    let st = local.status().unwrap();
    assert_eq!(st.published_revision, Some(2));
    assert_eq!(st.workers.len(), 1);
    assert_eq!(
        st.workers[0].active_version_id.as_deref(),
        Some(r1.version_id.as_str())
    );
    // Re-deploy same content: same version id, new revision.
    let r2 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    assert_eq!(r2.version_id, r1.version_id);
    assert_eq!(r2.revision, 3);
    // Rollback to the same version is accepted; unknown version fails.
    local.rollback("edge-api", &r1.version_id).unwrap();
    assert!(matches!(
        local.rollback("edge-api", "vrt_nope"),
        Err(LocalError::NotFound("version"))
    ));
    // oplog exists and has no secret material.
    let log = std::fs::read_to_string(root.join("state/oplog.jsonl")).unwrap();
    assert!(log.contains("\"deploy\"") && log.contains("\"rollback\""));
}

#[test]
fn single_writer_lock_conflicts() {
    let root = root();
    init(&root);
    // Hold the flock ourselves → every mutating op must fail Locked.
    let lock_path = root.join("state/lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    use std::os::unix::io::AsRawFd;
    assert_eq!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let local = VrtLocal::open(&root).unwrap();
    let dir = worker_dir(&root, "w");
    assert!(matches!(
        local.deploy(&dir, &DeployOverrides::default()),
        Err(LocalError::Locked)
    ));
    assert!(matches!(
        local.put_secret("local", "r", "v"),
        Err(LocalError::Locked)
    ));
}

#[test]
fn key_permissions_fail_closed() {
    let root = root();
    init(&root);
    let key = root.join("keys/signing.seed");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    assert!(matches!(
        VrtLocal::open(&root),
        Err(LocalError::Layout("key mode"))
    ));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    // Truncated seed → fail closed.
    std::fs::write(&key, b"short").unwrap();
    assert!(matches!(
        VrtLocal::open(&root),
        Err(LocalError::Layout("key length"))
    ));
}

#[test]
fn secrets_are_tenant_scoped() {
    let root = root();
    let local = init(&root);
    local.put_secret("local", "api-key", "s3cr3t").unwrap();
    // Cross-tenant writes are refused, not silently stored.
    assert!(matches!(
        local.put_secret("other-tenant", "k", "v"),
        Err(LocalError::WrongTenant)
    ));
    let secrets: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("secrets/secrets.json")).unwrap()).unwrap();
    let obj = secrets.as_object().unwrap();
    assert_eq!(obj.len(), 1, "only the local tenant namespace exists");
    assert!(obj.contains_key("local"));
    assert_eq!(secrets["local"]["api-key"], "s3cr3t");
    assert!(matches!(
        local.delete_secret("local", "missing"),
        Err(LocalError::NotFound(_))
    ));
    // Oplog must never contain the secret VALUE.
    let log = std::fs::read_to_string(root.join("state/oplog.jsonl")).unwrap();
    assert!(!log.contains("s3cr3t"));
    assert!(log.contains("api-key"));
}

#[test]
fn init_refuses_nonempty_root() {
    let root = root();
    init(&root);
    assert!(matches!(
        VrtLocal::init(
            &root,
            &InitParams {
                cell_id: "local".into(),
                tenant_id: "local".into(),
                facade_manifest: None,
                facade_dist_root: None,
                max_versions_per_worker: None,
            }
        ),
        Err(LocalError::Layout("root already initialized"))
    ));
}

#[test]
fn pack_rejects_unknown_module_types_and_symlinks() {
    let root = root();
    let dir = worker_dir(&root, "w");
    std::fs::write(dir.join("blob.xyz"), b"\x00\x01").unwrap();
    let local = init(&root.join("rt"));
    assert!(matches!(
        local.deploy(&dir, &DeployOverrides::default()),
        Err(LocalError::Worker("module type"))
    ));
    // Declared-but-missing module also fails.
    std::fs::remove_file(dir.join("blob.xyz")).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"w\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n\n[modules]\n\"ghost.bin\" = \"data\"\n",
    )
    .unwrap();
    assert!(matches!(
        local.deploy(&dir, &DeployOverrides::default()),
        Err(LocalError::Worker("modules missing"))
    ));
}

#[test]
fn pack_parity_with_ts_contract_fixture() {
    // Rebuild the exact worker dir the TypeScript contract fixture was
    // built from — stored bytes must equal `ts_version_bundle.json`.
    let root = root();
    let dir = root.join("parity");
    std::fs::create_dir_all(dir.join("lib")).unwrap();
    std::fs::create_dir_all(dir.join("data")).unwrap();
    std::fs::create_dir_all(dir.join("bin")).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"parity\"\nmainModule = \"index.js\"\n\
         compatibilityDate = \"2024-09-25\"\ncompatibilityFlags = [\"nodejs_compat\"]\n\n\
         [modules]\n\"lib/util.js\" = \"commonJs\"\n\"bin/blob.bin\" = \"data\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("index.js"),
        "export default { async fetch() { return new Response(\"ok\") } }",
    )
    .unwrap();
    std::fs::write(dir.join("lib/util.js"), "module.exports = {}").unwrap();
    std::fs::write(dir.join("data/hello.txt"), "hello vrt").unwrap();
    std::fs::write(
        dir.join("data/table.json"),
        "{\"answer\":42,\"list\":[1,2,3]}",
    )
    .unwrap();
    std::fs::write(dir.join("bin/blob.bin"), [0u8, 1, 2, 3]).unwrap();
    std::fs::write(dir.join("bin/mod.wasm"), [0u8, 97, 115, 109]).unwrap();

    let pkg = vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &|_| None)
        .unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../vrt-config/fixtures/ts_version_bundle.json"
    ))
    .unwrap();
    assert_eq!(pkg.digest, fixture["digest"].as_str().unwrap());
    assert_eq!(
        String::from_utf8(pkg.stored_document).unwrap(),
        fixture["storedDocument"].as_str().unwrap(),
        "stored bytes must equal the TS-produced document"
    );
}

#[test]
fn invalid_ops_do_not_poison_state() {
    let root = root();
    let local = init(&root);
    let a = worker_dir(&root, "alpha");
    let b = worker_dir(&root, "beta");
    local.deploy(&a, &DeployOverrides::default()).unwrap();
    local.deploy(&b, &DeployOverrides::default()).unwrap();
    local.set_hostname("alpha", "api.example.com").unwrap();
    // Global hostname uniqueness: assigning the same hostname to a
    // second worker must fail AND must not persist — the state file
    // stays clean so later valid ops still publish.
    assert!(matches!(
        local.set_hostname("beta", "api.example.com"),
        Err(LocalError::Desired(_))
    ));
    let rev_before = local.status().unwrap().revision;
    local.set_hostname("beta", "www.example.com").unwrap();
    let st = local.status().unwrap();
    assert_eq!(st.revision, rev_before + 1);
    assert_eq!(st.published_revision, Some(st.revision));
    // deploy-time poison: bad worker name must fail before any write.
    let bad = root.join("worker-bad");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(
        bad.join("vrt.toml"),
        "name = \"Bad_Name\"\nmainModule = \"m.js\"\ncompatibilityDate = \"2024-09-25\"\n",
    )
    .unwrap();
    std::fs::write(bad.join("m.js"), "export default {}").unwrap();
    assert!(local.deploy(&bad, &DeployOverrides::default()).is_err());
    let after = local.status().unwrap();
    assert!(
        after.workers.iter().all(|w| w.name != "Bad_Name"),
        "rejected worker must not be persisted"
    );
}

#[test]
fn t20_pack_rejects_undeliverable_pg_and_assets_bindings() {
    // T20/B10 (original): pg bindings had no delivery backend — a packed
    // "success" would be a dead binding (false success).
    // P1: pg IS deliverable via `[bindings].pg` / `[[pg]]` — credentials
    // resolve provider-side from `secrets/pg-credentials.json` under
    // `vrtpgcred:<id>` (never bundled). The declared path packs; the
    // overrides channel still rejects (it could smuggle arbitrary
    // resource ids past credential-ref minting).
    // T21: assets ARE deliverable — a `[bindings].assets` digest that
    // names no local-store manifest is rejected at deploy (fail
    // closed, typed error — the exact variant depends on which gate
    // fires first).
    let root = root();
    let local = init(&root);
    let mk = |name: &str, extra: &str| {
        let dir = worker_dir(&root, name);
        std::fs::write(
            dir.join("vrt.toml"),
            format!(
                "name = \"{name}\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n{extra}"
            ),
        )
        .unwrap();
        dir
    };
    // P1: declared `[bindings].pg` deploys — the binding lands in
    // desired as `vrtpg:<id>` + `vrtpgcred:<id>` (credentials stay in
    // the provider-side secrets file, never the bundle).
    let pg = mk("pgw", "[bindings]\npg = { DB = \"db-01\" }\n");
    std::fs::write(pg.join("main.js"), "export default {}").unwrap();
    local.deploy(&pg, &DeployOverrides::default()).unwrap();
    let desired: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("desired").join("envelope.json")).unwrap(),
    )
    .unwrap();
    let pg_binding = desired["payload"]["tenants"][0]["workers"][0]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["name"] == "DB")
        .expect("pg binding emitted")
        .clone();
    assert_eq!(pg_binding["databaseId"], "vrtpg:db-01");
    assert_eq!(pg_binding["credentialRef"], "vrtpgcred:db-01");
    // no token/endpoint bytes anywhere in desired — secrets channel only
    assert!(!serde_json::to_string(&pg_binding).unwrap().contains("token"));
    let assets = mk("aw", "[bindings]\nassets = { SITE = \"deadbeef\" }\n");
    assert!(
        local.deploy(&assets, &DeployOverrides::default()).is_err(),
        "assets binding naming a missing manifest must fail closed"
    );
    // overrides-injected pg is rejected the same way.
    let clean = mk("cw", "");
    let mut overrides = DeployOverrides::default();
    overrides
        .bindings
        .push(snapshot_protocol::compute_desired::BindingDesired::Pg(
            snapshot_protocol::compute_desired::PgBinding {
                name: "DB".into(),
                database_id: "db-01".into(),
                credential_ref: "vrtpgcred:db-01".into(),
                permissions: snapshot_protocol::compute_desired::BindingPermissions {
                    read: true,
                    write: true,
                },
            },
        ));
    assert!(matches!(
        local.deploy(&clean, &overrides),
        Err(LocalError::Worker(_))
    ));
    let st = local.status().unwrap();
    // P1: `pgw` persists (declared pg is deliverable); the assets and
    // overrides-pg rejections above persisted nothing.
    assert_eq!(
        st.workers.len(),
        1,
        "rejected packs must not persist workers (only pgw landed)"
    );
    assert_eq!(st.workers[0].name, "pgw");
}

#[test]
fn t20_service_binding_resolves_by_name_and_fails_closed_on_unknown() {
    let root = root();
    let local = init(&root);
    // unknown target name → typed error, no guess.
    let dir = worker_dir(&root, "caller");
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"caller\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n         [bindings]\nservice = { SVC = \"ghost\" }\n",
    )
    .unwrap();
    assert!(matches!(
        local.deploy(&dir, &DeployOverrides::default()),
        Err(LocalError::Worker(_))
    ));
    // real target → resolves to that worker's id in the desired state.
    let callee = worker_dir(&root, "callee");
    local.deploy(&callee, &DeployOverrides::default()).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"caller\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n         [bindings]\nservice = { SVC = \"callee\" }\n",
    )
    .unwrap();
    local.deploy(&dir, &DeployOverrides::default()).unwrap();
    let st = local.status().unwrap();
    assert_eq!(st.workers.len(), 2);
}

#[test]
fn t20_crons_declared_in_vrt_toml_land_in_state() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "cronw");
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"cronw\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n         [[crons]]\nscheduleId = \"cron:cronw:0\"\nexpression = \"*/15 * * * *\"\n",
    )
    .unwrap();
    local.deploy(&dir, &DeployOverrides::default()).unwrap();
    let st = local.status().unwrap();
    assert_eq!(st.workers.len(), 1);
    // state persisted the cron declaration on the version record.
    let raw = std::fs::read_to_string(root.join("state/state.json")).unwrap();
    assert!(raw.contains("*/15 * * * *"));
    assert!(raw.contains("cron:cronw:0"));
}

#[test]
fn t21_assets_dir_deploys_and_materializes_manifest() {
    // `[assets] dir` — the deploy packs the directory, writes verified
    // objects+manifest into the provider-shared store, and the ASSETS
    // binding lands in the deployed version's desired doc.
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "site");
    std::fs::create_dir_all(dir.join("public/docs")).unwrap();
    std::fs::write(dir.join("public/index.html"), b"<h1>home</h1>").unwrap();
    std::fs::write(dir.join("public/docs/index.html"), b"docs").unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"site\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\
         [assets]\ndir = \"public\"\n",
    )
    .unwrap();
    local.deploy(&dir, &DeployOverrides::default()).unwrap();
    // manifest + objects must be present in the provider asset store.
    let store = vrt_provider::assets::AssetStore::open(
        &vrt_local::layout::provider_dir(&root)
            .join("data")
            .join("assets"),
    )
    .unwrap();
    // digest lands in the published desired doc (envelope payload).
    let raw = std::fs::read(vrt_local::layout::desired_dir(&root).join("envelope.json")).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let digest = doc["payload"]["tenants"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|t| t["workers"].as_array().unwrap().iter())
        .flat_map(|w| w["bindings"].as_array().unwrap().iter())
        .find_map(|b| {
            (b["type"].as_str() == Some("assets"))
                .then(|| b["manifestDigest"].as_str().unwrap().to_string())
        })
        .expect("assets binding must be in desired");
    let manifest = store
        .manifest("local", &digest)
        .unwrap()
        .expect("manifest stored");
    assert_eq!(manifest.files.len(), 2);
    // traversal in the assets dir is impossible by construction
    // (scan rejects non-files), but an explicit `[bindings].assets`
    // digest that names nothing must still fail closed — covered by
    // t20_pack_rejects_undeliverable_pg_and_assets_bindings.
    let second = worker_dir(&root, "site2");
    std::fs::write(
        second.join("vrt.toml"),
        format!(
            "name = \"site2\"\nmainModule = \"main.js\"\n\
             compatibilityDate = \"2024-09-25\"\n\
             [bindings]\nassets = {{ SITE = \"{digest}\" }}\n"
        ),
    )
    .unwrap();
    // same-tenant manifest reuse across workers must deploy.
    local.deploy(&second, &DeployOverrides::default()).unwrap();
}

#[test]
fn t21_assets_rejects_traversal_outside_dir() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "esc");
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"esc\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\
         [assets]\ndir = \"../outside\"\n",
    )
    .unwrap();
    assert!(local.deploy(&dir, &DeployOverrides::default()).is_err());
}

/// T34c/F2: databaseId derivation includes tenant; an invalid user_id fails closed.
#[test]
fn pack_d1_derivation_is_tenant_scoped_and_validated() {
    use sha2::Digest;
    let mk = |root: &Path, dbid: &str| {
        let dir = root.join(format!("worker-d1-{dbid}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("wrangler.toml"),
            format!(
                "name = \"d1-worker\"\nmain = \"main.js\"\n\
                 compatibility_date = \"2025-01-01\"\n\
                 [[d1_databases]]\nbinding = \"DB\"\ndatabase_id = \"{dbid}\"\n"
            ),
        )
        .unwrap();
        std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
        dir
    };
    let root = root();
    let expected = |tenant: &str, uid: &str| {
        hex::encode(sha2::Sha256::digest(
            format!("vrt-d1:{tenant}:{uid}").as_bytes(),
        ))[..26]
            .to_string()
    };
    let dir = mk(&root, "demo-db");
    let pkg = vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &|_| None)
        .unwrap();
    let d1 = pkg
        .bindings
        .iter()
        .find_map(|b| match b {
            snapshot_protocol::compute_desired::BindingDesired::D1(d) => Some(d),
            _ => None,
        })
        .expect("d1 binding");
    assert_eq!(d1.database_id, expected("local", "demo-db"));
    // Same declaration, different tenant => different databaseId (T35 no migration needed).
    let pkg2 = vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "other", &|_| None)
        .unwrap();
    let d1b = pkg2
        .bindings
        .iter()
        .find_map(|b| match b {
            snapshot_protocol::compute_desired::BindingDesired::D1(d) => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(d1b.database_id, expected("other", "demo-db"));
    assert_ne!(d1b.database_id, d1.database_id);
    // Invalid user_id => fail closed.
    for bad in ["BAD$ID", "", "with space", "x".repeat(65).as_str()] {
        let dir = mk(&root, "bad");
        std::fs::write(
            dir.join("wrangler.toml"),
            format!(
                "name = \"d1-worker\"\nmain = \"main.js\"\n\
                 compatibility_date = \"2025-01-01\"\n\
                 [[d1_databases]]\nbinding = \"DB\"\ndatabase_id = \"{bad}\"\n"
            ),
        )
        .unwrap();
        assert!(
            vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &|_| None)
                .is_err(),
            "user id {bad:?} must fail closed"
        );
    }
}

#[test]
fn pack_wrangler_hyperdrive_and_ratelimits_into_bindings() {
    use sha2::Digest;
    use snapshot_protocol::compute_desired::BindingDesired;

    let root = root();
    let dir = root.join("worker-hd-rl");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("wrangler.toml"),
        "name = \"hd-rl-worker\"\nmain = \"main.js\"\n\
         compatibility_date = \"2025-01-01\"\n\
         [[hyperdrive]]\nbinding = \"DB\"\ndatabase_tenant = \"tenant-prod\"\nid = \"cf-id-ignored\"\n\
         [[ratelimits]]\nname = \"RL\"\nnamespace_id = \"1001\"\n\
         simple = { limit = 1500, period = 60 }\n",
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();

    let pkg =
        vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "tenant-a", &|_| None)
            .unwrap();
    let hyperdrive = pkg
        .bindings
        .iter()
        .find_map(|binding| match binding {
            BindingDesired::Hyperdrive(value) => Some(value),
            _ => None,
        })
        .expect("hyperdrive binding");
    let binding_id = "vrthd:tenant-a:DB";
    let digest = hex::encode(sha2::Sha256::digest(binding_id.as_bytes()));
    assert_eq!(hyperdrive.binding_id, binding_id);
    assert_eq!(
        hyperdrive.synthetic_host,
        format!("hd{}.hyperdrive.vrt.internal", &digest[..24])
    );
    assert_eq!(hyperdrive.credential_ref, "vrtpgcred:tenant-prod");
    assert_eq!(hyperdrive.permissions.read, true);
    assert_eq!(hyperdrive.permissions.write, true);

    let limiter = pkg
        .bindings
        .iter()
        .find_map(|binding| match binding {
            BindingDesired::Ratelimit(value) => Some(value),
            _ => None,
        })
        .expect("rate limit binding");
    assert_eq!(limiter.name, "RL");
    assert_eq!(limiter.namespace_id, "1001");
    assert_eq!(limiter.limit, 1500);
    assert_eq!(limiter.period, 60);
}

/// Queue-level `delivery_delay` reaches the signed `QueueBinding` from
/// both config sources; an out-of-range value fails closed at pack.
#[test]
fn pack_queue_delivery_delay_from_wrangler_and_vrt_toml() {
    use snapshot_protocol::compute_desired::BindingDesired;

    let queue_delay = |dir: &Path| {
        let pkg = vrt_local::pack_with_resolver(dir, &DeployOverrides::default(), "local", &|_| {
            None
        })
        .unwrap();
        pkg.bindings
            .iter()
            .find_map(|binding| match binding {
                BindingDesired::Queue(q) => Some(q.delivery_delay_seconds),
                _ => None,
            })
            .expect("queue binding")
    };

    let root = root();
    // wrangler source: `queues.producers[].delivery_delay`.
    let wr_dir = root.join("worker-queue-delay-wrangler");
    std::fs::create_dir_all(&wr_dir).unwrap();
    std::fs::write(
        wr_dir.join("wrangler.toml"),
        "name = \"q-worker\"\nmain = \"main.js\"\ncompatibility_date = \"2025-01-01\"\n\
         [[queues.producers]]\nbinding = \"Q\"\nqueue = \"jobs\"\ndelivery_delay = 60\n",
    )
    .unwrap();
    std::fs::write(wr_dir.join("main.js"), "export default {}\n").unwrap();
    assert_eq!(queue_delay(&wr_dir), Some(60));

    // Native vrt.toml source: `[bindings.queue]` table form.
    let toml_dir = root.join("worker-queue-delay-toml");
    std::fs::create_dir_all(&toml_dir).unwrap();
    std::fs::write(
        toml_dir.join("vrt.toml"),
        "name = \"q-worker\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2025-01-01\"\n\
         [bindings.queue]\nQ = { queue = \"jobs\", deliveryDelaySeconds = 90 }\n",
    )
    .unwrap();
    std::fs::write(toml_dir.join("main.js"), "export default {}\n").unwrap();
    assert_eq!(queue_delay(&toml_dir), Some(90));

    // Legacy string form: no queue-level default.
    let legacy_dir = root.join("worker-queue-delay-legacy");
    std::fs::create_dir_all(&legacy_dir).unwrap();
    std::fs::write(
        legacy_dir.join("vrt.toml"),
        "name = \"q-worker\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2025-01-01\"\n\
         [bindings.queue]\nQ = \"jobs\"\n",
    )
    .unwrap();
    std::fs::write(legacy_dir.join("main.js"), "export default {}\n").unwrap();
    assert_eq!(queue_delay(&legacy_dir), None);

    // Native config > 86400 fails at pack (the signed protocol bound).
    let bad_dir = root.join("worker-queue-delay-bad");
    std::fs::create_dir_all(&bad_dir).unwrap();
    std::fs::write(
        bad_dir.join("vrt.toml"),
        "name = \"q-worker\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2025-01-01\"\n\
         [bindings.queue]\nQ = { queue = \"jobs\", deliveryDelaySeconds = 86401 }\n",
    )
    .unwrap();
    std::fs::write(bad_dir.join("main.js"), "export default {}\n").unwrap();
    assert!(
        vrt_local::pack_with_resolver(&bad_dir, &DeployOverrides::default(), "local", &|_| None)
            .is_err()
    );
}

#[test]
fn pack_selects_wrangler_environment_from_deploy_overrides() {
    let root = root();
    let dir = root.join("worker-env");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("wrangler.json"),
        r#"{"name":"env-worker","main":"main.js","compatibility_date":"2025-01-01",
           "vars":{"MODE":"base"},
           "env":{"staging":{"vars":{"MODE":"staging","ONLY_STAGING":"yes"}}}}"#,
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();

    let overrides = DeployOverrides {
        env_name: Some("staging".into()),
        ..Default::default()
    };
    let pkg = vrt_local::pack_with_resolver(&dir, &overrides, "local", &|_| None).unwrap();
    let values: Vec<&str> = pkg.var_values.values().map(String::as_str).collect();
    assert_eq!(values, ["staging", "yes"]);

    let err = match vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &|_| {
        None
    }) {
        Ok(_) => panic!("named env must not silently use the base config"),
        Err(err) => err,
    };
    assert!(format!("{err:?}").contains("named environments found"));
}

// ---------- P3c-1: Durable Objects ----------

fn mk_do_worker(root: &Path, extra_toml: &str) -> PathBuf {
    let dir = root.join(format!("worker-do-{}", extra_toml.len()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("wrangler.toml"),
        format!("name = \"do-worker\"\nmain = \"main.js\"\ncompatibility_date = \"2025-01-01\"\n{extra_toml}"),
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    dir
}

fn do_ctx<'a>(
    prior: &'a [snapshot_protocol::compute_desired::DoClassDesired],
) -> vrt_local::DoPackCtx<'a> {
    vrt_local::DoPackCtx {
        pepper: [0xAB; 32],
        prior_classes: prior,
    }
}

#[test]
fn do_new_sqlite_class_mints_hmac_key_and_enables_sql() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[durable_objects]\nbindings = [{name=\"C\", class_name=\"Counter\"}]\n\
         [[migrations]]\ntag = \"v1\"\nnew_sqlite_classes = [\"Counter\"]\n",
    );
    let ctx = do_ctx(&[]);
    let pkg = vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .unwrap();
    let class = pkg
        .durable_object_classes
        .iter()
        .find(|c| c.class_name == "Counter")
        .expect("class declared");
    assert!(class.enable_sql);
    assert_eq!(class.unique_key.len(), 64);
    assert!(class.unique_key.chars().all(|c| c.is_ascii_hexdigit()));
    // uniqueKey = HMAC(pepper, "vrt-do:tenant:worker:class") — derived,
    // never user input; identical derivation function must agree.
    let expected = vrt_local::derive_do_unique_key(
        &[0xAB; 32],
        "local",
        &format!("w_{}", pkg.first_deploy_id),
        "Counter",
    );
    assert_eq!(class.unique_key, expected);
    // The binding resolves against the declaration.
    assert!(pkg.bindings.iter().any(|b| matches!(
        b,
        snapshot_protocol::compute_desired::BindingDesired::DurableObject(d)
            if d.class_name == "Counter" && d.service_worker_id.is_none()
    )));
}

#[test]
fn do_new_classes_maps_to_kv_surface() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[durable_objects]\nbindings = [{name=\"K\", class_name=\"KvObj\"}]\n\
         [[migrations]]\ntag = \"v1\"\nnew_classes = [\"KvObj\"]\n",
    );
    let ctx = do_ctx(&[]);
    let pkg = vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .unwrap();
    let class = pkg
        .durable_object_classes
        .iter()
        .find(|c| c.class_name == "KvObj")
        .expect("class declared");
    // KV-backed classes share the sqlite localDisk store; enableSql
    // only gates the `storage.sql` surface (CF parity).
    assert!(!class.enable_sql);
}

#[test]
fn do_rename_keeps_unique_key_lineage() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[[migrations]]\ntag = \"v2\"\nrenamed_classes = [{from=\"Old\", to=\"New\"}]\n\
         [durable_objects]\nbindings = [{name=\"N\", class_name=\"New\"}]\n",
    );
    let prior = [snapshot_protocol::compute_desired::DoClassDesired {
        class_name: "Old".into(),
        unique_key: "f".repeat(64),
        enable_sql: true,
        from_worker_id: None,
    }];
    let ctx = do_ctx(&prior);
    let pkg = vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .unwrap();
    let class = pkg
        .durable_object_classes
        .iter()
        .find(|c| c.class_name == "New")
        .expect("renamed class");
    // Data identity follows the lineage, NOT the new name.
    assert_eq!(class.unique_key, "f".repeat(64));
    assert!(class.enable_sql);
    assert!(!pkg
        .durable_object_classes
        .iter()
        .any(|c| c.class_name == "Old"));
}

#[test]
fn do_declaration_without_ctx_fails_closed() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[durable_objects]\nbindings = [{name=\"C\", class_name=\"Counter\"}]\n\
         [[migrations]]\ntag = \"v1\"\nnew_sqlite_classes = [\"Counter\"]\n",
    );
    // No DoPackCtx → no key material → uniqueKey could never be proven
    // server-derived → the pack must fail closed.
    assert!(vrt_local::pack(&dir, &DeployOverrides::default(), "local").is_err());
    assert!(
        vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &|_| None)
            .is_err()
    );
}

/// P3c-2: `transferred_classes` rejected at the packed-manifest
/// boundary too — a vrt.toml carrying `transferredClasses` hits
/// `deny_unknown_fields` on `DoMigrationToml`; the wrangler
/// `transferred_classes` key is refused earlier in normalize
/// (shared golden vector covers TS parity).
#[test]
fn do_transferred_classes_rejected_at_pack_boundary() {
    let root = root();
    // Layer 4 — vrt.toml verbatim into WorkerToml (deny_unknown_fields).
    let dir = root.join("worker-do-xfer-toml");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"do-worker\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2025-01-01\"\n         [[durableObjectMigrations]]\ntransferredClasses = [{from=\"A\", to=\"B\"}]\n",
    )
    .unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    let ctx = do_ctx(&[]);
    assert!(vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .is_err());
    // Layer 3 — wrangler `[[migrations]] transferred_classes`.
    let dir2 = mk_do_worker(
        &root,
        "[[migrations]]\ntag = \"v1\"\ntransferred_classes = [{from=\"A\", to=\"B\"}]\n",
    );
    assert!(vrt_local::pack_with_ctx(
        &dir2,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .is_err());
}

#[test]
fn do_binding_to_undeclared_class_fails_closed() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[durable_objects]\nbindings = [{name=\"C\", class_name=\"Ghost\"}]\n",
    );
    let ctx = do_ctx(&[]);
    assert!(vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .is_err());
}

#[test]
fn do_deleted_class_removed_and_redeclare_mints_new() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[[migrations]]\ntag = \"v2\"\ndeleted_classes = [\"Old\"]\n\
         [[migrations]]\ntag = \"v3\"\nnew_sqlite_classes = [\"Old\"]\n",
    );
    let prior = [snapshot_protocol::compute_desired::DoClassDesired {
        class_name: "Old".into(),
        unique_key: "f".repeat(64),
        enable_sql: true,
        from_worker_id: None,
    }];
    let ctx = do_ctx(&prior);
    let pkg = vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    )
    .unwrap();
    // delete+redeclare = a NEW identity (the old lineage is dead).
    let class = pkg
        .durable_object_classes
        .iter()
        .find(|c| c.class_name == "Old")
        .expect("redeclared");
    assert_ne!(class.unique_key, "f".repeat(64));
}

#[test]
fn do_script_name_resolves_same_tenant_only() {
    let root = root();
    let dir = mk_do_worker(
        &root,
        "[durable_objects]\nbindings = [{name=\"R\", class_name=\"Remote\", script_name=\"peer\"}]\n",
    );
    let ctx = do_ctx(&[]);
    // `peer` resolves to a worker id — the class check runs at desired
    // build (peer declares nothing here ⇒ pack fails closed anyway).
    let missing = vrt_local::pack_with_ctx(
        &dir,
        &DeployOverrides::default(),
        "local",
        &|_| None,
        Some(&ctx),
    );
    assert!(missing.is_err());
}

/// Per-worker quotas diagnostics: `vrt status` reports the declared
/// quotas AND whether this root's host actually renders them
/// (`render.quotas`). A declared limit with no known host capability
/// reads `quotasEnforced: false` — never a silent "looks enforced".
#[test]
fn status_reports_quotas_and_enforcement() {
    let root = root();
    let local = init(&root);
    let dir = worker_dir(&root, "quota-demo");
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"quota-demo\"\nmainModule = \"main.js\"\n\
         compatibilityDate = \"2024-09-25\"\n\n[quotas]\ncpuMs = 200\nsubrequests = 120\n",
    )
    .unwrap();
    local.deploy(&dir, &DeployOverrides::default()).unwrap();

    let st = local.status().unwrap();
    let w = &st.workers[0];
    assert_eq!(w.quotas.cpu_ms, 200);
    assert_eq!(w.quotas.subrequests, 120);
    // Partial `[quotas]` fills the remaining dimensions with the
    // platform default (one shared source of truth).
    assert_eq!(
        w.quotas.memory_mb,
        snapshot_protocol::compute_desired::DEFAULT_MEMORY_MB
    );
    // A bare init root has no host config → no render capability proven.
    assert!(!w.quotas_enforced);
    let v = serde_json::to_value(&st).unwrap();
    assert_eq!(v["workers"][0]["quotas"]["cpuMs"], 200);
    assert_eq!(v["workers"][0]["quotasEnforced"], false);

    // A host config with `render.quotas = true` (the pinned fork) flips
    // the diagnostic to enforced.
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::write(
        root.join("config").join("vrt-host.toml"),
        "[render]\nmode = \"capnp\"\nquotas = true\n",
    )
    .unwrap();
    let st = local.status().unwrap();
    assert!(st.workers[0].quotas_enforced);
    assert_eq!(
        serde_json::to_value(&st).unwrap()["workers"][0]["quotasEnforced"],
        true
    );
}

/// `streamingTailConsumers` pack invariants — shared resolver with
/// `tailConsumers` so self-target/dedupe/cap cannot drift: a
/// self-designator would amplify each invocation into another
/// unboundedly; duplicates collapse on the declared name; the resolved
/// list stays within MAX_TAIL_CONSUMERS_PER_WORKER.
#[test]
fn streaming_tail_consumers_self_dedupe_cap() {
    let root = root();
    let _local = init(&root);
    let resolve = |n: &str| Some(format!("w_{n}"));

    // Self-target is refused before resolution even runs (the resolver
    // above would resolve "svc" too — the name check must come first).
    let dir = worker_dir(&root, "svc");
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"svc\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n\
         streamingTailConsumers = [{service = \"svc\"}]\n",
    )
    .unwrap();
    assert!(vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &resolve)
        .is_err());

    // Duplicate declarations collapse to one designator.
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"svc\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n\
         streamingTailConsumers = [{service = \"a\"}, {service = \"a\"}]\n",
    )
    .unwrap();
    let pkg =
        vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &resolve).unwrap();
    assert_eq!(pkg.streaming_tail_consumers.len(), 1);

    // One entry past the cap fails closed.
    let entries: String = (0..=snapshot_protocol::compute_desired::MAX_TAIL_CONSUMERS_PER_WORKER)
        .map(|i| format!("{{service = \"c{i}\"}}"))
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        dir.join("vrt.toml"),
        format!(
            "name = \"svc\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n\
             streamingTailConsumers = [{entries}]\n"
        ),
    )
    .unwrap();
    assert!(vrt_local::pack_with_resolver(&dir, &DeployOverrides::default(), "local", &resolve)
        .is_err());
}

/// `streamingTailConsumers` is per-version state — a deploy that only
/// changes the consumer list must mint a new version (same class the
/// `queueConsumers` identity fix covered).
#[test]
fn streaming_tail_consumers_change_mints_new_version() {
    let root = root();
    let local = init(&root);
    let consumer = worker_dir(&root, "consumer");
    local.deploy(&consumer, &DeployOverrides::default()).unwrap();

    let dir = worker_dir(&root, "svc");
    let r1 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    std::fs::write(
        dir.join("vrt.toml"),
        "name = \"svc\"\nmainModule = \"main.js\"\ncompatibilityDate = \"2024-09-25\"\n\
         streamingTailConsumers = [{service = \"consumer\"}]\n",
    )
    .unwrap();
    let r2 = local.deploy(&dir, &DeployOverrides::default()).unwrap();
    assert_ne!(
        r1.version_id, r2.version_id,
        "a consumer-only change must mint a new version"
    );
    // And the new version's desired carries the designator.
    let envelope = std::fs::read_to_string(root.join("desired").join("envelope.json")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&envelope).unwrap();
    let workers = &v["payload"]["tenants"][0]["workers"];
    let svc = workers
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "svc")
        .unwrap();
    assert_eq!(
        svc["streamingTailConsumers"].as_array().map(|a| a.len()),
        Some(1),
        "{v}"
    );
}

/// CF `[[flagship]]` + `flagship.json` — the standalone flag store
/// gathers into tenant-level `flagshipApps` on the signed desired doc;
/// the binding itself is version-scoped. A bound-but-undeclared app
/// synthesizes an empty store (CF local-dev semantics); a redeploy
/// that drops the file keeps previously merged apps (tenant-level
/// data is not version data).
#[test]
fn flagship_bindings_and_flag_store_deploy() {
    let root = root();
    let local = init(&root);
    // `[[flagship]]` is wrangler surface and vrt.toml shadows
    // wrangler.toml — build the dir by hand with wrangler.toml only.
    let dir = root.join("worker-flags-svc");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("main.js"), "export default {}\n").unwrap();
    std::fs::write(
        dir.join("wrangler.toml"),
        "name = \"flags-svc\"\nmain = \"main.js\"\n\
         compatibility_date = \"2025-01-01\"\n\
         [[flagship]]\nbinding = \"FLAGS\"\napp_id = \"app-01\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("flagship.json"),
        r#"{"app-01":{"flags":{"new-checkout":{"enabled":true,"type":"boolean","variants":{"on":true,"off":false},"defaultVariant":"off"}}},"#
            .to_string()
                + r#""app-extra":{"flags":{"p":{"enabled":false,"type":"string","variants":{"a":"x"},"defaultVariant":"a"}}}}"#,
    )
    .unwrap();
    local.deploy(&dir, &DeployOverrides::default()).unwrap();

    let raw = std::fs::read(vrt_local::layout::desired_dir(&root).join("envelope.json")).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let tenant = &doc["payload"]["tenants"][0];
    // The binding rides the worker version.
    let bindings = tenant["workers"][0]["bindings"].as_array().unwrap();
    let f = bindings
        .iter()
        .find(|b| b["type"] == "flagship")
        .expect("flagship binding");
    assert_eq!(f["name"], "FLAGS");
    assert_eq!(f["appId"], "app-01");
    // Apps are tenant-level: both the declared app and the extra one
    // ship; the declared flag content is verbatim.
    let apps = tenant["flagshipApps"].as_array().unwrap();
    assert_eq!(apps.len(), 2, "{apps:?}");
    assert_eq!(apps[0]["appId"], "app-01");
    assert!(apps[0]["config"].as_str().unwrap().contains("new-checkout"));
    assert_eq!(apps[1]["appId"], "app-extra");

    // Update the store: app-01's flags flip, app-extra removed —
    // merge semantics overwrite app-01 only (tenant-level updates are
    // per-app, like the CF flag store).
    std::fs::write(
        dir.join("flagship.json"),
        r#"{"app-01":{"flags":{"new-checkout":{"enabled":false,"type":"boolean","variants":{"on":true,"off":false},"defaultVariant":"on"}}}}"#,
    )
    .unwrap();
    local.deploy(&dir, &DeployOverrides::default()).unwrap();
    let st_raw = std::fs::read_to_string(root.join("state/state.json")).unwrap();
    assert!(st_raw.contains("\"appId\": \"app-01\""));
    assert!(st_raw.contains("app-extra"), "prior app survives: {st_raw}");
    // `config` is a serialized-JSON string field — quotes escape in
    // state.json.
    assert!(
        st_raw.contains("defaultVariant\\\":\\\"on"),
        "updated config landed: {st_raw}"
    );

    // A bound app absent from the file AND from prior state still
    // resolves — the synthesized empty config ships the local-store
    // contract (evaluations return call-site defaults).
    let dir2 = root.join("worker-flags-two");
    std::fs::create_dir_all(&dir2).unwrap();
    std::fs::write(dir2.join("main.js"), "export default {}\n").unwrap();
    std::fs::write(
        dir2.join("wrangler.toml"),
        "name = \"flags-two\"\nmain = \"main.js\"\n\
         compatibility_date = \"2025-01-01\"\n\
         [[flagship]]\nbinding = \"FLAGS\"\napp_id = \"app-missing\"\n",
    )
    .unwrap();
    local.deploy(&dir2, &DeployOverrides::default()).unwrap();
    let raw = std::fs::read(vrt_local::layout::desired_dir(&root).join("envelope.json")).unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let apps = doc["payload"]["tenants"][0]["flagshipApps"]
        .as_array()
        .unwrap();
    let missing = apps.iter().find(|a| a["appId"] == "app-missing").unwrap();
    assert_eq!(
        missing["config"].as_str().unwrap(),
        r#"{"appId":"app-missing","flags":{}}"#
    );

    // Malformed flag content fails closed before anything publishes.
    let dir3 = root.join("worker-flags-bad");
    std::fs::create_dir_all(&dir3).unwrap();
    std::fs::write(dir3.join("main.js"), "export default {}\n").unwrap();
    std::fs::write(
        dir3.join("wrangler.toml"),
        "name = \"flags-bad\"\nmain = \"main.js\"\n\
         compatibility_date = \"2025-01-01\"\n\
         [[flagship]]\nbinding = \"FLAGS\"\napp_id = \"app-x\"\n",
    )
    .unwrap();
    std::fs::write(
        dir3.join("flagship.json"),
        r#"{"app-x":{"flags":{"bad":{"enabled":true,"type":"nope","variants":{},"defaultVariant":"x"}}}}"#,
    )
    .unwrap();
    assert!(local.deploy(&dir3, &DeployOverrides::default()).is_err());
}
