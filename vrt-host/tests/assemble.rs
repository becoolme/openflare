//! Bundle cache + assembly tests — content-addressed store integrity,
//! digest-mismatch eviction, traversal rejection, stub/capnp emit.

mod common;

use common::*;
use std::path::PathBuf;
use std::sync::Arc;
use vrt_host::artifact::{
    sha256_hex, ArtifactError, BundleCache, BundleSource, DirBundleSource, MemoryBundleSource,
};
use vrt_host::assemble::{Assembler, EmitMode};
use vrt_host::endpoint::WorkerEndpoint;
use vrt_host::supervisor::TenantAssembler;

#[test]
fn cache_roundtrip_and_digest_eviction() {
    let fx = Fixture::new();
    let cache = BundleCache::new(fx.dir.path().join("cache"));
    let source = MemoryBundleSource::default();

    // Raw object roundtrip via fetch.
    let digest = source.insert(b"module-bytes");
    let bytes = cache.get_or_fetch_raw(&digest, &source).unwrap();
    assert_eq!(bytes, b"module-bytes");
    // Second read comes from cache (source still has it — same result).
    assert_eq!(cache.get_raw(&digest).unwrap(), b"module-bytes");

    // Corrupt the cached file → next read evicts + fetches again.
    std::fs::write(cache.root().join("objects").join(&digest), b"corrupted").unwrap();
    assert!(matches!(
        cache.get_raw(&digest),
        Err(ArtifactError::DigestMismatch)
    ));
    assert!(!cache.root().join("objects").join(&digest).exists());
    let bytes = cache.get_or_fetch_raw(&digest, &source).unwrap();
    assert_eq!(bytes, b"module-bytes");

    // put() refuses mismatched content.
    assert!(matches!(
        cache.put_raw(&digest, b"different"),
        Err(ArtifactError::DigestMismatch)
    ));

    // Traversal impossible — only strict hex is a path segment.
    assert!(matches!(
        cache.get_raw("../../etc/passwd"),
        Err(ArtifactError::BadDigest)
    ));
    assert!(matches!(
        cache.get_raw(&"A".repeat(64)),
        Err(ArtifactError::BadDigest)
    ));
}

#[test]
fn bundle_digest_mismatch_fails_closed() {
    let fx = Fixture::new();
    let cache = BundleCache::new(fx.dir.path().join("cache"));
    let source = MemoryBundleSource::default();

    // A real bundle from vrt-config's own builder.
    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());

    let fetched = cache
        .get_or_fetch_bundle(bundle.digest(), &source)
        .expect("bundle fetch");
    assert_eq!(fetched.digest(), bundle.digest());

    // Poisoned object under a valid key → mismatch → error, nothing cached.
    let bad_key = sha256_hex(b"not-a-bundle");
    source.insert_aliased(&bad_key, b"garbage");
    assert!(matches!(
        cache.get_or_fetch_bundle(&bad_key, &source),
        Err(ArtifactError::DigestMismatch | ArtifactError::BundleInvalid)
    ));

    // Tampered stored document (signature-broken JSON) → fails verify.
    let mut tampered = bundle.stored_document().to_vec();
    tampered[0] = b'[';
    source.insert_aliased(bundle.digest(), &tampered);
    // Drop the cached good copy first so the fetch path runs.
    std::fs::remove_file(cache.root().join("bundles").join(bundle.digest())).unwrap();
    assert!(matches!(
        cache.get_or_fetch_bundle(bundle.digest(), &source),
        Err(ArtifactError::DigestMismatch | ArtifactError::BundleInvalid)
    ));
}

/// Real `VersionBundle` seeded into a memory source — stub assembly
/// (T15/B2) fetches every worker's declared digest, so tests need a
/// verifiable bundle, not a synthetic hex string.
fn seeded_bundle() -> (Arc<MemoryBundleSource>, String) {
    let source = Arc::new(MemoryBundleSource::default());
    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    (source, bundle.digest().to_string())
}

#[test]
fn dir_source_reads_by_digest() {
    let fx = Fixture::new();
    let dir = fx.dir.path().join("src");
    std::fs::create_dir_all(&dir).unwrap();
    let content = b"sys-module";
    let digest = sha256_hex(content);
    std::fs::write(dir.join(&digest), content).unwrap();
    let source = DirBundleSource::new(&dir);
    assert_eq!(source.fetch(&digest).unwrap(), content);
    assert!(matches!(
        source.fetch(&"0".repeat(64)),
        Err(ArtifactError::Missing)
    ));
}

#[test]
fn stub_assembly_materializes_spec_and_config() {
    let fx = Fixture::new();
    let (source, digest) = seeded_bundle();
    let assembler = Assembler::new(
        stub_binary(),
        fx.work_root(),
        Arc::new(BundleCache::new(fx.dir.path().join("cache"))),
        source,
        EmitMode::StubJson,
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    );
    let desired = tenant(
        "t1",
        vec![
            worker("w1", "a.test", &digest),
            worker("w2", "b.test", &digest),
        ],
    );
    let spec = assembler.assemble(&desired, 1, 1).unwrap();
    assert_eq!(spec.worker_endpoints.len(), 2);
    assert!(matches!(
        spec.worker_endpoints["w1"],
        WorkerEndpoint::Tcp(_)
    ));
    assert!(spec.config_path.ends_with("stub-config.json"));
    let config = std::fs::read_to_string(&spec.config_path).unwrap();
    assert!(config.contains("sock-w1"));
    // Same desired fragment → same semantic digest across generations.
    let spec2 = assembler.assemble(&desired, 1, 2).unwrap();
    assert_eq!(spec.config_digest, spec2.config_digest);
    assert_ne!(spec.config_path, spec2.config_path);
    // A different tenant → different digest.
    let spec3 = assembler
        .assemble(&tenant("t2", vec![worker("w1", "c.test", &digest)]), 1, 1)
        .unwrap();
    assert_ne!(spec.config_digest, spec3.config_digest);
}

#[test]
fn stub_do_storage_classes_cover_workflows_and_dos() {
    // W1-r4: `worker_classes` is the single typed class set — a
    // workflow-only worker MUST appear (its Engine namespace rides
    // placement/fencing/snapshot/sweep through this map), and a
    // worker declaring both kinds contributes both ukeys.
    let fx = Fixture::new();
    let (source, digest) = seeded_bundle();
    let assembler = Assembler::new(
        stub_binary(),
        fx.work_root(),
        Arc::new(BundleCache::new(fx.dir.path().join("cache"))),
        source,
        EmitMode::StubJson,
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    );
    let do_ukey = "d".repeat(64);
    let wf_ukey = "e".repeat(64);
    let mut w = worker("w1", "a.test", &digest);
    w.durable_object_classes = vec![snapshot_protocol::compute_desired::DoClassDesired {
        class_name: "Counter".into(),
        unique_key: do_ukey.clone(),
        enable_sql: true,
        from_worker_id: None,
    }];
    w.workflows = vec![snapshot_protocol::compute_desired::WorkflowDesired {
        service_worker_id: None,
        name: "order-flow".into(),
        binding: "WF".into(),
        class_name: "OrderFlow".into(),
        unique_key: wf_ukey.clone(),
        limits_steps: Some(0),
    }];
    let mut wf_only = worker("w2", "b.test", &digest);
    wf_only.workflows = vec![snapshot_protocol::compute_desired::WorkflowDesired {
        service_worker_id: None,
        name: "f".into(),
        binding: "WF".into(),
        class_name: "F".into(),
        unique_key: "f".repeat(64),
        limits_steps: Some(0),
    }];
    let spec = assembler
        .assemble(&tenant("t1", vec![w, wf_only]), 1, 1)
        .unwrap();
    let do_spec = spec.do_storage.expect("stub spec must carry do_storage");
    let w1 = &do_spec.worker_classes["w1"];
    assert!(w1.contains(&do_ukey), "DO ukey missing: {w1:?}");
    assert!(w1.contains(&wf_ukey), "workflow ukey missing: {w1:?}");
    assert_eq!(
        do_spec.worker_classes["w2"],
        vec!["f".repeat(64)],
        "workflow-only worker must appear in the class map"
    );
}

/// Minimal capnp `RuntimeParams` for assembly tests: one system service
/// (`sys`) that every facade/egress hook points at.
fn assemble_test_params(sys_digest: String) -> vrt_config::RuntimeParams {
    vrt_config::RuntimeParams {
        socket_dir: String::new(),
        binding_backend_socket: "/tmp/binding.sock".to_string(),
        egress: vrt_config::ServiceEndpoint {
            service: "sys".to_string(),
            entrypoint: "Entrypoint".to_string(),
        },
        egress_policy: vrt_config::EgressPolicy::default(),
        deny_outbound: vrt_config::ServiceEndpoint {
            service: "sys".to_string(),
            entrypoint: "Entrypoint".to_string(),
        },
        facades: vrt_config::FacadeRefs {
            kv: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
            r2: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
            assets: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
            pg: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
            d1: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
            queue: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
            ratelimit: vrt_config::ServiceEndpoint {
                service: "sys".into(),
                entrypoint: "Entrypoint".into(),
            },
        },
        system_workers: vec![vrt_config::SystemWorkerRef {
            service_name: "sys".to_string(),
            main_module: "sys.js".to_string(),
            modules: vec![vrt_config::SystemModuleRef {
                name: "sys.js".to_string(),
                digest: sys_digest.clone(),
            }],
            needs_binding_backend: false,
        }],
        system_compat_date: "2024-01-01".to_string(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "assemble-test-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

#[test]
fn capnp_assembly_renders_and_materializes_embeds() {
    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));

    // One bundle for the tenant worker + one system module artifact.
    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    let sys_bytes = b"export default class { fetch() { return new Response('sys') } }";
    let sys_digest = source.insert(sys_bytes);

    let params = assemble_test_params(sys_digest.clone());

    let assembler = Assembler::new(
        PathBuf::from("/bin/true"), // not launched — materialization only
        fx.work_root(),
        cache.clone(),
        source,
        EmitMode::Capnp { params },
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    );
    let desired = tenant("t1", vec![worker("w1", "a.test", bundle.digest())]);
    let spec = assembler.assemble(&desired, 7, 3).unwrap();
    assert!(spec.config_path.ends_with("config.capnp"));
    let capnp = std::fs::read_to_string(&spec.config_path).unwrap();
    assert!(capnp.contains("sock-w1"), "capnp: {capnp}");
    // Embeds materialized under the gen dir + verified.
    assert!(spec
        .work_dir
        .join(format!("bundles/{}/main.js", bundle.digest()))
        .exists());
    assert!(spec.work_dir.join(format!("sys/{sys_digest}")).exists());
    assert!(matches!(
        spec.worker_endpoints["w1"],
        WorkerEndpoint::Unix(_)
    ));

    // Missing bundle → assembly fails closed.
    let missing = "f".repeat(64);
    let tenant2 = tenant("t2", vec![worker("w1", "c.test", &missing)]);
    assert!(assembler.assemble(&tenant2, 7, 1).is_err());
}

// ------------------------------------------------------------------
// T13/M2 — per-tenant uid/gid isolation policy
// ------------------------------------------------------------------

#[test]
fn isolation_uid_is_deterministic_per_tenant() {
    let fx = Fixture::new();
    let (source, digest) = seeded_bundle();
    let assembler = Assembler::new(
        stub_binary(),
        fx.work_root(),
        Arc::new(BundleCache::new(fx.dir.path().join("cache"))),
        source,
        EmitMode::StubJson,
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    )
    .with_isolation(vrt_host::assemble::IsolationPolicy {
        base: 60_000,
        span: 2_000,
    });
    // H5-r2: converge syncs the desired set before any assembly —
    // assembly under a policy that was never synced fails closed.
    let desired = tenant("t-iso", vec![worker("w1", "a.test", &digest)]);
    assert!(assembler.assemble(&desired, 1, 1).is_err());
    assembler.sync_desired_tenants(&["t-iso".to_string()]);
    let a = assembler.assemble(&desired, 1, 1).unwrap();
    let b = assembler.assemble(&desired, 1, 2).unwrap();
    // Same tenant → identical uid across generations.
    assert_eq!(a.isolation, b.isolation);
    let iso = a.isolation.expect("isolation must be populated");
    assert!(iso.uid >= 60_000 && iso.uid < 62_000);
    assert_eq!(iso.gid, iso.uid);
}

fn iso_assembler(
    fx: &Fixture,
    source: Arc<MemoryBundleSource>,
    base: u32,
    span: u32,
) -> Assembler {
    Assembler::new(
        stub_binary(),
        fx.work_root(),
        Arc::new(BundleCache::new(fx.dir.path().join("cache"))),
        source,
        EmitMode::StubJson,
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    )
    .with_isolation(vrt_host::assemble::IsolationPolicy { base, span })
}

#[test]
fn isolation_uid_collision_fails_closed() {
    // H5-r2: span=1 forces every tenant onto the same uid — ALL tenants
    // in the group are refused, never a first-come winner. The verdict
    // is set-derived: order-independent, and a deleted tenant frees
    // its uid on the next sync.
    let fx = Fixture::new();
    let (source, digest) = seeded_bundle();
    let assembler = iso_assembler(&fx, source, 60_000, 1);
    let ta = tenant("ta", vec![worker("w1", "a.test", &digest)]);
    let tb = tenant("tb", vec![worker("w1", "b.test", &digest)]);

    // Forward order: both refused.
    assembler.sync_desired_tenants(&["ta".to_string(), "tb".to_string()]);
    let err = assembler.assemble(&ta, 1, 1).unwrap_err();
    assert!(format!("{err}").contains("collision"), "{err}");
    let err = assembler.assemble(&tb, 1, 1).unwrap_err();
    assert!(format!("{err}").contains("collision"), "{err}");

    // Reverse order: identical verdict (set property, not a map).
    assembler.sync_desired_tenants(&["tb".to_string(), "ta".to_string()]);
    assert!(assembler.assemble(&ta, 1, 2).is_err());
    assert!(assembler.assemble(&tb, 1, 2).is_err());

    // tb leaves the desired set → ta's uid is free; ta assembles again.
    assembler.sync_desired_tenants(&["ta".to_string()]);
    assembler.assemble(&ta, 1, 3).unwrap();
}

#[test]
fn isolation_uid_collision_real_pair_only_group_refused() {
    // A real FNV collision pair in a production-sized range: ONLY the
    // colliding tenants are refused — bystanders still launch.
    let fx = Fixture::new();
    let (source, digest) = seeded_bundle();
    let assembler = iso_assembler(&fx, source, 60_000, 2_000);
    let (a, b) = {
        let mut seen = std::collections::HashMap::new();
        let mut hit = None;
        for i in 0..8_000u64 {
            let t = format!("tenant-{i}");
            let uid = vrt_securefs::tenant_uid(60_000, 2_000, &t);
            if let Some(other) = seen.insert(uid, t.clone()) {
                hit = Some((other, t));
                break;
            }
        }
        hit.expect("a collision inside span 2000 must exist")
    };
    let bystander = tenant("bystander", vec![worker("w1", "c.test", &digest)]);
    let ta = tenant(&a, vec![worker("w1", "a.test", &digest)]);
    let tb = tenant(&b, vec![worker("w1", "b.test", &digest)]);

    assembler.sync_desired_tenants(&[a.clone(), "bystander".to_string(), b.clone()]);
    assert!(assembler.assemble(&ta, 1, 1).is_err());
    assert!(assembler.assemble(&tb, 1, 1).is_err());
    assembler.assemble(&bystander, 1, 1).unwrap();
    // One collider leaves → survivor recovers, bystander unaffected.
    assembler.sync_desired_tenants(&[a.clone(), "bystander".to_string()]);
    assembler.assemble(&ta, 1, 2).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn isolation_launch_requires_root() {
    // Non-root host + isolation demand → launch fails closed before
    // spawn. (CI/dev hosts are not euid 0; the assertion documents the
    // contract rather than the ambient environment.)
    let euid0 = std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim() == "0")
        .unwrap_or(false);
    if euid0 {
        return; // running as root — the drop would succeed; skip.
    }
    let mut fx = Fixture::new();
    let mut spec = fx.stub_spec("t-iso", &["w1"], &[]);
    spec.isolation = Some(vrt_host::spec::TenantIsolation {
        uid: 60_001,
        gid: 60_001,
    });
    let launcher = vrt_host::launch::ProcessWorkerdLauncher;
    match vrt_host::launch::WorkerdLauncher::launch(&launcher, &spec).await {
        Err(e) => assert!(format!("{e}").contains("euid 0"), "{e}"),
        Ok(_) => panic!("isolation launch must fail closed on non-root host"),
    }
}

// ------------------------------------------------------------------
// T31d/R0 — real-manifest assembly of an assets-first tenant
// ------------------------------------------------------------------

/// Reproduces the real-standalone regression: a worker with
/// `assetsFirst` emits `router-<workerId>` embeds whose `service_name`
/// must resolve inside `params.system_workers` — assembly keys the
/// lookup on the MANIFEST name (`facade-router`). With the rendered
/// `router-<id>` name every embed returned `artifact missing` and the
/// whole tenant generation failed, taking down even assets-free
/// workers.
#[test]
fn assets_first_tenant_assembles_with_real_manifest() {
    use snapshot_protocol::compute_desired::{AssetsBinding, BindingDesired, BindingPermissions};
    use std::path::Path;

    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));

    // Real manifest → real system_workers (includes facade-router).
    let facade_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade");
    let raw = std::fs::read_to_string(facade_dir.join("manifest.json")).unwrap();
    let manifest = vrt_config::parse_facade_manifest(&raw).unwrap();
    assert!(manifest
        .system_workers
        .iter()
        .any(|w| w.service_name == "facade-router"));

    // Seed module bytes exactly as standalone `seed_system_modules`
    // does: wire `dist` path bytes → objects/<sha256>.
    let wire: serde_json::Value = serde_json::from_str(&raw).unwrap();
    for worker in &manifest.system_workers {
        let modules = wire["workers"][&worker.service_name]["modules"]
            .as_array()
            .unwrap();
        for m in modules {
            let name = m["name"].as_str().unwrap();
            let bytes = std::fs::read(facade_dir.join("dist").join(name)).unwrap();
            let digest = sha256_hex(&bytes);
            assert_eq!(digest, m["sha256"].as_str().unwrap());
            source.insert_aliased(&digest, &bytes);
        }
    }

    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());

    let params = vrt_config::RuntimeParams {
        socket_dir: String::new(),
        binding_backend_socket: "/tmp/binding.sock".to_string(),
        egress: manifest.egress.clone(),
        egress_policy: vrt_config::EgressPolicy::default(),
        deny_outbound: manifest.deny_outbound.clone(),
        facades: manifest.facades.clone(),
        system_workers: manifest.system_workers.clone(),
        system_compat_date: "2024-01-01".to_string(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "assemble-test-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };
    // Assets binding requires a materialized manifest in the shared
    // store (assemble.rs:340 fails closed without a configured sync).
    let (mdigest, mbytes) = {
        let m = serde_json::json!({
            "files": [{
                "mime": "text/html", "path": "/index.html",
                "sha256": sha256_hex(b"hi"), "size": 2,
            }],
            "htmlHandling": "auto-trailing-slash",
            "notFoundHandling": "none",
            "schemaVersion": 1,
        });
        let bytes = serde_json::to_vec(&m).unwrap();
        (sha256_hex(&bytes), bytes)
    };
    struct FakeSrc {
        manifests: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
        objects: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
    }
    impl vrt_host::asset_sync::AssetSource for FakeSrc {
        fn fetch_manifest(&self, d: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
            Ok(self.manifests.lock().unwrap().get(d).cloned())
        }
        fn fetch_object(&self, d: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
            Ok(self.objects.lock().unwrap().get(d).cloned())
        }
    }
    let asset_source = Arc::new(FakeSrc {
        manifests: std::sync::Mutex::new([(mdigest.clone(), mbytes)].into_iter().collect()),
        objects: std::sync::Mutex::new([(sha256_hex(b"hi"), b"hi".to_vec())].into_iter().collect()),
    });
    let asset_root = fx.dir.path().join("assets-store");
    std::fs::create_dir_all(&asset_root).unwrap();
    let asset_sync = Arc::new(vrt_host::asset_sync::AssetSync::new(
        vrt_provider::assets::AssetStore::open(&asset_root).unwrap(),
        asset_source,
    ));
    let assembler = Assembler::new_with_assets(
        PathBuf::from("/bin/true"),
        fx.work_root(),
        cache,
        source,
        EmitMode::Capnp { params },
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
        Some(asset_sync),
    );

    let mut w = worker("w1", "a.test", bundle.digest());
    w.bindings.push(BindingDesired::Assets(AssetsBinding {
        name: "ASSETS".to_string(),
        manifest_digest: mdigest,
        permissions: BindingPermissions {
            read: true,
            write: false,
        },
        assets_first: Some(true),
        run_worker_first: None,
    }));
    let spec = assembler
        .assemble(&tenant("t-r", vec![w]), 1, 1)
        .expect("assembly must succeed — all embeds seeded");
    let capnp = std::fs::read_to_string(&spec.config_path).unwrap();
    assert!(capnp.contains("router-w1"), "capnp: {capnp}");
    // Every facade-router module was materialized under sys/.
    let router = manifest
        .system_workers
        .iter()
        .find(|s| s.service_name == "facade-router")
        .unwrap();
    for m in &router.modules {
        assert!(
            spec.work_dir.join("sys").join(&m.digest).exists(),
            "missing sys embed {}",
            m.digest
        );
    }
}

// ------------------------------------------------------------------
// T31g/G1.4 — `experimental` compat flag requires --experimental argv
// ------------------------------------------------------------------

/// Render params with an `experimental` compat flag — the renderer
/// will emit it into every system worker's compatibilityFlags, which
/// bare `workerd serve` refuses without `--experimental`.
fn experimental_params(sys_digest: &str) -> vrt_config::RuntimeParams {
    let endpoint = || vrt_config::ServiceEndpoint {
        service: "sys".into(),
        entrypoint: "Entrypoint".into(),
    };
    vrt_config::RuntimeParams {
        socket_dir: String::new(),
        binding_backend_socket: "/tmp/binding.sock".to_string(),
        egress: endpoint(),
        egress_policy: vrt_config::EgressPolicy::default(),
        deny_outbound: endpoint(),
        facades: vrt_config::FacadeRefs {
            kv: endpoint(),
            r2: endpoint(),
            assets: endpoint(),
            pg: endpoint(),
            d1: endpoint(),
            queue: vrt_config::ServiceEndpoint {
                service: "facade-queue".into(),
                entrypoint: "QueueFacade".into(),
            },
            ratelimit: endpoint(),
        },
        system_workers: vec![vrt_config::SystemWorkerRef {
            service_name: "sys".to_string(),
            main_module: "sys.js".to_string(),
            modules: vec![vrt_config::SystemModuleRef {
                name: "sys.js".to_string(),
                digest: sys_digest.to_string(),
            }],
            needs_binding_backend: false,
        }],
        system_compat_date: "2024-01-01".to_string(),
        system_compat_flags: vec!["experimental".to_string()],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "assemble-test-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    }
}

#[test]
fn experimental_flag_without_arg_fails_closed_at_assemble() {
    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));

    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    let sys_bytes = b"export default class { fetch() { return new Response('sys') } }";
    let sys_digest = source.insert(sys_bytes);

    let mk = |extra: Vec<String>| {
        Assembler::new(
            PathBuf::from("/bin/true"),
            fx.work_root(),
            cache.clone(),
            source.clone(),
            EmitMode::Capnp {
                params: experimental_params(&sys_digest),
            },
            Default::default(),
            Default::default(),
            extra,
            std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
                Default::default(),
            )),
        )
    };
    let desired = tenant("t-exp", vec![worker("w1", "a.test", bundle.digest())]);

    // Missing --experimental: fail closed with a typed LaunchArgs
    // error — workerd would refuse the emitted compat flag anyway.
    let err = mk(vec![]).assemble(&desired, 7, 1).unwrap_err();
    assert!(
        matches!(err, vrt_host::assemble::AssembleError::LaunchArgs(_)),
        "expected LaunchArgs, got {err:?}"
    );
    assert!(err.to_string().contains("--experimental"), "{err}");

    // With the flag present the same assembly succeeds.
    let spec = mk(vec!["--experimental".to_string()])
        .assemble(&desired, 7, 2)
        .expect("with --experimental, assembly must succeed");
    assert!(spec.extra_args.iter().any(|a| a == "--experimental"));
}

// T37b-r2 — `--experimental` argv is DERIVED from the emitted config
// ------------------------------------------------------------------

/// Params like `experimental_params` but with caller-chosen system
/// flags — the argv-derivation cases need `service_binding_extra_handlers`
/// configured without the `experimental` catch-all.
fn params_with_flags(sys_digest: &str, flags: &[&str]) -> vrt_config::RuntimeParams {
    let mut p = experimental_params(sys_digest);
    p.system_compat_flags = flags.iter().map(|f| f.to_string()).collect();
    // The qconsumer emit resolves `facade-qconsumer` among system
    // workers (its modules are template-embedded per worker).
    p.system_workers.push(vrt_config::SystemWorkerRef {
        service_name: "facade-qconsumer".to_string(),
        main_module: "qc.js".to_string(),
        modules: vec![vrt_config::SystemModuleRef {
            name: "qc.js".to_string(),
            digest: sys_digest.to_string(),
        }],
        needs_binding_backend: false,
    });
    p
}

/// A tenant whose worker consumes a queue — the emit forces
/// `service_binding_extra_handlers` ($experimental) into the qconsumer
/// worker, so assemble must put `--experimental` in spec.extra_args
/// WITHOUT any operator-supplied arg. A consumer-free tenant keeps the
/// minimal argv (no flag) — least-privilege argv per generation.
#[test]
fn queue_consumer_derives_experimental_argv() {
    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));

    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    let sys_bytes = b"export default class { fetch() { return new Response('sys') } }";
    let sys_digest = source.insert(sys_bytes);

    let mk = |extra: Vec<String>| {
        Assembler::new(
            PathBuf::from("/bin/true"),
            fx.work_root(),
            cache.clone(),
            source.clone(),
            EmitMode::Capnp {
                params: params_with_flags(&sys_digest, &[]),
            },
            Default::default(),
            Default::default(),
            extra,
            std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
                Default::default(),
            )),
        )
    };

    // No consumers → argv stays minimal (no --experimental).
    let plain = tenant("t-plain", vec![worker("w1", "a.test", bundle.digest())]);
    let spec = mk(vec![]).assemble(&plain, 7, 1).unwrap();
    assert!(
        !spec.extra_args.iter().any(|a| a == "--experimental"),
        "consumer-free tenant must not get --experimental: {:?}",
        spec.extra_args
    );

    // With a consumer → assemble derives --experimental itself.
    let mut consumer_worker = worker("w1", "a.test", bundle.digest());
    consumer_worker.queue_consumers.push(
        snapshot_protocol::compute_desired::QueueConsumerDesired {
            queue_id: "q".repeat(26),
            queue_name: "jobs".to_string(),
            dead_letter_queue_id: None,
            max_batch_size: None,
            max_batch_timeout_secs: None,
            max_retries: None,
            retry_delay_secs: None,
            max_concurrency: None,
        },
    );
    let consuming = tenant("t-cons", vec![consumer_worker]);
    let spec = mk(vec![]).assemble(&consuming, 7, 2).unwrap();
    assert!(
        spec.extra_args.iter().any(|a| a == "--experimental"),
        "consumer tenant must derive --experimental: {:?}",
        spec.extra_args
    );

    // An experimental SYSTEM flag (not the catch-all) also derives argv.
    let mk_exp_sys = |extra: Vec<String>| {
        Assembler::new(
            PathBuf::from("/bin/true"),
            fx.work_root(),
            cache.clone(),
            source.clone(),
            EmitMode::Capnp {
                params: params_with_flags(&sys_digest, &["service_binding_extra_handlers"]),
            },
            Default::default(),
            Default::default(),
            extra,
            std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
                Default::default(),
            )),
        )
    };
    let spec = mk_exp_sys(vec![]).assemble(&plain, 7, 3).unwrap();
    assert!(
        spec.extra_args.iter().any(|a| a == "--experimental"),
        "experimental system flag must derive --experimental: {:?}",
        spec.extra_args
    );
}


// ---------------------------------------------------------------------------
// H5-r1: binding-backend per-tenant socket derivation
// ---------------------------------------------------------------------------

/// Two tenants on one cell MUST get different binding-backend
/// external-addrs, each byte-identical to the provider's bound
/// `<dir>/<seg(tenant)>.sock` — derived with the same
/// `vrt_securefs::socket_path` helper. A verbatim shared socket
/// would route every tenant through ONE tenant's listener
/// (cross-tenant authorization — the socket name is the tenant
/// authority on the provider side).
#[test]
fn binding_backend_dir_derives_per_tenant_socket() {
    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));

    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    let sys_digest = source.insert(b"export default class { fetch() { return new Response('sys') } }");

    let params = vrt_config::RuntimeParams {
        socket_dir: String::new(),
        // The params-level socket is only a transport placeholder in
        // dir mode — assemble MUST rewrite it per tenant.
        binding_backend_socket: "/transport-placeholder.sock".to_string(),
        egress: vrt_config::ServiceEndpoint {
            service: "sys".to_string(),
            entrypoint: "Entrypoint".to_string(),
        },
        egress_policy: vrt_config::EgressPolicy::default(),
        deny_outbound: vrt_config::ServiceEndpoint {
            service: "sys".to_string(),
            entrypoint: "Entrypoint".to_string(),
        },
        facades: vrt_config::FacadeRefs {
            kv: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
            r2: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
            assets: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
            pg: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
            d1: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
            queue: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
            ratelimit: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
        },
        system_workers: vec![vrt_config::SystemWorkerRef {
            service_name: "sys".to_string(),
            main_module: "sys.js".to_string(),
            modules: vec![vrt_config::SystemModuleRef {
                name: "sys.js".to_string(),
                digest: sys_digest.clone(),
            }],
            // Emits the binding-backend external addr.
            needs_binding_backend: true,
        }],
        system_compat_date: "2024-01-01".to_string(),
        system_compat_flags: vec![],
        vars: Default::default(),
        secrets: Default::default(),
        do_storage_dir: String::new(),
        wfop_pepper: "assemble-test-pepper".into(),
        wf_delete_addr: String::new(),
        cache_addr: String::new(),
        heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
        log_tail: vrt_config::LogTailParams::default(),
    };

    let backend_dir = fx.dir.path().join("provider-listeners");
    let assembler = Assembler::new(
        PathBuf::from("/bin/true"),
        fx.work_root(),
        cache.clone(),
        source,
        EmitMode::Capnp { params },
        Default::default(),
        Default::default(),
        vec![],
        std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
            Default::default(),
        )),
    )
    .with_binding_backend_dir(backend_dir.clone());

    let mut addrs = Vec::new();
    for tid in ["tenant-alpha", "tenant-beta"] {
        let spec = assembler
            .assemble(&tenant(tid, vec![worker("w1", "a.test", bundle.digest())]), 7, 3)
            .unwrap_or_else(|e| panic!("assemble {tid}: {e}"));
        let addr = spec
            .external_addrs
            .iter()
            .find(|a| a.service == vrt_config::BINDING_BACKEND_SERVICE)
            .unwrap_or_else(|| panic!("{tid}: no binding-backend addr in {:?}", spec.external_addrs));
        // The SAME helper the provider binds with — byte-identical.
        let expected = format!(
            "unix:{}",
            vrt_securefs::socket_path(&backend_dir, tid)
                .to_str()
                .unwrap()
        );
        assert_eq!(addr.address, expected, "{tid} backend addr");
        addrs.push(addr.address.clone());
    }
    assert_ne!(addrs[0], addrs[1], "tenants must not share a socket");
    // Neither may carry the verbatim transport placeholder.
    for a in &addrs {
        assert!(!a.contains("transport-placeholder"), "{a}");
    }
}

/// The dir is part of the params fingerprint — swapping it must
/// rekey the render digest or a moved provider would reuse a stale
/// generation pointing at the old location.
#[test]
fn binding_backend_dir_rekeys_fingerprint() {
    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));

    let mk = |dir: Option<PathBuf>| {
        let mut a = Assembler::new(
            PathBuf::from("/bin/true"),
            fx.work_root(),
            cache.clone(),
            source.clone(),
            EmitMode::Capnp {
                params: vrt_config::RuntimeParams {
                    socket_dir: String::new(),
                    binding_backend_socket: "/placeholder.sock".to_string(),
                    egress: vrt_config::ServiceEndpoint {
                        service: "sys".to_string(),
                        entrypoint: "Entrypoint".to_string(),
                    },
                    egress_policy: vrt_config::EgressPolicy::default(),
                    deny_outbound: vrt_config::ServiceEndpoint {
                        service: "sys".to_string(),
                        entrypoint: "Entrypoint".to_string(),
                    },
                    facades: vrt_config::FacadeRefs {
                        kv: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                        r2: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                        assets: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                        pg: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                        d1: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                        queue: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                        ratelimit: vrt_config::ServiceEndpoint { service: "sys".into(), entrypoint: "Entrypoint".into() },
                    },
                    system_workers: vec![],
                    system_compat_date: "2024-01-01".to_string(),
                    system_compat_flags: vec![],
                    vars: Default::default(),
                    secrets: Default::default(),
                    do_storage_dir: String::new(),
                    wfop_pepper: "x".into(),
                    wf_delete_addr: String::new(),
                    cache_addr: String::new(),
                    heartbeat_socket: String::new(),
        waituntil_socket: String::new(),
                    log_tail: vrt_config::LogTailParams::default(),
                },
            },
            Default::default(),
            Default::default(),
            vec![],
            std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
                Default::default(),
            )),
        );
        if let Some(d) = dir {
            a = a.with_binding_backend_dir(d);
        }
        a
    };
    let none = mk(None).params_fingerprint().unwrap();
    let a = mk(Some(fx.dir.path().join("listeners-a")))
        .params_fingerprint()
        .unwrap();
    let b = mk(Some(fx.dir.path().join("listeners-b")))
        .params_fingerprint()
        .unwrap();
    assert_ne!(none, a);
    assert_ne!(a, b, "binding-backend dir must rekey the fingerprint");
}

/// A tenant declaring `[[worker_loaders]]` renders capnp `workerLoader`
/// bindings, which are `$experimental`-gated upstream: workerd refuses
/// to parse the config without `--experimental` in argv. The derive
/// must flip the flag for loader tenants — and only when needed.
#[test]
fn worker_loaders_enable_experimental_extra_arg() {
    let args = |loaders: bool| {
        vrt_host::assemble::experimental_extra_args(
            &[],
            vrt_host::assemble::ExperimentalNeeds {
                worker_loaders: loaders,
                ..Default::default()
            },
        )
    };
    assert!(args(true).iter().any(|a| a == "--experimental"));
    assert!(args(false).is_empty());
}

/// Per-worker quotas: the host renders `limits` for a declared quota,
/// and a host whose workerd cannot enforce it refuses the generation
/// instead of running the worker unlimited (`render.quotas = false`).
#[test]
fn quotas_capability_off_fails_closed() {
    let fx = Fixture::new();
    let source = Arc::new(MemoryBundleSource::default());
    let cache = Arc::new(BundleCache::new(fx.dir.path().join("cache")));
    let bundle = vrt_config::VersionBundle::from_pack(&serde_json::json!({
        "mainModule": "main.js",
        "modules": { "main.js": "export default { fetch() { return new Response('ok') } }" },
        "compatibilityDate": "2024-01-01",
    }))
    .unwrap();
    source.insert_aliased(bundle.digest(), bundle.stored_document());
    let sys_digest = source.insert(b"export default class {}");
    let params = assemble_test_params(sys_digest);
    let build = |quotas: bool| {
        Assembler::new(
            PathBuf::from("/bin/true"), // not launched — materialization only
            fx.work_root(),
            cache.clone(),
            source.clone(),
            EmitMode::Capnp {
                params: params.clone(),
            },
            Default::default(),
            Default::default(),
            vec![],
            std::sync::Arc::new(vrt_host::render_inputs::StaticRenderInputs(
                Default::default(),
            )),
        )
        .with_quotas(quotas)
    };
    let mut desired = tenant("t1", vec![worker("w1", "a.test", bundle.digest())]);
    desired.workers[0].quotas.cpu_ms = 900;

    let spec = build(true).assemble(&desired, 7, 1).unwrap();
    let capnp = std::fs::read_to_string(&spec.config_path).unwrap();
    assert!(
        capnp.contains("limits = (cpuMs = 900)"),
        "declared quota must render: {capnp}"
    );

    // Capability off: same tenant is refused before anything is written.
    let err = build(false).assemble(&desired, 7, 2).unwrap_err();
    assert!(
        err.to_string().contains("CONFIG_QUOTA_UNSUPPORTED")
            || matches!(
                err,
                vrt_host::assemble::AssembleError::Render(vrt_config::ConfigError::Quotas("cpuMs"))
            ),
        "unexpected error: {err}"
    );
    assert!(
        !fx.work_root().join("t1").join("gen-2").exists(),
        "a refused generation must not be materialized"
    );
}

