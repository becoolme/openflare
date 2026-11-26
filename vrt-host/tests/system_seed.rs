//! System-module seeding: verified image-baked facade modules land in
//! the object cache so assembly never touches the network source.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use vrt_host::artifact::{sha256_hex, ArtifactError, BundleCache, BundleSource};
use vrt_host::system_seed::{seed_system_modules, SeedError};

const SERVICES: [&str; 14] = [
    "facade-kv",
    "facade-r2",
    "facade-assets",
    "facade-pg",
    "facade-d1",
    "facade-queue",
    "facade-ratelimit",
    "facade-k2",
    "facade-email",
    "facade-images",
    "facade-vectorize",
    "facade-browser",
    "vrt-egress",
    "vrt-no-network",
];

struct Fixture {
    _tmp: tempfile::TempDir,
    manifest: PathBuf,
    cache: BundleCache,
    dist: PathBuf,
}

fn module_name(svc: &str) -> String {
    format!("sys/{svc}.js")
}
fn module_bytes(svc: &str) -> Vec<u8> {
    format!("export default {{ svc: \"{svc}\" }};").into_bytes()
}

/// `mutate` may rewrite the manifest JSON text before it is written.
fn fixture(mutate: impl Fn(String) -> String) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let facade = tmp.path().join("facade");
    let dist = facade.join("dist");
    std::fs::create_dir_all(dist.join("sys")).unwrap();
    let mut workers = serde_json::Map::new();
    for svc in SERVICES {
        let bytes = module_bytes(svc);
        std::fs::write(dist.join(module_name(svc)), &bytes).unwrap();
        workers.insert(
            svc.to_string(),
            serde_json::json!({
                "entrypoint": "Entry",
                "needsBindingBackend": false,
                "mainModule": module_name(svc),
                "modules": [{
                    "name": module_name(svc),
                    "dist": format!("facade/dist/{}", module_name(svc)),
                    "source": format!("facade/{svc}.ts"),
                    "sourceSha256": sha256_hex(b"src"),
                    "sha256": sha256_hex(&bytes),
                }],
            }),
        );
    }
    let doc = serde_json::json!({
        "schemaVersion": 3,
        "generatedBy": "test",
        "nodeVersion": "24",
        "externals": {},
        "workers": workers,
    });
    let manifest = facade.join("manifest.json");
    std::fs::write(&manifest, mutate(doc.to_string())).unwrap();
    let cache_root = tmp.path().join("cache");
    vrt_securefs::ensure_secure_dir(&cache_root).unwrap();
    Fixture {
        manifest,
        cache: BundleCache::new(cache_root),
        dist,
        _tmp: tmp,
    }
}

struct CountingSource(AtomicUsize);
impl BundleSource for CountingSource {
    fn fetch(&self, _digest: &str) -> Result<Vec<u8>, ArtifactError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(ArtifactError::Missing)
    }
}

#[test]
fn seeds_all_modules_and_assembly_never_hits_network() {
    let f = fixture(|s| s);
    let n = seed_system_modules(&f.cache, &f.manifest).unwrap();
    assert_eq!(n, SERVICES.len());
    let src = CountingSource(AtomicUsize::new(0));
    for svc in SERVICES {
        let digest = sha256_hex(&module_bytes(svc));
        let got = f.cache.get_or_fetch_raw(&digest, &src).unwrap();
        assert_eq!(got, module_bytes(svc));
    }
    assert_eq!(src.0.load(Ordering::SeqCst), 0, "seeded => no network fetch");
}

#[test]
fn unseeded_cache_would_hit_network() {
    // Control: documents the pre-fix behaviour the seeding removes.
    let f = fixture(|s| s);
    let src = CountingSource(AtomicUsize::new(0));
    let digest = sha256_hex(&module_bytes("facade-kv"));
    assert!(f.cache.get_or_fetch_raw(&digest, &src).is_err());
    assert_eq!(src.0.load(Ordering::SeqCst), 1);
}

#[test]
fn digest_mismatch_refuses() {
    let f = fixture(|s| s);
    std::fs::write(f.dist.join(module_name("facade-kv")), b"tampered").unwrap();
    let err = seed_system_modules(&f.cache, &f.manifest).unwrap_err();
    assert!(matches!(err, SeedError::DigestMismatch { .. }), "{err}");
}

#[test]
fn missing_file_refuses() {
    let f = fixture(|s| s);
    std::fs::remove_file(f.dist.join(module_name("facade-pg"))).unwrap();
    let err = seed_system_modules(&f.cache, &f.manifest).unwrap_err();
    assert!(matches!(err, SeedError::Missing { .. }), "{err}");
}

#[test]
fn path_traversal_name_refuses() {
    // A parent-relative module name is rejected by the manifest parser:
    // never read outside dist.
    let f = fixture(|s| s.replace("sys/facade-kv.js", "../escape.js"));
    std::fs::write(
        f.dist.parent().unwrap().join("escape.js"),
        module_bytes("facade-kv"),
    )
    .unwrap();
    let err = seed_system_modules(&f.cache, &f.manifest).unwrap_err();
    assert!(matches!(err, SeedError::Manifest), "{err}");
}

#[test]
fn symlink_module_refuses() {
    let f = fixture(|s| s);
    let target = f.dist.parent().unwrap().join("real.js");
    std::fs::write(&target, module_bytes("facade-kv")).unwrap();
    let link = f.dist.join(module_name("facade-kv"));
    std::fs::remove_file(&link).unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let err = seed_system_modules(&f.cache, &f.manifest).unwrap_err();
    assert!(matches!(err, SeedError::PathRejected { .. }), "{err}");
}

#[test]
fn symlinked_directory_escape_refuses() {
    let f = fixture(|s| s);
    let outside = f.dist.parent().unwrap().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    for svc in SERVICES {
        std::fs::write(outside.join(format!("{svc}.js")), module_bytes(svc)).unwrap();
    }
    std::fs::remove_dir_all(f.dist.join("sys")).unwrap();
    std::os::unix::fs::symlink(&outside, f.dist.join("sys")).unwrap();
    let err = seed_system_modules(&f.cache, &f.manifest).unwrap_err();
    assert!(matches!(err, SeedError::PathRejected { .. }), "{err}");
}

#[test]
fn invalid_manifest_refuses() {
    let f = fixture(|_| "{not json".to_string());
    assert!(matches!(
        seed_system_modules(&f.cache, &f.manifest).unwrap_err(),
        SeedError::Manifest
    ));
}
