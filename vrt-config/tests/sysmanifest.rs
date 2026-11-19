//! `facade/manifest.json` → `SystemWorkerRef`/`FacadeRefs` contract tests.
//! The positive case loads the real generated manifest and verifies every
//! declared module digest against the file bytes, so a stale manifest fails
//! here even if the JS-side check is skipped.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use vrt_config::{parse_facade_manifest, ConfigError};

const MANIFEST_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../facade/manifest.json"
));

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .unwrap()
}

fn module_rows() -> Vec<(String, String, String, String, String)> {
    let wire: serde_json::Value = serde_json::from_str(MANIFEST_JSON).unwrap();
    let mut out = Vec::new();
    for (_service, worker) in wire["workers"].as_object().unwrap() {
        for module in worker["modules"].as_array().unwrap() {
            out.push((
                module["name"].as_str().unwrap().to_owned(),
                module["dist"].as_str().unwrap().to_owned(),
                module["source"].as_str().unwrap().to_owned(),
                module["sha256"].as_str().unwrap().to_owned(),
                module["sourceSha256"].as_str().unwrap().to_owned(),
            ));
        }
    }
    out
}

#[test]
fn parses_generated_manifest_into_system_workers() {
    let parsed = parse_facade_manifest(MANIFEST_JSON).expect("real manifest parses");

    assert_eq!(parsed.facades.kv.service, "facade-kv");
    assert_eq!(parsed.facades.kv.entrypoint, "KVNamespace");
    assert_eq!(parsed.facades.r2.service, "facade-r2");
    assert_eq!(parsed.facades.r2.entrypoint, "R2Transport");
    assert_eq!(parsed.facades.assets.service, "facade-assets");
    assert_eq!(parsed.facades.assets.entrypoint, "AssetTransport");
    assert_eq!(parsed.facades.pg.service, "facade-pg");
    assert_eq!(parsed.facades.pg.entrypoint, "PgTransport");
    // T6: the egress endpoint is required and addressed by its real
    // entrypoint — never a placeholder.
    assert_eq!(parsed.egress.service, "vrt-egress");
    assert_eq!(parsed.egress.entrypoint, "EgressProxy");
    // T6 review: deny-all outbound target for non-egress system workers.
    assert_eq!(parsed.deny_outbound.service, "vrt-no-network");
    assert_eq!(parsed.deny_outbound.entrypoint, "DenyAll");

    let names: BTreeSet<&str> = parsed
        .system_workers
        .iter()
        .map(|w| w.service_name.as_str())
        .collect();
    for required in [
        "facade-kv",
        "facade-r2",
        "facade-assets",
        "facade-pg",
        "facade-d1",
        "facade-queue",
        "facade-ratelimit",
        "vrt-egress",
        "vrt-no-network",
    ] {
        assert!(names.contains(required), "missing {required}");
    }
    for worker in &parsed.system_workers {
        // Backend-proxying facades reach binding-backend. Exceptions:
        // facade-router (only calls facade-assets + user worker) and
        // facade-qconsumer (only calls the tenant worker's queue()
        // service binding) and facade-ratelimit (in-process counter) — none
        // of them sees the backend path.
        assert_eq!(
            worker.needs_binding_backend,
            worker.service_name.starts_with("facade-")
                && worker.service_name != "facade-router"
                && worker.service_name != "facade-qconsumer"
                && worker.service_name != "facade-ratelimit",
            "{} binding-backend flag",
            worker.service_name
        );
        assert!(worker.modules.iter().any(|m| m.name == worker.main_module));
        for module in &worker.modules {
            assert!(module.name.ends_with(".js"));
            assert_eq!(module.digest.len(), 64);
        }
    }
    // Vendored transports must be inside the facade worker graphs.
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    assert!(kv
        .modules
        .iter()
        .any(|m| m.name == "third_party/open-compute/facade/kv/transport.js"));
}

#[test]
fn manifest_digests_match_dist_and_source_bytes() {
    let root = repo_root();
    let mut saw_dist_differ = false;
    for (name, dist, source, sha256, source_sha256) in module_rows() {
        // `sha256` must be the digest of the published dist bytes — what
        // workerd embeds via `sys/<digest>` — never of the .ts source.
        let dist_bytes = std::fs::read(root.join(&dist))
            .unwrap_or_else(|e| panic!("{name}: cannot read {dist}: {e}"));
        assert_eq!(
            hex::encode(Sha256::digest(&dist_bytes)),
            sha256,
            "{name}: sha256 must cover dist bytes"
        );
        let source_bytes = std::fs::read(root.join(&source))
            .unwrap_or_else(|e| panic!("{name}: cannot read {source}: {e}"));
        assert_eq!(
            hex::encode(Sha256::digest(&source_bytes)),
            source_sha256,
            "{name}: sourceSha256 must cover the .ts source"
        );
        assert_eq!(
            dist,
            format!("facade/dist/{name}"),
            "{name}: dist path must mirror the logical name under dist/"
        );
        if sha256 != source_sha256 {
            saw_dist_differ = true;
        }
    }
    assert!(
        saw_dist_differ,
        "no module differs between source and dist; digest mapping is untested"
    );
}

fn wire(workers: serde_json::Value) -> String {
    serde_json::json!({
        "schemaVersion": 2,
        "generatedBy": "test",
        "nodeVersion": "24",
        "externals": { "cloudflare:workers": "workerd-provided external" },
        "workers": workers,
    })
    .to_string()
}

fn worker(modules: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "entrypoint": "KVNamespace",
        "needsBindingBackend": true,
        "mainModule": "facade/x.js",
        "modules": modules,
    })
}

fn module(name: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "dist": "facade/dist/facade/x.js",
        "source": "facade/x.ts",
        "sourceSha256": "b".repeat(64),
        "sha256": "a".repeat(64),
    })
}

fn minimal() -> serde_json::Value {
    let mut workers = serde_json::Map::new();
    for service in [
        "facade-kv",
        "facade-r2",
        "facade-assets",
        "facade-pg",
        "facade-d1",
        "facade-queue",
        "facade-ratelimit",
        "vrt-egress",
        "vrt-no-network",
    ] {
        workers.insert(
            service.to_owned(),
            worker(serde_json::json!([module("facade/x.js")])),
        );
    }
    serde_json::Value::Object(workers)
}

#[test]
fn rejects_bad_schema_version_and_top_level_shape() {
    let bad = serde_json::json!({
        "schemaVersion": 1,
        "generatedBy": "test",
        "nodeVersion": "24",
        "workers": minimal(),
    })
    .to_string();
    assert!(matches!(
        parse_facade_manifest(&bad),
        Err(ConfigError::Identifier("systemManifest.schemaVersion"))
    ));
    assert!(parse_facade_manifest("not json").is_err());
    let extra = serde_json::json!({
        "schemaVersion": 2,
        "generatedBy": "test",
        "nodeVersion": "24",
        "workers": minimal(),
        "surprise": true,
    })
    .to_string();
    assert!(parse_facade_manifest(&extra).is_err());
}

#[test]
fn rejects_missing_required_facade() {
    let mut workers = minimal();
    workers.as_object_mut().unwrap().remove("facade-pg");
    assert!(matches!(
        parse_facade_manifest(&wire(workers)),
        Err(ConfigError::ServiceReference("facades.required"))
    ));
}

#[test]
fn rejects_missing_egress() {
    let mut workers = minimal();
    workers.as_object_mut().unwrap().remove("vrt-egress");
    assert!(matches!(
        parse_facade_manifest(&wire(workers)),
        Err(ConfigError::ServiceReference("egress.required"))
    ));
}

#[test]
fn rejects_missing_deny_outbound() {
    let mut workers = minimal();
    workers.as_object_mut().unwrap().remove("vrt-no-network");
    assert!(matches!(
        parse_facade_manifest(&wire(workers)),
        Err(ConfigError::ServiceReference("denyOutbound.required"))
    ));
}

#[test]
fn rejects_worker_field_violations() {
    // mainModule not present in modules.
    let mut workers = minimal();
    workers.as_object_mut().unwrap().insert(
        "facade-kv".into(),
        worker(serde_json::json!([module("facade/other.js")])),
    );
    assert!(parse_facade_manifest(&wire(workers)).is_err());

    // duplicate module names.
    let mut workers = minimal();
    workers.as_object_mut().unwrap().insert(
        "facade-kv".into(),
        worker(serde_json::json!([
            module("facade/x.js"),
            module("facade/x.js")
        ])),
    );
    assert!(parse_facade_manifest(&wire(workers)).is_err());

    // non-hex digest.
    let mut workers = minimal();
    let mut m = module("facade/x.js");
    m["sha256"] = serde_json::json!("zz");
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), worker(serde_json::json!([m])));
    assert!(matches!(
        parse_facade_manifest(&wire(workers)),
        Err(ConfigError::Digest("workers.modules.sha256"))
    ));

    // path-escaping module name.
    let mut workers = minimal();
    workers.as_object_mut().unwrap().insert(
        "facade-kv".into(),
        worker(serde_json::json!([
            module("../outside.js"),
            module("facade/x.js")
        ])),
    );
    assert!(parse_facade_manifest(&wire(workers)).is_err());

    // invalid entrypoint and service name.
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["entrypoint"] = serde_json::json!("9bad");
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    assert!(parse_facade_manifest(&wire(workers)).is_err());

    let mut workers = minimal();
    let w = workers
        .as_object_mut()
        .unwrap()
        .remove("facade-kv")
        .unwrap();
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade/evil".into(), w);
    assert!(parse_facade_manifest(&wire(workers)).is_err());
}
