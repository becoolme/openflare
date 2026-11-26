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
    assert_eq!(parsed.facades.browser.service, "facade-browser");
    assert_eq!(parsed.facades.browser.entrypoint, "BrowserBinding");
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
        "facade-k2",
        "facade-email",
        "facade-images",
        "facade-vectorize",
        "vrt-egress",
        "vrt-no-network",
    ] {
        assert!(names.contains(required), "missing {required}");
    }
    for worker in &parsed.system_workers {
        // Backend-proxying facades reach binding-backend. Exceptions:
        // facade-router (only calls facade-assets + user worker) and
        // facade-qconsumer (only calls the tenant worker's queue()
        // service binding) and facade-ratelimit (in-process counter) and
        // the kitesurf pair (vendored engine + payload router — bindings
        // only, no binding-backend path) and facade-browser (the /v1
        // protocol layer — reaches only facade-kitesurf-front) — none of
        // them sees the backend.
        assert_eq!(
            worker.needs_binding_backend,
            worker.service_name.starts_with("facade-")
                && worker.service_name != "facade-router"
                && worker.service_name != "facade-qconsumer"
                && worker.service_name != "facade-ratelimit"
                && worker.service_name != "facade-images"
                && worker.service_name != "facade-browser"
                && !worker.service_name.starts_with("facade-kitesurf"),
            "{} binding-backend flag",
            worker.service_name
        );
        assert!(worker.modules.iter().any(|m| m.name == worker.main_module));
        for module in &worker.modules {
            // kitesurf text/data payloads: .css/.dat/.woff2/.bin beyond js+wasm.
            assert!(
                module.name.ends_with(".js")
                    || module.name.ends_with(".wasm")
                    || module.name.ends_with(".css")
                    || module.name.ends_with(".dat")
                    || module.name.ends_with(".woff2")
                    || module.name.ends_with(".bin"),
                "module kind suffix: {}",
                module.name
            );
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
        "schemaVersion": 3,
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
        "facade-k2",
        "facade-email",
        "facade-images",
        "facade-vectorize",
        "facade-browser",
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
        "schemaVersion": 3,
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
fn cpu_ms_values_match_manifest_tiers() {
    let parsed = parse_facade_manifest(MANIFEST_JSON).expect("real manifest parses");
    let get = |name: &str| {
        parsed
            .system_workers
            .iter()
            .find(|w| w.service_name == name)
            .unwrap_or_else(|| panic!("{name} missing"))
    };
    // Platform-declared budgets — the tiers gen-manifest.mjs emits.
    assert_eq!(get("facade-images").cpu_ms, 10_000);
    assert_eq!(get("vrt-egress").cpu_ms, 2_000);
    // memoryMb mirrors the same tiering: the decode/composite buffers
    // concentrate in facade-images; thin transports get tenant parity.
    assert_eq!(get("facade-images").memory_mb, 768);
    assert_eq!(get("vrt-egress").memory_mb, 192);
    // subRequests: the fetch fan-out tier (src + draw overlays + fonts)
    // versus the thin ≤2 binding-backend-call tier.
    assert_eq!(get("facade-images").sub_requests, 64);
    assert_eq!(get("vrt-egress").sub_requests, 64);
    for thin in [
        "facade-kv",
        "facade-r2",
        "facade-assets",
        "facade-pg",
        "facade-d1",
        "facade-queue",
        "facade-k2",
        "facade-email",
        "facade-vectorize",
        "facade-ratelimit",
        "facade-qconsumer",
        "facade-router",
        "facade-browser",
        "vrt-no-network",
    ] {
        assert_eq!(get(thin).cpu_ms, 1_000, "{thin}");
        assert_eq!(get(thin).memory_mb, 128, "{thin}");
        assert_eq!(get(thin).sub_requests, 8, "{thin}");
    }
}

#[test]
fn cpu_ms_parsing_and_bounds() {
    // Absent → unset (no `limits` group rendered downstream).
    let workers = minimal();
    let parsed = parse_facade_manifest(&wire(workers)).unwrap();
    assert!(
        parsed
            .system_workers
            .iter()
            .all(|w| w.cpu_ms == 0 && w.memory_mb == 0 && w.sub_requests == 0),
        "absent budgets must stay unset"
    );

    // 0 is accepted but stays unset — same semantic as the capnp
    // field and WorkerQuotas: zero never renders as a budget.
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["cpuMs"] = serde_json::json!(0);
    w["memoryMb"] = serde_json::json!(0);
    w["subRequests"] = serde_json::json!(0);
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    let parsed = parse_facade_manifest(&wire(workers)).unwrap();
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    assert_eq!((kv.cpu_ms, kv.memory_mb, kv.sub_requests), (0, 0, 0));

    // Same shape on all dimensions: 1 ok, protocol cap ok, cap+1 is
    // the field's Identifier error, wrong types fail at serde.
    for (key, cap, over) in [
        ("cpuMs", 300_000, 300_001),
        ("memoryMb", 4_096, 4_097),
        ("subRequests", 10_000, 10_001),
    ] {
        let ident = format!("workers.{key}");
        for (value, expect) in [
            (serde_json::json!(1), Ok(1)),
            (serde_json::json!(cap), Ok(cap)), // protocol cap edge
            (serde_json::json!(over), Err(ident.as_str())),
            // Wrong types fail at the serde boundary, not the bound check.
            (serde_json::json!(-1), Err("systemManifest")),
            (serde_json::json!("500"), Err("systemManifest")),
            (serde_json::json!(1.5), Err("systemManifest")),
        ] {
            let mut workers = minimal();
            let mut w = worker(serde_json::json!([module("facade/x.js")]));
            w[key] = value.clone();
            workers
                .as_object_mut()
                .unwrap()
                .insert("facade-kv".into(), w);
            match expect {
                Ok(v) => {
                    let parsed = parse_facade_manifest(&wire(workers)).unwrap();
                    let kv = parsed
                        .system_workers
                        .iter()
                        .find(|w| w.service_name == "facade-kv")
                        .unwrap();
                    let got = if key == "cpuMs" {
                        kv.cpu_ms
                    } else if key == "memoryMb" {
                        kv.memory_mb
                    } else {
                        kv.sub_requests
                    };
                    assert_eq!(got, v, "{key}");
                }
                Err(field) => {
                    assert!(
                        matches!(
                            parse_facade_manifest(&wire(workers)),
                            Err(ConfigError::Identifier(f)) if f == field
                        ),
                        "{key}={value}"
                    );
                }
            }
        }
    }
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

// Schema v3: typed module kinds, per-worker compatFlags/bindings/outbound —
// the surface a wasm fetch engine (facade-kitesurf) declares.
#[test]
fn v3_module_types_parse_and_stay_consistent() {
    fn typed_module(name: &str, ty: &str) -> serde_json::Value {
        let mut m = module(name);
        m["type"] = serde_json::json!(ty);
        m
    }

    let mut workers = minimal();
    workers.as_object_mut().unwrap().insert(
        "facade-kv".into(),
        worker(serde_json::json!([
            module("facade/x.js"),
            typed_module("facade/payload.css", "text"),
            typed_module("facade/blob.dat", "data"),
        ])),
    );
    let parsed = parse_facade_manifest(&wire(workers)).expect("v3 module types parse");
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    use vrt_config::ModuleType;
    assert_eq!(kv.modules[1].module_type, ModuleType::Text);
    assert_eq!(kv.modules[2].module_type, ModuleType::Data);

    // Byte-serving payload workers embed vendored `.wasm` payloads as
    // `data`/`text` — allowed. `wasm` claims on non-.wasm names and
    // unknown types fail closed — the capnp union member must be honest.
    let mut workers = minimal();
    workers.as_object_mut().unwrap().insert(
        "facade-kv".into(),
        worker(serde_json::json!([
            module("facade/x.js"),
            typed_module("facade/payload.bin.wasm", "data"),
            typed_module("facade/notes.wasm", "text"),
        ])),
    );
    let parsed = parse_facade_manifest(&wire(workers)).expect(".wasm as data/text parses");
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    assert_eq!(kv.modules[1].module_type, ModuleType::Data);
    assert_eq!(kv.modules[2].module_type, ModuleType::Text);

    for (name, ty) in [("facade/x.js", "wasm"), ("facade/x.css", "bogus")] {
        let mut workers = minimal();
        workers.as_object_mut().unwrap().insert(
            "facade-kv".into(),
            worker(serde_json::json!([module("facade/x.js"), typed_module(name, ty)])),
        );
        assert!(
            parse_facade_manifest(&wire(workers)).is_err(),
            "{name} type={ty} must fail"
        );
    }
}

#[test]
fn v3_compatibility_flags_stable_only() {
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["compatibilityFlags"] = serde_json::json!(["nodejs_compat", "global_fetch_strictly_public"]);
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    let parsed = parse_facade_manifest(&wire(workers)).expect("stable flags parse");
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    assert_eq!(
        kv.compatibility_flags,
        vec!["nodejs_compat".to_string(), "global_fetch_strictly_public".to_string()]
    );

    // Experimental/unknown flags fail closed.
    for flag in ["experimental", "unsafe_module_scope", "made_up_flag"] {
        let mut workers = minimal();
        let mut w = worker(serde_json::json!([module("facade/x.js")]));
        w["compatibilityFlags"] = serde_json::json!([flag]);
        workers
            .as_object_mut()
            .unwrap()
            .insert("facade-kv".into(), w);
        assert!(
            parse_facade_manifest(&wire(workers)).is_err(),
            "flag {flag} must be rejected"
        );
    }
}

#[test]
fn v3_outbound_public_only_requires_strictly_public() {
    // publicOnly + global_fetch_strictly_public → PublicOnly.
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["compatibilityFlags"] = serde_json::json!(["global_fetch_strictly_public"]);
    w["outbound"] = serde_json::json!("publicOnly");
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    let parsed = parse_facade_manifest(&wire(workers)).expect("publicOnly parses");
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    assert_eq!(kv.outbound, vrt_config::SystemOutbound::PublicOnly);
    // Default stays deny.
    let kv2 = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-r2")
        .unwrap();
    assert_eq!(kv2.outbound, vrt_config::SystemOutbound::Deny);

    // publicOnly WITHOUT the flag, and unknown values, fail closed.
    for (flags, outbound) in [
        (serde_json::json!([]), "publicOnly"),
        (serde_json::json!(["nodejs_compat"]), "publicOnly"),
        (serde_json::json!(["global_fetch_strictly_public"]), "internet"),
    ] {
        let mut workers = minimal();
        let mut w = worker(serde_json::json!([module("facade/x.js")]));
        w["compatibilityFlags"] = flags.clone();
        w["outbound"] = serde_json::json!(outbound);
        workers
            .as_object_mut()
            .unwrap()
            .insert("facade-kv".into(), w);
        assert!(
            parse_facade_manifest(&wire(workers)).is_err(),
            "outbound={outbound} flags={flags} must be rejected"
        );
    }
}

#[test]
fn v3_bindings_validation() {
    // Happy path — workerLoader + service on one worker.
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["bindings"] = serde_json::json!([
        { "type": "workerLoader", "name": "LOADER", "maxWorkers": 32 },
        { "type": "service", "name": "FONT_ASSETS", "service": "facade-r2", "entrypoint": "R2Transport" },
    ]);
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    let parsed = parse_facade_manifest(&wire(workers)).expect("bindings parse");
    let kv = parsed
        .system_workers
        .iter()
        .find(|w| w.service_name == "facade-kv")
        .unwrap();
    assert_eq!(kv.bindings.len(), 2);
    assert!(matches!(
        &kv.bindings[0],
        vrt_config::SystemBinding::WorkerLoader { name, max_workers: 32 } if name == "LOADER"
    ));
    assert!(matches!(
        &kv.bindings[1],
        vrt_config::SystemBinding::Service { name, service, entrypoint }
            if name == "FONT_ASSETS" && service == "facade-r2" && entrypoint == "R2Transport"
    ));

    // workerLoader bounds: 0 and >32 rejected; bad name rejected.
    for max_workers in [0, 33, u32::MAX] {
        let mut workers = minimal();
        let mut w = worker(serde_json::json!([module("facade/x.js")]));
        w["bindings"] = serde_json::json!([
            { "type": "workerLoader", "name": "LOADER", "maxWorkers": max_workers },
        ]);
        workers
            .as_object_mut()
            .unwrap()
            .insert("facade-kv".into(), w);
        assert!(
            parse_facade_manifest(&wire(workers)).is_err(),
            "maxWorkers={max_workers} must be rejected"
        );
    }
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["bindings"] = serde_json::json!([
        { "type": "workerLoader", "name": "9BAD", "maxWorkers": 4 },
    ]);
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    assert!(parse_facade_manifest(&wire(workers)).is_err());

    // service: the target must be a declared system worker — phantom
    // services, tenant names and bad entrypoints all fail closed.
    for (service, entrypoint) in [
        ("facade-missing", "Entrypoint"),
        ("tenant-worker", "Entrypoint"),
        ("facade-r2", "9bad"),
        ("facade-r2", "no/slash"),
    ] {
        let mut workers = minimal();
        let mut w = worker(serde_json::json!([module("facade/x.js")]));
        w["bindings"] = serde_json::json!([
            { "type": "service", "name": "FONT_ASSETS", "service": service, "entrypoint": entrypoint },
        ]);
        workers
            .as_object_mut()
            .unwrap()
            .insert("facade-kv".into(), w);
        assert!(
            parse_facade_manifest(&wire(workers)).is_err(),
            "service={service} entrypoint={entrypoint} must be rejected"
        );
    }

    // Duplicate binding names fail closed.
    let mut workers = minimal();
    let mut w = worker(serde_json::json!([module("facade/x.js")]));
    w["bindings"] = serde_json::json!([
        { "type": "workerLoader", "name": "LOADER", "maxWorkers": 4 },
        { "type": "service", "name": "LOADER", "service": "facade-r2", "entrypoint": "R2Transport" },
    ]);
    workers
        .as_object_mut()
        .unwrap()
        .insert("facade-kv".into(), w);
    assert!(parse_facade_manifest(&wire(workers)).is_err());
}
