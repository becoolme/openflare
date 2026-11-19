//! W1: vendored workflows engine integrity — the bytes shipped under
//! `system-workers/workflows/` must match `SHA256SUMS` exactly. A
//! silently edited vendor file fails closed here (the deploy path
//! would still emit it with a self-consistent digest).

use std::collections::BTreeMap;

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(bytes))
}

/// Parse `SHA256SUMS` — strict `sha  name` lines, no extras.
fn sums() -> BTreeMap<String, String> {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/system-workers/workflows/SHA256SUMS"
    ))
    .expect("SHA256SUMS readable");
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let (sha, name) = l
                .split_once("  ")
                .unwrap_or_else(|| panic!("malformed SHA256SUMS line: {l:?}"));
            assert_eq!(sha.len(), 64, "sha length for {name}");
            (name.to_string(), sha.to_string())
        })
        .collect()
}

#[test]
fn vendored_workflows_files_match_sha256sums() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/system-workers/workflows");
    let sums = sums();
    assert_eq!(
        sums.len(),
        3,
        "SHA256SUMS covers exactly the three artifacts"
    );
    for (name, sha) in &sums {
        let bytes =
            std::fs::read(format!("{dir}/{name}")).unwrap_or_else(|_| panic!("{name} readable"));
        assert_eq!(
            &sha256_hex(&bytes),
            sha,
            "{name} content drifted from SHA256SUMS — upgrade via VENDOR.md, never edit in place"
        );
    }
    // And the crate's compiled-in copies are the same bytes.
    assert_eq!(
        vrt_config::workflows::engine_worker_sha256(),
        sums["binding.worker.js"]
    );
    assert_eq!(
        sha256_hex(vrt_config::workflows::WRAPPED_BINDING_JS),
        sums["wrapped-binding.worker.js"]
    );
}

#[test]
fn vendored_engine_exports_expected_entrypoints() {
    let src = std::str::from_utf8(vrt_config::workflows::ENGINE_WORKER_JS).unwrap();
    assert!(src.contains("Engine"), "engine bundle exports Engine class");
    assert!(
        src.contains("WorkflowBinding"),
        "engine bundle exports WorkflowBinding entrypoint"
    );
    let wrapped = std::str::from_utf8(vrt_config::workflows::WRAPPED_BINDING_JS).unwrap();
    assert!(
        wrapped.contains("makeBinding"),
        "wrapped bundle exports makeBinding factory"
    );
}

/// Static import-closure check (W1-r2): every `import`/`from`/`import()`
/// specifier in the vendored closure must resolve to either
///   (a) a workerd built-in module — the whitelist below cites the
///       specifiers workerd serves unconditionally, or
///   (b) a module VRT itself registers (extension-internal modules and
///       the emitted `workflows.mjs` engine module).
/// W1-r1 shipped without `miniflare:zod`; every engine service died at
/// config load with `No such module "miniflare:zod"` and took the whole
/// tenant down. If a future vendor upgrade adds imports, this fails
/// until the upstream target is vendored + registered the same way.
#[test]
fn vendored_import_closure_is_fully_resolved() {
    use std::collections::{BTreeSet, VecDeque};
    // Module bytes by resolved name — mirrors the registrations in
    // capnp.rs (extensions) and the engine service's `workflows.mjs`.
    let mut modules: BTreeMap<&'static str, &'static [u8]> = BTreeMap::new();
    modules.insert("workflows.mjs", vrt_config::workflows::ENGINE_WORKER_JS);
    modules.insert(
        vrt_config::workflows::WRAPPED_BINDING_MODULE,
        vrt_config::workflows::WRAPPED_BINDING_JS,
    );
    modules.insert(
        vrt_config::workflows::ZOD_MODULE_NAME,
        vrt_config::workflows::ZOD_WORKER_JS,
    );
    // workerd built-ins the vendored closure may import:
    //   "cloudflare:workers"  — DurableObject/RpcTarget/WorkerEntrypoint/
    //                           WorkflowEntrypoint, always present.
    //   "node-internal:*"     — workerd's own internal module registry;
    //                           extension-internal modules resolve it
    //                           without nodejs_compat (same mechanism
    //                           miniflare relies on for zod/sha-worker).
    fn is_builtin(spec: &str) -> bool {
        spec == "cloudflare:workers" || spec.starts_with("node-internal:")
    }
    // Extract `from "x"` / `import "x"` / `import("x")` specifiers —
    // esbuild bundles use plain double-quoted static imports.
    fn imports_of(src: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for pat in ["from \"", "import \"", "import(\""] {
            let mut rest = src;
            while let Some(i) = rest.find(pat) {
                let after = &rest[i + pat.len()..];
                if let Some(end) = after.find('"') {
                    out.insert(after[..end].to_string());
                }
                rest = after;
            }
        }
        out
    }
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<&'static str> = modules.keys().copied().collect();
    while let Some(name) = queue.pop_front() {
        if !seen.insert(name.to_string()) {
            continue;
        }
        let src = std::str::from_utf8(modules[name]).expect("vendored utf8");
        for spec in imports_of(src) {
            if is_builtin(&spec) {
                continue;
            }
            assert!(
                modules.contains_key(spec.as_str()),
                "unresolved import {spec:?} in vendored module {name:?} — \
                 vendor + register the upstream target (VENDOR.md), \
                 never leave it to runtime failure"
            );
            queue.push_back(*modules.get_key_value(spec.as_str()).unwrap().0);
        }
    }
    // The names the closure covers must be exactly the registered set —
    // a dead vendored file is as much a drift bug as a missing one.
    assert_eq!(
        seen.len(),
        modules.len(),
        "vendored module unreachable from closure: {seen:?}"
    );
}
