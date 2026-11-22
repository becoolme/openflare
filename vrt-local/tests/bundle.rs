//! Bundler integration — wrangler-source projects compile `main` +
//! its import graph into a single esModule at gather time (CF esbuild
//! semantics), while `no_bundle` keeps the dir-scan module table.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use vrt_local::worker_pkg::{gather_dir_for_env, DeployOverrides};

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("of-bundle-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn module_names(d: &Path) -> Vec<String> {
    gather_dir_for_env(d, None)
        .unwrap()
        .files
        .keys()
        .cloned()
        .collect()
}

fn pack_types(d: &Path) -> BTreeMap<String, String> {
    let pkg = vrt_local::worker_pkg::pack(d, &DeployOverrides::default(), "local").unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&pkg.stored_document).unwrap();
    doc["modules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["name"].as_str().unwrap().to_string(),
                m["type"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn ts_entry_bundles_local_deps_into_single_module() {
    let d = dir("ts");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.ts\"\ncompatibility_date = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::write(d.join("helper.ts"), "export const x: number = 1;\n").unwrap();
    fs::write(
        d.join("index.ts"),
        "import { x } from './helper';\nexport default { fetch() { return new Response(String(x)) } };\n",
    )
    .unwrap();
    assert_eq!(module_names(&d), vec!["index.ts".to_string()]);
    let types = pack_types(&d);
    assert_eq!(types["index.ts"], "esModule");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn rule_typed_import_stays_external_wire_module() {
    let d = dir("rule");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"src/index.ts\"\ncompatibility_date = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::create_dir_all(d.join("src")).unwrap();
    fs::write(d.join("src/a.txt"), "hello").unwrap();
    fs::write(
        d.join("src/index.ts"),
        "import t from './a.txt';\nexport default { fetch() { return new Response(t) } };\n",
    )
    .unwrap();
    let g = gather_dir_for_env(&d, None).unwrap();
    // Rolldown emits the same-dir import as bare `a.txt` — an exact
    // module-name match for workerd.
    assert_eq!(
        g.files.keys().cloned().collect::<Vec<_>>(),
        vec!["a.txt".to_string(), "src/index.ts".to_string()]
    );
    let main_src = String::from_utf8(g.files["src/index.ts"].clone()).unwrap();
    assert!(main_src.contains("a.txt"), "{main_src}");
    let types = pack_types(&d);
    assert_eq!(types["a.txt"], "text");
    assert_eq!(types["src/index.ts"], "esModule");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn runtime_modules_stay_external_in_bundle() {
    let d = dir("rt");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.ts\"\ncompatibility_date = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::write(
        d.join("index.ts"),
        "import { connect } from 'cloudflare:sockets';\nimport crypto from 'node:crypto';\nexport default { fetch() { return new Response(typeof connect + typeof crypto.randomUUID) } };\n",
    )
    .unwrap();
    let g = gather_dir_for_env(&d, None).unwrap();
    let main_src = String::from_utf8(g.files["index.ts"].clone()).unwrap();
    assert!(main_src.contains("cloudflare:sockets"), "{main_src}");
    assert!(main_src.contains("node:crypto"), "{main_src}");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn no_bundle_keeps_dir_scan_module_table() {
    let d = dir("nobundle");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.js\"\ncompatibility_date = \"2026-01-01\"\nno_bundle = true\n",
    )
    .unwrap();
    fs::write(d.join("index.js"), "import { x } from './helper.js';\nexport default { fetch() { return new Response(String(x)) } };\n").unwrap();
    fs::write(d.join("helper.js"), "export const x = 1;\n").unwrap();
    let names = module_names(&d);
    assert_eq!(names, vec!["helper.js".to_string(), "index.js".to_string()]);
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn find_additional_modules_ships_unimported_rule_files() {
    let d = dir("fam");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.js\"\ncompatibility_date = \"2026-01-01\"\nfind_additional_modules = true\n",
    )
    .unwrap();
    fs::write(
        d.join("index.js"),
        "export default { fetch() { return new Response('ok') } };\n",
    )
    .unwrap();
    // `a.txt` is never imported — only find_additional_modules brings
    // it into the module table (implicit `**/*.txt` → Text rule).
    fs::write(d.join("a.txt"), "x").unwrap();
    let names = module_names(&d);
    assert_eq!(names, vec!["a.txt".to_string(), "index.js".to_string()]);
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn unimported_rule_files_excluded_without_find_additional() {
    let d = dir("nofam");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.js\"\ncompatibility_date = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::write(
        d.join("index.js"),
        "export default { fetch() { return new Response('ok') } };\n",
    )
    .unwrap();
    fs::write(d.join("a.txt"), "x").unwrap();
    fs::write(d.join("notes.md"), "x").unwrap();
    assert_eq!(module_names(&d), vec!["index.js".to_string()]);
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn npm_dependency_bundles_from_node_modules() {
    let d = dir("dep");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.js\"\ncompatibility_date = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::create_dir_all(d.join("node_modules/leftpad")).unwrap();
    fs::write(
        d.join("node_modules/leftpad/package.json"),
        r#"{"name":"leftpad","version":"1.0.0","main":"index.js"}"#,
    )
    .unwrap();
    fs::write(
        d.join("node_modules/leftpad/index.js"),
        "export function pad(s) { return '  ' + s }\n",
    )
    .unwrap();
    fs::write(
        d.join("index.js"),
        "import { pad } from 'leftpad';\nexport default { fetch() { return new Response(pad('x')) } };\n",
    )
    .unwrap();
    let g = gather_dir_for_env(&d, None).unwrap();
    assert_eq!(
        g.files.keys().cloned().collect::<Vec<_>>(),
        vec!["index.js".to_string()]
    );
    let main_src = String::from_utf8(g.files["index.js"].clone()).unwrap();
    assert!(main_src.contains("pad"), "{main_src}");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn unresolved_dependency_fails_closed() {
    let d = dir("unres");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.js\"\ncompatibility_date = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::write(
        d.join("index.js"),
        "import { x } from 'not-installed-pkg';\nexport default { fetch() { return new Response(String(x)) } };\n",
    )
    .unwrap();
    let e = match gather_dir_for_env(&d, None) {
        Ok(_) => panic!("unresolved dep must fail"),
        Err(e) => e,
    };
    let msg = e.to_string();
    assert!(msg.contains("not-installed-pkg"), "{msg}");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn jsx_and_define_and_minify_wire_through() {
    let d = dir("opts");
    fs::write(
        d.join("wrangler.toml"),
        "name = \"w\"\nmain = \"index.jsx\"\ncompatibility_date = \"2026-01-01\"\nminify = true\njsx_factory = \"h\"\n\n[define]\n__BUILD__ = \"\\\"test\\\"\"\n",
    )
    .unwrap();
    fs::write(
        d.join("index.jsx"),
        "const el = <div id={__BUILD__} />;\nexport default { fetch() { return new Response(el.props.id) } };\n",
    )
    .unwrap();
    let g = gather_dir_for_env(&d, None).unwrap();
    let main_src = String::from_utf8(g.files["index.jsx"].clone()).unwrap();
    // `define` folded the identifier; classic pragma emitted `h(...)`
    // calls; minify stripped whitespace (and may re-quote strings).
    assert!(main_src.contains("h("), "{main_src}");
    assert!(main_src.contains("test"), "{main_src}");
    assert!(!main_src.contains("__BUILD__"), "{main_src}");
    let _ = fs::remove_dir_all(&d);
}

#[test]
fn native_vrt_toml_stays_unbundled_without_bundle_key() {
    let d = dir("vrt");
    fs::write(
        d.join("vrt.toml"),
        "name = \"w\"\nmainModule = \"index.js\"\ncompatibilityDate = \"2026-01-01\"\n",
    )
    .unwrap();
    fs::write(d.join("index.js"), "import { x } from './helper.js';\nexport default { fetch() { return new Response(String(x)) } };\n").unwrap();
    fs::write(d.join("helper.js"), "export const x = 1;\n").unwrap();
    let names = module_names(&d);
    assert_eq!(names, vec!["helper.js".to_string(), "index.js".to_string()]);
    let _ = fs::remove_dir_all(&d);
}
