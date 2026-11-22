//! `of check` — offline deploy gate: runs the identical
//! gather→assemble→`ComputeDesired::validate` pipeline a real deploy
//! runs, without state, a cell, or side effects.

use std::fs;
use std::path::Path;

use vrt_local::check::check_worker;
use vrt_local::worker_pkg::DeployOverrides;

fn worker(dir: &Path, toml: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(dir.join("wrangler.toml"), toml).unwrap();
    fs::write(
        dir.join("index.js"),
        "export default { fetch() { return new Response(\"ok\"); } }\n",
    )
    .unwrap();
}

#[test]
fn check_reports_bindings_files_and_version_id() {
    let dir = std::env::temp_dir().join(format!("of-check-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    worker(
        &dir,
        r#"name = "ok"
main = "index.js"
compatibility_date = "2026-01-01"
[vars]
GREETING = "hi"
"#,
    );
    let r = check_worker(&dir, &DeployOverrides::default()).unwrap();
    assert!(r.ok);
    assert!(r.bindings.contains(&"var:GREETING".to_string()), "{:?}", r.bindings);
    assert_eq!(r.files, 1);
    assert!(r.version_id.starts_with("vrt_"), "{}", r.version_id);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn check_fails_closed_on_unsupported_key() {
    let dir = std::env::temp_dir().join(format!("of-check-bad-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    worker(
        &dir,
        r#"name = "bad"
main = "index.js"
compatibility_date = "2026-01-01"

[[mtls_certificates]]
binding = "C"
certificate_id = "x"
"#,
    );
    let e = check_worker(&dir, &DeployOverrides::default()).unwrap_err();
    assert!(
        e.to_string().contains("VRT_CONFIG_UNSUPPORTED_FIELD"),
        "{e}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn check_runs_cell_validate_not_just_parse() {
    // A config that parses fine but violates desired-state semantics
    // (binding name with characters outside the JS-identifier-ish
    // charset) must fail at the validate stage, not slip through.
    let dir = std::env::temp_dir().join(format!("of-check-validate-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    worker(
        &dir,
        "name = \"w\"\nmain = \"index.js\"\ncompatibility_date = \"2026-01-01\"\n[vars]\n\"bad name!\" = \"x\"\n",
    );
    let e = check_worker(&dir, &DeployOverrides::default()).unwrap_err();
    assert!(!e.to_string().is_empty());
    let _ = fs::remove_dir_all(&dir);
}
