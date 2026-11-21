//! Tail Workers (`tail_consumers`): wrangler parsing and pack-time
//! resolution. A consumer names a DIFFERENT already-deployed worker of
//! the same tenant; a miss, self-reference, malformed entry or duplicate
//! collapses/fails at deploy time — never a runtime event drop.

use std::path::PathBuf;
use vrt_local::{pack_with_resolver, DeployOverrides};

fn worker_dir(fields: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("wrangler.json"),
        format!(
            r#"{{"name":"producer","main":"main.js","compatibility_date":"2025-01-01"{fields}}}"#
        ),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("main.js"),
        "export default { fetch() { return new Response('ok') } }\n",
    )
    .unwrap();
    dir
}

fn resolver(name: &str) -> Option<String> {
    match name {
        "tail-worker" => Some("w_tail".into()),
        "audit-worker" => Some("w_audit".into()),
        _ => None,
    }
}

fn pack(dir: &PathBuf) -> Result<vrt_local::PackedWorker, vrt_local::LocalError> {
    pack_with_resolver(dir, &DeployOverrides::default(), "team-a", &resolver)
}

fn pack_err(dir: &PathBuf, what: &str) -> vrt_local::LocalError {
    match pack(dir) {
        Ok(_) => panic!("{what}: expected a deploy error"),
        Err(e) => e,
    }
}

#[test]
fn object_and_legacy_string_forms_resolve_and_dedupe() {
    let dir = worker_dir(
        r#","tail_consumers":[
            {"service":"tail-worker"},
            "audit-worker",
            {"service":"tail-worker"}
        ]"#,
    );
    let pkg = pack(&dir.path().to_path_buf()).unwrap();
    let ids: Vec<&str> = pkg
        .tail_consumers
        .iter()
        .map(|t| t.worker_id.as_str())
        .collect();
    // Declaration order, first occurrence wins.
    assert_eq!(ids, vec!["w_tail", "w_audit"]);
}

#[test]
fn empty_list_is_valid_and_removes_the_field() {
    let dir = worker_dir(r#","tail_consumers":[]"#);
    let pkg = pack(&dir.path().to_path_buf()).unwrap();
    assert!(pkg.tail_consumers.is_empty());
}

#[test]
fn unknown_service_fails_at_deploy_time() {
    let dir = worker_dir(r#","tail_consumers":[{"service":"ghost"}]"#);
    let err = pack_err(&dir.path().to_path_buf(), "unknown consumer");
    assert!(
        format!("{err:?}").contains("tail_consumers service target"),
        "{err:?}"
    );
}

#[test]
fn self_reference_fails_closed() {
    let dir = worker_dir(r#","tail_consumers":[{"service":"producer"}]"#);
    let err = pack_err(&dir.path().to_path_buf(), "self-tail");
    assert!(
        format!("{err:?}").contains("tail_consumers service must be another worker"),
        "{err:?}"
    );
}

#[test]
fn malformed_entries_fail_closed() {
    for fields in [
        r#","tail_consumers":"tail-worker""#,
        r#","tail_consumers":[{"service":""}]"#,
        r#","tail_consumers":[{"service":"tail-worker","environment":"staging"}]"#,
        r#","tail_consumers":[42]"#,
    ] {
        let dir = worker_dir(fields);
        assert!(
            pack(&dir.path().to_path_buf()).is_err(),
            "must reject {fields}"
        );
    }
}

#[test]
fn more_than_the_capacity_is_rejected() {
    let mut entries = Vec::new();
    for i in 0..17 {
        entries.push(format!(r#"{{"service":"tail-{i}"}}"#));
    }
    let dir = worker_dir(&format!(r#","tail_consumers":[{}]"#, entries.join(",")));
    let all = |name: &str| name.strip_prefix("tail-").map(|i| format!("w_{i}"));
    let err = match pack_with_resolver(
        &dir.path().to_path_buf(),
        &DeployOverrides::default(),
        "team-a",
        &all,
    ) {
        Ok(_) => panic!("over-cap must fail"),
        Err(e) => e,
    };
    assert!(format!("{err:?}").contains("at most 16"), "{err:?}");
}
