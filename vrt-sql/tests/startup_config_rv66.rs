//! RV-R66: fs persistence layer on the same device as the local db -> non-dev fails closed (verified 2026-11-15).
#![cfg(unix)]

use std::path::Path;
use vrt_sql::service::{check_fs_durability_distinct, DurabilityError};

fn tmp(name: &str) -> std::path::PathBuf {
    let p = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "rv66-{name}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn same_device_without_dev_flag_is_rejected() {
    let root = tmp("same");
    let (db, persist) = (root.join("db"), root.join("persistence"));
    std::fs::create_dir_all(&db).unwrap();
    // The persistence root has not been created yet: judged by the nearest ancestor, likewise the same device.
    let err = check_fs_durability_distinct(&db, &persist, false).unwrap_err();
    assert!(matches!(err, DurabilityError::SameDevice { .. }), "{err}");
    assert!(err.to_string().contains("VRT_SQL_DURABILITY_SAME_DEVICE"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn same_device_with_dev_flag_is_allowed_and_reported() {
    let root = tmp("dev");
    let (db, persist) = (root.join("db"), root.join("persistence"));
    std::fs::create_dir_all(&db).unwrap();
    std::fs::create_dir_all(&persist).unwrap();
    // Ok(true) = exemption applies, the caller must warn.
    assert_eq!(check_fs_durability_distinct(&db, &persist, true), Ok(true));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn allow_same_device_defaults_to_false_in_config() {
    let cfg: vrt_sql::service::ServiceConfig = toml::from_str(
        r#"
dbRoot = "/tmp/x"
listenTcp = "127.0.0.1:8090"
bookmarkSecretFile = "/tmp/b"
authSecretFile = "/tmp/a"
[persistence]
backend = "fs"
fs = { root = "/tmp/p" }
"#,
    )
    .unwrap();
    assert!(!cfg.persistence.allow_same_device_durability);
}

/// The same-device exemption is dev-only: no shipped deployment config
/// or example template may enable it (`of dev` and the standalone
/// compose quick start inject it into the configs they generate;
/// nothing else does, and no env var enables it). Markdown docs are
/// excluded on purpose — operator-facing READMEs must be able to name
/// the key to say "production never sets this".
#[test]
fn shipped_configs_never_enable_same_device_durability() {
    fn walk(dir: &std::path::Path, hits: &mut Vec<String>) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, hits);
            } else if p.extension().and_then(|x| x.to_str()) == Some("md") {
                continue;
            } else if let Ok(text) = std::fs::read_to_string(&p) {
                if text.contains("allowSameDeviceDurability") || text.contains("allow_same_device_durability") {
                    hits.push(p.display().to_string());
                }
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut hits = Vec::new();
    for d in ["deploy", "examples"] {
        walk(&root.join(d), &mut hits);
    }
    assert!(hits.is_empty(), "production-facing configs must not enable same-device durability: {hits:?}");
}
