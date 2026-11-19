//! RV-R66：fs 持久层与本地库同设备 → 非 dev fail closed（已验证 2026-11-15）。
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
    // persistence root 尚未创建：按最近祖先判定，同样同设备。
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
    // Ok(true) = 豁免生效，调用方须告警。
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
