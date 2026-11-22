//! The standalone compose quick start (dev/CI) keeps the SQLite db and
//! the fs persistence root on ONE Docker device, which vrt-sql's
//! fail-closed durability guard rejects. `of init` therefore needs an
//! explicit dev-only opt-in; the default render must stay armed.

use std::path::{Path, PathBuf};

fn fake_workerd(root: &Path) -> PathBuf {
    let fake = root.parent().unwrap().join(format!(
        "fake-workerd-{}",
        root.file_name().unwrap().to_string_lossy()
    ));
    std::fs::write(&fake, "#!/bin/sh\necho 'workerd 2026.1.0'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    fake
}

fn init(root: &Path, fake: &Path, extra: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_of"))
        .args(["init", "--root"])
        .arg(root)
        .args(["--cell", "local", "--tenant", "local", "--workerd"])
        .arg(fake)
        .args(extra)
        .output()
        .unwrap()
}

fn short_root(prefix: &str) -> Option<vrt_securefs::ShortRoot> {
    match vrt_securefs::ShortRoot::new(prefix) {
        Ok(r) => Some(r),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("SKIP: short root not writable ({e}) — sandboxed run");
            None
        }
        Err(e) => panic!("short root: {e}"),
    }
}

#[test]
fn dev_flag_writes_the_exemption_and_warns() {
    let Some(root) = short_root("vrtsqldev1") else {
        return;
    };
    let fake = fake_workerd(&root);
    let out = init(&root, &fake, &["--dev-allow-same-device-durability"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("DEV/CI ONLY") && stderr.contains("NOT guaranteed"),
        "the opt-in must be loud, got: {stderr}"
    );
    let sql = std::fs::read_to_string(root.join("config/vrt-sql.toml")).unwrap();
    assert_eq!(sql.matches("allowSameDeviceDurability").count(), 1, "{sql}");
    assert!(sql.contains("allowSameDeviceDurability = true"), "{sql}");
    assert!(sql.contains("DEV ONLY"), "{sql}");
}

#[test]
fn default_render_leaves_the_durability_guard_armed() {
    let Some(root) = short_root("vrtsqldev2") else {
        return;
    };
    let fake = fake_workerd(&root);
    let out = init(&root, &fake, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sql = std::fs::read_to_string(root.join("config/vrt-sql.toml")).unwrap();
    assert!(
        !sql.contains("allowSameDeviceDurability"),
        "production/config default must not carry the dev exemption: {sql}"
    );
}

#[test]
fn switching_the_flag_on_is_a_visible_config_change() {
    let Some(root) = short_root("vrtsqldev3") else {
        return;
    };
    let fake = fake_workerd(&root);
    assert!(init(&root, &fake, &[]).status.success());
    // Passing the flag later must not silently no-op (the old config
    // would keep vrt-sql refusing to boot).
    let out = init(&root, &fake, &["--dev-allow-same-device-durability"]);
    assert!(!out.status.success(), "drift must require --reconfigure");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("differs") && err.contains("--reconfigure"),
        "{err}"
    );
    assert!(err.contains("allowSameDeviceDurability"), "{err}");

    let out = init(
        &root,
        &fake,
        &["--dev-allow-same-device-durability", "--reconfigure"],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sql = std::fs::read_to_string(root.join("config/vrt-sql.toml")).unwrap();
    assert!(sql.contains("allowSameDeviceDurability = true"), "{sql}");
}
