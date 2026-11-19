//! T16/B3: the shipped cell-controller example config must load and
//! its pod channel must construct against a real 0700 pod dir.

use std::path::{Path, PathBuf};

/// T37b: drop guard — fixture roots must not litter target/tmp.
struct TmpDir(PathBuf);

impl std::ops::Deref for TmpDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("tmp cleanup {}: {e}", self.0.display());
        }
    }
}

fn root() -> TmpDir {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "cell-controller-examples-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let pod = root.join("pods/pod-0");
    std::fs::create_dir_all(&pod).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pod, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    TmpDir(root)
}

#[test]
fn platform_example_loads_and_channel_constructs() {
    let root = root();
    let raw = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/platform-cell-controller.toml"),
    )
    .unwrap();
    let text = raw.replace("__VRT_ROOT__", &root.to_string_lossy());
    let path = root.join("config.toml");
    std::fs::write(&path, text).unwrap();
    let config = cell_controller::prod::load_config(path.to_str().unwrap())
        .expect("example config must load");
    assert_eq!(config.pods.len(), 1);
    assert!(config.egress_required, "platform default stays fail closed");
    // Production channel constructor: dir mode/owner + readyz URL policy.
    cell_controller::prod::pod_channel(&config).expect("pod channel constructs");
}
