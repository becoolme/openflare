//! H1-r1: deployment TOMLs must load through `prod::load_config` —
//! the exact entry the binary uses (snake_case, deny_unknown_fields,
//! semantic checks: absolute dirs, loopback readyz, 0700 pod dirs,
//! trusted-key decode).
#[path = "../../test-support/deploy_tomls.rs"]
mod deploy_tomls;
use std::path::{Path, PathBuf};

struct TmpDir(PathBuf);
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tmp_root() -> TmpDir {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "cell-controller-deploy-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
}

#[test]
fn deploy_controller_tomls_load() {
    let Some(tomls) = deploy_tomls::deploy_tomls("cell-controller.toml") else {
        return;
    };
    for toml in tomls {
        let root = tmp_root();
        // The fs contract the initContainer creates: 0700 pod-delivery
        // and render-inputs dirs, the normalized 0600 token, and the
        // durable cell-state dir (fence + cron claims).
        for d in ["run/pod-delivery", "run/render-inputs", "run/secrets", "cell-data"] {
            std::fs::create_dir_all(root.0.join(d)).unwrap();
        }
        let token = root.0.join("run/secrets/cell-token");
        std::fs::write(&token, "test-cell-token-32bytes-000000000000").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
            for d in ["run/pod-delivery", "run/render-inputs", "run/secrets"] {
                std::fs::set_permissions(
                    root.0.join(d),
                    std::fs::Permissions::from_mode(0o700),
                )
                .unwrap();
            }
        }
        let text = std::fs::read_to_string(&toml)
            .unwrap()
            .replace("/run/vrt", &root.0.join("run").to_string_lossy())
            .replace("/cell-data", &root.0.join("cell-data").to_string_lossy())
            .replace("REPLACE_ME_CELL_ID", "cell-1")
            .replace("REPLACE_ME_CP_HOST", "cp.example.com")
            .replace("REPLACE_ME_KEY_ID", "deploy-key")
            .replace(
                "REPLACE_ME_PUBLIC_KEY",
                "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=",
            );
        let cfg_path = root.0.join("cell-controller.toml");
        std::fs::write(&cfg_path, &text).unwrap();
        cell_controller::prod::load_config(cfg_path.to_str().unwrap())
            .unwrap_or_else(|e| panic!("{} failed validation: {e}", toml.display()));
    }
}
