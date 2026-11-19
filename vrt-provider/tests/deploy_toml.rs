//! H1-r1: deployment TOMLs must parse via the crate's own serde struct
//! (deny_unknown_fields) and pass `validate()`. Placeholders are
//! substituted with valid values; real paths go to a temp root.
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
        "vrt-provider-deploy-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
}

fn provider_text(raw: &str, root: &Path) -> String {
    raw.replace("/run/vrt", &root.join("run").to_string_lossy())
        .replace("/data", &root.join("data").to_string_lossy())
        .replace("REPLACE_ME_CELL_ID", "cell-1")
        .replace("REPLACE_ME_KEY_ID", "deploy-key")
        .replace(
            "REPLACE_ME_PUBLIC_KEY",
            "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=",
        )
        .replace("REPLACE_ME_S3_ENDPOINT", "s3.example.com")
        .replace("REPLACE_ME_S3_REGION", "auto")
        .replace("REPLACE_ME_S3_BUCKET", "vrt-deploy-test")
}

#[test]
fn deploy_provider_tomls_parse_and_validate() {
    let Some(tomls) = deploy_tomls::deploy_tomls("vrt-provider.toml") else {
        return;
    };
    for toml in tomls {
        let root = tmp_root();
        for d in [
            "run/listeners",
            "run/secrets",
            "run/pod-delivery",
            "run/render-inputs",
        ] {
            std::fs::create_dir_all(root.0.join(d)).unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for d in [
                "run",
                "run/listeners",
                "run/secrets",
                "run/pod-delivery",
                "run/render-inputs",
            ] {
                std::fs::set_permissions(root.0.join(d), std::fs::Permissions::from_mode(0o700))
                    .unwrap();
            }
        }
        let cur = root.0.join("run/secrets/cursor-secret");
        std::fs::write(&cur, "0123456789abcdef0123456789abcdef").unwrap();
        let pg = root.0.join("run/render-inputs/pg-credentials.json");
        std::fs::write(&pg, "{}").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&cur, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::set_permissions(&pg, std::fs::Permissions::from_mode(0o600)).unwrap();
        }

        let text = provider_text(&std::fs::read_to_string(&toml).unwrap(), &root.0);
        // The binary's own parse path (vrt-provider.rs: `toml::from_str`
        // into the deny_unknown_fields struct), then validate().
        let config: vrt_provider::config::ProviderConfig = toml::from_str(&text)
            .unwrap_or_else(|e| panic!("{} failed serde parse: {e}", toml.display()));
        config
            .validate()
            .unwrap_or_else(|e| panic!("{} failed validation: {e}", toml.display()));
    }
}
