//! H1-r1: deployment TOMLs must parse via the crate's own serde struct
//! (deny_unknown_fields) and pass `ServiceConfig::validate()` plus
//! the binary's 0600 secret-file gate. Placeholders are substituted
//! with valid values; real paths go to a temp root.
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
        "vrt-sql-deploy-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
}

#[test]
fn deploy_sql_tomls_parse_validate_and_load_secrets() {
    let Some(tomls) = deploy_tomls::deploy_tomls("vrt-sql.toml") else {
        return;
    };
    for toml in tomls {
        let root = tmp_root();
        for d in ["data/db", "data/persistence", "run/secrets"] {
            std::fs::create_dir_all(root.0.join(d)).unwrap();
        }
        for k in ["vrt-sql-auth-secret", "vrt-sql-bookmark-secret"] {
            let f = root.0.join("run/secrets").join(k);
            std::fs::write(&f, "0123456789abcdef0123456789abcdef").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                root.0.join("run/secrets"),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        let text = std::fs::read_to_string(&toml)
            .unwrap()
            .replace("/data", &root.0.join("data").to_string_lossy())
            .replace("/run/vrt", &root.0.join("run").to_string_lossy());
        // The binary's own path: serde (deny_unknown_fields) then
        // validate(), then the 0600 secret loads it performs at boot.
        let config: vrt_sql::service::ServiceConfig = toml::from_str(&text)
            .unwrap_or_else(|e| panic!("{} failed serde parse: {e}", toml.display()));
        config
            .validate()
            .unwrap_or_else(|e| panic!("{} failed validation: {e}", toml.display()));
        for secret in [&config.bookmark_secret_file, &config.auth_secret_file] {
            let key = vrt_sql::service::load_secret_0600(secret, vrt_securefs::SecretPurpose::ServiceHmac).unwrap_or_else(|e| {
                panic!(
                    "{}: secret {} unreadable: {e}",
                    toml.display(),
                    secret.display()
                )
            });
            assert!(key.len() >= 32, "{}: secret too short", toml.display());
        }
    }
}
