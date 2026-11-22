//! T16/B3: both shipped provider example configs must parse + validate.

use std::path::{Path, PathBuf};
use vrt_provider::config::ProviderConfig;

/// T37b: drop guard — no fixture-root litter under target/tmp.
struct TmpDir(PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("tmp cleanup {}: {e}", self.0.display());
        }
    }
}

#[test]
fn platform_and_standalone_provider_examples_validate() {
    let root = TmpDir(
        Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("vrt-provider-examples-{}", std::process::id())),
    );
    std::fs::create_dir_all(&root.0).unwrap();
    let root = &root.0;
    for name in ["platform-vrt-provider.toml", "standalone-vrt-provider.toml"] {
        let raw = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../examples")
                .join(name),
        )
        .unwrap();
        let text = raw
            .replace("__VRT_ROOT__", &root.to_string_lossy())
            .replace("__VRT_KEY_ID__", "example-key")
            // RV-R72: every placeholder `of init` renders must be
            // substituted — an unparseable address now fails validate.
            .replace("__VRT_CELL__", "cell-example-1")
            .replace("__VRT_PROVIDER_METRICS__", "127.0.0.1:9100")
            .replace("__VRT_SQL_ADDR__", "http://127.0.0.1:8082")
            .replace(
                "__VRT_PUBKEY__",
                "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=",
            );
        let cfg: ProviderConfig = toml::from_str(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
        cfg.validate().unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}
