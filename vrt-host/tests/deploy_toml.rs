//! H1-r1: the deployment TOMLs must load through the REAL
//! `HostConfig::load` (deny_unknown_fields + semantic validation) —
//! a config the binary cannot parse means a pod that cannot start.
//! Placeholders (strings only) are substituted with valid values;
//! real paths are redirected into a temp root.
#[path = "../../test-support/deploy_tomls.rs"]
mod deploy_tomls;
use std::path::{Path, PathBuf};

/// Valid Ed25519 public key (base64) — same fixture the deploy TOMLs use.
const DEPLOY_PUBLIC_KEY: &str = "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=";

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
        "vrt-host-deploy-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    TmpDir(root)
}

fn secret_0600(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn host_text(raw: &str, root: &Path) -> String {
    // The two dialects are substituted separately: deployment TOMLs use
    // absolute paths (`/data`, `/run/vrt`, `/opt/vrt/workerd`), while
    // `examples/*.toml` use the documented `__VRT_*__` placeholders.
    // Mixing both passes would rewrite `/data` inside an example's own
    // `__VRT_ROOT__/provider/data/...` path.
    if raw.contains("__VRT_ROOT__") {
        return raw
            // `bindingBackendDir`/socket: ≤104B `sun_path` at emit.
            .replace("__VRT_ROOT__/provider/listeners", "/vrt-test/listeners")
            .replace("__VRT_BACKEND_SOCK__", "/vrt-test/listeners")
            .replace("__VRT_CELL__", "cell-1")
            .replace("__VRT_KEY_ID__", "deploy-key")
            .replace("__VRT_PUBKEY__", DEPLOY_PUBLIC_KEY)
            .replace("__VRT_LISTEN__", "127.0.0.1:18080")
            .replace("__VRT_CONTROL__", "127.0.0.1:19091")
            .replace("__VRT_METRICS__", "127.0.0.1:19093")
            .replace("__WORKERD__", &root.join("workerd").to_string_lossy())
            .replace("__VRT_ROOT__", &root.to_string_lossy());
    }
    // `bindingBackendDir` is validated like a socket path at emit
    // (≤104B `sun_path` bound) — deep CARGO_TARGET_TMPDIR paths
    // exceed it, so substitute a short literal dir BEFORE the
    // generic `/run/vrt` rewrite (provider validates the same bound
    // at bind; no bind happens in this test).
    raw.replace("/run/vrt/listeners", "/vrt-test/listeners")
        .replace("/opt/vrt/workerd", &root.join("workerd").to_string_lossy())
        .replace(
            "/opt/vrt/facade/manifest.json",
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../facade/manifest.json")
                .to_string_lossy(),
        )
        .replace("/data", &root.join("data").to_string_lossy())
        .replace("/run/vrt", &root.join("run").to_string_lossy())
        .replace("REPLACE_ME_CELL_ID", "cell-1")
        .replace("REPLACE_ME_CP_HOST", "cp.example.com")
        .replace("REPLACE_ME_KEY_ID", "deploy-key")
        .replace("REPLACE_ME_PUBLIC_KEY", DEPLOY_PUBLIC_KEY)
}

#[test]
fn deploy_host_tomls_load_and_emit() {
    let Some(tomls) = deploy_tomls::deploy_tomls("vrt-host.toml") else {
        return;
    };
    for toml in tomls {
        let root = tmp_root();
        // Recreate the fs contract the initContainer materializes:
        // work dirs, the seeded render-inputs file (0600), the
        // normalized cell token (0600), a stand-in workerd binary.
        for d in [
            "data/work", "data/cache", "data/do", "data/assets",
            "run/render-inputs", "run/secrets", "run/pod-delivery",
            "run/listeners",
            // examples/: `__VRT_ROOT__` layouts (platform pod dirs and the
            // standalone work/objects/desired set).
            "pods/pod-0/work", "pods/pod-0/cache", "provider/listeners",
            "secrets", "facade", "work", "objects", "desired",
        ] {
            std::fs::create_dir_all(root.0.join(d)).unwrap();
        }
        secret_0600(
            &root.0.join("run/render-inputs/render-inputs.json"),
            "{\"vars\":{},\"secrets\":{}}",
        );
        secret_0600(&root.0.join("run/secrets/cell-token"), "test-cell-token-32bytes-000000000000");
        // examples render/validate against the real facade manifest and the
        // `[bundles].tokenFile` / render-secrets paths under `__VRT_ROOT__`.
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../facade/manifest.json"),
            root.0.join("facade/manifest.json"),
        )
        .map(|_| ())
        .unwrap_or_else(|e| panic!("copy facade manifest: {e}"));
        secret_0600(&root.0.join("secrets/cell-token"), "test-cell-token-32bytes-000000000000");
        secret_0600(
            &root.0.join("secrets/render-secrets.json"),
            "{\"secrets\":{}}",
        );
        // standalone-vrt-host.toml keeps its render inputs under
        // `__VRT_ROOT__/{secrets,vars}/*.json` (platform uses
        // `secrets/render-secrets.json`), both 0600.
        std::fs::create_dir_all(root.0.join("vars")).unwrap();
        secret_0600(&root.0.join("secrets/secrets.json"), "{\"secrets\":{}}");
        secret_0600(&root.0.join("vars/vars.json"), "{\"vars\":{}}");
        std::fs::write(root.0.join("workerd"), "#!/bin/sh\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.0.join("run"), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            for d in ["run/secrets", "run/pod-delivery", "run/render-inputs", "run/listeners"] {
                std::fs::set_permissions(
                    root.0.join(d),
                    std::fs::Permissions::from_mode(0o700),
                )
                .unwrap();
            }
        }

        let text = host_text(&std::fs::read_to_string(&toml).unwrap(), &root.0);
        let cfg_path = root.0.join("vrt-host.toml");
        std::fs::write(&cfg_path, &text).unwrap();
        let config = vrt_host::config::HostConfig::load(&cfg_path)
            .unwrap_or_else(|e| panic!("{} failed validation: {e}", toml.display()));
        // emit_mode exercises facade-manifest → RuntimeParams derive
        // (the same code startup uses). bindingBackendDir mode must
        // come out the far side with a working params struct.
        let mode = config
            .emit_mode()
            .unwrap_or_else(|e| panic!("{} emit_mode failed: {e}", toml.display()));
        match mode {
            vrt_host::assemble::EmitMode::Capnp { params } => {
                assert!(!params.system_workers.is_empty());
            }
            #[cfg(feature = "dev-stub")]
            _ => panic!("{}: unexpected emit mode", toml.display()),
        }
    }
}
