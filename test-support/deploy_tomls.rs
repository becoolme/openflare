//! Locates operator deployment TOMLs for the `deploy_toml` integration
//! tests (vrt-host, vrt-provider, vrt-sql, cell-controller).
//!
//! OpenFlare does not ship cluster manifests. A deployment that keeps its
//! own manifests (for example a Kubernetes kustomize tree) points
//! `VRT_DEPLOY_TOML_DIRS` at one or more directories, separated like
//! `PATH`. Every file named `<component>.toml` below them is then loaded
//! through the component's real config loader, so a config the binary
//! cannot parse fails in CI instead of as a pod that cannot start.
//!
//! Unset: the tests print `skip:` and pass. Set: fail closed. A listed
//! path that is not a directory, or a set of directories with no matching
//! file at all, fails the test.
//!
//! Included by each crate's `tests/deploy_toml.rs` with `#[path]`, so the
//! lookup rule has one implementation.
use std::path::{Path, PathBuf};

pub const ENV: &str = "VRT_DEPLOY_TOML_DIRS";

pub fn deploy_tomls(file_name: &str) -> Option<Vec<PathBuf>> {
    let raw = match std::env::var_os(ENV) {
        Some(v) if !v.is_empty() => v,
        _ => {
            eprintln!("skip: {ENV} is not set; no deployment {file_name} to check");
            return None;
        }
    };
    let mut out = Vec::new();
    for dir in std::env::split_paths(&raw) {
        assert!(dir.is_dir(), "{ENV}: {} is not a directory", dir.display());
        walk(&dir, file_name, &mut out);
    }
    out.sort();
    out.dedup();
    assert!(
        !out.is_empty(),
        "{ENV} is set but no {file_name} was found below it"
    );
    Some(out)
}

fn walk(dir: &Path, file_name: &str, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{ENV}: cannot read {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("{ENV}: {}: {e}", dir.display()));
        // Do not follow symlinks: the walk stays inside the listed tree.
        let ty = entry
            .file_type()
            .unwrap_or_else(|e| panic!("{ENV}: {}: {e}", entry.path().display()));
        if ty.is_dir() {
            walk(&entry.path(), file_name, out);
        } else if ty.is_file() && entry.file_name() == file_name {
            out.push(entry.path());
        }
    }
}
