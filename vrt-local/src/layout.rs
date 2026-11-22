//! Standalone root layout — every directory `0700`, created via
//! `vrt_securefs::ensure_secure_dir` (mode + ownership verified).
//!
//! T25/L1: `init` never writes into `root` directly AND never removes
//! or renames `root` itself — it may be a mount point (compose named
//! volume). The layout is built inside a staging dir **under root**
//! (`<root>/.init-tmp-<pid>`), then committed by renaming each entry
//! into `root` and finally writing the `.vrt-init` marker — the
//! commit point. A crash mid-init leaves either cleanable staging/
//! residue (no marker ⇒ never treated as initialized) or a complete
//! root, never a half-formed one mistaken for initialized.

use crate::LocalError;
use std::path::{Path, PathBuf};

/// Base dirs every initialized root must contain (0700, validated by
/// [`check`] on every `open`).
const DIRS: [&str; 8] = [
    "keys", "state", "desired", "objects", "secrets", "vars", "config", "mgmt",
];

/// Extra dirs created by init (T25): host/provider runtime state and
/// the seeded facade manifest. Validated by [`check`] only when the
/// `.vrt-init` marker exists — roots created before T25 simply lack
/// them and keep passing. `objstore` (T36b): vrt-sql fs persistence root --
/// under the bind-mount/host-dir variant the in-volume directory is created by init (for a named volume the owner comes from
/// the image's /data/objstore, maintained in sync with Dockerfile.vrt-local).
const INIT_DIRS: [&str; 5] = ["facade", "provider", "work", "cache", "objstore"];

/// Commit marker — written LAST by [`init_commit`]. `open`/`check`
/// treat it as proof a commit completed; a root without it can only
/// be empty or cleanable residue.
const MARKER: &str = ".vrt-init";
/// Staging dir prefix inside `root`.
const STAGING_PREFIX: &str = ".init-tmp-";

pub fn objects_dir(root: &Path) -> PathBuf {
    root.join("objects")
}
pub fn desired_dir(root: &Path) -> PathBuf {
    root.join("desired")
}
pub fn keys_dir(root: &Path) -> PathBuf {
    root.join("keys")
}
pub fn state_dir(root: &Path) -> PathBuf {
    root.join("state")
}
pub fn config_dir(root: &Path) -> PathBuf {
    root.join("config")
}
pub fn mgmt_dir(root: &Path) -> PathBuf {
    root.join("mgmt")
}
pub fn secrets_file(root: &Path) -> PathBuf {
    root.join("secrets/secrets.json")
}
pub fn vars_file(root: &Path) -> PathBuf {
    root.join("vars/vars.json")
}
/// Seeded facade manifest the host renders against (T25/L4).
pub fn facade_manifest_path(root: &Path) -> PathBuf {
    root.join("facade/manifest.json")
}
/// Host-side work/cache dirs created by `of init` (vrt-local does not
/// touch their contents).
pub fn host_work_dir(root: &Path) -> PathBuf {
    root.join("work")
}
pub fn host_cache_dir(root: &Path) -> PathBuf {
    root.join("cache")
}
pub fn provider_dir(root: &Path) -> PathBuf {
    root.join("provider")
}
/// Per-tenant listener sockets the provider binds (T25/L6).
pub fn provider_listeners_dir(root: &Path) -> PathBuf {
    root.join("provider/listeners")
}
/// Provider fs backends (`data/kv`, `data/r2`, `data/assets`).
pub fn provider_data_dir(root: &Path) -> PathBuf {
    root.join("provider/data")
}

/// Every top-level entry a vrt-local root may legitimately contain —
/// used to tell "our half-init residue" (cleanable) from foreign data
/// (fail closed, never deleted).
const KNOWN_ENTRIES: [&str; 14] = [
    "keys", "state", "desired", "objects", "secrets", "vars", "config", "mgmt", "facade",
    "provider", "work", "cache", "objstore", MARKER,
];

fn is_staging_name(name: &str) -> bool {
    name.starts_with(STAGING_PREFIX)
}

/// True when `root` contains ONLY entries this tool could have created:
/// layout dirs/files, staging residue, the marker. A single foreign
/// entry makes the whole root foreign — fail closed, touch nothing.
fn is_our_residue(root: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    entries.flatten().all(|e| {
        e.file_name()
            .to_str()
            .map(|n| is_staging_name(n) || KNOWN_ENTRIES.contains(&n))
            .unwrap_or(false)
    })
}

/// Precondition gate — refuse when `root` holds anything we did not
/// create. Called before staging so an existing deployment (marker
/// present, or foreign files) is never touched.
/// T34e: standalone socket-path preflight — the provider listener and
/// workerd service sockets bind unix paths under `root`; if even the
/// *known* longest path exceeds the AF_UNIX budget, `of init` fails
/// HERE with a clear message instead of letting `vrt-provider` die at
/// bind time with a bare ENAMETOOLONG (the T34d dead end).
pub fn check_socket_path_budget(root: &Path, tenant_id: &str) -> Result<(), LocalError> {
    let socket = vrt_securefs::socket_path(&provider_listeners_dir(root), tenant_id);
    vrt_securefs::check_socket_path_len(&socket).map_err(|e| {
        LocalError::Op(
            "socket path budget",
            format!("{} — choose a shorter --root", e),
        )
    })?;
    // workerd service sockets: <work>/gen-<n>/sock/<name>.sock —
    // names are worker ids (bounded charset); estimate generously.
    let work_sock = host_work_dir(root)
        .join("gen-0/sock")
        .join("w_0000000000000000000000000000000.sock");
    vrt_securefs::check_socket_path_len(&work_sock).map_err(|e| {
        LocalError::Op(
            "socket path budget",
            format!("{} — choose a shorter --root", e),
        )
    })
}

pub fn check_init_preconditions(root: &Path) -> Result<(), LocalError> {
    match std::fs::metadata(root) {
        Err(_) => Ok(()), // absent — init creates it (0700)
        Ok(meta) if meta.is_dir() => {
            if std::fs::metadata(root.join(MARKER)).is_ok() {
                return Err(LocalError::Layout("root already initialized"));
            }
            let mut entries = std::fs::read_dir(root).map_err(LocalError::Io)?;
            if entries.next().is_none() {
                return Ok(()); // empty dir (e.g. a fresh mount point)
            }
            if is_our_residue(root) {
                return Ok(()); // crashed-init leftovers — init cleans
            }
            Err(LocalError::Layout("root not empty"))
        }
        Ok(_) => Err(LocalError::Layout("root not a dir")),
    }
}

/// Begin an init: create `root` when absent (never remove/rename it —
/// L1: mount points must survive), clean stale staging + residue from
/// earlier crashed inits, build the layout in `root/.init-tmp-<pid>`,
/// and return its path. The caller writes ALL contents into the
/// staging dir, then calls [`init_commit`]. On any error the caller
/// removes the staging dir.
pub fn init_begin(root: &Path) -> Result<PathBuf, LocalError> {
    // An existing EMPTY dir owned by us but not yet 0700 (the usual
    // `mkdir /data` / host-bind case, incl. mount points — chmod works
    // on mount roots) is tightened in place; anything else fails the
    // secure-dir check. `check_init_preconditions` already proved the
    // dir is empty or our residue.
    if let Ok(meta) = std::fs::metadata(root) {
        if meta.is_dir() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt, PermissionsExt};
// SAFETY: geteuid(2) has no preconditions and cannot fail.
                if meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o777 != 0o700 {
                    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))
                        .map_err(|e| {
                            LocalError::Op("init root", format!("{}: {e}", root.display()))
                        })?;
                }
            }
        }
    }
    vrt_securefs::ensure_secure_dir(root)
        .map_err(|e| LocalError::Op("init root", format!("{}: {e}", root.display())))?;
    // Clean crashed-init leftovers: stale staging dirs AND any layout
    // entries left by a commit that died before the marker write —
    // without the marker they are provably not an initialized root.
    let entries = std::fs::read_dir(root)
        .map_err(|e| LocalError::Op("init scan", format!("{}: {e}", root.display())))?;
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        if is_staging_name(name) || KNOWN_ENTRIES.contains(&name) {
            if name == MARKER {
                continue; // marker ⇒ initialized — unreachable via preconditions
            }
            let _ = std::fs::remove_dir_all(e.path());
            let _ = std::fs::remove_file(e.path());
        }
    }
    let staging = root.join(format!("{STAGING_PREFIX}{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&staging); // our own stale pid dir
    vrt_securefs::ensure_secure_dir(&staging)
        .map_err(|e| LocalError::Op("init staging", format!("{}: {e}", staging.display())))?;
    for d in DIRS.iter().chain(INIT_DIRS.iter()) {
        vrt_securefs::ensure_secure_dir(&staging.join(d)).map_err(|e| {
            LocalError::Op("init dir", format!("{}: {e}", staging.join(d).display()))
        })?;
    }
    // Config-referenced provider subdirs (L6): listeners + fs backends.
    for d in [
        "provider/listeners",
        "provider/data/kv",
        "provider/data/r2",
        "provider/data/assets",
    ] {
        vrt_securefs::ensure_secure_dir(&staging.join(d)).map_err(|e| {
            LocalError::Op("init dir", format!("{}: {e}", staging.join(d).display()))
        })?;
    }
    Ok(staging)
}

/// Commit a fully-populated staging dir: move every staging entry into
/// `root` (rename inside the same dir — same filesystem by
/// construction), then write the `.vrt-init` marker LAST. The marker
/// is the commit point: a crash earlier leaves residue a next init
/// cleans, never a root mistaken for initialized.
pub fn init_commit(root: &Path, staging: &Path) -> Result<(), LocalError> {
    if staging.parent() != Some(root)
        || !staging
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_staging_name)
    {
        return Err(LocalError::Layout("staging not under root"));
    }
    let entries = std::fs::read_dir(staging)
        .map_err(|e| LocalError::Op("commit scan", format!("{}: {e}", staging.display())))?;
    for e in entries.flatten() {
        let dest = root.join(e.file_name());
        if dest.exists() {
            return Err(LocalError::Layout("commit dest exists"));
        }
        std::fs::rename(e.path(), &dest)
            .map_err(|err| LocalError::Op("commit rename", format!("{}: {err}", dest.display())))?;
    }
    vrt_securefs::atomic_write(&root.join(MARKER), b"vrt-local init\n", 0o600)
        .map_err(|e| LocalError::Op("commit marker", format!("{}: {e}", root.display())))?;
    let _ = std::fs::remove_dir(staging); // empty by now
    Ok(())
}

/// Re-verify the layout on every `open` — a chmod regression must
/// fail closed, not be silently tolerated. F6: the root ITSELF is
/// checked (not just the subdirs). Marker-bearing roots additionally
/// validate the T25 init dirs; marker-less (pre-T25) roots are
/// validated against the base set only.
pub fn check(root: &Path) -> Result<(), LocalError> {
    vrt_securefs::validate_secure_dir(root).map_err(|_| LocalError::Layout("root"))?;
    for d in DIRS {
        vrt_securefs::validate_secure_dir(&root.join(d)).map_err(|_| LocalError::Layout("dirs"))?;
    }
    if root.join(MARKER).exists() {
        for d in INIT_DIRS {
            vrt_securefs::validate_secure_dir(&root.join(d))
                .map_err(|_| LocalError::Layout("dirs"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T34e: init fails closed when the derived socket path would
    /// exceed the AF_UNIX budget — clear error, no silent write.
    #[test]
    fn socket_budget_rejects_long_root() {
        let root = Path::new("/").join("r".repeat(60));
        let ok_root = Path::new("/tmp/vrt");
        check_socket_path_budget(ok_root, "local").unwrap();
        let err = check_socket_path_budget(&root, "local").unwrap_err();
        assert!(format!("{err:?}").contains("socket path budget"), "{err}");
        // Tenant names expand the path too.
        let err2 = check_socket_path_budget(ok_root, &"t".repeat(90)).unwrap_err();
        assert!(format!("{err2:?}").contains("socket path budget"), "{err2}");
    }
}
