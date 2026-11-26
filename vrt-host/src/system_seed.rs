//! Startup seeding of the system (facade) modules into the object cache.
//!
//! The system workers' ES modules (`facade-*.js`, `vrt-egress`, …) are
//! baked into the vrt-host image under `<facade dir>/dist/<module.name>`
//! next to `<facade dir>/manifest.json`. They are NOT published bundle
//! artifacts: the control plane's `/vrt/bundles/:digest` only serves
//! digests referenced by a published envelope (that restriction is
//! deliberate and must stay), so assembling a tenant that embeds a
//! system module can never fetch it over the network.
//!
//! [`seed_system_modules`] therefore runs once at startup, BEFORE any
//! tenant is assembled:
//!
//! 1. parse the manifest through the typed boundary
//!    (`vrt_config::parse_facade_manifest` — fail closed on any shape
//!    violation);
//! 2. for each module, resolve `<dist>/<name>` (names are already
//!    rejected for `..`/absolute/`.` segments by the parser; the
//!    resolved path is additionally canonicalised and must stay under
//!    the canonical dist root, and must be a regular non-symlink file);
//! 3. read the bytes ONCE (bounded), hash them, compare with the
//!    manifest `sha256`, and write exactly those verified bytes with
//!    `BundleCache::put_raw` (which re-verifies) — no re-read, so no
//!    check/use window;
//! 4. ANY failure aborts startup (missing file, digest mismatch,
//!    traversal, symlink, oversize): a host that cannot prove its
//!    system modules must not serve tenants.
//!
//! Manifest integrity comes from the image: `/opt/vrt/facade` is
//! root-owned and read-only for the runtime uid (Dockerfile.vrt-host),
//! and there is no separate compile-time digest/signature for it — the
//! same trust anchor `params_json_from_manifest` already relies on.

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::artifact::{sha256_hex, ArtifactError, BundleCache};

/// Upper bound for one system module (facade bundles were far smaller;
/// wasm-engine system workers ship ~27 MiB modules — same bound as the
/// pack-side `MAX_MODULE_BYTES` in vrt-config).
pub const MAX_SYSTEM_MODULE_BYTES: u64 = 64 * 1024 * 1024;

/// Why seeding refused. Every variant is fatal at startup.
#[derive(Debug, thiserror::Error)]
pub enum SeedError {
    #[error("facade manifest unreadable or invalid")]
    Manifest,
    #[error("system module {module}: path rejected")]
    PathRejected { module: String },
    #[error("system module {module}: file missing or unreadable")]
    Missing { module: String },
    #[error("system module {module}: too large")]
    TooLarge { module: String },
    #[error("system module {module}: sha256 does not match manifest")]
    DigestMismatch { module: String },
    #[error("system module {module}: cache write failed: {source}")]
    Cache {
        module: String,
        #[source]
        source: ArtifactError,
    },
}

/// Verify every system module under `<manifest dir>/dist` against the
/// manifest and write it into `cache`. Returns the number of distinct
/// digests seeded.
pub fn seed_system_modules(cache: &BundleCache, manifest_path: &Path) -> Result<usize, SeedError> {
    let raw = std::fs::read_to_string(manifest_path).map_err(|_| SeedError::Manifest)?;
    let manifest = vrt_config::parse_facade_manifest(&raw).map_err(|_| SeedError::Manifest)?;
    let facade_dir = manifest_path.parent().ok_or(SeedError::Manifest)?;
    let dist_root =
        std::fs::canonicalize(facade_dir.join("dist")).map_err(|_| SeedError::Manifest)?;

    let mut seeded = std::collections::BTreeSet::new();
    for worker in &manifest.system_workers {
        for m in &worker.modules {
            if seeded.contains(&m.digest) {
                // Same bytes already verified+written under this digest.
                continue;
            }
            let bytes = read_module(&dist_root, &m.name)?;
            if sha256_hex(&bytes) != m.digest {
                return Err(SeedError::DigestMismatch {
                    module: m.name.clone(),
                });
            }
            cache
                .put_raw(&m.digest, &bytes)
                .map_err(|source| SeedError::Cache {
                    module: m.name.clone(),
                    source,
                })?;
            seeded.insert(m.digest.clone());
        }
    }
    Ok(seeded.len())
}

fn read_module(dist_root: &Path, name: &str) -> Result<Vec<u8>, SeedError> {
    let rejected = || SeedError::PathRejected {
        module: name.to_owned(),
    };
    let missing = || SeedError::Missing {
        module: name.to_owned(),
    };
    let rel = Path::new(name);
    if rel.is_absolute()
        || rel
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(rejected());
    }
    let joined: PathBuf = dist_root.join(rel);
    let meta = std::fs::symlink_metadata(&joined).map_err(|_| missing())?;
    if meta.file_type().is_symlink() {
        return Err(rejected());
    }
    if !meta.is_file() {
        return Err(missing());
    }
    // Intermediate symlinked directories would escape the lexical check.
    let canon = std::fs::canonicalize(&joined).map_err(|_| missing())?;
    if !canon.starts_with(dist_root) {
        return Err(rejected());
    }
    let file = std::fs::File::open(&canon).map_err(|_| missing())?;
    let mut bytes = Vec::new();
    file.take(MAX_SYSTEM_MODULE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| missing())?;
    if bytes.len() as u64 > MAX_SYSTEM_MODULE_BYTES {
        return Err(SeedError::TooLarge {
            module: name.to_owned(),
        });
    }
    Ok(bytes)
}
