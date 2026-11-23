//! Wrangler-semantic bundling: `main` + its import graph compiled to a
//! single ESM mainModule by the `of-bundler` helper subprocess
//! (rolldown — the Rust bundler behind rolldown-vite).
//! `cloudflare:*`/`node:*` specifiers, bare Node builtins under
//! `nodejs_compat`, and rule-typed modules (Text/Data/CompiledWasm)
//! stay external so the pack carries each as a first-class wire
//! module — the same shape wrangler's esbuild step produces for the
//! upload API.
//!
//! The bundler is a separate binary, not a linked dependency:
//! rolldown's dep tree enables serde_json `preserve_order` +
//! `arbitrary_precision`, and cargo feature unification would leak
//! those semantics into every crate sharing a build graph with
//! vrt-local (signed desired-state docs, wire shapes, axum `Json`
//! serialization, golden fixtures). `of` resolves the helper in this
//! order:
//!   1. `$OF_BUNDLER` — explicit path (dev/CI override)
//!   2. embedded bytes — written to a hashed cache file and executed
//!      (release binaries ship self-contained)
//!   3. `<repo>/of-bundler/target/{debug,release}/of-bundler` —
//!      in-repo builds
//!   4. `of-bundler` next to the running executable (co-installed)
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use crate::bundle_spec::{BundleOutcome, BundleOptsSpec, BundleSpec};
use crate::LocalError;

/// Bundler knobs normalized from wrangler keys (`[bundle]` in the
/// WorkerToml shape). `None` keeps rolldown defaults.
#[derive(Debug, Clone, Default)]
pub struct BundleOpts {
    pub minify: Option<bool>,
    pub keep_names: Option<bool>,
    pub define: BTreeMap<String, String>,
    pub jsx_factory: Option<String>,
    pub jsx_fragment: Option<String>,
    /// `nodejs_compat` flag — bare Node builtins (`path`, `fs`, …)
    /// stay external for workerd's compat layer.
    pub nodejs_compat: bool,
    /// Specifier rewrites applied before resolution (`key$` = exact,
    /// otherwise prefix; values are absolute files). Deploy paths leave
    /// this empty — it exists for out-of-band tooling such as the
    /// `cloudflare.config.ts` sandbox spike.
    pub alias: BTreeMap<String, String>,
}

/// The compiled mainModule plus lazily loaded code-split chunks and
/// the external file-backed modules the emitted code still imports.
pub struct Bundled {
    /// Bundled ESM source for `mainModule`.
    pub main_source: String,
    /// `(module name, code)` for non-entry chunks produced by dynamic
    /// `import()` — shipped as extra ESM wire modules so the split
    /// stays lazy, matching wrangler's multi-file upload.
    pub chunks: Vec<(String, String)>,
    /// `(module name, project-relative file)` pairs — each becomes a
    /// wire module typed by the rule table (Text/Data/CompiledWasm/…).
    pub externals: Vec<(String, String)>,
}

/// `of-bundler` bytes embedded at build time via `OF_EMBED_BUNDLER`
/// (release packaging); empty in dev builds — see build.rs.
const EMBEDDED: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/of-bundler.bin"));

/// Where a materialized copy of the embedded helper lives. Hashed by
/// content so upgraded `of` binaries don't reuse a stale helper.
fn embedded_path() -> Result<PathBuf, LocalError> {
    if EMBEDDED.is_empty() {
        return Err(LocalError::Worker("of-bundler not embedded"));
    }
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in EMBEDDED {
        h = (h ^ b as u64).wrapping_mul(0x100000001b3);
    }
    let path = std::env::temp_dir()
        .join("openflare")
        .join(format!("of-bundler-{h:016x}"));
    if !path.is_file() {
        let dir = path.parent().unwrap();
        std::fs::create_dir_all(dir).map_err(|_| LocalError::Worker("of-bundler cache"))?;
        let tmp = dir.join(format!(".of-bundler-{}", std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|_| LocalError::Worker("of-bundler cache"))?;
            f.write_all(EMBEDDED)
                .map_err(|_| LocalError::Worker("of-bundler cache"))?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| LocalError::Worker("of-bundler cache"))?;
        }
        // Atomic publish; a racing same-version helper is byte-identical.
        let _ = std::fs::rename(&tmp, &path);
        let _ = std::fs::remove_file(&tmp);
    }
    Ok(path)
}

fn find_bundler() -> Result<PathBuf, LocalError> {
    if let Ok(p) = std::env::var("OF_BUNDLER") {
        if !p.is_empty() {
            return Ok(PathBuf::from(p));
        }
    }
    if let Ok(p) = embedded_path() {
        return Ok(p);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for profile in ["debug", "release"] {
        let p = manifest
            .join("../of-bundler/target")
            .join(profile)
            .join("of-bundler");
        if p.is_file() {
            return Ok(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let p = dir.join("of-bundler");
            if p.is_file() {
                return Ok(p);
            }
        }
    }
    Err(LocalError::WorkerOwned(
        "of-bundler helper not found (set OF_BUNDLER, build of-bundler/, or use a release binary)"
            .into(),
    ))
}

/// Bundle `entry` inside `root` via the `of-bundler` subprocess.
/// `rule_globs`/`rule_prefix` reproduce `worker_pkg`'s rule predicate:
/// a project-relative path under `rule_prefix` matching any glob is a
/// rule-typed module and stays external.
pub fn bundle_sync(
    root: PathBuf,
    entry: String,
    main_dir: String,
    rule_globs: Vec<String>,
    rule_prefix: String,
    opts: BundleOpts,
) -> Result<Bundled, LocalError> {
    let spec = BundleSpec {
        root: root.to_string_lossy().into_owned(),
        entry,
        main_dir,
        rule_prefix,
        rule_globs,
        opts: BundleOptsSpec {
            minify: opts.minify,
            keep_names: opts.keep_names,
            define: opts.define,
            jsx_factory: opts.jsx_factory,
            jsx_fragment: opts.jsx_fragment,
            nodejs_compat: opts.nodejs_compat,
            alias: opts.alias,
        },
    };
    let wire = serde_json::to_vec(&spec).map_err(|_| LocalError::Worker("bundle spec"))?;
    let exe = find_bundler()?;
    let mut child = Command::new(exe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|_| LocalError::Worker("of-bundler spawn"))?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&wire)
        .map_err(|_| LocalError::Worker("of-bundler stdin"))?;
    let out = child
        .wait_with_output()
        .map_err(|_| LocalError::Worker("of-bundler wait"))?;
    if !out.status.success() {
        return Err(LocalError::WorkerOwned(format!(
            "of-bundler exited {status}",
            status = out.status
        )));
    }
    let outcome: BundleOutcome =
        serde_json::from_slice(&out.stdout).map_err(|_| LocalError::Worker("of-bundler output"))?;
    match (outcome.ok, outcome.err) {
        (Some(b), None) => Ok(Bundled {
            main_source: b.main_source,
            chunks: b.chunks,
            externals: b.externals,
        }),
        (None, Some(e)) => Err(LocalError::WorkerOwned(format!("bundle: {e}"))),
        _ => Err(LocalError::Worker("of-bundler output")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // Aliases exist for out-of-band tooling (the cloudflare.config.ts
    // sandbox): the deploy path never sets them, but the wire and the
    // helper's resolve step must honour them.
    #[test]
    fn bundle_alias_rewrites_specifier_before_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("config-public.js");
        fs::write(&shim, "export const marker = 'public-shim';\n").unwrap();
        fs::write(
            dir.path().join("main.ts"),
            "import { marker } from '@cloudflare/config';\nexport default { fetch() { return new Response(marker) } };\n",
        )
        .unwrap();
        let mut alias = BTreeMap::new();
        alias.insert(
            "@cloudflare/config$".to_string(),
            shim.to_string_lossy().into_owned(),
        );
        let out = bundle_sync(
            dir.path().to_path_buf(),
            "main.ts".to_string(),
            String::new(),
            Vec::new(),
            String::new(),
            BundleOpts {
                alias,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(out.main_source.contains("public-shim"), "{}", out.main_source);
        assert!(!out.main_source.contains("@cloudflare/config"));
        assert!(out.externals.is_empty());
    }
}
