//! Wrangler-semantic bundling: `main` + its import graph compiled to a
//! single ESM mainModule via rolldown (the Rust bundler behind
//! rolldown-vite). `cloudflare:*`/`node:*` specifiers, bare Node
//! builtins under `nodejs_compat`, and rule-typed modules
//! (Text/Data/CompiledWasm) stay external so the pack carries each as a
//! first-class wire module — the same shape wrangler's esbuild step
//! produces for the upload API.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rolldown::{
    Bundler, BundlerOptions, BundlerTransformOptions, ChunkFilenamesOutputOption, InputItem,
    IsExternal, OutputFormat, Platform, RawMinifyOptions, ResolveOptions, TypeScriptOptions,
};
use rolldown_common::Output;

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

/// Normalize `p` to a `/`-separated relative path (no `.` segments —
/// the bundle format forbids them). `None` when the path escapes the
/// worker dir.
fn rel_norm(base: &Path, joined: &Path) -> Option<String> {
    let rel = joined.strip_prefix(base).ok().unwrap_or(joined);
    let mut out: Vec<String> = Vec::new();
    for c in rel.components() {
        match c {
            std::path::Component::Normal(s) => out.push(s.to_string_lossy().replace('\\', "/")),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if out.pop().is_none() {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(out.join("/"))
}

/// `(specifier → project-rel file)` recorded while marking externals,
/// plus the emitted-specifier → module-name mapping applied after
/// generate. Bare specifiers (`pkg/a.txt`) keep their written name —
/// workerd matches bare specifiers by exact module name.
struct ExtTracker {
    // specifier as written in source → resolved project-rel path
    by_spec: Mutex<BTreeMap<String, String>>,
}

impl ExtTracker {
    fn record(&self, spec: &str, rel: String) {
        self.by_spec.lock().unwrap().insert(spec.to_string(), rel);
    }
}

/// Run rolldown on `entry` inside `root` on a dedicated thread with a
/// current-thread runtime — safe to call from both sync and async
/// callers (`tokio::runtime` cannot nest).
pub fn bundle_sync(
    root: PathBuf,
    entry: String,
    main_dir: String,
    is_rule_module: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    opts: BundleOpts,
) -> Result<Bundled, LocalError> {
    std::thread::scope(|s| {
        s.spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| LocalError::Worker("bundle rt"))
                .and_then(|rt| {
                    rt.block_on(bundle_entry(&root, &entry, &main_dir, is_rule_module, opts))
                })
        })
        .join()
        .map_err(|_| LocalError::Worker("bundle panic"))?
    })
}

async fn bundle_entry(
    root: &Path,
    entry: &str,
    main_dir: &str,
    is_rule_module: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    opts: BundleOpts,
) -> Result<Bundled, LocalError> {
    let root_c = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let tracker = Arc::new(ExtTracker {
        by_spec: Mutex::new(BTreeMap::new()),
    });
    let tr = tracker.clone();
    let rule = is_rule_module.clone();
    let nodejs = opts.nodejs_compat;
    let root_cb = root_c.clone();
    let external = IsExternal::Fn(Some(Arc::new(move |spec, importer, _resolved| {
        let spec = spec.to_string();
        let importer = importer.map(|s| s.to_string());
        let tr = tr.clone();
        let rule = rule.clone();
        let root_cb = root_cb.clone();
        Box::pin(async move {
            if spec.starts_with("node:") || spec.starts_with("cloudflare:") {
                return Ok(true);
            }
            if nodejs
                && nodejs_built_in_modules::BUILTINS
                    .contains(&spec.split('/').next().unwrap_or(&spec))
            {
                return Ok(true);
            }
            // Rule-typed file? Resolve the specifier to a project-rel
            // path, then let the caller's rule table decide.
            let resolved = if spec.starts_with("./") || spec.starts_with("../") {
                importer
                    .as_deref()
                    .map(Path::new)
                    .and_then(|imp| imp.parent())
                    .map(|d| d.join(&spec))
            } else {
                // Bare specifier — resolve through node_modules chains
                // walking up from the importer's directory.
                importer
                    .as_deref()
                    .map(Path::new)
                    .and_then(|imp| imp.parent())
                    .map(|start| {
                        let mut dir = Some(start.to_path_buf());
                        loop {
                            let d = dir?;
                            let cand = d.join("node_modules").join(&spec);
                            if cand.is_file() {
                                return Some(cand);
                            }
                            dir = d.parent().map(|p| p.to_path_buf());
                            if !d.starts_with(&root_cb) {
                                return None;
                            }
                        }
                    })
                    .flatten()
            };
            let Some(abs) = resolved else {
                return Ok(false);
            };
            let Some(rel) = rel_norm(&root_cb, &abs) else {
                return Ok(false);
            };
            if !rule(&rel) {
                return Ok(false);
            }
            tr.record(&spec, rel);
            Ok(true)
        })
    })));

    let mut transform = BundlerTransformOptions::default();
    if opts.jsx_factory.is_some() || opts.jsx_fragment.is_some() {
        // Wrangler `jsx_factory`/`jsx_fragment` = esbuild classic
        // pragma — no auto-runtime import.
        transform.jsx = Some(rolldown::Either::Right(rolldown::JsxOptions {
            runtime: Some("classic".into()),
            pragma: opts.jsx_factory.clone(),
            pragma_frag: opts.jsx_fragment.clone(),
            ..Default::default()
        }));
        transform.typescript = Some(TypeScriptOptions {
            jsx_pragma: opts.jsx_factory.clone(),
            jsx_pragma_frag: opts.jsx_fragment.clone(),
            ..Default::default()
        });
    }

    let opts_rolldown = BundlerOptions {
        input: Some(vec![InputItem {
            name: None,
            import: entry.to_string(),
        }]),
        cwd: Some(root_c.clone()),
        external: Some(external),
        platform: Some(Platform::Neutral),
        format: Some(OutputFormat::Esm),
        keep_names: Some(opts.keep_names.unwrap_or(true)),
        // Logical location of every emitted chunk = the main module's
        // dir — emitted external specifiers come out relative to it,
        // matching how workerd resolves them against the module name.
        dir: Some(if main_dir.is_empty() {
            ".".to_string()
        } else {
            main_dir.to_string()
        }),
        // Emit the entry under its own basename so a lazy chunk's
        // `import("./index.ts")` resolves back to the main module name.
        entry_filenames: Some(ChunkFilenamesOutputOption::String(
            Path::new(entry)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| entry.to_string()),
        )),
        minify: opts.minify.map(RawMinifyOptions::Bool),
        define: Some(
            opts.define
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        transform: Some(transform),
        resolve: Some(ResolveOptions {
            extensions: Some(vec![
                ".js".into(),
                ".mjs".into(),
                ".ts".into(),
                ".mts".into(),
                ".jsx".into(),
                ".tsx".into(),
                ".cjs".into(),
                ".json".into(),
            ]),
            // `import './x.js'` resolving `./x.ts` — the TS-ecosystem
            // convention wrangler follows.
            extension_alias: Some(vec![
                (
                    ".js".into(),
                    vec![".ts".into(), ".tsx".into(), ".js".into(), ".jsx".into()],
                ),
                (".mjs".into(), vec![".mts".into(), ".mjs".into()]),
                (".cjs".into(), vec![".cts".into(), ".cjs".into()]),
            ]),
            condition_names: Some(vec![
                "workerd".into(),
                "worker".into(),
                "browser".into(),
                "import".into(),
                "module".into(),
                "default".into(),
            ]),
            main_fields: Some(vec!["browser".into(), "module".into(), "main".into()]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut bundler = Bundler::new(opts_rolldown).map_err(|_| LocalError::Worker("bundle init"))?;
    let out = bundler
        .generate()
        .await
        .map_err(|e| LocalError::WorkerOwned(format!("bundle: {e}")))?;
    bundler.close().await.ok();

    // Module names every emitted chunk occupies — inter-chunk edges
    // resolve to these, so they must not be mistaken for file externals.
    let mut emitted = std::collections::BTreeSet::new();
    let mut main_source = None;
    let mut chunks = Vec::new();
    let entry_name = rel_norm(Path::new("/"), Path::new(entry))
        .ok_or(LocalError::Worker("module escapes worker dir"))?;
    for asset in out.assets.iter() {
        let Output::Chunk(chunk) = asset else {
            continue;
        };
        if chunk.is_entry {
            main_source = Some(chunk.code.clone());
            emitted.insert(entry_name.clone());
        } else {
            let name = rel_norm(
                Path::new("/"),
                &Path::new(main_dir).join(chunk.filename.as_str()),
            )
            .ok_or(LocalError::Worker("module escapes worker dir"))?;
            emitted.insert(name.clone());
            chunks.push((name, chunk.code.clone()));
        }
    }
    let Some(main_source) = main_source else {
        return Err(LocalError::Worker("bundle output"));
    };

    let map = tracker.by_spec.lock().unwrap();
    let mut externals: BTreeMap<String, String> = BTreeMap::new();
    for asset in out.assets.iter() {
        let Output::Chunk(chunk) = asset else {
            continue;
        };
        for spec in chunk.imports.iter().chain(chunk.dynamic_imports.iter()) {
            let s = spec.as_str();
            if s.starts_with("node:") || s.starts_with("cloudflare:") {
                continue;
            }
            // Normalize the specifier to the module name it resolves to
            // from main_dir; an emitted-chunk name is an internal edge.
            let cand = rel_norm(Path::new("/"), &Path::new(main_dir).join(s));
            if cand.as_ref().is_some_and(|c| emitted.contains(c)) || emitted.contains(s) {
                continue;
            }
            if s.starts_with("./") || s.starts_with("../") {
                let Some(name) = cand else {
                    return Err(LocalError::Worker("module escapes worker dir"));
                };
                let rel = map.get(s).cloned().unwrap_or_else(|| name.clone());
                externals.entry(name).or_insert(rel);
                continue;
            }
            // Bare specifier: a recorded rule-typed file → wire module
            // under the emitted name (rolldown may emit a same-dir
            // `./x` import as bare `x`; the tracker keys on the source
            // spelling).
            let rel = map
                .get(s)
                .or_else(|| map.get(&format!("./{s}")))
                .or_else(|| map.get(&format!("../{s}")));
            if let Some(rel) = rel {
                externals.entry(s.to_string()).or_insert(rel.clone());
                continue;
            }
            if nodejs
                && nodejs_built_in_modules::BUILTINS.contains(&s.split('/').next().unwrap_or(s))
            {
                continue;
            }
            return Err(LocalError::WorkerOwned(format!("unresolved import: {s}")));
        }
    }

    Ok(Bundled {
        main_source,
        chunks,
        externals: externals.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn run(dir: &Path, entry: &str, main_dir: &str, rule: fn(&str) -> bool) -> Bundled {
        let rule: Arc<dyn Fn(&str) -> bool + Send + Sync> = Arc::new(rule);
        bundle_sync(
            dir.to_path_buf(),
            entry.to_string(),
            main_dir.to_string(),
            rule,
            BundleOpts::default(),
        )
        .unwrap()
    }

    #[test]
    fn ts_dep_bundled_externals_kept() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("helper.ts"), "export const x: number = 1;").unwrap();
        fs::write(
            dir.path().join("index.ts"),
            "import { x } from './helper';\nimport { connect } from 'cloudflare:sockets';\nimport crypto from 'node:crypto';\nexport default { fetch() { return new Response(String(x + typeof connect + typeof crypto.randomUUID)) } };",
        )
        .unwrap();
        let b = run(dir.path(), "index.ts", "", |_| false);
        assert!(b.main_source.contains("node:crypto"));
        assert!(b.main_source.contains("cloudflare:sockets"));
        assert!(b.externals.is_empty());
    }

    #[test]
    fn rule_externals_map_to_module_names() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("a.txt"), "hi").unwrap();
        fs::write(
            dir.path().join("src/index.ts"),
            "import t from '../a.txt';\nexport default { fetch() { return new Response(t) } };",
        )
        .unwrap();
        let b = run(dir.path(), "src/index.ts", "src", |rel| {
            rel.ends_with(".txt")
        });
        assert_eq!(
            b.externals,
            vec![("a.txt".to_string(), "a.txt".to_string())]
        );
        assert!(b.main_source.contains("../a.txt"));
    }

    #[test]
    fn dynamic_import_emits_lazy_chunk() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("src/lazy.ts"), "export const tag = 'lazy';").unwrap();
        fs::write(
            dir.path().join("src/index.ts"),
            "export default { async fetch() { const m = await import('./lazy.ts'); return new Response(m.tag) } };",
        )
        .unwrap();
        let b = run(dir.path(), "src/index.ts", "src", |_| false);
        // A lazy chunk rides under src/ as an esModule — the dynamic
        // edge must not appear among file externals.
        assert_eq!(b.chunks.len(), 1);
        assert!(b.chunks[0].0.starts_with("src/"));
        assert!(b.chunks[0].0.ends_with(".js"));
        assert!(b.main_source.contains("import("));
        assert!(b.externals.is_empty());
    }
}
