//! `of` bundling helper: reads one `BundleSpec` JSON object on stdin,
//! runs rolldown on `entry` inside `root`, writes one `BundleOutcome`
//! JSON object on stdout. Lives in its own cargo workspace so
//! rolldown's serde_json feature flags cannot leak into the main
//! workspace — see `vrt-local/src/bundle_spec.rs` for why.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rolldown::{
    Bundler, BundlerOptions, BundlerTransformOptions, ChunkFilenamesOutputOption, InputItem,
    IsExternal, OutputFormat, Platform, RawMinifyOptions, ResolveOptions, TypeScriptOptions,
};
use rolldown_common::Output;

#[path = "../../vrt-local/src/bundle_spec.rs"]
mod spec;
#[path = "../../vrt-local/src/glob.rs"]
mod glob;

use spec::{BundleSpec, BundledSpec, BundleOutcome};

fn main() {
    let mut input = String::new();
    let outcome = std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|e| e.to_string())
        .and_then(|_| serde_json::from_str::<BundleSpec>(&input).map_err(|e| e.to_string()))
        .and_then(|spec| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?
                .block_on(run(&spec))
        });
    let out = match outcome {
        Ok(ok) => BundleOutcome {
            ok: Some(ok),
            err: None,
        },
        Err(err) => BundleOutcome { ok: None, err: Some(err) },
    };
    let bytes = serde_json::to_vec(&out).unwrap_or_else(|_| {
        br#"{"err":"outcome serialization failed"}"#.to_vec()
    });
    let mut stdout = std::io::stdout();
    let _ = stdout.write_all(&bytes);
    let _ = stdout.flush();
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
    by_spec: Mutex<BTreeMap<String, String>>,
}

impl ExtTracker {
    fn record(&self, spec: &str, rel: String) {
        self.by_spec.lock().unwrap().insert(spec.to_string(), rel);
    }
}

async fn run(spec: &BundleSpec) -> Result<BundledSpec, String> {
    let root = PathBuf::from(&spec.root);
    let root_c = root.canonicalize().unwrap_or(root);
    let tracker = Arc::new(ExtTracker {
        by_spec: Mutex::new(BTreeMap::new()),
    });
    let tr = tracker.clone();
    let rule_prefix = spec.rule_prefix.clone();
    let rule_globs: Arc<Vec<String>> = Arc::new(spec.rule_globs.clone());
    let globs = rule_globs.clone();
    let nodejs = spec.opts.nodejs_compat;
    let root_cb = root_c.clone();
    let is_rule = move |rel: &str| -> bool {
        rel.strip_prefix(&rule_prefix).is_some_and(|eval| {
            globs.iter().any(|g| glob::glob_match(g, eval))
        })
    };
    let rule = Arc::new(is_rule);
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
    if spec.opts.jsx_factory.is_some() || spec.opts.jsx_fragment.is_some() {
        // Wrangler `jsx_factory`/`jsx_fragment` = esbuild classic
        // pragma — no auto-runtime import.
        transform.jsx = Some(rolldown::Either::Right(rolldown::JsxOptions {
            runtime: Some("classic".into()),
            pragma: spec.opts.jsx_factory.clone(),
            pragma_frag: spec.opts.jsx_fragment.clone(),
            ..Default::default()
        }));
        transform.typescript = Some(TypeScriptOptions {
            jsx_pragma: spec.opts.jsx_factory.clone(),
            jsx_pragma_frag: spec.opts.jsx_fragment.clone(),
            ..Default::default()
        });
    }

    let entry = spec.entry.as_str();
    let main_dir = spec.main_dir.as_str();
    let opts_rolldown = BundlerOptions {
        input: Some(vec![InputItem {
            name: None,
            import: entry.to_string(),
        }]),
        cwd: Some(root_c.clone()),
        external: Some(external),
        platform: Some(Platform::Neutral),
        format: Some(OutputFormat::Esm),
        keep_names: Some(spec.opts.keep_names.unwrap_or(true)),
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
        minify: spec.opts.minify.map(RawMinifyOptions::Bool),
        define: Some(
            spec.opts
                .define
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
            // Opt-in specifier rewrites (empty for every deploy path).
            // BTreeMap keeps the emitted order deterministic.
            alias: if spec.opts.alias.is_empty() {
                None
            } else {
                Some(
                    spec.opts
                        .alias
                        .iter()
                        .map(|(k, v)| (k.clone(), vec![Some(v.clone())]))
                        .collect(),
                )
            },
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut bundler = Bundler::new(opts_rolldown).map_err(|e| format!("bundle init: {e}"))?;
    let out = bundler
        .generate()
        .await
        .map_err(|e| format!("bundle: {e}"))?;
    bundler.close().await.ok();

    // Module names every emitted chunk occupies — inter-chunk edges
    // resolve to these, so they must not be mistaken for file externals.
    let mut emitted = BTreeSet::new();
    let mut main_source = None;
    let mut chunks = Vec::new();
    let entry_name = rel_norm(Path::new("/"), Path::new(entry))
        .ok_or("module escapes worker dir")?;
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
            .ok_or("module escapes worker dir")?;
            emitted.insert(name.clone());
            chunks.push((name, chunk.code.clone()));
        }
    }
    let Some(main_source) = main_source else {
        return Err("bundle output".into());
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
                    return Err("module escapes worker dir".into());
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
            return Err(format!("unresolved import: {s}"));
        }
    }

    Ok(BundledSpec {
        main_source,
        chunks,
        externals: externals.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::BundleOptsSpec;
    use std::fs;

    fn run_spec(dir: &Path, entry: &str, main_dir: &str, globs: &[&str]) -> BundledSpec {
        run_spec_with(dir, entry, main_dir, globs, BundleOptsSpec::default())
    }

    fn run_spec_with(
        dir: &Path,
        entry: &str,
        main_dir: &str,
        globs: &[&str],
        opts: BundleOptsSpec,
    ) -> BundledSpec {
        let spec = BundleSpec {
            root: dir.to_string_lossy().into_owned(),
            entry: entry.into(),
            main_dir: main_dir.into(),
            rule_prefix: String::new(),
            rule_globs: globs.iter().map(|s| s.to_string()).collect(),
            opts,
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(&spec))
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
        let b = run_spec(dir.path(), "index.ts", "", &[]);
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
        let b = run_spec(dir.path(), "src/index.ts", "src", &["*.txt", "**/*.txt"]);
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
        let b = run_spec(dir.path(), "src/index.ts", "src", &[]);
        // A lazy chunk rides under src/ as an esModule — the dynamic
        // edge must not appear among file externals.
        assert_eq!(b.chunks.len(), 1);
        assert!(b.chunks[0].0.starts_with("src/"));
        assert!(b.chunks[0].0.ends_with(".js"));
        assert!(b.main_source.contains("import("));
        assert!(b.externals.is_empty());
    }

    #[test]
    fn alias_rewrites_bare_specifier() {
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("shim-public.js");
        fs::write(&shim, "export const marker = 'from-public-shim';").unwrap();
        fs::write(
            dir.path().join("index.ts"),
            "import { marker } from '@cloudflare/config';\nexport default { fetch() { return new Response(marker) } };",
        )
        .unwrap();
        let mut alias = BTreeMap::new();
        alias.insert(
            "@cloudflare/config$".to_string(),
            shim.to_string_lossy().into_owned(),
        );
        let b = run_spec_with(
            dir.path(),
            "index.ts",
            "",
            &[],
            BundleOptsSpec {
                alias,
                ..Default::default()
            },
        );
        assert!(b.main_source.contains("from-public-shim"));
        assert!(!b.main_source.contains("@cloudflare/config"));
        assert!(b.externals.is_empty());
    }

    #[test]
    fn alias_rewrites_relative_specifier() {
        // `with { type: "cf-worker" }` entrypoints are a specifier, not a
        // module; aliasing the relative import to a shim exporting the
        // absolute path mirrors the Node loader's synthetic module.
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("entrypoint-shim.js");
        fs::write(&shim, "export default '/project/src/index.js';").unwrap();
        fs::write(
            dir.path().join("index.ts"),
            "import * as entrypoint from './src/index.js';\nexport default { fetch() { return new Response(entrypoint.default) } };",
        )
        .unwrap();
        let mut alias = BTreeMap::new();
        alias.insert(
            "./src/index.js$".to_string(),
            shim.to_string_lossy().into_owned(),
        );
        let b = run_spec_with(
            dir.path(),
            "index.ts",
            "",
            &[],
            BundleOptsSpec {
                alias,
                ..Default::default()
            },
        );
        assert!(b.main_source.contains("/project/src/index.js"));
        assert!(b.externals.is_empty());
    }
}
