//! Wire types for the `of` → `of-bundler` subprocess protocol —
//! `#[path]`-included by the helper so both sides serialize the same
//! shape. The helper lives in its own cargo workspace on purpose:
//! rolldown's dep tree enables serde_json `preserve_order` +
//! `arbitrary_precision`, and cargo feature unification would leak
//! those semantics into every crate in this workspace that shares a
//! build graph with vrt-local (signed desired-state docs, wire
//! shapes, golden fixtures).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Everything the bundler subprocess needs for one `bundle` call.
#[derive(Debug, Serialize, Deserialize)]
pub struct BundleSpec {
    /// Worker project root (cwd for resolution).
    pub root: String,
    /// Entry specifier, project-relative (`main` after normalization).
    pub entry: String,
    /// Dirname of `entry` (`""` for root) — emitted chunk dir.
    pub main_dir: String,
    /// `module_root` + `/` prefix (`""` at root): rule globs evaluate
    /// the path with this prefix stripped.
    pub rule_prefix: String,
    /// Flattened rule globs — a path under `rule_prefix` matching ANY
    /// of these is a rule-typed module and stays external.
    pub rule_globs: Vec<String>,
    pub opts: BundleOptsSpec,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BundleOptsSpec {
    pub minify: Option<bool>,
    pub keep_names: Option<bool>,
    #[serde(default)]
    pub define: BTreeMap<String, String>,
    pub jsx_factory: Option<String>,
    pub jsx_fragment: Option<String>,
    #[serde(default)]
    pub nodejs_compat: bool,
    /// Specifier rewrites applied before resolution: exact key when it
    /// ends in `$`, otherwise a prefix match (rolldown/oxc
    /// `resolve.alias` semantics). Values are absolute file paths — the
    /// config sandbox uses this to swap `@cloudflare/config` for a
    /// Workers-safe `./public` shim. Empty in every deploy path.
    #[serde(default)]
    pub alias: BTreeMap<String, String>,
}

/// Stdout payload — exactly one of `ok` / `err` is set.
#[derive(Debug, Serialize, Deserialize)]
pub struct BundleOutcome {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<BundledSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BundledSpec {
    /// Bundled ESM source for `mainModule`.
    pub main_source: String,
    /// `(module name, code)` non-entry chunks from dynamic `import()`.
    #[serde(default)]
    pub chunks: Vec<(String, String)>,
    /// `(module name, project-relative file)` rule-typed externals.
    #[serde(default)]
    pub externals: Vec<(String, String)>,
    /// Absolute paths of every file-backed module rolldown pulled into
    /// the graph (entry, relative imports, JSON modules and
    /// `node_modules` packages). Virtual modules are omitted. Sorted and
    /// deduped so callers get a deterministic list; the config sandbox
    /// reports it as the evaluation's `dependencies` (the Node loader
    /// contract).
    #[serde(default)]
    pub dependencies: Vec<String>,
}
