//! T31/C4 — wrangler.toml / wrangler.json / wrangler.jsonc → normalized
//! `WorkerProjectConfig`, mirroring `server/src/services/workerProjectConfig/
//! wranglerSource.ts` field-for-field (same candidate order, same env-merge
//! semantics as @wdl-dev/cli `resolveWranglerConfig`, same ignore-list,
//! same typed error codes). Cross-language parity is locked by the shared
//! golden vectors in `shared/vrt-project-config-vectors.json`.
//!
//! Standalone extras vs the platform path:
//!   * `vrt.toml` keeps precedence — it is the native standalone config;
//!   * `cloudflare.config.ts` / `wrangler.config.ts` are never executed —
//!     they fail closed telling the user to convert via the platform CLI;
//!   * a wrangler `service` entrypoint is validated and missing KV/R2 ids are
//!     derived at the WorkerToml conversion boundary (documented).

#[cfg(test)]
use serde::Deserialize;
use serde_json::{Map, Value};
#[cfg(test)]
use std::collections::BTreeMap;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use crate::worker_pkg::WorkerToml;
use crate::LocalError;

/// Deploy-config candidates, in selection order — shared with tests and
/// e2e assertions so the candidate set has a single source of truth.
pub const WRANGLER_CANDIDATES: [&str; 3] = ["wrangler.json", "wrangler.jsonc", "wrangler.toml"];
const CANDIDATES: [&str; 3] = WRANGLER_CANDIDATES;
const MAX_CONFIG_BYTES: usize = 256 * 1024;
/// Mirrors IGNORED_TOP_LEVEL in wranglerSource.ts.
const IGNORED_TOP_LEVEL: &[&str] = &[
    "account_id",
    "workers_dev",
    "send_metrics",
    "keep_vars",
    "build",
    "alias",
    "tsconfig",
    // `preserve_file_names`/`upload_source_maps` are upload-detail
    // knobs: additional modules keep their project-relative names in
    // the wire table (deterministic, content-addressed already) and
    // the bundler emits no sourcemaps — accepted and ignored.
    "preserve_file_names",
    "upload_source_maps",
    "cwd",
    "config",
    // MIG-14: wrangler `[placement]` (CF Smart Placement / region
    // hints) — dropped; the runtime has no placement semantics.
    "placement",
    // `$schema` is the standard JSON-Schema annotation wrangler.jsonc
    // files carry (`"$schema": "node_modules/wrangler/config-schema.json"`)
    // — editor metadata, not config.
    "$schema",
];
/// Mirrors @wdl-dev/cli NON_INHERITABLE_ENV_KEYS — env blocks REPLACE
/// these keys wholesale instead of merging the top-level value.
const NON_INHERITABLE_ENV_KEYS: &[&str] = &[
    "analytics_engine_datasets",
    "flagship",
    "define",
    "vars",
    "d1_databases",
    "durable_objects",
    "hyperdrive",
    "ratelimits",
    "kv_namespaces",
    "r2_buckets",
    "ai_search_namespaces",
    "ai_search",
    "vectorize",
    "services",
    "queues",
    "k2",
    "send_email",
    "workflows",
    "tail_consumers",
    "streaming_tail_consumers",
    "secrets",
    "secrets_store_secrets",
    "version_metadata",
    "worker_loaders",
];
/// Mirrors @wdl-dev/cli TOP_LEVEL_ONLY_ENV_KEYS.
const TOP_LEVEL_ONLY_ENV_KEYS: &[&str] = &["name", "keep_vars", "send_metrics"];

#[derive(Debug)]
pub struct ProjectConfigError {
    pub code: &'static str,
    pub message: String,
    pub field: Option<String>,
}
impl std::fmt::Display for ProjectConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.field {
            Some(field) => write!(f, "[{}] {} ({})", self.code, self.message, field),
            None => write!(f, "[{}] {}", self.code, self.message),
        }
    }
}
impl std::error::Error for ProjectConfigError {}

fn unsupported(field: &str, what: Option<&str>) -> ProjectConfigError {
    ProjectConfigError {
        code: "VRT_CONFIG_UNSUPPORTED_FIELD",
        message: format!(
            "{field}{} is not supported by the VRT runtime",
            what.map(|w| format!(" ({w})")).unwrap_or_default()
        ),
        field: Some(field.into()),
    }
}
fn invalid(field: &str, what: &str) -> ProjectConfigError {
    ProjectConfigError {
        code: "VRT_CONFIG_INVALID",
        message: format!("{field}: {what}"),
        field: Some(field.into()),
    }
}
fn invalid_code(code: &'static str, field: &str, what: &str) -> ProjectConfigError {
    ProjectConfigError {
        code,
        message: format!("{field}: {what}"),
        field: Some(field.into()),
    }
}

/// Which config source produced the meta — surfaced for observability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaSource {
    VrtToml,
    Wrangler(String),
}

/// Load the worker meta for `worker_dir`: `vrt.toml` wins; else the
/// wrangler family in packer order. Programmatic configs (.ts/.js) fail
/// closed — standalone has no JS evaluator by design.
pub fn load_worker_meta(worker_dir: &Path) -> Result<(WorkerToml, MetaSource), LocalError> {
    load_worker_meta_for_env(worker_dir, None)
}

/// Load worker meta from `vrt.toml` or Wrangler config, selecting the
/// requested Wrangler named environment before normalization. `vrt.toml`
/// has no environment blocks and rejects an explicit selection.
pub fn load_worker_meta_for_env(
    worker_dir: &Path,
    env_name: Option<&str>,
) -> Result<(WorkerToml, MetaSource), LocalError> {
    let vrt_path = worker_dir.join("vrt.toml");
    if vrt_path.exists() {
        if env_name.is_some() {
            return Err(LocalError::WorkerOwned(
                "[VRT_CONFIG_INVALID] --env only selects Wrangler named environments; vrt.toml has no env blocks".into(),
            ));
        }
        let raw = std::fs::read_to_string(&vrt_path).map_err(|_| LocalError::Worker("vrt.toml"))?;
        if raw.len() > 64 * 1024 {
            return Err(LocalError::Worker("vrt.toml size"));
        }
        let meta: WorkerToml = toml::from_str(&raw).map_err(|e| {
            // Surface the offending span + key path so the operator can
            // fix vrt.toml without a guessing game — but never echo raw
            // values (vars/secrets may sit on the same lines): toml
            // renders keys in backticks and values as Debug-style double
            //-quoted spans, so only the latter are redacted (see
            // `redact_toml_message` for the escape rules).
            let msg = redact_toml_message(e.message())
                .unwrap_or_else(|| "<message redacted>".to_string());
            let where_ = e
                .span()
                .map(|r| {
                    let line = raw[..r.start].matches('\n').count() + 1;
                    let col = r.start - raw[..r.start].rfind('\n').map(|i| i + 1).unwrap_or(0) + 1;
                    format!(" at {line}:{col}")
                })
                .unwrap_or_default();
            LocalError::WorkerOwned(format!("vrt.toml parse{where_}: {msg}"))
        })?;
        return Ok((meta, MetaSource::VrtToml));
    }

    for programmatic in [
        "cloudflare.config.ts",
        "wrangler.config.ts",
        "cloudflare.config.js",
        "wrangler.config.js",
    ] {
        if worker_dir.join(programmatic).exists() {
            return Err(LocalError::Worker(
                // standalone intentionally ships no JS evaluator —
                // convert programmatic configs with the platform CLI.
                "programmatic config file — convert to wrangler.toml/json/jsonc (or vrt.toml) with the platform CLI",
            ));
        }
    }

    let found: Vec<&str> = CANDIDATES
        .iter()
        .copied()
        .filter(|c| worker_dir.join(c).exists())
        .collect();
    match found.first() {
        None => Err(LocalError::Worker("vrt.toml")),
        Some(&file) => {
            let (config, _resolved, _shadowed) =
                load_wrangler_project_config(worker_dir, None, env_name)
                    .map_err(|e| LocalError::Worker(Box::leak(e.to_string().into_boxed_str())))?;
            let meta = wrangler_config_to_worker_toml(&config)
                .map_err(|e| LocalError::Worker(Box::leak(e.to_string().into_boxed_str())))?;
            let _ = file;
            Ok((meta, MetaSource::Wrangler(found[0].to_string())))
        }
    }
}

/// Config-file basenames that are never worker modules (module scan).
pub fn is_config_basename(name: &str) -> bool {
    name == "vrt.toml"
        || name == "flagship.json"
        || CANDIDATES.contains(&name)
        || matches!(
            name,
            "cloudflare.config.ts"
                | "wrangler.config.ts"
                | "cloudflare.config.js"
                | "wrangler.config.js"
        )
}

// ── parsing ──────────────────────────────────────────────────────────

/// Minimal JSONC → JSON: strips a UTF-8 BOM, `//` and `/* */` comments
/// outside strings, and trailing commas (wrangler's `allowTrailingComma`).
/// Two passes — comment stripping first so the comma lookahead only ever
/// sees structural characters.
fn jsonc_to_json(src: &str) -> Result<String, ProjectConfigError> {
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    // pass 1: comments
    let mut out = String::with_capacity(src.len());
    let mut chars = src.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if in_string {
            out.push(c);
            if c == '\\' {
                if let Some(n) = chars.next() {
                    out.push(n);
                }
                continue;
            }
            if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                out.push(c);
            }
            '/' => match chars.peek() {
                Some('/') => {
                    for n in chars.by_ref() {
                        if n == '\n' {
                            out.push('\n');
                            break;
                        }
                    }
                }
                Some('*') => {
                    chars.next();
                    let mut closed = false;
                    let mut last = '\0';
                    for n in chars.by_ref() {
                        if last == '*' && n == '/' {
                            closed = true;
                            break;
                        }
                        if n == '\n' {
                            out.push('\n');
                        }
                        last = n;
                    }
                    if !closed {
                        return Err(invalid_code(
                            "VRT_CONFIG_INVALID",
                            "jsonc",
                            "unterminated block comment",
                        ));
                    }
                }
                _ => out.push(c),
            },
            _ => out.push(c),
        }
    }
    if in_string {
        return Err(invalid_code(
            "VRT_CONFIG_INVALID",
            "jsonc",
            "unterminated string",
        ));
    }
    // pass 2: trailing commas (comments are gone — lookahead only sees
    // whitespace between `,` and the next structural char).
    let stripped = out;
    let mut final_out = String::with_capacity(stripped.len());
    let mut it = stripped.chars().peekable();
    let mut in_string = false;
    while let Some(c) = it.next() {
        if in_string {
            final_out.push(c);
            if c == '\\' {
                if let Some(n) = it.next() {
                    final_out.push(n);
                }
                continue;
            }
            if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                final_out.push(c);
            }
            ',' => {
                let mut lookahead = it.clone();
                let mut trailing = false;
                while let Some(n) = lookahead.next() {
                    match n {
                        ' ' | '\t' | '\n' | '\r' => continue,
                        ']' | '}' => {
                            trailing = true;
                            break;
                        }
                        _ => break,
                    }
                }
                if !trailing {
                    final_out.push(',');
                } else {
                    final_out.push(' ');
                }
            }
            _ => final_out.push(c),
        }
    }
    Ok(final_out)
}

fn toml_to_json(v: &toml::Value) -> Value {
    match v {
        toml::Value::String(s) => Value::String(s.clone()),
        toml::Value::Integer(i) => Value::from(*i),
        toml::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        toml::Value::Boolean(b) => Value::from(*b),
        toml::Value::Datetime(d) => Value::String(d.to_string()),
        toml::Value::Array(a) => Value::Array(a.iter().map(toml_to_json).collect()),
        toml::Value::Table(t) => Value::Object(
            t.iter()
                .map(|(k, v)| (k.clone(), toml_to_json(v)))
                .collect(),
        ),
    }
}

fn parse_config_file(path: &Path, name: &str) -> Result<Value, ProjectConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|_| invalid_code("VRT_CONFIG_NOT_FOUND", name, "cannot read config file"))?;
    if raw.len() > MAX_CONFIG_BYTES {
        return Err(invalid_code("VRT_CONFIG_INVALID", name, "exceeds 256 KiB"));
    }
    if name.ends_with(".toml") {
        let v: toml::Value = toml::from_str(&raw)
            .map_err(|e| invalid_code("VRT_CONFIG_INVALID", name, &e.to_string()))?;
        Ok(toml_to_json(&v))
    } else {
        let json = jsonc_to_json(&raw)?;
        serde_json::from_str(&json)
            .map_err(|e| invalid_code("VRT_CONFIG_INVALID", name, &e.to_string()))
    }
}

// ── env resolution (mirrors @wdl-dev/cli resolveWranglerConfig) ───────

fn named_envs(cfg: &Map<String, Value>) -> Vec<String> {
    cfg.get("env")
        .and_then(|e| e.as_object())
        .map(|e| e.keys().cloned().collect())
        .unwrap_or_default()
}

pub fn resolve_env(
    cfg: &Value,
    env_name: Option<&str>,
    file: &str,
) -> Result<Value, ProjectConfigError> {
    let cfg_obj = cfg
        .as_object()
        .ok_or_else(|| invalid_code("VRT_CONFIG_INVALID", file, "config must be an object"))?;
    let envs = named_envs(cfg_obj);
    if env_name.is_none() {
        if !envs.is_empty() {
            return Err(invalid_code(
                "VRT_CONFIG_INVALID",
                "env",
                &format!("named environments found ({})", envs.join(", ")),
            ));
        }
        return Ok(cfg.clone());
    }
    let name = env_name.unwrap();
    if envs.is_empty() {
        return Err(invalid_code(
            "VRT_CONFIG_INVALID",
            "env",
            &format!("environment \"{name}\" requested but no [env] config exists"),
        ));
    }
    let env_table = cfg_obj.get("env").and_then(|e| e.as_object()).unwrap();
    let env_cfg = env_table
        .get(name)
        .ok_or_else(|| {
            invalid_code(
                "VRT_CONFIG_INVALID",
                "env",
                &format!(
                    "environment \"{name}\" not found (available: {})",
                    envs.join(", ")
                ),
            )
        })?
        .as_object()
        .ok_or_else(|| invalid_code("VRT_CONFIG_INVALID", "env", "env must be an object"))?;
    for key in env_cfg.keys() {
        if TOP_LEVEL_ONLY_ENV_KEYS.contains(&key.as_str()) {
            return Err(invalid_code(
                "VRT_CONFIG_INVALID",
                "env",
                &format!("env.{name}.{key} is top-level only"),
            ));
        }
    }
    let mut resolved = Map::new();
    for (k, v) in cfg_obj {
        if k == "env" || k == "__proto__" || NON_INHERITABLE_ENV_KEYS.contains(&k.as_str()) {
            continue;
        }
        resolved.insert(k.clone(), v.clone());
    }
    for (k, v) in env_cfg {
        if k == "__proto__" {
            continue;
        }
        // `observability` is an inherited table, not a replace-only
        // list: an env block that only tunes e.g.
        // `logs.head_sampling_rate` must not silently reset the
        // top-level `enabled = false` back to the default `true`
        // (fail-open on a privacy/collection gate). Deep-merge the
        // table (and its `logs` sub-table) so the env value overrides
        // exactly the keys it declares.
        if k == "observability" {
            if let (Some(base), Some(over)) =
                (resolved.get(k).and_then(Value::as_object), v.as_object())
            {
                let mut merged = base.clone();
                for (ok, ov) in over {
                    if ok == "logs" {
                        if let (Some(b), Some(o)) =
                            (merged.get(ok).and_then(Value::as_object), ov.as_object())
                        {
                            let mut logs = b.clone();
                            for (lk, lv) in o {
                                logs.insert(lk.clone(), lv.clone());
                            }
                            merged.insert(ok.clone(), Value::Object(logs));
                            continue;
                        }
                    }
                    merged.insert(ok.clone(), ov.clone());
                }
                resolved.insert(k.clone(), Value::Object(merged));
                continue;
            }
        }
        resolved.insert(k.clone(), v.clone());
    }
    Ok(Value::Object(resolved))
}

// ── normalization (mirrors normalizeWranglerConfig) ──────────────────

fn req_str<'a>(v: &'a Value, field: &str) -> Result<&'a str, ProjectConfigError> {
    match v.as_str().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        Some(s) => Ok(s),
        None => Err(invalid(field, "must be a non-empty string")),
    }
}
fn req_bool(v: &Value, field: &str) -> Result<bool, ProjectConfigError> {
    v.as_bool()
        .ok_or_else(|| invalid(field, "must be a boolean"))
}
fn opt_str<'a>(v: Option<&'a Value>, field: &str) -> Result<Option<&'a str>, ProjectConfigError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(req_str(v, field)?)),
    }
}
fn opt_str_list<'a>(
    v: Option<&'a Value>,
    field: &str,
) -> Result<Vec<&'a str>, ProjectConfigError> {
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|i| req_str(i, field))
            .collect::<Result<Vec<_>, _>>(),
        Some(_) => Err(invalid(field, "must be an array of strings")),
    }
}

/// Same restrictive shape as snapshot-protocol `valid_email`: one `@`,
/// non-empty local/domain, dotted domain, RFC size caps, limited
/// ASCII. Normalize-time fail closed mirrors desired-state validation.
fn valid_email(addr: &str) -> bool {
    let Some((local, domain)) = addr.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && local.len() <= 64
        && !domain.is_empty()
        && domain.len() <= 253
        && addr.len() <= 254
        && domain.contains('.')
        && addr
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'@' | b'.' | b'-' | b'_' | b'+' | b'%'))
}

fn opt_u64(v: Option<&Value>, field: &str) -> Result<Option<u64>, ProjectConfigError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_u64() {
            Some(n) => Ok(Some(n)),
            None => Err(invalid(field, "must be a non-negative integer")),
        },
    }
}
fn opt_bool(v: Option<&Value>, field: &str) -> Result<Option<bool>, ProjectConfigError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_bool()
            .map(Some)
            .ok_or_else(|| invalid(field, "must be a boolean")),
    }
}
fn opt_rate(v: Option<&Value>, field: &str) -> Result<Option<f64>, ProjectConfigError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_f64().filter(|f| f.is_finite()) {
            Some(f) => Ok(Some(f)),
            None => Err(invalid(field, "must be a finite number")),
        },
    }
}
fn table_list<'a>(
    v: &'a Value,
    field: &str,
) -> Result<Vec<&'a Map<String, Value>>, ProjectConfigError> {
    let arr = v
        .as_array()
        .ok_or_else(|| invalid(field, "must be an array of tables"))?;
    arr.iter()
        .enumerate()
        .map(|(i, e)| {
            e.as_object()
                .ok_or_else(|| invalid(&format!("{field}[{i}]"), "must be a table"))
        })
        .collect()
}
/// TOML/JSONC values are JSON-representable by construction — the only
/// non-JSON shape a TOML parser can yield is an empty/heterogeneous
/// weirdness; numbers must be finite.
fn is_json_value(v: &Value) -> bool {
    match v {
        Value::Number(n) => n.as_f64().map(|f| f.is_finite()).unwrap_or(true),
        Value::Array(a) => a.iter().all(is_json_value),
        Value::Object(o) => o.values().all(is_json_value),
        _ => true,
    }
}

/// CF `custom_domain` route: `pattern` is the hostname itself — a bare
/// lowercase FQDN (>=2 labels), no wildcard, no path, no port. The same
/// strict rule `snapshot-protocol` applies to wire `hostnames`, so a
/// config that would be rejected downstream fails here instead.
fn validate_custom_domain(pattern: &str, field: &str) -> Result<String, ProjectConfigError> {
    let h = pattern.trim().to_lowercase();
    if h.contains('*') {
        return Err(unsupported(
            field,
            Some("wildcard custom domains are not supported"),
        ));
    }
    if !snapshot_protocol::compute_desired::valid_hostname(&h) {
        return Err(invalid(
            field,
            "custom_domain must be a bare hostname (no path, port, or wildcard)",
        ));
    }
    Ok(h)
}

/// wrangler `base_dir` — the directory rule globs are evaluated
/// against (CF `moduleRoot`; the pack defaults it to the `main`
/// directory). A tenant-controlled path: fail closed unless it is a
/// clean relative directory inside the project — no absolute/drive
/// roots, `\`, NUL, `:` or `.`/`..`/empty segments.
fn validate_base_dir(value: &str, field: &str) -> Result<String, ProjectConfigError> {
    let bad = || {
        invalid(
            field,
            "must be a clean relative directory inside the project",
        )
    };
    if value.is_empty()
        || value.len() > 1024
        || value.starts_with('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.contains(':')
        || value.ends_with('/')
    {
        return Err(bad());
    }
    if value
        .split('/')
        .any(|c| c.is_empty() || c == "." || c == "..")
    {
        return Err(bad());
    }
    Ok(value.to_string())
}

/// CF `rules` — ordered `{type, globs, fallthrough?}` module-typing
/// rules (wrangler `Rule` shape, `additionalProperties: false`). The
/// five supported types map onto wire module types at pack time;
/// `PythonModule`/`PythonRequirement` exist in the wrangler schema but
/// VRT has no Python runtime, so they fail closed.
fn normalize_module_rules(value: &Value) -> Result<Value, ProjectConfigError> {
    const MODULE_RULE_TYPES: [&str; 5] = ["ESModule", "CommonJS", "CompiledWasm", "Text", "Data"];
    const PYTHON_RULE_TYPES: [&str; 2] = ["PythonModule", "PythonRequirement"];
    let arr = value
        .as_array()
        .ok_or_else(|| invalid("rules", "must be an array"))?;
    if arr.len() > 64 {
        return Err(invalid("rules", "must declare at most 64 rules"));
    }
    let mut out_arr = Vec::with_capacity(arr.len());
    for (i, e) in arr.iter().enumerate() {
        let field = format!("rules[{i}]");
        let o = e
            .as_object()
            .ok_or_else(|| invalid(&field, "must be a { type, globs, fallthrough? } object"))?;
        for k in o.keys() {
            match k.as_str() {
                "type" | "globs" | "fallthrough" => {}
                _ => return Err(unsupported(&format!("{field}.{k}"), None)),
            }
        }
        let ty = req_str(
            o.get("type").unwrap_or(&Value::Null),
            &format!("{field}.type"),
        )?;
        if PYTHON_RULE_TYPES.contains(&ty) {
            return Err(unsupported(
                &format!("{field}.type"),
                Some(&format!(
                    "'{ty}' requires the Python runtime — not supported on VRT"
                )),
            ));
        }
        if !MODULE_RULE_TYPES.contains(&ty) {
            return Err(invalid(
                &format!("{field}.type"),
                &format!("must be one of {}", MODULE_RULE_TYPES.join(", ")),
            ));
        }
        let globs_arr = o
            .get("globs")
            .ok_or_else(|| invalid(&format!("{field}.globs"), "is required"))?
            .as_array()
            .ok_or_else(|| invalid(&format!("{field}.globs"), "must be an array"))?;
        let mut globs = Vec::with_capacity(globs_arr.len());
        for (j, g) in globs_arr.iter().enumerate() {
            globs.push(Value::from(req_str(g, &format!("{field}.globs[{j}]"))?));
        }
        let mut m = Map::new();
        m.insert("type".into(), Value::from(ty));
        m.insert("globs".into(), Value::Array(globs));
        if let Some(ft) = o.get("fallthrough") {
            if !ft.is_boolean() {
                return Err(invalid(
                    &format!("{field}.fallthrough"),
                    "must be a boolean",
                ));
            }
            m.insert("fallthrough".into(), ft.clone());
        }
        out_arr.push(Value::Object(m));
    }
    Ok(Value::Array(out_arr))
}

fn route_to_hostname(pattern: &str, field: &str) -> Result<String, ProjectConfigError> {
    let mut p = pattern.trim().to_lowercase();
    if let Some(stripped) = p.strip_suffix("/*") {
        p = stripped.to_string();
    }
    if p.contains('*') || p.contains('/') || p.contains(':') {
        return Err(unsupported(
            field,
            Some(&format!(
                "route '{pattern}' — only plain hostnames or <fqdn>/* are supported"
            )),
        ));
    }
    Ok(p)
}

pub fn normalize_wrangler_config(cfg: &Value, file: &str) -> Result<Value, ProjectConfigError> {
    let obj = cfg
        .as_object()
        .ok_or_else(|| invalid_code("VRT_CONFIG_INVALID", file, "config must be an object"))?;
    let mut out = Map::new();
    out.insert("schemaVersion".into(), Value::from(1));
    let mut bindings: Vec<Value> = Vec::new();
    let mut hostnames: Vec<String> = Vec::new();
    let mut custom_domains: Vec<String> = Vec::new();
    let mut routes_seen = false;
    let mut route_seen = false;
    // Wrangler bundler knobs fold into one normalized `bundle` doc —
    // wrangler's default is bundling ON (`no_bundle` absent), so the
    // doc is always emitted for wrangler sources.
    let mut bundle: Map<String, Value> = Map::new();

    for (key, value) in obj {
        match key.as_str() {
            "name" => {
                out.insert("name".into(), Value::from(req_str(value, "name")?));
            }
            "main" => {
                // Wrangler accepts `main = "./x.js"` and normalizes the
                // leading `./`; the signed bundle forbids a `.` path
                // segment, so fold it here rather than fail at pack.
                let mut main = req_str(value, "main")?;
                while let Some(rest) = main.strip_prefix("./") {
                    main = rest;
                }
                let main = main.to_string();
                out.insert("main".into(), Value::from(main));
            }
            "compatibility_date" => {
                out.insert(
                    "compatibilityDate".into(),
                    Value::from(req_str(value, "compatibility_date")?),
                );
            }
            "compatibility_flags" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| invalid("compatibility_flags", "must be an array"))?;
                let flags: Result<Vec<Value>, _> = arr
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        req_str(v, &format!("compatibility_flags[{i}]")).map(|s| Value::from(s))
                    })
                    .collect();
                out.insert("compatibilityFlags".into(), Value::Array(flags?));
            }
            "limits" => {
                out.insert("quotas".into(), normalize_limits(value)?);
            }
            // CF `[dev]` — wrangler dev-server settings, session-local
            // and never part of a deployment: accepted and dropped
            // here (mirroring the TS ignore list) so a config `wrangler
            // deploy` accepts stays deployable — `ip = "0.0.0.0"` or an
            // `inspector_port` must not fail `of deploy`. `of dev`
            // re-validates it strictly at session start via
            // `parse_dev_table` on the env-resolved config.
            "dev" => {}
            "vars" => {
                let vars = value
                    .as_object()
                    .ok_or_else(|| invalid("vars", "must be a table"))?;
                let mut out_vars = Map::new();
                for (k, v) in vars {
                    if !is_json_value(v) {
                        return Err(invalid(&format!("vars.{k}"), "must be a JSON value"));
                    }
                    out_vars.insert(k.clone(), v.clone());
                }
                out.insert("vars".into(), Value::Object(out_vars));
            }
            "kv_namespaces" => {
                for (i, e) in table_list(value, "kv_namespaces")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "id" | "preview_id" | "remote" => {}
                            _ => return Err(unsupported(&format!("kv_namespaces[{i}].{k}"), None)),
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("kv"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("kv_namespaces[{i}].binding"),
                        )?),
                    );
                    if let Some(id) = opt_str(e.get("id"), &format!("kv_namespaces[{i}].id"))? {
                        b.insert("id".into(), Value::from(id));
                    }
                    bindings.push(Value::Object(b));
                }
            }
            "d1_databases" => {
                for (i, e) in table_list(value, "d1_databases")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            // database_id is a USER-side identifier only;
                            // the server mints the real databaseId.
                            "binding"
                            | "database_name"
                            | "database_id"
                            | "preview_database_id"
                            | "remote" => {}
                            _ => return Err(unsupported(&format!("d1_databases[{i}].{k}"), None)),
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("d1"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("d1_databases[{i}].binding"),
                        )?),
                    );
                    if let Some(id) = opt_str(
                        e.get("database_id"),
                        &format!("d1_databases[{i}].database_id"),
                    )? {
                        b.insert("databaseId".into(), Value::from(id));
                    }
                    if let Some(n) = opt_str(
                        e.get("database_name"),
                        &format!("d1_databases[{i}].database_name"),
                    )? {
                        b.insert("databaseName".into(), Value::from(n));
                    }
                    bindings.push(Value::Object(b));
                }
            }
            "hyperdrive" => {
                // MIG-15: mirrors wranglerSource.ts exactly — `id` is
                // type-checked then ignored (never an identity);
                // `localConnectionString`/`remote` are dev-only keys
                // dropped silently; `database_tenant` is REQUIRED and
                // names a vrt DatabaseTenant resolved server-side in
                // the deploy team's scope. Any other key fails closed.
                for (i, e) in table_list(value, "hyperdrive")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "id" | "database_tenant" => {}
                            "localConnectionString" | "remote" => {}
                            _ => return Err(unsupported(&format!("hyperdrive[{i}].{k}"), None)),
                        }
                    }
                    // Type-check only — the value is never used.
                    opt_str(e.get("id"), &format!("hyperdrive[{i}].id"))?;
                    let tenant = req_str(
                        e.get("database_tenant").unwrap_or(&Value::Null),
                        &format!("hyperdrive[{i}].database_tenant"),
                    )?;
                    {
                        let ok = !tenant.is_empty()
                            && tenant.len() <= 63
                            && tenant
                                .bytes()
                                .next()
                                .map(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                                .unwrap_or(false)
                            && tenant.bytes().all(|c| {
                                c.is_ascii_lowercase()
                                    || c.is_ascii_digit()
                                    || c == b'-'
                                    || c == b'_'
                            });
                        if !ok {
                            return Err(invalid(
                                &format!("hyperdrive[{i}].database_tenant"),
                                "must be a vrt database tenant name (lowercase letters, digits, '-' or '_', max 63)",
                            ));
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("hyperdrive"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("hyperdrive[{i}].binding"),
                        )?),
                    );
                    b.insert("databaseTenant".into(), Value::from(tenant));
                    bindings.push(Value::Object(b));
                }
            }
            "ratelimits" => {
                // MIG-16: mirrors wranglerSource.ts exactly — name,
                // namespace_id (1..32 decimal digits, STRING), simple =
                // { limit 1..=1_000_000, period 10|60 }; <= 16 entries;
                // unknown keys fail closed.
                let entries = table_list(value, "ratelimits")?;
                if entries.len() > 16 {
                    return Err(invalid("ratelimits", "at most 16 entries per worker"));
                }
                for (i, e) in entries.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "name" | "namespace_id" | "simple" => {}
                            _ => return Err(unsupported(&format!("ratelimits[{i}].{k}"), None)),
                        }
                    }
                    let name = req_str(
                        e.get("name").unwrap_or(&Value::Null),
                        &format!("ratelimits[{i}].name"),
                    )?;
                    let ns = match e.get("namespace_id").and_then(|v| v.as_str()) {
                        Some(s)
                            if !s.is_empty()
                                && s.len() <= 32
                                && s.bytes().all(|c| c.is_ascii_digit()) =>
                        {
                            s
                        }
                        _ => {
                            return Err(invalid(
                                &format!("ratelimits[{i}].namespace_id"),
                                "must be a decimal-digit string of 1..32 digits",
                            ))
                        }
                    };
                    let simple = e.get("simple").and_then(|v| v.as_object()).ok_or_else(|| {
                        invalid(&format!("ratelimits[{i}].simple"), "must be a table")
                    })?;
                    for k in simple.keys() {
                        match k.as_str() {
                            "limit" | "period" => {}
                            _ => {
                                return Err(unsupported(
                                    &format!("ratelimits[{i}].simple.{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let limit = match simple.get("limit").and_then(|v| v.as_u64()) {
                        Some(n) if (1..=1_000_000).contains(&n) => n,
                        _ => {
                            return Err(invalid(
                                &format!("ratelimits[{i}].simple.limit"),
                                "must be an integer in 1..1000000",
                            ))
                        }
                    };
                    let period = match simple.get("period").and_then(|v| v.as_u64()) {
                        Some(n) if n == 10 || n == 60 => n,
                        _ => {
                            return Err(invalid(
                                &format!("ratelimits[{i}].simple.period"),
                                "must be 10 or 60",
                            ))
                        }
                    };
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("ratelimit"));
                    b.insert("name".into(), Value::from(name));
                    b.insert("namespaceId".into(), Value::from(ns));
                    b.insert("limit".into(), Value::from(limit));
                    b.insert("period".into(), Value::from(period));
                    bindings.push(Value::Object(b));
                }
            }
            "r2_buckets" => {
                for (i, e) in table_list(value, "r2_buckets")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "bucket_name" => {}
                            _ => return Err(unsupported(&format!("r2_buckets[{i}].{k}"), None)),
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("r2"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("r2_buckets[{i}].binding"),
                        )?),
                    );
                    b.insert(
                        "bucketName".into(),
                        Value::from(req_str(
                            e.get("bucket_name").unwrap_or(&Value::Null),
                            &format!("r2_buckets[{i}].bucket_name"),
                        )?),
                    );
                    bindings.push(Value::Object(b));
                }
            }
            "services" => {
                for (i, e) in table_list(value, "services")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "service" | "entrypoint" | "props" => {}
                            _ => return Err(unsupported(&format!("services[{i}].{k}"), None)),
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("service"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("services[{i}].binding"),
                        )?),
                    );
                    b.insert(
                        "service".into(),
                        Value::from(req_str(
                            e.get("service").unwrap_or(&Value::Null),
                            &format!("services[{i}].service"),
                        )?),
                    );
                    if let Some(ep) =
                        opt_str(e.get("entrypoint"), &format!("services[{i}].entrypoint"))?
                    {
                        b.insert("entrypoint".into(), Value::from(ep));
                    }
                    if let Some(props) = e.get("props") {
                        if !snapshot_protocol::compute_desired::valid_service_props(props) {
                            return Err(invalid(
                                &format!("services[{i}].props"),
                                "must be a JSON object of at most 8 KiB",
                            ));
                        }
                        b.insert("props".into(), props.clone());
                    }
                    bindings.push(Value::Object(b));
                }
            }
            "analytics_engine_datasets" => {
                // `[[analytics_engine_datasets]]` → analyticsEngine
                // bindings. `dataset` defaults to the binding name
                // (mirrors CF wrangler). `version` is rejected —
                // upstream only ever shipped v1/v2 and now treats the
                // key as deprecated; fail closed on unknown keys.
                for (i, e) in table_list(value, "analytics_engine_datasets")?
                    .iter()
                    .enumerate()
                {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "dataset" | "remote" => {}
                            _ => {
                                return Err(unsupported(
                                    &format!("analytics_engine_datasets[{i}].{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let binding = req_str(
                        e.get("binding").unwrap_or(&Value::Null),
                        &format!("analytics_engine_datasets[{i}].binding"),
                    )?;
                    let dataset = opt_str(
                        e.get("dataset"),
                        &format!("analytics_engine_datasets[{i}].dataset"),
                    )?
                    .unwrap_or(binding);
                    if dataset.is_empty()
                        || dataset.len() > 256
                        || !dataset
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    {
                        return Err(invalid(
                            &format!("analytics_engine_datasets[{i}].dataset"),
                            "must match [A-Za-z0-9_]{1,256}",
                        ));
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("analyticsEngine"));
                    b.insert("name".into(), Value::from(binding));
                    b.insert("dataset".into(), Value::from(dataset));
                    bindings.push(Value::Object(b));
                }
            }
            "flagship" => {
                // CF `[[flagship]] {binding, app_id}` — the Flagship
                // feature-flag app binding; `env.<binding>` is the
                // async evaluation API. `remote` is a CF local-dev
                // knob (evaluate against the remote app instead of the
                // local flag store): VRT has no remote/local split —
                // evaluation always uses the propagated config — so it
                // is accepted and ignored, same as AE's `remote`.
                for (i, e) in table_list(value, "flagship")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "app_id" | "remote" => {}
                            _ => return Err(unsupported(&format!("flagship[{i}].{k}"), None)),
                        }
                    }
                    let binding = req_str(
                        e.get("binding").unwrap_or(&Value::Null),
                        &format!("flagship[{i}].binding"),
                    )?;
                    let app_id = req_str(
                        e.get("app_id").unwrap_or(&Value::Null),
                        &format!("flagship[{i}].app_id"),
                    )?;
                    if app_id.len() > 128 {
                        return Err(invalid(
                            &format!("flagship[{i}].app_id"),
                            "must be at most 128 chars",
                        ));
                    }
                    if let Some(v) = e.get("remote") {
                        if !v.is_boolean() {
                            return Err(invalid(
                                &format!("flagship[{i}].remote"),
                                "must be a boolean",
                            ));
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("flagship"));
                    b.insert("name".into(), Value::from(binding));
                    b.insert("appId".into(), Value::from(app_id));
                    bindings.push(Value::Object(b));
                }
            }
            "k2" => {
                // CF `[[k2]] {binding, stream}` — the K2 durable event
                // log produce binding; `env.<binding>.send(records)`
                // resolves `{success}|{error}` (never throws). `stream`
                // is the user-side stream name; the 32-hex streamId is
                // derived at pack time (`vrt-k2:{tenant}:{name}`, same
                // derivation rule as queueId). `remote` is a CF
                // local-dev knob accepted and ignored (flagship/AE
                // precedent).
                for (i, e) in table_list(value, "k2")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "stream" | "remote" => {}
                            _ => return Err(unsupported(&format!("k2[{i}].{k}"), None)),
                        }
                    }
                    let binding = req_str(
                        e.get("binding").unwrap_or(&Value::Null),
                        &format!("k2[{i}].binding"),
                    )?;
                    let stream = req_str(
                        e.get("stream").unwrap_or(&Value::Null),
                        &format!("k2[{i}].stream"),
                    )?;
                    // Same charset derive_stream_id enforces
                    // ([A-Za-z0-9_-]{1,256}) — reject at normalize so
                    // the pack path never sees an underivable name
                    // (identical rule on the Vibeploy wranglerSource arm).
                    if stream.is_empty()
                        || stream.len() > 256
                        || !stream
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                    {
                        return Err(invalid(
                            &format!("k2[{i}].stream"),
                            "must match [A-Za-z0-9_-]{1,256}",
                        ));
                    }
                    if let Some(v) = e.get("remote") {
                        if !v.is_boolean() {
                            return Err(invalid(&format!("k2[{i}].remote"), "must be a boolean"));
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("k2"));
                    b.insert("name".into(), Value::from(binding));
                    b.insert("stream".into(), Value::from(stream));
                    bindings.push(Value::Object(b));
                }
            }
            "send_email" => {
                // CF `[[send_email]] {name, destination_address?,
                // allowed_destination_addresses?,
                // allowed_sender_addresses?}` — Email Service send
                // binding. `destination_address` pins every send;
                // `allowed_*` are allowlists (pin vs list are mutually
                // exclusive per CF send-bindings).
                for (i, e) in table_list(value, "send_email")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "name" | "destination_address"
                            | "allowed_destination_addresses" | "allowed_sender_addresses" => {}
                            _ => {
                                return Err(unsupported(&format!("send_email[{i}].{k}"), None))
                            }
                        }
                    }
                    let name = req_str(
                        e.get("name").unwrap_or(&Value::Null),
                        &format!("send_email[{i}].name"),
                    )?;
                    let destination = opt_str(
                        e.get("destination_address"),
                        &format!("send_email[{i}].destination_address"),
                    )?;
                    let allowed_dest = opt_str_list(
                        e.get("allowed_destination_addresses"),
                        &format!("send_email[{i}].allowed_destination_addresses"),
                    )?;
                    let allowed_senders = opt_str_list(
                        e.get("allowed_sender_addresses"),
                        &format!("send_email[{i}].allowed_sender_addresses"),
                    )?;
                    if destination.is_some() && !allowed_dest.is_empty() {
                        return Err(invalid(
                            &format!("send_email[{i}]"),
                            "destination_address and allowed_destination_addresses are mutually exclusive",
                        ));
                    }
                    for addr in destination
                        .iter()
                        .chain(allowed_dest.iter())
                        .chain(allowed_senders.iter())
                    {
                        if !valid_email(addr) {
                            return Err(invalid(
                                &format!("send_email[{i}]"),
                                "invalid email address",
                            ));
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("send_email"));
                    b.insert("name".into(), Value::from(name));
                    if let Some(d) = destination {
                        b.insert("destinationAddress".into(), Value::from(d));
                    }
                    if !allowed_dest.is_empty() {
                        b.insert(
                            "allowedDestinationAddresses".into(),
                            Value::Array(allowed_dest.into_iter().map(Value::from).collect()),
                        );
                    }
                    if !allowed_senders.is_empty() {
                        b.insert(
                            "allowedSenderAddresses".into(),
                            Value::Array(
                                allowed_senders.into_iter().map(Value::from).collect(),
                            ),
                        );
                    }
                    bindings.push(Value::Object(b));
                }
            }
            "secrets_store_secrets" => {
                // CF `[[secrets_store_secrets]] {binding, store_id?,
                // secret_name}` — Secrets Store binding; `env.<binding>`
                // is a read-only `SecretsStoreSecret` stub (`.get()`).
                // `store_id` defaults to `"default"` per CF; `remote`
                // does not exist for this binding. The value resolves
                // from the tenant secret map under the derived
                // `secrets-store:{store_id}:{secret_name}` ref.
                for (i, e) in table_list(value, "secrets_store_secrets")?
                    .iter()
                    .enumerate()
                {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "store_id" | "secret_name" => {}
                            _ => {
                                return Err(unsupported(
                                    &format!("secrets_store_secrets[{i}].{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let binding = req_str(
                        e.get("binding").unwrap_or(&Value::Null),
                        &format!("secrets_store_secrets[{i}].binding"),
                    )?;
                    let secret_name = req_str(
                        e.get("secret_name").unwrap_or(&Value::Null),
                        &format!("secrets_store_secrets[{i}].secret_name"),
                    )?;
                    let store_id = opt_str(
                        e.get("store_id"),
                        &format!("secrets_store_secrets[{i}].store_id"),
                    )?
                    .unwrap_or("default");
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("secrets_store"));
                    b.insert("name".into(), Value::from(binding));
                    b.insert("storeId".into(), Value::from(store_id));
                    b.insert("secretName".into(), Value::from(secret_name));
                    bindings.push(Value::Object(b));
                }
            }
            "images" => {
                // CF `[images] {binding}` — the Images binding
                // (`env.<binding>.info()`/`.input().transform().output()`);
                // transforms run inside the facade-images worker on the
                // vendored WASM engine. `remote` is a CF local-dev knob
                // accepted and ignored (k2/flagship/AE precedent).
                let t = value
                    .as_object()
                    .ok_or_else(|| invalid("images", "must be a table"))?;
                for k in t.keys() {
                    match k.as_str() {
                        "binding" | "remote" => {}
                        _ => return Err(unsupported(&format!("images.{k}"), None)),
                    }
                }
                let binding = req_str(
                    t.get("binding").unwrap_or(&Value::Null),
                    "images.binding",
                )?;
                if let Some(v) = t.get("remote") {
                    if !v.is_boolean() {
                        return Err(invalid("images.remote", "must be a boolean"));
                    }
                }
                let mut b = Map::new();
                b.insert("type".into(), Value::from("images"));
                b.insert("name".into(), Value::from(binding));
                bindings.push(Value::Object(b));
            }
            "version_metadata" => {
                // CF `[version_metadata] {binding}` — `env.<binding>`
                // is `{id, tag, timestamp}` of the worker's active
                // version (`WorkerVersionMetadata`). One binding only;
                // `tag`/`timestamp` are version-scoped (deploy `--tag`
                // + record creation), not config keys.
                let t = value
                    .as_object()
                    .ok_or_else(|| invalid("version_metadata", "must be a table"))?;
                for k in t.keys() {
                    match k.as_str() {
                        "binding" => {}
                        _ => return Err(unsupported(&format!("version_metadata.{k}"), None)),
                    }
                }
                let binding = req_str(
                    t.get("binding").unwrap_or(&Value::Null),
                    "version_metadata.binding",
                )?;
                let mut b = Map::new();
                b.insert("type".into(), Value::from("versionMetadata"));
                b.insert("name".into(), Value::from(binding));
                bindings.push(Value::Object(b));
            }
            "vectorize" => {
                // CF `[[vectorize]] {binding, index_name}` — the
                // Vectorize binding (v2 GA wire). CF carries
                // dimensions/metric on the provisioned index; the VRT
                // spec is signed at deploy so both keys are REQUIRED
                // here (documented deviation — there is no separate
                // index-provisioning API surface).
                for (i, e) in table_list(value, "vectorize")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "index_name" | "dimensions" | "metric" | "remote" => {}
                            _ => return Err(unsupported(&format!("vectorize[{i}].{k}"), None)),
                        }
                    }
                    let binding = req_str(
                        e.get("binding").unwrap_or(&Value::Null),
                        &format!("vectorize[{i}].binding"),
                    )?;
                    let index_name = req_str(
                        e.get("index_name").unwrap_or(&Value::Null),
                        &format!("vectorize[{i}].index_name"),
                    )?;
                    if index_name.is_empty()
                        || index_name.len() > 64
                        || !index_name
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                        || index_name.starts_with('-')
                        || index_name.ends_with('-')
                    {
                        return Err(invalid(
                            &format!("vectorize[{i}].index_name"),
                            "must be 1-64 chars of lowercase kebab-case",
                        ));
                    }
                    let dimensions = e
                        .get("dimensions")
                        .and_then(|v| v.as_u64())
                        .filter(|d| (1..=1536).contains(d))
                        .ok_or_else(|| {
                            invalid(
                                &format!("vectorize[{i}].dimensions"),
                                "must be an integer in 1..=1536",
                            )
                        })?;
                    let metric = e
                        .get("metric")
                        .and_then(|v| v.as_str())
                        .filter(|m| matches!(*m, "cosine" | "euclidean" | "dot-product"))
                        .ok_or_else(|| {
                            invalid(
                                &format!("vectorize[{i}].metric"),
                                "must be cosine, euclidean or dot-product",
                            )
                        })?;
                    if let Some(v) = e.get("remote") {
                        if !v.is_boolean() {
                            return Err(invalid(
                                &format!("vectorize[{i}].remote"),
                                "must be a boolean",
                            ));
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("vectorize"));
                    b.insert("name".into(), Value::from(binding));
                    b.insert("index_name".into(), Value::from(index_name));
                    b.insert("dimensions".into(), Value::from(dimensions));
                    b.insert("metric".into(), Value::from(metric));
                    bindings.push(Value::Object(b));
                }
            }
            "streaming_tail_consumers" => {
                // CF `streaming_tail_consumers = [{service = "..."}]` —
                // same-table shape as `tail_consumers` (worker-level,
                // same-tenant consumer referenced by name) but the
                // consumer receives TailStream events via its
                // `tailStream()` handler instead of batched `tail()`
                // calls — workerd `streamingTails` designators.
                // `{service}` is the only key CF's schema accepts.
                let mut consumers: Vec<Value> = Vec::new();
                for (i, e) in table_list(value, "streaming_tail_consumers")?
                    .iter()
                    .enumerate()
                {
                    for k in e.keys() {
                        if k.as_str() != "service" {
                            return Err(unsupported(
                                &format!("streaming_tail_consumers[{i}].{k}"),
                                None,
                            ));
                        }
                    }
                    let service = req_str(
                        e.get("service").unwrap_or(&Value::Null),
                        &format!("streaming_tail_consumers[{i}].service"),
                    )?;
                    let mut c = Map::new();
                    c.insert("service".into(), Value::from(service));
                    consumers.push(Value::Object(c));
                }
                out.insert("streamingTailConsumers".into(), Value::Array(consumers));
            }
            "worker_loaders" => {
                // Dynamic Workers (CF `[[worker_loaders]]`): a loader
                // carries only a binding name — the tenant supplies code
                // at runtime via `env.<binding>.get()/.load()`. Any
                // other key fails closed.
                for (i, e) in table_list(value, "worker_loaders")?.iter().enumerate() {
                    for k in e.keys() {
                        if k != "binding" {
                            return Err(unsupported(&format!("worker_loaders[{i}].{k}"), None));
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("workerLoader"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("worker_loaders[{i}].binding"),
                        )?),
                    );
                    bindings.push(Value::Object(b));
                }
            }
            "observability" => {
                // CF `[observability]` — Worker Logs gates + head
                // sampling, enforced by the platform `vrt-tail`
                // collector per worker. `enabled`/`logs.enabled`
                // false => the cell renders NO collector (nothing is
                // collected); `logs.head_sampling_rate` (or the
                // deprecated top-level alias) keeps that fraction of
                // INVOCATIONS whole — a dropped invocation keeps none
                // of its records; `logs.invocation_logs` gates just
                // the outcome record. `traces`/`persist`/`destinations`
                // have no counterpart here and fail closed.
                let t = value
                    .as_object()
                    .ok_or_else(|| invalid("observability", "must be a table"))?;
                let mut enabled = None;
                let mut rate_top = None;
                let mut rate_logs = None;
                let mut logs_enabled = None;
                let mut invocation_logs = None;
                for (k, v) in t {
                    match k.as_str() {
                        "enabled" => enabled = opt_bool(Some(v), "observability.enabled")?,
                        "head_sampling_rate" => {
                            rate_top = opt_rate(Some(v), "observability.head_sampling_rate")?
                        }
                        "logs" => {
                            let lt = v
                                .as_object()
                                .ok_or_else(|| invalid("observability.logs", "must be a table"))?;
                            for (lk, lv) in lt {
                                match lk.as_str() {
                                    "enabled" => {
                                        logs_enabled =
                                            opt_bool(Some(lv), "observability.logs.enabled")?
                                    }
                                    "head_sampling_rate" => {
                                        rate_logs = opt_rate(
                                            Some(lv),
                                            "observability.logs.head_sampling_rate",
                                        )?
                                    }
                                    "invocation_logs" => {
                                        invocation_logs = opt_bool(
                                            Some(lv),
                                            "observability.logs.invocation_logs",
                                        )?
                                    }
                                    _ => {
                                        return Err(unsupported(
                                            &format!("observability.logs.{lk}"),
                                            None,
                                        ))
                                    }
                                }
                            }
                        }
                        _ => return Err(unsupported(&format!("observability.{k}"), None)),
                    }
                }
                // When both the modern `logs.head_sampling_rate` and the
                // deprecated top-level alias are present, `logs.*` wins
                // (CF treats it as authoritative) — refusing the pair
                // would reject a valid, unambiguous CF config. The
                // alias is only a fallback for configs that predate the
                // nested table.
                let rate = rate_logs.or(rate_top).unwrap_or(1.0);
                if !(0.0..=1.0).contains(&rate) {
                    return Err(invalid(
                        "observability.head_sampling_rate",
                        "must be in [0,1]",
                    ));
                }
                // PPM integer — the signed wire's canonical JSON is
                // safe-integer-only (deterministic across Rust/TS).
                let ppm = (rate * 1_000_000.0).round() as u64;
                let mut o = Map::new();
                o.insert("enabled".into(), Value::from(enabled.unwrap_or(true)));
                o.insert(
                    "logsEnabled".into(),
                    Value::from(logs_enabled.unwrap_or(true)),
                );
                o.insert(
                    "invocationLogs".into(),
                    Value::from(invocation_logs.unwrap_or(true)),
                );
                o.insert("headSamplingRatePpm".into(), Value::from(ppm));
                out.insert("observability".into(), Value::Object(o));
            }
            "unsafe" => {
                // CF `[unsafe]` — raw-binding escape hatch
                // (`[[unsafe.bindings]] {name, type}`): the entry is
                // forwarded verbatim into upload metadata, so `type`
                // may name any binding kind. `unsafe_eval` is the ONE
                // type with no proper wrangler key that workerd runs
                // (native `unsafeEval` capability object —
                // eval/newFunction/newWasmModule); every other type has
                // a first-class key
                // in this surface or no runtime support, so it fails
                // closed with guidance. `unsafe.capnp`/`unsafe.metadata`
                // and friends inject raw workerd/upload structures —
                // rejected: there is no faithful platform projection.
                let table = value
                    .as_object()
                    .ok_or_else(|| unsupported("unsafe", Some("table")))?;
                for (k, v) in table {
                    if k != "bindings" {
                        return Err(unsupported(
                            &format!("unsafe.{k}"),
                            Some("only [[unsafe.bindings]] is supported"),
                        ));
                    }
                    for (i, et) in table_list(v, "unsafe.bindings")?.iter().enumerate() {
                        for k in et.keys() {
                            match k.as_str() {
                                "name" | "type" => {}
                                _ => {
                                    return Err(unsupported(
                                        &format!("unsafe.bindings[{i}].{k}"),
                                        None,
                                    ))
                                }
                            }
                        }
                        let name = req_str(
                            et.get("name").unwrap_or(&Value::Null),
                            &format!("unsafe.bindings[{i}].name"),
                        )?;
                        let ty = req_str(
                            et.get("type").unwrap_or(&Value::Null),
                            &format!("unsafe.bindings[{i}].type"),
                        )?;
                        if ty != "unsafe_eval" {
                            return Err(unsupported(
                                &format!("unsafe.bindings[{i}].type"),
                                Some(&format!(
                                    "`{ty}` has no unsafe-bindings support — use the first-class \
                                     key for this binding type (only `unsafe_eval` is supported)"
                                )),
                            ));
                        }
                        let mut b = Map::new();
                        b.insert("type".into(), Value::from("unsafeEval"));
                        b.insert("name".into(), Value::from(name));
                        bindings.push(Value::Object(b));
                    }
                }
            }
            "pg" => {
                // P1: `[[pg]] binding = "DB", id = "<logical tenant>"
                // — credentials NEVER in toml (secrets/pg-credentials.json).
                for (i, e) in table_list(value, "pg")?.iter().enumerate() {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "id" | "endpoint" | "tenant_slug" | "read_only" => {}
                            _ => return Err(unsupported(&format!("pg[{i}].{k}"), None)),
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("pg"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("pg[{i}].binding"),
                        )?),
                    );
                    b.insert(
                        "id".into(),
                        Value::from(req_str(
                            e.get("id").unwrap_or(&Value::Null),
                            &format!("pg[{i}].id"),
                        )?),
                    );
                    bindings.push(Value::Object(b));
                }
            }
            "queues" => {
                // T37: `queues.producers[]` → queue bindings.
                // T37b: `queues.consumers[]` -> `queueConsumers` (normalized
                // camelCase, field allowlist aligned with the CF wrangler schema;
                // `queue`/`dead_letter_queue` are display names, queueId is
                // derived at pack time from vrt-q:{tenant}:{name}). Other keys fail
                // closed.
                let q = value
                    .as_object()
                    .ok_or_else(|| invalid("queues", "must be a table"))?;
                for k in q.keys() {
                    if k != "producers" && k != "consumers" {
                        return Err(unsupported(&format!("queues.{k}"), None));
                    }
                }
                for (i, e) in table_list(
                    q.get("producers").unwrap_or(&Value::Array(vec![])),
                    "queues.producers",
                )?
                .iter()
                .enumerate()
                {
                    for k in e.keys() {
                        match k.as_str() {
                            "binding" | "queue" | "delivery_delay" => {}
                            _ => {
                                return Err(unsupported(
                                    &format!("queues.producers[{i}].{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let mut b = Map::new();
                    b.insert("type".into(), Value::from("queue"));
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("queues.producers[{i}].binding"),
                        )?),
                    );
                    b.insert(
                        "queue".into(),
                        Value::from(req_str(
                            e.get("queue").unwrap_or(&Value::Null),
                            &format!("queues.producers[{i}].queue"),
                        )?),
                    );
                    // Cloudflare `delivery_delay`: queue-level default
                    // send delay in seconds. Preserved on the normalized
                    // binding so the standalone packer can carry it into
                    // the signed snapshot.
                    if let Some(n) = opt_u64(
                        e.get("delivery_delay"),
                        &format!("queues.producers[{i}].delivery_delay"),
                    )? {
                        if n > 86_400 {
                            return Err(invalid(
                                &format!("queues.producers[{i}].delivery_delay"),
                                "must be in 0..=86400 seconds",
                            ));
                        }
                        b.insert("deliveryDelaySeconds".into(), Value::from(n));
                    }
                    bindings.push(Value::Object(b));
                }
                let mut consumers = Vec::new();
                for (i, e) in table_list(
                    q.get("consumers").unwrap_or(&Value::Array(vec![])),
                    "queues.consumers",
                )?
                .iter()
                .enumerate()
                {
                    for k in e.keys() {
                        match k.as_str() {
                            "queue" | "max_batch_size" | "max_batch_timeout" | "max_retries"
                            | "dead_letter_queue" | "retry_delay" | "max_concurrency" => {}
                            _ => {
                                return Err(unsupported(
                                    &format!("queues.consumers[{i}].{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let mut c = Map::new();
                    c.insert(
                        "queue".into(),
                        Value::from(req_str(
                            e.get("queue").unwrap_or(&Value::Null),
                            &format!("queues.consumers[{i}].queue"),
                        )?),
                    );
                    for (snake, camel) in [
                        ("max_batch_size", "maxBatchSize"),
                        ("max_retries", "maxRetries"),
                        ("max_concurrency", "maxConcurrency"),
                    ] {
                        if let Some(n) =
                            opt_u64(e.get(snake), &format!("queues.consumers[{i}].{snake}"))?
                        {
                            c.insert(camel.into(), Value::from(n));
                        }
                    }
                    // seconds -> milliseconds / same-name field -- the normalized schema matches TS
                    // (maxBatchTimeoutMs / retryDelaySeconds).
                    if let Some(n) = opt_u64(
                        e.get("max_batch_timeout"),
                        &format!("queues.consumers[{i}].max_batch_timeout"),
                    )? {
                        if n > 60 {
                            return Err(invalid(
                                &format!("queues.consumers[{i}].max_batch_timeout"),
                                "must be in 0..=60 seconds",
                            ));
                        }
                        c.insert("maxBatchTimeoutMs".into(), Value::from(n * 1000));
                    }
                    if let Some(n) = opt_u64(
                        e.get("retry_delay"),
                        &format!("queues.consumers[{i}].retry_delay"),
                    )? {
                        if n > 86_400 {
                            return Err(invalid(
                                &format!("queues.consumers[{i}].retry_delay"),
                                "must be in 0..=86400 seconds",
                            ));
                        }
                        c.insert("retryDelaySeconds".into(), Value::from(n));
                    }
                    if let Some(dlq) = opt_str(
                        e.get("dead_letter_queue"),
                        &format!("queues.consumers[{i}].dead_letter_queue"),
                    )? {
                        c.insert("deadLetterQueue".into(), Value::from(dlq));
                    }
                    consumers.push(Value::Object(c));
                }
                if !consumers.is_empty() {
                    out.insert("queueConsumers".into(), Value::Array(consumers));
                }
            }
            "durable_objects" => {
                // P3c-1: `durable_objects.bindings[]` →
                // `durableObjectBindings` (camelCase normalization; allowlist aligned with
                // the CF wrangler schema -- fields such as `environment`/`experimental_
                // version` are out of scope this round -> unsupported).
                let d = value
                    .as_object()
                    .ok_or_else(|| invalid("durable_objects", "must be a table"))?;
                for k in d.keys() {
                    if k != "bindings" {
                        return Err(unsupported(&format!("durable_objects.{k}"), None));
                    }
                }
                let mut arr = Vec::new();
                for (i, e) in table_list(
                    d.get("bindings").unwrap_or(&Value::Array(vec![])),
                    "durable_objects.bindings",
                )?
                .iter()
                .enumerate()
                {
                    for k in e.keys() {
                        match k.as_str() {
                            "name" | "class_name" | "script_name" => {}
                            _ => {
                                return Err(unsupported(
                                    &format!("durable_objects.bindings[{i}].{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let mut b = Map::new();
                    b.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("name").unwrap_or(&Value::Null),
                            &format!("durable_objects.bindings[{i}].name"),
                        )?),
                    );
                    b.insert(
                        "className".into(),
                        Value::from(req_str(
                            e.get("class_name").unwrap_or(&Value::Null),
                            &format!("durable_objects.bindings[{i}].class_name"),
                        )?),
                    );
                    if let Some(sn) = opt_str(
                        e.get("script_name"),
                        &format!("durable_objects.bindings[{i}].script_name"),
                    )? {
                        b.insert("scriptName".into(), Value::from(sn));
                    }
                    arr.push(Value::Object(b));
                }
                if !arr.is_empty() {
                    out.insert("durableObjectBindings".into(), Value::Array(arr));
                }
            }
            "migrations" => {
                // P3c-1: `[[migrations]]` -> `durableObjectMigrations`.
                // CF's cumulative migration semantics are evaluated in pack.
                // DO1 standalone decision: `transferred_classes` stays
                // REJECTED here — standalone mints uniqueKey as
                // HMAC(tenant:worker:class), so a transfer would
                // silently swap the storage identity onto a fresh
                // dir (data left under the source's segment). The
                // platform runtime carries `transferFromWorkerId`
                // lineage in the signed desired; standalone has no
                // control plane to mint it → explicit unsupported,
                // never a silent wrong key. Other keys fail closed.
                let arr = value
                    .as_array()
                    .ok_or_else(|| invalid("migrations", "must be an array of tables"))?;
                let mut out_arr = Vec::with_capacity(arr.len());
                for (i, e) in arr.iter().enumerate() {
                    let e = e
                        .as_object()
                        .ok_or_else(|| invalid(&format!("migrations[{i}]"), "must be a table"))?;
                    for k in e.keys() {
                        match k.as_str() {
                            "tag"
                            | "new_classes"
                            | "new_sqlite_classes"
                            | "renamed_classes"
                            | "deleted_classes" => {}
                            "transferred_classes" => {
                                return Err(unsupported(
                                    &format!("migrations[{i}].transferred_classes"),
                                    Some(
                                        "cross-worker DO transfer needs the platform control plane's uniqueKey lineage; standalone derives keys from (tenant,worker,class) — deploy via the platform or use renamed_classes within one worker",
                                    ),
                                ))
                            }
                            _ => {
                                return Err(unsupported(
                                    &format!("migrations[{i}].{k}"),
                                    None,
                                ))
                            }
                        }
                    }
                    let mut m = Map::new();
                    if let Some(tag) = opt_str(e.get("tag"), &format!("migrations[{i}].tag"))? {
                        m.insert("tag".into(), Value::from(tag));
                    }
                    for (snake, camel) in [
                        ("new_classes", "newClasses"),
                        ("new_sqlite_classes", "newSqliteClasses"),
                        ("deleted_classes", "deletedClasses"),
                    ] {
                        if let Some(list) = e.get(snake) {
                            let names: Result<Vec<Value>, _> = list
                                .as_array()
                                .ok_or_else(|| {
                                    invalid(
                                        &format!("migrations[{i}].{snake}"),
                                        "must be an array of class names",
                                    )
                                })?
                                .iter()
                                .enumerate()
                                .map(|(j, v)| {
                                    req_str(v, &format!("migrations[{i}].{snake}[{j}]"))
                                        .map(Value::from)
                                })
                                .collect();
                            m.insert(camel.into(), Value::Array(names?));
                        }
                    }
                    if let Some(list) = e.get("renamed_classes") {
                        let mut renames = Vec::new();
                        for (j, r) in list
                            .as_array()
                            .ok_or_else(|| {
                                invalid(
                                    &format!("migrations[{i}].renamed_classes"),
                                    "must be an array of {from,to}",
                                )
                            })?
                            .iter()
                            .enumerate()
                        {
                            let r = r.as_object().ok_or_else(|| {
                                invalid(
                                    &format!("migrations[{i}].renamed_classes[{j}]"),
                                    "must be a table",
                                )
                            })?;
                            for k in r.keys() {
                                if k != "from" && k != "to" {
                                    return Err(unsupported(
                                        &format!("migrations[{i}].renamed_classes[{j}].{k}"),
                                        None,
                                    ));
                                }
                            }
                            renames.push(serde_json::json!({
                                "from": req_str(
                                    r.get("from").unwrap_or(&Value::Null),
                                    &format!("migrations[{i}].renamed_classes[{j}].from"),
                                )?,
                                "to": req_str(
                                    r.get("to").unwrap_or(&Value::Null),
                                    &format!("migrations[{i}].renamed_classes[{j}].to"),
                                )?,
                            }));
                        }
                        m.insert("renamedClasses".into(), Value::Array(renames));
                    }
                    out_arr.push(Value::Object(m));
                }
                if !out_arr.is_empty() {
                    out.insert("durableObjectMigrations".into(), Value::Array(out_arr));
                }
            }
            "triggers" => {
                let t = value
                    .as_object()
                    .ok_or_else(|| invalid("triggers", "must be a table"))?;
                for k in t.keys() {
                    if k != "crons" {
                        return Err(unsupported(&format!("triggers.{k}"), None));
                    }
                }
                if let Some(crons) = t.get("crons") {
                    let arr = crons
                        .as_array()
                        .ok_or_else(|| invalid("triggers.crons", "must be an array"))?;
                    let v: Result<Vec<Value>, _> = arr
                        .iter()
                        .enumerate()
                        .map(|(i, x)| {
                            req_str(x, &format!("triggers.crons[{i}]")).map(|s| Value::from(s))
                        })
                        .collect();
                    out.insert("crons".into(), Value::Array(v?));
                }
            }
            "route" | "routes" => {
                if key == "route" {
                    route_seen = true;
                } else {
                    routes_seen = true;
                }
                if route_seen && routes_seen {
                    return Err(invalid(
                        "route(s)",
                        "specify either \"route\" or \"routes\", not both",
                    ));
                }
                let entries: Vec<Value> = if key == "route" {
                    vec![value.clone()]
                } else {
                    value
                        .as_array()
                        .ok_or_else(|| invalid("routes", "must be an array"))?
                        .clone()
                };
                for (i, e) in entries.iter().enumerate() {
                    let field = format!("{key}[{i}]");
                    let pattern = match e {
                        Value::Object(o) => {
                            // Booleans first — they decide which CF route
                            // shape this entry is (`custom_domain: true` →
                            // CustomDomainRoute, else zone/pattern route).
                            for flag in ["custom_domain", "enabled", "previews_enabled"] {
                                if let Some(v) = o.get(flag) {
                                    if !v.is_boolean() {
                                        return Err(invalid(
                                            &format!("{field}.{flag}"),
                                            "must be a boolean",
                                        ));
                                    }
                                }
                            }
                            let is_custom = o
                                .get("custom_domain")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let enabled =
                                o.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);
                            // CF route-shape whitelist: zone refs are
                            // accepted and ignored (VRT has no zone
                            // registry); `enabled`/`previews_enabled` exist
                            // only on the CustomDomainRoute shape.
                            for k in o.keys() {
                                match k.as_str() {
                                    "pattern" | "custom_domain" | "zone_id" | "zone_name" => {}
                                    "enabled" | "previews_enabled" if is_custom => {}
                                    _ => return Err(unsupported(&format!("{field}.{k}"), None)),
                                }
                            }
                            let p = opt_str(o.get("pattern"), &format!("{field}.pattern"))?
                                .ok_or_else(|| {
                                    invalid(&field, "must be a pattern string or { pattern }")
                                })?;
                            if is_custom {
                                if enabled {
                                    // Custom domain: `pattern` IS the
                                    // hostname — no wildcard/suffix.
                                    custom_domains.push(validate_custom_domain(
                                        p,
                                        &format!("{field}.pattern"),
                                    )?);
                                }
                            } else {
                                hostnames.push(route_to_hostname(p, &field)?);
                            }
                            continue;
                        }
                        Value::String(_) => Some(e.as_str().unwrap()),
                        _ => {
                            return Err(invalid(&field, "must be a pattern string or { pattern }"))
                        }
                    }
                    .ok_or_else(|| invalid(&field, "must be a pattern string or { pattern }"))?;
                    hostnames.push(route_to_hostname(pattern, &field)?);
                }
            }
            "assets" => {
                let a = value
                    .as_object()
                    .ok_or_else(|| invalid("assets", "must be a table"))?;
                for k in a.keys() {
                    match k.as_str() {
                        "directory" | "binding" | "html_handling" | "not_found_handling"
                        | "run_worker_first" => {}
                        _ => return Err(unsupported(&format!("assets.{k}"), None)),
                    }
                }
                let mut assets = Map::new();
                assets.insert(
                    "directory".into(),
                    Value::from(req_str(
                        a.get("directory").unwrap_or(&Value::Null),
                        "assets.directory",
                    )?),
                );
                if let Some(b) = opt_str(a.get("binding"), "assets.binding")? {
                    assets.insert("binding".into(), Value::from(b));
                }
                if let Some(h) = opt_str(a.get("html_handling"), "assets.html_handling")? {
                    assets.insert("htmlHandling".into(), Value::from(h));
                }
                if let Some(n) = opt_str(a.get("not_found_handling"), "assets.not_found_handling")?
                {
                    assets.insert("notFoundHandling".into(), Value::from(n));
                }
                if let Some(r) = a.get("run_worker_first") {
                    assets.insert("runWorkerFirst".into(), normalize_run_worker_first(r)?);
                }
                out.insert("assets".into(), Value::Object(assets));
            }
            "workflows" => {
                // W1: `[[workflows]]` → `workflows` normalized entries.
                // Allowed keys per entry: name/binding/class_name/
                // script_name/limits{steps}; anything else fails closed
                // (same policy as durable_objects/migrations).
                let arr = value
                    .as_array()
                    .ok_or_else(|| invalid("workflows", "must be an array of tables"))?;
                let mut out_arr = Vec::with_capacity(arr.len());
                for (i, e) in arr.iter().enumerate() {
                    let e = e
                        .as_object()
                        .ok_or_else(|| invalid(&format!("workflows[{i}]"), "must be a table"))?;
                    for k in e.keys() {
                        match k.as_str() {
                            "name" | "binding" | "class_name" | "script_name" | "limits" => {}
                            _ => return Err(unsupported(&format!("workflows[{i}].{k}"), None)),
                        }
                    }
                    let mut w = Map::new();
                    w.insert(
                        "name".into(),
                        Value::from(req_str(
                            e.get("name").unwrap_or(&Value::Null),
                            &format!("workflows[{i}].name"),
                        )?),
                    );
                    w.insert(
                        "binding".into(),
                        Value::from(req_str(
                            e.get("binding").unwrap_or(&Value::Null),
                            &format!("workflows[{i}].binding"),
                        )?),
                    );
                    w.insert(
                        "className".into(),
                        Value::from(req_str(
                            e.get("class_name").unwrap_or(&Value::Null),
                            &format!("workflows[{i}].class_name"),
                        )?),
                    );
                    if let Some(sn) =
                        opt_str(e.get("script_name"), &format!("workflows[{i}].script_name"))?
                    {
                        w.insert("scriptName".into(), Value::from(sn));
                    }
                    if let Some(lim) = e.get("limits") {
                        let lim = lim.as_object().ok_or_else(|| {
                            invalid(&format!("workflows[{i}].limits"), "must be a table")
                        })?;
                        for k in lim.keys() {
                            if k.as_str() != "steps" {
                                return Err(unsupported(
                                    &format!("workflows[{i}].limits.{k}"),
                                    None,
                                ));
                            }
                        }
                        if let Some(steps) = lim.get("steps") {
                            let steps = steps.as_i64().ok_or_else(|| {
                                invalid(
                                    &format!("workflows[{i}].limits.steps"),
                                    "must be a positive integer",
                                )
                            })?;
                            if steps <= 0 || steps > 100_000 {
                                return Err(invalid(
                                    &format!("workflows[{i}].limits.steps"),
                                    "must be in 1..=100000",
                                ));
                            }
                            w.insert("limitsSteps".into(), Value::from(steps));
                        }
                    }
                    out_arr.push(Value::Object(w));
                }
                if !out_arr.is_empty() {
                    out.insert("workflows".into(), Value::Array(out_arr));
                }
            }
            "tail_consumers" => {
                // CF semantics: a list of Tail Workers receiving this
                // worker's invocation events. Both the documented
                // `[{ service = "name" }]` form and the legacy string
                // array are accepted; only `service` is meaningful in
                // wrangler (no environment/namespace support here).
                // Duplicates collapse to the first occurrence so the
                // rendered `tails` list is stable, and an empty list is
                // valid CF config meaning "no consumers" (no wire key).
                let arr = value
                    .as_array()
                    .ok_or_else(|| invalid("tail_consumers", "must be an array"))?;
                let mut out_arr: Vec<Value> = Vec::new();
                let mut seen = std::collections::BTreeSet::new();
                for (i, entry) in arr.iter().enumerate() {
                    let field = format!("tail_consumers[{i}]");
                    let service = match entry {
                        Value::String(_) => req_str(entry, &field)?,
                        Value::Object(obj) => {
                            for k in obj.keys() {
                                if k != "service" {
                                    return Err(unsupported(&format!("{field}.{k}"), None));
                                }
                            }
                            req_str(
                                obj.get("service").unwrap_or(&Value::Null),
                                &format!("{field}.service"),
                            )?
                        }
                        _ => {
                            return Err(invalid(
                                &field,
                                "must be a service name or { service }",
                            ))
                        }
                    };
                    if seen.insert(service.to_string()) {
                        out_arr.push(serde_json::json!({ "service": service }));
                    }
                }
                if out_arr.len()
                    > snapshot_protocol::compute_desired::MAX_TAIL_CONSUMERS_PER_WORKER
                {
                    return Err(invalid(
                        "tail_consumers",
                        &format!(
                            "at most {} consumers per worker",
                            snapshot_protocol::compute_desired::MAX_TAIL_CONSUMERS_PER_WORKER
                        ),
                    ));
                }
                if !out_arr.is_empty() {
                    out.insert("tailConsumers".into(), Value::Array(out_arr));
                }
            }
            // Wrangler build-step knobs → one `bundle` doc. Semantics
            // per CF docs: `no_bundle` flips `enabled` off; the rest
            // pass through to the rolldown step at pack time.
            "no_bundle" => {
                bundle.insert(
                    "enabled".into(),
                    Value::Bool(!req_bool(value, "no_bundle")?),
                );
            }
            "find_additional_modules" => {
                bundle.insert(
                    "findAdditionalModules".into(),
                    Value::Bool(req_bool(value, "find_additional_modules")?),
                );
            }
            "minify" => {
                bundle.insert("minify".into(), Value::Bool(req_bool(value, "minify")?));
            }
            "keep_names" => {
                bundle.insert(
                    "keepNames".into(),
                    Value::Bool(req_bool(value, "keep_names")?),
                );
            }
            "jsx_factory" => {
                bundle.insert(
                    "jsxFactory".into(),
                    Value::from(req_str(value, "jsx_factory")?),
                );
            }
            "jsx_fragment" => {
                bundle.insert(
                    "jsxFragment".into(),
                    Value::from(req_str(value, "jsx_fragment")?),
                );
            }
            // CF `define` — `{IDENT: "<js expression>"}` identifier
            // replacement at bundle time. Values are code snippets
            // (`"\"production\""`, `"42"`), never secrets.
            "define" => {
                let defs = value
                    .as_object()
                    .ok_or_else(|| invalid("define", "must be a table of name → value"))?;
                if defs.len() > 64 {
                    return Err(invalid("define", "must declare at most 64 entries"));
                }
                let mut out_defs = Map::new();
                for (k, v) in defs {
                    if k.is_empty()
                        || k.len() > 256
                        || !k.bytes().all(|b| b.is_ascii() && b != b'\0')
                    {
                        return Err(invalid("define", "invalid identifier"));
                    }
                    let s = req_str(v, &format!("define.{k}"))?;
                    if s.len() > 8192 {
                        return Err(invalid("define", "value too long"));
                    }
                    out_defs.insert(k.clone(), Value::from(s));
                }
                bundle.insert("define".into(), Value::Object(out_defs));
            }
            "rules" => {
                out.insert("moduleRules".into(), normalize_module_rules(value)?);
            }
            "base_dir" => {
                let d = req_str(value, "base_dir")?;
                out.insert(
                    "baseDir".into(),
                    Value::from(validate_base_dir(d, "base_dir")?),
                );
            }
            "env" => {
                return Err(invalid(
                    "env",
                    "internal error — resolve environments before normalize",
                ))
            }
            _ => {
                if !IGNORED_TOP_LEVEL.contains(&key.as_str()) {
                    return Err(unsupported(key, None));
                }
            }
        }
    }

    if !bindings.is_empty() {
        out.insert("bindings".into(), Value::Array(bindings));
    }
    if !hostnames.is_empty() {
        let dedup: Vec<Value> = {
            let mut seen = std::collections::BTreeSet::new();
            hostnames
                .into_iter()
                .filter(|h| seen.insert(h.clone()))
                .map(Value::from)
                .collect()
        };
        // MIG-1: wrangler `route`/`routes` keep their provenance — on the
        // control plane they must name ALREADY-claimed hostnames (they
        // never auto-claim, unlike the native `hostnames` field).
        out.insert("routeHostnames".into(), Value::Array(dedup));
    }
    if !custom_domains.is_empty() {
        let dedup: Vec<Value> = {
            let mut seen = std::collections::BTreeSet::new();
            custom_domains
                .into_iter()
                .filter(|h| seen.insert(h.clone()))
                .map(Value::from)
                .collect()
        };
        // CF `custom_domain` routes attach the whole hostname to the
        // worker — the control plane auto-claims them (idempotent
        // vrtHostname claim + async verify), unlike `routeHostnames`
        // which must name already-claimed hostnames.
        out.insert("customDomains".into(), Value::Array(dedup));
    }
    // `bundle` materializes only when the config declares a bundler
    // knob (`no_bundle`, `minify`, …) — the normalized doc then stays
    // field-for-field identical to the TypeScript normalizer. The
    // bundle-by-default decision lives with the meta SOURCE at pack:
    // wrangler → on, vrt.toml → off (`no_bundle` flips it explicitly).
    if !bundle.is_empty() {
        out.insert("bundle".into(), Value::Object(bundle));
    }
    Ok(Value::Object(out))
}

/// Normalize wrangler `[limits]` into the internal `quotas` doc.
///
/// Cloudflare's `Limits` type exposes `cpu_ms` and `subrequests`; both
/// map onto the signed snapshot's `WorkerQuotas`. Unspecified dimensions
/// keep the platform default (never rendered as a `limits` entry), so a
/// worker that only declares `cpu_ms` behaves exactly like today for
/// memory/subrequests. Unknown keys and out-of-bounds values are
/// rejected with the failing path — never silently clamped.
fn normalize_limits(value: &Value) -> Result<Value, ProjectConfigError> {
    use snapshot_protocol::compute_desired::{
        DEFAULT_CPU_MS, DEFAULT_MEMORY_MB, DEFAULT_OUTBOUND_CONNECTIONS, DEFAULT_SUBREQUESTS,
        MAX_CPU_MS, MAX_SUBREQUESTS, MIN_CPU_MS,
    };
    let table = value
        .as_object()
        .ok_or_else(|| invalid("limits", "must be a table"))?;
    let mut cpu_ms = DEFAULT_CPU_MS;
    let mut subrequests = DEFAULT_SUBREQUESTS;
    for (k, v) in table {
        match k.as_str() {
            "cpu_ms" => {
                let n = v
                    .as_u64()
                    .ok_or_else(|| invalid("limits.cpu_ms", "must be an integer"))?;
                if !(u64::from(MIN_CPU_MS)..=u64::from(MAX_CPU_MS)).contains(&n) {
                    return Err(invalid(
                        "limits.cpu_ms",
                        &format!("must be within {MIN_CPU_MS}..={MAX_CPU_MS} milliseconds"),
                    ));
                }
                cpu_ms = n as u32;
            }
            "subrequests" => {
                let n = v
                    .as_u64()
                    .ok_or_else(|| invalid("limits.subrequests", "must be an integer"))?;
                // The platform cap is far below Cloudflare's paid
                // 10M: vrt rejects what it cannot account for instead
                // of accepting a limit it will never reach.
                if n > u64::from(MAX_SUBREQUESTS) {
                    return Err(invalid(
                        "limits.subrequests",
                        &format!("must be <= {MAX_SUBREQUESTS} on the VRT runtime"),
                    ));
                }
                subrequests = n as u32;
            }
            other => return Err(unsupported(&format!("limits.{other}"), None)),
        }
    }
    Ok(serde_json::json!({
        "cpuMs": cpu_ms,
        "memoryMb": DEFAULT_MEMORY_MB,
        "subrequests": subrequests,
        "outboundConnections": DEFAULT_OUTBOUND_CONNECTIONS,
    }))
}

/// CF `[dev]` — wrangler dev-server settings, strictly validated for
/// `of dev` at session start (deploy accepts and drops the table on
/// the shared normalize path, so this runs only on the dev entry).
/// `ip`/`port` pick the loopback ingress listen (CLI `--listen` still
/// wins); `host` registers an extra hostname for the session and must
/// satisfy the same hostname rule a desired-state route does, so a
/// bad value fails here instead of at publish. Keys that declare dev
/// behavior the standalone does not implement — `upstream_protocol`,
/// `inspector_*`, `remote`/`experimental_remote`, `enable_containers`,
/// `container_engine`, `generate_types` — fail closed; `local_protocol`
/// is a no-op only when `http` (the sole mode of dev serves).
pub(crate) fn parse_dev_table(
    value: &Value,
) -> Result<crate::worker_pkg::DevToml, ProjectConfigError> {
    let table = value
        .as_object()
        .ok_or_else(|| invalid("dev", "must be a table"))?;
    let mut out = crate::worker_pkg::DevToml::default();
    for (k, v) in table {
        match k.as_str() {
            "ip" => {
                let s = v
                    .as_str()
                    .ok_or_else(|| invalid("dev.ip", "must be a string"))?;
                // `localhost` — CF's default — resolves to IPv4
                // loopback (DevPorts::alloc enforces loopback for
                // `--listen` too; a public bind is refused either way).
                let ip: std::net::IpAddr = if s == "localhost" {
                    std::net::IpAddr::from([127, 0, 0, 1])
                } else {
                    s.parse()
                        .map_err(|_| invalid("dev.ip", "must be an IP address or 'localhost'"))?
                };
                if !ip.is_loopback() {
                    return Err(invalid("dev.ip", "of dev binds loopback only"));
                }
                out.ip = Some(ip.to_string());
            }
            "port" => {
                let n = v
                    .as_u64()
                    .ok_or_else(|| invalid("dev.port", "must be an integer"))?;
                if !(1..=65535).contains(&n) {
                    return Err(invalid("dev.port", "must be within 1..=65535"));
                }
                out.port = Some(n as u16);
            }
            "host" => {
                let s = v
                    .as_str()
                    .ok_or_else(|| invalid("dev.host", "must be a string"))?;
                if !snapshot_protocol::compute_desired::valid_hostname(s) {
                    return Err(invalid(
                        "dev.host",
                        "must be a lowercase hostname with at least two labels",
                    ));
                }
                out.host = Some(s.to_string());
            }
            "local_protocol" => {
                let s = v
                    .as_str()
                    .ok_or_else(|| invalid("dev.local_protocol", "must be a string"))?;
                if s != "http" {
                    return Err(unsupported(
                        "dev.local_protocol",
                        Some("of dev serves http only"),
                    ));
                }
            }
            other => return Err(unsupported(&format!("dev.{other}"), None)),
        }
    }
    Ok(out)
}

/// Read the env-resolved `[dev]` table of a wrangler project for
/// `of dev` — strict validation, applied only on the dev entry so a
/// deploy never rejects session-only config it would drop anyway.
/// `None` when the project has no `[dev]` (or ships `vrt.toml`, which
/// carries `dev` inside `WorkerToml` instead).
pub(crate) fn load_dev_table_for_env(
    worker_dir: &Path,
    env_name: Option<&str>,
) -> Result<Option<crate::worker_pkg::DevToml>, LocalError> {
    if worker_dir.join("vrt.toml").exists() {
        return Ok(None);
    }
    let (_config, resolved, _shadowed) =
        load_wrangler_project_config(worker_dir, None, env_name)
            .map_err(|e| LocalError::Worker(Box::leak(e.to_string().into_boxed_str())))?;
    resolved
        .get("dev")
        .map(parse_dev_table)
        .transpose()
        .map_err(|e| LocalError::WorkerOwned(e.to_string()))
}

/// T31c: normalize `run_worker_first` — Wrangler semantics:
/// boolean, or a list of `/`-prefixed globs with `!`-prefixed
/// asset-first exclusions. Bounds and rule syntax mirror
/// `valid_run_worker_first_rules` (snapshot-protocol, the desired-side
/// validator) — both sides reject the same malformed input.
fn normalize_run_worker_first(value: &Value) -> Result<Value, ProjectConfigError> {
    match value {
        Value::Bool(_) => Ok(value.clone()),
        Value::Array(rules) => {
            let mut out = Vec::with_capacity(rules.len());
            for (i, rule) in rules.iter().enumerate() {
                let Some(text) = rule.as_str() else {
                    return Err(invalid(
                        &format!("assets.run_worker_first[{i}]"),
                        "must be a pattern string",
                    ));
                };
                out.push(text.to_string());
            }
            if !snapshot_protocol::compute_desired::valid_run_worker_first_rules(&out) {
                return Err(invalid(
                    "assets.run_worker_first",
                    "rules must start '/' or '!/', contain no duplicates, keep at least one non-'!' rule, and stay within the 100-rule / 100-byte limits",
                ));
            }
            Ok(Value::Array(out.into_iter().map(Value::from).collect()))
        }
        _ => Err(invalid(
            "assets.run_worker_first",
            "must be a boolean or an array of pattern strings",
        )),
    }
}

/// File selection + parse + env-resolve + normalize — the wrangler path.
/// Returns (normalized config, resolved raw object, shadowed files).
pub fn load_wrangler_project_config(
    project_dir: &Path,
    config_file: Option<&str>,
    env_name: Option<&str>,
) -> Result<(Value, Value, Vec<String>), ProjectConfigError> {
    let (selected, shadowed) = match config_file {
        Some(name) => {
            if !CANDIDATES.contains(&name) {
                return Err(invalid_code(
                    "VRT_CONFIG_INVALID",
                    "configFile",
                    &format!(
                        "--config must name one of {} (got '{name}')",
                        CANDIDATES.join(", ")
                    ),
                ));
            }
            if !project_dir.join(name).exists() {
                return Err(invalid_code(
                    "VRT_CONFIG_NOT_FOUND",
                    "configFile",
                    &format!("config file not found in project: {name}"),
                ));
            }
            (
                name.to_string(),
                CANDIDATES
                    .iter()
                    .filter(|&&c| c != name && project_dir.join(c).exists())
                    .map(|s| s.to_string())
                    .collect(),
            )
        }
        None => {
            let found: Vec<String> = CANDIDATES
                .iter()
                .filter(|c| project_dir.join(c).exists())
                .map(|s| s.to_string())
                .collect();
            if found.is_empty() {
                return Err(invalid_code(
                    "VRT_CONFIG_NOT_FOUND",
                    "configFile",
                    "no wrangler config selected",
                ));
            }
            (found[0].clone(), found[1..].to_vec())
        }
    };
    let parsed = parse_config_file(&project_dir.join(&selected), &selected)?;
    let resolved = resolve_env(&parsed, env_name, &selected)?;
    let config = normalize_wrangler_config(&resolved, &selected)?;
    Ok((config, resolved, shadowed))
}

// ── WorkerToml conversion (standalone runtime shape) ─────────────────

/// Normalized config → `vrt.toml` wire shape for the packer.
/// Standalone derivations (documented in workers-runtime-project-config.md):
///   * missing KV id / R2 bucket → the binding name (the local store
///     treats declaration as creation);
///   * service `entrypoint`/`props` → `{ service, entrypoint, props }`
///     table (validated);
///   * crons get deterministic `scheduleId`s (`cfg:{name}:{i}`);
///   * non-string vars are JSON-encoded (vrt vars are string→string).
fn wrangler_config_to_worker_toml(config: &Value) -> Result<WorkerToml, ProjectConfigError> {
    let obj = config.as_object().unwrap();
    let mut wire = Map::new();
    if let Some(v) = obj.get("name") {
        wire.insert("name".into(), v.clone());
    }
    if let Some(v) = obj.get("main") {
        wire.insert("mainModule".into(), v.clone());
    }
    if let Some(v) = obj.get("compatibilityDate") {
        wire.insert("compatibilityDate".into(), v.clone());
    }
    if let Some(v) = obj.get("compatibilityFlags") {
        wire.insert("compatibilityFlags".into(), v.clone());
    }
    // Per-worker quotas: the wrangler `[limits]` block was normalized
    // into the internal `quotas` shape (all four dimensions, defaults
    // filled) — project it verbatim into the pack-time document.
    if let Some(v) = obj.get("quotas") {
        wire.insert("quotas".into(), v.clone());
    }
    // `[dev]` — dev-server settings, verbatim (consumed by `of dev`,
    // never by the desired-state path).
    if let Some(v) = obj.get("dev") {
        wire.insert("dev".into(), v.clone());
    }
    // `bundle` — bundler knobs, consumed by `gather_bundled` at pack.
    if let Some(v) = obj.get("bundle") {
        wire.insert("bundle".into(), v.clone());
    }
    // `rules`/`base_dir` — module-typing rules + the rule-eval root,
    // consumed by `build_pack_json` (already shape-proven above).
    if let Some(v) = obj.get("moduleRules") {
        wire.insert("moduleRules".into(), v.clone());
    }
    if let Some(v) = obj.get("baseDir") {
        wire.insert("baseDir".into(), v.clone());
    }
    if let Some(Value::Object(vars)) = obj.get("vars") {
        let mut out_vars = Map::new();
        for (k, v) in vars {
            let s = match v {
                Value::String(s) => s.clone(),
                other => serde_json::to_string(other)
                    .map_err(|_| invalid(&format!("vars.{k}"), "cannot encode"))?,
            };
            out_vars.insert(k.clone(), Value::from(s));
        }
        wire.insert("vars".into(), Value::Object(out_vars));
    }
    if let Some(Value::Object(assets)) = obj.get("assets") {
        let mut a = Map::new();
        a.insert(
            "dir".into(),
            assets.get("directory").cloned().unwrap_or(Value::Null),
        );
        if let Some(v) = assets.get("binding") {
            a.insert("binding".into(), v.clone());
        }
        if let Some(v) = assets.get("htmlHandling") {
            a.insert("htmlHandling".into(), v.clone());
        }
        if let Some(v) = assets.get("notFoundHandling") {
            a.insert("notFoundHandling".into(), v.clone());
        }
        // T31c: wrangler/cloudflare.config.ts sources carry Cloudflare
        // semantics — assets-first is the default, `run_worker_first`
        // overrides it. Native `vrt.toml` leaves `assetsFirst` unset
        // (worker-first), documented in workers-runtime-standalone.md.
        a.insert("assetsFirst".into(), Value::Bool(true));
        if let Some(v) = assets.get("runWorkerFirst") {
            a.insert("runWorkerFirst".into(), v.clone());
        }
        wire.insert("assets".into(), Value::Object(a));
    }
    {
        // MIG-1: standalone has no claim control plane — route-derived
        // hostnames and native hostnames both become wire `hostnames`
        // (deduped, order-preserving).
        let mut merged: Vec<Value> = Vec::new();
        for key in ["hostnames", "routeHostnames", "customDomains"] {
            if let Some(Value::Array(list)) = obj.get(key) {
                for v in list {
                    if !merged.contains(v) {
                        merged.push(v.clone());
                    }
                }
            }
        }
        if !merged.is_empty() {
            wire.insert("hostnames".into(), Value::Array(merged));
        }
    }
    if let Some(Value::Array(crons)) = obj.get("crons") {
        let name = obj.get("name").and_then(|n| n.as_str()).unwrap_or("worker");
        let arr: Vec<Value> = crons
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                serde_json::json!({
                    "scheduleId": format!("cfg:{name}:{i}"),
                    "expression": expr.as_str().unwrap_or_default(),
                })
            })
            .collect();
        wire.insert("crons".into(), Value::Array(arr));
    }
    if let Some(v) = obj.get("queueConsumers") {
        wire.insert("queueConsumers".into(), v.clone());
    }
    if let Some(v) = obj.get("streamingTailConsumers") {
        wire.insert("streamingTailConsumers".into(), v.clone());
    }
    if let Some(v) = obj.get("durableObjectBindings") {
        wire.insert("durableObjectBindings".into(), v.clone());
    }
    if let Some(v) = obj.get("durableObjectMigrations") {
        wire.insert("durableObjectMigrations".into(), v.clone());
    }
    if let Some(v) = obj.get("workflows") {
        wire.insert("workflows".into(), v.clone());
    }
    if let Some(v) = obj.get("tailConsumers") {
        wire.insert("tailConsumers".into(), v.clone());
    }
    if let Some(v) = obj.get("observability") {
        wire.insert("observability".into(), v.clone());
    }

    // W1-r1: STRUCTURAL anti-drop — every normalized top-level key
    // must reach the wire (or be an explicitly documented
    // non-carrying key). Anything else means the projection forgot a
    // section — fail closed instead of silently dropping a declared
    // capability (this is exactly how `workflows` was lost).
    const CARRIED: &[&str] = &[
        "name",
        "main",
        "compatibilityDate",
        "compatibilityFlags",
        "vars",
        "assets",
        "hostnames",
        "routeHostnames",
        "customDomains",
        "crons",
        "queueConsumers",
        "observability",
        "streamingTailConsumers",
        "durableObjectBindings",
        "durableObjectMigrations",
        "workflows",
        "tailConsumers",
        "quotas",
        "dev",
        "moduleRules",
        "baseDir",
        "bundle",
        "bindings",
    ];
    // Normalized-internal keys that legitimately do not carry to the
    // wire document — everything else must project or the deploy is
    // rejected (see comment above).
    const INTERNAL_NON_CARRIED: &[&str] = &["schemaVersion"];
    for k in obj.keys() {
        if !CARRIED.contains(&k.as_str()) && !INTERNAL_NON_CARRIED.contains(&k.as_str()) {
            return Err(unsupported(
                &format!("{k}"),
                Some(
                    "normalized key has no wire projection — update wrangler_config_to_worker_toml",
                ),
            ));
        }
    }
    if let Some(Value::Array(bindings)) = obj.get("bindings") {
        let mut kv = Map::new();
        let mut r2 = Map::new();
        let mut d1 = Map::new();
        let mut queue = Map::new();
        let mut service = Map::new();
        let mut pg = Map::new();
        let mut hyperdrive = Map::new();
        let mut ratelimit = Map::new();
        let mut worker_loaders = Vec::new();
        let mut unsafe_evals = Vec::new();
        let mut images = Vec::new();
        let mut flagship = Map::new();
        let mut k2 = Map::new();
        let mut send_email = Map::new();
        let mut secrets_store = Map::new();
        let mut version_metadata: Option<String> = None;
        let mut vectorize = Map::new();
        for b in bindings {
            let ty = b.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let name = b.get("name").and_then(|n| n.as_str()).unwrap_or("");
            match ty {
                "kv" => {
                    let id = b.get("id").and_then(|i| i.as_str()).unwrap_or(name);
                    kv.insert(name.into(), Value::from(id));
                }
                "r2" => {
                    let bucket = b.get("bucketName").and_then(|i| i.as_str()).unwrap_or(name);
                    r2.insert(name.into(), Value::from(bucket));
                }
                "d1" => {
                    // User-side identifier (database_id or binding name); the real
                    // databaseId is derived server-side at pack time.
                    let id = b.get("databaseId").and_then(|i| i.as_str()).unwrap_or(name);
                    d1.insert(name.into(), Value::from(id));
                }
                "service" => {
                    let target = b
                        .get("service")
                        .and_then(|s| s.as_str())
                        .unwrap_or_default();
                    let entrypoint = match b.get("entrypoint") {
                        None => None,
                        Some(ep) => {
                            let ep = ep.as_str().unwrap_or_default();
                            if !snapshot_protocol::compute_desired::valid_service_entrypoint(ep) {
                                return Err(unsupported(
                                    &format!("bindings.{name}.entrypoint"),
                                    Some(
                                        "entrypoint must be a JS identifier of at most 64 chars, \
                                         not `default` and not starting with `Vrt`/`vrt`",
                                    ),
                                ));
                            }
                            Some(ep)
                        }
                    };
                    let props = b.get("props").cloned();
                    if entrypoint.is_none() && props.is_none() {
                        // Legacy plain-string wire shape when neither a
                        // named entrypoint nor props are declared.
                        service.insert(name.into(), Value::from(target));
                    } else {
                        let mut t = Map::new();
                        t.insert("service".into(), Value::from(target));
                        if let Some(ep) = entrypoint {
                            t.insert("entrypoint".into(), Value::from(ep));
                        }
                        if let Some(props) = props {
                            t.insert("props".into(), props);
                        }
                        service.insert(name.into(), Value::Object(t));
                    }
                }
                "queue" => {
                    // User-side queue name (wrangler `queue`); the real queueId
                    // is derived at pack time from vrt-q:{tenant}:{name}.
                    let qn = b.get("queue").and_then(|i| i.as_str()).unwrap_or(name);
                    match b.get("deliveryDelaySeconds") {
                        // Legacy plain-string wire shape when no queue-level
                        // default is declared — existing packages keep their bytes.
                        None => {
                            queue.insert(name.into(), Value::from(qn));
                        }
                        Some(delay) => {
                            let mut q = Map::new();
                            q.insert("queue".into(), Value::from(qn));
                            q.insert("deliveryDelaySeconds".into(), delay.clone());
                            queue.insert(name.into(), Value::Object(q));
                        }
                    }
                }
                "pg" => {
                    // P1: `id` is the logical tenant id — credentials
                    // live in secrets/pg-credentials.json keyed
                    // `vrtpgcred:<id>` (provider-side, never bundled).
                    let id = b.get("id").and_then(|i| i.as_str()).unwrap_or(name);
                    pg.insert(name.into(), Value::from(id));
                }
                "hyperdrive" => {
                    let database_tenant = b
                        .get("databaseTenant")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    hyperdrive.insert(name.into(), Value::from(database_tenant));
                }
                "ratelimit" => {
                    let mut config = Map::new();
                    for key in ["namespaceId", "limit", "period"] {
                        if let Some(value) = b.get(key) {
                            config.insert(key.into(), value.clone());
                        }
                    }
                    ratelimit.insert(name.into(), Value::Object(config));
                }
                "workerLoader" => {
                    worker_loaders.push(Value::from(name));
                }
                "unsafeEval" => {
                    unsafe_evals.push(Value::from(name));
                }
                "flagship" => {
                    let app_id = b.get("appId").and_then(|i| i.as_str()).unwrap_or(name);
                    flagship.insert(name.into(), Value::from(app_id));
                }
                "k2" => {
                    // User-side stream name; the real streamId is
                    // derived at pack time from vrt-k2:{tenant}:{name}.
                    let sn = b.get("stream").and_then(|i| i.as_str()).unwrap_or(name);
                    k2.insert(name.into(), Value::from(sn));
                }
                "send_email" => {
                    // CF send-bindings restriction set — normalized
                    // record already validated the shape; pass it
                    // through to the pack wire as the camelCase object
                    // BindingsToml::SendEmailToml expects.
                    let mut r = Map::new();
                    if let Some(d) = b.get("destinationAddress") {
                        r.insert("destinationAddress".into(), d.clone());
                    }
                    if let Some(l) = b.get("allowedDestinationAddresses") {
                        r.insert("allowedDestinationAddresses".into(), l.clone());
                    }
                    if let Some(l) = b.get("allowedSenderAddresses") {
                        r.insert("allowedSenderAddresses".into(), l.clone());
                    }
                    send_email.insert(name.into(), Value::Object(r));
                }
                "secrets_store" => {
                    // `[[secrets_store_secrets]]` normalized target —
                    // pass `{storeId, secretName}` through to
                    // BindingsToml::SecretsStoreToml.
                    let mut s = Map::new();
                    if let Some(v) = b.get("storeId") {
                        s.insert("storeId".into(), v.clone());
                    }
                    s.insert(
                        "secretName".into(),
                        b.get("secretName").cloned().unwrap_or(Value::Null),
                    );
                    secrets_store.insert(name.into(), Value::Object(s));
                }
                "versionMetadata" => {
                    // `[version_metadata]` normalized target — a single
                    // binding name → `versionMetadata: "<name>"`.
                    version_metadata = Some(name.to_string());
                }
                "images" => {
                    // `[images] {binding}` — name list only (like
                    // workerLoaders): no resource reference.
                    images.push(Value::from(name));
                }
                "vectorize" => {
                    // `{type:"vectorize", index_name, dimensions,
                    // metric}` → `[bindings].vectorize.<name>`; the
                    // real indexId is derived at pack time
                    // (`vrt-vx:{tenant}:{index_name}`).
                    let mut v = Map::new();
                    v.insert(
                        "index_name".into(),
                        b.get("index_name").cloned().unwrap_or(Value::Null),
                    );
                    v.insert(
                        "dimensions".into(),
                        b.get("dimensions").cloned().unwrap_or(Value::Null),
                    );
                    v.insert(
                        "metric".into(),
                        b.get("metric").cloned().unwrap_or(Value::Null),
                    );
                    vectorize.insert(name.into(), Value::Object(v));
                }
                "secret" => {
                    // wrangler files never declare secrets — defensive
                    return Err(unsupported(
                        &format!("bindings.{name}"),
                        Some("secrets are declared via [secrets] in vrt.toml or `of secret put`"),
                    ));
                }
                // W1-r1: a binding type that survives normalize but has
                // no projection must not vanish — fail closed.
                _ => {
                    return Err(unsupported(
                        &format!("bindings.{name}.type"),
                        Some(&format!("unknown binding type {ty:?}")),
                    ))
                }
            }
        }
        let mut bind = Map::new();
        if !kv.is_empty() {
            bind.insert("kv".into(), Value::Object(kv));
        }
        if !r2.is_empty() {
            bind.insert("r2".into(), Value::Object(r2));
        }
        if !d1.is_empty() {
            bind.insert("d1".into(), Value::Object(d1));
        }
        if !queue.is_empty() {
            bind.insert("queue".into(), Value::Object(queue));
        }
        if !service.is_empty() {
            bind.insert("service".into(), Value::Object(service));
        }
        if !pg.is_empty() {
            bind.insert("pg".into(), Value::Object(pg));
        }
        if !hyperdrive.is_empty() {
            bind.insert("hyperdrive".into(), Value::Object(hyperdrive));
        }
        if !ratelimit.is_empty() {
            bind.insert("ratelimit".into(), Value::Object(ratelimit));
        }
        if !worker_loaders.is_empty() {
            bind.insert("workerLoaders".into(), Value::Array(worker_loaders));
        }
        if !images.is_empty() {
            bind.insert("images".into(), Value::Array(images));
        }
        if !unsafe_evals.is_empty() {
            bind.insert("unsafeEvals".into(), Value::Array(unsafe_evals));
        }
        if !flagship.is_empty() {
            bind.insert("flagship".into(), Value::Object(flagship));
        }
        if !k2.is_empty() {
            bind.insert("k2".into(), Value::Object(k2));
        }
        if !send_email.is_empty() {
            bind.insert("sendEmail".into(), Value::Object(send_email));
        }
        if !secrets_store.is_empty() {
            bind.insert("secretsStore".into(), Value::Object(secrets_store));
        }
        if let Some(vm) = version_metadata {
            bind.insert("versionMetadata".into(), Value::from(vm));
        }
        if !vectorize.is_empty() {
            bind.insert("vectorize".into(), Value::Object(vectorize));
        }
        if !bind.is_empty() {
            wire.insert("bindings".into(), Value::Object(bind));
        }
    }

    // WorkerToml requires name + mainModule + compatibilityDate.
    for req in ["name", "mainModule", "compatibilityDate"] {
        if !wire.contains_key(req) {
            return Err(invalid_code(
                "VRT_CONFIG_INVALID",
                req,
                "required — wrangler config must declare name, main and compatibility_date",
            ));
        }
    }
    serde_json::from_value::<WorkerToml>(Value::Object(wire))
        .map_err(|e| invalid_code("VRT_CONFIG_INVALID", "config", &e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[derive(Deserialize)]
    struct VectorsFile {
        vectors: Vec<Vector>,
    }
    #[derive(Deserialize)]
    struct Vector {
        name: String,
        files: BTreeMap<String, String>,
        #[serde(rename = "envName")]
        env_name: Option<String>,
        #[serde(rename = "configFile")]
        config_file: Option<String>,
        expect: Expectation,
        /// T34: the platform TS normalization layer temporarily rejects d1 (the resource API belongs to T35); standalone
        /// Rust side accepts it -- a vector may carry `rustExpect` to override `expect`.
        #[serde(rename = "rustExpect", default)]
        rust_expect: Option<Expectation>,
    }
    #[derive(Deserialize)]
    struct Expectation {
        ok: bool,
        config: Option<Value>,
        code: Option<String>,
        field: Option<String>,
    }

    fn vectors_path() -> PathBuf {
        // vrt-local → workers-runtime → server → repo root
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../shared/vrt-project-config-vectors.json")
    }

    #[test]
    fn golden_vectors_match_typescript() {
        let raw = fs::read_to_string(vectors_path()).expect("vectors file");
        let vf: VectorsFile = serde_json::from_str(&raw).expect("vectors parse");
        assert!(vf.vectors.len() >= 15, "expected >=15 vectors");
        for v in &vf.vectors {
            let dir = tempfile::tempdir().unwrap();
            for (name, content) in &v.files {
                fs::write(dir.path().join(name), content).unwrap();
            }
            let result = load_wrangler_project_config(
                dir.path(),
                v.config_file.as_deref(),
                v.env_name.as_deref(),
            );
            let expect = v.rust_expect.as_ref().unwrap_or(&v.expect);
            if expect.ok {
                let (config, _resolved, _shadowed) = result
                    .unwrap_or_else(|e| panic!("vector '{}': expected ok, got {e:?}", v.name));
                let expected = expect.config.as_ref().unwrap();
                assert_eq!(
                    &config, expected,
                    "vector '{}' normalized config mismatch",
                    v.name
                );
            } else {
                let err =
                    result.expect_err(&format!("vector '{}': expected error, got ok", v.name));
                assert_eq!(
                    err.code,
                    expect.code.as_deref().unwrap_or_default(),
                    "vector '{}' error code",
                    v.name
                );
                if let Some(field) = &expect.field {
                    assert_eq!(
                        err.field.as_deref().unwrap_or_default(),
                        field.as_str(),
                        "vector '{}' error field",
                        v.name
                    );
                }
            }
        }
    }

    #[test]
    fn jsonc_comments_and_commas() {
        let j = jsonc_to_json("{ // hi\n \"a\": 1, /* x */ \"b\": [1,2,], }").unwrap();
        let v: Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["b"][1], Value::from(2));
    }

    #[test]
    fn tail_consumers_normalize_and_fail_closed() {
        let normalized = |extra: Value| {
            let mut cfg = serde_json::json!({
                "name": "w",
                "main": "m.js",
                "compatibility_date": "2025-01-01"
            });
            cfg.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            normalize_wrangler_config(&cfg, "wrangler.json")
        };

        // Object and legacy string forms, first occurrence wins.
        let out = normalized(serde_json::json!({
            "tail_consumers": [
                {"service": "tail-a"},
                "tail-b",
                {"service": "tail-a"}
            ]
        }))
        .unwrap();
        assert_eq!(
            out["tailConsumers"],
            serde_json::json!([{"service": "tail-a"}, {"service": "tail-b"}])
        );

        // An empty list is valid CF config and leaves no wire key.
        let out = normalized(serde_json::json!({"tail_consumers": []})).unwrap();
        assert!(out.get("tailConsumers").is_none());

        // Unknown entry keys name the exact path.
        let err = normalized(serde_json::json!({
            "tail_consumers": [{"service": "tail-a", "environment": "staging"}]
        }))
        .unwrap_err();
        assert_eq!(err.code, "VRT_CONFIG_UNSUPPORTED_FIELD");
        assert_eq!(err.field.as_deref(), Some("tail_consumers[0].environment"));

        // Wrong shape / empty service are typed invalid-input errors.
        for entry in [serde_json::json!("tail-a"), serde_json::json!([42])] {
            let err = normalized(serde_json::json!({"tail_consumers": entry})).unwrap_err();
            assert_eq!(err.code, "VRT_CONFIG_INVALID");
        }
        let err = normalized(serde_json::json!({
            "tail_consumers": [{"service": "  "}]
        }))
        .unwrap_err();
        assert_eq!(err.code, "VRT_CONFIG_INVALID");
        assert_eq!(err.field.as_deref(), Some("tail_consumers[0].service"));
    }

    #[test]
    fn vrt_toml_keeps_precedence() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("vrt.toml"),
            "name=\"a\"\nmainModule=\"m.js\"\ncompatibilityDate=\"2025-01-01\"\n",
        )
        .unwrap();
        fs::write(dir.path().join("wrangler.toml"), "name=\"b\"").unwrap();
        let (_meta, src) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(src, MetaSource::VrtToml);
    }

    #[test]
    fn programmatic_config_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("cloudflare.config.ts"), "export default {}").unwrap();
        let err = load_worker_meta(dir.path()).expect_err("must fail");
        assert!(format!("{err:?}").contains("programmatic config"));
    }

    #[test]
    fn wrangler_named_environment_requires_and_uses_selection() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.json"),
            r#"{"name":"w","main":"m.js","compatibility_date":"2025-01-01",
               "vars":{"MODE":"base"},
               "env":{"staging":{"vars":{"MODE":"staging","ONLY_STAGING":"yes"}}}}"#,
        )
        .unwrap();

        let err = load_worker_meta(dir.path()).expect_err("named env must be selected");
        assert!(format!("{err:?}").contains("named environments found"));

        let (meta, source) = load_worker_meta_for_env(dir.path(), Some("staging")).unwrap();
        assert_eq!(source, MetaSource::Wrangler("wrangler.json".into()));
        assert_eq!(meta.vars.get("MODE").map(String::as_str), Some("staging"));
        assert_eq!(
            meta.vars.get("ONLY_STAGING").map(String::as_str),
            Some("yes")
        );
    }

    #[test]
    fn wrangler_main_leading_dot_slash_normalizes() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.json"),
            r#"{"name":"w","main":"./sub/m.js","compatibility_date":"2025-01-01"}"#,
        )
        .unwrap();
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(meta.main_module, "sub/m.js");
    }

    #[test]
    fn vrt_toml_rejects_wrangler_environment_selection() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("vrt.toml"),
            "name=\"w\"\nmainModule=\"m.js\"\ncompatibilityDate=\"2025-01-01\"\n",
        )
        .unwrap();
        let err = load_worker_meta_for_env(dir.path(), Some("staging"))
            .expect_err("vrt.toml cannot silently ignore --env");
        assert!(format!("{err:?}").contains("vrt.toml has no env blocks"));
    }

    #[test]
    fn wrangler_path_into_pack_shape() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.json"),
            r#"{"name":"w","main":"m.js","compatibility_date":"2025-01-01",
               "vars":{"A":"x","N":42},"kv_namespaces":[{"binding":"K"}],
               "triggers":{"crons":["*/5 * * * *"]},"routes":["a.example.com/*"]}"#,
        )
        .unwrap();
        let (meta, src) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(src, MetaSource::Wrangler("wrangler.json".into()));
        assert_eq!(meta.name, "w");
        assert_eq!(meta.main_module, "m.js");
        assert_eq!(meta.vars.get("N").map(|s| s.as_str()), Some("42"));
        assert_eq!(meta.hostnames, vec!["a.example.com"]);
        assert_eq!(meta.crons[0].expression, "*/5 * * * *");
        assert_eq!(meta.crons[0].schedule_id, "cfg:w:0");
        // missing kv id → binding name derivation
        let bind = meta.bindings.unwrap();
        assert_eq!(bind.kv.get("K").map(|s| s.as_str()), Some("K"));
    }

    fn wrangler_meta(toml: &str) -> Result<WorkerToml, LocalError> {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("wrangler.toml"), toml).unwrap();
        load_worker_meta(dir.path()).map(|(m, _)| m)
    }

    #[test]
    fn custom_domain_routes_into_hostnames() {
        // `{pattern, custom_domain = true}` — CF CustomDomainRoute: the
        // pattern IS the hostname (uppercase folds to lowercase; zone
        // refs are accepted and ignored, matching CF's anyOf shapes).
        let meta = wrangler_meta(
            "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n\
             routes=[{pattern=\"API.Example.com\",custom_domain=true},\n\
             {pattern=\"api.example.com/*\",zone_name=\"example.com\"}]\n",
        )
        .unwrap();
        // Both land in standalone `hostnames` (wire merge dedupes).
        assert_eq!(meta.hostnames, vec!["api.example.com"]);

        // Multiple custom domains dedup to first-occurrence order (same
        // as `routeHostnames`).
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.toml"),
            "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n\
             routes=[{pattern=\"b.example.com\",custom_domain=true},\n\
             {pattern=\"a.example.com\",custom_domain=true},\n\
             {pattern=\"b.example.com\",custom_domain=true}]\n",
        )
        .unwrap();
        let (cfg, _, _) = load_wrangler_project_config(dir.path(), None, None).unwrap();
        assert_eq!(
            cfg["customDomains"],
            serde_json::json!(["b.example.com", "a.example.com"])
        );
        // Singular `route` form carries the same entry shape.
        let meta = wrangler_meta(
            "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n\
             route={pattern=\"one.example.com\",custom_domain=true}\n",
        )
        .unwrap();
        assert_eq!(meta.hostnames, vec!["one.example.com"]);
    }

    #[test]
    fn custom_domain_disabled_and_false_are_not_claimed() {
        // `enabled = false` → not served, not claimed; `custom_domain
        // = false` on a zone route stays a plain pattern route.
        let meta = wrangler_meta(
            "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n\
             routes=[{pattern=\"off.example.com\",custom_domain=true,enabled=false},\n\
             {pattern=\"on.example.com/*\",zone_id=\"z1\",custom_domain=false}]\n",
        )
        .unwrap();
        assert_eq!(meta.hostnames, vec!["on.example.com"]);
    }

    #[test]
    fn custom_domain_fail_closed() {
        let bad = [
            // wildcard custom domains are out of the claim model's scope
            (
                "routes=[{pattern=\"*.example.com\",custom_domain=true}]",
                "VRT_CONFIG_UNSUPPORTED_FIELD",
            ),
            // a path/port is not a bare hostname
            (
                "routes=[{pattern=\"a.example.com/v1\",custom_domain=true}]",
                "VRT_CONFIG_INVALID",
            ),
            (
                "routes=[{pattern=\"a.example.com:443\",custom_domain=true}]",
                "VRT_CONFIG_INVALID",
            ),
            // single label is not a routable FQDN claim
            (
                "routes=[{pattern=\"localhost\",custom_domain=true}]",
                "VRT_CONFIG_INVALID",
            ),
            // non-boolean custom_domain
            (
                "routes=[{pattern=\"a.example.com\",custom_domain=\"yes\"}]",
                "VRT_CONFIG_INVALID",
            ),
            // `enabled` only exists on the custom-domain shape
            (
                "routes=[{pattern=\"a.example.com/*\",enabled=false}]",
                "VRT_CONFIG_UNSUPPORTED_FIELD",
            ),
            // unknown keys fail closed (CF additionalProperties:false)
            (
                "routes=[{pattern=\"a.example.com\",custom_domain=true,ttl=60}]",
                "VRT_CONFIG_UNSUPPORTED_FIELD",
            ),
        ];
        for (routes, code) in bad {
            let err = wrangler_meta(&format!(
                "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n{routes}\n"
            ))
            .expect_err(&format!("{routes} must fail"));
            assert!(
                format!("{err:?}").contains(code),
                "{routes} → {err:?}, want {code}"
            );
        }
    }

    #[test]
    fn module_rules_and_base_dir_normalize() {
        // `{type, globs, fallthrough?}` — CF Rule shape; `base_dir`
        // carries verbatim as the pack-time rule-eval root.
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.toml"),
            "name=\"w\"\nmain=\"src/index.mjs\"\ncompatibility_date=\"2025-01-01\"\n\
             base_dir=\"src\"\n\
             rules=[{type=\"Text\",globs=[\"**/*.md\"],fallthrough=true},\n\
             {type=\"Data\",globs=[\"**/*.bin\"]}]\n",
        )
        .unwrap();
        let (cfg, _, _) = load_wrangler_project_config(dir.path(), None, None).unwrap();
        assert_eq!(cfg["baseDir"], serde_json::json!("src"));
        assert_eq!(
            cfg["moduleRules"],
            serde_json::json!([
                {"type": "Text", "globs": ["**/*.md"], "fallthrough": true},
                {"type": "Data", "globs": ["**/*.bin"]},
            ])
        );
        // The pack-time meta carries them through the wire shape.
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(meta.base_dir.as_deref(), Some("src"));
        assert_eq!(meta.module_rules.len(), 2);
        assert_eq!(meta.module_rules[0].ty, "Text");
        assert_eq!(meta.module_rules[0].fallthrough, Some(true));
    }

    #[test]
    fn module_rules_fail_closed() {
        let bad = [
            // unknown type (CF enum)
            (
                "rules=[{type=\"Text2\",globs=[\"a\"]}]",
                "VRT_CONFIG_INVALID",
            ),
            // Python types are schema-valid but runtime-less here
            (
                "rules=[{type=\"PythonModule\",globs=[\"**/*.py\"]}]",
                "VRT_CONFIG_UNSUPPORTED_FIELD",
            ),
            // missing globs
            ("rules=[{type=\"Text\"}]", "VRT_CONFIG_INVALID"),
            // non-array globs / non-string entries
            (
                "rules=[{type=\"Text\",globs=\"*.txt\"}]",
                "VRT_CONFIG_INVALID",
            ),
            ("rules=[{type=\"Text\",globs=[42]}]", "VRT_CONFIG_INVALID"),
            // unknown key — CF additionalProperties:false
            (
                "rules=[{type=\"Text\",globs=[\"a\"],x=1}]",
                "VRT_CONFIG_UNSUPPORTED_FIELD",
            ),
            // non-boolean fallthrough
            (
                "rules=[{type=\"Text\",globs=[\"a\"],fallthrough=\"yes\"}]",
                "VRT_CONFIG_INVALID",
            ),
            // not an array
            ("rules={type=\"Text\"}", "VRT_CONFIG_INVALID"),
        ];
        for (rules, code) in bad {
            let err = wrangler_meta(&format!(
                "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n{rules}\n"
            ))
            .expect_err(&format!("{rules} must fail"));
            assert!(
                format!("{err:?}").contains(code),
                "{rules} → {err:?}, want {code}"
            );
        }
    }

    #[test]
    fn base_dir_fail_closed() {
        for (dir_value, code) in [
            // escapes the project
            ("base_dir=\"../x\"", "VRT_CONFIG_INVALID"),
            ("base_dir=\"/abs\"", "VRT_CONFIG_INVALID"),
            ("base_dir=\"a\\\\b\"", "VRT_CONFIG_INVALID"),
            ("base_dir=\"a/../b\"", "VRT_CONFIG_INVALID"),
            ("base_dir=\"./x\"", "VRT_CONFIG_INVALID"),
            ("base_dir=\"x/\"", "VRT_CONFIG_INVALID"),
            ("base_dir=42", "VRT_CONFIG_INVALID"),
        ] {
            let err = wrangler_meta(&format!(
                "name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n{dir_value}\n"
            ))
            .expect_err(&format!("{dir_value} must fail"));
            assert!(
                format!("{err:?}").contains(code),
                "{dir_value} → {err:?}, want {code}"
            );
        }
    }

    #[test]
    fn hyperdrive_and_ratelimits_project_into_standalone_shape() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.json"),
            r#"{"name":"w","main":"m.js","compatibility_date":"2025-01-01",
               "hyperdrive":[{"binding":"DB","database_tenant":"tenant-prod"}],
               "ratelimits":[{"name":"RL","namespace_id":"1001","simple":{"limit":1500,"period":60}}]}"#,
        )
        .unwrap();
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let bindings = meta.bindings.unwrap();
        assert_eq!(
            bindings.hyperdrive.get("DB").map(String::as_str),
            Some("tenant-prod")
        );
        let limiter = bindings.ratelimit.get("RL").expect("rate limit binding");
        assert_eq!(limiter.namespace_id, "1001");
        assert_eq!(limiter.limit, 1500);
        assert_eq!(limiter.period, 60);
    }

    fn wrangler_services(services: &str) -> Result<WorkerToml, LocalError> {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.json"),
            format!(
                r#"{{"name":"w","main":"m.js","compatibility_date":"2025-01-01",
                   "services":{services}}}"#
            ),
        )
        .unwrap();
        load_worker_meta(dir.path()).map(|(meta, _)| meta)
    }

    #[test]
    fn service_entrypoint_into_pack_shape() {
        use crate::worker_pkg::ServiceTargetToml;
        let meta = wrangler_services(
            r#"[{"binding":"A","service":"t"},
                {"binding":"B","service":"t","entrypoint":"Greeter"},
                {"binding":"C","service":"t","props":{"a":1,"nested":{"b":true}}},
                {"binding":"D","service":"t","entrypoint":"Api","props":{"region":"eu"}}]"#,
        )
        .unwrap();
        let svc = meta.bindings.unwrap().service;
        // Default entrypoint keeps the original plain-string wire shape.
        assert_eq!(svc.get("A"), Some(&ServiceTargetToml::Worker("t".into())));
        let b = svc.get("B").unwrap();
        assert_eq!(b.service(), "t");
        assert_eq!(b.entrypoint(), Some("Greeter"));
        assert_eq!(b.props(), None);
        let c = svc.get("C").unwrap();
        assert_eq!(c.service(), "t");
        assert_eq!(c.entrypoint(), None);
        assert_eq!(c.props(), Some(&serde_json::json!({"a":1,"nested":{"b":true}})));
        let d = svc.get("D").unwrap();
        assert_eq!(d.entrypoint(), Some("Api"));
        assert_eq!(d.props(), Some(&serde_json::json!({"region":"eu"})));
    }

    #[test]
    fn service_props_fail_closed() {
        // Non-object props are rejected at normalization with an exact field.
        let err = wrangler_services(r#"[{"binding":"B","service":"t","props":[1]}]"#)
            .expect_err("array props");
        assert!(
            format!("{err:?}").contains("VRT_CONFIG_INVALID"),
            "{err:?}"
        );
        assert!(format!("{err:?}").contains("services[0].props"), "{err:?}");

        // The serialized bound is enforced.
        let big = "x".repeat(snapshot_protocol::compute_desired::MAX_SERVICE_PROPS_BYTES);
        let err = wrangler_services(&format!(
            r#"[{{"binding":"B","service":"t","props":{{"padding":"{big}"}}}}]"#
        ))
        .expect_err("oversized props");
        assert!(
            format!("{err:?}").contains("VRT_CONFIG_INVALID"),
            "{err:?}"
        );
        assert!(format!("{err:?}").contains("services[0].props"), "{err:?}");
    }

    #[test]
    fn service_entrypoint_rules_fail_closed() {
        for ep in ["default", "VrtWaituntilProbe", "vrtX", "a-b", ""] {
            let err = wrangler_services(&format!(
                r#"[{{"binding":"B","service":"t","entrypoint":"{ep}"}}]"#
            ))
            .expect_err(ep);
            assert!(format!("{err:?}").contains("entrypoint"), "{ep}: {err:?}");
        }
    }

    #[test]
    fn vrt_toml_service_table_shape() {
        use crate::worker_pkg::{BindingsToml, ServiceTargetToml};
        let b: BindingsToml = toml::from_str(
            "[service]\nA = \"t\"\nB = { service = \"t\", entrypoint = \"Greeter\" }\nC = { service = \"t\", props = { a = 1, nested = { b = true } } }\nD = { service = \"t\", entrypoint = \"Api\", props = { region = \"eu\" } }\n",
        )
        .unwrap();
        assert_eq!(
            b.service.get("A"),
            Some(&ServiceTargetToml::Worker("t".into()))
        );
        assert_eq!(b.service.get("B").unwrap().entrypoint(), Some("Greeter"));
        let c = b.service.get("C").unwrap();
        assert_eq!(c.entrypoint(), None);
        assert_eq!(c.props(), Some(&serde_json::json!({"a":1,"nested":{"b":true}})));
        let d = b.service.get("D").unwrap();
        assert_eq!(d.entrypoint(), Some("Api"));
        assert_eq!(d.props(), Some(&serde_json::json!({"region":"eu"})));
        // Unknown keys inside the table fail closed.
        assert!(toml::from_str::<BindingsToml>(
            "[service]\nB = { service = \"t\", entrypoint = \"G\", ns = \"x\" }\n"
        )
        .is_err());
        assert!(toml::from_str::<BindingsToml>(
            "[service]\nB = { service = \"t\", props = { a = 1 }, ns = \"x\" }\n"
        )
        .is_err());
        // Plain string serializes exactly as before.
        let json = serde_json::to_string(&b.service.get("A").unwrap()).unwrap();
        assert_eq!(json, "\"t\"");
        // The entrypoint-only table keeps its pre-props serialization.
        let json = serde_json::to_string(&b.service.get("B").unwrap()).unwrap();
        assert_eq!(json, r#"{"service":"t","entrypoint":"Greeter"}"#);
    }

    #[test]
    fn native_service_props_reach_pack_and_fail_closed() {
        use crate::worker_pkg::{pack_with_resolver, DeployOverrides};
        use snapshot_protocol::compute_desired::BindingDesired;

        let pack = |props: &str| {
            let dir = tempfile::tempdir().unwrap();
            fs::write(
                dir.path().join("vrt.toml"),
                format!(
                    "name = \"w\"\nmainModule = \"m.js\"\ncompatibilityDate = \"2025-01-01\"\n\n[bindings.service]\nS = {{ service = \"t\", {props} }}\n"
                ),
            )
            .unwrap();
            fs::write(dir.path().join("m.js"), "export default {}").unwrap();
            let overrides = DeployOverrides {
                name: None,
                env_name: None,
                bindings: vec![],
                hostnames: vec![],
                vars: BTreeMap::new(),
                tag: None,
            };
            pack_with_resolver(dir.path(), &overrides, "tenant", &|_| Some("w-target".into()))
        };

        let pkg = pack("props = { a = 1, nested = { b = true } }").unwrap();
        let service = pkg
            .bindings
            .iter()
            .find_map(|b| match b {
                BindingDesired::Service(s) => Some(s),
                _ => None,
            })
            .expect("service binding packed");
        assert_eq!(service.entrypoint, None);
        assert_eq!(
            service.props,
            Some(serde_json::json!({"a":1,"nested":{"b":true}}))
        );

        for bad in ["props = 1", "props = [1, 2]"] {
            let err = pack(bad).err().unwrap_or_else(|| panic!("{bad}"));
            assert!(format!("{err:?}").contains("service props"), "{bad}: {err:?}");
        }
        let big = "x".repeat(snapshot_protocol::compute_desired::MAX_SERVICE_PROPS_BYTES);
        let err = pack(&format!("props = {{ padding = \"{big}\" }}"))
            .err()
            .expect("oversized");
        assert!(format!("{err:?}").contains("service props"), "{err:?}");
    }

    fn wrangler_loader_body(extra: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.toml"),
            format!("name=\"w\"\nmain=\"m.js\"\ncompatibility_date=\"2025-01-01\"\n{extra}"),
        )
        .unwrap();
        dir
    }

    #[test]
    fn worker_loaders_into_pack_shape() {
        let dir = wrangler_loader_body("[[worker_loaders]]\nbinding=\"LOADER\"\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(
            meta.bindings.unwrap().worker_loaders,
            vec!["LOADER".to_string()]
        );
    }

    #[test]
    fn worker_loaders_fail_closed() {
        // Unknown field under the array table.
        let dir = wrangler_loader_body("[[worker_loaders]]\nbinding=\"L\"\nid=\"x\"\n");
        let err = load_worker_meta(dir.path()).expect_err("unknown key must fail");
        assert!(
            format!("{err:?}").contains("worker_loaders[0].id"),
            "{err:?}"
        );
        // Missing binding.
        let dir = wrangler_loader_body("[[worker_loaders]]\n");
        assert!(load_worker_meta(dir.path()).is_err());
        // Non-string binding.
        let dir = wrangler_loader_body("[[worker_loaders]]\nbinding=1\n");
        assert!(load_worker_meta(dir.path()).is_err());
        // Not an array of tables.
        let dir = wrangler_loader_body("worker_loaders=\"L\"\n");
        assert!(load_worker_meta(dir.path()).is_err());
    }

    #[test]
    fn unsafe_eval_bindings_into_pack_shape() {
        let dir = wrangler_loader_body(
            "[unsafe]\n[[unsafe.bindings]]\nname=\"UNSAFE_EVAL\"\ntype=\"unsafe_eval\"\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(
            meta.bindings.unwrap().unsafe_evals,
            vec!["UNSAFE_EVAL".to_string()]
        );
    }

    #[test]
    fn unsafe_eval_bindings_fail_closed() {
        // A raw binding type other than unsafe_eval is refused — every
        // other kind either has a first-class key or no runtime support.
        let dir =
            wrangler_loader_body("[unsafe]\n[[unsafe.bindings]]\nname=\"Q\"\ntype=\"queue\"\n");
        let err = load_worker_meta(dir.path()).expect_err("queue type must fail");
        assert!(
            format!("{err:?}").contains("unsafe.bindings[0].type"),
            "{err:?}"
        );
        // Other `[unsafe]` keys (capnp/metadata raw injection) refused.
        let dir = wrangler_loader_body("[unsafe]\ncapnp=\"x\"\n");
        let err = load_worker_meta(dir.path()).expect_err("unsafe.capnp must fail");
        assert!(format!("{err:?}").contains("unsafe.capnp"), "{err:?}");
        // Missing name.
        let dir = wrangler_loader_body("[unsafe]\n[[unsafe.bindings]]\ntype=\"unsafe_eval\"\n");
        assert!(load_worker_meta(dir.path()).is_err());
        // Missing type.
        let dir = wrangler_loader_body("[unsafe]\n[[unsafe.bindings]]\nname=\"E\"\n");
        assert!(load_worker_meta(dir.path()).is_err());
        // Non-string name.
        let dir =
            wrangler_loader_body("[unsafe]\n[[unsafe.bindings]]\nname=1\ntype=\"unsafe_eval\"\n");
        assert!(load_worker_meta(dir.path()).is_err());
        // `unsafe` not a table.
        let dir = wrangler_loader_body("unsafe=\"x\"\n");
        assert!(load_worker_meta(dir.path()).is_err());
        // `unsafe.bindings` not an array of tables.
        let dir = wrangler_loader_body("[unsafe]\nbindings=\"x\"\n");
        assert!(load_worker_meta(dir.path()).is_err());
    }

    #[test]
    fn streaming_tail_consumers_into_pack_shape() {
        // CF `streaming_tail_consumers = [{service}]` — same shape as
        // `tail_consumers`, projected to its own pack key.
        let dir = wrangler_loader_body(
            "streaming_tail_consumers = [{service=\"t1\"},{service=\"t2\"}]\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let services: Vec<&str> = meta
            .streaming_tail_consumers
            .iter()
            .map(|c| c.service.as_str())
            .collect();
        assert_eq!(services, ["t1", "t2"]);

        // Only `service` is accepted — unknown keys fail closed.
        for toml in [
            "streaming_tail_consumers = [{service=\"t1\",environment=\"prod\"}]\n",
            "streaming_tail_consumers = [{service=\"t1\",remote=true}]\n",
            "streaming_tail_consumers = [{namespace=\"ns\"}]\n",
            "streaming_tail_consumers = \"svc\"\n",
            "streaming_tail_consumers = [1]\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
    }

    #[test]
    fn observability_into_pack_shape() {
        let dir = wrangler_loader_body(
            "[observability]\nenabled=true\nhead_sampling_rate=0.3\n[observability.logs]\ninvocation_logs=false\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let obs = meta.observability.expect("observability parsed");
        assert!(obs.enabled);
        assert!(obs.logs_enabled);
        assert!(!obs.invocation_logs);
        assert_eq!(obs.head_sampling_rate_ppm, 300_000);
        // `logs.head_sampling_rate` is the modern slot; folded the same.
        let dir =
            wrangler_loader_body("[observability.logs]\nhead_sampling_rate=0.5\nenabled=false\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let obs = meta.observability.unwrap();
        assert_eq!(obs.head_sampling_rate_ppm, 500_000);
        assert!(!obs.logs_enabled);
    }

    #[test]
    fn observability_defaults_and_fail_closed() {
        // An empty table folds to CF defaults: all gates on, rate 1.
        let dir = wrangler_loader_body("[observability]\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let obs = meta.observability.unwrap();
        assert_eq!(obs.head_sampling_rate_ppm, 1_000_000);
        assert!(obs.enabled && obs.logs_enabled && obs.invocation_logs);
        // Deprecated alias AND `logs.head_sampling_rate` together:
        // CF treats `logs.*` as authoritative, so the nested value wins
        // instead of rejecting an otherwise valid config.
        let dir = wrangler_loader_body(
            "[observability]\nhead_sampling_rate=0.5\n[observability.logs]\nhead_sampling_rate=0.25\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(meta.observability.unwrap().head_sampling_rate_ppm, 250_000);
        // A named env that only tunes sampling must not reset inherited
        // gates to their defaults (top-level `enabled = false` is a
        // privacy gate; losing it here is fail-open).
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("wrangler.json"),
            r#"{"name":"w","main":"m.js","compatibility_date":"2025-01-01",
               "observability":{"enabled":false,"logs":{"invocation_logs":false}},
               "env":{"production":{"observability":{"head_sampling_rate":0.25}}}}"#,
        )
        .unwrap();
        let (meta, _) = load_worker_meta_for_env(dir.path(), Some("production")).unwrap();
        let obs = meta.observability.expect("env observability parsed");
        assert!(!obs.enabled, "inherited enabled=false must survive");
        assert!(!obs.invocation_logs, "inherited logs gate must survive");
        assert_eq!(obs.head_sampling_rate_ppm, 250_000);
        // Rate bounds.
        for toml in [
            "[observability.logs]\nhead_sampling_rate=1.5\n",
            "[observability]\nhead_sampling_rate=-0.1\n",
            "[observability]\nhead_sampling_rate=\"half\"\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
        // No counterpart in this pipeline — every one fails closed.
        for toml in [
            "[observability.traces]\nenabled=true\n",
            "[observability.logs]\npersist=true\n",
            "[observability.logs]\ndestinations=[\"x\"]\n",
            "[observability]\ninvocation_logs=false\n",
            "[observability]\nenabled=\"yes\"\n",
            "observability=\"x\"\n",
            "[observability]\nlogs=\"x\"\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
    }

    #[test]
    fn flagship_bindings_into_pack_shape() {
        // CF `[[flagship]] {binding, app_id}` → `{type:"flagship"}`
        // binding records + the pack-shape `flagship` map (name →
        // appId). Multiple apps bind to one worker (CF: one worker may
        // consume several Flagship apps).
        let dir = wrangler_loader_body(
            "[[flagship]]\nbinding=\"FLAGS_A\"\napp_id=\"app-a\"\n[[flagship]]\nbinding=\"FLAGS_B\"\napp_id=\"app-b\"\nremote=true\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let b = meta.bindings.unwrap();
        assert_eq!(b.flagship.get("FLAGS_A").map(String::as_str), Some("app-a"));
        assert_eq!(b.flagship.get("FLAGS_B").map(String::as_str), Some("app-b"));

        // Fail closed: missing binding/app_id, non-string values,
        // oversized appId, non-boolean `remote`, unknown keys.
        for toml in [
            "[[flagship]]\napp_id=\"app-a\"\n",
            "[[flagship]]\nbinding=\"F\"\n",
            "[[flagship]]\nbinding=1\napp_id=\"a\"\n",
            "[[flagship]]\nbinding=\"F\"\napp_id=7\n",
            "[[flagship]]\nbinding=\"F\"\napp_id=\"a\"\nremote=\"yes\"\n",
            "[[flagship]]\nbinding=\"F\"\napp_id=\"a\"\nextra=1\n",
            "flagship={binding=\"F\",app_id=\"a\"}\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
        // appId cap.
        let dir = wrangler_loader_body(&format!(
            "[[flagship]]\nbinding=\"F\"\napp_id=\"{}\"\n",
            "a".repeat(129)
        ));
        assert!(load_worker_meta(dir.path()).is_err());
    }

    #[test]
    fn k2_bindings_into_pack_shape() {
        // CF `[[k2]] {binding, stream}` → `{type:"k2"}` binding records
        // + the pack-shape `k2` map (name → stream). `remote` accepted
        // and ignored (local-dev knob, no local/remote split here).
        let dir = wrangler_loader_body(
            "[[k2]]\nbinding=\"EVENTS\"\nstream=\"audit\"\n[[k2]]\nbinding=\"METRICS\"\nstream=\"metrics\"\nremote=true\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let b = meta.bindings.unwrap();
        assert_eq!(b.k2.get("EVENTS").map(String::as_str), Some("audit"));
        assert_eq!(b.k2.get("METRICS").map(String::as_str), Some("metrics"));

        // Fail closed: missing binding/stream, non-string values,
        // oversized stream, non-boolean `remote`, unknown keys.
        for toml in [
            "[[k2]]\nstream=\"s\"\n",
            "[[k2]]\nbinding=\"B\"\n",
            "[[k2]]\nbinding=1\nstream=\"s\"\n",
            "[[k2]]\nbinding=\"B\"\nstream=7\n",
            "[[k2]]\nbinding=\"B\"\nstream=\"s\"\nremote=\"yes\"\n",
            "[[k2]]\nbinding=\"B\"\nstream=\"s\"\nextra=1\n",
            "k2={binding=\"B\",stream=\"s\"}\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
        // stream name cap.
        let dir = wrangler_loader_body(&format!(
            "[[k2]]\nbinding=\"B\"\nstream=\"{}\"\n",
            "s".repeat(257)
        ));
        assert!(load_worker_meta(dir.path()).is_err());
    }

    #[test]
    fn secrets_store_bindings_into_pack_shape() {
        // CF `[[secrets_store_secrets]] {binding, store_id?,
        // secret_name}` → `{type:"secrets_store"}` records + the
        // pack-shape `secretsStore` map (name → {storeId, secretName}).
        // `store_id` defaults to `"default"` per CF.
        let dir = wrangler_loader_body(
            "[[secrets_store_secrets]]\nbinding=\"SECRET\"\nsecret_name=\"api-key\"\n[[secrets_store_secrets]]\nbinding=\"PROD\"\nstore_id=\"prod-store\"\nsecret_name=\"db-pw\"\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let b = meta.bindings.unwrap();
        let s = b.secrets_store.get("SECRET").unwrap();
        assert_eq!(s.secret_name, "api-key");
        assert_eq!(s.store_id.as_deref(), Some("default"));
        let p = b.secrets_store.get("PROD").unwrap();
        assert_eq!(p.secret_name, "db-pw");
        assert_eq!(p.store_id.as_deref(), Some("prod-store"));

        // Fail closed: missing binding/secret_name, non-string values,
        // `remote` (does not exist for this binding), unknown keys,
        // non-table-list shape.
        for toml in [
            "[[secrets_store_secrets]]\nsecret_name=\"s\"\n",
            "[[secrets_store_secrets]]\nbinding=\"B\"\n",
            "[[secrets_store_secrets]]\nbinding=1\nsecret_name=\"s\"\n",
            "[[secrets_store_secrets]]\nbinding=\"B\"\nsecret_name=7\n",
            "[[secrets_store_secrets]]\nbinding=\"B\"\nsecret_name=\"s\"\nstore_id=3\n",
            "[[secrets_store_secrets]]\nbinding=\"B\"\nsecret_name=\"s\"\nremote=true\n",
            "[[secrets_store_secrets]]\nbinding=\"B\"\nsecret_name=\"s\"\nextra=1\n",
            "secrets_store_secrets={binding=\"B\",secret_name=\"s\"}\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
    }

    #[test]
    fn version_metadata_binding_into_pack_shape() {
        // CF `[version_metadata] {binding}` → `{type:"versionMetadata"}`
        // record + the pack-shape `versionMetadata` scalar. The values
        // are version-scoped (deploy --tag + record creation), never
        // config keys — `binding` is the only accepted field.
        let dir = wrangler_loader_body("[version_metadata]\nbinding=\"CF_VERSION_METADATA\"\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let b = meta.bindings.unwrap();
        assert_eq!(b.version_metadata.as_deref(), Some("CF_VERSION_METADATA"));

        // Fail closed: missing binding, non-string binding, unknown
        // keys (`tag`/`timestamp` are not config), non-table shape.
        for toml in [
            "[version_metadata]\n",
            "[version_metadata]\nbinding=1\n",
            "[version_metadata]\nbinding=\"V\"\ntag=\"v1\"\n",
            "[version_metadata]\nbinding=\"V\"\nextra=1\n",
            "version_metadata=\"V\"\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
        // The equivalent inline-table form is accepted (same TOML
        // table shape as `[version_metadata]`).
        let dir = wrangler_loader_body("version_metadata={binding=\"V_META\"}\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(
            meta.bindings.unwrap().version_metadata.as_deref(),
            Some("V_META")
        );
    }

    #[test]
    fn send_email_bindings_into_pack_shape() {
        // CF `[[send_email]] {name, destination_address?,
        // allowed_*}` → `{type:"send_email"}` records + the pack-shape
        // `sendEmail` map carrying the restriction set.
        let dir = wrangler_loader_body(
            "[[send_email]]\nname=\"EMAIL\"\nallowed_sender_addresses=[\"a@x.com\",\"b@x.com\"]\n[[send_email]]\nname=\"PIN\"\ndestination_address=\"t@x.com\"\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let b = meta.bindings.unwrap();
        let e = b.send_email.get("EMAIL").unwrap();
        assert_eq!(e.allowed_sender_addresses, ["a@x.com", "b@x.com"]);
        assert!(e.destination_address.is_none());
        let pin = b.send_email.get("PIN").unwrap();
        assert_eq!(pin.destination_address.as_deref(), Some("t@x.com"));

        // Fail closed: missing name, pin+list both set, invalid email,
        // non-array allowlist, unknown keys, non-table shape.
        for toml in [
            "[[send_email]]\ndestination_address=\"t@x.com\"\n",
            "[[send_email]]\nname=\"E\"\ndestination_address=\"t@x.com\"\nallowed_destination_addresses=[\"a@x.com\"]\n",
            "[[send_email]]\nname=\"E\"\ndestination_address=\"not-an-email\"\n",
            "[[send_email]]\nname=\"E\"\nallowed_sender_addresses=[\"no-at\"]\n",
            "[[send_email]]\nname=\"E\"\nallowed_sender_addresses=\"a@x.com\"\n",
            "[[send_email]]\nname=\"E\"\nextra=1\n",
            "send_email={name=\"E\"}\n",
        ] {
            let dir = wrangler_loader_body(toml);
            assert!(load_worker_meta(dir.path()).is_err(), "{toml}");
        }
    }

    /// wrangler `[limits]` → internal `quotas` — the CF surface maps onto
    /// the signed snapshot's `WorkerQuotas`; unspecified dimensions keep the
    /// platform default (never rendered as a workerd `limits` entry) and
    /// out-of-bounds / unknown keys are refused with the exact path.
    #[test]
    fn wrangler_limits_normalize_into_quotas() {
        let one =
            normalize_limits(&serde_json::json!({"cpu_ms": 300})).expect("partial limits accepted");
        assert_eq!(one["cpuMs"], 300);
        assert_eq!(
            one["memoryMb"],
            snapshot_protocol::compute_desired::DEFAULT_MEMORY_MB
        );
        assert_eq!(
            one["subrequests"],
            snapshot_protocol::compute_desired::DEFAULT_SUBREQUESTS
        );
        assert_eq!(
            one["outboundConnections"],
            snapshot_protocol::compute_desired::DEFAULT_OUTBOUND_CONNECTIONS
        );

        let both =
            normalize_limits(&serde_json::json!({"cpu_ms": 100, "subrequests": 150})).unwrap();
        assert_eq!(both["cpuMs"], 100);
        assert_eq!(both["subrequests"], 150);

        // Bounds: below the isolate minimum, above the 5-minute CPU cap, and
        // above the platform subrequest cap.
        for (input, path) in [
            (serde_json::json!({"cpu_ms": 0}), "limits.cpu_ms"),
            (serde_json::json!({"cpu_ms": 300_001}), "limits.cpu_ms"),
            (serde_json::json!({"cpu_ms": "100"}), "limits.cpu_ms"),
            (
                serde_json::json!({"subrequests": 10_001}),
                "limits.subrequests",
            ),
            (serde_json::json!({"subrequests": -1}), "limits.subrequests"),
        ] {
            let err = normalize_limits(&input).expect_err("out of bounds must fail");
            assert_eq!(err.code, "VRT_CONFIG_INVALID", "{input}: {err:?}");
            assert_eq!(err.field.as_deref(), Some(path), "{input}: {err:?}");
        }
        // Unknown keys fail closed (CF `limits` has only cpu_ms/subrequests).
        let err = normalize_limits(&serde_json::json!({"memory_mb": 256}))
            .expect_err("unknown key must fail");
        assert_eq!(err.code, "VRT_CONFIG_UNSUPPORTED_FIELD");
        assert_eq!(err.field.as_deref(), Some("limits.memory_mb"));
    }

    #[test]
    fn parse_dev_table_gates_and_fail_closed() {
        // The wrangler `[dev]` surface of dev consumes: ip/port/host.
        let full = parse_dev_table(&serde_json::json!({
            "ip": "localhost",
            "port": 9_000,
            "host": "dev.example.com",
        }))
        .expect("ip/port/host accepted");
        assert_eq!(full.ip.as_deref(), Some("127.0.0.1"));
        assert_eq!(full.port, Some(9_000));
        assert_eq!(full.host.as_deref(), Some("dev.example.com"));

        // IPv6 loopback is accepted (DevPorts::alloc renders it as a
        // bracketed SocketAddr) and canonicalizes to `::1`.
        let v6 = parse_dev_table(&serde_json::json!({"ip": "::1"})).unwrap();
        assert_eq!(v6.ip.as_deref(), Some("::1"));

        // `local_protocol = "http"` is a no-op (the only mode).
        parse_dev_table(&serde_json::json!({"local_protocol": "http"})).unwrap();

        // Malformed values.
        for (input, path) in [
            (serde_json::json!(1), "dev"),
            (serde_json::json!({"ip": 1}), "dev.ip"),
            (serde_json::json!({"ip": "0.0.0.0"}), "dev.ip"),
            (serde_json::json!({"ip": "example.com"}), "dev.ip"),
            (serde_json::json!({"port": 0}), "dev.port"),
            (serde_json::json!({"port": 70000}), "dev.port"),
            (serde_json::json!({"port": "9000"}), "dev.port"),
            (serde_json::json!({"host": ""}), "dev.host"),
            (serde_json::json!({"host": "localhost"}), "dev.host"),
            (serde_json::json!({"host": "Dev.Example.com"}), "dev.host"),
            (serde_json::json!({"host": "*.example.com"}), "dev.host"),
        ] {
            let err = parse_dev_table(&input).expect_err("malformed must fail");
            assert_eq!(err.code, "VRT_CONFIG_INVALID", "{input}: {err:?}");
            assert_eq!(err.field.as_deref(), Some(path), "{input}: {err:?}");
        }

        // Unimplemented dev semantics fail closed.
        for (input, path) in [
            (
                serde_json::json!({"local_protocol": "https"}),
                "dev.local_protocol",
            ),
            (
                serde_json::json!({"upstream_protocol": "https"}),
                "dev.upstream_protocol",
            ),
            (
                serde_json::json!({"inspector_port": 9229}),
                "dev.inspector_port",
            ),
            (serde_json::json!({"remote": true}), "dev.remote"),
            (
                serde_json::json!({"generate_types": true}),
                "dev.generate_types",
            ),
        ] {
            let err = parse_dev_table(&input).expect_err("unsupported key must fail");
            assert_eq!(err.code, "VRT_CONFIG_UNSUPPORTED_FIELD", "{input}: {err:?}");
            assert_eq!(err.field.as_deref(), Some(path), "{input}: {err:?}");
        }
    }

    /// `of deploy` must accept every `[dev]` key `wrangler deploy` accepts:
    /// the shared normalize path drops the table instead of validating it,
    /// so `ip = "0.0.0.0"`, `inspector_port` and `remote` cannot fail a
    /// deploy. Only `of dev` re-validates strictly (`parse_dev_table`).
    #[test]
    fn deploy_projection_drops_dev_table() {
        let dir = wrangler_loader_body(
            "[dev]\nip = \"0.0.0.0\"\nport = 9000\nhost = \"dev.example.com\"\n\
             inspector_port = 9229\nremote = true\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert!(
            meta.dev.is_none(),
            "wrangler [dev] is session-local and must not enter the pack"
        );
        // The dev entry re-validates the same table strictly: a public
        // bind stays refused there even though deploy accepted it.
        assert!(
            load_dev_table_for_env(dir.path(), None).is_err(),
            "of dev must still refuse a non-loopback bind"
        );

        // A dev-acceptable table resolves on the dev entry.
        let loopback = wrangler_loader_body(
            "[dev]\nip = \"localhost\"\nport = 9000\nhost = \"dev.example.com\"\n",
        );
        let dev = load_dev_table_for_env(loopback.path(), None)
            .expect("dev entry resolves the table")
            .expect("table present");
        assert_eq!(dev.port, Some(9_000));
        assert_eq!(dev.host.as_deref(), Some("dev.example.com"));
    }

    /// The normalized `limits` block must survive the anti-drop projection
    /// (`CARRIED`) and deserialize into the pack-time `[quotas]` shape.
    #[test]
    fn wrangler_limits_reach_the_pack_shape() {
        let dir = wrangler_loader_body("[limits]\ncpu_ms = 250\nsubrequests = 120\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let quotas = meta.quotas.expect("quotas projected from limits");
        assert_eq!(quotas.cpu_ms, 250);
        assert_eq!(quotas.subrequests, 120);
        assert_eq!(
            quotas.memory_mb,
            snapshot_protocol::compute_desired::DEFAULT_MEMORY_MB
        );
    }

    /// Cloudflare `queues.producers[].delivery_delay`: accepted, bounded
    /// 0..=86400, and carried into the pack-time `[bindings.queue]` table
    /// form — the legacy string form stays byte-identical.
    #[test]
    fn wrangler_queue_delivery_delay_normalizes_and_validates() {
        use crate::worker_pkg::QueueTargetToml;

        let dir = wrangler_loader_body(
            "[[queues.producers]]\nbinding = \"Q\"\nqueue = \"q1\"\ndelivery_delay = 30\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let queue = meta.bindings.unwrap().queue;
        let target = queue.get("Q").expect("queue binding");
        assert_eq!(target.queue(), "q1");
        assert_eq!(target.delivery_delay_seconds(), Some(30));
        assert_eq!(
            serde_json::to_value(target).unwrap(),
            serde_json::json!({"queue": "q1", "deliveryDelaySeconds": 30})
        );

        // Upper bound is inclusive.
        let dir = wrangler_loader_body(
            "[[queues.producers]]\nbinding = \"Q\"\nqueue = \"q1\"\ndelivery_delay = 86400\n",
        );
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        assert_eq!(
            meta.bindings
                .unwrap()
                .queue
                .get("Q")
                .unwrap()
                .delivery_delay_seconds(),
            Some(86_400)
        );

        // Absent keeps the legacy plain-string wire shape.
        let dir = wrangler_loader_body("[[queues.producers]]\nbinding = \"Q\"\nqueue = \"q1\"\n");
        let (meta, _) = load_worker_meta(dir.path()).unwrap();
        let target = meta.bindings.unwrap().queue.remove("Q").unwrap();
        assert_eq!(target, QueueTargetToml::Name("q1".into()));
        assert_eq!(serde_json::to_string(&target).unwrap(), "\"q1\"");

        // Out-of-range / non-integer values are rejected with the field path.
        for (value, field) in [
            ("86401", "queues.producers[0].delivery_delay"),
            ("-1", "queues.producers[0].delivery_delay"),
            ("\"5\"", "queues.producers[0].delivery_delay"),
        ] {
            let dir = wrangler_loader_body(&format!(
                "[[queues.producers]]\nbinding = \"Q\"\nqueue = \"q1\"\ndelivery_delay = {value}\n"
            ));
            let err = load_worker_meta(dir.path()).expect_err("must fail closed");
            assert!(
                format!("{err:?}").contains(field),
                "value {value}: {err:?}"
            );
        }
    }

    /// Native `vrt.toml` equivalent: `NAME = { queue, deliveryDelaySeconds }`
    /// (camelCase like the rest of the native shape). Unknown keys fail closed
    /// and the legacy string form keeps serializing as a string.
    #[test]
    fn vrt_toml_queue_delay_table_shape() {
        use crate::worker_pkg::{BindingsToml, QueueTargetToml};

        let b: BindingsToml = toml::from_str(
            "[queue]\nQ = \"q1\"\nR = { queue = \"q2\", deliveryDelaySeconds = 5 }\n",
        )
        .unwrap();
        assert_eq!(b.queue.get("Q"), Some(&QueueTargetToml::Name("q1".into())));
        let r = b.queue.get("R").unwrap();
        assert_eq!(r.queue(), "q2");
        assert_eq!(r.delivery_delay_seconds(), Some(5));

        // Decode accepts the raw u32; the 0..=86400 bound is enforced by
        // the packer (covered in local_ops) and the signed protocol.
        let over: BindingsToml =
            toml::from_str("[queue]\nR = { queue = \"q2\", deliveryDelaySeconds = 86401 }\n")
                .unwrap();
        assert_eq!(
            over.queue.get("R").unwrap().delivery_delay_seconds(),
            Some(86_401)
        );

        // Unknown / snake_case / negative fails closed at decode.
        for bad in [
            "[queue]\nR = { queue = \"q2\", deliveryDelay = 5 }\n",
            "[queue]\nR = { queue = \"q2\", delivery_delay = 5 }\n",
            "[queue]\nR = { queue = \"q2\", deliveryDelaySeconds = -1 }\n",
        ] {
            assert!(
                toml::from_str::<BindingsToml>(bad).is_err(),
                "must reject {bad:?}"
            );
        }
    }
}

/// Redact every double-quoted span of a toml error message: toml/serde
/// render offending keys in backticks (field names — safe to keep) and
/// values via `Debug` (double-quoted, `\\` escapes inside). A plain
/// quote-toggle would treat `\\"` as a closing quote and echo the
/// value's tail, so escapes are consumed with the next char. Returns
/// `None` when the message ends inside an unterminated quote — the
/// caller must then use a fixed phrase instead of risking a partial
/// leak.
fn redact_toml_message(msg: &str) -> Option<String> {
    let mut out = String::with_capacity(msg.len());
    let mut chars = msg.chars();
    let mut in_val = false;
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                out.push('"');
                if in_val {
                    in_val = false;
                } else {
                    in_val = true;
                    out.push_str("<redacted>");
                }
            }
            '\\' if in_val => {
                // Escaped char inside the value — swallow it whole so
                // \" cannot end the span early.
                chars.next();
            }
            '`' if !in_val => {
                // Backticks are ambiguous: toml renders field names in
                // them (`unknown field `x`` — safe to keep) AND typed
                // values (`invalid type: integer `5`` — a bare number
                // can still be a secret). Keep only spans that follow
                // a field/key/variant/enum keyword; redact the rest.
                let span: String = chars.by_ref().take_while(|&c| c != '`').collect();
                let tail: String = out
                    .chars()
                    .rev()
                    .take(9)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect();
                let keep = ["field ", "key ", "variant ", "enum "]
                    .iter()
                    .any(|p| tail.ends_with(p));
                if keep {
                    out.push('`');
                    out.push_str(&span);
                    out.push('`');
                } else {
                    out.push_str("`<redacted>`");
                }
            }
            c if !in_val => out.push(c),
            _ => {}
        }
    }
    (!in_val).then_some(out)
}
