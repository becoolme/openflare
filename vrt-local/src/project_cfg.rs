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
//!   * a wrangler `service` entrypoint and missing KV/R2 ids are rejected
//!     or derived at the WorkerToml conversion boundary (documented).

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
    "rules",
    "alias",
    "tsconfig",
    "jsx_factory",
    "jsx_fragment",
    "no_bundle",
    "minify",
    "keep_names",
    "find_additional_modules",
    "preserve_file_names",
    "base_dir",
    "define",
    "upload_source_maps",
    "dev",
    "cwd",
    "config",
    // MIG-14: wrangler `[placement]` (CF Smart Placement / region
    // hints) — dropped; the runtime has no placement semantics.
    "placement",
];
/// Mirrors @wdl-dev/cli NON_INHERITABLE_ENV_KEYS — env blocks REPLACE
/// these keys wholesale instead of merging the top-level value.
const NON_INHERITABLE_ENV_KEYS: &[&str] = &[
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
    "workflows",
    "tail_consumers",
    "secrets",
    "secrets_store_secrets",
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
    let vrt_path = worker_dir.join("vrt.toml");
    if vrt_path.exists() {
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
                load_wrangler_project_config(worker_dir, None, None)
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
fn opt_str<'a>(v: Option<&'a Value>, field: &str) -> Result<Option<&'a str>, ProjectConfigError> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(req_str(v, field)?)),
    }
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
    let mut routes_seen = false;
    let mut route_seen = false;

    for (key, value) in obj {
        match key.as_str() {
            "name" => {
                out.insert("name".into(), Value::from(req_str(value, "name")?));
            }
            "main" => {
                out.insert("main".into(), Value::from(req_str(value, "main")?));
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
                            _ => {
                                return Err(unsupported(&format!("hyperdrive[{i}].{k}"), None))
                            }
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
                                c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'_'
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
                        Some(s) if !s.is_empty() && s.len() <= 32 && s.bytes().all(|c| c.is_ascii_digit()) => s,
                        _ => {
                            return Err(invalid(
                                &format!("ratelimits[{i}].namespace_id"),
                                "must be a decimal-digit string of 1..32 digits",
                            ))
                        }
                    };
                    let simple = e
                        .get("simple")
                        .and_then(|v| v.as_object())
                        .ok_or_else(|| invalid(&format!("ratelimits[{i}].simple"), "must be a table"))?;
                    for k in simple.keys() {
                        match k.as_str() {
                            "limit" | "period" => {}
                            _ => {
                                return Err(unsupported(&format!("ratelimits[{i}].simple.{k}"), None))
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
                            "binding" | "service" | "entrypoint" => {}
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
                    bindings.push(Value::Object(b));
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
                            "binding" | "queue" => {}
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
                        Value::Object(o) => opt_str(o.get("pattern"), &format!("{field}.pattern"))?,
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
    Ok(Value::Object(out))
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
///   * service `entrypoint` has no standalone binding field → rejected;
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
        for key in ["hostnames", "routeHostnames"] {
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
    if let Some(v) = obj.get("durableObjectBindings") {
        wire.insert("durableObjectBindings".into(), v.clone());
    }
    if let Some(v) = obj.get("durableObjectMigrations") {
        wire.insert("durableObjectMigrations".into(), v.clone());
    }
    if let Some(v) = obj.get("workflows") {
        wire.insert("workflows".into(), v.clone());
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
        "crons",
        "queueConsumers",
        "durableObjectBindings",
        "durableObjectMigrations",
        "workflows",
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
                    if b.get("entrypoint").is_some() {
                        return Err(unsupported(
                            &format!("bindings.{name}.entrypoint"),
                            Some("standalone service bindings have no entrypoint"),
                        ));
                    }
                    let target = b
                        .get("service")
                        .and_then(|s| s.as_str())
                        .unwrap_or_default();
                    service.insert(name.into(), Value::from(target));
                }
                "queue" => {
                    // User-side queue name (wrangler `queue`); the real queueId
                    // is derived at pack time from vrt-q:{tenant}:{name}.
                    let qn = b.get("queue").and_then(|i| i.as_str()).unwrap_or(name);
                    queue.insert(name.into(), Value::from(qn));
                }
                "pg" => {
                    // P1: `id` is the logical tenant id — credentials
                    // live in secrets/pg-credentials.json keyed
                    // `vrtpgcred:<id>` (provider-side, never bundled).
                    let id = b.get("id").and_then(|i| i.as_str()).unwrap_or(name);
                    pg.insert(name.into(), Value::from(id));
                }
                "secret" => {
                    // wrangler files never declare secrets — defensive
                    return Err(unsupported(
                        &format!("bindings.{name}"),
                        Some("secrets are declared via [secrets] in vrt.toml or `vrt secret put`"),
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
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../shared/vrt-project-config-vectors.json")
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
