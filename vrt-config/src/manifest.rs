//! Typed deployment manifest — the normalized form of a
//! `packWranglerProject` output (`server/src/services/workerIntegration.ts`
//! `packProject` → `@wdl-dev/cli` `packWranglerProject`).
//!
//! ## Correspondence to the pack wire shape
//!
//! `packWranglerProject` emits
//! `{ mainModule, modules: Record<relPath, wire>, bindings?, vars?,
//!    compatibilityDate?, compatibilityFlags?, ... }` where each module
//! wire value is one of (`lib/bundle-modules.js::toWireModule`):
//!
//! | pack wire value                | `ModuleType` | content              |
//! | ------------------------------ | ------------ | -------------------- |
//! | `"<utf8 source>"`              | `esModule`   | utf8 text            |
//! | `{ "cjs": "<utf8>" }`          | `commonJs`   | utf8 text            |
//! | `{ "text": "<utf8>" }`         | `text`       | utf8 text            |
//! | `{ "json": <parsed JSON> }`    | `json`       | canonical JSON text  |
//! | `{ "wasm_b64": "<base64>" }`   | `wasm`       | raw bytes            |
//! | `{ "data_b64": "<base64>" }`   | `data`       | raw bytes            |
//!
//! The normalized [`DeployManifest`] instead carries a `modules` *list* of
//! `{ name, type, text|dataBase64 }`. [`DeployManifest::from_pack`]
//! performs the wire → typed conversion (fail closed on unknown wire
//! forms); the control plane adapter may also construct the typed form
//! directly. `bindings`/`vars`/etc. are deliberately NOT part of this
//! manifest: vrt binding truth is the signed `compute.desired` snapshot,
//! not the uploaded document.

use crate::canonical::canonical_bytes;
use crate::error::BundleError;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Maximum length of a module name (UTF-8 bytes).
pub const MAX_MODULE_NAME_LEN: usize = 512;
/// Maximum modules per bundle.
pub const MAX_MODULES: usize = 1024;
/// Maximum decoded bytes of a single module.
pub const MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum total decoded module bytes per bundle.
pub const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum compatibility flags.
pub const MAX_COMPAT_FLAGS: usize = 32;
/// Maximum compatibility-flag token length (same rule as compute.desired).
pub const MAX_COMPAT_FLAG_LEN: usize = 64;
/// Maximum accepted stored-bundle document size.
pub const MAX_STORED_BYTES: usize = 96 * 1024 * 1024;

/// Module content kind carried by a [`DeployManifest`]. Names match the
/// `workerd.capnp` `Worker.Module` union fields they are emitted as.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum ModuleType {
    /// JavaScript ES module (utf8 source).
    EsModule,
    /// CommonJS module (utf8 source).
    CommonJs,
    /// Raw utf8 text module.
    Text,
    /// JSON module; content is canonical JSON text.
    Json,
    /// Raw bytes module.
    Data,
    /// WebAssembly module.
    Wasm,
}

impl ModuleType {
    /// The capnp `Worker.Module` union member this type emits.
    pub fn capnp_field(self) -> &'static str {
        match self {
            Self::EsModule => "esModule",
            Self::CommonJs => "commonJsModule",
            Self::Text => "text",
            Self::Json => "json",
            Self::Data => "data",
            Self::Wasm => "wasm",
        }
    }
}

/// One module in a deployment bundle. The on-disk shape is internally
/// tagged on `type`: text kinds carry `text`, binary kinds carry
/// `dataBase64` (canonical base64).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Module {
    /// ES module source.
    EsModule(TextModule),
    /// CommonJS module source.
    CommonJs(TextModule),
    /// utf8 text.
    Text(TextModule),
    /// Canonical JSON text (validated to parse).
    Json(TextModule),
    /// Raw bytes, base64 on the wire.
    Data(DataModule),
    /// WebAssembly bytes, base64 on the wire.
    Wasm(DataModule),
}

/// Shared fields of a text-kind module.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextModule {
    /// Module name — a relative path key from the pack manifest.
    pub name: String,
    /// Module source text.
    pub text: String,
}

/// Shared fields of a binary-kind module.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataModule {
    /// Module name — a relative path key from the pack manifest.
    pub name: String,
    /// Canonical base64 of the module bytes.
    pub data_base64: String,
}

impl Module {
    /// The module name.
    pub fn name(&self) -> &str {
        match self {
            Self::EsModule(m) | Self::CommonJs(m) | Self::Text(m) | Self::Json(m) => &m.name,
            Self::Data(m) | Self::Wasm(m) => &m.name,
        }
    }

    /// The module type.
    pub fn module_type(&self) -> ModuleType {
        match self {
            Self::EsModule(_) => ModuleType::EsModule,
            Self::CommonJs(_) => ModuleType::CommonJs,
            Self::Text(_) => ModuleType::Text,
            Self::Json(_) => ModuleType::Json,
            Self::Data(_) => ModuleType::Data,
            Self::Wasm(_) => ModuleType::Wasm,
        }
    }

    /// Raw content bytes: utf8 for text kinds, decoded bytes for
    /// binary kinds. Returns `Err(ModuleContent)` on bad base64.
    pub fn content_bytes(&self) -> Result<Vec<u8>, BundleError> {
        match self {
            Self::EsModule(m) | Self::CommonJs(m) | Self::Text(m) | Self::Json(m) => {
                Ok(m.text.as_bytes().to_vec())
            }
            Self::Data(m) | Self::Wasm(m) => STANDARD
                .decode(&m.data_base64)
                .map_err(|_| BundleError::ModuleContent),
        }
    }
}

/// The normalized deployment manifest — `vrt-config`'s typed input.
/// Unknown fields fail closed at decode.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeployManifest {
    /// Name of the entry module; must be present in `modules`.
    pub main_module: String,
    /// Module list; names must be unique.
    pub modules: Vec<Module>,
    /// Cloudflare `compatibility_date`, strict `YYYY-MM-DD`.
    pub compatibility_date: String,
    /// Cloudflare `compatibility_flags` tokens.
    pub compatibility_flags: Vec<String>,
}

impl DeployManifest {
    /// Build a typed manifest from the raw `packWranglerProject` manifest
    /// document (the `manifest` member of `PackResult`). Fails closed on
    /// unknown module wire forms, non-string dates, or missing members.
    /// Members unrelated to the bundle (`bindings`, `vars`, `routes`,
    /// `assets`, ...) are ignored by design — vrt binding truth comes
    /// from `compute.desired`.
    pub fn from_pack(value: &Value) -> Result<Self, BundleError> {
        let obj = value.as_object().ok_or(BundleError::Manifest("root"))?;
        let main_module = obj
            .get("mainModule")
            .and_then(Value::as_str)
            .ok_or(BundleError::Manifest("mainModule"))?
            .to_owned();
        let modules_obj = obj
            .get("modules")
            .and_then(Value::as_object)
            .ok_or(BundleError::Manifest("modules"))?;
        let compatibility_date = obj
            .get("compatibilityDate")
            .and_then(Value::as_str)
            .ok_or(BundleError::Manifest("compatibilityDate"))?
            .to_owned();
        let compatibility_flags = obj
            .get("compatibilityFlags")
            .map(|flags| {
                flags
                    .as_array()
                    .ok_or(BundleError::Manifest("compatibilityFlags"))?
                    .iter()
                    .map(|f| {
                        f.as_str()
                            .map(str::to_owned)
                            .ok_or(BundleError::Manifest("compatibilityFlags"))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        if modules_obj.len() > MAX_MODULES {
            return Err(BundleError::Capacity("modules"));
        }
        let mut modules = Vec::with_capacity(modules_obj.len());
        for (name, wire) in modules_obj {
            let module = match wire {
                Value::String(source) => Module::EsModule(TextModule {
                    name: name.clone(),
                    text: source.clone(),
                }),
                Value::Object(fields) if fields.len() == 1 => {
                    let (key, inner) = fields.iter().next().expect("single member");
                    let text = |field: &'static str| -> Result<String, BundleError> {
                        inner
                            .as_str()
                            .map(str::to_owned)
                            .ok_or(BundleError::Manifest(field))
                    };
                    match key.as_str() {
                        "cjs" => Module::CommonJs(TextModule {
                            name: name.clone(),
                            text: text("modules.cjs")?,
                        }),
                        "text" => Module::Text(TextModule {
                            name: name.clone(),
                            text: text("modules.text")?,
                        }),
                        "json" => Module::Json(TextModule {
                            name: name.clone(),
                            text: String::from_utf8(canonical_bytes(inner))
                                .map_err(|_| BundleError::ModuleContent)?,
                        }),
                        "wasm_b64" => Module::Wasm(DataModule {
                            name: name.clone(),
                            data_base64: text("modules.wasm_b64")?,
                        }),
                        "data_b64" => Module::Data(DataModule {
                            name: name.clone(),
                            data_base64: text("modules.data_b64")?,
                        }),
                        _ => return Err(BundleError::ModuleType),
                    }
                }
                _ => return Err(BundleError::ModuleType),
            };
            modules.push(module);
        }
        Ok(Self {
            main_module,
            modules,
            compatibility_date,
            compatibility_flags,
        })
    }

    /// Semantic validation beyond typed decode: module names, uniqueness,
    /// bounds, main-module presence, compat fields, and content
    /// representability (base64 decode + JSON re-serialization for
    /// `json` modules).
    pub fn validate(&self) -> Result<(), BundleError> {
        if self.modules.len() > MAX_MODULES {
            return Err(BundleError::Capacity("modules"));
        }
        if self.compatibility_flags.len() > MAX_COMPAT_FLAGS {
            return Err(BundleError::Capacity("compatibilityFlags"));
        }
        if !valid_compat_date(&self.compatibility_date) {
            return Err(BundleError::CompatibilityDate);
        }
        if self
            .compatibility_flags
            .iter()
            .any(|flag| !valid_compat_flag(flag))
        {
            return Err(BundleError::CompatibilityFlag);
        }
        if !valid_module_name(&self.main_module) {
            return Err(BundleError::ModuleName);
        }

        let mut names = std::collections::HashSet::with_capacity(self.modules.len());
        let mut folded = std::collections::HashSet::with_capacity(self.modules.len());
        let mut total = 0usize;
        for module in &self.modules {
            if !valid_module_name(module.name()) {
                return Err(BundleError::ModuleName);
            }
            if !names.insert(module.name()) {
                return Err(BundleError::DuplicateModule);
            }
            // Case-insensitive duplicates collide on case-insensitive
            // filesystem maps — the TypeScript bundle check applies the
            // same fold so both sides accept/reject identically.
            if !folded.insert(module.name().to_lowercase()) {
                return Err(BundleError::DuplicateModule);
            }
            let content = module.content_bytes()?;
            if content.len() > MAX_MODULE_BYTES {
                return Err(BundleError::Capacity("moduleBytes"));
            }
            total = total
                .checked_add(content.len())
                .ok_or(BundleError::Capacity("totalBytes"))?;
            if total > MAX_BUNDLE_BYTES {
                return Err(BundleError::Capacity("totalBytes"));
            }
            if let Module::Json(m) = module {
                // `json` module text must be parseable JSON so the emitted
                // capnp `json` field holds a valid JSON document.
                serde_json::from_str::<Value>(&m.text).map_err(|_| BundleError::ModuleContent)?;
            }
        }
        if !names.contains(self.main_module.as_str()) {
            return Err(BundleError::MainModule);
        }
        // T12/G3: a module name that is a PATH PREFIX of another name
        // (`lib` next to `lib/x.js`) materializes as file-vs-directory —
        // the assembler cannot create both. Compared on the folded form
        // so the TypeScript bundle check and this validator accept and
        // reject the same set of manifests byte-for-byte.
        for a in &folded {
            for b in &folded {
                if b.len() > a.len() && b.starts_with(&format!("{a}/")) {
                    return Err(BundleError::DuplicateModule);
                }
            }
        }
        Ok(())
    }
}

/// Module name safety rule: non-empty relative name that cannot escape
/// the bundle directory or alias another name after POSIX normalization:
/// no `..` anywhere, no `.`/`..`/empty path segments, no absolute paths,
/// no `:` (Windows separators), no backslashes, no control characters,
/// bounded length. (A `.` segment would materialize to the same file as
/// its sibling, so two distinct names could alias one path on disk.)
pub(crate) fn valid_module_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MODULE_NAME_LEN
        && !value.contains("..")
        && !value.contains('\\')
        && !value.contains(':')
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("//")
        && value != "."
        && !value.split('/').any(|seg| seg == ".")
        && value
            .bytes()
            .all(|b| (0x20..=0x7e).contains(&b) || b >= 0x80)
}

/// Strict `YYYY-MM-DD` (zero-padded, real calendar date) — same rule as
/// `compute_desired::VersionDesired::compatibilityDate`.
pub fn valid_compat_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
        && NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

/// Compatibility-flag token — same rule as `compute_desired`.
pub fn valid_compat_flag(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_COMPAT_FLAG_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'.' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn text_module(name: &str) -> Module {
        Module::EsModule(TextModule {
            name: name.to_owned(),
            text: "export default {}".to_owned(),
        })
    }

    fn manifest(modules: Vec<Module>) -> DeployManifest {
        DeployManifest {
            main_module: "index.js".into(),
            modules,
            compatibility_date: "2026-11-01".into(),
            compatibility_flags: vec!["nodejs_compat".into()],
        }
    }

    #[test]
    fn valid_manifest_passes() {
        manifest(vec![text_module("index.js"), text_module("lib/util.js")])
            .validate()
            .unwrap();
    }

    #[test]
    fn main_module_must_exist() {
        let mut m = manifest(vec![text_module("index.js")]);
        m.main_module = "missing.js".into();
        assert_eq!(m.validate(), Err(BundleError::MainModule));
        m.main_module = "../evil.js".into();
        assert_eq!(m.validate(), Err(BundleError::ModuleName));
    }

    #[test]
    fn module_name_rejects_traversal_and_injection() {
        for bad in [
            "../evil.js",
            "a\\b.js",
            "/abs.js",
            "a//b.js",
            "x.js\u{0}",
            "C:x.js",
            "dir/",
            ".",
            "..",
            "a..b.js",
            "a/./b.js",
            "./a.js",
            "nl\n.js",
        ] {
            assert!(!valid_module_name(bad), "accepted {bad:?}");
        }
        for good in [
            "index.js",
            "lib/util.js",
            ".hidden",
            "a-b_c.d@e.js",
            "\u{4e2d}\u{6587}\u{5b57}.js",
            &"x".repeat(MAX_MODULE_NAME_LEN),
        ] {
            assert!(valid_module_name(good), "rejected {good:?}");
        }
        assert!(!valid_module_name(&"x".repeat(MAX_MODULE_NAME_LEN + 1)));
    }
    /// T12/G3+G6: shared bundle boundary vectors — `moduleName` cases
    /// run through `valid_module_name` (byte-length incl. multibyte),
    /// `modulePrefix` cases build a two-module manifest and run full
    /// `validate()`. TS `vrtBundle.ts` asserts the same verdicts.
    #[test]
    fn shared_bundle_boundary_vectors() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../shared/vrt-bundle-boundary-vectors.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cases = doc["cases"].as_array().expect("cases array");
        assert!(!cases.is_empty());
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let field = case["field"].as_str().unwrap();
            let value = case["value"].as_str().unwrap();
            let expect = case["expect"].as_str().unwrap() == "accept";
            let actual = match field {
                "moduleName" => valid_module_name(value),
                "modulePrefix" => {
                    let other = case["other"].as_str().unwrap();
                    let m = DeployManifest {
                        main_module: value.to_string(),
                        modules: vec![text_module(value), text_module(other)],
                        compatibility_date: "2024-01-01".into(),
                        compatibility_flags: vec![],
                    };
                    m.validate().is_ok()
                }
                other => panic!("unknown field {other}"),
            };
            assert_eq!(actual, expect, "vector {name} ({field})");
        }
    }

    #[test]
    fn duplicate_module_rejected() {
        let m = manifest(vec![text_module("index.js"), text_module("index.js")]);
        assert_eq!(m.validate(), Err(BundleError::DuplicateModule));
    }

    #[test]
    fn case_insensitive_duplicate_module_rejected() {
        let m = manifest(vec![text_module("Index.JS"), text_module("index.js")]);
        assert_eq!(m.validate(), Err(BundleError::DuplicateModule));
    }

    #[test]
    fn pack_wire_conversion() {
        let pack = json!({
            "mainModule": "index.js",
            "modules": {
                "index.js": "export default { fetch(){ return new Response('x') } }",
                "lib.cjs": {"cjs": "module.exports = 1"},
                "a.txt": {"text": "hello"},
                "cfg.json": {"json": {"b": 2, "a": 1}},
                "m.wasm": {"wasm_b64": "AAEC"},
                "b.bin": {"data_b64": "AQIDBA=="},
                "weird": {"unknown": 1}
            },
            "compatibilityDate": "2026-11-01",
            "compatibilityFlags": ["nodejs_compat"],
            "bindings": {"KV": {"type": "kv", "id": "ignored"}},
            "vars": {"A": "ignored"}
        });
        // Unknown wire form fails closed.
        assert_eq!(
            DeployManifest::from_pack(&pack),
            Err(BundleError::ModuleType)
        );
        let mut pack2 = pack.clone();
        pack2["modules"].as_object_mut().unwrap().remove("weird");
        let m = DeployManifest::from_pack(&pack2).unwrap();
        m.validate().unwrap();
        assert_eq!(m.main_module, "index.js");
        assert_eq!(m.modules.len(), 6);
        let json = m.modules.iter().find(|m| m.name() == "cfg.json").unwrap();
        // JSON is canonicalized (keys sorted, no whitespace).
        assert_eq!(json.content_bytes().unwrap(), b"{\"a\":1,\"b\":2}");
        let wasm = m.modules.iter().find(|m| m.name() == "m.wasm").unwrap();
        assert_eq!(wasm.content_bytes().unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn pack_requires_typed_members() {
        assert_eq!(
            DeployManifest::from_pack(&json!({"mainModule": 1})),
            Err(BundleError::Manifest("mainModule"))
        );
        assert_eq!(
            DeployManifest::from_pack(&json!({
                "mainModule": "index.js",
                "modules": {},
                "compatibilityDate": "2026-11-01",
                "compatibilityFlags": "yes"
            })),
            Err(BundleError::Manifest("compatibilityFlags"))
        );
        // Non-string members inside a wire object fail closed too.
        assert_eq!(
            DeployManifest::from_pack(&json!({
                "mainModule": "index.js",
                "modules": {"index.js": {"cjs": 42}},
                "compatibilityDate": "2026-11-01"
            })),
            Err(BundleError::Manifest("modules.cjs"))
        );
    }
}
