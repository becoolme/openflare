//! [`VersionBundle`]: an immutable, content-addressed deployment artifact.
//!
//! ## Digest and object-key rules
//!
//! - Each module gets `moduleDigest = sha256(contentBytes)` (utf8 bytes
//!   for text kinds, decoded bytes for `data`/`wasm`; `json` module text
//!   is canonical JSON, so formatting does not affect identity).
//! - `bundleDigest = sha256(canonical manifest)` where the canonical
//!   manifest is
//!   `{"compatibilityDate":…,"compatibilityFlags":[…],"mainModule":…,
//!     "modules":[{"digest":…,"name":…,"type":…}, …sorted by name],
//!     "schemaVersion":1}` — module contents never appear inside it.
//! - Object-storage key: `workers/v1/bundles/<bundleDigest>` (see
//!   [`BUNDLE_KEY_PREFIX`]).
//!
//! ## Stored document
//!
//! The object at that key is itself canonical JSON (this crate's
//! canonical writer): `{compatibilityDate, compatibilityFlags, digest,
//! mainModule, modules, schemaVersion}` where `modules` are the same
//! `{name,type,text|dataBase64}` entries as [`DeployManifest`], sorted by
//! name. [`VersionBundle::parse_stored`] re-validates the manifest and
//! recomputes the bundle digest from the stored content — a corrupted or
//! swapped object fails closed.

use crate::canonical::{canonical_bytes, utf16_cmp};
use crate::error::BundleError;
use crate::manifest::{DeployManifest, Module, ModuleType, MAX_STORED_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Canonical-manifest schema version.
pub const SCHEMA_VERSION: u8 = 1;
/// Object-storage key prefix for version bundles; the key is
/// `workers/v1/bundles/<bundleDigest>`.
pub const BUNDLE_KEY_PREFIX: &str = "workers/v1/bundles/";

/// A [`DeployManifest`] with computed digests and its stored document.
/// Build via [`VersionBundle::build`] (typed input),
/// [`VersionBundle::from_pack`] (raw `packWranglerProject` output), or
/// [`VersionBundle::parse_stored`] (host-side fetch, full re-verify).
#[derive(Clone, Debug)]
pub struct VersionBundle {
    manifest: DeployManifest,
    digest: String,
    stored: Vec<u8>,
}

/// Per-module digest record, sorted by module name.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModuleDigest {
    /// Module name.
    pub name: String,
    /// Module type.
    #[serde(rename = "type")]
    pub module_type: ModuleType,
    /// Lowercase-hex sha256 of the module content bytes.
    pub digest: String,
}

impl VersionBundle {
    /// Validate `manifest` and compute its content-addressed bundle.
    /// `json` module text is re-canonicalized so the digest never
    /// depends on JSON formatting from a directly constructed manifest
    /// (`from_pack` output is already canonical).
    pub fn build(mut manifest: DeployManifest) -> Result<Self, BundleError> {
        manifest.validate()?;
        for module in &mut manifest.modules {
            if let Module::Json(m) = module {
                let parsed: Value =
                    serde_json::from_str(&m.text).map_err(|_| BundleError::ModuleContent)?;
                m.text = String::from_utf8(canonical_bytes(&parsed))
                    .map_err(|_| BundleError::ModuleContent)?;
            }
        }
        let digest = sha256_hex(&canonical_manifest(&manifest)?);
        let stored = stored_document(&manifest, &digest)?;
        Ok(Self {
            manifest,
            digest,
            stored,
        })
    }

    /// Build from a raw `packWranglerProject` manifest document.
    pub fn from_pack(value: &Value) -> Result<Self, BundleError> {
        Self::build(DeployManifest::from_pack(value)?)
    }

    /// Re-derive a bundle from a stored document fetched from object
    /// storage. Re-validates the manifest and recomputes every module
    /// digest plus the bundle digest from the stored content — a
    /// corrupted or swapped object fails closed.
    pub fn parse_stored(bytes: &[u8]) -> Result<Self, BundleError> {
        if bytes.len() > MAX_STORED_BYTES {
            return Err(BundleError::Capacity("storedBytes"));
        }
        let stored: StoredDocument =
            serde_json::from_slice(bytes).map_err(|_| BundleError::Manifest("storedDocument"))?;
        if stored.schema_version != SCHEMA_VERSION {
            return Err(BundleError::Manifest("schemaVersion"));
        }
        if !is_lower_hex(&stored.digest) {
            return Err(BundleError::Digest);
        }
        let manifest = DeployManifest {
            main_module: stored.main_module,
            modules: stored.modules,
            compatibility_date: stored.compatibility_date,
            compatibility_flags: stored.compatibility_flags,
        };
        manifest.validate()?;
        let digest = sha256_hex(&canonical_manifest(&manifest)?);
        if digest != stored.digest {
            return Err(BundleError::StoredVerify);
        }
        Ok(Self {
            manifest,
            digest,
            stored: bytes.to_vec(),
        })
    }

    /// Bundle digest: `sha256` of the canonical manifest (64 hex chars).
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Object-storage key (`workers/v1/bundles/<digest>`).
    pub fn object_key(&self) -> String {
        format!("{BUNDLE_KEY_PREFIX}{}", self.digest)
    }

    /// The canonical stored document bytes to persist at [`object_key`].
    pub fn stored_document(&self) -> &[u8] {
        &self.stored
    }

    /// The validated, normalized manifest.
    pub fn manifest(&self) -> &DeployManifest {
        &self.manifest
    }

    /// Module digest records sorted by module name.
    pub fn module_digests(&self) -> Vec<ModuleDigest> {
        let mut digests: Vec<ModuleDigest> = self
            .manifest
            .modules
            .iter()
            .map(|m| ModuleDigest {
                name: m.name().to_owned(),
                module_type: m.module_type(),
                digest: sha256_hex(&m.content_bytes().expect("validated module content")),
            })
            .collect();
        digests.sort_by(|a, b| utf16_cmp(&a.name, &b.name));
        digests
    }
}

/// sha256 over `bytes`, lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// `{"compatibilityDate":…,"compatibilityFlags":[…],"mainModule":…,
///  "modules":[{digest,name,type} sorted by name],"schemaVersion":1}`.
fn canonical_manifest(manifest: &DeployManifest) -> Result<Vec<u8>, BundleError> {
    let mut modules: Vec<&Module> = manifest.modules.iter().collect();
    modules.sort_by(|a, b| utf16_cmp(a.name(), b.name()));
    let mut entries = Vec::with_capacity(modules.len());
    for module in modules {
        entries.push(json!({
            "digest": sha256_hex(&module.content_bytes()?),
            "name": module.name(),
            "type": module.module_type(),
        }));
    }
    Ok(canonical_bytes(&json!({
        "compatibilityDate": manifest.compatibility_date,
        "compatibilityFlags": manifest.compatibility_flags,
        "mainModule": manifest.main_module,
        "modules": entries,
        "schemaVersion": SCHEMA_VERSION,
    })))
}

/// Build the canonical stored document (modules sorted by name, content
/// inline in the same `{name,type,text|dataBase64}` shape as
/// [`DeployManifest`]).
fn stored_document(manifest: &DeployManifest, digest: &str) -> Result<Vec<u8>, BundleError> {
    let mut modules: Vec<&Module> = manifest.modules.iter().collect();
    modules.sort_by(|a, b| utf16_cmp(a.name(), b.name()));
    let module_values: Vec<Value> = modules
        .into_iter()
        .map(|m| serde_json::to_value(m).expect("module serialization"))
        .collect();
    Ok(canonical_bytes(&json!({
        "compatibilityDate": manifest.compatibility_date,
        "compatibilityFlags": manifest.compatibility_flags,
        "digest": digest,
        "mainModule": manifest.main_module,
        "modules": module_values,
        "schemaVersion": SCHEMA_VERSION,
    })))
}

/// Stored-document decode shape; `modules` reuses the [`Module`] wire
/// shape so names/types/contents decode identically to a manifest.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredDocument {
    /// Canonical-manifest schema version.
    schema_version: u8,
    /// Declared bundle digest (re-verified against stored content).
    digest: String,
    /// Entry module name.
    main_module: String,
    /// Module entries (same wire shape as `DeployManifest.modules`).
    modules: Vec<Module>,
    /// `YYYY-MM-DD`.
    compatibility_date: String,
    /// Compat flag tokens.
    compatibility_flags: Vec<String>,
}

fn is_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{DeployManifest, TextModule};
    use serde_json::json;

    fn manifest() -> DeployManifest {
        DeployManifest::from_pack(&json!({
            "mainModule": "index.js",
            "modules": {
                "index.js": "export default {}",
                "lib/a.js": "export const a = 1",
                "b.bin": {"data_b64": "AAEC"}
            },
            "compatibilityDate": "2026-11-01",
            "compatibilityFlags": ["nodejs_compat"]
        }))
        .unwrap()
    }

    #[test]
    fn digest_is_stable_and_content_addressed() {
        let a = VersionBundle::build(manifest()).unwrap();
        let b = VersionBundle::build(manifest()).unwrap();
        assert_eq!(a.digest(), b.digest());
        assert_eq!(a.stored_document(), b.stored_document());
        assert_eq!(a.object_key(), format!("workers/v1/bundles/{}", a.digest()));
        assert!(is_lower_hex(a.digest()));

        // Any content change flips the digest.
        let mut m2 = manifest();
        for module in &mut m2.modules {
            if let Module::EsModule(m) = module {
                if m.name == "index.js" {
                    m.text = "export default {x:1}".into();
                }
            }
        }
        let c = VersionBundle::build(m2).unwrap();
        assert_ne!(a.digest(), c.digest());
    }

    #[test]
    fn module_order_does_not_affect_digest() {
        let mut m = manifest();
        m.modules.reverse();
        let a = VersionBundle::build(manifest()).unwrap();
        let b = VersionBundle::build(m).unwrap();
        assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn compat_fields_affect_digest() {
        let mut m = manifest();
        m.compatibility_flags = vec!["experimental".into()];
        let a = VersionBundle::build(manifest()).unwrap();
        let b = VersionBundle::build(m).unwrap();
        assert_ne!(a.digest(), b.digest());
    }

    #[test]
    fn json_modules_canonicalize_at_build() {
        // A directly constructed manifest with non-canonical JSON text
        // digests identically to a canonical one.
        let direct = DeployManifest {
            main_module: "index.js".into(),
            modules: vec![
                Module::EsModule(TextModule {
                    name: "index.js".into(),
                    text: "export default {}".into(),
                }),
                Module::Json(TextModule {
                    name: "cfg.json".into(),
                    text: "{ \"b\": 2,  \"a\": [ 1,2 ] }".into(),
                }),
            ],
            compatibility_date: "2026-11-01".into(),
            compatibility_flags: vec![],
        };
        let a = VersionBundle::build(direct).unwrap();
        let b = VersionBundle::build(
            DeployManifest::from_pack(&json!({
                "mainModule": "index.js",
                "modules": {
                    "index.js": "export default {}",
                    "cfg.json": {"json": {"b": 2, "a": [1, 2]}}
                },
                "compatibilityDate": "2026-11-01",
                "compatibilityFlags": []
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(a.digest(), b.digest());
        assert_eq!(a.stored_document(), b.stored_document());
    }

    #[test]
    fn stored_roundtrip_verifies() {
        let bundle = VersionBundle::build(manifest()).unwrap();
        let back = VersionBundle::parse_stored(bundle.stored_document()).unwrap();
        assert_eq!(back.digest(), bundle.digest());
        assert_eq!(back.manifest(), bundle.manifest());
    }

    #[test]
    fn tampered_stored_fails_closed() {
        let bundle = VersionBundle::build(manifest()).unwrap();

        // Tampered module name → recomputed digest differs.
        let mut doc: Value = serde_json::from_slice(bundle.stored_document()).unwrap();
        doc["modules"][0]["name"] = json!("evil.js");
        assert_eq!(
            VersionBundle::parse_stored(&serde_json::to_vec(&doc).unwrap()).unwrap_err(),
            BundleError::StoredVerify
        );

        // Forged declared digest → mismatch with recomputed.
        let mut doc: Value = serde_json::from_slice(bundle.stored_document()).unwrap();
        doc["digest"] = json!("0".repeat(64));
        assert_eq!(
            VersionBundle::parse_stored(&serde_json::to_vec(&doc).unwrap()).unwrap_err(),
            BundleError::StoredVerify
        );

        // Unknown member → decode fails closed.
        let mut doc: Value = serde_json::from_slice(bundle.stored_document()).unwrap();
        doc["extra"] = json!(1);
        assert_eq!(
            VersionBundle::parse_stored(&serde_json::to_vec(&doc).unwrap()).unwrap_err(),
            BundleError::Manifest("storedDocument")
        );

        // Malformed digest → typed error.
        let mut doc: Value = serde_json::from_slice(bundle.stored_document()).unwrap();
        doc["digest"] = json!("nope");
        assert_eq!(
            VersionBundle::parse_stored(&serde_json::to_vec(&doc).unwrap()).unwrap_err(),
            BundleError::Digest
        );
    }
}
