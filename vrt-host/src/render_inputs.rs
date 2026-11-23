//! T17/F1: hot-reloadable render inputs (vars + tenant secrets).
//!
//! Before T17 the capnp assembler read `varsFile`/`secretsFile` once at
//! process start — a rotated secret or a re-deployed var only took
//! effect after a host restart, and nothing in `readyz` could tell.
//! Now the [`RenderInputsProvider`] is consulted on every converge;
//! the loaded values flow into BOTH the render params and the
//! `spec_fingerprint`, so a change is a new expected spec (old
//! generation keeps serving until the new one is actually loaded —
//! "received ≠ loaded" stays false).
//!
//! The provider is the single typed boundary: platform mode points it
//! at the control-plane-rendered files, standalone mode at the
//! `vrt-local`-maintained files — same shape
//! `{tenantId: {varRef: value}}` / `{tenantId: {secretRef: value}}`,
//! same fail-closed checks.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::PathBuf;
use vrt_config::SecretValue;

/// Render inputs resolved per converge tick.
#[derive(Clone, Debug, Default)]
pub struct RenderInputs {
    /// `tenantId → varRef → plaintext` (non-secret — appears in config
    /// text). Tenant-scoped like `secrets`: a flat map let one tenant's
    /// binding resolve another tenant's varRef.
    pub vars: BTreeMap<String, BTreeMap<String, String>>,
    /// `tenantId → secretRef → value` (env-only, T13/M6 namespacing).
    pub secrets: BTreeMap<String, BTreeMap<String, SecretValue>>,
}

/// Load failures — constant names only (paths/values never logged).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RenderInputsError {
    /// File missing or unreadable.
    #[error("render inputs unreadable")]
    Io,
    /// File exists but is not `0600` owner-only.
    #[error("render inputs file permissions insecure")]
    Permissions,
    /// JSON shape/type mismatch.
    #[error("render inputs parse failed")]
    Parse,
    /// File exceeds the size cap.
    #[error("render inputs too large")]
    TooLarge,
}

/// Typed provider for hot-render inputs. `load` is called once per
/// converge (cheap: two small JSON files) and again inside assemble —
/// implementations must return the freshest committed state each call.
pub trait RenderInputsProvider: Send + Sync + Debug {
    /// Freshly load vars + secrets; any failure fails closed.
    fn load(&self) -> Result<RenderInputs, RenderInputsError>;
}

/// Absolute ceiling per input file — bounds JSON parse work.
const MAX_INPUT_FILE_BYTES: u64 = 4 * 1024 * 1024;

/// File-backed provider used by BOTH modes: platform renders these
/// files from the control plane; `vrt-local` writes them from local
/// state. Every `load` re-verifies `0600` + size before parsing — a
/// `chmod` regression between converges fails closed, never silently
/// keeps serving stale secrets.
#[derive(Debug)]
pub struct FileRenderInputs {
    vars_file: Option<PathBuf>,
    secrets_file: Option<PathBuf>,
    /// T19b/B4: single combined file `{"vars":{...},"secrets":{...}}`
    /// — the platform cell-controller's atomic write target. One file
    /// is the only shape where `load` can never observe a mixed
    /// vars/secrets generation.
    combined_file: Option<PathBuf>,
}

impl FileRenderInputs {
    /// `None` for a file means "no such inputs" — an absent config
    /// never fails, a configured-but-broken file always does.
    pub fn new(vars_file: Option<PathBuf>, secrets_file: Option<PathBuf>) -> Self {
        Self {
            vars_file,
            secrets_file,
            combined_file: None,
        }
    }

    /// Combined-file mode (platform cells). Takes precedence in
    /// `render_inputs_provider` when `renderInputsFile` is configured.
    pub fn new_combined(path: PathBuf) -> Self {
        Self {
            vars_file: None,
            secrets_file: None,
            combined_file: Some(path),
        }
    }
}

fn read_secure_json(path: &PathBuf) -> Result<serde_json::Value, RenderInputsError> {
    let meta = std::fs::metadata(path).map_err(|_| RenderInputsError::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o777 != 0o600 {
            return Err(RenderInputsError::Permissions);
        }
    }
    if meta.len() > MAX_INPUT_FILE_BYTES {
        return Err(RenderInputsError::TooLarge);
    }
    let raw = std::fs::read(path).map_err(|_| RenderInputsError::Io)?;
    serde_json::from_slice(&raw).map_err(|_| RenderInputsError::Parse)
}

impl RenderInputsProvider for FileRenderInputs {
    fn load(&self) -> Result<RenderInputs, RenderInputsError> {
        if let Some(path) = &self.combined_file {
            let doc: serde_json::Value = read_secure_json(path)?;
            let vars: BTreeMap<String, BTreeMap<String, String>> =
                serde_json::from_value(doc["vars"].clone())
                    .map_err(|_| RenderInputsError::Parse)?;
            let raw: BTreeMap<String, BTreeMap<String, String>> =
                serde_json::from_value(doc["secrets"].clone())
                    .map_err(|_| RenderInputsError::Parse)?;
            let secrets = raw
                .into_iter()
                .map(|(t, m)| {
                    (
                        t,
                        m.into_iter()
                            .map(|(k, v)| (k, SecretValue::new(v)))
                            .collect(),
                    )
                })
                .collect();
            return Ok(RenderInputs { vars, secrets });
        }
        // vars.json is tenant-scoped too (`{tenantId: {varRef: value}}`,
        // same shape as the combined file's "vars") — the standalone
        // writer wraps its single-tenant union itself.
        let vars: BTreeMap<String, BTreeMap<String, String>> = match &self.vars_file {
            None => BTreeMap::new(),
            Some(path) => serde_json::from_value(read_secure_json(path)?)
                .map_err(|_| RenderInputsError::Parse)?,
        };
        let secrets: BTreeMap<String, BTreeMap<String, SecretValue>> = match &self.secrets_file {
            None => BTreeMap::new(),
            Some(path) => {
                let raw: BTreeMap<String, BTreeMap<String, String>> =
                    serde_json::from_value(read_secure_json(path)?)
                        .map_err(|_| RenderInputsError::Parse)?;
                raw.into_iter()
                    .map(|(t, m)| {
                        (
                            t,
                            m.into_iter()
                                .map(|(k, v)| (k, SecretValue::new(v)))
                                .collect(),
                        )
                    })
                    .collect()
            }
        };
        Ok(RenderInputs { vars, secrets })
    }
}

/// Fingerprint material for loaded inputs — mixed into the assembler's
/// params fingerprint so ANY value change produces a new expected spec.
/// Secret values enter only via a keyed hash (`pepper`): the digest
/// changes with the value but can never reveal it.
pub fn inputs_material(inputs: &RenderInputs, pepper: &[u8; 32], material: &mut Vec<u8>) {
    use sha2::Digest;
    for (t, map) in &inputs.vars {
        for (k, v) in map {
            material.push(0);
            material.extend_from_slice(t.as_bytes());
            material.push(0);
            material.extend_from_slice(k.as_bytes());
            material.push(0);
            material.extend_from_slice(v.as_bytes());
        }
    }
    for (t, map) in &inputs.secrets {
        for (k, v) in map {
            let mut h = sha2::Sha256::new();
            h.update(pepper);
            h.update(t.as_bytes());
            h.update([0]);
            h.update(k.as_bytes());
            h.update([0]);
            h.update(v.expose().as_bytes());
            material.push(1);
            material.extend_from_slice(&h.finalize());
        }
    }
}

/// Convenience for tests/standalone: static inputs baked at startup
/// (used where hot reload is deliberately not wanted).
#[derive(Debug)]
pub struct StaticRenderInputs(pub RenderInputs);

impl RenderInputsProvider for StaticRenderInputs {
    fn load(&self) -> Result<RenderInputs, RenderInputsError> {
        Ok(self.0.clone())
    }
}
