//! Local Ed25519 key management. The private key is `0600` and
//! owner-only; any permission/ownership regression fails closed.

use crate::LocalError;
use snapshot_protocol::{SigningKey, TrustedKey};
use std::path::Path;

fn key_path(root: &Path) -> std::path::PathBuf {
    crate::layout::keys_dir(root).join("signing.seed")
}
fn trusted_path(root: &Path) -> std::path::PathBuf {
    crate::layout::keys_dir(root).join("trusted.json")
}

/// Persist a fresh key as its 32-byte seed (`0600`). The seed fully
/// determines the Ed25519 pair — it is exactly as sensitive as the
/// pair and gets the same permission/ownership treatment.
pub fn write_signing_key(root: &Path, seed: &[u8; 32]) -> Result<(), LocalError> {
    vrt_securefs::atomic_write(&key_path(root), seed, 0o600)
        .map_err(|_| LocalError::Layout("key write"))
}

pub fn write_trusted_key(root: &Path, key: &TrustedKey) -> Result<(), LocalError> {
    let doc = serde_json::json!({"keyId": key.key_id, "publicKey": key.public_key});
    vrt_securefs::atomic_write(
        &trusted_path(root),
        serde_json::to_string_pretty(&doc)
            .unwrap_or_default()
            .as_bytes(),
        0o644,
    )
    .map_err(|_| LocalError::Layout("trusted write"))
}

/// Load + permission-check the signing key. `0600` exact, owned by
/// the effective uid — anything else refuses to start.
pub fn load_signing_key(root: &Path) -> Result<SigningKey, LocalError> {
    let path = key_path(root);
    let meta = std::fs::metadata(&path).map_err(|_| LocalError::Layout("key missing"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o777 != 0o600 {
            return Err(LocalError::Layout("key mode"));
        }
// SAFETY: geteuid(2) has no preconditions and cannot fail.
        let euid = unsafe { libc::geteuid() };
        if meta.uid() != euid {
            return Err(LocalError::Layout("key owner"));
        }
    }
    let bytes = std::fs::read(&path).map_err(|_| LocalError::Layout("key read"))?;
    let seed: &[u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| LocalError::Layout("key length"))?;
    Ok(SigningKey::from_seed(seed)?)
}

/// P3c-1: raw 32-byte signing seed -- the HMAC pepper for DO `uniqueKey`
/// (the seed is key material; permission/ownership checks follow the same path as [`load_signing_key`]).
pub fn load_signing_seed(root: &Path) -> Result<[u8; 32], LocalError> {
    // Reuse all the permission/ownership/length checks of SigningKey loading, then read the seed directly.
    load_signing_key(root)?;
    let bytes = std::fs::read(key_path(root)).map_err(|_| LocalError::Layout("key read"))?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| LocalError::Layout("key length"))
}

/// PREVIEW ACCESS: derived HMAC key file (`keys/preview-token.secret`,
/// `0600`). `vrt-host` reads this path via `[preview] tokenSecretFile`;
/// the control plane derives the authoritative value from the signing
/// seed, so the file is a distribution artifact, not independent key
/// material.
pub fn preview_token_secret_path(root: &Path) -> std::path::PathBuf {
    crate::layout::keys_dir(root).join("preview-token.secret")
}

/// Derive and PERSIST the preview-token key (idempotent).
///
/// The file is rewritten when it is missing or does not match the
/// key derived from the signing seed (a stale copy would make every
/// freshly minted URL fail verification, so self-healing here is the
/// fail-closed direction: the host's verifier and the control plane's
/// minter converge on the same value). Callers that hold the writer
/// lock (deploy, prune) may call this; display-only paths use
/// [`read_preview_token_secret`].
pub fn ensure_preview_token_secret(
    root: &Path,
) -> Result<snapshot_protocol::preview_token::PreviewTokenKey, LocalError> {
    use snapshot_protocol::preview_token::PreviewTokenKey;
    let seed = load_signing_seed(root)?;
    let key = PreviewTokenKey::from_signing_seed(&seed);
    let want = key.to_hex();
    let path = preview_token_secret_path(root);
    let matches = std::fs::read_to_string(&path)
        .map(|existing| existing.trim().eq_ignore_ascii_case(&want))
        .unwrap_or(false);
    if !matches {
        vrt_securefs::atomic_write(&path, want.as_bytes(), 0o600)
            .map_err(|_| LocalError::Layout("preview token write"))?;
    }
    Ok(key)
}

/// Read the preview-token key WITHOUT writing.
///
/// `None` when the file is missing or does not match the derived key —
/// display paths (`previewUrl`, `of versions list`) must not advertise a
/// URL the host would refuse.
pub fn read_preview_token_secret(
    root: &Path,
) -> Option<snapshot_protocol::preview_token::PreviewTokenKey> {
    use snapshot_protocol::preview_token::PreviewTokenKey;
    let seed = load_signing_seed(root).ok()?;
    let key = PreviewTokenKey::from_signing_seed(&seed);
    let existing = std::fs::read_to_string(preview_token_secret_path(root)).ok()?;
    existing
        .trim()
        .eq_ignore_ascii_case(&key.to_hex())
        .then_some(key)
}

/// Load `keys/trusted.json` — the local verifier record used by
/// `read_verified_desired` (cron + queue dispatch). `keyId` must equal
/// the id DERIVED from `publicKey` — a tampered record fails closed.
pub fn load_trusted_key(root: &Path) -> Result<TrustedKey, LocalError> {
    let raw = std::fs::read(trusted_path(root)).map_err(|_| LocalError::Layout("trusted read"))?;
    let doc: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|_| LocalError::Layout("trusted malformed"))?;
    let public_key = doc["publicKey"]
        .as_str()
        .ok_or(LocalError::Layout("trusted publicKey"))?;
    let key = TrustedKey::from_public_key(public_key)
        .map_err(|_| LocalError::Layout("trusted publicKey"))?;
    let claimed = doc["keyId"]
        .as_str()
        .ok_or(LocalError::Layout("trusted keyId"))?;
    if claimed != key.key_id {
        return Err(LocalError::Layout("trusted keyId mismatch"));
    }
    Ok(key)
}

/// Management-API bearer token path (`mgmt/token`, 0600).
pub fn mgmt_token_path(root: &std::path::Path) -> std::path::PathBuf {
    crate::layout::mgmt_dir(root).join("token")
}

/// Generate and store the management token (hex, 32 random bytes).
pub fn write_mgmt_token(root: &std::path::Path) -> Result<(), LocalError> {
    use ring::rand::SecureRandom;
    let rng = ring::rand::SystemRandom::new();
    let mut raw = [0u8; 32];
    rng.fill(&mut raw)
        .map_err(|_| LocalError::Layout("token rng"))?;
    vrt_securefs::atomic_write(&mgmt_token_path(root), hex::encode(raw).as_bytes(), 0o600)
        .map_err(|_| LocalError::Layout("token write"))
}

/// Load the management token; missing/insecure/malformed → fail closed.
pub fn load_mgmt_token(root: &std::path::Path) -> Result<Vec<u8>, LocalError> {
    load_token_file(&mgmt_token_path(root))
}

/// Load a bearer-token file at an arbitrary path — the TCP
/// management listener uses a SEPARATE token (`--token-file`) from
/// the unix socket's `mgmt/token` so an exposed TCP credential never
/// doubles as the local one. Same rules either way: `0600`,
/// owner-only, exactly 64 lowercase-or-uppercase hex chars —
/// anything else fails closed.
pub fn load_token_file(path: &std::path::Path) -> Result<Vec<u8>, LocalError> {
    let meta = std::fs::metadata(path).map_err(|_| LocalError::Layout("token missing"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
// SAFETY: geteuid(2) has no preconditions and cannot fail.
        if meta.mode() & 0o777 != 0o600 || meta.uid() != unsafe { libc::geteuid() } {
            return Err(LocalError::Layout("token mode"));
        }
    }
    let raw = std::fs::read(path)?;
    if raw.len() != 64 || !raw.iter().all(|b| b.is_ascii_hexdigit()) {
        return Err(LocalError::Layout("token malformed"));
    }
    Ok(raw)
}

/// Provider list-cursor secret path (`keys/cursor.secret`, 0600) —
/// T25/L8. Read by vrt-provider via its `cursorSecretFile` setting;
/// never an env var or compose plaintext.
pub fn cursor_secret_path(root: &std::path::Path) -> std::path::PathBuf {
    crate::layout::keys_dir(root).join("cursor.secret")
}

/// Generate and store the cursor secret (hex, 32 random bytes — same
/// treatment as the management token).
pub fn write_cursor_secret(root: &std::path::Path) -> Result<(), LocalError> {
    use ring::rand::SecureRandom;
    let rng = ring::rand::SystemRandom::new();
    let mut raw = [0u8; 32];
    rng.fill(&mut raw)
        .map_err(|_| LocalError::Layout("cursor rng"))?;
    vrt_securefs::atomic_write(
        &cursor_secret_path(root),
        hex::encode(raw).as_bytes(),
        0o600,
    )
    .map_err(|_| LocalError::Layout("cursor write"))
}

/// Provider<->vrt-sql service-to-service auth secret (`keys/sql-auth.secret`, 0600) --
/// T34. Both ends share the same file; if it is not 0600 both sides fail closed.
pub fn sql_auth_secret_path(root: &std::path::Path) -> std::path::PathBuf {
    crate::layout::keys_dir(root).join("sql-auth.secret")
}

/// vrt-sql bookmark HMAC key (`keys/sql-bookmark.secret`, 0600).
pub fn sql_bookmark_secret_path(root: &std::path::Path) -> std::path::PathBuf {
    crate::layout::keys_dir(root).join("sql-bookmark.secret")
}

/// Generate both vrt-sql secrets (hex, 32 random bytes each).
pub fn write_sql_secrets(root: &std::path::Path) -> Result<(), LocalError> {
    use ring::rand::SecureRandom;
    let rng = ring::rand::SystemRandom::new();
    for path in [sql_auth_secret_path(root), sql_bookmark_secret_path(root)] {
        let mut raw = [0u8; 32];
        rng.fill(&mut raw)
            .map_err(|_| LocalError::Layout("sql secret rng"))?;
        vrt_securefs::atomic_write(&path, hex::encode(raw).as_bytes(), 0o600)
            .map_err(|_| LocalError::Layout("sql secret write"))?;
    }
    Ok(())
}
