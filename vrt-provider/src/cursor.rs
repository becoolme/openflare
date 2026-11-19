//! Signed opaque list cursors. Two formats mirror the upstream shapes:
//!
//! - KV: binary `[v(1) | scopeHash(32) | prefixHash(32) | issuedAtMs(8) |
//!   expiresAtMs(8) | keyLen(2) | lastKey | sig(32)]`, base64url-encoded,
//!   hard size cap 4096 decoded bytes.
//! - R2: `base64url(json) "." base64url(hmac)` where the JSON carries
//!   `{v, resourceId, generation, prefixSha256, delimiterSha256,
//!   includeMask, startAfterSha256, afterKey, expiresAtMs}` — matching the
//!   upstream `R2CursorPayload` field names.
//!
//! `scopeHash`/`resourceId` bind the cursor to `(tenant, namespace)` /
//! `(tenant, bucket)`; tampering or cross-scope replay fails closed.

use crate::limits::CURSOR_TTL_MS;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const KV_CURSOR_VERSION: u8 = 1;
const KV_CURSOR_MAX_BYTES: usize = 4096;
const R2_CURSOR_VERSION: u8 = 1;

/// Signer/verifier for opaque list cursors. `HMAC-SHA256` keyed from the
/// configured cursor secret (hashed once into a 32-byte key).
#[derive(Clone)]
pub struct CursorSigner {
    key: [u8; 32],
}

impl std::fmt::Debug for CursorSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CursorSigner").finish_non_exhaustive()
    }
}

fn sha256_hex(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
        hasher.update([0u8]);
    }
    hex::encode(hasher.finalize())
}

fn sha256_bytes(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
        hasher.update([0u8]);
    }
    hasher.finalize().into()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Error type for cursor verification — the caller maps it to the
/// product-family cursor error (`KV_CURSOR_INVALID`/`R2_CURSOR_INVALID`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorInvalid;

impl CursorSigner {
    /// Derive the signing key from raw secret material.
    pub fn new(secret: &[u8]) -> Self {
        Self {
            key: sha256_bytes(&[b"vrt-cursor-v1", secret]),
        }
    }

    fn mac(&self, payload: &[u8]) -> [u8; 32] {
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(payload);
        mac.finalize().into_bytes().into()
    }

    fn verify_mac(&self, payload: &[u8], sig: &[u8]) -> Result<(), CursorInvalid> {
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(payload);
        mac.verify_slice(sig).map_err(|_| CursorInvalid)
    }

    /// KV scope hash binding `(tenant, namespace)`.
    fn kv_scope(tenant: &str, namespace: &str) -> [u8; 32] {
        sha256_bytes(&[
            b"vrt-kv-cursor-scope",
            tenant.as_bytes(),
            namespace.as_bytes(),
        ])
    }

    /// Issue a KV cursor that resumes after `last_key`.
    pub fn sign_kv(&self, tenant: &str, namespace: &str, prefix: &[u8], last_key: &[u8]) -> String {
        let issued = now_ms();
        self.sign_kv_with_times(
            tenant,
            namespace,
            prefix,
            last_key,
            issued,
            issued + CURSOR_TTL_MS,
        )
    }

    /// Issue a KV cursor with explicit issued/expires times.
    #[doc(hidden)]
    pub fn sign_kv_with_times(
        &self,
        tenant: &str,
        namespace: &str,
        prefix: &[u8],
        last_key: &[u8],
        issued: i64,
        expires: i64,
    ) -> String {
        let mut payload = Vec::with_capacity(1 + 32 + 32 + 8 + 8 + 2 + last_key.len());
        payload.push(KV_CURSOR_VERSION);
        payload.extend_from_slice(&Self::kv_scope(tenant, namespace));
        payload.extend_from_slice(&sha256_bytes(&[b"vrt-kv-cursor-prefix", prefix]));
        payload.extend_from_slice(&issued.to_be_bytes());
        payload.extend_from_slice(&expires.to_be_bytes());
        payload.extend_from_slice(&(last_key.len() as u16).to_be_bytes());
        payload.extend_from_slice(last_key);
        let sig = self.mac(&payload);
        payload.extend_from_slice(&sig);
        URL_SAFE_NO_PAD.encode(payload)
    }

    /// Verify a KV cursor for `(tenant, namespace, prefix)`; returns the
    /// `startAfter` key bytes on success.
    pub fn verify_kv(
        &self,
        tenant: &str,
        namespace: &str,
        prefix: &[u8],
        cursor: &str,
    ) -> Result<Vec<u8>, CursorInvalid> {
        let raw = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| CursorInvalid)?;
        if raw.is_empty()
            || raw.len() > KV_CURSOR_MAX_BYTES
            || raw.len() < 1 + 32 + 32 + 8 + 8 + 2 + 32
        {
            return Err(CursorInvalid);
        }
        let (payload, sig) = raw.split_at(raw.len() - 32);
        self.verify_mac(payload, sig)?;
        if payload[0] != KV_CURSOR_VERSION {
            return Err(CursorInvalid);
        }
        if payload[1..33] != Self::kv_scope(tenant, namespace) {
            return Err(CursorInvalid);
        }
        if payload[33..65] != sha256_bytes(&[b"vrt-kv-cursor-prefix", prefix]) {
            return Err(CursorInvalid);
        }
        let expires = i64::from_be_bytes(payload[73..81].try_into().unwrap());
        if now_ms() > expires {
            return Err(CursorInvalid);
        }
        let key_len = u16::from_be_bytes(payload[81..83].try_into().unwrap()) as usize;
        if payload.len() != 83 + key_len {
            return Err(CursorInvalid);
        }
        Ok(payload[83..].to_vec())
    }

    /// R2 resource binding — opaque sha256 over `(tenant, bucket)` so the
    /// JSON carries no raw identifiers.
    pub fn r2_resource_id(tenant: &str, bucket: &str) -> String {
        sha256_hex(&[b"vrt-r2-cursor-scope", tenant.as_bytes(), bucket.as_bytes()])
    }

    /// Issue an R2 cursor. `payload.resource_id`, `generation`, digests and
    /// `expires_at_ms` must be filled by the caller — see
    /// [`crate::r2::wire::R2CursorPayload`].
    pub fn sign_r2(
        &self,
        payload: &crate::r2::wire::R2CursorPayload,
    ) -> Result<String, serde_json::Error> {
        let json = serde_json::to_vec(payload)?;
        let sig = self.mac(&json);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(json),
            URL_SAFE_NO_PAD.encode(sig)
        ))
    }

    /// Verify an R2 cursor's signature, version, expiry. Scope/params
    /// equality is the caller's job (they know the request params).
    pub fn verify_r2(
        &self,
        cursor: &str,
    ) -> Result<crate::r2::wire::R2CursorPayload, CursorInvalid> {
        let (json_b64, sig_b64) = cursor.split_once('.').ok_or(CursorInvalid)?;
        let json = URL_SAFE_NO_PAD
            .decode(json_b64)
            .map_err(|_| CursorInvalid)?;
        let sig = URL_SAFE_NO_PAD.decode(sig_b64).map_err(|_| CursorInvalid)?;
        if json.len() > R2_CURSOR_JSON_MAX || sig.len() != 32 {
            return Err(CursorInvalid);
        }
        self.verify_mac(&json, &sig)?;
        let payload: crate::r2::wire::R2CursorPayload =
            serde_json::from_slice(&json).map_err(|_| CursorInvalid)?;
        if payload.v != R2_CURSOR_VERSION || now_ms() > payload.expires_at_ms {
            return Err(CursorInvalid);
        }
        Ok(payload)
    }
}

const R2_CURSOR_JSON_MAX: usize = 8192;

// ---- W5: signed multipart upload ids ------------------------------------
//
// Tenant-facing `uploadId` is an opaque token issued by the provider at
// `createMultipartUpload`. Binary layout mirrors the KV cursor:
// `[v(1) | scope(32) | keyHash(32) | issuedAtMs(8) | expiresAtMs(8) |
//   idLen(2) | internalId | sig(32)]`, base64url-encoded.
// `scope` binds (tenant, bucket), `keyHash` binds the object key, the
// payload carries the backend-internal upload id (S3 UploadId, fs
// staging id, memory id) plus issue/expiry — so a token replayed for a
// different tenant, bucket, or key fails closed before the backend sees
// it. Raw tokens are never logged.

const UPLOAD_ID_VERSION: u8 = 1;
/// S3 UploadIds can run a few hundred bytes; 1 KiB is generous.
const UPLOAD_ID_MAX_INTERNAL: usize = 1024;
const UPLOAD_ID_MAX_BYTES: usize = 1 + 32 + 32 + 8 + 8 + 2 + UPLOAD_ID_MAX_INTERNAL + 32;

impl CursorSigner {
    /// Scope hash binding `(tenant, bucket)` for upload tokens.
    fn upload_scope(tenant: &str, bucket: &str) -> [u8; 32] {
        sha256_bytes(&[b"vrt-r2-upload-scope", tenant.as_bytes(), bucket.as_bytes()])
    }

    /// Key hash binding the exact object key.
    fn upload_key_hash(key: &str) -> [u8; 32] {
        sha256_bytes(&[b"vrt-r2-upload-key", key.as_bytes()])
    }

    /// Issue an upload token wrapping `internal_id` with `ttl_ms`
    /// lifetime; the token expires with the upload it names.
    pub fn sign_r2_upload(
        &self,
        tenant: &str,
        bucket: &str,
        key: &str,
        internal_id: &[u8],
        ttl_ms: i64,
    ) -> String {
        let issued = now_ms();
        self.sign_r2_upload_with_times(
            tenant,
            bucket,
            key,
            internal_id,
            issued,
            issued.saturating_add(ttl_ms),
        )
    }

    /// Issue an upload token whose expiry is decided by the backend
    /// (`MultipartCreateResult.expires_at_ms`) rather than a fixed TTL —
    /// fs staging TTL is configurable, so the token must follow the
    /// provider, not a compile-time constant. A past/zero expiry clamps
    /// to `issued` (token instantly invalid → fails closed on next op).
    pub fn sign_r2_upload_until(
        &self,
        tenant: &str,
        bucket: &str,
        key: &str,
        internal_id: &[u8],
        expires_at_ms: i64,
    ) -> String {
        let issued = now_ms();
        self.sign_r2_upload_with_times(
            tenant,
            bucket,
            key,
            internal_id,
            issued,
            expires_at_ms.max(issued),
        )
    }

    /// Issue an upload token with explicit times (tests).
    #[doc(hidden)]
    pub fn sign_r2_upload_with_times(
        &self,
        tenant: &str,
        bucket: &str,
        key: &str,
        internal_id: &[u8],
        issued: i64,
        expires: i64,
    ) -> String {
        let id_len = internal_id.len().min(UPLOAD_ID_MAX_INTERNAL);
        let mut payload = Vec::with_capacity(83 + id_len);
        payload.push(UPLOAD_ID_VERSION);
        payload.extend_from_slice(&Self::upload_scope(tenant, bucket));
        payload.extend_from_slice(&Self::upload_key_hash(key));
        payload.extend_from_slice(&issued.to_be_bytes());
        payload.extend_from_slice(&expires.to_be_bytes());
        payload.extend_from_slice(&(id_len as u16).to_be_bytes());
        payload.extend_from_slice(&internal_id[..id_len]);
        let sig = self.mac(&payload);
        payload.extend_from_slice(&sig);
        URL_SAFE_NO_PAD.encode(payload)
    }

    /// Verify an upload token for `(tenant, bucket, key)`; returns the
    /// backend-internal upload id on success. Any malformed, tampered,
    /// expired, or cross-scope token fails closed.
    pub fn verify_r2_upload(
        &self,
        tenant: &str,
        bucket: &str,
        key: &str,
        token: &str,
    ) -> Result<Vec<u8>, CursorInvalid> {
        let raw = URL_SAFE_NO_PAD.decode(token).map_err(|_| CursorInvalid)?;
        if raw.len() > UPLOAD_ID_MAX_BYTES || raw.len() < 1 + 32 + 32 + 8 + 8 + 2 + 1 + 32 {
            return Err(CursorInvalid);
        }
        let (payload, sig) = raw.split_at(raw.len() - 32);
        self.verify_mac(payload, sig)?;
        if payload[0] != UPLOAD_ID_VERSION {
            return Err(CursorInvalid);
        }
        if payload[1..33] != Self::upload_scope(tenant, bucket) {
            return Err(CursorInvalid);
        }
        if payload[33..65] != Self::upload_key_hash(key) {
            return Err(CursorInvalid);
        }
        let expires = i64::from_be_bytes(payload[73..81].try_into().unwrap());
        if now_ms() > expires {
            return Err(CursorInvalid);
        }
        let id_len = u16::from_be_bytes(payload[81..83].try_into().unwrap()) as usize;
        if id_len == 0 || payload.len() != 83 + id_len {
            return Err(CursorInvalid);
        }
        Ok(payload[83..].to_vec())
    }
}
