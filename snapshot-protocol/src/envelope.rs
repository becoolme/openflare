//! Signed snapshot envelope: signing, verification, and revision fencing.
//!
//! The cryptographic and canonicalization semantics are an equivalent
//! reimplementation of `postgres-tenant-gateway/src/config_protocol.rs`
//! (`verify_snapshot`, `verify_signature`, `decode_canonical_base64`,
//! `parse_protocol_time`, `is_lower_hex`), generalized from the gateway's
//! `channel`/`clusterId` binding to a generic `kind`/`scope` pair:
//!
//! - **Signed bytes**: canonical JSON of the envelope object with the
//!   `signature` member removed (same as the gateway).
//! - **Payload digest**: lowercase-hex sha256 of the canonical payload
//!   member (same as the gateway).
//! - **Key id**: first 24 hex chars of sha256 over the *base64 SPKI text*
//!   (not the decoded DER — same as the gateway and its TypeScript signer).
//! - **Signature**: Ed25519 (ring `ED25519`), 64 raw bytes, canonical
//!   base64 (re-encoding must reproduce the input).
//! - **Time**: `issuedAt`/`expiresAt` must be canonical
//!   `YYYY-MM-DDTHH:MM:SS.mmmZ`; lifetime within [60s, 24h]; `issuedAt` at
//!   most 30s in the future; `expiresAt` strictly after now.
//! - **Revision**: `1..=2^53-1`, monotone per `(kind, scope)` stream via
//!   [`RevisionGuard`]; a repeated revision must carry identical signed
//!   envelope bytes or it is a conflict.
//!
//! All checks fail closed: any mismatch returns a typed [`SnapshotError`];
//! nothing falls back to unsigned or best-effort parsing.

use crate::error::SnapshotError;
use crate::strict::{StrictValue, MAX_SAFE_INTEGER};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, SecondsFormat, Utc};
use ring::signature::{Ed25519KeyPair, KeyPair as _, UnparsedPublicKey, ED25519};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::Duration;

/// Envelope `version` produced and accepted by this implementation.
pub const PROTOCOL_VERSION: u8 = 1;
/// Envelope `canonicalVersion` produced and accepted by this implementation.
pub const CANONICAL_VERSION: u8 = 1;
/// Maximum wire size of one signed snapshot document.
pub const MAX_SNAPSHOT_BYTES: usize = 512 * 1024;
/// Minimum accepted `expiresAt - issuedAt`, identical to the gateway.
pub const MIN_TTL: Duration = Duration::from_secs(60);
/// Maximum accepted `expiresAt - issuedAt`, identical to the gateway.
pub const MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Accepted forward clock skew on `issuedAt`, identical to the gateway.
pub const CLOCK_SKEW: Duration = Duration::from_secs(30);

const MIN_TTL_MS: i64 = MIN_TTL.as_millis() as i64;
const MAX_TTL_MS: i64 = MAX_TTL.as_millis() as i64;
const CLOCK_SKEW_MS: i64 = CLOCK_SKEW.as_millis() as i64;

/// DER SPKI prefix for an Ed25519 public key (same constant as the gateway's
/// `V2_PUBLIC_KEY_SPKI_PREFIX`).
const ED25519_SPKI_PREFIX: &[u8] = &[
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// Members of the signed envelope object; anything else is rejected.
const ENVELOPE_MEMBERS: &[&str] = &[
    "version",
    "canonicalVersion",
    "kind",
    "scope",
    "revision",
    "issuedAt",
    "expiresAt",
    "keyId",
    "payloadDigest",
    "payload",
    "signature",
];

/// A trusted Ed25519 public key in control-plane format.
///
/// `public_key` is canonical base64 of the DER SPKI (12-byte Ed25519 prefix
/// + 32-byte key). `key_id` must equal the first 24 hex chars of sha256 over
/// the `public_key` *string bytes* — the same binding the gateway enforces,
/// so a key record whose id was copied inconsistently fails closed instead
/// of silently trusting an unbound key.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TrustedKey {
    pub key_id: String,
    pub public_key: String,
}

impl TrustedKey {
    /// Build a trusted key record from a canonical-base64 SPKI public key,
    /// deriving the protocol key id. Rejects malformed input.
    pub fn from_public_key(public_key: impl Into<String>) -> Result<Self, SnapshotError> {
        let public_key = public_key.into();
        raw_ed25519_from_spki_b64(&public_key).ok_or(SnapshotError::KeyMaterial)?;
        let key_id = derive_key_id(&public_key);
        Ok(Self { key_id, public_key })
    }
}

/// A local Ed25519 signing key. Wraps the ring key pair and caches the
/// SPKI/base64/key-id representation used on the wire.
pub struct SigningKey {
    pair: Ed25519KeyPair,
    public_key_b64: String,
    key_id: String,
}

impl SigningKey {
    /// Generate a fresh key pair from the OS CSPRNG.
    pub fn generate() -> Result<Self, SnapshotError> {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| SnapshotError::KeyMaterial)?;
        Self::from_pkcs8(pkcs8.as_ref())
    }

    /// Deterministically derive a key pair from a 32-byte seed. Intended for
    /// tests and for control-plane key management that keeps seeds in an
    /// HSM/KMS; never use a fixed seed in production wiring.
    pub fn from_seed(seed: &[u8; 32]) -> Result<Self, SnapshotError> {
        let pair =
            Ed25519KeyPair::from_seed_unchecked(seed).map_err(|_| SnapshotError::KeyMaterial)?;
        Ok(Self::from_pair(pair))
    }

    /// Load a key pair from a PKCS#8 v2 document (as produced by
    /// [`SigningKey::generate`] or a KMS export).
    pub fn from_pkcs8(pkcs8: &[u8]) -> Result<Self, SnapshotError> {
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| SnapshotError::KeyMaterial)?;
        Ok(Self::from_pair(pair))
    }

    fn from_pair(pair: Ed25519KeyPair) -> Self {
        let mut spki = Vec::with_capacity(ED25519_SPKI_PREFIX.len() + 32);
        spki.extend_from_slice(ED25519_SPKI_PREFIX);
        spki.extend_from_slice(pair.public_key().as_ref());
        let public_key_b64 = STANDARD.encode(&spki);
        let key_id = derive_key_id(&public_key_b64);
        Self {
            pair,
            public_key_b64,
            key_id,
        }
    }

    /// The protocol key id (first 24 hex chars of sha256 over the base64
    /// SPKI text), matching the control-plane derivation.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The matching [`TrustedKey`] record for verifiers.
    pub fn trusted_key(&self) -> TrustedKey {
        TrustedKey {
            key_id: self.key_id.clone(),
            public_key: self.public_key_b64.clone(),
        }
    }
}

/// Parameters for [`sign`]. `T` is the typed payload (e.g.
/// [`crate::compute_desired::ComputeDesired`]).
pub struct SignParams<'a, T: Serialize> {
    /// Snapshot kind, e.g. `compute.desired`. `[a-z0-9_.-]`, max 64 bytes.
    pub kind: &'a str,
    /// Audience scope binding, e.g. `cell:cell-01`. Identifier, max 128
    /// bytes; verifiers must supply the identical expected scope.
    pub scope: &'a str,
    /// Monotone revision for this `(kind, scope)` stream; `1..=2^53-1`.
    pub revision: u64,
    /// Issuance time; serialized canonically to millisecond precision.
    pub issued_at: DateTime<Utc>,
    /// Expiry time; `expires_at - issued_at` must be within
    /// `[MIN_TTL, MAX_TTL]`.
    pub expires_at: DateTime<Utc>,
    /// Typed payload to sign.
    pub payload: &'a T,
}

/// Output of [`sign`]: the canonical wire document plus extracted fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedSnapshot {
    /// Canonical JSON bytes of the full envelope (this exact byte string is
    /// the wire form; verifiers re-canonicalize after strict parsing, so
    /// equivalent documents with different layouts also verify).
    pub bytes: Vec<u8>,
    /// Lowercase-hex sha256 of the canonical payload member.
    pub payload_digest: String,
    /// Key id of the signing key.
    pub key_id: String,
    /// Base64 Ed25519 signature embedded in the envelope.
    pub signature: String,
    /// Revision carried by the envelope.
    pub revision: u64,
}

/// Sign `params.payload` into a canonical signed envelope.
///
/// Fails (rather than emitting an unusable document) when the revision or
/// lifetime bounds are violated, kind/scope are malformed, or the payload
/// cannot be represented under strict JSON rules.
pub fn sign<T: Serialize>(
    params: &SignParams<'_, T>,
    key: &SigningKey,
) -> Result<SignedSnapshot, SnapshotError> {
    validate_kind_scope(params.kind, params.scope)?;
    if params.revision == 0 || params.revision > MAX_SAFE_INTEGER {
        return Err(SnapshotError::InvalidRevision);
    }
    let lifetime = params
        .expires_at
        .timestamp_millis()
        .checked_sub(params.issued_at.timestamp_millis())
        .ok_or(SnapshotError::InvalidLifetime)?;
    if !(MIN_TTL_MS..=MAX_TTL_MS).contains(&lifetime) {
        return Err(SnapshotError::InvalidLifetime);
    }
    let payload_strict =
        StrictValue::from_serializable(params.payload).map_err(|_| SnapshotError::Serialization)?;
    let payload_digest = hex::encode(Sha256::digest(payload_strict.canonical().as_bytes()));

    let mut envelope = BTreeMap::new();
    let mut insert = |k: &str, v: StrictValue| {
        envelope.insert(k.to_string(), v);
    };
    insert("version", StrictValue::Unsigned(PROTOCOL_VERSION as u64));
    insert(
        "canonicalVersion",
        StrictValue::Unsigned(CANONICAL_VERSION as u64),
    );
    insert("kind", StrictValue::String(params.kind.to_string()));
    insert("scope", StrictValue::String(params.scope.to_string()));
    insert("revision", StrictValue::Unsigned(params.revision));
    insert(
        "issuedAt",
        StrictValue::String(format_protocol_time(&params.issued_at)),
    );
    insert(
        "expiresAt",
        StrictValue::String(format_protocol_time(&params.expires_at)),
    );
    insert("keyId", StrictValue::String(key.key_id.clone()));
    insert("payloadDigest", StrictValue::String(payload_digest.clone()));
    insert("payload", payload_strict);

    let unsigned = StrictValue::Object(envelope.clone());
    let signature = key.pair.sign(unsigned.canonical().as_bytes());
    let signature_b64 = STANDARD.encode(signature.as_ref());
    envelope.insert(
        "signature".to_string(),
        StrictValue::String(signature_b64.clone()),
    );

    let bytes = StrictValue::Object(envelope).canonical().into_bytes();
    Ok(SignedSnapshot {
        bytes,
        payload_digest,
        key_id: key.key_id.clone(),
        signature: signature_b64,
        revision: params.revision,
    })
}

/// The `(kind, scope)` pair a verifier accepts a snapshot for. An envelope
/// addressed to any other kind or scope is rejected before signature work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedScope<'a> {
    pub kind: &'a str,
    pub scope: &'a str,
}

/// A fully verified snapshot.
#[derive(Clone, Debug)]
pub struct Verified<T> {
    /// Typed payload decoded after signature verification.
    pub payload: T,
    /// Envelope `kind` (equals [`ExpectedScope::kind`]).
    pub kind: String,
    /// Envelope `scope` (equals [`ExpectedScope::scope`]).
    pub scope: String,
    /// Envelope revision.
    pub revision: u64,
    /// Key id that verified the signature.
    pub key_id: String,
    /// Lowercase-hex sha256 of the canonical payload member.
    pub payload_digest: String,
    /// Lowercase-hex sha256 of the canonical *full* envelope (incl.
    /// signature); identifies this exact signed document.
    pub envelope_digest: String,
    /// Parsed `issuedAt`.
    pub issued_at: DateTime<Utc>,
    /// Parsed `expiresAt`.
    pub expires_at: DateTime<Utc>,
    /// `expires_at - now` at verification time.
    pub remaining_ttl: Duration,
}

/// Durable monotone fence for one `(kind, scope)` snapshot stream.
///
/// Mirrors the gateway's `validate_watermark_advance` semantics adapted to
/// the generic envelope: a new snapshot must carry a revision strictly
/// greater than the last accepted one; the *same* revision is accepted only
/// when the signed envelope bytes are identical (idempotent re-delivery);
/// a different document at an already-accepted revision is a conflict.
///
/// [`verify`] deliberately does not take a guard: the fence must advance
/// only after the payload's *semantic* validation (e.g.
/// [`crate::compute_desired::ComputeDesired::validate`]) has also passed,
/// mirroring the gateway updating its watermark only for fully validated
/// snapshots. The canonical consumer sequence is:
///
/// ```text
/// let verified = verify::<T>(raw, &expected, &keys, now)?;
/// verified.payload.validate()?;              // payload-type-specific
/// guard.accept(verified.revision, &verified.envelope_digest)?;
/// ```
///
/// Callers persist `last_accepted()`/`last_envelope_digest()` after a
/// successful [`RevisionGuard::accept`] and restore with
/// [`RevisionGuard::restore`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RevisionGuard {
    last_revision: u64,
    last_envelope_digest: String,
}

impl RevisionGuard {
    /// A guard with no accepted revision yet (first snapshot wins the fence).
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild a guard from previously persisted watermark state. Returns
    /// `None` on malformed input rather than weakening the fence.
    pub fn restore(last_revision: u64, last_envelope_digest: &str) -> Option<Self> {
        if last_revision == 0
            || last_revision > MAX_SAFE_INTEGER
            || !is_lower_hex(last_envelope_digest, 64)
        {
            return None;
        }
        Some(Self {
            last_revision,
            last_envelope_digest: last_envelope_digest.to_string(),
        })
    }

    /// Last accepted revision; 0 when nothing has been accepted yet.
    pub fn last_accepted(&self) -> u64 {
        self.last_revision
    }

    /// Last accepted envelope digest; empty when nothing accepted.
    pub fn last_envelope_digest(&self) -> &str {
        &self.last_envelope_digest
    }

    /// Check the fence against a fully verified snapshot and, on success,
    /// record it as the new last-accepted revision. Call only after
    /// [`verify`] and all payload-level validation have passed; an accepted
    /// revision cannot be re-taken by a different document.
    ///
    /// - `revision < last` → [`SnapshotError::RevisionRollback`]
    /// - `revision == last` with identical envelope digest → idempotent
    ///   accept (re-delivery of the same signed document)
    /// - `revision == last` with a different digest →
    ///   [`SnapshotError::RevisionConflict`]
    /// - `revision > last` → advance (a pure renewal keeping the same
    ///   payload digest is a normal, allowed case)
    pub fn accept(&mut self, revision: u64, envelope_digest: &str) -> Result<(), SnapshotError> {
        if self.last_revision != 0 {
            if revision < self.last_revision {
                return Err(SnapshotError::RevisionRollback);
            }
            if revision == self.last_revision {
                if envelope_digest != self.last_envelope_digest {
                    return Err(SnapshotError::RevisionConflict);
                }
                return Ok(());
            }
        }
        self.last_revision = revision;
        self.last_envelope_digest = envelope_digest.to_string();
        Ok(())
    }
}

/// Parse and verify one signed snapshot document.
///
/// Steps, all fail-closed:
///
/// 1. size cap and strict JSON parse;
/// 2. envelope member set, version, `kind`/`scope` binding, revision range;
/// 3. canonical timestamps, lifetime bounds, clock skew, expiry;
/// 4. payload digest over canonical payload bytes;
/// 5. key-id resolution, trusted-key shape and id binding, Ed25519 over the
///    canonical unsigned envelope;
/// 6. typed payload decode (`deny_unknown_fields` applies inside `T`).
///
/// This function does NOT advance any revision fence: after it returns,
/// run the payload's semantic validation, then record acceptance via
/// [`RevisionGuard::accept`]. See the guard's documentation for the
/// canonical sequence.
pub fn verify<T: DeserializeOwned>(
    raw: &[u8],
    expected: &ExpectedScope<'_>,
    trusted_keys: &[TrustedKey],
    now: DateTime<Utc>,
) -> Result<Verified<T>, SnapshotError> {
    if raw.len() > MAX_SNAPSHOT_BYTES {
        return Err(SnapshotError::TooLarge);
    }
    let strict = StrictValue::parse(raw).map_err(|_| SnapshotError::InvalidJson)?;
    let object = strict.as_object().ok_or(SnapshotError::InvalidEnvelope)?;
    for member in ENVELOPE_MEMBERS {
        if !object.contains_key(*member) {
            return Err(SnapshotError::InvalidEnvelope);
        }
    }
    if object
        .keys()
        .any(|k| !ENVELOPE_MEMBERS.contains(&k.as_str()))
    {
        return Err(SnapshotError::InvalidEnvelope);
    }

    let version = member_u64(&strict, "version")?;
    let canonical_version = member_u64(&strict, "canonicalVersion")?;
    if version != PROTOCOL_VERSION as u64 || canonical_version != CANONICAL_VERSION as u64 {
        return Err(SnapshotError::UnsupportedVersion);
    }
    let kind = member_str(&strict, "kind")?;
    if kind != expected.kind {
        return Err(SnapshotError::KindMismatch);
    }
    let scope = member_str(&strict, "scope")?;
    if scope != expected.scope {
        return Err(SnapshotError::ScopeMismatch);
    }
    let revision = member_u64(&strict, "revision")?;
    if revision == 0 || revision > MAX_SAFE_INTEGER {
        return Err(SnapshotError::InvalidRevision);
    }

    let issued_at = member_str(&strict, "issuedAt")
        .ok()
        .and_then(parse_protocol_time)
        .ok_or(SnapshotError::InvalidTimestamp)?;
    let expires_at = member_str(&strict, "expiresAt")
        .ok()
        .and_then(parse_protocol_time)
        .ok_or(SnapshotError::InvalidTimestamp)?;
    let lifetime = expires_at
        .timestamp_millis()
        .checked_sub(issued_at.timestamp_millis())
        .ok_or(SnapshotError::InvalidLifetime)?;
    if !(MIN_TTL_MS..=MAX_TTL_MS).contains(&lifetime) {
        return Err(SnapshotError::InvalidLifetime);
    }
    if issued_at.timestamp_millis() > now.timestamp_millis() + CLOCK_SKEW_MS {
        return Err(SnapshotError::NotYetValid);
    }
    if expires_at <= now {
        return Err(SnapshotError::Expired);
    }

    let payload_strict = strict
        .get("payload")
        .ok_or(SnapshotError::InvalidEnvelope)?;
    let payload_digest = hex::encode(Sha256::digest(payload_strict.canonical().as_bytes()));
    let claimed_digest = member_str(&strict, "payloadDigest")?;
    if !is_lower_hex(claimed_digest, 64) || claimed_digest != payload_digest {
        return Err(SnapshotError::DigestMismatch);
    }

    let signature = member_str(&strict, "signature")?;
    let key_id = member_str(&strict, "keyId")?;
    verify_signature(&strict, signature, key_id, trusted_keys)?;

    let payload: T = serde_json::from_value(payload_strict.to_json_value())
        .map_err(|_| SnapshotError::PayloadInvalid)?;

    let envelope_digest = hex::encode(Sha256::digest(strict.canonical().as_bytes()));

    Ok(Verified {
        payload,
        kind: kind.to_string(),
        scope: scope.to_string(),
        revision,
        key_id: key_id.to_string(),
        payload_digest,
        envelope_digest,
        issued_at,
        expires_at,
        remaining_ttl: expires_at
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::ZERO),
    })
}

fn member_str<'a>(strict: &'a StrictValue, key: &str) -> Result<&'a str, SnapshotError> {
    strict
        .get(key)
        .and_then(StrictValue::as_str)
        .ok_or(SnapshotError::InvalidEnvelope)
}

fn member_u64(strict: &StrictValue, key: &str) -> Result<u64, SnapshotError> {
    strict
        .get(key)
        .and_then(StrictValue::as_u64)
        .ok_or(SnapshotError::InvalidEnvelope)
}

/// Ed25519 verification over the canonical envelope with `signature`
/// removed. Split into the same steps the gateway's `verify_signature`
/// performs so the gateway fixture exercises this exact code path in tests.
fn verify_signature(
    strict_envelope: &StrictValue,
    signature_text: &str,
    key_id: &str,
    keys: &[TrustedKey],
) -> Result<(), SnapshotError> {
    let key = keys
        .iter()
        .find(|key| key.key_id == key_id)
        .ok_or(SnapshotError::UnknownKeyId)?;
    let raw_key =
        raw_ed25519_from_spki_b64(&key.public_key).ok_or(SnapshotError::InvalidTrustedKey)?;
    // The control-plane helper hashes the base64 SPKI representation, not
    // the decoded DER bytes.
    if derive_key_id(&key.public_key) != key_id {
        return Err(SnapshotError::InvalidTrustedKey);
    }
    let signature =
        decode_canonical_base64(signature_text).ok_or(SnapshotError::SignatureInvalid)?;
    if signature.len() != 64 {
        return Err(SnapshotError::SignatureInvalid);
    }
    let mut unsigned = strict_envelope.clone();
    unsigned
        .remove_object_key("signature")
        .ok_or(SnapshotError::InvalidEnvelope)?;
    UnparsedPublicKey::new(&ED25519, &raw_key)
        .verify(unsigned.canonical().as_bytes(), &signature)
        .map_err(|_| SnapshotError::SignatureInvalid)
}

/// First 24 hex chars of sha256 over the base64 SPKI *text*.
fn derive_key_id(public_key_b64: &str) -> String {
    hex::encode(Sha256::digest(public_key_b64.as_bytes()))[..24].to_string()
}

/// Canonical-base64 decode + SPKI prefix strip, returning the raw 32-byte
/// Ed25519 key. `None` on any deviation.
fn raw_ed25519_from_spki_b64(public_key_b64: &str) -> Option<[u8; 32]> {
    let spki = decode_canonical_base64(public_key_b64)?;
    let raw = spki.strip_prefix(ED25519_SPKI_PREFIX)?;
    <[u8; 32]>::try_from(raw).ok()
}

fn decode_canonical_base64(encoded: &str) -> Option<Vec<u8>> {
    let decoded = STANDARD.decode(encoded).ok()?;
    (STANDARD.encode(&decoded) == encoded).then_some(decoded)
}

/// Canonical protocol time: exactly `YYYY-MM-DDTHH:MM:SS.mmmZ` (RFC3339
/// with milliseconds, UTC). Equivalent to the gateway's
/// `parse_protocol_time` — offsets and other precisions are rejected by
/// round-trip comparison.
fn parse_protocol_time(value: &str) -> Option<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .ok()?
        .with_timezone(&Utc);
    (parsed.to_rfc3339_opts(SecondsFormat::Millis, true) == value).then_some(parsed)
}

/// Format a time canonically (millisecond truncation, always `Z`).
pub fn format_protocol_time(value: &DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn is_lower_hex(value: &str, bytes: usize) -> bool {
    value.len() == bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_kind_scope(kind: &str, scope: &str) -> Result<(), SnapshotError> {
    let valid = |value: &str, max: usize| {
        !value.is_empty()
            && value.len() <= max
            && value.bytes().all(|b| {
                b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'.' | b'_' | b'-' | b':')
            })
    };
    if !valid(kind, 64) || !valid(scope, 128) {
        return Err(SnapshotError::InvalidEnvelope);
    }
    Ok(())
}

/// Compute the envelope digest of raw document bytes: strict-parse,
/// canonicalize, sha256, lowercase hex. `None` when the bytes are not a
/// strict-JSON document (unparseable input has no bindable digest).
/// Used by the cell's REJECTED-ack path (T11/F11) — the server compares
/// it to the stored publication's `envelopeDigest`.
pub fn compute_envelope_digest(raw: &[u8]) -> Option<String> {
    StrictValue::parse(raw)
        .ok()
        .map(|value| hex::encode(Sha256::digest(value.canonical().as_bytes())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn fixed_key() -> SigningKey {
        SigningKey::from_seed(&[0x42; 32]).unwrap()
    }

    pub(crate) fn other_key() -> SigningKey {
        SigningKey::from_seed(&[0x07; 32]).unwrap()
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-08T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sign_params<'a, T: Serialize>(payload: &'a T) -> SignParams<'a, T> {
        SignParams {
            kind: "compute.desired",
            scope: "cell:cell-01",
            revision: 7,
            issued_at: now(),
            expires_at: now() + Duration::from_secs(3600),
            payload,
        }
    }

    fn expected() -> ExpectedScope<'static> {
        ExpectedScope {
            kind: "compute.desired",
            scope: "cell:cell-01",
        }
    }

    fn mutate_json(
        raw: &[u8],
        f: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
    ) -> Vec<u8> {
        let mut v: serde_json::Value = serde_json::from_slice(raw).unwrap();
        f(v.as_object_mut().unwrap());
        serde_json::to_vec(&v).unwrap()
    }

    fn verify_err(raw: &[u8]) -> SnapshotError {
        verify::<serde_json::Value>(raw, &expected(), &[fixed_key().trusted_key()], now())
            .err()
            .expect("must fail")
    }

    #[test]
    fn sign_verify_roundtrip() {
        let payload = json!({"workers":[{"id":"w1"}],"n":3});
        let signed = sign(&sign_params(&payload), &fixed_key()).unwrap();
        let keys = [fixed_key().trusted_key()];
        let verified =
            verify::<serde_json::Value>(&signed.bytes, &expected(), &keys, now()).unwrap();
        assert_eq!(verified.payload, payload);
        assert_eq!(verified.revision, 7);
        assert_eq!(verified.kind, "compute.desired");
        assert_eq!(verified.scope, "cell:cell-01");
        assert_eq!(verified.payload_digest, signed.payload_digest);
        assert_eq!(verified.remaining_ttl, Duration::from_secs(3600));
        // sign emits canonical bytes: re-parsing must be a fixed point.
        let strict = StrictValue::parse(&signed.bytes).unwrap();
        assert_eq!(strict.canonical().as_bytes(), signed.bytes);
    }

    #[test]
    fn verify_accepts_non_canonical_wire_layout() {
        // The verifier re-canonicalizes, so a semantically identical
        // document with reordered keys and whitespace must verify.
        let payload = json!({"a":1});
        let signed = sign(&sign_params(&payload), &fixed_key()).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
        let reordered = format!(" { } ", serde_json::to_string_pretty(&value).unwrap());
        let keys = [fixed_key().trusted_key()];
        verify::<serde_json::Value>(reordered.as_bytes(), &expected(), &keys, now()).unwrap();
    }

    #[test]
    fn rejects_every_failure_path() {
        let payload = json!({"a":1});
        let signed = sign(&sign_params(&payload), &fixed_key()).unwrap();
        let keys = [fixed_key().trusted_key()];

        // TooLarge
        let oversized = vec![b' '; MAX_SNAPSHOT_BYTES + 1];
        assert_eq!(
            verify::<serde_json::Value>(&oversized, &expected(), &keys, now()).err(),
            Some(SnapshotError::TooLarge)
        );

        // InvalidJson: duplicate member
        let doc: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
        let dup = serde_json::to_string(&doc)
            .unwrap()
            .replacen("\"a\":1", "\"a\":1,\"a\":2", 1)
            .into_bytes();
        assert_eq!(verify_err(&dup), SnapshotError::InvalidJson);

        // InvalidEnvelope: missing member
        let raw = mutate_json(&signed.bytes, |o| {
            o.remove("keyId");
        });
        assert_eq!(verify_err(&raw), SnapshotError::InvalidEnvelope);
        // InvalidEnvelope: unknown member
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("extra".into(), json!(1));
        });
        assert_eq!(verify_err(&raw), SnapshotError::InvalidEnvelope);
        // InvalidEnvelope: signature not a string
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("signature".into(), json!(42));
        });
        assert_eq!(verify_err(&raw), SnapshotError::InvalidEnvelope);

        // UnsupportedVersion
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("version".into(), json!(2));
        });
        assert_eq!(verify_err(&raw), SnapshotError::UnsupportedVersion);
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("canonicalVersion".into(), json!(2));
        });
        assert_eq!(verify_err(&raw), SnapshotError::UnsupportedVersion);

        // KindMismatch / ScopeMismatch
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("kind".into(), json!("other.kind"));
        });
        assert_eq!(verify_err(&raw), SnapshotError::KindMismatch);
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("scope".into(), json!("cell:cell-02"));
        });
        assert_eq!(verify_err(&raw), SnapshotError::ScopeMismatch);

        // InvalidRevision: 0 and beyond safe integer (build raw docs since
        // sign() refuses).
        for bad in [json!(0), json!(9_007_199_254_740_991u64 + 1)] {
            let raw = mutate_json(&signed.bytes, |o| {
                o.insert("revision".into(), bad.clone());
            });
            let err = verify_err(&raw);
            assert!(
                matches!(
                    err,
                    SnapshotError::InvalidJson | SnapshotError::InvalidRevision
                ),
                "{err:?}"
            );
        }

        // InvalidTimestamp: non-canonical forms
        for bad in [
            "2026-11-08T00:00:00Z",
            "2026-11-08T01:00:00.000+01:00",
            "not-a-time",
        ] {
            let raw = mutate_json(&signed.bytes, |o| {
                o.insert("issuedAt".into(), json!(bad));
            });
            assert_eq!(verify_err(&raw), SnapshotError::InvalidTimestamp, "{bad}");
        }

        // InvalidLifetime: too short / too long
        for (i, e) in [
            ("2026-11-08T00:00:00.000Z", "2026-11-08T00:00:59.999Z"),
            ("2026-11-08T00:00:00.000Z", "2026-11-09T00:00:00.001Z"),
        ] {
            let raw = mutate_json(&signed.bytes, |o| {
                o.insert("issuedAt".into(), json!(i));
                o.insert("expiresAt".into(), json!(e));
            });
            assert_eq!(verify_err(&raw), SnapshotError::InvalidLifetime);
        }

        // NotYetValid: issuedAt > now + 30s skew
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("issuedAt".into(), json!("2026-11-08T00:00:31.000Z"));
        });
        assert_eq!(verify_err(&raw), SnapshotError::NotYetValid);

        // Expired: expiresAt <= now
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("issuedAt".into(), json!("2026-11-07T22:00:00.000Z"));
            o.insert("expiresAt".into(), json!("2026-11-07T23:59:59.999Z"));
        });
        assert_eq!(verify_err(&raw), SnapshotError::Expired);

        // DigestMismatch
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("payloadDigest".into(), json!("0".repeat(64)));
        });
        assert_eq!(verify_err(&raw), SnapshotError::DigestMismatch);
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("payloadDigest".into(), json!("ZZ".repeat(32)));
        });
        assert_eq!(verify_err(&raw), SnapshotError::DigestMismatch);

        // UnknownKeyId
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("keyId".into(), json!("f".repeat(24)));
        });
        assert_eq!(verify_err(&raw), SnapshotError::UnknownKeyId);

        // InvalidTrustedKey: keyId does not match derived id
        let bad_key = TrustedKey {
            key_id: "0".repeat(24),
            public_key: fixed_key().trusted_key().public_key,
        };
        let mut raw = mutate_json(&signed.bytes, |o| {
            o.insert("keyId".into(), json!("0".repeat(24)));
        });
        assert_eq!(
            verify::<serde_json::Value>(&raw, &expected(), &[bad_key], now()).err(),
            Some(SnapshotError::InvalidTrustedKey)
        );
        // InvalidTrustedKey: non-canonical base64 / wrong SPKI shape
        let bad_key = TrustedKey {
            key_id: derive_key_id("AAAA"),
            public_key: "AAAA".into(), // decodes but is not an Ed25519 SPKI
        };
        let doc: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let _ = doc;
        raw = mutate_json(&signed.bytes, |o| {
            o.insert("keyId".into(), json!(bad_key.key_id.clone()));
        });
        assert_eq!(
            verify::<serde_json::Value>(&raw, &expected(), &[bad_key], now()).err(),
            Some(SnapshotError::InvalidTrustedKey)
        );

        // SignatureInvalid: tampered signature, wrong signer, wrong message
        let raw = mutate_json(&signed.bytes, |o| {
            let sig = o["signature"].as_str().unwrap();
            let mut s = sig.to_string();
            s.replace_range(0..1, if s.starts_with('A') { "B" } else { "A" });
            o.insert("signature".into(), json!(s));
        });
        assert_eq!(verify_err(&raw), SnapshotError::SignatureInvalid);

        let wrong = sign(&sign_params(&payload), &other_key()).unwrap();
        assert_eq!(
            verify::<serde_json::Value>(&wrong.bytes, &expected(), &keys, now()).err(),
            Some(SnapshotError::UnknownKeyId) // other key's id is not trusted
        );
        // trust both keys but the envelope was signed by other_key → its own
        // key id is used, so verification needs the right key present:
        let both = [fixed_key().trusted_key(), other_key().trusted_key()];
        verify::<serde_json::Value>(&wrong.bytes, &expected(), &both, now()).unwrap();

        // signature over a different payload → SignatureInvalid
        let mut doc: serde_json::Value = serde_json::from_slice(&signed.bytes).unwrap();
        doc["payload"] = json!({"a":2});
        doc["payloadDigest"] = json!(hex::encode(Sha256::digest(
            StrictValue::from_serializable(&json!({"a":2}))
                .unwrap()
                .canonical()
                .as_bytes()
        )));
        let raw = serde_json::to_vec(&doc).unwrap();
        assert_eq!(verify_err(&raw), SnapshotError::SignatureInvalid);

        // PayloadInvalid: payload does not decode into T
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Narrow {
            #[allow(dead_code)]
            present: u64,
        }
        let payload2 = json!({"present":1,"unknown":2});
        let signed2 = sign(&sign_params(&payload2), &fixed_key()).unwrap();
        assert_eq!(
            verify::<Narrow>(&signed2.bytes, &expected(), &keys, now()).err(),
            Some(SnapshotError::PayloadInvalid)
        );
    }

    #[test]
    fn clock_skew_boundaries() {
        let payload = json!({"a":1});
        let mut p = sign_params(&payload);
        // issuedAt exactly now+30s is accepted; +30.001s is not.
        p.issued_at = now() + Duration::from_millis(30_000);
        p.expires_at = now() + Duration::from_millis(30_000) + Duration::from_secs(3600);
        let signed = sign(&p, &fixed_key()).unwrap();
        let keys = [fixed_key().trusted_key()];
        verify::<serde_json::Value>(&signed.bytes, &expected(), &keys, now()).unwrap();

        p.issued_at = now() + Duration::from_millis(30_001);
        p.expires_at = now() + Duration::from_millis(30_001) + Duration::from_secs(3600);
        let signed = sign(&p, &fixed_key()).unwrap();
        assert_eq!(
            verify::<serde_json::Value>(&signed.bytes, &expected(), &keys, now()).err(),
            Some(SnapshotError::NotYetValid)
        );

        // expiresAt exactly now is expired; now+1ms is not.
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("issuedAt".into(), json!("2026-11-07T23:00:00.000Z"));
            o.insert("expiresAt".into(), json!("2026-11-08T00:00:00.000Z"));
        });
        assert_eq!(verify_err(&raw), SnapshotError::Expired);
        let raw = mutate_json(&signed.bytes, |o| {
            o.insert("expiresAt".into(), json!("2026-11-08T00:00:00.001Z"));
        });
        // signature now stale → would be SignatureInvalid; re-sign instead.
        let _ = raw;
        let mut p = sign_params(&payload);
        p.issued_at = now() - Duration::from_secs(3600);
        p.expires_at = now() + Duration::from_millis(1);
        let signed = sign(&p, &fixed_key()).unwrap();
        verify::<serde_json::Value>(&signed.bytes, &expected(), &keys, now()).unwrap();
    }

    #[test]
    fn lifetime_bounds_boundaries() {
        let payload = json!({"a":1});
        let keys = [fixed_key().trusted_key()];
        for ttl in [60_000u64, 86_400_000u64] {
            let mut p = sign_params(&payload);
            p.expires_at = p.issued_at + Duration::from_millis(ttl);
            let signed = sign(&p, &fixed_key()).unwrap();
            verify::<serde_json::Value>(&signed.bytes, &expected(), &keys, now()).unwrap();
        }
        for ttl in [59_999u64, 86_400_001u64] {
            let mut p = sign_params(&payload);
            p.expires_at = p.issued_at + Duration::from_millis(ttl);
            assert_eq!(sign(&p, &fixed_key()), Err(SnapshotError::InvalidLifetime));
        }
    }

    /// Canonical consumer sequence: verify → payload validation → accept.
    fn verify_and_accept(
        raw: &[u8],
        guard: &mut RevisionGuard,
    ) -> Result<Verified<serde_json::Value>, SnapshotError> {
        let keys = [fixed_key().trusted_key()];
        let verified = verify::<serde_json::Value>(raw, &expected(), &keys, now())?;
        guard.accept(verified.revision, &verified.envelope_digest)?;
        Ok(verified)
    }

    #[test]
    fn revision_guard_monotone_and_idempotent() {
        let mut guard = RevisionGuard::new();
        let payload = json!({"v":1});
        let mut p = sign_params(&payload);

        let signed7 = sign(&p, &fixed_key()).unwrap();
        verify_and_accept(&signed7.bytes, &mut guard).unwrap();
        assert_eq!(guard.last_accepted(), 7);

        // idempotent re-delivery of the identical signed envelope
        verify_and_accept(&signed7.bytes, &mut guard).unwrap();

        // same revision, different document → conflict
        p.issued_at = now() + Duration::from_millis(1);
        p.expires_at = now() + Duration::from_secs(3601);
        let signed7b = sign(&p, &fixed_key()).unwrap();
        assert_eq!(
            verify_and_accept(&signed7b.bytes, &mut guard)
                .err()
                .map(|e| e.code()),
            Some("SNAPSHOT_REVISION_CONFLICT")
        );

        // rollback
        p.revision = 6;
        let signed6 = sign(&p, &fixed_key()).unwrap();
        assert_eq!(
            verify_and_accept(&signed6.bytes, &mut guard)
                .err()
                .map(|e| e.code()),
            Some("SNAPSHOT_REVISION_ROLLBACK")
        );

        // advance with renewed payload (pure renewal allowed)
        p.revision = 8;
        let signed8 = sign(&p, &fixed_key()).unwrap();
        verify_and_accept(&signed8.bytes, &mut guard).unwrap();
        assert_eq!(guard.last_accepted(), 8);

        // an accept() that follows a FAILED verification must not happen:
        // verify() erroring leaves nothing to accept, and the fence stays.
        p.revision = 9;
        let mut doc: serde_json::Value =
            serde_json::from_slice(&sign(&p, &fixed_key()).unwrap().bytes).unwrap();
        doc["payload"] = json!({"tampered":true});
        let raw = serde_json::to_vec(&doc).unwrap();
        assert!(verify_and_accept(&raw, &mut guard).is_err());
        assert_eq!(guard.last_accepted(), 8);

        // restore round-trips the fence
        let restored = RevisionGuard::restore(8, guard.last_envelope_digest()).unwrap();
        assert_eq!(restored, guard);
        assert!(RevisionGuard::restore(0, &"a".repeat(64)).is_none());
        assert!(RevisionGuard::restore(9, "not-hex").is_none());
        assert!(RevisionGuard::restore(u64::MAX, &"a".repeat(64)).is_none());
    }

    #[test]
    fn sign_rejects_bad_params() {
        let payload = json!({"a":1});
        let mut p = sign_params(&payload);
        p.revision = 0;
        assert_eq!(sign(&p, &fixed_key()), Err(SnapshotError::InvalidRevision));
        p.revision = MAX_SAFE_INTEGER + 1;
        assert_eq!(sign(&p, &fixed_key()), Err(SnapshotError::InvalidRevision));

        let mut p = sign_params(&payload);
        p.kind = "UPPER CASE";
        assert_eq!(sign(&p, &fixed_key()), Err(SnapshotError::InvalidEnvelope));
        let mut p = sign_params(&payload);
        p.scope = "";
        assert_eq!(sign(&p, &fixed_key()), Err(SnapshotError::InvalidEnvelope));
    }

    #[test]
    fn key_id_derivation_matches_gateway_rule() {
        // keyId = sha256(publicKey_base64_text)[..24] hex — the same
        // derivation the gateway verifies and its TS signer produces.
        let key = fixed_key();
        let trusted = key.trusted_key();
        let expect = hex::encode(Sha256::digest(trusted.public_key.as_bytes()))[..24].to_string();
        assert_eq!(trusted.key_id, expect);
        assert_eq!(key.key_id(), expect);
    }

    /// Equivalence proof against the production gateway protocol, using
    /// `shared/postgres-tenant-gateway-hot-config-protocol-golden.json`
    /// (a fixture the gateway's own tests verify). Exercises the exact
    /// helpers `verify()` uses: `StrictValue::parse`/`canonical`,
    /// `verify_signature`, `derive_key_id`, `decode_canonical_base64`,
    /// `parse_protocol_time`. The gateway envelope layout (channel +
    /// clusterId, version 2) differs from this crate's generic envelope
    /// (kind + scope, version 1) by design — what must be identical is the
    /// canonicalization, digest, key-id and signature math asserted here.
    #[test]
    fn gateway_golden_signed_envelope_verifies() {
        use crate::golden_fixture::*;
        let strict = StrictValue::parse(GATEWAY_FIXTURE_ENVELOPE.as_bytes()).unwrap();

        // 1. Canonical bytes of the unsigned envelope are byte-identical to
        // the TypeScript signer's output recorded in the fixture.
        let mut unsigned = strict.clone();
        unsigned.remove_object_key("signature").unwrap();
        assert_eq!(unsigned.canonical(), GATEWAY_FIXTURE_EXPECTED_CANONICAL);

        // 2. sha256 over the canonical payload member equals the declared
        // and expected payload digest.
        let payload_strict = strict.get("payload").unwrap();
        let digest = hex::encode(Sha256::digest(payload_strict.canonical().as_bytes()));
        assert_eq!(digest, GATEWAY_FIXTURE_EXPECTED_PAYLOAD_DIGEST);
        assert_eq!(
            strict.get("payloadDigest").unwrap().as_str().unwrap(),
            GATEWAY_FIXTURE_EXPECTED_PAYLOAD_DIGEST
        );

        // 3. The recorded Ed25519 signature verifies through the same
        // `verify_signature` helper used by `verify()`.
        let keys: Vec<TrustedKey> = serde_json::from_str(GATEWAY_FIXTURE_PUBLIC_KEYS).unwrap();
        let signature = strict
            .get("signature")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let key_id = strict.get("keyId").unwrap().as_str().unwrap().to_string();
        verify_signature(&strict, &signature, &key_id, &keys).unwrap();

        // 4. keyId = sha256(base64 SPKI text)[..24] — same derivation.
        assert_eq!(derive_key_id(&keys[0].public_key), keys[0].key_id);

        // 5. Fail-closed negatives on the same helper.
        let mut bad_sig = signature.clone();
        bad_sig.replace_range(0..1, if bad_sig.starts_with('A') { "B" } else { "A" });
        assert_eq!(
            verify_signature(&strict, &bad_sig, &key_id, &keys),
            Err(SnapshotError::SignatureInvalid)
        );
        assert_eq!(
            verify_signature(&strict, &signature, &"f".repeat(24), &keys),
            Err(SnapshotError::UnknownKeyId)
        );
        let wrong_id = TrustedKey {
            key_id: "0".repeat(24),
            public_key: keys[0].public_key.clone(),
        };
        assert_eq!(
            verify_signature(&strict, &signature, &"0".repeat(24), &[wrong_id]),
            Err(SnapshotError::InvalidTrustedKey)
        );

        // 6. Canonical protocol timestamps parse.
        for member in ["issuedAt", "expiresAt"] {
            let value = strict.get(member).unwrap().as_str().unwrap();
            assert!(parse_protocol_time(value).is_some(), "{member}");
        }
    }

    /// Frozen golden vector for the sign side: a fixed test seed
    /// (`0x42 * 32`, never used for real keys) produces deterministic
    /// canonical bytes, payload digest, key id and Ed25519 signature —
    /// Ed25519 signatures are deterministic per RFC 8032. The constants
    /// freeze the protocol math: any regression in canonicalization,
    /// digest, key-id derivation or signature format fails this test.
    /// `GOLDEN_CANONICAL_HEX` is the canonical wire document.
    const GOLDEN_PUBLIC_KEY_B64: &str =
        "MCowBQYDK2VwAyEAIVL40Zt5HSRFMkLhXy6rbLfP+ntqXtMAl5YOBpiB2xI=";
    const GOLDEN_KEY_ID: &str = "a80b718d5ae4b7ae28849beb";
    const GOLDEN_PAYLOAD_DIGEST: &str =
        "d5e0d51cbfbe70f1d6498c239356cf63f3e9a6b64b912a11e21b3c24da3c8cfd";
    const GOLDEN_SIGNATURE_B64: &str =
        "Tz7rX1FJ73DewcdERWl+2zzlaBCXCqU4rqtdopU+3UvvL3lWzyuH8FuSnfRo17sQRvJWcXOSwpYBeFwXmpRDDQ==";
    const GOLDEN_CANONICAL_HEX: &str = "7b2263616e6f6e6963616c56657273696f6e223a312c22657870697265734174223a22323032362d31312d30385430363a30303a30302e3030305a222c226973737565644174223a22323032362d31312d30385430303a30303a30302e3030305a222c226b65794964223a22613830623731386435616534623761653238383439626562222c226b696e64223a22636f6d707574652e64657369726564222c227061796c6f6164223a7b2263656c6c4964223a2263656c6c2d676f6c64656e2d3031222c227265766973696f6e223a34322c22736368656d6156657273696f6e223a312c2274656e616e7473223a5b5d7d2c227061796c6f6164446967657374223a2264356530643531636266626537306631643634393863323339333536636636336633653961366236346239313261313165323162336332346461336338636664222c227265766973696f6e223a34322c2273636f7065223a2263656c6c3a63656c6c2d676f6c64656e2d3031222c227369676e6174757265223a22547a37725831464a373344657763644552576c2b327a7a6c6142435843715534727174646f70552b335576764c336c577a797548384675536e66526f3137735152764a5763584f5377705942654677586d70524444513d3d222c2276657273696f6e223a317d";

    #[test]
    fn fixed_key_golden_vector_matches() {
        let payload = serde_json::json!({
            "schemaVersion": 1,
            "cellId": "cell-golden-01",
            "revision": 42,
            "tenants": []
        });
        let params = SignParams {
            kind: "compute.desired",
            scope: "cell:cell-golden-01",
            revision: 42,
            issued_at: now(),
            expires_at: now() + Duration::from_secs(21_600),
            payload: &payload,
        };
        let signed = sign(&params, &fixed_key()).unwrap();
        assert_eq!(fixed_key().trusted_key().public_key, GOLDEN_PUBLIC_KEY_B64);
        assert_eq!(fixed_key().key_id(), GOLDEN_KEY_ID);
        assert_eq!(signed.payload_digest, GOLDEN_PAYLOAD_DIGEST);
        assert_eq!(signed.signature, GOLDEN_SIGNATURE_B64);
        assert_eq!(hex::encode(&signed.bytes), GOLDEN_CANONICAL_HEX);

        // The frozen wire document verifies end-to-end, and the embedded
        // public key verifies the embedded signature independently.
        let bytes = hex::decode(GOLDEN_CANONICAL_HEX).unwrap();
        let keys = [TrustedKey {
            key_id: GOLDEN_KEY_ID.into(),
            public_key: GOLDEN_PUBLIC_KEY_B64.into(),
        }];
        let verified =
            verify::<serde_json::Value>(&bytes, &expected_scope(), &keys, now()).unwrap();
        assert_eq!(verified.payload, payload);
        assert_eq!(verified.payload_digest, GOLDEN_PAYLOAD_DIGEST);

        fn expected_scope() -> ExpectedScope<'static> {
            ExpectedScope {
                kind: "compute.desired",
                scope: "cell:cell-golden-01",
            }
        }
    }

    #[test]
    fn trusted_key_from_public_key_derives_id() {
        let trusted = fixed_key().trusted_key();
        let derived = TrustedKey::from_public_key(trusted.public_key.clone()).unwrap();
        assert_eq!(derived, trusted);
        assert!(TrustedKey::from_public_key("!!!not-base64!!!").is_err());
        assert!(TrustedKey::from_public_key("AAAA").is_err());
    }
}
