//! Typed snapshot errors with stable wire-safe error codes.
//!
//! Every rejection path in this crate maps to exactly one variant. `Display`
//! output is deliberately terse and never carries key material, signature
//! bytes, or attacker-controlled field values — only stable identifiers.
//! The `code()` strings are part of the protocol contract and must not
//! change without a documented changelog entry.

/// Typed failure for snapshot signing, parsing, or verification.
///
/// Stable codes are returned by [`SnapshotError::code`]. `Display` and
/// `source` never embed secret or attacker-controlled values.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// Wire document exceeds [`crate::envelope::MAX_SNAPSHOT_BYTES`].
    #[error("snapshot exceeds the maximum wire size")]
    TooLarge,

    /// Document failed strict JSON parsing (duplicate member, unsafe number,
    /// `__proto__` key, malformed UTF-16, or trailing garbage).
    #[error("snapshot is not valid strict protocol JSON")]
    InvalidJson,

    /// Top-level object is missing a required envelope member, carries an
    /// unknown member, or a member has the wrong type.
    #[error("snapshot envelope is malformed")]
    InvalidEnvelope,

    /// `version`/`canonicalVersion` do not match this implementation.
    #[error("unsupported snapshot protocol version")]
    UnsupportedVersion,

    /// `kind` does not equal the expected snapshot kind.
    #[error("snapshot kind does not match the expected kind")]
    KindMismatch,

    /// `scope` does not equal the expected audience scope.
    #[error("snapshot scope does not match the expected scope")]
    ScopeMismatch,

    /// `revision` is zero or exceeds the JavaScript safe-integer range.
    #[error("snapshot revision is outside the accepted range")]
    InvalidRevision,

    /// `issuedAt`/`expiresAt` is not canonical protocol time
    /// (`YYYY-MM-DDTHH:MM:SS.mmmZ`).
    #[error("snapshot timestamp is not canonical protocol time")]
    InvalidTimestamp,

    /// `expiresAt - issuedAt` is outside `[MIN_TTL, MAX_TTL]`.
    #[error("snapshot lifetime is outside the accepted bounds")]
    InvalidLifetime,

    /// `issuedAt` is more than the allowed clock skew in the future.
    #[error("snapshot issuedAt is too far in the future")]
    NotYetValid,

    /// `expiresAt` is at or before the verifier's clock.
    #[error("snapshot is expired")]
    Expired,

    /// `payloadDigest` is not lowercase-hex sha256 or does not equal the
    /// sha256 of the canonical payload bytes.
    #[error("payload digest does not match the signed payload")]
    DigestMismatch,

    /// No trusted key has the envelope's `keyId`.
    #[error("no trusted key matches the envelope keyId")]
    UnknownKeyId,

    /// A trusted key record is malformed: non-canonical base64, wrong SPKI
    /// shape, or the stored keyId does not match the derived key id.
    #[error("trusted key record is malformed")]
    InvalidTrustedKey,

    /// `signature` is not canonical base64, not 64 bytes, or Ed25519
    /// verification over the canonical unsigned envelope failed.
    #[error("snapshot signature verification failed")]
    SignatureInvalid,

    /// The signed payload failed typed decoding.
    #[error("snapshot payload failed typed decoding")]
    PayloadInvalid,

    /// Envelope revision is lower than the last accepted revision.
    #[error("snapshot revision regressed below the last accepted revision")]
    RevisionRollback,

    /// Envelope revision equals the last accepted revision but the signed
    /// envelope bytes differ.
    #[error("same revision carries a different signed envelope")]
    RevisionConflict,

    /// Signing failed to serialize the payload or envelope.
    #[error("snapshot serialization failed")]
    Serialization,

    /// Locally supplied key material (seed or PKCS#8) is malformed.
    #[error("local signing key material is malformed")]
    KeyMaterial,
}

impl SnapshotError {
    /// Stable protocol error code; never contains user or key material.
    pub fn code(&self) -> &'static str {
        match self {
            Self::TooLarge => "SNAPSHOT_TOO_LARGE",
            Self::InvalidJson => "SNAPSHOT_INVALID_JSON",
            Self::InvalidEnvelope => "SNAPSHOT_INVALID_ENVELOPE",
            Self::UnsupportedVersion => "SNAPSHOT_UNSUPPORTED_VERSION",
            Self::KindMismatch => "SNAPSHOT_KIND_MISMATCH",
            Self::ScopeMismatch => "SNAPSHOT_SCOPE_MISMATCH",
            Self::InvalidRevision => "SNAPSHOT_INVALID_REVISION",
            Self::InvalidTimestamp => "SNAPSHOT_INVALID_TIMESTAMP",
            Self::InvalidLifetime => "SNAPSHOT_INVALID_LIFETIME",
            Self::NotYetValid => "SNAPSHOT_NOT_YET_VALID",
            Self::Expired => "SNAPSHOT_EXPIRED",
            Self::DigestMismatch => "SNAPSHOT_DIGEST_MISMATCH",
            Self::UnknownKeyId => "SNAPSHOT_UNKNOWN_KEY_ID",
            Self::InvalidTrustedKey => "SNAPSHOT_INVALID_TRUSTED_KEY",
            Self::SignatureInvalid => "SNAPSHOT_SIGNATURE_INVALID",
            Self::PayloadInvalid => "SNAPSHOT_PAYLOAD_INVALID",
            Self::RevisionRollback => "SNAPSHOT_REVISION_ROLLBACK",
            Self::RevisionConflict => "SNAPSHOT_REVISION_CONFLICT",
            Self::Serialization => "SNAPSHOT_SERIALIZATION_FAILED",
            Self::KeyMaterial => "SNAPSHOT_KEY_MATERIAL_INVALID",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_stable_and_distinct() {
        let variants = [
            SnapshotError::TooLarge,
            SnapshotError::InvalidJson,
            SnapshotError::InvalidEnvelope,
            SnapshotError::UnsupportedVersion,
            SnapshotError::KindMismatch,
            SnapshotError::ScopeMismatch,
            SnapshotError::InvalidRevision,
            SnapshotError::InvalidTimestamp,
            SnapshotError::InvalidLifetime,
            SnapshotError::NotYetValid,
            SnapshotError::Expired,
            SnapshotError::DigestMismatch,
            SnapshotError::UnknownKeyId,
            SnapshotError::InvalidTrustedKey,
            SnapshotError::SignatureInvalid,
            SnapshotError::PayloadInvalid,
            SnapshotError::RevisionRollback,
            SnapshotError::RevisionConflict,
            SnapshotError::Serialization,
            SnapshotError::KeyMaterial,
        ];
        let mut codes: Vec<&'static str> = variants.iter().map(|v| v.code()).collect();
        let total = codes.len();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), total, "error codes must be distinct");
        for code in codes {
            assert!(code.starts_with("SNAPSHOT_"));
            assert!(code.bytes().all(|b| b.is_ascii_uppercase() || b == b'_'));
        }
    }

    #[test]
    fn display_contains_no_dynamic_material() {
        // Display strings are fully static — assert none interpolate data.
        let err = SnapshotError::SignatureInvalid;
        assert_eq!(err.to_string(), "snapshot signature verification failed");
    }
}
