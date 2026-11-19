//! `CellVerifier` — the `snapshot-protocol` trust boundary for this cell.
//!
//! Wraps `verify_compute_desired` (envelope shape + signature + expiry +
//! scope `cell:<cellId>` + typed payload + envelope/payload binding +
//! revision fence) and translates every failure into a typed
//! [`RejectReason`] suitable for the ACK `reason` field and structured
//! logs — reasons are fixed identifiers, never attacker-controlled bytes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snapshot_protocol::compute_desired::{
    verify_compute_desired, ComputeDesired, ComputeDesiredError,
};
use snapshot_protocol::envelope::{RevisionGuard, Verified};
use snapshot_protocol::{SnapshotError, TrustedKey};

/// Typed rejection categories — stable wire codes for the ACK `reason`
/// field. Deliberately coarse: the control plane needs to know the class
/// of failure, not attacker-controlled detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// Envelope is not strict protocol JSON / has a bad member set.
    MalformedEnvelope,
    /// `kind` is not `compute.desired`.
    WrongKind,
    /// `scope` is not `cell:<this cellId>`.
    WrongScope,
    /// Unsupported protocol/canonical version.
    UnsupportedVersion,
    /// Signature invalid, or `keyId` untrusted/malformed.
    SignatureOrKey,
    /// `payloadDigest` does not match the canonical payload.
    DigestMismatch,
    /// Timestamp/lifetime violation (incl. `expiresAt` in the past).
    Expired,
    /// `issuedAt` beyond the allowed future skew.
    NotYetValid,
    /// Revision strictly below the last accepted one.
    RevisionRollback,
    /// Same revision carrying different signed bytes.
    RevisionConflict,
    /// Typed payload failed decode or semantic validation.
    PayloadInvalid,
    /// Verified snapshot could not be persisted to the revision-fence
    /// store — transient I/O; nothing applied, fence already advanced
    /// in memory (monotone), next tick retries.
    FencePersist,
}

impl RejectReason {
    /// Whether the rejection is transient — the same bytes may verify
    /// later. Transient rejections are never reported as terminal
    /// `REJECTED` ACKs (see controller docs); deterministic violations
    /// (signature, schema, kind/scope, revision fence) are.
    pub fn is_transient(self) -> bool {
        matches!(self, Self::Expired | Self::NotYetValid | Self::FencePersist)
    }

    /// Stable lowercase wire code for ACKs + logs.
    pub fn code(self) -> &'static str {
        match self {
            Self::MalformedEnvelope => "malformed-envelope",
            Self::WrongKind => "wrong-kind",
            Self::WrongScope => "wrong-scope",
            Self::UnsupportedVersion => "unsupported-version",
            Self::SignatureOrKey => "signature-or-key",
            Self::DigestMismatch => "digest-mismatch",
            Self::Expired => "expired",
            Self::NotYetValid => "not-yet-valid",
            Self::RevisionRollback => "revision-rollback",
            Self::RevisionConflict => "revision-conflict",
            Self::PayloadInvalid => "payload-invalid",
            Self::FencePersist => "fence-persist",
        }
    }
}

impl From<&ComputeDesiredError> for RejectReason {
    fn from(error: &ComputeDesiredError) -> Self {
        match error {
            ComputeDesiredError::Snapshot(s) => RejectReason::from(s.clone()),
            ComputeDesiredError::Payload(_) => Self::PayloadInvalid,
        }
    }
}

impl From<SnapshotError> for RejectReason {
    fn from(error: SnapshotError) -> Self {
        match error {
            SnapshotError::TooLarge
            | SnapshotError::InvalidJson
            | SnapshotError::InvalidEnvelope
            | SnapshotError::InvalidTimestamp
            | SnapshotError::InvalidLifetime => Self::MalformedEnvelope,
            SnapshotError::KindMismatch => Self::WrongKind,
            SnapshotError::ScopeMismatch => Self::WrongScope,
            SnapshotError::UnsupportedVersion => Self::UnsupportedVersion,
            SnapshotError::UnknownKeyId
            | SnapshotError::InvalidTrustedKey
            | SnapshotError::SignatureInvalid => Self::SignatureOrKey,
            SnapshotError::DigestMismatch => Self::DigestMismatch,
            SnapshotError::Expired => Self::Expired,
            SnapshotError::NotYetValid => Self::NotYetValid,
            SnapshotError::RevisionRollback => Self::RevisionRollback,
            SnapshotError::RevisionConflict => Self::RevisionConflict,
            SnapshotError::InvalidRevision | SnapshotError::PayloadInvalid => Self::PayloadInvalid,
            other => {
                tracing::warn!(%other, "unmapped snapshot error rejected");
                Self::MalformedEnvelope
            }
        }
    }
}

/// Verifier for one cell: trusted keys + monotone revision fence.
/// `verify` is fail-closed on every axis — a rejected envelope changes
/// nothing except the (failed) fence attempt, and the last applied state
/// is owned by the controller, not here.
#[derive(Debug)]
pub struct CellVerifier {
    cell_id: String,
    trusted_keys: Vec<TrustedKey>,
    guard: RevisionGuard,
}

impl CellVerifier {
    /// New verifier with an empty fence (first good snapshot wins).
    pub fn new(cell_id: impl Into<String>, trusted_keys: Vec<TrustedKey>) -> Self {
        Self {
            cell_id: cell_id.into(),
            trusted_keys,
            guard: RevisionGuard::new(),
        }
    }

    /// Restore after restart: rebuild the fence from persisted state so a
    /// replayed older snapshot still loses the fence across restarts.
    pub fn restore(
        cell_id: impl Into<String>,
        trusted_keys: Vec<TrustedKey>,
        last_revision: u64,
        last_envelope_digest: &str,
    ) -> Option<Self> {
        Some(Self {
            cell_id: cell_id.into(),
            trusted_keys,
            guard: RevisionGuard::restore(last_revision, last_envelope_digest)?,
        })
    }

    /// Persist pair for `restore` (after a successful `verify`).
    pub fn fence(&self) -> (u64, &str) {
        (
            self.guard.last_accepted(),
            self.guard.last_envelope_digest(),
        )
    }

    /// Full fail-closed verification. On success returns the verified
    /// snapshot (revision fence advanced); on failure returns the typed
    /// reason and nothing is applied.
    pub fn verify(
        &mut self,
        raw: &[u8],
        now: DateTime<Utc>,
    ) -> Result<Verified<ComputeDesired>, RejectReason> {
        verify_compute_desired(raw, &self.cell_id, &self.trusted_keys, &mut self.guard, now)
            .map_err(|e| RejectReason::from(&e))
    }

    /// Whether `candidate` would be a fence-advancing new revision (for
    /// diagnostics / idempotent-replay detection before re-verifying).
    pub fn last_accepted_revision(&self) -> u64 {
        self.guard.last_accepted()
    }
}

/// Untrusted envelope fields extracted solely for the REJECTED ACK —
/// the server re-binds them to a stored publication row, so a forged
/// extract can never widen authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedEnvelope {
    /// `envelope.revision` when extractable.
    pub revision: Option<u64>,
    /// `envelope.payloadDigest` when extractable.
    pub payload_digest: Option<String>,
    /// sha256 of the document's canonical form — computable only when
    /// the bytes parse under strict JSON (T11/F11). The server compares
    /// it to the stored publication's envelopeDigest, so a forged claim
    /// can never bind.
    pub envelope_digest: Option<String>,
}

/// Best-effort extraction of `(revision, payloadDigest)` from raw
/// envelope bytes for the REJECTED report. Returns `None` fields when the
/// bytes do not parse — callers treat them as "no publication to blame".
pub fn extract_claimed(raw: &[u8]) -> ClaimedEnvelope {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(raw) else {
        return ClaimedEnvelope {
            revision: None,
            payload_digest: None,
            envelope_digest: None,
        };
    };
    // Best-effort digest of the RECEIVED document (not its claims): the
    // same canonical-then-sha256 rule the signer used.
    let envelope_digest = snapshot_protocol::compute_envelope_digest(raw);
    let revision = value
        .get("revision")
        .and_then(|v| v.as_u64())
        .filter(|r| *r >= 1);
    let payload_digest = value
        .get("payloadDigest")
        .and_then(|v| v.as_str())
        .filter(|d| {
            d.len() == 64
                && d.bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
        .map(str::to_string);
    ClaimedEnvelope {
        revision,
        payload_digest,
        envelope_digest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use snapshot_protocol::envelope::{sign, SignParams, SigningKey};

    fn key() -> SigningKey {
        SigningKey::from_seed(&[0x42; 32]).unwrap()
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-08T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn payload(cell: &str, revision: u64) -> serde_json::Value {
        json!({
            "schemaVersion": 1,
            "cellId": cell,
            "revision": revision,
            "tenants": []
        })
    }

    fn envelope(cell: &str, revision: u64) -> Vec<u8> {
        sign(
            &SignParams {
                kind: "compute.desired",
                scope: &format!("cell:{cell}"),
                revision,
                issued_at: now(),
                expires_at: now() + chrono::Duration::hours(1),
                payload: &payload(cell, revision),
            },
            &key(),
        )
        .unwrap()
        .bytes
    }

    fn verifier(cell: &str) -> CellVerifier {
        CellVerifier::new(cell, vec![key().trusted_key()])
    }

    #[test]
    fn verifies_good_snapshot_and_fences() {
        let mut v = verifier("cell-1");
        let ok = v.verify(&envelope("cell-1", 5), now()).unwrap();
        assert_eq!(ok.revision, 5);
        assert_eq!(v.last_accepted_revision(), 5);
        // Same revision + identical document → idempotent accept.
        assert!(v.verify(&envelope("cell-1", 5), now()).is_ok());
    }

    #[test]
    fn rejects_wrong_scope_and_wrong_kind() {
        let mut v = verifier("cell-1");
        assert_eq!(
            v.verify(&envelope("cell-other", 5), now()).unwrap_err(),
            RejectReason::WrongScope
        );
        let wrong_kind = sign(
            &SignParams {
                kind: "tenant.gateway",
                scope: "cell:cell-1",
                revision: 5,
                issued_at: now(),
                expires_at: now() + chrono::Duration::hours(1),
                payload: &payload("cell-1", 5),
            },
            &key(),
        )
        .unwrap()
        .bytes;
        assert_eq!(
            v.verify(&wrong_kind, now()).unwrap_err(),
            RejectReason::WrongKind
        );
    }

    #[test]
    fn rejects_tampered_signature_and_rollback_and_conflict() {
        let mut v = verifier("cell-1");
        let mut bad: serde_json::Value = serde_json::from_slice(&envelope("cell-1", 3)).unwrap();
        bad["revision"] = json!(4); // unsigned mutation → signature invalid
        let tampered = serde_json::to_vec(&bad).unwrap();
        assert_eq!(
            v.verify(&tampered, now()).unwrap_err(),
            RejectReason::SignatureOrKey
        );

        v.verify(&envelope("cell-1", 5), now()).unwrap();
        assert_eq!(
            v.verify(&envelope("cell-1", 4), now()).unwrap_err(),
            RejectReason::RevisionRollback
        );

        // Same revision, different document → conflict.
        let mut alt = payload("cell-1", 5);
        alt["tenants"] = json!([{"tenantId": "team-x", "workers": []}]);
        let conflict = sign(
            &SignParams {
                kind: "compute.desired",
                scope: "cell:cell-1",
                revision: 5,
                issued_at: now(),
                expires_at: now() + chrono::Duration::hours(1),
                payload: &alt,
            },
            &key(),
        )
        .unwrap()
        .bytes;
        assert_eq!(
            v.verify(&conflict, now()).unwrap_err(),
            RejectReason::RevisionConflict
        );
    }

    #[test]
    fn rejects_expired_and_extracts_claimed() {
        let mut v = verifier("cell-1");
        let expired_at = now() - chrono::Duration::seconds(1);
        let raw = sign(
            &SignParams {
                kind: "compute.desired",
                scope: "cell:cell-1",
                revision: 9,
                issued_at: expired_at - chrono::Duration::hours(1),
                expires_at: expired_at,
                payload: &payload("cell-1", 9),
            },
            &key(),
        )
        .unwrap()
        .bytes;
        assert_eq!(v.verify(&raw, now()).unwrap_err(), RejectReason::Expired);
        let claimed = extract_claimed(&raw);
        assert_eq!(claimed.revision, Some(9));
        assert!(claimed.payload_digest.is_some());
    }
}
