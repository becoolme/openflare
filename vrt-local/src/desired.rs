//! Signed-envelope file handling — atomic publish + read-back for
//! status. The host side (`FileDesiredStore`) verifies signature,
//! fence and expiry; this side only writes complete documents.

use crate::LocalError;
use std::path::Path;

pub fn write_envelope(root: &Path, bytes: &[u8]) -> Result<(), LocalError> {
    vrt_securefs::atomic_write(
        &crate::layout::desired_dir(root).join("envelope.json"),
        bytes,
        0o644,
    )
    .map_err(|_| LocalError::Layout("envelope write"))
}

/// Revision of the envelope on disk (for `status`), or `None` when
/// absent/unreadable — never used for authority, only reporting.
pub fn read_envelope_revision(root: &Path) -> Option<u64> {
    let raw = std::fs::read(crate::layout::desired_dir(root).join("envelope.json")).ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    doc["revision"].as_u64()
}

/// Expiry of the on-disk envelope (unix ms) for `serve` healthz.
/// `expiresAt` is an RFC3339 STRING on the wire (`format_protocol_time`,
/// envelope.rs) — parsing it as an integer silently yielded `None`
/// ("no envelope"), which healthz then read as "nothing to expire" and
/// reported healthy regardless of an actually-expired envelope.
pub fn read_envelope_expiry_ms(root: &Path) -> Option<i64> {
    let raw = std::fs::read(crate::layout::desired_dir(root).join("envelope.json")).ok()?;
    let doc: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    let s = doc["expiresAt"].as_str()?;
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|d| d.timestamp_millis())
}

/// Why a verified read did not produce a payload — `Missing` is "no
/// authority published yet" (idle, not an error); `Invalid` carries the
/// typed verify code (signature/expiry/fence/decode — never payload data).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesiredRead {
    /// Envelope absent or unreadable.
    Missing,
    /// Present but failed `verify_compute_desired` — signature, expiry,
    /// revision fence, scope or typed decode. Callers must NOT dispatch
    /// against it (fail closed); the code is for rate-limited logging.
    Invalid(&'static str),
}

/// T37b-r3: verified read-back for local consumers (cron + queue
/// dispatch). The previous lenient version only parsed `payload` and
/// read `expiresAt` with `as_i64()` — but `expiresAt` is an RFC3339
/// string on the wire, so every call returned `None` and standalone
/// dispatch stayed idle forever after every deploy. The read now runs
/// the SAME `verify_compute_desired` the host runs: signature against
/// the local trusted key, scope binding, expiry, typed decode, semantic
/// validate and the caller-held `RevisionGuard` fence. A tampered,
/// expired, rolled-back or same-revision-conflicting file is `Invalid`
/// — the caller must idle (and the dispatcher must tear its lanes
/// down), never fall back to the last good set silently.
pub fn read_verified_desired(
    root: &Path,
    cell_id: &str,
    trusted_keys: &[snapshot_protocol::TrustedKey],
    guard: &mut snapshot_protocol::envelope::RevisionGuard,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<
    (
        snapshot_protocol::compute_desired::ComputeDesired,
        chrono::DateTime<chrono::Utc>,
    ),
    DesiredRead,
> {
    let raw = std::fs::read(crate::layout::desired_dir(root).join("envelope.json"))
        .map_err(|_| DesiredRead::Missing)?;
    let verified = snapshot_protocol::compute_desired::verify_compute_desired(
        &raw,
        cell_id,
        trusted_keys,
        guard,
        now,
    )
    .map_err(|e| DesiredRead::Invalid(e.code()))?;
    Ok((verified.payload, verified.expires_at))
}

#[cfg(test)]
mod tests {
    use super::*;
    use snapshot_protocol::compute_desired::ComputeDesired;
    use snapshot_protocol::{sign, SignParams, SigningKey};

    fn root() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn desired_cell(
        cell_id: &str,
        revision: u64,
    ) -> (ComputeDesired, chrono::DateTime<chrono::Utc>) {
        (
            ComputeDesired {
                schema_version: 1,
                cell_id: cell_id.into(),
                revision,
                tenants: vec![],
                deletions: vec![],
},
            chrono::Utc::now(),
        )
    }

    fn publish(dir: &std::path::Path, cell_id: &str, revision: u64, key: &SigningKey) -> Vec<u8> {
        let (payload, now) = desired_cell(cell_id, revision);
        let signed = sign(
            &SignParams {
                kind: "compute.desired",
                scope: &format!("cell:{cell_id}"),
                revision,
                issued_at: now,
                expires_at: now + chrono::Duration::hours(1),
                payload: &payload,
            },
            key,
        )
        .unwrap();
        std::fs::create_dir_all(crate::layout::desired_dir(dir)).unwrap();
        write_envelope(dir, &signed.bytes).unwrap();
        signed.bytes
    }

    fn authority(key: &SigningKey) -> (String, Vec<snapshot_protocol::TrustedKey>) {
        ("local".into(), vec![key.trusted_key()])
    }

    #[test]
    fn missing_envelope_is_missing_not_invalid() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        let (cell, keys) = authority(&key);
        let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
        assert_eq!(
            read_verified_desired(tmp.path(), &cell, &keys, &mut guard, chrono::Utc::now())
                .unwrap_err(),
            DesiredRead::Missing
        );
    }

    #[test]
    fn signed_envelope_reads_back_verified() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        publish(tmp.path(), "local", 3, &key);
        let (cell, keys) = authority(&key);
        let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
        let (desired, _) =
            read_verified_desired(tmp.path(), &cell, &keys, &mut guard, chrono::Utc::now())
                .unwrap();
        assert_eq!(desired.revision, 3);
        assert_eq!(desired.cell_id, "local");
    }

    #[test]
    fn tampered_envelope_is_invalid() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        publish(tmp.path(), "local", 3, &key);
        // Flip a payload byte in place — signature no longer binds.
        let path = crate::layout::desired_dir(tmp.path()).join("envelope.json");
        let mut raw = std::fs::read(&path).unwrap();
        let i = raw.len() / 2;
        raw[i] = if raw[i] == b'0' { b'1' } else { b'0' };
        std::fs::write(&path, raw).unwrap();
        let (cell, keys) = authority(&key);
        let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
        assert!(matches!(
            read_verified_desired(tmp.path(), &cell, &keys, &mut guard, chrono::Utc::now()),
            Err(DesiredRead::Invalid(_))
        ));
    }

    #[test]
    fn expired_envelope_is_invalid() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        publish(tmp.path(), "local", 3, &key);
        let (cell, keys) = authority(&key);
        let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
        // Read with `now` beyond the 1h expiry → expired → invalid.
        let far = chrono::Utc::now() + chrono::Duration::hours(2);
        assert!(matches!(
            read_verified_desired(tmp.path(), &cell, &keys, &mut guard, far),
            Err(DesiredRead::Invalid("SNAPSHOT_EXPIRED"))
        ));
    }

    #[test]
    fn wrong_cell_scope_is_invalid() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        publish(tmp.path(), "other-cell", 3, &key);
        let (cell, keys) = authority(&key); // verify binds cell:local
        let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
        assert!(matches!(
            read_verified_desired(tmp.path(), &cell, &keys, &mut guard, chrono::Utc::now()),
            Err(DesiredRead::Invalid(_))
        ));
    }

    #[test]
    fn rolled_back_revision_is_fenced() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        publish(tmp.path(), "local", 5, &key);
        let (cell, keys) = authority(&key);
        let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
        read_verified_desired(tmp.path(), &cell, &keys, &mut guard, chrono::Utc::now()).unwrap();
        // A same-process rewrite at a LOWER revision — the persistent
        // guard rejects it (rollback) even though the signature is genuine.
        publish(tmp.path(), "local", 4, &key);
        assert!(matches!(
            read_verified_desired(tmp.path(), &cell, &keys, &mut guard, chrono::Utc::now()),
            Err(DesiredRead::Invalid("SNAPSHOT_REVISION_ROLLBACK"))
        ));
    }

    #[test]
    fn expiry_ms_reads_rfc3339_wire() {
        let tmp = root();
        let key = SigningKey::from_seed(&[7u8; 32]).unwrap();
        publish(tmp.path(), "local", 1, &key);
        // Regression: the wire `expiresAt` is an RFC3339 string — the
        // old `as_i64()` read returned None for every real envelope.
        let ms = read_envelope_expiry_ms(tmp.path()).expect("expiry parses");
        assert!(ms > chrono::Utc::now().timestamp_millis());
    }
}
