//! TS↔Rust contract test: verify a `compute.desired` envelope produced by
//! the T7 control-plane TypeScript signer. The fixture is shared — do NOT
//! generate a parallel document here.

use cell_controller::verify::CellVerifier;
use chrono::{DateTime, Utc};
use snapshot_protocol::envelope::RevisionGuard;
use snapshot_protocol::TrustedKey;

#[test]
fn verifies_ts_signed_compute_desired() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../snapshot-protocol/fixtures/ts_compute_desired.json"
    ))
    .unwrap();
    let envelope = fixture["envelope"].as_str().unwrap().as_bytes().to_vec();
    let verify_at = DateTime::parse_from_rfc3339(fixture["verifyAt"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    let key = TrustedKey {
        key_id: fixture["keyId"].as_str().unwrap().to_string(),
        public_key: fixture["publicKey"].as_str().unwrap().to_string(),
    };

    // Through the controller's verifier — same path production uses.
    let mut verifier = CellVerifier::new("cell-001", vec![key]);
    let verified = verifier
        .verify(&envelope, verify_at)
        .expect("TS envelope must verify");
    assert_eq!(verified.revision, 7);
    assert_eq!(verified.payload.cell_id, "cell-001");
    assert_eq!(verified.payload.tenants.len(), 2);
    assert_eq!(verified.payload.tenants[0].workers.len(), 2);

    // Replayed identical envelope → idempotent accept (same digest).
    assert!(verifier.verify(&envelope, verify_at).is_ok());

    // And a fresh RevisionGuard accepts the same document too.
    let mut guard = RevisionGuard::new();
    let verified2 = snapshot_protocol::compute_desired::verify_compute_desired(
        &envelope,
        "cell-001",
        &[TrustedKey {
            key_id: fixture["keyId"].as_str().unwrap().to_string(),
            public_key: fixture["publicKey"].as_str().unwrap().to_string(),
        }],
        &mut guard,
        verify_at,
    )
    .unwrap();
    assert_eq!(verified2.payload_digest, verified.payload_digest);
}
