//! Cross-implementation contract: the signed `compute.desired` envelope
//! produced by the TypeScript control plane
//! (`server/src/services/workerRuntime/vrtContractFixture.test.ts`) must
//! verify under this crate and decode as a valid [`ComputeDesired`].
//!
//! The fixture is committed at `fixtures/ts_compute_desired.json` and
//! regenerated deterministically — any drift in canonicalization, envelope
//! member set, digests, or payload shape fails either this test or the
//! TypeScript drift test.

use chrono::{DateTime, Utc};
use serde_json::Value;
use snapshot_protocol::compute_desired::ComputeDesired;
use snapshot_protocol::{verify, ExpectedScope, TrustedKey};

#[test]
fn ts_generated_compute_desired_verifies_and_validates() {
    let fixture: Value = serde_json::from_str(include_str!("../fixtures/ts_compute_desired.json"))
        .expect("fixture parses");
    let envelope = fixture["envelope"].as_str().expect("envelope string");
    let verify_at: DateTime<Utc> =
        DateTime::parse_from_rfc3339(fixture["verifyAt"].as_str().expect("verifyAt string"))
            .expect("verifyAt is rfc3339")
            .with_timezone(&Utc);
    let key = TrustedKey {
        key_id: fixture["keyId"].as_str().unwrap().to_owned(),
        public_key: fixture["publicKey"].as_str().unwrap().to_owned(),
    };

    // Full verification: strict parse, member set, versions, kind/scope,
    // key-id binding, signature over canonical unsigned bytes, lifetime.
    let verified = verify::<ComputeDesired>(
        envelope.as_bytes(),
        &ExpectedScope {
            kind: "compute.desired",
            scope: "cell:cell-001",
        },
        &[key],
        verify_at,
    )
    .expect("TS-produced envelope verifies");

    // Semantic validation of the payload itself (bounds, names, references).
    verified.payload.validate().expect("payload validates");

    assert_eq!(verified.revision, 7);
    assert_eq!(verified.scope, "cell:cell-001");
    assert_eq!(verified.payload.tenants.len(), 2);
    assert_eq!(
        verified.payload.tenants[0].tenant_id, "team-001",
        "tenants are sorted by tenantId"
    );
}
