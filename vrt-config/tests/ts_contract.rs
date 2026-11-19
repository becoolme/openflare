//! Cross-implementation contract: the `VersionBundle` stored document
//! produced by the TypeScript bundle builder
//! (`server/src/services/workerRuntime/vrtContractFixture.test.ts`) must
//! re-parse and re-verify under this crate — every module digest and the
//! bundle digest are recomputed from the stored content.

use serde_json::Value;
use vrt_config::VersionBundle;

#[test]
fn ts_generated_version_bundle_reverifies() {
    let fixture: Value = serde_json::from_str(include_str!("../fixtures/ts_version_bundle.json"))
        .expect("fixture parses");
    let stored = fixture["storedDocument"]
        .as_str()
        .expect("storedDocument string");

    // parse_stored re-validates the manifest AND recomputes the digest —
    // corrupted or swapped content fails closed.
    let bundle = VersionBundle::parse_stored(stored.as_bytes()).expect("stored doc reverifies");

    assert_eq!(bundle.digest(), fixture["digest"].as_str().unwrap());
    assert_eq!(bundle.object_key(), fixture["objectKey"].as_str().unwrap());
    assert_eq!(
        bundle.object_key(),
        format!("workers/v1/bundles/{}", bundle.digest())
    );
    assert_eq!(bundle.manifest().main_module, "index.js");
    // All six module wire forms exercised by the TS fixture.
    assert_eq!(bundle.module_digests().len(), 6);
}
