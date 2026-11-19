//! Shared digest vectors (`shared/vrt-bundle-digest-vectors.json`) —
//! the TypeScript builder and `VersionBundle::from_pack` must agree
//! byte-for-byte on digest AND storedDocument for identical pack
//! inputs. The file is regenerated only via `VRT_WRITE_VECTORS=1` in
//! `vrtBundleDigestVectors.test.ts`.

use serde_json::Value;

const VECTORS: &str = include_str!("../../shared/vrt-bundle-digest-vectors.json");

#[test]
fn rust_from_pack_matches_ts_bytes() {
    let doc: Value = serde_json::from_str(VECTORS).unwrap();
    let cases = doc["cases"].as_array().unwrap();
    assert!(cases.len() >= 4, "vector file shrank");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let bundle = vrt_config::VersionBundle::from_pack(&case["pack"]).expect(name);
        assert_eq!(bundle.digest(), case["digest"].as_str().unwrap(), "{name}");
        use base64::Engine;
        let expected = base64::engine::general_purpose::STANDARD
            .decode(case["storedDocumentBase64"].as_str().unwrap())
            .unwrap();
        assert_eq!(bundle.stored_document(), expected.as_slice(), "{name}");
    }
}

/// The pack→stored path is also pinned against the older
/// `vrt-config` TS contract fixture (independent regeneration point).
#[test]
fn all_forms_vector_matches_contract_fixture() {
    let doc: Value = serde_json::from_str(VECTORS).unwrap();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../vrt-config/fixtures/ts_version_bundle.json"
    ))
    .unwrap();
    let all_forms = doc["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "all-module-forms")
        .unwrap();
    assert_eq!(
        all_forms["digest"].as_str().unwrap(),
        fixture["digest"].as_str().unwrap()
    );
}
