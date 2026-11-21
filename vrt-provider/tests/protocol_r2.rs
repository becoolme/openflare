//! R2 wire-format tests — metadata DTO shape, get frame, range and
//! conditional semantics, checksums, list cursor payload.

mod support;

use serde_json::json;
use sha2::Digest;
use std::collections::BTreeMap;
use vrt_provider::cursor::CursorSigner;
use vrt_provider::r2::wire::{
    self, PutWireOptions, R2ChecksumAlgorithm, R2Condition, R2EtagMatch, R2HttpMetadata,
    R2ObjectMetadata, R2Range,
};
use vrt_provider::ProviderError;

fn meta() -> R2ObjectMetadata {
    R2ObjectMetadata {
        key: "dir/file.txt".to_string(),
        version: "01989abc-def0-7abc-8000-000000000000".to_string(),
        size: 42,
        etag: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
        http_etag: "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string(),
        uploaded: 1_700_000_000_000,
        http_metadata: Some(R2HttpMetadata {
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        }),
        custom_metadata: Some(BTreeMap::from([("k".to_string(), "v".to_string())])),
        range: None,
        checksums: wire::R2Checksums {
            md5: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
            ..Default::default()
        },
        storage_class: "Standard".to_string(),
        ssec_key_md5: None,
    }
}

#[test]
fn object_metadata_json_shape() {
    let value = serde_json::to_value(meta()).unwrap();
    // camelCase upstream field names
    for field in [
        "key",
        "version",
        "size",
        "etag",
        "httpEtag",
        "uploaded",
        "httpMetadata",
        "customMetadata",
        "checksums",
        "storageClass",
    ] {
        assert!(value.get(field).is_some(), "missing field {field}");
    }
    assert_eq!(value["httpMetadata"]["contentType"], "text/plain");
    assert_eq!(value["customMetadata"]["k"], "v");
    assert_eq!(
        value["checksums"]["md5"],
        "d41d8cd98f00b204e9800998ecf8427e"
    );
    // round trip through strict decode
    let decoded: R2ObjectMetadata = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, meta());
}

#[test]
fn range_shape_and_resolution() {
    // offset+length
    let r = R2Range {
        offset: Some(2),
        length: Some(5),
        suffix: None,
    };
    assert!(wire::validate_range_shape(r).is_ok());
    assert_eq!(wire::resolve_range(r, 10), Ok((2, 6)));
    // clamps at size
    let r = R2Range {
        offset: Some(8),
        length: Some(10),
        suffix: None,
    };
    assert_eq!(wire::resolve_range(r, 10), Ok((8, 9)));
    // offset to end
    let r = R2Range {
        offset: Some(4),
        length: None,
        suffix: None,
    };
    assert_eq!(wire::resolve_range(r, 10), Ok((4, 9)));
    // length from start
    let r = R2Range {
        offset: None,
        length: Some(3),
        suffix: None,
    };
    assert_eq!(wire::resolve_range(r, 10), Ok((0, 2)));
    // suffix
    let r = R2Range {
        offset: None,
        length: None,
        suffix: Some(4),
    };
    assert_eq!(wire::resolve_range(r, 10), Ok((6, 9)));
    let r = R2Range {
        offset: None,
        length: None,
        suffix: Some(100),
    };
    assert_eq!(wire::resolve_range(r, 10), Ok((0, 9)));
    // invalid shapes
    for bad in [
        R2Range {
            offset: None,
            length: None,
            suffix: None,
        },
        R2Range {
            offset: Some(1),
            length: Some(1),
            suffix: Some(1),
        },
        R2Range {
            offset: Some(1),
            length: Some(0),
            suffix: None,
        },
        R2Range {
            offset: None,
            length: Some(0),
            suffix: None,
        },
        R2Range {
            offset: None,
            length: None,
            suffix: Some(0),
        },
        R2Range {
            offset: None,
            length: Some(2),
            suffix: Some(2),
        },
    ] {
        assert!(
            matches!(
                wire::validate_range_shape(bad),
                Err(ProviderError::R2InvalidOptions)
            ),
            "{bad:?}"
        );
    }
    // out-of-bounds and empty object
    let r = R2Range {
        offset: Some(10),
        length: None,
        suffix: None,
    };
    assert!(matches!(
        wire::resolve_range(r, 10),
        Err(ProviderError::R2InvalidOptions)
    ));
    let r = R2Range {
        offset: Some(0),
        length: None,
        suffix: None,
    };
    assert!(matches!(
        wire::resolve_range(r, 0),
        Err(ProviderError::R2InvalidOptions)
    ));
}

#[test]
fn condition_semantics() {
    let etag = "abc";
    let uploaded = 1_700_000_000_000i64;
    // etagMatches wildcard always matches
    let c = R2Condition {
        etag_matches: vec![R2EtagMatch::Wildcard],
        ..Default::default()
    };
    assert!(c.matches_object(etag, uploaded));
    // strong match
    let c = R2Condition {
        etag_matches: vec![R2EtagMatch::Strong {
            value: "abc".into(),
        }],
        ..Default::default()
    };
    assert!(c.matches_object(etag, uploaded));
    let c = R2Condition {
        etag_matches: vec![R2EtagMatch::Strong {
            value: "other".into(),
        }],
        ..Default::default()
    };
    assert!(!c.matches_object(etag, uploaded));
    // weak validator does not satisfy etagMatches (strong compare)
    let c = R2Condition {
        etag_matches: vec![R2EtagMatch::Weak {
            value: "abc".into(),
        }],
        ..Default::default()
    };
    assert!(!c.matches_object(etag, uploaded));
    // etagDoesNotMatch: weak compare — weak match counts as match → fails
    let c = R2Condition {
        etag_does_not_match: vec![R2EtagMatch::Weak {
            value: "abc".into(),
        }],
        ..Default::default()
    };
    assert!(!c.matches_object(etag, uploaded));
    let c = R2Condition {
        etag_does_not_match: vec![R2EtagMatch::Strong {
            value: "zzz".into(),
        }],
        ..Default::default()
    };
    assert!(c.matches_object(etag, uploaded));
    // uploadedBefore/After
    let c = R2Condition {
        uploaded_before: Some(uploaded),
        ..Default::default()
    };
    assert!(c.matches_object(etag, uploaded));
    let c = R2Condition {
        uploaded_before: Some(uploaded - 1),
        ..Default::default()
    };
    assert!(!c.matches_object(etag, uploaded));
    let c = R2Condition {
        uploaded_after: Some(uploaded - 1),
        ..Default::default()
    };
    assert!(c.matches_object(etag, uploaded));
    // seconds granularity
    let c = R2Condition {
        uploaded_after: Some(uploaded + 500),
        seconds_granularity: true,
        ..Default::default()
    };
    assert!(!c.matches_object(etag, uploaded));
    // matches_missing: only etagDoesNotMatch/uploadedBefore pass on absent object
    assert!(R2Condition::default().matches_missing());
    let c = R2Condition {
        uploaded_after: Some(1),
        ..Default::default()
    };
    assert!(!c.matches_missing());
    let c = R2Condition {
        etag_matches: vec![R2EtagMatch::Wildcard],
        ..Default::default()
    };
    assert!(!c.matches_missing());
}

#[test]
fn checksum_parse_and_verify() {
    let checksums = wire::hash_bytes(b"data");
    assert_eq!(
        hex::encode(checksums.md5),
        "8d777f385d3dfec8815d20f7496026dc"
    );
    assert!(checksums.matches(&R2ChecksumAlgorithm::Md5(checksums.md5)));
    assert!(!checksums.matches(&R2ChecksumAlgorithm::Sha256([0u8; 32])));

    // wire: valid md5 checksum option
    let options: PutWireOptions = serde_json::from_value(json!({
        "checksum": {"algorithm": "md5", "hex": hex::encode(checksums.md5)}
    }))
    .unwrap();
    let parsed: vrt_provider::r2::R2PutOptions = options.try_into().unwrap();
    assert!(matches!(parsed.checksum, Some(R2ChecksumAlgorithm::Md5(_))));
    // invalid hex → R2InvalidOptions
    let options: PutWireOptions = serde_json::from_value(json!({
        "checksum": {"algorithm": "md5", "hex": "zz"}
    }))
    .unwrap();
    assert!(matches!(
        <PutWireOptions as TryInto<vrt_provider::r2::R2PutOptions>>::try_into(options),
        Err(ProviderError::R2InvalidOptions)
    ));
    // ssec: workerd forwards a 32-byte key as 64 lowercase hex chars —
    // accepted and carried into the backend (never silently dropped).
    let ssec_hex = "ab".repeat(32);
    let options: PutWireOptions = serde_json::from_value(json!({
        "ssecKey": ssec_hex
    }))
    .unwrap();
    let parsed: vrt_provider::r2::R2PutOptions = options.try_into().unwrap();
    assert_eq!(
        parsed.ssec_key,
        Some(vrt_provider::r2::R2SsecKey::parse(&ssec_hex).unwrap())
    );
    // Malformed key material (wrong length, non-hex/base64 input) is
    // rejected at the typed boundary.
    let base64_form =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0xabu8; 32]);
    for bad in [
        "ab".repeat(31), // 62 chars
        "ab".repeat(33), // 66 chars
        "zz".repeat(32), // not hex
        "A".repeat(64),  // workerd only forwards lowercase hex
        "a".repeat(63),  // odd length
        base64_form,     // 44-char base64, not the wire form
    ] {
        let options: PutWireOptions = serde_json::from_value(json!({
            "ssecKey": bad
        }))
        .unwrap();
        assert!(
            matches!(
                <PutWireOptions as TryInto<vrt_provider::r2::R2PutOptions>>::try_into(options),
                Err(ProviderError::R2SsecInvalid)
            ),
            "accepted malformed SSE-C key {bad:?}"
        );
    }
    // Get options decode the same wire form.
    let get: wire::GetOptions = serde_json::from_value(json!({
        "ssecKey": ssec_hex
    }))
    .unwrap();
    assert_eq!(get.ssec_key.as_deref(), Some(ssec_hex.as_str()));
    // storage class parse
    let options: PutWireOptions = serde_json::from_value(json!({
        "storageClass": "InfrequentAccess"
    }))
    .unwrap();
    let parsed: vrt_provider::r2::R2PutOptions = options.try_into().unwrap();
    assert_eq!(parsed.storage_class.as_str(), "InfrequentAccess");
    let options: PutWireOptions = serde_json::from_value(json!({
        "storageClass": "Glacier"
    }))
    .unwrap();
    assert!(matches!(
        <PutWireOptions as TryInto<vrt_provider::r2::R2PutOptions>>::try_into(options),
        Err(ProviderError::R2InvalidOptions)
    ));
}

#[test]
fn r2_cursor_sign_verify_scope() {
    let signer = CursorSigner::new(b"test-cursor-secret");
    let payload = wire::R2CursorPayload {
        v: 1,
        resource_id: CursorSigner::r2_resource_id("tenant-a", "bucket-1"),
        generation: 7,
        prefix_sha256: hex::encode(sha2::Sha256::digest(b"pre/")),
        delimiter_sha256: hex::encode(sha2::Sha256::digest(b"/")),
        include_mask: 3,
        start_after_sha256: hex::encode(sha2::Sha256::digest(b"")),
        after_key: Some("pre/k9".to_string()),
        expires_at_ms: chrono::Utc::now().timestamp_millis() + 60_000,
    };
    let cursor = signer.sign_r2(&payload).unwrap();
    let decoded = signer.verify_r2(&cursor).unwrap();
    assert_eq!(decoded.after_key.as_deref(), Some("pre/k9"));
    assert_eq!(decoded.generation, 7);

    // tampered payload rejected
    let (body, sig) = cursor.split_once('.').unwrap();
    let mut raw =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, body).unwrap();
    raw[3] ^= 1;
    let forged = format!(
        "{}.{sig}",
        base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, raw)
    );
    assert!(signer.verify_r2(&forged).is_err());
    // wrong signer rejected
    let other = CursorSigner::new(b"other-secret");
    assert!(other.verify_r2(&cursor).is_err());
    // expired rejected
    let mut expired = payload.clone();
    expired.expires_at_ms = 1;
    let cursor = signer.sign_r2(&expired).unwrap();
    assert!(signer.verify_r2(&cursor).is_err());
    // wrong version
    let mut bad = payload.clone();
    bad.v = 9;
    let cursor = signer.sign_r2(&bad).unwrap();
    assert!(signer.verify_r2(&cursor).is_err());
    // malformed
    assert!(signer.verify_r2("not-a-cursor").is_err());
    assert!(signer.verify_r2("a.b.c").is_err());
}

#[test]
fn kv_cursor_scope_and_replay() {
    let signer = CursorSigner::new(b"kvc");
    let cursor = signer.sign_kv("t1", "ns1", b"p/", b"k7");
    // replay in same scope works
    let after = signer.verify_kv("t1", "ns1", b"p/", &cursor).unwrap();
    assert_eq!(after.as_slice(), b"k7");
    // cross-namespace replay rejected
    assert!(signer.verify_kv("t1", "ns2", b"p/", &cursor).is_err());
    // cross-tenant replay rejected
    assert!(signer.verify_kv("t2", "ns1", b"p/", &cursor).is_err());
    // different prefix rejected
    assert!(signer.verify_kv("t1", "ns1", b"other/", &cursor).is_err());
    // expired rejected
    let now = chrono::Utc::now().timestamp_millis();
    let expired = signer.sign_kv_with_times("t1", "ns1", b"", b"k", now - 1000, now - 1);
    assert!(signer.verify_kv("t1", "ns1", b"", &expired).is_err());
    // oversized rejected
    let huge = signer.sign_kv("t1", "ns1", b"", &vec![b'x'; 5000]);
    assert!(signer.verify_kv("t1", "ns1", b"", &huge).is_err());
    // malformed rejected
    assert!(signer.verify_kv("t1", "ns1", b"", "%%%").is_err());
}

#[test]
fn include_mask_and_list_json() {
    let mask =
        wire::include_mask(&["httpMetadata".to_string(), "customMetadata".to_string()]).unwrap();
    assert_eq!(mask, 3);
    assert_eq!(wire::include_mask(&[]).unwrap(), 0);
    assert!(matches!(
        wire::include_mask(&["bogus".to_string()]),
        Err(ProviderError::R2InvalidOptions)
    ));
    let json = wire::list_object_json(meta(), 0);
    assert!(json.get("httpMetadata").is_none());
    assert!(json.get("customMetadata").is_none());
    assert!(json.get("key").is_some());
    let json = wire::list_object_json(meta(), 1);
    assert!(json.get("httpMetadata").is_some());
    assert!(json.get("customMetadata").is_none());
}
