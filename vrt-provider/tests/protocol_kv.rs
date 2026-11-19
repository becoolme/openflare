//! KV wire-format conformance tests — KVS1/KVB1 frames, put frames,
//! canonical metadata, key grammar, and list responses. Semantics are
//! re-implemented from the upstream contract, not copied code.

mod support;

use serde_json::json;
use vrt_provider::kv::{self, KvEntry, KvListRow, KvPutOptions};

fn entry(value: &[u8], meta: Option<&[u8]>, expires: Option<i64>) -> KvEntry {
    KvEntry {
        value: value.to_vec(),
        metadata_json: meta.map(|m| m.to_vec()),
        expires_at_ms: expires,
    }
}

#[test]
fn kvs1_present_entry_layout() {
    let e = entry(b"hello", Some(b"{\"a\":1}"), Some(1_700_000_000_000));
    let frame = kv::encode_kvs1(Some(&e));
    assert_eq!(&frame[..4], b"KVS1");
    assert_eq!(frame[4], 1);
    assert_eq!(&frame[5..13], &1_700_000_000_000_i64.to_be_bytes());
    let meta_len = u32::from_be_bytes(frame[13..17].try_into().unwrap()) as usize;
    assert_eq!(&frame[17..17 + meta_len], b"{\"a\":1}");
    let value_len_off = 17 + meta_len;
    let value_len =
        u32::from_be_bytes(frame[value_len_off..value_len_off + 4].try_into().unwrap()) as usize;
    assert_eq!(
        &frame[value_len_off + 4..value_len_off + 4 + value_len],
        b"hello"
    );
    assert_eq!(frame.len(), value_len_off + 4 + value_len);

    let (decoded, consumed) = kv::decode_entry(&frame[4..]).unwrap();
    assert_eq!(decoded, Some(e));
    assert_eq!(consumed, frame.len() - 4);
}

#[test]
fn kvs1_absent_entry_layout() {
    let frame = kv::encode_kvs1(None);
    assert_eq!(&frame[..4], b"KVS1");
    assert_eq!(frame.len(), 4 + 17);
    assert_eq!(frame[4], 0);
    assert_eq!(&frame[5..13], &(-1_i64).to_be_bytes());
    assert_eq!(&frame[13..17], &u32::MAX.to_be_bytes());
    assert_eq!(&frame[17..21], &u32::MAX.to_be_bytes());
    let (decoded, consumed) = kv::decode_entry(&frame[4..]).unwrap();
    assert_eq!(decoded, None);
    assert_eq!(consumed, 17);
}

#[test]
fn kvb1_multi_entry_layout() {
    let entries = vec![
        Some(entry(b"v1", None, None)),
        None,
        Some(entry(b"", Some(b"null"), Some(-5))),
    ];
    let frame = kv::encode_kvb1(&entries);
    assert_eq!(&frame[..4], b"KVB1");
    assert_eq!(u16::from_be_bytes(frame[4..6].try_into().unwrap()), 3);
    let mut rest = &frame[6..];
    let (d1, c1) = kv::decode_entry(rest).unwrap();
    assert_eq!(d1, entries[0].clone());
    rest = &rest[c1..];
    let (d2, c2) = kv::decode_entry(rest).unwrap();
    assert_eq!(d2, None);
    rest = &rest[c2..];
    let (d3, _) = kv::decode_entry(rest).unwrap();
    assert_eq!(d3, entries[2].clone());
}

#[test]
fn decode_entry_rejects_truncation() {
    assert!(kv::decode_entry(&[]).is_none());
    assert!(kv::decode_entry(&[1]).is_none());
    let mut frame = kv::encode_kvs1(Some(&entry(b"value", Some(b"{}"), None)));
    frame.truncate(frame.len() - 2);
    assert!(kv::decode_entry(&frame[4..]).is_none());
    // Absent-entry with wrong sentinel bytes is rejected.
    assert!(kv::decode_entry(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
}

#[test]
fn put_frame_round_trip() {
    let header = json!({
        "key": "k1",
        "expiration": 2_000_000_000u64,
        "metadata": {"b": 2, "a": 1},
        "metadataPresent": true,
    });
    let header_bytes = serde_json::to_vec(&header).unwrap();
    let mut frame = (header_bytes.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&header_bytes);
    frame.extend_from_slice(b"value-bytes");

    let (parsed, value) = kv::parse_put_frame(&frame).unwrap();
    assert_eq!(parsed.key, "k1");
    assert_eq!(parsed.expiration, Some(2_000_000_000));
    assert!(parsed.metadata_present);
    assert_eq!(value, b"value-bytes");
}

#[test]
fn put_frame_rejects_malformed() {
    assert!(kv::parse_put_frame(b"").is_err());
    assert!(kv::parse_put_frame(b"\x00\x00\x00").is_err());
    // header len exceeding 4096
    let mut frame = (5000u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&vec![b' '; 5000]);
    assert!(matches!(
        kv::parse_put_frame(&frame),
        Err(vrt_provider::ProviderError::KvInternalProtocolError)
    ));
    // header len beyond body
    let mut frame = (100u32).to_be_bytes().to_vec();
    frame.extend_from_slice(b"{}");
    assert!(matches!(
        kv::parse_put_frame(&frame),
        Err(vrt_provider::ProviderError::KvInternalProtocolError)
    ));
    // non-JSON header
    let mut frame = (3u32).to_be_bytes().to_vec();
    frame.extend_from_slice(b"nope");
    assert!(kv::parse_put_frame(&frame).is_err());
}

#[test]
fn put_frame_key_validation_and_expiration() {
    // empty key
    let header = json!({"key": ""});
    let hb = serde_json::to_vec(&header).unwrap();
    let mut frame = (hb.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&hb);
    assert!(matches!(
        kv::parse_put_frame(&frame),
        Err(vrt_provider::ProviderError::KvKeyInvalid)
    ));

    let now = 1_700_000_000_000i64;
    // expiration_ttl < 60 rejected
    let hdr: kv::FramePutHeader = serde_json::from_value(json!({
        "key": "k", "expirationTtl": 30
    }))
    .unwrap();
    assert!(matches!(
        kv::prepare_put_options(now, &hdr),
        Err(vrt_provider::ProviderError::KvInvalidOptions)
    ));
    // expiration < now + 60s rejected
    let hdr: kv::FramePutHeader = serde_json::from_value(json!({
        "key": "k", "expiration": (now / 1000 + 30) as u64
    }))
    .unwrap();
    assert!(matches!(
        kv::prepare_put_options(now, &hdr),
        Err(vrt_provider::ProviderError::KvInvalidOptions)
    ));
    // valid ttl → now + ttl*1000
    let hdr: kv::FramePutHeader = serde_json::from_value(json!({
        "key": "k", "expirationTtl": 120
    }))
    .unwrap();
    let options = kv::prepare_put_options(now, &hdr).unwrap();
    assert_eq!(options.expires_at_ms, Some(now + 120_000));
    // metadataPresent without metadata → canonical "null"
    let hdr: kv::FramePutHeader = serde_json::from_value(json!({
        "key": "k", "metadataPresent": true
    }))
    .unwrap();
    let options = kv::prepare_put_options(now, &hdr).unwrap();
    assert_eq!(options.metadata_json.as_deref(), Some(b"null".as_slice()));
    // no metadataPresent → None
    let hdr: kv::FramePutHeader = serde_json::from_value(json!({"key": "k"})).unwrap();
    let options = kv::prepare_put_options(now, &hdr).unwrap();
    assert_eq!(options, KvPutOptions::default());
}

#[test]
fn key_grammar() {
    assert!(matches!(
        kv::validate_key(""),
        Err(vrt_provider::ProviderError::KvKeyInvalid)
    ));
    assert!(matches!(
        kv::validate_key("."),
        Err(vrt_provider::ProviderError::KvKeyInvalid)
    ));
    assert!(matches!(
        kv::validate_key(".."),
        Err(vrt_provider::ProviderError::KvKeyInvalid)
    ));
    let long = "x".repeat(513);
    assert!(matches!(
        kv::validate_key(&long),
        Err(vrt_provider::ProviderError::KvKeyTooLarge)
    ));
    let max = "x".repeat(512);
    assert_eq!(kv::validate_key(&max).unwrap().len(), 512);
    // UTF-8 multibyte at the byte boundary: 256 × 2-byte chars = 512 ✓
    let multibyte = "é".repeat(256);
    assert_eq!(multibyte.len(), 512);
    assert!(kv::validate_key(&multibyte).is_ok());
}

#[test]
fn canonical_metadata_rules() {
    // key order canonicalized
    let meta = kv::canonical_metadata(&json!({"b": 1, "a": [true, null, {"z":0,"y":1}]})).unwrap();
    assert_eq!(meta, br#"{"a":[true,null,{"y":1,"z":0}],"b":1}"#.as_slice());
    // string metadata canonicalized
    assert_eq!(kv::canonical_metadata(&json!("s")).unwrap(), b"\"s\"");
    // >1024 canonical bytes rejected
    let big = json!({"k": "x".repeat(2000)});
    assert!(matches!(
        kv::canonical_metadata(&big),
        Err(vrt_provider::ProviderError::KvMetadataTooLarge)
    ));
}

#[test]
fn list_response_shape() {
    let rows = vec![
        KvListRow {
            key: b"a".to_vec(),
            metadata_json: None,
            expires_at_ms: None,
        },
        KvListRow {
            key: "键".as_bytes().to_vec(),
            metadata_json: Some(b"{\"x\":1}".to_vec()),
            expires_at_ms: Some(2_000_000_000_000),
        },
    ];
    let bytes = kv::encode_list_response(&rows, false, Some("cursor-token")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["keys"].as_array().unwrap().len(), 2);
    assert_eq!(value["keys"][0]["name"], "a");
    assert!(value["keys"][0]["metadata"].is_null());
    assert!(value["keys"][0]["expiration"].is_null());
    assert_eq!(value["keys"][1]["name"], "键");
    assert_eq!(value["keys"][1]["expiration"], 2_000_000_000);
    assert_eq!(value["list_complete"], false);
    assert_eq!(value["cursor"], "cursor-token");
    // non-UTF8 key → KvCorrupt
    let bad = vec![KvListRow {
        key: vec![0xff],
        metadata_json: None,
        expires_at_ms: None,
    }];
    assert!(matches!(
        kv::encode_list_response(&bad, true, None),
        Err(vrt_provider::ProviderError::KvCorrupt)
    ));
}

#[test]
fn frame_content_type_constant() {
    assert_eq!(
        vrt_provider::limits::KV_FRAME_CONTENT_TYPE,
        "application/vnd.open-compute.kv.v1+frame"
    );
}
