//! MemoryR2Provider semantics — put/get/head/delete, conditional
//! evaluation, ranges, checksums, delimiter-collapsing keyset list.

mod support;

use std::collections::BTreeMap;
use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::error::ProviderError;
use vrt_provider::r2::wire::{
    R2ChecksumAlgorithm, R2Condition, R2EtagMatch, R2PutOptions, R2Range,
};
use vrt_provider::r2::{MemoryR2Provider, R2GetResult, R2Provider};

fn ctx() -> vrt_provider::BindingContext {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(
                    WORKER,
                    VERSION,
                    DIGEST,
                    vec![r2_binding("BUCKET", BUCKET)],
                )],
            )],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let id = identity(WORKER, "BUCKET", WORKER, VERSION, DIGEST, BUCKET, 7);
    authorize(&auth, TENANT, &id, ResourceKind::R2).unwrap()
}

#[tokio::test]
async fn put_head_get_delete_cycle() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    let options = R2PutOptions {
        custom_metadata: BTreeMap::from([("a".to_string(), "1".to_string())]),
        ..Default::default()
    };
    let meta = provider
        .put(&ctx, "k1", b"body-bytes", &options)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.key, "k1");
    assert_eq!(meta.size, 10);
    assert!(meta.etag.len() == 32);
    assert_eq!(meta.http_etag, format!("\"{}\"", meta.etag));
    assert_eq!(meta.storage_class, "Standard");
    assert!(meta.checksums.md5.is_some());
    // version is a canonical uuidv7
    let version = uuid::Uuid::parse_str(&meta.version).unwrap();
    assert_eq!(version.get_version_num(), 7);

    let head = provider.head(&ctx, "k1").await.unwrap().unwrap();
    assert_eq!(head, meta);

    match provider.get(&ctx, "k1", None, None).await.unwrap() {
        R2GetResult::Body(m, bytes) => {
            assert_eq!(m.key, "k1");
            assert_eq!(bytes, b"body-bytes");
            assert!(m.range.is_none());
        }
        _ => panic!("expected body"),
    }

    provider.delete(&ctx, &["k1".to_string()]).await.unwrap();
    assert!(provider.head(&ctx, "k1").await.unwrap().is_none());
    match provider.get(&ctx, "k1", None, None).await.unwrap() {
        R2GetResult::Missing => {}
        _ => panic!("expected missing"),
    }
}

#[tokio::test]
async fn conditional_put() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    // etagMatches wildcard on missing object → precondition fails → None
    let options = R2PutOptions {
        only_if: Some(R2Condition {
            etag_matches: vec![R2EtagMatch::Wildcard],
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(provider
        .put(&ctx, "k", b"v", &options)
        .await
        .unwrap()
        .is_none());
    // etagDoesNotMatch wildcard on missing → ok
    let options = R2PutOptions {
        only_if: Some(R2Condition {
            etag_does_not_match: vec![R2EtagMatch::Wildcard],
            ..Default::default()
        }),
        ..Default::default()
    };
    let meta = provider
        .put(&ctx, "k", b"v1", &options)
        .await
        .unwrap()
        .unwrap();
    // now the object exists: IfNoneMatch fails
    assert!(provider
        .put(&ctx, "k", b"v2", &options)
        .await
        .unwrap()
        .is_none());
    // strong etag match succeeds
    let options = R2PutOptions {
        only_if: Some(R2Condition {
            etag_matches: vec![R2EtagMatch::Strong {
                value: meta.etag.clone(),
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(provider
        .put(&ctx, "k", b"v2", &options)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn conditional_get() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    let meta = provider
        .put(&ctx, "k", b"data", &R2PutOptions::default())
        .await
        .unwrap()
        .unwrap();
    // matching condition → body
    let ok = R2Condition {
        etag_matches: vec![R2EtagMatch::Wildcard],
        ..Default::default()
    };
    match provider.get(&ctx, "k", None, Some(&ok)).await.unwrap() {
        R2GetResult::Body(..) => {}
        _ => panic!("expected body"),
    }
    // failing condition → precondition metadata, no body
    let fail = R2Condition {
        etag_matches: vec![R2EtagMatch::Strong {
            value: "nope".to_string(),
        }],
        ..Default::default()
    };
    match provider.get(&ctx, "k", None, Some(&fail)).await.unwrap() {
        R2GetResult::Precondition(m) => assert_eq!(m.etag, meta.etag),
        _ => panic!("expected precondition"),
    }
}

#[tokio::test]
async fn ranged_get() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    provider
        .put(&ctx, "k", b"0123456789", &R2PutOptions::default())
        .await
        .unwrap();
    let range = R2Range {
        offset: Some(2),
        length: Some(4),
        suffix: None,
    };
    match provider.get(&ctx, "k", Some(range), None).await.unwrap() {
        R2GetResult::Body(m, bytes) => {
            assert_eq!(bytes, b"2345");
            assert_eq!(m.range.unwrap().offset, Some(2));
            assert_eq!(m.range.unwrap().length, Some(4));
        }
        _ => panic!("expected body"),
    }
    let suffix = R2Range {
        offset: None,
        length: None,
        suffix: Some(3),
    };
    match provider.get(&ctx, "k", Some(suffix), None).await.unwrap() {
        R2GetResult::Body(m, bytes) => {
            assert_eq!(bytes, b"789");
            assert_eq!(m.range.unwrap().offset, Some(7));
        }
        _ => panic!("expected body"),
    }
    // unsatisfiable → R2InvalidOptions
    let bad = R2Range {
        offset: Some(100),
        length: None,
        suffix: None,
    };
    assert!(matches!(
        provider.get(&ctx, "k", Some(bad), None).await,
        Err(ProviderError::R2InvalidOptions)
    ));
}

#[tokio::test]
async fn checksum_verification() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    let correct = vrt_provider::r2::wire::hash_bytes(b"payload");
    let options = R2PutOptions {
        checksum: Some(R2ChecksumAlgorithm::Sha256(correct.sha256)),
        ..Default::default()
    };
    let meta = provider
        .put(&ctx, "k", b"payload", &options)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        meta.checksums.sha256.as_deref(),
        Some(hex::encode(correct.sha256).as_str())
    );
    // mismatching checksum → R2ChecksumMismatch
    let options = R2PutOptions {
        checksum: Some(R2ChecksumAlgorithm::Md5([0u8; 16])),
        ..Default::default()
    };
    assert!(matches!(
        provider.put(&ctx, "k2", b"payload", &options).await,
        Err(ProviderError::R2ChecksumMismatch)
    ));
}

#[tokio::test]
async fn custom_metadata_cap() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    let big = BTreeMap::from([("k".to_string(), "x".repeat(5000))]);
    let options = R2PutOptions {
        custom_metadata: big,
        ..Default::default()
    };
    assert!(matches!(
        provider.put(&ctx, "k", b"v", &options).await,
        Err(ProviderError::R2MetadataTooLarge)
    ));
}

#[tokio::test]
async fn delimiter_collapse_and_keyset_resume() {
    let provider = MemoryR2Provider::new();
    let ctx = ctx();
    for key in ["a/1", "a/2", "a/3", "b/1", "c"] {
        provider
            .put(&ctx, key, b"v", &R2PutOptions::default())
            .await
            .unwrap();
    }
    // no delimiter
    let page = provider.list(&ctx, "", None, None, 10).await.unwrap();
    assert_eq!(page.objects.len(), 5);
    assert!(page.next_after.is_none());
    // delimiter "/" collapses a/*, b/*
    let page = provider.list(&ctx, "", Some("/"), None, 10).await.unwrap();
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, "c");
    assert_eq!(
        page.delimited_prefixes,
        vec!["a/".to_string(), "b/".to_string()]
    );
    // page through with limit 2: entries [a/, b/] then next_after=b/1
    let page = provider.list(&ctx, "", Some("/"), None, 2).await.unwrap();
    assert_eq!(
        page.delimited_prefixes,
        vec!["a/".to_string(), "b/".to_string()]
    );
    assert!(page.objects.is_empty());
    assert_eq!(page.next_after.as_deref(), Some("b/1"));
    let page = provider
        .list(&ctx, "", Some("/"), page.next_after.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, "c");
    assert!(page.next_after.is_none());
    // startAfter resume point
    let page = provider
        .list(&ctx, "", None, Some("a/2"), 10)
        .await
        .unwrap();
    assert_eq!(
        page.objects
            .iter()
            .map(|m| m.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a/3", "b/1", "c"]
    );
    // prefix filter
    let page = provider.list(&ctx, "a/", None, None, 10).await.unwrap();
    assert_eq!(page.objects.len(), 3);
}

#[tokio::test]
async fn buckets_are_isolated() {
    let provider = MemoryR2Provider::new();
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(
                    WORKER,
                    VERSION,
                    DIGEST,
                    vec![r2_binding("B1", "bucket-a"), r2_binding("B2", "bucket-b")],
                )],
            )],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let c1 = authorize(
        &auth,
        TENANT,
        &identity(WORKER, "B1", WORKER, VERSION, DIGEST, "bucket-a", 7),
        ResourceKind::R2,
    )
    .unwrap();
    let c2 = authorize(
        &auth,
        TENANT,
        &identity(WORKER, "B2", WORKER, VERSION, DIGEST, "bucket-b", 7),
        ResourceKind::R2,
    )
    .unwrap();
    provider
        .put(&c1, "k", b"aaa", &R2PutOptions::default())
        .await
        .unwrap();
    provider
        .put(&c2, "k", b"bbb", &R2PutOptions::default())
        .await
        .unwrap();
    match provider.get(&c1, "k", None, None).await.unwrap() {
        R2GetResult::Body(_, bytes) => assert_eq!(bytes, b"aaa"),
        _ => panic!(),
    }
    match provider.get(&c2, "k", None, None).await.unwrap() {
        R2GetResult::Body(_, bytes) => assert_eq!(bytes, b"bbb"),
        _ => panic!(),
    }
}
