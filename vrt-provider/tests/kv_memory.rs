//! MemoryKvProvider semantics — expiry, multi-get caps, atomic
//! value+metadata, keyset list, corrupt-detection paths.

mod support;

use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::error::ProviderError;
use vrt_provider::kv::{KvProvider, KvPutOptions, MemoryKvProvider};

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
                    vec![kv_binding("KV", NS_ID)],
                )],
            )],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap()
}

#[tokio::test]
async fn put_get_delete_cycle() {
    let provider = MemoryKvProvider::new();
    let ctx = ctx();
    let options = KvPutOptions {
        expires_at_ms: None,
        metadata_json: Some(b"{\"a\":1}".to_vec()),
    };
    provider.put(&ctx, b"k1", b"v1", &options).await.unwrap();
    let entry = provider.get(&ctx, b"k1").await.unwrap().unwrap();
    assert_eq!(entry.value, b"v1");
    assert_eq!(
        entry.metadata_json.as_deref(),
        Some(b"{\"a\":1}".as_slice())
    );
    provider.delete(&ctx, b"k1").await.unwrap();
    assert!(provider.get(&ctx, b"k1").await.unwrap().is_none());
    // delete of missing key is idempotent
    provider.delete(&ctx, b"k1").await.unwrap();
}

#[tokio::test]
async fn expired_entries_read_as_absent() {
    let provider = MemoryKvProvider::new();
    let ctx = ctx();
    let past = chrono::Utc::now().timestamp_millis() - 1;
    provider
        .put(
            &ctx,
            b"gone",
            b"v",
            &KvPutOptions {
                expires_at_ms: Some(past),
                metadata_json: None,
            },
        )
        .await
        .unwrap();
    assert!(provider.get(&ctx, b"gone").await.unwrap().is_none());
    let page = provider.list(&ctx, "", None, 10).await.unwrap();
    assert!(page.rows.is_empty() && page.complete);
}

#[tokio::test]
async fn namespaces_are_isolated() {
    let provider = MemoryKvProvider::new();
    let f = fixture();
    // two namespaces, two bindings on the same worker
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
                    vec![kv_binding("KV", "ns-1"), kv_binding("KV2", "ns-2")],
                )],
            )],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let ctx1 = authorize(
        &auth,
        TENANT,
        &identity(WORKER, "KV", WORKER, VERSION, DIGEST, "ns-1", 7),
        ResourceKind::Kv,
    )
    .unwrap();
    let ctx2 = authorize(
        &auth,
        TENANT,
        &identity(WORKER, "KV2", WORKER, VERSION, DIGEST, "ns-2", 7),
        ResourceKind::Kv,
    )
    .unwrap();
    provider
        .put(&ctx1, b"k", b"one", &KvPutOptions::default())
        .await
        .unwrap();
    provider
        .put(&ctx2, b"k", b"two", &KvPutOptions::default())
        .await
        .unwrap();
    assert_eq!(
        provider.get(&ctx1, b"k").await.unwrap().unwrap().value,
        b"one"
    );
    assert_eq!(
        provider.get(&ctx2, b"k").await.unwrap().unwrap().value,
        b"two"
    );
    assert!(provider
        .list(&ctx1, "", None, 10)
        .await
        .unwrap()
        .rows
        .iter()
        .all(|r| r.key == b"k"));
}

#[tokio::test]
async fn get_many_ordering_and_caps() {
    let provider = MemoryKvProvider::new();
    let ctx = ctx();
    provider
        .put(&ctx, b"a", b"1", &KvPutOptions::default())
        .await
        .unwrap();
    provider
        .put(&ctx, b"c", b"3", &KvPutOptions::default())
        .await
        .unwrap();
    let keys = vec![b"c".to_vec(), b"missing".to_vec(), b"a".to_vec()];
    let entries = provider.get_many(&ctx, &keys).await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].as_ref().unwrap().value, b"3");
    assert!(entries[1].is_none());
    assert_eq!(entries[2].as_ref().unwrap().value, b"1");
}

#[tokio::test]
async fn list_keyset_semantics() {
    let provider = MemoryKvProvider::new();
    let ctx = ctx();
    for key in ["a/1", "a/2", "a/3", "b/1", "c"] {
        provider
            .put(&ctx, key.as_bytes(), b"v", &KvPutOptions::default())
            .await
            .unwrap();
    }
    // prefix filter
    let page = provider.list(&ctx, "a/", None, 10).await.unwrap();
    assert_eq!(
        page.rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        vec![b"a/1".to_vec(), b"a/2".to_vec(), b"a/3".to_vec()]
    );
    assert!(page.complete);
    // keyset resume: after = "a/2" yields only a/3
    let page = provider
        .list(&ctx, "a/", Some(b"a/2".as_slice()), 10)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].key, b"a/3");
    // truncation: limit+1 probing
    let page = provider.list(&ctx, "", None, 2).await.unwrap();
    assert_eq!(page.rows.len(), 2);
    assert!(!page.complete);
    // invalid after: >512 or non-utf8 → KvCursorInvalid
    assert!(matches!(
        provider.list(&ctx, "", Some(&[0xffu8][..]), 10).await,
        Err(ProviderError::KvCursorInvalid)
    ));
    let big = vec![b'x'; 513];
    assert!(matches!(
        provider.list(&ctx, "", Some(&big), 10).await,
        Err(ProviderError::KvCursorInvalid)
    ));
    // limit bounds
    assert!(matches!(
        provider.list(&ctx, "", None, 0).await,
        Err(ProviderError::KvInvalidOptions)
    ));
    assert!(matches!(
        provider.list(&ctx, "", None, 1001).await,
        Err(ProviderError::KvInvalidOptions)
    ));
    // prefix > 512 → KvKeyTooLarge
    let prefix = "x".repeat(513);
    assert!(matches!(
        provider.list(&ctx, &prefix, None, 10).await,
        Err(ProviderError::KvKeyTooLarge)
    ));
}

#[tokio::test]
async fn put_rejects_noncanonical_stored_metadata() {
    let provider = MemoryKvProvider::new();
    let ctx = ctx();
    // non-canonical stored JSON (key order) → KvCorrupt
    let options = KvPutOptions {
        expires_at_ms: None,
        metadata_json: Some(b"{\"b\":1,\"a\":2}".to_vec()),
    };
    assert!(matches!(
        provider.put(&ctx, b"k", b"v", &options).await,
        Err(ProviderError::KvCorrupt)
    ));
    // oversized stored metadata → KvCorrupt
    let options = KvPutOptions {
        expires_at_ms: None,
        metadata_json: Some(vec![b'x'; 2048]),
    };
    assert!(matches!(
        provider.put(&ctx, b"k", b"v", &options).await,
        Err(ProviderError::KvCorrupt)
    ));
}
