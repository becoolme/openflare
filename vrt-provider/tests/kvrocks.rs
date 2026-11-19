//! Kvrocks integration tests — `#[ignore]` by default; only run when
//! `VRT_TEST_KVROCKS_URL` points at a scratch Kvrocks/Redis instance.
//! These never run in local `cargo test`.

mod support;

use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::kv::{KvProvider, KvPutOptions, KvrocksKvProvider};

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
    authorize(
        &auth,
        TENANT,
        &identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7),
        ResourceKind::Kv,
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires VRT_TEST_KVROCKS_URL"]
async fn kvrocks_round_trip() {
    let Ok(url) = std::env::var("VRT_TEST_KVROCKS_URL") else {
        eprintln!("skip: VRT_TEST_KVROCKS_URL not set");
        return;
    };
    let client = redis::Client::open(url).unwrap();
    let conn = client.get_multiplexed_tokio_connection().await.unwrap();
    let provider = KvrocksKvProvider::new(conn);
    let ctx = ctx();
    let options = KvPutOptions {
        expires_at_ms: Some(chrono::Utc::now().timestamp_millis() + 60_000),
        metadata_json: Some(b"{\"m\":1}".to_vec()),
    };
    provider
        .put(&ctx, b"it-key", b"it-value", &options)
        .await
        .unwrap();
    let entry = provider.get(&ctx, b"it-key").await.unwrap().unwrap();
    assert_eq!(entry.value, b"it-value");
    assert_eq!(
        entry.metadata_json.as_deref(),
        Some(b"{\"m\":1}".as_slice())
    );
    let page = provider.list(&ctx, "it-", None, 10).await.unwrap();
    assert_eq!(page.rows.len(), 1);
    provider.delete(&ctx, b"it-key").await.unwrap();
    assert!(provider.get(&ctx, b"it-key").await.unwrap().is_none());
}
