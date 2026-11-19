//! T17 filesystem backends — trigger-condition coverage: path
//! traversal impossibility, size caps, tenant/namespace isolation,
//! pagination + stale cursors, expiry, and filesystem failures.

mod support;

use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::error::ProviderError;
use vrt_provider::kv::{fs::FsKvProvider, KvProvider, KvPutOptions};
use vrt_provider::r2::{fs::FsR2Provider, wire::R2PutOptions, R2Provider};

fn root() -> support::TestDir {
    support::TestDir::new()
}

/// ctx for `tenant` — both tenants share the desired doc so the
/// backend sees distinct directory namespaces.
fn ctx(tid: &str, ns: &str) -> vrt_provider::BindingContext {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![
                tenant(
                    TENANT,
                    vec![worker(WORKER, VERSION, DIGEST, vec![kv_binding("KV", ns)])],
                ),
                tenant(
                    OTHER_TENANT,
                    vec![worker(
                        "w-other",
                        VERSION,
                        DIGEST,
                        vec![kv_binding("KV", ns)],
                    )],
                ),
            ],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let id = identity(
        if tid == TENANT { WORKER } else { "w-other" },
        "KV",
        if tid == TENANT { WORKER } else { "w-other" },
        VERSION,
        DIGEST,
        ns,
        7,
    );
    authorize(&auth, tid, &id, ResourceKind::Kv).unwrap()
}

fn r2_ctx(tid: &str, bucket: &str) -> vrt_provider::BindingContext {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![
                tenant(
                    TENANT,
                    vec![worker(
                        WORKER,
                        VERSION,
                        DIGEST,
                        vec![r2_binding("B", bucket)],
                    )],
                ),
                tenant(
                    OTHER_TENANT,
                    vec![worker(
                        "w-other",
                        VERSION,
                        DIGEST,
                        vec![r2_binding("B", bucket)],
                    )],
                ),
            ],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let id = identity(
        if tid == TENANT { WORKER } else { "w-other" },
        "B",
        if tid == TENANT { WORKER } else { "w-other" },
        VERSION,
        DIGEST,
        bucket,
        7,
    );
    authorize(&auth, tid, &id, ResourceKind::R2).unwrap()
}

#[tokio::test]
async fn kv_put_get_list_delete_and_tenant_isolation() {
    let dir = root();
    let p = FsKvProvider::new(dir.path().join("kv")).unwrap();
    let a = ctx(TENANT, NS_ID);
    let b = ctx(OTHER_TENANT, NS_ID);
    let opt = KvPutOptions {
        expires_at_ms: None,
        metadata_json: None,
    };
    p.put(&a, b"key/../escape", b"va", &opt).await.unwrap();
    p.put(&b, b"key/../escape", b"vb", &opt).await.unwrap();
    // Traversal-shaped keys are data, never paths — each tenant sees
    // only its own value.
    assert_eq!(
        p.get(&a, b"key/../escape").await.unwrap().unwrap().value,
        b"va"
    );
    assert_eq!(
        p.get(&b, b"key/../escape").await.unwrap().unwrap().value,
        b"vb"
    );
    // And nothing escaped onto disk outside the namespace dir.
    let ns_dir = dir.path().join("kv").join("team-1").join("ns-9f8e7d6c");
    assert!(ns_dir.is_dir());
    assert!(!dir.path().join("kv/team-1/key").exists());
}

#[tokio::test]
async fn kv_value_cap_and_expiry_and_stale_cursor() {
    let dir = root();
    let p = FsKvProvider::new(dir.path().join("kv")).unwrap();
    let c = ctx(TENANT, NS_ID);
    let opt = KvPutOptions {
        expires_at_ms: None,
        metadata_json: None,
    };
    // Oversized value fails closed.
    let big = vec![0u8; vrt_provider::limits::KV_MAX_VALUE_BYTES + 1];
    let err = p.put(&c, b"big", &big, &opt).await.unwrap_err();
    assert!(matches!(err, ProviderError::KvValueTooLarge));
    // Expiry: past deadline reads as absent.
    let past = KvPutOptions {
        expires_at_ms: Some(chrono::Utc::now().timestamp_millis() - 1),
        metadata_json: None,
    };
    p.put(&c, b"exp", b"x", &past).await.unwrap();
    assert!(p.get(&c, b"exp").await.unwrap().is_none());
    // Pagination + stale cursor: after the last key → empty page.
    for k in ["k1", "k2", "k3"] {
        p.put(&c, k.as_bytes(), b"v", &opt).await.unwrap();
    }
    let page1 = p.list(&c, "", None, 2).await.unwrap();
    assert_eq!(page1.rows.len(), 2);
    assert!(!page1.complete);
    let last = page1.rows.last().unwrap().key.clone();
    let page2 = p.list(&c, "", Some(&last), 100).await.unwrap();
    assert_eq!(page2.rows.len(), 1);
    assert!(page2.complete);
    let stale = p.list(&c, "", Some(b"zzzz"), 100).await.unwrap();
    assert!(stale.rows.is_empty());
}

#[tokio::test]
async fn kv_filesystem_failure_maps_typed() {
    let dir = root();
    let kv_root = dir.path().join("kv");
    let p = FsKvProvider::new(kv_root.clone()).unwrap();
    let c = ctx(TENANT, NS_ID);
    // Make the backend root unreadable → ops fail typed, not panic.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&kv_root, std::fs::Permissions::from_mode(0o000)).unwrap();
    let opt = KvPutOptions {
        expires_at_ms: None,
        metadata_json: None,
    };
    let err = p.put(&c, b"k", b"v", &opt).await.unwrap_err();
    assert!(matches!(
        err,
        ProviderError::KvUnavailable | ProviderError::KvInternalProtocolError
    ));
    std::fs::set_permissions(&kv_root, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn r2_put_head_get_range_list_and_isolation() {
    let dir = root();
    let p = FsR2Provider::new(dir.path().join("r2"), 8 * 1024).unwrap();
    let a = r2_ctx(TENANT, "bucket-1");
    let b = r2_ctx(OTHER_TENANT, "bucket-1");
    let opt = R2PutOptions::default();
    p.put(&a, "dir/../obj", b"hello-a", &opt).await.unwrap();
    p.put(&b, "dir/../obj", b"hello-b", &opt).await.unwrap();
    // Same key, different tenant → isolated bodies (digest-named files).
    let ga = p.get(&a, "dir/../obj", None, None).await.unwrap();
    let gb = p.get(&b, "dir/../obj", None, None).await.unwrap();
    match (ga, gb) {
        (
            vrt_provider::r2::R2GetResult::Body(_, ba),
            vrt_provider::r2::R2GetResult::Body(_, bb),
        ) => {
            assert_eq!(ba, b"hello-a");
            assert_eq!(bb, b"hello-b");
        }
        _ => panic!("both gets must return bodies"),
    }
    // head sees metadata without body read.
    let meta = p.head(&a, "dir/../obj").await.unwrap().unwrap();
    assert_eq!(meta.size, 7);
    assert_eq!(meta.key, "dir/../obj");
    // Range.
    let g = p
        .get(
            &a,
            "dir/../obj",
            Some(vrt_provider::r2::wire::R2Range {
                offset: Some(0),
                length: Some(5),
                suffix: None,
            }),
            None,
        )
        .await
        .unwrap();
    match g {
        vrt_provider::r2::R2GetResult::Body(m, body) => {
            assert_eq!(body, b"hello");
            assert!(m.range.is_some());
        }
        _ => panic!("ranged get"),
    }
    // List with delimiter collapse.
    p.put(&a, "a/x", b"1", &opt).await.unwrap();
    p.put(&a, "a/y", b"2", &opt).await.unwrap();
    p.put(&a, "b/z", b"3", &opt).await.unwrap();
    let page = p.list(&a, "", Some("/"), None, 100).await.unwrap();
    assert!(page.delimited_prefixes.contains(&"a/".to_string()));
    assert!(page.delimited_prefixes.contains(&"b/".to_string()));
    assert!(page.delimited_prefixes.contains(&"dir/".to_string()));
    // Oversized put fails closed.
    let err = p
        .put(&a, "huge", &vec![0u8; 9 * 1024], &opt)
        .await
        .unwrap_err();
    assert!(matches!(err, ProviderError::R2ObjectTooLarge));
    // delete.
    p.delete(&a, &["dir/../obj".to_string()]).await.unwrap();
    assert!(p.head(&a, "dir/../obj").await.unwrap().is_none());
}

/// RV-R8 #4: an entry whose DECLARED on-disk size already overflows the
/// aggregate budget must abort the whole `get_many` — the budget trips
/// on the header stage, before value bytes are pulled.
#[tokio::test]
async fn kv_get_many_declared_size_trips_budget_on_header() {
    use base64::Engine;
    use sha2::Digest;
    let dir = root();
    let kv_root = dir.path().join("kv");
    let p = FsKvProvider::new(kv_root.clone()).unwrap();
    let c = ctx(TENANT, NS_ID);

    p.put(&c, b"ok", b"small", &KvPutOptions::default())
        .await
        .unwrap();

    // Hand-craft a valid entry header, then sparse-extend the file so
    // its value region declares ~28MiB — over the 25MiB aggregate cap
    // without writing those bytes.
    let b64 = base64::engine::general_purpose::STANDARD;
    let header = serde_json::json!({
        "key": b64.encode(b"huge"),
        "metadata": serde_json::Value::Null,
        "expiresAtMs": serde_json::Value::Null,
    });
    let hb = serde_json::to_vec(&header).unwrap();
    let ns_dir = kv_root
        .join(vrt_securefs::segment_encode(TENANT))
        .join(vrt_securefs::segment_encode(NS_ID));
    let entry = ns_dir.join(hex::encode(sha2::Sha256::digest(b"huge")));
    std::fs::create_dir_all(&ns_dir).unwrap();
    {
        let mut f = std::fs::File::create(&entry).unwrap();
        use std::io::Write;
        f.write_all(&(hb.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&hb).unwrap();
        f.set_len(28 * 1024 * 1024).unwrap();
    }

    let err = p
        .get_many(&c, &[b"ok".to_vec(), b"huge".to_vec()])
        .await
        .unwrap_err();
    assert!(
        matches!(err, vrt_provider::error::ProviderError::KvResponseTooLarge),
        "declared oversize must trip the aggregate budget: {err:?}"
    );
}
