//! K2 stream adversarial tests: CF produce/consume/ack/nack/extend semantics,
//! subscription lifecycle, wire endpoint validation, drop/tombstone, and
//! lease-expiry recovery — mirroring queue_tests.rs coverage.

use std::sync::Arc;
use tempfile::TempDir;
use vrt_sql::engine::EngineRegistry;
use vrt_sql::k2::{K2Error, SqliteK2Backend, StreamId, LEASE_MS};
use vrt_sql::wire::{self, AppState};

fn sid() -> StreamId {
    StreamId::parse(&"ab".repeat(16)).unwrap()
}

fn backend(tmp: &TempDir) -> (Arc<EngineRegistry>, SqliteK2Backend) {
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let be = SqliteK2Backend::new(reg.clone());
    (reg, be)
}

async fn oneshot(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    use tower::ServiceExt;
    let req = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status().as_u16();
    let bytes = axum::body::to_bytes(res.into_body(), 16 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({})),
    )
}

// ---------- wire endpoints ----------

#[tokio::test]
async fn wire_full_cycle_produce_consume_ack_nack() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let s = "ab".repeat(16);

    // subscribe (earliest)
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"c","startAt":"earliest"}),
    )
    .await;
    assert_eq!(
        (code, body["success"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    let sub = body["subscription"]["id"].as_str().unwrap().to_string();

    // produce 3 records (hex content + headers)
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/produce"),
        serde_json::json!({"records":[
            {"content":hex::encode(b"r1"),"headers":{"k":"v"}},
            {"content":hex::encode(b"r2")},
            {"content":hex::encode(b"r3"),"headers":{"a":"b","c":"d"}},
        ]}),
    )
    .await;
    assert_eq!((code, body["produced"].as_u64()), (200, Some(3)), "{body}");

    // consume 2 → leased batch w1
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/consume"),
        serde_json::json!({"subscriptionId":sub,"workerId":"w1","maxRecords":2}),
    )
    .await;
    assert_eq!(
        (code, body["success"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    let batch = &body["batch"];
    let bid = batch["batchId"].as_str().unwrap().to_string();
    assert_eq!(batch["records"].as_array().unwrap().len(), 2);
    assert_eq!(
        hex::decode(batch["records"][0]["content"].as_str().unwrap()).unwrap(),
        b"r1"
    );
    assert_eq!(batch["records"][0]["headers"]["k"], "v");
    assert!(batch["leasedUntilMs"].as_i64().unwrap() > 0);

    // w1 re-consume replays the same batch (CF recovery)
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/consume"),
        serde_json::json!({"subscriptionId":sub,"workerId":"w1","maxRecords":10}),
    )
    .await;
    assert_eq!(body["batch"]["batchId"].as_str().unwrap(), bid);

    // w2 gets the next record only
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/consume"),
        serde_json::json!({"subscriptionId":sub,"workerId":"w2","maxRecords":10}),
    )
    .await;
    let bid2 = body["batch"]["batchId"].as_str().unwrap().to_string();
    assert_eq!(body["batch"]["records"].as_array().unwrap().len(), 1);
    assert_eq!(
        hex::decode(body["batch"]["records"][0]["content"].as_str().unwrap()).unwrap(),
        b"r3"
    );

    // ack w1; nack w2 → r3 redelivers to w3
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/ack"),
        serde_json::json!({"subscriptionId":sub,"batchId":bid}),
    )
    .await;
    assert_eq!(code, 200);
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/nack"),
        serde_json::json!({"subscriptionId":sub,"batchId":bid2}),
    )
    .await;
    assert_eq!(code, 200);
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/consume"),
        serde_json::json!({"subscriptionId":sub,"workerId":"w3","maxRecords":10}),
    )
    .await;
    assert_eq!(body["batch"]["records"].as_array().unwrap().len(), 1);
    assert_eq!(
        hex::decode(body["batch"]["records"][0]["content"].as_str().unwrap()).unwrap(),
        b"r3"
    );

    // ack is idempotent: same batch, unknown batch → success
    let bid3 = body["batch"]["batchId"].as_str().unwrap().to_string();
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/ack"),
        serde_json::json!({"subscriptionId":sub,"batchId":bid3}),
    )
    .await;
    assert_eq!(code, 200);
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/ack"),
        serde_json::json!({"subscriptionId":sub,"batchId":bid3}),
    )
    .await;
    assert_eq!(code, 200);
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/ack"),
        serde_json::json!({"subscriptionId":sub,"batchId":"nonexistent"}),
    )
    .await;
    assert_eq!(code, 200);

    // all acked → empty batch
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/consume"),
        serde_json::json!({"subscriptionId":sub,"workerId":"w4","maxRecords":10}),
    )
    .await;
    assert!(body["batch"]["batchId"].is_null());
    assert_eq!(body["batch"]["records"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn wire_id_and_body_validation() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let s = "ab".repeat(16);

    // illegal stream ids rejected at the entry
    for bad in [
        "UPPERCASEUPPERCASEUPPERCASEUPPERCA",
        "ab/../x",
        "short",
        &"ab".repeat(17),
    ] {
        let (c, _) = oneshot(
            app.clone(),
            "POST",
            &format!("/streams/{bad}/produce"),
            serde_json::json!({"records":[{"content":"61"}]}),
        )
        .await;
        assert!(c == 400 || c == 404, "{bad} -> {c}");
    }

    // malformed produce bodies
    for (label, body) in [
        ("not-array", serde_json::json!({"records":{}})),
        ("no-content", serde_json::json!({"records":[{}]})),
        ("bad-hex", serde_json::json!({"records":[{"content":"zz"}]})),
        (
            "headers-nonobject",
            serde_json::json!({"records":[{"content":"61","headers":[1]}]}),
        ),
        (
            "headers-nonstring",
            serde_json::json!({"records":[{"content":"61","headers":{"k":1}}]}),
        ),
        ("empty-records", serde_json::json!({"records":[]})),
    ] {
        let (c, b) = oneshot(app.clone(), "POST", &format!("/streams/{s}/produce"), body).await;
        assert_eq!(
            (c, b["success"].as_bool()),
            (400, Some(false)),
            "{label}: {b}"
        );
    }

    // headers >64 entries
    let headers: serde_json::Map<String, serde_json::Value> = (0..65)
        .map(|i| (format!("h{i}"), serde_json::json!("v")))
        .collect();
    let (c, b) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/produce"),
        serde_json::json!({"records":[{"content":"61","headers":headers}]}),
    )
    .await;
    assert_eq!((c, b["success"].as_bool()), (400, Some(false)), "{b}");

    // bad subscription shapes
    let (c, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"n","startAt":"middle"}),
    )
    .await;
    assert_eq!(c, 400);
    let (c, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"bad name!","startAt":"earliest"}),
    )
    .await;
    assert_eq!(c, 400);

    // unknown op → error, not panic
    let (c, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/frobnicate"),
        serde_json::json!({}),
    )
    .await;
    assert!(c == 400 || c == 404);
}

#[tokio::test]
async fn wire_subscription_lifecycle_and_config() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let s = "cd".repeat(16);

    // configure + read back
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/configure"),
        serde_json::json!({"name":"orders","retentionSeconds":86400}),
    )
    .await;
    assert_eq!(code, 200);
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/config"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(body["config"]["name"], "orders");
    assert_eq!(body["config"]["retentionSeconds"], 86400);

    // retention bounds (CF 3600..2592000)
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/configure"),
        serde_json::json!({"name":"orders","retentionSeconds":59}),
    )
    .await;
    assert_eq!(code, 400);

    // subscribe twice idempotent; conflicting start_at → 422
    let (_, a) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"c","startAt":"latest"}),
    )
    .await;
    let (_, b) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"c","startAt":"latest"}),
    )
    .await;
    assert_eq!(a["subscription"]["id"], b["subscription"]["id"]);
    let (code, b) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"C","startAt":"earliest"}),
    )
    .await;
    assert_eq!(
        (code, b["code"].as_str()),
        (422, Some("VRT_K2_CONFLICT")),
        "{b}"
    );

    // list → one; delete → gone; unknown delete → deleted:false
    let sub = a["subscription"]["id"].as_str().unwrap();
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscriptions"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(body["subscriptions"].as_array().unwrap().len(), 1);
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/unsubscribe"),
        serde_json::json!({"subscriptionId":sub}),
    )
    .await;
    assert_eq!(body["deleted"].as_bool(), Some(true));
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/unsubscribe"),
        serde_json::json!({"subscriptionId":sub}),
    )
    .await;
    assert_eq!(body["deleted"].as_bool(), Some(false));
}

#[tokio::test]
async fn wire_drop_tombstones_stream() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let s = "ef".repeat(16);

    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/produce"),
        serde_json::json!({"records":[{"content":hex::encode(b"x")}]}),
    )
    .await;
    assert_eq!(code, 200);

    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/drop"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        (code, body["dropped"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    // idempotent
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/drop"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(body["dropped"].as_bool(), Some(false));

    // ops after drop fail closed
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/produce"),
        serde_json::json!({"records":[{"content":hex::encode(b"x")}]}),
    )
    .await;
    assert_eq!(code, 410);
    assert_eq!(body["code"].as_str(), Some("VRT_K2_DROPPED"), "{body}");
}

#[tokio::test]
async fn wire_extend_and_lease_conflict() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let s = "12".repeat(16);
    oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/produce"),
        serde_json::json!({"records":[{"content":"61"}]}),
    )
    .await;
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/subscribe"),
        serde_json::json!({"name":"c","startAt":"earliest"}),
    )
    .await;
    let sub = body["subscription"]["id"].as_str().unwrap().to_string();
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/consume"),
        serde_json::json!({"subscriptionId":sub,"workerId":"w1","maxRecords":10}),
    )
    .await;
    let bid = body["batch"]["batchId"].as_str().unwrap().to_string();

    // extend by non-holder → 409 lease lost; by holder → new expiry
    let (code, b) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/extend"),
        serde_json::json!({"subscriptionId":sub,"batchId":bid,"workerId":"w2"}),
    )
    .await;
    assert_eq!(
        (code, b["code"].as_str()),
        (409, Some("VRT_K2_LEASE_LOST")),
        "{b}"
    );
    let (code, b) = oneshot(
        app.clone(),
        "POST",
        &format!("/streams/{s}/extend"),
        serde_json::json!({"subscriptionId":sub,"batchId":bid,"workerId":"w1"}),
    )
    .await;
    assert_eq!((code, b["success"].as_bool()), (200, Some(true)), "{b}");
    assert!(b["leasedUntilMs"].as_i64().unwrap() > 0);
}

// ---------- storage-level: expiry, caps, persistence ----------

#[tokio::test]
async fn lease_expiry_redelivers_to_any_worker() {
    let tmp = TempDir::new().unwrap();
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let clock = Arc::new(vrt_sql::clock::TestClock::new(1_700_000_000_000));
    let be = SqliteK2Backend::new(reg.clone()).with_clock(clock.clone());
    let s = sid();
    be.produce(
        &s,
        &[vrt_sql::k2::K2Record {
            content: b"r".to_vec(),
            headers: "{}".into(),
        }],
    )
    .await
    .unwrap();
    let sub = be.create_subscription(&s, "c", "earliest").await.unwrap();
    let b1 = be.consume(&s, &sub.id, "w1", 10).await.unwrap();
    assert!(b1.batch_id.is_some());
    // Past the 5-minute lease → expired.
    clock.advance(LEASE_MS as u64 + 1);

    // w2 picks the record up — the expired lease's coverage is reclaimed.
    let b2 = be.consume(&s, &sub.id, "w2", 10).await.unwrap();
    assert_eq!(b2.records.len(), 1);
    assert_eq!(b2.records[0].content, b"r");

    // Stale w1 extend on the live batch → LeaseLost.
    assert!(matches!(
        be.extend(&s, &sub.id, b2.batch_id.as_deref().unwrap(), "w1")
            .await,
        Err(K2Error::LeaseLost)
    ));
}

#[tokio::test]
async fn slots_full_at_128_leases() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let s = sid();
    for i in 0..130 {
        be.produce(
            &s,
            &[vrt_sql::k2::K2Record {
                content: format!("r{i}").into_bytes(),
                headers: "{}".into(),
            }],
        )
        .await
        .unwrap();
    }
    let sub = be.create_subscription(&s, "c", "earliest").await.unwrap();
    for i in 0..128 {
        be.consume(&s, &sub.id, &format!("w{i}"), 1).await.unwrap();
    }
    assert!(matches!(
        be.consume(&s, &sub.id, "w128", 1).await,
        Err(K2Error::SlotsFull)
    ));
    // a different subscription is unaffected
    let sub2 = be.create_subscription(&s, "c2", "earliest").await.unwrap();
    assert!(be.consume(&s, &sub2.id, "w", 1).await.is_ok());
}

#[tokio::test]
async fn subscription_cap_100() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let s = sid();
    for i in 0..100 {
        be.create_subscription(&s, &format!("s{i}"), "earliest")
            .await
            .unwrap();
    }
    assert!(matches!(
        be.create_subscription(&s, "overflow", "earliest").await,
        Err(K2Error::Conflict(_))
    ));
}

#[tokio::test]
async fn data_survives_registry_restart() {
    let tmp = TempDir::new().unwrap();
    let s = sid();
    let sub_id;
    {
        let (_r, be) = backend(&tmp);
        be.produce(
            &s,
            &[vrt_sql::k2::K2Record {
                content: b"persist".to_vec(),
                headers: "{}".into(),
            }],
        )
        .await
        .unwrap();
        sub_id = be
            .create_subscription(&s, "c", "earliest")
            .await
            .unwrap()
            .id;
    }
    // Fresh registry over the same root (process restart).
    let (_r, be) = backend(&tmp);
    let batch = be.consume(&s, &sub_id, "w", 10).await.unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].content, b"persist");
    assert_eq!(
        be.create_subscription(&s, "c", "earliest")
            .await
            .unwrap()
            .id,
        sub_id,
        "subscription idempotence survives restart"
    );
}

#[tokio::test]
async fn ack_and_nack_on_expired_lease_are_harmless() {
    let tmp = TempDir::new().unwrap();
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let clock = Arc::new(vrt_sql::clock::TestClock::new(1_700_000_000_000));
    let be = SqliteK2Backend::new(reg.clone()).with_clock(clock.clone());
    let s = sid();
    be.produce(
        &s,
        &[vrt_sql::k2::K2Record {
            content: b"r".to_vec(),
            headers: "{}".into(),
        }],
    )
    .await
    .unwrap();
    let sub = be.create_subscription(&s, "c", "earliest").await.unwrap();
    let b1 = be.consume(&s, &sub.id, "w1", 10).await.unwrap();
    let bid = b1.batch_id.unwrap();
    clock.advance(LEASE_MS as u64 + 1);

    // Stale ack on an expired lease must NOT tombstone the record — it owes a
    // redelivery. Both stale ack and stale nack are idempotent no-ops.
    be.ack(&s, &sub.id, &bid).await.unwrap();
    be.nack(&s, &sub.id, &bid).await.unwrap();
    let b2 = be.consume(&s, &sub.id, "w2", 10).await.unwrap();
    assert_eq!(b2.records.len(), 1);
    assert_eq!(b2.records[0].content, b"r");
}

#[tokio::test]
async fn retention_reap_removes_expired_records() {
    let tmp = TempDir::new().unwrap();
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let clock = Arc::new(vrt_sql::clock::TestClock::new(1_700_000_000_000));
    let be = SqliteK2Backend::new(reg.clone()).with_clock(clock.clone());
    let s = sid();
    be.configure(&s, "orders", vrt_sql::k2::RETENTION_MIN_MS)
        .await
        .unwrap();
    be.produce(
        &s,
        &[vrt_sql::k2::K2Record {
            content: b"old".to_vec(),
            headers: "{}".into(),
        }],
    )
    .await
    .unwrap();

    // Advance past retention → the record is expired.
    clock.advance(vrt_sql::k2::RETENTION_MIN_MS as u64 + 1);

    // Read-time TTL: consume must not deliver the expired record even before
    // the reaper physically removes it.
    let sub = be.create_subscription(&s, "c", "earliest").await.unwrap();
    let batch = be.consume(&s, &sub.id, "w", 10).await.unwrap();
    assert!(
        batch.records.is_empty(),
        "expired record delivered: {:?}",
        batch.records.len()
    );

    let (records, _coverage) = be.reap(&s).await.unwrap();
    assert_eq!(records, 1);
}

#[tokio::test]
async fn lease_ms_matches_cf_five_minutes() {
    assert_eq!(LEASE_MS, 300_000);
}
