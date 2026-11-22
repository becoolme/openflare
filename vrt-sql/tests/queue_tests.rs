//! T37 QueueBackend adversarial tests: limit boundaries / idempotence / lease semantics /
//! concurrent claim / db type gate / persistence recovery / tombstone / wire endpoints.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use vrt_objstore::{MemoryStore, StoreError};
use vrt_sql::clock::{Clock, SystemClock, TestClock};
use vrt_sql::engine::{EngineRegistry, PersisterPair, ResultsFormat, SqlStmt, TxnCtx};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::{self, Queue};
use vrt_sql::persist::{PersistConfig, Persister};
use vrt_sql::queue::{
    IncomingMsg, QueueBackend, QueueError, QueueId, SqliteQueueBackend, BATCH_MAX_BYTES,
    BATCH_MAX_MSGS, CLAIM_MAX, DELAY_MAX_SECS, MSG_MAX_BYTES,
};
use vrt_sql::wire::{self, AppState};

fn qid() -> QueueId {
    QueueId::parse(&"q".repeat(26)).unwrap()
}

fn msg(body: &[u8], ct: &str) -> IncomingMsg {
    IncomingMsg {
        id: None,
        body: body.to_vec(),
        content_type: ct.into(),
        delay_secs: 0,
        timestamp_ms: None,
    }
}

fn backend(tmp: &TempDir) -> (Arc<EngineRegistry>, SqliteQueueBackend) {
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let be = SqliteQueueBackend::new(reg.clone());
    (reg, be)
}

/// T37r: D1+Queue dual Persister (same store/cfg/clock; each with its own root facet).
fn pair(
    store: Arc<MemoryStore>,
    cfg: PersistConfig,
    clock: Arc<dyn Clock>,
    dbs: &std::path::Path,
) -> PersisterPair {
    PersisterPair {
        d1: Arc::new(Persister::new(
            store.clone(),
            cfg.clone(),
            clock.clone(),
            dbs.to_path_buf(),
            ResourceKind::D1,
        )),
        queue: Arc::new(Persister::new(
            store,
            cfg,
            clock,
            queue_root_for(dbs),
            ResourceKind::Queue,
        )),
    }
}

const NOW: i64 = 1_700_000_000_000;

// ---------- limit boundaries ----------

#[tokio::test]
async fn limits_boundaries() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();

    // single message 128_000 passes / +1 rejected
    assert!(be
        .send(&q, &[msg(&vec![b'x'; MSG_MAX_BYTES], "bytes")], NOW)
        .await
        .is_ok());
    assert!(matches!(
        be.send(&q, &[msg(&vec![b'x'; MSG_MAX_BYTES + 1], "bytes")], NOW)
            .await,
        Err(QueueError::TooLarge)
    ));

    // 100 messages pass / 101 rejected
    let hundred: Vec<IncomingMsg> = (0..BATCH_MAX_MSGS).map(|_| msg(b"a", "text")).collect();
    assert!(be.send(&q, &hundred, NOW).await.is_ok());
    let mut over = hundred.clone();
    over.push(msg(b"a", "text"));
    assert!(matches!(
        be.send(&q, &over, NOW).await,
        Err(QueueError::TooLarge)
    ));

    // batch total 288_000 passes / +1 rejected (each message individually legal)
    let full: Vec<IncomingMsg> = (0..99)
        .map(|_| msg(&vec![0u8; BATCH_MAX_BYTES / 100], "bytes"))
        .chain(std::iter::once(msg(
            &vec![0u8; BATCH_MAX_BYTES - 99 * (BATCH_MAX_BYTES / 100)],
            "bytes",
        )))
        .collect();
    assert!(be.send(&q, &full, NOW).await.is_ok());
    let mut over_bytes = vec![
        msg(&vec![0u8; BATCH_MAX_BYTES - 1], "bytes"),
        msg(b"ab", "bytes"),
    ];
    assert!(matches!(
        be.send(&q, &over_bytes, NOW).await,
        Err(QueueError::TooLarge)
    ));
    over_bytes.clear();

    // delay boundaries
    let mut d = msg(b"a", "text");
    d.delay_secs = DELAY_MAX_SECS;
    assert!(be.send(&q, &[d], NOW).await.is_ok());
    let mut d = msg(b"a", "text");
    d.delay_secs = DELAY_MAX_SECS + 1;
    assert!(matches!(
        be.send(&q, &[d], NOW).await,
        Err(QueueError::InvalidInput(_))
    ));

    // invalid content type / invalid id / empty batch
    assert!(matches!(
        be.send(&q, &[msg(b"a", "evil")], NOW).await,
        Err(QueueError::InvalidInput(_))
    ));
    let mut bad = msg(b"a", "text");
    bad.id = Some("has space".into());
    assert!(matches!(
        be.send(&q, &[bad], NOW).await,
        Err(QueueError::InvalidInput(_))
    ));
    assert!(matches!(
        be.send(&q, &[], NOW).await,
        Err(QueueError::InvalidInput(_))
    ));

    // A rejected batch enqueues nothing -- the backlog only holds the legal batch.
    let m = be.metrics(&q, NOW).await.unwrap();
    assert_eq!(m.backlog_count, 1 + 100 + 100 + 1, "{}", m.backlog_count);
}

// ---------- idempotence ----------

#[tokio::test]
async fn rejected_batch_enqueues_nothing() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut base = msg(b"baseline", "text");
    base.id = Some("baseline".into());
    be.send(&q, &[base], NOW).await.unwrap();

    // One oversize item in an otherwise fully valid batch: the whole send is
    // rejected before the transaction starts (no partial write).
    let mut a = msg(b"a", "text");
    a.id = Some("a".into());
    let mut b = msg(b"b", "text");
    b.id = Some("b".into());
    let over = msg(&vec![b'x'; MSG_MAX_BYTES + 1], "bytes");
    assert!(matches!(
        be.send(&q, &[a, over, b], NOW).await,
        Err(QueueError::TooLarge)
    ));

    // Same for a non-size violation in the last item.
    let mut c = msg(b"c", "text");
    c.id = Some("c".into());
    let mut bad = msg(b"d", "text");
    bad.id = Some("d".into());
    bad.delay_secs = DELAY_MAX_SECS + 1;
    assert!(matches!(
        be.send(&q, &[c, bad], NOW).await,
        Err(QueueError::InvalidInput(_))
    ));

    // Only the pre-existing message is claimable -- nothing from either
    // rejected batch was written.
    let got = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW)
        .await
        .unwrap();
    let ids: Vec<&str> = got.messages.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["baseline"]);
    assert_eq!(be.metrics(&q, NOW).await.unwrap().backlog_count, 1);
}

#[tokio::test]
async fn same_timestamp_claim_orders_by_id() {
    // One sendBatch shares visible_at/created_at; the provider assigns
    // time-ordered ids, so the id tie-break is what keeps send order.
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let batch: Vec<IncomingMsg> = ["0003", "0001", "0002"]
        .iter()
        .map(|id| {
            let mut m = msg(id.as_bytes(), "text");
            m.id = Some((*id).into());
            m
        })
        .collect();
    be.send(&q, &batch, NOW).await.unwrap();
    let got = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW)
        .await
        .unwrap();
    let ids: Vec<&str> = got.messages.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, ["0001", "0002", "0003"]);
}

#[tokio::test]
async fn duplicate_msg_id_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"hello", "text");
    m.id = Some("fixed-id".into());
    let a = be.send(&q, std::slice::from_ref(&m), NOW).await.unwrap();
    let b = be.send(&q, &[m], NOW).await.unwrap();
    assert_eq!((a.inserted, a.duplicates), (1, 0));
    assert_eq!((b.inserted, b.duplicates), (0, 1));
    assert_eq!(be.metrics(&q, NOW).await.unwrap().backlog_count, 1);
}

#[tokio::test]
async fn send_persistence_failure_no_partial_batch() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(MemoryStore::new());
    let reg = Arc::new(
        EngineRegistry::new(tmp.path().join("dbs")).with_persisters(pair(
            store.clone(),
            PersistConfig {
                lease_ms: 10_000,
                clock_skew_margin_ms: 500,
                holder: "h1".into(),
                checkpoint_every_commits: 0,
                retention_ms: 60_000,
                snapshot_max_bytes: 1 << 30,
            },
            Arc::new(SystemClock),
            &tmp.path().join("dbs"),
        )),
    );
    let be = SqliteQueueBackend::new(reg.clone());
    let q = qid();
    assert!(be.send(&q, &[msg(b"ok", "text")], NOW).await.is_ok());
    // Inject a persistence failure -> send must be retryable and the whole batch is not written.
    store.inject_put_failures(vec![StoreError::Unavailable {
        retryable: true,
        reason: "injected".into(),
    }]);
    let err = be
        .send(
            &q,
            &[
                {
                    let mut m = msg(b"p1", "text");
                    m.id = Some("p1".into());
                    m
                },
                {
                    let mut m = msg(b"p2", "text");
                    m.id = Some("p2".into());
                    m
                },
            ],
            NOW,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            QueueError::Unavailable {
                retryable: true,
                ..
            } | QueueError::Fenced(_)
        ),
        "{err:?}"
    );
    // Crash semantics: the h1 process vanishes -- the engine's drop releases the flock (the manifest lease
    // is not released; it expires naturally as h2 advances the clock = the takeover path).
    drop(be);
    drop(reg);
    // h2's clock advances past h1's lease expiry (crash takeover path) -> take over immediately.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let clock2: Arc<dyn Clock> = Arc::new(TestClock::new(now_ms + 11_000));
    // After the new engine takes over: only the first ok message, p1/p2 are absent.
    let reg2 = Arc::new(
        EngineRegistry::new(tmp.path().join("dbs")).with_persisters(pair(
            store.clone(),
            PersistConfig {
                lease_ms: 10_000,
                clock_skew_margin_ms: 500,
                holder: "h2".into(),
                checkpoint_every_commits: 0,
                retention_ms: 60_000,
                snapshot_max_bytes: 1 << 30,
            },
            clock2,
            &tmp.path().join("dbs"),
        )),
    );
    let be2 = SqliteQueueBackend::new(reg2);
    let m = be2.metrics(&q, NOW).await.unwrap();
    assert_eq!(
        m.backlog_count, 1,
        "no partial batch, got {}",
        m.backlog_count
    );
}

// ---------- lease semantics ----------

#[tokio::test]
async fn lease_expiry_reclaim_and_stale_ack() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"work", "text");
    m.id = Some("m1".into());
    be.send(&q, &[m], NOW).await.unwrap();

    // A claims (ttl=10s)
    let a = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW)
        .await
        .unwrap();
    assert_eq!(a.messages.len(), 1);
    assert_eq!(a.messages[0].attempts, 1);
    // B cannot get it while the lease has not expired
    let b_early = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW + 5_000)
        .await
        .unwrap();
    assert!(b_early.messages.is_empty());
    // lease expired -> B re-claims, attempts+1
    let b = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW + 11_000)
        .await
        .unwrap();
    assert_eq!(b.messages.len(), 1);
    assert_eq!(b.messages[0].attempts, 2);
    // A's old lease ack -> LeaseLost, the message stays under B's lease
    assert!(matches!(
        be.ack(&q, &a.lease_id, &["m1".into()], NOW + 12_000).await,
        Err(QueueError::LeaseLost)
    ));
    // B acks normally
    assert_eq!(
        be.ack(&q, &b.lease_id, &["m1".into()], NOW + 12_000)
            .await
            .unwrap(),
        1
    );
    assert_eq!(be.metrics(&q, NOW).await.unwrap().backlog_count, 0);
}

#[tokio::test]
async fn concurrent_claimers_no_duplicate_delivery() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let msgs: Vec<IncomingMsg> = (0..8)
        .map(|i| IncomingMsg {
            id: Some(format!("m{i}")),
            body: vec![i as u8],
            content_type: "bytes".into(),
            delay_secs: 0,
            timestamp_ms: None,
        })
        .collect();
    be.send(&q, &msgs, NOW).await.unwrap();

    let be = Arc::new(be);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let be = be.clone();
        let q = q.clone();
        handles.push(tokio::spawn(async move {
            be.claim(&q, "t", 300_000, 10, None, 60_000, NOW)
                .await
                .unwrap()
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        ids.extend(h.await.unwrap().messages.iter().map(|m| m.id.clone()));
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 8, "no duplicate delivery: {ids:?}");
}

#[tokio::test]
async fn retry_reschedules_and_dead_letter_moves() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"x", "text");
    m.id = Some("r1".into());
    be.send(&q, &[m], NOW).await.unwrap();
    let c = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW)
        .await
        .unwrap();
    // retry delay=60s -> invisible -> visible after 60s
    assert_eq!(
        be.retry(&q, &c.lease_id, &[("r1".into(), 60)], NOW)
            .await
            .unwrap(),
        1
    );
    assert!(be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW + 30_000)
        .await
        .unwrap()
        .messages
        .is_empty());
    let c2 = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW + 61_000)
        .await
        .unwrap();
    assert_eq!(c2.messages.len(), 1);
    // old lease retry -> LeaseLost
    assert!(matches!(
        be.retry(&q, &c.lease_id, &[("r1".into(), 0)], NOW + 62_000)
            .await,
        Err(QueueError::LeaseLost)
    ));
    // dead-letter: the message leaves the backlog and enters the audit table
    assert_eq!(
        be.dead_letter(
            &q,
            &c2.lease_id,
            &["r1".into()],
            "max attempts",
            NOW + 63_000
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(be.metrics(&q, NOW).await.unwrap().backlog_count, 0);
}

// ---------- db type gate ----------

#[tokio::test]
async fn d1_db_rejected_by_queue_ops_and_internal_tables_hidden() {
    let tmp = TempDir::new().unwrap();
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let q = qid();
    // D1 surface: create a user table + write a row.
    let eng = reg.get(Queue, q.as_str()).unwrap();
    let ctx = TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(30),
        dedup: None,
    };
    eng.query_batch_sync(
        &[SqlStmt {
            sql: "CREATE TABLE users(a)".into(),
            params: None,
        }],
        &ctx,
        ResultsFormat::ArrayOfObjects,
    )
    .unwrap();
    let be = SqliteQueueBackend::new(reg.clone());
    // With a user table -> queue ops are rejected (db type mismatch, fail closed).
    for r in [
        be.send(&q, &[msg(b"a", "text")], NOW).await.err(),
        be.claim(&q, "t", 300_000, 1, None, 1000, NOW).await.err(),
        be.metrics(&q, NOW).await.err(),
    ] {
        assert!(matches!(r, Some(QueueError::InvalidInput(_))), "{r:?}");
    }
    // Reverse: user SQL on a queue db reading `_vrt_q_*` is rejected (reserved prefix).
    let q2 = QueueId::parse(&"r".repeat(26)).unwrap();
    be.send(&q2, &[msg(b"a", "text")], NOW).await.unwrap();
    let eng2 = reg.get(Queue, q2.as_str()).unwrap();
    let denied = eng2.query_batch_sync(
        &[SqlStmt {
            sql: "SELECT * FROM _vrt_q_messages".into(),
            params: None,
        }],
        &ctx,
        ResultsFormat::ArrayOfObjects,
    );
    assert!(denied.is_err(), "user SQL must not read _vrt_q_*");
    let denied2 = eng2.query_batch_sync(
        &[SqlStmt { sql: "INSERT INTO _vrt_q_messages(id,body,content_type,visible_at,created_at) VALUES('x',x'00','text',0,0)".into(), params: None }],
        &ctx,
        ResultsFormat::ArrayOfObjects,
    );
    assert!(denied2.is_err(), "user SQL must not write _vrt_q_*");
}

// ---------- persistence recovery ----------

#[tokio::test]
async fn recovery_preserves_backlog_and_acked() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(MemoryStore::new());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mk = |holder: &str, root: &std::path::Path| {
        pair(
            store.clone(),
            PersistConfig {
                lease_ms: 10_000,
                clock_skew_margin_ms: 500,
                holder: holder.into(),
                checkpoint_every_commits: 0,
                retention_ms: 60_000,
                snapshot_max_bytes: 1 << 30,
            },
            clock.clone(),
            root,
        )
    };
    let reg = Arc::new(
        EngineRegistry::new(tmp.path().join("dbs"))
            .with_persisters(mk("h1", &tmp.path().join("dbs"))),
    );
    let be = SqliteQueueBackend::new(reg.clone());
    let q = qid();
    for i in 0..3 {
        let mut m = msg(format!("b{i}").as_bytes(), "text");
        m.id = Some(format!("k{i}"));
        be.send(&q, &[m], NOW).await.unwrap();
    }
    let c = be
        .claim(&q, "t", 300_000, 1, None, 60_000, NOW)
        .await
        .unwrap();
    be.ack(&q, &c.lease_id, &[c.messages[0].id.clone()], NOW)
        .await
        .unwrap();
    drop(be);
    reg.release_leases(std::time::Instant::now() + Duration::from_secs(5));
    drop(reg); // engine drop -> graceful lease release
               // New registry (= restart) recovers: backlog=2.
    let reg2 = Arc::new(
        EngineRegistry::new(tmp.path().join("dbs"))
            .with_persisters(mk("h2", &tmp.path().join("dbs"))),
    );
    let be2 = SqliteQueueBackend::new(reg2);
    let m = be2.metrics(&q, NOW).await.unwrap();
    assert_eq!(m.backlog_count, 2);
    assert!(m.oldest_message_timestamp_ms.is_some());
}

// ---------- tombstone ----------

#[tokio::test]
async fn dropped_queue_rejects_all_ops_idempotent() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    be.send(&q, &[msg(b"a", "text")], NOW).await.unwrap();
    assert!(be.drop_queue(&q).unwrap());
    assert!(!be.drop_queue(&q).unwrap(), "delete idempotent");
    for r in [
        be.send(&q, &[msg(b"a", "text")], NOW).await.err(),
        be.claim(&q, "t", 300_000, 1, None, 1000, NOW).await.err(),
        be.metrics(&q, NOW).await.err(),
    ] {
        assert!(matches!(r, Some(QueueError::Dropped)), "{r:?}");
    }
}

// ---------- wire endpoints ----------

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
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 22)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({})),
    )
}

#[tokio::test]
async fn wire_send_claim_ack_metrics_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let q = "q".repeat(26);

    // send
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{q}/send"),
        serde_json::json!({"messages":[
            {"id":"w1","body":hex::encode(b"hi"),"contentType":"text","delaySecs":0}
        ]}),
    )
    .await;
    assert_eq!(
        (code, body["success"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    assert_eq!(body["enqueued"].as_u64(), Some(1));
    assert_eq!(body["metrics"]["backlogCount"].as_i64(), Some(1));

    // illegal id rejected at the entry
    for bad in ["UPPERCASE", "q/../x", "short"] {
        let (c, _) = oneshot(
            app.clone(),
            "POST",
            &format!("/queues/{bad}/send"),
            serde_json::json!({"messages":[]}),
        )
        .await;
        assert!(c == 400 || c == 404, "{bad} -> {c}");
    }

    // claim → ack → metrics=0
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{q}/claim"),
        serde_json::json!({"holderId":"t","holderTtlMs":30000,"max":10,"leaseTtlMs":30000}),
    )
    .await;
    assert_eq!(
        (code, body["success"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    let lease = body["leaseId"].as_str().unwrap();
    assert_eq!(body["messages"][0]["id"].as_str(), Some("w1"));
    assert_eq!(
        hex::decode(body["messages"][0]["body"].as_str().unwrap()).unwrap(),
        b"hi"
    );
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{q}/ack"),
        serde_json::json!({"leaseId":lease,"ids":["w1"]}),
    )
    .await;
    assert_eq!(
        (code, body["success"].as_bool()),
        (200, Some(true)),
        "{body}"
    );
    // old lease acks again -> 409 LeaseLost
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{q}/ack"),
        serde_json::json!({"leaseId":lease,"ids":["w1"]}),
    )
    .await;
    assert_eq!(code, 409);
    assert_eq!(body["code"].as_str(), Some("VRT_QUEUE_LEASE_LOST"));
    let (code, body) = oneshot(
        app.clone(),
        "GET",
        &format!("/queues/{q}/metrics"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        (code, body["metrics"]["backlogCount"].as_i64()),
        (200, Some(0)),
        "{body}"
    );
}

// ---------- T37r: ResourceKind namespace separation ----------

fn sctx() -> TxnCtx {
    TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(30),
        dedup: None,
    }
}

/// The same id opened as D1 and as Queue: two disjoint databases -- local directory facet,
/// table-set facet, drop/tombstone facet, diag carries the kind.
#[tokio::test]
async fn same_id_d1_and_queue_are_independent() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(MemoryStore::new());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let reg = Arc::new(
        EngineRegistry::new(tmp.path().join("dbs")).with_persisters(pair(
            store.clone(),
            PersistConfig {
                lease_ms: 10_000,
                clock_skew_margin_ms: 500,
                holder: "h1".into(),
                checkpoint_every_commits: 0,
                retention_ms: 60_000,
                snapshot_max_bytes: 1 << 30,
            },
            clock.clone(),
            &tmp.path().join("dbs"),
        )),
    );
    let be = SqliteQueueBackend::new(reg.clone());
    let id = qid();
    be.send(&id, &[msg(b"qm", "text")], NOW).await.unwrap();

    // D1 db with the same id: a brand-new empty db -- no _vrt_q_*, user tables can be created.
    let d1 = reg
        .get(vrt_sql::paths::ResourceKind::D1, id.as_str())
        .unwrap();
    let st = |sql: &str| SqlStmt {
        sql: sql.into(),
        params: None,
    };
    d1.query_batch_sync(
        &[st("CREATE TABLE t(x)")],
        &sctx(),
        ResultsFormat::ArrayOfObjects,
    )
    .unwrap();
    let out = d1
        .query_batch_sync(
            &[st("SELECT name FROM sqlite_master ORDER BY name")],
            &sctx(),
            ResultsFormat::ArrayOfObjects,
        )
        .unwrap();
    let names: Vec<String> = out[0]
        .results
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["name"].as_str().map(str::to_owned))
        .collect();
    assert!(names.iter().any(|n| n == "t"), "{names:?}");
    assert!(
        names.iter().all(|n| !n.starts_with("_vrt_q")),
        "a D1 db must never contain queue internal tables: {names:?}"
    );
    // queue data is unaffected by opening a D1 with the same id.
    assert_eq!(be.metrics(&id, NOW).await.unwrap().backlog_count, 1);

    // Local path facet: both files exist and the paths differ.
    let dbs = tmp.path().join("dbs");
    let d1f = vrt_sql::paths::derive_db_file(&dbs, id.as_str()).unwrap();
    let qf =
        vrt_sql::paths::derive_db_file(&vrt_sql::paths::queue_root_for(&dbs), id.as_str()).unwrap();
    assert!(d1f.exists() && qf.exists(), "d1={d1f:?} q={qf:?}");
    assert_ne!(d1f, qf);
    assert!(d1f.to_string_lossy().contains("dbs/"));
    assert!(qf.to_string_lossy().contains("queues/"));

    // diag: each of the two kinds is its own entry and carries the kind.
    let diags = reg.diagnostics();
    let by_kind = |k| {
        diags
            .iter()
            .filter(|d| d.database_id == id.as_str() && d.kind == k)
            .count()
    };
    assert_eq!(by_kind(vrt_sql::paths::ResourceKind::D1), 1);
    assert_eq!(by_kind(vrt_sql::paths::ResourceKind::Queue), 1);
    let j = serde_json::to_value(&diags).unwrap();
    assert!(
        j.as_array()
            .unwrap()
            .iter()
            .any(|d| d["kind"] == "queue" && d["databaseId"] == id.as_str()),
        "diag JSON must carry kind: {j}"
    );

    // drop queue: the D1 db with the same id is unaffected; the queue-side tombstone takes effect.
    assert!(be.drop_queue(&id).unwrap());
    assert!(matches!(
        be.send(&id, &[msg(b"x", "text")], NOW).await.err(),
        Some(QueueError::Dropped)
    ));
    let out = d1
        .query_batch_sync(
            &[st("SELECT count(*) AS count FROM t")],
            &sctx(),
            ResultsFormat::ArrayOfObjects,
        )
        .unwrap();
    assert_eq!(out[0].results[0]["count"].as_i64().unwrap(), 0);
}

/// The `/dbs/{id}/query` endpoint goes through the D1 kind -- structurally unable to touch queue data:
/// when the queue with the same id has data, the D1 endpoint faces an independent empty db.
#[tokio::test]
async fn d1_query_endpoint_cannot_reach_queue_data() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let id = "q".repeat(26);
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{id}/send"),
        serde_json::json!({"messages":[
            {"body":hex::encode(b"hi"),"contentType":"text","delaySecs":0}
        ]}),
    )
    .await;
    assert_eq!(code, 200, "queue send");
    // The D1 endpoint opens the same id -> an independent db, no _vrt_q_messages (the table does not even exist).
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/dbs/{id}/query"),
        serde_json::json!({"sql": "SELECT name FROM sqlite_master"}),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    let names: Vec<String> = body[0]["results"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["name"].as_str().map(str::to_owned))
        .collect();
    assert!(
        names.iter().all(|n| !n.starts_with("_vrt_q")),
        "the D1 endpoint must never see queue internal tables: {names:?}"
    );
    // The queue data is still there.
    let (code, body) = oneshot(
        app,
        "GET",
        &format!("/queues/{id}/metrics"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["metrics"]["backlogCount"], 1, "{body}");
}

// ---------- T37r: client timestamp is untrusted ----------

/// Forged ancient/future timestamps: client_ts is only an echoed attribute; created_at /
/// visible_at / oldestMessageTimestamp always use the server clock.
#[tokio::test]
async fn forged_timestamp_cannot_affect_scheduling_or_metrics() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    // Ancient timestamp (epoch+1ms) -- a legal attribute, but it takes no part in scheduling/metrics.
    let mut ancient = msg(b"old", "text");
    ancient.timestamp_ms = Some(1);
    assert_eq!(be.send(&q, &[ancient], NOW).await.unwrap().inserted, 1);
    let m = be.metrics(&q, NOW).await.unwrap();
    assert_eq!(
        m.oldest_message_timestamp_ms,
        Some(NOW),
        "oldestMessageTimestamp must be driven by the server-side created_at"
    );
    let c = be
        .claim(&q, "t", 300_000, 1, None, 60_000, NOW)
        .await
        .unwrap();
    assert_eq!(c.messages.len(), 1);
    // The consumer echoes the client_ts attribute (DLQ forwarding semantics) -- no effect on lease/visibility.
    assert_eq!(c.messages[0].timestamp_ms, 1);
    // A future timestamp beyond tolerance (>now+5min) -> rejected rather than clamped.
    let mut future = msg(b"fut", "text");
    future.timestamp_ms = Some(NOW + 1_000_000_000);
    assert!(matches!(
        be.send(&q, &[future], NOW).await.err(),
        Some(QueueError::InvalidInput(_))
    ));
    // Within the tolerance boundary (now+5min) -> accepted as an attribute; metrics still use the server clock.
    let mut skew = msg(b"skew", "text");
    skew.timestamp_ms = Some(NOW + 300_000);
    assert_eq!(be.send(&q, &[skew], NOW).await.unwrap().inserted, 1);
    assert_eq!(
        be.metrics(&q, NOW)
            .await
            .unwrap()
            .oldest_message_timestamp_ms,
        Some(NOW)
    );
    assert_eq!(be.metrics(&q, NOW).await.unwrap().backlog_count, 2);
}

// ---------- T37r: independent body cap for the queue route ----------

async fn oneshot_raw(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: String,
) -> (u16, serde_json::Value) {
    use tower::ServiceExt;
    let req = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status().as_u16();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 22)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({})),
    )
}

/// QUEUE_BODY_MAX = 576_000 + 25_600 + 256 = 601_856:
/// the largest legal batch passes; +1 byte (including a junk non-JSON body) gets 413 before JSON parsing.
#[tokio::test]
async fn wire_queue_body_max_boundary() {
    const LIMIT: usize = 601_856;
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let q = "q".repeat(26);

    // Largest legal batch: 3 messages totaling 288_000B (128K+128K+32K; the per-message
    // cap is 128K), hex totals 576_000 -> 200.
    let mk = |n: usize, id: &str| {
        serde_json::json!({
            "id": id,
            "body": hex::encode(vec![b'x'; n]),
            "contentType": "v8",
            "delaySecs": 86400,
            "timestampMs": NOW,
        })
    };
    let msgs = vec![
        mk(vrt_sql::queue::MSG_MAX_BYTES, "a"),
        mk(vrt_sql::queue::MSG_MAX_BYTES, "b"),
        mk(
            vrt_sql::queue::BATCH_MAX_BYTES - 2 * vrt_sql::queue::MSG_MAX_BYTES,
            "c",
        ),
    ];
    let body = serde_json::json!({"messages": msgs}).to_string();
    assert!(
        body.len() <= LIMIT,
        "max legal batch must fit: {} > {LIMIT}",
        body.len()
    );
    let (code, resp) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), body).await;
    assert_eq!(
        (code, resp["success"].as_bool()),
        (200, Some(true)),
        "{resp}"
    );

    // Exactly LIMIT bytes: legal JSON + trailing padding spaces -> still passes (the cap includes the header but excludes padding).
    let json = serde_json::json!({"messages":[{"body":"","contentType":"text","delaySecs":0}]})
        .to_string();
    let padded = format!("{json}{}", " ".repeat(LIMIT - json.len()));
    assert_eq!(padded.len(), LIMIT);
    let (code, _) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), padded).await;
    assert_eq!(code, 200);

    // LIMIT+1 bytes of junk (non-JSON) -> 413, which happens before JSON parsing
    // (if the parse layer caught it, it would be 400 invalid json, not 413).
    let junk = "x".repeat(LIMIT + 1);
    let (code, resp) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), junk).await;
    assert_eq!(code, 413, "{resp}");

    // LIMIT+1 of legal JSON (padded past the cap) -> likewise 413 -- the cap sits above the semantic layer.
    let over = format!("{json}{}", " ".repeat(LIMIT - json.len() + 1));
    let (code, _) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), over).await;
    assert_eq!(code, 413);

    // The D1 endpoint cap is not enlarged: with the same process's default body_max=1MiB, D1 endpoint behavior
    // is unchanged -- a D1 request >QUEUE_BODY_MAX is still judged by its own 1MiB cap.
    // (Reverse assertion: the queue route uses min(QUEUE_BODY_MAX, body_max);
    //   a 601KB D1 request is within its own 1MiB limit and is accepted as usual.)
    let d1_ok = serde_json::json!({"sql":"SELECT 1"}).to_string();
    let d1_padded = format!("{d1_ok}{}", " ".repeat(700_000 - d1_ok.len()));
    let (code, _) = oneshot_raw(
        app.clone(),
        "POST",
        &format!("/dbs/{}/query", "d".repeat(26)),
        d1_padded,
    )
    .await;
    assert_ne!(code, 413, "the D1 endpoint must not be affected by the queue cap");
}

/// `/queues/{id}/drop` only tombstones the queue namespace -- cell-controller
/// routes by op payload.kind; the D1 db with the same id is unaffected, and vice versa
/// (T37r: before namespace separation, a queue DROP would tombstone the D1 namespace,
/// leak the queue db, and even delete a real D1 db with the same id if present).
#[tokio::test]
async fn queue_drop_endpoint_kind_scoped() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let id = "q".repeat(26);

    // The queue and the D1 db with the same id each hold data.
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{id}/send"),
        serde_json::json!({"messages":[
            {"body":hex::encode(b"hi"),"contentType":"text","delaySecs":0}
        ]}),
    )
    .await;
    assert_eq!(code, 200, "queue send");
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/dbs/{id}/query"),
        serde_json::json!({"sql": "CREATE TABLE t(x INTEGER)"}),
    )
    .await;
    assert_eq!(code, 200, "d1 create");

    // First verify reverse isolation: under queue-only semantics /dbs/{id}/drop only touches the D1
    // namespace (dropped:false or deletes the D1 db), and the queue stays usable.
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{id}/drop"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        (code, body["success"].as_bool(), body["dropped"].as_bool()),
        (200, Some(true), Some(true)),
        "{body}"
    );
    // queue-side tombstone: every op is rejected (410 VRT_QUEUE_DROPPED), idempotent.
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{id}/send"),
        serde_json::json!({"messages":[
            {"body":hex::encode(b"x"),"contentType":"text","delaySecs":0}
        ]}),
    )
    .await;
    assert_eq!(
        (code, body["code"].as_str()),
        (410, Some("VRT_QUEUE_DROPPED")),
        "{body}"
    );
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{id}/drop"),
        serde_json::json!({}),
    )
    .await;
    // Idempotent replay: the tombstone hits, dropped:false -- same as /dbs/{id}/drop
    // semantics (wire_tests drop_endpoint_roundtrip_and_tombstone).
    assert_eq!(
        (code, body["success"].as_bool(), body["dropped"].as_bool()),
        (200, Some(true), Some(false)),
        "drop idempotent: {body}"
    );
    // The D1 db with the same id is intact.
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/dbs/{id}/query"),
        serde_json::json!({"sql": "SELECT count(*) AS c FROM t"}),
    )
    .await;
    assert_eq!(code, 200, "d1 survives queue drop: {body}");

    // A D1 drop does not touch the queue tombstone set -- creating a queue with the same id again is rejected (the D1
    // tombstone belongs to a different namespace and they are not visible to each other).
    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/dbs/{id}/drop"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(code, 200, "d1 drop");
    let (_, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/dbs/{id}/query"),
        serde_json::json!({"sql": "SELECT 1"}),
    )
    .await;
    // The D1 error surface is HTTP 200 + success:false (wire semantics).
    assert_eq!(body["success"].as_bool(), Some(false), "{body}");
    assert_eq!(body["error"].as_str(), Some("database dropped"), "{body}");
}

/// T37b: dispatch lease mutual exclusion, renewal, takeover on expiry, pure renewal (max=0).
#[tokio::test]
async fn dispatch_lease_exclusion_renew_takeover() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"d", "text");
    m.id = Some("d1".into());
    be.send(&q, &[m], NOW).await.unwrap();

    // A holds the lease (ttl=10s) -> epoch=1, gets messages
    let a = be
        .claim(&q, "disp-a", 10_000, 10, None, 60_000, NOW)
        .await
        .unwrap();
    assert_eq!(a.dispatch_epoch, 1);
    assert_eq!(a.messages.len(), 1);
    // B claims concurrently -> DispatchLeaseLost (the holder and expiry time are observable)
    match be
        .claim(&q, "disp-b", 10_000, 10, None, 60_000, NOW + 1_000)
        .await
    {
        Err(QueueError::DispatchLeaseLost {
            holder,
            expires_at_ms,
        }) => {
            assert_eq!(holder, "disp-a");
            assert_eq!(expires_at_ms, NOW + 10_000);
        }
        other => panic!("expected DispatchLeaseLost, got {other:?}"),
    }
    // A renews before expiry -> expires moves later, and B still cannot take over at the original expiry point
    let renew = be
        .claim(&q, "disp-a", 10_000, 0, None, 60_000, NOW + 9_000)
        .await
        .unwrap();
    assert_eq!(renew.dispatch_epoch, 1, "self renew keeps epoch");
    assert!(renew.messages.is_empty(), "max=0 claims nothing");
    match be
        .claim(&q, "disp-b", 10_000, 10, None, 60_000, NOW + 10_500)
        .await
    {
        Err(QueueError::DispatchLeaseLost { expires_at_ms, .. }) => {
            assert_eq!(expires_at_ms, NOW + 19_000);
        }
        other => panic!("expected DispatchLeaseLost, got {other:?}"),
    }
    // A no longer renews -> after expiry B takes over the dispatch lease: epoch++. But B still cannot get
    // messages at that moment -- taking over the dispatch lease does not invalidate the message lease (the message
    // lease A claimed has not reached 60s; layered semantics: B must wait for the message lease to expire before re-claiming).
    let b = be
        .claim(&q, "disp-b", 10_000, 10, None, 60_000, NOW + 20_000)
        .await
        .unwrap();
    assert_eq!(b.dispatch_epoch, 2, "takeover increments epoch");
    assert!(b.messages.is_empty(), "msg lease still held by A's claim");
    let b2 = be
        .claim(&q, "disp-b", 10_000, 10, None, 60_000, NOW + 61_000)
        .await
        .unwrap();
    assert_eq!(b2.messages.len(), 1);
    assert_eq!(
        b2.messages[0].attempts, 2,
        "reclaimed after msg lease expiry"
    );
    // The old A's settlement is rejected -- the msg lease belongs to B's re-claim
    assert!(matches!(
        be.ack(&q, &a.lease_id, &["d1".into()], NOW + 62_000).await,
        Err(QueueError::LeaseLost)
    ));
}

/// T37b: dispatch lease parameter validation -- holder charset/length, ttl, max boundaries.
#[tokio::test]
async fn dispatch_lease_input_validation() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    for holder in ["", "has space", "semi;colon", &"x".repeat(65)] {
        assert!(
            matches!(
                be.claim(&q, holder, 10_000, 1, None, 60_000, NOW).await,
                Err(QueueError::InvalidInput(_))
            ),
            "holder {holder:?} must be rejected"
        );
    }
    for ttl in [0i64, -1, 300_001] {
        assert!(
            matches!(
                be.claim(&q, "t", ttl, 1, None, 60_000, NOW).await,
                Err(QueueError::InvalidInput(_))
            ),
            "holder ttl {ttl} must be rejected"
        );
    }
    assert!(matches!(
        be.claim(&q, "t", 10_000, CLAIM_MAX + 1, None, 60_000, NOW)
            .await,
        Err(QueueError::InvalidInput(_))
    ));
    // Boundaries: holder=64 chars, ttl=300_000, max=0 are all legal
    assert!(be
        .claim(&q, &"h".repeat(64), 300_000, 0, None, 60_000, NOW)
        .await
        .is_ok());
}

/// T37b-r: claim `maxBytes` -- accumulated by RAW body bytes, >=1 floor,
/// truncated once the budget is exhausted (no cross-batch topping up).
#[tokio::test]
async fn claim_max_bytes_budget() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    // 3 x 100B + 1 x 128KB large message.
    // Increasing created_at fixes the order (ORDER BY visible_at,created_at --
    // client_ts does not take part in ordering, and ties by PK cannot assume an order).
    for i in 0..3i64 {
        be.send(
            &q,
            &[IncomingMsg {
                id: Some(format!("s{i}")),
                body: vec![b'x'; 100],
                content_type: "text".into(),
                delay_secs: 0,
                timestamp_ms: None,
            }],
            NOW + i,
        )
        .await
        .unwrap();
    }
    be.send(
        &q,
        &[IncomingMsg {
            id: Some("big".into()),
            body: vec![b'y'; 128_000],
            content_type: "text".into(),
            delay_secs: 0,
            timestamp_ms: None,
        }],
        NOW + 3,
    )
    .await
    .unwrap();

    // Budget 250B -> only s0+s1 are taken (s2 would make the cumulative 300>250).
    let c = be
        .claim(&q, "h", 10_000, 10, Some(250), 60_000, NOW + 10)
        .await
        .unwrap();
    assert_eq!(
        c.messages.len(),
        2,
        "got: {:?}",
        c.messages
            .iter()
            .map(|m| (&m.id, m.body.len()))
            .collect::<Vec<_>>()
    );
    assert_eq!(c.messages[0].id, "s0");
    assert_eq!(c.messages[1].id, "s1");
    be.ack(&q, &c.lease_id, &["s0".into(), "s1".into()], NOW + 10)
        .await
        .unwrap();

    // Budget 50B < a single 100B -- the >=1 floor: s2 is still returned.
    let c2 = be
        .claim(&q, "h", 10_000, 10, Some(50), 60_000, NOW + 10)
        .await
        .unwrap();
    assert_eq!(
        c2.messages.len(),
        1,
        "≥1 guarantee: oversize single still taken"
    );
    assert_eq!(c2.messages[0].id, "s2");
    be.ack(&q, &c2.lease_id, &["s2".into()], NOW + 10)
        .await
        .unwrap();

    // Budget 1B -- the 128KB large message is still claimable (the floor takes priority over the budget).
    let c3 = be
        .claim(&q, "h", 10_000, 10, Some(1), 60_000, NOW + 10)
        .await
        .unwrap();
    assert_eq!(c3.messages.len(), 1);
    assert_eq!(c3.messages[0].id, "big");
    be.ack(&q, &c3.lease_id, &["big".into()], NOW + 10)
        .await
        .unwrap();

    // Boundaries: 0 / >32MB rejected; None keeps the old behavior.
    assert!(matches!(
        be.claim(&q, "h", 10_000, 10, Some(0), 60_000, NOW + 10)
            .await,
        Err(QueueError::InvalidInput(_))
    ));
    assert!(matches!(
        be.claim(
            &q,
            "h",
            10_000,
            10,
            Some(33 * 1024 * 1024),
            60_000,
            NOW + 10
        )
        .await,
        Err(QueueError::InvalidInput(_))
    ));
}

/// T37b-r4: e2e reproduction -- vrt-sql restarts while the dispatcher already holds the dispatch lease
/// (new EngineRegistry, same db directory; simulates a new process opening the same db after SIGTERM):
/// messages must still be there, the dispatch-lease CAS renews normally for the same holder, and claim returns messages.
/// Covers the real engine path that the old fake `recovers_after_sql_restart` never reached.
#[tokio::test]
async fn claim_recovers_across_registry_restart() {
    let tmp = TempDir::new().unwrap();
    let dbs = tmp.path().join("dbs");
    let q = qid();
    // Process 1: dispatcher claim -> `_vrt_q_dispatch` holder=vrt-serve lands on disk.
    {
        let reg = Arc::new(EngineRegistry::new(dbs.clone()));
        let be = SqliteQueueBackend::new(reg);
        let r = be
            .claim(&q, "vrt-serve", 30_000, 5, Some(4 << 20), 60_000, NOW)
            .await
            .unwrap();
        assert!(r.messages.is_empty());
    }
    // Process 2 (restart): producer send -> the dispatcher claim must get them.
    {
        let reg2 = Arc::new(EngineRegistry::new(dbs.clone()));
        let be2 = SqliteQueueBackend::new(reg2);
        let s = be2
            .send(
                &q,
                &[msg(b"m1", "text"), msg(b"m2", "text"), msg(b"m3", "text")],
                NOW + 1_000,
            )
            .await
            .unwrap();
        assert_eq!(s.inserted, 3);
        let r = be2
            .claim(
                &q,
                "vrt-serve",
                30_000,
                5,
                Some(4 << 20),
                60_000,
                NOW + 2_000,
            )
            .await
            .unwrap();
        assert_eq!(r.messages.len(), 3, "claim after restart must see the existing messages");
        // Lease CAS: same holder, unexpired -> renew (epoch stays at the first takeover value 1).
        assert_eq!(r.dispatch_epoch, 1, "renewal by the same holder must not bump the epoch again");
    }
    // Process 3 (restart + dispatch lease expired): after takeover epoch++, claim works normally.
    {
        let reg3 = Arc::new(EngineRegistry::new(dbs.clone()));
        let be3 = SqliteQueueBackend::new(reg3);
        let r = be3
            .claim(
                &q,
                "vrt-serve",
                30_000,
                5,
                Some(4 << 20),
                60_000,
                NOW + 40_000,
            )
            .await
            .unwrap();
        assert_eq!(r.dispatch_epoch, 2, "takeover on expiry must bump the epoch");
    }
}

/// T37b-r5: platform-side return -- `release` clears the lease, delays `visible_at`,
/// and `attempts-1` compensates the +1 of claim (atomic in the same UPDATE). Platform failures where the consumer did not
/// run neither burn the retry budget nor go to the dead-letter table.
#[tokio::test]
async fn release_compensates_attempt_and_delays_visibility() {
    let tmp = TempDir::new().unwrap();
    let (_reg, be) = backend(&tmp);
    let q = qid();
    let mut m1 = msg(b"a", "text");
    m1.id = Some("m1".into());
    let mut m2 = msg(b"b", "text");
    m2.id = Some("m2".into());
    be.send(&q, &[m1, m2], NOW).await.unwrap();
    // claim consumes attempts (+1=1).
    let c = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW)
        .await
        .unwrap();
    assert_eq!(c.messages.len(), 2);
    assert!(c.messages.iter().all(|m| m.attempts == 1));
    // release: attempts is compensated back to 0, visible_at is pushed to now+5s.
    be.release(
        &q,
        &c.lease_id,
        &[("m1".into(), 5), ("m2".into(), 5)],
        NOW + 10,
    )
    .await
    .unwrap();
    // Claim immediately -- the message is invisible (visible_at in the future) -> empty batch;
    // the dispatch lease is still held by h.
    let c2 = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW + 20)
        .await
        .unwrap();
    assert!(c2.messages.is_empty(), "released msgs must stay invisible");
    // visible expires -> claim again: attempts=1 (0 after compensation + claim's
    // +1) -- after one platform-failure round attempts nets +0.
    let c3 = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW + 6_000)
        .await
        .unwrap();
    assert_eq!(c3.messages.len(), 2);
    assert!(
        c3.messages.iter().all(|m| m.attempts == 1),
        "attempts must equal 1 after release+reclaim (platform failure is free)"
    );
    // Normal consumption -> ack.
    be.ack(&q, &c3.lease_id, &["m1".into(), "m2".into()], NOW + 6_000)
        .await
        .unwrap();
    let m = be.metrics(&q, NOW + 7_000).await.unwrap();
    assert_eq!(m.backlog_count, 0);
}

/// release's lease guard: wrong lease / expired lease -> LeaseLost, the message
/// is left as-is (settled by the real holder or redelivered on expiry).
#[tokio::test]
async fn release_rejects_stale_lease() {
    let tmp = TempDir::new().unwrap();
    let (_reg, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"a", "text");
    m.id = Some("m1".into());
    be.send(&q, &[m], NOW).await.unwrap();
    let c = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW)
        .await
        .unwrap();
    let err = be
        .release(&q, "not-the-lease", &[("m1".into(), 0)], NOW + 10)
        .await
        .unwrap_err();
    assert!(matches!(err, QueueError::LeaseLost), "{err:?}");
    // The original message is still held by the real lease -> ack completes normally.
    be.ack(&q, &c.lease_id, &["m1".into()], NOW + 20)
        .await
        .unwrap();
}

#[tokio::test]
async fn wire_internal_stats_reports_d1_shape_and_queue_metrics() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [9u8; 32]));
    let db = "01jqqm3b4y6zq8f0v2w5x7k9ae";
    let q = "q".repeat(26);

    for sql in ["CREATE TABLE t(a TEXT)", "INSERT INTO t VALUES('x')"] {
        let (code, body) = oneshot(
            app.clone(),
            "POST",
            &format!("/dbs/{db}/execute"),
            serde_json::json!({"sql": sql}),
        )
        .await;
        assert_eq!(code, 200, "{body}");
    }

    let (code, _) = oneshot(
        app.clone(),
        "POST",
        &format!("/queues/{q}/send"),
        serde_json::json!({"messages":[
            {"id":"m1","body":hex::encode(b"hi"),"contentType":"text","delaySecs":0}
        ]}),
    )
    .await;
    assert_eq!(code, 200);

    let (code, body) = oneshot(
        app.clone(),
        "POST",
        "/internal/stats",
        serde_json::json!({"d1":[db, "not-a-db"], "queues":[q, "short"]}),
    )
    .await;
    assert_eq!(code, 200, "{body}");
    assert_eq!(body["success"].as_bool(), Some(true));

    let dbs = body["databases"].as_array().unwrap();
    assert_eq!(dbs.len(), 2);
    assert_eq!(dbs[0]["databaseId"].as_str(), Some(db));
    assert_eq!(dbs[0]["stats"]["tableCount"].as_i64(), Some(1), "{body}");
    assert_eq!(dbs[0]["stats"]["tables"][0].as_str(), Some("t"));
    assert!(
        dbs[0]["stats"]["databaseBytes"].as_i64().unwrap() > 0,
        "{body}"
    );
    assert_eq!(
        dbs[1]["error"]["code"].as_str(),
        Some("VRT_INVALID_REQUEST"),
        "{body}"
    );

    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0]["queueId"].as_str(), Some(q.as_str()));
    assert_eq!(queues[0]["metrics"]["backlogCount"].as_i64(), Some(1));
    assert_eq!(
        queues[1]["error"]["code"].as_str(),
        Some("VRT_INVALID_REQUEST"),
        "{body}"
    );
}
