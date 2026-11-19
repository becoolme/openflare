//! T37 QueueBackend 对抗测试：限额边界 / 幂等 / lease 语义 /
//! 并发 claim / 库类型门禁 / 持久化恢复 / tombstone / wire 端点。

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

/// T37r：D1+Queue 双 Persister（同一 store/cfg/clock；root 各分面）。
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

// ---------- 限额边界 ----------

#[tokio::test]
async fn limits_boundaries() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();

    // 单消息 128_000 通过 / +1 拒
    assert!(be
        .send(&q, &[msg(&vec![b'x'; MSG_MAX_BYTES], "bytes")], NOW)
        .await
        .is_ok());
    assert!(matches!(
        be.send(&q, &[msg(&vec![b'x'; MSG_MAX_BYTES + 1], "bytes")], NOW)
            .await,
        Err(QueueError::TooLarge)
    ));

    // 100 条通过 / 101 拒
    let hundred: Vec<IncomingMsg> = (0..BATCH_MAX_MSGS).map(|_| msg(b"a", "text")).collect();
    assert!(be.send(&q, &hundred, NOW).await.is_ok());
    let mut over = hundred.clone();
    over.push(msg(b"a", "text"));
    assert!(matches!(
        be.send(&q, &over, NOW).await,
        Err(QueueError::TooLarge)
    ));

    // 批总 288_000 通过 / +1 拒（各自消息合法）
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

    // delay 边界
    let mut d = msg(b"a", "text");
    d.delay_secs = DELAY_MAX_SECS;
    assert!(be.send(&q, &[d], NOW).await.is_ok());
    let mut d = msg(b"a", "text");
    d.delay_secs = DELAY_MAX_SECS + 1;
    assert!(matches!(
        be.send(&q, &[d], NOW).await,
        Err(QueueError::InvalidInput(_))
    ));

    // 非法 content type / 非法 id / 空批
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

    // 被拒的批一个都不入队——backlog 只含合法批。
    let m = be.metrics(&q, NOW).await.unwrap();
    assert_eq!(m.backlog_count, 1 + 100 + 100 + 1, "{}", m.backlog_count);
}

// ---------- 幂等 ----------

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
    // 注入持久化失败 → send 必 retryable 且整批不落。
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
    // 崩溃语义：h1 进程消失——引擎析构释放 flock（manifest lease
    // 不释放，靠 h2 推进时钟自然过期=接管路径）。
    drop(be);
    drop(reg);
    // h2 时钟推进到 h1 lease 过期之后（崩溃接管路径）→ 立即接管。
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let clock2: Arc<dyn Clock> = Arc::new(TestClock::new(now_ms + 11_000));
    // 新引擎接管后：只有第一条 ok 消息，p1/p2 不在。
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

// ---------- lease 语义 ----------

#[tokio::test]
async fn lease_expiry_reclaim_and_stale_ack() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"work", "text");
    m.id = Some("m1".into());
    be.send(&q, &[m], NOW).await.unwrap();

    // A claims（ttl=10s）
    let a = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW)
        .await
        .unwrap();
    assert_eq!(a.messages.len(), 1);
    assert_eq!(a.messages[0].attempts, 1);
    // lease 未过期时 B 取不到
    let b_early = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW + 5_000)
        .await
        .unwrap();
    assert!(b_early.messages.is_empty());
    // lease 过期 → B 重取 attempts+1
    let b = be
        .claim(&q, "t", 300_000, 10, None, 10_000, NOW + 11_000)
        .await
        .unwrap();
    assert_eq!(b.messages.len(), 1);
    assert_eq!(b.messages[0].attempts, 2);
    // A 旧 lease ack → LeaseLost，消息仍在 B 租约下
    assert!(matches!(
        be.ack(&q, &a.lease_id, &["m1".into()], NOW + 12_000).await,
        Err(QueueError::LeaseLost)
    ));
    // B 正常 ack
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
    // retry delay=60s → 不可见 → 60s 后可见
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
    // 旧 lease retry → LeaseLost
    assert!(matches!(
        be.retry(&q, &c.lease_id, &[("r1".into(), 0)], NOW + 62_000)
            .await,
        Err(QueueError::LeaseLost)
    ));
    // dead-letter：消息出 backlog、入审记表
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

// ---------- 库类型门禁 ----------

#[tokio::test]
async fn d1_db_rejected_by_queue_ops_and_internal_tables_hidden() {
    let tmp = TempDir::new().unwrap();
    let reg = Arc::new(EngineRegistry::new(tmp.path().join("dbs")));
    let q = qid();
    // D1 面：建用户表 + 写行。
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
    // 有用户表 → 队列 op 拒（库类型不匹配，fail closed）。
    for r in [
        be.send(&q, &[msg(b"a", "text")], NOW).await.err(),
        be.claim(&q, "t", 300_000, 1, None, 1000, NOW).await.err(),
        be.metrics(&q, NOW).await.err(),
    ] {
        assert!(matches!(r, Some(QueueError::InvalidInput(_))), "{r:?}");
    }
    // 反向：queue 库上用户 SQL 读 `_vrt_q_*` 被拒（保留前缀）。
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

// ---------- 持久化恢复 ----------

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
    drop(reg); // 引擎析构 → graceful lease release
               // 新 registry（=重启）恢复：backlog=2。
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

// ---------- wire 端点 ----------

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

    // 非法 id 入口拒绝
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
    // 旧 lease 再 ack → 409 LeaseLost
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

// ---------- T37r：ResourceKind 命名空间分离 ----------

fn sctx() -> TxnCtx {
    TxnCtx {
        epoch: 1,
        deadline: Instant::now() + Duration::from_secs(30),
        dedup: None,
    }
}

/// 同一 id 以 D1 与 Queue 打开：互不相交的两个库——本地目录分面、
/// 表集合分面、drop/tombstone 分面、diag 带 kind。
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

    // 同 id 的 D1 库：全新空库——无 _vrt_q_*，可建用户表。
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
        "D1 库绝不含 queue 内部表：{names:?}"
    );
    // queue 数据不受同 id D1 打开影响。
    assert_eq!(be.metrics(&id, NOW).await.unwrap().backlog_count, 1);

    // 本地路径分面：两个文件都存在且路径不同。
    let dbs = tmp.path().join("dbs");
    let d1f = vrt_sql::paths::derive_db_file(&dbs, id.as_str()).unwrap();
    let qf =
        vrt_sql::paths::derive_db_file(&vrt_sql::paths::queue_root_for(&dbs), id.as_str()).unwrap();
    assert!(d1f.exists() && qf.exists(), "d1={d1f:?} q={qf:?}");
    assert_ne!(d1f, qf);
    assert!(d1f.to_string_lossy().contains("dbs/"));
    assert!(qf.to_string_lossy().contains("queues/"));

    // diag：两种 kind 各自成条目且带 kind。
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

    // drop queue：同 id D1 库不受影响；queue 侧 tombstone 生效。
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

/// `/dbs/{id}/query` 端点走 D1 kind——结构上无法触及 queue 数据：
/// 同 id 的 queue 已写入时，D1 端点面对的是独立空库。
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
    // D1 端点打开同 id → 独立库，无 _vrt_q_messages（连表都不存在）。
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
        "D1 端点绝不可见 queue 内部表：{names:?}"
    );
    // queue 数据仍在。
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

// ---------- T37r：客户端 timestamp 不可信 ----------

/// 伪造远古/未来 timestamp：client_ts 只作属性回显，created_at /
/// visible_at / oldestMessageTimestamp 一律服务端时钟。
#[tokio::test]
async fn forged_timestamp_cannot_affect_scheduling_or_metrics() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    // 远古 timestamp（epoch+1ms）——合法属性，但不参与任何调度/指标。
    let mut ancient = msg(b"old", "text");
    ancient.timestamp_ms = Some(1);
    assert_eq!(be.send(&q, &[ancient], NOW).await.unwrap().inserted, 1);
    let m = be.metrics(&q, NOW).await.unwrap();
    assert_eq!(
        m.oldest_message_timestamp_ms,
        Some(NOW),
        "oldestMessageTimestamp 必须由服务端 created_at 驱动"
    );
    let c = be
        .claim(&q, "t", 300_000, 1, None, 60_000, NOW)
        .await
        .unwrap();
    assert_eq!(c.messages.len(), 1);
    // consumer 回显 client_ts 属性（DLQ 转投语义）——不影响 lease/可见性。
    assert_eq!(c.messages[0].timestamp_ms, 1);
    // 未来 timestamp 超容忍（>now+5min）→ 拒绝而非截断。
    let mut future = msg(b"fut", "text");
    future.timestamp_ms = Some(NOW + 1_000_000_000);
    assert!(matches!(
        be.send(&q, &[future], NOW).await.err(),
        Some(QueueError::InvalidInput(_))
    ));
    // 容忍边界内（now+5min）→ 接受为属性，指标仍服务端时钟。
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

// ---------- T37r：queue 路由独立 body 上限 ----------

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

/// QUEUE_BODY_MAX = 576_000 + 25_600 + 256 = 601_856：
/// 最大合法批次通过；+1 字节（含垃圾非 JSON 体）在 JSON 解析前被 413。
#[tokio::test]
async fn wire_queue_body_max_boundary() {
    const LIMIT: usize = 601_856;
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let q = "q".repeat(26);

    // 最大合法批次：3 条消息共 288_000B（128K+128K+32K；单消息
    // 上限 128K），hex 合计 576_000 → 200。
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

    // 恰好 LIMIT 字节：合法 JSON + 尾部填充空格 → 仍通过（上限含头不含挤）。
    let json = serde_json::json!({"messages":[{"body":"","contentType":"text","delaySecs":0}]})
        .to_string();
    let padded = format!("{json}{}", " ".repeat(LIMIT - json.len()));
    assert_eq!(padded.len(), LIMIT);
    let (code, _) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), padded).await;
    assert_eq!(code, 200);

    // LIMIT+1 字节的垃圾（非 JSON）→ 413，发生在 JSON 解析之前
    // （若为解析层命中会是 400 invalid json，而非 413）。
    let junk = "x".repeat(LIMIT + 1);
    let (code, resp) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), junk).await;
    assert_eq!(code, 413, "{resp}");

    // LIMIT+1 的合法 JSON（填充到超限）→ 同样 413——上限在语义层之上。
    let over = format!("{json}{}", " ".repeat(LIMIT - json.len() + 1));
    let (code, _) = oneshot_raw(app.clone(), "POST", &format!("/queues/{q}/send"), over).await;
    assert_eq!(code, 413);

    // D1 端点上限未被放大：同进程 body_max=1MiB 默认下，D1 端点行为
    // 不变——>QUEUE_BODY_MAX 的 D1 请求仍按其自身 1MiB 上限判定。
    // （反向断言：queue 路由用的是 min(QUEUE_BODY_MAX, body_max)，
    //   601KB 的 D1 请求在其自身 1MiB 限制内，照常受理。）
    let d1_ok = serde_json::json!({"sql":"SELECT 1"}).to_string();
    let d1_padded = format!("{d1_ok}{}", " ".repeat(700_000 - d1_ok.len()));
    let (code, _) = oneshot_raw(
        app.clone(),
        "POST",
        &format!("/dbs/{}/query", "d".repeat(26)),
        d1_padded,
    )
    .await;
    assert_ne!(code, 413, "D1 端点不得受 queue 上限影响");
}

/// `/queues/{id}/drop` 只 tombstone queue 命名空间——cell-controller
/// 按 op payload.kind 路由；同 id 的 D1 库不受影响，反之亦然
///（T37r：命名空间分离前 queue DROP 会 tombstone D1 命名空间、
/// 泄漏 queue 库，同 id 真 D1 库存在时甚至误删）。
#[tokio::test]
async fn queue_drop_endpoint_kind_scoped() {
    let tmp = TempDir::new().unwrap();
    let app = wire::app(AppState::new(tmp.path().join("dbs"), [7u8; 32]));
    let id = "q".repeat(26);

    // queue 与同 id D1 库各有数据。
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

    // 先验证反向隔离：queue-only 语义下 /dbs/{id}/drop 只动 D1
    // 命名空间（dropped:false 或删掉 D1 库），queue 仍可用。
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
    // queue 侧 tombstone：一切 op 拒绝（410 VRT_QUEUE_DROPPED），幂等。
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
    // 幂等重放：tombstone 命中，dropped:false——同 /dbs/{id}/drop
    // 语义（wire_tests drop_endpoint_roundtrip_and_tombstone）。
    assert_eq!(
        (code, body["success"].as_bool(), body["dropped"].as_bool()),
        (200, Some(true), Some(false)),
        "drop idempotent: {body}"
    );
    // 同 id D1 库完好。
    let (code, body) = oneshot(
        app.clone(),
        "POST",
        &format!("/dbs/{id}/query"),
        serde_json::json!({"sql": "SELECT count(*) AS c FROM t"}),
    )
    .await;
    assert_eq!(code, 200, "d1 survives queue drop: {body}");

    // D1 drop 不触 queue tombstone 集——再建同 id queue 被拒（D1
    // tombstone 属于另一命名空间，不互相可见）。
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
    // D1 错误面是 HTTP 200 + success:false（wire 语义）。
    assert_eq!(body["success"].as_bool(), Some(false), "{body}");
    assert_eq!(body["error"].as_str(), Some("database dropped"), "{body}");
}

/// T37b：派发租约互斥、续约、过期接管、纯续约（max=0）。
#[tokio::test]
async fn dispatch_lease_exclusion_renew_takeover() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    let mut m = msg(b"d", "text");
    m.id = Some("d1".into());
    be.send(&q, &[m], NOW).await.unwrap();

    // A 持约（ttl=10s）→ epoch=1，拿到消息
    let a = be
        .claim(&q, "disp-a", 10_000, 10, None, 60_000, NOW)
        .await
        .unwrap();
    assert_eq!(a.dispatch_epoch, 1);
    assert_eq!(a.messages.len(), 1);
    // B 同期 claim → DispatchLeaseLost（持约者与到期时刻可观察）
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
    // A 到期前续约 → expires 推后，B 到原到期点仍不能接管
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
    // A 不再续约 → 过期后 B 接管派发租约：epoch++。但 B 当刻仍取
    // 不到消息——派发租约接管不等于消息租约失效（A claim 的消息
    // 租约 60s 未满，分层语义：B 要等消息租约过期才重取）。
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
    // 旧 A 结算被拒——msg lease 属于 B 的重新 claim
    assert!(matches!(
        be.ack(&q, &a.lease_id, &["d1".into()], NOW + 62_000).await,
        Err(QueueError::LeaseLost)
    ));
}

/// T37b：派发租约参数校验——holder 字符集/长度、ttl、max 边界。
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
    // 边界：holder=64 字符、ttl=300_000、max=0 均合法
    assert!(be
        .claim(&q, &"h".repeat(64), 300_000, 0, None, 60_000, NOW)
        .await
        .is_ok());
}

/// T37b-r：claim `maxBytes` —— 按 RAW body 字节累计、≥1 保底、
/// 预算耗尽即截断（不跨批补凑）。
#[tokio::test]
async fn claim_max_bytes_budget() {
    let tmp = TempDir::new().unwrap();
    let (_r, be) = backend(&tmp);
    let q = qid();
    // 3 × 100B + 1 × 128KB 大消息。
    // 递增 created_at 定序（ORDER BY visible_at,created_at——
    // client_ts 不参与排序，平局按 PK 不能假设顺序）。
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

    // 预算 250B → 只取 s0+s1（s2 会使累计 300>250）。
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

    // 预算 50B < 单条 100B —— ≥1 保底：仍返回 s2。
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

    // 预算 1B —— 128KB 大消息照样可取（保底优先于预算）。
    let c3 = be
        .claim(&q, "h", 10_000, 10, Some(1), 60_000, NOW + 10)
        .await
        .unwrap();
    assert_eq!(c3.messages.len(), 1);
    assert_eq!(c3.messages[0].id, "big");
    be.ack(&q, &c3.lease_id, &["big".into()], NOW + 10)
        .await
        .unwrap();

    // 边界：0 / >32MB 拒绝；None 兼容旧行为。
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

/// T37b-r4：e2e 复现——dispatcher 已持派发租约时 vrt-sql 重启
///（新 EngineRegistry、同 db 目录；模拟 SIGTERM 后新进程打开同库）：
/// 消息必须仍在、派发租约 CAS 对同 holder 正常续约、claim 返回消息。
/// 覆盖旧 fake `recovers_after_sql_restart` 未触达的真实引擎路径。
#[tokio::test]
async fn claim_recovers_across_registry_restart() {
    let tmp = TempDir::new().unwrap();
    let dbs = tmp.path().join("dbs");
    let q = qid();
    // 进程 1：dispatcher claim → `_vrt_q_dispatch` holder=vrt-serve 落盘。
    {
        let reg = Arc::new(EngineRegistry::new(dbs.clone()));
        let be = SqliteQueueBackend::new(reg);
        let r = be
            .claim(&q, "vrt-serve", 30_000, 5, Some(4 << 20), 60_000, NOW)
            .await
            .unwrap();
        assert!(r.messages.is_empty());
    }
    // 进程 2（重启）：producer send → dispatcher claim 必须取到。
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
        assert_eq!(r.messages.len(), 3, "restart 后 claim 必须看到存量消息");
        // 租约 CAS：同 holder 未过期 → 续约（epoch 保持首次接管值 1）。
        assert_eq!(r.dispatch_epoch, 1, "同 holder 续约不得再 bump epoch");
    }
    // 进程 3（重启 + 派发租约已过期）：接管 epoch++ 后 claim 正常。
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
        assert_eq!(r.dispatch_epoch, 2, "过期接管必须 bump epoch");
    }
}

/// T37b-r5：平台侧归还——`release` 清 lease、`visible_at` 延迟、
/// `attempts-1` 补偿 claim 的 +1（同一 UPDATE 原子）。consumer 未
/// 执行的平台失败不烧重试预算、不进死信。
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
    // claim 消费 attempts(+1=1)。
    let c = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW)
        .await
        .unwrap();
    assert_eq!(c.messages.len(), 2);
    assert!(c.messages.iter().all(|m| m.attempts == 1));
    // release：attempts 补偿回 0，visible_at 推到 now+5s。
    be.release(
        &q,
        &c.lease_id,
        &[("m1".into(), 5), ("m2".into(), 5)],
        NOW + 10,
    )
    .await
    .unwrap();
    // 立即 claim —— 消息不可见（visible_at 未来）→ 空批；
    // 派发租约仍由 h 持有。
    let c2 = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW + 20)
        .await
        .unwrap();
    assert!(c2.messages.is_empty(), "released msgs must stay invisible");
    // visible 到期 → 重新 claim：attempts=1（补偿后 0 + claim 的
    // +1）——平台失败一轮下来 attempts 净增 0。
    let c3 = be
        .claim(&q, "h", 10_000, 10, None, 60_000, NOW + 6_000)
        .await
        .unwrap();
    assert_eq!(c3.messages.len(), 2);
    assert!(
        c3.messages.iter().all(|m| m.attempts == 1),
        "attempts must equal 1 after release+reclaim (platform failure is free)"
    );
    // 正常消费 → ack。
    be.ack(&q, &c3.lease_id, &["m1".into(), "m2".into()], NOW + 6_000)
        .await
        .unwrap();
    let m = be.metrics(&q, NOW + 7_000).await.unwrap();
    assert_eq!(m.backlog_count, 0);
}

/// release 的 lease 守卫：错 lease / 过期 lease → LeaseLost，消息
/// 原状保留（由真实 holder 结算或到期重投）。
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
    // 原消息仍由真 lease 持有 → ack 正常完成。
    be.ack(&q, &c.lease_id, &["m1".into()], NOW + 20)
        .await
        .unwrap();
}
