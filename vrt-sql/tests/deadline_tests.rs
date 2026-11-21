//! RV-R71 item 1: D1/queue SQL never runs on a tokio worker without a
//! deadline; slow durable storage maps to fixed retryable 503 codes and
//! never to a success after the deadline.

use axum::http::StatusCode;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use vrt_objstore::{MemoryStore, ObjectStore, StoreError};
use vrt_sql::clock::TestClock;
use vrt_sql::engine::{EngineRegistry, PersisterPair};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::{self, D1};
use vrt_sql::persist::{PersistConfig, Persister};
use vrt_sql::queue::{IncomingMsg, QueueBackend, QueueError, QueueId, SqliteQueueBackend};
use vrt_sql::wire::{self, AppState, TrustedCallCtx};

const DB_A: &str = "abcdefghijklmnopqrstuvwxyz";
const DB_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbb";

/// Store whose segment uploads (`put_if_none_match`) for a chosen id
/// hang (async, cancellable) until the stall is cleared.
struct StallStore {
    inner: MemoryStore,
    stall: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl ObjectStore for StallStore {
    async fn get(&self, key: &str) -> Result<Option<vrt_objstore::StoredObject>, StoreError> {
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> Result<Option<vrt_objstore::ObjectMeta>, StoreError> {
        self.inner.head(key).await
    }
    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        let stalled = self
            .stall
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|s| key.contains(s) && key.contains("seg"));
        if stalled {
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
        self.inner.put_if_none_match(key, body).await
    }
    async fn put_if_match(
        &self,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<String, StoreError> {
        self.inner.put_if_match(key, expected, body).await
    }
    async fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, body).await
    }
    async fn list(&self, prefix: &str) -> Result<vrt_objstore::ListPage, StoreError> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

fn registry(root: PathBuf, store: Arc<StallStore>) -> Arc<EngineRegistry> {
    let store: Arc<dyn ObjectStore> = store;
    let clock = Arc::new(TestClock::new(1_000_000));
    let cfg = PersistConfig {
        lease_ms: 10_000,
        clock_skew_margin_ms: 500,
        holder: "A".into(),
        checkpoint_every_commits: 0,
        retention_ms: 60_000,
        snapshot_max_bytes: 1 << 30,
    };
    let d1 = Arc::new(Persister::new(
        store.clone(),
        cfg.clone(),
        clock.clone(),
        root.clone(),
        D1,
    ));
    let queue = Arc::new(Persister::new(
        store.clone(),
        cfg.clone(),
        clock.clone(),
        queue_root_for(&root),
        ResourceKind::Queue,
    ));
    let k2 = Arc::new(Persister::new(
        store,
        cfg,
        clock,
        vrt_sql::paths::k2_root_for(&root),
        ResourceKind::K2,
    ));
    Arc::new(EngineRegistry::new(root).with_persisters(PersisterPair { d1, queue, k2 }))
}

fn setup() -> (tempfile::TempDir, Arc<StallStore>, Arc<AppState>) {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(StallStore {
        inner: MemoryStore::new(),
        stall: Mutex::new(None),
    });
    let mut st = AppState::new(tmp.path().join("r"), [3u8; 32]);
    st.registry = registry(tmp.path().join("r"), store.clone());
    (tmp, store, Arc::new(st))
}

fn ctx(ms: u64) -> TrustedCallCtx {
    TrustedCallCtx::test(Instant::now() + Duration::from_millis(ms))
}

async fn q(st: &AppState, db: &str, sql: &str, ms: u64) -> wire::WireResponse {
    let body = json!({ "sql": sql }).to_string();
    wire::handle_d1(st, db, None, ctx(ms), body.as_bytes()).await
}

/// A durable publish that hangs past the deadline: fail closed with the
/// fixed UNKNOWN-outcome code, within the deadline budget; other
/// databases keep working on the same registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_timeout_is_result_unknown_and_isolated() {
    let (_tmp, store, st) = setup();
    assert_eq!(
        q(&st, DB_A, "CREATE TABLE t(a)", 5_000).await.status,
        StatusCode::OK
    );
    *store.stall.lock().unwrap() = Some(DB_A.into());

    let t = Instant::now();
    let r = q(&st, DB_A, "INSERT INTO t VALUES(1)", 400).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{:?}", r.body);
    assert_eq!(r.body["success"], false);
    assert_eq!(r.body["code"], "VRT_SQL_RESULT_UNKNOWN");
    assert_eq!(r.body["retryable"], true);
    assert!(r.bookmark.is_none(), "no bookmark / success after deadline");
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());

    // Another database is unaffected.
    let b = q(&st, DB_B, "CREATE TABLE t(a)", 5_000).await;
    assert_eq!(b.status, StatusCode::OK, "{:?}", b.body);
}

/// While one request holds the writer lock inside a hung publish, a
/// queued request for the same database gives up at ITS deadline with
/// the pre-commit timeout code (retry-safe) instead of pinning a thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_wait_times_out_with_vrt_sql_timeout() {
    let (_tmp, store, st) = setup();
    assert_eq!(
        q(&st, DB_A, "CREATE TABLE t(a)", 5_000).await.status,
        StatusCode::OK
    );
    *store.stall.lock().unwrap() = Some(DB_A.into());

    let st2 = st.clone();
    let holder = tokio::spawn(async move { q(&st2, DB_A, "INSERT INTO t VALUES(1)", 3_000).await });
    tokio::time::sleep(Duration::from_millis(400)).await; // holder is inside publish

    let t = Instant::now();
    let r = q(&st, DB_A, "SELECT 1", 300).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{:?}", r.body);
    assert_eq!(r.body["code"], "VRT_SQL_TIMEOUT");
    assert_eq!(r.body["retryable"], true);
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());

    let h = holder.await.unwrap();
    assert_eq!(h.body["code"], "VRT_SQL_RESULT_UNKNOWN");
}

/// Queue ops share the same bounded path: a hung publish yields a
/// retryable Unavailable carrying the fixed code, never Ok.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_send_publish_timeout_is_retryable_unknown() {
    let (_tmp, store, st) = setup();
    let be = SqliteQueueBackend::new(st.registry.clone()).with_op_budget_ms(400);
    let id = QueueId::parse(&"q".repeat(26)).unwrap();
    let m = |b: &[u8]| IncomingMsg {
        id: None,
        body: b.to_vec(),
        content_type: "text".into(),
        delay_secs: 0,
        timestamp_ms: None,
    };
    // warm: creates the queue db (publishes normally).
    let warm = SqliteQueueBackend::new(st.registry.clone());
    warm.send(&id, &[m(b"a")], 1_700_000_000_000).await.unwrap();
    *store.stall.lock().unwrap() = Some("qqqqqqqqqq".into());

    let t = Instant::now();
    let e = be
        .send(&id, &[m(b"b")], 1_700_000_000_000)
        .await
        .unwrap_err();
    match e {
        QueueError::Unavailable { retryable, reason } => {
            assert!(retryable);
            assert_eq!(reason, "VRT_SQL_RESULT_UNKNOWN");
        }
        other => panic!("{other:?}"),
    }
    assert!(t.elapsed() < Duration::from_secs(3));
}

/// The forwarded budget header is shrink-only: it can never enlarge the
/// service's own budget, and junk / zero is ignored.
#[test]
fn deadline_header_is_shrink_only() {
    let own = Duration::from_secs(30);
    assert_eq!(wire::clamp_budget(own, None), own);
    assert_eq!(
        wire::clamp_budget(own, Some("500")),
        Duration::from_millis(500)
    );
    assert_eq!(wire::clamp_budget(own, Some("999999999")), own);
    assert_eq!(wire::clamp_budget(own, Some("0")), own);
    assert_eq!(wire::clamp_budget(own, Some("-5")), own);
    assert_eq!(wire::clamp_budget(own, Some("abc")), own);
}
