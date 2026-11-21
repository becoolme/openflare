//! RV-R71 item 2: `EngineRegistry::get` cold open is single-flight and
//! never holds the global registry lock across claim / S3 restore.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use vrt_objstore::{MemoryStore, ObjectStore, StoreError};
use vrt_sql::clock::TestClock;
use vrt_sql::engine::{EngineError, EngineRegistry, PersisterPair};
use vrt_sql::paths::queue_root_for;
use vrt_sql::paths::ResourceKind::{self, D1};
use vrt_sql::persist::{PersistConfig, Persister};

const DB_A: &str = "abcdefghijklmnopqrstuvwxyz";
const DB_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbb";
const DB_C: &str = "cccccccccccccccccccccccccc";

/// A store whose behaviour is steered per database id:
///  - `park_get`: every `get` of a key containing this id blocks until
///    `release()`;
///  - `park_after_create`: the first `put_if_none_match` of a key
///    containing this id completes, THEN parks until `release()`;
///  - `fail_get`: the first `get` of a key containing this id fails
///    retryably.
struct SteerStore {
    inner: MemoryStore,
    park_get: Mutex<Option<String>>,
    park_after_create: Mutex<Option<String>>,
    fail_get: Mutex<Option<String>>,
    open: Mutex<bool>,
    cv: Condvar,
    entered: AtomicUsize,
    gets: AtomicUsize,
}

impl SteerStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryStore::new(),
            park_get: Mutex::new(None),
            park_after_create: Mutex::new(None),
            fail_get: Mutex::new(None),
            open: Mutex::new(false),
            cv: Condvar::new(),
            entered: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        })
    }
    fn park(&self) {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let mut g = self.open.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }
    fn wait_entered(&self, n: usize) {
        let t = Instant::now();
        while self.entered.load(Ordering::SeqCst) < n {
            assert!(t.elapsed() < Duration::from_secs(10), "opener never parked");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn hit(slot: &Mutex<Option<String>>, key: &str) -> bool {
    slot.lock()
        .unwrap()
        .as_deref()
        .is_some_and(|s| key.contains(s))
}

#[async_trait::async_trait]
impl ObjectStore for SteerStore {
    async fn get(&self, key: &str) -> Result<Option<vrt_objstore::StoredObject>, StoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        {
            let mut f = self.fail_get.lock().unwrap();
            if f.as_deref().is_some_and(|s| key.contains(s)) {
                *f = None;
                return Err(StoreError::unavailable(true, "injected"));
            }
        }
        if hit(&self.park_get, key) {
            self.park();
        }
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> Result<Option<vrt_objstore::ObjectMeta>, StoreError> {
        self.inner.head(key).await
    }
    async fn put_if_none_match(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        let r = self.inner.put_if_none_match(key, body).await;
        let parked = {
            let mut p = self.park_after_create.lock().unwrap();
            if p.as_deref().is_some_and(|s| key.contains(s)) {
                *p = None;
                true
            } else {
                false
            }
        };
        if parked {
            self.park();
        }
        r
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

fn registry(root: PathBuf, store: Arc<SteerStore>) -> EngineRegistry {
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
    EngineRegistry::new(root.clone()).with_persisters(PersisterPair { d1, queue, k2 })
}

/// While DB_A's cold open is parked inside the object store, a cached
/// DB_B and a cold DB_C must both still be served (the global lock is
/// not held across claim/restore).
#[test]
fn slow_cold_open_does_not_block_other_databases() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SteerStore::new();
    let reg = registry(tmp.path().join("r"), store.clone());
    let cached = reg.get(D1, DB_B).unwrap(); // cached before the stall
    *store.park_get.lock().unwrap() = Some(DB_A.into());

    let r2 = reg.clone();
    let opener = std::thread::spawn(move || r2.get(D1, DB_A).map(|e| e.lease_epoch()));
    store.wait_entered(1);

    let t = Instant::now();
    let again = reg.get(D1, DB_B).unwrap();
    assert!(Arc::ptr_eq(&cached, &again));
    let cold_c = reg.get(D1, DB_C).unwrap();
    assert_eq!(cold_c.lease_epoch(), 1);
    assert!(
        t.elapsed() < Duration::from_secs(5),
        "other databases must not wait for the parked open"
    );

    store.release();
    assert_eq!(opener.join().unwrap().unwrap(), 1);
}

/// N concurrent callers for the same cold key share ONE claim/restore.
#[test]
fn same_key_callers_share_one_open() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SteerStore::new();
    let reg = registry(tmp.path().join("r"), store.clone());
    *store.park_get.lock().unwrap() = Some(DB_A.into());

    let mut hs = vec![];
    for _ in 0..6 {
        let r = reg.clone();
        hs.push(std::thread::spawn(move || r.get(D1, DB_A)));
    }
    store.wait_entered(1);
    // Give the other five time to reach the slot; they must not also
    // enter the store.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        store.entered.load(Ordering::SeqCst),
        1,
        "exactly one opener"
    );
    store.release();
    let engines: Vec<_> = hs.into_iter().map(|h| h.join().unwrap().unwrap()).collect();
    for e in &engines[1..] {
        assert!(Arc::ptr_eq(&engines[0], e));
    }
    assert_eq!(engines[0].lease_epoch(), 1, "one claim -> epoch 1, not 6");
}

/// A same-key waiter fails closed (retryable 503) at its deadline
/// while the opener is still stuck; the opener is unaffected.
#[test]
fn waiter_times_out_at_deadline_while_open_in_flight() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SteerStore::new();
    let reg = registry(tmp.path().join("r"), store.clone());
    *store.park_get.lock().unwrap() = Some(DB_A.into());
    let r2 = reg.clone();
    let opener = std::thread::spawn(move || r2.get(D1, DB_A).is_ok());
    store.wait_entered(1);

    let t = Instant::now();
    let err = reg
        .get_until(D1, DB_A, Instant::now() + Duration::from_millis(150))
        .err()
        .unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    assert!(t.elapsed() < Duration::from_secs(3));

    store.release();
    assert!(opener.join().unwrap());
    assert!(reg.get(D1, DB_A).is_ok());
}

/// A failed open is shared with the current waiters, then the slot is
/// cleared so the next call retries (and can succeed).
#[test]
fn failed_open_clears_placeholder_and_is_retryable() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SteerStore::new();
    let reg = registry(tmp.path().join("r"), store.clone());
    *store.fail_get.lock().unwrap() = Some(DB_A.into());
    let err = reg.get(D1, DB_A).err().unwrap();
    assert!(matches!(err, EngineError::Unavailable(_)), "{err:?}");
    let e = reg.get(D1, DB_A).expect("retry after failure must work");
    assert_eq!(e.lease_epoch(), 1);
}

/// DROP lands while the (lock-free) open is in flight: the freshly
/// opened engine must be discarded, never cached; the caller and every
/// later call see Dropped.
#[test]
fn drop_during_open_discards_the_fresh_engine() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SteerStore::new();
    let reg = registry(tmp.path().join("r"), store.clone());
    *store.park_after_create.lock().unwrap() = Some(DB_A.into());
    let r2 = reg.clone();
    let opener = std::thread::spawn(move || r2.get(D1, DB_A).err().map(|e| format!("{e:?}")));
    store.wait_entered(1);

    // DROP while the opener holds a freshly claimed lease.
    let r3 = reg.clone();
    let dropper = std::thread::spawn(move || r3.drop_db(D1, DB_A));
    let dropped = dropper.join().unwrap();
    assert!(dropped.is_ok(), "{dropped:?}");

    store.release();
    let opened = opener.join().unwrap();
    assert_eq!(opened.as_deref(), Some("Dropped"), "{opened:?}");
    assert!(matches!(
        reg.get(D1, DB_A).err().unwrap(),
        EngineError::Dropped
    ));
}
