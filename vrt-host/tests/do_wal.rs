//! WAL1 integration: real sqlite WAL dbs + MemoryStore drive the
//! full pin→parse→cut→upload→replay pipeline. No workerd needed —
//! the tailer is an ordinary external reader (the property the PoC
//! `do_wal_poc.rs` verified holds against real workerd too).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use vrt_host::do_wal::{replay_for_restore, DoWalReplicator, WalCfg};
use vrt_objstore::{MemoryStore, ObjectStore};
use vrt_sql::seg;

fn tmpdir(tag: &str) -> PathBuf {
    let d = Path::new("target/tmp-wal1r4").join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn now_cell() -> (Arc<AtomicU64>, Arc<dyn Fn() -> u64 + Send + Sync>) {
    let t = Arc::new(AtomicU64::new(1_000_000));
    let t2 = t.clone();
    (t, Arc::new(move || t2.load(Ordering::SeqCst)))
}

fn bump(t: &Arc<AtomicU64>, ms: u64) {
    t.fetch_add(ms, Ordering::SeqCst);
}

/// Create a WAL-mode db and return an open writer conn.
fn mk_db(dir: &Path, stem: &str) -> rusqlite::Connection {
    let p = dir.join(format!("{stem}.sqlite"));
    let c = rusqlite::Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .unwrap();
    c.execute_batch("CREATE TABLE IF NOT EXISTS t(k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    c
}

fn put(c: &rusqlite::Connection, k: &str, v: &str) {
    c.execute("INSERT OR REPLACE INTO t VALUES (?1,?2)", [k, v])
        .unwrap();
}
/// Copy db+wal together as a snapshot image — equivalent to an
/// online-backup image (wal frames visible in the copy) WITHOUT
/// checkpointing the live db, so the live wal generation continues.
fn img_copy(class_dir: &Path, img: &Path, stem: &str) {
    std::fs::create_dir_all(img).unwrap();
    std::fs::copy(
        class_dir.join(format!("{stem}.sqlite")),
        img.join(format!("{stem}.sqlite")),
    )
    .unwrap();
    let wal = class_dir.join(format!("{stem}.sqlite-wal"));
    if wal.exists() {
        std::fs::copy(&wal, img.join(format!("{stem}.sqlite-wal"))).unwrap();
    }
}
/// Restore an img_copy'd image (db + wal) into a fresh dir.
fn img_restore(img: &Path, dst: &Path, stem: &str) {
    std::fs::create_dir_all(dst).unwrap();
    std::fs::copy(
        img.join(format!("{stem}.sqlite")),
        dst.join(format!("{stem}.sqlite")),
    )
    .unwrap();
    let wal = img.join(format!("{stem}.sqlite-wal"));
    if wal.exists() {
        std::fs::copy(&wal, dst.join(format!("{stem}.sqlite-wal"))).unwrap();
    }
}

#[allow(dead_code)]
fn count_rows(db: &Path) -> usize {
    let c = rusqlite::Connection::open(db).unwrap();
    c.query_row("SELECT COUNT(*) FROM t", [], |r| r.get::<_, i64>(0))
        .unwrap() as usize
}

fn get_val(db: &Path, k: &str) -> Option<String> {
    let c = rusqlite::Connection::open(db).unwrap();
    c.query_row("SELECT v FROM t WHERE k=?1", [k], |r| r.get(0))
        .ok()
}

fn cfg() -> WalCfg {
    WalCfg {
        enabled: true,
        tick_ms: 0, // no throttle — tests drive ticks explicitly
        max_dbs: 8,
        max_pending_bytes: 256 << 20,
        max_segment_bytes: 1 << 20,
        upload_attempts: 2,
        tick_upload_budget_ms: 500,
        idle_stop_ms: u64::MAX,
        flush_max_ms: 5_000,
        gc_max_per_tick: 64,
        replay_max_bytes: 1 << 30,
            rebaseline_chain_bytes: 1 << 29,
        baseline_backoff_max_ms: 1_800_000,
    }
}

fn scope(class_dir: PathBuf) -> vrt_host::do_placement::PlacementScope {
    vrt_host::do_placement::PlacementScope {
        tenant: "team".into(),
        worker: "w".into(),
        class_key: "c".repeat(64),
        class_dir,
    }
}

/// WAL1-r3: full baseline lifecycle — emission under `seq` needs its
/// {B,E} anchor, which lands only at image-taken.
fn baseline(rep: &DoWalReplicator, sc: &vrt_host::do_placement::PlacementScope, seq: u64) {
    rep.on_snapshot_begin(&sc.class_key, seq);
    rep.on_snapshot_image_taken(&sc.class_key, seq);
    rep.on_snapshot_committed(&sc.class_key, seq);
}

fn seg_keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let page = vrt_objstore::block_on_store(store.list(prefix)).unwrap();
    assert!(!page.truncated, "test listing must be complete");
    let mut ks: Vec<String> = page.entries.into_iter().map(|e| e.key).collect();
    ks.sort();
    ks
}

/// Store that fails put_if_none_match — uploads permanently down.
struct FailingStore;
#[async_trait::async_trait]
impl ObjectStore for FailingStore {
    async fn get(
        &self,
        _: &str,
    ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
        Ok(None)
    }
    async fn head(
        &self,
        _: &str,
    ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
        Ok(None)
    }
    async fn put_if_none_match(
        &self,
        _: &str,
        _: &[u8],
    ) -> Result<String, vrt_objstore::StoreError> {
        Err(vrt_objstore::StoreError::unavailable(true, "down"))
    }
    async fn put_if_match(
        &self,
        _: &str,
        _: &str,
        _: &[u8],
    ) -> Result<String, vrt_objstore::StoreError> {
        Err(vrt_objstore::StoreError::unavailable(true, "down"))
    }
    async fn put(&self, _: &str, _: &[u8]) -> Result<String, vrt_objstore::StoreError> {
        Err(vrt_objstore::StoreError::unavailable(true, "down"))
    }
    async fn list(
        &self,
        _: &str,
    ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
        Ok(vrt_objstore::ListPage {
            entries: vec![],
            truncated: false,
        })
    }
    async fn delete(&self, _: &str) -> Result<(), vrt_objstore::StoreError> {
        Ok(())
    }
}

const CK: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

// ---------- core pipeline ----------

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_genesis_segments_never_adopted() {
    // P0-C: `{E}/0/` segments are NEVER adopted — a fresh WAL's
    // ckpt_seq==0 cannot prove completeness (WAL delete+recreate
    // restarts it). Replay skips them (warn+metric), never errors,
    // and an empty dir launches empty — flag-off parity.
    let dir = tmpdir("genesis");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    // No baseline yet → NOTHING is emitted (no genesis exception).
    assert!(
        seg_keys(&store, &format!("do/{CK}/wal/")).is_empty(),
        "segments must not exist before an own-epoch baseline"
    );
    // …but a stale {5}/0/ object (old build) may exist — craft one.
    let stale = vrt_sql::seg::Segment {
        database_id: {
            let h = vrt_objstore::sigv4::sha256_hex(format!("{CK}/obj1").as_bytes());
            h[..32].to_string()
        },
        epoch: 5,
        lsn: 1,
        prev_lsn: 0,
        page_size: 4096,
        wal_begin: 0,
        frames: vec![vrt_sql::seg::SegFrame {
            page_no: 1,
            commit_db_size: 4096,
            data: vec![0u8; 4096],
        }],
    };
    vrt_objstore::block_on_store(store.put_if_none_match(
        &format!("do/{CK}/wal/obj1/0000000000000005/0000000000000000/0000000000000001.seg"),
        &seg::encode(&stale),
    ))
    .unwrap();
    drop(rep);
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    let rep = replay_for_restore(&store, CK, &dir2, None, 5, 1 << 30, "team").unwrap();
    assert_eq!(rep.segs_applied, 0);
    assert_eq!(rep.stems_applied, 0);
    assert_eq!(rep.genesis_segments_skipped, 1);
    assert!(!dir2.join("obj1.sqlite").exists(), "empty dir stays empty");
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_flag_off_uploads_nothing() {
    let dir = tmpdir("off");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let mut c = cfg();
    c.enabled = false;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir);
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    assert!(seg_keys(&store, &format!("do/{CK}/")).is_empty());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_fenced_epoch_segments_never_replayed() {
    // Segments under epoch 5 exist; restore for writer_epoch 7 must
    // adopt NONE of them (fenced writer's late stream).
    let dir = tmpdir("fenced");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    assert!(!seg_keys(&store, &format!("do/{CK}/wal/")).is_empty());
    drop(rep);
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    let rep = replay_for_restore(&store, CK, &dir2, None, 7, 1 << 30, "team").unwrap();
    assert_eq!(rep.segs_applied, 0);
    assert_eq!(rep.stems_applied, 0);
    assert!(!dir2.join("obj1.sqlite").exists());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_gap_in_chain_truncates_at_last_contiguous_commit() {
    let dir = tmpdir("gap");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img");
    img_copy(&class_dir, &img, "obj1"); // image covers a=1
    rep.on_snapshot_image_taken(&sc.class_key, 42); // anchor {B,E}
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key); // seg1 {b}
    put(&conn, "c", "3");
    rep.tick(&sc.class_key); // seg2 {c}
    drop(rep);
    let keys = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/"),
    );
    let segs: Vec<&String> = keys.iter().filter(|k| k.ends_with(".seg")).collect();
    assert!(segs.len() >= 2, "{keys:?}");
    vrt_objstore::block_on_store(store.delete(segs[0])).unwrap(); // hole at head
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let _rep = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"), "image served");
    assert_eq!(
        get_val(&db, "b"),
        None,
        "hole at head → whole chain skipped"
    );
    assert_eq!(get_val(&db, "c"), None);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_missing_head_seg_means_no_replay() {
    let dir = tmpdir("headgap");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    drop(rep);
    let keys = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/"),
    );
    let segs: Vec<&String> = keys.iter().filter(|k| k.ends_with(".seg")).collect();
    vrt_objstore::block_on_store(store.delete(segs[0])).unwrap(); // lsn=1 gone
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rep = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    assert_eq!(rep.segs_applied, 0, "dead chain head → nothing applied");
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "a").as_deref(),
        Some("1")
    );
    assert_eq!(get_val(&dir2.join("obj1.sqlite"), "b"), None);
    conn.close().ok();
}

/// RV-R28 #3: a class prefix holding >65k WAL keys (many dead
/// stems/epochs not yet GC'd) must not break single-instance
/// replay — stem discovery goes through the delimiter listing
/// (`list_children`), NOT a flat prefix list that S3 truncates at
/// LIST_KEY_BOUND = 65_536 (which previously errored every restore
/// or silently dropped instances).
#[tokio::test(flavor = "current_thread")]
async fn wal_replay_correct_with_70k_class_prefix_keys() {
    let dir = tmpdir("r28-70k");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    drop(rep);
    // Flood the class prefix with 70k dead keys across hundreds of
    // dead stems — S3's flat-list bound (65_536) sits below this.
    let n = vrt_objstore::block_on_store(async {
        let mut n = 0usize;
        for i in 0..350 {
            for j in 0..200 {
                let k = format!(
                    "do/{CK}/wal/junk{i:04}/0000000000000003/{j:016x}/0000000000000001.seg"
                );
                store.put(&k, b"x").await?;
                n += 1;
            }
        }
        Ok::<usize, vrt_objstore::StoreError>(n)
    })
    .unwrap();
    assert_eq!(n, 70_000);
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rep = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"), "image served");
    assert_eq!(
        get_val(&db, "b").as_deref(),
        Some("2"),
        "post-baseline writes must replay past a 70k-key class prefix"
    );
    assert!(rep.segs_applied > 0);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_upload_failure_breaks_on_pending_cap_and_requests_baseline() {
    let dir = tmpdir("cap");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    let store: Arc<dyn ObjectStore> = Arc::new(FailingStore);
    let (_t, clock) = now_cell();
    let mut c = cfg();
    c.max_pending_bytes = 64 << 10; // 64KiB cap → a few commits breach it
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    let payload = "x".repeat(32 << 10);
    let mut broke = false;
    for i in 0..8 {
        put(&conn, &format!("k{i}"), &payload);
        rep.tick(&sc.class_key);
        if rep.take_needs_baseline(&sc.class_key) {
            broke = true;
            break;
        }
    }
    assert!(
        broke,
        "pending cap must break the stream + request baseline"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_gc_deletes_only_covered_segments() {
    let dir = tmpdir("gc");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    // fabricate keys: {stem}/{ep}/{seq}/{lsn}
    let mk = |stem, ep, sq, lsn| format!("do/{CK}/wal/{stem}/{ep:016x}/{sq:016x}/{lsn:016x}.seg");
    for k in [
        mk("o1", 2, 300, 1), // old epoch → delete
        mk("o1", 5, 100, 1), // covered by committed 200 → delete
        mk("o1", 5, 200, 1), // live tail → keep
        mk("o1", 5, 250, 1), // in-flight label → keep
        mk("o2", 5, 100, 2), // other stem covered → delete
    ] {
        vrt_objstore::block_on_store(store.put(&k, b"x")).unwrap();
    }
    rep.gc_segments(CK, 5, 200, || true);
    let left = seg_keys(&store, &format!("do/{CK}/wal/"));
    assert_eq!(
        left,
        vec![mk("o1", 5, 200, 1), mk("o1", 5, 250, 1)],
        "only covered/stale-epoch segs deleted: {left:?}"
    );
}

/// RV-R24: seq labels are wall-clock — a successor holder on a slower
/// clock emits tail seqs BELOW our committed_seq. GC must never touch
/// `epoch > our_epoch` segments/anchors: deleting them strips frames
/// the restore chain needs.
#[tokio::test(flavor = "current_thread")]
async fn wal_gc_never_deletes_newer_epoch_segments() {
    let dir = tmpdir("gc-newer-epoch");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let mk =
        |stem, ep, sq, lsn| format!("do/{CK}/wal/{stem}/{ep:016x}/{sq:016x}/{lsn:016x}.seg");
    let anchor = |stem, ep, sq| format!("do/{CK}/wal/{stem}/{ep:016x}/{sq:016x}/anchor");
    for k in [
        mk("o1", 4, 300, 1), // older epoch → delete
        mk("o1", 5, 100, 1), // our epoch, covered → delete
        anchor("o1", 5, 100), // our epoch anchor, covered → delete
        mk("o1", 5, 200, 1), // live tail → keep
        mk("o1", 6, 50, 1),  // NEWER epoch, sq < committed → KEEP (RV-R24)
        anchor("o1", 6, 50), // newer-epoch anchor → KEEP
    ] {
        vrt_objstore::block_on_store(store.put(&k, b"x")).unwrap();
    }
    rep.gc_segments(CK, 5, 200, || true);
    let left = seg_keys(&store, &format!("do/{CK}/wal/"));
    assert_eq!(
        left,
        vec![mk("o1", 5, 200, 1), mk("o1", 6, 50, 1), anchor("o1", 6, 50)],
        "newer-epoch objects must survive regardless of seq: {left:?}"
    );
}

/// RV-R24: a holder that lost the lease must not garbage-collect at
/// all — the check is consulted inside gc_segments and skips the
/// whole pass.
#[tokio::test(flavor = "current_thread")]
async fn wal_gc_skipped_when_lease_lost() {
    let dir = tmpdir("gc-lease-lost");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let mk =
        |stem, ep, sq, lsn| format!("do/{CK}/wal/{stem}/{ep:016x}/{sq:016x}/{lsn:016x}.seg");
    for k in [mk("o1", 4, 300, 1), mk("o1", 5, 100, 1), mk("o1", 5, 200, 1)] {
        vrt_objstore::block_on_store(store.put(&k, b"x")).unwrap();
    }
    let calls = Arc::new(AtomicU64::new(0));
    let c2 = calls.clone();
    rep.gc_segments(CK, 5, 200, move || {
        c2.fetch_add(1, Ordering::SeqCst);
        false
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1, "lease check consulted once");
    assert_eq!(
        seg_keys(&store, &format!("do/{CK}/wal/")).len(),
        3,
        "fenced holder must delete nothing"
    );
    // Held again → the same pass collects the covered + stale keys.
    rep.gc_segments(CK, 5, 200, || true);
    assert_eq!(
        seg_keys(&store, &format!("do/{CK}/wal/")),
        vec![mk("o1", 5, 200, 1)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn wal_salt_reset_with_unconsumed_frames_breaks() {
    let dir = tmpdir("salt");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(FailingStore); // uploads die → pending holds frames
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0; // release pin immediately each tick
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key); // parses frames; uploads fail → pending/queue non-empty
    bump(&tclk, 10); // idle release fires → pin dropped
    rep.tick(&sc.class_key);
    // force a new WAL generation while frames are un-uploaded:
    // pin was released → TRUNCATE checkpoint can run.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    put(&conn, "b", "2"); // new generation writes with a fresh salt
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "reset with un-consumed frames must break the stream"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_reset_after_pin_release_always_breaks() {
    // WAL1-r2 I2: a reset observed while the pin was released is
    // UNVERIFIABLE — even with every consumed frame uploaded, two
    // consecutive resets could strand writes. ALL resets now break.
    let dir = tmpdir("reanchor");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    bump(&tclk, 10);
    rep.tick(&sc.class_key); // idle release → pin dropped
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "reset while un-pinned must break even with nothing pending"
    );
    drop(rep);
    // Pre-reset work never became adoptable segments (no baseline
    // existed) — the takeover serves the empty image; lost frames
    // land in the next baseline, never as a partial chain.
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    let rep = replay_for_restore(&store, CK, &dir2, None, 5, 1 << 30, "team").unwrap();
    assert_eq!(rep.segs_applied, 0);
    assert!(!dir2.join("obj1.sqlite").exists());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_replay_on_baseline_recovers_post_snapshot_commits() {
    // Baseline exists (snapshot image copied in); the seq-42 chain
    // carries post-image commits — replay adopts only frames past
    // the anchor boundary (I3).
    let dir = tmpdir("baseline");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    // Snapshot image = db+wal copied together (backup semantics:
    // image sees all commits through the copy point; the live wal
    // generation is NOT checkpointed → the chain stays continuous).
    rep.on_snapshot_begin(&sc.class_key, 42);
    let baseline_dir = dir.join("baseline-img");
    img_copy(&class_dir, &baseline_dir, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    rep.on_snapshot_committed(&sc.class_key, 42);
    drop(rep);
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&baseline_dir, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rep = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    assert!(!rep.truncated && !rep.epoch_mismatch);
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "a").as_deref(),
        Some("1")
    );
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "b").as_deref(),
        Some("2")
    );
    conn.close().ok();
}

// ---------- WAL1-r2 ----------

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_three_hop_epoch_chain_never_mixes() {
    // P0-1 scenario: E writes a baseline + its chain; E+1 restores
    // it, writes MORE, but crashes before its OWN baseline; E+2 must
    // recover a consistent state — never image+partial-E+1 pages.
    let dir = tmpdir("threehop");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    // --- epoch 5 (E): baseline seq 42 committed ---
    let rep5 = DoWalReplicator::new(cfg(), store.clone(), dir.join("t5"), clock.clone());
    let sc = scope(class_dir.clone());
    rep5.start(&sc, 5, 0);
    rep5.tick(&sc.class_key);
    rep5.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1");
    rep5.on_snapshot_image_taken(&sc.class_key, 42);
    rep5.on_snapshot_committed(&sc.class_key, 42);
    // post-image commits still stream under {5}/{42}
    put(&conn, "a", "1b");
    rep5.tick(&sc.class_key);
    drop(rep5); // E crashes

    // --- epoch 6 (E+1): restores baseline+chain; writes; crashes
    // before its own baseline commits ---
    let dir_b = dir.join("cellb");
    std::fs::create_dir_all(&dir_b).unwrap();
    img_restore(&img, &dir_b, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rb = replay_for_restore(&store, CK, &dir_b, Some(&snapref), 5, 1 << 30, "team").unwrap();
    assert_eq!(
        get_val(&dir_b.join("obj1.sqlite"), "a").as_deref(),
        Some("1b")
    );
    assert!(rb.segs_applied > 0);
    // E+1 tails under seq_label 42 — emit MUST stay disabled until
    // its own baseline begins (I1): no adoptable segs under {6}/*.
    let conn2 = rusqlite::Connection::open(dir_b.join("obj1.sqlite")).unwrap();
    conn2.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    let rep6 = DoWalReplicator::new(cfg(), store.clone(), dir.join("t6"), clock.clone());
    let sc6 = vrt_host::do_placement::PlacementScope {
        class_dir: dir_b.clone(),
        ..scope(dir_b.clone())
    };
    rep6.start(&sc6, 6, 42);
    rep6.tick(&sc6.class_key);
    conn2
        .execute_batch("CREATE TABLE IF NOT EXISTS t(k TEXT PRIMARY KEY, v TEXT)")
        .ok();
    conn2
        .execute("INSERT OR REPLACE INTO t VALUES ('c','3')", [])
        .unwrap();
    rep6.tick(&sc6.class_key);
    assert!(
        rep6.take_needs_baseline(&sc6.class_key),
        "takeover must immediately want its own baseline"
    );
    let ep6 = seg_keys(&store, &format!("do/{CK}/wal/obj1/0000000000000006/"));
    assert!(
        ep6.is_empty(),
        "no adoptable segs before own baseline: {ep6:?}"
    );
    drop(rep6); // E+1 crashes before its forced snapshot

    // --- epoch 7 (E+2): baseline is still epoch-5 seq-42;
    // writer_epoch=6 → I1 veto → serve the image alone ---
    let dir_c = dir.join("cellc");
    std::fs::create_dir_all(&dir_c).unwrap();
    img_restore(&img, &dir_c, "obj1");
    let rc = replay_for_restore(&store, CK, &dir_c, Some(&snapref), 6, 1 << 30, "team").unwrap();
    assert!(rc.epoch_mismatch, "cross-epoch chain must not adopt");
    assert_eq!(rc.stems_applied, 0);
    assert_eq!(
        get_val(&dir_c.join("obj1.sqlite"), "a").as_deref(),
        Some("1"),
        "result = bare image — consistent historical state, never mixed"
    );
    assert_eq!(get_val(&dir_c.join("obj1.sqlite"), "c"), None);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_reset_between_commit_and_next_tick_breaks() {
    // P0-2d: break → re-baseline commits → reset BEFORE the next
    // tick must not be treated as a clean continuation.
    let dir = tmpdir("commitreset");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0; // release the pin every tick
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    bump(&tclk, 10);
    rep.tick(&sc.class_key); // idle → pin released
                             // reset with pending work → break (unconsumed/unverifiable)
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    assert!(rep.take_needs_baseline(&sc.class_key));
    // own baseline re-arms + commits
    baseline(&rep, &sc, 42);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "commit clears the re-baseline request"
    );
    // Re-arm must actually recover the stream (a break followed by
    // a committed baseline leaves the db emitting under the new
    // label — regression: rearm() dead-looped on needs_baseline).
    put(&conn, "x", "9");
    rep.tick(&sc.class_key);
    assert!(
        seg_keys(&store, &format!("do/{CK}/wal/"))
            .iter()
            .any(|k| k.contains("/000000000000002a/")),
        "post-baseline stream must emit under seq 42"
    );
    // ANOTHER reset lands before the next tick — the stream must
    // break again rather than silently continue a mixed chain.
    bump(&tclk, 10);
    rep.tick(&sc.class_key); // idle release again
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    bump(&tclk, 31_000); // past the re-request rate limit
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "reset between commit and next tick must break, not continue"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_final_snapshot_adopts_no_chain() {
    // P0-3: a departing Final flushes pre-image frames under the OLD
    // label; even if only a PARTIAL set lands, replay on the Final
    // image adopts nothing — the result is the Final image itself.
    let dir = tmpdir("finalnc");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 42);
    // writer "exits" — last writes flush under the committed label.
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    rep.flush(&sc);
    // Final image (copy); segments under {5}/{99} never exist.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let img = dir.join("img-final");
    std::fs::create_dir_all(&img).unwrap();
    img_restore(&class_dir, &img, "obj1");
    drop(rep);
    // Even deleting a middle seg of the old chain must not matter —
    // Final adopts NO chain at all.
    let keys = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/"),
    );
    let segs: Vec<&String> = keys.iter().filter(|k| k.ends_with(".seg")).collect();
    if segs.len() > 1 {
        vrt_objstore::block_on_store(store.delete(segs[1])).unwrap();
    }
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 99,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Final,
    };
    let rep2 = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    assert_eq!(rep2.stems_applied, 0, "Final adopts no chain");
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "a").as_deref(),
        Some("1")
    );
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "b").as_deref(),
        Some("2"),
        "result is exactly the Final image"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_periodic_pending_partial_upload_stays_consistent() {
    // P0-3b: pending frames at begin flush under the OLD label;
    // post-begin frames under the new. Delete a post-begin seg →
    // replay truncates at the last contiguous commit — consistent.
    let dir = tmpdir("pendpart");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    // pending non-empty at begin → flushes under label 0 first
    put(&conn, "a", "1x");
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    // post-image commits → seq 42 chain
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    rep.on_snapshot_committed(&sc.class_key, 42);
    drop(rep);
    // delete the last seq-42 SEGMENT → partial chain (anchor kept)
    let keys = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/"),
    );
    let segs: Vec<&String> = keys.iter().filter(|k| k.ends_with(".seg")).collect();
    vrt_objstore::block_on_store(store.delete(segs.last().unwrap())).unwrap();
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let _rep2 = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    // The remaining chain is a contiguous prefix — consistent:
    // image has a=1x; applied frames add b only; c is absent.
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "a").as_deref(),
        Some("1x")
    );
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "b").as_deref(),
        Some("2")
    );
    assert_eq!(get_val(&dir2.join("obj1.sqlite"), "c"), None);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_integrity_failure_preserves_baseline() {
    // P0-4: a seg that decodes (valid seg checksum) but produces a
    // corrupt db → the staged result is rejected, baseline kept.
    let dir = tmpdir("integ");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 42);
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let img = dir.join("img");
    std::fs::create_dir_all(&img).unwrap();
    img_restore(&class_dir, &img, "obj1");
    drop(rep);
    // Craft a VALID-encoded seg writing page 1 with garbage —
    // apply succeeds mechanically but integrity_check must fail.
    let bad = vrt_sql::seg::Segment {
        database_id: {
            let h = vrt_objstore::sigv4::sha256_hex(format!("{CK}/obj1").as_bytes());
            h[..32].to_string()
        },
        epoch: 5,
        lsn: 9,
        prev_lsn: 8,
        page_size: 4096,
        wal_begin: 99,
        frames: vec![vrt_sql::seg::SegFrame {
            page_no: 1,
            commit_db_size: 4096,
            data: vec![0x5au8; 4096],
        }],
    };
    let key = format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/0000000000000001.seg");
    vrt_objstore::block_on_store(store.put_if_none_match(&key, &vrt_sql::seg::encode(&bad)))
        .unwrap();
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 5,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rep2 = replay_for_restore(&store, CK, &dir2, Some(&snapref), 5, 1 << 30, "team").unwrap();
    assert_eq!(rep2.stems_applied, 0, "integrity failure must not install");
    assert_eq!(
        get_val(&dir2.join("obj1.sqlite"), "a").as_deref(),
        Some("1"),
        "baseline image preserved"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_snapshot_failure_rolls_back_label_no_lsn_conflict() {
    // P1-1: begin(42)+anchor emits under 42; upload "fails" →
    // on_snapshot_failed rolls tails back to the ANCHORED label 41
    // (resuming its lsn); the next baseline anchors fresh — no lsn
    // collision and no emission under the orphan 42 afterwards.
    let dir = tmpdir("rollback");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 41);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key); // emits under 41
    rep.on_snapshot_begin(&sc.class_key, 42);
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    put(&conn, "c", "3");
    rep.tick(&sc.class_key); // emits under orphan-42
    rep.on_snapshot_failed(&sc.class_key); // roll back to anchored 41
    put(&conn, "d", "4");
    rep.tick(&sc.class_key); // resumes under 41
    let l41: Vec<String> = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000005/0000000000000029/"),
    )
    .into_iter()
    .filter(|k| k.ends_with(".seg"))
    .collect();
    let l42 = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/"),
    );
    assert!(!l41.is_empty() && !l42.is_empty());
    let lsns: Vec<u64> = l41
        .iter()
        .map(|k| {
            u64::from_str_radix(k.rsplit('/').next().unwrap().trim_end_matches(".seg"), 16).unwrap()
        })
        .collect();
    assert_eq!(lsns, (1..=lsns.len() as u64).collect::<Vec<_>>());
    baseline(&rep, &sc, 43);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_upload_budget_bounds_tick_time() {
    // P1-2: a store that sleeps per call must not stall the tail —
    // the per-tick upload budget leaves work for the next tick.
    struct SlowStore;
    #[async_trait::async_trait]
    impl ObjectStore for SlowStore {
        async fn get(
            &self,
            _: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
            Ok(None)
        }
        async fn head(
            &self,
            _: &str,
        ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
            Ok(None)
        }
        async fn put_if_none_match(
            &self,
            _: &str,
            _: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            std::thread::sleep(std::time::Duration::from_millis(120));
            Ok("e".into())
        }
        async fn put_if_match(
            &self,
            _: &str,
            _: &str,
            _: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            Ok("e".into())
        }
        async fn put(&self, _: &str, _: &[u8]) -> Result<String, vrt_objstore::StoreError> {
            Ok("e".into())
        }
        async fn list(
            &self,
            _: &str,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            Ok(vrt_objstore::ListPage {
                entries: vec![],
                truncated: false,
            })
        }
        async fn delete(&self, _: &str) -> Result<(), vrt_objstore::StoreError> {
            Ok(())
        }
    }
    let dir = tmpdir("budget");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(SlowStore);
    let (_t, clock) = now_cell();
    let mut c = cfg();
    c.tick_upload_budget_ms = 50; // each put sleeps 120ms > budget
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    for i in 0..6 {
        put(&conn, &format!("k{i}"), "v");
        rep.tick(&sc.class_key);
    }
    let t0 = std::time::Instant::now();
    put(&conn, "z", "9");
    rep.tick(&sc.class_key); // queue has several segs; budget caps work
    let spent = t0.elapsed();
    assert!(
        spent < std::time::Duration::from_millis(400),
        "one tick must stay bounded by the upload budget: {spent:?}"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r2_pin_unavailable_retries_with_backoff() {
    // P1-4: a pin failure backs off (bounded) and RECOVERS once the
    // db is readable again — never sticky-fatal. A non-sqlite file
    // makes `WalPin::acquire` fail deterministically.
    let dir = tmpdir("pinretry");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    std::fs::write(class_dir.join("obj1.sqlite"), b"not a database").unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key); // acquire fails on the bogus file
    assert!(seg_keys(&store, &format!("do/{CK}/wal/")).is_empty());
    // Replace with a real db — after the backoff window the pin is
    // re-acquired and the stream works (via the epoch's baseline).
    std::fs::remove_file(class_dir.join("obj1.sqlite")).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    bump(&tclk, 65_000); // past max backoff
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    assert!(
        !seg_keys(&store, &format!("do/{CK}/wal/")).is_empty(),
        "pin must recover after backoff — segments should stream"
    );
    conn.close().ok();
}

// ---------------- WAL1-r3 ----------------

fn snapref(epoch: u64, seq: u64) -> vrt_host::do_placement_store::SnapRef {
    vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch,
        seq,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    }
}
fn segs_of(store: &Arc<dyn ObjectStore>, stem: &str, seq: u64) -> Vec<String> {
    seg_keys(
        store,
        &format!("do/{CK}/wal/{stem}/0000000000000005/{seq:016x}/"),
    )
    .into_iter()
    .filter(|k| k.ends_with(".seg"))
    .collect()
}
/// Release the pin via idle timeout, then force a WAL restart.
fn force_reset_via_idle(
    conn: &rusqlite::Connection,
    rep: &DoWalReplicator,
    sc: &vrt_host::do_placement::PlacementScope,
    tclk: &Arc<std::sync::atomic::AtomicU64>,
) {
    bump(tclk, 400_000);
    rep.tick(&sc.class_key); // idle release → pin dropped
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_sealed_label_never_extended_after_break_and_rollback() {
    // P0-A case 1: chain S=42 has segments; a reset-while-unpinned
    // breaks it (sealing 42); begin(43) re-anchors but the snapshot
    // FAILS → rollback to sealed 42 must NOT resume appending; a
    // replay on baseline-42 must serve exactly "image + pre-break
    // frames" — nothing written after the break may appear.
    let dir = tmpdir("sealedrb");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    // baseline 42: image covers a; anchor {B,E} lands.
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img42 = dir.join("img42");
    img_copy(&class_dir, &img42, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    // pre-break chain under 42: b
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    let before = segs_of(&store, "obj1", 42);
    assert!(!before.is_empty());
    // reset while unpinned → break → 42 sealed
    force_reset_via_idle(&conn, &rep, &sc, &tclk);
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    assert!(rep.take_needs_baseline(&sc.class_key), "reset must break");
    // begin(43): re-anchors, drops parsed c-frames (pre-image), labels 43
    rep.on_snapshot_begin(&sc.class_key, 43);
    let img43 = dir.join("img43");
    img_copy(&class_dir, &img43, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 43);
    rep.on_snapshot_failed(&sc.class_key); // rollback → label 42 (SEALED)
    put(&conn, "d", "4");
    rep.tick(&sc.class_key);
    // The sealed 42 chain must be byte-identical to pre-break.
    assert_eq!(
        segs_of(&store, "obj1", 42),
        before,
        "sealed chain appended to after rollback"
    );
    bump(&tclk, 31_000);
    assert!(rep.take_needs_baseline(&sc.class_key));
    // replay on baseline-42: image + b only — c/d must not appear.
    let dir2 = dir.join("takeover");
    img_restore(&img42, &dir2, "obj1");
    let _r =
        replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 42)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
    assert_eq!(
        get_val(&db, "b").as_deref(),
        Some("2"),
        "pre-break frame applies"
    );
    assert_eq!(get_val(&db, "c"), None, "post-break write must not appear");
    assert_eq!(get_val(&db, "d"), None);
    assert!(vrt_sql::wal::integrity_ok(&db).unwrap());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_sealed_label_survives_crash_before_commit() {
    // P0-A case 2: same setup; crash (drop rep) between begin(43)
    // and commit — takeover on baseline-42 must see only the
    // pre-break chain.
    let dir = tmpdir("sealedcrash");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img42 = dir.join("img42");
    img_copy(&class_dir, &img42, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    let before = segs_of(&store, "obj1", 42);
    force_reset_via_idle(&conn, &rep, &sc, &tclk);
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    assert!(rep.take_needs_baseline(&sc.class_key));
    rep.on_snapshot_begin(&sc.class_key, 43);
    rep.on_snapshot_image_taken(&sc.class_key, 43);
    drop(rep); // crash before commit — 43 is an orphan label
               // sealed 42 unchanged
    assert_eq!(segs_of(&store, "obj1", 42), before);
    let dir2 = dir.join("takeover");
    img_restore(&img42, &dir2, "obj1");
    replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 42)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
    assert_eq!(get_val(&db, "c"), None);
    assert!(vrt_sql::wal::integrity_ok(&db).unwrap());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_chain_short_of_image_end_serves_image() {
    // P0-B.1: writes between begin and image completion are IN the
    // image AND required by E; a chain truncated below E applies
    // nothing (under the old B-only rule it would apply a prefix).
    let dir = tmpdir("imgend");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let mut c = cfg();
    c.max_segment_bytes = 4_200; // one 4KB frame per seg → txn-per-seg
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    // TWO commits inside (B, M] → E covers both; each lands in its
    // own segment (tiny cap) so an interior hole leaves lc < E-1.
    put(&conn, "k1", "v1");
    put(&conn, "k1b", "vb");
    let img = dir.join("img");
    img_copy(&class_dir, &img, "obj1"); // image has a+k1+k1b
    rep.on_snapshot_image_taken(&sc.class_key, 42); // E ≥ k1b's frames
    put(&conn, "k2", "v2");
    rep.tick(&sc.class_key); // chain: [k1][k1b][k2] under 42
    rep.on_snapshot_committed(&sc.class_key, 42);
    drop(rep);
    let segs = segs_of(&store, "obj1", 42);
    assert!(segs.len() >= 3, "{segs:?}");
    // Delete the MIDDLE seg → lsn hole truncates the chain at the
    // k1 commit → last_commit + 1 < E → truly short of image end.
    vrt_objstore::block_on_store(store.delete(&segs[1])).unwrap();
    let dir2 = dir.join("takeover");
    img_restore(&img, &dir2, "obj1");
    let r =
        replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 42)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(r.stems_applied, 0, "chain < E must apply NOTHING");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
    assert_eq!(get_val(&db, "k1").as_deref(), Some("v1"), "image served");
    assert_eq!(get_val(&db, "k2"), None);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_page_rewritten_inside_be_window_never_reverted() {
    // P0-B.2: same page modified twice within (B, E]; the chain is
    // truncated between the two writes → replay must serve the
    // image (newer value), never revert to the older frame.
    let dir = tmpdir("brev");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "v0");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let mut c = cfg();
    c.max_segment_bytes = 4_500; // split per-commit segs
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    put(&conn, "a", "v1"); // (B, M]: first rewrite
    put(&conn, "a", "v2"); // (B, M]: second rewrite — image gets v2
    let img = dir.join("img");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42); // E covers both
    rep.tick(&sc.class_key);
    rep.on_snapshot_committed(&sc.class_key, 42);
    drop(rep);
    let segs = segs_of(&store, "obj1", 42);
    assert!(segs.len() >= 2, "need split segs, got {segs:?}");
    vrt_objstore::block_on_store(store.delete(segs.last().unwrap())).unwrap();
    let dir2 = dir.join("takeover");
    img_restore(&img, &dir2, "obj1");
    let r =
        replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 42)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(r.stems_applied, 0, "truncated inside (B,E] -> nothing");
    assert_eq!(
        get_val(&db, "a").as_deref(),
        Some("v2"),
        "image wins — never v1"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_chain_reaching_e_applies_normally() {
    // P0-B.3: a chain whose last commit ≥ E applies as before.
    let dir = tmpdir("imgcross");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    put(&conn, "k1", "v1");
    let img = dir.join("img");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    put(&conn, "k2", "v2");
    rep.tick(&sc.class_key); // chain reaches ≥ E
    rep.on_snapshot_committed(&sc.class_key, 42);
    drop(rep);
    let dir2 = dir.join("takeover");
    img_restore(&img, &dir2, "obj1");
    let r =
        replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 42)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(r.stems_applied, 1);
    assert_eq!(get_val(&db, "k1").as_deref(), Some("v1"));
    assert_eq!(get_val(&db, "k2").as_deref(), Some("v2"));
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_local_wins_wal_recreated_no_snapshot_serves_empty() {
    // P0-C.1: epoch E writes; conn closes → WAL deleted; E' reopens
    // and writes; NO snapshot ever → takeover on an empty dir must
    // match flag-off semantics: empty db, no partial pages, no Err.
    let dir = tmpdir("localwins");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    {
        let conn = mk_db(&class_dir, "obj1");
        put(&conn, "a", "1");
        conn.close().unwrap(); // last conn → checkpoint+wal delete
    }
    assert!(!class_dir.join("obj1.sqlite-wal").exists());
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    let conn2 = mk_db(&class_dir, "obj1"); // E' reopens — fresh WAL
    put(&conn2, "b", "2");
    rep.start(&sc, 9, 0);
    rep.tick(&sc.class_key);
    // No baseline → nothing adoptable is emitted.
    assert!(segs_of(&store, "obj1", 0).is_empty());
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "seq-0 start must request a baseline"
    );
    drop(rep);
    let dir2 = dir.join("takeover");
    std::fs::create_dir_all(&dir2).unwrap();
    let r = replay_for_restore(&store, CK, &dir2, None, 9, 1 << 30, "team").unwrap();
    assert_eq!(r.stems_applied, 0);
    assert!(
        !dir2.join("obj1.sqlite").exists(),
        "flag-off parity: empty dir"
    );
    conn2.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_db_created_after_baseline_requests_next() {
    // P0-C.3: a db appearing after the current baseline is absent
    // from that image and anchor-less → emits nothing adoptable and
    // pulls the next baseline forward.
    let dir = tmpdir("uncovered");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 42);
    assert!(!rep.take_needs_baseline(&sc.class_key));
    // obj2 arrives after baseline-42's anchor → uncovered.
    let conn2 = mk_db(&class_dir, "obj2");
    put(&conn2, "x", "9");
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "uncovered db must pull the next baseline forward"
    );
    assert!(
        segs_of(&store, "obj2", 42).is_empty(),
        "anchorless → nothing adoptable"
    );
    baseline(&rep, &sc, 43);
    put(&conn2, "y", "8");
    rep.tick(&sc.class_key);
    assert!(
        !segs_of(&store, "obj2", 43).is_empty(),
        "emits under its own baseline"
    );
    conn.close().ok();
    conn2.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_anchor_conflict_breaks_and_rebaselines() {
    // P1-1: a same-key anchor with DIFFERENT content is not an
    // idempotent hit — fail closed: broken + rebaseline, and the
    // label emits nothing.
    let dir = tmpdir("anchorconf");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    // Second image_taken for the same label after more writes → E
    // moved → put_if_none_match hits a DIFFERENT anchor → conflict.
    // (identical-content collisions are accepted — see the dup test)
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    bump(&_t, 31_000); // re-request is 30s rate-limited
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "anchor content conflict must fail closed"
    );
    // And nothing further emits under the broken label.
    let before = segs_of(&store, "obj1", 42);
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    assert_eq!(segs_of(&store, "obj1", 42), before);
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_anchor_idempotent_retry_accepted() {
    // P1-1: identical-content anchor collision is a dup — accepted.
    let dir = tmpdir("anchordup");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    // Same seq, no intervening writes → identical {B,E} → dup OK.
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    assert!(
        !segs_of(&store, "obj1", 42).is_empty(),
        "idempotent anchor retry must keep the label emitting"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r3_idle_release_wal_recreate_breaks() {
    // P1-3: idle release → WAL deleted+recreated → regrowth → the
    // tail MUST detect the new generation and break (never silent).
    let dir = tmpdir("idlerecreate");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    bump(&tclk, 10);
    rep.tick(&sc.class_key); // idle release → pin dropped
    conn.close().unwrap(); // last conn → checkpoint + wal delete
    let conn2 = mk_db(&class_dir, "obj1"); // fresh WAL, fresh salt
    put(&conn2, "b", "2");
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "delete+recreate while unpinned must break the stream"
    );
    conn2.close().ok();
}

// ---------- WAL1-r4 ----------

#[tokio::test(flavor = "current_thread")]
async fn wal_r4_restart_residual_tail_no_break() {
    // P0: RESTART checkpoint does NOT truncate the wal — after the
    // next write, old-generation frames sit behind the new valid
    // region. Frontier mismatch must be "end of valid region", not
    // a break; image_taken must still land its {B,E} anchor.
    let dir = tmpdir("r4resid");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    // ~10 committed frames → long gen-1 tail
    for i in 0..10 {
        put(&conn, &format!("a{i}"), "1");
    }
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (tclk, clock) = now_cell();
    let mut c = cfg();
    c.idle_stop_ms = 0;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 1); // anchor(1) pinned
                            // Idle release the pin, then RESTART (does not truncate) + one
                            // short write → new gen region << file, old-gen residue behind.
    bump(&tclk, 400_000);
    rep.tick(&sc.class_key); // idle release
    conn.execute_batch("PRAGMA wal_checkpoint(RESTART)")
        .unwrap();
    put(&conn, "b", "2"); // gen-2: ~2 frames; residue after
    rep.tick(&sc.class_key); // salt changed while unpinned → break
    assert!(rep.take_needs_baseline(&sc.class_key));
    // Rebaseline 42: rearm anchors gen-2, frontier hits residue —
    // must NOT break; image_taken must reach caught_up and anchor.
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img42 = dir.join("img42");
    img_copy(&class_dir, &img42, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    let anchor = format!("do/{CK}/wal/obj1/0000000000000005/000000000000002a/anchor");
    assert!(
        vrt_objstore::block_on_store(store.get(&anchor))
            .unwrap()
            .is_some(),
        "anchor must land despite residual tail"
    );
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "frontier at old-gen residue must never break"
    );
    assert!(
        !segs_of(&store, "obj1", 42).is_empty(),
        "chain emits under 42"
    );
    // Replay on baseline-42: image(a*,b) + c — integrity must pass.
    let dir2 = dir.join("takeover");
    img_restore(&img42, &dir2, "obj1");
    let _r =
        replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 42)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
    assert_eq!(get_val(&db, "c").as_deref(), Some("3"));
    assert!(vrt_sql::wal::integrity_ok(&db).unwrap());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r4_uncommitted_tail_not_consumed() {
    // P0: frames of an uncommitted txn sitting in the wal must not
    // enter pending/segments; the commit unlocks them next tick.
    let dir = tmpdir("r4uncom");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 1);
    let n0 = segs_of(&store, "obj1", 1).len();
    // Open txn, force a cache spill → uncommitted frames in the wal.
    conn.execute_batch("PRAGMA cache_size=1; BEGIN;").unwrap();
    for i in 0..32 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![format!("u{i}"), "x".repeat(2048)],
        )
        .unwrap();
    }
    rep.tick(&sc.class_key);
    assert_eq!(
        segs_of(&store, "obj1", 1).len(),
        n0,
        "uncommitted frames must not be emitted"
    );
    conn.execute_batch("COMMIT").unwrap();
    rep.tick(&sc.class_key);
    assert!(
        segs_of(&store, "obj1", 1).len() > n0,
        "committed frames must emit next tick"
    );
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r4_rollback_spill_overwritten_clean() {
    // P0: a rolled-back txn's spill frames are overwritten in-place
    // by the next txn. Since uncommitted frames never advance
    // read_pos, the tail re-reads the CURRENT bytes — replay must
    // contain only committed data.
    let dir = tmpdir("r4spill");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 1);
    let img = dir.join("img1");
    img_copy(&class_dir, &img, "obj1");
    // Rollback txn that spills frames (touch many pages).
    conn.execute_batch("PRAGMA cache_size=1; BEGIN;").unwrap();
    for i in 0..48 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![format!("z{i}"), "r".repeat(2048)],
        )
        .unwrap();
    }
    conn.execute_batch("ROLLBACK").unwrap();
    // A smaller committed txn overwrites the same frame slots.
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "same-generation overwrite at unconsumed position is clean"
    );
    // Replay: b applied; z* (rolled back) must NOT appear.
    let dir2 = dir.join("takeover");
    img_restore(&img, &dir2, "obj1");
    let _r =
        replay_for_restore(&store, CK, &dir2, Some(&snapref(5, 1)), 5, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
    assert_eq!(get_val(&db, "z0"), None, "rolled-back page must not apply");
    assert_eq!(get_val(&db, "z47"), None);
    assert!(vrt_sql::wal::integrity_ok(&db).unwrap());
    conn.close().ok();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_r4_commit_keeps_uncovered_db_flagged() {
    // P1: a db whose anchor put fails must STAY needs_baseline after
    // commit — class wants_baseline persists until every tracked db
    // anchors a seq.
    struct FailAnchorOnce {
        inner: MemoryStore,
        failed: AtomicU64,
    }
    #[async_trait::async_trait]
    impl ObjectStore for FailAnchorOnce {
        async fn get(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::StoredObject>, vrt_objstore::StoreError> {
            self.inner.get(k).await
        }
        async fn head(
            &self,
            k: &str,
        ) -> Result<Option<vrt_objstore::ObjectMeta>, vrt_objstore::StoreError> {
            self.inner.head(k).await
        }
        async fn put_if_none_match(
            &self,
            k: &str,
            b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            if k.ends_with("/anchor") && k.contains("obj2") {
                // outlast put_durable's retry budget (upload_attempts=2)
                if self.failed.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err(vrt_objstore::StoreError::unavailable(true, "anchor down"));
                }
            }
            self.inner.put_if_none_match(k, b).await
        }
        async fn put_if_match(
            &self,
            k: &str,
            e: &str,
            b: &[u8],
        ) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put_if_match(k, e, b).await
        }
        async fn put(&self, k: &str, b: &[u8]) -> Result<String, vrt_objstore::StoreError> {
            self.inner.put(k, b).await
        }
        async fn list(
            &self,
            p: &str,
        ) -> Result<vrt_objstore::ListPage, vrt_objstore::StoreError> {
            self.inner.list(p).await
        }
        async fn delete(&self, k: &str) -> Result<(), vrt_objstore::StoreError> {
            self.inner.delete(k).await
        }
    }
    let dir = tmpdir("r4p1");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let c1 = mk_db(&class_dir, "obj1");
    let c2 = mk_db(&class_dir, "obj2");
    put(&c1, "a", "1");
    put(&c2, "x", "9");
    let store: Arc<dyn ObjectStore> = Arc::new(FailAnchorOnce {
        inner: MemoryStore::new(),
        failed: AtomicU64::new(0),
    });
    let (tclk, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 5, 0);
    rep.tick(&sc.class_key);
    // baseline(1): obj1 anchors; obj2's anchor put fails once.
    rep.on_snapshot_begin(&sc.class_key, 1);
    rep.on_snapshot_image_taken(&sc.class_key, 1);
    rep.on_snapshot_committed(&sc.class_key, 1);
    bump(&tclk, 31_000);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "uncovered db must keep wants_baseline after commit"
    );
    // Next baseline succeeds → both dbs anchored → flag clears.
    baseline(&rep, &sc, 2);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "all dbs anchored → no further baseline demand"
    );
    put(&c1, "b", "2");
    put(&c2, "y", "8");
    rep.tick(&sc.class_key);
    assert!(!segs_of(&store, "obj1", 2).is_empty());
    assert!(
        !segs_of(&store, "obj2", 2).is_empty(),
        "obj2 emits after own anchor"
    );
    c1.close().ok();
    c2.close().ok();
}

/// MIG-17-r2 (H1): a serialized DO switch CARRIES the lease — the
/// new generation starts `wal.start` on the SAME writer epoch.
/// Carry invariants proven here:
///   1. `final_snapshot_departing` never runs for a carried class —
///      NO Final snapshot, no tail stop: the replicator instance and
///      its DbTail simply live through the switch (the supervisor's
///      `service_do_wal` active set now includes `carried_leases`,
///      so `retain` cannot drop the tail mid-carry).
///   2. `start(scope, same_epoch, seq)` RETAINS the tail — same
///      dir+epoch → the stream continues under the tail's own
///      labeled_seq (the passed label is ignored by construction).
///   3. Post-switch commits emit at the NEXT lsn under the SAME
///      `{epoch}/{seq}` namespace — the chain stays contiguous and
///      monotnoe, so a later restore replaying on the still-current
///      `Periodic(42)` baseline adopts BOTH generations' commits;
///      no `(epoch, seq, lsn)` key collides (a same-key/different-
///      body conflict would mark the tail broken — checked below).
#[tokio::test(flavor = "current_thread")]
async fn wal_mig17r2_same_epoch_carry_no_collision() {
    let dir = tmpdir("carry-eq-epoch");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    // Old generation: epoch 7, baseline seq 42, live committed tail.
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1"); // baseline image: a only
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    let chain42 = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
    );
    assert_eq!(
        chain42.iter().filter(|k| k.ends_with(".seg")).count(),
        1,
        "old gen emitted b under 42: {chain42:?}"
    );
    // Serialized switch: old gen exits, lease CARRIED — no Final
    // snapshot, no flush, no retain (carried set stays active). The
    // new gen's pipeline reads seq_label=42 from the record's
    // Periodic baseline and starts wal on the SAME epoch.
    rep.start(&sc, 7, 42);
    // Retained: 'c' continues the SAME chain at lsn 2 — same
    // (epoch=7, seq=42) namespace as the departing gen's 'b'.
    put(&conn, "c", "3");
    rep.tick(&sc.class_key);
    let chain42 = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
    );
    assert_eq!(
        chain42.iter().filter(|k| k.ends_with(".seg")).count(),
        2,
        "new gen continues the same chain — no new namespace, no overwrite: {chain42:?}"
    );
    // A same-key/different-body collision would mark_broken the tail.
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "same-epoch carry never hit a segment-content conflict"
    );
    // Restore on the still-current Periodic(42) baseline — a fresh
    // cell replays BOTH generations' commits (b pre-switch, c post).
    let dir2 = dir.join("newcell");
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rep2 = replay_for_restore(&store, CK, &dir2, Some(&snapref), 7, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
    assert_eq!(
        get_val(&db, "b").as_deref(),
        Some("2"),
        "old gen's post-baseline commit adopted"
    );
    assert_eq!(
        get_val(&db, "c").as_deref(),
        Some("3"),
        "new gen's post-switch commit adopted — same chain, no collision"
    );
    assert!(rep2.segs_applied >= 2);
}

/// MIG-17-r3 (H1 carry WAL window): the LAST commit the old writer
/// makes before the process exits — still unparsed in the `-wal` —
/// must survive a serialized-switch carry. `WalPin`'s read-mark
/// blocks `wal_checkpoint(TRUNCATE)` from reclaiming frames the tail
/// hasn't uploaded; the retained tail emits them post-switch on the
/// SAME (epoch, seq) chain; a fresh cell's replay adopts them.
/// Proven end-to-end: local -wal keeps the frame AND the S3 chain
/// carries it.
#[tokio::test]
async fn wal_mig17r3_carry_last_commit_before_exit_survives() {
    let dir = tmpdir("carry-last-frame");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key); // tail discovered under label 0 — WalPin held
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1"); // baseline image: 'a' only
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key); // seg#1: 'b'

    // The final commit before old-workerd exit — never ticked, sits
    // unparsed in the -wal. MIG-17c: close the writer conn FIRST —
    // the exiting process's last connection closing is what fires
    // its implicit checkpoint — then probe an explicit TRUNCATE from
    // a separate conn. The replicator's pin must block both from
    // eating the unparsed frame.
    put(&conn, "last", "final-commit");
    conn.close().ok();
    let ck = rusqlite::Connection::open(class_dir.join("obj1.sqlite")).unwrap();
    let (busy, _log, folded): (i64, i64, i64) = ck
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    drop(ck);
    assert_eq!(
        busy, 1,
        "WalPin read-mark must block checkpoint truncation over unparsed frames (folded={folded})"
    );
    let wal_len = std::fs::metadata(class_dir.join("obj1.sqlite-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(
        wal_len > 32,
        "the -wal still holds the pinned, unparsed frame after the writer conn closed (len={wal_len})"
    );

    // Serialized switch: lease carried — SAME tail (and pin) retained,
    // no Final snapshot, no flush.
    rep.start(&sc, 7, 42);
    rep.tick(&sc.class_key);
    let chain = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
    );
    assert_eq!(
        chain.iter().filter(|k| k.ends_with(".seg")).count(),
        2,
        "the pre-exit commit is emitted on the SAME chain after carry: {chain:?}"
    );
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "no content conflict"
    );

    // S3-chain proof: replay on the Periodic(42) image adopts a, b,
    // AND the pre-exit 'last' — the frame the checkpoint could not
    // reclaim made it into the uploaded chain.
    let dir2 = dir.join("newcell");
    img_restore(&img, &dir2, "obj1");
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let rep2 =
        replay_for_restore(&store, CK, &dir2, Some(&snapref), 7, 1 << 30, "team").unwrap();
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
    assert_eq!(get_val(&db, "b").as_deref(), Some("2"));
    assert_eq!(
        get_val(&db, "last").as_deref(),
        Some("final-commit"),
        "RPO=0 through carry: the pre-exit commit survived"
    );
    assert!(rep2.segs_applied >= 2);
}

/// MIG-17c-r2 (item 2): a corrupt INTERIOR segment (later chain
/// segs provably exist) is NOT a torn tail — the stem is rejected
/// (fail closed, retryable); a bad seg at the very END of the chain
/// remains a legitimate truncated tail.
#[test]
fn wal_replay_interior_corrupt_rejected_tail_torn() {
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let fixture = |tag: &str| {
        let dir = tmpdir(tag);
        let class_dir = dir.join("cls");
        std::fs::create_dir_all(&class_dir).unwrap();
        let conn = mk_db(&class_dir, "obj1");
        put(&conn, "a", "1");
        let mem = Arc::new(MemoryStore::new());
        let store: Arc<dyn ObjectStore> = mem.clone();
        let (_t, clock) = now_cell();
        let rep =
            DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
        let sc = scope(class_dir.clone());
        rep.start(&sc, 7, 0);
        rep.tick(&sc.class_key);
        rep.on_snapshot_begin(&sc.class_key, 42);
        let img = dir.join("img42");
        img_copy(&class_dir, &img, "obj1");
        rep.on_snapshot_image_taken(&sc.class_key, 42);
        rep.on_snapshot_committed(&sc.class_key, 42);
        for (k, v) in [("b", "2"), ("c", "3"), ("d", "4")] {
            put(&conn, k, v);
            rep.tick(&sc.class_key); // one seg per commit boundary
        }
        conn.close().ok();
        let segs = seg_keys(
            &store,
            &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
        );
        let segs: Vec<String> =
            segs.into_iter().filter(|k| k.ends_with(".seg")).collect();
        assert_eq!(segs.len(), 3, "fixture built a 3-seg chain: {segs:?}");
        (dir, store, mem, img, segs)
    };
    let fetch_body = |store: &Arc<dyn ObjectStore>, key: &str| {
        vrt_objstore::block_on_store(store.get(key)).unwrap().unwrap().body
    };

    // ---- arm A: interior corrupt seg (sha256 fails) → rejected ----
    let (dir, store, mem, img, segs) = fixture("17c-interior");
    let orig = fetch_body(&store, &segs[1]);
    let mut bad = orig.clone();
    let n = bad.len();
    bad[n / 2] ^= 0xFF; // payload flip → trailing sha256 mismatch
    mem.corrupt(&segs[1], bad);
    let dst = dir.join("cellA");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(&store, CK, &dst, Some(&snapref), 7, 1 << 30, "team")
        .unwrap();
    assert_eq!(r.stems_rejected, 1, "interior corrupt → rejected: {r:?}");
    assert_eq!(r.segs_corrupt, 1);
    assert!(!r.truncated, "interior corruption is NOT truncation");
    // Repair (restore original body) → retry adopts the whole chain.
    mem.corrupt(&segs[1], orig);
    let dst = dir.join("cellA2");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(&store, CK, &dst, Some(&snapref), 7, 1 << 30, "team")
        .unwrap();
    assert_eq!(r.stems_rejected, 0);
    assert_eq!(get_val(&dst.join("obj1.sqlite"), "d").as_deref(), Some("4"));

    // ---- arm B: missing interior seg (deleted) → rejected ----
    let (dir, store, _mem, img, segs) = fixture("17c-hole");
    vrt_objstore::block_on_store(store.delete(&segs[1])).unwrap();
    let dst = dir.join("cellB");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(&store, CK, &dst, Some(&snapref), 7, 1 << 30, "team")
        .unwrap();
    assert_eq!(r.stems_rejected, 1, "missing interior seg → rejected: {r:?}");
    assert_eq!(r.segs_corrupt, 1);

    // ---- arm C: torn TAIL → legitimate truncation ----
    let (dir, store, mem, img, segs) = fixture("17c-torn");
    let mut bad = fetch_body(&store, segs.last().unwrap());
    let n = bad.len();
    bad[n - 9] ^= 0xFF; // inside the sha256 trailer → decode fails
    mem.corrupt(segs.last().unwrap(), bad);
    let dst = dir.join("cellC");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(&store, CK, &dst, Some(&snapref), 7, 1 << 30, "team")
        .unwrap();
    assert_eq!(r.stems_rejected, 0, "torn tail is a legit skip: {r:?}");
    assert_eq!(r.segs_corrupt, 0);
    assert!(r.truncated);
    assert_eq!(get_val(&dst.join("obj1.sqlite"), "c").as_deref(), Some("3"));
    assert_eq!(get_val(&dst.join("obj1.sqlite"), "d"), None);
}

/// MIG-17c-r2 (item 1): a broken stream's baseline request jumps the
/// backoff — `mark_broken` flags `urgent_baseline`, the next poll
/// grants immediately, and the flag is consumed (a permanently
/// broken stream still throttles on the escalated interval after).
#[test]
fn wal_broken_stream_baseline_jumps_backoff() {
    let dir = tmpdir("17c-urgent");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let (t, clock) = now_cell();
    let mut c = cfg();
    c.max_pending_bytes = 64 << 10; // small cap → pin-span breach
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key); // tail discovered → wants baseline
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "first request fires immediately"
    );
    bump(&t, 1);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "second poll inside backoff is blocked"
    );
    // Flood -wal past the pin-span cap → mark_broken → urgent flag.
    let filler = "x".repeat(3072);
    for i in 0..40 {
        put(&conn, &format!("f{i}"), &filler);
    }
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "broken stream jumps the backoff — urgent grant"
    );
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "urgent is one-shot — back on the escalated interval"
    );
    conn.close().ok();
}

/// MIG-17c-r3 (item 1): `replay_max_bytes` breach is its own
/// classification — `cap_exceeded` + cap metric, NOT `segs_corrupt`,
/// and it rejects the stem wherever the cap lands (the capped seg is
/// committed data). Retry alone can't converge; a bigger cap can.
#[test]
fn wal_replay_cap_exceeded_rejected_not_corrupt() {
    let dir = tmpdir("17c-cap");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    for (k, v) in [("b", "2"), ("c", "3"), ("d", "4")] {
        put(&conn, k, v);
        rep.tick(&sc.class_key);
    }
    conn.close().ok();
    let segs = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
    );
    let segs: Vec<String> =
        segs.into_iter().filter(|k| k.ends_with(".seg")).collect();
    assert_eq!(segs.len(), 3);
    // Per-seg frame byte totals to place the cap mid-chain / on tail.
    let seg_bytes: Vec<u64> = segs
        .iter()
        .map(|k| {
            let b = vrt_objstore::block_on_store(store.get(k))
                .unwrap()
                .unwrap()
                .body;
            seg::decode(&b).unwrap().frames.iter()
                .map(|f| f.data.len() as u64)
                .sum()
        })
        .collect();
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    // Arm 1 — cap lands mid-chain (before seg2): rejected, corrupt 0.
    let dst = dir.join("cellMid");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(
        &store, CK, &dst, Some(&snapref), 7, seg_bytes[0], "team",
    )
    .unwrap();
    assert_eq!(r.stems_rejected, 1, "mid-chain cap → rejected: {r:?}");
    assert_eq!(r.cap_exceeded, 1);
    assert_eq!(r.segs_corrupt, 0, "cap is NOT corruption");
    assert!(!r.truncated);
    // Arm 2 — cap lands exactly on the tail seg: STILL rejected — the
    // capped seg is committed data, not a torn tail.
    let dst = dir.join("cellTail");
    img_restore(&img, &dst, "obj1");
    let cap = seg_bytes[0] + seg_bytes[1];
    let r = replay_for_restore(&store, CK, &dst, Some(&snapref), 7, cap, "team")
        .unwrap();
    assert_eq!(r.stems_rejected, 1, "tail-landing cap → rejected: {r:?}");
    assert_eq!(r.cap_exceeded, 1);
    assert_eq!(r.segs_corrupt, 0);
    // Cap metric renders (registry is global — name+tenant presence).
    let rendered = vrt_host::metrics::registry().render();
    assert!(
        rendered.contains("vrt_host_do_wal_replay_cap_exceeded_total"),
        "cap metric rendered: {}",
        rendered
            .lines()
            .find(|l| l.contains("cap_exceeded"))
            .unwrap_or("<none>")
    );
    // Arm 3 — bigger cap → retry converges, full chain adopts.
    let dst = dir.join("cellBig");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(
        &store, CK, &dst, Some(&snapref), 7, 1 << 30, "team",
    )
    .unwrap();
    assert_eq!(r.stems_rejected, 0);
    assert_eq!(r.cap_exceeded, 0);
    assert_eq!(get_val(&dst.join("obj1.sqlite"), "d").as_deref(), Some("4"));
}

/// MIG-17d (item 1): emit-side chain-byte accounting — a label's
/// uploaded frame bytes crossing `rebaseline_chain_bytes` requests
/// a NORMAL baseline (no seal — the chain keeps growing), metric +
/// info once per label; a committed baseline's label transition
/// resets the counter so it doesn't retrigger.
#[test]
fn wal_chain_rebaseline_triggers_and_resets() {
    let dir = tmpdir("17d-chain");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let (t, clock) = now_cell();
    let mut c = cfg();
    c.rebaseline_chain_bytes = 8 << 10; // 8 KiB trigger
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 42); // takeover baseline committed
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "clean commit — nothing wants a baseline yet"
    );
    // Write past the 8 KiB threshold — uploaded segs accrue.
    let blob = "x".repeat(3072);
    let pfx = format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/");
    for i in 0..4 {
        put(&conn, &format!("k{i}"), &blob);
        rep.tick(&sc.class_key); // ~3 KiB+ per commit-boundary seg
    }
    let segs = seg_keys(&store, &pfx);
    assert!(
        segs.iter().filter(|k| k.ends_with(".seg")).count() >= 3,
        "chain kept uploading (not sealed): {segs:?}"
    );
    // Flag → first chain grant (baseline_requested_ms still None).
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "crossed threshold → baseline requested"
    );
    let rendered = vrt_host::metrics::registry().render();
    assert!(
        rendered.contains("vrt_host_do_wal_chain_rebaseline_total"),
        "rebaseline metric rendered"
    );
    // Baseline seq 43 commits → label transition resets bytes+flag.
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 43);
    bump(&t, 31_000);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "post-baseline reset — no retrigger"
    );
    // Chain at the NEW label still uploads (not sealed by trigger).
    put(&conn, "post", "1");
    rep.tick(&sc.class_key);
    let pfx43 = format!("do/{CK}/wal/obj1/0000000000000007/000000000000002b/");
    assert!(
        seg_keys(&store, &pfx43).iter().any(|k| k.ends_with(".seg")),
        "post-baseline writes emit under the new label"
    );
    // Cross the threshold AGAIN on the new label — the second grant
    // fires at the BASE 30s interval since the first request
    // (bumped 31s above), and an immediate re-poll holds.
    for i in 0..4 {
        put(&conn, &format!("m{i}"), &blob);
        rep.tick(&sc.class_key);
    }
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "re-triggered — base 30s elapsed since the first grant"
    );
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "immediate re-poll inside the base interval — held"
    );
    conn.close().ok();
}

/// MIG-17d: below the trigger nothing is requested — a baseline
/// granted for other reasons doesn't spuriously re-fire.
#[test]
fn wal_chain_below_threshold_no_trigger() {
    let dir = tmpdir("17d-below");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (t, clock) = now_cell();
    let mut c = cfg();
    c.rebaseline_chain_bytes = 64 << 10;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    baseline(&rep, &sc, 42);
    put(&conn, "small", "1");
    rep.tick(&sc.class_key); // ~1 frame — far below 64 KiB
    bump(&t, 31_000);
    bump(&t, 31_000);
    assert!(
        !rep.take_needs_baseline(&sc.class_key),
        "below threshold must not request"
    );
    conn.close().ok();
}

/// MIG-17d-r2 (item 1): the trigger is the CLASS-WIDE sum — replay's
/// `total_bytes` is shared across stems, so N dbs each below the
/// threshold must still fire when their sum crosses it. Conservative
/// label handling: a db at any label counts.
#[test]
fn wal_chain_rebaseline_class_sum_trigger() {
    let dir = tmpdir("17d-classsum");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn1 = mk_db(&class_dir, "obj1");
    let conn2 = mk_db(&class_dir, "obj2");
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let (t, clock) = now_cell();
    let mut c = cfg();
    c.rebaseline_chain_bytes = 8 << 10; // 8 KiB class-wide trigger
    c.replay_max_bytes = 128 << 10;   // headroom >> triggered sum
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1");
    img_copy(&class_dir, &img, "obj2");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    assert!(!rep.take_needs_baseline(&sc.class_key));
    // ~5 KiB per db — EACH below 8 KiB, SUM (~10 KiB) over it.
    let blob = "x".repeat(2560);
    for i in 0..2 {
        put(&conn1, &format!("a{i}"), &blob);
        rep.tick(&sc.class_key);
        put(&conn2, &format!("b{i}"), &blob);
        rep.tick(&sc.class_key);
    }
    bump(&t, 31_000);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "class-wide sum over threshold must trigger"
    );
    // Real replay with a cap ABOVE the fired sum: no cap_exceeded —
    // the trigger's purpose (baselining before the sum reaches the
    // cap) holds.
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq: 42,
        files: 2,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let dst = dir.join("cell");
    img_restore(&img, &dst, "obj1");
    img_restore(&img, &dst, "obj2");
    let r = replay_for_restore(
        &store, CK, &dst, Some(&snapref), 7, 128 << 10, "team",
    )
    .unwrap();
    assert_eq!(r.cap_exceeded, 0);
    assert_eq!(r.stems_rejected, 0);
    assert_eq!(
        get_val(&dst.join("obj2.sqlite"), "b1").as_deref(),
        Some(blob.as_str())
    );
    conn1.close().ok();
    conn2.close().ok();
}

/// MIG-17d (item 2 e2e): small replay cap + small trigger + steady
/// writes and baseline grants — a restore at the latest baseline
/// never hits `cap_exceeded`, and its chain adopts fully.
#[test]
fn wal_chain_rebaseline_keeps_restore_under_cap() {
    let dir = tmpdir("17d-e2e");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let (t, clock) = now_cell();
    let mut c = cfg();
    c.rebaseline_chain_bytes = 16 << 10;
    c.replay_max_bytes = 48 << 10; // 3× trigger headroom
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    let blob = "x".repeat(4096);
    let mut seq = 42u64;
    let img = dir.join("img");
    for round in 0..5 {
        // ~20 KiB of writes per round — exceeds the trigger.
        for i in 0..5 {
            put(&conn, &format!("r{round}k{i}"), &blob);
            rep.tick(&sc.class_key);
        }
        bump(&t, 31_000);
        if rep.take_needs_baseline(&sc.class_key) {
            seq += 1;
            rep.on_snapshot_begin(&sc.class_key, seq);
            img_copy(&class_dir, &img, "obj1");
            rep.on_snapshot_image_taken(&sc.class_key, seq);
            rep.on_snapshot_committed(&sc.class_key, seq);
        }
    }
    conn.close().ok();
    // Restore at the latest baseline: the chain under that label is
    // bounded by the trigger (<cap) → full adoption, no cap hit.
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };
    let dst = dir.join("cell");
    img_restore(&img, &dst, "obj1");
    let r = replay_for_restore(
        &store, CK, &dst, Some(&snapref), 7, 48 << 10, "team",
    )
    .unwrap();
    assert_eq!(r.cap_exceeded, 0, "rebaseline kept the chain under cap");
    assert_eq!(r.stems_rejected, 0);
    assert_eq!(
        get_val(&dst.join("obj1.sqlite"), "r4k4").as_deref(),
        Some(blob.as_str())
    );
}

/// MIG-17c (item 3) / r2 (item 7): a same-holder reaffirm that
/// returns a DIFFERENT epoch leaves the retained tail bound to the
/// OLD epoch. `start` replaces it WITHOUT draining: old-epoch
/// segments are epoch-namespaced and can NEVER be adopted by a
/// restore at the new epoch, so uploading the remnant is forensics
/// at the cost of `flush_max_ms` × N on the actor. RPO rides on the
/// SNAPSHOT path — the dropped pin lets the next checkpoint fold
/// the unparsed frames into the db, and the fresh tail's first
/// baseline request is already immediate.
#[test]
fn wal_start_epoch_takeover_drains_outgoing_tail() {
    let dir = tmpdir("17c-takeover");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key); // tail under label 0
    rep.on_snapshot_begin(&sc.class_key, 42);
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key); // seg#1 under {7}/42/

    // Carry reaffirm adopts epoch 9 while the tail still belongs to
    // epoch 7 — with 'c' committed but never ticked (unparsed in
    // -wal). start(9) replaces the tail outright — 'c' is NEVER
    // uploaded to the old chain (unadoptable at epoch 9 anyway).
    put(&conn, "c", "3");
    rep.start(&sc, 9, 0);
    let chain7 = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
    );
    assert_eq!(
        chain7.iter().filter(|k| k.ends_with(".seg")).count(),
        1,
        "no drain — only the pre-takeover seg rides the old chain: {chain7:?}"
    );
    let chain9 = seg_keys(&store, &format!("do/{CK}/wal/obj1/0000000000000009/"));
    assert!(
        chain9.iter().all(|k| !k.ends_with(".seg")),
        "epoch-9 stream starts empty: {chain9:?}"
    );
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "fresh epoch-9 tail wants a baseline (first request is immediate)"
    );
    // The unparsed commit is NOT lost: a checkpoint folds it into
    // the db file, and the next baseline image carries db+-wal —
    // either path lands 'c' in the image.
    let ck = rusqlite::Connection::open(class_dir.join("obj1.sqlite")).unwrap();
    ck.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(ck);
    let img = dir.join("img-takeover");
    img_copy(&class_dir, &img, "obj1");
    assert_eq!(
        get_val(&img.join("obj1.sqlite"), "c").as_deref(),
        Some("3"),
        "unparsed pre-takeover commit survives via checkpoint/image — RPO=0"
    );
    conn.close().ok();
}

/// MIG-17c (item 2): a stem SELECTED for replay (anchor-proven,
/// frames above the image floor) whose apply/integrity step fails
/// counts as `stems_rejected` — not `segs_skipped` — so callers can
/// fail closed. Repair + retry adopts complete data.
#[test]
fn wal_replay_rejected_stem_counted_and_retry_adopts() {
    let dir = tmpdir("17c-reject");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let (_t, clock) = now_cell();
    let rep = DoWalReplicator::new(cfg(), store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key);
    rep.on_snapshot_begin(&sc.class_key, 42);
    let img = dir.join("img42");
    img_copy(&class_dir, &img, "obj1");
    rep.on_snapshot_image_taken(&sc.class_key, 42);
    rep.on_snapshot_committed(&sc.class_key, 42);
    put(&conn, "b", "2");
    rep.tick(&sc.class_key);
    conn.close().ok();
    let chain = seg_keys(
        &store,
        &format!("do/{CK}/wal/obj1/0000000000000007/000000000000002a/"),
    );
    let seg_key = chain
        .iter()
        .find(|k| k.ends_with(".seg"))
        .expect("one seg")
        .clone();
    let snapref = vrt_host::do_placement_store::SnapRef {
        manifest_key: "m".into(),
        epoch: 7,
        seq: 42,
        files: 1,
        bytes: 0,
        at_ms: 0,
        kind: vrt_host::do_placement_store::SnapKind::Periodic,
    };

    // Corrupt every frame's page image, re-encode (sha256 still
    // valid → decodes, applies garbage, integrity_check rejects).
    let orig = vrt_objstore::block_on_store(store.get(&seg_key))
        .unwrap()
        .unwrap()
        .body;
    let mut sgm = seg::decode(&orig).unwrap();
    for f in &mut sgm.frames {
        for b in f.data.iter_mut().take(64) {
            *b = 0xAA;
        }
    }
    mem.corrupt(&seg_key, seg::encode(&sgm));

    let dir2 = dir.join("newcell");
    img_restore(&img, &dir2, "obj1");
    let rep1 = replay_for_restore(&store, CK, &dir2, Some(&snapref), 7, 1 << 30, "team")
        .unwrap();
    assert_eq!(rep1.stems_rejected, 1, "corrupt selected stem rejected: {rep1:?}");
    assert_eq!(rep1.segs_skipped, 0, "rejection is NOT a policy skip");
    // Baseline preserved, not half-applied.
    let db = dir2.join("obj1.sqlite");
    assert_eq!(get_val(&db, "a").as_deref(), Some("1"));
    assert_eq!(get_val(&db, "b"), None);

    // Repair + retry → the stem adopts.
    mem.corrupt(&seg_key, orig);
    let rep2 = replay_for_restore(&store, CK, &dir2, Some(&snapref), 7, 1 << 30, "team")
        .unwrap();
    assert_eq!(rep2.stems_rejected, 0);
    assert!(rep2.segs_applied >= 1);
    assert_eq!(get_val(&db, "b").as_deref(), Some("2"), "complete data after repair");
}

/// MIG-17c (item 6): the pin's protected span
/// (`wal_len - mark_pos`, i.e. post-pin unparsed growth) is bounded
/// by `max_pending_bytes` — a flooded -wal whose scan can't keep up
/// must break the stream (pin released, baseline re-armed, metric),
/// never let the pin protect unbounded -wal growth.
#[test]
fn wal_pin_span_over_cap_breaks_stream() {
    let dir = tmpdir("17c-pinspan");
    let class_dir = dir.join("cls");
    std::fs::create_dir_all(&class_dir).unwrap();
    let conn = mk_db(&class_dir, "obj1");
    put(&conn, "a", "1");
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
    let (_t, clock) = now_cell();
    // 64 KiB cap: a handful of 4 KiB pages of unparsed growth.
    let mut c = cfg();
    c.max_pending_bytes = 64 << 10;
    let rep = DoWalReplicator::new(c, store.clone(), dir.join("tmp"), clock);
    let sc = scope(class_dir.clone());
    rep.start(&sc, 7, 0);
    rep.tick(&sc.class_key); // tail + pin established, mark at wal_len
    // Flood past the cap WITHOUT ticking — the pinned span grows
    // while the pin blocks any checkpoint from reclaiming it.
    let blob = "x".repeat(3000);
    for i in 0..40 {
        put(&conn, &format!("flood{i}"), &blob); // ~120 KiB+ of frames
    }
    let wal_len = std::fs::metadata(class_dir.join("obj1.sqlite-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(wal_len > (64 << 10), "flooded past the cap: {wal_len}");
    rep.tick(&sc.class_key);
    assert!(
        rep.take_needs_baseline(&sc.class_key),
        "pinned span over cap → stream broken, baseline re-armed"
    );
    // The committed rows are NOT lost — the next baseline image
    // re-derives them (fail closed, never a silent drop): an img_copy
    // right now contains every flood row.
    let img = dir.join("img-postbreak");
    img_copy(&class_dir, &img, "obj1");
    let db = img.join("obj1.sqlite");
    assert_eq!(get_val(&db, "flood0").as_deref(), Some(blob.as_str()));
    assert_eq!(get_val(&db, "flood39").as_deref(), Some(blob.as_str()));
    conn.close().ok();
}
