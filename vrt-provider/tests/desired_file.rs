//! `FileDesiredStore` and the periodic reload task: verification gates,
//! monotonicity, keep-last-good on reload failure, and shutdown exit.

mod support;

use chrono::Duration;
use support::*;
use vrt_provider::desired::DesiredStore;
use vrt_provider::desired_file::{spawn_reload_task, FileDesiredStore};

fn write_snapshot(
    path: &std::path::Path,
    fixture: &Fixture,
    payload: &snapshot_protocol::compute_desired::ComputeDesired,
    revision: u64,
) {
    std::fs::write(path, sign_desired(fixture, payload, revision)).unwrap();
}

fn file_store(path: &std::path::Path, fixture: &Fixture) -> FileDesiredStore {
    FileDesiredStore::new(path, CELL, fixture.trusted.clone(), 1)
}

#[test]
fn load_verify_monotonic_and_expiry() {
    let dir = TestDir::new();
    let path = dir.path().join("desired.bin");
    let fixture = fixture();
    let payload = desired(
        5,
        vec![tenant(
            TENANT,
            vec![worker(WORKER, VERSION, DIGEST, vec![])],
        )],
    );
    write_snapshot(&path, &fixture, &payload, 5);

    let store = file_store(&path, &fixture);
    assert_eq!(store.reload(test_now()).unwrap(), 5);
    assert!(store.tenant_view(TENANT, test_now()).is_ok());
    // Identical bytes re-verify idempotently (periodic reload relies on it).
    assert_eq!(store.reload(test_now()).unwrap(), 5);
    // Same revision but different signed content -> revision conflict.
    let changed = desired(
        5,
        vec![tenant(
            TENANT,
            vec![worker(WORKER, VERSION, DIGEST2, vec![])],
        )],
    );
    write_snapshot(&path, &fixture, &changed, 5);
    assert!(store.reload(test_now()).is_err());
    // A lower revision is rejected; the good snapshot still serves.
    write_snapshot(&path, &fixture, &payload, 4);
    assert!(store.reload(test_now()).is_err());
    assert!(store.tenant_view(TENANT, test_now()).is_ok());
}

#[test]
fn reload_keeps_previous_on_corrupt_file() {
    let dir = TestDir::new();
    let path = dir.path().join("desired.bin");
    let fixture = fixture();
    let payload = desired(
        5,
        vec![tenant(
            TENANT,
            vec![worker(WORKER, VERSION, DIGEST, vec![])],
        )],
    );
    write_snapshot(&path, &fixture, &payload, 5);
    let store = file_store(&path, &fixture);
    store.reload(test_now()).unwrap();

    std::fs::write(&path, b"not-a-snapshot").unwrap();
    assert!(store.reload(test_now()).is_err());
    // Last good snapshot keeps serving until its own expiry.
    assert!(store.tenant_view(TENANT, test_now()).is_ok());
    // ... but it fails closed once expired.
    assert!(store
        .tenant_view(TENANT, test_now() + Duration::minutes(20))
        .is_err());
}

#[tokio::test]
async fn reload_task_reloads_then_exits_on_shutdown() {
    let dir = TestDir::new();
    let path = dir.path().join("desired.bin");
    let fixture = fixture();
    let payload = desired(
        5,
        vec![tenant(
            TENANT,
            vec![worker(WORKER, VERSION, DIGEST, vec![])],
        )],
    );
    write_snapshot(&path, &fixture, &payload, 5);
    let store = std::sync::Arc::new(file_store(&path, &fixture));
    store.reload(test_now()).unwrap();

    let (tx, rx) = tokio::sync::watch::channel(false);
    let task = spawn_reload_task(store.clone(), std::time::Duration::from_millis(110), rx);

    // Bump the on-disk snapshot; the task must pick it up on a tick.
    let newer = desired(
        6,
        vec![tenant(
            TENANT,
            vec![worker(WORKER, VERSION, DIGEST, vec![])],
        )],
    );
    // revision guard: the new envelope must carry a higher revision.
    write_snapshot(&path, &fixture, &newer, 6);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if store.revision() == Some(6) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("reload task picked up the newer snapshot");

    // The task must terminate promptly once shutdown is signalled —
    // regression test for the drain-blocking reload loop.
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .expect("reload task exited within the bound")
        .expect("reload task did not panic");
}

/// An empty (zero-tenant) verified snapshot is a legal standby state:
/// it loads, lists no tenants, hot-reloads empty -> non-empty -> empty,
/// and a tenant lookup is `Missing` (the "no route" path), never a panic.
#[test]
fn empty_snapshot_is_standby_and_hot_reloads_both_ways() {
    let dir = TestDir::new();
    let path = dir.path().join("desired.bin");
    let fixture = fixture();
    write_snapshot(&path, &fixture, &desired(1, vec![]), 1);
    let store = file_store(&path, &fixture);
    assert_eq!(store.reload(test_now()).unwrap(), 1);
    assert_eq!(store.tenant_ids(), Some(vec![]));
    assert!(store.tenant_view(TENANT, test_now()).is_err());

    // empty -> non-empty
    let one = desired(
        2,
        vec![tenant(TENANT, vec![worker(WORKER, VERSION, DIGEST, vec![])])],
    );
    write_snapshot(&path, &fixture, &one, 2);
    assert_eq!(store.reload(test_now()).unwrap(), 2);
    assert_eq!(store.tenant_ids(), Some(vec![TENANT.to_string()]));
    assert!(store.tenant_view(TENANT, test_now()).is_ok());

    // non-empty -> empty
    write_snapshot(&path, &fixture, &desired(3, vec![]), 3);
    assert_eq!(store.reload(test_now()).unwrap(), 3);
    assert_eq!(store.tenant_ids(), Some(vec![]));
}

/// Empty is NOT a back door: a tampered/untrusted empty snapshot still
/// fails verification (fail closed), distinct from a legit empty one.
#[test]
fn tampered_empty_snapshot_still_fails_closed() {
    let dir = TestDir::new();
    let path = dir.path().join("desired.bin");
    let fixture = fixture();
    let mut raw = sign_desired(&fixture, &desired(1, vec![]), 1);
    let last = raw.len() - 1;
    raw[last] ^= 0x01;
    std::fs::write(&path, raw).unwrap();
    let store = file_store(&path, &fixture);
    assert!(store.reload(test_now()).is_err());
    assert_eq!(store.tenant_ids(), None);
}
