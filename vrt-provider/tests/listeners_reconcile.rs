//! T13/M5: `ListenerSet::reconcile` — tenant-set diff binds new
//! listeners, drains removed ones, isolates per-tenant bind failures.
//! Spawn is injected so tests drive real TcpListeners.

use std::collections::HashSet;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use vrt_provider::listeners::{partition_colliding, ListenerSet};

/// Spawn a trivial axum service on a bound TcpListener.
fn tcp_spawn(
    bound: Arc<Mutex<HashSet<String>>>,
) -> impl Fn(&str) -> io::Result<(watch::Sender<bool>, JoinHandle<io::Result<()>>)> {
    move |tenant: &str| {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let (tx, mut rx) = watch::channel(false);
        let tenant = tenant.to_string();
        let bound = bound.clone();
        bound.lock().unwrap().insert(tenant.clone());
        let task = tokio::spawn(async move {
            let listener = TcpListener::from_std(listener)?;
            let app = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
            axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = rx.changed().await;
                })
                .await
        });
        Ok((tx, task))
    }
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn reconcile_adds_and_removes_tenants() {
    let bound = Arc::new(Mutex::new(HashSet::new()));
    let mut set = ListenerSet::new();
    let cleaned = Arc::new(Mutex::new(Vec::<String>::new()));
    let cl = cleaned.clone();

    let (added, removed, failed) =
        set.reconcile(&ids(&["t1", "t2"]), tcp_spawn(bound.clone()), |t| {
            cl.lock().unwrap().push(t.to_string())
        });
    assert_eq!(added, ids(&["t1", "t2"]));
    assert!(removed.is_empty() && failed.is_empty());
    assert_eq!(set.tenants().len(), 2);

    // Diff: t2 removed, t3 added — t1 untouched.
    let cl = cleaned.clone();
    let (added, removed, failed) =
        set.reconcile(&ids(&["t1", "t3"]), tcp_spawn(bound.clone()), move |t| {
            cl.lock().unwrap().push(t.to_string())
        });
    assert_eq!(added, ids(&["t3"]));
    assert_eq!(removed, ids(&["t2"]));
    assert!(failed.is_empty());
    assert_eq!(set.tenants().len(), 2);
    assert_eq!(*cleaned.lock().unwrap(), vec!["t2".to_string()]);
    // The removed tenant's task drained and ended.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(set.draining_count(), 0);
}

#[tokio::test]
async fn reconcile_isolates_bind_failures() {
    let bound = Arc::new(Mutex::new(HashSet::new()));
    let mut set = ListenerSet::new();
    // Bind t-fail's port first so its spawn errors out.
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let blocked_port = blocker.local_addr().unwrap().port();
    let spawn = move |tenant: &str| {
        if tenant == "t-fail" {
            // Simulate a bind failure (port held by us).
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("port {blocked_port} held"),
            ));
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let (tx, mut rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let listener = TcpListener::from_std(listener)?;
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/", axum::routing::get(|| async { "ok" }))
                    .into_make_service(),
            )
            .with_graceful_shutdown(async move {
                let _ = rx.changed().await;
            })
            .await
        });
        bound.lock().unwrap().insert(tenant.to_string());
        Ok((tx, task))
    };
    let (added, _removed, failed) = set.reconcile(&ids(&["t-ok", "t-fail"]), spawn, |_| {});
    assert_eq!(added, ids(&["t-ok"]));
    assert_eq!(failed, ids(&["t-fail"]));
    // Healthy tenant unaffected by its sibling's bind failure.
    assert_eq!(set.tenants(), ids(&["t-ok"]));
    drop(blocker);
    set.shutdown_all().await;
}

// ------------------------------------------------------------------
// H5-r2: uid-collision — colliding tenants never reach bind, both
// orders, released on removal.
// ------------------------------------------------------------------

/// Brute-force a real colliding pair inside a production-sized span.
fn colliding_pair(base: u32, span: u32) -> (String, String) {
    let mut seen = std::collections::HashMap::new();
    for i in 0..(span as u64 * 4) {
        let t = format!("tenant-{i}");
        let uid = vrt_securefs::tenant_uid(base, span, &t);
        if let Some(other) = seen.insert(uid, t.clone()) {
            return (other, t);
        }
    }
    panic!("no collision found in span {span}");
}

#[test]
fn partition_colliding_span_one_refuses_all() {
    // span=1: every tenant derives the same uid — ALL refused, order-
    // independent (forward AND reversed verdicts identical).
    let wanted = ids(&["ta", "tb", "tc"]);
    let (serveable, collided) = partition_colliding(Some((60_000, 1)), &wanted);
    assert!(serveable.is_empty());
    assert_eq!(collided.len(), 3);
    let mut rev = wanted.clone();
    rev.reverse();
    let (serveable2, collided2) = partition_colliding(Some((60_000, 1)), &rev);
    assert_eq!(serveable, serveable2);
    assert_eq!(collided, collided2);
}

#[test]
fn partition_colliding_no_isolation_serves_all() {
    let wanted = ids(&["ta", "tb"]);
    let (serveable, collided) = partition_colliding(None, &wanted);
    assert_eq!(serveable, wanted);
    assert!(collided.is_empty());
}

#[test]
fn partition_colliding_real_pair_only() {
    let (a, b) = colliding_pair(60_000, 2_000);
    let wanted = ids(&["solo", &a, &b]);
    let (serveable, collided) = partition_colliding(Some((60_000, 2_000)), &wanted);
    assert_eq!(serveable, ids(&["solo"]));
    assert_eq!(collided, [a.clone(), b.clone()].into_iter().collect());
    // Removing one collider frees the survivor.
    let wanted = ids(&["solo", &a]);
    let (serveable, collided) = partition_colliding(Some((60_000, 2_000)), &wanted);
    assert_eq!(serveable.len(), 2);
    assert!(collided.is_empty());
}

#[tokio::test]
async fn reconcile_never_binds_colliding_tenants() {
    // Driving the REAL reconcile path with the filtered wanted set:
    // colliding tenants get no spawn call at all, a live tenant that
    // becomes colliding is drained + cleaned, and the survivor rebinds
    // once the collision resolves.
    let _bound = Arc::new(Mutex::new(HashSet::<String>::new()));
    let spawned = Arc::new(Mutex::new(Vec::<String>::new()));
    let cleaned = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut set = ListenerSet::new();
    let iso = Some((60_000, 1u32)); // span=1: any pair collides

    let spawn = {
        let spawned = spawned.clone();
        move |tenant: &str| {
            spawned.lock().unwrap().push(tenant.to_string());
            tcp_spawn_noop()
        }
        // wrap: record then delegate to a trivial listener
    };

    // Desired {ta, tb}: both collide → neither is ever spawned.
    let wanted = ids(&["ta", "tb"]);
    let (serveable, _) = partition_colliding(iso, &wanted);
    let cl = cleaned.clone();
    let (added, _removed, _failed) = set.reconcile(&serveable, &spawn, |t| {
        cl.lock().unwrap().push(t.to_string())
    });
    assert!(added.is_empty());
    assert!(spawned.lock().unwrap().is_empty());
    assert!(set.tenants().is_empty());

    // Desired {ta, tb, tc}: still all colliding → nothing bound.
    let wanted = ids(&["ta", "tb", "tc"]);
    let (serveable, _) = partition_colliding(iso, &wanted);
    let cl = cleaned.clone();
    let (added, _r, _f) = set.reconcile(&serveable, &spawn, |t| {
        cl.lock().unwrap().push(t.to_string())
    });
    assert!(added.is_empty());
    assert!(spawned.lock().unwrap().is_empty());

    // tb,tc leave: ta alone no longer collides → bound on next tick.
    let wanted = ids(&["ta"]);
    let (serveable, _) = partition_colliding(iso, &wanted);
    let cl = cleaned.clone();
    let (added, _r, _f) = set.reconcile(&serveable, &spawn, |t| {
        cl.lock().unwrap().push(t.to_string())
    });
    assert_eq!(added, ids(&["ta"]));
    assert_eq!(set.tenants(), ids(&["ta"]));

    // tc arrives → collides with ta: BOTH withheld — the live ta is
    // drained + its socket cleaned, nothing new binds.
    let wanted = ids(&["ta", "tc"]);
    let (serveable, _) = partition_colliding(iso, &wanted);
    let cl = cleaned.clone();
    let (added, removed, _f) = set.reconcile(&serveable, &spawn, |t| {
        cl.lock().unwrap().push(t.to_string())
    });
    assert!(added.is_empty());
    assert_eq!(removed, ids(&["ta"]));
    assert_eq!(cleaned.lock().unwrap().clone(), ids(&["ta"]));
    assert!(set.tenants().is_empty());

    set.shutdown_all().await;
}

/// Spawn a trivial listener that stays up until shutdown fires —
/// records nothing (the `spawned` log is written by the caller's
/// wrapper), returns the same shape `reconcile` expects.
fn tcp_spawn_noop() -> io::Result<(watch::Sender<bool>, JoinHandle<io::Result<()>>)> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let (tx, mut rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let listener = TcpListener::from_std(listener)?;
        axum::serve(listener, axum::Router::new())
            .with_graceful_shutdown(async move {
                let _ = rx.changed().await;
            })
            .await
    });
    Ok((tx, task))
}
