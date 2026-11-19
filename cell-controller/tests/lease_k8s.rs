//! B9: `KubernetesLeaseStore` against a scripted HTTP mock of the
//! Kubernetes API — acquisition, create-on-404, renew, 409 CAS loss,
//! 5xx/timeout fail-closed, expiry takeover, held(), and a two-store
//! contention race where exactly one leads.

use cell_controller::lease::{LeaseOutcome, LeaseStore};
use cell_controller::lease_k8s::{K8sLeaseConfig, KubernetesLeaseStore};
use chrono::{DateTime, Duration, Utc};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

/// One scripted exchange: expected (method, path-prefix) → (status, body).
type Step = (&'static str, &'static str, u16, String);

/// Serve `steps` in order on an ephemeral loopback port; each request
/// asserts its method+path prefix. Returns the base URL + a joinable
/// handle so the test fails if a step was never consumed.
fn mock_api(steps: Vec<Step>) -> (String, Arc<Mutex<Vec<String>>>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(Mutex::new(Vec::new()));
    let hits2 = hits.clone();
    let handle = std::thread::spawn(move || {
        for (method, prefix, status, body) in steps {
            let Ok((mut stream, _)) = listener.accept() else {
                break;
            };
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            hits2.lock().unwrap().push(line.trim().to_string());
            assert!(
                line.starts_with(&format!("{method} {prefix}")),
                "expected {method} {prefix}, got: {line}"
            );
            // Drain headers + body.
            let mut len = 0usize;
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h.trim().is_empty() {
                    break;
                }
                if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
            }
            if len > 0 {
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).unwrap();
            }
            let status_line = match status {
                200 => "200 OK",
                201 => "201 Created",
                404 => "404 Not Found",
                409 => "409 Conflict",
                500 => "500 Internal Server Error",
                _ => "500 Internal Server Error",
            };
            let resp = format!(
                "HTTP/1.1 {status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).unwrap();
            stream.flush().unwrap();
        }
    });
    (format!("http://127.0.0.1:{port}"), hits, handle)
}

fn store(base: &str) -> KubernetesLeaseStore {
    let dir = std::env::temp_dir().join(format!("vrt-k8s-lease-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join(format!("token-{}", rand_suffix()));
    std::fs::write(&token, "test-token").unwrap();
    KubernetesLeaseStore::new(&K8sLeaseConfig {
        api_url: base.into(),
        namespace: "vrt".into(),
        lease_name: "cron".into(),
        token_path: token.to_string_lossy().into(),
        ca_path: String::new(),
        timeout: std::time::Duration::from_secs(3),
        allow_insecure: true,
    })
    .unwrap()
}

// T29/R1: was nanos-only — same-nanosecond calls collided.
fn rand_suffix() -> u64 {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    n ^ SEQ
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .rotate_left(32)
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-08T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + Duration::seconds(secs)
}

fn lease_json(rv: &str, holder: &str, renew: &str, secs: i64, transitions: u64) -> String {
    format!(
        r#"{{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{{"name":"cron","namespace":"vrt","resourceVersion":"{rv}"}},"spec":{{"holderIdentity":"{holder}","leaseDurationSeconds":{secs},"renewTime":"{renew}","leaseTransitions":{transitions}}}}}"#
    )
}

const PATH: &str = "/apis/coordination.k8s.io/v1/namespaces/vrt/leases/cron";

#[tokio::test]
async fn acquire_creates_on_404_then_renews() {
    let (base, _hits, h) = mock_api(vec![
        ("GET", PATH, 404, "{}".into()),
        (
            "POST",
            "/apis/coordination.k8s.io/v1/namespaces/vrt/leases",
            201,
            "{}".into(),
        ),
        (
            "GET",
            PATH,
            200,
            lease_json("2", "pod-a", "2026-11-08T00:00:00Z", 15, 0),
        ),
        ("PUT", PATH, 200, "{}".into()),
    ]);
    let s = store(&base);
    let LeaseOutcome::Held(c1) = s
        .claim("pod-a", Duration::seconds(15), at(0))
        .await
        .unwrap()
    else {
        panic!("first claim must hold")
    };
    assert_eq!(c1.holder, "pod-a");
    assert!(s.held(&c1, at(5)));
    // Same holder renews via PUT (CAS rv=2), epoch pinned ≥1.
    let LeaseOutcome::Held(c2) = s
        .claim("pod-a", Duration::seconds(15), at(5))
        .await
        .unwrap()
    else {
        panic!("renew must hold")
    };
    assert!(c2.epoch >= 1);
    // RV-R56: first create is epoch 1 (not the 0 baseline) and a
    // same-holder renew does not change it (verified 2026-11-15).
    assert_eq!(c1.epoch, 1);
    assert_eq!(c2.epoch, c1.epoch);
    h.join().unwrap();
}

#[tokio::test]
async fn foreign_unexpired_lease_reports_lost() {
    let (base, _h, handle) = mock_api(vec![(
        "GET",
        PATH,
        200,
        lease_json("7", "pod-b", "2026-11-08T00:00:05Z", 30, 3),
    )]);
    let s = store(&base);
    // renew=+5s duration=30 → expiry +35s; claim at +10s → still held.
    assert_eq!(
        s.claim("pod-a", Duration::seconds(15), at(10))
            .await
            .unwrap(),
        LeaseOutcome::Lost
    );
    handle.join().unwrap();
}

#[tokio::test]
async fn expired_foreign_lease_is_taken_with_bumped_epoch() {
    let (base, _h, handle) = mock_api(vec![
        (
            "GET",
            PATH,
            200,
            lease_json("9", "pod-b", "2026-11-08T00:00:00Z", 15, 2),
        ),
        ("PUT", PATH, 200, "{}".into()),
    ]);
    let s = store(&base);
    // expiry at +15s; claim at +20 → takeover, transitions 2→3.
    let LeaseOutcome::Held(c) = s
        .claim("pod-a", Duration::seconds(15), at(20))
        .await
        .unwrap()
    else {
        panic!("expired lease must be takeable")
    };
    assert_eq!(c.epoch, 3);
    assert!(s.held(&c, at(25)));
    handle.join().unwrap();
}

#[tokio::test]
async fn cas_conflict_reports_lost() {
    let (base, _h, handle) = mock_api(vec![
        (
            "GET",
            PATH,
            200,
            lease_json("9", "pod-b", "2026-11-08T00:00:00Z", 15, 2),
        ),
        (
            "PUT",
            PATH,
            409,
            r#"{"kind":"Status","reason":"Conflict"}"#.into(),
        ),
    ]);
    let s = store(&base);
    assert_eq!(
        s.claim("pod-a", Duration::seconds(15), at(20))
            .await
            .unwrap(),
        LeaseOutcome::Lost
    );
    handle.join().unwrap();
}

#[tokio::test]
async fn api_5xx_is_unavailable_fail_closed() {
    let (base, _h, handle) = mock_api(vec![("GET", PATH, 500, "{}".into())]);
    let s = store(&base);
    assert!(matches!(
        s.claim("pod-a", Duration::seconds(15), at(0)).await,
        Err(cell_controller::lease::LeaseError::Unavailable)
    ));
    handle.join().unwrap();
}

#[tokio::test]
async fn unreachable_api_is_unavailable() {
    // Nothing listens on the port → connect error → Unavailable.
    let dir = std::env::temp_dir().join("vrt-k8s-lease-dead");
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join("token");
    std::fs::write(&token, "t").unwrap();
    let s = KubernetesLeaseStore::new(&K8sLeaseConfig {
        api_url: "http://127.0.0.1:1".into(), // port 1 — refused
        namespace: "vrt".into(),
        lease_name: "cron".into(),
        token_path: token.to_string_lossy().into(),
        ca_path: String::new(),
        timeout: std::time::Duration::from_millis(500),
        allow_insecure: true,
    })
    .unwrap();
    assert!(matches!(
        s.claim("pod-a", Duration::seconds(15), at(0)).await,
        Err(cell_controller::lease::LeaseError::Unavailable)
    ));
}

#[tokio::test]
async fn api_timeout_is_unavailable_fail_closed() {
    // Server accepts the connection but never responds → the bounded
    // request timeout must map to Unavailable (never an infinite hang).
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        if let Ok((_stream, _)) = listener.accept() {
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    });
    let dir = std::env::temp_dir().join("vrt-k8s-lease-hang");
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join("token");
    std::fs::write(&token, "t").unwrap();
    let s = KubernetesLeaseStore::new(&K8sLeaseConfig {
        api_url: format!("http://127.0.0.1:{port}"),
        namespace: "vrt".into(),
        lease_name: "cron".into(),
        token_path: token.to_string_lossy().into(),
        ca_path: String::new(),
        timeout: std::time::Duration::from_millis(300),
        allow_insecure: true,
    })
    .unwrap();
    assert!(matches!(
        s.claim("pod-a", Duration::seconds(15), at(0)).await,
        Err(cell_controller::lease::LeaseError::Unavailable)
    ));
    drop(handle); // the sleeping thread exits with the test process
}

#[tokio::test]
async fn held_is_false_for_stale_or_foreign_claim() {
    let (base, _h, handle) = mock_api(vec![
        ("GET", PATH, 404, "{}".into()),
        (
            "POST",
            "/apis/coordination.k8s.io/v1/namespaces/vrt/leases",
            201,
            "{}".into(),
        ),
    ]);
    let s = store(&base);
    let LeaseOutcome::Held(c) = s
        .claim("pod-a", Duration::seconds(10), at(0))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(s.held(&c, at(5)));
    assert!(!s.held(&c, at(11)), "expired local view must not be held");
    let foreign = cell_controller::lease::LeaseClaim {
        holder: "pod-b".into(),
        until: at(30),
        epoch: 99,
    };
    assert!(!s.held(&foreign, at(5)));
    handle.join().unwrap();
}

#[tokio::test]
async fn two_contenders_exactly_one_leads() {
    // Both stores claim at the same instant: first GET 404 + create
    // wins; the loser's create hits 409 → Lost.
    let (base, _h, handle) = mock_api(vec![
        ("GET", PATH, 404, "{}".into()),
        (
            "POST",
            "/apis/coordination.k8s.io/v1/namespaces/vrt/leases",
            201,
            "{}".into(),
        ),
        ("GET", PATH, 404, "{}".into()),
        (
            "POST",
            "/apis/coordination.k8s.io/v1/namespaces/vrt/leases",
            409,
            "{}".into(),
        ),
    ]);
    let a = store(&base);
    let b = store(&base);
    let (ra, rb) = (
        a.claim("pod-a", Duration::seconds(15), at(0))
            .await
            .unwrap(),
        b.claim("pod-a", Duration::seconds(15), at(0))
            .await
            .unwrap(),
    );
    let held = [
        matches!(ra, LeaseOutcome::Held(_)),
        matches!(rb, LeaseOutcome::Held(_)),
    ]
    .iter()
    .filter(|h| **h)
    .count();
    assert_eq!(held, 1, "exactly one contender may hold the lease");
    handle.join().unwrap();
}
