//! RV-R71 item 4: the kvrocks provider dials lazily, redials after a
//! broken link, rate-limits failed dials, and its probe reflects reality.
//! A tiny fake RESP server (answers `PING`, `HELLO`, `CLIENT`) stands in
//! for kvrocks; no external service is needed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vrt_provider::error::ProviderError;
use vrt_provider::kv::KvrocksKvProvider;

struct Fake {
    port: u16,
    accepts: Arc<AtomicUsize>,
    /// Dropping connections: when set, every open socket is closed.
    kill: Arc<tokio::sync::Notify>,
    handle: tokio::task::JoinHandle<()>,
}

async fn serve_on(listener: TcpListener) -> Fake {
    let port = listener.local_addr().unwrap().port();
    let accepts = Arc::new(AtomicUsize::new(0));
    let kill = Arc::new(tokio::sync::Notify::new());
    let (a, k) = (accepts.clone(), kill.clone());
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            a.fetch_add(1, Ordering::SeqCst);
            let k = k.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                loop {
                    tokio::select! {
                        _ = k.notified() => return,
                        n = sock.read(&mut buf) => {
                            let Ok(n) = n else { return };
                            if n == 0 { return; }
                            let text = String::from_utf8_lossy(&buf[..n]).to_uppercase();
                            // One reply per RESP command in this read.
                            let mut out = Vec::new();
                            for cmd in text.split('*').filter(|c| !c.trim().is_empty()) {
                                if cmd.contains("PING") {
                                    out.extend_from_slice(b"+PONG\r\n");
                                } else {
                                    out.extend_from_slice(b"+OK\r\n");
                                }
                            }
                            if sock.write_all(&out).await.is_err() { return; }
                        }
                    }
                }
            });
        }
    });
    Fake {
        port,
        accepts,
        kill,
        handle,
    }
}

async fn fake() -> Fake {
    serve_on(TcpListener::bind("127.0.0.1:0").await.unwrap()).await
}

fn provider(port: u16) -> KvrocksKvProvider {
    KvrocksKvProvider::lazy(&format!("redis://127.0.0.1:{port}/"), Duration::from_millis(500))
        .unwrap()
        .with_timeout(Duration::from_millis(500))
        .without_read_retry()
}

#[tokio::test]
async fn lazy_construction_does_not_dial() {
    let f = fake().await;
    let _p = provider(f.port);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(f.accepts.load(Ordering::SeqCst), 0);
    f.handle.abort();
}

#[tokio::test]
async fn unreachable_at_start_fails_closed_then_recovers_after_cooldown() {
    // Reserve a port, close it: nothing is listening.
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let p = provider(port);
    assert_eq!(p.probe().await.unwrap_err(), ProviderError::KvUnavailable);

    // Server comes up on the same port; inside the cooldown the provider
    // does NOT dial again (bounded dial rate) ...
    let f = serve_on(TcpListener::bind(("127.0.0.1", port)).await.unwrap()).await;
    assert_eq!(p.probe().await.unwrap_err(), ProviderError::KvUnavailable);
    assert_eq!(f.accepts.load(Ordering::SeqCst), 0, "cooldown must gate dials");

    // ... and after it, the very next call recovers without a restart.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    p.probe().await.unwrap();
    assert_eq!(f.accepts.load(Ordering::SeqCst), 1);
    f.handle.abort();
}

#[tokio::test]
async fn broken_link_is_redialed() {
    let f = fake().await;
    let p = provider(f.port);
    p.probe().await.unwrap();
    p.probe().await.unwrap();
    assert_eq!(f.accepts.load(Ordering::SeqCst), 1, "connection is reused");

    // Server drops every socket; the next call sees the dead link ...
    f.kill.notify_waiters();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut healed = false;
    for _ in 0..5 {
        if p.probe().await.is_ok() {
            healed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // ... and a fresh connection is made (the old one would fail forever).
    assert!(healed, "provider never reconnected");
    assert!(f.accepts.load(Ordering::SeqCst) >= 2);
    f.handle.abort();
}

#[tokio::test]
async fn hung_server_times_out_and_probe_is_bounded() {
    // Accepts but never answers.
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let h = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = l.accept().await {
            held.push(s);
        }
    });
    let p = provider(port);
    let t = std::time::Instant::now();
    assert_eq!(p.probe().await.unwrap_err(), ProviderError::KvUnavailable);
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    h.abort();
}

#[tokio::test]
async fn malformed_url_is_rejected_at_construction() {
    assert!(KvrocksKvProvider::lazy("not a url", Duration::from_millis(100)).is_err());
}
