//! Test-only pg-wire backend: accepts the startup handshake, then
//! answers simple queries. Per-test switches make a socket fail on the
//! checkout probe or on `DISCARD ALL`, and every statement it sees is
//! recorded so pool ordering (e.g. "reset before reuse") is assertable.
//!
//! Unit tests only — never part of a shipped build.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pgwire;

/// Counters and failure switches for one mock backend instance.
#[derive(Default)]
pub(crate) struct Mock {
    accepted: AtomicU64,
    pings: AtomicU64,
    resets: AtomicU64,
    fail_ping: AtomicBool,
    fail_reset: AtomicBool,
    statements: Mutex<Vec<String>>,
}

impl Mock {
    /// TCP connections accepted (= backend dials).
    pub(crate) fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }

    /// `SELECT 1` health checks served.
    pub(crate) fn pings(&self) -> u64 {
        self.pings.load(Ordering::SeqCst)
    }

    /// `DISCARD ALL` statements served.
    pub(crate) fn resets(&self) -> u64 {
        self.resets.load(Ordering::SeqCst)
    }

    /// Every simple-query statement received, in order.
    pub(crate) fn statements(&self) -> Vec<String> {
        self.statements
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Drop the socket when the next `SELECT 1` arrives.
    pub(crate) fn fail_probe(&self, on: bool) {
        self.fail_ping.store(on, Ordering::SeqCst);
    }

    /// Answer `DISCARD ALL` with an ErrorResponse.
    pub(crate) fn fail_reset(&self, on: bool) {
        self.fail_reset.store(on, Ordering::SeqCst);
    }
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Spawn a mock backend on a loopback port; returns the port and its
/// counters.
pub(crate) async fn backend() -> (u16, Arc<Mock>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mock = Arc::new(Mock::default());
    let m = Arc::clone(&mock);
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            m.accepted.fetch_add(1, Ordering::SeqCst);
            let m = Arc::clone(&m);
            tokio::spawn(async move { serve(sock, m).await });
        }
    });
    (port, mock)
}

async fn serve(sock: TcpStream, mock: Arc<Mock>) {
    let (mut rd, mut wr) = sock.into_split();
    // Startup: u32 length word + body (protocol word + key/value pairs).
    let Ok(len) = rd.read_u32().await else {
        return;
    };
    let mut body = vec![0u8; (len as usize).saturating_sub(4)];
    if rd.read_exact(&mut body).await.is_err() {
        return;
    }
    if wr
        .write_all(&frame(b'R', &0u32.to_be_bytes()))
        .await
        .is_err()
    {
        return;
    }
    if wr.write_all(&frame(b'Z', b"I")).await.is_err() {
        return;
    }
    loop {
        let Ok(msg) = pgwire::read_message(&mut rd).await else {
            return;
        };
        match msg.tag {
            b'Q' => {
                let sql = String::from_utf8_lossy(&msg.body)
                    .trim_end_matches('\0')
                    .to_string();
                mock.statements
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(sql.clone());
                if sql == "SELECT 1" {
                    mock.pings.fetch_add(1, Ordering::SeqCst);
                    if mock.fail_ping.load(Ordering::SeqCst) {
                        return; // the socket dies on the probe
                    }
                } else if sql == "DISCARD ALL" {
                    mock.resets.fetch_add(1, Ordering::SeqCst);
                    if mock.fail_reset.load(Ordering::SeqCst) {
                        let _ = wr
                            .write_all(&frame(b'E', b"SERROR\0CXX000\0Mreset\0\0"))
                            .await;
                        let _ = wr.write_all(&frame(b'Z', b"I")).await;
                        continue;
                    }
                }
                let tag = format!("{}\0", sql.split(' ').next().unwrap_or("SELECT"));
                let _ = wr.write_all(&frame(b'C', tag.as_bytes())).await;
                let _ = wr.write_all(&frame(b'Z', b"I")).await;
            }
            b'X' => return,
            _ => return,
        }
    }
}
