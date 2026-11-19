//! RV-R8 #4: `get_many` must charge the aggregate budget against
//! DECLARED value lengths (HSTRLEN) BEFORE issuing a single `HGET v` —
//! this test proves the early abort at the wire level with a fake
//! RESP server: the batch dies on the length probe and no value
//! command ever reaches the "backend".

mod support;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::error::ProviderError;
use vrt_provider::kv::{KvProvider, KvrocksKvProvider};

fn ctx() -> vrt_provider::BindingContext {
    let f = fixture();
    let store = store_with(
        &f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![worker(
                    WORKER,
                    VERSION,
                    DIGEST,
                    vec![kv_binding("KV", NS_ID)],
                )],
            )],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    authorize(
        &auth,
        TENANT,
        &identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7),
        ResourceKind::Kv,
    )
    .unwrap()
}

/// Minimal RESP server: parses command arrays, answers from a canned
/// table keyed on the entry-key suffix, records every command seen.
struct FakeRedis {
    addr: std::net::SocketAddr,
    /// Commands received (arg vectors).
    seen: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

fn read_command(r: &mut BufReader<impl Read>) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    if r.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let n: usize = line.trim().strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        let mut l = String::new();
        r.read_line(&mut l).ok()?;
        let len: usize = l.trim().strip_prefix('$')?.parse().ok()?;
        let mut buf = vec![0u8; len + 2];
        r.read_exact(&mut buf).ok()?;
        buf.truncate(len);
        args.push(buf);
    }
    Some(args)
}

fn resp_bulk(w: &mut impl Write, v: Option<&[u8]>) {
    match v {
        Some(b) => {
            write!(w, "${}\r\n", b.len()).unwrap();
            w.write_all(b).unwrap();
            w.write_all(b"\r\n").unwrap();
        }
        None => w.write_all(b"$-1\r\n").unwrap(),
    }
}

fn handle_command(w: &mut impl Write, args: &[Vec<u8>]) {
    let cmd = String::from_utf8_lossy(args.first().map_or(b"", |a| a)).to_ascii_uppercase();
    let key = args.get(1).map(|k| String::from_utf8_lossy(k).into_owned());
    let big = key.as_deref().is_some_and(|k| k.contains("big"));
    match cmd.as_str() {
        // HMGET <key> f1 f2 ... — `e` → "-1" (no expiry), `m` → nil.
        "HMGET" => {
            let nfields = args.len() - 2;
            write!(w, "*{nfields}\r\n").unwrap();
            for f in &args[2..] {
                match f.as_slice() {
                    b"e" => resp_bulk(w, Some(b"-1")),
                    _ => resp_bulk(w, None),
                }
            }
        }
        // HSTRLEN <key> v — big is 30MiB (over the 25MiB cap), others tiny.
        "HSTRLEN" => {
            let n: usize = if big { 30 * 1024 * 1024 } else { 4 };
            write!(w, ":{n}\r\n").unwrap();
        }
        // HGET <key> v — single-field read.
        "HGET" => resp_bulk(w, Some(b"tiny")),
        _ => w.write_all(b"+OK\r\n").unwrap(),
    }
}

fn fake_redis() -> FakeRedis {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            while let Some(args) = read_command(&mut reader) {
                seen2.lock().unwrap().push(args.clone());
                handle_command(&mut stream, &args);
            }
        }
    });
    FakeRedis { addr, seen }
}

#[tokio::test]
async fn get_many_aborts_on_declared_length_before_any_value_fetch() {
    let srv = fake_redis();
    let client = redis::Client::open(format!("redis://{}/", srv.addr)).unwrap();
    let conn = client.get_multiplexed_tokio_connection().await.unwrap();
    let provider = KvrocksKvProvider::new(conn).without_read_retry();
    let c = ctx();

    // small fits, big declares 30MiB — the whole batch must die on the
    // length probe; phase 2 must never issue a value fetch.
    let err = provider
        .get_many(&c, &[b"small".to_vec(), b"big".to_vec()])
        .await
        .unwrap_err();
    assert!(
        matches!(err, ProviderError::KvResponseTooLarge),
        "aggregate over-budget: {err:?}"
    );
    let seen = srv.seen.lock().unwrap();
    assert!(
        !seen
            .iter()
            .any(|cmd| cmd.first().is_some_and(|a| a == b"HGET")),
        "no HGET v may reach the backend after the budget trips: {seen:?}"
    );
    assert!(seen
        .iter()
        .any(|cmd| cmd.first().is_some_and(|a| a == b"HSTRLEN")));
}

#[tokio::test]
async fn get_many_fetches_values_only_for_fitting_keys() {
    let srv = fake_redis();
    let client = redis::Client::open(format!("redis://{}/", srv.addr)).unwrap();
    let conn = client.get_multiplexed_tokio_connection().await.unwrap();
    let provider = KvrocksKvProvider::new(conn).without_read_retry();
    let c = ctx();

    // Control: fitting entries go through all three phases.
    let entries = provider.get_many(&c, &[b"small".to_vec()]).await.unwrap();
    assert_eq!(entries.len(), 1);
    let entry = entries[0].as_ref().expect("small is present");
    assert_eq!(entry.value, b"tiny");
    let seen = srv.seen.lock().unwrap();
    assert!(seen
        .iter()
        .any(|cmd| cmd.first().is_some_and(|a| a == b"HMGET")));
    assert!(seen
        .iter()
        .any(|cmd| cmd.first().is_some_and(|a| a == b"HSTRLEN")));
    assert!(seen
        .iter()
        .any(|cmd| cmd.first().is_some_and(|a| a == b"HGET")));
}
