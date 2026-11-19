//! RV-R8 #1 + #7: S3 wire-level regression --
//! * `send()` must really send the signed body: the mock S3 asserts that the received
//!   bytes match the hash declared by `x-amz-content-sha256`, and a GET round trip
//!   returns the same data. (Before the fix, PUT always uploaded an empty object.)
//! * when `list` hits the pagination bound it must report it explicitly via `ListPage.truncated`,
//!   never silently dropping tail keys.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use vrt_objstore::sigv4::{sha256_hex, SecretString};
use vrt_objstore::{ObjectStore, S3Store, S3StoreConfig};

fn block<T: Send>(f: impl std::future::Future<Output = T> + Send) -> T {
    vrt_objstore::block_on_store(async move { Ok::<T, ()>(f.await) }).unwrap()
}

#[derive(Default)]
struct MockState {
    /// key -> stored bytes (the key part of the path-style `/{bucket}/{key}`).
    objects: BTreeMap<String, Vec<u8>>,
    /// Full record of (method, path+query, headers, body).
    requests: Vec<(String, String, Vec<(String, String)>, Vec<u8>)>,
    /// list behavior: true -> always IsTruncated+token (probes truncation reporting).
    list_forever_truncated: bool,
    list_pages: usize,
}

fn read_request(
    reader: &mut BufReader<impl Read>,
) -> (String, String, Vec<(String, String)>, Vec<u8>) {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut it = line.split(' ');
    let method = it.next().unwrap().to_string();
    let path = it.next().unwrap_or_default().to_string();
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).unwrap();
        let h = h.trim();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).unwrap();
    (method, path, headers, body)
}

fn respond(stream: &mut impl Write, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    let mut out = format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    stream.write_all(out.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
}

fn serve(state: Arc<Mutex<MockState>>, listener: TcpListener) {
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let (method, path, headers, body) = read_request(&mut reader);
        let mut st = state.lock().unwrap();
        st.requests
            .push((method.clone(), path.clone(), headers.clone(), body.clone()));
        // path-style: /{bucket}/{key}
        let key = path
            .split('?')
            .next()
            .unwrap()
            .trim_start_matches("/b/")
            .to_string();
        let is_list = path.split('?').nth(1).unwrap_or("").contains("list-type=2");
        if method == "PUT" {
            st.objects.insert(key, body);
            drop(st);
            respond(&mut stream, "200 OK", &[("etag", "\"deadbeef\"")], b"");
        } else if method == "GET" && is_list {
            st.list_pages += 1;
            let page = st.list_pages;
            let forever = st.list_forever_truncated;
            // RV-R28 #3: functional list mock — honors prefix /
            // start-after / delimiter like real S3 so wire tests
            // can observe the signed query params and the response
            // shape the store parses (Contents + CommonPrefixes).
            let query = path.split('?').nth(1).unwrap_or("").to_string();
            let qp = |name: &str| -> Option<String> {
                for part in query.split('&') {
                    if let Some((k, v)) = part.split_once('=') {
                        if k == name {
                            return Some(v.replace("%2F", "/").replace("%2f", "/"));
                        }
                    }
                }
                None
            };
            let (pfx, after, delim) = (
                qp("prefix").unwrap_or_default(),
                qp("start-after"),
                qp("delimiter"),
            );
            let mut xml = String::from(
                "<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>",
            );
            if forever {
                xml.push_str(&format!(
                    "true</IsTruncated>\
                     <NextContinuationToken>tok-{page}</NextContinuationToken>\
                     <Contents><Key>k-{page}-a</Key><Size>1</Size></Contents>\
                     <Contents><Key>k-{page}-b</Key><Size>2</Size></Contents>\
                     </ListBucketResult>"
                ));
            } else {
                xml.push_str("false</IsTruncated>");
                let mut cps = Vec::new();
                for (k, v) in st.objects.range(pfx.clone()..) {
                    if !k.starts_with(pfx.as_str()) {
                        break;
                    }
                    if let Some(a) = &after {
                        if k.as_str() <= a.as_str() {
                            continue;
                        }
                    }
                    let rest = &k[pfx.len()..];
                    if let Some(d) = &delim {
                        if let Some(i) = rest.find(d.as_str()) {
                            cps.push(format!("{}{}", pfx, &rest[..i + d.len()]));
                            continue;
                        }
                    }
                    xml.push_str(&format!(
                        "<Contents><Key>{k}</Key><Size>{}</Size></Contents>",
                        v.len()
                    ));
                }
                cps.sort();
                cps.dedup();
                for cp in cps {
                    xml.push_str(&format!("<CommonPrefixes><Prefix>{cp}</Prefix></CommonPrefixes>"));
                }
                xml.push_str("</ListBucketResult>");
            }
            drop(st);
            respond(
                &mut stream,
                "200 OK",
                &[("content-type", "application/xml")],
                xml.as_bytes(),
            );
        } else if method == "GET" {
            match st.objects.get(&key) {
                Some(bytes) => {
                    let bytes = bytes.clone();
                    drop(st);
                    respond(&mut stream, "200 OK", &[("etag", "\"deadbeef\"")], &bytes);
                }
                None => {
                    drop(st);
                    respond(&mut stream, "404 Not Found", &[], b"");
                }
            }
        } else {
            drop(st);
            respond(&mut stream, "400 Bad Request", &[], b"");
        }
    }
}

fn store_at(addr: std::net::SocketAddr) -> S3Store {
    S3Store::new(S3StoreConfig {
        endpoint: format!("http://{addr}"),
        region: "auto".into(),
        bucket: "b".into(),
        access_key: "ak".into(),
        secret_key: SecretString::new("sk"),
        timeout: std::time::Duration::from_secs(5),
        conditional_writes: true,
        retries: 1,
        prefix: String::new(),
    })
    .unwrap()
}

fn spawn_mock(list_forever_truncated: bool) -> (S3Store, Arc<Mutex<MockState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(Mutex::new(MockState {
        list_forever_truncated,
        ..Default::default()
    }));
    let st = state.clone();
    std::thread::spawn(move || serve(st, listener));
    (store_at(addr), state)
}

#[test]
fn put_transmits_the_signed_body_and_get_roundtrips() {
    let (store, state) = spawn_mock(false);
    let payload: Vec<u8> = (0u8..=255).cycle().take(37_001).collect();

    block(store.put("w/seg-1.seg", &payload)).unwrap();

    let st = state.lock().unwrap();
    let (method, _path, headers, wire_body) = st
        .requests
        .iter()
        .find(|(m, p, _, _)| m == "PUT" && p.starts_with("/b/w/seg-1.seg"))
        .expect("PUT must reach the mock");
    assert_eq!(method, "PUT");
    // On-the-wire body == original bytes, and matches the payload hash committed to in the signature --
    // before the fix this was 0 bytes (the signature said one thing, the wire carried another).
    assert_eq!(wire_body, &payload, "PUT body must be transmitted");
    let sha_header = headers
        .iter()
        .find(|(k, _)| k == "x-amz-content-sha256")
        .map(|(_, v)| v.as_str())
        .expect("payload-hash header signed");
    assert_eq!(sha_header, sha256_hex(&payload).as_str());
    let cl = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .map(|(_, v)| v.clone());
    assert_eq!(cl.as_deref(), Some(payload.len().to_string().as_str()));
    drop(st);

    // GET round trip: what the mock stored is what arrived on the wire.
    let obj = block(store.get("w/seg-1.seg")).unwrap().expect("object exists");
    assert_eq!(obj.body, payload);
}

#[test]
fn put_if_none_match_also_transmits_body() {
    let (store, state) = spawn_mock(false);
    block(store.put_if_none_match("once", b"hello")).unwrap();
    let st = state.lock().unwrap();
    let (_, _, headers, wire_body) = st
        .requests
        .iter()
        .find(|(m, p, _, _)| m == "PUT" && p == "/b/once")
        .unwrap();
    assert_eq!(wire_body, b"hello");
    assert!(headers
        .iter()
        .any(|(k, v)| k == "if-none-match" && v == "*"));
}

#[test]
fn empty_body_put_declares_content_length_zero() {
    // R2 returns 411 Length Required for an empty-body PUT without an
    // explicit Content-Length (vrt-sql self_check probe).
    let (store, state) = spawn_mock(false);
    block(store.put_if_none_match("probe", b"")).unwrap();
    block(store.put_if_none_match("probe2", b"abc")).unwrap();
    let st = state.lock().unwrap();
    let cl = |path: &str| -> Option<String> {
        st.requests
            .iter()
            .find(|(m, p, _, _)| m == "PUT" && p == path)
            .and_then(|(_, _, h, _)| h.iter().find(|(k, _)| k == "content-length"))
            .map(|(_, v)| v.clone())
    };
    assert_eq!(cl("/b/probe").as_deref(), Some("0"));
    assert_eq!(cl("/b/probe2").as_deref(), Some("3"));
    // Signature unaffected: content-length is not a signed header.
    let (_, _, headers, _) = st
        .requests
        .iter()
        .find(|(m, p, _, _)| m == "PUT" && p == "/b/probe")
        .unwrap();
    let auth = headers.iter().find(|(k, _)| k == "authorization").unwrap();
    assert!(!auth.1.contains("content-length"));
    assert!(auth.1.contains("x-amz-content-sha256"));
}

#[test]
fn list_reports_truncation_instead_of_silent_prefix() {
    let (store, state) = spawn_mock(true);
    let page = block(store.list("k-")).unwrap();
    // The mock gives IsTruncated+token on every page -> after filling LIST_FETCH_BOUND=64 pages
    // it must truthfully report truncation, never return a "looks complete" short list.
    assert!(page.truncated, "bounded listing must surface truncation");
    assert_eq!(page.entries.len(), 64 * 2);
    assert_eq!(state.lock().unwrap().list_pages, 64);
}

#[test]
fn list_complete_page_is_not_truncated() {
    let (store, _state) = spawn_mock(false);
    block(store.put("w/a", b"1")).unwrap();
    block(store.put("w/b", b"2")).unwrap();
    let page = block(store.list("w/")).unwrap();
    assert!(!page.truncated);
    assert_eq!(page.entries.len(), 2);
}

#[test]
fn put_file_rejects_source_over_single_object_bound() {
    // RV-R8 #7: the default put_file is the whole-file buffering path -- an oversized file must be refused
    // at the stat stage, without allocating equivalent memory for it.
    let store = vrt_objstore::MemoryStore::new();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("big.bin");
    let f = std::fs::File::create(&src).unwrap();
    f.set_len(vrt_objstore::MAX_PUT_FILE_BYTES + 1).unwrap(); // sparse
    let err = block(store.put_file("k/big", &src, false)).unwrap_err();
    match err {
        vrt_objstore::StoreError::Unavailable { retryable, reason } => {
            assert!(!retryable, "oversized put_file is a hard reject");
            assert!(reason.contains("MAX_PUT_FILE_BYTES"), "reason: {reason}");
        }
        other => panic!("expected Unavailable, got {other:?}"),
    }
    // A file within the limit goes through (control).
    let small = dir.path().join("small.bin");
    std::fs::write(&small, b"payload").unwrap();
    let out = block(store.put_file("k/ok", &small, false)).unwrap();
    assert_eq!(out.len, 7);
    assert_eq!(out.sha256, sha256_hex(b"payload"));
}

/// RV-R28 #3: `list_children` must send `delimiter=` on the wire and
/// fold `<CommonPrefixes>` into `common_prefixes` — WAL stem
/// discovery depends on this instead of a boundable flat list.
#[test]
fn list_children_sends_delimiter_and_folds_common_prefixes() {
    let (store, state) = spawn_mock(false);
    block(store.put("w/a/1", b"1")).unwrap();
    block(store.put("w/a/2", b"22")).unwrap();
    block(store.put("w/b/1", b"1")).unwrap();
    block(store.put("w/top", b"t")).unwrap();

    let page = block(store.list_children("w/", "/")).unwrap();
    assert!(!page.truncated);
    assert_eq!(
        page.common_prefixes,
        vec!["w/a/".to_string(), "w/b/".to_string()]
    );
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, "w/top");

    let st = state.lock().unwrap();
    let (_, path, _, _) = st
        .requests
        .iter()
        .find(|(m, p, _, _)| m == "GET" && p.contains("list-type=2"))
        .expect("list request must reach the mock");
    assert!(
        path.contains("delimiter="),
        "delimiter must be on the wire: {path}"
    );
}

/// RV-R28 #3: `list_after` must send `start-after=` and only return
/// keys strictly past the cursor — the GC sweep resumes there
/// instead of restarting at the head of a truncated page.
#[test]
fn list_after_sends_start_after_cursor() {
    let (store, state) = spawn_mock(false);
    block(store.put("w/a", b"1")).unwrap();
    block(store.put("w/b", b"2")).unwrap();
    block(store.put("w/c", b"3")).unwrap();

    let page = block(store.list_after("w/", Some("w/a"))).unwrap();
    let keys: Vec<String> = page.entries.iter().map(|e| e.key.clone()).collect();
    assert_eq!(keys, vec!["w/b".to_string(), "w/c".to_string()]);

    let st = state.lock().unwrap();
    let (_, path, _, _) = st
        .requests
        .iter()
        .find(|(m, p, _, _)| m == "GET" && p.contains("list-type=2"))
        .expect("list request must reach the mock");
    assert!(
        path.contains("start-after="),
        "start-after must be on the wire: {path}"
    );
}

/// RV-R71: an S3 that always returns 503; returns the request counter.
fn spawn_always_503(retries: u32) -> (S3Store, Arc<Mutex<usize>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(Mutex::new(0usize));
    let c = count.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let _ = read_request(&mut reader);
            *c.lock().unwrap() += 1;
            respond(&mut stream, "503 Service Unavailable", &[], b"");
        }
    });
    let store = S3Store::new(S3StoreConfig {
        endpoint: format!("http://{addr}"),
        region: "auto".into(),
        bucket: "b".into(),
        access_key: "ak".into(),
        secret_key: SecretString::new("sk"),
        timeout: std::time::Duration::from_secs(5),
        conditional_writes: true,
        retries,
        prefix: String::new(),
    })
    .unwrap();
    (store, count)
}

#[test]
fn rv_r71_idempotent_get_retries_bounded_conditional_write_does_not() {
    let (store, count) = spawn_always_503(3);
    let r = block(async { Ok::<_, ()>(store.get("k").await) });
    assert!(r.unwrap().is_err());
    assert_eq!(*count.lock().unwrap(), 3, "GET is retried exactly `retries` times");

    *count.lock().unwrap() = 0;
    let r = block(async { Ok::<_, ()>(store.put_if_none_match("k2", b"x").await) });
    assert!(r.unwrap().is_err());
    assert_eq!(*count.lock().unwrap(), 1, "conditional write must be single-shot");

    *count.lock().unwrap() = 0;
    let r = block(async { Ok::<_, ()>(store.put_if_match("k3", "etag", b"x").await) });
    assert!(r.unwrap().is_err());
    assert_eq!(*count.lock().unwrap(), 1, "CAS must be single-shot");
}

/// 2026-11-15 regression: the sha256 returned by S3 `get_to_file` must be the sha256 of the downloaded content,
/// and match what `put_file` reported at upload. Before the fix the s3 backend fed
/// `hasher.finalize()` (the raw digest) into `sha256_hex` again -- returning
/// sha256(sha256(x)), which made vrt-sql restore always fail with "snapshot sha256
/// mismatch".
#[test]
fn get_to_file_sha256_matches_content_and_put_file_report() {
    let (store, _state) = spawn_mock(false);
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.bin");
    let payload: Vec<u8> = (0u8..=255).cycle().take(100_003).collect();
    std::fs::write(&src, &payload).unwrap();

    let put = block(store.put_file("w/snap-0.snap", &src, true)).unwrap();
    assert_eq!(put.sha256, sha256_hex(&payload));

    let dst = dir.path().join("dst.bin");
    let got = block(store.get_to_file("w/snap-0.snap", &dst))
        .unwrap()
        .expect("object exists");
    assert_eq!(std::fs::read(&dst).unwrap(), payload);
    assert_eq!(got.len, payload.len() as u64);
    assert_eq!(got.sha256, sha256_hex(&payload), "sha must hash the content once");
    assert_eq!(got.sha256, put.sha256, "put_file and get_to_file must agree");
}
