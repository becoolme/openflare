//! T13/M3: `onlyIf` pushed down as S3 conditional-write headers —
//! `If-Match` anchors the HEAD-seen etag (existing object),
//! `If-None-Match: *` anchors absence. 412 ⇒ precondition not met
//! (`Ok(None)`); `conditional_writes=false` ⇒ `CapabilityUnsupported`
//! rather than a non-atomic write.

mod support;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::error::ProviderError;
use vrt_provider::r2::s3::{S3Config, S3R2Provider, SecretString};
use vrt_provider::r2::wire::{R2Condition, R2EtagMatch, R2PutOptions, R2SsecKey};
use vrt_provider::r2::R2Provider;

#[derive(Default)]
struct MockState {
    /// `version`, `uploaded`, `etag`, `len` of the stored object.
    object: Option<(String, String, String, usize)>,
    /// Captured (method, path, headers) for assertions.
    requests: Vec<(String, Vec<(String, String)>)>,
    /// Next PUTs that must answer 412.
    put_412s: usize,
    /// When set, any SSE-C HEAD must present this base64 key MD5 or the
    /// mock answers 400 (wrong-key classification).
    ssec_key_md5: Option<String>,
}

struct MockS3 {
    addr: std::net::SocketAddr,
    state: Arc<Mutex<MockState>>,
}

fn read_request(reader: &mut BufReader<impl Read>) -> (String, Vec<(String, String)>, Vec<u8>) {
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let method = line.split(' ').next().unwrap().to_string();
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
    (method, headers, body)
}

fn respond(stream: &mut impl Write, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    // HEAD responses carry the OBJECT's content-length, not the body
    // size — let an explicit header win over the default.
    let declared = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map(|(_, v)| v.to_string())
        .unwrap_or_else(|| body.len().to_string());
    let mut out =
        format!("HTTP/1.1 {status}\r\ncontent-length: {declared}\r\nconnection: close\r\n");
    for (k, v) in headers {
        if !k.eq_ignore_ascii_case("content-length") {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
    }
    out.push_str("\r\n");
    stream.write_all(out.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
}

/// Start a mock S3 that serves HEAD/PUT deterministically.
fn mock_s3() -> MockS3 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(Mutex::new(MockState::default()));
    let st = state.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let mut stream = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let st = st.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let (method, headers, body) = read_request(&mut reader);
                let mut g = st.lock().unwrap();
                g.requests.push((method.clone(), headers.clone()));
                match method.as_str() {
                    "HEAD" => {
                        if let Some(expected) = &g.ssec_key_md5 {
                            let presented = headers
                                .iter()
                                .find(|(k, _)| {
                                    k.eq_ignore_ascii_case(
                                        "x-amz-server-side-encryption-customer-key-MD5",
                                    )
                                })
                                .map(|(_, v)| v.as_str());
                            if presented != Some(expected.as_str()) {
                                respond(
                                    &mut stream,
                                    "400 Bad Request",
                                    &[],
                                    b"<Error><Code>InvalidRequest</Code></Error>",
                                );
                                return;
                            }
                        }
                        match &g.object {
                            Some((ver, up, etag, len)) => {
                                let hs = vec![
                                    ("x-amz-meta-vrt-schema", "1".to_string()),
                                    ("x-amz-meta-vrt-version", ver.clone()),
                                    ("x-amz-meta-vrt-uploaded", up.clone()),
                                    ("x-amz-meta-vrt-md5", "aa".to_string()),
                                    ("x-amz-meta-vrt-sha1", "aa".to_string()),
                                    ("x-amz-meta-vrt-sha256", "aa".to_string()),
                                    ("x-amz-meta-vrt-sha384", "aa".to_string()),
                                    ("x-amz-meta-vrt-sha512", "aa".to_string()),
                                    ("x-amz-meta-vrt-class", "standard".to_string()),
                                    ("etag", format!("\"{etag}\"")),
                                    ("content-length", len.to_string()),
                                ];
                                let pairs: Vec<(&str, &str)> =
                                    hs.iter().map(|(k, v)| (*k, v.as_str())).collect();
                                respond(&mut stream, "200 OK", &pairs, &[]);
                            }
                            None => respond(&mut stream, "404 Not Found", &[], &[]),
                        }
                    }
                    "PUT" => {
                        if g.put_412s > 0 {
                            g.put_412s -= 1;
                            respond(&mut stream, "412 Precondition Failed", &[], &[]);
                        } else {
                            let get = |k: &str| {
                                headers
                                    .iter()
                                    .find(|(h, _)| h == k)
                                    .map(|(_, v)| v.clone())
                                    .unwrap_or_default()
                            };
                            g.object = Some((
                                get("x-amz-meta-vrt-version"),
                                get("x-amz-meta-vrt-uploaded"),
                                "obj-etag".to_string(),
                                body.len(),
                            ));
                            respond(&mut stream, "200 OK", &[("etag", "\"obj-etag\"")], &[]);
                        }
                    }
                    _ => respond(&mut stream, "405 Method Not Allowed", &[], &[]),
                }
            });
        }
    });
    MockS3 { addr, state }
}

fn provider(mock: &MockS3) -> S3R2Provider {
    S3R2Provider::new(S3Config {
        endpoint: format!("http://{}", mock.addr),
        region: "auto".to_string(),
        bucket: "test-bucket".to_string(),
        access_key: "ak".to_string(),
        secret_key: SecretString::new("sk".to_string()),
        timeout: std::time::Duration::from_secs(10),
        max_object_bytes: 64 * 1024 * 1024,
        conditional_writes: true,
        multipart_ttl_ms: 60_000,
    })
    .unwrap()
}

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
                    vec![r2_binding("BUCKET", BUCKET)],
                )],
            )],
        ),
        7,
    );
    let auth = authorizer(store, 30_000);
    let id = identity(WORKER, "BUCKET", WORKER, VERSION, DIGEST, BUCKET, 7);
    authorize(&auth, TENANT, &id, ResourceKind::R2).unwrap()
}

fn only_if_not_exists() -> R2PutOptions {
    R2PutOptions {
        only_if: Some(R2Condition {
            etag_does_not_match: vec![R2EtagMatch::Wildcard],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Missing object + onlyIf ⇒ PUT carries `If-None-Match: *`.
#[tokio::test]
async fn conditional_put_missing_object_sends_if_none_match() {
    let mock = mock_s3();
    let p = provider(&mock);
    let c = ctx();
    let meta = match p.put(&c, "k1", b"body", &only_if_not_exists()).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("PUT failed: {:?}", e);
            eprintln!("requests: {:?}", mock.state.lock().unwrap().requests);
            panic!("put failed");
        }
    };
    assert!(meta.is_some());
    let g = mock.state.lock().unwrap();
    let put = g
        .requests
        .iter()
        .find(|(m, _)| m == "PUT")
        .expect("put request");
    assert!(
        put.1.iter().any(|(k, v)| k == "if-none-match" && v == "*"),
        "missing If-None-Match: * — headers: {:?}",
        put.1
    );
}

/// Existing object + onlyIf ⇒ PUT anchors `If-Match: <seen-etag>`.
#[tokio::test]
async fn conditional_put_existing_object_anchors_etag() {
    let mock = mock_s3();
    mock.state.lock().unwrap().object = Some((
        "v-0".to_string(),
        "1700000000000".to_string(),
        "obj-etag".to_string(),
        4,
    ));
    let p = provider(&mock);
    let c = ctx();
    let options = R2PutOptions {
        only_if: Some(R2Condition {
            etag_matches: vec![R2EtagMatch::Strong {
                value: "obj-etag".to_string(),
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let meta = p.put(&c, "k1", b"body", &options).await.unwrap();
    assert!(meta.is_some());
    let g = mock.state.lock().unwrap();
    let put = g.requests.iter().find(|(m, _)| m == "PUT").expect("put");
    assert!(
        put.1
            .iter()
            .any(|(k, v)| k == "if-match" && v.contains("obj-etag")),
        "missing If-Match anchor — headers: {:?}",
        put.1
    );
}

/// 412 from the backend ⇒ precondition not met (`Ok(None)`), with one
/// bounded HEAD+PUT retry — never a silent TOCTOU overwrite.
#[tokio::test]
async fn conditional_put_412_maps_to_none() {
    let mock = mock_s3();
    mock.state.lock().unwrap().put_412s = 2;
    let p = provider(&mock);
    let c = ctx();
    let res = p
        .put(&c, "k1", b"body", &only_if_not_exists())
        .await
        .unwrap();
    assert!(res.is_none(), "412 must map to precondition-not-met");
    let puts = mock
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(m, _)| m == "PUT")
        .count();
    assert_eq!(puts, 2, "exactly one bounded retry, not a loop");
}

/// Backend declared without conditional-write support ⇒ typed
/// `CapabilityUnsupported`, never a degraded non-atomic write.
#[tokio::test]
async fn conditional_put_unsupported_backend_fails_closed() {
    let mock = mock_s3();
    // Provider built with conditional_writes=false.
    let p = S3R2Provider::new(S3Config {
        endpoint: format!("http://{}", mock.addr),
        region: "auto".to_string(),
        bucket: "test-bucket".to_string(),
        access_key: "ak".to_string(),
        secret_key: SecretString::new("sk".to_string()),
        timeout: std::time::Duration::from_secs(10),
        max_object_bytes: 64 * 1024 * 1024,
        conditional_writes: false,
        multipart_ttl_ms: 60_000,
    })
    .unwrap();
    let c = ctx();
    let err = p
        .put(&c, "k1", b"body", &only_if_not_exists())
        .await
        .unwrap_err();
    assert_eq!(err, ProviderError::CapabilityUnsupported);
    // Unconditional put still works on the same backend.
    let ok = p
        .put(&c, "k1", b"body", &R2PutOptions::default())
        .await
        .unwrap();
    assert!(ok.is_some());
}

fn request_header<'a>(request: &'a (String, Vec<(String, String)>), name: &str) -> Option<&'a str> {
    request
        .1
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// A key-bearing put presents the three SSE-C headers on the preflight
/// HEAD, the PUT itself and the post-write verification HEAD.
#[tokio::test]
async fn ssec_put_sends_customer_key_headers() {
    let mock = mock_s3();
    let p = provider(&mock);
    let c = ctx();
    let key = R2SsecKey::parse(&"ab".repeat(32)).unwrap();
    let options = R2PutOptions {
        ssec_key: Some(key.clone()),
        ..Default::default()
    };
    assert!(p.put(&c, "k1", b"body", &options).await.unwrap().is_some());

    let g = mock.state.lock().unwrap();
    let with_key: Vec<_> = g
        .requests
        .iter()
        .filter(|(m, _)| m == "HEAD" || m == "PUT")
        .collect();
    assert_eq!(with_key.len(), 3, "HEAD, PUT, verification HEAD");
    for request in with_key {
        assert_eq!(
            request_header(request, "x-amz-server-side-encryption-customer-algorithm"),
            Some("AES256"),
            "{request:?}"
        );
        assert_eq!(
            request_header(request, "x-amz-server-side-encryption-customer-key"),
            Some(key.s3_key_b64().as_str()),
            "{request:?}"
        );
        assert_eq!(
            request_header(request, "x-amz-server-side-encryption-customer-key-MD5"),
            Some(key.s3_key_md5_b64().as_str()),
            "{request:?}"
        );
    }
}

/// A plain put must be byte-for-byte unchanged: no SSE-C headers anywhere.
#[tokio::test]
async fn plain_put_sends_no_ssec_headers() {
    let mock = mock_s3();
    let p = provider(&mock);
    let c = ctx();
    assert!(p
        .put(&c, "k1", b"body", &R2PutOptions::default())
        .await
        .unwrap()
        .is_some());
    let g = mock.state.lock().unwrap();
    for request in &g.requests {
        assert!(
            !request
                .1
                .iter()
                .any(|(k, _)| k.starts_with("x-amz-server-side-encryption-customer-")),
            "plain request carried SSE-C headers: {request:?}"
        );
    }
}

/// A GET with the wrong customer key surfaces the backend rejection as
/// the typed SSE-C error — never a 200 with undecryptable bytes.
#[tokio::test]
async fn ssec_get_wrong_key_is_typed_error() {
    let mock = mock_s3();
    mock.state.lock().unwrap().object = Some((
        "v-0".to_string(),
        "1700000000000".to_string(),
        "obj-etag".to_string(),
        4,
    ));
    let good = R2SsecKey::parse(&"ab".repeat(32)).unwrap();
    mock.state.lock().unwrap().ssec_key_md5 = Some(good.s3_key_md5_b64());
    let p = provider(&mock);
    let c = ctx();
    let wrong = R2SsecKey::parse(&"cd".repeat(32)).unwrap();
    let err = p.get(&c, "k1", None, None, Some(&wrong)).await.unwrap_err();
    assert_eq!(err, ProviderError::R2SsecInvalid);
}
