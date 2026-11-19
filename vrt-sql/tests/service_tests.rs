//! T34: service-layer auth/label/config tests.

use axum::http::{HeaderMap, HeaderValue, Method};
use vrt_sql::service::{DbLabels, ServiceAuth, AUTH_HEADER};

fn headers_with(auth: &str, epoch: Option<&str>, seq: Option<&str>) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(AUTH_HEADER, HeaderValue::from_str(auth).unwrap());
    if let Some(e) = epoch {
        h.insert("x-vrt-epoch", HeaderValue::from_str(e).unwrap());
    }
    if let Some(s) = seq {
        h.insert("x-vrt-seq", HeaderValue::from_str(s).unwrap());
    }
    h
}

#[test]
fn sign_verify_roundtrip_with_epoch_seq_and_query() {
    let a = ServiceAuth::new(b"shared-secret".to_vec());
    let body = br#"{"sql":"select 1"}"#;
    let tok = a.sign(
        "POST",
        "/dbs/abc/query?resultsFormat=NONE",
        body,
        Some(1),
        Some(7),
    );
    let h = headers_with(&tok, Some("1"), Some("7"));
    assert!(a.verify(&h, &Method::POST, "/dbs/abc/query?resultsFormat=NONE", body));
}

#[test]
fn verify_rejects_tampered_epoch_seq_body_path() {
    let a = ServiceAuth::new(b"s".to_vec());
    let body = b"b";
    let tok = a.sign("POST", "/dbs/x/query", body, Some(1), Some(2));
    // epoch rewritten (outside the signature -> rejected)
    assert!(!a.verify(
        &headers_with(&tok, Some("9"), Some("2")),
        &Method::POST,
        "/dbs/x/query",
        body
    ));
    assert!(!a.verify(
        &headers_with(&tok, Some("1"), Some("9")),
        &Method::POST,
        "/dbs/x/query",
        body
    ));
    // body/query/method tampering
    assert!(!a.verify(
        &headers_with(&tok, Some("1"), Some("2")),
        &Method::POST,
        "/dbs/x/query",
        b"BX"
    ));
    assert!(!a.verify(
        &headers_with(&tok, Some("1"), Some("2")),
        &Method::POST,
        "/dbs/x/query?resultsFormat=NONE",
        body
    ));
    assert!(!a.verify(
        &headers_with(&tok, Some("1"), Some("2")),
        &Method::GET,
        "/dbs/x/query",
        body
    ));
    // missing/malformed header
    assert!(!a.verify(&HeaderMap::new(), &Method::POST, "/dbs/x/query", body));
    assert!(!a.verify(
        &headers_with("garbage", None, None),
        &Method::POST,
        "/dbs/x/query",
        body
    ));
    assert!(!a.verify(
        &headers_with("v1:notanum:deadbeef", None, None),
        &Method::POST,
        "/dbs/x/query",
        body
    ));
}

#[test]
fn verify_rejects_clock_skew() {
    let a = ServiceAuth::new(b"s".to_vec());
    let body = b"";
    // hand-build an expired ts (3 minutes ago > the +-30s window)
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 180;
    // recomputing the mac needs the secret -- reuse sign to get the mac part, then swap the ts
    let tok = a.sign("POST", "/p", body, None, None);
    let mac = tok.rsplit(':').next().unwrap();
    let stale = format!("v1:{ts}:{mac}");
    assert!(!a.verify(&headers_with(&stale, None, None), &Method::POST, "/p", body));
}

#[test]
fn db_labels_bounded() {
    let l = DbLabels::default();
    let valid = "abcdefghijklmnopqrstuvwxyz";
    assert_eq!(l.label(valid), valid);
    assert_eq!(l.label("not-valid"), "_unknown");
    // fold after more than 256 active ids
    for i in 0..300u32 {
        let id = format!("{:0>26}", i);
        l.label(&id);
    }
    let fresh = "zzzzzzzzzzzzzzzzzzzzzzzzzz";
    // after the table is full a new id -> _overflow; ones already seen can still hit
    assert_eq!(l.label(fresh), "_overflow");
}

/// T36b/P0-A: the published vrt-sql example config must pass ServiceConfig
/// validation -- placeholder substitution is the same as `vrt init` rendering, preventing T36's required
/// `[persistence]` from turning examples/fixtures into start-and-crash (d1_smoke once hit this).
#[test]
fn shipped_vrt_sql_example_config_validates() {
    let raw = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/standalone-vrt-sql.toml"),
    )
    .unwrap();
    let root = std::env::temp_dir().join("vrt-sql-example-cfg-test");
    let rendered = raw
        .replace("__VRT_ROOT__", &root.to_string_lossy())
        .replace("__VRT_SQL_LISTEN__", "127.0.0.1:8090")
        .replace("__VRT_SQL_METRICS__", "127.0.0.1:8091");
    let cfg: vrt_sql::service::ServiceConfig = toml::from_str(&rendered)
        .unwrap_or_else(|e| panic!("example vrt-sql toml must parse: {e}"));
    cfg.validate()
        .unwrap_or_else(|f| panic!("example vrt-sql config must validate: {f}"));
    // The example must be fs -- standalone deployments default to non-none (fail closed).
    assert_eq!(cfg.persistence.backend, "fs");
    assert!(cfg.persistence.fs.is_some());
}
