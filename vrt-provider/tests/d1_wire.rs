//! T34: provider->vrt-sql signing wire compatibility test -- the provider's
//! `SqlAuthSigner` must be accepted by vrt-sql's `ServiceAuth::verify`.

use axum::http::{HeaderMap, HeaderValue, Method};
use vrt_provider::d1::{
    D1Client, SqlAuthSigner, SQL_AUTH_HEADER, SQL_EPOCH_HEADER, SQL_SEQ_HEADER,
};

#[test]
fn signer_output_verifies_against_vrt_sql() {
    let secret = b"shared-secret".to_vec();
    let signer = SqlAuthSigner::new(secret.clone());
    let verifier = vrt_sql::service::ServiceAuth::new(secret);
    let body = br#"{"sql":"select 1","params":[]}"#;
    let pq = "/dbs/abcdefghijklmnopqrstuvwxyz/query?resultsFormat=NONE";
    let token = signer.sign("POST", pq, body, 1, 42);
    let mut h = HeaderMap::new();
    h.insert(SQL_AUTH_HEADER, HeaderValue::from_str(&token).unwrap());
    h.insert(SQL_EPOCH_HEADER, HeaderValue::from_static("1"));
    h.insert(SQL_SEQ_HEADER, HeaderValue::from_static("42"));
    assert!(verifier.verify(&h, &Method::POST, pq, body));
    // seq differs from what was actually sent -> rejected (the signature covers seq).
    h.insert(SQL_SEQ_HEADER, HeaderValue::from_static("43"));
    assert!(!verifier.verify(&h, &Method::POST, pq, body));
}

#[test]
fn precheck_bounds() {
    let ok = serde_json::json!([{"sql": "select ?", "params": [1]}]).to_string();
    assert!(D1Client::precheck(ok.as_bytes()).is_ok());
    let long =
        serde_json::json!({"sql": "x".repeat(vrt_provider::d1::D1_MAX_SQL_BYTES + 1)}).to_string();
    assert!(D1Client::precheck(long.as_bytes()).is_err());
    let many = serde_json::json!({"sql": "select 1", "params": vec![0; vrt_provider::d1::D1_MAX_PARAMS + 1]}).to_string();
    assert!(D1Client::precheck(many.as_bytes()).is_err());
    assert!(D1Client::precheck(b"not-json").is_err());
}

#[test]
fn client_addr_validated() {
    assert!(D1Client::new(
        "http://127.0.0.1:9".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1)
    )
    .is_ok());
    assert!(D1Client::new(
        "unix:/tmp/x".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1)
    )
    .is_err());
    assert!(D1Client::new(
        "http://x/".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1)
    )
    .is_err());
}

#[test]
fn seq_monotonic_across_client_restart() {
    // T34b: after a restart the new client's seq must be higher than any value the old client already issued
    // (epoch fixed at 1, wrapping back would collide with _vrt_dedup). A gap >1ms guarantees the nanosecond seed moves forward.
    let a = D1Client::new(
        "http://127.0.0.1:9".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1),
    )
    .unwrap();
    let sa = a.next_seq();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let b = D1Client::new(
        "http://127.0.0.1:9".into(),
        b"s".to_vec(),
        std::time::Duration::from_secs(1),
    )
    .unwrap();
    let sb = b.next_seq();
    assert!(sb > sa, "restart seq regressed: {sa} -> {sb}");
    // Strictly increasing within the process.
    assert!(b.next_seq() > sb);
}
