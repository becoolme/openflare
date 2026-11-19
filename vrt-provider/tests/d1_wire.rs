//! T34: provider→vrt-sql 签名 wire 兼容测试——provider 的
//! `SqlAuthSigner` 必须被 vrt-sql 的 `ServiceAuth::verify` 接受。

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
    // seq 与实际上送不一致 → 拒绝（签名覆盖 seq）。
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
    // T34b: 重启后新 client 的 seq 必须高于旧 client 已发出的值
    // （epoch 固定 1，回绕会撞 _vrt_dedup）。间隔 >1ms 使纳秒种子必然前移。
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
    // 进程内严格递增。
    assert!(b.next_seq() > sb);
}
