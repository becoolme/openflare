//! RV-R72：`backend="none"` 与 s3 http endpoint 的 fail-closed 门禁。

fn cfg(persistence: &str) -> vrt_sql::service::ServiceConfig {
    let raw = format!(
        r#"
dbRoot = "/tmp/vrt-sql-rv72"
listenTcp = "127.0.0.1:8090"
bookmarkSecretFile = "/tmp/b"
authSecretFile = "/tmp/a"
[persistence]
{persistence}
"#
    );
    toml::from_str(&raw).unwrap_or_else(|e| panic!("parse: {e}"))
}

#[test]
fn backend_none_requires_explicit_dev_switch() {
    let denied = cfg(r#"backend = "none""#).validate().unwrap_err();
    assert!(denied.contains("allowDevBackend"), "{denied}");
    assert!(cfg("backend = \"none\"\nallowDevBackend = false")
        .validate()
        .is_err());
    assert!(cfg("backend = \"none\"\nallowDevBackend = true")
        .validate()
        .is_ok());
}

#[test]
fn s3_endpoint_must_be_https_unless_loopback_or_dev() {
    let s3 = |ep: &str, extra: &str| {
        cfg(&format!(
            "backend = \"s3\"\n{extra}\n[persistence.s3]\nendpoint = \"{ep}\"\nregion = \"r\"\nbucket = \"b\"\naccessKeyFile = \"/tmp/ak\"\nsecretKeyFile = \"/tmp/sk\"\n"
        ))
    };
    assert!(s3("https://s3.example.com", "").validate().is_ok());
    assert!(s3("http://127.0.0.1:9000", "").validate().is_ok());
    assert!(s3("http://s3.example.com", "").validate().is_err());
    assert!(s3("http://s3.example.com", "allowDevBackend = true")
        .validate()
        .is_ok());
}
