//! RV-R72: startup config bounds + endpoint protocol rules.
use vrt_provider::config::ProviderConfig;

const BASE: &str = r#"
cellId = "cell-1"
listenerDir = "/tmp/vrt-rv72-listeners"
[desired]
path = "/tmp/vrt-rv72-envelope.json"
trustedKeys = [
  { keyId = "k", publicKey = "MCowBQYDK2VwAyEAO4cbNjsO7FQzisOvZshKKMa67lIim3ikig7RdntOAZM=" },
]
"#;

fn parse(top: &str, tables: &str) -> ProviderConfig {
    // top-level keys must precede the first [table]
    let (head, tail) = BASE.split_once("[desired]").unwrap();
    let text = format!("{head}\n{top}\n[desired]{tail}\n{tables}");
    toml::from_str(&text).unwrap_or_else(|e| panic!("parse: {e}"))
}

fn err(top: &str, tables: &str) -> String {
    parse(top, tables).validate().unwrap_err().to_string()
}

const S3: &str = "region = \"r\"\nbucket = \"b\"\naccessKeyEnv = \"A\"\nsecretKeyEnv = \"S\"\n";

#[test]
fn baseline_is_valid() {
    parse("", "").validate().unwrap();
}

#[test]
fn timeouts_outside_100ms_to_300s_are_rejected_with_field_name() {
    for (top, field) in [
        ("operationTimeoutMs = 99", "operationTimeoutMs"),
        ("operationTimeoutMs = 0", "operationTimeoutMs"),
        ("bodyReadTimeoutMs = 300001", "bodyReadTimeoutMs"),
        ("drainTimeoutMs = 0", "drainTimeoutMs"),
    ] {
        let e = err(top, "");
        assert!(e.contains(field) && e.contains("[100, 300_000]"), "{top}: {e}");
    }
    for (tables, field) in [
        (format!("[s3]\nendpoint = \"https://s3.example.com\"\n{S3}timeoutMs = 50"), "s3.timeoutMs"),
        ("[kvrocks]\nurlEnv = \"U\"\ntimeoutMs = 400000".to_string(), "kvrocks.timeoutMs"),
        ("[kvrocks]\nurlEnv = \"U\"\nconnectTimeoutMs = 1".to_string(), "kvrocks.connectTimeoutMs"),
        ("[vrtSql]\naddr = \"http://vrt-sql:8082\"\nauthSecretFile = \"/tmp/x\"\ntimeoutMs = 0".to_string(), "vrtSql.timeoutMs"),
    ] {
        let e = err("", &tables);
        assert!(e.contains(field), "{tables}: {e}");
    }
}

#[test]
fn concurrency_must_be_positive_and_tenant_not_above_global() {
    assert!(err("tenantConcurrency = 0", "").contains("tenantConcurrency"));
    assert!(err("globalConcurrency = 0", "").contains("globalConcurrency"));
    assert!(err("tenantConcurrency = 100\nglobalConcurrency = 10", "").contains("<="));
    parse("tenantConcurrency = 10\nglobalConcurrency = 10", "")
        .validate()
        .unwrap();
}

#[test]
fn ttls_must_be_positive_and_bounded() {
    let s3 = |ttl: i64| format!("[s3]\nendpoint = \"https://s3.example.com\"\n{S3}multipartTtlMs = {ttl}");
    assert!(err("", &s3(0)).contains("s3.multipartTtlMs"));
    assert!(err("", &s3(-5)).contains("s3.multipartTtlMs"));
    assert!(err("", &s3(31 * 24 * 3600 * 1000)).contains("s3.multipartTtlMs"));
    parse("", &s3(604_800_000)).validate().unwrap();
    let r2 = |ttl: i64| format!("[r2]\ndir = \"/tmp/r2\"\nstagingTtlMs = {ttl}");
    assert!(err("", &r2(0)).contains("r2.stagingTtlMs"));
    assert!(err("", &r2(i64::MAX)).contains("r2.stagingTtlMs"));
}

#[test]
fn s3_endpoint_requires_https_unless_loopback_or_dev_switch() {
    let s3 = |ep: &str| format!("[s3]\nendpoint = \"{ep}\"\n{S3}");
    parse("", &s3("https://s3.example.com")).validate().unwrap();
    parse("", &s3("http://127.0.0.1:9000")).validate().unwrap();
    parse("", &s3("http://localhost:9000")).validate().unwrap();
    parse("", &s3("http://[::1]:9000")).validate().unwrap();
    for bad in ["http://s3.example.com", "http://203.0.113.9:9000", "http://localhost.evil.com", "s3.example.com"] {
        let e = err("", &s3(bad));
        assert!(e.contains("s3.endpoint"), "{bad}: {e}");
    }
    // Explicit dev switch re-allows plain http.
    parse("allowDevBackend = true", &s3("http://s3.example.com"))
        .validate()
        .unwrap();
}

#[test]
fn vrt_sql_addr_allows_cluster_http_but_not_public_http() {
    let sql = |a: &str| format!("[vrtSql]\naddr = \"{a}\"\nauthSecretFile = \"/tmp/x\"");
    for ok in ["http://vrt-sql:8082", "http://127.0.0.1:8090", "http://vrt-sql.ns.svc.cluster.local:8082"] {
        parse("", &sql(ok)).validate().unwrap();
    }
    let e = err("", &sql("http://sql.example.com:8082"));
    assert!(e.contains("vrtSql.addr"), "{e}");
    parse("allowDevBackend = true", &sql("http://sql.example.com:8082"))
        .validate()
        .unwrap();
}
