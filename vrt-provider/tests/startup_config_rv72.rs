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
    // In-cluster Service FQDN with explicit port (in-cluster S3, e.g. silo).
    parse("", &s3("http://vrt-r2-silo.vibeploy-data.svc:9000")).validate().unwrap();
    for bad in ["http://vrt-r2-silo:9000", "http://vrt-r2-silo.vibeploy-data.svc", "http://10.43.0.10:9000"] {
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

#[test]
fn vrt_sql_replicas_are_optional_but_validated() {
    let sql = |replicas: &str| {
        format!(
            "[vrtSql]\naddr = \"http://vrt-sql:8082\"\nauthSecretFile = \"/tmp/x\"\nreplicas = [{replicas}]"
        )
    };
    parse("", &sql("")).validate().unwrap();
    parse("", &sql("\"http://vrt-sql-replica-0:8082\", \"http://127.0.0.1:8091\""))
        .validate()
        .unwrap();
    for bad in [
        "\"http://sql.example.com:8082\"",
        "\"http://vrt-sql-replica-0:8082/\"",
        "\"\"",
    ] {
        let e = err("", &sql(bad));
        assert!(e.contains("vrtSql.replicas"), "{bad}: {e}");
    }
    let many = (0..9)
        .map(|i| format!("\"http://replica-{i}:8082\""))
        .collect::<Vec<_>>()
        .join(", ");
    assert!(err("", &sql(&many)).contains("vrtSql.replicas"));
    parse("allowDevBackend = true", &sql("\"http://sql.example.com:8082\""))
        .validate()
        .unwrap();
}

// ---------------------------------------------- MIG-15 r3 session pool

/// `[pg]` + `[pgProxy]` + `[hyperdrive.pool]` with `body` inside the
/// pool table — the shape a standalone `vrt-provider.toml` uses.
fn pool_toml(body: &str) -> String {
    format!(
        "[pg]\ncredentialsFile = \"/tmp/pg-credentials.json\"\n\n\
         [pgProxy]\nlisten = \"127.0.0.1:19543\"\n\n\
         [hyperdrive.pool]\n{body}"
    )
}

#[test]
fn hyperdrive_pool_defaults_are_bounded_and_valid() {
    let cfg = parse("", &pool_toml(""));
    cfg.validate().unwrap();
    let pool = cfg.hyperdrive.expect("pool configured").pool;
    assert_eq!(pool.max_connections, 8);
    assert_eq!(pool.acquire_timeout_ms, 5_000);
    assert_eq!(pool.idle_timeout_ms, 30_000);
    assert_eq!(pool.max_lifetime_ms, 1_800_000);
    assert!(pool.reset_on_return);

    parse(
        "",
        &pool_toml(
            "maxConnections = 1024\nacquireTimeoutMs = 100\nidleTimeoutMs = 100\n\
             maxLifetimeMs = 2592000000\nresetOnReturn = false",
        ),
    )
    .validate()
    .unwrap();
}

#[test]
fn hyperdrive_pool_rejects_out_of_range_values() {
    for (body, field) in [
        ("maxConnections = 0", "hyperdrive.pool.maxConnections"),
        ("maxConnections = 1025", "hyperdrive.pool.maxConnections"),
        ("acquireTimeoutMs = 0", "hyperdrive.pool.acquireTimeoutMs"),
        ("acquireTimeoutMs = 99", "hyperdrive.pool.acquireTimeoutMs"),
        (
            "acquireTimeoutMs = 300001",
            "hyperdrive.pool.acquireTimeoutMs",
        ),
        ("idleTimeoutMs = 0", "hyperdrive.pool.idleTimeoutMs"),
        ("maxLifetimeMs = 0", "hyperdrive.pool.maxLifetimeMs"),
        ("maxLifetimeMs = 999", "hyperdrive.pool.maxLifetimeMs"),
        (
            "maxLifetimeMs = 2592000001",
            "hyperdrive.pool.maxLifetimeMs",
        ),
    ] {
        let e = err("", &pool_toml(body));
        assert!(e.contains(field), "{body}: {e}");
    }
    // Negative values are not representable: the config fails to parse
    // rather than wrapping around.
    for body in [
        "maxConnections = -1",
        "acquireTimeoutMs = -5",
        "maxLifetimeMs = -1",
    ] {
        assert!(
            toml::from_str::<vrt_provider::config::ProviderConfig>(&pool_toml(body)).is_err(),
            "{body} must not parse"
        );
    }
}

#[test]
fn hyperdrive_pool_rejects_unknown_keys_and_orphan_sections() {
    // Unknown pool key (e.g. a transaction-pooling knob that does not
    // exist) fails closed at parse time.
    for body in [
        "maxSessions = 8",
        "minConnections = 1",
        "connectionTimeoutMs = 100",
    ] {
        assert!(
            toml::from_str::<vrt_provider::config::ProviderConfig>(&pool_toml(body)).is_err(),
            "{body} must not parse"
        );
    }
    // `[hyperdrive]` with no pool table is a configuration error, not a
    // silent no-op.
    let orphan = "[pg]\ncredentialsFile = \"/tmp/c.json\"\n[pgProxy]\nlisten = \"127.0.0.1:19543\"\n[hyperdrive]\n";
    assert!(toml::from_str::<vrt_provider::config::ProviderConfig>(orphan).is_err());
}

#[test]
fn hyperdrive_pool_requires_the_proxy_and_its_credentials() {
    let no_proxy =
        "[pg]\ncredentialsFile = \"/tmp/c.json\"\n[hyperdrive.pool]\nmaxConnections = 4\n";
    let cfg = parse("", no_proxy);
    let e = cfg.validate().unwrap_err().to_string();
    assert!(e.contains("hyperdrive requires pgProxy"), "{e}");

    let no_pg = "[pgProxy]\nlisten = \"127.0.0.1:19543\"\n[hyperdrive.pool]\nmaxConnections = 4\n";
    let e = parse("", no_pg).validate().unwrap_err().to_string();
    assert!(e.contains("pgProxy requires pg"), "{e}");
}
