//! G1 PoC5/PoC6 — egress bypass matrix + same-host cross-tenant
//! isolation, on REAL workerd (no probe-skip). Probes batch inside
//! the tenant worker so each test is ~2 host→workerd roundtrips.
//!
//! Verified against workerd 1.20260916.1 + the real dist `vrt-egress`:
//! `x-vrt-egress-denied` carries the typed code (not a boolean); the
//! port check runs BEFORE the IP check; WHATWG URL normalization folds
//! decimal/octal/hex/short IPv4 into the quad form; `ftp:` is rejected
//! inside workerd's fetch API; URL userinfo is stripped before the
//! egress sees the request.
//!
//!   VRT_WORKERD_BIN=/path/to/workerd cargo test -p vrt-host \
//!       --test isolation_gate_egress -- --nocapture

mod common;
use common::g1::*;
use serde_json::json;
use vrt_config::{EgressAllowEntry, EgressPolicy};

/// PoC 5 — egress bypass matrix on fetch() and connect().
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc5_egress_bypass_matrix() {
    let bin = workerd_bin();
    let dir = gate_root("poc5");
    let _guard = DirGuard(dir.clone());
    // Internal server on a REAL open port — "the port was closed" can
    // never explain a deny.
    let (ctrl_port, _ctrl) = mini_http_spawn();

    let (cfg, ports, args, text) = render(
        &dir,
        &[("wp", "probe", MOD_PROBE, json!([]))],
        EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: vec![EgressAllowEntry {
                host: "example.com".into(),
                port: 443,
            }],
            deny_names: Vec::new(),
            request_timeout_ms: 8_000,
            ..Default::default()
        },
    );
    assert!(
        text.contains("vrt-egress"),
        "missing egress worker:\n{text}"
    );
    assert!(
        text.contains("deny = [\"private\", \"local\", \"unix\", \"unix-abstract\"]"),
        "outbound-network deny list missing:\n{text}"
    );

    let proc = spawn(&bin, &cfg, &dir, &args);
    let port = ports["wp"];
    let (st, _) = http(port, "/");
    assert_eq!(st, 200, "ingress not up: {}", proc.log());

    let batch = |kind: &str, ts: &[String]| -> Vec<serde_json::Value> {
        let joined = ts.iter().map(|t| urlenc(t)).collect::<Vec<_>>().join("%1f");
        let (st, body) = http(port, &format!("/batch?kind={kind}&ts={joined}"));
        assert_eq!(st, 200, "batch {kind} failed: {body} {}", proc.log());
        serde_json::from_str::<serde_json::Value>(&body)
            .unwrap_or_else(|_| panic!("bad batch json: {body}"))["results"]
            .as_array()
            .unwrap()
            .clone()
    };

    // Code column = `x-vrt-egress-denied` value (the header IS the
    // typed code). Port-80 URLs reach the IP check; explicit high
    // ports stop at the port check first — both are layer-3 denials.
    let cases: &[(&str, &str)] = &[
        ("http://127.0.0.1/", "ip_loopback"),
        ("http://localhost/", "denied_name"),
        ("http://[::1]/", "ip_loopback"),
        ("http://0.0.0.0/", "ip_this_net"),
        ("http://169.254.169.254/", "ip_link_local"),
        ("http://169.254.169.254:443/", "ip_link_local"),
        ("http://127.1/", "ip_loopback"),
        ("http://2130706433/", "ip_loopback"),
        ("http://0177.0.0.1/", "ip_loopback"),
        ("http://0x7f000001/", "ip_loopback"),
        ("http://127.0.0.01/", "ip_loopback"),
        ("http://10.0.0.1/", "ip_private"),
        ("http://172.16.0.1/", "ip_private"),
        ("http://192.168.1.1/", "ip_private"),
        ("http://100.64.0.1/", "ip_cgnat"),
        ("http://203.0.113.1/", "ip_reserved"),
        ("http://[::ffff:127.0.0.1]/", "ip_embedded"),
        ("http://[::ffff:7f00:1]/", "ip_embedded"),
        ("http://[64:ff9b::7f00:1]:443/", "ip_embedded"),
        ("http://[fd00::1]:443/", "ip_ula"),
        ("http://[fe80::1]:443/", "ip_link_local"),
        ("http://example.com:22/", "port"),
        ("http://example.com:8080/", "port"),
        ("http://internal/", "unqualified"),
        // open-port loopback — denied at the port check (precedence)
        (&format!("http://127.0.0.1:{ctrl_port}/"), "port"),
        (&format!("http://localhost:{ctrl_port}/"), "port"),
        (&format!("http://[::1]:{ctrl_port}/"), "port"),
        (&format!("http://0.0.0.0:{ctrl_port}/"), "port"),
        (&format!("http://[::ffff:127.0.0.1]:{ctrl_port}/"), "port"),
        // scheme/userinfo/positive control — asserted below
        ("ftp://example.com/", "threw"),
        ("http://user:pw@example.com/", "userinfo-stripped"),
        ("https://example.com/", "pass"),
    ];
    let targets: Vec<String> = cases.iter().map(|(t, _)| t.to_string()).collect();
    let results = batch("fetch", &targets);
    for ((t, want), r) in cases.iter().zip(results.iter()) {
        eprintln!("poc5 fetch {t} -> {r}");
        match *want {
            "threw" => assert_eq!(
                r["threw"].as_bool(),
                Some(true),
                "ftp: must throw tenant-side, not reach egress: {r}"
            ),
            "userinfo-stripped" => assert!(
                r["denied"].is_null(),
                "userinfo URL must NOT produce a layer-3 deny — info stripped: {r}"
            ),
            "pass" => assert!(
                r["denied"].is_null(),
                "allowlisted control hit layer-3 deny — denies are not per-target: {r}"
            ),
            code => {
                assert_eq!(r["status"].as_u64().unwrap_or(0), 403, "{t} not 403: {r}");
                assert_eq!(
                    r["denied"].as_str().unwrap_or(""),
                    code,
                    "{t} denied-code mismatch: {r}"
                );
                let body_code = r["body"]
                    .as_str()
                    .and_then(|b| serde_json::from_str::<serde_json::Value>(b).ok())
                    .and_then(|v| v["code"].as_str().map(str::to_string))
                    .unwrap_or_default();
                assert_eq!(body_code, code, "{t} body code: {r}");
                assert!(
                    !r["body"].as_str().unwrap_or("").contains("INT-HIT"),
                    "{t} reached internal server: {r}"
                );
            }
        }
    }

    // connect(): raw sockets route through the egress worker's
    // connect() handler — invocation verified on this build by a
    // logging-marker experiment (facade throws
    // `vrt egress denies connect()`, severing the socket; workerd
    // neither propagates the message nor logs it, so in-test
    // attribution = a KNOWN-OPEN port failing + uniform denial of
    // public targets + the mechanism experiment).
    let ctargets: Vec<String> = [
        format!("127.0.0.1:{ctrl_port}"),
        "127.0.0.1:1".into(),
        "8.8.8.8:53".into(),
        "[::1]:80".into(),
        "10.0.0.1:443".into(),
        "example.com:443".into(), // allowlisted for fetch — connect still denied
    ]
    .into();
    for r in batch("connect", &ctargets) {
        eprintln!("poc5 connect {} -> {r}", r["t"]);
        assert_eq!(
            r["opened"].as_bool(),
            Some(false),
            "connect {} reached a socket — egress bypass!: {r}",
            r["t"]
        );
    }
    eprintln!(
        "poc5 done: 24 typed deny codes + port precedence + ftp/userinfo + 6 connect denials"
    );
}

/// PoC 6 — two tenants in ONE workerd process: tenant A must not
/// reach tenant B's ingress socket, nor management/provider/cache/
/// cron listeners, nor any loopback port — via fetch or connect.
#[test]
#[ignore = "requires VRT_WORKERD_BIN"]
fn g1_poc6_same_process_cross_tenant() {
    let bin = workerd_bin();
    let dir = gate_root("poc6");
    let _guard = DirGuard(dir.clone());
    // Real OPEN loopback endpoints standing in for B's ingress,
    // management, provider, cache and sock-cron listeners — all
    // answer INT-HIT if actually reached, which must never happen.
    let (p_b_ingress, _1) = mini_http_spawn();
    let (p_mgmt, _2) = mini_http_spawn();
    let (p_provider, _3) = mini_http_spawn();
    let (p_cache, _4) = mini_http_spawn();
    let (p_cron, _5) = mini_http_spawn();
    let closed = free_port();

    let (cfg, ports, args, _text) = render(
        &dir,
        &[
            ("wp", "probe", MOD_PROBE, json!([])),
            (
                "wb",
                "victim",
                r#"export default { fetch() { return new Response("B-CANARY"); } }"#,
                json!([]),
            ),
        ],
        EgressPolicy {
            allowed_ports: vec![80, 443],
            allow: Vec::new(),
            deny_names: Vec::new(),
            request_timeout_ms: 8_000,
            ..Default::default()
        },
    );
    let proc = spawn(&bin, &cfg, &dir, &args);
    for id in ["wp", "wb"] {
        let (st, _) = http(ports[id], "/");
        assert_eq!(st, 200, "{id} ingress not up: {}", proc.log());
    }
    let wp = ports["wp"];
    let batch = |kind: &str, ts: &[String]| -> Vec<serde_json::Value> {
        let joined = ts.iter().map(|t| urlenc(t)).collect::<Vec<_>>().join("%1f");
        let (st, body) = http(wp, &format!("/batch?kind={kind}&ts={joined}"));
        assert_eq!(st, 200, "batch {kind} failed: {body}");
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["results"]
            .as_array()
            .unwrap()
            .clone()
    };

    let sockets = [
        format!("127.0.0.1:{p_b_ingress}"),
        format!("127.0.0.1:{}", ports["wb"]), // B's REAL ingress port
        format!("127.0.0.1:{p_mgmt}"),
        format!("127.0.0.1:{p_provider}"),
        format!("127.0.0.1:{p_cache}"),
        format!("127.0.0.1:{p_cron}"),
        format!("127.0.0.1:{closed}"),
        format!("[::1]:{p_b_ingress}"),
        format!("localhost:{p_b_ingress}"),
        format!("0.0.0.0:{p_b_ingress}"),
    ];
    let ftargets: Vec<String> = sockets
        .iter()
        .flat_map(|t| [format!("http://{t}/"), format!("https://{t}/")])
        .collect();
    for r in batch("fetch", &ftargets) {
        eprintln!(
            "poc6 fetch {} -> denied={:?} status={}",
            r["t"], r["denied"], r["status"]
        );
        assert_eq!(r["status"].as_u64().unwrap_or(0), 403, "{}: {r}", r["t"]);
        assert!(
            r["denied"].as_str().is_some_and(|c| !c.is_empty()),
            "{} not denied at egress layer: {r}",
            r["t"]
        );
        let body = r["body"].as_str().unwrap_or("");
        assert!(
            !body.contains("INT-HIT") && !body.contains("B-CANARY"),
            "{} LEAKED internal data: {r}",
            r["t"]
        );
    }
    for r in batch("connect", &sockets) {
        eprintln!("poc6 connect {} -> {r}", r["t"]);
        assert_eq!(
            r["opened"].as_bool(),
            Some(false),
            "connect {} bypassed egress: {r}",
            r["t"]
        );
    }
    // unix-path connect — not expressible as host:port; must fail.
    for r in batch("connect", &["/tmp/evil.sock".to_string()]) {
        assert_eq!(
            r["opened"].as_bool(),
            Some(false),
            "unix-path connect did not fail: {r}"
        );
    }
    eprintln!("poc6 done: 20 fetch + 11 connect denials, zero INT-HIT/B-CANARY leakage");
}
