//! Ingress tests — host normalization, route-table-driven dispatch,
//! forged internal header stripping, typed errors, body cap, and a
//! real supervisor+stub worker behind the proxy.

mod common;

use common::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use vrt_host::ingress::IngressState;
use vrt_host::route::RouteTable;
use vrt_host::state::TenantState;
use vrt_host::supervisor::{SupervisorCmd, SupervisorHandle, TenantRegistry, TenantRuntime};

fn desired_with_routes() -> snapshot_protocol::compute_desired::ComputeDesired {
    snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![
            tenant("t1", vec![worker("w1", "App.Example.com", "d1")]),
            tenant("t2", vec![worker("w2", "b.example.com", "d2")]),
        ],
        deletions: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_normalization_and_exact_routing() {
    let registry = TenantRegistry::new();
    let rt1 = TenantRuntime::new("t1");
    let rt2 = TenantRuntime::new("t2");
    registry.insert("t1".to_string(), rt1.clone());
    registry.insert("t2".to_string(), rt2.clone());
    // t1 ready behind a real stub; t2 ready pointing at a dead port.
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt1, spec);
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(8)).await;
    let dead: std::net::SocketAddr = "127.0.0.1:9".parse().unwrap(); // discard port, closed
    rt2.test_install(BTreeMap::from([(
        "w2".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(dead),
    )]));

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // Exact host, lowercase+dotted.
    let (status, _, body) = http_get(addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 200);
    assert!(body.contains("stub ok"));

    // Case + port normalization.
    let (status, _, _) = http_get(addr, "APP.Example.COM:8443", "/", &[]).await;
    assert_eq!(status, 200);

    // Unknown host → 404 typed.
    let (status, _, body) = http_get(addr, "unknown.test", "/", &[]).await;
    assert_eq!(status, 404);
    assert!(body.contains("unknown_host"));

    // No fuzzy/fallback: `sub.app.example.com` must NOT hit app.example.com.
    let (status, _, _) = http_get(addr, "sub.app.example.com", "/", &[]).await;
    assert_eq!(status, 404);

    // Upstream connect failure → 502 typed.
    let (status, _, body) = http_get(addr, "b.example.com", "/", &[]).await;
    assert_eq!(status, 502);
    assert!(body.contains("upstream_connect"));

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_internal_headers_are_stripped_and_rewritten() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // The stub echoes every `x-*` request header it sees — forged values
    // must never reach it; our rewritten identity headers must.
    let (status, _, body) = http_get(
        addr,
        "app.example.com",
        "/",
        &[
            ("x-vrt-tenant-id", "evil-tenant"),
            ("x-vrt-worker-id", "evil-worker"),
            ("x-open-compute-worker-id", "spoofed"),
            ("x-custom", "kept"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("hdr x-vrt-tenant-id=t1"), "body: {body}");
    assert!(body.contains("hdr x-vrt-worker-id=w1"), "body: {body}");
    assert!(
        !body.contains("evil"),
        "forged header reached worker: {body}"
    );
    assert!(!body.contains("spoofed"), "body: {body}");
    assert!(body.contains("hdr x-custom=kept"), "body: {body}");

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// MIG-7: `request.url`'s scheme comes from workerd's
/// `forwardedProtoHeader` = the `x-forwarded-proto` value vrt-host
/// writes — only the trusted-source policy may promote it to https.
/// The stub echoes `x-*` headers back as `hdr <name>=<value>` lines.
async fn proto_fixture(
    policy: vrt_host::ingress::ForwardedProto,
) -> (IngressGuard, Arc<TenantRuntime>, SupervisorHandle) {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    )
    .with_forwarded_proto(policy);
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    (ig, rt, handle)
}

async fn proto_teardown(ig: IngressGuard, rt: Arc<TenantRuntime>, handle: SupervisorHandle) {
    drop(ig);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

// ---- MIG-9: typed upstream client-IP trust (`cf-connecting-ip`) ----

/// Same shape as `proto_fixture`, but installs a [`ClientIpPolicy`].
/// The test peer is always loopback — `127.0.0.0/8` in `trusted`
/// means "trusted", any other range means "untrusted".
async fn client_ip_fixture(
    policy: vrt_host::ingress::ClientIpPolicy,
) -> (IngressGuard, Arc<TenantRuntime>, SupervisorHandle) {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    )
    .with_client_ip(policy);
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    (ig, rt, handle)
}

fn cidr(text: &str) -> vrt_config::Cidr {
    vrt_config::Cidr::parse(text).unwrap()
}

/// The Cloudflare-mode policy used by most cases: loopback peer
/// trusted; CF edge ranges = the two real published shapes.
fn cf_policy() -> vrt_host::ingress::ClientIpPolicy {
    vrt_host::ingress::ClientIpPolicy::Cloudflare {
        trusted: vec![cidr("127.0.0.0/8")],
        cloudflare: vec![cidr("172.64.0.0/13"), cidr("2400:cb00::/32")],
    }
}

/// `cf-connecting-ip` the worker echoed back (the stub emits `cf-*`
/// headers as `hdr <name>=<value>` lines), or None when absent.
fn echoed_client_ip(body: &str) -> Option<String> {
    body.lines()
        .find_map(|l| l.strip_prefix("hdr cf-connecting-ip=").map(str::to_string))
}

/// Trusted peer + CF-shaped XFF + `cf-connecting-ip` → the header
/// value is the client IP the worker sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_cloudflare_trusted_cf_hop_honors_header() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147"),
            ("cf-connecting-ip", "203.0.113.7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(echoed_client_ip(&body).as_deref(), Some("203.0.113.7"), "body: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// `true-client-ip` is honored when `cf-connecting-ip` is absent
/// (Enterprise alternate header), still under the CF-adjacent gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_cloudflare_true_client_ip_fallback() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147"),
            ("true-client-ip", "198.51.100.9"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(echoed_client_ip(&body).as_deref(), Some("198.51.100.9"), "body: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// Trusted peer, but the adjacent XFF hop is NOT a Cloudflare edge —
/// a direct client or non-CF front. The upstream `cf-connecting-ip`
/// is never believed; the injected value is the XFF-derived rightmost
/// untrusted hop (the non-CF edge itself).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_cloudflare_non_cf_hop_never_honors_header() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 8.8.8.8"),
            ("cf-connecting-ip", "203.0.113.7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        echoed_client_ip(&body).as_deref(),
        Some("8.8.8.8"),
        "must inject the derived non-CF edge, not the forged header: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

/// An UNTRUSTED TCP peer can never inject anything — even a perfectly
/// CF-shaped chain with a `cf-connecting-ip`. The peer's own address
/// stays the only identity the worker sees (no header at all).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_untrusted_peer_cannot_forge_anything() {
    let (ig, rt, handle) = client_ip_fixture(vrt_host::ingress::ClientIpPolicy::Cloudflare {
        trusted: vec![cidr("10.96.0.0/12")], // excludes the loopback test peer
        cloudflare: vec![cidr("172.64.0.0/13")],
    })
    .await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147"),
            ("cf-connecting-ip", "203.0.113.7"),
            ("x-real-ip", "203.0.113.7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        echoed_client_ip(&body).is_none(),
        "untrusted peer injected cf-connecting-ip: {body}"
    );
    assert!(
        !body.contains("203.0.113.7"),
        "forged identity leaked into any echo: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

/// A malformed `cf-connecting-ip` under a CF-adjacent chain is skipped
/// (counted via `vrt_host_client_ip_invalid_total`), never injected —
/// the derived edge IP is the fallback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_malformed_header_falls_back_to_derived() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147"),
            ("cf-connecting-ip", "definitely-not-an-ip"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        echoed_client_ip(&body).as_deref(),
        Some("172.70.80.147"),
        "malformed header must fall back to the derived edge IP: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

/// IPv6 end-to-end: CF v6 edge + v6 client address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_cloudflare_ipv6() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "2001:db8::7, 2400:cb00::1"),
            ("cf-connecting-ip", "2001:db8::7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(echoed_client_ip(&body).as_deref(), Some("2001:db8::7"), "body: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// IPv4-mapped IPv6 input is canonicalized — the worker sees the
/// plain v4 literal and v4 CIDR matching applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_ipv4_mapped_is_canonicalized() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "::ffff:203.0.113.7, 172.70.80.147"),
            ("cf-connecting-ip", "::ffff:203.0.113.7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        echoed_client_ip(&body).as_deref(),
        Some("203.0.113.7"),
        "mapped v6 must inject the canonical v4: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

/// A chain whose LAST entry is itself a trusted proxy (a second
/// internal hop appended after the CF edge) — the adjacent hop is
/// not CF, so the header is not believed; fallback skips the trusted
/// tail and lands on the CF edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_trailing_trusted_hop_defeats_cf_header() {
    let (ig, rt, handle) = client_ip_fixture(cf_policy()).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147, 127.0.0.2"),
            ("cf-connecting-ip", "203.0.113.7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        echoed_client_ip(&body).as_deref(),
        Some("172.70.80.147"),
        "trailing trusted hop → derived edge, never the header: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

/// `mode = "none"` — even a fully CF-shaped request injects nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_mode_none_never_injects() {
    let (ig, rt, handle) = client_ip_fixture(vrt_host::ingress::ClientIpPolicy::None).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147"),
            ("cf-connecting-ip", "203.0.113.7"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert!(echoed_client_ip(&body).is_none(), "mode=none must not inject: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// `mode = "trusted-xff"`: the client IP is the rightmost non-trusted
/// XFF hop — the upstream `cf-connecting-ip` is never honored (a
/// non-CF front can send arbitrary values).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_ip_trusted_xff_derives_and_ignores_cf_header() {
    let (ig, rt, handle) = client_ip_fixture(vrt_host::ingress::ClientIpPolicy::TrustedXff {
        trusted: vec![cidr("127.0.0.0/8")],
    })
    .await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-for", "203.0.113.7, 172.70.80.147"),
            ("cf-connecting-ip", "66.66.66.66"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        echoed_client_ip(&body).as_deref(),
        Some("172.70.80.147"),
        "trusted-xff derives from the chain, never the header: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

/// Trusted peer (loopback ∈ 127.0.0.0/8) reporting https reaches the
/// worker as https — the Traefik-terminated path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_proto_trusted_peer_reports_https() {
    let (ig, rt, handle) = proto_fixture(vrt_host::ingress::ForwardedProto::TrustedHeader {
        trusted: vec![vrt_config::Cidr::parse("127.0.0.0/8").unwrap()],
    })
    .await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[("x-forwarded-proto", "https")],
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("hdr x-forwarded-proto=https"), "body: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// The SAME header from a peer outside the trusted range stays http —
/// a forged `x-forwarded-proto: https` never promotes the scheme.
/// (Loopback is the only test peer, so "untrusted" = a CIDR that
/// excludes it.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_proto_untrusted_peer_cannot_forge_https() {
    let (ig, rt, handle) = proto_fixture(vrt_host::ingress::ForwardedProto::TrustedHeader {
        trusted: vec![vrt_config::Cidr::parse("10.96.0.0/12").unwrap()],
    })
    .await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[
            ("x-forwarded-proto", "https"),
            ("cf-visitor", "{\"scheme\":\"https\"}"),
            ("cf-connecting-ip", "203.0.113.9"),
            ("cf-ray", "8f8f8f-SJC"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("hdr x-forwarded-proto=http"), "body: {body}");
    // cf-* never passes through, whatever the peer.
    for forged in ["cf-visitor", "cf-connecting-ip", "cf-ray"] {
        assert!(
            !body.contains(&format!("hdr {forged}=")),
            "forged {forged} reached worker: {body}"
        );
    }
    proto_teardown(ig, rt, handle).await;
}

/// Trusted peer but a garbage/missing header value fails closed to
/// http — only the exact `https` token promotes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_proto_trusted_garbage_value_stays_http() {
    let (ig, rt, handle) = proto_fixture(vrt_host::ingress::ForwardedProto::TrustedHeader {
        trusted: vec![vrt_config::Cidr::parse("127.0.0.0/8").unwrap()],
    })
    .await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[("x-forwarded-proto", "https,http")],
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("hdr x-forwarded-proto=http"), "body: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// Missing policy — the default `fixed-http` never promotes, forged
/// header or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_proto_default_policy_is_http() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;

    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[("x-forwarded-proto", "https")],
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("hdr x-forwarded-proto=http"), "body: {body}");

    drop(ig);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// `fixed-https` — every request reports https regardless of inbound
/// headers (websecure-only deployments).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_proto_fixed_https_mode() {
    let (ig, rt, handle) = proto_fixture(vrt_host::ingress::ForwardedProto::FixedHttps).await;
    let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 200);
    assert!(body.contains("hdr x-forwarded-proto=https"), "body: {body}");
    proto_teardown(ig, rt, handle).await;
}

/// A trusted proxy's `x-forwarded-for` chain passes through; an
/// untrusted client's chain is replaced by the peer IP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_for_trusted_chain_only() {
    let (ig, rt, handle) = proto_fixture(vrt_host::ingress::ForwardedProto::TrustedHeader {
        trusted: vec![vrt_config::Cidr::parse("127.0.0.0/8").unwrap()],
    })
    .await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[("x-forwarded-for", "203.0.113.7, 10.1.2.3")],
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("hdr x-forwarded-for=203.0.113.7, 10.1.2.3"),
        "trusted chain must pass: {body}"
    );
    proto_teardown(ig, rt, handle).await;

    let (ig, rt, handle) = proto_fixture(vrt_host::ingress::ForwardedProto::FixedHttp).await;
    let (status, _, body) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[("x-forwarded-for", "203.0.113.7")],
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("hdr x-forwarded-for=127.0.0.1"),
        "untrusted chain must be replaced by peer: {body}"
    );
    proto_teardown(ig, rt, handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_state_maps_to_typed_errors() {
    let registry = TenantRegistry::new();
    let starting = TenantRuntime::new("t1");
    let circuit = TenantRuntime::new("t2");
    registry.insert("t1".to_string(), starting);
    registry.insert("t2".to_string(), circuit.clone());
    circuit.test_set_state(TenantState::CircuitOpen);

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        64, // tiny body cap
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // t1 registered but still Starting → 503 tenant_unavailable.
    let (status, _, body) = http_get(addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 503);
    assert!(body.contains("tenant_unavailable"), "body: {body}");

    // t2 circuit-open → 503 tenant_circuit_open.
    let (status, _, body) = http_get(addr, "b.example.com", "/", &[]).await;
    assert_eq!(status, 503);
    assert!(body.contains("tenant_circuit_open"), "body: {body}");
}

/// T37b-r2: a tenant latched `SwitchFailed` keeps serving the OLD
/// generation — matching `Ready` alone 503'd every sibling worker when
/// one bad spec latched (the standalone blast radius). Ingress must use
/// `can_serve()` (Ready | SwitchFailed); the tenant-state matrix keeps
/// 503s for genuinely unserving states.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_failed_tenant_keeps_serving_old_generation() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    // Real stub generation installs Ready, then force the latched state.
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    rt.test_set_state(TenantState::SwitchFailed);

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;

    // The old generation answers through ingress under SwitchFailed.
    let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 200, "SwitchFailed must serve old gen: {body}");
    assert!(body.contains("stub ok"), "{body}");

    drop(ig);
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_body_rejected_before_proxying() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        64,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"POST / HTTP/1.1\r\nhost: app.example.com\r\ncontent-length: 4096\r\nconnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    stream.write_all(&vec![b'x'; 256]).await.unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf);
    assert!(text.contains("413"), "response: {text}");
    assert!(text.contains("payload_too_large"), "response: {text}");

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_response_cannot_forge_internal_markers() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // The worker emits forged internal markers; the proxy must strip all
    // x-vrt-*/x-open-compute-* from the upstream response while keeping
    // ordinary headers.
    let (status, headers, _body) = http_get(
        addr,
        "app.example.com",
        "/",
        &[(
            "x-stub-emit-header",
            "x-vrt-error=forged,x-vrt-tenant-id=evil,x-open-compute-worker-id=spoof",
        )],
    )
    .await;
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
    assert_eq!(status, 200);
    let names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
    for forged in ["x-vrt-error", "x-vrt-tenant-id", "x-open-compute-worker-id"] {
        assert!(
            !names.contains(&forged),
            "forged marker leaked to client: {names:?}"
        );
    }
    assert!(names.contains(&"x-stub-worker"), "{names:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_panic_returns_typed_500_not_reset() {
    // Regression: a panic inside the handler must surface as a typed
    // 500 — a connection-task abort would show up as a bare TCP reset
    // (the same symptom a unix-socket client saw when the test harness
    // dropped its shutdown sender early).
    init_logs();
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    rt.test_install(BTreeMap::from([(
        "w1".to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp("127.0.0.1:9".parse().unwrap()),
    )]));
    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;

    state
        .panic_once
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (status, headers, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 500, "status/body: {status} {body}");
    assert!(body.contains("internal"), "typed body: {body}");
    assert!(
        headers
            .iter()
            .any(|(k, v)| k == "x-vrt-error" && v == "internal"),
        "x-vrt-error: {headers:?}"
    );

    // Injection consumed: the NEXT request follows normal typed paths
    // (Ready tenant, dead upstream → 502), proving the connection
    // stayed usable rather than the listener dying with the panic.
    let (status, _, body) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 502, "status/body: {status} {body}");
    assert!(body.contains("upstream_connect"), "body: {body}");

    // After a clean stop the serve task itself must have finished —
    // the deterministic "listener is gone" signal (a fresh connect's
    // errno depends on the kernel's backlog state, so don't probe that).
    assert!(ig.stop().await, "serve task did not finish after shutdown");
}

/// T25/C1: requests whose authority lives in the URI (h2 `:authority`
/// shape; here via h1 absolute-form) route identically to Host-header
/// requests — no Host is synthesized by hyper in that shape, so the
/// ingress falls back to `uri.authority()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uri_authority_routes_when_host_header_absent() {
    let registry = TenantRegistry::new();
    let rt1 = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt1.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt1, spec);
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;

    // Absolute-form request-target, NO Host header — authority only in
    // the URI (the same shape an h2 request has post-translation).
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(ig.addr)
        .await
        .expect("connect");
    stream
        .write_all(b"GET http://app.example.com/ HTTP/1.1\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b12_metrics_count_requests_and_stay_off_facade() {
    // Registry is process-global — unique worker/tenant ids keep the
    // series exact for this test.
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("tm");
    registry.insert("tm".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("tm", &["wmetrics"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant(
            "tm",
            vec![worker("wmetrics", "m.example.com", "dm")],
        )],
        deletions: vec![],
};
    state.set_routes(RouteTable::from_desired(&desired));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // Two proxied 200s, one routed 502 (dead upstream not needed —
    // an unrouted host produces no series at all).
    for _ in 0..2 {
        let (st, _, _) = http_get(addr, "m.example.com", "/", &[]).await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = http_get(addr, "unrouted.test", "/", &[]).await;
    assert_eq!(st, 404);

    // The public facade must NOT answer /metrics itself — a routed
    // request to /metrics is proxied to the worker like any path.
    let (st, _, body) = http_get(addr, "m.example.com", "/metrics", &[]).await;
    assert_eq!(st, 200);
    assert!(
        body.contains("stub ok"),
        "facade /metrics must proxy, got {body}"
    );
    assert!(!body.contains("vrt_worker_requests_total"));

    // The control-plane metrics router exposes the exposition text.
    let router = vrt_host::metrics::router(registry.clone());
    use tower::ServiceExt;
    let resp = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = String::from_utf8(
        axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    // 2 routed requests + the facade /metrics probe = 3 (the probe is
    // proxied to the worker like any path — counted, not exempted).
    assert!(
        text.contains("vrt_worker_requests_total{status_class=\"2xx\",worker_id=\"wmetrics\"} 3"),
        "request series missing: {text}"
    );
    assert!(
        text.contains("vrt_worker_request_duration_seconds_count{worker_id=\"wmetrics\"} 3"),
        "duration histogram missing: {text}"
    );
    assert!(
        text.contains("vrt_worker_response_bytes_total{worker_id=\"wmetrics\"}"),
        "response bytes missing: {text}"
    );
    assert!(
        !text.contains("unrouted.test"),
        "no request data may appear as a label: {text}"
    );
    assert!(
        text.contains("vrt_host_tenant_processes"),
        "instance metrics missing: {text}"
    );

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_asset_route_marker_never_relabels_worker_metrics() {
    // T31c: `x-vrt-asset-route` is honored only for routes that front a
    // `router-<id>` — a legacy worker emitting the header keeps Worker
    // metrics, and the marker never reaches the client.
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["wforge"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    // Route a distinct worker id so the process-global registry never
    // collides with other tests in this binary.
    let mut d = desired_with_routes();
    d.tenants[0].workers = vec![worker("wforge", "app.example.com", "d1")];
    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&d));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // The worker forges the asset marker; the route is worker-first so
    // the proxy must ignore it for metrics and strip it for the client.
    let (status, headers, _body) = http_get(
        addr,
        "app.example.com",
        "/",
        &[("x-stub-emit-header", "x-vrt-asset-route=assets")],
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        !headers.iter().any(|(k, _)| k == "x-vrt-asset-route"),
        "marker leaked to client: {headers:?}"
    );

    let router = vrt_host::metrics::router(registry.clone());
    let res = tower::ServiceExt::oneshot(
        router,
        axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let text = String::from_utf8(
        axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains("vrt_worker_requests_total{status_class=\"2xx\",worker_id=\"wforge\"} 1"),
        "worker metric missing: {text}"
    );
    assert!(
        !text.contains(
            "vrt_worker_assets_route_requests_total{status_class=\"2xx\",worker_id=\"wforge\"}"
        ),
        "forged marker relabeled metrics: {text}"
    );

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_first_route_counts_marker_as_asset_hit() {
    // Same forged header, but on a route that actually fronts a router:
    // the response counts as an asset hit, not a Worker invocation.
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["wassets"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let mut w = worker("wassets", "app.example.com", "d1");
    w.bindings
        .push(snapshot_protocol::compute_desired::BindingDesired::Assets(
            snapshot_protocol::compute_desired::AssetsBinding {
                name: "ASSETS".into(),
                manifest_digest: "b".repeat(64),
                permissions: snapshot_protocol::compute_desired::BindingPermissions {
                    read: true,
                    write: false,
                },
                assets_first: Some(true),
                run_worker_first: None,
                base_path: None,
            },
        ));
    let mut d = desired_with_routes();
    d.tenants[0].workers = vec![w];

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&d));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    let (status, _, _body) = http_get(
        addr,
        "app.example.com",
        "/",
        &[("x-stub-emit-header", "x-vrt-asset-route=assets")],
    )
    .await;
    assert_eq!(status, 200);

    let router = vrt_host::metrics::router(registry.clone());
    let res = tower::ServiceExt::oneshot(
        router,
        axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let text = String::from_utf8(
        axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains("vrt_worker_assets_route_requests_total{reason=\"none\",status_class=\"2xx\",worker_id=\"wassets\"}"),
        "assets-route metric missing: {text}"
    );
    assert!(
        !text.contains("vrt_worker_requests_total{status_class=\"2xx\",worker_id=\"wassets\"}"),
        "asset hit counted as worker request: {text}"
    );

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// T31f/F0.4: a router 503 carrying a bounded `x-vrt-route-error`
/// reason counts under the `reason` label of the assets-route metric.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_route_error_reason_lands_on_metric_label() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["wreason"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let mut w = worker("wreason", "app.example.com", "d1");
    w.bindings
        .push(snapshot_protocol::compute_desired::BindingDesired::Assets(
            snapshot_protocol::compute_desired::AssetsBinding {
                name: "ASSETS".into(),
                manifest_digest: "b".repeat(64),
                permissions: snapshot_protocol::compute_desired::BindingPermissions {
                    read: true,
                    write: false,
                },
                assets_first: Some(true),
                run_worker_first: None,
                base_path: None,
            },
        ));
    let mut d = desired_with_routes();
    d.tenants[0].workers = vec![w];

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&d));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // The stub worker emits both the route marker and the bounded
    // reason header — ingress maps the whitelisted value onto the
    // metric's `reason` label.
    let (status, headers, _body) = http_get(
        addr,
        "app.example.com",
        "/",
        &[(
            "x-stub-emit-header",
            "x-vrt-asset-route=assets,x-vrt-route-error=route_index_rpc_failed",
        )],
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        !headers.iter().any(|(k, _)| k == "x-vrt-route-error"),
        "reason header leaked to client: {headers:?}"
    );

    let router = vrt_host::metrics::router(registry.clone());
    let res = tower::ServiceExt::oneshot(
        router,
        axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let text = String::from_utf8(
        axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        text.contains(
            "vrt_worker_assets_route_requests_total{reason=\"route_index_rpc_failed\",status_class=\"2xx\",worker_id=\"wreason\"}"
        ),
        "reason label missing: {text}"
    );
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// W4-r2: during a serialized DO switch the gate holds requests
/// (bounded) instead of 503ing — a sibling worker's assets must not
/// die because one worker's class dirs are switching.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_hold_bridges_the_serialized_window() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    let endpoint = rt.endpoint_slot("w1").unwrap().endpoint().clone();

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // Open the switch window: routes emptied + gate active.
    rt.switch_gate.begin();
    rt.test_install(BTreeMap::new());

    // The request must be HELD (no instant 503) until routes return.
    let req = tokio::spawn(http_get(addr, "app.example.com", "/", &[]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!req.is_finished(), "request 503'd instead of holding");

    // Generation installed → gate clears → held request forwards.
    rt.test_install(BTreeMap::from([("w1".to_string(), endpoint)]));
    rt.switch_gate.end();
    let (status, _, _) = tokio::time::timeout(Duration::from_secs(5), req)
        .await
        .expect("held request never resolved")
        .unwrap();
    assert_eq!(status, 200);

    // Budget expiry → honest 503 (+Retry-After already on the type).
    rt.switch_gate.begin();
    rt.test_install(BTreeMap::new());
    let short = state.with_switch_hold(Duration::from_millis(150), 256, 1 << 20);
    let ig2 = start_ingress(&short).await;
    let (status, _, _) = http_get(ig2.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 503);
    rt.switch_gate.end();
    ig2.stop().await;

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// W4-r3 P0-1: a held request dropped mid-wait (client disconnect /
/// hyper abort) must release its hold slot — the RAII permit frees
/// on Drop where a manual unhold branch never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_hold_permit_releases_on_cancellation() {
    async fn held_settles(rt: &Arc<TenantRuntime>, n: usize) -> bool {
        // Time-based poll — a yield loop outruns the HTTP roundtrip.
        for _ in 0..500 {
            if rt.switch_gate.held() == n {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    let endpoint = rt.endpoint_slot("w1").unwrap().endpoint().clone();

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;

    rt.switch_gate.begin();
    rt.test_install(BTreeMap::new());

    // One cancellation: held() must return to 0.
    let req = tokio::spawn(http_get(ig.addr, "app.example.com", "/", &[]));
    assert!(
        held_settles(&rt, 1).await,
        "request never reached held state"
    );
    req.abort();
    let _ = req.await;
    assert!(held_settles(&rt, 0).await, "cancelled hold leaked its slot");

    // 300 cancellations back-to-back — accounting must not accumulate.
    for _ in 0..300 {
        let req = tokio::spawn(http_get(ig.addr, "app.example.com", "/", &[]));
        let _ = held_settles(&rt, 1).await;
        req.abort();
        let _ = req.await;
        assert!(held_settles(&rt, 0).await, "hold accounting leaked");
    }
    assert_eq!(rt.switch_gate.held(), 0);

    // Capacity is intact — a fresh request still holds (not a
    // capacity-503) and forwards when the gate clears.
    let req = tokio::spawn(http_get(ig.addr, "app.example.com", "/", &[]));
    assert!(
        held_settles(&rt, 1).await,
        "hold must still admit after cancellations"
    );
    rt.test_install(BTreeMap::from([("w1".to_string(), endpoint)]));
    rt.switch_gate.end();
    let (status, _, _) = tokio::time::timeout(Duration::from_secs(5), req)
        .await
        .expect("held request never resolved")
        .unwrap();
    assert_eq!(status, 200);

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// W4-r3 P0-2: `end()` landing between the gate's `active()` check
/// and the notified await must STILL wake the held request — the
/// reservation was `enable()`d before `active()` was read, so the
/// clear cannot be lost. The `SwitchGate::pre_wait_hook` seam (MIG-11:
/// moved from IngressState onto the gate so every held entry point —
/// ingress, cron trigger, queue dispatch, wfops — shares it) fires
/// inside that exact window; without the fix this test burns the full
/// 10s budget before forwarding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switch_hold_end_between_check_and_await_wakes_immediately() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;
    let endpoint = rt.endpoint_slot("w1").unwrap().endpoint().clone();

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));

    // The seam: restore routes + clear the gate INSIDE the window —
    // after `active()` read true, before the Notified is awaited.
    // (Install path stores routes before ending the gate.)
    let rt2 = rt.clone();
    let ep = endpoint.clone();
    let once = std::sync::atomic::AtomicBool::new(false);
    *rt.switch_gate.pre_wait_hook.lock().unwrap() = Some(std::sync::Arc::new(move || {
        if !once.swap(true, std::sync::atomic::Ordering::SeqCst) {
            rt2.test_install(BTreeMap::from([("w1".to_string(), ep.clone())]));
            rt2.switch_gate.end();
        }
    }));
    let ig = start_ingress(&state).await;

    rt.switch_gate.begin();
    rt.test_install(BTreeMap::new());

    let (status, _, _) = tokio::time::timeout(
        Duration::from_secs(4),
        http_get(ig.addr, "app.example.com", "/", &[]),
    )
    .await
    .expect("request burned its budget — the end() was lost");
    assert_eq!(status, 200);

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

// ---------------------------------------------------------------------
// WebSocket upgrade passthrough. The fake upstream is a bare TCP
// listener speaking just enough HTTP/1.1 to 101 and echo.
// ---------------------------------------------------------------------

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Copy)]
enum FakeMode {
    /// 101 + echo every byte.
    Echo,
    /// 403, no upgrade.
    Reject,
}

/// Spawns the fake upstream; returns its address and a channel that
/// receives each request head it saw.
async fn fake_ws_upstream(
    mode: FakeMode,
) -> (
    std::net::SocketAddr,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    let mut c = [0u8; 4096];
                    let n = s.read(&mut c).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&c[..n]);
                    if find_sub(&buf, b"\r\n\r\n").is_some() {
                        break;
                    }
                }
                let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
                match mode {
                    FakeMode::Reject => {
                        let _ = s
                            .write_all(
                                b"HTTP/1.1 403 Forbidden\r\ncontent-length: 4\r\nconnection: close\r\n\r\nnope",
                            )
                            .await;
                    }
                    FakeMode::Echo => {
                        let _ = s
                            .write_all(
                                b"HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: abc=\r\nsec-websocket-protocol: chat\r\n\r\n",
                            )
                            .await;
                        let mut c = [0u8; 4096];
                        loop {
                            let n = s.read(&mut c).await.unwrap_or(0);
                            if n == 0 {
                                return;
                            }
                            if s.write_all(&c[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    (addr, rx)
}

fn find_sub(h: &[u8], n: &[u8]) -> Option<usize> {
    h.windows(n.len()).position(|w| w == n)
}

async fn ws_state(
    worker_id: &str,
    upstream: std::net::SocketAddr,
    idle: Duration,
    max_sessions: usize,
) -> (Arc<TenantRuntime>, Arc<IngressState>, IngressGuard) {
    ws_state_pt(worker_id, upstream, idle, max_sessions, 256).await
}

async fn ws_state_pt(
    worker_id: &str,
    upstream: std::net::SocketAddr,
    idle: Duration,
    max_sessions: usize,
    per_tenant: usize,
) -> (Arc<TenantRuntime>, Arc<IngressState>, IngressGuard) {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    rt.test_install(BTreeMap::from([(
        worker_id.to_string(),
        vrt_host::endpoint::WorkerEndpoint::Tcp(upstream),
    )]));
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant(
            "t1",
            vec![worker(worker_id, "app.example.com", "d1")],
        )],
        deletions: vec![],
    };
    let state = IngressState::new(
        registry,
        Duration::from_millis(800),
        Duration::from_millis(500),
        1024,
    )
    .with_ws_limits(idle, Duration::from_secs(60), max_sessions, per_tenant);
    state.set_routes(RouteTable::from_desired(&desired));
    let ig = start_ingress(&state).await;
    (rt, state, ig)
}

/// Sends a ws handshake, returns (stream, response head).
async fn ws_handshake(addr: std::net::SocketAddr) -> (tokio::net::TcpStream, String) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(
        b"GET /chat HTTP/1.1\r\nhost: app.example.com\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\nsec-websocket-protocol: chat\r\n\r\n",
    )
    .await
    .unwrap();
    let mut buf = Vec::new();
    loop {
        let mut c = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut c))
            .await
            .expect("handshake response timed out")
            .unwrap();
        assert!(n > 0, "closed before response head");
        buf.extend_from_slice(&c[..n]);
        if let Some(i) = find_sub(&buf, b"\r\n\r\n") {
            buf.truncate(i + 4);
            break;
        }
    }
    (s, String::from_utf8_lossy(&buf).to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upgrade_echo_and_in_flight_lifecycle() {
    let (up, mut seen) = fake_ws_upstream(FakeMode::Echo).await;
    let (rt, _state, ig) = ws_state("wws1", up, Duration::from_secs(30), 16).await;

    let (mut s, head) = ws_handshake(ig.addr).await;
    let lower = head.to_ascii_lowercase();
    assert!(head.starts_with("HTTP/1.1 101"), "head: {head}");
    assert!(lower.contains("upgrade: websocket"), "head: {head}");
    assert!(lower.contains("connection: upgrade"), "head: {head}");
    assert!(lower.contains("sec-websocket-accept: abc="), "head: {head}");
    assert!(lower.contains("sec-websocket-protocol: chat"), "head: {head}");

    // Upstream saw normalized upgrade headers + passthrough sec-* and
    // our rewritten identity.
    let req = seen.recv().await.unwrap().to_ascii_lowercase();
    assert!(req.contains("upgrade: websocket"), "req: {req}");
    assert!(req.contains("connection: upgrade"), "req: {req}");
    assert!(req.contains("sec-websocket-key: "), "req: {req}");
    assert!(req.contains("x-vrt-tenant-id: t1"), "req: {req}");

    // Bidirectional bytes.
    s.write_all(b"hello-ws").await.unwrap();
    let mut got = [0u8; 8];
    tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut got))
        .await
        .expect("echo timed out")
        .unwrap();
    assert_eq!(&got, b"hello-ws");

    // The session holds the in-flight guard (drain/hibernate accounting)
    // well past the 800 ms request_timeout.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(rt.in_flight(), 1, "session must hold the slot");
    s.write_all(b"again").await.unwrap();
    let mut got = [0u8; 5];
    s.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"again");

    drop(s);
    wait_for(
        || rt.in_flight() == 0,
        Duration::from_secs(5),
        "in_flight to return to 0 after client close",
    )
    .await;
    ig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_idle_timeout_closes_session() {
    let (up, _seen) = fake_ws_upstream(FakeMode::Echo).await;
    let (rt, _state, ig) = ws_state("wws2", up, Duration::from_millis(400), 16).await;

    let (mut s, head) = ws_handshake(ig.addr).await;
    assert!(head.starts_with("HTTP/1.1 101"), "head: {head}");
    assert_eq!(rt.in_flight(), 1);

    // No traffic: the ingress must close the client side.
    let mut c = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), s.read(&mut c))
        .await
        .expect("idle session was not closed")
        .unwrap_or(0);
    assert_eq!(n, 0, "expected EOF after idle timeout");
    wait_for(
        || rt.in_flight() == 0,
        Duration::from_secs(5),
        "in_flight to return to 0 after idle close",
    )
    .await;
    ig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_concurrency_cap_returns_typed_503() {
    let (up, _seen) = fake_ws_upstream(FakeMode::Echo).await;
    let (rt, _state, ig) = ws_state("wws3", up, Duration::from_secs(30), 1).await;

    let (s1, head) = ws_handshake(ig.addr).await;
    assert!(head.starts_with("HTTP/1.1 101"), "head: {head}");

    let (_s2, head2) = ws_handshake(ig.addr).await;
    assert!(head2.starts_with("HTTP/1.1 503"), "head2: {head2}");
    assert_eq!(rt.in_flight(), 1, "rejected attempt must not leak a guard");

    // Releasing the first frees the slot for a new session.
    drop(s1);
    wait_for(
        || rt.in_flight() == 0,
        Duration::from_secs(5),
        "first session to release",
    )
    .await;
    let (_s3, head3) = ws_handshake(ig.addr).await;
    assert!(head3.starts_with("HTTP/1.1 101"), "head3: {head3}");
    ig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upstream_non_101_passes_through_and_releases() {
    let (up, _seen) = fake_ws_upstream(FakeMode::Reject).await;
    let (rt, _state, ig) = ws_state("wws4", up, Duration::from_secs(30), 1).await;

    let (_s, head) = ws_handshake(ig.addr).await;
    assert!(head.starts_with("HTTP/1.1 403"), "head: {head}");
    wait_for(
        || rt.in_flight() == 0,
        Duration::from_secs(5),
        "in_flight to return to 0 after rejected upgrade",
    )
    .await;
    // The cap permit was returned: with cap=1 a second attempt is still
    // answered by the upstream (403), not by the capacity gate.
    let (_s2, head2) = ws_handshake(ig.addr).await;
    assert!(head2.starts_with("HTTP/1.1 403"), "head2: {head2}");
    ig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_websocket_upgrade_is_stripped_not_tunnelled() {
    let (up, mut seen) = fake_ws_upstream(FakeMode::Reject).await;
    let (_rt, _state, ig) = ws_state("wws5", up, Duration::from_secs(30), 4).await;

    let (status, _, _) = http_get(
        ig.addr,
        "app.example.com",
        "/",
        &[("upgrade", "h2c"), ("connection", "Upgrade, HTTP2-Settings")],
    )
    .await;
    assert_eq!(status, 403);
    let req = seen.recv().await.unwrap().to_ascii_lowercase();
    assert!(!req.contains("upgrade:"), "req: {req}");
    assert!(!req.contains("h2c"), "req: {req}");
    ig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_per_tenant_cap_rejects_with_503_and_frees_on_close() {
    let (up, _seen) = fake_ws_upstream(FakeMode::Echo).await;
    // Global cap is generous; the tenant cap (1) is what must bite.
    let (rt, _state, ig) = ws_state_pt("wws6", up, Duration::from_secs(30), 100, 1).await;

    let (s1, head) = ws_handshake(ig.addr).await;
    assert!(head.starts_with("HTTP/1.1 101"), "head: {head}");
    let (_s2, head2) = ws_handshake(ig.addr).await;
    assert!(head2.starts_with("HTTP/1.1 503"), "head2: {head2}");
    assert_eq!(rt.in_flight(), 1, "refused attempt must not leak a guard");

    drop(s1);
    wait_for(
        || rt.in_flight() == 0,
        Duration::from_secs(5),
        "first session to release",
    )
    .await;
    let (_s3, head3) = ws_handshake(ig.addr).await;
    assert!(head3.starts_with("HTTP/1.1 101"), "head3: {head3}");
    ig.stop().await;
}

/// `of dev` only: `ingress.devCatchAll` routes ANY unmatched Host to
/// the desired doc's single worker. Production strictness holds in
/// both directions — flag off → `unknown_host`, flag on with >1
/// worker → still `unknown_host` (never a guess between workers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dev_catch_all_routes_only_single_worker_desired() {
    let registry = TenantRegistry::new();
    let rt = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt, spec);
    wait_state(&rt, TenantState::Ready, Duration::from_secs(8)).await;

    let single = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t1", vec![worker("w1", "app.example.com", "d1")])],
        deletions: vec![],
    };

    // Flag OFF (production default): unmatched Host is a typed 404 —
    // proves the catch-all never engages without the dev switch.
    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&single));
    let ig = start_ingress(&state).await;
    let (status, _, body) = http_get(ig.addr, "localhost", "/", &[]).await;
    assert_eq!(status, 404);
    assert!(body.contains("unknown_host"), "strict routing: {body}");
    ig.stop().await;

    // Flag ON (dev): every Host — including an unmatched one — lands
    // on the single worker.
    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_dev_catch_all(true);
    state.set_routes(RouteTable::from_desired(&single));
    let ig = start_ingress(&state).await;
    for host in ["localhost", "anything.example.com", "unmatched.test"] {
        let (status, _, body) = http_get(ig.addr, host, "/", &[]).await;
        assert_eq!(status, 200, "{host} must catch-all to the worker: {body}");
        assert!(body.contains("stub ok"), "{host}: {body}");
    }
    ig.stop().await;

    // Flag ON but TWO workers in the desired doc: no guess — the
    // unmatched Host still fails closed with unknown_host. Exact
    // matches keep working.
    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_dev_catch_all(true);
    state.set_routes(RouteTable::from_desired(&desired_with_routes()));
    let ig = start_ingress(&state).await;
    let (status, _, body) = http_get(ig.addr, "localhost", "/", &[]).await;
    assert_eq!(status, 404);
    assert!(
        body.contains("unknown_host"),
        "two-worker doc must not guess: {body}"
    );
    let (status, _, _) = http_get(ig.addr, "app.example.com", "/", &[]).await;
    assert_eq!(status, 200, "exact route unaffected");
    ig.stop().await;

    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}

/// POST a JSON body with one auth header; returns (status, body).
/// Local helper — the cache-purge admin endpoint is the only POST the
/// ingress tests drive.
async fn http_post_json(
    addr: std::net::SocketAddr,
    path: &str,
    token: &str,
    json: &str,
) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\nx-vrt-cache-token: {token}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{json}",
        json.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

/// Workers Cache round trip — the ingress-level integration the wc.rs
/// unit tests cannot cover: a cache-enabled route serves MISS then HIT
/// out of the pod-local store (the body proves the upstream is never
/// recontacted), and a purge through the admin HTTP endpoint forces the
/// next request back upstream. One `wc::Store` instance backs lookup,
/// single-flight and invalidation — this exercises the wiring, not the
/// units.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workers_cache_hit_then_purge_then_miss() {
    let registry = TenantRegistry::new();
    let rt1 = TenantRuntime::new("t1");
    registry.insert("t1".to_string(), rt1.clone());
    let mut fx = Fixture::new();
    let spec = fx.stub_spec("t1", &["w1"], &[]);
    let handle = spawn(&rt1, spec);
    wait_state(&rt1, TenantState::Ready, Duration::from_secs(8)).await;

    // Same fixture as the routing tests but with the entrypoint cache on.
    let mut w = worker("w1", "app.example.com", "d1");
    w.cache = Some(snapshot_protocol::compute_desired::CacheDesired {
        enabled: true,
        cross_version_cache: false,
        key: None,
    });
    let desired = snapshot_protocol::compute_desired::ComputeDesired {
        schema_version: 1,
        cell_id: "cell".into(),
        revision: 1,
        tenants: vec![tenant("t1", vec![w])],
        deletions: vec![],
    };

    let state = IngressState::new(
        registry.clone(),
        Duration::from_secs(5),
        Duration::from_millis(500),
        1024,
    );
    state.set_routes(RouteTable::from_desired(&desired));
    let ig = start_ingress(&state).await;
    let addr = ig.addr;

    // The stub emits `cache-control: max-age=60` through its emit hook,
    // making the 200 explicitly storable; its body carries a per-connection
    // sequence number, so a served-from-cache copy is byte-identical while
    // any upstream recontact advances it.
    let emit = [("x-stub-emit-header", "cache-control=max-age=60")];
    fn status_of<'a>(headers: &'a [(String, String)]) -> &'a str {
        headers
            .iter()
            .find(|(k, _)| k == "cf-cache-status")
            .map(|(_, v)| v.as_str())
            .unwrap_or("<absent>")
    }

    // Cold: proxied upstream and stored — MISS.
    let (status, headers, cold_body) = http_get(addr, "app.example.com", "/", &emit).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "MISS", "headers: {headers:?}");
    assert!(cold_body.contains("stub ok"), "{cold_body}");

    // Warm: served from the pod-local store without upstream contact —
    // the byte-identical body (same conn seq) proves it.
    let (status, headers, warm_body) = http_get(addr, "app.example.com", "/", &emit).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "HIT", "headers: {headers:?}");
    assert_eq!(warm_body, cold_body, "HIT must serve the stored copy");

    // Purge through the admin endpoint wired to the SAME store instance
    // the ingress serves — the production topology in miniature.
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();
    let admin_app = vrt_host::wc_admin::router(vrt_host::wc_admin::AdminState {
        store: state.cache_store().clone(),
        token: "test-token".into(),
    });
    let admin_task = tokio::spawn(async move {
        axum::serve(admin_listener, admin_app).await.unwrap();
    });
    let (status, body) = http_post_json(
        admin_addr,
        "/internal/cache/purge",
        "test-token",
        r#"{"tenantId":"t1","workerId":"w1"}"#,
    )
    .await;
    assert_eq!(status, 200, "purge: {body}");
    assert!(body.contains("\"purged\":1"), "one entry purged: {body}");

    // Cold again: the purge invalidated the entry, so the upstream is
    // recontacted — the conn sequence advances and the status is MISS.
    let (status, headers, repurge_body) = http_get(addr, "app.example.com", "/", &emit).await;
    assert_eq!(status, 200);
    assert_eq!(status_of(&headers), "MISS", "headers: {headers:?}");
    assert_ne!(
        repurge_body, cold_body,
        "post-purge response must come from a fresh upstream fetch"
    );

    admin_task.abort();
    ig.stop().await;
    handle.send(SupervisorCmd::Stop).await;
    wait_state(&rt1, TenantState::Stopped, Duration::from_secs(8)).await;
    handle.join().await;
}
