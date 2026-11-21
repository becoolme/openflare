//! Local HTTP ingress — the Pod-internal entry point.
//!
//! `Host` header → [`RouteTable`] (exact match, verified snapshot only)
//! → tenant runtime → worker [`EndpointSlot`] → hyper/1.1 client over
//! the worker's unix socket (loopback TCP in tests).
//!
//! Discipline:
//! - Inbound forged internal headers (`x-vrt-*`, `x-open-compute-*`)
//!   are stripped unconditionally, then trusted `x-vrt-*` identity
//!   headers are written by us.
//! - Hop-by-hop headers are removed in both directions.
//! - Request bodies stream with a hard size cap; response bodies stream
//!   while holding the generation's in-flight guard, so a draining
//!   process is never killed mid-response.
//! - WebSocket upgrades (HTTP/1.1 `GET` + `Upgrade: websocket` only) are
//!   tunnelled byte-for-byte after the worker answers `101`; the tunnel
//!   holds the in-flight guard, is bounded by idle / max-duration /
//!   concurrency limits, and every other `Upgrade` is stripped.
//! - Every failure is a typed status (`502 upstream_connect`,
//!   `503 tenant_unavailable|tenant_circuit_open`, …) — never a bare 500.
//!
//! Traefik / tenant-gateway integration and its exact contract are
//! future work (documented in `workers-runtime-host.md`).

use crate::route::{normalize_host, RouteTable};
use crate::supervisor::{InFlightGuard, TenantRegistry};
use arc_swap::ArcSwap;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;

/// Hop-by-hop + forged headers removed before forwarding.
///
/// MIG-7: proxy-derived headers are platform-owned in the request
/// direction — the values a workerd sees are ALWAYS written by
/// `scrubbed_headers` from the trusted-source policy below, so a
/// client (or a non-Traefik pod on the ingress port) can never inject
/// a scheme, client-IP chain, or Cloudflare edge header. `cf-*` is
/// stripped unconditionally: the front proxy does not sanitize it
/// and MIG-10/MIG-31 synthesize `request.cf` only from the verified
/// Cloudflare chain.
const STRIP_REQUEST: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "cdn-loop",
    "cf-connecting-ip",
    "cf-ew-via",
    "cf-ipcountry",
    // MIG-31: visitor-location managed-transform headers. Inbound
    // copies are always dropped; only the verified CF chain's values
    // are re-injected below (and mirrored into `request.cf`).
    "cf-ipcity",
    "cf-ipcontinent",
    "cf-iplatitude",
    "cf-iplongitude",
    "cf-metro-code",
    "cf-postal-code",
    "cf-ray",
    "cf-region",
    "cf-region-code",
    "cf-timezone",
    "cf-visitor",
    "true-client-ip",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-forwarded-proto",
    "x-real-ip",
];

/// Internal prefixes stripped from inbound requests — a client can never
/// forge identity/ownership headers into the worker plane.
const STRIP_PREFIXES: &[&str] = &["x-vrt-", "x-open-compute-"];

/// WebSocket tunnel defaults: idle 300 s, lifetime 4 h, 4096 sessions
/// per ingress.
pub const WS_DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
pub const WS_DEFAULT_MAX_DURATION: Duration = Duration::from_secs(4 * 60 * 60);
pub const WS_DEFAULT_MAX_SESSIONS: usize = 4096;
/// Default per-tenant cap on concurrent WebSocket sessions.
pub const WS_DEFAULT_MAX_PER_TENANT: usize = 256;

/// T31f: bounded route-failure reasons the in-workerd router emits on
/// `x-vrt-route-error` — must match facade `router-wire.ts`
/// `ROUTE_ERROR_REASONS` (cross-pinned by a vrt-config test).
const ROUTE_ERROR_REASONS: &[&str] = &[
    "route_index_rpc_failed",
    "route_index_invalid",
    "route_index_digest_mismatch",
    "route_index_timeout",
    "can_fetch_rpc_failed",
    "can_fetch_timeout",
];

/// MIG-7: how the `x-forwarded-proto` value written to workerd is
/// derived. workerd's `forwardedProtoHeader` makes `request.url`'s
/// scheme this header's value, so the value must come from a trusted
/// source only — a forged client header must never reach the worker
/// (Cloudflare-compatible semantics: `new URL(request.url).protocol`
/// reflects the client's real TLS state).
#[derive(Clone, Debug)]
pub enum ForwardedProto {
    /// Fail-closed default: every request is reported `http`.
    FixedHttp,
    /// Every request is reported `https` — only for deployments where
    /// the sole network path to the ingress is already TLS-terminated
    /// at the edge (e.g. a `websecure`-only Traefik Ingress) and no
    /// cleartext route can ever be added. Drift risk: adding an http
    /// entrypoint later would mislabel cleartext requests — prefer
    /// [`Self::TrustedHeader`] when the edge handles mixed schemes.
    FixedHttps,
    /// Honor the inbound `x-forwarded-proto` only when the TCP peer is
    /// inside `trusted` (the front proxy's source ranges — the same
    /// set NetworkPolicy allows to the ingress port). Only the exact
    /// value `https` (ASCII case-insensitive, trimmed) promotes the
    /// scheme; missing, malformed, or untrusted values fail closed to
    /// `http`.
    TrustedHeader { trusted: Vec<vrt_config::Cidr> },
}

impl ForwardedProto {
    /// Whether `peer` may supply the trusted `x-forwarded-*` values.
    fn peer_is_trusted(&self, peer: &std::net::IpAddr) -> bool {
        match self {
            Self::TrustedHeader { trusted } => {
                trusted.iter().any(|c| c.contains(peer))
            }
            Self::FixedHttp | Self::FixedHttps => false,
        }
    }
}

/// MIG-9: typed upstream client-IP trust — which sources may supply
/// proxy-derived client identity, and how the value injected for the
/// worker (`cf-connecting-ip`, Cloudflare-compatible) is derived.
/// Orthogonal to [`ForwardedProto`]: scheme trust and client-IP trust
/// are separate policies, each carrying its own explicit trusted-proxy
/// CIDR set (the deployment sources both from the same addresses the
/// ingress-port NetworkPolicy `from` peers enumerate).
#[derive(Clone, Debug)]
pub enum ClientIpPolicy {
    /// Fail-closed default: `cf-connecting-ip` is never synthesized —
    /// the worker sees only the platform-written `x-forwarded-for`.
    None,
    /// A trusted TCP peer may supply the client IP via its XFF chain:
    /// the value is the rightmost chain entry that is not itself a
    /// trusted proxy (walking right-to-left past proxy hops).
    /// Upstream `cf-connecting-ip`/`true-client-ip` are NEVER
    /// honored — anything in front of the proxy can forge them.
    TrustedXff { trusted: Vec<vrt_config::Cidr> },
    /// Cloudflare-compatible semantics: a trusted TCP peer AND the
    /// XFF hop adjacent to it (the LAST chain entry — what the proxy
    /// appended) inside `cloudflare` — only then is the upstream
    /// `cf-connecting-ip` (fallback `true-client-ip`) believed and
    /// injected for the worker. Any mismatch or unparseable value
    /// falls back to the XFF-derived rightmost untrusted hop; an
    /// untrusted peer never injects anything.
    Cloudflare {
        trusted: Vec<vrt_config::Cidr>,
        cloudflare: Vec<vrt_config::Cidr>,
    },
}

impl ClientIpPolicy {
    /// The trusted-proxy CIDR set for modes that have one.
    fn trusted_cidrs(&self) -> Option<&[vrt_config::Cidr]> {
        match self {
            Self::None => None,
            Self::TrustedXff { trusted } | Self::Cloudflare { trusted, .. } => Some(trusted),
        }
    }
}

/// Canonicalize for comparison/injection — `::ffff:a.b.c.d` (the form
/// a dual-stack listener reports for a v4 peer) becomes the plain v4
/// address so it can satisfy a v4 CIDR and reads cleanly to workers.
fn canon_ip(ip: std::net::IpAddr) -> std::net::IpAddr {
    ip.to_canonical()
}

/// Parse a single-IP header (`cf-connecting-ip`, `true-client-ip`),
/// canonicalized. `Err(true)` when the header was PRESENT but
/// unparseable — the caller counts it, never injects it.
fn parse_ip_header(inbound: &HeaderMap, name: &str) -> Result<Option<std::net::IpAddr>, bool> {
    let Some(v) = inbound.get(name) else {
        return Ok(None);
    };
    match v
        .to_str()
        .ok()
        .and_then(|t| t.trim().parse::<std::net::IpAddr>().ok())
    {
        Some(ip) => Ok(Some(canon_ip(ip))),
        None => Err(true),
    }
}

/// All inbound `x-forwarded-for` entries — comma-split, parsed,
/// canonicalized. Unparseable entries (`unknown`, junk) are skipped:
/// they can be neither a trusted hop nor an injectable value.
fn xff_chain(inbound: &HeaderMap) -> Vec<std::net::IpAddr> {
    let mut out = Vec::new();
    for v in inbound.get_all("x-forwarded-for").iter() {
        let Ok(text) = v.to_str() else { continue };
        for part in text.split(',') {
            if let Ok(ip) = part.trim().parse::<std::net::IpAddr>() {
                out.push(canon_ip(ip));
            }
        }
    }
    out
}

/// The rightmost XFF entry that is not itself a trusted proxy — the
/// identity of the last untrusted downstream hop.
fn xff_derived_client(
    chain: &[std::net::IpAddr],
    trusted: &[vrt_config::Cidr],
) -> Option<std::net::IpAddr> {
    chain
        .iter()
        .rev()
        .copied()
        .find(|ip| !trusted.iter().any(|c| c.contains(ip)))
}

/// MIG-9: the client IP to inject as `cf-connecting-ip`, or `None`
/// to inject nothing. Fail closed at every step: an untrusted TCP
/// peer can never supply proxy-derived identity; a trusted peer is
/// believed only for what its mode allows; every injected value is a
/// parsed, canonicalized IP — a malformed candidate is skipped and
/// counted (`vrt_host_client_ip_invalid_total`), never passed through.
fn resolve_client_ip(
    policy: &ClientIpPolicy,
    peer: &std::net::IpAddr,
    inbound: &HeaderMap,
) -> Option<std::net::IpAddr> {
    let trusted = policy.trusted_cidrs()?;
    let peer = canon_ip(*peer);
    if !trusted.iter().any(|c| c.contains(&peer)) {
        return None;
    }
    let chain = xff_chain(inbound);
    let derived = xff_derived_client(&chain, trusted);
    match policy {
        ClientIpPolicy::None => None,
        ClientIpPolicy::TrustedXff { .. } => derived,
        ClientIpPolicy::Cloudflare { .. } => {
            // The hop ADJACENT to our trusted proxy is the LAST chain
            // entry — the address the proxy appended for its own
            // downstream. Only when that hop is a Cloudflare edge may
            // the upstream `cf-connecting-ip`/`true-client-ip` be
            // believed; anything else (a direct client, a non-CF
            // front, a forged chain tail) must not propagate.
            if upstream_is_cloudflare(policy, &peer, inbound) {
                for name in ["cf-connecting-ip", "true-client-ip"] {
                    match parse_ip_header(inbound, name) {
                        Ok(Some(ip)) => return Some(ip),
                        Ok(None) => {}
                        Err(_) => {
                            crate::metrics::registry().inc(
                                "vrt_host_client_ip_invalid_total",
                                &[("source", name)],
                                1.0,
                            );
                        }
                    }
                }
            }
            derived
        }
    }
}

/// MIG-10: request header carrying the synthesized `request.cf` JSON
/// — workerd's `cfBlobHeader` (rendered by vrt-config) reads it.
/// `x-vrt-` is a STRIP_PREFIXES prefix, so only this host can set the
/// value; cross-pinned with `vrt_config::capnp::CF_BLOB_HEADER`.
const CF_BLOB_HEADER: &str = vrt_config::capnp::CF_BLOB_HEADER;

/// Bound on the synthesized blob — far above the whitelist's real
/// size; a regression that would emit more is cut to `{}`.
const MAX_CF_BLOB_BYTES: usize = 1024;

/// MIG-10: whether the request arrived through the trusted-proxy →
/// Cloudflare-edge chain — the same peer+adjacent-hop test that gates
/// `cf-connecting-ip` injection. Only then may upstream `cf-*`
/// metadata (country/colo/ray) be believed for the worker.
fn upstream_is_cloudflare(
    policy: &ClientIpPolicy,
    peer: &std::net::IpAddr,
    inbound: &HeaderMap,
) -> bool {
    let ClientIpPolicy::Cloudflare {
        trusted,
        cloudflare,
    } = policy
    else {
        return false;
    };
    if !trusted.iter().any(|c| c.contains(&canon_ip(*peer))) {
        return false;
    }
    xff_chain(inbound)
        .last()
        .is_some_and(|ip| cloudflare.iter().any(|c| c.contains(ip)))
}

/// `cf-ipcountry` value shape — two-letter region code, or CF's
/// sentinels. Anything else is dropped, never passed through.
fn valid_cf_country(s: &str) -> bool {
    s == "T1"
        || s == "XX"
        || (s.len() == 2 && s.bytes().all(|b| b.is_ascii_uppercase()))
}

/// Split a `cf-ray` value into (id, colo). The real format is
/// `<hex>-<AAA>`; a malformed value yields NOTHING — the whole ray is
/// dropped rather than passing a forged tail through.
fn parse_cf_ray(raw: &str) -> Option<(&str, &str)> {
    let (id, colo) = raw.rsplit_once('-')?;
    let ok = (6..=32).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_hexdigit())
        && colo.len() == 3
        && colo.bytes().all(|b| b.is_ascii_uppercase());
    ok.then_some((id, colo))
}

/// MIG-10: trusted Cloudflare request metadata — synthesized ONLY
/// from a verified CF chain; every field independently validated.
/// Absent/invalid upstream values are dropped, never echoed.
#[derive(Default)]
struct CfMetadata {
    /// `cf-ipcountry` → request.cf.country AND the re-injected header.
    country: Option<String>,
    /// Last `-AAA` segment of `cf-ray` → request.cf.colo.
    colo: Option<String>,
    /// Full validated `cf-ray` for header pass-through.
    ray: Option<String>,
    // MIG-31: visitor location managed transform (`Add visitor location
    // headers`). Every value is independently validated; CF exposes all
    // of them as strings in `request.cf` and forwards the headers.
    city: Option<String>,
    continent: Option<String>,
    region: Option<String>,
    region_code: Option<String>,
    metro_code: Option<String>,
    postal_code: Option<String>,
    timezone: Option<String>,
    latitude: Option<String>,
    longitude: Option<String>,
    // Local-MMDB-only fields (upstream `cf-*` headers never carry
    // them; Cloudflare derives them edge-side). `asn` is a JSON
    // NUMBER in `request.cf`, not a string.
    asn: Option<u32>,
    as_org: Option<String>,
}

impl CfMetadata {
    /// Fill fields a verified upstream chain did NOT provide from a
    /// local [`crate::geoip::GeoRecord`]. Verified upstream values
    /// always win — the MMDB only covers what the edge in front of
    /// us did not describe (direct ingress, non-CF proxy, mirror
    /// deployments), which is exactly what `request.cf` on
    /// Cloudflare itself reports for the connecting client.
    fn merge_geo(&mut self, r: &crate::geoip::GeoRecord) {
        self.country = self.country.take().or_else(|| r.country.clone());
        self.continent = self.continent.take().or_else(|| r.continent.clone());
        self.city = self.city.take().or_else(|| r.city.clone());
        self.region = self.region.take().or_else(|| r.region.clone());
        self.region_code = self.region_code.take().or_else(|| r.region_code.clone());
        self.metro_code = self.metro_code.take().or_else(|| r.metro_code.clone());
        self.postal_code = self.postal_code.take().or_else(|| r.postal_code.clone());
        self.timezone = self.timezone.take().or_else(|| r.timezone.clone());
        self.latitude = self.latitude.take().or_else(|| r.latitude.clone());
        self.longitude = self.longitude.take().or_else(|| r.longitude.clone());
        self.asn = self.asn.or(r.asn);
        self.as_org = self.as_org.take().or_else(|| r.as_org.clone());
    }
}

/// MIG-31: bounded, control-free UTF-8 header value (Cloudflare encodes
/// non-ASCII location names as UTF-8, so `HeaderValue::to_str` — visible
/// ASCII only — is not enough).
fn cf_header_text(inbound: &HeaderMap, name: &str, max: usize) -> Option<String> {
    let raw = inbound.get(name)?.as_bytes();
    let s = std::str::from_utf8(raw).ok()?.trim();
    if s.is_empty() || s.len() > max || s.chars().any(|c| c.is_control()) {
        return None;
    }
    Some(s.to_string())
}

/// `ip.src.continent` is one of the seven continent codes.
fn valid_cf_continent(s: &str) -> bool {
    matches!(s, "AF" | "AN" | "AS" | "EU" | "NA" | "OC" | "SA")
}

/// ISO 3166-2 first-level region code (`13`, `JP-13`, `TX`, ...).
fn valid_cf_region_code(s: &str) -> bool {
    s.len() <= 16
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
}

/// `ip.src.metro_code` (US DMA), digits only.
fn valid_cf_metro_code(s: &str) -> bool {
    s.len() <= 8 && s.bytes().all(|b| b.is_ascii_digit())
}

/// Postal codes are alphanumeric with optional spaces/hyphens.
fn valid_cf_postal_code(s: &str) -> bool {
    s.len() <= 32
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b' ' || b == b'-')
}

/// IANA timezone name (`Asia/Tokyo`, `UTC`, `America/Argentina/Buenos_Aires`).
fn valid_cf_timezone(s: &str) -> bool {
    s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'-' | b'/'))
}

/// Coordinate string: parses as a finite float inside the axis range.
fn valid_cf_coord(s: &str, min: f64, max: f64) -> bool {
    s.len() <= 24
        && s.parse::<f64>()
            .map(|v| v.is_finite() && (min..=max).contains(&v))
            .unwrap_or(false)
}

/// EU member states (as used for `request.cf.isEUCountry`).
const EU_COUNTRIES: &[&str] = &[
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT",
    "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
];

fn cf_metadata(policy: &ClientIpPolicy, peer: &std::net::IpAddr, inbound: &HeaderMap) -> CfMetadata {
    let mut m = CfMetadata::default();
    if !upstream_is_cloudflare(policy, peer, inbound) {
        return m;
    }
    if let Some(v) = inbound.get("cf-ipcountry").and_then(|v| v.to_str().ok()) {
        if valid_cf_country(v) {
            m.country = Some(v.to_string());
        }
    }
    if let Some(v) = inbound.get("cf-ray").and_then(|v| v.to_str().ok()) {
        if let Some((id, colo)) = parse_cf_ray(v) {
            m.ray = Some(format!("{id}-{colo}"));
            m.colo = Some(colo.to_string());
        }
    }
    // MIG-31: visitor location managed transform. Only the verified CF
    // chain is read, so a direct client can never inject any of these.
    m.city = cf_header_text(inbound, "cf-ipcity", 96);
    m.continent = cf_header_text(inbound, "cf-ipcontinent", 2).filter(|v| valid_cf_continent(v));
    m.region = cf_header_text(inbound, "cf-region", 96);
    m.region_code =
        cf_header_text(inbound, "cf-region-code", 16).filter(|v| valid_cf_region_code(v));
    m.metro_code = cf_header_text(inbound, "cf-metro-code", 8).filter(|v| valid_cf_metro_code(v));
    m.postal_code = cf_header_text(inbound, "cf-postal-code", 32).filter(|v| valid_cf_postal_code(v));
    m.timezone = cf_header_text(inbound, "cf-timezone", 64).filter(|v| valid_cf_timezone(v));
    m.latitude =
        cf_header_text(inbound, "cf-iplatitude", 24).filter(|v| valid_cf_coord(v, -90.0, 90.0));
    m.longitude =
        cf_header_text(inbound, "cf-iplongitude", 24).filter(|v| valid_cf_coord(v, -180.0, 180.0));
    m
}

/// The `request.cf` JSON injected via [`CF_BLOB_HEADER`]. Whitelist
/// fields only — `clientPid`/`clientUid` (the unix-peer credentials)
/// can never appear here; workerd replaces cf wholesale with this
/// blob. Bounded by [`MAX_CF_BLOB_BYTES`].
fn cf_blob_json(meta: &CfMetadata, http_protocol: &str) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "httpProtocol".to_string(),
        serde_json::Value::String(http_protocol.to_string()),
    );
    if let Some(c) = &meta.country {
        map.insert(
            "country".to_string(),
            serde_json::Value::String(c.clone()),
        );
        // `isEUCountry` is the string "1" when true (CF omits it
        // otherwise), never a boolean.
        if EU_COUNTRIES.contains(&c.as_str()) {
            map.insert(
                "isEUCountry".to_string(),
                serde_json::Value::String("1".to_string()),
            );
        }
    }
    if let Some(c) = &meta.colo {
        map.insert("colo".to_string(), serde_json::Value::String(c.clone()));
    }
    // Local-MMDB fields — `asn` is a NUMBER in `request.cf` (CF
    // emits `395747`, not `"395747"`).
    if let Some(a) = meta.asn {
        map.insert("asn".to_string(), serde_json::Value::Number(a.into()));
    }
    if let Some(o) = &meta.as_org {
        map.insert(
            "asOrganization".to_string(),
            serde_json::Value::String(o.clone()),
        );
    }
    // MIG-31: visitor location fields are strings in `request.cf`.
    for (key, value) in [
        ("city", &meta.city),
        ("continent", &meta.continent),
        ("region", &meta.region),
        ("regionCode", &meta.region_code),
        ("metroCode", &meta.metro_code),
        ("postalCode", &meta.postal_code),
        ("timezone", &meta.timezone),
        ("latitude", &meta.latitude),
        ("longitude", &meta.longitude),
    ] {
        if let Some(v) = value {
            map.insert(key.to_string(), serde_json::Value::String(v.clone()));
        }
    }
    let s = serde_json::to_string(&serde_json::Value::Object(map))
        .unwrap_or_else(|_| "{}".to_string());
    if s.len() > MAX_CF_BLOB_BYTES {
        return "{}".to_string();
    }
    s
}

/// `request.cf.httpProtocol` spelling — CF's `HTTP/1.1`/`HTTP/2` form.
fn http_protocol(version: axum::http::Version) -> &'static str {
    match version {
        axum::http::Version::HTTP_10 => "HTTP/1.0",
        axum::http::Version::HTTP_2 => "HTTP/2",
        axum::http::Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/1.1",
    }
}

/// Shared ingress state.
pub struct IngressState {
    /// Test instrumentation (`#[doc(hidden)]`): when set, the next
    /// request panics inside the handler — proves the catch_unwind
    /// boundary turns panics into typed 500s, never into a reset.
    pub panic_once: std::sync::atomic::AtomicBool,
    routes: ArcSwap<RouteTable>,
    registry: Arc<TenantRegistry>,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_body_bytes: usize,
    /// W4-r2: serialized-switch request-hold budget (per request).
    switch_hold_budget: Duration,
    /// W4-r2: per-tenant bound on simultaneously held requests.
    switch_hold_max_reqs: usize,
    /// W4-r2: per-tenant bound on declared bytes held.
    switch_hold_max_bytes: usize,
    /// MIG-7: trusted-source policy for the forwarded scheme.
    forwarded_proto: ForwardedProto,
    /// MIG-9: typed upstream client-IP trust (`cf-connecting-ip`).
    client_ip: ClientIpPolicy,
    /// Local GeoIP database for `request.cf` geo/ASN fields —
    /// `None` disables lookups (config `geoip.enabled = false`).
    /// The handle itself hot-reloads; the Option is fixed at
    /// startup like every other policy field.
    geoip: Option<Arc<crate::geoip::GeoIp>>,
    /// WebSocket tunnel: close after this long with no bytes in either
    /// direction.
    ws_idle_timeout: Duration,
    /// WebSocket tunnel: hard ceiling on one session's lifetime.
    ws_max_duration: Duration,
    /// WebSocket tunnel: per-ingress cap on concurrent sessions.
    ws_max_sessions: usize,
    /// Live + reserved WebSocket sessions (see [`WsPermit`]).
    ws_active: Arc<AtomicUsize>,
    /// WebSocket tunnel: per-tenant cap on concurrent sessions, so one
    /// tenant cannot exhaust the ingress-wide cap.
    ws_max_per_tenant: usize,
    /// Live + reserved sessions per tenant. A key is removed when its
    /// count reaches zero, so the map is bounded by live tenants.
    ws_tenant_active: Arc<Mutex<HashMap<String, usize>>>,
    /// Worker-log hub: host-observed routing failures are recorded as
    /// `level=platform` records. Set once after construction; unset =
    /// log collection disabled (zero overhead).
    logs: std::sync::OnceLock<Arc<crate::logs::LogHub>>,
    /// `vrt dev` only: when set, an unknown Host falls back to the
    /// route table's single-worker catch-all target. Production
    /// configs never set it — `RouteTable::lookup` stays exact-match.
    dev_catch_all: Arc<AtomicBool>,
}

impl IngressState {
    /// New state. `routes` is replaced wholesale on each converge.
    pub fn new(
        registry: Arc<TenantRegistry>,
        request_timeout: Duration,
        connect_timeout: Duration,
        max_body_bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            routes: ArcSwap::from_pointee(RouteTable::default()),
            registry,
            request_timeout,
            connect_timeout,
            max_body_bytes,
            panic_once: std::sync::atomic::AtomicBool::new(false),
            // W4-r2 defaults — see `with_switch_hold` for the rationale.
            switch_hold_budget: Duration::from_secs(10),
            switch_hold_max_reqs: 256,
            switch_hold_max_bytes: 32 * 1024 * 1024,
            // MIG-7 fail closed: `fixed-http` until config proves a
            // trusted source — a client can never fake https.
            forwarded_proto: ForwardedProto::FixedHttp,
            // MIG-9 fail closed: `none` until config proves a trusted
            // source — `cf-connecting-ip` is never forged for clients.
            client_ip: ClientIpPolicy::None,
            geoip: None,
            ws_idle_timeout: WS_DEFAULT_IDLE_TIMEOUT,
            ws_max_duration: WS_DEFAULT_MAX_DURATION,
            ws_max_sessions: WS_DEFAULT_MAX_SESSIONS,
            ws_active: Arc::new(AtomicUsize::new(0)),
            ws_tenant_active: Arc::new(Mutex::new(HashMap::new())),
            ws_max_per_tenant: WS_DEFAULT_MAX_PER_TENANT,
            logs: std::sync::OnceLock::new(),
            dev_catch_all: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Attach the dev-mode catch-all switch (once; default off).
    /// `vrt dev` sets it after construction, like `set_log_hub`.
    pub fn set_dev_catch_all(&self, on: bool) {
        self.dev_catch_all
            .store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// Attach the worker-log hub (once; later calls are ignored).
    pub fn set_log_hub(&self, hub: Arc<crate::logs::LogHub>) {
        let _ = self.logs.set(hub);
    }

    /// W4-r2: bound the serialized-switch hold. Default 10s covers a
    /// workerd drain+launch+ready gap (~5s observed) with margin, while
    /// never exceeding one client-visible retry cadence; 256 reqs /
    /// 32MiB declared per tenant bounds memory under a burst.
    pub fn with_switch_hold(
        self: &Arc<Self>,
        budget: Duration,
        max_reqs: usize,
        max_bytes: usize,
    ) -> Arc<Self> {
        // Rebuild with overrides — IngressState is created once at
        // startup, so a rebuild is cheaper than interior mutability.
        let next = Self::new(
            self.registry.clone(),
            self.request_timeout,
            self.connect_timeout,
            self.max_body_bytes,
        );
        // SAFETY-of-shape: fields are private; construct directly.
        Arc::new(IngressState {
            routes: ArcSwap::new(self.routes.load_full()),
            registry: next.registry.clone(),
            request_timeout: next.request_timeout,
            connect_timeout: next.connect_timeout,
            max_body_bytes: next.max_body_bytes,
            switch_hold_budget: budget,
            switch_hold_max_reqs: max_reqs,
            switch_hold_max_bytes: max_bytes,
            forwarded_proto: self.forwarded_proto.clone(),
            client_ip: self.client_ip.clone(),
            geoip: self.geoip.clone(),
            ws_idle_timeout: self.ws_idle_timeout,
            ws_max_duration: self.ws_max_duration,
            ws_max_sessions: self.ws_max_sessions,
            ws_active: self.ws_active.clone(),
            ws_tenant_active: self.ws_tenant_active.clone(),
            ws_max_per_tenant: self.ws_max_per_tenant,
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
            dev_catch_all: self.dev_catch_all.clone(),
        })
    }

    /// MIG-7: install the forwarded-proto policy resolved from
    /// `[ingress]` config (rebuild pattern, same as
    /// [`Self::with_switch_hold`]).
    pub fn with_forwarded_proto(self: &Arc<Self>, policy: ForwardedProto) -> Arc<Self> {
        Arc::new(IngressState {
            routes: ArcSwap::new(self.routes.load_full()),
            registry: self.registry.clone(),
            request_timeout: self.request_timeout,
            connect_timeout: self.connect_timeout,
            max_body_bytes: self.max_body_bytes,
            switch_hold_budget: self.switch_hold_budget,
            switch_hold_max_reqs: self.switch_hold_max_reqs,
            switch_hold_max_bytes: self.switch_hold_max_bytes,
            forwarded_proto: policy,
            client_ip: self.client_ip.clone(),
            geoip: self.geoip.clone(),
            ws_idle_timeout: self.ws_idle_timeout,
            ws_max_duration: self.ws_max_duration,
            ws_max_sessions: self.ws_max_sessions,
            ws_active: self.ws_active.clone(),
            ws_tenant_active: self.ws_tenant_active.clone(),
            ws_max_per_tenant: self.ws_max_per_tenant,
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
            dev_catch_all: self.dev_catch_all.clone(),
        })
    }

    /// MIG-9: install the client-IP policy resolved from
    /// `[ingress.clientIp]` config (rebuild pattern, same as
    /// [`Self::with_forwarded_proto`]).
    pub fn with_client_ip(self: &Arc<Self>, policy: ClientIpPolicy) -> Arc<Self> {
        Arc::new(IngressState {
            routes: ArcSwap::new(self.routes.load_full()),
            registry: self.registry.clone(),
            request_timeout: self.request_timeout,
            connect_timeout: self.connect_timeout,
            max_body_bytes: self.max_body_bytes,
            switch_hold_budget: self.switch_hold_budget,
            switch_hold_max_reqs: self.switch_hold_max_reqs,
            switch_hold_max_bytes: self.switch_hold_max_bytes,
            forwarded_proto: self.forwarded_proto.clone(),
            client_ip: policy,
            geoip: self.geoip.clone(),
            ws_idle_timeout: self.ws_idle_timeout,
            ws_max_duration: self.ws_max_duration,
            ws_max_sessions: self.ws_max_sessions,
            ws_active: self.ws_active.clone(),
            ws_tenant_active: self.ws_tenant_active.clone(),
            ws_max_per_tenant: self.ws_max_per_tenant,
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
            dev_catch_all: self.dev_catch_all.clone(),
        })
    }

    /// Install the local GeoIP handle resolved from `[geoip]` config
    /// (rebuild pattern, same as [`Self::with_client_ip`]). `None`
    /// disables lookups entirely — `request.cf` then carries only the
    /// upstream-verified fields, exactly as before.
    pub fn with_geoip(self: &Arc<Self>, geo: Option<Arc<crate::geoip::GeoIp>>) -> Arc<Self> {
        Arc::new(IngressState {
            routes: ArcSwap::new(self.routes.load_full()),
            registry: self.registry.clone(),
            request_timeout: self.request_timeout,
            connect_timeout: self.connect_timeout,
            max_body_bytes: self.max_body_bytes,
            switch_hold_budget: self.switch_hold_budget,
            switch_hold_max_reqs: self.switch_hold_max_reqs,
            switch_hold_max_bytes: self.switch_hold_max_bytes,
            forwarded_proto: self.forwarded_proto.clone(),
            client_ip: self.client_ip.clone(),
            geoip: geo,
            ws_idle_timeout: self.ws_idle_timeout,
            ws_max_duration: self.ws_max_duration,
            ws_max_sessions: self.ws_max_sessions,
            ws_active: self.ws_active.clone(),
            ws_tenant_active: self.ws_tenant_active.clone(),
            ws_max_per_tenant: self.ws_max_per_tenant,
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
            dev_catch_all: self.dev_catch_all.clone(),
        })
    }

    /// WebSocket tunnel bounds (rebuild pattern, same as
    /// [`Self::with_client_ip`]). Zero values are clamped so a
    /// misconfiguration can never mean "unbounded".
    pub fn with_ws_limits(
        self: &Arc<Self>,
        idle_timeout: Duration,
        max_duration: Duration,
        max_sessions: usize,
        max_per_tenant: usize,
    ) -> Arc<Self> {
        let at_least = |d: Duration| d.max(Duration::from_millis(1));
        Arc::new(IngressState {
            routes: ArcSwap::new(self.routes.load_full()),
            registry: self.registry.clone(),
            request_timeout: self.request_timeout,
            connect_timeout: self.connect_timeout,
            max_body_bytes: self.max_body_bytes,
            switch_hold_budget: self.switch_hold_budget,
            switch_hold_max_reqs: self.switch_hold_max_reqs,
            switch_hold_max_bytes: self.switch_hold_max_bytes,
            forwarded_proto: self.forwarded_proto.clone(),
            client_ip: self.client_ip.clone(),
            geoip: self.geoip.clone(),
            ws_idle_timeout: at_least(idle_timeout),
            ws_max_duration: at_least(max_duration),
            ws_max_sessions: max_sessions.max(1),
            ws_active: self.ws_active.clone(),
            ws_tenant_active: self.ws_tenant_active.clone(),
            ws_max_per_tenant: max_per_tenant.max(1),
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
            dev_catch_all: self.dev_catch_all.clone(),
        })
    }

    /// Swap the route table (converge output).
    pub fn set_routes(&self, table: RouteTable) {
        self.routes.store(Arc::new(table));
    }
}

/// Typed ingress failure → status + machine-readable code.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// Malformed `Host` header.
    #[error("bad_host")]
    BadHost,
    /// Hostname has no route in the snapshot.
    #[error("unknown_host")]
    UnknownHost,
    /// Tenant exists but cannot serve right now.
    #[error("tenant_unavailable")]
    TenantUnavailable(&'static str),
    /// Tenant circuit breaker open.
    #[error("tenant_circuit_open")]
    CircuitOpen,
    /// Scale-to-zero: the wake exceeded its deadline.
    #[error("tenant_wake_timeout")]
    WakeTimeout,
    /// Scale-to-zero: the wake launch / ready probe failed.
    #[error("tenant_wake_failed")]
    WakeFailed,
    /// Scale-to-zero: host wake capacity exhausted.
    #[error("wake_capacity")]
    WakeCapacity,
    /// Worker socket missing on the live generation.
    #[error("worker_unavailable")]
    WorkerUnavailable,
    /// Request body exceeds the configured cap.
    #[error("payload_too_large")]
    PayloadTooLarge,
    /// Could not connect / handshake to the worker process.
    #[error("upstream_connect")]
    UpstreamConnect,
    /// Upstream did not respond inside the request timeout.
    #[error("upstream_timeout")]
    UpstreamTimeout,
    /// Upstream I/O / protocol failure mid-request.
    #[error("upstream_error")]
    UpstreamError,
    /// WebSocket session limit reached on this ingress.
    #[error("ws_capacity")]
    WsCapacity,
    /// Handler panic or other internal fault (boundary-caught).
    #[error("internal")]
    Internal,
}

impl IngressError {
    /// B12: stable reason token for `vrt_worker_exceptions_total` —
    /// only failures the host itself can attribute (never guessed).
    /// HTTP status + detail returned to the client for this error.
    pub fn status_and_detail(&self) -> (StatusCode, &str) {
        match self {
            Self::BadHost => (StatusCode::BAD_REQUEST, "malformed Host header"),
            Self::UnknownHost => (StatusCode::NOT_FOUND, "no route for host"),
            Self::TenantUnavailable(reason) => (StatusCode::SERVICE_UNAVAILABLE, reason),
            Self::CircuitOpen => (
                StatusCode::SERVICE_UNAVAILABLE,
                "tenant circuit breaker open",
            ),
            Self::WakeTimeout => (StatusCode::SERVICE_UNAVAILABLE, "tenant wake timed out"),
            Self::WakeFailed => (StatusCode::SERVICE_UNAVAILABLE, "tenant wake failed"),
            Self::WakeCapacity => (
                StatusCode::SERVICE_UNAVAILABLE,
                "tenant wake capacity exhausted",
            ),
            Self::WorkerUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "worker unavailable on this generation",
            ),
            Self::WsCapacity => (
                StatusCode::SERVICE_UNAVAILABLE,
                "websocket session capacity exhausted",
            ),
            Self::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
            Self::UpstreamConnect => (StatusCode::BAD_GATEWAY, "cannot reach worker"),
            Self::UpstreamTimeout => (StatusCode::GATEWAY_TIMEOUT, "worker response timeout"),
            Self::UpstreamError => (StatusCode::BAD_GATEWAY, "worker protocol error"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal ingress error"),
        }
    }

    /// Platform log-event code (typed, stable). None for pre-route /
    /// client-fault errors that are not worker-attributable events.
    pub fn platform_code(&self) -> Option<crate::logs::PlatformCode> {
        use crate::logs::PlatformCode as C;
        match self {
            Self::BadHost | Self::UnknownHost | Self::PayloadTooLarge => None,
            Self::TenantUnavailable(_) => Some(C::TenantUnavailable),
            Self::CircuitOpen => Some(C::CircuitOpen),
            Self::WakeTimeout => Some(C::WakeTimeout),
            Self::WakeFailed => Some(C::WakeFailed),
            Self::WakeCapacity => Some(C::WakeCapacity),
            Self::WsCapacity => Some(C::WsCapacity),
            Self::WorkerUnavailable => Some(C::WorkerUnavailable),
            Self::UpstreamConnect => Some(C::UpstreamConnect),
            Self::UpstreamTimeout => Some(C::UpstreamTimeout),
            Self::UpstreamError => Some(C::UpstreamError),
            Self::Internal => Some(C::Internal),
        }
    }

    pub fn metric_reason(&self) -> Option<&'static str> {
        match self {
            Self::UpstreamConnect => Some("connect"),
            Self::UpstreamTimeout => Some("timeout"),
            Self::UpstreamError => Some("upstream"),
            Self::WorkerUnavailable => Some("worker_unavailable"),
            Self::TenantUnavailable(_) => Some("tenant_unavailable"),
            Self::CircuitOpen => Some("circuit_open"),
            Self::WakeTimeout => Some("wake_timeout"),
            Self::WakeFailed => Some("wake_failed"),
            Self::WakeCapacity => Some("wake_capacity"),
            Self::WsCapacity => Some("ws_capacity"),
            Self::PayloadTooLarge => Some("payload_too_large"),
            _ => Some("error"),
        }
    }
}

impl IntoResponse for IngressError {
    fn into_response(self) -> Response {
        let (status, detail) = self.status_and_detail();
        let code = self.to_string();
        let mut response = (
            status,
            axum::Json(serde_json::json!({
                "error": { "code": code, "message": detail }
            })),
        )
            .into_response();
        response.headers_mut().insert(
            "x-vrt-error",
            HeaderValue::from_str(&code).unwrap_or(HeaderValue::from_static("ingress")),
        );
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_static(if status == StatusCode::SERVICE_UNAVAILABLE {
                "1"
            } else {
                "0"
            }),
        );
        response
    }
}

/// Axum router over `state`; serve with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn router(state: Arc<IngressState>) -> Router {
    Router::new().fallback(any(proxy)).with_state(state)
}

/// Serve the ingress on `listener` until `shutdown` resolves; stops
/// accepting first so in-flight proxied requests can drain.
pub async fn serve(
    listener: TcpListener,
    state: Arc<IngressState>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

type UpstreamBody = http_body_util::combinators::UnsyncBoxBody<Bytes, axum::Error>;

async fn proxy(
    State(state): State<Arc<IngressState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
) -> Result<Response, IngressError> {
    // Hard boundary: a panic inside the handler must surface as a typed
    // 500 — never as an aborted connection task (which shows up client-
    // side as a bare TCP reset with no typed error).
    use futures_util::FutureExt;
    let inner = std::panic::AssertUnwindSafe(proxy_inner(state, peer, req));
    match inner.catch_unwind().await {
        Ok(result) => result,
        Err(_) => {
            tracing::error!("ingress handler panicked");
            Err(IngressError::Internal)
        }
    }
}

async fn proxy_inner(
    state: Arc<IngressState>,
    peer: SocketAddr,
    req: Request,
) -> Result<Response, IngressError> {
    if state
        .panic_once
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        panic!("ingress panic injection");
    }
    // Host resolution: h1 carries `Host`; h2/h3 carry `:authority`,
    // which hyper maps into `uri.authority()` (no Host header is
    // synthesized). Per RFC 9110 §7.2 an explicit Host header wins
    // when both are present.
    let host_raw = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| req.uri().authority().map(|a| a.as_str().to_string()))
        .ok_or(IngressError::BadHost)?;
    let host = normalize_host(&host_raw).ok_or(IngressError::BadHost)?;
    let routes = state.routes.load();
    let target = routes
        .lookup(&host)
        .or_else(|| {
            state
                .dev_catch_all
                .load(std::sync::atomic::Ordering::Relaxed)
                .then(|| routes.catch_all())
                .flatten()
        })
        .ok_or(IngressError::UnknownHost)?;

    // B12: request accounting starts once routing resolves to a
    // concrete platform worker_id — pre-route failures (bad/unknown
    // host) have no worker label and are not worker metrics.
    let mut metric = crate::metrics::RequestMetric::begin(&target.worker_id);
    let logs = state.logs.get().cloned();
    match handle_routed(state, peer, req, host.as_str(), &target, &mut metric).await {
        Ok(r) => Ok(r),
        Err(e) => {
            metric.finish(None, e.metric_reason());
            // Platform-authored record (never tenant text); the hub
            // coalesces per (worker, code) and bounds everything, so a
            // flood of failing requests cannot grow memory or slow
            // this path beyond one short critical section.
            if let (Some(hub), Some(code)) = (logs, e.platform_code()) {
                hub.platform_event(
                    &target.worker_id,
                    code,
                    Some(e.status_and_detail().0.as_u16()),
                    &format!("request failed: {e}"),
                );
            }
            Err(e)
        }
    }
}

/// Scale-to-zero: map a wake failure onto its typed 503.
fn wake_error_to_ingress(e: crate::hibernate::WakeError) -> IngressError {
    use crate::hibernate::WakeError as W;
    match e {
        W::Timeout => IngressError::WakeTimeout,
        W::Failed(_) => IngressError::WakeFailed,
        W::Capacity => IngressError::WakeCapacity,
        W::CircuitOpen => IngressError::CircuitOpen,
    }
}

/// If the tenant is hibernating / hibernated / waking, join the
/// single-flight wake and wait for the actor's verdict (held-request
/// accounting applies). `Ok(true)` = a wake was awaited and succeeded;
/// `Ok(false)` = the tenant was not asleep (nothing done — the only
/// path taken when scale-to-zero is off).
async fn wake_if_asleep(
    state: &IngressState,
    runtime: &Arc<crate::supervisor::TenantRuntime>,
    hold_bytes: usize,
) -> Result<bool, IngressError> {
    let s = runtime.state();
    if !(s.is_asleep() || s == crate::state::TenantState::Hibernating) {
        return Ok(false);
    }
    runtime
        .await_wake(
            hold_bytes,
            state.switch_hold_max_reqs,
            state.switch_hold_max_bytes,
            crate::hibernate::WakeSourceKind::Http,
        )
        .await
        .map(|()| true)
        .map_err(wake_error_to_ingress)
}

/// Routed request path — the caller owns `metric` so EVERY error exit
/// is accounted with the typed reason.
async fn handle_routed(
    state: Arc<IngressState>,
    peer: SocketAddr,
    req: Request,
    host: &str,
    target: &crate::route::RouteTarget,
    metric: &mut crate::metrics::RequestMetric,
) -> Result<Response, IngressError> {
    let runtime = state
        .registry
        .get(&target.tenant_id)
        .ok_or(IngressError::TenantUnavailable("tenant not running"))?;
    // T37b-r2: `can_serve` — Ready AND SwitchFailed both serve the old
    // generation. Matching `Ready` alone 503'd the ENTIRE tenant once a
    // bad spec latched SwitchFailed — one worker's launch failure took
    // down every sibling worker (the T37b-r2 blast radius).
    // Scale-to-zero: every request is activity (cheap atomics; a no-op
    // when the feature is off), and a sleeping tenant is woken through
    // the single-flight wake path before the normal resolve.
    runtime.touch();
    // WebSocket: decide once, up front. The permit is reserved BEFORE any
    // wake / upstream work so an over-limit client cannot wake a sleeping
    // tenant or open an upstream connection.
    let is_ws = is_websocket_upgrade(req.method(), req.version(), req.headers());
    let mut ws_permit = None;
    if is_ws {
        match WsPermit::try_acquire(&state, &target.tenant_id, &target.worker_id) {
            Ok(p) => ws_permit = Some(p),
            Err(denied) => {
                ws_outcome(&target.worker_id, denied.outcome());
                return Err(IngressError::WsCapacity);
            }
        }
    }
    let hold_bytes = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if wake_if_asleep(&state, &runtime, hold_bytes).await? {
        // Woken (or the hibernation attempt was aborted): fall through.
    }
    match runtime.state() {
        s if s.can_serve() => {}
        crate::state::TenantState::CircuitOpen => return Err(IngressError::CircuitOpen),
        _ => return Err(IngressError::TenantUnavailable("tenant not ready")),
    }

    // Slot acquire with one re-lookup: between `endpoint_slot` and
    // `acquire` the generation may close (switch/drain) — retry once on
    // the freshly swapped map.
    // W4-r2/MIG-11: while the serialized DO switch holds the gate,
    // route misses are HELD through the shared bounded wait
    // (`TenantRuntime::resolve_through_switch`) instead of an
    // immediate 503 — the gap is seconds, and an asset fetch on a
    // non-DO sibling must not die because one worker's class dirs are
    // switching. Chunked/no-length requests charge 0 bytes — the body
    // is never read while held (TCP backpressure bounds the socket
    // buffer). The hold permit drops inside the resolver — after
    // `slot`+`guard` are acquired but BEFORE the body is streamed — so
    // a held request frees its slot exactly once on every path,
    // including a future dropped mid-await (W4-r3 P0-1).
    // MIG-11-r2: ONE deadline shared by the initial resolve AND the
    // post-`acquire()`-failure re-lookup — a fresh budget per pass
    // would double the worst-case hold under a switch storm.
    let hold_deadline = tokio::time::Instant::now() + state.switch_hold_budget;
    let (slot, guard) = {
        let mut attempt = 0;
        loop {
            let resolved = match runtime
                .resolve_through_switch(
                    hold_bytes,
                    hold_deadline.saturating_duration_since(tokio::time::Instant::now()),
                    state.switch_hold_max_reqs,
                    state.switch_hold_max_bytes,
                    |rt| rt.endpoint_slot(&target.worker_id),
                )
                .await
            {
                Ok(v) => v,
                Err(reason) => {
                    crate::metrics::registry().inc(
                        "vrt_host_switch_hold_rejects_total",
                        &[
                            ("tenant", &target.tenant_id),
                            (
                                "reason",
                                match reason {
                                    crate::supervisor::SwitchHoldReject::Capacity => "capacity",
                                    crate::supervisor::SwitchHoldReject::Timeout => "timeout",
                                },
                            ),
                        ],
                        1.0,
                    );
                    return Err(IngressError::WorkerUnavailable);
                }
            };
            let slot = match resolved {
                crate::supervisor::SwitchResolve::Hit(slot) => slot,
                // MIG-11-r2: both miss classes keep the existing
                // `worker_unavailable` 503 — ingress never reported a
                // typed "not declared" and the tenant-level outage is
                // the same shape to clients.
                crate::supervisor::SwitchResolve::NotDeclared
                | crate::supervisor::SwitchResolve::NoServing => {
                    // The tenant may have gone to sleep between the
                    // state check and the resolve: wake, then retry.
                    attempt += 1;
                    if attempt < 3 && wake_if_asleep(&state, &runtime, hold_bytes).await? {
                        continue;
                    }
                    return Err(IngressError::WorkerUnavailable);
                }
            };
            match slot.acquire() {
                Some(guard) => break (slot, guard),
                None => {
                    attempt += 1;
                    // A slot closed by a hibernation attempt: wake
                    // (aborts the attempt / relaunches) and retry.
                    if attempt < 3 && wake_if_asleep(&state, &runtime, hold_bytes).await? {
                        continue;
                    }
                    if attempt >= 2 {
                        return Err(IngressError::WorkerUnavailable);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    };

    // Content-length fast reject before streaming.
    if let Some(len) = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
    {
        if len > state.max_body_bytes {
            return Err(IngressError::PayloadTooLarge);
        }
    }

    let mut req = req;
    let client_upgrade = if is_ws {
        match req.extensions_mut().remove::<hyper::upgrade::OnUpgrade>() {
            Some(u) => Some(u),
            None => {
                // The serving stack always attaches it for h1; absence
                // means we cannot complete the handshake - fail closed
                // before the worker is told to accept a session.
                ws_outcome(&target.worker_id, "client_upgrade_failed");
                return Err(IngressError::Internal);
            }
        }
    } else {
        None
    };
    let (parts, body) = req.into_parts();
    let limited = Limited::new(body, state.max_body_bytes);
    let counted = crate::metrics::CountedBody::new(
        limited,
        target.worker_id.clone(),
        crate::metrics::ByteDir::Request,
    );
    let upstream_body: UpstreamBody = counted.map_err(axum::Error::new).boxed_unsync();

    let mut upstream_req = hyper::Request::builder()
        .method(parts.method.clone())
        .uri(path_and_query(&parts.uri))
        .body(upstream_body)
        .map_err(|_| IngressError::UpstreamError)?;
    *upstream_req.headers_mut() = scrubbed_headers(
        &parts.headers,
        &host,
        &target,
        peer,
        &state.forwarded_proto,
        &state.client_ip,
        state
            .geoip
            .as_deref()
            .map(|g| g as &dyn crate::geoip::GeoLookup),
        parts.version,
        is_ws,
    );

    let endpoint = slot.endpoint().clone();
    let log_hub = state.logs.get().cloned();
    // `request_timeout` bounds connect + response headers only; a
    // WebSocket tunnel that follows a 101 is governed by the ws limits.
    let connect_timeout = state.connect_timeout;
    let response = tokio::time::timeout(state.request_timeout, async move {
        let io = tokio::time::timeout(connect_timeout, endpoint.connect())
            .await
            .map_err(|_| IngressError::UpstreamConnect)?
            .map_err(|_| IngressError::UpstreamConnect)?;
        upstream_call(io, upstream_req).await
    })
    .await
    .map_err(|_| IngressError::UpstreamTimeout)??;

    if is_ws {
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            return finish_ws_upgrade(
                &state,
                response,
                client_upgrade,
                guard,
                ws_permit,
                &target.worker_id,
                metric,
            );
        }
        // Worker declined the upgrade: ordinary response below. Release
        // the session reservation now; the body keeps the guard.
        ws_outcome(&target.worker_id, "rejected_upstream");
        drop(ws_permit.take());
    } else if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        // 101 to a request that never asked for a tunnel: protocol
        // violation (and we hold no OnUpgrade) - fail closed.
        return Err(IngressError::UpstreamError);
    }

    let (mut resp_parts, resp_body) = response.into_parts();
    // T31c: the in-workerd router marks asset-served responses before
    // scrubbing removes the internal header. The marker is trusted
    // because only the facade-router system worker can emit it — it
    // strips the header from every user-worker response it proxies.
    // The marker is honored only when this route's socket fronts a
    // router — a legacy worker that forges the header cannot relabel
    // its own Worker metrics as asset hits.
    let served_by_assets = target.assets_first
        && resp_parts
            .headers
            .get("x-vrt-asset-route")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == "assets")
            .unwrap_or(false);
    // T31f/F0.4: the router emits a bounded `x-vrt-route-error` enum on
    // its fail-closed 503s — map it onto the metric's `reason` label.
    // Whitelist-gated: an unlisted value folds to "other", a missing
    // header to "none"; the header is scrubbed before the response
    // leaves and forgeable user responses never reach this path (the
    // router strips it, like the route marker itself).
    let route_error_reason: &'static str = if served_by_assets {
        match resp_parts
            .headers
            .get("x-vrt-route-error")
            .and_then(|v| v.to_str().ok())
        {
            Some(v) if ROUTE_ERROR_REASONS.contains(&v) => {
                // SAFETY-of-cardinality: values come from a fixed const
                // list — cast to 'static is safe because the header
                // value is compared against string literals and the
                // literal is stored, not the input.
                match v {
                    "route_index_rpc_failed" => "route_index_rpc_failed",
                    "route_index_invalid" => "route_index_invalid",
                    "route_index_digest_mismatch" => "route_index_digest_mismatch",
                    "route_index_timeout" => "route_index_timeout",
                    "can_fetch_rpc_failed" => "can_fetch_rpc_failed",
                    "can_fetch_timeout" => "can_fetch_timeout",
                    _ => unreachable!(),
                }
            }
            Some(_) => "other",
            None => "none",
        }
    } else {
        "none"
    };
    scrub_response_headers(&mut resp_parts.headers);
    let counted = crate::metrics::CountedBody::new(
        resp_body,
        target.worker_id.clone(),
        crate::metrics::ByteDir::Response,
    );
    let guarded = GuardedBody {
        inner: counted,
        _guard: guard,
    };
    let status = resp_parts.status;
    // 5xx from the worker counts as a surfaced exception (the runtime
    // boundary turns uncaught exceptions/timeouts into 5xx — finer
    // classification needs workerd internals; see the metrics doc).
    if served_by_assets {
        if route_error_reason != "none" && route_error_reason != "other" {
            if let (Some(hub), Some(code)) = (
                &log_hub,
                crate::logs::PlatformCode::from_route_error(route_error_reason),
            ) {
                hub.platform_event(
                    &target.worker_id,
                    code,
                    Some(status.as_u16()),
                    &format!("router fail-closed: {route_error_reason}"),
                );
            }
        }
        metric.finish_assets_route(Some(status), route_error_reason);
    } else {
        let exc = (status.is_server_error()).then_some("worker_error");
        metric.finish(Some(status), exc);
    }
    let mut response = Response::new(Body::new(guarded));
    *response.status_mut() = status;
    *response.headers_mut() = resp_parts.headers;
    Ok(response)
}

/// Cloudflare-compatible upgrade detection: HTTP/1.1 `GET`, a single
/// `Upgrade: websocket` token (case-insensitive), a `Connection` header
/// listing `upgrade`, and no request-body framing. Everything else,
/// including other protocols (`h2c`) and mixed lists, is NOT an
/// upgrade and keeps being stripped (fail closed).
fn is_websocket_upgrade(method: &Method, version: axum::http::Version, h: &HeaderMap) -> bool {
    if method != Method::GET || version != axum::http::Version::HTTP_11 {
        return false;
    }
    let has_body = h
        .get(header::CONTENT_LENGTH)
        .map(|v| v.as_bytes() != b"0")
        .unwrap_or(false)
        || h.contains_key(header::TRANSFER_ENCODING);
    if has_body {
        return false;
    }
    let tokens = |name: header::HeaderName| -> Option<Vec<String>> {
        let mut out = Vec::new();
        for v in h.get_all(name) {
            for t in v.to_str().ok()?.split(',') {
                let t = t.trim();
                if !t.is_empty() {
                    out.push(t.to_ascii_lowercase());
                }
            }
        }
        Some(out)
    };
    let upgrade_ok = matches!(tokens(header::UPGRADE).as_deref(), Some([t]) if t == "websocket");
    let connection_ok = tokens(header::CONNECTION)
        .map(|ts| ts.iter().any(|t| t == "upgrade"))
        .unwrap_or(false);
    upgrade_ok && connection_ok
}

fn ws_outcome(worker_id: &str, outcome: &'static str) {
    crate::metrics::registry().inc(
        "vrt_host_ws_sessions_total",
        &[("worker_id", worker_id), ("outcome", outcome)],
        1.0,
    );
}

/// RAII reservation against the per-ingress WebSocket session cap.
/// Taken before any upstream work, released on every exit path (and
/// held by the tunnel task for the whole session).
struct WsPermit {
    active: Arc<AtomicUsize>,
    tenants: Arc<Mutex<HashMap<String, usize>>>,
    tenant_id: String,
    worker_id: String,
    counted: bool,
}

/// Which cap refused a [`WsPermit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WsDenied {
    /// Ingress-wide cap.
    Global,
    /// Per-tenant cap.
    Tenant,
}

impl WsDenied {
    fn outcome(self) -> &'static str {
        match self {
            WsDenied::Global => "rejected_capacity",
            WsDenied::Tenant => "rejected_tenant_capacity",
        }
    }
}

impl WsPermit {
    /// Reserves one slot against BOTH the global and the per-tenant cap.
    /// Either refusal rolls back whatever was already reserved.
    fn try_acquire(
        state: &IngressState,
        tenant_id: &str,
        worker_id: &str,
    ) -> Result<Self, WsDenied> {
        // Reserve-then-check: never exceeds the cap under races.
        let prev = state.ws_active.fetch_add(1, Ordering::SeqCst);
        if prev >= state.ws_max_sessions {
            state.ws_active.fetch_sub(1, Ordering::SeqCst);
            return Err(WsDenied::Global);
        }
        {
            // A poisoned lock only means another thread panicked while
            // holding plain counters; the counts themselves stay valid.
            let mut map = state
                .ws_tenant_active
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let n = map.entry(tenant_id.to_string()).or_insert(0);
            if *n >= state.ws_max_per_tenant {
                // ws_max_per_tenant >= 1, so an existing entry here is
                // always > 0 and a fresh zero entry is never refused.
                drop(map);
                state.ws_active.fetch_sub(1, Ordering::SeqCst);
                return Err(WsDenied::Tenant);
            }
            *n += 1;
        }
        Ok(Self {
            active: state.ws_active.clone(),
            tenants: state.ws_tenant_active.clone(),
            tenant_id: tenant_id.to_string(),
            worker_id: worker_id.to_string(),
            counted: false,
        })
    }

    /// The session is live: surface it on the active-sessions gauge.
    fn activate(&mut self) {
        if !self.counted {
            self.counted = true;
            crate::metrics::registry().add(
                "vrt_host_ws_active_sessions",
                &[("worker_id", &self.worker_id)],
                1.0,
            );
        }
    }
}

impl Drop for WsPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        let mut map = self.tenants.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = map.get_mut(&self.tenant_id) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                map.remove(&self.tenant_id);
            }
        }
        drop(map);
        if self.counted {
            crate::metrics::registry().add(
                "vrt_host_ws_active_sessions",
                &[("worker_id", &self.worker_id)],
                -1.0,
            );
        }
    }
}

/// Upstream answered 101: answer the client with a normalized 101 and
/// hand both upgraded halves to the tunnel task.
fn finish_ws_upgrade(
    state: &IngressState,
    mut response: hyper::Response<hyper::body::Incoming>,
    client_upgrade: Option<hyper::upgrade::OnUpgrade>,
    guard: InFlightGuard,
    permit: Option<WsPermit>,
    worker_id: &str,
    metric: &mut crate::metrics::RequestMetric,
) -> Result<Response, IngressError> {
    let (Some(client_upgrade), Some(mut permit)) = (client_upgrade, permit) else {
        return Err(IngressError::Internal);
    };
    // The worker must confirm the protocol we asked for.
    let confirmed = response
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);
    if !confirmed {
        return Err(IngressError::UpstreamError);
    }
    let upstream_upgrade = hyper::upgrade::on(&mut response);
    let mut headers = std::mem::take(response.headers_mut());
    scrub_upgrade_response_headers(&mut headers);

    permit.activate();
    ws_outcome(worker_id, "upgraded");
    metric.finish(Some(StatusCode::SWITCHING_PROTOCOLS), None);
    let tunnel = WsTunnel {
        worker_id: worker_id.to_string(),
        idle_timeout: state.ws_idle_timeout,
        max_duration: state.ws_max_duration,
        upgrade_wait: state.request_timeout,
    };
    tokio::spawn(tunnel.run(client_upgrade, upstream_upgrade, guard, permit));

    let mut out = Response::new(Body::empty());
    *out.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    *out.headers_mut() = headers;
    Ok(out)
}

/// 101 headers: same scrub as any response, then re-add the two
/// hop-by-hop headers the handshake requires, with canonical values.
fn scrub_upgrade_response_headers(headers: &mut HeaderMap) {
    scrub_response_headers(headers);
    headers.remove(header::CONTENT_LENGTH);
    headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
}

/// Wraps the client half of the tunnel: records last-activity time and
/// byte counts for BOTH directions (every byte either way crosses it).
struct ActivityIo<T> {
    inner: T,
    started: Instant,
    last_ms: Arc<AtomicU64>,
    from_client: Arc<AtomicU64>,
    to_client: Arc<AtomicU64>,
}

impl<T> ActivityIo<T> {
    fn touch(&self) {
        self.last_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ActivityIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let n = (buf.filled().len() - before) as u64;
            if n > 0 {
                self.from_client.fetch_add(n, Ordering::Relaxed);
                self.touch();
            }
        }
        poll
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ActivityIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            if *n > 0 {
                self.to_client.fetch_add(*n as u64, Ordering::Relaxed);
                self.touch();
            }
        }
        poll
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Bounded byte tunnel between the two upgraded halves.
struct WsTunnel {
    worker_id: String,
    idle_timeout: Duration,
    max_duration: Duration,
    /// Bound on awaiting each side's upgrade (the client side completes
    /// only after our 101 is flushed).
    upgrade_wait: Duration,
}

impl WsTunnel {
    /// Owns `guard` and `permit` for the whole session, so tenant
    /// hibernation / drain observe it and the session cap is exact.
    /// Every exit, including a panic unwind, drops both.
    async fn run(
        self,
        client: hyper::upgrade::OnUpgrade,
        upstream: hyper::upgrade::OnUpgrade,
        _guard: InFlightGuard,
        _permit: WsPermit,
    ) {
        use hyper_util::rt::TokioIo;
        let reg = crate::metrics::registry();
        let w = self.worker_id.as_str();
        let client = match tokio::time::timeout(self.upgrade_wait, client).await {
            Ok(Ok(u)) => TokioIo::new(u),
            _ => {
                ws_outcome(w, "client_upgrade_failed");
                return;
            }
        };
        let mut upstream = match tokio::time::timeout(self.upgrade_wait, upstream).await {
            Ok(Ok(u)) => TokioIo::new(u),
            _ => {
                ws_outcome(w, "upstream_upgrade_failed");
                return;
            }
        };
        let started = Instant::now();
        let last_ms = Arc::new(AtomicU64::new(0));
        let from_client = Arc::new(AtomicU64::new(0));
        let to_client = Arc::new(AtomicU64::new(0));
        let mut client = ActivityIo {
            inner: client,
            started,
            last_ms: last_ms.clone(),
            from_client: from_client.clone(),
            to_client: to_client.clone(),
        };

        let idle_ms = self.idle_timeout.as_millis() as u64;
        let idle_watch = async {
            loop {
                let last = last_ms.load(Ordering::Relaxed);
                let now = started.elapsed().as_millis() as u64;
                let idle_for = now.saturating_sub(last);
                if idle_for >= idle_ms {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(idle_ms - idle_for)).await;
            }
        };
        let reason = tokio::select! {
            r = tokio::io::copy_bidirectional(&mut client, &mut upstream) => match r {
                Ok(_) => "closed",
                Err(_) => "error",
            },
            _ = idle_watch => "idle_timeout",
            _ = tokio::time::sleep(self.max_duration) => "max_duration",
        };
        // Dropping both halves closes the sockets.
        drop(client);
        drop(upstream);
        reg.inc(
            "vrt_host_ws_session_end_total",
            &[("worker_id", w), ("reason", reason)],
            1.0,
        );
        for (dir, n) in [
            ("client_to_worker", from_client.load(Ordering::Relaxed)),
            ("worker_to_client", to_client.load(Ordering::Relaxed)),
        ] {
            if n > 0 {
                reg.inc(
                    "vrt_host_ws_bytes_total",
                    &[("worker_id", w), ("dir", dir)],
                    n as f64,
                );
            }
        }
    }
}

fn path_and_query(uri: &axum::http::Uri) -> String {
    uri.path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string()
}

/// Which IP the local GeoIP lookup is allowed to describe. The TCP
/// peer is only a valid lookup subject when it IS the client: policy
/// `none`, or a peer outside the trusted CIDR set (its XFF is
/// ignored — it's just another client). A TRUSTED proxy whose XFF
/// resolves to nothing must not have its own datacenter IP looked
/// up: that would re-inject the proxy's geo as edge-verified
/// `cf-ipcountry`/`request.cf` — confidently wrong. No client
/// identity ⇒ `None` ⇒ no local geo.
fn geoip_lookup_ip(
    client_ip_policy: &ClientIpPolicy,
    peer: &std::net::IpAddr,
    inbound: &HeaderMap,
) -> Option<std::net::IpAddr> {
    let peer_is_untrusted_client = client_ip_policy
        .trusted_cidrs()
        .is_none_or(|cidrs| !cidrs.iter().any(|c| c.contains(peer)));
    resolve_client_ip(client_ip_policy, peer, inbound)
        .or_else(|| peer_is_untrusted_client.then(|| canon_ip(*peer)))
}

#[allow(clippy::too_many_arguments)]
fn scrubbed_headers(
    inbound: &HeaderMap,
    host: &str,
    target: &crate::route::RouteTarget,
    peer: SocketAddr,
    proto_policy: &ForwardedProto,
    client_ip_policy: &ClientIpPolicy,
    geoip: Option<&dyn crate::geoip::GeoLookup>,
    version: axum::http::Version,
    ws_upgrade: bool,
) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(inbound.len() + 8);
    for (name, value) in inbound {
        let lower = name.as_str();
        if STRIP_REQUEST.contains(&lower) || STRIP_PREFIXES.iter().any(|p| lower.starts_with(p)) {
            continue;
        }
        if name == header::HOST {
            continue; // rewritten below
        }
        out.append(name.clone(), value.clone());
    }
    out.insert(
        header::HOST,
        HeaderValue::from_str(host).unwrap_or(HeaderValue::from_static("localhost")),
    );
    out.insert(
        "x-vrt-tenant-id",
        HeaderValue::from_str(&target.tenant_id).unwrap_or(HeaderValue::from_static("invalid")),
    );
    out.insert(
        "x-vrt-worker-id",
        HeaderValue::from_str(&target.worker_id).unwrap_or(HeaderValue::from_static("invalid")),
    );
    out.insert(
        "x-forwarded-host",
        HeaderValue::from_str(host).unwrap_or(HeaderValue::from_static("invalid")),
    );
    // MIG-7: workerd's `forwardedProtoHeader` reads this — the scheme
    // the worker sees in `request.url`. Only an exact `https` from a
    // trusted peer (or `fixed-https` mode) promotes; everything else —
    // forged values, garbage, missing — fails closed to `http`.
    let https = match proto_policy {
        ForwardedProto::FixedHttp => false,
        ForwardedProto::FixedHttps => true,
        ForwardedProto::TrustedHeader { .. } => {
            proto_policy.peer_is_trusted(&peer.ip())
                && inbound
                    .get("x-forwarded-proto")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim().eq_ignore_ascii_case("https"))
                    .unwrap_or(false)
        }
    };
    out.insert(
        "x-forwarded-proto",
        HeaderValue::from_static(if https { "https" } else { "http" }),
    );
    // MIG-7: a trusted front proxy may pass its `x-forwarded-for`
    // chain through (it already saw the real client); every other
    // source is overwritten with the TCP peer — client-supplied
    // chains can never reach the worker.
    let mut forwarded = false;
    if proto_policy.peer_is_trusted(&peer.ip()) {
        for value in inbound.get_all("x-forwarded-for").iter() {
            out.append("x-forwarded-for", value.clone());
            forwarded = true;
        }
    }
    if !forwarded {
        out.insert(
            "x-forwarded-for",
            HeaderValue::from_str(&peer.ip().to_string())
                .unwrap_or(HeaderValue::from_static("127.0.0.1")),
        );
    }
    // MIG-9: Cloudflare-compatible client identity — injected ONLY
    // from the typed trusted-source decision above; the inbound copy
    // was stripped at the top, so a client (or any non-trusted peer)
    // can never reach the worker with a forged `cf-connecting-ip`.
    if let Some(ip) = resolve_client_ip(client_ip_policy, &peer.ip(), inbound) {
        out.insert(
            "cf-connecting-ip",
            HeaderValue::from_str(&ip.to_string())
                .unwrap_or(HeaderValue::from_static("127.0.0.1")),
        );
    }
    // MIG-10: trusted CF metadata — ONLY behind the verified
    // trusted-proxy → Cloudflare-edge chain, every field validated.
    // Inbound copies were stripped at the top; a client (or any
    // non-trusted peer) can never forge country/colo/ray into the
    // worker. `cf-visitor` is synthesized from our own scheme verdict
    // (the inbound JSON is never echoed — its shape varies by CF
    // product and carries fields we do not model).
    let mut cf = cf_metadata(client_ip_policy, &peer.ip(), inbound);
    // Local GeoIP: fill the fields a verified upstream chain did not
    // provide. The lookup IP is the client identity the typed policy
    // derived — falling back to the TCP peer, which in `none` mode
    // (no trusted proxy) IS the client as far as this edge can
    // verify. A database record can never override verified upstream
    // data and never supplies `cf-connecting-ip`/`colo`/`ray`.
    if let Some(geo) = geoip {
        // The TCP peer is only a valid lookup subject when it IS the
        // client: policy `none`, or a peer outside the trusted CIDR
        // set (its XFF is ignored — it's just another client). A
        // TRUSTED proxy whose XFF resolves to nothing must not have
        // its own datacenter IP looked up: that would re-inject the
        // proxy's geo as edge-verified `cf-ipcountry`/`request.cf` —
        // confidently wrong. No client identity ⇒ no local geo.
        if let Some(rec) = geoip_lookup_ip(client_ip_policy, &peer.ip(), inbound)
            .and_then(|ip| geo.lookup(ip))
        {
            cf.merge_geo(&rec);
        }
    }
    if let Some(ray) = &cf.ray {
        if let Ok(v) = HeaderValue::from_str(ray) {
            out.insert("cf-ray", v);
        }
    }
    if let Some(country) = &cf.country {
        if let Ok(v) = HeaderValue::from_str(country) {
            out.insert("cf-ipcountry", v);
        }
    }
    // MIG-31: the remaining visitor-location headers, re-injected from
    // the same validated metadata. `from_bytes` (not `from_str`) so a
    // UTF-8 city/region name survives; every value already passed the
    // control-char and length checks.
    for (name, value) in [
        ("cf-ipcity", &cf.city),
        ("cf-ipcontinent", &cf.continent),
        ("cf-region", &cf.region),
        ("cf-region-code", &cf.region_code),
        ("cf-metro-code", &cf.metro_code),
        ("cf-postal-code", &cf.postal_code),
        ("cf-timezone", &cf.timezone),
        ("cf-iplatitude", &cf.latitude),
        ("cf-iplongitude", &cf.longitude),
    ] {
        if let Some(v) = value {
            if let Ok(hv) = HeaderValue::from_bytes(v.as_bytes()) {
                out.insert(name, hv);
            }
        }
    }
    if upstream_is_cloudflare(client_ip_policy, &peer.ip(), inbound) {
        if let Ok(v) = HeaderValue::from_str(&format!(
            "{{\"scheme\":\"{}\"}}",
            if https { "https" } else { "http" }
        )) {
            out.insert("cf-visitor", v);
        }
    }
    // MIG-10: cfBlob — ALWAYS emitted so `request.cf` is host-defined:
    // a minimal blob on untrusted paths still overrides workerd's
    // unix-socket peer credentials (clientPid/clientUid), which must
    // never reach tenant code.
    if let Ok(v) = HeaderValue::from_str(&cf_blob_json(&cf, http_protocol(version))) {
        out.insert(CF_BLOB_HEADER, v);
    }
    // WebSocket: the two hop-by-hop headers were stripped above like any
    // other; for a verified upgrade request (see `is_websocket_upgrade`)
    // we write canonical values; the client's originals never pass.
    if ws_upgrade {
        out.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        out.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    }
    out
}

fn scrub_response_headers(headers: &mut HeaderMap) {
    for name in STRIP_REQUEST {
        headers.remove(*name);
    }
    // Internal prefixes are platform-owned in both directions — a worker
    // must not forge `x-vrt-error` or identity markers to the client.
    let internal: Vec<axum::http::HeaderName> = headers
        .keys()
        .filter(|n| STRIP_PREFIXES.iter().any(|p| n.as_str().starts_with(p)))
        .cloned()
        .collect();
    for name in internal {
        headers.remove(name);
    }
}

async fn upstream_call(
    io: crate::endpoint::UpstreamIo,
    req: hyper::Request<UpstreamBody>,
) -> Result<hyper::Response<hyper::body::Incoming>, IngressError> {
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake::<_, UpstreamBody>(hyper_util::rt::TokioIo::new(io))
            .await
            .map_err(|_| IngressError::UpstreamConnect)?;
    // `with_upgrades` is inert for ordinary exchanges; it lets a 101
    // hand the raw socket over via `hyper::upgrade::on`.
    tokio::spawn(async move {
        let _ = conn.with_upgrades().await;
    });
    sender
        .ready()
        .await
        .map_err(|_| IngressError::UpstreamConnect)?;
    sender
        .send_request(req)
        .await
        .map_err(|_| IngressError::UpstreamError)
}

/// Response body that keeps the generation's in-flight guard until the
/// last frame is consumed — a draining process sees the true end of its
/// final request.
struct GuardedBody {
    inner: crate::metrics::CountedBody<hyper::body::Incoming>,
    _guard: InFlightGuard,
}

impl http_body::Body for GuardedBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod platform_code_tests {
    use super::*;
    use crate::logs::PlatformCode as C;

    #[test]
    fn platform_log_code_is_the_documented_error_code_not_the_metric_reason() {
        // Metric label stays `timeout`; the log code is `upstream_timeout`.
        assert_eq!(IngressError::UpstreamTimeout.metric_reason(), Some("timeout"));
        let e = IngressError::UpstreamTimeout;
        assert_eq!(e.platform_code(), Some(C::UpstreamTimeout));
        assert_eq!(e.platform_code().unwrap().as_str(), e.to_string());
        assert_eq!(e.status_and_detail().0, StatusCode::GATEWAY_TIMEOUT);
    }

    #[test]
    fn worker_attributable_errors_have_codes_matching_the_wire_code() {
        for e in [
            IngressError::UpstreamConnect,
            IngressError::UpstreamError,
            IngressError::WorkerUnavailable,
            IngressError::CircuitOpen,
            IngressError::Internal,
        ] {
            let c = e.platform_code().expect("has code");
            assert_eq!(c.as_str(), e.to_string());
            assert!(C::ALL.contains(&c));
        }
        assert_eq!(
            IngressError::TenantUnavailable("x").platform_code(),
            Some(C::TenantUnavailable)
        );
        for e in [
            IngressError::BadHost,
            IngressError::UnknownHost,
            IngressError::PayloadTooLarge,
        ] {
            assert_eq!(e.platform_code(), None);
        }
    }
}

#[cfg(test)]
mod cf_blob_tests {
    //! MIG-10: cfBlob / trusted CF metadata — forgery, trust gating,
    //! field validation, and the clientPid guarantee.
    use super::*;
    use std::str::FromStr;

    fn cidr(t: &str) -> vrt_config::Cidr {
        vrt_config::Cidr::parse(t).unwrap()
    }
    fn cf_policy() -> ClientIpPolicy {
        ClientIpPolicy::Cloudflare {
            trusted: vec![vrt_config::Cidr::parse("10.42.0.0/16").unwrap()],
            cloudflare: vec![vrt_config::Cidr::parse("104.16.0.0/13").unwrap()],
        }
    }
    fn peer() -> std::net::IpAddr {
        std::net::IpAddr::from_str("10.42.0.1").unwrap()
    }
    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_str(k).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn cloudflare_chain_trust_requires_both_hops() {
        let p = cf_policy();
        let inbound = headers(&[("x-forwarded-for", "203.0.113.7, 104.16.1.2")]);
        assert!(upstream_is_cloudflare(&p, &peer(), &inbound));
        // Adjacent hop NOT a CF edge → untrusted.
        let inbound = headers(&[("x-forwarded-for", "203.0.113.7, 198.51.100.9")]);
        assert!(!upstream_is_cloudflare(&p, &peer(), &inbound));
        // Peer outside the trusted set → untrusted even with a CF hop.
        let inbound = headers(&[("x-forwarded-for", "203.0.113.7, 104.16.1.2")]);
        let untrusted_peer = std::net::IpAddr::from_str("10.99.0.1").unwrap();
        assert!(!upstream_is_cloudflare(&p, &untrusted_peer, &inbound));
        // Non-cloudflare modes never trust cf metadata.
        assert!(!upstream_is_cloudflare(
            &ClientIpPolicy::None,
            &peer(),
            &inbound
        ));
        assert!(!upstream_is_cloudflare(
            &ClientIpPolicy::TrustedXff {
                trusted: vec![vrt_config::Cidr::parse("10.42.0.0/16").unwrap()]
            },
            &peer(),
            &inbound
        ));
    }

    #[test]
    fn cf_metadata_drops_everything_from_untrusted_peers() {
        let inbound = headers(&[
            ("cf-ipcountry", "JP"),
            ("cf-ray", "8f1234abcd-NRT"),
            ("cf-ipcity", "Tokyo"),
            ("cf-ipcontinent", "AS"),
            ("cf-region", "Tokyo"),
            ("cf-region-code", "13"),
            ("cf-metro-code", "0"),
            ("cf-postal-code", "100-0001"),
            ("cf-timezone", "Asia/Tokyo"),
            ("cf-iplatitude", "35.6895"),
            ("cf-iplongitude", "139.6917"),
            ("x-forwarded-for", "203.0.113.7, 104.16.1.2"),
        ]);
        let bad_peer = std::net::IpAddr::from_str("192.0.2.55").unwrap();
        let m = cf_metadata(&cf_policy(), &bad_peer, &inbound);
        assert!(m.country.is_none() && m.colo.is_none() && m.ray.is_none());
        assert!(m.city.is_none() && m.continent.is_none() && m.region.is_none());
        assert!(m.region_code.is_none() && m.metro_code.is_none());
        assert!(m.postal_code.is_none() && m.timezone.is_none());
        assert!(m.latitude.is_none() && m.longitude.is_none());
    }

    fn cf_inbound(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut all = vec![("x-forwarded-for", "203.0.113.7, 104.16.1.2")];
        all.extend_from_slice(pairs);
        headers(&all)
    }

    #[test]
    fn cf_metadata_accepts_visitor_location_fields() {
        let m = cf_metadata(
            &cf_policy(),
            &peer(),
            &cf_inbound(&[
                ("cf-ipcity", "Sao Paulo"),
                ("cf-ipcontinent", "SA"),
                ("cf-region", "Sao Paulo"),
                ("cf-region-code", "SP"),
                ("cf-metro-code", "0"),
                ("cf-postal-code", "01310-100"),
                ("cf-timezone", "America/Sao_Paulo"),
                ("cf-iplatitude", "-23.5505"),
                ("cf-iplongitude", "-46.6333"),
            ]),
        );
        assert_eq!(m.city.as_deref(), Some("Sao Paulo"));
        assert_eq!(m.continent.as_deref(), Some("SA"));
        assert_eq!(m.region.as_deref(), Some("Sao Paulo"));
        assert_eq!(m.region_code.as_deref(), Some("SP"));
        assert_eq!(m.metro_code.as_deref(), Some("0"));
        assert_eq!(m.postal_code.as_deref(), Some("01310-100"));
        assert_eq!(m.timezone.as_deref(), Some("America/Sao_Paulo"));
        assert_eq!(m.latitude.as_deref(), Some("-23.5505"));
        assert_eq!(m.longitude.as_deref(), Some("-46.6333"));
    }

    #[test]
    fn cf_metadata_rejects_malformed_visitor_location_fields() {
        // Every field is independently validated: a bad value is
        // dropped, never echoed, and never affects its neighbours.
        let cases: &[(&str, &str)] = &[
            ("cf-ipcontinent", "EUROPE"),
            ("cf-ipcontinent", "eu"),
            ("cf-region-code", "lowercase"),
            ("cf-region-code", "has space"),
            ("cf-metro-code", "12a"),
            ("cf-postal-code", "100;DROP"),
            ("cf-timezone", "Asia/Tokyo\n"),
            ("cf-timezone", "../etc/passwd"),
            ("cf-iplatitude", "91"),
            ("cf-iplatitude", "nan"),
            ("cf-iplatitude", "inf"),
            ("cf-iplongitude", "-181"),
        ];
        for (name, value) in cases {
            // CR/LF/NUL cannot even be encoded as a header value —
            // those are rejected by the HTTP layer, not by us.
            let Ok(hv) = HeaderValue::from_bytes(value.as_bytes()) else {
                continue;
            };
            let mut h = HeaderMap::new();
            h.insert(axum::http::HeaderName::from_str(name).unwrap(), hv);
            h.insert(
                axum::http::HeaderName::from_static("x-forwarded-for"),
                HeaderValue::from_static("203.0.113.7, 104.16.1.2"),
            );
            let m = cf_metadata(&cf_policy(), &peer(), &h);
            let field = match *name {
                "cf-ipcontinent" => m.continent,
                "cf-region-code" => m.region_code,
                "cf-metro-code" => m.metro_code,
                "cf-postal-code" => m.postal_code,
                "cf-timezone" => m.timezone,
                "cf-iplatitude" => m.latitude,
                "cf-iplongitude" => m.longitude,
                other => panic!("unexpected case {other}"),
            };
            assert!(field.is_none(), "{name} accepted {value:?}");
        }
    }

    #[test]
    fn cf_blob_never_contains_peer_credentials() {
        let m = CfMetadata {
            country: Some("JP".into()),
            colo: Some("NRT".into()),
            ray: Some("8f9-NRT".into()),
            city: Some("Tokyo".into()),
            continent: Some("AS".into()),
            region: Some("Tokyo".into()),
            region_code: Some("13".into()),
            metro_code: Some("0".into()),
            postal_code: Some("100-0001".into()),
            timezone: Some("Asia/Tokyo".into()),
            latitude: Some("35.6895".into()),
            longitude: Some("139.6917".into()),
            asn: Some(2516),
            as_org: Some("KDDI CORPORATION".into()),
        };
        let blob = cf_blob_json(&m, "HTTP/1.1");
        assert!(!blob.contains("Pid") && !blob.contains("Uid"));
        let v: serde_json::Value = serde_json::from_str(&blob).unwrap();
        assert_eq!(v["country"], "JP");
        assert_eq!(v["colo"], "NRT");
        assert_eq!(v["httpProtocol"], "HTTP/1.1");
        assert_eq!(v["city"], "Tokyo");
        assert_eq!(v["continent"], "AS");
        assert_eq!(v["region"], "Tokyo");
        assert_eq!(v["regionCode"], "13");
        assert_eq!(v["metroCode"], "0");
        assert_eq!(v["postalCode"], "100-0001");
        assert_eq!(v["timezone"], "Asia/Tokyo");
        // Strings, not numbers — CF's request.cf types.
        assert_eq!(v["latitude"], "35.6895");
        assert_eq!(v["longitude"], "139.6917");
        // asn is the exception: a JSON number, matching CF.
        assert_eq!(v["asn"], 2516);
        assert!(v["asn"].is_u64());
        assert_eq!(v["asOrganization"], "KDDI CORPORATION");
        // Japan is not an EU member.
        assert!(v["isEUCountry"].is_null());
        // EU member state → "1".
        let de = CfMetadata {
            country: Some("DE".into()),
            ..Default::default()
        };
        let v: serde_json::Value =
            serde_json::from_str(&cf_blob_json(&de, "HTTP/2")).unwrap();
        assert_eq!(v["isEUCountry"], "1");
        // Untrusted path still yields a host-defined blob.
        let empty = cf_blob_json(&CfMetadata::default(), "HTTP/1.1");
        let v: serde_json::Value = serde_json::from_str(&empty).unwrap();
        assert!(v["country"].is_null() && v["colo"].is_null());
        assert_eq!(v["httpProtocol"], "HTTP/1.1");
    }

    /// Local GeoIP fills only the fields a verified upstream chain
    /// did not provide — upstream values always win, and the
    /// MMDB-only fields (asn/asOrganization) appear alongside them.
    #[test]
    fn merge_geo_fills_gaps_never_overrides() {
        let geo = crate::geoip::GeoRecord {
            country: Some("US".into()),
            city: Some("Omaha".into()),
            region_code: Some("NE".into()),
            asn: Some(15169),
            as_org: Some("Google LLC".into()),
            ..Default::default()
        };
        // Upstream already described country+city; MMDB disagrees.
        let mut m = CfMetadata {
            country: Some("JP".into()),
            city: Some("Tokyo".into()),
            ..Default::default()
        };
        m.merge_geo(&geo);
        assert_eq!(m.country.as_deref(), Some("JP")); // upstream wins
        assert_eq!(m.city.as_deref(), Some("Tokyo")); // upstream wins
        assert_eq!(m.region_code.as_deref(), Some("NE")); // gap filled
        assert_eq!(m.asn, Some(15169)); // MMDB-only field
        assert_eq!(m.as_org.as_deref(), Some("Google LLC"));
        // And the same record fills everything on an empty metadata
        // (the no-upstream direct-ingress path).
        let mut bare = CfMetadata::default();
        bare.merge_geo(&geo);
        assert_eq!(bare.country.as_deref(), Some("US"));
        assert_eq!(bare.asn, Some(15169));
    }

    #[test]
    fn strip_list_covers_every_cf_header() {
        for name in [
            "cf-ipcountry",
            "cf-ray",
            "cf-ipcity",
            "cf-ipcontinent",
            "cf-region",
            "cf-region-code",
            "cf-metro-code",
            "cf-postal-code",
            "cf-timezone",
            "cf-iplatitude",
            "cf-iplongitude",
        ] {
            assert!(STRIP_REQUEST.contains(&name), "{name} not stripped");
        }
    }

    #[test]
    fn scrubbed_headers_reinject_visitor_location_only_behind_cloudflare() {
        let target = crate::route::RouteTarget {
            tenant_id: "t".into(),
            worker_id: "w".into(),
            assets_first: false,
        };
        let inbound = cf_inbound(&[
            ("cf-ipcountry", "JP"),
            ("cf-ipcity", "Tokyo"),
            ("cf-ipcontinent", "AS"),
            ("cf-region", "Tokyo"),
            ("cf-region-code", "13"),
            ("cf-timezone", "Asia/Tokyo"),
            ("cf-iplatitude", "35.6895"),
            ("cf-iplongitude", "139.6917"),
        ]);
        let trusted = scrubbed_headers(
            &inbound,
            "app.example.com",
            &target,
            "10.42.0.1:4000".parse().unwrap(),
            &ForwardedProto::FixedHttp,
            &cf_policy(),
            None,
            axum::http::Version::HTTP_11,
            false,
        );
        assert_eq!(trusted.get("cf-ipcity").unwrap(), "Tokyo");
        assert_eq!(trusted.get("cf-ipcontinent").unwrap(), "AS");
        assert_eq!(trusted.get("cf-region").unwrap(), "Tokyo");
        assert_eq!(trusted.get("cf-region-code").unwrap(), "13");
        assert_eq!(trusted.get("cf-timezone").unwrap(), "Asia/Tokyo");
        assert_eq!(trusted.get("cf-iplatitude").unwrap(), "35.6895");
        assert_eq!(trusted.get("cf-iplongitude").unwrap(), "139.6917");

        // Same inbound headers on an untrusted peer are dropped.
        let untrusted = scrubbed_headers(
            &inbound,
            "app.example.com",
            &target,
            "192.0.2.55:4000".parse().unwrap(),
            &ForwardedProto::FixedHttp,
            &cf_policy(),
            None,
            axum::http::Version::HTTP_11,
            false,
        );
        assert!(untrusted.get("cf-ipcity").is_none());
        assert!(untrusted.get("cf-ipcontinent").is_none());
        assert!(untrusted.get("cf-region").is_none());
        assert!(untrusted.get("cf-timezone").is_none());
        assert!(untrusted.get("cf-iplatitude").is_none());
    }

    #[test]
    fn cf_metadata_validates_country_and_colo() {
        let p = cf_policy();
        let good = headers(&[
            ("x-forwarded-for", "203.0.113.7, 104.16.1.2"),
            ("cf-ipcountry", "JP"),
            ("cf-ray", "8f9aa1234-NRT"),
        ]);
        let m = cf_metadata(&p, &peer(), &good);
        assert_eq!(m.country.as_deref(), Some("JP"));
        assert_eq!(m.colo.as_deref(), Some("NRT"));
        assert_eq!(m.ray.as_deref(), Some("8f9aa1234-NRT"));
        // Invalid country shapes dropped.
        for bad in ["jP", "J", "JPN", "1A", "A1", "T2", "XY ", ""] {
            let h = headers(&[
                ("x-forwarded-for", "203.0.113.7, 104.16.1.2"),
                ("cf-ipcountry", bad),
            ]);
            let m = cf_metadata(&p, &peer(), &h);
            assert!(m.country.is_none(), "{bad:?}");
        }
        for ok in ["US", "T1", "XX"] {
            let h = headers(&[
                ("x-forwarded-for", "203.0.113.7, 104.16.1.2"),
                ("cf-ipcountry", ok),
            ]);
            assert_eq!(cf_metadata(&p, &peer(), &h).country.as_deref(), Some(ok));
        }
        // Malformed rays drop colo AND ray wholesale.
        for bad in ["NRT", "x-NRT", "8f9-sg1", "8f9aa1234-nrt", "-NRT", "8f9aa1234-", "8f9aa1234-NRTX", "not-a-ray-SIN", "8F9AA1234-NRT "] {
            let h = headers(&[
                ("x-forwarded-for", "203.0.113.7, 104.16.1.2"),
                ("cf-ray", bad),
            ]);
            let m = cf_metadata(&p, &peer(), &h);
            assert!(m.colo.is_none() && m.ray.is_none(), "{bad:?}");
        }
    }

    #[test]
    fn header_name_is_internal_and_matches_vrt_config() {
        assert_eq!(CF_BLOB_HEADER, "x-vrt-cf-blob");
        assert!(STRIP_PREFIXES
            .iter()
            .any(|p| CF_BLOB_HEADER.starts_with(p)));
    }
}

#[cfg(test)]
mod ws_tests {
    //! WebSocket upgrade detection + header normalization + typed
    //! capacity error. End-to-end tunnel behaviour lives in
    //! `tests/ingress.rs`.
    use super::*;
    use axum::http::Version;

    fn hm(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn up(method: Method, version: Version, pairs: &[(&str, &str)]) -> bool {
        is_websocket_upgrade(&method, version, &hm(pairs))
    }

    #[test]
    fn upgrade_truth_table() {
        let ok = [("upgrade", "websocket"), ("connection", "Upgrade")];
        assert!(up(Method::GET, Version::HTTP_11, &ok));
        // Case-insensitive tokens.
        assert!(up(
            Method::GET,
            Version::HTTP_11,
            &[("upgrade", "WebSocket"), ("connection", "UPGRADE")]
        ));
        // Connection with several tokens, any order / spacing.
        assert!(up(
            Method::GET,
            Version::HTTP_11,
            &[("upgrade", "websocket"), ("connection", "keep-alive, Upgrade")]
        ));
        // Connection split across header lines.
        assert!(up(
            Method::GET,
            Version::HTTP_11,
            &[
                ("upgrade", "websocket"),
                ("connection", "keep-alive"),
                ("connection", "upgrade")
            ]
        ));
        // Non-GET, wrong version.
        assert!(!up(Method::POST, Version::HTTP_11, &ok));
        assert!(!up(Method::GET, Version::HTTP_10, &ok));
        assert!(!up(Method::GET, Version::HTTP_2, &ok));
        // Other protocols are never tunnelled.
        assert!(!up(
            Method::GET,
            Version::HTTP_11,
            &[("upgrade", "h2c"), ("connection", "Upgrade, HTTP2-Settings")]
        ));
        // A mixed list is not "exactly websocket": fail closed.
        assert!(!up(
            Method::GET,
            Version::HTTP_11,
            &[("upgrade", "websocket, h2c"), ("connection", "upgrade")]
        ));
        // Missing / non-matching Connection.
        assert!(!up(Method::GET, Version::HTTP_11, &[("upgrade", "websocket")]));
        assert!(!up(
            Method::GET,
            Version::HTTP_11,
            &[("upgrade", "websocket"), ("connection", "keep-alive")]
        ));
        // Missing Upgrade.
        assert!(!up(Method::GET, Version::HTTP_11, &[("connection", "upgrade")]));
        // Request-body framing disqualifies.
        assert!(!up(
            Method::GET,
            Version::HTTP_11,
            &[
                ("upgrade", "websocket"),
                ("connection", "upgrade"),
                ("content-length", "5")
            ]
        ));
        assert!(!up(
            Method::GET,
            Version::HTTP_11,
            &[
                ("upgrade", "websocket"),
                ("connection", "upgrade"),
                ("transfer-encoding", "chunked")
            ]
        ));
        // Explicit zero length is fine.
        assert!(up(
            Method::GET,
            Version::HTTP_11,
            &[
                ("upgrade", "websocket"),
                ("connection", "upgrade"),
                ("content-length", "0")
            ]
        ));
    }

    fn scrub(inbound: &HeaderMap, ws: bool) -> HeaderMap {
        let target = crate::route::RouteTarget {
            tenant_id: "t".into(),
            worker_id: "w".into(),
            assets_first: false,
        };
        scrubbed_headers(
            inbound,
            "app.example.com",
            &target,
            "203.0.113.9:4000".parse().unwrap(),
            &ForwardedProto::FixedHttp,
            &ClientIpPolicy::None,
            None,
            Version::HTTP_11,
            ws,
        )
    }

    #[test]
    fn scrubbed_headers_normalize_upgrade_only_for_ws() {
        let inbound = hm(&[
            ("upgrade", "WebSocket"),
            ("connection", "keep-alive, upgrade"),
            ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ("sec-websocket-version", "13"),
            ("sec-websocket-protocol", "chat"),
            ("sec-websocket-extensions", "permessage-deflate"),
            ("x-vrt-tenant-id", "evil"),
            ("x-forwarded-for", "6.6.6.6"),
            ("cf-ray", "forged"),
        ]);
        let out = scrub(&inbound, true);
        assert_eq!(out.get(header::UPGRADE).unwrap(), "websocket");
        assert_eq!(out.get(header::CONNECTION).unwrap(), "Upgrade");
        assert_eq!(out.get_all(header::CONNECTION).iter().count(), 1);
        for h in [
            "sec-websocket-key",
            "sec-websocket-version",
            "sec-websocket-protocol",
            "sec-websocket-extensions",
        ] {
            assert!(out.contains_key(h), "{h} must pass through");
        }
        // Trust-boundary rewrites are unchanged.
        assert_eq!(out.get("x-vrt-tenant-id").unwrap(), "t");
        assert_eq!(out.get("x-forwarded-for").unwrap(), "203.0.113.9");
        assert!(!out.contains_key("cf-ray"));

        // Non-upgrade (e.g. h2c attempt): both stay stripped.
        let h2c = hm(&[("upgrade", "h2c"), ("connection", "upgrade")]);
        let out = scrub(&h2c, false);
        assert!(!out.contains_key(header::UPGRADE));
        assert!(!out.contains_key(header::CONNECTION));
    }

    #[test]
    fn upgrade_response_headers_keep_handshake_headers_only_for_101() {
        let mut h = hm(&[
            ("upgrade", "WebSocket"),
            ("connection", "Upgrade"),
            ("sec-websocket-accept", "abc"),
            ("sec-websocket-protocol", "chat"),
            ("x-vrt-error", "forged"),
            ("content-length", "0"),
        ]);
        scrub_upgrade_response_headers(&mut h);
        assert_eq!(h.get(header::UPGRADE).unwrap(), "websocket");
        assert_eq!(h.get(header::CONNECTION).unwrap(), "Upgrade");
        assert_eq!(h.get("sec-websocket-accept").unwrap(), "abc");
        assert_eq!(h.get("sec-websocket-protocol").unwrap(), "chat");
        assert!(!h.contains_key("x-vrt-error"));
        assert!(!h.contains_key(header::CONTENT_LENGTH));
        // Ordinary responses still lose both.
        let mut h = hm(&[("upgrade", "websocket"), ("connection", "Upgrade")]);
        scrub_response_headers(&mut h);
        assert!(h.is_empty());
    }

    #[test]
    fn ws_capacity_is_a_typed_503_with_platform_code() {
        let e = IngressError::WsCapacity;
        assert_eq!(e.status_and_detail().0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(e.to_string(), "ws_capacity");
        assert_eq!(e.metric_reason(), Some("ws_capacity"));
        let c = e.platform_code().expect("has code");
        assert_eq!(c.as_str(), e.to_string());
        assert!(crate::logs::PlatformCode::ALL.contains(&c));
    }

    fn st(max: usize, per_tenant: usize) -> Arc<IngressState> {
        let reg = crate::supervisor::TenantRegistry::new();
        IngressState::new(reg, Duration::from_secs(1), Duration::from_secs(1), 1024)
            .with_ws_limits(
                Duration::from_secs(1),
                Duration::from_secs(1),
                max,
                per_tenant,
            )
    }

    #[test]
    fn permit_cap_is_exact_and_released_on_drop() {
        let st = st(2, 2);
        let a = WsPermit::try_acquire(&st, "t", "w-permit").unwrap();
        let b = WsPermit::try_acquire(&st, "t", "w-permit").unwrap();
        assert_eq!(
            WsPermit::try_acquire(&st, "t", "w-permit").err(),
            Some(WsDenied::Global)
        );
        assert_eq!(st.ws_active.load(Ordering::SeqCst), 2);
        drop(a);
        let c = WsPermit::try_acquire(&st, "t", "w-permit").unwrap();
        drop((b, c));
        assert_eq!(st.ws_active.load(Ordering::SeqCst), 0);
        assert!(st.ws_tenant_active.lock().unwrap().is_empty());
    }

    #[test]
    fn tenant_cap_isolates_tenants_and_rolls_back() {
        let st = st(10, 2);
        let a1 = WsPermit::try_acquire(&st, "a", "w").unwrap();
        let a2 = WsPermit::try_acquire(&st, "a", "w").unwrap();
        // N+1 for tenant a is refused by the tenant cap, and the global
        // reservation made for it is rolled back (no leak).
        assert_eq!(
            WsPermit::try_acquire(&st, "a", "w").err(),
            Some(WsDenied::Tenant)
        );
        assert_eq!(st.ws_active.load(Ordering::SeqCst), 2);
        assert_eq!(WsDenied::Tenant.outcome(), "rejected_tenant_capacity");
        assert_eq!(WsDenied::Global.outcome(), "rejected_capacity");
        // Another tenant is unaffected.
        let b1 = WsPermit::try_acquire(&st, "b", "w").unwrap();
        assert_eq!(st.ws_active.load(Ordering::SeqCst), 3);
        // Release makes room again.
        drop(a1);
        let a3 = WsPermit::try_acquire(&st, "a", "w").unwrap();
        // Global cap wins when both would refuse and is reported as such.
        drop((a2, a3, b1));
        assert_eq!(st.ws_active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn tenant_map_keys_are_removed_at_zero() {
        let st = st(10, 3);
        let p1 = WsPermit::try_acquire(&st, "x", "w").unwrap();
        let p2 = WsPermit::try_acquire(&st, "y", "w").unwrap();
        assert_eq!(st.ws_tenant_active.lock().unwrap().len(), 2);
        drop(p1);
        {
            let m = st.ws_tenant_active.lock().unwrap();
            assert!(!m.contains_key("x"));
            assert_eq!(m.get("y"), Some(&1));
        }
        // A refused attempt must not leave a key behind either.
        let st2 = st_full();
        assert_eq!(
            WsPermit::try_acquire(&st2, "z", "w").err(),
            Some(WsDenied::Global)
        );
        assert!(!st2.ws_tenant_active.lock().unwrap().contains_key("z"));
        drop(p2);
        assert!(st.ws_tenant_active.lock().unwrap().is_empty());
    }

    /// An ingress whose global cap (1) is already consumed.
    fn st_full() -> Arc<IngressState> {
        let s = st(1, 1);
        std::mem::forget(WsPermit::try_acquire(&s, "other", "w").unwrap());
        s
    }

    /// Fixed-table GeoIP for the injection proof — the real reader
    /// needs a 100 MB MMDB fixture, but everything downstream of
    /// `lookup` (IP selection, gap-filling, header/blob emission) is
    /// the production path.
    struct FakeGeo {
        record: crate::geoip::GeoRecord,
        asked: std::sync::Mutex<Vec<std::net::IpAddr>>,
    }

    impl crate::geoip::GeoLookup for FakeGeo {
        fn lookup(&self, ip: std::net::IpAddr) -> Option<crate::geoip::GeoRecord> {
            self.asked.lock().unwrap().push(ip);
            Some(self.record.clone())
        }
    }

    fn scrub_with_geo(
        inbound: &HeaderMap,
        peer: &str,
        geo: Option<&dyn crate::geoip::GeoLookup>,
    ) -> HeaderMap {
        let target = crate::route::RouteTarget {
            tenant_id: "t".into(),
            worker_id: "w".into(),
            assets_first: false,
        };
        scrubbed_headers(
            inbound,
            "app.example.com",
            &target,
            peer.parse().unwrap(),
            &ForwardedProto::FixedHttp,
            &ClientIpPolicy::None,
            geo,
            Version::HTTP_11,
            false,
        )
    }

    /// End to end: no trusted upstream chain (direct ingress) + a local
    /// record ⇒ the worker receives `cf-ipcountry`/`cf-ipcontinent`/
    /// `request.cf` geo+ASN for the TCP peer, and the lookup subject is
    /// that peer (never a client-supplied header). With NO local record
    /// the same request carries no geo at all — the degrade path.
    #[test]
    fn local_geoip_injects_cf_headers_and_blob() {
        use std::sync::Mutex;
        let geo = FakeGeo {
            record: crate::geoip::GeoRecord {
                country: Some("JP".into()),
                continent: Some("AS".into()),
                city: Some("Tokyo".into()),
                region: Some("Tokyo".into()),
                region_code: Some("13".into()),
                timezone: Some("Asia/Tokyo".into()),
                latitude: Some("35.68950".into()),
                longitude: Some("139.69170".into()),
                asn: Some(2516),
                as_org: Some("KDDI CORPORATION".into()),
                ..Default::default()
            },
            asked: Mutex::new(Vec::new()),
        };
        // A hostile client tries to steer the lookup with XFF; with no
        // trusted proxy the peer is the client (policy `none`).
        let inbound = hm(&[("x-forwarded-for", "198.51.100.7")]);
        let out = scrub_with_geo(&inbound, "203.0.113.9:4000", Some(&geo));
        assert_eq!(
            geo.asked.lock().unwrap().as_slice(),
            &["203.0.113.9".parse::<std::net::IpAddr>().unwrap()]
        );
        assert_eq!(out.get("cf-ipcountry").unwrap(), "JP");
        assert_eq!(out.get("cf-ipcontinent").unwrap(), "AS");
        assert_eq!(out.get("cf-ipcity").unwrap(), "Tokyo");
        assert_eq!(out.get("cf-region-code").unwrap(), "13");
        assert_eq!(out.get("cf-timezone").unwrap(), "Asia/Tokyo");
        let blob: serde_json::Value =
            serde_json::from_str(out.get(CF_BLOB_HEADER).unwrap().to_str().unwrap()).unwrap();
        assert_eq!(blob["country"], "JP");
        assert_eq!(blob["city"], "Tokyo");
        assert_eq!(blob["asn"], 2516);
        assert!(blob["asn"].is_u64());
        assert_eq!(blob["asOrganization"], "KDDI CORPORATION");
        // Local geo never fabricates the CF-only identity fields.
        assert!(out.get("cf-connecting-ip").is_none());
        assert!(out.get("cf-ray").is_none());

        // No lookup surface ⇒ exactly the pre-GeoIP behaviour.
        let plain = scrub_with_geo(&inbound, "203.0.113.9:4000", None);
        assert!(plain.get("cf-ipcountry").is_none());
        let blob: serde_json::Value =
            serde_json::from_str(plain.get(CF_BLOB_HEADER).unwrap().to_str().unwrap()).unwrap();
        assert!(blob.get("country").is_none() && blob.get("asn").is_none());
    }

    #[test]
    fn geoip_lookup_ip_never_resolves_a_trusted_proxy() {
        use std::str::FromStr;
        let policy = ClientIpPolicy::Cloudflare {
            trusted: vec![vrt_config::Cidr::parse("10.42.0.0/16").unwrap()],
            cloudflare: vec![vrt_config::Cidr::parse("104.16.0.0/13").unwrap()],
        };
        let peer_ip = std::net::IpAddr::from_str("10.42.0.1").unwrap();
        let headers = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(
                axum::http::HeaderName::from_str("x-forwarded-for").unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
            h
        };
        let inbound = HeaderMap::new();
        // Trusted CF-fronting peer + empty XFF chain ⇒ resolve_client_ip
        // finds no client identity. The proxy's own datacenter IP must
        // NOT be looked up (its geo would be re-injected as
        // edge-verified cf-ipcountry/request.cf).
        assert!(geoip_lookup_ip(&policy, &peer_ip, &inbound).is_none());
        // Unparseable XFF: same result.
        assert!(geoip_lookup_ip(&policy, &peer_ip, &headers("unknown,junk")).is_none());
        // Trusted peer + real client in XFF ⇒ the client, not the peer.
        assert_eq!(
            geoip_lookup_ip(&policy, &peer_ip, &headers("203.0.113.9")),
            Some(std::net::IpAddr::from_str("203.0.113.9").unwrap())
        );
        // Peer OUTSIDE the trusted set: its XFF is ignored, so the peer
        // itself is the client as far as this edge can verify.
        let outside = std::net::IpAddr::from_str("192.0.2.55").unwrap();
        assert_eq!(geoip_lookup_ip(&policy, &outside, &inbound), Some(outside));
        // No trusted-proxy policy at all ⇒ the peer is the client.
        assert_eq!(
            geoip_lookup_ip(&ClientIpPolicy::None, &peer_ip, &inbound),
            Some(peer_ip)
        );
    }
}
