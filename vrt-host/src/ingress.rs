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
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;

/// Hop-by-hop + forged headers removed before forwarding.
///
/// MIG-7: proxy-derived headers are platform-owned in the request
/// direction — the values a workerd sees are ALWAYS written by
/// `scrubbed_headers` from the trusted-source policy below, so a
/// client (or a non-Traefik pod on the ingress port) can never inject
/// a scheme, client-IP chain, or Cloudflare edge header. `cf-*` is
/// stripped unconditionally: the front proxy does not sanitize it
/// and this runtime exposes no `request.cf` surface.
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
    "cf-ray",
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
}

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
    }
    if let Some(c) = &meta.colo {
        map.insert("colo".to_string(), serde_json::Value::String(c.clone()));
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
    /// Worker-log hub: host-observed routing failures are recorded as
    /// `level=platform` records. Set once after construction; unset =
    /// log collection disabled (zero overhead).
    logs: std::sync::OnceLock<Arc<crate::logs::LogHub>>,
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
            logs: std::sync::OnceLock::new(),
        })
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
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
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
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
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
            panic_once: std::sync::atomic::AtomicBool::new(false),
            logs: match self.logs.get() {
                Some(h) => {
                    let c = std::sync::OnceLock::new();
                    let _ = c.set(h.clone());
                    c
                }
                None => std::sync::OnceLock::new(),
            },
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
    let target = routes.lookup(&host).ok_or(IngressError::UnknownHost)?;

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
        parts.version,
    );

    let endpoint = slot.endpoint().clone();
    let log_hub = state.logs.get().cloned();
    let response = tokio::time::timeout(state.request_timeout, async move {
        let io = tokio::time::timeout(state.connect_timeout, endpoint.connect())
            .await
            .map_err(|_| IngressError::UpstreamConnect)?
            .map_err(|_| IngressError::UpstreamConnect)?;
        upstream_call(io, upstream_req).await
    })
    .await
    .map_err(|_| IngressError::UpstreamTimeout)??;

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

fn path_and_query(uri: &axum::http::Uri) -> String {
    uri.path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/")
        .to_string()
}

fn scrubbed_headers(
    inbound: &HeaderMap,
    host: &str,
    target: &crate::route::RouteTarget,
    peer: SocketAddr,
    proto_policy: &ForwardedProto,
    client_ip_policy: &ClientIpPolicy,
    version: axum::http::Version,
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
    let cf = cf_metadata(client_ip_policy, &peer.ip(), inbound);
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
    tokio::spawn(async move {
        let _ = conn.await;
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
            trusted: vec![cidr("10.42.0.0/16")],
            cloudflare: vec![cidr("104.16.0.0/13")],
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
                trusted: vec![cidr("10.42.0.0/16")]
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
            ("x-forwarded-for", "203.0.113.7, 104.16.1.2"),
        ]);
        let bad_peer = std::net::IpAddr::from_str("192.0.2.55").unwrap();
        let m = cf_metadata(&cf_policy(), &bad_peer, &inbound);
        assert!(m.country.is_none() && m.colo.is_none() && m.ray.is_none());
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
    fn cf_blob_never_contains_peer_credentials() {
        let m = CfMetadata {
            country: Some("JP".into()),
            colo: Some("NRT".into()),
            ray: Some("8f9-NRT".into()),
        };
        let blob = cf_blob_json(&m, "HTTP/1.1");
        assert!(!blob.contains("Pid") && !blob.contains("Uid"));
        let v: serde_json::Value = serde_json::from_str(&blob).unwrap();
        assert_eq!(v["country"], "JP");
        assert_eq!(v["colo"], "NRT");
        assert_eq!(v["httpProtocol"], "HTTP/1.1");
        // Untrusted path still yields a host-defined blob.
        let empty = cf_blob_json(&CfMetadata::default(), "HTTP/1.1");
        let v: serde_json::Value = serde_json::from_str(&empty).unwrap();
        assert!(v["country"].is_null() && v["colo"].is_null());
        assert_eq!(v["httpProtocol"], "HTTP/1.1");
    }

    #[test]
    fn header_name_is_internal_and_matches_vrt_config() {
        assert_eq!(CF_BLOB_HEADER, "x-vrt-cf-blob");
        assert!(STRIP_PREFIXES
            .iter()
            .any(|p| CF_BLOB_HEADER.starts_with(p)));
    }
}
