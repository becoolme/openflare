//! Typed policy for the `vrt-egress` system worker (`facade/egress/`, T6).
//!
//! The platform renders this struct as canonical JSON into the
//! `VRT_EGRESS_POLICY` text binding on the egress worker service. The
//! worker re-validates the document on construction (the parsing mirror
//! lives in `facade/egress/policy.ts`), so a malformed injection still
//! fails closed at the worker. Field names and bounds are shared
//! contract: keep this module and `policy.ts` in lockstep.

use crate::canonical::canonical_bytes;
use crate::error::ConfigError;
use serde_json::json;

/// Worker-side binding carrying the canonical policy JSON.
pub const EGRESS_POLICY_BINDING: &str = "VRT_EGRESS_POLICY";
pub const DEFAULT_REQUEST_TIMEOUT_MS: u32 = 30_000;
pub const MIN_REQUEST_TIMEOUT_MS: u32 = 1_000;
pub const MAX_REQUEST_TIMEOUT_MS: u32 = 120_000;
pub const MAX_ALLOWED_PORTS: usize = 32;
pub const MAX_ALLOW_ENTRIES: usize = 64;
pub const MAX_DENY_NAMES: usize = 64;
pub const MAX_POLICY_HOST_LEN: usize = 253;
/// MIG-14: raw-TCP `connect()` bounds. `tcpAllowedPorts` shares the
/// MIG-15: provider pg-wire proxy endpoint — every internalTcpTargets
/// entry's upstream. Loopback-only by construction (the facade's
/// INTERNAL_NET binding lands on the `internal-network` whose allow
/// list is `["local"]`). Port MUST equal `[pgProxy].listen` in
/// deployment vrt-provider.toml files.
pub const PG_PROXY_HOST: &str = "127.0.0.1";
pub const PG_PROXY_PORT: u16 = 19543;

/// HTTP port cap; internal exceptions share the allow-entry cap.
pub const MAX_TCP_ALLOWED_PORTS: usize = MAX_ALLOWED_PORTS;
pub const MAX_INTERNAL_TCP_ALLOW: usize = MAX_ALLOW_ENTRIES;
/// MIG-14: mail ports a TCP policy may never carry — not even via an
/// internal exception. SMTP egress is how hosted Workers become spam
/// sources, and WHOIS/PG never legitimately need them.
pub const TCP_NEVER_PORTS: [u16; 3] = [25, 465, 587];
pub const DEFAULT_TCP_CONNECT_TIMEOUT_MS: u32 = 10_000;
pub const MIN_TCP_TIMEOUT_MS: u32 = 1_000;
pub const MAX_TCP_CONNECT_TIMEOUT_MS: u32 = 120_000;
pub const DEFAULT_TCP_IDLE_TIMEOUT_MS: u32 = 60_000;
pub const MAX_TCP_IDLE_TIMEOUT_MS: u32 = 600_000;
pub const DEFAULT_TCP_MAX_CONCURRENT: u32 = 64;
pub const MAX_TCP_MAX_CONCURRENT: u32 = 1024;

/// Names a whitelist entry may never target — the worker's builtin
/// exact-deny table, mirrored. IP-literal allow entries are
/// re-canonicalized and range-checked worker-side (the authoritative
/// check; a violation fails closed by invalidating the whole policy).
const NEVER_ALLOW_HOSTS: [&str; 2] = ["localhost", "metadata.google.internal"];

/// The built-in non-public IPv4 deny ranges as `(cidr, worker-code)`.
///
/// **Single source of truth** for three consumers:
/// - `facade/egress/ip.ts` `IPV4_DENY` — byte-exact mirror, drift-checked
///   against `contract/egress-deny-ranges.json` by the node test suite
///   and `contract/check-egress-deny-ranges.mjs`;
/// - `netpolicy.rs` — every CIDR lands in the public `ipBlock` `except`
///   list;
/// - the fixture itself, rendered by `tests/egress.rs`
///   (`UPDATE_GOLDEN=1` regenerates).
pub const IPV4_DENY_RANGES: &[(&str, &str)] = &[
    ("0.0.0.0/8", "ip_this_net"),
    ("10.0.0.0/8", "ip_private"),
    ("100.64.0.0/10", "ip_cgnat"),
    ("127.0.0.0/8", "ip_loopback"),
    ("169.254.0.0/16", "ip_link_local"),
    ("172.16.0.0/12", "ip_private"),
    ("192.0.0.0/24", "ip_reserved"),
    ("192.0.2.0/24", "ip_reserved"),
    ("192.168.0.0/16", "ip_private"),
    ("198.18.0.0/15", "ip_reserved"),
    ("198.51.100.0/24", "ip_reserved"),
    ("203.0.113.0/24", "ip_reserved"),
    ("224.0.0.0/4", "ip_multicast"),
    // 240.0.0.0/4 also covers 255.255.255.255/32 (limited broadcast).
    ("240.0.0.0/4", "ip_reserved"),
];

/// The built-in non-public IPv6 deny ranges as `(cidr, worker-code)`.
/// `2002::/16` (6to4) is denied whole rather than per-embedded-IPv4:
/// the embedded address sits in bits 16..48, the relay scheme is
/// deprecated anycast, and a whole-range deny is strictly stronger.
/// `64:ff9b:1::/48` (RFC8215 local-use NAT64) is denied whole — a
/// cluster NAT64/DNS64 could otherwise bridge tenants into private
/// space.
pub const IPV6_DENY_RANGES: &[(&str, &str)] = &[
    ("::/128", "ip_unspecified"),
    ("::1/128", "ip_loopback"),
    ("100::/64", "ip_reserved"),
    ("2001::/32", "ip_reserved"),
    ("2001:db8::/32", "ip_reserved"),
    ("2002::/16", "ip_reserved"),
    ("64:ff9b:1::/48", "ip_reserved"),
    ("fc00::/7", "ip_ula"),
    ("fe80::/10", "ip_link_local"),
    ("ff00::/8", "ip_multicast"),
];

/// Prefixes whose low 32 bits embed an IPv4 the worker unwraps and
/// re-classifies (`ip_embedded` when denied). The NetworkPolicy blocks
/// these prefixes whole — embedded forms never leave the pod.
pub const EMBEDDED_IPV4_PREFIXES: &[&str] = &["::ffff:0.0.0.0/96", "64:ff9b::/96", "::/96"];

/// Subset of [`EMBEDDED_IPV4_PREFIXES`] that a Kubernetes NetworkPolicy
/// `ipBlock.except` cannot express: the API server rejects IPv4-mapped
/// IPv6 prefixes ("must not have an IPv4-mapped IPv6 address"). Leaving
/// it out of the policy is safe because a `connect()` to
/// `::ffff:a.b.c.d` is sent on the wire as an IPv4 packet, so the v4
/// `except` list (derived from [`IPV4_DENY_RANGES`]) already governs
/// it; the worker-side facade IP check (`ip_embedded`, driven by the
/// full [`EMBEDDED_IPV4_PREFIXES`] / `contract/egress-deny-ranges.json`)
/// still denies the mapped form and is NOT affected by this list.
pub const NETPOL_UNREPRESENTABLE_PREFIXES: &[&str] = &["::ffff:0.0.0.0/96"];

/// Embedded-IPv4 prefixes that go into the NetworkPolicy v6 `except`
/// list: every worker-side prefix minus the ones k8s cannot represent.
pub fn netpol_embedded_excepts() -> impl Iterator<Item = &'static str> {
    EMBEDDED_IPV4_PREFIXES
        .iter()
        .copied()
        .filter(|p| !NETPOL_UNREPRESENTABLE_PREFIXES.contains(p))
}

/// Whitelist `(host, port)` — the only path to internal endpoints.
/// `host` may be a DNS name, an IPv4 literal, or a `[v6]` literal; the
/// worker canonicalizes it through WHATWG URL parsing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EgressAllowEntry {
    pub host: String,
    pub port: u16,
}

/// Operator-added denied names; `*.suffix` is a label-boundary suffix.
/// Built-in denies (localhost, `*.internal`, …) live in the worker and
/// cannot be removed — this list can only extend them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EgressNameRule {
    Exact(String),
    Suffix(String),
}

impl EgressNameRule {
    /// `"*.foo"` → `Suffix("foo")`, `"foo"` → `Exact("foo")`.
    pub fn parse(text: &str) -> Option<Self> {
        match text.strip_prefix("*.") {
            Some(rest) if !rest.is_empty() => Some(Self::Suffix(rest.to_owned())),
            Some(_) => None,
            None if !text.is_empty() => Some(Self::Exact(text.to_owned())),
            None => None,
        }
    }

    fn wire(&self) -> String {
        match self {
            Self::Exact(name) => name.clone(),
            Self::Suffix(name) => format!("*.{name}"),
        }
    }
}

/// The egress policy. `Default` is the restrictive set: HTTP ports
/// 80/443, TCP port 43 (WHOIS) only, no host whitelist, no extra
/// denied names, 30 s fetch timeout. An EMPTY `tcp_allowed_ports`
/// keeps the pre-MIG-14 posture (all raw TCP denied).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EgressPolicy {
    pub allowed_ports: Vec<u16>,
    pub allow: Vec<EgressAllowEntry>,
    pub deny_names: Vec<EgressNameRule>,
    pub request_timeout_ms: u32,
    /// MIG-14: raw-TCP `connect()` ports allowed to public space.
    /// `TCP_NEVER_PORTS` (mail) may never appear — validation rejects
    /// the whole policy rather than silently dropping them.
    pub tcp_allowed_ports: Vec<u16>,
    /// MIG-14/MIG-15 reserved: platform-injected exact `{host,port}`
    /// internal exceptions. They bypass the TCP port list AND the
    /// name/IP deny checks — including private/CGNAT/ULA literals,
    /// which is their purpose (in-cluster gateways) — but never the
    /// [`TCP_NEVER_PORTS`], never `localhost`/`metadata.google.internal`,
    /// and never trivially-local literals (this-net, loopback,
    /// link-local, multicast, unspecified, on either family or an
    /// embedded unwrap). This field is NOT reachable from the operator
    /// params file (`EgressPolicyToml` in vrt-host has `deny_unknown_fields`
    /// and no such key) nor from any tenant surface: only platform Rust
    /// code can produce a non-empty list.
    pub internal_tcp_allow: Vec<EgressAllowEntry>,
    /// MIG-15: typed "internal TCP target provider" entries. Exact
    /// `{host,port}` matches route `connect()` away from the public
    /// path to the `INTERNAL_NET` service binding (internal-network,
    /// loopback-only) which terminates at the provider's pg-wire proxy;
    /// the first host label is the platform-minted binding id the
    /// worker injects as a `VRT-PG1 <id>` preamble. Platform-injected
    /// only — `EgressPolicyToml` has no such key (deny_unknown_fields).
    /// Entries are names, never literals, so the existing worker-side
    /// literal deny checks are not weakened.
    pub internal_tcp_targets: Vec<EgressAllowEntry>,
    /// MIG-14: upstream TCP `connect()` establishment bound (ms).
    pub tcp_connect_timeout_ms: u32,
    /// MIG-14: bidirectional inactivity bound before teardown (ms).
    pub tcp_idle_timeout_ms: u32,
    /// MIG-14: concurrent TCP connections per egress isolate (the
    /// CONNECT event carries no tenant identity; isolate-wide is the
    /// enforceable granularity).
    pub tcp_max_concurrent: u32,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self {
            allowed_ports: vec![80, 443],
            allow: Vec::new(),
            deny_names: Vec::new(),
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            tcp_allowed_ports: vec![43],
            internal_tcp_allow: Vec::new(),
            internal_tcp_targets: Vec::new(),
            tcp_connect_timeout_ms: DEFAULT_TCP_CONNECT_TIMEOUT_MS,
            tcp_idle_timeout_ms: DEFAULT_TCP_IDLE_TIMEOUT_MS,
            tcp_max_concurrent: DEFAULT_TCP_MAX_CONCURRENT,
        }
    }
}

fn valid_policy_host(host: &str) -> bool {
    if host.is_empty() || host.len() > MAX_POLICY_HOST_LEN {
        return false;
    }
    if let Some(inner) = host.strip_prefix('[') {
        // v6 literal: charset + at least one ":" so `[bad]` can't pass —
        // full validation still happens worker-side via the URL parser.
        return inner.ends_with(']')
            && inner.len() > 2
            && inner[..inner.len() - 1].contains(':')
            && inner[..inner.len() - 1]
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || b == b':' || b == b'.');
    }
    // Trailing dots are canonicalized away by the worker's URL parser;
    // mirror that here so `10.0.0.5.` is accepted like the worker does.
    let name = host.trim_end_matches('.');
    !name.is_empty()
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

fn valid_deny_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_POLICY_HOST_LEN
        && name.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// MIG-14: is `host` an IP literal an internal exception may never
/// target? Internal exceptions exist to reach private/CGNAT/ULA space
/// (in-cluster gateways) — but this-net, loopback, link-local,
/// multicast and unspecified are never legitimate gateway addresses.
/// Embedded-IPv4 forms (mapped/compat/NAT64) unwrap to the inner v4
/// and classify it by the same subset. Names return `false` here —
/// the never-allow-name check handles them.
fn never_internal_lit(host: &str) -> bool {
    use std::net::{IpAddr, Ipv4Addr};
    fn v4_bad(a: &Ipv4Addr) -> bool {
        a.octets()[0] == 0 // 0.0.0.0/8 this-network
            || a.is_loopback() // 127.0.0.0/8
            || a.is_link_local() // 169.254.0.0/16 (cloud metadata)
            || a.is_multicast() // 224.0.0.0/4
    }
    let text = host.trim_end_matches('.');
    let bare = text
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(text);
    let Ok(addr) = bare.parse::<IpAddr>() else {
        return false;
    };
    match addr {
        IpAddr::V4(a) => v4_bad(&a),
        IpAddr::V6(a) => {
            if a.is_unspecified() || a.is_loopback() || a.is_multicast() {
                return true;
            }
            // fe80::/10 link-local.
            if (a.segments()[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            // ::ffff:a.b.c.d (mapped) and ::a.b.c.d (compat) unwrap.
            if let Some(inner) = a.to_ipv4_mapped().or_else(|| a.to_ipv4()) {
                return v4_bad(&inner);
            }
            // 64:ff9b::/96 NAT64 — inner is the low 32 bits.
            let segs = a.segments();
            if segs[0] == 0x0064 && segs[1] == 0xff9b {
                let inner = Ipv4Addr::new(
                    (segs[6] >> 8) as u8,
                    segs[6] as u8,
                    (segs[7] >> 8) as u8,
                    segs[7] as u8,
                );
                return v4_bad(&inner);
            }
            false
        }
    }
}

impl EgressPolicy {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.allowed_ports.is_empty() || self.allowed_ports.len() > MAX_ALLOWED_PORTS {
            return Err(ConfigError::Capacity("egressPolicy.allowedPorts"));
        }
        if self.allowed_ports.iter().any(|p| *p == 0) {
            return Err(ConfigError::Identifier("egressPolicy.allowedPorts"));
        }
        if self.allow.len() > MAX_ALLOW_ENTRIES {
            return Err(ConfigError::Capacity("egressPolicy.allow"));
        }
        for entry in &self.allow {
            if entry.port == 0 || !valid_policy_host(&entry.host) {
                return Err(ConfigError::Identifier("egressPolicy.allow"));
            }
            let name = entry.host.trim_end_matches('.').to_ascii_lowercase();
            if NEVER_ALLOW_HOSTS.contains(&name.as_str()) {
                return Err(ConfigError::Identifier("egressPolicy.allow"));
            }
        }
        if self.deny_names.len() > MAX_DENY_NAMES {
            return Err(ConfigError::Capacity("egressPolicy.denyNames"));
        }
        for rule in &self.deny_names {
            let name = match rule {
                EgressNameRule::Exact(n) | EgressNameRule::Suffix(n) => n,
            };
            if !valid_deny_name(name) {
                return Err(ConfigError::Identifier("egressPolicy.denyNames"));
            }
        }
        if !(MIN_REQUEST_TIMEOUT_MS..=MAX_REQUEST_TIMEOUT_MS).contains(&self.request_timeout_ms) {
            return Err(ConfigError::Identifier("egressPolicy.requestTimeoutMs"));
        }
        // MIG-14 TCP dimension. Empty `tcp_allowed_ports` is legal
        // (deny all); a mail port fails the whole policy closed.
        if self.tcp_allowed_ports.len() > MAX_TCP_ALLOWED_PORTS
            || self
                .tcp_allowed_ports
                .iter()
                .any(|p| *p == 0 || TCP_NEVER_PORTS.contains(p))
        {
            return Err(ConfigError::Identifier("egressPolicy.tcpAllowedPorts"));
        }
        if self.internal_tcp_allow.len() > MAX_INTERNAL_TCP_ALLOW {
            return Err(ConfigError::Capacity("egressPolicy.internalTcpAllow"));
        }
        if self.internal_tcp_targets.len() > MAX_INTERNAL_TCP_ALLOW {
            return Err(ConfigError::Capacity("egressPolicy.internalTcpTargets"));
        }
        for entry in &self.internal_tcp_targets {
            if entry.port == 0 || !valid_policy_host(&entry.host) {
                return Err(ConfigError::Identifier("egressPolicy.internalTcpTargets"));
            }
        }
        for entry in &self.internal_tcp_allow {
            let name = entry.host.trim_end_matches('.').to_ascii_lowercase();
            if entry.port == 0
                || TCP_NEVER_PORTS.contains(&entry.port)
                || !valid_policy_host(&entry.host)
                || NEVER_ALLOW_HOSTS.contains(&name.as_str())
                || never_internal_lit(&entry.host)
            {
                return Err(ConfigError::Identifier("egressPolicy.internalTcpAllow"));
            }
        }
        if !(MIN_TCP_TIMEOUT_MS..=MAX_TCP_CONNECT_TIMEOUT_MS)
            .contains(&self.tcp_connect_timeout_ms)
        {
            return Err(ConfigError::Identifier("egressPolicy.tcpConnectTimeoutMs"));
        }
        if !(MIN_TCP_TIMEOUT_MS..=MAX_TCP_IDLE_TIMEOUT_MS).contains(&self.tcp_idle_timeout_ms) {
            return Err(ConfigError::Identifier("egressPolicy.tcpIdleTimeoutMs"));
        }
        if self.tcp_max_concurrent == 0 || self.tcp_max_concurrent > MAX_TCP_MAX_CONCURRENT {
            return Err(ConfigError::Identifier("egressPolicy.tcpMaxConcurrent"));
        }
        Ok(())
    }

    /// Canonical JSON for the `VRT_EGRESS_POLICY` binding. `allowedPorts`
    /// is sorted+deduped so logically equal policies render identically;
    /// `allow`/`denyNames` keep declaration order (operator-meaningful).
    pub fn to_policy_json(&self) -> Result<String, ConfigError> {
        self.validate()?;
        let mut ports = self.allowed_ports.clone();
        ports.sort_unstable();
        ports.dedup();
        let mut tcp_ports = self.tcp_allowed_ports.clone();
        tcp_ports.sort_unstable();
        tcp_ports.dedup();
        let entry = |e: &EgressAllowEntry| json!({ "host": e.host, "port": e.port });
        let doc = json!({
            "allow": self.allow.iter().map(entry).collect::<Vec<_>>(),
            "allowedPorts": ports,
            "denyNames": self.deny_names.iter().map(EgressNameRule::wire).collect::<Vec<_>>(),
            "internalTcpAllow": self.internal_tcp_allow.iter().map(entry).collect::<Vec<_>>(),
            // MIG-15: upstream is ALWAYS the platform's loopback pg-wire
            // proxy — the policy surface cannot select it. `kind` is
            // the typed preamble-protocol selector (r2): the facade maps
            // it to a preamble encoder and fails closed on unknown
            // values, so egress code holds no pg-specific branch.
            "internalTcpTargets": self.internal_tcp_targets.iter().map(|e| {
                json!({ "host": e.host, "port": e.port, "kind": "pg-v1", "upstreamHost": PG_PROXY_HOST, "upstreamPort": PG_PROXY_PORT })
            }).collect::<Vec<_>>(),
            "requestTimeoutMs": self.request_timeout_ms,
            "tcpAllowedPorts": tcp_ports,
            "tcpConnectTimeoutMs": self.tcp_connect_timeout_ms,
            "tcpIdleTimeoutMs": self.tcp_idle_timeout_ms,
            "tcpMaxConcurrent": self.tcp_max_concurrent,
        });
        String::from_utf8(canonical_bytes(&doc))
            .map_err(|_| ConfigError::Identifier("egressPolicy"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_restrictive_and_valid() {
        let policy = EgressPolicy::default();
        assert!(policy.validate().is_ok());
        assert_eq!(
            policy.to_policy_json().unwrap(),
            "{\"allow\":[],\"allowedPorts\":[80,443],\"denyNames\":[],\"internalTcpAllow\":[],\"internalTcpTargets\":[],\"requestTimeoutMs\":30000,\"tcpAllowedPorts\":[43],\"tcpConnectTimeoutMs\":10000,\"tcpIdleTimeoutMs\":60000,\"tcpMaxConcurrent\":64}"
        );
    }

    #[test]
    fn canonical_json_sorts_ports_and_wires_rules() {
        let policy = EgressPolicy {
            allowed_ports: vec![443, 80, 443],
            allow: vec![EgressAllowEntry {
                host: "api.internal".into(),
                port: 8443,
            }],
            deny_names: vec![
                EgressNameRule::Suffix("tenant.example".into()),
                EgressNameRule::Exact("blocked.example".into()),
            ],
            request_timeout_ms: 5_000,
            ..Default::default()
        };
        assert_eq!(
            policy.to_policy_json().unwrap(),
            "{\"allow\":[{\"host\":\"api.internal\",\"port\":8443}],\"allowedPorts\":[80,443],\"denyNames\":[\"*.tenant.example\",\"blocked.example\"],\"internalTcpAllow\":[],\"internalTcpTargets\":[],\"requestTimeoutMs\":5000,\"tcpAllowedPorts\":[43],\"tcpConnectTimeoutMs\":10000,\"tcpIdleTimeoutMs\":60000,\"tcpMaxConcurrent\":64}"
        );
    }

    #[test]
    fn rejects_out_of_bounds() {
        for policy in [
            EgressPolicy {
                allowed_ports: vec![],
                ..Default::default()
            },
            EgressPolicy {
                allowed_ports: vec![0],
                ..Default::default()
            },
            EgressPolicy {
                allowed_ports: vec![1; 33],
                ..Default::default()
            },
            EgressPolicy {
                allow: vec![EgressAllowEntry {
                    host: String::new(),
                    port: 443,
                }],
                ..Default::default()
            },
            EgressPolicy {
                allow: vec![EgressAllowEntry {
                    host: "ok.example".into(),
                    port: 0,
                }],
                ..Default::default()
            },
            EgressPolicy {
                allow: vec![EgressAllowEntry {
                    host: "bad host!".into(),
                    port: 443,
                }],
                ..Default::default()
            },
            EgressPolicy {
                deny_names: vec![EgressNameRule::Exact("bad name".into())],
                ..Default::default()
            },
            EgressPolicy {
                request_timeout_ms: 999,
                ..Default::default()
            },
            EgressPolicy {
                request_timeout_ms: 120_001,
                ..Default::default()
            },
            // MIG-14 TCP bounds.
            EgressPolicy {
                tcp_allowed_ports: vec![1; 33],
                ..Default::default()
            },
            EgressPolicy {
                tcp_allowed_ports: vec![0],
                ..Default::default()
            },
            EgressPolicy {
                tcp_allowed_ports: vec![43, 25],
                ..Default::default()
            },
            EgressPolicy {
                tcp_connect_timeout_ms: 999,
                ..Default::default()
            },
            EgressPolicy {
                tcp_connect_timeout_ms: 120_001,
                ..Default::default()
            },
            EgressPolicy {
                tcp_idle_timeout_ms: 600_001,
                ..Default::default()
            },
            EgressPolicy {
                tcp_max_concurrent: 0,
                ..Default::default()
            },
            EgressPolicy {
                tcp_max_concurrent: 1025,
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "pg.internal".into(),
                    port: 25,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "localhost".into(),
                    port: 5432,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "metadata.google.internal".into(),
                    port: 80,
                }],
                ..Default::default()
            },
            // Internal exceptions may carry private literals, never
            // trivially-local ones.
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "127.0.0.1".into(),
                    port: 5432,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "169.254.169.254".into(),
                    port: 80,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "0.0.0.1".into(),
                    port: 443,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "224.0.0.1".into(),
                    port: 443,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "[::1]".into(),
                    port: 5432,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "[fe80::1]".into(),
                    port: 5432,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "[::ffff:127.0.0.1]".into(),
                    port: 5432,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "[::ffff:169.254.169.254]".into(),
                    port: 80,
                }],
                ..Default::default()
            },
            EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: "[64:ff9b::127.0.0.1]".into(),
                    port: 80,
                }],
                ..Default::default()
            },
        ] {
            assert!(policy.validate().is_err(), "{policy:?}");
            assert!(policy.to_policy_json().is_err(), "{policy:?}");
        }
    }

    /// MIG-14: private/CGNAT/ULA literals and internal names ARE the
    /// internal exception's purpose — they validate; SMTP ports and
    /// local space never do.
    #[test]
    fn internal_tcp_allow_accepts_private_literals_and_internal_names() {
        for host in [
            "10.42.3.7",
            "192.168.1.20",
            "172.16.8.9",
            "100.64.1.1",
            "203.0.113.9", // reserved/doc — allowed, just unreachable
            "[fd00::10]",
            "pg.vibeploy.internal",
            "pg.vrt-system.svc",
            "8.8.8.8",
            "[::ffff:10.42.3.7]", // embedded private unwrap → allowed
        ] {
            let policy = EgressPolicy {
                internal_tcp_allow: vec![EgressAllowEntry {
                    host: host.into(),
                    port: 5432,
                }],
                ..Default::default()
            };
            assert!(policy.validate().is_ok(), "{host}");
        }
    }

    /// MIG-14: mail ports are unreachable through TCP even when a
    /// platform exception tries to name them — checked at both the
    /// tcp_allowed_ports and internal_tcp_allow levels above, and
    /// worker-side again before evaluation.
    #[test]
    fn tcp_allowed_ports_may_be_empty() {
        let policy = EgressPolicy {
            tcp_allowed_ports: vec![],
            ..Default::default()
        };
        assert!(policy.validate().is_ok());
        assert!(policy.to_policy_json().unwrap().contains("\"tcpAllowedPorts\":[]"));
    }

    #[test]
    fn accepts_bracket_v6_allow_host() {
        let policy = EgressPolicy {
            allow: vec![EgressAllowEntry {
                host: "[fd00::10]".into(),
                port: 9000,
            }],
            ..Default::default()
        };
        assert!(policy.validate().is_ok());
    }
}
