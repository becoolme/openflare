//! Kubernetes `NetworkPolicy` rendering for runtime-cell pods (T6).
//!
//! The rendered manifest is the second egress defense layer described in
//! `doc/platform/workers-runtime-egress.md`: the pod may talk to
//! kube-dns, to the explicitly configured platform backends (Kvrocks,
//! S3/PG gateways), and to *public* IP space only — every private,
//! loopback, link-local, CGNAT, multicast, ULA, and cluster CIDR is cut
//! out via `ipBlock.except`. Ingress is restricted to the configured
//! entry selectors on the vrt-host port.
//!
//! Output is deterministic YAML bytes (no `yaml` dependency — scalars
//! are charset-validated then quoted). Nothing here touches a cluster:
//! the T8 controller applies the manifest.
//!
//! 未证实: CNI `except` 支持与 IPv6 NetworkPolicy 支持取决于部署的
//! CNI（Cilium/Calico 支持，部分轻量 CNI 不实现）— 见 egress 文档。

use crate::error::ConfigError;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::str::FromStr;

/// A canonical CIDR — host bits must be zero on construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let (addr_s, prefix_s) = text
            .split_once('/')
            .ok_or(ConfigError::Identifier("netpolicy.cidr"))?;
        let addr =
            IpAddr::from_str(addr_s).map_err(|_| ConfigError::Identifier("netpolicy.cidr"))?;
        if prefix_s.is_empty()
            || prefix_s.len() > 3
            || !prefix_s.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(ConfigError::Identifier("netpolicy.cidr"));
        }
        let prefix: u8 = prefix_s
            .parse()
            .map_err(|_| ConfigError::Identifier("netpolicy.cidr"))?;
        if prefix > addr_bits(&addr) {
            return Err(ConfigError::Identifier("netpolicy.cidr"));
        }
        let cidr = Self { addr, prefix };
        if !cidr.is_canonical() {
            return Err(ConfigError::Identifier("netpolicy.cidr"));
        }
        Ok(cidr)
    }

    pub fn is_ipv4(&self) -> bool {
        self.addr.is_ipv4()
    }

    /// MIG-9: prefix length (0..=32 v4, 0..=128 v6) — lets config
    /// validation bound how broad a trusted CIDR may be.
    pub fn prefix_len(&self) -> u8 {
        self.prefix
    }

    /// Canonical display: `IpAddr` Display is already compressed/lower.
    pub fn text(&self) -> String {
        format!("{}/{}", self.addr, self.prefix)
    }

    /// MIG-7: membership test — does `ip` fall inside this prefix?
    /// Families must match (a v4 peer never satisfies a v6 range).
    pub fn contains(&self, ip: &IpAddr) -> bool {
        if self.is_ipv4() != ip.is_ipv4() {
            return false;
        }
        if self.prefix == 0 {
            return true;
        }
        let host_bits = u32::from(addr_bits(ip)) - u32::from(self.prefix);
        let ip_u128 = match ip {
            IpAddr::V4(v) => u128::from(u32::from(*v)),
            IpAddr::V6(v) => u128::from(*v),
        };
        (self.to_u128() ^ ip_u128) >> host_bits == 0
    }

    /// True when this CIDR is, lies inside, or contains any part of the
    /// IPv4-mapped IPv6 range `::ffff:0:0/96`. Decided on the parsed
    /// address bits (80 zero bits then 16 one bits), never on spelling.
    pub fn overlaps_ipv4_mapped(&self) -> bool {
        const MAPPED: u128 = 0xffff_u128 << 32;
        match self.addr {
            IpAddr::V4(_) => false,
            IpAddr::V6(_) => {
                let bits = u32::from(self.prefix.min(96));
                if bits == 0 {
                    return true;
                }
                let shift = 128 - bits;
                (self.to_u128() >> shift) == (MAPPED >> shift)
            }
        }
    }

    fn to_u128(&self) -> u128 {
        match self.addr {
            IpAddr::V4(a) => u128::from(u32::from(a)),
            IpAddr::V6(a) => u128::from(a),
        }
    }

    fn is_canonical(&self) -> bool {
        let host = u32::from(addr_bits(&self.addr)) - u32::from(self.prefix);
        if host == 0 {
            return true;
        }
        let mask = if host >= 128 {
            u128::MAX
        } else {
            (1u128 << host) - 1
        };
        self.to_u128() & mask == 0
    }
}

impl<'de> serde::Deserialize<'de> for Cidr {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        Cidr::parse(&text).map_err(serde::de::Error::custom)
    }
}

fn addr_bits(addr: &IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
    Sctp,
}

impl Protocol {
    fn yaml(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
            Self::Sctp => "SCTP",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetPort {
    pub protocol: Protocol,
    pub port: u16,
}

/// A `namespaceSelector`+`podSelector` peer (AND semantics); at least
/// one label map must be non-empty.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SelectorPeer {
    pub namespace_labels: BTreeMap<String, String>,
    pub pod_labels: BTreeMap<String, String>,
}

/// A platform backend the pod may egress to — either a selector peer or
/// an explicit CIDR, plus its single reachable port.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum EgressBackend {
    Selector { peer: SelectorPeer, port: NetPort },
    Cidr { cidr: Cidr, port: NetPort },
}

/// Typed input for the runtime-pod NetworkPolicy. Deserializable so
/// the cell-controller config can carry it directly (`netpolicy` table).
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeNetPolicy {
    pub policy_name: String,
    pub namespace: String,
    /// Labels selecting the runtime pods this policy governs.
    pub pod_selector: BTreeMap<String, String>,
    /// Entry sources allowed to reach `ingress_port` (Traefik /
    /// tenant-gateway), one `from` peer each.
    pub ingress_peers: Vec<SelectorPeer>,
    /// The vrt-host listen port the entry may reach.
    pub ingress_port: NetPort,
    /// Where kube-dns lives (namespace + pod labels). DNS egress is
    /// restricted to this peer on UDP+TCP 53 only.
    pub dns_peer: SelectorPeer,
    /// Kvrocks / S3 / PG gateways — selector- or CIDR-addressed.
    pub backends: Vec<EgressBackend>,
    /// T13/M1: ports reachable on PUBLIC ips — bare `0.0.0.0/0`+`::/0`
    /// with no port list meant every public port was reachable. Must
    /// be non-empty; TCP 80/443 is the expected production set.
    pub public_ports: Vec<NetPort>,
    /// Cluster Pod CIDRs cut out of the public `0.0.0.0/0` `except` list.
    /// Non-empty (and per enabled family — see `ipv6_enabled`).
    pub pod_cidrs: Vec<Cidr>,
    /// Cluster Service CIDRs cut out the same way. Non-empty.
    pub service_cidrs: Vec<Cidr>,
    /// T13/M1: node public IPs and platform entry points (kube-apiserver
    /// 6443, kubelet 10250, Traefik, public DB ports) — public-shaped
    /// addresses that must never be reachable from tenant pods. An
    /// operator-supplied REQUIRED list; auto-collecting from Node
    /// objects is a follow-up (未证实).
    pub node_excepts: Vec<Cidr>,
    /// Additional operator CIDRs to except (e.g. a platform LAN that is
    /// public-shaped but must not be reachable).
    #[serde(default)]
    pub extra_excepts: Vec<Cidr>,
    /// T13/M1: when true the pod/service CIDR lists must each contain
    /// at least one IPv6 entry — cluster IPv6 is enabled, so a v4-only
    /// except list would leave v6 GUA pod space reachable.
    #[serde(default)]
    pub ipv6_enabled: bool,
}

const MAX_SELECTOR_LABELS: usize = 16;
const MAX_PEERS: usize = 16;
const MAX_BACKENDS: usize = 16;
const MAX_EXCEPTS: usize = 64;

/// The built-in non-public CIDRs cut out of the public `ipBlock`s —
/// derived from [`crate::egress::IPV4_DENY_RANGES`] /
/// [`crate::egress::IPV6_DENY_RANGES`] (single source of truth, also
/// exported to `contract/egress-deny-ranges.json`). For v6 the
/// embedded-IPv4 prefixes are blocked whole as well, except the
/// IPv4-mapped one k8s cannot express (see
/// [`crate::egress::NETPOL_UNREPRESENTABLE_PREFIXES`]).
fn builtin_excepts(family_v4: bool) -> Vec<Cidr> {
    let texts: Vec<&str> = if family_v4 {
        crate::egress::IPV4_DENY_RANGES
            .iter()
            .map(|r| r.0)
            .collect()
    } else {
        crate::egress::IPV6_DENY_RANGES
            .iter()
            .map(|r| r.0)
            .chain(crate::egress::netpol_embedded_excepts())
            .collect()
    };
    texts
        .iter()
        .map(|t| Cidr::parse(t).expect("builtin deny CIDR"))
        .collect()
}

// --- Kubernetes label/DNS validation -----------------------------

fn valid_label_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
        && s.bytes().last().is_some_and(|b| b.is_ascii_alphanumeric())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn valid_label_value(s: &str) -> bool {
    s.is_empty() || valid_label_name(s)
}

fn valid_dns_subdomain(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 253
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-'))
        && s.split('.')
            .all(|l| !l.is_empty() && !l.starts_with('-') && !l.ends_with('-'))
}

fn valid_dns_label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !s.starts_with('-')
        && !s.ends_with('-')
}

fn valid_label_key(s: &str) -> bool {
    match s.split_once('/') {
        None => valid_label_name(s),
        Some((prefix, name)) => {
            prefix.len() <= 253 && valid_dns_subdomain(prefix) && valid_label_name(name)
        }
    }
}

fn valid_labels(map: &BTreeMap<String, String>) -> bool {
    map.len() <= MAX_SELECTOR_LABELS
        && map
            .iter()
            .all(|(k, v)| valid_label_key(k) && valid_label_value(v))
}

fn valid_peer(peer: &SelectorPeer) -> bool {
    (!peer.namespace_labels.is_empty() || !peer.pod_labels.is_empty())
        && valid_labels(&peer.namespace_labels)
        && valid_labels(&peer.pod_labels)
}

fn valid_port(port: &NetPort) -> bool {
    port.port != 0
}

// --- validation ---------------------------------------------------

fn validate(input: &RuntimeNetPolicy) -> Result<(), ConfigError> {
    if !valid_dns_subdomain(&input.policy_name) {
        return Err(ConfigError::Identifier("netpolicy.name"));
    }
    if !valid_dns_label(&input.namespace) {
        return Err(ConfigError::Identifier("netpolicy.namespace"));
    }
    if input.pod_selector.is_empty() || !valid_labels(&input.pod_selector) {
        return Err(ConfigError::Identifier("netpolicy.podSelector"));
    }
    if input.ingress_peers.is_empty() || input.ingress_peers.len() > MAX_PEERS {
        return Err(ConfigError::Capacity("netpolicy.ingress"));
    }
    for peer in &input.ingress_peers {
        if !valid_peer(peer) {
            return Err(ConfigError::Identifier("netpolicy.ingress"));
        }
    }
    if !valid_port(&input.ingress_port) {
        return Err(ConfigError::Identifier("netpolicy.ingressPort"));
    }
    if !valid_peer(&input.dns_peer) {
        return Err(ConfigError::Identifier("netpolicy.dns"));
    }
    if input.backends.is_empty() || input.backends.len() > MAX_BACKENDS {
        return Err(ConfigError::Capacity("netpolicy.backends"));
    }
    for backend in &input.backends {
        match backend {
            EgressBackend::Selector { peer, port } => {
                if !valid_peer(peer) || !valid_port(port) {
                    return Err(ConfigError::Identifier("netpolicy.backends"));
                }
            }
            EgressBackend::Cidr { port, .. } => {
                if !valid_port(port) {
                    return Err(ConfigError::Identifier("netpolicy.backends"));
                }
            }
        }
    }
    if input.pod_cidrs.len()
        + input.service_cidrs.len()
        + input.extra_excepts.len()
        + input.node_excepts.len()
        > MAX_EXCEPTS
    {
        return Err(ConfigError::Capacity("netpolicy.excepts"));
    }
    // The Kubernetes API server rejects IPv4-mapped IPv6 prefixes in
    // `ipBlock.except`; fail closed here instead of failing the rollout.
    for (field, list) in [
        ("netpolicy.podCidrs", &input.pod_cidrs),
        ("netpolicy.serviceCidrs", &input.service_cidrs),
        ("netpolicy.nodeExcepts", &input.node_excepts),
        ("netpolicy.extraExcepts", &input.extra_excepts),
    ] {
        if let Some(c) = list.iter().find(|c| c.overlaps_ipv4_mapped()) {
            return Err(ConfigError::Ipv4MappedExcept {
                field,
                cidr: c.text(),
            });
        }
    }
    // T13/M1: the public-space rule must be port-bounded — an unbounded
    // `0.0.0.0/0` rule opens every port on every public host.
    if input.public_ports.is_empty()
        || input.public_ports.len() > MAX_PUBLIC_PORTS
        || input.public_ports.iter().any(|p| !valid_port(p))
    {
        return Err(ConfigError::Identifier("netpolicy.publicPorts"));
    }
    // CIDR coverage is REQUIRED — without it private cluster space
    // is reachable through the "public" ipBlocks.
    if input.pod_cidrs.is_empty() {
        return Err(ConfigError::Identifier("netpolicy.podCidrs"));
    }
    if input.service_cidrs.is_empty() {
        return Err(ConfigError::Identifier("netpolicy.serviceCidrs"));
    }
    // Node/platform public addresses are REQUIRED excepts — the
    // operator must declare them explicitly (auto-discovery is a
    // follow-up; see the egress doc's 未证实 notes).
    if input.node_excepts.is_empty() {
        return Err(ConfigError::Identifier("netpolicy.nodeExcepts"));
    }
    // Every enabled address family must be covered: when the cluster
    // is dual-stack, v4-only except lists leave v6 GUA pod/service
    // space reachable through the `::/0` rule.
    if input.ipv6_enabled {
        if !input.pod_cidrs.iter().any(|c| !c.is_ipv4()) {
            return Err(ConfigError::Identifier("netpolicy.podCidrs"));
        }
        if !input.service_cidrs.iter().any(|c| !c.is_ipv4()) {
            return Err(ConfigError::Identifier("netpolicy.serviceCidrs"));
        }
    }
    Ok(())
}

const MAX_PUBLIC_PORTS: usize = 8;

// --- deterministic YAML emit (charset-validated scalars, quoted) -----

fn line(out: &mut String, indent: usize, text: &str) {
    out.push_str(&" ".repeat(indent));
    out.push_str(text);
    out.push('\n');
}

fn q(s: &str) -> String {
    format!("\"{s}\"")
}

fn emit_label_map(out: &mut String, indent: usize, labels: &BTreeMap<String, String>) {
    for (k, v) in labels {
        line(out, indent, &format!("{}: {}", q(k), q(v)));
    }
}

fn emit_selector_peer(out: &mut String, indent: usize, peer: &SelectorPeer) {
    if !peer.namespace_labels.is_empty() {
        line(out, indent, "- namespaceSelector:");
        line(out, indent + 4, "matchLabels:");
        emit_label_map(out, indent + 6, &peer.namespace_labels);
    }
    if !peer.pod_labels.is_empty() {
        // Shares the `- ` item when a namespaceSelector precedes it.
        let base = if peer.namespace_labels.is_empty() {
            indent
        } else {
            indent + 2
        };
        if peer.namespace_labels.is_empty() {
            line(out, base, "- podSelector:");
        } else {
            line(out, base, "podSelector:");
        }
        line(out, indent + 4, "matchLabels:");
        emit_label_map(out, indent + 6, &peer.pod_labels);
    }
}

fn emit_port(out: &mut String, indent: usize, port: &NetPort) {
    line(
        out,
        indent,
        &format!("- protocol: {}", q(port.protocol.yaml())),
    );
    line(out, indent + 2, &format!("port: {}", port.port));
}

/// Sorted (by address value, then prefix) unique canonical texts of all
/// `except` CIDRs for one address family.
fn excepts(family_v4: bool, input: &RuntimeNetPolicy) -> Vec<String> {
    let mut all: Vec<Cidr> = builtin_excepts(family_v4);
    all.extend(input.pod_cidrs.iter().filter(|c| c.is_ipv4() == family_v4));
    all.extend(
        input
            .service_cidrs
            .iter()
            .filter(|c| c.is_ipv4() == family_v4),
    );
    all.extend(
        input
            .node_excepts
            .iter()
            .filter(|c| c.is_ipv4() == family_v4),
    );
    all.extend(
        input
            .extra_excepts
            .iter()
            .filter(|c| c.is_ipv4() == family_v4),
    );
    all.sort_by_key(|c| (c.to_u128(), c.prefix));
    all.dedup_by_key(|c| (c.to_u128(), c.prefix));
    all.iter().map(Cidr::text).collect()
}

/// Render the runtime-pod `NetworkPolicy` as deterministic YAML.
pub fn render_network_policy(input: &RuntimeNetPolicy) -> Result<String, ConfigError> {
    validate(input)?;
    let mut out = String::new();
    line(
        &mut out,
        0,
        "# Generated by vrt-config render_network_policy — deterministic, do not edit.",
    );
    line(
        &mut out,
        0,
        &format!("apiVersion: {}", q("networking.k8s.io/v1")),
    );
    line(&mut out, 0, &format!("kind: {}", q("NetworkPolicy")));
    line(&mut out, 0, "metadata:");
    line(&mut out, 2, &format!("name: {}", q(&input.policy_name)));
    line(&mut out, 2, &format!("namespace: {}", q(&input.namespace)));
    line(&mut out, 0, "spec:");
    line(&mut out, 2, "podSelector:");
    line(&mut out, 4, "matchLabels:");
    emit_label_map(&mut out, 6, &input.pod_selector);
    line(&mut out, 2, "policyTypes:");
    line(&mut out, 4, &format!("- {}", q("Ingress")));
    line(&mut out, 4, &format!("- {}", q("Egress")));
    line(&mut out, 2, "ingress:");
    for peer in &input.ingress_peers {
        line(&mut out, 4, "- from:");
        emit_selector_peer(&mut out, 8, peer);
        line(&mut out, 6, "ports:");
        emit_port(&mut out, 8, &input.ingress_port);
    }
    line(&mut out, 2, "egress:");
    // DNS to kube-dns only, UDP+TCP 53.
    line(&mut out, 4, "- to:");
    emit_selector_peer(&mut out, 8, &input.dns_peer);
    line(&mut out, 6, "ports:");
    emit_port(
        &mut out,
        8,
        &NetPort {
            protocol: Protocol::Udp,
            port: 53,
        },
    );
    emit_port(
        &mut out,
        8,
        &NetPort {
            protocol: Protocol::Tcp,
            port: 53,
        },
    );
    // Platform backends (Kvrocks, S3 gateway, PG gateway, …).
    for backend in &input.backends {
        line(&mut out, 4, "- to:");
        match backend {
            EgressBackend::Selector { peer, port } => {
                emit_selector_peer(&mut out, 8, peer);
                line(&mut out, 6, "ports:");
                emit_port(&mut out, 8, port);
            }
            EgressBackend::Cidr { cidr, port } => {
                line(&mut out, 8, "- ipBlock:");
                line(&mut out, 12, &format!("cidr: {}", q(&cidr.text())));
                line(&mut out, 6, "ports:");
                emit_port(&mut out, 8, port);
            }
        }
    }
    // Public space only — everything private/loopback/link-local/ULA/
    // multicast/cluster-CIDR is cut out of both `0.0.0.0/0` and `::/0`.
    line(&mut out, 4, "- to:");
    line(&mut out, 8, "- ipBlock:");
    line(&mut out, 12, &format!("cidr: {}", q("0.0.0.0/0")));
    line(&mut out, 12, "except:");
    for c in excepts(true, input) {
        line(&mut out, 14, &format!("- {}", q(&c)));
    }
    line(&mut out, 8, "- ipBlock:");
    line(&mut out, 12, &format!("cidr: {}", q("::/0")));
    line(&mut out, 12, "except:");
    for c in excepts(false, input) {
        line(&mut out, 14, &format!("- {}", q(&c)));
    }
    // T13/M1: the public rule is PORT-BOUNDED (e.g. TCP 80/443) — no
    // more all-ports-all-protocols to public space.
    line(&mut out, 6, "ports:");
    for port in &input.public_ports {
        emit_port(&mut out, 8, port);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// MIG-7: Cidr::contains — family match + prefix math + edge cases.
    #[test]
    fn cidr_contains() {
        let pod = Cidr::parse("10.42.0.0/16").unwrap();
        assert!(pod.contains(&IpAddr::from_str("10.42.1.9").unwrap()));
        assert!(pod.contains(&IpAddr::from_str("10.42.0.0").unwrap()));
        assert!(pod.contains(&IpAddr::from_str("10.42.255.255").unwrap()));
        assert!(!pod.contains(&IpAddr::from_str("10.43.0.1").unwrap()));
        assert!(!pod.contains(&IpAddr::from_str("10.41.255.255").unwrap()));
        // Family mismatch never matches.
        let v6 = Cidr::parse("fd00::/8").unwrap();
        assert!(!v6.contains(&IpAddr::from_str("10.42.1.9").unwrap()));
        assert!(!pod.contains(&IpAddr::from_str("fd00::1").unwrap()));
        assert!(v6.contains(&IpAddr::from_str("fd12::1").unwrap()));
        // Exact /32 and /128.
        let host4 = Cidr::parse("192.0.2.7/32").unwrap();
        assert!(host4.contains(&IpAddr::from_str("192.0.2.7").unwrap()));
        assert!(!host4.contains(&IpAddr::from_str("192.0.2.8").unwrap()));
        let host6 = Cidr::parse("2001:db8::1/128").unwrap();
        assert!(host6.contains(&IpAddr::from_str("2001:db8::1").unwrap()));
        assert!(!host6.contains(&IpAddr::from_str("2001:db8::2").unwrap()));
        // /0 matches everything in its family only.
        let all4 = Cidr::parse("0.0.0.0/0").unwrap();
        assert!(all4.contains(&IpAddr::from_str("203.0.113.9").unwrap()));
        assert!(!all4.contains(&IpAddr::from_str("::1").unwrap()));
        // A v4-mapped-v6 peer is IpAddr::V6 → v4 CIDR never matches it
        // (no silent cross-family promotion).
        assert!(!pod.contains(&IpAddr::from_str("::ffff:10.42.1.9").unwrap()));
    }
}
