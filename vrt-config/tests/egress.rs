//! T6 egress integration tests: `EgressPolicy` render wiring is covered
//! by `golden.rs`; this file pins `netpolicy` — typed input validation
//! and the deterministic NetworkPolicy YAML golden.

use std::collections::BTreeMap;
use vrt_config::*;

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

/// The representative runtime-cell policy: k3s conventions for
/// kube-dns/traefik, selector-addressed kvrocks + PG gateway, and a
/// CIDR-addressed S3 gateway.
pub fn net_policy() -> RuntimeNetPolicy {
    RuntimeNetPolicy {
        policy_name: "vrt-runtime-egress".into(),
        namespace: "vrt-runtime".into(),
        pod_selector: labels(&[("app", "vrt-host")]),
        ingress_peers: vec![
            SelectorPeer {
                namespace_labels: labels(&[("kubernetes.io/metadata.name", "traefik")]),
                pod_labels: labels(&[("app.kubernetes.io/name", "traefik")]),
            },
            SelectorPeer {
                namespace_labels: labels(&[("kubernetes.io/metadata.name", "vrt-system")]),
                pod_labels: labels(&[("app", "tenant-gateway")]),
            },
        ],
        ingress_port: NetPort {
            protocol: Protocol::Tcp,
            port: 8080,
        },
        dns_peer: SelectorPeer {
            namespace_labels: labels(&[("kubernetes.io/metadata.name", "kube-system")]),
            pod_labels: labels(&[("k8s-app", "kube-dns")]),
        },
        backends: vec![
            EgressBackend::Selector {
                peer: SelectorPeer {
                    namespace_labels: labels(&[("kubernetes.io/metadata.name", "vrt-system")]),
                    pod_labels: labels(&[("app", "kvrocks")]),
                },
                port: NetPort {
                    protocol: Protocol::Tcp,
                    port: 6666,
                },
            },
            EgressBackend::Selector {
                peer: SelectorPeer {
                    namespace_labels: labels(&[("kubernetes.io/metadata.name", "vrt-system")]),
                    pod_labels: labels(&[("app", "pg-gateway")]),
                },
                port: NetPort {
                    protocol: Protocol::Tcp,
                    port: 8432,
                },
            },
            EgressBackend::Cidr {
                cidr: Cidr::parse("10.30.0.0/24").unwrap(),
                port: NetPort {
                    protocol: Protocol::Tcp,
                    port: 443,
                },
            },
        ],
        // T13/M1 + MIG-14: public egress restricted to TCP 43/80/443 —
        // the egress policy's default tcpAllowedPorts + allowedPorts.
        public_ports: vec![
            NetPort {
                protocol: Protocol::Tcp,
                port: 43,
            },
            NetPort {
                protocol: Protocol::Tcp,
                port: 80,
            },
            NetPort {
                protocol: Protocol::Tcp,
                port: 443,
            },
        ],
        pod_cidrs: vec![Cidr::parse("10.42.0.0/16").unwrap()],
        service_cidrs: vec![Cidr::parse("10.43.0.0/16").unwrap()],
        // T13/M1: node/platform public addresses are REQUIRED excepts.
        node_excepts: vec![
            Cidr::parse("203.0.113.10/32").unwrap(), // node public IP
            Cidr::parse("198.51.100.5/32").unwrap(), // platform entrypoint
        ],
        ipv6_enabled: false,
        extra_excepts: vec![
            Cidr::parse("10.20.1.0/24").unwrap(),
            Cidr::parse("fd00:20::/64").unwrap(),
        ],
    }
}

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/network-policy.yaml"
);

#[test]
fn golden_yaml_matches_byte_for_byte() {
    let rendered = render_network_policy(&net_policy()).unwrap();
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN, &rendered).unwrap();
    }
    let expected = std::fs::read_to_string(GOLDEN)
        .expect("missing golden file; run UPDATE_GOLDEN=1 cargo test -p vrt-config");
    assert_eq!(rendered, expected, "golden mismatch");
}

/// k8s rejects IPv4-mapped ipBlock excepts and requires every except
/// to be a strict subset of its cidr: assert both on the rendered text.
#[test]
fn rendered_excepts_are_never_ipv4_mapped_and_strict_subsets() {
    let out = render_network_policy(&net_policy()).unwrap();
    assert!(!out.contains("::ffff:"), "IPv4-mapped prefix rendered:\n{out}");
    // The worker-side table still carries the mapped prefix (facade denies it).
    assert!(egress::EMBEDDED_IPV4_PREFIXES.contains(&"::ffff:0.0.0.0/96"));
    for p in egress::NETPOL_UNREPRESENTABLE_PREFIXES {
        assert!(egress::EMBEDDED_IPV4_PREFIXES.contains(p));
        assert!(!egress::netpol_embedded_excepts().any(|x| x == *p));
    }
    let mut cidr: Option<Cidr> = None;
    let mut in_except = false;
    let mut seen = 0;
    for raw in out.lines() {
        let l = raw.trim();
        if let Some(rest) = l.strip_prefix("cidr:") {
            cidr = Some(Cidr::parse(rest.trim().trim_matches('"')).unwrap());
            in_except = false;
        } else if l.starts_with("except:") {
            in_except = true;
        } else if in_except && l.starts_with("- \"") {
            let text = l[2..].trim().trim_matches('"');
            let ex = Cidr::parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            let base = cidr.expect("except before cidr");
            assert_eq!(ex.is_ipv4(), base.is_ipv4(), "family mismatch {l}");
            assert!(ex.prefix_len() > base.prefix_len(), "not strict subset {l}");
            if let Ok(std::net::IpAddr::V6(a)) = text.split('/').next().unwrap().parse() {
                assert!(a.segments()[..6] != [0, 0, 0, 0, 0, 0xffff], "ipv4-mapped except {l}");
            }
            seen += 1;
        } else {
            in_except = false;
        }
    }
    assert!(seen > 20, "too few excepts parsed: {seen}");
}

#[test]
fn render_is_deterministic() {
    let a = render_network_policy(&net_policy()).unwrap();
    let b = render_network_policy(&net_policy()).unwrap();
    assert_eq!(a, b);
}

#[test]
fn rendered_policy_shape() {
    let out = render_network_policy(&net_policy()).unwrap();
    // podSelector + policyTypes + ingress/egress rules present.
    assert!(out.contains("\"app\": \"vrt-host\""));
    assert!(out.contains("policyTypes:"));
    // DNS: kube-dns selector + UDP/TCP 53.
    assert!(out.contains("\"kubernetes.io/metadata.name\": \"kube-system\""));
    assert!(out.contains("\"k8s-app\": \"kube-dns\""));
    assert!(out.contains("- protocol: \"UDP\"\n          port: 53"));
    assert!(out.contains("- protocol: \"TCP\"\n          port: 53"));
    // Backends.
    assert!(out.contains("\"app\": \"kvrocks\""));
    assert!(out.contains("port: 6666"));
    assert!(out.contains("\"app\": \"pg-gateway\""));
    assert!(out.contains("cidr: \"10.30.0.0/24\""));
    // Public blocks with the full except lists.
    // The public `except` list is exactly the built-in deny table
    // (single source: `egress::IPV4/IPV6_DENY_RANGES` + embedded
    // prefixes) plus the configured cluster CIDRs and extra_excepts.
    assert!(out.contains("cidr: \"0.0.0.0/0\""));
    for (cidr, _) in egress::IPV4_DENY_RANGES {
        assert!(
            out.contains(&format!("- \"{cidr}\"")),
            "missing except {cidr}"
        );
    }
    for cidr in ["10.42.0.0/16", "10.43.0.0/16", "10.20.1.0/24"] {
        assert!(
            out.contains(&format!("- \"{cidr}\"")),
            "missing except {cidr}"
        );
    }
    assert!(out.contains("cidr: \"::/0\""));
    for (cidr, _) in egress::IPV6_DENY_RANGES {
        assert!(
            out.contains(&format!("- \"{cidr}\"")),
            "missing except {cidr}"
        );
    }
    for cidr in egress::netpol_embedded_excepts() {
        assert!(
            out.contains(&format!("- \"{cidr}\"")),
            "missing except {cidr}"
        );
    }
    assert!(out.contains("- \"fd00:20::/64\""));
}

#[test]
fn rejects_invalid_inputs() {
    let base = net_policy();
    for mutate in [
        |p: &mut RuntimeNetPolicy| p.policy_name = "Bad_Name".into(),
        |p: &mut RuntimeNetPolicy| p.policy_name = "".into(),
        |p: &mut RuntimeNetPolicy| p.namespace = "Bad Ns".into(),
        |p: &mut RuntimeNetPolicy| p.pod_selector.clear(),
        |p: &mut RuntimeNetPolicy| {
            p.pod_selector.insert("bad key!".into(), "v".into());
        },
        |p: &mut RuntimeNetPolicy| {
            p.pod_selector.insert("k".into(), "bad value!".into());
        },
        |p: &mut RuntimeNetPolicy| p.ingress_peers.clear(),
        |p: &mut RuntimeNetPolicy| {
            p.ingress_peers.push(SelectorPeer::default()); // empty peer = match-all
        },
        |p: &mut RuntimeNetPolicy| p.ingress_port.port = 0,
        |p: &mut RuntimeNetPolicy| p.dns_peer = SelectorPeer::default(),
        |p: &mut RuntimeNetPolicy| p.backends.clear(),
        |p: &mut RuntimeNetPolicy| p.public_ports.clear(),
        |p: &mut RuntimeNetPolicy| {
            p.public_ports.push(NetPort {
                protocol: Protocol::Tcp,
                port: 0,
            });
        },
        |p: &mut RuntimeNetPolicy| p.pod_cidrs.clear(),
        |p: &mut RuntimeNetPolicy| p.service_cidrs.clear(),
        |p: &mut RuntimeNetPolicy| p.node_excepts.clear(),
        |p: &mut RuntimeNetPolicy| {
            p.ipv6_enabled = true; // dual-stack without v6 cidrs → reject
        },
        |p: &mut RuntimeNetPolicy| {
            p.backends.push(EgressBackend::Cidr {
                cidr: Cidr::parse("0.0.0.0/0").unwrap(),
                port: NetPort {
                    protocol: Protocol::Tcp,
                    port: 0,
                },
            });
        },
        |p: &mut RuntimeNetPolicy| {
            p.dns_peer.pod_labels.insert("bad-key!!".into(), "v".into());
        },
    ] {
        let mut p = base.clone();
        mutate(&mut p);
        assert!(render_network_policy(&p).is_err(), "{p:?}");
    }
}

#[test]
fn cidr_validation() {
    assert!(Cidr::parse("10.0.0.0/8").is_ok());
    assert!(Cidr::parse("::/0").is_ok());
    assert!(Cidr::parse("fc00::/7").is_ok());
    for bad in [
        "10.0.0.1/8",  // host bits set — not canonical
        "010.0.0.0/8", // leading zero — Rust std rejects octal-looking octets
        "10.0.0.0/33", // prefix out of range
        "10.0.0.0/-1",
        "10.0.0.0/",
        "10.0.0.0",
        "300.0.0.0/8",
        "fc00::/129",
        "gg::/7",
        "10.0.0.0/8junk",
        "",
    ] {
        assert!(Cidr::parse(bad).is_err(), "expected reject: {bad:?}");
    }
    // Case-insensitive v6 spellings normalize to the canonical text.
    assert_eq!(Cidr::parse("FC00::/7").unwrap().text(), "fc00::/7");
}

/// Cross-language contract: `contract/egress-deny-ranges.json` is the
/// canonical export of the built-in deny tables. Regenerate with
/// `UPDATE_GOLDEN=1 cargo test -p vrt-config`; the byte-exact compare
/// below is the drift gate, and `contract/check-egress-deny-ranges.mjs
/// --check` performs the same assertion against the published TS tables.
#[test]
fn deny_ranges_fixture_matches() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../contract/egress-deny-ranges.json"
    );
    let rows = |ranges: &[(&str, &str)]| -> Vec<serde_json::Value> {
        ranges
            .iter()
            .map(|(cidr, code)| serde_json::json!({ "cidr": cidr, "code": code }))
            .collect()
    };
    let doc = serde_json::json!({
        "schemaVersion": 1,
        "generatedBy": "vrt-config egress.rs deny-range tables (single source of truth)",
        "ipv4": rows(egress::IPV4_DENY_RANGES),
        "ipv6": rows(egress::IPV6_DENY_RANGES),
        "embeddedPrefixes": egress::EMBEDDED_IPV4_PREFIXES,
    });
    // serde_json::to_string_pretty matches JSON.stringify(doc, null, 2)
    // byte-for-byte for this plain-ASCII document (BTreeMap sorts keys:
    // embeddedPrefixes < generatedBy < ipv4 < ipv6 < schemaVersion;
    // each row object is cidr < code).
    let rendered = serde_json::to_string_pretty(&doc).unwrap() + "\n";
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(FIXTURE, &rendered).unwrap();
        return;
    }
    let golden = std::fs::read_to_string(FIXTURE)
        .expect("missing contract fixture; run UPDATE_GOLDEN=1 cargo test -p vrt-config");
    assert_eq!(golden, rendered, "contract fixture drifted");
}

/// Every fixture CIDR must be valid and canonical — the same strictness
/// netpolicy applies to operator-supplied CIDRs.
#[test]
fn deny_ranges_are_canonical_cidrs() {
    for (text, _) in egress::IPV4_DENY_RANGES
        .iter()
        .chain(egress::IPV6_DENY_RANGES.iter())
    {
        let cidr = Cidr::parse(text).unwrap_or_else(|_| panic!("bad builtin cidr {text}"));
        assert_eq!(cidr.text(), *text, "non-canonical builtin cidr {text}");
    }
    for text in egress::EMBEDDED_IPV4_PREFIXES {
        let cidr = Cidr::parse(text).unwrap_or_else(|_| panic!("bad embedded cidr {text}"));
        assert_eq!(cidr.text(), *text);
    }
}

#[test]
fn extra_excepts_reject_ipv4_mapped_ranges() {
    for text in [
        "::ffff:0.0.0.0/96",
        "::ffff:0:0/96",
        "::FFFF:0.0.0.0/96",
        "0:0:0:0:0:ffff:0:0/96",
        "0000:0000:0000:0000:0000:ffff:0000:0000/96",
        "::ffff:10.0.0.0/104",
        "::ffff:10.1.2.3/128",
        "::ffff:a00:0/104",
        "::/0",  // contains the mapped range
        "::/64", // contains the mapped range
        "::/80",
    ] {
        let cidr = Cidr::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
        let mut p = net_policy();
        p.extra_excepts.push(cidr.clone());
        let err = render_network_policy(&p).expect_err(text);
        match &err {
            ConfigError::Ipv4MappedExcept { field, cidr: c } => {
                assert_eq!(*field, "netpolicy.extraExcepts", "{text}");
                assert_eq!(c, &cidr.text(), "{text}");
            }
            other => panic!("{text}: unexpected {other:?}"),
        }
        assert!(err.to_string().contains("netpolicy.extraExcepts"), "{text}");
        assert_eq!(err.code(), "CONFIG_NETPOL_IPV4_MAPPED_EXCEPT");
    }
}

#[test]
fn mapped_ranges_rejected_in_every_except_list() {
    let m = || Cidr::parse("::ffff:0.0.0.0/96").unwrap();
    let cases: [(&str, fn(&mut RuntimeNetPolicy, Cidr)); 3] = [
        ("netpolicy.podCidrs", |p, c| p.pod_cidrs.push(c)),
        ("netpolicy.serviceCidrs", |p, c| p.service_cidrs.push(c)),
        ("netpolicy.nodeExcepts", |p, c| p.node_excepts.push(c)),
    ];
    for (field, add) in cases {
        let mut p = net_policy();
        add(&mut p, m());
        let err = render_network_policy(&p).unwrap_err();
        assert!(err.to_string().contains(field), "{err}");
    }
}

#[test]
fn extra_excepts_accept_plain_v6_v4_and_adjacent_ranges() {
    for text in [
        "fd00::/8",
        "fd00:20::/64",
        "10.0.0.0/8",
        "64:ff9b::/96",
        "::/96",
        "::fffe:0:0/96", // adjacent to, not inside, the mapped range
        "::1:0:0/96",
    ] {
        let mut p = net_policy();
        p.extra_excepts.push(Cidr::parse(text).unwrap());
        render_network_policy(&p).unwrap_or_else(|e| panic!("{text}: {e}"));
    }
}

#[test]
fn cidr_parse_rejects_malformed_or_non_canonical() {
    for text in [
        "not-a-cidr",
        "10.0.0.1/8",
        "fd00::1/8",
        "10.0.0.0/33",
        "::/129",
        "10.0.0.0",
    ] {
        assert!(Cidr::parse(text).is_err(), "{text}");
    }
}
