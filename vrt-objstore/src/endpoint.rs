//! RV-R72: shared endpoint-URL policy for S3-compatible object stores
//! and the in-cluster vrt-sql address.
//!
//! The URL is *parsed* (never prefix-matched): `http://localhost.evil.com`
//! or `http://127.0.0.1@evil.com` must not pass as loopback.

use reqwest::Url;
use std::net::IpAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EndpointError {
    #[error("endpoint is not a valid absolute URL")]
    Invalid,
    #[error("endpoint scheme must be https (http only for loopback, a <svc>.<ns>.svc:port Service, or allowDevBackend)")]
    InsecureScheme,
    #[error("endpoint must not carry credentials, query or fragment")]
    Forbidden,
    #[error("endpoint must have a host")]
    NoHost,
    #[error("endpoint must not carry a path")]
    HasPath,
}

fn host_is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

fn ip_is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => v.is_loopback() || v.is_private() || v.is_link_local(),
        IpAddr::V6(v) => {
            v.is_loopback()
                // fc00::/7 unique-local, fe80::/10 link-local
                || (v.segments()[0] & 0xfe00) == 0xfc00
                || (v.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

/// Cluster-internal host: single-label service name, `*.svc`,
/// `*.svc.cluster.local`, `*.cluster.local`, or a private/link-local IP.
fn host_is_cluster_internal(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(d)) => {
            let d = d.to_ascii_lowercase();
            !d.contains('.') || d.ends_with(".svc") || d.ends_with(".cluster.local")
        }
        Some(url::Host::Ipv4(ip)) => ip_is_private(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => ip_is_private(IpAddr::V6(ip)),
        None => false,
    }
}

fn parse_common(raw: &str) -> Result<Url, EndpointError> {
    let url = Url::parse(raw).map_err(|_| EndpointError::Invalid)?;
    if url.host().is_none() {
        return Err(EndpointError::NoHost);
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(EndpointError::Forbidden);
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err(EndpointError::HasPath);
    }
    Ok(url)
}

/// Kubernetes Service FQDN with an explicit port: `<svc>.<ns>.svc[:port]`
/// or `<svc>.<ns>.svc.cluster.local[:port]` — exactly two labels before
/// the suffix, each a DNS-1123 label. Narrower than
/// `host_is_cluster_internal`: no single-label names (search-domain
/// expansion), no bare `*.cluster.local`, no IP literals — an in-cluster
/// object store is addressed by its Service, and the egress NetworkPolicy
/// pins the namespace/port it may reach.
fn host_is_k8s_service_fqdn(url: &Url) -> bool {
    if url.port().is_none() {
        return false;
    }
    let Some(url::Host::Domain(d)) = url.host() else {
        return false;
    };
    let d = d.to_ascii_lowercase();
    let stem = if let Some(s) = d.strip_suffix(".svc.cluster.local") {
        s
    } else if let Some(s) = d.strip_suffix(".svc") {
        s
    } else {
        return false;
    };
    let labels: Vec<&str> = stem.split('.').collect();
    labels.len() == 2
        && labels.iter().all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && l.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !l.starts_with('-')
                && !l.ends_with('-')
        })
}

/// External object-store endpoint: `https://` always; `http://` only for
/// a loopback host or when `allow_dev` is set.
pub fn validate_object_store_endpoint(raw: &str, allow_dev: bool) -> Result<(), EndpointError> {
    let url = parse_common(raw)?;
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_dev || host_is_loopback(&url) || host_is_k8s_service_fqdn(&url) => Ok(()),
        _ => Err(EndpointError::InsecureScheme),
    }
}

/// In-cluster service address (vrt-sql): plain `http://` is the shipped
/// design for pod-to-service traffic, but only toward loopback or a
/// cluster-internal host; a public host needs `https://` (or the dev
/// switch).
pub fn validate_internal_service_addr(raw: &str, allow_dev: bool) -> Result<(), EndpointError> {
    let url = parse_common(raw)?;
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_dev || host_is_loopback(&url) || host_is_cluster_internal(&url) => Ok(()),
        _ => Err(EndpointError::InsecureScheme),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_requires_https_except_loopback_or_dev() {
        assert!(validate_object_store_endpoint("https://s3.example.com", false).is_ok());
        assert!(validate_object_store_endpoint("https://s3.example.com:9000", false).is_ok());
        for ok in ["http://127.0.0.1:9000", "http://localhost:9000", "http://[::1]:9000", "http://127.9.9.9"] {
            assert!(validate_object_store_endpoint(ok, false).is_ok(), "{ok}");
        }
        for bad in [
            "http://s3.example.com",
            "http://203.0.113.5:9000",
            "http://localhost.evil.com",
            "http://127.0.0.1.evil.com",
            "http://127.0.0.1@evil.com",
            "ftp://s3.example.com",
        ] {
            assert!(validate_object_store_endpoint(bad, false).is_err(), "{bad}");
        }
        assert!(validate_object_store_endpoint("http://s3.example.com", true).is_ok());
    }

    #[test]
    fn object_store_allows_http_only_to_k8s_service_fqdn_with_port() {
        for ok in [
            "http://vrt-r2-silo.vibeploy-data.svc:9000",
            "http://vrt-r2-silo.vibeploy-data.svc.cluster.local:9000",
            "http://S3.NS.SVC:9000",
        ] {
            assert!(validate_object_store_endpoint(ok, false).is_ok(), "{ok}");
        }
        for bad in [
            // no explicit port
            "http://vrt-r2-silo.vibeploy-data.svc",
            // single label / search-domain expansion
            "http://silo:9000",
            // wrong label count
            "http://silo.svc:9000",
            "http://a.b.c.svc:9000",
            "http://a.b.c.svc.cluster.local:9000",
            // bare cluster.local, private IP literal
            "http://silo.cluster.local:9000",
            "http://10.43.0.10:9000",
            // suffix spoofing / bad labels
            "http://silo.ns.svc.evil.com:9000",
            "http://silo.ns.svcx:9000",
            "http://-silo.ns.svc:9000",
            "http://si_lo.ns.svc:9000",
            "http://silo.ns.svc@evil.com:9000",
            "http://silo.ns.svc:9000/path",
        ] {
            assert!(validate_object_store_endpoint(bad, false).is_err(), "{bad}");
        }
    }

    #[test]
    fn object_store_rejects_creds_path_query_and_garbage() {
        for bad in ["", "s3.example.com", "https://", "https://u:p@s3.example.com", "https://s3.example.com/x", "https://s3.example.com/?a=1", "https://s3.example.com/#f"] {
            assert!(validate_object_store_endpoint(bad, false).is_err(), "{bad}");
        }
    }

    #[test]
    fn internal_addr_allows_cluster_http_only() {
        for ok in ["http://vrt-sql:8082", "http://vrt-sql.ns.svc:8082", "http://vrt-sql.ns.svc.cluster.local:8082", "http://10.0.0.5:8082", "http://127.0.0.1:8082", "https://sql.example.com"] {
            assert!(validate_internal_service_addr(ok, false).is_ok(), "{ok}");
        }
        for bad in ["http://sql.example.com:8082", "http://203.0.113.5:8082", "http://vrt-sql@evil.com", "http://vrt-sql:8082/x"] {
            assert!(validate_internal_service_addr(bad, false).is_err(), "{bad}");
        }
        assert!(validate_internal_service_addr("http://sql.example.com:8082", true).is_ok());
    }
}
