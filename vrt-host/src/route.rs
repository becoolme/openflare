//! `Host -> (tenantId, workerId)` route table, built exclusively from a
//! verified `compute.desired` snapshot (`worker.routes[].hostname` is
//! globally unique per the snapshot validator). Exact-match only — no
//! fuzzy matching, no wildcard, no cross-tenant fallback.

use snapshot_protocol::compute_desired::ComputeDesired;
use std::collections::HashMap;
use std::sync::Arc;

/// Where a hostname routes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteTarget {
    /// Tenant owning the worker.
    pub tenant_id: String,
    /// Worker serving this hostname.
    pub worker_id: String,
    /// T31c: the worker's socket fronts a `router-<id>` system worker —
    /// only then is the `x-vrt-asset-route` response marker honored for
    /// metrics (a legacy worker cannot self-label as an asset hit).
    pub assets_first: bool,
}

/// Immutable host→target map. Swapped atomically (`ArcSwap`) on each
/// converge — never mutated in place.
#[derive(Clone, Debug, Default)]
pub struct RouteTable {
    map: Arc<HashMap<String, RouteTarget>>,
    /// Dev-only catch-all target — populated when the desired doc
    /// contains exactly ONE worker (the `vrt dev` shape). The ingress
    /// consults it only when its `devCatchAll` switch is on, so
    /// production routing is byte-identical to before.
    catch_all: Option<RouteTarget>,
}

/// Host header bounds: 253 chars total (DNS hostname limit).
pub const MAX_HOST_LEN: usize = 253;

/// Normalize a `Host` header value into a lookup key:
/// lowercase, strip any port suffix or IPv6 brackets, reject empty,
/// over-length, or non-DNS characters (`[a-z0-9.-]`; underscores, spaces,
/// `@`, `/` … are not hostnames). Returns `None` on any malformed input —
/// fail closed.
pub fn normalize_host(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.len() > MAX_HOST_LEN + 64 {
        return None;
    }
    // Strip `:<port>` — a single trailing colon+digits suffix. Bracketed
    // IPv6 literals are not valid route hostnames here; reject '['.
    if raw.contains('[') || raw.contains(']') {
        return None;
    }
    let host = match raw.rsplit_once(':') {
        Some((h, p)) if !h.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => h,
        _ => raw,
    };
    if host.is_empty() || host.len() > MAX_HOST_LEN {
        return None;
    }
    if !host
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
    {
        // Lowercase first, then re-check.
        let lower = host.to_ascii_lowercase();
        if !lower
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        {
            return None;
        }
        return finish_host(lower);
    }
    finish_host(host.to_string())
}

fn finish_host(host: String) -> Option<String> {
    // Labels must be non-empty (rejects `..`, leading/trailing dots).
    if host.split('.').any(|label| label.is_empty()) {
        return None;
    }
    Some(host)
}

impl RouteTable {
    /// Build from a verified snapshot.
    pub fn from_desired(desired: &ComputeDesired) -> Self {
        let mut map = HashMap::new();
        // Single-worker desired docs get a catch-all candidate; the
        // ingress decides (via devCatchAll) whether to honor it.
        let mut only_target: Option<RouteTarget> = None;
        let mut worker_count = 0usize;
        for tenant in &desired.tenants {
            for worker in &tenant.workers {
                worker_count += 1;
                let assets_first = worker.bindings.iter().any(|b| match b {
                    snapshot_protocol::compute_desired::BindingDesired::Assets(a) => {
                        a.assets_first_enabled()
                    }
                    _ => false,
                });
                only_target = Some(RouteTarget {
                    tenant_id: tenant.tenant_id.clone(),
                    worker_id: worker.worker_id.clone(),
                    assets_first,
                });
                for route in &worker.routes {
                    // Snapshot validation guarantees global uniqueness;
                    // a duplicate still fails closed (first wins, never
                    // overwrites — the snapshot itself is trusted).
                    map.entry(route.hostname.to_ascii_lowercase())
                        .or_insert(RouteTarget {
                            tenant_id: tenant.tenant_id.clone(),
                            worker_id: worker.worker_id.clone(),
                            assets_first,
                        });
                }
            }
        }
        Self {
            map: Arc::new(map),
            catch_all: (worker_count == 1).then_some(only_target).flatten(),
        }
    }

    /// Exact-match lookup on a normalized host.
    pub fn lookup(&self, normalized_host: &str) -> Option<&RouteTarget> {
        self.map.get(normalized_host)
    }

    /// Dev-mode fallback: the single worker this desired doc contains,
    /// if and only if there is exactly one — never a guess between
    /// multiple tenants/workers.
    pub fn catch_all(&self) -> Option<&RouteTarget> {
        self.catch_all.as_ref()
    }

    /// Route count (diagnostics).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Empty check.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_case_port_and_form() {
        assert_eq!(
            normalize_host("App.Example.COM"),
            Some("app.example.com".into())
        );
        assert_eq!(
            normalize_host("app.example.com:443"),
            Some("app.example.com".into())
        );
        assert_eq!(
            normalize_host("app.example.com:0"),
            Some("app.example.com".into())
        );
        assert_eq!(normalize_host("a.b.c"), Some("a.b.c".into()));
        // Invalid charset / shape → reject.
        for bad in [
            "",
            " ",
            "app..example.com",
            ".app.com",
            "app.com.",
            "app_example.com",
            "app example.com",
            "app/example.com",
            "user@app.com",
            "[::1]",
            "[::1]:80",
            "app.com:abc",
            ":80",
            &"a".repeat(300),
        ] {
            assert_eq!(normalize_host(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn table_is_exact_match_only() {
        let desired = ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 1,
            tenants: vec![snapshot_protocol::compute_desired::TenantDesired {
                tenant_id: "t1".into(),
                workers: vec![snapshot_protocol::compute_desired::WorkerDesired {
                    worker_id: "w1".into(),
                    name: "w1".into(),
                    active_version: snapshot_protocol::compute_desired::VersionDesired {
                        version_id: "v1".into(),
                        artifact_digest: "d".repeat(64),
                        compatibility_date: "2024-01-01".into(),
                        compatibility_flags: vec![],
                    },
                    bindings: vec![],
                    routes: vec![snapshot_protocol::compute_desired::RouteDesired {
                        hostname: "app.example.com".into(),
                    }],
                    crons: vec![],
                    queue_consumers: vec![],
                                        observability: None,
                    durable_object_classes: vec![],
                    workflows: vec![],
                    tail_consumers: vec![],
                    streaming_tail_consumers: vec![],
                    quotas: snapshot_protocol::compute_desired::WorkerQuotas {
                        cpu_ms: 50,
                        memory_mb: 128,
                        subrequests: 0,
                        outbound_connections: 0,
                    },
                }],
                flagship_apps: vec![],
            }],
            deletions: vec![],
};
        let table = RouteTable::from_desired(&desired);
        assert_eq!(table.lookup("app.example.com").unwrap().tenant_id, "t1");
        assert!(table.lookup("x.app.example.com").is_none());
        assert!(table.lookup("app.example.com.evil.com").is_none());
        assert!(table.lookup("").is_none());
    }
}
