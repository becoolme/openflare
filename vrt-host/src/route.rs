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
    /// IMG1: the worker declares an `images` binding — the ingress owns
    /// `/cdn-cgi/image/` for its hostnames (transforms run through the
    /// per-worker `facade-images` socket). Without the binding the path
    /// falls through to tenant code untouched.
    pub images: bool,
    /// PREVIEW: the version this route serves AS A PREVIEW (either a
    /// preview instance or the active version reached through a preview
    /// hostname). `None` for production hostnames. The ingress stamps
    /// `x-vrt-preview-version` on every such response so a preview can
    /// never be mistaken for production traffic, and requires a signed
    /// token bound to this `(version_id, worker_id)` pair.
    pub preview: Option<PreviewBinding>,
    /// Version currently serving this route (cache key input; previews
    /// carry the preview version).
    pub version_id: String,
    /// Workers Cache config (absent = cache off). Resolved from the
    /// signed snapshot per worker; never from a client header.
    pub cache: Option<snapshot_protocol::compute_desired::CacheDesired>,
}

/// What a preview route is authorized to serve. `worker_id` is the
/// PRODUCTION worker (the token audience), not the preview instance id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreviewBinding {
    pub version_id: String,
    pub worker_id: String,
}

/// Immutable host→target map. Swapped atomically (`ArcSwap`) on each
/// converge — never mutated in place.
#[derive(Clone, Debug, Default)]
pub struct RouteTable {
    map: Arc<HashMap<String, RouteTarget>>,
    /// Dev-only catch-all target — populated when the desired doc
    /// contains exactly ONE worker (the `of dev` shape). The ingress
    /// consults it only when its `devCatchAll` switch is on, so
    /// production routing is byte-identical to before.
    catch_all: Option<RouteTarget>,
}

/// Host header bounds: 253 chars total (DNS hostname limit).
pub const MAX_HOST_LEN: usize = 253;

/// Normalize a `Host` header value into a lookup key:
/// lowercase, strip any port suffix or IPv6 brackets, reject empty,
/// over-length, or non-DNS characters (`[a-z0-9.-]`; spaces, `@`, `/` …
/// are not hostnames). Returns `None` on any malformed input —
/// fail closed.
///
/// `_` is accepted here for exactly one reason: preview hostnames are
/// `<versionId>--<worker>.<previewDomain>` and version ids carry a
/// `vrt_` prefix. Production hostnames are validated by
/// `compute_desired::valid_hostname` (which never allows `_`), so
/// accepting `_` can only ever match a preview route — it cannot widen
/// or shadow production routing.
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
    let allowed =
        |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'_';
    if !host.bytes().all(allowed) {
        // Lowercase first, then re-check.
        let lower = host.to_ascii_lowercase();
        if !lower.bytes().all(allowed) {
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
    /// Build from a verified snapshot, production hostnames only.
    ///
    /// Preview hostnames stay unroutable here: the preview domain is host
    /// configuration, so a caller that does not know it must not invent
    /// one. `vrt-host` uses [`Self::from_desired_with_preview_domain`].
    pub fn from_desired(desired: &ComputeDesired) -> Self {
        Self::from_desired_with_preview_domain(desired, None)
    }

    /// Build from a verified snapshot plus the host's `[preview] domain`.
    ///
    /// Production routing is byte-identical whether or not a preview
    /// domain is configured; with `preview_domain = None` no preview
    /// hostname resolves at all (every `<…>.preview` request is a 404 —
    /// the feature is off by default).
    ///
    /// When configured, each preview instance and each worker's ACTIVE
    /// version earns one derived hostname:
    /// `<versionId>--<productionWorkerName>.<previewDomain>`. The host is
    /// never taken from the snapshot, so a signed snapshot can neither
    /// invent a preview domain nor shadow a production hostname: derived
    /// entries never overwrite an existing route, and production
    /// hostnames cannot contain the `_` that every version id carries.
    pub fn from_desired_with_preview_domain(
        desired: &ComputeDesired,
        preview_domain: Option<&str>,
    ) -> Self {
        let mut map = HashMap::new();
        // Single-worker desired docs get a catch-all candidate; the
        // ingress decides (via devCatchAll) whether to honor it.
        let mut only_target: Option<RouteTarget> = None;
        let mut worker_count = 0usize;
        for tenant in &desired.tenants {
            for worker in &tenant.workers {
                worker_count += 1;
                let target = Self::target_for(tenant, worker);
                only_target = Some(target.clone());
                for route in &worker.routes {
                    // Snapshot validation guarantees global uniqueness;
                    // a duplicate still fails closed (first wins, never
                    // overwrites — the snapshot itself is trusted).
                    map.entry(route.hostname.to_ascii_lowercase())
                        .or_insert_with(|| target.clone());
                }
            }
        }
        if let Some(domain) = preview_domain.filter(|d| !d.is_empty()) {
            for tenant in &desired.tenants {
                for worker in &tenant.workers {
                    // A preview instance is named after the PRODUCTION
                    // worker it previews (`preview.workerId`); the
                    // active version is named after its own worker. The
                    // token audience is always the production worker id.
                    let (version_id, owner_id, name) = match &worker.preview {
                        Some(preview) => (
                            preview.version_id.clone(),
                            preview.worker_id.clone(),
                            tenant
                                .workers
                                .iter()
                                .find(|w| w.worker_id == preview.worker_id)
                                .map(|w| w.name.as_str())
                                .unwrap_or(worker.name.as_str()),
                        ),
                        None => (
                            worker.active_version.version_id.clone(),
                            worker.worker_id.clone(),
                            worker.name.as_str(),
                        ),
                    };
                    let host = format!("{}--{}.{}", version_id, name, domain).to_ascii_lowercase();
                    if map.contains_key(&host) {
                        // Never overwrite a production hostname (or an
                        // earlier preview) with a derived one.
                        continue;
                    }
                    let mut target = Self::target_for(tenant, worker);
                    target.preview = Some(PreviewBinding {
                        version_id,
                        worker_id: owner_id,
                    });
                    map.insert(host, target);
                }
            }
        }
        Self {
            map: Arc::new(map),
            catch_all: (worker_count == 1).then_some(only_target).flatten(),
        }
    }

    /// Route target for one desired worker entry.
    fn target_for(
        tenant: &snapshot_protocol::compute_desired::TenantDesired,
        worker: &snapshot_protocol::compute_desired::WorkerDesired,
    ) -> RouteTarget {
        let assets_first = worker.bindings.iter().any(|b| match b {
            snapshot_protocol::compute_desired::BindingDesired::Assets(a) => {
                a.assets_first_enabled()
            }
            _ => false,
        });
        let images = worker.bindings.iter().any(|b| {
            matches!(
                b,
                snapshot_protocol::compute_desired::BindingDesired::Images(_)
            )
        });
        RouteTarget {
            tenant_id: tenant.tenant_id.clone(),
            worker_id: worker.worker_id.clone(),
            assets_first,
            images,
            preview: None,
            version_id: worker.active_version.version_id.clone(),
            cache: worker.cache,
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
        // PREVIEW: version ids carry a `vrt_` prefix, so a preview Host
        // needs `_`. Production hostnames can never contain it (the
        // snapshot validator refuses), so this cannot widen routing.
        assert_eq!(
            normalize_host("vrt_abc123--edge-api.preview.example.com:443"),
            Some("vrt_abc123--edge-api.preview.example.com".into())
        );
        assert_eq!(
            normalize_host("VRT_ABC123--Edge-API.Preview.Example.COM"),
            Some("vrt_abc123--edge-api.preview.example.com".into())
        );
        // Invalid charset / shape → reject.
        for bad in [
            "",
            " ",
            "app..example.com",
            ".app.com",
            "app.com.",
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

    fn preview_desired() -> ComputeDesired {
        use snapshot_protocol::compute_desired::{
            BindingDesired, PreviewDesired, RouteDesired, TenantDesired, VersionDesired,
            WorkerDesired, WorkerQuotas,
        };
        let version = |id: &str| VersionDesired {
            version_id: id.into(),
            artifact_digest: "d".repeat(64),
            compatibility_date: "2024-01-01".into(),
            compatibility_flags: vec![],
            tag: String::new(),
            created_at: String::new(),
        };
        let quotas = WorkerQuotas {
            cpu_ms: 50,
            memory_mb: 128,
            subrequests: 0,
            outbound_connections: 0,
        };
        ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 3,
            tenants: vec![TenantDesired {
                tenant_id: "t1".into(),
                workers: vec![
                    WorkerDesired {
                        cache: None,
                        worker_id: "w1".into(),
                        name: "edge-api".into(),
                        active_version: version("vrt_active"),
                        bindings: vec![],
                        routes: vec![RouteDesired {
                            hostname: "api.example.com".into(),
                        }],
                        crons: vec![],
                        queue_consumers: vec![],
                        durable_object_classes: vec![],
                        workflows: vec![],
                        tail_consumers: vec![],
                        streaming_tail_consumers: vec![],
                        observability: None,
                        quotas: quotas.clone(),
                        preview: None,
                    },
                    WorkerDesired {
                        cache: None,
                        worker_id: "w1.pv01234567".into(),
                        name: "edge-api-pv01234567".into(),
                        active_version: version("vrt_preview"),
                        bindings: vec![],
                        routes: vec![],
                        crons: vec![],
                        queue_consumers: vec![],
                        durable_object_classes: vec![],
                        workflows: vec![],
                        tail_consumers: vec![],
                        streaming_tail_consumers: vec![],
                        observability: None,
                        quotas: quotas.clone(),
                        preview: Some(PreviewDesired {
                            worker_id: "w1".into(),
                            version_id: "vrt_preview".into(),
                        }),
                    },
                    WorkerDesired {
                        cache: None,
                        worker_id: "w2".into(),
                        name: "assets".into(),
                        active_version: version("vrt_active2"),
                        bindings: vec![BindingDesired::Images(
                            snapshot_protocol::compute_desired::ImagesBinding {
                                name: "IMAGES".into(),
                            },
                        )],
                        routes: vec![RouteDesired {
                            hostname: "img.example.com".into(),
                        }],
                        crons: vec![],
                        queue_consumers: vec![],
                        durable_object_classes: vec![],
                        workflows: vec![],
                        tail_consumers: vec![],
                        streaming_tail_consumers: vec![],
                        observability: None,
                        quotas,
                        preview: None,
                    },
                ],
                flagship_apps: vec![],
            }],
            deletions: vec![],
        }
    }

    #[test]
    fn preview_routes_are_off_without_a_configured_domain() {
        let desired = preview_desired();
        let table = RouteTable::from_desired(&desired);
        // Production routing is unchanged.
        assert_eq!(table.lookup("api.example.com").unwrap().worker_id, "w1");
        assert!(table.lookup("api.example.com").unwrap().preview.is_none());
        // No preview domain → the preview hostname is unknown (404).
        assert!(table
            .lookup("vrt_preview--edge-api.preview.example.com")
            .is_none());
        assert!(table
            .lookup("vrt_active--edge-api.preview.example.com")
            .is_none());
    }

    #[test]
    fn preview_routes_select_the_version_without_touching_production() {
        let desired = preview_desired();
        let table =
            RouteTable::from_desired_with_preview_domain(&desired, Some("preview.example.com"));
        // Preview instance: routed to ITS OWN instance, labelled with the
        // version, while production still points at the active version.
        let pv = table
            .lookup("vrt_preview--edge-api.preview.example.com")
            .expect("preview route");
        assert_eq!(pv.worker_id, "w1.pv01234567");
        assert_eq!(
            pv.preview.as_ref().map(|p| p.version_id.as_str()),
            Some("vrt_preview")
        );
        assert_eq!(
            pv.preview.as_ref().map(|p| p.worker_id.as_str()),
            Some("w1"),
            "the token audience is the production worker id"
        );
        let prod = table.lookup("api.example.com").expect("prod route");
        assert_eq!(prod.worker_id, "w1");
        assert!(prod.preview.is_none());
        // The ACTIVE version is reachable through its preview hostname and
        // is served by the production instance.
        let active = table
            .lookup("vrt_active--edge-api.preview.example.com")
            .expect("active preview route");
        assert_eq!(active.worker_id, "w1");
        assert_eq!(
            active.preview.as_ref().map(|p| p.version_id.as_str()),
            Some("vrt_active")
        );
        // A version that is neither preview nor active has no route.
        assert!(table
            .lookup("vrt_archived--edge-api.preview.example.com")
            .is_none());
        // Preview hosts are case-insensitive and never the production host.
        assert!(table
            .lookup("vrt_preview--edge-api.preview.example.com")
            .is_some());
        assert_ne!(
            pv.worker_id,
            table.lookup("api.example.com").unwrap().worker_id
        );
        // Other workers keep their own preview plumbing.
        assert_eq!(
            table
                .lookup("vrt_active2--assets.preview.example.com")
                .unwrap()
                .images,
            true
        );
    }

    #[test]
    fn derived_preview_route_never_overwrites_a_production_hostname() {
        use snapshot_protocol::compute_desired::{
            PreviewDesired, RouteDesired, TenantDesired, VersionDesired, WorkerDesired,
            WorkerQuotas,
        };
        let version = |id: &str| VersionDesired {
            version_id: id.into(),
            artifact_digest: "d".repeat(64),
            compatibility_date: "2024-01-01".into(),
            compatibility_flags: vec![],
            tag: String::new(),
            created_at: String::new(),
        };
        // A worker whose production hostname is EXACTLY what the derived
        // preview host would be. First-wins keeps production semantics.
        let desired = ComputeDesired {
            schema_version: 1,
            cell_id: "cell".into(),
            revision: 1,
            tenants: vec![TenantDesired {
                tenant_id: "t1".into(),
                workers: vec![
                    WorkerDesired {
                        cache: None,
                        worker_id: "w1".into(),
                        name: "edge-api".into(),
                        active_version: version("vrt_preview"),
                        bindings: vec![],
                        routes: vec![RouteDesired {
                            hostname: "vrt_preview--edge-api.preview.example.com".into(),
                        }],
                        crons: vec![],
                        queue_consumers: vec![],
                        durable_object_classes: vec![],
                        workflows: vec![],
                        tail_consumers: vec![],
                        streaming_tail_consumers: vec![],
                        observability: None,
                        quotas: WorkerQuotas::platform_default(),
                        preview: None,
                    },
                    WorkerDesired {
                        cache: None,
                        worker_id: "w1.pv01234567".into(),
                        name: "edge-api-pv01234567".into(),
                        active_version: version("vrt_preview2"),
                        bindings: vec![],
                        routes: vec![],
                        crons: vec![],
                        queue_consumers: vec![],
                        durable_object_classes: vec![],
                        workflows: vec![],
                        tail_consumers: vec![],
                        streaming_tail_consumers: vec![],
                        observability: None,
                        quotas: WorkerQuotas::platform_default(),
                        preview: Some(PreviewDesired {
                            worker_id: "w1".into(),
                            version_id: "vrt_preview".into(),
                        }),
                    },
                ],
                flagship_apps: vec![],
            }],
            deletions: vec![],
        };
        let table =
            RouteTable::from_desired_with_preview_domain(&desired, Some("preview.example.com"));
        // The production route wins and is NOT relabelled as a preview.
        let prod = table
            .lookup("vrt_preview--edge-api.preview.example.com")
            .unwrap();
        assert_eq!(prod.worker_id, "w1");
        assert!(prod.preview.is_none());
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
                    cache: None,
                    worker_id: "w1".into(),
                    name: "w1".into(),
                    active_version: snapshot_protocol::compute_desired::VersionDesired {
                        version_id: "v1".into(),
                        artifact_digest: "d".repeat(64),
                        compatibility_date: "2024-01-01".into(),
                        compatibility_flags: vec![],
                        tag: String::new(),
                        created_at: String::new(),
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
                    preview: None,
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
