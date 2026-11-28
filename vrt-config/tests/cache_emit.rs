//! C1: Cache API emit — `params.cache_addr` non-empty emits ONE shared
//! `vrt-cache` external service + `cacheApiOutbound` inside EVERY
//! tenant worker + the matching `ExternalAddr`. Empty ⇒ nothing —
//! `caches.*` then throws `No Cache was configured` (explicit).

mod common;

use common::*;
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

#[test]
fn cache_absent_emits_nothing() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("vrt-cache"), "{}", r.text);
    assert!(!r.text.contains("cacheApiOutbound"), "{}", r.text);
    assert!(
        r.external_addrs.iter().all(|a| a.service != "vrt-cache"),
        "{:?}",
        r.external_addrs
    );
}

#[test]
fn cache_emits_service_outbound_and_addr() {
    let t = tenant();
    let mut params = runtime_params();
    params.cache_addr = "unix:/run/vrt/cache/team-alpha.sock".into();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;

    // Exactly ONE external service definition (the count excludes
    // the `cacheApiOutbound = (name = "vrt-cache")` references).
    assert_eq!(
        out.matches("name = \"vrt-cache\", external = (").count(),
        1,
        "{out}"
    );
    // Every tenant worker binds it via cacheApiOutbound — window is
    // THIS worker's block (bounded by the next worker marker).
    let worker_positions: Vec<usize> = t
        .workers
        .iter()
        .map(|w| {
            out.find(&format!("name = \"worker-{}\", worker = (", w.worker_id))
                .expect("worker block")
        })
        .collect();
    for (i, worker) in t.workers.iter().enumerate() {
        let pos = worker_positions[i];
        let end = worker_positions
            .iter()
            .copied()
            .filter(|p| *p > pos)
            .min()
            .unwrap_or(out.len());
        let window = &out[pos..end];
        assert!(
            window.contains("cacheApiOutbound = (name = \"vrt-cache\")"),
            "worker {} missing cacheApiOutbound\n{window}",
            worker.worker_id
        );
    }
    // And the total count equals the worker count — none duplicated.
    assert_eq!(
        out.matches("cacheApiOutbound = (name = \"vrt-cache\")")
            .count(),
        t.workers.len(),
        "{out}"
    );
    // The socket path reaches the launcher verbatim.
    let addr = r
        .external_addrs
        .iter()
        .find(|a| a.service == "vrt-cache")
        .expect("vrt-cache external addr");
    assert_eq!(addr.address, "unix:/run/vrt/cache/team-alpha.sock");
}

// Workers Cache façade: `ctx.cache.purge()` is attached by the waitUntil
// wrapper only when the worker's version enables caching; the per-worker
// shim owns the token/scope and forwards to the host control listener.
#[test]
fn ctx_cache_facade_only_when_enabled() {
    use snapshot_protocol::compute_desired::CacheDesired;

    let mut t = tenant();
    let mut params = runtime_params();
    params.wf_delete_addr = "http://127.0.0.1:9091".into();

    // Cache off (default): no binding, no shim, no external.
    let r0 = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r0.text.contains("VRT_CACHE_CTL"), "{}", r0.text);
    assert!(!r0.text.contains("vrt-cachectl-"), "{}", r0.text);
    assert!(r0
        .external_addrs
        .iter()
        .all(|a| a.service != "vrt-cachedel"));

    t.workers[0].cache = Some(CacheDesired {
        enabled: true,
        ..CacheDesired::default()
    });
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    let out = &r.text;
    let worker = &t.workers[0].worker_id;
    assert!(out.contains("VRT_CACHE_CTL"), "{out}");
    assert!(out.contains(&format!("vrt-cachectl-{worker}")), "{out}");
    assert!(
        out.contains("name = \"vrt-cachedel\", external = ("),
        "{out}"
    );
    assert!(out.contains("VRT_CACHE_PURGE_TOKEN"), "{out}");
    assert!(out.contains("VRT_CACHE_SCOPE"), "{out}");
    // The injected wrapper carries the façade implementation.
    assert!(out.contains("__makeCache"), "{out}");
    assert!(
        r.external_addrs.iter().any(|a| a.service == "vrt-cachedel"),
        "{:?}",
        r.external_addrs
    );
    // Only the cache-enabled worker gets the tenant binding.
    assert_eq!(out.matches("(name = \"VRT_CACHE_CTL\"").count(), 1, "{out}");
}
