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
