//! Tail Workers (`tail_consumers`): user-declared consumers render as
//! workerd `tails` designators alongside the platform log collector.
//! The internal tail is never replaced, user order is stable, and a
//! dangling/self target fails closed before any config is written.

mod common;

use common::*;
use snapshot_protocol::compute_desired::{TailConsumerDesired, TenantDesired};
use vrt_config::*;

fn enable_logs() -> RuntimeParams {
    let mut p = runtime_params();
    p.log_tail = LogTailParams {
        sink_addr: "127.0.0.1:9494".into(),
        pepper: "pepper-tails".into(),
    };
    p
}

/// A third worker with its own bundle so the multi-tail case has two
/// distinct same-tenant targets.
fn bundle_third() -> VersionBundle {
    VersionBundle::build(
        DeployManifest::from_pack(&serde_json::json!({
            "mainModule": "third.js",
            "modules": {
                "third.js": "export default { fetch() { return new Response('third') } }"
            },
            "compatibilityDate": "2026-10-20",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap()
}

fn with_third_worker(mut tenant: TenantDesired) -> (TenantDesired, VersionBundle) {
    let bundle = bundle_third();
    let mut third = tenant.workers[1].clone();
    third.worker_id = "w-alpha-3".into();
    third.name = "audit".into();
    third.active_version.version_id = "v-2026-11-08-3".into();
    third.active_version.artifact_digest = bundle.digest().to_string();
    third.active_version.compatibility_date = "2026-10-20".into();
    third.routes = vec![serde_json::from_value(serde_json::json!({
        "hostname": "audit--team-alpha.vibeploy.app"
    }))
    .unwrap()];
    third.bindings.clear();
    third.crons.clear();
    tenant.workers.push(third);
    (tenant, bundle)
}

fn consumers(ids: &[&str]) -> Vec<TailConsumerDesired> {
    ids.iter()
        .map(|id| TailConsumerDesired {
            worker_id: (*id).to_string(),
        })
        .collect()
}

#[test]
fn user_tail_renders_when_platform_logs_are_disabled() {
    let mut tenant = tenant();
    tenant.workers[0].tail_consumers = consumers(&["w-alpha-2"]);
    let params = runtime_params();
    let r = render_tenant_config(&input(&tenant, &[bundle_main(), bundle_aux()], &params)).unwrap();
    let out = &r.text;
    assert!(
        out.contains("      tails = [(name = \"worker-w-alpha-2\")],\n"),
        "{out}"
    );
    // No internal collector exists in this render.
    assert!(!out.contains("vrt-tail-"), "{out}");
    assert!(!out.contains("vrt-logsink"), "{out}");
    // The consumer itself declares no tail consumer.
    let start = out
        .find("(name = \"worker-w-alpha-2\", worker = (")
        .unwrap();
    let rest = &out[start..];
    let end = rest.find("\n    )),").unwrap();
    assert!(!rest[..end].contains("tails ="), "{}", &rest[..end]);
}

#[test]
fn user_tail_coexists_with_internal_log_tail_and_keeps_order() {
    let (mut tenant, third) = with_third_worker(tenant());
    tenant.workers[0].tail_consumers = consumers(&["w-alpha-3", "w-alpha-2"]);
    let params = enable_logs();
    let bundles = [bundle_main(), bundle_aux(), third];
    let r = render_tenant_config(&input(&tenant, &bundles, &params)).unwrap();
    let out = &r.text;
    // Internal collector FIRST (stable), then declaration order.
    assert!(
        out.contains(
            "      tails = [(name = \"vrt-tail-w-alpha-1\"), \
             (name = \"worker-w-alpha-3\"), (name = \"worker-w-alpha-2\")],\n"
        ),
        "{out}"
    );
    // The other workers keep only the platform collector.
    assert!(
        out.contains("      tails = [(name = \"vrt-tail-w-alpha-2\")],\n"),
        "{out}"
    );
    assert!(
        out.contains("      tails = [(name = \"vrt-tail-w-alpha-3\")],\n"),
        "{out}"
    );
    // One internal tail service per worker; user consumers are the
    // normal worker services (no duplicate service definitions).
    for id in ["w-alpha-1", "w-alpha-2", "w-alpha-3"] {
        assert_eq!(
            out.matches(&format!("(name = \"vrt-tail-{id}\", worker = ("))
                .count(),
            1,
            "internal tail service for {id}\n{out}"
        );
        assert_eq!(
            out.matches(&format!("(name = \"worker-{id}\", worker = ("))
                .count(),
            1,
            "worker service for {id}\n{out}"
        );
    }
    // The log sink external is still emitted exactly once.
    assert_eq!(
        out.matches("name = \"vrt-logsink\", external = (").count(),
        1,
        "{out}"
    );
}

#[test]
fn dangling_target_fails_closed() {
    let mut tenant = tenant();
    tenant.workers[0].tail_consumers = consumers(&["ghost"]);
    let r = render_tenant_config(&input(
        &tenant,
        &[bundle_main(), bundle_aux()],
        &runtime_params(),
    ));
    assert!(r.is_err(), "unknown consumer must not render");
}

#[test]
fn self_and_cross_tenant_targets_fail_closed() {
    let mut self_tenant = tenant();
    self_tenant.workers[0].tail_consumers = consumers(&["w-alpha-1"]);
    assert!(render_tenant_config(&input(
        &self_tenant,
        &[bundle_main(), bundle_aux()],
        &runtime_params()
    ))
    .is_err());

    // `w-alpha-2` exists in the tenant but a consumer naming the same
    // worker twice is not a stable list.
    let mut dup_tenant = tenant();
    dup_tenant.workers[0].tail_consumers = consumers(&["w-alpha-2", "w-alpha-2"]);
    assert!(render_tenant_config(&input(
        &dup_tenant,
        &[bundle_main(), bundle_aux()],
        &runtime_params()
    ))
    .is_err());
}
