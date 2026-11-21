//! Byte-for-byte golden comparison of the rendered `config.capnp`.
//!
//! Regenerate with `UPDATE_GOLDEN=1 cargo test -p vrt-config`, then review
//! the diff — the golden file is the reviewed contract of the emitted
//! workerd configuration.

mod common;

use common::*;
use snapshot_protocol::compute_desired::{TailConsumerDesired, TenantDesired};
use vrt_config::render_tenant_config;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/tenant.capnp");
const GOLDEN_TAILS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/tenant-tails.capnp"
);
const GOLDEN_TAILS_DIGEST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/tenant-tails.digest"
);

#[test]
fn golden_capnp_matches_byte_for_byte() {
    let tenant = tenant();
    let bundles = vec![bundle_main(), bundle_aux()];
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&tenant, &bundles, &params)).unwrap();

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN, &rendered.text).unwrap();
    }
    let expected = std::fs::read_to_string(GOLDEN)
        .expect("missing golden file; run UPDATE_GOLDEN=1 cargo test -p vrt-config");
    assert_eq!(rendered.text, expected, "golden mismatch");
}

#[test]
fn golden_digest_is_stable() {
    let tenant = tenant();
    let bundles = vec![bundle_main(), bundle_aux()];
    let params = runtime_params();
    let rendered = render_tenant_config(&input(&tenant, &bundles, &params)).unwrap();
    assert_eq!(
        rendered.digest,
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/golden/tenant.digest"
        ))
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|_| {
            std::fs::write(
                concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/tenant.digest"),
                &rendered.digest,
            )
            .unwrap();
            rendered.digest.clone()
        })
    );
}

/// Tail-consumer golden: two user-declared consumers on one producer,
/// coexisting with the platform log collector in the same `tails` list.
fn tails_tenant() -> (TenantDesired, Vec<vrt_config::VersionBundle>) {
    let third_bundle = vrt_config::VersionBundle::build(
        vrt_config::DeployManifest::from_pack(&serde_json::json!({
            "mainModule": "third.js",
            "modules": {
                "third.js": "export default { fetch() { return new Response('third') } }"
            },
            "compatibilityDate": "2026-10-20",
            "compatibilityFlags": []
        }))
        .unwrap(),
    )
    .unwrap();
    let mut tenant = tenant();
    let mut third = tenant.workers[1].clone();
    third.worker_id = "w-alpha-3".into();
    third.name = "audit".into();
    third.active_version.version_id = "v-2026-11-08-3".into();
    third.active_version.artifact_digest = third_bundle.digest().to_string();
    third.active_version.compatibility_date = "2026-10-20".into();
    third.routes = vec![serde_json::from_value(serde_json::json!({
        "hostname": "audit--team-alpha.vibeploy.app"
    }))
    .unwrap()];
    third.bindings.clear();
    third.crons.clear();
    tenant.workers[0].tail_consumers = vec![
        TailConsumerDesired {
            worker_id: "w-alpha-3".into(),
        },
        TailConsumerDesired {
            worker_id: "w-alpha-2".into(),
        },
    ];
    tenant.workers.push(third);
    (tenant, vec![bundle_main(), bundle_aux(), third_bundle])
}

#[test]
fn golden_tail_consumers_match_byte_for_byte() {
    let (tenant, bundles) = tails_tenant();
    let mut params = runtime_params();
    params.log_tail = vrt_config::LogTailParams {
        sink_addr: "127.0.0.1:9494".into(),
        pepper: "golden-tails-pepper".into(),
    };
    let rendered = render_tenant_config(&input(&tenant, &bundles, &params)).unwrap();

    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN_TAILS, &rendered.text).unwrap();
        std::fs::write(GOLDEN_TAILS_DIGEST, &rendered.digest).unwrap();
    }
    let expected =
        std::fs::read_to_string(GOLDEN_TAILS).expect("missing tail golden; run UPDATE_GOLDEN=1");
    assert_eq!(rendered.text, expected, "tail golden mismatch");
    assert_eq!(
        rendered.digest,
        std::fs::read_to_string(GOLDEN_TAILS_DIGEST)
            .expect("missing tail golden digest")
            .trim()
    );
}
