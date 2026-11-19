//! Byte-for-byte golden comparison of the rendered `config.capnp`.
//!
//! Regenerate with `UPDATE_GOLDEN=1 cargo test -p vrt-config`, then review
//! the diff — the golden file is the reviewed contract of the emitted
//! workerd configuration.

mod common;

use common::*;
use vrt_config::render_tenant_config;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/tenant.capnp");

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
