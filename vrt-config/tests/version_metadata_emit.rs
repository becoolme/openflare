//! Version-metadata emit — `{"type":"versionMetadata"}` bindings
//! render a `json` binding holding `{id, tag, timestamp}` of the
//! worker's ACTIVE version (`WorkerVersionMetadata`, CF
//! `[version_metadata]`). The values are version-scoped
//! (`activeVersion.{versionId,tag,createdAt}`) — never carried by the
//! binding, so content-addressed version dedup survives re-deploys.

mod common;

use common::*;
use snapshot_protocol::compute_desired::{BindingDesired, TenantDesired, VersionMetadataBinding};
use vrt_config::*;

fn bs() -> Vec<VersionBundle> {
    vec![bundle_main(), bundle_aux()]
}

fn tenant_with_version_metadata() -> TenantDesired {
    let mut t = tenant();
    t.workers[0]
        .bindings
        .push(BindingDesired::VersionMetadata(VersionMetadataBinding {
            name: "CF_VERSION_METADATA".into(),
        }));
    t
}

#[test]
fn version_metadata_emits_active_version_json() {
    let mut t = tenant_with_version_metadata();
    t.workers[0].active_version.tag = "v1".into();
    t.workers[0].active_version.created_at = "2026-11-22T04:00:00.000Z".into();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();

    // Canonical JSON (sorted keys: id < tag < timestamp) with the
    // worker's own version id — worker 1's, not worker 2's.
    assert!(
        r.text.contains(
            "(name = \"CF_VERSION_METADATA\", json = \"{\\\"id\\\":\\\"v-2026-11-08-1\\\",\\\"tag\\\":\\\"v1\\\",\\\"timestamp\\\":\\\"2026-11-22T04:00:00.000Z\\\"}\")",
        ),
        "{}",
        r.text
    );
    assert!(!r.text.contains("v-2026-11-08-2\\\",\\\"tag"), "{}", r.text);
}

#[test]
fn version_metadata_untagged_emits_empty_tag() {
    // CF: a version deployed without --tag reports `tag: ""`.
    let t = tenant_with_version_metadata();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(
        r.text.contains(
            "\\\"id\\\":\\\"v-2026-11-08-1\\\",\\\"tag\\\":\\\"\\\",\\\"timestamp\\\":\\\"\\\"",
        ),
        "{}",
        r.text
    );
}

#[test]
fn version_metadata_absent_emits_no_binding() {
    let t = tenant();
    let params = runtime_params();
    let r = render_tenant_config(&input(&t, &bs(), &params)).unwrap();
    assert!(!r.text.contains("VERSION_METADATA"), "{}", r.text);
}
