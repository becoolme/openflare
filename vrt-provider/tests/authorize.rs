//! Authorizer rejection matrix — every denial path must produce the
//! uniform `BINDING_PERMISSION_DENIED` (or `RESOURCE_UNAVAILABLE` for
//! snapshot faults) while logging a structured reason.

mod support;

use support::*;
use vrt_provider::context::ResourceKind;
use vrt_provider::desired::MemoryDesiredStore;
use vrt_provider::error::{ForbiddenReason, ProviderError};

fn base_store(f: &Fixture) -> MemoryDesiredStore {
    store_with(
        f,
        &desired(
            7,
            vec![tenant(
                TENANT,
                vec![
                    worker(
                        WORKER,
                        VERSION,
                        DIGEST,
                        vec![kv_binding("KV", NS_ID), r2_binding("BUCKET", BUCKET)],
                    ),
                    worker(
                        OTHER_WORKER,
                        "v-other",
                        DIGEST2,
                        vec![kv_binding("KV", "ns-other")],
                    ),
                ],
            )],
        ),
        7,
    )
}

fn denied(err: ProviderError) -> ForbiddenReason {
    match err {
        ProviderError::Forbidden(reason) => reason,
        other => panic!("expected Forbidden, got {other}"),
    }
}

#[test]
fn ok_grants_context() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    let ctx = authorize(&auth, TENANT, &id, ResourceKind::Kv).expect("grant");
    assert_eq!(ctx.tenant_id(), TENANT);
    assert_eq!(ctx.worker_id(), WORKER);
    assert_eq!(ctx.version_id(), VERSION);
    assert_eq!(ctx.binding_name(), "KV");
    assert_eq!(ctx.resource().resource_id(), NS_ID);
    assert!(ctx.permissions().read && ctx.permissions().write);
    assert_eq!(ctx.snapshot_revision(), 7);
}

#[test]
fn wrong_tenant_socket_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    let err = authorize(&auth, OTHER_TENANT, &id, ResourceKind::Kv).unwrap_err();
    // tenant not in snapshot → worker lookup fails → Missing? tenant not
    // present yields DesiredUnavailable::Missing → ResourceUnavailable.
    assert!(matches!(err, ProviderError::ResourceUnavailable));
}

#[test]
fn worker_id_header_mismatch() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let mut id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    id.worker_id_header = OTHER_WORKER.to_string();
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::WorkerIdMismatch);
}

#[test]
fn binding_id_header_mismatch() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let mut id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    id.binding_id_header = Some("w-aaa~OTHER".to_string());
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::BindingIdMismatch);
}

#[test]
fn unknown_worker_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity("w-ghost", "KV", "w-ghost", VERSION, DIGEST, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::WorkerNotFound);
}

#[test]
fn version_mismatch_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "KV", WORKER, "v-9999", DIGEST, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::VersionMismatch);
}

#[test]
fn draining_version_accepted_then_aged_out() {
    let f = fixture();
    let store = MemoryDesiredStore::new(1);
    let d1 = desired(
        7,
        vec![tenant(
            TENANT,
            vec![worker(
                WORKER,
                "v-old",
                DIGEST,
                vec![kv_binding("KV", NS_ID)],
            )],
        )],
    );
    store
        .apply(&verify(&f, &sign_desired(&f, &d1, 7), test_now()))
        .unwrap();
    // revision 8 promotes v-new; v-old drains under grace=1
    let d2 = desired(
        8,
        vec![tenant(
            TENANT,
            vec![worker(
                WORKER,
                "v-new",
                DIGEST2,
                vec![kv_binding("KV", NS_ID)],
            )],
        )],
    );
    store
        .apply(&verify(&f, &sign_desired(&f, &d2, 8), test_now()))
        .unwrap();
    let auth = authorizer(store, 30_000);
    let id = identity(WORKER, "KV", WORKER, "v-old", DIGEST, NS_ID, 8);
    authorize(&auth, TENANT, &id, ResourceKind::Kv).expect("draining version");
    let id = identity(WORKER, "KV", WORKER, "v-new", DIGEST2, NS_ID, 8);
    authorize(&auth, TENANT, &id, ResourceKind::Kv).expect("active version");
}

#[test]
fn digest_mismatch_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST2, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::DigestMismatch);
}

#[test]
fn unknown_binding_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "NOPE", WORKER, VERSION, DIGEST, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::BindingNotFound);
}

#[test]
fn kind_mismatch_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    // KV binding requested via R2 product path.
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::R2).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::BindingKindMismatch);
}

#[test]
fn cross_namespace_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    // Facade claims a different namespace than authorized.
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, "ns-other", 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::ResourceMismatch);
}

#[test]
fn cross_worker_resource_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    // w-bbb's binding id used with w-aaa's worker header → mismatch early.
    let id = identity(
        OTHER_WORKER,
        "KV",
        WORKER,
        "v-other",
        DIGEST2,
        "ns-other",
        7,
    );
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::WorkerIdMismatch);
}

#[test]
fn generation_beyond_revision_denied() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 8);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert_eq!(denied(err), ForbiddenReason::SpecGenerationMismatch);
}

#[test]
fn missing_snapshot_fails_closed() {
    let auth = authorizer(MemoryDesiredStore::new(1), 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert!(matches!(err, ProviderError::ResourceUnavailable));
}

#[test]
fn expired_snapshot_fails_closed() {
    let f = fixture();
    let mut guard = snapshot_protocol::envelope::RevisionGuard::new();
    let payload = desired(
        7,
        vec![tenant(
            TENANT,
            vec![worker(
                WORKER,
                VERSION,
                DIGEST,
                vec![kv_binding("KV", NS_ID)],
            )],
        )],
    );
    let bytes = snapshot_protocol::envelope::sign(
        &snapshot_protocol::envelope::SignParams {
            kind: "compute.desired",
            scope: &snapshot_protocol::compute_desired::cell_scope(CELL),
            revision: 7,
            issued_at: test_now() - chrono::Duration::minutes(20),
            expires_at: test_now() - chrono::Duration::minutes(10),
            payload: &payload,
        },
        &f.key,
    )
    .unwrap();
    // verification at an earlier `now` inside the window
    let verified = snapshot_protocol::compute_desired::verify_compute_desired(
        &bytes.bytes,
        CELL,
        &f.trusted,
        &mut guard,
        test_now() - chrono::Duration::minutes(15),
    )
    .unwrap();
    let store = MemoryDesiredStore::new(1);
    store.apply(&verified).unwrap();
    let auth = authorizer(store, 30_000);
    let id = identity(WORKER, "KV", WORKER, VERSION, DIGEST, NS_ID, 7);
    let err = authorize(&auth, TENANT, &id, ResourceKind::Kv).unwrap_err();
    assert!(matches!(err, ProviderError::ResourceUnavailable));
}

#[test]
fn r2_binding_grants_r2_context() {
    let f = fixture();
    let auth = authorizer(base_store(&f), 30_000);
    let id = identity(WORKER, "BUCKET", WORKER, VERSION, DIGEST, BUCKET, 7);
    let ctx = authorize(&auth, TENANT, &id, ResourceKind::R2).expect("grant");
    assert_eq!(ctx.resource().resource_id(), BUCKET);
    assert!(matches!(
        ctx.resource(),
        vrt_provider::context::BindingResource::R2Bucket { .. }
    ));
}

#[test]
fn binding_id_wire_format() {
    // single path segment; `~` separator cannot be forged inside charsets
    let id = vrt_provider::identity::binding_id("w-1", "KV");
    assert_eq!(id, "w-1~KV");
    assert!(!id.contains('/'));
    let (worker, name) = vrt_provider::identity::parse_binding_id(&id).unwrap();
    assert_eq!((worker.as_str(), name.as_str()), ("w-1", "KV"));
    for bad in ["w-1", "w-1~", "~KV", "w~1~K", "", "w 1~KV", "w-1~1bad"] {
        assert!(
            vrt_provider::identity::parse_binding_id(bad).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn identity_header_parsing() {
    use axum::http::HeaderMap;
    let binding = vrt_provider::identity::binding_id(WORKER, "KV");
    let mut headers = HeaderMap::new();
    for (name, value) in standard_headers(&binding, WORKER, VERSION, DIGEST, NS_ID, 7) {
        headers.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    headers.insert("x-open-compute-binding-id", binding.parse().unwrap());
    let id = vrt_provider::identity::parse_request_identity(&binding, &headers).unwrap();
    assert_eq!(id.binding_worker_id, WORKER);
    assert_eq!(id.binding_name, "KV");
    // missing required header → protocol error
    let mut headers2 = headers.clone();
    headers2.remove("x-open-compute-worker-id");
    assert!(vrt_provider::identity::parse_request_identity(&binding, &headers2).is_err());
    // malformed digest
    let mut headers3 = headers.clone();
    headers3.insert("x-open-compute-descriptor-sha256", "zz".parse().unwrap());
    assert!(vrt_provider::identity::parse_request_identity(&binding, &headers3).is_err());
    // non-canonical uuid
    let mut headers4 = headers.clone();
    headers4.insert(
        "x-open-compute-request-id",
        "11111111222233334444555555555555".parse().unwrap(),
    );
    assert!(vrt_provider::identity::parse_request_identity(&binding, &headers4).is_err());
    // generation 0 rejected
    let mut headers5 = headers.clone();
    headers5.insert(
        "x-open-compute-resource-spec-generation",
        "0".parse().unwrap(),
    );
    assert!(vrt_provider::identity::parse_request_identity(&binding, &headers5).is_err());
}
