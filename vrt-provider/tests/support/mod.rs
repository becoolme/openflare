#![allow(dead_code)]

//! Shared test helpers: signed `compute.desired` fixtures, authorizer
//! plumbing, and a minimal HTTP/1.1-over-unix client.

use chrono::{DateTime, Duration, Utc};
use snapshot_protocol::compute_desired::verify_compute_desired;
use snapshot_protocol::compute_desired::{
    BindingDesired, BindingPermissions, ComputeDesired, KvBinding, QueueBinding, R2Binding,
    RouteDesired, TenantDesired, VersionDesired, WorkerDesired, WorkerQuotas,
};
use snapshot_protocol::envelope::Verified;
use snapshot_protocol::envelope::{sign, RevisionGuard, SignParams, SigningKey, TrustedKey};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use vrt_provider::authorize::Authorizer;
use vrt_provider::context::{BindingContext, ResourceKind};
use vrt_provider::desired::MemoryDesiredStore;
use vrt_provider::error::ProviderError;
use vrt_provider::identity::{self, RequestIdentity};

pub const CELL: &str = "cell-a";
pub const TENANT: &str = "team-1";
pub const OTHER_TENANT: &str = "team-2";
pub const WORKER: &str = "w-aaa";
pub const OTHER_WORKER: &str = "w-bbb";
pub const VERSION: &str = "v-1001";
pub const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const DIGEST2: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const NS_ID: &str = "ns-9f8e7d6c";
pub const BUCKET: &str = "bucket-1";
pub const REQUEST_ID: &str = "11111111-2222-3333-4444-555555555555";

pub fn test_now() -> DateTime<Utc> {
    Utc::now()
}

pub fn quotas() -> WorkerQuotas {
    WorkerQuotas {
        cpu_ms: 50,
        memory_mb: 128,
        subrequests: 100,
        outbound_connections: 16,
    }
}

pub fn version(version_id: &str, digest: &str) -> VersionDesired {
    VersionDesired {
        version_id: version_id.to_string(),
        artifact_digest: digest.to_string(),
        compatibility_date: "2026-01-01".to_string(),
        compatibility_flags: vec![],
    }
}

pub fn queue_binding_perms(name: &str, queue_id: &str, read: bool, write: bool) -> BindingDesired {
    BindingDesired::Queue(QueueBinding {
        name: name.to_string(),
        queue_id: queue_id.to_string(),
        queue_name: name.to_string(),
        permissions: BindingPermissions { read, write },
    })
}

pub fn queue_binding(name: &str, queue_id: &str) -> BindingDesired {
    queue_binding_perms(name, queue_id, true, true)
}

pub fn kv_binding_perms(name: &str, namespace: &str, read: bool, write: bool) -> BindingDesired {
    BindingDesired::Kv(KvBinding {
        name: name.to_string(),
        namespace_id: namespace.to_string(),
        permissions: snapshot_protocol::compute_desired::BindingPermissions { read, write },
    })
}

pub fn kv_binding(name: &str, namespace: &str) -> BindingDesired {
    kv_binding_perms(name, namespace, true, true)
}

pub fn r2_binding_perms(name: &str, bucket: &str, read: bool, write: bool) -> BindingDesired {
    BindingDesired::R2(R2Binding {
        name: name.to_string(),
        bucket: bucket.to_string(),
        permissions: snapshot_protocol::compute_desired::BindingPermissions { read, write },
    })
}

pub fn r2_binding(name: &str, bucket: &str) -> BindingDesired {
    r2_binding_perms(name, bucket, true, true)
}

pub fn worker(
    id: &str,
    version_id: &str,
    digest: &str,
    bindings: Vec<BindingDesired>,
) -> WorkerDesired {
    WorkerDesired {
        worker_id: id.to_string(),
        name: id.to_string(),
        active_version: version(version_id, digest),
        bindings,
        routes: vec![RouteDesired {
            hostname: format!("{id}.example.test"),
        }],
        crons: vec![],
        queue_consumers: vec![],
        durable_object_classes: vec![],
        workflows: vec![],
        tail_consumers: vec![],
        streaming_tail_consumers: vec![],
        quotas: quotas(),
    }
}

pub fn desired(revision: u64, tenants: Vec<TenantDesired>) -> ComputeDesired {
    ComputeDesired {
        schema_version: 1,
        cell_id: CELL.to_string(),
        revision,
        tenants,
        deletions: vec![],
    }
}

pub fn tenant(tenant_id: &str, workers: Vec<WorkerDesired>) -> TenantDesired {
    TenantDesired {
        tenant_id: tenant_id.to_string(),
        workers,
    }
}

pub struct Fixture {
    pub key: SigningKey,
    pub trusted: Vec<TrustedKey>,
    pub expires: DateTime<Utc>,
}

pub fn fixture() -> Fixture {
    let key = SigningKey::from_seed(&[7u8; 32]).expect("seeded key");
    Fixture {
        trusted: vec![key.trusted_key()],
        expires: test_now() + Duration::minutes(10),
        key,
    }
}

pub fn sign_desired(fixture: &Fixture, payload: &ComputeDesired, revision: u64) -> Vec<u8> {
    let signed = sign(
        &SignParams {
            kind: "compute.desired",
            scope: &snapshot_protocol::compute_desired::cell_scope(CELL),
            revision,
            issued_at: test_now(),
            expires_at: fixture.expires,
            payload,
        },
        &fixture.key,
    )
    .expect("sign");
    signed.bytes
}

pub fn verify(fixture: &Fixture, bytes: &[u8], now: DateTime<Utc>) -> Verified<ComputeDesired> {
    let mut guard = RevisionGuard::new();
    verify_compute_desired(bytes, CELL, &fixture.trusted, &mut guard, now).expect("verify")
}

pub fn store_with(
    fixture: &Fixture,
    payload: &ComputeDesired,
    revision: u64,
) -> MemoryDesiredStore {
    let bytes = sign_desired(fixture, payload, revision);
    let verified = verify(fixture, &bytes, test_now());
    let store = MemoryDesiredStore::new(1);
    store.apply(&verified).expect("apply");
    store
}

pub fn authorizer(store: MemoryDesiredStore, timeout_ms: u64) -> Authorizer {
    Authorizer::new(
        Arc::new(store),
        std::time::Duration::from_millis(timeout_ms),
    )
}

pub fn identity(
    binding_worker: &str,
    binding_name: &str,
    worker_header: &str,
    version_id: &str,
    digest: &str,
    resource: &str,
    generation: u64,
) -> RequestIdentity {
    RequestIdentity {
        binding_worker_id: binding_worker.to_string(),
        binding_name: binding_name.to_string(),
        version_id: version_id.to_string(),
        descriptor_sha256: digest.to_string(),
        request_id: REQUEST_ID.to_string(),
        worker_id_header: worker_header.to_string(),
        namespace_resource_id: resource.to_string(),
        spec_generation: generation,
        binding_id_header: None,
    }
}

pub fn authorize(
    auth: &Authorizer,
    tenant: &str,
    id: &RequestIdentity,
    kind: ResourceKind,
) -> Result<BindingContext, ProviderError> {
    auth.authorize(
        tenant,
        id,
        &identity::binding_id(&id.binding_worker_id, &id.binding_name),
        kind,
    )
}

/// Minimal HTTP/1.1 POST over loopback TCP (unix `bind()` is unavailable
/// in the test sandbox; the axum drain path is identical). Returns
/// (status, headers, body).
pub async fn http_post(
    addr: std::net::SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: unix\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    stream.write_all(body).await.expect("write body");
    // Fail-closed handlers answer before draining the body; closing the
    // socket with unread bytes makes the kernel RST mid-response — a
    // reset after delivered bytes is end-of-stream, not failure.
    let mut raw = Vec::new();
    loop {
        let mut chunk = [0u8; 8192];
        match stream.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => break,
            Err(e) => panic!("read: {e}"),
        }
    }
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header/body separator");
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let body = raw[sep + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status");
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap_or((line, ""));
            (name.trim().to_lowercase(), value.trim().to_string())
        })
        .collect();
    (status, headers, body)
}

pub fn standard_headers(
    binding_id: &str,
    worker: &str,
    version_id: &str,
    digest: &str,
    resource: &str,
    generation: u64,
) -> Vec<(String, String)> {
    vec![
        (
            "x-open-compute-version-id".to_string(),
            version_id.to_string(),
        ),
        (
            "x-open-compute-descriptor-sha256".to_string(),
            digest.to_string(),
        ),
        (
            "x-open-compute-request-id".to_string(),
            REQUEST_ID.to_string(),
        ),
        ("x-open-compute-worker-id".to_string(), worker.to_string()),
        (
            "x-open-compute-namespace-resource-id".to_string(),
            resource.to_string(),
        ),
        (
            "x-open-compute-resource-spec-generation".to_string(),
            generation.to_string(),
        ),
        (
            "x-open-compute-binding-id".to_string(),
            binding_id.to_string(),
        ),
    ]
}

/// Short scratch dir for unix-socket tests: `tempfile` under `$TMPDIR`
/// cannot be hardened to 0700 in this environment and deep worktree paths
/// exceed `SUN_LEN`, so use a fixed short base instead.
pub struct TestDir(std::path::PathBuf);

impl TestDir {
    pub fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let base = std::path::PathBuf::from("/tmp/claude/vrt-provider-tests");
        std::fs::create_dir_all(&base).unwrap();
        let dir = base.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
