//! `BindingContext` — the sealed, typed authorization scope every provider
//! call runs under. It can only be constructed by the crate's authorizer
//! (the constructor is `pub(crate)`); request code and providers cannot
//! fabricate one.

use std::time::Duration;
use tokio::time::Instant;

/// Product family a binding resource belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceKind {
    /// Workers KV namespace.
    Kv,
    /// R2 bucket.
    R2,
    /// Hyperdrive-style Postgres database.
    Pg,
    /// Static-assets manifest (T21).
    Assets,
    /// D1-style SQLite database via vrt-sql (T34).
    D1,
    /// Message queue via vrt-sql QueueBackend (T37).
    Queue,
    /// K2 durable event-log stream via vrt-sql K2Backend.
    K2,
    /// Email sending binding — upstream adapter relay, no VRT storage.
    Email,
    /// Vectorize index — sqlite-vec exact-KNN backend in-process.
    Vectorize,
}

/// Typed binding resource resolved from the authorized desired state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindingResource {
    /// `{"type":"kv"}` — platform-assigned namespace id.
    KvNamespace {
        /// Platform KV namespace identifier.
        id: String,
    },
    /// `{"type":"r2"}` — bucket name.
    R2Bucket {
        /// Bucket name (lowercase DNS label).
        name: String,
    },
    /// `{"type":"pg"}` — platform database tenant reference (P1).
    PgDatabase {
        /// `vrtpg:<databaseTenantId>` — server-minted tenant reference.
        id: String,
        /// P1: render-inputs credential key — provider resolves
        /// token/endpoint/slug from pg-credentials.json, never from
        /// request data.
        credential_ref: String,
    },
    /// `{"type":"assets"}` — manifest digest (T21).
    AssetsManifest {
        /// sha256 manifest digest.
        digest: String,
    },
    /// `{"type":"d1"}` — server-assigned database + writer epoch (T34).
    D1Database {
        /// Platform database id (26 lowercase alnum).
        id: String,
        /// Writer epoch from the signed snapshot.
        epoch: u64,
        /// Optional per-database writer endpoint from the signed snapshot.
        writer_addr: Option<String>,
    },
    /// `{"type":"queue"}` — server-assigned queue id (T37).
    Queue {
        /// Platform queue id (26 lowercase alnum).
        id: String,
    },
    /// `{"type":"k2"}` — server-assigned K2 stream id.
    K2Stream {
        /// Platform stream id (32 lowercase hex).
        id: String,
    },
    /// `{"type":"sendEmail"}` — the resource IS the signed
    /// send-bindings restriction set (no namespaced object exists);
    /// the binding name is the resource key.
    SendEmail {
        /// Binding name — the resource identifier.
        name: String,
        /// `destination_address` pin (mutually exclusive with the list).
        destination_address: Option<String>,
        /// `allowed_destination_addresses` allowlist.
        allowed_destination_addresses: Vec<String>,
        /// `allowed_sender_addresses` allowlist.
        allowed_sender_addresses: Vec<String>,
    },
    /// `{"type":"vectorize"}` — server-assigned index id + signed spec.
    VectorizeIndex {
        /// Platform index id (26 lowercase alnum).
        id: String,
        /// Fixed dimension count — signed at deploy; the on-disk index
        /// must match (fail closed on drift).
        dimensions: u32,
        /// Distance metric (`cosine`/`euclidean`/`dot-product`).
        metric: String,
    },
}

impl BindingResource {
    /// The product family of this resource.
    pub fn kind(&self) -> ResourceKind {
        match self {
            Self::KvNamespace { .. } => ResourceKind::Kv,
            Self::R2Bucket { .. } => ResourceKind::R2,
            Self::PgDatabase { .. } => ResourceKind::Pg,
            Self::AssetsManifest { .. } => ResourceKind::Assets,
            Self::D1Database { .. } => ResourceKind::D1,
            Self::Queue { .. } => ResourceKind::Queue,
            Self::K2Stream { .. } => ResourceKind::K2,
            Self::SendEmail { .. } => ResourceKind::Email,
            Self::VectorizeIndex { .. } => ResourceKind::Vectorize,
        }
    }

    /// The resource identifier compared against `namespaceResourceId`.
    pub fn resource_id(&self) -> &str {
        match self {
            Self::KvNamespace { id } => id,
            Self::R2Bucket { name } => name,
            Self::PgDatabase { id, .. } => id,
            Self::AssetsManifest { digest } => digest,
            Self::D1Database { id, .. } => id,
            Self::Queue { id } => id,
            Self::K2Stream { id } => id,
            // No server resource id exists; the binding name is the
            // resource key (the signed props are the state).
            Self::SendEmail { name, .. } => name,
            Self::VectorizeIndex { id, .. } => id,
        }
    }
}

/// Per-binding read/write flags, sourced from authorized desired state —
/// never from request headers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Permissions {
    /// Read operations (get/head/list).
    pub read: bool,
    /// Write operations (put/delete/multipart).
    pub write: bool,
}

/// Authorization scope granted to one binding-backend request.
///
/// Constructed exclusively by [`crate::authorize::Authorizer`]. All fields
/// are private; read-only accessors are public.
#[derive(Debug)]
pub struct BindingContext {
    tenant_id: String,
    worker_id: String,
    version_id: String,
    binding_id: String,
    binding_name: String,
    resource: BindingResource,
    permissions: Permissions,
    spec_generation: u64,
    snapshot_revision: u64,
    deadline: Instant,
}

impl BindingContext {
    /// Sealed constructor — only the authorizer may grant a context.
    /// The argument list is the authorization record itself; grouping it
    /// into a builder type would just move the same fields elsewhere.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn grant(
        tenant_id: String,
        worker_id: String,
        version_id: String,
        binding_id: String,
        binding_name: String,
        resource: BindingResource,
        permissions: Permissions,
        spec_generation: u64,
        snapshot_revision: u64,
        deadline: Instant,
    ) -> Self {
        Self {
            tenant_id,
            worker_id,
            version_id,
            binding_id,
            binding_name,
            resource,
            permissions,
            spec_generation,
            snapshot_revision,
            deadline,
        }
    }

    /// Listener-bound tenant identity (never from request headers).
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    /// Authorized worker id.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
    /// Authorized active or draining version id.
    pub fn version_id(&self) -> &str {
        &self.version_id
    }
    /// Full binding id from the request path (`<workerId>~<bindingName>`).
    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }
    /// JS binding name (`env.<name>`).
    pub fn binding_name(&self) -> &str {
        &self.binding_name
    }
    /// Typed resource from the authorized binding table.
    pub fn resource(&self) -> &BindingResource {
        &self.resource
    }
    /// Permissions from the authorized binding table.
    pub fn permissions(&self) -> Permissions {
        self.permissions
    }
    /// Desired-state generation presented by the facade (bounded by revision).
    pub fn spec_generation(&self) -> u64 {
        self.spec_generation
    }
    /// Snapshot revision this context was authorized against.
    pub fn snapshot_revision(&self) -> u64 {
        self.snapshot_revision
    }
    /// Absolute operation deadline.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// Remaining time before the deadline; saturates at zero.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}
