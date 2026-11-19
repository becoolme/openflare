//! Protocol limits and wire constants, mirroring the upstream
//! `binding_backend` contract (open-compute `crates/service`,
//! `crates/storage/src/kv`, `crates/artifacts/src/r2_model`).

/// KV binary frame content type (all KV ops).
pub const KV_FRAME_CONTENT_TYPE: &str = "application/vnd.open-compute.kv.v1+frame";
/// R2 JSON content type (head, delete, list, multipart create/complete/abort).
pub const R2_JSON_CONTENT_TYPE: &str = "application/vnd.open-compute.r2.v1+json";
/// R2 frame content type (get, put, uploadPart).
pub const R2_FRAME_CONTENT_TYPE: &str = "application/vnd.open-compute.r2.v1+frame";

/// Error-code header returned on every error response.
pub const ERROR_HEADER: &str = "x-open-compute-error-code";
/// Active worker version id.
pub const VERSION_ID_HEADER: &str = "x-open-compute-version-id";
/// sha256 of the version descriptor/artifact, lowercase hex.
pub const DESCRIPTOR_HEADER: &str = "x-open-compute-descriptor-sha256";
/// Canonical-UUID request id assigned by the facade.
pub const REQUEST_ID_HEADER: &str = "x-open-compute-request-id";
/// Platform worker id (vrt extension, must equal the binding id owner).
pub const WORKER_ID_HEADER: &str = "x-open-compute-worker-id";
/// Resource id/name the facade believes it is bound to (vrt extension).
pub const NAMESPACE_RESOURCE_ID_HEADER: &str = "x-open-compute-namespace-resource-id";
/// Desired-state generation the facade props were issued at (vrt extension).
pub const SPEC_GENERATION_HEADER: &str = "x-open-compute-resource-spec-generation";
/// Optional defense-in-depth copy of the path binding id (vrt extension).
pub const BINDING_ID_HEADER: &str = "x-open-compute-binding-id";

/// Maximum UTF-8 bytes in a KV key.
pub const KV_MAX_KEY_BYTES: usize = 512;
/// Maximum bytes in one KV value.
pub const KV_MAX_VALUE_BYTES: usize = 25 * 1024 * 1024;
/// Maximum bytes in canonical KV metadata JSON.
pub const KV_MAX_METADATA_BYTES: usize = 1024;
/// Maximum keys in one multi-get.
pub const KV_MAX_MULTI_GET_KEYS: usize = 100;
/// Maximum bytes in one aggregate multi-get response.
pub const KV_MAX_MULTI_GET_RESPONSE_BYTES: usize = 25 * 1024 * 1024;
/// Maximum KV list page size.
pub const KV_MAX_LIST_LIMIT: u16 = 1000;
/// Minimum relative expiration in seconds.
pub const KV_MIN_EXPIRATION_TTL_SECONDS: u64 = 60;
/// Minimum accepted cache TTL in seconds.
pub const KV_MIN_CACHE_TTL_SECONDS: u64 = 30;
/// Maximum bytes in the JSON part of a KV put frame header.
pub const KV_PUT_HEADER_MAX_BYTES: usize = 4096;
/// Maximum bytes in any KV non-put request body.
pub const KV_MAX_FRAME_BODY_BYTES: usize = KV_MAX_VALUE_BYTES + 64 * 1024;

/// Maximum UTF-8 bytes in an R2 object key.
pub const R2_MAX_KEY_BYTES: usize = 1024;
/// Maximum keys accepted by one R2 delete call.
pub const R2_MAX_DELETE_KEYS: usize = 1000;
/// Maximum R2 list page size.
pub const R2_MAX_LIST_LIMIT: u16 = 1000;
/// Maximum canonical custom-metadata JSON bytes.
pub const R2_MAX_CUSTOM_METADATA_JSON_BYTES: usize = 4 * 1024;
/// Maximum bytes in an R2 JSON request body / frame header.
pub const R2_MAX_METADATA_BYTES: usize = 16 * 1024;
/// Maximum bytes in an R2 delete request body.
pub const R2_MAX_DELETE_BODY_BYTES: usize = 1024 * 1024 + R2_MAX_METADATA_BYTES;

/// Signed list-cursor lifetime in milliseconds.
pub const CURSOR_TTL_MS: i64 = 15 * 60 * 1000;

/// Hard cap on a request's Content-Length before it is even read (KV family).
pub const DECLARED_BODY_HARD_CAP: u64 = (KV_MAX_FRAME_BODY_BYTES as u64) + 1024;
