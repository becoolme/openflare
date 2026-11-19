//! Typed provider errors mapped to the upstream binding-backend error
//! contract: `{ "ok": false, "error": { code, retryable, resultUnknown } }`
//! plus the `x-open-compute-error-code` header.
//!
//! `Display` renders only the stable SCREAMING_SNAKE code — never secrets,
//! backend addresses, credentials, or attacker-controlled values.

use crate::limits::ERROR_HEADER;
use axum::body::Body;
use axum::http::{header, HeaderName, HeaderValue, Response, StatusCode};

/// Reason an authorization check failed. Internal, structured-logging only —
/// never serialized into an error response (uniform `403` denial).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForbiddenReason {
    /// `x-open-compute-worker-id` disagrees with the binding id owner.
    WorkerIdMismatch,
    /// `x-open-compute-binding-id` header disagrees with the path id.
    BindingIdMismatch,
    /// Worker is not assigned to the listener tenant in the snapshot.
    WorkerNotFound,
    /// `versionId` is neither the active version nor a draining version.
    VersionMismatch,
    /// `descriptorSha256` does not match the authorized version digest.
    DigestMismatch,
    /// Binding name does not exist on the worker.
    BindingNotFound,
    /// Binding kind does not match the requested product family.
    BindingKindMismatch,
    /// `namespaceResourceId` does not match the authorized resource.
    ResourceMismatch,
    /// `resourceSpecGeneration` is outside the accepted range.
    SpecGenerationMismatch,
    /// The operation exceeds the binding's declared read/write grant.
    PermissionDenied,
}

impl ForbiddenReason {
    /// Stable reason token for structured logs (never the values compared).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WorkerIdMismatch => "worker_id_mismatch",
            Self::BindingIdMismatch => "binding_id_mismatch",
            Self::WorkerNotFound => "worker_not_found",
            Self::VersionMismatch => "version_mismatch",
            Self::DigestMismatch => "digest_mismatch",
            Self::BindingNotFound => "binding_not_found",
            Self::BindingKindMismatch => "binding_kind_mismatch",
            Self::ResourceMismatch => "resource_mismatch",
            Self::SpecGenerationMismatch => "spec_generation_mismatch",
            Self::PermissionDenied => "permission_denied",
        }
    }
}

/// Typed provider error with a stable upstream-compatible code.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProviderError {
    /// Malformed protocol input (path, headers, JSON body, frame).
    #[error("BINDING_PROTOCOL_ERROR")]
    Protocol,
    /// Declared or framed request exceeded a fixed byte budget.
    #[error("BINDING_LIMIT_EXCEEDED")]
    LimitExceeded,
    /// Capability not implemented by this provider release (e.g. multipart).
    #[error("BINDING_CAPABILITY_UNSUPPORTED")]
    CapabilityUnsupported,
    /// Authorization mismatch; uniform `BINDING_PERMISSION_DENIED` outside.
    #[error("BINDING_PERMISSION_DENIED")]
    Forbidden(ForbiddenReason),
    /// Desired-state snapshot or a resource backend is unavailable.
    #[error("RESOURCE_UNAVAILABLE")]
    ResourceUnavailable,

    /// KV key is empty, `.`, or `..`.
    #[error("KV_KEY_INVALID")]
    KvKeyInvalid,
    /// KV key exceeds 512 bytes.
    #[error("KV_KEY_TOO_LARGE")]
    KvKeyTooLarge,
    /// KV value exceeds 25 MiB.
    #[error("KV_VALUE_TOO_LARGE")]
    KvValueTooLarge,
    /// KV metadata is not canonical JSON-compatible data.
    #[error("KV_METADATA_INVALID")]
    KvMetadataInvalid,
    /// KV metadata exceeds 1024 canonical bytes.
    #[error("KV_METADATA_TOO_LARGE")]
    KvMetadataTooLarge,
    /// KV expiration, cache, or list options are invalid.
    #[error("KV_INVALID_OPTIONS")]
    KvInvalidOptions,
    /// KV multi-get contains more than 100 keys.
    #[error("KV_TOO_MANY_KEYS")]
    KvTooManyKeys,
    /// KV aggregate multi-get response exceeds 25 MiB.
    #[error("KV_RESPONSE_TOO_LARGE")]
    KvResponseTooLarge,
    /// KV list cursor is malformed, expired, or scoped incorrectly.
    #[error("KV_CURSOR_INVALID")]
    KvCursorInvalid,
    /// KV backend is temporarily busy.
    #[error("KV_BUSY")]
    KvBusy,
    /// KV backend storage quota was reached.
    #[error("KV_STORAGE_FULL")]
    KvStorageFull,
    /// KV backend is unavailable or timed out on a read.
    #[error("KV_UNAVAILABLE")]
    KvUnavailable,
    /// KV backend data failed integrity validation.
    #[error("KV_CORRUPT")]
    KvCorrupt,
    /// A KV mutation may have committed before its result was observed.
    #[error("KV_RESULT_UNKNOWN")]
    KvResultUnknown,
    /// The KV private protocol frame was malformed.
    #[error("KV_INTERNAL_PROTOCOL_ERROR")]
    KvInternalProtocolError,

    /// R2 object key exceeds 1024 bytes.
    #[error("R2_KEY_TOO_LARGE")]
    R2KeyTooLarge,
    /// R2 range, conditional, metadata, or list options are invalid.
    #[error("R2_INVALID_OPTIONS")]
    R2InvalidOptions,
    /// Caller-supplied checksum does not match the object bytes.
    #[error("R2_CHECKSUM_MISMATCH")]
    R2ChecksumMismatch,
    /// SSE-C key material is malformed or unsupported by this provider.
    #[error("R2_SSEC_INVALID")]
    R2SsecInvalid,
    /// Multipart upload identity, part set, or lifecycle is invalid.
    #[error("R2_MULTIPART_INVALID")]
    R2MultipartInvalid,
    /// R2 object exceeds the bucket's single-part limit.
    #[error("R2_OBJECT_TOO_LARGE")]
    R2ObjectTooLarge,
    /// R2 metadata frame or custom metadata exceeds its budget.
    #[error("R2_METADATA_TOO_LARGE")]
    R2MetadataTooLarge,
    /// R2 list cursor is malformed, expired, or scoped incorrectly.
    #[error("R2_CURSOR_INVALID")]
    R2CursorInvalid,
    /// An R2 conditional operation did not match the current object.
    #[error("R2_PRECONDITION_FAILED")]
    R2PreconditionFailed,
    /// R2 capacity is temporarily saturated.
    #[error("R2_OVERLOADED")]
    R2Overloaded,
    /// The configured object backend is unavailable or timed out.
    #[error("R2_PROVIDER_UNAVAILABLE")]
    R2ProviderUnavailable,
    /// An R2 mutation may have committed before its result was observed.
    #[error("R2_RESULT_UNKNOWN")]
    R2ResultUnknown,
    /// Stored R2 object metadata failed integrity validation.
    #[error("R2_OBJECT_METADATA_INVALID")]
    R2ObjectMetadataInvalid,

    /// P1: PG credential ref absent from pg-credentials.json (tenant
    /// gone, rotation window, or the cell has not delivered it yet).
    #[error("PG_CREDENTIAL_UNAVAILABLE")]
    PgCredentialUnavailable,
    /// P1: gateway rejected the delivered tenant token (401 — rotated
    /// or revoked upstream); converges on the next render-inputs sync.
    #[error("PG_CREDENTIAL_EXPIRED")]
    PgCredentialExpired,
    /// P1: tenant unknown to the gateway (404 — deleted or moved).
    #[error("PG_TENANT_GONE")]
    PgTenantGone,
    /// P1: write attempted under a read-only grant or migration fence.
    #[error("PG_WRITE_FENCED")]
    PgWriteFenced,
    /// P1: gateway rate limit (429).
    #[error("PG_RATE_LIMITED")]
    PgRateLimited,
    /// P1: gateway 5xx/connect failure/timeout on a read-only request.
    #[error("PG_BACKEND_UNAVAILABLE")]
    PgBackendUnavailable,
    /// P1: upstream response exceeded the provider relay cap.
    #[error("PG_RESPONSE_TOO_LARGE")]
    PgResponseTooLarge,
    /// P1: a write may have committed before the error was observed —
    /// never auto-retried.
    #[error("PG_RESULT_UNKNOWN")]
    PgResultUnknown,

    /// Unclassified internal failure; never leaks details.
    #[error("INTERNAL_ERROR")]
    Internal,
}

impl ProviderError {
    /// Stable protocol error code carried in the body and error header.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Protocol => "BINDING_PROTOCOL_ERROR",
            Self::LimitExceeded => "BINDING_LIMIT_EXCEEDED",
            Self::CapabilityUnsupported => "BINDING_CAPABILITY_UNSUPPORTED",
            Self::Forbidden(_) => "BINDING_PERMISSION_DENIED",
            Self::ResourceUnavailable => "RESOURCE_UNAVAILABLE",
            Self::KvKeyInvalid => "KV_KEY_INVALID",
            Self::KvKeyTooLarge => "KV_KEY_TOO_LARGE",
            Self::KvValueTooLarge => "KV_VALUE_TOO_LARGE",
            Self::KvMetadataInvalid => "KV_METADATA_INVALID",
            Self::KvMetadataTooLarge => "KV_METADATA_TOO_LARGE",
            Self::KvInvalidOptions => "KV_INVALID_OPTIONS",
            Self::KvTooManyKeys => "KV_TOO_MANY_KEYS",
            Self::KvResponseTooLarge => "KV_RESPONSE_TOO_LARGE",
            Self::KvCursorInvalid => "KV_CURSOR_INVALID",
            Self::KvBusy => "KV_BUSY",
            Self::KvStorageFull => "KV_STORAGE_FULL",
            Self::KvUnavailable => "KV_UNAVAILABLE",
            Self::KvCorrupt => "KV_CORRUPT",
            Self::KvResultUnknown => "KV_RESULT_UNKNOWN",
            Self::KvInternalProtocolError => "KV_INTERNAL_PROTOCOL_ERROR",
            Self::R2KeyTooLarge => "R2_KEY_TOO_LARGE",
            Self::R2InvalidOptions => "R2_INVALID_OPTIONS",
            Self::R2ChecksumMismatch => "R2_CHECKSUM_MISMATCH",
            Self::R2SsecInvalid => "R2_SSEC_INVALID",
            Self::R2MultipartInvalid => "R2_MULTIPART_INVALID",
            Self::R2ObjectTooLarge => "R2_OBJECT_TOO_LARGE",
            Self::R2MetadataTooLarge => "R2_METADATA_TOO_LARGE",
            Self::R2CursorInvalid => "R2_CURSOR_INVALID",
            Self::R2PreconditionFailed => "R2_PRECONDITION_FAILED",
            Self::R2Overloaded => "R2_OVERLOADED",
            Self::R2ProviderUnavailable => "R2_PROVIDER_UNAVAILABLE",
            Self::R2ResultUnknown => "R2_RESULT_UNKNOWN",
            Self::R2ObjectMetadataInvalid => "R2_OBJECT_METADATA_INVALID",
            Self::PgCredentialUnavailable => "PG_CREDENTIAL_UNAVAILABLE",
            Self::PgCredentialExpired => "PG_CREDENTIAL_EXPIRED",
            Self::PgTenantGone => "PG_TENANT_GONE",
            Self::PgWriteFenced => "PG_WRITE_FENCED",
            Self::PgRateLimited => "PG_RATE_LIMITED",
            Self::PgBackendUnavailable => "PG_BACKEND_UNAVAILABLE",
            Self::PgResponseTooLarge => "PG_RESPONSE_TOO_LARGE",
            Self::PgResultUnknown => "PG_RESULT_UNKNOWN",
            Self::Internal => "INTERNAL_ERROR",
        }
    }

    /// HTTP status for this error under the upstream mapping table.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Protocol
            | Self::KvKeyInvalid
            | Self::KvMetadataInvalid
            | Self::KvInvalidOptions
            | Self::KvCursorInvalid
            | Self::KvInternalProtocolError
            | Self::R2InvalidOptions
            | Self::R2ChecksumMismatch
            | Self::R2SsecInvalid
            | Self::R2MultipartInvalid
            | Self::R2CursorInvalid
            | Self::R2PreconditionFailed => StatusCode::BAD_REQUEST,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::LimitExceeded
            | Self::KvKeyTooLarge
            | Self::KvValueTooLarge
            | Self::KvMetadataTooLarge
            | Self::KvResponseTooLarge
            | Self::KvTooManyKeys
            | Self::R2KeyTooLarge
            | Self::R2ObjectTooLarge
            | Self::R2MetadataTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::CapabilityUnsupported | Self::KvCorrupt | Self::R2ObjectMetadataInvalid => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::PgTenantGone => StatusCode::NOT_FOUND,
            Self::PgWriteFenced => StatusCode::CONFLICT,
            Self::PgRateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::ResourceUnavailable
            | Self::KvBusy
            | Self::KvStorageFull
            | Self::KvUnavailable
            | Self::KvResultUnknown
            | Self::R2Overloaded
            | Self::R2ProviderUnavailable
            | Self::R2ResultUnknown
            | Self::PgCredentialUnavailable
            | Self::PgCredentialExpired
            | Self::PgBackendUnavailable
            | Self::PgResponseTooLarge
            | Self::PgResultUnknown => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// `retryable` flag in the upstream error body for a product family.
    /// The KV and R2 tables differ upstream: KV marks
    /// `BINDING_PROTOCOL_ERROR` retryable; R2 marks only the
    /// provider-availability family retryable.
    pub fn retryable_in(&self, product: crate::context::ResourceKind) -> bool {
        use crate::context::ResourceKind;
        match product {
            ResourceKind::Kv => matches!(self, Self::Protocol | Self::ResourceUnavailable),
            ResourceKind::R2 => matches!(
                self,
                Self::ResourceUnavailable
                    | Self::R2Overloaded
                    | Self::R2ProviderUnavailable
                    | Self::R2ResultUnknown
            ),
            ResourceKind::Assets | ResourceKind::D1 | ResourceKind::Queue => {
                matches!(self, Self::ResourceUnavailable)
            }
            ResourceKind::Pg => matches!(
                self,
                Self::Protocol
                    | Self::ResourceUnavailable
                    | Self::PgCredentialUnavailable
                    | Self::PgCredentialExpired
                    | Self::PgRateLimited
                    | Self::PgBackendUnavailable
                    | Self::PgResultUnknown
            ),
        }
    }

    /// `resultUnknown` flag — the mutation may have committed.
    pub fn result_unknown(&self) -> bool {
        matches!(
            self,
            Self::KvResultUnknown | Self::R2ResultUnknown | Self::PgResultUnknown
        )
    }

    /// Authorization-denial reason for structured logs; `None` otherwise.
    pub fn forbidden_reason(&self) -> Option<&'static str> {
        match self {
            Self::Forbidden(reason) => Some(reason.as_str()),
            _ => None,
        }
    }
}

/// Build an upstream-shaped error response for an explicit code/status
/// (used for protocol rejects where the upstream status differs from the
/// default mapping — e.g. `404` on unparseable KV paths).
pub fn error_response(
    code: &'static str,
    status: StatusCode,
    retryable: bool,
    result_unknown: bool,
) -> Response<Body> {
    let body = serde_json::json!({
        "ok": false,
        "error": {
            "code": code,
            "retryable": retryable,
            "resultUnknown": result_unknown,
        }
    });
    let mut response = Response::new(Body::from(serde_json::to_vec(&body).unwrap_or_else(|_| {
        b"{\"ok\":false,\"error\":{\"code\":\"INTERNAL_ERROR\",\"retryable\":false,\"resultUnknown\":false}}".to_vec()
    })));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Ok(value) = HeaderValue::from_str(code) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(ERROR_HEADER), value);
    }
    response
}
