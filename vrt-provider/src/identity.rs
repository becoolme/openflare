//! Request identity extraction: path binding id plus the
//! `x-open-compute-*` / vrt extension headers. Everything here is
//! untrusted input — parsing only bounds format and length; authorization
//! decisions happen in [`crate::authorize`].
//!
//! Binding id wire format: `<workerId>~<bindingName>` — a single URL path
//! segment. `~` is outside both charsets, so `split_once('~')` is
//! unambiguous.

use crate::error::ProviderError;
use axum::http::HeaderMap;
use uuid::Uuid;

/// Parsed request identity.
#[derive(Clone, Debug)]
pub struct RequestIdentity {
    /// Worker id taken from the binding id in the path.
    pub binding_worker_id: String,
    /// Binding name (`env.<name>`).
    pub binding_name: String,
    /// `x-open-compute-version-id`.
    pub version_id: String,
    /// `x-open-compute-descriptor-sha256`, lowercase hex (64).
    pub descriptor_sha256: String,
    /// `x-open-compute-request-id`, canonical UUID string.
    pub request_id: String,
    /// `x-open-compute-worker-id` (must equal `binding_worker_id`).
    pub worker_id_header: String,
    /// `x-open-compute-namespace-resource-id`.
    pub namespace_resource_id: String,
    /// `x-open-compute-resource-spec-generation` (`>= 1`).
    pub spec_generation: u64,
    /// `x-open-compute-binding-id`, when present, must equal the path id.
    pub binding_id_header: Option<String>,
}

/// Identifier charset shared with the snapshot protocol:
/// `[A-Za-z0-9_.:-]`, `1..=128` bytes.
pub fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

/// Binding-name charset (`env.<name>`): `[A-Za-z0-9_$]`, `1..=64` bytes,
/// first char not a digit — same shape the snapshot validates.
pub fn is_valid_binding_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && !value.as_bytes()[0].is_ascii_digit()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// Split a path binding id into `(worker_id, binding_name)`. Both halves
/// must satisfy their charsets; `~` can appear exactly once.
pub fn parse_binding_id(id: &str) -> Result<(String, String), ProviderError> {
    let (worker, name) = id.split_once('~').ok_or(ProviderError::Protocol)?;
    if worker.contains('~') || !is_valid_identifier(worker) || !is_valid_binding_name(name) {
        return Err(ProviderError::Protocol);
    }
    Ok((worker.to_string(), name.to_string()))
}

/// Re-join `(worker_id, binding_name)` into the wire binding id.
pub fn binding_id(worker_id: &str, binding_name: &str) -> String {
    format!("{worker_id}~{binding_name}")
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ProviderError> {
    let value = headers.get(name).ok_or(ProviderError::Protocol)?;
    value.to_str().map_err(|_| ProviderError::Protocol)
}

fn parse_version_id(headers: &HeaderMap) -> Result<String, ProviderError> {
    let raw = header(headers, crate::limits::VERSION_ID_HEADER)?;
    if !is_valid_identifier(raw) {
        return Err(ProviderError::Protocol);
    }
    Ok(raw.to_string())
}

fn parse_descriptor(headers: &HeaderMap) -> Result<String, ProviderError> {
    let raw = header(headers, crate::limits::DESCRIPTOR_HEADER)?;
    if raw.len() != 64 || !raw.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(ProviderError::Protocol);
    }
    Ok(raw.to_string())
}

fn parse_request_id(headers: &HeaderMap) -> Result<String, ProviderError> {
    let raw = header(headers, crate::limits::REQUEST_ID_HEADER)?;
    let uuid = Uuid::parse_str(raw).map_err(|_| ProviderError::Protocol)?;
    if uuid.as_hyphenated().to_string() != raw {
        return Err(ProviderError::Protocol);
    }
    Ok(raw.to_string())
}

fn parse_worker_id(headers: &HeaderMap) -> Result<String, ProviderError> {
    let raw = header(headers, crate::limits::WORKER_ID_HEADER)?;
    if !is_valid_identifier(raw) {
        return Err(ProviderError::Protocol);
    }
    Ok(raw.to_string())
}

fn parse_namespace_resource_id(headers: &HeaderMap) -> Result<String, ProviderError> {
    let raw = header(headers, crate::limits::NAMESPACE_RESOURCE_ID_HEADER)?;
    if raw.is_empty() || raw.len() > 128 {
        return Err(ProviderError::Protocol);
    }
    Ok(raw.to_string())
}

fn parse_spec_generation(headers: &HeaderMap) -> Result<u64, ProviderError> {
    let raw = header(headers, crate::limits::SPEC_GENERATION_HEADER)?;
    let generation: u64 = raw.parse().map_err(|_| ProviderError::Protocol)?;
    if generation == 0 {
        return Err(ProviderError::Protocol);
    }
    Ok(generation)
}

/// Extract the full request identity. Any malformed or missing field is a
/// `BINDING_PROTOCOL_ERROR` — authorization itself is separate.
pub fn parse_request_identity(
    binding_id: &str,
    headers: &HeaderMap,
) -> Result<RequestIdentity, ProviderError> {
    let (binding_worker_id, binding_name) = parse_binding_id(binding_id)?;
    Ok(RequestIdentity {
        binding_worker_id,
        binding_name,
        version_id: parse_version_id(headers)?,
        descriptor_sha256: parse_descriptor(headers)?,
        request_id: parse_request_id(headers)?,
        worker_id_header: parse_worker_id(headers)?,
        namespace_resource_id: parse_namespace_resource_id(headers)?,
        spec_generation: parse_spec_generation(headers)?,
        binding_id_header: headers
            .get(crate::limits::BINDING_ID_HEADER)
            .map(|v| v.to_str().map(str::to_string))
            .transpose()
            .map_err(|_| ProviderError::Protocol)?,
    })
}
