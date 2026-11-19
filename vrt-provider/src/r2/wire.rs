//! R2 wire types and validation, mirroring upstream
//! `open_compute_artifacts::r2_model` / `service::r2_protocol`.

use crate::error::ProviderError;
use crate::limits::{R2_MAX_CUSTOM_METADATA_JSON_BYTES, R2_MAX_KEY_BYTES};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Six HTTP metadata fields supported by the facade.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2HttpMetadata {
    /// `Content-Type`.
    pub content_type: Option<String>,
    /// `Content-Language`.
    pub content_language: Option<String>,
    /// `Content-Disposition`.
    pub content_disposition: Option<String>,
    /// `Content-Encoding`.
    pub content_encoding: Option<String>,
    /// `Cache-Control`.
    pub cache_control: Option<String>,
    /// `Expires` as unix milliseconds.
    pub cache_expiry: Option<i64>,
}

/// Single HTTP byte range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2Range {
    /// First byte offset, absent for suffix ranges.
    pub offset: Option<u64>,
    /// Requested byte count, absent for open-ended or suffix ranges.
    pub length: Option<u64>,
    /// Last N bytes; mutually exclusive with offset/length.
    pub suffix: Option<u64>,
}

/// Opaque, weak, or wildcard ETag from `onlyIf`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum R2EtagMatch {
    /// RFC 7232 wildcard `*`.
    Wildcard,
    /// Strong validator.
    Strong {
        /// Unquoted ETag value.
        value: String,
    },
    /// Weak validator.
    Weak {
        /// Unquoted ETag value.
        value: String,
    },
}

impl R2EtagMatch {
    fn strong_matches(&self, etag: &str) -> bool {
        match self {
            Self::Wildcard => true,
            Self::Strong { value } => value == etag,
            Self::Weak { .. } => false,
        }
    }

    fn weak_matches(&self, etag: &str) -> bool {
        match self {
            Self::Wildcard => true,
            Self::Strong { value } | Self::Weak { value } => value == etag,
        }
    }
}

/// ETag/date read or write condition (`onlyIf` + Headers form).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2Condition {
    /// At least one listed validator must match. Accepts the
    /// tenant-facing `onlyIf` shape (`"etag"` / `"*"` / single string
    /// or object array) as well as the normalized `R2EtagMatch` list —
    /// the facade transport forwards options verbatim.
    #[serde(default, deserialize_with = "de_etag_match_list")]
    pub etag_matches: Vec<R2EtagMatch>,
    /// Every listed validator must differ.
    #[serde(default, deserialize_with = "de_etag_match_list")]
    pub etag_does_not_match: Vec<R2EtagMatch>,
    /// Object must not have been uploaded after this unix ms.
    pub uploaded_before: Option<i64>,
    /// Object must have been uploaded after this unix ms.
    pub uploaded_after: Option<i64>,
    /// Compare upload timestamps at whole-second resolution.
    #[serde(default)]
    pub seconds_granularity: bool,
    /// Apply RFC conditional-header precedence rather than field
    /// conjunction.
    #[serde(default)]
    pub http_headers: bool,
}

/// Bounds for the lenient `onlyIf` string forms: etags are short
/// validators; caps keep a hostile wire payload from allocating.
const MAX_ETAG_LEN: usize = 256;
const MAX_ETAG_LIST: usize = 16;

/// Parse one etag token in the lenient string form (CF `onlyIf`
/// semantics): `*` is the RFC 7232 wildcard, `W/"v"` is a weak
/// validator, any other unquoted token is a strong validator. Quoted
/// (`"v"`), empty, or overlong tokens are rejected at the typed
/// boundary — matching the facade's "Conditional ETag should not be
/// wrapped in quotes" rule rather than silently re-mangling.
fn parse_etag_token(s: &str) -> Result<R2EtagMatch, String> {
    if s.is_empty() {
        return Err("etag is empty".to_string());
    }
    if s.len() > MAX_ETAG_LEN {
        return Err("etag too long".to_string());
    }
    if s == "*" {
        return Ok(R2EtagMatch::Wildcard);
    }
    if let Some(rest) = s.strip_prefix("W/") {
        if rest.len() >= 2 && rest.starts_with('"') && rest.ends_with('"') {
            return Ok(R2EtagMatch::Weak {
                value: rest[1..rest.len() - 1].to_string(),
            });
        }
        return Err("weak etag must be quoted (W/\"v\")".to_string());
    }
    if s.starts_with('"') || s.ends_with('"') {
        return Err("etag must not be wrapped in quotes".to_string());
    }
    Ok(R2EtagMatch::Strong {
        value: s.to_string(),
    })
}

/// Lenient boundary form for `etagMatches`/`etagDoesNotMatch`:
/// tenant code sends strings (CF `onlyIf` API), internal callers may
/// send normalized `{kind,value}` objects.
fn de_etag_match_list<'de, D>(d: D) -> Result<Vec<R2EtagMatch>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Item {
        S(String),
        M(R2EtagMatch),
    }
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        S(String),
        V(Vec<Item>),
    }
    Ok(match Raw::deserialize(d)? {
        Raw::S(s) => vec![parse_etag_token(&s).map_err(serde::de::Error::custom)?],
        Raw::V(items) => {
            if items.len() > MAX_ETAG_LIST {
                return Err(serde::de::Error::custom("etag list too long"));
            }
            items
                .into_iter()
                .map(|i| match i {
                    Item::S(s) => parse_etag_token(&s).map_err(serde::de::Error::custom),
                    Item::M(m) => Ok(m),
                })
                .collect::<Result<_, _>>()?
        }
    })
}

fn compare_time(millis: i64, seconds: bool) -> i64 {
    if seconds {
        millis.div_euclid(1000)
    } else {
        millis
    }
}

impl R2Condition {
    /// Whether a missing object satisfies this condition (used by put).
    pub fn matches_missing(&self) -> bool {
        self.etag_matches.is_empty()
            && (self.http_headers && !self.etag_matches.is_empty()
                || self.uploaded_before.is_none())
            && (self.http_headers && !self.etag_does_not_match.is_empty()
                || self.uploaded_after.is_none())
    }

    /// Whether one stored object satisfies this condition.
    pub fn matches_object(&self, etag: &str, uploaded: i64) -> bool {
        let uploaded = compare_time(uploaded, self.seconds_granularity);
        (self.etag_matches.is_empty() || self.etag_matches.iter().any(|m| m.strong_matches(etag)))
            && self
                .etag_does_not_match
                .iter()
                .all(|m| !m.weak_matches(etag))
            && (self.http_headers && !self.etag_matches.is_empty()
                || self
                    .uploaded_before
                    .is_none_or(|t| uploaded <= compare_time(t, self.seconds_granularity)))
            && (self.http_headers && !self.etag_does_not_match.is_empty()
                || self
                    .uploaded_after
                    .is_none_or(|t| uploaded > compare_time(t, self.seconds_granularity)))
    }
}

/// Worker API storage class names.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum R2StorageClass {
    /// Default hot class.
    #[default]
    Standard,
    /// Infrequent-access class; no billing simulation.
    InfrequentAccess,
}

impl R2StorageClass {
    /// Parse a Worker API storage-class token.
    pub fn parse(value: &str) -> Result<Self, ProviderError> {
        match value {
            "Standard" => Ok(Self::Standard),
            "InfrequentAccess" => Ok(Self::InfrequentAccess),
            _ => Err(ProviderError::R2InvalidOptions),
        }
    }

    /// Tenant-visible class name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "Standard",
            Self::InfrequentAccess => "InfrequentAccess",
        }
    }
}

/// Exactly one caller-supplied checksum algorithm.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum R2ChecksumAlgorithm {
    /// 16-byte MD5.
    Md5([u8; 16]),
    /// 20-byte SHA-1.
    Sha1([u8; 20]),
    /// 32-byte SHA-256.
    Sha256([u8; 32]),
    /// 48-byte SHA-384.
    Sha384([u8; 48]),
    /// 64-byte SHA-512.
    Sha512([u8; 64]),
}

/// All checksums computed over staged object bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct R2ComputedChecksums {
    /// MD5.
    pub md5: [u8; 16],
    /// SHA-1.
    pub sha1: [u8; 20],
    /// SHA-256.
    pub sha256: [u8; 32],
    /// SHA-384.
    pub sha384: [u8; 48],
    /// SHA-512.
    pub sha512: [u8; 64],
}

impl R2ComputedChecksums {
    /// Which checksums the wire metadata exposes given the caller's
    /// requested algorithm (MD5 is always exposed).
    pub(crate) fn exposed(&self, requested: Option<&R2ChecksumAlgorithm>) -> R2Checksums {
        let mut checksums = R2Checksums {
            md5: Some(hex::encode(self.md5)),
            ..R2Checksums::default()
        };
        match requested {
            None | Some(R2ChecksumAlgorithm::Md5(_)) => {}
            Some(R2ChecksumAlgorithm::Sha1(_)) => checksums.sha1 = Some(hex::encode(self.sha1)),
            Some(R2ChecksumAlgorithm::Sha256(_)) => {
                checksums.sha256 = Some(hex::encode(self.sha256))
            }
            Some(R2ChecksumAlgorithm::Sha384(_)) => {
                checksums.sha384 = Some(hex::encode(self.sha384))
            }
            Some(R2ChecksumAlgorithm::Sha512(_)) => {
                checksums.sha512 = Some(hex::encode(self.sha512))
            }
        }
        checksums
    }

    /// Compare one caller-supplied checksum against staged bytes.
    pub fn matches(&self, expected: &R2ChecksumAlgorithm) -> bool {
        match expected {
            R2ChecksumAlgorithm::Md5(v) => self.md5 == *v,
            R2ChecksumAlgorithm::Sha1(v) => self.sha1 == *v,
            R2ChecksumAlgorithm::Sha256(v) => self.sha256 == *v,
            R2ChecksumAlgorithm::Sha384(v) => self.sha384 == *v,
            R2ChecksumAlgorithm::Sha512(v) => self.sha512 == *v,
        }
    }
}

/// Compute every pinned checksum for an in-memory buffer.
pub fn hash_bytes(bytes: &[u8]) -> R2ComputedChecksums {
    use sha2::Digest;
    R2ComputedChecksums {
        md5: md5::compute(bytes).0,
        sha1: sha1::Sha1::digest(bytes).into(),
        sha256: sha2::Sha256::digest(bytes).into(),
        sha384: sha2::Sha384::digest(bytes).into(),
        sha512: sha2::Sha512::digest(bytes).into(),
    }
}

/// Hex checksums returned on object metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2Checksums {
    /// MD5 hex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    /// SHA-1 hex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha1: Option<String>,
    /// SHA-256 hex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// SHA-384 hex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha384: Option<String>,
    /// SHA-512 hex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha512: Option<String>,
}

/// One completed part returned to the tenant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2UploadedPart {
    /// Part number `1..=10000`.
    pub part_number: i32,
    /// Provider part ETag.
    pub etag: String,
}

/// Tenant-visible object metadata DTO.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2ObjectMetadata {
    /// Exact unnormalized tenant key.
    pub key: String,
    /// Provider `UUIDv7` version.
    pub version: String,
    /// Full object size in bytes.
    pub size: u64,
    /// Opaque unquoted provider ETag.
    pub etag: String,
    /// Quoted RFC form of `etag`.
    pub http_etag: String,
    /// Upload time, unix ms.
    pub uploaded: i64,
    /// HTTP metadata; omitted from unincluded list entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_metadata: Option<R2HttpMetadata>,
    /// Custom metadata; omitted from unincluded list entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_metadata: Option<BTreeMap<String, String>>,
    /// Returned byte range, when a range request resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<R2Range>,
    /// Stored checksums (lowercase hex).
    pub checksums: R2Checksums,
    /// Worker API storage class.
    pub storage_class: String,
    /// MD5 of the SSE-C key when encrypted (never emitted — v1 rejects SSE-C).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssec_key_md5: Option<String>,
}

/// Provider list result — objects carry full metadata (include mask is
/// applied at the response layer); `next_after` is the opaque resume key.
#[derive(Clone, Debug)]
pub struct R2ListPage {
    /// Object entries in order (full metadata).
    pub objects: Vec<R2ObjectMetadata>,
    /// Collapsed delimiter prefixes in order.
    pub delimited_prefixes: Vec<String>,
    /// Resume key when truncated (last raw key scanned).
    pub next_after: Option<String>,
}

/// One logical object or collapsed delimiter prefix in list order.
/// The metadata variant is intentionally unboxed — list pages already
/// allocate a `Vec` per entry, and boxing would only add an indirection
/// to the overwhelmingly common variant.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum R2ListEntry {
    /// Concrete object metadata.
    Object(R2ObjectMetadata),
    /// Common prefix ending in the delimiter.
    DelimitedPrefix(String),
}

/// Put options decoded from the wire header.
#[derive(Clone, Debug, Default)]
pub struct R2PutOptions {
    /// HTTP metadata to store.
    pub http_metadata: R2HttpMetadata,
    /// Custom metadata (`<= 4 KiB` canonical JSON).
    pub custom_metadata: BTreeMap<String, String>,
    /// Conditional write precondition.
    pub only_if: Option<R2Condition>,
    /// Caller-supplied checksum to verify.
    pub checksum: Option<R2ChecksumAlgorithm>,
    /// Storage class.
    pub storage_class: R2StorageClass,
}

/// Minimum size for every part except the final one (upstream R2/S3).
pub const R2_MULTIPART_MIN_PART_BYTES: u64 = 5 * 1024 * 1024;
/// Valid `partNumber` range is `1..=R2_MULTIPART_MAX_PART_NUMBER`.
pub const R2_MULTIPART_MAX_PART_NUMBER: i32 = 10_000;
/// Maximum number of parts a complete request may list.
pub const R2_MULTIPART_MAX_PARTS: usize = 10_000;
/// Maximum multipart uploads tracked per `(tenant, bucket)` — caps
/// unfinished-upload accumulation; excess create returns `R2_OVERLOADED`.
/// Default per-tenant (cross-bucket) active-upload cap — deliberately
/// small for standalone: 64 unfinished uploads × up to
/// `max_object_bytes` staged bytes stays bounded; the fs backend
/// exposes `[r2] maxActiveUploads` / `stagingMaxBytesPerTenant` to
/// widen it explicitly.
pub const R2_MULTIPART_MAX_ACTIVE_UPLOADS: usize = 64;
/// Default expiry for an unfinished multipart upload (upstream ~7d).
pub const R2_MULTIPART_DEFAULT_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// Multipart create options (normalized; HTTP/custom metadata stored on
/// completion, storage class applied to the final object).

#[derive(Clone, Debug, Default)]
pub struct R2MultipartCreateOptions {
    /// HTTP metadata to store on completion.
    pub http_metadata: R2HttpMetadata,
    /// Custom metadata.
    pub custom_metadata: BTreeMap<String, String>,
    /// Storage class.
    pub storage_class: R2StorageClass,
}

/// Multipart create result (trait placeholder).
#[derive(Clone, Debug)]
pub struct MultipartCreateResult {
    /// Opaque upload id.
    pub upload_id: String,
    /// Backend-decided expiry (unix ms) — the server signs the tenant
    /// token with exactly this deadline so the opaque `uploadId` never
    /// outlives or underlives the backend upload it names (W5-r2).
    pub expires_at_ms: i64,
}

/// Signed cursor payload (`{"v","resourceId","generation",
/// "prefixSha256","delimiterSha256","includeMask","startAfterSha256",
/// "afterKey","expiresAtMs"}`) — camelCase to match upstream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct R2CursorPayload {
    /// Cursor format version (`1`).
    pub v: u8,
    /// sha256 hex over `tenant\0bucket` — binds the cursor to its scope.
    pub resource_id: String,
    /// Snapshot revision the cursor was issued at (observability; NOT a
    /// gate — snapshot renewals must not invalidate cursors).
    pub generation: u64,
    /// sha256 hex of `prefix` (`""` → hash of empty).
    pub prefix_sha256: String,
    /// sha256 hex of `delimiter` (`""` when absent).
    pub delimiter_sha256: String,
    /// `["httpMetadata","customMetadata"]` bit mask.
    pub include_mask: u8,
    /// sha256 hex of `startAfter` (`""` when absent).
    pub start_after_sha256: String,
    /// Opaque resume key (last raw key scanned).
    pub after_key: Option<String>,
    /// Expiry, unix ms.
    pub expires_at_ms: i64,
}

/// Validate a tenant object key (no normalization); `<= 1024` bytes.
/// Returns the key unchanged on success.
pub fn validate_object_key(key: &str) -> Result<&str, ProviderError> {
    if key.len() > R2_MAX_KEY_BYTES {
        return Err(ProviderError::R2KeyTooLarge);
    }
    // T12/G7: `.`/`..` path segments are REJECTED, not normalized.
    // `reqwest::Url::parse` silently collapses them (`a/./b` → `a/b`),
    // which would make the signed path and the wire path diverge.
    // Stricter than Cloudflare R2 (which stores them verbatim — its API
    // does not route keys through a URL parser); documented divergence.
    if key.split('/').any(|seg| seg == "." || seg == "..") {
        return Err(ProviderError::R2InvalidOptions);
    }
    Ok(key)
}

/// Reject malformed range shapes (before size is known).
pub fn validate_range_shape(range: R2Range) -> Result<(), ProviderError> {
    match (range.offset, range.length, range.suffix) {
        (Some(_), Some(length), None) if length > 0 => Ok(()),
        (Some(_), None, None) => Ok(()),
        (None, Some(length), None) if length > 0 => Ok(()),
        (None, None, Some(suffix)) if suffix > 0 => Ok(()),
        _ => Err(ProviderError::R2InvalidOptions),
    }
}

/// Resolve a shape-valid range against `size` into `(start, end)`
/// inclusive. Empty objects and out-of-bounds shapes are invalid.
pub fn resolve_range(range: R2Range, size: u64) -> Result<(u64, u64), ProviderError> {
    if size == 0 {
        return Err(ProviderError::R2InvalidOptions);
    }
    match (range.offset, range.length, range.suffix) {
        (Some(start), Some(length), None) if length > 0 && start < size => {
            Ok((start, start.saturating_add(length - 1).min(size - 1)))
        }
        (Some(start), None, None) if start < size => Ok((start, size - 1)),
        (None, Some(length), None) if length > 0 => Ok((0, length.min(size) - 1)),
        (None, None, Some(suffix)) if suffix > 0 => Ok((size.saturating_sub(suffix), size - 1)),
        _ => Err(ProviderError::R2InvalidOptions),
    }
}

/// Canonical custom metadata: JSON object `<= 4 KiB`, keys ordered.
pub fn canonical_custom_metadata(
    metadata: &BTreeMap<String, String>,
) -> Result<Vec<u8>, ProviderError> {
    let bytes = serde_json::to_vec(metadata).map_err(|_| ProviderError::R2InvalidOptions)?;
    if bytes.len() > R2_MAX_CUSTOM_METADATA_JSON_BYTES {
        return Err(ProviderError::R2MetadataTooLarge);
    }
    Ok(bytes)
}

/// Display key used for delimiter-collapse dedupe (upstream
/// `list_entry_key`): the object key or the delimited prefix.
pub fn list_entry_key(entry: &R2ListEntry) -> &str {
    match entry {
        R2ListEntry::Object(meta) => &meta.key,
        R2ListEntry::DelimitedPrefix(prefix) => prefix,
    }
}

// --------------------------------------------------------- request DTOs

/// `{"key": ...}` — head request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRequest {
    /// Object key (`<= 1024` bytes).
    pub key: String,
}

/// `{"key": ..., "options": {range?, onlyIf?, ssecKey?}}` — get body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    /// Object key.
    pub key: String,
    /// Read options.
    #[serde(default)]
    pub options: GetOptions,
}

/// Get options (`range`, `onlyIf`, `ssecKey`).
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetOptions {
    /// Byte range.
    pub range: Option<R2Range>,
    /// Read precondition.
    pub only_if: Option<R2Condition>,
    /// SSE-C key hex (64 chars) — rejected: unsupported in v1.
    pub ssec_key: Option<String>,
}

/// Put frame JSON header (`<u32 len><json><value bytes>`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutHeader {
    /// Object key.
    pub key: String,
    /// Write options.
    #[serde(default)]
    pub options: PutWireOptions,
}

/// Put wire options (`onlyIf`, `httpMetadata`, `customMetadata`,
/// `checksum`, `storageClass`, `ssecKey`).
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PutWireOptions {
    /// Write precondition.
    #[serde(default)]
    pub only_if: Option<R2Condition>,
    /// HTTP metadata to store.
    #[serde(default)]
    pub http_metadata: R2HttpMetadata,
    /// Custom metadata.
    #[serde(default)]
    pub custom_metadata: BTreeMap<String, String>,
    /// Caller checksum `{algorithm, hex}`.
    pub checksum: Option<ChecksumWire>,
    /// `Standard` | `InfrequentAccess`.
    pub storage_class: Option<String>,
    /// SSE-C key hex — rejected: unsupported in v1.
    pub ssec_key: Option<String>,
}

/// `{"algorithm": "md5|sha1|sha256|sha384|sha512", "hex": "..."}`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChecksumWire {
    /// Algorithm name.
    pub algorithm: String,
    /// Lowercase hex digest.
    pub hex: String,
}

impl PutWireOptions {
    fn validate(&self) -> Result<(), ProviderError> {
        if let Some(class) = self.storage_class.as_deref() {
            R2StorageClass::parse(class)?;
        }
        // SSE-C is unsupported in v1 — reject any supplied key material
        // rather than silently treating the write as unencrypted.
        if self.ssec_key.is_some() {
            return Err(ProviderError::R2SsecInvalid);
        }
        Ok(())
    }
}

fn parse_checksum(value: &ChecksumWire) -> Result<R2ChecksumAlgorithm, ProviderError> {
    let bytes = hex::decode(&value.hex).map_err(|_| ProviderError::R2InvalidOptions)?;
    match value.algorithm.as_str() {
        "md5" => Ok(R2ChecksumAlgorithm::Md5(
            bytes
                .try_into()
                .map_err(|_| ProviderError::R2InvalidOptions)?,
        )),
        "sha1" => Ok(R2ChecksumAlgorithm::Sha1(
            bytes
                .try_into()
                .map_err(|_| ProviderError::R2InvalidOptions)?,
        )),
        "sha256" => Ok(R2ChecksumAlgorithm::Sha256(
            bytes
                .try_into()
                .map_err(|_| ProviderError::R2InvalidOptions)?,
        )),
        "sha384" => Ok(R2ChecksumAlgorithm::Sha384(
            bytes
                .try_into()
                .map_err(|_| ProviderError::R2InvalidOptions)?,
        )),
        "sha512" => Ok(R2ChecksumAlgorithm::Sha512(
            bytes
                .try_into()
                .map_err(|_| ProviderError::R2InvalidOptions)?,
        )),
        _ => Err(ProviderError::R2InvalidOptions),
    }
}

impl TryFrom<PutWireOptions> for R2PutOptions {
    type Error = ProviderError;

    fn try_from(value: PutWireOptions) -> Result<Self, Self::Error> {
        value.validate()?;
        Ok(Self {
            http_metadata: value.http_metadata,
            custom_metadata: value.custom_metadata,
            only_if: value.only_if,
            checksum: value.checksum.as_ref().map(parse_checksum).transpose()?,
            storage_class: value
                .storage_class
                .as_deref()
                .map(R2StorageClass::parse)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

/// `{"keys": [...]}` — delete request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRequest {
    /// Keys (`1..=1000`).
    pub keys: Vec<String>,
}

/// List request body.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListRequest {
    /// Key prefix (`""` default).
    #[serde(default)]
    pub prefix: String,
    /// Delimiter (empty string invalid).
    pub delimiter: Option<String>,
    /// Signed opaque cursor.
    pub cursor: Option<String>,
    /// Keyset start (`>`).
    pub start_after: Option<String>,
    /// Page size `0..=1000` (0 → metadata-only probe).
    #[serde(default = "default_list_limit")]
    pub limit: u16,
    /// `["httpMetadata", "customMetadata"]` subset.
    #[serde(default)]
    pub include: Vec<String>,
}

const fn default_list_limit() -> u16 {
    crate::limits::R2_MAX_LIST_LIMIT
}

/// `include` names → bit mask (`httpMetadata` = 1, `customMetadata` = 2).
pub fn include_mask(include: &[String]) -> Result<u8, ProviderError> {
    let mut mask = 0u8;
    for item in include {
        mask |= match item.as_str() {
            "httpMetadata" => 1,
            "customMetadata" => 2,
            _ => return Err(ProviderError::R2InvalidOptions),
        };
    }
    Ok(mask)
}

/// Strip `httpMetadata`/`customMetadata` per the include mask, then
/// serialize for the list response.
pub fn list_object_json(mut metadata: R2ObjectMetadata, mask: u8) -> serde_json::Value {
    if mask & 1 == 0 {
        metadata.http_metadata = None;
    }
    if mask & 2 == 0 {
        metadata.custom_metadata = None;
    }
    serde_json::to_value(metadata).unwrap_or(serde_json::Value::Null)
}

/// `createMultipartUpload` request body — upstream `{key, options}`.
/// `ssecKey` presence is rejected (SSE-C unsupported), matching `put`.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateMultipartRequest {
    /// Object key.
    pub key: String,
    /// Create options.
    #[serde(default)]
    pub options: MultipartCreateWireOptions,
}

/// Wire `options` for `createMultipartUpload`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MultipartCreateWireOptions {
    /// HTTP metadata stored on completion.
    pub http_metadata: Option<R2HttpMetadata>,
    /// Custom metadata.
    pub custom_metadata: Option<BTreeMap<String, String>>,
    /// Storage class.
    pub storage_class: Option<String>,
    /// SSE-C key — present → `R2_SSEC_INVALID`.
    pub ssec_key: Option<String>,
}

/// `uploadPart` frame header — body bytes follow the 4-byte JSON header.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UploadPartHeader {
    /// Object key.
    pub key: String,
    /// Signed opaque upload id issued at create.
    pub upload_id: String,
    /// Part number `1..=10000`.
    pub part_number: i64,
    /// SSE-C key — present → `R2_SSEC_INVALID`.
    pub ssec_key: Option<String>,
}

/// `completeMultipartUpload` request body.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompleteMultipartRequest {
    /// Object key.
    pub key: String,
    /// Signed opaque upload id.
    pub upload_id: String,
    /// Parts in ascending `partNumber` order, etags as returned by
    /// `uploadPart`.
    pub parts: Vec<R2UploadedPart>,
}

/// `abortMultipartUpload` request body.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AbortMultipartRequest {
    /// Object key.
    pub key: String,
    /// Signed opaque upload id.
    pub upload_id: String,
}

/// Normalize `MultipartCreateWireOptions` into the provider-facing type;
/// rejects `ssecKey`.
impl MultipartCreateWireOptions {
    /// Convert to [`R2MultipartCreateOptions`]; `ssecKey` →
    /// `R2_SSEC_INVALID`, unknown storage class → `R2_INVALID_OPTIONS`.
    pub fn into_options(self) -> Result<R2MultipartCreateOptions, ProviderError> {
        if self.ssec_key.is_some() {
            return Err(ProviderError::R2SsecInvalid);
        }
        Ok(R2MultipartCreateOptions {
            http_metadata: self.http_metadata.unwrap_or_default(),
            custom_metadata: self.custom_metadata.unwrap_or_default(),
            storage_class: self
                .storage_class
                .as_deref()
                .map(R2StorageClass::parse)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

/// `partNumber` range check — `1..=10000` else `R2_MULTIPART_INVALID`.
pub fn validate_part_number(n: i64) -> Result<i32, ProviderError> {
    if !(1..=R2_MULTIPART_MAX_PART_NUMBER as i64).contains(&n) {
        return Err(ProviderError::R2MultipartInvalid);
    }
    Ok(n as i32)
}

/// Complete-request part list: non-empty, `<= 10000`, strictly
/// ascending `partNumber` (implies uniqueness), each in range, etag
/// non-empty.
pub fn validate_complete_parts(parts: &[R2UploadedPart]) -> Result<(), ProviderError> {
    if parts.is_empty() || parts.len() > R2_MULTIPART_MAX_PARTS {
        return Err(ProviderError::R2MultipartInvalid);
    }
    let mut prev = 0i32;
    for p in parts {
        validate_part_number(p.part_number as i64)?;
        if p.part_number <= prev || p.etag.trim_matches('"').is_empty() {
            return Err(ProviderError::R2MultipartInvalid);
        }
        prev = p.part_number;
    }
    Ok(())
}

/// Compare a caller-supplied etag with a stored one, tolerating
/// surrounding quotes/case differences.
pub fn etag_matches(supplied: &str, stored: &str) -> bool {
    supplied
        .trim_matches('"')
        .eq_ignore_ascii_case(stored.trim_matches('"'))
}

/// Multipart composite etag: `md5(concat(part md5 bytes))-N` hex —
/// the upstream R2/S3 format. `md5s` are 32-hex strings.
pub fn multipart_etag(md5s: &[String]) -> Result<String, ProviderError> {
    if md5s.is_empty() {
        return Err(ProviderError::R2MultipartInvalid);
    }
    let mut concat = Vec::with_capacity(md5s.len() * 16);
    for m in md5s {
        let raw =
            hex::decode(m.trim_matches('"')).map_err(|_| ProviderError::R2MultipartInvalid)?;
        if raw.len() != 16 {
            return Err(ProviderError::R2MultipartInvalid);
        }
        concat.extend_from_slice(&raw);
    }
    Ok(format!("{:x}-{}", md5::compute(&concat), md5s.len()))
}

/// `md5` hex of raw body bytes — the per-part etag.
pub fn part_md5_hex(body: &[u8]) -> String {
    format!("{:x}", md5::compute(body))
}

/// Upstream minimum-part-size rule: with 2+ listed parts every part
/// except the LAST listed one must be `>= 5 MiB` (single-part uploads
/// are exempt — any size allowed). `sizes` is indexed by list position.
pub fn check_min_part_sizes(sizes: &[u64]) -> Result<(), ProviderError> {
    if sizes.len() <= 1 {
        return Ok(());
    }
    for size in &sizes[..sizes.len() - 1] {
        if *size < R2_MULTIPART_MIN_PART_BYTES {
            return Err(ProviderError::R2MultipartInvalid);
        }
    }
    Ok(())
}

#[cfg(test)]
mod g7_tests {
    use super::*;

    #[test]
    fn object_key_rejects_dot_segments() {
        // T12/G7: `.`/`..` segments rejected outright — reqwest would
        // normalize them away, diverging signed vs wire path.
        for bad in [".", "..", "a/./b", "a/../b", "./x", "x/..", "a/b/../c"] {
            assert_eq!(
                validate_object_key(bad),
                Err(ProviderError::R2InvalidOptions),
                "accepted {bad:?}"
            );
        }
        for good in ["a/b/c", "file.txt", "a..b", "..x", "x.", "a/b"] {
            assert!(validate_object_key(good).is_ok(), "rejected {good:?}");
        }
        assert_eq!(
            validate_object_key(&"k".repeat(R2_MAX_KEY_BYTES + 1)),
            Err(ProviderError::R2KeyTooLarge)
        );
    }

    fn de_list(v: serde_json::Value) -> Result<Vec<R2EtagMatch>, String> {
        de_etag_match_list(v).map_err(|e| e.to_string())
    }

    #[test]
    fn etag_match_list_cf_and_wire_forms() {
        // CF tenant-facing string forms.
        assert_eq!(
            de_list(serde_json::json!("e1")).unwrap(),
            vec![R2EtagMatch::Strong { value: "e1".into() }]
        );
        assert_eq!(
            de_list(serde_json::json!("*")).unwrap(),
            vec![R2EtagMatch::Wildcard]
        );
        assert_eq!(
            de_list(serde_json::json!("W/\"v9\"")).unwrap(),
            vec![R2EtagMatch::Weak { value: "v9".into() }]
        );
        // Internal normalized `{kind,value}` objects stay accepted.
        assert_eq!(
            de_list(serde_json::json!([{"kind": "strong", "value": "a"}])).unwrap(),
            vec![R2EtagMatch::Strong { value: "a".into() }]
        );
        assert_eq!(
            de_list(serde_json::json!([{"kind": "wildcard"}])).unwrap(),
            vec![R2EtagMatch::Wildcard]
        );
        // Mixed forms (string + normalized) in one list.
        assert_eq!(
            de_list(serde_json::json!(["x", {"kind": "weak", "value": "y"}])).unwrap(),
            vec![
                R2EtagMatch::Strong { value: "x".into() },
                R2EtagMatch::Weak { value: "y".into() }
            ]
        );
        // Empty list = no condition.
        assert_eq!(de_list(serde_json::json!([])).unwrap(), vec![]);
    }

    #[test]
    fn etag_match_list_rejects_bad_forms() {
        // Quoted strong etags are a tenant-side error per CF semantics
        // ("Conditional ETag should not be wrapped in quotes").
        // Note: `W/""` is syntactically valid RFC 7232 (empty weak
        // validator) and accepted — it simply never matches an object.
        for bad in [
            "\"q\"",
            "\"unclosed",
            "opened\"",
            "",
            "W/x",
            "W/",
            "W\"",
            "WW/\"v\"",
        ] {
            assert!(de_list(serde_json::json!(bad)).is_err(), "accepted {bad:?}");
        }
        // Overlong token.
        let big = "e".repeat(MAX_ETAG_LEN + 1);
        assert!(de_list(serde_json::json!(big)).is_err());
        // Overlong list.
        let many = vec!["e"; MAX_ETAG_LIST + 1];
        assert!(de_list(serde_json::json!(many)).is_err());
        // Wrong wire type entirely.
        assert!(de_list(serde_json::json!(42)).is_err());
        assert!(de_list(serde_json::json!([42])).is_err());
    }

    #[test]
    fn condition_uploaded_fields_take_unix_ms() {
        // Facade converts tenant `Date` → unix ms (`dateMillis`); the
        // wire fields accept that numeric form on camelCase names.
        let c: R2Condition = serde_json::from_value(serde_json::json!({
            "etagMatches": "e1",
            "uploadedBefore": 1_762_000_000_000i64,
            "uploadedAfter": 1_760_000_000_000i64
        }))
        .unwrap();
        assert_eq!(c.uploaded_before, Some(1_762_000_000_000));
        assert_eq!(c.uploaded_after, Some(1_760_000_000_000));
        // Non-numeric timestamps (e.g. ISO strings) are rejected at the
        // typed boundary — the facade owns the Date→ms conversion.
        assert!(serde_json::from_value::<R2Condition>(serde_json::json!({
            "uploadedBefore": "2026-01-01T00:00:00Z"
        }))
        .is_err());
    }
}
