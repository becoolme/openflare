//! Typed R2 provider boundary plus backends.
//!
//! - [`wire`]: binding-backend DTOs (`R2ObjectMetadata`, ranges,
//!   conditions, checksums, list cursor payload) matching upstream
//!   `open_compute_artifacts::r2_model` / `r2_protocol` shapes.
//! - [`memory`]: deterministic in-memory provider.
//! - [`s3`]: S3-compatible backend over `reqwest` + SigV4 (path-style).
//!
//! W5: multipart uploads are implemented by all three backends with
//! shared semantics — signed opaque upload ids bound to
//! (tenant, bucket, key), upstream part rules (>=5MiB non-final parts,
//! `partNumber` 1..=10000, ascending unique parts with matching etags,
//! same-partNumber re-upload replaces, terminal-state ops →
//! `R2_MULTIPART_INVALID`), and ~7-day expiry on unfinished uploads.

pub mod fs;
pub mod memory;
pub mod s3;
pub mod wire;

pub use memory::MemoryR2Provider;
pub use s3::S3R2Provider;
pub use wire::{
    canonical_custom_metadata, hash_bytes, resolve_range, validate_object_key,
    validate_range_shape, R2ChecksumAlgorithm, R2Checksums, R2ComputedChecksums, R2Condition,
    R2CursorPayload, R2EtagMatch, R2HttpMetadata, R2ListPage, R2ObjectMetadata, R2PutOptions,
    R2Range, R2StorageClass, R2UploadedPart,
};

use crate::context::BindingContext;
use crate::error::ProviderError;
use async_trait::async_trait;
use std::fmt::Debug;

/// `get()` result — mirrors upstream `R2GetResult`.
#[derive(Clone, Debug)]
pub enum R2GetResult {
    /// Object absent.
    Missing,
    /// `onlyIf` failed — metadata without body.
    Precondition(R2ObjectMetadata),
    /// Condition passed — metadata plus (possibly ranged) body bytes.
    Body(R2ObjectMetadata, Vec<u8>),
}

/// Typed R2 provider. Every call runs under a sealed [`BindingContext`]
/// and must honor `ctx.deadline()`; backend failures map to the `R2_*`
/// family of [`ProviderError`].
#[async_trait]
pub trait R2Provider: Send + Sync + Debug {
    /// Authoritative metadata for `key` (no body).
    async fn head(
        &self,
        ctx: &BindingContext,
        key: &str,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError>;

    /// Conditional, optionally ranged read. `range` is shape-validated by
    /// the caller; `only_if` is evaluated against the current object.
    async fn get(
        &self,
        ctx: &BindingContext,
        key: &str,
        range: Option<R2Range>,
        only_if: Option<&R2Condition>,
    ) -> Result<R2GetResult, ProviderError>;

    /// Atomic put. Returns `Some(metadata)` on commit, `None` when
    /// `options.only_if` rejected the write (facade maps to `204`).
    async fn put(
        &self,
        ctx: &BindingContext,
        key: &str,
        body: &[u8],
        options: &R2PutOptions,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError>;

    /// Delete up to 1000 logical keys (idempotent).
    async fn delete(&self, ctx: &BindingContext, keys: &[String]) -> Result<(), ProviderError>;

    /// Ordered prefix listing with delimiter collapsing; `after` is an
    /// opaque resume key (last raw key scanned) or the caller's
    /// `startAfter`. `limit >= 1`.
    async fn list(
        &self,
        ctx: &BindingContext,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
    ) -> Result<R2ListPage, ProviderError>;

    /// Create a multipart upload; `MultipartCreateResult.upload_id`
    /// is the BACKEND-INTERNAL id (S3 UploadId / fs staging id) — the
    /// server wraps it into the signed tenant-facing token.
    async fn create_multipart(
        &self,
        _ctx: &BindingContext,
        _key: &str,
        _options: &wire::R2MultipartCreateOptions,
    ) -> Result<wire::MultipartCreateResult, ProviderError> {
        Err(ProviderError::CapabilityUnsupported)
    }

    /// Upload one part (`upload_id` = verified internal id);
    /// re-uploading a `partNumber` replaces it.
    async fn upload_part(
        &self,
        _ctx: &BindingContext,
        _key: &str,
        _upload_id: &str,
        _part_number: i32,
        _body: &[u8],
    ) -> Result<wire::R2UploadedPart, ProviderError> {
        Err(ProviderError::CapabilityUnsupported)
    }

    /// Complete a multipart upload; `parts` must be ascending,
    /// unique, and each etag must match the stored part. Post-complete
    /// or -abort → `R2_MULTIPART_INVALID` (upstream `NoSuchUpload`).
    async fn complete_multipart(
        &self,
        _ctx: &BindingContext,
        _key: &str,
        _upload_id: &str,
        _parts: &[R2UploadedPart],
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        Err(ProviderError::CapabilityUnsupported)
    }

    /// Abort a multipart upload; removes staging. Missing/finished
    /// uploads → `R2_MULTIPART_INVALID`.
    async fn abort_multipart(
        &self,
        _ctx: &BindingContext,
        _key: &str,
        _upload_id: &str,
    ) -> Result<(), ProviderError> {
        Err(ProviderError::CapabilityUnsupported)
    }
}
