//! R2 data-plane surface for control-plane initiated object ops
//! (`R2_LIST/HEAD/GET/PUT/DELETE` cell-ops, CLI import/export).
//!
//! Mirrors `kv::KvDataBackend`: the caller names `(tenant_id, bucket_id)`
//! as plain identity — never a physical key/prefix. The `BindingContext`
//! stays sealed (`pub(crate)` constructor): this module is the ONLY place
//! that mints one for a data op, and only after the same id-shape
//! validation purge uses (`valid_purge_tenant` / `valid_r2_purge_id`), so a
//! forged identity can never widen the physical prefix
//! (`vrt/{tenant}/{bucket}/…` / `<root>/{tenant}/{bucket}/…`, derived
//! inside each backend with segment encoding).

use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;

use super::{R2GetResult, R2ListPage, R2ObjectMetadata, R2Provider, R2PutOptions, R2Range};
use crate::context::{BindingContext, BindingResource, Permissions};
use crate::error::ProviderError;
use crate::purge::{valid_purge_tenant, valid_r2_purge_id};

/// Namespace-scoped R2 data surface — every R2 backend gets it via the
/// blanket impl below, so S3 / fs / memory stay in lockstep.
#[async_trait]
pub trait R2DataBackend: Send + Sync + Debug {
    async fn data_head(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        budget: Duration,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError>;
    /// Whole-object read that never buffers more than `max_bytes + 1`
    /// bytes: a body longer than `max_bytes` is `R2ObjectTooLarge`, even
    /// when the object grew after the caller's HEAD.
    async fn data_get(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        max_bytes: u64,
        budget: Duration,
    ) -> Result<R2GetResult, ProviderError>;
    async fn data_put(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        body: &[u8],
        options: &R2PutOptions,
        budget: Duration,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError>;
    async fn data_delete(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        keys: &[String],
        budget: Duration,
    ) -> Result<(), ProviderError>;
    #[allow(clippy::too_many_arguments)]
    async fn data_list(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
        budget: Duration,
    ) -> Result<R2ListPage, ProviderError>;
}

/// Mint the sealed context for one data op. Fails closed on any id that
/// is not a registry-shaped tenant / bucket identity.
fn data_ctx(
    tenant_id: &str,
    bucket_id: &str,
    budget: Duration,
) -> Result<BindingContext, ProviderError> {
    if !valid_purge_tenant(tenant_id) || !valid_r2_purge_id(bucket_id) {
        return Err(ProviderError::R2InvalidOptions);
    }
    Ok(BindingContext::grant(
        tenant_id.to_string(),
        "data-op".into(),
        "data-op".into(),
        "data-op~R2".into(),
        "R2".into(),
        BindingResource::R2Bucket {
            name: bucket_id.to_string(),
        },
        Permissions {
            read: true,
            write: true,
        },
        0,
        0,
        tokio::time::Instant::now() + budget,
    ))
}

#[async_trait]
impl<T: R2Provider> R2DataBackend for T {
    async fn data_head(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        budget: Duration,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let ctx = data_ctx(tenant_id, bucket_id, budget)?;
        self.head(&ctx, super::validate_object_key(key)?).await
    }
    async fn data_get(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        max_bytes: u64,
        budget: Duration,
    ) -> Result<R2GetResult, ProviderError> {
        let ctx = data_ctx(tenant_id, bucket_id, budget)?;
        let key = super::validate_object_key(key)?;
        // Ask for at most max_bytes + 1: if that many come back the
        // object is larger than the cap and is rejected, never truncated.
        let bounded = R2Range {
            offset: Some(0),
            length: Some(max_bytes.saturating_add(1)),
            suffix: None,
        };
        let result = match self.get(&ctx, key, Some(bounded), None).await {
            // A range is invalid only on an empty object; an unranged read
            // of it is bounded by the size check below.
            Err(ProviderError::R2InvalidOptions) => self.get(&ctx, key, None, None).await?,
            other => other?,
        };
        match result {
            R2GetResult::Body(mut meta, body) => {
                if body.len() as u64 > max_bytes {
                    return Err(ProviderError::R2ObjectTooLarge);
                }
                // Whole-object semantics for the caller.
                meta.range = None;
                Ok(R2GetResult::Body(meta, body))
            }
            other => Ok(other),
        }
    }
    async fn data_put(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        key: &str,
        body: &[u8],
        options: &R2PutOptions,
        budget: Duration,
    ) -> Result<Option<R2ObjectMetadata>, ProviderError> {
        let ctx = data_ctx(tenant_id, bucket_id, budget)?;
        self.put(&ctx, super::validate_object_key(key)?, body, options)
            .await
    }
    async fn data_delete(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        keys: &[String],
        budget: Duration,
    ) -> Result<(), ProviderError> {
        let ctx = data_ctx(tenant_id, bucket_id, budget)?;
        for k in keys {
            super::validate_object_key(k)?;
        }
        self.delete(&ctx, keys).await
    }
    async fn data_list(
        &self,
        tenant_id: &str,
        bucket_id: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<&str>,
        limit: u16,
        budget: Duration,
    ) -> Result<R2ListPage, ProviderError> {
        let ctx = data_ctx(tenant_id, bucket_id, budget)?;
        self.list(&ctx, prefix, delimiter, after, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::r2::MemoryR2Provider;

    const T: &str = "team_abc";
    const B: &str = "vrt-0123456789";
    const BUDGET: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn rejects_forged_identity_shapes() {
        let p = MemoryR2Provider::new();
        for (t, b) in [
            ("../x", B),
            ("", B),
            (T, "bucket"),
            (T, "vrt-"),
            (T, "vrt-a/b"),
            (T, "vrt-a b"),
        ] {
            assert!(matches!(
                p.data_head(t, b, "k", BUDGET).await,
                Err(ProviderError::R2InvalidOptions)
            ));
        }
    }

    #[tokio::test]
    async fn rejects_traversal_keys_and_isolates_tenants() {
        let p = MemoryR2Provider::new();
        assert!(p.data_head(T, B, "a/../b", BUDGET).await.is_err());
        assert!(p
            .data_delete(T, B, &["ok".into(), "..".into()], BUDGET)
            .await
            .is_err());
        p.data_put(T, B, "k", b"v", &R2PutOptions::default(), BUDGET)
            .await
            .unwrap();
        assert!(p.data_head(T, B, "k", BUDGET).await.unwrap().is_some());
        assert!(p
            .data_head("team_other", B, "k", BUDGET)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn get_is_bounded_and_never_truncates() {
        let p = MemoryR2Provider::new();
        let opts = R2PutOptions::default();
        p.data_put(T, B, "empty", b"", &opts, BUDGET).await.unwrap();
        p.data_put(T, B, "exact", b"abcd", &opts, BUDGET).await.unwrap();
        p.data_put(T, B, "big", b"abcde", &opts, BUDGET).await.unwrap();
        match p.data_get(T, B, "empty", 4, BUDGET).await.unwrap() {
            R2GetResult::Body(m, body) => {
                assert!(body.is_empty());
                assert!(m.range.is_none());
            }
            other => panic!("{other:?}"),
        }
        match p.data_get(T, B, "exact", 4, BUDGET).await.unwrap() {
            R2GetResult::Body(m, body) => {
                assert_eq!(body, b"abcd");
                assert_eq!(m.size, 4);
                assert!(m.range.is_none());
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            p.data_get(T, B, "big", 4, BUDGET).await,
            Err(ProviderError::R2ObjectTooLarge)
        ));
        assert!(matches!(
            p.data_get(T, B, "missing", 4, BUDGET).await.unwrap(),
            R2GetResult::Missing
        ));
    }
}
