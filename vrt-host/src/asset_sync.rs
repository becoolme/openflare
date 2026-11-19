//! Static-assets materialization (T21): bring every ASSETS binding's
//! manifest + objects into the provider-shared content-addressed store
//! BEFORE the tenant generation launches.
//!
//! Two sources:
//! - [`HttpAssetSource`] — platform cells pull `GET {base}/vrt/assets/
//!   manifests/{digest}` and `…/objects/{digest}` with the scoped cell
//!   token (same trust model as [`crate::bundle_http`]).
//! - [`AbsentAssetSource`] — standalone: `vrt deploy --assets` writes
//!   the same store layout directly; the sync only VERIFIES presence —
//!   missing bytes fail closed (`AssembleError::Artifact`), the old
//!   generation keeps serving (the render-inputs-unavailable pattern).

use crate::artifact::ArtifactError;
use std::sync::Arc;
use vrt_provider::assets::{AssetStore, MAX_ASSET_OBJECT_BYTES};

/// Where an asset object/manifest comes from when the local store
/// doesn't have it yet.
pub trait AssetSource: Send + Sync {
    /// Fetch manifest JSON bytes by digest (`Ok(None)` = absent).
    fn fetch_manifest(&self, digest: &str) -> Result<Option<Vec<u8>>, ArtifactError>;
    /// Fetch one object by its sha256 (`Ok(None)` = absent).
    fn fetch_object(&self, digest: &str) -> Result<Option<Vec<u8>>, ArtifactError>;
}

/// Standalone source: deploy pre-materializes, so anything missing is
/// an error — never fetched, never guessed.
pub struct AbsentAssetSource;

impl AssetSource for AbsentAssetSource {
    fn fetch_manifest(&self, _digest: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        Ok(None)
    }
    fn fetch_object(&self, _digest: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        Ok(None)
    }
}

/// Platform pull: `GET {base}/vrt/assets/manifests/{d}` /
/// `{base}/vrt/assets/objects/{d}` — same loopback-free (https or
/// loopback only) + bounded-retry contract as `HttpBundleSource`; the
/// control plane derives ownership from signed publications.
pub struct HttpAssetSource {
    client: reqwest::Client,
    base_url: String,
    token: String,
    max_bytes: usize,
    request_timeout: std::time::Duration,
    deadline: std::time::Duration,
    max_attempts: u32,
}

impl std::fmt::Debug for HttpAssetSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpAssetSource")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl HttpAssetSource {
    pub fn new(
        base_url: String,
        token: String,
        request_timeout: std::time::Duration,
        deadline: std::time::Duration,
        max_attempts: u32,
    ) -> Result<Self, ArtifactError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ArtifactError::Unavailable(format!("client: {e}")))?;
        Ok(Self {
            client,
            base_url,
            token,
            max_bytes: MAX_ASSET_OBJECT_BYTES as usize,
            request_timeout,
            deadline,
            max_attempts,
        })
    }

    async fn fetch_bounded(
        &self,
        kind: &str,
        digest: &str,
    ) -> Result<Option<Vec<u8>>, ArtifactError> {
        let deadline = std::time::Instant::now() + self.deadline;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if std::time::Instant::now() >= deadline {
                return Err(ArtifactError::Unavailable("deadline exceeded".into()));
            }
            let url = format!("{}/vrt/assets/{}/{}", self.base_url, kind, digest);
            let send = self
                .client
                .get(&url)
                .header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", self.token),
                )
                .timeout(self.request_timeout)
                .send()
                .await;
            match send {
                Ok(res) if res.status().is_success() => {
                    if let Some(len) = res.content_length() {
                        if len as usize > self.max_bytes {
                            return Err(ArtifactError::TooLarge);
                        }
                    }
                    let mut body = Vec::new();
                    use futures_util::StreamExt;
                    let mut stream = res.bytes_stream();
                    let mut failed = false;
                    while let Some(chunk) = stream.next().await {
                        let Ok(chunk) = chunk else {
                            failed = true;
                            break;
                        };
                        if body.len() + chunk.len() > self.max_bytes {
                            return Err(ArtifactError::TooLarge);
                        }
                        body.extend_from_slice(&chunk);
                    }
                    if failed {
                        if attempt >= self.max_attempts {
                            return Err(ArtifactError::Unavailable("stream failed".into()));
                        }
                        continue;
                    }
                    return Ok(Some(body));
                }
                Ok(res) if res.status().as_u16() == 404 => return Ok(None),
                Ok(res) if res.status().is_server_error() || res.status().as_u16() == 429 => {}
                Ok(_) => return Err(ArtifactError::Unavailable("unauthorized".into())),
                Err(e) if e.is_status() => {
                    let s = e.status().unwrap_or_default();
                    if s.as_u16() == 404 {
                        return Ok(None);
                    }
                    if s.is_client_error() && s.as_u16() != 429 {
                        return Err(ArtifactError::Unavailable("unauthorized".into()));
                    }
                }
                Err(_) => {}
            }
            if attempt >= self.max_attempts {
                return Err(ArtifactError::Unavailable("attempts exhausted".into()));
            }
            let backoff =
                std::time::Duration::from_millis(100u64 << attempt.saturating_sub(1).min(4))
                    .min(deadline.saturating_duration_since(std::time::Instant::now()));
            tokio::time::sleep(backoff).await;
        }
    }
}

fn run_blocking<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| h.block_on(fut))
        }
        Ok(_) => panic!("HttpAssetSource requires a multi-thread runtime or sync context"),
        Err(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("mini runtime")
                        .block_on(fut)
                })
                .join()
                .expect("fetch thread")
        }),
    }
}

impl AssetSource for HttpAssetSource {
    fn fetch_manifest(&self, digest: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        run_blocking(self.fetch_bounded("manifests", digest))
    }
    fn fetch_object(&self, digest: &str) -> Result<Option<Vec<u8>>, ArtifactError> {
        run_blocking(self.fetch_bounded("objects", digest))
    }
}

/// Materializer: ensure `<tenant>` owns a verified copy of every
/// asset the snapshot declares. Idempotent — already-verified entries
/// are skipped; a poisoned local entry fails closed (Err → old
/// generation keeps serving).
pub struct AssetSync {
    store: AssetStore,
    source: Arc<dyn AssetSource>,
}

impl AssetSync {
    pub fn new(store: AssetStore, source: Arc<dyn AssetSource>) -> Self {
        Self { store, source }
    }

    /// Ensure `manifest_digest` + all its objects are present and
    /// verified for `tenant`. Fail closed on any gap.
    pub fn ensure(&self, tenant: &str, manifest_digest: &str) -> Result<(), ArtifactError> {
        let manifest = match self.store.manifest(tenant, manifest_digest) {
            Ok(Some(m)) => m,
            Ok(None) => {
                let bytes = self
                    .source
                    .fetch_manifest(manifest_digest)?
                    .ok_or(ArtifactError::Missing)?;
                self.store
                    .put_manifest_verified(tenant, manifest_digest, &bytes)
                    .map_err(|_| ArtifactError::BundleInvalid)?;
                self.store
                    .manifest(tenant, manifest_digest)
                    .map_err(ArtifactError::Unavailable)?
                    .ok_or(ArtifactError::Missing)?
            }
            Err(e) => return Err(ArtifactError::Unavailable(e)),
        };
        for file in &manifest.files {
            match self.store.object(tenant, file) {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    let bytes = self
                        .source
                        .fetch_object(&file.sha256)?
                        .ok_or(ArtifactError::Missing)?;
                    self.store
                        .put_object_verified(tenant, &file.sha256, &bytes)
                        .map_err(|_| ArtifactError::DigestMismatch)?;
                }
                Err(e) => return Err(ArtifactError::Unavailable(e)),
            }
        }
        Ok(())
    }

    /// RV-R53.5: one bounded mark-and-sweep pass. `keep` = tenant →
    /// manifest digests still referenced by applied specs.
    pub fn sweep(
        &self,
        keep: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    ) -> vrt_provider::assets::AssetGcReport {
        self.store.gc_sweep(
            keep,
            vrt_provider::assets::ASSET_GC_GRACE,
            vrt_provider::assets::ASSET_GC_MAX_PER_ROUND,
        )
    }
}
