//! Workers Cache shared upper tier (A4) — the cross-pod layer.
//!
//! The lower tier (`wc::Store`) is pod-local. This module defines the
//! optional shared port and the semantics the ingress relies on:
//!
//! * read: lower miss → shared; a shared hit is backfilled into the lower
//!   tier so subsequent requests stay local;
//! * write: write-through (lower first, then shared); a shared failure never
//!   fails the response and never yields a fake HIT (a hit is always a real
//!   stored entry);
//! * purge: fans out to both tiers (worker / tag / prefix / everything).
//!
//! This slice ships the typed port, the expiry/classification helpers and an
//! in-memory implementation used by tests and the control-plane e2e. The
//! Redis/Kvrocks adapter and the `[cache.upper]` config land next; until then
//! production constructs `Store::new()` and behaves exactly as before.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;
use http::HeaderMap;

use crate::wc::{Entry, Lookup};

/// One shared-tier failure. The caller logs it and degrades to the lower
/// tier — it must never surface as a cache hit.
#[derive(Debug, Clone)]
pub struct SharedError(pub String);

impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SharedError {}

/// Cross-pod cache port. Implementations must be `Send + Sync` and bound
/// every operation with their own timeout (the caller never blocks on a
/// shared outage for the response path).
#[async_trait]
pub trait SharedTier: Send + Sync {
    /// Matching, still-live entry for `base`, if any.
    async fn get(
        &self,
        base: &str,
        req: &HeaderMap,
        now: SystemTime,
    ) -> Result<Option<Entry>, SharedError>;
    /// Store/replace one variant (write-through).
    async fn put(&self, base: &str, entry: &Entry) -> Result<(), SharedError>;
    async fn purge_worker(&self, tenant: &str, worker: &str) -> Result<usize, SharedError>;
    async fn purge_tags(&self, tenant: &str, tags: &[String]) -> Result<usize, SharedError>;
    async fn purge_prefix(&self, tenant: &str, prefix: &str) -> Result<usize, SharedError>;
    async fn purge_all(&self, tenant: &str) -> Result<usize, SharedError>;
}

/// Log a shared-tier failure with NO key material (paths can carry
/// sensitive query strings). The caller degrades to the lower tier.
pub fn log_shared_failure(op: &'static str, error: &SharedError) {
    tracing::warn!(
        op,
        error = %error,
        "workers cache shared tier failed; degrading to the lower tier"
    );
}

/// `Entry::matches`, shared so a non-local tier matches Vary exactly like the
/// lower store (all variants share a purge identity but not a hit identity).
pub fn entry_matches(entry: &Entry, req: &HeaderMap) -> bool {
    entry.vary.iter().all(|(name, want)| {
        let got = req
            .get(name.as_str())
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        got.as_deref() == want.as_deref()
    })
}

/// Classify a stored entry against `now` without touching a store (shared
/// hits have their original `stored_at`; the caller decides HIT/UPDATING/
/// STALE/EXPIRED exactly like the lower tier).
pub fn classify_entry(entry: Entry, now: SystemTime) -> Lookup {
    let age = now.duration_since(entry.stored_at).unwrap_or_default();
    let has_swr = entry.swr_for > std::time::Duration::ZERO;
    if entry.no_cache && !has_swr {
        return Lookup::Expired;
    }
    if age >= entry.fresh_for && has_swr && age < entry.fresh_for + entry.swr_for {
        return Lookup::Updating(entry);
    }
    if entry.no_cache {
        return Lookup::Expired;
    }
    if age >= entry.fresh_for {
        if age < entry.fresh_for + entry.stale_for {
            return Lookup::Stale(entry);
        }
        return Lookup::Expired;
    }
    Lookup::Hit(entry)
}

/// True once an entry is past every serving window (upper tiers may drop it).
pub fn entry_expired(entry: &Entry, now: SystemTime) -> bool {
    let age = now.duration_since(entry.stored_at).unwrap_or_default();
    let extra = entry.stale_for.max(entry.swr_for);
    age >= entry.fresh_for + extra
}

/// Path component of a base key (`tenant\u{1f}worker\u{1f}version\u{1f}entrypoint\u{1f}path?query`).
fn key_path(key: &str) -> Option<&str> {
    let path = key.split('\u{1f}').nth(4)?;
    Some(path.split('?').next().unwrap_or(path))
}

/// In-memory shared tier: one process, no cross-pod reach. Used by tests and
/// the control-plane e2e; the Redis adapter implements the same port.
#[derive(Default)]
pub struct InMemorySharedTier {
    inner: Mutex<HashMap<String, Vec<Entry>>>,
}

impl InMemorySharedTier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().values().map(Vec::len).sum()
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
}

/// One base may hold at most this many Vary variants (mirrors the lower
/// store's `MAX_VARY` bound; prevents a hostile stream of variants growing a
/// shared key without bound).
const MAX_VARIANTS_PER_BASE: usize = 64;

#[async_trait]
impl SharedTier for InMemorySharedTier {
    async fn get(
        &self,
        base: &str,
        req: &HeaderMap,
        now: SystemTime,
    ) -> Result<Option<Entry>, SharedError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(entries) = inner.get_mut(base) else {
            return Ok(None);
        };
        entries.retain(|e| !entry_expired(e, now));
        if entries.is_empty() {
            inner.remove(base);
            return Ok(None);
        }
        Ok(entries
            .iter()
            .find(|e| entry_matches(e, req))
            .cloned())
    }

    async fn put(&self, base: &str, entry: &Entry) -> Result<(), SharedError> {
        let now = SystemTime::now();
        let mut inner = self.inner.lock().unwrap();
        let entries = inner.entry(base.to_string()).or_default();
        entries.retain(|e| !entry_expired(e, now));
        entries.retain(|e| !same_variant(e, entry));
        if entries.len() >= MAX_VARIANTS_PER_BASE {
            entries.remove(0);
        }
        entries.push(entry.clone());
        Ok(())
    }

    async fn purge_worker(&self, tenant: &str, worker: &str) -> Result<usize, SharedError> {
        let prefix = format!("{tenant}\u{1f}{worker}\u{1f}");
        Ok(self.purge_where(|base, _| base.starts_with(&prefix)))
    }

    async fn purge_tags(&self, tenant: &str, tags: &[String]) -> Result<usize, SharedError> {
        let want: Vec<String> = tags.iter().map(|t| t.to_ascii_lowercase()).collect();
        let tenant_prefix = format!("{tenant}\u{1f}");
        let mut inner = self.inner.lock().unwrap();
        let mut removed = 0usize;
        for (base, entries) in inner.iter_mut() {
            if !base.starts_with(&tenant_prefix) {
                continue;
            }
            let before = entries.len();
            entries.retain(|e| {
                !e.tags
                    .iter()
                    .any(|t| want.iter().any(|w| w == &t.to_ascii_lowercase()))
            });
            removed += before - entries.len();
        }
        inner.retain(|_, entries| !entries.is_empty());
        Ok(removed)
    }

    async fn purge_prefix(&self, tenant: &str, prefix: &str) -> Result<usize, SharedError> {
        let tenant_prefix = format!("{tenant}\u{1f}");
        Ok(self.purge_where(|base, _| {
            base.starts_with(&tenant_prefix) && key_path(base).is_some_and(|p| p.starts_with(prefix))
        }))
    }

    async fn purge_all(&self, tenant: &str) -> Result<usize, SharedError> {
        let tenant_prefix = format!("{tenant}\u{1f}");
        Ok(self.purge_where(|base, _| base.starts_with(&tenant_prefix)))
    }
}

impl InMemorySharedTier {
    fn purge_where(&self, pred: impl Fn(&str, &[Entry]) -> bool) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let mut removed = 0usize;
        let keys: Vec<String> = inner
            .iter()
            .filter(|(base, entries)| pred(base, entries))
            .map(|(base, _)| base.clone())
            .collect();
        for key in keys {
            removed += inner.remove(&key).map(|e| e.len()).unwrap_or(0);
        }
        removed
    }
}

fn same_variant(a: &Entry, b: &Entry) -> bool {
    a.vary.len() == b.vary.len()
        && a.vary
            .iter()
            .all(|(n, v)| b.vary.iter().any(|(bn, bv)| bn == n && bv == v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};
    use std::time::Duration;

    fn base() -> String {
        "t\u{1f}w\u{1f}v\u{1f}default\u{1f}/x?".to_string()
    }

    fn entry() -> Entry {
        Entry {
            status: http::StatusCode::OK,
            version: http::Version::HTTP_11,
            headers: vec![],
            body: bytes::Bytes::from_static(b"x"),
            stored_at: SystemTime::now(),
            fresh_for: Duration::from_secs(60),
            stale_for: Duration::ZERO,
            swr_for: Duration::ZERO,
            no_cache: false,
            vary: vec![],
            tags: vec!["page".to_string()],
            size: 0,
        }
    }

    #[tokio::test]
    async fn put_get_purge_shared() {
        let tier = InMemorySharedTier::new();
        let b = base();
        tier.put(&b, &entry()).await.unwrap();
        assert!(tier.get(&b, &HeaderMap::new(), SystemTime::now()).await.unwrap().is_some());
        assert_eq!(tier.purge_worker("t", "w").await.unwrap(), 1);
        assert!(tier.get(&b, &HeaderMap::new(), SystemTime::now()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn vary_variants_match_exactly() {
        let tier = InMemorySharedTier::new();
        let b = base();
        let mut e = entry();
        e.vary = vec![("x-v".to_string(), Some("a".to_string()))];
        tier.put(&b, &e).await.unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-v"),
            HeaderValue::from_static("b"),
        );
        assert!(tier.get(&b, &h, SystemTime::now()).await.unwrap().is_none());
        h.insert(
            HeaderName::from_static("x-v"),
            HeaderValue::from_static("a"),
        );
        assert!(tier.get(&b, &h, SystemTime::now()).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn purge_tags_and_prefix() {
        let tier = InMemorySharedTier::new();
        let b = base();
        tier.put(&b, &entry()).await.unwrap();
        assert_eq!(tier.purge_tags("t", &["PAGE".to_string()]).await.unwrap(), 1);
        tier.put(&b, &entry()).await.unwrap();
        assert_eq!(tier.purge_prefix("t", "/x").await.unwrap(), 1);
        tier.put(&b, &entry()).await.unwrap();
        assert_eq!(tier.purge_all("t").await.unwrap(), 1);
    }

    #[test]
    fn expired_after_all_windows() {
        let mut e = entry();
        e.stored_at = SystemTime::now() - Duration::from_secs(61);
        assert!(entry_expired(&e, SystemTime::now()));
        assert!(matches!(classify_entry(e, SystemTime::now()), Lookup::Expired));
    }
}
