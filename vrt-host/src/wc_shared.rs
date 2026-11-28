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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Version};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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


/// One stored entry on the shared wire (body base64; metadata JSON). Kept
/// explicit so a future adapter (Redis/Kvrocks) can evolve the encoding
/// without touching the `Entry` type.
#[derive(Debug, Serialize, Deserialize)]
struct SharedRecord {
    status: u16,
    version: u16,
    headers: Vec<(String, String)>,
    body: String,
    stored_at_ms: u64,
    fresh_for_ms: u64,
    stale_for_ms: u64,
    swr_for_ms: u64,
    no_cache: bool,
    vary: Vec<(String, Option<String>)>,
    tags: Vec<String>,
}

impl From<&Entry> for SharedRecord {
    fn from(entry: &Entry) -> Self {
        Self {
            status: entry.status.as_u16(),
            version: match entry.version {
                Version::HTTP_10 => 10,
                Version::HTTP_2 => 2,
                Version::HTTP_3 => 3,
                _ => 11,
            },
            headers: entry
                .headers
                .iter()
                .filter_map(|(n, v)| {
                    Some((n.as_str().to_string(), v.to_str().ok()?.to_string()))
                })
                .collect(),
            body: base64::engine::general_purpose::STANDARD.encode(&entry.body),
            stored_at_ms: entry
                .stored_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            fresh_for_ms: entry.fresh_for.as_millis() as u64,
            stale_for_ms: entry.stale_for.as_millis() as u64,
            swr_for_ms: entry.swr_for.as_millis() as u64,
            no_cache: entry.no_cache,
            vary: entry.vary.clone(),
            tags: entry.tags.clone(),
        }
    }
}

impl SharedRecord {
    fn into_entry(self) -> Option<Entry> {
        let status = StatusCode::from_u16(self.status).ok()?;
        let version = match self.version {
            10 => Version::HTTP_10,
            2 => Version::HTTP_2,
            3 => Version::HTTP_3,
            _ => Version::HTTP_11,
        };
        let headers = self
            .headers
            .into_iter()
            .filter_map(|(n, v)| {
                Some((
                    HeaderName::from_bytes(n.as_bytes()).ok()?,
                    HeaderValue::from_str(&v).ok()?,
                ))
            })
            .collect();
        let body = base64::engine::general_purpose::STANDARD
            .decode(self.body.as_bytes())
            .ok()?;
        Some(Entry {
            status,
            version,
            headers,
            body: Bytes::from(body),
            stored_at: UNIX_EPOCH + Duration::from_millis(self.stored_at_ms),
            fresh_for: Duration::from_millis(self.fresh_for_ms),
            stale_for: Duration::from_millis(self.stale_for_ms),
            swr_for: Duration::from_millis(self.swr_for_ms),
            no_cache: self.no_cache,
            vary: self.vary,
            tags: self.tags,
            size: 0,
        })
    }
}

/// Stable per-variant hash (the response's captured Vary pairs). No Vary
/// entries share the `default` field.
fn variant_field(entry: &Entry) -> String {
    if entry.vary.is_empty() {
        return "default".to_string();
    }
    let mut pairs = entry.vary.clone();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = Sha256::new();
    for (name, value) in &pairs {
        h.update(name.as_bytes());
        h.update(b"\0");
        match value {
            Some(v) => h.update(v.as_bytes()),
            None => h.update(b"\x01"),
        };
        h.update(b"\x1f");
    }
    hex::encode(h.finalize())
}

fn redis_key(base: &str) -> String {
    format!("vrt:wc:{base}")
}
fn tag_key(tenant: &str, tag: &str) -> String {
    format!("vrt:wc:tag:{tenant}:{tag}")
}
fn base_of_key(key: &str) -> &str {
    key.strip_prefix("vrt:wc:").unwrap_or(key)
}

/// Redis/Kvrocks shared tier: one Redis hash per base key (field = variant),
/// a tag SET per (tenant, tag), bounded commands with a timeout. Any command
/// error/timeout surfaces as `SharedError` and the ingress degrades locally.
pub struct RedisSharedTier {
    conn: ConnectionManager,
    timeout: Duration,
}

impl RedisSharedTier {
    pub async fn connect(url: &str, timeout_ms: u64) -> Result<Self, SharedError> {
        let client = redis::Client::open(url).map_err(|e| SharedError(e.to_string()))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| SharedError(e.to_string()))?;
        Ok(Self {
            conn,
            timeout: Duration::from_millis(timeout_ms.max(1)),
        })
    }

    async fn cmd<F, T>(&self, future: F) -> Result<T, SharedError>
    where
        F: std::future::Future<Output = redis::RedisResult<T>>,
    {
        match tokio::time::timeout(self.timeout, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(SharedError(error.to_string())),
            Err(_) => Err(SharedError("redis command timed out".to_string())),
        }
    }

    async fn keys_with_prefix(&self, tenant: &str) -> Result<Vec<String>, SharedError> {
        let pattern = format!("vrt:wc:{tenant}\u{1f}*");
        let mut cursor = "0".to_string();
        let mut out = Vec::new();
        loop {
            let mut conn = self.conn.clone();
            let (next, batch): (String, Vec<String>) = self
                .cmd(
                    redis::cmd("SCAN")
                        .arg(&cursor)
                        .arg("MATCH")
                        .arg(&pattern)
                        .arg("COUNT")
                        .arg(500)
                        .query_async(&mut conn),
                )
                .await?;
            out.extend(batch);
            if next == "0" {
                break;
            }
            cursor = next;
        }
        Ok(out)
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<usize, SharedError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn.clone();
        let _: () = self
            .cmd(redis::cmd("DEL").arg(keys).query_async(&mut conn))
            .await?;
        Ok(keys.len())
    }

    async fn delete_matching(
        &self,
        tenant: &str,
        pred: impl Fn(&str) -> bool,
    ) -> Result<usize, SharedError> {
        let keys = self.keys_with_prefix(tenant).await?;
        let keys: Vec<String> = keys
            .into_iter()
            .filter(|k| pred(base_of_key(k)))
            .collect();
        self.delete_keys(&keys).await
    }
}

#[async_trait]
impl SharedTier for RedisSharedTier {
    async fn get(
        &self,
        base: &str,
        req: &HeaderMap,
        now: SystemTime,
    ) -> Result<Option<Entry>, SharedError> {
        let mut conn = self.conn.clone();
        let map: HashMap<String, String> = self.cmd(conn.hgetall(redis_key(base))).await?;
        for raw in map.values() {
            let Ok(record) = serde_json::from_str::<SharedRecord>(raw) else {
                continue;
            };
            let Some(entry) = record.into_entry() else {
                continue;
            };
            if entry_expired(&entry, now) {
                continue;
            }
            if entry_matches(&entry, req) {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn put(&self, base: &str, entry: &Entry) -> Result<(), SharedError> {
        let key = redis_key(base);
        let field = variant_field(entry);
        let value =
            serde_json::to_string(&SharedRecord::from(entry)).map_err(|e| SharedError(e.to_string()))?;
        let ttl = (entry.fresh_for + entry.stale_for.max(entry.swr_for))
            .as_millis()
            .max(1) as i64;
        // One round trip: metadata + TTL + every tag-index membership. A
        // response may carry up to MAX_TAGS tags; per-tag commands would make
        // a single store cost thousands of round trips.
        let mut pipe = redis::pipe();
        pipe.hset_multiple(&key, &[(field.as_str(), value.as_str())])
            .ignore();
        pipe.pexpire(&key, ttl).ignore();
        if let Some(tenant) = base.split('\u{1f}').next() {
            for tag in &entry.tags {
                pipe.sadd(tag_key(tenant, &tag.to_ascii_lowercase()), key.as_str())
                    .ignore();
            }
        }
        let mut conn = self.conn.clone();
        let _: () = self.cmd(pipe.query_async(&mut conn)).await?;
        Ok(())
    }

    async fn purge_worker(&self, tenant: &str, worker: &str) -> Result<usize, SharedError> {
        let prefix = format!("{tenant}\u{1f}{worker}\u{1f}");
        self.delete_matching(tenant, |base| base.starts_with(&prefix))
            .await
    }

    async fn purge_tags(&self, tenant: &str, tags: &[String]) -> Result<usize, SharedError> {
        let want: Vec<String> = tags.iter().map(|t| t.to_ascii_lowercase()).collect();
        let mut removed = 0usize;
        for tag in &want {
            let idx = tag_key(tenant, tag);
            let mut conn = self.conn.clone();
            let members: Vec<String> = self.cmd(conn.smembers(&idx)).await?;
            for key in members {
                let mut conn = self.conn.clone();
                let map: HashMap<String, String> = self.cmd(conn.hgetall(&key)).await?;
                let mut fields = Vec::new();
                for (field, raw) in &map {
                    let Ok(record) = serde_json::from_str::<SharedRecord>(raw) else {
                        continue;
                    };
                    if record
                        .tags
                        .iter()
                        .any(|t| want.iter().any(|w| w == &t.to_ascii_lowercase()))
                    {
                        fields.push(field.clone());
                    }
                }
                if !fields.is_empty() {
                    removed += fields.len();
                    let mut conn = self.conn.clone();
                    let _: () = self.cmd(conn.hdel(&key, fields)).await?;
                }
                let mut conn = self.conn.clone();
                let remaining: usize = self.cmd(conn.hlen(&key)).await?;
                if remaining == 0 {
                    let mut conn = self.conn.clone();
                    let _: () = self.cmd(conn.del(&key)).await?;
                }
            }
            let mut conn = self.conn.clone();
            let _: () = self.cmd(conn.del(&idx)).await?;
        }
        Ok(removed)
    }

    async fn purge_prefix(&self, tenant: &str, prefix: &str) -> Result<usize, SharedError> {
        self.delete_matching(tenant, |base| {
            key_path(base).is_some_and(|p| p.starts_with(prefix))
        })
        .await
    }

    async fn purge_all(&self, tenant: &str) -> Result<usize, SharedError> {
        self.delete_matching(tenant, |_| true).await
    }
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
    #[test]
    fn shared_record_roundtrip() {
        let mut e = entry();
        e.headers = vec![(
            HeaderName::from_static("x-a"),
            HeaderValue::from_static("b"),
        )];
        e.body = Bytes::from_static(b"hello");
        e.vary = vec![("x-v".to_string(), Some("a".to_string()))];
        let back = SharedRecord::from(&e).into_entry().unwrap();
        assert_eq!(back.status, e.status);
        assert_eq!(back.body, e.body);
        assert_eq!(back.vary, e.vary);
        assert_eq!(back.tags, e.tags);
        assert_eq!(back.headers.len(), 1);
    }

    #[tokio::test]
    async fn redis_roundtrip_when_configured() {
        let Ok(url) = std::env::var("VRT_TEST_REDIS_URL") else {
            return;
        };
        let tier = RedisSharedTier::connect(&url, 1_000).await.unwrap();
        let tenant = format!("rt-{}", std::process::id());
        let worker = "w";
        let base = format!("{tenant}\u{1f}{worker}\u{1f}v\u{1f}default\u{1f}/rt?");
        tier.put(&base, &entry()).await.unwrap();
        assert!(tier
            .get(&base, &HeaderMap::new(), SystemTime::now())
            .await
            .unwrap()
            .is_some());
        assert_eq!(tier.purge_worker(&tenant, worker).await.unwrap(), 1);
        assert!(tier
            .get(&base, &HeaderMap::new(), SystemTime::now())
            .await
            .unwrap()
            .is_none());
    }

}
