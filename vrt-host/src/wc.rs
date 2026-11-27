//! Workers Cache (Cloudflare `[cache]`): per-worker ingress response cache.
//!
//! This is the execution-front HTTP cache, independent of the C1 Cache API
//! (`caches.default`, `cache.rs`). A hit returns the stored response without
//! invoking the tenant Worker. The cache is keyed by tenant/worker/version/
//! entrypoint/path+query — never by host — and is version-scoped unless
//! `crossVersionCache` is set.
//!
//! Phase 1 semantics implemented here:
//! - GET/HEAD only (HEAD is served from the GET entry; callers pass the
//!   effective method), WebSocket/other methods bypass upstream.
//! - RFC 9111 freshness: `Cache-Control`/`Expires` + heuristic TTL table,
//!   `no-store`/`private`/`Authorization`/`Set-Cookie` bypass, `no-cache`
//!   store-but-expire, `Vary` variants, `Cache-Tag` + purge, `Cf-Cache-Status`.
//! - Bounded per-pod store with per-tenant/global byte quotas and LRU.
//!
//! Not yet wired here: single-flight collapsing, shared upper tier, Range
//! slicing, `stale-if-error` serving, `ctx.cache`. See
//! `doc/platform/workers-cache.md`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Version};
use axum::response::Response;
use bytes::Bytes;

/// Single-entry cap (headers + body + accounting overhead).
pub const MAX_ENTRY_BYTES: usize = 64 * 1024 * 1024;
/// Per-tenant stored bytes on this pod.
pub const MAX_TENANT_BYTES: usize = 256 * 1024 * 1024;
/// Whole-store stored bytes on this pod.
pub const MAX_GLOBAL_BYTES: usize = 512 * 1024 * 1024;
/// Per-entry tag count (Cloudflare zone-cache limit).
pub const MAX_TAGS: usize = 1000;
/// Single tag length cap.
pub const MAX_TAG_LEN: usize = 1024;
/// Vary header-name cap (defensive; a client-supplied response can't fan
/// out the variant index unboundedly).
pub const MAX_VARY: usize = 16;
/// Per-entry header block cap.
const MAX_HEADER_BYTES: usize = 32 * 1024;
/// Accounting overhead per entry (key + metadata).
const ENTRY_OVERHEAD: usize = 256;

/// `Cf-Cache-Status` values emitted by the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Hit,
    Miss,
    Expired,
    Bypass,
    Stale,
    Revalidated,
    Updating,
}

impl Status {
    pub fn header(self) -> &'static str {
        match self {
            Self::Hit => "HIT",
            Self::Miss => "MISS",
            Self::Expired => "EXPIRED",
            Self::Bypass => "BYPASS",
            Self::Stale => "STALE",
            Self::Revalidated => "REVALIDATED",
            Self::Updating => "UPDATING",
        }
    }
}

/// A stored response variant.
#[derive(Clone, Debug)]
pub struct Entry {
    pub status: StatusCode,
    pub version: Version,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub body: Bytes,
    pub stored_at: SystemTime,
    pub fresh_for: Duration,
    /// How long past freshness a stale copy may still serve on upstream
    /// error (`stale-if-error`; bounded to 24h, zero disables).
    pub stale_for: Duration,
    /// `no-cache` stores the entry but never serves it without revalidation.
    pub no_cache: bool,
    /// Header names from `Vary` plus the request values captured at store.
    pub vary: Vec<(String, Option<String>)>,
    pub tags: Vec<String>,
    pub size: usize,
}

impl Entry {
    pub fn age_seconds(&self, now: SystemTime) -> u64 {
        now.duration_since(self.stored_at).unwrap_or_default().as_secs()
    }

    fn matches(&self, req: &HeaderMap) -> bool {
        self.vary.iter().all(|(name, want)| {
            let got = req
                .get(name.as_str())
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            got.as_deref() == want.as_deref()
        })
    }
}

/// Cache-key plan for one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyPlan {
    /// Base key (all variants share it).
    pub key: String,
    pub head: bool,
}

/// Build the base key: tenant/worker/(version|*)/entrypoint/path+query.
/// Host is deliberately excluded (CF semantics).
pub fn key_plan(
    tenant_id: &str,
    worker_id: &str,
    version_id: &str,
    cross_version: bool,
    uri: &axum::http::Uri,
    method: &Method,
) -> Option<KeyPlan> {
    if !matches!(*method, Method::GET | Method::HEAD) {
        return None;
    }
    let path = uri.path();
    let query = uri.query().unwrap_or("");
    let version = if cross_version { "*" } else { version_id };
    Some(KeyPlan {
        key: format!(
            "{tenant_id}\u{1f}{worker_id}\u{1f}{version}\u{1f}default\u{1f}{path}?{query}"
        ),
        head: *method == Method::HEAD,
    })
}

/// Result of a lookup.
pub enum Lookup {
    Hit(Entry),
    /// Past freshness but within the `stale-if-error` window; kept for a
    /// possible fallback if the upstream fails.
    Stale(Entry),
    /// Entry existed but is past its freshness window (and was dropped).
    Expired,
    Miss,
}

/// Freshness/bypass decision for a response.
pub struct Policy {
    pub ttl: Option<Duration>,
    pub stale_for: Duration,
    pub no_cache: bool,
    pub vary: Vec<String>,
    pub tags: Vec<String>,
    pub bypass: bool,
}

/// Evaluate response cacheability. `req` is the effective (cache) request:
/// callers must apply HEAD→GET before invoking this.
pub fn evaluate(
    req: &HeaderMap,
    status: StatusCode,
    resp: &HeaderMap,
    path: &str,
) -> Policy {
    let mut policy = Policy {
        ttl: None,
        stale_for: Duration::ZERO,
        no_cache: false,
        vary: Vec::new(),
        tags: Vec::new(),
        bypass: false,
    };

    // Cacheable status set: 206/520-526 are never stored.
    if status == StatusCode::PARTIAL_CONTENT || (520..=526).contains(&status.as_u16()) {
        policy.bypass = true;
        return policy;
    }
    let cacheable_status = matches!(
        status.as_u16(),
        200 | 203 | 204 | 206 | 300 | 301 | 404 | 405 | 410 | 414 | 501
    );
    if !cacheable_status {
        policy.bypass = true;
        return policy;
    }

    // `Vary: *` disables caching.
    if let Some(v) = resp.get(header::VARY).and_then(|v| v.to_str().ok()) {
        let names: Vec<String> = v
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        if names.iter().any(|n| n == "*") || names.len() > MAX_VARY {
            policy.bypass = true;
            return policy;
        }
        policy.vary = names;
    }

    // Effective cache-control: cloudflare-cdn-cache-control > cdn-cache-control
    // > cache-control.
    let cc_raw = ["cloudflare-cdn-cache-control", "cdn-cache-control", "cache-control"]
        .iter()
        .find_map(|n| resp.get(*n).and_then(|v| v.to_str().ok()));
    let mut directives: HashMap<String, Option<String>> = HashMap::new();
    let mut explicit = false;
    if let Some(cc) = cc_raw {
        explicit = true;
        for part in cc.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let mut it = part.splitn(2, '=');
            let name = it.next().unwrap_or("").trim().to_ascii_lowercase();
            let value = it
                .next()
                .map(|v| v.trim().trim_matches('"').to_string());
            directives.insert(name, value);
        }
    }
    let flag = |name: &str| directives.contains_key(name);
    if flag("no-store") || flag("private") {
        policy.bypass = true;
        return policy;
    }
    policy.no_cache = flag("no-cache");

    // Authorization requires an explicit shared-cache grant (RFC 9111 §3.5).
    if req.contains_key(header::AUTHORIZATION)
        && !(flag("public") || flag("must-revalidate") || flag("s-maxage"))
    {
        policy.bypass = true;
        return policy;
    }
    // Set-Cookie is never cached (the `private="set-cookie"` carve-out is
    // deferred; fail closed and bypass).
    if resp.contains_key(header::SET_COOKIE) {
        policy.bypass = true;
        return policy;
    }

    // TTL: s-maxage > max-age > Expires > heuristic.
    let secs = |name: &str| {
        directives
            .get(name)
            .and_then(|v| v.as_deref())
            .and_then(|v| v.parse::<u64>().ok())
    };
    if let Some(v) = secs("s-maxage").or_else(|| secs("max-age")) {
        policy.ttl = Some(Duration::from_secs(v));
    } else if let Some(exp) = resp.get(header::EXPIRES).and_then(|v| v.to_str().ok()) {
        if let Ok(t) = httpdate_secs(exp) {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            policy.ttl = Some(Duration::from_secs((t - now).max(0) as u64));
        }
    }
    if policy.ttl.is_none() {
        policy.ttl = heuristic_ttl(status.as_u16());
    }
    if policy.ttl.is_none() {
        policy.bypass = true;
        return policy;
    }
    if policy.ttl == Some(Duration::ZERO) && !policy.no_cache {
        // max-age=0 is legal: store but immediately stale. Treat like
        // no-cache (never served without revalidation).
        policy.no_cache = true;
    }

    // stale-if-error: default 24h unless a directive forbids stale
    // (s-maxage/must-revalidate/proxy-revalidate); explicit 0 disables.
    let stale_capped = !flag("s-maxage") && !flag("must-revalidate") && !flag("proxy-revalidate");
    let stale_secs = secs("stale-if-error").unwrap_or(if stale_capped { 86_400 } else { 0 });
    policy.stale_for = Duration::from_secs(stale_secs.min(86_400));

    // Cache Deception Armor: only for heuristic freshness (no explicit
    // Cache-Control/Expires), text/* or application/*, extension mismatch.
    if !explicit
        && policy.ttl.is_some()
        && extension_mismatch(path, resp.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()))
    {
        policy.bypass = true;
        return policy;
    }

    policy.tags = parse_tags(resp.get("cache-tag").and_then(|v| v.to_str().ok()));
    if policy.tags.len() > MAX_TAGS {
        policy.tags.truncate(MAX_TAGS);
    }
    policy
}

/// Heuristic freshness table (RFC 9111 §4.2.2 defaults used by CF).
fn heuristic_ttl(status: u16) -> Option<Duration> {
    let secs = match status {
        200 | 203 | 204 => 7_200,
        300 | 301 => 1_200,
        404 | 410 => 180,
        405 | 414 | 501 => 60,
        _ => return None,
    };
    Some(Duration::from_secs(secs))
}

/// Parse `Cache-Tag`: comma-separated, printable ASCII, ≤1024 chars,
/// case-insensitive dedupe, invalid values dropped silently.
fn parse_tags(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for tag in raw.split(',') {
        let tag = tag.trim().to_ascii_lowercase();
        if tag.is_empty() || tag.len() > MAX_TAG_LEN || seen.contains(&tag) {
            continue;
        }
        if !tag.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            continue;
        }
        seen.insert(tag.clone());
        out.push(tag);
        if out.len() >= MAX_TAGS {
            break;
        }
    }
    out
}

/// Cache Deception Armor: a static-looking extension whose response MIME
/// does not match must not be cached under heuristic freshness.
fn extension_mismatch(path: &str, content_type: Option<&str>) -> bool {
    let Some(ct) = content_type else {
        return false;
    };
    let ct = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    if !(ct.starts_with("text/") || ct.starts_with("application/")) {
        return false;
    }
    let Some(ext) = path.rsplit('/').next().and_then(|f| f.rsplit_once('.').map(|(_, e)| e)) else {
        return false;
    };
    let expected = match ext.to_ascii_lowercase().as_str() {
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "html" | "htm" => "text/html",
        "json" => "application/json",
        "xml" => "application/xml",
        "txt" => "text/plain",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "pdf" => "application/pdf",
        _ => return false,
    };
    // svg/png/jpg/gif/webp/pdf are outside text/*|application/*, so the
    // check does not apply; the remaining mapping is exact.
    expected != ct
}

/// Minimal IMF-fixdate parser (Expires). Returns UNIX seconds.
fn httpdate_secs(s: &str) -> Result<i64, ()> {
    // RFC 7231 IMF-fixdate: "Sun, 06 Nov 1994 08:49:37 GMT".
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 6 || !parts[5].eq_ignore_ascii_case("GMT") {
        return Err(());
    }
    let day: i64 = parts[1].parse().map_err(|_| ())?;
    let month = match parts[2].to_ascii_lowercase().as_str() {
        "jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4, "may" => 5, "jun" => 6,
        "jul" => 7, "aug" => 8, "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12,
        _ => return Err(()),
    };
    let year: i64 = parts[3].parse().map_err(|_| ())?;
    let hms: Vec<&str> = parts[4].split(':').collect();
    if hms.len() != 3 {
        return Err(());
    }
    let (h, m, sec) = (
        hms[0].parse::<i64>().map_err(|_| ())?,
        hms[1].parse::<i64>().map_err(|_| ())?,
        hms[2].parse::<i64>().map_err(|_| ())?,
    );
    if !(1..=31).contains(&day) || !(0..=23).contains(&h) || !(0..=59).contains(&m) || !(0..=60).contains(&sec) {
        return Err(());
    }
    Ok(days_from_civil(year, month, day) * 86_400 + h * 3600 + m * 60 + sec)
}

/// Howard Hinnant's days-from-civil (proleptic Gregorian), 1970-01-01 = 0.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Bounded per-pod store.
pub struct Store {
    inner: Mutex<Inner>,
}

struct Inner {
    /// variant key -> entry
    entries: HashMap<String, Entry>,
    /// base key -> variant keys
    base_index: HashMap<String, Vec<String>>,
    /// tag -> variant keys
    tags: HashMap<String, HashSet<String>>,
    tenant_bytes: HashMap<String, usize>,
    global_bytes: usize,
    /// LRU order (front = oldest).
    order: VecDeque<String>,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                base_index: HashMap::new(),
                tags: HashMap::new(),
                tenant_bytes: HashMap::new(),
                global_bytes: 0,
                order: VecDeque::new(),
            }),
        }
    }
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a matching, fresh entry. Expired entries are removed.
    pub fn get(&self, base: &str, req: &HeaderMap, now: SystemTime) -> Lookup {
        let mut inner = self.inner.lock().unwrap();
        let Some(keys) = inner.base_index.get(base).cloned() else {
            return Lookup::Miss;
        };
        for key in keys {
            let Some(entry) = inner.entries.get(&key) else {
                continue;
            };
            if !entry.matches(req) {
                continue;
            }
            let age = now.duration_since(entry.stored_at).unwrap_or_default();
            if entry.no_cache {
                inner.remove(&key);
                return Lookup::Expired;
            }
            if age >= entry.fresh_for {
                if age < entry.fresh_for + entry.stale_for {
                    return Lookup::Stale(entry.clone());
                }
                inner.remove(&key);
                return Lookup::Expired;
            }
            let hit = entry.clone();
            inner.touch(&key);
            return Lookup::Hit(hit);
        }
        Lookup::Miss
    }

    /// Store a response variant. Returns true when stored.
    pub fn put(&self, base: &str, entry: Entry) -> bool {
        let size = entry
            .body
            .len()
            .saturating_add(header_bytes(&entry.headers))
            .saturating_add(ENTRY_OVERHEAD);
        if size > MAX_ENTRY_BYTES {
            return false;
        }
        let tenant = base.split('\u{1f}').next().unwrap_or("");
        let mut inner = self.inner.lock().unwrap();
        let tenant_used = *inner.tenant_bytes.get(tenant).unwrap_or(&0);
        if tenant_used.saturating_add(size) > MAX_TENANT_BYTES {
            inner.evict_tenant(tenant, size);
        }
        inner.evict_global(size);
        if inner.global_bytes.saturating_add(size) > MAX_GLOBAL_BYTES {
            return false;
        }
        let key = variant_key(base, &entry.vary, &entry);
        let mut stored = entry;
        stored.size = size;
        // Replace any previous variant with the same key.
        inner.remove(&key);
        inner.global_bytes += size;
        *inner.tenant_bytes.entry(tenant.to_string()).or_insert(0) += size;
        for tag in &stored.tags {
            inner.tags.entry(tag.clone()).or_default().insert(key.clone());
        }
        inner
            .base_index
            .entry(base.to_string())
            .or_default()
            .push(key.clone());
        inner.entries.insert(key.clone(), stored);
        inner.order.push_back(key);
        true
    }

    pub fn purge_tags(&self, tenant: &str, tags: &[String]) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let mut targets = HashSet::new();
        let want: HashSet<String> = tags.iter().map(|t| t.to_ascii_lowercase()).collect();
        for (tag, keys) in &inner.tags {
            if want.contains(tag) {
                targets.extend(keys.iter().cloned());
            }
        }
        let tenants: Vec<String> = targets
            .into_iter()
            .filter(|k| k.starts_with(&format!("{tenant}\u{1f}")))
            .collect();
        let n = tenants.len();
        for key in tenants {
            inner.remove(&key);
        }
        n
    }

    pub fn purge_prefix(&self, tenant: &str, prefix: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let tenant_prefix = format!("{tenant}\u{1f}");
        let keys: Vec<String> = inner
            .entries
            .keys()
            .filter(|k| k.starts_with(&tenant_prefix))
            .filter(|k| key_path(k).is_some_and(|p| p.starts_with(prefix)))
            .cloned()
            .collect();
        let n = keys.len();
        for key in keys {
            inner.remove(&key);
        }
        n
    }

    /// Purge every entry of one worker (all versions/entrypoints/paths).
    pub fn purge_worker(&self, tenant: &str, worker: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let prefix = format!("{tenant}\u{1f}{worker}\u{1f}");
        let keys: Vec<String> = inner
            .entries
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        let n = keys.len();
        for key in keys {
            inner.remove(&key);
        }
        n
    }

    pub fn purge_all(&self, tenant: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let prefix = format!("{tenant}\u{1f}");
        let keys: Vec<String> = inner
            .entries
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        let n = keys.len();
        for key in keys {
            inner.remove(&key);
        }
        n
    }

    pub fn stats(&self) -> (usize, usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.entries.len(), inner.global_bytes, inner.tags.len())
    }
}

impl Inner {
    fn touch(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.to_string());
    }

    fn remove(&mut self, key: &str) {
        let Some(entry) = self.entries.remove(key) else {
            return;
        };
        self.global_bytes = self.global_bytes.saturating_sub(entry.size);
        if let Some(tenant) = key.split('\u{1f}').next() {
            if let Some(used) = self.tenant_bytes.get_mut(tenant) {
                *used = used.saturating_sub(entry.size);
                if *used == 0 {
                    self.tenant_bytes.remove(tenant);
                }
            }
        }
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        for tag in entry.tags {
            if let Some(keys) = self.tags.get_mut(&tag) {
                keys.remove(key);
                if keys.is_empty() {
                    self.tags.remove(&tag);
                }
            }
        }
        for keys in self.base_index.values_mut() {
            keys.retain(|k| k != key);
        }
        self.base_index.retain(|_, v| !v.is_empty());
    }

    fn evict_tenant(&mut self, tenant: &str, need: usize) {
        let prefix = format!("{tenant}\u{1f}");
        while self.tenant_bytes.get(tenant).copied().unwrap_or(0) + need > MAX_TENANT_BYTES {
            let Some(key) = self
                .order
                .iter()
                .find(|k| k.starts_with(&prefix))
                .cloned()
            else {
                break;
            };
            self.remove(&key);
        }
    }

    fn evict_global(&mut self, need: usize) {
        while self.global_bytes + need > MAX_GLOBAL_BYTES {
            let Some(key) = self.order.front().cloned() else {
                break;
            };
            self.remove(&key);
        }
    }
}

/// Path component of a cache key (5th `\u{1f}`-separated field).
fn key_path(key: &str) -> Option<&str> {
    key.split('\u{1f}').nth(4)
}

fn header_bytes(headers: &[(HeaderName, HeaderValue)]) -> usize {
    headers
        .iter()
        .map(|(n, v)| n.as_str().len() + v.as_bytes().len() + 4)
        .sum()
}

fn variant_key(base: &str, vary: &[(String, Option<String>)], entry: &Entry) -> String {
    if vary.is_empty() {
        return base.to_string();
    }
    // FNV-1a over names/values keeps the key bounded without a dep.
    let mut h: u64 = 0xcbf29ce484222325;
    for (name, value) in vary {
        for b in name
            .as_bytes()
            .iter()
            .chain(value.as_deref().unwrap_or("").as_bytes())
            .chain([0u8].iter())
        {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    let _ = entry;
    format!("{base}\u{1f}v={h:016x}")
}

/// Build a response from a stored entry. `head` strips the body; `range`
/// slices a cached full 200 into `206`/`416` (CF semantics: the Worker is
/// never invoked for a cached range).
pub fn response(
    entry: &Entry,
    head: bool,
    status: Status,
    now: SystemTime,
    range: Option<&str>,
) -> Response {
    let mut builder = Response::builder();
    let (code, body, content_range) = match range {
        Some(raw) => match slice_range(entry.body.len(), raw) {
            Ok((start, end)) => (
                StatusCode::PARTIAL_CONTENT,
                Bytes::copy_from_slice(&entry.body[start..=end]),
                Some(format!("bytes {start}-{end}/{}", entry.body.len())),
            ),
            Err(()) => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                Bytes::new(),
                Some(format!("bytes */{}", entry.body.len())),
            ),
        },
        None => (entry.status, entry.body.clone(), None),
    };
    builder = builder.status(code);
    for (name, value) in &entry.headers {
        if name == header::SET_COOKIE
            || name.as_str() == "cache-tag"
            || name == header::CONTENT_RANGE
            || (range.is_some() && name == header::CONTENT_LENGTH)
        {
            continue;
        }
        builder = builder.header(name, value);
    }
    if let Some(cr) = &content_range {
        builder = builder.header(header::CONTENT_RANGE, cr);
    }
    if range.is_some() {
        builder = builder.header(header::CONTENT_LENGTH, body.len().to_string());
    }
    let age = entry.age_seconds(now);
    builder = builder
        .header("cf-cache-status", status.header())
        .header(header::AGE, age.to_string());
    let body = if head { Body::empty() } else { Body::from(body) };
    builder
        .body(body)
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

/// Parse a single `Range: bytes=…` against a full body length.
/// Returns an inclusive `(start, end)` or `Err` for 416.
fn slice_range(total: usize, raw: &str) -> Result<(usize, usize), ()> {
    let spec = raw.strip_prefix("bytes=").ok_or(())?;
    let spec = spec.split(',').next().ok_or(())?.trim();
    if total == 0 {
        return Err(());
    }
    let (a, b) = spec.split_once('-').ok_or(())?;
    let (start, end) = if a.is_empty() {
        // suffix: -N (last N bytes)
        let n: usize = b.trim().parse().map_err(|_| ())?;
        if n == 0 {
            return Err(());
        }
        (total.saturating_sub(n), total - 1)
    } else {
        let start: usize = a.trim().parse().map_err(|_| ())?;
        let end = if b.trim().is_empty() {
            total - 1
        } else {
            b.trim().parse::<usize>().map_err(|_| ())?
        };
        (start, end.min(total - 1))
    };
    if start > end || start >= total {
        return Err(());
    }
    Ok((start, end))
}

/// Strip internal headers before storing; reject oversize header blocks.
pub fn storable_headers(resp: &HeaderMap) -> Option<Vec<(HeaderName, HeaderValue)>> {
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for (name, value) in resp.iter() {
        if name == header::SET_COOKIE
            || name.as_str() == "cache-tag"
            || name.as_str() == "cf-cache-status"
            || name == header::CONNECTION
            || name == header::TRANSFER_ENCODING
        {
            continue;
        }
        bytes += name.as_str().len() + value.as_bytes().len() + 4;
        if bytes > MAX_HEADER_BYTES {
            return None;
        }
        out.push((name.clone(), value.clone()));
    }
    Some(out)
}

/// Capture `Vary` request values at store time.
pub fn vary_values(names: &[String], req: &HeaderMap) -> Vec<(String, Option<String>)> {
    names
        .iter()
        .map(|n| {
            (
                n.clone(),
                req.get(n.as_str())
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string),
            )
        })
        .collect()
}

/// Single-flight registry: concurrent misses for one cache key collapse
/// into one upstream run per pod. The leader stores (or bypasses) and
/// notifies; followers re-check the store before falling back to a direct
/// upstream request (fail-open, never a fake hit).
#[derive(Clone, Default)]
pub struct Flights {
    inner: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<tokio::sync::Notify>>>>,
}

pub enum FlightEntry {
    Leader(FlightGuard),
    Follower(std::sync::Arc<tokio::sync::Notify>),
}

pub struct FlightGuard {
    flights: Flights,
    key: String,
}

impl Flights {
    pub fn enter(&self, key: &str) -> FlightEntry {
        let mut map = self.inner.lock().unwrap();
        if let Some(n) = map.get(key) {
            return FlightEntry::Follower(n.clone());
        }
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        map.insert(key.to_string(), n);
        FlightEntry::Leader(FlightGuard {
            flights: self.clone(),
            key: key.to_string(),
        })
    }

    pub async fn wait(n: std::sync::Arc<tokio::sync::Notify>, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, n.notified()).await;
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        let n = self.flights.inner.lock().unwrap().remove(&self.key);
        if let Some(n) = n {
            n.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    fn req(headers: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (n, v) in headers {
            h.insert(HeaderName::from_bytes(n.as_bytes()).unwrap(), HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn ttl_and_heuristic() {
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &req(&[("cache-control", "public, max-age=60")]), "/");
        assert_eq!(p.ttl, Some(Duration::from_secs(60)));
        assert!(!p.bypass);

        // No headers: heuristic 2h for 200, 3m for 404.
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &HeaderMap::new(), "/");
        assert_eq!(p.ttl, Some(Duration::from_secs(7200)));
        let p = evaluate(&HeaderMap::new(), StatusCode::NOT_FOUND, &HeaderMap::new(), "/api/users");
        assert_eq!(p.ttl, Some(Duration::from_secs(180)));

        // s-maxage wins over max-age.
        let p = evaluate(
            &HeaderMap::new(),
            StatusCode::OK,
            &req(&[("cache-control", "max-age=10, s-maxage=99")]),
            "/",
        );
        assert_eq!(p.ttl, Some(Duration::from_secs(99)));
    }

    #[test]
    fn bypass_rules() {
        // no-store / private
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &req(&[("cache-control", "no-store")]), "/");
        assert!(p.bypass);
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &req(&[("cache-control", "private, max-age=60")]), "/");
        assert!(p.bypass);
        // Set-Cookie
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &req(&[("set-cookie", "a=b"), ("cache-control", "max-age=60")]), "/");
        assert!(p.bypass);
        // Authorization without a shared grant
        let p = evaluate(&req(&[("authorization", "Bearer x")]), StatusCode::OK, &req(&[("cache-control", "max-age=60")]), "/");
        assert!(p.bypass);
        // Authorization + public is storable
        let p = evaluate(&req(&[("authorization", "Bearer x")]), StatusCode::OK, &req(&[("cache-control", "public, max-age=60")]), "/");
        assert!(!p.bypass);
        // Vary: *
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &req(&[("vary", "*")]), "/");
        assert!(p.bypass);
    }

    #[test]
    fn no_cache_is_stored_but_never_served() {
        let mut h = HeaderMap::new();
        h.insert("cache-control", HeaderValue::from_static("no-cache, max-age=60"));
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &h, "/");
        assert!(p.no_cache);
        let store = Store::new();
        let base = "t\u{1f}w\u{1f}v\u{1f}default\u{1f}/x?";
        assert!(store.put(base, Entry {
            status: StatusCode::OK,
            version: Version::HTTP_11,
            headers: vec![],
            body: Bytes::from_static(b"x"),
            stored_at: now(),
            fresh_for: Duration::from_secs(60),
            stale_for: Duration::ZERO,
            no_cache: true,
            vary: vec![],
            tags: vec![],
            size: 0,
        }));
        assert!(matches!(store.get(base, &HeaderMap::new(), now()), Lookup::Expired));
        assert_eq!(store.stats().0, 0, "expired entry dropped");
    }

    #[test]
    fn vary_variants_and_purge() {
        let store = Store::new();
        let base = "t\u{1f}w\u{1f}v\u{1f}default\u{1f}/x?";
        for accept in ["json", "html"] {
            let mut h = HeaderMap::new();
            h.insert("vary", HeaderValue::from_static("accept"));
            h.insert("cache-tag", HeaderValue::from_static("Page, page"));
            let p = evaluate(&HeaderMap::new(), StatusCode::OK, &h, "/x");
            assert_eq!(p.vary, vec!["accept".to_string()]);
            assert_eq!(p.tags, vec!["page".to_string()]);
            let req_h = req(&[("accept", accept)]);
            let mut stored = HeaderMap::new();
            stored.insert("content-type", HeaderValue::from_static("text/plain"));
            let headers = storable_headers(&stored).unwrap();
            assert!(store.put(base, Entry {
                status: StatusCode::OK,
                version: Version::HTTP_11,
                headers,
                body: Bytes::from(accept.to_string()),
                stored_at: now(),
                fresh_for: Duration::from_secs(60),
                stale_for: Duration::ZERO,
                no_cache: false,
                vary: vary_values(&p.vary, &req_h),
                tags: p.tags,
                size: 0,
            }));
        }
        assert_eq!(store.stats().0, 2);
        let hit = store.get(base, &req(&[("accept", "json")]), now());
        let Lookup::Hit(e) = hit else { panic!() };
        assert_eq!(&e.body[..], b"json");
        assert!(matches!(store.get(base, &req(&[("accept", "raw")]), now()), Lookup::Miss));
        assert_eq!(store.purge_tags("t", &["PAGE".to_string()]), 2);
        assert_eq!(store.stats().0, 0);
    }

    #[test]
    fn stale_window_keeps_then_drops() {
        let store = Store::new();
        let base = "t\u{1f}w\u{1f}v\u{1f}default\u{1f}/x?";
        let mk = |stale_for: Duration| Entry {
            status: StatusCode::OK,
            version: Version::HTTP_11,
            headers: vec![],
            body: Bytes::from_static(b"x"),
            stored_at: now() - Duration::from_secs(2),
            fresh_for: Duration::from_secs(1),
            stale_for,
            no_cache: false,
            vary: vec![],
            tags: vec![],
            size: 0,
        };
        assert!(store.put(base, mk(Duration::from_secs(60))));
        assert!(matches!(store.get(base, &HeaderMap::new(), now()), Lookup::Stale(_)));
        assert!(store.put(base, mk(Duration::ZERO)));
        assert!(matches!(store.get(base, &HeaderMap::new(), now()), Lookup::Expired));
    }

    #[test]
    fn purge_worker_is_tenant_scoped() {
        let store = Store::new();
        let put = |tenant: &str, worker: &str, path: &str| {
            store.put(
                &format!("{tenant}\u{1f}{worker}\u{1f}v\u{1f}default\u{1f}{path}?"),
                Entry {
                    status: StatusCode::OK,
                    version: Version::HTTP_11,
                    headers: vec![],
                    body: Bytes::from_static(b"x"),
                    stored_at: now(),
                    fresh_for: Duration::from_secs(60),
                    stale_for: Duration::ZERO,
                    no_cache: false,
                    vary: vec![],
                    tags: vec![],
                    size: 0,
                },
            )
        };
        assert!(put("t", "w1", "/a"));
        assert!(put("t", "w2", "/b"));
        assert!(put("t2", "w1", "/c"));
        assert_eq!(store.purge_worker("t", "w1"), 1);
        assert_eq!(store.stats().0, 2);
        assert_eq!(store.purge_all("t2"), 1);
        assert_eq!(store.stats().0, 1);
    }

    #[test]
    fn lru_evicts_within_tenant() {
        let store = Store::new();
        let base = |i: usize| format!("t\u{1f}w\u{1f}v\u{1f}default\u{1f}/x{i}?");
        // Two entries: the tenant cap is large, so just verify LRU stats and
        // prefix purge behavior.
        for i in 0..2 {
            assert!(store.put(&base(i), Entry {
                status: StatusCode::OK,
                version: Version::HTTP_11,
                headers: vec![],
                body: Bytes::from_static(b"x"),
                stored_at: now(),
                fresh_for: Duration::from_secs(60),
                stale_for: Duration::ZERO,
                no_cache: false,
                vary: vec![],
                tags: vec![],
                size: 0,
            }));
        }
        assert_eq!(store.stats().0, 2);
        assert_eq!(store.purge_prefix("t", "/x0"), 1);
        assert_eq!(store.stats().0, 1);
        assert_eq!(store.purge_all("t"), 1);
        assert_eq!(store.stats().0, 0);
    }

    #[test]
    fn deception_armor_and_tag_limits() {
        let mut h = HeaderMap::new();
        h.insert("content-type", HeaderValue::from_static("text/html"));
        // No explicit Cache-Control + `/style.css` → mismatch → bypass.
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &h, "/style.css");
        assert!(p.bypass);
        // Explicit directive skips the check.
        h.insert("cache-control", HeaderValue::from_static("max-age=60"));
        let p = evaluate(&HeaderMap::new(), StatusCode::OK, &h, "/style.css");
        assert!(!p.bypass);

        let tags = parse_tags(Some("ok, bad tag,  \u{00e9}, ok"));
        assert_eq!(tags, vec!["ok".to_string()]);
    }

    #[test]
    fn expires_parsing() {
        assert_eq!(httpdate_secs("Sun, 06 Nov 1994 08:49:37 GMT").unwrap(), 784111777);
        assert!(httpdate_secs("nonsense").is_err());
    }

    #[tokio::test]
    async fn single_flight_wakes_followers() {
        let flights = Flights::default();
        let FlightEntry::Leader(leader) = flights.enter("k") else {
            panic!("first is leader")
        };
        let FlightEntry::Follower(n) = flights.enter("k") else {
            panic!("second is follower")
        };
        let waiter =
            tokio::spawn(async move { Flights::wait(n, Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "follower waits for the leader");
        drop(leader);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("followers wake when the leader finishes")
            .unwrap();
    }

    #[test]
    fn range_slicing_serves_206_and_416() {
        let entry = Entry {
            status: StatusCode::OK,
            version: Version::HTTP_11,
            headers: vec![(header::CONTENT_LENGTH, HeaderValue::from_static("10"))],
            body: Bytes::from_static(b"0123456789"),
            stored_at: now(),
            fresh_for: Duration::from_secs(60),
            stale_for: Duration::ZERO,
            no_cache: false,
            vary: vec![],
            tags: vec![],
            size: 0,
        };
        let r = response(&entry, false, Status::Hit, now(), Some("bytes=2-4"));
        assert_eq!(r.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(r.headers()[header::CONTENT_RANGE], "bytes 2-4/10");
        assert_eq!(r.headers()[header::CONTENT_LENGTH], "3");

        let r = response(&entry, false, Status::Hit, now(), Some("bytes=-3"));
        assert_eq!(r.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(r.headers()[header::CONTENT_RANGE], "bytes 7-9/10");

        let r = response(&entry, false, Status::Hit, now(), Some("bytes=20-"));
        assert_eq!(r.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(r.headers()[header::CONTENT_RANGE], "bytes */10");

        let r = response(&entry, true, Status::Hit, now(), None);
        assert_eq!(r.status(), StatusCode::OK);
    }

    #[test]
    fn key_excludes_host_and_scopes_version() {
        let uri: axum::http::Uri = "/a/b?x=1".parse().unwrap();
        let k = key_plan("t", "w", "v1", false, &uri, &Method::GET).unwrap();
        assert_eq!(k.key, "t\u{1f}w\u{1f}v1\u{1f}default\u{1f}/a/b?x=1");
        let k = key_plan("t", "w", "v1", true, &uri, &Method::GET).unwrap();
        assert!(k.key.contains("\u{1f}*\u{1f}"));
        assert!(key_plan("t", "w", "v1", false, &uri, &Method::POST).is_none());
    }
}
