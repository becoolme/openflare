//! Static-assets data plane (T21): content-addressed, tenant-scoped
//! store shared with `vrt-host`/`vrt deploy`, plus the
//! `POST /internal/assets/v1/fetch` resolver.
//!
//! Layout under `root`:
//!
//! ```text
//! <root>/<enc(tenant)>/manifests/<manifestDigest>.json
//! <root>/<enc(tenant)>/objects/<sha256>
//! ```
//!
//! Trust model: every write is sha256-verified BEFORE it lands
//! (`put_*_verified` — the filename is the digest, a forged body can
//! never be committed), and every READ re-verifies the stored bytes
//! against the declared digest, so a poisoned store (manual edits,
//! partial rsync, disk corruption) fails closed as
//! `ASSET_STORAGE_UNAVAILABLE` instead of serving wrong bytes.
//!
//! Manifest document (produced by the platform upload API or
//! `vrt deploy --assets`; canonical JSON — sorted keys):
//!
//! ```json
//! {"schemaVersion":1,
//!  "htmlHandling":"auto-trailing-slash",
//!  "notFoundHandling":"none",
//!  "files":[{"path":"/index.html","sha256":"…","size":12,"mime":"text/html"}]}
//! ```

use axum::body::Body;
use axum::http::{header, HeaderValue, Request, Response, StatusCode};
use crate::assets_headers::{self, HeaderRule};
use crate::assets_redirects::{self, RedirectAction, RedirectRule};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Manifest document byte cap — a manifest is JSON metadata only;
/// larger is a malformed/abusive write (512 files × ~200 B ≪ 1 MiB).
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
/// Single asset object byte cap — same bound as the platform upload.
pub const MAX_ASSET_OBJECT_BYTES: u64 = 8 * 1024 * 1024;
/// Maximum files per manifest (defense-in-depth at resolve time).
/// Platform `assetMaxFiles` hard ceiling is 8192
/// (vrtFeatureFlag.ts:106) — the read-side cap must accept every
/// manifest upload could legally produce, otherwise an admin-raised
/// site 503s on every request. Over this bound the manifest is
/// genuinely malformed.
const MAX_MANIFEST_FILES: usize = 8192;

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetManifest {
    #[serde(default)]
    pub files: Vec<AssetFile>,
    #[serde(default)]
    pub html_handling: HtmlHandling,
    #[serde(default)]
    pub not_found_handling: NotFoundHandling,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct AssetFile {
    /// Request path (`/index.html`, `/css/site.css`) — already validated
    /// at upload; re-validated here because the store is shared state.
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub mime: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
pub enum HtmlHandling {
    /// Individual files are served without a trailing slash, folder
    /// index files with one; every other form redirects to its
    /// canonical URL (Cloudflare's `auto-trailing-slash`).
    #[default]
    #[serde(rename = "auto-trailing-slash")]
    AutoTrailingSlash,
    /// Every HTML route is canonical with a trailing slash.
    #[serde(rename = "force-trailing-slash")]
    ForceTrailingSlash,
    /// Every HTML route is canonical without a trailing slash.
    #[serde(rename = "drop-trailing-slash")]
    DropTrailingSlash,
    /// Exact path only.
    #[serde(rename = "none")]
    None,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
pub enum NotFoundHandling {
    /// Typed 404 (`ASSET_NOT_FOUND`).
    #[default]
    #[serde(rename = "none")]
    None,
    /// Serve `/404.html` when present, else typed 404.
    #[serde(rename = "404-page")]
    Page404,
    /// Serve `/index.html` (SPA fallback), else typed 404.
    #[serde(rename = "single-page-application")]
    SinglePageApplication,
}

/// Resolution outcome for one normalized request path.
#[derive(Debug, PartialEq, Eq)]
pub enum Resolved {
    /// Serve this manifest file entry.
    Serve(AssetFile),
    /// 307 redirect to this path (non-canonical HTML route)
    /// slash under `auto-trailing-slash`).
    Redirect(String),
    /// SPA fallback hit — serves `/index.html` under
    /// `single-page-application` not-found handling (distinguished for
    /// B12 `vrt_worker_assets_requests_total{outcome="spa_fallback"}`).
    SpaFallback(AssetFile),
    /// `404-page` hit — serves `/404.html` with status 404 (Cloudflare
    /// semantics: the page body, but never a 200).
    NotFoundPage(AssetFile),
    /// Typed miss after html resolution + not-found fallbacks.
    NotFound,
}

/// Percent-decode one path segment — mirror of Miniflare
/// `decodePath` (assets.worker.js:1057–1062): `decodeURIComponent`
/// per segment; malformed escapes or non-UTF-8 decodes keep the raw
/// segment instead of erroring.
fn decode_segment(seg: &str) -> String {
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let mut saw_escape = false;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            // decodeURIComponent parity: ANY malformed escape fails the
            // WHOLE segment — `%zz`, `%2`, lone `%` all keep raw.
            if i + 2 >= bytes.len() {
                return seg.to_string();
            }
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            match (hi, lo) {
                (Some(h), Some(l)) => {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    saw_escape = true;
                    continue;
                }
                _ => return seg.to_string(),
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    if !saw_escape {
        return seg.to_string();
    }
    match String::from_utf8(out) {
        Ok(decoded) => decoded,
        Err(_) => seg.to_string(), // malformed UTF-8 — keep raw
    }
}

/// Percent-encode one path segment — mirror of Cloudflare's
/// `encodePath`/`encodeURIComponent` (handler.ts): unreserved ASCII
/// (`A-Za-z0-9-_.!~*'()`) stays literal, every other byte is
/// `%XX` (uppercase). Redirect `Location`s are built from decoded
/// paths, so this is what keeps reserved filename characters
/// (`?`, `#`, `%`, `/`) meaning "part of the filename".
fn encode_segment(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for b in seg.bytes() {
        match b {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Substitute `:name` / `:splat` placeholders in one pass.
///
/// Shared by `_redirects` destinations and `_headers` values. Only
/// complete, longest-match tokens are recognized, and inserted capture
/// values are never rescanned — so `:id` cannot eat the prefix of
/// `:id2`, and a captured literal `:other` stays literal.
pub(crate) fn substitute_placeholders(template: &str, captures: &[(String, String)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(idx) = rest.find(':') {
        out.push_str(&rest[..idx]);
        let after = &rest[idx + 1..];
        let name_len = after
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
            .count();
        if name_len == 0 {
            out.push(':');
            rest = after;
            continue;
        }
        let name = &after[..name_len];
        match captures.iter().find(|(n, _)| n == name) {
            Some((_, value)) => out.push_str(value),
            None => {
                out.push(':');
                out.push_str(name);
            }
        }
        rest = &after[name_len..];
    }
    out.push_str(rest);
    out
}

/// `_headers` / `_redirects` are configuration, never served.
fn is_config_path(path: &str) -> bool {
    path == "/_headers" || path == "/_redirects"
}

/// Encode a decoded path segment by segment (Cloudflare `encodePath`).
pub fn encode_path(path: &str) -> String {
    path.split('/')
        .map(encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// Normalize a request path to manifest form: percent-DECODE per
/// segment first (manifest stores decoded file paths — Miniflare
/// `decodePath` does the same before lookup), then require leading
/// `/`, no empty/`.`/`..` components, no backslash/NUL. `Err(())` →
/// protocol 400.
pub fn normalize_asset_path(raw: &str) -> Result<String, ()> {
    if raw.is_empty() || raw.len() > 2048 || raw.contains(['\\', '\0']) {
        return Err(());
    }
    let mut out = String::with_capacity(raw.len() + 1);
    if !raw.starts_with('/') {
        out.push('/');
    }
    let trailing_slash = raw.ends_with('/') && raw.len() > 1;
    for (i, part) in raw.split('/').filter(|p| !p.is_empty()).enumerate() {
        let decoded = decode_segment(part);
        let part = decoded.as_str();
        if part == "." || part == ".." || part.contains(['\\', '\0']) {
            return Err(());
        }
        if i > 0 || !out.ends_with('/') {
            out.push('/');
        }
        out.push_str(part);
    }
    if trailing_slash && !out.ends_with('/') {
        out.push('/');
    }
    if out.is_empty() {
        out.push('/');
    }
    // Cloudflare `decodePath` collapses repeated slashes after decoding
    // (`.replace(/\/+/g, "/")`). A decoded `%2F` must not add a second
    // structural slash: `/%2Fevil.com` is `/evil.com`, never a
    // protocol-relative `//evil.com` redirect target.
    while out.contains("//") {
        out = out.replace("//", "/");
    }
    Ok(out)
}

/// Resolve `path` against `manifest` (pure — the serving decision the
/// Cloudflare-compatible subset makes).
pub fn resolve(manifest: &AssetManifest, path: &str) -> Resolved {
    resolve_with_nfh(manifest, path, manifest.not_found_handling)
}

/// Resolve with an explicit `not_found_handling` override — the
/// router's canFetch probe forces `none` for non-navigation requests
/// (Miniflare assets.worker.js canFetch semantics, T31c).
pub fn resolve_with_nfh(manifest: &AssetManifest, path: &str, nfh: NotFoundHandling) -> Resolved {
    match html_intent(manifest, path, nfh, false) {
        Intent::Serve(asset) => Resolved::Serve(asset),
        Intent::NotFoundPage(asset) => Resolved::NotFoundPage(asset),
        Intent::SpaFallback(asset) => Resolved::SpaFallback(asset),
        Intent::Redirect(target) => Resolved::Redirect(target),
        Intent::None => Resolved::NotFound,
    }
}

/// One html-handling resolution outcome — a port of
/// `Intent` in `workers-shared/asset-worker/src/handler.ts`:
/// either an asset to serve at the requested URL, or the canonical
/// URL to redirect to.
enum Intent {
    None,
    Serve(AssetFile),
    NotFoundPage(AssetFile),
    SpaFallback(AssetFile),
    Redirect(String),
}

impl Intent {
    /// The asset this intent would serve, if any (the redirect
    /// validator compares assets by content).
    fn asset(&self) -> Option<&AssetFile> {
        match self {
            Intent::Serve(a) | Intent::NotFoundPage(a) | Intent::SpaFallback(a) => Some(a),
            _ => None,
        }
    }
}

fn find_asset(manifest: &AssetManifest, path: &str) -> Option<AssetFile> {
    manifest.files.iter().find(|f| f.path == path).cloned()
}

fn not_found_intent(manifest: &AssetManifest, nfh: NotFoundHandling) -> Intent {
    match nfh {
        NotFoundHandling::None => Intent::None,
        NotFoundHandling::Page404 => {
            find_asset(manifest, "/404.html").map_or(Intent::None, Intent::NotFoundPage)
        }
        NotFoundHandling::SinglePageApplication => {
            find_asset(manifest, "/index.html").map_or(Intent::None, Intent::SpaFallback)
        }
    }
}

/// Cloudflare `safeRedirect`: only follow a canonical rewrite when the
/// destination is a free URL AND resolves (without further redirects)
/// to the very same asset. This keeps `/foo.html` reachable when
/// `/foo` is a different file, and refuses redirects whose target a
/// different asset shadows.
fn safe_redirect(
    manifest: &AssetManifest,
    file: &str,
    dest: &str,
    nfh: NotFoundHandling,
    skip: bool,
) -> Option<String> {
    if skip || dest.is_empty() || find_asset(manifest, dest).is_some() {
        return None;
    }
    let target = find_asset(manifest, file)?;
    let intent = html_intent(manifest, dest, nfh, true);
    let asset = intent.asset()?;
    (asset.sha256 == target.sha256).then(|| dest.to_string())
}

/// Mode-dispatching resolution, mirroring Cloudflare's `getIntent`.
fn html_intent(
    manifest: &AssetManifest,
    path: &str,
    nfh: NotFoundHandling,
    skip: bool,
) -> Intent {
    match manifest.html_handling {
        HtmlHandling::None => find_asset(manifest, path)
            .map_or_else(|| not_found_intent(manifest, nfh), Intent::Serve),
        HtmlHandling::AutoTrailingSlash => auto_trailing_slash(manifest, path, nfh, skip),
        HtmlHandling::ForceTrailingSlash => force_trailing_slash(manifest, path, nfh, skip),
        HtmlHandling::DropTrailingSlash => drop_trailing_slash(manifest, path, nfh, skip),
    }
}

/// Strip the literal `suffix` off `path`, keeping whatever precedes it
/// (`/foo/index` − `index` → `/foo/`, `/index` − `index` → `/`).
fn strip_keep_prefix<'a>(path: &'a str, suffix: &str) -> &'a str {
    &path[..path.len() - suffix.len()]
}

/// `auto-trailing-slash` — row for row from Cloudflare's
/// `htmlHandlingAutoTrailingSlash`.
fn auto_trailing_slash(
    manifest: &AssetManifest,
    path: &str,
    nfh: NotFoundHandling,
    skip: bool,
) -> Intent {
    let exact = find_asset(manifest, path);
    if path.ends_with("/index") {
        // A binary file literally named `/index` wins; only then are
        // the `.html` aliases considered.
        if let Some(f) = exact.clone() {
            return Intent::Serve(f);
        }
        let dir = strip_keep_prefix(path, "index"); // `/foo/` or `/`
        if let Some(dest) = safe_redirect(manifest, &format!("{path}.html"), dir, nfh, skip) {
            return Intent::Redirect(dest); // /foo/index.html → /foo/
        }
        let stem = dir.trim_end_matches('/');
        if let Some(dest) =
            safe_redirect(manifest, &format!("{stem}.html"), stem, nfh, skip)
        {
            return Intent::Redirect(dest); // /foo.html → /foo
        }
    } else if path.ends_with("/index.html") {
        let dir = strip_keep_prefix(path, "index.html"); // `/foo/` or `/`
        if let Some(dest) = safe_redirect(manifest, path, dir, nfh, skip) {
            return Intent::Redirect(dest); // /foo/index.html → /foo/
        }
        let stem = dir.trim_end_matches('/');
        if let Some(dest) =
            safe_redirect(manifest, &format!("{stem}.html"), stem, nfh, skip)
        {
            return Intent::Redirect(dest); // /foo.html → /foo
        }
    } else if path.ends_with('/') {
        if let Some(f) = find_asset(manifest, &format!("{path}index.html")) {
            return Intent::Serve(f);
        }
        let stem = path.trim_end_matches('/');
        if let Some(dest) =
            safe_redirect(manifest, &format!("{stem}.html"), stem, nfh, skip)
        {
            return Intent::Redirect(dest); // /foo/ → /foo
        }
    } else if let Some(stem) = path.strip_suffix(".html") {
        if let Some(dest) = safe_redirect(manifest, path, stem, nfh, skip) {
            return Intent::Redirect(dest); // /foo.html → /foo
        }
        if let Some(dest) = safe_redirect(
            manifest,
            &format!("{stem}/index.html"),
            &format!("{stem}/"),
            nfh,
            skip,
        ) {
            return Intent::Redirect(dest); // /foo.html → /foo/
        }
    }
    // Any path also resolves through its own `.html` / `/index.html`
    // aliases — this is what makes dotted stems (`/release-1.2` →
    // `/release-1.2.html`) and exact extensionless files reachable.
    if let Some(f) = exact {
        return Intent::Serve(f);
    }
    if let Some(f) = find_asset(manifest, &format!("{path}.html")) {
        return Intent::Serve(f);
    }
    if let Some(dest) = safe_redirect(
        manifest,
        &format!("{path}/index.html"),
        &format!("{path}/"),
        nfh,
        skip,
    ) {
        return Intent::Redirect(dest);
    }
    not_found_intent(manifest, nfh)
}

/// `force-trailing-slash` — Cloudflare's
/// `htmlHandlingForceTrailingSlash`.
fn force_trailing_slash(
    manifest: &AssetManifest,
    path: &str,
    nfh: NotFoundHandling,
    skip: bool,
) -> Intent {
    let exact = find_asset(manifest, path);
    if path.ends_with("/index") {
        if let Some(f) = exact.clone() {
            return Intent::Serve(f);
        }
        let dir = strip_keep_prefix(path, "index"); // `/foo/` or `/`
        if let Some(dest) = safe_redirect(manifest, &format!("{path}.html"), dir, nfh, skip) {
            return Intent::Redirect(dest);
        }
        let stem = dir.trim_end_matches('/');
        if let Some(dest) =
            safe_redirect(manifest, &format!("{stem}.html"), dir, nfh, skip)
        {
            return Intent::Redirect(dest);
        }
    } else if path.ends_with("/index.html") {
        let dir = strip_keep_prefix(path, "index.html"); // `/foo/` or `/`
        if let Some(dest) = safe_redirect(manifest, path, dir, nfh, skip) {
            return Intent::Redirect(dest);
        }
        let stem = dir.trim_end_matches('/');
        if let Some(dest) =
            safe_redirect(manifest, &format!("{stem}.html"), dir, nfh, skip)
        {
            return Intent::Redirect(dest);
        }
    } else if path.ends_with('/') {
        if let Some(f) = find_asset(manifest, &format!("{path}index.html")) {
            return Intent::Serve(f);
        }
        let stem = path.trim_end_matches('/');
        if let Some(f) = find_asset(manifest, &format!("{stem}.html")) {
            return Intent::Serve(f);
        }
    } else if let Some(stem) = path.strip_suffix(".html") {
        let slashed = format!("{stem}/");
        if let Some(dest) = safe_redirect(manifest, path, &slashed, nfh, skip) {
            return Intent::Redirect(dest);
        }
        // Both `/foo.html` and `/foo/index.html` exist: the rewrite
        // would land on the index, so serve the exact file here only.
        if let Some(f) = exact.clone() {
            return Intent::Serve(f);
        }
        if let Some(dest) = safe_redirect(
            manifest,
            &format!("{stem}/index.html"),
            &slashed,
            nfh,
            skip,
        ) {
            return Intent::Redirect(dest);
        }
    }
    if let Some(f) = exact {
        return Intent::Serve(f);
    }
    let slashed = format!("{path}/");
    if let Some(dest) = safe_redirect(manifest, &format!("{path}.html"), &slashed, nfh, skip) {
        return Intent::Redirect(dest);
    }
    if let Some(dest) = safe_redirect(
        manifest,
        &format!("{path}/index.html"),
        &slashed,
        nfh,
        skip,
    ) {
        return Intent::Redirect(dest);
    }
    not_found_intent(manifest, nfh)
}

/// `drop-trailing-slash` — Cloudflare's
/// `htmlHandlingDropTrailingSlash`.
fn drop_trailing_slash(
    manifest: &AssetManifest,
    path: &str,
    nfh: NotFoundHandling,
    skip: bool,
) -> Intent {
    let exact = find_asset(manifest, path);
    if path.ends_with("/index") {
        if let Some(f) = exact.clone() {
            return Intent::Serve(f);
        }
        let stem = strip_keep_prefix(path, "/index"); // `/foo` or ``
        if stem.is_empty() {
            // `/index` → `/`
            if let Some(dest) = safe_redirect(manifest, "/index.html", "/", nfh, skip) {
                return Intent::Redirect(dest);
            }
        } else {
            if let Some(dest) =
                safe_redirect(manifest, &format!("{stem}.html"), stem, nfh, skip)
            {
                return Intent::Redirect(dest);
            }
            if let Some(dest) =
                safe_redirect(manifest, &format!("{path}.html"), stem, nfh, skip)
            {
                return Intent::Redirect(dest);
            }
        }
    } else if path.ends_with("/index.html") {
        if path == "/index.html" {
            // Never drop the root slash.
            if let Some(dest) = safe_redirect(manifest, "/index.html", "/", nfh, skip) {
                return Intent::Redirect(dest);
            }
        } else {
            let stem = strip_keep_prefix(path, "/index.html");
            if let Some(dest) = safe_redirect(manifest, path, stem, nfh, skip) {
                return Intent::Redirect(dest);
            }
            // Both files exist: the rewrite would serve `/foo.html`, so
            // `/foo/index.html` stays reachable at its exact URL.
            if let Some(f) = exact.clone() {
                return Intent::Serve(f);
            }
            if let Some(dest) =
                safe_redirect(manifest, &format!("{stem}.html"), stem, nfh, skip)
            {
                return Intent::Redirect(dest);
            }
        }
    } else if path.ends_with('/') {
        if path == "/" {
            if let Some(f) = find_asset(manifest, "/index.html") {
                return Intent::Serve(f);
            }
        } else {
            let stem = path.trim_end_matches('/');
            if let Some(dest) =
                safe_redirect(manifest, &format!("{stem}.html"), stem, nfh, skip)
            {
                return Intent::Redirect(dest);
            }
            if let Some(dest) =
                safe_redirect(manifest, &format!("{stem}/index.html"), stem, nfh, skip)
            {
                return Intent::Redirect(dest);
            }
        }
    } else if let Some(stem) = path.strip_suffix(".html") {
        if let Some(dest) = safe_redirect(manifest, path, stem, nfh, skip) {
            return Intent::Redirect(dest);
        }
        if let Some(dest) = safe_redirect(
            manifest,
            &format!("{stem}/index.html"),
            stem,
            nfh,
            skip,
        ) {
            return Intent::Redirect(dest);
        }
    }
    if let Some(f) = exact {
        return Intent::Serve(f);
    }
    if let Some(f) = find_asset(manifest, &format!("{path}.html")) {
        return Intent::Serve(f);
    }
    if let Some(f) = find_asset(manifest, &format!("{path}/index.html")) {
        return Intent::Serve(f);
    }
    not_found_intent(manifest, nfh)
}

/// Content-addressed asset store rooted at `dir` (provider-shared with
/// the materializer). All paths are digest-derived — traversal is
/// impossible by construction.
#[derive(Debug, Clone)]
pub struct AssetStore {
    root: PathBuf,
    /// Parsed `_redirects` per (tenant, file digest) — a manifest's rules
    /// are immutable, so the cache is keyed by content hash. Bounded by a
    /// clear-on-overflow policy (manifests churn slowly).
    redirect_cache: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<Vec<RedirectRule>>>>,
    >,
    /// Parsed `_headers` per (tenant, file digest), same policy.
    header_cache: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<Vec<HeaderRule>>>>,
    >,
}

fn is_lower_hex(value: &str, bytes: usize) -> bool {
    value.len() == bytes
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

impl AssetStore {
    pub fn open(root: &Path) -> Result<Self, String> {
        if !root.exists() {
            std::fs::create_dir_all(root).map_err(|e| format!("asset store: {e}"))?;
        }
        Ok(Self {
            root: root.to_path_buf(),
            redirect_cache: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            header_cache: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        })
    }

    fn tenant_dir(&self, tenant: &str) -> PathBuf {
        self.root.join(vrt_securefs::segment_encode(tenant))
    }

    fn manifest_path(&self, tenant: &str, digest: &str) -> Option<PathBuf> {
        if !is_lower_hex(digest, 64) {
            return None;
        }
        Some(
            self.tenant_dir(tenant)
                .join("manifests")
                .join(format!("{digest}.json")),
        )
    }

    fn object_path(&self, tenant: &str, digest: &str) -> Option<PathBuf> {
        if !is_lower_hex(digest, 64) {
            return None;
        }
        Some(self.tenant_dir(tenant).join("objects").join(digest))
    }

    /// Read + verify a manifest by digest. Any read/parse/digest
    /// failure is `Err` — callers map to `ASSET_STORAGE_UNAVAILABLE`;
    /// absent is `Ok(None)`; `fetch` maps it to 503 (fail closed).
    pub fn manifest(&self, tenant: &str, digest: &str) -> Result<Option<AssetManifest>, String> {
        let Some(path) = self.manifest_path(tenant, digest) else {
            return Err("manifest digest malformed".into());
        };
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(format!("manifest read: {e}")),
        };
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err("manifest exceeds cap".into());
        }
        if sha256_hex(&bytes) != digest {
            return Err("manifest digest mismatch (poisoned store)".into());
        }
        let manifest: AssetManifest =
            serde_json::from_slice(&bytes).map_err(|_| "manifest unparseable".to_string())?;
        if manifest.files.len() > MAX_MANIFEST_FILES {
            return Err("manifest file count exceeds cap".into());
        }
        // Re-validate every entry — the store is shared state, so a
        // forged manifest must fail closed here even if upload
        // validation was bypassed.
        for file in &manifest.files {
            if normalize_asset_path(&file.path).is_err()
                || normalize_asset_path(&file.path).unwrap() != file.path
                || !is_lower_hex(&file.sha256, 64)
                || file.size > MAX_ASSET_OBJECT_BYTES
                || file.mime.is_empty()
                || file.mime.len() > 128
                || !file.mime.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
            {
                return Err("manifest entry invalid".into());
            }
        }
        Ok(Some(manifest))
    }

    /// Read + verify one object. `expect` is the manifest-declared
    /// digest+size — both must match or the store is poisoned.
    pub fn object(&self, tenant: &str, expect: &AssetFile) -> Result<Option<Vec<u8>>, String> {
        let Some(path) = self.object_path(tenant, &expect.sha256) else {
            return Err("object digest malformed".into());
        };
        let meta = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(format!("object stat: {e}")),
        };
        if meta.len() > MAX_ASSET_OBJECT_BYTES || meta.len() != expect.size {
            return Err("object size mismatch (poisoned store)".into());
        }
        let bytes = std::fs::read(&path).map_err(|e| format!("object read: {e}"))?;
        if sha256_hex(&bytes) != expect.sha256 {
            return Err("object digest mismatch (poisoned store)".into());
        }
        Ok(Some(bytes))
    }

    /// Load + parse the manifest's `/_redirects` file. `None` when the
    /// manifest has no such file. The parsed rules are immutable for a
    /// content digest and cached; an unreadable/oversized file is an
    /// error (fail closed — routing must not silently ignore redirects).
    pub fn redirect_rules(
        &self,
        tenant: &str,
        manifest: &AssetManifest,
    ) -> Result<Option<std::sync::Arc<Vec<RedirectRule>>>, String> {
        let Some(file) = manifest.files.iter().find(|f| f.path == "/_redirects") else {
            return Ok(None);
        };
        if file.size as usize > assets_redirects::MAX_FILE_BYTES {
            return Err("_redirects exceeds the parse cap".into());
        }
        let key = format!("{tenant}:{}", file.sha256);
        {
            let cache = self
                .redirect_cache
                .lock()
                .map_err(|_| "redirect cache poisoned".to_string())?;
            if let Some(hit) = cache.get(&key) {
                return Ok(Some(hit.clone()));
            }
        }
        let Some(bytes) = self.object(tenant, file)? else {
            return Err("_redirects object missing from the store".into());
        };
        if bytes.len() > assets_redirects::MAX_FILE_BYTES {
            return Err("_redirects exceeds the parse cap".into());
        }
        let text =
            String::from_utf8(bytes).map_err(|_| "_redirects is not valid UTF-8".to_string())?;
        let rules = std::sync::Arc::new(assets_redirects::parse_redirects(&text));
        let mut cache = self
            .redirect_cache
            .lock()
            .map_err(|_| "redirect cache poisoned".to_string())?;
        if cache.len() >= 64 {
            cache.clear();
        }
        cache.insert(key, rules.clone());
        Ok(Some(rules))
    }

    /// Load + parse the manifest's `/_headers` file. `None` when absent;
    /// unreadable/oversized is an error (fail closed).
    pub fn header_rules(
        &self,
        tenant: &str,
        manifest: &AssetManifest,
    ) -> Result<Option<std::sync::Arc<Vec<HeaderRule>>>, String> {
        let Some(file) = manifest.files.iter().find(|f| f.path == "/_headers") else {
            return Ok(None);
        };
        if file.size as usize > assets_headers::MAX_FILE_BYTES {
            return Err("_headers exceeds the parse cap".into());
        }
        let key = format!("{tenant}:{}", file.sha256);
        {
            let cache = self
                .header_cache
                .lock()
                .map_err(|_| "header cache poisoned".to_string())?;
            if let Some(hit) = cache.get(&key) {
                return Ok(Some(hit.clone()));
            }
        }
        let Some(bytes) = self.object(tenant, file)? else {
            return Err("_headers object missing from the store".into());
        };
        if bytes.len() > assets_headers::MAX_FILE_BYTES {
            return Err("_headers exceeds the parse cap".into());
        }
        let text =
            String::from_utf8(bytes).map_err(|_| "_headers is not valid UTF-8".to_string())?;
        let rules = std::sync::Arc::new(assets_headers::parse_headers(&text));
        let mut cache = self
            .header_cache
            .lock()
            .map_err(|_| "header cache poisoned".to_string())?;
        if cache.len() >= 64 {
            cache.clear();
        }
        cache.insert(key, rules.clone());
        Ok(Some(rules))
    }

    /// Write object bytes after verifying `sha256(bytes) == digest`;
    /// atomic rename — readers never see a partial file.
    pub fn put_object_verified(
        &self,
        tenant: &str,
        digest: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        if sha256_hex(bytes) != digest {
            return Err("object digest mismatch on write".into());
        }
        let path = self
            .object_path(tenant, digest)
            .ok_or_else(|| "object digest malformed".to_string())?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("object dir: {e}"))?;
        }
        vrt_securefs::atomic_write(&path, bytes, 0o600).map_err(|e| format!("object write: {e}"))
    }

    /// Write manifest bytes after verifying `sha256(bytes) == digest`.
    pub fn put_manifest_verified(
        &self,
        tenant: &str,
        digest: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        if sha256_hex(bytes) != digest {
            return Err("manifest digest mismatch on write".into());
        }
        let path = self
            .manifest_path(tenant, digest)
            .ok_or_else(|| "manifest digest malformed".to_string())?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("manifest dir: {e}"))?;
        }
        vrt_securefs::atomic_write(&path, bytes, 0o600).map_err(|e| format!("manifest write: {e}"))
    }
}

/// RV-R53.5: mark-and-sweep grace window — unreferenced manifests and
/// objects younger than this are kept, covering the control-plane
/// rollback window and any in-flight materialization.
pub const ASSET_GC_GRACE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);
/// Bounded deletions per sweep round.
pub const ASSET_GC_MAX_PER_ROUND: usize = 256;

/// One sweep's accounting — surfaced as metrics + structured logs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AssetGcReport {
    /// Candidate manifests+objects inspected.
    pub scanned: u64,
    pub manifests_deleted: u64,
    pub objects_deleted: u64,
    /// Unreferenced but inside the grace window — kept.
    pub grace_kept: u64,
    /// Per-file stat/delete failures — isolated, retried next round.
    pub failures: u64,
}

impl AssetStore {
    /// RV-R53.5: mark-and-sweep GC for the content-addressed store.
    /// `keep` maps tenant → manifest digests the currently applied
    /// specs reference. Unreferenced manifests and objects whose
    /// mtime is older than `grace` are deleted — at most `max`
    /// deletions per round; the rest wait for the next sweep (the
    /// store is append-only otherwise, so progress is monotone).
    /// Entries with non-digest names (in-flight atomic-write temp
    /// files, foreign data) are never touched. A manifest that cannot
    /// be read aborts the object sweep for that tenant — fail closed,
    /// never collect bytes a live manifest may still reference.
    pub fn gc_sweep(
        &self,
        keep: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
        grace: std::time::Duration,
        max: usize,
    ) -> AssetGcReport {
        let mut out = AssetGcReport::default();
        let now = std::time::SystemTime::now();
        let mut budget = max;
        let Ok(tenants) = std::fs::read_dir(&self.root) else {
            return out;
        };
        'tenants: for t in tenants.flatten() {
            if budget == 0 {
                break;
            }
            if !t.file_type().map(|f| f.is_dir()).unwrap_or(false) {
                continue;
            }
            let Some(tenant) = vrt_securefs::segment_decode(&t.file_name().to_string_lossy())
            else {
                continue; // foreign/undecodable dir — not ours, skip
            };
            let dir = t.path();
            let keep_set = keep.get(&tenant);
            let older_than_grace = |p: &Path| -> Option<bool> {
                match std::fs::metadata(p).and_then(|m| m.modified()) {
                    Ok(mt) => Some(now.duration_since(mt).unwrap_or_default() > grace),
                    Err(_) => None, // stat failed — counted by caller
                }
            };
            // 1) unreferenced manifests past grace
            let mdir = dir.join("manifests");
            if let Ok(entries) = std::fs::read_dir(&mdir) {
                for e in entries.flatten() {
                    if budget == 0 {
                        break 'tenants;
                    }
                    let name = e.file_name().to_string_lossy().into_owned();
                    let Some(digest) = name.strip_suffix(".json") else {
                        continue; // temp/foreign file — never collect
                    };
                    if !is_lower_hex(digest, 64) || keep_set.is_some_and(|k| k.contains(digest)) {
                        continue;
                    }
                    out.scanned += 1;
                    match older_than_grace(&e.path()) {
                        Some(true) => {
                            if std::fs::remove_file(e.path()).is_ok() {
                                out.manifests_deleted += 1;
                                budget -= 1;
                            } else {
                                out.failures += 1;
                            }
                        }
                        Some(false) => out.grace_kept += 1,
                        None => out.failures += 1,
                    }
                }
            }
            // 2) objects referenced by surviving kept manifests
            let mut protected = std::collections::BTreeSet::new();
            let mut manifest_err = false;
            if let Some(k) = keep_set {
                for d in k {
                    match self.manifest(&tenant, d) {
                        Ok(Some(m)) => protected.extend(m.files.iter().map(|f| f.sha256.clone())),
                        Ok(None) => {} // referenced but absent — nothing protects
                        Err(_) => manifest_err = true,
                    }
                }
            }
            if manifest_err {
                continue; // fail closed — next round retries
            }
            let odir = dir.join("objects");
            if let Ok(entries) = std::fs::read_dir(&odir) {
                for e in entries.flatten() {
                    if budget == 0 {
                        break 'tenants;
                    }
                    let name = e.file_name().to_string_lossy().into_owned();
                    if !is_lower_hex(&name, 64) || protected.contains(&name) {
                        continue; // digest-named only — temps untouched
                    }
                    out.scanned += 1;
                    match older_than_grace(&e.path()) {
                        Some(true) => {
                            if std::fs::remove_file(e.path()).is_ok() {
                                out.objects_deleted += 1;
                                budget -= 1;
                            } else {
                                out.failures += 1;
                            }
                        }
                        Some(false) => out.grace_kept += 1,
                        None => out.failures += 1,
                    }
                }
            }
        }
        out
    }
}

// ------------------------------------------------------------------
// /internal/assets/v1/fetch
// ------------------------------------------------------------------

fn typed(code: &'static str, status: StatusCode) -> Response<Body> {
    crate::error::error_response(code, status, true, false)
}

/// Internal response header that lists the header names `_headers`
/// touched (set or detached). The assets facade forwards exactly these
/// names — never arbitrary backend headers — after stripping the
/// internal prefix. Bounded to keep the value small.
pub const CUSTOM_HEADER_NAMES: &str = "x-open-compute-asset-header-names";
/// Same bounds the deployment validator enforces
/// (`assets_headers::validate_file`), so an accepted `_headers` can
/// never make the forwarding list overflow here.
const MAX_CUSTOM_HEADER_NAMES: usize = assets_headers::MAX_FORWARDED_NAMES;
const MAX_CUSTOM_HEADER_NAME_BYTES: usize = assets_headers::MAX_FORWARDED_NAME_BYTES;

/// Apply the manifest's `_headers` to one response, then advertise the
/// touched names for the facade.
///
/// Cloudflare applies `_headers` to *every* response its asset handler
/// produces — `attachCustomHeaders` is the single return of
/// `handleRequest` (`asset-worker/src/handler.ts:324-364`), so served
/// assets, 304s, html-handling 307s and authored `_redirects` 3xx are
/// all covered. "Redirects are applied before headers" describes the
/// routing order, not the set of responses the header rules reach.
fn apply_header_rules(response: &mut Response<Body>, rules: &[HeaderRule], path: &str) {
    let names = assets_headers::apply_headers(rules, path, response.headers_mut());
    attach_custom_header_names(response, names);
}

fn attach_custom_header_names(response: &mut Response<Body>, names: Vec<String>) {
    if names.is_empty() {
        return;
    }
    let joined = names.join(",");
    if names.len() > MAX_CUSTOM_HEADER_NAMES || joined.len() > MAX_CUSTOM_HEADER_NAME_BYTES {
        // Deployment validation rejects such a file, so this only fires
        // for a manifest published before validation existed.
        tracing::warn!(
            names = names.len(),
            bytes = joined.len(),
            "assets: _headers names list over the forwarding bound; dropping it"
        );
        return;
    }
    if let Ok(value) = HeaderValue::from_str(&joined) {
        response.headers_mut().insert(CUSTOM_HEADER_NAMES, value);
    }
}

/// `POST /internal/assets/v1/fetch` — identity is already authorized
/// by the caller (`BindingContext`); this resolves + serves the bytes.
pub fn fetch(
    store: &AssetStore,
    tenant: &str,
    manifest_digest: &str,
    request: &Request<Body>,
) -> Response<Body> {
    let header_str = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    let method = header_str("x-open-compute-asset-method")
        .unwrap_or_default()
        .to_ascii_uppercase();
    if method != "GET" && method != "HEAD" {
        return typed(
            "BINDING_CAPABILITY_UNSUPPORTED",
            StatusCode::UNPROCESSABLE_ENTITY,
        );
    }
    let url = header_str("x-open-compute-asset-url").unwrap_or_default();
    // `search` (with `?`) is carried through html-handling redirects
    // verbatim — Cloudflare appends the original `url.search` to its
    // encoded destination — and `_redirects` forwards `query` when the
    // destination does not carry its own. A fragment never reaches the
    // origin.
    let url = url.split('#').next().unwrap_or_default();
    let (raw_path, search) = match url.find('?') {
        Some(i) => (&url[..i], &url[i..]),
        None => (url, ""),
    };
    let query = search.strip_prefix('?').filter(|q| !q.is_empty());
    let raw_path = raw_path
        .find("://")
        .map(|i| {
            raw_path[i + 3..]
                .find('/')
                .map(|p| &raw_path[i + 3 + p..])
                .unwrap_or("/")
        })
        .unwrap_or(raw_path);
    let Ok(path) = normalize_asset_path(raw_path) else {
        return typed("BINDING_PROTOCOL_ERROR", StatusCode::BAD_REQUEST);
    };

    let manifest = match store.manifest(tenant, manifest_digest) {
        Ok(Some(m)) => m,
        // Fail closed: the binding pins this digest, so an absent
        // manifest is storage damage / missing upload, not a missing
        // asset. ASSET_NOT_FOUND is reserved for `Resolved::NotFound`.
        Ok(None) => {
            tracing::warn!(
                manifest_digest = %manifest_digest,
                "assets fetch: pinned manifest missing from store"
            );
            crate::metrics::assets_outcome(&crate::metrics::worker_label(request), "error");
            return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE);
        }
        Err(_) => return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE),
    };
    // B12: outcome label decided by the resolution + served status.
    let worker_id = crate::metrics::worker_label(request);
    // `_headers` / `_redirects` are configuration files: Cloudflare
    // never serves them.
    if is_config_path(&path) {
        crate::metrics::assets_outcome(&worker_id, "not_found");
        return typed("ASSET_NOT_FOUND", StatusCode::NOT_FOUND);
    }
    let redirect_rules = match store.redirect_rules(tenant, &manifest) {
        Ok(rules) => rules,
        Err(e) => {
            tracing::warn!(
                manifest_digest = %manifest_digest,
                error = %e,
                "assets fetch: _redirects unreadable"
            );
            crate::metrics::assets_outcome(&worker_id, "error");
            return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    let header_rules = match store.header_rules(tenant, &manifest) {
        Ok(rules) => rules,
        Err(e) => {
            tracing::warn!(
                manifest_digest = %manifest_digest,
                error = %e,
                "assets fetch: _headers unreadable"
            );
            crate::metrics::assets_outcome(&worker_id, "error");
            return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    // `_redirects` matches the RAW (still percent-encoded) request path:
    // captures are inserted into `Location` verbatim, so an encoded `/`
    // (`%2F`), `?` (`%3F`) or `#` (`%23`) keeps its filename meaning
    // instead of turning into a separator, query or fragment.
    let mut proxied = false;
    let resolved = match redirect_rules
        .as_deref()
        .and_then(|rules| assets_redirects::match_redirect(rules, raw_path, query))
    {
        Some(RedirectAction::Redirect { status, location }) => {
            let Ok(code) = StatusCode::from_u16(status) else {
                crate::metrics::assets_outcome(&worker_id, "error");
                return typed("ASSET_INTERNAL_ERROR", StatusCode::INTERNAL_SERVER_ERROR);
            };
            let Ok(location) = HeaderValue::from_str(&location) else {
                crate::metrics::assets_outcome(&worker_id, "error");
                return typed("ASSET_INTERNAL_ERROR", StatusCode::INTERNAL_SERVER_ERROR);
            };
            let mut r = Response::new(Body::empty());
            *r.status_mut() = code;
            r.headers_mut().insert(header::LOCATION, location);
            // Cloudflare routes the authored redirect through the same
            // `attachCustomHeaders` tail, so `_headers` applies here too.
            if let Some(rules) = header_rules.as_deref() {
                apply_header_rules(&mut r, rules, &path);
            }
            crate::metrics::assets_outcome(&worker_id, "redirect");
            return r;
        }
        // `200` proxying: serve a same-manifest path, never recursing
        // through the redirect table again. A missing target is a 404,
        // not the manifest's not-found fallback.
        Some(RedirectAction::Proxy(target)) => {
            proxied = true;
            // The destination is an internal path, not a client request:
            // decode it (manifest paths are decoded), apply the same
            // configuration-file exclusion as for the request path, and
            // serve an exact file as-is (no `.html` canonical redirect).
            match normalize_asset_path(&target) {
                Ok(target) if !is_config_path(&target) => {
                    match manifest.files.iter().find(|f| f.path == target) {
                        Some(file) => Resolved::Serve(file.clone()),
                        None => resolve_with_nfh(&manifest, &target, NotFoundHandling::None),
                    }
                }
                _ => Resolved::NotFound,
            }
        }
        None => resolve(&manifest, &path),
    };
    // Cloudflare re-encodes the canonical path and redirects when it
    // differs from the request pathname (`/a b.txt` → `/a%20b.txt`),
    // combined with html-handling so a request only redirects once. A
    // `200` rewrite keeps the client URL untouched.
    let canonical_path = encode_path(&path);
    let needs_encode_redirect = !proxied
        && !raw_path.is_empty()
        && canonical_path != raw_path
        && !matches!(resolved, Resolved::NotFound | Resolved::Redirect(_));
    let redirect_location = |target: &str| {
        let mut r = Response::new(Body::empty());
        // Cloudflare's html-handling redirects are 307 (method and
        // body preserved), not 308.
        *r.status_mut() = StatusCode::TEMPORARY_REDIRECT;
        // Every html-handling target is origin-relative; never emit a
        // protocol-relative (`//host`) Location, which a browser would
        // resolve against the network path.
        let target = if target.starts_with("//") {
            format!("/{}", target.trim_start_matches('/'))
        } else {
            target.to_string()
        };
        let location = format!("{target}{search}");
        if let Ok(v) = HeaderValue::from_str(&location) {
            r.headers_mut().insert(header::LOCATION, v);
        }
        r
    };
    let outcome = if needs_encode_redirect {
        "redirect"
    } else {
        match &resolved {
            Resolved::SpaFallback(_) => "spa_fallback",
            Resolved::Redirect(_) => "redirect",
            Resolved::NotFound => "not_found",
            Resolved::Serve(_) => "hit",
            Resolved::NotFoundPage(_) => "not_found",
        }
    };
    let is_404_page = matches!(resolved, Resolved::NotFoundPage(_));
    // Cloudflare's `attachCustomHeaders` tail covers every asset response,
    // including html-handling 307s (`handler.test.ts` "Custom headers are
    // applied even to redirect responses"). Only the typed protocol 404 —
    // an internal error surface, not an asset response — bypasses it.
    let header_applicable = !matches!(resolved, Resolved::NotFound);
    let mut response = match resolved {
        Resolved::Redirect(to) => redirect_location(&encode_path(&to)),
        Resolved::NotFound => typed("ASSET_NOT_FOUND", StatusCode::NOT_FOUND),
        Resolved::Serve(_) | Resolved::SpaFallback(_) | Resolved::NotFoundPage(_)
            if needs_encode_redirect =>
        {
            redirect_location(&canonical_path)
        }
        Resolved::Serve(file) | Resolved::SpaFallback(file) | Resolved::NotFoundPage(file) => {
            let bytes = match store.object(tenant, &file) {
                Ok(Some(b)) => b,
                Ok(None) => {
                    // Manifest lists the file but the object is gone:
                    // storage damage, never a plain miss.
                    tracing::warn!(
                        manifest_digest = %manifest_digest,
                        object_sha256 = %file.sha256,
                        "assets fetch: manifest-listed object missing from store"
                    );
                    return {
                        crate::metrics::assets_outcome(&worker_id, "error");
                        typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE)
                    };
                }
                Err(_) => {
                    return {
                        crate::metrics::assets_outcome(&worker_id, "error");
                        typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE)
                    }
                }
            };
            let etag = format!("\"{}\"", file.sha256);
            if !is_404_page
                && header_str("if-none-match")
                    .map(|v| v.split(',').any(|t| t.trim() == etag || t.trim() == "*"))
                    .unwrap_or(false)
            {
                let mut r = Response::new(Body::empty());
                *r.status_mut() = StatusCode::NOT_MODIFIED;
                if let Ok(v) = HeaderValue::from_str(&etag) {
                    r.headers_mut().insert(header::ETAG, v);
                }
                if let Some(rules) = header_rules.as_deref() {
                    apply_header_rules(&mut r, rules, &path);
                }
                crate::metrics::assets_outcome(&worker_id, "not_modified");
                return r;
            }
            let mut r = Response::new(if method == "HEAD" {
                Body::empty()
            } else {
                Body::from(bytes)
            });
            *r.status_mut() = if is_404_page { StatusCode::NOT_FOUND } else { StatusCode::OK };
            if let Ok(v) = HeaderValue::from_str(&file.mime) {
                r.headers_mut().insert(header::CONTENT_TYPE, v);
            }
            if let Ok(v) = HeaderValue::from_str(&etag) {
                r.headers_mut().insert(header::ETAG, v);
            }
            if let Ok(v) = HeaderValue::from_str(&file.size.to_string()) {
                r.headers_mut().insert(header::CONTENT_LENGTH, v);
            }
            r
        }
    };
    if header_applicable {
        if let Some(rules) = header_rules.as_deref() {
            apply_header_rules(&mut response, rules, &path);
        }
    }
    if matches!(response.status(), StatusCode::NOT_FOUND) && outcome == "not_found" {
        crate::metrics::assets_outcome(&worker_id, "not_found");
    } else if !response.status().is_success() && !response.status().is_redirection() {
        crate::metrics::assets_outcome(&worker_id, "error");
    } else {
        crate::metrics::assets_outcome(&worker_id, outcome);
    }
    response
}

// ------------------------------------------------------------------
// T31c: router endpoints — /internal/assets/v1/{route-index,can-fetch}
// ------------------------------------------------------------------

/// Route-index payload bound — mirrored by facade-router
/// `MAX_INDEX_PATHS`; both sides must move together (facade.md).
pub const MAX_ROUTE_INDEX_PATHS: usize = 4096;
/// Same pairing as `MAX_INDEX_BYTES` — approx index size bound.
pub const MAX_ROUTE_INDEX_BYTES: usize = 4 * 1024 * 1024;

fn nfh_wire(nfh: NotFoundHandling) -> &'static str {
    match nfh {
        NotFoundHandling::None => "none",
        NotFoundHandling::Page404 => "404-page",
        NotFoundHandling::SinglePageApplication => "single-page-application",
    }
}

fn html_wire(html: HtmlHandling) -> &'static str {
    match html {
        HtmlHandling::AutoTrailingSlash => "auto-trailing-slash",
        HtmlHandling::ForceTrailingSlash => "force-trailing-slash",
        HtmlHandling::DropTrailingSlash => "drop-trailing-slash",
        HtmlHandling::None => "none",
    }
}

/// Effective not-found handling for a canFetch-style probe —
/// Miniflare `unstable_canFetch` (assets.worker.js:562–573): when the
/// request neither carries static routing nor is a browser navigation
/// (`Sec-Fetch-Mode: navigate`), the not-found fallbacks do not make
/// an asset "fetchable" — force `none`.
pub fn probe_nfh(
    manifest: &AssetManifest,
    has_static_routing: bool,
    navigate: bool,
) -> NotFoundHandling {
    if has_static_routing || navigate {
        manifest.not_found_handling
    } else {
        NotFoundHandling::None
    }
}

/// `POST /internal/assets/v1/route-index` — the bounded path index a
/// router caches per manifest digest: file paths + the two handling
/// modes. No bytes, no per-file metadata — the router only needs
/// existence checks for its routing decision; serving still goes
/// through `fetch`.
pub fn route_index(
    store: &AssetStore,
    tenant: &str,
    manifest_digest: &str,
    request: &Request<Body>,
) -> Response<Body> {
    let worker_id = crate::metrics::worker_label(request);
    let manifest = match store.manifest(tenant, manifest_digest) {
        Ok(Some(m)) => m,
        Ok(None) => {
            crate::metrics::assets_outcome(&worker_id, "route_error");
            return typed("ASSET_NOT_FOUND", StatusCode::NOT_FOUND);
        }
        Err(_) => {
            crate::metrics::assets_outcome(&worker_id, "route_error");
            return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    // Pairing with facade-router `MAX_INDEX_PATHS` / `MAX_INDEX_BYTES`:
    // over the cacheable bound we answer `tooLarge` WITHOUT the path
    // list — the router degrades to per-request `can-fetch` probes
    // (outcome `probe`) instead of buffering multi-MB JSON on the
    // digest's first request. Not an error; not fail-closed.
    let approx_bytes: usize = 64
        + manifest
            .files
            .iter()
            .map(|f| f.path.len() + 8)
            .sum::<usize>();
    let redirects = match store.redirect_rules(tenant, &manifest) {
        Ok(rules) => rules,
        Err(_) => {
            crate::metrics::assets_outcome(&worker_id, "route_error");
            return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    let redirect_entries: Vec<serde_json::Value> = redirects
        .as_deref()
        .map(|rules| {
            rules
                .iter()
                .map(|r| serde_json::json!({"source": r.source, "status": r.status}))
                .collect()
        })
        .unwrap_or_default();
    let approx_bytes = approx_bytes
        + redirect_entries
            .iter()
            .map(|v| v.to_string().len())
            .sum::<usize>();
    let too_large =
        manifest.files.len() > MAX_ROUTE_INDEX_PATHS || approx_bytes > MAX_ROUTE_INDEX_BYTES;
    // E3: every response names the manifest it describes — the router
    // refuses an index whose digest does not match its cfg binding.
    let body_doc = if too_large {
        serde_json::json!({
            "htmlHandling": html_wire(manifest.html_handling),
            "notFoundHandling": nfh_wire(manifest.not_found_handling),
            "manifestDigest": manifest_digest,
            "tooLarge": true,
        })
    } else {
        let paths: Vec<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
        serde_json::json!({
            "htmlHandling": html_wire(manifest.html_handling),
            "notFoundHandling": nfh_wire(manifest.not_found_handling),
            "manifestDigest": manifest_digest,
            "paths": paths,
            "redirects": redirect_entries,
        })
    };
    let body = match serde_json::to_vec(&body_doc) {
        Ok(b) => b,
        Err(_) => {
            crate::metrics::assets_outcome(&worker_id, "route_error");
            return typed("ASSET_INTERNAL_ERROR", StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    crate::metrics::assets_outcome(&worker_id, "route_index");
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = StatusCode::OK;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

/// `POST /internal/assets/v1/can-fetch` — single-request existence
/// probe used when the router's index cache cannot hold this manifest
/// (degradation path; counted as outcome `probe`). Semantics mirror
/// Miniflare `unstable_canFetch`: `has_static_routing` or a navigation
/// request keeps the manifest not-found handling, anything else forces
/// `none`; non-GET/HEAD is never fetchable.
pub fn can_fetch(
    store: &AssetStore,
    tenant: &str,
    manifest_digest: &str,
    request: &Request<Body>,
) -> Response<Body> {
    let worker_id = crate::metrics::worker_label(request);
    let header_str = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    let method = header_str("x-open-compute-asset-method")
        .unwrap_or_default()
        .to_ascii_uppercase();
    let url = header_str("x-open-compute-asset-url").unwrap_or_default();
    let has_static_routing = header_str("x-open-compute-asset-has-static-routing")
        .map(|v| v == "true")
        .unwrap_or(false);
    let navigate = header_str("sec-fetch-mode")
        .map(|v| v == "navigate")
        .unwrap_or(false);

    let fetchable = if method != "GET" && method != "HEAD" {
        false
    } else {
        let raw_path = url.split(['?', '#']).next().unwrap_or_default();
        let raw_path = raw_path
            .find("://")
            .map(|i| {
                raw_path[i + 3..]
                    .find('/')
                    .map(|p| &raw_path[i + 3 + p..])
                    .unwrap_or("/")
            })
            .unwrap_or(raw_path);
        match normalize_asset_path(raw_path) {
            Err(()) => false,
            Ok(path) => match store.manifest(tenant, manifest_digest) {
                Ok(Some(manifest)) => {
                    if is_config_path(&path) {
                        false
                    } else {
                        // Redirect rules match the raw request path, the
                        // same input `fetch` gives them.
                        let redirect_hit = match store.redirect_rules(tenant, &manifest) {
                            Ok(rules) => rules.as_deref().is_some_and(|rules| {
                                assets_redirects::match_redirect(rules, raw_path, None).is_some()
                            }),
                            Err(_) => {
                                crate::metrics::assets_outcome(&worker_id, "route_error");
                                return typed(
                                    "ASSET_STORAGE_UNAVAILABLE",
                                    StatusCode::SERVICE_UNAVAILABLE,
                                );
                            }
                        };
                        if redirect_hit {
                            true
                        } else {
                            let nfh = probe_nfh(&manifest, has_static_routing, navigate);
                            !matches!(resolve_with_nfh(&manifest, &path, nfh), Resolved::NotFound)
                        }
                    }
                }
                Ok(None) => {
                    crate::metrics::assets_outcome(&worker_id, "route_error");
                    return typed("ASSET_NOT_FOUND", StatusCode::NOT_FOUND);
                }
                Err(_) => {
                    crate::metrics::assets_outcome(&worker_id, "route_error");
                    return typed("ASSET_STORAGE_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE);
                }
            },
        }
    };
    crate::metrics::assets_outcome(&worker_id, "probe");
    let mut r = Response::new(Body::from(if fetchable {
        b"true".to_vec()
    } else {
        b"false".to_vec()
    }));
    *r.status_mut() = StatusCode::OK;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    r
}

#[cfg(test)]
mod normalize_tests {
    use super::normalize_asset_path;

    /// T31d/R4: per-segment percent decode before match (Miniflare
    /// decodePath parity) — encoded space/CJK resolve to decoded
    /// manifest paths; malformed escapes keep the raw segment; decoded
    /// separators split; decoded traversal still fails closed.
    #[test]
    fn percent_decode_before_normalize() {
        assert_eq!(
            normalize_asset_path("/my%20file.txt").unwrap(),
            "/my file.txt"
        );
        assert_eq!(
            normalize_asset_path("/%E4%B8%AD%E6%96%87%20page.html").unwrap(),
            "/\u{4e2d}\u{6587} page.html"
        );
        // Malformed escape keeps raw — never panics, never matches.
        assert_eq!(normalize_asset_path("/bad%zzfile").unwrap(), "/bad%zzfile");
        assert_eq!(
            normalize_asset_path("/truncated%2").unwrap(),
            "/truncated%2"
        );
        // T31e/E1 — shared table with facade test/router.test.mjs:
        // decodeURIComponent parity means ANY bad escape fails the WHOLE
        // segment (never partially decode), non-UTF-8 likewise.
        assert_eq!(normalize_asset_path("/a%zz%20b").unwrap(), "/a%zz%20b");
        assert_eq!(
            normalize_asset_path("/%E4%B8").unwrap(),
            "/%E4%B8" // truncated UTF-8 → raw
        );
        assert_eq!(normalize_asset_path("/x%").unwrap(), "/x%");
        // Decoded '/' becomes a real separator (collapse semantics).
        assert_eq!(normalize_asset_path("/a%2Fb.txt").unwrap(), "/a/b.txt");
        // ...including at the start: a decoded `%2F` never yields a
        // second structural slash (`//host` = protocol-relative URL).
        assert_eq!(normalize_asset_path("/%2Fevil.com").unwrap(), "/evil.com");
        assert_eq!(normalize_asset_path("//evil.com").unwrap(), "/evil.com");
        // Decoded traversal / NUL / backslash still fail closed.
        assert!(normalize_asset_path("/%2e%2e/etc").is_err());
        assert!(normalize_asset_path("/%00").is_err());
        assert!(normalize_asset_path("/%5C").is_err());
        // Plain paths unchanged.
        assert_eq!(normalize_asset_path("/index.html").unwrap(), "/index.html");
    }
}

#[cfg(test)]
mod gc_sweep_tests {
    //! RV-R53.5: mark-and-sweep — referenced kept, grace kept, expired
    //! deleted, bound enforced, temps/foreign names never touched,
    //! unreadable kept manifest fails closed (objects kept).
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    const ZERO: std::time::Duration = std::time::Duration::ZERO;
    const LONG: std::time::Duration = std::time::Duration::from_secs(60 * 60 * 24 * 365);

    struct Tmp(std::path::PathBuf);
    impl Tmp {
        fn new(tag: &str) -> Self {
            let d = std::env::temp_dir().join(format!(
                "vrt-assets-gc-{tag}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            Self(d)
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    impl std::ops::Deref for Tmp {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            &self.0
        }
    }

    fn keep(tenant: &str, digests: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
        let mut m = BTreeMap::new();
        m.insert(
            tenant.to_string(),
            digests.iter().map(|d| d.to_string()).collect(),
        );
        m
    }
    fn hex64(c: char) -> String {
        std::iter::repeat(c).take(64).collect()
    }

    /// Referenced manifest + its objects survive a sweep with zero
    /// grace; an unreferenced manifest + orphan object are collected.
    #[test]
    fn sweep_keeps_referenced_collects_stale() {
        let tmp = Tmp::new("live");
        let store = AssetStore::open(&tmp).unwrap();
        let t = "team-1";
        let live_bytes = b"live-object".to_vec();
        let live_d = sha256_hex(&live_bytes);
        store.put_object_verified(t, &live_d, &live_bytes).unwrap();
        let dead_bytes = b"dead-object".to_vec();
        let dead_d = sha256_hex(&dead_bytes);
        store.put_object_verified(t, &dead_d, &dead_bytes).unwrap();
        let manifest_json = format!(
            r#"{{"schemaVersion":1,"htmlHandling":"none","notFoundHandling":"none","files":[{{"path":"/a","sha256":"{live_d}","size":11,"mime":"text/plain"}}]}}"#
        );
        let man_d = sha256_hex(manifest_json.as_bytes());
        store
            .put_manifest_verified(t, &man_d, manifest_json.as_bytes())
            .unwrap();
        // orphan dead manifest (no keep ref) — write real manifest too
        let dead_man_json = format!(
            r#"{{"schemaVersion":1,"htmlHandling":"none","notFoundHandling":"none","files":[{{"path":"/b","sha256":"{dead_d}","size":11,"mime":"text/plain"}}]}}"#
        );
        let dead_man_d = sha256_hex(dead_man_json.as_bytes());
        store
            .put_manifest_verified(t, &dead_man_d, dead_man_json.as_bytes())
            .unwrap();

        let rep = store.gc_sweep(&keep(t, &[&man_d]), ZERO, 256);
        assert_eq!(rep.manifests_deleted, 1);
        assert_eq!(rep.objects_deleted, 1);
        assert!(store.manifest(t, &man_d).unwrap().is_some());
        assert!(store.manifest(t, &dead_man_d).unwrap().is_none());
        assert!(store.object_path(t, &live_d).unwrap().exists());
        assert!(!store.object_path(t, &dead_d).unwrap().exists());
    }

    /// Inside the grace window even unreferenced entries are kept.
    #[test]
    fn sweep_grace_keeps_unreferenced() {
        let tmp = Tmp::new("grace");
        let store = AssetStore::open(&tmp).unwrap();
        let t = "team-1";
        let b = b"obj".to_vec();
        let d = sha256_hex(&b);
        store.put_object_verified(t, &d, &b).unwrap();
        let rep = store.gc_sweep(&BTreeMap::new(), LONG, 256);
        assert_eq!(rep.objects_deleted, 0);
        assert_eq!(rep.grace_kept, 1);
        assert!(store.object_path(t, &d).unwrap().exists());
    }

    /// Round bound: max=1 deletes exactly one entry; a second round
    /// finishes the rest (monotone progress, no starvation).
    #[test]
    fn sweep_bounded_round() {
        let tmp = Tmp::new("bound");
        let store = AssetStore::open(&tmp).unwrap();
        let t = "team-1";
        for i in 0..3u8 {
            let b = vec![i];
            let d = sha256_hex(&b);
            store.put_object_verified(t, &d, &b).unwrap();
        }
        let r1 = store.gc_sweep(&BTreeMap::new(), ZERO, 1);
        assert_eq!(r1.objects_deleted, 1);
        let r2 = store.gc_sweep(&BTreeMap::new(), ZERO, 256);
        assert_eq!(r2.objects_deleted, 2);
    }

    /// Temp/foreign filenames inside objects/ are never collected;
    /// a poisoned kept manifest aborts that tenant's object sweep.
    #[test]
    fn sweep_skips_temps_and_fails_closed_on_bad_manifest() {
        let tmp = Tmp::new("closed");
        let store = AssetStore::open(&tmp).unwrap();
        let t = "team-1";
        // temp name — not 64-hex
        let odir = store.tenant_dir(t).join("objects");
        std::fs::create_dir_all(&odir).unwrap();
        std::fs::write(odir.join(".tmp-upload-1"), b"partial").unwrap();
        // poisoned manifest in keep set: digest-verified name but bad
        // content — manifest() errors → objects must be kept.
        let bad_d = hex64('e');
        std::fs::create_dir_all(store.tenant_dir(t).join("manifests")).unwrap();
        std::fs::write(
            store.manifest_path(t, &bad_d).unwrap(),
            b"not the digest of me",
        )
        .unwrap();
        let obj_b = b"protected".to_vec();
        let obj_d = sha256_hex(&obj_b);
        store.put_object_verified(t, &obj_d, &obj_b).unwrap();

        let rep = store.gc_sweep(&keep(t, &[&bad_d]), ZERO, 256);
        assert!(odir.join(".tmp-upload-1").exists(), "temp never touched");
        assert!(
            store.object_path(t, &obj_d).unwrap().exists(),
            "objects kept — poisoned kept manifest fails closed"
        );
        assert_eq!(rep.objects_deleted, 0);
    }
}

#[cfg(test)]
mod html_handling_tests {
    //! Cloudflare's four `html_handling` modes, row for row from the
    //! `workers-shared-asset-config` fixture
    //! (`test-cases/html-handling-test-cases.ts`) and
    //! `asset-worker/src/handler.ts`.
    use super::*;

    fn f(path: &str) -> AssetFile {
        AssetFile {
            path: path.to_string(),
            // Distinct content per path — `safe_redirect` compares
            // assets by digest, exactly like Cloudflare's eTag check.
            sha256: sha256_hex(path.as_bytes()),
            size: 1,
            mime: "text/html".to_string(),
        }
    }

    fn manifest(html: HtmlHandling, files: &[&str]) -> AssetManifest {
        AssetManifest {
            files: files.iter().map(|p| f(p)).collect(),
            html_handling: html,
            not_found_handling: NotFoundHandling::None,
        }
    }

    fn serve(path: &str) -> Resolved {
        Resolved::Serve(f(path))
    }

    fn redirect(path: &str) -> Resolved {
        Resolved::Redirect(path.to_string())
    }

    fn check(mode: HtmlHandling, files: &[&str], cases: &[(&str, Resolved)]) {
        for (req, want) in cases {
            assert_eq!(
                resolve_with_nfh(&manifest(mode, files), req, NotFoundHandling::None),
                *want,
                "{mode:?} {files:?} {req}"
            );
        }
    }

    #[test]
    fn auto_trailing_slash_matches_cloudflare() {
        use HtmlHandling::AutoTrailingSlash as M;
        check(M, &["/index.html"], &[
            ("/index.html", redirect("/")),
            ("/index", redirect("/")),
            ("/", serve("/index.html")),
        ]);
        // `/both.html` and `/both/index.html` both exist: each request
        // form serves its own file, and only canonicalizes when the
        // destination resolves to that same file.
        let both = ["/both.html", "/both/index.html"];
        check(M, &both, &[
            ("/both", serve("/both.html")),
            ("/both.html", redirect("/both")),
            ("/both/", serve("/both/index.html")),
            ("/both/index.html", redirect("/both/")),
            ("/both/index", redirect("/both/")),
        ]);
        let file = ["/file.html"];
        check(M, &file, &[
            ("/file", serve("/file.html")),
            ("/file.html", redirect("/file")),
            ("/file/", redirect("/file")),
            ("/file/index", redirect("/file")),
            ("/file/index.html", redirect("/file")),
        ]);
        let folder = ["/folder/index.html"];
        check(M, &folder, &[
            ("/folder", redirect("/folder/")),
            ("/folder.html", redirect("/folder/")),
            ("/folder/", serve("/folder/index.html")),
            ("/folder/index", redirect("/folder/")),
            ("/folder/index.html", redirect("/folder/")),
        ]);
        // An exact non-HTML file shadows the `.html` rewrite: `/file-bin`
        // exists, so `/file-bin.html` keeps its own URL and `/file-bin/`
        // has no canonical form.
        let bin = ["/file-bin", "/file-bin.html"];
        check(M, &bin, &[
            ("/file-bin", serve("/file-bin")),
            ("/file-bin.html", serve("/file-bin.html")),
            ("/file-bin/", Resolved::NotFound),
            ("/file-bin/index", Resolved::NotFound),
            ("/file-bin/index.html", Resolved::NotFound),
        ]);
        // Dotted stems stay reachable through their `.html` alias.
        check(M, &["/release-1.2.html"], &[
            ("/release-1.2", serve("/release-1.2.html")),
            ("/release-1.2.html", redirect("/release-1.2")),
        ]);
        // Dotted directory names under drop/auto forms.
        check(M, &["/v1.0/index.html"], &[
            ("/v1.0", redirect("/v1.0/")),
            ("/v1.0/", serve("/v1.0/index.html")),
        ]);
        // Exact extensionless `/index` files are served, not aliased.
        check(M, &["/api/index"], &[
            ("/api/index", serve("/api/index")),
        ]);
        // Non-HTML assets are never rewritten.
        check(M, &["/app.js"], &[("/app.js", serve("/app.js"))]);
    }

    #[test]
    fn force_trailing_slash_matches_cloudflare() {
        use HtmlHandling::ForceTrailingSlash as M;
        check(M, &["/index.html"], &[
            ("/index.html", redirect("/")),
            ("/index", redirect("/")),
            ("/", serve("/index.html")),
        ]);
        let both = ["/both.html", "/both/index.html"];
        check(M, &both, &[
            ("/both", redirect("/both/")),
            ("/both.html", serve("/both.html")),
            ("/both/", serve("/both/index.html")),
            ("/both/index", redirect("/both/")),
            ("/both/index.html", redirect("/both/")),
        ]);
        let file = ["/file.html"];
        check(M, &file, &[
            ("/file", redirect("/file/")),
            ("/file.html", redirect("/file/")),
            ("/file/", serve("/file.html")),
            ("/file/index", redirect("/file/")),
            ("/file/index.html", redirect("/file/")),
        ]);
        let folder = ["/folder/index.html"];
        check(M, &folder, &[
            ("/folder", redirect("/folder/")),
            ("/folder.html", redirect("/folder/")),
            ("/folder/", serve("/folder/index.html")),
            ("/folder/index", redirect("/folder/")),
            ("/folder/index.html", redirect("/folder/")),
        ]);
        let bin = ["/file-bin", "/file-bin.html"];
        check(M, &bin, &[
            ("/file-bin", serve("/file-bin")),
            ("/file-bin.html", redirect("/file-bin/")),
            ("/file-bin/", serve("/file-bin.html")),
            ("/file-bin/index", redirect("/file-bin/")),
            ("/file-bin/index.html", redirect("/file-bin/")),
        ]);
        check(M, &["/release-1.2.html"], &[
            ("/release-1.2", redirect("/release-1.2/")),
            ("/release-1.2/", serve("/release-1.2.html")),
        ]);
        check(M, &["/app.js"], &[("/app.js", serve("/app.js"))]);
    }

    #[test]
    fn drop_trailing_slash_matches_cloudflare() {
        use HtmlHandling::DropTrailingSlash as M;
        check(M, &["/index.html"], &[
            ("/index.html", redirect("/")),
            ("/index", redirect("/")),
            ("/", serve("/index.html")),
        ]);
        let both = ["/both.html", "/both/index.html"];
        check(M, &both, &[
            ("/both", serve("/both.html")),
            ("/both.html", redirect("/both")),
            ("/both/", redirect("/both")),
            ("/both/index", redirect("/both")),
            // `/both` already serves `/both.html`, so the index keeps
            // its exact URL instead of redirecting into the other page.
            ("/both/index.html", serve("/both/index.html")),
        ]);
        let file = ["/file.html"];
        check(M, &file, &[
            ("/file", serve("/file.html")),
            ("/file.html", redirect("/file")),
            ("/file/", redirect("/file")),
            ("/file/index", redirect("/file")),
            ("/file/index.html", redirect("/file")),
        ]);
        let folder = ["/folder/index.html"];
        check(M, &folder, &[
            ("/folder", serve("/folder/index.html")),
            ("/folder.html", redirect("/folder")),
            ("/folder/", redirect("/folder")),
            ("/folder/index", redirect("/folder")),
            ("/folder/index.html", redirect("/folder")),
        ]);
        let bin = ["/file-bin", "/file-bin.html"];
        check(M, &bin, &[
            ("/file-bin", serve("/file-bin")),
            ("/file-bin.html", serve("/file-bin.html")),
            ("/file-bin/", Resolved::NotFound),
            ("/file-bin/index", Resolved::NotFound),
            ("/file-bin/index.html", Resolved::NotFound),
        ]);
        check(M, &["/release-1.2.html"], &[
            ("/release-1.2", serve("/release-1.2.html")),
            ("/release-1.2.html", redirect("/release-1.2")),
        ]);
        check(M, &["/v1.0/index.html"], &[
            ("/v1.0", serve("/v1.0/index.html")),
            ("/v1.0/", redirect("/v1.0")),
        ]);
        check(M, &["/app.js"], &[("/app.js", serve("/app.js"))]);
    }

    #[test]
    fn none_serves_exact_paths_only() {
        use HtmlHandling::None as M;
        let files = ["/index.html", "/file.html", "/folder/index.html", "/app.js"];
        check(M, &files, &[
            ("/file", Resolved::NotFound),
            ("/file.html", serve("/file.html")),
            ("/folder", Resolved::NotFound),
            ("/folder/", Resolved::NotFound),
            ("/", Resolved::NotFound),
            ("/app.js", serve("/app.js")),
        ]);
    }

    #[test]
    fn misses_are_not_found() {
        use HtmlHandling::*;
        check(AutoTrailingSlash, &["/file.html"], &[("/nope", Resolved::NotFound)]);
        check(ForceTrailingSlash, &["/file.html"], &[("/nope.html", Resolved::NotFound)]);
        check(DropTrailingSlash, &["/file.html"], &[("/nope/", Resolved::NotFound)]);
    }

    #[test]
    fn encode_path_matches_encode_uri_component() {
        // Unreserved keep, reserved/non-ASCII percent-encode per segment.
        assert_eq!(encode_path("/a b/c.txt"), "/a%20b/c.txt");
        assert_eq!(encode_path("/a?b#c%d"), "/a%3Fb%23c%25d");
        assert_eq!(encode_path("/\u{4e2d}\u{6587} page"), "/%E4%B8%AD%E6%96%87%20page");
        assert_eq!(encode_path("/[boop]"), "/%5Bboop%5D");
        assert_eq!(encode_path("/a-b_c.d!e~f*g'h(i)"), "/a-b_c.d!e~f*g'h(i)");
    }

    #[test]
    fn wire_names_match_cloudflare() {
        assert_eq!(html_wire(HtmlHandling::AutoTrailingSlash), "auto-trailing-slash");
        assert_eq!(html_wire(HtmlHandling::ForceTrailingSlash), "force-trailing-slash");
        assert_eq!(html_wire(HtmlHandling::DropTrailingSlash), "drop-trailing-slash");
        assert_eq!(html_wire(HtmlHandling::None), "none");
    }
}
