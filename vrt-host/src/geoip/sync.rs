//! Background refresh of the local GeoIP MMDB (default source:
//! `NetworkCats/Merged-IP-Data` GitHub releases, updated daily).
//!
//! Polls the releases API for a tag change — cheap, a few KB — and
//! only downloads the ~100 MB database when the tag moved. The file
//! lands under `<mmdb>.part`, is validated by opening it as MMDB,
//! then atomically renamed over the live path, so the heap-backed
//! reader never sees a partial file. Failures keep the previous database;
//! a missing database is also fine — ingress simply emits no geo
//! fields. Nothing here may panic or block shutdown.
//!
//! Mirror mode: set `releasesApiUrl = ""` in config. The tag check is
//! skipped and the file is re-downloaded whenever its mtime is older
//! than `refresh_interval` — for mirrors that serve only the binary
//! (the GitHub API shape is not required of them).
//!
//! Credential discipline: an optional `VRT_GEOIP_GITHUB_TOKEN`
//! environment variable is read once at task start and sent only to
//! the configured API host; it is never logged and never appears in
//! the config file.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sha2::Digest;
use tokio::io::AsyncWriteExt;

use super::GeoIp;

/// Upstream release metadata endpoint (GitHub releases API).
pub const DEFAULT_RELEASES_API: &str =
    "https://api.github.com/repos/NetworkCats/Merged-IP-Data/releases/latest";
/// Upstream database asset.
pub const DEFAULT_DOWNLOAD_URL: &str =
    "https://github.com/NetworkCats/Merged-IP-Data/releases/latest/download/Merged-IP.mmdb";

/// Env var for an optional GitHub API token (avoids the anonymous
/// 60 req/h/IP ceiling when many cells share a NAT address).
const TOKEN_ENV: &str = "VRT_GEOIP_GITHUB_TOKEN";
/// Retry sooner than a full interval when an attempt failed — the
/// previous database (or none) keeps serving meanwhile.
const FAILURE_RETRY_SECS: u64 = 1800;
/// Bound a single download — the file is ~100 MB over a CDN link.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
const METADATA_TIMEOUT: Duration = Duration::from_secs(30);
/// The release JSON is a few KiB — bound it before serde so a hostile
/// or broken API response cannot spike RSS.
const MAX_METADATA_BYTES: usize = 512 * 1024;
/// Merged-IP MMDBs run ~100 MB; bound the streamed body so a hostile
/// endpoint cannot fill the disk (the timeout alone does not cap
/// bytes on a fast link).
const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;

/// An API-supplied `browser_download_url` is untrusted input: require
/// https plus the exact GitHub release hosts so a compromised
/// release/mirror cannot point the cell at `http://169.254.169.254/`
/// or an internal https service (SSRF from inside the cluster).
/// `github.com` serves the release link; `objects.githubusercontent.com`
/// is where the GitHub CDN redirects the asset body — nothing else is
/// acceptable. Operator-configured `downloadUrl` is exempt — its own
/// https check lives in config.rs.
fn validate_asset_url(url: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("asset url parse: {e}"))?;
    if parsed.scheme() != "https" {
        return Err(format!("asset url scheme must be https: {url}"));
    }
    // No userinfo, no non-default port: both are ways to make the
    // allowlisted hostname resolve/serve something else.
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!("asset url must not carry userinfo: {url}"));
    }
    if parsed.port_or_known_default() != Some(443) {
        return Err(format!("asset url must use port 443: {url}"));
    }
    match parsed.host_str().unwrap_or("") {
        "github.com" | "objects.githubusercontent.com" => Ok(()),
        host => Err(format!("asset url host not allowed: {host}")),
    }
}

#[derive(Clone, Debug)]
pub struct GeoIpSyncConfig {
    pub mmdb_path: PathBuf,
    /// GitHub releases API URL; EMPTY string = mirror mode (skip the
    /// tag check, refresh on file age).
    pub releases_api_url: String,
    /// Direct download URL used when the API is disabled OR as the
    /// fallback when the API response names no usable asset.
    pub download_url: String,
    pub refresh_interval: Duration,
}

/// On-disk `*.state.json` — lets restarts skip an already-fetched
/// tag without a HEAD/GET on the 100 MB asset.
#[derive(Default, serde::Deserialize, serde::Serialize)]
struct SyncState {
    tag: String,
    sha256: String,
    bytes: u64,
}

#[derive(serde::Deserialize)]
struct Release {
    tag_name: String,
    #[serde(default)]
    assets: Vec<Asset>,
}

#[derive(serde::Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
}

/// The refresh loop. Exits cleanly when `shutdown` flips true.
pub async fn run(
    cfg: GeoIpSyncConfig,
    geo: Arc<GeoIp>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let token = std::env::var(TOKEN_ENV).ok().filter(|t| !t.is_empty());
    let build = |redirect: reqwest::redirect::Policy| {
        reqwest::Client::builder()
            .user_agent(concat!("vrt-host/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .redirect(redirect)
            .build()
    };
    let client = match build(reqwest::redirect::Policy::limited(5)) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "geoip: http client build failed — sync disabled");
            return;
        }
    };
    // API-named assets use their own client: every redirect hop must
    // stay on the two allowlisted GitHub hosts. Validating only the
    // first URL would let a redirect walk the cell to an internal
    // service (SSRF); the operator's own mirror URL keeps the plain
    // client (its host is trusted config, its CDN hops are arbitrary).
    let asset_client = match build(reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= 5 {
            return attempt.error(std::io::Error::other("geoip asset: too many redirects"));
        }
        match validate_asset_url(attempt.url().as_str()) {
            Ok(()) => attempt.follow(),
            Err(e) => attempt.error(std::io::Error::new(std::io::ErrorKind::PermissionDenied, e)),
        }
    })) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "geoip: http client build failed — sync disabled");
            return;
        }
    };
    loop {
        let delay = match sync_once(&cfg, &geo, &client, &asset_client, token.as_deref()).await {
            Ok(outcome) => {
                tracing::info!(outcome, "geoip: sync check complete");
                jittered(cfg.refresh_interval)
            }
            Err(e) => {
                tracing::warn!(error = %e, "geoip: sync failed — keeping previous database");
                crate::metrics::registry().inc("vrt_host_geoip_sync_errors_total", &[], 1.0);
                jittered(Duration::from_secs(FAILURE_RETRY_SECS))
            }
        };
        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("geoip: sync task stopped");
                return;
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// ±10% jitter so cells do not stampede the API on a shared clock.
fn jittered(d: Duration) -> Duration {
    let nanos = d.as_nanos() as u64;
    let spread = nanos / 10;
    let jitter = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.subsec_nanos() as u64 ^ t.as_secs())
        .unwrap_or(0))
        % (2 * spread.max(1));
    Duration::from_nanos(nanos - spread + jitter)
}

async fn sync_once(
    cfg: &GeoIpSyncConfig,
    geo: &Arc<GeoIp>,
    client: &reqwest::Client,
    asset_client: &reqwest::Client,
    token: Option<&str>,
) -> Result<&'static str, String> {
    if let Some(parent) = cfg.mmdb_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut download_url = cfg.download_url.clone();
    let mut new_tag: Option<String> = None;
    if !cfg.releases_api_url.is_empty() {
        // Tag check first: a few KB decide whether a 100 MB fetch
        // is needed at all.
        let mut req = client.get(&cfg.releases_api_url).timeout(METADATA_TIMEOUT);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        let mut resp = req
            .send()
            .await
            .map_err(|e| format!("releases api: {e}"))?
            .error_for_status()
            .map_err(|e| format!("releases api status: {e}"))?;
        // Bound the metadata body before serde — a hostile/broken API
        // response must not spike RSS. A release JSON is a few KiB.
        let mut body = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| format!("releases api body: {e}"))?
        {
            body.extend_from_slice(&chunk);
            if body.len() > MAX_METADATA_BYTES {
                return Err("releases api body exceeds cap".to_string());
            }
        }
        let rel: Release =
            serde_json::from_slice(&body).map_err(|e| format!("releases api json: {e}"))?;
        let state = read_state(&cfg.mmdb_path).await;
        // Tag + size alone are insufficient: a truncated, padded or
        // bit-rotted live database with an intact state.json must
        // re-download. `state.sha256` is the digest the installer
        // recorded, so compare against the file itself (a pre-digest
        // state re-downloads once, repairing provenance).
        let on_disk_ok = cfg
            .mmdb_path
            .metadata()
            .ok()
            .is_some_and(|m| m.is_file() && m.len() == state.bytes && state.bytes > 0);
        let digest_ok = if on_disk_ok && !state.sha256.is_empty() {
            let path = cfg.mmdb_path.clone();
            tokio::task::spawn_blocking(move || file_sha256(&path).ok())
                .await
                .ok()
                .flatten()
                .is_some_and(|sha| sha == state.sha256)
        } else {
            false
        };
        if state.tag == rel.tag_name && digest_ok {
            return Ok("unchanged");
        }
        let mut api_asset = false;
        if let Some(asset) = rel.assets.iter().find(|a| a.name.ends_with(".mmdb")) {
            // The asset URL is API-controlled — validate it like
            // untrusted input (the configured downloadUrl got its
            // https check in config.rs; this one did not).
            validate_asset_url(&asset.browser_download_url)?;
            download_url = asset.browser_download_url.clone();
            api_asset = true;
            // Only stamp the upstream tag when it names the artifact
            // actually installed — a fallback download must not be
            // recorded as `rel.tag_name` (it would freeze updates and
            // mislabel provenance).
            new_tag = Some(rel.tag_name);
        }
        if download_url.is_empty() {
            return Err("geoip: no download url configured".to_string());
        }
        // API-named asset ⇒ the redirect-guarded client (every hop on
        // the allowlisted GitHub hosts); a fallback/mirror URL is the
        // operator's trusted config.
        let dl = if api_asset { asset_client } else { client };
        return fetch_and_install(cfg, geo, dl, &download_url, new_tag).await;
    } else {
        // Mirror mode: refresh only when the file is older than the
        // interval (or absent).
        let fresh = cfg
            .mmdb_path
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age < cfg.refresh_interval);
        if fresh {
            return Ok("unchanged");
        }
    }
    if download_url.is_empty() {
        return Err("geoip: no download url configured".to_string());
    }
    fetch_and_install(cfg, geo, client, &download_url, new_tag).await
}

async fn fetch_and_install(
    cfg: &GeoIpSyncConfig,
    geo: &Arc<GeoIp>,
    client: &reqwest::Client,
    url: &str,
    tag: Option<String>,
) -> Result<&'static str, String> {
    let tmp = cfg.mmdb_path.with_extension("part");
    let mut resp = client
        .get(url)
        .timeout(DOWNLOAD_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("download: {e}"))?
        .error_for_status()
        .map_err(|e| format!("download status: {e}"))?;
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .map_err(|e| format!("create {}: {e}", tmp.display()))?;
    let mut hasher = sha2::Sha256::new();
    let mut bytes: u64 = 0;
    let result: Result<(), String> = async {
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| format!("download body: {e}"))?
        {
            hasher.update(&chunk);
            bytes += chunk.len() as u64;
            if bytes > MAX_DOWNLOAD_BYTES {
                return Err(format!("download exceeds {MAX_DOWNLOAD_BYTES} byte cap"));
            }
            file.write_all(&chunk)
                .await
                .map_err(|e| format!("write: {e}"))?;
        }
        file.sync_all().await.map_err(|e| format!("fsync: {e}"))?;
        Ok(())
    }
    .await;
    if let Err(e) = result {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    drop(file);
    // Validate BEFORE the swap: open the downloaded file as MMDB and
    // prove the data section decodes. A truncated or HTML error page
    // can never reach the heap-backed reader.
    let probe = tmp.clone();
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let reader =
            maxminddb::Reader::open_readfile(&probe).map_err(|e| format!("mmdb validate: {e}"))?;
        // A real lookup exercises the search tree + data section.
        let _ = reader.lookup(std::net::IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)));
        Ok(())
    })
    .await
    .map_err(|e| format!("validate task: {e}"))?
    .inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    tokio::fs::rename(&tmp, &cfg.mmdb_path)
        .await
        .map_err(|e| format!("rename: {e}"))?;
    let sha = hex::encode(hasher.finalize());
    write_state(&cfg.mmdb_path, tag.as_deref().unwrap_or(""), &sha, bytes).await;
    geo.reload();
    crate::metrics::registry().set(
        "vrt_host_geoip_last_sync_unix",
        &[],
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as f64)
            .unwrap_or(0.0),
    );
    tracing::info!(
        path = %cfg.mmdb_path.display(),
        bytes,
        sha256 = %sha,
        tag = tag.as_deref().unwrap_or(""),
        "geoip: database updated"
    );
    Ok("updated")
}

/// Stream-hash an on-disk file with SHA-256. Blocking on purpose —
/// callers run it on the blocking pool (`spawn_blocking`): a ~100 MB
/// read must never sit on a runtime worker.
fn file_sha256(path: &std::path::Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn state_path(mmdb: &std::path::Path) -> PathBuf {
    mmdb.with_extension("state.json")
}

async fn read_state(mmdb: &std::path::Path) -> SyncState {
    tokio::fs::read_to_string(state_path(mmdb))
        .await
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

async fn write_state(mmdb: &std::path::Path, tag: &str, sha: &str, bytes: u64) {
    let p = state_path(mmdb);
    let body = serde_json::json!({
        "tag": tag,
        "sha256": sha,
        "bytes": bytes,
    });
    if let Ok(s) = serde_json::to_string(&body) {
        // Atomic like the database itself: a torn state file would
        // just cost one redundant download, but there is no reason
        // not to write it correctly.
        let tmp = p.with_extension("state.tmp");
        if tokio::fs::write(&tmp, s).await.is_ok() {
            let _ = tokio::fs::rename(&tmp, &p).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the exact GitHub release hosts are acceptable, on https and
    /// the default port: suffix matches (`github.com.evil.test`,
    /// `evil.githubusercontent.com`) and URL tricks (`user@host`,
    /// `:8080`) are SSRF vectors, not asset links.
    #[test]
    fn asset_url_allowlist_is_exact() {
        for ok in [
            "https://github.com/NetworkCats/Merged-IP-Data/releases/download/v1/Merged-IP.mmdb",
            "https://objects.githubusercontent.com/github-production-release-asset/x",
        ] {
            validate_asset_url(ok).unwrap();
        }
        for bad in [
            "http://github.com/x.mmdb",
            "https://github.com.evil.test/x.mmdb",
            "https://evil.githubusercontent.com/x.mmdb",
            "https://api.github.com/x.mmdb",
            "https://user:pw@github.com/x.mmdb",
            "https://github.com:8443/x.mmdb",
            "https://169.254.169.254/latest/meta-data",
            "https://internal.svc.cluster.local/x.mmdb",
            "file:///etc/passwd",
            "not a url",
        ] {
            assert!(validate_asset_url(bad).is_err(), "{bad} must be rejected");
        }
    }

    #[test]
    fn file_sha256_matches_known_digest() {
        let dir = std::env::temp_dir().join(format!("vrt-geoip-sha-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("f");
        std::fs::write(&p, b"abc").unwrap();
        assert_eq!(
            file_sha256(&p).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(file_sha256(&dir.join("missing")).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
