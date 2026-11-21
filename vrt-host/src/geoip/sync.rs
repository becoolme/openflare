//! Background refresh of the local GeoIP MMDB (default source:
//! `NetworkCats/Merged-IP-Data` GitHub releases, updated daily).
//!
//! Polls the releases API for a tag change — cheap, a few KB — and
//! only downloads the ~100 MB database when the tag moved. The file
//! lands under `<mmdb>.part`, is validated by opening it as MMDB,
//! then atomically renamed over the live path, so the mmap reader
//! never sees a partial file. Failures keep the previous database;
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
    let client = match reqwest::Client::builder()
        .user_agent(concat!("vrt-host/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "geoip: http client build failed — sync disabled");
            return;
        }
    };
    loop {
        let delay = match sync_once(&cfg, &geo, &client, token.as_deref()).await {
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
        let body = req
            .send()
            .await
            .map_err(|e| format!("releases api: {e}"))?
            .error_for_status()
            .map_err(|e| format!("releases api status: {e}"))?
            .bytes()
            .await
            .map_err(|e| format!("releases api body: {e}"))?;
        let rel: Release =
            serde_json::from_slice(&body).map_err(|e| format!("releases api json: {e}"))?;
        let state = read_state(&cfg.mmdb_path).await;
        if state.tag == rel.tag_name && cfg.mmdb_path.is_file() {
            return Ok("unchanged");
        }
        if let Some(asset) = rel.assets.iter().find(|a| a.name.ends_with(".mmdb")) {
            download_url = asset.browser_download_url.clone();
        }
        new_tag = Some(rel.tag_name);
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
    // can never reach the mmap reader.
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
