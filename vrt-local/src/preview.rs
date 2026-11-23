//! Versioned preview URLs — the control-plane half.
//!
//! Routing lives in `vrt-host`: it owns the `[preview]` config and turns
//! each preview instance in the signed snapshot into
//! `<versionId>--<workerName>.<domain>`. This module *reports* the URL the
//! operator can open, by reading the same host config file
//! (`<root>/config/vrt-host.toml`). It is deliberately display-only:
//!
//! - an absent/unreadable/invalid config yields `None` (the API returns
//!   `previewUrl: null`), never a guessed URL;
//! - the host re-derives the hostname from its own config on every
//!   converge, so a stale/malformed value here can never create a route;
//! - previews REQUIRE a signed token by default: a URL is only reported
//!   when the host config declares `[preview] tokenSecretFile` and that
//!   file carries the key derived from this root's signing seed. With no
//!   token secret configured the preview domain stays closed (the host
//!   routes nothing and this module reports no URL).
//!
//! Wire shape: `<versionId>--<workerName>.<previewDomain>`. The `--`
//! separator cannot appear in a worker name (the protocol forbids it, it
//! is the platform's `<worker>--<team>` separator), and version ids are
//! `<prefix>_<hex>`, so the two halves are unambiguous.

use snapshot_protocol::preview_token::{PreviewTokenKey, DEFAULT_TTL_SECS};
use std::path::{Path, PathBuf};

/// Host config file name under `<root>/config/`.
pub const HOST_CONFIG: &str = "vrt-host.toml";

/// Maximum hostname length (DNS).
const MAX_HOSTNAME_LEN: usize = 253;

/// `[preview]` as resolved from the host config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreviewAccess {
    /// Normalized public suffix.
    pub domain: String,
    /// Absolute path of the HMAC key file the host verifies with.
    pub token_secret_file: PathBuf,
}

/// A signed preview URL plus its expiry (unix seconds).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreviewUrl {
    /// `https://<version>--<worker>.<domain>/?__vrt_preview_token=…`
    pub url: String,
    /// Token expiry (unix seconds). The URL stops working at this time.
    pub expires_at_unix_s: u64,
}

/// Read and validate `[preview]` from `<root>/config/vrt-host.toml`.
///
/// Both `domain` and an ABSOLUTE `tokenSecretFile` are required: a domain
/// without a token secret is a closed preview domain, not an open one.
/// Documents are parsed leniently on purpose: a host config this build
/// does not fully understand (a newer field) must not turn `previewUrl`
/// into an error.
pub fn access(root: &Path) -> Option<PreviewAccess> {
    let text = std::fs::read_to_string(crate::layout::config_dir(root).join(HOST_CONFIG)).ok()?;
    let doc: toml::Value = toml::from_str(&text).ok()?;
    let preview = doc.get("preview")?;
    let domain = normalize_domain(preview.get("domain")?.as_str()?)?;
    let token_secret_file = PathBuf::from(preview.get("tokenSecretFile")?.as_str()?);
    if !token_secret_file.is_absolute() {
        return None;
    }
    Some(PreviewAccess {
        domain,
        token_secret_file,
    })
}

/// Read `[preview] domain` alone (routing shape without the access
/// requirement). Kept for diagnostics/tests; [`access`] is what gates a
/// URL.
pub fn preview_domain(root: &Path) -> Option<String> {
    let text = std::fs::read_to_string(crate::layout::config_dir(root).join(HOST_CONFIG)).ok()?;
    let doc: toml::Value = toml::from_str(&text).ok()?;
    normalize_domain(doc.get("preview")?.get("domain")?.as_str()?)
}

/// Validate + canonicalize a preview domain.
///
/// Accepted: one or more lowercase DNS labels (`[a-z0-9-]`, no leading or
/// trailing `-`), no scheme, no port, no path, no wildcard, no trailing
/// dot. Case is lowercased (DNS is case-insensitive) but nothing else is
/// rewritten — a domain with an underscore is refused because hostname
/// routing itself rejects such Host headers.
pub fn normalize_domain(raw: &str) -> Option<String> {
    let domain = raw.trim().to_ascii_lowercase();
    if domain.is_empty() || domain.len() > MAX_HOSTNAME_LEN {
        return None;
    }
    if domain.contains('/') || domain.contains(':') || domain.contains('*') {
        return None;
    }
    if domain.split('.').any(|label| {
        label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }) {
        return None;
    }
    Some(domain)
}

/// Preview hostname for one version of one worker. The caller passes an
/// already-normalized domain (`normalize_domain`) — a raw config value is
/// never interpolated into a hostname.
pub fn preview_host(domain: &str, version_id: &str, worker_name: &str) -> String {
    format!("{}--{}.{}", version_id, worker_name, domain)
}

/// Unsigned `https://…/` preview URL, or `None` for an unusable input.
///
/// This URL opens nothing on a host that requires tokens (the default):
/// use [`signed_url`] for an access-bearing link.
pub fn preview_url(domain: &str, version_id: &str, worker_name: &str) -> Option<String> {
    if version_id.is_empty() || worker_name.is_empty() || domain.is_empty() {
        return None;
    }
    Some(format!(
        "https://{}/",
        preview_host(domain, version_id, worker_name)
    ))
}

/// Mint a signed, expiring preview URL for `(version, worker)`.
///
/// `worker_id` is the PRODUCTION worker id (the token audience); the
/// token is bound to `(version_id, worker_id)` and cannot be replayed on
/// another version or worker. `refresh_key` controls whether a missing
/// `keys/preview-token.secret` is (re)created from the signing seed:
/// mutating callers pass `true`; read-only listings pass `false` so a GET
/// never writes key material.
///
/// Returns `None` when previews are not fully configured (no domain, no
/// absolute `tokenSecretFile`, key file absent or not matching this
/// root's derived key). A URL is never shown unless the host can verify
/// it.
pub fn signed_url(
    root: &Path,
    version_id: &str,
    worker_id: &str,
    worker_name: &str,
    ttl_secs: u64,
    refresh_key: bool,
) -> Option<PreviewUrl> {
    let access = access(root)?;
    let key = if refresh_key {
        crate::keys::ensure_preview_token_secret(root).ok()?
    } else {
        crate::keys::read_preview_token_secret(root)?
    };
    // The host verifies with the file, not with our derived value: only
    // advertise a URL when both sides provably hold the same key.
    // Mirror the host's loader checks: the verifier refuses a
    // world-readable key file at startup, so advertising a URL for it
    // would print a dead link.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(&access.token_secret_file).ok()?;
        if meta.permissions().mode() & 0o777 != 0o600 {
            return None;
        }
    }
    let on_disk = std::fs::read_to_string(&access.token_secret_file).ok()?;
    if !on_disk.trim().eq_ignore_ascii_case(&key.to_hex()) {
        return None;
    }
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let ttl = ttl_secs.clamp(
        snapshot_protocol::preview_token::MIN_TTL_SECS,
        snapshot_protocol::preview_token::MAX_TTL_SECS,
    );
    let expires_at_unix_s = now.saturating_add(ttl);
    Some(PreviewUrl {
        url: signed_url_for_key(
            &access.domain,
            &key,
            version_id,
            worker_id,
            worker_name,
            expires_at_unix_s,
        ),
        expires_at_unix_s,
    })
}

/// Convenience wrapper for the default display TTL (one hour).
pub fn signed_url_default(
    root: &Path,
    version_id: &str,
    worker_id: &str,
    worker_name: &str,
    refresh_key: bool,
) -> Option<PreviewUrl> {
    signed_url(
        root,
        version_id,
        worker_id,
        worker_name,
        DEFAULT_TTL_SECS,
        refresh_key,
    )
}

fn signed_url_for_key(
    domain: &str,
    key: &PreviewTokenKey,
    version_id: &str,
    worker_id: &str,
    worker_name: &str,
    expires_at_unix_s: u64,
) -> String {
    let token = key.mint(version_id, worker_id, expires_at_unix_s);
    format!(
        "https://{}/?{}={}",
        preview_host(domain, version_id, worker_name),
        snapshot_protocol::preview_token::TOKEN_PARAM,
        token
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root_with(domain: Option<&str>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("config")).unwrap();
        let body = match domain {
            Some(d) => format!("cellId = \"local\"\n\n[preview]\ndomain = \"{d}\"\n"),
            None => "cellId = \"local\"\n".to_string(),
        };
        std::fs::write(dir.path().join("config/vrt-host.toml"), body).unwrap();
        dir
    }

    #[test]
    fn domain_is_read_from_the_host_config() {
        let dir = root_with(Some("preview.example.com"));
        assert_eq!(
            preview_domain(dir.path()).as_deref(),
            Some("preview.example.com")
        );
        assert_eq!(access(dir.path()), None, "no tokenSecretFile -> closed");
    }

    #[test]
    fn absent_section_or_file_is_none() {
        let dir = root_with(None);
        assert_eq!(preview_domain(dir.path()), None);
        assert_eq!(access(dir.path()), None);
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(preview_domain(empty.path()), None);
    }

    #[test]
    fn unusable_domains_are_refused_not_guessed() {
        for bad in [
            "",
            " ",
            "Preview.Example.COM:443",
            "https://preview.example.com",
            "*.preview.example.com",
            "preview_example.com",
            "preview.example.com.",
            ".preview.example.com",
            "-preview.example.com",
            "preview-.example.com",
            &format!("{}.com", "a".repeat(64)),
        ] {
            assert_eq!(normalize_domain(bad), None, "{bad:?}");
        }
        // Case is the only rewrite.
        assert_eq!(
            normalize_domain("Preview.Example.COM").as_deref(),
            Some("preview.example.com")
        );
    }

    #[test]
    fn url_shape_uses_version_dash_dash_worker() {
        let url = preview_url("preview.example.com", "vrt_abc123", "edge-api").unwrap();
        assert_eq!(url, "https://vrt_abc123--edge-api.preview.example.com/");
        assert!(preview_url("", "vrt_abc123", "edge-api").is_none());
        assert!(preview_url("preview.example.com", "", "edge-api").is_none());
    }

    #[test]
    fn access_requires_an_absolute_token_secret_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("config")).unwrap();
        std::fs::write(
            dir.path().join("config/vrt-host.toml"),
            "cellId = \"local\"\n\n[preview]\ndomain = \"preview.example.com\"\n\
             tokenSecretFile = \"keys/preview-token.secret\"\n",
        )
        .unwrap();
        assert_eq!(access(dir.path()), None, "relative path is refused");
    }
}
