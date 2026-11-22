//! Versioned preview URLs — the control-plane half.
//!
//! Routing lives in `vrt-host`: it owns the `[preview] domain` config and
//! turns each preview instance in the signed snapshot into
//! `<versionId>--<workerName>.<domain>`. This module only *reports* the
//! URL the operator can open, by reading the same host config file
//! (`<root>/config/vrt-host.toml`). It is deliberately display-only:
//!
//! - an absent/unreadable/invalid `[preview] domain` yields `None`
//!   (the API returns `previewUrl: null`), never a guessed URL;
//! - the host re-derives the hostname from its own config on every
//!   converge, so a stale/malformed value here can never create a route.
//!
//! Wire shape: `<versionId>--<workerName>.<previewDomain>`. The `--`
//! separator cannot appear in a worker name (the protocol forbids it, it
//! is the platform's `<worker>--<team>` separator), and version ids are
//! `<prefix>_<hex>`, so the two halves are unambiguous.

use std::path::Path;

/// Host config file name under `<root>/config/`.
pub const HOST_CONFIG: &str = "vrt-host.toml";

/// Maximum hostname length (DNS).
const MAX_HOSTNAME_LEN: usize = 253;

/// Read `[preview] domain` from `<root>/config/vrt-host.toml`.
///
/// Returns `None` when the file is missing, unparsable, has no
/// `[preview]` section, or carries a domain this module will not render
/// into a URL. Documents are parsed leniently on purpose: a host config
/// this build does not fully understand (a newer field) must not turn
/// `previewUrl` into an error.
pub fn preview_domain(root: &Path) -> Option<String> {
    let path = crate::layout::config_dir(root).join(HOST_CONFIG);
    let text = std::fs::read_to_string(path).ok()?;
    let doc: toml::Value = toml::from_str(&text).ok()?;
    let domain = doc.get("preview")?.get("domain")?.as_str()?;
    normalize_domain(domain)
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

/// Full `https://…/` preview URL, or `None` for an unusable input.
pub fn preview_url(domain: &str, version_id: &str, worker_name: &str) -> Option<String> {
    if version_id.is_empty() || worker_name.is_empty() || domain.is_empty() {
        return None;
    }
    Some(format!(
        "https://{}/",
        preview_host(domain, version_id, worker_name)
    ))
}

/// Convenience: the preview URL for `(version, worker)` in `root`, or
/// `None` when this root has no usable preview domain configured.
pub fn url_for(root: &Path, version_id: &str, worker_name: &str) -> Option<String> {
    preview_url(&preview_domain(root)?, version_id, worker_name)
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
    }

    #[test]
    fn absent_section_or_file_is_none() {
        let dir = root_with(None);
        assert_eq!(preview_domain(dir.path()), None);
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
    fn url_for_requires_a_configured_domain() {
        let dir = root_with(Some("preview.example.com"));
        assert_eq!(
            url_for(dir.path(), "vrt_abc123", "edge-api").as_deref(),
            Some("https://vrt_abc123--edge-api.preview.example.com/")
        );
        let off = root_with(None);
        assert_eq!(url_for(off.path(), "vrt_abc123", "edge-api"), None);
    }
}
